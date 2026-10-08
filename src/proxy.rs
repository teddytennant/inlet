use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::error::{err, Result};
use crate::text::redact;

#[derive(Debug)]
pub enum Note {
    Cost { id: String, tokens: u64 },
    Empty { id: String },
    Debug { level: u8, msg: String },
}

struct Meter {
    cap: u64,
    spent: u64,
    held: u64,
    lent: u64,
}

impl Meter {
    fn room(&self) -> u64 {
        self.cap
            .saturating_sub(self.spent)
            .saturating_sub(self.held)
            .saturating_sub(self.lent)
    }
}

struct Plan {
    output: u64,
    hold: u64,
}

#[derive(Debug)]
enum PlanError {
    Empty,
    Busy,
}

struct Inner {
    meters: HashMap<String, Meter>,
    tokens: HashMap<String, String>,
    upstream: Option<String>,
    key: Option<String>,
}

pub struct Hub {
    inner: Mutex<Inner>,
    notes: Mutex<Option<Sender<Note>>>,
    debug: Arc<AtomicU8>,
}

impl Hub {
    pub fn new(upstream: Option<String>, key: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                meters: HashMap::new(),
                tokens: HashMap::new(),
                upstream,
                key,
            }),
            notes: Mutex::new(None),
            debug: Arc::new(AtomicU8::new(1)),
        })
    }

    pub fn set_notes(&self, tx: Sender<Note>) {
        *self.notes.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
    }

    pub fn set_debug(&self, level: u8) {
        self.debug.store(level, Ordering::Relaxed);
    }

    pub fn id_of(&self, token: &str) -> Option<String> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.tokens.get(token).cloned()
    }

    pub fn insert(&self, id: &str, token: &str, cap: u64) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.tokens.insert(token.to_string(), id.to_string());
        g.meters.insert(
            id.to_string(),
            Meter {
                cap,
                spent: 0,
                held: 0,
                lent: 0,
            },
        );
    }

    /// Child slice stays out of the parent's room until the child settles.
    pub fn lend(&self, id: &str, n: u64) {
        if n == 0 {
            return;
        }
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(meter) = g.meters.get_mut(id) {
            meter.lent = meter.lent.saturating_add(n);
        }
    }

    /// Return a child slice. `used` stays spent; the rest is room again.
    pub fn reclaim(&self, id: &str, borrowed: u64, used: u64) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(meter) = g.meters.get_mut(id) else {
            return;
        };
        let borrowed = borrowed.min(meter.lent);
        meter.lent -= borrowed;
        let charge = used.min(borrowed);
        let room = meter.room();
        meter.spent = meter.spent.saturating_add(charge.min(room));
    }

    pub fn remove(&self, id: &str) -> u64 {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.tokens.retain(|_, task| task != id);
        g.meters
            .remove(id)
            .map(|m| {
                m.spent
                    .saturating_add(m.held)
                    .saturating_add(m.lent)
                    .min(m.cap)
            })
            .unwrap_or(0)
    }

    fn note(&self, note: Note) {
        if let Some(tx) = self
            .notes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            let _ = tx.send(note);
        }
    }
}

pub fn listen(path: &Path, hub: Arc<Hub>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _ = fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let hub = hub.clone();
            thread::spawn(move || {
                let _ = handle(conn, &hub);
            });
        }
    });
    Ok(())
}

fn handle(mut client: UnixStream, hub: &Hub) -> Result<()> {
    let _ = client.set_read_timeout(Some(Duration::from_secs(120)));
    let mut header = Vec::new();
    read_headers(&mut client, &mut header)?;
    let text = String::from_utf8_lossy(&header);
    let mut lines = text.split("\r\n");
    let request = lines.next().unwrap_or("");
    let mut parts = request.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method != "POST" || (path != "/v1/chat/completions" && path != "/v1/responses") {
        return write_json(
            &mut client,
            404,
            r#"{"error":{"type":"not_found","message":"unsupported"}}"#,
        );
    }
    let token = bearer(&text).ok_or_else(|| err("no token"))?;
    let len = content_length(&text).unwrap_or(0);
    if len > 8 * 1024 * 1024 {
        return write_json(
            &mut client,
            413,
            r#"{"error":{"type":"too_large","message":"body"}}"#,
        );
    }
    let mut body = vec![0u8; len];
    if len > 0 {
        client.read_exact(&mut body)?;
    }
    let id = {
        let g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.tokens.get(&token).cloned()
    };
    let Some(id) = id else {
        return write_json(
            &mut client,
            401,
            r#"{"error":{"type":"unauthorized","message":"token"}}"#,
        );
    };

    let mut json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if json.is_null() {
        hub.note(Note::Empty { id });
        return write_json(
            &mut client,
            402,
            r#"{"error":{"type":"empty_purse","message":"stop and post"}}"#,
        );
    }
    let planned = {
        let mut g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
        let decision = {
            let Some(meter) = g.meters.get_mut(&id) else {
                return write_json(
                    &mut client,
                    401,
                    r#"{"error":{"type":"unauthorized","message":"token"}}"#,
                );
            };
            match plan_request(meter, &json, body.len()) {
                Ok(plan) => {
                    apply_output(&mut json, plan.output);
                    meter.held = meter.held.saturating_add(plan.hold);
                    Ok(plan)
                }
                Err(error) => Err(error),
            }
        };
        match decision {
            Ok(plan) => plan,
            Err(PlanError::Empty) => {
                drop(g);
                hub.note(Note::Empty { id });
                return write_json(
                    &mut client,
                    402,
                    r#"{"error":{"type":"empty_purse","message":"stop and post"}}"#,
                );
            }
            Err(PlanError::Busy) => {
                return write_json(
                    &mut client,
                    402,
                    r#"{"error":{"type":"empty_purse","message":"stop and post"}}"#,
                );
            }
        }
    };
    let payload = serde_json::to_vec(&json)?;
    let upstream = hub
        .inner
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .upstream
        .clone();
    let key = hub
        .inner
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .key
        .clone();
    let Some(upstream) = upstream else {
        release_hold(hub, &id, planned.hold);
        return write_json(
            &mut client,
            503,
            r#"{"error":{"type":"no_upstream","message":"proxy.upstream is unset"}}"#,
        );
    };
    if hub.debug.load(Ordering::Relaxed) >= 4 {
        let msg = redact(&String::from_utf8_lossy(&payload));
        let msg = msg.chars().take(2048).collect::<String>();
        hub.note(Note::Debug { level: 4, msg });
    }
    match forward(&upstream, key.as_deref(), path, &payload, &mut client) {
        Ok((bytes, usage)) => {
            let actual = usage.unwrap_or(bytes.div_ceil(4));
            let (charge, kill) = settle(hub, &id, planned.hold, actual);
            if charge > 0 {
                hub.note(Note::Cost {
                    id: id.clone(),
                    tokens: charge,
                });
            }
            if kill {
                hub.note(Note::Empty { id });
            }
            Ok(())
        }
        Err(e) => {
            // The hold stays spent. Fail closed. Do not kill unless the purse is actually empty.
            let charge = settle_fail(hub, &id, planned.hold);
            if charge > 0 {
                hub.note(Note::Cost {
                    id: id.clone(),
                    tokens: charge,
                });
            }
            let _ = write_json(
                &mut client,
                502,
                &format!(
                    r#"{{"error":{{"type":"upstream","message":"{}"}}}}"#,
                    redact(&e.to_string()).replace('"', "'")
                ),
            );
            Err(e)
        }
    }
}

fn plan_request(
    meter: &Meter,
    body: &Value,
    raw_len: usize,
) -> std::result::Result<Plan, PlanError> {
    let room = meter.room();
    if room == 0 {
        return Err(if meter.held == 0 {
            PlanError::Empty
        } else {
            PlanError::Busy
        });
    }
    let prompt = (raw_len as u64 / 4).max(1);
    let output = match explicit_output(body, room) {
        Some(n) => n,
        None => (room / 2).max(1).min(room),
    };
    let hold = prompt
        .saturating_add(output)
        .min(room)
        .max(output)
        .min(room);
    if hold == 0 {
        return Err(if meter.held == 0 {
            PlanError::Empty
        } else {
            PlanError::Busy
        });
    }
    Ok(Plan { output, hold })
}

fn explicit_output(body: &Value, room: u64) -> Option<u64> {
    let mut capped = None;
    for key in ["max_tokens", "max_completion_tokens", "max_output_tokens"] {
        if let Some(n) = body.get(key).and_then(|v| v.as_u64()) {
            let next = n.min(room);
            capped = Some(capped.map(|cur: u64| cur.min(next)).unwrap_or(next));
        }
    }
    capped.map(|n| n.max(1).min(room))
}

fn apply_output(body: &mut Value, output: u64) {
    let mut saw = false;
    for key in ["max_tokens", "max_completion_tokens", "max_output_tokens"] {
        if body.get(key).is_some() {
            body[key] = Value::from(output);
            saw = true;
        }
    }
    if !saw {
        body["max_tokens"] = Value::from(output);
    }
}

fn settle(hub: &Hub, id: &str, hold: u64, actual: u64) -> (u64, bool) {
    let mut g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
    let Some(meter) = g.meters.get_mut(id) else {
        return (0, false);
    };
    meter.held = meter.held.saturating_sub(hold);
    let room = meter.room();
    let charge = actual.min(room);
    meter.spent = meter.spent.saturating_add(charge);
    (charge, actual > charge)
}

fn settle_fail(hub: &Hub, id: &str, hold: u64) -> u64 {
    let mut g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
    let Some(meter) = g.meters.get_mut(id) else {
        return 0;
    };
    meter.held = meter.held.saturating_sub(hold);
    let charge = hold.min(meter.room());
    meter.spent = meter.spent.saturating_add(charge);
    charge
}

fn release_hold(hub: &Hub, id: &str, n: u64) {
    if n == 0 {
        return;
    }
    let mut g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(meter) = g.meters.get_mut(id) {
        meter.held = meter.held.saturating_sub(n);
    }
}

fn forward(
    upstream: &str,
    key: Option<&str>,
    path: &str,
    body: &[u8],
    client: &mut UnixStream,
) -> Result<(u64, Option<u64>)> {
    let origin = parse_origin(upstream)?;
    let mut stream = connect(&origin)?;
    let host = if origin.port == origin.default_port() {
        origin.host.clone()
    } else {
        format!("{}:{}", origin.host, origin.port)
    };
    let auth = match key {
        Some(k) => format!("Authorization: Bearer {k}\r\n"),
        None => String::new(),
    };
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(req.as_bytes())?;
    stream.write_all(body)?;
    let mut header = Vec::new();
    read_headers(&mut stream, &mut header)?;
    client.write_all(&header)?;
    let header_text = String::from_utf8_lossy(&header);
    let mut bytes = 0u64;
    let mut tail = Vec::new();
    if let Some(len) = content_length(&header_text) {
        copy_n(&mut stream, client, len, &mut bytes, &mut tail)?;
    } else {
        let mut buf = [0u8; 8192];
        loop {
            let n = stream.read(&mut buf)?;
            if n == 0 {
                break;
            }
            client.write_all(&buf[..n])?;
            push_tail(&mut tail, &buf[..n]);
            bytes += n as u64;
        }
    }
    Ok((bytes, extract_usage(&tail)))
}

fn copy_n(
    src: &mut dyn Read,
    dst: &mut dyn Write,
    mut left: usize,
    bytes: &mut u64,
    tail: &mut Vec<u8>,
) -> Result<()> {
    let mut buf = [0u8; 8192];
    while left > 0 {
        let cap = left.min(buf.len());
        let n = src.read(&mut buf[..cap])?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])?;
        push_tail(tail, &buf[..n]);
        *bytes += n as u64;
        left -= n;
    }
    Ok(())
}

fn push_tail(tail: &mut Vec<u8>, chunk: &[u8]) {
    tail.extend_from_slice(chunk);
    if tail.len() > 24 * 1024 {
        let drop_n = tail.len() - 24 * 1024;
        tail.drain(..drop_n);
    }
}

fn extract_usage(buf: &[u8]) -> Option<u64> {
    let text = String::from_utf8_lossy(buf);
    if let Ok(v) = serde_json::from_str::<Value>(text.trim()) {
        if let Some(n) = usage_of(&v) {
            return Some(n);
        }
    }
    let mut best = None;
    let mut rest = text.as_ref();
    while let Some(idx) = rest.find("\"usage\"") {
        let after = &rest[idx + "\"usage\"".len()..];
        if let Some(start) = after.find('{') {
            if let Some(obj) = take_object(&after[start..]) {
                if let Ok(v) = serde_json::from_str::<Value>(obj) {
                    best = usage_obj(&v).or(best);
                }
            }
        }
        rest = &rest[idx + 7..];
    }
    best
}

fn usage_of(v: &Value) -> Option<u64> {
    v.get("usage").and_then(usage_obj)
}

fn usage_obj(v: &Value) -> Option<u64> {
    if let Some(t) = v.get("total_tokens").and_then(|n| n.as_u64()) {
        return Some(t);
    }
    let prompt = v
        .get("prompt_tokens")
        .or_else(|| v.get("input_tokens"))
        .and_then(|n| n.as_u64());
    let completion = v
        .get("completion_tokens")
        .or_else(|| v.get("output_tokens"))
        .and_then(|n| n.as_u64());
    match (prompt, completion) {
        (None, None) => None,
        (p, c) => Some(p.unwrap_or(0) + c.unwrap_or(0)),
    }
}

fn take_object(s: &str) -> Option<&str> {
    let mut depth = 0i32;
    let mut end = None;
    for (i, ch) in s.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i + 1);
                    break;
                }
            }
            _ => {}
        }
    }
    end.map(|n| &s[..n])
}

struct Origin {
    https: bool,
    host: String,
    port: u16,
}

impl Origin {
    fn default_port(&self) -> u16 {
        if self.https {
            443
        } else {
            80
        }
    }
}

fn parse_origin(raw: &str) -> Result<Origin> {
    let (https, rest) = if let Some(r) = raw.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = raw.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(err(format!("proxy upstream must be http(s): {raw}")));
    };
    let rest = rest.split('/').next().unwrap_or(rest);
    let (host, port) = if let Some((h, p)) = rest.rsplit_once(':') {
        if h.starts_with('[') {
            (rest.to_string(), if https { 443 } else { 80 })
        } else {
            let port = p.parse::<u16>().map_err(|_| err("bad upstream port"))?;
            (h.to_string(), port)
        }
    } else {
        (rest.to_string(), if https { 443 } else { 80 })
    };
    if host.is_empty() {
        return Err(err("empty upstream host"));
    }
    Ok(Origin { https, host, port })
}

fn connect(origin: &Origin) -> Result<Box<dyn Rw>> {
    let tcp = TcpStream::connect((origin.host.as_str(), origin.port))?;
    tcp.set_nodelay(true)?;
    if !origin.https {
        return Ok(Box::new(tcp));
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(origin.host.clone())
        .map_err(|_| err("bad tls name"))?;
    let conn =
        rustls::ClientConnection::new(Arc::new(config), name).map_err(|e| err(e.to_string()))?;
    Ok(Box::new(rustls::StreamOwned::new(conn, tcp)))
}

trait Rw: Read + Write {}
impl<T: Read + Write> Rw for T {}

fn read_headers(r: &mut dyn Read, out: &mut Vec<u8>) -> Result<()> {
    let mut byte = [0u8; 1];
    while out.len() < 64 * 1024 {
        let n = r.read(&mut byte)?;
        if n == 0 {
            break;
        }
        out.push(byte[0]);
        if out.ends_with(b"\r\n\r\n") {
            return Ok(());
        }
    }
    Err(err("headers"))
}

fn content_length(header: &str) -> Option<usize> {
    for line in header.split("\r\n") {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("content-length") {
            return v.trim().parse().ok();
        }
    }
    None
}

fn bearer(header: &str) -> Option<String> {
    for line in header.split("\r\n") {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("authorization") {
            let v = v.trim();
            let token = v
                .strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))?;
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    None
}

fn write_json(w: &mut impl Write, status: u16, body: &str) -> Result<()> {
    let reason = match status {
        401 => "Unauthorized",
        402 => "Payment Required",
        404 => "Not Found",
        413 => "Payload Too Large",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Error",
    };
    write!(
        w,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    Ok(())
}

pub fn channel() -> (Sender<Note>, mpsc::Receiver<Note>) {
    mpsc::channel()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_and_usage() {
        let mut body = serde_json::json!({"max_tokens": 500, "messages": []});
        let meter = Meter {
            cap: 40,
            spent: 0,
            held: 0,
            lent: 0,
        };
        let plan = plan_request(&meter, &body, body.to_string().len()).unwrap();
        assert_eq!(plan.output, 40);
        apply_output(&mut body, plan.output);
        assert_eq!(body["max_tokens"], 40);
        let raw = br#"{"usage":{"prompt_tokens":3,"completion_tokens":4}}"#;
        assert_eq!(extract_usage(raw), Some(7));
        assert_eq!(extract_usage(b"no usage here, 8 bytes"), None);
    }

    #[test]
    fn headers_skip_the_request_line() {
        let header = "POST /v1/chat/completions HTTP/1.1\r\nAuthorization: Bearer abc\r\nContent-Length: 12\r\n\r\n";
        assert_eq!(bearer(header).as_deref(), Some("abc"));
        assert_eq!(content_length(header), Some(12));
    }

    #[test]
    fn empty_reserve_fails() {
        let hub = Hub::new(None, None);
        hub.insert("t", "tok", 10);
        assert!(hold_n(&hub, "t", 10));
        assert!(!hold_n(&hub, "t", 1));
        release_hold(&hub, "t", 4);
        assert!(hold_n(&hub, "t", 4));
    }

    #[test]
    fn unbounded_calls_split_the_room() {
        let mut meter = Meter {
            cap: 100,
            spent: 0,
            held: 0,
            lent: 0,
        };
        let body = serde_json::json!({"messages": []});
        let first = plan_request(&meter, &body, 4).unwrap();
        meter.held += first.hold;
        let second = plan_request(&meter, &body, 4).unwrap();
        assert!(first.output >= 40, "{}", first.output);
        assert!(second.output >= 20, "{}", second.output);
        assert!(first.hold + second.hold <= 100);
    }

    #[test]
    fn usage_above_the_hold_is_charged() {
        let hub = Hub::new(None, None);
        hub.insert("t", "tok", 100);
        {
            let mut g = hub.inner.lock().unwrap();
            g.meters.get_mut("t").unwrap().held = 10;
        }
        let (charge, kill) = settle(&hub, "t", 10, 22);
        assert_eq!(charge, 22);
        assert!(!kill);
        let g = hub.inner.lock().unwrap();
        assert_eq!(g.meters.get("t").unwrap().spent, 22);
    }

    #[test]
    fn explicit_max_keeps_the_output_cap() {
        let meter = Meter {
            cap: 30,
            spent: 0,
            held: 0,
            lent: 0,
        };
        let body = serde_json::json!({"max_tokens": 20});
        let plan = plan_request(&meter, &body, 16).unwrap();
        assert_eq!(plan.output, 20);
        assert!(plan.hold <= 30 && plan.hold >= 20);
    }

    fn hold_n(hub: &Hub, id: &str, n: u64) -> bool {
        let mut g = hub.inner.lock().unwrap();
        let Some(meter) = g.meters.get_mut(id) else {
            return false;
        };
        if meter.room() < n {
            return false;
        }
        meter.held += n;
        true
    }
}
