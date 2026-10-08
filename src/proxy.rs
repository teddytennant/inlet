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
    reserved: u64,
    used: u64,
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

    pub fn insert(&self, id: &str, token: &str, reserved: u64) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.tokens.insert(token.to_string(), id.to_string());
        g.meters.insert(id.to_string(), Meter { reserved, used: 0 });
    }

    pub fn remove(&self, id: &str) -> u64 {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.tokens.retain(|_, task| task != id);
        g.meters.remove(id).map(|m| m.used).unwrap_or(0)
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
    let remainder = {
        let g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.meters
            .get(&id)
            .map(|m| m.reserved.saturating_sub(m.used))
            .unwrap_or(0)
    };
    if remainder == 0 || json.is_null() {
        hub.note(Note::Empty { id });
        return write_json(
            &mut client,
            402,
            r#"{"error":{"type":"empty_purse","message":"stop and post"}}"#,
        );
    }
    let want = clamp_body(&mut json, remainder);
    if !reserve(hub, &id, want) {
        hub.note(Note::Empty { id: id.clone() });
        return write_json(
            &mut client,
            402,
            r#"{"error":{"type":"empty_purse","message":"stop and post"}}"#,
        );
    }
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
        release(hub, &id, want);
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
            let actual = usage.unwrap_or(bytes.div_ceil(4)).min(want);
            let give_back = want.saturating_sub(actual);
            release(hub, &id, give_back);
            if actual > 0 {
                hub.note(Note::Cost {
                    id: id.clone(),
                    tokens: actual,
                });
            }
            if usage.is_some_and(|u| u > want) {
                hub.note(Note::Empty { id });
            }
            Ok(())
        }
        Err(e) => {
            // The reservation stays spent. Fail closed.
            hub.note(Note::Cost { id, tokens: want });
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

fn reserve(hub: &Hub, id: &str, n: u64) -> bool {
    let mut g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
    let Some(m) = g.meters.get_mut(id) else {
        return false;
    };
    if m.used.saturating_add(n) > m.reserved {
        return false;
    }
    m.used += n;
    true
}

fn release(hub: &Hub, id: &str, n: u64) {
    if n == 0 {
        return;
    }
    let mut g = hub.inner.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(m) = g.meters.get_mut(id) {
        m.used = m.used.saturating_sub(n);
    }
}

fn clamp_body(body: &mut Value, remainder: u64) -> u64 {
    let mut saw = false;
    let mut capped = remainder;
    for key in ["max_tokens", "max_completion_tokens", "max_output_tokens"] {
        if let Some(n) = body.get(key).and_then(|v| v.as_u64()) {
            let next = n.min(remainder);
            body[key] = Value::from(next);
            capped = capped.min(next);
            saw = true;
        }
    }
    if !saw {
        body["max_tokens"] = Value::from(remainder);
        remainder
    } else {
        capped.max(1).min(remainder)
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
        assert_eq!(clamp_body(&mut body, 40), 40);
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
        assert!(reserve(&hub, "t", 10));
        assert!(!reserve(&hub, "t", 1));
        release(&hub, "t", 4);
        assert!(reserve(&hub, "t", 4));
    }
}
