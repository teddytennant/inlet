//! One short call for p_success. Headers, samples, constraints, a few posts.
//! Not transcripts, not registry bodies, not another worker's scratch.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{err, Result};

pub const CACHE_TTL: Duration = Duration::from_secs(5);
pub const POST_WINDOW: usize = 8;

#[derive(Debug, Clone)]
pub struct Header {
    pub worker: String,
    pub tags: Vec<String>,
    pub goal: String,
    pub value: u64,
    pub tokens: u64,
    pub seconds: u64,
    pub memory_mb: u64,
    pub pids: u64,
    pub verifier: bool,
}

#[derive(Debug, Clone)]
pub struct ConstraintIn {
    pub id: String,
    pub text: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PostIn {
    pub author: String,
    pub role: String,
    pub weight: u64,
    pub channel: String,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct Call {
    pub model: String,
    pub header: Header,
    pub parents: Vec<Header>,
    pub samples: Vec<u64>,
    pub constraints: Vec<ConstraintIn>,
    pub posts: Vec<PostIn>,
    pub human_weight: u64,
}

#[derive(Debug, Clone)]
pub struct Answer {
    pub p_num: u64,
    pub p_den: u64,
    pub conflicts: Vec<String>,
    pub tokens: u64,
}

impl Call {
    pub fn body(&self) -> Value {
        json!({
            "model": self.model,
            "header": header_json(&self.header),
            "parents": self.parents.iter().map(header_json).collect::<Vec<_>>(),
            "samples": self.samples,
            "constraints": self.constraints.iter().map(|c| json!({
                "id": c.id,
                "text": c.text,
                "tags": c.tags,
            })).collect::<Vec<_>>(),
            "posts": self.posts.iter().map(|p| json!({
                "author": p.author,
                "role": p.role,
                "weight": p.weight,
                "channel": p.channel,
                "text": p.text,
            })).collect::<Vec<_>>(),
            "human_weight": self.human_weight,
        })
    }

    pub fn key(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.body().to_string().hash(&mut hasher);
        hasher.finish()
    }
}

fn header_json(h: &Header) -> Value {
    json!({
        "worker": h.worker,
        "tags": h.tags,
        "goal": h.goal,
        "value": h.value,
        "budget": {
            "tokens": h.tokens,
            "seconds": h.seconds,
            "memory_mb": h.memory_mb,
            "pids": h.pids,
        },
        "verifier": h.verifier,
    })
}

pub fn ask(endpoint: &str, call: &Call, timeout: Duration) -> Result<Answer> {
    let url = parse_url(endpoint)?;
    let body = serde_json::to_vec(&call.body())?;
    let mut stream = connect(&url, timeout)?;
    write!(
        stream,
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        url.path,
        url.host_header(),
        body.len()
    )?;
    stream.write_all(&body)?;
    let mut raw = Vec::new();
    read_http(&mut stream, &mut raw, timeout)?;
    let text = String::from_utf8_lossy(&raw);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        return Err(err(format!("decision http {status}")));
    }
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(raw.len());
    let payload = &raw[header_end..];
    parse_answer(payload)
}

/// One short summary. The caller charges the decision purse and skips the call when it is empty.
pub fn summarize(endpoint: &str, prompt: &str, timeout: Duration) -> Result<(String, u64)> {
    let url = parse_url(endpoint)?;
    let body = serde_json::to_vec(&json!({
        "model": "rollup",
        "messages": [{ "role": "user", "content": prompt }],
    }))?;
    let mut stream = connect(&url, timeout)?;
    write!(
        stream,
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        url.path,
        url.host_header(),
        body.len()
    )?;
    stream.write_all(&body)?;
    let mut raw = Vec::new();
    read_http(&mut stream, &mut raw, timeout)?;
    let text = String::from_utf8_lossy(&raw);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        return Err(err(format!("rollup http {status}")));
    }
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(raw.len());
    let value: Value =
        serde_json::from_slice(&raw[header_end..]).map_err(|_| err("rollup body"))?;
    let summary = value
        .pointer("/choices/0/message/content")
        .and_then(|c| c.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| err("rollup body"))?
        .to_string();
    let tokens = usage_tokens(&value).unwrap_or(1).max(1);
    Ok((summary, tokens))
}

pub fn parse_answer(body: &[u8]) -> Result<Answer> {
    let value: Value = serde_json::from_slice(body).map_err(|_| err("decision body"))?;
    let tokens = usage_tokens(&value).unwrap_or_else(|| (body.len() as u64 / 4).max(1));
    let judged = unwrap_content(&value);
    let (p_num, p_den) = probability(&judged).ok_or_else(|| err("decision p"))?;
    Ok(Answer {
        p_num,
        p_den: p_den.max(1),
        conflicts: conflicts_of(&judged),
        tokens: tokens.max(1),
    })
}

fn unwrap_content(value: &Value) -> Value {
    let content = value
        .pointer("/choices/0/message/content")
        .or_else(|| value.pointer("/choices/0/text"));
    if let Some(text) = content.and_then(|c| c.as_str()) {
        if let Ok(inner) = serde_json::from_str::<Value>(text) {
            return inner;
        }
    }
    if let Some(obj) = content.filter(|c| c.is_object()) {
        return obj.clone();
    }
    value.clone()
}

fn probability(value: &Value) -> Option<(u64, u64)> {
    for key in ["p_success", "score", "predicate", "noul"] {
        if let Some(p) = value.get(key).and_then(as_unit) {
            return Some(p);
        }
    }
    if let Some(p) = value.pointer("/noul/score").and_then(as_unit) {
        return Some(p);
    }
    None
}

fn as_unit(value: &Value) -> Option<(u64, u64)> {
    if let Some(flag) = value.as_bool() {
        return Some(if flag { (1, 1) } else { (0, 1) });
    }
    let n = value.as_f64()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let n = n.min(1.0);
    Some(((n * 1_000_000.0).round() as u64, 1_000_000))
}

fn conflicts_of(value: &Value) -> Vec<String> {
    value
        .get("conflicts")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    item.as_str().map(str::to_string).or_else(|| {
                        item.get("id")
                            .and_then(|id| id.as_str())
                            .map(str::to_string)
                    })
                })
                .filter(|id| !id.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn usage_tokens(value: &Value) -> Option<u64> {
    value
        .pointer("/usage/total_tokens")
        .or_else(|| value.pointer("/usage/total"))
        .and_then(|n| n.as_u64())
        .filter(|n| *n > 0)
}

struct Url {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

impl Url {
    fn host_header(&self) -> String {
        let default = if self.https { 443 } else { 80 };
        if self.port == default {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

fn parse_url(raw: &str) -> Result<Url> {
    let (https, rest) = if let Some(rest) = raw.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix("http://") {
        (false, rest)
    } else {
        return Err(err("decision endpoint must be http(s)"));
    };
    if rest.is_empty() {
        return Err(err("empty decision endpoint"));
    }
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, tail) = rest.split_once(']').ok_or_else(|| err("bad endpoint"))?;
        let port = if let Some(p) = tail.strip_prefix(':') {
            p.parse::<u16>().map_err(|_| err("bad endpoint port"))?
        } else if https {
            443
        } else {
            80
        };
        (host.to_string(), port)
    } else if let Some((host, p)) = authority.rsplit_once(':') {
        let port = p.parse::<u16>().map_err(|_| err("bad endpoint port"))?;
        (host.to_string(), port)
    } else {
        (authority.to_string(), if https { 443 } else { 80 })
    };
    if host.is_empty() {
        return Err(err("empty endpoint host"));
    }
    let path = if path.is_empty() {
        "/".to_string()
    } else {
        path
    };
    Ok(Url {
        https,
        host,
        port,
        path,
    })
}

fn connect(url: &Url, timeout: Duration) -> Result<Box<dyn Rw>> {
    let addr = (url.host.as_str(), url.port)
        .to_socket_addrs()
        .map_err(|_| err("decision dns"))?
        .next()
        .ok_or_else(|| err("decision dns"))?;
    let tcp = TcpStream::connect_timeout(&addr, timeout).map_err(|_| err("decision connect"))?;
    tcp.set_nodelay(true)?;
    let timeout = Some(timeout);
    tcp.set_read_timeout(timeout)?;
    tcp.set_write_timeout(timeout)?;
    if !url.https {
        return Ok(Box::new(tcp));
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from(url.host.clone())
        .map_err(|_| err("bad tls name"))?;
    let conn =
        rustls::ClientConnection::new(Arc::new(config), name).map_err(|e| err(e.to_string()))?;
    Ok(Box::new(rustls::StreamOwned::new(conn, tcp)))
}

trait Rw: Read + Write {}
impl<T: Read + Write> Rw for T {}

fn read_http(r: &mut dyn Read, out: &mut Vec<u8>, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    let mut byte = [0u8; 1];
    loop {
        if start.elapsed() > timeout.saturating_mul(2) {
            return Err(err("decision timeout"));
        }
        let n = r.read(&mut byte).map_err(|_| err("decision read"))?;
        if n == 0 {
            break;
        }
        out.push(byte[0]);
        if out.len() > 256 * 1024 {
            return Err(err("decision body"));
        }
        if out.ends_with(b"\r\n\r\n") {
            let text = String::from_utf8_lossy(out);
            let Some(len) = content_length(&text) else {
                loop {
                    let n = r.read(&mut byte).unwrap_or(0);
                    if n == 0 || out.len() > 64 * 1024 {
                        break;
                    }
                    out.push(byte[0]);
                }
                return Ok(());
            };
            if len > 64 * 1024 {
                return Err(err("decision body"));
            }
            let mut rest = vec![0u8; len];
            if len > 0 {
                r.read_exact(&mut rest).map_err(|_| err("decision body"))?;
                out.extend_from_slice(&rest);
            }
            return Ok(());
        }
    }
    if out.is_empty() {
        Err(err("decision read"))
    } else {
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn call(goal: &str) -> Call {
        Call {
            model: "gpt-6-luna".into(),
            header: Header {
                worker: "pi".into(),
                tags: vec!["code".into()],
                goal: goal.into(),
                value: 10,
                tokens: 5,
                seconds: 1,
                memory_mb: 32,
                pids: 2,
                verifier: true,
            },
            parents: Vec::new(),
            samples: vec![4, 5],
            constraints: Vec::new(),
            posts: vec![PostIn {
                author: "you".into(),
                role: "human".into(),
                weight: 4,
                channel: "general".into(),
                text: "hi".into(),
            }],
            human_weight: 4,
        }
    }

    #[test]
    fn payload_is_a_header_not_a_transcript() {
        let body = call("ship").body();
        let text = body.to_string();
        assert!(text.contains("ship"));
        assert!(text.contains("\"samples\":[4,5]"));
        assert!(text.contains("\"constraints\":[]"));
        assert!(text.contains("\"human_weight\":4"));
        assert!(text.contains("\"weight\":4"));
        assert!(!text.contains("transcript"));
        assert!(body.get("registry").is_none());
        assert!(body.get("scratch").is_none());
        let other = call("else");
        assert_ne!(call("ship").key(), other.key());
        assert_eq!(call("ship").key(), call("ship").key());
    }

    #[test]
    fn predicate_score_and_noul_are_one_slot() {
        let pred = parse_answer(br#"{"predicate":true,"conflicts":[]}"#).unwrap();
        assert_eq!((pred.p_num, pred.p_den), (1, 1));
        let no = parse_answer(br#"{"predicate":false}"#).unwrap();
        assert_eq!((no.p_num, no.p_den), (0, 1));
        let score = parse_answer(br#"{"score":0.25,"conflicts":["c1"]}"#).unwrap();
        assert_eq!(score.p_num, 250_000);
        assert_eq!(score.conflicts, vec!["c1".to_string()]);
        let noul = parse_answer(br#"{"noul":{"score":1},"usage":{"total_tokens":9}}"#).unwrap();
        assert_eq!((noul.p_num, noul.p_den), (1_000_000, 1_000_000));
        assert_eq!(noul.tokens, 9);
        let wrapped = parse_answer(
            br#"{"choices":[{"message":{"content":"{\"p_success\":0.5,\"conflicts\":[{\"id\":\"k\"}]}"}}],"usage":{"total_tokens":3}}"#,
        )
        .unwrap();
        assert_eq!(wrapped.p_num, 500_000);
        assert_eq!(wrapped.conflicts, vec!["k".to_string()]);
        assert_eq!(wrapped.tokens, 3);
        assert!(parse_answer(br#"{"nope":true}"#).is_err());
    }
}
