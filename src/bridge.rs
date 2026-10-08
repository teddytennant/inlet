//! A chat bridge is a client of the operator socket, not a second board.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::error::{err, Result};
use crate::paths;

#[derive(Clone)]
struct Api {
    https: bool,
    host: String,
    port: u16,
    prefix: String,
}

struct Incoming {
    update_id: i64,
    chat: i64,
    text: String,
}

pub fn telegram(home: &Path) -> Result<()> {
    let token = std::env::var("TELEGRAM_BOT_TOKEN")
        .ok()
        .filter(|token| {
            !token.is_empty() && !token.chars().any(|c| c == '/' || c == ' ' || c == '?')
        })
        .ok_or_else(|| err("TELEGRAM_BOT_TOKEN"))?;
    let base = std::env::var("TELEGRAM_API_BASE")
        .ok()
        .filter(|base| !base.is_empty())
        .unwrap_or_else(|| "https://api.telegram.org".to_string());
    let only = match std::env::var("TELEGRAM_CHAT") {
        Ok(raw) if !raw.is_empty() => Some(raw.parse::<i64>().map_err(|_| err("TELEGRAM_CHAT"))?),
        _ => None,
    };
    let api = parse_base(&base)?;
    let stream = std::os::unix::net::UnixStream::connect(paths::operator_sock(home))
        .map_err(|_| err("daemon is not up"))?;
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    let mut writer = stream.try_clone()?;
    writer.write_all(b"{\"op\":\"watch\",\"debug\":0}\n")?;
    writer.flush()?;
    let chat = Arc::new(Mutex::new(only));
    let writer = Arc::new(Mutex::new(writer));
    let poll_writer = Arc::clone(&writer);
    let poll_chat = Arc::clone(&chat);
    let poll_api = api.clone();
    let poll_token = token.clone();
    thread::spawn(move || {
        let mut offset = 0i64;
        loop {
            match pull(&poll_api, &poll_token, offset) {
                Ok(msgs) if msgs.is_empty() => thread::sleep(Duration::from_millis(200)),
                Ok(msgs) => {
                    for msg in msgs {
                        offset = offset.max(msg.update_id.saturating_add(1));
                        let allow = {
                            let mut slot = poll_chat.lock().unwrap_or_else(|p| p.into_inner());
                            match *slot {
                                Some(only) => msg.chat == only,
                                None => {
                                    *slot = Some(msg.chat);
                                    true
                                }
                            }
                        };
                        if !allow {
                            continue;
                        }
                        let line = serde_json::json!({"op":"say","text": msg.text}).to_string();
                        let mut out = poll_writer.lock().unwrap_or_else(|p| p.into_inner());
                        let _ = writeln!(out, "{line}");
                        let _ = out.flush();
                    }
                }
                Err(_) => thread::sleep(Duration::from_millis(500)),
            }
        }
    });
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => return Err(err("daemon closed")),
            Ok(_) => {
                let Some(text) = outbound(line.trim()) else {
                    continue;
                };
                let chat_id = *chat.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(chat_id) = chat_id {
                    let _ = push(&api, &token, chat_id, &text);
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                continue;
            }
            Err(_) => return Err(err("operator socket")),
        }
    }
}

fn outbound(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    if value.get("ev").and_then(|ev| ev.as_str()) != Some("post") {
        return None;
    }
    if value.get("role").and_then(|role| role.as_str()) == Some("human") {
        return None;
    }
    let author = value
        .get("author")
        .and_then(|author| author.as_str())
        .unwrap_or("?");
    let text = value
        .get("text")
        .and_then(|text| text.as_str())
        .unwrap_or("");
    if text.is_empty() {
        return None;
    }
    let mut text = format!("{author}: {text}");
    if text.chars().count() > 4000 {
        text = text.chars().take(4000).collect();
    }
    Some(text)
}

fn incoming(body: &str) -> Result<Vec<Incoming>> {
    let value: Value = serde_json::from_str(body).map_err(|_| err("telegram"))?;
    if value.get("ok").and_then(|ok| ok.as_bool()) != Some(true) {
        return Err(err("telegram"));
    }
    let mut out = Vec::new();
    for item in value
        .get("result")
        .and_then(|result| result.as_array())
        .into_iter()
        .flatten()
    {
        let update_id = item.get("update_id").and_then(|n| n.as_i64()).unwrap_or(0);
        let Some(message) = item.get("message") else {
            continue;
        };
        let Some(text) = message.get("text").and_then(|text| text.as_str()) else {
            continue;
        };
        let Some(chat) = message
            .get("chat")
            .and_then(|chat| chat.get("id"))
            .and_then(|n| n.as_i64())
        else {
            continue;
        };
        if text.trim().is_empty() {
            continue;
        }
        out.push(Incoming {
            update_id,
            chat,
            text: text.to_string(),
        });
    }
    Ok(out)
}

fn pull(api: &Api, token: &str, offset: i64) -> Result<Vec<Incoming>> {
    let path = format!(
        "{}/bot{token}/getUpdates?offset={offset}&timeout=1",
        api.prefix
    );
    let body = exchange(api, "GET", &path, None)?;
    incoming(&body)
}

fn push(api: &Api, token: &str, chat: i64, text: &str) -> Result<()> {
    let path = format!("{}/bot{token}/sendMessage", api.prefix);
    let body = serde_json::json!({"chat_id": chat, "text": text}).to_string();
    let response = exchange(api, "POST", &path, Some(body.as_bytes()))?;
    incoming_ok(&response)
}

fn incoming_ok(body: &str) -> Result<()> {
    let value: Value = serde_json::from_str(body).map_err(|_| err("telegram"))?;
    if value.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
        Ok(())
    } else {
        Err(err("telegram"))
    }
}

fn parse_base(raw: &str) -> Result<Api> {
    let (https, rest) = if let Some(rest) = raw.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix("http://") {
        (false, rest)
    } else {
        return Err(err("telegram base"));
    };
    let (authority, prefix) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].trim_end_matches('/').to_string()),
        None => (rest, String::new()),
    };
    if authority.is_empty() {
        return Err(err("telegram base"));
    }
    let (host, port) = if let Some((host, port)) = authority.rsplit_once(':') {
        let port = port.parse::<u16>().map_err(|_| err("telegram base"))?;
        (host.to_string(), port)
    } else {
        (authority.to_string(), if https { 443 } else { 80 })
    };
    if host.is_empty() {
        return Err(err("telegram base"));
    }
    Ok(Api {
        https,
        host,
        port,
        prefix,
    })
}

fn exchange(api: &Api, method: &str, path: &str, body: Option<&[u8]>) -> Result<String> {
    let mut stream = connect(api)?;
    let host = if (api.https && api.port == 443) || (!api.https && api.port == 80) {
        api.host.clone()
    } else {
        format!("{}:{}", api.host, api.port)
    };
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    if let Some(body) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes())?;
    if let Some(body) = body {
        stream.write_all(body)?;
    }
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|_| err("telegram read"))?;
    let text = String::from_utf8_lossy(&buf);
    if !text.starts_with("HTTP/1.1 200") && !text.starts_with("HTTP/1.0 200") {
        return Err(err("telegram"));
    }
    text.split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .ok_or_else(|| err("telegram"))
}

fn connect(api: &Api) -> Result<Box<dyn Rw>> {
    let tcp = TcpStream::connect((api.host.as_str(), api.port)).map_err(|_| err("telegram"))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(5)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))?;
    if !api.https {
        return Ok(Box::new(tcp));
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name =
        rustls::pki_types::ServerName::try_from(api.host.clone()).map_err(|_| err("telegram"))?;
    let conn =
        rustls::ClientConnection::new(Arc::new(config), name).map_err(|_| err("telegram"))?;
    Ok(Box::new(rustls::StreamOwned::new(conn, tcp)))
}

trait Rw: Read + Write {}
impl<T: Read + Write> Rw for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humans_stay_on_the_socket() {
        assert!(outbound(r#"{"ev":"post","role":"human","author":"you","text":"hi"}"#).is_none());
        assert_eq!(
            outbound(r#"{"ev":"post","role":"worker","author":"abc","text":"yo"}"#).as_deref(),
            Some("abc: yo")
        );
        assert!(outbound(r#"{"ok":true,"live":1}"#).is_none());
        let parsed = incoming(
            r#"{"ok":true,"result":[{"update_id":3,"message":{"chat":{"id":9},"text":"hey"}},{"update_id":4,"message":{"chat":{"id":9}}}]}"#,
        )
        .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].update_id, 3);
        assert_eq!(parsed[0].chat, 9);
        assert_eq!(parsed[0].text, "hey");
        assert!(parse_base("http://127.0.0.1:9").is_ok());
        assert!(parse_base("notaurl").is_err());
    }
}
