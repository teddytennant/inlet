//! Discord gateway and REST. A client of the operator socket.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{json, Value};

use crate::board;
use crate::error::{err, Result};

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const INTENTS: u64 = 33281;

struct HttpApi {
    https: bool,
    host: String,
    port: u16,
    prefix: String,
}

struct WsUrl {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

struct Note {
    author: String,
    text: String,
    channel: String,
    worker: String,
    thread: String,
}

struct Sock {
    stream: Box<dyn Io>,
    buf: Vec<u8>,
}

trait Io: Read + Write {}
impl<T: Read + Write> Io for T {}

pub fn run(home: &Path) -> Result<()> {
    let token = load_token(home)?;
    let guild = std::env::var("DISCORD_GUILD")
        .ok()
        .filter(|guild| !guild.is_empty())
        .ok_or_else(|| err("DISCORD_GUILD"))?;
    let api = parse_http(
        &std::env::var("DISCORD_API_BASE")
            .ok()
            .filter(|base| !base.is_empty())
            .unwrap_or_else(|| "https://discord.com/api/v10".to_string()),
    )?;
    let gateway = std::env::var("DISCORD_GATEWAY")
        .ok()
        .filter(|url| !url.is_empty())
        .unwrap_or_else(|| "wss://gateway.discord.gg/?v=10&encoding=json".to_string());
    let stream = std::os::unix::net::UnixStream::connect(crate::paths::operator_sock(home))
        .map_err(|_| err("daemon is not up"))?;
    stream.set_read_timeout(Some(Duration::from_secs(60)))?;
    let mut writer = stream.try_clone()?;
    writer.write_all(b"{\"op\":\"watch\",\"debug\":0}\n")?;
    writer.flush()?;
    let _hold = crate::spin::Hold::start(&crate::spin::BRIDGE)?;
    let writer = Arc::new(Mutex::new(writer));
    let say = Arc::clone(&writer);
    let gw_token = token.clone();
    thread::spawn(move || gateway_loop(&gateway, &gw_token, say));
    let queue = Arc::new(Mutex::new(Vec::<Note>::new()));
    let flush_queue = Arc::clone(&queue);
    let rest_token = token;
    thread::spawn(move || flush_loop(flush_queue, api, guild, rest_token));
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => return Err(err("daemon closed")),
            Ok(_) => {
                if let Some(note) = note_of(line.trim()) {
                    queue
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(note);
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

fn load_token(home: &Path) -> Result<String> {
    if let Ok(token) = std::env::var("DISCORD_BOT_TOKEN") {
        return clean_token(&token);
    }
    let path = home.join("keys/discord.token");
    let meta = std::fs::metadata(&path).map_err(|_| err("DISCORD_BOT_TOKEN"))?;
    let mode = std::os::unix::fs::PermissionsExt::mode(&meta.permissions()) & 0o777;
    if mode & 0o077 != 0 {
        return Err(err("discord token file is loose"));
    }
    let token = std::fs::read_to_string(&path).map_err(|_| err("DISCORD_BOT_TOKEN"))?;
    clean_token(token.trim())
}

fn clean_token(token: &str) -> Result<String> {
    if token.is_empty() || token.chars().any(|c| c.is_whitespace()) {
        return Err(err("DISCORD_BOT_TOKEN"));
    }
    Ok(token.to_string())
}

fn note_of(line: &str) -> Option<Note> {
    let value: Value = serde_json::from_str(line).ok()?;
    if value.get("ev").and_then(|ev| ev.as_str()) != Some("post") {
        return None;
    }
    let role = value
        .get("role")
        .and_then(|role| role.as_str())
        .unwrap_or("");
    if role == "human" {
        return None;
    }
    let text = value
        .get("text")
        .and_then(|text| text.as_str())
        .unwrap_or("");
    if text.is_empty() {
        return None;
    }
    let author = value
        .get("author")
        .and_then(|author| author.as_str())
        .unwrap_or("?")
        .to_string();
    let channel = value
        .get("channel")
        .and_then(|channel| channel.as_str())
        .filter(|channel| !channel.is_empty())
        .unwrap_or("general")
        .to_string();
    let worker = value
        .get("worker")
        .and_then(|worker| worker.as_str())
        .unwrap_or("")
        .to_string();
    let thread = board::thread_of(role, &author);
    Some(Note {
        author,
        text: text.to_string(),
        channel,
        worker,
        thread,
    })
}

fn flush_loop(queue: Arc<Mutex<Vec<Note>>>, api: HttpApi, guild: String, token: String) {
    let mut room = Room {
        api,
        guild,
        token,
        cache: BTreeMap::new(),
    };
    loop {
        thread::sleep(Duration::from_millis(300));
        let batch = {
            let mut queue = queue.lock().unwrap_or_else(|poison| poison.into_inner());
            std::mem::take(&mut *queue)
        };
        if batch.is_empty() {
            continue;
        }
        if batch.len() == 1 {
            if room.send_one(&batch[0]).is_err() {
                let _ = room.send_digest(&batch);
            }
        } else {
            let _ = room.send_digest(&batch);
        }
    }
}

struct Room {
    api: HttpApi,
    guild: String,
    token: String,
    cache: BTreeMap<String, String>,
}

impl Room {
    fn send_one(&mut self, note: &Note) -> Result<()> {
        let text = clip(&format!("{}: {}", note.author, note.text), 2000);
        self.post_text(&note.channel, &note.worker, &note.thread, &text)
    }

    fn send_digest(&mut self, batch: &[Note]) -> Result<()> {
        let mut groups: BTreeMap<String, Vec<&Note>> = BTreeMap::new();
        for note in batch {
            groups.entry(note.channel.clone()).or_default().push(note);
        }
        for (channel, notes) in groups {
            let item = board::digest(notes.iter().map(|note| board::Brief {
                author: &note.author,
                text: &note.text,
                mentions: usize::from(note.text.contains('@')),
            }));
            let text = board::digest_line(&channel, &item);
            let worker = notes
                .iter()
                .find_map(|note| {
                    if note.worker.is_empty() {
                        None
                    } else {
                        Some(note.worker.as_str())
                    }
                })
                .unwrap_or("");
            self.post_text(&channel, worker, "", &text)?;
        }
        Ok(())
    }

    fn post_text(
        &mut self,
        channel: &str,
        worker: &str,
        thread: &str,
        content: &str,
    ) -> Result<()> {
        if !worker.is_empty() {
            self.ensure_category(worker)?;
        }
        let category = self.ensure_category(channel)?;
        let chan = self.ensure_channel(channel, &category)?;
        let dest = if thread.is_empty() {
            chan
        } else {
            self.ensure_thread(thread, &chan)?
        };
        let body = json!({"content": clip(content, 2000)}).to_string();
        let path = format!("{}/channels/{dest}/messages", self.api.prefix);
        let status = self.http("POST", &path, Some(body.as_bytes()))?.0;
        if status == 429 {
            return Err(err("discord rate"));
        }
        if !(200..300).contains(&status) {
            return Err(err("discord"));
        }
        Ok(())
    }

    fn ensure_category(&mut self, name: &str) -> Result<String> {
        let name = discord_name(name);
        let key = format!("cat:{name}");
        if let Some(id) = self.cache.get(&key) {
            return Ok(id.clone());
        }
        let body = json!({"name": name, "type": 4}).to_string();
        let path = format!("{}/guilds/{}/channels", self.api.prefix, self.guild);
        self.cached(key, "POST", &path, &body)
    }

    fn ensure_channel(&mut self, name: &str, parent: &str) -> Result<String> {
        let name = discord_name(name);
        let key = format!("chan:{parent}:{name}");
        if let Some(id) = self.cache.get(&key) {
            return Ok(id.clone());
        }
        let body = json!({"name": name, "type": 0, "parent_id": parent}).to_string();
        let path = format!("{}/guilds/{}/channels", self.api.prefix, self.guild);
        self.cached(key, "POST", &path, &body)
    }

    fn ensure_thread(&mut self, name: &str, parent: &str) -> Result<String> {
        let name = thread_name(name);
        let key = format!("thread:{parent}:{name}");
        if let Some(id) = self.cache.get(&key) {
            return Ok(id.clone());
        }
        let body = json!({"name": name, "type": 11}).to_string();
        let path = format!("{}/channels/{parent}/threads", self.api.prefix);
        self.cached(key, "POST", &path, &body)
    }

    fn cached(&mut self, key: String, method: &str, path: &str, body: &str) -> Result<String> {
        let (status, raw) = self.http(method, path, Some(body.as_bytes()))?;
        if status == 429 {
            return Err(err("discord rate"));
        }
        if !(200..300).contains(&status) {
            return Err(err("discord"));
        }
        let id = json_id(&raw)?;
        self.cache.insert(key, id.clone());
        Ok(id)
    }

    fn http(&self, method: &str, path: &str, body: Option<&[u8]>) -> Result<(u16, String)> {
        exchange(&self.api, method, path, body, &self.token)
    }
}

fn json_id(body: &str) -> Result<String> {
    let value: Value = serde_json::from_str(body).map_err(|_| err("discord"))?;
    value
        .get("id")
        .and_then(|id| id.as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .ok_or_else(|| err("discord"))
}

fn discord_name(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    if out.is_empty() {
        out = "general".into();
    }
    out.truncate(100);
    out
}

fn thread_name(name: &str) -> String {
    let mut out: String = name.chars().filter(|c| !c.is_control()).take(100).collect();
    if out.is_empty() {
        out = "task".into();
    }
    out
}

fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        text.chars().take(max).collect()
    }
}

fn gateway_loop(url: &str, token: &str, say: Arc<Mutex<impl Write>>) {
    loop {
        if gateway_once(url, token, &say).is_err() {
            thread::sleep(Duration::from_millis(400));
        }
    }
}

fn gateway_once(url: &str, token: &str, say: &Mutex<impl Write>) -> Result<()> {
    let mut sock = ws_connect(url)?;
    let mut seq: Option<i64> = None;
    let mut interval = Duration::from_secs(40);
    let mut last = Instant::now();
    let mut identified = false;
    loop {
        if identified && last.elapsed() >= interval {
            let payload = match seq {
                Some(n) => format!(r#"{{"op":1,"d":{n}}}"#),
                None => r#"{"op":1,"d":null}"#.to_string(),
            };
            sock.write_text(&payload)?;
            last = Instant::now();
        }
        match sock.next_text()? {
            None => continue,
            Some(text) => {
                if let Some(ms) = hello_interval(&text) {
                    interval = Duration::from_millis(ms.max(250));
                    if !identified {
                        sock.write_text(&identify(token))?;
                        identified = true;
                        last = Instant::now();
                    }
                }
                if let Some(n) = seq_of(&text) {
                    seq = Some(n);
                }
                if let Some(content) = inbound_text(&text) {
                    let line = json!({"op":"say","text": content}).to_string();
                    let mut out = say.lock().unwrap_or_else(|poison| poison.into_inner());
                    let _ = writeln!(out, "{line}");
                    let _ = out.flush();
                }
            }
        }
    }
}

fn identify(token: &str) -> String {
    json!({
        "op": 2,
        "d": {
            "token": token,
            "intents": INTENTS,
            "properties": {"os": "linux", "browser": "inlet", "device": "inlet"}
        }
    })
    .to_string()
}

fn hello_interval(text: &str) -> Option<u64> {
    let value: Value = serde_json::from_str(text).ok()?;
    if value.get("op").and_then(|op| op.as_i64()) != Some(10) {
        return None;
    }
    value
        .pointer("/d/heartbeat_interval")
        .and_then(|n| n.as_u64())
}

fn seq_of(text: &str) -> Option<i64> {
    let value: Value = serde_json::from_str(text).ok()?;
    value.get("s").and_then(|n| n.as_i64())
}

fn inbound_text(text: &str) -> Option<String> {
    let value: Value = serde_json::from_str(text).ok()?;
    if value.get("op").and_then(|op| op.as_i64()) != Some(0) {
        return None;
    }
    if value.get("t").and_then(|t| t.as_str()) != Some("MESSAGE_CREATE") {
        return None;
    }
    let data = value.get("d")?;
    if data.pointer("/author/bot").and_then(|bot| bot.as_bool()) == Some(true) {
        return None;
    }
    let content = data
        .get("content")
        .and_then(|content| content.as_str())
        .unwrap_or("")
        .trim();
    if content.is_empty() {
        None
    } else {
        Some(content.to_string())
    }
}

fn ws_connect(raw: &str) -> Result<Sock> {
    let url = parse_ws(raw)?;
    let tcp = TcpStream::connect((url.host.as_str(), url.port)).map_err(|_| err("discord"))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(2)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut stream: Box<dyn Io> = if url.https {
        Box::new(tls_wrap(tcp, &url.host)?)
    } else {
        Box::new(tcp)
    };
    let key = b64(&random_bytes(16)?);
    let host = host_header(url.https, &url.host, url.port);
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n",
        path = url.path
    );
    stream.write_all(req.as_bytes())?;
    stream.flush()?;
    let (header, rest) = read_header(&mut *stream)?;
    if !header.starts_with("HTTP/1.1 101") && !header.starts_with("HTTP/1.0 101") {
        return Err(err("discord"));
    }
    let accept = header_value(&header, "sec-websocket-accept");
    if accept != ws_accept(&key) {
        return Err(err("discord"));
    }
    Ok(Sock { stream, buf: rest })
}

fn tls_wrap(
    tcp: TcpStream,
    host: &str,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name =
        rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|_| err("discord"))?;
    let conn = rustls::ClientConnection::new(std::sync::Arc::new(config), name)
        .map_err(|_| err("discord"))?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

impl Sock {
    fn write_text(&mut self, text: &str) -> Result<()> {
        write_frame(&mut *self.stream, text)
    }

    fn next_text(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(frame) = pop_frame(&mut self.buf)? {
                if frame.op == 0x8 {
                    return Err(err("discord closed"));
                }
                if frame.op == 0x9 {
                    write_pong(&mut *self.stream, &frame.data)?;
                    continue;
                }
                if frame.op == 0x1 && frame.fin {
                    return Ok(Some(String::from_utf8_lossy(&frame.data).into_owned()));
                }
                continue;
            }
            let mut tmp = [0u8; 4096];
            match self.stream.read(&mut tmp) {
                Ok(0) => return Err(err("discord closed")),
                Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::TimedOut
                        || e.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    return Ok(None);
                }
                Err(_) => return Err(err("discord")),
            }
        }
    }
}

struct Frame {
    fin: bool,
    op: u8,
    data: Vec<u8>,
}

fn write_frame(stream: &mut dyn Write, text: &str) -> Result<()> {
    let mask = random_bytes(4)?;
    let payload = text.as_bytes();
    let mut header = Vec::with_capacity(14);
    header.push(0x81);
    let n = payload.len();
    if n < 126 {
        header.push(0x80 | n as u8);
    } else if n <= u16::MAX as usize {
        header.push(0x80 | 126);
        header.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        header.push(0x80 | 127);
        header.extend_from_slice(&(n as u64).to_be_bytes());
    }
    header.extend_from_slice(&mask);
    stream.write_all(&header)?;
    let mut masked = payload.to_vec();
    for (i, byte) in masked.iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
    stream.write_all(&masked)?;
    stream.flush()?;
    Ok(())
}

fn write_pong(stream: &mut dyn Write, payload: &[u8]) -> Result<()> {
    let mask = random_bytes(4)?;
    let mut header = vec![0x8A];
    if payload.len() < 126 {
        header.push(0x80 | payload.len() as u8);
    } else {
        return Ok(());
    }
    header.extend_from_slice(&mask);
    stream.write_all(&header)?;
    let mut masked = payload.to_vec();
    for (i, byte) in masked.iter_mut().enumerate() {
        *byte ^= mask[i % 4];
    }
    stream.write_all(&masked)?;
    stream.flush()?;
    Ok(())
}

fn pop_frame(buf: &mut Vec<u8>) -> Result<Option<Frame>> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let fin = buf[0] & 0x80 != 0;
    let op = buf[0] & 0x0f;
    let masked = buf[1] & 0x80 != 0;
    let mut len = (buf[1] & 0x7f) as usize;
    let mut pos = 2;
    if len == 126 {
        if buf.len() < 4 {
            return Ok(None);
        }
        len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        pos = 4;
    } else if len == 127 {
        if buf.len() < 10 {
            return Ok(None);
        }
        let wide = u64::from_be_bytes(buf[2..10].try_into().unwrap_or([0; 8]));
        if wide > 1024 * 1024 {
            return Err(err("discord"));
        }
        len = wide as usize;
        pos = 10;
    }
    if len > 1024 * 1024 {
        return Err(err("discord"));
    }
    let mask_len = if masked { 4 } else { 0 };
    if buf.len() < pos + mask_len + len {
        return Ok(None);
    }
    let mask = if masked {
        buf[pos..pos + 4].to_vec()
    } else {
        Vec::new()
    };
    pos += mask_len;
    let mut data = buf[pos..pos + len].to_vec();
    if masked {
        for (i, byte) in data.iter_mut().enumerate() {
            *byte ^= mask[i % 4];
        }
    }
    buf.drain(..pos + len);
    Ok(Some(Frame { fin, op, data }))
}

fn read_header(stream: &mut dyn Read) -> Result<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        match stream.read(&mut tmp) {
            Ok(0) => return Err(err("discord")),
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock =>
            {
                if buf.is_empty() {
                    continue;
                }
                return Err(err("discord"));
            }
            Err(_) => return Err(err("discord")),
        }
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&buf[..i]).into_owned();
            let rest = buf[i + 4..].to_vec();
            return Ok((header, rest));
        }
        if buf.len() > 16 * 1024 {
            return Err(err("discord"));
        }
    }
}

fn header_value(header: &str, name: &str) -> String {
    header
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            if key.eq_ignore_ascii_case(name) {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn ws_accept(key: &str) -> String {
    let mut ctx = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
    ctx.update(key.as_bytes());
    ctx.update(GUID.as_bytes());
    b64(ctx.finish().as_ref())
}

fn random_bytes(n: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    SystemRandom::new()
        .fill(&mut buf)
        .map_err(|_| err("discord"))?;
    Ok(buf)
}

fn b64(data: &[u8]) -> String {
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i + 3 <= data.len() {
        let n = ((data[i] as u32) << 16) | ((data[i + 1] as u32) << 8) | data[i + 2] as u32;
        out.push(ALPHA[((n >> 18) & 63) as usize] as char);
        out.push(ALPHA[((n >> 12) & 63) as usize] as char);
        out.push(ALPHA[((n >> 6) & 63) as usize] as char);
        out.push(ALPHA[(n & 63) as usize] as char);
        i += 3;
    }
    if i < data.len() {
        let left = data.len() - i;
        let mut n = (data[i] as u32) << 16;
        if left == 2 {
            n |= (data[i + 1] as u32) << 8;
        }
        out.push(ALPHA[((n >> 18) & 63) as usize] as char);
        out.push(ALPHA[((n >> 12) & 63) as usize] as char);
        if left == 2 {
            out.push(ALPHA[((n >> 6) & 63) as usize] as char);
            out.push('=');
        } else {
            out.push('=');
            out.push('=');
        }
    }
    out
}

fn host_header(https: bool, host: &str, port: u16) -> String {
    let default = if https { 443 } else { 80 };
    if port == default {
        host.to_string()
    } else {
        format!("{host}:{port}")
    }
}

fn parse_ws(raw: &str) -> Result<WsUrl> {
    let (https, rest) = if let Some(rest) = raw.strip_prefix("wss://") {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix("ws://") {
        (false, rest)
    } else {
        return Err(err("discord gateway"));
    };
    split_url(https, rest, true).map(|(host, port, path)| WsUrl {
        https,
        host,
        port,
        path,
    })
}

fn parse_http(raw: &str) -> Result<HttpApi> {
    let (https, rest) = if let Some(rest) = raw.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix("http://") {
        (false, rest)
    } else {
        return Err(err("discord base"));
    };
    split_url(https, rest, false).map(|(host, port, prefix)| HttpApi {
        https,
        host,
        port,
        prefix,
    })
}

fn split_url(https: bool, rest: &str, keep_query: bool) -> Result<(String, u16, String)> {
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() {
        return Err(err("discord"));
    }
    let (host, port) = if let Some((host, port)) = authority.rsplit_once(':') {
        let port = port.parse::<u16>().map_err(|_| err("discord"))?;
        (host.to_string(), port)
    } else {
        (authority.to_string(), if https { 443 } else { 80 })
    };
    if host.is_empty() {
        return Err(err("discord"));
    }
    let path = if keep_query {
        if path.is_empty() {
            "/".to_string()
        } else {
            path
        }
    } else {
        path.split('?')
            .next()
            .unwrap_or("/")
            .trim_end_matches('/')
            .to_string()
    };
    Ok((host, port, path))
}

fn exchange(
    api: &HttpApi,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    token: &str,
) -> Result<(u16, String)> {
    let tcp = TcpStream::connect((api.host.as_str(), api.port)).map_err(|_| err("discord"))?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(Duration::from_secs(5)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut stream: Box<dyn Io> = if api.https {
        Box::new(tls_wrap(tcp, &api.host)?)
    } else {
        Box::new(tcp)
    };
    let host = host_header(api.https, &api.host, api.port);
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: inlet\r\nAuthorization: Bot {token}\r\nConnection: close\r\n"
    );
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
    stream.read_to_end(&mut buf).map_err(|_| err("discord"))?;
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse::<u16>()
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_matches_the_rfc_vector() {
        assert_eq!(
            ws_accept("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn a_masked_frame_roundtrips() {
        let mut buf = Vec::new();
        write_frame(&mut buf, "hi").unwrap();
        let frame = pop_frame(&mut buf).unwrap().unwrap();
        assert!(frame.fin);
        assert_eq!(frame.op, 0x1);
        assert_eq!(frame.data, b"hi");
        assert!(buf.is_empty());
    }

    #[test]
    fn bots_and_humans_stay_off_the_relay() {
        assert!(inbound_text(
            r#"{"op":0,"t":"MESSAGE_CREATE","d":{"content":"bot-secret","author":{"bot":true}}}"#
        )
        .is_none());
        assert_eq!(
            inbound_text(
                r#"{"op":0,"t":"MESSAGE_CREATE","s":1,"d":{"content":" hello ","author":{"bot":false}}}"#
            )
            .as_deref(),
            Some("hello")
        );
        assert!(
            note_of(r#"{"ev":"post","role":"human","author":"you","text":"secret"}"#).is_none()
        );
        let note = note_of(
            r#"{"ev":"post","role":"worker","author":"TASK","text":"yo","channel":"code","worker":"sleeper"}"#,
        )
        .unwrap();
        assert_eq!(note.thread, "TASK");
        assert_eq!(note.channel, "code");
        assert_eq!(note.worker, "sleeper");
        assert!(parse_ws("ws://127.0.0.1:9/gateway").is_ok());
        assert!(parse_http("http://127.0.0.1:9/api/v10").is_ok());
        assert!(parse_ws("http://nope").is_err());
    }
}
