use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use serde_json::Value;

use crate::cli::{self, shell_split};
use crate::error::{err, Result};
use crate::paths;

const RING: usize = 400;

enum Pending {
    Sign,
    Clear(String),
}

pub struct Ui {
    pub live: u64,
    pub queued: u64,
    pub spent: u64,
    pub cap: u64,
    pub debug: u64,
    pub follow: Option<String>,
    pub lines: std::collections::VecDeque<String>,
    pub input: String,
    home: PathBuf,
    secret: Option<Pending>,
}

impl Default for Ui {
    fn default() -> Self {
        Self {
            live: 0,
            queued: 0,
            spent: 0,
            cap: 0,
            debug: 1,
            follow: None,
            lines: std::collections::VecDeque::new(),
            input: String::new(),
            home: PathBuf::new(),
            secret: None,
        }
    }
}

impl Ui {
    pub fn on_line(&mut self, raw: &str) {
        let Ok(value) = serde_json::from_str::<Value>(raw) else {
            return;
        };
        if let Some(posts) = value.get("posts").and_then(|p| p.as_array()) {
            for post in posts {
                self.push_post(post);
            }
        }
        if value.get("live").is_some() {
            self.live = value["live"].as_u64().unwrap_or(self.live);
            self.queued = value["queued"].as_u64().unwrap_or(self.queued);
            self.spent = value["spent"].as_u64().unwrap_or(self.spent);
            self.cap = value["cap"].as_u64().unwrap_or(self.cap);
            self.debug = value["debug"].as_u64().unwrap_or(self.debug);
        }
        match value.get("ev").and_then(|e| e.as_str()) {
            Some("post") => self.push_post(&value),
            Some("exit") => self.push(format!(
                "exit {} {}",
                short(value["id"].as_str().unwrap_or("")),
                value["reason"].as_str().unwrap_or("")
            )),
            Some("deny") => self.push(format!(
                "deny {} {}",
                short(value["id"].as_str().unwrap_or("")),
                value["reason"].as_str().unwrap_or("")
            )),
            Some("bind") => self.push(format!(
                "bind {} {}",
                short(value["id"].as_str().unwrap_or("")),
                value["text"].as_str().unwrap_or("")
            )),
            Some("clear") => self.push(format!(
                "clear {}",
                short(value["id"].as_str().unwrap_or(""))
            )),
            Some("vote") => self.push(format!(
                "vote {} {} {}",
                short(value["voter"].as_str().unwrap_or("")),
                value["choice"].as_str().unwrap_or(""),
                short(value["target"].as_str().unwrap_or(""))
            )),
            Some("moderation") => {
                let action = value["action"].as_str().unwrap_or("");
                let target = value["target"].as_str().unwrap_or("");
                if action == "mute" {
                    let prefix = format!("{target}  ");
                    self.lines.retain(|line| !line.starts_with(&prefix));
                }
                self.push(format!("moderation {action} {}", short(target)));
            }
            Some("admit") | Some("spawn") | Some("kill") | Some("task") | Some("reset") => {
                self.push(format!(
                    "{ev} {id}",
                    ev = value["ev"].as_str().unwrap_or(""),
                    id = short(value["id"].as_str().unwrap_or(""))
                ));
            }
            _ => {}
        }
    }

    fn push_post(&mut self, post: &Value) {
        let author = post["author"].as_str().unwrap_or("?");
        let text = post["text"].as_str().unwrap_or("");
        self.push(format!("{author}  {text}"));
    }

    fn push(&mut self, line: String) {
        if self.lines.len() >= RING {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
    }

    pub fn visible(&self) -> Vec<String> {
        self.lines
            .iter()
            .filter(|line| match &self.follow {
                Some(id) => line.contains(id),
                None => true,
            })
            .cloned()
            .collect()
    }

    /// Returns a line to send, or quit.
    pub fn key(&mut self, code: KeyCode, modifiers: KeyModifiers) -> KeyAction {
        if code == KeyCode::Char('c') && modifiers.contains(KeyModifiers::CONTROL) {
            return KeyAction::Quit;
        }
        match code {
            KeyCode::Esc => {
                self.follow = None;
                KeyAction::None
            }
            KeyCode::Backspace => {
                self.input.pop();
                KeyAction::None
            }
            KeyCode::Enter => {
                let line = std::mem::take(&mut self.input);
                if let Some(pending) = self.secret.take() {
                    self.finish_secret(pending, line)
                } else {
                    self.command(line)
                }
            }
            KeyCode::Char(c) => {
                self.input.push(c);
                KeyAction::None
            }
            _ => KeyAction::None,
        }
    }

    fn command(&mut self, line: String) -> KeyAction {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return KeyAction::None;
        }
        if let Some(rest) = trimmed.strip_prefix('/') {
            let mut parts = shell_split(rest);
            if parts.is_empty() {
                return KeyAction::None;
            }
            let cmd = parts.remove(0);
            match cmd.as_str() {
                "quit" => KeyAction::Quit,
                "debug" => {
                    let level = parts
                        .first()
                        .and_then(|n| n.parse::<u8>().ok())
                        .unwrap_or(1)
                        .min(4);
                    self.debug = u64::from(level);
                    KeyAction::Send(format!(r#"{{"op":"debug","level":{level}}}"#))
                }
                "follow" => {
                    self.follow = parts.first().cloned();
                    KeyAction::None
                }
                "pin" => {
                    let Some(name) = parts.first() else {
                        self.push("pin needs a name".into());
                        return KeyAction::None;
                    };
                    KeyAction::Send(format!(r#"{{"op":"pin","name":"{name}"}}"#))
                }
                "bind" => {
                    let text = parts.join(" ");
                    if text.is_empty() {
                        self.push("bind needs text".into());
                        return KeyAction::None;
                    }
                    KeyAction::Send(serde_json::json!({"op":"bind","text": text}).to_string())
                }
                "sign" => {
                    self.secret = Some(Pending::Sign);
                    self.push("passphrase".into());
                    KeyAction::None
                }
                "clear" => {
                    let Some(id) = parts.first().cloned() else {
                        self.push("clear needs an id".into());
                        return KeyAction::None;
                    };
                    self.secret = Some(Pending::Clear(id));
                    self.push("passphrase".into());
                    KeyAction::None
                }
                "vote" => {
                    let Some(target) = parts.first() else {
                        self.push("vote needs a target and a choice".into());
                        return KeyAction::None;
                    };
                    let Some(choice) = parts.get(1) else {
                        self.push("vote needs a target and a choice".into());
                        return KeyAction::None;
                    };
                    let channel = parts.get(2).map(String::as_str).unwrap_or("general");
                    KeyAction::Send(
                        serde_json::json!({
                            "op": "vote",
                            "target": target,
                            "choice": choice,
                            "channel": channel,
                            "human": true,
                        })
                        .to_string(),
                    )
                }
                "kill" => {
                    let Some(id) = parts.first() else {
                        self.push("kill needs an id".into());
                        return KeyAction::None;
                    };
                    KeyAction::Send(format!(r#"{{"op":"kill","id":"{id}"}}"#))
                }
                "add" => match cli::parse_add_flags(&parts) {
                    Ok(task) => KeyAction::Send(
                        serde_json::json!({
                            "op": "add",
                            "worker": task.worker,
                            "goal": task.goal,
                            "verify": task.verifier,
                            "no_verify": task.no_verify,
                            "tokens": task.tokens,
                            "seconds": task.seconds,
                            "memory_mb": task.memory_mb,
                            "pids": task.pids,
                            "value": task.value,
                            "tags": task.tags,
                            "parent": task.parent,
                            "recipe": task.recipe,
                        })
                        .to_string(),
                    ),
                    Err(e) => {
                        self.push(e.to_string());
                        KeyAction::None
                    }
                },
                other => {
                    self.push(format!("{other} is not in this build"));
                    KeyAction::None
                }
            }
        } else {
            let text = serde_json::json!({"op":"say","text": trimmed});
            KeyAction::Send(text.to_string())
        }
    }

    fn finish_secret(&mut self, pending: Pending, passphrase: String) -> KeyAction {
        let passphrase = passphrase.trim().to_string();
        if passphrase.is_empty() {
            self.push("passphrase stays empty".into());
            return KeyAction::None;
        }
        let wrapped = match std::fs::read(paths::key_priv(&self.home)) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.push("no signing key".into());
                return KeyAction::None;
            }
        };
        let msg = match &pending {
            Pending::Sign => match std::fs::read(paths::policy_draft(&self.home)) {
                Ok(bytes) => bytes,
                Err(_) => {
                    self.push("no policy.draft.lua".into());
                    return KeyAction::None;
                }
            },
            Pending::Clear(id) => format!("clear\n{id}\n").into_bytes(),
        };
        let sig = match crate::sign::sign_with(&wrapped, &passphrase, &msg) {
            Ok(sig) => crate::sign::hex_encode(&sig),
            Err(e) => {
                self.push(e.to_string());
                return KeyAction::None;
            }
        };
        if self.lines.iter().any(|line| line.contains(&passphrase)) {
            self.push("passphrase leaked into the scrollback".into());
        }
        match pending {
            Pending::Sign => {
                KeyAction::Send(serde_json::json!({"op":"sign","sig": sig}).to_string())
            }
            Pending::Clear(id) => {
                KeyAction::Send(serde_json::json!({"op":"clear","id": id, "sig": sig}).to_string())
            }
        }
    }
}

pub enum KeyAction {
    None,
    Quit,
    Send(String),
}

pub fn draw(frame: &mut Frame, ui: &Ui) {
    let chunks = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .split(frame.area());
    let follow = ui
        .follow
        .as_ref()
        .map(|id| format!("  follow {id}"))
        .unwrap_or_default();
    let status = format!(
        " live {}  queue {}  spent {}/{}  debug {}{follow}",
        ui.live, ui.queued, ui.spent, ui.cap, ui.debug
    );
    frame.render_widget(
        Paragraph::new(status).style(Style::default().fg(Color::Cyan)),
        chunks[0],
    );
    let lines: Vec<Line> = ui.visible().into_iter().map(Line::from).collect();
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), chunks[1]);
    let prompt = if ui.secret.is_some() {
        format!("passphrase {}", "*".repeat(ui.input.chars().count()))
    } else {
        format!("> {}", ui.input)
    };
    frame.render_widget(Paragraph::new(prompt), chunks[2]);
}

pub fn attach(home: &Path) -> Result<()> {
    let stream = std::os::unix::net::UnixStream::connect(paths::operator_sock(home))
        .map_err(|_| err("daemon is not up"))?;
    let mut writer = stream.try_clone()?;
    let reader = io::BufReader::new(stream);
    writer.write_all(b"{\"op\":\"hello\",\"debug\":1}\n")?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || reader_loop(reader, tx));
    let mut ui = Ui {
        home: home.to_path_buf(),
        ..Ui::default()
    };
    enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(ratatui::backend::CrosstermBackend::new(io::stdout()))?;
    let result = event_loop(&mut terminal, &mut ui, &rx, &mut writer);
    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;
    result
}

fn reader_loop(mut reader: impl BufRead, tx: mpsc::Sender<String>) {
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                if tx.send(line.trim_end().to_string()).is_err() {
                    break;
                }
            }
        }
    }
}

fn event_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
    ui: &mut Ui,
    rx: &Receiver<String>,
    writer: &mut impl Write,
) -> Result<()> {
    let mut last_status = std::time::Instant::now();
    loop {
        while let Ok(line) = rx.try_recv() {
            ui.on_line(&line);
        }
        terminal.draw(|frame| draw(frame, ui))?;
        if last_status.elapsed() >= Duration::from_secs(1) {
            let _ = writer.write_all(b"{\"op\":\"status\"}\n");
            last_status = std::time::Instant::now();
        }
        if event::poll(Duration::from_millis(40))? {
            let Event::Key(key) = event::read()? else {
                continue;
            };
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match ui.key(key.code, key.modifiers) {
                KeyAction::Quit => return Ok(()),
                KeyAction::None => {}
                KeyAction::Send(line) => {
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
        }
    }
}

fn short(id: &str) -> &str {
    if id.len() > 10 {
        &id[..10]
    } else {
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyModifiers};
    use ratatui::backend::TestBackend;

    #[test]
    fn status_line_and_human_post() {
        let mut ui = Ui::default();
        ui.on_line(
            r#"{"ok":true,"live":2,"queued":4,"spent":10,"cap":100,"debug":1,"posts":[{"author":"you","role":"human","text":"hello @all","channel":"general"}]}"#,
        );
        ui.on_line(r#"{"ev":"post","author":"you","role":"human","text":"again"}"#);
        assert_eq!(ui.live, 2);
        assert_eq!(ui.queued, 4);
        assert!(ui.visible().iter().any(|l| l.starts_with("you  ")));
        let backend = TestBackend::new(80, 12);
        let mut term = Terminal::new(backend).unwrap();
        term.draw(|f| draw(f, &ui)).unwrap();
        let text = term
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect::<String>();
        assert!(text.contains("live 2"), "{text}");
        assert!(text.contains("queue 4"), "{text}");
        assert!(text.contains("spent 10/100"), "{text}");
        assert!(text.contains("you"), "{text}");
    }

    #[test]
    fn slash_is_not_a_post() {
        let mut ui = Ui::default();
        match ui.command("/quit".into()) {
            KeyAction::Quit => {}
            other => panic!(
                "expected quit, got send? {}",
                matches!(other, KeyAction::Send(_))
            ),
        }
        match ui.command("hello @all".into()) {
            KeyAction::Send(line) => {
                assert!(line.contains("\"op\":\"say\""));
                assert!(line.contains("@all"));
            }
            _ => panic!("text should post"),
        }
        match ui.command("/vote abc mute code".into()) {
            KeyAction::Send(line) => {
                assert!(line.contains("\"op\":\"vote\""));
                assert!(line.contains("\"human\":true"));
                assert!(line.contains("mute"));
                assert!(line.contains("code"));
            }
            _ => panic!("vote"),
        }
        match ui.command("/budget x 1".into()) {
            KeyAction::None => {}
            _ => panic!("budget stays off the board"),
        }
        match ui.command("/debug 3".into()) {
            KeyAction::Send(line) => assert!(line.contains("\"level\":3")),
            _ => panic!("debug"),
        }
        match ui.command("/bind stay out #code".into()) {
            KeyAction::Send(line) => {
                assert!(line.contains("\"op\":\"bind\""));
                assert!(line.contains("#code"));
                assert!(!line.contains("passphrase"));
            }
            _ => panic!("bind"),
        }
        assert!(matches!(ui.command("/sign".into()), KeyAction::None));
        ui.input = "keyboard-cat".into();
        match ui.key(KeyCode::Enter, KeyModifiers::NONE) {
            KeyAction::Send(line) => assert!(!line.contains("keyboard-cat"), "{line}"),
            KeyAction::None => {}
            KeyAction::Quit => panic!("quit"),
        }
        assert!(ui.lines.iter().all(|line| !line.contains("keyboard-cat")));
    }
}
