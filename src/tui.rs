use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
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

use crate::board::{self, Digest};
use crate::cli::{self, shell_split};
use crate::error::{err, Result};
use crate::paths;

const RING: usize = 400;

enum Pending {
    Sign,
    Clear(String),
}

struct Hit {
    author: String,
    text: String,
    channel: String,
    thread: String,
    worker: String,
}

struct Slot {
    posts: u32,
    unread: u32,
    mentions: u32,
    authors: BTreeSet<String>,
    last_author: String,
    last_text: String,
}

struct Row {
    group: String,
    channel: String,
    thread: String,
    label: String,
}

pub struct Ui {
    pub live: u64,
    pub queued: u64,
    pub spent: u64,
    pub cap: u64,
    pub debug: u64,
    pub follow: Option<String>,
    pub lines: VecDeque<String>,
    pub input: String,
    home: PathBuf,
    secret: Option<Pending>,
    hits: VecDeque<Hit>,
    slots: BTreeMap<(String, String), Slot>,
    kinds: HashMap<String, String>,
    folded: HashSet<String>,
    show_folded: bool,
    open_group: String,
    open_channel: String,
    open_thread: String,
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
            lines: VecDeque::new(),
            input: String::new(),
            home: PathBuf::new(),
            secret: None,
            hits: VecDeque::new(),
            slots: BTreeMap::new(),
            kinds: HashMap::new(),
            folded: HashSet::new(),
            show_folded: false,
            open_group: "tag:general".into(),
            open_channel: "general".into(),
            open_thread: String::new(),
        }
    }
}

impl Ui {
    pub fn on_line(&mut self, raw: &str) {
        let Ok(value) = serde_json::from_str::<Value>(raw) else {
            return;
        };
        if let Some(tasks) = value.get("tasks").and_then(|t| t.as_array()) {
            for task in tasks {
                if let (Some(id), Some(worker)) = (task["id"].as_str(), task["worker"].as_str()) {
                    self.kinds.insert(id.to_string(), worker.to_string());
                }
            }
        }
        if let Some(items) = value.get("moderation").and_then(|m| m.as_array()) {
            for item in items {
                self.note_fold(item);
            }
        }
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
                self.note_fold(&value);
                let action = value["action"].as_str().unwrap_or("");
                let target = value["target"].as_str().unwrap_or("");
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

    fn note_fold(&mut self, item: &Value) {
        let action = item["action"].as_str().unwrap_or("");
        let target = item["target"].as_str().unwrap_or("");
        if matches!(action, "mute" | "demote") && !target.is_empty() {
            self.folded.insert(target.to_string());
        }
    }

    fn push_post(&mut self, post: &Value) {
        let author = post["author"].as_str().unwrap_or("?").to_string();
        let role = post["role"].as_str().unwrap_or("").to_string();
        let text = post["text"].as_str().unwrap_or("").to_string();
        let channel = post
            .get("channel")
            .and_then(|c| c.as_str())
            .filter(|c| !c.is_empty())
            .unwrap_or("general")
            .to_string();
        let worker = post
            .get("worker")
            .and_then(|w| w.as_str())
            .filter(|w| !w.is_empty())
            .map(str::to_string)
            .or_else(|| self.kinds.get(&author).cloned())
            .unwrap_or_default();
        let thread = board::thread_of(&role, &author);
        let mentions = post
            .get("mentions")
            .and_then(|m| m.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|m| m.as_str())
                    .filter(|m| *m == "all" || *m == "you")
                    .count()
            })
            .unwrap_or_else(|| {
                crate::text::mentions(&text)
                    .into_iter()
                    .filter(|m| m == "all" || m == "you")
                    .count()
            });
        let folded = self.folded.contains(&author);
        self.bump(
            &format!("tag:{channel}"),
            "",
            &author,
            &text,
            mentions,
            folded,
        );
        if !thread.is_empty() {
            self.bump(
                &format!("tag:{channel}"),
                &thread,
                &author,
                &text,
                mentions,
                folded,
            );
        }
        if !worker.is_empty() {
            self.bump(
                &format!("kind:{worker}"),
                "",
                &author,
                &text,
                mentions,
                folded,
            );
            if !thread.is_empty() {
                self.bump(
                    &format!("kind:{worker}"),
                    &thread,
                    &author,
                    &text,
                    mentions,
                    folded,
                );
            }
        }
        if self.hits.len() >= RING {
            self.hits.pop_front();
        }
        self.hits.push_back(Hit {
            author,
            text,
            channel,
            thread,
            worker,
        });
    }

    fn bump(
        &mut self,
        group: &str,
        thread: &str,
        author: &str,
        text: &str,
        mentions: usize,
        folded: bool,
    ) {
        let open = self.open_group == group && self.open_thread == thread;
        let slot = self
            .slots
            .entry((group.to_string(), thread.to_string()))
            .or_insert_with(|| Slot {
                posts: 0,
                unread: 0,
                mentions: 0,
                authors: BTreeSet::new(),
                last_author: String::new(),
                last_text: String::new(),
            });
        slot.posts = slot.posts.saturating_add(1);
        if !folded && !open {
            slot.unread = slot.unread.saturating_add(1);
        }
        if mentions > 0 && !folded {
            slot.mentions = slot.mentions.saturating_add(1);
        }
        slot.authors.insert(author.to_string());
        slot.last_author = author.to_string();
        slot.last_text = text.to_string();
    }

    fn push(&mut self, line: String) {
        if self.lines.len() >= RING {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
    }

    pub fn visible(&self) -> Vec<String> {
        let mut out = self.room_lines();
        if self.open_channel == "general" && self.open_thread.is_empty() {
            out.extend(self.lines.iter().cloned());
        }
        match &self.follow {
            Some(id) => out.into_iter().filter(|line| line.contains(id)).collect(),
            None => out,
        }
    }

    fn room_lines(&self) -> Vec<String> {
        let slot = self
            .slots
            .get(&(self.open_group.clone(), self.open_thread.clone()));
        let posts = slot.map(|s| s.posts).unwrap_or(0) as usize;
        if posts >= board::ROLLUP_AT {
            let item = Digest {
                posts,
                authors: slot.map(|s| s.authors.len()).unwrap_or(0),
                mentions: slot.map(|s| s.mentions).unwrap_or(0) as usize,
                last_author: slot.map(|s| s.last_author.clone()).unwrap_or_default(),
                last_text: slot.map(|s| s.last_text.clone()).unwrap_or_default(),
            };
            let scope = if self.open_thread.is_empty() {
                self.open_channel.clone()
            } else {
                short(&self.open_thread).to_string()
            };
            return vec![board::digest_line(&scope, &item)];
        }
        self.hits
            .iter()
            .filter(|hit| self.hit_open(hit))
            .map(|hit| format!("{}  {}", hit.author, hit.text))
            .collect()
    }

    fn hit_open(&self, hit: &Hit) -> bool {
        if self.folded.contains(&hit.author) && !self.show_folded {
            return false;
        }
        if self.open_group.starts_with("kind:") {
            let kind = self.open_group.trim_start_matches("kind:");
            if hit.worker != kind {
                return false;
            }
        } else if hit.channel != self.open_channel {
            return false;
        }
        if self.open_thread.is_empty() {
            true
        } else {
            hit.thread == self.open_thread
        }
    }

    pub fn sidebar(&self) -> Vec<String> {
        let mut lines = Vec::new();
        let mut folded_here = 0u32;
        let rows = self.rows();
        let mut last_group = String::new();
        for row in &rows {
            if row.group != last_group {
                lines.push(group_label(&row.group));
                last_group = row.group.clone();
            }
            let mark = if row.group == self.open_group
                && row.channel == self.open_channel
                && row.thread == self.open_thread
            {
                ">"
            } else {
                " "
            };
            lines.push(format!("{mark}{}", row.label));
        }
        for (group, thread) in self.slots.keys() {
            if group.starts_with("tag:")
                && *group == self.open_group
                && !thread.is_empty()
                && self.folded.contains(thread)
            {
                folded_here += 1;
            }
        }
        if folded_here > 0 && !self.show_folded {
            lines.push(format!("  {folded_here} folded"));
        }
        if lines.is_empty() {
            lines.push("general".into());
            lines.push("># general".into());
        }
        lines
    }

    fn rows(&self) -> Vec<Row> {
        let mut groups = BTreeSet::new();
        for (group, _) in self.slots.keys() {
            groups.insert(group.clone());
        }
        groups.insert("tag:general".into());
        let mut ordered: Vec<String> = groups.into_iter().collect();
        ordered.sort_by(|a, b| group_rank(a).cmp(&group_rank(b)).then(a.cmp(b)));
        let mut rows = Vec::new();
        for group in ordered {
            let channel = group
                .trim_start_matches("tag:")
                .trim_start_matches("kind:")
                .to_string();
            let slot = self.slots.get(&(group.clone(), String::new()));
            rows.push(Row {
                group: group.clone(),
                channel: channel.clone(),
                thread: String::new(),
                label: format!("# {:<10}{}", channel, counts(slot)),
            });
            if group != self.open_group {
                continue;
            }
            let mut threads: Vec<(&str, &Slot)> = self
                .slots
                .iter()
                .filter(|((g, thread), _)| g == &group && !thread.is_empty())
                .map(|((_, thread), slot)| (thread.as_str(), slot))
                .filter(|(thread, _)| self.show_folded || !self.folded.contains(*thread))
                .collect();
            threads.sort_by(|a, b| b.1.posts.cmp(&a.1.posts).then(a.0.cmp(b.0)));
            for (thread, slot) in threads.into_iter().take(12) {
                rows.push(Row {
                    group: group.clone(),
                    channel: channel.clone(),
                    thread: thread.to_string(),
                    label: format!("  {:<10} {}", short(thread), counts(Some(slot))),
                });
            }
        }
        rows
    }

    fn open_row(&mut self, delta: isize) {
        let rows = self.rows();
        if rows.is_empty() {
            return;
        }
        let cur = rows
            .iter()
            .position(|row| {
                row.group == self.open_group
                    && row.channel == self.open_channel
                    && row.thread == self.open_thread
            })
            .unwrap_or(0);
        let n = rows.len() as isize;
        let next = (cur as isize + delta).rem_euclid(n) as usize;
        self.open_group = rows[next].group.clone();
        self.open_channel = rows[next].channel.clone();
        self.open_thread = rows[next].thread.clone();
        if let Some(slot) = self
            .slots
            .get_mut(&(self.open_group.clone(), self.open_thread.clone()))
        {
            slot.unread = 0;
        }
    }

    /// Returns a line to send, or quit.
    pub fn key(&mut self, code: KeyCode, modifiers: KeyModifiers) -> KeyAction {
        if code == KeyCode::Char('c') && modifiers.contains(KeyModifiers::CONTROL) {
            return KeyAction::Quit;
        }
        if code == KeyCode::Char('n') && modifiers.contains(KeyModifiers::CONTROL) {
            self.open_row(1);
            return KeyAction::None;
        }
        if code == KeyCode::Char('p') && modifiers.contains(KeyModifiers::CONTROL) {
            self.open_row(-1);
            return KeyAction::None;
        }
        if code == KeyCode::Char('f') && modifiers.contains(KeyModifiers::CONTROL) {
            self.show_folded = !self.show_folded;
            return KeyAction::None;
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
        if self.lines.iter().any(|line| line.contains(&passphrase))
            || self.hits.iter().any(|hit| hit.text.contains(&passphrase))
        {
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

fn group_label(group: &str) -> String {
    if let Some(name) = group.strip_prefix("kind:") {
        name.to_string()
    } else if let Some(name) = group.strip_prefix("tag:") {
        name.to_string()
    } else {
        group.to_string()
    }
}

fn group_rank(group: &str) -> u8 {
    if group == "tag:general" {
        0
    } else if group.starts_with("tag:") {
        1
    } else {
        2
    }
}

fn counts(slot: Option<&Slot>) -> String {
    let Some(slot) = slot else {
        return "0".into();
    };
    if slot.mentions > 0 {
        format!("{} @{}", slot.unread, slot.mentions)
    } else {
        format!("{}", slot.unread)
    }
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
        Paragraph::new(status).style(Style::default().fg(Color::DarkGray)),
        chunks[0],
    );
    let body = if chunks[1].width >= 48 {
        let split =
            Layout::horizontal([Constraint::Length(22), Constraint::Min(1)]).split(chunks[1]);
        let side: Vec<Line> = ui.sidebar().into_iter().map(Line::from).collect();
        frame.render_widget(Paragraph::new(side), split[0]);
        split[1]
    } else {
        chunks[1]
    };
    let lines: Vec<Line> = ui.visible().into_iter().map(Line::from).collect();
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), body);
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

    #[test]
    fn sidebar_switches_and_folds() {
        let mut ui = Ui::default();
        ui.on_line(r#"{"ev":"post","author":"w1","role":"worker","text":"secret","channel":"code","worker":"sleeper"}"#);
        ui.on_line(r#"{"ev":"post","author":"you","role":"human","text":"hello @all","channel":"general"}"#);
        let side = ui.sidebar().join("\n");
        assert!(side.contains("general"), "{side}");
        assert!(side.contains("code"), "{side}");
        assert!(side.contains("sleeper"), "{side}");
        assert!(side.contains('@'), "{side}");
        ui.key(KeyCode::Char('n'), KeyModifiers::CONTROL);
        assert_eq!(ui.open_channel, "code");
        assert!(ui.visible().iter().any(|line| line.contains("secret")));
        ui.on_line(r#"{"ev":"moderation","action":"mute","target":"w1"}"#);
        let side = ui.sidebar().join("\n");
        assert!(side.contains("folded"), "{side}");
        assert!(ui.visible().iter().all(|line| !line.contains("secret")));
        ui.key(KeyCode::Char('f'), KeyModifiers::CONTROL);
        assert!(ui.visible().iter().any(|line| line.contains("secret")));
        ui.on_line(r#"{"ev":"moderation","action":"demote","target":"w1"}"#);
        ui.key(KeyCode::Char('f'), KeyModifiers::CONTROL);
        assert!(ui.visible().iter().all(|line| !line.contains("secret")));
    }

    #[test]
    fn thousands_of_posts_stay_a_digest() {
        let mut ui = Ui::default();
        let started = std::time::Instant::now();
        for i in 0..4_000 {
            ui.on_line(&format!(
                r#"{{"ev":"post","author":"w{i}","role":"worker","text":"n","channel":"code","worker":"sleeper"}}"#
            ));
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "sidebar took {:?}",
            started.elapsed()
        );
        ui.key(KeyCode::Char('n'), KeyModifiers::CONTROL);
        let lines = ui.visible();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("4000 posts"), "{}", lines[0]);
        assert!(lines[0].contains("4000 workers"), "{}", lines[0]);
        let side = ui.sidebar();
        assert!(side.iter().any(|line| line.contains("code")), "{side:?}");
        assert!(side.iter().any(|line| line.contains("sleeper")), "{side:?}");
        assert!(side.len() < 40, "{side:?}");
        let backend = TestBackend::new(80, 24);
        let mut term = Terminal::new(backend).unwrap();
        let drawn = std::time::Instant::now();
        term.draw(|frame| draw(frame, &ui)).unwrap();
        assert!(drawn.elapsed() < Duration::from_millis(200));
    }
}
