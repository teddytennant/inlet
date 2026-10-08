use std::fs;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Value};

use crate::config::DEFAULT_POLICY;
use crate::daemon;
use crate::error::{err, Result};
use crate::paths;
use crate::proto::{self, NewTask};
use crate::tui;

pub fn run() -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let home = take_home(&mut args)?;
    match args.first().map(String::as_str) {
        None | Some("attach") => tui::attach(&home),
        Some("up") => {
            let foreground = args.iter().any(|a| a == "-f" || a == "--foreground");
            daemon::serve(&home, foreground)?;
            if !foreground {
                println!("inlet up");
            }
            Ok(())
        }
        Some("init") => init(&home, &args[1..]),
        Some("add") => add(&home, &args[1..]),
        Some("bind") => bind(&home, &args[1..]),
        Some("clear") => clear(&home, args.get(1).map(String::as_str)),
        Some("sign") => sign_policy(&home),
        Some("diff") => diff_policy(&home),
        Some("draft") => draft_policy(&home),
        Some("snap") => snap(&home),
        Some("status") => status(&home, args.iter().any(|a| a == "--json")),
        Some("bridge") => bridge_cmd(&home, &args[1..]),
        Some("post") => {
            let text = args[1..].join(" ");
            if text.trim().is_empty() {
                return Err(err("usage: inlet post <text>"));
            }
            let v = proto::rpc(&home, json!({"op":"post","text": text}))?;
            print_ok(&v)
        }
        Some("vote") => vote(&home, &args[1..]),
        Some("pin") => {
            let name = args.get(1).ok_or_else(|| err("usage: inlet pin <name>"))?;
            let v = proto::rpc(&home, json!({"op":"pin","name": name}))?;
            print_ok(&v)
        }
        Some("kill") => {
            let id = args.get(1).ok_or_else(|| err("usage: inlet kill <id>"))?;
            let v = proto::rpc(&home, json!({"op":"kill","id": id}))?;
            print_ok(&v)
        }
        Some("watch") => watch(&home, &args[1..]),
        Some("-h") | Some("--help") | Some("help") => {
            print!("{HELP}");
            Ok(())
        }
        Some(other) => Err(err(format!("unknown command {other}\n{HELP}"))),
    }
}

const HELP: &str = "\
inlet up [-f]            daemon. -f stays in the foreground
inlet                    attach the TUI
inlet init               write policy.lua and pin a signing key
inlet add -w W -g GOAL (--verify CMD | --no-verify) [--tokens N] [--seconds N]
        [--memory-mb N] [--pids N] [--value N] [-t TAG]... [--parent ID] [--recipe NAME]
        [--seed DIR]
inlet add -f tasks.jsonl
inlet post <text>
inlet vote <target> <choice> [--channel NAME] [--human]
inlet bridge telegram
inlet bind <text>
inlet clear <id>
inlet sign
inlet diff
inlet snap
inlet kill <id>
inlet pin <name>
inlet watch [--debug N] [--worker ID]
inlet status [--json]
";

fn bridge_cmd(home: &Path, args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("telegram") => crate::bridge::telegram(home),
        _ => Err(err("usage: inlet bridge telegram")),
    }
}

fn vote(home: &Path, args: &[String]) -> Result<()> {
    let usage = "usage: inlet vote <target> <choice> [--channel NAME] [--human]";
    let mut target = None;
    let mut choice = None;
    let mut channel = String::from("general");
    let mut human = false;
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--human" {
            human = true;
        } else if arg == "--channel" {
            i += 1;
            channel = args.get(i).cloned().ok_or_else(|| err(usage))?;
        } else if target.is_none() {
            target = Some(arg.clone());
        } else if choice.is_none() {
            choice = Some(arg.clone());
        } else {
            return Err(err(usage));
        }
        i += 1;
    }
    let target = target.ok_or_else(|| err(usage))?;
    let choice = choice.ok_or_else(|| err(usage))?;
    let v = proto::rpc(
        home,
        json!({"op":"vote","target": target, "choice": choice, "channel": channel, "human": human}),
    )?;
    print_ok(&v)
}

fn init(home: &Path, args: &[String]) -> Result<()> {
    if !args.is_empty() {
        return Err(err("passphrase is read from /dev/tty"));
    }
    fs::create_dir_all(home)?;
    fs::create_dir_all(home.join("ledger"))?;
    fs::create_dir_all(home.join("registry"))?;
    fs::create_dir_all(home.join("run"))?;
    fs::create_dir_all(home.join("keys"))?;
    let policy = paths::policy(home);
    if !policy.exists() {
        fs::write(&policy, DEFAULT_POLICY)?;
        println!("wrote {}", policy.display());
    } else {
        println!("{} already exists", policy.display());
    }
    if paths::key_pub(home).exists() {
        println!("signing key already pinned");
        return Ok(());
    }
    let passphrase = crate::sign::read_passphrase("passphrase: ")?;
    let (public, wrapped) = crate::sign::generate(&passphrase)?;
    let body = fs::read(&policy)?;
    let sig = crate::sign::sign_with(&wrapped, &passphrase, &body)?;
    write_private(&paths::key_pub(home), &public)?;
    write_private(&paths::key_priv(home), &wrapped)?;
    write_private(&paths::policy_sig(home), &sig)?;
    println!("pinned signing key");
    Ok(())
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn bind(home: &Path, args: &[String]) -> Result<()> {
    let text = args.join(" ");
    if text.trim().is_empty() {
        return Err(err("usage: inlet bind <text>"));
    }
    let v = proto::rpc(home, json!({"op":"bind","text": text}))?;
    print_ok(&v)
}

fn clear(home: &Path, id: Option<&str>) -> Result<()> {
    let id = id.ok_or_else(|| err("usage: inlet clear <id>"))?;
    let passphrase = crate::sign::read_passphrase("passphrase: ")?;
    let wrapped = fs::read(paths::key_priv(home)).map_err(|_| err("no signing key"))?;
    let msg = format!("clear\n{id}\n");
    let bar = ProgressBar::new_spinner();
    if io::stderr().is_terminal() {
        bar.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
        bar.set_message("clear");
        bar.enable_steady_tick(Duration::from_millis(80));
    }
    let sig = crate::sign::sign_with(&wrapped, &passphrase, msg.as_bytes())?;
    bar.finish_and_clear();
    let v = proto::rpc(
        home,
        json!({"op":"clear","id": id, "sig": crate::sign::hex_encode(&sig)}),
    )?;
    print_ok(&v)
}

fn sign_policy(home: &Path) -> Result<()> {
    let draft = fs::read(paths::policy_draft(home)).map_err(|_| err("no policy.draft.lua"))?;
    let passphrase = crate::sign::read_passphrase("passphrase: ")?;
    let wrapped = fs::read(paths::key_priv(home)).map_err(|_| err("no signing key"))?;
    let bar = ProgressBar::new_spinner();
    if io::stderr().is_terminal() {
        bar.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
        bar.set_message("signing");
        bar.enable_steady_tick(Duration::from_millis(80));
    }
    let sig = crate::sign::sign_with(&wrapped, &passphrase, &draft)?;
    bar.finish_and_clear();
    let v = proto::rpc(
        home,
        json!({"op":"sign","sig": crate::sign::hex_encode(&sig)}),
    )?;
    print_ok(&v)
}

fn snap(home: &Path) -> Result<()> {
    let bar = ProgressBar::new_spinner();
    if io::stderr().is_terminal() {
        bar.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
        bar.set_message("snapshot");
        bar.enable_steady_tick(Duration::from_millis(80));
    }
    let done = match proto::rpc(home, json!({"op":"snap"})) {
        Ok(v) => v,
        Err(e) if e.to_string() == "daemon is not up" => {
            let (offset, sha) = crate::snap::offline(home)?;
            bar.finish_and_clear();
            println!("snap {offset} {sha}");
            return Ok(());
        }
        Err(e) => {
            bar.finish_and_clear();
            return Err(e);
        }
    };
    bar.finish_and_clear();
    if done.get("ok").and_then(|b| b.as_bool()) != Some(true) {
        return Err(err(done
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("snap failed")));
    }
    let offset = done.get("offset").and_then(|n| n.as_u64()).unwrap_or(0);
    let sha = done.get("commit").and_then(|s| s.as_str()).unwrap_or("");
    println!("snap {offset} {sha}");
    Ok(())
}

fn diff_policy(home: &Path) -> Result<()> {
    let v = proto::rpc(home, json!({"op":"diff"}))?;
    if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
        return Err(err(v
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("diff failed")));
    }
    let loaded = v.get("loaded").and_then(|s| s.as_str()).unwrap_or("");
    let draft = v.get("draft").and_then(|s| s.as_str()).unwrap_or("");
    print_diff(loaded, draft);
    Ok(())
}

fn print_diff(loaded: &str, draft: &str) {
    if draft.is_empty() {
        println!("no policy.draft.lua");
        return;
    }
    let old: Vec<&str> = loaded.lines().collect();
    let new: Vec<&str> = draft.lines().collect();
    let mut changed = false;
    let n = old.len().max(new.len());
    for i in 0..n {
        match (old.get(i), new.get(i)) {
            (Some(a), Some(b)) if a == b => {}
            (Some(a), Some(b)) => {
                changed = true;
                println!("-{a}");
                println!("+{b}");
            }
            (Some(a), None) => {
                changed = true;
                println!("-{a}");
            }
            (None, Some(b)) => {
                changed = true;
                println!("+{b}");
            }
            (None, None) => {}
        }
    }
    if !changed {
        println!("policy.draft.lua matches the loaded snapshot");
    }
}

fn draft_policy(home: &Path) -> Result<()> {
    let mut text = String::new();
    io::stdin().read_to_string(&mut text)?;
    let v = proto::rpc(home, json!({"op":"draft","text": text}))?;
    print_ok(&v)
}

fn add(home: &Path, args: &[String]) -> Result<()> {
    if let Some(path) = flag(args, "-f").or_else(|| flag(args, "--file")) {
        let tasks = read_jsonl(Path::new(&path))?;
        let bar = ProgressBar::new(tasks.len() as u64);
        bar.set_style(
            ProgressStyle::with_template("intake {bar:28.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
        let body = json!({
            "op": "add_batch",
            "tasks": tasks.iter().map(proto::task_value).collect::<Vec<_>>(),
        });
        let v = proto::rpc(home, body)?;
        if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
            bar.abandon();
            return Err(err(v
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("add failed")));
        }
        bar.set_position(tasks.len() as u64);
        let n = v
            .get("ids")
            .and_then(|i| i.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        bar.finish_with_message(format!("{n} queued"));
        return Ok(());
    }
    let task = parse_add_flags(args)?;
    let mut body = proto::task_value(&task);
    body["op"] = json!("add");
    let v = proto::rpc(home, body)?;
    if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
        return Err(err(v
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("add failed")));
    }
    if let Some(id) = v
        .get("ids")
        .and_then(|i| i.as_array())
        .and_then(|a| a.first())
        .and_then(|s| s.as_str())
    {
        println!("queued {id}");
    }
    Ok(())
}

fn read_jsonl(path: &Path) -> Result<Vec<NewTask>> {
    let file = fs::File::open(path)?;
    let mut tasks = Vec::new();
    for (n, line) in io::BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .map_err(|e| err(format!("{}:{}: {e}", path.display(), n + 1)))?;
        tasks.push(proto::parse_task(&value)?);
    }
    if tasks.is_empty() {
        return Err(err("no tasks in file"));
    }
    Ok(tasks)
}

pub fn parse_add_flags(args: &[String]) -> Result<NewTask> {
    let mut worker = None;
    let mut goal = None;
    let mut verify = None;
    let mut no_verify = false;
    let mut tokens = None;
    let mut seconds = None;
    let mut memory_mb = None;
    let mut pids = None;
    let mut value = None;
    let mut tags = Vec::new();
    let mut parent = None;
    let mut recipe = None;
    let mut seed = None;
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let next = || {
            args.get(i + 1)
                .cloned()
                .ok_or_else(|| err(format!("{arg} needs a value")))
        };
        match arg.as_str() {
            "-w" | "--worker" => {
                worker = Some(next()?);
                i += 2;
            }
            "-g" | "--goal" => {
                goal = Some(next()?);
                i += 2;
            }
            "--verify" => {
                verify = Some(next()?);
                i += 2;
            }
            "--no-verify" => {
                no_verify = true;
                i += 1;
            }
            "--tokens" => {
                tokens = Some(parse_u64(&next()?)?);
                i += 2;
            }
            "--seconds" => {
                seconds = Some(parse_u64(&next()?)?);
                i += 2;
            }
            "--memory-mb" => {
                memory_mb = Some(parse_u64(&next()?)?);
                i += 2;
            }
            "--pids" => {
                pids = Some(parse_u64(&next()?)?);
                i += 2;
            }
            "--value" => {
                value = Some(parse_u64(&next()?)?);
                i += 2;
            }
            "-t" | "--tag" => {
                tags.push(next()?);
                i += 2;
            }
            "--parent" => {
                parent = Some(next()?);
                i += 2;
            }
            "--recipe" => {
                recipe = Some(next()?);
                i += 2;
            }
            "--seed" => {
                seed = Some(next()?);
                i += 2;
            }
            other if other.starts_with('-') => return Err(err(format!("unknown flag {other}"))),
            other => {
                if goal.is_none() {
                    goal = Some(other.to_string());
                    i += 1;
                } else {
                    return Err(err(format!("unexpected {other}")));
                }
            }
        }
    }
    let worker = worker.ok_or_else(|| err("add needs --worker"))?;
    let goal = goal.ok_or_else(|| err("add needs --goal"))?;
    if verify.is_some() && no_verify {
        return Err(err("pass either --verify or --no-verify"));
    }
    if verify.is_none() && !no_verify {
        return Err(err("pass --verify or --no-verify"));
    }
    Ok(NewTask {
        worker,
        goal,
        verifier: verify,
        no_verify,
        tokens,
        seconds,
        memory_mb,
        pids,
        value,
        tags,
        parent,
        recipe,
        seed,
    })
}

fn status(home: &Path, json_out: bool) -> Result<()> {
    let v = proto::rpc(home, json!({"op":"status"}))?;
    if json_out {
        println!("{v}");
        return Ok(());
    }
    let cap = v.get("cap").and_then(|n| n.as_u64()).unwrap_or(1).max(1);
    let spent = v.get("spent").and_then(|n| n.as_u64()).unwrap_or(0);
    let bar = ProgressBar::new(cap);
    bar.set_style(
        ProgressStyle::with_template("{prefix} {bar:28.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    bar.set_prefix(format!(
        "live {}  queue {}  debug {}",
        v["live"].as_u64().unwrap_or(0),
        v["queued"].as_u64().unwrap_or(0),
        v["debug"].as_u64().unwrap_or(0),
    ));
    bar.set_position(spent.min(cap));
    bar.set_message(format!(
        "held {}  free {}",
        v["held"].as_u64().unwrap_or(0),
        v["available"].as_u64().unwrap_or(0),
    ));
    bar.abandon();
    if v.get("gate").and_then(|k| k.as_str()).unwrap_or("off") != "off" {
        let cap = v.get("gate_cap").and_then(|n| n.as_u64()).unwrap_or(0);
        if cap > 0 {
            let spent = v.get("gate_spent").and_then(|n| n.as_u64()).unwrap_or(0);
            let gate = ProgressBar::new(cap);
            gate.set_style(
                ProgressStyle::with_template("{prefix} {bar:28.cyan/blue} {pos}/{len}")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            gate.set_prefix("gate");
            gate.set_position(spent.min(cap));
            gate.abandon();
        }
    }
    if let Some(tasks) = v.get("tasks").and_then(|t| t.as_array()) {
        for task in tasks {
            println!(
                "{}  {:<8} {}  {}",
                task["id"].as_str().unwrap_or(""),
                task["state"].as_str().unwrap_or(""),
                task["worker"].as_str().unwrap_or(""),
                task["goal"].as_str().unwrap_or(""),
            );
        }
    }
    Ok(())
}

fn watch(home: &Path, args: &[String]) -> Result<()> {
    let debug = flag(args, "--debug").unwrap_or_else(|| "1".into());
    let worker = flag(args, "--worker");
    let mut stream = std::os::unix::net::UnixStream::connect(paths::operator_sock(home))
        .map_err(|_| err("daemon is not up"))?;
    let body = json!({"op":"watch","debug": debug.parse::<u8>().unwrap_or(1), "worker": worker});
    let mut line = serde_json::to_string(&body)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    let tty = io::stderr().is_terminal();
    let bar = ProgressBar::new_spinner();
    if tty {
        bar.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
        bar.enable_steady_tick(Duration::from_millis(80));
    }
    let mut reader = io::BufReader::new(stream);
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        let value: Value = match serde_json::from_str(buf.trim_end()) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if value.get("ev").is_none() {
            if tty {
                bar.set_message(format!(
                    "live {}  queue {}  spent {}/{}",
                    value["live"].as_u64().unwrap_or(0),
                    value["queued"].as_u64().unwrap_or(0),
                    value["spent"].as_u64().unwrap_or(0),
                    value["cap"].as_u64().unwrap_or(0),
                ));
            }
            continue;
        }
        let rendered = render_event(&value);
        if tty {
            bar.println(rendered);
        } else {
            println!("{rendered}");
            let _ = io::stdout().flush();
        }
        if value.get("ev").and_then(|e| e.as_str()) == Some("reset") && tty {
            bar.set_message(format!(
                "reset  free {}  held {}",
                value["available"].as_u64().unwrap_or(0),
                value["held"].as_u64().unwrap_or(0),
            ));
        }
    }
    if tty {
        bar.finish_and_clear();
    }
    Ok(())
}

fn render_event(v: &Value) -> String {
    let ev = v.get("ev").and_then(|e| e.as_str()).unwrap_or("?");
    match ev {
        "post" => format!(
            "{}  {}",
            v["author"].as_str().unwrap_or("?"),
            v["text"].as_str().unwrap_or("")
        ),
        "admit" => format!(
            "admit {}  tokens {}",
            v["id"].as_str().unwrap_or(""),
            v["tokens"].as_u64().unwrap_or(0)
        ),
        "deny" => format!(
            "deny {}  {}",
            v["id"].as_str().unwrap_or(""),
            v["reason"].as_str().unwrap_or("")
        ),
        "spawn" => format!(
            "spawn {}  pid {}",
            v["id"].as_str().unwrap_or(""),
            v["pid"].as_u64().unwrap_or(0)
        ),
        "exit" => format!(
            "exit {}  {} {}",
            v["id"].as_str().unwrap_or(""),
            v["code"].as_i64().unwrap_or(0),
            v["reason"].as_str().unwrap_or("")
        ),
        "kill" => format!("kill {}", v["id"].as_str().unwrap_or("")),
        "cost" => format!(
            "cost {}  {}",
            v["id"].as_str().unwrap_or(""),
            v["tokens"].as_u64().unwrap_or(0)
        ),
        "reset" => "purse reset".into(),
        "task" => format!(
            "task {}  {}",
            v["id"].as_str().unwrap_or(""),
            v["worker"].as_str().unwrap_or("")
        ),
        _ => v.to_string(),
    }
}

fn print_ok(v: &Value) -> Result<()> {
    if v.get("ok").and_then(|b| b.as_bool()) == Some(false) {
        return Err(err(v
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("failed")));
    }
    if let Some(id) = v.get("id").and_then(|s| s.as_str()) {
        println!("{id}");
    }
    Ok(())
}

fn take_home(args: &mut Vec<String>) -> Result<PathBuf> {
    let mut home = std::env::var("INLET_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--home" {
            let value = args
                .get(i + 1)
                .cloned()
                .ok_or_else(|| err("--home needs a directory"))?;
            home = PathBuf::from(value);
            args.drain(i..i + 2);
        } else {
            i += 1;
        }
    }
    Ok(home)
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn parse_u64(raw: &str) -> Result<u64> {
    raw.parse::<u64>()
        .map_err(|_| err(format!("not a number: {raw}")))
}

pub fn shell_split(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote = false;
    for ch in line.chars() {
        match ch {
            '"' => quote = !quote,
            ' ' | '\t' if !quote => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[allow(dead_code)]
fn _sleep_import() {
    thread::sleep(Duration::from_millis(0));
}
