use std::fs;
use std::io::{self, BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Value};

use crate::daemon;
use crate::error::{err, Result};
use crate::paths;
use crate::proto::{self, NewTask};
use crate::tui;

pub fn run() -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let home = take_home(&mut args)?;
    if matches!(
        args.first().map(String::as_str),
        Some("status" | "get" | "describe" | "logs" | "door")
    ) {
        // These print and exit. `inlet get | head` should end quietly, not panic.
        unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
    }
    match args.first().map(String::as_str) {
        None | Some("attach") => {
            if !paths::policy(&home).exists() {
                crate::setup::init(&home, &[])?;
                if !io::stdout().is_terminal() {
                    return Ok(());
                }
            }
            tui::attach(&home)
        }
        Some("up") => {
            let foreground = args.iter().any(|a| a == "-f" || a == "--foreground");
            let standby = args.iter().any(|a| a == "--standby");
            let replica = flag(&args, "--replica");
            if (standby || replica.is_some()) && !foreground {
                return Err(err(
                    "--standby and --replica run in the foreground: inlet up -f --standby",
                ));
            }
            if foreground {
                daemon::serve(&home, true, standby, replica)?;
            } else {
                {
                    let _hold = crate::spin::Hold::start(&crate::spin::UP)?;
                    daemon::serve(&home, false, false, None)?;
                }
                println!("inlet up");
            }
            Ok(())
        }
        Some("replica") => match args.get(1).map(String::as_str) {
            Some("serve") => crate::replica::serve(&home),
            Some("vote") => crate::replica::vote(&home),
            _ => Err(err(
                "usage: inlet replica serve|vote (run by other replicas)",
            )),
        },
        Some("doctor") => doctor(&home),
        Some("down") => down(&home, args.iter().any(|a| a == "--keep")),
        Some("init") => crate::setup::init(&home, &args[1..]),
        Some("settings") | Some("--settings") => crate::setup::settings(&home, &args[1..]),
        Some("add") => add(&home, &args[1..]),
        Some("apply") => apply(&home, &args[1..]),
        Some("get") => get(&home, &args[1..]),
        Some("describe") => describe(&home, args.get(1).map(String::as_str)),
        Some("logs") => logs(&home, &args[1..]),
        Some("scale") => scale(&home, &args[1..]),
        Some("delete") => delete(&home, &args[1..]),
        Some("door") => door(&args[1..]),
        Some("bind") => bind(&home, &args[1..]),
        Some("clear") => clear(&home, args.get(1).map(String::as_str)),
        Some("sign") => sign_policy(&home),
        Some("diff") => diff_policy(&home),
        Some("draft") => draft_policy(&home),
        Some("snap") => snap(&home),
        Some("status") => status(
            &home,
            args.iter().any(|a| a == "--json"),
            flag(&args, "--leader"),
        ),
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
        Some("digest") => {
            let channel = args.get(1).map(String::as_str).unwrap_or("general");
            digest(&home, channel)
        }
        Some("pin") => {
            let name = args.get(1).ok_or_else(|| err("usage: inlet pin <name>"))?;
            let v = {
                let _hold = crate::spin::Hold::start(&crate::spin::PIN)?;
                proto::rpc(&home, json!({"op":"pin","name": name}))?
            };
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
inlet up -f --standby    wait for the leader on this home to go, then take over
inlet doctor             check the policy, the cell, the upstream, the disk, and what a crash left
inlet down [--keep]      stop the daemon and its workers. --keep leaves the workers for the next daemon
inlet                    attach the tui
inlet init [--defaults] [--no-smoke]
inlet settings           view or change setup
inlet --settings
inlet add -w W -g GOAL (--verify CMD | --no-verify) [--tokens N] [--seconds N]
        [--memory-mb N] [--pids N] [--value N] [-t TAG]... [--parent ID] [--recipe NAME]
        [--seed DIR] [--retries N]
inlet add -f tasks.jsonl
inlet apply -f FILE      swarms and tasks from json. - is stdin
inlet get [tasks|swarms|nodes] [--json]
inlet describe <id>      one task, its attempts, and what it printed
inlet logs <id> [--verify]
inlet scale <swarm> <replicas>
inlet delete <swarm> [--kill]
inlet door <op>          inside a cell: post, board, spawn, wait, children, draft, vote
inlet post <text>
inlet vote <target> <choice> [--channel NAME] [--human]
inlet bridge telegram
inlet bridge discord
inlet digest [channel]
inlet bind [--for 2h] <text>
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
        Some("discord") => crate::discord::run(home),
        _ => Err(err("usage: inlet bridge telegram|discord")),
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
    let v = {
        let _hold = crate::spin::Hold::start(&crate::spin::VOTE)?;
        proto::rpc(
            home,
            json!({"op":"vote","target": target, "choice": choice, "channel": channel, "human": human}),
        )?
    };
    print_ok(&v)
}

fn bind(home: &Path, args: &[String]) -> Result<()> {
    let usage = "usage: inlet bind [--for 2h] <text>";
    let mut ttl_ms = 0;
    let mut words = args;
    if words.first().map(String::as_str) == Some("--for") {
        ttl_ms = crate::config::parse_span(words.get(1).ok_or_else(|| err(usage))?)?;
        words = &words[2..];
    }
    let text = words.join(" ");
    if text.trim().is_empty() {
        return Err(err(usage));
    }
    let v = proto::rpc(home, json!({"op":"bind","text": text, "ttl_ms": ttl_ms}))?;
    print_ok(&v)
}

fn clear(home: &Path, id: Option<&str>) -> Result<()> {
    let id = id.ok_or_else(|| err("usage: inlet clear <id>"))?;
    if !paths::key_priv(home).exists() {
        // No key was ever made, so there is nothing to sign with.
        let v = proto::rpc(home, json!({"op":"clear","id": id}))?;
        return print_ok(&v);
    }
    let passphrase = crate::sign::read_passphrase("passphrase: ")?;
    let wrapped = fs::read(paths::key_priv(home)).map_err(|_| err("no signing key"))?;
    let msg = format!("clear\n{id}\n");
    let bar = ProgressBar::new_spinner();
    if io::stderr().is_terminal() {
        bar.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
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
        bar.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
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
        bar.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
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

fn digest(home: &Path, channel: &str) -> Result<()> {
    let state = digest_spin(home, channel);
    let v = {
        let _hold = crate::spin::Hold::start(state)?;
        proto::rpc(home, json!({"op":"digest","channel": channel}))?
    };
    if v.get("ok").and_then(|b| b.as_bool()) == Some(false) {
        return Err(err(v
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("failed")));
    }
    println!("{}", v.get("text").and_then(|t| t.as_str()).unwrap_or(""));
    Ok(())
}

fn digest_spin(home: &Path, channel: &str) -> &'static crate::spin::State {
    let Ok(policy) = crate::config::Policy::load(&paths::policy(home)) else {
        return &crate::spin::ROLLUP;
    };
    if policy.cfg.rollup != crate::config::Rollup::Model {
        return &crate::spin::DIGEST;
    }
    if policy.cfg.decision.endpoint.is_none() {
        return &crate::spin::DIGEST;
    }
    let Ok(status) = proto::rpc(home, json!({"op":"status"})) else {
        return &crate::spin::ROLLUP;
    };
    let spent = status
        .get("gate_spent")
        .and_then(|n| n.as_u64())
        .unwrap_or(0);
    let cap = status.get("gate_cap").and_then(|n| n.as_u64()).unwrap_or(0);
    let posts = status.get("posts").and_then(|p| p.as_array());
    let seen = status.get("posts_seen").and_then(|n| n.as_u64());
    let count = match (posts, seen) {
        (Some(posts), Some(seen)) if seen == posts.len() as u64 => Some(
            posts
                .iter()
                .filter(|post| post.get("channel").and_then(|c| c.as_str()) == Some(channel))
                .count(),
        ),
        _ => None,
    };
    crate::spin::model_digest_state(spent, cap, count)
}

fn add(home: &Path, args: &[String]) -> Result<()> {
    if let Some(path) = flag(args, "-f").or_else(|| flag(args, "--file")) {
        let tasks = read_jsonl(Path::new(&path))?;
        let body = json!({
            "op": "add_batch",
            "tasks": tasks.iter().map(proto::task_value).collect::<Vec<_>>(),
        });
        let v = {
            let _hold = crate::spin::Hold::start(&crate::spin::INTAKE)?;
            proto::rpc(home, body)?
        };
        if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
            return Err(err(v
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("add failed")));
        }
        let n = v
            .get("ids")
            .and_then(|i| i.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        println!("{n} queued");
        return Ok(());
    }
    let task = parse_add_flags(args)?;
    let mut body = proto::task_value(&task);
    body["op"] = json!("add");
    let v = {
        let _hold = crate::spin::Hold::start(&crate::spin::ADD)?;
        proto::rpc(home, body)?
    };
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
    // `-` is stdin, which is how a leader hands a task to a node.
    let input: Box<dyn io::Read> = if path == Path::new("-") {
        Box::new(io::stdin())
    } else {
        Box::new(fs::File::open(path)?)
    };
    let mut tasks = Vec::new();
    for (n, line) in io::BufReader::new(input).lines().enumerate() {
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
    let mut retries = None;
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
            "--retries" => {
                retries = Some(parse_u64(&next()?)?.min(64) as u32);
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
        retries,
    })
}

/// What an operator checks before trusting a box with work, in one place.
/// Exit status is non-zero when a check that matters fails.
fn doctor(home: &Path) -> Result<()> {
    let mut bad = 0;
    let mut line = |ok: bool, what: &str, detail: String| {
        if !ok {
            bad += 1;
        }
        println!("{}  {what:<10} {detail}", if ok { "ok  " } else { "FAIL" });
    };
    let policy = crate::config::Policy::load(&paths::policy(home));
    match &policy {
        Ok(p) => line(
            !p.cfg.workers.is_empty(),
            "policy",
            format!(
                "{} workers, max_live {}, adopt {}",
                p.cfg.workers.len(),
                p.cfg.caps.max_live,
                p.cfg.adopt
            ),
        ),
        Err(e) => line(false, "policy", e.to_string()),
    }
    line(
        crate::cell::probe(home),
        "cell",
        "user, mount, and pid namespaces with Landlock".into(),
    );
    if let Ok(p) = &policy {
        match &p.cfg.proxy.upstream {
            None => line(
                true,
                "upstream",
                "none set; workers cannot call a model".into(),
            ),
            Some(url) => {
                let rest = url.split("://").nth(1).unwrap_or(url);
                let host = rest.split('/').next().unwrap_or(rest);
                let addr = if host.contains(':') {
                    host.to_string()
                } else {
                    format!("{host}:{}", if url.starts_with("https") { 443 } else { 80 })
                };
                use std::net::ToSocketAddrs;
                let reached = addr
                    .to_socket_addrs()
                    .ok()
                    .and_then(|mut a| a.next())
                    .is_some_and(|a| {
                        std::net::TcpStream::connect_timeout(&a, Duration::from_secs(5)).is_ok()
                    });
                line(
                    reached,
                    "upstream",
                    format!(
                        "{addr} {}",
                        if reached {
                            "answers"
                        } else {
                            "does not answer"
                        }
                    ),
                );
            }
        }
    }
    let free = crate::daemon::free_bytes(home).unwrap_or(0);
    line(
        free >= 1024 * 1024 * 1024,
        "disk",
        format!("{} MB free under the home", free / (1024 * 1024)),
    );
    let ledger = fs::metadata(paths::ledger(home))
        .map(|m| m.len())
        .unwrap_or(0);
    match proto::rpc(home, json!({"op":"status","brief": true})) {
        Ok(v) => {
            let stalled = v["swarms"]
                .as_array()
                .map(|all| all.iter().filter(|s| s["phase"] == "stalled").count())
                .unwrap_or(0);
            line(
                true,
                "daemon",
                format!(
                    "up, {} live, {} queued, {} of {} tokens free, ledger {} MB, checkpoint at {} MB",
                    v["live"],
                    v["queued"],
                    v["available"],
                    v["cap"],
                    ledger / (1024 * 1024),
                    v["snap_offset"].as_u64().unwrap_or(0) / (1024 * 1024),
                ),
            );
            line(stalled == 0, "swarms", format!("{stalled} stalled"));
        }
        Err(_) => {
            // Not an error by itself. What matters is whether work is waiting on it.
            let kept = fs::read_dir(home.join("run/live"))
                .map(|d| d.count())
                .unwrap_or(0);
            line(
                kept == 0,
                "daemon",
                format!("down, {kept} workers waiting to be taken back"),
            );
        }
    }
    if bad > 0 {
        return Err(err(format!("{bad} checks failed")));
    }
    Ok(())
}

/// Ctrl-C from anywhere. `--keep` is the other signal: the daemon goes, the
/// work stays, and the next `inlet up` takes it back.
fn down(home: &Path, keep: bool) -> Result<()> {
    let pid: i32 = fs::read_to_string(paths::pid_file(home))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .ok_or_else(|| err("daemon is not up"))?;
    // The pid file outlives a daemon that was killed, and the number in it
    // may be someone else's by now. Only a daemon that answers gets a signal.
    if proto::rpc(home, json!({"op":"status","brief": true})).is_err() {
        return Err(err("daemon is not up"));
    }
    let sig = if keep { libc::SIGTERM } else { libc::SIGINT };
    if unsafe { libc::kill(pid, sig) } != 0 {
        return Err(err("daemon is not up"));
    }
    for _ in 0..600 {
        if unsafe { libc::kill(pid, 0) } != 0 {
            println!("{}", if keep { "down, workers kept" } else { "down" });
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(err("daemon is still stopping"))
}

fn checked(v: Value, what: &str) -> Result<Value> {
    if v.get("ok").and_then(|b| b.as_bool()) != Some(true) {
        let why = v.get("error").and_then(|e| e.as_str()).unwrap_or("failed");
        return Err(err(format!("{what}: {why}")));
    }
    Ok(v)
}

/// One json value, an array of them, or one per line. `kind` picks swarm or task.
fn read_manifests(path: &str) -> Result<Vec<Value>> {
    let mut text = String::new();
    if path == "-" {
        io::stdin().read_to_string(&mut text)?;
    } else {
        text = fs::read_to_string(path).map_err(|e| err(format!("{path}: {e}")))?;
    }
    let docs = match serde_json::from_str::<Value>(&text) {
        Ok(Value::Array(items)) => items,
        Ok(one) => vec![one],
        Err(_) => {
            let mut docs = Vec::new();
            for (n, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                docs.push(
                    serde_json::from_str(line)
                        .map_err(|e| err(format!("{path}:{}: {e}", n + 1)))?,
                );
            }
            docs
        }
    };
    if docs.is_empty() {
        return Err(err("nothing to apply"));
    }
    Ok(docs)
}

fn apply(home: &Path, args: &[String]) -> Result<()> {
    let path = flag(args, "-f")
        .or_else(|| flag(args, "--file"))
        .ok_or_else(|| err("usage: inlet apply -f FILE"))?;
    let mut tasks = Vec::new();
    let mut swarms = Vec::new();
    for doc in read_manifests(&path)? {
        match doc.get("kind").and_then(|k| k.as_str()).unwrap_or("task") {
            "swarm" => {
                // Refuse a bad manifest before any of the file takes effect.
                proto::parse_swarm(&doc)?;
                swarms.push(doc);
            }
            "task" => tasks.push(proto::task_value(&proto::parse_task(&doc)?)),
            other => return Err(err(format!("unknown kind {other}"))),
        }
    }
    for mut doc in swarms {
        doc["op"] = json!("apply");
        let v = checked(proto::rpc(home, doc)?, "apply")?;
        println!(
            "swarm {} generation {}",
            v["name"].as_str().unwrap_or(""),
            v["generation"].as_u64().unwrap_or(0)
        );
    }
    if !tasks.is_empty() {
        let v = checked(
            proto::rpc(home, json!({"op": "add_batch", "tasks": tasks}))?,
            "apply",
        )?;
        let n = v["ids"].as_array().map(|a| a.len()).unwrap_or(0);
        println!("{n} queued");
    }
    Ok(())
}

fn get(home: &Path, args: &[String]) -> Result<()> {
    let what = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .map(String::as_str)
        .unwrap_or("tasks");
    let key = match what {
        "tasks" | "task" => "tasks",
        "swarms" | "swarm" => "swarms",
        "nodes" | "node" => "nodes",
        "replicas" | "replica" => "replicas",
        _ => {
            return Err(err(
                "usage: inlet get [tasks|swarms|nodes|replicas] [--json]",
            ))
        }
    };
    let v = checked(
        proto::rpc(home, json!({"op":"status","brief": key != "tasks"}))?,
        "get",
    )?;
    let rows = v
        .get(key)
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();
    if args.iter().any(|a| a == "--json") {
        println!("{}", Value::Array(rows));
        return Ok(());
    }
    let text = |row: &Value, field: &str| row[field].as_str().unwrap_or("").to_string();
    let num = |row: &Value, field: &str| row[field].as_u64().unwrap_or(0);
    if key == "replicas" {
        println!("{:<20}  {:<10}  {:>12}", "NAME", "STATE", "HELD");
        for row in &rows {
            let (held, ledger) = (num(row, "held"), num(row, "ledger"));
            println!(
                "{:<20}  {:<10}  {:>12}",
                text(row, "name"),
                if held >= ledger { "full" } else { "filling" },
                format!("{held}/{ledger}"),
            );
        }
        return Ok(());
    }
    if key == "nodes" {
        println!("{:<20}  {:<9}  {:>8}", "NAME", "STATE", "PLACED");
        for row in &rows {
            println!(
                "{:<20}  {:<9}  {:>8}",
                text(row, "name"),
                if row["ready"] == true {
                    "ready"
                } else {
                    "not ready"
                },
                format!("{}/{}", num(row, "placed"), num(row, "slots")),
            );
        }
        return Ok(());
    }
    if key == "swarms" {
        println!(
            "{:<20}  {:<8}  {:>6}  {:>10}  {:>6}  {:<10}  GOAL",
            "NAME", "PHASE", "ACTIVE", "DONE", "FAILED", "WORKER"
        );
        for row in &rows {
            let done = match num(row, "completions") {
                0 => format!("{}", num(row, "done")),
                total => format!("{}/{total}", num(row, "done")),
            };
            println!(
                "{:<20}  {:<8}  {:>6}  {:>10}  {:>6}  {:<10}  {}",
                text(row, "name"),
                text(row, "phase"),
                format!("{}/{}", num(row, "active"), num(row, "replicas")),
                done,
                num(row, "failed"),
                text(row, "worker"),
                one_line(&text(row, "goal"), 60),
            );
        }
        return Ok(());
    }
    println!(
        "{:<26}  {:<8}  {:<5}  {:<12}  {:<10}  {:<10}  GOAL",
        "ID", "STATE", "TRY", "SWARM", "WORKER", "REASON"
    );
    for row in &rows {
        println!(
            "{:<26}  {:<8}  {:<5}  {:<12}  {:<10}  {:<10}  {}",
            text(row, "id"),
            text(row, "state"),
            format!("{}/{}", num(row, "attempt"), num(row, "max_attempts")),
            text(row, "swarm"),
            text(row, "worker"),
            text(row, "reason"),
            one_line(&text(row, "goal"), 60),
        );
    }
    Ok(())
}

fn one_line(text: &str, max: usize) -> String {
    let flat: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if flat.chars().count() <= max {
        return flat;
    }
    let mut out: String = flat.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn describe(home: &Path, id: Option<&str>) -> Result<()> {
    let id = id.ok_or_else(|| err("usage: inlet describe <id>"))?;
    let v = checked(
        proto::rpc(home, json!({"op":"describe","id": id}))?,
        "describe",
    )?;
    let task = &v["task"];
    let text = |field: &str| task[field].as_str().unwrap_or("");
    println!("id        {}", text("id"));
    println!("state     {} {}", text("state"), text("reason"));
    println!("worker    {}", text("worker"));
    println!(
        "attempt   {} of {}",
        task["attempt"].as_u64().unwrap_or(1),
        task["max_attempts"].as_u64().unwrap_or(1)
    );
    if let Some(swarm) = task["swarm"].as_str() {
        println!("swarm     {swarm}");
    }
    if let Some(parent) = task["parent"].as_str() {
        println!("parent    {parent}");
    }
    println!("goal      {}", text("goal"));
    println!("verifier  {}", v["verifier"].as_str().unwrap_or("none"));
    if let Some(chain) = v["chain"].as_array().filter(|c| c.len() > 1) {
        println!("attempts");
        for link in chain {
            println!(
                "  {}  {}  {} {}",
                link["attempt"].as_u64().unwrap_or(1),
                link["id"].as_str().unwrap_or(""),
                link["state"].as_str().unwrap_or(""),
                link["reason"].as_str().unwrap_or(""),
            );
        }
    }
    for post in v["posts"].as_array().into_iter().flatten() {
        println!("post      {}", post["text"].as_str().unwrap_or(""));
    }
    for (title, field) in [("output", "output"), ("verifier output", "verifier_output")] {
        let body = v[field].as_str().unwrap_or("");
        if !body.trim().is_empty() {
            println!("{title}");
            for line in body.trim_end().lines() {
                println!("  {line}");
            }
        }
    }
    Ok(())
}

/// The transcript is a file in the home. The daemon does not have to be up.
fn logs(home: &Path, args: &[String]) -> Result<()> {
    let usage = "usage: inlet logs <id> [--verify]";
    let id = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .ok_or_else(|| err(usage))?;
    if id.contains('/') || id.starts_with('.') {
        return Err(err(usage));
    }
    let path = if args.iter().any(|a| a == "--verify") {
        paths::verify_log(home, id)
    } else {
        paths::transcript(home, id)
    };
    let bytes = fs::read(&path).map_err(|_| err(format!("no log for {id}")))?;
    io::stdout().write_all(&bytes)?;
    Ok(())
}

fn scale(home: &Path, args: &[String]) -> Result<()> {
    let usage = "usage: inlet scale <swarm> <replicas>";
    let name = args.first().ok_or_else(|| err(usage))?;
    let replicas = parse_u64(args.get(1).ok_or_else(|| err(usage))?)?;
    let v = proto::rpc(
        home,
        json!({"op":"scale","name": name, "replicas": replicas}),
    )?;
    checked(v, "scale")?;
    println!("swarm {name} replicas {replicas}");
    Ok(())
}

fn delete(home: &Path, args: &[String]) -> Result<()> {
    let name = args
        .iter()
        .find(|a| !a.starts_with('-'))
        .ok_or_else(|| err("usage: inlet delete <swarm> [--kill]"))?;
    let kill = args.iter().any(|a| a == "--kill");
    let v = checked(
        proto::rpc(home, json!({"op":"delete","name": name, "kill": kill}))?,
        "delete",
    )?;
    println!(
        "swarm {name} deleted, {} stopped",
        v["stopped"].as_u64().unwrap_or(0)
    );
    Ok(())
}

/// The parent a kept worker runs under, started by the cell and by nothing
/// else. It copies the worker's output to the transcript and writes the wait
/// status down, so neither needs the daemon to be alive.
fn door_keep(args: &[String]) -> Result<()> {
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};

    let usage = "inlet door keep is run by the cell";
    let num = |i: usize| -> Result<u64> { parse_u64(args.get(i).ok_or_else(|| err(usage))?) };
    let (log_fd, status_fd, cap, deadline) = (num(0)?, num(1)?, num(2)?, num(3)?);
    let memory_kb = num(4)?.saturating_mul(1024);
    let args = &[&args[..4], &args[5..]].concat()[..];
    if args.get(4).map(String::as_str) != Some("--")
        || args.len() < 6
        || log_fd < 3
        || status_fd < 3
    {
        return Err(err(usage));
    }
    // These crossed the exec to get here and must go no further. A worker
    // holding the status file could write its own exit code, and one holding
    // the log could write past the cap.
    for fd in [log_fd as i32, status_fd as i32] {
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
            return Err(err(usage));
        }
    }
    let mut log = unsafe { fs::File::from_raw_fd(log_fd as i32) };
    let mut status = unsafe { fs::File::from_raw_fd(status_fd as i32) };
    let mut ends = [0i32; 2];
    if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(err("pipe"));
    }
    let (mut read, write) = unsafe {
        (
            fs::File::from_raw_fd(ends[0]),
            OwnedFd::from_raw_fd(ends[1]),
        )
    };
    let mut child = Command::new(&args[5])
        .args(&args[6..])
        .stdout(Stdio::from(write.try_clone()?))
        .stderr(Stdio::from(write))
        .spawn();
    let code = match &mut child {
        Ok(child) => {
            unsafe {
                let flags = libc::fcntl(ends[0], libc::F_GETFL);
                libc::fcntl(ends[0], libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            // As pid 1 of the cell a signal with no handler never arrives, so
            // a polite stop from the daemon would be lost. Take it and pass it on.
            unsafe {
                libc::signal(libc::SIGTERM, note_term as *const () as libc::sighandler_t);
            }
            let started = std::time::Instant::now();
            let mut ticks = 0u64;
            let mut over = false;
            let mut written = 0u64;
            let mut buf = [0u8; 8192];
            let mut pump = |log: &mut fs::File| {
                // A bounded gulp. A worker that never stops printing must not
                // keep the keeper from its clock and its memory check.
                for _ in 0..256 {
                    let Ok(n) = read.read(&mut buf) else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    let room = cap.saturating_sub(written).min(n as u64) as usize;
                    let _ = log.write_all(&buf[..room]);
                    written += room as u64;
                }
            };
            loop {
                pump(&mut log);
                if let Ok(Some(done)) = child.try_wait() {
                    // Once more for what it printed last.
                    pump(&mut log);
                    break (done.into_raw(), over);
                }
                if TERM.swap(false, std::sync::atomic::Ordering::Relaxed) {
                    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
                }
                // With no daemon left to time it out, this is the clock.
                if deadline > 0 && started.elapsed() > Duration::from_secs(deadline) {
                    let _ = child.kill();
                }
                // Resident memory of everything in the cell, five times a second.
                // Node, Bun, the JVM, and Go all reserve far more than they
                // touch, so only what is resident can be a cap.
                ticks += 1;
                if memory_kb > 0 && ticks.is_multiple_of(10) && cell_rss_kb() > memory_kb {
                    let _ = writeln!(log, "inlet: over {} MB, stopped", memory_kb / 1024);
                    let _ = child.kill();
                    over = true;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
        Err(e) => {
            let _ = writeln!(log, "inlet: {}: {e}", args[5]);
            (127 << 8, false)
        }
    };
    // The wait status, then why the keeper itself stopped the worker, if it did.
    let (code, over) = code;
    status.write_all(&code.to_ne_bytes())?;
    status.write_all(&i32::from(over).to_ne_bytes())?;
    status.sync_all()?;
    std::process::exit(if libc::WIFEXITED(code) {
        libc::WEXITSTATUS(code)
    } else {
        128 + libc::WTERMSIG(code)
    });
}

static TERM: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

extern "C" fn note_term(_sig: i32) {
    TERM.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Resident memory of every process this one can see. As pid 1 of a cell that
/// is the cell. Outside one it would be the whole host, so it reports nothing.
fn cell_rss_kb() -> u64 {
    if std::process::id() != 1 {
        return 0;
    }
    let page_kb = (unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(4096) as u64) / 1024;
    let Ok(procs) = fs::read_dir("/proc") else {
        return 0;
    };
    procs
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .bytes()
                .all(|b| b.is_ascii_digit())
        })
        .filter_map(|entry| fs::read_to_string(entry.path().join("statm")).ok())
        // Resident less shared: pages a preforking worker's children all map
        // are counted once per child otherwise, and that is a false kill.
        .filter_map(|statm| {
            let mut fields = statm.split(' ').skip(1);
            let resident = fields.next()?.parse::<u64>().ok()?;
            let shared = fields.next()?.parse::<u64>().ok()?;
            Some(resident.saturating_sub(shared))
        })
        .sum::<u64>()
        .saturating_mul(page_kb)
}

/// Most harnesses want a base URL, not a unix socket. This puts the proxy on
/// a loopback port for one command and gets out of the way.
fn door_run(args: &[String]) -> Result<()> {
    use std::net::{Shutdown, TcpListener};
    use std::os::unix::net::UnixStream;
    use std::os::unix::process::ExitStatusExt;

    // A fixed port suits a config file that cannot read the environment. Give
    // such a worker `net = "none"`, so each has the port to itself.
    let (port, args) = match args.first().map(String::as_str) {
        Some("--port") => (
            parse_u64(args.get(1).ok_or_else(|| err("--port needs a number"))?)? as u16,
            &args[2..],
        ),
        _ => (0, args),
    };
    let cmd = match args.first().map(String::as_str) {
        Some("--") => &args[1..],
        _ => args,
    };
    let Some(program) = cmd.first() else {
        return Err(err("usage: inlet door run [--port N] -- CMD..."));
    };
    let sock = std::env::var("INLET_PROXY_SOCK").map_err(|_| err("INLET_PROXY_SOCK is not set"))?;
    loopback_up();
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let port = listener.local_addr()?.port();
    thread::spawn(move || {
        for client in listener.incoming().flatten() {
            let sock = sock.clone();
            thread::spawn(move || {
                // A refusal is the daemon restarting. Hold the caller, do not fail it.
                let started = std::time::Instant::now();
                let upstream = loop {
                    match UnixStream::connect(&sock) {
                        Ok(stream) => break stream,
                        Err(_) if started.elapsed() > Duration::from_secs(60) => return,
                        Err(_) => thread::sleep(Duration::from_millis(250)),
                    }
                };
                let (Ok(mut client_r), Ok(mut upstream_w)) =
                    (client.try_clone(), upstream.try_clone())
                else {
                    return;
                };
                let up = thread::spawn(move || {
                    let _ = io::copy(&mut client_r, &mut upstream_w);
                    let _ = upstream_w.shutdown(Shutdown::Write);
                });
                let (mut upstream_r, mut client_w) = (upstream, client);
                let _ = io::copy(&mut upstream_r, &mut client_w);
                let _ = client_w.shutdown(Shutdown::Both);
                let _ = up.join();
            });
        }
    });
    let base = format!("http://127.0.0.1:{port}");
    let status = std::process::Command::new(program)
        .args(&cmd[1..])
        .env("INLET_PROXY_URL", &base)
        .env("OPENAI_BASE_URL", format!("{base}/v1"))
        .env("OPENAI_API_BASE", format!("{base}/v1"))
        .env("ANTHROPIC_BASE_URL", &base)
        .status()
        .map_err(|e| err(format!("{program}: {e}")))?;
    std::process::exit(
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
    );
}

/// A cell with `net = "none"` has a loopback that was never brought up.
fn loopback_up() {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return;
        }
        let mut req: libc::ifreq = std::mem::zeroed();
        req.ifr_name[0] = b'l' as libc::c_char;
        req.ifr_name[1] = b'o' as libc::c_char;
        if libc::ioctl(fd, libc::SIOCGIFFLAGS as _, &mut req) == 0 {
            req.ifr_ifru.ifru_flags |= libc::IFF_UP as libc::c_short;
            libc::ioctl(fd, libc::SIOCSIFFLAGS as _, &req);
        }
        libc::close(fd);
    }
}

/// The worker's side of the worker socket. Reads `INLET_SOCK` and `INLET_TOKEN`.
fn door(args: &[String]) -> Result<()> {
    const USAGE: &str = "\
inlet door post <text> [--channel NAME]
inlet door board
inlet door spawn -w W -g GOAL --verify CMD [--tokens N] [--seconds N] [--retries N] [-t TAG]...
inlet door children
inlet door wait [--timeout SECONDS]
inlet door draft <name> --run FILE --verify CMD [-t TAG]...
inlet door vote <target> <choice> [--channel NAME]
inlet door run [--port N] -- CMD...   run CMD with the proxy on a local port, OPENAI_BASE_URL set";
    if args.first().map(String::as_str) == Some("run") {
        return door_run(args.get(1..).unwrap_or(&[]));
    }
    if args.first().map(String::as_str) == Some("keep") {
        return door_keep(args.get(1..).unwrap_or(&[]));
    }
    let sock = std::env::var("INLET_SOCK").map_err(|_| err("INLET_SOCK is not set"))?;
    let token = std::env::var("INLET_TOKEN").map_err(|_| err("INLET_TOKEN is not set"))?;
    let call = |mut body: Value| -> Result<Value> {
        body["token"] = json!(token);
        let mut line = serde_json::to_string(&body)?;
        line.push('\n');
        // A refused connection, a dropped one, or a daemon that does not know
        // this token yet all mean the same thing: the daemon is restarting,
        // and it takes its workers back. Ask again, for a minute.
        let started = std::time::Instant::now();
        loop {
            let once = || -> Result<Value> {
                let mut stream = std::os::unix::net::UnixStream::connect(&sock)?;
                stream.write_all(line.as_bytes())?;
                let mut reply = String::new();
                io::BufReader::new(stream).read_line(&mut reply)?;
                serde_json::from_str(&reply).map_err(|_| err("the daemon went away mid-answer"))
            };
            let settling =
                |v: &Value| v["error"] == "token" && started.elapsed() < Duration::from_secs(15);
            match once() {
                Ok(v) if !settling(&v) => return checked(v, "door"),
                Err(e) if started.elapsed() > Duration::from_secs(60) => {
                    return Err(err(format!("worker socket: {e}")));
                }
                _ => thread::sleep(Duration::from_millis(250)),
            }
        }
    };
    let rest = args.get(1..).unwrap_or(&[]);
    let channel = flag(rest, "--channel").unwrap_or_else(|| "general".into());
    let words = || -> Vec<String> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < rest.len() {
            if rest[i] == "--channel" {
                i += 2;
                continue;
            }
            out.push(rest[i].clone());
            i += 1;
        }
        out
    };
    match args.first().map(String::as_str) {
        Some("post") => {
            let text = words().join(" ");
            let v = call(json!({"op":"post","text": text, "channel": channel}))?;
            println!("{}", v["id"].as_str().unwrap_or("ok"));
        }
        Some("board") => println!("{}", call(json!({"op":"board"}))?["posts"]),
        Some("spawn") => {
            let task = parse_add_flags(rest)?;
            if task.verifier.is_none() {
                return Err(err("a child needs --verify"));
            }
            let mut body = proto::task_value(&task);
            body["op"] = json!("spawn");
            // Asked twice, because the first answer was lost, it must be one
            // child and not two. The key says they are the same ask.
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            body["key"] = json!(format!("{:x}-{:x}", std::process::id(), nanos));
            let v = call(body)?;
            if let Some(why) = v["denied"].as_str() {
                return Err(err(format!("denied: {why}")));
            }
            println!("{}", v["ids"][0].as_str().unwrap_or(""));
        }
        Some("children") => println!("{}", call(json!({"op":"children"}))?["children"]),
        Some("wait") => {
            let limit = match flag(rest, "--timeout") {
                Some(raw) => Some(Duration::from_secs(parse_u64(&raw)?)),
                None => None,
            };
            let started = std::time::Instant::now();
            loop {
                let v = call(json!({"op":"children"}))?;
                if v["settled"].as_bool() == Some(true) {
                    println!("{}", v["children"]);
                    break;
                }
                if limit.is_some_and(|limit| started.elapsed() >= limit) {
                    println!("{}", v["children"]);
                    return Err(err("children are still running"));
                }
                thread::sleep(Duration::from_millis(250));
            }
        }
        Some("draft") => {
            let name = rest
                .first()
                .filter(|n| !n.starts_with('-'))
                .ok_or_else(|| err(USAGE))?;
            let run = flag(rest, "--run").ok_or_else(|| err(USAGE))?;
            let verify = flag(rest, "--verify").ok_or_else(|| err(USAGE))?;
            let mut tags = Vec::new();
            for pair in rest.windows(2) {
                if pair[0] == "-t" || pair[0] == "--tag" {
                    tags.push(pair[1].clone());
                }
            }
            let run = fs::read_to_string(&run).map_err(|e| err(format!("{run}: {e}")))?;
            call(json!({"op":"draft","name": name, "run": run, "verifier": verify, "tags": tags}))?;
            println!("draft {name}");
        }
        Some("vote") => {
            let words = words();
            let (Some(target), Some(choice)) = (words.first(), words.get(1)) else {
                return Err(err(USAGE));
            };
            let v =
                call(json!({"op":"vote","target": target, "choice": choice, "channel": channel}))?;
            println!("{}", v["id"].as_str().unwrap_or("ok"));
        }
        _ => return Err(err(USAGE)),
    }
    Ok(())
}

fn status(home: &Path, json_out: bool, leader: Option<String>) -> Result<()> {
    let v = proto::rpc(home, json!({"op":"status","leader": leader}))?;
    if json_out {
        println!("{v}");
        return Ok(());
    }
    let cap = v.get("cap").and_then(|n| n.as_u64()).unwrap_or(1).max(1);
    let spent = v.get("spent").and_then(|n| n.as_u64()).unwrap_or(0);
    let bar = ProgressBar::new(cap);
    bar.set_style(
        ProgressStyle::with_template("{prefix} {bar:28} {pos}/{len} {msg}")
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
                ProgressStyle::with_template("{prefix} {bar:28} {pos}/{len}")
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
                "{:<26}  {:<8}  {:<12}  {}",
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
        bar.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
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
