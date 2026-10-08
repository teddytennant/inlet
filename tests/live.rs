//! The daemon, a real cell, and the proxy. Each test gets its own home.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use inlet::ledger;
use inlet::model::{Decoded, Record};
use serde_json::Value;

struct Daemon {
    home: PathBuf,
    child: Child,
    log_path: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill this process, not whatever pid file is in the home. A restart
        // writes a new pid before the previous guard is dropped.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Daemon {
    fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_inlet"))
}

fn scratch(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("inlet-{name}-{}-{}", std::process::id(), now_ms()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn policy(home: &Path, body: &str) {
    fs::create_dir_all(home.join("ledger")).unwrap();
    fs::create_dir_all(home.join("run")).unwrap();
    fs::write(home.join("policy.lua"), body).unwrap();
}

fn base_policy(extra_workers: &str, admit: &str, caps: &str, proxy: &str) -> String {
    format!(
        r#"
caps = {{
  max_live = 4,
  max_depth = 3,
  max_tokens = 10000,
  max_memory_mb = 8192,
  max_pids = 64,
  token_period = "1d",
  {caps}
}}
setup = "box"
isolator = "rlimit"
human_weight = 4
min_ev = 0
value = 400000
debug = 1
default_budget = {{ tokens = 200, seconds = 30, memory_mb = 64, pids = 8 }}
proxy = {{ {proxy} }}
workers = {{
  sleeper = {{ cmd = {{ "/bin/sleep", "60" }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},
  {extra_workers}
}}
function admit(ctx)
  {admit}
end
"#
    )
}

fn start(home: &Path) -> Daemon {
    start_env(home, &[])
}

fn start_env(home: &Path, env: &[(&str, &str)]) -> Daemon {
    let _ = fs::remove_file(home.join("run/operator.sock"));
    let _ = fs::remove_file(home.join("run/inlet.pid"));
    let log_path = home.join("test-daemon.log");
    let log = File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut cmd = Command::new(bin());
    cmd.args(["--home", home.to_str().unwrap(), "up", "-f"])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err));
    for (key, value) in env {
        cmd.env(key, value);
    }
    let mut child = cmd.spawn().unwrap();
    let sock = home.join("run/operator.sock");
    let deadline = Instant::now() + Duration::from_secs(8);
    let want = child.id() as i32;
    while Instant::now() < deadline {
        if sock.exists() && pid_of(home) == Some(want) {
            return Daemon {
                home: home.to_path_buf(),
                child,
                log_path,
            };
        }
        if let Some(status) = child.try_wait().ok().flatten() {
            let text = fs::read_to_string(&log_path).unwrap_or_default();
            panic!("daemon exited {status}: {text}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    let text = fs::read_to_string(&log_path).unwrap_or_default();
    let _ = child.kill();
    let _ = child.wait();
    panic!("daemon did not come up: {text}");
}

fn sh_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', r"'\''"))
}

fn pid_of(home: &Path) -> Option<i32> {
    let text = fs::read_to_string(home.join("run/inlet.pid")).ok()?;
    text.trim().parse().ok()
}

fn inlet(home: &Path, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.arg("--home").arg(home);
    cmd.args(args);
    let out = cmd.output().unwrap();
    if !out.status.success() {
        let pid = pid_of(home);
        let alive = pid
            .map(|p| Path::new(&format!("/proc/{p}")).exists())
            .unwrap_or(false);
        panic!(
            "inlet {} failed (pid {pid:?} alive {alive}): {}\n{}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
    }
    out
}

fn status(home: &Path) -> Value {
    let out = inlet(home, &["status", "--json"]);
    serde_json::from_slice(&out.stdout).unwrap()
}

fn tasks(v: &Value) -> Vec<&Value> {
    v.get("tasks")
        .and_then(|t| t.as_array())
        .map(|a| a.iter().collect())
        .unwrap_or_default()
}

fn wait_status(home: &Path, mut pred: impl FnMut(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut last = Value::Null;
    while Instant::now() < deadline {
        last = status(home);
        if pred(&last) {
            return last;
        }
        thread::sleep(Duration::from_millis(40));
    }
    let log = fs::read_to_string(home.join("test-daemon.log")).unwrap_or_default();
    panic!("timed out waiting for status: {last}\n{log}");
}

fn read_log(home: &Path) -> Vec<Decoded> {
    let buf = fs::read(home.join("ledger/log")).unwrap_or_default();
    match ledger::scan(&buf) {
        Ok((recs, _)) => recs,
        Err(e) => panic!("scan: {e}"),
    }
}

fn stop(daemon: &mut Daemon) {
    if let Some(pid) = pid_of(&daemon.home) {
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
    }
    let _ = daemon.child.wait();
}

#[test]
fn spawn_hides_the_host_and_keeps_the_key() {
    let home = scratch("hide");
    fs::write(home.join("key"), "KEYMATERIAL\n").unwrap();
    fs::write(home.join("policy.draft.lua"), "DRAFTPOLICY\n").unwrap();
    fs::create_dir_all(home.join("work/other")).unwrap();
    fs::write(home.join("work/other/mine"), "SIBLING\n").unwrap();
    let ledger_path = sh_quote(&home.join("ledger/log"));
    let sock = sh_quote(&home.join("run/operator.sock"));
    let key = sh_quote(&home.join("key"));
    let pubkey = sh_quote(&home.join("keys/policy.pub"));
    let draft = sh_quote(&home.join("policy.draft.lua"));
    let policy_path = sh_quote(&home.join("policy.lua"));
    let sib = sh_quote(&home.join("work/other/mine"));
    let work = sh_quote(&home.join("work"));
    let script = format!(
        "echo OK > /work/result
mark() {{ if [ -e \"$1\" ]; then echo \"$2LEAK\"; else echo \"$2OK\"; fi >> /work/result; }}
mark {ledger_path} LEDGER
mark {sock} SOCK
mark {key} KEY
mark {pubkey} PUB
mark {draft} DRAFT
mark {policy_path} POLICY
mark {sib} SIB
mark {work} WORK
if [ -e /work/../other/mine ]; then echo SIBWALK >> /work/result; else echo SIBWALKOK >> /work/result; fi
if [ -r /proc/1/root/etc/passwd ]; then echo PROCLEAK >> /work/result; else echo PROCOK >> /work/result; fi
sleep 30"
    );
    policy(
        &home,
        &base_policy(
            &format!(
                r#"hider = {{ cmd = {{ "/bin/sh", "-c", {script:?} }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},"#
            ),
            r#"return "allow""#,
            "",
            "",
        ),
    );
    let (public, wrapped) = inlet::sign::generate("cell-pass").unwrap();
    let body = fs::read(home.join("policy.lua")).unwrap();
    let sig = inlet::sign::sign_with(&wrapped, "cell-pass", &body).unwrap();
    fs::create_dir_all(home.join("keys")).unwrap();
    fs::write(home.join("keys/policy.pub"), &public).unwrap();
    fs::write(home.join("keys/policy.key"), &wrapped).unwrap();
    fs::write(home.join("policy.sig"), &sig).unwrap();
    let daemon = start_env(&home, &[("OPENAI_API_KEY", "sk-hostsecret")]);
    assert!(home.join("run/operator.sock").exists(), "{}", daemon.log());
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "hider",
            "--goal",
            "look",
            "--no-verify",
            "--tokens",
            "50",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["state"] == "running" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "hider")
        .unwrap();
    let pid = task["pid"].as_i64().unwrap();
    let env = fs::read(format!("/proc/{pid}/environ")).unwrap_or_default();
    let env = String::from_utf8_lossy(&env);
    assert!(!env.contains("sk-hostsecret"), "host key leaked: {env}");
    let token = env
        .split('\0')
        .find_map(|e| e.strip_prefix("INLET_TOKEN="))
        .unwrap_or("")
        .to_string();
    assert!(!token.is_empty());
    assert!(env.contains(&format!("OPENAI_API_KEY={token}")));
    let fds = fs::read_dir(format!("/proc/{pid}/fd")).unwrap();
    for fd in fds.flatten() {
        let link = fs::read_link(fd.path()).unwrap_or_default();
        let text = link.display().to_string();
        assert!(
            !text.contains("ledger/log"),
            "worker inherited the ledger fd {text}"
        );
    }
    let result = home
        .join("work")
        .join(task["id"].as_str().unwrap())
        .join("result");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut body = String::new();
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(&result) {
            if text.contains("SIBWALKOK") {
                body = text;
                break;
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    for mark in [
        "LEDGEROK",
        "SOCKOK",
        "KEYOK",
        "PUBOK",
        "DRAFTOK",
        "POLICYOK",
        "SIBOK",
        "WORKOK",
        "SIBWALKOK",
        "PROCOK",
    ] {
        assert!(
            body.contains(mark),
            "missing {mark} in {body} log {}",
            daemon.log()
        );
    }
    assert!(!body.contains("LEAK"), "cell leaked a host path: {body}");
}

#[test]
fn kill9_charges_the_slice_once() {
    let home = scratch("kill9");
    policy(
        &home,
        &base_policy("", r#"return "allow""#, "max_tokens = 1000,", ""),
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "nap",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "30",
        ],
    );
    let st = wait_status(&home, |v| v["live"].as_u64() == Some(1));
    assert_eq!(st["held"].as_u64(), Some(100));
    assert_eq!(st["available"].as_u64(), Some(900));
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let worker_pid = tasks(&st)[0]["pid"].as_i64().unwrap();
    assert!(
        Path::new(&format!("/proc/{worker_pid}")).exists(),
        "worker already gone"
    );
    let admits_while_live = read_log(&home)
        .into_iter()
        .filter(|r| {
            matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id: i, .. } if i == &id))
        })
        .count();
    assert_eq!(
        admits_while_live, 1,
        "admit was not durable before the worker was killed"
    );
    let pid = pid_of(&home).unwrap();
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = daemon.child.wait();
    // The lock died with the process. A second start must charge this admit once.
    let daemon = start(&home);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == id && t["state"] == "failed")
    });
    assert_eq!(st["spent"].as_u64(), Some(100), "{}", daemon.log());
    assert_eq!(st["held"].as_u64(), Some(0));
    assert_eq!(st["available"].as_u64(), Some(900));
    let task = tasks(&st).into_iter().find(|t| t["id"] == id).unwrap();
    assert_eq!(task["reason"], "crash");
    drop(daemon);
    let daemon = start(&home);
    let st = status(&home);
    assert_eq!(
        st["spent"].as_u64(),
        Some(100),
        "second restart double-spent"
    );
    assert_eq!(st["available"].as_u64(), Some(900));
    let admits = read_log(&home)
        .into_iter()
        .filter(|r| matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id: i, .. } if i == &id)))
        .count();
    let exits = read_log(&home)
        .into_iter()
        .filter(|r| matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Exit { id: i, .. } if i == &id)))
        .count();
    assert_eq!(admits, 1);
    assert_eq!(exits, 1);
    let _ = daemon;
}

#[test]
fn crash_requeue_is_a_new_admission() {
    let home = scratch("requeue");
    policy(
        &home,
        &base_policy(
            r#"prover = { cmd = { "/usr/bin/python3", "-c", "import os; os.abort()" }, tags = { "math" }, net = "none", on_crash = "requeue" },"#,
            r#"return "allow""#,
            "max_tokens = 250,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "prover",
            "--goal",
            "proof",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "10",
            "--memory-mb",
            "256",
        ],
    );
    let st = wait_status(&home, |v| {
        let ts = tasks(v);
        let crashed = ts.iter().any(|t| t["reason"] == "crash");
        let retried = ts.iter().any(|t| t["retry_of"].is_string());
        let quiet = v["live"].as_u64() == Some(0) && v["queued"].as_u64() == Some(0);
        crashed && retried && quiet
    });
    assert_eq!(st["spent"].as_u64(), Some(200), "{st} {}", daemon.log());
    let original = tasks(&st)
        .into_iter()
        .find(|t| t["retry_of"].is_null() && t["worker"] == "prover")
        .unwrap();
    assert_eq!(original["reason"], "crash");
    drop(daemon);
    let _daemon = start(&home);
    let again = status(&home);
    assert_eq!(again["spent"].as_u64(), Some(200));
    assert_eq!(again["available"].as_u64(), Some(50));
}

#[test]
fn torn_tail_and_damage_and_opaque() {
    let home = scratch("log");
    policy(&home, &base_policy("", r#"return "allow""#, "", ""));
    let mut daemon = start(&home);
    inlet(&home, &["post", "keep me"]);
    thread::sleep(Duration::from_millis(250));
    stop(&mut daemon);
    let path = home.join("ledger/log");
    let before = fs::read(&path).unwrap();
    assert!(before.len() > 8);
    {
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[1, 2, 3, 4, 5]).unwrap();
    }
    let daemon = start(&home);
    let after = fs::read(&path).unwrap();
    assert_eq!(after, before, "torn tail was not cut");
    inlet(&home, &["post", "still here"]);
    thread::sleep(Duration::from_millis(250));
    drop(daemon);

    let body = br#"{"kind":"vote","voter":"you","choice":"pin","weight":4}"#;
    let mut frame = Vec::new();
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&ledger::crc32(body).to_le_bytes());
    frame.extend_from_slice(body);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&frame)
        .unwrap();
    let _daemon = start(&home);
    let decoded = read_log(&home);
    assert!(decoded.iter().any(|d| matches!(d, Decoded::Opaque)));
    assert!(status(&home)["ok"].as_bool().unwrap());
    drop(_daemon);

    let mut buf = fs::read(&path).unwrap();
    // Flip a byte in the first body, leaving a later good frame in place.
    buf[12] ^= 0xff;
    fs::write(&path, &buf).unwrap();
    let log_path = home.join("damage.log");
    let log = File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut child = Command::new(bin())
        .args(["--home", home.to_str().unwrap(), "up", "-f"])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    let status = child.wait().unwrap();
    let text = fs::read_to_string(&log_path).unwrap_or_default();
    assert!(!status.success(), "{text}");
    assert!(text.contains("damage"), "{text}");
}

#[test]
fn lua_deny_and_instruction_budget() {
    let home = scratch("lua");
    policy(
        &home,
        &base_policy(
            "",
            r#"if ctx.tags.spam then return "deny" end; return "allow""#,
            "",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "nope",
            "--no-verify",
            "-t",
            "spam",
        ],
    );
    let st = wait_status(&home, |v| tasks(v).iter().any(|t| t["reason"] == "lua"));
    assert_eq!(st["live"].as_u64(), Some(0));
    drop(daemon);

    let home = scratch("lua-loop");
    policy(
        &home,
        &base_policy("", r#"while true do end; return "allow""#, "", ""),
    );
    let daemon = start(&home);
    let started = Instant::now();
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "spin",
            "--no-verify",
        ],
    );
    let st = wait_status(&home, |v| tasks(v).iter().any(|t| t["reason"] == "lua"));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{}",
        daemon.log()
    );
    assert_eq!(st["tasks"][0]["state"], "failed");
}

#[test]
fn batch_and_posts_land_in_the_log() {
    let home = scratch("batch");
    policy(
        &home,
        &base_policy(
            "",
            r#"if ctx.tags.spam then return "deny" end; return "allow""#,
            "",
            "",
        ),
    );
    let daemon = start(&home);
    let jsonl = home.join("tasks.jsonl");
    fs::write(
        &jsonl,
        "{\"worker\":\"sleeper\",\"goal\":\"a\",\"no_verify\":true,\"tags\":[\"spam\"]}\n\
         {\"worker\":\"sleeper\",\"goal\":\"b\",\"no_verify\":true,\"tags\":[\"spam\"]}\n\
         {\"worker\":\"sleeper\",\"goal\":\"c\",\"no_verify\":true,\"tags\":[\"spam\"]}\n",
    )
    .unwrap();
    inlet(&home, &["add", "-f", jsonl.to_str().unwrap()]);
    let decoded = read_log(&home);
    let goals: Vec<String> = decoded
        .iter()
        .filter_map(|d| match d {
            Decoded::Rec(r) => match r.as_ref() {
                Record::Task { goal, .. } => Some(goal.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        goals.contains(&"a".into()) && goals.contains(&"b".into()) && goals.contains(&"c".into())
    );
    inlet(&home, &["post", "hello @sleeper @all"]);
    say(&home, "from you @sleeper");
    thread::sleep(Duration::from_millis(300));
    let decoded = read_log(&home);
    let posts: Vec<&Record> = decoded
        .iter()
        .filter_map(|d| match d {
            Decoded::Rec(r) => match r.as_ref() {
                Record::Post { .. } => Some(r.as_ref()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(posts.iter().any(|p| matches!(p, Record::Post { role, channel, mentions, text, .. } if role == "operator" && channel == "general" && text.contains("hello") && mentions.iter().any(|m| m == "sleeper") && mentions.iter().any(|m| m == "all"))));
    assert!(posts.iter().any(|p| matches!(p, Record::Post { author, role, weight, channel, .. } if author == "you" && role == "human" && *weight == 4 && channel == "general")));
    let _ = daemon;
}

#[test]
fn reset_does_not_credit_a_live_slice() {
    let home = scratch("reset");
    policy(
        &home,
        &base_policy(
            "",
            r#"return "allow""#,
            "max_tokens = 1000,\n  token_period = \"2s\",",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "hold",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "20",
        ],
    );
    wait_status(&home, |v| v["live"].as_u64() == Some(1));
    let st = wait_status(&home, |v| {
        v["resets"].as_u64().unwrap_or(0) >= 2 && v["live"].as_u64() == Some(1)
    });
    assert_eq!(st["held"].as_u64(), Some(100), "{st}");
    assert_eq!(st["available"].as_u64(), Some(900), "{st} {}", daemon.log());
    let id = tasks(&st)[0]["id"].as_str().unwrap();
    inlet(&home, &["kill", id]);
    let st = wait_status(&home, |v| {
        v["live"].as_u64() == Some(0) && v["held"].as_u64() == Some(0)
    });
    assert_eq!(st["available"].as_u64(), Some(1000), "{st}");
}

#[test]
fn proxy_drains_the_purse_and_kills() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            thread::spawn(|| serve_upstream(conn));
        }
    });
    let home = scratch("proxy");
    policy(
        &home,
        &base_policy(
            "",
            r#"return "allow""#,
            "max_tokens = 1000,",
            &format!("upstream = \"http://127.0.0.1:{port}\", key = \"test-key\""),
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "spend",
            "--no-verify",
            "--tokens",
            "30",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let pid = tasks(&st)[0]["pid"].as_i64().unwrap();
    let env =
        fs::read(format!("/proc/{pid}/environ")).unwrap_or_else(|_| panic!("{}", daemon.log()));
    let env = String::from_utf8_lossy(&env);
    let token = env
        .split('\0')
        .find_map(|e| e.strip_prefix("INLET_TOKEN="))
        .unwrap()
        .to_string();
    let sock = home.join("proxy/proxy.sock");
    let first = proxy_post(&sock, &token, 20);
    assert!(first.contains("200") || first.contains("usage"), "{first}");
    let second = proxy_post(&sock, &token, 20);
    assert!(
        !second.contains("empty_purse"),
        "clamped call should pass: {second}"
    );
    let third = proxy_post(&sock, &token, 20);
    assert!(third.contains("empty_purse"), "{third}");
    let st = wait_status(&home, |v| tasks(v).iter().any(|t| t["reason"] == "purse"));
    assert_eq!(st["tasks"][0]["state"], "failed");
    assert_eq!(st["available"].as_u64(), Some(970), "{st}");
}

#[test]
fn code_crash_stays_failed() {
    let home = scratch("code-crash");
    policy(
        &home,
        &base_policy(
            r#"bomber = { cmd = { "/usr/bin/python3", "-c", "import os; os.abort()" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "bomber",
            "--goal",
            "boom",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "10",
            "--memory-mb",
            "256",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "bomber" && t["state"] == "failed")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "bomber")
        .unwrap();
    assert_eq!(task["reason"], "crash", "{st} {}", daemon.log());
    assert!(task["retry_of"].is_null(), "{st}");
    assert_eq!(tasks(&st).len(), 1, "{st}");
    assert_eq!(st["spent"].as_u64(), Some(100));
    assert_eq!(st["held"].as_u64(), Some(0));
    assert_eq!(st["available"].as_u64(), Some(900));
}

#[test]
fn clean_exit_refunds_the_slice() {
    let home = scratch("exit");
    policy(
        &home,
        &base_policy(
            r#"done = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "done",
            "--goal",
            "finish",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "done" && t["state"] == "done")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "done")
        .unwrap();
    assert_eq!(task["reason"], "ok", "{st} {}", daemon.log());
    assert_eq!(st["held"].as_u64(), Some(0));
    assert_eq!(st["available"].as_u64(), Some(1000), "{st}");
    assert_eq!(st["spent"].as_u64(), Some(0));
}

#[test]
fn slurm_isolator_denies() {
    let home = scratch("slurm");
    let body = base_policy("", r#"return "allow""#, "", "")
        .replace("isolator = \"rlimit\"", "isolator = \"slurm\"");
    policy(&home, &body);
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "cluster",
            "--no-verify",
            "--tokens",
            "50",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["reason"] == "isolator")
    });
    assert_eq!(st["live"].as_u64(), Some(0), "{st} {}", daemon.log());
    assert_eq!(st["tasks"][0]["state"], "failed");
    assert!(
        !read_log(&home).iter().any(|d| matches!(
            d,
            Decoded::Rec(r) if matches!(r.as_ref(), Record::Spawn { .. })
        )),
        "slurm placed a worker"
    );
}

#[test]
fn watch_prints_a_post() {
    let home = scratch("watch");
    policy(&home, &base_policy("", r#"return "allow""#, "", ""));
    let daemon = start(&home);
    let mut child = Command::new(bin())
        .args(["--home", home.to_str().unwrap(), "watch"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        for next in BufReader::new(stdout).lines() {
            match next {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut line = String::new();
    let mut posted = false;
    while Instant::now() < deadline {
        if !posted {
            thread::sleep(Duration::from_millis(200));
            inlet(&home, &["post", "board hello"]);
            posted = true;
        }
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(next) if next.contains("board hello") => {
                line = next;
                break;
            }
            _ => {}
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        line.contains("operator") && line.contains("board hello"),
        "watch line {line:?} log {}",
        daemon.log()
    );
}

fn token_of(pid: i64) -> String {
    let env = fs::read(format!("/proc/{pid}/environ")).unwrap_or_default();
    let env = String::from_utf8_lossy(&env);
    env.split('\0')
        .find_map(|e| e.strip_prefix("INLET_TOKEN="))
        .unwrap_or("")
        .to_string()
}

fn worker_rpc(home: &Path, body: Value) -> Value {
    let stream = UnixStream::connect(home.join("run/worker.sock")).unwrap();
    let mut stream = BufReader::new(stream);
    let mut line = serde_json::to_string(&body).unwrap();
    line.push('\n');
    stream.get_mut().write_all(line.as_bytes()).unwrap();
    let mut buf = String::new();
    stream.read_line(&mut buf).unwrap();
    serde_json::from_str(&buf).unwrap_or_else(|_| panic!("worker reply {buf}"))
}

#[test]
fn verifier_lands_on_the_ledger() {
    let home = scratch("verify");
    policy(
        &home,
        &base_policy(
            r#"job = { cmd = { "/bin/sh", "-c", "echo hi > out" }, tags = { "code" }, net = "host", on_crash = "fail" },
            bad = { cmd = { "/bin/false" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "job",
            "--goal",
            "pass",
            "--verify",
            "grep -q hi out",
            "--tokens",
            "40",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "pass" && t["state"] == "done")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "pass")
        .unwrap();
    assert_eq!(task["reason"], "ok", "{st} {}", daemon.log());
    assert_eq!(st["available"].as_u64(), Some(1000), "{st}");
    assert!(read_log(&home).iter().any(|d| {
        matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Result { id, ok: true, .. } if id == task["id"].as_str().unwrap()))
    }));

    inlet(
        &home,
        &[
            "add",
            "--worker",
            "job",
            "--goal",
            "fail",
            "--verify",
            "/bin/false",
            "--tokens",
            "40",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "fail" && t["state"] == "failed")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "fail")
        .unwrap();
    assert_eq!(task["reason"], "verifier", "{st} {}", daemon.log());
    assert_eq!(st["available"].as_u64(), Some(1000), "{st}");
    assert!(read_log(&home).iter().any(|d| {
        matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Result { id, ok: false, .. } if id == task["id"].as_str().unwrap()))
    }));

    inlet(
        &home,
        &[
            "add",
            "--worker",
            "bad",
            "--goal",
            "exit",
            "--verify",
            "/bin/true",
            "--tokens",
            "40",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "exit" && t["state"] == "failed")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "exit")
        .unwrap();
    assert_eq!(task["reason"], "exit", "{st} {}", daemon.log());
    let id = task["id"].as_str().unwrap();
    assert!(!read_log(&home).iter().any(|d| {
        matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Result { id: got, .. } if got == id))
    }));
}

#[test]
fn child_slice_or_deny() {
    let home = scratch("child");
    policy(
        &home,
        &base_policy(
            r#"holder = { cmd = { "/bin/sh", "-c", "if [ -S /run/worker.sock ]; then echo DOOROK > /work/door; fi; sleep 40" }, tags = { "code" }, net = "host", on_crash = "fail" },
            kid = { cmd = { "/bin/sleep", "30" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "holder",
            "--goal",
            "hold",
            "--no-verify",
            "--tokens",
            "80",
            "--seconds",
            "30",
            "--memory-mb",
            "64",
            "--pids",
            "8",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "holder" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let parent = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "holder")
        .unwrap();
    let parent_id = parent["id"].as_str().unwrap().to_string();
    let pid = parent["pid"].as_i64().unwrap();
    let door = home.join("work").join(&parent_id).join("door");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && fs::read_to_string(&door).ok().as_deref() != Some("DOOROK\n")
    {
        thread::sleep(Duration::from_millis(30));
    }
    assert_eq!(
        fs::read_to_string(&door).unwrap_or_default().trim(),
        "DOOROK",
        "{}",
        daemon.log()
    );
    let token = token_of(pid);
    assert!(!token.is_empty());
    let spawned = worker_rpc(
        &home,
        serde_json::json!({
            "op": "spawn",
            "token": token,
            "worker": "kid",
            "goal": "nap",
            "verify": "/bin/true",
            "tokens": 30,
            "seconds": 10,
            "memory_mb": 16,
            "pids": 2
        }),
    );
    assert_eq!(spawned["ok"], true, "{spawned} {}", daemon.log());
    assert!(spawned["denied"].is_null(), "{spawned}");
    let st = wait_status(&home, |v| {
        let ts = tasks(v);
        ts.iter()
            .any(|t| t["id"] == parent_id && t["state"] == "blocked")
            && ts
                .iter()
                .any(|t| t["worker"] == "kid" && t["state"] == "running")
    });
    assert_eq!(
        st["available"].as_u64(),
        Some(920),
        "child minted from the pool: {st}"
    );
    assert_eq!(st["held"].as_u64(), Some(80), "{st}");
    let child = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "kid")
        .unwrap();
    assert_eq!(child["parent"].as_str(), Some(parent_id.as_str()));
    let child_id = child["id"].as_str().unwrap().to_string();
    let admits = read_log(&home)
        .into_iter()
        .filter(|r| matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id, .. } if id == &child_id)))
        .count();
    assert_eq!(admits, 1, "child admit was not durable");
    let refused = worker_rpc(
        &home,
        serde_json::json!({"op":"spawn","token": token, "worker":"kid","goal":"nope"}),
    );
    assert_eq!(refused["ok"], false, "{refused}");
    assert!(
        refused["error"].as_str().unwrap_or("").contains("verifier"),
        "{refused}"
    );
    inlet(&home, &["kill", &child_id]);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == parent_id && t["state"] == "running")
    });
    assert_eq!(st["available"].as_u64(), Some(920), "{st}");
    inlet(&home, &["kill", &parent_id]);
    let st = wait_status(&home, |v| {
        v["live"].as_u64() == Some(0) && v["held"].as_u64() == Some(0)
    });
    assert_eq!(st["available"].as_u64(), Some(1000), "{st}");
    drop(daemon);

    let home = scratch("child-fat");
    policy(
        &home,
        &base_policy(
            r#"holder = { cmd = { "/bin/sleep", "40" }, tags = { "code" }, net = "host", on_crash = "fail" },
            kid = { cmd = { "/bin/sleep", "30" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "holder",
            "--goal",
            "hold",
            "--no-verify",
            "--tokens",
            "80",
            "--seconds",
            "30",
            "--memory-mb",
            "64",
            "--pids",
            "8",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let pid = tasks(&st)[0]["pid"].as_i64().unwrap();
    let token = token_of(pid);
    let spawned = worker_rpc(
        &home,
        serde_json::json!({
            "op":"spawn","token": token, "worker":"kid","goal":"too big","verify":"/bin/true",
            "tokens": 200, "seconds": 10, "memory_mb": 16, "pids": 2
        }),
    );
    assert_eq!(spawned["denied"], "purse", "{spawned} {}", daemon.log());
    let st = status(&home);
    assert_eq!(st["available"].as_u64(), Some(920), "{st}");
    assert_eq!(st["live"].as_u64(), Some(1), "{st}");
    let rows = tasks(&st);
    let kid = rows.iter().find(|t| t["worker"] == "kid").unwrap();
    assert_eq!(kid["state"], "failed");
    assert_eq!(kid["reason"], "purse");
    let kid_id = kid["id"].as_str().unwrap().to_string();
    let parent_id = rows.iter().find(|t| t["worker"] == "holder").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(!read_log(&home).iter().any(|d| {
        matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Spawn { id, .. } if id == &kid_id))
    }));
    let posted = worker_rpc(
        &home,
        serde_json::json!({"op":"post","token": token, "text":"from the child door @all"}),
    );
    assert_eq!(posted["ok"], true, "{posted}");
    thread::sleep(Duration::from_millis(200));
    let decoded = read_log(&home);
    assert!(decoded.iter().any(|d| matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Post { role, author, mentions, .. } if role == "worker" && author == &parent_id && mentions.iter().any(|m| m == "all")))));
    drop(daemon);

    let home = scratch("child-depth");
    policy(
        &home,
        &base_policy(
            r#"holder = { cmd = { "/bin/sleep", "20" }, tags = { "code" }, net = "host", on_crash = "fail" },
            kid = { cmd = { "/bin/sleep", "20" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_depth = 1,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "holder",
            "--goal",
            "hold",
            "--no-verify",
            "--tokens",
            "50",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let pid = tasks(&st)[0]["pid"].as_i64().unwrap();
    let token = token_of(pid);
    let spawned = worker_rpc(
        &home,
        serde_json::json!({
            "op":"spawn","token": token, "worker":"kid","goal":"too deep","verify":"/bin/true",
            "tokens": 10, "seconds": 5, "memory_mb": 16, "pids": 2
        }),
    );
    assert_eq!(spawned["denied"], "depth", "{spawned} {}", daemon.log());
    assert_eq!(status(&home)["live"].as_u64(), Some(1));
}

fn say(home: &Path, text: &str) {
    let stream = UnixStream::connect(home.join("run/operator.sock")).unwrap();
    let mut stream = BufReader::new(stream);
    let line = format!(
        "{{\"op\":\"say\",\"text\":{}}}\n",
        serde_json::to_string(text).unwrap()
    );
    stream.get_mut().write_all(line.as_bytes()).unwrap();
    let mut buf = String::new();
    stream.read_line(&mut buf).unwrap();
    assert!(buf.contains("\"ok\":true"), "{buf}");
}

fn proxy_post(sock: &Path, token: &str, max_tokens: u64) -> String {
    let body = format!(r#"{{"max_tokens":{max_tokens}}}"#);
    proxy_post_body(sock, token, &body)
}

fn proxy_post_body(sock: &Path, token: &str, body: &str) -> String {
    let req = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: inlet\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let mut stream = UnixStream::connect(sock).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(req.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    String::from_utf8_lossy(&buf).into_owned()
}

fn serve_upstream(mut sock: std::net::TcpStream) {
    let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while buf.len() < 64 * 1024 {
        match sock.read(&mut byte) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let header = String::from_utf8_lossy(&buf);
    let len = header
        .lines()
        .find_map(|l| {
            l.split_once(':').and_then(|(k, v)| {
                (k.eq_ignore_ascii_case("content-length")).then(|| v.trim().parse::<usize>().ok())
            })
        })
        .flatten()
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    if len > 0 {
        let _ = sock.read_exact(&mut body);
    }
    let want = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("max_tokens").and_then(|n| n.as_u64()))
        .unwrap_or(1);
    let payload = format!(r#"{{"usage":{{"total_tokens":{want}}}}}"#);
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = sock.write_all(resp.as_bytes());
}

fn recipe_run() -> &'static str {
    "#!/bin/sh\necho ok\n"
}

fn recipe_verifier() -> &'static str {
    "test \"$(./run)\" = ok"
}

#[test]
fn second_run_readies_then_pin_promotes() {
    let home = scratch("pin");
    policy(
        &home,
        &base_policy(
            r#"nap = { cmd = { "/bin/sleep", "30" }, tags = { "code" }, net = "host", on_crash = "fail" },
            checker = { cmd = { "/bin/sh", "-c", "./run" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "nap",
            "--goal",
            "author",
            "--no-verify",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let author = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let drafted = worker_rpc(
        &home,
        serde_json::json!({
            "op": "draft",
            "token": token,
            "name": "add",
            "run": recipe_run(),
            "verifier": recipe_verifier(),
        }),
    );
    assert_eq!(drafted["ok"], true, "{drafted} {}", daemon.log());
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "checker",
            "--goal",
            "second",
            "--verify",
            recipe_verifier(),
            "--recipe",
            "add",
            "--tokens",
            "40",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "second" && t["state"] == "done")
    });
    assert_eq!(
        tasks(&st).iter().find(|t| t["goal"] == "second").unwrap()["reason"],
        "ok",
        "{st} {}",
        daemon.log()
    );
    assert!(
        !home.join("registry/recipes/add/run").exists(),
        "unattended is off, pin should be required"
    );
    let meta: Value = serde_json::from_slice(
        &fs::read(home.join("drafts").join(&author).join("add/meta.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["ready"], true, "{meta}");
    assert_eq!(meta["stub"], false, "{meta}");
    inlet(&home, &["pin", "add"]);
    let run = fs::read_to_string(home.join("registry/recipes/add/run")).unwrap();
    assert!(run.contains("echo ok"), "{run}");
    assert!(read_log(&home).iter().any(|d| {
        matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Promote { name, by, .. } if name == "add" && by == "operator"))
    }));
    fs::remove_file(home.join("registry/recipes/add/run")).unwrap();
    drop(daemon);
    let _daemon = start(&home);
    assert!(
        home.join("registry/recipes/add/run").is_file(),
        "restart did not rebuild a promoted recipe"
    );
}

#[test]
fn unattended_promotes_and_a_stub_does_not() {
    let home = scratch("auto");
    let mut body = base_policy(
        r#"nap = { cmd = { "/bin/sleep", "20" }, tags = { "code" }, net = "host", on_crash = "fail" },
        checker = { cmd = { "/bin/sh", "-c", "./run" }, tags = { "code" }, net = "host", on_crash = "fail" },
        plain = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
        r#"return "allow""#,
        "max_tokens = 1000,",
        "",
    );
    body.push_str("\nunattended = true\n");
    policy(&home, &body);
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "nap",
            "--goal",
            "author",
            "--no-verify",
            "--tokens",
            "30",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let drafted = worker_rpc(
        &home,
        serde_json::json!({"op":"draft","token": token, "name":"add","run": recipe_run(), "verifier": recipe_verifier()}),
    );
    assert_eq!(drafted["ok"], true, "{drafted}");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "plain",
            "--goal",
            "bare",
            "--no-verify",
            "--recipe",
            "add",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "bare" && t["state"] == "done")
    });
    assert!(
        !home.join("registry/recipes/add/run").exists(),
        "a task with no verifier promoted"
    );
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "checker",
            "--goal",
            "second",
            "--verify",
            recipe_verifier(),
            "--recipe",
            "add",
            "--tokens",
            "30",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "second" && t["state"] == "done")
    });
    let second = tasks(&st).iter().find(|t| t["goal"] == "second").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        home.join("registry/recipes/add/run").is_file(),
        "{st} {}",
        daemon.log()
    );
    assert!(read_log(&home).iter().any(|d| {
        matches!(d, Decoded::Rec(r) if matches!(r.as_ref(), Record::Promote { name, by, .. } if name == "add" && by == &second))
    }));
    drop(daemon);

    let home = scratch("stub");
    let mut body = base_policy(
        r#"nap = { cmd = { "/bin/sleep", "20" }, tags = { "code" }, net = "host", on_crash = "fail" },
        plain = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
        r#"return "allow""#,
        "",
        "",
    );
    body.push_str("\nunattended = true\n");
    policy(&home, &body);
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "nap",
            "--goal",
            "author",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let author = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    worker_rpc(
        &home,
        serde_json::json!({"op":"draft","token": token, "name":"noop","run": recipe_run(), "verifier": "/bin/true"}),
    );
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "plain",
            "--goal",
            "second",
            "--verify",
            "/bin/true",
            "--recipe",
            "noop",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "second" && t["state"] == "done")
    });
    assert!(
        !home.join("registry/recipes/noop/run").exists(),
        "{}",
        daemon.log()
    );
    let meta: Value = serde_json::from_slice(
        &fs::read(home.join("drafts").join(&author).join("noop/meta.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["stub"], true, "{meta}");
    assert_eq!(meta["ready"], false, "{meta}");
    inlet(&home, &["pin", "noop"]);
    assert!(home.join("registry/recipes/noop/run").is_file());
}

#[test]
fn author_does_not_promote_and_the_cell_keeps_the_preamble() {
    let home = scratch("author");
    let draft_path = home.join("drafts/will-exist");
    fs::create_dir_all(&draft_path).unwrap();
    fs::write(draft_path.join("secret"), "nope").unwrap();
    let secret = draft_path.join("secret");
    let script = format!(
        "cp /etc/preamble /work/preamble; if [ -e '{}' ]; then echo LEAK; else echo DRAFTHIDDEN; fi > /work/hide; while [ ! -f go ]; do sleep 0.05; done; exit 0",
        secret.display()
    );
    policy(
        &home,
        &base_policy(
            &format!(
                r#"hold = {{ cmd = {{ "/bin/sh", "-c", {script:?} }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},
                plain = {{ cmd = {{ "/bin/true" }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},
                look = {{ cmd = {{ "/bin/sh", "-c", "if [ -f /registry/recipes/add/run ]; then echo SEEN > /work/seen; else echo MISSING > /work/seen; fi" }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},
                watch = {{ cmd = {{ "/bin/sh", "-c", "for i in 1 2 3 4 5 6 7 8 9 10 11 12; do if [ -f /registry/recipes/add/run ]; then echo SEEN > /work/seen; exit 0; fi; sleep 0.2; done; echo HIDDEN > /work/seen; sleep 15" }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},"#
            ),
            r#"return "allow""#,
            "max_tokens = 2000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "hold",
            "--goal",
            "mine",
            "--verify",
            recipe_verifier(),
            "--recipe",
            "add",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "hold" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let author = tasks(&st).iter().find(|t| t["worker"] == "hold").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let token = token_of(
        tasks(&st).iter().find(|t| t["worker"] == "hold").unwrap()["pid"]
            .as_i64()
            .unwrap(),
    );
    let drafted = worker_rpc(
        &home,
        serde_json::json!({"op":"draft","token": token, "name":"add","run": recipe_run(), "verifier": recipe_verifier()}),
    );
    assert_eq!(drafted["ok"], true, "{drafted} {}", daemon.log());
    let preamble_path = home.join("work").join(&author).join("preamble");
    let hide_path = home.join("work").join(&author).join("hide");
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline
        && (fs::read_to_string(&preamble_path)
            .unwrap_or_default()
            .is_empty()
            || fs::read_to_string(&hide_path)
                .unwrap_or_default()
                .trim()
                .is_empty())
    {
        thread::sleep(Duration::from_millis(30));
    }
    let preamble = fs::read_to_string(&preamble_path).unwrap_or_default();
    assert!(preamble.contains(&author), "{preamble}");
    assert!(
        preamble.contains("Do not open a private channel to a sibling"),
        "{preamble}"
    );
    assert!(preamble.contains("Talk on the board"), "{preamble}");
    assert!(
        preamble.contains("Empty purse: stop and post"),
        "{preamble}"
    );
    assert!(
        preamble.len() < 1500,
        "preamble is {} bytes",
        preamble.len()
    );
    assert_eq!(
        fs::read_to_string(&hide_path).unwrap_or_default().trim(),
        "DRAFTHIDDEN"
    );
    fs::write(home.join("work").join(&author).join("go"), "1").unwrap();
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == author && t["state"] == "done")
    });
    let meta: Value = serde_json::from_slice(
        &fs::read(home.join("drafts").join(&author).join("add/meta.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(meta["ready"], false, "author promoted itself: {meta}");
    assert!(!home.join("registry/recipes/add/run").exists());

    inlet(
        &home,
        &[
            "add",
            "--worker",
            "watch",
            "--goal",
            "snap",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "watch" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let watch_id = tasks(&st).iter().find(|t| t["worker"] == "watch").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    inlet(&home, &["pin", "add"]);
    let seen = home.join("work").join(&watch_id).join("seen");
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut body = String::new();
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(&seen) {
            if !text.trim().is_empty() {
                body = text;
                break;
            }
        }
        thread::sleep(Duration::from_millis(40));
    }
    assert_eq!(
        body.trim(),
        "HIDDEN",
        "running worker saw a new recipe: {body} {}",
        daemon.log()
    );
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "look",
            "--goal",
            "next",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "look" && t["state"] == "done")
    });
    let look_id = tasks(&st).iter().find(|t| t["worker"] == "look").unwrap()["id"]
        .as_str()
        .unwrap();
    let seen = fs::read_to_string(home.join("work").join(look_id).join("seen")).unwrap_or_default();
    assert_eq!(seen.trim(), "SEEN", "{seen}");
}

#[derive(Clone, Debug)]
struct DecisionHit {
    path: String,
    body: String,
}

enum DecisionReply {
    Body(Vec<u8>),
    Hang,
    Drop,
}

struct FakeDecision {
    port: u16,
    hits: Arc<Mutex<Vec<DecisionHit>>>,
}

fn fake_decision(reply: Arc<dyn Fn(&str) -> DecisionReply + Send + Sync>) -> FakeDecision {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(Mutex::new(Vec::new()));
    let saved = hits.clone();
    thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { continue };
            let _ = conn.set_read_timeout(Some(Duration::from_secs(2)));
            let raw = read_http_req(&mut conn);
            let (path, body) = split_http(&raw);
            if path.is_empty() && body.is_empty() {
                continue;
            }
            saved.lock().unwrap().push(DecisionHit {
                path,
                body: body.clone(),
            });
            match reply(&body) {
                DecisionReply::Body(buf) => {
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        buf.len()
                    );
                    let _ = conn.write_all(header.as_bytes());
                    let _ = conn.write_all(&buf);
                }
                DecisionReply::Hang => thread::sleep(Duration::from_secs(3)),
                DecisionReply::Drop => drop(conn),
            }
        }
    });
    FakeDecision { port, hits }
}

fn read_http_req(conn: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    let header_end;
    loop {
        match conn.read(&mut tmp) {
            Ok(0) | Err(_) => return buf,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    header_end = i + 4;
                    break;
                }
                if buf.len() > 64 * 1024 {
                    return buf;
                }
            }
        }
    }
    let header = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let len = header
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            if k.eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    while buf.len() < header_end + len {
        match conn.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
    buf
}

fn split_http(raw: &[u8]) -> (String, String) {
    let text = String::from_utf8_lossy(raw);
    let mut lines = text.split("\r\n");
    let request = lines.next().unwrap_or("");
    let path = request.split_whitespace().nth(1).unwrap_or("").to_string();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (path, body)
}

fn decision_policy(workers: &str, port: u16, extra: &str) -> String {
    format!(
        "{}\ndecision = {{\n  kind = \"openai\",\n  endpoint = \"http://127.0.0.1:{port}/decide\",\n  model = \"gpt-6-luna\",\n  timeout_ms = 800,\n  purse_tokens = 50000,\n{extra}}}\n",
        base_policy(
            workers,
            r#"return "allow""#,
            "max_tokens = 2000000,",
            "",
        )
    )
}

fn scripted_reply(body: &str) -> DecisionReply {
    let raw: &[u8] = if body.contains("clash-goal") {
        br#"{"p_success":1,"conflicts":["c1"],"usage":{"total_tokens":2}}"#
    } else if body.contains("zero-p") {
        br#"{"score":0,"usage":{"total_tokens":2}}"#
    } else if body.contains("via-predicate") {
        br#"{"predicate":true,"usage":{"total_tokens":2}}"#
    } else if body.contains("via-noul") {
        br#"{"noul":1,"usage":{"total_tokens":2}}"#
    } else {
        br#"{"p_success":1,"conflicts":[],"usage":{"total_tokens":2}}"#
    };
    DecisionReply::Body(raw.to_vec())
}

fn wait_bodies(fake: &FakeDecision, pred: impl Fn(&[DecisionHit]) -> bool) -> Vec<DecisionHit> {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let got = fake.hits.lock().unwrap().clone();
        if pred(&got) || Instant::now() > deadline {
            return got;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn decision_sends_headers_and_caches() {
    let fake = fake_decision(Arc::new(scripted_reply));
    let home = scratch("decide");
    fs::create_dir_all(home.join("registry/recipes/leak")).unwrap();
    fs::write(home.join("registry/recipes/leak/run"), "SECRET_RECIPE_BODY").unwrap();
    fs::create_dir_all(home.join("runs")).unwrap();
    fs::write(home.join("runs/nope.log"), "SECRET_TRANSCRIPT").unwrap();
    fs::create_dir_all(home.join("scratch/other")).unwrap();
    fs::write(home.join("scratch/other/note"), "SECRET_SCRATCH").unwrap();
    policy(
        &home,
        &decision_policy(
            r#"quick = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            fake.port,
            "",
        ),
    );
    let daemon = start(&home);
    inlet(&home, &["post", "board-hello-9"]);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "seed-sample",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "seed-sample" && t["state"] == "done")
    });
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "with-samples",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let bodies = wait_bodies(&fake, |hits| {
        hits.iter().any(|h| h.body.contains("with-samples"))
    });
    let seed = bodies
        .iter()
        .find(|h| h.body.contains("seed-sample"))
        .unwrap_or_else(|| panic!("no seed call: {bodies:?} {}", daemon.log()));
    assert_eq!(seed.path, "/decide");
    assert!(seed.body.contains("\"human_weight\":4"), "{}", seed.body);
    assert!(seed.body.contains("\"constraints\":[]"), "{}", seed.body);
    assert!(seed.body.contains("board-hello-9"), "{}", seed.body);
    assert!(seed.body.contains("\"worker\":\"quick\""), "{}", seed.body);
    assert!(!seed.body.contains("SECRET_"), "{}", seed.body);
    assert!(!seed.body.contains("transcript"), "{}", seed.body);
    let sampled = bodies
        .iter()
        .find(|h| h.body.contains("with-samples"))
        .unwrap();
    assert!(
        sampled.body.contains("\"samples\":[0]"),
        "samples missing: {}",
        sampled.body
    );
    let before = fake.hits.lock().unwrap().len();
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "same-nap",
            "--no-verify",
            "--tokens",
            "30",
            "--seconds",
            "20",
        ],
    );
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "same-nap",
            "--no-verify",
            "--tokens",
            "30",
            "--seconds",
            "20",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .filter(|t| t["goal"] == "same-nap" && t["state"] == "running")
            .count()
            == 2
    });
    let naps = fake
        .hits
        .lock()
        .unwrap()
        .iter()
        .filter(|h| h.body.contains("same-nap"))
        .count();
    assert_eq!(naps, 1, "cached call ran twice, before {before}");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "parent-header",
            "--verify",
            "true",
            "--tokens",
            "400",
            "--seconds",
            "30",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "parent-header" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let parent = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "parent-header")
        .unwrap();
    let token = token_of(parent["pid"].as_i64().unwrap());
    let spawned = worker_rpc(
        &home,
        serde_json::json!({
            "op": "spawn",
            "token": token,
            "worker": "quick",
            "goal": "child-header",
            "verify": "true",
            "tokens": 30,
            "seconds": 10,
            "memory_mb": 32,
            "pids": 2,
        }),
    );
    assert_eq!(spawned["ok"], true, "{spawned}");
    let bodies = wait_bodies(&fake, |hits| {
        hits.iter().any(|h| h.body.contains("child-header"))
    });
    let child = bodies
        .iter()
        .find(|h| h.body.contains("child-header"))
        .unwrap();
    assert!(
        child.body.contains("parent-header"),
        "parent header missing: {}",
        child.body
    );
    assert!(child.body.contains("\"parents\":[{"), "{}", child.body);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "clash-goal",
            "--verify",
            "true",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "clash-goal" && t["state"] == "failed")
    });
    let clash = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "clash-goal")
        .unwrap();
    assert_eq!(clash["reason"], "constraint", "{clash}");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "zero-p",
            "--verify",
            "true",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "zero-p" && t["state"] == "failed")
    });
    let zero = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "zero-p")
        .unwrap();
    assert_eq!(zero["reason"], "ev", "{zero}");
    for goal in ["via-predicate", "via-noul"] {
        inlet(
            &home,
            &[
                "add",
                "--worker",
                "quick",
                "--goal",
                goal,
                "--no-verify",
                "--tokens",
                "250000",
                "--seconds",
                "10",
            ],
        );
        let st = wait_status(&home, |v| {
            tasks(v).iter().any(|t| {
                t["goal"] == goal && matches!(t["state"].as_str(), Some("done" | "failed"))
            })
        });
        let task = tasks(&st).into_iter().find(|t| t["goal"] == goal).unwrap();
        assert_eq!(
            task["state"],
            "done",
            "p was not applied: {task} {}",
            daemon.log()
        );
    }
}

#[test]
fn decision_down_lets_the_verifier_finish() {
    let open = Arc::new(AtomicBool::new(true));
    let flag = open.clone();
    let fake = fake_decision(Arc::new(move |_| {
        if !flag.load(Ordering::SeqCst) {
            return DecisionReply::Drop;
        }
        DecisionReply::Body(br#"{"p_success":1,"usage":{"total_tokens":2}}"#.to_vec())
    }));
    let home = scratch("decide-down");
    policy(
        &home,
        &decision_policy(
            r#"paced = { cmd = { "/bin/sleep", "1" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            fake.port,
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "paced",
            "--goal",
            "finish-me",
            "--verify",
            "true",
            "--tokens",
            "40",
            "--seconds",
            "8",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "finish-me" && t["state"] == "running")
    });
    open.store(false, Ordering::SeqCst);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "finish-me" && t["state"] == "done")
    });
    let done = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "finish-me")
        .unwrap();
    assert_eq!(done["reason"], "ok", "{done} {}", daemon.log());
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "after-down",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "after-down" && t["state"] == "failed")
    });
    let denied = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "after-down")
        .unwrap();
    assert_eq!(denied["reason"], "gate", "{denied}");
}

#[test]
fn empty_decision_purse_does_not_call() {
    let fake = fake_decision(Arc::new(|_| {
        DecisionReply::Body(br#"{"p_success":1}"#.to_vec())
    }));
    let home = scratch("decide-empty");
    policy(
        &home,
        &decision_policy("", fake.port, "  purse_tokens = 0,\n"),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "no-purse",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "no-purse" && t["state"] == "failed")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "no-purse")
        .unwrap();
    assert_eq!(task["reason"], "gate", "{task} {}", daemon.log());
    assert!(
        fake.hits.lock().unwrap().is_empty(),
        "empty purse still called"
    );
    assert_eq!(st["gate_spent"].as_u64(), Some(0));
}

#[test]
fn decision_timeout_denies() {
    let fake = fake_decision(Arc::new(|_| DecisionReply::Hang));
    let home = scratch("decide-slow");
    policy(
        &home,
        &decision_policy("", fake.port, "  timeout_ms = 300,\n"),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "too-slow",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "too-slow" && t["state"] == "failed")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "too-slow")
        .unwrap();
    assert_eq!(task["reason"], "gate", "{task} {}", daemon.log());
    assert!(task["pid"].is_null(), "timeout still spawned {task}");
}

#[test]
fn decision_spend_survives_kill9() {
    let fake = fake_decision(Arc::new(|_| {
        DecisionReply::Body(br#"{"p_success":1,"usage":{"total_tokens":40}}"#.to_vec())
    }));
    let home = scratch("decide-spend");
    policy(
        &home,
        &decision_policy(
            r#"quick = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            fake.port,
            "  purse_tokens = 40,\n",
        ),
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "once",
            "--no-verify",
            "--tokens",
            "15",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "once" && t["state"] == "done")
    });
    assert_eq!(st["gate_spent"].as_u64(), Some(40), "{st}");
    let id = tasks(&st).iter().find(|t| t["goal"] == "once").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let admits = read_log(&home)
        .into_iter()
        .filter(|r| {
            matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id: i, .. } if i == &id))
        })
        .count();
    assert_eq!(admits, 1);
    let pid = pid_of(&home).unwrap();
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = daemon.child.wait();
    let daemon = start(&home);
    let st = status(&home);
    assert_eq!(st["gate_spent"].as_u64(), Some(40), "{st} {}", daemon.log());
    assert_eq!(fake.hits.lock().unwrap().len(), 1);
    let admits = read_log(&home)
        .into_iter()
        .filter(|r| {
            matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id: i, .. } if i == &id))
        })
        .count();
    assert_eq!(admits, 1, "kill -9 admitted the slice again");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "twice",
            "--no-verify",
            "--tokens",
            "15",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "twice" && t["state"] == "failed")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "twice")
        .unwrap();
    assert_eq!(task["reason"], "gate", "{task}");
    assert_eq!(st["gate_spent"].as_u64(), Some(40), "{st}");
    assert_eq!(
        fake.hits.lock().unwrap().len(),
        1,
        "down purse called again"
    );
}

fn tty_inlet(home: &Path, args: &[&str], passphrase: &str) -> (bool, String) {
    use std::os::unix::io::FromRawFd;
    use std::os::unix::process::CommandExt;
    let mut master: i32 = 0;
    let mut slave: i32 = 0;
    let rc = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert_eq!(rc, 0, "openpty");
    let slave_fd = slave;
    let mut cmd = Command::new(bin());
    cmd.arg("--home").arg(home).args(args);
    unsafe {
        cmd.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::ioctl(slave_fd, libc::TIOCSCTTY, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let slave_in = unsafe { File::from_raw_fd(slave) };
    let slave_out = slave_in.try_clone().unwrap();
    let slave_err = slave_in.try_clone().unwrap();
    cmd.stdin(Stdio::from(slave_in))
        .stdout(Stdio::from(slave_out))
        .stderr(Stdio::from(slave_err));
    let mut child = cmd.spawn().unwrap();
    unsafe {
        let flags = libc::fcntl(master, libc::F_GETFL);
        libc::fcntl(master, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    let mut master_file = unsafe { File::from_raw_fd(master) };
    let mut seen = String::new();
    let mut buf = [0u8; 256];
    let deadline = Instant::now() + Duration::from_secs(5);
    while !seen.contains("passphrase:") && Instant::now() < deadline {
        match master_file.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => seen.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
    master_file
        .write_all(format!("{passphrase}\n").as_bytes())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return (false, format!("tty timeout\n{seen}"));
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut out = seen;
    loop {
        match master_file.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => out.push_str(&String::from_utf8_lossy(&buf[..n])),
        }
    }
    (status.success(), out)
}

fn inlet_raw(home: &Path, args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap()
}

fn expect_refuse(home: &Path) {
    let _ = fs::remove_file(home.join("run/operator.sock"));
    let _ = fs::remove_file(home.join("run/inlet.pid"));
    let log_path = home.join("refuse.log");
    let log = File::create(&log_path).unwrap();
    let err = log.try_clone().unwrap();
    let mut child = Command::new(bin())
        .args(["--home", home.to_str().unwrap(), "up", "-f"])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            let text = fs::read_to_string(&log_path).unwrap_or_default();
            panic!("tampered policy stayed up: {text}");
        }
        thread::sleep(Duration::from_millis(30));
    };
    let text = fs::read_to_string(&log_path).unwrap_or_default();
    assert!(!status.success(), "tampered policy started: {text}");
    assert!(
        text.contains("signature") || text.contains("not signed"),
        "{text}"
    );
}

#[test]
fn signed_policy_ignores_unsigned_edits() {
    let home = scratch("sign");
    policy(
        &home,
        &base_policy("", r#"return "allow""#, "max_tokens = 1000,", ""),
    );
    let argv = inlet_raw(&home, &["init", "keyboard-cat"]);
    assert!(!argv.status.success(), "passphrase accepted from argv");
    assert!(!home.join("keys/policy.key").exists());
    let (ok, out) = tty_inlet(&home, &["init"], "keyboard-cat");
    assert!(ok, "init failed: {out}");
    assert!(out.contains("pinned"), "{out}");
    let wrapped = fs::read(home.join("keys/policy.key")).unwrap();
    assert!(!wrapped.windows(8).any(|w| w == b"keyboard"));
    let daemon = start(&home);
    assert_eq!(status(&home)["cap"].as_u64(), Some(1000));
    let mut tampered = fs::read_to_string(home.join("policy.lua")).unwrap();
    tampered = tampered.replace("max_tokens = 1000", "max_tokens = 9000");
    fs::write(home.join("policy.lua"), &tampered).unwrap();
    assert_eq!(
        status(&home)["cap"].as_u64(),
        Some(1000),
        "unsigned edit applied"
    );
    let peek = inlet_raw(
        &home,
        &[
            "add",
            "--worker",
            "peek",
            "--goal",
            "look",
            "--no-verify",
            "--tokens",
            "20",
        ],
    );
    assert!(!peek.status.success(), "unsigned worker was admitted");
    let draft = r#"
caps = { max_live = 4, max_depth = 3, max_tokens = 5000, max_memory_mb = 8192, max_pids = 64, token_period = "1d" }
setup = "box"
isolator = "rlimit"
human_weight = 9
min_ev = 0
value = 400000
debug = 1
preamble = "SIGNED-PREAMBLE {id}\n"
default_budget = { tokens = 200, seconds = 30, memory_mb = 64, pids = 8 }
workers = {
  sleeper = { cmd = { "/bin/sleep", "30" }, tags = { "code" }, net = "host", on_crash = "fail" },
  peek = { cmd = { "/bin/sh", "-c", "cp /etc/preamble /work/seen" }, tags = { "code" }, net = "host", on_crash = "fail" },
}
function admit(ctx)
  if ctx.goal == "lua-no" then return "deny" end
  return "allow"
end
"#;
    let mut child = Command::new(bin())
        .arg("--home")
        .arg(&home)
        .arg("draft")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(draft.as_bytes())
        .unwrap();
    let drafted = child.wait_with_output().unwrap();
    assert!(drafted.status.success(), "{drafted:?}");
    let diff = inlet(&home, &["diff"]);
    let diff_text = String::from_utf8_lossy(&diff.stdout);
    assert!(
        diff_text.contains("SIGNED-PREAMBLE") || diff_text.contains("5000"),
        "{diff_text}"
    );
    let (bad, bad_out) = tty_inlet(&home, &["sign"], "wrong-horse");
    assert!(!bad, "wrong passphrase signed: {bad_out}");
    assert_eq!(status(&home)["cap"].as_u64(), Some(1000));
    let (signed, signed_out) = tty_inlet(&home, &["sign"], "keyboard-cat");
    assert!(signed, "sign failed: {signed_out} {}", daemon.log());
    let st = status(&home);
    assert_eq!(st["cap"].as_u64(), Some(5000), "{st}");
    say(&home, "weight-check");
    thread::sleep(Duration::from_millis(200));
    let weight = read_log(&home).into_iter().find_map(|rec| match rec {
        Decoded::Rec(r) => match r.as_ref() {
            Record::Post { text, weight, .. } if text.contains("weight-check") => Some(*weight),
            _ => None,
        },
        _ => None,
    });
    assert_eq!(weight, Some(9), "human weight was not in the signed policy");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "peek",
            "--goal",
            "lua-no",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "lua-no" && t["state"] == "failed")
    });
    let denied = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "lua-no")
        .unwrap();
    assert_eq!(denied["reason"], "lua", "{denied}");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "peek",
            "--goal",
            "show-preamble",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "show-preamble" && t["state"] == "done")
    });
    let id = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "show-preamble")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let seen = fs::read_to_string(home.join("work").join(id).join("seen")).unwrap_or_default();
    assert!(
        seen.contains("SIGNED-PREAMBLE"),
        "preamble {seen} {}",
        daemon.log()
    );
    drop(daemon);
    let mut bytes = fs::read(home.join("policy.lua")).unwrap();
    bytes.push(b' ');
    fs::write(home.join("policy.lua"), bytes).unwrap();
    expect_refuse(&home);
}

#[test]
fn constraints_bind_until_a_signed_clear() {
    let home = scratch("bind");
    policy(
        &home,
        &base_policy(
            r#"prover = { cmd = { "/bin/true" }, tags = { "math" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 2000,",
            "",
        ),
    );
    let daemon = start(&home);
    let bare = inlet_raw(&home, &["bind", "no tag here"]);
    assert!(!bare.status.success(), "tagless constraint was kept");
    assert!(
        !read_log(&home)
            .iter()
            .any(|r| matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Bind { .. }))),
        "tagless bind was recorded"
    );
    let bound = inlet(&home, &["bind", "stay out #code"]);
    let id = String::from_utf8_lossy(&bound.stdout).trim().to_string();
    assert!(!id.is_empty(), "bind id missing");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "coded",
            "--no-verify",
            "--tokens",
            "30",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "coded" && t["state"] == "failed")
    });
    assert_eq!(
        tasks(&st).iter().find(|t| t["goal"] == "coded").unwrap()["reason"],
        "constraint"
    );
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "prover",
            "--goal",
            "mathy",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "mathy" && t["state"] == "done")
    });
    let sock = UnixStream::connect(home.join("run/operator.sock")).unwrap();
    let mut sock = BufReader::new(sock);
    let line = format!("{{\"op\":\"clear\",\"id\":{id:?},\"passphrase\":\"keyboard-cat\"}}\n");
    sock.get_mut().write_all(line.as_bytes()).unwrap();
    let mut buf = String::new();
    sock.read_line(&mut buf).unwrap();
    assert!(
        buf.contains("tty"),
        "passphrase accepted on the socket: {buf}"
    );
    assert!(status(&home)["constraints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["id"] == id));
    let (made, made_out) = tty_inlet(&home, &["init"], "keyboard-cat");
    assert!(made, "init failed: {made_out}");
    let (cleared, clear_out) = tty_inlet(&home, &["clear", &id], "keyboard-cat");
    assert!(cleared, "clear failed: {clear_out} {}", daemon.log());
    assert!(status(&home)["constraints"].as_array().unwrap().is_empty());
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "after-clear",
            "--no-verify",
            "--tokens",
            "30",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "after-clear" && t["state"] == "running")
    });
    assert_ne!(
        tasks(&st)
            .iter()
            .find(|t| t["goal"] == "after-clear")
            .unwrap()["reason"],
        "constraint"
    );
}

#[test]
fn constraint_and_purse_survive_kill9() {
    let home = scratch("bind9");
    policy(
        &home,
        &base_policy("", r#"return "allow""#, "max_tokens = 1000,", ""),
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "nap",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "30",
        ],
    );
    let st = wait_status(&home, |v| v["live"].as_u64() == Some(1));
    assert_eq!(st["spent"].as_u64(), Some(100));
    inlet(&home, &["bind", "stay out #code"]);
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let pid = pid_of(&home).unwrap();
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = daemon.child.wait();
    let daemon = start(&home);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == id && t["state"] == "failed")
    });
    assert_eq!(st["spent"].as_u64(), Some(100), "{}", daemon.log());
    let admits = read_log(&home)
        .into_iter()
        .filter(|r| {
            matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id: i, .. } if i == &id))
        })
        .count();
    assert_eq!(admits, 1, "kill -9 admitted the slice again");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "still-bound",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "still-bound" && t["state"] == "failed")
    });
    assert_eq!(
        tasks(&st)
            .iter()
            .find(|t| t["goal"] == "still-bound")
            .unwrap()["reason"],
        "constraint"
    );
}

#[test]
fn endpoint_judges_the_constraint() {
    let fake = fake_decision(Arc::new(|body: &str| {
        if body.contains("clash-bind") {
            let value: Value = serde_json::from_str(body).unwrap_or(Value::Null);
            let ids: Vec<&str> = value
                .get("constraints")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|c| c.get("id").and_then(|s| s.as_str()))
                        .collect()
                })
                .unwrap_or_default();
            let conflicts = serde_json::to_string(&ids).unwrap_or_else(|_| "[]".into());
            return DecisionReply::Body(
                format!(
                    r#"{{"p_success":1,"conflicts":{conflicts},"usage":{{"total_tokens":2}}}}"#
                )
                .into_bytes(),
            );
        }
        DecisionReply::Body(
            br#"{"p_success":1,"conflicts":[],"usage":{"total_tokens":2}}"#.to_vec(),
        )
    }));
    let home = scratch("bind-judge");
    policy(&home, &decision_policy("", fake.port, ""));
    let daemon = start(&home);
    let bound = inlet(&home, &["bind", "judge this please"]);
    let id = String::from_utf8_lossy(&bound.stdout).trim().to_string();
    assert!(
        !id.is_empty(),
        "tagless bind refused while the endpoint is on"
    );
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "ok-bind",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "ok-bind" && t["state"] == "running")
    });
    assert_ne!(
        tasks(&st).iter().find(|t| t["goal"] == "ok-bind").unwrap()["reason"],
        "constraint"
    );
    let bodies = wait_bodies(&fake, |hits| {
        hits.iter().any(|h| h.body.contains("ok-bind"))
    });
    let call = bodies.iter().find(|h| h.body.contains("ok-bind")).unwrap();
    assert!(call.body.contains("judge this please"), "{}", call.body);
    assert!(call.body.contains("\"human_weight\":4"), "{}", call.body);
    assert!(!call.body.contains("SECRET_"), "{}", call.body);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "clash-bind",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "clash-bind" && t["state"] == "failed")
    });
    assert_eq!(
        tasks(&st)
            .iter()
            .find(|t| t["goal"] == "clash-bind")
            .unwrap()["reason"],
        "constraint",
        "{st} {}",
        daemon.log()
    );
}

#[test]
fn operator_skill_is_the_same_thin_wrapper() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let pi = fs::read(root.join("skills/pi/SKILL.md")).unwrap();
    let wizard = fs::read(root.join("skills/wizard/SKILL.md")).unwrap();
    assert_eq!(pi, wizard, "pi and wizard skills diverged");
    let text = String::from_utf8(pi).unwrap();
    for line in [
        "You are on the inlet operator socket, outside the workers.",
        "Use the inlet CLI for status, add, post, bind, kill, budget, pin, diff, and watch.",
        "inlet watch --debug N",
        "You do not carry a message between them.",
        "You cannot sign.",
        "edit policy.draft.lua",
        "inlet diff",
        "The human signs.",
        "Use `--no-verify` only when the human asked.",
        "Do not enter a cell.",
        "The socket is the door.",
    ] {
        assert!(text.contains(line), "skill missing {line}");
    }
    assert!(
        !text.contains("inlet sign"),
        "skill tells the agent to sign"
    );
    assert!(
        !text.contains("inlet clear"),
        "skill tells the agent to clear"
    );
    assert!(
        !text.contains("inlet shell"),
        "skill tells the agent to shell in"
    );
    let wrap_pi = fs::read(root.join("skills/pi/inlet")).unwrap();
    let wrap_wizard = fs::read(root.join("skills/wizard/inlet")).unwrap();
    assert_eq!(wrap_pi, wrap_wizard);
    let wrap = String::from_utf8(wrap_pi).unwrap();
    assert!(wrap.contains("exec inlet"), "{wrap}");
    assert!(!wrap.contains("sign"), "wrapper grew a signing path");
}

#[test]
fn empty_daemon_rss_when_asked() {
    if std::env::var("INLET_RSS").ok().as_deref() != Some("1") {
        return;
    }
    let home = scratch("rss");
    policy(
        &home,
        &base_policy("", r#"return "deny""#, "max_live = 0,", ""),
    );
    let daemon = start(&home);
    let rss = rss_kb(pid_of(&home).unwrap());
    assert!(rss < 30_000, "empty RSS {rss} kB");
    drop(daemon);

    let home = scratch("rss10k");
    policy(
        &home,
        &base_policy("", r#"return "deny""#, "max_live = 0,", ""),
    );
    {
        let opened = ledger::open(&home.join("ledger/log")).unwrap();
        let mut led = opened.ledger;
        let mut batch = Vec::new();
        for i in 0..10_000 {
            batch.push(Record::Task {
                id: format!("t{i:05}"),
                parent: None,
                worker: "sleeper".into(),
                tags: vec!["code".into()],
                goal: "g".into(),
                verifier: None,
                value: 1,
                budget: inlet::model::Budget {
                    tokens: 1,
                    seconds: 1,
                    memory_mb: 64,
                    pids: 1,
                },
                retry_of: None,
                recipe: None,
                ts: 1,
            });
        }
        led.append_all(&batch, true).unwrap();
    }
    let daemon = start(&home);
    let _ = status(&home);
    let rss = rss_kb(pid_of(&home).unwrap());
    assert!(rss < 80_000, "10k header RSS {rss} kB");
    let _ = daemon;
}

fn rss_kb(pid: i32) -> u64 {
    let text = fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next().unwrap().parse().unwrap();
        }
    }
    panic!("no VmRSS");
}

fn parse_snap(out: &std::process::Output) -> (u64, String) {
    let text = String::from_utf8_lossy(&out.stdout);
    let mut parts = text.split_whitespace();
    assert_eq!(parts.next(), Some("snap"), "{text}");
    let offset = parts
        .next()
        .unwrap_or("0")
        .parse()
        .unwrap_or_else(|_| panic!("offset in {text}"));
    let sha = parts.next().unwrap_or("").to_string();
    assert_eq!(sha.len(), 40, "{text}");
    (offset, sha)
}

fn git_out(home: &Path, args: &[&str]) -> String {
    let mut cmd = Command::new("git");
    cmd.arg("--git-dir").arg(home.join("snap.git")).args(args);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "git {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn folded(home: &Path) -> (u64, u64, u64, u64) {
    let policy = inlet::config::Policy::load(&home.join("policy.lua")).unwrap();
    let opened = ledger::open(&home.join("ledger/log")).unwrap();
    let mut state = inlet::state::State::new(&policy.cfg);
    for decoded in &opened.records {
        if let Decoded::Rec(rec) = decoded {
            state.apply(rec);
        }
    }
    (
        state.purse.spent(),
        state.purse.available,
        state.purse.held(),
        state.purse.resets,
    )
}

#[test]
fn snap_replays_from_the_offset() {
    let fake = fake_decision(Arc::new(scripted_reply));
    let home = scratch("snap");
    fs::create_dir_all(home.join("keys")).unwrap();
    fs::write(home.join("keys/policy.key"), "SECRETKEY\n").unwrap();
    fs::create_dir_all(home.join("registry/recipes/keep")).unwrap();
    fs::write(home.join("registry/recipes/keep/run"), "echo ok\n").unwrap();
    policy(
        &home,
        &decision_policy(
            r#"quick = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            fake.port,
            "",
        ),
    );
    let mut daemon = start(&home);
    inlet(&home, &["post", "board-hello-snap"]);
    inlet(&home, &["bind", "leave this #frozen"]);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "before-snap",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "before-snap" && t["state"] == "done")
    });
    let (offset, sha) = parse_snap(&inlet(&home, &["snap"]));
    assert!(offset > 64, "{offset}");
    let st = status(&home);
    assert_eq!(st["snap_offset"].as_u64(), Some(offset));
    assert_eq!(st["lease"].as_bool(), Some(true));
    assert_eq!(st["fence"].as_u64(), Some(0));
    assert!(st["constraints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["text"] == "leave this #frozen"));
    let names = git_out(&home, &["ls-tree", "-r", "--name-only", "HEAD"]);
    for want in [
        "policy.lua",
        "snap/index.json",
        "lease.json",
        "registry/recipes/keep/run",
    ] {
        assert!(names.contains(want), "{names}");
    }
    assert!(!names.contains("policy.key"), "{names}");
    assert!(!names.contains("ledger"), "{names}");
    assert_eq!(
        git_out(&home, &["rev-list", "--count", "HEAD"]).trim(),
        "1",
        "the live log is not a commit per event"
    );
    assert_eq!(git_out(&home, &["rev-parse", "HEAD"]).trim(), sha);
    assert!(
        git_out(&home, &["tag", "-l"]).contains(&format!("offset-{offset}")),
        "missing offset tag"
    );
    let index = fs::read_to_string(home.join("snap/index.json")).unwrap();
    assert!(index.contains("board-hello-snap"), "{index}");
    let spent = st["spent"].clone();
    let available = st["available"].clone();
    let held = st["held"].clone();
    let resets = st["resets"].clone();
    stop(&mut daemon);
    let mut buf = fs::read(home.join("ledger/log")).unwrap();
    assert!(buf.len() as u64 >= offset);
    buf[12] ^= 0xff;
    fs::write(home.join("ledger/log"), &buf).unwrap();
    assert!(
        ledger::open(&home.join("ledger/log")).is_err(),
        "a full scan of the damaged prefix must refuse"
    );
    let daemon = start(&home);
    let st = status(&home);
    assert_eq!(st["snap_offset"].as_u64(), Some(offset), "{}", daemon.log());
    assert_eq!(st["spent"], spent);
    assert_eq!(st["available"], available);
    assert_eq!(st["held"], held);
    assert_eq!(st["resets"], resets);
    assert!(tasks(&st)
        .iter()
        .any(|t| t["goal"] == "before-snap" && t["state"] == "done"));
    assert!(st["constraints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["text"] == "leave this #frozen"));
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "after-snap",
            "--no-verify",
            "--tokens",
            "20",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "after-snap" && t["state"] == "done")
    });
    let hits = wait_bodies(&fake, |hits| {
        hits.iter().any(|hit| hit.body.contains("after-snap"))
    });
    let body = hits
        .iter()
        .find(|hit| hit.body.contains("after-snap"))
        .unwrap();
    assert!(
        body.body.contains("board-hello-snap"),
        "post window did not survive the snapshot: {}",
        body.body
    );
    drop(daemon);
    let daemon = start(&home);
    let st = status(&home);
    assert!(tasks(&st)
        .iter()
        .any(|t| t["goal"] == "before-snap" && t["state"] == "done"));
    assert!(tasks(&st)
        .iter()
        .any(|t| t["goal"] == "after-snap" && t["state"] == "done"));
    assert_eq!(st["snap_offset"].as_u64(), Some(offset));
    let _ = daemon;
}

#[test]
fn snap_kill9_purse_matches_the_ledger() {
    let home = scratch("snapkill");
    policy(
        &home,
        &base_policy("", r#"return "allow""#, "max_tokens = 1000,", ""),
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "nap",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "30",
        ],
    );
    let st = wait_status(&home, |v| v["live"].as_u64() == Some(1));
    assert_eq!(st["held"].as_u64(), Some(100));
    let (offset, _) = parse_snap(&inlet(&home, &["snap"]));
    assert!(offset > 0);
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    unsafe {
        libc::kill(pid_of(&home).unwrap(), libc::SIGKILL);
    }
    let _ = daemon.child.wait();
    let daemon = start(&home);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == id && t["state"] == "failed")
    });
    assert_eq!(st["spent"].as_u64(), Some(100), "{}", daemon.log());
    assert_eq!(st["held"].as_u64(), Some(0));
    assert_eq!(st["available"].as_u64(), Some(900));
    assert_eq!(task_reason(&st, &id), "crash");
    let purse = folded(&home);
    assert_eq!(
        (
            st["spent"].as_u64().unwrap(),
            st["available"].as_u64().unwrap(),
            st["held"].as_u64().unwrap(),
            st["resets"].as_u64().unwrap(),
        ),
        purse,
        "purse diverged from a full scan"
    );
    let admits = read_log(&home)
        .into_iter()
        .filter(|r| {
            matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { id: i, .. } if i == &id))
        })
        .count();
    assert_eq!(admits, 1);
    drop(daemon);
    let daemon = start(&home);
    let st = status(&home);
    assert_eq!(
        st["spent"].as_u64(),
        Some(100),
        "second restart double-spent"
    );
    assert_eq!(st["available"].as_u64(), Some(900));
    assert_eq!(st["held"].as_u64(), Some(0));
    let purse = folded(&home);
    assert_eq!(
        (
            st["spent"].as_u64().unwrap(),
            st["available"].as_u64().unwrap(),
            st["held"].as_u64().unwrap(),
            st["resets"].as_u64().unwrap(),
        ),
        purse
    );
    let _ = daemon;
}

fn task_reason<'a>(st: &'a Value, id: &str) -> &'a str {
    tasks(st)
        .into_iter()
        .find(|t| t["id"] == id)
        .and_then(|t| t["reason"].as_str())
        .unwrap_or("")
}

#[test]
fn losing_fence_does_not_spend() {
    let home = scratch("fence");
    policy(
        &home,
        &base_policy(
            r#"quick = { cmd = { "/bin/true" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let mut daemon = start(&home);
    let before = status(&home);
    assert_eq!(before["lease"].as_bool(), Some(true));
    assert_eq!(before["fence"].as_u64(), Some(0));
    stop(&mut daemon);
    let _ = parse_snap(&inlet(&home, &["snap"]));
    {
        let mut opened = ledger::open(&home.join("ledger/log")).unwrap();
        let id = "stolen-fence".to_string();
        opened
            .ledger
            .append(
                &Record::Task {
                    id: id.clone(),
                    parent: None,
                    worker: "sleeper".into(),
                    tags: vec!["code".into()],
                    goal: "stolen".into(),
                    verifier: None,
                    value: 1,
                    budget: inlet::model::Budget {
                        tokens: 400,
                        seconds: 30,
                        memory_mb: 64,
                        pids: 8,
                    },
                    retry_of: None,
                    recipe: None,
                    ts: 1,
                },
                true,
            )
            .unwrap();
        opened
            .ledger
            .append(
                &Record::Admit {
                    id,
                    fence: 99,
                    tokens: 400,
                    seconds: 30,
                    memory_mb: 64,
                    pids: 8,
                    ts: 2,
                },
                true,
            )
            .unwrap();
    }
    let daemon = start(&home);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "stolen" && t["state"] == "failed")
    });
    assert_eq!(
        task_reason(&st, "stolen-fence"),
        "fence",
        "{}",
        daemon.log()
    );
    assert_eq!(st["spent"].as_u64(), Some(0));
    assert_eq!(st["available"].as_u64(), Some(1000));
    assert_eq!(st["held"].as_u64(), Some(0));
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "honest",
            "--no-verify",
            "--tokens",
            "40",
            "--seconds",
            "10",
        ],
    );
    wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "honest" && t["state"] == "done")
    });
    let st = status(&home);
    assert_eq!(st["spent"].as_u64(), Some(0), "stolen fence stuck a debit");
    assert_eq!(task_reason(&st, "stolen-fence"), "fence");
    let _ = daemon;
}

#[test]
fn follower_pulls_the_snapshot_and_does_not_admit() {
    let home = scratch("leader");
    policy(
        &home,
        &base_policy("", r#"return "allow""#, "max_tokens = 1000,", ""),
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "nap",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "30",
        ],
    );
    let st = wait_status(&home, |v| v["live"].as_u64() == Some(1));
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    unsafe {
        libc::kill(pid_of(&home).unwrap(), libc::SIGKILL);
    }
    let _ = daemon.child.wait();
    let mut daemon = start(&home);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == id && t["state"] == "failed")
    });
    assert_eq!(st["spent"].as_u64(), Some(100));
    assert_eq!(folded(&home), (100, 900, 0, st["resets"].as_u64().unwrap()));
    inlet(&home, &["snap"]);
    assert_eq!(status(&home)["lease"].as_bool(), Some(true));
    stop(&mut daemon);
    let home_b = scratch("follower");
    fs::remove_dir_all(&home_b).unwrap();
    let clone = Command::new("git")
        .args([
            "clone",
            home.join("snap.git").to_str().unwrap(),
            home_b.to_str().unwrap(),
        ])
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .unwrap();
    assert!(
        clone.status.success(),
        "{}",
        String::from_utf8_lossy(&clone.stderr)
    );
    assert!(!home_b.join("ledger/log").exists());
    assert!(!home_b.join("keys/policy.key").exists());
    fs::create_dir_all(home_b.join("run")).unwrap();
    fs::copy(home_b.join("lease.json"), home_b.join("run/lease")).unwrap();
    let daemon = start(&home_b);
    let st = status(&home_b);
    assert_eq!(st["lease"].as_bool(), Some(false), "{}", daemon.log());
    assert_eq!(st["spent"].as_u64(), Some(100));
    assert_eq!(st["available"].as_u64(), Some(900));
    assert_eq!(st["held"].as_u64(), Some(0));
    assert!(tasks(&st)
        .iter()
        .any(|t| t["id"] == id && t["reason"] == "crash"));
    inlet(
        &home_b,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "nope",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "30",
        ],
    );
    thread::sleep(Duration::from_millis(600));
    let st = status(&home_b);
    assert!(
        tasks(&st)
            .iter()
            .any(|t| t["goal"] == "nope" && t["state"] == "queued"),
        "{st}"
    );
    assert_eq!(st["spent"].as_u64(), Some(100));
    assert_eq!(st["live"].as_u64(), Some(0));
    let admits = read_log(&home_b)
        .into_iter()
        .filter(|r| matches!(r, Decoded::Rec(b) if matches!(b.as_ref(), Record::Admit { .. })))
        .count();
    assert_eq!(admits, 0);
    drop(daemon);
    let daemon = start(&home_b);
    let st = status(&home_b);
    assert_eq!(
        st["spent"].as_u64(),
        Some(100),
        "follower restart spent the slice"
    );
    assert_eq!(st["available"].as_u64(), Some(900));
    assert!(tasks(&st)
        .iter()
        .any(|t| t["goal"] == "nope" && t["state"] == "queued"));
    assert_eq!(st["lease"].as_bool(), Some(false));
    let _ = daemon;
}

fn cost_sum(home: &Path, id: &str) -> u64 {
    read_log(home)
        .iter()
        .filter_map(|decoded| match decoded {
            Decoded::Rec(rec) => match rec.as_ref() {
                Record::Cost {
                    id: cid, tokens, ..
                } if cid == id => Some(*tokens),
                _ => None,
            },
            _ => None,
        })
        .sum()
}

fn wait_cost(home: &Path, id: &str, want: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut got = 0;
    while Instant::now() < deadline {
        got = cost_sum(home, id);
        if got == want {
            return got;
        }
        thread::sleep(Duration::from_millis(40));
    }
    got
}

fn wait_text(path: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(path) {
            if !text.is_empty() {
                return text;
            }
        }
        thread::sleep(Duration::from_millis(30));
    }
    panic!("missing {}", path.display());
}

fn spawn_upstream(usages: &[&str], delay_ms: u64) -> (u16, Arc<Mutex<Vec<u64>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_bg = seen.clone();
    let usages: Vec<String> = usages.iter().map(|s| (*s).to_string()).collect();
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let usage = usages
                .get(i)
                .cloned()
                .unwrap_or_else(|| usages.last().cloned().unwrap_or_else(|| "{}".into()));
            let seen_bg = seen_bg.clone();
            thread::spawn(move || serve_canned(conn, &usage, delay_ms, &seen_bg));
        }
    });
    (port, seen)
}

fn serve_canned(mut sock: TcpStream, usage: &str, delay_ms: u64, seen: &Mutex<Vec<u64>>) {
    let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while buf.len() < 1024 * 1024 {
        match sock.read(&mut byte) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let header = String::from_utf8_lossy(&buf);
    let len = header
        .lines()
        .find_map(|line| {
            line.split_once(':').and_then(|(k, v)| {
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse::<usize>().ok())
            })
        })
        .flatten()
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    if len > 0 {
        let _ = sock.read_exact(&mut body);
    }
    let want = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v.get("max_tokens").and_then(|n| n.as_u64()))
        .unwrap_or(0);
    seen.lock().unwrap().push(want);
    if delay_ms > 0 {
        thread::sleep(Duration::from_millis(delay_ms));
    }
    let payload = format!(r#"{{"usage":{usage}}}"#);
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = sock.write_all(resp.as_bytes());
}

#[test]
fn verifier_cannot_touch_the_host() {
    let home = scratch("seal");
    let wizard = home.join(".wizard");
    fs::create_dir_all(&wizard).unwrap();
    let ledger = home.join("ledger/log");
    let sock = home.join("run/operator.sock");
    let mark_ledger = home.join("ledger/escape");
    let mark_wizard = wizard.join("escape");
    let mark_sock = home.join("run/sock-escape");
    let evil = format!(
        "touch {}; touch {}; touch {}; echo pwn >> {}; echo pwn >> {}; test \"$(./run)\" = ok",
        mark_ledger.display(),
        mark_wizard.display(),
        mark_sock.display(),
        ledger.display(),
        sock.display()
    );
    let check = format!(
        "cat > /work/check.sh << 'END'\n#!/bin/sh\necho ran > /work/ran\ntouch {}\ntouch {}\ntouch {}\necho pwn >> {}\necho pwn >> {}\nexit 0\nEND\nchmod +x /work/check.sh\n",
        mark_ledger.display(),
        mark_wizard.display(),
        mark_sock.display(),
        ledger.display(),
        sock.display()
    );
    let py = format!(
        "cat > /work/check.py << 'END'\nopen('/work/pyran','w').write('ok\\n')\nimport pathlib\nfor p in [{0:?},{1:?},{2:?},{3:?},{4:?}]:\n    try:\n        pathlib.Path(p).open('a').write('x\\n')\n    except OSError:\n        pass\nEND\n",
        mark_ledger.display().to_string(),
        mark_wizard.display().to_string(),
        mark_sock.display().to_string(),
        ledger.display().to_string(),
        sock.display().to_string()
    );
    policy(
        &home,
        &base_policy(
            &format!(
                r#"quick = {{ cmd = {{ "/bin/true" }}, tags = {{ "code" }}, net = "none", on_crash = "fail" }},
                writer = {{ cmd = {{ "/bin/sh", "-c", {check:?} }}, tags = {{ "code" }}, net = "none", on_crash = "fail" }},
                py = {{ cmd = {{ "/bin/sh", "-c", {py:?} }}, tags = {{ "code" }}, net = "none", on_crash = "fail" }},"#
            ),
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "hold",
            "--no-verify",
            "--tokens",
            "40",
            "--seconds",
            "40",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "sleeper" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let sleeper = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "sleeper")
        .unwrap();
    let token = token_of(sleeper["pid"].as_i64().unwrap());
    let drafted = worker_rpc(
        &home,
        serde_json::json!({
            "op": "draft",
            "token": token,
            "name": "evil",
            "run": "#!/bin/sh\necho ok\n",
            "verifier": evil,
        }),
    );
    assert_eq!(drafted["ok"], true, "{drafted} {}", daemon.log());
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "quick",
            "--goal",
            "check",
            "--recipe",
            "evil",
            "--verify",
            "test \"$(./run)\" = ok",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "check" && t["state"] == "done")
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "check")
        .unwrap();
    assert_eq!(task["reason"], "ok", "{st} {}", daemon.log());
    let mut ready = false;
    for author in fs::read_dir(home.join("drafts")).unwrap().flatten() {
        let path = author.path().join("evil/meta.json");
        if let Ok(text) = fs::read_to_string(path) {
            let meta: Value = serde_json::from_str(&text).unwrap();
            ready = meta["ready"] == true;
        }
    }
    assert!(ready, "draft verifier did not pass {}", daemon.log());
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "writer",
            "--goal",
            "script",
            "--verify",
            "sh ./check.sh",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "script" && (t["state"] == "done" || t["state"] == "failed"))
    });
    let task = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "script")
        .unwrap();
    assert_eq!(task["reason"], "ok", "{st} {}", daemon.log());
    let ran = home
        .join("work")
        .join(task["id"].as_str().unwrap())
        .join("ran");
    assert_eq!(fs::read_to_string(&ran).unwrap_or_default().trim(), "ran");
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "py",
            "--goal",
            "py",
            "--verify",
            "python3 -B check.py",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "py" && (t["state"] == "done" || t["state"] == "failed"))
    });
    let task = tasks(&st).into_iter().find(|t| t["goal"] == "py").unwrap();
    assert_eq!(task["reason"], "ok", "{st} {}", daemon.log());
    let pyran = home
        .join("work")
        .join(task["id"].as_str().unwrap())
        .join("pyran");
    assert_eq!(wait_text(&pyran).trim(), "ok");
    assert!(!mark_ledger.exists(), "ledger marker");
    assert!(!mark_wizard.exists(), "wizard marker");
    assert!(!mark_sock.exists(), "socket marker");
    let log = fs::read(&ledger).unwrap_or_default();
    assert!(!log.windows(3).any(|w| w == b"pwn"), "ledger was written");
    assert!(sock.metadata().unwrap().file_type().is_socket());
    let _ = daemon;
}

#[test]
fn cell_cannot_signal_the_host() {
    let mut decoy = Command::new("/bin/sleep").arg("120").spawn().unwrap();
    let decoy_pid = decoy.id();
    let home = scratch("signal");
    let script = format!(
        "kill -0 {decoy_pid}; echo $? > /work/sig; kill -9 -1; echo after > /work/after; sleep 30"
    );
    policy(
        &home,
        &base_policy(
            &format!(
                r#"sig = {{ cmd = {{ "/bin/sh", "-c", {script:?} }}, tags = {{ "code" }}, net = "none", on_crash = "fail" }},"#
            ),
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sig",
            "--goal",
            "signal",
            "--no-verify",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "sig" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let id = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "sig")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let sig = wait_text(&home.join("work").join(&id).join("sig"));
    assert_ne!(sig.trim(), "0", "signalled the host decoy: {sig}");
    assert!(
        Path::new(&format!("/proc/{decoy_pid}")).exists(),
        "decoy died"
    );
    let st = status(&home);
    assert!(st.get("cap").is_some(), "{st} {}", daemon.log());
    thread::sleep(Duration::from_millis(200));
    assert!(
        Path::new(&format!("/proc/{decoy_pid}")).exists(),
        "kill -1 reached the decoy"
    );
    assert!(pid_of(&home).is_some(), "daemon died");
    let _ = decoy.kill();
    let _ = decoy.wait();
    let _ = daemon;
}

#[test]
fn proxy_charges_the_prompt() {
    let (port, _seen) = spawn_upstream(
        &[r#"{"prompt_tokens":400,"completion_tokens":8,"total_tokens":408}"#],
        0,
    );
    let home = scratch("prompt");
    policy(
        &home,
        &base_policy(
            "",
            r#"return "allow""#,
            "max_tokens = 1000,",
            &format!("upstream = \"http://127.0.0.1:{port}\", key = \"test-key\""),
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "prompt",
            "--no-verify",
            "--tokens",
            "800",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let padding = "x".repeat(1600);
    let body =
        format!(r#"{{"max_tokens":8,"messages":[{{"role":"user","content":"{padding}"}}]}}"#);
    let reply = proxy_post_body(&home.join("proxy/proxy.sock"), &token, &body);
    assert!(reply.contains("200"), "{reply}");
    assert!(!reply.contains("empty_purse"), "{reply}");
    assert_eq!(wait_cost(&home, &id, 408), 408, "{}", daemon.log());
    thread::sleep(Duration::from_millis(200));
    let st = status(&home);
    assert_eq!(tasks(&st)[0]["state"], "running", "{st}");
    let _ = daemon;
}

#[test]
fn parallel_calls_share_the_purse() {
    let (port, _seen) = spawn_upstream(&[r#"{"total_tokens":10}"#], 400);
    let home = scratch("share");
    policy(
        &home,
        &base_policy(
            "",
            r#"return "allow""#,
            "max_tokens = 1000,",
            &format!("upstream = \"http://127.0.0.1:{port}\", key = \"test-key\""),
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "share",
            "--no-verify",
            "--tokens",
            "400",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let sock = home.join("proxy/proxy.sock");
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2 {
        let barrier = barrier.clone();
        let sock = sock.clone();
        let token = token.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            proxy_post_body(
                &sock,
                &token,
                r#"{"messages":[{"role":"user","content":"hi"}]}"#,
            )
        }));
    }
    let replies: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    for reply in &replies {
        assert!(reply.contains("200"), "{reply}");
        assert!(!reply.contains("empty_purse"), "{reply}");
    }
    assert_eq!(
        wait_cost(&home, &id, 20),
        20,
        "{replies:?} {}",
        daemon.log()
    );
    thread::sleep(Duration::from_millis(300));
    let st = status(&home);
    assert_eq!(tasks(&st)[0]["state"], "running", "{st}");
    let _ = daemon;
}

#[test]
fn proxy_charges_real_usage() {
    let (port, _seen) = spawn_upstream(&[r#"{"total_tokens":22}"#], 0);
    let home = scratch("real-usage");
    policy(
        &home,
        &base_policy(
            "",
            r#"return "allow""#,
            "max_tokens = 1000,",
            &format!("upstream = \"http://127.0.0.1:{port}\", key = \"test-key\""),
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "usage",
            "--no-verify",
            "--tokens",
            "200",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let reply = proxy_post(&home.join("proxy/proxy.sock"), &token, 10);
    assert!(reply.contains("200"), "{reply}");
    assert_eq!(wait_cost(&home, &id, 22), 22, "{}", daemon.log());
    thread::sleep(Duration::from_millis(200));
    let st = status(&home);
    assert_eq!(tasks(&st)[0]["state"], "running", "{st} {}", daemon.log());
    inlet(&home, &["kill", &id]);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == id && t["reason"] == "killed")
    });
    assert_eq!(st["available"].as_u64(), Some(978), "{st}");
    let exit = read_log(&home)
        .into_iter()
        .find_map(|decoded| match decoded {
            Decoded::Rec(rec) => match rec.as_ref() {
                Record::Exit {
                    id: eid,
                    tokens_used,
                    refund_tokens,
                    reason,
                    ..
                } if eid == &id => Some((*tokens_used, *refund_tokens, reason.clone())),
                _ => None,
            },
            _ => None,
        });
    assert_eq!(exit, Some((22, 178, "killed".into())), "{st}");
    let _ = daemon;
}

#[test]
fn overspend_takes_the_rest_of_the_purse() {
    let (port, _seen) =
        spawn_upstream(&[r#"{"total_tokens":10}"#, r#"{"total_tokens":100000}"#], 0);
    let home = scratch("over");
    policy(
        &home,
        &base_policy(
            "",
            r#"return "allow""#,
            "max_tokens = 1000,",
            &format!("upstream = \"http://127.0.0.1:{port}\", key = \"test-key\""),
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "over",
            "--no-verify",
            "--tokens",
            "50",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let id = tasks(&st)[0]["id"].as_str().unwrap().to_string();
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let sock = home.join("proxy/proxy.sock");
    let first = proxy_post(&sock, &token, 10);
    assert!(first.contains("200"), "{first}");
    assert_eq!(wait_cost(&home, &id, 10), 10, "{}", daemon.log());
    let second = proxy_post(&sock, &token, 10);
    assert!(second.contains("200"), "{second}");
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == id && t["reason"] == "purse")
    });
    assert_eq!(wait_cost(&home, &id, 50), 50, "{st} {}", daemon.log());
    assert_eq!(st["available"].as_u64(), Some(950), "{st}");
    let exit = read_log(&home)
        .into_iter()
        .find_map(|decoded| match decoded {
            Decoded::Rec(rec) => match rec.as_ref() {
                Record::Exit {
                    id: eid,
                    tokens_used,
                    refund_tokens,
                    reason,
                    ..
                } if eid == &id => Some((*tokens_used, *refund_tokens, reason.clone())),
                _ => None,
            },
            _ => None,
        });
    assert_eq!(exit, Some((50, 0, "purse".into())), "{st}");
    let _ = daemon;
}

#[test]
fn child_slice_shrinks_the_parent_proxy() {
    let (port, seen) = spawn_upstream(&[r#"{"total_tokens":0}"#], 0);
    let home = scratch("lend");
    policy(
        &home,
        &base_policy(
            r#"kid = { cmd = { "/bin/sleep", "30" }, tags = { "code" }, net = "none", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            &format!("upstream = \"http://127.0.0.1:{port}\", key = \"test-key\""),
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "parent",
            "--no-verify",
            "--tokens",
            "100",
            "--seconds",
            "30",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "parent" && t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let parent = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "parent")
        .unwrap();
    let parent_id = parent["id"].as_str().unwrap().to_string();
    let token = token_of(parent["pid"].as_i64().unwrap());
    let spawned = worker_rpc(
        &home,
        serde_json::json!({
            "op": "spawn",
            "token": token,
            "worker": "kid",
            "goal": "nap",
            "verify": "/bin/true",
            "tokens": 40,
            "seconds": 10
        }),
    );
    assert_eq!(spawned["ok"], true, "{spawned} {}", daemon.log());
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "kid" && t["state"] == "running")
    });
    let child_id = tasks(&st)
        .into_iter()
        .find(|t| t["worker"] == "kid")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let sock = home.join("proxy/proxy.sock");
    let reply = proxy_post(&sock, &token, 1000);
    assert!(reply.contains("200"), "{reply} {}", daemon.log());
    let capped = seen.lock().unwrap().clone();
    assert_eq!(capped.last().copied(), Some(60), "{capped:?}");
    inlet(&home, &["kill", &child_id]);
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["id"] == parent_id && t["state"] == "running")
    });
    assert_eq!(st["available"].as_u64(), Some(900), "{st}");
    let reply = proxy_post(&sock, &token, 1000);
    assert!(reply.contains("200"), "{reply}");
    let capped = seen.lock().unwrap().clone();
    assert_eq!(capped.last().copied(), Some(100), "{capped:?}");
    let _ = daemon;
}

#[test]
fn seed_lands_in_the_workdir() {
    let home = scratch("seed");
    let seed = home.join("seed-src");
    fs::create_dir_all(seed.join("sub")).unwrap();
    fs::write(seed.join("hello.txt"), "hi\n").unwrap();
    fs::write(seed.join("sub").join("more.txt"), "more\n").unwrap();
    policy(
        &home,
        &base_policy(
            r#"see = { cmd = { "/bin/sh", "-c", "cat /work/hello.txt /work/sub/more.txt > /work/seen" }, tags = { "code" }, net = "none", on_crash = "fail" },"#,
            r#"return "allow""#,
            "max_tokens = 1000,",
            "",
        ),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "see",
            "--goal",
            "seed",
            "--no-verify",
            "--seed",
            seed.to_str().unwrap(),
            "--tokens",
            "40",
            "--seconds",
            "15",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "seed" && t["state"] == "done")
    });
    let id = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "seed")
        .unwrap()["id"]
        .as_str()
        .unwrap();
    let seen = fs::read_to_string(home.join("work").join(id).join("seen")).unwrap_or_default();
    assert_eq!(seen, "hi\nmore\n", "{seen} {}", daemon.log());
    let _ = daemon;
}

#[test]
fn worker_line_posts() {
    let home = scratch("line");
    policy(
        &home,
        &base_policy("", r#"return "allow""#, "max_tokens = 1000,", ""),
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "--worker",
            "sleeper",
            "--goal",
            "line",
            "--no-verify",
            "--tokens",
            "40",
            "--seconds",
            "20",
        ],
    );
    let st = wait_status(&home, |v| {
        tasks(v).iter().any(|t| t["pid"].as_i64().unwrap_or(0) > 0)
    });
    let token = token_of(tasks(&st)[0]["pid"].as_i64().unwrap());
    let helper = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/inlet-line.py");
    let body =
        serde_json::json!({"op":"post","token": token, "text":"from the helper"}).to_string();
    let out = Command::new("python3")
        .arg(&helper)
        .arg(home.join("run/worker.sock"))
        .arg(body)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("\"ok\":true"),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut found = false;
    while Instant::now() < deadline {
        found = read_log(&home).iter().any(|decoded| {
            matches!(decoded, Decoded::Rec(rec) if matches!(rec.as_ref(), Record::Post { text, .. } if text == "from the helper"))
        });
        if found {
            break;
        }
        thread::sleep(Duration::from_millis(40));
    }
    assert!(found, "{}", daemon.log());
    let _ = daemon;
}
