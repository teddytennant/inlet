//! The daemon, a real cell, and the proxy. Each test gets its own home.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
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
    fs::create_dir_all(home.join("work/other")).unwrap();
    fs::write(home.join("work/other/mine"), "SIBLING\n").unwrap();
    let ledger_path = sh_quote(&home.join("ledger/log"));
    let sock = sh_quote(&home.join("run/operator.sock"));
    let key = sh_quote(&home.join("key"));
    let policy_path = sh_quote(&home.join("policy.lua"));
    let sib = sh_quote(&home.join("work/other/mine"));
    let work = sh_quote(&home.join("work"));
    let script = format!(
        "echo OK > /work/result
mark() {{ if [ -e \"$1\" ]; then echo \"$2LEAK\"; else echo \"$2OK\"; fi >> /work/result; }}
mark {ledger_path} LEDGER
mark {sock} SOCK
mark {key} KEY
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
            r#"prover = { cmd = { "/bin/sh", "-c", "kill -9 $$" }, tags = { "math" }, net = "none", on_crash = "requeue" },"#,
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
            r#"bomber = { cmd = { "/bin/sh", "-c", "kill -9 $$" }, tags = { "code" }, net = "host", on_crash = "fail" },"#,
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
    let deadline = Instant::now() + Duration::from_secs(4);
    while Instant::now() < deadline && !preamble_path.exists() {
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
        fs::read_to_string(home.join("work").join(&author).join("hide"))
            .unwrap_or_default()
            .trim(),
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
