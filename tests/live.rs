//! The daemon, a real cell, and the proxy. Each test gets its own home.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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
