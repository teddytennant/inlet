//! The control loop: prompts, retries, swarms, and the door. Each test gets
//! its own home and a real daemon.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

struct Daemon {
    child: Child,
    log_path: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
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
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("inlet-c-{name}-{}-{stamp}", std::process::id()));
    fs::create_dir_all(dir.join("ledger")).unwrap();
    fs::create_dir_all(dir.join("run")).unwrap();
    dir
}

/// A policy with no retries and no backoff unless `extra` says otherwise.
fn policy(home: &Path, workers: &str, extra: &str) {
    let body = format!(
        r#"
caps = {{ max_live = 4, max_depth = 3, max_tokens = 10000, max_memory_mb = 8192, max_pids = 64 }}
setup = "box"
isolator = "rlimit"
retry = {{ max = 0, backoff = 0 }}
default_budget = {{ tokens = 100, seconds = 30, memory_mb = 64, pids = 8 }}
workers = {{
  {workers}
}}
{extra}
"#
    );
    fs::write(home.join("policy.lua"), body).unwrap();
}

fn start(home: &Path) -> Daemon {
    let _ = fs::remove_file(home.join("run/operator.sock"));
    let _ = fs::remove_file(home.join("run/inlet.pid"));
    let log_path = home.join("test-daemon.log");
    let log = File::options()
        .create(true)
        .append(true)
        .open(&log_path)
        .unwrap();
    let mut child = Command::new(bin())
        .args(["--home", home.to_str().unwrap(), "up", "-f"])
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let want = child.id().to_string();
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        let pid = fs::read_to_string(home.join("run/inlet.pid")).unwrap_or_default();
        if home.join("run/operator.sock").exists() && pid.trim() == want {
            return Daemon { child, log_path };
        }
        if let Some(status) = child.try_wait().ok().flatten() {
            panic!(
                "daemon exited {status}: {}",
                fs::read_to_string(&log_path).unwrap_or_default()
            );
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("daemon did not come up");
}

fn run(home: &Path, args: &[&str]) -> Output {
    Command::new(bin())
        .arg("--home")
        .arg(home)
        .args(args)
        .output()
        .unwrap()
}

fn inlet(home: &Path, args: &[&str]) -> String {
    let out = run(home, args);
    assert!(
        out.status.success(),
        "inlet {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn status(home: &Path) -> Value {
    serde_json::from_str(&inlet(home, &["status", "--json"])).unwrap()
}

fn tasks(v: &Value) -> Vec<Value> {
    v["tasks"].as_array().cloned().unwrap_or_default()
}

fn swarm(v: &Value, name: &str) -> Value {
    v["swarms"]
        .as_array()
        .and_then(|all| all.iter().find(|s| s["name"] == name))
        .cloned()
        .unwrap_or(Value::Null)
}

fn wait(home: &Path, daemon: &Daemon, mut pred: impl FnMut(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last = Value::Null;
    while Instant::now() < deadline {
        last = status(home);
        if pred(&last) {
            return last;
        }
        thread::sleep(Duration::from_millis(40));
    }
    panic!("timed out: {last}\n{}", daemon.log());
}

fn manifest(home: &Path, body: &str) {
    let path = home.join("manifest.json");
    fs::write(&path, body).unwrap();
    inlet(home, &["apply", "-f", path.to_str().unwrap()]);
}

fn work(home: &Path, id: &Value, file: &str) -> String {
    let path = home.join("work").join(id.as_str().unwrap()).join(file);
    fs::read_to_string(path).unwrap_or_default()
}

#[test]
fn the_prompt_reaches_the_worker_three_ways() {
    let home = scratch("prompt");
    policy(
        &home,
        r#"argv = { cmd = { "/bin/sh", "-c", "printf %s \"$1\" > arg; cp \"$INLET_PROMPT_FILE\" file; cp \"$INLET_GOAL_FILE\" goal; cp \"$INLET_TASK_FILE\" task; echo \"$MODE\" > mode", "sh", "{prompt}" },
               env = { MODE = "batch" } },
        pipe = { cmd = { "/bin/sh", "-c", "cat > stdin" }, stdin = "prompt", preamble = "Only {worker} {id}." },"#,
        "",
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "argv",
            "-g",
            "sum {id} and {prompt}",
            "--verify",
            "test -s arg",
        ],
    );
    inlet(
        &home,
        &["add", "-w", "pipe", "-g", "piped goal", "--no-verify"],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().filter(|t| t["state"] == "done").count() == 2
    });
    let all = tasks(&st);
    let argv = all.iter().find(|t| t["worker"] == "argv").unwrap();
    let id = argv["id"].as_str().unwrap();
    let arg = work(&home, &argv["id"], "arg");
    assert_eq!(arg, work(&home, &argv["id"], "file"));
    assert!(
        arg.contains(&format!("You are worker {id} in inlet")),
        "{arg}"
    );
    // The goal is data. Its braces are not fields.
    assert!(arg.contains("## Task\nsum {id} and {prompt}\n"), "{arg}");
    assert!(arg.contains("## Done means"), "{arg}");
    assert!(arg.contains("    test -s arg\n"), "{arg}");
    assert!(!arg.contains("## Last attempt"), "{arg}");
    assert_eq!(work(&home, &argv["id"], "goal"), "sum {id} and {prompt}");
    assert_eq!(work(&home, &argv["id"], "mode").trim(), "batch");
    let header: Value = serde_json::from_str(&work(&home, &argv["id"], "task")).unwrap();
    assert_eq!(header["id"], id);
    assert_eq!(header["verifier"], "test -s arg");
    assert_eq!(header["attempt"], 1);

    let pipe = all.iter().find(|t| t["worker"] == "pipe").unwrap();
    let stdin = work(&home, &pipe["id"], "stdin");
    assert!(
        stdin.starts_with(&format!("Only pipe {}.\n", pipe["id"].as_str().unwrap())),
        "{stdin}"
    );
    assert!(stdin.contains("## Task\npiped goal\n"), "{stdin}");
    assert!(stdin.contains("Nothing checks this task"), "{stdin}");
}

#[test]
fn a_failed_verifier_retries_with_what_went_wrong() {
    let home = scratch("retry");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "cat \"$INLET_PROMPT_FILE\" >> prompts; echo try >> tries; echo worker-said-this" } },"#,
        "",
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "job",
            "-g",
            "third time lucky",
            "--retries",
            "3",
            "--verify",
            "n=$(wc -l < tries); echo \"have $n tries, need 3\"; [ \"$n\" -ge 3 ]",
        ],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "done")
    });
    let all = tasks(&st);
    assert_eq!(all.len(), 3, "{st}");
    let done = all.iter().find(|t| t["state"] == "done").unwrap();
    assert_eq!(done["attempt"], 3);
    assert_eq!(done["max_attempts"], 4);
    let failed: Vec<&Value> = all.iter().filter(|t| t["state"] == "failed").collect();
    assert!(failed.iter().all(|t| t["reason"] == "verifier"), "{st}");
    // One workdir carried through the chain.
    assert_eq!(work(&home, &done["id"], "tries").lines().count(), 3);
    let prompts = work(&home, &done["id"], "prompts");
    assert!(prompts.contains("attempt 3 of 4"), "{prompts}");
    assert!(prompts.contains("## Last attempt"), "{prompts}");
    assert!(
        prompts.contains("ended: the verifier did not exit 0"),
        "{prompts}"
    );
    assert!(prompts.contains("    have 2 tries, need 3\n"), "{prompts}");
    assert!(prompts.contains("    worker-said-this\n"), "{prompts}");
    // Each attempt was its own admission and its own refund.
    assert_eq!(st["held"], 0);
    assert_eq!(st["available"], 10000);

    let shown = inlet(&home, &["describe", done["id"].as_str().unwrap()]);
    assert!(shown.contains("attempt   3 of 4"), "{shown}");
    assert!(shown.contains("attempts"), "{shown}");
    assert!(shown.contains("have 3 tries, need 3"), "{shown}");
    let first = failed.iter().find(|t| t["attempt"] == 1).unwrap();
    let log = inlet(&home, &["logs", first["id"].as_str().unwrap(), "--verify"]);
    assert_eq!(log, "have 1 tries, need 3\n");
}

#[test]
fn retries_stop_at_the_limit_and_wait_out_the_backoff() {
    let home = scratch("limit");
    policy(
        &home,
        r#"job = { cmd = { "true" } },"#,
        r#"retry = { max = 1, backoff = "2s" }"#,
    );
    let daemon = start(&home);
    inlet(
        &home,
        &["add", "-w", "job", "-g", "never", "--verify", "false"],
    );
    let st = wait(&home, &daemon, |v| tasks(v).len() == 2);
    let all = tasks(&st);
    let second = all.iter().find(|t| t["attempt"] == 2).unwrap();
    assert_eq!(second["state"], "queued", "{st}");
    let first = all.iter().find(|t| t["attempt"] == 1).unwrap();
    assert_eq!(second["retry_of"], first["id"]);
    thread::sleep(Duration::from_millis(700));
    let held = status(&home);
    assert_eq!(
        tasks(&held)[1]["state"],
        "queued",
        "retry ran inside its backoff: {held}"
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().all(|t| t["state"] == "failed")
    });
    thread::sleep(Duration::from_millis(300));
    assert_eq!(tasks(&status(&home)).len(), 2, "{st}");
}

#[test]
fn a_short_purse_holds_the_queue_instead_of_denying_it() {
    let home = scratch("hold");
    policy(
        &home,
        r#"nap = { cmd = { "sleep", "0.4" } },"#,
        "caps = { max_live = 4, max_depth = 3, max_tokens = 100, max_memory_mb = 8192, max_pids = 64 }",
    );
    let daemon = start(&home);
    for goal in ["a", "b", "c"] {
        inlet(
            &home,
            &[
                "add",
                "-w",
                "nap",
                "-g",
                goal,
                "--no-verify",
                "--tokens",
                "100",
            ],
        );
    }
    let mid = status(&home);
    assert_eq!(mid["live"], 1, "{mid}");
    assert_eq!(mid["queued"], 2, "{mid}");
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().all(|t| t["state"] == "done")
    });
    assert_eq!(tasks(&st).len(), 3);
    // One that can never fit is still a deny.
    inlet(
        &home,
        &[
            "add",
            "-w",
            "nap",
            "-g",
            "fat",
            "--no-verify",
            "--tokens",
            "101",
        ],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "fat" && t["state"] == "failed")
    });
    let fat = tasks(&st).into_iter().find(|t| t["goal"] == "fat").unwrap();
    assert_eq!(fat["reason"], "purse");
}

#[test]
fn a_swarm_runs_to_its_completions() {
    let home = scratch("swarm");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "cat \"$INLET_GOAL_FILE\" > goal; sleep 0.2" } },"#,
        "",
    );
    let daemon = start(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"sweep","replicas":2,"completions":5,
            "template":{"worker":"job","goal":"seed {index}","verify":"grep -q seed goal","tokens":50}}"#,
    );
    let mut peak = 0;
    let st = wait(&home, &daemon, |v| {
        peak = peak.max(v["live"].as_u64().unwrap_or(0));
        swarm(v, "sweep")["phase"] == "done"
    });
    let sweep = swarm(&st, "sweep");
    assert_eq!(sweep["done"], 5, "{st}");
    assert_eq!(sweep["created"], 5, "{st}");
    assert_eq!(sweep["active"], 0, "{st}");
    assert!(peak <= 2, "replicas is a ceiling, saw {peak} live");
    let mut goals: Vec<String> = tasks(&st)
        .iter()
        .map(|t| t["goal"].as_str().unwrap().to_string())
        .collect();
    goals.sort();
    assert_eq!(goals, ["seed 1", "seed 2", "seed 3", "seed 4", "seed 5"]);
    assert!(tasks(&st).iter().all(|t| t["swarm"] == "sweep"));

    let table = inlet(&home, &["get", "swarms"]);
    assert!(table.contains("sweep"), "{table}");
    assert!(table.contains("5/5"), "{table}");
    let rows = inlet(&home, &["get", "tasks"]);
    assert_eq!(rows.lines().count(), 6, "{rows}");
}

#[test]
fn a_failing_swarm_stalls_and_a_scale_restarts_it() {
    let home = scratch("stall");
    policy(&home, r#"job = { cmd = { "true" } },"#, "");
    let daemon = start(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"doomed","replicas":1,"completions":3,"backoff_limit":2,
            "template":{"worker":"job","goal":"no","verify":"false"}}"#,
    );
    let st = wait(&home, &daemon, |v| swarm(v, "doomed")["phase"] == "stalled");
    let doomed = swarm(&st, "doomed");
    assert_eq!(doomed["failed"], 2, "{st}");
    assert_eq!(doomed["created"], 2, "{st}");
    thread::sleep(Duration::from_millis(300));
    let st = status(&home);
    assert_eq!(tasks(&st).len(), 2, "a stalled swarm stamps nothing: {st}");
    let said = st["posts"].to_string();
    assert!(
        said.contains("swarm doomed stalled after 2 failures"),
        "{said}"
    );

    inlet(&home, &["scale", "doomed", "1"]);
    let st = wait(&home, &daemon, |v| {
        let s = swarm(v, "doomed");
        s["generation"] == 2 && s["phase"] == "stalled"
    });
    assert_eq!(swarm(&st, "doomed")["created"], 4, "{st}");

    // An unknown worker or a missing verifier never becomes a swarm.
    let bad = home.join("bad.json");
    fs::write(
        &bad,
        r#"{"kind":"swarm","name":"x","template":{"worker":"job","goal":"g"}}"#,
    )
    .unwrap();
    let out = run(&home, &["apply", "-f", bad.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(swarm(&status(&home), "x").is_null());
}

#[test]
fn a_retry_inside_a_swarm_is_not_a_swarm_failure() {
    let home = scratch("chain");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "echo try >> tries" } },"#,
        "",
    );
    let daemon = start(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"twice","replicas":1,"completions":1,"backoff_limit":1,
            "template":{"worker":"job","goal":"g","retries":1,"verify":"[ $(wc -l < tries) -ge 2 ]"}}"#,
    );
    let st = wait(&home, &daemon, |v| swarm(v, "twice")["phase"] != "active");
    let twice = swarm(&st, "twice");
    // The first attempt failed, but backoff_limit 1 did not stall the swarm.
    assert_eq!(twice["phase"], "done", "{st}");
    assert_eq!(twice["failed"], 0, "{st}");
    assert_eq!(twice["created"], 1, "{st}");
    assert_eq!(tasks(&st).len(), 2, "{st}");
}

#[test]
fn a_swarm_picks_up_after_kill9() {
    let home = scratch("kill9");
    policy(&home, r#"nap = { cmd = { "sleep", "0.5" } },"#, "");
    let mut daemon = start(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"long","replicas":2,"completions":4,
            "template":{"worker":"nap","goal":"n {index}","no_verify":true,"tokens":50}}"#,
    );
    wait(&home, &daemon, |v| v["live"] == 2);
    daemon.child.kill().unwrap();
    daemon.child.wait().unwrap();
    let daemon = start(&home);
    let st = wait(&home, &daemon, |v| swarm(v, "long")["phase"] == "done");
    let long = swarm(&st, "long");
    assert_eq!(long["done"], 4, "{st}");
    assert_eq!(long["active"], 0, "{st}");
    // The two it was running died with the daemon and were replaced.
    assert_eq!(long["failed"], 2, "{st}");
    assert_eq!(long["created"], 6, "{st}");
    let crashed = tasks(&st).iter().filter(|t| t["reason"] == "crash").count();
    assert_eq!(crashed, 2, "{st}");
    // A crash keeps its slice. Everything else came back.
    assert_eq!(st["held"], 0, "{st}");
    assert_eq!(st["available"], 10000 - 100, "{st}");
}

#[test]
fn delete_stops_a_swarm_with_no_end() {
    let home = scratch("delete");
    policy(&home, r#"nap = { cmd = { "sleep", "30" } },"#, "");
    let daemon = start(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"forever","replicas":2,"completions":0,
            "template":{"worker":"nap","goal":"keep going","no_verify":true}}"#,
    );
    wait(&home, &daemon, |v| v["live"] == 2);
    inlet(&home, &["scale", "forever", "3"]);
    wait(&home, &daemon, |v| v["live"] == 3);
    let said = inlet(&home, &["delete", "forever", "--kill"]);
    assert_eq!(said.trim(), "swarm forever deleted, 3 stopped");
    let st = wait(&home, &daemon, |v| v["live"] == 0);
    assert!(swarm(&st, "forever").is_null(), "{st}");
    assert!(tasks(&st).iter().all(|t| t["state"] == "killed"), "{st}");
    thread::sleep(Duration::from_millis(200));
    assert_eq!(tasks(&status(&home)).len(), 3);
}

#[test]
fn a_parent_hands_off_through_the_door_and_reads_the_answer() {
    let home = scratch("door");
    policy(
        &home,
        r#"lead = { cmd = { "/bin/sh", "-c", "\"$INLET_BIN\" door spawn -w kid -g 'the part' --verify 'test -f made' --tokens 20 > kid && \"$INLET_BIN\" door wait --timeout 10 > kids && \"$INLET_BIN\" door post lead-finished" } },
        kid = { cmd = { "/bin/sh", "-c", "touch made; \"$INLET_BIN\" door post 'kid found 42'" } },"#,
        "",
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "lead",
            "-g",
            "delegate",
            "--verify",
            "test -s kids",
        ],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v)
            .iter()
            .any(|t| t["worker"] == "lead" && (t["state"] == "done" || t["state"] == "failed"))
    });
    let all = tasks(&st);
    let lead = all.iter().find(|t| t["worker"] == "lead").unwrap();
    assert_eq!(lead["state"], "done", "{st} {}", daemon.log());
    let kid = all.iter().find(|t| t["worker"] == "kid").unwrap();
    assert_eq!(kid["parent"], lead["id"]);
    assert_eq!(
        work(&home, &lead["id"], "kid").trim(),
        kid["id"].as_str().unwrap()
    );
    let kids: Value = serde_json::from_str(&work(&home, &lead["id"], "kids")).unwrap();
    assert_eq!(kids[0]["state"], "done", "{kids}");
    assert_eq!(kids[0]["last_post"], "kid found 42", "{kids}");
    assert!(st["posts"].to_string().contains("lead-finished"));
}

#[test]
fn a_silent_worker_is_stopped_and_tried_again() {
    let home = scratch("silent");
    policy(
        &home,
        r#"mute = { cmd = { "/bin/sh", "-c", "[ -f seen ] && exit 0; touch seen; sleep 30" }, stall = 1 },"#,
        "retry = { max = 1, backoff = 0 }",
    );
    let daemon = start(&home);
    inlet(
        &home,
        &["add", "-w", "mute", "-g", "hang once", "--no-verify"],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "done")
    });
    let all = tasks(&st);
    assert_eq!(all.len(), 2, "{st}");
    let first = all.iter().find(|t| t["attempt"] == 1).unwrap();
    assert_eq!(first["reason"], "stall", "{st}");
}

#[test]
fn a_constraint_lapses_and_clears_without_a_key() {
    let home = scratch("bind");
    policy(
        &home,
        r#"job = { cmd = { "true" }, tags = { "code" } },"#,
        "",
    );
    let daemon = start(&home);
    inlet(&home, &["bind", "--for", "1", "freeze #code"]);
    inlet(&home, &["add", "-w", "job", "-g", "early", "--no-verify"]);
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "failed")
    });
    assert_eq!(tasks(&st)[0]["reason"], "constraint");
    thread::sleep(Duration::from_millis(1100));
    inlet(&home, &["add", "-w", "job", "-g", "late", "--no-verify"]);
    let st = wait(&home, &daemon, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "late" && t["state"] == "done")
    });
    assert_eq!(st["constraints"].as_array().unwrap().len(), 0, "{st}");

    inlet(&home, &["bind", "hold #code"]);
    let held = status(&home);
    let id = held["constraints"][0]["id"].as_str().unwrap().to_string();
    inlet(&home, &["clear", &id]);
    assert_eq!(status(&home)["constraints"].as_array().unwrap().len(), 0);
}

#[test]
fn a_daemon_stop_reruns_what_it_interrupted() {
    let home = scratch("stop");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "echo run >> runs; [ -f go ] || sleep 30" } },"#,
        "",
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "job",
            "-g",
            "survive",
            "--verify",
            "test -f runs",
        ],
    );
    let st = wait(&home, &daemon, |v| v["live"] == 1);
    let first = tasks(&st)[0]["id"].clone();
    while work(&home, &first, "runs").is_empty() {
        thread::sleep(Duration::from_millis(20));
    }
    fs::write(
        home.join("work").join(first.as_str().unwrap()).join("go"),
        "",
    )
    .unwrap();
    unsafe { libc::kill(daemon.child.id() as i32, libc::SIGTERM) };
    daemon.child.wait().unwrap();

    let daemon = start(&home);
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "done")
    });
    let all = tasks(&st);
    assert_eq!(all.len(), 2, "{st}");
    let stopped = all.iter().find(|t| t["id"] == first).unwrap();
    assert_eq!(stopped["reason"], "stopped", "{st}");
    let rerun = all.iter().find(|t| t["state"] == "done").unwrap();
    assert_eq!(rerun["retry_of"], first);
    // Policy allows no retries, and this did not use one.
    assert_eq!(rerun["attempt"], 1, "{st}");
    assert_eq!(work(&home, &rerun["id"], "runs").lines().count(), 2);
    assert_eq!(st["available"], 10000, "{st}");
}
