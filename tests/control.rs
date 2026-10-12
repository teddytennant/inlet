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
    let deadline = Instant::now() + Duration::from_secs(40);
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
    // Generous: the suite runs in parallel, often on a busy machine.
    let deadline = Instant::now() + Duration::from_secs(40);
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
    // Without adoption a dead daemon takes its workers with it.
    policy(
        &home,
        r#"nap = { cmd = { "sleep", "0.5" } },"#,
        "adopt = false",
    );
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
        r#"lead = { cmd = { "/bin/sh", "-c", "\"$INLET_BIN\" door spawn -w kid -g 'the part' --verify 'test -f made' > kid && \"$INLET_BIN\" door wait --timeout 10 > kids && \"$INLET_BIN\" door post lead-finished" } },
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
    // It named no budget and got half of what its parent had left.
    let shown = inlet(&home, &["get", "tasks", "--json"]);
    let rows: Value = serde_json::from_str(&shown).unwrap();
    let row = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["worker"] == "kid")
        .unwrap();
    assert_eq!(row["budget_tokens"], 50, "{row}");
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
    inlet(&home, &["bind", "--for", "3", "freeze #code"]);
    inlet(&home, &["add", "-w", "job", "-g", "early", "--no-verify"]);
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "failed")
    });
    assert_eq!(tasks(&st)[0]["reason"], "constraint");
    thread::sleep(Duration::from_millis(3100));
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
    unsafe { libc::kill(daemon.child.id() as i32, libc::SIGINT) };
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

fn kill9(daemon: &mut Daemon) {
    daemon.child.kill().unwrap();
    daemon.child.wait().unwrap();
}

#[test]
fn a_worker_outlives_the_daemon_and_is_taken_back() {
    let home = scratch("adopt");
    policy(
        &home,
        r#"slow = { cmd = { "/bin/sh", "-c", "echo started; echo $$ > pid; sleep 1.5; echo finished; \"$INLET_BIN\" door post after-the-restart; touch made" } },"#,
        "",
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "slow",
            "-g",
            "ride it out",
            "--verify",
            "test -f made",
        ],
    );
    let st = wait(&home, &daemon, |v| v["live"] == 1);
    let id = tasks(&st)[0]["id"].clone();
    while work(&home, &id, "pid").is_empty() {
        thread::sleep(Duration::from_millis(20));
    }
    kill9(&mut daemon);
    thread::sleep(Duration::from_millis(200));

    let daemon = start(&home);
    assert!(daemon.log().contains("took back"), "{}", daemon.log());
    let mid = status(&home);
    assert_eq!(mid["live"], 1, "{mid}");
    assert_eq!(tasks(&mid).len(), 1, "{mid}");
    let st = wait(&home, &daemon, |v| tasks(v)[0]["state"] != "running");
    let task = &tasks(&st)[0];
    // One task, one attempt, one process from start to finish.
    assert_eq!(task["state"], "done", "{st} {}", daemon.log());
    assert_eq!(task["attempt"], 1);
    assert_eq!(tasks(&st).len(), 1);
    let log = inlet(&home, &["logs", id.as_str().unwrap()]);
    assert!(log.starts_with("started\nfinished\n"), "{log}");
    // Its token still opened the door, on a socket the new daemon bound.
    assert!(
        st["posts"].to_string().contains("after-the-restart"),
        "{st}"
    );
    assert_eq!(st["held"], 0, "{st}");
    assert_eq!(st["available"], 10000, "{st}");
}

#[test]
fn a_worker_that_finished_while_the_daemon_was_down_still_counts() {
    let home = scratch("offline");
    policy(
        &home,
        r#"quick = { cmd = { "/bin/sh", "-c", "sleep 2; echo $1 > out; exit $1", "sh", "{goal}" } },"#,
        "",
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &["add", "-w", "quick", "-g", "0", "--verify", "grep -q 0 out"],
    );
    inlet(&home, &["add", "-w", "quick", "-g", "3", "--no-verify"]);
    wait(&home, &daemon, |v| v["live"] == 2);
    kill9(&mut daemon);
    thread::sleep(Duration::from_millis(2600));

    let daemon = start(&home);
    let st = wait(&home, &daemon, |v| v["live"] == 0);
    let all = tasks(&st);
    assert_eq!(all.len(), 2, "{st}");
    let ok = all.iter().find(|t| t["goal"] == "0").unwrap();
    assert_eq!(ok["state"], "done", "{st} {}", daemon.log());
    // The exit code survived with nobody there to wait for it.
    let bad = all.iter().find(|t| t["goal"] == "3").unwrap();
    assert_eq!(
        (bad["state"].as_str(), bad["reason"].as_str()),
        (Some("failed"), Some("exit"))
    );
    assert_eq!(st["available"], 10000, "{st}");
}

#[test]
fn a_kept_worker_cannot_outrun_its_clock_or_a_kill() {
    let home = scratch("clock");
    policy(
        &home,
        r#"hang = { cmd = { "/bin/sh", "-c", "echo $$ > pid; sleep 60" } },"#,
        "",
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "hang",
            "-g",
            "late",
            "--no-verify",
            "--seconds",
            "6",
        ],
    );
    inlet(
        &home,
        &["add", "-w", "hang", "-g", "unwanted", "--no-verify"],
    );
    let st = wait(&home, &daemon, |v| v["live"] == 2);
    let unwanted = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "unwanted")
        .unwrap();
    kill9(&mut daemon);

    let daemon = start(&home);
    inlet(&home, &["kill", unwanted["id"].as_str().unwrap()]);
    let st = wait(&home, &daemon, |v| v["live"] == 0);
    let all = tasks(&st);
    let late = all.iter().find(|t| t["goal"] == "late").unwrap();
    assert_eq!(late["reason"], "timeout", "{st}");
    let gone = all.iter().find(|t| t["goal"] == "unwanted").unwrap();
    assert_eq!(gone["state"], "killed", "{st}");
    assert!(run(&home, &["status"]).status.success());
    // Nothing of either is left on the host.
    for task in &all {
        let live = home.join("run/live").join(task["id"].as_str().unwrap());
        assert!(!live.exists(), "{}", live.display());
    }
}

/// An upstream that answers every post with the same completion and usage.
fn fake_upstream() -> u16 {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        for mut conn in listener.incoming().flatten() {
            let _ = conn.set_read_timeout(Some(Duration::from_secs(2)));
            let mut raw = Vec::new();
            let mut buf = [0u8; 4096];
            while let Ok(n) = conn.read(&mut buf) {
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&raw);
                if let Some(head) = text.find("\r\n\r\n") {
                    let want = text
                        .to_ascii_lowercase()
                        .split("content-length:")
                        .nth(1)
                        .and_then(|rest| rest.lines().next().map(|n| n.trim().to_string()))
                        .and_then(|n| n.parse::<usize>().ok())
                        .unwrap_or(0);
                    if raw.len() >= head + 4 + want {
                        break;
                    }
                }
            }
            let body = if raw.starts_with(b"GET /v1/models ") {
                r#"{"object":"list","data":[{"id":"model-from-upstream"}]}"#
            } else if raw.starts_with(b"POST ") {
                r#"{"choices":[{"message":{"role":"assistant","content":"from-upstream"}}],"usage":{"total_tokens":22}}"#
            } else {
                continue;
            };
            let _ = write!(
                conn,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    port
}

#[test]
fn a_harness_that_wants_a_base_url_gets_the_metered_proxy() {
    let home = scratch("baseurl");
    let port = fake_upstream();
    // What any OpenAI client does: post to $OPENAI_BASE_URL with $OPENAI_API_KEY.
    let seed = home.join("seed");
    fs::create_dir_all(&seed).unwrap();
    fs::write(
        seed.join("client.py"),
        r#"import os, urllib.request
req = urllib.request.Request(
    os.environ["OPENAI_BASE_URL"] + "/chat/completions",
    data=b'{"messages": [], "max_tokens": 10}',
    headers={"Authorization": "Bearer " + os.environ["OPENAI_API_KEY"], "Content-Type": "application/json"},
)
open("reply", "w").write(urllib.request.urlopen(req).read().decode())
listing = urllib.request.Request(
    os.environ["OPENAI_BASE_URL"] + "/models",
    headers={"Authorization": "Bearer " + os.environ["OPENAI_API_KEY"]},
)
open("models", "w").write(urllib.request.urlopen(listing).read().decode())
"#,
    )
    .unwrap();
    let cmd = r#"cmd = { "/bin/sh", "-c", "exec \"$INLET_BIN\" door run -- python3 client.py" }"#;
    policy(
        &home,
        &format!(r#"open = {{ {cmd}, net = "host" }}, shut = {{ {cmd}, net = "none" }},"#),
        &format!(r#"proxy = {{ upstream = "http://127.0.0.1:{port}", key = "k" }}"#),
    );
    let daemon = start(&home);
    for worker in ["open", "shut"] {
        inlet(
            &home,
            &[
                "add",
                "-w",
                worker,
                "-g",
                "call the model",
                "--seed",
                seed.to_str().unwrap(),
                "--verify",
                "grep -q from-upstream reply && grep -q model-from-upstream models",
            ],
        );
    }
    let st = wait(&home, &daemon, |v| {
        tasks(v)
            .iter()
            .all(|t| t["state"] == "done" || t["state"] == "failed")
    });
    for task in tasks(&st) {
        assert_eq!(task["state"], "done", "{st} {}", daemon.log());
    }
    // Both calls went through the purse: 22 tokens each, the rest refunded.
    assert_eq!(st["available"], 10000 - 44, "{st}");
}

#[test]
fn sigterm_leaves_the_work_running() {
    let home = scratch("term");
    policy(
        &home,
        r#"slow = { cmd = { "/bin/sh", "-c", "echo up > up; sleep 1.2; touch made" } },"#,
        "",
    );
    let mut daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "slow",
            "-g",
            "keep going",
            "--verify",
            "test -f made",
        ],
    );
    let st = wait(&home, &daemon, |v| v["live"] == 1);
    let id = tasks(&st)[0]["id"].clone();
    while work(&home, &id, "up").is_empty() {
        thread::sleep(Duration::from_millis(20));
    }
    // `inlet down --keep` is that signal with a name.
    let reaper = thread::spawn({
        let home = home.clone();
        move || run(&home, &["down", "--keep"])
    });
    assert!(daemon.child.wait().unwrap().success());
    let said = reaper.join().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&said.stdout).trim(),
        "down, workers kept"
    );

    let daemon = start(&home);
    let st = wait(&home, &daemon, |v| tasks(v)[0]["state"] != "running");
    assert_eq!(tasks(&st).len(), 1, "{st}");
    assert_eq!(tasks(&st)[0]["state"], "done", "{st} {}", daemon.log());
    assert_eq!(st["available"], 10000, "{st}");
}

#[test]
fn a_single_file_in_cell_ro_can_be_run() {
    use std::os::unix::fs::PermissionsExt;
    let home = scratch("rofile");
    // Outside /tmp, which a cell covers with its own.
    let tool = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/ro-file-test.sh");
    fs::write(&tool, "#!/bin/sh\necho ran > out\n").unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    let tool = tool.to_str().unwrap();
    policy(
        &home,
        &format!(r#"tool = {{ cmd = {{ "{tool}" }} }},"#),
        &format!(r#"cell = {{ ro = {{ "{tool}", "/tmp/hidden-by-the-cell" }} }}"#),
    );
    let daemon = start(&home);
    assert!(
        daemon
            .log()
            .contains("cell.ro /tmp/hidden-by-the-cell refused"),
        "{}",
        daemon.log()
    );
    inlet(
        &home,
        &[
            "add",
            "-w",
            "tool",
            "-g",
            "run",
            "--verify",
            "grep -q ran out",
        ],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v)[0]["state"] != "running" && tasks(v)[0]["state"] != "queued"
    });
    assert_eq!(tasks(&st)[0]["state"], "done", "{st} {}", daemon.log());
}

#[test]
fn a_cell_has_its_own_proc() {
    let home = scratch("proc");
    policy(
        &home,
        r#"look = { cmd = { "/bin/sh", "-c", "cat /proc/self/maps > /dev/null && ls /proc | grep -c '^[0-9]' > count && grep -c . /proc/1/cmdline > init" } },"#,
        "",
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "look",
            "-g",
            "look",
            "--verify",
            "test -s init",
        ],
    );
    let st = wait(&home, &daemon, |v| v["live"] == 0 && v["queued"] == 0);
    let task = &tasks(&st)[0];
    assert_eq!(task["state"], "done", "{st} {}", daemon.log());
    // Its own pid namespace: the keeper, the shell, and what the shell ran. Not the host.
    let count: u32 = work(&home, &task["id"], "count").trim().parse().unwrap();
    assert!((2..=6).contains(&count), "{count} processes visible");
}

#[test]
fn memory_is_capped_by_what_a_worker_really_uses() {
    let home = scratch("rss");
    policy(
        &home,
        r#"hog = { cmd = { "python3", "-c", "import time; x = b'x' * (400 * 1024 * 1024); time.sleep(30)" } },
        thin = { cmd = { "python3", "-c", "import mmap; m = mmap.mmap(-1, 8 * 1024 * 1024 * 1024); open('out', 'w').write('ok')" } },"#,
        "",
    );
    let daemon = start(&home);
    inlet(
        &home,
        &[
            "add",
            "-w",
            "hog",
            "-g",
            "eat",
            "--no-verify",
            "--memory-mb",
            "150",
        ],
    );
    // Eight gigabytes reserved and never touched is not eight gigabytes used.
    inlet(
        &home,
        &[
            "add",
            "-w",
            "thin",
            "-g",
            "reserve",
            "--verify",
            "grep -q ok out",
            "--memory-mb",
            "150",
        ],
    );
    let st = wait(&home, &daemon, |v| v["live"] == 0 && v["queued"] == 0);
    let all = tasks(&st);
    let hog = all.iter().find(|t| t["worker"] == "hog").unwrap();
    assert_eq!(hog["state"], "failed", "{st}");
    let log = inlet(&home, &["logs", hog["id"].as_str().unwrap()]);
    assert!(log.contains("over 150 MB, stopped"), "{log}");
    // Its own doing, not a crash: named for what it was, and not charged a slice.
    assert_eq!(hog["reason"], "memory", "{st}");
    assert_eq!(st["available"], 10000, "{st}");
    let thin = all.iter().find(|t| t["worker"] == "thin").unwrap();
    assert_eq!(thin["state"], "done", "{st} {}", daemon.log());
}

#[test]
fn an_upstream_that_is_down_or_refuses_costs_nothing() {
    use std::io::{Read, Write};
    let home = scratch("outage");
    // One port nobody listens on, one that answers every call with a 503.
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_port = busy.local_addr().unwrap().port();
    thread::spawn(move || {
        for mut conn in busy.incoming().flatten() {
            let _ = conn.set_read_timeout(Some(Duration::from_secs(2)));
            let mut raw = Vec::new();
            let mut buf = [0u8; 4096];
            // The whole request, or closing with bytes unread resets the caller.
            while let Ok(n) = conn.read(&mut buf) {
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&raw).to_ascii_lowercase();
                if let Some(head) = text.find("\r\n\r\n") {
                    let want = text
                        .split("content-length:")
                        .nth(1)
                        .and_then(|rest| rest.lines().next().map(|n| n.trim().to_string()))
                        .and_then(|n| n.parse::<usize>().ok())
                        .unwrap_or(0);
                    if raw.len() >= head + 4 + want {
                        break;
                    }
                }
            }
            let body = r#"{"error":{"type":"overloaded","message":"try later"}}"#;
            let _ = write!(
                conn,
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let seed = home.join("seed");
    fs::create_dir_all(&seed).unwrap();
    fs::write(
        seed.join("client.py"),
        r#"import os, urllib.request, urllib.error
codes = []
for _ in range(4):
    req = urllib.request.Request(
        os.environ["OPENAI_BASE_URL"] + "/chat/completions",
        data=b'{"messages": [], "max_tokens": 50}',
        headers={"Authorization": "Bearer " + os.environ["OPENAI_API_KEY"], "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
        codes.append(200)
    except urllib.error.HTTPError as e:
        codes.append(e.code)
open("codes", "w").write(" ".join(map(str, codes)))
"#,
    )
    .unwrap();
    for (name, port, want) in [
        ("dead", dead, "502 502 502 502"),
        ("busy", busy_port, "503 503 503 503"),
    ] {
        let home = home.join(name);
        fs::create_dir_all(home.join("ledger")).unwrap();
        fs::create_dir_all(home.join("run")).unwrap();
        policy(
            &home,
            r#"caller = { cmd = { "/bin/sh", "-c", "exec \"$INLET_BIN\" door run -- python3 client.py" }, net = "host" },"#,
            &format!(r#"proxy = {{ upstream = "http://127.0.0.1:{port}" }}"#),
        );
        let daemon = start(&home);
        inlet(
            &home,
            &[
                "add",
                "-w",
                "caller",
                "-g",
                "call",
                "--seed",
                seed.to_str().unwrap(),
                "--verify",
                "test -s codes",
            ],
        );
        let st = wait(&home, &daemon, |v| v["live"] == 0 && v["queued"] == 0);
        let task = &tasks(&st)[0];
        assert_eq!(task["state"], "done", "{name}: {st} {}", daemon.log());
        assert_eq!(work(&home, &task["id"], "codes"), want, "{name}");
        // Four calls that no model answered. The slice came back whole.
        assert_eq!(st["available"], 10000, "{name}: {st}");
    }
}

#[test]
fn old_tasks_are_forgotten_and_stay_forgotten() {
    let home = scratch("retain");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "echo hi > out" } },"#,
        "retain = 2",
    );
    let mut daemon = start(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"old","replicas":3,"completions":3,
            "template":{"worker":"job","goal":"g {index}","verify":"test -s out"}}"#,
    );
    let st = wait(&home, &daemon, |v| swarm(v, "old")["phase"] == "done");
    let ids: Vec<String> = tasks(&st)
        .iter()
        .map(|t| t["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 3);
    assert!(home.join("work").join(&ids[0]).exists());
    // Past retain, the headers go, and the files with them.
    let st = wait(&home, &daemon, |v| tasks(v).is_empty());
    assert_eq!(
        swarm(&st, "old")["done"],
        3,
        "the swarm keeps its count: {st}"
    );
    for id in &ids {
        assert!(!home.join("work").join(id).exists(), "workdir kept");
        assert!(
            !home.join("runs").join(format!("{id}.log")).exists(),
            "transcript kept"
        );
    }
    // A restart replays from the checkpoint and does not bring them back.
    kill9(&mut daemon);
    let daemon = start(&home);
    let st = status(&home);
    assert!(tasks(&st).is_empty(), "{st}");
    assert_eq!(swarm(&st, "old")["done"], 3, "{st}");
    assert!(st["snap_offset"].as_u64().unwrap() > 0, "{st}");
    inlet(
        &home,
        &["add", "-w", "job", "-g", "after", "--verify", "test -s out"],
    );
    let st = wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "done")
    });
    assert_eq!(st["available"], 10000, "{st}");

    // A half-written index is skipped, not fatal: the log is replayed whole.
    drop(daemon);
    let index = fs::read_dir(home.join("snap"))
        .ok()
        .and_then(|dir| {
            dir.flatten()
                .find(|e| e.file_name().to_string_lossy().contains("index"))
        })
        .map(|e| e.path());
    if let Some(index) = index {
        fs::write(&index, b"{\"offset\": 12, \"head").unwrap();
        let daemon = start(&home);
        assert!(
            daemon.log().contains("snapshot index is unreadable"),
            "{}",
            daemon.log()
        );
        assert!(status(&home)["ok"] == true);
    }
}

#[test]
fn a_worker_cannot_write_its_own_exit_status() {
    let home = scratch("forge");
    // Every descriptor it was born with, tried as the status file: four zero
    // bytes would read as exit 0. Then it fails for real.
    policy(
        &home,
        r#"liar = { cmd = { "python3", "-c", "import os\nfor fd in range(3, 64):\n    try:\n        os.pwrite(fd, bytes(4), 0)\n    except OSError:\n        pass\nos._exit(3)" } },"#,
        "",
    );
    let daemon = start(&home);
    inlet(&home, &["add", "-w", "liar", "-g", "lie", "--no-verify"]);
    let st = wait(&home, &daemon, |v| v["live"] == 0 && v["queued"] == 0);
    let task = &tasks(&st)[0];
    assert_eq!(
        (task["state"].as_str(), task["reason"].as_str()),
        (Some("failed"), Some("exit")),
        "{st}"
    );
}

#[test]
fn a_kill_reaches_a_queued_task_and_outlives_a_crash() {
    let home = scratch("killq");
    policy(
        &home,
        r#"nap = { cmd = { "sleep", "30" } },"#,
        "caps = { max_live = 1, max_depth = 3, max_tokens = 10000, max_memory_mb = 8192, max_pids = 64 }",
    );
    let mut daemon = start(&home);
    inlet(&home, &["add", "-w", "nap", "-g", "running", "--no-verify"]);
    inlet(&home, &["add", "-w", "nap", "-g", "waiting", "--no-verify"]);
    let st = wait(&home, &daemon, |v| v["live"] == 1 && v["queued"] == 1);
    let id = |goal: &str| {
        tasks(&st).into_iter().find(|t| t["goal"] == goal).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    inlet(&home, &["kill", &id("waiting")]);
    let st2 = wait(&home, &daemon, |v| v["queued"] == 0);
    let waiting = tasks(&st2)
        .into_iter()
        .find(|t| t["goal"] == "waiting")
        .unwrap();
    assert_eq!(waiting["reason"], "killed", "{st2}");
    assert!(!run(&home, &["kill", "no-such-task"]).status.success());

    // The ask is written to the ledger before the signal is sent. Play the
    // crash in between: the daemon dies, and the record is all that is left.
    let running = id("running");
    kill9(&mut daemon);
    let body = format!(r#"{{"kind":"kill","id":"{running}","ts":1}}"#);
    let mut frame = Vec::new();
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&inlet::ledger::crc32(body.as_bytes()).to_le_bytes());
    frame.extend_from_slice(body.as_bytes());
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(home.join("ledger/log"))
        .unwrap()
        .write_all(&frame)
        .unwrap();
    let daemon = start(&home);
    let st3 = wait(&home, &daemon, |v| v["live"] == 0);
    let task = tasks(&st3)
        .into_iter()
        .find(|t| t["goal"] == "running")
        .unwrap();
    // Taken back, and then killed as asked. Not left running, not a crash.
    assert_eq!(task["state"], "killed", "{st3} {}", daemon.log());
    assert_eq!(tasks(&st3).len(), 2, "no retry of a kill: {st3}");
}

#[test]
fn doctor_says_what_is_wrong() {
    let home = scratch("doctor");
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    policy(
        &home,
        r#"job = { cmd = { "true" } },"#,
        &format!(r#"proxy = {{ upstream = "http://127.0.0.1:{dead}" }}"#),
    );
    let daemon = start(&home);
    let out = run(&home, &["doctor"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success(), "{text}");
    assert!(text.contains("ok    policy"), "{text} {}", daemon.log());
    assert!(text.contains("ok    cell"), "{text}");
    assert!(text.contains("FAIL  upstream"), "{text}");
    assert!(text.contains("does not answer"), "{text}");
    assert!(text.contains("ok    daemon     up, 0 live"), "{text}");
    // The daemon still works after being probed.
    inlet(&home, &["add", "-w", "job", "-g", "after", "--no-verify"]);
    wait(&home, &daemon, |v| {
        tasks(v).iter().any(|t| t["state"] == "done")
    });
}

#[test]
fn the_ledger_rotates_and_nothing_is_lost() {
    let home = scratch("rotate");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "echo hi > out" } },
        nap = { cmd = { "/bin/sh", "-c", "sleep 4; echo hi > out" } },"#,
        "retain = 1",
    );
    let boot = |home: &Path| {
        let log_path = home.join("test-daemon.log");
        let log = File::options()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap();
        let mut child = Command::new(bin())
            .args(["--home", home.to_str().unwrap(), "up", "-f"])
            .env("INLET_ROTATE_BYTES", "3000")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let want = child.id().to_string();
        for _ in 0..800 {
            let pid = fs::read_to_string(home.join("run/inlet.pid")).unwrap_or_default();
            if pid.trim() == want {
                return Daemon { child, log_path };
            }
            assert!(child.try_wait().unwrap().is_none(), "daemon died");
            thread::sleep(Duration::from_millis(25));
        }
        panic!("daemon did not come up");
    };
    let mut daemon = boot(&home);
    manifest(
        &home,
        r#"{"kind":"swarm","name":"many","replicas":4,"completions":24,
            "template":{"worker":"job","goal":"g {index}","verify":"test -s out"}}"#,
    );
    wait(&home, &daemon, |v| swarm(v, "many")["phase"] == "done");
    // A task still running when the log turns over.
    inlet(
        &home,
        &[
            "add",
            "-w",
            "nap",
            "-g",
            "across",
            "--verify",
            "test -s out",
        ],
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !daemon.log().contains("rotated the ledger") {
        assert!(Instant::now() < deadline, "no rotation: {}", daemon.log());
        thread::sleep(Duration::from_millis(100));
    }
    assert!(home.join("ledger/log.old").exists());
    // The swarm alone wrote several times the threshold. What is left in the
    // live log is what came after the last turn.
    let old = fs::metadata(home.join("ledger/log.old")).unwrap().len();
    assert!(old >= 3000, "old log is {old} bytes");
    // Dead and back, on the short log and the checkpoint.
    kill9(&mut daemon);
    let daemon = boot(&home);
    let st = wait(&home, &daemon, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "across" && t["state"] == "done")
            || (v["live"] == 0 && tasks(v).iter().all(|t| t["goal"] != "across"))
    });
    assert_eq!(swarm(&st, "many")["done"], 24, "{st}");
    assert_eq!(swarm(&st, "many")["created"], 24, "{st}");
    assert_eq!(st["held"], 0, "{st}");
    assert_eq!(st["available"], 10000, "{st}");
    inlet(
        &home,
        &["add", "-w", "job", "-g", "after", "--verify", "test -s out"],
    );
    wait(&home, &daemon, |v| {
        tasks(v)
            .iter()
            .any(|t| t["goal"] == "after" && t["state"] == "done")
    });
}

/// Two daemons on one machine. The second is a node, reached by running its
/// CLI, the way `ssh host inlet --home ...` would.
fn node_home(name: &str, workers: &str) -> PathBuf {
    let home = scratch(name);
    policy(&home, workers, "");
    home
}

fn node_policy(leader: &Path, nodes: &[(&str, &Path, usize)], extra: &str) {
    let list: String = nodes
        .iter()
        .map(|(name, home, slots)| {
            format!(
                r#"{name} = {{ cmd = {{ "{}", "--home", "{}" }}, slots = {slots} }},"#,
                bin().display(),
                home.display()
            )
        })
        .collect();
    policy(
        leader,
        r#"job = { on = "nodes" }, here = { cmd = { "/bin/sh", "-c", "echo local > out" } },"#,
        &format!("nodes = {{ {list} }}\nnode_grace = 8\n{extra}"),
    );
}

#[test]
fn a_swarm_runs_on_nodes_and_the_leader_keeps_the_books() {
    let job = r#"job = { cmd = { "/bin/sh", "-c", "echo try >> tries; cat \"$INLET_GOAL_FILE\" > goal; sleep 0.3" } },"#;
    let east = node_home("east", job);
    let west = node_home("west", job);
    let leader = scratch("leader");
    node_policy(&leader, &[("east", &east, 2), ("west", &west, 2)], "");
    let east_d = start(&east);
    let west_d = start(&west);
    let daemon = start(&leader);
    wait(&leader, &daemon, |v| {
        v["nodes"]
            .as_array()
            .is_some_and(|n| n.len() == 2 && n.iter().all(|n| n["ready"] == true))
    });
    // One goal in three needs a second attempt, which the node gives it.
    manifest(
        &leader,
        r#"{"kind":"swarm","name":"far","replicas":4,"completions":9,
            "template":{"worker":"job","goal":"g {index}","retries":2,
                        "verify":"n=$(cut -d' ' -f2 goal); [ $((n % 3)) -ne 0 ] || [ $(wc -l < tries) -ge 2 ]"}}"#,
    );
    inlet(
        &leader,
        &[
            "add",
            "-w",
            "here",
            "-g",
            "stays",
            "--verify",
            "grep -q local out",
        ],
    );
    let mut peak = 0;
    let st = wait(&leader, &daemon, |v| {
        let placed: u64 = v["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["placed"].as_u64().unwrap())
            .sum();
        peak = peak.max(placed);
        swarm(v, "far")["phase"] == "done" && v["live"] == 0
    });
    assert_eq!(swarm(&st, "far")["done"], 9, "{st}");
    assert!(
        (3..=4).contains(&peak),
        "both nodes used, never past their slots: {peak}"
    );
    // Nothing ran in a cell on the leader but the one local task.
    let local = fs::read_dir(leader.join("work"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name() != "probe")
        .count();
    assert_eq!(local, 1);
    assert_eq!(st["held"], 0, "{st}");
    assert_eq!(st["available"], 10000, "{st}");
    // The nodes did the work, and the retries.
    let (e, w) = (tasks(&status(&east)), tasks(&status(&west)));
    assert!(
        !e.is_empty() && !w.is_empty(),
        "east {} west {}",
        e.len(),
        w.len()
    );
    assert_eq!(e.len() + w.len(), 12, "nine goals, three of them twice");
    assert_eq!(
        e.iter().chain(&w).filter(|t| t["state"] == "done").count(),
        9
    );
    let table = inlet(&leader, &["get", "nodes"]);
    assert!(table.contains("east") && table.contains("ready"), "{table}");
    drop((east_d, west_d));
}

#[test]
fn a_node_that_goes_dark_loses_its_tasks_to_another() {
    let job = r#"job = { cmd = { "/bin/sh", "-c", "sleep 1.5; echo ok > out" } },"#;
    let east = node_home("east2", job);
    let west = node_home("west2", job);
    let leader = scratch("leader2");
    node_policy(&leader, &[("east", &east, 2), ("west", &west, 2)], "");
    // East is the only node up at first.
    let mut east_d = start(&east);
    let daemon = start(&leader);
    wait(&leader, &daemon, |v| v["nodes"][0]["ready"] == true);
    for goal in ["a", "b"] {
        inlet(
            &leader,
            &["add", "-w", "job", "-g", goal, "--verify", "grep -q ok out"],
        );
    }
    wait(&leader, &daemon, |v| v["nodes"][0]["placed"] == 2);
    wait(&east, &east_d, |v| v["live"] == 2);
    // East dies, daemon and workers both. West comes up.
    unsafe { libc::kill(east_d.child.id() as i32, libc::SIGINT) };
    east_d.child.wait().unwrap();
    let west_d = start(&west);
    let st = wait(&leader, &daemon, |v| {
        tasks(v).iter().filter(|t| t["state"] == "done").count() == 2
    });
    let all = tasks(&st);
    // Lost with the node, and placed again as the same attempt.
    let lost: Vec<&Value> = all.iter().filter(|t| t["reason"] == "node").collect();
    assert_eq!(lost.len(), 2, "{st}");
    for t in all.iter().filter(|t| t["state"] == "done") {
        assert_eq!(t["attempt"], 1, "{st}");
        assert!(lost.iter().any(|l| l["id"] == t["retry_of"]), "{st}");
    }
    let west_done = tasks(&status(&west))
        .iter()
        .filter(|t| t["state"] == "done")
        .count();
    assert_eq!(west_done, 2);
    assert_eq!(st["nodes"][0]["ready"], false, "{st}");
    assert_eq!(st["available"], 10000, "{st}");
    drop(west_d);
}

#[test]
fn a_leader_that_dies_finds_its_placed_tasks_again() {
    let job = r#"job = { cmd = { "/bin/sh", "-c", "sleep 2; echo ok > out" } },"#;
    let east = node_home("east3", job);
    let leader = scratch("leader3");
    node_policy(&leader, &[("east", &east, 4)], "");
    let east_d = start(&east);
    let mut daemon = start(&leader);
    wait(&leader, &daemon, |v| v["nodes"][0]["ready"] == true);
    for goal in ["a", "b", "c"] {
        inlet(
            &leader,
            &["add", "-w", "job", "-g", goal, "--verify", "grep -q ok out"],
        );
    }
    wait(&east, &east_d, |v| v["live"] == 3);
    kill9(&mut daemon);
    let daemon = start(&leader);
    let st = wait(&leader, &daemon, |v| v["live"] == 0 && v["queued"] == 0);
    let all = tasks(&st);
    assert_eq!(all.len(), 3, "no crash, no second placement: {st}");
    assert!(all.iter().all(|t| t["state"] == "done"), "{st}");
    // Asked for once each, however many times the leader looked.
    assert_eq!(tasks(&status(&east)).len(), 3);

    // A kill goes through to the node.
    inlet(
        &leader,
        &[
            "add",
            "-w",
            "job",
            "-g",
            "doomed",
            "--verify",
            "grep -q ok out",
        ],
    );
    wait(&east, &east_d, |v| v["live"] == 1);
    let doomed = tasks(&status(&leader))
        .into_iter()
        .find(|t| t["goal"] == "doomed")
        .unwrap();
    inlet(&leader, &["kill", doomed["id"].as_str().unwrap()]);
    let st = wait(&leader, &daemon, |v| v["live"] == 0);
    let killed = tasks(&st)
        .into_iter()
        .find(|t| t["goal"] == "doomed")
        .unwrap();
    assert_eq!(killed["state"], "killed", "{st}");
    assert_eq!(st["available"], 10000, "{st}");
    drop(east_d);
}

#[test]
fn a_task_no_node_will_take_does_not_wait_for_ever() {
    // The node is up but has no worker called `job`.
    let east = node_home("east4", r#"other = { cmd = { "true" } },"#);
    let leader = scratch("leader4");
    node_policy(
        &leader,
        &[("east", &east, 4)],
        r#"workers.lost = { on = "nowhere" }"#,
    );
    let east_d = start(&east);
    let daemon = start(&leader);
    wait(&leader, &daemon, |v| v["nodes"][0]["ready"] == true);
    inlet(
        &leader,
        &["add", "-w", "job", "-g", "unwanted", "--no-verify"],
    );
    inlet(
        &leader,
        &["add", "-w", "lost", "-g", "adrift", "--no-verify"],
    );
    let st = wait(&leader, &daemon, |v| v["live"] == 0 && v["queued"] == 0);
    for goal in ["unwanted", "adrift"] {
        let task = tasks(&st).into_iter().find(|t| t["goal"] == goal).unwrap();
        assert_eq!(
            (task["state"].as_str(), task["reason"].as_str()),
            (Some("failed"), Some("placement")),
            "{st}"
        );
    }
    assert_eq!(tasks(&st).len(), 2, "not retried: {st}");
    assert_eq!(st["available"], 10000, "{st}");
    // A stale pid file is not a daemon.
    let stale = scratch("stale");
    fs::write(
        stale.join("run/inlet.pid"),
        format!("{}\n", std::process::id()),
    )
    .unwrap();
    let out = run(&stale, &["down"]);
    assert!(!out.status.success());
    drop(east_d);
}

#[test]
fn a_node_lets_go_of_work_when_its_leader_goes_quiet() {
    let job = r#"job = { cmd = { "/bin/sh", "-c", "sleep 30; echo ok > out" } },"#;
    let east = node_home("east5", job);
    let leader = scratch("leader5");
    node_policy(&leader, &[("east", &east, 4)], "node_grace = 4");
    let east_d = start(&east);
    let mut daemon = start(&leader);
    wait(&leader, &daemon, |v| v["nodes"][0]["ready"] == true);
    for goal in ["a", "b"] {
        inlet(&leader, &["add", "-w", "job", "-g", goal, "--no-verify"]);
    }
    wait(&east, &east_d, |v| v["live"] == 2);
    // The leader vanishes. Half its patience later the node has stopped the
    // work on its own, before any leader would place it somewhere else.
    kill9(&mut daemon);
    let st = wait(&east, &east_d, |v| v["live"] == 0);
    assert!(tasks(&st).iter().all(|t| t["reason"] == "fenced"), "{st}");
    assert_eq!(st["available"], 10000, "{st}");
    // Back again, the leader reads that as the node's failure and places anew.
    let daemon = start(&leader);
    let st = wait(&leader, &daemon, |v| {
        tasks(v).iter().filter(|t| t["reason"] == "node").count() == 2
    });
    assert_eq!(tasks(&st).len(), 4, "each placed once more: {st}");
    drop(east_d);
}

#[test]
fn a_standby_takes_over_when_the_leader_dies() {
    let job = r#"job = { cmd = { "/bin/sh", "-c", "sleep 0.4; echo ok > out" } },"#;
    let east = node_home("east6", job);
    let leader = scratch("leader6");
    node_policy(&leader, &[("east", &east, 3)], "");
    let east_d = start(&east);
    let mut daemon = start(&leader);
    wait(&leader, &daemon, |v| v["nodes"][0]["ready"] == true);
    // A second daemon on the same home. It can do nothing but wait.
    let log_path = leader.join("standby.log");
    let log = File::create(&log_path).unwrap();
    let standby = Command::new(bin())
        .args(["--home", leader.to_str().unwrap(), "up", "-f", "--standby"])
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    let standby = Daemon {
        child: standby,
        log_path,
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while !standby.log().contains("standing by") {
        assert!(Instant::now() < deadline, "standby never stood by");
        thread::sleep(Duration::from_millis(50));
    }
    manifest(
        &leader,
        r#"{"kind":"swarm","name":"far","replicas":3,"completions":12,
            "template":{"worker":"job","goal":"g {index}","verify":"grep -q ok out"}}"#,
    );
    wait(&leader, &daemon, |v| {
        swarm(v, "far")["done"].as_u64().unwrap_or(0) >= 3
    });
    kill9(&mut daemon);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !standby.log().contains("taking over") {
        assert!(Instant::now() < deadline, "standby never took over");
        thread::sleep(Duration::from_millis(50));
    }
    // It has the lock. Give it a moment to replay and open its sockets.
    while !run(&leader, &["status", "--json"]).status.success() {
        assert!(Instant::now() < deadline, "standby never answered");
        thread::sleep(Duration::from_millis(50));
    }
    // The same home, a different daemon, the swarm where it was.
    let st = wait(&leader, &standby, |v| swarm(v, "far")["phase"] == "done");
    assert_eq!(swarm(&st, "far")["done"], 12, "{st}");
    assert_eq!(
        swarm(&st, "far")["created"],
        12,
        "nothing placed twice: {st}"
    );
    assert_eq!(st["available"], 10000, "{st}");
    drop(east_d);
}

#[test]
fn a_standby_never_leads_while_the_leader_lives_through_a_rotation() {
    let home = scratch("standrot");
    policy(
        &home,
        r#"job = { cmd = { "/bin/sh", "-c", "echo hi > out" } },"#,
        "retain = 1",
    );
    let spawn = |args: &[&str], log: &str| {
        let log_path = home.join(log);
        let file = File::create(&log_path).unwrap();
        let child = Command::new(bin())
            .arg("--home")
            .arg(&home)
            .args(args)
            .env("INLET_ROTATE_BYTES", "2000")
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file))
            .spawn()
            .unwrap();
        Daemon { child, log_path }
    };
    let mut leader = spawn(&["up", "-f"], "leader.log");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !run(&home, &["status", "--json"]).status.success() {
        assert!(Instant::now() < deadline, "leader never answered");
        thread::sleep(Duration::from_millis(50));
    }
    let standby = spawn(&["up", "-f", "--standby"], "standby.log");
    // Many rotations with a standby poking at the lock the whole time.
    manifest(
        &home,
        r#"{"kind":"swarm","name":"many","replicas":4,"completions":40,
            "template":{"worker":"job","goal":"g {index}","verify":"test -s out"}}"#,
    );
    wait(&home, &leader, |v| swarm(v, "many")["phase"] == "done");
    thread::sleep(Duration::from_millis(2500));
    let rotations = leader.log().matches("rotated the ledger").count();
    assert!(
        rotations >= 2,
        "only {rotations} rotations: {}",
        leader.log()
    );
    assert!(
        !standby.log().contains("taking over"),
        "two leaders: {}",
        standby.log()
    );
    assert!(
        leader.child.try_wait().unwrap().is_none(),
        "leader died: {}",
        leader.log()
    );
    // And when the leader really goes, the standby has all of it.
    kill9(&mut leader);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !standby.log().contains("taking over")
        || !run(&home, &["status", "--json"]).status.success()
    {
        assert!(
            Instant::now() < deadline,
            "standby never took over: {}",
            standby.log()
        );
        thread::sleep(Duration::from_millis(50));
    }
    let st = status(&home);
    assert_eq!(swarm(&st, "many")["done"], 40, "{st}");
    assert_eq!(st["available"], 10000, "{st}");
}
