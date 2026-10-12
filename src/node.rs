//! Other machines. A node is an inlet daemon of its own, reached by running
//! its CLI through a command prefix, usually ssh. The leader decides where a
//! task goes and watches how it ends. The node does everything else: the
//! cell, the proxy, the verifier, retries, and taking its workers back.

use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

/// How long one remote command may take, at most.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(20);
pub const POLL_EVERY: Duration = Duration::from_secs(2);

/// The tag a placed task carries on its node, so it can be found again by
/// the leader's id and never added twice.
pub fn tag(id: &str) -> String {
    format!("inlet:{id}")
}

#[derive(Debug, Clone)]
pub struct Remote {
    pub id: String,
    pub state: String,
    pub reason: String,
    pub used: u64,
    /// The leader's id, from the tag.
    pub leader: String,
}

pub enum Msg {
    /// `None` is a node that did not answer.
    Status {
        node: String,
        tasks: Option<Vec<Remote>>,
    },
    /// A node has heard from a newer leader of the same home.
    Deposed,
    Added {
        id: String,
        ok: bool,
        /// The node replied. Not ok and answered is a no; not answered is a bad link.
        answered: bool,
    },
}

/// Run the node's CLI with `args` and return what it printed. `None` when it
/// could not be started or hung.
pub fn call(
    cmd: &[String],
    args: &[String],
    input: Option<&[u8]>,
    limit: Duration,
) -> Option<String> {
    let (program, rest) = cmd.split_first()?;
    let mut child = Command::new(program)
        .args(rest)
        .args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    if let (Some(bytes), Some(mut stdin)) = (input, child.stdin.take()) {
        // On a thread: a link that has stopped reading would block the write,
        // and the timeout below is what has to end that.
        let bytes = bytes.to_vec();
        thread::spawn(move || {
            use std::io::Write;
            let _ = stdin.write_all(&bytes);
        });
    }
    let mut stdout = child.stdout.take()?;
    let reader = thread::spawn(move || {
        let mut out = String::new();
        let _ = stdout.read_to_string(&mut out);
        out
    });
    // The daemon reaps every child it sees, and may take this one's status
    // before this thread does. So the exit code cannot be relied on: wait for
    // the output to end, and let the caller judge by what was printed.
    let started = Instant::now();
    while !reader.is_finished() {
        if started.elapsed() >= limit {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.try_wait();
    reader.join().ok()
}

/// Ask a node, forever, what it is running for this leader.
pub fn watch(name: String, cmd: Vec<String>, tx: Sender<Msg>, grace_ms: u64, me: String) {
    // Often enough that a healthy node hears from its leader several times
    // inside the lease, which is half the grace.
    let every = Duration::from_millis((grace_ms / 8).clamp(200, POLL_EVERY.as_millis() as u64));
    // One call that hangs must not use up the whole grace. A quarter of it,
    // and the next poll gets its chance.
    let limit = Duration::from_millis(grace_ms / 4).clamp(Duration::from_secs(1), CALL_TIMEOUT);
    thread::spawn(move || loop {
        // `--leader` tells the node this leader is still here. A node that
        // stops hearing it lets go of what the leader placed.
        let raw = call(
            &cmd,
            &[
                "status".into(),
                "--json".into(),
                "--leader".into(),
                me.clone(),
            ],
            None,
            limit,
        );
        if raw.as_deref().is_some_and(deposed) {
            let _ = tx.send(Msg::Deposed);
            return;
        }
        let tasks = raw.and_then(|out| parse(&out));
        if tx
            .send(Msg::Status {
                node: name.clone(),
                tasks,
            })
            .is_err()
        {
            return;
        }
        thread::sleep(every);
    });
}

fn deposed(out: &str) -> bool {
    serde_json::from_str::<Value>(out).is_ok_and(|v| v["deposed"] == true)
}

fn parse(out: &str) -> Option<Vec<Remote>> {
    let v: Value = serde_json::from_str(out).ok()?;
    let tasks = v.get("tasks")?.as_array()?;
    Some(
        tasks
            .iter()
            .filter_map(|t| {
                let leader = t["tags"]
                    .as_array()?
                    .iter()
                    .find_map(|tag| tag.as_str()?.strip_prefix("inlet:"))?
                    .to_string();
                Some(Remote {
                    id: t["id"].as_str()?.to_string(),
                    state: t["state"].as_str()?.to_string(),
                    reason: t["reason"].as_str().unwrap_or("").to_string(),
                    used: t["used"].as_u64().unwrap_or(0),
                    leader,
                })
            })
            .collect(),
    )
}

/// Enqueue on a node. The task goes over as one line of JSON on stdin, not
/// as arguments: the prefix is usually ssh, and a goal with a space or a
/// quote in it would not survive the remote shell.
pub fn add(cmd: Vec<String>, task: Value, id: String, tx: Sender<Msg>) {
    thread::spawn(move || {
        let mut line = task.to_string();
        line.push('\n');
        let args = ["add".to_string(), "-f".to_string(), "-".to_string()];
        let out = call(&cmd, &args, Some(line.as_bytes()), CALL_TIMEOUT);
        let ok = out.as_ref().is_some_and(|out| out.contains("queued"));
        // The CLI prints an error and nothing on stdout when the daemon says
        // no, and prints nothing at all when it was never reached. Ask the
        // node which: a status that answers means the no was the node's.
        let answered = ok
            || call(
                &cmd,
                &["status".into(), "--json".into()],
                None,
                CALL_TIMEOUT,
            )
            .is_some_and(|out| out.contains("\"ok\":true"));
        let _ = tx.send(Msg::Added { id, ok, answered });
    });
}

/// Best effort. The leader has already settled the task on its side.
pub fn kill(cmd: Vec<String>, remote: Vec<String>) {
    thread::spawn(move || {
        for id in remote {
            let _ = call(&cmd, &["kill".into(), id], None, CALL_TIMEOUT);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_tagged_tasks_are_this_leaders() {
        let out = r#"{"tasks":[
            {"id":"r1","state":"done","reason":"ok","used":7,"tags":["code","inlet:L1"]},
            {"id":"r2","state":"running","reason":"","tags":["code"]},
            {"id":"r3","state":"failed","reason":"verifier","tags":["inlet:L2"]}]}"#;
        let got = parse(out).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].leader.as_str(), got[0].used), ("L1", 7));
        assert_eq!(got[1].reason, "verifier");
        assert!(parse("not json").is_none());
        assert_eq!(
            call(&["false".into()], &[], None, CALL_TIMEOUT).as_deref(),
            Some("")
        );
        assert!(call(&["/nonexistent/inlet".into()], &[], None, CALL_TIMEOUT).is_none());
        assert_eq!(
            call(&["echo".into()], &["hi".into()], None, CALL_TIMEOUT).as_deref(),
            Some("hi\n")
        );
        assert_eq!(
            call(&["cat".into()], &[], Some(b"in"), CALL_TIMEOUT).as_deref(),
            Some("in")
        );
    }
}
