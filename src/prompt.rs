//! What a worker is told. One contract for every harness: the preamble, the
//! goal, how the result is judged, and what the last attempt got wrong.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use serde_json::json;

use crate::error::{err, Result};

pub const PREAMBLE: &str = "\
You are worker {id} in inlet, attempt {attempt} of {max_attempts}. Budget: {tokens} tokens, {seconds}s, {memory_mb}MB. Depth {depth}/{max_depth}.

Nobody is watching this run and nobody will answer a question. Decide, act, and finish. Work in the current directory. Exit 0 when you are done. The supervisor judges the result. You do not grade yourself.

`$INLET_BIN door` is the way out of the cell:
  post <text>                                 speak on the board. Your last post is what your parent reads.
  board                                       read the channels you are on
  spawn -w <worker> -g <goal> --verify <cmd>  hand a part to a child. It spends your budget.
  wait                                        block until your children settle and print how each ended
  draft <name> --run <file> --verify <cmd>    offer a tool you built to the registry
  vote <target> <choice>                      weigh in when the board asks

$INLET_REGISTRY holds tools another worker built and a second one verified. Look there before you build.

You have no API keys. Model calls go through the proxy and stop when the budget is spent. You cannot kill, rebudget, or raise caps.
";

/// Bytes of the last verifier output and transcript a retry gets to read.
const VERIFIER_TAIL: u64 = 2000;
const TRANSCRIPT_TAIL: u64 = 1500;
/// Linux refuses a single argv string past 128KB.
const ARG_MAX: usize = 120 * 1024;

pub struct Ctx<'a> {
    pub id: &'a str,
    pub worker: &'a str,
    pub tags: &'a [String],
    pub goal: &'a str,
    pub verifier: Option<&'a str>,
    pub tokens: u64,
    pub seconds: u64,
    pub memory_mb: u64,
    pub depth: u32,
    pub max_depth: u32,
    pub attempt: u32,
    pub max_attempts: u32,
    pub parent: Option<&'a str>,
    pub owner: Option<&'a str>,
    pub last: Option<Last>,
}

/// The attempt this one follows.
pub struct Last {
    pub id: String,
    pub reason: String,
    pub verifier_tail: String,
    pub transcript_tail: String,
}

impl Last {
    pub fn load(id: &str, reason: &str, verify_log: &Path, transcript: &Path) -> Self {
        Last {
            id: id.to_string(),
            reason: reason.to_string(),
            verifier_tail: tail(verify_log, VERIFIER_TAIL),
            transcript_tail: tail(transcript, TRANSCRIPT_TAIL),
        }
    }
}

pub fn preamble(ctx: &Ctx<'_>, template: Option<&str>) -> String {
    template
        .unwrap_or(PREAMBLE)
        .replace("{id}", ctx.id)
        .replace("{worker}", ctx.worker)
        .replace("{tags}", &ctx.tags.join(","))
        .replace("{tokens}", &ctx.tokens.to_string())
        .replace("{seconds}", &ctx.seconds.to_string())
        .replace("{memory_mb}", &ctx.memory_mb.to_string())
        .replace("{depth}", &ctx.depth.to_string())
        .replace("{max_depth}", &ctx.max_depth.to_string())
        .replace("{attempt}", &ctx.attempt.to_string())
        .replace("{max_attempts}", &ctx.max_attempts.to_string())
        .replace("{verifier}", ctx.verifier.unwrap_or(""))
        // Last, so braces inside a goal are never read as fields.
        .replace("{goal}", ctx.goal)
}

/// The whole prompt. Sections appear only when they apply.
pub fn compose(ctx: &Ctx<'_>, template: Option<&str>) -> String {
    let mut out = preamble(ctx, template);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str("\n## Task\n");
    out.push_str(ctx.goal.trim_end());
    out.push_str("\n\n## Done means\n");
    match ctx.verifier.filter(|v| !v.trim().is_empty()) {
        Some(verifier) => {
            out.push_str(
                "After you exit 0 the supervisor runs this in your working directory. \
                 Its exit 0 is the only thing that counts as done:\n\n    ",
            );
            out.push_str(&verifier.trim().replace('\n', "\n    "));
            out.push('\n');
        }
        None => out.push_str(
            "Nothing checks this task. Exit 0 is recorded as done, so post what you did.\n",
        ),
    }
    if let Some(last) = &ctx.last {
        out.push_str(&format!(
            "\n## Last attempt\nAttempt {} ({}) ended: {}. Your working directory is as it left it. Do not repeat what failed.\n",
            ctx.attempt.saturating_sub(1).max(1),
            last.id,
            explain(&last.reason, ctx.seconds),
        ));
        if !last.verifier_tail.trim().is_empty() {
            out.push_str("\nVerifier output, last lines:\n");
            out.push_str(&indent(&last.verifier_tail));
        }
        if !last.transcript_tail.trim().is_empty() {
            out.push_str("\nYour output, last lines:\n");
            out.push_str(&indent(&last.transcript_tail));
        }
    }
    out
}

/// The header as data, for a harness that would rather parse than read.
pub fn task_json(ctx: &Ctx<'_>) -> String {
    let value = json!({
        "id": ctx.id,
        "worker": ctx.worker,
        "tags": ctx.tags,
        "goal": ctx.goal,
        "verifier": ctx.verifier,
        "budget": {"tokens": ctx.tokens, "seconds": ctx.seconds, "memory_mb": ctx.memory_mb},
        "depth": ctx.depth,
        "max_depth": ctx.max_depth,
        "attempt": ctx.attempt,
        "max_attempts": ctx.max_attempts,
        "parent": ctx.parent,
        "swarm": ctx.owner,
        "last": ctx.last.as_ref().map(|last| json!({
            "id": last.id,
            "reason": last.reason,
            "verifier_tail": last.verifier_tail,
            "transcript_tail": last.transcript_tail,
        })),
    });
    let mut out = value.to_string();
    out.push('\n');
    out
}

/// Fill `{prompt}`, `{goal}`, and `{id}` into a worker command. Each argument
/// is scanned once, so text that arrives in a goal is never expanded again.
pub fn fill(cmd: &[String], prompt: &str, goal: &str, id: &str) -> Result<Vec<String>> {
    let fields = [("{prompt}", prompt), ("{goal}", goal), ("{id}", id)];
    let mut out = Vec::with_capacity(cmd.len());
    for arg in cmd {
        let mut filled = String::with_capacity(arg.len());
        let mut rest = arg.as_str();
        'scan: while !rest.is_empty() {
            for (key, value) in fields {
                if let Some(after) = rest.strip_prefix(key) {
                    filled.push_str(value);
                    rest = after;
                    continue 'scan;
                }
            }
            let ch = rest.chars().next().unwrap_or(' ');
            filled.push(ch);
            rest = &rest[ch.len_utf8()..];
        }
        if filled.len() > ARG_MAX {
            return Err(err(
                "prompt is too long for an argument; set stdin = \"prompt\" on the worker",
            ));
        }
        if filled.contains('\0') {
            return Err(err("prompt contains a nul"));
        }
        out.push(filled);
    }
    Ok(out)
}

fn explain(reason: &str, seconds: u64) -> String {
    match reason {
        "verifier" => "the verifier did not exit 0".into(),
        "timeout" => format!("it ran past its {seconds}s"),
        "crash" => "the process was killed, or the supervisor went down under it".into(),
        "exit" => "the worker exited non-zero".into(),
        "stall" => "it went silent and was stopped".into(),
        "gate" => "it was never admitted; the gate was unreachable".into(),
        "stopped" => "the supervisor was shut down while it ran".into(),
        other => other.to_string(),
    }
}

fn indent(text: &str) -> String {
    let mut out = String::new();
    for line in text.trim_end().lines() {
        out.push_str("    ");
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// The last `max` bytes of a file as text, secrets stripped.
pub fn tail(path: &Path, max: u64) -> String {
    let Ok(mut file) = File::open(path) else {
        return String::new();
    };
    let len = file.metadata().map(|m| m.len()).unwrap_or(0);
    let cut = len > max;
    if cut && file.seek(SeekFrom::Start(len - max)).is_err() {
        return String::new();
    }
    let mut bytes = Vec::new();
    if file.take(max).read_to_end(&mut bytes).is_err() {
        return String::new();
    }
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if cut {
        // Drop the partial first line.
        if let Some(pos) = text.find('\n') {
            text.drain(..=pos);
        }
    }
    crate::text::redact(&text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(goal: &'a str, verifier: Option<&'a str>, last: Option<Last>) -> Ctx<'a> {
        Ctx {
            id: "T1",
            worker: "pi",
            tags: &[],
            goal,
            verifier,
            tokens: 100,
            seconds: 30,
            memory_mb: 64,
            depth: 0,
            max_depth: 3,
            attempt: if last.is_some() { 2 } else { 1 },
            max_attempts: 3,
            parent: None,
            owner: None,
            last,
        }
    }

    #[test]
    fn sections_follow_the_task() {
        let plain = compose(&ctx("fix it", None, None), None);
        assert!(plain.contains("worker T1"));
        assert!(plain.contains("attempt 1 of 3"));
        assert!(plain.contains("## Task\nfix it\n"));
        assert!(plain.contains("Nothing checks this task"));
        assert!(!plain.contains("## Last attempt"));

        let last = Last {
            id: "T0".into(),
            reason: "verifier".into(),
            verifier_tail: "assert 1 == 2\n".into(),
            transcript_tail: String::new(),
        };
        let again = compose(&ctx("fix it", Some("make test"), Some(last)), None);
        assert!(again.contains("    make test\n"));
        assert!(again.contains("Attempt 1 (T0) ended: the verifier did not exit 0"));
        assert!(again.contains("    assert 1 == 2\n"));
        assert!(!again.contains("Your output, last lines"));
    }

    #[test]
    fn a_goal_is_not_a_template() {
        let c = ctx("print {id} and {prompt}", None, None);
        let text = compose(&c, Some("I am {id}. {goal}"));
        assert!(text.starts_with("I am T1. print {id} and {prompt}\n"));
        let argv = fill(
            &[
                "agent".into(),
                "-p".into(),
                "{prompt}".into(),
                "--tag={id}".into(),
            ],
            "do {goal} now",
            "g",
            "T1",
        )
        .unwrap();
        assert_eq!(argv, ["agent", "-p", "do {goal} now", "--tag=T1"]);
        assert!(fill(&["{prompt}".into()], &"x".repeat(ARG_MAX + 1), "", "").is_err());
    }

    #[test]
    fn tail_keeps_whole_lines() {
        let path = std::env::temp_dir().join(format!("inlet-tail-{}", std::process::id()));
        std::fs::write(&path, "first line\nsecond line\nthird line\n").unwrap();
        assert_eq!(tail(&path, 20), "third line\n");
        assert_eq!(tail(&path, 4096), "first line\nsecond line\nthird line\n");
        assert_eq!(tail(Path::new("/nonexistent/inlet"), 10), "");
        let _ = std::fs::remove_file(&path);
    }
}
