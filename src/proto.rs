use std::path::Path;

use serde_json::{json, Value};

use crate::error::{err, Result};
use crate::paths;

#[derive(Debug, Clone)]
pub struct NewTask {
    pub worker: String,
    pub goal: String,
    pub verifier: Option<String>,
    pub no_verify: bool,
    pub tokens: Option<u64>,
    pub seconds: Option<u64>,
    pub memory_mb: Option<u64>,
    pub pids: Option<u64>,
    pub value: Option<u64>,
    pub tags: Vec<String>,
    pub parent: Option<String>,
    pub recipe: Option<String>,
    pub seed: Option<String>,
    /// Retries after the first attempt. `None` is the policy's `retry.max`.
    pub retries: Option<u32>,
}

/// A swarm manifest, as `inlet apply` sends it.
#[derive(Debug, Clone)]
pub struct SwarmSpec {
    pub name: String,
    pub replicas: u64,
    /// `None` is one run per replica.
    pub completions: Option<u64>,
    pub backoff_limit: Option<u32>,
    pub template: NewTask,
}

#[derive(Debug)]
pub enum Request {
    /// `brief` leaves out the task list, which grows with history.
    Status {
        brief: bool,
    },
    Add(Box<NewTask>),
    AddBatch(Vec<NewTask>),
    Post {
        text: String,
        human: bool,
    },
    Kill(String),
    Watch {
        debug: u8,
        worker: Option<String>,
    },
    Hello {
        debug: u8,
    },
    Debug(u8),
    Pin(String),
    Bind {
        text: String,
        ttl_ms: u64,
    },
    Clear {
        id: String,
        sig: Option<String>,
    },
    Apply(Box<SwarmSpec>),
    Scale {
        name: String,
        replicas: u64,
    },
    Delete {
        name: String,
        kill: bool,
    },
    Describe(String),
    Sign(String),
    Draft(String),
    Diff,
    Snap,
    Vote {
        target: String,
        choice: String,
        channel: String,
        human: bool,
    },
    Digest(String),
}

pub fn parse_request(line: &str) -> Result<Request> {
    let v: Value = serde_json::from_str(line)?;
    if v.get("passphrase").is_some() {
        return Err(err("passphrase stays on the tty"));
    }
    let op = v.get("op").and_then(|o| o.as_str()).unwrap_or("");
    match op {
        "status" => Ok(Request::Status {
            brief: v.get("brief").and_then(|b| b.as_bool()).unwrap_or(false),
        }),
        "add" => Ok(Request::Add(Box::new(parse_task(&v)?))),
        "add_batch" => {
            let tasks = v
                .get("tasks")
                .and_then(|t| t.as_array())
                .ok_or_else(|| err("add_batch needs tasks"))?;
            let mut out = Vec::with_capacity(tasks.len());
            for task in tasks {
                out.push(parse_task(task)?);
            }
            Ok(Request::AddBatch(out))
        }
        "post" => Ok(Request::Post {
            text: string(&v, "text")?,
            human: false,
        }),
        "say" => Ok(Request::Post {
            text: string(&v, "text")?,
            human: true,
        }),
        "kill" => Ok(Request::Kill(string(&v, "id")?)),
        "watch" => Ok(Request::Watch {
            debug: v.get("debug").and_then(|n| n.as_u64()).unwrap_or(1) as u8,
            worker: v.get("worker").and_then(|s| s.as_str()).map(str::to_string),
        }),
        "hello" => Ok(Request::Hello {
            debug: v.get("debug").and_then(|n| n.as_u64()).unwrap_or(1) as u8,
        }),
        "debug" => Ok(Request::Debug(
            v.get("level").and_then(|n| n.as_u64()).unwrap_or(1).min(4) as u8,
        )),
        "pin" => Ok(Request::Pin(string(&v, "name")?)),
        "bind" => Ok(Request::Bind {
            text: string(&v, "text")?,
            ttl_ms: num(&v, "ttl_ms").unwrap_or(0),
        }),
        "clear" => Ok(Request::Clear {
            id: string(&v, "id")?,
            sig: string(&v, "sig").ok(),
        }),
        "apply" => Ok(Request::Apply(Box::new(parse_swarm(&v)?))),
        "scale" => Ok(Request::Scale {
            name: string(&v, "name")?,
            replicas: num(&v, "replicas").ok_or_else(|| err("missing replicas"))?,
        }),
        "delete" => Ok(Request::Delete {
            name: string(&v, "name")?,
            kill: v.get("kill").and_then(|b| b.as_bool()).unwrap_or(false),
        }),
        "describe" => Ok(Request::Describe(string(&v, "id")?)),
        "sign" => Ok(Request::Sign(string(&v, "sig")?)),
        "draft" => Ok(Request::Draft(string(&v, "text")?)),
        "diff" => Ok(Request::Diff),
        "snap" => Ok(Request::Snap),
        "digest" => Ok(Request::Digest(
            v.get("channel")
                .and_then(|s| s.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("general")
                .to_string(),
        )),
        "vote" => Ok(Request::Vote {
            target: string(&v, "target")?,
            choice: string(&v, "choice")?,
            channel: v
                .get("channel")
                .and_then(|s| s.as_str())
                .filter(|s| !s.is_empty())
                .unwrap_or("general")
                .to_string(),
            human: v.get("human").and_then(|b| b.as_bool()).unwrap_or(false),
        }),
        other => Err(err(format!("unknown op {other}"))),
    }
}

pub fn parse_task(v: &Value) -> Result<NewTask> {
    let no_verify = v
        .get("no_verify")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    let verifier = v
        .get("verify")
        .or_else(|| v.get("verifier"))
        .and_then(|s| s.as_str())
        .map(str::to_string);
    if verifier.is_some() && no_verify {
        return Err(err("pass either --verify or --no-verify"));
    }
    if verifier.is_none() && !no_verify {
        return Err(err("pass --verify or --no-verify"));
    }
    let tags = v
        .get("tags")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    Ok(NewTask {
        worker: string(v, "worker")?,
        goal: string(v, "goal")?,
        verifier,
        no_verify,
        tokens: num(v, "tokens"),
        seconds: num(v, "seconds"),
        memory_mb: num(v, "memory_mb"),
        pids: num(v, "pids"),
        value: num(v, "value"),
        tags,
        parent: v.get("parent").and_then(|s| s.as_str()).map(str::to_string),
        recipe: v
            .get("recipe")
            .and_then(|s| s.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty()),
        seed: v
            .get("seed")
            .and_then(|s| s.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty()),
        retries: num(v, "retries").map(|n| n.min(64) as u32),
    })
}

/// `{"kind":"swarm","name":..,"replicas":N,"completions":M,"template":{..}}`.
/// The template takes the fields of `add`.
pub fn parse_swarm(v: &Value) -> Result<SwarmSpec> {
    let name = string(v, "name")?;
    let ok = name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.');
    if !ok {
        return Err(err("swarm name is letters, digits, - _ . and at most 64"));
    }
    let template = v
        .get("template")
        .ok_or_else(|| err("swarm needs a template"))?;
    let mut template = parse_task(template)?;
    if template.parent.is_some() {
        return Err(err("a swarm template has no parent"));
    }
    template.parent = None;
    for key in ["replicas", "completions", "backoff_limit"] {
        if v.get(key)
            .is_some_and(|n| !n.is_null() && n.as_u64().is_none())
        {
            return Err(err(format!("{key} is a whole number, zero or more")));
        }
    }
    Ok(SwarmSpec {
        name,
        replicas: num(v, "replicas").unwrap_or(1),
        completions: num(v, "completions"),
        backoff_limit: num(v, "backoff_limit").map(|n| n.min(u32::MAX as u64) as u32),
        template,
    })
}

fn string(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(|s| s.as_str())
        .map(str::to_string)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| err(format!("missing {key}")))
}

fn num(v: &Value, key: &str) -> Option<u64> {
    v.get(key).and_then(|n| n.as_u64())
}

pub fn rpc(home: &Path, body: Value) -> Result<Value> {
    use std::io::{BufRead, BufReader, Write};
    let mut stream = std::os::unix::net::UnixStream::connect(paths::operator_sock(home))
        .map_err(|_| err("daemon is not up"))?;
    let mut line = serde_json::to_string(&body)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response)?;
    if response.is_empty() {
        return Err(err("empty reply"));
    }
    Ok(serde_json::from_str(&response)?)
}

pub fn task_value(task: &NewTask) -> Value {
    json!({
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
        "seed": task.seed,
        "retries": task.retries,
    })
}
