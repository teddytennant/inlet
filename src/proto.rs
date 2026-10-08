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
}

#[derive(Debug)]
pub enum Request {
    Status,
    Add(NewTask),
    AddBatch(Vec<NewTask>),
    Post { text: String, human: bool },
    Kill(String),
    Watch { debug: u8, worker: Option<String> },
    Hello { debug: u8 },
    Debug(u8),
}

pub fn parse_request(line: &str) -> Result<Request> {
    let v: Value = serde_json::from_str(line)?;
    let op = v.get("op").and_then(|o| o.as_str()).unwrap_or("");
    match op {
        "status" => Ok(Request::Status),
        "add" => Ok(Request::Add(parse_task(&v)?)),
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
    })
}
