use serde::{Deserialize, Serialize};

use crate::error::{err, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Blocked,
    Done,
    Failed,
    Killed,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Queued => "queued",
            TaskState::Running => "running",
            TaskState::Blocked => "blocked",
            TaskState::Done => "done",
            TaskState::Failed => "failed",
            TaskState::Killed => "killed",
        }
    }

    pub fn is_live(self) -> bool {
        matches!(self, TaskState::Running | TaskState::Blocked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub tokens: u64,
    pub seconds: u64,
    pub memory_mb: u64,
    pub pids: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Task {
        id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent: Option<String>,
        worker: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
        goal: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        verifier: Option<String>,
        value: u64,
        budget: Budget,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_of: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recipe: Option<String>,
        ts: u64,
    },
    Admit {
        id: String,
        fence: u64,
        tokens: u64,
        seconds: u64,
        memory_mb: u64,
        pids: u64,
        ts: u64,
    },
    Deny {
        id: String,
        reason: String,
        ts: u64,
    },
    Spawn {
        id: String,
        pid: u32,
        ts: u64,
    },
    Exit {
        id: String,
        code: i32,
        reason: String,
        tokens_used: u64,
        refund_tokens: u64,
        ts: u64,
    },
    Post {
        id: String,
        author: String,
        role: String,
        text: String,
        weight: u64,
        channel: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        mentions: Vec<String>,
        ts: u64,
    },
    Cost {
        id: String,
        tokens: u64,
        ts: u64,
    },
    Kill {
        id: String,
        ts: u64,
    },
    Reset {
        grant: u64,
        ts: u64,
    },
    /// Supervisor ran the verifier. `ok` is exit 0.
    Result {
        id: String,
        ok: bool,
        code: i32,
        ts: u64,
    },
    Promote {
        name: String,
        by: String,
        ts: u64,
    },
}

#[derive(Debug)]
pub enum Decoded {
    Rec(Box<Record>),
    /// A future kind (`vote`, `moderation`, …). Valid framing, not our problem yet.
    Opaque,
}

pub fn decode_record(value: serde_json::Value) -> Result<Decoded> {
    let kind = value
        .get("kind")
        .and_then(|k| k.as_str())
        .ok_or_else(|| err("record missing kind"))?;
    match kind {
        "task" | "admit" | "deny" | "spawn" | "exit" | "post" | "cost" | "kill" | "reset"
        | "result" | "promote" => Ok(Decoded::Rec(Box::new(serde_json::from_value(value)?))),
        _ => Ok(Decoded::Opaque),
    }
}

pub fn encode_record(rec: &Record) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(rec)?)
}
