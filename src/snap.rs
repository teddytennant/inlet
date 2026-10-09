//! A snapshot is a git commit of the registry, the signed policy, and a
//! header index tagged with the log offset it covers. The live log is not
//! a commit per event.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::config::{Config, Policy};
use crate::error::{err, Result};
use crate::model::{Budget, Decoded, TaskState};
use crate::paths;
use crate::purse::{OpenSlice, Purse};
use crate::state::{
    ChannelStat, ConstraintView, ModerationView, PostView, State, TaskView, VoteView,
};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Lease {
    pub gen: u64,
    pub node: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Index {
    pub offset: u64,
    fence: u64,
    headers: Vec<HeaderSnap>,
    queue: Vec<String>,
    posts: Vec<PostSnap>,
    samples: BTreeMap<String, Vec<u64>>,
    constraints: Vec<ConstraintSnap>,
    retried: Vec<String>,
    decision_spent: u64,
    purse: PurseSnap,
    #[serde(default)]
    votes: Vec<VoteSnap>,
    #[serde(default)]
    moderation: Vec<ModerationSnap>,
    #[serde(default)]
    posts_seen: u64,
    #[serde(default)]
    channels: Vec<ChannelSnap>,
}

#[derive(Clone, Serialize, Deserialize)]
struct HeaderSnap {
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
    state: TaskState,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_of: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recipe: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    admit_ms: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PostSnap {
    id: String,
    author: String,
    role: String,
    text: String,
    weight: u64,
    channel: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mentions: Vec<String>,
    ts: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct VoteSnap {
    id: String,
    voter: String,
    role: String,
    target: String,
    channel: String,
    choice: String,
    weight: u64,
    ts: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct ModerationSnap {
    id: String,
    target: String,
    action: String,
    channel: String,
    weight: u64,
    ts: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct ChannelSnap {
    channel: String,
    posts: u64,
    mentions: u64,
    authors: Vec<String>,
    last_author: String,
    last_text: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct ConstraintSnap {
    id: String,
    text: String,
    tags: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SliceSnap {
    id: String,
    tokens: u64,
    used: u64,
    memory_mb: u64,
    memory_lent: u64,
    pids: u64,
    pids_lent: u64,
    seconds: u64,
    root: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct PurseSnap {
    cap: u64,
    memory_cap: u64,
    pids_cap: u64,
    period_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    period_start: Option<u64>,
    available: u64,
    memory_held: u64,
    pids_held: u64,
    resets: u64,
    open: Vec<SliceSnap>,
}

pub(crate) fn node_id(home: &Path) -> Result<String> {
    let path = paths::node(home);
    if let Ok(text) = fs::read_to_string(&path) {
        let id = text.trim();
        if !id.is_empty() {
            return Ok(id.to_string());
        }
    }
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    durable(&path, format!("{id}\n").as_bytes())?;
    Ok(id)
}

pub(crate) fn ensure_lease(home: &Path, node: &str) -> Result<Lease> {
    let path = paths::lease(home);
    if let Ok(bytes) = fs::read(&path) {
        return Ok(serde_json::from_slice(&bytes)?);
    }
    let lease = Lease {
        gen: 0,
        node: node.to_string(),
    };
    durable(&path, &serde_json::to_vec(&lease)?)?;
    Ok(lease)
}

pub(crate) fn load(home: &Path) -> Result<Option<Index>> {
    let path = paths::snap_index(home);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(&path)?;
    Ok(Some(
        serde_json::from_slice(&bytes).map_err(|_| err("bad snapshot index"))?,
    ))
}

pub(crate) fn store(home: &Path, index: &Index) -> Result<()> {
    durable(&paths::snap_index(home), &serde_json::to_vec(index)?)
}

pub(crate) fn checkpoint(state: &State, offset: u64) -> Index {
    let headers = state
        .tasks
        .values()
        .map(|task| HeaderSnap {
            id: task.id.clone(),
            parent: task.parent.clone(),
            worker: task.worker.clone(),
            tags: task.tags.clone(),
            goal: task.goal.clone(),
            verifier: task.verifier.clone(),
            value: task.value,
            budget: task.budget.clone(),
            state: task.state,
            reason: task.reason.clone(),
            retry_of: task.retry_of.clone(),
            recipe: task.recipe.clone(),
            admit_ms: task.admit_ms,
        })
        .collect();
    let posts = state
        .posts
        .iter()
        .map(|post| PostSnap {
            id: post.id.clone(),
            author: post.author.clone(),
            role: post.role.clone(),
            text: post.text.clone(),
            weight: post.weight,
            channel: post.channel.clone(),
            mentions: post.mentions.clone(),
            ts: post.ts,
        })
        .collect();
    let constraints = state
        .constraints
        .iter()
        .map(|c| ConstraintSnap {
            id: c.id.clone(),
            text: c.text.clone(),
            tags: c.tags.clone(),
        })
        .collect();
    let mut retried: Vec<String> = state.retried.iter().cloned().collect();
    retried.sort();
    let purse = &state.purse;
    let open = purse
        .open
        .iter()
        .map(|(id, slice)| SliceSnap {
            id: id.clone(),
            tokens: slice.tokens,
            used: slice.used,
            memory_mb: slice.memory_mb,
            memory_lent: slice.memory_lent,
            pids: slice.pids,
            pids_lent: slice.pids_lent,
            seconds: slice.seconds,
            root: slice.root,
        })
        .collect();
    Index {
        offset,
        fence: state.fence,
        headers,
        queue: state.queue.iter().cloned().collect(),
        posts,
        samples: state.samples.dump(),
        constraints,
        retried,
        decision_spent: state.decision_spent,
        votes: state
            .votes
            .values()
            .map(|vote| VoteSnap {
                id: vote.id.clone(),
                voter: vote.voter.clone(),
                role: vote.role.clone(),
                target: vote.target.clone(),
                channel: vote.channel.clone(),
                choice: vote.choice.clone(),
                weight: vote.weight,
                ts: vote.ts,
            })
            .collect(),
        posts_seen: state.posts_seen,
        channels: state
            .channels
            .iter()
            .map(|(channel, stat)| ChannelSnap {
                channel: channel.clone(),
                posts: stat.posts,
                mentions: stat.mentions,
                authors: stat.authors.iter().cloned().collect(),
                last_author: stat.last_author.clone(),
                last_text: stat.last_text.clone(),
            })
            .collect(),
        moderation: state
            .moderation
            .iter()
            .map(|item| ModerationSnap {
                id: item.id.clone(),
                target: item.target.clone(),
                action: item.action.clone(),
                channel: item.channel.clone(),
                weight: item.weight,
                ts: item.ts,
            })
            .collect(),
        purse: PurseSnap {
            cap: purse.cap,
            memory_cap: purse.memory_cap,
            pids_cap: purse.pids_cap,
            period_ms: purse.period_ms,
            period_start: purse.period_start,
            available: purse.available,
            memory_held: purse.memory_held,
            pids_held: purse.pids_held,
            resets: purse.resets,
            open,
        },
    }
}

pub(crate) fn restore(state: &mut State, index: &Index) {
    state.tasks = index
        .headers
        .iter()
        .map(|header| {
            (
                header.id.clone(),
                TaskView {
                    id: header.id.clone(),
                    parent: header.parent.clone(),
                    worker: header.worker.clone(),
                    tags: header.tags.clone(),
                    goal: header.goal.clone(),
                    verifier: header.verifier.clone(),
                    value: header.value,
                    budget: header.budget.clone(),
                    state: header.state,
                    reason: header.reason.clone(),
                    retry_of: header.retry_of.clone(),
                    recipe: header.recipe.clone(),
                    pid: None,
                    admit_ms: header.admit_ms,
                },
            )
        })
        .collect();
    state.queue = index.queue.iter().cloned().collect();
    state.posts = index
        .posts
        .iter()
        .map(|post| PostView {
            id: post.id.clone(),
            author: post.author.clone(),
            role: post.role.clone(),
            text: post.text.clone(),
            weight: post.weight,
            channel: post.channel.clone(),
            mentions: post.mentions.clone(),
            ts: post.ts,
        })
        .collect();
    state.samples.load(index.samples.clone());
    state.constraints = index
        .constraints
        .iter()
        .map(|c| ConstraintView {
            id: c.id.clone(),
            text: c.text.clone(),
            tags: c.tags.clone(),
        })
        .collect();
    state.retried = index.retried.iter().cloned().collect();
    state.decision_spent = index.decision_spent;
    state.posts_seen = index.posts_seen;
    state.channels = index
        .channels
        .iter()
        .map(|item| {
            (
                item.channel.clone(),
                ChannelStat {
                    posts: item.posts,
                    mentions: item.mentions,
                    authors: item.authors.iter().cloned().collect(),
                    last_author: item.last_author.clone(),
                    last_text: item.last_text.clone(),
                },
            )
        })
        .collect();
    state.votes = index
        .votes
        .iter()
        .map(|vote| {
            (
                (
                    vote.voter.clone(),
                    vote.target.clone(),
                    vote.channel.clone(),
                ),
                VoteView {
                    id: vote.id.clone(),
                    voter: vote.voter.clone(),
                    role: vote.role.clone(),
                    target: vote.target.clone(),
                    channel: vote.channel.clone(),
                    choice: vote.choice.clone(),
                    weight: vote.weight,
                    ts: vote.ts,
                },
            )
        })
        .collect();
    state.moderation = index
        .moderation
        .iter()
        .map(|item| ModerationView {
            id: item.id.clone(),
            target: item.target.clone(),
            action: item.action.clone(),
            channel: item.channel.clone(),
            weight: item.weight,
            ts: item.ts,
        })
        .collect();
    let mut open = BTreeMap::new();
    for slice in &index.purse.open {
        open.insert(
            slice.id.clone(),
            OpenSlice {
                tokens: slice.tokens,
                used: slice.used,
                memory_mb: slice.memory_mb,
                memory_lent: slice.memory_lent,
                pids: slice.pids,
                pids_lent: slice.pids_lent,
                seconds: slice.seconds,
                root: slice.root,
            },
        );
    }
    state.purse = Purse {
        cap: index.purse.cap,
        memory_cap: index.purse.memory_cap,
        pids_cap: index.purse.pids_cap,
        period_ms: index.purse.period_ms,
        period_start: index.purse.period_start,
        available: index.purse.available,
        memory_held: index.purse.memory_held,
        pids_held: index.purse.pids_held,
        resets: index.purse.resets,
        open,
    };
}

/// A signed cap increase adds the delta. A decrease does not mint a refund.
pub(crate) fn align_caps(purse: &mut Purse, cfg: &Config) {
    if cfg.caps.max_tokens > purse.cap {
        purse.available = purse
            .available
            .saturating_add(cfg.caps.max_tokens - purse.cap);
    }
    purse.cap = cfg.caps.max_tokens;
    purse.memory_cap = cfg.caps.max_memory_mb;
    purse.pids_cap = cfg.caps.max_pids;
    purse.period_ms = cfg.caps.token_period_ms;
}

pub(crate) fn commit(home: &Path, offset: u64) -> Result<String> {
    let git_dir = paths::snap_git(home);
    if !git_dir.join("HEAD").is_file() {
        git_ok(
            home,
            &[
                "init",
                "--bare",
                "-b",
                "main",
                git_dir.to_str().unwrap_or("snap.git"),
            ],
        )?;
    }
    let work = home.join("run/snap-work");
    let _ = fs::remove_dir_all(&work);
    fs::create_dir_all(&work)?;
    stage(home, &work)?;
    let git_s = git_dir.to_str().ok_or_else(|| err("snap path"))?;
    let work_s = work.to_str().ok_or_else(|| err("snap path"))?;
    git_ok(
        home,
        &["--git-dir", git_s, "--work-tree", work_s, "add", "-A"],
    )?;
    let msg = format!("snap {offset}");
    let committed = git(
        home,
        &[
            "-c",
            "commit.gpgsign=false",
            "--git-dir",
            git_s,
            "--work-tree",
            work_s,
            "commit",
            "-m",
            &msg,
        ],
    )?;
    if !committed.status.success() {
        let text = String::from_utf8_lossy(&committed.stderr);
        if !text.contains("nothing to commit") && !text.contains("working tree clean") {
            let _ = fs::remove_dir_all(&work);
            return Err(err(format!("git commit: {text}")));
        }
    }
    git_ok(
        home,
        &["--git-dir", git_s, "tag", "-f", &format!("offset-{offset}")],
    )?;
    let sha = git_ok(home, &["--git-dir", git_s, "rev-parse", "HEAD"])?;
    let _ = fs::remove_dir_all(&work);
    Ok(sha)
}

/// The daemon is down. Fold the log and commit that index.
pub(crate) fn offline(home: &Path) -> Result<(u64, String)> {
    let policy = load_policy(home)?;
    let node = node_id(home)?;
    let lease = ensure_lease(home, &node)?;
    let opened = crate::ledger::open(&paths::ledger(home))?;
    opened.ledger.lock()?;
    let mut state = State::new(&policy.cfg);
    state.fence = lease.gen;
    for decoded in &opened.records {
        if let Decoded::Rec(rec) = decoded {
            state.apply(rec);
        }
    }
    align_caps(&mut state.purse, &policy.cfg);
    let offset = opened.ledger.len();
    store(home, &checkpoint(&state, offset))?;
    let sha = commit(home, offset)?;
    Ok((offset, sha))
}

fn load_policy(home: &Path) -> Result<Policy> {
    let text = fs::read_to_string(paths::policy(home))
        .map_err(|_| err("no policy at policy.lua (inlet init)"))?;
    if paths::key_pub(home).is_file() {
        let public = fs::read(paths::key_pub(home))?;
        let sig = fs::read(paths::policy_sig(home)).map_err(|_| err("policy is not signed"))?;
        crate::sign::verify(&public, text.as_bytes(), &sig)?;
    }
    Policy::parse(&text)
}

fn stage(home: &Path, work: &Path) -> Result<()> {
    copy_file(&paths::policy(home), &work.join("policy.lua"))?;
    copy_file(&paths::policy_sig(home), &work.join("policy.sig"))?;
    copy_file(&paths::key_pub(home), &work.join("keys/policy.pub"))?;
    copy_file(&paths::snap_index(home), &work.join("snap/index.json"))?;
    copy_file(&paths::lease(home), &work.join("lease.json"))?;
    if paths::registry(home).is_dir() {
        copy_tree(&paths::registry(home), &work.join("registry"))?;
    }
    Ok(())
}

fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    if !src.is_file() {
        return Ok(());
    }
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src, dst)?;
    Ok(())
}

fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_tree(&entry.path(), &to)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

fn durable(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn git(home: &Path, args: &[&str]) -> Result<std::process::Output> {
    Command::new("git")
        .args(args)
        .current_dir(home)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_AUTHOR_NAME", "inlet")
        .env("GIT_AUTHOR_EMAIL", "inlet@local")
        .env("GIT_COMMITTER_NAME", "inlet")
        .env("GIT_COMMITTER_EMAIL", "inlet@local")
        .output()
        .map_err(|_| err("git is required to snap"))
}

fn git_ok(home: &Path, args: &[&str]) -> Result<String> {
    let out = git(home, args)?;
    if !out.status.success() {
        let text = String::from_utf8_lossy(&out.stderr);
        return Err(err(format!("git {}: {text}", args.join(" "))));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
