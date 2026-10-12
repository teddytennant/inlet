use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::config::{Config, OnCrash, RetryCfg};
use crate::gate::samples_key;
use crate::model::{Budget, Plan, Record, TaskState, Template};
use crate::purse::Purse;

/// Failures a new attempt can fix. `killed` was asked for, `purse` would
/// run dry again on the same budget, and a deny by a cap is not transient.
pub const RETRYABLE: [&str; 7] = [
    "crash", "verifier", "timeout", "exit", "stall", "gate", "stopped",
];

pub const POST_RING: usize = 400;

#[derive(Debug, Clone)]
pub struct TaskView {
    pub id: String,
    pub parent: Option<String>,
    pub worker: String,
    pub tags: Vec<String>,
    pub goal: String,
    pub verifier: Option<String>,
    pub value: u64,
    pub budget: Budget,
    pub state: TaskState,
    pub reason: String,
    pub retry_of: Option<String>,
    pub recipe: Option<String>,
    pub pid: Option<i32>,
    pub admit_ms: Option<u64>,
    pub plan: Plan,
}

const SWARM_PACE_MS: u64 = 1_000;

/// A swarm's spec and what the ledger says has come of it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmView {
    pub name: String,
    pub replicas: u64,
    pub completions: u64,
    pub backoff_limit: u32,
    pub template: Template,
    /// Tasks stamped so far. The next one is `created + 1`.
    pub created: u64,
    /// Owned tasks that have not settled.
    pub active: u64,
    pub done: u64,
    pub failed: u64,
    /// Failures in a row, counted after a task's own retries ran out.
    pub streak: u32,
    /// When the last owned task settled, if that settle calls for a pause:
    /// a failure, or a success in a swarm with no end. 0 is no pause.
    #[serde(default)]
    pub paced_ms: u64,
    pub generation: u64,
}

impl SwarmView {
    pub fn finished(&self) -> bool {
        self.completions > 0 && self.done >= self.completions
    }

    pub fn stalled(&self) -> bool {
        self.backoff_limit > 0 && self.streak >= self.backoff_limit
    }

    /// No new task before this time. The pause is worked out from the policy
    /// in force now, so the ledger alone decides the state.
    pub fn next_ms(&self, retry: &RetryCfg) -> u64 {
        if self.paced_ms == 0 {
            return 0;
        }
        // Never less than a second, whatever `retry.backoff` says: an instant
        // task in a swarm with no end, or one that always fails, would spin the ledger.
        self.paced_ms
            .saturating_add(retry.delay_ms(self.streak.max(1)).max(SWARM_PACE_MS))
    }

    /// How many tasks to stamp now.
    pub fn wants(&self, now: u64, retry: &RetryCfg) -> u64 {
        if self.finished() || self.stalled() || now < self.next_ms(retry) {
            return 0;
        }
        let room = self.replicas.saturating_sub(self.active);
        if self.completions == 0 {
            return room;
        }
        room.min(
            self.completions
                .saturating_sub(self.done)
                .saturating_sub(self.active),
        )
    }

    pub fn phase(&self) -> &'static str {
        if self.finished() {
            "done"
        } else if self.stalled() {
            "stalled"
        } else {
            "active"
        }
    }
}

#[derive(Debug, Clone)]
pub struct PostView {
    pub id: String,
    pub author: String,
    pub role: String,
    pub text: String,
    pub weight: u64,
    pub channel: String,
    pub mentions: Vec<String>,
    pub ts: u64,
}

#[derive(Debug, Default)]
pub struct Samples {
    inner: HashMap<String, Vec<u64>>,
}

impl Samples {
    pub fn median(&self, key: &str) -> Option<u64> {
        let values = self.inner.get(key)?;
        if values.len() < 5 {
            return None;
        }
        let mut sorted = values.clone();
        sorted.sort_unstable();
        Some(sorted[sorted.len() / 2])
    }

    pub fn push(&mut self, key: String, tokens: u64) {
        let slot = self.inner.entry(key).or_default();
        slot.push(tokens);
        if slot.len() > 64 {
            slot.remove(0);
        }
    }

    pub fn series(&self, key: &str) -> Vec<u64> {
        self.inner.get(key).cloned().unwrap_or_default()
    }

    pub fn dump(&self) -> BTreeMap<String, Vec<u64>> {
        self.inner
            .iter()
            .map(|(key, values)| (key.clone(), values.clone()))
            .collect()
    }

    pub fn load(&mut self, dumped: BTreeMap<String, Vec<u64>>) {
        self.inner = dumped.into_iter().collect();
    }
}

#[derive(Debug, Clone)]
pub struct ConstraintView {
    pub id: String,
    pub text: String,
    pub tags: Vec<String>,
    /// 0 holds until cleared.
    pub expires: u64,
}

impl ConstraintView {
    pub fn holds(&self, now: u64) -> bool {
        self.expires == 0 || now < self.expires
    }
}

#[derive(Debug)]
pub struct State {
    pub tasks: BTreeMap<String, TaskView>,
    pub queue: VecDeque<String>,
    pub posts: VecDeque<PostView>,
    pub samples: Samples,
    pub purse: Purse,
    pub retried: HashSet<String>,
    pub constraints: Vec<ConstraintView>,
    pub decision_spent: u64,
    /// Lease generation. An admit with any other fence does not debit.
    pub fence: u64,
    /// Last vote per (voter, target, channel). Earlier votes stay in the log.
    pub votes: BTreeMap<(String, String, String), VoteView>,
    pub moderation: Vec<ModerationView>,
    pub posts_seen: u64,
    pub channels: BTreeMap<String, ChannelStat>,
    pub swarms: BTreeMap<String, SwarmView>,
}

#[derive(Debug, Clone, Default)]
pub struct ChannelStat {
    pub posts: u64,
    pub mentions: u64,
    pub authors: BTreeSet<String>,
    pub last_author: String,
    pub last_text: String,
}

#[derive(Debug, Clone)]
pub struct VoteView {
    pub id: String,
    pub voter: String,
    pub role: String,
    pub target: String,
    pub channel: String,
    pub choice: String,
    pub weight: u64,
    pub ts: u64,
}

#[derive(Debug, Clone)]
pub struct ModerationView {
    pub id: String,
    pub target: String,
    pub action: String,
    pub channel: String,
    pub weight: u64,
    pub ts: u64,
}

impl State {
    pub fn new(cfg: &Config) -> Self {
        Self {
            tasks: BTreeMap::new(),
            queue: VecDeque::new(),
            posts: VecDeque::new(),
            samples: Samples::default(),
            purse: Purse::new(
                cfg.caps.max_tokens,
                cfg.caps.max_memory_mb,
                cfg.caps.max_pids,
                cfg.caps.token_period_ms,
            ),
            retried: HashSet::new(),
            constraints: Vec::new(),
            decision_spent: 0,
            fence: 0,
            votes: BTreeMap::new(),
            moderation: Vec::new(),
            posts_seen: 0,
            channels: BTreeMap::new(),
            swarms: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, rec: &Record) {
        match rec {
            Record::Task {
                id,
                parent,
                worker,
                tags,
                goal,
                verifier,
                value,
                budget,
                retry_of,
                recipe,
                plan,
                ts: _,
            } => {
                if let Some(prev) = retry_of {
                    self.retried.insert(prev.clone());
                }
                if let Some(swarm) = plan.owner.as_ref().and_then(|o| self.swarms.get_mut(o)) {
                    swarm.active += 1;
                    if retry_of.is_some() {
                        // The failure this retries was not the chain's last word.
                        swarm.failed = swarm.failed.saturating_sub(1);
                        swarm.streak = swarm.streak.saturating_sub(1);
                    } else {
                        swarm.created += 1;
                    }
                }
                self.tasks.insert(
                    id.clone(),
                    TaskView {
                        id: id.clone(),
                        parent: parent.clone(),
                        worker: worker.clone(),
                        tags: tags.clone(),
                        goal: goal.clone(),
                        verifier: verifier.clone(),
                        value: *value,
                        budget: budget.clone(),
                        state: TaskState::Queued,
                        reason: String::new(),
                        retry_of: retry_of.clone(),
                        recipe: recipe.clone(),
                        pid: None,
                        admit_ms: None,
                        plan: plan.clone(),
                    },
                );
                self.queue.push_back(id.clone());
                self.note_parent(id);
            }
            Record::Admit {
                id,
                fence,
                tokens,
                seconds,
                memory_mb,
                pids,
                ts,
            } => {
                if *fence != self.fence {
                    let mut lost = false;
                    if let Some(task) = self.tasks.get_mut(id) {
                        if task.state == TaskState::Queued {
                            task.state = TaskState::Failed;
                            task.reason = "fence".into();
                            lost = true;
                        }
                    }
                    if lost {
                        self.settle_owner(id, false, *ts);
                    }
                    self.queue.retain(|q| q != id);
                    self.note_parent(id);
                } else {
                    let parent = self.tasks.get(id).and_then(|t| t.parent.clone());
                    let debited = if let Some(parent) = parent.as_deref() {
                        self.purse
                            .debit_parent(parent, id, *tokens, *seconds, *memory_mb, *pids)
                            .is_ok()
                    } else {
                        self.purse
                            .debit_root(id, *tokens, *seconds, *memory_mb, *pids)
                            .is_ok()
                    };
                    if debited {
                        if let Some(task) = self.tasks.get_mut(id) {
                            task.state = TaskState::Running;
                            task.admit_ms = Some(*ts);
                            task.reason.clear();
                        }
                        self.queue.retain(|q| q != id);
                    }
                    self.note_parent(id);
                }
            }
            Record::Deny { id, reason, ts } => {
                let mut open = false;
                if let Some(task) = self.tasks.get_mut(id) {
                    open = !task.state.is_settled();
                    task.state = TaskState::Failed;
                    task.reason = reason.clone();
                }
                if open {
                    self.settle_owner(id, false, *ts);
                }
                self.queue.retain(|q| q != id);
                self.note_parent(id);
            }
            Record::Spawn { id, pid, .. } => {
                if let Some(task) = self.tasks.get_mut(id) {
                    task.pid = Some(*pid as i32);
                }
            }
            Record::Exit {
                id,
                code,
                reason,
                tokens_used,
                refund_tokens,
                ts,
            } => {
                let parent = self.tasks.get(id).and_then(|t| t.parent.clone());
                let worker_tags = self
                    .tasks
                    .get(id)
                    .map(|t| (t.worker.clone(), t.tags.clone()));
                self.purse
                    .credit_exit(id, *refund_tokens, parent.as_deref());
                if let Some((worker, tags)) = worker_tags {
                    self.samples.push(samples_key(&worker, &tags), *tokens_used);
                }
                let mut settled = None;
                if let Some(task) = self.tasks.get_mut(id) {
                    let open = !task.state.is_settled();
                    task.pid = None;
                    task.reason = reason.clone();
                    task.state = if reason == "killed" {
                        TaskState::Killed
                    } else if *code == 0 && reason == "ok" {
                        TaskState::Done
                    } else {
                        TaskState::Failed
                    };
                    if open {
                        settled = Some(task.state == TaskState::Done);
                    }
                }
                if let Some(done) = settled {
                    self.settle_owner(id, done, *ts);
                }
                self.note_parent(id);
            }
            Record::Post {
                id,
                author,
                role,
                text,
                weight,
                channel,
                mentions,
                ts,
            } => {
                if self.posts.len() >= POST_RING {
                    self.posts.pop_front();
                }
                self.posts.push_back(PostView {
                    id: id.clone(),
                    author: author.clone(),
                    role: role.clone(),
                    text: text.clone(),
                    weight: *weight,
                    channel: channel.clone(),
                    mentions: mentions.clone(),
                    ts: *ts,
                });
                self.posts_seen = self.posts_seen.saturating_add(1);
                let stat = self.channels.entry(channel.clone()).or_default();
                stat.posts = stat.posts.saturating_add(1);
                if !mentions.is_empty() {
                    stat.mentions = stat.mentions.saturating_add(1);
                }
                stat.authors.insert(author.clone());
                stat.last_author = author.clone();
                stat.last_text = text.clone();
            }
            Record::Cost { id, tokens, .. } => {
                self.purse.note_used(id, *tokens);
            }
            Record::Gate { tokens, .. } => {
                self.decision_spent = self.decision_spent.saturating_add(*tokens);
            }
            Record::Bind {
                id,
                text,
                tags,
                expires,
                ..
            } => {
                if !self.constraints.iter().any(|c| c.id == *id) {
                    self.constraints.push(ConstraintView {
                        id: id.clone(),
                        text: text.clone(),
                        tags: tags.clone(),
                        expires: *expires,
                    });
                }
            }
            Record::Swarm {
                name,
                replicas,
                completions,
                backoff_limit,
                template,
                ..
            } => {
                let swarm = self
                    .swarms
                    .entry(name.clone())
                    .or_insert_with(|| SwarmView {
                        name: name.clone(),
                        replicas: 0,
                        completions: 0,
                        backoff_limit: 0,
                        template: template.clone(),
                        created: 0,
                        active: 0,
                        done: 0,
                        failed: 0,
                        streak: 0,
                        paced_ms: 0,
                        generation: 0,
                    });
                swarm.replicas = *replicas;
                swarm.completions = *completions;
                swarm.backoff_limit = *backoff_limit;
                swarm.template = template.clone();
                // A new spec is a new chance.
                swarm.streak = 0;
                swarm.paced_ms = 0;
                swarm.generation += 1;
            }
            Record::SwarmDelete { name, .. } => {
                self.swarms.remove(name);
                // Tasks still in flight outlive it. They must not settle against
                // a later swarm that takes the same name.
                for task in self.tasks.values_mut() {
                    if task.plan.owner.as_deref() == Some(name.as_str()) {
                        task.plan.owner = None;
                    }
                }
            }
            Record::Clear { id, .. } => {
                self.constraints.retain(|c| &c.id != id);
            }
            Record::Vote {
                id,
                voter,
                role,
                target,
                channel,
                choice,
                weight,
                ts,
            } => {
                self.votes.insert(
                    (voter.clone(), target.clone(), channel.clone()),
                    VoteView {
                        id: id.clone(),
                        voter: voter.clone(),
                        role: role.clone(),
                        target: target.clone(),
                        channel: channel.clone(),
                        choice: choice.clone(),
                        weight: *weight,
                        ts: *ts,
                    },
                );
            }
            Record::Moderation {
                id,
                target,
                action,
                channel,
                weight,
                ts,
            } => {
                if !self
                    .moderation
                    .iter()
                    .any(|m| m.target == *target && m.action == *action && m.channel == *channel)
                {
                    self.moderation.push(ModerationView {
                        id: id.clone(),
                        target: target.clone(),
                        action: action.clone(),
                        channel: channel.clone(),
                        weight: *weight,
                        ts: *ts,
                    });
                }
            }
            Record::Kill { .. }
            | Record::Reset { .. }
            | Record::Result { .. }
            | Record::Promote { .. }
            | Record::Sign { .. } => {}
        }
        if let Record::Reset { ts, .. } = rec {
            self.purse.apply_reset(*ts);
        }
    }

    /// An owned task settled. The swarm's counts follow the ledger, not the config,
    /// so a replay under a different policy lands on the same numbers.
    fn settle_owner(&mut self, id: &str, done: bool, ts: u64) {
        let Some(owner) = self.tasks.get(id).and_then(|t| t.plan.owner.clone()) else {
            return;
        };
        let Some(swarm) = self.swarms.get_mut(&owner) else {
            return;
        };
        swarm.active = swarm.active.saturating_sub(1);
        if done {
            swarm.done += 1;
            swarm.streak = 0;
            swarm.paced_ms = if swarm.completions == 0 { ts } else { 0 };
        } else {
            swarm.failed += 1;
            swarm.streak = swarm.streak.saturating_add(1);
            swarm.paced_ms = ts;
        }
    }

    /// The next attempt at a failed task, or `None` when the chain is spent.
    /// A retry is a new task and a new admission. The old slice stays as charged.
    pub fn retry_for(&self, id: &str, reason: &str, cfg: &Config, ts: u64) -> Option<Record> {
        let task = self.tasks.get(id)?;
        if self.retried.contains(id) || !RETRYABLE.contains(&reason) {
            return None;
        }
        let attempt = task.plan.attempt();
        let mut max = task.plan.max_attempts();
        if reason == "stopped" {
            // The daemon was shut down under it. That is not the task's failure,
            // so the rerun is the same attempt. A child goes down with its parent,
            // which will hand the part off again.
            if task.parent.is_some() {
                return None;
            }
            return Some(Record::Task {
                id: crate::id::ulid(),
                parent: None,
                worker: task.worker.clone(),
                tags: task.tags.clone(),
                goal: task.goal.clone(),
                verifier: task.verifier.clone(),
                value: task.value,
                budget: task.budget.clone(),
                retry_of: Some(task.id.clone()),
                recipe: task.recipe.clone(),
                plan: Plan {
                    not_before: 0,
                    ..task.plan.clone()
                },
                ts,
            });
        }
        if reason == "crash" {
            match cfg.workers.get(&task.worker).and_then(|w| w.on_crash) {
                Some(OnCrash::Fail) => return None,
                Some(OnCrash::Requeue) => max = max.max(2),
                None => {}
            }
        }
        if attempt >= max {
            return None;
        }
        if let Some(parent) = &task.parent {
            // Nobody is left to read the answer.
            if !self.tasks.get(parent).is_some_and(|p| p.state.is_live()) {
                return None;
            }
        }
        Some(Record::Task {
            id: crate::id::ulid(),
            parent: task.parent.clone(),
            worker: task.worker.clone(),
            tags: task.tags.clone(),
            goal: task.goal.clone(),
            verifier: task.verifier.clone(),
            value: task.value,
            budget: task.budget.clone(),
            retry_of: Some(task.id.clone()),
            recipe: task.recipe.clone(),
            plan: Plan {
                attempt: attempt + 1,
                max_attempts: max,
                not_before: ts.saturating_add(cfg.retry.delay_ms(attempt)),
                owner: task.plan.owner.clone(),
            },
            ts,
        })
    }

    /// The chain a task belongs to, oldest first.
    pub fn chain(&self, id: &str) -> Vec<String> {
        let mut out = vec![id.to_string()];
        let mut cursor = self.tasks.get(id).and_then(|t| t.retry_of.clone());
        while let Some(prev) = cursor {
            if out.len() > 64 || out.contains(&prev) {
                break;
            }
            cursor = self.tasks.get(&prev).and_then(|t| t.retry_of.clone());
            out.push(prev);
        }
        out.reverse();
        out
    }

    /// A live parent with a queued or live child is blocked. A settled child releases it.
    fn note_parent(&mut self, id: &str) {
        let Some(parent) = self.tasks.get(id).and_then(|t| t.parent.clone()) else {
            return;
        };
        let waiting = self.tasks.values().any(|t| {
            t.parent.as_deref() == Some(parent.as_str())
                && matches!(
                    t.state,
                    TaskState::Queued | TaskState::Running | TaskState::Blocked
                )
        });
        if let Some(task) = self.tasks.get_mut(&parent) {
            if task.state.is_live() {
                task.state = if waiting {
                    TaskState::Blocked
                } else {
                    TaskState::Running
                };
            }
        }
    }

    pub fn live(&self) -> usize {
        self.tasks.values().filter(|t| t.state.is_live()).count()
    }

    /// Admit with no exit: the process is gone. Charge the token slice.
    /// A retry, when the chain allows one, is a fresh task, not a refund.
    /// `kept` are the tasks whose worker outlived the daemon.
    pub fn recover(&self, cfg: &Config, ts: u64, kept: &HashSet<String>) -> Vec<Record> {
        let mut out = Vec::new();
        for task in self.tasks.values() {
            if !task.state.is_live() || kept.contains(&task.id) {
                continue;
            }
            let used = task.budget.tokens;
            out.push(Record::Exit {
                id: task.id.clone(),
                code: -1,
                reason: "crash".into(),
                tokens_used: used,
                refund_tokens: 0,
                ts,
            });
        }
        // Children first lose their parent, so only roots and orphan-free chains retry.
        let dead: std::collections::HashSet<&str> = self
            .tasks
            .values()
            .filter(|t| t.state.is_live() && !kept.contains(&t.id))
            .map(|t| t.id.as_str())
            .collect();
        for task in self.tasks.values() {
            if !dead.contains(task.id.as_str()) {
                continue;
            }
            if task.parent.as_deref().is_some_and(|p| dead.contains(p)) {
                continue;
            }
            if let Some(rec) = self.retry_for(&task.id, "crash", cfg, ts) {
                out.push(rec);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Budget, Record};

    fn task(id: &str) -> Record {
        Record::Task {
            id: id.into(),
            parent: None,
            worker: "pi".into(),
            tags: vec!["code".into()],
            goal: "g".into(),
            verifier: None,
            value: 1,
            budget: Budget {
                tokens: 50,
                seconds: 1,
                memory_mb: 1,
                pids: 1,
            },
            retry_of: None,
            recipe: None,
            plan: Plan::default(),
            ts: 1,
        }
    }

    fn admit(id: &str, fence: u64) -> Record {
        Record::Admit {
            id: id.into(),
            fence,
            tokens: 50,
            seconds: 1,
            memory_mb: 1,
            pids: 1,
            ts: 2,
        }
    }

    fn owned(id: &str, retry_of: Option<&str>) -> Record {
        let Record::Task {
            worker,
            tags,
            goal,
            value,
            budget,
            ..
        } = task(id)
        else {
            unreachable!()
        };
        Record::Task {
            id: id.into(),
            parent: None,
            worker,
            tags,
            goal,
            verifier: None,
            value,
            budget,
            retry_of: retry_of.map(str::to_string),
            recipe: None,
            plan: Plan {
                owner: Some("s".into()),
                ..Plan::default()
            },
            ts: 1,
        }
    }

    fn exit(id: &str, ok: bool) -> Record {
        Record::Exit {
            id: id.into(),
            code: if ok { 0 } else { 1 },
            reason: if ok { "ok" } else { "verifier" }.into(),
            tokens_used: 0,
            refund_tokens: 50,
            ts: 10,
        }
    }

    #[test]
    fn swarm_counts_follow_the_ledger() {
        let mut state = State::new(&crate::config::preset("box"));
        state.apply(&Record::Swarm {
            name: "s".into(),
            replicas: 2,
            completions: 3,
            backoff_limit: 2,
            template: Template::default(),
            ts: 1,
        });
        let retry = crate::config::preset("box").retry;
        assert_eq!(state.swarms["s"].wants(5, &retry), 2);
        state.apply(&owned("a", None));
        state.apply(&owned("b", None));
        assert_eq!(state.swarms["s"].wants(5, &retry), 0);
        for id in ["a", "b"] {
            state.apply(&admit(id, 0));
        }
        state.apply(&exit("a", true));
        // One done, one in flight, three wanted: room for one more.
        assert_eq!(state.swarms["s"].wants(20, &retry), 1);

        state.apply(&exit("b", false));
        let swarm = &state.swarms["s"];
        assert_eq!((swarm.failed, swarm.streak, swarm.active), (1, 1, 0));
        assert_eq!(swarm.next_ms(&retry), 10 + 10_000, "a failure backs off");
        assert_eq!(swarm.wants(11, &retry), 0);
        // The pause is the policy's, read when asked. The ledger holds no delay.
        let quick = RetryCfg {
            backoff_ms: 0,
            ..retry
        };
        assert_eq!(
            swarm.next_ms(&quick),
            10 + 1_000,
            "never faster than a second"
        );
        // The retry takes the failure back and holds the slot.
        state.apply(&owned("b2", Some("b")));
        let swarm = &state.swarms["s"];
        assert_eq!((swarm.failed, swarm.streak, swarm.active), (0, 0, 1));
        assert_eq!(swarm.created, 2, "a retry is not a new index");

        state.apply(&admit("b2", 0));
        state.apply(&exit("b2", false));
        state.apply(&owned("c", None));
        state.apply(&admit("c", 0));
        state.apply(&exit("c", false));
        assert!(state.swarms["s"].stalled());
        assert_eq!(state.swarms["s"].wants(u64::MAX, &retry), 0);
        // A new spec clears the streak and keeps the counts.
        state.apply(&Record::Swarm {
            name: "s".into(),
            replicas: 5,
            completions: 3,
            backoff_limit: 2,
            template: Template::default(),
            ts: 30,
        });
        let swarm = &state.swarms["s"];
        assert_eq!((swarm.generation, swarm.done, swarm.failed), (2, 1, 2));
        assert_eq!(
            swarm.wants(30, &retry),
            2,
            "completions bound the stamp, not replicas"
        );

        state.apply(&Record::SwarmDelete {
            name: "s".into(),
            ts: 40,
        });
        state.apply(&owned("late", None));
        assert!(state.swarms.is_empty());

        // The name comes back while an old task is still running. Its exit
        // belongs to the swarm that is gone.
        let mut state = State::new(&crate::config::preset("box"));
        let spec = Record::Swarm {
            name: "s".into(),
            replicas: 1,
            completions: 1,
            backoff_limit: 2,
            template: Template::default(),
            ts: 1,
        };
        state.apply(&spec);
        state.apply(&owned("old", None));
        state.apply(&admit("old", 0));
        state.apply(&Record::SwarmDelete {
            name: "s".into(),
            ts: 2,
        });
        state.apply(&spec);
        state.apply(&owned("new", None));
        state.apply(&exit("old", true));
        let swarm = &state.swarms["s"];
        assert_eq!((swarm.done, swarm.active, swarm.created), (0, 1, 1));
        assert!(!swarm.finished());
    }

    #[test]
    fn retries_follow_the_plan_and_the_worker() {
        let mut cfg = crate::config::preset("box");
        cfg.retry.backoff_ms = 1_000;
        let mut state = State::new(&cfg);
        let mut first = task("a");
        if let Record::Task { plan, .. } = &mut first {
            plan.max_attempts = 2;
        }
        state.apply(&first);
        state.apply(&admit("a", 0));
        assert!(state.retry_for("a", "killed", &cfg, 5).is_none());
        assert!(state.retry_for("a", "purse", &cfg, 5).is_none());
        let Some(Record::Task {
            id, plan, retry_of, ..
        }) = state.retry_for("a", "verifier", &cfg, 5)
        else {
            panic!("no retry");
        };
        assert_eq!(retry_of.as_deref(), Some("a"));
        assert_eq!(
            (plan.attempt, plan.max_attempts, plan.not_before),
            (2, 2, 1_005)
        );

        let mut second = task(&id);
        if let Record::Task {
            plan: p, retry_of, ..
        } = &mut second
        {
            *p = plan;
            *retry_of = Some("a".into());
        }
        state.apply(&second);
        assert!(
            state.retry_for("a", "verifier", &cfg, 5).is_none(),
            "one retry per task"
        );
        assert!(
            state.retry_for(&id, "verifier", &cfg, 5).is_none(),
            "the chain is spent"
        );
        assert_eq!(state.chain(&id), ["a".to_string(), id.clone()]);
        // A daemon stop is nobody's failure. It reruns without using an attempt.
        let Some(Record::Task { plan, .. }) = state.retry_for(&id, "stopped", &cfg, 5) else {
            panic!("no rerun");
        };
        assert_eq!((plan.attempt, plan.not_before), (2, 0));
    }

    #[test]
    fn losing_fence_does_not_debit() {
        let mut state = State::new(&crate::config::preset("box"));
        state.apply(&task("a"));
        state.apply(&admit("a", 99));
        assert_eq!(state.purse.spent(), 0);
        assert_eq!(state.tasks["a"].reason, "fence");
        assert_eq!(state.tasks["a"].state, TaskState::Failed);

        let mut held = State::new(&crate::config::preset("box"));
        held.apply(&task("a"));
        held.apply(&admit("a", 0));
        assert_eq!(held.purse.spent(), 50);
        held.apply(&admit("a", 99));
        assert_eq!(held.purse.spent(), 50);
        assert_eq!(held.tasks["a"].state, TaskState::Running);
    }
}
