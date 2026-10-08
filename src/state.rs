use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::config::{Config, OnCrash};
use crate::gate::samples_key;
use crate::model::{Budget, Record, TaskState};
use crate::purse::Purse;

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
                ts: _,
            } => {
                if let Some(prev) = retry_of {
                    self.retried.insert(prev.clone());
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
                    if let Some(task) = self.tasks.get_mut(id) {
                        if task.state == TaskState::Queued {
                            task.state = TaskState::Failed;
                            task.reason = "fence".into();
                        }
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
            Record::Deny { id, reason, .. } => {
                if let Some(task) = self.tasks.get_mut(id) {
                    task.state = TaskState::Failed;
                    task.reason = reason.clone();
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
                ..
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
                if let Some(task) = self.tasks.get_mut(id) {
                    task.pid = None;
                    task.reason = reason.clone();
                    task.state = if reason == "killed" {
                        TaskState::Killed
                    } else if *code == 0 && reason == "ok" {
                        TaskState::Done
                    } else {
                        TaskState::Failed
                    };
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
            }
            Record::Cost { id, tokens, .. } => {
                self.purse.note_used(id, *tokens);
            }
            Record::Gate { tokens, .. } => {
                self.decision_spent = self.decision_spent.saturating_add(*tokens);
            }
            Record::Bind { id, text, tags, .. } => {
                if !self.constraints.iter().any(|c| c.id == *id) {
                    self.constraints.push(ConstraintView {
                        id: id.clone(),
                        text: text.clone(),
                        tags: tags.clone(),
                    });
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
    /// Math workers (or `on_crash = requeue`) get a fresh task, not a refund.
    pub fn recover(&self, cfg: &Config, ts: u64) -> Vec<Record> {
        let mut out = Vec::new();
        for task in self.tasks.values() {
            if !task.state.is_live() {
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
            let requeue = cfg
                .workers
                .get(&task.worker)
                .map(|w| w.on_crash == OnCrash::Requeue)
                .unwrap_or(false);
            if requeue && !self.retried.contains(&task.id) {
                out.push(Record::Task {
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
                    ts,
                });
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
