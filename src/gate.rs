use crate::config::Config;
use crate::model::TaskState;
use crate::purse::Purse;
use crate::state::{Samples, TaskView};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// `live` is full. Stay queued. Do not deny.
    Queue,
    Deny(&'static str),
}

#[derive(Clone, Copy)]
pub struct Ctx<'a> {
    pub cfg: &'a Config,
    pub purse: &'a Purse,
    pub samples: &'a Samples,
    pub live: usize,
    pub depth: u32,
    pub cell_ok: bool,
    pub lua_says: Option<bool>,
}

pub fn decide(task: &TaskView, ctx: &Ctx<'_>) -> Verdict {
    if task.state != TaskState::Queued {
        return Verdict::Deny("state");
    }
    if !ctx.cell_ok {
        return Verdict::Deny("cell");
    }
    if matches!(ctx.cfg.isolator, crate::config::Isolator::Slurm) {
        return Verdict::Deny("isolator");
    }
    if ctx.live >= ctx.cfg.caps.max_live {
        return Verdict::Queue;
    }
    if ctx.depth >= ctx.cfg.caps.max_depth {
        return Verdict::Deny("depth");
    }
    if !fits(task, ctx.purse) {
        return Verdict::Deny("purse");
    }
    let expected = ctx
        .samples
        .median(&samples_key(&task.worker, &task.tags))
        .unwrap_or(task.budget.tokens);
    let ev = expected_value(task.verifier.is_some(), task.value, expected);
    if ev < i128::from(ctx.cfg.min_ev) {
        return Verdict::Deny("ev");
    }
    // Lua may only tighten. `None` means no function, or it returned allow.
    if ctx.lua_says == Some(false) {
        return Verdict::Deny("lua");
    }
    Verdict::Allow
}

pub fn expected_value(verified: bool, value: u64, cost: u64) -> i128 {
    // p = 1 with a verifier, 0.5 without. Integer half, so cold unverified
    // passes only when budget <= value/2.
    let weighted = if verified {
        i128::from(value)
    } else {
        i128::from(value) / 2
    };
    weighted - i128::from(cost)
}

pub fn samples_key(worker: &str, tags: &[String]) -> String {
    let mut tags = tags.to_vec();
    tags.sort();
    tags.dedup();
    format!("{worker}|{}", tags.join(","))
}

fn fits(task: &TaskView, purse: &Purse) -> bool {
    let tokens = task.budget.tokens;
    let memory = task.budget.memory_mb;
    let pids = task.budget.pids;
    if let Some(parent) = &task.parent {
        let Some(slice) = purse.open.get(parent) else {
            return false;
        };
        slice.tokens.saturating_sub(slice.used) >= tokens
            && slice.memory_mb.saturating_sub(slice.memory_lent) >= memory
            && slice.pids.saturating_sub(slice.pids_lent) >= pids
            && slice.seconds >= task.budget.seconds
    } else {
        purse.available >= tokens
            && purse.memory_held.saturating_add(memory) <= purse.memory_cap
            && purse.pids_held.saturating_add(pids) <= purse.pids_cap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::model::{Budget, TaskState};
    use crate::purse::Purse;
    use crate::state::TaskView;
    use std::time::Instant;

    fn task(tokens: u64, verifier: bool) -> TaskView {
        TaskView {
            id: "t".into(),
            parent: None,
            worker: "pi".into(),
            tags: vec!["code".into()],
            goal: "g".into(),
            verifier: verifier.then(|| "true".into()),
            value: 400_000,
            budget: Budget {
                tokens,
                seconds: 10,
                memory_mb: 32,
                pids: 4,
            },
            recipe: None,
            state: TaskState::Queued,
            reason: String::new(),
            retry_of: None,
            pid: None,
            admit_ms: None,
        }
    }

    #[test]
    fn cold_off_formula() {
        assert!(expected_value(true, 400_000, 200_000) >= 0);
        assert_eq!(expected_value(false, 400_000, 200_000), 0);
        assert!(expected_value(false, 400_000, 200_001) < 0);
    }

    #[test]
    fn verified_clears_and_fat_unverified_does_not() {
        let cfg = Config::builtin_box();
        let mut purse = Purse::new(
            cfg.caps.max_tokens,
            cfg.caps.max_memory_mb,
            cfg.caps.max_pids,
            86_400_000,
        );
        purse.apply_reset(0);
        let samples = Samples::default();
        let allow = Ctx {
            cfg: &cfg,
            purse: &purse,
            samples: &samples,
            live: 0,
            depth: 0,
            cell_ok: true,
            lua_says: None,
        };
        assert_eq!(decide(&task(200_000, true), &allow), Verdict::Allow);
        assert_eq!(decide(&task(200_001, false), &allow), Verdict::Deny("ev"));
        assert_eq!(
            decide(
                &task(200_000, true),
                &Ctx {
                    live: cfg.caps.max_live,
                    ..allow
                }
            ),
            Verdict::Queue
        );
        assert_eq!(
            decide(
                &task(1, true),
                &Ctx {
                    cell_ok: false,
                    ..allow
                }
            ),
            Verdict::Deny("cell")
        );
    }

    #[test]
    fn admission_decision_is_under_5ms() {
        let cfg = Config::builtin_box();
        let mut purse = Purse::new(2_000_000, 8192, 64, 86_400_000);
        purse.apply_reset(0);
        let samples = Samples::default();
        let task = task(200_000, true);
        let mut times = Vec::with_capacity(200);
        for _ in 0..200 {
            let t = Instant::now();
            let v = decide(
                &task,
                &Ctx {
                    cfg: &cfg,
                    purse: &purse,
                    samples: &samples,
                    live: 0,
                    depth: 0,
                    cell_ok: true,
                    lua_says: None,
                },
            );
            times.push(t.elapsed());
            assert_eq!(v, Verdict::Allow);
        }
        times.sort();
        let median = times[times.len() / 2];
        assert!(
            median.as_millis() < 5,
            "median admission {median:?} (fsync excluded)"
        );
    }
}
