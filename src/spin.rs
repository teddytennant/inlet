//! Spinner verbs. A pool is one real state, and plain mode names that state.

use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::board;
use crate::error::{err, Result};

pub struct State {
    pub name: &'static str,
    pub pool: &'static [&'static str],
}

pub const CELL: State = State {
    name: "cell",
    pool: &["minting a cell", "fencing"],
};

pub const UPSTREAM: State = State {
    name: "upstream",
    pool: &["reaching upstream", "probing the model"],
};

pub const POLICY: State = State {
    name: "policy",
    pool: &["writing the policy"],
};

pub const SIGN: State = State {
    name: "sign",
    pool: &["wrapping the key", "sealing the policy"],
};

pub const UP: State = State {
    name: "up",
    pool: &["opening the ledger", "binding the socket"],
};

pub const ADD: State = State {
    name: "add",
    pool: &["admitting", "asking the gate", "sealing the ledger"],
};

pub const INTAKE: State = State {
    name: "intake",
    pool: &["herding", "swarming"],
};

pub const QUEUED: State = State {
    name: "queued",
    pool: &["admitting", "asking the gate"],
};

pub const RUNNING: State = State {
    name: "running",
    pool: &["minting a cell", "fencing"],
};

pub const BLOCKED: State = State {
    name: "blocked",
    pool: &["waiting"],
};

pub const DIGEST: State = State {
    name: "digest",
    pool: &["rolling up", "counting the room"],
};

pub const DIGEST_MODEL: State = State {
    name: "digest",
    pool: &["rolling up", "metering"],
};

/// The rollup might still be a count or a model call.
pub const ROLLUP: State = State {
    name: "digest",
    pool: &["rolling up"],
};

pub const BRIDGE: State = State {
    name: "bridge",
    pool: &["listening", "holding the bridge"],
};

pub const VOTE: State = State {
    name: "vote",
    pool: &["settling votes"],
};

pub const PIN: State = State {
    name: "pin",
    pool: &["promoting"],
};

#[derive(Clone, Copy)]
pub enum Sink {
    Stdout,
    Stderr,
}

pub fn label(state: &State, tick: u64, plain: bool) -> &'static str {
    if plain || state.pool.is_empty() {
        state.name
    } else {
        state.pool[(tick as usize) % state.pool.len()]
    }
}

pub fn tick_at(elapsed: Duration) -> u64 {
    elapsed.as_secs() / 3
}

pub fn wants_plain(flag: Option<&str>, is_tty: bool) -> bool {
    flag == Some("1") || !is_tty
}

pub fn plain_mode(is_tty: bool) -> bool {
    wants_plain(std::env::var("INLET_PLAIN").ok().as_deref(), is_tty)
}

/// Once the policy asks for a model rollup, pick a pool that matches the work.
/// `posts` is `None` when the channel count is not exact.
pub fn model_digest_state(spent: u64, cap: u64, posts: Option<usize>) -> &'static State {
    let room = spent < cap;
    match posts {
        Some(n) if n >= board::ROLLUP_AT && room => &DIGEST_MODEL,
        Some(_) => &DIGEST,
        None if room => &ROLLUP,
        None => &DIGEST,
    }
}

struct Face {
    state: &'static State,
    extra: String,
}

pub struct Hold {
    bar: Option<ProgressBar>,
    stop: Option<Arc<AtomicBool>>,
    join: Option<JoinHandle<()>>,
    face: Option<Arc<Mutex<Face>>>,
    shown: Arc<Mutex<&'static str>>,
    plain: bool,
    sink: Sink,
    started: Instant,
}

impl Hold {
    pub fn start(state: &'static State) -> Result<Self> {
        Self::start_on(state, Sink::Stderr)
    }

    pub fn start_on(state: &'static State, sink: Sink) -> Result<Self> {
        let plain = plain_mode(sink.is_tty());
        let shown = Arc::new(Mutex::new(state.name));
        if plain {
            emit(sink, state.name);
            return Ok(Self {
                bar: None,
                stop: None,
                join: None,
                face: None,
                shown,
                plain: true,
                sink,
                started: Instant::now(),
            });
        }
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template("{spinner} {msg}  {elapsed}")
                .map_err(|_| err("progress"))?,
        );
        bar.set_draw_target(sink.target());
        bar.set_message(compose(state, 0, ""));
        bar.enable_steady_tick(Duration::from_millis(80));
        let face = Arc::new(Mutex::new(Face {
            state,
            extra: String::new(),
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let shared = Arc::clone(&face);
        let paint = bar.clone();
        let started = Instant::now();
        let join = thread::spawn(move || {
            let mut last = String::new();
            while !flag.load(Ordering::Acquire) {
                let tick = tick_at(started.elapsed());
                let next = {
                    let guard = shared.lock().unwrap_or_else(|poison| poison.into_inner());
                    compose(guard.state, tick, &guard.extra)
                };
                if next != last {
                    paint.set_message(next.clone());
                    last = next;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        Ok(Self {
            bar: Some(bar),
            stop: Some(stop),
            join: Some(join),
            face: Some(face),
            shown,
            plain: false,
            sink,
            started,
        })
    }

    pub fn show(&self, state: &'static State, extra: &str) {
        if self.plain {
            let mut shown = self
                .shown
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if *shown != state.name {
                emit(self.sink, state.name);
                *shown = state.name;
            }
            return;
        }
        let Some(face) = &self.face else {
            return;
        };
        {
            let mut guard = face.lock().unwrap_or_else(|poison| poison.into_inner());
            guard.state = state;
            guard.extra = extra.to_string();
        }
        if let Some(bar) = &self.bar {
            let tick = tick_at(self.started.elapsed());
            bar.set_message(compose(state, tick, extra));
        }
    }

    fn stop_thread(&mut self) {
        if let Some(stop) = &self.stop {
            stop.store(true, Ordering::Release);
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.stop_thread();
        if let Some(bar) = self.bar.take() {
            bar.finish_and_clear();
        }
    }
}

impl Sink {
    fn is_tty(self) -> bool {
        match self {
            Sink::Stdout => io::stdout().is_terminal(),
            Sink::Stderr => io::stderr().is_terminal(),
        }
    }

    fn target(self) -> ProgressDrawTarget {
        match self {
            Sink::Stdout => ProgressDrawTarget::stdout(),
            Sink::Stderr => ProgressDrawTarget::stderr(),
        }
    }
}

fn emit(sink: Sink, text: &str) {
    match sink {
        Sink::Stdout => {
            println!("{text}");
            let _ = io::stdout().flush();
        }
        Sink::Stderr => {
            eprintln!("{text}");
            let _ = io::stderr().flush();
        }
    }
}

fn compose(state: &State, tick: u64, extra: &str) -> String {
    let verb = label(state, tick, false);
    if extra.is_empty() {
        verb.to_string()
    } else {
        format!("{verb}  {extra}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: &[&State] = &[
        &CELL,
        &UPSTREAM,
        &POLICY,
        &SIGN,
        &UP,
        &ADD,
        &INTAKE,
        &QUEUED,
        &RUNNING,
        &BLOCKED,
        &DIGEST,
        &DIGEST_MODEL,
        &ROLLUP,
        &BRIDGE,
        &VOTE,
        &PIN,
    ];

    #[test]
    fn each_state_draws_its_own_pool() {
        for state in ALL {
            assert!(!state.pool.is_empty(), "{}", state.name);
            for tick in 0..24 {
                let word = label(state, tick, false);
                assert!(state.pool.contains(&word), "{} drew {word}", state.name);
            }
            let mut seen = Vec::new();
            for tick in 0..state.pool.len() as u64 {
                let word = label(state, tick, false);
                if !seen.contains(&word) {
                    seen.push(word);
                }
            }
            assert_eq!(seen.len(), state.pool.len(), "{}", state.name);
            assert_eq!(label(state, state.pool.len() as u64, false), state.pool[0]);
        }
    }

    #[test]
    fn plain_names_the_state() {
        for state in ALL {
            for tick in 0..8 {
                assert_eq!(label(state, tick, true), state.name);
            }
        }
    }

    #[test]
    fn verbs_stay_on_their_state() {
        only("metering", &[&DIGEST_MODEL]);
        only("counting the room", &[&DIGEST]);
        only("swarming", &[&INTAKE]);
        only("herding", &[&INTAKE]);
        only("sealing the ledger", &[&ADD]);
        only("slicing the purse", &[]);
        only("wrapping the key", &[&SIGN]);
        only("sealing the policy", &[&SIGN]);
        only("writing the policy", &[&POLICY]);
        only("opening the ledger", &[&UP]);
        only("binding the socket", &[&UP]);
        only("reaching upstream", &[&UPSTREAM]);
        only("listening", &[&BRIDGE]);
        only("holding the bridge", &[&BRIDGE]);
        only("relaying", &[]);
        only("settling votes", &[&VOTE]);
        only("promoting", &[&PIN]);
        only("minting a cell", &[&CELL, &RUNNING]);
        only("fencing", &[&CELL, &RUNNING]);
        only("admitting", &[&ADD, &QUEUED]);
        only("asking the gate", &[&ADD, &QUEUED]);
        only("rolling up", &[&DIGEST, &DIGEST_MODEL, &ROLLUP]);
        only("waiting", &[&BLOCKED]);
    }

    #[test]
    fn tick_steps_every_three_seconds() {
        assert_eq!(tick_at(Duration::from_secs(0)), 0);
        assert_eq!(tick_at(Duration::from_secs(2)), 0);
        assert_eq!(tick_at(Duration::from_millis(2999)), 0);
        assert_eq!(tick_at(Duration::from_secs(3)), 1);
        assert_eq!(tick_at(Duration::from_secs(5)), 1);
        assert_eq!(tick_at(Duration::from_secs(6)), 2);
    }

    #[test]
    fn plain_follows_the_flag_and_the_tty() {
        assert!(wants_plain(Some("1"), true));
        assert!(wants_plain(Some("1"), false));
        assert!(wants_plain(None, false));
        assert!(wants_plain(Some("0"), false));
        assert!(!wants_plain(None, true));
        assert!(!wants_plain(Some("0"), true));
    }

    #[test]
    fn model_digest_picks_the_honest_pool() {
        assert!(model_digest_state(0, 100, Some(24))
            .pool
            .contains(&"metering"));
        assert!(!model_digest_state(0, 100, Some(23))
            .pool
            .contains(&"metering"));
        assert!(model_digest_state(0, 100, Some(23))
            .pool
            .contains(&"counting the room"));
        assert!(!model_digest_state(0, 0, Some(40))
            .pool
            .contains(&"metering"));
        assert!(model_digest_state(5, 5, Some(40))
            .pool
            .contains(&"counting the room"));
        let unsure = model_digest_state(0, 100, None);
        assert!(unsure.pool.contains(&"rolling up"));
        assert!(!unsure.pool.contains(&"metering"));
        assert!(!unsure.pool.contains(&"counting the room"));
        assert_eq!(model_digest_state(0, 100, Some(24)).name, "digest");
        assert_eq!(
            label(model_digest_state(0, 100, Some(24)), 1, true),
            "digest"
        );
    }

    fn only(verb: &str, owners: &[&State]) {
        for state in ALL {
            let has = state.pool.contains(&verb);
            let owner = owners.iter().any(|owner| std::ptr::eq(*owner, *state));
            assert_eq!(has, owner, "{verb} on {}", state.name);
        }
    }
}
