use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::board;
use crate::cell::{self, SpawnRequest};
use crate::config::{AdmitCtx, DecisionKind, OnCrash, Policy, Rollup};
use crate::decision::{self, Answer, Call, ConstraintIn, Header, PostIn};
use crate::error::{err, Result};
use crate::gate::{self, Verdict};
use crate::id::{self, now_ms};
use crate::isolate;
use crate::ledger::{self, Ledger};
use crate::model::{Budget, Decoded, Record, TaskState};
use crate::paths;
use crate::proto::{self, NewTask, Request};
use crate::proxy::{self, Hub, Note};
use crate::registry;
use crate::snap;
use crate::state::{PostView, State};
use crate::text::mentions;

const SOFT_FLUSH: Duration = Duration::from_millis(50);
const TRANSCRIPT_CAP: u64 = 16 * 1024 * 1024;

static SIGNAL_FD: AtomicI32 = AtomicI32::new(-1);

extern "C" fn on_signal(sig: i32) {
    let fd = SIGNAL_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        let byte = [sig as u8];
        unsafe {
            libc::write(fd, byte.as_ptr().cast(), 1);
        }
    }
}

enum Incoming {
    Conn {
        id: u64,
        tx: SyncSender<String>,
        worker: bool,
    },
    Line {
        id: u64,
        line: String,
    },
    Gone(u64),
    Signal,
}

struct Client {
    tx: SyncSender<String>,
    debug: u8,
    follow: Option<String>,
    watch: bool,
    worker: bool,
}

struct Worker {
    id: String,
    pid: i32,
    stdout: File,
    log: File,
    log_len: u64,
}

struct Check {
    id: String,
    pid: i32,
    started: Instant,
    stdout: File,
}

struct GateHit {
    at: Instant,
    failed: bool,
    p_num: u64,
    p_den: u64,
    conflicts: Vec<String>,
}

enum GateMsg {
    Ok { key: u64, answer: Answer },
    Err { key: u64 },
}

enum GatePrep {
    Off,
    Wait,
    Down,
    Ready {
        p_num: u64,
        p_den: u64,
        conflicts: Vec<String>,
    },
}

struct Daemon {
    home: PathBuf,
    policy: Policy,
    ledger: Ledger,
    state: State,
    cell_ok: bool,
    hub: Arc<Hub>,
    notes: Receiver<Note>,
    gate_tx: Sender<GateMsg>,
    gate_rx: Receiver<GateMsg>,
    gate_cache: HashMap<u64, GateHit>,
    gate_inflight: HashSet<u64>,
    cmds: Receiver<Incoming>,
    clients: HashMap<u64, Client>,
    workers: Vec<Worker>,
    checks: Vec<Check>,
    pending: HashMap<String, String>,
    soft: Vec<Record>,
    last_flush: Instant,
    shutting_down: bool,
    holder: bool,
    fence: u64,
    snap_offset: u64,
}

pub fn serve(home: &Path, foreground: bool) -> Result<()> {
    fs::create_dir_all(home.join("run"))?;
    fs::create_dir_all(home.join("proxy"))?;
    fs::create_dir_all(home.join("work"))?;
    fs::create_dir_all(home.join("runs"))?;
    if !foreground {
        return daemonize(home);
    }
    run(home)
}

fn daemonize(home: &Path) -> Result<()> {
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(err("fork"));
    }
    if pid > 0 {
        let sock = paths::operator_sock(home);
        for _ in 0..150 {
            if sock.exists() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(20));
        }
        return Err(err("daemon failed to start"));
    }
    unsafe {
        libc::setsid();
        if let Ok(log) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths::daemon_log(home))
        {
            let fd = log.as_raw_fd();
            libc::dup2(fd, 1);
            libc::dup2(fd, 2);
        }
        let devnull = libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
        if devnull >= 0 {
            libc::dup2(devnull, 0);
            libc::close(devnull);
        }
    }
    match run(home) {
        Ok(()) => unsafe { libc::_exit(0) },
        Err(e) => {
            eprintln!("inlet: {e}");
            unsafe { libc::_exit(1) }
        }
    }
}

fn run(home: &Path) -> Result<()> {
    let policy = load_signed(home)?;
    let node = snap::node_id(home)?;
    let lease = snap::ensure_lease(home, &node)?;
    let mut index = snap::load(home)?;
    let log_path = paths::ledger(home);
    let log_len = fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
    if let Some(saved) = &index {
        if log_len == 0 && saved.offset > 0 {
            let mut adopted = saved.clone();
            adopted.offset = 0;
            snap::store(home, &adopted)?;
            index = Some(adopted);
        } else if log_len < saved.offset {
            return Err(err("snapshot is ahead of the log"));
        }
    }
    let snap_offset = index.as_ref().map(|saved| saved.offset).unwrap_or(0);
    let opened = if snap_offset > 0 {
        ledger::open_from(&log_path, snap_offset)?
    } else {
        ledger::open(&log_path)?
    };
    opened.ledger.lock()?;
    let mut state = State::new(&policy.cfg);
    state.fence = lease.gen;
    if let Some(saved) = &index {
        snap::restore(&mut state, saved);
        state.fence = lease.gen;
    }
    for decoded in &opened.records {
        if let Decoded::Rec(rec) = decoded {
            state.apply(rec);
        }
    }
    snap::align_caps(&mut state.purse, &policy.cfg);
    registry::replay(home, &opened.records)?;
    let hub = Hub::new(
        policy.cfg.proxy.upstream.clone(),
        policy.cfg.proxy.key.clone(),
    );
    let (note_tx, notes) = proxy::channel();
    hub.set_notes(note_tx);
    hub.set_debug(policy.cfg.debug);
    proxy::listen(&paths::proxy_sock(home), hub.clone())?;

    let (cmd_tx, cmds) = mpsc::channel();
    let (gate_tx, gate_rx) = mpsc::channel();
    install_signals(cmd_tx.clone())?;
    listen_socket(&paths::worker_sock(home), cmd_tx.clone(), true)?;
    listen_socket(&paths::operator_sock(home), cmd_tx, false)?;
    let cell_ok = cell::probe(home);

    let mut daemon = Daemon {
        home: home.to_path_buf(),
        policy,
        ledger: opened.ledger,
        state,
        cell_ok,
        hub,
        notes,
        gate_tx,
        gate_rx,
        gate_cache: HashMap::new(),
        gate_inflight: HashSet::new(),
        cmds,
        clients: HashMap::new(),
        workers: Vec::new(),
        checks: Vec::new(),
        pending: HashMap::new(),
        soft: Vec::new(),
        last_flush: Instant::now(),
        shutting_down: false,
        holder: lease.node == node,
        fence: lease.gen,
        snap_offset,
    };
    daemon.recover_and_arm()?;
    fs::write(paths::pid_file(home), format!("{}\n", std::process::id()))?;
    loop {
        match daemon.cmds.recv_timeout(Duration::from_millis(20)) {
            Ok(Incoming::Conn { id, tx, worker }) => {
                daemon.clients.insert(
                    id,
                    Client {
                        tx,
                        debug: daemon.policy.cfg.debug,
                        follow: None,
                        watch: false,
                        worker,
                    },
                );
            }
            Ok(Incoming::Line { id, line }) => daemon.handle(id, &line)?,
            Ok(Incoming::Gone(id)) => {
                daemon.clients.remove(&id);
                daemon.hub.set_debug(daemon.max_debug());
            }
            Ok(Incoming::Signal) => {
                daemon.shutdown()?;
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        daemon.pump()?;
    }
    daemon.flush_soft()?;
    Ok(())
}

fn install_signals(tx: Sender<Incoming>) -> Result<()> {
    let mut fds = [0; 2];
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } != 0 {
        return Err(err("signal pipe"));
    }
    SIGNAL_FD.store(fds[1], Ordering::Relaxed);
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = on_signal as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        libc::sigaction(libc::SIGINT, &action, std::ptr::null_mut());
        libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut());
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    let read = fds[0];
    thread::spawn(move || {
        let mut buf = [0u8; 8];
        loop {
            let n = unsafe { libc::read(read, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                for _ in 0..n {
                    let _ = tx.send(Incoming::Signal);
                }
            } else if n < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
                break;
            } else {
                break;
            }
        }
    });
    Ok(())
}

fn listen_socket(path: &Path, tx: Sender<Incoming>, worker: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let _ = fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    let next = Arc::new(std::sync::atomic::AtomicU64::new(if worker {
        1 << 32
    } else {
        1
    }));
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            if !same_user(&conn) {
                continue;
            }
            let id = next.fetch_add(1, Ordering::Relaxed);
            let Ok(writer) = conn.try_clone() else {
                continue;
            };
            let (reply_tx, reply_rx) = mpsc::sync_channel::<String>(256);
            thread::spawn(move || {
                let mut writer = writer;
                for msg in reply_rx {
                    if writer.write_all(msg.as_bytes()).is_err() {
                        break;
                    }
                }
            });
            let _ = tx.send(Incoming::Conn {
                id,
                tx: reply_tx,
                worker,
            });
            let tx = tx.clone();
            thread::spawn(move || {
                let mut reader = std::io::BufReader::new(conn);
                let mut line = String::new();
                loop {
                    line.clear();
                    match std::io::BufRead::read_line(&mut reader, &mut line) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let clean = line.trim_end().to_string();
                            if clean.is_empty() {
                                continue;
                            }
                            if tx.send(Incoming::Line { id, line: clean }).is_err() {
                                break;
                            }
                        }
                    }
                }
                let _ = tx.send(Incoming::Gone(id));
            });
        }
    });
    Ok(())
}

fn same_user(stream: &std::os::unix::net::UnixStream) -> bool {
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    rc == 0 && cred.uid == unsafe { libc::geteuid() }
}

impl Daemon {
    fn recover_and_arm(&mut self) -> Result<()> {
        let now = now_ms();
        let extra = self.state.recover(&self.policy.cfg, now);
        if !extra.is_empty() {
            self.commit(&extra, true)?;
        }
        if self.state.purse.due(now_ms()) {
            self.commit(
                &[Record::Reset {
                    grant: self.policy.cfg.caps.max_tokens,
                    ts: now_ms(),
                }],
                true,
            )?;
        }
        self.settle()
    }

    fn commit(&mut self, recs: &[Record], hard: bool) -> Result<()> {
        if hard {
            self.flush_soft()?;
            self.ledger.append_all(recs, true)?;
        } else {
            self.soft.extend(recs.iter().cloned());
        }
        for rec in recs {
            self.state.apply(rec);
            self.emit(rec);
        }
        Ok(())
    }

    fn flush_soft(&mut self) -> Result<()> {
        if self.soft.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.soft);
        self.ledger.append_all(&batch, true)?;
        self.last_flush = Instant::now();
        Ok(())
    }

    fn pump(&mut self) -> Result<()> {
        while let Ok(note) = self.notes.try_recv() {
            self.on_note(note)?;
        }
        while let Ok(msg) = self.gate_rx.try_recv() {
            self.on_gate(msg)?;
        }
        self.drain_logs();
        self.reap()?;
        self.timeouts()?;
        if self.state.purse.due(now_ms()) && !self.shutting_down {
            self.commit(
                &[Record::Reset {
                    grant: self.policy.cfg.caps.max_tokens,
                    ts: now_ms(),
                }],
                true,
            )?;
        }
        if !self.shutting_down {
            self.admit()?;
        }
        if self.last_flush.elapsed() >= SOFT_FLUSH {
            self.flush_soft()?;
        }
        Ok(())
    }

    fn on_note(&mut self, note: Note) -> Result<()> {
        match note {
            Note::Cost { id, tokens } => {
                self.commit(
                    &[Record::Cost {
                        id,
                        tokens,
                        ts: now_ms(),
                    }],
                    false,
                )?;
            }
            Note::Empty { id } => {
                self.pending.insert(id.clone(), "purse".into());
                self.signal(&id, libc::SIGKILL);
            }
            Note::Debug { level, msg } => self.broadcast(level, json!({"ev":"debug","text": msg})),
        }
        Ok(())
    }

    fn on_gate(&mut self, msg: GateMsg) -> Result<()> {
        let (key, failed, answer) = match msg {
            GateMsg::Ok { key, answer } => (key, false, Some(answer)),
            GateMsg::Err { key } => (key, true, None),
        };
        self.gate_inflight.remove(&key);
        if failed {
            self.gate_cache.insert(
                key,
                GateHit {
                    at: Instant::now(),
                    failed: true,
                    p_num: 0,
                    p_den: 1,
                    conflicts: Vec::new(),
                },
            );
            return Ok(());
        }
        let answer = answer.unwrap_or(Answer {
            p_num: 0,
            p_den: 1,
            conflicts: Vec::new(),
            tokens: 1,
        });
        let extra = answer.tokens.saturating_sub(1);
        if extra > 0 {
            self.commit(
                &[Record::Gate {
                    tokens: extra,
                    ts: now_ms(),
                }],
                true,
            )?;
        }
        self.gate_cache.insert(
            key,
            GateHit {
                at: Instant::now(),
                failed: false,
                p_num: answer.p_num,
                p_den: answer.p_den.max(1),
                conflicts: answer.conflicts,
            },
        );
        Ok(())
    }

    /// `Wait` leaves the task queued. A cache hit does not start a second call.
    fn gate_for(&mut self, task: &crate::state::TaskView) -> Result<GatePrep> {
        let kind = self.policy.cfg.decision.kind;
        let endpoint = self.policy.cfg.decision.endpoint.clone();
        let timeout_ms = self.policy.cfg.decision.timeout_ms;
        let purse = self.policy.cfg.decision.purse_tokens;
        if kind == DecisionKind::Off {
            return Ok(GatePrep::Off);
        }
        let call = self.decision_call(task);
        let key = call.key();
        if let Some(hit) = self.gate_cache.get(&key) {
            if hit.at.elapsed() < decision::CACHE_TTL {
                if hit.failed {
                    return Ok(GatePrep::Down);
                }
                return Ok(GatePrep::Ready {
                    p_num: hit.p_num,
                    p_den: hit.p_den,
                    conflicts: hit.conflicts.clone(),
                });
            }
        }
        if self.gate_inflight.contains(&key) {
            return Ok(GatePrep::Wait);
        }
        let Some(endpoint) = endpoint else {
            return Ok(GatePrep::Down);
        };
        let timeout = Duration::from_millis(timeout_ms.max(1));
        let remaining = purse.saturating_sub(self.state.decision_spent);
        if remaining == 0 {
            return Ok(GatePrep::Down);
        }
        // One token is durable before the call leaves. The rest settles on the reply.
        self.commit(
            &[Record::Gate {
                tokens: 1,
                ts: now_ms(),
            }],
            true,
        )?;
        self.gate_inflight.insert(key);
        let tx = self.gate_tx.clone();
        thread::spawn(move || {
            let msg = match decision::ask(&endpoint, &call, timeout) {
                Ok(answer) => GateMsg::Ok { key, answer },
                Err(_) => GateMsg::Err { key },
            };
            let _ = tx.send(msg);
        });
        Ok(GatePrep::Wait)
    }

    fn decision_call(&self, task: &crate::state::TaskView) -> Call {
        let mut parents = Vec::new();
        let mut cursor = task.parent.clone();
        while let Some(id) = cursor {
            let Some(parent) = self.state.tasks.get(&id) else {
                break;
            };
            parents.push(header_of(parent));
            if parents.len() >= 8 {
                break;
            }
            cursor = parent.parent.clone();
        }
        let key = gate::samples_key(&task.worker, &task.tags);
        let posts = self
            .state
            .posts
            .iter()
            .rev()
            .take(decision::POST_WINDOW)
            .map(|post| PostIn {
                author: post.author.clone(),
                role: post.role.clone(),
                weight: post.weight,
                channel: post.channel.clone(),
                text: clip(&crate::text::redact(&post.text), 160),
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        Call {
            model: self.policy.cfg.decision.model.clone(),
            header: header_of(task),
            parents,
            samples: self.state.samples.series(&key),
            constraints: self
                .state
                .constraints
                .iter()
                .map(|c| ConstraintIn {
                    id: c.id.clone(),
                    text: clip(&c.text, 240),
                    tags: c.tags.clone(),
                })
                .collect(),
            posts,
            human_weight: self.policy.cfg.human_weight,
        }
    }

    fn admit(&mut self) -> Result<()> {
        if !self.holder {
            return Ok(());
        }
        let queued: Vec<String> = self.state.queue.iter().cloned().collect();
        let mut batch: Vec<Record> = Vec::new();
        let mut chosen: Vec<String> = Vec::new();
        let mut hold_tokens = 0u64;
        let mut hold_mem = 0u64;
        let mut hold_pids = 0u64;
        let mut child_hold: HashMap<String, (u64, u64, u64)> = HashMap::new();
        for id in queued {
            if self.state.live() + chosen.len() >= self.policy.cfg.caps.max_live {
                break;
            }
            let Some(task) = self.state.tasks.get(&id).cloned() else {
                continue;
            };
            if task.state != TaskState::Queued {
                continue;
            }
            let (p, blocked) = match self.gate_for(&task)? {
                GatePrep::Wait => continue,
                GatePrep::Down => {
                    self.commit(
                        &[Record::Deny {
                            id: id.clone(),
                            reason: "gate".into(),
                            ts: now_ms(),
                        }],
                        true,
                    )?;
                    continue;
                }
                GatePrep::Off => (None, self.tag_blocked(&task)),
                GatePrep::Ready {
                    p_num,
                    p_den,
                    conflicts,
                } => (Some((p_num, p_den)), !conflicts.is_empty()),
            };
            let depth = depth_of(&self.state, task.parent.as_deref());
            let lua = self.policy.admit(&AdmitCtx {
                depth,
                live: self.state.live() + chosen.len(),
                queued: self.state.queue.len(),
                worker: &task.worker,
                goal: &task.goal,
                value: task.value,
                tags: &task.tags,
            });
            // Purse fit uses a virtual hold so one batch cannot oversubscribe.
            let saved = (
                self.state.purse.available,
                self.state.purse.memory_held,
                self.state.purse.pids_held,
            );
            let saved_slice = task.parent.as_ref().and_then(|parent| {
                self.state
                    .purse
                    .open
                    .get(parent)
                    .map(|slice| (slice.used, slice.memory_lent, slice.pids_lent))
            });
            if task.parent.is_none() {
                self.state.purse.available = self.state.purse.available.saturating_sub(hold_tokens);
                self.state.purse.memory_held =
                    self.state.purse.memory_held.saturating_add(hold_mem);
                self.state.purse.pids_held = self.state.purse.pids_held.saturating_add(hold_pids);
            } else if let Some(parent) = task.parent.clone() {
                if let Some((tokens, memory, pids)) = child_hold.get(&parent).copied() {
                    if let Some(slice) = self.state.purse.open.get_mut(&parent) {
                        slice.used = slice.used.saturating_add(tokens);
                        slice.memory_lent = slice.memory_lent.saturating_add(memory);
                        slice.pids_lent = slice.pids_lent.saturating_add(pids);
                    }
                }
            }
            let verdict = gate::decide(
                &task,
                &gate::Ctx {
                    cfg: &self.policy.cfg,
                    purse: &self.state.purse,
                    samples: &self.state.samples,
                    live: self.state.live() + chosen.len(),
                    depth,
                    cell_ok: self.cell_ok,
                    lua_says: lua,
                    p,
                    blocked,
                },
            );
            self.state.purse.available = saved.0;
            self.state.purse.memory_held = saved.1;
            self.state.purse.pids_held = saved.2;
            if let (Some(parent), Some((used, memory, pids))) = (&task.parent, saved_slice) {
                if let Some(slice) = self.state.purse.open.get_mut(parent) {
                    slice.used = used;
                    slice.memory_lent = memory;
                    slice.pids_lent = pids;
                }
            }
            match verdict {
                Verdict::Queue => break,
                Verdict::Deny(reason) => {
                    self.commit(
                        &[Record::Deny {
                            id: id.clone(),
                            reason: reason.into(),
                            ts: now_ms(),
                        }],
                        true,
                    )?;
                }
                Verdict::Allow => {
                    if task.parent.is_none() {
                        hold_tokens += task.budget.tokens;
                        hold_mem += task.budget.memory_mb;
                        hold_pids += task.budget.pids;
                    } else if let Some(parent) = task.parent.clone() {
                        let slot = child_hold.entry(parent).or_insert((0, 0, 0));
                        slot.0 += task.budget.tokens;
                        slot.1 += task.budget.memory_mb;
                        slot.2 += task.budget.pids;
                    }
                    batch.push(Record::Admit {
                        id: id.clone(),
                        fence: self.fence,
                        tokens: task.budget.tokens,
                        seconds: task.budget.seconds,
                        memory_mb: task.budget.memory_mb,
                        pids: task.budget.pids,
                        ts: now_ms(),
                    });
                    chosen.push(id);
                }
            }
        }
        if batch.is_empty() {
            return Ok(());
        }
        // Durable before exec. One fsync for the whole wave.
        self.commit(&batch, true)?;
        for id in chosen {
            if let Err(e) = self.spawn_worker(&id) {
                self.finish(&id, 127, "cell", true)?;
                let _ = e;
            }
        }
        Ok(())
    }

    fn spawn_worker(&mut self, id: &str) -> Result<()> {
        let task = self
            .state
            .tasks
            .get(id)
            .cloned()
            .ok_or_else(|| err("missing task"))?;
        let worker = self
            .policy
            .cfg
            .workers
            .get(&task.worker)
            .cloned()
            .ok_or_else(|| err("missing worker"))?;
        let placement = isolate::place(
            self.policy.cfg.isolator,
            id,
            task.budget.memory_mb,
            task.budget.pids,
        );
        if placement.kind == "slurm" {
            return Err(err("slurm isolator cannot place a worker"));
        }
        let token = id::token();
        let net_none = matches!(worker.net, crate::config::Net::None);
        if let Some(recipe) = &task.recipe {
            let _ = registry::stage_run(&self.home, recipe, &paths::work(&self.home, id));
        }
        let depth = depth_of(&self.state, task.parent.as_deref());
        let preamble = registry::render_preamble(
            id,
            task.budget.tokens,
            task.budget.seconds,
            task.budget.memory_mb,
            depth,
            self.policy.cfg.caps.max_depth,
            self.policy.cfg.preamble.as_deref(),
        );
        let spawned = cell::spawn(&SpawnRequest {
            cmd: worker.cmd,
            work: paths::work(&self.home, id),
            scratch: paths::scratch(&self.home, id),
            registry: paths::registry(&self.home),
            root: paths::cell_root(&self.home, id),
            proxy_sock: paths::proxy_sock(&self.home),
            worker_sock: paths::worker_sock(&self.home),
            preamble,
            net_none,
            memory_mb: task.budget.memory_mb,
            pids: task.budget.pids,
            seconds: task.budget.seconds,
            token: token.clone(),
            task_id: id.to_string(),
            cgroup: placement.cgroup,
            door: true,
        })?;
        self.hub.insert(id, &token, task.budget.tokens);
        if let Some(parent) = &task.parent {
            self.hub.lend(parent, task.budget.tokens);
        }
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths::transcript(&self.home, id))?;
        self.workers.push(Worker {
            id: id.to_string(),
            pid: spawned.pid,
            stdout: spawned.stdout,
            log,
            log_len: 0,
        });
        self.commit(
            &[Record::Spawn {
                id: id.to_string(),
                pid: spawned.pid as u32,
                ts: now_ms(),
            }],
            false,
        )?;
        Ok(())
    }

    fn reap(&mut self) -> Result<()> {
        loop {
            let mut status = 0;
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid <= 0 {
                break;
            }
            if let Some(pos) = self.checks.iter().position(|check| check.pid == pid) {
                let id = self.checks.remove(pos).id;
                let code = if libc::WIFEXITED(status) {
                    libc::WEXITSTATUS(status) as i32
                } else if libc::WIFSIGNALED(status) {
                    128 + libc::WTERMSIG(status)
                } else {
                    -1
                };
                self.complete_check(&id, code)?;
                continue;
            }
            let Some(pos) = self.workers.iter().position(|w| w.pid == pid) else {
                continue;
            };
            let id = self.workers[pos].id.clone();
            let code = if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status) as i32
            } else if libc::WIFSIGNALED(status) {
                128 + libc::WTERMSIG(status)
            } else {
                -1
            };
            let signalled = libc::WIFSIGNALED(status);
            let reason = self.pending.remove(&id);
            match reason.as_deref() {
                Some("killed") => self.finish(&id, code, "killed", true)?,
                Some("timeout") => self.finish(&id, code, "timeout", true)?,
                // Crash keeps the slice. A purse kill refunds whatever the proxy did not spend.
                Some("purse") => self.finish(&id, code, "purse", true)?,
                Some(other) => self.finish(&id, code, other, other != "crash")?,
                None if signalled => self.crash(&id)?,
                None if code == 0 && self.has_verifier(&id) => self.start_check(&id)?,
                None if code == 0 => self.finish(&id, 0, "ok", true)?,
                None => self.finish(&id, code, "exit", true)?,
            }
        }
        Ok(())
    }

    fn has_verifier(&self, id: &str) -> bool {
        self.state
            .tasks
            .get(id)
            .and_then(|task| task.verifier.as_ref())
            .is_some_and(|cmd| !cmd.is_empty())
    }

    fn start_check(&mut self, id: &str) -> Result<()> {
        self.workers.retain(|worker| worker.id != id);
        if let Some(task) = self.state.tasks.get_mut(id) {
            task.pid = None;
        }
        let task = self.state.tasks.get(id).cloned();
        let Some(task) = task else {
            return self.finish(id, 127, "verifier", true);
        };
        let verifier = task.verifier.clone().unwrap_or_default();
        let dir = paths::work(&self.home, id);
        if let Some(recipe) = &task.recipe {
            let _ = registry::stage_run(&self.home, recipe, &dir);
        }
        let memory = task.budget.memory_mb.max(512);
        match cell::spawn(&SpawnRequest {
            cmd: vec!["/bin/sh".into(), "-c".into(), verifier],
            work: dir,
            scratch: self.home.join("run/checks").join(id),
            registry: paths::registry(&self.home),
            root: paths::cell_root(&self.home, &format!("{id}-check")),
            proxy_sock: PathBuf::from("/dev/null"),
            worker_sock: PathBuf::from("/dev/null"),
            preamble: String::new(),
            net_none: true,
            memory_mb: memory,
            pids: task.budget.pids.max(32),
            seconds: 20,
            token: String::new(),
            task_id: id.to_string(),
            cgroup: None,
            door: false,
        }) {
            Ok(child) => {
                self.checks.push(Check {
                    id: id.to_string(),
                    pid: child.pid,
                    started: Instant::now(),
                    stdout: child.stdout,
                });
                Ok(())
            }
            Err(_) => self.finish(id, 127, "verifier", true),
        }
    }

    fn complete_check(&mut self, id: &str, code: i32) -> Result<()> {
        let ok = code == 0;
        let reason = if ok { "ok" } else { "verifier" };
        let mut extra = vec![Record::Result {
            id: id.to_string(),
            ok,
            code,
            ts: now_ms(),
        }];
        let mut published = None;
        if ok {
            if let Some(rec) = self.maybe_promote(id)? {
                if let Record::Promote { name, .. } = &rec {
                    published = Some(name.clone());
                }
                extra.push(rec);
            }
        }
        self.finish_with(id, code, reason, true, extra)?;
        if let Some(name) = published {
            registry::materialize(&self.home, &name)?;
        }
        Ok(())
    }

    fn crash(&mut self, id: &str) -> Result<()> {
        let task = self.state.tasks.get(id).cloned();
        self.finish(id, -1, "crash", false)?;
        let Some(task) = task else {
            return Ok(());
        };
        let requeue = self
            .policy
            .cfg
            .workers
            .get(&task.worker)
            .map(|w| w.on_crash == OnCrash::Requeue)
            .unwrap_or(false);
        if requeue && !self.state.retried.contains(id) {
            self.commit(
                &[Record::Task {
                    id: id::ulid(),
                    parent: task.parent.clone(),
                    worker: task.worker.clone(),
                    tags: task.tags.clone(),
                    goal: task.goal.clone(),
                    verifier: task.verifier.clone(),
                    value: task.value,
                    budget: task.budget.clone(),
                    retry_of: Some(id.to_string()),
                    recipe: task.recipe.clone(),
                    ts: now_ms(),
                }],
                true,
            )?;
        }
        Ok(())
    }

    fn finish(&mut self, id: &str, code: i32, reason: &str, refund_unused: bool) -> Result<()> {
        self.finish_with(id, code, reason, refund_unused, Vec::new())
    }

    fn finish_with(
        &mut self,
        id: &str,
        code: i32,
        reason: &str,
        refund_unused: bool,
        mut extra: Vec<Record>,
    ) -> Result<()> {
        let parent = self
            .state
            .tasks
            .get(id)
            .and_then(|task| task.parent.clone());
        let lent = self
            .state
            .purse
            .open
            .get(id)
            .map(|slice| slice.used)
            .unwrap_or(0);
        let metered = self.hub.remove(id);
        let reserved = self
            .state
            .tasks
            .get(id)
            .map(|t| t.budget.tokens)
            .unwrap_or(0);
        // `used` is proxy spend plus tokens still lent to children.
        let used = lent.max(metered).min(reserved);
        let refund = if refund_unused {
            reserved.saturating_sub(used)
        } else {
            0
        };
        let tokens_used = reserved.saturating_sub(refund);
        if let Some(parent) = parent {
            self.hub.reclaim(&parent, reserved, tokens_used);
        }
        self.workers.retain(|w| w.id != id);
        if reason == "killed" {
            self.commit(
                &[Record::Kill {
                    id: id.to_string(),
                    ts: now_ms(),
                }],
                false,
            )?;
        }
        extra.push(Record::Exit {
            id: id.to_string(),
            code,
            reason: reason.into(),
            tokens_used,
            refund_tokens: refund,
            ts: now_ms(),
        });
        self.commit(&extra, true)?;
        Ok(())
    }

    fn timeouts(&mut self) -> Result<()> {
        let now = now_ms();
        let due: Vec<String> = self
            .state
            .tasks
            .values()
            .filter(|t| {
                t.state.is_live()
                    && t.admit_ms
                        .map(|start| {
                            now.saturating_sub(start) >= t.budget.seconds.saturating_mul(1000)
                        })
                        .unwrap_or(false)
            })
            .map(|t| t.id.clone())
            .collect();
        for id in due {
            self.pending.insert(id.clone(), "timeout".into());
            self.signal(&id, libc::SIGKILL);
        }
        let slow: Vec<i32> = self
            .checks
            .iter()
            .filter(|check| check.started.elapsed() >= Duration::from_secs(20))
            .map(|check| check.pid)
            .collect();
        for pid in slow {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        Ok(())
    }

    fn signal(&self, id: &str, sig: i32) {
        if let Some(worker) = self.workers.iter().find(|w| w.id == id) {
            unsafe {
                libc::kill(-worker.pid, sig);
                libc::kill(worker.pid, sig);
            }
        }
    }

    fn drain_logs(&mut self) {
        for worker in &mut self.workers {
            let mut buf = [0u8; 8192];
            loop {
                let n = unsafe {
                    libc::read(
                        worker.stdout.as_raw_fd(),
                        buf.as_mut_ptr().cast(),
                        buf.len(),
                    )
                };
                if n > 0 {
                    let n = n as usize;
                    let room = TRANSCRIPT_CAP.saturating_sub(worker.log_len) as usize;
                    if room > 0 {
                        let take = n.min(room);
                        let _ = worker.log.write_all(&buf[..take]);
                        worker.log_len += take as u64;
                    }
                } else {
                    break;
                }
            }
        }
        for check in &mut self.checks {
            let mut buf = [0u8; 8192];
            loop {
                let n = unsafe {
                    libc::read(check.stdout.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len())
                };
                if n <= 0 {
                    break;
                }
            }
        }
    }

    fn handle(&mut self, id: u64, line: &str) -> Result<()> {
        if self.clients.get(&id).is_some_and(|client| client.worker) {
            return self.handle_worker(id, line);
        }
        let req = match proto::parse_request(line) {
            Ok(req) => req,
            Err(e) => {
                self.reply(id, json!({"ok": false, "error": e.to_string()}));
                return Ok(());
            }
        };
        match req {
            Request::Status => self.reply(id, self.status_json()),
            Request::Add(task) => match self.enqueue(std::slice::from_ref(task.as_ref())) {
                Ok(ids) => self.reply(id, json!({"ok": true, "ids": ids})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::AddBatch(tasks) => match self.enqueue(&tasks) {
                Ok(ids) => self.reply(id, json!({"ok": true, "ids": ids})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Post { text, human } => match self.post(&text, human) {
                Ok(pid) => self.reply(id, json!({"ok": true, "id": pid})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Kill(task) => {
                self.pending.insert(task.clone(), "killed".into());
                self.signal(&task, libc::SIGKILL);
                self.reply(id, json!({"ok": true, "id": task}));
            }
            Request::Watch { debug, worker } => {
                if let Some(client) = self.clients.get_mut(&id) {
                    client.debug = debug.min(4);
                    client.follow = worker;
                    client.watch = true;
                }
                self.hub.set_debug(self.max_debug());
                self.reply(id, self.status_json());
            }
            Request::Hello { debug } => {
                if let Some(client) = self.clients.get_mut(&id) {
                    client.debug = debug.min(4);
                    client.watch = true;
                }
                self.hub.set_debug(self.max_debug());
                self.reply(id, self.status_json());
            }
            Request::Digest(channel) => match self.digest_channel(&channel) {
                Ok(body) => self.reply(id, body),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Vote {
                target,
                choice,
                channel,
                human,
            } => {
                let (voter, role) = if human {
                    ("you", "human")
                } else {
                    ("operator", "operator")
                };
                match self.cast(voter, role, &target, &choice, &channel) {
                    Ok(body) => self.reply(id, body),
                    Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
                }
            }
            Request::Bind(text) => match self.bind_text(&text) {
                Ok(cid) => self.reply(id, json!({"ok": true, "id": cid})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Clear { id: cid, sig } => match self.clear_constraint(&cid, &sig) {
                Ok(()) => self.reply(id, json!({"ok": true, "id": cid})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Sign(sig) => match self.install_signed(&sig) {
                Ok(()) => self.reply(id, json!({"ok": true})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Draft(text) => match self.write_draft(&text) {
                Ok(()) => self.reply(id, json!({"ok": true})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Diff => self.reply(id, self.diff_json()),
            Request::Snap => match self.take_snap() {
                Ok(v) => self.reply(id, v),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Pin(name) => match self.pin(&name) {
                Ok(()) => self.reply(id, json!({"ok": true, "name": name})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Debug(level) => {
                if let Some(client) = self.clients.get_mut(&id) {
                    client.debug = level;
                }
                self.hub.set_debug(self.max_debug());
                self.reply(id, json!({"ok": true, "debug": level}));
            }
        }
        Ok(())
    }

    fn handle_worker(&mut self, id: u64, line: &str) -> Result<()> {
        let value: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                self.reply(id, json!({"ok": false, "error": e.to_string()}));
                return Ok(());
            }
        };
        let reply = match value.get("op").and_then(|op| op.as_str()).unwrap_or("") {
            "spawn" => self.worker_spawn(&value),
            "post" => self.worker_post(&value),
            "board" => self.worker_board(&value),
            "vote" => self.worker_vote(&value),
            "draft" => self.worker_draft(&value),
            other => Err(err(format!("unknown op {other}"))),
        };
        match reply {
            Ok(body) => self.reply(id, body),
            Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
        }
        Ok(())
    }

    fn worker_id(&self, value: &Value) -> Result<String> {
        let token = value
            .get("token")
            .and_then(|t| t.as_str())
            .ok_or_else(|| err("token"))?;
        self.hub.id_of(token).ok_or_else(|| err("token"))
    }

    fn worker_spawn(&mut self, value: &Value) -> Result<Value> {
        let parent = self.worker_id(value)?;
        let verify = value
            .get("verify")
            .or_else(|| value.get("verifier"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty());
        let Some(verify) = verify else {
            return Err(err("verifier required"));
        };
        let parent_task = self
            .state
            .tasks
            .get(&parent)
            .ok_or_else(|| err("parent is not a task"))?;
        if !parent_task.state.is_live() {
            return Err(err("parent is not running"));
        }
        let tags = value
            .get("tags")
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let task = NewTask {
            worker: value
                .get("worker")
                .and_then(|s| s.as_str())
                .ok_or_else(|| err("missing worker"))?
                .to_string(),
            goal: value
                .get("goal")
                .and_then(|s| s.as_str())
                .ok_or_else(|| err("missing goal"))?
                .to_string(),
            verifier: Some(verify),
            no_verify: false,
            tokens: value.get("tokens").and_then(|n| n.as_u64()),
            seconds: value.get("seconds").and_then(|n| n.as_u64()),
            memory_mb: value.get("memory_mb").and_then(|n| n.as_u64()),
            pids: value.get("pids").and_then(|n| n.as_u64()),
            value: value.get("value").and_then(|n| n.as_u64()),
            tags,
            parent: Some(parent),
            recipe: value
                .get("recipe")
                .and_then(|s| s.as_str())
                .map(str::to_string)
                .filter(|s| !s.is_empty()),
            seed: None,
        };
        let ids = self.enqueue(std::slice::from_ref(&task))?;
        let child = ids.first().cloned().unwrap_or_default();
        let denied = self
            .state
            .tasks
            .get(&child)
            .filter(|task| task.state == TaskState::Failed)
            .map(|task| task.reason.clone());
        Ok(json!({"ok": true, "ids": ids, "denied": denied}))
    }

    fn worker_post(&mut self, value: &Value) -> Result<Value> {
        let author = self.worker_id(value)?;
        let text = value
            .get("text")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .trim();
        if text.is_empty() {
            return Err(err("empty post"));
        }
        let channel = value
            .get("channel")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("general");
        let tags = self
            .state
            .tasks
            .get(&author)
            .map(|task| task.tags.clone())
            .unwrap_or_default();
        board::ensure_post(&tags, &self.state, &author, channel)?;
        let id = id::ulid();
        self.commit(
            &[Record::Post {
                id: id.clone(),
                author: author.clone(),
                role: "worker".into(),
                text: text.into(),
                weight: 1,
                channel: channel.into(),
                mentions: mentions(text),
                ts: now_ms(),
            }],
            false,
        )?;
        Ok(json!({"ok": true, "id": id}))
    }

    fn worker_board(&self, value: &Value) -> Result<Value> {
        let reader = self.worker_id(value)?;
        let tags = self
            .state
            .tasks
            .get(&reader)
            .map(|task| task.tags.clone())
            .unwrap_or_default();
        let posts: Vec<Value> = self
            .state
            .posts
            .iter()
            .filter(|post| board::show_post(&self.state, &reader, &tags, post))
            .map(post_json)
            .collect();
        Ok(json!({"ok": true, "posts": posts}))
    }

    fn worker_vote(&mut self, value: &Value) -> Result<Value> {
        let voter = self.worker_id(value)?;
        let target = value
            .get("target")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .trim();
        let choice = value
            .get("choice")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .trim();
        let channel = value
            .get("channel")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("general");
        self.cast(&voter, "worker", target, choice, channel)
    }

    fn cast(
        &mut self,
        voter: &str,
        role: &str,
        target: &str,
        choice: &str,
        channel: &str,
    ) -> Result<Value> {
        let target = target.trim();
        let choice = choice.trim();
        if target.is_empty() || choice.is_empty() {
            return Err(err("missing target"));
        }
        board::ensure_choice(choice, channel)?;
        let weight = board::weight_for(
            role,
            self.policy.cfg.human_weight,
            board::demoted(&self.state, voter),
        );
        let id = id::ulid();
        self.commit(
            &[Record::Vote {
                id: id.clone(),
                voter: voter.to_string(),
                role: role.to_string(),
                target: target.to_string(),
                channel: channel.to_string(),
                choice: choice.to_string(),
                weight,
                ts: now_ms(),
            }],
            false,
        )?;
        self.settle()?;
        let moderation = self
            .state
            .moderation
            .iter()
            .rev()
            .find(|item| item.target == target && item.action == choice && item.channel == channel)
            .map(|item| item.id.clone());
        Ok(json!({"ok": true, "id": id, "weight": weight, "moderation": moderation}))
    }

    fn settle(&mut self) -> Result<()> {
        let due = board::passing(&self.state, self.policy.cfg.human_weight);
        if due.is_empty() {
            return Ok(());
        }
        let ts = now_ms();
        let mut recs = Vec::new();
        for item in due {
            recs.push(Record::Moderation {
                id: id::ulid(),
                target: item.target.clone(),
                action: item.action.clone(),
                channel: item.channel.clone(),
                weight: item.weight,
                ts,
            });
            if item.action == "flag" {
                recs.push(Record::Post {
                    id: id::ulid(),
                    author: "board".into(),
                    role: "operator".into(),
                    text: format!("flag {}", item.target),
                    weight: 1,
                    channel: item.channel,
                    mentions: Vec::new(),
                    ts,
                });
            }
        }
        self.commit(&recs, false)
    }

    fn worker_draft(&mut self, value: &Value) -> Result<Value> {
        let author = self.worker_id(value)?;
        let name = value
            .get("name")
            .and_then(|s| s.as_str())
            .ok_or_else(|| err("missing name"))?;
        let run = value
            .get("run")
            .and_then(|s| s.as_str())
            .ok_or_else(|| err("missing run"))?;
        let verifier = value
            .get("verifier")
            .or_else(|| value.get("verify"))
            .and_then(|s| s.as_str())
            .ok_or_else(|| err("missing verifier"))?;
        let tags = value
            .get("tags")
            .and_then(|t| t.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        registry::submit(&self.home, &author, name, run, verifier, tags)?;
        Ok(json!({"ok": true, "name": name}))
    }

    fn pin(&mut self, name: &str) -> Result<()> {
        let rec = registry::promote_record(name, "operator");
        // The draft has to exist before the record is durable.
        if registry::stage_run(&self.home, name, &self.home.join("run/pin-stage")).is_err() {
            return Err(err(format!("no draft {name}")));
        }
        let _ = fs::remove_dir_all(self.home.join("run/pin-stage"));
        self.commit(&[rec], true)?;
        registry::materialize(&self.home, name)?;
        Ok(())
    }

    fn maybe_promote(&mut self, id: &str) -> Result<Option<Record>> {
        let Some(task) = self.state.tasks.get(id).cloned() else {
            return Ok(None);
        };
        let Some(recipe) = task.recipe else {
            return Ok(None);
        };
        if task.verifier.is_none() {
            return Ok(None);
        }
        registry::consider(&self.home, self.policy.cfg.unattended, id, &recipe)
    }

    fn enqueue(&mut self, tasks: &[NewTask]) -> Result<Vec<String>> {
        let mut recs = Vec::with_capacity(tasks.len());
        for task in tasks {
            recs.push(self.task_record(task)?);
        }
        let ids: Vec<String> = recs
            .iter()
            .filter_map(|r| match r {
                Record::Task { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect();
        for (task, id) in tasks.iter().zip(ids.iter()) {
            if let Some(seed) = &task.seed {
                copy_seed(Path::new(seed), &paths::work(&self.home, id))?;
            }
        }
        self.commit(&recs, true)?;
        self.admit()?;
        Ok(ids)
    }

    fn task_record(&self, task: &NewTask) -> Result<Record> {
        let worker = self
            .policy
            .cfg
            .workers
            .get(&task.worker)
            .ok_or_else(|| err(format!("unknown worker {}", task.worker)))?;
        if task.goal.trim().is_empty() {
            return Err(err("empty goal"));
        }
        let mut tags = task.tags.clone();
        for tag in &worker.tags {
            if !tags.iter().any(|t| t == tag) {
                tags.push(tag.clone());
            }
        }
        tags.sort();
        tags.dedup();
        let defaults = &self.policy.cfg.default_budget;
        let budget = Budget {
            tokens: task.tokens.unwrap_or(defaults.tokens),
            seconds: task.seconds.unwrap_or(defaults.seconds),
            memory_mb: task.memory_mb.unwrap_or(defaults.memory_mb),
            pids: task.pids.unwrap_or(defaults.pids),
        };
        if let Some(parent) = &task.parent {
            if !self.state.tasks.contains_key(parent) {
                return Err(err("parent is not a task"));
            }
        }
        Ok(Record::Task {
            id: id::ulid(),
            parent: task.parent.clone(),
            worker: task.worker.clone(),
            tags,
            goal: task.goal.clone(),
            verifier: task.verifier.clone(),
            value: task.value.unwrap_or(self.policy.cfg.value),
            budget,
            retry_of: None,
            recipe: task.recipe.clone(),
            ts: now_ms(),
        })
    }

    fn post(&mut self, text: &str, human: bool) -> Result<String> {
        let text = text.trim();
        if text.is_empty() {
            return Err(err("empty post"));
        }
        let (author, role, weight) = if human {
            ("you", "human", self.policy.cfg.human_weight)
        } else {
            ("operator", "operator", 1)
        };
        let id = id::ulid();
        self.commit(
            &[Record::Post {
                id: id.clone(),
                author: author.into(),
                role: role.into(),
                text: text.into(),
                weight,
                channel: "general".into(),
                mentions: mentions(text),
                ts: now_ms(),
            }],
            false,
        )?;
        Ok(id)
    }

    fn status_json(&self) -> Value {
        let purse = &self.state.purse;
        let tasks: Vec<Value> = self
            .state
            .tasks
            .values()
            .map(|t| {
                json!({
                    "id": t.id,
                    "state": t.state.as_str(),
                    "worker": t.worker,
                    "tags": t.tags,
                    "goal": t.goal,
                    "reason": t.reason,
                    "pid": t.pid,
                    "parent": t.parent,
                    "spent": t.budget.tokens,
                    "budget_tokens": t.budget.tokens,
                    "verifier": t.verifier.is_some(),
                    "retry_of": t.retry_of,
                })
            })
            .collect();
        json!({
            "ok": true,
            "live": self.state.live(),
            "queued": self.state.queue.len(),
            "spent": purse.spent(),
            "held": purse.held(),
            "available": purse.available,
            "cap": purse.cap,
            "debug": self.policy.cfg.debug,
            "resets": purse.resets,
            "cell": self.cell_ok,
            "gate": self.policy.cfg.decision.kind.as_str(),
            "gate_spent": self.state.decision_spent,
            "gate_cap": self.policy.cfg.decision.purse_tokens,
            "constraints": self.state.constraints.iter().map(|c| json!({
                "id": c.id,
                "text": c.text,
                "tags": c.tags,
            })).collect::<Vec<_>>(),
            "lease": self.holder,
            "fence": self.fence,
            "posts_seen": self.state.posts_seen,
            "snap_offset": self.snap_offset,
            "tasks": tasks,
            "posts": self.state.posts.iter().filter(|post| board::operator_sees(&self.state, post)).map(post_json).collect::<Vec<_>>(),
            "moderation": self.state.moderation.iter().map(|item| json!({
                "id": item.id,
                "target": item.target,
                "action": item.action,
                "channel": item.channel,
                "weight": item.weight,
                "ts": item.ts,
            })).collect::<Vec<_>>(),
        })
    }

    fn digest_channel(&mut self, channel: &str) -> Result<Value> {
        let channel = if channel.is_empty() {
            "general"
        } else {
            channel
        };
        let stat = self.state.channels.get(channel);
        let posts = stat.map(|s| s.posts).unwrap_or(0) as usize;
        let mentions = stat.map(|s| s.mentions).unwrap_or(0) as usize;
        let authors = stat.map(|s| s.authors.len()).unwrap_or(0);
        let last_author = stat.map(|s| s.last_author.clone()).unwrap_or_default();
        let last_text = stat.map(|s| s.last_text.clone()).unwrap_or_default();
        let item = board::Digest {
            posts,
            authors,
            mentions,
            last_author,
            last_text,
        };
        let line = board::digest_line(channel, &item);
        let spent = self.state.decision_spent;
        let cap = self.policy.cfg.decision.purse_tokens;
        let want_model = self.policy.cfg.rollup == Rollup::Model
            && board::model_allowed(spent, cap, posts)
            && self.policy.cfg.decision.endpoint.is_some();
        if !want_model {
            return Ok(json!({"ok": true, "text": line, "model": false, "posts": posts}));
        }
        let endpoint = self
            .policy
            .cfg
            .decision
            .endpoint
            .clone()
            .unwrap_or_default();
        let timeout = Duration::from_millis(self.policy.cfg.decision.timeout_ms.max(1));
        match decision::summarize(&endpoint, &line, timeout) {
            Ok((summary, tokens)) => {
                let left = cap.saturating_sub(spent);
                let charge = tokens.min(left).max(1);
                self.commit(
                    &[Record::Gate {
                        tokens: charge,
                        ts: now_ms(),
                    }],
                    true,
                )?;
                Ok(
                    json!({"ok": true, "text": summary, "model": true, "posts": posts, "tokens": charge}),
                )
            }
            Err(_) => Ok(json!({"ok": true, "text": line, "model": false, "posts": posts})),
        }
    }

    fn tag_blocked(&self, task: &crate::state::TaskView) -> bool {
        self.state.constraints.iter().any(|constraint| {
            constraint
                .tags
                .iter()
                .any(|tag| task.tags.iter().any(|have| have == tag))
        })
    }

    fn bind_text(&mut self, text: &str) -> Result<String> {
        let text = text.trim();
        if text.is_empty() {
            return Err(err("empty constraint"));
        }
        let tags = crate::sign::tags_in(text);
        if tags.is_empty() && self.policy.cfg.decision.kind == DecisionKind::Off {
            return Err(err("constraint needs a #tag"));
        }
        let id = id::ulid();
        self.commit(
            &[Record::Bind {
                id: id.clone(),
                text: text.to_string(),
                tags,
                ts: now_ms(),
            }],
            true,
        )?;
        Ok(id)
    }

    fn clear_constraint(&mut self, id: &str, sig_hex: &str) -> Result<()> {
        let public = fs::read(paths::key_pub(&self.home)).map_err(|_| err("no signing key"))?;
        let sig = crate::sign::hex_decode(sig_hex)?;
        let msg = format!("clear\n{id}\n");
        crate::sign::verify(&public, msg.as_bytes(), &sig)?;
        if !self.state.constraints.iter().any(|c| c.id == id) {
            return Err(err("no constraint"));
        }
        self.commit(
            &[Record::Clear {
                id: id.to_string(),
                ts: now_ms(),
            }],
            true,
        )?;
        Ok(())
    }

    fn install_signed(&mut self, sig_hex: &str) -> Result<()> {
        let body = fs::read_to_string(paths::policy_draft(&self.home))
            .map_err(|_| err("no policy.draft.lua"))?;
        let public = fs::read(paths::key_pub(&self.home)).map_err(|_| err("no signing key"))?;
        let sig = crate::sign::hex_decode(sig_hex)?;
        crate::sign::verify(&public, body.as_bytes(), &sig)?;
        let parsed = Policy::parse(&body)?;
        durable_write(&paths::policy_sig(&self.home), &sig)?;
        durable_write(&paths::policy(&self.home), body.as_bytes())?;
        self.raise_caps(&parsed.cfg);
        self.policy = parsed;
        self.commit(&[Record::Sign { ts: now_ms() }], true)?;
        Ok(())
    }

    fn write_draft(&self, text: &str) -> Result<()> {
        if text.len() > 256 * 1024 {
            return Err(err("draft is too large"));
        }
        durable_write(&paths::policy_draft(&self.home), text.as_bytes())
    }

    fn diff_json(&self) -> Value {
        let draft = fs::read_to_string(paths::policy_draft(&self.home)).unwrap_or_default();
        json!({
            "ok": true,
            "loaded": self.policy.source,
            "draft": draft,
        })
    }

    fn raise_caps(&mut self, cfg: &crate::config::Config) {
        snap::align_caps(&mut self.state.purse, cfg);
    }

    fn take_snap(&mut self) -> Result<Value> {
        self.flush_soft()?;
        let offset = self.ledger.len();
        snap::store(&self.home, &snap::checkpoint(&self.state, offset))?;
        let sha = snap::commit(&self.home, offset)?;
        self.snap_offset = offset;
        Ok(json!({"ok": true, "offset": offset, "commit": sha}))
    }

    fn reply(&self, id: u64, value: Value) {
        if let Some(client) = self.clients.get(&id) {
            if let Ok(mut line) = serde_json::to_string(&value) {
                line.push('\n');
                let _ = client.tx.try_send(line);
            }
        }
    }

    fn emit(&self, rec: &Record) {
        if let Record::Post { author, .. } = rec {
            if board::muted(&self.state, author) {
                return;
            }
        }
        let (level, value) = match rec {
            Record::Post {
                author,
                role,
                text,
                weight,
                channel,
                mentions,
                ts,
                id,
            } => (
                0,
                json!({"ev":"post","id":id,"author":author,"role":role,"text":text,"weight":weight,"channel":channel,"mentions":mentions,"ts":ts,"worker": self.state.tasks.get(author).map(|task| task.worker.as_str()).unwrap_or("")}),
            ),
            Record::Exit {
                id,
                code,
                reason,
                ts,
                ..
            } => (
                0,
                json!({"ev":"exit","id":id,"code":code,"reason":reason,"ts":ts}),
            ),
            Record::Deny { id, reason, ts } => {
                (1, json!({"ev":"deny","id":id,"reason":reason,"ts":ts}))
            }
            Record::Admit { id, tokens, ts, .. } => {
                (1, json!({"ev":"admit","id":id,"tokens":tokens,"ts":ts}))
            }
            Record::Spawn { id, pid, ts } => (1, json!({"ev":"spawn","id":id,"pid":pid,"ts":ts})),
            Record::Kill { id, ts } => (1, json!({"ev":"kill","id":id,"ts":ts})),
            Record::Cost { id, tokens, ts } => {
                (1, json!({"ev":"cost","id":id,"tokens":tokens,"ts":ts}))
            }
            Record::Gate { tokens, ts } => (2, json!({"ev":"gate","tokens":tokens,"ts":ts})),
            Record::Reset { grant, ts } => (
                1,
                json!({"ev":"reset","grant":grant,"ts":ts,"available":self.state.purse.available,"held":self.state.purse.held()}),
            ),
            Record::Task { id, worker, ts, .. } => {
                (1, json!({"ev":"task","id":id,"worker":worker,"ts":ts}))
            }
            Record::Result { id, ok, code, ts } => (
                0,
                json!({"ev":"result","id":id,"ok":ok,"code":code,"ts":ts}),
            ),
            Record::Promote { name, by, ts } => {
                (1, json!({"ev":"promote","name":name,"by":by,"ts":ts}))
            }
            Record::Bind { id, text, tags, ts } => (
                0,
                json!({"ev":"bind","id":id,"text":text,"tags":tags,"ts":ts}),
            ),
            Record::Clear { id, ts } => (0, json!({"ev":"clear","id":id,"ts":ts})),
            Record::Sign { ts } => (1, json!({"ev":"sign","ts":ts})),
            Record::Vote {
                id,
                voter,
                role,
                target,
                channel,
                choice,
                weight,
                ts,
            } => (
                0,
                json!({"ev":"vote","id":id,"voter":voter,"role":role,"target":target,"channel":channel,"choice":choice,"weight":weight,"ts":ts}),
            ),
            Record::Moderation {
                id,
                target,
                action,
                channel,
                weight,
                ts,
            } => (
                0,
                json!({"ev":"moderation","id":id,"target":target,"action":action,"channel":channel,"weight":weight,"ts":ts}),
            ),
        };
        self.broadcast(level, value);
    }

    fn broadcast(&self, level: u8, value: Value) {
        let line = match serde_json::to_string(&value) {
            Ok(mut s) => {
                s.push('\n');
                s
            }
            Err(_) => return,
        };
        let event_id = value
            .get("id")
            .or_else(|| value.get("author"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        for client in self.clients.values() {
            if !client.watch || client.debug < level {
                continue;
            }
            if let Some(follow) = &client.follow {
                if event_id != follow.as_str() && !value.to_string().contains(follow) {
                    continue;
                }
            }
            let _ = client.tx.try_send(line.clone());
        }
    }

    fn max_debug(&self) -> u8 {
        self.clients
            .values()
            .filter(|c| c.watch)
            .map(|c| c.debug)
            .max()
            .unwrap_or(self.policy.cfg.debug)
    }

    fn shutdown(&mut self) -> Result<()> {
        self.shutting_down = true;
        for check in &self.checks {
            unsafe {
                libc::kill(check.pid, libc::SIGKILL);
            }
        }
        let ids: Vec<String> = self.workers.iter().map(|w| w.id.clone()).collect();
        for id in &ids {
            self.pending.insert(id.clone(), "killed".into());
            self.signal(id, libc::SIGTERM);
        }
        thread::sleep(Duration::from_millis(50));
        for id in &ids {
            self.signal(id, libc::SIGKILL);
        }
        for _ in 0..50 {
            self.reap()?;
            if self.workers.is_empty() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        self.flush_soft()?;
        let _ = fs::remove_file(paths::pid_file(&self.home));
        Ok(())
    }
}

fn load_signed(home: &Path) -> Result<Policy> {
    let path = paths::policy(home);
    let text = fs::read_to_string(&path)
        .map_err(|_| err(format!("no policy at {} (inlet init)", path.display())))?;
    if paths::key_pub(home).exists() {
        let public = fs::read(paths::key_pub(home))?;
        let sig = fs::read(paths::policy_sig(home)).map_err(|_| err("policy is not signed"))?;
        crate::sign::verify(&public, text.as_bytes(), &sig)?;
    }
    Policy::parse(&text)
}

fn durable_write(path: &Path, bytes: &[u8]) -> Result<()> {
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
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn post_json(post: &PostView) -> Value {
    json!({
        "id": post.id,
        "author": post.author,
        "role": post.role,
        "text": post.text,
        "weight": post.weight,
        "channel": post.channel,
        "mentions": post.mentions,
        "ts": post.ts,
    })
}

fn header_of(task: &crate::state::TaskView) -> Header {
    Header {
        worker: task.worker.clone(),
        tags: task.tags.clone(),
        goal: clip(&crate::text::redact(&task.goal), 400),
        value: task.value,
        tokens: task.budget.tokens,
        seconds: task.budget.seconds,
        memory_mb: task.budget.memory_mb,
        pids: task.budget.pids,
        verifier: task.verifier.as_ref().is_some_and(|v| !v.is_empty()),
    }
}

fn clip(text: &str, n: usize) -> String {
    text.chars().take(n).collect()
}

fn copy_seed(src: &Path, dst: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(src).map_err(|_| err("seed is missing"))?;
    if !meta.file_type().is_dir() {
        return Err(err("seed is not a directory"));
    }
    fs::create_dir_all(dst)?;
    copy_seed_dir(src, dst)
}

fn copy_seed_dir(src: &Path, dst: &Path) -> Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if kind.is_dir() {
            fs::create_dir_all(&to)?;
            copy_seed_dir(&entry.path(), &to)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}

fn depth_of(state: &State, start: Option<&str>) -> u32 {
    let mut depth = 0u32;
    let mut parent = start.map(str::to_string);
    while let Some(id) = parent {
        depth += 1;
        if depth > 64 {
            break;
        }
        parent = state.tasks.get(&id).and_then(|t| t.parent.clone());
    }
    depth
}
