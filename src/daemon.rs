use std::collections::{BTreeMap, HashMap, HashSet};
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
use crate::config::{AdmitCtx, DecisionKind, Policy, Rollup};
use crate::decision::{self, Answer, Call, ConstraintIn, Header, PostIn};
use crate::error::{err, Result};
use crate::gate::{self, Verdict};
use crate::id::{self, now_ms};
use crate::isolate;
use crate::ledger::{self, Ledger};
use crate::model::{Budget, Decoded, Plan, Record, TaskState, Template};
use crate::node;
use crate::paths;
use crate::prompt;
use crate::proto::{self, NewTask, Request, SwarmSpec};
use crate::proxy::{self, Hub, Note};
use crate::registry;
use crate::replica;
use crate::snap;
use crate::state::{PostView, State};
use crate::text::mentions;

const SOFT_FLUSH: Duration = Duration::from_millis(50);
const TRANSCRIPT_CAP: u64 = 16 * 1024 * 1024;
const VERIFY_LOG_CAP: u64 = 256 * 1024;
const GC_EVERY: Duration = Duration::from_secs(60);
/// Past this the log is rotated at the next checkpoint. `INLET_ROTATE_BYTES`
/// overrides it, for tests.
const ROTATE_BYTES: u64 = 256 * 1024 * 1024;
const DISK_EVERY: Duration = Duration::from_secs(5);
/// Below this much free space nothing new is admitted. The ledger must be
/// able to record how the running tasks end.
const DISK_FLOOR: u64 = 256 * 1024 * 1024;
/// A checkpoint is taken once the log has grown this much past the last one.
const CHECKPOINT_BYTES: u64 = 32 * 1024 * 1024;
/// Tasks one swarm may stamp per pass. The rest come on the next one.
const STAMP_BATCH: u64 = 64;

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
    Signal(i32),
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
    /// Last output or model call. A worker past its `stall` is stopped.
    last_io: Instant,
    /// Runs under a keeper, which writes the transcript and the wait status.
    kept: bool,
    /// Taken back after a restart. Not this daemon's child, so no waitpid.
    adopted: bool,
    /// Start time from /proc, so a reused pid is not mistaken for it.
    start: u64,
}

struct Check {
    id: String,
    pid: i32,
    started: Instant,
    stdout: File,
    log: Option<File>,
    log_len: u64,
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
    cell_bin: PathBuf,
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
    /// Swarm generations already reported stalled.
    told: HashSet<(String, u64)>,
    last_gc: Instant,
    /// Free space under the home, checked every few seconds.
    disk: (Instant, bool),
    node_tx: Sender<node::Msg>,
    node_rx: Receiver<node::Msg>,
    /// Each node: when it last answered, and whether it is taking work.
    nodes: BTreeMap<String, (Instant, bool)>,
    /// Placed tasks whose add is on its way to the node.
    adding: HashSet<String>,
    /// What each placed task is called on its node.
    remote_ids: HashMap<String, Vec<String>>,
    /// How many times a node has turned a placed task away.
    refused: HashMap<String, u32>,
    /// On a node: when each leader last asked how its tasks were doing.
    leaders: HashMap<String, Instant>,
    /// When the daemon came up, which is as far back as any lease can count.
    born: Instant,
    last_pump: Instant,
    /// Remote tasks this leader has asked a node to stop, and when.
    stale_asked: HashMap<String, Instant>,
    /// This daemon's name as a leader.
    me: String,
    /// Which leader of this home it is, by count. A higher number on disk,
    /// or in a node's mouth, means another has taken over.
    epoch: u64,
    /// The other replicas of the ledger, when there are any.
    links: Option<replica::Links>,
    /// This replica's name, and the set it last gave its links.
    replica: Option<String>,
    replicas: Vec<(String, Vec<String>)>,
    policy_seen: Option<std::time::SystemTime>,
    /// Set when this daemon may not go on: replaced, or without most of its
    /// replicas. An error on the way to a client is not enough to stop it.
    fatal: Option<String>,
    epoch_seen: Instant,
    /// On a node: the newest epoch heard from each leader.
    epochs: HashMap<String, u64>,
}

pub fn serve(home: &Path, foreground: bool, standby: bool, replica: Option<String>) -> Result<()> {
    fs::create_dir_all(home.join("run"))?;
    fs::create_dir_all(home.join("proxy"))?;
    fs::create_dir_all(home.join("work"))?;
    fs::create_dir_all(home.join("runs"))?;
    if !foreground {
        return daemonize(home);
    }
    run(home, standby, replica)
}

fn daemonize(home: &Path) -> Result<()> {
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(err("fork"));
    }
    if pid > 0 {
        // Up means this daemon wrote its own pid, which it does once it has
        // replayed the ledger and taken its workers back. A socket file alone
        // may be the last daemon's.
        let sock = paths::operator_sock(home);
        let want = pid.to_string();
        for _ in 0..1500 {
            let mine = fs::read_to_string(paths::pid_file(home)).is_ok_and(|p| p.trim() == want);
            if mine && sock.exists() {
                return Ok(());
            }
            let mut status = 0;
            if unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) } == pid {
                let log = fs::read_to_string(paths::daemon_log(home)).unwrap_or_default();
                let why = log.lines().last().unwrap_or("see the daemon log");
                return Err(err(format!("daemon did not start: {why}")));
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
    match run(home, false, None) {
        Ok(()) => unsafe { libc::_exit(0) },
        Err(e) => {
            eprintln!("inlet: {e}");
            unsafe { libc::_exit(1) }
        }
    }
}

/// `cell.ro` may not open the home, or shadow a path the cell builds itself.
fn guard_ro(home: &Path, policy: &mut Policy) {
    let home = fs::canonicalize(home).unwrap_or_else(|_| home.to_path_buf());
    policy.cfg.cell_ro.retain(|path| {
        let real = fs::canonicalize(path).unwrap_or_else(|_| path.clone());
        // The cell puts its own tree at each of these. A bind under one would
        // be hidden, and the worker would be told a path that is not there.
        let shadowed = [
            "/etc",
            "/run",
            "/proc",
            "/dev",
            "/tmp",
            "/work",
            "/scratch",
            "/registry",
        ]
        .iter()
        .any(|dir| real.starts_with(dir));
        let ok = real != Path::new("/")
            && !shadowed
            && !home.starts_with(&real)
            && !real.starts_with(&home);
        if !ok {
            eprintln!("inlet: cell.ro {} refused", path.display());
        }
        ok
    });
}

fn run(home: &Path, standby: bool, replica: Option<String>) -> Result<()> {
    // One of several homes that each keep the ledger. Nothing is led, and
    // nothing here is read, until the others have voted this one in.
    let mut peers = Vec::new();
    let mut elected = None;
    if let Some(me) = &replica {
        let read = || peers_of(home, me);
        let has_log = fs::metadata(paths::ledger(home)).is_ok_and(|m| m.len() > 0);
        if !read()?.is_empty() && has_log && replica::terms(home).is_empty() {
            // The others would start empty, elect one of themselves, and
            // this home's ledger would be cut to match theirs.
            return Err(err(format!(
                "this home has a ledger from before it was a replica. Start it alone first, with replicas = {{ {me} = ... }} and nothing else, then add the others one at a time"
            )));
        }
        // Work this home started when it last led belongs to no leader now.
        // Whoever leads next counts those tasks as crashed and runs them
        // again; left alone they would be done twice.
        stop_leftover(home);
        elected = Some(replica::stand_by(home, me, &read)?);
        peers = read()?;
    }
    // The lock first, and nothing read until it is held. A standby waits
    // here for the leader's process to go, which is when the kernel lets the
    // lock go. Only then are the policy, the index, and the log looked at,
    // so what is replayed is everything the leader wrote and nothing it was
    // still writing.
    let log_path = paths::ledger(home);
    let mut waiting = false;
    let file = loop {
        match ledger::try_lock(&log_path, !standby || !waiting && !log_path.exists())? {
            Some(file) => {
                if waiting {
                    eprintln!("inlet: the leader is gone, taking over");
                }
                break file;
            }
            // A link from the last leader may be mid-append on this log.
            None if elected.is_some() => thread::sleep(Duration::from_millis(50)),
            None if standby => {
                if !waiting {
                    eprintln!("inlet: standing by");
                }
                waiting = true;
                thread::sleep(Duration::from_millis(250));
            }
            None => return Err(err("another inlet is holding the ledger")),
        }
    };
    let epoch = match elected {
        Some(term) => term,
        None => snap::bump_epoch(home)?,
    };
    let mut policy = load_signed(home)?;
    guard_ro(home, &mut policy);
    let node = snap::node_id(home)?;
    let lease = snap::ensure_lease(home, &node)?;
    let mut index = snap::load(home)?;
    let log_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if let Some(saved) = &index {
        if log_len == 0 && saved.offset > 0 {
            // A rotation that got as far as the new log and not as far as
            // the new index. The index covers everything; the log is new.
            let mut adopted = saved.clone();
            adopted.offset = 0;
            snap::store(home, &adopted)?;
            index = Some(adopted);
        } else if log_len < saved.offset {
            return Err(err("snapshot is ahead of the log"));
        }
    }
    let snap_offset = index.as_ref().map(|saved| saved.offset).unwrap_or(0);
    if snap_offset < replica::floor(home) {
        return Err(err(
            "the log before its floor is gone and no snapshot reaches that far; restore snap/index.json",
        ));
    }
    let opened = ledger::open_file(file, snap_offset)?;
    // The set the links are given, not the set some later read of the file
    // finds. If the file has moved on, the first beat sees that it has.
    let replica_names: Vec<(String, Vec<String>)> = policy
        .cfg
        .replicas
        .iter()
        .filter(|(name, _)| Some(*name) == replica.as_ref())
        .map(|(name, cmd)| (name.clone(), cmd.clone()))
        .chain(peers.iter().map(|p| (p.name.clone(), p.cmd.clone())))
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_iter()
        .collect();
    let links = match elected {
        Some(term) => {
            let len = opened.ledger.len();
            let terms = replica::begin_term(home, term, len)?;
            Some(replica::Links::start(home, term, &peers, terms, len))
        }
        None => None,
    };
    let node = snap::node_id(home)?;
    // Every replica answers to the same name, so a node keeps its leases
    // and knows an old leader from a new one across a change of leader.
    let me = match &replica {
        // Not made from the members' names: the set can change, and the
        // name a node knows its leader by must not change with it.
        Some(_) => format!("replicas:{}", policy.cfg.cluster),
        None => node.clone(),
    };
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
    let (node_tx, node_rx) = mpsc::channel();
    install_signals(cmd_tx.clone())?;
    listen_socket(&paths::worker_sock(home), cmd_tx.clone(), true)?;
    // The old path, for anything on the host that still dials it.
    let old = home.join("run/worker.sock");
    let _ = fs::remove_file(&old);
    let _ = std::os::unix::fs::symlink("../door/worker.sock", &old);
    listen_socket(&paths::operator_sock(home), cmd_tx, false)?;
    let cell_ok = cell::probe(home);
    if policy.cfg.isolator == crate::config::Isolator::Cgroup && !isolate::prepare() {
        eprintln!("inlet: no delegated cgroup here, workers get rlimits");
    }
    // The binary the cells run is a copy taken now. An upgrade that replaces
    // the file on disk must not change, or break, a daemon that is running.
    let cell_bin = home
        .join("run/bin")
        .join(format!("inlet-{}", std::process::id()));
    fs::create_dir_all(home.join("run/bin"))?;
    // A link is the same file under a second name: instant, and it stays
    // whole when an upgrade renames a new binary over the first name. Across
    // filesystems there is no linking, so copy.
    let linked = std::env::current_exe()
        .ok()
        .is_some_and(|exe| fs::hard_link(exe, &cell_bin).is_ok());
    if !linked {
        fs::copy("/proc/self/exe", &cell_bin)?;
    }
    fs::set_permissions(&cell_bin, fs::Permissions::from_mode(0o755))?;

    let mut daemon = Daemon {
        home: home.to_path_buf(),
        policy,
        ledger: opened.ledger,
        state,
        cell_ok,
        cell_bin,
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
        told: HashSet::new(),
        last_gc: Instant::now(),
        disk: (Instant::now() - DISK_EVERY, true),
        node_tx,
        node_rx,
        nodes: BTreeMap::new(),
        adding: HashSet::new(),
        remote_ids: HashMap::new(),
        refused: HashMap::new(),
        leaders: HashMap::new(),
        born: Instant::now(),
        last_pump: Instant::now(),
        stale_asked: HashMap::new(),
        me,
        epoch,
        links,
        replicas: replica_names,
        replica: replica.clone(),
        policy_seen: None,
        fatal: None,
        epoch_seen: Instant::now(),
        epochs: HashMap::new(),
    };
    if daemon.links.is_some() {
        // The term's own record. Until most replicas hold it, nothing from
        // an earlier term is known to be kept either.
        daemon.commit(
            &[Record::Term {
                term: epoch,
                ts: now_ms(),
            }],
            true,
        )?;
    }
    daemon.sync_nodes();
    // A swarm that stalled before the restart already said so.
    daemon.told = daemon
        .state
        .swarms
        .values()
        .filter(|swarm| swarm.stalled())
        .map(|swarm| (swarm.name.clone(), swarm.generation))
        .collect();
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
            Ok(Incoming::Signal(sig)) => {
                // SIGTERM is the daemon going away: a restart, an upgrade. The
                // work stays up and the next daemon takes it back. Ctrl-C is a
                // person saying stop.
                if sig == libc::SIGTERM
                    && daemon.policy.cfg.adopt
                    && daemon.workers.iter().all(|w| w.kept)
                {
                    daemon.detach()?;
                } else {
                    daemon.shutdown()?;
                }
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        if let Some(msg) = daemon.fatal.take() {
            return Err(err(msg));
        }
        daemon.pump()?;
        if let Some(msg) = daemon.fatal.take() {
            return Err(err(msg));
        }
    }
    daemon.flush_soft()?;
    Ok(())
}

/// The other replicas, as the policy on disk has them now.
fn peers_of(home: &Path, me: &str) -> Result<Vec<replica::Peer>> {
    let cfg = load_signed(home)?.cfg;
    if !cfg.replicas.contains_key(me) {
        return Err(err(format!("the policy's replicas do not name {me}")));
    }
    Ok(cfg
        .replicas
        .iter()
        .filter(|(name, _)| *name != me)
        .map(|(name, cmd)| replica::Peer {
            name: name.clone(),
            cmd: cmd.clone(),
        })
        .collect())
}

fn names(replicas: &[(String, Vec<String>)]) -> String {
    let names: Vec<&str> = replicas.iter().map(|(name, _)| name.as_str()).collect();
    names.join(", ")
}

fn stop_leftover(home: &Path) {
    let Ok(notes) = fs::read_dir(home.join("run/live")) else {
        return;
    };
    let mut stopped = 0;
    for note in notes.flatten() {
        let v: Option<Value> = fs::read(note.path())
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        if let Some(v) = v {
            let pid = v["pid"].as_i64().unwrap_or(0) as i32;
            if pid > 1 && proc_start(pid) == v["start"].as_u64() {
                unsafe {
                    libc::kill(-pid, libc::SIGKILL);
                    libc::kill(pid, libc::SIGKILL);
                }
                stopped += 1;
            }
        }
        let _ = fs::remove_file(note.path());
    }
    if stopped > 0 {
        eprintln!("inlet: stopped {stopped} workers left from when this replica led");
    }
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
        // Restart what the signal interrupted. A spawn caught mid-handshake by
        // SIGTERM once took a read error for a dead child and waited on a live one.
        action.sa_flags = libc::SA_RESTART;
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
                for sig in &buf[..n as usize] {
                    let _ = tx.send(Incoming::Signal(i32::from(*sig)));
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
        let (kept, finished, recheck) = self.adopt();
        let extra = self.state.recover(&self.policy.cfg, now, &kept);
        if !extra.is_empty() {
            self.commit(&extra, true)?;
        }
        for (id, status) in finished {
            self.worker_exited(&id, status)?;
        }
        for id in recheck {
            self.start_check(&id)?;
        }
        self.sweep();
        // Now, not a minute from now: a daemon that is restarted often would
        // otherwise never get round to forgetting anything.
        self.gc()?;
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

    /// Remove what a dead daemon left for tasks that are no longer live: cell
    /// roots, scratch, notes. A task that crashed with the daemon never went
    /// through the path that cleans up after it.
    fn sweep(&self) {
        let live = |name: &str| {
            let id = name.trim_end_matches("-check");
            self.state.tasks.get(id).is_some_and(|t| t.state.is_live())
        };
        // An older daemon's copy of the binary may still be the path a taken-back
        // worker was given. With none taken back, nothing needs it.
        if self.workers.is_empty() {
            if let Ok(dir) = fs::read_dir(self.home.join("run/bin")) {
                for entry in dir.flatten() {
                    if entry.path() != self.cell_bin {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        for id in isolate::groups() {
            if !self.state.tasks.get(&id).is_some_and(|t| t.state.is_live()) {
                isolate::release(&id);
            }
        }
        for sub in ["run/cells", "run/checks", "run/live", "run/exit", "scratch"] {
            let Ok(dir) = fs::read_dir(self.home.join(sub)) else {
                continue;
            };
            for entry in dir.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name == "probe" || live(&name) {
                    continue;
                }
                let path = entry.path();
                if path.is_dir() {
                    let _ = fs::remove_dir_all(&path);
                } else {
                    let _ = fs::remove_file(&path);
                }
            }
        }
    }

    /// Find the workers that outlived the last daemon. Returns the tasks that
    /// are not crashes: still running, finished while nobody watched (with the
    /// wait status), or stopped mid-verifier (to be checked again).
    #[allow(clippy::type_complexity)]
    fn adopt(&mut self) -> (HashSet<String>, Vec<(String, i32)>, Vec<String>) {
        // A task on a node has no process here. It is not a crash for that.
        let mut kept: HashSet<String> = self.state.placed.keys().cloned().collect();
        let mut finished = Vec::new();
        let mut recheck = Vec::new();
        // Ids sort by creation, so a parent is seen before its children.
        let live: Vec<crate::state::TaskView> = self
            .state
            .tasks
            .values()
            .filter(|t| t.state.is_live())
            .cloned()
            .collect();
        for task in &live {
            let id = &task.id;
            let note: Option<Value> = fs::read(paths::live_file(&self.home, id))
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok());
            let Some(note) = note else { continue };
            let pid = note["pid"].as_i64().unwrap_or(0) as i32;
            let start = note["start"].as_u64().unwrap_or(0);
            let running = pid > 1 && proc_start(pid) == Some(start);
            // A child whose parent died with the daemon has nobody to answer
            // to. One whose parent had already settled runs on, as it would
            // have with no restart.
            let orphan = task
                .parent
                .as_ref()
                .is_some_and(|p| live.iter().any(|t| &t.id == p) && !kept.contains(p));
            if orphan || (running && !self.policy.cfg.adopt) {
                if running {
                    unsafe {
                        libc::kill(-pid, libc::SIGKILL);
                        libc::kill(pid, libc::SIGKILL);
                    }
                }
                continue;
            }
            if note["verify"].as_bool() == Some(true) {
                kept.insert(id.clone());
                recheck.push(id.clone());
            } else if running {
                let token = note["token"].as_str().unwrap_or("");
                let Ok(log) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(paths::transcript(&self.home, id))
                else {
                    continue;
                };
                let Ok(stdout) = File::open("/dev/null") else {
                    continue;
                };
                self.hub.insert(id, token, task.budget.tokens);
                self.workers.push(Worker {
                    id: id.clone(),
                    pid,
                    stdout,
                    log_len: log.metadata().map(|m| m.len()).unwrap_or(0),
                    log,
                    last_io: Instant::now(),
                    kept: true,
                    adopted: true,
                    start,
                });
                kept.insert(id.clone());
                eprintln!("inlet: took back {id} (pid {pid})");
            } else if let Some(status) = read_exit(&self.home, id) {
                kept.insert(id.clone());
                finished.push((id.clone(), status));
            }
        }
        // Rebuild each meter from the ledger. A child that is still to settle
        // has its slice lent out of its parent. One that died with the daemon
        // has spent it.
        for task in &live {
            if !self.workers.iter().any(|w| w.id == task.id) {
                continue;
            }
            // Lent is what children still to settle hold. Everything else
            // the slice has used is spent: its own model calls, what settled
            // children used, and the whole slice of any child that died.
            let lent: u64 = live
                .iter()
                .filter(|c| c.parent.as_deref() == Some(task.id.as_str()))
                .filter(|c| kept.contains(&c.id) && self.state.purse.open.contains_key(&c.id))
                .map(|c| c.budget.tokens)
                .sum();
            let used = self
                .state
                .purse
                .open
                .get(&task.id)
                .map(|slice| slice.used)
                .unwrap_or(0);
            self.hub.lend(&task.id, lent);
            self.hub.charge(&task.id, used.saturating_sub(lent));
            // A kill asked for before the crash still stands.
            if self.state.killing.contains(&task.id) {
                self.pending.insert(task.id.clone(), "killed".into());
                self.signal(&task.id, libc::SIGKILL);
            }
        }
        (kept, finished, recheck)
    }

    fn deposed(&mut self) -> crate::error::Error {
        // Nothing more is written, not even what was waiting to be.
        self.soft.clear();
        let msg = "another leader has taken this home; stopping without writing";
        self.fatal = Some(msg.into());
        err(msg)
    }

    /// Still the leader, by the count on disk. Looked at before anything is
    /// made durable: a lock can be lost on storage that only pretends to
    /// lock, and two daemons appending to one log is the end of the log.
    fn still_leader(&mut self) -> Result<()> {
        self.epoch_seen = Instant::now();
        if snap::read_epoch(&self.home) > self.epoch
            || self.links.as_ref().is_some_and(|l| l.deposed())
        {
            return Err(self.deposed());
        }
        if let Some(links) = &self.links {
            if links.alone_for() >= replica::QUORUM_WAIT {
                let msg = "most replicas are out of reach; stopping rather than lead alone";
                self.soft.clear();
                self.fatal = Some(msg.into());
                return Err(err(msg));
            }
            // This home's own standby patience, and a vote asked of it.
            replica::touch(&self.home);
            self.sync_replicas();
        }
        Ok(())
    }

    /// The set of replicas is the policy file's, and may be changed while
    /// this leads: one replica added or one taken out at a time, on every
    /// replica's policy. Any majority of the old set and any of the new then
    /// share a member, which is what keeps two leaders from both being right.
    fn sync_replicas(&mut self) {
        let seen = fs::metadata(paths::policy(&self.home))
            .and_then(|m| m.modified())
            .ok();
        if seen == self.policy_seen {
            return;
        }
        let (Some(me), Some(links)) = (self.replica.clone(), &self.links) else {
            return;
        };
        // A file caught half written, or ahead of its signature, is read
        // again on the next beat. Only one that loaded is done with.
        let Ok(cfg) = load_signed(&self.home).map(|p| p.cfg) else {
            return;
        };
        self.policy_seen = seen;
        let now: Vec<(String, Vec<String>)> = cfg
            .replicas
            .iter()
            .map(|(name, cmd)| (name.clone(), cmd.clone()))
            .collect();
        if now == self.replicas {
            return;
        }
        // A name reached a new way is another home: one out and one in.
        let changed = now.iter().filter(|n| !self.replicas.contains(n)).count()
            + self.replicas.iter().filter(|n| !now.contains(n)).count();
        if changed > 1 {
            // The standbys read the same file and will count majorities of
            // the new set. Leading on with the old one would be two sets,
            // each with a majority of its own.
            self.fatal = Some(format!(
                "replicas change one at a time, and the policy changes more than one of {}; stopping",
                names(&self.replicas)
            ));
            return;
        }
        if !now.iter().any(|(name, _)| *name == me) {
            self.fatal = Some("this replica has been taken out of the policy".into());
            return;
        }
        let peers: Vec<replica::Peer> = cfg
            .replicas
            .iter()
            .filter(|(name, _)| **name != me)
            .map(|(name, cmd)| replica::Peer {
                name: name.clone(),
                cmd: cmd.clone(),
            })
            .collect();
        links.set_peers(&peers);
        eprintln!("inlet: replicas are now {}", names(&now));
        self.replicas = now;
    }

    /// With replicas, written is not kept until most of them have it.
    fn ship(&mut self) -> Result<()> {
        let Some(links) = &self.links else {
            return Ok(());
        };
        if let Err(e) = links.commit(self.ledger.len()) {
            self.soft.clear();
            self.fatal = Some(e.to_string());
            return Err(e);
        }
        // The wait may have been long, and this home may have voted for
        // another in the meantime. Then its own copy does not count, and
        // what looked like most replicas was not. Under the same lock a
        // vote is given under: one that began before this write is waited
        // for, and one that begins after sees the write.
        let _guard = replica::guard(&self.home)?;
        if snap::read_epoch(&self.home) > self.epoch {
            return Err(self.deposed());
        }
        Ok(())
    }

    fn commit(&mut self, recs: &[Record], hard: bool) -> Result<()> {
        if hard {
            self.still_leader()?;
            self.flush_soft()?;
            self.ledger.append_all(recs, true)?;
            self.ship()?;
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
        // Buffered records are writes too.
        self.still_leader()?;
        let batch = std::mem::take(&mut self.soft);
        self.ledger.append_all(&batch, true)?;
        self.ship()?;
        self.last_flush = Instant::now();
        Ok(())
    }

    fn pump(&mut self) -> Result<()> {
        // If this loop itself stood still, on a slow fsync say, a leader's
        // calls were queueing unanswered. That silence is not the leader's.
        if self.epoch_seen.elapsed() >= Duration::from_millis(500) {
            self.still_leader()?;
        }
        let gap = self.last_pump.elapsed();
        if gap >= Duration::from_millis(500) {
            self.born += gap;
            for heard in self.leaders.values_mut() {
                *heard += gap;
            }
        }
        self.last_pump = Instant::now();
        while let Ok(note) = self.notes.try_recv() {
            self.on_note(note)?;
        }
        while let Ok(msg) = self.gate_rx.try_recv() {
            self.on_gate(msg)?;
        }
        while let Ok(msg) = self.node_rx.try_recv() {
            self.on_node(msg)?;
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
            self.reconcile()?;
            self.admit()?;
            let retain = Duration::from_millis(self.policy.cfg.retain_ms.max(1000));
            if self.last_gc.elapsed() >= GC_EVERY.min(retain) {
                self.gc()?;
                self.sweep();
            }
        }
        if self.last_flush.elapsed() >= SOFT_FLUSH {
            self.flush_soft()?;
        }
        Ok(())
    }

    fn on_note(&mut self, note: Note) -> Result<()> {
        match note {
            Note::Cost { id, tokens } => {
                if let Some(worker) = self.workers.iter_mut().find(|w| w.id == id) {
                    worker.last_io = Instant::now();
                }
                // A kept worker's meter is rebuilt from these after a crash,
                // so they cannot wait for the next soft flush.
                self.commit(
                    &[Record::Cost {
                        id,
                        tokens,
                        ts: now_ms(),
                    }],
                    self.policy.cfg.adopt,
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
                .filter(|c| c.holds(now_ms()))
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
        if queued.is_empty() {
            return Ok(());
        }
        if self.disk.0.elapsed() >= DISK_EVERY {
            let room = free_bytes(&self.home).is_none_or(|free| free >= DISK_FLOOR);
            if !room && self.disk.1 {
                eprintln!("inlet: disk is nearly full, holding the queue");
                self.commit(
                    &[Record::Post {
                        id: id::ulid(),
                        author: "inlet".into(),
                        role: "operator".into(),
                        text: "the disk under the home is nearly full. Nothing new is admitted until there is room.".into(),
                        weight: 1,
                        channel: "general".into(),
                        mentions: Vec::new(),
                        ts: now_ms(),
                    }],
                    false,
                )?;
            }
            self.disk = (Instant::now(), room);
        }
        if !self.disk.1 {
            return Ok(());
        }
        // What runs on a node does not take one of this machine's places.
        let live = self.state.live().saturating_sub(self.state.placed.len());
        let now = now_ms();
        let mut batch: Vec<Record> = Vec::new();
        let mut chosen: Vec<String> = Vec::new();
        let mut placing: Vec<String> = Vec::new();
        let mut node_load: HashMap<String, usize> = HashMap::new();
        for (node, _) in self.state.placed.values() {
            *node_load.entry(node.clone()).or_insert(0) += 1;
        }
        let mut hold_tokens = 0u64;
        let mut hold_mem = 0u64;
        let mut hold_pids = 0u64;
        let mut child_hold: HashMap<String, (u64, u64, u64)> = HashMap::new();
        for id in queued {
            let Some(mut task) = self.state.tasks.get(&id).cloned() else {
                continue;
            };
            if task.state != TaskState::Queued || task.plan.not_before > now {
                continue;
            }
            let on = self
                .policy
                .cfg
                .workers
                .get(&task.worker)
                .and_then(|w| w.on.clone());
            // The node with the most room, among those that answer.
            let node = on.as_ref().and_then(|want| {
                self.policy
                    .cfg
                    .nodes
                    .iter()
                    .filter(|(name, _)| want == "nodes" || want == *name)
                    .filter(|(name, _)| self.nodes.get(*name).is_some_and(|rt| rt.1))
                    .map(|(name, cfg)| {
                        (
                            cfg.slots
                                .saturating_sub(node_load.get(name).copied().unwrap_or(0)),
                            name.clone(),
                        )
                    })
                    .filter(|(room, _)| *room > 0)
                    .max()
                    .map(|(_, name)| name)
            });
            if let Some(want) = &on {
                // A worker pointed at a node the policy does not have would
                // wait for ever. Say so now.
                let known = self
                    .policy
                    .cfg
                    .nodes
                    .keys()
                    .any(|name| want == "nodes" || want == name);
                if !known {
                    self.deny(&id, "placement")?;
                    continue;
                }
                if node.is_none() {
                    // No node has room, or none answers. It waits.
                    continue;
                }
                // Its memory and processes are the node's to count.
                task.budget.memory_mb = 0;
                task.budget.pids = 0;
            } else if live + chosen.len() >= self.policy.cfg.caps.max_live {
                // Full here. Something further back may still go to a node.
                if self.policy.cfg.nodes.is_empty() {
                    break;
                }
                continue;
            }
            if gate::waits_for_purse(&task, &self.state.purse) {
                continue;
            }
            let (p, blocked) = match self.gate_for(&task)? {
                GatePrep::Wait => continue,
                GatePrep::Down => {
                    self.deny(&id, "gate")?;
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
                live: live + chosen.len(),
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
                    live: if on.is_some() { 0 } else { live + chosen.len() },
                    depth,
                    cell_ok: self.cell_ok || on.is_some(),
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
                Verdict::Hold => continue,
                Verdict::Deny(reason) => self.deny(&id, reason)?,
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
                    match node {
                        Some(node) => {
                            // Where it goes is durable before the node is asked.
                            batch.push(Record::Place {
                                id: id.clone(),
                                node: node.clone(),
                                ts: now_ms(),
                            });
                            *node_load.entry(node).or_insert(0) += 1;
                            placing.push(id);
                        }
                        None => chosen.push(id),
                    }
                }
            }
        }
        if batch.is_empty() {
            return Ok(());
        }
        // Durable before exec. One fsync for the whole wave.
        self.commit(&batch, true)?;
        for id in placing {
            self.send_add(&id);
        }
        for id in chosen {
            if let Err(e) = self.spawn_worker(&id) {
                self.finish(&id, 127, "cell", true)?;
                let _ = e;
            }
        }
        Ok(())
    }

    /// Forget what is past `retain`, and keep restart time bounded. Without
    /// this the headers, the workdirs, and the replay all grow for as long as
    /// the daemon lives.
    fn gc(&mut self) -> Result<()> {
        self.last_gc = Instant::now();
        // A task placed on a node the policy no longer names has nobody
        // watching it. It is placed again somewhere that exists.
        let adrift: Vec<String> = self
            .state
            .placed
            .iter()
            .filter(|(_, (node, _))| !self.policy.cfg.nodes.contains_key(node))
            .map(|(id, _)| id.clone())
            .collect();
        for id in adrift {
            self.finish_remote(&id, -1, "node", 0)?;
        }
        let mut pruned = 0;
        if self.policy.cfg.retain_ms > 0 {
            let before = now_ms().saturating_sub(self.policy.cfg.retain_ms);
            let old = self.state.forgettable(before);
            remove_later(
                old.iter()
                    .flat_map(|id| {
                        [
                            paths::work(&self.home, id),
                            paths::transcript(&self.home, id),
                            paths::verify_log(&self.home, id),
                        ]
                    })
                    .collect(),
            );
            pruned = old.len();
            self.state.forget(&old);
        }
        // The index is a shortcut past the log. Take it when it would save a
        // long replay, or when forgetting would otherwise be undone by one.
        let grown = self.ledger.len().saturating_sub(self.snap_offset);
        let rotate_at = std::env::var("INLET_ROTATE_BYTES")
            .ok()
            .and_then(|n| n.parse().ok())
            .unwrap_or(ROTATE_BYTES);
        // Replicas count in offsets from the start of one log.
        let rotate = self.ledger.len() >= rotate_at && self.links.is_none();
        // With replicas the log keeps its offsets and gives up its past.
        let compact = self.links.is_some()
            && self.ledger.len().saturating_sub(replica::floor(&self.home)) >= rotate_at;
        if pruned > 0 || grown >= CHECKPOINT_BYTES || rotate || compact {
            // The index and a rotation are writes a replaced leader must not make.
            self.still_leader()?;
            self.flush_soft()?;
            let offset = self.ledger.len();
            snap::store(&self.home, &snap::checkpoint(&self.state, offset))?;
            self.snap_offset = offset;
            if compact {
                // The snapshot is on disk and stands for everything before
                // it. The floor is written first: a log with a hole in it
                // and nothing saying so would be read as a log.
                replica::set_floor(&self.home, offset)?;
                if let Some(links) = &self.links {
                    links.set_floor(offset);
                }
                self.ledger.punch(offset);
                eprintln!("inlet: let go of the ledger before {offset}");
            }
            if rotate {
                // The checkpoint holds everything the log said. A crash after
                // the rename and before the second index finds an empty log
                // under an index with an offset, which startup reads as this.
                self.ledger.rotate(&paths::ledger(&self.home))?;
                snap::store(&self.home, &snap::checkpoint(&self.state, 0))?;
                self.snap_offset = 0;
                eprintln!("inlet: rotated the ledger at {offset} bytes");
            }
        }
        Ok(())
    }

    /// Start watching any node the policy names that is not watched yet.
    /// Called at startup and when a signed policy is installed.
    fn sync_nodes(&mut self) {
        for (name, cfg) in &self.policy.cfg.nodes {
            if self.nodes.contains_key(name) {
                continue;
            }
            // Unknown until it answers. Nothing is placed on a node not yet heard from.
            self.nodes.insert(name.clone(), (Instant::now(), false));
            node::watch(
                name.clone(),
                cfg.cmd.clone(),
                self.node_tx.clone(),
                self.policy.cfg.node_grace_ms,
                format!("{}@{}", self.me, self.epoch),
            );
        }
    }

    /// Ask the node to run a placed task. The tag makes the ask safe to
    /// repeat: the leader looks for it before asking again.
    fn send_add(&mut self, id: &str) {
        let (Some(task), Some((node, _))) = (self.state.tasks.get(id), self.state.placed.get(id))
        else {
            return;
        };
        let Some(cfg) = self.policy.cfg.nodes.get(node) else {
            return;
        };
        let mut tags: Vec<String> = task
            .tags
            .iter()
            .filter(|t| !t.starts_with("inlet:") && !t.starts_with("epoch:"))
            .cloned()
            .collect();
        tags.push(node::tag(id));
        // Milliseconds, and half of what the leader itself waits.
        tags.push(format!(
            "lease:{}:{}",
            self.policy.cfg.node_grace_ms / 2,
            self.me
        ));
        tags.push(format!("epoch:{}", self.epoch));
        let verifier = task.verifier.clone().filter(|v| !v.is_empty());
        let spec = json!({
            "worker": task.worker,
            "goal": task.goal,
            "no_verify": verifier.is_none(),
            "verify": verifier,
            "tokens": task.budget.tokens,
            "seconds": task.budget.seconds,
            "memory_mb": task.budget.memory_mb,
            "pids": task.budget.pids,
            "value": task.value,
            // The node retries, with the workdir and the verifier's output.
            "retries": task.plan.max_attempts().saturating_sub(task.plan.attempt()),
            "tags": tags,
            "seed": task.plan.seed,
        });
        self.adding.insert(id.to_string());
        node::add(cfg.cmd.clone(), spec, id.to_string(), self.node_tx.clone());
    }

    fn on_node(&mut self, msg: node::Msg) -> Result<()> {
        let (name, tasks) = match msg {
            node::Msg::Added { id, ok, answered } => {
                // Failed or not, the next status says whether it is there.
                self.adding.remove(&id);
                if ok {
                    self.refused.remove(&id);
                    return Ok(());
                }
                // A node that answers its status but will not take the task
                // has a reason that asking again will not change: no such
                // worker there, most likely.
                if !answered {
                    // A link that dropped or hung is not a node saying no.
                    return Ok(());
                }
                let tries = self.refused.entry(id.clone()).or_insert(0);
                *tries += 1;
                let answering = self
                    .state
                    .placed
                    .get(&id)
                    .and_then(|(node, _)| self.nodes.get(node))
                    .is_some_and(|rt| rt.1);
                if *tries >= 5 && answering {
                    eprintln!("inlet: a node will not take {id}");
                    self.refused.remove(&id);
                    self.finish_remote(&id, 1, "placement", 0)?;
                }
                return Ok(());
            }
            node::Msg::Deposed => return Err(self.deposed()),
            node::Msg::Status { node, tasks } => (node, tasks),
        };
        let mine: Vec<(String, u64)> = self
            .state
            .placed
            .iter()
            .filter(|(_, (node, _))| *node == name)
            .map(|(id, (_, ts))| (id.clone(), *ts))
            .collect();
        let Some(tasks) = tasks else {
            let grace = Duration::from_millis(self.policy.cfg.node_grace_ms);
            let Some(rt) = self.nodes.get_mut(&name) else {
                return Ok(());
            };
            if rt.0.elapsed() < grace {
                return Ok(());
            }
            if rt.1 {
                eprintln!("inlet: node {name} is not answering");
            }
            rt.1 = false;
            if self.policy.cfg.node_wait {
                // Nothing new goes there, and what is there stays there. The
                // node stops what it was running when its lease runs out and
                // says so when it is back; only then is it placed again.
                return Ok(());
            }
            // Given up on. Its tasks go to another node. If it comes back it
            // may still finish them: twice done, never lost.
            for (id, _) in mine {
                self.finish_remote(&id, -1, "node", 0)?;
            }
            return Ok(());
        };
        if let Some(rt) = self.nodes.get_mut(&name) {
            if !rt.1 {
                eprintln!("inlet: node {name} is answering");
            }
            *rt = (Instant::now(), true);
        }
        // What it still runs for this leader that this leader has moved on
        // from: placed again elsewhere, or killed. Stop it.
        let stale: Vec<String> = tasks
            .iter()
            .filter(|t| !matches!(t.state.as_str(), "done" | "failed" | "killed"))
            // Only what is known to be this leader's. Another may share the node.
            .filter(|t| self.state.tasks.contains_key(&t.leader))
            .filter(|t| {
                self.state
                    .placed
                    .get(&t.leader)
                    .is_none_or(|(node, _)| *node != name)
            })
            .map(|t| t.id.clone())
            .collect();
        // A node that was cut off and finished the work anyway is believed.
        // If the task placed in its stead is still running, that one is
        // stopped and takes the result: the goal is done once, not twice.
        let finished: Vec<String> = tasks
            .iter()
            .filter(|t| t.state == "done")
            .map(|t| t.leader.clone())
            .collect();
        for orig in finished {
            let Some(heir) = self.heir_of(&orig) else {
                continue;
            };
            let Some((on, _)) = self.state.placed.get(&heir).cloned() else {
                continue;
            };
            eprintln!("inlet: node {name} had finished {orig}; stopping {heir} on {on}");
            if let (Some(cfg), Some(ids)) =
                (self.policy.cfg.nodes.get(&on), self.remote_ids.get(&heir))
            {
                node::kill(cfg.cmd.clone(), ids.clone());
            }
            // Nothing charged here: what the node spent is charged below.
            self.finish_remote(&heir, 0, "ok", 0)?;
        }
        // What a node spent on a task this leader had already given up on and
        // refunded. The spend was real. It comes out of the pool, once.
        let late: Vec<Record> = tasks
            .iter()
            .filter(|t| t.used > 0 && matches!(t.state.as_str(), "done" | "failed" | "killed"))
            .filter(|t| !self.state.late.contains(&t.id))
            .filter(|t| {
                self.state
                    .tasks
                    .get(&t.leader)
                    // Not one already charged when it was given up.
                    .is_some_and(|mine| {
                        mine.state.is_settled() && mine.reason == "node" && mine.used == 0
                    })
            })
            .map(|t| Record::Charge {
                id: t.leader.clone(),
                remote: t.id.clone(),
                tokens: t.used,
                ts: now_ms(),
            })
            .collect();
        if !late.is_empty() {
            self.commit(&late, true)?;
        }
        // Asked once, and again only if it is still there ten seconds on.
        self.stale_asked
            .retain(|_, at| at.elapsed() < Duration::from_secs(60));
        let stale: Vec<String> = stale
            .into_iter()
            .filter(|id| {
                self.stale_asked
                    .get(id)
                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(10))
            })
            .collect();
        if !stale.is_empty() {
            if let Some(cfg) = self.policy.cfg.nodes.get(&name) {
                for id in &stale {
                    self.stale_asked.insert(id.clone(), Instant::now());
                }
                node::kill(cfg.cmd.clone(), stale);
            }
        }
        let now = now_ms();
        for (id, placed_at) in mine {
            let there: Vec<&node::Remote> = tasks.iter().filter(|t| t.leader == id).collect();
            if there.is_empty() {
                // The add was lost, or the leader died before sending it.
                if !self.adding.contains(&id) && now.saturating_sub(placed_at) > 5_000 {
                    self.send_add(&id);
                }
                continue;
            }
            self.remote_ids
                .insert(id.clone(), there.iter().map(|t| t.id.clone()).collect());
            let settled =
                |t: &&node::Remote| matches!(t.state.as_str(), "done" | "failed" | "killed");
            if !there.iter().all(settled) {
                continue;
            }
            let used: u64 = there.iter().map(|t| t.used).sum();
            if there.iter().any(|t| t.state == "done") {
                self.finish_remote(&id, 0, "ok", used)?;
            } else if there.iter().any(|t| t.reason == "fenced") {
                // The node let go of it while this leader was out of reach.
                self.finish_remote(&id, -1, "node", used)?;
            } else {
                // The node's chain is spent. Its last word is the reason.
                let last = there
                    .iter()
                    .max_by(|a, b| a.id.cmp(&b.id))
                    .map(|t| t.reason.clone());
                let reason = last
                    .filter(|r| !r.is_empty())
                    .unwrap_or_else(|| "exit".into());
                self.finish_remote(&id, 1, &reason, used)?;
            }
        }
        Ok(())
    }

    /// The live task standing in for one that was lost with its node.
    fn heir_of(&self, orig: &str) -> Option<String> {
        let mut at = self.state.tasks.get(orig)?;
        for _ in 0..8 {
            if !(at.state.is_settled() && at.reason == "node") {
                break;
            }
            let id = at.id.clone();
            at = self
                .state
                .tasks
                .values()
                .find(|t| t.retry_of.as_deref() == Some(id.as_str()))?;
        }
        (at.id != orig && at.state.is_live()).then(|| at.id.clone())
    }

    /// Settle a task that ran on a node. Only a lost node is tried again
    /// here: every other failure already used the node's retries.
    fn finish_remote(&mut self, id: &str, code: i32, reason: &str, used: u64) -> Result<()> {
        let Some(task) = self.state.tasks.get(id) else {
            return Ok(());
        };
        if !task.state.is_live() {
            return Ok(());
        }
        let reserved = task.budget.tokens;
        let used = used.min(reserved);
        let ts = now_ms();
        let mut recs = Vec::new();
        if used > 0 {
            recs.push(Record::Cost {
                id: id.to_string(),
                tokens: used,
                ts,
            });
        }
        recs.push(Record::Exit {
            id: id.to_string(),
            code,
            reason: reason.into(),
            tokens_used: used,
            refund_tokens: reserved - used,
            ts,
        });
        if reason == "node" {
            recs.extend(self.state.retry_for(id, reason, &self.policy.cfg, ts));
        }
        self.adding.remove(id);
        self.remote_ids.remove(id);
        self.refused.remove(id);
        self.commit(&recs, true)
    }

    /// Stop a placed task: tell the node, and settle it here without waiting.
    fn kill_remote(&mut self, id: &str) -> Result<()> {
        if let Some((node, _)) = self.state.placed.get(id) {
            if let Some(cfg) = self.policy.cfg.nodes.get(node) {
                let remote = self.remote_ids.get(id).cloned().unwrap_or_default();
                node::kill(cfg.cmd.clone(), remote);
            }
        }
        self.finish_remote(id, -1, "killed", 0)
    }

    /// A deny settles the task. A transient one leaves a retry behind it.
    fn deny(&mut self, id: &str, reason: &str) -> Result<()> {
        let ts = now_ms();
        let mut recs = vec![Record::Deny {
            id: id.to_string(),
            reason: reason.into(),
            ts,
        }];
        recs.extend(self.state.retry_for(id, reason, &self.policy.cfg, ts));
        self.commit(&recs, true)
    }

    /// Stamp tasks until every swarm has its replicas in flight.
    fn reconcile(&mut self) -> Result<()> {
        if !self.holder {
            return Ok(());
        }
        let now = now_ms();
        let mut recs = Vec::new();
        let mut seeds = Vec::new();
        let mut stalled = Vec::new();
        for swarm in self.state.swarms.values() {
            if swarm.stalled() {
                stalled.push((swarm.name.clone(), swarm.generation, swarm.streak));
            }
            let want = swarm.wants(now, &self.policy.cfg.retry).min(STAMP_BATCH);
            for n in 0..want {
                let index = swarm.created + 1 + n;
                let template = &swarm.template;
                let id = id::ulid();
                let remote = self
                    .policy
                    .cfg
                    .workers
                    .get(&template.worker)
                    .is_some_and(|w| w.on.is_some());
                if let (Some(seed), false) = (&template.seed, remote) {
                    seeds.push((seed.clone(), id.clone()));
                }
                recs.push(Record::Task {
                    id,
                    parent: None,
                    worker: template.worker.clone(),
                    tags: template.tags.clone(),
                    goal: template.goal.replace("{index}", &index.to_string()),
                    verifier: template.verifier.clone(),
                    value: template.value,
                    budget: template.budget.clone(),
                    retry_of: None,
                    recipe: None,
                    plan: Plan {
                        attempt: 0,
                        max_attempts: template.max_attempts,
                        not_before: 0,
                        owner: Some(swarm.name.clone()),
                        seed: template.seed.clone().filter(|_| remote),
                        moves: 0,
                    },
                    ts: now,
                });
            }
        }
        for (name, generation, streak) in stalled {
            if self.told.insert((name.clone(), generation)) {
                self.commit(
                    &[Record::Post {
                        id: id::ulid(),
                        author: "swarm".into(),
                        role: "operator".into(),
                        text: format!(
                            "swarm {name} stalled after {streak} failures in a row. inlet apply or inlet scale restarts it."
                        ),
                        weight: 1,
                        channel: "general".into(),
                        mentions: Vec::new(),
                        ts: now,
                    }],
                    false,
                )?;
            }
        }
        if recs.is_empty() {
            return Ok(());
        }
        for (seed, id) in seeds {
            // A missing seed is the template's fault, and the verifier will say so.
            let _ = copy_seed(Path::new(&seed), &paths::work(&self.home, &id));
        }
        self.commit(&recs, true)
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
        let work = paths::work(&self.home, id);
        // A retry picks up where the last attempt that ran stopped. A link that
        // was denied before it spawned left no workdir and no output.
        let last = task.retry_of.as_ref().map(|prev| {
            let mut links = self.state.chain(prev);
            links.reverse();
            if !work.exists() {
                if let Some(from) = links
                    .iter()
                    .map(|link| paths::work(&self.home, link))
                    .find(|dir| dir.exists())
                {
                    let _ = fs::rename(&from, &work);
                }
            }
            let ran = links
                .iter()
                .find(|link| paths::transcript(&self.home, link).exists())
                .unwrap_or(prev);
            let reason = self
                .state
                .tasks
                .get(ran)
                .map(|t| t.reason.as_str())
                .unwrap_or("");
            prompt::Last::load(
                ran,
                reason,
                &paths::verify_log(&self.home, ran),
                &paths::transcript(&self.home, ran),
            )
        });
        if let Some(recipe) = &task.recipe {
            let _ = registry::stage_run(&self.home, recipe, &work);
        }
        let depth = depth_of(&self.state, task.parent.as_deref());
        let ctx = prompt::Ctx {
            id,
            worker: &task.worker,
            tags: &task.tags,
            goal: &task.goal,
            verifier: task.verifier.as_deref(),
            tokens: task.budget.tokens,
            seconds: task.budget.seconds,
            memory_mb: task.budget.memory_mb,
            depth,
            max_depth: self.policy.cfg.caps.max_depth,
            attempt: task.plan.attempt(),
            max_attempts: task.plan.max_attempts(),
            parent: task.parent.as_deref(),
            owner: task.plan.owner.as_deref(),
            last,
        };
        let template = worker
            .preamble
            .as_deref()
            .or(self.policy.cfg.preamble.as_deref());
        let preamble = prompt::preamble(&ctx, template);
        let full = prompt::compose(&ctx, template);
        let cmd = prompt::fill(&worker.cmd, &full, &task.goal, id)?;
        let keep = self.policy.cfg.adopt;
        if keep {
            fs::create_dir_all(self.home.join("run/exit"))?;
        }
        let spawned = cell::spawn(&SpawnRequest {
            cmd,
            work,
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
            ro: self.policy.cfg.cell_ro.clone(),
            env: worker.env.clone(),
            files: vec![
                ("goal".into(), task.goal.clone()),
                ("task.json".into(), prompt::task_json(&ctx)),
                ("prompt".into(), full),
            ],
            stdin: worker.stdin.then(|| "prompt".to_string()),
            exe: Some(self.cell_bin.clone()),
            keep: keep.then(|| cell::Keep {
                log: paths::transcript(&self.home, id),
                status: paths::exit_file(&self.home, id),
                cap: TRANSCRIPT_CAP,
                // The daemon times a worker out itself. This is for when it cannot.
                deadline: task.budget.seconds.saturating_add(15),
            }),
        })?;
        let start = proc_start(spawned.pid).unwrap_or(0);
        if keep {
            durable_write(
                &paths::live_file(&self.home, id),
                json!({"pid": spawned.pid, "start": start, "token": token})
                    .to_string()
                    .as_bytes(),
            )?;
        }
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
            last_io: Instant::now(),
            kept: keep,
            adopted: false,
            start,
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
                drain_check(&mut self.checks[pos]);
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
            let Some(pos) = self.workers.iter().position(|w| w.pid == pid && !w.adopted) else {
                continue;
            };
            let id = self.workers[pos].id.clone();
            // The keeper's own exit says little. It wrote down the worker's.
            let status = if self.workers[pos].kept {
                read_exit(&self.home, &id).unwrap_or(status)
            } else {
                status
            };
            self.worker_exited(&id, status)?;
        }
        // An adopted worker is not a child. Watch for its pid to go.
        let gone: Vec<String> = self
            .workers
            .iter()
            .filter(|w| w.adopted && proc_start(w.pid) != Some(w.start))
            .map(|w| w.id.clone())
            .collect();
        for id in gone {
            // No status means its keeper was killed with it.
            let status = read_exit(&self.home, &id).unwrap_or(libc::SIGKILL);
            self.worker_exited(&id, status)?;
        }
        Ok(())
    }

    /// `status` is a raw wait status.
    fn worker_exited(&mut self, id: &str, status: i32) -> Result<()> {
        let id = id.to_string();
        let code = if libc::WIFEXITED(status) {
            libc::WEXITSTATUS(status)
        } else if libc::WIFSIGNALED(status) {
            128 + libc::WTERMSIG(status)
        } else {
            -1
        };
        let signalled = libc::WIFSIGNALED(status);
        // The kernel's verdict, where a cgroup gave it one. A worker stopped
        // for memory failed on its own terms: it keeps what it did not spend,
        // and a retry with the same budget would end the same way.
        let over = fs::read(paths::exit_file(&self.home, &id))
            .is_ok_and(|bytes| bytes.get(4..8) == Some(&1i32.to_ne_bytes()[..]));
        if !self.pending.contains_key(&id) && (over || isolate::oom_killed(&id)) {
            self.pending.insert(id.clone(), "memory".into());
        }
        let reason = self.pending.remove(&id);
        match reason.as_deref() {
            Some("killed") => self.finish(&id, code, "killed", true)?,
            Some("timeout") => self.finish(&id, code, "timeout", true)?,
            // Crash keeps the slice. A purse kill refunds whatever the proxy did not spend.
            Some("purse") => self.finish(&id, code, "purse", true)?,
            Some(other) => self.finish(&id, code, other, other != "crash")?,
            None if signalled => self.finish(&id, -1, "crash", false)?,
            None if code == 0 && self.has_verifier(&id) => self.start_check(&id)?,
            None if code == 0 => self.finish(&id, 0, "ok", true)?,
            None => self.finish(&id, code, "exit", true)?,
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
        if self.policy.cfg.adopt {
            // If the daemon dies here, the next one runs the verifier again.
            let _ = fs::create_dir_all(self.home.join("run/live"));
            let _ = durable_write(&paths::live_file(&self.home, id), br#"{"verify":true}"#);
        }
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
            seconds: self.policy.cfg.verify_seconds,
            token: String::new(),
            task_id: id.to_string(),
            cgroup: None,
            door: false,
            ro: self.policy.cfg.cell_ro.clone(),
            ..Default::default()
        }) {
            Ok(child) => {
                self.checks.push(Check {
                    id: id.to_string(),
                    pid: child.pid,
                    started: Instant::now(),
                    stdout: child.stdout,
                    // Kept so the next attempt can read what failed.
                    log: File::create(paths::verify_log(&self.home, id)).ok(),
                    log_len: 0,
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
        if reason == "killed" && !self.state.killing.contains(id) {
            self.commit(
                &[Record::Kill {
                    id: id.to_string(),
                    ts: now_ms(),
                }],
                false,
            )?;
        }
        let ts = now_ms();
        extra.push(Record::Exit {
            id: id.to_string(),
            code,
            reason: reason.into(),
            tokens_used,
            refund_tokens: refund,
            ts,
        });
        // Same fsync as the exit, so a failure never loses its retry.
        if !(code == 0 && reason == "ok") && (!self.shutting_down || reason == "stopped") {
            extra.extend(self.state.retry_for(id, reason, &self.policy.cfg, ts));
        }
        self.commit(&extra, true)?;
        // Only now, with the exit durable. Removed first, a crash in between
        // would leave a live task with no note: a full-charge crash for work
        // that had finished.
        let _ = fs::remove_file(paths::live_file(&self.home, id));
        let _ = fs::remove_file(paths::exit_file(&self.home, id));
        // The cell was scaffolding. The workdir and the transcripts are the
        // record. A scratch directory can be gigabytes, and this is the main
        // loop, so the removing happens on a thread of its own.
        remove_later(vec![
            paths::cell_root(&self.home, id),
            paths::cell_root(&self.home, &format!("{id}-check")),
            paths::scratch(&self.home, id),
            self.home.join("run/checks").join(id),
        ]);
        isolate::release(id);
        Ok(())
    }

    fn timeouts(&mut self) -> Result<()> {
        let now = now_ms();
        // Only what has a process. A walk over every header each pass is what
        // slows a swarm down as its history grows.
        let due: Vec<String> = self
            .workers
            .iter()
            .filter_map(|w| self.state.tasks.get(&w.id))
            .filter(|t| {
                t.admit_ms
                    .map(|start| now.saturating_sub(start) >= t.budget.seconds.saturating_mul(1000))
                    .unwrap_or(false)
            })
            .map(|t| t.id.clone())
            .collect();
        for id in due {
            self.pending.insert(id.clone(), "timeout".into());
            self.signal(&id, libc::SIGKILL);
        }
        // A leader that has stopped asking has given this node up, or will
        // soon, and will place its tasks somewhere else. Stop them here first,
        // so no goal is worked on twice. The lease in the tag is half the
        // leader's patience: this side always lets go before that side moves.
        let born = self.born;
        let leaders = &self.leaders;
        // `lease:<ms>:<leader>`. Each leader's silence is its own.
        let lapsed = |tag: &str| -> Option<bool> {
            let mut parts = tag.strip_prefix("lease:")?.splitn(2, ':');
            let lease = parts.next()?.parse::<u64>().ok()?;
            let heard = leaders
                .get(parts.next().unwrap_or(""))
                .copied()
                .unwrap_or(born);
            Some(heard.elapsed() >= Duration::from_millis(lease))
        };
        let fenced: Vec<String> = self
            .workers
            .iter()
            .map(|w| &w.id)
            .chain(self.checks.iter().map(|c| &c.id))
            .chain(self.state.queue.iter())
            .filter_map(|id| self.state.tasks.get(id))
            .filter(|t| !t.state.is_settled())
            .filter(|t| t.tags.iter().find_map(|tag| lapsed(tag)) == Some(true))
            .map(|t| t.id.clone())
            .collect();
        for id in fenced {
            if self
                .state
                .tasks
                .get(&id)
                .is_some_and(|t| t.state == TaskState::Queued)
            {
                self.deny(&id, "fenced")?;
            } else if !self.pending.contains_key(&id) {
                self.pending.insert(id.clone(), "fenced".into());
                self.signal(&id, libc::SIGKILL);
                if let Some(check) = self.checks.iter().find(|c| c.id == id) {
                    unsafe { libc::kill(check.pid, libc::SIGKILL) };
                }
            }
        }
        // Silence is not the same as waiting on a child.
        let silent: Vec<String> = self
            .workers
            .iter()
            .filter(|w| !self.pending.contains_key(&w.id))
            .filter(|w| {
                let Some(task) = self.state.tasks.get(&w.id) else {
                    return false;
                };
                let stall = self
                    .policy
                    .cfg
                    .workers
                    .get(&task.worker)
                    .map(|cfg| cfg.stall_seconds)
                    .unwrap_or(0);
                stall > 0
                    && task.state == TaskState::Running
                    && w.last_io.elapsed() >= Duration::from_secs(stall)
            })
            .map(|w| w.id.clone())
            .collect();
        for id in silent {
            self.pending.insert(id.clone(), "stall".into());
            self.signal(&id, libc::SIGKILL);
        }
        let limit = Duration::from_secs(self.policy.cfg.verify_seconds);
        let slow: Vec<i32> = self
            .checks
            .iter()
            .filter(|check| check.started.elapsed() >= limit)
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
            if worker.kept {
                // The keeper writes the transcript. Growth is the sign of life.
                let len = worker.log.metadata().map(|m| m.len()).unwrap_or(0);
                if len != worker.log_len {
                    worker.log_len = len;
                    worker.last_io = Instant::now();
                }
                continue;
            }
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
                    worker.last_io = Instant::now();
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
            drain_check(check);
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
            Request::Status { brief, leader } => {
                if let Some(leader) = leader {
                    // `name@epoch`. An older epoch of a name already heard
                    // is a leader that has been replaced and does not know.
                    let (name, epoch) = match leader.rsplit_once('@') {
                        Some((name, n)) => (name.to_string(), n.parse().unwrap_or(0)),
                        None => (leader, 0),
                    };
                    let newest = self.epochs.entry(name.clone()).or_insert(epoch);
                    if epoch < *newest {
                        let mut v = self.status_json(true);
                        v["deposed"] = json!(true);
                        self.reply(id, v);
                        return Ok(());
                    }
                    *newest = epoch;
                    self.leaders.insert(name, Instant::now());
                }
                self.reply(id, self.status_json(brief))
            }
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
                let state = self.state.tasks.get(&task).map(|t| t.state);
                let done = match state {
                    None => Err(err(format!("no task {task}"))),
                    // Nothing to signal. It leaves the queue and does not retry.
                    Some(TaskState::Queued) => self.commit(
                        &[Record::Deny {
                            id: task.clone(),
                            reason: "killed".into(),
                            ts: now_ms(),
                        }],
                        true,
                    ),
                    Some(s) if s.is_live() && self.state.placed.contains_key(&task) => {
                        self.kill_remote(&task)
                    }
                    Some(s) if s.is_live() => {
                        // Durable before the signal: a daemon that dies in
                        // between must not come back and rerun it as a crash.
                        let noted = self.commit(
                            &[Record::Kill {
                                id: task.clone(),
                                ts: now_ms(),
                            }],
                            true,
                        );
                        self.pending.insert(task.clone(), "killed".into());
                        self.signal(&task, libc::SIGKILL);
                        // A verifier is the only process it has left.
                        if let Some(check) = self.checks.iter().find(|c| c.id == task) {
                            unsafe { libc::kill(check.pid, libc::SIGKILL) };
                        }
                        noted
                    }
                    Some(_) => Ok(()),
                };
                match done {
                    Ok(()) => self.reply(id, json!({"ok": true, "id": task})),
                    Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
                }
            }
            Request::Watch { debug, worker } => {
                if let Some(client) = self.clients.get_mut(&id) {
                    client.debug = debug.min(4);
                    client.follow = worker;
                    client.watch = true;
                }
                self.hub.set_debug(self.max_debug());
                self.reply(id, self.status_json(false));
            }
            Request::Hello { debug } => {
                if let Some(client) = self.clients.get_mut(&id) {
                    client.debug = debug.min(4);
                    client.watch = true;
                }
                self.hub.set_debug(self.max_debug());
                self.reply(id, self.status_json(false));
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
            Request::Apply(spec) => match self.apply_swarm(&spec) {
                Ok(body) => self.reply(id, body),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Scale { name, replicas } => match self.scale_swarm(&name, replicas) {
                Ok(body) => self.reply(id, body),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Delete { name, kill } => match self.delete_swarm(&name, kill) {
                Ok(body) => self.reply(id, body),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Describe(task) => match self.describe(&task) {
                Ok(body) => self.reply(id, body),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Bind { text, ttl_ms } => match self.bind_text(&text, ttl_ms) {
                Ok(cid) => self.reply(id, json!({"ok": true, "id": cid})),
                Err(e) => self.reply(id, json!({"ok": false, "error": e.to_string()})),
            },
            Request::Clear { id: cid, sig } => match self.clear_constraint(&cid, sig.as_deref()) {
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
            "children" => self.worker_children(&value),
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
            retries: value
                .get("retries")
                .and_then(|n| n.as_u64())
                .map(|n| n.min(64) as u32),
        };
        // The same ask again is the same child.
        let mut task = task;
        let key = value
            .get("key")
            .and_then(|k| k.as_str())
            .filter(|k| !k.is_empty() && k.len() <= 64)
            .map(|k| format!("req:{k}"));
        if let Some(key) = &key {
            let parent = task.parent.clone();
            if let Some(first) = self
                .state
                .tasks
                .values()
                .find(|t| t.parent == parent && t.tags.contains(key))
            {
                let denied = (first.state == TaskState::Failed).then(|| first.reason.clone());
                return Ok(json!({"ok": true, "ids": [first.id], "denied": denied}));
            }
            task.tags.push(key.clone());
        }
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

    /// How each child ended, and the last thing it said. This is how a result
    /// travels up: a parent never reads a child's workdir.
    fn worker_children(&self, value: &Value) -> Result<Value> {
        let parent = self.worker_id(value)?;
        let children: Vec<Value> = self
            .state
            .tasks
            .values()
            .filter(|t| t.parent.as_deref() == Some(parent.as_str()))
            // A retried attempt is not the answer. Its successor is.
            .filter(|t| !self.state.retried.contains(&t.id))
            .map(|t| {
                let said = self
                    .state
                    .chain(&t.id)
                    .iter()
                    .rev()
                    .find_map(|id| self.state.posts.iter().rev().find(|p| &p.author == id))
                    .map(|p| p.text.clone());
                json!({
                    "id": t.id,
                    "state": t.state.as_str(),
                    "reason": t.reason,
                    "goal": clip(&t.goal, 200),
                    "attempt": t.plan.attempt(),
                    "last_post": said,
                })
            })
            .collect();
        let settled = children.iter().all(|c| {
            matches!(
                c["state"].as_str(),
                Some("done") | Some("failed") | Some("killed")
            )
        });
        Ok(json!({"ok": true, "settled": settled, "children": children}))
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
        // A leader's hand-off carries the leader's id in a tag, and a leader
        // that is unsure whether an add arrived sends it again. The second
        // one finds the first and is that task, not another.
        if let [task] = tasks {
            let tagged = |prefix: &str| task.tags.iter().find_map(|t| t.strip_prefix(prefix));
            let from = tagged("lease:")
                .and_then(|l| l.split_once(':'))
                .map(|(_, name)| name);
            let epoch = tagged("epoch:").and_then(|n| n.parse::<u64>().ok());
            if let (Some(from), Some(epoch)) = (from, epoch) {
                if self.epochs.get(from).is_some_and(|newest| epoch < *newest) {
                    return Err(err("a newer leader has this home"));
                }
            }
            if let Some(tag) = task.tags.iter().find(|t| t.starts_with("inlet:")) {
                if let Some(first) = self.state.tasks.values().find(|t| t.tags.contains(tag)) {
                    return Ok(vec![first.id.clone()]);
                }
            }
        }
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
            // A seed for a task that runs on a node is a path on the node.
            let here = self
                .policy
                .cfg
                .workers
                .get(&task.worker)
                .is_some_and(|w| w.on.is_none());
            if let (Some(seed), true) = (&task.seed, here) {
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
        let mut defaults = self.policy.cfg.default_budget.clone();
        // A child that names no budget gets half of what its parent has left.
        // The policy default is sized for a root, and would never fit inside one.
        if let Some(slice) = task
            .parent
            .as_ref()
            .and_then(|p| self.state.purse.open.get(p))
        {
            let half = |room: u64| (room / 2).max(1);
            defaults.tokens = defaults
                .tokens
                .min(half(slice.tokens.saturating_sub(slice.used)));
            defaults.memory_mb = defaults
                .memory_mb
                .min(half(slice.memory_mb.saturating_sub(slice.memory_lent)));
            defaults.pids = defaults
                .pids
                .min(half(slice.pids.saturating_sub(slice.pids_lent)));
            let elapsed = task
                .parent
                .as_ref()
                .and_then(|p| self.state.tasks.get(p))
                .and_then(|t| t.admit_ms)
                .map(|start| now_ms().saturating_sub(start) / 1000)
                .unwrap_or(0);
            defaults.seconds = defaults
                .seconds
                .min(slice.seconds.saturating_sub(elapsed).max(1));
        }
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
            plan: Plan {
                attempt: 0,
                max_attempts: 1 + task.retries.unwrap_or(self.policy.cfg.retry.max),
                not_before: 0,
                owner: None,
                seed: task.seed.clone().filter(|_| worker.on.is_some()),
                moves: 0,
            },
            ts: now_ms(),
        })
    }

    fn apply_swarm(&mut self, spec: &SwarmSpec) -> Result<Value> {
        let Record::Task {
            worker,
            tags,
            goal,
            verifier,
            value,
            budget,
            plan,
            ..
        } = self.task_record(&spec.template)?
        else {
            return Err(err("bad template"));
        };
        let completions = spec.completions.unwrap_or(spec.replicas);
        self.commit(
            &[Record::Swarm {
                name: spec.name.clone(),
                replicas: spec.replicas,
                completions,
                backoff_limit: spec.backoff_limit.unwrap_or(6),
                template: Template {
                    worker,
                    goal,
                    verifier,
                    tags,
                    value,
                    budget,
                    max_attempts: plan.max_attempts,
                    seed: spec.template.seed.clone(),
                },
                ts: now_ms(),
            }],
            true,
        )?;
        self.reconcile()?;
        self.admit()?;
        let generation = self
            .state
            .swarms
            .get(&spec.name)
            .map(|s| s.generation)
            .unwrap_or(0);
        Ok(json!({"ok": true, "name": spec.name, "generation": generation}))
    }

    fn scale_swarm(&mut self, name: &str, replicas: u64) -> Result<Value> {
        let swarm = self
            .state
            .swarms
            .get(name)
            .cloned()
            .ok_or_else(|| err(format!("no swarm {name}")))?;
        self.commit(
            &[Record::Swarm {
                name: swarm.name,
                replicas,
                completions: swarm.completions,
                backoff_limit: swarm.backoff_limit,
                template: swarm.template,
                ts: now_ms(),
            }],
            true,
        )?;
        Ok(json!({"ok": true, "name": name, "replicas": replicas}))
    }

    /// Stops the stamping. With `kill`, also stops what it stamped.
    fn delete_swarm(&mut self, name: &str, kill: bool) -> Result<Value> {
        if !self.state.swarms.contains_key(name) {
            return Err(err(format!("no swarm {name}")));
        }
        let ts = now_ms();
        let mut recs = vec![Record::SwarmDelete {
            name: name.to_string(),
            ts,
        }];
        let mut stopped = 0;
        if kill {
            let owned: Vec<(String, TaskState)> = self
                .state
                .tasks
                .values()
                .filter(|t| t.plan.owner.as_deref() == Some(name) && !t.state.is_settled())
                .map(|t| (t.id.clone(), t.state))
                .collect();
            stopped = owned.len();
            for (id, state) in owned {
                if state == TaskState::Queued {
                    recs.push(Record::Deny {
                        id,
                        reason: "deleted".into(),
                        ts,
                    });
                } else if self.state.placed.contains_key(&id) {
                    self.kill_remote(&id)?;
                } else {
                    self.pending.insert(id.clone(), "killed".into());
                    self.signal(&id, libc::SIGKILL);
                }
            }
        }
        self.commit(&recs, true)?;
        Ok(json!({"ok": true, "name": name, "stopped": stopped}))
    }

    /// One task with its attempts and the tail of what it and its verifier printed.
    fn describe(&self, id: &str) -> Result<Value> {
        let task = self
            .state
            .tasks
            .get(id)
            .ok_or_else(|| err(format!("no task {id}")))?;
        let chain: Vec<Value> = self
            .state
            .chain(id)
            .iter()
            .filter_map(|link| self.state.tasks.get(link))
            .map(|t| json!({"id": t.id, "state": t.state.as_str(), "reason": t.reason, "attempt": t.plan.attempt()}))
            .collect();
        let posts: Vec<Value> = self
            .state
            .posts
            .iter()
            .filter(|post| post.author == id)
            .map(post_json)
            .collect();
        Ok(json!({
            "ok": true,
            "task": self.task_json(task),
            "verifier": task.verifier,
            "chain": chain,
            "posts": posts,
            "output": prompt::tail(&paths::transcript(&self.home, id), 4000),
            "verifier_output": prompt::tail(&paths::verify_log(&self.home, id), 4000),
        }))
    }

    fn task_json(&self, t: &crate::state::TaskView) -> Value {
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
            "attempt": t.plan.attempt(),
            "max_attempts": t.plan.max_attempts(),
            "not_before": t.plan.not_before,
            "swarm": t.plan.owner,
            "node": self.state.placed.get(&t.id).map(|(node, _)| node),
            "used": t.used,
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

    fn status_json(&self, brief: bool) -> Value {
        let purse = &self.state.purse;
        let now = now_ms();
        // Every header, every time, is what a poller should not have to pay for.
        let tasks: Vec<Value> = if brief {
            Vec::new()
        } else {
            self.state
                .tasks
                .values()
                .map(|t| self.task_json(t))
                .collect()
        };
        let swarms: Vec<Value> = self
            .state
            .swarms
            .values()
            .map(|swarm| {
                json!({
                    "name": swarm.name,
                    "phase": swarm.phase(),
                    "replicas": swarm.replicas,
                    "completions": swarm.completions,
                    "backoff_limit": swarm.backoff_limit,
                    "active": swarm.active,
                    "done": swarm.done,
                    "failed": swarm.failed,
                    "streak": swarm.streak,
                    "created": swarm.created,
                    "generation": swarm.generation,
                    "next_ms": swarm.next_ms(&self.policy.cfg.retry),
                    "worker": swarm.template.worker,
                    "goal": swarm.template.goal,
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
            "constraints": self.state.constraints.iter().filter(|c| c.holds(now)).map(|c| json!({
                "id": c.id,
                "text": c.text,
                "tags": c.tags,
                "expires": c.expires,
            })).collect::<Vec<_>>(),
            "swarms": swarms,
            "nodes": self.policy.cfg.nodes.iter().map(|(name, cfg)| json!({
                "name": name,
                "ready": self.nodes.get(name).is_some_and(|rt| rt.1),
                "slots": cfg.slots,
                "placed": self.state.placed.values().filter(|(node, _)| node == name).count(),
            })).collect::<Vec<_>>(),
            "unattended": self.policy.cfg.unattended,
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
        let now = now_ms();
        self.state.constraints.iter().any(|constraint| {
            constraint.holds(now)
                && constraint
                    .tags
                    .iter()
                    .any(|tag| task.tags.iter().any(|have| have == tag))
        })
    }

    fn bind_text(&mut self, text: &str, ttl_ms: u64) -> Result<String> {
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
                expires: if ttl_ms > 0 {
                    now_ms().saturating_add(ttl_ms)
                } else {
                    0
                },
                ts: now_ms(),
            }],
            true,
        )?;
        Ok(id)
    }

    /// With a signing key, a clear needs its signature. An install that never
    /// made a key has nobody to ask, so the operator socket is enough.
    fn clear_constraint(&mut self, id: &str, sig_hex: Option<&str>) -> Result<()> {
        if paths::key_pub(&self.home).exists() {
            let public = fs::read(paths::key_pub(&self.home)).map_err(|_| err("no signing key"))?;
            let sig =
                crate::sign::hex_decode(sig_hex.ok_or_else(|| err("clear needs a signature"))?)?;
            let msg = format!("clear\n{id}\n");
            crate::sign::verify(&public, msg.as_bytes(), &sig)?;
        }
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
        let mut parsed = Policy::parse(&body)?;
        guard_ro(&self.home, &mut parsed);
        durable_write(&paths::policy_sig(&self.home), &sig)?;
        durable_write(&paths::policy(&self.home), body.as_bytes())?;
        self.raise_caps(&parsed.cfg);
        self.policy = parsed;
        self.sync_nodes();
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
        // The index and a rotation are writes a replaced leader must not make.
        self.still_leader()?;
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
            Record::Task {
                id,
                worker,
                retry_of,
                plan,
                ts,
                ..
            } => (
                1,
                json!({"ev":"task","id":id,"worker":worker,"retry_of":retry_of,"attempt":plan.attempt(),"swarm":plan.owner,"ts":ts}),
            ),
            Record::Result { id, ok, code, ts } => (
                0,
                json!({"ev":"result","id":id,"ok":ok,"code":code,"ts":ts}),
            ),
            Record::Promote { name, by, ts } => {
                (1, json!({"ev":"promote","name":name,"by":by,"ts":ts}))
            }
            Record::Bind {
                id,
                text,
                tags,
                expires,
                ts,
            } => (
                0,
                json!({"ev":"bind","id":id,"text":text,"tags":tags,"expires":expires,"ts":ts}),
            ),
            Record::Swarm {
                name,
                replicas,
                completions,
                ts,
                ..
            } => (
                1,
                json!({"ev":"swarm","name":name,"replicas":replicas,"completions":completions,"ts":ts}),
            ),
            Record::SwarmDelete { name, ts } => {
                (1, json!({"ev":"swarm_delete","name":name,"ts":ts}))
            }
            Record::Place { id, node, ts } => {
                (1, json!({"ev":"place","id":id,"node":node,"ts":ts}))
            }
            Record::Charge {
                id,
                remote,
                tokens,
                ts,
            } => (
                1,
                json!({"ev":"charge","id":id,"remote":remote,"tokens":tokens,"ts":ts}),
            ),
            Record::Clear { id, ts } => (0, json!({"ev":"clear","id":id,"ts":ts})),
            Record::Sign { ts } => (1, json!({"ev":"sign","ts":ts})),
            Record::Term { term, ts } => (1, json!({"ev":"term","term":term,"ts":ts})),
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

    /// Leave without stopping a worker. Each has a keeper and a note on disk.
    fn detach(&mut self) -> Result<()> {
        self.shutting_down = true;
        // A verifier is cheap to run again, and its note says to.
        for check in std::mem::take(&mut self.checks) {
            unsafe {
                libc::kill(check.pid, libc::SIGKILL);
                libc::waitpid(check.pid, std::ptr::null_mut(), 0);
            }
        }
        self.flush_soft()?;
        let _ = fs::remove_file(paths::pid_file(&self.home));
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        self.shutting_down = true;
        // A task whose verifier was running did its work. It reruns, it does not fail.
        for check in std::mem::take(&mut self.checks) {
            unsafe {
                libc::kill(check.pid, libc::SIGKILL);
                libc::waitpid(check.pid, std::ptr::null_mut(), 0);
            }
            self.finish(&check.id, -1, "stopped", true)?;
        }
        let ids: Vec<String> = self.workers.iter().map(|w| w.id.clone()).collect();
        for id in &ids {
            // Not `killed`: nobody asked for this task to end. It reruns on the next start.
            self.pending
                .entry(id.clone())
                .or_insert_with(|| "stopped".into());
            self.signal(id, libc::SIGTERM);
        }
        // Two seconds to put things down. A harness mid-write gets to finish
        // the write. Then there is no more asking.
        for _ in 0..100 {
            self.reap()?;
            if self.workers.is_empty() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
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

/// Remove files and trees off the main loop.
fn remove_later(paths: Vec<PathBuf>) {
    thread::spawn(move || {
        for path in paths {
            if path.is_dir() {
                let _ = fs::remove_dir_all(&path);
            } else {
                let _ = fs::remove_file(&path);
            }
        }
    });
}

/// Bytes an unprivileged process may still write under `path`.
pub fn free_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut vfs) } != 0 {
        return None;
    }
    Some((vfs.f_bavail as u64).saturating_mul(vfs.f_frsize as u64))
}

/// A process's start time, in clock ticks since boot. `None` once it is gone.
fn proc_start(pid: i32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let mut fields = stat.get(stat.rfind(')')? + 2..)?.split(' ');
    if matches!(fields.next()?, "Z" | "X") {
        return None;
    }
    fields.nth(18)?.parse().ok()
}

/// The wait status a keeper left behind.
fn read_exit(home: &Path, id: &str) -> Option<i32> {
    let bytes = fs::read(paths::exit_file(home, id)).ok()?;
    Some(i32::from_ne_bytes(bytes.get(..4)?.try_into().ok()?))
}

fn drain_check(check: &mut Check) {
    let mut buf = [0u8; 8192];
    loop {
        let n = unsafe { libc::read(check.stdout.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            break;
        }
        let room = VERIFY_LOG_CAP.saturating_sub(check.log_len) as usize;
        let take = (n as usize).min(room);
        if take > 0 {
            if let Some(log) = &mut check.log {
                let _ = log.write_all(&buf[..take]);
            }
            check.log_len += take as u64;
        }
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
