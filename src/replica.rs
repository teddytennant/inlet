//! A ledger kept on more than one machine.
//!
//! Each replica has a home of its own and reaches the others by a command,
//! the way a leader reaches a node. One of them leads. It appends to its own
//! log, sends the same bytes to the rest, and a write counts once most of
//! them have it on disk. The others stand by, and when the leader has been
//! quiet too long they hold a vote. This is Raft with byte offsets where
//! Raft has entries: a term is an epoch, and `ledger/terms` says at which
//! offset each term's bytes begin.
//!
//! What a replica keeps, all under `ledger/`:
//!   epoch   the highest term it has seen
//!   vote    whom it voted for in which term
//!   terms   [[term, offset], ...]
//!   heard   touched whenever a leader is heard from
//!   floor   the offset below which the log has been given back to the disk
//!   log     the ledger
//!
//! The log is never renamed or restarted, so an offset means the same thing
//! on every replica for ever. To keep it from growing without end the leader
//! sends its snapshot, which stands for everything before some offset, and
//! each replica then punches that part of its own log out: same length, same
//! offsets, no disk. A replica too far behind to be sent bytes that no longer
//! exist is sent the snapshot and carries on from there.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde_json::{json, Value};

use crate::error::{err, Result};
use crate::{ledger, node, paths, snap};

/// A leader says something this often, work or no work.
const BEAT: Duration = Duration::from_millis(500);
/// Quiet this long, give or take, and a standby calls a vote.
const QUIET_MS: u64 = 4_000;
/// One call to a peer may take this long.
const CALL: Duration = Duration::from_secs(4);
/// A write waits this long for most replicas before the leader gives up.
pub const QUORUM_WAIT: Duration = Duration::from_secs(15);
const CHUNK: u64 = 1 << 20;

pub type Terms = Vec<(u64, u64)>;

fn terms_path(home: &Path) -> PathBuf {
    home.join("ledger/terms")
}
fn vote_path(home: &Path) -> PathBuf {
    home.join("ledger/vote")
}
fn heard_path(home: &Path) -> PathBuf {
    home.join("ledger/heard")
}

fn floor_path(home: &Path) -> PathBuf {
    home.join("ledger/floor")
}

/// Below this the log reads as zeros and only a snapshot will do.
pub fn floor(home: &Path) -> u64 {
    fs::read_to_string(floor_path(home))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

pub fn set_floor(home: &Path, at: u64) -> Result<()> {
    snap::durable(&floor_path(home), at.to_string().as_bytes())
}

fn snapshot_at(home: &Path) -> Option<u64> {
    let bytes = fs::read(paths::snap_index(home)).ok()?;
    serde_json::from_slice::<Value>(&bytes).ok()?["offset"].as_u64()
}

/// Whether this home could be replayed: its snapshot reaches the floor.
pub fn usable(home: &Path) -> bool {
    let floor = floor(home);
    let len = log_len(home);
    match snapshot_at(home) {
        Some(at) => at >= floor && at <= len,
        None => floor == 0,
    }
}

/// The term of the last byte in the log, and where the log ends. A term
/// noted down whose bytes never arrived does not count.
fn last(home: &Path) -> (u64, u64) {
    let len = log_len(home);
    let term = terms(home)
        .iter()
        .rev()
        .find(|(_, start)| *start < len)
        .map_or(0, |t| t.0);
    (term, len)
}

fn refill_path(home: &Path) -> PathBuf {
    home.join("ledger/refill")
}

/// A home that had nothing when a leader first reached it is being filled.
/// The mark is where that leader's log ended and in which term: everything
/// this home may once have promised and lost is inside that. Until its own
/// log is that long it does not stand, and it votes only for a candidate
/// whose log reaches at least as far as the mark.
fn refill(home: &Path) -> Option<(u64, u64)> {
    let text = fs::read_to_string(refill_path(home)).ok()?;
    let mut parts = text.split_whitespace();
    Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
}

fn filling(home: &Path) -> bool {
    refill(home).is_some_and(|(_, upto)| upto > log_len(home))
}

/// Note, on first contact, how far an empty home has to be filled.
fn mark_refill(home: &Path, req: &Value) -> Result<()> {
    let upto = req["at"].as_u64().unwrap_or(0);
    if upto == 0 || log_len(home) > 0 || refill(home).is_some_and(|(_, old)| old >= upto) {
        return Ok(());
    }
    let term = pairs(&req["terms"])
        .iter()
        .rev()
        .find(|(_, start)| *start < upto)
        .map_or(0, |t| t.0);
    snap::durable(&refill_path(home), format!("{term} {upto}").as_bytes())
}

/// Full, or as full as the leader filling it: the mark has done its work.
fn clear_refill(home: &Path, len: u64, req: &Value) {
    let Some((_, upto)) = refill(home) else {
        return;
    };
    if len >= upto || req["want"].as_u64().is_some_and(|want| len >= want) {
        let _ = fs::remove_file(refill_path(home));
    }
}

fn id_path(home: &Path) -> PathBuf {
    home.join("ledger/cluster")
}

/// Which set of replicas this home belongs to: a number made up by the
/// first leader there ever was and handed on with everything it sends.
pub fn cluster(home: &Path) -> Option<String> {
    let id = fs::read_to_string(id_path(home)).ok()?;
    Some(id.trim().to_string()).filter(|id| !id.is_empty())
}

/// Whether a request comes from another set of replicas altogether: a
/// daemon left running from an older cluster, or a policy pointed at the
/// wrong machine. Such a one is not obeyed, however high its term.
fn foreign(home: &Path, req: &Value) -> bool {
    match (cluster(home), req["id"].as_str()) {
        (Some(mine), Some(theirs)) => mine != theirs,
        // One side has no name yet. That is a replica at its beginning, or
        // one whose first leader died before the name was settled; it is
        // not another cluster.
        _ => false,
    }
}

/// A home takes a cluster's name with its bytes, not before. A name taken
/// at a leader's first word, from a leader that then died having given
/// nobody anything, would belong to a cluster that never was.
fn adopt(home: &Path, req: &Value, len: u64) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    if let (None, Some(id)) = (cluster(home), req["id"].as_str()) {
        snap::durable(&id_path(home), id.as_bytes())?;
    }
    Ok(())
}

pub fn terms(home: &Path) -> Terms {
    fs::read(terms_path(home))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn store_terms(home: &Path, terms: &Terms) -> Result<()> {
    snap::durable(&terms_path(home), &serde_json::to_vec(terms)?)
}

fn voted(home: &Path) -> (u64, String) {
    let text = fs::read_to_string(vote_path(home)).unwrap_or_default();
    let mut parts = text.trim().splitn(2, ' ');
    let term = parts.next().and_then(|n| n.parse().ok()).unwrap_or(0);
    (term, parts.next().unwrap_or("").to_string())
}

fn set_epoch(home: &Path, term: u64) -> Result<()> {
    snap::durable(&paths::epoch(home), term.to_string().as_bytes())
}

/// A leader was heard from, or is here. Standbys time their patience from this.
pub fn touch(home: &Path) {
    let _ = fs::create_dir_all(home.join("ledger"));
    let _ = fs::write(heard_path(home), b"");
}

fn quiet_for(home: &Path) -> Duration {
    fs::metadata(heard_path(home))
        .and_then(|m| m.modified())
        .ok()
        // A time still to come is a vote just given: quiet for no time yet.
        .map(|at| SystemTime::now().duration_since(at).unwrap_or_default())
        .unwrap_or(Duration::from_secs(3600))
}

fn log_len(home: &Path) -> u64 {
    fs::metadata(paths::ledger(home))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// One at a time on this home: two leaders' links, or a link and a vote.
pub fn guard(home: &Path) -> Result<File> {
    fs::create_dir_all(home.join("ledger"))?;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(home.join("ledger/replica.lock"))?;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(err("replica lock"));
    }
    Ok(file)
}

/// How far two logs are the same, told from their term tables alone. One
/// leader writes one sequence of bytes in a term, so two logs that agree on
/// where every term up to some offset began are the same up to there.
pub fn agreed(mine: &Terms, mine_len: u64, theirs: &Terms, theirs_len: u64) -> u64 {
    let mut same = 0;
    for i in 0..mine.len().min(theirs.len()) {
        if mine[i] != theirs[i] {
            break;
        }
        let my_end = mine.get(i + 1).map_or(mine_len, |next| next.1);
        let their_end = theirs.get(i + 1).map_or(theirs_len, |next| next.1);
        same = my_end.min(their_end);
        if my_end != their_end {
            break;
        }
    }
    same.min(mine_len).min(theirs_len)
}

fn pairs(v: &Value) -> Terms {
    v.as_array()
        .map(|list| {
            list.iter()
                .filter_map(|p| Some((p.get(0)?.as_u64()?, p.get(1)?.as_u64()?)))
                .collect()
        })
        .unwrap_or_default()
}

/// The far end of a leader's link: `inlet replica serve`, started over ssh
/// by the leader and fed on stdin. A header line, then that many bytes.
pub fn serve(home: &Path) -> Result<()> {
    let stdin = std::io::stdin();
    let mut input = BufReader::new(stdin.lock());
    let mut out = std::io::stdout();
    loop {
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let Ok(req) = serde_json::from_str::<Value>(&line) else {
            return Err(err("replica: bad header"));
        };
        let mut bytes = vec![0u8; req["n"].as_u64().unwrap_or(0) as usize];
        input.read_exact(&mut bytes)?;
        let reply = if req["op"] == "snap" {
            install(home, &req, &bytes)?
        } else {
            append(home, &req, &bytes)?
        };
        writeln!(out, "{reply}")?;
        out.flush()?;
    }
}

/// Take the leader's snapshot: everything before `at`, in one piece. Then
/// the log before `at` is not needed here, and is given back to the disk.
fn install(home: &Path, req: &Value, bytes: &[u8]) -> Result<Value> {
    let _guard = guard(home)?;
    if foreign(home, req) {
        return Ok(json!({"ok": false, "foreign": true}));
    }
    let term = req["term"].as_u64().unwrap_or(0);
    let current = snap::read_epoch(home);
    if term < current {
        return Ok(json!({"ok": false, "term": current}));
    }
    mark_refill(home, req)?;
    if term > current {
        set_epoch(home, term)?;
    }
    touch(home);
    let path = paths::ledger(home);
    let Some(file) = ledger::try_lock(&path, true)? else {
        return Ok(json!({"ok": false, "busy": true}));
    };
    let at = req["at"].as_u64().unwrap_or(0);
    let theirs = pairs(&req["terms"]);
    let mine = terms(home);
    let mut len = file.metadata()?.len();
    let same = agreed(&mine, len, &theirs, u64::MAX);
    let whole = usable(home);
    if at <= floor(home) && whole {
        // Already have one at least as new.
        return Ok(json!({"ok": true, "len": len.min(same.max(floor(home)))}));
    }
    snap::durable(&paths::snap_index(home), bytes)?;
    if same < at || !whole {
        // Too far behind, or on a branch the leader's history did not take.
        // The snapshot is what was before `at`; the log starts over there.
        // No moment may leave bytes above the floor that are not log. A
        // floor going up is written before the file is stretched over the
        // gap; one coming down, after the file has been cut back to it.
        let old = floor(home);
        if at > old {
            set_floor(home, at)?;
        }
        let known: Terms = theirs
            .into_iter()
            .filter(|(_, start)| *start <= at)
            .collect();
        store_terms(home, &known)?;
        file.set_len(at)?;
        file.sync_all()?;
        len = at;
        if at < old {
            set_floor(home, at)?;
        }
    } else {
        set_floor(home, at)?;
    }
    ledger::punch(&file, at);
    clear_refill(home, len, req);
    adopt(home, req, len)?;
    touch(home);
    Ok(json!({"ok": true, "len": len.min(same.max(at))}))
}

fn append(home: &Path, req: &Value, bytes: &[u8]) -> Result<Value> {
    let _guard = guard(home)?;
    if foreign(home, req) {
        return Ok(json!({"ok": false, "foreign": true}));
    }
    let term = req["term"].as_u64().unwrap_or(0);
    let current = snap::read_epoch(home);
    if term < current {
        return Ok(json!({"ok": false, "term": current}));
    }
    mark_refill(home, req)?;
    if term > current {
        // A daemon here that led an older term reads this and stops.
        set_epoch(home, term)?;
    }
    touch(home);
    let path = paths::ledger(home);
    let Some(mut file) = ledger::try_lock(&path, true)? else {
        // That daemon has not let go of the log yet.
        return Ok(json!({"ok": false, "busy": true}));
    };
    let at = req["at"].as_u64().unwrap_or(0);
    let theirs = pairs(&req["terms"]);
    let mut mine = terms(home);
    let mut len = file.metadata()?.len();
    let same = agreed(&mine, len, &theirs, u64::MAX);
    if same < len {
        // What is past here was written by a leader that never got most of
        // the replicas to take it. It was never promised to anyone.
        file.set_len(same)?;
        file.sync_all()?;
        len = same;
        mine.retain(|(_, start)| *start < same);
        store_terms(home, &mine)?;
        // A snapshot taken while leading may cover what was just cut.
        if snapshot_at(home).is_some_and(|at| at > same) {
            let _ = fs::remove_file(paths::snap_index(home));
        }
    }
    if !usable(home) {
        // Nothing here to build on. Ask for all of it.
        return Ok(json!({"ok": false, "need": 0}));
    }
    if at > len {
        return Ok(json!({"ok": false, "need": len}));
    }
    let skip = (len - at) as usize;
    let more = bytes.len().saturating_sub(skip) as u64;
    // Which terms the bytes belong to is written down before the bytes are.
    // The other way round, a crash in between leaves bytes of a new term
    // filed under an old one, and the next leader would take them for its own.
    let known: Terms = theirs
        .into_iter()
        .filter(|(_, start)| *start <= len + more)
        .collect();
    if known != mine {
        store_terms(home, &known)?;
    }
    if more > 0 {
        file.seek(SeekFrom::End(0))?;
        file.write_all(&bytes[skip..])?;
        file.sync_all()?;
        len += more;
    }
    clear_refill(home, len, req);
    adopt(home, req, len)?;
    // Again, now that the disk has been waited on: a slow fsync is not the
    // leader going quiet.
    touch(home);
    Ok(json!({"ok": true, "len": len}))
}

/// `inlet replica vote`: one request on stdin, one answer on stdout.
pub fn vote(home: &Path) -> Result<()> {
    let mut text = String::new();
    std::io::stdin().read_to_string(&mut text)?;
    let req: Value = serde_json::from_str(&text).map_err(|_| err("replica: bad vote"))?;
    println!("{}", answer_vote(home, &req)?);
    Ok(())
}

fn answer_vote(home: &Path, req: &Value) -> Result<Value> {
    let _guard = guard(home)?;
    if foreign(home, req) {
        // Another cluster's, or one that belongs to none yet: a home that
        // has been taken into a cluster is not led out of it.
        return Ok(json!({"granted": false, "term": 0}));
    }
    if !usable(home) {
        // Half way through taking a snapshot.
        return Ok(json!({"granted": false, "term": 0}));
    }
    let term = req["term"].as_u64().unwrap_or(0);
    let candidate = req["candidate"].as_str().unwrap_or("");
    let dry = req["pre"] == true;
    let current = snap::read_epoch(home);
    let my_last = last(home);
    let their_last = (
        req["last_term"].as_u64().unwrap_or(0),
        req["len"].as_u64().unwrap_or(0),
    );
    let (voted_term, voted_for) = voted(home);
    let free = voted_term < term || voted_for == candidate;
    // A replica that can still hear a leader does not help unseat it.
    let leaderless = quiet_for(home) >= Duration::from_millis(QUIET_MS);
    // A home with no history of its own is new to the set, or has lost
    // its disk. Either way it knows nothing of what was promised, and its
    // vote could elect a replica that is missing some of it. It votes only
    // when there is no history anywhere: the first election there ever is.
    // One a leader has begun to fill knows this much: that leader's log,
    // as far as it went, held everything kept. A candidate at least that
    // far along is missing nothing this home ever promised.
    let fresh = their_last >= my_last
        && match refill(home).filter(|_| filling(home)) {
            Some(mark) => their_last >= mark,
            None => my_last.1 > 0 || their_last == (0, 0),
        };
    if dry {
        // Would it be granted. Nothing is written down for a question.
        let granted = term > current && fresh && leaderless;
        return Ok(json!({"granted": granted, "term": current}));
    }
    let again = voted_term == term && voted_for == candidate;
    let granted = term >= current && fresh && free && (leaderless || again);
    if term > current && leaderless {
        set_epoch(home, term)?;
    }
    if granted {
        snap::durable(&vote_path(home), format!("{term} {candidate}").as_bytes())?;
        // The one chosen has a log to read before it can say a word. Give
        // it that long, on top of the usual patience, before looking again.
        touch(home);
        if let Ok(file) = File::open(heard_path(home)) {
            let _ = file.set_modified(SystemTime::now() + Duration::from_secs(10));
        }
    }
    Ok(json!({"granted": granted, "term": snap::read_epoch(home), "id": cluster(home)}))
}

#[derive(Clone)]
pub struct Peer {
    pub name: String,
    pub cmd: Vec<String>,
}

/// More than half of every replica, this one counted.
fn most(peers: usize) -> usize {
    let all = peers + 1;
    all / 2 + 1
}

fn jitter(span: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    (nanos ^ (std::process::id() as u64).wrapping_mul(2_654_435_761)) % span.max(1)
}

/// Ask every peer, at once, and count the ones that say yes. This replica's
/// own yes is counted by the caller.
fn poll(peers: &[Peer], req: &Value) -> (usize, u64, Option<String>) {
    let (tx, rx) = mpsc::channel();
    for peer in peers {
        let (tx, cmd, body) = (tx.clone(), peer.cmd.clone(), req.to_string());
        thread::spawn(move || {
            let out = node::call(
                &cmd,
                &["replica".into(), "vote".into()],
                Some(body.as_bytes()),
                CALL,
            );
            let v = out.and_then(|out| serde_json::from_str::<Value>(out.trim()).ok());
            let _ = tx.send(v);
        });
    }
    drop(tx);
    let (mut yes, mut newest, mut id) = (0, 0, None);
    for v in rx.iter().flatten() {
        if v["granted"] == true {
            yes += 1;
            // The cluster's name, from one that holds bytes under it.
            id = id.or(v["id"].as_str().map(str::to_string));
        }
        newest = newest.max(v["term"].as_u64().unwrap_or(0));
    }
    (yes, newest, id)
}

/// Wait, as a standby, until this replica is elected. Returns its term.
pub fn stand_by(home: &Path, me: &str, peers: &dyn Fn() -> Result<Vec<Peer>>) -> Result<u64> {
    eprintln!("inlet: replica {me} standing by");
    // Patience counted from now: a replica that just started has heard
    // nobody, and that is not the same as the leader being gone.
    touch(home);
    let mut patience = QUIET_MS + jitter(QUIET_MS);
    loop {
        thread::sleep(Duration::from_millis(200));
        if quiet_for(home) < Duration::from_millis(patience) {
            continue;
        }
        if !usable(home) || filling(home) {
            // A home that could not be replayed cannot lead. A leader will
            // send it what it is missing.
            touch(home);
            continue;
        }
        patience = QUIET_MS + jitter(QUIET_MS);
        // Read again each time: the set of replicas is the policy's, and
        // the policy may have changed since this one sat down.
        let peers = &peers()?;
        let most = most(peers.len());
        let (last_term, len) = last(home);
        let next = snap::read_epoch(home) + 1;
        let id = cluster(home);
        let ask = |term: u64, pre: bool| json!({"term": term, "candidate": me, "last_term": last_term, "len": len, "pre": pre, "id": id});
        // First whether a vote could be won, without disturbing anybody. A
        // replica cut off from the rest asks for ever and changes nothing.
        let (yes, newest, _) = poll(peers, &ask(next, true));
        if yes + 1 < most {
            if newest >= next {
                // They are in a later term than this replica knew of. Asking
                // again for the same one would be refused for ever.
                let _guard = guard(home)?;
                if newest > snap::read_epoch(home) {
                    set_epoch(home, newest)?;
                }
            }
            touch(home);
            continue;
        }
        let term = {
            let _guard = guard(home)?;
            if quiet_for(home) < Duration::from_millis(QUIET_MS) || last(home) != (last_term, len) {
                // Somebody else got there while this one was asking.
                continue;
            }
            let term = snap::read_epoch(home).max(voted(home).0) + 1;
            set_epoch(home, term)?;
            snap::durable(&vote_path(home), format!("{term} {me}").as_bytes())?;
            term
        };
        let (yes, newest, named) = poll(peers, &ask(term, false));
        if newest > term {
            let _guard = guard(home)?;
            if newest > snap::read_epoch(home) {
                set_epoch(home, newest)?;
            }
        }
        if yes + 1 >= most && snap::read_epoch(home) == term {
            if let Some(id) = named {
                // A leader that lost its name, or never had one, leads the
                // cluster its voters are in and does not make another.
                keep_cluster(home, &id)?;
            }
            eprintln!("inlet: replica {me} elected for term {term}");
            touch(home);
            return Ok(term);
        }
        touch(home);
    }
}

/// The new leader's first act: note where its term's bytes begin.
/// Whether this home holds a ledger from before it was a replica.
pub fn unseeded(home: &Path) -> bool {
    terms(home).is_empty() && (log_len(home) > 0 || paths::snap_index(home).exists())
}

/// The cluster's name is this home's own once most replicas have taken it.
/// Written down any sooner, a first leader that died having reached nobody
/// would come back a stranger to the cluster the others then made.
pub fn keep_cluster(home: &Path, id: &str) -> Result<()> {
    let _guard = guard(home)?;
    if cluster(home).is_none() {
        snap::durable(&id_path(home), id.as_bytes())?;
    }
    Ok(())
}

/// Returns the term table, and whether an older ledger was taken in.
pub fn begin_term(home: &Path, term: u64, len: u64) -> Result<(Terms, bool)> {
    let _guard = guard(home)?;
    let seeded = unseeded(home);
    let mut mine = terms(home);
    if seeded {
        // A ledger from before this home was a replica. It is the history,
        // and is filed under a term older than any there will be.
        mine.push((0, 0));
    }
    // The log may have lost a torn tail on the way in.
    mine.retain(|(_, start)| *start <= len);
    if mine.last().is_none_or(|last| last.0 != term) {
        mine.push((term, len));
    }
    store_terms(home, &mine)?;
    Ok((mine, seeded))
}

struct Shared {
    /// When the leader's own loop last came round. Links go quiet when it
    /// has not: a leader that is stuck must not sound like one that is well.
    alive: Instant,
    /// When each peer last answered.
    seen: std::collections::HashMap<String, Instant>,
    /// Followers behind this are sent the snapshot, not bytes.
    floor: u64,
    want: u64,
    /// How much of the log each peer is known to hold. A peer with no
    /// entry here has been taken out of the set, and its link ends.
    acked: std::collections::HashMap<String, (u64, Vec<String>)>,
    deposed: bool,
}

/// The leader's side: one thread and one long-lived command per peer.
pub struct Links {
    /// The cluster's name: this home's, or one made up now if there is none.
    id: String,
    shared: Arc<(Mutex<Shared>, Condvar)>,
    home: PathBuf,
    term: u64,
    terms: Terms,
}

impl Links {
    pub fn start(home: &Path, term: u64, peers: &[Peer], terms: Terms, len: u64) -> Links {
        let shared = Arc::new((
            Mutex::new(Shared {
                floor: floor(home),
                want: len,
                acked: Default::default(),
                seen: Default::default(),
                alive: Instant::now(),
                deposed: false,
            }),
            Condvar::new(),
        ));
        let id = cluster(home).unwrap_or_else(|| {
            // The first leader there ever is names the cluster.
            let mut raw = [0u8; 8];
            let _ = File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut raw));
            raw.iter().map(|b| format!("{b:02x}")).collect()
        });
        let links = Links {
            id,
            shared,
            home: home.to_path_buf(),
            term,
            terms,
        };
        links.set_peers(peers);
        links
    }

    /// The set of replicas, less this one. Called again when the policy
    /// changes it: a new peer gets a link and is brought up to date, and one
    /// taken out stops counting at once.
    pub fn set_peers(&self, peers: &[Peer]) {
        let (lock, wake) = &*self.shared;
        let mut shared = lock.lock().unwrap();
        shared
            .acked
            .retain(|name, (_, cmd)| peers.iter().any(|p| &p.name == name && &p.cmd == cmd));
        for peer in peers {
            if shared.acked.contains_key(&peer.name) {
                continue;
            }
            shared
                .acked
                .insert(peer.name.clone(), (0, peer.cmd.clone()));
            shared.seen.insert(peer.name.clone(), Instant::now());
            let id = self.id.clone();
            let (shared, peer, terms, term) = (
                self.shared.clone(),
                peer.clone(),
                self.terms.clone(),
                self.term,
            );
            let log = paths::ledger(&self.home);
            let index = paths::snap_index(&self.home);
            thread::spawn(move || loop {
                if link(&shared, &peer, term, &terms, &log, &index, &id) {
                    return;
                }
                thread::sleep(Duration::from_millis(300));
            });
        }
        wake.notify_all();
    }

    /// Block until most replicas have the log up to `len`.
    pub fn commit(&self, len: u64) -> Result<()> {
        let (lock, wake) = &*self.shared;
        let mut shared = lock.lock().unwrap();
        shared.want = shared.want.max(len);
        wake.notify_all();
        let started = Instant::now();
        loop {
            if shared.deposed {
                return Err(err(
                    "another leader has taken this home; stopping without writing",
                ));
            }
            let have = 1 + shared.acked.values().filter(|a| a.0 >= len).count();
            if have >= most(shared.acked.len()) {
                return Ok(());
            }
            let left = QUORUM_WAIT.saturating_sub(started.elapsed());
            if left.is_zero() {
                return Err(err(
                    "most replicas are out of reach; stopping rather than promise what cannot be kept",
                ));
            }
            // Waiting here is the loop being busy, not stuck.
            shared.alive = Instant::now();
            shared = wake.wait_timeout(shared, left.min(BEAT)).unwrap().0;
        }
    }

    /// The leader has a snapshot at `at` and has let go of the log before
    /// it. Every follower is sent the snapshot so it can do the same.
    pub fn set_floor(&self, at: u64) {
        let (lock, wake) = &*self.shared;
        let mut shared = lock.lock().unwrap();
        shared.floor = shared.floor.max(at);
        wake.notify_all();
    }

    /// How long since most replicas, this one counted, were last in reach.
    /// A leader with nothing to write would otherwise never find out.
    pub fn alone_for(&self) -> Duration {
        let shared = self.shared.0.lock().unwrap();
        let need = most(shared.acked.len()) - 1;
        if need == 0 {
            return Duration::ZERO;
        }
        let mut ages: Vec<Duration> = shared
            .acked
            .keys()
            .map(|name| {
                shared
                    .seen
                    .get(name)
                    .map_or(Duration::MAX, |at| at.elapsed())
            })
            .collect();
        ages.sort();
        ages[need - 1]
    }

    /// The leader's loop is turning.
    pub fn beat(&self) {
        self.shared.0.lock().unwrap().alive = Instant::now();
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn deposed(&self) -> bool {
        self.shared.0.lock().unwrap().deposed
    }
}

/// One connection's life. True when the leader has been replaced and the
/// link should not come back.
fn link(
    shared: &Arc<(Mutex<Shared>, Condvar)>,
    peer: &Peer,
    term: u64,
    terms: &Terms,
    log: &Path,
    index: &Path,
    id: &str,
) -> bool {
    let Some((program, rest)) = peer.cmd.split_first() else {
        return true;
    };
    let Ok(mut child) = Command::new(program)
        .args(rest)
        .args(["replica", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return false;
    };
    let (Some(mut stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return false;
    };
    // Replies come through a channel so that a peer gone silent can be
    // timed out. A read on the pipe itself would wait for ever.
    let (tx, rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(|l| l.ok()) {
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    let done = |child: &mut Child, deposed: bool| {
        let _ = child.kill();
        let _ = child.try_wait();
        deposed
    };
    let Ok(mut file) = File::open(log) else {
        return done(&mut child, false);
    };
    let (lock, wake) = &**shared;
    // Where the peer's log ends. Unknown at first: an empty append at the
    // leader's own end makes the peer say.
    if let Some(acked) = lock.lock().unwrap().acked.get_mut(&peer.name) {
        // A new connection knows nothing of what the peer holds.
        acked.0 = 0;
    }
    let mut theirs: Option<u64> = None;
    // The newest snapshot this peer is known to hold.
    let mut snapped = 0u64;
    loop {
        let (want, floor) = {
            let shared = lock.lock().unwrap();
            if shared.acked.get(&peer.name).is_none_or(|a| a.1 != peer.cmd) {
                drop(shared);
                return done(&mut child, true);
            }
            if shared.alive.elapsed() > Duration::from_secs(3) {
                // Say nothing. The followers will take the silence for what
                // it is and choose another.
                drop(shared);
                thread::sleep(Duration::from_millis(200));
                continue;
            }
            (shared.want, shared.floor)
        };
        let at = theirs.unwrap_or(want).min(want);
        let (header, bytes) = if theirs.is_some() && snapped < floor {
            // Bytes before the floor are gone from here, and the peer may
            // let go of its own once it has what stands for them.
            let Ok(bytes) = fs::read(index) else {
                return done(&mut child, false);
            };
            let Some(covers) = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v["offset"].as_u64())
                .filter(|covers| *covers >= floor)
            else {
                return done(&mut child, false);
            };
            snapped = covers;
            let header = json!({"op": "snap", "term": term, "at": covers, "n": bytes.len(), "terms": terms, "id": id, "want": want});
            (header, bytes)
        } else {
            let n = (want - at).min(CHUNK);
            let mut bytes = vec![0u8; n as usize];
            if file.seek(SeekFrom::Start(at)).is_err() || file.read_exact(&mut bytes).is_err() {
                return done(&mut child, false);
            }
            if n > 0 && at < lock.lock().unwrap().floor {
                // The leader let go of this part of the log while it was
                // being read. What was read may be the hole.
                snapped = 0;
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            (
                json!({"term": term, "at": at, "n": n, "terms": terms, "id": id, "want": want}),
                bytes,
            )
        };
        if writeln!(stdin, "{header}").is_err()
            || stdin.write_all(&bytes).is_err()
            || stdin.flush().is_err()
        {
            return done(&mut child, false);
        }
        // A snapshot is large and is fsynced more than once at the far end.
        let limit = if header["op"] == "snap" {
            // It answered a moment ago to be sent this at all. The time a
            // big snapshot takes to cross and be synced is not it going away.
            lock.lock()
                .unwrap()
                .seen
                .insert(peer.name.clone(), Instant::now() + Duration::from_secs(600));
            Duration::from_secs(600)
        } else {
            CALL
        };
        let Ok(line) = rx.recv_timeout(limit) else {
            // Whatever allowance it was given for a snapshot, it did not answer.
            let mut shared = lock.lock().unwrap();
            if let Some(at) = shared.seen.get_mut(&peer.name) {
                *at = (*at).min(Instant::now());
            }
            drop(shared);
            return done(&mut child, false);
        };
        if header["op"] == "snap" {
            // Answered: the allowance is over, whatever the answer was.
            let mut shared = lock.lock().unwrap();
            if let Some(at) = shared.seen.get_mut(&peer.name) {
                *at = (*at).min(Instant::now());
            }
        }
        let Ok(reply) = serde_json::from_str::<Value>(&line) else {
            return done(&mut child, false);
        };
        if reply["ok"] == true {
            let len = reply["len"].as_u64().unwrap_or(0);
            theirs = Some(len);
            let mut shared = lock.lock().unwrap();
            shared.seen.insert(peer.name.clone(), Instant::now());
            // More than the leader has is a log this leader has not vouched for.
            let want = shared.want;
            match shared.acked.get_mut(&peer.name) {
                Some(acked) if acked.1 == peer.cmd => {
                    // Down as well as up: a peer that lost its disk no
                    // longer has what it once said it had.
                    if len <= want && len != acked.0 {
                        acked.0 = len;
                        wake.notify_all();
                    }
                }
                // No longer one of the replicas, or reached another way now.
                _ => {
                    drop(shared);
                    return done(&mut child, true);
                }
            }
        } else if let Some(need) = reply["need"].as_u64() {
            theirs = Some(need);
            // Not a spin, if it keeps asking for the same thing.
            thread::sleep(Duration::from_millis(20));
            if need < floor {
                // It has lost what it was sent, or never had it.
                snapped = 0;
            }
            continue;
        } else if reply["foreign"] == true {
            // That home belongs to another set of replicas. It is not this
            // leader's to fill, and nothing it says counts.
            eprintln!(
                "inlet: replica {} belongs to another cluster; leaving it alone",
                peer.name
            );
            thread::sleep(Duration::from_secs(10));
            return done(&mut child, false);
        } else if reply["term"].as_u64().is_some_and(|t| t > term) {
            lock.lock().unwrap().deposed = true;
            wake.notify_all();
            return done(&mut child, true);
        } else {
            // Busy, most likely. Nothing is known to have arrived.
            snapped = 0;
            thread::sleep(Duration::from_millis(200));
            continue;
        }
        // Caught up. Wait for more, and say something anyway before long.
        let shared = lock.lock().unwrap();
        if shared.deposed {
            return done(&mut child, true);
        }
        if theirs == Some(shared.want) && snapped >= shared.floor {
            let _ = wake.wait_timeout(shared, BEAT).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_agree_as_far_as_their_terms_do() {
        // The same history.
        assert_eq!(agreed(&vec![(1, 0)], 50, &vec![(1, 0)], 80), 50);
        // A follower behind in an old term, the leader two terms on.
        assert_eq!(agreed(&vec![(1, 0)], 50, &vec![(1, 0), (3, 80)], 120), 50);
        // A deposed leader with a tail nobody took: term 2 from 80, where
        // the real history went to term 3 at 60.
        assert_eq!(
            agreed(&vec![(1, 0), (2, 80)], 100, &vec![(1, 0), (3, 60)], 90),
            60
        );
        // Same term start, the follower's term-1 run longer than the leader's.
        assert_eq!(agreed(&vec![(1, 0)], 100, &vec![(1, 0), (2, 70)], 90), 70);
        // Nothing in common.
        assert_eq!(agreed(&vec![(2, 0)], 100, &vec![(1, 0)], 90), 0);
        assert_eq!(agreed(&vec![], 0, &vec![(1, 0)], 90), 0);
    }

    fn home(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("inlet-rep-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("ledger")).unwrap();
        dir
    }

    fn send(home: &Path, term: u64, at: u64, terms: &Terms, bytes: &[u8]) -> Value {
        let req = json!({"term": term, "at": at, "n": bytes.len(), "terms": terms});
        append(home, &req, bytes).unwrap()
    }

    #[test]
    fn a_follower_drops_a_tail_nobody_promised() {
        let home = home("cut");
        // Term 1 wrote 60 bytes here. Then this replica led term 2 alone and
        // wrote 40 more that no other replica ever took.
        let t1 = vec![(1, 0)];
        assert_eq!(send(&home, 1, 0, &t1, &[b'a'; 60])["len"], 60);
        fs::OpenOptions::new()
            .append(true)
            .open(paths::ledger(&home))
            .unwrap()
            .write_all(&[b'x'; 40])
            .unwrap();
        store_terms(&home, &vec![(1, 0), (2, 60)]).unwrap();
        set_epoch(&home, 2).unwrap();
        // The leader of term 3 never saw term 2. Its own bytes start at 60.
        let t3 = vec![(1, 0), (3, 60)];
        // It starts by asking at its own end, and is told where to start.
        assert_eq!(send(&home, 3, 90, &t3, b"")["need"], 60);
        assert_eq!(send(&home, 3, 60, &t3, &[b'b'; 30])["len"], 90);
        let log = fs::read(paths::ledger(&home)).unwrap();
        assert_eq!(&log[..60], &[b'a'; 60][..]);
        assert_eq!(&log[60..], &[b'b'; 30][..]);
        assert_eq!(terms(&home), t3);
        // The same bytes again change nothing, and an older leader is told the term.
        assert_eq!(send(&home, 3, 60, &t3, &[b'b'; 30])["len"], 90);
        assert_eq!(send(&home, 2, 60, &vec![(1, 0), (2, 60)], b"zz")["term"], 3);
        assert_eq!(fs::read(paths::ledger(&home)).unwrap().len(), 90);
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn one_vote_a_term_and_none_for_a_shorter_log() {
        let home = home("vote");
        send(&home, 1, 0, &vec![(1, 0)], &[b'a'; 60]);
        let ask = |term: u64, who: &str, last_term: u64, len: u64, pre: bool| {
            let req = json!({"term": term, "candidate": who, "last_term": last_term, "len": len, "pre": pre});
            answer_vote(&home, &req).unwrap()["granted"] == true
        };
        // A leader was just heard from: nobody is helped to unseat it.
        touch(&home);
        assert!(!ask(2, "b", 1, 60, true));
        touch(&home);
        assert!(!ask(2, "b", 1, 60, false));
        // Long quiet now.
        let old = SystemTime::now() - Duration::from_secs(60);
        File::open(heard_path(&home))
            .unwrap()
            .set_modified(old)
            .unwrap();
        // A candidate missing some of the log does not get the vote.
        assert!(!ask(2, "b", 1, 59, false));
        assert!(!ask(2, "b", 0, 500, false));
        File::open(heard_path(&home))
            .unwrap()
            .set_modified(old)
            .unwrap();
        // Refused, but the term it named was heard and is kept.
        assert_eq!(snap::read_epoch(&home), 2);
        // A question changes nothing; the vote does, once.
        assert!(ask(3, "b", 1, 60, true));
        assert_eq!(snap::read_epoch(&home), 2);
        assert!(ask(3, "b", 1, 60, false));
        assert_eq!(snap::read_epoch(&home), 3);
        assert!(!ask(3, "c", 1, 60, false));
        assert!(ask(3, "b", 1, 60, false));
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn a_term_whose_bytes_never_came_does_not_count() {
        let home = home("last");
        send(&home, 1, 0, &vec![(1, 0)], &[b'a'; 60]);
        // What a crash leaves between noting a term and writing its bytes.
        store_terms(&home, &vec![(1, 0), (5, 60)]).unwrap();
        assert_eq!(last(&home), (1, 60));
        // A leader of term 3 that never knew of 5 is not fooled by the note.
        let t3 = vec![(1, 0), (3, 60)];
        assert_eq!(send(&home, 5, 60, &t3, &[b'b'; 10])["len"], 70);
        assert_eq!(last(&home), (3, 70));
        assert_eq!(terms(&home), t3);
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn a_home_with_no_history_votes_only_when_nobody_has_any() {
        // New to the set, or its disk was lost: it cannot know what was
        // promised, so it does not help choose who keeps the promises.
        let home = home("blank");
        let ask = |last_term: u64, len: u64| {
            let req = json!({"term": 7, "candidate": "b", "last_term": last_term, "len": len, "pre": true});
            answer_vote(&home, &req).unwrap()["granted"] == true
        };
        assert!(!ask(3, 500));
        // The first election there ever is: every home is like this one.
        assert!(ask(0, 0));
        // Once a leader has filled it, it votes as any other.
        send(&home, 6, 0, &vec![(6, 0)], &[b'a'; 60]);
        File::open(heard_path(&home))
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(60))
            .unwrap();
        assert!(ask(6, 60));
        assert!(!ask(6, 59));
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn a_leader_from_another_cluster_is_not_obeyed() {
        let home = home("foreign");
        let t = vec![(1, 0)];
        let from = |id: &str, term: u64, at: u64, bytes: &[u8]| {
            let req = json!({"term": term, "at": at, "n": bytes.len(), "terms": t, "id": id});
            append(&home, &req, bytes).unwrap()
        };
        // A home that belonged to nobody takes the first leader's name.
        assert_eq!(from("aaaa", 1, 0, &[b'a'; 40])["len"], 40);
        assert_eq!(cluster(&home).as_deref(), Some("aaaa"));
        // Another cluster's leader, with a far higher term, changes nothing.
        assert_eq!(from("bbbb", 99, 0, &[b'z'; 80])["foreign"], true);
        assert_eq!(snap::read_epoch(&home), 1);
        assert_eq!(fs::read(paths::ledger(&home)).unwrap(), vec![b'a'; 40]);
        let vote = json!({"term": 100, "candidate": "x", "last_term": 99, "len": 999, "pre": false, "id": "bbbb"});
        assert_eq!(answer_vote(&home, &vote).unwrap()["granted"], false);
        assert_eq!(snap::read_epoch(&home), 1);
        // Its own goes on as before.
        assert_eq!(from("aaaa", 1, 40, &[b'a'; 10])["len"], 50);
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn a_home_being_filled_does_not_vote_until_it_is_full() {
        // Its disk was lost. What it once took and acknowledged, it no
        // longer has, and a candidate missing the same would look fresh to it.
        let home = home("refill");
        let t = vec![(4, 0)];
        let vote = |home: &Path| {
            let old = SystemTime::now() - Duration::from_secs(60);
            File::open(heard_path(home))
                .unwrap()
                .set_modified(old)
                .unwrap();
            let req = json!({"term": 9, "candidate": "b", "last_term": 4, "len": 600, "pre": true});
            answer_vote(home, &req).unwrap()["granted"] == true
        };
        // The leader's first word says how much there is.
        assert_eq!(send(&home, 4, 1000, &t, b"")["need"], 0);
        assert!(!vote(&home));
        assert_eq!(send(&home, 4, 0, &t, &[b'a'; 500])["len"], 500);
        assert!(!vote(&home), "half full, and b at 600 looks fresh to it");
        // But one as far along as the leader that began the filling is
        // missing nothing this home ever held. Without that, a first leader
        // that died mid-fill could never be chosen again.
        let old = SystemTime::now() - Duration::from_secs(60);
        File::open(heard_path(&home))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let full = json!({"term": 9, "candidate": "a", "last_term": 4, "len": 1000, "pre": true});
        assert_eq!(answer_vote(&home, &full).unwrap()["granted"], true);
        assert_eq!(send(&home, 4, 500, &t, &[b'a'; 500])["len"], 1000);
        // Full: b, at 600, is plainly behind, and one at 1000 is not.
        assert!(!vote(&home));
        let old = SystemTime::now() - Duration::from_secs(60);
        File::open(heard_path(&home))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let req = json!({"term": 9, "candidate": "c", "last_term": 4, "len": 1000, "pre": true});
        assert_eq!(answer_vote(&home, &req).unwrap()["granted"], true);
        let _ = fs::remove_dir_all(&home);
    }

    #[test]
    fn a_name_is_taken_with_bytes_and_handed_on_with_a_vote() {
        let home = home("name");
        let t = vec![(1, 0)];
        let from = |at: u64, bytes: &[u8]| {
            let req = json!({"term": 1, "at": at, "n": bytes.len(), "terms": t, "id": "aaaa"});
            append(&home, &req, bytes).unwrap()
        };
        // A first word, and nothing after it: that leader may have died
        // having given nobody anything, and its name with it.
        from(50, b"");
        assert_eq!(cluster(&home), None);
        assert_eq!(from(0, &[b'a'; 50])["len"], 50);
        assert_eq!(cluster(&home).as_deref(), Some("aaaa"));
        // The leader itself wrote its name down only after most had its
        // record, and died first. It comes back with the log and no name.
        // It is not a stranger, and the vote tells it the name.
        let old = SystemTime::now() - Duration::from_secs(60);
        File::open(heard_path(&home))
            .unwrap()
            .set_modified(old)
            .unwrap();
        let ask = json!({"term": 2, "candidate": "a", "last_term": 1, "len": 50, "pre": false, "id": null});
        let reply = answer_vote(&home, &ask).unwrap();
        assert_eq!(reply["granted"], true);
        assert_eq!(reply["id"], "aaaa");
        // Having voted, it waits out the new leader's reading of its log.
        assert!(quiet_for(&home) < Duration::from_secs(1));
        let _ = fs::remove_dir_all(&home);
    }
}
