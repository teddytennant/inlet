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

/// Which set of replicas this home belongs to: the policy's `cluster`,
/// written here the first time anything is asked of the home in its name.
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

/// A home that belonged to no cluster belongs to the first that speaks to
/// it. The name is the policy's, the same on every replica of a set, so
/// there is no moment at which two leaders of one set could differ on it.
fn adopt(home: &Path, req: &Value) -> Result<()> {
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

/// A leader has just been chosen and has a log to read before it can say
/// a word. Count it as heard from for that long, on top of the usual.
fn grace(home: &Path) {
    touch(home);
    if let Ok(file) = File::open(heard_path(home)) {
        let _ = file.set_modified(SystemTime::now() + Duration::from_secs(10));
    }
}

fn quiet_for(home: &Path) -> Duration {
    fs::metadata(heard_path(home))
        .and_then(|m| m.modified())
        .ok()
        .map(|at| match at.duration_since(SystemTime::now()) {
            // A time a little way off is a vote just given: quiet for no
            // time yet. One far off is a clock that was set back, and says
            // nothing about when a leader was last heard.
            Ok(ahead) if ahead <= Duration::from_secs(15) => Duration::ZERO,
            Ok(_) => Duration::from_secs(3600),
            Err(behind) => behind.duration(),
        })
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
    adopt(home, req)?;
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
    adopt(home, req)?;
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
        grace(home);
    }
    Ok(json!({"granted": granted, "term": snap::read_epoch(home)}))
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
/// How many granted, and the newest term any of them named.
type Tally = (usize, u64);

fn poll(peers: &[Peer], req: &Value) -> Tally {
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
    let (mut yes, mut newest) = (0, 0);
    for v in rx.iter().flatten() {
        if v["granted"] == true {
            yes += 1;
        }
        newest = newest.max(v["term"].as_u64().unwrap_or(0));
    }
    (yes, newest)
}

/// Wait, as a standby, until this replica is elected. Returns its term.
pub fn stand_by(
    home: &Path,
    me: &str,
    id: &str,
    peers: &dyn Fn() -> Result<Vec<Peer>>,
) -> Result<u64> {
    match cluster(home) {
        Some(mine) if mine != id => {
            return Err(err(format!(
                "this home holds the ledger of cluster {mine:?}, and the policy says {id:?}"
            )));
        }
        Some(_) => {}
        None => snap::durable(&id_path(home), id.as_bytes())?,
    }
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
        let ask = |req: &Value| poll(peers, req);
        if let Some(term) = campaign(home, me, id, most(peers.len()), &ask)? {
            eprintln!("inlet: replica {me} elected for term {term}");
            return Ok(term);
        }
    }
}

/// One try at being elected. `ask` puts a request to every other replica
/// and says how many granted it, the newest term any of them named, and
/// the cluster's name if a granting one had it.
fn campaign(
    home: &Path,
    me: &str,
    id: &str,
    most: usize,
    ask: &dyn Fn(&Value) -> Tally,
) -> Result<Option<u64>> {
    let (last_term, len) = last(home);
    let next = snap::read_epoch(home) + 1;
    let req = |term: u64, pre: bool| json!({"term": term, "candidate": me, "last_term": last_term, "len": len, "pre": pre, "id": id});
    // First whether a vote could be won, without disturbing anybody. A
    // replica cut off from the rest asks for ever and changes nothing.
    let (yes, newest) = ask(&req(next, true));
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
        return Ok(None);
    }
    let term = {
        let _guard = guard(home)?;
        if quiet_for(home) < Duration::from_millis(QUIET_MS) || last(home) != (last_term, len) {
            // Somebody else got there while this one was asking.
            return Ok(None);
        }
        let term = snap::read_epoch(home).max(voted(home).0) + 1;
        set_epoch(home, term)?;
        snap::durable(&vote_path(home), format!("{term} {me}").as_bytes())?;
        term
    };
    let (yes, newest) = ask(&req(term, false));
    if newest > term {
        let _guard = guard(home)?;
        if newest > snap::read_epoch(home) {
            set_epoch(home, newest)?;
        }
    }
    if yes + 1 >= most && snap::read_epoch(home) == term {
        // Its own home votes too, and must give it the same time to
        // read its log that the others do.
        grace(home);
        return Ok(Some(term));
    }
    touch(home);
    Ok(None)
}

/// The new leader's first act: note where its term's bytes begin.
/// Whether this home holds a ledger from before it was a replica.
pub fn unseeded(home: &Path) -> bool {
    terms(home).is_empty() && (log_len(home) > 0 || paths::snap_index(home).exists())
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
    /// The cluster's name, sent with everything.
    id: String,
    shared: Arc<(Mutex<Shared>, Condvar)>,
    home: PathBuf,
    term: u64,
    terms: Terms,
}

impl Links {
    pub fn start(
        home: &Path,
        term: u64,
        peers: &[Peer],
        terms: Terms,
        len: u64,
        id: String,
    ) -> Links {
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

    /// How much of the log each other replica is known to hold, by name.
    pub fn progress(&self) -> Vec<(String, u64)> {
        let shared = self.shared.0.lock().unwrap();
        let mut all: Vec<(String, u64)> = shared
            .acked
            .iter()
            .map(|(name, acked)| (name.clone(), acked.0))
            .collect();
        all.sort();
        all
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

    /// A whole set of replicas in one thread, driven at random: elections,
    /// writes, links that are up or down, daemons that die, and processes
    /// killed between one durable write and the next. Everything a home
    /// does is the real code. What is modelled is the leader's side, which
    /// in the daemon is threads and pipes.
    struct Sim {
        homes: Vec<PathBuf>,
        /// The daemon leading on each home, if one is: its term, its term
        /// table, the log held open and locked, and what each peer has.
        leaders: Vec<Option<Lead>>,
        /// Every byte a client was told was kept.
        kept: Vec<u8>,
        won: std::collections::HashMap<u64, usize>,
        rng: u64,
        stamp: u8,
        /// How many disks may be lost in a run, and how many have been.
        wipes: usize,
        wiped: usize,
        stuck: bool,
        /// Leaders let go of the log behind a snapshot now and then.
        compacts: bool,
        /// Which homes each home's policy names as replicas, and what every
        /// policy is on its way to saying. One home's policy is changed at
        /// a time, and the set by one member at a time.
        views: Vec<Vec<bool>>,
        target: Vec<bool>,
        grows: bool,
        changes: usize,
    }

    struct Lead {
        term: u64,
        terms: Terms,
        id: String,
        log: File,
        theirs: Vec<Option<u64>>,
        acked: Vec<u64>,
        /// The newest snapshot each peer is known to hold.
        snapped: Vec<u64>,
        /// The replicas as this leader last read them from its policy.
        set: Vec<bool>,
    }

    impl Sim {
        fn new(tag: &str, n: usize, seed: u64) -> Sim {
            // With a changing set there are homes in waiting, empty, that
            // no policy names yet.
            let grows = tag.starts_with('m');
            let slots = if grows { 5 } else { n };
            let target: Vec<bool> = (0..slots).map(|i| i < n).collect();
            Sim {
                views: vec![target.clone(); slots],
                target,
                grows,
                changes: 0,
                homes: (0..slots)
                    .map(|i| home(&format!("{tag}-{seed}-{i}")))
                    .collect(),
                leaders: (0..slots).map(|_| None).collect(),
                kept: Vec::new(),
                won: Default::default(),
                rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
                stamp: 0,
                wipes: 0,
                wiped: 0,
                stuck: false,
                compacts: false,
            }
        }

        fn roll(&mut self, n: u64) -> u64 {
            self.rng ^= self.rng << 13;
            self.rng ^= self.rng >> 7;
            self.rng ^= self.rng << 17;
            self.rng % n
        }

        fn len(&self, i: usize) -> u64 {
            log_len(&self.homes[i])
        }

        /// Whether home `i` holds everything kept. Below its floor its log
        /// is a hole and its snapshot stands for it, so only what is above
        /// can be compared.
        fn holds_kept(&self, i: usize) -> bool {
            let mine = fs::read(paths::ledger(&self.homes[i])).unwrap();
            let from = (floor(&self.homes[i]) as usize).min(self.kept.len());
            mine.len() >= self.kept.len() && mine[from..self.kept.len()] == self.kept[from..]
        }

        /// Time passes: nobody has heard a leader for a long while.
        fn quiet(&self, i: usize) {
            touch(&self.homes[i]);
            let old = SystemTime::now() - Duration::from_secs(60);
            let _ = File::open(heard_path(&self.homes[i])).and_then(|f| f.set_modified(old));
        }

        /// Maybe the next thing this thread does is killed part way.
        fn arm(&mut self, odds: u64) {
            let n = if self.roll(odds) == 0 {
                self.roll(4) as i64
            } else {
                -1
            };
            snap::CRASH_IN.with(|c| c.set(n));
        }

        fn disarm(&self) {
            snap::CRASH_IN.with(|c| c.set(-1));
        }

        /// What `sync_replicas` does: the leader reads its policy again.
        fn sync(&mut self, i: usize) {
            let view = self.views[i].clone();
            let Some(lead) = &mut self.leaders[i] else {
                return;
            };
            if lead.set == view {
                return;
            }
            let changed = (0..view.len()).filter(|k| view[*k] != lead.set[*k]).count();
            if changed > 1 || !view[i] {
                self.leaders[i] = None;
                return;
            }
            for (k, member) in view.iter().enumerate() {
                if *member && !lead.set[k] {
                    lead.theirs[k] = None;
                    lead.acked[k] = 0;
                    lead.snapped[k] = 0;
                }
            }
            lead.set = view;
        }

        /// One step of a change to the set: begin one, or carry the one in
        /// hand to one more home's policy.
        fn reshape(&mut self) {
            let slots = self.homes.len();
            if let Some(k) = (0..slots).find(|k| self.views[*k] != self.target) {
                let late: Vec<usize> = (k..slots)
                    .filter(|k| self.views[*k] != self.target)
                    .collect();
                let pick = late[self.roll(late.len() as u64) as usize];
                self.views[pick] = self.target.clone();
                return;
            }
            let members: Vec<usize> = (0..slots).filter(|k| self.target[*k]).collect();
            // The next change waits for the last one to be finished: every
            // policy the same, which was just seen, and every replica full.
            // A set changed again while a new member is still empty can be
            // left with too few homes that hold the ledger.
            let full = members.iter().map(|k| self.len(*k)).max().unwrap_or(0);
            let led = members.iter().any(|k| self.leaders[*k].is_some());
            let behind = members
                .iter()
                .any(|k| self.len(*k) < full || filling(&self.homes[*k]));
            // Not at every chance: a few changes in a run, with long
            // stretches of trouble between them.
            if self.roll(8) != 0 || ((full == 0 || !led || behind) && !self.calm(&members)) {
                return;
            }
            let spare = (0..slots).find(|k| !self.target[*k] && log_len(&self.homes[*k]) == 0);
            match spare {
                Some(new) if members.len() < 5 && self.roll(2) == 0 => {
                    self.target[new] = true;
                    self.changes += 1;
                }
                _ if members.len() > 3 => {
                    let out = members[self.roll(members.len() as u64) as usize];
                    if self.leaders[out].is_none() {
                        self.target[out] = false;
                        self.changes += 1;
                    }
                }
                _ => {}
            }
        }

        /// The operator waits for a quiet moment before changing the set: a
        /// leader up, and every replica holding all of the ledger. Here the
        /// quiet is arranged. What follows it, while the change makes its
        /// way from one policy to the next, is as rough as the rest.
        fn calm(&mut self, members: &[usize]) -> bool {
            let all = vec![true; self.homes.len()];
            for round in 0..25 {
                let leading: Vec<usize> = members
                    .iter()
                    .copied()
                    .filter(|i| self.leaders[*i].is_some())
                    .collect();
                for i in &leading {
                    self.write(*i);
                    for _ in 0..15 {
                        for j in members.iter().copied().filter(|j| j != i) {
                            self.send(*i, j, u64::MAX);
                        }
                    }
                    self.answer(*i);
                }
                if let [i] = leading[..] {
                    let full = self.len(i);
                    let done = self.leaders[i].is_some()
                        && full > 0
                        && self.kept.len() as u64 == full
                        && members
                            .iter()
                            .all(|k| self.len(*k) == full && !filling(&self.homes[*k]));
                    if done {
                        return true;
                    }
                    continue;
                }
                for k in members.iter().copied() {
                    if self.leaders[k].is_none() {
                        self.quiet(k);
                    }
                }
                self.elect(members[round % members.len()], &all, u64::MAX);
            }
            false
        }

        fn elect(&mut self, i: usize, up: &[bool], faults: u64) {
            if self.leaders[i].is_some() || !self.views[i][i] {
                return;
            }
            let home = self.homes[i].clone();
            if !usable(&home)
                || filling(&home)
                || quiet_for(&home) < Duration::from_millis(QUIET_MS)
            {
                return;
            }
            let n = self.homes.len();
            let homes = self.homes.clone();
            let set = self.views[i].clone();
            let peers = set.iter().filter(|m| **m).count() - 1;
            let ask = |req: &Value| -> Tally {
                let (mut yes, mut newest) = (0, 0);
                for (j, peer) in homes.iter().enumerate() {
                    if j == i || !up[j] || !set[j] {
                        continue;
                    }
                    let Ok(v) = answer_vote(peer, req) else {
                        continue;
                    };
                    if v["granted"] == true {
                        yes += 1;
                    }
                    newest = newest.max(v["term"].as_u64().unwrap_or(0));
                }
                (yes, newest)
            };
            self.arm(faults);
            let won = campaign(&home, &format!("r{i}"), "sim", most(peers), &ask);
            let Ok(Some(term)) = won else {
                self.disarm();
                return;
            };
            // Elected. What the daemon does next: lock, note the term, link.
            let started = (|| -> Result<Lead> {
                let log = ledger::try_lock(&paths::ledger(&home), true)?
                    .ok_or_else(|| err("log is held"))?;
                let len = log.metadata()?.len();
                let (terms, _) = begin_term(&home, term, len)?;
                Ok(Lead {
                    term,
                    terms,
                    id: "sim".into(),
                    log,
                    theirs: vec![None; n],
                    acked: vec![0; n],
                    snapped: vec![0; n],
                    set: set.clone(),
                })
            })();
            self.disarm();
            let Ok(lead) = started else {
                return;
            };
            assert!(
                self.won.insert(term, i).is_none_or(|was| was == i),
                "two leaders in term {term}"
            );
            assert!(
                self.holds_kept(i),
                "r{i} was elected for term {term} without {} kept bytes (has {})",
                self.kept.len(),
                self.len(i)
            );
            self.leaders[i] = Some(lead);
            self.write(i);
        }

        /// The leader on `i` appends a record to its own log.
        fn write(&mut self, i: usize) {
            self.sync(i);
            let home = self.homes[i].clone();
            self.stamp = self.stamp.wrapping_add(1);
            let (stamp, n) = (self.stamp, 20 + self.roll(60) as usize);
            let Some(lead) = &mut self.leaders[i] else {
                return;
            };
            if snap::read_epoch(&home) > lead.term {
                self.leaders[i] = None;
                return;
            }
            touch(&home);
            lead.log.seek(SeekFrom::End(0)).unwrap();
            lead.log.write_all(&vec![stamp; n]).unwrap();
            lead.log.sync_all().unwrap();
        }

        /// One exchange on the link from the leader on `i` to home `j`.
        fn send(&mut self, i: usize, j: usize, faults: u64) {
            let (home, peer) = (self.homes[i].clone(), self.homes[j].clone());
            let want = self.len(i);
            let chunk = 1 + self.roll(120);
            self.sync(i);
            if self.leaders[i].as_ref().is_none_or(|lead| !lead.set[j]) {
                return;
            }
            self.arm(faults);
            let Some(lead) = &mut self.leaders[i] else {
                self.disarm();
                return;
            };
            touch(&home);
            let at = lead.theirs[j].unwrap_or(want).min(want);
            let floor = floor(&home);
            let reply = if lead.theirs[j].is_some() && lead.snapped[j] < floor {
                // As the link does: the snapshot before any byte past it.
                let bytes = fs::read(paths::snap_index(&home)).unwrap();
                let covers = serde_json::from_slice::<Value>(&bytes).unwrap()["offset"]
                    .as_u64()
                    .unwrap();
                lead.snapped[j] = covers;
                let header = json!({"op": "snap", "term": lead.term, "at": covers, "n": bytes.len(), "terms": lead.terms, "id": lead.id, "want": want});
                install(&peer, &header, &bytes)
            } else {
                let n = (want - at).min(chunk);
                let mut bytes = vec![0u8; n as usize];
                lead.log.seek(SeekFrom::Start(at)).unwrap();
                lead.log.read_exact(&mut bytes).unwrap();
                assert!(
                    n == 0 || at >= floor,
                    "the link read the log below its floor"
                );
                let header = json!({"term": lead.term, "at": at, "n": n, "terms": lead.terms, "id": lead.id, "want": want});
                append(&peer, &header, &bytes)
            };
            self.disarm();
            let lead = self.leaders[i].as_mut().unwrap();
            let Ok(reply) = reply else {
                // The far end was killed part way. The link starts over.
                lead.theirs[j] = None;
                lead.acked[j] = 0;
                lead.snapped[j] = 0;
                return;
            };
            if reply["ok"] == true {
                let len = reply["len"].as_u64().unwrap();
                lead.theirs[j] = Some(len);
                if len <= want {
                    lead.acked[j] = len;
                }
            } else if let Some(need) = reply["need"].as_u64() {
                lead.theirs[j] = Some(need);
                if need < floor {
                    lead.snapped[j] = 0;
                }
            } else if reply["term"].as_u64().is_some_and(|t| t > lead.term) {
                self.leaders[i] = None;
            }
        }

        /// What `ship` does: if most replicas hold the log, and this home
        /// has not voted for another meanwhile, the client is answered.
        fn answer(&mut self, i: usize) {
            self.sync(i);
            let home = self.homes[i].clone();
            let Some(lead) = &self.leaders[i] else {
                return;
            };
            let len = self.len(i);
            let others: Vec<usize> = (0..lead.set.len())
                .filter(|k| lead.set[*k] && *k != i)
                .collect();
            let have = 1 + others.iter().filter(|k| lead.acked[**k] >= len).count();
            if have < most(others.len()) {
                return;
            }
            let _guard = guard(&home).unwrap();
            if snap::read_epoch(&home) > lead.term {
                self.leaders[i] = None;
                return;
            }
            let term = lead.term;
            assert!(
                self.holds_kept(i),
                "the leader of term {term} answers for a log that lost kept bytes"
            );
            // The true bytes: what is below this leader's floor it no longer
            // has, and they are kept from when it did.
            let mine = fs::read(paths::ledger(&home)).unwrap();
            let from = (floor(&home) as usize).min(self.kept.len());
            self.kept.truncate(from);
            self.kept.extend_from_slice(&mine[from..]);
            if self.compacts && self.roll(3) == 0 {
                // Everything is kept, so a snapshot of it stands for it.
                // The snapshot, the floor, then the hole, in that order.
                let at = mine.len() as u64;
                let index = json!({"offset": at}).to_string();
                snap::durable(&paths::snap_index(&home), index.as_bytes()).unwrap();
                set_floor(&home, at).unwrap();
                ledger::punch(&self.leaders[i].as_ref().unwrap().log, at);
            }
        }

        /// The disk under a home is replaced. Everything it held is gone,
        /// what it had promised included.
        fn wipe(&mut self, i: usize) {
            if self.leaders[i].is_some() || !self.target[i] {
                return;
            }
            let _ = fs::remove_dir_all(self.homes[i].join("ledger"));
            fs::create_dir_all(self.homes[i].join("ledger")).unwrap();
            self.wiped += 1;
        }

        fn run(&mut self, steps: usize, faults: u64) {
            let n = self.homes.len();
            for step in 0..steps {
                if self.wipes > 0 && step > 50 && self.wiped < self.wipes && self.roll(150) == 0 {
                    let i = self.roll(n as u64) as usize;
                    self.wipe(i);
                }
                if self.grows && self.roll(12) == 0 {
                    self.reshape();
                }
                let i = self.roll(n as u64) as usize;
                let j = self.roll(n as u64) as usize;
                match self.roll(20) {
                    0..=2 => {
                        for k in 0..n {
                            if self.roll(3) > 0 && self.leaders[k].is_none() {
                                self.quiet(k);
                            }
                        }
                        let up: Vec<bool> = (0..n).map(|_| self.roll(5) > 0).collect();
                        self.elect(i, &up, faults);
                    }
                    3..=6 => self.write(i),
                    7..=15 if i != j => self.send(i, j, faults),
                    16..=17 => self.answer(i),
                    // The daemon dies. Its home, and what is on it, stay.
                    18 if self.roll(3) == 0 => self.leaders[i] = None,
                    // A leader that has stopped being heard, alive or not.
                    19 => self.quiet(i),
                    _ => {}
                }
            }
        }

        /// No more faults, every link up. A leader must come of it, every
        /// home must end with the same log, and nothing kept may be missing.
        fn settle(&mut self) {
            let n = self.homes.len();
            self.disarm();
            let all = vec![true; n];
            // Every policy says the same thing by now.
            self.views = vec![self.target.clone(); n];
            let members: Vec<usize> = (0..n).filter(|k| self.target[*k]).collect();
            for i in 0..n {
                self.sync(i);
            }
            for round in 0..200 {
                let leading: Vec<usize> = (0..n).filter(|i| self.leaders[*i].is_some()).collect();
                if let [i] = leading[..] {
                    self.write(i);
                    for _ in 0..40 {
                        for j in members.iter().copied().filter(|j| *j != i) {
                            self.send(i, j, u64::MAX);
                        }
                    }
                    self.answer(i);
                    let same = members.iter().all(|j| self.len(*j) == self.len(i));
                    if self.leaders[i].is_some() && same && self.kept.len() as u64 == self.len(i) {
                        let top = members
                            .iter()
                            .map(|j| floor(&self.homes[*j]))
                            .max()
                            .unwrap() as usize;
                        let log = fs::read(paths::ledger(&self.homes[i])).unwrap();
                        for (j, home) in members.iter().map(|j| (*j, &self.homes[*j])) {
                            assert_eq!(fs::read(paths::ledger(home)).unwrap()[top..], log[top..]);
                            assert!(self.holds_kept(j), "r{j} ended without what was kept");
                            assert!(
                                usable(home),
                                "r{j} ended with a home that cannot be replayed"
                            );
                        }
                        return;
                    }
                    continue;
                }
                // None, or more than one that has not yet learned better.
                for i in leading {
                    self.write(i);
                    for j in members.iter().copied().filter(|j| *j != i) {
                        self.send(i, j, u64::MAX);
                    }
                }
                for k in members.iter().copied() {
                    if self.leaders[k].is_none() {
                        self.quiet(k);
                    }
                }
                self.elect(members[round % members.len()], &all, u64::MAX);
            }
            let state: Vec<String> = (0..n)
                .map(|i| {
                    let h = &self.homes[i];
                    format!(
                        "r{i}: leads {} epoch {} len {} last {:?} mark {:?} id {:?} usable {}",
                        self.leaders[i].is_some(),
                        snap::read_epoch(h),
                        log_len(h),
                        last(h),
                        refill(h),
                        cluster(h),
                        usable(h)
                    )
                })
                .collect();
            // A lost disk can leave too few homes that know enough to choose
            // a leader safely. Then nobody leads, and somebody has to say
            // which home has the ledger. Without a lost disk, never.
            assert!(
                self.wiped > 0,
                "with every home up and no more faults, nobody came to lead:\n{}",
                state.join("\n")
            );
            self.stuck = true;
        }

        fn done(self) {
            for home in &self.homes {
                let _ = fs::remove_dir_all(home);
            }
        }
    }

    fn storm(tag: &str, n: usize, seeds: std::ops::Range<u64>, steps: usize, faults: u64) {
        storm_with(tag, n, seeds, steps, faults, 0)
    }

    fn storm_with(
        tag: &str,
        n: usize,
        seeds: std::ops::Range<u64>,
        steps: usize,
        faults: u64,
        wipes: usize,
    ) {
        let (mut stuck, mut changes) = (0, 0);
        let runs = seeds.end - seeds.start;
        for seed in seeds {
            let mut sim = Sim::new(tag, n, seed);
            sim.wipes = wipes;
            sim.compacts = tag.starts_with('c');
            sim.run(steps, faults);
            sim.settle();
            stuck += sim.stuck as u64;
            changes += sim.changes as u64;
            sim.done();
        }
        // A run of changing sets in which no set changed has tested nothing.
        assert!(
            !tag.starts_with('m') || changes >= runs,
            "only {changes} changes of set in {runs} runs"
        );
        // Stuck is allowed after a lost disk, but it must be the exception.
        assert!(
            stuck * 4 <= runs,
            "{stuck} of {runs} runs ended with nobody able to lead"
        );
    }

    #[test]
    fn three_homes_at_random_lose_nothing_kept_and_end_with_a_leader() {
        storm("s3", 3, 0..40, 400, u64::MAX);
    }

    #[test]
    fn five_homes_at_random_lose_nothing_kept_and_end_with_a_leader() {
        storm("s5", 5, 0..20, 500, u64::MAX);
    }

    #[test]
    fn homes_killed_between_durable_writes_lose_nothing_kept() {
        storm("k3", 3, 0..40, 400, 6);
        storm("k5", 5, 0..15, 500, 6);
    }

    #[test]
    fn homes_that_let_go_of_the_log_lose_nothing_kept() {
        storm("c3", 3, 0..40, 500, u64::MAX);
        storm("c5", 5, 0..15, 600, u64::MAX);
        // And killed between durable writes, snapshot installs included.
        storm("ck3", 3, 0..40, 500, 6);
        // And with a disk lost.
        storm_with("cw3", 3, 0..30, 500, u64::MAX, 1);
    }

    #[test]
    fn a_set_that_grows_and_shrinks_loses_nothing_kept() {
        storm("m3", 3, 0..25, 500, u64::MAX);
        // And with kills between durable writes.
        storm("mk3", 3, 0..20, 500, 6);
    }

    #[test]
    fn a_home_that_loses_its_disk_costs_nothing_kept() {
        storm_with("w3", 3, 0..40, 500, u64::MAX, 1);
        storm_with("w5", 5, 0..15, 600, u64::MAX, 1);
    }
}
