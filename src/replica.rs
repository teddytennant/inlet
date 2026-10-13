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
//!   log     the ledger

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
const QUORUM_WAIT: Duration = Duration::from_secs(15);
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
        .and_then(|at| SystemTime::now().duration_since(at).ok())
        .unwrap_or(Duration::from_secs(3600))
}

fn log_len(home: &Path) -> u64 {
    fs::metadata(paths::ledger(home))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// One at a time on this home: two leaders' links, or a link and a vote.
fn guard(home: &Path) -> Result<File> {
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
        let reply = append(home, &req, &bytes)?;
        writeln!(out, "{reply}")?;
        out.flush()?;
    }
}

fn append(home: &Path, req: &Value, bytes: &[u8]) -> Result<Value> {
    let _guard = guard(home)?;
    let term = req["term"].as_u64().unwrap_or(0);
    let current = snap::read_epoch(home);
    if term < current {
        return Ok(json!({"ok": false, "term": current}));
    }
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
        let _ = fs::remove_file(paths::snap_index(home));
    }
    if at > len {
        return Ok(json!({"ok": false, "need": len}));
    }
    let skip = (len - at) as usize;
    if skip < bytes.len() {
        file.seek(SeekFrom::End(0))?;
        file.write_all(&bytes[skip..])?;
        file.sync_all()?;
        len += (bytes.len() - skip) as u64;
    }
    let known: Terms = theirs
        .into_iter()
        .filter(|(_, start)| *start <= len)
        .collect();
    if known != mine {
        store_terms(home, &known)?;
    }
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
    let term = req["term"].as_u64().unwrap_or(0);
    let candidate = req["candidate"].as_str().unwrap_or("");
    let dry = req["pre"] == true;
    let current = snap::read_epoch(home);
    let mine = terms(home);
    let my_last = (mine.last().map_or(0, |t| t.0), log_len(home));
    let their_last = (
        req["last_term"].as_u64().unwrap_or(0),
        req["len"].as_u64().unwrap_or(0),
    );
    let (voted_term, voted_for) = voted(home);
    let free = voted_term < term || voted_for == candidate;
    // A replica that can still hear a leader does not help unseat it.
    let leaderless = quiet_for(home) >= Duration::from_millis(QUIET_MS);
    let fresh = their_last >= my_last;
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
        touch(home);
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
fn poll(peers: &[Peer], req: &Value) -> (usize, u64) {
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
pub fn stand_by(home: &Path, me: &str, peers: &[Peer]) -> Result<u64> {
    let most = most(peers.len());
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
        patience = QUIET_MS + jitter(QUIET_MS);
        let last = |home: &Path| (terms(home).last().map_or(0, |t| t.0), log_len(home));
        let (last_term, len) = last(home);
        let next = snap::read_epoch(home) + 1;
        let ask = |term: u64, pre: bool| json!({"term": term, "candidate": me, "last_term": last_term, "len": len, "pre": pre});
        // First whether a vote could be won, without disturbing anybody. A
        // replica cut off from the rest asks for ever and changes nothing.
        let (yes, _) = poll(peers, &ask(next, true));
        if yes + 1 < most {
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
        let (yes, newest) = poll(peers, &ask(term, false));
        if newest > term {
            let _guard = guard(home)?;
            if newest > snap::read_epoch(home) {
                set_epoch(home, newest)?;
            }
        }
        if yes + 1 >= most && snap::read_epoch(home) == term {
            eprintln!("inlet: replica {me} elected for term {term}");
            touch(home);
            return Ok(term);
        }
        touch(home);
    }
}

/// The new leader's first act: note where its term's bytes begin.
pub fn begin_term(home: &Path, term: u64, len: u64) -> Result<Terms> {
    let _guard = guard(home)?;
    let mut mine = terms(home);
    // The log may have lost a torn tail on the way in.
    mine.retain(|(_, start)| *start <= len);
    if mine.last().is_none_or(|last| last.0 != term) {
        mine.push((term, len));
    }
    store_terms(home, &mine)?;
    Ok(mine)
}

struct Shared {
    want: u64,
    acked: Vec<u64>,
    deposed: bool,
}

/// The leader's side: one thread and one long-lived command per peer.
pub struct Links {
    shared: Arc<(Mutex<Shared>, Condvar)>,
    most: usize,
}

impl Links {
    pub fn start(home: &Path, term: u64, peers: &[Peer], terms: Terms, len: u64) -> Links {
        let shared = Arc::new((
            Mutex::new(Shared {
                want: len,
                acked: vec![0; peers.len()],
                deposed: false,
            }),
            Condvar::new(),
        ));
        for (i, peer) in peers.iter().enumerate() {
            let (shared, peer, terms) = (shared.clone(), peer.clone(), terms.clone());
            let log = paths::ledger(home);
            thread::spawn(move || loop {
                if link(&shared, i, &peer, term, &terms, &log) {
                    return;
                }
                thread::sleep(Duration::from_millis(300));
            });
        }
        Links {
            shared,
            most: most(peers.len()),
        }
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
            let have = 1 + shared.acked.iter().filter(|a| **a >= len).count();
            if have >= self.most {
                return Ok(());
            }
            let left = QUORUM_WAIT.saturating_sub(started.elapsed());
            if left.is_zero() {
                return Err(err(
                    "most replicas are out of reach; stopping rather than promise what cannot be kept",
                ));
            }
            shared = wake.wait_timeout(shared, left).unwrap().0;
        }
    }

    pub fn deposed(&self) -> bool {
        self.shared.0.lock().unwrap().deposed
    }
}

/// One connection's life. True when the leader has been replaced and the
/// link should not come back.
fn link(
    shared: &Arc<(Mutex<Shared>, Condvar)>,
    i: usize,
    peer: &Peer,
    term: u64,
    terms: &Terms,
    log: &Path,
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
    let mut theirs: Option<u64> = None;
    loop {
        let want = lock.lock().unwrap().want;
        let at = theirs.unwrap_or(want).min(want);
        let n = (want - at).min(CHUNK);
        let mut bytes = vec![0u8; n as usize];
        if file.seek(SeekFrom::Start(at)).is_err() || file.read_exact(&mut bytes).is_err() {
            return done(&mut child, false);
        }
        let header = json!({"term": term, "at": at, "n": n, "terms": terms});
        if writeln!(stdin, "{header}").is_err()
            || stdin.write_all(&bytes).is_err()
            || stdin.flush().is_err()
        {
            return done(&mut child, false);
        }
        let Ok(line) = rx.recv_timeout(CALL) else {
            return done(&mut child, false);
        };
        let Ok(reply) = serde_json::from_str::<Value>(&line) else {
            return done(&mut child, false);
        };
        if reply["ok"] == true {
            let len = reply["len"].as_u64().unwrap_or(0);
            theirs = Some(len);
            let mut shared = lock.lock().unwrap();
            // More than the leader has is a log this leader has not vouched for.
            if len <= shared.want && len > shared.acked[i] {
                shared.acked[i] = len;
                wake.notify_all();
            }
        } else if let Some(need) = reply["need"].as_u64() {
            theirs = Some(need);
            continue;
        } else if reply["term"].as_u64().is_some_and(|t| t > term) {
            lock.lock().unwrap().deposed = true;
            wake.notify_all();
            return done(&mut child, true);
        } else {
            thread::sleep(Duration::from_millis(200));
            continue;
        }
        // Caught up. Wait for more, and say something anyway before long.
        let shared = lock.lock().unwrap();
        if shared.deposed {
            return done(&mut child, true);
        }
        if theirs == Some(shared.want) {
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
        assert!(!ask(2, "b", 1, 60, true));
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
}
