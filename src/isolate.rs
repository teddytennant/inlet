use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::config::Isolator;

#[derive(Debug)]
pub struct Placement {
    pub kind: &'static str,
    pub cgroup: Option<PathBuf>,
}

/// cgroup v2 when the subtree is delegated, otherwise rlimit.
/// Slurm is named and refused: this build does not submit jobs.
pub fn place(prefer: Isolator, id: &str, memory_mb: u64, pids: u64) -> Placement {
    if prefer == Isolator::Slurm {
        return Placement {
            kind: "slurm",
            cgroup: None,
        };
    }
    if prefer == Isolator::Cgroup {
        if let Some(path) = try_cgroup(id, memory_mb, pids) {
            return Placement {
                kind: "cgroup",
                cgroup: Some(path),
            };
        }
    }
    Placement {
        kind: "rlimit",
        cgroup: None,
    }
}

pub fn assign(cgroup: &Path, pid: i32) -> bool {
    let procs = cgroup.join("cgroup.procs");
    let mut file = match fs::OpenOptions::new().write(true).open(&procs) {
        Ok(f) => f,
        Err(_) => return false,
    };
    write!(file, "{pid}").is_ok()
}

/// The cgroup worker groups are made under, once `prepare` has run.
static BASE: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

/// cgroup v2 will not hand controllers to children of a group that has
/// processes of its own, and the daemon is such a process. So the daemon
/// moves itself into a leaf, and its old group becomes the parent of one
/// group per worker. Needs a delegated group: a systemd unit with
/// `Delegate=yes`, or a user unit. Anywhere else this quietly does nothing
/// and workers get rlimits.
pub fn prepare() -> bool {
    BASE.get_or_init(|| {
        let parent = self_cgroup()?;
        // A restarted daemon lands in the leaf the last one made.
        let parent = if parent.file_name().is_some_and(|n| n == "inlet-supervisor") {
            parent.parent()?.to_path_buf()
        } else {
            parent
        };
        let leaf = parent.join("inlet-supervisor");
        fs::create_dir_all(&leaf).ok()?;
        if !assign(&leaf, std::process::id() as i32) {
            return None;
        }
        fs::write(parent.join("cgroup.subtree_control"), "+memory +pids").ok()?;
        let _ = fs::write(parent.join("cgroup.subtree_control"), "+cpu");
        Some(parent)
    })
    .is_some()
}

/// The group a task ran in, if it had one.
pub fn group(id: &str) -> Option<PathBuf> {
    let dir = BASE.get()?.as_ref()?.join(format!("inlet-{id}"));
    dir.is_dir().then_some(dir)
}

/// Whether the kernel killed something in the group for memory.
pub fn oom_killed(id: &str) -> bool {
    group(id)
        .and_then(|dir| fs::read_to_string(dir.join("memory.events")).ok())
        .is_some_and(|events| {
            events
                .lines()
                .any(|l| l.strip_prefix("oom_kill ").is_some_and(|n| n.trim() != "0"))
        })
}

/// Kill whatever is left in a task's group and remove it.
pub fn release(id: &str) {
    if let Some(dir) = group(id) {
        let _ = fs::write(dir.join("cgroup.kill"), "1");
        for _ in 0..50 {
            if fs::remove_dir(&dir).is_ok() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

fn try_cgroup(id: &str, memory_mb: u64, pids: u64) -> Option<PathBuf> {
    let parent = BASE.get()?.clone()?;
    let dir = parent.join(format!("inlet-{id}"));
    fs::create_dir_all(&dir).ok()?;
    let mem = memory_mb.saturating_mul(1024 * 1024).max(1);
    fs::write(dir.join("memory.max"), mem.to_string()).ok()?;
    // No swap to hide in, and the whole group dies together, not one victim.
    let _ = fs::write(dir.join("memory.swap.max"), "0");
    let _ = fs::write(dir.join("memory.oom.group"), "1");
    fs::write(dir.join("pids.max"), pids.max(1).to_string()).ok()?;
    let _ = fs::write(dir.join("cpu.max"), "max 100000");
    Some(dir)
}

fn self_cgroup() -> Option<PathBuf> {
    let text = fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in text.lines() {
        let Some((_, path)) = line.split_once("::") else {
            continue;
        };
        let rel = path.trim_start_matches('/');
        if rel.is_empty() {
            return Some(PathBuf::from("/sys/fs/cgroup"));
        }
        return Some(PathBuf::from("/sys/fs/cgroup").join(rel));
    }
    None
}
