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

fn try_cgroup(id: &str, memory_mb: u64, pids: u64) -> Option<PathBuf> {
    let parent = self_cgroup()?;
    let _ = fs::write(parent.join("cgroup.subtree_control"), "+memory +pids +cpu");
    let dir = parent.join(format!("inlet-{id}"));
    fs::create_dir_all(&dir).ok()?;
    let mem = memory_mb.saturating_mul(1024 * 1024).max(1);
    fs::write(dir.join("memory.max"), mem.to_string()).ok()?;
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
