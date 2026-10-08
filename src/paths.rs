use std::path::{Path, PathBuf};

pub fn ledger(home: &Path) -> PathBuf {
    home.join("ledger/log")
}
pub fn policy(home: &Path) -> PathBuf {
    home.join("policy.lua")
}
pub fn operator_sock(home: &Path) -> PathBuf {
    home.join("run/operator.sock")
}
pub fn proxy_sock(home: &Path) -> PathBuf {
    home.join("proxy/proxy.sock")
}
pub fn pid_file(home: &Path) -> PathBuf {
    home.join("run/inlet.pid")
}
pub fn work(home: &Path, id: &str) -> PathBuf {
    home.join("work").join(id)
}
pub fn scratch(home: &Path, id: &str) -> PathBuf {
    home.join("scratch").join(id)
}
pub fn registry(home: &Path) -> PathBuf {
    home.join("registry")
}
pub fn cell_root(home: &Path, id: &str) -> PathBuf {
    home.join("run/cells").join(id)
}
pub fn transcript(home: &Path, id: &str) -> PathBuf {
    home.join("runs").join(format!("{id}.log"))
}
pub fn daemon_log(home: &Path) -> PathBuf {
    home.join("run/daemon.log")
}
