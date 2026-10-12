use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::chown;
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{Duration, Instant};

use crate::error::{err, Result};

const FS_READ: u64 = (1 << 0) | (1 << 2) | (1 << 3); // execute, read file, read dir
const FS_WRITE: u64 = (1 << 1)
    | (1 << 4)
    | (1 << 5)
    | (1 << 7)
    | (1 << 8)
    | (1 << 9)
    | (1 << 10)
    | (1 << 12)
    | (1 << 13)
    | (1 << 14);
const FS_ALL: u64 = FS_READ | FS_WRITE;

#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
    handled_access_net: u64,
    scoped: u64,
}

/// Landlock ABI 6. A sandboxed process cannot signal a process outside its domain.
const SCOPE_SIGNAL: u64 = 1 << 1;

#[repr(C, packed)]
struct PathBeneath {
    allowed_access: u64,
    parent_fd: i32,
}

#[derive(Default)]
pub struct SpawnRequest {
    pub cmd: Vec<String>,
    pub work: PathBuf,
    pub scratch: PathBuf,
    pub registry: PathBuf,
    pub root: PathBuf,
    pub proxy_sock: PathBuf,
    pub worker_sock: PathBuf,
    pub preamble: String,
    pub net_none: bool,
    pub memory_mb: u64,
    pub pids: u64,
    pub seconds: u64,
    pub token: String,
    pub task_id: String,
    pub cgroup: Option<PathBuf>,
    /// Worker door: proxy socket, worker socket, and a token. Verifiers leave this off.
    pub door: bool,
    /// Extra read-only host paths, visible at the same path inside.
    pub ro: Vec<PathBuf>,
    /// Extra environment for the worker.
    pub env: Vec<(String, String)>,
    /// Files written under the cell's `etc/`, next to `preamble`.
    pub files: Vec<(String, String)>,
    /// Name of an `etc/` file to feed on stdin. `None` is `/dev/null`.
    pub stdin: Option<String>,
    /// Let the worker outlive the daemon.
    pub keep: Option<Keep>,
}

/// A kept worker runs under a small parent that stays in the cell with it.
/// That parent writes the worker's output to `log` and its wait status to
/// `status`, so neither needs the daemon to be alive.
pub struct Keep {
    pub log: PathBuf,
    /// Four bytes, the raw wait status, written when the worker exits.
    pub status: PathBuf,
    /// Output past this many bytes is dropped.
    pub cap: u64,
    /// Seconds after which the worker is killed with no daemon to do it. 0 is never.
    pub deadline: u64,
}

pub struct Spawned {
    pub pid: i32,
    pub stdout: File,
}

/// One read-only bind into the new root. `flags` carries the source mount's
/// locked flags, or a remount inside a user namespace is refused.
#[repr(C)]
struct Bind {
    src: *const libc::c_char,
    dst: *const libc::c_char,
    flags: libc::c_ulong,
}

type Rule = (*const libc::c_char, u64);

struct Raw {
    parent: i32,
    ready_w: i32,
    ack_r: i32,
    err_w: i32,
    out_w: i32,
    net_none: bool,
    newroot: *const libc::c_char,
    work: *const libc::c_char,
    scratch: *const libc::c_char,
    devnull: *const libc::c_char,
    devzero: *const libc::c_char,
    devurandom: *const libc::c_char,
    nr_work: *const libc::c_char,
    nr_scratch: *const libc::c_char,
    nr_null: *const libc::c_char,
    nr_zero: *const libc::c_char,
    nr_urandom: *const libc::c_char,
    nr_old: *const libc::c_char,
    binds: *const Bind,
    n_binds: usize,
    rules_pivot: *const Rule,
    n_rules_pivot: usize,
    rules_host: *const Rule,
    n_rules_host: usize,
    argv: *const *const libc::c_char,
    envp_pivot: *const *const libc::c_char,
    envp_host: *const *const libc::c_char,
    /// Null is `/dev/null`.
    stdin_pivot: *const libc::c_char,
    stdin_host: *const libc::c_char,
    memory_mb: u64,
    pids: u64,
    seconds: u64,
    in_user_ns: bool,
    keep: bool,
    log_fd: i32,
    status_fd: i32,
    cap: u64,
    deadline: u64,
}

/// Host paths every cell sees read-only: `/usr`, the top-level dirs a
/// merged-usr distro symlinks into it when they are real, and the Nix store.
pub fn system_ro() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for dir in [
        "/usr",
        "/bin",
        "/sbin",
        "/lib",
        "/lib64",
        "/lib32",
        "/nix/store",
    ] {
        let path = Path::new(dir);
        if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_dir()) {
            out.push(path.to_path_buf());
        }
    }
    out
}

fn visible(req: &SpawnRequest) -> Vec<PathBuf> {
    let mut out = system_ro();
    for path in &req.ro {
        if path.is_absolute() && path.exists() && !out.contains(path) {
            out.push(path.clone());
        }
    }
    out.sort();
    out
}

/// The daemon's PATH, resolved, keeping the entries a cell can see.
pub fn cell_path(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    let host = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&host).chain(["/usr/bin".into(), "/bin".into()]) {
        let Ok(real) = fs::canonicalize(&dir) else {
            continue;
        };
        for seen in [real, dir] {
            if roots.iter().any(|root| seen.starts_with(root)) && !out.contains(&seen) {
                out.push(seen);
            }
        }
    }
    out
}

/// execve does not search PATH. A bare name is looked up where the cell can see it.
fn resolve_cmd(name: &str, path: &[PathBuf]) -> Result<String> {
    if name.contains('/') {
        return Ok(name.to_string());
    }
    for dir in path {
        let full = dir.join(name);
        if full.is_file() {
            return Ok(full.to_string_lossy().into_owned());
        }
    }
    Err(err(format!("worker command not found: {name}")))
}

/// The kernel's value. musl's libc crate does not export it.
const ST_RELATIME: libc::c_ulong = 4096;

fn remount_flags(path: &Path) -> libc::c_ulong {
    let mut flags = libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY;
    let Ok(c) = c_path(path) else {
        return flags;
    };
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut vfs) } != 0 {
        return flags;
    }
    for (have, want) in [
        (libc::ST_NOSUID, libc::MS_NOSUID),
        (libc::ST_NODEV, libc::MS_NODEV),
        (libc::ST_NOEXEC, libc::MS_NOEXEC),
        (libc::ST_NOATIME, libc::MS_NOATIME),
        (libc::ST_NODIRATIME, libc::MS_NODIRATIME),
        (ST_RELATIME, libc::MS_RELATIME),
    ] {
        if vfs.f_flag & have != 0 {
            flags |= want;
        }
    }
    flags
}

/// Returns false when neither a mount namespace nor Landlock can be entered.
pub fn probe(home: &Path) -> bool {
    let root = home.join("run/probe-cell");
    let work = home.join("work/probe");
    let scratch = home.join("scratch/probe");
    let registry = home.join("registry");
    let proxy = home.join("proxy/probe.sock");
    let _ = fs::create_dir_all(proxy.parent().unwrap());
    let _ = fs::create_dir_all(&registry);
    let _ = fs::remove_file(&proxy);
    // Held for the bind mount. Dropped when the probe returns.
    let listener = std::os::unix::net::UnixListener::bind(&proxy).ok();
    if listener.is_none() {
        return false;
    }
    let req = SpawnRequest {
        cmd: vec!["true".into()],
        work,
        scratch,
        registry,
        root,
        worker_sock: proxy.clone(),
        proxy_sock: proxy,
        preamble: "probe\n".into(),
        net_none: true,
        memory_mb: 64,
        pids: 32,
        seconds: 5,
        token: "probe".into(),
        task_id: "probe".into(),
        cgroup: None,
        door: true,
        ..Default::default()
    };
    match spawn(&req) {
        Ok(child) => {
            let mut status = 0;
            let pid = unsafe { libc::waitpid(child.pid, &mut status, 0) };
            let ok = pid == child.pid && libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0;
            if !ok {
                eprintln!("inlet: cell probe exited {status}");
            }
            ok
        }
        Err(e) => {
            eprintln!("inlet: cell probe failed: {e}");
            false
        }
    }
}

pub fn spawn(req: &SpawnRequest) -> Result<Spawned> {
    if req.cmd.is_empty() {
        return Err(err("empty worker command"));
    }
    let ro = visible(req);
    let path = cell_path(&ro);
    let mut cmd = req.cmd.clone();
    cmd[0] = resolve_cmd(&cmd[0], &path)?;
    fs::create_dir_all(&req.work)?;
    fs::create_dir_all(&req.scratch)?;
    fs::create_dir_all(&req.registry)?;
    prepare_root(&req.root, &ro)?;
    crate::registry::snapshot(&req.registry, &req.root.join("registry"))?;
    fs::write(req.root.join("etc/preamble"), &req.preamble)?;
    for (name, body) in &req.files {
        if name.contains('/') || name.starts_with('.') {
            return Err(err("bad cell file name"));
        }
        fs::write(req.root.join("etc").join(name), body)?;
    }
    if unsafe { libc::geteuid() } == 0 {
        let _ = chown(&req.work, Some(65534), Some(65534));
        let _ = chown(&req.scratch, Some(65534), Some(65534));
    }

    let (ready_r, ready_w) = pipe_cloexec()?;
    let (ack_r, ack_w) = pipe_cloexec()?;
    let (err_r, err_w) = pipe_cloexec()?;
    let (out_r, out_w) = pipe_cloexec()?;

    let c = |p: &Path| c_path(p);
    let newroot = c(&req.root)?;
    let work = c(&req.work)?;
    let scratch = c(&req.scratch)?;
    let registry = c(&req.root.join("registry"))?;
    let host_etc = c(&req.root.join("etc"))?;
    let proxy = c(&req.proxy_sock)?;
    let worker = c(&req.worker_sock)?;
    let devnull = c_path(Path::new("/dev/null"))?;
    let devzero = c_path(Path::new("/dev/zero"))?;
    let devurandom = c_path(Path::new("/dev/urandom"))?;
    let join = |name: &str| c_path(&req.root.join(name));
    let nr_work = join("work")?;
    let nr_scratch = join("scratch")?;
    let nr_null = join("dev/null")?;
    let nr_zero = join("dev/zero")?;
    let nr_urandom = join("dev/urandom")?;
    let nr_old = join("old")?;

    // The worker's client for the door is this binary.
    let exe = if req.door {
        std::env::current_exe().ok()
    } else {
        None
    };
    let mut bind_c: Vec<(CString, CString, libc::c_ulong)> = Vec::new();
    for src in &ro {
        let rel = src.strip_prefix("/").unwrap_or(src);
        bind_c.push((c(src)?, c(&req.root.join(rel))?, remount_flags(src)));
    }
    if let Some(exe) = &exe {
        fs::File::create(req.root.join("run/inlet"))?;
        bind_c.push((c(exe)?, join("run/inlet")?, remount_flags(exe)));
    }
    if req.door {
        // The directory, not the socket: a restarted daemon binds a new inode
        // at the same name, and a kept worker has to find it. Read-only, so a
        // worker cannot swap the socket out from under its siblings.
        for (sock, dir, link) in [
            (&req.proxy_sock, "run/px", "run/proxy.sock"),
            (&req.worker_sock, "run/wk", "run/worker.sock"),
        ] {
            let (Some(parent), Some(name)) = (sock.parent(), sock.file_name()) else {
                return Err(err("socket path has no directory"));
            };
            fs::create_dir_all(req.root.join(dir))?;
            bind_c.push((c(parent)?, join(dir)?, remount_flags(parent)));
            let target = Path::new(dir.trim_start_matches("run/")).join(name);
            std::os::unix::fs::symlink(target, req.root.join(link))?;
        }
    }
    let kept = match &req.keep {
        Some(keep) => {
            let open = |path: &Path, flags: i32| -> Result<OwnedFd> {
                let fd =
                    unsafe { libc::open(c_path(path)?.as_ptr(), flags | libc::O_CLOEXEC, 0o600) };
                if fd < 0 {
                    return Err(err(format!("open {}", path.display())));
                }
                Ok(OwnedFd(fd))
            };
            Some((
                open(&keep.log, libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND)?,
                open(&keep.status, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC)?,
            ))
        }
        None => None,
    };
    let binds: Vec<Bind> = bind_c
        .iter()
        .map(|(src, dst, flags)| Bind {
            src: src.as_ptr(),
            dst: dst.as_ptr(),
            flags: *flags,
        })
        .collect();

    let ro_c: Vec<CString> = ro.iter().map(|p| c(p)).collect::<Result<_>>()?;
    let exe_c = match &exe {
        Some(exe) => Some(c(exe)?),
        None => None,
    };
    let run_access = if req.door { FS_ALL } else { FS_READ };
    let mut rules_pivot: Vec<Rule> = ro_c.iter().map(|p| (p.as_ptr(), FS_READ)).collect();
    rules_pivot.extend([
        (c"/work".as_ptr(), FS_ALL),
        (c"/scratch".as_ptr(), FS_ALL),
        (c"/registry".as_ptr(), FS_READ),
        (c"/proc".as_ptr(), FS_READ),
        (c"/dev".as_ptr(), FS_ALL),
        (c"/tmp".as_ptr(), FS_ALL),
        (c"/etc".as_ptr(), FS_READ),
        (c"/run".as_ptr(), run_access),
    ]);
    let mut rules_host: Vec<Rule> = ro_c.iter().map(|p| (p.as_ptr(), FS_READ)).collect();
    rules_host.extend([
        (work.as_ptr(), FS_ALL),
        (scratch.as_ptr(), FS_ALL),
        (registry.as_ptr(), FS_READ),
        (host_etc.as_ptr(), FS_READ),
        (c"/proc".as_ptr(), FS_READ),
        (c"/dev".as_ptr(), FS_ALL),
        (c"/etc".as_ptr(), FS_READ),
        (c"/tmp".as_ptr(), FS_ALL),
    ]);
    if req.door {
        rules_host.push((proxy.as_ptr(), FS_ALL));
        rules_host.push((worker.as_ptr(), FS_ALL));
        if let Some(exe) = &exe_c {
            rules_host.push((exe.as_ptr(), FS_READ));
        }
    }

    let argv_c = c_strings(&cmd)?;
    let mut argv: Vec<*const libc::c_char> = argv_c.iter().map(|s| s.as_ptr()).collect();
    argv.push(ptr::null());
    let path_env = std::env::join_paths(&path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "/usr/bin:/bin".into());
    let env_pivot_c = c_strings(&env_strings(req, &path_env, true, exe.as_deref()))?;
    let env_host_c = c_strings(&env_strings(req, &path_env, false, exe.as_deref()))?;
    let ptrs = |strings: &[CString]| {
        let mut out: Vec<*const libc::c_char> = strings.iter().map(|s| s.as_ptr()).collect();
        out.push(ptr::null());
        out
    };
    let envp_pivot = ptrs(&env_pivot_c);
    let envp_host = ptrs(&env_host_c);
    let stdin_c = match &req.stdin {
        Some(name) => Some((
            c(&Path::new("/etc").join(name))?,
            c(&req.root.join("etc").join(name))?,
        )),
        None => None,
    };

    let mut raw = Raw {
        parent: unsafe { libc::getpid() },
        ready_w: ready_w.0,
        ack_r: ack_r.0,
        err_w: err_w.0,
        out_w: out_w.0,
        net_none: req.net_none,
        newroot: newroot.as_ptr(),
        work: work.as_ptr(),
        scratch: scratch.as_ptr(),
        devnull: devnull.as_ptr(),
        devzero: devzero.as_ptr(),
        devurandom: devurandom.as_ptr(),
        nr_work: nr_work.as_ptr(),
        nr_scratch: nr_scratch.as_ptr(),
        nr_null: nr_null.as_ptr(),
        nr_zero: nr_zero.as_ptr(),
        nr_urandom: nr_urandom.as_ptr(),
        nr_old: nr_old.as_ptr(),
        binds: binds.as_ptr(),
        n_binds: binds.len(),
        rules_pivot: rules_pivot.as_ptr(),
        n_rules_pivot: rules_pivot.len(),
        rules_host: rules_host.as_ptr(),
        n_rules_host: rules_host.len(),
        argv: argv.as_ptr(),
        envp_pivot: envp_pivot.as_ptr(),
        envp_host: envp_host.as_ptr(),
        stdin_pivot: stdin_c.as_ref().map_or(ptr::null(), |s| s.0.as_ptr()),
        stdin_host: stdin_c.as_ref().map_or(ptr::null(), |s| s.1.as_ptr()),
        memory_mb: req.memory_mb,
        pids: req.pids,
        seconds: req.seconds,
        in_user_ns: true,
        keep: kept.is_some(),
        log_fd: kept.as_ref().map_or(-1, |k| k.0 .0),
        status_fd: kept.as_ref().map_or(-1, |k| k.1 .0),
        cap: req.keep.as_ref().map_or(0, |k| k.cap),
        deadline: req.keep.as_ref().map_or(0, |k| k.deadline),
    };

    let stack = map_stack()?;
    let mut pid = unsafe {
        libc::clone(
            trampoline,
            stack.top(),
            libc::SIGCHLD | libc::CLONE_NEWUSER | libc::CLONE_NEWPID,
            &mut raw as *mut Raw as *mut libc::c_void,
        )
    };
    if pid < 0 {
        // No new pid namespace. The child still unshares a user namespace itself.
        raw.in_user_ns = false;
        pid = unsafe { libc::fork() };
        if pid == 0 {
            unsafe { child_entry(&raw) }
        }
    }
    if pid < 0 {
        return Err(err("fork"));
    }

    drop(ready_w);
    drop(ack_r);
    drop(err_w);
    drop(out_w);
    drop(kept);

    let mut byte = [0u8; 1];
    if read_full(ready_r.0, &mut byte).is_err() {
        let _ = unsafe { libc::waitpid(pid, ptr::null_mut(), 0) };
        return Err(err("worker died before the user namespace was ready"));
    }
    if let Err(e) = write_id_maps(pid) {
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = unsafe { libc::waitpid(pid, ptr::null_mut(), 0) };
        return Err(e);
    }
    if let Some(cg) = &req.cgroup {
        let _ = crate::isolate::assign(cg, pid);
    }
    let _ = write_full(ack_w.0, &[1]);
    drop(ack_w);
    drop(ready_r);

    let mut code = [0u8; 4];
    let n = read_full(err_r.0, &mut code);
    drop(err_r);
    if n.is_ok() {
        let errno = i32::from_ne_bytes(code);
        let _ = unsafe { libc::waitpid(pid, ptr::null_mut(), 0) };
        return Err(err(format!("cell setup failed ({errno})")));
    }

    set_nonblock(out_r.0);
    let stdout = unsafe { File::from_raw_fd(out_r.into_raw()) };
    Ok(Spawned { pid, stdout })
}

fn prepare_root(root: &Path, ro: &[PathBuf]) -> Result<()> {
    let _ = fs::remove_dir_all(root);
    for rel in [
        "usr", "work", "scratch", "registry", "proc", "dev", "tmp", "etc", "run", "old",
    ] {
        fs::create_dir_all(root.join(rel))?;
    }
    for rel in ["dev/null", "dev/zero", "dev/urandom"] {
        File::create(root.join(rel))?;
    }
    for src in ro {
        let dst = root.join(src.strip_prefix("/").unwrap_or(src));
        if src.is_dir() {
            fs::create_dir_all(&dst)?;
        } else {
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            File::create(&dst)?;
        }
    }
    // Merged-usr hosts keep these as links. A host with real dirs had them bound above.
    for (link, target) in [
        ("bin", "usr/bin"),
        ("lib", "usr/lib"),
        ("lib64", "usr/lib64"),
        ("sbin", "usr/sbin"),
    ] {
        let path = root.join(link);
        if path.is_dir() {
            continue;
        }
        let _ = fs::remove_file(&path);
        std::os::unix::fs::symlink(target, path)?;
    }
    if let Ok(resolv) = fs::read("/etc/resolv.conf") {
        let mut f = File::create(root.join("etc/resolv.conf"))?;
        f.write_all(&resolv)?;
    }
    fs::write(
        root.join("etc/nsswitch.conf"),
        "hosts: files dns\npasswd: files\n",
    )?;
    // HTTPS from a worker needs the host's roots. /etc is not the host's.
    for bundle in [
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
    ] {
        if let Ok(pem) = fs::read(bundle) {
            fs::create_dir_all(root.join("etc/ssl/certs"))?;
            fs::write(root.join("etc/ssl/certs/ca-certificates.crt"), pem)?;
            break;
        }
    }
    Ok(())
}

/// Not pivoted is the Landlock fallback, where every path is the host's.
fn env_strings(req: &SpawnRequest, path: &str, pivoted: bool, exe: Option<&Path>) -> Vec<String> {
    let show = |p: &Path| p.to_string_lossy().into_owned();
    let etc = if pivoted {
        "/etc".to_string()
    } else {
        show(&req.root.join("etc"))
    };
    let mut env = vec![
        format!(
            "HOME={}",
            if pivoted {
                "/work".into()
            } else {
                show(&req.work)
            }
        ),
        format!("PATH={path}"),
        "TMPDIR=/tmp".into(),
        "LANG=C".into(),
    ];
    if req.root.join("etc/ssl/certs/ca-certificates.crt").exists() {
        env.push(format!("SSL_CERT_FILE={etc}/ssl/certs/ca-certificates.crt"));
    }
    if req.door {
        let (proxy, sock) = if pivoted {
            (
                "/run/proxy.sock".to_string(),
                "/run/worker.sock".to_string(),
            )
        } else {
            (show(&req.proxy_sock), show(&req.worker_sock))
        };
        env.push(format!("INLET_TOKEN={}", req.token));
        env.push(format!("INLET_ID={}", req.task_id));
        env.push(format!("INLET_PROXY_SOCK={proxy}"));
        env.push(format!("INLET_SOCK={sock}"));
        env.push(format!("OPENAI_API_KEY={}", req.token));
        env.push(format!(
            "INLET_REGISTRY={}",
            if pivoted {
                "/registry".into()
            } else {
                show(&req.root.join("registry"))
            }
        ));
        if let Some(exe) = exe {
            let bin = if pivoted {
                "/run/inlet".into()
            } else {
                show(exe)
            };
            env.push(format!("INLET_BIN={bin}"));
        }
        for (name, _) in &req.files {
            let key = name.split('.').next().unwrap_or(name).to_uppercase();
            env.push(format!("INLET_{key}_FILE={etc}/{name}"));
        }
    }
    for (key, value) in &req.env {
        if !key.is_empty() && !key.contains('=') && !key.starts_with("INLET_") {
            env.push(format!("{key}={value}"));
        }
    }
    env
}

extern "C" fn trampoline(arg: *mut libc::c_void) -> libc::c_int {
    let raw = unsafe { &*(arg as *const Raw) };
    unsafe { child_entry(raw) }
}

/// Child side. Never returns to Rust.
unsafe fn child_entry(a: &Raw) -> ! {
    if !a.keep {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
        // pid 1 of a new pid namespace sees getppid() == 0. The fallback fork does not.
        if !a.in_user_ns && libc::getppid() != a.parent {
            libc::raise(libc::SIGKILL);
        }
    }
    if !a.in_user_ns && libc::unshare(libc::CLONE_NEWUSER) != 0 {
        fail(a.err_w, errno());
    }
    let _ = libc::write(a.ready_w, [1u8].as_ptr().cast(), 1);
    let mut ack = [0u8; 1];
    if libc::read(a.ack_r, ack.as_mut_ptr().cast(), 1) != 1 {
        fail(a.err_w, errno());
    }
    libc::setpgid(0, 0);
    let mut flags = libc::CLONE_NEWNS;
    if a.net_none {
        flags |= libc::CLONE_NEWNET;
    }
    if libc::unshare(flags) != 0 {
        fail(a.err_w, errno());
    }

    let pivoted = pivot(a) == 0;
    let rules = if pivoted {
        std::slice::from_raw_parts(a.rules_pivot, a.n_rules_pivot)
    } else {
        std::slice::from_raw_parts(a.rules_host, a.n_rules_host)
    };
    let locked = landlock(rules);
    if locked != 0 {
        fail(a.err_w, locked);
    }
    set_limits(a.memory_mb, a.pids, a.seconds);
    let _ = libc::chdir(if pivoted { c"/work".as_ptr() } else { a.work });

    let mut out = a.out_w;
    if a.keep {
        let mut ends = [0i32; 2];
        if libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) != 0 {
            fail(a.err_w, errno());
        }
        let worker = libc::fork();
        if worker < 0 {
            fail(a.err_w, errno());
        }
        if worker > 0 {
            libc::close(ends[1]);
            keeper(a, worker, ends[0]);
        }
        out = ends[1];
    }

    let input = if pivoted { a.stdin_pivot } else { a.stdin_host };
    let mut stdin = -1;
    if !input.is_null() {
        stdin = libc::open(input, libc::O_RDONLY);
    }
    if stdin < 0 {
        stdin = libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
    }
    if stdin >= 0 {
        libc::dup2(stdin, 0);
        libc::close(stdin);
    }
    libc::dup2(out, 1);
    libc::dup2(out, 2);
    close_except(a.err_w);
    let envp = if pivoted { a.envp_pivot } else { a.envp_host };
    libc::execve(a.argv.read(), a.argv, envp);
    fail(a.err_w, errno());
}

/// The kept worker's parent. Holds nothing of the daemon's: not its lock on
/// the ledger, not its sockets. Copies output to the log, waits, records the
/// status, and leaves. No allocation; this runs in a forked child.
unsafe fn keeper(a: &Raw, worker: i32, pipe: i32) -> ! {
    // Move the three descriptors it needs out of the way, then close the rest.
    let pipe = libc::fcntl(pipe, libc::F_DUPFD_CLOEXEC, 3);
    let log = libc::fcntl(a.log_fd, libc::F_DUPFD_CLOEXEC, 3);
    let status = libc::fcntl(a.status_fd, libc::F_DUPFD_CLOEXEC, 3);
    let mut held = [pipe, log, status];
    held.sort_unstable();
    if held[0] < 3 {
        libc::_exit(127);
    }
    let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR);
    for fd in 0..3 {
        if null >= 0 {
            libc::dup2(null, fd);
        }
    }
    // The setup pipe closes here. The daemon's read of it ends when the
    // worker's copy does, on exec or on failure.
    let mut from = 3u32;
    for fd in held {
        if fd as u32 > from {
            libc::syscall(libc::SYS_close_range, from, fd as u32 - 1, 0);
        }
        from = fd as u32 + 1;
    }
    libc::syscall(libc::SYS_close_range, from, u32::MAX, 0);
    let flags = libc::fcntl(pipe, libc::F_GETFL);
    libc::fcntl(pipe, libc::F_SETFL, flags | libc::O_NONBLOCK);

    let mut started: libc::timespec = std::mem::zeroed();
    libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut started);
    let mut written = 0u64;
    let mut buf = [0u8; 8192];
    let mut code = -1;
    let mut done = false;
    loop {
        loop {
            let n = libc::read(pipe, buf.as_mut_ptr().cast(), buf.len());
            if n <= 0 {
                break;
            }
            let room = a.cap.saturating_sub(written).min(n as u64) as usize;
            let mut put = 0;
            while put < room {
                let w = libc::write(log, buf[put..].as_ptr().cast(), room - put);
                if w <= 0 {
                    break;
                }
                put += w as usize;
            }
            written += room as u64;
        }
        if done {
            break;
        }
        if libc::waitpid(worker, &mut code, libc::WNOHANG) == worker {
            // One more pass for what it printed last.
            done = true;
            continue;
        }
        if a.deadline > 0 {
            let mut now: libc::timespec = std::mem::zeroed();
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now);
            if (now.tv_sec - started.tv_sec) as u64 > a.deadline {
                libc::kill(worker, libc::SIGKILL);
            }
        }
        let nap = libc::timespec {
            tv_sec: 0,
            tv_nsec: 20_000_000,
        };
        libc::nanosleep(&nap, ptr::null_mut());
    }
    let bytes = code.to_ne_bytes();
    libc::write(status, bytes.as_ptr().cast(), 4);
    libc::fsync(status);
    libc::_exit(if libc::WIFEXITED(code) {
        libc::WEXITSTATUS(code)
    } else {
        128 + libc::WTERMSIG(code)
    });
}

unsafe fn pivot(a: &Raw) -> i32 {
    if libc::mount(
        ptr::null(),
        c"/".as_ptr(),
        ptr::null(),
        libc::MS_REC | libc::MS_PRIVATE,
        ptr::null(),
    ) != 0
    {
        return errno();
    }
    if libc::mount(
        a.newroot,
        a.newroot,
        ptr::null(),
        libc::MS_BIND | libc::MS_REC,
        ptr::null(),
    ) != 0
    {
        return errno();
    }
    for bind in std::slice::from_raw_parts(a.binds, a.n_binds) {
        let rc = mount_bind(bind.src, bind.dst, bind.flags);
        if rc != 0 {
            return rc;
        }
    }
    if mount_bind(a.work, a.nr_work, 0) != 0 || mount_bind(a.scratch, a.nr_scratch, 0) != 0 {
        return errno();
    }
    // Registry is a copy taken at spawn, not a live bind, so a promotion
    // during this turn stays invisible until the next task.
    let _ = mount_bind(a.devnull, a.nr_null, 0);
    let _ = mount_bind(a.devzero, a.nr_zero, 0);
    let _ = mount_bind(a.devurandom, a.nr_urandom, 0);
    if libc::syscall(libc::SYS_pivot_root, a.newroot, a.nr_old) != 0 {
        return errno();
    }
    if libc::chdir(c"/".as_ptr()) != 0 {
        return errno();
    }
    if libc::umount2(c"/old".as_ptr(), libc::MNT_DETACH) != 0 {
        return errno();
    }
    let _ = libc::rmdir(c"/old".as_ptr());
    let _ = libc::mount(
        c"proc".as_ptr(),
        c"/proc".as_ptr(),
        c"proc".as_ptr(),
        0,
        ptr::null(),
    );
    let _ = libc::mount(
        c"tmpfs".as_ptr(),
        c"/tmp".as_ptr(),
        c"tmpfs".as_ptr(),
        0,
        c"size=64m,mode=1777".as_ptr().cast(),
    );
    0
}

/// `remount` is 0 for a writable bind, or the flags that make it read-only.
unsafe fn mount_bind(
    src: *const libc::c_char,
    dst: *const libc::c_char,
    remount: libc::c_ulong,
) -> i32 {
    if libc::mount(
        src,
        dst,
        ptr::null(),
        libc::MS_BIND | libc::MS_REC,
        ptr::null(),
    ) != 0
    {
        return errno();
    }
    if remount != 0 && libc::mount(ptr::null(), dst, ptr::null(), remount, ptr::null()) != 0 {
        return errno();
    }
    0
}

unsafe fn landlock(rules: &[Rule]) -> i32 {
    let mut attr = RulesetAttr {
        handled_access_fs: FS_ALL,
        handled_access_net: 0,
        scoped: SCOPE_SIGNAL,
    };
    let mut size = size_of::<RulesetAttr>();
    let mut rs = libc::syscall(libc::SYS_landlock_create_ruleset, &attr, size, 0);
    if rs < 0 {
        attr.scoped = 0;
        size = size_of::<u64>() * 2;
        rs = libc::syscall(libc::SYS_landlock_create_ruleset, &attr, size, 0);
    }
    if rs < 0 {
        return errno();
    }
    let mut added = 0;
    for (path, allow) in rules {
        let fd = libc::open(*path, libc::O_PATH | libc::O_CLOEXEC);
        if fd < 0 {
            continue;
        }
        let pb = PathBeneath {
            allowed_access: *allow & FS_ALL,
            parent_fd: fd,
        };
        let rc = libc::syscall(libc::SYS_landlock_add_rule, rs, 1i32, &pb, 0);
        libc::close(fd);
        if rc == 0 {
            added += 1;
        }
    }
    if added == 0 {
        libc::close(rs as i32);
        return libc::EPERM;
    }
    libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
    let rc = libc::syscall(libc::SYS_landlock_restrict_self, rs, 0);
    libc::close(rs as i32);
    if rc < 0 {
        errno()
    } else {
        0
    }
}

unsafe fn set_limits(memory_mb: u64, pids: u64, seconds: u64) {
    // The dynamic linker needs address space. Below 64MB the cap is a lie, so floor it.
    // RLIMIT_NPROC is per uid, not per worker. The cgroup pids controller is the real cap.
    let mem = memory_mb.saturating_mul(1024 * 1024).max(64 * 1024 * 1024);
    let as_lim = libc::rlimit {
        rlim_cur: mem,
        rlim_max: mem,
    };
    libc::setrlimit(libc::RLIMIT_AS, &as_lim);
    let nofile = libc::rlimit {
        rlim_cur: 1024,
        rlim_max: 1024,
    };
    libc::setrlimit(libc::RLIMIT_NOFILE, &nofile);
    let nproc = pids.max(1024);
    let plim = libc::rlimit {
        rlim_cur: nproc,
        rlim_max: nproc,
    };
    libc::setrlimit(libc::RLIMIT_NPROC, &plim);
    if seconds > 0 {
        let cpu = libc::rlimit {
            rlim_cur: seconds,
            rlim_max: seconds.saturating_add(1),
        };
        libc::setrlimit(libc::RLIMIT_CPU, &cpu);
    }
    libc::setpriority(libc::PRIO_PROCESS, 0, 10);
}

unsafe fn close_except(keep: i32) {
    if keep > 3 {
        libc::syscall(libc::SYS_close_range, 3, keep - 1, 0);
    }
    libc::syscall(libc::SYS_close_range, keep as u32 + 1, u32::MAX, 0);
}

unsafe fn fail(err_w: i32, code: i32) -> ! {
    let buf = code.to_ne_bytes();
    libc::write(err_w, buf.as_ptr().cast(), 4);
    libc::_exit(127);
}

unsafe fn errno() -> i32 {
    *libc::__errno_location()
}

fn write_id_maps(pid: i32) -> Result<()> {
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let outside_uid = if uid == 0 { 65534 } else { uid };
    let outside_gid = if gid == 0 { 65534 } else { gid };
    fs::write(format!("/proc/{pid}/setgroups"), "deny")?;
    fs::write(
        format!("/proc/{pid}/uid_map"),
        format!("0 {outside_uid} 1\n"),
    )?;
    fs::write(
        format!("/proc/{pid}/gid_map"),
        format!("0 {outside_gid} 1\n"),
    )?;
    Ok(())
}

struct OwnedFd(i32);

impl Drop for OwnedFd {
    fn drop(&mut self) {
        if self.0 >= 0 {
            unsafe { libc::close(self.0) };
            self.0 = -1;
        }
    }
}

impl OwnedFd {
    fn into_raw(mut self) -> i32 {
        let fd = self.0;
        self.0 = -1;
        fd
    }
}

fn pipe_cloexec() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(err("pipe"));
    }
    Ok((OwnedFd(fds[0]), OwnedFd(fds[1])))
}

fn read_full(fd: i32, buf: &mut [u8]) -> Result<()> {
    let mut got = 0;
    while got < buf.len() {
        let n = unsafe { libc::read(fd, buf[got..].as_mut_ptr().cast(), buf.len() - got) };
        if n == 0 {
            return Err(err("eof"));
        }
        if n < 0 {
            return Err(err("read"));
        }
        got += n as usize;
    }
    Ok(())
}

fn write_full(fd: i32, buf: &[u8]) -> Result<()> {
    let mut put = 0;
    while put < buf.len() {
        let n = unsafe { libc::write(fd, buf[put..].as_ptr().cast(), buf.len() - put) };
        if n < 0 {
            return Err(err("write"));
        }
        put += n as usize;
    }
    Ok(())
}

fn set_nonblock(fd: i32) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

struct Stack {
    ptr: *mut libc::c_void,
    len: usize,
}

impl Stack {
    fn top(&self) -> *mut libc::c_void {
        let top = unsafe { self.ptr.add(self.len) };
        (top as usize & !15) as *mut libc::c_void
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { libc::munmap(self.ptr, self.len) };
            self.ptr = ptr::null_mut();
        }
    }
}

fn map_stack() -> Result<Stack> {
    let len = 256 * 1024;
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(err("stack"));
    }
    Ok(Stack { ptr, len })
}

/// Run a verifier in a cell with no token and no proxy. Never falls back to the host.
pub fn run_sealed(cmd: &[String], work: &Path) -> i32 {
    let stamp = crate::id::now_ms();
    let scratch = std::env::temp_dir().join(format!("inlet-seal-s-{stamp}"));
    let root = std::env::temp_dir().join(format!("inlet-seal-r-{stamp}"));
    let registry = std::env::temp_dir().join(format!("inlet-seal-g-{stamp}"));
    let req = SpawnRequest {
        cmd: cmd.to_vec(),
        work: work.to_path_buf(),
        scratch: scratch.clone(),
        registry,
        root: root.clone(),
        proxy_sock: PathBuf::from("/dev/null"),
        worker_sock: PathBuf::from("/dev/null"),
        preamble: String::new(),
        net_none: true,
        memory_mb: 512,
        pids: 64,
        seconds: 8,
        token: String::new(),
        task_id: String::new(),
        cgroup: None,
        door: false,
        ..Default::default()
    };
    let spawned = match spawn(&req) {
        Ok(child) => child,
        Err(_) => {
            let _ = fs::remove_dir_all(&scratch);
            let _ = fs::remove_dir_all(&root);
            return 127;
        }
    };
    let started = Instant::now();
    let code = loop {
        let mut status = 0;
        let pid = unsafe { libc::waitpid(spawned.pid, &mut status, libc::WNOHANG) };
        if pid == spawned.pid {
            break if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else if libc::WIFSIGNALED(status) {
                128 + libc::WTERMSIG(status)
            } else {
                1
            };
        }
        drain_fd(spawned.stdout.as_raw_fd());
        if started.elapsed() >= Duration::from_secs(8) {
            unsafe {
                libc::kill(spawned.pid, libc::SIGKILL);
                libc::waitpid(spawned.pid, ptr::null_mut(), 0);
            }
            break 124;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let _ = fs::remove_dir_all(&scratch);
    let _ = fs::remove_dir_all(&root);
    code
}

fn drain_fd(fd: i32) {
    let mut buf = [0u8; 4096];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if n <= 0 {
            break;
        }
    }
}

fn c_path(path: &Path) -> Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| err("path contains a nul"))
}

fn c_strings(parts: &[String]) -> Result<Vec<CString>> {
    parts
        .iter()
        .map(|s| CString::new(s.as_str()).map_err(|_| err("string contains a nul")))
        .collect()
}

// Silence unused import if CStr is only used via c"..." macros.
#[allow(dead_code)]
fn _cstr(s: &CStr) -> &CStr {
    s
}
