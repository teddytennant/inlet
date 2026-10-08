use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io::Write;
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::chown;
use std::path::{Path, PathBuf};
use std::ptr;

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
}

#[repr(C, packed)]
struct PathBeneath {
    allowed_access: u64,
    parent_fd: i32,
}

pub struct SpawnRequest {
    pub cmd: Vec<String>,
    pub work: PathBuf,
    pub scratch: PathBuf,
    pub registry: PathBuf,
    pub root: PathBuf,
    pub proxy_sock: PathBuf,
    pub net_none: bool,
    pub memory_mb: u64,
    pub pids: u64,
    pub seconds: u64,
    pub token: String,
    pub task_id: String,
    pub cgroup: Option<PathBuf>,
}

pub struct Spawned {
    pub pid: i32,
    pub stdout: File,
}

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
    registry: *const libc::c_char,
    proxy: *const libc::c_char,
    usr: *const libc::c_char,
    devnull: *const libc::c_char,
    devzero: *const libc::c_char,
    devurandom: *const libc::c_char,
    nr_usr: *const libc::c_char,
    nr_work: *const libc::c_char,
    nr_scratch: *const libc::c_char,
    nr_registry: *const libc::c_char,
    nr_null: *const libc::c_char,
    nr_zero: *const libc::c_char,
    nr_urandom: *const libc::c_char,
    nr_sock: *const libc::c_char,
    nr_old: *const libc::c_char,
    argv: *const *const libc::c_char,
    envp: *const *const libc::c_char,
    memory_mb: u64,
    pids: u64,
    seconds: u64,
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
    let true_bin = if Path::new("/bin/true").exists() {
        "/bin/true"
    } else {
        "/usr/bin/true"
    };
    let req = SpawnRequest {
        cmd: vec![true_bin.into()],
        work,
        scratch,
        registry,
        root,
        proxy_sock: proxy,
        net_none: true,
        memory_mb: 64,
        pids: 32,
        seconds: 5,
        token: "probe".into(),
        task_id: "probe".into(),
        cgroup: None,
    };
    match spawn(&req) {
        Ok(child) => {
            let mut status = 0;
            let pid = unsafe { libc::waitpid(child.pid, &mut status, 0) };
            pid == child.pid && libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0
        }
        Err(_) => false,
    }
}

pub fn spawn(req: &SpawnRequest) -> Result<Spawned> {
    if req.cmd.is_empty() {
        return Err(err("empty worker command"));
    }
    fs::create_dir_all(&req.work)?;
    fs::create_dir_all(&req.scratch)?;
    fs::create_dir_all(&req.registry)?;
    prepare_root(&req.root)?;
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
    let registry = c(&req.registry)?;
    let proxy = c(&req.proxy_sock)?;
    let usr = c_path(Path::new("/usr"))?;
    let devnull = c_path(Path::new("/dev/null"))?;
    let devzero = c_path(Path::new("/dev/zero"))?;
    let devurandom = c_path(Path::new("/dev/urandom"))?;
    let join = |name: &str| c_path(&req.root.join(name));
    let nr_usr = join("usr")?;
    let nr_work = join("work")?;
    let nr_scratch = join("scratch")?;
    let nr_registry = join("registry")?;
    let nr_null = join("dev/null")?;
    let nr_zero = join("dev/zero")?;
    let nr_urandom = join("dev/urandom")?;
    let nr_sock = join("run/proxy.sock")?;
    let nr_old = join("old")?;

    let argv_c = c_strings(&req.cmd)?;
    let mut argv: Vec<*const libc::c_char> = argv_c.iter().map(|s| s.as_ptr()).collect();
    argv.push(ptr::null());
    let env_owned = env_strings(&req.token, &req.task_id);
    let env_c = c_strings(&env_owned)?;
    let mut envp: Vec<*const libc::c_char> = env_c.iter().map(|s| s.as_ptr()).collect();
    envp.push(ptr::null());

    let raw = Raw {
        parent: unsafe { libc::getpid() },
        ready_w: ready_w.0,
        ack_r: ack_r.0,
        err_w: err_w.0,
        out_w: out_w.0,
        net_none: req.net_none,
        newroot: newroot.as_ptr(),
        work: work.as_ptr(),
        scratch: scratch.as_ptr(),
        registry: registry.as_ptr(),
        proxy: proxy.as_ptr(),
        usr: usr.as_ptr(),
        devnull: devnull.as_ptr(),
        devzero: devzero.as_ptr(),
        devurandom: devurandom.as_ptr(),
        nr_usr: nr_usr.as_ptr(),
        nr_work: nr_work.as_ptr(),
        nr_scratch: nr_scratch.as_ptr(),
        nr_registry: nr_registry.as_ptr(),
        nr_null: nr_null.as_ptr(),
        nr_zero: nr_zero.as_ptr(),
        nr_urandom: nr_urandom.as_ptr(),
        nr_sock: nr_sock.as_ptr(),
        nr_old: nr_old.as_ptr(),
        argv: argv.as_ptr(),
        envp: envp.as_ptr(),
        memory_mb: req.memory_mb,
        pids: req.pids,
        seconds: req.seconds,
    };

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(err("fork"));
    }
    if pid == 0 {
        // SAFETY: child uses only libc until exec or _exit. No allocator.
        unsafe { child_entry(&raw) }
    }

    drop(ready_w);
    drop(ack_r);
    drop(err_w);
    drop(out_w);

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

fn prepare_root(root: &Path) -> Result<()> {
    let _ = fs::remove_dir_all(root);
    for rel in [
        "usr", "work", "scratch", "registry", "proc", "dev", "tmp", "etc", "run", "old",
    ] {
        fs::create_dir_all(root.join(rel))?;
    }
    for rel in ["dev/null", "dev/zero", "dev/urandom", "run/proxy.sock"] {
        File::create(root.join(rel))?;
    }
    for (link, target) in [
        ("bin", "usr/bin"),
        ("lib", "usr/lib"),
        ("lib64", "usr/lib64"),
        ("sbin", "usr/sbin"),
    ] {
        let path = root.join(link);
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
    Ok(())
}

fn env_strings(token: &str, id: &str) -> Vec<String> {
    vec![
        format!("INLET_TOKEN={token}"),
        format!("INLET_ID={id}"),
        "INLET_PROXY_SOCK=/run/proxy.sock".into(),
        format!("OPENAI_API_KEY={token}"),
        "HOME=/work".into(),
        "PATH=/usr/bin:/bin".into(),
        "TMPDIR=/tmp".into(),
        "LANG=C".into(),
    ]
}

/// Child side. Never returns to Rust.
unsafe fn child_entry(a: &Raw) -> ! {
    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
    if libc::getppid() != a.parent {
        libc::raise(libc::SIGKILL);
    }
    libc::setpgid(0, 0);
    if libc::unshare(libc::CLONE_NEWUSER) != 0 {
        fail(a.err_w, errno());
    }
    let _ = libc::write(a.ready_w, [1u8].as_ptr().cast(), 1);
    let mut ack = [0u8; 1];
    if libc::read(a.ack_r, ack.as_mut_ptr().cast(), 1) != 1 {
        fail(a.err_w, errno());
    }
    let mut flags = libc::CLONE_NEWNS;
    if a.net_none {
        flags |= libc::CLONE_NEWNET;
    }
    if libc::unshare(flags) != 0 {
        fail(a.err_w, errno());
    }

    let pivoted = pivot(a) == 0;
    let locked = if pivoted {
        landlock_pivoted()
    } else {
        landlock_host(a)
    };
    if locked != 0 {
        fail(a.err_w, locked);
    }
    set_limits(a.memory_mb, a.pids, a.seconds);
    let _ = libc::chdir(if pivoted { c"/work".as_ptr() } else { a.work });

    let devnull = libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY);
    if devnull >= 0 {
        libc::dup2(devnull, 0);
        libc::close(devnull);
    }
    libc::dup2(a.out_w, 1);
    libc::dup2(a.out_w, 2);
    close_except(a.err_w);
    libc::execve(a.argv.read(), a.argv, a.envp);
    fail(a.err_w, errno());
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
    if mount_bind(a.usr, a.nr_usr, true) != 0
        || mount_bind(a.work, a.nr_work, false) != 0
        || mount_bind(a.scratch, a.nr_scratch, false) != 0
    {
        return errno();
    }
    let _ = mount_bind(a.registry, a.nr_registry, true);
    let _ = mount_bind(a.devnull, a.nr_null, false);
    let _ = mount_bind(a.devzero, a.nr_zero, false);
    let _ = mount_bind(a.devurandom, a.nr_urandom, false);
    let _ = mount_bind(a.proxy, a.nr_sock, false);
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

unsafe fn mount_bind(src: *const libc::c_char, dst: *const libc::c_char, ro: bool) -> i32 {
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
    if ro
        && libc::mount(
            ptr::null(),
            dst,
            ptr::null(),
            libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY | libc::MS_REC,
            ptr::null(),
        ) != 0
    {
        return errno();
    }
    0
}

unsafe fn landlock_pivoted() -> i32 {
    let rules = [
        (c"/usr".as_ptr(), FS_READ),
        (c"/work".as_ptr(), FS_ALL),
        (c"/scratch".as_ptr(), FS_ALL),
        (c"/registry".as_ptr(), FS_READ),
        (c"/proc".as_ptr(), FS_READ),
        (c"/dev".as_ptr(), FS_ALL),
        (c"/tmp".as_ptr(), FS_ALL),
        (c"/etc".as_ptr(), FS_READ),
        (c"/run".as_ptr(), FS_ALL),
    ];
    landlock(&rules)
}

unsafe fn landlock_host(a: &Raw) -> i32 {
    let rules = [
        (a.usr, FS_READ),
        (a.work, FS_ALL),
        (a.scratch, FS_ALL),
        (a.registry, FS_READ),
        (c"/proc".as_ptr(), FS_READ),
        (c"/dev".as_ptr(), FS_ALL),
        (c"/etc".as_ptr(), FS_READ),
        (c"/tmp".as_ptr(), FS_ALL),
        (a.proxy, FS_ALL),
    ];
    landlock(&rules)
}

unsafe fn landlock(rules: &[(*const libc::c_char, u64)]) -> i32 {
    let attr = RulesetAttr {
        handled_access_fs: FS_ALL,
        handled_access_net: 0,
    };
    let rs = libc::syscall(
        libc::SYS_landlock_create_ruleset,
        &attr,
        size_of::<RulesetAttr>(),
        0,
    );
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
