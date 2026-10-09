//! Guided setup and later edits. Secrets stay out of the policy file.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

use crate::config::{self, Policy};
use crate::error::{err, Result};
use crate::paths;

const STEPS: u64 = 8;

struct Answers {
    upstream: String,
    key_env: String,
    max_tokens: u64,
    period: String,
    budget: u64,
    worker: String,
    cmd: Vec<String>,
    passphrase: Option<String>,
    telegram: Option<String>,
    discord: Option<String>,
}

pub fn init(home: &Path, args: &[String]) -> Result<()> {
    let mut smoke = true;
    let mut defaults = !io::stdout().is_terminal();
    for arg in args {
        match arg.as_str() {
            "--defaults" => defaults = true,
            "--no-smoke" => smoke = false,
            "--smoke" => smoke = true,
            other => return Err(err(format!("unknown flag {other}"))),
        }
    }
    prepare(home)?;
    if paths::policy(home).exists() {
        return pin_existing(home, defaults);
    }
    if defaults {
        let answers = answers_from_env()?;
        plain_init(home, &answers, smoke)
    } else {
        answers_from_tty(home, smoke)
    }
}

pub fn settings(home: &Path, args: &[String]) -> Result<()> {
    let policy = paths::policy(home);
    if !policy.exists() {
        return Err(err("run inlet init"));
    }
    let mut tokens = env_u64("INLET_TOKENS")?;
    let mut period = env_string("INLET_TOKEN_PERIOD");
    let mut upstream = env_string("INLET_UPSTREAM");
    let mut key_env = env_string("INLET_KEY_ENV");
    let mut worker = env_string("INLET_WORKER");
    let mut worker_cmd = env_string("INLET_WORKER_CMD");
    let mut budget = env_u64("INLET_BUDGET")?;
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let next = |i: &mut usize| -> Result<String> {
            *i += 1;
            args.get(*i)
                .cloned()
                .ok_or_else(|| err(format!("{arg} needs a value")))
        };
        match arg.as_str() {
            "--tokens" => tokens = Some(parse_tokens(&next(&mut i)?)?),
            "--period" => {
                let value = next(&mut i)?;
                config::parse_period(&value)?;
                period = Some(value);
            }
            "--upstream" => upstream = Some(check_upstream(&next(&mut i)?)?),
            "--key-env" => key_env = Some(check_env_name(&next(&mut i)?)?),
            "--worker" => worker = Some(check_worker(&next(&mut i)?)?),
            "--worker-cmd" => worker_cmd = Some(next(&mut i)?),
            "--budget" => budget = Some(parse_tokens(&next(&mut i)?)?),
            other => return Err(err(format!("unknown flag {other}"))),
        }
        i += 1;
    }
    let interactive = io::stdout().is_terminal()
        && tokens.is_none()
        && period.is_none()
        && upstream.is_none()
        && key_env.is_none()
        && worker.is_none()
        && worker_cmd.is_none()
        && budget.is_none()
        && env_string("INLET_TELEGRAM_TOKEN").is_none()
        && env_string("INLET_DISCORD_TOKEN").is_none();
    if interactive {
        print_view(home)?;
        loop {
            let field = ask("field", "")?;
            if field.is_empty() {
                break;
            }
            let value = if field == "telegram" || field == "discord" {
                ask_secret(&format!("{field} token"))?
            } else {
                ask("value", "")?
            };
            apply_field(home, &field, &value)?;
        }
        return print_view(home);
    }
    if let Some(value) = tokens {
        patch(home, &format!("caps.max_tokens = {value}"))?;
    }
    if let Some(value) = period {
        patch(home, &format!("caps.token_period = {}", lua_string(&value)))?;
    }
    if let Some(value) = upstream {
        patch(home, &format!("proxy.upstream = {}", lua_string(&value)))?;
    }
    if let Some(value) = key_env {
        patch(
            home,
            &format!("proxy.key = {}", lua_string(&format!("env:{value}"))),
        )?;
    }
    if let Some(value) = budget {
        patch(home, &format!("default_budget.tokens = {value}"))?;
    }
    if worker.is_some() || worker_cmd.is_some() {
        patch_worker(home, worker.as_deref(), worker_cmd.as_deref())?;
    }
    if let Some(token) = env_string("INLET_TELEGRAM_TOKEN") {
        write_token(&home.join("keys/telegram.token"), &token)?;
    }
    if let Some(token) = env_string("INLET_DISCORD_TOKEN") {
        write_token(&home.join("keys/discord.token"), &token)?;
    }
    print_view(home)
}

fn plain_init(home: &Path, answers: &Answers, smoke: bool) -> Result<()> {
    let cell = crate::cell::probe(home);
    println!("1/{STEPS} checking the cell");
    if cell {
        println!("cell ready");
    } else {
        println!("cell unavailable");
        print_cell_fix();
    }
    println!("2/{STEPS} model");
    println!("upstream {}", answers.upstream);
    println!("key env:{}", answers.key_env);
    println!("3/{STEPS} budgets");
    println!("max_tokens {}", answers.max_tokens);
    println!("token_period {}", answers.period);
    println!("4/{STEPS} worker");
    println!("worker {} {}", answers.worker, answers.cmd.join(" "));
    println!("5/{STEPS} passphrase");
    println!(
        "{}",
        if answers.passphrase.is_some() {
            "signed"
        } else {
            "unsigned"
        }
    );
    println!("6/{STEPS} bridges");
    println!(
        "telegram {}",
        if answers.telegram.is_some() {
            "set"
        } else {
            "unset"
        }
    );
    println!(
        "discord {}",
        if answers.discord.is_some() {
            "set"
        } else {
            "unset"
        }
    );
    println!("7/{STEPS} writing the policy");
    println!("8/{STEPS} starting the daemon");
    finish(home, answers, smoke, None)
}

fn answers_from_env() -> Result<Answers> {
    let upstream = check_upstream(
        &env_string("INLET_UPSTREAM").unwrap_or_else(|| "env:MODEL_UPSTREAM".into()),
    )?;
    let key_env =
        check_env_name(&env_string("INLET_KEY_ENV").unwrap_or_else(|| "MODEL_API_KEY".into()))?;
    let max_tokens = env_u64("INLET_TOKENS")?.unwrap_or(2_000_000);
    let period = env_string("INLET_TOKEN_PERIOD").unwrap_or_else(|| "1d".into());
    config::parse_period(&period)?;
    let budget = env_u64("INLET_BUDGET")?.unwrap_or(200_000);
    let worker = check_worker(&env_string("INLET_WORKER").unwrap_or_else(|| "check".into()))?;
    let cmd = split_cmd(&env_string("INLET_WORKER_CMD").unwrap_or_else(|| "/bin/true".into()))?;
    let passphrase = match env_string("INLET_PASSPHRASE") {
        Some(value) if value.len() < 4 => return Err(err("passphrase too short")),
        Some(value) => Some(value),
        None => None,
    };
    Ok(Answers {
        upstream,
        key_env,
        max_tokens,
        period,
        budget,
        worker,
        cmd,
        passphrase,
        telegram: env_string("INLET_TELEGRAM_TOKEN"),
        discord: env_string("INLET_DISCORD_TOKEN"),
    })
}

fn answers_from_tty(home: &Path, smoke: bool) -> Result<()> {
    let bar = ProgressBar::new(STEPS);
    bar.set_style(
        ProgressStyle::with_template("{spinner} {pos}/{len} {msg}").map_err(|_| err("progress"))?,
    );
    bar.enable_steady_tick(Duration::from_millis(80));
    bar.set_message("checking the cell");
    let cell = crate::cell::probe(home);
    if cell {
        bar.set_message("cell ready");
    } else {
        bar.set_message("cell unavailable");
        bar.suspend(print_cell_fix);
    }
    bar.set_position(1);
    bar.set_message("model");
    let upstream = bar.suspend(|| {
        let raw = ask("upstream", "env:MODEL_UPSTREAM")?;
        check_upstream(&raw)
    })?;
    let key_env = bar.suspend(|| {
        let raw = ask("key env", "MODEL_API_KEY")?;
        check_env_name(&raw)
    })?;
    bar.set_position(2);
    bar.set_message("budgets");
    let max_tokens = bar.suspend(|| {
        let raw = ask("max tokens", "2000000")?;
        parse_tokens(&raw)
    })?;
    let period = bar.suspend(|| {
        let raw = ask("token period", "1d")?;
        config::parse_period(&raw)?;
        Ok::<_, crate::error::Error>(raw)
    })?;
    let budget = bar.suspend(|| {
        let raw = ask("budget tokens", "200000")?;
        parse_tokens(&raw)
    })?;
    bar.set_position(3);
    bar.set_message("worker");
    let worker = bar.suspend(|| {
        let raw = ask("worker", "check")?;
        check_worker(&raw)
    })?;
    let cmd = bar.suspend(|| {
        let raw = ask("command", "/bin/true")?;
        split_cmd(&raw)
    })?;
    bar.set_position(4);
    bar.set_message("passphrase");
    let passphrase = bar.suspend(|| ask_secret("passphrase"))?;
    let passphrase = match passphrase {
        value if value.is_empty() => None,
        value if value.len() < 4 => return Err(err("passphrase too short")),
        value => Some(value),
    };
    bar.set_position(5);
    bar.set_message("bridges");
    let telegram = bar.suspend(|| ask_secret("telegram token"))?;
    let discord = bar.suspend(|| ask_secret("discord token"))?;
    bar.set_position(6);
    bar.set_message("writing the policy");
    let answers = Answers {
        upstream,
        key_env,
        max_tokens,
        period,
        budget,
        worker,
        cmd,
        passphrase,
        telegram: none_if_empty(telegram),
        discord: none_if_empty(discord),
    };
    finish(home, &answers, smoke, Some(bar))
}

fn finish(home: &Path, answers: &Answers, smoke: bool, bar: Option<ProgressBar>) -> Result<()> {
    let body = render(answers);
    Policy::parse(&body)?;
    let policy = paths::policy(home);
    if !policy.exists() {
        write_secret(&policy, body.as_bytes())?;
    }
    if let Some(pass) = &answers.passphrase {
        if !paths::key_pub(home).exists() {
            let (public, wrapped) = crate::sign::generate(pass)?;
            let bytes = fs::read(&policy)?;
            let sig = crate::sign::sign_with(&wrapped, pass, &bytes)?;
            write_secret(&paths::key_pub(home), &public)?;
            write_secret(&paths::key_priv(home), &wrapped)?;
            write_secret(&paths::policy_sig(home), &sig)?;
        }
    }
    if let Some(token) = &answers.telegram {
        write_token(&home.join("keys/telegram.token"), token)?;
    }
    if let Some(token) = &answers.discord {
        write_token(&home.join("keys/discord.token"), token)?;
    }
    if let Some(bar) = &bar {
        bar.set_position(7);
        bar.set_message("starting the daemon");
    }
    start_daemon(home)?;
    if smoke {
        if let Some(bar) = &bar {
            bar.set_message("smoke");
        } else {
            println!("smoke");
        }
        smoke_task(home, &answers.worker)?;
        if bar.is_none() {
            println!("smoke ok");
        }
    }
    if let Some(bar) = bar {
        bar.finish_with_message("ready");
    }
    Ok(())
}

fn render(answers: &Answers) -> String {
    let cmd = answers
        .cmd
        .iter()
        .map(|part| lua_string(part))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"caps = {{
  max_live = 8,
  max_depth = 3,
  max_tokens = {tokens},
  max_memory_mb = 8192,
  max_pids = 64,
  token_period = {period},
}}
setup = "box"
isolator = "cgroup"
human_weight = 4
min_ev = 0
value = 400000
unattended = false
debug = 1
rollup = "count"
default_budget = {{ tokens = {budget}, seconds = 3600, memory_mb = 1024, pids = 8 }}
decision = {{
  kind = "off",
  endpoint = "env:DECISION_ENDPOINT",
  model = "gpt-6-luna",
  timeout_ms = 800,
  purse_tokens = 50000,
}}
proxy = {{
  upstream = {upstream},
  key = {key},
}}
workers = {{
  {worker} = {{ cmd = {{ {cmd} }}, tags = {{ "code" }}, net = "host", on_crash = "fail" }},
}}
function admit(ctx)
  if ctx.tags.math and ctx.depth > 1 then return "deny" end
  return "allow"
end
"#,
        tokens = answers.max_tokens,
        period = lua_string(&answers.period),
        budget = answers.budget,
        upstream = lua_string(&answers.upstream),
        key = lua_string(&format!("env:{}", answers.key_env)),
        worker = answers.worker,
    )
}

fn start_daemon(home: &Path) -> Result<()> {
    if daemon_up(home) {
        println!("daemon is up");
        return Ok(());
    }
    let log_path = home.join("run/init.log");
    let log = File::create(&log_path)?;
    let err_log = log.try_clone()?;
    let exe = std::env::current_exe().map_err(|_| err("inlet"))?;
    let child = Command::new(exe)
        .args([
            "--home",
            home.to_str().ok_or_else(|| err("home"))?,
            "up",
            "-f",
        ])
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err_log))
        .spawn()?;
    let want = child.id() as i32;
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        if daemon_up(home) && pid_of(home) == Some(want) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    let text = fs::read_to_string(&log_path).unwrap_or_default();
    Err(err(format!("daemon did not come up: {text}")))
}

fn smoke_task(home: &Path, worker: &str) -> Result<()> {
    let reply = crate::proto::rpc(
        home,
        serde_json::json!({
            "op": "add",
            "worker": worker,
            "goal": "smoke",
            "no_verify": true,
            "tokens": 20,
            "seconds": 15,
        }),
    )?;
    if reply.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
        return Err(err(reply
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("smoke")));
    }
    let deadline = Instant::now() + Duration::from_secs(12);
    while Instant::now() < deadline {
        let status = crate::proto::rpc(home, serde_json::json!({"op":"status"}))?;
        let done = status
            .get("tasks")
            .and_then(|tasks| tasks.as_array())
            .map(|tasks| {
                tasks.iter().any(|task| {
                    task.get("goal").and_then(|g| g.as_str()) == Some("smoke")
                        && task.get("state").and_then(|s| s.as_str()) == Some("done")
                })
            })
            .unwrap_or(false);
        if done {
            return Ok(());
        }
        let failed = status
            .get("tasks")
            .and_then(|tasks| tasks.as_array())
            .map(|tasks| {
                tasks.iter().any(|task| {
                    task.get("goal").and_then(|g| g.as_str()) == Some("smoke")
                        && task.get("state").and_then(|s| s.as_str()) == Some("failed")
                })
            })
            .unwrap_or(false);
        if failed {
            return Err(err("smoke failed"));
        }
        thread::sleep(Duration::from_millis(40));
    }
    Err(err("smoke timed out"))
}

fn print_view(home: &Path) -> Result<()> {
    let src = fs::read_to_string(paths::policy(home))?;
    let policy = Policy::parse(&src)?;
    let cfg = &policy.cfg;
    row("setup", &cfg.setup);
    row("isolator", isolator_name(cfg.isolator));
    if crate::cell::probe(home) {
        row("cell", "ready");
    } else {
        row("cell", "unavailable");
        print_cell_fix();
    }
    row("max_tokens", &cfg.caps.max_tokens.to_string());
    row(
        "token_period",
        &last_quoted(&src, "token_period").unwrap_or_else(|| "1d".into()),
    );
    row("budget", &cfg.default_budget.tokens.to_string());
    row(
        "proxy",
        &last_quoted(&src, "upstream").unwrap_or_else(|| "unset".into()),
    );
    row(
        "key",
        &last_quoted(&src, "key").unwrap_or_else(|| "unset".into()),
    );
    let workers = cfg.workers.keys().cloned().collect::<Vec<_>>().join(" ");
    row(
        "workers",
        if workers.is_empty() { "none" } else { &workers },
    );
    row(
        "policy",
        if paths::policy_sig(home).exists() {
            "signed"
        } else {
            "unsigned"
        },
    );
    row("rollup", cfg.rollup.as_str());
    row("telegram", token_state(&home.join("keys/telegram.token")));
    row("discord", token_state(&home.join("keys/discord.token")));
    Ok(())
}

fn isolator_name(kind: config::Isolator) -> &'static str {
    match kind {
        config::Isolator::Cgroup => "cgroup",
        config::Isolator::Rlimit => "rlimit",
        config::Isolator::Slurm => "slurm",
    }
}

fn row(name: &str, value: &str) {
    println!("{name:<14}{value}");
}

fn print_cell_fix() {
    println!("kernel.apparmor_restrict_unprivileged_userns=0");
    println!("kernel.unprivileged_userns_clone=1");
}

fn token_state(path: &Path) -> &'static str {
    match fs::metadata(path) {
        Ok(meta) => {
            let mode = std::os::unix::fs::PermissionsExt::mode(&meta.permissions()) & 0o777;
            if mode & 0o077 != 0 {
                "loose"
            } else if meta.len() == 0 {
                "unset"
            } else {
                "set"
            }
        }
        Err(_) => "unset",
    }
}

fn apply_field(home: &Path, field: &str, value: &str) -> Result<()> {
    match field {
        "max_tokens" => patch(home, &format!("caps.max_tokens = {}", parse_tokens(value)?)),
        "token_period" => {
            config::parse_period(value)?;
            patch(home, &format!("caps.token_period = {}", lua_string(value)))
        }
        "upstream" => patch(
            home,
            &format!("proxy.upstream = {}", lua_string(&check_upstream(value)?)),
        ),
        "key" => patch(
            home,
            &format!(
                "proxy.key = {}",
                lua_string(&format!("env:{}", check_env_name(value)?))
            ),
        ),
        "budget" => patch(
            home,
            &format!("default_budget.tokens = {}", parse_tokens(value)?),
        ),
        "worker" => patch_worker(home, Some(&check_worker(value)?), None),
        "command" => patch_worker(home, None, Some(value)),
        "telegram" => write_token(&home.join("keys/telegram.token"), value),
        "discord" => write_token(&home.join("keys/discord.token"), value),
        other => Err(err(format!("unknown field {other}"))),
    }
}

fn patch(home: &Path, line: &str) -> Result<()> {
    let path = paths::policy(home);
    let src = fs::read_to_string(&path)?;
    let next = apply_line(&src, line);
    Policy::parse(&next)?;
    store_policy(home, &next)
}

fn patch_worker(home: &Path, name: Option<&str>, cmd: Option<&str>) -> Result<()> {
    let path = paths::policy(home);
    let src = fs::read_to_string(&path)?;
    let policy = Policy::parse(&src)?;
    let name = match name {
        Some(name) => name.to_string(),
        None => policy
            .cfg
            .workers
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| err("worker needs a name"))?,
    };
    check_worker(&name)?;
    let cmd = if let Some(cmd) = cmd {
        split_cmd(cmd)?
    } else {
        policy
            .cfg
            .workers
            .get(&name)
            .map(|worker| worker.cmd.clone())
            .unwrap_or_else(|| vec!["/bin/true".into()])
    };
    let rendered = cmd
        .iter()
        .map(|part| lua_string(part))
        .collect::<Vec<_>>()
        .join(", ");
    let line = format!(
        "workers.{name} = {{ cmd = {{ {rendered} }}, tags = {{ \"code\" }}, net = \"host\", on_crash = \"fail\" }}"
    );
    let next = apply_line(&src, &line);
    Policy::parse(&next)?;
    store_policy(home, &next)
}

fn apply_line(src: &str, line: &str) -> String {
    let (base, mut have) = split_trailer(src);
    if let Some((key, value)) = line.split_once('=') {
        have.insert(key.trim().to_string(), value.trim().to_string());
    }
    let mut out = base;
    out.push_str("\n\n-- inlet settings\n");
    for (key, value) in have {
        out.push_str(&format!("{key} = {value}\n"));
    }
    out
}

fn split_trailer(src: &str) -> (String, BTreeMap<String, String>) {
    let mut have = BTreeMap::new();
    let base = match src.find("-- inlet settings") {
        Some(at) => {
            for line in src[at..].lines().skip(1) {
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    if !key.is_empty() && !key.starts_with('-') {
                        have.insert(key.to_string(), value.trim().to_string());
                    }
                }
            }
            src[..at].trim_end().to_string()
        }
        None => src.trim_end().to_string(),
    };
    (base, have)
}

fn store_policy(home: &Path, body: &str) -> Result<()> {
    let signed = paths::policy_sig(home).exists();
    if !signed {
        write_secret(&paths::policy(home), body.as_bytes())?;
        return Ok(());
    }
    let pass = passphrase()?;
    let wrapped = fs::read(paths::key_priv(home)).map_err(|_| err("no signing key"))?;
    let sig = crate::sign::sign_with(&wrapped, &pass, body.as_bytes())?;
    if daemon_up(home) {
        fs::write(paths::policy_draft(home), body)?;
        let reply = crate::proto::rpc(
            home,
            serde_json::json!({"op":"sign","sig": crate::sign::hex_encode(&sig)}),
        )?;
        if reply.get("ok").and_then(|ok| ok.as_bool()) == Some(false) {
            return Err(err(reply
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("sign")));
        }
        Ok(())
    } else {
        write_secret(&paths::policy(home), body.as_bytes())?;
        write_secret(&paths::policy_sig(home), &sig)?;
        Ok(())
    }
}

fn passphrase() -> Result<String> {
    if let Some(value) = env_string("INLET_PASSPHRASE") {
        if value.len() < 4 {
            return Err(err("passphrase too short"));
        }
        return Ok(value);
    }
    if !io::stdout().is_terminal() {
        return Err(err("passphrase is read from /dev/tty"));
    }
    let value = ask_secret("passphrase")?;
    if value.len() < 4 {
        return Err(err("passphrase too short"));
    }
    Ok(value)
}

fn pin_existing(home: &Path, defaults: bool) -> Result<()> {
    if paths::key_pub(home).exists() {
        println!("signing key already pinned");
        return Ok(());
    }
    let pass = if defaults {
        env_string("INLET_PASSPHRASE").ok_or_else(|| err("passphrase is read from /dev/tty"))?
    } else {
        crate::sign::read_passphrase("passphrase: ")?
    };
    if pass.len() < 4 {
        return Err(err("passphrase too short"));
    }
    let body = fs::read(paths::policy(home))?;
    let (public, wrapped) = crate::sign::generate(&pass)?;
    let sig = crate::sign::sign_with(&wrapped, &pass, &body)?;
    write_secret(&paths::key_pub(home), &public)?;
    write_secret(&paths::key_priv(home), &wrapped)?;
    write_secret(&paths::policy_sig(home), &sig)?;
    println!("pinned signing key");
    Ok(())
}

fn prepare(home: &Path) -> Result<()> {
    for dir in ["ledger", "registry", "run", "keys", "work", "scratch"] {
        fs::create_dir_all(home.join(dir))?;
    }
    Ok(())
}

fn daemon_up(home: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(paths::operator_sock(home)).is_ok()
}

fn pid_of(home: &Path) -> Option<i32> {
    fs::read_to_string(paths::pid_file(home))
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

fn write_token(path: &Path, token: &str) -> Result<()> {
    if token.is_empty() {
        let _ = fs::remove_file(path);
        return Ok(());
    }
    if token.chars().any(|c| c.is_whitespace()) {
        return Err(err("token"));
    }
    write_secret(path, token.as_bytes())
}

fn write_secret(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn ask(label: &str, default: &str) -> Result<String> {
    let mut tty = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .map_err(|_| err("no tty"))?;
    if default.is_empty() {
        writeln!(tty, "{label}")?;
    } else {
        writeln!(tty, "{label} [{default}]")?;
    }
    tty.flush()?;
    let mut line = String::new();
    BufReader::new(tty).read_line(&mut line)?;
    let line = line.trim();
    if line.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(line.to_string())
    }
}

fn ask_secret(label: &str) -> Result<String> {
    let mut tty = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .map_err(|_| err("no tty"))?;
    let fd = std::os::fd::AsRawFd::as_raw_fd(&tty);
    let mut saved: libc::termios = unsafe { std::mem::zeroed() };
    let echoed = unsafe { libc::tcgetattr(fd, &mut saved) } == 0;
    if echoed {
        let mut hidden = saved;
        hidden.c_lflag &= !libc::ECHO;
        unsafe {
            libc::tcsetattr(fd, libc::TCSANOW, &hidden);
        }
    }
    writeln!(tty, "{label}")?;
    tty.flush()?;
    let mut line = String::new();
    let mut reader = BufReader::new(tty.try_clone()?);
    reader.read_line(&mut line)?;
    if echoed {
        unsafe {
            libc::tcsetattr(fd, libc::TCSANOW, &saved);
        }
        let _ = writeln!(tty);
    }
    Ok(line.trim().to_string())
}

fn none_if_empty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_u64(name: &str) -> Result<Option<u64>> {
    match env_string(name) {
        Some(value) => Ok(Some(parse_tokens(&value)?)),
        None => Ok(None),
    }
}

fn parse_tokens(raw: &str) -> Result<u64> {
    let n: u64 = raw
        .trim()
        .parse()
        .map_err(|_| err(format!("bad number {raw}")))?;
    if n == 0 {
        Err(err("token budget is empty"))
    } else {
        Ok(n)
    }
}

fn check_upstream(raw: &str) -> Result<String> {
    let raw = raw.trim();
    if let Some(name) = raw.strip_prefix("env:") {
        check_env_name(name)?;
        return Ok(raw.to_string());
    }
    if raw.starts_with("https://") || raw.starts_with("http://") {
        return Ok(raw.to_string());
    }
    Err(err("upstream is a url or env:NAME"))
}

fn check_env_name(raw: &str) -> Result<String> {
    let raw = raw.trim().strip_prefix("env:").unwrap_or(raw.trim());
    let ok = !raw.is_empty()
        && raw
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && raw.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !raw.starts_with("sk-");
    if ok {
        Ok(raw.to_string())
    } else {
        Err(err("key stays in the environment"))
    }
}

fn check_worker(raw: &str) -> Result<String> {
    let raw = raw.trim();
    let ok = !raw.is_empty()
        && raw.len() <= 32
        && raw.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && raw
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok {
        Ok(raw.to_string())
    } else {
        Err(err("bad worker name"))
    }
}

fn split_cmd(raw: &str) -> Result<Vec<String>> {
    let cmd: Vec<String> = raw.split_whitespace().map(str::to_string).collect();
    if cmd.is_empty() {
        Err(err("worker cmd is required"))
    } else {
        Ok(cmd)
    }
}

fn lua_string(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn last_quoted(src: &str, key: &str) -> Option<String> {
    let mut found = None;
    let bytes = src.as_bytes();
    let mut i = 0;
    while let Some(at) = src[i..].find(key) {
        let abs = i + at;
        let prev_ok = abs == 0 || !bytes[abs - 1].is_ascii_alphanumeric();
        let after = src[abs + key.len()..].trim_start();
        if prev_ok {
            if let Some(value) = after.strip_prefix('=') {
                if let Some(text) = quoted(value.trim_start()) {
                    found = Some(text);
                }
            }
        }
        i = abs + key.len();
    }
    found
}

fn quoted(value: &str) -> Option<String> {
    let value = value.trim();
    if !value.starts_with('"') {
        return None;
    }
    let mut out = String::new();
    let mut chars = value[1..].chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else if c == '"' {
            return Some(out);
        } else {
            out.push(c);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_settings_line_overrides_the_cap() {
        let src = "caps = { max_tokens = 10, token_period = \"1d\" }\nproxy = { upstream = \"env:MODEL_UPSTREAM\", key = \"env:MODEL_API_KEY\" }\n";
        let next = apply_line(src, "caps.max_tokens = 1234");
        let policy = Policy::parse(&next).unwrap();
        assert_eq!(policy.cfg.caps.max_tokens, 1234);
        assert_eq!(
            last_quoted(&next, "upstream").as_deref(),
            Some("env:MODEL_UPSTREAM")
        );
        assert!(check_upstream("sk-live").is_err());
        assert!(check_env_name("sk-live").is_err());
        assert_eq!(
            check_env_name("env:MODEL_API_KEY").unwrap(),
            "MODEL_API_KEY"
        );
    }
}
