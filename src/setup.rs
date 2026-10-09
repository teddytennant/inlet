//! Guided setup and later edits. Secrets stay out of the policy file.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, IsTerminal, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crossterm::cursor::MoveToPreviousLine;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType};
use crossterm::ExecutableCommand;

use crate::config::{self, Policy};
use crate::error::{err, Result};
use crate::paths;
use crate::spin::{self, Sink};

const STEPS: u64 = 8;

struct Answers {
    upstream: String,
    key_env: String,
    max_tokens: u64,
    period: String,
    budget: u64,
    worker: String,
    cmd: Vec<String>,
    tags: Vec<String>,
    net: String,
    on_crash: String,
    passphrase: Option<String>,
    telegram: Option<String>,
    discord: Option<String>,
}

struct WorkerPreset {
    name: &'static str,
    cmd: &'static [&'static str],
    tags: &'static [&'static str],
    net: &'static str,
    on_crash: &'static str,
}

const UPSTREAMS: &[(&str, &str)] = &[
    ("OpenAI-compatible", "https://api.openai.com/v1"),
    ("xAI", "https://api.x.ai/v1"),
    ("OpenRouter", "https://openrouter.ai/api/v1"),
    ("local", "http://127.0.0.1:8080/v1"),
    ("environment", "env:MODEL_UPSTREAM"),
];

const KEYS: &[(&str, &str)] = &[
    ("MODEL_API_KEY", "MODEL_API_KEY"),
    ("OPENAI_API_KEY", "OPENAI_API_KEY"),
    ("XAI_API_KEY", "XAI_API_KEY"),
    ("OPENROUTER_API_KEY", "OPENROUTER_API_KEY"),
];

const WORKER_CHOICES: &[(&str, &str)] = &[
    ("check", "/bin/true"),
    ("sleeper", "/bin/sleep 60"),
    ("pi", "pi --mode rpc"),
    ("prover", "prover"),
];

const WORKER_PRESETS: &[WorkerPreset] = &[
    WorkerPreset {
        name: "check",
        cmd: &["/bin/true"],
        tags: &["code"],
        net: "host",
        on_crash: "fail",
    },
    WorkerPreset {
        name: "sleeper",
        cmd: &["/bin/sleep", "60"],
        tags: &["code"],
        net: "host",
        on_crash: "fail",
    },
    WorkerPreset {
        name: "pi",
        cmd: &["pi", "--mode", "rpc"],
        tags: &["code"],
        net: "host",
        on_crash: "fail",
    },
    WorkerPreset {
        name: "prover",
        cmd: &["prover"],
        tags: &["math"],
        net: "none",
        on_crash: "requeue",
    },
];

const UPSTREAM_VALUES: &[&str] = &[
    "https://api.openai.com/v1",
    "https://api.x.ai/v1",
    "https://openrouter.ai/api/v1",
    "http://127.0.0.1:8080/v1",
    "env:MODEL_UPSTREAM",
];

const KEY_VALUES: &[&str] = &[
    "MODEL_API_KEY",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
    "OPENROUTER_API_KEY",
];

const WORKER_NAMES: &[&str] = &["check", "sleeper", "pi", "prover"];

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
        return settings_tty(home);
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
    finish(home, answers, smoke)
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
        tags: vec!["code".into()],
        net: "host".into(),
        on_crash: "fail".into(),
        passphrase,
        telegram: env_string("INLET_TELEGRAM_TOKEN"),
        discord: env_string("INLET_DISCORD_TOKEN"),
    })
}

fn answers_from_tty(home: &Path, smoke: bool) -> Result<()> {
    step(1, "cell", "whether this machine can isolate a worker");
    let cell = {
        let _hold = spin::Hold::start_on(&spin::CELL, Sink::Stdout)?;
        crate::cell::probe(home)
    };
    if cell {
        println!("cell ready");
    } else {
        println!("cell unavailable");
        print_cell_fix();
    }

    step(2, "upstream", "where model calls are sent");
    let mut picked = index_of(UPSTREAMS, "env:MODEL_UPSTREAM");
    let upstream = loop {
        picked = choose(UPSTREAMS, picked)?;
        let value = UPSTREAMS[picked].1.to_string();
        if value.starts_with("env:") || probe_upstream(&value).is_ok() {
            break value;
        }
        println!("upstream unreachable");
    };

    step(3, "key", "which environment variable holds the key");
    let key_env = KEYS[choose(KEYS, 0)?].1.to_string();

    step(4, "worker", "which worker to register");
    let preset = &WORKER_PRESETS[choose(WORKER_CHOICES, 0)?];

    step(5, "budgets", "the period cap and the default task budget");
    let max_tokens = validate_tokens(&prompt_text("max tokens", "2000000", false, |raw| {
        validate_tokens(raw).map(|_| ())
    })?)?;
    let period = prompt_text("token period", "1d", false, |raw| {
        config::parse_period(raw).map(|_| ())
    })?;
    let budget = validate_tokens(&prompt_text("budget", "200000", false, |raw| {
        validate_tokens(raw).map(|_| ())
    })?)?;

    step(
        6,
        "passphrase",
        "signs the policy. empty leaves it unsigned",
    );
    let passphrase = prompt_text("passphrase", "", true, |raw| {
        if raw.is_empty() || raw.len() >= 4 {
            Ok(())
        } else {
            Err(err("passphrase too short"))
        }
    })?;
    let passphrase = none_if_empty(passphrase);

    step(7, "bridges", "optional bridge tokens. empty skips");
    let telegram = none_if_empty(prompt_text("telegram", "", true, check_token)?);
    let discord = none_if_empty(prompt_text("discord", "", true, check_token)?);

    let answers = Answers {
        upstream,
        key_env,
        max_tokens,
        period,
        budget,
        worker: preset.name.to_string(),
        cmd: preset.cmd.iter().map(|part| (*part).to_string()).collect(),
        tags: preset.tags.iter().map(|tag| (*tag).to_string()).collect(),
        net: preset.net.to_string(),
        on_crash: preset.on_crash.to_string(),
        passphrase,
        telegram,
        discord,
    };

    step(8, "daemon", "start inlet and run one smoke task");
    let writing = if answers.passphrase.is_some() {
        &spin::SIGN
    } else {
        &spin::POLICY
    };
    {
        let _hold = spin::Hold::start_on(writing, Sink::Stdout)?;
        commit(home, &answers)?;
    }
    {
        let _hold = spin::Hold::start_on(&spin::UP, Sink::Stdout)?;
        if !daemon_up(home) {
            spawn_daemon(home)?;
        }
    }
    println!("daemon is up");
    if smoke {
        let hold = spin::Hold::start_on(&spin::QUEUED, Sink::Stdout)?;
        smoke_task(home, &answers.worker, Some(&hold))?;
    }
    print_summary(&answers, cell);
    Ok(())
}

fn finish(home: &Path, answers: &Answers, smoke: bool) -> Result<()> {
    commit(home, answers)?;
    start_daemon(home)?;
    if smoke {
        println!("smoke");
        smoke_task(home, &answers.worker, None)?;
        println!("smoke ok");
    }
    Ok(())
}

fn commit(home: &Path, answers: &Answers) -> Result<()> {
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
    Ok(())
}

fn render(answers: &Answers) -> String {
    let cmd = answers
        .cmd
        .iter()
        .map(|part| lua_string(part))
        .collect::<Vec<_>>()
        .join(", ");
    let tags = answers
        .tags
        .iter()
        .map(|tag| lua_string(tag))
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
  {worker} = {{ cmd = {{ {cmd} }}, tags = {{ {tags} }}, net = {net}, on_crash = {crash} }},
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
        tags = tags,
        net = lua_string(&answers.net),
        crash = lua_string(&answers.on_crash),
    )
}

fn start_daemon(home: &Path) -> Result<()> {
    if daemon_up(home) {
        println!("daemon is up");
        return Ok(());
    }
    spawn_daemon(home)
}

fn spawn_daemon(home: &Path) -> Result<()> {
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

fn smoke_task(home: &Path, worker: &str, hold: Option<&spin::Hold>) -> Result<()> {
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
        let (label, live, queued, done, failed) = smoke_view(&status);
        if let Some(hold) = hold {
            if let Some(state) = smoke_spin(label) {
                hold.show(state, &format!("live {live}  queued {queued}"));
            }
        }
        if done {
            return Ok(());
        }
        if failed {
            return Err(err("smoke failed"));
        }
        thread::sleep(Duration::from_millis(40));
    }
    Err(err("smoke timed out"))
}

fn smoke_view(status: &serde_json::Value) -> (&str, u64, u64, bool, bool) {
    let live = status.get("live").and_then(|v| v.as_u64()).unwrap_or(0);
    let queued = status.get("queued").and_then(|v| v.as_u64()).unwrap_or(0);
    let mut label = "queued";
    let mut done = false;
    let mut failed = false;
    if let Some(tasks) = status.get("tasks").and_then(|tasks| tasks.as_array()) {
        for task in tasks {
            if task.get("goal").and_then(|g| g.as_str()) != Some("smoke") {
                continue;
            }
            if let Some(state) = task.get("state").and_then(|s| s.as_str()) {
                label = state;
                done = state == "done";
                failed = state == "failed" || state == "killed";
            }
        }
    }
    (label, live, queued, done, failed)
}

fn smoke_spin(label: &str) -> Option<&'static spin::State> {
    match label {
        "queued" => Some(&spin::QUEUED),
        "running" => Some(&spin::RUNNING),
        "blocked" => Some(&spin::BLOCKED),
        _ => None,
    }
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

fn apply_field_with(home: &Path, field: &str, value: &str, pass: Option<&str>) -> Result<()> {
    match field {
        "max_tokens" => patch_pass(
            home,
            &format!("caps.max_tokens = {}", parse_tokens(value)?),
            pass,
        ),
        "token_period" => {
            config::parse_period(value)?;
            patch_pass(
                home,
                &format!("caps.token_period = {}", lua_string(value)),
                pass,
            )
        }
        "upstream" => patch_pass(
            home,
            &format!("proxy.upstream = {}", lua_string(&check_upstream(value)?)),
            pass,
        ),
        "key" => patch_pass(
            home,
            &format!(
                "proxy.key = {}",
                lua_string(&format!("env:{}", check_env_name(value)?))
            ),
            pass,
        ),
        "budget" => patch_pass(
            home,
            &format!("default_budget.tokens = {}", parse_tokens(value)?),
            pass,
        ),
        "worker" => patch_worker_pass(home, Some(&check_worker(value)?), None, pass),
        "command" => patch_worker_pass(home, None, Some(value), pass),
        "telegram" => write_token(&home.join("keys/telegram.token"), value),
        "discord" => write_token(&home.join("keys/discord.token"), value),
        other => Err(err(format!("unknown field {other}"))),
    }
}

fn patch(home: &Path, line: &str) -> Result<()> {
    patch_pass(home, line, None)
}

fn patch_pass(home: &Path, line: &str, pass: Option<&str>) -> Result<()> {
    let path = paths::policy(home);
    let src = fs::read_to_string(&path)?;
    let next = apply_line(&src, line);
    Policy::parse(&next)?;
    store_policy_pass(home, &next, pass)
}

fn patch_worker(home: &Path, name: Option<&str>, cmd: Option<&str>) -> Result<()> {
    patch_worker_pass(home, name, cmd, None)
}

fn patch_worker_pass(
    home: &Path,
    name: Option<&str>,
    cmd: Option<&str>,
    pass: Option<&str>,
) -> Result<()> {
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
    store_policy_pass(home, &next, pass)
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

fn store_policy_pass(home: &Path, body: &str, given: Option<&str>) -> Result<()> {
    let signed = paths::policy_sig(home).exists();
    if !signed {
        write_secret(&paths::policy(home), body.as_bytes())?;
        return Ok(());
    }
    let owned;
    let pass = match given {
        Some(value) => value,
        None => {
            owned = passphrase()?;
            owned.as_str()
        }
    };
    let wrapped = fs::read(paths::key_priv(home)).map_err(|_| err("no signing key"))?;
    let sig = crate::sign::sign_with(&wrapped, pass, body.as_bytes())?;
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

#[derive(Clone)]
struct Field {
    name: &'static str,
    value: String,
    secret: bool,
    editable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Nav {
    Up,
    Down,
    Left,
    Right,
    Enter,
    Esc,
    Backspace,
    Char(char),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectStep {
    Stay(usize),
    Pick(usize),
    Cancel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EditStep {
    Stay,
    Submit,
    Cancel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SettingsStep {
    Stay(usize),
    Edit(usize),
    Exit,
}

struct RawGuard;

impl RawGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().map_err(|e| err(e.to_string()))?;
        Ok(RawGuard)
    }
}

impl Drop for RawGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

struct Cooked;

impl Cooked {
    fn enter() -> Result<Self> {
        disable_raw_mode().map_err(|e| err(e.to_string()))?;
        Ok(Cooked)
    }
}

impl Drop for Cooked {
    fn drop(&mut self) {
        let _ = enable_raw_mode();
    }
}

fn settings_tty(home: &Path) -> Result<()> {
    let before = load_fields(home)?;
    if before
        .iter()
        .any(|field| field.name == "cell" && field.value == "unavailable")
    {
        print_cell_fix();
    }
    let signed = before
        .iter()
        .any(|field| field.name == "policy" && field.value == "signed");
    let hint = if signed {
        "signed  arrows move, enter edits, esc or q leaves"
    } else {
        "unsigned  arrows move, enter edits, esc or q leaves"
    };
    println!("{}", dim(hint));
    let mut rows = before.clone();
    let mut cursor = 0usize;
    let mut editing = false;
    let mut buf = String::new();
    let mut asking_pass = false;
    let mut pass_buf = String::new();
    let mut cache: Option<String> = None;
    let _raw = RawGuard::enter()?;
    loop {
        let lines = draw_settings(&rows, cursor, editing, &buf, asking_pass, &pass_buf);
        let nav = read_nav()?;
        if !editing && !asking_pass {
            match settings_browse(cursor, rows.len(), &nav) {
                SettingsStep::Exit => break,
                SettingsStep::Stay(next) => {
                    rewind(lines);
                    cursor = next;
                }
                SettingsStep::Edit(next) => {
                    rewind(lines);
                    cursor = next;
                    if rows[next].editable {
                        editing = true;
                        buf.clear();
                    }
                }
            }
            continue;
        }
        rewind(lines);
        if asking_pass {
            match edit_apply(&mut pass_buf, &nav) {
                EditStep::Stay => {}
                EditStep::Cancel => {
                    asking_pass = false;
                    editing = false;
                    pass_buf.clear();
                    buf.clear();
                }
                EditStep::Submit => {
                    if pass_buf.len() < 4 {
                        emit("passphrase too short");
                        continue;
                    }
                    let name = rows[cursor].name;
                    let value = buf.clone();
                    match apply_field_with(home, name, &value, Some(&pass_buf)) {
                        Ok(()) => {
                            cache = Some(std::mem::take(&mut pass_buf));
                            asking_pass = false;
                            editing = false;
                            buf.clear();
                            rows = load_fields(home)?;
                        }
                        Err(e) => {
                            emit(&e.to_string());
                            pass_buf.clear();
                        }
                    }
                }
            }
            continue;
        }
        let presets = field_presets(rows[cursor].name);
        let current = rows[cursor].value.clone();
        match editing_key(&mut buf, presets, &current, &nav) {
            EditStep::Stay => {}
            EditStep::Cancel => {
                editing = false;
                buf.clear();
            }
            EditStep::Submit => {
                if buf.trim().is_empty() {
                    editing = false;
                    buf.clear();
                    continue;
                }
                let name = rows[cursor].name;
                let normalized = match validate_field(name, &buf) {
                    Ok(value) => value,
                    Err(e) => {
                        emit(&e.to_string());
                        continue;
                    }
                };
                if name == "upstream" && !normalized.starts_with("env:") {
                    if let Err(e) = probe_upstream_raw(&normalized) {
                        emit(&e.to_string());
                        continue;
                    }
                }
                buf = normalized;
                let needs = signed && name != "telegram" && name != "discord";
                if needs && cache.is_none() {
                    asking_pass = true;
                    continue;
                }
                let pass = if needs { cache.as_deref() } else { None };
                match apply_field_with(home, name, &buf, pass) {
                    Ok(()) => {
                        editing = false;
                        buf.clear();
                        rows = load_fields(home)?;
                    }
                    Err(e) => {
                        emit(&e.to_string());
                        if needs {
                            cache = None;
                            asking_pass = true;
                        }
                    }
                }
            }
        }
    }
    drop(_raw);
    let after = load_fields(home)?;
    let before_pairs: Vec<(&str, &str)> = before
        .iter()
        .map(|field| (field.name, field.value.as_str()))
        .collect();
    let after_pairs: Vec<(&str, &str)> = after
        .iter()
        .map(|field| (field.name, field.value.as_str()))
        .collect();
    println!("{}", diff_line(&before_pairs, &after_pairs));
    Ok(())
}

fn load_fields(home: &Path) -> Result<Vec<Field>> {
    let src = fs::read_to_string(paths::policy(home))?;
    let policy = Policy::parse(&src)?;
    let cfg = &policy.cfg;
    let (worker, command) = match cfg.workers.iter().next() {
        Some((name, worker)) => (name.clone(), worker.cmd.join(" ")),
        None => ("check".into(), "/bin/true".into()),
    };
    let cell = if crate::cell::probe(home) {
        "ready"
    } else {
        "unavailable"
    };
    let signed = if paths::policy_sig(home).exists() {
        "signed"
    } else {
        "unsigned"
    };
    Ok(vec![
        Field {
            name: "cell",
            value: cell.into(),
            secret: false,
            editable: false,
        },
        Field {
            name: "max_tokens",
            value: cfg.caps.max_tokens.to_string(),
            secret: false,
            editable: true,
        },
        Field {
            name: "token_period",
            value: last_quoted(&src, "token_period").unwrap_or_else(|| "1d".into()),
            secret: false,
            editable: true,
        },
        Field {
            name: "budget",
            value: cfg.default_budget.tokens.to_string(),
            secret: false,
            editable: true,
        },
        Field {
            name: "upstream",
            value: last_quoted(&src, "upstream").unwrap_or_else(|| "unset".into()),
            secret: false,
            editable: true,
        },
        Field {
            name: "key",
            value: last_quoted(&src, "key").unwrap_or_else(|| "unset".into()),
            secret: false,
            editable: true,
        },
        Field {
            name: "worker",
            value: worker,
            secret: false,
            editable: true,
        },
        Field {
            name: "command",
            value: command,
            secret: false,
            editable: true,
        },
        Field {
            name: "policy",
            value: signed.into(),
            secret: false,
            editable: false,
        },
        Field {
            name: "telegram",
            value: token_state(&home.join("keys/telegram.token")).into(),
            secret: true,
            editable: true,
        },
        Field {
            name: "discord",
            value: token_state(&home.join("keys/discord.token")).into(),
            secret: true,
            editable: true,
        },
    ])
}

fn draw_settings(
    rows: &[Field],
    cursor: usize,
    editing: bool,
    buf: &str,
    asking_pass: bool,
    pass_buf: &str,
) -> u16 {
    let mut n = 0u16;
    for (i, row) in rows.iter().enumerate() {
        let mark = if i == cursor { ">" } else { " " };
        let shown = if editing && i == cursor {
            if buf.is_empty() {
                dim(&row.value)
            } else if row.secret {
                masked(buf)
            } else {
                buf.to_string()
            }
        } else {
            row.value.clone()
        };
        emit(&format!("{mark} {name:<14}{shown}", name = row.name));
        n += 1;
    }
    if asking_pass {
        let shown = if pass_buf.is_empty() {
            dim("passphrase")
        } else {
            masked(pass_buf)
        };
        emit(&format!("  {shown}"));
        n += 1;
    }
    n
}

fn step(n: u64, title: &str, hint: &str) {
    println!("{}  {title}", dim(&format!("{n}/{STEPS}")));
    println!("{}", dim(hint));
    let _ = io::stdout().flush();
}

fn print_summary(answers: &Answers, cell: bool) {
    row("cell", if cell { "ready" } else { "unavailable" });
    row("upstream", &answers.upstream);
    row("key", &answers.key_env);
    row("max_tokens", &answers.max_tokens.to_string());
    row("token_period", &answers.period);
    row("budget", &answers.budget.to_string());
    row("worker", &answers.worker);
    row(
        "policy",
        if answers.passphrase.is_some() {
            "signed"
        } else {
            "unsigned"
        },
    );
    row(
        "telegram",
        if answers.telegram.is_some() {
            "set"
        } else {
            "unset"
        },
    );
    row(
        "discord",
        if answers.discord.is_some() {
            "set"
        } else {
            "unset"
        },
    );
    println!();
    println!("inlet up");
    println!("inlet add -w {} -g hello --no-verify", answers.worker);
    println!("inlet");
}

fn choose(options: &[(&str, &str)], start: usize) -> Result<usize> {
    let _raw = RawGuard::enter()?;
    let mut index = start.min(options.len().saturating_sub(1));
    loop {
        let lines = draw_choices(options, index);
        let nav = read_nav()?;
        rewind(lines);
        match select_index(index, options.len(), &nav) {
            SelectStep::Stay(next) => index = next,
            SelectStep::Pick(picked) => {
                let (label, value) = options[picked];
                if value.is_empty() || value == label {
                    emit(label);
                } else {
                    emit(&format!("{label}  {value}"));
                }
                return Ok(picked);
            }
            SelectStep::Cancel => {}
        }
    }
}

fn draw_choices(options: &[(&str, &str)], index: usize) -> u16 {
    let mut n = 0u16;
    for (i, (label, value)) in options.iter().enumerate() {
        let mark = if i == index { ">" } else { " " };
        if i == index && *value != *label && !value.is_empty() {
            emit(&format!("{mark} {label}  {}", dim(value)));
        } else {
            emit(&format!("{mark} {label}"));
        }
        n += 1;
    }
    n
}

fn prompt_text(
    label: &str,
    placeholder: &str,
    secret: bool,
    check: impl Fn(&str) -> Result<()>,
) -> Result<String> {
    let _raw = RawGuard::enter()?;
    let mut buf = String::new();
    loop {
        let lines = draw_prompt(label, placeholder, &buf, secret);
        let nav = read_nav()?;
        rewind(lines);
        match edit_apply(&mut buf, &nav) {
            EditStep::Stay => {}
            EditStep::Cancel => buf.clear(),
            EditStep::Submit => {
                let value = if buf.trim().is_empty() {
                    placeholder.to_string()
                } else {
                    buf.trim().to_string()
                };
                match check(&value) {
                    Ok(()) => {
                        let shown = if secret {
                            if value.is_empty() {
                                "skipped".to_string()
                            } else {
                                masked(&value)
                            }
                        } else {
                            value.clone()
                        };
                        emit(&format!("{label}  {shown}"));
                        return Ok(value);
                    }
                    Err(e) => emit(&e.to_string()),
                }
            }
        }
    }
}

fn draw_prompt(label: &str, placeholder: &str, buf: &str, secret: bool) -> u16 {
    let shown = if buf.is_empty() {
        let ghost = if placeholder.is_empty() {
            "skip"
        } else {
            placeholder
        };
        dim(ghost)
    } else if secret {
        masked(buf)
    } else {
        buf.to_string()
    };
    emit(&format!("{label}  {shown}"));
    1
}

fn probe_upstream(url: &str) -> Result<()> {
    let _hold = spin::Hold::start_on(&spin::UPSTREAM, Sink::Stdout)?;
    reach_upstream(url)
}

fn probe_upstream_raw(url: &str) -> Result<()> {
    let _cooked = Cooked::enter()?;
    probe_upstream(url)
}

fn reach_upstream(raw: &str) -> Result<()> {
    let raw = raw.trim();
    if raw.starts_with("env:") {
        return Ok(());
    }
    let rest = if let Some(rest) = raw.strip_prefix("https://") {
        rest
    } else if let Some(rest) = raw.strip_prefix("http://") {
        rest
    } else {
        return Err(err("upstream unreachable"));
    };
    let hostport = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    if hostport.is_empty() {
        return Err(err("upstream unreachable"));
    }
    let default_port: u16 = if raw.starts_with("https://") { 443 } else { 80 };
    let target = if hostport.starts_with('[') || hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:{default_port}")
    };
    let addr = match target.to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => addr,
            None => return Err(err("upstream unreachable")),
        },
        Err(_) => return Err(err("upstream unreachable")),
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(800))
        .map_err(|_| err("upstream unreachable"))?;
    Ok(())
}

fn validate_tokens(raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.parse::<u64>().is_err() {
        return Err(err("tokens are a number"));
    }
    parse_tokens(raw)
}

fn validate_field(name: &str, value: &str) -> Result<String> {
    match name {
        "max_tokens" | "budget" => validate_tokens(value).map(|n| n.to_string()),
        "token_period" => {
            config::parse_period(value)?;
            Ok(value.trim().to_string())
        }
        "upstream" => check_upstream(value),
        "key" => check_env_name(value),
        "worker" => check_worker(value),
        "command" => {
            split_cmd(value)?;
            Ok(value.trim().to_string())
        }
        "telegram" | "discord" => check_token(value).map(|()| value.to_string()),
        other => Err(err(format!("unknown field {other}"))),
    }
}

fn check_token(raw: &str) -> Result<()> {
    if raw.chars().any(|c| c.is_whitespace()) {
        Err(err("token"))
    } else {
        Ok(())
    }
}

fn field_presets(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "upstream" => Some(UPSTREAM_VALUES),
        "key" => Some(KEY_VALUES),
        "worker" => Some(WORKER_NAMES),
        _ => None,
    }
}

fn index_of(options: &[(&str, &str)], value: &str) -> usize {
    options
        .iter()
        .position(|(_, item)| *item == value)
        .unwrap_or(0)
}

fn select_index(index: usize, len: usize, nav: &Nav) -> SelectStep {
    let last = len.saturating_sub(1);
    match nav {
        Nav::Up => SelectStep::Stay(index.saturating_sub(1)),
        Nav::Down => SelectStep::Stay((index + 1).min(last)),
        Nav::Enter => SelectStep::Pick(index.min(last)),
        Nav::Esc => SelectStep::Cancel,
        _ => SelectStep::Stay(index.min(last)),
    }
}

fn edit_apply(buf: &mut String, nav: &Nav) -> EditStep {
    match nav {
        Nav::Char(c) if !c.is_control() => {
            buf.push(*c);
            EditStep::Stay
        }
        Nav::Backspace => {
            buf.pop();
            EditStep::Stay
        }
        Nav::Enter => EditStep::Submit,
        Nav::Esc => EditStep::Cancel,
        _ => EditStep::Stay,
    }
}

fn editing_key(buf: &mut String, presets: Option<&[&str]>, current: &str, nav: &Nav) -> EditStep {
    if let Some(presets) = presets {
        if matches!(nav, Nav::Left | Nav::Right) && !presets.is_empty() {
            let base = if buf.is_empty() {
                current
            } else {
                buf.as_str()
            };
            *buf = cycle_value(base, presets, nav);
            return EditStep::Stay;
        }
    }
    edit_apply(buf, nav)
}

fn cycle_index(index: usize, len: usize, nav: &Nav) -> usize {
    if len == 0 {
        return 0;
    }
    let last = len - 1;
    match nav {
        Nav::Left => index.min(last).saturating_sub(1),
        Nav::Right => (index + 1).min(last),
        _ => index.min(last),
    }
}

fn cycle_value(current: &str, presets: &[&str], nav: &Nav) -> String {
    let stripped = current.strip_prefix("env:").unwrap_or(current);
    let index = presets
        .iter()
        .position(|item| *item == current || *item == stripped)
        .unwrap_or(0);
    presets[cycle_index(index, presets.len(), nav)].to_string()
}

fn settings_browse(cursor: usize, len: usize, nav: &Nav) -> SettingsStep {
    let last = len.saturating_sub(1);
    match nav {
        Nav::Up => SettingsStep::Stay(cursor.saturating_sub(1)),
        Nav::Down => SettingsStep::Stay((cursor + 1).min(last)),
        Nav::Enter => SettingsStep::Edit(cursor.min(last)),
        Nav::Esc => SettingsStep::Exit,
        Nav::Char('q') => SettingsStep::Exit,
        _ => SettingsStep::Stay(cursor.min(last)),
    }
}

fn masked(secret: &str) -> String {
    "*".repeat(secret.chars().count())
}

fn diff_line(before: &[(&str, &str)], after: &[(&str, &str)]) -> String {
    let mut parts = Vec::new();
    let n = before.len().min(after.len());
    for i in 0..n {
        if before[i].1 != after[i].1 {
            parts.push(format!("{} {} -> {}", before[i].0, before[i].1, after[i].1));
        }
    }
    if parts.is_empty() {
        "unchanged".into()
    } else {
        format!("changed  {}", parts.join(", "))
    }
}

fn dim(text: &str) -> String {
    format!("\x1b[2m{text}\x1b[0m")
}

fn emit(line: &str) {
    let mut out = io::stdout();
    let _ = write!(out, "{line}\r\n");
    let _ = out.flush();
}

fn rewind(n: u16) {
    if n == 0 {
        return;
    }
    let mut out = io::stdout();
    let _ = out.execute(MoveToPreviousLine(n));
    let _ = out.execute(Clear(ClearType::FromCursorDown));
}

fn read_nav() -> Result<Nav> {
    loop {
        let ev = event::read().map_err(|e| err(e.to_string()))?;
        let Event::Key(key) = ev else {
            continue;
        };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            return Err(err("interrupted"));
        }
        let nav = match key.code {
            KeyCode::Up => Nav::Up,
            KeyCode::Down => Nav::Down,
            KeyCode::Left => Nav::Left,
            KeyCode::Right => Nav::Right,
            KeyCode::Enter => Nav::Enter,
            KeyCode::Esc => Nav::Esc,
            KeyCode::Backspace | KeyCode::Delete => Nav::Backspace,
            KeyCode::Char(c) => Nav::Char(c),
            _ => continue,
        };
        return Ok(nav);
    }
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

    #[test]
    fn select_clamps_and_picks() {
        assert_eq!(select_index(0, 4, &Nav::Up), SelectStep::Stay(0));
        assert_eq!(select_index(3, 4, &Nav::Down), SelectStep::Stay(3));
        assert_eq!(select_index(1, 4, &Nav::Down), SelectStep::Stay(2));
        assert_eq!(select_index(1, 4, &Nav::Enter), SelectStep::Pick(1));
        assert_eq!(select_index(1, 4, &Nav::Esc), SelectStep::Cancel);
        assert_eq!(select_index(2, 4, &Nav::Left), SelectStep::Stay(2));
    }

    #[test]
    fn edit_pushes_and_submits() {
        let mut buf = String::new();
        assert_eq!(edit_apply(&mut buf, &Nav::Char('n')), EditStep::Stay);
        assert_eq!(edit_apply(&mut buf, &Nav::Char('o')), EditStep::Stay);
        assert_eq!(edit_apply(&mut buf, &Nav::Backspace), EditStep::Stay);
        assert_eq!(buf, "n");
        assert_eq!(edit_apply(&mut buf, &Nav::Enter), EditStep::Submit);
        assert_eq!(edit_apply(&mut buf, &Nav::Esc), EditStep::Cancel);
        assert_eq!(buf, "n");
    }

    #[test]
    fn editing_cycles_presets_and_types() {
        let presets = ["check", "sleeper", "pi", "prover"];
        let mut buf = String::new();
        assert_eq!(
            editing_key(&mut buf, Some(&presets), "check", &Nav::Right),
            EditStep::Stay
        );
        assert_eq!(buf, "sleeper");
        assert_eq!(
            editing_key(&mut buf, Some(&presets), "check", &Nav::Right),
            EditStep::Stay
        );
        assert_eq!(buf, "pi");
        assert_eq!(
            editing_key(&mut buf, Some(&presets), "check", &Nav::Char('x')),
            EditStep::Stay
        );
        assert_eq!(buf, "pix");
        assert_eq!(
            editing_key(&mut buf, None, "1d", &Nav::Left),
            EditStep::Stay
        );
        assert_eq!(buf, "pix");
        let keys = ["MODEL_API_KEY", "OPENAI_API_KEY"];
        let mut key = String::new();
        assert_eq!(
            editing_key(&mut key, Some(&keys), "env:MODEL_API_KEY", &Nav::Right),
            EditStep::Stay
        );
        assert_eq!(key, "OPENAI_API_KEY");
        assert_eq!(
            cycle_value("env:MODEL_UPSTREAM", UPSTREAM_VALUES, &Nav::Left),
            "http://127.0.0.1:8080/v1"
        );
        assert_eq!(
            cycle_value("http://127.0.0.1:8080/v1", UPSTREAM_VALUES, &Nav::Left),
            "https://openrouter.ai/api/v1"
        );
        assert_eq!(
            cycle_value(UPSTREAM_VALUES[0], UPSTREAM_VALUES, &Nav::Left),
            UPSTREAM_VALUES[0]
        );
        assert_eq!(
            cycle_value(
                UPSTREAM_VALUES[UPSTREAM_VALUES.len() - 1],
                UPSTREAM_VALUES,
                &Nav::Right
            ),
            UPSTREAM_VALUES[UPSTREAM_VALUES.len() - 1]
        );
    }

    #[test]
    fn settings_browse_moves_and_leaves() {
        assert_eq!(settings_browse(0, 3, &Nav::Up), SettingsStep::Stay(0));
        assert_eq!(settings_browse(2, 3, &Nav::Down), SettingsStep::Stay(2));
        assert_eq!(settings_browse(1, 3, &Nav::Down), SettingsStep::Stay(2));
        assert_eq!(settings_browse(1, 3, &Nav::Enter), SettingsStep::Edit(1));
        assert_eq!(settings_browse(1, 3, &Nav::Esc), SettingsStep::Exit);
        assert_eq!(settings_browse(1, 3, &Nav::Char('q')), SettingsStep::Exit);
        assert_eq!(
            settings_browse(1, 3, &Nav::Char('x')),
            SettingsStep::Stay(1)
        );
    }

    #[test]
    fn secrets_stay_masked() {
        let secret = "mint-leaf";
        let shown = masked(secret);
        assert_eq!(shown, "*********");
        assert!(!shown.contains("mint"));
        assert!(!shown.contains(secret));
    }

    #[test]
    fn diff_line_names_the_change() {
        assert_eq!(
            diff_line(&[("max_tokens", "1")], &[("max_tokens", "1")]),
            "unchanged"
        );
        assert_eq!(
            diff_line(
                &[("max_tokens", "2000000"), ("budget", "200000")],
                &[("max_tokens", "250000"), ("budget", "200000")]
            ),
            "changed  max_tokens 2000000 -> 250000"
        );
        assert_eq!(
            diff_line(
                &[("max_tokens", "1"), ("budget", "2")],
                &[("max_tokens", "3"), ("budget", "4")]
            ),
            "changed  max_tokens 1 -> 3, budget 2 -> 4"
        );
    }

    #[test]
    fn tokens_and_periods_fail_inline() {
        assert_eq!(
            validate_tokens("nope").unwrap_err().to_string(),
            "tokens are a number"
        );
        assert_eq!(
            validate_tokens("0").unwrap_err().to_string(),
            "token budget is empty"
        );
        assert_eq!(validate_tokens("200").unwrap(), 200);
        assert_eq!(
            config::parse_period("yesterday").unwrap_err().to_string(),
            "bad token_period yesterday"
        );
        assert!(config::parse_period("1d").is_ok());
        assert_eq!(
            validate_field("worker", "Nope").unwrap_err().to_string(),
            "bad worker name"
        );
    }

    #[test]
    fn upstream_reach_skips_env_and_refuses_a_closed_port() {
        assert!(reach_upstream("env:MODEL_UPSTREAM").is_ok());
        assert_eq!(
            reach_upstream("http://127.0.0.1:1")
                .unwrap_err()
                .to_string(),
            "upstream unreachable"
        );
        assert_eq!(
            reach_upstream("not a url").unwrap_err().to_string(),
            "upstream unreachable"
        );
    }

    fn sample_answers(worker: &str) -> Answers {
        Answers {
            upstream: "env:MODEL_UPSTREAM".into(),
            key_env: "MODEL_API_KEY".into(),
            max_tokens: 2_000_000,
            period: "1d".into(),
            budget: 200_000,
            worker: worker.into(),
            cmd: vec!["/bin/true".into()],
            tags: vec!["code".into()],
            net: "host".into(),
            on_crash: "fail".into(),
            passphrase: None,
            telegram: None,
            discord: None,
        }
    }

    #[test]
    fn prover_preset_is_a_math_worker() {
        let body = render(&sample_answers("check"));
        assert!(body.contains(
            "check = { cmd = { \"/bin/true\" }, tags = { \"code\" }, net = \"host\", on_crash = \"fail\" }"
        ));
        Policy::parse(&body).unwrap();
        let mut answers = sample_answers("prover");
        answers.cmd = vec!["prover".into()];
        answers.tags = vec!["math".into()];
        answers.net = "none".into();
        answers.on_crash = "requeue".into();
        let policy = Policy::parse(&render(&answers)).unwrap();
        assert_eq!(policy.cfg.workers["prover"].net, config::Net::None);
        assert_eq!(
            policy.cfg.workers["prover"].on_crash,
            config::OnCrash::Requeue
        );
        assert_eq!(policy.cfg.workers["prover"].tags, vec!["math".to_string()]);
        assert_eq!(WORKER_PRESETS.len(), WORKER_CHOICES.len());
        assert_eq!(WORKER_PRESETS[0].name, "check");
        assert_eq!(WORKER_PRESETS[3].net, "none");
        assert_eq!(WORKER_PRESETS[3].on_crash, "requeue");
    }
}
