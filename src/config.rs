use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::rc::Rc;

use mlua::{Function, HookTriggers, Lua, LuaOptions, StdLib, Table, Value, VmState};

use crate::error::{err, Result};
use crate::model::Budget;

pub const DEFAULT_POLICY: &str = r#"caps = {
  max_live      = 8,
  max_depth     = 3,
  max_tokens    = 2000000,
  max_memory_mb = 8192,
  max_pids      = 64,
  token_period  = "1d",
}

setup        = "box"
isolator     = "cgroup"
human_weight = 4
min_ev       = 0
value        = 400000
unattended   = false
debug        = 1

default_budget = { tokens = 200000, seconds = 3600, memory_mb = 1024, pids = 8 }

decision = {
  kind         = "off",
  endpoint     = "env:DECISION_ENDPOINT",
  model        = "gpt-6-luna",
  timeout_ms   = 800,
  purse_tokens = 50000,
}

proxy = {
  upstream = "env:MODEL_UPSTREAM",
  key      = "env:MODEL_API_KEY",
}

workers = {
  pi     = { cmd = { "pi", "--mode", "rpc" }, tags = { "code" }, net = "host", on_crash = "fail" },
  prover = { cmd = { "prover" },              tags = { "math" }, net = "none", on_crash = "requeue" },
}

function admit(ctx)
  if ctx.tags.math and ctx.depth > 1 then return "deny" end
  return "allow"
end
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolator {
    Cgroup,
    Rlimit,
    Slurm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Net {
    Host,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCrash {
    Fail,
    Requeue,
}

#[derive(Debug, Clone)]
pub struct Caps {
    pub max_live: usize,
    pub max_depth: u32,
    pub max_tokens: u64,
    pub max_memory_mb: u64,
    pub max_pids: u64,
    pub token_period_ms: u64,
}

#[derive(Debug, Clone)]
pub struct WorkerCfg {
    pub cmd: Vec<String>,
    pub tags: Vec<String>,
    pub net: Net,
    pub on_crash: OnCrash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionKind {
    Off,
    OpenAi,
    Jev,
}

impl DecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            DecisionKind::Off => "off",
            DecisionKind::OpenAi => "openai",
            DecisionKind::Jev => "jev",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DecisionCfg {
    pub kind: DecisionKind,
    pub endpoint: Option<String>,
    pub model: String,
    pub timeout_ms: u64,
    pub purse_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct ProxyCfg {
    pub upstream: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub caps: Caps,
    pub setup: String,
    pub isolator: Isolator,
    pub human_weight: u64,
    pub min_ev: i64,
    pub value: u64,
    pub unattended: bool,
    pub debug: u8,
    pub default_budget: Budget,
    pub decision: DecisionCfg,
    pub proxy: ProxyCfg,
    pub workers: BTreeMap<String, WorkerCfg>,
}

pub struct Policy {
    pub cfg: Config,
    lua: Lua,
    has_admit: bool,
}

impl Policy {
    pub fn load(path: &Path) -> Result<Self> {
        let src = fs::read_to_string(path)
            .map_err(|_| err(format!("no policy at {} (inlet init)", path.display())))?;
        Self::parse(&src)
    }

    pub fn parse(src: &str) -> Result<Self> {
        let lua = Lua::new_with(
            StdLib::COROUTINE | StdLib::TABLE | StdLib::STRING | StdLib::UTF8 | StdLib::MATH,
            LuaOptions::default(),
        )?;
        lua.load(
            r#"
            local _load = load
            if _load then
              function load(chunk, chunkname, mode, env)
                if type(chunk) == "string" then
                  if string.byte(chunk, 1) == 27 then error("bytecode denied") end
                  mode = "t"
                end
                return _load(chunk, chunkname, mode, env)
              end
            end
            "#,
        )
        .exec()?;
        let ticks = Rc::new(Cell::new(0u32));
        arm_budget(&lua, ticks.clone())?;
        lua.load(src)
            .exec()
            .map_err(|e| err(format!("policy: {e}")))?;
        if ticks.get() > BUDGET_TICKS {
            return Err(err("policy exceeded the instruction budget"));
        }
        lua.remove_hook();
        let cfg = read_config(&lua)?;
        let has_admit = lua.globals().get::<Option<Function>>("admit")?.is_some();
        Ok(Policy {
            cfg,
            lua,
            has_admit,
        })
    }

    /// `Some(false)` denies. `Some(true)` does not override a Rust deny.
    /// An overrun or a Lua error is a deny.
    pub fn admit(&self, ctx: &AdmitCtx) -> Option<bool> {
        if !self.has_admit {
            return None;
        }
        let ticks = Rc::new(Cell::new(0u32));
        if arm_budget(&self.lua, ticks.clone()).is_err() {
            return Some(false);
        }
        let result = (|| {
            let func: Function = self.lua.globals().get("admit")?;
            let table = self.lua.create_table()?;
            table.set("depth", ctx.depth)?;
            table.set("live", ctx.live)?;
            table.set("queued", ctx.queued)?;
            table.set("worker", ctx.worker)?;
            table.set("goal", ctx.goal)?;
            table.set("value", ctx.value)?;
            let tags = self.lua.create_table()?;
            for tag in ctx.tags {
                tags.set(tag.as_str(), true)?;
            }
            table.set("tags", tags)?;
            let answer: String = func.call(table)?;
            Ok::<String, mlua::Error>(answer)
        })();
        self.lua.remove_hook();
        match result {
            Ok(answer) if answer == "allow" && ticks.get() <= BUDGET_TICKS => Some(true),
            _ => Some(false),
        }
    }
}

pub struct AdmitCtx<'a> {
    pub depth: u32,
    pub live: usize,
    pub queued: usize,
    pub worker: &'a str,
    pub goal: &'a str,
    pub value: u64,
    pub tags: &'a [String],
}

const BUDGET_TICKS: u32 = 100;

fn arm_budget(lua: &Lua, ticks: Rc<Cell<u32>>) -> Result<()> {
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(1_000),
        move |_lua, _dbg| {
            let n = ticks.get() + 1;
            ticks.set(n);
            if n > BUDGET_TICKS {
                Err(mlua::Error::external("instruction budget"))
            } else {
                Ok(VmState::Continue)
            }
        },
    );
    Ok(())
}

fn read_config(lua: &Lua) -> Result<Config> {
    let g = lua.globals();
    let setup: String = g
        .get::<Option<String>>("setup")?
        .unwrap_or_else(|| "box".into());
    let mut cfg = preset(&setup);
    cfg.setup = setup;
    if let Some(v) = g.get::<Option<String>>("isolator")? {
        cfg.isolator = parse_isolator(&v)?;
    }
    if let Some(v) = g.get::<Option<i64>>("human_weight")? {
        cfg.human_weight = v.max(0) as u64;
    }
    if let Some(v) = g.get::<Option<i64>>("min_ev")? {
        cfg.min_ev = v;
    }
    if let Some(v) = g.get::<Option<i64>>("value")? {
        cfg.value = v.max(0) as u64;
    }
    if let Some(v) = g.get::<Option<bool>>("unattended")? {
        cfg.unattended = v;
    }
    if let Some(v) = g.get::<Option<i64>>("debug")? {
        cfg.debug = v.clamp(0, 4) as u8;
    }
    if let Some(table) = g.get::<Option<Table>>("caps")? {
        overlay_caps(&mut cfg.caps, &table)?;
    }
    if let Some(table) = g.get::<Option<Table>>("default_budget")? {
        overlay_budget(&mut cfg.default_budget, &table)?;
    }
    if let Some(table) = g.get::<Option<Table>>("decision")? {
        if let Some(v) = table.get::<Option<String>>("kind")? {
            cfg.decision.kind = parse_decision_kind(&v)?;
        }
        if let Some(v) = table.get::<Option<String>>("endpoint")? {
            cfg.decision.endpoint = resolve_env(v);
        }
        if let Some(v) = table.get::<Option<String>>("model")? {
            if !v.is_empty() {
                cfg.decision.model = v;
            }
        }
        if let Some(v) = table.get::<Option<i64>>("timeout_ms")? {
            cfg.decision.timeout_ms = v.max(1) as u64;
        }
        if let Some(v) = table.get::<Option<i64>>("purse_tokens")? {
            cfg.decision.purse_tokens = v.max(0) as u64;
        }
    }
    if let Some(table) = g.get::<Option<Table>>("proxy")? {
        if let Some(v) = table.get::<Option<String>>("upstream")? {
            cfg.proxy.upstream = resolve_env(v);
        }
        if let Some(v) = table.get::<Option<String>>("key")? {
            cfg.proxy.key = resolve_env(v);
        }
    }
    if let Some(table) = g.get::<Option<Table>>("workers")? {
        cfg.workers.clear();
        for pair in table.pairs::<String, Table>() {
            let (name, spec) = pair?;
            cfg.workers.insert(name, read_worker(&spec)?);
        }
    }
    Ok(cfg)
}

fn read_worker(spec: &Table) -> Result<WorkerCfg> {
    let cmd: Vec<String> = spec
        .get::<Option<Vec<String>>>("cmd")?
        .filter(|c| !c.is_empty())
        .ok_or_else(|| err("worker cmd is required"))?;
    let tags = spec.get::<Option<Vec<String>>>("tags")?.unwrap_or_default();
    let net = match spec.get::<Option<String>>("net")? {
        Some(v) => parse_net(&v)?,
        None => default_net(&tags),
    };
    let on_crash = match spec.get::<Option<String>>("on_crash")? {
        Some(v) => parse_crash(&v)?,
        None => default_crash(&tags),
    };
    Ok(WorkerCfg {
        cmd,
        tags,
        net,
        on_crash,
    })
}

pub fn default_net(tags: &[String]) -> Net {
    if tags.iter().any(|t| t == "math") && !tags.iter().any(|t| t == "code") {
        Net::None
    } else {
        Net::Host
    }
}

pub fn default_crash(tags: &[String]) -> OnCrash {
    if tags.iter().any(|t| t == "math") && !tags.iter().any(|t| t == "code") {
        OnCrash::Requeue
    } else {
        OnCrash::Fail
    }
}

fn parse_net(v: &str) -> Result<Net> {
    match v {
        "host" => Ok(Net::Host),
        "none" => Ok(Net::None),
        other => Err(err(format!("unknown net {other}"))),
    }
}

fn parse_crash(v: &str) -> Result<OnCrash> {
    match v {
        "fail" => Ok(OnCrash::Fail),
        "requeue" => Ok(OnCrash::Requeue),
        other => Err(err(format!("unknown on_crash {other}"))),
    }
}

fn parse_decision_kind(v: &str) -> Result<DecisionKind> {
    match v {
        "off" => Ok(DecisionKind::Off),
        "openai" => Ok(DecisionKind::OpenAi),
        "jev" => Ok(DecisionKind::Jev),
        other => Err(err(format!("unknown decision.kind {other}"))),
    }
}

fn parse_isolator(v: &str) -> Result<Isolator> {
    match v {
        "cgroup" => Ok(Isolator::Cgroup),
        "rlimit" => Ok(Isolator::Rlimit),
        "slurm" => Ok(Isolator::Slurm),
        other => Err(err(format!("unknown isolator {other}"))),
    }
}

fn overlay_caps(caps: &mut Caps, table: &Table) -> Result<()> {
    if let Some(v) = table.get::<Option<i64>>("max_live")? {
        caps.max_live = v.max(0) as usize;
    }
    if let Some(v) = table.get::<Option<i64>>("max_depth")? {
        caps.max_depth = v.max(0) as u32;
    }
    if let Some(v) = table.get::<Option<i64>>("max_tokens")? {
        caps.max_tokens = v.max(0) as u64;
    }
    if let Some(v) = table.get::<Option<i64>>("max_memory_mb")? {
        caps.max_memory_mb = v.max(0) as u64;
    }
    if let Some(v) = table.get::<Option<i64>>("max_pids")? {
        caps.max_pids = v.max(0) as u64;
    }
    if table.contains_key("token_period")? {
        caps.token_period_ms = read_period(table.get::<Value>("token_period")?)?;
    }
    Ok(())
}

fn overlay_budget(budget: &mut Budget, table: &Table) -> Result<()> {
    if let Some(v) = table.get::<Option<i64>>("tokens")? {
        budget.tokens = v.max(0) as u64;
    }
    if let Some(v) = table.get::<Option<i64>>("seconds")? {
        budget.seconds = v.max(0) as u64;
    }
    if let Some(v) = table.get::<Option<i64>>("memory_mb")? {
        budget.memory_mb = v.max(0) as u64;
    }
    if let Some(v) = table.get::<Option<i64>>("pids")? {
        budget.pids = v.max(0) as u64;
    }
    Ok(())
}

fn read_period(value: Value) -> Result<u64> {
    match value {
        Value::Integer(n) => Ok((n.max(1) as u64).saturating_mul(1000)),
        Value::Number(n) => Ok((n.max(1.0) as u64).saturating_mul(1000)),
        Value::String(s) => {
            let text = s.to_str().map_err(|_| err("token_period is not utf-8"))?;
            parse_period(&text)
        }
        _ => Err(err("token_period must be a duration")),
    }
}

pub fn parse_period(raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(err("empty token_period"));
    }
    if let Ok(secs) = raw.parse::<u64>() {
        return Ok(secs.max(1).saturating_mul(1000));
    }
    let (num, mult) = if let Some(n) = raw.strip_suffix('d') {
        (n, 86_400u64)
    } else if let Some(n) = raw.strip_suffix('h') {
        (n, 3_600)
    } else if let Some(n) = raw.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = raw.strip_suffix('s') {
        (n, 1)
    } else {
        return Err(err(format!("bad token_period {raw}")));
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| err(format!("bad token_period {raw}")))?;
    Ok(n.max(1).saturating_mul(mult).saturating_mul(1000))
}

fn resolve_env(raw: String) -> Option<String> {
    if let Some(name) = raw.strip_prefix("env:") {
        std::env::var(name).ok().filter(|v| !v.is_empty())
    } else if raw.is_empty() {
        None
    } else {
        Some(raw)
    }
}

pub fn preset(setup: &str) -> Config {
    let (live, mem, pids, isolator) = match setup {
        "laptop" => (4usize, 4096u64, 32u64, Isolator::Cgroup),
        "cluster" => (8, 8192, 64, Isolator::Slurm),
        _ => (8, 8192, 64, Isolator::Cgroup),
    };
    Config {
        caps: Caps {
            max_live: live,
            max_depth: 3,
            max_tokens: 2_000_000,
            max_memory_mb: mem,
            max_pids: pids,
            token_period_ms: 86_400_000,
        },
        setup: setup.to_string(),
        isolator,
        human_weight: 4,
        min_ev: 0,
        value: 400_000,
        unattended: false,
        debug: 1,
        default_budget: Budget {
            tokens: 200_000,
            seconds: 3600,
            memory_mb: 1024,
            pids: 8,
        },
        decision: DecisionCfg {
            kind: DecisionKind::Off,
            endpoint: None,
            model: "gpt-6-luna".into(),
            timeout_ms: 800,
            purse_tokens: 50_000,
        },
        proxy: ProxyCfg {
            upstream: None,
            key: None,
        },
        workers: BTreeMap::new(),
    }
}

impl Config {
    pub fn builtin_box() -> Self {
        preset("box")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_tags() {
        assert_eq!(default_net(&["code".into()]), Net::Host);
        assert_eq!(default_net(&["math".into()]), Net::None);
        assert_eq!(default_net(&["math".into(), "code".into()]), Net::Host);
        assert_eq!(default_crash(&["math".into()]), OnCrash::Requeue);
        assert_eq!(default_crash(&["code".into()]), OnCrash::Fail);
    }

    #[test]
    fn explicit_keys_beat_the_preset() {
        let policy = Policy::parse(
            r#"
            setup = "laptop"
            caps = { max_live = 2, token_period = "30m" }
            workers = {
              prover = { cmd = { "prover" }, tags = { "math" } },
              pi = { cmd = { "pi" }, tags = { "code" }, net = "none", on_crash = "requeue" },
            }
            "#,
        )
        .unwrap();
        assert_eq!(policy.cfg.caps.max_live, 2);
        assert_eq!(policy.cfg.caps.max_depth, 3);
        assert_eq!(policy.cfg.caps.token_period_ms, 30 * 60 * 1000);
        assert_eq!(policy.cfg.workers["prover"].net, Net::None);
        assert_eq!(policy.cfg.workers["prover"].on_crash, OnCrash::Requeue);
        assert_eq!(policy.cfg.workers["pi"].net, Net::None);
        assert_eq!(policy.cfg.workers["pi"].on_crash, OnCrash::Requeue);
    }

    #[test]
    fn decision_table_is_read() {
        let policy = Policy::parse(
            r#"
            decision = {
              kind = "jev",
              endpoint = "http://127.0.0.1:9/decide",
              model = "m",
              timeout_ms = 50,
              purse_tokens = 3,
            }
            "#,
        )
        .unwrap();
        assert_eq!(policy.cfg.decision.kind, DecisionKind::Jev);
        assert_eq!(
            policy.cfg.decision.endpoint.as_deref(),
            Some("http://127.0.0.1:9/decide")
        );
        assert_eq!(policy.cfg.decision.timeout_ms, 50);
        assert_eq!(policy.cfg.decision.purse_tokens, 3);
        assert_eq!(
            Policy::parse("decision = { kind = \"off\" }")
                .unwrap()
                .cfg
                .decision
                .kind,
            DecisionKind::Off
        );
        assert!(Policy::parse("decision = { kind = \"guess\" }").is_err());
    }

    #[test]
    fn looping_admit_denies() {
        let policy = Policy::parse(
            r#"
            workers = { pi = { cmd = { "true" }, tags = { "code" } } }
            function admit(ctx)
              while true do end
              return "allow"
            end
            "#,
        )
        .unwrap();
        let tags = vec!["code".to_string()];
        let answer = policy.admit(&AdmitCtx {
            depth: 0,
            live: 0,
            queued: 0,
            worker: "pi",
            goal: "g",
            value: 1,
            tags: &tags,
        });
        assert_eq!(answer, Some(false));
    }

    #[test]
    fn bytecode_load_is_refused() {
        let msg = Policy::parse("load('\\27Lua')")
            .err()
            .expect("bytecode should fail")
            .to_string();
        assert!(msg.contains("policy") || msg.contains("bytecode"), "{msg}");
    }
}
