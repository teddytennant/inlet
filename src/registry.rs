//! Recipe drafts, the read-only registry, and the stub check.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{err, Result};
use crate::id::now_ms;
use crate::model::{Decoded, Record};

const PREAMBLE: &str = "\
You are worker {id} in inlet. Left: {tokens} tokens, {seconds}s, {memory_mb}MB. Depth {depth}/{max_depth}.

Search the registry before you build. If a recipe matches, use it. If none does, build one in scratch on your budget and submit it. A draft is not shared.

Bash is enough. A missing tool is something you build after a registry miss, inside the budget.

Talk on the board. Post to the channels you can read. @mention a worker, or @all, when you need them. Do not open a private channel to a sibling. Cast a vote when the board asks. A vote is a ledger record. It can moderate the board or recommend. It cannot admit, rebudget, or kill.

You cannot kill, rebudget, or raise caps. No API keys. Calls go through the proxy. Empty purse: stop and post.
";

pub fn render_preamble(
    id: &str,
    tokens: u64,
    seconds: u64,
    memory_mb: u64,
    depth: u32,
    max_depth: u32,
    template: Option<&str>,
) -> String {
    let template = template.unwrap_or(PREAMBLE);
    template
        .replace("{id}", id)
        .replace("{tokens}", &tokens.to_string())
        .replace("{seconds}", &seconds.to_string())
        .replace("{memory_mb}", &memory_mb.to_string())
        .replace("{depth}", &depth.to_string())
        .replace("{max_depth}", &max_depth.to_string())
}

pub fn snapshot(live: &Path, dest: &Path) -> Result<()> {
    copy_tree(&live.join("recipes"), &dest.join("recipes"))
}

pub fn submit(
    home: &Path,
    author: &str,
    name: &str,
    run: &str,
    verifier: &str,
    tags: Vec<String>,
) -> Result<()> {
    check_name(name)?;
    if run.len() > 64 * 1024 || verifier.len() > 4 * 1024 {
        return Err(err("draft is too large"));
    }
    if verifier.trim().is_empty() {
        return Err(err("draft needs a verifier"));
    }
    let dir = home.join("drafts").join(author).join(name);
    fs::create_dir_all(&dir)?;
    fs::write(dir.join("run"), run)?;
    fs::set_permissions(dir.join("run"), fs::Permissions::from_mode(0o755))?;
    write_meta(
        &dir,
        &Meta {
            name: name.into(),
            author: author.into(),
            verifier: verifier.into(),
            tags,
            ready: false,
            stub: false,
        },
    )?;
    Ok(())
}

/// Copy a recipe's `run` into a workdir. Promoted copy wins over a draft.
pub fn stage_run(home: &Path, name: &str, work: &Path) -> Result<()> {
    check_name(name)?;
    let promoted = home.join("registry/recipes").join(name).join("run");
    let src = if promoted.is_file() {
        promoted
    } else {
        find_draft(home, name)?.join("run")
    };
    fs::create_dir_all(work)?;
    fs::copy(&src, work.join("run"))?;
    fs::set_permissions(work.join("run"), fs::Permissions::from_mode(0o755))?;
    Ok(())
}

/// A second worker passed. Author does not count. A vacuous verifier only pins.
/// Returns a promote record when unattended mode should publish now.
pub fn consider(
    home: &Path,
    unattended: bool,
    task_id: &str,
    recipe: &str,
) -> Result<Option<Record>> {
    let draft = find_draft(home, recipe)?;
    let mut meta = read_meta(&draft)?;
    if meta.author == task_id {
        return Ok(None);
    }
    if !real_passes(&meta.verifier, &draft) {
        return Ok(None);
    }
    if vacuous(&meta.verifier, &draft) {
        meta.stub = true;
        meta.ready = false;
        write_meta(&draft, &meta)?;
        return Ok(None);
    }
    meta.stub = false;
    meta.ready = true;
    write_meta(&draft, &meta)?;
    if unattended {
        Ok(Some(promote_record(recipe, task_id)))
    } else {
        Ok(None)
    }
}

pub fn promote_record(name: &str, by: &str) -> Record {
    Record::Promote {
        name: name.to_string(),
        by: by.to_string(),
        ts: now_ms(),
    }
}

pub fn ready(home: &Path, name: &str) -> bool {
    find_draft(home, name)
        .ok()
        .and_then(|dir| read_meta(&dir).ok())
        .is_some_and(|meta| meta.ready)
}

pub fn materialize(home: &Path, name: &str) -> Result<()> {
    let draft = find_draft(home, name)?;
    let meta = read_meta(&draft)?;
    let dest = home.join("registry/recipes").join(name);
    fs::create_dir_all(&dest)?;
    fs::copy(draft.join("run"), dest.join("run"))?;
    fs::set_permissions(dest.join("run"), fs::Permissions::from_mode(0o755))?;
    let body = json!({
        "tags": meta.tags,
        "verifier": meta.verifier,
        "author": meta.author,
        "cost": 0,
        "last_verified": now_ms(),
    });
    fs::write(dest.join("meta.json"), serde_json::to_vec(&body)?)?;
    Ok(())
}

pub fn replay(home: &Path, records: &[Decoded]) -> Result<()> {
    for decoded in records {
        let Decoded::Rec(rec) = decoded else {
            continue;
        };
        let Record::Promote { name, .. } = rec.as_ref() else {
            continue;
        };
        let run = home.join("registry/recipes").join(name).join("run");
        if !run.is_file() {
            let _ = materialize(home, name);
        }
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Meta {
    name: String,
    author: String,
    verifier: String,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    stub: bool,
}

fn check_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 48
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        Err(err("bad recipe name"))
    }
}

fn find_draft(home: &Path, name: &str) -> Result<PathBuf> {
    check_name(name)?;
    let root = home.join("drafts");
    let mut hits = Vec::new();
    if root.is_dir() {
        for author in fs::read_dir(&root)?.flatten() {
            let dir = author.path().join(name);
            if dir.join("meta.json").is_file() {
                hits.push(dir);
            }
        }
    }
    if hits.len() == 1 {
        return Ok(hits.remove(0));
    }
    let ready: Vec<_> = hits
        .into_iter()
        .filter(|dir| read_meta(dir).is_ok_and(|meta| meta.ready))
        .collect();
    if ready.len() == 1 {
        Ok(ready.into_iter().next().unwrap())
    } else {
        Err(err(format!("no draft {name}")))
    }
}

fn read_meta(dir: &Path) -> Result<Meta> {
    let bytes = fs::read(dir.join("meta.json"))?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn write_meta(dir: &Path, meta: &Meta) -> Result<()> {
    fs::write(dir.join("meta.json"), serde_json::to_vec(meta)?)?;
    Ok(())
}

fn real_passes(verifier: &str, draft: &Path) -> bool {
    let tmp = scratch_dir("real");
    let Ok(()) = (|| -> Result<()> {
        fs::create_dir_all(&tmp)?;
        fs::copy(draft.join("run"), tmp.join("run"))?;
        fs::set_permissions(tmp.join("run"), fs::Permissions::from_mode(0o755))?;
        Ok(())
    })() else {
        let _ = fs::remove_dir_all(&tmp);
        return false;
    };
    let code = run_cmd(verifier, &tmp);
    let _ = fs::remove_dir_all(&tmp);
    code == 0
}

fn vacuous(verifier: &str, _draft: &Path) -> bool {
    let tmp = scratch_dir("stub");
    if fs::create_dir_all(&tmp).is_err() {
        return false;
    }
    let wrote = fs::write(tmp.join("run"), "#!/bin/sh\nexit 0\n").is_ok()
        && fs::set_permissions(tmp.join("run"), fs::Permissions::from_mode(0o755)).is_ok();
    if !wrote {
        let _ = fs::remove_dir_all(&tmp);
        return false;
    }
    let code = run_cmd(verifier, &tmp);
    let _ = fs::remove_dir_all(&tmp);
    code == 0
}

fn scratch_dir(kind: &str) -> PathBuf {
    std::env::temp_dir().join(format!("inlet-{kind}-{}-{}", std::process::id(), now_ms()))
}

fn run_cmd(cmd: &str, dir: &Path) -> i32 {
    crate::cell::run_sealed(&["/bin/sh".into(), "-c".into(), cmd.into()], dir)
}

fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    if !src.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), &to)?;
        }
    }
    Ok(())
}
