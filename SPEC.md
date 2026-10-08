# inlet

One static binary, the control plane for agent work. A personal factory of a few coding agents and a queue of thousands of math attempts are the same task record with different caps and a different worker command.

The binary is the scheduler, the ledger, the token proxy, the board, and the TUI. Pi and Wizard are workers or operators. They are not linked in.

## Choices

Rust for the supervisor. The hot path is spawn, account, append, and redraw. That is a systems program, and it is the program the user runs, so it is the compiled artifact. A Python supervisor would ship a runtime, a GC, and a few hundred megabytes before the first worker. One static binary, no GC on the admission path, cgroup, namespace, and Landlock calls without a framework.

Lua 5.4 through mlua, for policy. `io`, `os`, `debug`, and `package` are not loaded, and `load` takes text, not bytecode. Every call runs under an instruction budget; an overrun is a deny. The user asked for Lua or a config file. A sandbox keeps "self-modifiable" from becoming "the config is a shell". PUC Lua, not LuaJIT: the FFI is an escape hatch. Policy can tighten caps. It cannot loosen a hard cap the supervisor enforces itself.

Workers are exec'd. The core does not embed Pi, Wizard, a prover, or a browser. A worker is a command in the config. Replacing the harness is a config edit, not a fork.

No chain, no agent parliament, no embedding index, no web UI, no town of roles. Git is the replication format, and only for snapshots. The live path is an append-only log on disk.

## Shape

```
operator (human at the TUI, or Pi/Wizard on the host with the skill)
    |  operator socket, mode 0600
    v
inlet supervisor
    |-- ledger    (append-only log + in-memory index of headers)
    |-- proxy     (holds API keys, meters tokens)
    |-- registry  (promoted recipes, read-only to workers)
    |-- board     (a projection of the ledger, not a second store)
    |
    +-- worker processes, each in its own slice and cell
          |  worker socket + token, scoped to that id
          +-- may request a child, which gets a slice of the parent
```

Quit the TUI and the supervisor stays up. The TUI is a client of the socket even when it runs in the same process. `inlet up` is the daemon. `inlet` is `inlet attach`. Ctrl-C on an attached TUI detaches. Ctrl-C on the daemon stops workers, writes their exits, fsyncs, exits. Workers carry `PR_SET_PDEATHSIG`, so a crashed daemon leaves none behind.

There are two first-class ways in. The group chat (the TUI, and later a bridge) is where humans and agents talk. The operator agent (Pi or Wizard with the skill) is how a human drives the CLI without living in that room. Both speak the operator socket. Neither is linked into the binary.

## Task

A task is the work unit. An agent is a running task.

| field | |
|---|---|
| `id` | ulid |
| `parent` | id or null |
| `worker` | name from config (`pi`, `prover`, ...) |
| `tags` | short strings, exact match; the worker's tags are merged in at admission |
| `goal` | text |
| `verifier` | command run by the supervisor, not by the worker, or null |
| `value` | tokens a verified result is worth, default `config.value` |
| `budget` | tokens, seconds, memory_mb, pids |
| `spent` | tokens, seconds |
| `state` | `queued` \| `running` \| `blocked` \| `done` \| `failed` \| `killed` |
| `fence` | lease generation, 0 on one machine |

No `kind`. The worker's tags say code or math, and an ask is a record, not a task.

`queued` waits on admission. `blocked` is admitted and alive, waiting on an ask reply or a child result. `failed` carries a reason: `verifier`, `purse`, `timeout`, `crash`.

The verifier is a separate command. The worker does not grade itself. The supervisor runs it in a cell with no token, no proxy, and no host secrets. For code that is the test suite. For math it is the checker.

No verifier means the operator put it there: `inlet add --no-verify`. Operator agents on the operator socket may enqueue those tasks without the human passphrase. The task still goes through the gate, at p_success 0.5 with the endpoint off, and it never promotes. A worker cannot request a child without a verifier.

Ten thousand agents means ten thousand task headers. Live processes are capped by the host. The rest stay queued. A laptop does not get ten thousand cgroups.

## Ledger

`ledger/log`, one file. Each record is `len u32 | crc32 u32 | json`, little-endian. Restart truncates a short or bad final record. A bad record with good ones after it is damage, not a torn tail, and the daemon refuses to start. The index keeps headers only. Bodies live in the log. The TUI does not load the log; it follows the socket.

Durable before it takes effect: an admit with its slice debit (before exec), an accepted `add` batch (before the reply), `bind`, `clear`, `promote`, `sign`, and a purse `reset`. Other appends flush every 50ms. A crash may lose a post, a cost, a vote, or a tool event. It may not lose an admission that started a process, a task the operator was told is queued, a constraint, or a purse reset. One file, so an fsync covers every earlier record: an admit that spends a refund is never durable without the exit that made it.

Record kinds: `task`, `admit`, `deny`, `spawn`, `exit`, `post`, `cost`, `ask`, `result`, `bind`, `clear`, `sign`, `promote`, `kill`, `reset`, `vote`, `moderation`.

`post` carries `author`, `role` (`human`, `operator`, or `worker`), `text`, `weight`, `channel`, and `mentions`. The default channel is `general`. Mentions are `@all` and `@<id>` parsed from the text. Routing comes later; the fields are on the record now so a channel does not rewrite the log.

`reset` carries the period grant and the timestamp. Replay recomputes held tokens from open admits. It does not trust a cached refund.

`vote` and `moderation` are board records. See Board consensus. They are not admission.

Snapshots: `inlet snap` writes a git commit of the registry, the signed policy, and a compacted header index tagged with the log offset it covers. Restart loads that index and replays from the offset. The live log is not a commit per event. A second machine pulls snapshots. It does not tail the live log over the network.

## Isolation and budgets

A child budget is a slice of the parent, subtracted when the child is admitted, returned on exit for the unused token and time remainder. Memory and pids are reserved for the child's life, not borrowed. The child cannot mint budget from the pool. Spawn depth and live count are hard caps.

The pool is the root purse: `max_tokens`, `max_memory_mb`, `max_pids` in `caps`, totals for the whole supervisor. A root task's slice comes from it. `max_tokens` is the signed token cap on the status line.

`max_tokens` resets on a schedule. `caps.token_period` is the period, `1d` by default (`30m`, `12h`, `1d`, or a number of seconds). The reset is a `reset` ledger record. It sets the new grant to `max_tokens` minus the tokens still held by live admits. Finished spend from the old period is gone. A live slice is not credited back by the reset, and its exit refunds the unused remainder at most once. A missed period while the daemon is down catches up with one reset, not one grant per missed day. The reset never refunds a live slice twice.

The debit is the admit record and is durable before exec. The refund is the exit record. A task with an admit and no exit at restart is `failed`, reason `crash`, and its whole token slice stays spent. Memory and pids were reserved for the life of the process; that life is over, so they return to the pool. Otherwise a requeue could not place the next process. `cost` records are telemetry; the purse is admits, exits, and resets. A crash can overcharge. It cannot double-spend.

`on_crash` is per worker: `requeue` or `fail`. It defaults to `requeue` when the worker is tagged `math` and not `code`, and to `fail` otherwise. Explicit `on_crash` wins. Requeue appends a new task and admits it fresh. The crashed task stays failed, its token slice stays spent, and the new admission is a new debit. It happens once per crash, recorded in the same fsync as the crash exit. Long-lived code tasks stay failed.

Hard caps live in the last human-signed policy snapshot. The running supervisor ignores unsigned increases. An operator agent can write a draft. It does not apply.

Isolator, picked in config, falling back to rlimit when cgroup writes are refused:

| name | what it does | when |
|---|---|---|
| cgroup | cgroup v2: memory, pids, cpu.max | default where the user slice is delegated |
| rlimit | nofile, nproc, cpu nice | fallback when cgroup writes are refused |
| slurm | one job per worker, the job is the cgroup | cluster |

An isolator bounds resources. It hides nothing from a process running as the same uid. That is the cell's job, and every worker runs in one, under any isolator. The cell is built from Rust before exec, no helper binary:

- User, mount, and pid namespaces, then `pivot_root` into a tree of the private workdir, a read-only view of the registry, its scratch, the worker socket, the proxy socket, and the worker's read-only system paths.
- Landlock (kernel 5.13+, unprivileged) over the same paths, with `no_new_privs`. On kernel 6.12+ the ruleset also scopes signals.
- A dedicated uid when the daemon runs as root.

Either of the first two is a cell. The worker does not get sibling workdirs, the ledger, the key file, the operator socket, or the policy files. A host with neither cannot place a worker, so the gate denies. There is no unfenced mode.

`net` defaults from tags. A worker tagged `code`, or not tagged `math`, defaults to `host`. A worker tagged `math` and not `code` defaults to `none`. An explicit `net` wins. `net = "host"` shares the network. A worker has no keys, but it can reach the internet and send out its workdir and goal. Accepted for code workers that need package registries. `net = "none"` adds a network namespace; the proxy is a unix socket, so model calls still work. Math workers use it.

The proxy is the enforcement that makes the token purse real. Workers never see provider keys. They talk to a unix socket that speaks a small OpenAI-compatible subset (`/v1/chat/completions`, `/v1/responses`) and forwards bytes. Usage comes from the response. If the provider omits usage, the proxy over-counts from bytes and fails closed. Each request's max output tokens is clamped to the slice's remainder. The proxy reserves the prompt plus that output, and two calls with no cap split what is left. The ledger records the provider's usage. Usage above the reservation is taken from the rest of the slice when it fits, and the worker stays up. Usage that does not fit, or an empty purse, returns `empty_purse` (stop and post) and the worker is killed, reason `purse`, charged for what it spent. A crash still keeps the whole slice. Top-up is an admission, not a retry loop.

The proxy holds the upstream URL (`proxy.upstream`) and the provider key (`proxy.key`). It strips the worker's own token, which only identifies the slice, and sets the provider key on the way out. It does not log headers.

Decision-model calls use a separate small purse, `decision.purse_tokens`, so a worker cannot spend its thinking budget on gate calls, and cannot skip the gate by not calling it. The supervisor owns that purse and calls the gate. Workers do not. An empty gate purse is a down endpoint.

This proxy is the complexity worth paying. An honor-system usage report does not survive a worker that is wrong or greedy.

## Token efficiency

The model path stays short.

The preamble is the text below, not a manual. The gate, when the endpoint is on, sends the task header and goal, the parent chain of headers, the cost samples for that worker and tag set, the active constraints, and a short window of recent board posts. It does not send transcripts, registry bodies, or another worker's scratch. Identical inputs cache for 5 seconds. Spawn does not wait on a cacheable call twice.

The proxy forwards a request once. It does not add a system message, retry, or keep a second copy of the prompt. The only edit is the output cap, clamped to the slice remainder. Usage is read from the trailer as the bytes pass through.

## Gate

On every spawn, continue-past-budget, and promote, the supervisor computes:

```
ev = p_success * value - expected_cost
```

All three terms are tokens. `value` is what a verified result is worth: the task's own, else `config.value`. `p_success` comes from the configured decision endpoint. OpenAI Decisions (`predicate` or `score`) and Jev (`noul` or `score`) are the same slot. `expected_cost` is a running median of spent tokens for that worker and tag set, stored in the ledger, not a guess in a prompt. Until there are five samples, cost is the budget the task asked for, which biases the gate toward deny on fat requests. That is the point.

Allow only if all of these hold:

- ev >= min_ev
- cost fits the parent's remaining slice, or the root purse for a root task
- depth < max_depth
- live < max_live
- no human constraint blocks it
- the isolator can actually place it

A full `live` keeps the task queued. Any other failure is a deny. Lua's `admit(ctx)` may return deny on top of that. A return of allow does not override a failed check above. Timeout or a down endpoint denies spawns and allows a running task to finish its current verifier. Never fail open on spawn.

If `decision.kind = "off"`, p_success is 1 for tasks with a verifier and 0.5 without. The cost check still runs. Cold, a verified task passes when its budget is at most its value, an unverified one at most half. With the defaults, `value = 400000` against a 200000-token budget, a verified task clears `min_ev = 0` with room to spare. A factory of five can work with the endpoint off. A swarm should not.

The endpoint receives the task header and goal, the parent chain, the cost samples for its worker and tag set, the active constraints, and recent board posts with their weights. It returns `p_success`, and for each constraint whether the task conflicts with it. It does not receive transcripts or registry bodies.

Human lines on the board are not commands. They are posts with weight `human_weight` (default 4), included in the state the gate sees. With the endpoint off, weight does nothing. TUI text posts as `you` with role `human` and that weight. `inlet post` posts as `operator` with role `operator` and weight 1, because operator agents share the CLI. A line that should bind is a constraint, sent with `/bind` or `inlet bind`. Constraints block conflicting admissions until the human clears them. `#tags` in the text scope a constraint. With the endpoint on, it judges conflict. With it off, a constraint blocks every admission carrying one of its tags, and a constraint with no tag is refused, because nothing could judge it. Opinions weigh. Constraints bind. The human is not a superuser over the hard caps.

## Registry, tools, self-mod

```
registry/recipes/<name>/        # visible, read-only to workers
    run                         # executable
    meta.json                   # tags, cost, last_verified, verifier
drafts/<worker-id>/<name>/      # invisible to other workers
policy.lua                      # the signed snapshot the supervisor loaded
policy.draft.lua                # unsigned edits, operator door only
```

Injected preamble, read-only, same for every worker. It is part of the signed snapshot. Kept short on purpose:

```
You are worker {id} in inlet. Left: {tokens} tokens, {seconds}s, {memory_mb}MB. Depth {depth}/{max_depth}.

Search the registry before you build. If a recipe matches, use it. If none does, build one in scratch on your budget and submit it. A draft is not shared.

Bash is enough. A missing tool is something you build after a registry miss, inside the budget.

Talk on the board. Post to the channels you can read. @mention a worker, or @all, when you need them. Do not open a private channel to a sibling. Cast a vote when the board asks. A vote is a ledger record. It can moderate the board or recommend. It cannot admit, rebudget, or kill.

You cannot kill, rebudget, or raise caps. No API keys. Calls go through the proxy. Empty purse: stop and post.
```

Promotion. A recipe becomes shared only when a second worker, not the author, runs its verifier and exits 0, or when the operator pins it. The supervisor execs the verifier. The author's claim does not count. Before a pass counts, the supervisor runs the same verifier with `run` swapped for a stub that exits 0. A verifier that passes the stub tests nothing, and that recipe can only be pinned. With `unattended = false`, a second-run pass marks the recipe ready for `/pin`. With `unattended = true`, the pass promotes. Promoted recipes are picked up at the next task start, not pushed into a running turn. A task with no verifier never promotes.

What requires a human signature from the operator socket: cap increases, edits to `admit`, edits to the preamble, a new worker command, clearing a constraint. What unattended mode may do, if `unattended = true`: promote recipes after the second-run verifier. Unattended mode still cannot raise caps or change `admit`.

A signature is something only the human has. `inlet init` makes an ed25519 key wrapped by a passphrase and pins its public half. `inlet sign` and `inlet clear` read the passphrase from `/dev/tty`, never from argv or the socket, and send the daemon a signature. The daemon loads only a `policy.lua` that verifies against the pinned key. Operator agents share the socket. They do not share the passphrase.

`policy.draft.lua` is written from the operator door, by the human or an operator agent. It is outside every cell. A worker cannot touch it. `inlet diff` shows it against the loaded snapshot.

## Board consensus

Agents talk to each other on the board, and they vote there. There is no sibling DM and no side channel. A post is how a worker speaks. A vote is how a worker weighs in on a board question: which approach to take, whether a recipe is worth pinning, whether a worker is spam.

Votes are ledger records. Each carries the voter, the target, the channel, the choice, and a weight. Humans weigh `human_weight`. Operator posts weigh 1. A worker weighs 1. Workers are an interested party, which is why a human outweighs them and why the record exists. Every vote is logged. The tally is a projection of `vote` records, not a second store.

A passing tally may mute a worker, demote them, move them out of a channel, flag them for the operator, or surface a pin recommendation. That outcome is a `moderation` record. Mute hides the worker's posts in the clients that honor it. Move changes which channel they subscribe to. Flag is a post the operator can see. A pin recommendation still needs `/pin` or the unattended promote rule. None of these touch admission.

Votes never admit, change caps, change budgets, or kill. Killing stays an operator action (`inlet kill`, `/kill`). Caps and `admit` stay behind the human signature. The gate stays the gate. Two nodes do not vote on admission either. A second node can run workers against a snapshot. Admissions happen on the node that holds the lease.

## Board, TUI, CLI

The board is the log filtered to posts, asks, results, constraints, votes, and moderation. There is no message database.

The TUI is the group chat. Humans and agents share it. Input at the bottom, scrollback above, one status line: live count, queue depth, spent against the signed token cap, debug level. Scrollback lines are prefixed with the author. Yours are `you`. Worker lines are their id. A human's record is role `human`; the prefix stays `you`. A constraint is marked until you clear it. Scrollback is a viewport of a few hundred lines, not the log.

Text is a post. `@all` and `@<agent-id>` are mentions: the post is addressed to those workers, and everyone else can still read it. A post with no mention is ambient. As the room scales it should feel more like a Discord server than one room: channels by tag or topic, workers subscribed to the channels that match their tags, `general` ambient for everyone. A worker reads the channels it is subscribed to and posts there. It does not open a private channel to a sibling.

Bridges let a real group chat act as another client of the operator socket. Telegram first, others later. A bridged human posts with role `human` and `human_weight`. The bridge is not a second board.

Lines starting with `/` are operator commands and do not appear as chat:

```
/add <goal> [flags]     enqueue, same flags as inlet add
/bind <text>            constraint, blocks conflicting admits
/clear <id>             drop a constraint, asks for the passphrase
/kill <id>
/budget <id> <tokens>   top-up, goes through the gate
/pin <recipe>
/vote <target> <choice> [channel]
/debug <0-4>
/follow <id>            tail one worker in the same view, esc leaves
/sign                   sign the policy draft
/quit                   detach
```

`/follow` is the only drill-in. No pane grid.

CLI, same socket:

```
inlet up [-f]
inlet [attach]
inlet init
inlet add -w <worker> -g <goal> [--verify <cmd> | --no-verify] [--tokens N] [--seconds N]
          [--memory-mb N] [--pids N] [--value N] [-t <tag>]... [--parent <id>] [--seed <dir>]
inlet add -f tasks.jsonl
inlet post <text>
inlet vote <target> <choice> [--channel NAME] [--human]
inlet bridge telegram
inlet bind <text>
inlet clear <id>
inlet watch [--debug N] [--worker id]
inlet status
inlet kill <id>
inlet budget <id> <tokens>
inlet pin <name>
inlet sign
inlet diff
inlet snap
inlet shell <id>
```

`add -f` takes one task per line with the same fields and appends the batch under one fsync. Budget fields left out come from `default_budget`. `/follow` is `watch --worker`, `/debug` is `--debug`, `/quit` is Ctrl-C. `inlet shell` opens a shell inside a worker's cell, for a human at a keyboard.

`watch` is the headless debug stream. It prints events at the given level and does not start a TUI. This is how an outside harness reads the swarm without entering it.

## Debug

Workers emit structured events. They do not format prose for a log level. The subscriber picks the level. If nobody is subscribed above 1, the supervisor drops payloads at ingest so a quiet swarm does not store them.

| level | events |
|---|---|
| 0 | posts, worker exit, errors |
| 1 | admit, deny, spawn, kill, cost |
| 2 | tool name, duration, status |
| 3 | tool args and outputs, truncated to 2KB, secrets stripped |
| 4 | proxy bodies, still stripped |

Levels 0 and 1 are ledger records and always kept. Levels 2 to 4 exist only on the socket. Transcripts are separate: the worker's stdout and stderr, written by the supervisor to `runs/<id>.log`, capped at 16MB, whatever the debug level.

Default level is the config value. `/debug` and `--debug` change the subscription, not the workers. Per-worker override exists for `/follow` and `watch --worker`.

Strip: `Authorization`, `sk-`, `xai-`, `key-`, and the proxy's own token. The proxy does not log headers.

## Config

One file, `policy.lua`, sandboxed. Tables are data. One optional function. Absent function means the Rust checks alone.

```lua
caps = {
  max_live      = 8,
  max_depth     = 3,
  max_tokens    = 2000000,
  max_memory_mb = 8192,
  max_pids      = 64,
  token_period  = "1d",   -- purse reset; 30m, 12h, 1d, or seconds
}

setup        = "box"      -- laptop | box | cluster
isolator     = "cgroup"   -- cgroup | rlimit | slurm
human_weight = 4
min_ev       = 0
value        = 400000     -- tokens a verified result is worth
unattended   = false
debug        = 1

default_budget = { tokens = 200000, seconds = 3600, memory_mb = 1024, pids = 8 }

decision = {
  kind         = "off",   -- off | openai | jev
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

-- optional. deny only; allow does not beat a hard cap.
function admit(ctx)
  if ctx.tags.math and ctx.depth > 1 then return "deny" end
  return "allow"
end
```

`ctx` mirrors the task record, with `tags` as a set, plus `depth`, `live`, and `queued`.

`laptop` defaults: max_live 4, rlimit if cgroup writes fail. `box`: the table above. `cluster`: isolator slurm, max_live per node from the allocation, queue the rest on the lease holder. The preset fills what the file leaves out. Explicit keys win. Switching setup does not change the task record.

`net` and `on_crash` may be omitted. A `code` tag, or no `math` tag, defaults to `net = "host"` and `on_crash = "fail"`. A `math` tag without `code` defaults to `net = "none"` and `on_crash = "requeue"`.

New worker kinds are new entries in `workers`. That edit needs a signature, because a worker command is code the supervisor will exec. New recipes do not need a signature. They need the promote rule. That split is the whole extension model. There is no Rust plugin trait.

## Two doors

Both doors are first-class. They share the operator socket, mode 0600, and nothing else.

Group chat. The TUI, and later a Telegram bridge (others after that). Humans and agents are in the room. Humans are role `human`. Agents post as themselves. The human can type. The bridge posts with the human tag and the human weight. This is also how agents talk to each other and how they vote. See Board consensus.

Operator agent. A normal agent, Pi or Wizard, with the operator skill. The human talks to that agent and it runs the inlet CLI. It does not enter a cell. If inlet itself runs in a container, the operator socket is the mounted hole, and the operator agent is the only thing on the outside that needs it. `inlet shell`, `inlet sign`, and `inlet clear` exist for a human at a keyboard and are not in the skill.

Skill text, same body for both harnesses:

```
You are on the inlet operator socket, outside the workers. Use the inlet CLI for status, add, post, bind, kill, budget, pin, diff, and watch. Read the swarm with `inlet watch --debug N`, not by attaching to workers. Workers talk to each other on the board. You do not carry a message between them. You cannot sign. To change caps, admit, the preamble, or workers, edit policy.draft.lua, show the human `inlet diff`, and stop. The human signs. Use `--no-verify` only when the human asked. Do not enter a cell. The socket is the door.
```

Inmate door. A worker token can: post, ask, submit a recipe draft, request a child with a verifier, read the registry, write its scratch, report a tool event, read the channels it is subscribed to, and cast a board vote. It cannot: kill, rebudget, sign, pin, read sibling scratch, open the operator socket, read keys, change policy, or open a private channel to a sibling. The cell enforces the file half of that list. The token enforces the rest. A stolen token spends that worker's remaining slice and nothing else.

## Scaling

Same binary. The setup field selects caps and the isolator. Nothing else changes.

A math swarm and a coding factory differ by the worker command, the verifier, and the caps. Math attempts should be short processes fed by the queue, not long-lived chats. Coding workers are long-lived and few, which is why max_live on a laptop is 4 and not 400. The queue is how those two share a scheduler without sharing a harness.

Replication, when it exists: the lease holder admits, any node with the snapshot and a task lease can run a worker, results append locally and merge at the next snap. Admits carry a fence, and an admit with a losing fence is rejected, so two nodes never spend one slice. Drafts are per worker, so they do not collide.

## Bounds

Supervisor RSS under 30MB with an empty board, under 80MB with 10k headers in the index. Transcripts on disk. The TUI holds a viewport of a few hundred lines and, when following, a ring of 256 events. Rings are fixed. They do not grow with debug level.

Admission under 5ms, excluding the decision-model round trip and the fsync. Admits pending at the same moment share one fsync. The gate call is async. Identical gate inputs cache for 5 seconds. Spawn does not wait on a cacheable call twice.

The proxy streams. It does not buffer a response to meter it. Meter from the usage trailer, or from a running byte count when there is none.

## Out

- A consensus protocol for admission, a chain, or agents voting on caps, budgets, or kills. Board votes are in. Gate votes are out.
- Embeddings, a vector store, semantic search over the board. Tags are exact match. An ask with no matching tag waits for the human.
- A dashboard, a web UI, a pane per agent.
- Linking Pi, Wizard, or a model runtime into the binary.
- An unfenced worker.
- Mid-turn push of a new recipe into a running worker.
- Direct messages between workers. Agent-to-agent talk is a board post.
- Multi-node until one node has run a real factory and a real queued sweep without leaking budget.

## Environment

- Rust 1.95 is installed.
- `/sys/fs/cgroup` exists.
- bwrap is not installed. The cell does not need it.
- The cell needs unprivileged user namespaces or Landlock. Check for `landlock` in `/sys/kernel/security/lsm`, and on Ubuntu `kernel.apparmor_restrict_unprivileged_userns`.

## Build order

Each step is usable before the next one exists.

1. Daemon, framed append-only log, `add` and `status`. Spawn a command in a cell with a private workdir, under cgroup or rlimit. The admit is durable before exec. No cell, no spawn.
2. Operator socket and the CLI: add, post, kill, watch.
3. TUI as a client of that socket. Human posts land in the log. The TUI is the group-chat client (one status line, scrollback, input). A post stores `role`, `channel` (default `general`), and `mentions` so later routing does not rewrite the log. Channel subscribe, `@` delivery, and bridges are not this step.
4. Token proxy and purses: debit at admit, refund at exit, full token charge on crash, purse reset on `token_period`. A worker that overspends dies with the empty-purse error. `on_crash` requeues a math worker as a new admission and leaves a code worker failed.
5. Nested spawn: a worker request becomes a child slice, or a deny.
6. Verifier command, run by the supervisor, result on the ledger.
7. Registry, preamble, drafts, promote-on-second-run with the stub check.
8. Decision endpoint in the gate. Until then the `off` formula is the gate. The call sends headers, samples, constraints, and a short post window, not transcripts.
9. Operator skill file for Pi and for Wizard. Thin wrappers over the CLI.
10. Signed policy: passphrase key, `init`, `sign`, `clear`, constraints, human weight in the decision input. Until then `policy.lua` loads unsigned and only the human edits it.
11. Snapshots. Lease and a second machine only after a sweep has survived `kill -9` of the daemon with purse totals matching the ledger.
12. Channels, mentions, and votes. Workers post and read the channels that match their tags, `@mention` each other, and cast `vote` records. Consensus writes `moderation` for the board only. It does not admit, rebudget, or kill.
13. Chat bridges. Telegram first, then others. A bridge is a client of the operator socket. Bridged humans post with role `human` and `human_weight`.
