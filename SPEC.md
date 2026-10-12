# inlet

One static binary, the control plane for agent work. A personal factory of a few coding agents and a queue of thousands of math attempts are the same task record with different caps and a different worker command.

The binary is the scheduler, the ledger, the token proxy, the board, and the TUI. Pi and Wizard are workers or operators. They are not linked in.

You say what should be true and the supervisor keeps it true. A swarm is a count of tasks to keep in flight. A failed task is tried again with what went wrong in its prompt. A dead daemon comes back and picks up where the ledger stops. No step of that waits on a person. The verifier, the budget, and the caps stand where a reviewer would.

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

Quit the TUI and the supervisor stays up. The TUI is a client of the socket even when it runs in the same process. `inlet up` is the daemon. `inlet` is `inlet attach`. Ctrl-C on an attached TUI detaches. Ctrl-C on the daemon stops workers, writes their exits, fsyncs, exits. SIGTERM is the daemon going away, not the work: it fsyncs and exits, and the workers stay up for the next one. A daemon that dies without that chance leaves its workers running, and the next one takes them back. See Desired state.

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
| `plan` | `attempt`, `max_attempts`, `not_before`, and the owning swarm. Empty on a plain `add` with no retries |

No `kind`. The worker's tags say code or math. A question is a board post or a child task. There is no ask record and nothing that waits for a person to answer.

`queued` waits on admission. `blocked` is admitted and alive, waiting on a child result. `failed` carries a reason: `verifier`, `exit`, `timeout`, `stall`, `crash`, `stopped`, `purse`, `cell`, or the gate's deny.

The verifier is a separate command. The worker does not grade itself. The supervisor runs it in a cell with no token, no proxy, and no host secrets. For code that is the test suite. For math it is the checker.

No verifier means the operator put it there: `inlet add --no-verify`. Operator agents on the operator socket may enqueue those tasks without the human passphrase. The task still goes through the gate, at p_success 0.5 with the endpoint off, and it never promotes. A worker cannot request a child without a verifier.

Ten thousand agents means ten thousand task headers. Live processes are capped by the host. The rest stay queued. A laptop does not get ten thousand cgroups.

## Desired state

Two loops run on every pass of the supervisor, before admission. Both are projections of the ledger. Neither keeps a store of its own.

Retry. A task that fails for a reason a new attempt can fix gets one: `verifier`, `exit`, `timeout`, `stall`, `crash`, `gate`. Not `killed`, which someone asked for. Not `purse`, which would run dry again on the same budget. Not a deny by a cap. The retry is a new task with `retry_of` set, a new admission, and a new debit. It is written in the same fsync as the exit that caused it, so a failure cannot lose its retry. `plan.attempt` counts up to `plan.max_attempts`, which is `1 + retries` from the task, or `1 + retry.max` from the policy. `plan.not_before` holds it back: `retry.backoff`, doubled per failure, capped at `retry.max_backoff`.

The retry keeps the workdir. The supervisor renames the old one into place, so the worker starts where the last attempt stopped. Its prompt says which attempt this is, how the last one ended, and the last lines of the verifier's output and its own. That is the whole feedback path. No human reads a failure and rewrites the goal.

`stopped` is a task the daemon shut down under. It reruns on the next start as the same attempt, because the task did nothing wrong. A child does not rerun alone. It goes down with its parent, and the parent hands the part off again.

Swarm. `inlet apply` writes a `swarm` record: a name, `replicas`, `completions`, `backoff_limit`, and a task template. The supervisor stamps tasks from the template until `replicas` are in flight, and stops when `completions` are done. `completions` defaults to `replicas`. `completions = 0` never finishes, and paces itself by `retry.backoff` after each task so an instant worker cannot spin the ledger. `{index}` in the goal is the number of the task in its swarm.

A stamped task carries the swarm's name in `plan`. Its own retries run first. A failure counts against the swarm only when the chain is spent. Failures in a row back the swarm off like a retry, and at `backoff_limit` (default 6, 0 for never) the swarm stalls: it stamps nothing and says so on `general`. Any new spec for that name, `apply` or `scale`, clears the streak. A swarm never stamps faster than once a second after a failure, or after a success when it has no end, whatever `retry.backoff` says. Counts are kept by the record stream, not by the running policy, so a replay under an edited policy lands on the same numbers. The pause is not stored at all: the swarm keeps when it last settled, and the policy in force says how long to wait from there.

`inlet scale` writes the same record with a new `replicas`. It changes how many run at once, not how many run in total. `inlet delete` stops the stamping, and with `--kill` stops what was stamped. Tasks left running no longer belong to a swarm, so a later one of the same name starts from zero. Applying a name again replaces the spec and keeps the counts. Tasks already stamped keep the old template.

Adoption. The control plane can die without taking the work with it. Each worker runs under a keeper, a parent that stays in the cell with it and holds nothing of the daemon's: not the ledger lock, not a socket, not its memory or environment. The keeper is this binary exec'd fresh inside the cell, never a fork left running. The keeper copies the worker's output to the transcript, and when the worker exits it writes the wait status to `run/exit/<id>`. The daemon notes pid, start time, and token in `run/live/<id>`, mode 0600, outside every cell.

A new daemon reads those notes before it calls anything a crash. A worker still running is taken back: same task, same attempt, same token, with its spend so far read from the ledger. One that finished while nobody watched is settled from its recorded status, and its verifier runs. A task whose verifier was running gets the verifier again. Only a task with no process and no status is a `crash`, and then the two loops do the rest: chains with attempts left retry, and swarms stamp replacements. A child whose parent did not make it is killed.

The sockets are mounted into a cell by directory, read-only, so a kept worker finds the socket the new daemon binds. A model call in flight when the daemon dies is lost, and until the new daemon is up the proxy refuses connections. `inlet door` retries for a minute. A harness should do the same for its model calls, and the preamble says so. With no daemon there is no proxy, so an orphan spends nothing, and its keeper kills it 15 seconds past its `seconds`. A kept worker's meter is rebuilt from `cost` records, so with adoption on they are durable appends, not soft ones. `adopt = false` goes back to `PR_SET_PDEATHSIG`: the workers die with the daemon.

## Ledger

`ledger/log`, one file. Each record is `len u32 | crc32 u32 | json`, little-endian. Restart truncates a short or bad final record. A bad record with good ones after it is damage, not a torn tail, and the daemon refuses to start. The index keeps headers only. Bodies live in the log. The TUI does not load the log; it follows the socket.

Durable before it takes effect: an admit with its slice debit (before exec), an accepted `add` batch (before the reply), a `swarm` and the tasks it stamps, a retry (with the exit it follows), `bind`, `clear`, `promote`, `sign`, and a purse `reset`. Other appends flush every 50ms. A crash may lose a post, a cost, a vote, or a tool event. It may not lose an admission that started a process, a task the operator was told is queued, a constraint, or a purse reset. One file, so an fsync covers every earlier record: an admit that spends a refund is never durable without the exit that made it.

Record kinds: `task`, `admit`, `deny`, `spawn`, `exit`, `post`, `cost`, `gate`, `result`, `bind`, `clear`, `sign`, `promote`, `kill`, `reset`, `vote`, `moderation`, `swarm`, `swarm_delete`. A binary that meets a kind it does not know leaves it in the log and moves on.

`post` carries `author`, `role` (`human`, `operator`, or `worker`), `text`, `weight`, `channel`, and `mentions`. The default channel is `general`. Mentions are `@all` and `@<id>` parsed from the text. Routing comes later; the fields are on the record now so a channel does not rewrite the log.

`reset` carries the period grant and the timestamp. Replay recomputes held tokens from open admits. It does not trust a cached refund.

`vote` and `moderation` are board records. See Board consensus. They are not admission.

Snapshots: `inlet snap` writes a git commit of the registry, the signed policy, and a compacted header index tagged with the log offset it covers. Restart loads that index and replays from the offset. The live log is not a commit per event. A second machine pulls snapshots. It does not tail the live log over the network.

## Isolation and budgets

A child budget is a slice of the parent, subtracted when the child is admitted, returned on exit for the unused token and time remainder. A child that names no budget gets half of what its parent has left of tokens, memory, and pids, and no more seconds than the parent has. The policy default is sized for a root and would never fit inside one. Memory and pids are reserved for the child's life, not borrowed. The child cannot mint budget from the pool. Spawn depth and live count are hard caps.

The pool is the root purse: `max_tokens`, `max_memory_mb`, `max_pids` in `caps`, totals for the whole supervisor. A root task's slice comes from it. `max_tokens` is the signed token cap on the status line.

`max_tokens` resets on a schedule. `caps.token_period` is the period, `1d` by default (`30m`, `12h`, `1d`, or a number of seconds). The reset is a `reset` ledger record. It sets the new grant to `max_tokens` minus the tokens still held by live admits. Finished spend from the old period is gone. A live slice is not credited back by the reset, and its exit refunds the unused remainder at most once. A missed period while the daemon is down catches up with one reset, not one grant per missed day. The reset never refunds a live slice twice.

The debit is the admit record and is durable before exec. The refund is the exit record. A task with an admit and no exit at restart is `failed`, reason `crash`, and its whole token slice stays spent. Memory and pids were reserved for the life of the process; that life is over, so they return to the pool. Otherwise a requeue could not place the next process. `cost` records are telemetry; the purse is admits, exits, and resets. A crash can overcharge. It cannot double-spend.

`on_crash` is per worker and only speaks to `crash`. Left out, a crash follows `retry` like any other failure. `fail` never retries a crash, for a worker whose half-finished state is not safe to resume. `requeue` allows one retry of a crash even when `retry.max` is 0, and is the default for a worker tagged `math` and not `code`. The crashed task stays failed, its token slice stays spent, and the new admission is a new debit.

Hard caps live in the last human-signed policy snapshot. The running supervisor ignores unsigned increases. An operator agent can write a draft. It does not apply.

Isolator, picked in config, falling back to rlimit when cgroup writes are refused:

| name | what it does | when |
|---|---|---|
| cgroup | cgroup v2: memory, pids, cpu.max | default where the user slice is delegated |
| rlimit | data, nofile, nproc, cpu nice | fallback when cgroup writes are refused |
| slurm | one job per worker, the job is the cgroup | cluster |

An isolator bounds resources. It hides nothing from a process running as the same uid. That is the cell's job, and every worker runs in one, under any isolator. The cell is built from Rust before exec, no helper binary:

- User, mount, and pid namespaces, then `pivot_root` into a tree of the private workdir, a read-only view of the registry, its scratch, the worker socket, the proxy socket, and the worker's read-only system paths.
- The system paths are whatever the host has: `/usr`, the top-level `/bin`, `/lib`, `/lib64`, `/sbin` where they are real directories and not links into `/usr`, and `/nix/store`. `cell.ro` in the policy adds more, at the same path inside. A path that would open the home, or that the cell would shadow with its own `/tmp`, `/etc`, `/run`, `/proc`, or `/dev`, is refused at startup with a line in the daemon log. Each is remounted read-only with the source mount's own `nosuid`, `nodev`, and atime flags, because a user namespace may not drop them.
- `PATH` inside is the daemon's `PATH`, resolved, keeping the entries the cell can see. A bare worker command is looked up there before exec.
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
- no constraint blocks it
- the isolator can actually place it
- `plan.not_before` has passed

A full `live` keeps the task queued. So does a root purse that is short for now: the task waits for the reset or for a live slice to come back, and smaller tasks pass it. A queue of thousands outlives the day's tokens. A task that could never fit the caps, and a child whose parent has no slice left, are denies. Any other failure is a deny. A deny for `gate` is retried like a failure, with backoff. Lua's `admit(ctx)` may return deny on top of that. A return of allow does not override a failed check above. Timeout or a down endpoint denies spawns and allows a running task to finish its current verifier. Never fail open on spawn.

If `decision.kind = "off"`, p_success is 1 for tasks with a verifier and 0.5 without. The cost check still runs. Cold, a verified task passes when its budget is at most its value, an unverified one at most half. With the defaults, `value = 400000` against a 200000-token budget, a verified task clears `min_ev = 0` with room to spare. A factory of five can work with the endpoint off. A swarm should not.

The endpoint receives the task header and goal, the parent chain, the cost samples for its worker and tag set, the active constraints, and recent board posts with their weights. It returns `p_success`, and for each constraint whether the task conflicts with it. It does not receive transcripts or registry bodies.

Human lines on the board are not commands. They are posts with weight `human_weight` (default 4), included in the state the gate sees. With the endpoint off, weight does nothing. TUI text posts as `you` with role `human` and that weight. `inlet post` posts as `operator` with role `operator` and weight 1, because operator agents share the CLI. A line that should bind is a constraint, sent with `/bind` or `inlet bind`. Constraints block conflicting admissions until they are cleared or they lapse. `inlet bind --for 2h` writes one that expires on its own, so a freeze does not need someone to come back and lift it. `#tags` in the text scope a constraint. With the endpoint on, it judges conflict. With it off, a constraint blocks every admission carrying one of its tags, and a constraint with no tag is refused, because nothing could judge it. Opinions weigh. Constraints bind. The human is not a superuser over the hard caps.

## Registry, tools, self-mod

```
registry/recipes/<name>/        # visible, read-only to workers
    run                         # executable
    meta.json                   # tags, cost, last_verified, verifier
drafts/<worker-id>/<name>/      # invisible to other workers
policy.lua                      # the signed snapshot the supervisor loaded
policy.draft.lua                # unsigned edits, operator door only
```

## Prompt

One contract for every harness. The supervisor writes the prompt, and the worker command says how it wants to receive it. Nothing in it names a tool, a language, or a model.

The preamble is read-only, the same for every worker, and part of the signed snapshot. Kept short on purpose:

```
You are worker {id} in inlet, attempt {attempt} of {max_attempts}. Budget: {tokens} tokens, {seconds}s, {memory_mb}MB. Depth {depth}/{max_depth}.

Nobody is watching this run and nobody will answer a question. Decide, act, and finish. Work in the current directory. Exit 0 when you are done. The supervisor judges the result. You do not grade yourself.

`$INLET_BIN door` is the way out of the cell:
  post <text>                                 speak on the board. Your last post is what your parent reads.
  board                                       read the channels you are on
  spawn -w <worker> -g <goal> --verify <cmd>  hand a part to a child. It spends your budget.
  wait                                        block until your children settle and print how each ended
  draft <name> --run <file> --verify <cmd>    offer a tool you built to the registry
  vote <target> <choice>                      weigh in when the board asks

$INLET_REGISTRY holds tools another worker built and a second one verified. Look there before you build.

You have no API keys. Model calls go through the proxy and stop when the budget is spent. If the proxy or the door refuses a connection, the supervisor is restarting: wait and try again. You cannot kill, rebudget, or raise caps.
```

`preamble` in the policy replaces it for every worker, and `preamble` on a worker replaces it for that one. Fields: `{id}`, `{worker}`, `{tags}`, `{tokens}`, `{seconds}`, `{memory_mb}`, `{depth}`, `{max_depth}`, `{attempt}`, `{max_attempts}`, `{verifier}`, `{goal}`.

The prompt is the preamble and then the sections that apply:

- `## Task`, the goal as written. Always. A goal is data. Braces in it are not fields.
- `## Done means`, the verifier command and the fact that its exit 0 is the only thing that counts. With no verifier it says nothing checks the task.
- `## Last attempt`, on a retry: how the last one ended, the last 2KB of the verifier's output, and the last 1.5KB of the worker's own, secrets stripped.

It reaches the worker every way a harness might want, so the task record never has to know which harness it is:

| where | |
|---|---|
| `{prompt}`, `{goal}`, `{id}` in `cmd` | filled into the arguments. `cmd = { "agent", "-p", "{prompt}" }`. Each argument is scanned once |
| `stdin = "prompt"` on the worker | the prompt on stdin instead of `/dev/null` |
| `$INLET_PROMPT_FILE` | the prompt |
| `$INLET_GOAL_FILE` | the goal alone |
| `$INLET_TASK_FILE` | the header as JSON: id, goal, verifier, budget, depth, attempt, parent, swarm, and the last attempt |
| `/etc/preamble` | the preamble alone |

`env` on a worker adds variables, with `env:NAME` read from the daemon's environment. `stall` on a worker stops one that has printed nothing and made no model call for that long, reason `stall`. A worker waiting on a child is not stalled.

The memory rlimit is `RLIMIT_DATA`, writable private memory, not address space. Node, the JVM, and Go reserve gigabytes they never touch, and a cap on address space kills them before they start.

A harness that wants a base URL and not a unix socket runs under `$INLET_BIN door run -- CMD`. That puts the proxy on a loopback port for the life of the command and sets `OPENAI_BASE_URL`. `--port N` fixes the port, for a harness whose config file cannot read the environment; give that worker `net = "none"` so each has the port to itself. A cell with no network still has its own loopback.

When a task settles its cell root and scratch are removed. The workdir and the transcripts are the record.

`$INLET_BIN` is this binary, bound into the cell. `inlet door` reads `INLET_SOCK` and `INLET_TOKEN`. A result travels up as the child's last post, which `door wait` returns to the parent. A parent never reads a child's workdir.

Promotion. A recipe becomes shared only when a second worker, not the author, runs its verifier and exits 0, or when the operator pins it. The supervisor execs the verifier. The author's claim does not count. Before a pass counts, the supervisor runs the same verifier with `run` swapped for a stub that exits 0. A verifier that passes the stub tests nothing, and that recipe can only be pinned. With `unattended = true`, the default, the pass promotes. With `unattended = false`, it marks the recipe ready for `/pin`. Promoted recipes are picked up at the next task start, not pushed into a running turn. A task with no verifier never promotes.

The signed policy is the boundary, and the only place a person is required. On an install with a key, these need the signature: cap increases, edits to `admit`, edits to the preamble, a new worker command, clearing a constraint. Everything inside the boundary runs without one: admission, retries, swarms, scaling, promotion after the second-run verifier, and constraints that lapse. Nothing running can raise a cap or change `admit`.

An install that never made a key has no signer to wait for. There, `inlet clear` works from the operator socket alone.

A signature is something only the human has. `inlet init` makes an ed25519 key wrapped by a passphrase and pins its public half. With no passphrase it leaves the policy unsigned. `inlet settings` and `inlet --settings` show the cell, the model, the budgets, the workers, and whether a bridge token is set. Changing a signed policy still requires the passphrase. Bridge tokens live in `keys/` and are not part of the signed file. `inlet sign` and `inlet clear` read the passphrase from `/dev/tty`, never from argv or the socket, and send the daemon a signature. The daemon loads only a `policy.lua` that verifies against the pinned key. Operator agents share the socket. They do not share the passphrase.

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

A channel group is the tag, or the worker kind. A worker post is a thread named for the task. Humans and the operator stay on the channel. Counts live on the projection, so the scrollback ring can stay short. A room with 24 or more posts shows one digest line: posts, authors, mentions, and the last line. `rollup = "count"` is the default and calls no model. `rollup = "model"` summarizes that line through the decision endpoint only when `inlet digest` asks and the decision purse has room. The charge is a `gate` record. An empty purse keeps the count line. The TUI sidebar lists groups, unread, and mentions. Ctrl-n and Ctrl-p move. Ctrl-f shows muted and demoted threads. They stay folded otherwise.

Bridges let a real group chat act as another client of the operator socket. Telegram and Discord are in this binary. A bridged human posts with role `human` and `human_weight`. The bridge is not a second board. Discord uses the gateway websocket for inbound text and REST for outbound. An inlet channel group is a category, a channel is a channel, and a task thread is a thread. When the room is busy, or REST returns 429, the bridge sends one digest instead of the raw posts. The bot token is `DISCORD_BOT_TOKEN` or `keys/discord.token` mode `0600`. It is not written to the log.

Lines starting with `/` are operator commands and do not appear as chat:

```
/add <goal> [flags]     enqueue, same flags as inlet add
/apply <file>           one swarm manifest
/scale <swarm> <n>
/delete <swarm> [--kill]
/bind <text>            constraint, blocks conflicting admits
/clear <id>             drop a constraint, asks for the passphrase
/kill <id>
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
inlet init [--defaults] [--no-smoke]
inlet settings
inlet --settings
inlet add -w <worker> -g <goal> [--verify <cmd> | --no-verify] [--tokens N] [--seconds N]
          [--memory-mb N] [--pids N] [--value N] [-t <tag>]... [--parent <id>] [--seed <dir>]
          [--retries N]
inlet add -f tasks.jsonl
inlet apply -f FILE
inlet get [tasks|swarms] [--json]
inlet describe <id>
inlet logs <id> [--verify]
inlet scale <swarm> <replicas>
inlet delete <swarm> [--kill]
inlet door <op>
inlet post <text>
inlet vote <target> <choice> [--channel NAME] [--human]
inlet digest [channel]
inlet bridge telegram
inlet bridge discord
inlet bind [--for 2h] <text>
inlet clear <id>
inlet watch [--debug N] [--worker id]
inlet status
inlet kill <id>
inlet pin <name>
inlet sign
inlet diff
inlet snap
```

`apply -f` takes one JSON value, an array, or one per line, and `-` for stdin. `"kind": "swarm"` is a swarm. Anything else is a task with the fields of `add`. A manifest of the wrong shape is refused before any of the file takes effect.

```json
{"kind": "swarm", "name": "sweep", "replicas": 4, "completions": 200,
 "template": {"worker": "prover", "goal": "attempt {index}", "verify": "./check", "tokens": 20000, "retries": 1}}
```

`get swarms` asks for a status with no task list, so a poller pays the same on the ten-thousandth task as on the first. `get` is the table, `describe` is one task with its attempts and the tail of what it and its verifier printed, and `logs` is the whole transcript, read from the home with the daemon up or down.

`add -f` takes one task per line with the same fields and appends the batch under one fsync. Budget fields left out come from `default_budget`. `/follow` is `watch --worker`, `/debug` is `--debug`, `/quit` is Ctrl-C.

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

Levels 0 and 1 are ledger records and always kept. Levels 2 to 4 exist only on the socket. Transcripts are separate: the worker's stdout and stderr, written by the supervisor to `runs/<id>.log`, capped at 16MB, whatever the debug level. The verifier's output goes to `runs/<id>.verify.log`, capped at 256KB. The next attempt reads the end of both.

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
unattended   = true
adopt        = true       -- workers outlive a dead daemon and are taken back
debug        = 1

default_budget = { tokens = 200000, seconds = 3600, memory_mb = 1024, pids = 8 }

retry          = { max = 2, backoff = "10s", max_backoff = "10m" }
verify_seconds = 300
cell           = { ro = { "/opt/tools" } }   -- extra read-only paths in every cell

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
  agent  = { cmd = { "agent", "-p", "{prompt}" }, stall = "15m", env = { MODE = "batch" } },
  piped  = { cmd = { "agent" }, stdin = "prompt" },
}

-- optional. deny only; allow does not beat a hard cap.
function admit(ctx)
  if ctx.tags.math and ctx.depth > 1 then return "deny" end
  return "allow"
end
```

`ctx` mirrors the task record, with `tags` as a set, plus `depth`, `live`, and `queued`.

`laptop` defaults: max_live 4, rlimit if cgroup writes fail. `box`: the table above. `cluster`: isolator slurm, max_live per node from the allocation, queue the rest on the lease holder. The preset fills what the file leaves out. Explicit keys win. Switching setup does not change the task record.

Only `cmd` is required on a worker. A `code` tag, or no `math` tag, defaults to `net = "host"` and no `on_crash`, so a crash follows `retry`. A `math` tag without `code` defaults to `net = "none"` and `on_crash = "requeue"`. `stdin`, `env`, `stall`, and `preamble` are in Prompt.

`verify_seconds` is how long a verifier may run. A real test suite needs minutes.

New worker kinds are new entries in `workers`. That edit needs a signature, because a worker command is code the supervisor will exec. New recipes do not need a signature. They need the promote rule. That split is the whole extension model. There is no Rust plugin trait.

## Two doors

Both doors are first-class. They share the operator socket, mode 0600, and nothing else.

Group chat. The TUI, and later a Telegram bridge (others after that). Humans and agents are in the room. Humans are role `human`. Agents post as themselves. The human can type. The bridge posts with the human tag and the human weight. This is also how agents talk to each other and how they vote. See Board consensus.

Operator agent. A normal agent, Pi or Wizard, with the operator skill. It runs the inlet CLI, for a human who talks to it or on its own with a goal and nobody behind it. It does not enter a cell. If inlet itself runs in a container, the operator socket is the mounted hole, and the operator agent is the only thing on the outside that needs it. `inlet sign` exists for a human at a keyboard and is not in the skill. Neither is `inlet clear`, which needs the same passphrase on a signed install.

Skill text, same body for both harnesses:

```
You are on the inlet operator socket, outside the workers. You run this swarm and nobody approves your steps. Say what should be true with `inlet apply` and let the supervisor keep it true. Do not feed tasks one at a time or rerun a failure by hand: retries and swarms do that. Use the inlet CLI for apply, add, get, describe, logs, scale, delete, status, post, bind, kill, pin, diff, and watch. Give every task a verifier. The verifier is what lets it run with nobody watching. Use `--no-verify` only for work nothing can check. Read the swarm with `inlet get`, `inlet describe <id>`, and `inlet watch --debug N`, not by attaching to workers. Workers talk to each other on the board. You do not carry a message between them. Caps, admit, the preamble, and worker commands are the boundary. You cannot sign. If the boundary is wrong, edit policy.draft.lua, say so on the board, and keep working inside the one you have. Do not enter a cell. The socket is the door.
```

Inmate door. A worker token can: post, submit a recipe draft, request a child with a verifier, read how its children ended and what each last said, read the registry, write its scratch, report a tool event, read the channels it is subscribed to, and cast a board vote. It cannot: kill, rebudget, sign, pin, read sibling scratch, open the operator socket, read keys, change policy, or open a private channel to a sibling. The cell enforces the file half of that list. The token enforces the rest. A stolen token spends that worker's remaining slice and nothing else.

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
- Embeddings, a vector store, semantic search over the board. Tags are exact match.
- A step that waits on a person. Approval queues, review gates, an ask that blocks until someone answers. A person sets the boundary before the swarm runs and may talk on the board while it does.
- A dependency graph in the manifest. A task that needs another's result spawns it as a child and waits.
- A dashboard, a web UI, a pane per agent.
- Linking Pi, Wizard, or a model runtime into the binary.
- An unfenced worker.
- Mid-turn push of a new recipe into a running worker.
- Direct messages between workers. Agent-to-agent talk is a board post.
- Multi-node until one node has run a real factory and a real queued sweep without leaking budget.

## Environment

- Rust 1.95 is installed.
- This box is NixOS. `/usr/bin` holds `env` and nothing else, and every binary is under `/nix/store`. The cell binds what the host has.
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
13. Chat bridges. Telegram and Discord. A bridge is a client of the operator socket. Bridged humans post with role `human` and `human_weight`. Discord maps groups, channels, and threads onto categories, channels, and threads.
14. Prompt contract. The goal reaches the worker by argument, stdin, and file. The prompt says how the task is judged. `inlet door` is the worker's client, bound into the cell.
15. Retry. A failed task becomes a new attempt in the same fsync as its exit, with backoff, the old workdir, and the last failure in the prompt. A short root purse holds the queue. A daemon stop reruns what it interrupted.
16. Swarms. `apply`, `scale`, `delete`, `get`, `describe`, `logs`. The reconcile loop stamps tasks to the replica count, backs off on a failure streak, and stalls at the limit. Counts survive `kill -9`.
17. Adoption. A keeper in each cell, the wait status on disk, sockets mounted by directory. A daemon killed with `kill -9` comes back to the same workers.
