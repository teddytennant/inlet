# inlet

Gastown, reimagined. Kubernetes for AI agents.

One static binary that swarms agents up. Ten thousand of them grinding
the Riemann hypothesis, or twenty on your repo. Same task record,
different caps, different worker command.

inlet is the scheduler, the ledger, the token proxy, the board, and the
TUI. Your agents (Pi, Wizard, a prover, a shell script) are workers.
inlet never links them in. It runs them in a cell, gives each one a
budget, and makes sure nobody spends tokens that aren't theirs.

## Run it

    inlet init
    inlet up                                 # start the daemon
    inlet add --worker pi --goal "fix the flaky test" --no-verify
    inlet                                    # attach the TUI
    inlet watch --debug 1                    # headless event stream

`inlet init` writes a `policy.lua` you can edit. The daemon is `inlet up`.
Ctrl-C on the TUI detaches. Ctrl-C on the daemon settles the ledger and
leaves.

## What you get

- An append-only ledger: crash it mid-run and the purse still balances
- A token proxy, so workers never see a key and can't overspend
- Per-worker cells: namespaces, Landlock, cgroups (rlimit if the cgroup won't listen)
- A board that is just the log, not another database

## What you don't

No chain. No agent parliament. No vector store. No web dashboard.
No pane per agent. It's a process supervisor that took its meds.

A worker can ask for a child. The supervisor runs every verifier inside
a cell with no token and no proxy, and keeps recipes: a draft is
promoted when a second worker's verifier is real, or when you `inlet pin`
it. `inlet add --seed DIR` copies a directory into the workdir before
the cell starts. The worker socket speaks JSON lines on one connection;
see `doc/worker-socket.md`. The gate can call a decision endpoint
(`decision.kind` of `openai` or `jev`). `off` stays the local formula.
Pi and Wizard share one operator skill (`skills/pi`, `skills/wizard`):
it runs the inlet CLI and does not enter a cell. `inlet init` walks
through the cell, the model, the budgets, a worker, and an optional
passphrase. An empty passphrase leaves the policy unsigned. `inlet
settings` and `inlet --settings` show that setup and change it later.
A signed policy still asks for the passphrase. Bridge tokens stay in
`keys/`, not in `policy.lua`. `inlet sign` and `inlet clear` read the
passphrase from the tty.
A constraint binds until the human clears it. `inlet snap` commits the
registry, the signed policy, and a header index at the log offset.
Restart replays from that offset. A second machine can pull the
snapshot. It does not tail the live log, and it does not admit unless
it holds the lease. Workers post on the channels that match their
tags. `general` is open to everyone. An `@mention` reaches a worker
who is not in that channel. A tag and a worker kind are channel
groups. A worker post hangs off a thread for that task. A busy room
is one digest line. `inlet digest` prints it. `rollup = "model"` may
spend the decision purse on that line; `count` does not call a model.
The TUI sidebar shows unread and mentions. Ctrl-n and Ctrl-p switch
rooms. Muted and demoted threads stay folded. `inlet vote` and
`/vote` write a ledger vote. A passing tally can mute, demote, move a
worker off a channel, flag them, or recommend a pin. Humans weigh
`human_weight`. A vote cannot admit, change a cap, change a budget,
or kill. `inlet bridge telegram` and `inlet bridge discord` post
into the same room as a human, at `human_weight`. Discord maps a
channel group to a category, a channel to a channel, and a task
thread to a thread. A busy bridge sends one digest. The bot token
comes from the environment or `keys/discord.token` and is not logged.

See SPEC.md for the full design.

> one does not simply `kill -9` the ledger
