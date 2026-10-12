# usage

Two ways in. Both sit on the operator socket.

## group chat

Humans and agents share channels.

    inlet init
    inlet up
    inlet

Type to post. `@all` and `@id` mention. Ctrl-n and Ctrl-p move. Ctrl-f folds. Esc leaves follow.

    /add -w sleeper -g hello --no-verify
    /bind stay out #code
    /vote <id> mute
    /quit

Bridges are the same room from another client. The daemon is already up.

    TELEGRAM_BOT_TOKEN=... inlet bridge telegram
    DISCORD_GUILD=... DISCORD_BOT_TOKEN=... inlet bridge discord

Discord also reads `keys/discord.token` mode 0600. Telegram reads `TELEGRAM_BOT_TOKEN`.

## a swarm

Say what should be running. The supervisor keeps it running.

    cat > sweep.json <<'EOF'
    {"kind": "swarm", "name": "sweep", "replicas": 4, "completions": 200,
     "template": {"worker": "prover", "goal": "attempt {index}", "verify": "./check", "tokens": 20000}}
    EOF
    inlet apply -f sweep.json
    inlet get swarms
    inlet get tasks
    inlet describe <id>
    inlet logs <id>
    inlet scale sweep 8
    inlet delete sweep --kill

Four run at once until 200 are done. `"completions": 0` never finishes. A task that fails its verifier is tried again, in the same workdir, with the verifier's output in its prompt. `--retries N` on `add`, `"retries"` in a template, or `retry.max` in the policy says how often. Six failures in a row stall a swarm, and `inlet apply` or `inlet scale` restarts it.

## a worker

A worker is a command. The prompt reaches it however it reads one.

    workers = {
      agent = { cmd = { "agent", "-p", "{prompt}" } },
      piped = { cmd = { "agent" }, stdin = "prompt" },
      other = { cmd = { "sh", "-c", "agent < $INLET_PROMPT_FILE" }, stall = "15m" },
    }

The prompt is the preamble, the goal, the verifier that will judge it, and on a retry what the last attempt got wrong. Inside the cell a worker hands work off with `$INLET_BIN door spawn` and collects it with `$INLET_BIN door wait`. See [doc/worker-socket.md](../doc/worker-socket.md).

`examples/agent.py` is a whole worker in 90 lines: it reads the prompt, asks the model through the proxy what to run, and runs it until the model says DONE. `examples/claude-upstream.py` is a model endpoint for a box with a Claude login and no API key.

    python3 examples/claude-upstream.py 8791 &

    proxy   = { upstream = "http://127.0.0.1:8791" }
    cell    = { ro = { "/path/to/inlet/examples" } }
    workers = { agent = { cmd = { "python3", "/path/to/inlet/examples/agent.py" } } }

Pi runs as a worker the same way, with no network at all. Its model calls go out through `door run`:

    cell    = { ro = { "/path/to/node_modules/@earendil-works" } }
    workers = {
      pi = { cmd = { "/bin/sh", "-c",
               "exec \"$INLET_BIN\" door run --port 18400 -- node /path/to/pi-coding-agent/dist/bundle/cli.js -p --no-session --provider inlet --model worker \"$1\"",
               "sh", "{prompt}" },
             net = "none" },
    }

and a seed directory holding `.pi/agent/models.json`, passed with `--seed` or `"seed"` in a template:

    {"providers": {"inlet": {"baseUrl": "http://127.0.0.1:18400/v1", "api": "openai-completions",
      "apiKey": "$INLET_TOKEN", "models": [{"id": "worker", "contextWindow": 200000, "maxTokens": 8000}]}}}

A worker's binary has to be somewhere the cell can see: `/usr`, `/bin`, `/nix/store`, or a path listed in `cell = { ro = { ... } }`.

## when the daemon dies

Nothing has to be done. `kill -9` the daemon, or stop it with SIGTERM, and its workers keep running. Ctrl-C on a foreground daemon is the one that stops them. `inlet up` takes them back: same task, same attempt. One that finished in between is settled from the status its keeper wrote down. A model call that was in flight is lost, so a harness should retry a refused proxy connection for a minute, as `examples/agent.py` does. `adopt = false` in the policy turns this off, and then a dead daemon takes its workers with it.

## more machines

A node is another machine running `inlet up` with its own policy. The leader needs a way to run its CLI, and nothing else:

    nodes = {
      east = { cmd = { "ssh", "east", "inlet", "--home", "/srv/inlet" }, slots = 8 },
    }
    workers = { pi = { on = "nodes" } }

A second leader waits its turn: `inlet up -f --standby` on the same home takes over the moment the first one's process is gone.

The worker `pi` is defined in east's policy. Tasks for it are placed on the node with the most room, run there in cells, and are retried there. `inlet get nodes` shows who is answering. A node silent for `node_grace` (a minute by default) loses its tasks to another.

## keeping the daemon up

inlet does not supervise itself. Put it under whatever supervises the box:

    [Unit]
    Description=inlet
    After=network.target

    [Service]
    ExecStart=%h/.local/bin/inlet up -f
    Restart=always
    RestartSec=1
    KillMode=process
    Delegate=yes

    [Install]
    WantedBy=default.target

`Delegate=yes` lets the daemon make one cgroup per worker, so the kernel enforces each worker's `memory_mb` and `pids` and a worker over its memory ends as `memory`, not as a crash. Without it workers get rlimits and a five-times-a-second check of resident memory.

`KillMode=process` matters: systemd then signals the daemon alone, the workers stay up, and the restarted daemon takes them back. That goes for `systemctl stop` too, so stopping the unit leaves the work running. `inlet down` is what stops the workers; `inlet down --keep` stops only the daemon. The same file is in `contrib/inlet.service`.

`inlet doctor` checks the policy, the cell, the upstream, the disk, the daemon, and whether a dead daemon left workers waiting. It exits non-zero when something is wrong.

To check a build before trusting it with work:

    scripts/chaos.py target/release/inlet --tasks 600 --seconds 240

## operator skill

A harness with a shell (Pi, Wizard, Claude Code) loads the skill and runs the swarm. Nobody approves its steps. It cannot sign.

The skill is `skills/pi/SKILL.md`. `skills/wizard/SKILL.md` is the same file.

    cp -a skills/pi ~/.pi/agent/skills/inlet
    cp -a skills/pi ~/.claude/skills/inlet
    cp -a skills/wizard ~/.agents/skills/inlet

Caps, `admit`, the preamble, and worker commands are the boundary. They change in `policy.draft.lua`, and on an install with a key `inlet sign` takes the passphrase. Everything inside the boundary runs without one.

## install

    curl -fsSL https://raw.githubusercontent.com/teddytennant/inlet/main/install.sh | sh

That puts the binary in `~/.local/bin`. `install.sh --system` uses sudo and `/usr/local/bin`. A tty runs `inlet init` after. `GITHUB_TOKEN` is optional.
