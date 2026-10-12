# harnesses

A worker is a command, so a harness is a worker once it can find its model.
Each of these ran in a cell with `net = "none"`, its model calls going out
through `inlet door run` to the metered proxy. The seed directory holds the
harness's own config file, pointed at `http://127.0.0.1:18400`.

    workers = {
      pi     = { cmd = { "/bin/sh", "-c", "exec \"$INLET_BIN\" door run --port 18400 -- node /path/pi-coding-agent/dist/bundle/cli.js -p --no-session --provider inlet --model haiku \"$1\"", "sh", "{prompt}" }, net = "none" },
      wizard = { cmd = { "/bin/sh", "-c", "exec \"$INLET_BIN\" door run --port 18400 -- /path/wizard --mode sovereign --no-completion-review -p \"$1\"", "sh", "{prompt}" }, net = "none" },
      crush  = { cmd = { "/bin/sh", "-c", "exec \"$INLET_BIN\" door run --port 18400 -- /path/crush run -q \"$1\"", "sh", "{prompt}" }, net = "none" },
      hermes = { cmd = { "/bin/sh", "-c", "export HERMES_HOME=$HOME/.hermes; exec \"$INLET_BIN\" door run --port 18400 -- /path/hermes --yolo chat -Q -q \"$1\"", "sh", "{prompt}" }, net = "none" },
      claude = { cmd = { "/bin/sh", "-c", "export ANTHROPIC_AUTH_TOKEN=$INLET_TOKEN ANTHROPIC_MODEL=claude-sonnet-5-5 IS_SANDBOX=1; exec \"$INLET_BIN\" door run --port 18400 -- /path/claude -p --dangerously-skip-permissions \"$1\" < /dev/null", "sh", "{prompt}" }, net = "none" },
    }

| harness | config in the seed | speaks |
|---|---|---|
| Pi | `.pi/agent/models.json`, a provider with `baseUrl` and `"apiKey": "$INLET_TOKEN"` | chat completions |
| Wizard | `.wizard/config.toml`, a `[[providers]]` of `kind = "openai"` with `api_key_env = "INLET_TOKEN"` | chat completions |
| Crush | `.config/crush/crush.json`, a provider of `type = "openai-compat"` | chat completions |
| Hermes | `.hermes/config.yaml`, `model.provider: custom` with `base_url` | chat completions |
| Claude Code | none, `ANTHROPIC_BASE_URL` is set by `door run` | messages |

Each binary has to be listed in `cell = { ro = { ... } }`. Codex speaks only
the responses API; the proxy forwards it, but the bridge below does not
answer it, so it was not run.

## one measurement

Four tasks, each twice, each judged by its verifier: write an N-th prime
script, a base-7 converter, a word reverser, and fix a failing Python test
without touching the test. Retries allowed, two after the first attempt. The
model behind all of them was Claude through `examples/claude-upstream.py`,
which answers tool calls by asking the model to write them out, so this
measures harness and bridge together and not a provider's native tool calling.
Haiku for every row but the one marked.

| harness | passed first try | passed | median seconds | median tokens | model calls |
|---|---|---|---|---|---|
| `examples/agent.py` | 8/8 | 8/8 | 11 | 3,570 | 2.2 |
| Wizard | 8/8 | 8/8 | 23 | 60,964 | 3.8 |
| Hermes | 8/8 | 8/8 | 41 | 117,771 | 4.6 |
| Claude Code, Sonnet 5.5 | 7/8 | 8/8 | 24 | 16,383 | 3.0 |
| Pi | 7/8 | 8/8 | 26 | 17,743 | 4.4 |
| Crush | 6/8 | 8/8 | 28 | 84,105 | 5.6 |
| Claude Code, Haiku | 0/8 | 0/8 | | | |

Eight tasks a row is a small sample. Read it as "all of these work as
workers", and the token column as how much each harness says before it acts.
Claude Code on Haiku wrote its answers as prose and never called a tool
through the bridge; that row is the bridge's limit with that model, not the
harness's.
