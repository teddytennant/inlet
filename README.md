# inlet

![inlet](docs/logo.svg)

Gastown, reimagined. Kubernetes for AI agents.

    curl -fsSL https://raw.githubusercontent.com/teddytennant/inlet/main/install.sh | sh
    inlet init
    inlet up
    inlet add -w pi -g "fix the flaky test" --verify "cargo test"
    inlet apply -f sweep.json
    inlet get swarms

Say what should be running. inlet keeps it running: a failed task is tried again with the verifier's output in its prompt, a swarm is topped back up to its count, and a daemon killed with `kill -9` comes back to the same running workers. Nothing waits on a person. [docs/usage.md](docs/usage.md)

    {"kind": "swarm", "name": "sweep", "replicas": 4, "completions": 200,
     "template": {"worker": "prover", "goal": "attempt {index}", "verify": "./check"}}

- one static binary, 3.16MB
- scheduler, reconciler, ledger, token proxy, board, tui
- 17,002 lines in src/
- 2.5MB RSS, empty
- 135 tests

Workers run in a cell. A spend is a ledger record. Any agent CLI is a worker: the prompt goes in by argument, stdin, or file, and `inlet door run` gives it the metered proxy as a base URL.

A 10,000-task swarm with one failure in ten and a `kill -9` halfway: all done, nothing held, 15MB RSS. Twelve Pi agents in cells with no network, daemon killed mid-run: four taken back, twelve done.

SPEC.md

> one does not simply `kill -9` the ledger
