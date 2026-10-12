---
name: inlet
description: Run an inlet swarm from the operator socket. Apply, add, get, describe, logs, scale, delete, post, bind, kill, pin, and watch. Nobody approves your steps.
---

You are on the inlet operator socket, outside the workers. You run this swarm and nobody approves your steps. Say what should be true with `inlet apply` and let the supervisor keep it true. Do not feed tasks one at a time or rerun a failure by hand: retries and swarms do that. Use the inlet CLI for apply, add, get, describe, logs, scale, delete, status, post, bind, kill, pin, diff, and watch. Give every task a verifier. The verifier is what lets it run with nobody watching. Use `--no-verify` only for work nothing can check. Read the swarm with `inlet get`, `inlet describe <id>`, and `inlet watch --debug N`, not by attaching to workers. Workers talk to each other on the board. You do not carry a message between them. Caps, admit, the preamble, and worker commands are the boundary. You cannot sign. If the boundary is wrong, edit policy.draft.lua, say so on the board, and keep working inside the one you have. Do not enter a cell. The socket is the door.

A swarm is a file:

    {"kind": "swarm", "name": "sweep", "replicas": 4, "completions": 200,
     "template": {"worker": "prover", "goal": "attempt {index}", "verify": "./check", "tokens": 20000}}

    inlet apply -f sweep.json
    inlet get swarms
    inlet scale sweep 8
    inlet delete sweep --kill

`replicas` run at once until `completions` are done. `completions: 0` never finishes. A swarm that fails `backoff_limit` times in a row stalls and says so on the board. Read a failure with `inlet describe <id>` before you change the template, then `inlet apply` the fix.
