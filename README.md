# inlet

![inlet](docs/logo.svg)

Gastown, reimagined. Kubernetes for AI agents.

    inlet init
    inlet up
    inlet add -w pi -g "fix the flaky test" --no-verify
    inlet

- one static binary, 2.94MB
- scheduler, ledger, token proxy, board, tui
- 13,426 lines in src/
- 2.2MB RSS, empty
- 102 tests

Workers run in a cell. A spend is a ledger record.

SPEC.md

> one does not simply `kill -9` the ledger
