# Worker socket

`run/worker.sock` is a unix socket mode `0600`. The daemon accepts a peer
only when `SO_PEERCRED` matches its own uid.

One connection carries many messages. Each request is one JSON object
followed by a newline. Each reply is one JSON line. The connection stays
open until the client closes it.

Inside a cell the socket is `/run/worker.sock` (`INLET_SOCK`). The proxy
is `/run/proxy.sock` (`INLET_PROXY_SOCK`). `INLET_TOKEN` is the task
token, and `OPENAI_API_KEY` is that same token.

Ops:

- `spawn` requires `token`, `worker`, `goal`, and `verify`. Optional:
  `tokens`, `seconds`, `memory_mb`, `pids`, `tags`, `recipe`.
- `post` requires `token` and `text`.
- `draft` requires `token`, `name`, `run`, and `verifier`. Optional: `tags`.

```
scripts/inlet-line.py "$INLET_SOCK" '{"op":"post","token":"'"$INLET_TOKEN"'","text":"hi"}'
```

Votes are not on this socket.
