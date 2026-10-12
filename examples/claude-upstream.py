#!/usr/bin/env python3
"""An OpenAI-shaped endpoint that answers with the `claude` CLI, for a box
that has a Claude login and no API key. Point `proxy.upstream` at it:

    python3 examples/claude-upstream.py 8791 &
    proxy = { upstream = "http://127.0.0.1:8791" }

Each call is one `claude -p` with no tools. It runs outside every cell, so
workers never see the login. Usage comes back from the CLI and the proxy
meters it against the task.
"""
import json
import os
import subprocess
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL = os.environ.get("CLAUDE_MODEL", "haiku")
# An empty directory, so the CLI picks up no project instructions.
CWD = tempfile.mkdtemp(prefix="claude-upstream-")


def complete(messages):
    system = "\n\n".join(m["content"] for m in messages if m["role"] == "system")
    turns = [m for m in messages if m["role"] != "system"]
    text = "\n\n".join("[%s]\n%s" % (m["role"], m["content"]) for m in turns)
    text += "\n\n[assistant]\n"
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE")}
    cmd = ["claude", "-p", "--model", MODEL, "--output-format", "json", "--tools", ""]
    if system:
        cmd += ["--system-prompt", system]
    run = subprocess.run(cmd, input=text, capture_output=True, text=True, cwd=CWD, env=env, timeout=300)
    out = json.loads(run.stdout)
    if out.get("is_error"):
        raise RuntimeError(out.get("result", "claude failed"))
    usage = out.get("usage", {})
    prompt = sum(usage.get(k, 0) for k in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"))
    done = usage.get("output_tokens", 0)
    return out["result"], prompt, done


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        try:
            text, prompt, done = complete(body.get("messages", []))
            status, reply = 200, {
                "object": "chat.completion",
                "model": MODEL,
                "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": text}}],
                "usage": {"prompt_tokens": prompt, "completion_tokens": done, "total_tokens": prompt + done},
            }
        except Exception as e:  # the worker gets the reason, not a hung socket
            status, reply = 502, {"error": {"type": "upstream", "message": str(e)[:300]}}
        raw = json.dumps(reply).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s\n" % (fmt % args))


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8791
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
