#!/usr/bin/env python3
"""An OpenAI-shaped endpoint that answers with the `claude` CLI, for a box
that has a Claude login and no API key. Point `proxy.upstream` at it:

    python3 examples/claude-upstream.py 8791 &
    proxy = { upstream = "http://127.0.0.1:8791" }

Each call is one `claude -p` with no tools of its own. It runs outside every
cell, so workers never see the login. Usage comes back from the CLI and the
proxy meters it against the task.

A harness that sends `tools` gets tool calls back: the tool list goes into the
system prompt and a reply that is a JSON tool call is turned into one. That is
enough to drive a real coding harness. `stream: true` is answered as one
server-sent event stream.
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL = os.environ.get("CLAUDE_MODEL", "haiku")
# An empty directory, so the CLI picks up no project instructions.
CWD = tempfile.mkdtemp(prefix="claude-upstream-")

TOOL_RULES = """

# Tools
You can call these tools. Each is a name, a description, and a JSON schema for its arguments:

%s

You have no built-in way to call them. The only way is to write the call out as text: reply with one JSON object and nothing else, no prose and no code fence:
{"tool_calls": [{"name": "<tool name>", "arguments": {<arguments>}}]}
The harness reads that text, runs the tools, and continues the conversation.
The tools run on the harness's machine, in its working directory, not here. Use relative paths unless the conversation itself names a directory. Any directory you know of from outside this conversation does not exist there.
You will get each result back in a message that starts with [tool result]. When no tool is needed, reply in plain text."""


def text_of(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(part.get("text", "") for part in content if isinstance(part, dict))
    return "" if content is None else str(content)


def render(messages, tools):
    system = "\n\n".join(text_of(m.get("content")) for m in messages if m["role"] in ("system", "developer"))
    if tools:
        listed = [t.get("function", t) for t in tools]
        system += TOOL_RULES % json.dumps(
            [{"name": t["name"], "description": t.get("description", ""), "parameters": t.get("parameters", {})} for t in listed],
            indent=1,
        )
    lines = []
    for m in messages:
        role = m["role"]
        if role in ("system", "developer"):
            continue
        if role == "tool":
            lines.append("[tool result]\n" + text_of(m.get("content")))
        elif role == "assistant" and m.get("tool_calls"):
            calls = [
                {"name": c["function"]["name"], "arguments": json.loads(c["function"].get("arguments") or "{}")}
                for c in m["tool_calls"]
            ]
            lines.append("[assistant]\n" + json.dumps({"tool_calls": calls}))
        else:
            lines.append("[%s]\n%s" % (role, text_of(m.get("content"))))
    return system, "\n\n".join(lines) + "\n\n[assistant]\n"


def tool_calls_in(text):
    """The reply as tool calls, or None when it is prose."""
    body = text.strip()
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", body, re.S)
    if fenced:
        body = fenced.group(1)
    start = body.find('{"tool_calls"')
    if start < 0:
        return None
    try:
        parsed, _ = json.JSONDecoder().raw_decode(body[start:])
    except ValueError:
        return None
    calls = parsed.get("tool_calls")
    if not isinstance(calls, list) or not calls:
        return None
    return [
        {
            "id": "call_" + uuid.uuid4().hex[:12],
            "type": "function",
            "function": {"name": c.get("name", ""), "arguments": json.dumps(c.get("arguments", {}))},
        }
        for c in calls
        if isinstance(c, dict)
    ]


def complete(body):
    system, text = render(body.get("messages", []), body.get("tools"))
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE")}
    cmd = ["claude", "-p", "--model", MODEL, "--output-format", "json", "--tools", "",
           "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}']
    if system:
        cmd += ["--system-prompt", system]
    run = subprocess.run(cmd, input=text, capture_output=True, text=True, cwd=CWD, env=env, timeout=300)
    out = json.loads(run.stdout)
    if out.get("is_error"):
        raise RuntimeError(out.get("result", "claude failed"))
    usage = out.get("usage", {})
    prompt = sum(usage.get(k, 0) for k in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens"))
    done = usage.get("output_tokens", 0)
    reply = out["result"]
    calls = tool_calls_in(reply) if body.get("tools") else None
    message = {"role": "assistant", "content": None if calls else reply}
    if calls:
        message["tool_calls"] = calls
    return message, {"prompt_tokens": prompt, "completion_tokens": done, "total_tokens": prompt + done}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        try:
            message, usage = complete(body)
        except Exception as e:  # the worker gets the reason, not a hung socket
            return self.send(502, "application/json", json.dumps({"error": {"type": "upstream", "message": str(e)[:300]}}))
        finish = "tool_calls" if message.get("tool_calls") else "stop"
        base = {"id": "chatcmpl-" + uuid.uuid4().hex[:12], "model": MODEL}
        if not body.get("stream"):
            reply = dict(base, object="chat.completion", usage=usage,
                         choices=[{"index": 0, "finish_reason": finish, "message": message}])
            return self.send(200, "application/json", json.dumps(reply))
        delta = {"role": "assistant"}
        if message.get("tool_calls"):
            delta["tool_calls"] = [dict(call, index=i) for i, call in enumerate(message["tool_calls"])]
        else:
            delta["content"] = message["content"]
        chunk = dict(base, object="chat.completion.chunk")
        events = [
            dict(chunk, choices=[{"index": 0, "delta": delta, "finish_reason": None}]),
            dict(chunk, choices=[{"index": 0, "delta": {}, "finish_reason": finish}]),
            dict(chunk, choices=[], usage=usage),
        ]
        stream = "".join("data: %s\n\n" % json.dumps(e) for e in events) + "data: [DONE]\n\n"
        self.send(200, "text/event-stream", stream)

    def do_GET(self):
        # Harnesses ask what models there are before they send anything.
        if self.path.rstrip("/").endswith("/models"):
            listing = {"object": "list", "data": [{"id": MODEL, "object": "model", "owned_by": "claude"}]}
            return self.send(200, "application/json", json.dumps(listing))
        self.send(404, "application/json", '{"error":{"type":"not_found","message":"unsupported"}}')

    def send(self, status, kind, text):
        raw = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s\n" % (fmt % args))


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8791
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
