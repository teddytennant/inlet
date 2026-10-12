#!/usr/bin/env python3
"""A whole worker in one file. It reads the prompt inlet wrote, asks the model
what to run, runs it, and shows the model the output, until the model says DONE.

It knows nothing about the task. Everything it is told comes from
$INLET_PROMPT_FILE, and every model call goes through $INLET_PROXY_SOCK with
the task's own token, so the supervisor meters it.

    workers = { agent = { cmd = { "python3", "/path/to/examples/agent.py" } } }
"""
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import time

RULES = (
    "You work by running shell commands. Reply with exactly one fenced sh block "
    "and nothing else. I run it in the working directory and reply with what it "
    "printed. When the task is finished, reply with the single word DONE and no block."
)
TURNS = int(os.environ.get("AGENT_TURNS", "12"))


class Proxy(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("inlet")
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.path)


def ask(messages):
    body = json.dumps({"model": os.environ.get("AGENT_MODEL", "default"), "messages": messages})
    headers = {"Authorization": "Bearer " + os.environ["INLET_TOKEN"], "Content-Type": "application/json"}
    waited = 0
    while True:
        try:
            conn = Proxy(os.environ["INLET_PROXY_SOCK"])
            conn.request("POST", "/v1/chat/completions", body, headers)
            reply = conn.getresponse()
            data = json.loads(reply.read())
            break
        except (OSError, http.client.HTTPException, ValueError) as e:
            # A dead socket is the supervisor restarting. It takes its workers back.
            if waited >= 90:
                sys.exit("proxy gone: %s" % e)
            print("proxy unreachable, waiting", flush=True)
            time.sleep(2)
            waited += 2
    if reply.status != 200:
        # empty_purse lands here. The contract is to stop.
        sys.exit("proxy %d: %s" % (reply.status, json.dumps(data.get("error", data))))
    return data["choices"][0]["message"]["content"]


def main():
    with open(os.environ["INLET_PROMPT_FILE"]) as f:
        prompt = f.read()
    messages = [{"role": "system", "content": RULES}, {"role": "user", "content": prompt}]
    for turn in range(TURNS):
        text = ask(messages)
        messages.append({"role": "assistant", "content": text})
        block = re.search(r"```(?:sh|bash|shell)?\n(.*?)```", text, re.S)
        if block is None:
            print("turn %d: %s" % (turn + 1, text.strip()[:200]))
            if "DONE" in text:
                return 0
            messages.append({"role": "user", "content": "Reply with one sh block, or DONE."})
            continue
        script = block.group(1)
        print("turn %d $ %s" % (turn + 1, script.strip()[:400]), flush=True)
        try:
            run = subprocess.run(
                ["sh", "-c", script], capture_output=True, text=True, timeout=120
            )
            out = (run.stdout + run.stderr)[-2000:]
            note = "exit %d\n%s" % (run.returncode, out)
        except subprocess.TimeoutExpired:
            note = "timed out after 120s"
        print(note.strip()[:600], flush=True)
        messages.append({"role": "user", "content": note})
    print("out of turns")
    return 1


if __name__ == "__main__":
    sys.exit(main())
