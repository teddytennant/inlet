#!/usr/bin/env python3
"""Three machines, a replica daemon on each, and any of them may lead.

    scripts/three-leaders.py path/to/inlet HOST_B NODE_C --key FILE

A is this machine. B is reached by ssh. C is reached through B. B and C
reach each other by ssh, and reach A back through a unix socket that an
`ssh -R` from here puts on B: a relay on this side runs `inlet replica serve`
or `vote` on A's home and nothing else. The node that does the work is on B.

The leader is killed where it runs and held down, four times over, so the
lead has to move from machine to machine. Checked: every answered add is in
the ledger and done, and the three logs end with one sha256.
"""
import argparse, hashlib, json, os, re, shlex, signal, socket, subprocess, sys, tempfile, threading, time

ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("b")
ap.add_argument("c", help="C's host name as B knows it")
ap.add_argument("--key", default="")
ap.add_argument("--dir", default="/tmp/inlet-3l")
args = ap.parse_args()
binary = os.path.abspath(args.inlet)
root = tempfile.mkdtemp(prefix="inlet-3l-", dir=os.path.expanduser("~/.cache"))
D = args.dir
BASE = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "ControlPath=none"]
if args.key:
    BASE += ["-i", args.key, "-o", "IdentitiesOnly=yes"]
# C is reached from here as the same user B is.
user = [l.split()[1] for l in subprocess.run(["ssh", "-G", args.b], capture_output=True, text=True).stdout.splitlines() if l.startswith("user ")][0]
TO = {"b": BASE + [args.b], "c": BASE + ["-o", "ProxyCommand=ssh -o BatchMode=yes -o ControlPath=none -W %%h:%%p %s" % args.b, "%s@%s" % (user, args.c)]}
HOME = {"a": root + "/a", "b": D + "/b", "c": D + "/c"}
BIN = {"a": binary, "b": D + "/inlet", "c": D + "/inlet"}
os.makedirs(HOME["a"] + "/ledger")


def rsh(name, script, limit=40, stdin=None):
    cmd = ["sh", "-c", script] if name == "a" else TO[name] + [script]
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=limit, stdin=stdin)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(cmd, 124, "", "timeout")


def inlet(name, *argv, limit=40):
    return rsh(name, " ".join(shlex.quote(a) for a in [BIN[name], "--home", HOME[name], *argv]), limit)


# The way back to A. Only these two commands, only on A's home.
relay = socket.socket()
relay.bind(("127.0.0.1", 0))
relay.listen(16)
PORT = relay.getsockname()[1]


def serve_relay():
    while True:
        conn, _ = relay.accept()

        def one(conn=conn):
            line = b""
            while not line.endswith(b"\n") and len(line) < 200:
                got = conn.recv(1)
                if not got:
                    break
                line += got
            if line.strip() in (b"replica serve", b"replica vote"):
                subprocess.run([binary, "--home", HOME["a"], *line.decode().split()], stdin=conn.fileno(), stdout=conn.fileno())
            conn.close()
        threading.Thread(target=one, daemon=True).start()


threading.Thread(target=serve_relay, daemon=True).start()
CLIENT = """import os, socket, sys, threading
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[1])
s.sendall((" ".join(sys.argv[2:]) + "\\n").encode())
def up():
    while True:
        b = os.read(0, 65536)
        if not b:
            break
        s.sendall(b)
    try:
        s.shutdown(socket.SHUT_WR)
    except OSError:
        pass
threading.Thread(target=up, daemon=True).start()
while True:
    b = s.recv(65536)
    if not b:
        break
    os.write(1, b)
"""
quote = lambda argv: ", ".join('"%s"' % a.replace('"', '\\"') for a in argv)
plain = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]
VIEW = {  # how each machine reaches each replica, and the node
    "a": {"a": [binary, "--home", HOME["a"]], "b": TO["b"] + [BIN["b"], "--home", HOME["b"]], "c": TO["c"] + [BIN["c"], "--home", HOME["c"]],
          "n0": TO["b"] + [D + "/inlet", "--home", D + "/n0"]},
    "b": {"a": ["python3", D + "/client.py", D + "/a.sock"], "b": [BIN["b"], "--home", HOME["b"]], "c": plain + [args.c, BIN["c"], "--home", HOME["c"]],
          "n0": [D + "/inlet", "--home", D + "/n0"]},
    "c": {"a": plain + ["B", "python3", D + "/client.py", D + "/a.sock"], "b": plain + ["B", BIN["b"], "--home", HOME["b"]], "c": [BIN["c"], "--home", HOME["c"]],
          "n0": plain + ["B", D + "/inlet", "--home", D + "/n0"]},
}
COMMON = """
caps = { max_live = 8, max_depth = 2, max_tokens = 50000000, max_memory_mb = 4000, max_pids = 800 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 60, memory_mb = 128, pids = 32 }
"""


def policy(me, b_name):
    view = {k: [b_name if a == "B" else a for a in v] for k, v in VIEW[me].items()}
    return COMMON + "node_grace = 40\nreplicas = {\n%s}\nnodes = { n0 = { cmd = { %s }, slots = 6 } }\nworkers = { plain = { on = \"nodes\" } }\n" % (
        "".join("  %s = { cmd = { %s } },\n" % (r, quote(view[r])) for r in "abc"), quote(view["n0"]))


LOOP = "cd %s && rm -f stop hold && nohup sh -c 'while [ ! -e stop ]; do while [ -e hold ]; do sleep 1; done; ./inlet --home %s up -f --replica %s >> %s.log 2>&1; sleep 0.5; done' > /dev/null 2>&1 &"
KILL = "touch %s/hold; for p in $(pgrep -x inlet); do grep -qa -- --replica /proc/$p/cmdline && kill -9 $p; done; true" % D
procs = {}
hold = {"a": False}
problems = []
tunnel = None
try:
    out = rsh("b", "rm -rf %s; mkdir -p %s/b/ledger %s/n0 && chmod 700 %s && hostname -s" % (D, D, D, D))
    assert out.returncode == 0, out.stderr
    b_name = out.stdout.strip()
    assert rsh("c", "rm -rf %s; mkdir -p %s/c/ledger && chmod 700 %s" % (D, D, D)).returncode == 0
    for n in ("b", "c"):
        assert rsh(n, "cat > %s/inlet && chmod 755 %s/inlet" % (D, D), stdin=open(binary, "rb")).returncode == 0
    files = {"b": {"b/policy.lua": policy("b", b_name), "client.py": CLIENT,
                   "n0/policy.lua": COMMON + 'workers = { plain = { cmd = { "/bin/sh", "-c", "sleep 0.3; cat \\"$INLET_GOAL_FILE\\" > out" } } }\n'},
             "c": {"c/policy.lua": policy("c", b_name)}}
    for n, group in files.items():
        for path, text in group.items():
            tmp = os.path.join(root, "push")
            open(tmp, "w").write(text)
            assert rsh(n, "cat > %s/%s" % (D, path), stdin=open(tmp)).returncode == 0
    open(HOME["a"] + "/policy.lua", "w").write(policy("a", b_name))
    tunnel = subprocess.Popen(BASE + ["-N", "-o", "StreamLocalBindUnlink=yes", "-o", "ExitOnForwardFailure=yes",
                                      "-R", "%s/a.sock:127.0.0.1:%d" % (D, PORT), args.b])
    time.sleep(3)
    assert tunnel.poll() is None, "the way back to A would not open"
    assert rsh("b", "%s/inlet --home %s/n0 up" % (D, D)).returncode == 0
    def keep_a():
        log = open(root + "/a.log", "a")
        while "stop" not in hold:
            if hold["a"]:
                time.sleep(0.5)
                continue
            procs["a"] = subprocess.Popen([binary, "--home", HOME["a"], "up", "-f", "--replica", "a"], stdout=log, stderr=log)
            procs["a"].wait()
            time.sleep(0.5)
    threading.Thread(target=keep_a, daemon=True).start()
    for n in ("b", "c"):
        rsh(n, LOOP % (D, HOME[n], n, n))

    def leader(limit=90, skip=None):
        end = time.time() + limit
        while time.time() < end:
            for n in "abc":
                if n == skip:
                    continue
                out = inlet(n, "status", "--json", limit=15)
                if out.returncode == 0 and out.stdout.strip().startswith("{"):
                    return n, json.loads(out.stdout)
            time.sleep(0.5)
        return None, None

    acked, goal, led = {}, 0, []

    def adds(n):
        global goal
        took = []
        for _ in range(n):
            goal += 1
            for _ in range(40):
                who, _ = leader(60)
                if who:
                    t0 = time.time()
                    out = inlet(who, "add", "-w", "plain", "-g", str(goal), "--verify", "test -s out")
                    m = re.search(r"queued (\S+)", out.stdout)
                    if out.returncode == 0 and m:
                        acked[m.group(1)] = goal
                        took.append(time.time() - t0)
                        break
                time.sleep(0.5)
        return len(took)

    def down(name):
        if name == "a":
            hold["a"] = True
            procs["a"].send_signal(signal.SIGKILL)
        else:
            rsh(name, KILL)

    def back(name):
        if name == "a":
            hold["a"] = False
        else:
            rsh(name, "rm -f %s/hold" % D)

    where = {"a": "here", "b": b_name, "c": args.c}
    who, _ = leader()
    assert who, "nobody was elected"
    for round in range(4):
        led.append(who)
        print("%s (%s) leads: %d adds answered" % (who, where[who], adds(12)), flush=True)
        down(who)
        nxt, _ = leader(skip=who)
        if not nxt or nxt == who:
            problems.append("nobody else took over from %s" % who)
            back(who)
            break
        print("  killed on its own machine and held down. %s (%s) took over: %d adds" % (nxt, where[nxt], adds(6)), flush=True)
        back(who)
        who = nxt
    led.append(who)
    if len(set(led)) < 3:
        problems.append("the lead only ever sat on %s" % sorted(set(led)))
    sums = {}
    end = time.time() + 240
    while time.time() < end:
        who, st = leader()
        if st and st["live"] == 0 and st["queued"] == 0:
            sums = {"a": hashlib.sha256(open(HOME["a"] + "/ledger/log", "rb").read()).hexdigest()}
            for n in ("b", "c"):
                sums[n] = rsh(n, "sha256sum %s/ledger/log" % HOME[n]).stdout.split(" ")[0]
            if len(set(sums.values())) == 1:
                break
        time.sleep(3)
    else:
        problems.append("the three logs never became the same: %s" % sums)
    who, st = leader()
    tasks = {t["id"]: t for t in st["tasks"]} if st else {}
    heir = {t["retry_of"]: t for t in tasks.values() if t.get("retry_of")}

    def last(t):
        while t["id"] in heir:
            t = heir[t["id"]]
        return t
    lost = [i for i in acked if i not in tasks]
    done = sum(1 for i in acked if i in tasks and last(tasks[i])["state"] == "done")
    if lost:
        problems.append("%d answered adds are not in the ledger" % len(lost))
    if done != len(acked):
        problems.append("%d done of %d answered" % (done, len(acked)))
    print("led in turn by %s. %d answered adds, %d done. sha256 %s on all three machines" % (
        " ".join(led), len(acked), done, sums.get("a", "")[:16]))
finally:
    hold["stop"] = True
    if procs.get("a") and procs["a"].poll() is None:
        procs["a"].send_signal(signal.SIGTERM)
    for n in ("b", "c"):
        rsh(n, "touch %s/stop; for p in $(pgrep -x inlet); do grep -qa -- --replica /proc/$p/cmdline && kill $p; done; true" % D)
    rsh("b", "%s/inlet --home %s/n0 down; sleep 1; rm -rf %s" % (D, D, D))
    rsh("c", "sleep 1; rm -rf %s" % D)
    if tunnel:
        tunnel.terminate()
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("home kept at " + root)
    sys.exit(1)
print("ok: every check held")
