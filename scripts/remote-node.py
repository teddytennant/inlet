#!/usr/bin/env python3
"""One node on another machine, reached by ssh across a real network, and one
node here. The link to the far node is then really cut: this box drops the
packets of the leader's ssh, and only those, marked by DSCP so nothing else
going to that host is touched.

    scripts/remote-node.py path/to/inlet user@host [--dir /tmp/inlet-node]

The binary is copied to the far machine first. Needs sudo here for iptables.
Checked: the swarm finishes, the far node lets go of its work on its own while
cut off, a short cut moves nothing, purses add up, nothing is left running.
"""
import argparse, collections, json, os, signal, subprocess, sys, tempfile, time

ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("host")
ap.add_argument("--dir", default="/tmp/inlet-node")
ap.add_argument("--tasks", type=int, default=120)
ap.add_argument("--wait", action="store_true")
args = ap.parse_args()
binary = os.path.abspath(args.inlet)
root = tempfile.mkdtemp(prefix="inlet-remote-", dir=os.path.expanduser("~/.cache"))
CAP, GRACE = 50_000_000, 20
RULE = ["OUTPUT", "-p", "tcp", "--dport", "22", "-m", "dscp", "--dscp-class", "CS3", "-j", "DROP"]
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ControlPath=none"]
MARKED = SSH + ["-o", "IPQoS=cs3", args.host]
far_bin, far_home = args.dir + "/inlet", args.dir + "/home"


def sh(*cmd, **kw):
    return subprocess.run(list(cmd), capture_output=True, text=True, **kw)


def cut(on):
    sh("sudo", "-n", "iptables", "-I" if on else "-D", *RULE)


def far(*argv):  # an unmarked ssh: sees the node even while the leader cannot
    return sh(*SSH, args.host, far_bin, "--home", far_home, *argv)


WORKER = 'plain = { cmd = { "/bin/sh", "-c", "n=$(cat \\"$INLET_GOAL_FILE\\"); sleep $((2 + n % 16)); echo $n > out" } },'
COMMON = """
caps = { max_live = 12, max_depth = 2, max_tokens = %d, max_memory_mb = 4000, max_pids = 800 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 120, memory_mb = 128, pids = 32 }
""" % CAP
leader, near = root + "/leader", root + "/near"
for home in (leader, near):
    os.makedirs(home)
open(near + "/policy.lua", "w").write(COMMON + "workers = { %s }\n" % WORKER)
quote = lambda argv: ", ".join('"%s"' % a for a in argv)
open(leader + "/policy.lua", "w").write(COMMON + "node_grace = %d\n%snodes = {\n  far = { cmd = { %s }, slots = 4 },\n  near = { cmd = { %s }, slots = 4 },\n}\nworkers = { plain = { on = \"nodes\" } }\n" % (
    GRACE, 'node_loss = "wait"\n' if args.wait else "", quote(MARKED + [far_bin, "--home", far_home]), quote([binary, "--home", near])))
assert sh(*SSH, args.host, "rm -rf %s && mkdir -p %s && chmod 700 %s" % (far_home, far_home, args.dir)).returncode == 0
assert sh("scp", "-q", "-o", "BatchMode=yes", binary, "%s:%s" % (args.host, far_bin)).returncode == 0
assert sh("scp", "-q", "-o", "BatchMode=yes", near + "/policy.lua", "%s:%s/policy.lua" % (args.host, far_home)).returncode == 0


def local(home, *argv):
    return sh(binary, "--home", home, *argv)


def status(call):
    for _ in range(60):
        out = call("status", "--json")
        if out.returncode == 0 and out.stdout.strip():
            return json.loads(out.stdout)
        time.sleep(0.5)
    raise RuntimeError("no status")


lead = lambda *a: local(leader, *a)
problems = []
cut(False)
try:
    assert far("up").returncode == 0, "far node would not start"
    assert local(near, "up").returncode == 0 and lead("up").returncode == 0
    for _ in range(60):
        if all(n["ready"] for n in status(lead)["nodes"]):
            break
        time.sleep(0.5)
    t0 = time.time()
    rtt = sh(*MARKED, "true")
    print("one ssh round trip to the far node: %.2fs" % (time.time() - t0))
    json.dump({"kind": "swarm", "name": "plain", "replicas": 8, "completions": args.tasks,
               "template": {"worker": "plain", "goal": "{index}", "verify": "test -s out", "retries": 3}}, open(root + "/m.json", "w"))
    assert lead("apply", "-f", root + "/m.json").returncode == 0
    started = time.time()
    stamp = lambda: "t+%3d " % (time.time() - started)

    time.sleep(15)
    print(stamp() + "dropping the leader's packets to the far node for %ds" % (GRACE + 15))
    cut(True)
    time.sleep(GRACE // 2 + 6)
    seen = status(far)
    fenced = sum(1 for t in seen["tasks"] if t["reason"] == "fenced")
    print(stamp() + "far node, asked another way: live %s, fenced %d" % (seen["live"], fenced))
    if seen["live"] != 0:
        problems.append("the far node kept working while cut off")
    time.sleep(GRACE // 2 + 9)
    ready = {n["name"]: n["ready"] for n in status(lead)["nodes"]}
    print(stamp() + "leader sees %s" % ready)
    if ready["far"]:
        problems.append("the leader never gave the far node up")
    cut(False)
    print(stamp() + "healed")

    time.sleep(12)
    print(stamp() + "kill -9 the leader")
    os.kill(int(open(leader + "/run/inlet.pid").read()), signal.SIGKILL)
    time.sleep(1)
    assert lead("up").returncode == 0

    time.sleep(10)
    before = sum(1 for t in status(lead)["tasks"] if t["reason"] == "node")
    print(stamp() + "dropping packets for 3s, inside a lease")
    cut(True)
    time.sleep(3)
    cut(False)
    time.sleep(8)
    after = sum(1 for t in status(lead)["tasks"] if t["reason"] == "node")
    if after != before:
        problems.append("a 3s cut moved %d tasks" % (after - before))

    deadline = time.time() + 900
    while time.time() < deadline:
        st = status(lead)
        if st["swarms"][0]["phase"] != "active" and st["live"] == 0 and st["queued"] == 0:
            break
        time.sleep(3)
    st = status(lead)
    swarm = st["swarms"][0]
    if swarm["phase"] != "done" or swarm["done"] != args.tasks:
        problems.append("swarm %s, %s of %s" % (swarm["phase"], swarm["done"], args.tasks))
    if st["held"] != 0:
        problems.append("leader held %s" % st["held"])
    twice = collections.Counter()
    share = {}
    for name, call in (("far", far), ("near", lambda *a: local(near, *a))):
        end = time.time() + 60
        while time.time() < end and status(call)["live"]:
            time.sleep(1)
        ns = status(call)
        if ns["held"] != 0 or ns["live"] != 0:
            problems.append("%s: held %s live %s" % (name, ns["held"], ns["live"]))
        share[name] = sum(1 for t in ns["tasks"] if t["state"] == "done")
        for t in ns["tasks"]:
            if t["state"] == "done":
                twice[t["goal"]] += 1
    doubles = sum(1 for n in twice.values() if n > 1)
    if args.wait and doubles:
        problems.append("told to wait, and %d goals were finished twice" % doubles)
    moved = sum(1 for t in st["tasks"] if t["reason"] == "node")
    print("%d tasks in %ds  done on far %d, near %d  moved %d  fenced on far %d  goals finished twice %d" % (
        args.tasks, time.time() - started, share["far"], share["near"], moved, fenced, doubles))
finally:
    cut(False)
    lead("down")
    local(near, "down")
    far("down")
    time.sleep(1)
    left = sh(*SSH, args.host, "pgrep -u $USER -f %s | wc -l; rm -rf %s" % (far_home, args.dir)).stdout.split()
    if left and left[0] not in ("0", "1"):
        problems.append("processes left on the far machine: %s" % left[0])
if sh("sudo", "-n", "iptables", "-C", *RULE).returncode == 0:
    problems.append("the drop rule was left in place")
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("homes kept at " + root)
    sys.exit(1)
print("ok: every check held")
