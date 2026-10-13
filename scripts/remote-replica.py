#!/usr/bin/env python3
"""Two replicas here and a third on another machine, reached by ssh. The far
one runs no daemon: its disk holds the log and its `inlet replica vote`
answers, which is all a majority needs of it.

    scripts/remote-replica.py path/to/inlet user@host [--dir /tmp/inlet-replica]

What is done to it:
  the leader is killed and kept down, so the other local replica can only
    win with the far one's vote, and every write then crosses the network
  the dead one comes back and is brought up to date
  the link to the far replica is cut: the two here are still most of three
  with the link cut the leader is killed too: one of three must not lead
  the link heals, and it does

Checked: every answered add is in the ledger and done, and all three logs,
the far one included, end as the same bytes. Needs sudo here for iptables.
"""
import argparse, hashlib, json, os, re, signal, subprocess, sys, tempfile, threading, time

ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("host")
ap.add_argument("--dir", default="/tmp/inlet-replica")
args = ap.parse_args()
binary = os.path.abspath(args.inlet)
root = tempfile.mkdtemp(prefix="inlet-remote-replica-", dir=os.path.expanduser("~/.cache"))
RULE = ["OUTPUT", "-p", "tcp", "--dport", "22", "-m", "dscp", "--dscp-class", "CS3", "-j", "DROP"]
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ControlPath=none"]
far_bin, far_home = args.dir + "/inlet", args.dir + "/home"


def sh(*cmd, **kw):
    return subprocess.run(list(cmd), capture_output=True, text=True, **kw)


def cut(on):
    sh("sudo", "-n", "iptables", "-I" if on else "-D", *RULE)


homes = {n: os.path.join(root, n) for n in ("a", "b", "n0")}
for home in homes.values():
    os.makedirs(home + "/ledger")
quote = lambda argv: ", ".join('"%s"' % a for a in argv)
# A replica that is down is a machine that is down: its home answers nobody.
gate = lambda name: os.path.join(root, name + ".down")
gated = lambda name: ["/bin/sh", "-c", 'test -e %s && exit 1; exec %s --home %s \\"$@\\"' % (gate(name), binary, homes[name]), "sh"]
COMMON = """
caps = { max_live = 8, max_depth = 2, max_tokens = 50000000, max_memory_mb = 8000, max_pids = 800 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 60, memory_mb = 128, pids = 32 }
"""
open(homes["n0"] + "/policy.lua", "w").write(COMMON + 'workers = { plain = { cmd = { "/bin/sh", "-c", "sleep 0.3; cat \\"$INLET_GOAL_FILE\\" > out" } } }\n')
for r in ("a", "b"):
    open(homes[r] + "/policy.lua", "w").write(COMMON + "node_grace = 30\nreplicas = {\n  a = { cmd = { %s } },\n  b = { cmd = { %s } },\n  c = { cmd = { %s } },\n}\nnodes = { n0 = { cmd = { %s }, slots = 6 } }\nworkers = { plain = { on = \"nodes\" } }\n" % (
        quote(gated("a")), quote(gated("b")),
        quote(SSH + ["-o", "IPQoS=cs3", args.host, far_bin, "--home", far_home]), quote([binary, "--home", homes["n0"]])))
assert sh(*SSH, args.host, "rm -rf %s && mkdir -p %s/ledger && chmod 700 %s" % (far_home, far_home, args.dir)).returncode == 0
assert sh("scp", "-q", "-o", "BatchMode=yes", binary, "%s:%s" % (args.host, far_bin)).returncode == 0


def run(home, *argv, limit=25):
    try:
        return subprocess.run([binary, "--home", home, *argv], capture_output=True, text=True, timeout=limit)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(argv, 124, "", "timeout")


procs, wanted, stopping = {}, {"a": True, "b": True}, False


def keep(name):
    log = open(os.path.join(root, name + ".log"), "a")
    while not stopping:
        if not wanted[name]:
            time.sleep(0.2)
            continue
        procs[name] = subprocess.Popen([binary, "--home", homes[name], "up", "-f", "--replica", name], stdout=log, stderr=log)
        procs[name].wait()
        time.sleep(0.5)


def leader(limit=60):
    end = time.time() + limit
    while time.time() < end:
        for r in ("a", "b"):
            out = run(homes[r], "status", "--json", limit=5)
            if out.returncode == 0 and out.stdout.strip():
                return r, json.loads(out.stdout)
        time.sleep(0.3)
    return None, None


def back(name):
    os.remove(gate(name))
    wanted[name] = True


def down(name):
    open(gate(name), "w").close()
    wanted[name] = False
    procs[name].send_signal(signal.SIGKILL)
    procs[name].wait()


acked, slow = {}, []


def add(goal, limit=25):
    who, _ = leader(limit)
    if not who:
        return False
    t0 = time.time()
    out = run(homes[who], "add", "-w", "plain", "-g", str(goal), "--verify", "test -s out", limit=limit)
    m = re.search(r"queued (\S+)", out.stdout)
    if out.returncode == 0 and m:
        acked[m.group(1)] = goal
        slow.append(time.time() - t0)
        return True
    return False


def adds(first, n):
    took = len(slow)
    for goal in range(first, first + n):
        for _ in range(40):
            if add(goal):
                break
            time.sleep(0.5)
    mine = sorted(slow[took:])
    return "%d adds, median %.2fs" % (len(mine), mine[len(mine) // 2] if mine else -1)


problems = []
cut(False)
try:
    assert run(homes["n0"], "up").returncode == 0
    for r in ("a", "b"):
        threading.Thread(target=keep, args=(r,), daemon=True).start()
    first, _ = leader()
    assert first, "nobody was elected"
    print("leader %s. all three reachable: %s" % (first, adds(1, 20)))
    other = "b" if first == "a" else "a"
    down(first)
    second, _ = leader()
    print("killed %s and kept it down: %s leads, on the far replica's vote" % (first, second))
    if second != other:
        problems.append("nobody took over with the far replica's vote")
    print("  every write now crosses the network: %s" % adds(21, 20))
    back(first)
    print("  %s is back: %s" % (first, adds(41, 10)))
    cut(True)
    print("link to the far replica cut: %s" % adds(51, 20))
    who, _ = leader()
    down(who)
    alone, _ = leader(limit=25)
    print("killed %s with the link cut: %s" % (who, "nobody leads, as it should be" if not alone else "%s LEADS ALONE" % alone))
    if alone:
        problems.append("one replica of three led")
    cut(False)
    healed, _ = leader()
    print("link healed: %s leads. %s" % (healed, adds(71, 20)))
    if not healed:
        problems.append("nobody leads after the link healed")
    back(who)
    far_sum = ""
    end = time.time() + 180
    while time.time() < end:
        who, st = leader()
        if st and st["live"] == 0 and st["queued"] == 0 and len(st["tasks"]) >= len(acked):
            sums = {r: hashlib.sha256(open(homes[r] + "/ledger/log", "rb").read()).hexdigest() for r in ("a", "b")}
            far_sum = sh(*SSH, args.host, "sha256sum %s/ledger/log" % far_home).stdout.split(" ")[0]
            if sums["a"] == sums["b"] == far_sum:
                break
        time.sleep(2)
    else:
        problems.append("the three logs never became the same")
    who, st = leader()
    tasks = {t["id"]: t for t in st["tasks"]} if st else {}
    lost = [i for i in acked if i not in tasks]
    if lost:
        problems.append("%d answered adds are not in the ledger" % len(lost))
    done = sum(1 for t in tasks.values() if t["state"] == "done")
    if done < len(acked):
        problems.append("%d done of %d answered" % (done, len(acked)))
    print("%d answered adds, %d done, log %d bytes on all three, sha256 %s" % (len(acked), done, os.path.getsize(homes["a"] + "/ledger/log"), far_sum[:16]))
finally:
    cut(False)
    stopping = True
    for p in procs.values():
        if p.poll() is None:
            p.send_signal(signal.SIGINT)
    run(homes["n0"], "down")
    sh(*SSH, args.host, "rm -rf %s" % args.dir)
if sh("sudo", "-n", "iptables", "-C", *RULE).returncode == 0:
    problems.append("the drop rule was left in place")
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("homes kept at " + root)
    sys.exit(1)
print("ok: every check held")
