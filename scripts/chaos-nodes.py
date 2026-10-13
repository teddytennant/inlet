#!/usr/bin/env python3
"""The chaos test across machines: a leader and some nodes, all on this box,
each a daemon of its own. Leader and nodes are killed at random, and a node
is sometimes left down long enough to be given up on.

    scripts/chaos-nodes.py path/to/inlet [--nodes 3] [--tasks 300] [--seconds 180] [--seed 1]
                           [--flaky] [--standby]

Checked at the end: the swarm finished, every goal is done exactly once as
the leader sees it, the leader's purse is whole, each node's purse is short
only by what crashed there, and nothing is left running.
"""
import argparse, collections, json, os, random, re, signal, subprocess, sys, tempfile, time

def cmdline(p):
    try:
        return open("/proc/%s/cmdline" % p).read().replace("\0", " ")
    except OSError:
        return "link.py"


ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("--nodes", type=int, default=3)
ap.add_argument("--tasks", type=int, default=300)
ap.add_argument("--seconds", type=int, default=180)
ap.add_argument("--seed", type=int, default=int(time.time()))
ap.add_argument("--home", default="")
ap.add_argument("--flaky", action="store_true", help="the leader reaches its nodes through a link that drops, delays, and hangs")
ap.add_argument("--standby", action="store_true", help="a standby leader takes over when the leader is killed")
args = ap.parse_args()
rng = random.Random(args.seed)
binary = os.path.abspath(args.inlet)
root = args.home or tempfile.mkdtemp(prefix="inlet-chaos-nodes-")
CAP = 50_000_000
GRACE = 6
names = ["n%d" % i for i in range(args.nodes)]
homes = {name: os.path.join(root, name) for name in names + ["leader"]}
for home in homes.values():
    os.makedirs(home + "/ledger", exist_ok=True)
    os.makedirs(home + "/run", exist_ok=True)

COMMON = """
caps = { max_live = 12, max_depth = 2, max_tokens = %d, max_memory_mb = 30000, max_pids = 4000 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 60, memory_mb = 256, pids = 32 }
""" % CAP
for name in names:
    open(homes[name] + "/policy.lua", "w").write(COMMON + """
workers = {
  plain = { cmd = { "/bin/sh", "-c", "n=$(cat \\"$INLET_GOAL_FILE\\"); echo run >> runs; sleep 0.$((n % 9))$((n % 7)); [ $((n % 6)) -eq 0 ] && [ $(wc -l < runs) -lt 2 ] && exit 1; echo $n > out" } },
}
""")
# A bad link, for --flaky: one call in six never arrives, one in twenty hangs
# past the leader's own timeout, and the rest are late by up to a third of a
# second.
link = os.path.join(root, "link.py")
open(link, "w").write("""#!/usr/bin/env python3
import os, random, sys, time
roll = random.random()
if roll < 0.16:
    sys.exit(1)
if roll < 0.21:
    time.sleep(25)
    sys.exit(1)
time.sleep(random.random() * 0.3)
os.execv(sys.argv[1], sys.argv[1:])
""")
os.chmod(link, 0o755)
via = '"%s", "%s"' % (link, binary) if args.flaky else '"%s"' % binary
open(homes["leader"] + "/policy.lua", "w").write(COMMON + "node_grace = %d\nnodes = {\n%s}\nworkers = { plain = { on = \"nodes\" } }\n" % (
    GRACE,
    "".join('  %s = { cmd = { %s, "--home", "%s" }, slots = 6 },\n' % (n, via, homes[n]) for n in names),
))


def run(home, *argv):
    return subprocess.run([binary, "--home", home, *argv], capture_output=True, text=True)


def up(home):
    for _ in range(100):
        if run(home, "up").returncode == 0:
            return
        time.sleep(0.1)
    raise RuntimeError("daemon would not come up in " + home)


def status(home):
    for _ in range(100):
        out = run(home, "status", "--json")
        if out.returncode == 0:
            return json.loads(out.stdout)
        time.sleep(0.1)
    # Nobody answered for ten seconds. Say what there is to see before
    # giving up: who is running, who holds the ledger, what they last said.
    import fcntl, glob
    print("no status from %s for 10s" % home)
    log = home + "/ledger/log"
    try:
        probe = open(log, "rb")
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print("  the ledger is not locked by anybody")
        except OSError:
            print("  the ledger is locked")
        ino = os.stat(log).st_ino
        for fd in glob.glob("/proc/[0-9]*/fd/*"):
            try:
                if os.stat(fd).st_ino == ino:
                    p = fd.split("/")[2]
                    print("  open in %s: %s" % (p, cmdline(p)[-110:]))
            except OSError:
                pass
    except OSError as e:
        print("  ledger: %s" % e)
    for p in subprocess.run(["pgrep", "-f", home + " "], capture_output=True, text=True).stdout.split():
        print("  running %s: %s" % (p, cmdline(p)[-110:]))
    for name in ("standby.log", "leader/run/daemon.log"):
        try:
            print("  %s: %s" % (name, open(os.path.join(root, name)).read()[-400:].replace("\n", " | ")))
        except OSError:
            pass
    raise RuntimeError("no status from " + home)


def pid(home):
    try:
        return int(open(home + "/run/inlet.pid").read())
    except (OSError, ValueError):
        return 0


standbys = []


def stand_by():
    log = open(os.path.join(root, "standby.log"), "a")
    standbys.append(subprocess.Popen([binary, "--home", homes["leader"], "up", "-f", "--standby"], stdout=log, stderr=log))


for home in homes.values():
    up(home)
if args.standby:
    stand_by()
manifest = os.path.join(root, "m.json")
json.dump({"kind": "swarm", "name": "plain", "replicas": 6 * args.nodes, "completions": args.tasks,
           "template": {"worker": "plain", "goal": "{index}", "verify": "test -s out", "retries": 3}}, open(manifest, "w"))
out = run(homes["leader"], "apply", "-f", manifest)
assert out.returncode == 0, out.stderr

kills = collections.Counter()
started = time.time()
finished = False
while time.time() - started < args.seconds:
    time.sleep(rng.uniform(2.0, 5.0))
    if status(homes["leader"])["swarms"][0]["phase"] != "active":
        finished = True
        break
    who = rng.choice(list(homes))
    sig = rng.choice([signal.SIGKILL, signal.SIGKILL, signal.SIGTERM])
    victim = pid(homes[who])
    if not victim:
        continue
    try:
        os.kill(victim, sig)
    except ProcessLookupError:
        pass
    # Mostly back at once. Now and then gone for longer than the leader waits.
    gone = rng.choice([0.05, 0.5, 0.5, GRACE + 4])
    kills["%s %s%s" % ("leader" if who == "leader" else "node", sig.name, " long" if gone > GRACE else "")] += 1
    time.sleep(gone)
    deadline = time.time() + 10
    while os.path.exists("/proc/%d" % victim) and time.time() < deadline:
        time.sleep(0.02)
    if who == "leader" and args.standby:
        # Nobody starts it again. The one standing by takes the home, and a
        # new one lines up behind it.
        status(homes["leader"])
        stand_by()
    else:
        up(homes[who])

deadline = time.time() + 240
while time.time() < deadline:
    st = status(homes["leader"])
    if st["swarms"][0]["phase"] != "active" and st["live"] == 0 and st["queued"] == 0:
        break
    time.sleep(1)
# A node given up on may still be finishing what it had.
deadline = time.time() + 90
while time.time() < deadline and any(status(homes[n])["live"] + status(homes[n])["queued"] for n in names):
    time.sleep(1)

problems = []


def check(ok, what):
    if not ok:
        problems.append(what)


def ledger(home):
    raw = open(home + "/ledger/log", "rb").read()
    recs = []
    for m in re.finditer(rb'\{"kind":"(?:task|admit|exit|deny)"[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', raw):
        try:
            recs.append(json.loads(m.group(0)))
        except ValueError:
            pass
    tasks = {r["id"]: r for r in recs if r["kind"] == "task"}
    ends = collections.defaultdict(list)
    for r in recs:
        if r["kind"] in ("exit", "deny"):
            ends[r["id"]].append(r)
    budget = {r["id"]: r["tokens"] for r in recs if r["kind"] == "admit"}
    return tasks, ends, budget


st = status(homes["leader"])
swarm = st["swarms"][0]
check(swarm["phase"] == "done", "swarm phase %s" % swarm["phase"])
check(swarm["done"] == args.tasks, "swarm done %s of %s" % (swarm["done"], args.tasks))
check(swarm["active"] == 0, "swarm active %s" % swarm["active"])
check(st["held"] == 0, "leader held %s" % st["held"])
tasks, ends, _ = ledger(homes["leader"])
# The leader runs no cell and calls no model. All it can be short by is what a
# node reported as spent, and no worker here spends anything, so that is only
# ever a slice a node kept because the task crashed there.
reported = sum(e[0].get("tokens_used", 0) for e in ends.values() if len(e) == 1 and e[0]["kind"] == "exit")
# And what a node spent on a task the leader had given up on, charged late.
late = {}
for home_log in (homes["leader"] + "/ledger/log.old", homes["leader"] + "/ledger/log"):
    if os.path.exists(home_log):
        for m in re.finditer(rb'\{"kind":"charge","id":"[^"]*","remote":"([^"]*)","tokens":(\d+)', open(home_log, "rb").read()):
            late.setdefault(m.group(1), int(m.group(2)))
reported += sum(late.values())
check(st["available"] + reported == CAP, "leader purse: available %d + reported by nodes %d != %d" % (st["available"], reported, CAP))
check(all(len(ends[i]) == 1 for i in tasks), "leader tasks not settled exactly once: %d" % sum(1 for i in tasks if len(ends[i]) != 1))
done = collections.Counter(t["goal"] for i, t in tasks.items() if ends[i] and ends[i][0].get("reason") == "ok")
check(len(done) == args.tasks and all(n == 1 for n in done.values()), "goals done once on the leader: %d of %d, repeats %d" % (len(done), args.tasks, sum(1 for n in done.values() if n > 1)))
moved = sum(1 for i in tasks if ends[i] and ends[i][0].get("reason") == "node")

twice = collections.Counter()
node_lost = 0
for name in names:
    ns = status(homes[name])
    ntasks, nends, nbudget = ledger(homes[name])
    check(ns["live"] == 0 and ns["queued"] == 0, "%s: live %s queued %s" % (name, ns["live"], ns["queued"]))
    check(ns["held"] == 0, "%s: held %s" % (name, ns["held"]))
    lost = sum(nbudget.get(i, 0) for i in ntasks if nends[i] and nends[i][0].get("reason") == "crash")
    node_lost += lost
    check(ns["available"] + lost == CAP, "%s: purse available %d + crashed %d != %d" % (name, ns["available"], lost, CAP))
    for i, t in ntasks.items():
        if nends[i] and nends[i][0].get("reason") == "ok":
            twice[t["goal"]] += 1

check(reported <= node_lost, "leader was charged %d, nodes only lost %d to crashes" % (reported, node_lost))

# Standbys first, and wait for them: one still waiting would take the home
# the moment the leader below lets go of it.
lead = pid(homes["leader"])
for proc in standbys:
    if proc.poll() is None and proc.pid != lead:
        proc.kill()
        proc.wait()
for home in homes.values():
    victim = pid(home)
    if victim:
        os.kill(victim, signal.SIGINT)
time.sleep(2)
for proc in standbys:
    if proc.poll() is None:
        proc.wait(timeout=10)
# Give a daemon that is putting its workers down the time it is allowed.
left = []
for _ in range(30):
    left = [p for p in subprocess.run(["pgrep", "-f", root + "/"], capture_output=True, text=True).stdout.split() if p != str(os.getpid())]
    # The bad link is this script's own: a call it is holding up is not a leak.
    left = [p for p in left if "link.py" not in cmdline(p)]
    if not left:
        break
    time.sleep(0.5)
def what(p):
    try:
        return "%s %s" % (p, open("/proc/%s/cmdline" % p).read().replace("\0", " ")[-90:])
    except OSError:
        return p


check(not left, "processes left: %s" % [what(p) for p in left[:5]])
for name, home in homes.items():
    for sub in ("run/live", "run/exit", "run/cells"):
        path = os.path.join(home, sub)
        n = len(os.listdir(path)) if os.path.isdir(path) else 0
        check(n == 0, "%s %s not empty: %d left" % (name, sub, n))

print("seed %d  kills %s  finished under fire: %s" % (args.seed, dict(kills), finished))
if args.standby:
    took = open(os.path.join(root, "standby.log")).read().count("taking over")
    print("standby takeovers %d" % took)
print("leader tasks %d  placed again after a lost node %d  goals a node finished more than once %d" % (
    len(tasks), moved, sum(1 for n in twice.values() if n > 1)))
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("homes kept at " + root)
    sys.exit(1)
print("ok: every invariant held")
