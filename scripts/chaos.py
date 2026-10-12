#!/usr/bin/env python3
"""Kill the daemon at random while two swarms run, then check that nothing
was lost, nothing ran twice, and nothing was left behind.

    scripts/chaos.py path/to/inlet [--tasks 600] [--seconds 240] [--seed 1]

One swarm is plain tasks that sometimes fail their first attempt. The other
is parents that each hand a part to a child through the door and wait for it.
Every few seconds the daemon gets SIGKILL or SIGTERM and is started again.
Exit status is 0 only when every invariant holds.
"""
import argparse, collections, json, os, random, signal, subprocess, sys, tempfile, time

ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("--tasks", type=int, default=600)
ap.add_argument("--seconds", type=int, default=240)
ap.add_argument("--seed", type=int, default=int(time.time()))
ap.add_argument("--home", default="")
args = ap.parse_args()
rng = random.Random(args.seed)
binary = os.path.abspath(args.inlet)
# Not under /tmp by default on purpose: nothing here depends on where it is.
home = args.home or tempfile.mkdtemp(prefix="inlet-chaos-")
os.makedirs(home + "/ledger", exist_ok=True)
os.makedirs(home + "/run", exist_ok=True)
CAP = 50_000_000
PLAIN = args.tasks
PARENTS = max(args.tasks // 10, 4)

open(home + "/policy.lua", "w").write("""
caps = { max_live = 24, max_depth = 3, max_tokens = %d, max_memory_mb = 30000, max_pids = 4000 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 60, memory_mb = 256, pids = 32 }
workers = {
  plain = { cmd = { "/bin/sh", "-c", "n=$(cat \\"$INLET_GOAL_FILE\\"); echo run >> runs; sleep 0.$((n %% 9))$((n %% 7)); [ $((n %% 6)) -eq 0 ] && [ $(wc -l < runs) -lt 2 ] && exit 1; echo $n > out" } },
  lead  = { cmd = { "/bin/sh", "-c", "echo run >> runs; [ -f kid ] || \\"$INLET_BIN\\" door spawn -w kid -g part --verify 'test -f made' --tokens 100 > kid || exit 1; \\"$INLET_BIN\\" door wait --timeout 50 > kids || exit 1; grep -q done kids && touch finished" } },
  kid   = { cmd = { "/bin/sh", "-c", "sleep 0.4; touch made; \\"$INLET_BIN\\" door post made-it" } },
}
""" % CAP)


def cli(*argv, check=True):
    out = subprocess.run([binary, "--home", home, *argv], capture_output=True, text=True)
    if check and out.returncode != 0:
        raise RuntimeError("inlet %s: %s" % (" ".join(argv), out.stderr.strip()))
    return out.stdout


def up():
    for _ in range(100):
        if subprocess.run([binary, "--home", home, "up"], capture_output=True).returncode == 0:
            for _ in range(100):
                if subprocess.run([binary, "--home", home, "status", "--json"], capture_output=True).returncode == 0:
                    return
                time.sleep(0.05)
        time.sleep(0.1)
    raise RuntimeError("daemon would not come up: " + open(home + "/run/daemon.log").read()[-600:])


def status():
    for _ in range(100):
        out = subprocess.run([binary, "--home", home, "status", "--json"], capture_output=True, text=True)
        if out.returncode == 0:
            return json.loads(out.stdout)
        time.sleep(0.1)
    raise RuntimeError("no status")


def pid():
    try:
        return int(open(home + "/run/inlet.pid").read())
    except (OSError, ValueError):
        return 0


up()
manifest = home + "/m.json"
json.dump([
    {"kind": "swarm", "name": "plain", "replicas": 16, "completions": PLAIN,
     "template": {"worker": "plain", "goal": "{index}", "verify": "test -s out", "tokens": 1000}},
    {"kind": "swarm", "name": "leads", "replicas": 4, "completions": PARENTS,
     "template": {"worker": "lead", "goal": "lead {index}", "verify": "test -f finished", "tokens": 1000}},
], open(manifest, "w"))
cli("apply", "-f", manifest)

kills = collections.Counter()
started = time.time()
done = False
while time.time() - started < args.seconds:
    time.sleep(rng.uniform(1.5, 5.0))
    swarms = {s["name"]: s for s in status()["swarms"]}
    if all(s["phase"] != "active" for s in swarms.values()):
        done = True
        break
    sig = rng.choice([signal.SIGKILL, signal.SIGKILL, signal.SIGTERM])
    victim = pid()
    if victim:
        try:
            os.kill(victim, sig)
        except ProcessLookupError:
            pass
        kills[sig.name] += 1
        # Sometimes come straight back, sometimes leave the work alone for a while.
        time.sleep(rng.choice([0.05, 0.3, 1.5]))
        deadline = time.time() + 10
        while os.path.exists("/proc/%d" % victim) and time.time() < deadline:
            time.sleep(0.02)
    up()

# Let it finish in peace.
deadline = time.time() + 180
while time.time() < deadline:
    st = status()
    if all(s["phase"] != "active" for s in st["swarms"]) and st["live"] == 0 and st["queued"] == 0:
        break
    time.sleep(1)
st = status()
tasks = st["tasks"]
swarms = {s["name"]: s for s in st["swarms"]}
problems = []


def check(ok, what):
    if not ok:
        problems.append(what)


for name, want in (("plain", PLAIN), ("leads", PARENTS)):
    s = swarms.get(name, {})
    check(s.get("phase") == "done", "%s: phase %s" % (name, s.get("phase")))
    check(s.get("done") == want, "%s: done %s of %s" % (name, s.get("done"), want))
    check(s.get("active") == 0, "%s: active %s" % (name, s.get("active")))
check(st["live"] == 0 and st["queued"] == 0, "live %s queued %s" % (st["live"], st["queued"]))
check(st["held"] == 0, "held %s" % st["held"])

# Each goal is done exactly once.
done_by_goal = collections.Counter(t["goal"] for t in tasks if t["state"] == "done" and t["swarm"])
dupes = {g: n for g, n in done_by_goal.items() if n != 1}
check(not dupes, "goals done other than once: %s" % dict(list(dupes.items())[:5]))
check(len([g for g in done_by_goal if g.isdigit()]) == PLAIN, "plain goals done: %d" % len([g for g in done_by_goal if g.isdigit()]))

# The purse adds up: what is gone is exactly what crashes kept.
crashed = sum(t["budget_tokens"] for t in tasks if t["reason"] == "crash")
check(st["available"] + crashed == CAP, "purse: available %d + crashed %d != cap %d" % (st["available"], crashed, CAP))

# No task is settled twice or stuck unsettled.
states = collections.Counter(t["state"] for t in tasks)
check(set(states) <= {"done", "failed", "killed"}, "unsettled tasks: %s" % dict(states))

# A worker never ran more attempts than its plan allows.
over = [t["id"] for t in tasks if t["attempt"] > t["max_attempts"]]
check(not over, "attempts past the limit: %s" % over[:3])

# Nothing left behind on the host.
cli("status")
os.kill(pid(), signal.SIGINT)
time.sleep(1.5)
left = subprocess.run(["pgrep", "-f", home + "/"], capture_output=True, text=True).stdout.split()
left = [p for p in left if p != str(os.getpid())]
check(not left, "processes left: %s" % left[:5])
for sub in ("run/live", "run/exit", "run/cells"):
    path = os.path.join(home, sub)
    names = os.listdir(path) if os.path.isdir(path) else []
    check(not names, "%s not empty: %d left" % (sub, len(names)))

reasons = collections.Counter(t["reason"] for t in tasks if t["state"] != "done")
print("seed %d  kills %s  finished under fire: %s" % (args.seed, dict(kills), done))
print("tasks %d  done %d  not done by reason %s" % (len(tasks), states["done"], dict(reasons)))
print("taken back %d" % open(home + "/run/daemon.log").read().count("took back"))
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("home kept at " + home)
    sys.exit(1)
print("ok: every invariant held")
