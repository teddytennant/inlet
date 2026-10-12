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
ap.add_argument("--retain", type=int, default=0, help="forget settled tasks after this many seconds, so checkpoints and pruning happen under fire")
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
retain = %s
default_budget = { tokens = 1000, seconds = 60, memory_mb = 256, pids = 32 }
workers = {
  plain = { cmd = { "/bin/sh", "-c", "n=$(cat \\"$INLET_GOAL_FILE\\"); echo run >> runs; sleep 0.$((n %% 9))$((n %% 7)); [ $((n %% 6)) -eq 0 ] && [ $(wc -l < runs) -lt 2 ] && exit 1; echo $n > out" } },
  lead  = { cmd = { "/bin/sh", "-c", "echo run >> runs; [ -f kid ] || \\"$INLET_BIN\\" door spawn -w kid -g part --verify 'test -f made' --tokens 100 > kid || exit 1; \\"$INLET_BIN\\" door wait --timeout 50 > kids || exit 1; grep -q done kids && touch finished" } },
  kid   = { cmd = { "/bin/sh", "-c", "sleep 0.4; touch made; \\"$INLET_BIN\\" door post made-it" } },
}
""" % (CAP, '"30d"' if not args.retain else args.retain))


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
finished = False
while time.time() - started < args.seconds:
    time.sleep(rng.uniform(1.5, 5.0))
    swarms = {s["name"]: s for s in status()["swarms"]}
    if all(s["phase"] != "active" for s in swarms.values()):
        finished = True
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
swarms = {s["name"]: s for s in st["swarms"]}
problems = []

# The books are checked against the ledger, not the daemon's memory: with
# --retain the daemon has forgotten most of these tasks by now.
import re
raw = open(home + "/ledger/log", "rb").read()
records = []
for m in re.finditer(rb'\{"kind":"(?:task|admit|exit|deny)"[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', raw):
    try:
        records.append(json.loads(m.group(0)))
    except ValueError:
        pass
tasks = {r["id"]: r for r in records if r["kind"] == "task"}
admits = collections.Counter(r["id"] for r in records if r["kind"] == "admit")
exits = collections.defaultdict(list)
for r in records:
    if r["kind"] in ("exit", "deny"):
        exits[r["id"]].append(r)


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

# Every task settled exactly once, and nothing was admitted twice.
check(all(len(exits[i]) == 1 for i in tasks), "tasks not settled exactly once: %d" % sum(1 for i in tasks if len(exits[i]) != 1))
check(all(n == 1 for n in admits.values()), "admitted more than once: %d" % sum(1 for n in admits.values() if n != 1))


def done(i):
    e = exits[i]
    return len(e) == 1 and e[0]["kind"] == "exit" and e[0]["reason"] == "ok"


# Each swarm goal is done exactly once.
done_by_goal = collections.Counter(t["goal"] for i, t in tasks.items() if done(i) and t.get("plan", {}).get("owner"))
dupes = {g: n for g, n in done_by_goal.items() if n != 1}
check(not dupes, "goals done other than once: %s" % dict(list(dupes.items())[:5]))
plain_done = len([g for g in done_by_goal if g.isdigit()])
check(plain_done == PLAIN, "plain goals done: %d" % plain_done)

# The purse adds up to the token: admitted, less refunded, is what is gone.
debited = sum(r["tokens"] for r in records if r["kind"] == "admit" and not tasks.get(r["id"], {}).get("parent"))
kept = {}
for i, t in tasks.items():
    e = exits[i]
    if len(e) == 1 and e[0]["kind"] == "exit":
        kept[i] = e[0]["tokens_used"]
roots_kept = sum(n for i, n in kept.items() if not tasks[i].get("parent"))
check(st["available"] + roots_kept == CAP, "purse: available %d + kept by roots %d != cap %d" % (st["available"], roots_kept, CAP))
# No worker called a model, so only a crash may keep anything.
wrong = [i for i, n in kept.items() if n and exits[i][0]["reason"] != "crash"]
check(not wrong, "tokens kept by a task that did not crash: %s" % wrong[:3])

# A worker never ran more attempts than its plan allows.
over = [i for i, t in tasks.items() if t.get("plan", {}).get("attempt", 1) > max(t.get("plan", {}).get("max_attempts", 1), 1)]
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

reasons = collections.Counter(exits[i][0]["reason"] for i in tasks if exits[i] and not done(i))
print("seed %d  kills %s  finished under fire: %s" % (args.seed, dict(kills), finished))
print("tasks %d  done %d  not done by reason %s  in memory %d" % (len(tasks), sum(1 for i in tasks if done(i)), dict(reasons), len(st["tasks"])))
print("taken back %d" % open(home + "/run/daemon.log").read().count("took back"))
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("home kept at " + home)
    sys.exit(1)
print("ok: every invariant held")
