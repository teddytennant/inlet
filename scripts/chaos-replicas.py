#!/usr/bin/env python3
"""Three replicas of the ledger, each with a home of its own, and two nodes
that do the work. Replicas are killed, stopped in their tracks with SIGSTOP
and let go again, and restarted, while a client keeps adding tasks to whoever
answers.

    scripts/chaos-replicas.py path/to/inlet [--tasks 150] [--seconds 120] [--flaky]

Checked at the end:
  every add that was answered "queued" is in the ledger, and settled once
  the three logs are the same bytes
  terms only ever go up, and each is written by one replica
  nothing is held and nothing is left running
"""
import argparse, collections, json, os, random, re, signal, subprocess, sys, tempfile, threading, time

def cmdline(p):
    try:
        return open("/proc/%s/cmdline" % p).read().replace("\0", " ")
    except OSError:
        return "link.py"


ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("--tasks", type=int, default=150)
ap.add_argument("--seconds", type=int, default=120)
ap.add_argument("--seed", type=int, default=int(time.time()))
ap.add_argument("--home", default="")
ap.add_argument("--compact", type=int, default=0, help="replicas let go of the log every this many bytes")
ap.add_argument("--grow", action="store_true", help="a fourth replica is added part way through, and one of the first three taken out later")
ap.add_argument("--flaky", action="store_true", help="replicas reach each other over a link that drops and delays")
args = ap.parse_args()
rng = random.Random(args.seed)
binary = os.path.abspath(args.inlet)
root = args.home or tempfile.mkdtemp(prefix="inlet-chaos-replicas-")
CAP = 50_000_000
reps, nodes = ["a", "b", "c"], ["n0", "n1"]
homes = {name: os.path.join(root, name) for name in reps + ["d"] + nodes}
for home in homes.values():
    os.makedirs(home + "/ledger", exist_ok=True)
    os.makedirs(home + "/run", exist_ok=True)
link = os.path.join(root, "link.py")
open(link, "w").write("""#!/usr/bin/env python3
import os, random, sys, time
roll = random.random()
if roll < 0.10:
    sys.exit(1)
if roll < 0.13:
    time.sleep(20)
    sys.exit(1)
time.sleep(random.random() * 0.2)
os.execv(sys.argv[1], sys.argv[1:])
""")
os.chmod(link, 0o755)
via = '"%s", "%s"' % (link, binary) if args.flaky else '"%s"' % binary
COMMON = """
caps = { max_live = 12, max_depth = 2, max_tokens = %d, max_memory_mb = 30000, max_pids = 4000 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 60, memory_mb = 256, pids = 32 }
""" % CAP
for n in nodes:
    open(homes[n] + "/policy.lua", "w").write(COMMON + """
workers = { plain = { cmd = { "/bin/sh", "-c", "n=$(cat \\"$INLET_GOAL_FILE\\"); sleep 0.$((n % 9))$((n % 7)); echo $n > out" } } }
""")
def write_policies(members):
    """The same set of replicas in every member's policy."""
    for r in members:
        tmp = homes[r] + "/policy.lua.new"
        open(tmp, "w").write(COMMON + "node_grace = 20\nreplicas = {\n%s}\nnodes = {\n%s}\nworkers = { plain = { on = \"nodes\" } }\n" % (
            "".join('  %s = { cmd = { %s, "--home", "%s" } },\n' % (p, via, homes[p]) for p in members),
            "".join('  %s = { cmd = { "%s", "--home", "%s" }, slots = 6 },\n' % (n, binary, homes[n]) for n in nodes)))
        os.rename(tmp, homes[r] + "/policy.lua")


write_policies(reps)


def run(home, *argv, limit=20):
    try:
        return subprocess.run([binary, "--home", home, *argv], capture_output=True, text=True, timeout=limit)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(argv, 124, "", "timeout")


procs, stopping = {}, False
guard = threading.Lock()


def keep(name):
    """What systemd would do: when the replica exits, start it again."""
    log = open(os.path.join(root, name + ".log"), "a")
    while not stopping and name in reps:
        env = dict(os.environ, INLET_ROTATE_BYTES=str(args.compact)) if args.compact else None
        p = subprocess.Popen([binary, "--home", homes[name], "up", "-f", "--replica", name], stdout=log, stderr=log, env=env)
        with guard:
            procs[name] = p
        p.wait()
        time.sleep(0.5)


for n in nodes:
    assert run(homes[n], "up").returncode == 0
for r in reps:
    threading.Thread(target=keep, args=(r,), daemon=True).start()


def leader():
    for r in reps:
        out = run(homes[r], "status", "--json", limit=5)
        if out.returncode == 0 and out.stdout.strip():
            try:
                return r, json.loads(out.stdout)
            except ValueError:
                pass
    return None, None


def floor(r):
    try:
        return int(open(homes[r] + "/ledger/floor").read())
    except (OSError, ValueError):
        return 0


acked, unsure = {}, 0


def client():
    global unsure
    for goal in range(1, args.tasks + 1):
        while not stopping:
            who, _ = leader()
            if not who:
                time.sleep(0.3)
                continue
            out = run(homes[who], "add", "-w", "plain", "-g", str(goal), "--verify", "test -s out", limit=25)
            m = re.search(r"queued (\S+)", out.stdout)
            if out.returncode == 0 and m:
                acked[m.group(1)] = goal
                break
            # Refused, or cut off before the answer. It may or may not be in.
            unsure += 1
            time.sleep(0.3)
        time.sleep(rng.uniform(0.0, 0.15))


adder = threading.Thread(target=client, daemon=True)
adder.start()
kills = collections.Counter()
started = time.time()
grown = shrunk = False
while time.time() - started < args.seconds and adder.is_alive():
    time.sleep(rng.uniform(3.0, 7.0))
    who, _ = leader()
    if args.grow and not grown and len(acked) >= args.tasks * 0.25:
        # One more replica, in every policy, and nothing is restarted for it.
        grown = True
        write_policies(reps + ["d"])
        reps.append("d")
        threading.Thread(target=keep, args=("d",), daemon=True).start()
        kills["replica added"] += 1
        continue
    if args.grow and grown and not shrunk and who and len(acked) >= args.tasks * 0.6:
        # One of the first three goes, for good. Not the leader: it would
        # step down by itself, and this is the harder case to get wrong.
        shrunk = True
        out = [r for r in ("a", "b", "c") if r != who][0]
        write_policies([r for r in reps if r != out])
        reps.remove(out)
        with guard:
            p = procs.get(out)
        if p and p.poll() is None:
            p.send_signal(signal.SIGKILL)
        kills["replica %s removed" % out] += 1
        continue
    # The leader more often than not: that is the case that matters.
    name = who if who and rng.random() < 0.6 else rng.choice(reps)
    with guard:
        p = procs.get(name)
    if not p or p.poll() is not None:
        continue
    what = rng.choice(["kill", "kill", "term", "stop"])
    try:
        if what == "stop":
            # Frozen, not dead: it wakes up still thinking it leads.
            p.send_signal(signal.SIGSTOP)
            time.sleep(rng.uniform(6.0, 12.0))
            p.send_signal(signal.SIGCONT)
        else:
            p.send_signal(signal.SIGKILL if what == "kill" else signal.SIGTERM)
    except ProcessLookupError:
        continue
    kills["%s %s" % ("leader" if name == who else "follower", what)] += 1
adder.join(timeout=600)
problems = []


def check(ok, msg):
    if not ok:
        problems.append(msg)


check(not adder.is_alive(), "the client never got all its tasks in")
deadline = time.time() + 300
st = None
while time.time() < deadline:
    who, st = leader()
    if st and st["live"] == 0 and st["queued"] == 0 and all(t["state"] in ("done", "failed", "killed") for t in st["tasks"]):
        lens = {r: os.path.getsize(homes[r] + "/ledger/log") for r in reps}
        if len(set(lens.values())) == 1 and (not args.compact or all(floor(r) > 0 for r in reps)):
            break
    time.sleep(1)
for _ in range(60):
    who, st = leader()
    if st:
        break
    time.sleep(1)
check(st is not None, "nobody leads at the end")
if st:
    tasks = {t["id"]: t for t in st["tasks"]}
    lost = [i for i in acked if i not in tasks]
    check(not lost, "%d answered adds are not in the ledger: %s" % (len(lost), lost[:3]))
    heirs = collections.defaultdict(list)
    for t in st["tasks"]:
        if t.get("retry_of"):
            heirs[t["retry_of"]].append(t["id"])

    def end(i):
        while heirs.get(i):
            check(len(heirs[i]) == 1, "task %s was retried %d ways" % (i, len(heirs[i])))
            i = heirs[i][0]
        return tasks[i]
    undone = [i for i in acked if i in tasks and end(i)["state"] != "done"]
    check(not undone, "%d answered adds did not end done: %s" % (len(undone), [(end(i)["state"], end(i)["reason"]) for i in undone[:3]]))
    check(st["held"] == 0 and st["live"] == 0, "held %s live %s" % (st["held"], st["live"]))
# Below a replica's floor its log is a hole. Past the highest floor they must match.
floors = {r: floor(r) for r in reps}
logs = {r: open(homes[r] + "/ledger/log", "rb").read() for r in reps}
check(len({len(b) for b in logs.values()}) == 1 and len({b[max(floors.values()):] for b in logs.values()}) == 1,
      "the logs differ: %s bytes, floors %s" % ({r: len(b) for r, b in logs.items()}, floors))
if args.compact:
    check(all(f > 0 for f in floors.values()), "a replica never let go of any log: %s" % floors)
if args.grow:
    check(grown and shrunk and "d" in reps and len(reps) == 3, "the set was never changed both ways: %s" % reps)
terms = [int(n) for n in re.findall(rb'\{"kind":"term","term":(\d+)', logs[reps[0]])]
check(terms == sorted(set(terms)), "terms in the log are not strictly rising: %s" % terms)
elected = collections.defaultdict(set)
for r in reps:
    for n in re.findall(r"elected for term (\d+)", open(os.path.join(root, r + ".log")).read()):
        elected[int(n)].add(r)
both = {t: sorted(w) for t, w in elected.items() if len(w) > 1}
check(not both, "a term with two leaders: %s" % both)
stopping = True
with guard:
    for p in procs.values():
        if p.poll() is None:
            # Not SIGINT: a shell that started this script in the background
            # has set that to be ignored, and a replica standing by inherits it.
            p.send_signal(signal.SIGTERM)
for n in nodes:
    run(homes[n], "down")
time.sleep(2)
left = []
for _ in range(30):
    left = [p for p in subprocess.run(["pgrep", "-f", root + "/"], capture_output=True, text=True).stdout.split() if p != str(os.getpid())]
    # The bad link is this script's own: a call it is holding up is not a leak.
    left = [p for p in left if "link.py" not in cmdline(p)]
    if not left:
        break
    time.sleep(0.5)
check(not left, "processes left: %s" % [cmdline(p)[-80:] for p in left[:6]])
print("seed %d  %s" % (args.seed, dict(kills)))
print("adds answered %d  unsure %d  terms %s  log %d bytes  floors %s" % (len(acked), unsure, terms, len(logs[reps[0]]), sorted(floors.values())))
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("homes kept at " + root)
    sys.exit(1)
print("ok: every invariant held")
