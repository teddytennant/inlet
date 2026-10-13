#!/usr/bin/env python3
"""The ledger on three machines: this one, and two others reached by ssh.
The two far homes run no daemon; they hold the log and vote. Each far link
can be cut on its own, by DSCP mark, without touching other traffic.

    scripts/three-hosts.py path/to/inlet HOST_B HOST_C [--jump HOST_B] [--key FILE]

  writes with all three, then with one far host cut off (two of three)
  both cut off: the leader here must stop, and nobody may lead
  one link back: it leads again
  the leader killed outright and started again
  everything healed: three logs, three machines, one sha256
"""
import argparse, hashlib, json, os, re, signal, subprocess, sys, tempfile, threading, time

ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("b")
ap.add_argument("c")
ap.add_argument("--jump", default="", help="reach C through this host")
ap.add_argument("--key", default="")
ap.add_argument("--dir", default="/tmp/inlet-three")
args = ap.parse_args()
binary = os.path.abspath(args.inlet)
root = tempfile.mkdtemp(prefix="inlet-three-", dir=os.path.expanduser("~/.cache"))
BASE = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6", "-o", "ControlPath=none"]
if args.key:
    BASE += ["-i", args.key, "-o", "IdentitiesOnly=yes"]
MARK = {"b": "cs3", "c": "cs4"}
HOST = {"b": args.b, "c": args.c}


def ssh(name, marked):
    cmd = list(BASE)
    qos = ["-o", "IPQoS=" + MARK[name]] if marked else []
    if name == "c" and args.jump:
        cmd += ["-o", "ProxyCommand=ssh -o BatchMode=yes -o ControlPath=none %s -W %%h:%%p %s" % (" ".join(qos), args.jump)]
    else:
        cmd += qos
    return cmd + [HOST[name]]


def sh(*cmd, **kw):
    return subprocess.run(list(cmd), capture_output=True, text=True, **kw)


def rule(name):
    return ["OUTPUT", "-p", "tcp", "--dport", "22", "-m", "dscp", "--dscp-class", MARK[name].upper(), "-j", "DROP"]


def cut(name, on):
    sh("sudo", "-n", "iptables", "-I" if on else "-D", *rule(name))


far_bin, far_home = args.dir + "/inlet", args.dir + "/home"
homes = {n: os.path.join(root, n) for n in ("a", "n0")}
for home in homes.values():
    os.makedirs(home + "/ledger")
quote = lambda argv: ", ".join('"%s"' % a for a in argv)
COMMON = """
caps = { max_live = 8, max_depth = 2, max_tokens = 50000000, max_memory_mb = 8000, max_pids = 800 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 60, memory_mb = 128, pids = 32 }
"""
open(homes["n0"] + "/policy.lua", "w").write(COMMON + 'workers = { plain = { cmd = { "/bin/sh", "-c", "sleep 0.3; cat \\"$INLET_GOAL_FILE\\" > out" } } }\n')
open(homes["a"] + "/policy.lua", "w").write(COMMON + "node_grace = 30\nreplicas = {\n  a = { cmd = { %s } },\n  b = { cmd = { %s } },\n  c = { cmd = { %s } },\n}\nnodes = { n0 = { cmd = { %s }, slots = 6 } }\nworkers = { plain = { on = \"nodes\" } }\n" % (
    quote([binary, "--home", homes["a"]]),
    quote(ssh("b", True) + [far_bin, "--home", far_home]),
    quote(ssh("c", True) + [far_bin, "--home", far_home]),
    quote([binary, "--home", homes["n0"]])))
names = {}
for n in ("b", "c"):
    out = sh(*ssh(n, False), "rm -rf %s && mkdir -p %s/ledger && chmod 700 %s && hostname" % (far_home, far_home, args.dir))
    assert out.returncode == 0, out.stderr
    names[n] = out.stdout.strip()
    push = subprocess.run(ssh(n, False) + ["cat > %s && chmod 755 %s" % (far_bin, far_bin)], stdin=open(binary, "rb"))
    assert push.returncode == 0
print("a here, b on %s, c on %s" % (names["b"], names["c"]))


def run(*argv, home=None, limit=25):
    try:
        return subprocess.run([binary, "--home", home or homes["a"], *argv], capture_output=True, text=True, timeout=limit)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(argv, 124, "", "timeout")


proc, stopping = {}, False


def keep():
    log = open(os.path.join(root, "a.log"), "a")
    while not stopping:
        proc["a"] = subprocess.Popen([binary, "--home", homes["a"], "up", "-f", "--replica", "a"], stdout=log, stderr=log)
        proc["a"].wait()
        time.sleep(0.5)


def leading(limit=60):
    end = time.time() + limit
    while time.time() < end:
        out = run("status", "--json", limit=5)
        if out.returncode == 0 and out.stdout.strip():
            return json.loads(out.stdout)
        time.sleep(0.3)
    return None


acked = {}


def adds(first, n):
    took = []
    for goal in range(first, first + n):
        for _ in range(60):
            if leading(30):
                t0 = time.time()
                out = run("add", "-w", "plain", "-g", str(goal), "--verify", "test -s out")
                m = re.search(r"queued (\S+)", out.stdout)
                if out.returncode == 0 and m:
                    acked[m.group(1)] = goal
                    took.append(time.time() - t0)
                    break
            time.sleep(0.5)
    took.sort()
    return "%d adds, median %.2fs" % (len(took), took[len(took) // 2] if took else -1)


problems = []
for n in MARK:
    cut(n, False)
try:
    assert run("up", home=homes["n0"]).returncode == 0
    threading.Thread(target=keep, daemon=True).start()
    assert leading(), "never elected"
    print("elected with the far votes. three of three: %s" % adds(1, 25))
    cut("b", True)
    print("b cut off, two of three: %s" % adds(26, 20))
    cut("c", True)
    out = run("add", "-w", "plain", "-g", "0", "--verify", "test -s out", limit=40)
    time.sleep(22)
    alone = leading(limit=20)
    print("b and c cut off: the add was %s, and %s" % ("refused" if out.returncode else "ANSWERED", "nobody leads" if not alone else "IT STILL LEADS"))
    if out.returncode == 0:
        problems.append("an add was answered with one replica of three")
    if alone:
        problems.append("one replica of three led")
    cut("b", False)
    back = leading(limit=90)
    print("b back: %s. %s" % ("leads again" if back else "NOBODY LEADS", adds(46, 20)))
    proc["a"].send_signal(signal.SIGKILL)
    print("leader killed and started again, c still cut off: %s" % adds(66, 15))
    cut("c", False)
    print("all healed: %s" % adds(81, 20))
    sums = {}
    end = time.time() + 180
    while time.time() < end:
        st = leading()
        if st and st["live"] == 0 and st["queued"] == 0:
            sums = {"a": hashlib.sha256(open(homes["a"] + "/ledger/log", "rb").read()).hexdigest()}
            for n in ("b", "c"):
                sums[n] = sh(*ssh(n, False), "sha256sum %s/ledger/log" % far_home).stdout.split(" ")[0]
            if len(set(sums.values())) == 1:
                break
        time.sleep(2)
    else:
        problems.append("the three logs never became the same: %s" % sums)
    st = leading()
    tasks = {t["id"]: t for t in st["tasks"]} if st else {}
    lost = [i for i in acked if i not in tasks]
    if lost:
        problems.append("%d answered adds are not in the ledger" % len(lost))
    done = sum(1 for i in acked if i in tasks and tasks[i]["state"] == "done")
    if done != len(acked):
        problems.append("%d done of %d answered" % (done, len(acked)))
    print("%d answered adds, %d done. log %d bytes, sha256 %s on %s, %s and here" % (
        len(acked), done, os.path.getsize(homes["a"] + "/ledger/log"), sums.get("a", "")[:16], names["b"], names["c"]))
finally:
    for n in MARK:
        cut(n, False)
    stopping = True
    if proc.get("a") and proc["a"].poll() is None:
        proc["a"].send_signal(signal.SIGTERM)
    run("down", home=homes["n0"])
    for n in ("b", "c"):
        sh(*ssh(n, False), "rm -rf %s" % args.dir)
for n in MARK:
    if sh("sudo", "-n", "iptables", "-C", *rule(n)).returncode == 0:
        problems.append("a drop rule was left in place")
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("homes kept at " + root)
    sys.exit(1)
print("ok: every check held")
