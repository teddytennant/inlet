#!/usr/bin/env python3
"""Nodes behind a real network. Each node gets its own network namespace,
its own address, and its own sshd, and the leader reaches it the way it
would reach another machine: ssh over TCP. The link is then made slow and
lossy with netem, cut for longer than the leader will wait, healed, and cut
again for less than a lease.

Needs root for the namespaces (it calls sudo) and is not run in CI.

    scripts/netns-nodes.py path/to/inlet [--tasks 200]

Checked: the swarm finishes, a node cut off lets go of its work on its own,
a short cut costs nothing, the purses add up, and nothing is left behind.
"""
import argparse, collections, json, os, re, signal, subprocess, sys, tempfile, time

ap = argparse.ArgumentParser()
ap.add_argument("inlet")
ap.add_argument("--tasks", type=int, default=200)
ap.add_argument("--home", default="")
args = ap.parse_args()
binary = os.path.abspath(args.inlet)
root = args.home or tempfile.mkdtemp(prefix="inlet-netns-", dir=os.path.expanduser("~/.cache"))
os.makedirs(root, exist_ok=True)
CAP = 50_000_000
GRACE = 20
NODES = [1, 2]
SSHD = subprocess.run(["which", "sshd"], capture_output=True, text=True).stdout.strip()
me = os.environ.get("USER", "nixos")


def sh(*cmd, check=True, **kw):
    return subprocess.run(list(cmd), check=check, capture_output=True, text=True, **kw)


def sudo(*cmd, check=True):
    return sh("sudo", "-n", *cmd, check=check)


def addr(i):
    return "10.250.%d.2" % i


def ssh_prefix(i):
    return ["ssh", "-p", "2222", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
            "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR", addr(i)]


def teardown():
    for i in NODES:
        pidfile = os.path.join(root, "sshd-%d.pid" % i)
        if os.path.exists(pidfile):
            sudo("kill", open(pidfile).read().strip(), check=False)
        sudo("ip", "netns", "pids", "inl%d" % i, check=False)
        pids = sudo("ip", "netns", "pids", "inl%d" % i, check=False).stdout.split()
        if pids:
            sudo("kill", "-9", *pids, check=False)
        sudo("ip", "link", "del", "inlh%d" % i, check=False)
        sudo("ip", "netns", "del", "inl%d" % i, check=False)


teardown()
sh("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", os.path.join(root, "hostkey"), check=False)
homes = {"leader": os.path.join(root, "leader")}
for i in NODES:
    ns, host, peer = "inl%d" % i, "inlh%d" % i, "inln%d" % i
    sudo("ip", "netns", "add", ns)
    sudo("ip", "link", "add", host, "type", "veth", "peer", "name", peer)
    sudo("ip", "link", "set", peer, "netns", ns)
    sudo("ip", "addr", "add", "10.250.%d.1/24" % i, "dev", host)
    sudo("ip", "link", "set", host, "up")
    sudo("ip", "netns", "exec", ns, "ip", "addr", "add", addr(i) + "/24", "dev", peer)
    sudo("ip", "netns", "exec", ns, "ip", "link", "set", peer, "up")
    sudo("ip", "netns", "exec", ns, "ip", "link", "set", "lo", "up")
    cfg = os.path.join(root, "sshd-%d.conf" % i)
    open(cfg, "w").write("""Port 2222
ListenAddress %s
HostKey %s
PidFile %s
AuthorizedKeysFile /home/%s/.ssh/authorized_keys /etc/ssh/authorized_keys.d/%%u
StrictModes no
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
""" % (addr(i), os.path.join(root, "hostkey"), os.path.join(root, "sshd-%d.pid" % i), me))
    sudo("ip", "netns", "exec", ns, SSHD, "-f", cfg, "-E", os.path.join(root, "sshd-%d.log" % i))
    homes["n%d" % i] = os.path.join(root, "n%d" % i)

COMMON = """
caps = { max_live = 12, max_depth = 2, max_tokens = %d, max_memory_mb = 30000, max_pids = 4000 }
isolator = "rlimit"
retry = { max = 3, backoff = "1s", max_backoff = "2s" }
default_budget = { tokens = 1000, seconds = 120, memory_mb = 256, pids = 32 }
""" % CAP
for name, home in homes.items():
    os.makedirs(home + "/ledger", exist_ok=True)
    os.makedirs(home + "/run", exist_ok=True)
    if name == "leader":
        nodes = "".join('  n%d = { cmd = { %s, "%s", "--home", "%s" }, slots = 6 },\n' % (
            i, ", ".join('"%s"' % a for a in ssh_prefix(i)), binary, homes["n%d" % i]) for i in NODES)
        open(home + "/policy.lua", "w").write(COMMON + "node_grace = %d\nnodes = {\n%s}\nworkers = { plain = { on = \"nodes\" } }\n" % (GRACE, nodes))
    else:
        open(home + "/policy.lua", "w").write(COMMON + """
workers = {
  plain = { cmd = { "/bin/sh", "-c", "n=$(cat \\"$INLET_GOAL_FILE\\"); sleep $((2 + n % 16)); echo $n > out" } },
}
""")


def remote(i, *argv):
    return sh(*ssh_prefix(i), binary, "--home", homes["n%d" % i], *argv, check=False)


def local(*argv):
    return sh(binary, "--home", homes["leader"], *argv, check=False)


def status(home_cmd):
    for _ in range(60):
        out = home_cmd("status", "--json")
        if out.returncode == 0 and out.stdout.strip():
            return json.loads(out.stdout)
        time.sleep(0.5)
    raise RuntimeError("no status")


problems = []
try:
    deadline = time.time() + 20
    for i in NODES:
        while remote(i, "up").returncode != 0:
            if time.time() > deadline:
                raise RuntimeError("node %d would not start over ssh: %s" % (i, remote(i, "up").stderr[-300:]))
            time.sleep(0.5)
    # The daemon runs inside the namespace: it is not reachable any other way.
    inside = sudo("ip", "netns", "pids", "inl1").stdout.split()
    assert open(homes["n1"] + "/run/inlet.pid").read().strip() in inside, "node 1's daemon is not in its namespace"
    assert local("up").returncode == 0
    for _ in range(40):
        if all(n["ready"] for n in status(local)["nodes"]):
            break
        time.sleep(0.5)
    # A bad link, for real: every packet late, one in ten lost.
    netem = True
    for i in NODES:
        if sudo("tc", "qdisc", "add", "dev", "inlh%d" % i, "root", "netem", "delay", "60ms", "30ms", "loss", "10%", check=False).returncode != 0:
            netem = False
    manifest = os.path.join(root, "m.json")
    json.dump({"kind": "swarm", "name": "plain", "replicas": 10, "completions": args.tasks,
               "template": {"worker": "plain", "goal": "{index}", "verify": "test -s out", "retries": 3}}, open(manifest, "w"))
    assert local("apply", "-f", manifest).returncode == 0
    started = time.time()

    time.sleep(12)
    print("t+%3d  cutting node 1 off for %ds, past the leader's patience" % (time.time() - started, GRACE + 13))
    sudo("ip", "link", "set", "inlh1", "down")
    time.sleep(GRACE // 2 + 4)
    # Nobody can reach it. Look from inside its namespace: it has let go.
    out = sudo("ip", "netns", "exec", "inl1", "sudo", "-u", me, binary, "--home", homes["n1"], "status", "--json", check=False)
    cut = json.loads(out.stdout) if out.returncode == 0 else {"tasks": [], "live": -1}
    fenced = sum(1 for t in cut["tasks"] if t["reason"] == "fenced")
    print("t+%3d  node 1, seen from inside: live %s, fenced %d" % (time.time() - started, cut["live"], fenced))
    # Whatever was still running when the lease ran out has been stopped.
    if cut["live"] != 0 or fenced == 0:
        problems.append("node 1 did not let go of its work while cut off: live %s, fenced %d" % (cut["live"], fenced))
    time.sleep(GRACE // 2 + 9)
    st = status(local)
    print("t+%3d  leader sees node 1 ready=%s" % (time.time() - started, st["nodes"][0]["ready"]))
    if st["nodes"][0]["ready"]:
        problems.append("the leader never gave node 1 up")
    sudo("ip", "link", "set", "inlh1", "up")
    print("t+%3d  healed" % (time.time() - started))

    time.sleep(10)
    print("t+%3d  kill -9 the leader" % (time.time() - started))
    os.kill(int(open(homes["leader"] + "/run/inlet.pid").read()), signal.SIGKILL)
    time.sleep(1)
    assert local("up").returncode == 0

    time.sleep(8)
    moved_before = sum(1 for t in status(local)["tasks"] if t["reason"] == "node")
    print("t+%3d  cutting node 2 off for 3s, inside a lease" % (time.time() - started))
    sudo("ip", "link", "set", "inlh2", "down")
    time.sleep(3)
    sudo("ip", "link", "set", "inlh2", "up")
    time.sleep(6)
    moved_after = sum(1 for t in status(local)["tasks"] if t["reason"] == "node")
    if moved_after != moved_before:
        problems.append("a 3s cut moved %d tasks" % (moved_after - moved_before))

    deadline = time.time() + 600
    while time.time() < deadline:
        st = status(local)
        if st["swarms"][0]["phase"] != "active" and st["live"] == 0 and st["queued"] == 0:
            break
        time.sleep(2)
    for i in NODES:
        end = time.time() + 60
        while time.time() < end and status(lambda *a: remote(i, *a))["live"]:
            time.sleep(1)

    st = status(local)
    swarm = st["swarms"][0]
    if swarm["phase"] != "done" or swarm["done"] != args.tasks:
        problems.append("swarm %s, %s of %s" % (swarm["phase"], swarm["done"], args.tasks))
    if st["held"] != 0:
        problems.append("leader held %s" % st["held"])
    moved = sum(1 for t in st["tasks"] if t["reason"] == "node")
    if moved == 0:
        problems.append("nothing was moved off the node that was cut off")
    twice = collections.Counter()
    for i in NODES:
        ns = status(lambda *a: remote(i, *a))
        lost = sum(t["budget_tokens"] for t in ns["tasks"] if t["reason"] == "crash")
        if ns["held"] != 0 or ns["available"] + lost != CAP:
            problems.append("node %d purse: held %s, available %s, crashed %s" % (i, ns["held"], ns["available"], lost))
        for t in ns["tasks"]:
            if t["state"] == "done":
                twice[t["goal"]] += 1
    reported = CAP - st["available"]
    print("%d tasks in %ds over %s  moved off the cut node %d  fenced there %d  goals finished twice %d  leader charged %d" % (
        args.tasks, time.time() - started, "netem delay 60ms loss 10%" if netem else "a clean link (no netem)",
        moved, fenced, sum(1 for n in twice.values() if n > 1), reported))
finally:
    local("down")
    for i in NODES:
        remote(i, "down")
    time.sleep(1)
    teardown()

left = sh("ip", "netns", "list", check=False).stdout
if "inl1" in left or "inl2" in left:
    problems.append("namespaces left behind")
if problems:
    print("FAILED")
    for p in problems:
        print("  " + p)
    print("homes kept at " + root)
    sys.exit(1)
print("ok: every check held")
