#!/usr/bin/env python3
"""Drive a local domain through `coord-jepsen` under faults, and check it.

This is the shim's smoke test without Jepsen: it provisions a three-voter
domain on loopback, runs several `coord-jepsen` clients doing Elle-style
list-append transactions on a few contended keys, kills (or pauses) a
voter every so often and brings it back, and checks the history it
recorded. See docs/operations/jepsen.md.

    scripts/e2e/shim-stress.py RUN_DIR [--seconds 120] [--fault leader]

Faults: `none`, `leader` (kill the voter that last said it leads, then
restart it), `random` (kill any voter, then restart it), `pause` (SIGSTOP
a voter for eight seconds). One voter at a time, so a majority is always
up.

The checks are the ones a list-append history can be held to without a
full Elle run:

* every read of a key is a prefix of the longest read of it, and of the
  final read when there is one;
* no element appears twice in a list;
* no append reported `fail` is ever read;
* an append reported `ok` is in every read that began after it ended;
* a read that began after another read ended is not shorter.

Exit status: 0 clean, 1 an anomaly, 2 the domain did not serve the final
read (a liveness failure; the history checks still ran). The run
directory keeps each voter's log and `history.json`.
"""

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def parse():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("run", help="run directory (created; must not exist)")
    p.add_argument("--bin", default=os.path.join(ROOT, "target", "debug"),
                   help="directory holding coordd, coord-harness and coord-jepsen")
    p.add_argument("--seconds", type=float, default=120)
    p.add_argument("--fault", choices=["none", "leader", "random", "pause"], default="leader")
    p.add_argument("--clients", type=int, default=6)
    p.add_argument("--keys", type=int, default=4)
    p.add_argument("--interval", type=float, default=20, help="seconds between faults")
    p.add_argument("--recovery", type=float, default=45, help="seconds to wait before the final read")
    return p.parse_args()


A = parse()
RUN = os.path.abspath(A.run)
COORDD = os.path.join(A.bin, "coordd")
SHIM = os.path.join(A.bin, "coord-jepsen")
PREFIX = f"stress-{int(time.time())}/"

history = []
lock = threading.Lock()
counter = [0]
stop = threading.Event()
daemons = {}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def start(n):
    out = open(os.path.join(RUN, f"n{n}", "coordd.log"), "a")
    daemons[n] = subprocess.Popen([COORDD, "--config", os.path.join(RUN, f"n{n}", "coordd.toml")],
                                  stdout=out, stderr=out, stdin=subprocess.DEVNULL)


def said(n):
    try:
        with open(os.path.join(RUN, f"n{n}", "coordd.log")) as f:
            return f.read()
    except FileNotFoundError:
        return ""


def last_count(text, prefix):
    counts = [line[len(prefix):].split(" ")[0] for line in text.splitlines() if line.startswith(prefix)]
    return int(counts[-1]) if counts else None


def up():
    subprocess.run([os.path.join(A.bin, "coord-harness"), "provision", "--dir", RUN],
                   check=True, stdout=subprocess.DEVNULL)
    for n in (1, 2, 3):
        subprocess.run([COORDD, "--config", os.path.join(RUN, f"n{n}", "coordd.toml"), "init"],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        start(n)
    deadline = time.time() + 90
    while time.time() < deadline:
        if all(last_count(said(n), "peers connected=") == 2
               and last_count(said(n), "voters submittable=") == 2 for n in (1, 2, 3)):
            return
        time.sleep(0.5)
    sys.exit("the domain never meshed")


def open_shim(voter, instance):
    p = subprocess.Popen([SHIM, "--dir", RUN, "--voter", str(voter), "--instance", str(instance),
                          "--attempt-ms", "1000", "--budget-ms", "5000", "--connect-ms", "3000",
                          "--prefix", PREFIX],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                         text=True)
    ready = json.loads(p.stdout.readline() or '{"ready": false, "error": "no ready line"}')
    if not ready.get("ready"):
        p.kill()
        return None, ready
    return p, ready


def client(i):
    voter, instance, p = i % 3 + 1, i, None
    while not stop.is_set():
        if p is None:
            instance += 100
            p, ready = open_shim(voter, instance)
            if p is None:
                with lock:
                    history.append(["open-fail", i, ready.get("error")])
                time.sleep(0.5)
                continue
        mops = []
        for _ in range(random.randint(1, 3)):
            k = random.randrange(A.keys)
            if random.random() < 0.5:
                mops.append(["r", k, None])
            else:
                with lock:
                    counter[0] += 1
                    mops.append(["append", k, counter[0]])
        t0 = time.time()
        try:
            p.stdin.write(json.dumps({"f": "txn", "value": mops}) + "\n")
            p.stdin.flush()
            answer = json.loads(p.stdout.readline())
        except Exception as e:  # the shim died
            answer = {"type": "info", "error": f"shim died: {e}"}
        with lock:
            history.append(["op", i, mops, answer, t0, time.time()])
        if answer["type"] == "info":
            # As Jepsen does: an indeterminate operation ends the process.
            p.kill()
            p = None
    if p:
        p.stdin.close()
        p.wait()


def leader():
    who = 1
    for n in (1, 2, 3):
        for line in said(n).splitlines():
            if line.startswith("this voter leads ballot"):
                who = n
    return who


def nemesis():
    stop.wait(5)
    while not stop.is_set() and A.fault != "none":
        n = leader() if A.fault == "leader" else random.randint(1, 3)
        if daemons[n].poll() is not None:
            log(f"voter {n} is not running (exit {daemons[n].returncode}); restarting it")
            start(n)
        elif A.fault == "pause":
            daemons[n].send_signal(signal.SIGSTOP)
            log(f"paused voter {n}")
            stop.wait(8)
            daemons[n].send_signal(signal.SIGCONT)
            log(f"resumed voter {n}")
        else:
            daemons[n].kill()
            daemons[n].wait()
            log(f"killed voter {n}")
            stop.wait(5)
            start(n)
            log(f"restarted voter {n}")
        stop.wait(A.interval)


def final_read():
    for attempt in range(12):
        p, _ = open_shim(attempt % 3 + 1, 60000 + attempt)
        if p is None:
            time.sleep(3)
            continue
        p.stdin.write(json.dumps({"f": "txn", "value": [["r", k, None] for k in range(A.keys)]}) + "\n")
        p.stdin.flush()
        answer = json.loads(p.stdout.readline() or '{"type": "fail"}')
        p.stdin.close()
        p.wait()
        if answer["type"] == "ok":
            return {m[1]: (m[2] or []) for m in answer["value"]}
        time.sleep(3)
    return None


def check(ops, final):
    problems, failed, reads = [], set(), {}
    for _, _, mops, answer, t0, t1 in ops:
        for m in mops:
            if m[0] == "append" and answer["type"] == "fail":
                failed.add(m[2])
        if answer["type"] == "ok":
            for m in answer["value"]:
                if m[0] == "r":
                    reads.setdefault(m[1], []).append((m[2] or [], t0, t1))
    # The final read is a read like the others, begun after everything
    # else ended: a value an earlier read saw must still be in it.
    if final is not None:
        for k, lst in final.items():
            reads.setdefault(k, []).append((lst, float("inf"), float("inf")))
    for k, rs in reads.items():
        longest = max((r for r, _, _ in rs), key=len)
        if final is not None:
            longest = final[k] if len(final[k]) >= len(longest) else longest
        for r, s, e in rs:
            if r != longest[:len(r)]:
                problems.append(f"key {k}: read {r} is not a prefix of {longest}")
            if len(set(r)) != len(r):
                problems.append(f"key {k}: duplicate element in {r}")
            problems += [f"key {k}: failed append {v} was read" for v in r if v in failed]
            problems += [f"key {k}: read {r2} began after read {r} ended and is shorter"
                         for r2, s2, _ in rs if s2 > e and len(r2) < len(r)]
    for _, _, mops, answer, _, t1 in ops:
        if answer["type"] != "ok":
            continue
        for m in (m for m in mops if m[0] == "append"):
            for r, s, _ in reads.get(m[1], []):
                if s > t1 and m[2] not in r:
                    problems.append(f"key {m[1]}: ok append {m[2]} missing from a later read")
            if final is not None and final[m[1]].count(m[2]) != 1:
                problems.append(f"key {m[1]}: ok append {m[2]} appears "
                                f"{final[m[1]].count(m[2])} times in the final read")
    return problems


def main():
    up()
    log(f"domain up in {RUN}; {A.clients} clients, fault {A.fault}, {A.seconds:.0f}s")
    threads = [threading.Thread(target=client, args=(i,)) for i in range(A.clients)]
    threads.append(threading.Thread(target=nemesis))
    for t in threads:
        t.start()
    stop.wait(A.seconds)
    stop.set()
    for t in threads:
        t.join()
    for n in (1, 2, 3):
        if daemons[n].poll() is not None:
            log(f"voter {n} is not running (exit {daemons[n].returncode}); restarting it")
            start(n)
    log(f"healed; waiting {A.recovery:.0f}s before the final read")
    time.sleep(A.recovery)
    final = final_read()
    for n, d in daemons.items():
        d.kill()
        d.wait()
    ops = [h for h in history if h[0] == "op"]
    with open(os.path.join(RUN, "history.json"), "w") as f:
        json.dump({"history": history, "final": final}, f)
    kinds = {}
    for op in ops:
        kinds[op[3]["type"]] = kinds.get(op[3]["type"], 0) + 1
    reasons = {}
    for op in ops:
        if op[3]["type"] != "ok":
            reasons[op[3].get("error")] = reasons.get(op[3].get("error"), 0) + 1
    log(f"{len(ops)} operations {kinds}; not ok: {reasons}")
    for n in (1, 2, 3):
        panics = [l for l in said(n).splitlines() if "panicked at" in l]
        if panics:
            log(f"voter {n} panicked {len(panics)} time(s): {panics[0]}")
    problems = check(ops, final)
    for p in problems[:20]:
        log(f"ANOMALY {p}")
    if problems:
        sys.exit(1)
    if final is None:
        log("no anomaly in the history, but the domain did not serve the final read")
        sys.exit(2)
    log(f"no anomaly; final list lengths {[len(final[k]) for k in sorted(final)]}")


if __name__ == "__main__":
    main()
