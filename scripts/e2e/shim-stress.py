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
a voter for eight seconds), all one voter at a time so a majority is
always up; `majority` (kill the leader and one other voter at once,
restart both five seconds later), which takes the quorum away; `all`
(kill every voter at once and restart them, then an interval later kill
and restart whichever leads); and `pause-majority` (SIGSTOP both of the
leader's followers for ten seconds while clients go on submitting to the
leader, kill the leader, resume the followers with what queued for them,
and restart the leader five seconds later): the three-voter form of a
five-node Jepsen run's window before a divergence stop.
`--faults 2,1` replaces the random choice with a fixed sequence of voters
to kill and restart, one per interval, and then no more faults: a run
that went wrong can be replayed on purpose.

The checks are the ones a list-append history can be held to without a
full Elle run:

* every read of a key is a prefix of the longest read of it, and of the
  final read when there is one;
* no element appears twice in a list;
* no append reported `fail` is ever read;
* an append reported `ok` is in every read that began after it ended;
* a read that began after another read ended is not shorter;
* no two ok transactions read the same value of a key and then both
  append to it (a lost update).

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
    p.add_argument("--fault", choices=["none", "leader", "random", "pause", "majority", "all", "pause-majority",
                                       "follower-out"], default="leader")
    p.add_argument("--out", type=float, default=30,
                   help="with --fault follower-out: seconds a follower stays down")
    p.add_argument("--capacity", type=int,
                   help="each voter's command table capacity (limits.command_table_capacity)")
    p.add_argument("--clients", type=int, default=6)
    p.add_argument("--rate", type=float,
                   help="operations a second across all clients (default: as fast as answered)")
    p.add_argument("--keys", type=int, default=4)
    p.add_argument("--interval", type=float, default=20, help="seconds between faults")
    p.add_argument("--recovery", type=float, default=45, help="seconds to wait before the final read")
    p.add_argument("--faults", type=lambda v: [int(x) for x in v.split(",")],
                   help="kill and restart these voters in order (e.g. 2,1), then stop faulting; "
                        "only with --fault random")
    return p.parse_args()


A = parse()
if A.faults is not None and (A.fault != "random" or not all(1 <= n <= 3 for n in A.faults)):
    sys.exit("--faults takes voters 1 to 3, with --fault random")
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
        if A.capacity is not None:
            toml = os.path.join(RUN, f"n{n}", "coordd.toml")
            with open(toml) as f:
                text = f.read()
            line = f"command_table_capacity = {A.capacity}\n"
            if "\n[limits]\n" in text:
                text = text.replace("\n[limits]\n", "\n[limits]\n" + line, 1)
            else:
                # A [limits] table names every limit; these are the defaults.
                text += ("\n[limits]\nmax_request_bytes = 2097152\nmax_response_bytes = 8388608\n"
                         "max_outstanding_per_session = 256\nmax_live_subscriptions = 4096\n" + line)
            with open(toml, "w") as f:
                f.write(text)
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
        if A.rate:
            # Each client takes its share of the rate; an operation that
            # took longer than its slot is not made up for.
            stop.wait(A.clients / A.rate)
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
    # Each log keeps every ballot its voter ever led, so the leader is
    # the voter that announced the highest one. Ballots order by number,
    # then by leader, and voter n is provisioned as replica [n; 16].
    highest = (-1, 1)
    for n in (1, 2, 3):
        for line in said(n).splitlines():
            if line.startswith("this voter leads ballot"):
                highest = max(highest, (int(line.split()[-1]), n))
    return highest[1]


def nemesis():
    stop.wait(5)
    while not stop.is_set() and A.fault != "none":
        if A.fault == "all":
            # Every voter at once, then, an interval later, whichever
            # leads: the Jepsen run where a leader came back behind its
            # followers, led the next ballot and was killed in it.
            for m in (1, 2, 3):
                if daemons[m].poll() is None:
                    daemons[m].kill()
                    daemons[m].wait()
            log("killed every voter")
            stop.wait(5)
            for m in (1, 2, 3):
                start(m)
            log("restarted every voter")
            stop.wait(A.interval)
            if stop.is_set():
                break
            n = leader()
            daemons[n].kill()
            daemons[n].wait()
            log(f"killed voter {n}, the leader")
            stop.wait(5)
            start(n)
            log(f"restarted voter {n}")
            stop.wait(A.interval)
            continue
        if A.fault == "pause-majority":
            # The leader runs alone with both followers paused, taking
            # submissions it cannot commit; it is killed, and the pause
            # lifts with the followers' backlog.
            first = leader()
            followers = [m for m in (1, 2, 3) if m != first]
            paused = [m for m in followers if daemons[m].poll() is None]
            for m in paused:
                daemons[m].send_signal(signal.SIGSTOP)
            log(f"paused voters {paused}; voter {first} leads alone")
            stop.wait(10)
            if daemons[first].poll() is None:
                daemons[first].kill()
                daemons[first].wait()
            log(f"killed voter {first}, the leader")
            for m in paused:
                daemons[m].send_signal(signal.SIGCONT)
            log(f"resumed voters {paused}")
            stop.wait(5)
            start(first)
            log(f"restarted voter {first}")
            stop.wait(A.interval)
            continue
        if A.fault == "majority":
            # Two of three at once, the leader among them: the domain has
            # no quorum until they are back.
            first = leader()
            pair = [first, random.choice([m for m in (1, 2, 3) if m != first])]
            for m in pair:
                if daemons[m].poll() is None:
                    daemons[m].kill()
                    daemons[m].wait()
            log(f"killed voters {pair}")
            stop.wait(5)
            for m in pair:
                start(m)
            log(f"restarted voters {pair}")
            stop.wait(A.interval)
            continue
        if A.fault == "follower-out":
            # #113's shape: one follower down for --out seconds while
            # the others take the load, then back. Reported: how long after
            # its restart it first serves a read, and whether it refused
            # work as Backpressure after that. Once, then no more faults.
            n = next(m for m in (1, 2, 3) if m != leader())
            daemons[n].kill()
            daemons[n].wait()
            with lock:
                before = counter[0]
            log(f"took voter {n} out")
            stop.wait(A.out)
            with lock:
                during = counter[0] - before
            back = time.time()
            start(n)
            log(f"brought voter {n} back after {A.out:.0f}s; about {during} appends went on without it")
            served = probe(n, back)
            log(f"voter {n} served a read {served:.1f}s after it came back" if served is not None
                else f"voter {n} served no read within 60s of coming back")
            with lock:
                history.append(["follower-out", n, during, served])
            break
        if A.faults is not None:
            if not A.faults:
                break
            n = A.faults.pop(0)
        else:
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


def probe(n, since):
    """Seconds after `since` at which voter n first served a read, or None."""
    attempt = 0
    while time.time() - since < 60:
        attempt += 1
        p, ready = open_shim(n, 50000 + attempt)
        if p is None:
            log(f"probe of voter {n}: no session at +{time.time() - since:.1f}s: {ready.get('error')}")
            time.sleep(0.5)
            continue
        p.stdin.write(json.dumps({"f": "txn", "value": [["r", 0, None]]}) + "\n")
        p.stdin.flush()
        answer = json.loads(p.stdout.readline() or '{"type": "fail"}')
        p.stdin.close()
        p.wait()
        if answer["type"] == "ok":
            return time.time() - since
        log(f"probe of voter {n}: {answer.get('type')} at +{time.time() - since:.1f}s: {answer.get('error')}")
        time.sleep(0.5)
    return None


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
    # Two ok transactions that read the same value of a key and then both
    # appended to it: one append was lost (or the guard did not hold).
    seen = {}
    for _, _, mops, answer, _, _ in ops:
        if answer["type"] != "ok":
            continue
        read = {}
        for m in answer["value"]:
            if m[0] == "r" and m[1] not in read:
                read[m[1]] = tuple(m[2] or [])
            elif m[0] == "append" and m[1] in read:
                seen.setdefault((m[1], read[m[1]]), []).append(m[2])
                del read[m[1]]
    problems += [f"key {k}: appends {vs} all read {list(r)} first (lost update)"
                 for (k, r), vs in seen.items() if len(vs) > 1]
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
    # ok operations per 10 s, so a domain that stops serving shows where.
    if ops:
        t0 = min(op[4] for op in ops)
        windows = {}
        for op in ops:
            if op[3]["type"] == "ok":
                w = int((op[4] - t0) // 10) * 10
                windows[w] = windows.get(w, 0) + 1
        last = int((max(op[4] for op in ops) - t0) // 10) * 10
        log("ok per 10 s: " + " ".join(f"{w}s:{windows.get(w, 0)}" for w in range(0, last + 1, 10)))
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
