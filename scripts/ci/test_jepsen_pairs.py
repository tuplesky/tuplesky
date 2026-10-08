"""Tests for jepsen_pairs.py: python3 -m unittest test_jepsen_pairs (from scripts/ci)."""

import json
import os
import tempfile
import unittest

import jepsen_pairs as jp


def op(ts, worker, kind, f="read"):
    return f"2026-09-27 {ts}{{GMT}}\tINFO\t[jepsen worker {worker}] jepsen.print: {worker}\t:{kind}\t:{f}\tnil\n"


def metrics(served, executed, loop_s, process_s, streams=None):
    cost = {
        "executed": executed,
        "busy": {"secs": 1, "nanos": 0},
        "uptime": {"secs": 10, "nanos": 0},
        "recent": {"Unavailable": "NotInstrumented"},
        "reads": {"served": served, "refused": 0, "rounds": served, "confirmed": served, "waited_ms": 0},
        "cpu": {"Observed": {"domain": {"secs": 0, "nanos": int(loop_s * 1e9)}, "process": {"secs": process_s, "nanos": 0}}},
    }
    if streams is not None:
        cost.update(
            established_fast=100,
            established_slow=900,
            fast_path={"missed_path": 810, "missed_deps": 90},
            traffic={"Observed": {"sent_frames": 10000, "sent_streams": streams, "sent_lost": 64 if streams < 5000 else 2,
                                  "sent_lost_streams": 1 if streams < 5000 else 2, "datagrams_sent": 5000}},
        )
    return "metrics " + json.dumps({"stages": [], "cost": {"Observed": cost}}) + "\n"


def store(root, name, read_ms, leader_loop_s, process_s):
    """A run of four reads over 10 s (0.4 `ok`/s), each taking read_ms,
    with a leader n1 and a follower n2 that each executed 1000 commands."""
    path = os.path.join(root, name)
    os.makedirs(os.path.join(path, "n1"))
    os.makedirs(os.path.join(path, "n2"))
    lines = []
    for i, second in enumerate((0, 3, 6, 10)):
        lines.append(op(f"10:00:{second:02d},000", i, "invoke"))
        lines.append(op(f"10:00:{second:02d},{read_ms:03d}", i, "ok"))
    with open(os.path.join(path, "jepsen.log"), "w") as f:
        f.writelines(lines)
    with open(os.path.join(path, "n1", "coordd.log"), "w") as f:
        f.write(metrics(4, 1000, leader_loop_s, process_s))
    with open(os.path.join(path, "n2", "coordd.log"), "w") as f:
        f.write(metrics(0, 1000, 0.4, process_s))
    return path


class PairTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = self.tmp.name
        # Pair 1 ran base then head, pair 2 head then base.
        self.runs = [
            jp.read_run("base", store(root, "1", 50, 0.6, 2)),
            jp.read_run("head", store(root, "2", 40, 0.5, 2)),
            jp.read_run("head", store(root, "3", 30, 0.5, 1)),
            jp.read_run("base", store(root, "4", 60, 0.7, 2)),
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_run_reads_its_measures(self):
        run = self.runs[0]
        # Four `ok` from the first invocation to the last completion, 10.05 s.
        self.assertAlmostEqual(run.ok_per_s, 4 / 10.05)
        self.assertEqual(run.read_p99, 50)
        # Both voters' process CPU (2 s each) over the 4 operations.
        self.assertAlmostEqual(run.voters_cpu_per_op, 1000.0)
        self.assertAlmostEqual(run.leader_loop, 0.6)
        self.assertAlmostEqual(run.followers_loop, 0.4)
        self.assertAlmostEqual(run.leader_excess, 0.2)
        self.assertIsNone(run.servers_cpu_per_op)

    def test_pairs_take_either_order(self):
        paired = jp.pairs(self.runs)
        self.assertEqual([(b.store[-1], h.store[-1]) for b, h in paired], [("1", "2"), ("4", "3")])

    def test_the_table_gives_each_pair_and_the_mean(self):
        text = jp.render(self.runs, "Paired")
        self.assertIn("| 1 | base | 0.4 | 50 | 1000.00 | - | - | - | 0.600 | 0.400 | 0.200 |", text)
        # Pair 1: read p99 40 against 50; pair 2: 30 against 60.
        self.assertIn("| 1 | +0.0 (+0.1%) | -10 (-20.0%) | +0.00 (+0.0%) | - | - | - | -0.100 (-16.7%) | +0.000 (+0.0%) | -0.100 (-50.0%) |", text)
        self.assertIn("| 2 | +0.0 (+0.3%) | -30 (-50.0%) | -500.00 (-50.0%) | - | - | - | -0.200 (-28.6%) |", text)
        self.assertIn("| mean (smallest to largest) of 2 | +0.0 (0.0 to 0.0) | -20 (-30 to -10) |", text)

    def test_profiles_are_costed_per_command_by_build(self):
        profile = (
            "leader thread 1, 0.30 of a core over the 5 s before, sampled at 999 Hz; Samples: 5K\n"
            "by object: coordd 60.00%, libc.so.6 25.00%\n"
            "    {resend:>6}%  coordd  [.] coord_consensus::leader::Leader::resend_unvoted\n"
            "     5.00%  libc.so.6  [.] malloc\n"
        )
        for run in self.runs:
            with open(os.path.join(run.store, "leader-profile.txt"), "w") as f:
                f.write(profile.format(resend="20.00" if run.label == "base" else "10.00"))
        runs = [jp.read_run(r.label, r.store) for r in self.runs]
        self.assertEqual(runs[0].profile[("coordd", "coord_consensus::leader::Leader::resend_unvoted")], 20.0)
        text = jp.render(runs, "Paired")
        # Base: 20% of 0.6 and 0.7 ms, 130 µs; head: 10% of 0.5 ms, 50 µs.
        self.assertIn("| `coord_consensus::leader::Leader::resend_unvoted` | `coordd` | 130.0 | 50.0 | -80.0 |", text)
        self.assertIn("| `malloc` | `libc.so.6` | 32.5 | 25.0 | -7.5 |", text)

    def test_traffic_and_fast_path_by_pair(self):
        root = self.tmp.name
        runs = []
        for name, label, streams in (("t1", "base", 10000), ("t2", "head", 3100)):
            path = store(root, name, 50, 0.6, 2)
            with open(os.path.join(path, "n1", "coordd.log"), "w") as f:
                f.write(metrics(4, 1000, 0.6, 2, streams))
            runs.append(jp.read_run(label, path))
        self.assertAlmostEqual(runs[1].frames_per_stream, 10000 / 3100)
        text = jp.render(runs, "Paired")
        self.assertIn("| 1 | base | 10.0% | 90.0% | 10.00 | 10.00 | 1.00 | 5.00 | 2 | 2 |", text)
        self.assertIn("| 2 | head | 10.0% | 90.0% | 10.00 | 3.10 | 3.23 | 5.00 | 64 | 1 |", text)
        self.assertIn("| 1 | +0.0% (+0.0%) | +0.0% (+0.0%) | +0.00 (+0.0%) | -6.90 (-69.0%) | +2.23 (+222.6%) "
                      "| +0.00 (+0.0%) | +62 (+3100.0%) | -1 (-50.0%) |", text)

    def test_a_call_graph_run_is_costed_with_its_callees(self):
        with open(os.path.join(self.runs[1].store, "leader-profile-inclusive.txt"), "w") as f:
            f.write("leader thread 1, call graph\n"
                    "    60.00%     2.00%  coordd  [.] coordd::serve::Domain<P>::run\n"
                    "    20.00%    10.00%  coordd  [.] coord_daemon::voter::Voter<P>::pump_reads\n")
        runs = [jp.read_run(r.label, r.store) for r in self.runs]
        text = jp.render(runs, "Paired")
        self.assertIn("(run 2, head, the job's call-graph profile; microseconds per command, of its 500)", text)
        self.assertIn("| `coord_daemon::voter::Voter<P>::pump_reads` | `coordd` | 100.0 | 50.0 |", text)

    def test_a_call_graph_run_is_split_by_phase_per_command(self):
        turn = "coordd::serve::Domain<P>::run::{{closure}};coordd::serve::Domain<P>::turn::{{closure}}"
        with open(os.path.join(self.runs[1].store, "leader-profile-chains.txt"), "w") as f:
            f.write("leader thread 1, call graph\n"
                    "    60.00%  libc.so.6  [.] malloc\n"
                    f"60.00% {turn};coord_daemon::voter::Voter<P>::pump_reads;malloc\n"
                    "    40.00%  coordd  [.] coord_consensus::leader::Leader::resend_unvoted\n"
                    f"40.00% {turn};coord_daemon::voter::Voter<P>::resend_proposals;"
                    "coord_consensus::leader::Leader::resend_unvoted\n")
        runs = [jp.read_run(r.label, r.store) for r in self.runs]
        text = jp.render(runs, "Paired")
        # 60% and 40% of the run's 500 µs per command.
        self.assertIn("| `coord_daemon::voter::Voter<P>::pump_reads` | 300.0 | 300.0 |", text)
        self.assertIn("| `coord_daemon::voter::Voter<P>::resend_proposals` | 200.0 | 0.0 |", text)
        self.assertIn("(run 2, head: 300.0 µs per command in", text)

    def test_the_resend_timer_and_the_profiled_share_by_pair(self):
        root = self.tmp.name
        runs = []
        for name, label, resends, share in (("r1", "base", None, "10.00"), ("r2", "head", True, "1.00")):
            path = store(root, name, 50, 0.6, 2)
            if resends:
                text = metrics(4, 1000, 0.6, 2)
                cost = json.loads(text[len("metrics "):])
                cost["cost"]["Observed"]["resends"] = {"decided": 0, "acknowledged": 0, "unanswered": 0,
                                                       "duplicate_votes": 0, "calls": 400, "scanned": 10000,
                                                       "time": {"secs": 0, "nanos": 40000000},
                                                       "longest": {"secs": 0, "nanos": 2000000}}
                with open(os.path.join(path, "n1", "coordd.log"), "w") as f:
                    f.write("metrics " + json.dumps(cost) + "\n")
            with open(os.path.join(path, "leader-profile.txt"), "w") as f:
                f.write("leader thread 1\nby object: coordd 60.00%\n"
                        f"    {share}%  coordd  [.] coord_consensus::leader::Leader::resend_unvoted\n")
            runs.append(jp.read_run(label, path))
        self.assertAlmostEqual(runs[1].resend_ms_per_call, 0.1)
        text = jp.render(runs, "Paired")
        # The base has no timer; its profile gives 10% of 0.6 ms, 60 µs.
        self.assertIn("| 1 | base | - | - | - | 60.0 |", text)
        self.assertIn("| 2 | head | 0.100 | 2.00 | 25.0 | 6.0 |", text)
        self.assertIn("| 1 | - | - | - | -54.0 (-90.0%) |", text)

    def test_the_allocator_copies_and_memory_by_pair(self):
        for run, (alloc_sym, alloc_pct, rss) in zip(self.runs[:2], (("_int_malloc", "20.00", 130), ("mi_malloc", "5.00", 175))):
            with open(os.path.join(run.store, "leader-profile.txt"), "w") as f:
                f.write("leader thread 1\nby object: coordd 60.00%\n"
                        f"    {alloc_pct}%  libc.so.6  [.] {alloc_sym}\n"
                        "    10.00%  libc.so.6  [.] __memcmp_avx2_movbe\n")
            with open(os.path.join(run.store, "cpu-samples-memory.csv"), "w") as f:
                f.write("time,pid,start,rss_kib,hwm_kib\n"
                        f"2026-10-07 10:00:00.000000,1,1,{rss * 1024},{(rss + 10) * 1024}\n")
        runs = [jp.read_run(r.label, r.store) for r in self.runs[:2]]
        # Base: 20% of 0.6 ms; head: 5% of 0.5 ms.
        self.assertAlmostEqual(runs[0].alloc_us, 120.0)
        self.assertAlmostEqual(runs[1].alloc_us, 25.0)
        text = jp.render(runs, "Paired")
        self.assertIn("| 1 | base | 120.0 | 60.0 | 0.0 |", text)
        self.assertIn("| 2 | head | 25.0 | 50.0 | 0.0 |", text)
        self.assertIn("| 1 | -95.0 (-79.2%) | -10.0 (-16.7%) |", text)
        self.assertIn("| 1 | base | 130 | 130 | 140 |", text)
        self.assertIn("| 1 | +45 (+34.6%) | +45 (+34.6%) | +45 (+32.1%) |", text)

    def test_the_loops_and_tokio_threads_over_the_workload(self):
        path = self.runs[0].store
        with open(os.path.join(path, "cpu-samples.csv"), "w") as f:
            f.write("time,host_busy_s,host_total_s,cpus,servers_s,steal_s\n"
                    "2026-09-27 09:59:59.000000,0,0,4,10,0\n"
                    "2026-09-27 10:00:11.000000,0,0,4,14,0\n")
        # Two voters; a third that started mid-run has no first sample.
        with open(os.path.join(path, "cpu-samples-threads.csv"), "w") as f:
            f.write("time,pid,start,loop_cpu_s,loop_runq_s,workers_cpu_s,workers_runq_s,workers\n"
                    "2026-09-27 09:59:59.000000,1,1,1.0,0,2.0,0,8\n"
                    "2026-09-27 09:59:59.000000,2,1,1.0,0,2.0,0,8\n"
                    "2026-09-27 10:00:11.000000,1,1,1.6,0,2.8,0,8\n"
                    "2026-09-27 10:00:11.000000,2,1,1.2,0,2.4,0,8\n"
                    "2026-09-27 10:00:11.000000,3,5,9.0,0,9.0,0,8\n")
        run = jp.read_run("base", path)
        # Four completed reads: 4 s, 0.8 s and 1.2 s over them.
        self.assertAlmostEqual(run.servers_cpu_per_op, 1000.0)
        self.assertAlmostEqual(run.loops_cpu_per_op, 200.0)
        self.assertAlmostEqual(run.workers_cpu_per_op, 300.0)

    def test_the_leaders_tokio_threads_by_build(self):
        for run, futex in zip(self.runs[:2], (2000, 1000)):
            with open(os.path.join(run.store, "leader-profile-syscalls.txt"), "w") as f:
                f.write("leader process 100, each thread's CPU and system calls, counted from 2026-10-08 10:00:00 "
                        "to 2026-10-08 10:00:20 UTC (20 s); perf stat -x, --per-thread\n"
                        "coordd-100,600.00,msec,task-clock,1,100.00,0.03,CPUs utilized\n"
                        "tokio-rt-worker-101,300.00,msec,task-clock,1,100.00,0.015,CPUs utilized\n"
                        f"tokio-rt-worker-101,{futex},,syscalls:sys_enter_futex,1,100.00,1,/sec\n")
            with open(os.path.join(run.store, "transport-profile-chains.txt"), "w") as f:
                f.write("header\n    100.00%  [kernel.kallsyms]  [k] futex_wake\n"
                        "50.00% start_thread;tokio::runtime::park::Inner::unpark;__x64_sys_futex;futex_wake\n"
                        "50.00% start_thread;quinn_proto::connection::Connection::poll_transmit;__x64_sys_sendmsg;futex_wake\n")
        runs = [jp.read_run(r.label, r.store) for r in self.runs[:2]]
        text = jp.render(runs, "Paired")
        # Base: 600 ms of loop over 0.6 ms per command, 1000 commands; head: 0.5 ms, 1200.
        self.assertIn("| domain loop: CPU (µs) | 600.0 | 500.0 | -100.0 |", text)
        self.assertIn("| tokio threads: CPU (µs) | 300.0 | 250.0 | -50.0 |", text)
        self.assertIn("| `futex`, tokio threads | 2.00 | 0.83 | -1.17 |", text)
        self.assertIn("| tokio: send (`sendmsg` and the kernel's UDP send) (µs) | 150.0 | 125.0 | -25.0 |", text)

    def test_acknowledgements_and_the_two_planes(self):
        for run, acks in zip(self.runs[:2], (1500, 500)):
            for node, served in (("n1", 4), ("n2", 0)):
                text = metrics(served, 1000, 0.6, 2, streams=4000)
                cost = json.loads(text[len("metrics "):])
                traffic = cost["cost"]["Observed"]["traffic"]["Observed"]
                traffic.update(send_calls=6000, acks_sent=acks, acks_received=acks,
                               api={"datagrams_sent": 3000, "datagrams_received": 2000, "send_calls": 3000,
                                    "acks_sent": 1000, "acks_received": acks})
                with open(os.path.join(run.store, node, "coordd.log"), "w") as f:
                    f.write("metrics " + json.dumps(cost) + "\n")
        runs = [jp.read_run(r.label, r.store) for r in self.runs[:2]]
        self.assertAlmostEqual(runs[0].send_calls_per_cmd, 6.0)
        # Two voters, each 5000 peer datagrams over its 1000 commands.
        self.assertAlmostEqual(runs[0].peer_datagrams_all, 10.0)
        self.assertAlmostEqual(runs[1].api_acks_received_all, 1.0)
        text = jp.render(runs, "Paired")
        self.assertIn("Acknowledgements and the two planes", text)
        self.assertIn("| 1 | base | 6.00 | 1.50 | 1.50 | 10.00 | 6.00 | 4.00 | 6.00 | 2.00 | 3.00 |", text)
        self.assertIn("| 1 | +0.00 (+0.0%) | -1.00 (-66.7%) | -1.00 (-66.7%) |", text)
        # Before task-d70 there is no api block: the group's api columns stay empty.
        self.assertIsNone(self.runs[2].api_send_calls_all)

    def test_runs_that_are_not_side_by_side_are_not_paired(self):
        text = jp.render([self.runs[0], self.runs[3]], "Paired")
        self.assertIn("no pair to compare", text)


if __name__ == "__main__":
    unittest.main()
