"""Tests for jepsen_sweep.py: python3 -m unittest test_jepsen_sweep (from scripts/ci)."""

import json
import os
import tempfile
import unittest

import jepsen_sweep as sw


def op(ts, worker, kind, f):
    return f"2026-09-27 {ts}{{GMT}}\tINFO\t[jepsen worker {worker}] jepsen.print: {worker}\t:{kind}\t:{f}\tnil\n"


def metrics(served, executed, loop_s, appends=None, commits=None, run_queue_s=None):
    cost = {
        "executed": executed,
        "busy": {"secs": 1, "nanos": 0},
        "uptime": {"secs": 10, "nanos": 0},
        "recent": {"Unavailable": "NotInstrumented"},
        "reads": {"served": served, "refused": 0, "rounds": served, "confirmed": served, "waited_ms": 0},
        "cpu": {"Observed": {"domain": {"secs": 0, "nanos": int(loop_s * 1e9)}, "process": {"secs": 2, "nanos": 0}}},
    }
    if appends is not None:
        cost["journal_appends"] = appends
        cost["projection_commits"] = commits
    if run_queue_s is not None:
        cost["cpu"]["Observed"]["domain_scheduling"] = {
            "Observed": {"run_queue": {"secs": 0, "nanos": int(run_queue_s * 1e9)}}
        }
    return "metrics " + json.dumps({"stages": [], "cost": {"Observed": cost}}) + "\n"


SAMPLES = (
    "time,host_busy_s,host_total_s,cpus,servers_s,clients_s,jvm_s,plumbing_s,steal_s\n"
    "2026-09-27 09:59:59.000000,100,400,4,10,1,2,0.5,0\n"
    # 12 s apart: 40 s of the host's, 24 busy, 1 of it steal.
    "2026-09-27 10:00:11.000000,124,440,4,22,2,6,1.5,1\n"
)
THREADS = (
    "time,pid,start,loop_cpu_s,loop_runq_s,workers_cpu_s,workers_runq_s,workers\n"
    "2026-09-27 09:59:59.000000,1,1,1.0,0,2.0,0,1\n"
    "2026-09-27 09:59:59.000000,2,1,1.0,0,2.0,0,1\n"
    "2026-09-27 10:00:11.000000,1,1,2.0,0,4.0,0,1\n"
    "2026-09-27 10:00:11.000000,2,1,2.0,0,4.0,0,1\n"
)
WAKES = (
    "time,pid,start,kind,threads,cpu_s,run_queue_s,timeslices\n"
    "2026-09-27 09:59:59.000000,1,1,appender,1,0.5,0,10\n"
    "2026-09-27 09:59:59.000000,1,1,materializer,1,0.5,0,10\n"
    "2026-09-27 09:59:59.000000,1,1,other,2,0.5,0,10\n"
    "2026-09-27 10:00:11.000000,1,1,appender,1,1.5,0,20\n"
    "2026-09-27 10:00:11.000000,1,1,materializer,1,2.5,0,20\n"
    "2026-09-27 10:00:11.000000,1,1,other,2,1.0,0,20\n"
)


def history():
    """Over 10 s: worker 0 reads twice (100 ms, 300 ms), worker 1 writes
    once (1 s) and fails a cas (500 ms), worker 2 has a cas in flight at
    the end, from 10:00:08."""
    return [
        op("10:00:00,000", 0, "invoke", "read"),
        op("10:00:00,100", 0, "ok", "read"),
        op("10:00:01,000", 1, "invoke", "write"),
        op("10:00:02,000", 1, "ok", "write"),
        op("10:00:03,000", 0, "invoke", "read"),
        op("10:00:03,300", 0, "ok", "read"),
        op("10:00:04,000", 1, "invoke", "cas"),
        op("10:00:04,500", 1, "fail", "cas"),
        op("10:00:08,000", 2, "invoke", "cas"),
        op("10:00:10,000", 0, "invoke", "read"),
        op("10:00:10,000", 0, "ok", "read"),
    ]


class SweepTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def store(self, name, tuplesky=True, samples=True):
        path = os.path.join(self.tmp.name, name)
        os.makedirs(path)
        with open(os.path.join(path, "jepsen.log"), "w") as f:
            f.writelines(history())
        if tuplesky:
            for node, served, loop in (("n1", 4, 3.0), ("n2", 0, 1.0)):
                os.makedirs(os.path.join(path, node))
                with open(os.path.join(path, node, "coordd.log"), "w") as f:
                    f.write(metrics(served, 10, loop, 4, 5, 0.02) if served else metrics(served, 10, loop))
        if samples:
            for suffix, text in (("", SAMPLES), ("-threads", THREADS), ("-wakeups", WAKES)):
                with open(os.path.join(path, f"cpu-samples{suffix}.csv"), "w") as f:
                    f.write(text)
            with open(os.path.join(path, "cpu-samples-host.txt"), "w") as f:
                f.write("model=Some CPU\nmhz=2000\ncpus=4\n")
        return path

    def test_labels(self):
        self.assertEqual(sw.label("head@30=/s/1"), ("head", "30", "/s/1"))
        self.assertEqual(sw.label("etcd@6n=/s/2"), ("etcd", "6n", "/s/2"))
        with self.assertRaises(ValueError):
            sw.label("other@30=/s/1")

    def test_clients_mix_and_latency(self):
        step = sw.read_step("head", "30", self.store("h"))
        # 5 completed (3 reads, a write, a failed cas), 4 `ok`, over 10 s.
        self.assertEqual(step.completed, 5)
        self.assertAlmostEqual(step.ok_per_s, 0.4)
        self.assertAlmostEqual(step.completed_per_s, 0.5)
        # 0.1 + 1 + 0.3 + 0.5 + 0 s done, and 2 s in flight at the end.
        self.assertAlmostEqual(step.busy_clients, 0.39)
        self.assertAlmostEqual(step.mix[":read"], 0.6)
        self.assertAlmostEqual(step.writes_per_op, 0.4)
        self.assertEqual(step.latency[":read"], (100.0, 300.0))
        self.assertNotIn(":cas", step.latency)

    def test_the_runners_cpu_per_operation_by_part(self):
        step = sw.read_step("head", "30", self.store("h"))
        cpu = step.cpu
        # Per operation, over the five completed: 12 s of the servers'.
        self.assertAlmostEqual(cpu["servers"], 2400.0)
        self.assertAlmostEqual(cpu["clients"], 200.0)
        self.assertAlmostEqual(cpu["jvm"], 800.0)
        self.assertAlmostEqual(cpu["plumbing"], 200.0)
        # 24 busy, less 1 steal and 18 in the groups.
        self.assertAlmostEqual(cpu["else"], 1000.0)
        self.assertAlmostEqual(cpu["busy"], 4800.0)
        # 2 s of loops, split 3:1 by the voters' own lines.
        self.assertAlmostEqual(cpu["leader_loop"], 300.0)
        self.assertAlmostEqual(cpu["followers_loops"], 100.0)
        self.assertAlmostEqual(cpu["tokio"], 800.0)
        self.assertAlmostEqual(cpu["appender"], 200.0)
        self.assertAlmostEqual(cpu["materializer"], 400.0)
        self.assertAlmostEqual(cpu["voters_other"], 100.0)
        self.assertAlmostEqual(step.idle_cores, 16 / 12)
        self.assertAlmostEqual(step.steal_cores, 1 / 12)
        self.assertEqual((step.model, step.cpus), ("Some CPU", "4"))

    def test_the_leaders_commands_groups_and_run_queue(self):
        step = sw.read_step("head", "30", self.store("h"))
        self.assertAlmostEqual(step.commands_per_op, 2.0)
        self.assertAlmostEqual(step.per_group, 2.5)
        self.assertAlmostEqual(step.per_commit, 2.0)
        self.assertAlmostEqual(step.run_queue_per_cmd_ms, 2.0)

    def test_etcd_has_no_commands(self):
        step = sw.read_step("etcd", "30", self.store("e", tuplesky=False))
        self.assertIsNone(step.commands_per_op)
        self.assertNotIn("leader_loop", step.cpu)
        self.assertAlmostEqual(step.cpu["servers"], 2400.0)

    def test_a_run_without_samples(self):
        step = sw.read_step("head", "30", self.store("h", samples=False))
        self.assertEqual(step.cpu, {})
        self.assertAlmostEqual(step.commands_per_op, 2.0)

    def test_the_tables_and_the_ratio_to_etcd_at_each_step(self):
        steps = [
            sw.read_step("head", "30", self.store("h30")),
            sw.read_step("base", "30", self.store("b30")),
            sw.read_step("etcd", "30", self.store("e30", tuplesky=False)),
            sw.read_step("head", "60", self.store("h60")),
        ]
        text = sw.render(steps, "Sweep")
        self.assertIn("## Sweep", text)
        self.assertIn("**Runner:** Some CPU (4 CPUs)", text)
        self.assertIn("| 30 | head | 0.4 | 0.5 | 0.4 | 60% (1) / 20% (1) / 20% (2) | 100.0 / 300.0 | 1000.0 / 1000.0 | - |", text)
        self.assertIn("| 30 | etcd | 0.4 | 0.5 | 0.4 |", text)
        # Per command: 2400 / 2 commands an operation; per write: 2400 / 0.4.
        self.assertIn("| 30 | head | 2.000 | 1200.000 | 200.000 | 400.000 | 6000.000 | 2.50 | 2.00 | 2.000 | - |", text)
        self.assertIn("| 30 | etcd | - | - | - | - | 6000.000 | - | - | - | - |", text)
        self.assertIn("**Against etcd at the same step**", text)
        self.assertIn("| 30 | head | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× | - |", text)
        # No etcd at 60: no ratio row for it.
        self.assertNotIn("| 60 | head | 1.00×", text)


if __name__ == "__main__":
    unittest.main()
