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

    def test_the_journal_and_projection_threads(self):
        path = self.store("h")
        # n1's line with its pipeline threads: six appends of about 1 ms and
        # two of 16.4 to 20 ms, 5 s busy; a materializer with no slow job.
        with open(os.path.join(path, "n1", "coordd.log")) as f:
            line = json.loads(f.read()[len("metrics "):])
        line["cost"]["Observed"]["waits"] = {"Observed": {
            "appender_jobs": {"count": 8, "served": {"secs": 5, "nanos": 0},
                              "served_each": {"buckets": [[35, 6], [52, 2]], "max": {"secs": 0, "nanos": 20000000}}},
            "materializer_jobs": {"count": 3, "served": {"secs": 1, "nanos": 0},
                                  "served_each": {"buckets": [[35, 3]], "max": {"secs": 0, "nanos": 1000000}}},
        }}
        line["cost"]["Observed"]["release"] = {"distributions": {
            "round": {"buckets": [[35, 10]], "max": {"secs": 0, "nanos": 1000000}}}}
        with open(os.path.join(path, "n1", "coordd.log"), "w") as f:
            f.write("metrics " + json.dumps(line) + "\n")
        step = sw.read_step("head", "30", path)
        self.assertEqual(list(step.pipelines), ["n1"])
        appender = step.pipelines["n1"]["appender"]
        self.assertEqual(appender.jobs, 8)
        self.assertAlmostEqual(appender.busy_s, 5.0)
        self.assertAlmostEqual(appender.slow_jobs, 0.25)
        # Midpoints: 960 µs six times, (16384 + 20000) / 2 twice.
        self.assertAlmostEqual(appender.slow_time, 2 * 18192 / (6 * 960 + 2 * 18192))
        self.assertEqual(step.pipelines["n1"]["materializer"].slow_jobs, 0)
        self.assertEqual(step.round_ms, (1.0, 1.0))
        text = sw.render([step], "Sweep")
        self.assertIn("**The voters' journal and projection threads**", text)
        self.assertIn("| 30 | head | 1.00 / 1.00 | n1 | 8 | 5.0 | 50% | 25% | 86% | 3 | 1.0 | 10% | 0% | 0% |", text)

    def test_counts_a_command_over_the_voters(self):
        path = self.store("h")
        # Both voters carry traffic; only n1 counts journal groups. The
        # leader, n1, executed 10 commands.
        for node, datagrams, acks in (("n1", 30, 10), ("n2", 20, 5)):
            with open(os.path.join(path, node, "coordd.log")) as f:
                line = json.loads(f.read()[len("metrics "):])
            line["cost"]["Observed"]["traffic"] = {"Observed": {
                "datagrams_sent": datagrams, "acks_sent": acks, "sent_frames": 40,
                "api": {"datagrams_sent": 5, "acks_sent": 1},
                "api_frames": {"submissions": 2, "releases": 3},
            }}
            with open(os.path.join(path, node, "coordd.log"), "w") as f:
                f.write("metrics " + json.dumps(line) + "\n")
        step = sw.read_step("head", "30", path)
        self.assertAlmostEqual(step.counts["peer_datagrams"], 5.0)
        self.assertAlmostEqual(step.counts["peer_acks"], 1.5)
        self.assertAlmostEqual(step.counts["peer_frames"], 8.0)
        self.assertAlmostEqual(step.counts["api_datagrams"], 1.0)
        self.assertAlmostEqual(step.counts["api_releases"], 0.6)
        self.assertAlmostEqual(step.counts["journal_groups"], 0.4)
        self.assertNotIn("evidence_for_marked", step.counts)
        self.assertNotIn("relayed", step.counts)
        text = sw.render([step], "Sweep")
        self.assertIn("**Counts a command, over the five voters**", text)
        # A count no voter carries gets no column.
        self.assertIn("| 30 | head | 5.00 | 1.50 | 8.00 | 1.00 | 0.20 | 0.40 | 0.60 | 0.40 | 0.50 |", text)
        self.assertNotIn("Relayed to the leader alone", text)
        # task-d102's relay and proposal frames, where a build counts them.
        for node, relayed in (("n1", 0), ("n2", 8)):
            with open(os.path.join(path, node, "coordd.log")) as f:
                line = json.loads(f.read()[len("metrics "):])
            line["cost"]["Observed"]["relay"] = {"relayed": relayed, "fallbacks": 1, "not_leading": 0}
            line["cost"]["Observed"]["proposals"] = {"proposals": 40 if node == "n1" else 0, "frames": 10 if node == "n1" else 0}
            with open(os.path.join(path, node, "coordd.log"), "w") as f:
                f.write("metrics " + json.dumps(line) + "\n")
        step = sw.read_step("head", "30", path)
        self.assertAlmostEqual(step.counts["relayed"], 0.8)
        self.assertAlmostEqual(step.counts["relay_fallbacks"], 0.2)
        self.assertAlmostEqual(step.counts["proposals"], 4.0)
        self.assertAlmostEqual(step.counts["proposal_frames"], 1.0)
        self.assertNotIn("proposal_bytes", step.counts)
        self.assertIn("Relayed to the leader alone", sw.render([step], "Sweep"))

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

    def test_commands_a_projection_commit_at_each_voter(self):
        path = self.store("h")
        # n2, a follower, executed 10 commands in 4 projection commits; n1,
        # the leader, in 5.
        with open(os.path.join(path, "n2", "coordd.log"), "w") as f:
            f.write(metrics(0, 10, 1.0, 4, 4))
        step = sw.read_step("head", "30", path)
        self.assertEqual(step.per_projection_commit, {"n1": (2.0, True), "n2": (2.5, False)})
        text = sw.render([step], "Sweep")
        self.assertIn("| Idle cores | Steal cores | Commands a projection commit, by voter |", text)
        self.assertIn(" | 2.00* / 2.50 |", text)
        self.assertIn("not the rows each writes", text)

    def test_a_voter_without_projection_commits_is_left_out(self):
        step = sw.read_step("head", "30", self.store("h"))
        self.assertEqual(step.per_projection_commit, {"n1": (2.0, True)})
        etcd = sw.read_step("etcd", "30", self.store("e", tuplesky=False))
        self.assertEqual(etcd.per_projection_commit, {})
        self.assertIn("| 30 | etcd |", sw.render([etcd], "Sweep"))

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
        self.assertIn("on this runner, Some CPU (4 CPUs);", text)
        self.assertIn("| 30 | head | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× | 1.00× | - |", text)
        # No etcd at 60: no ratio row for it.
        self.assertNotIn("| 60 | head | 1.00×", text)

    def test_voters_behind(self):
        path = self.store("h120")
        # n1 executed to 5000 and kept up; n2 stopped 2600 short, was
        # refused for backpressure twice in its boot and pulled 41 pages.
        with open(os.path.join(path, "n1", "executed-at-end"), "w") as f:
            f.write("5000\n")
        with open(os.path.join(path, "n2", "executed-at-end"), "w") as f:
            f.write("2400\n")
        with open(os.path.join(path, "n2", "coordd.log"), "a") as f:
            f.write("this collector was refused: Backpressure (1 so far)\n")
            f.write("this collector was refused: Backpressure (2 so far)\n")
            f.write("catch-up paths: pacer_asks=3 asked=9 pages=4 pulled=256\n")
            f.write("catch-up paths: pacer_asks=9 asked=50 pages=41 pulled=2624\n")
        step = sw.read_step("head", "120", path)
        self.assertEqual(step.behind, {"n1": (0, 0, None), "n2": (2600, 2, 41)})
        text = sw.render([step, sw.read_step("etcd", "120", self.store("e120", tuplesky=False))], "Sweep")
        self.assertIn("**Voters behind**", text)
        self.assertIn("| Step | Side | n1 | n2 |", text)
        self.assertIn("| 120 | head | 0 / 0 / - | 2600 / 2 / 41 |", text)
        self.assertNotIn("| 120 | etcd | 0 / ", text)


if __name__ == "__main__":
    unittest.main()
