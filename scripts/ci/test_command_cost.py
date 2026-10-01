"""Tests for the per-command cost reduction and its gate (task-d45)."""
import json
import unittest

import command_cost as cost


def duration(seconds):
    whole = int(seconds)
    return {"secs": whole, "nanos": int(round((seconds - whole) * 1e9))}


def snapshot(executed=100, lowerings=500, appends=500, syncs=505, commits=500,
             busy=2.0, uptime=10.0, syncs_observed=True):
    journal_syncs = {"Observed": syncs} if syncs_observed else {"Unavailable": "NotInstrumented"}
    return {
        "frontiers": {"Observed": {"journal": 1, "materialized": 1, "checkpoint": 0}},
        "cost": {"Observed": {
            "executed": executed,
            "lowerings": lowerings,
            "journal_appends": appends,
            "journal_syncs": journal_syncs,
            "projection_commits": commits,
            "busy": duration(busy),
            "uptime": duration(uptime),
            "recent": {"Unavailable": "NoSamples"},
        }},
    }


def log(*snapshots, tail=""):
    lines = ["coordd phase=live"]
    lines += ["metrics " + json.dumps(s) for s in snapshots]
    return "\n".join(lines) + "\n" + tail


def run(callers, busy_ms, syncs, tail_ms=(1.0, 1.0)):
    return {"callers": callers, "voters": [
        {"node": "n1", "busy_ms_per_command": busy_ms[0], "journal_syncs_per_command": syncs[0],
         "tail_busy_ms_per_command": tail_ms[0]},
        {"node": "n2", "busy_ms_per_command": busy_ms[1], "journal_syncs_per_command": syncs[1],
         "tail_busy_ms_per_command": tail_ms[1]},
    ]}


BASELINE = {
    "margins": {"busy_ms_per_command": 0.25, "tail_busy_ms_per_command": 0.25,
                "journal_syncs_per_command": 0.10},
    "runs": [
        {"callers": 1, "busy_ms_per_command": 10.0, "tail_busy_ms_per_command": 1.0,
         "journal_syncs_per_command": 5.0},
        {"callers": 10, "busy_ms_per_command": 8.0, "tail_busy_ms_per_command": 1.0,
         "journal_syncs_per_command": 4.0},
    ],
}


class ReduceTests(unittest.TestCase):
    def test_the_last_snapshot_is_the_one_read(self):
        text = log(snapshot(executed=10), snapshot(executed=100))
        self.assertEqual(cost.last_snapshot(text)["cost"]["Observed"]["executed"], 100)

    def test_a_line_cut_by_a_kill_is_skipped(self):
        text = log(snapshot(executed=10), tail='metrics {"cost": {"Obs')
        self.assertEqual(cost.last_snapshot(text)["cost"]["Observed"]["executed"], 10)

    def test_per_command_divides_by_what_the_voter_executed(self):
        reading = cost.per_command("n1", snapshot())
        self.assertAlmostEqual(reading["lowerings_per_command"], 5.0)
        self.assertAlmostEqual(reading["journal_syncs_per_command"], 5.05)
        self.assertAlmostEqual(reading["busy_ms_per_command"], 20.0)
        self.assertAlmostEqual(reading["busy_fraction"], 0.2)

    def test_an_absent_reading_is_absent_not_zero(self):
        with self.assertRaises(cost.Absent):
            cost.per_command("n1", snapshot(syncs_observed=False))
        with self.assertRaises(cost.Absent):
            cost.per_command("n1", snapshot(executed=0))
        with self.assertRaises(cost.Absent):
            cost.per_command("n1", {"cost": {"Unavailable": "NotThisRole"}})
        with self.assertRaises(cost.Absent):
            cost.reduce(1, 60.0, None, {"n1": "coordd phase=live\n"})

    def test_the_last_quarter_is_read_between_the_snapshots_that_bracket_it(self):
        text = log(
            snapshot(executed=50, busy=1.0),
            snapshot(executed=80, busy=2.0),
            snapshot(executed=100, busy=4.0),
            # After the load: idle, and not in the window.
            snapshot(executed=100, busy=4.5),
        )
        # From 80 executed to 100: two seconds over twenty commands.
        self.assertAlmostEqual(cost.tail_busy("n1", text), 100.0)

    def test_a_last_quarter_no_two_snapshots_bracket_is_absent(self):
        with self.assertRaises(cost.Absent):
            cost.tail_busy("n1", log(snapshot(executed=10), snapshot(executed=100)))

    def test_reduce_reports_throughput_without_gating_on_it(self):
        bench = {"achieved": {"completed": 600, "wall_ns": 60 * 10**9}}
        two = log(snapshot(executed=90, busy=1.8), snapshot())
        result = cost.reduce(1, 61.0, bench, {"n1": two, "n2": two})
        self.assertAlmostEqual(result["completed_per_second"], 10.0)
        self.assertEqual([v["node"] for v in result["voters"]], ["n1", "n2"])


class GateTests(unittest.TestCase):
    def test_within_the_margins_passes(self):
        result = {"runs": [run(1, (12.0, 9.0), (5.4, 5.0)), run(10, (9.9, 2.0), (4.3, 1.0))]}
        self.assertEqual(cost.gate(BASELINE, result), [])

    def test_the_busiest_voter_is_what_is_gated(self):
        result = {"runs": [run(1, (2.0, 12.6), (5.0, 5.0)), run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("busy_ms_per_command", failures[0])

    def test_a_last_quarter_past_the_margin_fails(self):
        result = {"runs": [run(1, (10.0, 10.0), (5.0, 5.0), tail_ms=(1.0, 1.3)),
                           run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("tail_busy_ms_per_command", failures[0])

    def test_syncs_past_the_margin_fail(self):
        result = {"runs": [run(1, (10.0, 10.0), (5.6, 5.0)), run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("journal_syncs_per_command", failures[0])

    def test_the_median_of_the_repeats_is_what_is_gated(self):
        # One repeat far past the margin, two within it: the median passes.
        result = {"runs": [run(1, (20.0, 1.0), (5.0, 5.0)), run(1, (10.0, 1.0), (5.0, 5.0)),
                           run(1, (11.0, 1.0), (5.0, 5.0)), run(10, (8.0, 8.0), (4.0, 4.0))]}
        self.assertEqual(cost.gate(BASELINE, result), [])
        # Two of three past it: the median fails.
        result["runs"][1] = run(1, (13.0, 1.0), (5.0, 5.0))
        result["runs"][2] = run(1, (14.0, 1.0), (5.0, 5.0))
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("median of 3", failures[0])

    def test_a_run_the_baseline_records_and_the_result_lacks_fails(self):
        result = {"runs": [run(1, (10.0, 10.0), (5.0, 5.0))]}
        self.assertEqual(cost.gate(BASELINE, result), ["10 callers: not run"])

    def test_the_committed_baseline_is_well_formed(self):
        import pathlib
        path = pathlib.Path(__file__).with_name("command-cost-baseline.json")
        baseline = json.loads(path.read_text())
        self.assertEqual(sorted(baseline["margins"]), sorted(cost.GATED))
        self.assertEqual(sorted(r["callers"] for r in baseline["runs"]), [1, 10])
        for r in baseline["runs"]:
            for field in cost.GATED:
                self.assertGreater(r[field], 0)


if __name__ == "__main__":
    unittest.main()
