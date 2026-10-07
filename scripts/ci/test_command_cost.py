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


def run(callers, busy_ms, syncs, tail_ms=None, head_ms=None):
    # By default each voter's last quarter, and what came before it,
    # cost what its whole run did.
    tail_ms = tail_ms or busy_ms
    head_ms = head_ms or busy_ms
    return {"callers": callers, "voters": [
        {"node": node, "busy_ms_per_command": busy_ms[i], "journal_syncs_per_command": syncs[i],
         "tail_busy_ms_per_command": tail_ms[i], "head_busy_ms_per_command": head_ms[i]}
        for i, node in enumerate(("n1", "n2"))
    ]}


BASELINE = {
    "margins": {"busy_ms_per_command": 0.25, "tail_busy_ms_per_command": 0.25,
                "tail_ratio": 0.30, "journal_syncs_per_command": 0.10},
    "runs": [
        {"callers": 1, "busy_ms_per_command": 10.0, "tail_busy_ms_per_command": 10.0,
         "tail_ratio": 1.0, "journal_syncs_per_command": 5.0},
        {"callers": 10, "busy_ms_per_command": 8.0, "tail_busy_ms_per_command": 8.0,
         "tail_ratio": 1.0, "journal_syncs_per_command": 4.0},
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

    def test_resends_are_read_per_command_when_reported(self):
        snap = snapshot()
        snap["cost"]["Observed"]["resends"] = {
            "deferred": 0, "decided": 2, "acknowledged": 1, "unanswered": 3,
            "lost": 2, "late": 1, "handed_off": 0, "duplicate_votes": 4,
        }
        reading = cost.per_command("n1", snap)
        self.assertAlmostEqual(reading["resent_per_command"], 0.06)
        self.assertAlmostEqual(reading["duplicate_votes_per_command"], 0.04)
        self.assertNotIn("resent_per_command", cost.per_command("n1", snapshot()))
        self.assertNotIn("resend_ms_per_call", reading)

    def test_a_resend_call_is_read_per_call_when_timed(self):
        snap = snapshot()
        snap["cost"]["Observed"]["resends"] = {
            "deferred": 0, "decided": 0, "acknowledged": 0, "unanswered": 0,
            "lost": 0, "late": 0, "handed_off": 0, "duplicate_votes": 0,
            "calls": 40, "scanned": 200,
            "time": {"secs": 0, "nanos": 2_000_000},
            "longest": {"secs": 0, "nanos": 300_000},
        }
        reading = cost.per_command("n1", snap)
        self.assertAlmostEqual(reading["resend_ms_per_call"], 0.05)
        self.assertAlmostEqual(reading["resend_longest_ms"], 0.3)
        self.assertAlmostEqual(reading["resend_scanned_per_call"], 5.0)

    def test_the_fast_path_share_is_read_when_reported(self):
        snap = snapshot()
        snap["cost"]["Observed"]["established_fast"] = 25
        snap["cost"]["Observed"]["established_slow"] = 75
        self.assertAlmostEqual(cost.per_command("n1", snap)["fast_path_share"], 0.25)
        self.assertNotIn("fast_path_share", cost.per_command("n1", snapshot()))

    def test_the_read_barrier_waits_are_read_as_means_when_reported(self):
        snap = snapshot()
        snap["cost"]["Observed"]["reads"] = {
            "served": 200, "refused": 3, "rounds": 90, "confirmed": 88,
            "waited_confirm_ms": 100, "waited_index_ms": 600, "waited_ms": 800,
        }
        reads = cost.per_command("n1", snap)["reads"]
        self.assertEqual((reads["served"], reads["refused"]), (200, 3))
        self.assertAlmostEqual(reads["mean_confirm_ms"], 0.5)
        self.assertAlmostEqual(reads["mean_index_ms"], 3.0)
        self.assertAlmostEqual(reads["mean_served_ms"], 4.0)
        self.assertNotIn("reads", cost.per_command("n1", snapshot()))

    def test_the_read_waits_are_taken_apart_into_three(self):
        snap = snapshot()
        snap["cost"]["Observed"]["reads"] = {
            "served": 200, "refused": 0, "rounds": 80, "confirmed": 80,
            "waited_confirm_ms": 100, "waited_index_ms": 600, "waited_ms": 800,
            "snapshots": 90, "behind": 220,
        }
        reads = cost.per_command("n1", snap)["reads"]
        self.assertAlmostEqual(reads["mean_after_confirm_ms"], 2.5)
        self.assertAlmostEqual(reads["mean_after_index_ms"], 1.0)
        self.assertAlmostEqual(reads["reads_per_round"], 2.5)
        self.assertAlmostEqual(reads["behind_per_read"], 1.1)
        self.assertAlmostEqual(reads["snapshots_per_read"], 0.45)

    def test_why_the_fast_path_missed_is_read_and_summed_when_reported(self):
        snap = snapshot()
        observed = snap["cost"]["Observed"]
        observed["established_fast"] = 5
        observed["established_slow"] = 20
        observed["fast_path"] = {
            "missed_path": 15, "missed_deps": 1, "missed_missing": 2,
            "missed_slow_first": 1, "missed_unclassified": 1,
            "acks": 26, "acks_reordered": 19,
        }
        observed["unordered"] = {"pending": 1, "reordered": 1, "oldest": duration(30)}
        observed["release"] = {
            "commands": 20, "predecessors": duration(0.04), "group": duration(0.02),
            "projection": duration(0.1),
        }
        reading = cost.per_command("n1", snap)
        paths = reading["fast_path"]
        self.assertEqual(paths["missed"]["path"], 15)
        self.assertTrue(paths["reasons_sum"])
        self.assertEqual((paths["acks"], paths["acks_reordered"]), (26, 19))
        self.assertEqual(reading["unordered"]["oldest_seconds"], 30)
        self.assertAlmostEqual(reading["release"]["predecessors_ms"], 2.0)
        self.assertAlmostEqual(reading["release"]["projection_ms"], 5.0)
        observed["fast_path"]["missed_path"] = 14
        self.assertFalse(cost.per_command("n1", snap)["fast_path"]["reasons_sum"])
        self.assertNotIn("release", cost.per_command("n1", snapshot()))

    def test_peer_traffic_and_pipeline_jobs_are_read_per_command(self):
        snap = snapshot()
        observed = snap["cost"]["Observed"]
        observed["traffic"] = {"Observed": {
            "sent_frames": 900, "sent_bytes": 270000, "sent_streams": 300, "sent_lost": 2,
            "sent_lost_streams": 1, "datagrams_sent": 500, "datagrams_received": 450,
            "send_calls": 480, "acks_sent": 200, "acks_received": 210,
            "received_frames": 880, "received_bytes": 190000, "received_streams": 880,
        }}
        jobs = lambda n, q, s, c: {"count": n, "queued": duration(q), "served": duration(s),
                                   "completed": duration(c)}
        observed["waits"] = {"Observed": {
            "appender": {"count": 9, "time": duration(0.04)},
            "materializer": {"count": 3, "time": duration(0.012)},
            "appender_jobs": jobs(90, 0.003, 0.18, 0.026),
            "materializer_jobs": jobs(12, 0.004, 0.07, 0.009),
        }}
        reading = cost.per_command("n1", snap)
        traffic = reading["traffic_per_command"]
        self.assertAlmostEqual(traffic["sent_frames"], 9.0)
        self.assertAlmostEqual(traffic["received_bytes"], 1900.0)
        self.assertAlmostEqual(traffic["sent_lost"], 0.02)
        self.assertAlmostEqual(reading["frames_per_stream_sent"], 3.0)
        self.assertAlmostEqual(reading["frames_per_stream_received"], 1.0)
        self.assertAlmostEqual(reading["frames_per_lost_stream"], 2.0)
        appender = reading["appender_jobs"]
        self.assertAlmostEqual(appender["jobs_per_command"], 0.9)
        self.assertAlmostEqual(appender["served_ms_per_command"], 1.8)
        self.assertAlmostEqual(reading["materializer_jobs"]["completed_ms_per_command"], 0.09)
        table = cost.paths_table({"runs": [{"callers": 1, "voters": [reading]}]})
        self.assertIn("9.00/8.80 (3.00/8.80)", table)
        self.assertIn("5.00/4.50 (4.80, 2.00)", table)
        self.assertNotIn("traffic_per_command", cost.per_command("n1", snapshot()))

    def test_a_run_with_no_fast_decision_is_a_finding(self):
        def voter(fast):
            snap = snapshot()
            snap["cost"]["Observed"]["established_fast"] = fast
            snap["cost"]["Observed"]["established_slow"] = 100 - fast
            return cost.per_command("n1", snap)
        self.assertEqual(cost.findings_of([voter(0), voter(0)]),
                         ["no voter decided a command on the fast path"])
        self.assertEqual(cost.findings_of([voter(0), voter(4)]), [])
        table = cost.paths_table({"runs": [
            {"callers": 10, "voters": [voter(0)], "findings": ["no fast decision"]}]})
        self.assertIn("**finding** | no fast decision", table)

    def test_cpu_is_read_per_command_when_reported(self):
        snap = snapshot(executed=200)
        snap["cost"]["Observed"]["cpu"] = {
            "Observed": {"domain": duration(0.3), "process": duration(0.9)},
        }
        reading = cost.per_command("n1", snap)
        self.assertAlmostEqual(reading["domain_cpu_ms_per_command"], 1.5)
        self.assertAlmostEqual(reading["process_cpu_ms_per_command"], 4.5)
        unavailable = snapshot()
        unavailable["cost"]["Observed"]["cpu"] = {"Unavailable": "NotInstrumented"}
        self.assertNotIn("domain_cpu_ms_per_command", cost.per_command("n1", unavailable))
        self.assertNotIn("domain_cpu_ms_per_command", cost.per_command("n1", snapshot()))

    def test_pipeline_waits_are_read_per_command_when_reported(self):
        snap = snapshot(executed=200)
        snap["cost"]["Observed"]["waits"] = {
            "Observed": {
                "appender": {"count": 30, "time": duration(0.1)},
                "materializer": {"count": 4, "time": duration(0.02)},
            },
        }
        reading = cost.per_command("n1", snap)
        self.assertAlmostEqual(reading["appender_wait_ms_per_command"], 0.5)
        self.assertAlmostEqual(reading["materializer_wait_ms_per_command"], 0.1)
        self.assertNotIn("appender_wait_ms_per_command", cost.per_command("n1", snapshot()))

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
            snapshot(executed=75, busy=1.875),
            snapshot(executed=100, busy=4.375),
            # After the load: idle, and not in the window.
            snapshot(executed=100, busy=4.875),
        )
        # From 75 executed to 100: two and a half seconds over 25 commands.
        self.assertAlmostEqual(cost.tail_busy("n1", text), 100.0)
        # Before it: 1.875 seconds over the first 75.
        head, _ = cost.head_and_tail_busy("n1", text)
        self.assertAlmostEqual(head, 25.0)

    def test_a_last_quarter_inside_one_snapshot_interval_is_interpolated(self):
        # A run fast enough that a quarter of it passes between two
        # snapshots a second apart. Between 40 and 100 executed every
        # command cost 25 ms, so three quarters falls at 1.875 s busy.
        text = log(
            snapshot(executed=0, busy=0.5),
            snapshot(executed=40, busy=1.0),
            snapshot(executed=100, busy=2.5),
            snapshot(executed=100, busy=3.0),
        )
        head, tail = cost.head_and_tail_busy("n1", text)
        self.assertAlmostEqual(head, 25.0)
        self.assertAlmostEqual(tail, 25.0)

    def test_a_cost_that_doubles_linearly_reads_past_the_margin(self):
        # A command's cost grows from 1 ms to 2 ms over 1,000 commands,
        # a snapshot every 50: the last quarter over the first three
        # reads about 1.36, where over the whole run it would read 1.25.
        busy, snaps = 0.0, []
        for executed in range(1, 1001):
            busy += (1 + executed / 1000) / 1000
            if executed % 50 == 0:
                snaps.append(snapshot(executed=executed, busy=busy))
        head, tail = cost.head_and_tail_busy("n1", log(*snaps))
        self.assertAlmostEqual(tail / head, 1.36, places=2)
        # With a snapshot only every 200, as a second apart is on a fast
        # runner's short run, and none on three quarters, it still reads
        # within 0.02 of that.
        coarse = [s for s in snaps if s["cost"]["Observed"]["executed"] % 200 == 0]
        head, tail = cost.head_and_tail_busy("n1", log(*coarse))
        self.assertAlmostEqual(tail / head, 1.36, delta=0.02)

    def test_a_last_quarter_no_two_snapshots_bracket_is_absent(self):
        # No snapshot was taken before three quarters had executed.
        with self.assertRaises(cost.Absent):
            cost.tail_busy("n1", log(snapshot(executed=80), snapshot(executed=100)))
        with self.assertRaises(cost.Absent):
            cost.tail_busy("n1", log(snapshot(executed=0), snapshot(executed=0)))

    def test_reduce_reports_throughput_without_gating_on_it(self):
        bench = {"achieved": {"completed": 600, "wall_ns": 60 * 10**9}}
        two = log(snapshot(executed=0, busy=0.2), snapshot(executed=90, busy=1.8), snapshot())
        result = cost.reduce(1, 61.0, bench, {"n1": two, "n2": two})
        self.assertAlmostEqual(result["completed_per_second"], 10.0)
        self.assertEqual([v["node"] for v in result["voters"]], ["n1", "n2"])


class GateTests(unittest.TestCase):
    def test_within_the_margins_passes(self):
        result = {"runs": [run(1, (12.0, 9.0), (5.4, 5.0)), run(10, (9.9, 2.0), (4.3, 1.0))]}
        self.assertEqual(cost.gate(BASELINE, result), [])

    def test_the_busiest_voter_is_what_is_gated(self):
        result = {"runs": [run(1, (2.0, 12.6), (5.0, 5.0), tail_ms=(2.0, 12.0)),
                           run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("busy_ms_per_command", failures[0])

    def test_a_last_quarter_past_the_margin_fails(self):
        result = {"runs": [run(1, (10.0, 10.0), (5.0, 5.0), tail_ms=(10.0, 12.8)),
                           run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("tail_busy_ms_per_command", failures[0])

    def test_a_last_quarter_grown_past_the_run_fails_on_any_machine(self):
        # A faster machine: both readings under the baseline's, but the
        # last quarter has grown against the run past the ratio's margin.
        result = {"runs": [run(1, (7.0, 7.0), (5.0, 5.0), tail_ms=(7.0, 9.5)),
                           run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("tail_ratio", failures[0])

    def test_the_ratio_is_each_voters_own(self):
        # A follower's last quarter grew 40% over its own run while the
        # leader, busier throughout, did not: the largest last quarter
        # over the largest whole run (1.0) would hide it.
        result = {"runs": [run(1, (10.0, 2.0), (5.0, 5.0), tail_ms=(10.0, 2.8)),
                           run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("tail_ratio 1.400", failures[0])

    def test_syncs_past_the_margin_fail(self):
        result = {"runs": [run(1, (10.0, 10.0), (5.6, 5.0)), run(10, (8.0, 8.0), (4.0, 4.0))]}
        failures = cost.gate(BASELINE, result)
        self.assertEqual(len(failures), 1)
        self.assertIn("journal_syncs_per_command", failures[0])

    def test_the_median_of_the_repeats_is_what_is_gated(self):
        # One repeat far past the margin, two within it: the median passes.
        flat = (10.0, 1.0)
        result = {"runs": [run(1, (20.0, 1.0), (5.0, 5.0), flat),
                           run(1, (10.0, 1.0), (5.0, 5.0), flat),
                           run(1, (11.0, 1.0), (5.0, 5.0), flat),
                           run(10, (8.0, 8.0), (4.0, 4.0))]}
        self.assertEqual(cost.gate(BASELINE, result), [])
        # Two of three past it: the median fails.
        result["runs"][1] = run(1, (13.0, 1.0), (5.0, 5.0), flat)
        result["runs"][2] = run(1, (14.0, 1.0), (5.0, 5.0), flat)
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
