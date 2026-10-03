"""Tests for the Jepsen job summary."""
import os
import tempfile
import unittest

import jepsen_summary as js


def file_line(ts, thread, msg):
    return f"2026-09-27 {ts},000{{GMT}}\tINFO\t[{thread}] jepsen.print: {msg}\n"


LOG = "".join(
    [
        "2026-09-27 10:00:00,000{GMT}\tINFO\t[jepsen test runner] jepsen.core: Running test\n",
        file_line("10:00:01", "jepsen worker 0", "0\t:invoke\t:txn\t[[:append 1 1]]"),
        file_line("10:00:02", "jepsen worker 0", "0\t:ok\t:txn\t[[:append 1 1]]"),
        file_line("10:00:03", "jepsen worker 1", "1\t:invoke\t:txn\t[[:r 1 nil]]"),
        file_line("10:00:04", "jepsen nemesis", ":nemesis\t:info\t:kill\t:all"),
        file_line("10:00:05", "jepsen nemesis", ':nemesis\t:info\t:kill\t{"n1" "", "n2" ""}'),
        # In flight across the heal: not a final read.
        file_line("10:00:40", "jepsen nemesis", ":nemesis\t:info\t:start\t:all"),
        file_line("10:00:41", "jepsen worker 1", "1\t:fail\t:txn\t[[:r 1 nil]]\ttimeout"),
        # The final reads: worker 0 on n1 served, worker 1 on n2 did not.
        file_line("10:01:41", "jepsen worker 0", "0\t:invoke\t:txn\t[[:r 1 nil]]"),
        file_line("10:01:41", "jepsen worker 1", "1\t:invoke\t:txn\t[[:r 1 nil]]"),
        file_line("10:01:42", "jepsen worker 0", "0\t:ok\t:txn\t[[:r 1 [1]]]"),
        file_line(
            "10:01:51",
            "jepsen worker 1",
            '1\t:fail\t:txn\t[[:r 1 nil]]\t[:no-client "throw+: {:type :jepsen.tuplesky.client/shim-not-ready,'
            ' :node \\"n2\\", :error \\"bind: Timeout\\"}"]',
        ),
    ]
)

RESULTS = """{:perf {:latency-graph {:valid? true}, :valid? true},
 :stats {:valid? true, :count 5},
 :workload {:valid? false, :anomaly-types (:G1a :lost-update)},
 :valid? false}
"""

VOTER = """coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true
recovered promise=None records=0 payloads=0 executed=0 history=0 frontier=0 position=0
this voter leads ballot 0
this voter's machine refused: Backpressure (1 so far)
this voter's machine refused: Backpressure (2 so far)
this voter's machine refused: Backpressure (4 so far)
cannot reach a voter on the peer plane: voter 03 Control: 127.0.0.1:7003: Rejected(Transport("connection lost")) (2 so far)
metrics {"stages":[]}
coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true
recovered promise=Some(1) records=10 payloads=10 executed=9 history=0 frontier=9 position=9
this voter follows ballot 3 led by 02020202
this voter's machine refused: Backpressure (1 so far)
this voter's machine refused: Promise(CandidateBehind { candidate: ExecutionPosition(1), own: ExecutionPosition(9) }) (1 so far)
cannot reach a voter on the peer plane: voter 02 Control: 127.0.0.1:7002: Rejected(Transport("connection lost")) (3 so far)
cannot reach a voter to submit to it: voter 02: 127.0.0.1:7102: Rejected(Timeout) (1 so far)
metrics {"stages":[{"stage":"Admission","metrics":{"Observed":{"entered":40,"completed":38,"refused":2,"latency":{"count":38,"total":{"secs":0,"nanos":19000000},"max":{"secs":0,"nanos":2000000}}}}},{"stage":"ClientTransit","metrics":{"Unavailable":"NotInstrumented"}},{"stage":"Journal","metrics":{"Observed":{"entered":120,"completed":120,"refused":0,"latency":{"count":120,"total":{"secs":1,"nanos":200000000},"max":{"secs":0,"nanos":45500000}}}}},{"stage":"Materialization","metrics":{"Observed":{"entered":0,"completed":0,"refused":0,"latency":{"count":0,"total":{"secs":0,"nanos":0},"max":{"secs":0,"nanos":0}}}}}],"durability":{"Unavailable":"NotInstrumented"},"cost":{"Observed":{"executed":400,"busy":{"secs":6,"nanos":500000000},"uptime":{"secs":10,"nanos":0},"recent":{"Observed":{"span":{"secs":1,"nanos":0},"busy":{"secs":0,"nanos":900000000},"executed":40}},"established_fast":30,"established_slow":90,"reads":{"served":300,"refused":2,"rounds":290,"confirmed":290,"waited_confirm_ms":600,"waited_index_ms":1500,"waited_ms":2340},"journal_syncs":{"Observed":100},"waits":{"Observed":{"appender":{"count":20,"time":{"secs":0,"nanos":200000000}},"materializer":{"count":4,"time":{"secs":0,"nanos":40000000}}}},"cpu":{"Observed":{"domain":{"secs":3,"nanos":0},"process":{"secs":8,"nanos":0}}}}}}
"""


class ParseTests(unittest.TestCase):
    def test_file_and_console_layouts(self):
        console = "INFO [2026-09-27 10:00:02,000] jepsen worker 0 - jepsen.print 0\t:ok\t:txn\t[[:append 1 1]]\n"
        for line in (file_line("10:00:02", "jepsen worker 0", "0\t:ok\t:txn\t[[:append 1 1]]"), console):
            (op,) = js.parse_ops([line])
            self.assertEqual((op.thread, op.process, op.type, op.f), ("jepsen worker 0", "0", "ok", ":txn"))

    def test_non_op_lines_are_skipped(self):
        self.assertEqual(js.parse_ops(["2026-09-27 10:00:00,000{GMT}\tINFO\t[x] jepsen.core: hello\n"]), [])

    def test_results(self):
        self.assertEqual(js.parse_results(RESULTS), ("false", [":G1a", ":lost-update"]))
        self.assertEqual(js.parse_results(""), ("missing", []))

    def test_voter_counts_sum_each_boots_highest(self):
        v = js.parse_voter(VOTER.splitlines(keepends=True))
        self.assertEqual(v.boots, 2)
        self.assertEqual(v.executed, "9")
        self.assertEqual(v.role, "follows ballot 3 led by 02020202")
        self.assertEqual(v.ballot, 3)
        self.assertEqual(v.counts["Backpressure"], 5)
        self.assertEqual(v.counts["CandidateBehind"], 1)
        # Each plane keeps its own counter, so each has its own column.
        self.assertEqual(v.counts["cannot reach (peer)"], 5)
        self.assertEqual(v.counts["cannot reach (collector)"], 1)
        # The other plane's refusal is by design, not a failure.
        self.assertNotIn("alert 120", v.counts)
        self.assertNotIn("stopped", v.counts)
        self.assertEqual(v.after_start, {})

    def test_stages_from_the_last_metrics_line(self):
        v = js.parse_voter(VOTER.splitlines(keepends=True))
        # The first boot's empty snapshot is replaced by the last one, and
        # a stage that is not instrumented is left out, not read as zero.
        self.assertEqual(set(v.stages), {"Admission", "Journal", "Materialization"})
        self.assertEqual(v.stages["Journal"], (120, 120, 0, 120, 1.2, 0.0455))
        self.assertEqual(js.parse_voter(["metrics {not json\n"]).stages, {})

    def test_cost_from_the_last_metrics_line(self):
        v = js.parse_voter(VOTER.splitlines(keepends=True))
        self.assertEqual(v.cost, js.Cost(400, 6.5, 10.0, (0.9, 1.0), 300, 2, 2340, 30, 90, (3.0, 8.0), 100, (0.2, 0.04)))
        # A first snapshot has no interval, a voter before task-d50 no
        # reads, one before task-d54 no CPU, and an unavailable cost no
        # reading at all.
        first = {"Observed": {"executed": 0, "busy": {"secs": 0, "nanos": 0}, "uptime": {"secs": 1, "nanos": 0},
                              "recent": {"Unavailable": "NotInstrumented"}}}
        self.assertEqual(js.parse_cost({"cost": first}), js.Cost(0, 0.0, 1.0, None, 0, 0, 0))
        self.assertIsNone(js.parse_cost({"cost": {"Unavailable": "NotInstrumented"}}))
        self.assertIsNone(js.parse_cost({}))
        # A later snapshot without a cost clears an earlier one's.
        self.assertIsNone(js.parse_voter(VOTER.splitlines(keepends=True) + ['metrics {"stages":[]}\n']).cost)

    def test_counts_after_the_final_start(self):
        republished = "this voter's machine refused: ProposalRepublished(CommandIdDigest32(ab)) ({} so far)\n"
        running = [
            "coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true\n",
            republished.format(1),
            "2026-09-28 02:40:28 Jepsen starting  /opt/tuplesky/coordd --config coordd.toml\n",
            republished.format(1),  # an earlier start: not the final one
            republished.format(2),
            "2026-09-28 02:45:28 Jepsen starting  /opt/tuplesky/coordd --config coordd.toml\n",
            "/opt/tuplesky/coordd already running.\n",
            republished.format(4),
            republished.format(8),
        ]
        v = js.parse_voter(running)
        self.assertEqual(v.counts["ProposalRepublished"], 8)
        self.assertEqual(v.after_start, {"ProposalRepublished": 6})
        # Started again by the final heal: its new boot counts from zero.
        restarted = running[:6] + [
            "coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true\n",
            republished.format(1),
        ]
        v = js.parse_voter(restarted)
        self.assertEqual(v.after_start, {"ProposalRepublished": 1})
        # Quiet after the final start.
        self.assertEqual(js.parse_voter(running[:7]).after_start, {})


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        store = self.dir.name
        with open(os.path.join(store, "jepsen.log"), "w") as f:
            f.write(LOG)
        with open(os.path.join(store, "results.edn"), "w") as f:
            f.write(RESULTS)
        os.mkdir(os.path.join(store, "n1"))
        with open(os.path.join(store, "n1", "coordd.log"), "w") as f:
            f.write(VOTER)
        with open(os.path.join(store, "n1", "executed-at-end"), "w") as f:
            f.write("5820\n")
        os.mkdir(os.path.join(store, "n2"))
        with open(os.path.join(store, "n2", "coordd.log"), "w") as f:
            f.write(VOTER)
        with open(os.path.join(store, "n2", "executed-at-end"), "w") as f:
            f.write("cannot open n2.redb: I/O error\n")
        self.text = js.summarize(store, ["n1", "n2"], "Jepsen")

    def tearDown(self):
        self.dir.cleanup()

    def test_verdict_and_anomalies(self):
        self.assertIn("`:valid? false`", self.text)
        self.assertIn("`:G1a`, `:lost-update`", self.text)

    def test_counts_and_last_ok(self):
        self.assertIn("| 4 | 2 | 2 | 0 | 10:01:42 (+101 s) | 10:00:40 (+39 s) |", self.text)

    def test_throughput_until_the_heal(self):
        # One ok before the heal at +39 s; the final read after it is left out.
        self.assertIn("**Throughput:** 1 `ok` in 39 s until the final heal, 0.0 `ok`/s", self.text)
        self.assertIn("| `:txn` | 1 | 1000 | 1000 | 1000 | 1000 |", self.text)

    def test_final_reads_pair_invocations_after_the_heal(self):
        self.assertIn("1 of 2 nodes served a final read", self.text)
        self.assertIn("| n1 | 1 | 0 |  |", self.text)
        self.assertIn("| n2 | 0 | 1 | `shim-not-ready: bind: Timeout` ×1 |", self.text)
        self.assertNotIn("`timeout` ×", self.text)

    def test_faults_and_voters(self):
        self.assertIn("<summary>Faults (2)</summary>", self.text)
        self.assertIn('| 10:00:04 | 3 | `:kill` | :all | {"n1" "", "n2" ""} |', self.text)
        self.assertIn("| 10:00:40 | 39 | `:start` | :all | - |", self.text)
        self.assertIn("| n1 | 2 | 9 | 5820 | follows ballot 3 led by 02020202 | 3 |", self.text)
        # A store that did not open reads "?", not a number.
        self.assertIn("| n2 | 2 | 9 | ? | follows ballot 3 led by 02020202 | 3 |", self.text)

    def test_stages(self):
        self.assertIn("| n1 | Journal | 120 | 0 | 10.00 | 45.5 | 1.2 |", self.text)
        self.assertIn("| n2 | Admission | 38 | 2 | 0.50 | 2.0 | 0.0 |", self.text)
        # A stage nothing passed through has no row.
        self.assertNotIn("Materialization", self.text)

    def test_domain_loop(self):
        self.assertIn("Reads served | Reads refused | Mean read wait (ms) |", self.text)
        self.assertIn("Loop CPU per command (ms) | Process CPU per command (ms) |", self.text)
        self.assertIn("Journal syncs per command | Loop CPU per command (ms)", self.text)
        self.assertIn("Process CPU per command (ms) | Appender wait per command (ms) | Materializer wait per command (ms) |", self.text)
        self.assertIn(
            "| n1 | 400 | 6.5 | 10.0 | 65% | 90% | 16.25 | 25% of 120 | 0.25 | 7.50 | 20.00 | 0.50 | 0.10 | 300 | 2 | 7.8 |", self.text
        )

    def test_executed_at_end_without_a_file(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(js.read_executed_at_end(d), "-")

    def test_throughput_without_faults(self):
        ops = js.parse_ops(
            [
                file_line("10:00:00", "jepsen worker 0", "0\t:invoke\t:read\tnil"),
                file_line("10:00:01", "jepsen worker 1", "1\t:invoke\t:write\t3"),
                file_line("10:00:01", "jepsen worker 0", "0\t:ok\t:read\t3"),
                file_line("10:00:02", "jepsen worker 0", "0\t:invoke\t:read\tnil"),
                file_line("10:00:03", "jepsen worker 1", "1\t:info\t:write\t3\ttimeout"),
                file_line("10:00:04", "jepsen worker 0", "0\t:ok\t:read\t3"),
            ]
        )
        oks, secs, latencies = js.throughput(ops, None)
        self.assertEqual((oks, secs), (2, 4.0))
        self.assertEqual(latencies, {":read": [1000.0, 2000.0]})

    def test_percentiles_are_nearest_rank(self):
        ms = [float(i) for i in range(1, 101)]
        self.assertEqual([js.percentile(ms, q) for q in (0.5, 0.95, 0.99)], [50.0, 95.0, 99.0])
        self.assertEqual(js.percentile([7.0], 0.99), 7.0)

    def test_network_under_a_simulated_wan(self):
        def wan_line(msg):
            return f"2026-09-27 10:00:00,000{{GMT}}\tINFO\t[jepsen nemesis] jepsen.tuplesky.wan: {msg}\n"

        lines = [
            wan_line('WAN regions: {"n1" "us-east", "n2" "us-west"}'),
            wan_line("WAN clients: first"),
            wan_line("WAN round trip n1 -> n2 : 66.4 ms, profile 66 ms"),
            wan_line("WAN round trip n2 -> control : nil ms, profile 66 ms"),
            file_line("10:00:01", "jepsen worker 0", "0\t:invoke\t:read\tnil"),
        ]
        self.assertEqual(
            js.parse_network(lines),
            ("first", [("n1", "n2", "66.4", "66"), ("n2", "control", "nil", "66")]),
        )
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "jepsen.log"), "w") as f:
                f.writelines(lines)
            text = js.summarize(d, ["n1", "n2"], "Jepsen")
        self.assertIn("round trips measured at setup; clients beside the first node", text)
        self.assertIn("| n1 | n2 | 66.4 | 66 |", text)

    def test_missing_files_leave_sections_out(self):
        with tempfile.TemporaryDirectory() as empty:
            text = js.summarize(empty, [], "Jepsen")
        self.assertIn("no results.edn", text)
        self.assertIn("No `jepsen.log`", text)
        self.assertNotIn("Voters", text)


STRICT_VOTER = """coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true
recovered promise=None records=0 payloads=0 executed=0 history=0 frontier=0 position=0
metrics {"stages":[{"stage":"Materialization","metrics":{"Observed":{"entered":5,"completed":5,"refused":0,"latency":{"count":5,"total":{"secs":0,"nanos":50000000},"max":{"secs":0,"nanos":20000000}}}}}],"frontiers":{"Observed":{"journal":120,"materialized":120,"checkpoint":0}}}
"""

# Killed after its last interval line: what it had applied, and how much of
# that its projection held durably.
REPLAY_VOTER = STRICT_VOTER.replace(
    '"materialized":120,"checkpoint":0}', '"materialized":118,"checkpoint":0,"projection_durable":64}'
)


class ProfileTests(unittest.TestCase):
    """task-j06: the journal profile the voters ran, from their own metrics."""

    def summarize(self, voter, profile=None):
        with tempfile.TemporaryDirectory() as store:
            for node in ["n1", "n2"]:
                os.mkdir(os.path.join(store, node))
                with open(os.path.join(store, node, "coordd.log"), "w") as f:
                    f.write(voter)
            return js.summarize(store, ["n1", "n2"], "TupleSky", profile)

    def test_a_replay_run_shows_each_voters_projection_durable(self):
        text = self.summarize(REPLAY_VOTER, "replay")
        self.assertNotIn("dispatched as", text)
        self.assertIn("| Executed at end | Projection durable | Last role |", text)
        self.assertIn("| n1 | 1 | 0 | - | 64 of 118 | - |", text)
        self.assertIn("a `Materialization` entry is a working projection commit", text)
        self.assertIn("so under this profile it is the projection's last durable commit", text)
        self.assertIn("| n1 | Materialization | 5 | 0 | 10.00 | 20.0 | 0.1 |", text)

    def test_a_strict_run_has_no_projection_column_or_caption(self):
        text = self.summarize(STRICT_VOTER, "strict")
        self.assertNotIn("dispatched as", text)
        self.assertNotIn("Projection durable", text)
        self.assertNotIn("working projection commit", text)
        self.assertNotIn("last durable commit", text)

    def test_a_run_whose_voters_ran_another_profile_says_so_first(self):
        text = self.summarize(STRICT_VOTER, "replay")
        self.assertTrue(
            text.startswith(
                "## TupleSky\n\n**The voters ran the strict journal profile; this run was dispatched as replay.**"
            ),
            text[:200],
        )
        self.assertIn("dispatched as strict", self.summarize(REPLAY_VOTER, "strict"))

    def test_voters_that_report_no_frontiers_are_not_judged(self):
        # A build from before task-61's frontiers, or no metrics line at all.
        self.assertNotIn("dispatched as", self.summarize(VOTER, "replay"))


if __name__ == "__main__":
    unittest.main()


class RunQueueTests(unittest.TestCase):
    """task-d55: the domain loop's run-queue time, where the voters report
    it, is a column of its own; a run whose voters do not report it has
    none."""

    def summary(self, voter: str) -> str:
        with tempfile.TemporaryDirectory() as store:
            with open(os.path.join(store, "jepsen.log"), "w") as f:
                f.write(LOG)
            with open(os.path.join(store, "results.edn"), "w") as f:
                f.write(RESULTS)
            os.mkdir(os.path.join(store, "n1"))
            with open(os.path.join(store, "n1", "coordd.log"), "w") as f:
                f.write(voter)
            return js.summarize(store, ["n1"], "Jepsen")

    def test_a_voter_that_reports_its_run_queue_has_the_column(self):
        voter = VOTER.replace(
            '"process":{"secs":8,"nanos":0}}',
            '"process":{"secs":8,"nanos":0},"domain_scheduling":{"Observed":'
            '{"run_queue":{"secs":0,"nanos":600000000},"voluntary":900,"involuntary":40}}}',
        )
        self.assertNotEqual(voter, VOTER)
        self.assertEqual(js.parse_voter(voter.splitlines(keepends=True)).cost.run_queue, 0.6)
        text = self.summary(voter)
        self.assertIn("Materializer wait per command (ms) | Loop run queue per command (ms) |", text)
        self.assertIn("| 0.50 | 0.10 | 1.50 | 300 |", text)
        self.assertIn("run queue is the time", text)

    def test_a_voter_that_does_not_has_no_column(self):
        self.assertIsNone(js.parse_voter(VOTER.splitlines(keepends=True)).cost.run_queue)
        text = self.summary(VOTER)
        self.assertNotIn("run queue", text.lower())
