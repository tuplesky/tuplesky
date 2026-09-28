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
        self.assertEqual(v.counts["cannot reach a voter"], 5)
        # The other plane's refusal is by design, not a failure.
        self.assertNotIn("alert 120", v.counts)
        self.assertNotIn("stopped", v.counts)
        self.assertEqual(v.after_start, {})

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
        self.text = js.summarize(store, ["n1", "n2"], "Jepsen")

    def tearDown(self):
        self.dir.cleanup()

    def test_verdict_and_anomalies(self):
        self.assertIn("`:valid? false`", self.text)
        self.assertIn("`:G1a`, `:lost-update`", self.text)

    def test_counts_and_last_ok(self):
        self.assertIn("| 4 | 2 | 2 | 0 | 10:01:42 (+101 s) | 10:00:40 (+39 s) |", self.text)

    def test_final_reads_pair_invocations_after_the_heal(self):
        self.assertIn("1 of 2 nodes served a final read", self.text)
        self.assertIn("| n1 | 1 | 0 |  |", self.text)
        self.assertIn("| n2 | 0 | 1 | `shim-not-ready: bind: Timeout` ×1 |", self.text)
        self.assertNotIn("`timeout` ×", self.text)

    def test_faults_and_voters(self):
        self.assertIn("<summary>Faults (2)</summary>", self.text)
        self.assertIn('| 10:00:04 | 3 | `:kill` | :all | {"n1" "", "n2" ""} |', self.text)
        self.assertIn("| 10:00:40 | 39 | `:start` | :all | - |", self.text)
        self.assertIn("| n1 | 2 | 9 | follows ballot 3 led by 02020202 | 3 |", self.text)

    def test_missing_files_leave_sections_out(self):
        with tempfile.TemporaryDirectory() as empty:
            text = js.summarize(empty, [], "Jepsen")
        self.assertIn("no results.edn", text)
        self.assertIn("No `jepsen.log`", text)
        self.assertNotIn("Voters", text)


if __name__ == "__main__":
    unittest.main()
