"""Tests for the Jepsen job summary."""
import datetime
import json
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
metrics {"stages":[{"stage":"Admission","metrics":{"Observed":{"entered":40,"completed":38,"refused":2,"latency":{"count":38,"total":{"secs":0,"nanos":19000000},"max":{"secs":0,"nanos":2000000}}}}},{"stage":"ClientTransit","metrics":{"Unavailable":"NotInstrumented"}},{"stage":"Journal","metrics":{"Observed":{"entered":120,"completed":120,"refused":0,"latency":{"count":120,"total":{"secs":1,"nanos":200000000},"max":{"secs":0,"nanos":45500000}}}}},{"stage":"Materialization","metrics":{"Observed":{"entered":0,"completed":0,"refused":0,"latency":{"count":0,"total":{"secs":0,"nanos":0},"max":{"secs":0,"nanos":0}}}}}],"durability":{"Unavailable":"NotInstrumented"},"cost":{"Observed":{"executed":400,"busy":{"secs":6,"nanos":500000000},"uptime":{"secs":10,"nanos":0},"recent":{"Observed":{"span":{"secs":1,"nanos":0},"busy":{"secs":0,"nanos":900000000},"executed":40}},"established_fast":30,"established_slow":90,"reads":{"served":300,"refused":2,"rounds":290,"confirmed":290,"waited_confirm_ms":600,"waited_index_ms":1500,"waited_ms":2340,"snapshots":150,"behind":4},"journal_syncs":{"Observed":100},"waits":{"Observed":{"appender":{"count":20,"time":{"secs":0,"nanos":200000000}},"materializer":{"count":4,"time":{"secs":0,"nanos":40000000}}}},"cpu":{"Observed":{"domain":{"secs":3,"nanos":0},"process":{"secs":8,"nanos":0}}}}}}
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

    def test_process_cpu_sums_each_boots_last_reading(self):
        metrics = next(line for line in VOTER.splitlines() if '"cost"' in line)
        reading = lambda secs: metrics.replace('"process":{"secs":8', f'"process":{{"secs":{secs}')
        boot = "coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true"
        # The first boot's 5 s is its last reading; the second's counter
        # starts again, and its last reading is 8 s.
        v = js.parse_voter([boot, reading(2), reading(5), boot, reading(3), reading(8)])
        self.assertEqual(v.process_cpu, 13.0)
        self.assertIsNone(js.parse_voter([boot]).process_cpu)

    def test_a_profile_is_costed_at_the_boot_it_sampled(self):
        metrics = next(line for line in VOTER.splitlines() if '"cost"' in line)
        # A reading at `up` s of uptime, with the loop's CPU and the commands
        # executed by then.
        at = lambda up, cpu, executed: (metrics.replace('"domain":{"secs":3', f'"domain":{{"secs":{cpu}')
                                        .replace('"uptime":{"secs":10', f'"uptime":{{"secs":{up}')
                                        .replace('"executed":400,', f'"executed":{executed},'))
        start = lambda t: f"2026-10-08 {t} Jepsen starting  /opt/tuplesky/coordd --config coordd.toml"
        boot = "coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true"
        header = "leader thread 7 (n1), 0.90 of a core over the 5 s before, sampled at 99 Hz from 2026-10-08 10:01:00 to 2026-10-08 10:01:20 UTC; Samples: 1K\n"
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile.txt")
            with open(path, "w") as f:
                f.write(header)
            # One boot, read at 50 s (10:00:50) and 90 s (10:01:30) of
            # uptime, either side of the window, and at 300 s after a
            # change of load: the window's own 3 s over 600 commands.
            v = js.parse_voter([start("10:00:00"), boot, at(50, 2, 400), at(90, 5, 1000), at(300, 60, 2000)])
            window = js.sampled_cost(v, path)
            self.assertEqual((window.cpu[0], window.executed), (3.0, 600))
            # Killed and restarted after the window: the first boot's, not
            # the last line's.
            v = js.parse_voter([start("10:00:00"), boot, at(50, 2, 400), at(90, 5, 1000),
                                start("10:02:00"), boot, at(10, 1, 50)])
            self.assertEqual(v.cost.cpu[0], 1.0)
            self.assertEqual(js.sampled_cost(v, path).cpu[0], 3.0)
            # Killed within the window, before a reading after it: from the
            # reading before it to its last.
            v = js.parse_voter([start("10:00:00"), boot, at(50, 2, 400), at(65, 4, 700), start("10:03:00"), boot])
            window = js.sampled_cost(v, path)
            self.assertEqual((window.cpu[0], window.executed), (2.0, 300))
            # No reading before the window: the first after it counts from
            # the boot's start, so it is left uncosted.
            v = js.parse_voter([start("10:00:55"), boot, at(40, 2, 400)])
            self.assertIsNone(js.sampled_cost(v, path))
            # No command between the readings either side: left uncosted.
            v = js.parse_voter([start("10:00:00"), boot, at(50, 2, 400), at(90, 3, 400)])
            self.assertIsNone(js.sampled_cost(v, path))
            # Restarted within the window: not known.
            v = js.parse_voter([start("10:00:00"), boot, at(50, 2, 400), start("10:01:10"), boot, at(10, 1, 50)])
            self.assertIsNone(js.sampled_cost(v, path))

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
        self.assertEqual(v.cost, js.Cost(400, 6.5, 10.0, (0.9, 1.0), 300, 2, 2340, 30, 90, (3.0, 8.0), 100, (0.2, 0.04), None, 290, 290, 150, 4))
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

    def test_each_node_back_serving_after_a_fault_ends(self):
        # The start at 10:00:40 is the final heal, after the workload's
        # last invocation: the final reads tell what followed it.
        self.assertNotIn("Back serving", self.text)

    def test_back_serving_takes_the_slowest_nodes_median(self):
        at = lambda s: datetime.datetime(2026, 9, 27, 10, 0, s)
        op = lambda s, w, t="ok", f=":txn": js.Op(at(s), f"jepsen worker {w}", str(w), t, f, "", "")
        nem = lambda s, f: js.Op(at(s), "jepsen nemesis", ":nemesis", "info", f, "", "")
        inv = lambda s, w: op(s, w, "invoke")
        # n2's operation invoked at 9, during the pause, and answered at 12
        # is held across the resume: not evidence that n2 serves again.
        client = [inv(0, 0), inv(0, 1), op(1, 0), op(1, 1), inv(9, 1), inv(12, 0), op(12, 0), op(12, 1),
                  inv(14, 1), op(14, 1), inv(31, 0), op(31, 0), inv(32, 1), op(32, 1)]
        nemesis = [nem(2, ":pause"), nem(3, ":pause"), nem(10, ":resume"), nem(11, ":resume"),
                   nem(20, ":start-partition"), nem(21, ":start-partition"), nem(29, ":stop-partition"), nem(30, ":stop-partition"),
                   # The final heal, after the last invocation: left out.
                   nem(40, ":resume"), nem(41, ":resume")]
        text = "\n".join(js.back_serving(client, nemesis, ["n1", "n2"], at(0)))
        self.assertIn("median 2.0 s, at most 3.0 s (n2, after `:resume` at +11 s)", text)
        self.assertIn("| `:resume` | 11 | 1.0 | 3.0 | 3.0 (n2) |", text)
        self.assertIn("| `:stop-partition` | 30 | 1.0 | 2.0 | 2.0 (n2) |", text)
        self.assertNotIn("| `:resume` | 41 |", text)

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
        self.assertIn(
            "Reads served | Reads refused | Mean read wait (ms) | Rounds | Reads per round | Rounds confirmed |"
            " Snapshots | Snapshots per read | Held behind |",
            self.text,
        )
        self.assertIn("Loop CPU per command (ms) | Process CPU per command (ms) |", self.text)
        self.assertIn("Journal syncs per command | Loop CPU per command (ms)", self.text)
        self.assertIn("Process CPU per command (ms) | Appender wait per command (ms) | Materializer wait per command (ms) |", self.text)
        self.assertIn(
            "| n1 | 400 | 6.5 | 10.0 | 65% | 90% | 16.25 | 25% of 120 | 0.25 | 7.50 | 20.00 | 0.50 | 0.10 | 300 | 2 | 7.8 |"
            " 290 | 1.03 | 100% | 150 | 0.50 | 4 |",
            self.text
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


def booted(at: str, executed: int, replayed: str | None = None) -> str:
    """One start of a replay voter: Jepsen's line and coordd's."""
    lines = [
        f"{at} Jepsen starting  /opt/tuplesky/coordd --config coordd.toml",
        "coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true",
        "storage projection=./state/gen-000001 journaled_through=Some(1) owed=0 baseline=0",
        f"recovered promise=None records={executed} payloads={executed} executed={executed} history=0 "
        f"frontier={executed} position={executed}",
    ]
    if replayed:
        lines.append(replayed)
    return "\n".join(lines) + "\n"


class BootTests(unittest.TestCase):
    """task-d55: every start of every voter, with what its attach replayed."""

    LOG = (
        booted("2026-10-03 01:41:25", 0, "replayed records=0 from=0 through=0 took_ms=0.0 attach_ms=3.1")
        + booted("2026-10-03 01:43:05", 95, "replayed records=90 from=200 through=290 took_ms=41.5 attach_ms=52.0")
        # A build before task-d55 prints no `replayed` line.
        + booted("2026-10-03 01:44:00", 95)
    )

    def test_each_boot_is_parsed(self):
        boots = js.parse_voter(self.LOG.splitlines(keepends=True)).boot_rows
        self.assertEqual([b.started for b in boots], ["2026-10-03 01:41:25", "2026-10-03 01:43:05", "2026-10-03 01:44:00"])
        self.assertEqual(boots[1].replayed, (90, 200, 290, 41.5, 52.0))
        self.assertIsNone(boots[2].replayed)
        self.assertEqual([b.executed for b in boots], ["0", "95", "95"])

    def summarize(self, voter):
        with tempfile.TemporaryDirectory() as store:
            os.mkdir(os.path.join(store, "n1"))
            with open(os.path.join(store, "n1", "coordd.log"), "w") as f:
                f.write(voter)
            return js.summarize(store, ["n1"], "TupleSky", "replay")

    def test_the_table_has_a_row_a_boot(self):
        text = self.summarize(self.LOG)
        self.assertIn("**Boots** (every start of each voter", text)
        self.assertIn("| n1 | 1 | 2026-10-03 01:41:25 | 0 | 0 | 0 | 0.0 | 3.1 | 0 |", text)
        self.assertIn("| n1 | 2 | 2026-10-03 01:43:05 | 90 | 200 | 290 | 41.5 | 52.0 | 95 |", text)
        self.assertIn("| n1 | 3 | 2026-10-03 01:44:00 | - | - | - | - | - | 95 |", text)

    def test_voters_that_report_no_replay_have_no_table(self):
        self.assertNotIn("**Boots**", self.summarize(REPLAY_VOTER))



class CheckpointTests(unittest.TestCase):
    """task-d51: each voter's publications, and what each held the loop."""

    LOG = (
        "coordd domain=tuplesky-harness roles=[Voter] phase=starting votes=true\n"
        "checkpoint represented=4095 retired=true reclaimed=0 took_ms=171 loop_ms=4.5 pin_ms=0.1 export_ms=77.7 "
        "write_ms=82.2 drain_ms=0.0 sync_ms=3.8 append_ms=0.2 retire_ms=0.6 reclaim_ms=0.0\n"
        "checkpoint represented=8190 retired=true reclaimed=1 took_ms=240 loop_ms=12.5 pin_ms=0.1 export_ms=90.0 "
        "write_ms=100.0 drain_ms=2.0 sync_ms=0.0 append_ms=5.0 retire_ms=5.0 reclaim_ms=30.0\n"
        "this node could not publish a recovery checkpoint after 61000 ms: abandoned\n"
    )
    # Before task-d51 the line has no loop_ms.
    OLD = (
        "checkpoint represented=65527 retired=true reclaimed=0 took_ms=171 export_ms=77.7 write_ms=82.2 "
        "drain_ms=0.0 sync_ms=3.8 append_ms=0.2 retire_ms=0.6 reclaim_ms=0.0\n"
    )

    def summarize(self, voter):
        with tempfile.TemporaryDirectory() as store:
            os.mkdir(os.path.join(store, "n1"))
            with open(os.path.join(store, "n1", "coordd.log"), "w") as f:
                f.write(voter)
            return js.summarize(store, ["n1"], "TupleSky")

    def test_each_publication_is_parsed(self):
        v = js.parse_voter(self.LOG.splitlines(keepends=True))
        self.assertEqual(v.checkpoints, [(171, 4.5), (240, 12.5)])
        self.assertEqual(v.checkpoints_failed, 1)
        self.assertEqual(js.parse_voter(self.OLD.splitlines(keepends=True)).checkpoints, [(171, None)])

    def test_the_table_reads_the_loop(self):
        text = self.summarize(self.LOG)
        self.assertIn("**Checkpoints**", text)
        self.assertIn("| n1 | 2 | 1 | 8.5 | 12.5 | 1 | 240 |", text)

    def test_a_line_without_loop_ms_leaves_its_columns_empty(self):
        self.assertIn("| n1 | 1 | 0 | - | - | - | 171 |", self.summarize(self.OLD))

    def test_no_publication_no_table(self):
        self.assertNotIn("**Checkpoints**", self.summarize(REPLAY_VOTER))



class RunnerCpuTests(unittest.TestCase):
    """Where the runner's CPU went over the workload, from the sampler's
    rows around it: the first invocation (10:00:01) to the final heal
    (10:00:40), one operation completed in it."""

    SAMPLES = (
        "time,host_busy_s,host_total_s,cpus,servers_s,clients_s,jvm_s,plumbing_s\n"
        "2026-09-27 10:00:00.500000,0.00,0.00,4,0.00,0.00,0.00,0.00\n"
        "2026-09-27 10:00:20.000000,40.00,80.00,4,20.00,4.00,10.00,1.00\n"
        "2026-09-27 10:00:41.500000,82.00,164.00,4,40.00,8.00,20.00,2.00\n"
        "2026-09-27 10:01:50.000000,300.00,440.00,4,90.00,9.00,180.00,3.00\n"
    )

    def summary(self, samples):
        with tempfile.TemporaryDirectory() as store:
            with open(os.path.join(store, "jepsen.log"), "w") as f:
                f.write(LOG)
            with open(os.path.join(store, "results.edn"), "w") as f:
                f.write(RESULTS)
            if samples is not None:
                with open(os.path.join(store, "cpu-samples.csv"), "w") as f:
                    f.write(samples)
            return js.summarize(store, ["n1", "n2"], "Jepsen")

    def test_the_rows_around_the_workload_are_differenced(self):
        text = self.summary(self.SAMPLES)
        self.assertIn("over the 41 s between the samples around the workload, 4 CPUs", text)
        self.assertIn("over the 1 operations completed in that time", text)
        self.assertIn("| the servers under test | 40.0 | 0.98 | 40000.00 |", text)
        self.assertIn("| their Jepsen clients (shims) | 8.0 | 0.20 | 8000.00 |", text)
        self.assertIn("| Jepsen's JVM | 20.0 | 0.49 | 20000.00 |", text)
        self.assertIn("| everything else (the kernel's interrupts included) | 12.0 | 0.29 | 12000.00 |", text)
        self.assertIn("| **the host, busy** | 82.0 | 2.00 | 82000.00 |", text)
        self.assertIn("| idle | 82.0 | 2.00 | - |", text)

    def test_no_samples_no_table(self):
        self.assertNotIn("Runner CPU", self.summary(None))

    def test_samples_that_do_not_bracket_the_workload_are_left_out(self):
        late = self.SAMPLES.splitlines(keepends=True)
        self.assertNotIn("Runner CPU", self.summary(late[0] + "".join(late[3:])))

    def test_a_row_that_does_not_parse_drops_the_table(self):
        self.assertNotIn("Runner CPU", self.summary(self.SAMPLES + "garbage,1,2,3,4,5,6,7\n"))

    def test_steal_is_its_own_row_and_not_everything_else(self):
        samples = "".join(
            line.rstrip("\n") + ("," + steal if i else ",steal_s") + "\n"
            for i, (line, steal) in enumerate(zip(self.SAMPLES.splitlines(), ("", "0.00", "1.00", "5.00", "9.00")))
        )
        text = self.summary(samples)
        self.assertIn("| everything else (the kernel's interrupts included) | 7.0 | 0.17 | 7000.00 |", text)
        self.assertIn("| steal (the VM runnable, its hypervisor running something else) | 5.0 | 0.12 | 5000.00 |", text)
        self.assertIn("| **the host, busy** | 82.0 | 2.00 | 82000.00 |", text)


class LeaderLoopTests(unittest.TestCase):
    """The leader's loop against the host's idle, from the sampler's
    thread rows: two voters, n1's loop the busier in the first second and
    n2's in the second, over the samples around the workload (10:00:00.5
    to 10:00:41.5, with one at 10:00:20 between)."""

    SAMPLES = (
        "time,host_busy_s,host_total_s,cpus,servers_s,clients_s,jvm_s,plumbing_s,steal_s\n"
        "2026-09-27 10:00:00.500000,0.00,0.00,4,0.00,0.00,0.00,0.00,0.00\n"
        "2026-09-27 10:00:20.000000,58.50,78.00,4,20.00,4.00,10.00,1.00,1.95\n"
        "2026-09-27 10:00:41.500000,144.50,164.00,4,40.00,8.00,20.00,2.00,1.95\n"
    )
    THREADS = (
        "time,pid,start,loop_cpu_s,loop_runq_s,workers_cpu_s,workers_runq_s,workers\n"
        "2026-09-27 10:00:00.500000,101,7,1.0,1.0,1.0,1.0,4\n"
        "2026-09-27 10:00:00.500000,102,7,1.0,1.0,1.0,1.0,4\n"
        "2026-09-27 10:00:20.000000,101,7,10.75,20.5,5.0,9.0,4\n"
        "2026-09-27 10:00:20.000000,102,7,2.0,2.0,3.0,3.0,4\n"
        "2026-09-27 10:00:41.500000,101,7,11.75,21.5,7.0,11.0,4\n"
        "2026-09-27 10:00:41.500000,102,7,23.5,2.0,9.0,13.0,4\n"
    )

    def summary(self, threads):
        with tempfile.TemporaryDirectory() as store:
            for name, text in (("jepsen.log", LOG), ("results.edn", RESULTS), ("cpu-samples.csv", self.SAMPLES)):
                with open(os.path.join(store, name), "w") as f:
                    f.write(text)
            if threads is not None:
                with open(os.path.join(store, "cpu-samples-threads.csv"), "w") as f:
                    f.write(threads)
            return js.summarize(store, ["n1", "n2"], "Jepsen")

    def test_the_leader_is_the_busiest_loop_in_each_second(self):
        text = self.summary(self.THREADS)
        self.assertIn("once a second over the 2 seconds between the samples around the workload", text)
        # 19.5 s then 21.5 s: idle 1.00 then 0.00 cores; steal 0.10 then
        # 0; the leader n1 (500 ms/s of CPU, 1000 ms/s queued) then n2
        # (1000 ms/s of CPU, none queued).
        self.assertIn("| host idle (cores) | 0.50 | 0.00 | 1.00 |", text)
        self.assertIn("| steal (cores) | 0.05 | 0.00 | 0.10 |", text)
        self.assertIn("| leader's loop CPU (ms/s) | 750 | 1000 | 500 |", text)
        self.assertIn("| leader's loop run queue (ms/s) | 500 | 0 | 1000 |", text)
        self.assertIn("The voters' tokio threads, the transport's workers and the blocking pool (2 voters, up to 4 each): CPU 14.0 s (0.34 cores, 14000.00 ms per operation), "
                      "run queue 22.0 s (0.54 cores, 22000.00 ms per operation).", text)

    def test_a_correlation_needs_three_seconds(self):
        self.assertIn("second by second: -.", self.summary(self.THREADS))

    def test_no_thread_samples_no_table(self):
        self.assertNotIn("leader's loop and the host's idle", self.summary(None))
        self.assertNotIn("leader's loop and the host's idle", self.summary(self.THREADS + "garbage\n" + "x,1,2,3,4,5,6,7\n"))



def d62_metrics(fast, slow, reasons, oldest_s, uptime_s, traffic=True):
    path, deps, missing, slow_first, unclassified = reasons
    cost = {
        "executed": 1000,
        "busy": {"secs": 1, "nanos": 0},
        "uptime": {"secs": uptime_s, "nanos": 0},
        "recent": {"Unavailable": "NotInstrumented"},
        "established_fast": fast,
        "established_slow": slow,
        "reads": {"served": 0, "refused": 0, "rounds": 0, "confirmed": 0, "waited_ms": 0},
        "fast_path": {"missed_path": path, "missed_deps": deps, "missed_missing": missing,
                      "missed_slow_first": slow_first, "missed_unclassified": unclassified,
                      "acks": 900, "acks_reordered": 180},
        "unordered": {"pending": 3, "reordered": 1, "oldest": {"secs": oldest_s, "nanos": 0}, "leader_log": 7},
        "release": {"commands": 1000, "predecessors": {"secs": 1, "nanos": 760000000},
                    "group": {"secs": 0, "nanos": 900000000}, "projection": {"secs": 7, "nanos": 800000000}},
        "traffic": {"Observed": {"sent_frames": 9960, "sent_bytes": 1, "sent_streams": 3100, "sent_lost": 64,
                                 "sent_lost_streams": 1, "received_frames": 1, "received_bytes": 1,
                                 "received_streams": 1, "datagrams_sent": 5090}}
        if traffic else {"Unavailable": "NotInstrumented"},
    }
    return "metrics " + json.dumps({"stages": [], "cost": {"Observed": cost}}) + "\n"


def d59_metrics(served, loop_ms_per_cmd, resends=None):
    """A voter that executed 1000 commands with its loop's CPU at
    loop_ms_per_cmd each, its read barrier having served `served`."""
    cost = {
        "executed": 1000,
        "busy": {"secs": 1, "nanos": 0},
        "uptime": {"secs": 120, "nanos": 0},
        "recent": {"Unavailable": "NotInstrumented"},
        "reads": {"served": served, "refused": 0, "rounds": served, "confirmed": served, "waited_ms": 0},
        # 1000 commands at loop_ms_per_cmd milliseconds each is that many seconds.
        "cpu": {"Observed": {"domain": {"secs": int(loop_ms_per_cmd), "nanos": round(loop_ms_per_cmd % 1 * 1e9)},
                             "process": {"secs": 2, "nanos": 0}}},
    }
    if resends is not None:
        cost["resends"] = resends
    return "metrics " + json.dumps({"stages": [], "cost": {"Observed": cost}}) + "\n"


class ResendAndFollowerTests(unittest.TestCase):
    def voters(self, *lines):
        return {f"n{i}": js.parse_voter([line]) for i, line in enumerate(lines, 1)}

    def test_the_timer_per_call_on_the_voter_that_ran_it(self):
        voters = self.voters(
            d59_metrics(500, 0.8, {"decided": 3, "acknowledged": 1, "unanswered": 2, "duplicate_votes": 0, "calls": 480,
                                   "scanned": 12000, "time": {"secs": 0, "nanos": 48000000},
                                   "longest": {"secs": 0, "nanos": 1500000}}),
            d59_metrics(0, 0.6, {"decided": 0, "acknowledged": 0, "unanswered": 0, "duplicate_votes": 0, "calls": 0,
                                 "scanned": 0, "time": {"secs": 0, "nanos": 0}, "longest": {"secs": 0, "nanos": 0}}),
        )
        text = "\n".join(js.resend_table(voters))
        self.assertIn("| n1 | 480 | 0.100 | 1.50 | 25.0 | 3 / 1 / 2 |", text)
        self.assertNotIn("| n2 |", text)
        # A build before task-d59 has no timer to show.
        self.assertEqual(js.resend_table(self.voters(d59_metrics(500, 0.8, {"decided": 1, "acknowledged": 0,
                                                                          "unanswered": 0, "duplicate_votes": 0}))), [])

    def test_the_leader_beside_a_follower_by_phase(self):
        voters = self.voters(d59_metrics(500, 0.8), d59_metrics(0, 0.6), d59_metrics(0, 0.6))
        run = "main;coordd::serve::Domain<P>::run::{{closure}}"
        with tempfile.TemporaryDirectory() as store:
            with open(os.path.join(store, "leader-profile-chains.txt"), "w") as f:
                f.write("leader\n    100.00%  coordd  [.] x\n"
                        f"50.00% {run};coord_daemon::node::Node<P>::settle;x\n"
                        f"50.00% {run};coord_daemon::node::Machine::resend_unvoted;x\n")
            with open(os.path.join(store, "follower-profile-chains.txt"), "w") as f:
                f.write("follower\n    100.00%  coordd  [.] x\n"
                        f"100.00% {run};coord_daemon::node::Node<P>::settle;x\n")
            with open(os.path.join(store, "leader-profile-alloc-chains.txt"), "w") as f:
                f.write("leader dwarf\n    10.00%  libc.so.6  [.] malloc\n"
                        f"10.00% {run};coord_daemon::voter::Voter<P>::pump_reads;__rust_alloc;malloc\n")
            text = "\n".join(js.leader_and_follower(store, voters))
        # Leader 800 µs a command, half each; the follower 600, all settle.
        self.assertIn("| `coord_daemon::node::Node<P>::settle` | 400.0 | 600.0 | -200.0 |", text)
        self.assertIn("| `coord_daemon::node::Machine::resend_unvoted` | 400.0 | 0.0 | +400.0 |", text)
        self.assertIn("| **all** | 800.0 | 600.0 | +200.0 |", text)
        self.assertIn("| `coord_daemon::voter::Voter<P>::pump_reads` | 80.0 |", text)


    def test_the_sampled_follower_is_costed_at_its_own_loop(self):
        # n3 is slower than n2: the profile sampled n3, so its phases are
        # costed at its 900 µs, not the followers' mean of 750.
        voters = self.voters(d59_metrics(500, 0.8), d59_metrics(0, 0.6), d59_metrics(0, 0.9))
        run = "main;coordd::serve::Domain<P>::run::{{closure}}"
        with tempfile.TemporaryDirectory() as store:
            with open(os.path.join(store, "leader-profile-chains.txt"), "w") as f:
                f.write("leader thread 1 (n1), 0.30 of a core\n    100.00%  coordd  [.] x\n"
                        f"100.00% {run};coord_daemon::node::Node<P>::settle;x\n")
            with open(os.path.join(store, "follower-profile-chains.txt"), "w") as f:
                f.write("follower thread 3 (n3), 0.25 of a core over the 5 s before\n    100.00%  coordd  [.] x\n"
                        f"100.00% {run};coord_daemon::node::Node<P>::settle;x\n")
            self.assertEqual(js.profiled_node(os.path.join(store, "follower-profile-chains.txt")), "n3")
            text = "\n".join(js.leader_and_follower(store, voters))
        self.assertIn("900 on the sampled follower, n3", text)
        self.assertIn("| `coord_daemon::node::Node<P>::settle` | 800.0 | 900.0 | -100.0 |", text)

    def test_the_sampled_leader_is_costed_at_its_own_loop(self):
        # n1 served the reads at the end, but the profile sampled n2 while
        # it led: the leader's phases are costed at n2's 600 µs.
        voters = self.voters(d59_metrics(500, 0.8), d59_metrics(100, 0.6), d59_metrics(0, 0.9))
        run = "main;coordd::serve::Domain<P>::run::{{closure}}"
        with tempfile.TemporaryDirectory() as store:
            with open(os.path.join(store, "leader-profile.txt"), "w") as f:
                f.write("leader thread 2 (n2), 0.30 of a core over the 5 s before; Samples: 5K\n")
            with open(os.path.join(store, "leader-profile-chains.txt"), "w") as f:
                f.write("leader thread 2 (n2), 0.30 of a core\n    100.00%  coordd  [.] x\n"
                        f"100.00% {run};coord_daemon::node::Node<P>::settle;x\n")
            with open(os.path.join(store, "follower-profile-chains.txt"), "w") as f:
                f.write("follower thread 3 (n3), 0.25 of a core\n    100.00%  coordd  [.] x\n"
                        f"100.00% {run};coord_daemon::node::Node<P>::settle;x\n")
            text = "\n".join(js.leader_and_follower(store, voters))
        self.assertIn("| `coord_daemon::node::Node<P>::settle` | 600.0 | 900.0 | -300.0 |", text)


class MemoryAndCopiesTests(unittest.TestCase):
    def test_each_voter_at_its_end_and_its_peak(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "cpu-samples-memory.csv")
            with open(path, "w") as f:
                f.write("time,pid,start,rss_kib,hwm_kib\n"
                        "2026-10-07 10:00:00.000000,11,1,102400,102400\n"
                        "2026-10-07 10:00:00.000000,12,1,153600,153600\n"
                        "2026-10-07 10:00:01.000000,11,1,133120,204800\n"
                        "2026-10-07 10:00:01.000000,12,1,174080,174080\n")
            samples = js.read_memory(path)
        text = "\n".join(js.voters_memory(samples))
        # pid 11: 130 MiB at the end, 130 the largest sampled, 200 its high-water mark.
        self.assertIn("| 11 | 2 | 130 | 130 | 200 |", text)
        self.assertEqual(js.memory_summary(samples), {"processes": 2, "end_mean": 150.0, "end_max": 170.0, "hwm_max": 200.0})
        self.assertEqual(js.voters_memory({}), [])

    def test_memcmp_and_memmove_by_caller_and_what_a_phase_calls(self):
        run = "main;coordd::serve::Domain<P>::run::{{closure}}"
        flush = run + ";coordd::serve::Domain<P>::stops_on_flush"
        chains = js.read_chains_from_lines([
            "header",
            "    30.00%  libc.so.6  [.] __memcmp_avx2_movbe",
            f"30.00% {flush};coord_consensus::leader::Leader::propose;alloc::collections::btree::search::search_tree (inlined);__memcmp_avx2_movbe",
            "    20.00%  libc.so.6  [.] __memmove_avx_unaligned_erms",
            f"20.00% {flush};coord_journal_api::record::JournalRecordV1::encode;alloc::raw_vec::finish_grow;__memmove_avx_unaligned_erms",
            "    50.00%  coordd  [.] mi_malloc",
            f"50.00% {flush};mi_malloc",
        ])
        text = "\n".join(js.copies_table(chains, 10.0))
        # 30% and 20% of a 1000 µs loop.
        self.assertIn("| `coord_consensus::leader::Leader::propose` | `alloc::collections::btree::search::search_tree` | 300.0 | 0.0 |", text)
        self.assertIn("| `coord_journal_api::record::JournalRecordV1::encode` | `alloc::raw_vec::finish_grow` | 0.0 | 200.0 |", text)
        # mimalloc counts as the allocator.
        self.assertAlmostEqual(js.loop_split(chains)["alloc"], 50.0)
        children = "\n".join(js.children_table(chains, 10.0, "follower"))
        self.assertIn("| `coordd::serve::Domain<P>::stops_on_flush` | **all** | **1000.0** |", children)
        self.assertIn("| | `coord_consensus::leader::Leader::propose` | 300.0 |", children)
        self.assertIn("| | `(in mi_malloc)` | 500.0 |", children)


TRANSPORT_SYSCALLS = """leader process 100, each thread's CPU and system calls, counted from 2026-10-08 10:00:00 to 2026-10-08 10:00:20 UTC (20 s); perf stat -x, --per-thread
coordd-100,1000.00,msec,task-clock,1000000000,100.00,0.050,CPUs utilized
tokio-rt-worker-101,1500.00,msec,task-clock,1500000000,100.00,0.075,CPUs utilized
tokio-rt-worker-102,500.00,msec,task-clock,500000000,100.00,0.025,CPUs utilized
appender-103,100.00,msec,task-clock,100000000,100.00,0.005,CPUs utilized
coordd-100,2000,,syscalls:sys_enter_futex,1000000000,100.00,100.000,/sec
tokio-rt-worker-101,6000,,syscalls:sys_enter_futex,1500000000,100.00,300.000,/sec
tokio-rt-worker-102,4000,,syscalls:sys_enter_futex,500000000,100.00,200.000,/sec
appender-103,500,,syscalls:sys_enter_futex,100000000,100.00,25.000,/sec
tokio-rt-worker-101,3000,,syscalls:sys_enter_sendmsg,1500000000,100.00,150.000,/sec
tokio-rt-worker-102,1000,,syscalls:sys_enter_sendmmsg,500000000,100.00,50.000,/sec
tokio-rt-worker-101,2000,,syscalls:sys_enter_recvmsg,1500000000,100.00,100.000,/sec
"""

TRANSPORT_CHAINS = [
    "header",
    "    40.00%  [kernel.kallsyms]  [k] _raw_spin_unlock_irqrestore",
    "8.00% start_thread;tokio::runtime::task::harness::Harness<T,S>::poll;quinn::connection::State::drive_transmit;"
    "quinn_proto::connection::Connection::poll_transmit;__libc_sendmsg;__x64_sys_sendmsg;udp_sendmsg;_raw_spin_unlock_irqrestore",
    "12.00% start_thread;tokio::runtime::scheduler::multi_thread::worker::Context::park_timeout;"
    "tokio::runtime::scheduler::multi_thread::park::Parker::park;std::sys::sync::condvar::futex::Condvar::wait_timeout;"
    "syscall;__x64_sys_futex;futex_wait;_raw_spin_unlock_irqrestore",
    "20.00% start_thread;coord_transport::endpoint::Transport::deliver;tokio::sync::mpsc::chan::Tx<T,S>::send;"
    "tokio::runtime::park::Inner::unpark;syscall;__x64_sys_futex;futex_wake;_raw_spin_unlock_irqrestore",
    "    30.00%  coordd  [.] aes_gcm_encrypt_avx512",
    "30.00% start_thread;quinn_proto::connection::Connection::poll_transmit;ring::aead::seal;aes_gcm_encrypt_avx512",
    "    20.00%  coordd  [.] quinn_proto::frame::Ack::encode",
    "10.00% start_thread;quinn_proto::connection::Connection::poll_transmit;quinn_proto::frame::Ack::encode",
    "10.00% start_thread;quinn_proto::connection::Connection::handle_event;quinn_proto::frame::Ack::encode",
    "    10.00%  coordd  [.] coord_transport::wire::decode",
    "10.00% start_thread;tokio::runtime::task::harness::Harness<T,S>::poll;coord_transport::wire::decode",
]


class TransportTests(unittest.TestCase):
    def test_the_leaders_threads_per_command(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile-syscalls.txt")
            with open(path, "w") as f:
                f.write(TRANSPORT_SYSCALLS)
            kinds, commands = js.syscalls_per_command(path, 500.0)
        # The loop's 1000 ms over its 500 µs per command: 2000 commands.
        self.assertAlmostEqual(commands, 2000.0)
        n, cpu, calls = kinds["tokio threads"]
        self.assertEqual(n, 2)
        self.assertAlmostEqual(cpu, 1000.0)
        self.assertAlmostEqual(calls["futex"], 5.0)
        self.assertAlmostEqual(calls["sends"], 2.0)
        self.assertAlmostEqual(calls["receives"], 1.0)
        self.assertAlmostEqual(kinds["domain loop"][1], 500.0)
        self.assertAlmostEqual(kinds["`appender`"][2]["futex"], 0.25)
        self.assertEqual(js.syscalls_per_command(os.path.join("/nonexistent", "x"), 500.0), ({}, None))

    def test_task_clock_in_nanoseconds(self):
        # The runner's perf gives task-clock in ns, where 6.8 gave msec.
        text = TRANSPORT_SYSCALLS.replace("1000.00,msec,task-clock", "1000000000,,task-clock").replace(
            "1500.00,msec,task-clock", "1500000000,,task-clock").replace(
            "500.00,msec,task-clock", "500000000,,task-clock").replace(
            # A thread that ran 10 ms: as small as milliseconds would be,
            # read by the file's unit, not its own size.
            "100.00,msec,task-clock", "10000000,,task-clock")
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile-syscalls.txt")
            with open(path, "w") as f:
                f.write(text)
            kinds, commands = js.syscalls_per_command(path, 500.0)
        self.assertAlmostEqual(commands, 2000.0)
        self.assertAlmostEqual(kinds["tokio threads"][2]["futex"], 5.0)
        self.assertAlmostEqual(kinds["tokio threads"][1], 1000.0)
        self.assertAlmostEqual(kinds["`appender`"][1], 5.0)

    def test_calls_perf_did_not_count_are_said_not_zero(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile-syscalls.txt")
            with open(path, "w") as f:
                f.write(TRANSPORT_SYSCALLS.splitlines()[0] + "\n"
                        "coordd-100,1000.00,msec,task-clock,1000000000,100.00,0.050,CPUs utilized\n"
                        "coordd-100,<not supported>,,syscalls:sys_enter_futex,0,100.00,,\n"
                        "tokio-rt-worker-101,<not supported>,,syscalls:sys_enter_futex,0,100.00,,\n")
            kinds, _ = js.syscalls_per_command(path, 500.0)
            note = js.syscall_note(path)
        self.assertIsNone(kinds["domain loop"][2]["futex"])
        self.assertEqual(note[0], "perf counted none of these system calls: `futex` (<not supported>)")

    def test_the_tokio_threads_by_what_they_did(self):
        chains = js.read_chains_from_lines(TRANSPORT_CHAINS)
        kinds, parking = js.transport_split(chains)
        # The send under quinn's transmit is the send; the cipher under it crypto.
        self.assertAlmostEqual(kinds["send (`sendmsg` and the kernel's UDP send)"], 8.0)
        self.assertAlmostEqual(kinds["crypto (packet protection)"], 30.0)
        self.assertAlmostEqual(kinds["parking and waking (futex, epoll, the I/O driver's waker)"], 32.0)
        # A frame's caller above it names the QUIC kind, not the leaf alone.
        self.assertAlmostEqual(kinds["QUIC transmit (quinn's packet building)"], 10.0)
        self.assertAlmostEqual(kinds["QUIC receive (quinn's packet handling)"], 10.0)
        self.assertAlmostEqual(kinds["TupleSky: `coord_transport`"], 10.0)
        self.assertAlmostEqual(parking["tokio::runtime::park::Inner::unpark"], 20.0)
        self.assertAlmostEqual(parking["tokio::runtime::scheduler::multi_thread::park::Parker::park"], 12.0)

    def test_the_tables_in_microseconds_per_command(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "leader-profile-syscalls.txt"), "w") as f:
                f.write(TRANSPORT_SYSCALLS)
            with open(os.path.join(d, "transport-profile-chains.txt"), "w") as f:
                f.write("\n".join(TRANSPORT_CHAINS) + "\n")
            voters = {"n1": js.parse_voter([d59_metrics(4, 0.5)])}
            text = "\n".join(js.transport_tables(d, voters))
        # The file has no epoll or write lines: not counted, not none.
        self.assertIn("| domain loop | 1 | 500.0 | 1.00 | 0.00 | 0.00 | - | - |", text)
        self.assertIn("| tokio threads | 2 | 1000.0 | 5.00 | 2.00 | 1.00 | - | - |", text)
        # 30% of the tokio threads' 1000 µs per command.
        self.assertIn("| crypto (packet protection) | 300.0 |", text)
        self.assertIn("| `tokio::runtime::park::Inner::unpark` | 200.0 |", text)


class FastPathTests(unittest.TestCase):
    def summary(self, n1, n2):
        with tempfile.TemporaryDirectory() as store:
            for name, text in (("jepsen.log", LOG), ("results.edn", RESULTS)):
                with open(os.path.join(store, name), "w") as f:
                    f.write(text)
            for node, text in (("n1", n1), ("n2", n2)):
                os.makedirs(os.path.join(store, node))
                with open(os.path.join(store, node, "coordd.log"), "w") as f:
                    f.write(text)
            return js.summarize(store, ["n1", "n2"], "Jepsen")

    def test_reasons_held_release_and_traffic(self):
        text = self.summary(
            d62_metrics(40, 960, (900, 20, 30, 10, 0), 12, 60) + d62_metrics(40, 960, (900, 20, 30, 10, 0), 2, 120),
            d62_metrics(0, 1000, (900, 20, 30, 10, 0), 0, 120, traffic=False),
        )
        self.assertIn("| n1 | 40 | 960 | 900 / 20 / 30 / 10 / 0 | yes | 900 (180) | 3 (1) | 2.0 / 12.0 | 7 "
                      "| 1.76 / 0.90 / 7.80 | 9.96 / 3.10 / 5.09 | 3.21 | 64 (1) |", text)
        self.assertIn("| n2 | 0 | 1000 | 900 / 20 / 30 / 10 / 0 | **no** (960 of 1000) |", text)
        self.assertNotIn("no voter established", text)

    def test_a_run_with_no_fast_decision_is_a_finding(self):
        line = d62_metrics(0, 960, (900, 20, 30, 10, 0), 0, 60)
        self.assertIn("**Finding: no voter established a command on the fast path** (1920 slow, 0 fast).",
                      self.summary(line, line))


class LeaderProfileTests(unittest.TestCase):
    def test_the_header_objects_and_symbols(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile.txt")
            with open(path, "w") as f:
                f.write(
                    "leader thread 7, 0.28 of a core over the 5 s before; Samples: 5K\n"
                    "by object: coordd 61.20%, libc.so.6 22.10%\n"
                    "     7.17%  coordd  [.] coord_consensus::leader::Leader::resend_unvoted\n"
                    "     1.81%  [kernel.kallsyms]  [k] irqentry_exit_to_user_mode\n"
                )
            text = "\n".join(js.leader_profile(path))
        self.assertIn("leader thread 7, 0.28 of a core", text)
        self.assertIn("Its samples by object: coordd 61.20%, libc.so.6 22.10%.", text)
        self.assertIn("| 7.17% | `coordd` | `coord_consensus::leader::Leader::resend_unvoted` |", text)
        self.assertIn("| 1.81% | `[kernel.kallsyms]` | `irqentry_exit_to_user_mode` |", text)

    def test_a_call_graph_gives_shares_with_callees(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile-inclusive.txt")
            with open(path, "w") as f:
                f.write("leader thread 7, call graph\n    60.00%     2.00%  coordd  [.] coordd::serve::Domain<P>::run\n")
            text = "\n".join(js.leader_profile_inclusive(path))
        self.assertIn("| 60.00% | 2.00% | `coordd` | `coordd::serve::Domain<P>::run` |", text)
        self.assertEqual(js.leader_profile_inclusive(os.path.join(d, "none.txt")), [])

    def test_no_profile_no_block(self):
        self.assertEqual(js.leader_profile("/nonexistent/leader-profile.txt"), [])
        self.assertEqual(js.leader_loop_split("/nonexistent/leader-profile-chains.txt"), [])

    def test_folded_stacks_split_by_phase_and_allocator_caller(self):
        run = "main;tokio::runtime::Runtime::block_on;coordd::serve::Domain<P>::run::{{closure}}"
        turn = run + ";coordd::serve::Domain<P>::turn::{{closure}} (inlined)"
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "leader-profile-chains.txt")
            with open(path, "w") as f:
                f.write(
                    "leader thread 7, call graph; each symbol, then its stacks\n"
                    "    40.00%  coordd  [.] coord_consensus::leader::Leader::resend_unvoted\n"
                    f"40.00% {turn};coord_daemon::voter::Voter<P>::resend_proposals (inlined);"
                    "coord_consensus::leader::Leader::resend_unvoted\n"
                    "    30.00%  libc.so.6  [.] malloc\n"
                    f"20.00% {turn};coord_daemon::voter::Voter<P>::pump_reads;"
                    "coord_daemon::reads::Reads::due;alloc::vec::Vec<T>::push;__rust_alloc;malloc\n"
                    f"10.00% {run};coordd::serve::Domain<P>::carry;<alloc::vec::Vec<T> as core::clone::Clone>::clone;malloc\n"
                    "    20.00%  libc.so.6  [.] 0x00000000001621f8\n"
                    f"15.00% {turn};coord_daemon::voter::Voter<P>::pump_reads;alloc::raw_vec::finish_grow;"
                    "alloc::alloc::realloc (inlined);0x00000000001621f8\n"
                    "5.00% 0x00000000001621f8\n"
                    "    10.00%  libc.so.6  [.] unlink_chunk.isra.0\n"
                    f"10.00% {turn};coord_daemon::voter::Voter<P>::pump_reads;free;_int_free;unlink_chunk.isra.0\n"
                )
            split = js.loop_split(js.read_chains(path))
            text = "\n".join(js.leader_loop_split(path))
        self.assertAlmostEqual(split["reached"], 95.0)
        # glibc's internals count, by their bare names.
        self.assertAlmostEqual(split["alloc"], 55.0)
        self.assertAlmostEqual(split["phases"]["coord_daemon::voter::Voter<P>::pump_reads"], 45.0)
        self.assertAlmostEqual(split["owners"]["coord_daemon::reads::Reads::due"], 20.0)
        # A Rust allocator frame marks an unresolved libc address as allocation.
        self.assertAlmostEqual(split["owners"]["coord_daemon::voter::Voter<P>::pump_reads"], 25.0)
        self.assertIn("The stacks reached the loop in 95.0% of 100.0%.", text)
        self.assertIn("| `coord_daemon::voter::Voter<P>::resend_proposals` | 40.00% | 0.00% |", text)
        self.assertIn("| `coordd::serve::Domain<P>::carry` | 10.00% | 10.00% |", text)
        self.assertIn("| `(the stack did not unwind to the loop)` | 5.00% | 0.00% |", text)
        self.assertIn("| `coord_daemon::reads::Reads::due` | 20.00% |", text)
