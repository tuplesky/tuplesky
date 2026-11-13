import datetime
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))

import jepsen_key_groups as kg  # noqa: E402
import jepsen_summary as js  # noqa: E402

T0 = datetime.datetime(2026, 11, 13, 10, 0, 0)


def invoke(ms, thread, key, f=":write"):
    return js.Op(T0 + datetime.timedelta(milliseconds=ms), f"jepsen worker {thread}", str(thread), "invoke", f, f"[{key} 1]", "")


def run(groups, size, keys_a_group, ops_a_key, last_ms=60_000, drop=None, stop=None):
    """Invocations of `groups` groups of `size` threads, each group running
    `keys_a_group` keys of `ops_a_key` operations, its threads in turn, the
    group's keys numbered from the group (key = group + groups × i), spread
    over `last_ms`. `drop(group, key_index)` names threads left off a key;
    `stop` is {thread: ms} after which a thread invokes no more. Threads below
    5 of group 0 read."""
    ops = []
    for g in range(groups):
        threads = list(range(g * size, (g + 1) * size))
        for i in range(keys_a_group):
            key = g + groups * i
            left = drop(g, i) if drop else set()
            mine = [t for t in threads if t not in left]
            for j in range(ops_a_key):
                ms = (i * ops_a_key + j) * last_ms // (keys_a_group * ops_a_key)
                t = mine[j % len(mine)]
                if stop and t in stop and ms > stop[t]:
                    continue
                ops.append(invoke(ms, t, key, ":read" if t < 5 else ":write"))
    ops.sort(key=lambda o: o.at)
    return ops


class KeyGroupTests(unittest.TestCase):
    def test_whole_groups_pass(self):
        g = kg.check("head@30", run(3, 10, 20, 100))
        self.assertFalse(g.failed)
        self.assertEqual((g.threads, g.size, g.keys, g.finished), (30, 10, 60, 57))
        self.assertEqual(g.readers, {0: {0, 1, 2, 3, 4}})
        self.assertIn("| ok |", kg.line(g))
        self.assertIn("group 0: 0-4", kg.line(g))

    def test_a_key_short_of_its_group_fails(self):
        # Group 2's fifth key ran on threads 28 and 29 alone.
        g = kg.check("head@30", run(3, 10, 20, 100, drop=lambda grp, i: set(range(20, 28)) if (grp, i) == (2, 4) else set()))
        self.assertTrue(g.failed)
        self.assertEqual(g.short, [(14, 2, 2)])
        self.assertIn("key 14 (group 2) on 2 of 10", kg.line(g))
        self.assertIn("**fails**", kg.line(g))

    def test_a_small_key_short_is_reported_not_failed(self):
        g = kg.check("swiftpaxos@30", run(3, 10, 20, 20, drop=lambda grp, i: {29} if (grp, i) == (2, 4) else set()))
        self.assertFalse(g.failed)
        self.assertEqual(g.short_small, 1)
        self.assertIn("1 under 5 a thread on fewer (not counted)", kg.line(g))

    def test_a_group_that_stops_fails(self):
        # Threads 20-29 hand in nothing after 1 s: the third group stopped.
        g = kg.check("head@30", run(3, 10, 20, 100, stop={t: 1000 for t in range(20, 30)}))
        self.assertTrue(g.failed)
        self.assertEqual([t for t, _ in g.stopped], list(range(20, 30)))
        self.assertIn("20 (59 s before the end)", kg.line(g))

    def test_unkeyed_runs_are_skipped(self):
        ops = [js.Op(T0, "jepsen worker 0", "0", "invoke", ":txn", "[[:append 1 2]]", "")]
        g = kg.check("head@30", ops)
        self.assertFalse(g.failed)
        self.assertEqual(g.note, "not keyed")

    def test_cli_exit_status(self):
        def log_of(ops):
            return "".join(
                f"{o.at:%Y-%m-%d %H:%M:%S},{o.at.microsecond // 1000:03d}{{GMT}}\tINFO\t[{o.thread}] jepsen.util: "
                f"{o.process}\t:{o.type}\t{o.f}\t{o.value}\n"
                for o in ops
            )

        with tempfile.TemporaryDirectory() as tmp:
            good, bad = os.path.join(tmp, "good"), os.path.join(tmp, "bad")
            for d, ops in ((good, run(3, 10, 5, 100)), (bad, run(3, 10, 5, 100, stop={t: 1000 for t in range(20, 30)}))):
                os.makedirs(d)
                with open(os.path.join(d, "jepsen.log"), "w") as f:
                    f.write(log_of(ops))
            script = os.path.join(os.path.dirname(__file__), "jepsen_key_groups.py")
            env = {k: v for k, v in os.environ.items() if k != "GITHUB_STEP_SUMMARY"}
            ok = subprocess.run([sys.executable, script, f"head@30={good}"], capture_output=True, text=True, env=env)
            self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
            self.assertIn("| head@30 | 30 | 10 × 3 | 12 of 15 |", ok.stdout)
            no = subprocess.run(
                [sys.executable, script, f"head@30={good}", f"etcd#1={bad}"], capture_output=True, text=True, env=env
            )
            self.assertEqual(no.returncode, 1, no.stdout + no.stderr)
            self.assertIn("| etcd#1 |", no.stdout)


if __name__ == "__main__":
    unittest.main()
