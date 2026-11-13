import subprocess
import unittest
from unittest import mock

import server_profile as sp


class FoldTests(unittest.TestCase):
    def test_a_sample_a_line_leaf_first_with_the_kernel_marked(self):
        text = (
            "\t          4a1b2c runtime.mallocgc+0x1f\n"
            "\t          4a2000 go.etcd.io/raft/v3.(*raft).Step\n"
            "\n"
            "\tffffffff81000000 __x64_sys_futex+0x10\n"
            "\t          455000 runtime.futex\n"
            "\t          0 [unknown]\n"
            "\n"
        )
        self.assertEqual(sp.fold(text), [
            "runtime.mallocgc;go.etcd.io/raft/v3.(*raft).Step",
            "[k]__x64_sys_futex;runtime.futex;0x0",
        ])

    def test_a_last_sample_without_its_blank_line(self):
        self.assertEqual(sp.fold("\t 1 a\n\t 2 b"), ["a;b"])


class SymbolizeTests(unittest.TestCase):
    """etcd's release binary is stripped: its frames come as addresses,
    named from the binary's own Go function table."""

    def run_with(self, stdout):
        done = subprocess.CompletedProcess([], 0, stdout=stdout, stderr="")
        with mock.patch.object(sp.shutil, "which", return_value="/usr/bin/go"), \
                mock.patch.object(sp.subprocess, "run", return_value=done) as run:
            out = sp.symbolize(["0x4a1b2c;0x4a2000", "[k]do_syscall_64;0x4a1b2c"], "/tmp/etcd")
        return out, run

    def test_addresses_are_named_once_each_and_the_kernel_is_left_alone(self):
        out, run = self.run_with(
            "runtime.mallocgc\nruntime/malloc.go:1\ngo.etcd.io/raft/v3.(*raft).Step\nraft.go:2\n")
        self.assertEqual(out, ["runtime.mallocgc;go.etcd.io/raft/v3.(*raft).Step",
                               "[k]do_syscall_64;runtime.mallocgc"])
        self.assertEqual(run.call_args.kwargs["input"], "0x4a1b2c\n0x4a2000\n")
        self.assertEqual(run.call_args.args[0], ["/usr/bin/go", "tool", "addr2line", "/tmp/etcd"])

    def test_a_frame_it_cannot_name_stays_an_address(self):
        out, _ = self.run_with("?\n?:0\ngo.etcd.io/raft/v3.(*raft).Step\nraft.go:2\n")
        self.assertEqual(out[0], "0x4a1b2c;go.etcd.io/raft/v3.(*raft).Step")

    def test_without_go_nothing_changes(self):
        with mock.patch.object(sp.shutil, "which", return_value=None), \
                mock.patch.object(sp.glob, "glob", return_value=[]):
            self.assertEqual(sp.symbolize(["0x1;0x2"], "/tmp/etcd"), ["0x1;0x2"])


if __name__ == "__main__":
    unittest.main()
