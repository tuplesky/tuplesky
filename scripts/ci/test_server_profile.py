import unittest

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
            "[k]__x64_sys_futex;runtime.futex;[unknown]",
        ])

    def test_a_last_sample_without_its_blank_line(self):
        self.assertEqual(sp.fold("\t 1 a\n\t 2 b"), ["a;b"])


if __name__ == "__main__":
    unittest.main()
