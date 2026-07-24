import unittest

from datavis.research.fresh_run19_adoption import (
    PIPELINE_MODULE,
    RUN_ATTEMPT,
    RUN_BRANCH,
    RUN_ID,
    RUN_SHA,
    is_target_parent,
    is_target_pipeline,
    option_once,
    parse_process_stat,
)


class FreshRun19AdoptionTests(unittest.TestCase):
    def test_parses_proc_stat_with_parentheses_in_name(self):
        fields = [
            "R",
            "41",
            "42",
            "43",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "1",
            "2",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "98765",
        ]
        parsed = parse_process_stat(
            "123 (python worker (sealed)) " + " ".join(fields)
        )
        self.assertEqual(parsed["state"], "R")
        self.assertEqual(parsed["ppid"], 41)
        self.assertEqual(parsed["pgrp"], 42)
        self.assertEqual(parsed["session"], 43)
        self.assertEqual(parsed["start_ticks"], 98765)

    def test_option_once_rejects_duplicate(self):
        arguments = ("python", "--output-dir", "a", "--output-dir", "b")
        self.assertIsNone(option_once(arguments, "--output-dir"))

    def test_exact_pipeline_shape(self):
        arguments = (
            "/tmp/fresh-xauusd-worktree.abc123/.fresh-venv/bin/python",
            "-m",
            PIPELINE_MODULE,
            "--repository-root",
            "/tmp/fresh-xauusd-worktree.abc123",
            "--output-dir",
            "/tmp/fresh-xauusd-output.abc123",
            "--scratch-dir",
            "/state/run.30042880650.1.abc123",
            "--research-state-dir",
            "/state",
            "--restart-v4-artifact-dir",
            "/tmp/fresh-xauusd-restart.abc123",
            "--execute",
        )
        self.assertTrue(is_target_pipeline(arguments))
        self.assertFalse(is_target_pipeline(arguments + ("--extra",)))

    def test_exact_parent_shape(self):
        arguments = (
            "bash",
            "-s",
            "--",
            RUN_BRANCH,
            RUN_SHA,
            RUN_ID,
            RUN_ATTEMPT,
            "/tmp/fresh-xauusd-transfer.abc123/run17-restart.tgz",
            (
                "/tmp/fresh-xauusd-transfer.abc123/"
                "fresh-xauusd-30042880650-1.tgz"
            ),
        )
        self.assertTrue(is_target_parent(arguments))
        changed = list(arguments)
        changed[4] = "0" * 40
        self.assertFalse(is_target_parent(tuple(changed)))


if __name__ == "__main__":
    unittest.main()
