"""Static safety checks for the detached v5 infrastructure launcher."""

from __future__ import annotations

from pathlib import Path
import re
import unittest

from importlib.util import module_from_spec, spec_from_file_location


ROOT = Path(__file__).resolve().parent
CONTROLLER = ROOT / ".github/scripts/fresh-xauusd-v5-controller.sh"
WORKFLOW = ROOT / ".github/workflows/fresh-xauusd-v5-detached-launch.yml"
EXTRACTOR = ROOT / ".github/scripts/extract_workflow_run.py"
KNOWN_HOSTS = ROOT / ".github/ssh/fresh-xauusd-ec2-known-hosts"
LAUNCH_STEP = "Launch detached v5 controller and obtain immutable receipt"


def _load_extractor():
    specification = spec_from_file_location("fresh_v5_extractor", EXTRACTOR)
    if specification is None or specification.loader is None:
        raise RuntimeError("could not load workflow extractor")
    module = module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _python_heredocs(shell_source: str) -> tuple[str, ...]:
    lines = shell_source.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        if "<<'PY'" not in lines[index]:
            index += 1
            continue
        start = index + 1
        end = start
        while end < len(lines) and lines[end] != "PY":
            end += 1
        if end >= len(lines):
            raise AssertionError("unterminated Python heredoc")
        blocks.append("\n".join(lines[start:end]) + "\n")
        index = end + 1
    return tuple(blocks)


class FreshV5LauncherTests(unittest.TestCase):
    def test_all_embedded_python_is_syntactically_valid(self) -> None:
        controller = CONTROLLER.read_text(encoding="utf-8")
        workflow_shell = _load_extractor().extract_run_block(
            WORKFLOW,
            LAUNCH_STEP,
        )
        blocks = (*_python_heredocs(controller), *_python_heredocs(workflow_shell))
        self.assertGreaterEqual(len(blocks), 10)
        for index, source in enumerate(blocks):
            compile(source, f"<fresh-v5-heredoc-{index}>", "exec")

    def test_shell_continuations_and_descriptor_detachment_are_explicit(self) -> None:
        controller = CONTROLLER.read_text(encoding="utf-8")
        workflow_shell = _load_extractor().extract_run_block(
            WORKFLOW,
            LAUNCH_STEP,
        )
        for source in (controller, workflow_shell):
            self.assertIsNone(re.search(r"!=[ \t]*\n", source))
        self.assertIn("nohup setsid --fork", workflow_shell)
        self.assertIn("</dev/null", workflow_shell)
        self.assertIn('>"${controller_stdout}"', workflow_shell)
        self.assertIn('2>"${controller_stderr}"', workflow_shell)
        self.assertIn('>"${log_path}" 2>&1 &', controller)

    def test_finalizer_waits_and_durable_archive_is_fsynced(self) -> None:
        controller = CONTROLLER.read_text(encoding="utf-8")
        finalizer = controller[
            controller.index("finalize() {") : controller.index(
                'if [[ "${branch}"',
            )
        ]
        self.assertLess(
            finalizer.index('wait "${pipeline_pid}"'),
            finalizer.index('tar -C "${output}"'),
        )
        self.assertIn('fsync_regular_and_parent "${terminal_archive}"', finalizer)
        self.assertIn(
            'artifact_root="/home/ec2-user/.local/state/datavis/'
            'fresh-xauusd-artifacts-v1"',
            controller,
        )
        self.assertIn('exec 9<"${launch_root_resolved}"', controller)

    def test_ec2_host_key_and_run19_input_are_pinned(self) -> None:
        known_hosts = KNOWN_HOSTS.read_text(encoding="utf-8")
        self.assertEqual(
            known_hosts,
            (
                "datavis.au,www.datavis.au,3.27.110.195,"
                "ec2-3-27-110-195.ap-southeast-2.compute.amazonaws.com "
                "ssh-ed25519 "
                "AAAAC3NzaC1lZDI1NTE5AAAAIJaLbSMHMPiqscPzaqqsOoa41AKxQseBtEWVngSLj6nk\n"
            ),
        )
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("StrictHostKeyChecking=yes", workflow)
        self.assertNotIn("StrictHostKeyChecking=accept-new", workflow)
        self.assertIn(
            "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
