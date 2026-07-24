"""Extract one literal GitHub Actions ``run: |`` block for shell parsing."""

from __future__ import annotations

from pathlib import Path
import sys


def extract_run_block(workflow_path: Path, step_name: str) -> str:
    lines = workflow_path.read_text(encoding="utf-8").splitlines()
    marker = f"      - name: {step_name}"
    positions = [index for index, line in enumerate(lines) if line == marker]
    if len(positions) != 1:
        raise ValueError("workflow step name must occur exactly once")
    start = positions[0] + 1
    while start < len(lines) and lines[start] != "        run: |":
        if lines[start].startswith("      - name: "):
            raise ValueError("workflow step has no literal run block")
        start += 1
    if start >= len(lines):
        raise ValueError("workflow step has no literal run block")
    body: list[str] = []
    for line in lines[start + 1 :]:
        if line and not line.startswith("          "):
            break
        body.append(line[10:] if line else "")
    if not body:
        raise ValueError("workflow run block is empty")
    return "\n".join(body) + "\n"


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: extract_workflow_run.py WORKFLOW STEP_NAME")
    sys.stdout.write(extract_run_block(Path(sys.argv[1]), sys.argv[2]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
