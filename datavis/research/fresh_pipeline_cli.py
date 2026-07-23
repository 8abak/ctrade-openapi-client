"""Command-line entry point for the complete fresh chronological study."""

from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from datavis.db import db_connection
from datavis.research.fresh_pipeline import run_registered_fresh_research


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--repository-root", default=str(Path.cwd()))
    parser.add_argument(
        "--research-state-dir",
        help="durable host directory for consumed-window and holdout locks",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="required acknowledgement that the frozen chronological run may begin",
    )
    lineage = parser.add_mutually_exclusive_group()
    lineage.add_argument(
        "--resume-artifact-dir",
        help=(
            "exact extracted run-14 artifact directory for the sole audited "
            "discovery continuation"
        ),
    )
    lineage.add_argument(
        "--restart-artifact-dir",
        help=(
            "exact extracted run-16 artifact directory that proves eligibility "
            "for the separately preregistered v3 study"
        ),
    )
    return parser


@contextmanager
def _registered_connection_context() -> Iterator[Any]:
    with db_connection(readonly=True, autocommit=False) as connection:
        yield connection


def _progress(payload: Mapping[str, Any]) -> None:
    print(json.dumps(dict(payload), sort_keys=True, allow_nan=False), flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if not arguments.execute:
        raise SystemExit("refusing to open research outcomes without --execute")
    if not arguments.research_state_dir:
        raise SystemExit("refusing to execute without a durable --research-state-dir")
    summary = run_registered_fresh_research(
        _registered_connection_context,
        repository_root=arguments.repository_root,
        output_directory=arguments.output_dir,
        research_state_directory=arguments.research_state_dir,
        progress=_progress,
        resume_artifact_directory=arguments.resume_artifact_dir,
        infrastructure_restart_artifact_directory=(
            arguments.restart_artifact_dir
        ),
    )
    print(
        json.dumps(
            {
                "stage": "fresh_research_complete",
                "status": summary["status"],
                "holdoutOpened": summary["holdoutOpened"],
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
