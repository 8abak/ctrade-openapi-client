"""Command-line entry point for the outcome-blind source bootstrap."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from datavis.db import db_connection
from datavis.research.fresh_bootstrap import (
    ConnectionContextFactory,
    build_fresh_source_bootstrap,
    registered_fresh_bootstrap_config,
    write_fresh_source_bootstrap,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stream the registered XAUUSD source range read-only, run data QC, "
            "and freeze inventory/corpus/split manifests."
        )
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Required before the command opens the read-only database connection.",
    )
    return parser


def _registered_connection_context():
    return db_connection(readonly=True, autocommit=False)


def _progress_line(item: Mapping[str, Any]) -> str:
    return json.dumps(dict(item), sort_keys=True, separators=(",", ":"))


def run_registered_bootstrap(
    output_directory: str | Path,
    *,
    connection_context_factory: ConnectionContextFactory = _registered_connection_context,
    on_progress: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, str]:
    config = registered_fresh_bootstrap_config()
    artifacts = build_fresh_source_bootstrap(
        connection_context_factory,
        config=config,
        on_progress=on_progress,
    )
    return write_fresh_source_bootstrap(output_directory, artifacts)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.execute:
        raise SystemExit(
            "Refusing to open the database without --execute; no data was read."
        )
    try:
        paths = run_registered_bootstrap(
            args.output_dir,
            on_progress=lambda item: print(_progress_line(item), flush=True),
        )
    except (FileExistsError, PermissionError, RuntimeError, TypeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps({"stage": "source_bootstrap_complete", "artifacts": paths}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_parser", "main", "run_registered_bootstrap"]
