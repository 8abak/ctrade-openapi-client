from __future__ import annotations

from datavis.control.api import ORCHESTRATOR, SETTINGS
from datavis.control.config import ensure_runtime_dirs
from datavis.control.db import connection


def main() -> int:
    ensure_runtime_dirs(SETTINGS)
    ORCHESTRATOR.run_forever(lambda **kwargs: connection(SETTINGS, application_name="datavis.control.orchestrator", **kwargs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
