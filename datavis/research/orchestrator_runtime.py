from __future__ import annotations

from datavis.research.config import ensure_runtime_dirs, load_settings
from datavis.research.db import connection
from datavis.research.orchestrator import ResearchOrchestrator


def main() -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    orchestrator = ResearchOrchestrator(settings)
    orchestrator.run_forever(lambda **kwargs: connection(settings, application_name="datavis.research.orchestrator", **kwargs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
