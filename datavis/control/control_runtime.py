from __future__ import annotations

import uvicorn

from datavis.control.config import ensure_runtime_dirs, load_settings


def main() -> int:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    uvicorn.run(
        "datavis.control.api:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        factory=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

