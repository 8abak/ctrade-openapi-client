from __future__ import annotations

import uvicorn

from datavis.control.runtime import get_control_runtime


def main() -> int:
    settings = get_control_runtime().settings
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
