#!/usr/bin/env python3
from __future__ import annotations

import os


def resolve_db_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    return ""


if __name__ == "__main__":
    print(resolve_db_url())
