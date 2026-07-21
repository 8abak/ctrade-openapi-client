from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import psycopg2
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def _database_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    raise RuntimeError("DATABASE_URL or DATAVIS_DB_URL is not configured")


def db_connect(*, readonly: bool = False, autocommit: bool = False) -> Any:
    conn = psycopg2.connect(_database_url())
    conn.autocommit = autocommit
    if readonly:
        conn.set_session(readonly=True, autocommit=autocommit)
    return conn


@contextmanager
def db_connection(*, readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
    conn = db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()
