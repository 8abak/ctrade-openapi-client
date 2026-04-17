from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator, Optional

import psycopg2
import psycopg2.extras

from datavis.db import db_connect as shared_db_connect
from datavis.research.config import ResearchSettings


def connect(
    settings: ResearchSettings,
    *,
    readonly: bool = False,
    autocommit: bool = False,
    application_name: Optional[str] = None,
) -> Any:
    if settings.database_url:
        conn = psycopg2.connect(settings.database_url)
        conn.autocommit = autocommit
        if readonly:
            conn.set_session(readonly=True, autocommit=autocommit)
    else:
        conn = shared_db_connect(readonly=readonly, autocommit=autocommit)
    with conn.cursor() as cur:
        if application_name:
            cur.execute("SET application_name = %s", (application_name,))
        cur.execute("SET statement_timeout = %s", (f"{int(settings.statement_timeout_ms)}ms",))
        cur.execute("SET lock_timeout = %s", (f"{int(settings.lock_timeout_ms)}ms",))
        cur.execute("SET idle_in_transaction_session_timeout = %s", ("30000ms",))
    return conn


@contextmanager
def connection(
    settings: ResearchSettings,
    *,
    readonly: bool = False,
    autocommit: bool = False,
    application_name: Optional[str] = None,
) -> Generator[Any, None, None]:
    conn = connect(settings, readonly=readonly, autocommit=autocommit, application_name=application_name)
    try:
        yield conn
    finally:
        conn.close()


def realdict_cursor(conn: Any) -> Any:
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

