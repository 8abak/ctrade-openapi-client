from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

import psycopg2


# Keep this aligned with tickCollectorRawToDB.py.
DB_KW = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)


def db_connect(*, readonly: bool = False, autocommit: bool = False) -> Any:
    conn = psycopg2.connect(**DB_KW)
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
