"""Research-only portable DB adapter. Credentials supplied outside source control.

The deployed run instead reused its existing datavis.db read-only connection.
This adapter is not the active service database module.
"""
import os
from contextlib import contextmanager

import psycopg2


@contextmanager
def db_connection(*,readonly=True):
    if not readonly:
        raise ValueError('Research adapter permits read-only database access only')
    conn = psycopg2.connect(os.environ['ACD_RESEARCH_DB_DSN'])
    conn.set_session(readonly=True)
    try:
        yield conn
    finally:
        conn.close()
