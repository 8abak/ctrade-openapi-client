from __future__ import annotations

import argparse
import json
import math
import os
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Generator, Iterable, List, Optional, Sequence

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from datavis.brokerday import brokerday_bounds, brokerday_for_timestamp, tick_mid
from datavis.db import db_connect as shared_db_connect


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

MAVG_VERSION = 1
DEFAULT_SYMBOL = os.getenv("DATAVIS_SYMBOL", "XAUUSD").strip().upper() or "XAUUSD"
DEFAULT_BATCH_SIZE = 400
DEFAULT_REFRESH_SECONDS = max(2.0, float(os.getenv("DATAVIS_MAVG_CONFIG_REFRESH_SECONDS", "10.0")))
MAX_BIGPICTURE_POINTS = max(200, int(os.getenv("DATAVIS_BIGPICTURE_MAX_POINTS", "2400")))
MAX_MAVG_STREAM_BATCH = max(200, int(os.getenv("DATAVIS_MAVG_STREAM_BATCH", "4000")))
EMA_WARMUP_MULTIPLIER = max(2, int(os.getenv("DATAVIS_MAVG_EMA_WARMUP_MULTIPLIER", "5")))
MAX_BOOTSTRAP_SECONDS = max(3600, int(os.getenv("DATAVIS_MAVG_MAX_BOOTSTRAP_SECONDS", "21600")))


@dataclass(frozen=True)
class MavgConfig:
    id: int
    name: str
    method: str
    source: str
    windowseconds: int
    isenabled: bool
    showonlive: bool
    showonbig: bool
    color: Optional[str]
    updatedat: Optional[datetime]


def database_url() -> str:
    for env_name in ("DATABASE_URL", "DATAVIS_DB_URL"):
        value = os.getenv(env_name, "").strip()
        if value:
            if value.startswith("postgresql+psycopg2://"):
                return value.replace("postgresql+psycopg2://", "postgresql://", 1)
            return value
    return ""


def db_connect(*, readonly: bool = False, autocommit: bool = False) -> Any:
    url = database_url()
    if url:
        conn = psycopg2.connect(url)
        conn.autocommit = autocommit
        if readonly:
            conn.set_session(readonly=True, autocommit=autocommit)
        return conn
    return shared_db_connect(readonly=readonly, autocommit=autocommit)


@contextmanager
def db_connection(*, readonly: bool = False, autocommit: bool = False) -> Generator[Any, None, None]:
    conn = db_connect(readonly=readonly, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def normalize_method(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_source(value: Any) -> str:
    return str(value or "").strip().lower()


def source_value(row: Dict[str, Any], source: str) -> Optional[float]:
    normalized = normalize_source(source)
    if normalized == "mid":
        return _safe_float(tick_mid(row))
    if normalized in {"bid", "ask", "kal", "k2"}:
        return _safe_float(row.get(normalized))
    return None


def config_signature(config: MavgConfig) -> Dict[str, Any]:
    return {
        "version": MAVG_VERSION,
        "method": config.method,
        "source": config.source,
        "windowseconds": config.windowseconds,
    }


def config_bootstrap_seconds(config: MavgConfig) -> int:
    if config.method == "EMA":
        return min(MAX_BOOTSTRAP_SECONDS, max(config.windowseconds, config.windowseconds * EMA_WARMUP_MULTIPLIER))
    return min(MAX_BOOTSTRAP_SECONDS, max(config.windowseconds, config.windowseconds + 60))


def config_from_row(row: Dict[str, Any]) -> MavgConfig:
    return MavgConfig(
        id=int(row["id"]),
        name=str(row["name"]),
        method=normalize_method(row.get("method")),
        source=normalize_source(row.get("source")),
        windowseconds=max(1, int(row.get("windowseconds") or 0)),
        isenabled=bool(row.get("isenabled")),
        showonlive=bool(row.get("showonlive")),
        showonbig=bool(row.get("showonbig")),
        color=str(row["color"]).strip() if row.get("color") else None,
        updatedat=_as_utc(row.get("updatedat")),
    )


def load_enabled_configs(conn: Any, *, page: Optional[str] = None) -> List[MavgConfig]:
    where = ["isenabled = TRUE"]
    if page == "live":
        where.append("showonlive = TRUE")
    elif page == "big":
        where.append("showonbig = TRUE")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, method, source, windowseconds, isenabled, showonlive, showonbig, color, updatedat
            FROM public.mavgconfig
            WHERE {where_sql}
            ORDER BY windowseconds ASC, method ASC, id ASC
            """.format(where_sql=" AND ".join(where))
        )
        return [config_from_row(dict(row)) for row in cur.fetchall()]


def load_state_row(conn: Any, *, configid: int) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM public.mavgstate
            WHERE configid = %s
            LIMIT 1
            """,
            (configid,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def latest_tick(conn: Any, *, symbol: str) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, timestamp
            FROM public.ticks
            WHERE symbol = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def fetch_ticks_after(conn: Any, *, symbol: str, after_id: int, limit: int) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread, kal, k2
            FROM public.ticks
            WHERE symbol = %s
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (symbol, after_id, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def iter_ticks_between(
    conn: Any,
    *,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    batch_size: int,
) -> Iterable[List[Dict[str, Any]]]:
    with conn.cursor(name="mavg_ticks_between", cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.itersize = max(1, int(batch_size))
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread, kal, k2
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, start_ts, end_ts),
        )
        while True:
            rows = cur.fetchmany(cur.itersize)
            if not rows:
                return
            yield [dict(row) for row in rows]


def load_sma_seed_rows(
    conn: Any,
    *,
    symbol: str,
    end_time: datetime,
    end_tickid: int,
    windowseconds: int,
) -> List[Dict[str, Any]]:
    cutoff = end_time - timedelta(seconds=max(1, int(windowseconds)))
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, symbol, timestamp, bid, ask, mid, spread, kal, k2
            FROM public.ticks
            WHERE symbol = %s
              AND timestamp >= %s
              AND (timestamp < %s OR (timestamp = %s AND id <= %s))
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, cutoff, end_time, end_time, end_tickid),
        )
        return [dict(row) for row in cur.fetchall()]


def delete_config_rows(conn: Any, *, configid: int) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.mavgvalue WHERE configid = %s", (configid,))
        cur.execute("DELETE FROM public.mavgstate WHERE configid = %s", (configid,))


def insert_values(conn: Any, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO public.mavgvalue (configid, tickid, ticktime, value)
            VALUES %s
            ON CONFLICT (configid, tickid)
            DO UPDATE SET
                ticktime = EXCLUDED.ticktime,
                value = EXCLUDED.value
            """,
            [(
                int(row["configid"]),
                int(row["tickid"]),
                row["ticktime"],
                float(row["value"]),
            ) for row in rows],
            page_size=min(2000, len(rows)),
        )


def upsert_state(conn: Any, row: Optional[Dict[str, Any]]) -> None:
    if not row:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.mavgstate (
                configid, symbol, lasttickid, lastticktime, lastvalue, statejson, updatedat
            ) VALUES (
                %(configid)s, %(symbol)s, %(lasttickid)s, %(lastticktime)s, %(lastvalue)s, %(statejson)s, %(updatedat)s
            )
            ON CONFLICT (configid)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                lasttickid = EXCLUDED.lasttickid,
                lastticktime = EXCLUDED.lastticktime,
                lastvalue = EXCLUDED.lastvalue,
                statejson = EXCLUDED.statejson,
                updatedat = EXCLUDED.updatedat
            """,
            row,
        )


class MavgProcessor:
    def __init__(self, *, config: MavgConfig, symbol: str) -> None:
        self.config = config
        self.symbol = symbol
        self.lasttickid = 0
        self.lastticktime: Optional[datetime] = None
        self.lastvalue: Optional[float] = None

    @property
    def signature(self) -> Dict[str, Any]:
        return config_signature(self.config)

    def reset(self) -> None:
        self.lasttickid = 0
        self.lastticktime = None
        self.lastvalue = None

    def load_state(self, state_row: Dict[str, Any]) -> bool:
        statejson = state_row.get("statejson") or {}
        if isinstance(statejson, str):
            try:
                statejson = json.loads(statejson)
            except json.JSONDecodeError:
                return False
        if not isinstance(statejson, dict):
            return False
        if statejson.get("signature") != self.signature:
            return False
        self.lasttickid = int(state_row.get("lasttickid") or 0)
        self.lastticktime = _as_utc(state_row.get("lastticktime"))
        self.lastvalue = _safe_float(state_row.get("lastvalue"))
        return True

    def restore_runtime(self, conn: Any, state_row: Dict[str, Any]) -> bool:
        return self.load_state(state_row)

    def process_tick(self, row: Dict[str, Any], *, emit: bool = True) -> Optional[Dict[str, Any]]:
        tickid = int(row.get("id") or 0)
        ticktime = _as_utc(row.get("timestamp"))
        if tickid <= self.lasttickid or ticktime is None:
            return None
        value = self.compute_value(row, ticktime=ticktime)
        self.lasttickid = tickid
        self.lastticktime = ticktime
        if value is None:
            return None
        self.lastvalue = float(value)
        if not emit:
            return None
        return {
            "configid": self.config.id,
            "tickid": tickid,
            "ticktime": ticktime,
            "value": float(value),
        }

    def state_row(self) -> Dict[str, Any]:
        return {
            "configid": self.config.id,
            "symbol": self.symbol,
            "lasttickid": self.lasttickid or None,
            "lastticktime": self.lastticktime,
            "lastvalue": self.lastvalue,
            "statejson": json.dumps(
                {
                    "signature": self.signature,
                    **self.extra_state_json(),
                }
            ),
            "updatedat": utc_now(),
        }

    def extra_state_json(self) -> Dict[str, Any]:
        return {}

    def compute_value(self, row: Dict[str, Any], *, ticktime: datetime) -> Optional[float]:
        raise NotImplementedError


class SmaProcessor(MavgProcessor):
    def __init__(self, *, config: MavgConfig, symbol: str) -> None:
        super().__init__(config=config, symbol=symbol)
        self.samples: Deque[tuple[int, datetime, float]] = deque()
        self.total = 0.0

    def reset(self) -> None:
        super().reset()
        self.samples = deque()
        self.total = 0.0

    def restore_runtime(self, conn: Any, state_row: Dict[str, Any]) -> bool:
        if not self.load_state(state_row):
            return False
        if self.lastticktime is None or self.lasttickid <= 0:
            self.samples = deque()
            self.total = 0.0
            return True
        seed_rows = load_sma_seed_rows(
            conn,
            symbol=self.symbol,
            end_time=self.lastticktime,
            end_tickid=self.lasttickid,
            windowseconds=self.config.windowseconds,
        )
        self.samples = deque()
        self.total = 0.0
        for row in seed_rows:
            tickid = int(row.get("id") or 0)
            ticktime = _as_utc(row.get("timestamp"))
            sample = source_value(row, self.config.source)
            if tickid <= 0 or ticktime is None or sample is None:
                continue
            self._append_sample(tickid, ticktime, sample)
            self._evict(ticktime)
        return True

    def extra_state_json(self) -> Dict[str, Any]:
        return {"samplecount": len(self.samples)}

    def compute_value(self, row: Dict[str, Any], *, ticktime: datetime) -> Optional[float]:
        sample = source_value(row, self.config.source)
        if sample is None:
            return self.lastvalue
        self._append_sample(int(row["id"]), ticktime, float(sample))
        self._evict(ticktime)
        if not self.samples:
            return None
        return self.total / float(len(self.samples))

    def _append_sample(self, tickid: int, ticktime: datetime, sample: float) -> None:
        self.samples.append((tickid, ticktime, sample))
        self.total += float(sample)

    def _evict(self, ticktime: datetime) -> None:
        cutoff = ticktime - timedelta(seconds=self.config.windowseconds)
        while self.samples and self.samples[0][1] < cutoff:
            _, _, value = self.samples.popleft()
            self.total -= float(value)


class EmaProcessor(MavgProcessor):
    def __init__(self, *, config: MavgConfig, symbol: str) -> None:
        super().__init__(config=config, symbol=symbol)
        self.base_alpha = 2.0 / (float(max(1, config.windowseconds)) + 1.0)

    def compute_value(self, row: Dict[str, Any], *, ticktime: datetime) -> Optional[float]:
        sample = source_value(row, self.config.source)
        if sample is None:
            return self.lastvalue
        numeric = float(sample)
        if self.lastvalue is None or self.lastticktime is None:
            return numeric
        dt_seconds = max(0.001, (ticktime - self.lastticktime).total_seconds())
        alpha = 1.0 - math.pow(1.0 - self.base_alpha, dt_seconds)
        return float(self.lastvalue + (alpha * (numeric - self.lastvalue)))


def build_processor(config: MavgConfig, *, symbol: str) -> MavgProcessor:
    if config.method == "SMA":
        return SmaProcessor(config=config, symbol=symbol)
    if config.method == "EMA":
        return EmaProcessor(config=config, symbol=symbol)
    raise ValueError("Unsupported mavg method: {0}".format(config.method))


def bootstrap_processor_recent(
    conn: Any,
    *,
    processor: MavgProcessor,
    latest_row: Optional[Dict[str, Any]],
    batch_size: int,
) -> Dict[str, int]:
    processor.reset()
    if latest_row is None or latest_row.get("timestamp") is None:
        upsert_state(conn, processor.state_row())
        return {"valuecount": 0, "tickcount": 0}
    latest_time = _as_utc(latest_row.get("timestamp"))
    if latest_time is None:
        upsert_state(conn, processor.state_row())
        return {"valuecount": 0, "tickcount": 0}
    start_time = latest_time - timedelta(seconds=config_bootstrap_seconds(processor.config))
    total_values = 0
    total_ticks = 0
    pending: List[Dict[str, Any]] = []
    for batch in iter_ticks_between(conn, symbol=processor.symbol, start_ts=start_time, end_ts=latest_time, batch_size=batch_size):
        total_ticks += len(batch)
        for row in batch:
            point = processor.process_tick(row, emit=True)
            if point:
                pending.append(point)
        if pending:
            insert_values(conn, pending)
            pending = []
        upsert_state(conn, processor.state_row())
        total_values = max(total_values, processor.lasttickid)
    if pending:
        insert_values(conn, pending)
    upsert_state(conn, processor.state_row())
    return {"valuecount": len(pending), "tickcount": total_ticks}


def list_page_config_rows(cur: Any, *, page: str) -> List[Dict[str, Any]]:
    show_column = "showonlive" if page == "live" else "showonbig"
    cur.execute(
        """
        SELECT id, name, method, source, windowseconds, showonlive, showonbig, color
        FROM public.mavgconfig
        WHERE isenabled = TRUE
          AND {show_column} = TRUE
        ORDER BY windowseconds ASC, method ASC, id ASC
        """.format(show_column=show_column)
    )
    return [dict(row) for row in cur.fetchall()]


def query_point_rows_for_tick_range(
    cur: Any,
    *,
    page: str,
    start_id: int,
    end_id: int,
) -> List[Dict[str, Any]]:
    show_column = "showonlive" if page == "live" else "showonbig"
    cur.execute(
        """
        SELECT v.id, v.configid, v.tickid, v.ticktime, v.value
        FROM public.mavgvalue v
        JOIN public.mavgconfig c
          ON c.id = v.configid
        WHERE c.isenabled = TRUE
          AND c.{show_column} = TRUE
          AND v.tickid >= %s
          AND v.tickid <= %s
        ORDER BY v.configid ASC, v.tickid ASC
        """.format(show_column=show_column),
        (start_id, end_id),
    )
    return [dict(row) for row in cur.fetchall()]


def query_point_rows_after_value_id(
    cur: Any,
    *,
    page: str,
    after_value_id: int,
    limit: int = MAX_MAVG_STREAM_BATCH,
) -> List[Dict[str, Any]]:
    show_column = "showonlive" if page == "live" else "showonbig"
    cur.execute(
        """
        SELECT v.id, v.configid, v.tickid, v.ticktime, v.value
        FROM public.mavgvalue v
        JOIN public.mavgconfig c
          ON c.id = v.configid
        WHERE c.isenabled = TRUE
          AND c.{show_column} = TRUE
          AND v.id > %s
        ORDER BY v.id ASC
        LIMIT %s
        """.format(show_column=show_column),
        (after_value_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def query_point_rows_for_time_range(
    cur: Any,
    *,
    page: str,
    start_ts: datetime,
    end_ts: datetime,
    target_points: int,
) -> List[Dict[str, Any]]:
    show_column = "showonlive" if page == "live" else "showonbig"
    bucket_count = max(1, min(MAX_BIGPICTURE_POINTS, target_points) // 4)
    cur.execute(
        """
        WITH params AS (
            SELECT
                %s::timestamptz AS start_ts,
                %s::timestamptz AS end_ts,
                %s::int AS bucket_count
        ),
        ranked AS (
            SELECT
                v.id,
                v.configid,
                v.tickid,
                v.ticktime,
                v.value,
                LEAST(
                    p.bucket_count,
                    GREATEST(
                        1,
                        width_bucket(
                            EXTRACT(EPOCH FROM v.ticktime),
                            EXTRACT(EPOCH FROM p.start_ts),
                            EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                            p.bucket_count
                        )
                    )
                ) AS bucket,
                row_number() OVER (
                    PARTITION BY v.configid,
                    LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM v.ticktime),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY v.ticktime ASC, v.tickid ASC
                ) AS rn_first,
                row_number() OVER (
                    PARTITION BY v.configid,
                    LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM v.ticktime),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY v.ticktime DESC, v.tickid DESC
                ) AS rn_last,
                row_number() OVER (
                    PARTITION BY v.configid,
                    LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM v.ticktime),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY v.value ASC, v.ticktime ASC, v.tickid ASC
                ) AS rn_low,
                row_number() OVER (
                    PARTITION BY v.configid,
                    LEAST(
                        p.bucket_count,
                        GREATEST(
                            1,
                            width_bucket(
                                EXTRACT(EPOCH FROM v.ticktime),
                                EXTRACT(EPOCH FROM p.start_ts),
                                EXTRACT(EPOCH FROM p.end_ts) + 0.000001,
                                p.bucket_count
                            )
                        )
                    )
                    ORDER BY v.value DESC, v.ticktime ASC, v.tickid ASC
                ) AS rn_high
            FROM public.mavgvalue v
            JOIN public.mavgconfig c
              ON c.id = v.configid
            CROSS JOIN params p
            WHERE c.isenabled = TRUE
              AND c.{show_column} = TRUE
              AND v.ticktime >= p.start_ts
              AND v.ticktime <= p.end_ts
        )
        SELECT id, configid, tickid, ticktime, value
        FROM ranked
        WHERE rn_first = 1 OR rn_last = 1 OR rn_low = 1 OR rn_high = 1
        ORDER BY configid ASC, tickid ASC
        """.format(show_column=show_column),
        (start_ts, end_ts, bucket_count),
    )
    return [dict(row) for row in cur.fetchall()]


class MavgLiveRuntime:
    def __init__(self, *, symbol: str, batch_size: int = DEFAULT_BATCH_SIZE, config_refresh_seconds: float = DEFAULT_REFRESH_SECONDS) -> None:
        self.symbol = symbol
        self.batch_size = max(1, int(batch_size))
        self.config_refresh_seconds = max(2.0, float(config_refresh_seconds))
        self._processors: Dict[int, MavgProcessor] = {}
        self._next_refresh_at = 0.0

    @property
    def processors(self) -> Dict[int, MavgProcessor]:
        return self._processors

    def refresh_configs(self, conn: Any, *, force: bool = False) -> Dict[str, int]:
        now = time.monotonic()
        if not force and now < self._next_refresh_at:
            return {"configcount": len(self._processors), "bootstrapped": 0}
        configs = load_enabled_configs(conn, page="live")
        latest = latest_tick(conn, symbol=self.symbol)
        next_processors: Dict[int, MavgProcessor] = {}
        bootstrapped = 0
        for config in configs:
            current = self._processors.get(config.id)
            if current and current.signature == config_signature(config):
                current.config = config
                next_processors[config.id] = current
                continue

            processor = build_processor(config, symbol=self.symbol)
            state_row = load_state_row(conn, configid=config.id)
            restored = bool(state_row and processor.restore_runtime(conn, state_row))
            if not restored:
                delete_config_rows(conn, configid=config.id)
                bootstrap_processor_recent(conn, processor=processor, latest_row=latest, batch_size=self.batch_size)
                bootstrapped += 1
            next_processors[config.id] = processor
        self._processors = next_processors
        self._next_refresh_at = now + self.config_refresh_seconds
        return {"configcount": len(self._processors), "bootstrapped": bootstrapped}

    def bootstrap(self, conn: Any) -> Dict[str, int]:
        summary = self.refresh_configs(conn, force=True)
        processed = self.process_once(conn, refresh=False)
        return {
            "configcount": summary["configcount"],
            "bootstrapped": summary["bootstrapped"],
            "tickcount": processed["tickcount"],
            "valuecount": processed["valuecount"],
        }

    def process_once(self, conn: Any, *, refresh: bool = True) -> Dict[str, int]:
        if refresh:
            self.refresh_configs(conn, force=False)
        if not self._processors:
            return {"configcount": 0, "tickcount": 0, "valuecount": 0}
        min_after_id = min(int(processor.lasttickid or 0) for processor in self._processors.values())
        rows = fetch_ticks_after(conn, symbol=self.symbol, after_id=min_after_id, limit=self.batch_size)
        if not rows:
            for processor in self._processors.values():
                upsert_state(conn, processor.state_row())
            return {"configcount": len(self._processors), "tickcount": 0, "valuecount": 0}
        points: List[Dict[str, Any]] = []
        for row in rows:
            for processor in self._processors.values():
                point = processor.process_tick(row, emit=True)
                if point:
                    points.append(point)
        insert_values(conn, points)
        for processor in self._processors.values():
            upsert_state(conn, processor.state_row())
        return {"configcount": len(self._processors), "tickcount": len(rows), "valuecount": len(points)}


def backfill_recent(
    conn: Any,
    *,
    symbol: str,
    days: float,
    batch_size: int,
) -> Dict[str, int]:
    latest = latest_tick(conn, symbol=symbol)
    if latest is None or latest.get("timestamp") is None:
        return {"configcount": 0, "tickcount": 0, "valuecount": 0}
    end_time = _as_utc(latest.get("timestamp"))
    if end_time is None:
        return {"configcount": 0, "tickcount": 0, "valuecount": 0}
    lookback_seconds = max(3600, int(round(float(days) * 86400.0)))
    start_time = end_time - timedelta(seconds=lookback_seconds)
    configs = load_enabled_configs(conn, page=None)
    total_ticks = 0
    total_values = 0
    for config in configs:
        delete_config_rows(conn, configid=config.id)
        processor = build_processor(config, symbol=symbol)
        warmup_start = start_time - timedelta(seconds=config_bootstrap_seconds(config))
        pending: List[Dict[str, Any]] = []
        for batch in iter_ticks_between(conn, symbol=symbol, start_ts=warmup_start, end_ts=end_time, batch_size=batch_size):
            total_ticks += len(batch)
            for row in batch:
                row_time = _as_utc(row.get("timestamp"))
                emit = bool(row_time and row_time >= start_time)
                point = processor.process_tick(row, emit=emit)
                if point:
                    pending.append(point)
            if pending:
                insert_values(conn, pending)
                total_values += len(pending)
                pending = []
            upsert_state(conn, processor.state_row())
        if pending:
            insert_values(conn, pending)
            total_values += len(pending)
        upsert_state(conn, processor.state_row())
    return {"configcount": len(configs), "tickcount": total_ticks, "valuecount": total_values}


def _print(message: str) -> None:
    print(message, flush=True)


def build_jobs_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Moving-average operational jobs.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Symbol to process.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Tick batch size.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    recent = subparsers.add_parser("backfill-recent", help="Backfill enabled MA configs for a recent lookback window.")
    recent.add_argument("--days", type=float, default=3.0, help="Recent lookback window in broker days.")
    subparsers.add_parser("bootstrap-enabled", help="Seed enabled MA configs with a recent lightweight history.")
    return parser


def jobs_main() -> int:
    args = build_jobs_parser().parse_args()
    symbol = str(args.symbol or DEFAULT_SYMBOL).strip().upper() or DEFAULT_SYMBOL
    batch_size = max(1, int(args.batch_size))
    with db_connection(readonly=False, autocommit=False) as conn:
        if args.command == "backfill-recent":
            result = backfill_recent(conn, symbol=symbol, days=max(0.25, float(args.days)), batch_size=batch_size)
            conn.commit()
            _print(
                "configs={0} ticks={1} values={2}".format(
                    result["configcount"],
                    result["tickcount"],
                    result["valuecount"],
                )
            )
            return 0
        if args.command == "bootstrap-enabled":
            runtime = MavgLiveRuntime(symbol=symbol, batch_size=batch_size)
            result = runtime.bootstrap(conn)
            conn.commit()
            _print(
                "configs={0} bootstrapped={1} ticks={2} values={3}".format(
                    result["configcount"],
                    result["bootstrapped"],
                    result["tickcount"],
                    result["valuecount"],
                )
            )
            return 0
    return 1
