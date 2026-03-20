"""
Usage:
  python -m jobs.buildTconfirm
  python -m jobs.buildTconfirm --day-id 123
  python -m jobs.buildTconfirm --start-day-id 100 --end-day-id 150

Build public.tconfirm from public.tepisode, driven by public.days.
Each row is a first-pass Layer 3 confirmation extract for one top episode.
"""

from __future__ import annotations

import argparse
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg2.extras

from backend.db import columns_exist, detect_mid_expr, detect_ts_col, get_conn, table_columns
from jobs.layer2common import (
    DEFAULT_BUILDVER as DEFAULT_LAYER2_BUILDVER,
    DayRow,
    PivotRow,
    add_day_args,
    delete_day_rows,
    execute_values_insert,
    list_days,
    load_day_pivots,
    require_table_columns,
    setup_logger,
)


DEFAULT_BUILDVER = "layer3.v1"
DEFAULT_PRELOOKBACK_TICKS = 200
DEFAULT_COMPRESSION_SHORT_TICKS = 50
DEFAULT_COMPRESSION_LONG_TICKS = 200
TARGET_DIR = "top"
ANCHOR_LAYER = "nano"
CASCADE_LAYER = "micro"

TEXT_TYPES = {"text", "character varying", "character"}

TEPISODE_COLUMNS = (
    "id",
    "dayid",
    "tzoneid",
    "dir",
    "reppivotid",
    "repts",
    "topprice",
    "zonepos",
    "buildver",
)

TCONFIRM_REQUIRED_COLUMNS = (
    "dayid",
    "anchorts",
    "anchorprice",
    "anchorpivotid",
    "microreversalts",
    "microreversalticks",
    "lowerhights",
    "lowerhighticks",
    "lowerlowts",
    "lowerlowticks",
    "breakhights",
    "breakhighticks",
    "invalidated",
    "invalidationreason",
    "incomingmove200",
    "kalminusk2athigh",
    "compression50to200",
    "zonepos",
    "inzone",
    "truthmatch",
    "confirmstate",
    "buildver",
    "createdts",
    "updatedts",
)


@dataclass
class TepisodeRow:
    id: int
    dayid: int
    tzoneid: Optional[int]
    dir: str
    reppivotid: int
    repts: object
    topprice: float
    zonepos: Optional[str]


@dataclass
class TickRow:
    id: int
    ts: object
    mid: Optional[float]
    kal: Optional[float]
    k2: Optional[float]

    @property
    def basis(self) -> Optional[float]:
        if self.kal is not None:
            return float(self.kal)
        if self.mid is not None:
            return float(self.mid)
        return None


@dataclass
class TickEvent:
    tickid: int
    ts: object


class SkipEpisodeError(RuntimeError):
    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = str(reason)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Layer 3 tconfirm rows from tepisode.")
    add_day_args(parser)
    parser.add_argument("--buildver", default=DEFAULT_BUILDVER, help="Target build version")
    parser.add_argument(
        "--episode-buildver",
        default=DEFAULT_LAYER2_BUILDVER,
        help="tepisode build version used as the source (defaults to layer2.v1)",
    )
    parser.add_argument(
        "--delete-all-buildvers",
        action="store_true",
        help="Delete all existing day rows regardless of buildver before rebuilding",
    )
    parser.add_argument("--dir", default=TARGET_DIR)
    parser.add_argument("--anchor-layer", default=ANCHOR_LAYER)
    parser.add_argument("--cascade-layer", default=CASCADE_LAYER)
    parser.add_argument("--prelookback-ticks", type=int, default=DEFAULT_PRELOOKBACK_TICKS)
    parser.add_argument("--compression-short-ticks", type=int, default=DEFAULT_COMPRESSION_SHORT_TICKS)
    parser.add_argument("--compression-long-ticks", type=int, default=DEFAULT_COMPRESSION_LONG_TICKS)
    return parser.parse_args()


def load_tepisodes(conn, *, day_id: int, buildver: str, dir_name: str) -> List[TepisodeRow]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, dayid, tzoneid, dir, reppivotid, repts, topprice, zonepos
            FROM public.tepisode
            WHERE dayid = %s
              AND buildver = %s
              AND dir = %s
            ORDER BY repts ASC, id ASC
            """,
            (int(day_id), str(buildver), str(dir_name)),
        )
        rows = cur.fetchall()

    return [
        TepisodeRow(
            id=int(row["id"]),
            dayid=int(row["dayid"]),
            tzoneid=int(row["tzoneid"]) if row["tzoneid"] is not None else None,
            dir=str(row["dir"]),
            reppivotid=int(row["reppivotid"]),
            repts=row["repts"],
            topprice=float(row["topprice"]),
            zonepos=str(row["zonepos"]) if row["zonepos"] is not None else None,
        )
        for row in rows
    ]


def load_ticks_for_day(conn, day: DayRow, *, prelookback_ticks: int) -> Tuple[List[TickRow], bool]:
    ts_col = detect_ts_col(conn)
    mid_expr = detect_mid_expr(conn)
    has_kal = "kal" in columns_exist(conn, "ticks", ["kal"])
    has_k2 = "k2" in columns_exist(conn, "ticks", ["k2"])

    kal_sel = ", kal" if has_kal else ""
    k2_sel = ", k2" if has_k2 else ""
    start_id = max(1, int(day.startid) - max(0, int(prelookback_ticks)))

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT id,
                   {ts_col} AS ts,
                   {mid_expr} AS mid
                   {kal_sel}
                   {k2_sel}
            FROM public.ticks
            WHERE id BETWEEN %s AND %s
              AND {ts_col} IS NOT NULL
            ORDER BY id ASC
            """,
            (start_id, int(day.endid)),
        )
        rows = cur.fetchall()

    ticks: List[TickRow] = []
    for row in rows:
        ticks.append(
            TickRow(
                id=int(row["id"]),
                ts=row["ts"],
                mid=float(row["mid"]) if row.get("mid") is not None else None,
                kal=float(row["kal"]) if has_kal and row.get("kal") is not None else None,
                k2=float(row["k2"]) if has_k2 and row.get("k2") is not None else None,
            )
        )
    return ticks, has_k2


def load_tconfirm_column_types(conn) -> Dict[str, str]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'tconfirm'
            ORDER BY ordinal_position ASC
            """
        )
        rows = cur.fetchall()
    return {str(row["column_name"]): str(row["data_type"]) for row in rows}


def resolve_insert_columns(conn) -> Tuple[List[str], Dict[str, str]]:
    col_types = load_tconfirm_column_types(conn)
    present = set(col_types)

    episode_col = "tepisodeid" if "tepisodeid" in present else ("episodeid" if "episodeid" in present else None)
    if episode_col is None:
        raise RuntimeError(
            "public.tconfirm must contain tepisodeid (or episodeid) to support one-row-per-tepisode builds."
        )

    missing = [col for col in TCONFIRM_REQUIRED_COLUMNS if col not in present]
    if missing:
        raise RuntimeError(
            f"Missing required table/columns for public.tconfirm: {', '.join(missing)}. "
            "Apply the required DDL first via sql.html."
        )

    ordered = ["dayid", episode_col]

    zone_col = None
    if "tzoneid" in present:
        zone_col = "tzoneid"
        ordered.append(zone_col)
    elif "zoneid" in present:
        zone_col = "zoneid"
        ordered.append(zone_col)

    if "dir" in present:
        ordered.append("dir")

    ordered.extend(
        [
            "anchorts",
            "anchorprice",
            "anchorpivotid",
            "microreversalts",
            "microreversalticks",
            "lowerhights",
            "lowerhighticks",
            "lowerlowts",
            "lowerlowticks",
            "breakhights",
            "breakhighticks",
            "invalidated",
            "invalidationreason",
            "incomingmove200",
            "kalminusk2athigh",
            "compression50to200",
            "zonepos",
            "inzone",
            "truthmatch",
            "confirmstate",
            "buildver",
            "createdts",
            "updatedts",
        ]
    )
    return ordered, col_types


def price_range(ticks: Sequence[TickRow]) -> Optional[float]:
    vals = [tick.basis for tick in ticks if tick.basis is not None]
    if len(vals) < 2:
        return None
    return float(max(vals) - min(vals))


def find_first_break_high(ticks: Sequence[TickRow], start_idx: int, anchor_price: float) -> Optional[TickEvent]:
    for tick in ticks[start_idx + 1 :]:
        basis = tick.basis
        if basis is None:
            continue
        if float(basis) > float(anchor_price):
            return TickEvent(tickid=int(tick.id), ts=tick.ts)
    return None


def first_pivot_after(
    pivots: Sequence[PivotRow],
    pivot_ids: Sequence[int],
    after_tickid: int,
    predicate=None,
) -> Optional[PivotRow]:
    idx = bisect_right(pivot_ids, int(after_tickid))
    while idx < len(pivots):
        pivot = pivots[idx]
        if predicate is None or predicate(pivot):
            return pivot
        idx += 1
    return None


def event_tick_distance(anchor_tickid: int, event_tickid: Optional[int]) -> Optional[int]:
    if event_tickid is None:
        return None
    return int(event_tickid - anchor_tickid)


def coerce_value(value, col_type: str):
    if value is None:
        return None
    if col_type in TEXT_TYPES and isinstance(value, bool):
        return "true" if value else "false"
    return value


def build_confirm_row(
    *,
    episode: TepisodeRow,
    anchor_pivot: PivotRow,
    micro_highs: Sequence[PivotRow],
    micro_high_ids: Sequence[int],
    micro_lows: Sequence[PivotRow],
    micro_low_ids: Sequence[int],
    tick_by_id: Dict[int, TickRow],
    ticks: Sequence[TickRow],
    tick_ids: Sequence[int],
    has_k2: bool,
    args: argparse.Namespace,
    created_at: datetime,
    insert_cols: Sequence[str],
    col_types: Dict[str, str],
) -> Tuple[Dict[str, object], str]:
    anchor_tickid = int(anchor_pivot.tickid)
    anchor_price = float(anchor_pivot.px)
    anchor_idx = bisect_right(tick_ids, anchor_tickid) - 1
    if anchor_idx < 0 or anchor_idx >= len(ticks) or int(ticks[anchor_idx].id) != anchor_tickid:
        raise SkipEpisodeError(
            "missinganchortick",
            f"Anchor tick {anchor_tickid} for tepisode {episode.id} was not found in ticks.",
        )

    micro_reversal = first_pivot_after(micro_lows, micro_low_ids, anchor_tickid)
    lower_high = None
    lower_low = None

    if micro_reversal is not None:
        lower_high = first_pivot_after(
            micro_highs,
            micro_high_ids,
            int(micro_reversal.tickid),
            predicate=lambda pivot: float(pivot.px) < float(anchor_price),
        )
        if lower_high is not None:
            lower_low = first_pivot_after(
                micro_lows,
                micro_low_ids,
                int(lower_high.tickid),
                predicate=lambda pivot: float(pivot.px) < float(micro_reversal.px),
            )

    break_high = find_first_break_high(ticks, anchor_idx, anchor_price)

    confirm_state = "unfinished"
    invalidated = False
    invalidation_reason = None

    if break_high is not None and (lower_low is None or int(break_high.tickid) < int(lower_low.tickid)):
        confirm_state = "invalidated"
        invalidated = True
        invalidation_reason = "breakhighbeforeconfirm"
        if micro_reversal is not None and int(micro_reversal.tickid) >= int(break_high.tickid):
            micro_reversal = None
        if lower_high is not None and int(lower_high.tickid) >= int(break_high.tickid):
            lower_high = None
        lower_low = None
    elif lower_low is not None:
        confirm_state = "confirmed"
        break_high = None
    else:
        break_high = None

    lookback_ticks = max(1, int(args.prelookback_ticks))
    short_ticks = max(1, int(args.compression_short_ticks))
    long_ticks = max(short_ticks, int(args.compression_long_ticks))

    lookback_idx = max(0, anchor_idx - lookback_ticks)
    lookback_basis = ticks[lookback_idx].basis
    incoming_move_200 = (
        float(anchor_price - lookback_basis)
        if lookback_idx < anchor_idx and lookback_basis is not None
        else None
    )

    short_start = max(0, anchor_idx - short_ticks + 1)
    long_start = max(0, anchor_idx - long_ticks + 1)
    short_range = price_range(ticks[short_start : anchor_idx + 1])
    long_range = price_range(ticks[long_start : anchor_idx + 1])
    compression_50_to_200 = (
        float(short_range / long_range)
        if short_range is not None and long_range is not None and long_range > 0.0
        else None
    )

    anchor_tick = tick_by_id.get(anchor_tickid)
    kal_minus_k2_at_high = None
    if has_k2 and anchor_tick is not None and anchor_tick.kal is not None and anchor_tick.k2 is not None:
        kal_minus_k2_at_high = float(anchor_tick.kal - anchor_tick.k2)

    zone_pos = episode.zonepos
    in_zone = bool(
        episode.tzoneid is not None
        and zone_pos is not None
        and str(zone_pos).lower() == "inside"
    )
    truth_match = bool(in_zone)

    values = {
        "dayid": int(episode.dayid),
        "tepisodeid": int(episode.id),
        "episodeid": int(episode.id),
        "tzoneid": int(episode.tzoneid) if episode.tzoneid is not None else None,
        "zoneid": int(episode.tzoneid) if episode.tzoneid is not None else None,
        "dir": str(episode.dir),
        "anchorts": anchor_pivot.ts,
        "anchorprice": float(anchor_price),
        "anchorpivotid": int(anchor_pivot.id),
        "microreversalts": micro_reversal.ts if micro_reversal is not None else None,
        "microreversalticks": (
            event_tick_distance(anchor_tickid, int(micro_reversal.tickid))
            if micro_reversal is not None
            else None
        ),
        "lowerhights": lower_high.ts if lower_high is not None else None,
        "lowerhighticks": (
            event_tick_distance(anchor_tickid, int(lower_high.tickid))
            if lower_high is not None
            else None
        ),
        "lowerlowts": lower_low.ts if lower_low is not None else None,
        "lowerlowticks": (
            event_tick_distance(anchor_tickid, int(lower_low.tickid))
            if lower_low is not None
            else None
        ),
        "breakhights": break_high.ts if break_high is not None else None,
        "breakhighticks": (
            event_tick_distance(anchor_tickid, int(break_high.tickid))
            if break_high is not None
            else None
        ),
        "invalidated": bool(invalidated),
        "invalidationreason": invalidation_reason,
        "incomingmove200": incoming_move_200,
        "kalminusk2athigh": kal_minus_k2_at_high,
        "compression50to200": compression_50_to_200,
        "zonepos": zone_pos,
        "inzone": bool(in_zone),
        "truthmatch": bool(truth_match),
        "confirmstate": confirm_state,
        "buildver": str(args.buildver),
        "createdts": created_at,
        "updatedts": created_at,
    }

    coerced: Dict[str, object] = {}
    for col in insert_cols:
        coerced[col] = coerce_value(values.get(col), col_types[col])
    return coerced, confirm_state


def insert_tconfirms(conn, insert_cols: Sequence[str], rows: List[Dict[str, object]]) -> int:
    if not rows:
        return 0

    sql_cols = ", ".join(insert_cols)
    values = [tuple(row.get(col) for col in insert_cols) for row in rows]
    return execute_values_insert(
        conn,
        f"""
        INSERT INTO public.tconfirm (
            {sql_cols}
        )
        VALUES %s
        """,
        values,
        page_size=1000,
    )


def process_day(conn, logger, day: DayRow, args: argparse.Namespace) -> Dict[str, int]:
    insert_cols, col_types = resolve_insert_columns(conn)
    deleted = delete_day_rows(
        conn,
        table="tconfirm",
        day_id=day.id,
        buildver=args.buildver,
        delete_all_buildvers=bool(args.delete_all_buildvers),
    )

    tepisodes = load_tepisodes(
        conn,
        day_id=day.id,
        buildver=args.episode_buildver,
        dir_name=args.dir,
    )
    anchor_pivots = load_day_pivots(conn, day_id=day.id, layer=args.anchor_layer, ptype="h")
    micro_highs = load_day_pivots(conn, day_id=day.id, layer=args.cascade_layer, ptype="h")
    micro_lows = load_day_pivots(conn, day_id=day.id, layer=args.cascade_layer, ptype="l")
    ticks, has_k2 = load_ticks_for_day(conn, day, prelookback_ticks=args.prelookback_ticks)

    anchor_by_id = {int(pivot.id): pivot for pivot in anchor_pivots}
    tick_by_id = {int(tick.id): tick for tick in ticks}
    tick_ids = [int(tick.id) for tick in ticks]
    micro_high_ids = [int(pivot.tickid) for pivot in micro_highs]
    micro_low_ids = [int(pivot.tickid) for pivot in micro_lows]

    created_at = datetime.now(timezone.utc)
    rows: List[Dict[str, object]] = []
    states = {"confirmed": 0, "invalidated": 0, "unfinished": 0}
    skipped = 0
    skip_reasons: Dict[str, int] = {}

    logger.info(
        "DAY source | day_id=%s tepisodes_read=%s episode_buildver=%s anchor_pivots=%s micro_highs=%s micro_lows=%s ticks=%s",
        day.id,
        len(tepisodes),
        args.episode_buildver,
        len(anchor_pivots),
        len(micro_highs),
        len(micro_lows),
        len(ticks),
    )

    for episode in tepisodes:
        anchor_pivot = anchor_by_id.get(int(episode.reppivotid))
        if anchor_pivot is None:
            skipped += 1
            skip_reasons["missinganchorpivot"] = skip_reasons.get("missinganchorpivot", 0) + 1
            logger.warning(
                "EPISODE skip | day_id=%s tepisode_id=%s reason=missinganchorpivot reppivotid=%s anchor_layer=%s",
                day.id,
                episode.id,
                episode.reppivotid,
                args.anchor_layer,
            )
            continue

        try:
            row, state = build_confirm_row(
                episode=episode,
                anchor_pivot=anchor_pivot,
                micro_highs=micro_highs,
                micro_high_ids=micro_high_ids,
                micro_lows=micro_lows,
                micro_low_ids=micro_low_ids,
                tick_by_id=tick_by_id,
                ticks=ticks,
                tick_ids=tick_ids,
                has_k2=has_k2,
                args=args,
                created_at=created_at,
                insert_cols=insert_cols,
                col_types=col_types,
            )
        except SkipEpisodeError as exc:
            skipped += 1
            skip_reasons[exc.reason] = skip_reasons.get(exc.reason, 0) + 1
            logger.warning(
                "EPISODE skip | day_id=%s tepisode_id=%s reason=%s detail=%s",
                day.id,
                episode.id,
                exc.reason,
                str(exc),
            )
            continue

        rows.append(row)
        states[state] += 1

    inserted = insert_tconfirms(conn, insert_cols, rows)
    conn.commit()

    logger.info(
        "DAY finish | day_id=%s deleted=%s tepisodes_read=%s tepisodes_skipped=%s skip_reasons=%s anchor_pivots=%s micro_highs=%s micro_lows=%s ticks=%s inserted=%s confirmed=%s invalidated=%s unfinished=%s buildver=%s episode_buildver=%s",
        day.id,
        deleted,
        len(tepisodes),
        skipped,
        ",".join(f"{k}:{v}" for k, v in sorted(skip_reasons.items())) or "none",
        len(anchor_pivots),
        len(micro_highs),
        len(micro_lows),
        len(ticks),
        inserted,
        states["confirmed"],
        states["invalidated"],
        states["unfinished"],
        args.buildver,
        args.episode_buildver,
    )
    return {
        "deleted": deleted,
        "tepisodes": len(tepisodes),
        "skipped": skipped,
        "inserted": inserted,
        "confirmed": states["confirmed"],
        "invalidated": states["invalidated"],
        "unfinished": states["unfinished"],
    }


def main() -> None:
    args = parse_args()
    logger = setup_logger("buildTconfirm", "buildTconfirm.log")

    conn = get_conn()
    conn.autocommit = False

    try:
        require_table_columns(conn, "days", ("id", "startid", "endid", "startts", "endts"))
        require_table_columns(
            conn,
            "pivots",
            ("id", "dayid", "layer", "tickid", "ts", "px", "ptype", "pivotno", "dayrow"),
        )
        require_table_columns(conn, "tepisode", TEPISODE_COLUMNS)
        if not table_columns(conn, "tconfirm"):
            raise RuntimeError("public.tconfirm does not exist. Apply the required DDL first via sql.html.")
        resolve_insert_columns(conn)

        days = list_days(conn, args)
        logger.info(
            "START buildTconfirm | day_id=%s start_day_id=%s end_day_id=%s days=%s buildver=%s episode_buildver=%s dir=%s anchor_layer=%s cascade_layer=%s",
            args.day_id,
            args.start_day_id,
            args.end_day_id,
            len(days),
            args.buildver,
            args.episode_buildver,
            args.dir,
            args.anchor_layer,
            args.cascade_layer,
        )

        totals = {
            "days": 0,
            "deleted": 0,
            "tepisodes": 0,
            "skipped": 0,
            "inserted": 0,
            "confirmed": 0,
            "invalidated": 0,
            "unfinished": 0,
        }
        for day in days:
            try:
                stats = process_day(conn, logger, day, args)
                totals["days"] += 1
                totals["deleted"] += int(stats["deleted"])
                totals["tepisodes"] += int(stats["tepisodes"])
                totals["skipped"] += int(stats["skipped"])
                totals["inserted"] += int(stats["inserted"])
                totals["confirmed"] += int(stats["confirmed"])
                totals["invalidated"] += int(stats["invalidated"])
                totals["unfinished"] += int(stats["unfinished"])
            except Exception:
                conn.rollback()
                logger.exception("DAY error | day_id=%s", day.id)
                raise

        logger.info(
            "FINISH buildTconfirm | days=%s deleted=%s tepisodes_read=%s tepisodes_skipped=%s inserted=%s confirmed=%s invalidated=%s unfinished=%s buildver=%s episode_buildver=%s",
            totals["days"],
            totals["deleted"],
            totals["tepisodes"],
            totals["skipped"],
            totals["inserted"],
            totals["confirmed"],
            totals["invalidated"],
            totals["unfinished"],
            args.buildver,
            args.episode_buildver,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
