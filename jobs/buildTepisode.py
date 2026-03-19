"""
Usage:
  python -m jobs.buildTepisode
  python -m jobs.buildTepisode --day-id 123
  python -m jobs.buildTepisode --start-day-id 100 --end-day-id 150

Build public.tepisode from public.pivots, driven by public.days.
Each row is a compressed top episode derived from nano high pivots.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2.extras

from backend.db import get_conn
from jobs.layer2common import (
    DEFAULT_BUILDVER,
    DEFAULT_TICK_SIZE,
    absolute_duration_ms,
    DayRow,
    PivotRow,
    add_day_args,
    delete_day_rows,
    duration_ms,
    execute_values_insert,
    list_days,
    load_day_pivots,
    price_span_to_ticks,
    require_table_columns,
    setup_logger,
)


DEFAULT_MERGE_GAP_MS = 90_000
DEFAULT_MERGE_GAP_TICKS = 12
DEFAULT_ZONE_NEAR_MS = 90_000
TARGET_DIR = "top"
TARGET_EPISODE_STATE = "closed"
SOURCE_LAYER = "nano"
SOURCE_PTYPE = "h"

TZONE_COLUMNS = (
    "id",
    "dayid",
    "pivotid",
    "layername",
    "dir",
    "startts",
    "endts",
    "centerts",
    "topprice",
    "lowprice",
    "highprice",
    "widthticks",
    "widthms",
    "status",
    "buildver",
    "createdts",
    "updatedts",
)

TEPISODE_COLUMNS = (
    "id",
    "dayid",
    "tzoneid",
    "dir",
    "episodestate",
    "firstpivotid",
    "lastpivotid",
    "reppivotid",
    "firstts",
    "lastts",
    "repts",
    "topprice",
    "lowprice",
    "highprice",
    "pivotcount",
    "spanticks",
    "spanms",
    "zonepos",
    "buildver",
    "createdts",
    "updatedts",
)


@dataclass
class TzoneRow:
    id: int
    centerts: object
    startts: object
    endts: object
    topprice: float


@dataclass
class EpisodeAccumulator:
    firstpivot: PivotRow
    lastpivot: PivotRow
    reppivot: PivotRow
    lowprice: float
    highprice: float
    pivotcount: int

    @classmethod
    def from_pivot(cls, pivot: PivotRow) -> "EpisodeAccumulator":
        return cls(
            firstpivot=pivot,
            lastpivot=pivot,
            reppivot=pivot,
            lowprice=float(pivot.px),
            highprice=float(pivot.px),
            pivotcount=1,
        )

    def can_merge(self, pivot: PivotRow, *, max_gap_ms: int, price_gap: float) -> bool:
        time_gap = duration_ms(self.lastpivot.ts, pivot.ts)
        if time_gap > int(max_gap_ms):
            return False

        low_bound = self.lowprice - float(price_gap)
        high_bound = self.highprice + float(price_gap)
        return low_bound <= float(pivot.px) <= high_bound

    def add(self, pivot: PivotRow) -> None:
        self.lastpivot = pivot
        self.lowprice = min(self.lowprice, float(pivot.px))
        self.highprice = max(self.highprice, float(pivot.px))
        self.pivotcount += 1

        rep = self.reppivot
        if float(pivot.px) > float(rep.px):
            self.reppivot = pivot
        elif float(pivot.px) == float(rep.px) and (pivot.ts, pivot.id) >= (rep.ts, rep.id):
            self.reppivot = pivot


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Layer 2 tepisode rows from nano high pivots.")
    add_day_args(parser)
    parser.add_argument("--buildver", default=DEFAULT_BUILDVER, help="Target build version")
    parser.add_argument(
        "--zone-buildver",
        default=None,
        help="tzone build version used for linking (defaults to --buildver)",
    )
    parser.add_argument(
        "--delete-all-buildvers",
        action="store_true",
        help="Delete all existing day rows regardless of buildver before rebuilding",
    )
    parser.add_argument("--tick-size", type=float, default=DEFAULT_TICK_SIZE)
    parser.add_argument("--merge-gap-ms", type=int, default=DEFAULT_MERGE_GAP_MS)
    parser.add_argument("--merge-gap-ticks", type=int, default=DEFAULT_MERGE_GAP_TICKS)
    parser.add_argument("--zone-near-ms", type=int, default=DEFAULT_ZONE_NEAR_MS)
    parser.add_argument("--episodestate", default=TARGET_EPISODE_STATE)
    parser.add_argument("--dir", default=TARGET_DIR)
    parser.add_argument("--source-layer", default=SOURCE_LAYER)
    return parser.parse_args()


def load_tzones(conn, *, day_id: int, buildver: str, dir_name: str) -> List[TzoneRow]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, centerts, startts, endts, topprice
            FROM public.tzone
            WHERE dayid = %s
              AND buildver = %s
              AND dir = %s
            ORDER BY centerts ASC, id ASC
            """,
            (int(day_id), str(buildver), str(dir_name)),
        )
        rows = cur.fetchall()

    return [
        TzoneRow(
            id=int(row["id"]),
            centerts=row["centerts"],
            startts=row["startts"],
            endts=row["endts"],
            topprice=float(row["topprice"]),
        )
        for row in rows
    ]


def compress_episodes(pivots: List[PivotRow], args: argparse.Namespace) -> List[EpisodeAccumulator]:
    if not pivots:
        return []

    price_gap = max(0, int(args.merge_gap_ticks)) * float(args.tick_size)
    episodes: List[EpisodeAccumulator] = []
    current = EpisodeAccumulator.from_pivot(pivots[0])

    for pivot in pivots[1:]:
        if current.can_merge(pivot, max_gap_ms=max(0, int(args.merge_gap_ms)), price_gap=price_gap):
            current.add(pivot)
            continue
        episodes.append(current)
        current = EpisodeAccumulator.from_pivot(pivot)

    episodes.append(current)
    return episodes


def classify_zone_position(rep_ts, zone: TzoneRow, near_ms: int) -> str:
    if zone.startts <= rep_ts <= zone.endts:
        return "inside"
    if rep_ts < zone.startts and duration_ms(rep_ts, zone.startts) <= int(near_ms):
        return "nearbefore"
    if rep_ts > zone.endts and duration_ms(zone.endts, rep_ts) <= int(near_ms):
        return "nearafter"
    return "outside"


def pick_zone(episode: EpisodeAccumulator, zones: List[TzoneRow], near_ms: int) -> Tuple[Optional[int], str]:
    if not zones:
        return None, "unassigned"

    rep_ts = episode.reppivot.ts
    rep_px = float(episode.reppivot.px)
    best = min(
        zones,
        key=lambda zone: (
            absolute_duration_ms(zone.centerts, rep_ts),
            abs(float(zone.topprice) - rep_px),
            zone.id,
        ),
    )
    return int(best.id), classify_zone_position(rep_ts, best, near_ms)


def make_tepisode_rows(
    day: DayRow,
    episodes: List[EpisodeAccumulator],
    zones: List[TzoneRow],
    args: argparse.Namespace,
) -> List[tuple]:
    created_at = datetime.now(timezone.utc)
    rows: List[tuple] = []

    for episode in episodes:
        zone_id, zone_pos = pick_zone(episode, zones, max(0, int(args.zone_near_ms)))
        rows.append(
            (
                int(day.id),
                zone_id,
                str(args.dir),
                str(args.episodestate),
                int(episode.firstpivot.id),
                int(episode.lastpivot.id),
                int(episode.reppivot.id),
                episode.firstpivot.ts,
                episode.lastpivot.ts,
                episode.reppivot.ts,
                float(episode.reppivot.px),
                float(episode.lowprice),
                float(episode.highprice),
                int(episode.pivotcount),
                price_span_to_ticks(episode.lowprice, episode.highprice, float(args.tick_size)),
                duration_ms(episode.firstpivot.ts, episode.lastpivot.ts),
                zone_pos,
                str(args.buildver),
                created_at,
                created_at,
            )
        )
    return rows


def insert_tepisodes(conn, rows: List[tuple]) -> int:
    return execute_values_insert(
        conn,
        """
        INSERT INTO public.tepisode (
            dayid, tzoneid, dir, episodestate, firstpivotid, lastpivotid, reppivotid,
            firstts, lastts, repts, topprice, lowprice, highprice, pivotcount,
            spanticks, spanms, zonepos, buildver, createdts, updatedts
        )
        VALUES %s
        """,
        rows,
        page_size=1000,
    )


def process_day(conn, logger, day: DayRow, args: argparse.Namespace) -> Dict[str, int]:
    deleted = delete_day_rows(
        conn,
        table="tepisode",
        day_id=day.id,
        buildver=args.buildver,
        delete_all_buildvers=bool(args.delete_all_buildvers),
    )
    pivots = load_day_pivots(conn, day_id=day.id, layer=args.source_layer, ptype=SOURCE_PTYPE)
    zones = load_tzones(
        conn,
        day_id=day.id,
        buildver=args.zone_buildver or args.buildver,
        dir_name=args.dir,
    )
    episodes = compress_episodes(pivots, args)
    rows = make_tepisode_rows(day, episodes, zones, args)
    inserted = insert_tepisodes(conn, rows)
    conn.commit()

    logger.info(
        "DAY finish | day_id=%s deleted=%s nano_high_pivots=%s zones=%s episodes=%s inserted=%s buildver=%s zone_buildver=%s",
        day.id,
        deleted,
        len(pivots),
        len(zones),
        len(episodes),
        inserted,
        args.buildver,
        args.zone_buildver or args.buildver,
    )
    return {
        "deleted": deleted,
        "pivots": len(pivots),
        "zones": len(zones),
        "episodes": len(episodes),
        "inserted": inserted,
    }


def main() -> None:
    args = parse_args()
    logger = setup_logger("buildTepisode", "buildTepisode.log")

    conn = get_conn()
    conn.autocommit = False

    try:
        require_table_columns(conn, "days", ("id", "startid", "endid", "startts", "endts"))
        require_table_columns(
            conn,
            "pivots",
            ("id", "dayid", "layer", "tickid", "ts", "px", "ptype", "pivotno", "dayrow"),
        )
        require_table_columns(conn, "tzone", TZONE_COLUMNS)
        require_table_columns(conn, "tepisode", TEPISODE_COLUMNS)

        days = list_days(conn, args)
        logger.info(
            "START buildTepisode | day_id=%s start_day_id=%s end_day_id=%s days=%s buildver=%s zone_buildver=%s source_layer=%s",
            args.day_id,
            args.start_day_id,
            args.end_day_id,
            len(days),
            args.buildver,
            args.zone_buildver or args.buildver,
            args.source_layer,
        )

        totals = {"days": 0, "deleted": 0, "pivots": 0, "zones": 0, "episodes": 0, "inserted": 0}
        for day in days:
            try:
                stats = process_day(conn, logger, day, args)
                totals["days"] += 1
                totals["deleted"] += int(stats["deleted"])
                totals["pivots"] += int(stats["pivots"])
                totals["zones"] += int(stats["zones"])
                totals["episodes"] += int(stats["episodes"])
                totals["inserted"] += int(stats["inserted"])
            except Exception:
                conn.rollback()
                logger.exception("DAY error | day_id=%s", day.id)
                raise

        logger.info(
            "FINISH buildTepisode | days=%s deleted=%s pivots=%s zones=%s episodes=%s inserted=%s buildver=%s zone_buildver=%s",
            totals["days"],
            totals["deleted"],
            totals["pivots"],
            totals["zones"],
            totals["episodes"],
            totals["inserted"],
            args.buildver,
            args.zone_buildver or args.buildver,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
