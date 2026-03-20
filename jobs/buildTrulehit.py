"""
Usage:
  python -m jobs.buildTrulehit
  python -m jobs.buildTrulehit --day-id 123
  python -m jobs.buildTrulehit --start-day-id 100 --end-day-id 150

Build public.trulehit from public.tconfirm, driven by public.days.
Each row is a first-pass Layer 4 rule-hit evaluation for one top-direction confirm row.
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg2.extras

from backend.db import get_conn, table_columns
from jobs.buildTconfirm import DEFAULT_BUILDVER as DEFAULT_CONFIRM_BUILDVER
from jobs.layer2common import (
    DayRow,
    add_day_args,
    execute_values_insert,
    list_days,
    require_table_columns,
    setup_logger,
)


DEFAULT_BUILDVER = "layer4.v1"
RULE_NAME = "StrictB"
RULE_VERSION = "v1"
TARGET_DIR = "top"

DEFAULT_MICROREVERSAL_MAX_TICKS = 60
DEFAULT_LOWERHIGH_MAX_TICKS = 200
DEFAULT_LOWERLOW_MAX_TICKS = 300
DEFAULT_INCOMINGMOVE200_MIN = 2.18
DEFAULT_KALMINUSK2_MIN = 0.62
DEFAULT_COMPRESSION_MAX = 0.37156

TEXT_TYPES = {"text", "character varying", "character"}

TCONFIRM_REQUIRED_COLUMNS = (
    "id",
    "dayid",
    "dir",
    "confirmstate",
    "microreversalticks",
    "lowerhighticks",
    "lowerlowticks",
    "incomingmove200",
    "kalminusk2athigh",
    "compression50to200",
    "buildver",
)

TRULEHIT_REQUIRED_COLUMNS = (
    "dayid",
    "dir",
    "rulename",
    "rulever",
    "ishit",
    "score",
    "reason",
    "createdts",
)


@dataclass
class TconfirmRow:
    id: int
    dayid: int
    tepisodeid: int
    dir: str
    confirmstate: Optional[str]
    microreversalticks: Optional[int]
    lowerhighticks: Optional[int]
    lowerlowticks: Optional[int]
    incomingmove200: Optional[float]
    kalminusk2athigh: Optional[float]
    compression50to200: Optional[float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Layer 4 trulehit rows from tconfirm.")
    add_day_args(parser)
    parser.add_argument("--buildver", default=DEFAULT_BUILDVER, help="Target build version")
    parser.add_argument(
        "--confirm-buildver",
        default=DEFAULT_CONFIRM_BUILDVER,
        help="tconfirm build version used as the source (defaults to layer3.v1)",
    )
    parser.add_argument(
        "--delete-all-buildvers",
        action="store_true",
        help="Delete all existing day rows for the rule slice regardless of buildver before rebuilding",
    )
    parser.add_argument("--dir", default=TARGET_DIR)
    parser.add_argument("--rulename", default=RULE_NAME)
    parser.add_argument("--rulever", default=RULE_VERSION)
    parser.add_argument("--microreversal-max-ticks", type=int, default=DEFAULT_MICROREVERSAL_MAX_TICKS)
    parser.add_argument("--lowerhigh-max-ticks", type=int, default=DEFAULT_LOWERHIGH_MAX_TICKS)
    parser.add_argument("--lowerlow-max-ticks", type=int, default=DEFAULT_LOWERLOW_MAX_TICKS)
    parser.add_argument("--incomingmove200-min", type=float, default=DEFAULT_INCOMINGMOVE200_MIN)
    parser.add_argument("--kalminusk2-min", type=float, default=DEFAULT_KALMINUSK2_MIN)
    parser.add_argument("--compression-max", type=float, default=DEFAULT_COMPRESSION_MAX)
    args = parser.parse_args()
    args.dir = str(args.dir).strip().lower()
    args.rulename = str(args.rulename).strip()
    args.rulever = str(args.rulever).strip()
    return args


def validate_rule_args(args: argparse.Namespace) -> None:
    if args.dir != TARGET_DIR:
        raise RuntimeError("buildTrulehit currently supports only dir='top'.")
    if args.rulename != RULE_NAME or args.rulever != RULE_VERSION:
        raise RuntimeError(f"buildTrulehit currently supports only {RULE_NAME} {RULE_VERSION}.")


def load_column_types(conn, table: str) -> Dict[str, str]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position ASC
            """,
            (str(table),),
        )
        rows = cur.fetchall()
    return {str(row["column_name"]): str(row["data_type"]) for row in rows}


def resolve_tconfirm_episode_col(conn) -> str:
    present = table_columns(conn, "tconfirm")
    if not present:
        raise RuntimeError("public.tconfirm does not exist. Apply the required DDL first via sql.html.")

    missing = [col for col in TCONFIRM_REQUIRED_COLUMNS if col not in present]
    if missing:
        raise RuntimeError(
            f"Missing required table/columns for public.tconfirm: {', '.join(missing)}. "
            "Apply the required DDL first via sql.html."
        )

    if "tepisodeid" in present:
        return "tepisodeid"
    if "episodeid" in present:
        return "episodeid"
    raise RuntimeError(
        "public.tconfirm must contain tepisodeid (or episodeid) to support one-row-per-confirm rule builds."
    )


def resolve_insert_columns(conn) -> Tuple[List[str], Dict[str, str], bool]:
    col_types = load_column_types(conn, "trulehit")
    present = set(col_types)
    if not present:
        raise RuntimeError("public.trulehit does not exist. Apply the required DDL first via sql.html.")

    missing = [col for col in TRULEHIT_REQUIRED_COLUMNS if col not in present]
    if missing:
        raise RuntimeError(
            f"Missing required table/columns for public.trulehit: {', '.join(missing)}. "
            "Apply the required DDL first via sql.html."
        )

    episode_col = "tepisodeid" if "tepisodeid" in present else ("episodeid" if "episodeid" in present else None)
    if episode_col is None:
        raise RuntimeError(
            "public.trulehit must contain tepisodeid (or episodeid) to support one-row-per-confirm rule builds."
        )

    confirm_col = "tconfirmid" if "tconfirmid" in present else ("confirmid" if "confirmid" in present else None)
    if confirm_col is None:
        raise RuntimeError(
            "public.trulehit must contain tconfirmid (or confirmid) to support one-row-per-confirm rule builds."
        )

    ordered = [
        "dayid",
        episode_col,
        confirm_col,
        "dir",
        "rulename",
        "rulever",
        "ishit",
        "score",
        "reason",
    ]
    has_buildver = "buildver" in present
    if has_buildver:
        ordered.append("buildver")
    ordered.append("createdts")
    if "updatedts" in present:
        ordered.append("updatedts")
    return ordered, col_types, has_buildver


def load_tconfirms(
    conn,
    *,
    day_id: int,
    confirm_buildver: str,
    dir_name: str,
    episode_col: str,
) -> List[TconfirmRow]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                id,
                dayid,
                {episode_col} AS tepisodeid,
                dir,
                confirmstate,
                microreversalticks,
                lowerhighticks,
                lowerlowticks,
                incomingmove200,
                kalminusk2athigh,
                compression50to200
            FROM public.tconfirm
            WHERE dayid = %s
              AND buildver = %s
              AND dir = %s
            ORDER BY id ASC
            """,
            (int(day_id), str(confirm_buildver), str(dir_name)),
        )
        rows = cur.fetchall()

    return [
        TconfirmRow(
            id=int(row["id"]),
            dayid=int(row["dayid"]),
            tepisodeid=int(row["tepisodeid"]),
            dir=str(row["dir"]),
            confirmstate=str(row["confirmstate"]) if row["confirmstate"] is not None else None,
            microreversalticks=(
                int(row["microreversalticks"]) if row["microreversalticks"] is not None else None
            ),
            lowerhighticks=int(row["lowerhighticks"]) if row["lowerhighticks"] is not None else None,
            lowerlowticks=int(row["lowerlowticks"]) if row["lowerlowticks"] is not None else None,
            incomingmove200=float(row["incomingmove200"]) if row["incomingmove200"] is not None else None,
            kalminusk2athigh=(
                float(row["kalminusk2athigh"]) if row["kalminusk2athigh"] is not None else None
            ),
            compression50to200=(
                float(row["compression50to200"]) if row["compression50to200"] is not None else None
            ),
        )
        for row in rows
    ]


def load_tconfirm_buildver_counts(conn, *, day_id: int, dir_name: str) -> Dict[str, int]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT buildver, COUNT(*)::bigint AS rowcount
            FROM public.tconfirm
            WHERE dayid = %s
              AND dir = %s
            GROUP BY buildver
            ORDER BY buildver ASC
            """,
            (int(day_id), str(dir_name)),
        )
        rows = cur.fetchall()
    return {str(row["buildver"]): int(row["rowcount"]) for row in rows}


def delete_day_rule_rows(
    conn,
    *,
    day_id: int,
    dir_name: str,
    buildver: str,
    delete_all_buildvers: bool,
    has_buildver: bool,
) -> int:
    where = [
        "dayid = %s",
        "dir = %s",
        "rulename = %s",
        "rulever = %s",
    ]
    params: List[object] = [
        int(day_id),
        str(dir_name),
        str(RULE_NAME),
        str(RULE_VERSION),
    ]
    if has_buildver and not delete_all_buildvers:
        where.append("buildver = %s")
        params.append(str(buildver))

    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM public.trulehit WHERE {' AND '.join(where)}", tuple(params))
        return int(cur.rowcount or 0)


def coerce_value(value, col_type: str):
    if value is None:
        return None
    if col_type in TEXT_TYPES and isinstance(value, bool):
        return "true" if value else "false"
    return value


def is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def evaluate_rule(row: TconfirmRow, args: argparse.Namespace) -> Tuple[bool, float, str]:
    if (row.confirmstate or "").lower() != "confirmed":
        return False, 0.0, "fail_confirmstate"

    required_values = (
        row.microreversalticks,
        row.lowerhighticks,
        row.lowerlowticks,
        row.incomingmove200,
        row.kalminusk2athigh,
        row.compression50to200,
    )
    if any(is_missing(value) for value in required_values):
        return False, 0.0, "fail_missingfield"

    if int(row.microreversalticks) > int(args.microreversal_max_ticks):
        return False, 0.0, "fail_microreversal"
    if int(row.lowerhighticks) > int(args.lowerhigh_max_ticks):
        return False, 0.0, "fail_lowerhigh"
    if int(row.lowerlowticks) > int(args.lowerlow_max_ticks):
        return False, 0.0, "fail_lowerlow"
    if float(row.incomingmove200) < float(args.incomingmove200_min):
        return False, 0.0, "fail_incomingmove"
    if float(row.kalminusk2athigh) < float(args.kalminusk2_min):
        return False, 0.0, "fail_kalminusk2"
    if float(row.compression50to200) > float(args.compression_max):
        return False, 0.0, "fail_compression"
    return True, 1.0, "pass"


def make_rule_row(
    *,
    row: TconfirmRow,
    args: argparse.Namespace,
    created_at: datetime,
    insert_cols: Sequence[str],
    col_types: Dict[str, str],
) -> Dict[str, object]:
    is_hit, score, reason = evaluate_rule(row, args)
    values = {
        "dayid": int(row.dayid),
        "tepisodeid": int(row.tepisodeid),
        "episodeid": int(row.tepisodeid),
        "tconfirmid": int(row.id),
        "confirmid": int(row.id),
        "dir": str(row.dir),
        "rulename": str(args.rulename),
        "rulever": str(args.rulever),
        "ishit": bool(is_hit),
        "score": float(score),
        "reason": str(reason),
        "buildver": str(args.buildver),
        "createdts": created_at,
        "updatedts": created_at,
    }

    out: Dict[str, object] = {}
    for col in insert_cols:
        out[col] = coerce_value(values.get(col), col_types[col])
    return out


def insert_trulehits(conn, insert_cols: Sequence[str], rows: List[Dict[str, object]]) -> int:
    if not rows:
        return 0

    sql_cols = ", ".join(insert_cols)
    values = [tuple(row.get(col) for col in insert_cols) for row in rows]
    return execute_values_insert(
        conn,
        f"""
        INSERT INTO public.trulehit (
            {sql_cols}
        )
        VALUES %s
        """,
        values,
        page_size=1000,
    )


def process_day(
    conn,
    logger,
    day: DayRow,
    args: argparse.Namespace,
    *,
    episode_col: str,
    insert_cols: Sequence[str],
    col_types: Dict[str, str],
    has_buildver: bool,
) -> Dict[str, int]:
    buildver_counts = load_tconfirm_buildver_counts(conn, day_id=day.id, dir_name=args.dir)
    deleted = delete_day_rule_rows(
        conn,
        day_id=day.id,
        dir_name=args.dir,
        buildver=args.buildver,
        delete_all_buildvers=bool(args.delete_all_buildvers),
        has_buildver=has_buildver,
    )
    tconfirms = load_tconfirms(
        conn,
        day_id=day.id,
        confirm_buildver=args.confirm_buildver,
        dir_name=args.dir,
        episode_col=episode_col,
    )
    source_rows_found = len(tconfirms)
    eligible_rows = source_rows_found
    skip_count = 0

    logger.info(
        "DAY source | day_id=%s source_rows_found=%s eligible=%s skips=%s confirm_buildver=%s dir=%s available_buildvers=%s",
        day.id,
        source_rows_found,
        eligible_rows,
        skip_count,
        args.confirm_buildver,
        args.dir,
        ",".join(f"{k}:{v}" for k, v in sorted(buildver_counts.items())) or "none",
    )
    if source_rows_found == 0:
        logger.warning(
            "DAY no_source_rows | day_id=%s confirm_buildver=%s dir=%s available_buildvers=%s",
            day.id,
            args.confirm_buildver,
            args.dir,
            ",".join(f"{k}:{v}" for k, v in sorted(buildver_counts.items())) or "none",
        )

    created_at = datetime.now(timezone.utc)
    rows = [
        make_rule_row(
            row=row,
            args=args,
            created_at=created_at,
            insert_cols=insert_cols,
            col_types=col_types,
        )
        for row in tconfirms
    ]
    inserted = insert_trulehits(conn, insert_cols, rows)
    conn.commit()

    reason_counts: Dict[str, int] = {}
    hits = 0
    misses = 0
    for item in rows:
        reason = str(item["reason"])
        if reason == "pass":
            hits += 1
        else:
            misses += 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    logger.info(
        "DAY finish | day_id=%s deleted=%s source_rows_found=%s eligible=%s inserted=%s hits=%s misses=%s skips=%s reasons=%s buildver=%s confirm_buildver=%s rulename=%s rulever=%s dir=%s",
        day.id,
        deleted,
        source_rows_found,
        eligible_rows,
        inserted,
        hits,
        misses,
        skip_count,
        ",".join(f"{k}:{v}" for k, v in sorted(reason_counts.items())) or "none",
        args.buildver,
        args.confirm_buildver,
        args.rulename,
        args.rulever,
        args.dir,
    )
    return {
        "deleted": deleted,
        "source_rows_found": source_rows_found,
        "eligible": eligible_rows,
        "inserted": inserted,
        "hits": hits,
        "misses": misses,
        "skips": skip_count,
    }


def main() -> None:
    args = parse_args()
    logger = setup_logger("buildTrulehit", "buildTrulehit.log")

    validate_rule_args(args)

    conn = get_conn()
    conn.autocommit = False

    try:
        require_table_columns(conn, "days", ("id", "startid", "endid", "startts", "endts"))
        episode_col = resolve_tconfirm_episode_col(conn)
        insert_cols, col_types, has_buildver = resolve_insert_columns(conn)

        days = list_days(conn, args)
        logger.info(
            "START buildTrulehit | day_id=%s start_day_id=%s end_day_id=%s days=%s buildver=%s confirm_buildver=%s rulename=%s rulever=%s dir=%s",
            args.day_id,
            args.start_day_id,
            args.end_day_id,
            len(days),
            args.buildver,
            args.confirm_buildver,
            args.rulename,
            args.rulever,
            args.dir,
        )

        totals = {
            "days": 0,
            "deleted": 0,
            "source_rows_found": 0,
            "eligible": 0,
            "inserted": 0,
            "hits": 0,
            "misses": 0,
            "skips": 0,
        }
        for day in days:
            try:
                stats = process_day(
                    conn,
                    logger,
                    day,
                    args,
                    episode_col=episode_col,
                    insert_cols=insert_cols,
                    col_types=col_types,
                    has_buildver=has_buildver,
                )
                totals["days"] += 1
                totals["deleted"] += int(stats["deleted"])
                totals["source_rows_found"] += int(stats["source_rows_found"])
                totals["eligible"] += int(stats["eligible"])
                totals["inserted"] += int(stats["inserted"])
                totals["hits"] += int(stats["hits"])
                totals["misses"] += int(stats["misses"])
                totals["skips"] += int(stats["skips"])
            except Exception:
                conn.rollback()
                logger.exception("DAY error | day_id=%s", day.id)
                raise

        logger.info(
            "FINISH buildTrulehit | days=%s deleted=%s source_rows_found=%s eligible=%s inserted=%s hits=%s misses=%s skips=%s buildver=%s confirm_buildver=%s rulename=%s rulever=%s dir=%s",
            totals["days"],
            totals["deleted"],
            totals["source_rows_found"],
            totals["eligible"],
            totals["inserted"],
            totals["hits"],
            totals["misses"],
            totals["skips"],
            args.buildver,
            args.confirm_buildver,
            args.rulename,
            args.rulever,
            args.dir,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
