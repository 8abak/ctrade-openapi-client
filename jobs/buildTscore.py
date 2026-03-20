"""
Usage:
  python -m jobs.buildTscore
  python -m jobs.buildTscore --day-id 123
  python -m jobs.buildTscore --start-day-id 100 --end-day-id 150

Build public.tscore from public.tconfirm, driven by public.days.
Each row is a first-pass Layer 5 structural score evaluation for one top-direction confirm row.
"""

from __future__ import annotations

import argparse
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


SCORE_NAME = "StructScore"
SCORE_VERSION = "v1"
TARGET_DIR = "top"

TEXT_TYPES = {"text", "character varying", "character"}

TCONFIRM_REQUIRED_COLUMNS = (
    "id",
    "dayid",
    "dir",
    "confirmstate",
    "microreversalticks",
    "lowerhighticks",
    "lowerlowticks",
    "breakhighticks",
    "invalidated",
    "incomingmove200",
    "kalminusk2athigh",
    "compression50to200",
    "truthmatch",
    "inzone",
    "zonepos",
    "buildver",
)

TSCORE_REQUIRED_COLUMNS = (
    "dayid",
    "dir",
    "scorename",
    "scorever",
    "structurescore",
    "contextscore",
    "truthscore",
    "penaltyscore",
    "totalscore",
    "scoregrade",
    "reason",
    "sourcebuildver",
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
    breakhighticks: Optional[int]
    invalidated: Optional[bool]
    incomingmove200: Optional[float]
    kalminusk2athigh: Optional[float]
    compression50to200: Optional[float]
    truthmatch: Optional[bool]
    inzone: Optional[bool]
    zonepos: Optional[str]
    sourcebuildver: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Layer 5 tscore rows from tconfirm.")
    add_day_args(parser)
    parser.add_argument(
        "--confirm-buildver",
        default=DEFAULT_CONFIRM_BUILDVER,
        help="tconfirm build version used as the source (defaults to layer3.v1)",
    )
    parser.add_argument("--dir", default=TARGET_DIR)
    parser.add_argument("--scorename", default=SCORE_NAME)
    parser.add_argument("--scorever", default=SCORE_VERSION)
    args = parser.parse_args()
    args.dir = str(args.dir).strip().lower()
    args.scorename = str(args.scorename).strip()
    args.scorever = str(args.scorever).strip()
    return args


def validate_score_args(args: argparse.Namespace) -> None:
    if args.dir != TARGET_DIR:
        raise RuntimeError("buildTscore currently supports only dir='top'.")
    if args.scorename != SCORE_NAME or args.scorever != SCORE_VERSION:
        raise RuntimeError(f"buildTscore currently supports only {SCORE_NAME} {SCORE_VERSION}.")


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
        "public.tconfirm must contain tepisodeid (or episodeid) to support one-row-per-confirm score builds."
    )


def resolve_insert_columns(conn) -> Tuple[List[str], Dict[str, str]]:
    col_types = load_column_types(conn, "tscore")
    present = set(col_types)
    if not present:
        raise RuntimeError("public.tscore does not exist. Apply the required DDL first via sql.html.")

    missing = [col for col in TSCORE_REQUIRED_COLUMNS if col not in present]
    if missing:
        raise RuntimeError(
            f"Missing required table/columns for public.tscore: {', '.join(missing)}. "
            "Apply the required DDL first via sql.html."
        )

    episode_col = "tepisodeid" if "tepisodeid" in present else ("episodeid" if "episodeid" in present else None)
    if episode_col is None:
        raise RuntimeError(
            "public.tscore must contain tepisodeid (or episodeid) to support one-row-per-confirm score builds."
        )

    confirm_col = "tconfirmid" if "tconfirmid" in present else ("confirmid" if "confirmid" in present else None)
    if confirm_col is None:
        raise RuntimeError(
            "public.tscore must contain tconfirmid (or confirmid) to support one-row-per-confirm score builds."
        )

    ordered = [
        "dayid",
        episode_col,
        confirm_col,
        "dir",
        "scorename",
        "scorever",
        "structurescore",
        "contextscore",
        "truthscore",
        "penaltyscore",
        "totalscore",
        "scoregrade",
        "reason",
        "sourcebuildver",
        "createdts",
    ]
    if "updatedts" in present:
        ordered.append("updatedts")
    return ordered, col_types


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
                breakhighticks,
                invalidated,
                incomingmove200,
                kalminusk2athigh,
                compression50to200,
                truthmatch,
                inzone,
                zonepos,
                buildver
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
            breakhighticks=int(row["breakhighticks"]) if row["breakhighticks"] is not None else None,
            invalidated=bool(row["invalidated"]) if row["invalidated"] is not None else None,
            incomingmove200=float(row["incomingmove200"]) if row["incomingmove200"] is not None else None,
            kalminusk2athigh=(
                float(row["kalminusk2athigh"]) if row["kalminusk2athigh"] is not None else None
            ),
            compression50to200=(
                float(row["compression50to200"]) if row["compression50to200"] is not None else None
            ),
            truthmatch=bool(row["truthmatch"]) if row["truthmatch"] is not None else None,
            inzone=bool(row["inzone"]) if row["inzone"] is not None else None,
            zonepos=str(row["zonepos"]) if row["zonepos"] is not None else None,
            sourcebuildver=str(row["buildver"]),
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


def delete_day_score_rows(
    conn,
    *,
    day_id: int,
    dir_name: str,
    scorename: str,
    scorever: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.tscore
            WHERE dayid = %s
              AND dir = %s
              AND scorename = %s
              AND scorever = %s
            """,
            (int(day_id), str(dir_name), str(scorename), str(scorever)),
        )
        return int(cur.rowcount or 0)


def coerce_value(value, col_type: str):
    if value is None:
        return None
    if col_type in TEXT_TYPES and isinstance(value, bool):
        return "true" if value else "false"
    return value


def score_descending_threshold(value: Optional[float], thresholds: Sequence[Tuple[float, float]]) -> float:
    if value is None:
        return 0.0
    current = float(value)
    for minimum, score in thresholds:
        if current >= float(minimum):
            return float(score)
    return 0.0


def score_ascending_threshold(value: Optional[float], thresholds: Sequence[Tuple[float, float]]) -> float:
    if value is None:
        return 0.0
    current = float(value)
    for maximum, score in thresholds:
        if current <= float(maximum):
            return float(score)
    return 0.0


def structure_score(row: TconfirmRow) -> float:
    micro_score = score_ascending_threshold(
        row.microreversalticks,
        (
            (60, 20),
            (120, 16),
            (240, 12),
            (400, 8),
            (700, 4),
        ),
    )
    lowerhigh_score = score_ascending_threshold(
        row.lowerhighticks,
        (
            (200, 20),
            (400, 16),
            (700, 12),
            (1000, 8),
            (1500, 4),
        ),
    )
    lowerlow_score = score_ascending_threshold(
        row.lowerlowticks,
        (
            (300, 20),
            (600, 16),
            (1000, 12),
            (1500, 8),
            (2500, 4),
        ),
    )
    return float(micro_score + lowerhigh_score + lowerlow_score)


def context_score(row: TconfirmRow) -> float:
    incoming_score = score_descending_threshold(
        row.incomingmove200,
        (
            (3.0, 10),
            (2.18, 8),
            (1.5, 6),
            (1.0, 4),
            (0.5, 2),
        ),
    )
    kal_score = score_descending_threshold(
        row.kalminusk2athigh,
        (
            (1.0, 10),
            (0.62, 8),
            (0.4, 6),
            (0.2, 4),
            (0.0, 2),
        ),
    )
    compression_score = score_ascending_threshold(
        row.compression50to200,
        (
            (0.30, 5),
            (0.37156, 4),
            (0.45, 3),
            (0.60, 2),
            (0.80, 1),
        ),
    )
    return float(incoming_score + kal_score + compression_score)


def truth_score(row: TconfirmRow) -> float:
    if bool(row.truthmatch):
        return 15.0
    if bool(row.inzone):
        return 12.0

    zone_pos = (row.zonepos or "").strip().lower()
    if zone_pos == "nearbefore":
        return 8.0
    if zone_pos == "nearafter":
        return 6.0
    if zone_pos == "outside":
        return 2.0
    return 0.0


def penalty_score(row: TconfirmRow) -> float:
    total = 0.0

    if bool(row.invalidated):
        total += 25.0

    confirm_state = (row.confirmstate or "").strip().lower()
    if confirm_state == "invalidated":
        total += 15.0
    elif confirm_state == "unfinished":
        total += 10.0
    elif confirm_state == "confirmed":
        total += 0.0
    else:
        total += 10.0

    total += score_ascending_threshold(
        row.breakhighticks,
        (
            (100, 10),
            (250, 7),
            (500, 4),
            (1000, 2),
        ),
    )
    return float(total)


def total_score(
    *,
    structurescore: float,
    contextscore: float,
    truthscore: float,
    penaltyscore: float,
) -> float:
    raw_score = float(structurescore + contextscore + truthscore - penaltyscore)
    if raw_score < 0.0:
        return 0.0
    if raw_score > 100.0:
        return 100.0
    return raw_score


def score_grade(value: float) -> str:
    total = float(value)
    if total >= 90.0:
        return "A"
    if total >= 75.0:
        return "B"
    if total >= 60.0:
        return "C"
    if total >= 40.0:
        return "D"
    return "F"


def score_reason(row: TconfirmRow, totalscore: float) -> str:
    if bool(row.invalidated):
        reason = "invalidated"
    elif float(totalscore) >= 75.0:
        reason = "strong"
    elif float(totalscore) >= 60.0:
        reason = "good"
    elif float(totalscore) >= 40.0:
        reason = "moderate"
    else:
        reason = "weak"

    if (
        row.microreversalticks is None
        and row.lowerhighticks is None
        and row.lowerlowticks is None
    ):
        reason = "missingstructure"
    if (
        row.incomingmove200 is None
        and row.kalminusk2athigh is None
        and row.compression50to200 is None
    ):
        reason = "missingcontext"
    return reason


def make_score_row(
    *,
    row: TconfirmRow,
    args: argparse.Namespace,
    created_at: datetime,
    insert_cols: Sequence[str],
    col_types: Dict[str, str],
) -> Dict[str, object]:
    structurescore = structure_score(row)
    contextscore = context_score(row)
    truthscore_value = truth_score(row)
    penaltyscore_value = penalty_score(row)
    totalscore_value = total_score(
        structurescore=structurescore,
        contextscore=contextscore,
        truthscore=truthscore_value,
        penaltyscore=penaltyscore_value,
    )
    scoregrade_value = score_grade(totalscore_value)
    reason_value = score_reason(row, totalscore_value)

    values = {
        "dayid": int(row.dayid),
        "tepisodeid": int(row.tepisodeid),
        "episodeid": int(row.tepisodeid),
        "tconfirmid": int(row.id),
        "confirmid": int(row.id),
        "dir": str(row.dir),
        "scorename": str(args.scorename),
        "scorever": str(args.scorever),
        "structurescore": float(structurescore),
        "contextscore": float(contextscore),
        "truthscore": float(truthscore_value),
        "penaltyscore": float(penaltyscore_value),
        "totalscore": float(totalscore_value),
        "scoregrade": str(scoregrade_value),
        "reason": str(reason_value),
        "sourcebuildver": str(row.sourcebuildver),
        "createdts": created_at,
        "updatedts": created_at,
    }

    out: Dict[str, object] = {}
    for col in insert_cols:
        out[col] = coerce_value(values.get(col), col_types[col])
    return out


def insert_tscores(conn, insert_cols: Sequence[str], rows: List[Dict[str, object]]) -> int:
    if not rows:
        return 0

    sql_cols = ", ".join(insert_cols)
    values = [tuple(row.get(col) for col in insert_cols) for row in rows]
    return execute_values_insert(
        conn,
        f"""
        INSERT INTO public.tscore (
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
) -> Dict[str, int]:
    buildver_counts = load_tconfirm_buildver_counts(conn, day_id=day.id, dir_name=args.dir)
    deleted = delete_day_score_rows(
        conn,
        day_id=day.id,
        dir_name=args.dir,
        scorename=args.scorename,
        scorever=args.scorever,
    )
    tconfirms = load_tconfirms(
        conn,
        day_id=day.id,
        confirm_buildver=args.confirm_buildver,
        dir_name=args.dir,
        episode_col=episode_col,
    )
    source_rows_found = len(tconfirms)

    logger.info(
        "DAY source | day_id=%s source_rows_found=%s confirm_buildver=%s dir=%s available_buildvers=%s",
        day.id,
        source_rows_found,
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
        make_score_row(
            row=row,
            args=args,
            created_at=created_at,
            insert_cols=insert_cols,
            col_types=col_types,
        )
        for row in tconfirms
    ]
    inserted = insert_tscores(conn, insert_cols, rows)
    conn.commit()

    grade_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    for item in rows:
        grade = str(item["scoregrade"])
        reason = str(item["reason"])
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    logger.info(
        "DAY finish | day_id=%s deleted=%s source_rows_found=%s inserted=%s grades=%s reasons=%s scorename=%s scorever=%s confirm_buildver=%s dir=%s",
        day.id,
        deleted,
        source_rows_found,
        inserted,
        ",".join(f"{k}:{v}" for k, v in sorted(grade_counts.items())) or "none",
        ",".join(f"{k}:{v}" for k, v in sorted(reason_counts.items())) or "none",
        args.scorename,
        args.scorever,
        args.confirm_buildver,
        args.dir,
    )
    return {
        "deleted": deleted,
        "source_rows_found": source_rows_found,
        "inserted": inserted,
        "grade_a": grade_counts.get("A", 0),
        "grade_b": grade_counts.get("B", 0),
        "grade_c": grade_counts.get("C", 0),
        "grade_d": grade_counts.get("D", 0),
        "grade_f": grade_counts.get("F", 0),
    }


def main() -> None:
    args = parse_args()
    logger = setup_logger("buildTscore", "buildTscore.log")

    validate_score_args(args)

    conn = get_conn()
    conn.autocommit = False

    try:
        require_table_columns(conn, "days", ("id", "startid", "endid", "startts", "endts"))
        episode_col = resolve_tconfirm_episode_col(conn)
        insert_cols, col_types = resolve_insert_columns(conn)

        days = list_days(conn, args)
        logger.info(
            "START buildTscore | day_id=%s start_day_id=%s end_day_id=%s days=%s confirm_buildver=%s scorename=%s scorever=%s dir=%s",
            args.day_id,
            args.start_day_id,
            args.end_day_id,
            len(days),
            args.confirm_buildver,
            args.scorename,
            args.scorever,
            args.dir,
        )

        totals = {
            "days": 0,
            "deleted": 0,
            "source_rows_found": 0,
            "inserted": 0,
            "grade_a": 0,
            "grade_b": 0,
            "grade_c": 0,
            "grade_d": 0,
            "grade_f": 0,
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
                )
                totals["days"] += 1
                totals["deleted"] += int(stats["deleted"])
                totals["source_rows_found"] += int(stats["source_rows_found"])
                totals["inserted"] += int(stats["inserted"])
                totals["grade_a"] += int(stats["grade_a"])
                totals["grade_b"] += int(stats["grade_b"])
                totals["grade_c"] += int(stats["grade_c"])
                totals["grade_d"] += int(stats["grade_d"])
                totals["grade_f"] += int(stats["grade_f"])
            except Exception:
                conn.rollback()
                logger.exception("DAY error | day_id=%s", day.id)
                raise

        logger.info(
            "FINISH buildTscore | days=%s deleted=%s source_rows_found=%s inserted=%s grades=%s confirm_buildver=%s scorename=%s scorever=%s dir=%s",
            totals["days"],
            totals["deleted"],
            totals["source_rows_found"],
            totals["inserted"],
            ",".join(
                [
                    f"A:{totals['grade_a']}",
                    f"B:{totals['grade_b']}",
                    f"C:{totals['grade_c']}",
                    f"D:{totals['grade_d']}",
                    f"F:{totals['grade_f']}",
                ]
            ),
            args.confirm_buildver,
            args.scorename,
            args.scorever,
            args.dir,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
