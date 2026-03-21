"""
Usage:
  python -m jobs.buildPipe --start-day 150
  python -m jobs.buildPipe --start-day 150 --end-day 175
  python -m jobs.buildPipe --start-day 150 --sleep-ms 250

Rebuild the market-structure pipeline from Layer 1 upward, one day at a time.
Trusted source tables are public.ticks and public.days. Everything above that is
rebuilt by reusing the existing builder jobs in sequence.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

import psycopg2.extras

from backend.db import get_conn, table_columns
from jobs.buildTconfirm import (
    ANCHOR_LAYER,
    CASCADE_LAYER,
    DEFAULT_BUILDVER as DEFAULT_TCONFIRM_BUILDVER,
    TARGET_DIR as TCONFIRM_TARGET_DIR,
)
from jobs.buildTepisode import SOURCE_LAYER as TEPISODE_SOURCE_LAYER, TARGET_DIR as TEPISODE_TARGET_DIR
from jobs.buildTrulehit import (
    DEFAULT_BUILDVER as DEFAULT_TRULEHIT_BUILDVER,
    RULE_NAME,
    RULE_VERSION,
    TARGET_DIR as TRULEHIT_TARGET_DIR,
)
from jobs.buildTscore import SCORE_NAME, SCORE_VERSION, TARGET_DIR as TSCORE_TARGET_DIR
from jobs.buildTzone import SOURCE_LAYER as TZONE_SOURCE_LAYER, TARGET_DIR as TZONE_TARGET_DIR
from jobs.layer2common import DEFAULT_BUILDVER as DEFAULT_LAYER2_BUILDVER


ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs" / "buildPipe"


def _resolve_active_dir() -> str:
    dirs = {
        str(TZONE_TARGET_DIR).strip().lower(),
        str(TEPISODE_TARGET_DIR).strip().lower(),
        str(TCONFIRM_TARGET_DIR).strip().lower(),
        str(TRULEHIT_TARGET_DIR).strip().lower(),
        str(TSCORE_TARGET_DIR).strip().lower(),
    }
    if len(dirs) != 1:
        raise RuntimeError(f"Inconsistent target direction across builders: {sorted(dirs)}")
    return next(iter(dirs))


ACTIVE_DIR = _resolve_active_dir()


@dataclass(frozen=True)
class DayRow:
    id: int
    startts: object
    endts: object


@dataclass(frozen=True)
class StageSpec:
    name: str
    module: str
    builder_log: str
    make_args: Callable[[int], List[str]]
    summarize: Callable[[int], str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the Layer 1+ market-structure pipeline day-by-day.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-day",
        "--start-day-id",
        dest="start_day",
        type=int,
        required=True,
        help="First days.id to rebuild",
    )
    parser.add_argument(
        "--end-day",
        "--end-day-id",
        dest="end_day",
        type=int,
        default=None,
        help="Last days.id to rebuild (defaults to latest available day)",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop after the first failed day instead of continuing to the next day",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Sleep this many milliseconds between completed/failed day attempts",
    )
    return parser.parse_args()


def setup_logger() -> tuple[logging.Logger, Path]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_path = LOG_DIR / f"buildPipe_{stamp}.log"

    logger = logging.getLogger("buildPipe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    return logger, log_path


def progress(logger: logging.Logger, message: str) -> None:
    print(f"[buildPipe] {message}", flush=True)
    logger.info("PROGRESS | %s", message)


def _format_ts(value: object) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _summarize_ids(values: Sequence[int], limit: int = 20) -> str:
    if not values:
        return "none"
    head = ",".join(str(v) for v in values[:limit])
    if len(values) <= limit:
        return head
    return f"{head},...(+{len(values) - limit} more)"


def resolve_days_window(start_day: int, end_day: Optional[int]) -> tuple[List[DayRow], int, int, List[int]]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT MIN(id) AS min_id, MAX(id) AS max_id, COUNT(*) AS rowcount FROM public.days")
            meta = cur.fetchone()
            if not meta or meta["rowcount"] in (None, 0):
                raise RuntimeError("public.days is empty; no rebuild window can be resolved.")

            latest_day = int(meta["max_id"])
            if int(start_day) > latest_day:
                raise RuntimeError(
                    f"Requested start day {int(start_day)} is above the latest available day {latest_day}."
                )

            resolved_end = latest_day if end_day is None else min(int(end_day), latest_day)
            if resolved_end < int(start_day):
                raise RuntimeError(
                    f"Resolved end day {resolved_end} is before requested start day {int(start_day)}."
                )

            cur.execute(
                """
                SELECT id, startts, endts
                FROM public.days
                WHERE id BETWEEN %s AND %s
                ORDER BY id ASC
                """,
                (int(start_day), int(resolved_end)),
            )
            rows = cur.fetchall()

        days = [DayRow(id=int(row["id"]), startts=row["startts"], endts=row["endts"]) for row in rows]
        if not days:
            raise RuntimeError(
                f"No day rows were found in public.days for the requested window {int(start_day)}..{resolved_end}."
            )

        ids = [day.id for day in days]
        if int(start_day) not in ids:
            raise RuntimeError(
                f"Requested start day {int(start_day)} does not exist in public.days; rerun with an existing day id."
            )

        present = set(ids)
        missing = [day_id for day_id in range(int(start_day), int(resolved_end) + 1) if day_id not in present]
        return days, latest_day, resolved_end, missing
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_one(sql_text: str, params: Sequence[object]) -> Dict[str, int]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql_text, tuple(params))
            row = cur.fetchone() or {}
        out: Dict[str, int] = {}
        for key, value in row.items():
            out[str(key)] = int(value or 0)
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass


def summarize_pivots(day_id: int) -> str:
    row = _fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE layer = 'macro')::bigint AS macro,
            COUNT(*) FILTER (WHERE layer = 'micro')::bigint AS micro,
            COUNT(*) FILTER (WHERE layer = 'nano')::bigint AS nano,
            COUNT(*) FILTER (WHERE layer = 'macro' AND ptype = 'h')::bigint AS macro_high
        FROM public.pivots
        WHERE dayid = %s
        """,
        (int(day_id),),
    )
    return (
        f"rows={row.get('total', 0)} macro={row.get('macro', 0)} micro={row.get('micro', 0)} "
        f"nano={row.get('nano', 0)} macro_high={row.get('macro_high', 0)}"
    )


def summarize_tzone(day_id: int) -> str:
    row = _fetch_one(
        """
        SELECT COUNT(*)::bigint AS total
        FROM public.tzone
        WHERE dayid = %s
          AND buildver = %s
          AND dir = %s
        """,
        (int(day_id), DEFAULT_LAYER2_BUILDVER, ACTIVE_DIR),
    )
    return f"rows={row.get('total', 0)} buildver={DEFAULT_LAYER2_BUILDVER} dir={ACTIVE_DIR}"


def summarize_tepisode(day_id: int) -> str:
    row = _fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE tzoneid IS NOT NULL)::bigint AS linked
        FROM public.tepisode
        WHERE dayid = %s
          AND buildver = %s
          AND dir = %s
        """,
        (int(day_id), DEFAULT_LAYER2_BUILDVER, ACTIVE_DIR),
    )
    return f"rows={row.get('total', 0)} linked={row.get('linked', 0)} buildver={DEFAULT_LAYER2_BUILDVER} dir={ACTIVE_DIR}"


def summarize_tconfirm(day_id: int) -> str:
    row = _fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE confirmstate = 'confirmed')::bigint AS confirmed,
            COUNT(*) FILTER (WHERE confirmstate = 'invalidated')::bigint AS invalidated,
            COUNT(*) FILTER (WHERE confirmstate = 'unfinished')::bigint AS unfinished
        FROM public.tconfirm
        WHERE dayid = %s
          AND buildver = %s
          AND dir = %s
        """,
        (int(day_id), DEFAULT_TCONFIRM_BUILDVER, ACTIVE_DIR),
    )
    return (
        f"rows={row.get('total', 0)} confirmed={row.get('confirmed', 0)} "
        f"invalidated={row.get('invalidated', 0)} unfinished={row.get('unfinished', 0)} "
        f"buildver={DEFAULT_TCONFIRM_BUILDVER} dir={ACTIVE_DIR}"
    )


def summarize_trulehit(day_id: int) -> str:
    conn = get_conn()
    try:
        cols = table_columns(conn, "trulehit")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    where = ["dayid = %s", "dir = %s", "rulename = %s", "rulever = %s"]
    params: List[object] = [int(day_id), ACTIVE_DIR, RULE_NAME, RULE_VERSION]
    if "buildver" in cols:
        where.append("buildver = %s")
        params.append(DEFAULT_TRULEHIT_BUILDVER)

    row = _fetch_one(
        f"""
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE COALESCE(ishit, FALSE) IS TRUE)::bigint AS hits,
            COUNT(*) FILTER (WHERE COALESCE(ishit, FALSE) IS FALSE)::bigint AS misses
        FROM public.trulehit
        WHERE {' AND '.join(where)}
        """,
        params,
    )
    buildver_text = DEFAULT_TRULEHIT_BUILDVER if "buildver" in cols else "none"
    return (
        f"rows={row.get('total', 0)} hits={row.get('hits', 0)} misses={row.get('misses', 0)} "
        f"buildver={buildver_text} rulename={RULE_NAME} rulever={RULE_VERSION} dir={ACTIVE_DIR}"
    )


def summarize_tscore(day_id: int) -> str:
    row = _fetch_one(
        """
        SELECT
            COUNT(*)::bigint AS total,
            COUNT(*) FILTER (WHERE scoregrade = 'A')::bigint AS grade_a,
            COUNT(*) FILTER (WHERE scoregrade = 'B')::bigint AS grade_b,
            COUNT(*) FILTER (WHERE scoregrade = 'C')::bigint AS grade_c,
            COUNT(*) FILTER (WHERE scoregrade = 'D')::bigint AS grade_d,
            COUNT(*) FILTER (WHERE scoregrade = 'F')::bigint AS grade_f
        FROM public.tscore
        WHERE dayid = %s
          AND dir = %s
          AND scorename = %s
          AND scorever = %s
          AND sourcebuildver = %s
        """,
        (int(day_id), ACTIVE_DIR, SCORE_NAME, SCORE_VERSION, DEFAULT_TCONFIRM_BUILDVER),
    )
    return (
        f"rows={row.get('total', 0)} grades=A:{row.get('grade_a', 0)},B:{row.get('grade_b', 0)},"
        f"C:{row.get('grade_c', 0)},D:{row.get('grade_d', 0)},F:{row.get('grade_f', 0)} "
        f"scorename={SCORE_NAME} scorever={SCORE_VERSION} dir={ACTIVE_DIR} "
        f"sourcebuildver={DEFAULT_TCONFIRM_BUILDVER}"
    )


def build_stage_specs() -> List[StageSpec]:
    return [
        StageSpec(
            name="buildPivots",
            module="jobs.buildPivots",
            builder_log="logs/buildPivots.log",
            make_args=lambda day_id: ["--day-id", str(day_id)],
            summarize=summarize_pivots,
        ),
        StageSpec(
            name="buildTzone",
            module="jobs.buildTzone",
            builder_log="logs/buildTzone.log",
            make_args=lambda day_id: [
                "--day-id",
                str(day_id),
                "--buildver",
                DEFAULT_LAYER2_BUILDVER,
                "--dir",
                ACTIVE_DIR,
                "--source-layer",
                TZONE_SOURCE_LAYER,
            ],
            summarize=summarize_tzone,
        ),
        StageSpec(
            name="buildTepisode",
            module="jobs.buildTepisode",
            builder_log="logs/buildTepisode.log",
            make_args=lambda day_id: [
                "--day-id",
                str(day_id),
                "--buildver",
                DEFAULT_LAYER2_BUILDVER,
                "--zone-buildver",
                DEFAULT_LAYER2_BUILDVER,
                "--dir",
                ACTIVE_DIR,
                "--source-layer",
                TEPISODE_SOURCE_LAYER,
            ],
            summarize=summarize_tepisode,
        ),
        StageSpec(
            name="buildTconfirm",
            module="jobs.buildTconfirm",
            builder_log="logs/buildTconfirm.log",
            make_args=lambda day_id: [
                "--day-id",
                str(day_id),
                "--buildver",
                DEFAULT_TCONFIRM_BUILDVER,
                "--episode-buildver",
                DEFAULT_LAYER2_BUILDVER,
                "--dir",
                ACTIVE_DIR,
                "--anchor-layer",
                ANCHOR_LAYER,
                "--cascade-layer",
                CASCADE_LAYER,
            ],
            summarize=summarize_tconfirm,
        ),
        StageSpec(
            name="buildTrulehit",
            module="jobs.buildTrulehit",
            builder_log="logs/buildTrulehit.log",
            make_args=lambda day_id: [
                "--day-id",
                str(day_id),
                "--buildver",
                DEFAULT_TRULEHIT_BUILDVER,
                "--confirm-buildver",
                DEFAULT_TCONFIRM_BUILDVER,
                "--dir",
                ACTIVE_DIR,
                "--rulename",
                RULE_NAME,
                "--rulever",
                RULE_VERSION,
            ],
            summarize=summarize_trulehit,
        ),
        StageSpec(
            name="buildTscore",
            module="jobs.buildTscore",
            builder_log="logs/buildTscore.log",
            make_args=lambda day_id: [
                "--day-id",
                str(day_id),
                "--confirm-buildver",
                DEFAULT_TCONFIRM_BUILDVER,
                "--dir",
                ACTIVE_DIR,
                "--scorename",
                SCORE_NAME,
                "--scorever",
                SCORE_VERSION,
            ],
            summarize=summarize_tscore,
        ),
    ]


def _log_output(logger: logging.Logger, day_id: int, stage: str, stream_name: str, text: str) -> None:
    if not text:
        return
    for line in text.splitlines():
        logger.info("STAGE %s | day_id=%s stage=%s | %s", stream_name, day_id, stage, line)


def run_stage(logger: logging.Logger, day_id: int, stage: StageSpec) -> str:
    cmd = [sys.executable, "-m", stage.module, *stage.make_args(day_id)]
    cmd_text = subprocess.list2cmdline(cmd)

    logger.info(
        "STAGE start | day_id=%s stage=%s command=%s builder_log=%s",
        day_id,
        stage.name,
        cmd_text,
        stage.builder_log,
    )
    progress(logger, f"day_id={day_id} stage={stage.name} start")

    start = time.perf_counter()
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    elapsed = time.perf_counter() - start

    stdout_text = proc.stdout.strip()
    stderr_text = proc.stderr.strip()
    _log_output(logger, day_id, stage.name, "stdout", stdout_text)
    _log_output(logger, day_id, stage.name, "stderr", stderr_text)

    try:
        summary = stage.summarize(day_id)
    except Exception as exc:
        logger.warning(
            "STAGE summary_error | day_id=%s stage=%s error=%s",
            day_id,
            stage.name,
            str(exc),
        )
        summary = f"unavailable ({exc})"

    logger.info(
        "STAGE finish | day_id=%s stage=%s returncode=%s elapsed_sec=%.3f summary=%s",
        day_id,
        stage.name,
        proc.returncode,
        elapsed,
        summary,
    )

    if proc.returncode != 0:
        raise RuntimeError(
            f"Stage {stage.name} failed for day {day_id} with exit code {proc.returncode}. "
            f"See {stage.builder_log} and the buildPipe log for details."
        )

    progress(logger, f"day_id={day_id} stage={stage.name} ok {summary}")
    return summary


def main() -> None:
    args = parse_args()
    logger, log_path = setup_logger()
    started_at = time.perf_counter()

    progress(logger, f"log={log_path}")
    logger.info(
        "PIPELINE config | start_day=%s end_day=%s stop_on_error=%s sleep_ms=%s",
        args.start_day,
        args.end_day,
        bool(args.stop_on_error),
        max(0, int(args.sleep_ms)),
    )
    logger.info(
        "PIPELINE versions | layer2_buildver=%s layer3_buildver=%s layer4_buildver=%s dir=%s "
        "tzone_source_layer=%s tepisode_source_layer=%s anchor_layer=%s cascade_layer=%s "
        "rule=%s/%s score=%s/%s",
        DEFAULT_LAYER2_BUILDVER,
        DEFAULT_TCONFIRM_BUILDVER,
        DEFAULT_TRULEHIT_BUILDVER,
        ACTIVE_DIR,
        TZONE_SOURCE_LAYER,
        TEPISODE_SOURCE_LAYER,
        ANCHOR_LAYER,
        CASCADE_LAYER,
        RULE_NAME,
        RULE_VERSION,
        SCORE_NAME,
        SCORE_VERSION,
    )

    try:
        days, latest_day, resolved_end, missing_days = resolve_days_window(args.start_day, args.end_day)
        stages = build_stage_specs()

        logger.info(
            "PIPELINE window | requested_start_day=%s requested_end_day=%s latest_day=%s resolved_end_day=%s days_found=%s",
            args.start_day,
            args.end_day,
            latest_day,
            resolved_end,
            len(days),
        )
        if missing_days:
            logger.warning(
                "PIPELINE missing_days | count=%s ids=%s",
                len(missing_days),
                _summarize_ids(missing_days),
            )
            progress(logger, f"missing day ids in range: {_summarize_ids(missing_days)}")

        progress(
            logger,
            f"window start_day={args.start_day} resolved_end_day={resolved_end} latest_day={latest_day} days={len(days)}",
        )

        days_attempted = 0
        days_completed = 0
        failures: List[Dict[str, object]] = []

        for idx, day in enumerate(days, start=1):
            day_started = time.perf_counter()
            days_attempted += 1
            logger.info(
                "DAY start | day_id=%s index=%s/%s startts=%s endts=%s",
                day.id,
                idx,
                len(days),
                _format_ts(day.startts),
                _format_ts(day.endts),
            )
            progress(logger, f"day_id={day.id} start ({idx}/{len(days)})")

            day_failed = False
            for stage in stages:
                try:
                    run_stage(logger, day.id, stage)
                except Exception as exc:
                    day_failed = True
                    failure = {
                        "day_id": day.id,
                        "stage": stage.name,
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    }
                    failures.append(failure)
                    logger.error(
                        "DAY failure | day_id=%s stage=%s error=%s",
                        day.id,
                        stage.name,
                        str(exc),
                    )
                    logger.error("DAY failure_traceback | day_id=%s stage=%s\n%s", day.id, stage.name, failure["traceback"])
                    progress(logger, f"day_id={day.id} failed at stage={stage.name}")
                    break

            day_elapsed = time.perf_counter() - day_started
            if not day_failed:
                days_completed += 1
                logger.info("DAY finish | day_id=%s status=ok elapsed_sec=%.3f", day.id, day_elapsed)
                progress(logger, f"day_id={day.id} complete elapsed={day_elapsed:.2f}s")
            else:
                logger.info("DAY finish | day_id=%s status=failed elapsed_sec=%.3f", day.id, day_elapsed)
                if args.stop_on_error:
                    progress(logger, "stop_on_error=true, stopping after first failed day")
                    break

            sleep_ms = max(0, int(args.sleep_ms))
            if sleep_ms > 0 and idx < len(days):
                logger.info("PIPELINE sleep | after_day_id=%s sleep_ms=%s", day.id, sleep_ms)
                time.sleep(sleep_ms / 1000.0)

        total_elapsed = time.perf_counter() - started_at
        logger.info(
            "PIPELINE finish | days_attempted=%s days_completed=%s failures=%s elapsed_sec=%.3f",
            days_attempted,
            days_completed,
            len(failures),
            total_elapsed,
        )
        for failure in failures:
            logger.info(
                "PIPELINE failure_item | day_id=%s stage=%s error=%s",
                failure["day_id"],
                failure["stage"],
                failure["error"],
            )

        progress(
            logger,
            f"finish attempted={days_attempted} completed={days_completed} failures={len(failures)} elapsed={total_elapsed:.2f}s",
        )
        if failures:
            raise SystemExit(1)
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("PIPELINE fatal | error=%s", str(exc))
        progress(logger, f"fatal error: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
