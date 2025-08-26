"""
walkforward_batch.py
--------------------

Run a long walk-forward backtest over many ticks, one macro segment at a time.
After each newly closed macro leg:
  - detect micro events
  - resolve outcomes (for any events that now have forward data)
  - train on fully-resolved prior segments, predict on the next segment's events

Target: advance until reaching a tick bound (default: start_id + 2_000_000 - 1).

Outputs:
  - CSV with per-segment statistics
  - PNG chart of cumulative correct/incorrect over time
  - Console summary

Assumptions (matches the rest of the repo):
  - DB: PostgreSQL, SQLAlchemy engine available from backend.main: `engine`
  - Tables (as delivered): ticks, macro_segments, micro_events, outcomes, predictions
  - Predictions: a row is a "guess" if `decided = true` (i.e., p_tp >= threshold)
  - Correctness: outcome == 'TP' => correct; 'SL' or 'Timeout' => incorrect

Run:
  $ source venv/bin/activate
  $ python -m backend.walkforward_batch --start 1 --max-ticks 2000000

Optional args:
  --start START_TICK_ID  (default: min(ticks.id))
  --max-ticks N         (default: 2_000_000)
  --sleep-ms MS         (default: 0) sleep between steps
  --limit-steps K       (safety cap; default: very large)
  --report-tag TAG      (suffix for output dir)

Artifacts:
  backend/reports/wf2m-YYYYMMDD-HHMMSS[-TAG]/report.csv
  backend/reports/wf2m-YYYYMMDD-HHMMSS[-TAG]/cumulative.png
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Dict, Any, Optional, List, Tuple

# Make relative imports work when run as a module
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Reuse your engine + modules
from backend.main import engine  # type: ignore
from backend.label_macro_segments import BuildOrExtendSegments  # type: ignore
from backend.label_micro_events import DetectMicroEventsForLatestClosedSegment  # type: ignore
from backend.compute_outcomes import ResolveOutcomes  # type: ignore
from backend.train_predict import TrainAndPredict  # type: ignore

import pandas as pd  # pip install pandas
import matplotlib
matplotlib.use("Agg")  # headless save
import matplotlib.pyplot as plt

# -----------------------------
# SQL helpers
# -----------------------------

SQL_TICK_BOUNDS = """
SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM ticks;
"""

SQL_LAST_CLOSED_SEGMENT = """
SELECT segment_id, end_tick_id
FROM macro_segments
WHERE end_tick_id IS NOT NULL
ORDER BY end_tick_id DESC
LIMIT 1;
"""

SQL_EVAL = """
SELECT
  p.prediction_id, p.event_id, p.model_version, p.p_tp, p.threshold, p.decided, p.predicted_at,
  e.segment_id, e.tick_id,
  o.outcome, o.tp_hit_ts, o.sl_hit_ts, o.timeout_ts
FROM predictions p
JOIN micro_events e ON e.event_id = p.event_id
JOIN outcomes o ON o.event_id = p.event_id
ORDER BY e.segment_id, e.tick_id;
"""

SQL_SEG_LIST = """
SELECT segment_id, start_tick_id, end_tick_id, direction, confidence
FROM macro_segments
ORDER BY segment_id;
"""

def _nowstamp() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def jprint(msg: str):
    print(f"[{_nowstamp()}] {msg}", flush=True)

# -----------------------------
# Core driver
# -----------------------------

def get_tick_bounds() -> Tuple[int, int]:
    with engine.connect() as conn:
        r = conn.execute(pd.read_sql_query(SQL_TICK_BOUNDS, conn)).fetchone()  # type: ignore
    # Using pandas for portability, but we only need min/max
    with engine.connect() as conn2:
        row = conn2.execute(SQL_TICK_BOUNDS).fetchone()
    if row is None or row[0] is None or row[1] is None:
        raise RuntimeError("ticks table appears empty.")
    return int(row[0]), int(row[1])

def get_last_closed_segment_end_id() -> int:
    with engine.connect() as conn:
        row = conn.execute(SQL_LAST_CLOSED_SEGMENT).fetchone()
    if not row:
        return 0
    return int(row[1] or 0)

def step_once(target_end_tick_id: int) -> Dict[str, Any]:
    """
    Perform one "click": try to extend/build one new macro leg (if possible),
    then micro events, outcomes, train & predict. Return a dict summary.
    """
    out: Dict[str, Any] = {"macro": None, "micro": None, "outcomes": None, "predict": None}
    # 1) Build/extend segments, respecting target_end_tick_id
    macro = BuildOrExtendSegments(engine, until_tick_id=target_end_tick_id)
    out["macro"] = macro

    # 2) Detect micro entries for the latest CLOSED segment
    micro = DetectMicroEventsForLatestClosedSegment(engine)
    out["micro"] = micro

    # 3) Resolve outcomes for any events that now have enough forward data
    oc = ResolveOutcomes(engine)
    out["outcomes"] = oc

    # 4) Train & predict (train on fully-resolved past, predict on next segment’s events)
    pr = TrainAndPredict(engine)
    out["predict"] = pr
    return out

# -----------------------------
# Evaluation and reporting
# -----------------------------

def evaluate_predictions() -> pd.DataFrame:
    """
    Join predictions with outcomes and compute correctness for decided predictions.
    Returns a dataframe with:
      segment_id, tick_id, decided, outcome, correct (bool), ts (predicted_at)
    """
    with engine.connect() as conn:
        df = pd.read_sql(SQL_EVAL, conn)

    if df.empty:
        return df

    # A "guess" is decided==true
    df = df[df["decided"] == True].copy()

    # Correctness: TP = correct; SL/Timeout = incorrect
    df["correct"] = (df["outcome"] == "TP")

    # For temporal ordering use event tick_id (and segment_id as group)
    df.sort_values(["segment_id", "tick_id"], inplace=True, ignore_index=True)
    return df

def per_segment_summary(df_eval: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per segment: n_decided, n_correct, n_incorrect
    """
    if df_eval.empty:
        return pd.DataFrame(columns=["segment_id", "n_decided", "n_correct", "n_incorrect", "acc"])

    g = df_eval.groupby("segment_id", as_index=False).agg(
        n_decided=("correct", "size"),
        n_correct=("correct", "sum")
    )
    g["n_incorrect"] = g["n_decided"] - g["n_correct"]
    g["acc"] = (g["n_correct"] / g["n_decided"]).round(4).fillna(0)
    return g

def cumulative_plot(df_seg: pd.DataFrame, out_png: str):
    if df_seg.empty:
        # Create a tiny placeholder
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "No decided predictions to plot.", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(out_png, dpi=150)
        plt.close(fig)
        return

    df_seg = df_seg.sort_values("segment_id").copy()
    df_seg["cum_correct"]   = df_seg["n_correct"].cumsum()
    df_seg["cum_incorrect"] = df_seg["n_incorrect"].cumsum()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df_seg["segment_id"], df_seg["cum_correct"],   label="Cumulative correct",   linewidth=2)
    ax.plot(df_seg["segment_id"], df_seg["cum_incorrect"], label="Cumulative incorrect", linewidth=2)
    ax.set_xlabel("Segment ID")
    ax.set_ylabel("Count")
    ax.set_title("Walk-forward cumulative decisions")
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=160)
    plt.close(fig)

# -----------------------------
# Main run
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Long walk-forward backtest driver")
    ap.add_argument("--start", type=int, default=None, help="Start tick id (default: min id)")
    ap.add_argument("--max-ticks", type=int, default=2_000_000, help="Max number of ticks to traverse (default: 2,000,000)")
    ap.add_argument("--sleep-ms", type=int, default=0, help="Sleep between steps")
    ap.add_argument("--limit-steps", type=int, default=10_000_000, help="Safety cap on number of steps")
    ap.add_argument("--report-tag", type=str, default="", help="Optional tag for the report folder name")
    args = ap.parse_args()

    # DB bounds
    min_id, max_id = get_tick_bounds()
    start_id = args.start if args.start is not None else min_id
    if start_id < min_id: start_id = min_id
    target_end = start_id + args.max_ticks - 1
    if target_end > max_id: target_end = max_id

    jprint(f"Tick bounds: DB [{min_id}, {max_id}] | start={start_id}, target_end={target_end}")

    # Prepare report dir
    stamp = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    tag = f"-{args.report_tag}" if args.report_tag else ""
    outdir = os.path.join(os.path.dirname(__file__), "reports", f"wf2m-{stamp}{tag}")
    os.makedirs(outdir, exist_ok=True)
    csv_path = os.path.join(outdir, "report.csv")
    png_path = os.path.join(outdir, "cumulative.png")
    log_path = os.path.join(outdir, "run.log")

    with open(log_path, "a", encoding="utf-8") as flog:
        flog.write(f"[{_nowstamp()}] start walk-forward batch\n")
        flog.flush()

        steps = 0
        stalled = 0
        last_progress_end = get_last_closed_segment_end_id()

        while steps < args.limit-steps:
            # Stop if we reached target ticks
            cur_end = get_last_closed_segment_end_id()
            if cur_end >= target_end:
                jprint(f"Reached target_end at end_tick_id={cur_end}.")
                flog.write(f"[{_nowstamp()}] reached target_end={target_end}\n")
                break

            # One step
            try:
                out = step_once(target_end_tick_id=target_end)
            except Exception as e:
                jprint(f"Step error: {e}")
                flog.write(f"[{_nowstamp()}] step error: {e}\n")
                flog.flush()
                # continue attempting; if this loops without progress, bail by stalled logic
                time.sleep(1.0)
                steps += 1
                continue

            steps += 1

            # Logging
            macro = out.get("macro") or {}
            micro = out.get("micro") or {}
            ocm   = out.get("outcomes") or {}
            pred  = out.get("predict") or {}
            msg = f"step={steps} macro={macro} micro={micro} outcomes={ocm} pred={pred}"
            jprint(msg)
            flog.write(f"[{_nowstamp()}] {msg}\n")
            flog.flush()

            # Detect progress (based on last closed macro end)
            new_end = get_last_closed_segment_end_id()
            if new_end > last_progress_end:
                stalled = 0
                last_progress_end = new_end
            else:
                stalled += 1

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

            # If no progress for many steps, bail
            if stalled >= 50:
                jprint("No forward progress for 50 steps; stopping.")
                flog.write(f"[{_nowstamp()}] stalled; stopping\n")
                break

        # Evaluation
        jprint("Evaluating predictions vs outcomes…")
        df_eval = evaluate_predictions()
        df_seg  = per_segment_summary(df_eval)

        # Save report CSV
        with open(csv_path, "w", encoding="utf-8") as fcsv:
            if df_seg is not None and not df_seg.empty:
                df_seg.to_csv(fcsv, index=False)
            else:
                fcsv.write("segment_id,n_decided,n_correct,n_incorrect,acc\n")

        # Plot cumulative
        cumulative_plot(df_seg, png_path)

        # Console summary
        total_decided   = int(df_seg["n_decided"].sum()) if not df_seg.empty else 0
        total_correct   = int(df_seg["n_correct"].sum()) if not df_seg.empty else 0
        total_incorrect = int(df_seg["n_incorrect"].sum()) if not df_seg.empty else 0
        acc = (total_correct / total_decided) if total_decided else 0.0

        jprint(f"Done. Steps={steps}, decided={total_decided}, correct={total_correct}, incorrect={total_incorrect}, accuracy={acc:.3f}")
        jprint(f"CSV: {csv_path}")
        jprint(f"Chart: {png_path}")

        flog.write(f"[{_nowstamp()}] done steps={steps} decided={total_decided} correct={total_correct} incorrect={total_incorrect} acc={acc:.4f}\n")
        flog.flush()

if __name__ == "__main__":
    main()
