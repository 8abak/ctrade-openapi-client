"""
walkforward.py
--------------

Run a long walk-forward backtest over many ticks, one macro segment at a time.
Per step:
  1) Build/extend macro segments (compat with old/new signatures)
  2) Detect micro events for the latest CLOSED segment
  3) Resolve outcomes for eligible events
  4) Train & predict (train on fully resolved past, predict on next segment’s events)

Stops when the latest closed segment's end_tick_id reaches the target bound
(or when progress stalls).

Artifacts are saved under: backend/reports/wf2m-YYYYMMDD-HHMMSS[-TAG]/

Usage (EC2):

  $ source venv/bin/activate
  $ python -m backend.walkforward --max-ticks 2000000
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Dict, Any, Tuple

from sqlalchemy import text

# Allow running both as module (-m backend.walkforward) and directly
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Shared DB engine + pipeline modules
from .main import engine  # type: ignore
from .label_macro_segments import BuildOrExtendSegments  # type: ignore
from .label_micro_events import DetectMicroEventsForLatestClosedSegment  # type: ignore
from .compute_outcomes import ResolveOutcomes  # type: ignore
from .train_predict import TrainAndPredict  # type: ignore

import pandas as pd  # type: ignore
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt  # type: ignore

# -----------------------------
# Small utilities
# -----------------------------

def _nowstamp() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def jprint(msg: str):
    print(f"[{_nowstamp()}] {msg}", flush=True)

# -----------------------------
# SQL helpers
# -----------------------------

SQL_TICK_BOUNDS = text("""
    SELECT MIN(id) AS min_id, MAX(id) AS max_id
    FROM ticks;
""")

SQL_LAST_CLOSED_SEGMENT = text("""
    SELECT segment_id, end_tick_id
    FROM macro_segments
    WHERE end_tick_id IS NOT NULL
    ORDER BY end_tick_id DESC
    LIMIT 1;
""")

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

def get_tick_bounds() -> Tuple[int, int]:
    with engine.connect() as conn:
        row = conn.execute(SQL_TICK_BOUNDS).first()
    if not row or row[0] is None or row[1] is None:
        raise RuntimeError("ticks table appears empty.")
    return int(row[0]), int(row[1])

def get_last_closed_segment_end_id() -> int:
    with engine.connect() as conn:
        row = conn.execute(SQL_LAST_CLOSED_SEGMENT).first()
    if not row:
        return 0
    return int(row[1] or 0)

# -----------------------------
# One step
# -----------------------------

def _build_or_extend_macro(engine, target_end_tick_id: int) -> Dict[str, Any]:
    """
    Compatibility wrapper:
    - Try new signature: BuildOrExtendSegments(engine, until_tick_id=…)
    - Fall back to old signature: BuildOrExtendSegments(engine)
    """
    try:
        return BuildOrExtendSegments(engine, until_tick_id=target_end_tick_id)  # type: ignore[arg-type]
    except TypeError:
        # Current codebase still on old signature — proceed without bound
        return BuildOrExtendSegments(engine)  # type: ignore[call-arg]

def step_once(target_end_tick_id: int) -> Dict[str, Any]:
    """
    Perform one step: build/extend (one leg), micro events, outcomes, train & predict.
    Return a dict summary for logging.
    """
    out: Dict[str, Any] = {"macro": None, "micro": None, "outcomes": None, "predict": None}
    out["macro"]    = _build_or_extend_macro(engine, target_end_tick_id)
    out["micro"]    = DetectMicroEventsForLatestClosedSegment(engine)
    out["outcomes"] = ResolveOutcomes(engine)
    out["predict"]  = TrainAndPredict(engine)
    return out

# -----------------------------
# Evaluation + reporting
# -----------------------------

def evaluate_predictions() -> pd.DataFrame:
    """
    Return dataframe of decided predictions joined with outcomes,
    sorted by (segment_id, tick_id), with a 'correct' column (TP=correct).
    """
    with engine.connect() as conn:
        df = pd.read_sql(SQL_EVAL, conn)

    if df.empty:
        return df

    df = df[df["decided"] == True].copy()
    df["correct"] = (df["outcome"] == "TP")  # SL/Timeout counted as incorrect
    df.sort_values(["segment_id", "tick_id"], inplace=True, ignore_index=True)
    return df

def per_segment_summary(df_eval: pd.DataFrame) -> pd.DataFrame:
    if df_eval.empty:
        return pd.DataFrame(columns=["segment_id", "n_decided", "n_correct", "n_incorrect", "acc"])
    g = df_eval.groupby("segment_id", as_index=False).agg(
        n_decided=("correct", "size"),
        n_correct=("correct", "sum"),
    )
    g["n_incorrect"] = g["n_decided"] - g["n_correct"]
    g["acc"] = (g["n_correct"] / g["n_decided"]).round(4).fillna(0)
    return g

def cumulative_plot(df_seg: pd.DataFrame, out_png: str):
    if df_seg.empty:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "No decided predictions to plot.", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        fig.savefig(out_png, dpi=150)
        plt.close(fig)
        return

    df = df_seg.sort_values("segment_id").copy()
    df["cum_correct"] = df["n_correct"].cumsum()
    df["cum_incorrect"] = df["n_incorrect"].cumsum()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df["segment_id"], df["cum_correct"],   label="Cumulative correct",   linewidth=2)
    ax.plot(df["segment_id"], df["cum_incorrect"], label="Cumulative incorrect", linewidth=2)
    ax.set_xlabel("Segment ID")
    ax.set_ylabel("Count")
    ax.set_title("Walk-forward cumulative decisions")
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=160)
    plt.close(fig)

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Long walk-forward backtest driver")
    ap.add_argument("--start", type=int, default=None, help="Start tick id (default: DB min id)")
    ap.add_argument("--max-ticks", type=int, default=2_000_000, help="Max ticks to traverse")
    ap.add_argument("--sleep-ms", type=int, default=0, help="Sleep between steps")
    ap.add_argument("--limit-steps", type=int, default=10_000_000, help="Safety cap")
    ap.add_argument("--stalled-limit", type=int, default=50, help="Stop if no progress for N steps")
    ap.add_argument("--report-tag", type=str, default="", help="Optional tag for report folder")
    args = ap.parse_args()

    # DB bounds
    min_id, max_id = get_tick_bounds()
    start_id = args.start if args.start is not None else min_id
    if start_id < min_id:
        start_id = min_id
    target_end = start_id + args.max_ticks - 1
    if target_end > max_id:
        target_end = max_id

    jprint(f"Tick bounds DB=[{min_id},{max_id}] | start={start_id} → target_end={target_end}")

    # Prepare output dir
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

        while steps < args.limit_steps:
            cur_end = get_last_closed_segment_end_id()
            if cur_end >= target_end:
                jprint(f"Reached target_end at end_tick_id={cur_end}.")
                flog.write(f"[{_nowstamp()}] reached target_end={target_end}\n")
                break

            try:
                out = step_once(target_end_tick_id=target_end)
            except Exception as e:
                jprint(f"Step error: {e}")
                flog.write(f"[{_nowstamp()}] step error: {e}\n")
                flog.flush()
                time.sleep(1.0)
                steps += 1
                continue

            steps += 1

            macro = out.get("macro") or {}
            micro = out.get("micro") or {}
            ocm   = out.get("outcomes") or {}
            pred  = out.get("predict") or {}
            msg = f"step={steps} macro={macro} micro={micro} outcomes={ocm} pred={pred}"
            jprint(msg)
            flog.write(f"[{_nowstamp()}] {msg}\n")
            flog.flush()

            new_end = get_last_closed_segment_end_id()
            if new_end > last_progress_end:
                stalled = 0
                last_progress_end = new_end
            else:
                stalled += 1

            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

            if stalled >= args.stalled_limit:
                jprint(f"No forward progress for {args.stalled_limit} steps; stopping.")
                flog.write(f"[{_nowstamp()}] stalled; stopping\n")
                break

        # Evaluation
        jprint("Evaluating predictions vs outcomes…")
        with engine.connect() as conn:
            df_eval = pd.read_sql(SQL_EVAL, conn)
        if not df_eval.empty:
            df_eval = df_eval[df_eval["decided"] == True].copy()
            df_eval["correct"] = (df_eval["outcome"] == "TP")
            df_eval.sort_values(["segment_id", "tick_id"], inplace=True, ignore_index=True)

        if df_eval.empty:
            df_seg = pd.DataFrame(columns=["segment_id", "n_decided", "n_correct", "n_incorrect", "acc"])
        else:
            g = df_eval.groupby("segment_id", as_index=False).agg(
                n_decided=("correct", "size"),
                n_correct=("correct", "sum"),
            )
            g["n_incorrect"] = g["n_decided"] - g["n_correct"]
            g["acc"] = (g["n_correct"] / g["n_decided"]).round(4).fillna(0)
            df_seg = g

        # Save CSV + plot
        with open(csv_path, "w", encoding="utf-8") as fcsv:
            if not df_seg.empty:
                df_seg.to_csv(fcsv, index=False)
            else:
                fcsv.write("segment_id,n_decided,n_correct,n_incorrect,acc\n")

        # Plot cumulative
        if df_seg.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.text(0.5, 0.5, "No decided predictions to plot.", ha="center", va="center")
            ax.axis("off")
            fig.tight_layout()
            fig.savefig(png_path, dpi=150)
            plt.close(fig)
            total_decided = total_correct = total_incorrect = 0
            acc = 0.0
        else:
            dfp = df_seg.sort_values("segment_id").copy()
            dfp["cum_correct"] = dfp["n_correct"].cumsum()
            dfp["cum_incorrect"] = dfp["n_incorrect"].cumsum()

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(dfp["segment_id"], dfp["cum_correct"],   label="Cumulative correct",   linewidth=2)
            ax.plot(dfp["segment_id"], dfp["cum_incorrect"], label="Cumulative incorrect", linewidth=2)
            ax.set_xlabel("Segment ID")
            ax.set_ylabel("Count")
            ax.set_title("Walk-forward cumulative decisions")
            ax.grid(True, linestyle="--", alpha=0.3)
            ax.legend()
            fig.tight_layout()
            fig.savefig(png_path, dpi=160)
            plt.close(fig)

            total_decided   = int(df_seg["n_decided"].sum())
            total_correct   = int(df_seg["n_correct"].sum())
            total_incorrect = int(df_seg["n_incorrect"].sum())
            acc = (total_correct / total_decided) if total_decided else 0.0

        jprint(f"Done. steps={steps} decided={total_decided} correct={total_correct} "
               f"incorrect={total_incorrect} accuracy={acc:.3f}")
        jprint(f"CSV:   {csv_path}")
        jprint(f"Chart: {png_path}")

        flog.write(f"[{_nowstamp()}] done steps={steps} decided={total_decided} "
                   f"correct={total_correct} incorrect={total_incorrect} acc={acc:.4f}\n")
        flog.flush()

if __name__ == "__main__":
    main()
