# ml/labeler.py  — REPLACE ENTIRE FILE
import math
from typing import List, Dict, Any

from sqlalchemy import text

from .db import db_conn, upsert_many

Z_MIN = 0.8
KAPPA_MAX = 0.25
K_SEG = 20  # mark first K ticks of each up/down segment

def build_labels_range(start: int, end: int) -> int:
    """
    Label ticks in [start..end] using causal zSlope/kappa rules.
    Writes to trend_labels (UPSERT). Also flags first K ticks of each up/down segment.
    """
    with db_conn() as conn:
        rows = conn.execute(text("""
            SELECT f.tickid, f.slope, f.vol_ewstd
            FROM ml_features f
            WHERE f.tickid BETWEEN :s AND :e
            ORDER BY f.tickid
        """), {"s": int(start), "e": int(end)}).mappings().all()

    if not rows:
        return 0

    out = []
    prev_z = 0.0
    seg_dir = 0   # current segment direction (-1/0/+1)
    seg_len = 0
    for r in rows:
        tid = int(r["tickid"])
        slope = float(r["slope"] if r["slope"] is not None else 0.0)
        vol = float(r["vol_ewstd"] if r["vol_ewstd"] is not None else 0.0)
        denom = vol + 1e-9
        z = slope / denom
        kappa = abs(z - prev_z)

        # raw direction by thresholds
        if z >= Z_MIN and kappa <= KAPPA_MAX:
            direction = 1
        elif z <= -Z_MIN and kappa <= KAPPA_MAX:
            direction = -1
        else:
            direction = 0

        # segment tracking
        is_start = False
        if direction != 0:
            if direction != seg_dir:
                # new segment
                seg_dir = direction
                seg_len = 1
                is_start = True
            else:
                seg_len += 1
                if seg_len <= K_SEG:
                    is_start = True
        else:
            seg_dir = 0
            seg_len = 0

        out.append({
            "tickid": tid,
            "direction": int(direction),
            "is_segment_start": bool(is_start),
            "meta": None
        })
        prev_z = z

        if len(out) >= 5000:
            upsert_many("trend_labels", out, conflict_key="tickid")
            out.clear()

    if out:
        upsert_many("trend_labels", out, conflict_key="tickid")

    return len(rows)
