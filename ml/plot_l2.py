# ml/plot_l2.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import matplotlib.pyplot as plt

from backend.db import get_conn, dict_cur


@dataclass
class TickRow:
    ts: object
    kal: float


def plot_parent_with_l2(parent_segment_id: int, run_id: str | None = None) -> None:
    conn = get_conn()
    with dict_cur(conn) as cur:
        cur.execute(
            """
            SELECT id, symbol, start_tick_id, end_tick_id, start_ts
            FROM segms
            WHERE id = %s
            """,
            (parent_segment_id,),
        )
        p = cur.fetchone()
        if not p:
            raise RuntimeError("parent not found")

        symbol = p["symbol"]
        a = int(p["start_tick_id"])
        b = int(p["end_tick_id"])
        parent_start_ts = p["start_ts"]

        cur.execute(
            """
            SELECT timestamp AS ts, kal
            FROM ticks
            WHERE symbol=%s AND id BETWEEN %s AND %s
            ORDER BY timestamp ASC, id ASC
            """,
            (symbol, a, b),
        )
        ticks = [TickRow(ts=r["ts"], kal=float(r["kal"])) for r in cur.fetchall() if r["kal"] is not None]

        if run_id:
            cur.execute(
                """
                SELECT local_seg_index, start_ts, end_ts, slope, intercept
                FROM segms_l2
                WHERE parent_segment_id=%s AND run_id=%s
                ORDER BY local_seg_index
                """,
                (parent_segment_id, run_id),
            )
        else:
            cur.execute(
                """
                SELECT local_seg_index, start_ts, end_ts, slope, intercept
                FROM segms_l2
                WHERE parent_segment_id=%s
                ORDER BY local_seg_index
                """,
                (parent_segment_id,),
            )
        segs = cur.fetchall()

    # Plot ticks
    xs = [t.ts for t in ticks]
    ys = [t.kal for t in ticks]
    plt.figure()
    plt.plot(xs, ys)

    # Overlay L2 segments as fitted lines
    for s in segs:
        st = s["start_ts"]
        en = s["end_ts"]
        a = float(s["slope"])
        b0 = float(s["intercept"])

        # line in parent-relative seconds
        t_st = (st - parent_start_ts).total_seconds()
        t_en = (en - parent_start_ts).total_seconds()

        y_st = a * t_st + b0
        y_en = a * t_en + b0

        plt.plot([st, en], [y_st, y_en])

    plt.title(f"{symbol} parent_segment_id={parent_segment_id} l2_count={len(segs)}")
    plt.show()
