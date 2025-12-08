# ml/buildMicroSeg.py

import logging
from datetime import datetime

from backend import db  # assuming backend is on PYTHONPATH

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def iter_macros(conn, symbol=None):
    sql = """
        SELECT DISTINCT s.id AS macro_id
        FROM piv_swings s
        JOIN hhll_piv p ON p.swing_id = s.id
        JOIN ticks t     ON t.id = p.tick_id
        WHERE (%(symbol)s IS NULL OR t.symbol = %(symbol)s)
        ORDER BY s.id
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"symbol": symbol})
        for (macro_id,) in cur:
            yield macro_id


def build_segments_for_macro(conn, macro_id):
    sql_pivots = """
        SELECT p.id, p.tick_id, p.ts, p.mid,
               s.id AS macro_id, s.pivot_id,
               t.symbol
        FROM hhll_piv p
        JOIN piv_swings s ON s.id = p.swing_id
        JOIN ticks t       ON t.id = p.tick_id
        WHERE s.id = %(macro_id)s
        ORDER BY p.ts
    """
    with conn.cursor() as cur:
        cur.execute(sql_pivots, {"macro_id": macro_id})
        rows = cur.fetchall()

    if len(rows) < 2:
        return 0

    insert_sql = """
        INSERT INTO piv_micro_seg (
            symbol, start_pivot_id, end_pivot_id,
            start_tick_id, end_tick_id,
            middle_id, macro_id,
            ts_start, ts_end,
            price_start, price_end,
            direction, delta_price, delta_time_sec,
            is_valid
        )
        VALUES (
            %(symbol)s, %(start_pivot_id)s, %(end_pivot_id)s,
            %(start_tick_id)s, %(end_tick_id)s,
            %(middle_id)s, %(macro_id)s,
            %(ts_start)s, %(ts_end)s,
            %(price_start)s, %(price_end)s,
            %(direction)s, %(delta_price)s, %(delta_time_sec)s,
            TRUE
        )
        ON CONFLICT (start_pivot_id, end_pivot_id) DO UPDATE
        SET
            symbol         = EXCLUDED.symbol,
            start_tick_id  = EXCLUDED.start_tick_id,
            end_tick_id    = EXCLUDED.end_tick_id,
            middle_id      = EXCLUDED.middle_id,
            macro_id       = EXCLUDED.macro_id,
            ts_start       = EXCLUDED.ts_start,
            ts_end         = EXCLUDED.ts_end,
            price_start    = EXCLUDED.price_start,
            price_end      = EXCLUDED.price_end,
            direction      = EXCLUDED.direction,
            delta_price    = EXCLUDED.delta_price,
            delta_time_sec = EXCLUDED.delta_time_sec,
            is_valid       = TRUE;
    """

    inserted = 0
    with conn.cursor() as cur:
        for i in range(len(rows) - 1):
            (id_i, tick_i, ts_i, mid_i, macro_id, pivot_id, symbol) = rows[i]
            (id_j, tick_j, ts_j, mid_j, _,       _,        _)      = rows[i + 1]

            delta_price = mid_j - mid_i
            dt = (ts_j - ts_i).total_seconds()
            if dt <= 0:
                dt = 1e-9  # avoid zero/negative

            if delta_price > 0:
                direction = 1
            elif delta_price < 0:
                direction = -1
            else:
                direction = 0

            params = {
                "symbol": symbol,
                "start_pivot_id": id_i,
                "end_pivot_id": id_j,
                "start_tick_id": tick_i,
                "end_tick_id": tick_j,
                "middle_id": pivot_id,
                "macro_id": macro_id,
                "ts_start": ts_i,
                "ts_end": ts_j,
                "price_start": mid_i,
                "price_end": mid_j,
                "direction": direction,
                "delta_price": delta_price,
                "delta_time_sec": dt,
            }
            cur.execute(insert_sql, params)
            inserted += 1

    return inserted


def main(symbol=None):
    conn = db.get_conn()
    total = 0
    start = datetime.utcnow()
    try:
        for n, macro_id in enumerate(iter_macros(conn, symbol=symbol), start=1):
            cnt = build_segments_for_macro(conn, macro_id)
            conn.commit()
            total += cnt
            if n % 100 == 0:
                log.info("Processed %d macros, total segments=%d", n, total)
    finally:
        conn.close()
    log.info("Done. Total segments inserted/updated: %d in %.1fs",
             total, (datetime.utcnow() - start).total_seconds())


if __name__ == "__main__":
    # optionally: parse CLI args for symbol or macro ranges
    main()