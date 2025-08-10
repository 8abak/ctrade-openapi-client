#!/usr/bin/env python3
import psycopg2
from datetime import timedelta
from daily_trend_runner import db, sydney_day_window, largest_segment, AEST

def evaluate_day(day):
    with db() as conn, conn.cursor() as cur:
        day0, day1 = sydney_day_window(day)
        # last run that targeted this window
        cur.execute("SELECT id FROM model_runs WHERE day_start=%s AND day_end=%s", (day0, day1))
        row = cur.fetchone()
        if not row: return
        run_id = row[0]

        for lvl, table in (("micro","micro_trends"), ("medium","medium_trends"), ("maxi","maxi_trends")):
            cur.execute(f"SELECT start_price,end_price FROM {table} WHERE run_day=%s", (day,))
            segs_db = [dict(start_price=r[0], end_price=r[1]) for r in cur.fetchall()]
            top = largest_segment(segs_db)
            if not top: continue

            cur.execute("SELECT predicted_dir, predicted_mag FROM predictions WHERE run_id=%s AND level=%s",
                        (run_id, lvl))
            p = cur.fetchone()
            if not p: continue
            pdir, pmag = p
            mae = abs((pmag or 0) - top["mag"])
            acc = 1.0 if (pdir == top["dir"]) else 0.0

            cur.execute("""
              INSERT INTO evaluations(run_id, level, actual_dir, actual_mag, mae_mag, accuracy_dir)
              VALUES (%s,%s,%s,%s,%s,%s)
            """, (run_id, lvl, top["dir"], top["mag"], mae, acc))
        conn.commit()

if __name__ == "__main__":
    from datetime import datetime
    today = datetime.now(tz=AEST).date()
    # example: evaluate yesterday
    from datetime import timedelta
    evaluate_day(today - timedelta(days=1))
