# jobs/build_labels.py
import pandas as pd
from sqlalchemy import create_engine, text
import os
from ml.labels import detect_swings_from_kalman, resolve_outcome

PG_DSN = os.environ.get("PG_DSN", "postgresql+psycopg2://postgres@localhost/ctrade")

THRESHOLDS = [2,3,4,5]
MAX_TICKS = 15000
REVERSAL_USD = float(os.environ.get("REVERSAL_USD", "1.0"))

def fetch_kalman(engine, start: int, end: int) -> pd.DataFrame:
    sql = text("""
        SELECT tickid, level
        FROM kalman_states
        WHERE tickid BETWEEN :a AND :b
        ORDER BY tickid
    """)
    return pd.read_sql(sql, engine, params={'a':start, 'b':end})

def main(start: int, end: int):
    eng = create_engine(PG_DSN)
    k = fetch_kalman(eng, start, end)
    if k.empty:
        print("No kalman rows.")
        return

    swings = detect_swings_from_kalman(k, REVERSAL_USD)
    k_series = k.set_index('tickid')['level']

    with eng.begin() as conn:
        for sw in swings:
            for T in THRESHOLDS:
                outcome, tto, price_res, tick_res = resolve_outcome(
                    k_series, sw.tickid, sw.price, threshold_usd=T, max_ticks=MAX_TICKS
                )
                conn.execute(text("""
                    INSERT INTO move_labels
                        (tickid_start, price_start, threshold_usd,
                         dir_guess, p_up, tickid_resolve, price_resolve, outcome, time_to_outcome, is_open)
                    VALUES
                        (:t0, :p0, :T,
                         NULL, NULL, :t1, :p1, :outc, :tto, FALSE)
                    ON CONFLICT DO NOTHING
                """), dict(t0=sw.tickid, p0=sw.price, T=T,
                           t1=tick_res, p1=price_res, outc=outcome, tto=tto))
    print(f"Inserted labels for swings={len(swings)} thresholds={THRESHOLDS}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    args = ap.parse_args()
    main(args.start, args.end)
