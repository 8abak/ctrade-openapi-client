#!/usr/bin/env python3
"""
Backfill labels & p_up for a tick range (throttled) using the SAME DB connection
style as tickCollectorToDB.py (plain psycopg2 -> db 'trading', user 'babak').

- Detect swing starts from Kalman (dollar reversal)
- First-touch outcomes for thresholds T in {2,3,4,5}
- Tiny multinomial logistic per T (dependency-light) to score p_up at starts
- Writes into:
    - move_labels(tickid_start, price_start, threshold_usd, p_up, dir_guess, outcome, ...)
    - predictions(tickid, p_up, model_id)

Run:
  python -m jobs.backfill --start 1 --end 200000

Optional ENV to tweak throttling:
  REVERSAL_USD   (default 1.0)
  MAX_TICKS      (default 15000)
  BATCH_SWINGS   (default 200)
  BATCH_SLEEP_MS (default 150)
"""

import os
import time
import argparse
from dataclasses import dataclass
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as pgx

# ---------- DB CONFIG: SAME AS tickCollectorToDB.py ----------
DB_NAME = "trading"
DB_USER = "babak"
DB_PASS = "babak33044"
DB_HOST = "localhost"
DB_PORT = 5432

# ---------- TUNABLES ----------
REVERSAL_USD   = float(os.environ.get("REVERSAL_USD", "1.0"))
MAX_TICKS      = int(os.environ.get("MAX_TICKS", "15000"))
BATCH_SWINGS   = int(os.environ.get("BATCH_SWINGS", "200"))
BATCH_SLEEP_MS = int(os.environ.get("BATCH_SLEEP_MS", "150"))
THRESHOLDS     = [2, 3, 4, 5]

# ---------- TABLE DDL ----------
DDL_MOVE_LABELS = """
CREATE TABLE IF NOT EXISTS move_labels (
  id               BIGSERIAL PRIMARY KEY,
  tickid_start     BIGINT      NOT NULL,
  ts_start         TIMESTAMPTZ NOT NULL DEFAULT now(),
  price_start      DOUBLE PRECISION NOT NULL,
  threshold_usd    INTEGER     NOT NULL,
  dir_guess        CHAR(2) NOT NULL DEFAULT 'nt',
  p_up             DOUBLE PRECISION,
  run_id           TEXT,
  tickid_resolve   BIGINT,
  ts_resolve       TIMESTAMPTZ,
  price_resolve    DOUBLE PRECISION,
  outcome          CHAR(2),
  time_to_outcome  INTEGER,
  is_open          BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS ix_move_labels_open   ON move_labels(is_open);
CREATE INDEX IF NOT EXISTS ix_move_labels_start  ON move_labels(tickid_start);
"""

DDL_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS predictions (
  tickid     BIGINT PRIMARY KEY,
  p_up       DOUBLE PRECISION NOT NULL,
  model_id   TEXT,
  run_id     TEXT,
  ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

# ---------- DB UTILS ----------
def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
    )

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_MOVE_LABELS)
        cur.execute(DDL_PREDICTIONS)
        # In case an older move_labels exists with NOT NULL but no default for dir_guess,
        # make it safe by ensuring a default (keeps NOT NULL intact).
        try:
            cur.execute("ALTER TABLE move_labels ALTER COLUMN dir_guess SET DEFAULT 'nt';")
        except Exception:
            pass
    conn.commit()

def read_df(conn, sql: str, params: tuple) -> pd.DataFrame:
    with conn.cursor(cursor_factory=pgx.DictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        cols = [desc.name for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)

# ---------- REPLACE detect_swings_from_kalman WITH THIS ----------
@dataclass
class SwingStart:
    tickid: int
    price: float  # kalman level at start

def detect_swings_from_kalman(kalman_df: pd.DataFrame,
                              reversal_usd: float = 1.0) -> List[SwingStart]:
    """
    State-machine detector: maintain an extreme for the current leg.
    - In an up leg: update max; reverse if price <= max - reversal_usd -> start a DOWN swing.
    - In a down leg: update min; reverse if price >= min + reversal_usd -> start an UP swing.
    Always seed the first tick as a swing start for anchoring.
    """
    if kalman_df.empty:
        return []

    x = kalman_df['tickid'].to_numpy()
    y = kalman_df['level'].to_numpy(dtype=float)

    swings: List[SwingStart] = [SwingStart(int(x[0]), float(y[0]))]

    # Start with unknown direction; initialize extreme to first price
    direction = 0           # 0 unknown, +1 up leg, -1 down leg
    extreme = y[0]          # current leg extreme (max if up, min if down)

    for i in range(1, len(y)):
        p = y[i]

        if direction == 0:
            # decide initial leg when we first move by >= reversal_usd
            if p >= extreme + reversal_usd:
                direction = +1
                extreme = p  # new max
            elif p <= extreme - reversal_usd:
                direction = -1
                extreme = p  # new min
            else:
                # still undecided, keep tracking the more extreme side
                extreme = max(extreme, p) if p > extreme else min(extreme, p)
                continue

        if direction == +1:
            # Up leg: extend max; look for down reversal
            if p > extreme:
                extreme = p
            elif p <= extreme - reversal_usd:
                # down reversal -> start new swing here
                swings.append(SwingStart(int(x[i]), float(p)))
                direction = -1
                extreme = p  # start tracking min
        else:
            # Down leg: extend min; look for up reversal
            if p < extreme:
                extreme = p
            elif p >= extreme + reversal_usd:
                # up reversal -> start new swing here
                swings.append(SwingStart(int(x[i]), float(p)))
                direction = +1
                extreme = p  # start tracking max

    return swings


def resolve_outcome(price_series: pd.Series,
                    start_tick: int,
                    start_price: float,
                    threshold_usd: int,
                    max_ticks: int = 15000) -> Tuple[str, int, float, Optional[int]]:
    up_t = start_price + threshold_usd
    dn_t = start_price - threshold_usd
    future = price_series.loc[start_tick:]
    if len(future) == 0:
        return ('nt', 0, float(start_price), None)
    view = future.iloc[:max_ticks]
    touch_up = view[view >= up_t]
    touch_dn = view[view <= dn_t]
    if not touch_up.empty and not touch_dn.empty:
        t_up = touch_up.index[0]; t_dn = touch_dn.index[0]
        if t_up < t_dn:
            return ('up', int(t_up - start_tick), float(view.loc[t_up]), int(t_up))
        else:
            return ('dn', int(t_dn - start_tick), float(view.loc[t_dn]), int(t_dn))
    elif not touch_up.empty:
        t_up = touch_up.index[0]
        return ('up', int(t_up - start_tick), float(view.loc[t_up]), int(t_up))
    elif not touch_dn.empty:
        t_dn = touch_dn.index[0]
        return ('dn', int(t_dn - start_tick), float(view.loc[t_dn]), int(t_dn))
    else:
        return ('nt', int(view.index[-1] - start_tick), float(view.iloc[-1]), None)

# ---------- FEATURES ----------
def build_event_features(kalman_df: pd.DataFrame,
                         raw_df: pd.DataFrame,
                         start_tickids: List[int]) -> pd.DataFrame:
    k = kalman_df.set_index('tickid').sort_index()
    if 'mid' in raw_df.columns and not raw_df.empty:
        r = raw_df.set_index('tickid').sort_index()
    else:
        r = k.rename(columns={'level':'mid'})

    df = pd.DataFrame({'tickid': start_tickids}).set_index('tickid')
    kk = k['level']; rr = r['mid']

    for w in (1, 5, 20, 50):
        df[f'k_slope_{w}'] = kk.diff(w).reindex(df.index)
    for w in (1, 5, 20):
        df[f'k_acc_{w}'] = kk.diff(w).diff(w).reindex(df.index)
    for w in (50, 200, 1000):
        df[f'mom_{w}'] = kk.diff(w).reindex(df.index)
    for w in (50, 200, 1000):
        df[f'vol_{w}'] = rr.rolling(w).std().reindex(df.index)

    df.fillna(0.0, inplace=True)
    df.reset_index(inplace=True)
    return df

# ---------- Tiny multinomial logistic ----------
class TinyLogit:
    def __init__(self, n_features: int, classes=('dn','nt','up'), lr=0.05, l2=1e-6):
        self.classes = list(classes)
        self.lr = lr; self.l2 = l2
        self.W = np.zeros((len(self.classes), n_features), dtype=np.float64)
        self.b = np.zeros(len(self.classes), dtype=np.float64)
        self.cls_idx = {c:i for i,c in enumerate(self.classes)}
    def _softmax(self, Z):
        Z = Z - Z.max(axis=1, keepdims=True)
        e = np.exp(Z)
        return e / np.clip(e.sum(axis=1, keepdims=True), 1e-12, None)
    def partial_fit(self, X: np.ndarray, y_labels: List[str], epochs=5, batch=512):
        y_idx = np.array([self.cls_idx.get(c,1) for c in y_labels], dtype=np.int64)  # default 'nt'
        N = X.shape[0]
        for _ in range(epochs):
            for s in range(0, N, batch):
                e = min(N, s+batch)
                xb = X[s:e]
                logits = xb @ self.W.T + self.b
                P = self._softmax(logits)
                Y = np.zeros_like(P); Y[np.arange(e-s), y_idx[s:e]] = 1.0
                G = (P - Y) / max(1, (e - s))
                gW = G.T @ xb + self.l2*self.W
                gb = G.sum(axis=0)
                self.W -= self.lr * gW
                self.b -= self.lr * gb
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        logits = X @ self.W.T + self.b
        return self._softmax(logits)

# ---------- DATA FETCH ----------
def fetch_kalman(conn, start: int, end: int) -> pd.DataFrame:
    sql = """
        SELECT tickid, level
        FROM public.kalman_states
        WHERE tickid BETWEEN %s AND %s
        ORDER BY tickid
    """
    return read_df(conn, sql, (start, end))

def fetch_raw_mid(conn, start: int, end: int) -> pd.DataFrame:
    sql = """
        SELECT id AS tickid, mid
        FROM public.ticks
        WHERE id BETWEEN %s AND %s
        ORDER BY id
    """
    try:
        return read_df(conn, sql, (start, end))
    except Exception:
        return pd.DataFrame(columns=['tickid','mid'])

# ---------- BACKFILL (throttled) ----------
def backfill_labels(conn, kdf: pd.DataFrame) -> Tuple[int,int]:
    swings = detect_swings_from_kalman(kdf, REVERSAL_USD)
    k_series = kdf.set_index('tickid')['level']

    rows = []
    written = 0
    for sw in swings:
        for T in THRESHOLDS:
            outc, tto, pres, tres = resolve_outcome(k_series, sw.tickid, sw.price, T, MAX_TICKS)
            rows.append({
                't0': sw.tickid, 'p0': sw.price, 'T': T,
                't1': tres, 'p1': pres, 'outc': outc, 'tto': tto,
                'dir': 'nt'  # <-- ensure NOT NULL constraint satisfied
            })
        if len(rows) >= BATCH_SWINGS * len(THRESHOLDS):
            _insert_labels(conn, rows); written += len(rows); rows.clear()
            time.sleep(BATCH_SLEEP_MS/1000.0)
    if rows:
        _insert_labels(conn, rows); written += len(rows)
    return len(swings), written

def _insert_labels(conn, rows: List[dict]):
    sql = """
      INSERT INTO move_labels
        (tickid_start, price_start, threshold_usd,
         dir_guess, p_up, tickid_resolve, price_resolve, outcome, time_to_outcome, is_open)
      VALUES
        (%(t0)s, %(p0)s, %(T)s,
         %(dir)s, NULL, %(t1)s, %(p1)s, %(outc)s, %(tto)s, FALSE)
      ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        pgx.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()

def bootstrap_train_and_predict(conn, kdf: pd.DataFrame, rdf: pd.DataFrame,
                                start: int, end: int) -> Tuple[int,int]:
    sql = """
      SELECT tickid_start, threshold_usd, outcome
      FROM move_labels
      WHERE tickid_start BETWEEN %s AND %s AND is_open=FALSE
      ORDER BY tickid_start
    """
    all_starts = read_df(conn, sql, (start, end))
    if all_starts.empty:
        return 0, 0

    feats_all = build_event_features(kdf, rdf, sorted(all_starts['tickid_start'].unique()))
    X_full = feats_all.set_index('tickid')

    updated_labels = 0
    wrote_preds = 0

    for T in THRESHOLDS:
        part = all_starts[all_starts['threshold_usd'] == T]
        if part.empty: continue

        Xi = X_full.loc[part['tickid_start'].values].values.astype(np.float64)
        yi = part['outcome'].astype(str).tolist()

        model = TinyLogit(n_features=Xi.shape[1], classes=('dn','nt','up'), lr=0.05, l2=1e-6)
        model.partial_fit(Xi, yi, epochs=5, batch=512)

        P = model.predict_proba(Xi)   # ('dn','nt','up')
        p_up = P[:, 2]

        upd_rows = []
        pred_rows = []
        for tick, p in zip(part['tickid_start'].values, p_up):
            upd_rows.append({'p': float(p), 'dir': 'up' if p >= 0.5 else 'dn',
                             't': int(tick), 'T': int(T)})
            pred_rows.append({'t': int(tick), 'p': float(p), 'm': f"move_{T}"})

        with conn.cursor() as cur:
            pgx.execute_batch(cur, """
              UPDATE move_labels
                 SET p_up=%(p)s, dir_guess=%(dir)s
               WHERE tickid_start=%(t)s AND threshold_usd=%(T)s
            """, upd_rows, page_size=500)
            pgx.execute_batch(cur, """
              INSERT INTO predictions (tickid, p_up, model_id)
              VALUES (%(t)s, %(p)s, %(m)s)
              ON CONFLICT (tickid) DO UPDATE
                SET p_up=EXCLUDED.p_up, model_id=EXCLUDED.model_id
            """, pred_rows, page_size=500)
        conn.commit()

        updated_labels += len(upd_rows)
        wrote_preds    += len(pred_rows)
        time.sleep(BATCH_SLEEP_MS/1000.0)

    return updated_labels, wrote_preds

# ---------- MAIN ----------
def main(start: int, end: int):
    conn = get_conn()
    try:
        ensure_tables(conn)

        kalman = fetch_kalman(conn, start, end)
        if kalman.empty:
            print("No kalman rows in range (check public.kalman_states).")
            return

        raw = fetch_raw_mid(conn, start, end)  # optional; used for volatility features

        n_swings, n_writes = backfill_labels(conn, kalman)
        print(f"[labels] swings={n_swings}, rows_written={n_writes}")

        n_upd, n_preds = bootstrap_train_and_predict(conn, kalman, raw, start, end)
        print(f"[predict] labels_updated={n_upd}, predictions_rows={n_preds}")
    finally:
        conn.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    args = ap.parse_args()
    main(args.start, args.end)
