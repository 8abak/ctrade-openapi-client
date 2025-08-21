# jobs/backfill.py
"""
Backfill labels & p_up for a tick range (throttled).
- Detect swing starts from Kalman (dollar reversal)
- First-touch outcomes for thresholds T in {2,3,4,5}
- Train a tiny multinomial logistic per T (fast)
- Write p_up at swing starts (and into `predictions` for the Review UI)

ENV (optional overrides):
  PG_DSN                 e.g. postgresql+psycopg2://user:pass@host:5432/db
  REVERSAL_USD           default 1.0 (swing start reversal threshold on kalman)
  MAX_TICKS              default 15000 (timeout for 'nt')
  BATCH_SWINGS           default 200   (commit every N swing starts)
  BATCH_SLEEP_MS         default 150   (sleep this many ms between batches)

Usage:
  python -m jobs.backfill --start 1 --end 200000
"""

import os
import time
import math
import argparse
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Tuple, Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

# ------------------ CONFIG (hardcoded DB like tickCollectortoDB.py) ------------------
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
DB_NAME = "ctrade"
DB_USER = "postgres"
DB_PASSWORD = "babak33044"  # <— your password here

# Build DSN with password embedded (URL-escaped). Allow PG_DSN env to override.
PG_DSN = os.getenv(
    "PG_DSN",
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Throttling / labeling defaults (overridable via env)
REVERSAL_USD   = float(os.environ.get("REVERSAL_USD", "1.0"))
MAX_TICKS      = int(os.environ.get("MAX_TICKS", "15000"))
BATCH_SWINGS   = int(os.environ.get("BATCH_SWINGS", "200"))
BATCH_SLEEP_MS = int(os.environ.get("BATCH_SLEEP_MS", "150"))

THRESHOLDS = [2, 3, 4, 5]

# ------------------ TABLE DDL ------------------
DDL_MOVE_LABELS = """
CREATE TABLE IF NOT EXISTS move_labels (
  id               BIGSERIAL PRIMARY KEY,
  tickid_start     BIGINT      NOT NULL,
  ts_start         TIMESTAMPTZ NOT NULL DEFAULT now(),
  price_start      DOUBLE PRECISION NOT NULL,
  threshold_usd    INTEGER     NOT NULL,
  dir_guess        CHAR(2),
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

# ------------------ SWING DETECTION & OUTCOME ------------------
@dataclass
class SwingStart:
    tickid: int
    price: float  # kalman level at start

def detect_swings_from_kalman(kalman_df: pd.DataFrame,
                              reversal_usd: float = 1.0) -> List[SwingStart]:
    """
    Detect swing starts on Kalman line via dollar reversal threshold.
    kalman_df columns: ['tickid','level'] sorted by tickid ascending.
    """
    x = kalman_df['tickid'].values
    y = kalman_df['level'].values
    if len(x) == 0:
        return []
    swings: List[SwingStart] = []
    last_ext_price = float(y[0])
    direction = 0  # 0 unknown, +1 up leg, -1 down leg

    for i in range(1, len(y)):
        if direction >= 0:
            if y[i] - last_ext_price >= 0:
                last_ext_price = float(y[i]); direction = +1
            elif (last_ext_price - y[i]) >= reversal_usd:
                swings.append(SwingStart(tickid=int(x[i]), price=float(y[i])))
                last_ext_price = float(y[i]); direction = -1
        if direction <= 0:
            if (last_ext_price - y[i]) <= 0:
                last_ext_price = float(y[i]); direction = -1 if direction != 0 else -1
            elif (y[i] - last_ext_price) >= reversal_usd:
                swings.append(SwingStart(tickid=int(x[i]), price=float(y[i])))
                last_ext_price = float(y[i]); direction = +1

    # Ensure first bar is a swing start (optional but useful)
    if not swings or swings[0].tickid != int(x[0]):
        swings.insert(0, SwingStart(tickid=int(x[0]), price=float(y[0])))
    return swings

def resolve_outcome(price_series: pd.Series,
                    start_tick: int,
                    start_price: float,
                    threshold_usd: int,
                    max_ticks: int = 15000) -> Tuple[str, int, float, Optional[int]]:
    """
    First-touch outcome from start: 'up' if price first >= start+T, 'dn' if first <= start-T,
    'nt' if neither within max_ticks. Returns (outcome, time_to_outcome, price_at_resolve, tickid_resolve).
    price_series must be indexed by tickid ascending (kalman level).
    """
    up_target = start_price + threshold_usd
    dn_target = start_price - threshold_usd
    future = price_series.loc[start_tick:]
    if len(future) == 0:
        return ('nt', 0, float(start_price), None)
    view = future.iloc[:max_ticks]
    touch_up = view[view >= up_target]
    touch_dn = view[view <= dn_target]
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

# ------------------ FEATURES (compact, no leakage) ------------------
def build_event_features(kalman_df: pd.DataFrame,
                         raw_df: pd.DataFrame,
                         start_tickids: List[int]) -> pd.DataFrame:
    """
    kalman_df: ['tickid','level']  (sorted)
    raw_df:    ['tickid','mid']    (sorted) — if missing, falls back to kalman as 'mid'
    Returns one row per start tick with features and 'tickid' column.
    """
    k = kalman_df.set_index('tickid').sort_index()
    if 'mid' in raw_df.columns:
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

# ------------------ Tiny multinomial logistic (fast, dependency-light) ------------------
class TinyLogit:
    def __init__(self, n_features: int, classes=('dn','nt','up'), lr=0.05, l2=1e-6):
        self.classes = list(classes)
        self.lr = lr; self.l2 = l2
        self.W = np.zeros((len(self.classes), n_features), dtype=np.float64)
        self.b = np.zeros(len(self.classes), dtype=np.float64)
        self.class_to_idx = {c:i for i,c in enumerate(self.classes)}

    def _softmax(self, Z):
        Z = Z - Z.max(axis=1, keepdims=True)
        e = np.exp(Z)
        return e / np.clip(e.sum(axis=1, keepdims=True), 1e-12, None)

    def partial_fit(self, X: np.ndarray, y_labels: List[str], epochs=5, batch=512):
        y_idx = np.array([self.class_to_idx.get(c, 1) for c in y_labels], dtype=np.int64)  # default to 'nt'
        N = X.shape[0]
        for _ in range(epochs):
            for s in range(0, N, batch):
                e = min(N, s+batch)
                xb = X[s:e]
                logits = xb @ self.W.T + self.b
                probs = self._softmax(logits)
                Y = np.zeros_like(probs); Y[np.arange(e-s), y_idx[s:e]] = 1.0
                grad = (probs - Y) / max(1, (e - s))
                gW = grad.T @ xb + self.l2 * self.W
                gb = grad.sum(axis=0)
                self.W -= self.lr * gW
                self.b -= self.lr * gb

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        logits = X @ self.W.T + self.b
        return self._softmax(logits)

# ------------------ DB helpers ------------------
def ensure_tables(engine):
    with engine.begin() as conn:
        for stmt in filter(None, DDL_MOVE_LABELS.split(";")):
            s = stmt.strip()
            if s: conn.execute(text(s))
        for stmt in filter(None, DDL_PREDICTIONS.split(";")):
            s = stmt.strip()
            if s: conn.execute(text(s))

def fetch_kalman(engine, start: int, end: int) -> pd.DataFrame:
    sql = text("""
        SELECT tickid, level
        FROM kalman_states
        WHERE tickid BETWEEN :a AND :b
        ORDER BY tickid
    """)
    return pd.read_sql(sql, engine, params={'a':start, 'b':end})

def fetch_raw_mid(engine, start: int, end: int) -> pd.DataFrame:
    # If your mid lives elsewhere, adapt this query.
    sql = text("""
        SELECT id AS tickid, mid
        FROM ticks
        WHERE id BETWEEN :a AND :b
        ORDER BY id
    """)
    try:
        return pd.read_sql(sql, engine, params={'a':start, 'b':end})
    except Exception:
        # fallback: use empty; features builder will substitute kalman
        return pd.DataFrame(columns=['tickid','mid'])

# ------------------ Backfill (throttled) ------------------
def backfill_labels(engine, kdf: pd.DataFrame):
    """
    Detect swings and write outcomes for thresholds. Batched with sleeps.
    Returns (num_swings, rows_written)
    """
    swings = detect_swings_from_kalman(kdf, REVERSAL_USD)
    k_series = kdf.set_index('tickid')['level']

    insert_sql = text("""
      INSERT INTO move_labels
        (tickid_start, price_start, threshold_usd,
         dir_guess, p_up, tickid_resolve, price_resolve, outcome, time_to_outcome, is_open)
      VALUES
        (:t0, :p0, :T, NULL, NULL, :t1, :p1, :outc, :tto, FALSE)
      ON CONFLICT DO NOTHING
    """)

    batch = []
    written = 0
    with engine.begin() as conn:
        for i, sw in enumerate(swings):
            for T in THRESHOLDS:
                outc, tto, pres, tres = resolve_outcome(k_series, sw.tickid, sw.price, T, MAX_TICKS)
                batch.append(dict(t0=sw.tickid, p0=sw.price, T=T,
                                  t1=tres, p1=pres, outc=outc, tto=tto))
            if len(batch) >= BATCH_SWINGS * len(THRESHOLDS):
                conn.execute(insert_sql, batch)
                written += len(batch)
                batch.clear()
                time.sleep(BATCH_SLEEP_MS / 1000.0)
        if batch:
            conn.execute(insert_sql, batch)
            written += len(batch)
    return len(swings), written

def bootstrap_train_and_predict(engine, kdf: pd.DataFrame, rdf: pd.DataFrame,
                                start: int, end: int):
    """
    Train tiny multinomial logistic per T on resolved outcomes; write p_up at starts.
    Returns (labels_updated, predictions_rows)
    """
    all_starts = pd.read_sql(text("""
      SELECT tickid_start, threshold_usd, outcome
      FROM move_labels
      WHERE tickid_start BETWEEN :a AND :b AND is_open=FALSE
      ORDER BY tickid_start
    """), engine, params={'a':start, 'b':end})

    if all_starts.empty:
        return 0, 0

    feats_all = build_event_features(kdf, rdf, sorted(all_starts['tickid_start'].unique()))
    X_full = feats_all.set_index('tickid')  # keep tickid index for join

    wrote_preds = 0
    updated_labels = 0

    for T in THRESHOLDS:
        part = all_starts[all_starts['threshold_usd'] == T].copy()
        if part.empty:
            continue
        Xi = X_full.loc[part['tickid_start'].values].values.astype(np.float64)
        yi = part['outcome'].astype(str).tolist()

        model = TinyLogit(n_features=Xi.shape[1], classes=('dn','nt','up'), lr=0.05, l2=1e-6)
        model.partial_fit(Xi, yi, epochs=5, batch=512)

        P = model.predict_proba(Xi)  # columns order ('dn','nt','up')
        p_up = P[:, 2]

        rows = []
        pred_rows = []
        for tick, p in zip(part['tickid_start'].values, p_up):
            rows.append({'p': float(p), 'dir': 'up' if p >= 0.5 else 'dn',
                         't': int(tick), 'T': int(T)})
            pred_rows.append({'t': int(tick), 'p': float(p), 'm': f"move_{T}"})

        with engine.begin() as conn:
            conn.execute(text("""
              UPDATE move_labels
              SET p_up=:p, dir_guess=:dir
              WHERE tickid_start=:t AND threshold_usd=:T
            """), rows)
            updated_labels += len(rows)

            conn.execute(text("""
              INSERT INTO predictions (tickid, p_up, model_id)
              VALUES (:t, :p, :m)
              ON CONFLICT (tickid) DO UPDATE SET p_up=EXCLUDED.p_up, model_id=EXCLUDED.model_id
            """), pred_rows)
            wrote_preds += len(pred_rows)

        time.sleep(BATCH_SLEEP_MS / 1000.0)

    return updated_labels, wrote_preds

# ------------------ MAIN ------------------
def main(start: int, end: int):
    # conservative pool to avoid pressure
    eng = create_engine(PG_DSN, pool_size=2, max_overflow=0)

    ensure_tables(eng)

    kalman = fetch_kalman(eng, start, end)
    if kalman.empty:
        print("No kalman rows in range."); return

    raw = fetch_raw_mid(eng, start, end)  # used for volatility features; falls back if missing

    n_swings, n_writes = backfill_labels(eng, kalman)
    print(f"[labels] swings={n_swings}, rows_written={n_writes}")

    n_upd, n_preds = bootstrap_train_and_predict(eng, kalman, raw, start, end)
    print(f"[predict] labels_updated={n_upd}, predictions_rows={n_preds}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, required=True)
    ap.add_argument("--end", type=int, required=True)
    args = ap.parse_args()
    main(args.start, args.end)
