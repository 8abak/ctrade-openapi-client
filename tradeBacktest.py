#!/usr/bin/env python3
"""
trade_backtest.py

Stage 8.3 – Offline backtest for Kalman continuation model.

Uses:
  - final_trade_signals view (entry_approved / exit_approved)
  - ticks table (id, mid)

For each approved entry:
  - direction = long or short
  - target = +1.0 dollars
  - stop   = -1.0 dollars
  - max horizon = 300 ticks
  - optional early exit when exit_approved appears first

Outputs:
  - summary metrics
  - optional CSV with all trades (for later analysis)
"""

import psycopg2
import pandas as pd
import numpy as np
from dataclasses import dataclass


# -----------------------------
# Config
# -----------------------------

DB_NAME = "trading"
DB_USER = "babak"
DB_PASSWORD = "babak33044"  # <-- change this
DB_HOST = "localhost"
DB_PORT = 5432

TP_DOLLARS = 1.0      # take-profit (price move)
SL_DOLLARS = 1.0      # stop-loss (price move)
MAX_TICKS = 300       # max ticks to hold a trade
OUTPUT_CSV = "trade_backtest_results.csv"


@dataclass
class TradeResult:
    vel_grp: int
    id_entry: int
    id_exit: int
    direction: int          # +1 long, -1 short
    entry_price: float
    exit_price: float
    pnl: float
    n_ticks: int
    reason: str             # 'tp', 'sl', 'signal_exit', 'timeout'
    p_continue: float
    p_break: float
    signal_strength: float
    model_name: str
    chunk_id: int


# -----------------------------
# Helpers
# -----------------------------

def connect_db():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = False
    return conn


def load_ticks_slice(conn):
    """
    Load all ticks covering the range where final_trade_signals exist,
    plus a little buffer for MAX_TICKS.
    """
    print("Loading ticks slice...")
    query = f"""
    WITH bounds AS (
        SELECT MIN(id_start) AS min_id,
               MAX(id_start) AS max_id
        FROM final_trade_signals
    )
    SELECT t.id, t.mid
    FROM ticks t
    JOIN bounds b
      ON t.id BETWEEN b.min_id AND (b.max_id + {MAX_TICKS * 2})
    ORDER BY t.id;
    """
    df = pd.read_sql_query(query, conn)
    print(f"  -> Loaded {len(df):,} ticks.")
    if df.empty:
        raise RuntimeError("No ticks loaded – check final_trade_signals / ticks tables.")
    # Add integer position index for fast slicing
    df["pos"] = np.arange(len(df), dtype=np.int64)
    return df


def load_signals(conn):
    """
    Load all final_trade_signals rows, then split into entries and exits.
    """
    print("Loading final_trade_signals...")
    query = """
    SELECT
        vel_grp,
        id_start,
        kal_grp_start,
        p_continue,
        p_break,
        signal_strength,
        kal_chg,
        trend_dir,
        enter_long,
        enter_short,
        exit_approved,
        entry_approved,
        model_name,
        chunk_id
    FROM final_trade_signals
    ORDER BY id_start;
    """
    df = pd.read_sql_query(query, conn)
    print(f"  -> Loaded {len(df):,} signal rows.")

    entries = df[df["entry_approved"]].copy()
    exits = df[df["exit_approved"]].copy()

    print(f"  -> Entries approved: {len(entries):,}")
    print(f"  -> Exits approved:   {len(exits):,}")

    return df, entries.reset_index(drop=True), exits.reset_index(drop=True)


def build_id_to_pos_map(ticks_df):
    """
    Map tick id -> integer position index in ticks_df.
    """
    print("Building id->pos map...")
    ids = ticks_df["id"].values
    # For searchsorted we just keep the sorted id array
    return ids


def simulate_trade_for_entry(entry_row, ticks_df, tick_ids, exit_ids):
    """
    Simulate a single trade for one entry signal.

    entry_row: row from entries_df
    ticks_df: full ticks slice (id, mid, pos)
    tick_ids: sorted numpy array of tick ids (ticks_df["id"].values)
    exit_ids: sorted numpy array of exit signal id_start values
    """
    entry_id = int(entry_row["id_start"])
    vel_grp = int(entry_row["vel_grp"])
    p_cont = float(entry_row["p_continue"])
    p_brk = float(entry_row["p_break"])
    strength = float(entry_row["signal_strength"])
    model_name = str(entry_row["model_name"])
    chunk_id = int(entry_row["chunk_id"])

    # Determine direction
    if bool(entry_row["enter_long"]) and not bool(entry_row["enter_short"]):
        direction = 1
    elif bool(entry_row["enter_short"]) and not bool(entry_row["enter_long"]):
        direction = -1
    else:
        # Invalid / ambiguous: skip
        return None

    # Find entry tick position
    pos0 = np.searchsorted(tick_ids, entry_id)
    if pos0 >= len(tick_ids) or tick_ids[pos0] != entry_id:
        # Entry id not found in ticks slice; skip trade
        return None

    entry_price = float(ticks_df.loc[pos0, "mid"])

    # Define trade horizon in positions
    end_pos = min(pos0 + MAX_TICKS, len(tick_ids) - 1)
    if end_pos <= pos0:
        return None

    # Slice the price path after entry
    mids = ticks_df.loc[pos0 + 1 : end_pos, "mid"].values
    pos_indices = np.arange(pos0 + 1, end_pos + 1, dtype=np.int64)

    # Compute price deltas in trade direction
    deltas = direction * (mids - entry_price)

    # --- 1) TP / SL detection ---
    tp_indices = np.where(deltas >= TP_DOLLARS)[0]
    sl_indices = np.where(deltas <= -SL_DOLLARS)[0]

    tp_pos = pos_indices[tp_indices[0]] if len(tp_indices) > 0 else None
    sl_pos = pos_indices[sl_indices[0]] if len(sl_indices) > 0 else None

    # --- 2) exit_approved detection ---
    # We have sorted exit_ids; find first exit id > entry_id
    idx_exit = np.searchsorted(exit_ids, entry_id + 1)
    exit_pos = None
    if idx_exit < len(exit_ids):
        exit_id = int(exit_ids[idx_exit])
        # Map it to tick position if within horizon
        exit_tick_pos = np.searchsorted(tick_ids, exit_id)
        if exit_tick_pos <= end_pos and tick_ids[exit_tick_pos] == exit_id:
            exit_pos = exit_tick_pos
        else:
            exit_id = None
    else:
        exit_id = None

    # --- 3) Choose earliest event among TP / SL / exit ---
    candidates = []
    if tp_pos is not None:
        candidates.append(("tp", tp_pos))
    if sl_pos is not None:
        candidates.append(("sl", sl_pos))
    if exit_pos is not None:
        candidates.append(("signal_exit", exit_pos))

    if candidates:
        # Pick earliest position
        reason, close_pos = min(candidates, key=lambda x: x[1])
    else:
        reason = "timeout"
        close_pos = end_pos

    close_id = int(tick_ids[close_pos])
    close_price = float(ticks_df.loc[close_pos, "mid"])
    pnl = direction * (close_price - entry_price)
    n_ticks = int(close_pos - pos0)

    return TradeResult(
        vel_grp=vel_grp,
        id_entry=entry_id,
        id_exit=close_id,
        direction=direction,
        entry_price=entry_price,
        exit_price=close_price,
        pnl=pnl,
        n_ticks=n_ticks,
        reason=reason,
        p_continue=p_cont,
        p_break=p_brk,
        signal_strength=strength,
        model_name=model_name,
        chunk_id=chunk_id,
    )


def run_backtest():
    conn = connect_db()

    try:
        ticks_df = load_ticks_slice(conn)
        signals_df, entries_df, exits_df = load_signals(conn)

        tick_ids = build_id_to_pos_map(ticks_df)
        exit_ids = exits_df["id_start"].values

        print("\nRunning backtest...")
        results = []

        for idx, row in entries_df.iterrows():
            if (idx + 1) % 100 == 0:
                print(f"  -> processed {idx + 1} / {len(entries_df)} entries", end="\r")
            trade = simulate_trade_for_entry(row, ticks_df, tick_ids, exit_ids)
            if trade is not None:
                results.append(trade)

        print(f"\nBacktest finished. Trades simulated: {len(results):,}")

        if not results:
            print("No valid trades were generated. Check entry_approved rules.")
            return

        # Convert to DataFrame
        res_df = pd.DataFrame([t.__dict__ for t in results])

        # Summary stats
        total_trades = len(res_df)
        wins = (res_df["pnl"] > 0).sum()
        losses = (res_df["pnl"] < 0).sum()
        breakeven = (res_df["pnl"] == 0).sum()
        win_rate = wins / total_trades

        avg_pnl = res_df["pnl"].mean()
        avg_ticks = res_df["n_ticks"].mean()
        max_drawdown = res_df["pnl"].cumsum().min()
        total_pnl = res_df["pnl"].sum()

        print("\n=== Backtest summary ===")
        print(f"Total trades      : {total_trades:,}")
        print(f"Wins              : {wins:,}")
        print(f"Losses            : {losses:,}")
        print(f"Breakeven         : {breakeven:,}")
        print(f"Win rate          : {win_rate:.4f}")
        print(f"Average PnL ($)   : {avg_pnl:.4f}")
        print(f"Total PnL ($)     : {total_pnl:.2f}")
        print(f"Avg ticks / trade : {avg_ticks:.1f}")
        print(f"Max drawdown ($)  : {max_drawdown:.2f}")

        print("\nPnL by reason:")
        print(res_df.groupby("reason")["pnl"].agg(["count", "mean", "sum"]))

        # Save trades to CSV for later analysis
        res_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nDetailed trade list saved to: {OUTPUT_CSV}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_backtest()
