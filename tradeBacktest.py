#!/usr/bin/env python3
"""
tradeBacktest.py

Stage 8.3 (v2) — Sequential backtest with:
  - At most ONE trade open at a time.
  - Entry: entry_approved rows from final_trade_signals.
  - Exit conditions:
      1) exit_approved signal appears (close_signal).
      2) Kalman direction (sign(kal_chg)) flips against trade direction
         (kalman_flip).
      3) Fallback: end_of_data if nothing else happens.

No fixed tick horizon, no TP/SL. Trades can last as long as needed.
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
DB_PASSWORD = "babak33044"
DB_HOST = "localhost"

OUTPUT_CSV = "trade_backtest_results_v2.csv"


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
    reason: str             # 'close_signal', 'kalman_flip', 'end_of_data'
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
    )
    conn.autocommit = False
    return conn


def load_ticks_with_kal(conn):
    """
    Load ticks + kal_chg for the entire range where final_trade_signals exist.
    """
    print("Loading ticks + kal_chg slice...")
    query = """
    WITH bounds AS (
        SELECT
            MIN(id_start) AS min_id,
            MAX(id_start) AS max_id
        FROM final_trade_signals
        WHERE entry_approved
    )
    SELECT
        t.id,
        t.mid,
        s.kal_chg
    FROM ticks t
    JOIN segments s ON s.id = t.id
    JOIN bounds b
      ON t.id BETWEEN b.min_id AND (b.max_id + 10000) -- small buffer
    ORDER BY t.id;
    """
    df = pd.read_sql_query(query, conn)
    print(f"  -> Loaded {len(df):,} ticks.")
    if df.empty:
        raise RuntimeError("No ticks loaded – check final_trade_signals / ticks tables.")

    # Trend direction per tick: sign of kal_chg
    df["trend_dir"] = np.sign(df["kal_chg"]).astype(int)
    return df


def load_signals(conn):
    """
    Load final_trade_signals, and extract entries/exits.
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


def run_backtest():
    conn = connect_db()

    try:
        ticks_df = load_ticks_with_kal(conn)
        signals_df, entries_df, exits_df = load_signals(conn)

        # Arrays for fast iteration
        tick_ids = ticks_df["id"].values
        tick_mids = ticks_df["mid"].values
        tick_trend = ticks_df["trend_dir"].values

        # Map entry id -> entry row (only first one if duplicates)
        entry_map = {}
        for _, row in entries_df.iterrows():
            eid = int(row["id_start"])
            # if multiple signals share same id_start, keep the first
            if eid not in entry_map:
                entry_map[eid] = row

        # Set of exit ids for quick lookup
        exit_ids = set(int(x) for x in exits_df["id_start"].values)

        print("\nRunning sequential backtest (one trade at a time)...")
        results = []

        current_trade = None  # dict with trade info
        n_ticks = len(tick_ids)

        for pos in range(n_ticks):
            tid = int(tick_ids[pos])
            mid = float(tick_mids[pos])
            trend_dir = int(tick_trend[pos])

            if current_trade is None:
                # No open trade: can we open one here?
                entry_row = entry_map.get(tid)
                if entry_row is not None:
                    enter_long = bool(entry_row["enter_long"])
                    enter_short = bool(entry_row["enter_short"])

                    # skip ambiguous or empty signals
                    if enter_long == enter_short:
                        continue

                    direction = 1 if enter_long else -1

                    current_trade = {
                        "vel_grp": int(entry_row["vel_grp"]),
                        "id_entry": tid,
                        "entry_pos": pos,
                        "entry_price": mid,
                        "direction": direction,
                        "p_continue": float(entry_row["p_continue"]),
                        "p_break": float(entry_row["p_break"]),
                        "signal_strength": float(entry_row["signal_strength"]),
                        "model_name": str(entry_row["model_name"]),
                        "chunk_id": int(entry_row["chunk_id"]),
                    }
            else:
                # Trade is open: check closing conditions
                close_reason = None

                # 1) close signal
                if tid in exit_ids:
                    close_reason = "close_signal"
                # 2) Kalman direction flips against trade
                elif trend_dir != 0 and trend_dir != current_trade["direction"]:
                    close_reason = "kalman_flip"

                if close_reason is not None:
                    # Close trade
                    exit_price = mid
                    pnl = current_trade["direction"] * (exit_price - current_trade["entry_price"])
                    n_tr_ticks = pos - current_trade["entry_pos"]

                    results.append(
                        TradeResult(
                            vel_grp=current_trade["vel_grp"],
                            id_entry=current_trade["id_entry"],
                            id_exit=tid,
                            direction=current_trade["direction"],
                            entry_price=current_trade["entry_price"],
                            exit_price=exit_price,
                            pnl=pnl,
                            n_ticks=n_tr_ticks,
                            reason=close_reason,
                            p_continue=current_trade["p_continue"],
                            p_break=current_trade["p_break"],
                            signal_strength=current_trade["signal_strength"],
                            model_name=current_trade["model_name"],
                            chunk_id=current_trade["chunk_id"],
                        )
                    )
                    current_trade = None  # flat again

        # End of data – close any open trade
        if current_trade is not None and n_ticks > 0:
            tid = int(tick_ids[-1])
            mid = float(tick_mids[-1])
            exit_price = mid
            pnl = current_trade["direction"] * (exit_price - current_trade["entry_price"])
            n_tr_ticks = (n_ticks - 1) - current_trade["entry_pos"]

            results.append(
                TradeResult(
                    vel_grp=current_trade["vel_grp"],
                    id_entry=current_trade["id_entry"],
                    id_exit=tid,
                    direction=current_trade["direction"],
                    entry_price=current_trade["entry_price"],
                    exit_price=exit_price,
                    pnl=pnl,
                    n_ticks=n_tr_ticks,
                    reason="end_of_data",
                    p_continue=current_trade["p_continue"],
                    p_break=current_trade["p_break"],
                    signal_strength=current_trade["signal_strength"],
                    model_name=current_trade["model_name"],
                    chunk_id=current_trade["chunk_id"],
                )
            )

        print(f"\nBacktest finished. Trades simulated: {len(results):,}")

        if not results:
            print("No trades were generated. Check entry_approved rules.")
            return

        res_df = pd.DataFrame([t.__dict__ for t in results])

        # Summary stats
        total_trades = len(res_df)
        wins = (res_df["pnl"] > 0).sum()
        losses = (res_df["pnl"] < 0).sum()
        breakeven = (res_df["pnl"] == 0).sum()
        win_rate = wins / total_trades

        avg_pnl = res_df["pnl"].mean()
        total_pnl = res_df["pnl"].sum()
        avg_ticks = res_df["n_ticks"].mean()
        max_drawdown = res_df["pnl"].cumsum().min()

        print("\n=== Backtest summary (v2) ===")
        print(f"Total trades      : {total_trades:,}")
        print(f"Wins              : {wins:,}")
        print(f"Losses            : {losses:,}")
        print(f"Breakeven         : {breakeven:,}")
        print(f"Win rate          : {win_rate:.4f}")
        print(f"Average PnL ($)   : {avg_pnl:.6f}")
        print(f"Total PnL ($)     : {total_pnl:.2f}")
        print(f"Avg ticks / trade : {avg_ticks:.1f}")
        print(f"Max drawdown ($)  : {max_drawdown:.2f}")

        print("\nPnL by reason:")
        print(res_df.groupby("reason")["pnl"].agg(["count", "mean", "sum"]))

        res_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nDetailed trade list saved to: {OUTPUT_CSV}")

    finally:
        conn.close()


if __name__ == "__main__":
    run_backtest()
