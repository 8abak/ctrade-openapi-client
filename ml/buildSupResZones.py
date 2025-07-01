from sqlalchemy import create_engine, text
import pandas as pd

# ───────────────────────────────────────────────────────
# DB SETUP
engine = create_engine("postgresql+psycopg2://babak:babak33044@localhost:5432/trading")

# ───────────────────────────────────────────────────────
# PARAMETERS
START_DATE = "2025-06-25"
WINDOW = 2
HOLD_TICKS = 100
TOLERANCE = 0.1  # price buffer
ZONE_WIDTH = 0.15  # visual band thickness

# ───────────────────────────────────────────────────────
# FETCH TICKS FROM DB
with engine.begin() as conn:
    first_tick = conn.execute(text("""
        SELECT id FROM ticks
        WHERE timestamp >= :date
        ORDER BY id ASC
        LIMIT 1
    """), {"date": START_DATE}).fetchone()

    if not first_tick:
        raise Exception("No ticks found on or after 25 June")

    df = pd.read_sql(
        text("SELECT id, mid FROM ticks WHERE id >= :start_id ORDER BY id ASC"),
        conn,
        params={"start_id": first_tick[0]}
    )


# ───────────────────────────────────────────────────────
# FIND SWING HIGHS / LOWS
def detect_pivots(df, window):
    highs, lows = [], []
    for i in range(window, len(df) - window):
        mid = df.mid.iloc[i]
        left = df.mid.iloc[i-window:i]
        right = df.mid.iloc[i+1:i+1+window]
        if all(mid > x for x in left) and all(mid > x for x in right):
            highs.append((df.id.iloc[i], mid))
        elif all(mid < x for x in left) and all(mid < x for x in right):
            lows.append((df.id.iloc[i], mid))
    return highs, lows

highs, lows = detect_pivots(df, window=WINDOW)

# ───────────────────────────────────────────────────────
# TRACK ZONES
def build_zones(df, pivots, zone_type):
    zones = []
    for tickid, price in pivots:
        idx = df.index[df.id == tickid]
        if idx.empty:
            continue
        start_index = idx[0]
        sliced = df.iloc[start_index+1:]
        price_low = price - ZONE_WIDTH if zone_type == "support" else price - TOLERANCE
        price_high = price + TOLERANCE if zone_type == "support" else price + ZONE_WIDTH

        tickid_end = None
        for i in range(start_index+1, len(df)):
            p = df.mid.iloc[i]
            if (zone_type == "support" and p < price_low) or (zone_type == "resistance" and p > price_high):
                tickid_end = df.id.iloc[i]
                break

        zones.append(dict(
            level_type=zone_type,
            price_low=round(price_low, 5),
            price_high=round(price_high, 5),
            tickid_start=tickid,
            tickid_end=tickid_end,
            confirmed=True
        ))
    return zones

support_zones = build_zones(df, lows, "support")
resistance_zones = build_zones(df, highs, "resistance")
all_zones = support_zones + resistance_zones

# ───────────────────────────────────────────────────────
# INSERT TO DATABASE
with engine.begin() as conn:
    for z in all_zones:
        conn.execute(text("""
            INSERT INTO supRes (level_type, price_low, price_high, tickid_start, tickid_end, confirmed)
            VALUES (:level_type, :price_low, :price_high, :tickid_start, :tickid_end, :confirmed)
        """), z)

print(f"Inserted {len(all_zones)} support/resistance zones into supRes.")
