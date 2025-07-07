#!/usr/bin/env python3

import asyncio
import json
import asyncpg
from datetime import datetime
from openapi import SpotwareClient
from openapi.models import ProtoOASpotEvent

# Load credentials
with open("creds.json", "r") as f:
    creds = json.load(f)

client_id = creds["clientId"]
client_secret = creds["clientSecret"]
access_token = creds["accessToken"]
account_id = creds["accountId"]
symbol_id = creds["symbolId"]
connection_type = creds.get("connectionType", "live").lower()

db_config = {
    "user": "babak",
    "password": "babak33044",
    "database": "trading",
    "host": "localhost",
    "port": 5432,
}

seen_timestamps = set()
last_valid_bid = None
last_valid_ask = None


async def save_tick(conn, timestamp_ms, bid, ask):
    global last_valid_bid, last_valid_ask

    if timestamp_ms in seen_timestamps:
        return

    seen_timestamps.add(timestamp_ms)

    if bid == 0 and last_valid_bid is not None:
        bid = last_valid_bid
    elif bid != 0:
        last_valid_bid = bid

    if ask == 0 and last_valid_ask is not None:
        ask = last_valid_ask
    elif ask != 0:
        last_valid_ask = ask

    dt = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
    bid_float = bid / 100000.0
    ask_float = ask / 100000.0
    mid = round((bid_float + ask_float) / 2, 2)

    try:
        await conn.execute("""
            INSERT INTO ticks (symbol, timestamp, bid, ask, mid)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (symbol, timestamp) DO NOTHING
        """, "XAUUSD", dt, bid_float, ask_float, mid)

        print(f"✅ Tick saved: {dt} | bid={bid_float} ask={ask_float} mid={mid}", flush=True)
    except Exception as e:
        print(f"❌ DB insert error: {e}", flush=True)


async def main():
    db = await asyncpg.connect(**db_config)

    async with SpotwareClient(client_id, client_secret, access_token, live=(connection_type == "live")) as client:
        await client.authenticate()
        await client.account_authorize(account_id)
        await client.subscribe_spots(account_id, symbol_id)

        print(f"📡 Listening for ticks on symbol ID {symbol_id}...")

        async for message in client.listen():
            if isinstance(message, ProtoOASpotEvent):
                tick = message
                await save_tick(db, tick.timestamp, getattr(tick, "bid", 0), getattr(tick, "ask", 0))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Exiting.")
