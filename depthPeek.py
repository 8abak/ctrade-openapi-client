import asyncio
from datetime import datetime
from ctrader_open_api.client import Client
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOADepthEvent,
    ProtoOASubscribeDepthQuotesReq
)

# From your tickCollector
CLIENT_ID = "12367_TW3qWwxsCpLIBjiuU6QQBkzqkfqd6j9cgYiCNikEGFtNlbwMum"
CLIENT_SECRET = "8Uh3BVz5QzjsLKmaL0iuyfQ4ANaGiS3hlPulL4eyPk31P5b8t2"
ACCOUNT_ID = 41216916
SYMBOL_ID = 1  # XAUUSD

depth_snapshots = []

def on_depth(event):
    timestamp = datetime.utcnow().isoformat()
    print(f"📊 Depth Snapshot @ {timestamp}")
    for q in event.newQuotes:
        print(f"  Price: {q.price / 100000:.5f}, Volume: {q.volume / 100:.2f}")
    print("-" * 40)

    snapshot = {
        "timestamp": timestamp,
        "quotes": [(q.price / 100000, q.volume / 100) for q in event.newQuotes]
    }
    depth_snapshots.append(snapshot)

    if len(depth_snapshots) >= 30:
        asyncio.get_event_loop().stop()

async def run_depth_peek():
    client = Client(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    await client.connect()

    client.on(ProtoOADepthEvent, on_depth)

    await client.send(ProtoOASubscribeDepthQuotesReq(
        ctidTraderAccountId=ACCOUNT_ID,
        symbolId=SYMBOL_ID
    ))

    print("📡 Subscribed to depth stream. Waiting for 30 events...")
    await asyncio.get_event_loop().create_future()

if __name__ == "__main__":
    asyncio.run(run_depth_peek())
