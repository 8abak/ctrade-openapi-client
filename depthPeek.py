import asyncio
from datetime import datetime
from ctrader_open_api.client import Client
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOASubscribeDepthQuotesReq,
    ProtoOAQuoteDepthEvent
)

# From your tickCollector
CLIENT_ID = "12367_TW3qWwxsCpLIBjiuU6QQBkzqkfqd6j9cgYiCNikEGFtNlbwMum"
CLIENT_SECRET = "8Uh3BVz5QzjsLKmaL0iuyfQ4ANaGiS3hlPulL4eyPk31P5b8t2"
ACCOUNT_ID = 41216916
SYMBOL_ID = 1  # XAUUSD

depth_snapshots = []

def on_depth(event):
    timestamp = datetime.utcnow().isoformat()
    bids = [(b.price, b.volume) for b in event.depthQuote.bid]
    asks = [(a.price, a.volume) for a in event.depthQuote.ask]
    snapshot = {
        "timestamp": timestamp,
        "symbolId": event.symbolId,
        "bids": bids,
        "asks": asks
    }
    print(f"📊 Depth Snapshot @ {timestamp} — Bids: {len(bids)}, Asks: {len(asks)}")
    print("  Top 3 Bids:")
    for b in bids[:3]:
        print(f"    Price: {b[0]:.5f} | Volume: {b[1]:.2f}")
    print("  Top 3 Asks:")
    for a in asks[:3]:
        print(f"    Price: {a[0]:.5f} | Volume: {a[1]:.2f}")
    print("-" * 40)

    depth_snapshots.append(snapshot)
    if len(depth_snapshots) >= 30:
        asyncio.get_event_loop().stop()

async def run_depth_peek():
    client = Client(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    await client.connect()
    client.on(ProtoOAQuoteDepthEvent, on_depth)
    await client.send(ProtoOASubscribeDepthQuotesReq(
        ctidTraderAccountId=ACCOUNT_ID,
        symbolId=SYMBOL_ID
    ))
    print("📡 Subscribed to depth stream. Waiting for 30 events...")
    await asyncio.get_event_loop().create_future()

if __name__ == "__main__":
    asyncio.run(run_depth_peek())
