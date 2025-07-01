import json
import os
import asyncio
from datetime import datetime
from ctrader_open_api.client import Client
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOADepthEvent,
    ProtoOASubscribeDepthQuotesReq
)

# Load credentials
with open(os.path.expanduser("~/cTrade/creds.json"), "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
accessToken = creds["accessToken"]
symbolId = creds["symbolId"]
connectionType = creds.get("connectionType", "live").lower()


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
    client = Client()
    await client.connect(client_id=clientId, client_secret=clientSecret)

    client.on(ProtoOADepthEvent, on_depth)

    await client.send(ProtoOASubscribeDepthQuotesReq(
        ctidTraderAccountId=accountId,
        symbolId=symbolId
    ))

    print("📡 Subscribed to depth stream. Waiting for 30 events...")
    await asyncio.get_event_loop().create_future()

if __name__ == "__main__":
    asyncio.run(run_depth_peek())
