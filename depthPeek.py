from ctrader_open_api.client import Client
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOADepthEvent,
    ProtoOASubscribeDepthQuotesReq
)
from ctrader_open_api.protocol import ProtoMessageFactory
from ctrader_open_api.service import MyService
from twisted.internet import reactor
import json, os
from datetime import datetime

# Load creds
with open(os.path.expanduser("~/cTrade/creds.json"), "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
symbolId = creds["symbolId"]
host = creds.get("host", "demo.ctraderapi.com")  # or live.ctraderapi.com
port = creds.get("port", 5035)
protocol = creds.get("protocol", "protobuf")  # usually protobuf

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
        reactor.stop()

def main():
    client = Client(host, port, protocol)
    client.on(ProtoOADepthEvent, on_depth)

    service = MyService(client, clientId, clientSecret, accountId)
    
    def on_auth(_):
        print("✅ Authenticated. Subscribing to depth...")
        client.send(ProtoOASubscribeDepthQuotesReq(
            ctidTraderAccountId=accountId,
            symbolId=symbolId
        ))

    service.on_auth = on_auth
    service.startService()

if __name__ == "__main__":
    main()
    reactor.run()
