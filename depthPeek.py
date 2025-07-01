from ctrader_open_api.client import Client
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOADepthEvent,
    ProtoOASubscribeDepthQuotesReq,
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq
)
from twisted.internet import reactor
import json, os
from datetime import datetime

# Load credentials
with open(os.path.expanduser("~/cTrade/creds.json"), "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
symbolId = creds["symbolId"]
host = creds.get("host", "demo.ctraderapi.com")
port = creds.get("port", 5035)
protocol = creds.get("protocol", "protobuf")

depth_snapshots = []

def on_connected(client):
    print("✅ Connected to server. Sending application auth...")
    client.send(ProtoOAApplicationAuthReq(
        clientId=clientId,
        clientSecret=clientSecret
    ))

def on_message(client, message):
    payload = message.payload
    name = type(payload).__name__

    if name == "ProtoOAApplicationAuthRes":
        print("🔐 Application authenticated. Sending account auth...")
        client.send(ProtoOAAccountAuthReq(
            ctidTraderAccountId=accountId,
            accessToken=""
        ))

    elif name == "ProtoOAAccountAuthRes":
        print("✅ Account authenticated. Subscribing to depth...")
        client.send(ProtoOASubscribeDepthQuotesReq(
            ctidTraderAccountId=accountId,
            symbolId=symbolId
        ))

    elif name == "ProtoOADepthEvent":
        on_depth(payload)

def on_depth(event):
    timestamp = datetime.utcnow().isoformat()
    print(f"📊 Depth Snapshot @ {timestamp}")
    for q in event.newQuotes:
        print(f"  Price: {q.price / 100000:.5f}, Volume: {q.volume / 100:.2f}")
    print("-" * 40)
    depth_snapshots.append({
        "timestamp": timestamp,
        "quotes": [(q.price / 100000, q.volume / 100) for q in event.newQuotes]
    })
    if len(depth_snapshots) >= 30:
        reactor.stop()

def main():
    client = Client(host, port, protocol)
    client.setConnectedCallback(on_connected)
    client.setMessageReceivedCallback(on_message)
    client.startService()

if __name__ == "__main__":
    main()
    reactor.run()
