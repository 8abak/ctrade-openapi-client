#!/usr/bin/env python3

import json
import os
import csv
from datetime import datetime
from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq, ProtoOASubscribeSpotsReq
import signal

# Load credentials
with open(os.path.expanduser("~/cTrade/creds.json"), "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
accessToken = creds["accessToken"]
symbolId = creds["symbolId"]
connectionType = creds.get("connectionType", "live").lower()

host = EndPoints.PROTOBUF_LIVE_HOST if connectionType == "live" else EndPoints.PROTOBUF_DEMO_HOST
port = EndPoints.PROTOBUF_PORT

client = Client(host=host, port=port, protocol=TcpProtocol)

csv_file = "ticks.csv"
lastTimestamp = None

# Prepare CSV
if not os.path.exists(csv_file):
    with open(csv_file, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "datetime", "symbolId", "bid", "ask"])

def write_tick(timestamp, symbolId, bid, ask):
    global lastTimestamp
    if timestamp == lastTimestamp:
        return
    lastTimestamp = timestamp
    with open(csv_file, mode="a", newline="") as f:
        writer = csv.writer(f)
        dt = datetime.fromtimestamp(timestamp / 1000.0).strftime("%Y-%m-%d %H:%M:%S.%f")
        writer.writerow([timestamp, dt, symbolId, bid / 100000.0, ask / 100000.0])
        print(f"💾 Saved tick: {symbolId} @ {dt} | bid={bid / 100000.0}, ask={ask / 100000.0}")

def connected(_):
    print("✅ Connected. Subscribing to spot data...")
    # Step 1: Authenticate app
    authMsg = ProtoOAApplicationAuthReq()
    authMsg.clientId = clientId
    authMsg.clientSecret = clientSecret
    deferred = client.send(authMsg)

    def afterAppAuth(_):
        print("🎉 API Application authorized")
        # Step 2: Authenticate account
        accountAuth = ProtoOAAccountAuthReq()
        accountAuth.ctidTraderAccountId = accountId
        accountAuth.accessToken = accessToken
        return client.send(accountAuth)

    def afterAccountAuth(_):
        print(f"🔐 Account {accountId} authorized. Starting tick logging.")
        subscribeToSpot()

    deferred.addCallback(afterAppAuth)
    deferred.addCallback(afterAccountAuth)
    deferred.addErrback(onError)

def subscribeToSpot():
    req = ProtoOASubscribeSpotsReq()
    req.ctidTraderAccountId = accountId
    req.symbolId.append(symbolId)
    req.subscribeToSpotTimestamp = True
    client.send(req)

def disconnected(_, reason):
    print(f"
🔌 Disconnected: {reason}")
    reactor.stop()

def onMessage(_, message):
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASpotEvent
    if message.payloadType == ProtoOASpotEvent().payloadType:
        spot = Protobuf.extract(message)
        write_tick(spot.timestamp, spot.symbolId, getattr(spot, "bid", 0), getattr(spot, "ask", 0))

def onError(err):
    print("❌ Error during connection or authentication:")
    print(err)
    reactor.stop()

def handle_sigint(signum, frame):
    print("\n🛑 Gracefully shutting down...")
    reactor.stop()

signal.signal(signal.SIGINT, handle_sigint)

client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMessage)

client.startService()
reactor.run()
