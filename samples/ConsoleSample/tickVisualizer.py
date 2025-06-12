#!/usr/bin/env python3

import json
import os
import csv
import signal
from datetime import datetime
import streamlit as st
import pandas as pd
from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOASubscribeSpotsReq,
    ProtoOAUnsubscribeSpotsReq,
    ProtoOASpotEvent
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

host = EndPoints.PROTOBUF_LIVE_HOST if connectionType == "live" else EndPoints.PROTOBUF_DEMO_HOST
port = EndPoints.PROTOBUF_PORT

client = Client(host=host, port=port, protocol=TcpProtocol)

csvFile = "ticks.csv"
seenTimestamps = set()

# Prepare CSV
if os.path.exists(csvFile):
    with open(csvFile, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            seenTimestamps.add(row["timestamp"])
else:
    with open(csvFile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "datetime", "symbolId", "bid", "ask"])

placeholder = st.empty()

@st.cache_data(ttl=0.0, show_spinner=False)
def load_data():
    if os.path.exists(csvFile):
        return pd.read_csv(csvFile)
    return pd.DataFrame(columns=["timestamp", "datetime", "symbolId", "bid", "ask"])

def update_stream():
    df = load_data()
    placeholder.dataframe(df.tail(20), use_container_width=True)

def writeTick(timestamp, symbolId, bid, ask):
    if str(timestamp) in seenTimestamps:
        print(f"⏩ Duplicate tick skipped: {timestamp}")
        return

    seenTimestamps.add(str(timestamp))
    dt = datetime.fromtimestamp(timestamp / 1000.0).strftime("%Y-%m-%d %H:%M:%S.%f")
    with open(csvFile, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, dt, symbolId, bid / 100000.0, ask / 100000.0])
    print(f"📂 Tick saved: symbolId={symbolId}, bid={bid / 100000.0}, ask={ask / 100000.0} @ {dt}")
    update_stream()

def connected(_):
    print("✅ Connected. Subscribing to spot data...")
    authMsg = ProtoOAApplicationAuthReq()
    authMsg.clientId = clientId
    authMsg.clientSecret = clientSecret
    deferred = client.send(authMsg)

    def afterAppAuth(_):
        print("🎉 API Application authorized")
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
    print(f"🔌 Disconnected: {reason}")
    reactor.stop()

def onMessage(_, message):
    if message.payloadType == ProtoOASpotEvent().payloadType:
        spot = Protobuf.extract(message)
        writeTick(spot.timestamp, spot.symbolId, getattr(spot, "bid", 0), getattr(spot, "ask", 0))

def onError(err):
    print("❌ Error during connection or authentication:")
    print(err)
    reactor.stop()

def handleSigint(signum, frame):
    print("\n🚩 Gracefully shutting down...")
    reactor.stop()

signal.signal(signal.SIGINT, handleSigint)

client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMessage)

client.startService()
reactor.run()
