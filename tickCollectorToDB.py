#!/usr/bin/env python3

import json
import os
import signal
import psycopg2
from datetime import datetime, timedelta
import pytz
from twisted.internet import reactor
from threading import Event

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOASubscribeSpotsReq,
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

# Setup PostgreSQL connection
conn = psycopg2.connect(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)
cur = conn.cursor()

client = Client(host=host, port=port, protocol=TcpProtocol)

seenTimestamps = set()
lastValidBid = None
lastValidAsk = None
shutdown_event = Event()

def writeTick(timestamp, symbolId, bid, ask):
    global lastValidBid, lastValidAsk

    if str(timestamp) in seenTimestamps:
        return

    # Forward-fill
    if bid == 0.0 and lastValidBid is not None:
        bid = lastValidBid
    elif bid != 0.0:
        lastValidBid = bid

    if ask == 0.0 and lastValidAsk is not None:
        ask = lastValidAsk
    elif ask != 0.0:
        lastValidAsk = ask

    seenTimestamps.add(str(timestamp))

    # Convert from milliseconds since epoch to datetime in Sydney time
    utc_dt = datetime.utcfromtimestamp(timestamp / 1000.0)
    utc_dt = pytz.utc.localize(utc_dt)
    sydney_dt = utc_dt.astimezone(pytz.timezone("Australia/Sydney"))

    bidFloat = bid / 100000.0
    askFloat = ask / 100000.0
    mid = round((bidFloat + askFloat) / 2, 2)

    try:
        print(f"🕒 Inserting timestamp: {sydney_dt} | tzinfo: {sydney_dt.tzinfo}", flush=True)
        cur.execute(
            """
            INSERT INTO ticks (symbol, timestamp, bid, ask, mid)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING;
            """,
            ("XAUUSD", sydney_dt, bidFloat, askFloat, mid)
        )
        conn.commit()
    except Exception as e:
        print(f"❌ DB error: {e}", flush=True)
        conn.rollback()


def connected(_):
    print("✅ Connected. Subscribing to spot data...", flush=True)
    authMsg = ProtoOAApplicationAuthReq()
    authMsg.clientId = clientId
    authMsg.clientSecret = clientSecret
    deferred = client.send(authMsg)

    def afterAppAuth(_):
        print("🎉 API Application authorized", flush=True)
        accountAuth = ProtoOAAccountAuthReq()
        accountAuth.ctidTraderAccountId = accountId
        accountAuth.accessToken = accessToken
        return client.send(accountAuth)

    def afterAccountAuth(_):
        print(f"🔐 Account {accountId} authorized. Starting tick logging.", flush=True)
        print("🚦 Calling subscribeToSpot now...", flush=True)
        subscribeToSpot()

    deferred.addCallback(afterAppAuth)
    deferred.addCallback(afterAccountAuth)
    deferred.addErrback(onError)

def subscribeToSpot():
    print("📡 Subscribing to symbolId:", symbolId, flush=True)
    req = ProtoOASubscribeSpotsReq()
    req.ctidTraderAccountId = accountId
    req.symbolId.append(symbolId)
    req.subscribeToSpotTimestamp = True
    client.send(req)

def disconnected(_, reason):
    print(f"🔌 Disconnected: {reason}", flush=True)
    shutdown()

def onMessage(_, message):
    #print("📦 Raw message received:", message.payloadType, "→", Protobuf.get(message.payloadType).__class__.__name__, flush=True)

    if message.payloadType == ProtoOASpotEvent().payloadType:
        try:
            spot = Protobuf.extract(message)
            print("📩 Spot received →", spot.symbolId, spot.timestamp, getattr(spot, "bid", 0), getattr(spot, "ask", 0))
            writeTick(spot.timestamp, spot.symbolId, getattr(spot, "bid", 0), getattr(spot, "ask", 0))
        except Exception as e:
            print("⚠️ Error processing spot message:", e, flush=True)

    if message.payloadType == 2142:  # ProtoOAErrorRes
        try:
            error = Protobuf.extract(message)
            print("❌ Error Received:")
            print("  Error Code:", error.errorCode)
            print("  Description:", error.description)
            print("  Payload Type:", error.payloadType)
        except Exception as e:
            print("⚠️ Failed to parse error message:", e, flush=True)





def onError(err):
    print("❌ Error during connection or authentication:", flush=True)
    print(err, flush=True)
    shutdown()

def shutdown():
    if shutdown_event.is_set():
        return
    shutdown_event.set()
    print("🚩 Gracefully shutting down....", flush=True)
    try:
        cur.close()
        conn.close()
    except:
        pass
    if reactor.running:
        reactor.callFromThread(reactor.stop)

signal.signal(signal.SIGINT, lambda s, f: shutdown())
signal.signal(signal.SIGTERM, lambda s, f: shutdown())

client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMessage)

client.startService()
reactor.run()
