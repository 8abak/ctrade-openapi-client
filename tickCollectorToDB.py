#!/usr/bin/env python3

import json
import os
import psycopg2
import signal
from datetime import datetime, timezone
from twisted.internet import reactor
from ctrader_open_api import Client, EndPoints
from ctrader_open_api.tcpProtocol import TcpProtocol
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

# Database connection
conn = psycopg2.connect(dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432)
cur = conn.cursor()

# Tick memory
lastValidBid = None
lastValidAsk = None

def on_tick(message):
    global lastValidBid, lastValidAsk

    if not isinstance(message, ProtoOASpotEvent):
        return

    timestamp = datetime.fromtimestamp(message.timestamp / 1000.0, tz=timezone.utc)
    bid = message.bid
    ask = message.ask

    if bid == 0.0 and lastValidBid is not None:
        bid = lastValidBid
    elif bid != 0.0:
        lastValidBid = bid

    if ask == 0.0 and lastValidAsk is not None:
        ask = lastValidAsk
    elif ask != 0.0:
        lastValidAsk = ask

    try:
        cur.execute(
            """
            INSERT INTO ticks (symbol, timestamp, bid, ask)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING;
            """,
            ("XAUUSD", timestamp, bid, ask)
        )
        conn.commit()
    except Exception as e:
        print(f"DB error: {e}")
        conn.rollback()

# Setup protocol + client
protocol = TcpProtocol()
client = Client(host, port, protocol)

# Callback setup
def on_connect():
    print("Connected to server.")
    client.send(ProtoOAApplicationAuthReq(clientId=clientId, clientSecret=clientSecret))

def on_message(_, message):
    if isinstance(message, ProtoOASpotEvent):
        on_tick(message)
    elif message.payloadType == 2105:  # Application Auth Response
        client.send(ProtoOAAccountAuthReq(accessToken=accessToken, ctidTraderAccountId=accountId))
    elif message.payloadType == 2107:  # Account Auth Response
        client.send(ProtoOASubscribeSpotsReq(ctidTraderAccountId=accountId, symbolId=symbolId))

client.setConnectedCallback(lambda _: on_connect())
client.setDisconnectedCallback(lambda *_: print("Disconnected."))
client.setMessageReceivedCallback(on_message)

# Graceful shutdown
def shutdown(*args):
    print("Shutting down...")
    client.stopService()
    cur.close()
    conn.close()
    reactor.stop()

signal.signal(signal.SIGINT, shutdown)

# Start
client.startService()
reactor.run()
