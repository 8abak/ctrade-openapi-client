
#!/usr/bin/env python3

import json
import os
import psycopg2
from datetime import datetime
from twisted.internet import reactor
from ctrader_open_api import Client, EndPoints
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

# Database connection setup
conn = psycopg2.connect(dbname="trading", user="babak", password="BB@bb33044", host="localhost", port=5432)
cur = conn.cursor()

# Tick memory for forward-fill
lastValidBid = None
lastValidAsk = None

# Tick handler
def on_tick(message):
    global lastValidBid, lastValidAsk

    if not isinstance(message, ProtoOASpotEvent):
        return

    timestamp = datetime.utcfromtimestamp(message.timestamp / 1000.0)
    bid = message.bid
    ask = message.ask

    # Forward-fill 0.0
    if bid == 0.0 and lastValidBid is not None:
        bid = lastValidBid
    elif bid != 0.0:
        lastValidBid = bid

    if ask == 0.0 and lastValidAsk is not None:
        ask = lastValidAsk
    elif ask != 0.0:
        lastValidAsk = ask

    # Insert into DB
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

# Client setup
client = Client(host, port)

def on_connect():
    print("Connected to server.")
    client.send(ProtoOAApplicationAuthReq(clientId=clientId, clientSecret=clientSecret))

def on_app_auth_response():
    client.send(ProtoOAAccountAuthReq(accessToken=accessToken, ctidTraderAccountId=accountId))

def on_account_auth_response():
    client.send(ProtoOASubscribeSpotsReq(ctidTraderAccountId=accountId, symbolId=symbolId))

client.on_connect = on_connect
client.on_disconnect = lambda: print("Disconnected.")
client.on_receive = lambda message: (
    on_tick(message) if isinstance(message, ProtoOASpotEvent) else None
)

client.on_application_auth_response = on_app_auth_response
client.on_account_auth_response = on_account_auth_response

# Start the connection
client.connect()
reactor.run()
