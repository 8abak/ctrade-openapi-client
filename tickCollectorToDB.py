#!/usr/bin/env python3
import json
import os
import signal
import psycopg2
from datetime import datetime
import pytz
from threading import Event
import requests

from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAAccountAuthReq,
    ProtoOASubscribeSpotsReq,
    ProtoOASpotEvent,
)

# ---------------------------
# Load credentials
# ---------------------------
creds_file = os.path.expanduser("~/cTrade/creds.json")
with open(creds_file, "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
accessToken = creds["accessToken"]
refreshToken = creds["refreshToken"]
symbolId = creds["symbolId"]

connectionType = creds.get("connectionType", "live").lower()
host = EndPoints.PROTOBUF_LIVE_HOST if connectionType == "live" else EndPoints.PROTOBUF_DEMO_HOST
port = EndPoints.PROTOBUF_PORT

# --------------------------------------------
# PostgreSQL connection
# --------------------------------------------
DB_KW = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)
conn = None

def ensure_conn():
    global conn
    if conn is None or getattr(conn, "closed", 1):
        conn = psycopg2.connect(**DB_KW)

# ---------------------------------
# Initialize cTrader Open API client
# ---------------------------------
client = Client(host=host, port=port, protocol=TcpProtocol)

lastValidBid = None
lastValidAsk = None
shutdown_event = Event()

# --------------------------------------------
# Simple 1D Kalman filter for mid -> kal
# --------------------------------------------

class ScalarKalmanFilter:
    """
    x_t = x_{t-1} + w_t      (process noise ~ N(0, q))
    z_t = x_t     + v_t      (measurement noise ~ N(0, r))
    """
    def __init__(self, process_var=1e-4, meas_var=1e-2, init_var=1.0):
        self.q = float(process_var)
        self.r = float(meas_var)
        self.init_var = float(init_var)
        self.x = None  # state estimate
        self.P = None  # variance

    def step(self, z: float) -> float:
        """
        Advance filter with observation z, return updated state x_t.
        If uninitialized, start at x0 = z.
        """
        z = float(z)
        if self.x is None or self.P is None:
            self.x = z
            self.P = self.init_var

        # Predict
        x_prior = self.x
        P_prior = self.P + self.q

        # Update
        K = P_prior / (P_prior + self.r)
        x_post = x_prior + K * (z - x_prior)
        P_post = (1.0 - K) * P_prior

        self.x = x_post
        self.P = P_post
        return x_post

kalman_filter = ScalarKalmanFilter(
    process_var=1e-4,
    meas_var=1e-2,
    init_var=1.0,
)


# --------------------------------------------------
# Token refresh logic
# --------------------------------------------------
def refresh_tokens():
    global accessToken, refreshToken, creds
    print("🔄 Refreshing tokens...", flush=True)
    resp = requests.post(
        "https://openapi.ctrader.com/apps/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refreshToken,
            "client_id": clientId,
            "client_secret": clientSecret,
        },
    )

    if resp.status_code != 200:
        print(f"❌ Failed to refresh tokens: {resp.text}", flush=True)
        return False

    tokens = resp.json()
    accessToken = tokens["access_token"]
    if "refresh_token" in tokens:
        refreshToken = tokens["refresh_token"]
    creds["accessToken"] = accessToken
    creds["refreshToken"] = refreshToken
    creds["tokenType"] = tokens.get("token_type", "Bearer")

    with open(creds_file, "w") as f:
        json.dump(creds, f, indent=2)

    print("✅ Tokens refreshed and creds.json updated", flush=True)
    return True

# --------------------------------------------------
# Write one tick  (fast path, no duplicate SELECT)
# --------------------------------------------------
def writeTick(timestamp, _symbolId, bid, ask):
    global lastValidBid, lastValidAsk, conn

    # Forward-fill bid/ask if zeros appear
    if bid == 0.0 and lastValidBid is not None:
        bid = lastValidBid
    elif bid != 0.0:
        lastValidBid = bid

    if ask == 0.0 and lastValidAsk is not None:
        ask = lastValidAsk
    elif ask != 0.0:
        lastValidAsk = ask

    # If still missing (first tick etc.), just skip
    if bid == 0.0 or ask == 0.0:
        print("⚠️ Skipping tick with no valid bid/ask", flush=True)
        return

    # Convert timestamp (ms since epoch, UTC) to Sydney local time
    utc_dt = datetime.utcfromtimestamp(timestamp / 1000.0).replace(tzinfo=pytz.utc)
    sydney_tz = pytz.timezone("Australia/Sydney")
    sydney_dt = utc_dt.astimezone(sydney_tz)

    # Scale cTrader prices from 1e-5 units
    bidFloat = bid / 100000.0
    askFloat = ask / 100000.0
    mid = round((bidFloat + askFloat) / 2.0, 2)
    spread = round(askFloat - bidFloat, 2)

    try:
        ensure_conn()
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO ticks (symbol, timestamp, bid, ask, mid, spread)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    "XAUUSD",
                    sydney_dt,
                    bidFloat,
                    askFloat,
                    mid,
                    spread,
                ),
            )
        conn.commit()
        print(
            f"✅ Saved tick: {sydney_dt} → bid={bidFloat:.2f}, ask={askFloat:.2f}, "
            f"mid={mid:.2f}, spread={spread:.2f}",
            flush=True,
        )

    except Exception as e:
        print(f"❌ DB error: {e}", flush=True)
        try:
            if conn and not getattr(conn, "closed", 1):
                conn.rollback()
        except Exception:
            pass

# ---------------------------
# Connection/auth flow
# ---------------------------
def connected(_):
    print("✅ Connected. Subscribing to spot data...", flush=True)
    authMsg = ProtoOAApplicationAuthReq()
    authMsg.clientId = clientId
    authMsg.clientSecret = clientSecret
    deferred = client.send(authMsg)

    def afterAppAuth(_):
        print("🎉 Application authorized", flush=True)
        accountAuth = ProtoOAAccountAuthReq()
        accountAuth.ctidTraderAccountId = accountId
        accountAuth.accessToken = accessToken
        return client.send(accountAuth)

    def afterAccountAuth(_):
        print(f"🔐 Account {accountId} authorized. Subscribing...", flush=True)
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
    print(f"🔌 Disconnected: {reason}", flush=True)
    shutdown()

def onMessage(_, message):
    if message.payloadType == ProtoOASpotEvent().payloadType:
        try:
            spot = Protobuf.extract(message)
            writeTick(
                spot.timestamp,
                spot.symbolId,
                getattr(spot, "bid", 0),
                getattr(spot, "ask", 0),
            )
        except Exception as e:
            print("⚠️ Error processing spot message:", e, flush=True)

    # ProtoOAErrorRes
    if message.payloadType == 2142:
        try:
            error = Protobuf.extract(message)
            print("❌ Error:")
            print("  Code:", error.errorCode)
            print("  Desc:", error.description)

            # Auto refresh if token invalid
            if error.errorCode in ("CH_ACCESS_TOKEN_INVALID", "INVALID_REQUEST"):
                if refresh_tokens():
                    print("🔄 Restarting auth flow with new token...", flush=True)
                    reactor.callLater(2, connected, None)

        except Exception as e:
            print("⚠️ Failed to parse error message:", e, flush=True)

def onError(err):
    print("❌ Connection/auth error:", err, flush=True)
    shutdown()

def shutdown():
    global conn
    if shutdown_event.is_set():
        return
    shutdown_event.set()
    print("🛑 Shutting down...", flush=True)
    try:
        if conn and not getattr(conn, "closed", 1):
            conn.close()
    except Exception:
        pass
    if reactor.running:
        reactor.callFromThread(reactor.stop)

# ----------------
# Wire & run
# ----------------
signal.signal(signal.SIGINT, lambda s, f: shutdown())
signal.signal(signal.SIGTERM, lambda s, f: shutdown())

client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMessage)

client.startService()
reactor.run()
