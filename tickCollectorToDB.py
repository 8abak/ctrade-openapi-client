#!/usr/bin/env python3
import json
import os
import signal
import time
import threading
from collections import deque
from datetime import datetime

import pytz
import psycopg2
import psycopg2.extras
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

def DbConnect():
    conn = psycopg2.connect(**DB_KW)
    conn.autocommit = False
    return conn

# --------------------------------------------
# Simple 1D Kalman filter
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
        self.x = None
        self.P = None

    def Step(self, z: float) -> float:
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


# mid -> kal
KalFilter = ScalarKalmanFilter(process_var=1e-4, meas_var=1e-2, init_var=1.0)
# kal -> k2 (same idea as buildK2.py: Q=1e-5, R=5e-3)
K2Filter = ScalarKalmanFilter(process_var=1e-5, meas_var=5e-3, init_var=1.0)
K2Primed = False

# --------------------------------------------
# Ingest queue (fast path)
# --------------------------------------------
QueueLock = threading.Lock()
TickQueue = deque()  # each item: (timestamp_ms, bid_int, ask_int)
MaxQueue = 200000    # safety cap (large). If you ever hit this, machine is falling behind.

LastValidBid = None
LastValidAsk = None

StopEvent = threading.Event()

SydneyTz = pytz.timezone("Australia/Sydney")

# --------------------------------------------
# Token refresh logic
# --------------------------------------------
def RefreshTokens():
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
        timeout=20,
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

# --------------------------------------------
# Prime K2 from DB (best effort)
# --------------------------------------------
def PrimeK2FromDb(conn):
    global K2Primed
    if K2Primed:
        return

    try:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT k2, kal
                FROM ticks
                WHERE symbol = %s
                ORDER BY timestamp DESC, id DESC
                LIMIT 1
                """,
                ("XAUUSD",),
            )
            row = c.fetchone()

        if row:
            last_k2, last_kal = row[0], row[1]
            if last_k2 is not None:
                K2Filter.x = float(last_k2)
                K2Filter.P = 1.0
            elif last_kal is not None:
                K2Filter.x = float(last_kal)
                K2Filter.P = 1.0

        K2Primed = True
    except Exception as e:
        print(f"⚠️ K2 prime skipped: {e}", flush=True)
        K2Primed = True

# --------------------------------------------
# Writer loop: drain queue -> compute -> batch insert
# --------------------------------------------
def WriterLoop():
    conn = None
    cur = None

    InsertedTotal = 0
    InsertedSince = 0
    LastStatTime = time.time()

    BatchSize = 250   # tune: 200-1000 for t2.micro
    FlushInterval = 0.10  # seconds (also flushes even if queue is small)

    NextFlush = time.time() + FlushInterval

    while not StopEvent.is_set():
        try:
            if conn is None or getattr(conn, "closed", 1):
                conn = DbConnect()
                cur = conn.cursor()
                PrimeK2FromDb(conn)

            # Drain a batch from queue
            batch = []
            with QueueLock:
                while TickQueue and len(batch) < BatchSize:
                    batch.append(TickQueue.popleft())

                # safety: if queue is exploding, drain more aggressively next cycles
                qlen = len(TickQueue)

            now = time.time()
            should_flush = (batch and (now >= NextFlush)) or (len(batch) >= BatchSize)

            if not should_flush:
                # sleep a tiny bit to yield CPU, but keep it responsive
                time.sleep(0.01)
                continue

            NextFlush = now + FlushInterval

            rows = []
            max_ts_in_batch = None

            for (ts_ms, bid_int, ask_int) in batch:
                # forward-fill bid/ask if zero
                global LastValidBid, LastValidAsk
                if bid_int == 0 and LastValidBid is not None:
                    bid_int = LastValidBid
                elif bid_int != 0:
                    LastValidBid = bid_int

                if ask_int == 0 and LastValidAsk is not None:
                    ask_int = LastValidAsk
                elif ask_int != 0:
                    LastValidAsk = ask_int

                if bid_int == 0 or ask_int == 0:
                    continue

                utc_dt = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=pytz.utc)
                syd_dt = utc_dt.astimezone(SydneyTz)

                bid = bid_int / 100000.0
                ask = ask_int / 100000.0

                mid = round((bid + ask) / 2.0, 2)
                spread = round(ask - bid, 2)

                kal = round(KalFilter.Step(mid), 2)
                k2 = round(K2Filter.Step(kal), 2)

                rows.append(("XAUUSD", syd_dt, bid, ask, mid, spread, kal, k2))
                max_ts_in_batch = syd_dt if max_ts_in_batch is None else max(max_ts_in_batch, syd_dt)

            if rows:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO ticks (symbol, timestamp, bid, ask, mid, spread, kal, k2)
                    VALUES %s
                    """,
                    rows,
                    page_size=min(1000, len(rows)),
                )
                conn.commit()

                InsertedTotal += len(rows)
                InsertedSince += len(rows)

            # Stats every 5 seconds
            stat_now = time.time()
            if stat_now - LastStatTime >= 5.0:
                dt = stat_now - LastStatTime
                ips = InsertedSince / dt if dt > 0 else 0.0
                lag_s = None
                if max_ts_in_batch is not None:
                    lag_s = (datetime.now(tz=SydneyTz) - max_ts_in_batch).total_seconds()

                if lag_s is None:
                    print(f"📈 ticks: total={InsertedTotal} rate={ips:.1f}/s queue={qlen}", flush=True)
                else:
                    print(f"📈 ticks: total={InsertedTotal} rate={ips:.1f}/s queue={qlen} lag={lag_s:.3f}s", flush=True)

                InsertedSince = 0
                LastStatTime = stat_now

            # If queue is growing too much, yield less sleep
            if qlen > MaxQueue:
                print(f"🚨 WARNING: queue overflow risk: {qlen} > {MaxQueue}. This instance cannot keep up.", flush=True)

        except Exception as e:
            # Any DB or runtime error: rollback & reconnect
            try:
                if conn and not getattr(conn, "closed", 1):
                    conn.rollback()
            except Exception:
                pass
            print(f"❌ WriterLoop error: {e}", flush=True)
            time.sleep(1.0)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None
            cur = None

    # clean shutdown
    try:
        if conn and not getattr(conn, "closed", 1):
            conn.close()
    except Exception:
        pass

# --------------------------------------------
# cTrader Open API client
# --------------------------------------------
client = Client(host=host, port=port, protocol=TcpProtocol)

def connected(_):
    print("✅ Connected. Authorizing...", flush=True)
    authMsg = ProtoOAApplicationAuthReq()
    authMsg.clientId = clientId
    authMsg.clientSecret = clientSecret
    d = client.send(authMsg)

    def afterAppAuth(_):
        print("🎉 Application authorized", flush=True)
        accountAuth = ProtoOAAccountAuthReq()
        accountAuth.ctidTraderAccountId = accountId
        accountAuth.accessToken = accessToken
        return client.send(accountAuth)

    def afterAccountAuth(_):
        print(f"🔐 Account {accountId} authorized. Subscribing spots...", flush=True)
        subscribeToSpot()

    d.addCallback(afterAppAuth)
    d.addCallback(afterAccountAuth)
    d.addErrback(onError)

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
    # Spot ticks
    if message.payloadType == ProtoOASpotEvent().payloadType:
        try:
            spot = Protobuf.extract(message)
            ts = int(spot.timestamp)
            bid = int(getattr(spot, "bid", 0))
            ask = int(getattr(spot, "ask", 0))

            # Fast enqueue only (do not block reactor)
            with QueueLock:
                TickQueue.append((ts, bid, ask))
                # Hard cap: if we ever exceed, we must drop oldest or stop.
                # We choose to drop oldest to keep liveness. You asked for "no miss":
                # if you ever see this, you must upgrade instance or add spill-to-disk.
                if len(TickQueue) > MaxQueue:
                    TickQueue.popleft()

        except Exception as e:
            print(f"⚠️ Spot parse/enqueue error: {e}", flush=True)

    # ProtoOAErrorRes is commonly 2142
    if message.payloadType == 2142:
        try:
            error = Protobuf.extract(message)
            print("❌ cTrader Error:", error.errorCode, error.description, flush=True)

            if error.errorCode in ("CH_ACCESS_TOKEN_INVALID", "INVALID_REQUEST"):
                if RefreshTokens():
                    print("🔄 Token refreshed. Re-auth in 2s...", flush=True)
                    reactor.callLater(2, connected, None)

        except Exception as e:
            print(f"⚠️ Failed to parse error message: {e}", flush=True)

def onError(err):
    print("❌ Connection/auth error:", err, flush=True)
    shutdown()

def shutdown():
    if StopEvent.is_set():
        return
    StopEvent.set()
    print("🛑 Shutting down...", flush=True)
    try:
        client.stopService()
    except Exception:
        pass
    if reactor.running:
        reactor.callFromThread(reactor.stop)

# ----------------
# Main
# ----------------
def main():
    # Start writer thread
    t = threading.Thread(target=WriterLoop, name="DbWriter", daemon=True)
    t.start()

    # Wire client
    client.setConnectedCallback(connected)
    client.setDisconnectedCallback(disconnected)
    client.setMessageReceivedCallback(onMessage)

    # Signals
    signal.signal(signal.SIGINT, lambda s, f: shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: shutdown())

    client.startService()
    reactor.run()

if __name__ == "__main__":
    main()