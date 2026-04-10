#!/usr/bin/env python3
import json
import os
import queue
import signal
import threading
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import pytz
import requests
from twisted.internet import reactor

from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthReq,
    ProtoOAApplicationAuthReq,
    ProtoOASpotEvent,
    ProtoOASubscribeSpotsReq,
)
from datavis.broker_creds import locked_creds_file, read_creds_file, resolve_creds_file, token_tail, write_creds_file_atomic


CREDS_FILE = str(resolve_creds_file(Path(__file__).resolve().parent))
with open(CREDS_FILE, "r", encoding="utf-8") as f:
    CREDS = json.load(f)

CLIENT_ID = CREDS["clientId"]
CLIENT_SECRET = CREDS["clientSecret"]
ACCOUNT_ID = CREDS["accountId"]
ACCESS_TOKEN = CREDS["accessToken"]
REFRESH_TOKEN = CREDS["refreshToken"]
SYMBOL_ID = CREDS["symbolId"]
SYMBOL_NAME = CREDS.get("symbol", "XAUUSD")

CONNECTION_TYPE = CREDS.get("connectionType", "live").lower()
HOST = EndPoints.PROTOBUF_LIVE_HOST if CONNECTION_TYPE == "live" else EndPoints.PROTOBUF_DEMO_HOST
PORT = EndPoints.PROTOBUF_PORT

DB_KW = dict(dbname="trading", user="babak", password="babak33044", host="localhost", port=5432)
SYDNEY_TZ = pytz.timezone("Australia/Sydney")

STOP_EVENT = threading.Event()
TICK_QUEUE: "queue.SimpleQueue[tuple[int, int, int]]" = queue.SimpleQueue()

QUEUE_LOCK = threading.Lock()
PRODUCED = 0
CONSUMED = 0
LAST_VALID_BID_INT = 0
LAST_VALID_ASK_INT = 0
RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 30.0
CLIENT_STARTED = False


def sync_tokens_from_disk(context):
    global ACCESS_TOKEN, REFRESH_TOKEN, CREDS
    try:
        latest = read_creds_file(Path(CREDS_FILE))
    except Exception as exc:
        print(f"token reload failed context={context} error={exc}", flush=True)
        return False
    updated = False
    access_token = str(latest.get("accessToken", "")).strip()
    refresh_token = str(latest.get("refreshToken", "")).strip()
    if access_token and access_token != ACCESS_TOKEN:
        ACCESS_TOKEN = access_token
        CREDS["accessToken"] = ACCESS_TOKEN
        updated = True
    if refresh_token and refresh_token != REFRESH_TOKEN:
        REFRESH_TOKEN = refresh_token
        CREDS["refreshToken"] = REFRESH_TOKEN
        updated = True
    if updated:
        print(
            f"token reload updated context={context} access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)}",
            flush=True,
        )
    return updated


def db_connect():
    conn = psycopg2.connect(**DB_KW)
    conn.autocommit = False
    return conn


def refresh_tokens():
    global ACCESS_TOKEN, REFRESH_TOKEN, CREDS
    print(
        f"refreshing cTrader token access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)}",
        flush=True,
    )
    try:
        with locked_creds_file(Path(CREDS_FILE)):
            disk_access_before = ACCESS_TOKEN
            if sync_tokens_from_disk("pre_refresh_lock") and ACCESS_TOKEN != disk_access_before:
                print(
                    f"token refresh reused rotated access token from disk access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)}",
                    flush=True,
                )
                return True

            resp = requests.post(
                "https://openapi.ctrader.com/apps/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": REFRESH_TOKEN,
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                },
                timeout=20,
            )
            if resp.status_code != 200:
                print(
                    f"token refresh failed status={resp.status_code} access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)}",
                    flush=True,
                )
                return False

            tokens = resp.json()
            access_token = str(tokens.get("access_token", "")).strip()
            if not access_token:
                print(
                    f"token refresh failed missing access token access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)}",
                    flush=True,
                )
                return False
            ACCESS_TOKEN = access_token
            refreshed = str(tokens.get("refresh_token", "")).strip()
            if refreshed:
                REFRESH_TOKEN = refreshed

            CREDS["accessToken"] = ACCESS_TOKEN
            CREDS["refreshToken"] = REFRESH_TOKEN
            CREDS["tokenType"] = tokens.get("token_type", "Bearer")
            write_creds_file_atomic(Path(CREDS_FILE), CREDS)
    except Exception as exc:
        print(
            f"token refresh exception access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)} error={exc}",
            flush=True,
        )
        return False

    print(
        f"token refresh succeeded persisted=true access_tail={token_tail(ACCESS_TOKEN)} refresh_tail={token_tail(REFRESH_TOKEN)}",
        flush=True,
    )
    return True


def writer_loop():
    global CONSUMED, LAST_VALID_BID_INT, LAST_VALID_ASK_INT

    conn = None
    inserted_total = 0
    inserted_since = 0
    stats_at = time.time()
    max_ts = None
    last_flush_ms = 0.0

    batch_size = 200
    flush_interval = 0.02

    while not STOP_EVENT.is_set():
        try:
            if conn is None or conn.closed:
                conn = db_connect()

            batch = []
            started = time.time()
            while len(batch) < batch_size:
                timeout = max(0.0, flush_interval - (time.time() - started))
                if timeout <= 0.0 and batch:
                    break
                try:
                    item = TICK_QUEUE.get(timeout=timeout if timeout > 0 else 0.0)
                    batch.append(item)
                except queue.Empty:
                    break

            if not batch:
                continue

            rows = []
            for ts_ms, bid_int, ask_int in batch:
                if bid_int == 0 and LAST_VALID_BID_INT:
                    bid_int = LAST_VALID_BID_INT
                elif bid_int:
                    LAST_VALID_BID_INT = bid_int

                if ask_int == 0 and LAST_VALID_ASK_INT:
                    ask_int = LAST_VALID_ASK_INT
                elif ask_int:
                    LAST_VALID_ASK_INT = ask_int

                if bid_int == 0 or ask_int == 0:
                    continue

                utc_dt = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=pytz.utc)
                ts = utc_dt.astimezone(SYDNEY_TZ)
                bid = bid_int / 100000.0
                ask = ask_int / 100000.0
                mid = round((bid + ask) / 2.0, 2)
                spread = round(ask - bid, 2)

                rows.append((SYMBOL_NAME, ts, bid, ask, mid, spread, None, None))
                if max_ts is None or ts > max_ts:
                    max_ts = ts

            if rows:
                flush_started = time.time()
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO ticks (symbol, timestamp, bid, ask, mid, spread, kal, k2)
                        VALUES %s
                        """,
                        rows,
                        page_size=min(1000, len(rows)),
                    )
                    inserted = len(rows)
                conn.commit()
                last_flush_ms = (time.time() - flush_started) * 1000.0
                inserted_total += inserted
                inserted_since += inserted

            with QUEUE_LOCK:
                CONSUMED += len(batch)

            now = time.time()
            if now - stats_at >= 5.0:
                elapsed = now - stats_at
                rate = inserted_since / elapsed if elapsed > 0 else 0.0
                with QUEUE_LOCK:
                    qlen = PRODUCED - CONSUMED
                lag_s = None
                if max_ts is not None:
                    lag_s = (datetime.now(tz=SYDNEY_TZ) - max_ts).total_seconds()
                if lag_s is None:
                    print(
                        f"collector stats total={inserted_total} rate={rate:.1f}/s queue={qlen} flush_ms={last_flush_ms:.2f}",
                        flush=True,
                    )
                else:
                    print(
                        f"collector stats total={inserted_total} rate={rate:.1f}/s queue={qlen} lag={lag_s:.3f}s flush_ms={last_flush_ms:.2f}",
                        flush=True,
                    )
                inserted_since = 0
                stats_at = now

        except Exception as e:
            print(f"collector writer error: {e}", flush=True)
            try:
                if conn and not conn.closed:
                    conn.rollback()
            except Exception:
                pass
            try:
                if conn and not conn.closed:
                    conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(1.0)

    try:
        if conn and not conn.closed:
            conn.close()
    except Exception:
        pass


CLIENT = Client(host=HOST, port=PORT, protocol=TcpProtocol)


def schedule_reconnect():
    global RECONNECT_DELAY
    if STOP_EVENT.is_set():
        return
    delay = RECONNECT_DELAY
    RECONNECT_DELAY = min(MAX_RECONNECT_DELAY, RECONNECT_DELAY * 2.0)
    print(f"reconnecting in {delay:.1f}s", flush=True)
    reactor.callLater(delay, start_client)


def start_client():
    global CLIENT_STARTED
    if STOP_EVENT.is_set():
        return
    if CLIENT_STARTED:
        return
    CLIENT_STARTED = True
    try:
        CLIENT.startService()
    except Exception as e:
        CLIENT_STARTED = False
        print(f"startService failed: {e}", flush=True)
        schedule_reconnect()


def connected(_):
    global RECONNECT_DELAY
    RECONNECT_DELAY = 1.0
    print("connected, authorizing", flush=True)
    auth_msg = ProtoOAApplicationAuthReq()
    auth_msg.clientId = CLIENT_ID
    auth_msg.clientSecret = CLIENT_SECRET
    d = CLIENT.send(auth_msg)

    def after_app_auth(_):
        account_auth = ProtoOAAccountAuthReq()
        account_auth.ctidTraderAccountId = ACCOUNT_ID
        account_auth.accessToken = ACCESS_TOKEN
        return CLIENT.send(account_auth)

    def after_account_auth(_):
        print(f"authorized account={ACCOUNT_ID}, subscribing symbolId={SYMBOL_ID}", flush=True)
        subscribe_to_spot()

    d.addCallback(after_app_auth)
    d.addCallback(after_account_auth)
    d.addErrback(on_error)


def subscribe_to_spot():
    req = ProtoOASubscribeSpotsReq()
    req.ctidTraderAccountId = ACCOUNT_ID
    req.symbolId.append(SYMBOL_ID)
    req.subscribeToSpotTimestamp = True
    CLIENT.send(req)


def disconnected(_, reason):
    global CLIENT_STARTED
    CLIENT_STARTED = False
    if STOP_EVENT.is_set():
        return
    print(f"disconnected: {reason}", flush=True)
    schedule_reconnect()


def on_message(_, message):
    global PRODUCED
    if message.payloadType == ProtoOASpotEvent().payloadType:
        try:
            spot = Protobuf.extract(message)
            ts = int(spot.timestamp)
            bid = int(getattr(spot, "bid", 0))
            ask = int(getattr(spot, "ask", 0))
            TICK_QUEUE.put((ts, bid, ask))
            with QUEUE_LOCK:
                PRODUCED += 1
        except Exception as e:
            print(f"spot parse error: {e}", flush=True)

    if message.payloadType == 2142:
        try:
            error = Protobuf.extract(message)
            print(f"ctrader error code={error.errorCode} desc={error.description}", flush=True)
            if error.errorCode in ("CH_ACCESS_TOKEN_INVALID", "INVALID_REQUEST"):
                if refresh_tokens():
                    reactor.callLater(2.0, connected, None)
        except Exception as e:
            print(f"error parse failed: {e}", flush=True)


def on_error(err):
    global CLIENT_STARTED
    CLIENT_STARTED = False
    if STOP_EVENT.is_set():
        return
    print(f"connection/auth error: {err}", flush=True)
    schedule_reconnect()


def shutdown():
    if STOP_EVENT.is_set():
        return
    STOP_EVENT.set()
    print("collector shutting down", flush=True)
    try:
        CLIENT.stopService()
    except Exception:
        pass
    if reactor.running:
        reactor.callFromThread(reactor.stop)


def main():
    t = threading.Thread(target=writer_loop, name="TickRawDbWriter", daemon=True)
    t.start()

    CLIENT.setConnectedCallback(connected)
    CLIENT.setDisconnectedCallback(disconnected)
    CLIENT.setMessageReceivedCallback(on_message)

    signal.signal(signal.SIGINT, lambda s, f: shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: shutdown())

    start_client()
    reactor.run()


if __name__ == "__main__":
    main()
