#!/usr/bin/env python3
import json
import queue
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
import pytz
from twisted.internet import reactor

from ctrader_open_api import Client, EndPoints, TcpProtocol
from datavis.broker_creds import resolve_creds_file
from datavis.ctrader_auth import CTraderTokenManager
from datavis.tickcollector_runtime import ReconnectRetryPolicy, TickCollectorController


CREDS_FILE = resolve_creds_file(Path(__file__).resolve().parent)
with open(CREDS_FILE, "r", encoding="utf-8") as f:
    CREDS = json.load(f)

CLIENT_ID = str(CREDS["clientId"]).strip()
CLIENT_SECRET = str(CREDS["clientSecret"]).strip()
ACCOUNT_ID = int(CREDS["accountId"])
ACCESS_TOKEN = str(CREDS["accessToken"]).strip()
REFRESH_TOKEN = str(CREDS["refreshToken"]).strip()
SYMBOL_ID = int(CREDS["symbolId"])
SYMBOL_NAME = str(CREDS.get("symbol", "XAUUSD")).strip() or "XAUUSD"

CONNECTION_TYPE = str(CREDS.get("connectionType", "live")).strip().lower() or "live"
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

CLIENT: Optional[Client] = None
CONTROLLER: Optional[TickCollectorController] = None


def log(message: str) -> None:
    print(str(message), flush=True)


def db_connect():
    conn = psycopg2.connect(**DB_KW)
    conn.autocommit = False
    return conn


def enqueue_tick(ts_ms: int, bid_int: int, ask_int: int) -> None:
    global PRODUCED
    TICK_QUEUE.put((int(ts_ms), int(bid_int), int(ask_int)))
    with QUEUE_LOCK:
        PRODUCED += 1


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
                        INSERT INTO public.ticks (symbol, timestamp, bid, ask, mid, spread, kal, k2)
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
                    log(f"collector stats total={inserted_total} rate={rate:.1f}/s queue={qlen} flush_ms={last_flush_ms:.2f}")
                else:
                    log(
                        f"collector stats total={inserted_total} rate={rate:.1f}/s "
                        f"queue={qlen} lag={lag_s:.3f}s flush_ms={last_flush_ms:.2f}"
                    )
                inserted_since = 0
                stats_at = now

        except Exception as exc:
            log(f"collector writer error={exc}")
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


def build_runtime() -> tuple[Client, TickCollectorController]:
    token_manager = CTraderTokenManager(
        creds_file=Path(CREDS_FILE),
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        access_token=ACCESS_TOKEN,
        refresh_token=REFRESH_TOKEN,
    )
    reconnect_policy = ReconnectRetryPolicy(log=log, initial=1.0, maximum=60.0)
    client = Client(
        host=HOST,
        port=PORT,
        protocol=TcpProtocol,
        retryPolicy=reconnect_policy,
    )
    controller = TickCollectorController(
        client=client,
        reactor_api=reactor,
        stop_event=STOP_EVENT,
        token_manager=token_manager,
        account_id=ACCOUNT_ID,
        symbol_id=SYMBOL_ID,
        connection_type=CONNECTION_TYPE,
        on_tick=enqueue_tick,
        log=log,
        reconnect_policy=reconnect_policy,
    )
    return client, controller


def shutdown() -> None:
    global CLIENT
    if STOP_EVENT.is_set():
        return
    STOP_EVENT.set()
    log("collector shutting down")
    try:
        if CLIENT is not None:
            CLIENT.stopService()
    except Exception:
        pass
    if reactor.running:
        reactor.callFromThread(reactor.stop)


def main():
    global CLIENT, CONTROLLER

    writer = threading.Thread(target=writer_loop, name="TickRawDbWriter", daemon=True)
    writer.start()

    CLIENT, CONTROLLER = build_runtime()
    CLIENT.setConnectedCallback(CONTROLLER.connected)
    CLIENT.setDisconnectedCallback(CONTROLLER.disconnected)
    CLIENT.setMessageReceivedCallback(CONTROLLER.on_message)

    signal.signal(signal.SIGINT, lambda s, f: shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: shutdown())

    CLIENT.startService()
    reactor.run()


if __name__ == "__main__":
    main()
