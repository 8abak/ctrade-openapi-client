"""Read-only PostgreSQL tick consumer for the isolated ACD paper ledger."""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from datavis.acd_continuation import Config, PaperRobot
from datavis.db import db_connection
from datavis.hlrtacd.models import Tick


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True)
    parser.add_argument("--command", choices=["run", "status", "pause", "resume", "close", "watchdog"], default="run")
    args = parser.parse_args()
    root = Path(args.directory)
    root.mkdir(parents=True, exist_ok=True)
    ledger = str(root / "paper.sqlite")
    command = root / "control.json"
    health = root / "health.json"
    if args.command == "status":
        db = sqlite3.connect(f"file:{ledger}?mode=ro", uri=True)
        print(db.execute("SELECT data FROM state WHERE id=1").fetchone()[0])
        print(health.read_text())
        db.close()
        return
    if args.command in {"pause", "resume", "close"}:
        temp = root / "control.tmp"
        temp.write_text(json.dumps(dict(command=args.command)))
        os.replace(temp, command)
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.command == "watchdog":
        age = time.time()-health.stat().st_mtime if health.exists() else float("inf")
        snapshot = json.loads(health.read_text()) if health.exists() else {}
        stamp = snapshot.get("lastQuote")
        quote_age = (datetime.now(timezone.utc)-datetime.fromisoformat(stamp)).total_seconds() if stamp else float("inf")
        if age > 30 or quote_age > 60 or quote_age < -1:
            # Do not fabricate executable prices or claim broker closure.
            temp = root / "control.tmp"
            temp.write_text('{"command":"close"}')
            os.replace(temp, command)
            logging.error("PRIMARY_UNHEALTHY heartbeat_age=%s quote_age=%s; queued paper liquidation and pause",age,quote_age)
            raise SystemExit(1)
        return
    config_file = root / "config.json"
    config = Config(**json.loads(config_file.read_text())) if config_file.exists() else Config()
    robot = PaperRobot(ledger,config)
    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    cursor = 0
    # Cold boot does not replay historic entries. Start from the latest quote;
    # the current formation must be observed before this service can enter.
    with db_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id),0) FROM public.ticks WHERE symbol=%s",("XAUUSD",))
            cursor = int(cur.fetchone()[0])
    logging.info("Paper service started; broker orders disabled; feed cursor=%s",cursor)
    try:
        while running:
            control = json.loads(command.read_text()) if command.exists() else {}
            with db_connection(readonly=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id,timestamp,bid,ask FROM public.ticks WHERE symbol=%s AND id>%s ORDER BY id LIMIT 20000",("XAUUSD",cursor))
                    rows = cur.fetchall()
            now = datetime.now(timezone.utc)
            processed = 0
            for row in rows:
                cursor = int(row[0])
                stamp, bid, ask = row[1],float(row[2]),float(row[3])
                # Persist controls atomically with the next fresh executable quote.
                if control.get("command") in {"pause","close"}:
                    robot.s["paused"] = True
                elif control.get("command") == "resume":
                    robot.s["paused"] = False
                tick = Tick(cursor,stamp,bid,ask,(bid+ask)/2)
                if control.get("command") == "close" and robot.s["position"]:
                    robot.s["position"]["deadline"] = stamp.isoformat()
                previous = robot.s["last"]
                robot.process(tick,now=now)
                processed += previous != robot.s["last"]
            if processed and control:
                command.unlink(missing_ok=True)
            payload = dict(mode="paper", brokerOrdersEnabled=False, timestamp=now.isoformat(), lastQuote=robot.s["last"], processed=processed, position=robot.s["position"], latched=robot.s["latched"], paused=robot.s["paused"], controlPending=bool(control))
            temp = root / "health.tmp"
            temp.write_text(json.dumps(payload))
            os.replace(temp,health)
            if processed:
                logging.info("quotes=%s equity=%.2f position=%s latched=%s",processed,robot.s.get("equity",500), bool(robot.s["position"]),robot.s["latched"])
            time.sleep(1)
    finally:
        robot.close()


if __name__ == "__main__":
    main()
