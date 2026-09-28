"""Read-only XAUUSD tick watcher. No broker or order execution dependencies.

The small state machine emits a candidate only after a range break holds and
the first retest rejects the old boundary. Times refer to market tick times.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Optional


@dataclass
class WatchEngine:
    symbol: str = "XAUUSD"
    window_seconds: float = 300
    min_history_seconds: float = 120
    accept_seconds: float = 10
    retest_seconds: float = 90
    cooldown_seconds: float = 300
    max_spread: float = 1.0
    history: Deque[tuple[float, float]] = field(default_factory=deque)
    pending: Optional[Dict[str, Any]] = None
    last_event_at: float = float("-inf")
    last_time: float = float("-inf")

    def process(self, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ts = tick["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = ts.timestamp()
        bid, ask = float(tick["bid"]), float(tick["ask"])
        if not all(map(math.isfinite, (now, bid, ask))) or bid <= 0 or ask < bid:
            return None
        if now < self.last_time:
            return None
        if now - self.last_time > self.window_seconds:
            self.history.clear()
            self.pending = None
        self.last_time = now
        mid, spread = (bid + ask) / 2, ask - bid
        while self.history and self.history[0][0] < now - self.window_seconds:
            self.history.popleft()
        if spread > self.max_spread:
            self.pending = None
            return None

        event = None
        if self.pending:
            p = self.pending
            boundary, direction = p["boundary"], p["direction"]
            margin = p["margin"]
            distance = direction * (mid - boundary)
            if distance < -margin or now - p["started"] > self.retest_seconds:
                self.pending = None
            elif now - p["started"] >= self.accept_seconds:
                p["accepted"] = True
                if abs(mid - boundary) <= margin:
                    p["retested"] = True
                elif p["retested"] and distance >= 2 * margin:
                    event = {
                        "eventId": f"{self.symbol}:{tick['id']}:{'up' if direction > 0 else 'down'}",
                        "type": "BREAK_RETEST_CONFIRMED",
                        "direction": "up" if direction > 0 else "down",
                        "symbol": self.symbol,
                        "tickId": int(tick["id"]),
                        "timestamp": ts.isoformat(),
                        "price": round(mid, 3),
                        "rangeLow": round(p["low"], 3),
                        "rangeHigh": round(p["high"], 3),
                        "boundary": round(boundary, 3),
                        "invalidation": round(boundary - direction * margin, 3),
                        "spread": round(spread, 3),
                        "evidence": ["prior five-minute range", "break held", "boundary retested", "rejection"],
                        "status": "unreviewed",
                    }
                    self.last_event_at = now
                    self.pending = None

        if self.pending is None and event is None and now - self.last_event_at >= self.cooldown_seconds:
            if self.history and now - self.history[0][0] >= self.min_history_seconds:
                prices = [price for _, price in self.history]
                low, high = min(prices), max(prices)
                width = high - low
                margin = max(0.15, 1.5 * spread)
                if 0.5 <= width <= 30:
                    direction = 1 if mid > high + margin else -1 if mid < low - margin else 0
                    if direction:
                        self.pending = {
                            "direction": direction, "boundary": high if direction > 0 else low,
                            "low": low, "high": high, "margin": margin,
                            "started": now, "accepted": False, "retested": False,
                        }
        self.history.append((now, mid))
        return event


def write_snapshot(path: Path, snapshot: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)


def run() -> None:
    import psycopg2
    import psycopg2.extras

    parser = argparse.ArgumentParser(description="Read-only structural event watcher")
    parser.add_argument("--state", type=Path, default=Path("logs/structure_watch.json"))
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--symbol", default="XAUUSD")
    args = parser.parse_args()
    if args.poll_seconds < 0.1:
        parser.error("poll-seconds must be at least 0.1")
    database_url = os.getenv("DATABASE_URL", "").replace("postgresql+psycopg2://", "postgresql://", 1)
    if not database_url:
        parser.error("DATABASE_URL is required; use a read-only PostgreSQL role")
    engine = WatchEngine(symbol=args.symbol)
    cursor_id = 0
    if args.state.exists():
        try:
            cursor_id = int(json.loads(args.state.read_text(encoding="utf-8")).get("lastTickId", 0))
        except (ValueError, OSError):
            pass
    while True:
        try:
            with psycopg2.connect(database_url, options="-c statement_timeout=3000") as conn:
                conn.set_session(readonly=True)
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if not cursor_id:
                        cur.execute("SELECT id FROM public.ticks WHERE symbol=%s ORDER BY id DESC LIMIT 1", (args.symbol,))
                        row = cur.fetchone()
                        cursor_id = int(row["id"]) if row else 0
                    while True:
                        cur.execute(
                            "SELECT id, timestamp, bid, ask FROM public.ticks "
                            "WHERE symbol=%s AND id>%s ORDER BY id LIMIT 1000",
                            (args.symbol, cursor_id),
                        )
                        rows = cur.fetchall()
                        if not rows:
                            time.sleep(args.poll_seconds)
                            continue
                        for row in rows:
                            cursor_id = int(row["id"])
                            tick_time = row["timestamp"]
                            if tick_time.tzinfo is None:
                                tick_time = tick_time.replace(tzinfo=timezone.utc)
                            age_seconds = (datetime.now(timezone.utc) - tick_time).total_seconds()
                            if age_seconds > 30:
                                engine = WatchEngine(symbol=args.symbol)
                                event = None
                            else:
                                event = engine.process(row)
                            snapshot = {
                                "symbol": args.symbol, "lastTickId": cursor_id,
                                "lastTickTime": row["timestamp"].isoformat(),
                                "pending": engine.pending, "recentEvent": event,
                            }
                            if event:
                                args.state.parent.mkdir(parents=True, exist_ok=True)
                                with args.state.with_suffix(".jsonl").open("a", encoding="utf-8") as output:
                                    output.write(json.dumps(event) + "\n")
                                print(json.dumps(event), flush=True)
                        args.state.parent.mkdir(parents=True, exist_ok=True)
                        write_snapshot(args.state, snapshot)
        except (psycopg2.Error, OSError) as exc:
            print(f"watch reconnecting: {type(exc).__name__}: {exc}", flush=True)
            engine = WatchEngine(symbol=args.symbol)
            time.sleep(5)


if __name__ == "__main__":
    run()
