"""Candidate continuation strategy. Paper execution only; no broker order path.

The deliberately separate paper ledger must never be used as broker equity.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from datavis.hlrtacd.acd import CausalAcdEngine, acd_session_for_timestamp
from datavis.hlrtacd.models import Tick


@dataclass(frozen=True)
class Config:
    mode: str = "paper"
    a_offset: float = .75
    c_offset: float = 1.
    hold_seconds: float = 450.
    entry_minutes: float = 120.
    late_hours: float = 3.
    opposite_buffer: float = .25
    trailing_activation: float = 3.
    trailing_reference: str = "entry"
    trend_threshold: float = 3.
    quote_gap_seconds: float = 60.
    liquidation_buffer: float = 5.
    cost_per_trade: float = .14
    slippage: float = .10
    delay_reserve: float = 5.
    # Hypothetical paper contract only, NOT broker verification.
    ounces_per_lot: float = 100.
    swap_per_day: float = 0.

    def __post_init__(self):
        if self.mode != "paper":
            raise ValueError("Live/demo orders blocked: unresolved mandate and unverified broker isolation")
        for key, value in asdict(self).items():
            if isinstance(value, (int, float)) and (not math.isfinite(value) or value < 0):
                raise ValueError(f"Invalid {key}")
        if self.hold_seconds <= 0 or self.quote_gap_seconds <= 0 or self.ounces_per_lot <= 0:
            raise ValueError("Time and contract values must be positive")
        if self.liquidation_buffer >= 50 or self.trailing_reference not in {"entry", "edge"}:
            raise ValueError("Invalid risk/trailing setting")


class PaperRobot:
    """Single-writer transactional paper state; quote history drives the existing ACD.

    A crash rolls back the entire quote, including allowance, fills and exits.
    Restart requires a new inside-edge observation before a breakout can qualify.
    """
    def __init__(self, path: str, config: Config = Config()):
        self.config = config
        self.db = sqlite3.connect(path, timeout=0, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), data TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, timestamp TEXT, kind TEXT, data TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS owner (id INTEGER PRIMARY KEY CHECK(id=1))")
        # OS file lock held for process lifetime; a second process fails closed.
        self.lock = open(path + ".lock", "a+b")
        self.lock.seek(0)
        if __import__("os").name == "nt":
            import msvcrt
            self.lock.seek(0, 2)
            if self.lock.tell() == 0:
                self.lock.write(b"0")
                self.lock.flush()
            self.lock.seek(0)
            try:
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                self.lock.close()
                self.db.close()
                raise
        else:
            import fcntl
            try:
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                self.lock.close()
                self.db.close()
                raise
        row = self.db.execute("SELECT data FROM state WHERE id=1").fetchone()
        self.s = json.loads(row[0]) if row else dict(realized=0., latched=False, used=[], position=None, last=None, session=None, history=[], previous_move="uncertain", paused=False)
        if self.s.get("config", asdict(config)) != asdict(config):
            self.close()
            raise ValueError("Configuration changed: use a separate paper ledger")
        self.s["config"] = asdict(config)
        self.acd = None
        self.armed = {"up": False, "down": False}
        self.since = {"up": None, "down": None, "opposite": None}
        self.previous = None

    def close(self):
        self.db.close()
        self.lock.close()

    def event(self, tick, kind, data):
        self.db.execute("INSERT INTO events(timestamp,kind,data) VALUES(?,?,?)", (tick.timestamp.isoformat(), kind, json.dumps(data)))

    def _restore(self, session):
        self.acd = CausalAcdEngine(session, a_offset=self.config.a_offset, c_offset=self.config.c_offset, acceptance_minutes=self.config.hold_seconds / 60, maximum_quote_gap_seconds=self.config.quote_gap_seconds)
        for row in self.s["history"]:
            self.acd.process_tick(Tick(row[0], datetime.fromisoformat(row[1]), row[2], row[3], (row[2]+row[3])/2))

    def process(self, tick: Tick, *, now: datetime | None = None, tradable_until: datetime | None = None):
        if not all(math.isfinite(v) and v > 0 for v in (tick.bid, tick.ask)):
            raise ValueError("Invalid executable quote")
        if self.s["last"] and tick.timestamp <= datetime.fromisoformat(self.s["last"]):
            return
        if now is not None and not 0 <= (now-tick.timestamp).total_seconds() <= self.config.quote_gap_seconds:
            self.armed = {"up": False, "down": False}
            self.since = dict(up=None, down=None, opposite=None)
            return
        self.db.execute("BEGIN IMMEDIATE")
        backup = json.dumps(self.s)
        try:
            self._process(tick, tradable_until)
            self.s["last"] = tick.timestamp.isoformat()
            self.db.execute("INSERT OR REPLACE INTO state VALUES(1,?)", (json.dumps(self.s),))
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            self.s = json.loads(backup)
            self.acd = None
            self.previous = None
            self.armed = dict(up=False, down=False)
            self.since = dict(up=None, down=None, opposite=None)
            raise

    def _process(self, t, tradable_until):
        c, s = self.config, self.s
        session = acd_session_for_timestamp(t.timestamp)
        key = session.session_date.isoformat()
        gap = self.previous is None or (t.timestamp-self.previous).total_seconds() > c.quote_gap_seconds
        self.previous = t.timestamp
        if gap:
            self.armed = dict(up=False, down=False)
            self.since = dict(up=None, down=None, opposite=None)
        if key != s["session"]:
            history = s["history"]
            move = history[-1][2] - history[0][3] if history else 0
            if not history or s.get("formation_gap",True) or not s.get("session_end") or (datetime.fromisoformat(s["session_end"])-datetime.fromisoformat(history[-1][1])).total_seconds() > c.quote_gap_seconds:
                move = 0
            s["previous_move"] = "up" if move > c.trend_threshold else "down" if move < -c.trend_threshold else "uncertain"
            s["session"], s["history"] = key, []
            s["session_end"] = session.end.isoformat()
            s["formation_gap"] = False
            self.acd = None
            self.armed = dict(up=False, down=False)
            self.since = dict(up=None, down=None, opposite=None)
        if self.acd is None:
            self._restore(session)
        self.acd.process_tick(t)
        # Retain formation quotes and the latest quote; sufficient to rebuild levels
        # and define the candidate completed-session move without lookahead.
        row = [t.tick_id, t.timestamp.isoformat(), t.bid, t.ask]
        if t.timestamp < session.opening_end:
            if s["history"] and (t.timestamp-datetime.fromisoformat(s["history"][-1][1])).total_seconds() > c.quote_gap_seconds:
                s["formation_gap"] = True
            s["history"].append(row)
            # The ACD engine needs only the mid extrema for exact OR rebuilding.
            # Keep first/last for coverage and previous-move classification.
            history = s["history"]
            selected = [history[0], min(history,key=lambda r:r[2]+r[3]), max(history,key=lambda r:r[2]+r[3]),history[-1]]
            s["history"] = sorted({r[0]:r for r in selected}.values(),key=lambda r:r[1])
        elif not s["history"] or datetime.fromisoformat(s["history"][-1][1]) < session.opening_end:
            s["history"].append(row)
        else:
            s["history"][-1] = row
        p = s["position"]
        units = .01 * c.ounces_per_lot
        quote = t.bid if not p or p["side"] == "up" else t.ask
        pnl = 0. if not p else (quote-p["entry"]) * p["sign"] * units - c.cost_per_trade - c.swap_per_day * max(0., (t.timestamp-datetime.fromisoformat(p["opened"])).total_seconds()/86400)
        equity = 500 + s["realized"] + pnl
        s["equity"] = equity
        if equity <= 450 + c.liquidation_buffer:
            s["latched"] = True
        if p:
            reason = "capital" if s["latched"] else None
            deadline = datetime.fromisoformat(p["deadline"])
            if tradable_until:
                deadline = min(deadline, tradable_until-timedelta(minutes=5))
            if t.timestamp >= deadline:
                reason = reason or "timed"
            p["extreme"] = max(p["extreme"], quote) if p["sign"] == 1 else min(p["extreme"], quote)
            reference = p["entry"] if c.trailing_reference == "entry" else p["edge"]
            if (p["extreme"]-reference)*p["sign"] > c.trailing_activation:
                candidate = p["edge"] + .5*(p["extreme"]-p["edge"])
                p["trail"] = candidate if p["trail"] is None else (max(p["trail"], candidate) if p["sign"] == 1 else min(p["trail"], candidate))
            if p["trail"] is not None and (quote-p["trail"])*p["sign"] <= 0:
                reason = reason or "trailing"
            beyond = (quote-p["opposite"])*p["sign"] < 0
            self.since["opposite"] = (self.since["opposite"] or t.timestamp) if beyond else None
            if self.since["opposite"] and (t.timestamp-self.since["opposite"]).total_seconds() >= c.hold_seconds:
                reason = reason or "opposite"
            if reason:
                net = pnl-c.slippage*units
                s["realized"] += net
                s["equity"] = 500+s["realized"]
                s["latched"] = s["latched"] or s["equity"] <= 450+c.liquidation_buffer
                self.event(t, "CLOSE", dict(reason=reason, net=net, position=p))
                s["position"] = None
            return
        levels = self.acd.levels
        if not levels or s["latched"] or s["paused"] or key in s["used"]:
            return
        formation = [r for r in s["history"] if datetime.fromisoformat(r[1]) < session.opening_end]
        # Missing opening coverage is a data failure, not a smaller valid range.
        if not formation or (datetime.fromisoformat(formation[0][1])-session.start).total_seconds() > c.quote_gap_seconds:
            return
        if (session.opening_end-datetime.fromisoformat(formation[-1][1])).total_seconds() > c.quote_gap_seconds:
            return
        if s.get("formation_gap",True):
            return
        cutoff = min(session.opening_end+timedelta(minutes=c.entry_minutes), session.end-timedelta(hours=c.late_hours))
        if not session.opening_end <= t.timestamp < cutoff:
            return
        for side, price, edge in (("up",t.bid,levels["aUp"]),("down",t.ask,levels["aDown"])):
            sign = 1 if side == "up" else -1
            if (price-edge)*sign <= 0:
                self.armed[side] = True
                self.since[side] = None
                continue
            if not self.armed[side] or side != s["previous_move"]:
                continue
            self.since[side] = self.since[side] or t.timestamp
            if (t.timestamp-self.since[side]).total_seconds() < c.hold_seconds:
                continue
            opposite = levels["aDown"]-c.opposite_buffer if sign == 1 else levels["aUp"]+c.opposite_buffer
            entry = (t.ask if sign == 1 else t.bid)+sign*c.slippage
            risk = abs(entry-opposite)*units+c.cost_per_trade+c.slippage*units+c.delay_reserve
            if risk >= s["equity"]-450-c.liquidation_buffer:
                continue
            deadline = session.end-timedelta(hours=1)
            if tradable_until:
                deadline = min(deadline, tradable_until-timedelta(minutes=5))
            if t.timestamp >= deadline:
                continue
            s["used"].append(key)
            s["position"] = dict(side=side, sign=sign, entry=entry, edge=edge, opposite=opposite, extreme=price, trail=None, opened=t.timestamp.isoformat(), deadline=deadline.isoformat(), lots=.01)
            self.event(t, "ENTRY", s["position"])
            return


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("--csv", help="timestamp,bid,ask tick CSV in chronological order")
    parser.add_argument("--config")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    config = Config(**json.loads(Path(args.config).read_text())) if args.config else Config()
    robot = PaperRobot(args.state, config)
    try:
        if args.csv:
            with open(args.csv, newline="") as stream:
                for i, row in enumerate(csv.DictReader(stream), 1):
                    stamp = datetime.fromisoformat(row["timestamp"])
                    if stamp.tzinfo is None:
                        raise ValueError("CSV timestamps must have UTC offset")
                    bid, ask = float(row["bid"]), float(row["ask"])
                    robot.process(Tick(i,stamp,bid,ask,(bid+ask)/2))
        print(json.dumps(robot.s, indent=2))
    finally:
        robot.close()


if __name__ == "__main__":
    main()
