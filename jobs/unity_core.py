from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime
from statistics import median
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple


EPS = 1e-9


@dataclass
class UnityConfig:
    symbol: str = "XAUUSD"
    keepmaxticks: int = 12000
    noisewindow: int = 120
    spreadwindow: int = 60
    confirmmult: float = 6.0
    confirmspread: float = 0.25
    confirmmin: float = 0.18
    noisefloor: float = 0.05
    swingfactor: float = 4.0
    swingback: int = 3
    cleanpivotkeep: int = 24
    cleanbufferfrac: float = 0.16
    cleanbuffermin: int = 20
    cleanbuffermax: int = 90
    islandticks: int = 140
    islandconviction: float = 0.58
    transitionticks: int = 90
    signalminscore: float = 62.0
    signalminlag: int = 10
    signalmaxlag: int = 300
    signalminmultiple: float = 0.90
    signalmaturemultiple: float = 6.50
    signalmaturelag: int = 450
    signalfliplookback: int = 500
    signalalignbonus: float = 8.0
    tradenoisebuffer: float = 0.45
    tradespreadbuffer: float = 1.50
    tradebuffermin: float = 0.10
    trademinrisk: float = 0.25
    trademaxrisk: float = 8.00
    breakevenprofit: float = 1.00
    traildistance: float = 1.00

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TickData:
    id: int
    time: datetime
    bid: Optional[float]
    ask: Optional[float]
    mid: float
    spread: float

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "TickData":
        time_value = row["timestamp"]
        if isinstance(time_value, str):
            time_value = datetime.fromisoformat(time_value)
        spread = row.get("spread")
        if spread is None:
            bid = row.get("bid")
            ask = row.get("ask")
            if bid is not None and ask is not None:
                spread = float(ask) - float(bid)
            else:
                spread = 0.0
        return cls(
            id=int(row["id"]),
            time=time_value,
            bid=float(row["bid"]) if row.get("bid") is not None else None,
            ask=float(row["ask"]) if row.get("ask") is not None else None,
            mid=float(row["mid"]),
            spread=float(spread or 0.0),
        )


@dataclass
class PivotPoint:
    tickid: int
    time: datetime
    price: float
    kind: str
    noise: float
    thresh: float
    state: str
    legtick: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tickid": self.tickid,
            "time": self.time.isoformat(),
            "price": self.price,
            "kind": self.kind,
            "noise": self.noise,
            "thresh": self.thresh,
            "state": self.state,
            "legtick": self.legtick,
        }

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "PivotPoint":
        time_value = row["time"]
        if isinstance(time_value, str):
            time_value = datetime.fromisoformat(time_value)
        return cls(
            tickid=int(row["tickid"]),
            time=time_value,
            price=float(row["price"]),
            kind=str(row["kind"]),
            noise=float(row["noise"]),
            thresh=float(row["thresh"]),
            state=str(row["state"]),
            legtick=int(row["legtick"]),
        )


@dataclass
class SwingSegment:
    starttick: int
    endtick: int
    starttime: datetime
    endtime: datetime
    startprice: float
    endprice: float
    state: str
    ticks: int
    move: float
    efficiency: float
    multiple: float
    conviction: float

    def to_db_row(self, symbol: str) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "starttick": self.starttick,
            "endtick": self.endtick,
            "starttime": self.starttime,
            "endtime": self.endtime,
            "startprice": self.startprice,
            "endprice": self.endprice,
            "state": self.state,
            "ticks": self.ticks,
            "move": self.move,
            "efficiency": self.efficiency,
            "multiple": self.multiple,
            "conviction": self.conviction,
        }


@dataclass
class TradeState:
    signaltickid: int
    side: str
    state: str
    opentick: int
    opentime: datetime
    openprice: float
    pivotkind: str
    pivottickid: int
    pivotprice: float
    buffer: float
    risk: float
    stopprice: float
    targetprice: float
    bearmed: bool = False
    trailarmed: bool = False
    bestprice: Optional[float] = None
    bestfavor: float = 0.0
    bestadverse: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signaltickid": self.signaltickid,
            "side": self.side,
            "state": self.state,
            "opentick": self.opentick,
            "opentime": self.opentime.isoformat(),
            "openprice": self.openprice,
            "pivotkind": self.pivotkind,
            "pivottickid": self.pivottickid,
            "pivotprice": self.pivotprice,
            "buffer": self.buffer,
            "risk": self.risk,
            "stopprice": self.stopprice,
            "targetprice": self.targetprice,
            "bearmed": self.bearmed,
            "trailarmed": self.trailarmed,
            "bestprice": self.bestprice,
            "bestfavor": self.bestfavor,
            "bestadverse": self.bestadverse,
        }

    @classmethod
    def from_dict(cls, row: Dict[str, Any]) -> "TradeState":
        time_value = row["opentime"]
        if isinstance(time_value, str):
            time_value = datetime.fromisoformat(time_value)
        return cls(
            signaltickid=int(row["signaltickid"]),
            side=str(row["side"]),
            state=str(row["state"]),
            opentick=int(row["opentick"]),
            opentime=time_value,
            openprice=float(row["openprice"]),
            pivotkind=str(row["pivotkind"]),
            pivottickid=int(row["pivottickid"]),
            pivotprice=float(row["pivotprice"]),
            buffer=float(row["buffer"]),
            risk=float(row["risk"]),
            stopprice=float(row["stopprice"]),
            targetprice=float(row["targetprice"]),
            bearmed=bool(row.get("bearmed", False)),
            trailarmed=bool(row.get("trailarmed", False)),
            bestprice=float(row["bestprice"]) if row.get("bestprice") is not None else None,
            bestfavor=float(row.get("bestfavor", 0.0)),
            bestadverse=float(row.get("bestadverse", 0.0)),
        )


def _safe_median(values: Iterable[float], fallback: float) -> float:
    rows = [float(v) for v in values]
    if not rows:
        return float(fallback)
    return float(median(rows))


def _side_from_state(state: str) -> Optional[str]:
    if state == "green":
        return "long"
    if state == "red":
        return "short"
    return None


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_value(v) for v in value]
    return value


class UnityEngine:
    def __init__(self, config: Optional[UnityConfig] = None, state: Optional[Dict[str, Any]] = None):
        self.config = config or UnityConfig()
        self.absdiffs: Deque[float] = deque(maxlen=self.config.noisewindow)
        self.spreads: Deque[float] = deque(maxlen=self.config.spreadwindow)
        self.flipids: Deque[int] = deque()
        self.ticks: List[TickData] = []
        self.rows: List[Dict[str, Any]] = []
        self.pivots: List[PivotPoint] = []
        self.swingsegments: List[SwingSegment] = []
        self.opentrade: Optional[TradeState] = None

        self.currentdir: int = 0
        self.anchortickid: Optional[int] = None
        self.anchorprice: Optional[float] = None
        self.anchortime: Optional[datetime] = None
        self.extremetickid: Optional[int] = None
        self.extremeprice: Optional[float] = None
        self.extremetime: Optional[datetime] = None
        self.lastpivottickid: Optional[int] = None
        self.lastpivotprice: Optional[float] = None
        self.lastpivottime: Optional[datetime] = None
        self.lastpivotkind: Optional[str] = None
        self.legtravelabs: float = 0.0
        self.legthreshsum: float = 0.0
        self.legthreshcount: int = 0
        self.prevmid: Optional[float] = None
        self.prevcausalstate: str = "yellow"
        self.lastsignaledleg: Optional[int] = None
        self.lastsignaltickid: Optional[int] = None
        self.lastcleanstate: str = "yellow"
        self.lastcleanfromtick: Optional[int] = None

        self.dirtytickids: set[int] = set()
        self.newpivots: List[Dict[str, Any]] = []
        self.newsignals: List[Dict[str, Any]] = []
        self.newtrades: Dict[int, Dict[str, Any]] = {}
        self.tradeevents: List[Dict[str, Any]] = []
        self.swingdirtyfrom: Optional[int] = None

        if state:
            self._load_state(state)

    def _load_state(self, state: Dict[str, Any]) -> None:
        self.currentdir = int(state.get("currentdir", 0))
        self.anchortickid = state.get("anchortickid")
        self.anchorprice = float(state["anchorprice"]) if state.get("anchorprice") is not None else None
        self.anchortime = datetime.fromisoformat(state["anchortime"]) if state.get("anchortime") else None
        self.extremetickid = state.get("extremetickid")
        self.extremeprice = float(state["extremeprice"]) if state.get("extremeprice") is not None else None
        self.extremetime = datetime.fromisoformat(state["extremetime"]) if state.get("extremetime") else None
        self.lastpivottickid = state.get("lastpivottickid")
        self.lastpivotprice = float(state["lastpivotprice"]) if state.get("lastpivotprice") is not None else None
        self.lastpivottime = datetime.fromisoformat(state["lastpivottime"]) if state.get("lastpivottime") else None
        self.lastpivotkind = state.get("lastpivotkind")
        self.legtravelabs = float(state.get("legtravelabs", 0.0))
        self.legthreshsum = float(state.get("legthreshsum", 0.0))
        self.legthreshcount = int(state.get("legthreshcount", 0))
        self.prevmid = float(state["prevmid"]) if state.get("prevmid") is not None else None
        self.prevcausalstate = str(state.get("prevcausalstate", "yellow"))
        self.lastsignaledleg = int(state["lastsignaledleg"]) if state.get("lastsignaledleg") is not None else None
        self.lastsignaltickid = int(state["lastsignaltickid"]) if state.get("lastsignaltickid") is not None else None
        self.lastcleanstate = str(state.get("lastcleanstate", "yellow"))
        self.lastcleanfromtick = int(state["lastcleanfromtick"]) if state.get("lastcleanfromtick") is not None else None

        self.absdiffs.extend(float(v) for v in state.get("absdiffs", []))
        self.spreads.extend(float(v) for v in state.get("spreads", []))
        self.flipids.extend(int(v) for v in state.get("flipids", []))

        self.ticks = []
        for row in state.get("ticks", []):
            self.ticks.append(
                TickData(
                    id=int(row["id"]),
                    time=datetime.fromisoformat(row["time"]),
                    bid=float(row["bid"]) if row.get("bid") is not None else None,
                    ask=float(row["ask"]) if row.get("ask") is not None else None,
                    mid=float(row["mid"]),
                    spread=float(row.get("spread", 0.0)),
                )
            )

        self.rows = []
        for row in state.get("rows", []):
            cooked = dict(row)
            cooked["time"] = datetime.fromisoformat(cooked["time"])
            self.rows.append(cooked)

        self.pivots = [PivotPoint.from_dict(row) for row in state.get("pivots", [])]
        if state.get("opentrade"):
            self.opentrade = TradeState.from_dict(state["opentrade"])

        self._rebuild_clean_layer(force_full=True)
        self._trim_history()

    def export_state(self) -> Dict[str, Any]:
        return {
            "config": self.config.to_dict(),
            "currentdir": self.currentdir,
            "anchortickid": self.anchortickid,
            "anchorprice": self.anchorprice,
            "anchortime": self.anchortime.isoformat() if self.anchortime else None,
            "extremetickid": self.extremetickid,
            "extremeprice": self.extremeprice,
            "extremetime": self.extremetime.isoformat() if self.extremetime else None,
            "lastpivottickid": self.lastpivottickid,
            "lastpivotprice": self.lastpivotprice,
            "lastpivottime": self.lastpivottime.isoformat() if self.lastpivottime else None,
            "lastpivotkind": self.lastpivotkind,
            "legtravelabs": self.legtravelabs,
            "legthreshsum": self.legthreshsum,
            "legthreshcount": self.legthreshcount,
            "prevmid": self.prevmid,
            "prevcausalstate": self.prevcausalstate,
            "lastsignaledleg": self.lastsignaledleg,
            "lastsignaltickid": self.lastsignaltickid,
            "lastcleanstate": self.lastcleanstate,
            "lastcleanfromtick": self.lastcleanfromtick,
            "absdiffs": list(self.absdiffs),
            "spreads": list(self.spreads),
            "flipids": list(self.flipids),
            "ticks": [
                {
                    "id": t.id,
                    "time": t.time.isoformat(),
                    "bid": t.bid,
                    "ask": t.ask,
                    "mid": t.mid,
                    "spread": t.spread,
                }
                for t in self.ticks
            ],
            "rows": [
                _json_value(row)
                for row in self.rows
            ],
            "pivots": [p.to_dict() for p in self.pivots],
            "opentrade": self.opentrade.to_dict() if self.opentrade else None,
        }

    def drain_changes(self) -> Dict[str, Any]:
        dirtyrows = [
            row
            for row in self.rows
            if int(row["tickid"]) in self.dirtytickids
        ]
        swings = []
        if self.swingdirtyfrom is not None:
            swings = [
                segment.to_db_row(self.config.symbol)
                for segment in self.swingsegments
                if segment.endtick >= self.swingdirtyfrom
            ]
        out = {
            "ticks": dirtyrows,
            "pivots": list(self.newpivots),
            "swings": swings,
            "swingdirtyfrom": self.swingdirtyfrom,
            "signals": list(self.newsignals),
            "trades": list(self.newtrades.values()),
            "events": list(self.tradeevents),
        }
        self.dirtytickids.clear()
        self.newpivots.clear()
        self.newsignals.clear()
        self.newtrades.clear()
        self.tradeevents.clear()
        self.swingdirtyfrom = None
        return out

    def process_tick(self, raw: Dict[str, Any]) -> None:
        tick = TickData.from_row(raw)
        trade_open_at_start = self.opentrade is not None

        diff = 0.0 if self.prevmid is None else abs(tick.mid - self.prevmid)
        if self.prevmid is not None:
            self.absdiffs.append(diff)
        self.spreads.append(tick.spread)

        noise = self._noise_unit()
        thresh = self._confirm_threshold(noise)
        pivot = self._update_pivot_state(tick, noise, thresh)
        row = self._append_tick_row(tick, noise, thresh)
        if pivot is not None:
            self._mark_swing_dirty(pivot.tickid)

        self._rebuild_clean_layer()

        signal = self._maybe_emit_signal(row)
        if signal is not None:
            if trade_open_at_start and signal["favored"]:
                signal["used"] = False
                signal["status"] = "skipped"
                signal["skipreason"] = "opentrade"
                self.newsignals.append(signal)
            elif signal["favored"]:
                opened = self._open_trade_if_possible(row, signal)
                if opened is not None:
                    signal["used"] = True
                    signal["status"] = "opened"
                    signal["skipreason"] = None
                else:
                    signal["used"] = False
                    signal["status"] = "rejected"
                self.newsignals.append(signal)
            else:
                signal["used"] = False
                signal["status"] = "rejected"
                self.newsignals.append(signal)

        self._update_trade(row)
        self.prevmid = tick.mid
        self._trim_history()

    def _append_tick_row(self, tick: TickData, noise: float, thresh: float) -> Dict[str, Any]:
        if self.currentdir == 0 or self.lastpivottickid is None or self.lastpivotprice is None:
            causalstate = "yellow"
            causalscore = 70.0
            legeff = 0.0
            legmultiple = 0.0
            legtick = tick.id
        else:
            legtick = int(self.lastpivottickid)
            netmove = abs(tick.mid - float(self.lastpivotprice))
            meanthresh = self.legthreshsum / max(1, self.legthreshcount)
            legeff = netmove / (self.legtravelabs + EPS)
            legmultiple = netmove / max(EPS, meanthresh)
            legage = max(0, tick.id - legtick)
            transition = max(0.0, 1.0 - min(1.0, legage / max(1.0, float(self.config.transitionticks))))
            basetrend = max(0.0, min(100.0, 35.0 + 65.0 * legeff))
            baseyellow = max(0.0, min(100.0, 15.0 + 55.0 * (1.0 - legeff) + 20.0 * transition))
            if self.currentdir > 0:
                greenraw = basetrend
                redraw = 8.0
            else:
                greenraw = 8.0
                redraw = basetrend
            yellowraw = baseyellow
            totals = greenraw + redraw + yellowraw + EPS
            green = 100.0 * greenraw / totals
            red = 100.0 * redraw / totals
            yellow = 100.0 * yellowraw / totals
            if green >= red and green >= yellow:
                causalstate = "green"
            elif red >= green and red >= yellow:
                causalstate = "red"
            else:
                causalstate = "yellow"
            causalscore = max(green, red, yellow)

        if causalstate != self.prevcausalstate and causalstate in ("green", "red"):
            self.flipids.append(tick.id)
            while self.flipids and (tick.id - self.flipids[0]) > self.config.signalfliplookback:
                self.flipids.popleft()

        row = {
            "symbol": self.config.symbol,
            "tickid": tick.id,
            "time": tick.time,
            "price": tick.mid,
            "spread": tick.spread,
            "noise": noise,
            "thresh": thresh,
            "legtick": legtick,
            "legdir": self.currentdir,
            "legeff": round(legeff, 6),
            "legmultiple": round(legmultiple, 6),
            "causalscore": round(causalscore, 6),
            "causalstate": causalstate,
            "causalzone": legtick,
            "cleanstate": causalstate if causalstate in ("green", "red") else "yellow",
            "cleanzone": tick.id,
            "swingtick": None,
            "cleanconviction": 0.0,
            "revised": tick.time,
        }
        self.ticks.append(tick)
        self.rows.append(row)
        self.dirtytickids.add(tick.id)
        self.prevcausalstate = causalstate
        return row

    def _noise_unit(self) -> float:
        fallback = self.config.noisefloor
        if self.absdiffs:
            fallback = max(self.config.noisefloor, _safe_median(self.absdiffs, self.config.noisefloor))
        if len(self.absdiffs) < 20:
            return fallback
        return max(fallback * 0.5, _safe_median(self.absdiffs, fallback))

    def _confirm_threshold(self, noise: float) -> float:
        spreadmean = sum(self.spreads) / len(self.spreads) if self.spreads else 0.0
        return max(self.config.confirmmin, self.config.confirmmult * noise + self.config.confirmspread * spreadmean)

    def _update_pivot_state(self, tick: TickData, noise: float, thresh: float) -> Optional[PivotPoint]:
        if self.anchortickid is None:
            self.anchortickid = tick.id
            self.anchorprice = tick.mid
            self.anchortime = tick.time
            self.extremetickid = tick.id
            self.extremeprice = tick.mid
            self.extremetime = tick.time
            return None

        if self.currentdir == 0:
            move = tick.mid - float(self.anchorprice)
            if abs(move) > thresh:
                self.currentdir = 1 if move > 0 else -1
                self.lastpivottickid = self.anchortickid
                self.lastpivotprice = self.anchorprice
                self.lastpivottime = self.anchortime
                self.lastpivotkind = "low" if self.currentdir > 0 else "high"
                self.extremetickid = tick.id
                self.extremeprice = tick.mid
                self.extremetime = tick.time
                self.legtravelabs = abs(tick.mid - float(self.anchorprice))
                self.legthreshsum = thresh
                self.legthreshcount = 1
            return None

        self.legtravelabs += 0.0 if self.prevmid is None else abs(tick.mid - self.prevmid)
        self.legthreshsum += thresh
        self.legthreshcount += 1

        if self.currentdir > 0:
            if self.extremeprice is None or tick.mid >= self.extremeprice:
                self.extremetickid = tick.id
                self.extremeprice = tick.mid
                self.extremetime = tick.time
            drawdown = float(self.extremeprice) - tick.mid
            if drawdown >= thresh:
                pivot = PivotPoint(
                    tickid=int(self.extremetickid),
                    time=self.extremetime,
                    price=float(self.extremeprice),
                    kind="high",
                    noise=noise,
                    thresh=thresh,
                    state="red",
                    legtick=int(self.extremetickid),
                )
                self._accept_pivot(pivot)
                self.currentdir = -1
                self.lastpivottickid = pivot.tickid
                self.lastpivotprice = pivot.price
                self.lastpivottime = pivot.time
                self.lastpivotkind = pivot.kind
                self.extremetickid = tick.id
                self.extremeprice = tick.mid
                self.extremetime = tick.time
                self.legtravelabs = abs(tick.mid - pivot.price)
                self.legthreshsum = thresh
                self.legthreshcount = 1
                return pivot
        else:
            if self.extremeprice is None or tick.mid <= self.extremeprice:
                self.extremetickid = tick.id
                self.extremeprice = tick.mid
                self.extremetime = tick.time
            bounce = tick.mid - float(self.extremeprice)
            if bounce >= thresh:
                pivot = PivotPoint(
                    tickid=int(self.extremetickid),
                    time=self.extremetime,
                    price=float(self.extremeprice),
                    kind="low",
                    noise=noise,
                    thresh=thresh,
                    state="green",
                    legtick=int(self.extremetickid),
                )
                self._accept_pivot(pivot)
                self.currentdir = 1
                self.lastpivottickid = pivot.tickid
                self.lastpivotprice = pivot.price
                self.lastpivottime = pivot.time
                self.lastpivotkind = pivot.kind
                self.extremetickid = tick.id
                self.extremeprice = tick.mid
                self.extremetime = tick.time
                self.legtravelabs = abs(tick.mid - pivot.price)
                self.legthreshsum = thresh
                self.legthreshcount = 1
                return pivot
        return None

    def _accept_pivot(self, pivot: PivotPoint) -> None:
        self.pivots.append(pivot)
        self.newpivots.append(
            {
                "symbol": self.config.symbol,
                "tickid": pivot.tickid,
                "time": pivot.time,
                "price": pivot.price,
                "kind": pivot.kind,
                "noise": pivot.noise,
                "thresh": pivot.thresh,
                "state": pivot.state,
                "legtick": pivot.legtick,
            }
        )

    def _mark_swing_dirty(self, tickid: int) -> None:
        if self.swingdirtyfrom is None:
            self.swingdirtyfrom = tickid
        else:
            self.swingdirtyfrom = min(self.swingdirtyfrom, tickid)

    def _rebuild_clean_layer(self, force_full: bool = False) -> None:
        if not self.rows:
            return

        revstart = self._clean_revision_start()
        if revstart is None:
            revstart = self.rows[0]["tickid"]
        if force_full:
            revstart = self.rows[0]["tickid"]

        revindex = 0
        for i, row in enumerate(self.rows):
            if int(row["tickid"]) >= int(revstart):
                revindex = i
                break

        localrows = self.rows[revindex:]
        if not localrows:
            return

        relevant = [p for p in self.pivots if p.tickid >= revstart]
        compressed: List[PivotPoint] = []
        for pivot in relevant:
            if not compressed:
                compressed.append(pivot)
                continue
            last = compressed[-1]
            if pivot.kind == last.kind:
                if pivot.kind == "high" and pivot.price >= last.price:
                    compressed[-1] = pivot
                elif pivot.kind == "low" and pivot.price <= last.price:
                    compressed[-1] = pivot
            else:
                compressed.append(pivot)

        accepted: List[PivotPoint] = []
        for pivot in compressed:
            if not accepted:
                accepted.append(pivot)
                continue
            last = accepted[-1]
            if pivot.kind == last.kind:
                if pivot.kind == "high" and pivot.price >= last.price:
                    accepted[-1] = pivot
                elif pivot.kind == "low" and pivot.price <= last.price:
                    accepted[-1] = pivot
                continue
            moveabs = abs(pivot.price - last.price)
            needabs = max(pivot.thresh, last.thresh) * self.config.swingfactor
            if moveabs >= needabs:
                accepted.append(pivot)

        labels = {int(row["tickid"]): "yellow" for row in localrows}
        zones = {int(row["tickid"]): int(row["tickid"]) for row in localrows}
        conviction = {int(row["tickid"]): 0.0 for row in localrows}
        swingtick = {int(row["tickid"]): None for row in localrows}
        segments: List[SwingSegment] = []

        if len(accepted) >= 2:
            for idx in range(len(accepted) - 1):
                a = accepted[idx]
                b = accepted[idx + 1]
                if a.kind == "low" and b.kind == "high":
                    state = "green"
                elif a.kind == "high" and b.kind == "low":
                    state = "red"
                else:
                    state = "yellow"
                tickslice = [row for row in localrows if a.tickid <= int(row["tickid"]) <= b.tickid]
                if not tickslice:
                    continue
                prices = [float(row["price"]) for row in tickslice]
                netmoveabs = abs(prices[-1] - prices[0])
                travelabs = sum(abs(prices[i] - prices[i - 1]) for i in range(1, len(prices)))
                efficiency = netmoveabs / (travelabs + EPS)
                meanthresh = sum(float(row["thresh"]) for row in tickslice) / len(tickslice)
                multiple = netmoveabs / (meanthresh + EPS)
                conv = min(1.0, max(0.0, 0.55 * efficiency + 0.45 * min(1.0, multiple / 8.0)))
                segments.append(
                    SwingSegment(
                        starttick=a.tickid,
                        endtick=b.tickid,
                        starttime=a.time,
                        endtime=b.time,
                        startprice=a.price,
                        endprice=b.price,
                        state=state,
                        ticks=(b.tickid - a.tickid + 1),
                        move=netmoveabs,
                        efficiency=efficiency,
                        multiple=multiple,
                        conviction=conv,
                    )
                )
                for row in tickslice:
                    tickid = int(row["tickid"])
                    labels[tickid] = state
                    zones[tickid] = a.tickid
                    conviction[tickid] = conv
                    swingtick[tickid] = a.tickid

            for idx in range(1, len(accepted) - 1):
                piv = accepted[idx]
                prevp = accepted[idx - 1]
                nextp = accepted[idx + 1]
                leftlen = max(1, piv.tickid - prevp.tickid)
                rightlen = max(1, nextp.tickid - piv.tickid)
                buff = int(min(self.config.cleanbuffermax, max(self.config.cleanbuffermin, min(leftlen, rightlen) * self.config.cleanbufferfrac)))
                left = piv.tickid - buff
                right = piv.tickid + buff
                for row in localrows:
                    tickid = int(row["tickid"])
                    if left <= tickid <= right:
                        labels[tickid] = "yellow"
                        zones[tickid] = piv.tickid
                        conviction[tickid] = 0.0
                        swingtick[tickid] = None

        ordered = [int(row["tickid"]) for row in localrows]
        runs = self._build_runs(ordered, labels, conviction)
        changed = True
        while changed and len(runs) >= 3:
            changed = False
            for idx in range(1, len(runs) - 1):
                prevrun = runs[idx - 1]
                currun = runs[idx]
                nextrun = runs[idx + 1]
                if prevrun["state"] == nextrun["state"] and currun["state"] != prevrun["state"]:
                    if currun["ticks"] <= self.config.islandticks or currun["conv"] <= self.config.islandconviction:
                        for pos in range(currun["start"], currun["end"]):
                            tickid = ordered[pos]
                            labels[tickid] = prevrun["state"]
                            zones[tickid] = ordered[prevrun["start"]]
                            conviction[tickid] = max(conviction[tickid], max(prevrun["conv"], nextrun["conv"]))
                        changed = True
            if changed:
                runs = self._build_runs(ordered, labels, conviction)

        self.swingsegments = segments
        self.lastcleanfromtick = revstart
        self.swingdirtyfrom = revstart if self.swingdirtyfrom is None else min(self.swingdirtyfrom, revstart)

        for row in localrows:
            tickid = int(row["tickid"])
            row["cleanstate"] = labels[tickid]
            row["cleanzone"] = zones[tickid]
            row["swingtick"] = swingtick[tickid]
            row["cleanconviction"] = round(conviction[tickid], 6)
            self.dirtytickids.add(tickid)

        self.lastcleanstate = localrows[-1]["cleanstate"]

    def _build_runs(self, ordered: List[int], labels: Dict[int, str], conviction: Dict[int, float]) -> List[Dict[str, Any]]:
        runs: List[Dict[str, Any]] = []
        start = 0
        while start < len(ordered):
            end = start + 1
            while end < len(ordered) and labels[ordered[end]] == labels[ordered[start]]:
                end += 1
            runids = ordered[start:end]
            meanconv = sum(conviction[tickid] for tickid in runids) / max(1, len(runids))
            runs.append({"start": start, "end": end, "state": labels[ordered[start]], "ticks": len(runids), "conv": meanconv})
            start = end
        return runs

    def _clean_revision_start(self) -> Optional[int]:
        if len(self.pivots) >= self.config.cleanpivotkeep:
            return self.pivots[-self.config.cleanpivotkeep].tickid
        if len(self.pivots) >= self.config.swingback:
            return self.pivots[-self.config.swingback].tickid
        if self.rows:
            return self.rows[0]["tickid"]
        return None

    def _maybe_emit_signal(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        state = row["causalstate"]
        side = _side_from_state(state)
        legtick = int(row["legtick"])
        if side is None:
            return None
        if self.lastsignaledleg == legtick:
            return None

        score, favored, reason, detail = self._score_signal(row)
        self.lastsignaledleg = legtick
        self.lastsignaltickid = int(row["tickid"])
        return {
            "symbol": self.config.symbol,
            "tickid": int(row["tickid"]),
            "time": row["time"],
            "side": side,
            "state": state,
            "price": float(row["price"]),
            "score": round(score, 6),
            "favored": bool(favored),
            "reason": reason,
            "detail": _json_value(detail),
            "context": _json_value(
                {
                    "causalzone": row["causalzone"],
                    "cleanzone": row["cleanzone"],
                    "legtick": row["legtick"],
                    "swingtick": row["swingtick"],
                    "cleanstate": row["cleanstate"],
                }
            ),
            "used": False,
            "skipreason": None,
            "status": "seen",
        }

    def _score_signal(self, row: Dict[str, Any]) -> Tuple[float, bool, str, Dict[str, Any]]:
        ticklag = int(row["tickid"]) - int(row["legtick"])
        price_lag = abs(float(row["price"]) - float(self.lastpivotprice or row["price"]))
        multiple = float(row["legmultiple"])
        legeff = float(row["legeff"])
        cleanmatch = row["cleanstate"] == row["causalstate"]
        recentflip = sum(1 for flipid in self.flipids if (int(row["tickid"]) - flipid) <= self.config.signalfliplookback)
        distprev = int(row["tickid"]) - int(self.lastsignaltickid) if self.lastsignaltickid is not None else 999999
        conviction = float(row["cleanconviction"])
        mature = multiple >= self.config.signalmaturemultiple or ticklag >= self.config.signalmaturelag
        tooearly = ticklag < self.config.signalminlag
        toolate = ticklag > self.config.signalmaxlag

        score = 0.0
        score += min(22.0, 22.0 * min(1.0, multiple / max(self.config.signalminmultiple, EPS)))
        score += 24.0 * max(0.0, min(1.0, legeff))
        score += 18.0 * max(0.0, min(1.0, conviction))
        score += 12.0 * max(0.0, min(1.0, price_lag / max(float(row["thresh"]), EPS)))
        score += min(8.0, distprev / 40.0)
        if cleanmatch:
            score += self.config.signalignbonus
        score -= min(18.0, recentflip * 4.0)
        if tooearly:
            score -= 16.0
        if toolate:
            score -= 12.0
        if mature:
            score -= 20.0

        favored = (
            score >= self.config.signalminscore
            and not tooearly
            and not toolate
            and not mature
            and multiple >= self.config.signalminmultiple
        )

        reasons: List[str] = []
        if cleanmatch:
            reasons.append("cleanalign")
        if legeff >= 0.60:
            reasons.append("efficient")
        if conviction >= 0.55:
            reasons.append("clean")
        if multiple >= 1.50:
            reasons.append("impulse")
        if tooearly:
            reasons.append("early")
        if toolate:
            reasons.append("late")
        if mature:
            reasons.append("mature")
        if recentflip >= 3:
            reasons.append("choppy")
        if not reasons:
            reasons.append("neutral")
        detail = {
            "ticklag": ticklag,
            "pricelag": round(price_lag, 6),
            "multiple": round(multiple, 6),
            "legeff": round(legeff, 6),
            "cleanmatch": cleanmatch,
            "cleanconviction": round(conviction, 6),
            "recentflip": recentflip,
            "distprev": distprev,
            "mature": mature,
            "tooearly": tooearly,
            "toolate": toolate,
        }
        return score, favored, ",".join(reasons), detail

    def _open_trade_if_possible(self, row: Dict[str, Any], signal: Dict[str, Any]) -> Optional[TradeState]:
        if self.opentrade is not None:
            return None
        pivot = self._trade_pivot_for_side(signal["side"])
        if pivot is None:
            signal["skipreason"] = "nopivot"
            signal["reason"] = f"{signal['reason']},nopivot"
            signal["favored"] = False
            return None

        tick = self.ticks[-1]
        if signal["side"] == "long":
            entry = tick.ask if tick.ask is not None else tick.mid
        else:
            entry = tick.bid if tick.bid is not None else tick.mid

        buffer = max(
            self.config.tradebuffermin,
            self.config.tradenoisebuffer * float(row["noise"]),
            self.config.tradespreadbuffer * float(row["spread"]),
        )
        if signal["side"] == "long":
            stop = pivot.price - buffer
            risk = entry - stop
            target = entry + risk
        else:
            stop = pivot.price + buffer
            risk = stop - entry
            target = entry - risk

        if risk < self.config.trademinrisk or risk > self.config.trademaxrisk:
            signal["skipreason"] = "risk"
            signal["reason"] = f"{signal['reason']},risk"
            signal["favored"] = False
            return None

        trade = TradeState(
            signaltickid=int(signal["tickid"]),
            side=signal["side"],
            state=str(signal["state"]),
            opentick=int(signal["tickid"]),
            opentime=row["time"],
            openprice=float(entry),
            pivotkind=pivot.kind,
            pivottickid=pivot.tickid,
            pivotprice=pivot.price,
            buffer=float(buffer),
            risk=float(risk),
            stopprice=float(stop),
            targetprice=float(target),
            bestprice=float(entry),
        )
        self.opentrade = trade
        payload = {
            "symbol": self.config.symbol,
            "signaltickid": trade.signaltickid,
            "side": trade.side,
            "state": trade.state,
            "opentick": trade.opentick,
            "opentime": trade.opentime,
            "openprice": trade.openprice,
            "pivottickid": trade.pivottickid,
            "pivotprice": trade.pivotprice,
            "buffer": trade.buffer,
            "risk": trade.risk,
            "stopprice": trade.stopprice,
            "targetprice": trade.targetprice,
            "bearmed": False,
            "trailarmed": False,
            "bestprice": trade.bestprice,
            "bestfavor": 0.0,
            "bestadverse": 0.0,
            "status": "open",
            "closetick": None,
            "closetime": None,
            "closeprice": None,
            "pnl": None,
            "exitreason": None,
        }
        self.newtrades[trade.signaltickid] = payload
        self.tradeevents.append(
            {
                "symbol": self.config.symbol,
                "signaltickid": trade.signaltickid,
                "tickid": trade.opentick,
                "time": trade.opentime,
                "kind": "open",
                "price": trade.openprice,
                "stopprice": trade.stopprice,
                "targetprice": trade.targetprice,
                "reason": "open",
                "detail": _json_value(
                    {
                        "pivotkind": trade.pivotkind,
                        "pivottickid": trade.pivottickid,
                        "pivotprice": trade.pivotprice,
                        "buffer": trade.buffer,
                        "risk": trade.risk,
                    }
                ),
            }
        )
        return trade

    def _trade_pivot_for_side(self, side: str) -> Optional[PivotPoint]:
        want = "low" if side == "long" else "high"
        for pivot in reversed(self.pivots):
            if pivot.kind == want:
                return pivot
        if self.lastpivottickid is not None and self.lastpivotkind == want and self.lastpivotprice is not None and self.lastpivottime is not None:
            return PivotPoint(
                tickid=int(self.lastpivottickid),
                time=self.lastpivottime,
                price=float(self.lastpivotprice),
                kind=str(self.lastpivotkind),
                noise=float(self.rows[-1]["noise"]),
                thresh=float(self.rows[-1]["thresh"]),
                state=self.rows[-1]["causalstate"],
                legtick=int(self.lastpivottickid),
            )
        return None

    def _update_trade(self, row: Dict[str, Any]) -> None:
        if self.opentrade is None:
            return

        trade = self.opentrade
        tick = self.ticks[-1]
        if trade.side == "long":
            mark = tick.bid if tick.bid is not None else tick.mid
            favor = mark - trade.openprice
            adverse = trade.openprice - mark
            if trade.bestprice is None or mark > trade.bestprice:
                trade.bestprice = mark
                self.tradeevents.append(
                    {
                        "symbol": self.config.symbol,
                        "signaltickid": trade.signaltickid,
                        "tickid": tick.id,
                        "time": tick.time,
                        "kind": "best",
                        "price": mark,
                        "stopprice": trade.stopprice,
                        "targetprice": trade.targetprice,
                        "reason": "best",
                        "detail": _json_value({"favor": favor}),
                    }
                )
            trade.bestfavor = max(trade.bestfavor, favor)
            trade.bestadverse = max(trade.bestadverse, max(0.0, adverse))
            if trade.bestfavor >= self.config.breakevenprofit and not trade.bearmed:
                trade.bearmed = True
                trade.stopprice = max(trade.stopprice, trade.openprice)
                self.tradeevents.append(
                    {
                        "symbol": self.config.symbol,
                        "signaltickid": trade.signaltickid,
                        "tickid": tick.id,
                        "time": tick.time,
                        "kind": "breakeven",
                        "price": mark,
                        "stopprice": trade.stopprice,
                        "targetprice": trade.targetprice,
                        "reason": "breakeven",
                        "detail": _json_value({"favor": trade.bestfavor}),
                    }
                )
            if trade.bestfavor >= self.config.breakevenprofit and not trade.trailarmed:
                trade.trailarmed = True
                self.tradeevents.append(
                    {
                        "symbol": self.config.symbol,
                        "signaltickid": trade.signaltickid,
                        "tickid": tick.id,
                        "time": tick.time,
                        "kind": "trailarm",
                        "price": mark,
                        "stopprice": trade.stopprice,
                        "targetprice": trade.targetprice,
                        "reason": "trailarm",
                        "detail": _json_value({"favor": trade.bestfavor}),
                    }
                )
            if trade.trailarmed and trade.bestprice is not None:
                trade.stopprice = max(trade.stopprice, trade.bestprice - self.config.traildistance)
            exitreason = None
            exitprice = None
            if mark >= trade.targetprice:
                exitreason = "tp"
                exitprice = trade.targetprice
            elif mark <= trade.stopprice:
                if trade.trailarmed and trade.stopprice > trade.openprice + EPS:
                    exitreason = "trail"
                elif trade.bearmed and abs(trade.stopprice - trade.openprice) <= EPS:
                    exitreason = "breakeven"
                else:
                    exitreason = "sl"
                exitprice = trade.stopprice
            elif row["causalstate"] == "red":
                exitreason = "regimechange"
                exitprice = mark
        else:
            mark = tick.ask if tick.ask is not None else tick.mid
            favor = trade.openprice - mark
            adverse = mark - trade.openprice
            if trade.bestprice is None or mark < trade.bestprice:
                trade.bestprice = mark
                self.tradeevents.append(
                    {
                        "symbol": self.config.symbol,
                        "signaltickid": trade.signaltickid,
                        "tickid": tick.id,
                        "time": tick.time,
                        "kind": "best",
                        "price": mark,
                        "stopprice": trade.stopprice,
                        "targetprice": trade.targetprice,
                        "reason": "best",
                        "detail": _json_value({"favor": favor}),
                    }
                )
            trade.bestfavor = max(trade.bestfavor, favor)
            trade.bestadverse = max(trade.bestadverse, max(0.0, adverse))
            if trade.bestfavor >= self.config.breakevenprofit and not trade.bearmed:
                trade.bearmed = True
                trade.stopprice = min(trade.stopprice, trade.openprice)
                self.tradeevents.append(
                    {
                        "symbol": self.config.symbol,
                        "signaltickid": trade.signaltickid,
                        "tickid": tick.id,
                        "time": tick.time,
                        "kind": "breakeven",
                        "price": mark,
                        "stopprice": trade.stopprice,
                        "targetprice": trade.targetprice,
                        "reason": "breakeven",
                        "detail": _json_value({"favor": trade.bestfavor}),
                    }
                )
            if trade.bestfavor >= self.config.breakevenprofit and not trade.trailarmed:
                trade.trailarmed = True
                self.tradeevents.append(
                    {
                        "symbol": self.config.symbol,
                        "signaltickid": trade.signaltickid,
                        "tickid": tick.id,
                        "time": tick.time,
                        "kind": "trailarm",
                        "price": mark,
                        "stopprice": trade.stopprice,
                        "targetprice": trade.targetprice,
                        "reason": "trailarm",
                        "detail": _json_value({"favor": trade.bestfavor}),
                    }
                )
            if trade.trailarmed and trade.bestprice is not None:
                trade.stopprice = min(trade.stopprice, trade.bestprice + self.config.traildistance)
            exitreason = None
            exitprice = None
            if mark <= trade.targetprice:
                exitreason = "tp"
                exitprice = trade.targetprice
            elif mark >= trade.stopprice:
                if trade.trailarmed and trade.stopprice < trade.openprice - EPS:
                    exitreason = "trail"
                elif trade.bearmed and abs(trade.stopprice - trade.openprice) <= EPS:
                    exitreason = "breakeven"
                else:
                    exitreason = "sl"
                exitprice = trade.stopprice
            elif row["causalstate"] == "green":
                exitreason = "regimechange"
                exitprice = mark

        payload = self.newtrades.get(trade.signaltickid, {
            "symbol": self.config.symbol,
            "signaltickid": trade.signaltickid,
            "side": trade.side,
            "state": trade.state,
            "opentick": trade.opentick,
            "opentime": trade.opentime,
            "openprice": trade.openprice,
            "pivottickid": trade.pivottickid,
            "pivotprice": trade.pivotprice,
            "buffer": trade.buffer,
            "risk": trade.risk,
        })
        payload.update(
            {
                "stopprice": trade.stopprice,
                "targetprice": trade.targetprice,
                "bearmed": trade.bearmed,
                "trailarmed": trade.trailarmed,
                "bestprice": trade.bestprice,
                "bestfavor": trade.bestfavor,
                "bestadverse": trade.bestadverse,
                "status": "open",
                "closetick": None,
                "closetime": None,
                "closeprice": None,
                "pnl": None,
                "exitreason": None,
            }
        )

        if exitreason is not None and exitprice is not None:
            pnl = exitprice - trade.openprice if trade.side == "long" else trade.openprice - exitprice
            payload.update(
                {
                    "status": "closed",
                    "closetick": tick.id,
                    "closetime": tick.time,
                    "closeprice": exitprice,
                    "pnl": pnl,
                    "exitreason": exitreason,
                }
            )
            self.tradeevents.append(
                {
                    "symbol": self.config.symbol,
                    "signaltickid": trade.signaltickid,
                    "tickid": tick.id,
                    "time": tick.time,
                    "kind": "close",
                    "price": exitprice,
                    "stopprice": trade.stopprice,
                    "targetprice": trade.targetprice,
                    "reason": exitreason,
                    "detail": _json_value({"pnl": pnl}),
                }
            )
            self.opentrade = None

        self.newtrades[trade.signaltickid] = payload

    def _trim_history(self) -> None:
        if len(self.rows) <= self.config.keepmaxticks:
            return
        keepfrom = self._clean_revision_start()
        if keepfrom is None:
            keepfrom = self.rows[-self.config.keepmaxticks]["tickid"]
        keepfrom = min(keepfrom, self.rows[-self.config.keepmaxticks]["tickid"])
        cut = 0
        for i, row in enumerate(self.rows):
            if int(row["tickid"]) >= int(keepfrom):
                cut = i
                break
        if cut <= 0:
            return
        self.rows = self.rows[cut:]
        self.ticks = self.ticks[cut:]
        min_keep_tick = int(self.rows[0]["tickid"])
        self.pivots = [pivot for pivot in self.pivots if pivot.tickid >= min_keep_tick]
        while self.flipids and self.flipids[0] < min_keep_tick:
            self.flipids.popleft()


def config_from_json(raw: Optional[str]) -> UnityConfig:
    if not raw:
        return UnityConfig()
    data = json.loads(raw)
    return UnityConfig(**data)
