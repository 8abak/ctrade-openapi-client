from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterable, List, Optional, Sequence, Tuple


OTT_SOURCES = ("ask", "bid", "mid")
OTT_MA_TYPES = ("SMA", "EMA", "WMA", "TMA", "VAR", "WWMA", "ZLEMA", "TSF")
OTT_SIGNAL_MODES = ("support", "price", "color")

DEFAULT_OTT_SOURCE = "mid"
DEFAULT_OTT_MA_TYPE = "VAR"
DEFAULT_OTT_LENGTH = 2
DEFAULT_OTT_PERCENT = 1.4
DEFAULT_OTT_SIGNAL_MODE = "support"
DEFAULT_OTT_HIGHLIGHT = True

EPSILON = 1e-12


@dataclass(frozen=True)
class OttConfig:
    source: str = DEFAULT_OTT_SOURCE
    matype: str = DEFAULT_OTT_MA_TYPE
    length: int = DEFAULT_OTT_LENGTH
    percent: float = DEFAULT_OTT_PERCENT

    def normalized(self) -> "OttConfig":
        source = (self.source or DEFAULT_OTT_SOURCE).lower()
        matype = (self.matype or DEFAULT_OTT_MA_TYPE).upper()
        length = max(1, int(self.length))
        percent = float(self.percent)
        if source not in OTT_SOURCES:
            raise ValueError("Unsupported OTT source: {0}".format(source))
        if matype not in OTT_MA_TYPES:
            raise ValueError("Unsupported OTT moving average type: {0}".format(matype))
        return OttConfig(source=source, matype=matype, length=length, percent=percent)

    def job_name(self, symbol: str) -> str:
        safe_percent = ("{0:.8f}".format(self.percent)).rstrip("0").rstrip(".") or "0"
        return "ott:{0}:{1}:{2}:{3}:{4}".format(symbol, self.source, self.matype, self.length, safe_percent)


def select_price(row: Dict[str, Any], source: str) -> float:
    source = source.lower()
    if source == "ask":
        value = row.get("ask")
    elif source == "bid":
        value = row.get("bid")
    else:
        value = row.get("mid")
        if value is None:
            bid = row.get("bid")
            ask = row.get("ask")
            value = ((float(bid) + float(ask)) / 2.0) if bid is not None and ask is not None else row.get("price")
    if value is None:
        raise ValueError("Missing source value for tick {0}".format(row.get("id")))
    return float(value)


def nz(value: Optional[float], replacement: float = 0.0) -> float:
    return replacement if value is None else float(value)


def crossover(current_left: Optional[float], current_right: Optional[float], prev_left: Optional[float], prev_right: Optional[float]) -> bool:
    if None in (current_left, current_right, prev_left, prev_right):
        return False
    return float(current_left) > float(current_right) and float(prev_left) <= float(prev_right)


def crossunder(current_left: Optional[float], current_right: Optional[float], prev_left: Optional[float], prev_right: Optional[float]) -> bool:
    if None in (current_left, current_right, prev_left, prev_right):
        return False
    return float(current_left) < float(current_right) and float(prev_left) >= float(prev_right)


def linreg(values: Sequence[float], offset: int) -> Optional[float]:
    count = len(values)
    if count == 0:
        return None

    xs = [float(index) for index in range(count)]
    ys = [float(value) for value in values]
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xx = sum(value * value for value in xs)
    sum_xy = sum(x_value * y_value for x_value, y_value in zip(xs, ys))
    denom = (count * sum_xx) - (sum_x * sum_x)
    if abs(denom) <= EPSILON:
        slope = 0.0
        intercept = sum_y / float(count)
    else:
        slope = ((count * sum_xy) - (sum_x * sum_y)) / denom
        intercept = (sum_y - (slope * sum_x)) / float(count)
    return intercept + slope * float(count - 1 - offset)


class OttCalculator:
    def __init__(self, config: OttConfig, state: Optional[Dict[str, Any]] = None):
        self.config = config.normalized()
        self.length = self.config.length
        self.percent = self.config.percent
        self.outer_tma_length = (self.length // 2) + 1
        self.inner_tma_length = (self.length + 1) // 2 if self.length % 2 else self.length // 2
        self.zlema_lag = self.length // 2 if self.length % 2 == 0 else (self.length - 1) // 2
        self.source_history: Deque[float] = deque(maxlen=max(self.length, self.zlema_lag + 1, 9))
        self.tma_history: Deque[float] = deque(maxlen=self.outer_tma_length)
        self.var_up_history: Deque[float] = deque(maxlen=9)
        self.var_down_history: Deque[float] = deque(maxlen=9)
        self.ott_history: Deque[Optional[float]] = deque(maxlen=6)
        self.ema_prev: Optional[float] = None
        self.var_prev: Optional[float] = None
        self.wwma_prev: Optional[float] = None
        self.zlema_ema_prev: Optional[float] = None
        self.prev_src: Optional[float] = None
        self.prev_mavg: Optional[float] = None
        self.prev_ott2: Optional[float] = None
        self.prev_ott3: Optional[float] = None
        self.long_stop_prev_value: Optional[float] = None
        self.short_stop_prev_value: Optional[float] = None
        self.dir_value: int = 1
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.source_history = deque((float(value) for value in state.get("sourcehistory", [])), maxlen=self.source_history.maxlen)
        self.tma_history = deque((float(value) for value in state.get("tmahistory", [])), maxlen=self.tma_history.maxlen)
        self.var_up_history = deque((float(value) for value in state.get("varuphistory", [])), maxlen=self.var_up_history.maxlen)
        self.var_down_history = deque((float(value) for value in state.get("vardownhistory", [])), maxlen=self.var_down_history.maxlen)
        self.ott_history = deque(state.get("otthistory", []), maxlen=self.ott_history.maxlen)
        self.ema_prev = self._optional_float(state.get("emaprev"))
        self.var_prev = self._optional_float(state.get("varprev"))
        self.wwma_prev = self._optional_float(state.get("wwmaprev"))
        self.zlema_ema_prev = self._optional_float(state.get("zlemaemaprev"))
        self.prev_src = self._optional_float(state.get("prevsrc"))
        self.prev_mavg = self._optional_float(state.get("prevmavg"))
        self.prev_ott2 = self._optional_float(state.get("prevott2"))
        self.prev_ott3 = self._optional_float(state.get("prevott3"))
        self.long_stop_prev_value = self._optional_float(state.get("longstopprev"))
        self.short_stop_prev_value = self._optional_float(state.get("shortstopprev"))
        self.dir_value = int(state.get("dir", 1))

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "sourcehistory": list(self.source_history),
            "tmahistory": list(self.tma_history),
            "varuphistory": list(self.var_up_history),
            "vardownhistory": list(self.var_down_history),
            "otthistory": list(self.ott_history),
            "emaprev": self.ema_prev,
            "varprev": self.var_prev,
            "wwmaprev": self.wwma_prev,
            "zlemaemaprev": self.zlema_ema_prev,
            "prevsrc": self.prev_src,
            "prevmavg": self.prev_mavg,
            "prevott2": self.prev_ott2,
            "prevott3": self.prev_ott3,
            "longstopprev": self.long_stop_prev_value,
            "shortstopprev": self.short_stop_prev_value,
            "dir": self.dir_value,
        }

    def process_tick(self, row: Dict[str, Any]) -> Dict[str, Any]:
        tickid = int(row["id"])
        src = select_price(row, self.config.source)
        prev_src = self.prev_src

        self.source_history.append(src)

        vud1 = (src - prev_src) if prev_src is not None and src > prev_src else 0.0
        vdd1 = (prev_src - src) if prev_src is not None and src < prev_src else 0.0
        self.var_up_history.append(vud1)
        self.var_down_history.append(vdd1)

        vud = sum(self.var_up_history)
        vdd = sum(self.var_down_history)
        denom = vud + vdd
        vcmo = 0.0 if abs(denom) <= EPSILON else (vud - vdd) / denom
        valpha = 2.0 / float(self.length + 1)
        var_value = (valpha * abs(vcmo) * src) + ((1.0 - (valpha * abs(vcmo))) * nz(self.var_prev))
        self.var_prev = var_value

        wwalpha = 1.0 / float(self.length)
        wwma_value = (wwalpha * src) + ((1.0 - wwalpha) * nz(self.wwma_prev))
        self.wwma_prev = wwma_value

        alpha = 2.0 / float(self.length + 1)
        ema_value = src if self.ema_prev is None else (alpha * src) + ((1.0 - alpha) * self.ema_prev)
        self.ema_prev = ema_value

        sma_value = self._sma(self.source_history, self.length)
        wma_value = self._wma(self.source_history, self.length)

        inner_tma_value = self._sma(self.source_history, self.inner_tma_length)
        if inner_tma_value is not None:
            self.tma_history.append(inner_tma_value)
        tma_value = self._sma(self.tma_history, self.outer_tma_length)

        zlema_input = None
        if len(self.source_history) > self.zlema_lag:
            lagged = list(self.source_history)[-(self.zlema_lag + 1)]
            zlema_input = src + (src - lagged)
            self.zlema_ema_prev = zlema_input if self.zlema_ema_prev is None else (alpha * zlema_input) + ((1.0 - alpha) * self.zlema_ema_prev)
        zlema_value = self.zlema_ema_prev if zlema_input is not None else None

        tsf_value = None
        if len(self.source_history) >= self.length:
            window = list(self.source_history)[-self.length:]
            lrc = linreg(window, 0)
            lrc1 = linreg(window, 1)
            if lrc is not None and lrc1 is not None:
                lrs = lrc - lrc1
                tsf_value = lrc + lrs

        mavg = {
            "SMA": sma_value,
            "EMA": ema_value,
            "WMA": wma_value,
            "TMA": tma_value,
            "VAR": var_value,
            "WWMA": wwma_value,
            "ZLEMA": zlema_value,
            "TSF": tsf_value,
        }[self.config.matype]

        fark = None
        long_stop = None
        short_stop = None
        direction = self.dir_value
        mt = None
        ott = None

        if mavg is not None:
            fark = mavg * self.percent * 0.01
            base_long_stop = mavg - fark
            long_stop_prev = self.long_stop_prev_value if self.long_stop_prev_value is not None else base_long_stop
            long_stop = max(base_long_stop, long_stop_prev) if mavg > long_stop_prev else base_long_stop

            base_short_stop = mavg + fark
            short_stop_prev = self.short_stop_prev_value if self.short_stop_prev_value is not None else base_short_stop
            short_stop = min(base_short_stop, short_stop_prev) if mavg < short_stop_prev else base_short_stop

            direction_prev = self.dir_value
            direction = direction_prev
            if direction_prev == -1 and mavg > short_stop_prev:
                direction = 1
            elif direction_prev == 1 and mavg < long_stop_prev:
                direction = -1

            mt = long_stop if direction == 1 else short_stop
            ott = mt * (200.0 + self.percent) / 200.0 if mavg > mt else mt * (200.0 - self.percent) / 200.0

            self.long_stop_prev_value = long_stop
            self.short_stop_prev_value = short_stop
            self.dir_value = direction

        ott2 = self.ott_history[-2] if len(self.ott_history) >= 2 else None
        ott3 = self.ott_history[-3] if len(self.ott_history) >= 3 else None

        support_buy = crossover(mavg, ott2, self.prev_mavg, self.prev_ott2)
        support_sell = crossunder(mavg, ott2, self.prev_mavg, self.prev_ott2)
        price_buy = crossover(src, ott2, prev_src, self.prev_ott2)
        price_sell = crossunder(src, ott2, prev_src, self.prev_ott2)
        color_buy = crossover(ott2, ott3, self.prev_ott2, self.prev_ott3)
        color_sell = crossunder(ott2, ott3, self.prev_ott2, self.prev_ott3)
        ott_color = None if ott2 is None or ott3 is None else ("green" if ott2 > ott3 else "red")

        result = {
            "tickid": tickid,
            "symbol": row["symbol"],
            "source": self.config.source,
            "matype": self.config.matype,
            "length": self.length,
            "percent": self.percent,
            "timestamp": row["timestamp"],
            "price": src,
            "mavg": mavg,
            "fark": fark,
            "longstop": long_stop,
            "shortstop": short_stop,
            "dir": direction,
            "mt": mt,
            "ott": ott,
            "ott2": ott2,
            "ott3": ott3,
            "ottcolor": ott_color,
            "highlightup": bool(mavg is not None and ott is not None and mavg > ott),
            "highlightdown": bool(mavg is not None and ott is not None and mavg < ott),
            "supportbuy": support_buy,
            "supportsell": support_sell,
            "pricebuy": price_buy,
            "pricesell": price_sell,
            "colorbuy": color_buy,
            "colorsell": color_sell,
        }

        self.prev_src = src
        self.prev_mavg = mavg
        self.prev_ott2 = ott2
        self.prev_ott3 = ott3
        self.ott_history.append(ott)
        return result

    @staticmethod
    def _optional_float(value: Any) -> Optional[float]:
        return None if value is None else float(value)

    @staticmethod
    def _sma(values: Iterable[float], length: int) -> Optional[float]:
        window = list(values)
        if len(window) < length:
            return None
        segment = window[-length:]
        return sum(segment) / float(length)

    @staticmethod
    def _wma(values: Iterable[float], length: int) -> Optional[float]:
        window = list(values)
        if len(window) < length:
            return None
        segment = window[-length:]
        weights = list(range(1, length + 1))
        numerator = sum(weight * value for weight, value in zip(weights, segment))
        denominator = sum(weights)
        return numerator / float(denominator)


def compute_ott_rows(
    rows: Sequence[Dict[str, Any]],
    config: OttConfig,
    state: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    calculator = OttCalculator(config, state=state)
    results = [calculator.process_tick(row) for row in rows]
    return results, calculator.snapshot_state()


@dataclass
class BacktestTrade:
    entrytickid: int
    exittickid: int
    entryts: Any
    exitts: Any
    direction: str
    entryprice: float
    exitprice: float
    pnl: float
    pnlpoints: float
    barsorticksheld: int
    signalentrytype: str
    signalexittype: str

    def to_record(self) -> Dict[str, Any]:
        return {
            "entrytickid": self.entrytickid,
            "exittickid": self.exittickid,
            "entryts": self.entryts,
            "exitts": self.exitts,
            "direction": self.direction,
            "entryprice": self.entryprice,
            "exitprice": self.exitprice,
            "pnl": self.pnl,
            "pnlpoints": self.pnlpoints,
            "barsorticksheld": self.barsorticksheld,
            "signalentrytype": self.signalentrytype,
            "signalexittype": self.signalexittype,
        }


def signal_columns(signalmode: str) -> Tuple[str, str]:
    signalmode = (signalmode or DEFAULT_OTT_SIGNAL_MODE).lower()
    if signalmode not in OTT_SIGNAL_MODES:
        raise ValueError("Unsupported OTT signal mode: {0}".format(signalmode))
    return "{0}buy".format(signalmode), "{0}sell".format(signalmode)


def execution_price(row: Dict[str, Any], direction: str, side: str) -> float:
    bid = row.get("bid")
    ask = row.get("ask")
    if direction == "long":
        raw = ask if side == "entry" and ask is not None else bid if bid is not None else row.get("price")
    else:
        raw = bid if side == "entry" and bid is not None else ask if ask is not None else row.get("price")
    if raw is None:
        raise ValueError("Missing execution price on tick {0}".format(row.get("tickid") or row.get("id")))
    return float(raw)


def run_ott_backtest(rows: Sequence[Dict[str, Any]], signalmode: str = DEFAULT_OTT_SIGNAL_MODE) -> Dict[str, Any]:
    buy_column, sell_column = signal_columns(signalmode)
    trades: List[BacktestTrade] = []
    position: Optional[Dict[str, Any]] = None

    for index, row in enumerate(rows):
        buy_signal = bool(row.get(buy_column))
        sell_signal = bool(row.get(sell_column))
        if buy_signal and sell_signal:
            continue

        if buy_signal:
            if position and position["direction"] == "short":
                exit_price = execution_price(row, "short", "exit")
                entry_price = float(position["entryprice"])
                trades.append(
                    BacktestTrade(
                        entrytickid=position["entrytickid"],
                        exittickid=int(row["tickid"]),
                        entryts=position["entryts"],
                        exitts=row["timestamp"],
                        direction="short",
                        entryprice=entry_price,
                        exitprice=exit_price,
                        pnl=entry_price - exit_price,
                        pnlpoints=entry_price - exit_price,
                        barsorticksheld=max(1, index - int(position["entryindex"])),
                        signalentrytype=position["signalentrytype"],
                        signalexittype=signalmode,
                    )
                )
                position = None
            if position is None:
                position = {
                    "direction": "long",
                    "entrytickid": int(row["tickid"]),
                    "entryts": row["timestamp"],
                    "entryprice": execution_price(row, "long", "entry"),
                    "entryindex": index,
                    "signalentrytype": signalmode,
                }
            continue

        if sell_signal:
            if position and position["direction"] == "long":
                exit_price = execution_price(row, "long", "exit")
                entry_price = float(position["entryprice"])
                trades.append(
                    BacktestTrade(
                        entrytickid=position["entrytickid"],
                        exittickid=int(row["tickid"]),
                        entryts=position["entryts"],
                        exitts=row["timestamp"],
                        direction="long",
                        entryprice=entry_price,
                        exitprice=exit_price,
                        pnl=exit_price - entry_price,
                        pnlpoints=exit_price - entry_price,
                        barsorticksheld=max(1, index - int(position["entryindex"])),
                        signalentrytype=position["signalentrytype"],
                        signalexittype=signalmode,
                    )
                )
                position = None
            if position is None:
                position = {
                    "direction": "short",
                    "entrytickid": int(row["tickid"]),
                    "entryts": row["timestamp"],
                    "entryprice": execution_price(row, "short", "entry"),
                    "entryindex": index,
                    "signalentrytype": signalmode,
                }

    if position and rows:
        last_row = rows[-1]
        direction = str(position["direction"])
        exit_price = execution_price(last_row, direction, "exit")
        entry_price = float(position["entryprice"])
        pnl = (exit_price - entry_price) if direction == "long" else (entry_price - exit_price)
        trades.append(
            BacktestTrade(
                entrytickid=position["entrytickid"],
                exittickid=int(last_row["tickid"]),
                entryts=position["entryts"],
                exitts=last_row["timestamp"],
                direction=direction,
                entryprice=entry_price,
                exitprice=exit_price,
                pnl=pnl,
                pnlpoints=pnl,
                barsorticksheld=max(1, len(rows) - 1 - int(position["entryindex"])),
                signalentrytype=position["signalentrytype"],
                signalexittype="final",
            )
        )

    gross_pnl = sum(trade.pnl for trade in trades)
    return {
        "tradecount": len(trades),
        "grosspnl": gross_pnl,
        "netpnl": gross_pnl,
        "trades": [trade.to_record() for trade in trades],
    }
