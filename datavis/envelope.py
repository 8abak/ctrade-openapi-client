from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple

from datavis.ott import select_price


ENVELOPE_SOURCES = ("ask", "bid", "mid")

DEFAULT_ENVELOPE_SOURCE = "mid"
DEFAULT_ENVELOPE_LENGTH = 500
DEFAULT_ENVELOPE_BANDWIDTH = 8.0
DEFAULT_ENVELOPE_MULT = 3.0


def safe_float_token(value: float) -> str:
    return ("{0:.8f}".format(float(value))).rstrip("0").rstrip(".") or "0"


@dataclass(frozen=True)
class EnvelopeConfig:
    source: str = DEFAULT_ENVELOPE_SOURCE
    length: int = DEFAULT_ENVELOPE_LENGTH
    bandwidth: float = DEFAULT_ENVELOPE_BANDWIDTH
    mult: float = DEFAULT_ENVELOPE_MULT

    def normalized(self) -> "EnvelopeConfig":
        source = (self.source or DEFAULT_ENVELOPE_SOURCE).lower()
        length = max(1, int(self.length))
        bandwidth = float(self.bandwidth)
        mult = float(self.mult)
        if source not in ENVELOPE_SOURCES:
            raise ValueError("Unsupported envelope source: {0}".format(source))
        if bandwidth <= 0:
            raise ValueError("Envelope bandwidth must be greater than zero.")
        if mult < 0:
            raise ValueError("Envelope multiplier must be non-negative.")
        return EnvelopeConfig(source=source, length=length, bandwidth=bandwidth, mult=mult)

    @property
    def error_length(self) -> int:
        return max(1, self.length - 1)

    @property
    def seed_tick_count(self) -> int:
        return self.length + self.error_length - 1

    def key(self) -> str:
        return "{0}:{1}:{2}:{3}".format(
            self.source,
            self.length,
            safe_float_token(self.bandwidth),
            safe_float_token(self.mult),
        )

    def worker_job_name(self, symbol: str) -> str:
        return "envelope:{0}:{1}:worker".format(symbol, self.key())

    def backfill_job_name(self, symbol: str, range_token: str) -> str:
        return "envelope:{0}:{1}:backfill:{2}".format(symbol, self.key(), range_token)


class EnvelopeCalculator:
    def __init__(self, config: EnvelopeConfig, state: Optional[Dict[str, Any]] = None):
        self.config = config.normalized()
        self.length = self.config.length
        self.error_length = self.config.error_length
        self.bandwidth = self.config.bandwidth
        self.mult = self.config.mult
        self.source_history: Deque[float] = deque(maxlen=self.length)
        self.error_history: Deque[float] = deque(maxlen=self.error_length)
        self.weights = [math.exp(-((float(index) * float(index)) / (self.bandwidth * self.bandwidth * 2.0))) for index in range(self.length)]
        self.denominator = sum(self.weights)
        if state:
            self.load_state(state)

    def load_state(self, state: Dict[str, Any]) -> None:
        self.source_history = deque((float(value) for value in state.get("sourcehistory", [])), maxlen=self.source_history.maxlen)
        self.error_history = deque((float(value) for value in state.get("errorhistory", [])), maxlen=self.error_history.maxlen)

    def snapshot_state(self) -> Dict[str, Any]:
        return {
            "sourcehistory": list(self.source_history),
            "errorhistory": list(self.error_history),
        }

    def process_value(
        self,
        *,
        tickid: int,
        symbol: str,
        timestamp: Any,
        price: float,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        src = float(price)
        self.source_history.append(src)

        basis = self._basis()
        mae = None
        upper = None
        lower = None

        if basis is not None:
            self.error_history.append(abs(src - basis))
            if len(self.error_history) >= self.error_length:
                mae = sum(self.error_history) / float(self.error_length)
                band = mae * self.mult
                upper = basis + band
                lower = basis - band

        record = {
            "tickid": tickid,
            "symbol": symbol,
            "source": self.config.source,
            "length": self.length,
            "bandwidth": self.bandwidth,
            "mult": self.mult,
            "timestamp": timestamp,
            "price": src,
            "basis": basis,
            "mae": mae,
            "upper": upper,
            "lower": lower,
        }
        if extra:
            record.update(extra)
        return record

    def process_tick(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self.process_value(
            tickid=int(row["id"]),
            symbol=row["symbol"],
            timestamp=row["timestamp"],
            price=float(select_price(row, self.config.source)),
        )

    def _basis(self) -> Optional[float]:
        if len(self.source_history) < self.length:
            return None
        values = list(self.source_history)
        numerator = sum(float(value) * float(weight) for value, weight in zip(reversed(values), self.weights))
        return numerator / float(self.denominator)


def compute_envelope_rows(
    rows: Sequence[Dict[str, Any]],
    config: EnvelopeConfig,
    state: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    calculator = EnvelopeCalculator(config, state=state)
    results = [calculator.process_tick(row) for row in rows]
    return results, calculator.snapshot_state()
