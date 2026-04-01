from __future__ import annotations

from dataclasses import dataclass

from datavis.envelope import (
    DEFAULT_ENVELOPE_BANDWIDTH,
    DEFAULT_ENVELOPE_LENGTH,
    DEFAULT_ENVELOPE_MULT,
    safe_float_token,
)
from datavis.zigzag import ZIG_LEVELS


DEFAULT_ENVELOPE_ZIG_LEVEL = "micro"


@dataclass(frozen=True)
class EnvelopeZigConfig:
    level: str = DEFAULT_ENVELOPE_ZIG_LEVEL
    length: int = DEFAULT_ENVELOPE_LENGTH
    bandwidth: float = DEFAULT_ENVELOPE_BANDWIDTH
    mult: float = DEFAULT_ENVELOPE_MULT

    def normalized(self) -> "EnvelopeZigConfig":
        level = (self.level or DEFAULT_ENVELOPE_ZIG_LEVEL).lower()
        length = max(1, int(self.length))
        bandwidth = float(self.bandwidth)
        mult = float(self.mult)
        if level not in ZIG_LEVELS:
            raise ValueError("Unsupported zig envelope level: {0}".format(level))
        if bandwidth <= 0:
            raise ValueError("Envelope bandwidth must be greater than zero.")
        if mult < 0:
            raise ValueError("Envelope multiplier must be non-negative.")
        return EnvelopeZigConfig(level=level, length=length, bandwidth=bandwidth, mult=mult)

    def key(self) -> str:
        return "{0}:{1}:{2}:{3}".format(
            self.level,
            self.length,
            safe_float_token(self.bandwidth),
            safe_float_token(self.mult),
        )

    def worker_job_name(self, symbol: str) -> str:
        return "envelopezig:{0}:{1}:worker".format(symbol, self.key())

    def backfill_job_name(self, symbol: str, range_token: str) -> str:
        return "envelopezig:{0}:{1}:backfill:{2}".format(symbol, self.key(), range_token)
