"""Offline, execution-aware research tools for XAUUSD strategies."""

from __future__ import annotations

import dataclasses as _dataclasses
import sys as _sys
from functools import wraps as _wraps
from typing import Any as _Any
from typing import Callable as _Callable


_DataclassDecorator = _Callable[..., _Any]


def _without_dataclass_slots(original: _DataclassDecorator) -> _DataclassDecorator:
    """Make Python 3.9 ignore the Python 3.10-only ``slots`` option."""

    @_wraps(original)
    def compatible(
        cls: type[_Any] | None = None, /, **kwargs: _Any
    ) -> _Any:
        kwargs.pop("slots", None)
        if cls is None:
            return lambda selected: original(selected, **kwargs)
        return original(cls, **kwargs)

    return compatible


if _sys.version_info < (3, 10):
    _dataclasses.dataclass = _without_dataclass_slots(_dataclasses.dataclass)
