"""Exact, byte-bounded temporary storage for finite floating-point series.

The scorer appends values in causal order and asks only for ordered reads,
medians, and linear quantiles.  Primary values are stored as native IEEE-754
doubles.  Order statistics are computed with fixed-size stable sorted runs and
a k-way merge, so no operation maps or partitions an entire series in memory.
"""

from __future__ import annotations

import errno
import hashlib
import heapq
import math
import os
import secrets
import shutil
import tempfile
from array import array
from numbers import Real
from pathlib import Path
from typing import BinaryIO, Iterator

import numpy as np


NUMERIC_SPOOL_DIRECTORY_PREFIX = ".fresh-numeric-spool-"
_FLOAT_BYTES = 8
_BUFFER_VALUES = 4_096
_SORT_RUN_VALUES = 262_144
_RUN_READ_VALUES = 256
_MERGE_FAN_IN = 64

_Run = tuple[Path, int, int]


class NumericSpoolStateError(RuntimeError):
    """Raised when a numeric spool is used outside its supported lifecycle."""


class NumericSpoolCapacityError(OSError):
    """Raised before the configured owned-byte ceiling can be exceeded."""

    def __init__(self, maximum_bytes: int) -> None:
        super().__init__(
            errno.ENOSPC,
            (
                "configured numeric spool byte limit of "
                f"{maximum_bytes} would be exceeded"
            ),
        )
        self.maximum_bytes = maximum_bytes


def _write_all(destination: BinaryIO, payload: memoryview) -> None:
    view = payload.cast("B")
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if not isinstance(written, int) or written <= 0:
            raise OSError("short write while streaming numeric spool values")
        offset += written


def _write_array(destination: BinaryIO, values: array[float]) -> None:
    """Write an array completely; kept separate for fault-injection tests."""

    _write_all(destination, memoryview(values))


def _read_array_chunk(source: BinaryIO, maximum_values: int) -> array[float]:
    payload = source.read(maximum_values * _FLOAT_BYTES)
    if len(payload) % _FLOAT_BYTES:
        raise EOFError("numeric spool file ended within a floating-point value")
    result = array("d")
    result.frombytes(payload)
    return result


class _FloatRunReader:
    """Small buffered reader over one externally sorted run."""

    __slots__ = ("_buffer", "_file", "_index", "_remaining")

    def __init__(self, path: Path, value_count: int) -> None:
        self._file = path.open("rb")
        self._remaining = value_count
        self._buffer = array("d")
        self._index = 0

    def close(self) -> None:
        self._file.close()

    def next_value(self) -> float | None:
        if self._index >= len(self._buffer):
            if self._remaining == 0:
                if self._file.read(1):
                    raise RuntimeError("sorted numeric run contains trailing bytes")
                return None
            requested = min(_RUN_READ_VALUES, self._remaining)
            self._buffer = _read_array_chunk(self._file, requested)
            if len(self._buffer) != requested:
                raise EOFError("sorted numeric run ended early")
            self._remaining -= requested
            self._index = 0
        value = float(self._buffer[self._index])
        self._index += 1
        return value


class FloatSeriesSpool:
    """Context-managed, exact numeric series storage with a strict byte cap."""

    def __init__(
        self,
        parent_directory: str | Path | None,
        *,
        maximum_bytes: int,
    ) -> None:
        if (
            not isinstance(maximum_bytes, int)
            or isinstance(maximum_bytes, bool)
            or maximum_bytes <= 0
        ):
            raise ValueError("maximum_bytes must be a positive integer")
        self._parent_directory = (
            None if parent_directory is None else Path(parent_directory).resolve()
        )
        self._maximum_bytes = maximum_bytes
        self._stored_bytes = 0
        self._temporary_bytes = 0
        self._directory: Path | None = None
        self._created_parent: Path | None = None
        self._filenames: dict[str, str] = {}
        self._keys_by_filename: dict[str, str] = {}
        self._counts: dict[str, int] = {}
        self._buffers: dict[str, array[float]] = {}
        self._closed = False

    def __enter__(self) -> "FloatSeriesSpool":
        if self._directory is not None or self._closed:
            raise NumericSpoolStateError("a spool instance can be entered only once")
        if array("d").itemsize != _FLOAT_BYTES:
            raise RuntimeError("native C double is not an eight-byte value")
        if np.dtype(np.float64).itemsize != _FLOAT_BYTES:
            raise RuntimeError("NumPy float64 is not an eight-byte value")

        parent = self._parent_directory
        if parent is None:
            parent = Path(tempfile.gettempdir()).resolve()
        if not parent.exists():
            raise FileNotFoundError(parent)
        if not parent.is_dir():
            raise NotADirectoryError(parent)

        for _ in range(100):
            directory = (
                parent
                / f"{NUMERIC_SPOOL_DIRECTORY_PREFIX}{secrets.token_hex(16)}"
            )
            try:
                directory.mkdir(mode=(0o777 if os.name == "nt" else 0o700))
            except FileExistsError:
                continue
            self._created_parent = parent
            self._directory = directory
            break
        else:
            raise FileExistsError(
                "could not allocate a unique numeric spool directory"
            )
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.close()
        return False

    @property
    def directory(self) -> Path:
        self._require_active()
        assert self._directory is not None
        return self._directory

    @property
    def maximum_bytes(self) -> int:
        return self._maximum_bytes

    @property
    def stored_bytes(self) -> int:
        """Logical bytes in primary files and still-buffered primary values."""

        self._require_active()
        return self._stored_bytes

    @property
    def temporary_bytes(self) -> int:
        """Bytes currently owned by external-sort run files."""

        self._require_active()
        return self._temporary_bytes

    @property
    def owned_bytes(self) -> int:
        self._require_active()
        return self._stored_bytes + self._temporary_bytes

    @property
    def keys(self) -> tuple[str, ...]:
        self._require_active()
        return tuple(sorted(self._counts))

    def count(self, key: str) -> int:
        self._require_active()
        self._validate_key(key)
        return self._counts.get(key, 0)

    def append(self, key: str, value: Real) -> None:
        """Append one finite value without deduplication."""

        self._require_active()
        self._validate_key(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, Real)
            or not math.isfinite(float(value))
        ):
            raise ValueError("value must be a finite real number")
        converted = float(value)
        self._ensure_capacity(_FLOAT_BYTES)

        new_key = key not in self._counts
        if new_key:
            self._register_key(key)
        buffer = self._buffers[key]
        buffer.append(converted)
        self._counts[key] += 1
        self._stored_bytes += _FLOAT_BYTES
        try:
            if len(buffer) >= _BUFFER_VALUES:
                self._flush_key(key)
        except BaseException:
            # _flush_key is transactional and therefore still owns the buffer.
            removed = buffer.pop()
            assert removed == converted or (
                removed == 0.0 and converted == 0.0
            )
            self._counts[key] -= 1
            self._stored_bytes -= _FLOAT_BYTES
            if new_key and self._counts[key] == 0 and not buffer:
                path = self.directory / self._filenames[key]
                if path.exists():
                    path.unlink()
                filename = self._filenames.pop(key)
                self._keys_by_filename.pop(filename)
                self._counts.pop(key)
                self._buffers.pop(key)
            raise

    def values(self, key: str) -> Iterator[float]:
        """Yield one series in exact append order using bounded reads."""

        self._require_active()
        self._validate_key(key)
        if key not in self._counts:
            return
        self._flush_key(key)
        path = self.directory / self._filenames[key]
        remaining = self._counts[key]
        with path.open("rb") as source:
            while remaining:
                requested = min(_BUFFER_VALUES, remaining)
                chunk = _read_array_chunk(source, requested)
                if len(chunk) != requested:
                    raise EOFError("numeric spool series ended early")
                remaining -= requested
                for value in chunk:
                    yield float(value)
            if source.read(1):
                raise RuntimeError("numeric spool series contains trailing bytes")

    def median(self, key: str) -> float | None:
        """Return the exact ``statistics.median`` order statistic."""

        self._require_active()
        self._validate_key(key)
        count = self._counts.get(key, 0)
        if count == 0:
            return None
        midpoint = count // 2
        if count % 2:
            return self._select_ranks(key, (midpoint,))[midpoint]
        selected = self._select_ranks(key, (midpoint - 1, midpoint))
        return (selected[midpoint - 1] + selected[midpoint]) / 2

    def quantile(self, key: str, probability: float) -> float | None:
        """Return NumPy's exact linear quantile with bounded endpoint storage."""

        self._require_active()
        self._validate_key(key)
        if (
            isinstance(probability, bool)
            or not isinstance(probability, Real)
            or not math.isfinite(float(probability))
            or float(probability) < 0.0
            or float(probability) > 1.0
        ):
            raise ValueError("probability must be a finite number in [0, 1]")
        count = self._counts.get(key, 0)
        if count == 0:
            return None
        selected_probability = float(probability)
        position = selected_probability * (count - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        selected = self._select_ranks(key, (lower, upper))
        if lower == upper:
            return selected[lower]
        fraction = position - lower
        endpoints = np.asarray(
            (selected[lower], selected[upper]),
            dtype=np.float64,
        )
        return float(np.quantile(endpoints, fraction, method="linear"))

    def close(self) -> None:
        """Flush and securely remove this spool's exact private directory."""

        if self._closed:
            return
        if self._directory is None:
            self._closed = True
            return

        pending_error: BaseException | None = None
        pending_traceback = None
        try:
            self._flush_all()
        except BaseException as exc:
            pending_error = exc
            pending_traceback = exc.__traceback__

        try:
            self._remove_owned_directory()
        except BaseException as exc:
            if pending_error is None:
                pending_error = exc
                pending_traceback = exc.__traceback__
        finally:
            self._directory = None
            self._created_parent = None
            self._filenames.clear()
            self._keys_by_filename.clear()
            self._counts.clear()
            self._buffers.clear()
            self._stored_bytes = 0
            self._temporary_bytes = 0
            self._closed = True

        if pending_error is not None:
            raise pending_error.with_traceback(pending_traceback)

    def _select_ranks(self, key: str, ranks: tuple[int, ...]) -> dict[int, float]:
        count = self._counts[key]
        targets = tuple(sorted(set(ranks)))
        if not targets or targets[0] < 0 or targets[-1] >= count:
            raise ValueError("requested numeric-spool rank is out of range")
        if _MERGE_FAN_IN < 2:
            raise NumericSpoolStateError("external merge fan-in must be at least two")

        self._flush_key(key)
        if self._temporary_bytes != 0:
            raise NumericSpoolStateError(
                "a numeric order statistic cannot overlap temporary runs"
            )
        series_bytes = count * _FLOAT_BYTES
        initial_run_counts = [
            min(_SORT_RUN_VALUES, count - offset)
            for offset in range(0, count, _SORT_RUN_VALUES)
        ]
        merge_extra_bytes = self._merge_peak_extra_bytes(initial_run_counts)
        # Initial runs duplicate the selected primary series.  A bounded merge
        # pass temporarily also owns the output for its largest input group.
        self._ensure_capacity(series_bytes + merge_extra_bytes)
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        nonce = secrets.token_hex(8)
        runs: list[_Run] = []
        owned_runs: dict[Path, int] = {}
        readers: list[_FloatRunReader] = []
        try:
            path = self.directory / self._filenames[key]
            remaining = count
            with path.open("rb") as source:
                run_index = 0
                while remaining:
                    requested = min(_SORT_RUN_VALUES, remaining)
                    chunk = _read_array_chunk(source, requested)
                    if len(chunk) != requested:
                        raise EOFError("numeric spool series ended early")
                    remaining -= requested
                    # Python's sort is stable, including for numerically equal
                    # negative and positive zero values.
                    sorted_chunk = array("d", sorted(chunk))
                    run_path = (
                        self.directory
                        / f".sort-{digest}-{nonce}-initial-{run_index:08d}.f64"
                    )
                    runs.append(
                        self._write_complete_run(
                            run_path,
                            sorted_chunk,
                            owned_runs=owned_runs,
                        )
                    )
                    run_index += 1
                if source.read(1):
                    raise RuntimeError("numeric spool series contains trailing bytes")

            merge_pass = 0
            while len(runs) > _MERGE_FAN_IN:
                merged_runs: list[_Run] = []
                for group_index, offset in enumerate(
                    range(0, len(runs), _MERGE_FAN_IN)
                ):
                    group = runs[offset : offset + _MERGE_FAN_IN]
                    if len(group) == 1:
                        merged_runs.append(group[0])
                        continue
                    run_path = (
                        self.directory
                        / (
                            f".sort-{digest}-{nonce}-merge-{merge_pass:04d}-"
                            f"{group_index:08d}.f64"
                        )
                    )
                    merged_runs.append(
                        self._merge_run_group(
                            group,
                            run_path,
                            owned_runs=owned_runs,
                        )
                    )
                if len(merged_runs) >= len(runs):
                    raise RuntimeError("external numeric merge did not converge")
                runs = merged_runs
                merge_pass += 1

            heap: list[tuple[float, int]] = []
            for run_index, (run_path, run_count, _run_bytes) in enumerate(runs):
                reader = _FloatRunReader(run_path, run_count)
                readers.append(reader)
                value = reader.next_value()
                if value is None:
                    raise RuntimeError("sorted numeric run is unexpectedly empty")
                heapq.heappush(heap, (value, run_index))

            target_set = set(targets)
            selected: dict[int, float] = {}
            for rank in range(targets[-1] + 1):
                if not heap:
                    raise EOFError("external numeric merge ended early")
                value, run_index = heapq.heappop(heap)
                if rank in target_set:
                    selected[rank] = value
                following = readers[run_index].next_value()
                if following is not None:
                    heapq.heappush(heap, (following, run_index))
            if len(selected) != len(targets):
                raise EOFError("external numeric merge did not resolve every rank")
            return selected
        finally:
            for reader in readers:
                reader.close()
            cleanup_error: BaseException | None = None
            for run_path, run_bytes in tuple(owned_runs.items()):
                try:
                    self._discard_run(
                        (run_path, 0, run_bytes),
                        owned_runs=owned_runs,
                        missing_is_error=False,
                    )
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
            if self._temporary_bytes < 0:
                raise RuntimeError("numeric spool temporary-byte accounting underflow")
            if cleanup_error is not None:
                raise cleanup_error
            if owned_runs or self._temporary_bytes != 0:
                raise RuntimeError("numeric spool temporary-byte cleanup is incomplete")

    @staticmethod
    def _merge_peak_extra_bytes(initial_run_counts: list[int]) -> int:
        """Return the largest bounded merge output owned beside all input runs."""

        run_bytes = [count * _FLOAT_BYTES for count in initial_run_counts]
        peak = 0
        while len(run_bytes) > _MERGE_FAN_IN:
            merged_bytes: list[int] = []
            for offset in range(0, len(run_bytes), _MERGE_FAN_IN):
                group = run_bytes[offset : offset + _MERGE_FAN_IN]
                if len(group) == 1:
                    merged_bytes.append(group[0])
                    continue
                group_bytes = sum(group)
                peak = max(peak, group_bytes)
                merged_bytes.append(group_bytes)
            if len(merged_bytes) >= len(run_bytes):
                raise NumericSpoolStateError("external merge sizing did not converge")
            run_bytes = merged_bytes
        return peak

    def _write_complete_run(
        self,
        run_path: Path,
        values: array[float],
        *,
        owned_runs: dict[Path, int],
    ) -> _Run:
        run_bytes = len(values) * _FLOAT_BYTES
        self._ensure_capacity(run_bytes)
        try:
            with run_path.open("xb") as destination:
                _write_array(destination, values)
                destination.flush()
        except BaseException:
            self._remove_partial_run(run_path, owned_runs=owned_runs)
            raise
        if run_path.stat().st_size != run_bytes:
            self._remove_partial_run(run_path, owned_runs=owned_runs)
            raise OSError("sorted numeric run size changed after writing")
        self._temporary_bytes += run_bytes
        owned_runs[run_path] = run_bytes
        return run_path, len(values), run_bytes

    def _merge_run_group(
        self,
        runs: list[_Run],
        output_path: Path,
        *,
        owned_runs: dict[Path, int],
    ) -> _Run:
        expected_count = sum(run_count for _path, run_count, _bytes in runs)
        expected_bytes = expected_count * _FLOAT_BYTES
        self._ensure_capacity(expected_bytes)
        readers: list[_FloatRunReader] = []
        accounted_output_bytes = 0
        try:
            heap: list[tuple[float, int]] = []
            for run_index, (run_path, run_count, _run_bytes) in enumerate(runs):
                reader = _FloatRunReader(run_path, run_count)
                readers.append(reader)
                value = reader.next_value()
                if value is None:
                    raise RuntimeError("sorted numeric run is unexpectedly empty")
                heapq.heappush(heap, (value, run_index))

            written_count = 0
            output_buffer = array("d")
            with output_path.open("xb") as destination:
                while heap:
                    value, run_index = heapq.heappop(heap)
                    output_buffer.append(value)
                    written_count += 1
                    following = readers[run_index].next_value()
                    if following is not None:
                        heapq.heappush(heap, (following, run_index))
                    if len(output_buffer) >= _BUFFER_VALUES:
                        _write_array(destination, output_buffer)
                        flushed_bytes = len(output_buffer) * _FLOAT_BYTES
                        self._temporary_bytes += flushed_bytes
                        accounted_output_bytes += flushed_bytes
                        del output_buffer[:]
                if output_buffer:
                    _write_array(destination, output_buffer)
                    flushed_bytes = len(output_buffer) * _FLOAT_BYTES
                    self._temporary_bytes += flushed_bytes
                    accounted_output_bytes += flushed_bytes
                destination.flush()
            if written_count != expected_count:
                raise RuntimeError("bounded external merge changed its value count")
            if (
                accounted_output_bytes != expected_bytes
                or output_path.stat().st_size != expected_bytes
            ):
                raise OSError("bounded external merge output size changed")
            owned_runs[output_path] = expected_bytes
        except BaseException:
            self._temporary_bytes -= accounted_output_bytes
            self._remove_partial_run(output_path, owned_runs=owned_runs)
            raise
        finally:
            for reader in readers:
                reader.close()

        for run in runs:
            self._discard_run(
                run,
                owned_runs=owned_runs,
                missing_is_error=True,
            )
        return output_path, expected_count, expected_bytes

    def _remove_partial_run(
        self,
        run_path: Path,
        *,
        owned_runs: dict[Path, int],
    ) -> None:
        try:
            run_path.unlink()
        except FileNotFoundError:
            return
        except BaseException:
            # Keep exact accounting if a hostile filesystem prevents removal.
            if run_path.exists() and run_path not in owned_runs:
                partial_bytes = run_path.stat().st_size
                owned_runs[run_path] = partial_bytes
                self._temporary_bytes += partial_bytes
            raise

    def _discard_run(
        self,
        run: _Run,
        *,
        owned_runs: dict[Path, int],
        missing_is_error: bool,
    ) -> None:
        run_path, _run_count, run_bytes = run
        tracked_bytes = owned_runs.get(run_path)
        if tracked_bytes != run_bytes:
            raise RuntimeError("numeric run ownership accounting changed")
        missing = False
        try:
            run_path.unlink()
        except FileNotFoundError:
            missing = True
        except BaseException:
            raise
        else:
            if run_path.exists():
                raise OSError("numeric run still exists after successful unlink")
        owned_runs.pop(run_path)
        self._temporary_bytes -= run_bytes
        if missing and missing_is_error:
            raise RuntimeError("an owned numeric run disappeared before removal")

    def _register_key(self, key: str) -> None:
        filename = f"{hashlib.sha256(key.encode('utf-8')).hexdigest()}.f64"
        existing_key = self._keys_by_filename.get(filename)
        if existing_key is not None and existing_key != key:
            raise RuntimeError("SHA-256 collision between numeric spool keys")
        self._filenames[key] = filename
        self._keys_by_filename[filename] = key
        self._counts[key] = 0
        self._buffers[key] = array("d")

    def _flush_key(self, key: str) -> None:
        buffer = self._buffers[key]
        if not buffer:
            return
        path = self.directory / self._filenames[key]
        existed = path.exists()
        try:
            with path.open("a+b") as destination:
                destination.seek(0, os.SEEK_END)
                start = destination.tell()
                try:
                    _write_array(destination, buffer)
                    destination.flush()
                except BaseException:
                    destination.seek(start)
                    destination.truncate()
                    raise
        except BaseException:
            if not existed and path.exists():
                path.unlink()
            raise
        del buffer[:]

    def _flush_all(self) -> None:
        for key in tuple(self._buffers):
            self._flush_key(key)

    def _ensure_capacity(self, additional_bytes: int) -> None:
        if self._stored_bytes + self._temporary_bytes + additional_bytes > (
            self._maximum_bytes
        ):
            raise NumericSpoolCapacityError(self._maximum_bytes)

    def _remove_owned_directory(self) -> None:
        directory = self._directory
        parent = self._created_parent
        assert directory is not None
        if (
            parent is None
            or directory.parent != parent
            or not directory.name.startswith(NUMERIC_SPOOL_DIRECTORY_PREFIX)
            or directory.name == NUMERIC_SPOOL_DIRECTORY_PREFIX
            or directory.is_symlink()
        ):
            raise NumericSpoolStateError(
                "refusing to remove an unverified numeric spool directory"
            )
        if directory.exists():
            shutil.rmtree(directory)

    def _require_active(self) -> None:
        if self._closed:
            raise NumericSpoolStateError("numeric spool is closed")
        if self._directory is None:
            raise NumericSpoolStateError(
                "numeric spool must be used as a context manager"
            )

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
