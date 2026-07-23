"""Bounded-memory, keyed temporary storage for fresh-research objects.

The spool writes each appended object as an independently framed, fast-zlib
compressed pickle in the stream for its key.  It keeps only key metadata in
memory, never the appended object, and permits only one key stream to be read
at a time.  Files are named from a SHA-256 digest of the key so a candidate
identifier can never become a path.
"""

from __future__ import annotations

import hashlib
import io
import errno
import os
import pickle
import secrets
import shutil
import struct
import tempfile
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import (
    Dict,
    Generator,
    Generic,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    Union,
)


T = TypeVar("T")

SPOOL_DIRECTORY_PREFIX = ".fresh-entry-spool-"
_RECORD_LENGTH = struct.Struct(">Q")
_STREAM_CHUNK_BYTES = 64 * 1024


def _write_all(destination, payload: bytes) -> None:
    view = memoryview(payload)
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if not isinstance(written, int) or written <= 0:
            raise OSError("short write while streaming an object spool record")
        offset += written


class SpoolCapacityError(OSError):
    """Raised before a configured spool payload budget can be exceeded."""

    def __init__(self, maximum_bytes: int) -> None:
        super().__init__(
            errno.ENOSPC,
            f"configured object spool byte limit of {maximum_bytes} would be exceeded",
        )
        self.maximum_bytes = maximum_bytes


class _CompressedPickleWriter:
    """Pickler sink that writes one zlib stream without whole-object buffers."""

    __slots__ = (
        "_compressor",
        "_destination",
        "_finished",
        "_maximum_compressed_bytes",
        "_reported_maximum_bytes",
        "compressed_bytes",
    )

    def __init__(
        self,
        destination,
        *,
        maximum_compressed_bytes: int | None = None,
        reported_maximum_bytes: int | None = None,
    ) -> None:
        self._compressor = zlib.compressobj(level=1)
        self._destination = destination
        self._finished = False
        self._maximum_compressed_bytes = maximum_compressed_bytes
        self._reported_maximum_bytes = (
            maximum_compressed_bytes
            if reported_maximum_bytes is None
            else reported_maximum_bytes
        )
        self.compressed_bytes = 0

    def _write_compressed(self, compressed: bytes) -> None:
        if not compressed:
            return
        maximum = self._maximum_compressed_bytes
        if maximum is not None and self.compressed_bytes + len(compressed) > maximum:
            assert self._reported_maximum_bytes is not None
            raise SpoolCapacityError(self._reported_maximum_bytes)
        _write_all(self._destination, compressed)
        self.compressed_bytes += len(compressed)

    def write(self, payload: bytes) -> int:
        if self._finished:
            raise ValueError("compressed pickle writer is already finished")
        self._write_compressed(self._compressor.compress(payload))
        return len(payload)

    def finish(self) -> int:
        if self._finished:
            raise ValueError("compressed pickle writer is already finished")
        self._write_compressed(self._compressor.flush())
        self._finished = True
        return self.compressed_bytes


class _BoundedZlibReader(io.RawIOBase):
    """Expose exactly one length-framed zlib record as a streaming reader."""

    def __init__(self, source, compressed_bytes: int) -> None:
        super().__init__()
        self._source = source
        self._remaining = compressed_bytes
        self._decompressor = zlib.decompressobj()
        self._unconsumed = b""
        self._finished = False

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        if self._finished:
            return 0
        view = memoryview(target).cast("B")
        if not view:
            return 0

        while True:
            if self._unconsumed:
                compressed = self._unconsumed
                self._unconsumed = b""
            elif self._remaining:
                requested = min(_STREAM_CHUNK_BYTES, self._remaining)
                compressed = self._source.read(requested)
                if len(compressed) != requested:
                    raise EOFError("compressed object spool record ended early")
                self._remaining -= requested
            else:
                if not self._decompressor.eof:
                    raise zlib.error("incomplete compressed object spool record")
                self._finished = True
                return 0

            output = self._decompressor.decompress(
                compressed,
                max_length=len(view),
            )
            self._unconsumed = self._decompressor.unconsumed_tail
            if self._decompressor.unused_data:
                raise zlib.error("compressed object spool record has trailing bytes")
            if output:
                view[: len(output)] = output
                return len(output)
            if self._decompressor.eof:
                if self._unconsumed or self._remaining:
                    raise zlib.error(
                        "compressed object spool record has trailing bytes"
                    )
                self._finished = True
                return 0


class SpoolStateError(RuntimeError):
    """Raised when the spool is used outside its supported lifecycle."""


class SpoolCorruptionError(RuntimeError):
    """Raised when a key stream does not match its in-memory inventory."""


class KeyedObjectSpool(Generic[T]):
    """Context-managed pickle spool with bounded, one-key-at-a-time reads."""

    def __init__(
        self,
        parent_directory: Optional[Union[str, Path]] = None,
        *,
        maximum_bytes: int | None = None,
    ) -> None:
        if maximum_bytes is not None and (
            not isinstance(maximum_bytes, int)
            or isinstance(maximum_bytes, bool)
            or maximum_bytes <= 0
        ):
            raise ValueError("maximum_bytes must be a positive integer or None")
        self._parent_directory = (
            None if parent_directory is None else Path(parent_directory).resolve()
        )
        self._maximum_bytes = maximum_bytes
        self._stored_bytes = 0
        self._directory: Optional[Path] = None
        self._created_parent: Optional[Path] = None
        self._filenames: Dict[str, str] = {}
        self._keys_by_filename: Dict[str, str] = {}
        self._object_counts: Dict[str, int] = {}
        self._active_key: Optional[str] = None
        self._active_iterator: Optional[Generator[T, None, None]] = None
        self._closed = False

    def __enter__(self) -> "KeyedObjectSpool[T]":
        if self._directory is not None or self._closed:
            raise SpoolStateError("a spool instance can be entered only once")
        parent = self._parent_directory
        if parent is None:
            parent = Path(tempfile.gettempdir()).resolve()
        if not parent.exists():
            raise FileNotFoundError(parent)
        if not parent.is_dir():
            raise NotADirectoryError(parent)

        # Atomic mkdir plus a cryptographically random suffix has the same
        # uniqueness property needed from a temporary-directory helper while
        # avoiding platform-specific ACL rewriting of its parent directory.
        for _ in range(100):
            directory = parent / f"{SPOOL_DIRECTORY_PREFIX}{secrets.token_hex(16)}"
            try:
                directory.mkdir(mode=(0o777 if os.name == "nt" else 0o700))
            except FileExistsError:
                continue
            self._created_parent = parent
            self._directory = directory.resolve()
            break
        else:
            raise FileExistsError("could not allocate a unique spool directory")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.close()
        return False

    @property
    def directory(self) -> Path:
        """Return the active temporary directory."""

        self._require_active()
        assert self._directory is not None
        return self._directory

    @property
    def keys(self) -> Tuple[str, ...]:
        """Return a deterministic, lexicographically ordered key inventory."""

        self._require_active()
        return tuple(sorted(self._object_counts))

    @property
    def inventory(self) -> Tuple[Tuple[str, int], ...]:
        """Return deterministic ``(key, object_count)`` inventory entries."""

        return tuple((key, self._object_counts[key]) for key in self.keys)

    @property
    def stored_bytes(self) -> int:
        """Return framed compressed bytes currently owned by this spool."""

        self._require_active()
        return self._stored_bytes

    @property
    def maximum_bytes(self) -> int | None:
        """Return the configured framed compressed-byte ceiling."""

        return self._maximum_bytes

    def register_key(self, key: str) -> None:
        """Register an empty key stream, retaining zero-object candidates."""

        self._require_writable()
        self._validate_key(key)
        if key in self._filenames:
            return
        filename = self._filename_for_key(key)
        existing_key = self._keys_by_filename.get(filename)
        if existing_key is not None and existing_key != key:
            raise RuntimeError("SHA-256 collision between spool keys")
        path = self.directory / filename
        path.touch(exist_ok=False)
        self._filenames[key] = filename
        self._keys_by_filename[filename] = key
        self._object_counts[key] = 0

    def append(self, key: str, value: T) -> None:
        """Append one object without deduplication or retaining its reference."""

        self._require_writable()
        self._validate_key(key)
        if key not in self._filenames:
            self.register_key(key)

        path = self.directory / self._filenames[key]
        with path.open("r+b") as destination:
            destination.seek(0, os.SEEK_END)
            record_start = destination.tell()
            try:
                remaining = (
                    None
                    if self._maximum_bytes is None
                    else self._maximum_bytes
                    - self._stored_bytes
                    - _RECORD_LENGTH.size
                )
                if remaining is not None and remaining < 0:
                    raise SpoolCapacityError(self._maximum_bytes)
                _write_all(destination, _RECORD_LENGTH.pack(0))
                writer = _CompressedPickleWriter(
                    destination,
                    maximum_compressed_bytes=remaining,
                    reported_maximum_bytes=self._maximum_bytes,
                )
                pickle.Pickler(
                    writer,
                    protocol=pickle.HIGHEST_PROTOCOL,
                ).dump(value)
                compressed_bytes = writer.finish()
                record_end = destination.tell()
                destination.seek(record_start)
                _write_all(
                    destination,
                    _RECORD_LENGTH.pack(compressed_bytes),
                )
                destination.seek(record_end)
            except BaseException:
                destination.seek(record_start)
                destination.truncate()
                raise
        self._stored_bytes += _RECORD_LENGTH.size + compressed_bytes
        self._object_counts[key] += 1

    @contextmanager
    def load(self, key: str) -> Iterator[Iterator[T]]:
        """Yield one key's objects in append order and exclude concurrent loads."""

        self._require_active()
        self._validate_key(key)
        if key not in self._filenames:
            raise KeyError(key)
        if self._active_key is not None:
            raise SpoolStateError("only one key may be loaded from a spool at a time")

        iterator = self._iter_key(key)
        self._active_key = key
        self._active_iterator = iterator
        try:
            yield iterator
        finally:
            iterator.close()
            if self._active_iterator is iterator:
                self._active_iterator = None
                self._active_key = None

    def count(self, key: str) -> int:
        """Return the number of objects appended for ``key``."""

        self._require_active()
        self._validate_key(key)
        try:
            return self._object_counts[key]
        except KeyError:
            raise KeyError(key) from None

    def close(self) -> None:
        """Close an active reader and remove the exact temporary directory."""

        if self._closed:
            return
        active_iterator = self._active_iterator
        if active_iterator is not None:
            active_iterator.close()
        self._active_iterator = None
        self._active_key = None

        directory = self._directory
        created_parent = self._created_parent
        try:
            if directory is not None and directory.exists():
                if (
                    created_parent is None
                    or directory.parent != created_parent
                    or not directory.name.startswith(SPOOL_DIRECTORY_PREFIX)
                    or directory.is_symlink()
                ):
                    raise SpoolStateError("refusing unsafe spool cleanup target")
                shutil.rmtree(directory)
        finally:
            self._directory = None
            self._created_parent = None
            self._filenames.clear()
            self._keys_by_filename.clear()
            self._object_counts.clear()
            self._stored_bytes = 0
            self._closed = True

    def _iter_key(self, key: str) -> Generator[T, None, None]:
        path = self.directory / self._filenames[key]
        expected_count = self._object_counts[key]
        with path.open("rb") as source:
            for ordinal in range(expected_count):
                header = source.read(_RECORD_LENGTH.size)
                if len(header) != _RECORD_LENGTH.size:
                    raise SpoolCorruptionError(
                        f"key {key!r} ended before object {ordinal + 1}"
                    )
                size = _RECORD_LENGTH.unpack(header)[0]
                try:
                    raw = _BoundedZlibReader(source, size)
                    with io.BufferedReader(
                        raw,
                        buffer_size=_STREAM_CHUNK_BYTES,
                    ) as buffered:
                        unpickler = pickle.Unpickler(buffered)
                        value = unpickler.load()
                        try:
                            unpickler.load()
                        except EOFError:
                            pass
                        else:
                            raise pickle.UnpicklingError(
                                "record contains more than one pickle"
                            )
                except (EOFError, pickle.PickleError, zlib.error) as error:
                    raise SpoolCorruptionError(
                        f"key {key!r} object {ordinal + 1} is corrupt"
                    ) from error
                yield value
            if source.read(1):
                raise SpoolCorruptionError(
                    f"key {key!r} contains bytes outside its inventory"
                )

    def _require_active(self) -> None:
        if self._closed:
            raise SpoolStateError("object spool is closed")
        if self._directory is None:
            raise SpoolStateError("object spool must be used as a context manager")

    def _require_writable(self) -> None:
        self._require_active()
        if self._active_key is not None:
            raise SpoolStateError("cannot append while a key is being loaded")

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str):
            raise TypeError("spool keys must be strings")

    @staticmethod
    def _filename_for_key(key: str) -> str:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return f"{digest}.pickle"


__all__ = [
    "KeyedObjectSpool",
    "SPOOL_DIRECTORY_PREFIX",
    "SpoolCapacityError",
    "SpoolCorruptionError",
    "SpoolStateError",
]
