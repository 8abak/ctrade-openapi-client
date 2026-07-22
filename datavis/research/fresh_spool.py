"""Bounded-memory, keyed temporary storage for fresh-research objects.

The spool writes each appended object as an independently framed, fast-zlib
compressed pickle in the stream for its key.  It keeps only key metadata in
memory, never the appended object, and permits only one key stream to be read
at a time.  Files are named from a SHA-256 digest of the key so a candidate
identifier can never become a path.
"""

from __future__ import annotations

import hashlib
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


class SpoolStateError(RuntimeError):
    """Raised when the spool is used outside its supported lifecycle."""


class SpoolCorruptionError(RuntimeError):
    """Raised when a key stream does not match its in-memory inventory."""


class KeyedObjectSpool(Generic[T]):
    """Context-managed pickle spool with bounded, one-key-at-a-time reads."""

    def __init__(
        self,
        parent_directory: Optional[Union[str, Path]] = None,
    ) -> None:
        self._parent_directory = (
            None if parent_directory is None else Path(parent_directory).resolve()
        )
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

        # Serialise before opening the stream.  The byte payload, and therefore
        # every reference held by Pickler, becomes unreachable when this call
        # returns; the spool stores only the key metadata below.
        serialized = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        payload = zlib.compress(serialized, level=1)
        record = _RECORD_LENGTH.pack(len(payload)) + payload
        path = self.directory / self._filenames[key]
        with path.open("ab") as destination:
            written = destination.write(record)
        if written != len(record):
            raise OSError("short write while appending to object spool")
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
                compressed = source.read(size)
                if len(compressed) != size:
                    raise SpoolCorruptionError(
                        f"key {key!r} ended inside object {ordinal + 1}"
                    )
                try:
                    value = pickle.loads(zlib.decompress(compressed))
                except (pickle.PickleError, zlib.error) as error:
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
    "SpoolCorruptionError",
    "SpoolStateError",
]
