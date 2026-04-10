from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Optional


def resolve_creds_file(base_dir: Optional[Path] = None) -> Path:
    env_creds_file = (os.getenv("DATAVIS_CTRADER_CREDS_FILE") or "").strip()
    if env_creds_file:
        return Path(env_creds_file).expanduser().resolve()
    if base_dir is not None:
        return (Path(base_dir) / "creds.json").resolve()
    return Path("~/cTrade/creds.json").expanduser().resolve()


def read_creds_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def token_tail(token: Optional[str], *, keep: int = 6) -> Optional[str]:
    value = str(token or "").strip()
    if not value:
        return None
    return f"...{value[-max(1, int(keep)):]}"


@contextmanager
def locked_creds_file(path: Path) -> Iterator[None]:
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+b") as lock_handle:
        _lock_file(lock_handle)
        try:
            yield
        finally:
            _unlock_file(lock_handle)


def write_creds_file_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    temp_path: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
            temp_path = handle.name
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass
            except Exception:
                pass


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    directory_fd: Optional[int] = None
    try:
        directory_fd = os.open(str(path), os.O_RDONLY)
        os.fsync(directory_fd)
    except Exception:
        return
    finally:
        if directory_fd is not None:
            try:
                os.close(directory_fd)
            except Exception:
                pass


def _lock_file(handle: Any) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        if handle.tell() == 0 and handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


def _unlock_file(handle: Any) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
