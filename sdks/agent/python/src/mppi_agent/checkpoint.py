"""Agent-owned local intent journal; no keys, prompts or provider storage.

An exclusive lock protects one client's checkpoint for its lifetime. Writes
replace the complete record atomically after fsync, before economic submission.
Python's Unix flock and os.replace/fsync semantics are required (macOS/Linux).
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from mppi_protocol.errors import ValidationCode, ValidationError

MAX_CHECKPOINT_BYTES = 64 << 20


def _invalid(message: str) -> ValidationError:
    return ValidationError(ValidationCode.INVALID_VALUE, message)


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _invalid("duplicate checkpoint field")
        result[key] = value
    return result


class Checkpoint:
    """One mode-0600, bounded, exclusively owned non-secret agent record."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path).absolute()
        self._lock: int | None = None
        try:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            lock_path = self.path.with_name(self.path.name + ".lock")
            fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            self._lock = fd
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise _invalid("checkpoint lock permissions must be 0600")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.load()
        except Exception:
            self.close()
            raise _invalid("checkpoint is unavailable, unsafe or already in use") from None

    def load(self) -> dict[str, Any] | None:
        if self._lock is None:
            raise _invalid("checkpoint is closed")
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        try:
            with os.fdopen(fd, "rb") as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                    raise _invalid("checkpoint permissions must be 0600")
                raw = source.read(MAX_CHECKPOINT_BYTES + 1)
            if len(raw) > MAX_CHECKPOINT_BYTES:
                raise _invalid("checkpoint exceeds its byte limit")
            result = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
            if (
                not isinstance(result, dict)
                or type(result.get("version")) is not int
                or result["version"] != 1
            ):
                raise _invalid("unsupported checkpoint record")
            return result
        except Exception:
            raise _invalid("checkpoint cannot be validated") from None

    def save(self, record: dict[str, Any]) -> None:
        if self._lock is None or type(record.get("version")) is not int or record["version"] != 1:
            raise _invalid("checkpoint is closed or has an invalid version")
        raw = json.dumps(record, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()
        if len(raw) > MAX_CHECKPOINT_BYTES:
            raise _invalid("checkpoint exceeds its byte limit")
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.path.parent, prefix="." + self.path.name, delete=False
            ) as target:
                temporary = target.name
                target.write(raw)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, self.path)
            temporary = None
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception:
            raise _invalid("checkpoint could not be durably written") from None
        finally:
            if temporary is not None:
                os.unlink(temporary)

    def close(self) -> None:
        if self._lock is not None:
            fd, self._lock = self._lock, None
            os.close(fd)
