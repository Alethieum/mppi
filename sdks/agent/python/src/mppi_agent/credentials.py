"""Explicit local storage for the agent's fixed signer on macOS/Linux.

Keys are separate from public configuration and payment checkpoints. This
protects against other local users, not the account owner or privileged code.
Use a local filesystem with Unix permissions and durable fsync semantics.

Sources checked: Python 3.12 os.open (dir_fd), os.fsync and secrets.token_bytes:
https://docs.python.org/3.12/library/os.html#os.open
https://docs.python.org/3.12/library/os.html#os.fsync
https://docs.python.org/3.12/library/secrets.html#secrets.token_bytes
O_EXCL/O_NOFOLLOW and directory durability:
https://man7.org/linux/man-pages/man2/open.2.html
https://man7.org/linux/man-pages/man2/fsync.2.html
https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html
"""

from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from mppi_protocol.signing import LocalSessionSigner


class CredentialError(RuntimeError):
    """A fixed signer could not be safely retained or loaded; never rotate it."""


@contextmanager
def private_directory(path: Path, *, create: bool = False) -> Iterator[int]:
    """Open a private owned directory without following any path symlinks.

    Yield an owned directory descriptor for relative filesystem operations.
    Missing components are created only when explicitly requested. Existing
    ancestors may be root-owned; writable shared ancestors must be root-owned
    sticky directories. The final directory permits no group or other access.
    """
    descriptor: int | None = None
    try:
        selected = Path(path).absolute()
        if ".." in selected.parts or selected.anchor != "/":
            raise CredentialError("private directory path is invalid")
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        descriptor = os.open("/", flags)
        for part in selected.parts[1:]:
            info = os.fstat(descriptor)
            shared = info.st_mode & 0o022
            if info.st_uid not in (0, os.geteuid()) or (
                shared and not (info.st_uid == 0 and info.st_mode & stat.S_ISVTX)
            ):
                raise CredentialError("private directory ancestry is unsafe")
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(part, mode=0o700, dir_fd=descriptor)
                    os.fsync(descriptor)
                except FileExistsError:
                    pass
                child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        info = os.fstat(descriptor)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise CredentialError(
                "private directory must be owned and accessible only by this user"
            )
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        raise CredentialError("private directory is unavailable or unsafe") from None
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _read_signer(
    directory: int, name: str, expected_address: bytes | None, *, durable: bool = False
) -> LocalSessionSigner:
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
            or info.st_nlink != 1
            or info.st_size != 32
        ):
            raise CredentialError("session signer file is unsafe or incomplete")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            signer = LocalSessionSigner(source.read(33))
        if expected_address is not None and (
            type(expected_address) is not bytes or signer.address != expected_address
        ):
            raise CredentialError("session signer differs from its configured address")
        if durable:
            os.fsync(descriptor)
            os.fsync(directory)
        return signer
    finally:
        os.close(descriptor)


def load_session_signer(path: Path, expected_address: bytes | None = None) -> LocalSessionSigner:
    """Read one existing private 32-byte key; missing/invalid files never create keys."""
    try:
        selected = Path(path).absolute()
        with private_directory(selected.parent) as directory:
            return _read_signer(directory, selected.name, expected_address)
    except Exception:
        raise CredentialError(
            "session signer is unavailable, unsafe or differs from configuration"
        ) from None


def create_session_signer(path: Path) -> LocalSessionSigner:
    """Durably create one key, or load the original; never replace an existing file.

    A failed write leaves its file in place. An incomplete retained file is an
    error on rerun, never permission to generate a replacement signing identity.
    """
    try:
        selected = Path(path).absolute()
        with private_directory(selected.parent, create=True) as directory:
            try:
                return _read_signer(directory, selected.name, None, durable=True)
            except FileNotFoundError:
                pass
            key = secrets.token_bytes(32)
            signer = LocalSessionSigner(key)
            try:
                descriptor = os.open(
                    selected.name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=directory,
                )
            except FileExistsError:
                return _read_signer(directory, selected.name, None, durable=True)
            try:
                os.fchmod(descriptor, 0o600)
                remaining = key
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise CredentialError("session signer could not be retained")
                    remaining = remaining[written:]
                os.fsync(descriptor)
                os.fsync(directory)
            finally:
                os.close(descriptor)
            return signer
    except Exception:
        raise CredentialError("session signer could not be safely created or loaded") from None
