import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest
from mppi_agent.credentials import (
    CredentialError,
    create_session_signer,
    load_session_signer,
    private_directory,
)
from mppi_protocol.signing import LocalSessionSigner

KEY = (1).to_bytes(32, "big")  # Disclosed fixture, never a production credential.


def test_create_retains_private_signer_and_reruns_without_new_entropy(tmp_path: Path) -> None:
    path = tmp_path / "state" / "session.key"
    with patch("mppi_agent.credentials.secrets.token_bytes", return_value=KEY):
        signer = create_session_signer(path)
    assert path.read_bytes() == KEY
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    before = path.stat()
    with patch(
        "mppi_agent.credentials.secrets.token_bytes", side_effect=AssertionError("rotation")
    ):
        assert create_session_signer(path).address == signer.address
        assert load_session_signer(path, signer.address).address == signer.address
    assert path.stat().st_ino == before.st_ino
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert KEY.hex() not in repr(signer)


def test_load_missing_credential_never_creates_directories_or_key(tmp_path: Path) -> None:
    path = tmp_path / "absent" / "session.key"
    with pytest.raises(CredentialError):
        load_session_signer(path)
    assert not path.parent.exists()


@pytest.mark.parametrize("value", [b"", KEY[:-1], KEY + b"x", bytes(32), bytes([255]) * 32])
def test_invalid_retained_key_never_replaced(tmp_path: Path, value: bytes) -> None:
    path = tmp_path / "session.key"
    path.write_bytes(value)
    path.chmod(0o600)
    with patch(
        "mppi_agent.credentials.secrets.token_bytes", side_effect=AssertionError("rotation")
    ):
        for operation in (create_session_signer, load_session_signer):
            with pytest.raises(CredentialError):
                operation(path)
    assert path.read_bytes() == value


@pytest.mark.parametrize("kind", ["permissions", "symlink", "hardlink", "fifo", "directory"])
def test_unsafe_secret_file_is_rejected_without_replacement(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "session.key"
    original = tmp_path / "original.key"
    original.write_bytes(KEY)
    original.chmod(0o600)
    if kind == "symlink":
        path.symlink_to(original)
    elif kind == "hardlink":
        os.link(original, path)
    elif kind == "fifo":
        os.mkfifo(path, 0o600)
    elif kind == "directory":
        path.mkdir(mode=0o700)
    else:
        path.write_bytes(KEY)
        path.chmod(0o640)
    for operation in (create_session_signer, load_session_signer):
        with pytest.raises(CredentialError):
            operation(path)
    assert original.read_bytes() == KEY


@pytest.mark.parametrize(
    "kind", ["permissions", "symlink", "ancestor_symlink", "writable_ancestor", "parent_step"]
)
def test_unsafe_directory_path_is_rejected(tmp_path: Path, kind: str) -> None:
    parent = tmp_path / "private"
    parent.mkdir(mode=0o700)
    selected = parent
    if kind == "permissions":
        parent.chmod(0o750)
    elif kind == "writable_ancestor":
        parent.chmod(0o777)
        selected = parent / "nested"
    elif kind in ("symlink", "ancestor_symlink"):
        selected = tmp_path / "alias"
        selected.symlink_to(parent, target_is_directory=True)
        if kind == "ancestor_symlink":
            selected = selected / "nested"
    else:
        selected = parent / ".." / "other"
    for operation in (create_session_signer, load_session_signer):
        with pytest.raises(CredentialError):
            operation(selected / "session.key")
    assert not (parent / "session.key").exists()
    assert not (parent / "nested").exists()


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_wrong_owner_is_rejected(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "session.key"
    path.write_bytes(KEY)
    path.chmod(0o600)
    actual_stat = os.fstat

    def foreign_file(descriptor):
        info = actual_stat(descriptor)
        selected = path if kind == "file" else path.parent
        if info.st_ino == selected.stat().st_ino:
            fields = list(info)
            fields[4] = os.geteuid() + 1
            return os.stat_result(fields)
        return info

    with patch("mppi_agent.credentials.os.fstat", side_effect=foreign_file):
        with pytest.raises(CredentialError):
            load_session_signer(path)
    assert path.read_bytes() == KEY


def test_concurrent_creator_loads_exclusive_winner_without_replacement(tmp_path: Path) -> None:
    path = tmp_path / "session.key"
    winning_key = (2).to_bytes(32, "big")
    actual_open = os.open

    def concurrent_open(name, flags, mode=0o777, *, dir_fd=None):
        if name == path.name and flags & os.O_EXCL:
            descriptor = actual_open(name, flags, mode, dir_fd=dir_fd)
            try:
                os.write(descriptor, winning_key)
            finally:
                os.close(descriptor)
            raise FileExistsError
        return actual_open(name, flags, mode, dir_fd=dir_fd)

    with (
        patch("mppi_agent.credentials.secrets.token_bytes", return_value=KEY),
        patch("mppi_agent.credentials.os.open", side_effect=concurrent_open),
    ):
        signer = create_session_signer(path)
    assert path.read_bytes() == winning_key
    assert signer.address == LocalSessionSigner(winning_key).address


def test_expected_signer_mismatch_is_redacted(tmp_path: Path) -> None:
    path = tmp_path / "session.key"
    path.write_bytes(KEY)
    path.chmod(0o600)
    with pytest.raises(CredentialError) as raised:
        load_session_signer(path, b"x" * 20)
    assert KEY.hex() not in str(raised.value)
    assert path.read_bytes() == KEY


def test_failed_partial_write_keeps_incomplete_key_and_prevents_rotation(tmp_path: Path) -> None:
    path = tmp_path / "session.key"
    actual_write = os.write
    count = 0

    def fail_after_partial(descriptor, data):
        nonlocal count
        count += 1
        if count == 1:
            return actual_write(descriptor, data[:8])
        raise OSError("sensitive exception content")

    with (
        patch("mppi_agent.credentials.secrets.token_bytes", return_value=KEY),
        patch("mppi_agent.credentials.os.write", side_effect=fail_after_partial),
        pytest.raises(CredentialError) as raised,
    ):
        create_session_signer(path)
    assert "sensitive" not in str(raised.value)
    assert path.read_bytes() == KEY[:8]
    with pytest.raises(CredentialError):
        create_session_signer(path)
    assert path.read_bytes() == KEY[:8]


@pytest.mark.parametrize("failed_type", ["file", "directory"])
def test_fsync_failure_never_returns_signer_and_rerun_retains_key(
    tmp_path: Path, failed_type: str
) -> None:
    path = tmp_path / "session.key"
    actual_fsync = os.fsync

    def fail_selected(descriptor):
        directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        if directory == (failed_type == "directory"):
            raise OSError("sensitive disk error")
        actual_fsync(descriptor)

    with (
        patch("mppi_agent.credentials.secrets.token_bytes", return_value=KEY),
        patch("mppi_agent.credentials.os.fsync", side_effect=fail_selected),
        pytest.raises(CredentialError),
    ):
        create_session_signer(path)
    assert path.read_bytes() == KEY
    assert create_session_signer(path).address == LocalSessionSigner(KEY).address


def test_private_directory_yields_noninheritable_owned_descriptor(tmp_path: Path) -> None:
    path = tmp_path / "state"
    with private_directory(path, create=True) as descriptor:
        assert stat.S_ISDIR(os.fstat(descriptor).st_mode)
        assert not os.get_inheritable(descriptor)
    with pytest.raises(OSError):
        os.fstat(descriptor)
