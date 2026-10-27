import json
import os
import threading
from pathlib import Path
from unittest.mock import patch

import pytest
from mppi_protocol.errors import ValidationError
from mppi_protocol.local_state import Checkpoint


def test_checkpoint_persists_nonsecret_intent_and_excludes_another_owner(tmp_path: Path) -> None:
    path = tmp_path / "agent.json"
    checkpoint = Checkpoint(path)
    try:
        assert checkpoint.load() is None
        record = {
            "version": 1,
            "signer_reference": "secret-manager/session-key",
            "operation_ref": "open-1",
        }
        checkpoint.save(record)
        assert checkpoint.load() == record
        assert path.stat().st_mode & 0o777 == 0o600
        with pytest.raises(ValidationError):
            Checkpoint(path)
    finally:
        checkpoint.close()
    reopened = Checkpoint(path)
    assert reopened.load() == record
    reopened.close()


def test_a_lock_held_only_by_an_inherited_copy_is_awaited(tmp_path: Path) -> None:
    path = tmp_path / "agent.json"
    checkpoint = Checkpoint(path)
    assert checkpoint._lock is not None
    # A child process spawned meanwhile holds a copy until it execs.
    inherited = os.dup(checkpoint._lock)
    checkpoint.close()
    exec_ = threading.Timer(0.05, os.close, (inherited,))
    exec_.start()
    try:
        Checkpoint(path).close()
    finally:
        exec_.join()


def test_a_lock_another_owner_holds_is_awaited_up_to_the_wait_given(tmp_path: Path) -> None:
    path = tmp_path / "agent.json"
    holder = Checkpoint(path)
    release = threading.Timer(1.5, holder.close)
    release.start()
    try:
        # Past the default second, within the wait given.
        Checkpoint(path, lock_wait=10).close()
    finally:
        release.join()
    for wait in (-1, float("nan"), float("inf"), True, "1"):
        with pytest.raises(ValidationError):
            Checkpoint(path, lock_wait=wait)  # type: ignore[arg-type]


def test_failed_replace_preserves_previous_economic_intent(tmp_path: Path) -> None:
    checkpoint = Checkpoint(tmp_path / "agent.json")
    try:
        checkpoint.save({"version": 1, "operation_ref": "original"})
        with patch("mppi_protocol.local_state.os.replace", side_effect=OSError("disk")):
            with pytest.raises(ValidationError):
                checkpoint.save({"version": 1, "operation_ref": "replacement"})
        assert checkpoint.load() == {"version": 1, "operation_ref": "original"}
        assert not list(tmp_path.glob(".agent.json*"))
    finally:
        checkpoint.close()


@pytest.mark.parametrize(
    "kind", ["duplicate", "version", "boolean_version", "permissions", "symlink", "fifo"]
)
def test_unsafe_or_invalid_existing_checkpoint_fails_closed(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "agent.json"
    path.write_text(json.dumps({"version": 1}))
    path.chmod(0o600)
    if kind == "duplicate":
        path.write_text('{"version":1,"operation_ref":"a","operation_ref":"b"}')
    elif kind == "version":
        path.write_text('{"version":3}')  # 1 and 2 are the supported formats.
    elif kind == "boolean_version":
        path.write_text('{"version":true}')
    elif kind == "permissions":
        path.chmod(0o644)
    elif kind == "symlink":
        original = tmp_path / "target.json"
        os.replace(path, original)
        path.symlink_to(original)
    else:
        path.unlink()
        os.mkfifo(path, 0o600)
    with pytest.raises(ValidationError):
        Checkpoint(path)
