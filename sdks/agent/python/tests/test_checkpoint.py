import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from mppi_agent.checkpoint import Checkpoint
from mppi_protocol.errors import ValidationError


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


def test_failed_replace_preserves_previous_economic_intent(tmp_path: Path) -> None:
    checkpoint = Checkpoint(tmp_path / "agent.json")
    try:
        checkpoint.save({"version": 1, "operation_ref": "original"})
        with patch("mppi_agent.checkpoint.os.replace", side_effect=OSError("disk")):
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
        path.write_text('{"version":2}')
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
