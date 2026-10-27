"""Incomplete verification must not produce successful build evidence."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SPEC = importlib.util.spec_from_file_location(
    "mppi_checks", Path(__file__).resolve().parents[1] / "scripts/check.py"
)
assert SPEC is not None and SPEC.loader is not None
CHECKS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKS)


@pytest.mark.parametrize("completed", [0, 2])
@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(0), ValueError("unexpected")])
def test_unexpected_exit_never_reports_success(
    tmp_path: Path, completed: int, error: BaseException
) -> None:
    report = tmp_path / "report.json"
    effects = [subprocess.CompletedProcess([], 0) for _ in range(completed)] + [error]
    with (
        patch.object(sys, "argv", ["check.py", "--report", str(report)]),
        patch.object(CHECKS.subprocess, "run", side_effect=effects),
        pytest.raises(type(error)),
    ):
        CHECKS.main()
    evidence = json.loads(report.read_text())
    assert evidence["status"] == "FAIL"
    assert len(evidence["checks"]) == completed


@pytest.mark.parametrize("failure", [OSError("unavailable"), subprocess.CompletedProcess([], 7)])
def test_handled_failures_report_failure(tmp_path: Path, failure: object) -> None:
    report = tmp_path / "report.json"
    with (
        patch.object(sys, "argv", ["check.py", "--report", str(report)]),
        patch.object(CHECKS.subprocess, "run", side_effect=[failure]),
    ):
        assert CHECKS.main() == 1
    assert json.loads(report.read_text())["status"] == "FAIL"


def test_rust_audit_runs_where_cargo_audit_reads_the_crate_exceptions() -> None:
    calls: list[tuple[list[str], Path]] = []

    def run(command: list[str], **options: object) -> subprocess.CompletedProcess[bytes]:
        calls.append((command, Path(str(options["cwd"]))))
        if "--example" in command:
            canonical = CHECKS.RUST.parent / "src/generated/mppi.v1.rs"
            (Path(command[-1]) / "mppi.v1.rs").write_bytes(canonical.read_bytes())
        return subprocess.CompletedProcess(command, 0)

    with (
        patch.object(sys, "argv", ["check.py"]),
        patch.object(CHECKS.subprocess, "run", side_effect=run),
    ):
        assert CHECKS.main() == 0
    command, cwd = calls[-1]
    assert command == ["cargo", "audit", "--file", str(CHECKS.RUST.with_name("Cargo.lock"))]
    # cargo-audit 0.22.2 reads ./.cargo/audit.toml from its working directory.
    assert (cwd / ".cargo/audit.toml").is_file()
