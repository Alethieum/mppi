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
