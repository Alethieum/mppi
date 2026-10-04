"""Run local foundation checks. Execute with `uv run python scripts/check.py`."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUST = ROOT / "sdks/provider/rust/Cargo.toml"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report", type=Path, help="Write a machine-readable result outside sources"
    )
    args = parser.parse_args()
    results: list[dict[str, object]] = []
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"

    def check(name: str, command: list[str], cwd: Path = ROOT) -> None:
        print(f"\n>>> {name}", flush=True)
        started = time.monotonic()
        completed = subprocess.run(command, cwd=cwd, env=env, check=False)
        results.append(
            {
                "name": name,
                "command": command,
                "returncode": completed.returncode,
                "seconds": round(time.monotonic() - started, 3),
            }
        )
        if completed.returncode:
            raise subprocess.CalledProcessError(completed.returncode, command)

    # An interrupt or unexpected exception must never leave successful evidence.
    failed = True
    try:
        with tempfile.TemporaryDirectory(prefix="mppi-check-") as directory:
            temporary = Path(directory)
            check("Python generated-code drift", [sys.executable, "scripts/generate.py", "--check"])
            check("Protobuf lint", ["buf", "lint"])
            check("Protobuf formatting", ["buf", "format", "--diff", "--exit-code"])
            check("Python lint", ["ruff", "check", "."])
            check("Python formatting", ["ruff", "format", "--check", "."])
            check("Python types", ["mypy"])
            check("Python and wire conformance", ["pytest", "-q"])
            check(
                "Rust formatting", ["cargo", "fmt", "--manifest-path", str(RUST), "--", "--check"]
            )
            check(
                "Rust lint",
                [
                    "cargo",
                    "clippy",
                    "--manifest-path",
                    str(RUST),
                    "--locked",
                    "--all-targets",
                    "--all-features",
                    "--",
                    "-D",
                    "warnings",
                ],
            )
            check(
                "Rust conformance",
                [
                    "cargo",
                    "test",
                    "--manifest-path",
                    str(RUST),
                    "--locked",
                    "--all-targets",
                    "--all-features",
                ],
            )
            generated = temporary / "rust-generated"
            generated.mkdir()
            check(
                "Rust regeneration",
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(RUST),
                    "--locked",
                    "--features",
                    "codegen",
                    "--example",
                    "generate",
                    "--",
                    str(ROOT / "proto"),
                    str(generated),
                ],
            )
            expected = RUST.parent / "src/generated/mppi.v1.rs"
            if (generated / "mppi.v1.rs").read_bytes() != expected.read_bytes():
                raise RuntimeError("Generated Rust bindings differ from the canonical schema")
            check("Solidity formatting", ["forge", "fmt", "--check"], ROOT / "contracts")
            check("Solidity conformance", ["forge", "test", "-vv"], ROOT / "contracts")
            check("Compiled ABI drift", [sys.executable, "scripts/check_abi.py"])
            check(
                "Python distribution builds",
                ["uv", "build", "--all-packages", "--out-dir", str(temporary / "dist")],
            )
            check(
                "Rust independent package build",
                ["cargo", "package", "--manifest-path", str(RUST), "--locked", "--allow-dirty"],
            )
            check(
                "Installed Python distributions",
                [sys.executable, "scripts/check_wheels.py", str(temporary / "dist")],
            )
            requirements = temporary / "requirements.txt"
            check(
                "Export locked runtime dependencies",
                [
                    "uv",
                    "export",
                    "--locked",
                    "--all-packages",
                    "--no-emit-workspace",
                    "--no-dev",
                    "--format",
                    "requirements-txt",
                    "--output-file",
                    str(requirements),
                    "--quiet",
                ],
            )
            check(
                "Python runtime dependency audit",
                ["pip-audit", "--disable-pip", "--no-deps", "-r", str(requirements)],
            )
        failed = False
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"\nFAILED: {error}", file=sys.stderr)
        failed = True
    finally:
        report = {
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "status": "FAIL" if failed else "PASS",
            "scope": "Local foundation generation, language conformance and independent package builds. No deployed payment or wallet integration claim.",
            "checks": results,
        }
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(f"\n{report['status']}: {len(results)} checks executed", flush=True)
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
