"""Install only the public crate and exercise its command/process boundary.

Cargo package/install behavior was checked against the Cargo Book commands
documentation. These tests do not claim live Circle access or endpoint readiness.
"""

from __future__ import annotations

import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "sdks/provider/rust"


@pytest.fixture(scope="module")
def installed(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    work = tmp_path_factory.mktemp("mppi-installed-rust")
    subprocess.run(
        ["cargo", "package", "--locked", "--allow-dirty", "--no-verify"],
        cwd=CRATE,
        check=True,
        capture_output=True,
        timeout=180,
    )
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=CRATE
        )
    )
    archive = Path(metadata["target_directory"]) / "package/mppi-provider-0.1.0.crate"
    with tarfile.open(archive) as package:
        license_file = package.extractfile("mppi-provider-0.1.0/LICENSE")
        assert license_file is not None
        assert license_file.read() == (ROOT / "LICENSE").read_bytes()
        package.extractall(work / "source", filter="data")
    source = work / "source/mppi-provider-0.1.0"
    # Reuse compiled dependencies, but compile the installed executable from the
    # extracted package. The source path is outside the original checkout.
    subprocess.run(
        [
            "cargo",
            "install",
            "--path",
            str(source),
            "--locked",
            "--debug",
            "--bin",
            "mppi",
            "--root",
            str(work / "installed"),
            "--target-dir",
            metadata["target_directory"],
        ],
        cwd=work,
        check=True,
        capture_output=True,
        timeout=600,
    )
    return work / "installed/bin/mppi", work


def run(binary: Path, directory: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment.pop("CIRCLE_API_KEY", None)
    environment.pop("CIRCLE_ENTITY_SECRET", None)
    return subprocess.run(
        [str(binary), *arguments],
        cwd=directory,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def test_installed_commands_reject_before_credentials_and_redact_arguments(installed) -> None:
    binary, work = installed
    assert run(binary, work, "--version").stdout.strip() == "mppi 0.1.0"
    for command in ("onboard", "serve", "update", "verify"):
        help_result = run(binary, work, "provider", command, "--help")
        assert help_result.returncode == 0
        assert command in help_result.stdout
        invalid = run(binary, work, "provider", command, "--api-key", "secret-marker")
        assert invalid.returncode == 1
        assert "secret-marker" not in invalid.stdout + invalid.stderr
    assert not (work / "mppi.toml").exists()
    assert not list(work.glob("*.json"))


def test_installed_serve_directly_executes_explicit_provider_host(installed) -> None:
    binary, work = installed
    config = work / "provider config.toml"
    config.write_text(
        (ROOT / "examples/provider/mppi.toml").read_text()
        + '\n[listen]\nbind="127.0.0.1:8443"\ntls_mode="ingress"\n'
    )

    def pin(value: int) -> dict[str, str]:
        return {"address": "0x" + f"{value:02x}" * 20, "code_hash": "0x" + "ab" * 32}

    profile = work / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "version": 1,
                "chain_id": 5042002,
                "proxy": pin(1),
                "implementation": pin(2),
                "registry": pin(3),
                "registry_implementation": pin(4),
            }
        )
    )
    host = work / "provider host"
    host.write_text(
        "#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\nsys.exit(37)\n"
    )
    host.chmod(0o700)
    result = run(
        binary,
        work,
        "provider",
        "serve",
        "--config",
        str(config),
        "--profile",
        str(profile),
        "--host",
        str(host),
        "--upstream",
        "http://localhost:8000/private/v1",
        "--testnet",
    )
    assert result.returncode == 37
    assert json.loads(result.stdout) == [
        "--config",
        str(config),
        "--profile",
        str(profile),
        "--upstream",
        "http://localhost:8000/private/v1",
        "--testnet",
    ]
    assert 'url = "http://127.0.0.1:8080/v1"' in config.read_text()
    assert not list(work.glob("*.wallet-*.json"))
