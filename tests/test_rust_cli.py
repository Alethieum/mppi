"""Install only the public crate and exercise its command/process boundary.

Cargo package/install behavior was checked against the Cargo Book commands
documentation. These tests do not claim live Circle access or endpoint readiness.
"""

from __future__ import annotations

import json
import os
import subprocess
import tarfile
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CRATE = ROOT / "sdks/provider/rust"
VERSION = tomllib.loads((CRATE / "Cargo.toml").read_text())["package"]["version"]


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
    archive = Path(metadata["target_directory"]) / f"package/mppi-provider-{VERSION}.crate"
    with tarfile.open(archive) as package:
        license_file = package.extractfile(f"mppi-provider-{VERSION}/LICENSE")
        assert license_file is not None
        assert license_file.read() == (ROOT / "LICENSE").read_bytes()
        for network in ("mainnet", "testnet"):
            profile = package.extractfile(f"mppi-provider-{VERSION}/src/arc-{network}.json")
            assert profile is not None
            assert profile.read() == (ROOT / f"deployments/arc-{network}.json").read_bytes()
        package.extractall(work / "source", filter="data")
    source = work / f"source/mppi-provider-{VERSION}"
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
    assert run(binary, work, "--version").stdout.strip() == f"mppi {VERSION}"
    for command in ("onboard", "serve", "update", "verify"):
        help_result = run(binary, work, "provider", command, "--help")
        assert help_result.returncode == 0
        assert command in help_result.stdout
        invalid = run(binary, work, "provider", command, "--api-key", "secret-marker")
        assert invalid.returncode == 1
        assert "secret-marker" not in invalid.stdout + invalid.stderr
    assert not (work / "mppi.toml").exists()
    assert not list(work.glob("*.json"))


@pytest.mark.parametrize("network", ["mainnet", "testnet"])
@pytest.mark.parametrize("explicit_profile", [False, True])
@pytest.mark.parametrize("network_override", [False, True])
def test_installed_serve_directly_executes_explicit_provider_host(
    installed, network: str, explicit_profile: bool, network_override: bool
) -> None:
    binary, work = installed
    config = work / "provider config.toml"
    content = (ROOT / "examples/provider/mppi.toml").read_text()
    content = content.replace('network = "testnet"', f'network = "{network}"')
    content += '\n[listen]\nbind="127.0.0.1:8443"\ntls_mode="ingress"\n'
    config.write_text(content)
    profile = work / "profile.json"
    profile.write_bytes((ROOT / f"deployments/arc-{network}.json").read_bytes())
    host = work / "provider host"
    host.write_text(
        "#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\nsys.exit(37)\n"
    )
    host.chmod(0o700)
    expected = ["--config", str(config)]
    if explicit_profile:
        expected.extend(["--profile", str(profile)])
    expected.extend(["--upstream", "http://localhost:8000/private/v1"])
    if network_override:
        expected.append(f"--{network}")
    result = run(binary, work, "provider", "serve", *expected, "--host", str(host))
    assert result.returncode == 37
    assert json.loads(result.stdout) == expected
    assert config.read_text() == content
    assert not list(work.glob("*.wallet-*.json"))
