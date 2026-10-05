"""Rebuild sdists, then check each SDK alone outside the source checkout."""

from __future__ import annotations

import os
import subprocess
import sys
import tarfile
import tempfile
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile


def check_wheel(wheel: Path, license_text: bytes) -> None:
    packages = ("mppi_agent", "mppi_provider", "mppi_protocol", "mppi")
    with ZipFile(wheel) as archive:
        for name in archive.namelist():
            top = name.split("/", 1)[0]
            if top not in packages and not top.endswith(".dist-info"):
                raise RuntimeError(f"Unexpected public wheel content: {wheel.name}: {name}")
        metadata_paths = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_paths) != 1:
            raise RuntimeError(f"Expected one metadata record: {wheel.name}")
        metadata_path = metadata_paths[0]
        metadata = BytesParser().parsebytes(archive.read(metadata_path))
        if metadata.get("License-Expression") != "Apache-2.0":
            raise RuntimeError(f"Missing Apache-2.0 license metadata: {wheel.name}")
        if metadata.get_all("License-File") != ["LICENSE"]:
            raise RuntimeError(f"Unexpected license-file metadata: {wheel.name}")
        license_path = metadata_path.removesuffix("METADATA") + "licenses/LICENSE"
        if archive.read(license_path) != license_text:
            raise RuntimeError(f"Packaged license differs from public license: {wheel.name}")


def main() -> None:
    directory = Path(sys.argv[1]).resolve()
    wheels = sorted(directory.glob("*.whl"))
    sdists = sorted(directory.glob("*.tar.gz"))
    if len(wheels) != 3 or len(sdists) != 3:
        raise RuntimeError("Expected two SDK wheels/sdists and one shared support wheel/sdist")
    public = Path(__file__).resolve().parents[1]
    license_text = (public / "LICENSE").read_bytes()
    testnet_profile = (public / "deployments/arc-testnet.json").read_bytes()
    for wheel in wheels:
        check_wheel(wheel, license_text)
    for sdist in sdists:
        with tarfile.open(sdist) as archive:
            license_file = archive.extractfile(sdist.name.removesuffix(".tar.gz") + "/LICENSE")
            if license_file is None or license_file.read() != license_text:
                raise RuntimeError(f"Packaged license differs from public license: {sdist.name}")
    with tempfile.TemporaryDirectory(prefix="mppi-installed-") as temporary:
        root = Path(temporary)
        rebuilt = root / "rebuilt"
        for sdist in sdists:
            subprocess.run(
                ["uv", "build", "--wheel", "--out-dir", str(rebuilt), str(sdist)],
                cwd=root,
                check=True,
            )
        for wheel in rebuilt.glob("*.whl"):
            check_wheel(wheel, license_text)
        support = next(rebuilt.glob("mppi_protocol-*.whl"))
        config = root / "mypy.ini"
        config.write_text("[mypy]\nstrict = True\n")
        for role, opposite in (("agent", "provider"), ("provider", "agent")):
            environment = root / role
            subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True)
            python = environment / "bin/python"
            wheel = next(rebuilt.glob(f"mppi_{role}-*.whl"))
            subprocess.run(
                ["uv", "pip", "install", "--python", str(python), str(support), str(wheel)],
                check=True,
            )
            code = f"""
from importlib.util import find_spec
from importlib.resources import files
assert find_spec('mppi_{opposite}') is None
import mppi_{role}
assert files('mppi_{role}').joinpath('arc-testnet.json').read_bytes() == {testnet_profile!r}
from mppi.v1 import mppi_pb2, mppi_pb2_grpc
from mppi_protocol.chain import ChainReader, HttpJsonRpc
from mppi_protocol.follower import ChainFollower
from mppi_protocol.contract import ContractCalls, decode_result, encode_call
from mppi_protocol.circle import CircleDeveloperWallet
from mppi_protocol.deployment import profile_from_mapping, profile_to_mapping
from mppi_protocol.registry import RegistryCalls, RegistryReader
from mppi_protocol.signing import LocalSessionSigner, session_id, verify_voucher
from mppi_protocol.types import Domain, SessionDescriptor
# Disclosed test key, never generated or retained by production configuration.
signer = LocalSessionSigner((1).to_bytes(32, 'big'))
domain = Domain(5042002, b'c' * 20)
descriptor = SessionDescriptor(b'a' * 20, signer.address, 2**200, b'r' * 20, b'o' * 20, bytes.fromhex('36' + '00'*19), b's'*32)
identifier = session_id(domain, descriptor)
verify_voucher(domain, descriptor, 5510000, signer.sign_voucher(domain, identifier, 5510000))
assert len(ContractCalls(domain, 'USDC', '2').release_idle(descriptor).call.data) == 4 + 7*32
assert mppi_pb2.Voucher(cumulative=0).HasField('cumulative')
assert mppi_pb2.DESCRIPTOR.package == 'mppi.v1'
print('{role} SDK runtime loads and signs independently from rebuilt installed artifacts')
"""
            runtime = "Client" if role == "agent" else "Mppi"
            code += f"\nfrom mppi_{role} import {runtime}\nassert callable({runtime})\n"
            if role == "agent":
                code += "\nfrom mppi_agent import onboard\nfrom mppi_agent.circle import CircleAgentWallet\nassert callable(onboard)\n"
            else:
                code += "\nfrom inspect import iscoroutinefunction\nfrom mppi_provider import SetupError\nassert callable(Mppi.from_config) and iscoroutinefunction(Mppi.from_config)\nassert issubclass(SetupError, RuntimeError)\n"
            subprocess.run([str(python), "-I", "-c", code], cwd=root, check=True)
            if role == "agent":
                subprocess.run(
                    [str(environment / "bin/mppi-agent"), "onboard", "--help"],
                    cwd=root,
                    check=True,
                )
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "mypy",
                    "--config-file",
                    str(config),
                    "--python-executable",
                    str(python),
                    "--no-incremental",
                    # --no-incremental still writes a cache; isolated package
                    # checks need none. Avoid SQLite cache I/O in small tmpfs.
                    # https://mypy.readthedocs.io/en/stable/command_line.html#cmdoption-mypy-cache-dir
                    "--cache-dir",
                    os.devnull,
                    "-c",
                    "from mppi.v1.mppi_pb2 import Voucher; from typing import assert_type; "
                    "from mppi_protocol.signing import LocalSessionSigner; "
                    "assert_type(Voucher(cumulative=0).cumulative, int); "
                    "assert_type(LocalSessionSigner((1).to_bytes(32, 'big')).address, bytes); "
                    f"from mppi_{role} import {runtime}; "
                    f"runtime_type: type[{runtime}] = {runtime}; "
                    f"assert_type(runtime_type, type[{runtime}])",
                ],
                cwd=root,
                check=True,
            )


if __name__ == "__main__":
    main()
