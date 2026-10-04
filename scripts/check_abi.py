"""Compare application and full implementation ABIs with the pinned compiler."""

import json
import subprocess
from pathlib import Path

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts"


def main() -> None:
    outputs = {}
    for name in ("IMPPI", "MPPI"):
        compiled = json.loads(
            subprocess.check_output(
                ["forge", "inspect", f"src/{name}.sol:{name}", "abi", "--json"], cwd=CONTRACTS
            )
        )
        recorded = json.loads((CONTRACTS / f"abi/{name}.json").read_text())
        if compiled != recorded:
            raise RuntimeError(f"Compiled {name} ABI differs from contracts/abi/{name}.json")
        outputs[name] = compiled
    if any(item not in outputs["MPPI"] for item in outputs["IMPPI"]):
        raise RuntimeError("MPPI implementation does not preserve the application ABI")
    packaged = CONTRACTS.parent / "proto/python/src/mppi_protocol/contract_abi.json"
    if json.loads(packaged.read_text()) != outputs["IMPPI"]:
        raise RuntimeError("Packaged Python ABI differs from the compiled application interface")
    rust_interface = CONTRACTS.parent / "sdks/provider/rust/src/abi/IMPPI.sol"
    if rust_interface.read_bytes() != (CONTRACTS / "src/IMPPI.sol").read_bytes():
        raise RuntimeError("Packaged Rust interface differs from contracts/src/IMPPI.sol")
    print("Compiled application and implementation ABIs match their checked-in artifacts")


if __name__ == "__main__":
    main()
