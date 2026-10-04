"""Generate the public Python bindings/descriptor with pinned grpcio-tools.

Run from the public root: uv run python scripts/generate.py [--check]
No service, registry upload or credentials are needed.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "proto"
OUTPUT = SOURCE / "python" / "src"
FILES = ("mppi/v1/mppi_pb2.py", "mppi/v1/mppi_pb2.pyi", "mppi/v1/mppi_pb2_grpc.py")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Fail on drift without modifying files"
    )
    args = parser.parse_args()
    version = importlib.metadata.version("grpcio-tools")
    if version != "1.84.0":
        raise SystemExit(f"Expected grpcio-tools 1.84.0, found {version}; sync the lockfile")
    with tempfile.TemporaryDirectory(prefix="mppi-generation-") as directory:
        generated = Path(directory)
        command = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"--proto_path={SOURCE}",
            f"--python_out={generated}",
            f"--pyi_out={generated}",
            f"--grpc_python_out={generated}",
            f"--descriptor_set_out={generated / 'mppi.v1.binpb'}",
            "--include_imports",
            str(SOURCE / "mppi/v1/mppi.proto"),
        ]
        subprocess.run(command, check=True)
        destinations = {name: OUTPUT / name for name in FILES}
        destinations["mppi.v1.binpb"] = SOURCE / "mppi.v1.binpb"
        changed = []
        for name, target in destinations.items():
            expected = generated / name
            if not target.exists() or target.read_bytes() != expected.read_bytes():
                changed.append(str(target.relative_to(ROOT)))
                if not args.check:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(expected, target)
        if args.check and changed:
            print("Generated artifacts differ:", *changed, sep="\n  ", file=sys.stderr)
            return 1
    print("Generated artifacts match" if args.check else "Generated Python bindings and descriptor")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
