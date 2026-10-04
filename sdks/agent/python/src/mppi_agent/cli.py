"""Minimal agent onboarding CLI; wallet login and credentials stay with the caller."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import sys
from pathlib import Path
from typing import cast

from mppi_protocol.capabilities import TransactionWallet
from mppi_protocol.chain import ChainReader, HttpJsonRpc
from mppi_protocol.deployment import load_profile
from mppi_protocol.discovery import address_hex
from mppi_protocol.registry import RegistryReader
from mppi_protocol.types import uint256

from .onboarding import OnboardingError, onboard


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mppi-agent", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser(
        "onboard", help="check funding and complete ERC-8004 registration"
    )
    command.add_argument(
        "--profile", type=Path, required=True, help="verified deployment profile JSON"
    )
    command.add_argument(
        "--config", type=Path, required=True, help="reusable non-secret agent configuration"
    )
    command.add_argument(
        "--wallet-factory",
        required=True,
        metavar="MODULE:FUNCTION",
        help="your trusted integration; function(profile) returns the selected wallet",
    )
    command.add_argument(
        "--rpc-env", default="MPPI_RPC_URL", help="environment variable containing the Arc RPC URL"
    )
    command.add_argument(
        "--testnet",
        action="store_true",
        help="require Arc testnet in the supplied trusted profile; RPC remains explicit",
    )
    command.add_argument(
        "--agent-id", help="adopt an existing identity owned by the selected wallet"
    )
    args = parser.parse_args(argv)
    try:
        if (
            re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*",
                args.wallet_factory,
            )
            is None
        ):
            raise OnboardingError("wallet factory must name a trusted module:function")
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.rpc_env) is None:
            raise OnboardingError("RPC environment variable name is invalid")
        endpoint = os.environ.get(args.rpc_env)
        if not endpoint:
            raise OnboardingError("configure the selected Arc RPC environment variable")
        identity = None
        if args.agent_id is not None:
            if re.fullmatch(r"0|[1-9][0-9]{0,77}", args.agent_id) is None:
                raise OnboardingError("agent id must be a canonical uint256 decimal string")
            identity = uint256(int(args.agent_id), field="agent_id")
        profile = load_profile(args.profile)
        if args.testnet and profile.domain.chain_id != 5042002:
            raise OnboardingError("--testnet requires an Arc testnet deployment profile")
        module, name = args.wallet_factory.split(":", 1)
        factory = getattr(importlib.import_module(module), name)
        wallet = cast(TransactionWallet, factory(profile))
        result = onboard(
            wallet=wallet,
            registry=RegistryReader(ChainReader(HttpJsonRpc(endpoint), profile)),
            config_path=args.config,
            agent_id=identity,
        )
        operation = result.operation
        print(
            json.dumps(
                {
                    "complete": result.complete,
                    "agent_id": str(result.identity.agent_id)
                    if result.identity is not None
                    else None,
                    "wallet": address_hex(wallet.address),
                    "usdc_balance": str(result.balance),
                    "submitted": result.submitted,
                    "operation_ref": operation.operation_ref if operation is not None else None,
                    "operation_state": operation.state.value if operation is not None else None,
                    "tx_hash": "0x" + operation.tx_hash.hex()
                    if operation is not None and operation.tx_hash is not None
                    else None,
                },
                separators=(",", ":"),
            )
        )
        return 0 if result.complete else 2
    except OnboardingError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        # Do not echo wallet/RPC credentials, imported service exceptions or data.
        print(
            "onboarding could not complete; check wallet access, deployment and RPC configuration; retained operations are reconciled on rerun",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
