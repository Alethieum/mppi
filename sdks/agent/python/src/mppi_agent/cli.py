"""Onboard a supplied wallet and prepare the installed MPPI inference client."""

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
from .setup import DEFAULT_CONFIG, DEFAULT_RPC_ENV, onboard_circle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mppi-agent", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser(
        "onboard", help="check funding and complete ERC-8004 registration"
    )
    command.add_argument(
        "--profile", type=Path, help="verified deployment profile for a custom wallet factory"
    )
    command.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help="reusable agent configuration (default: .mppi/agent.json)",
    )
    command.add_argument(
        "--wallet-factory",
        metavar="MODULE:FUNCTION",
        help="your trusted integration; function(profile) returns the selected wallet",
    )
    command.add_argument(
        "--wallet", help="address of your authenticated, funded Circle Agent Wallet"
    )
    command.add_argument("--rpc-env", help="environment variable containing the Arc RPC URL")
    command.add_argument(
        "--testnet",
        action="store_true",
        help="select built-in Arc testnet setup, or require testnet in a custom trusted profile",
    )
    command.add_argument(
        "--agent-id", help="adopt an existing identity owned by the selected wallet"
    )
    args = parser.parse_args(argv)
    if args.wallet_factory is not None and args.profile is None:
        parser.error("--wallet-factory requires --profile")
    try:
        if args.wallet_factory is None:
            if not args.testnet or args.wallet is None or args.profile is not None:
                raise OnboardingError(
                    "use onboard --testnet --wallet ADDRESS for built-in Circle setup; custom profiles require --wallet-factory"
                )
            selected_id = None
            if args.agent_id is not None:
                if re.fullmatch(r"0|[1-9][0-9]{0,77}", args.agent_id) is None:
                    raise OnboardingError("agent id must be a canonical uint256 decimal string")
                selected_id = uint256(int(args.agent_id), field="agent_id")
            setup = onboard_circle(
                args.wallet,
                config_path=args.config,
                rpc_env=args.rpc_env or DEFAULT_RPC_ENV,
                agent_id=selected_id,
            )
            _print_result(setup.onboarding, setup.wallet_address, config=str(setup.config_path))
            return 0 if setup.onboarding.complete else 2
        if args.wallet is not None or args.profile is None:
            raise OnboardingError(
                "custom wallet factories require --profile and do not accept --wallet"
            )
        args.rpc_env = args.rpc_env or "MPPI_RPC_URL"
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
        _print_result(result, wallet.address)
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


def _print_result(result: object, wallet_address: bytes, *, config: str | None = None) -> None:
    from .onboarding import OnboardingResult

    selected = cast(OnboardingResult, result)
    operation = selected.operation
    value = {
        "complete": selected.complete,
        "agent_id": str(selected.identity.agent_id) if selected.identity is not None else None,
        "wallet": address_hex(wallet_address),
        "usdc_balance": str(selected.balance),
        "submitted": selected.submitted,
        "operation_ref": operation.operation_ref if operation is not None else None,
        "operation_state": operation.state.value if operation is not None else None,
        "tx_hash": "0x" + operation.tx_hash.hex()
        if operation is not None and operation.tx_hash is not None
        else None,
    }
    if config is not None:
        value["config"] = config
    print(json.dumps(value, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
