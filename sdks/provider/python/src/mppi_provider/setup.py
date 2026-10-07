"""Read-only composition from confirmed provider configuration.

The provider supplies authenticated Circle CLI access, trusted deployment
selection and execution integration. Restore the native CLI's original wallet
operation record; composition creates no wallet, identity or retained state.
Circle CLI 1.1.4 wallet/list and transaction evidence are checked by the shared
adapter in mppi_protocol.circle. USDC checks follow the same canonical profile.
"""

from __future__ import annotations

import os
import re
from importlib.resources import files
from pathlib import Path

from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile, HttpJsonRpc
from mppi_protocol.circle import CircleAgentWallet, supported_account_pins
from mppi_protocol.contract import ARC_USDC, ContractCalls, _decode_exact, _hash
from mppi_protocol.deployment import profile_from_mapping
from mppi_protocol.discovery import parse_document

from .config import ProviderConfig


class SetupError(RuntimeError):
    """Setup cannot establish the selected configuration; messages exclude secrets."""


def _profile(
    config: ProviderConfig, testnet: bool, profile: DeploymentProfile | None
) -> DeploymentProfile:
    if type(testnet) is not bool:
        raise SetupError("testnet must be an explicit boolean")
    if config.chain.network != ("testnet" if testnet else "mainnet"):
        raise SetupError("provider configuration differs from the selected network")
    if profile is None:
        name = "arc-testnet.json" if testnet else "arc-mainnet.json"
        profile = profile_from_mapping(
            parse_document(files("mppi_provider").joinpath(name).read_bytes())
        )
    if (
        not isinstance(profile, DeploymentProfile)
        or profile.domain.chain_id != config.chain.chain_id
    ):
        raise SetupError("deployment profile differs from the selected network")
    return profile


def _rpc(config: ProviderConfig, rpc_env: str | None) -> HttpJsonRpc:
    if rpc_env is None:
        return HttpJsonRpc(config.chain.rpc_url)
    if type(rpc_env) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", rpc_env) is None:
        raise SetupError("RPC selection must name an environment variable")
    endpoint = os.environ.get(rpc_env)
    if not endpoint:
        raise SetupError("configure the selected RPC environment variable")
    return HttpJsonRpc(endpoint)


def _wallet(
    config: ProviderConfig, reader: ChainReader, config_path: str | Path
) -> CircleAgentWallet:
    path = Path(config_path)
    try:
        proxy_hash, implementation = supported_account_pins(config.chain.chain_id)
        return CircleAgentWallet(
            chain=reader,
            wallet=CodePin(config.identity.wallet_address, proxy_hash),
            implementation=implementation,
            checkpoint_path=path.with_name(path.name + ".wallet-operations.json"),
            require_existing=True,
        )
    except Exception:
        raise SetupError(
            "restore the original Circle wallet operation record for this configuration"
        ) from None


def _verify_usdc(reader: ChainReader) -> None:
    """Verify fixed Arc USDC/2/6 and the actual separator at one canonical block."""
    block = reader.block()
    reader.verify_deployment(block)
    for function, output, expected in (
        ("name()", "string", "USDC"),
        ("version()", "string", "2"),
        ("decimals()", "uint8", 6),
    ):
        raw = reader._call(ARC_USDC, _hash(function.encode("ascii"))[:4], block)
        if _decode_exact([output], raw) != (expected,):
            raise SetupError("Arc USDC authorization domain differs from supported values")
    expected_separator = _hash(
        _hash(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
        + _hash(b"USDC")
        + _hash(b"2")
        + reader.profile.domain.chain_id.to_bytes(32, "big")
        + ARC_USDC.rjust(32, b"\0")
    )
    if reader._call(ARC_USDC, _hash(b"DOMAIN_SEPARATOR()")[:4], block) != expected_separator:
        raise SetupError("Arc USDC authorization domain separator is inconsistent")
    reader._check_canonical(block)


def _prepare(
    config_path: str | Path,
    *,
    testnet: bool,
    profile: DeploymentProfile | None,
    rpc_env: str | None,
) -> tuple[ProviderConfig, ChainReader, CircleAgentWallet, ContractCalls]:
    """Blocking configuration and verification; creates no event-loop objects."""
    config = ProviderConfig.from_file(config_path)
    try:
        selected = _profile(config, testnet, profile)
        rpc = _rpc(config, rpc_env)
        reader = ChainReader(rpc, selected)
        wallet = _wallet(config, reader, config_path)
    except SetupError:
        raise
    except Exception:
        raise SetupError("provider setup configuration is unavailable or invalid") from None
    try:
        wallet.verify_wallet()
    except Exception:
        raise SetupError("Circle wallet verification failed") from None
    try:
        _verify_usdc(reader)
    except SetupError:
        raise
    except Exception:
        raise SetupError("deployment or USDC verification failed") from None
    return config, reader, wallet, ContractCalls(selected.domain, "USDC", "2")
