"""Read-only composition from confirmed provider configuration.

The provider supplies the secret environment, trusted deployment selection and
execution integration. No wallet, identity, listener or retained state is created.
Circle verification uses the existing adapter's read-only ``verify_wallet``:
https://developers.circle.com/api-reference/wallets/developer-controlled-wallets/get-wallet
The registered entity secret is 32 bytes, represented here by the CLI's 64 hex
characters: https://developers.circle.com/wallets/dev-controlled/entity-secret-management
Sources checked 2026-10-06; USDC checks match mppi_agent/setup.py:_verify_usdc.
"""

from __future__ import annotations

import os
import re
from importlib.resources import files
from pathlib import Path

from mppi_protocol.chain import ChainReader, DeploymentProfile, HttpJsonRpc
from mppi_protocol.circle import CircleDeveloperWallet
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
        if not testnet:
            raise SetupError("mainnet requires an explicitly trusted deployment profile")
        profile = profile_from_mapping(
            parse_document(files("mppi_provider").joinpath("arc-testnet.json").read_bytes())
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


def _wallet(config: ProviderConfig) -> CircleDeveloperWallet:
    if config.identity.wallet_id is None:
        raise SetupError("confirmed provider configuration requires a Circle wallet id")
    api_key = os.environ.get("CIRCLE_API_KEY")
    if not api_key:
        raise SetupError("configure CIRCLE_API_KEY through the provider secret environment")
    secret = os.environ.get("CIRCLE_ENTITY_SECRET")
    if secret is None or re.fullmatch(r"[0-9a-fA-F]{64}", secret) is None:
        raise SetupError("CIRCLE_ENTITY_SECRET must be the registered 32-byte hexadecimal secret")
    try:
        return CircleDeveloperWallet(
            api_key=api_key,
            entity_secret=bytes.fromhex(secret),
            wallet_id=config.identity.wallet_id,
            address=config.identity.wallet_address,
            chain_id=config.chain.chain_id,
            timeout_ms=30_000,
        )
    except Exception:
        raise SetupError("invalid Circle wallet configuration") from None


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
) -> tuple[ProviderConfig, ChainReader, CircleDeveloperWallet, ContractCalls]:
    """Blocking configuration and verification; creates no event-loop objects."""
    config = ProviderConfig.from_file(config_path)
    try:
        selected = _profile(config, testnet, profile)
        rpc = _rpc(config, rpc_env)
        wallet = _wallet(config)
    except SetupError:
        raise
    except Exception:
        raise SetupError("provider setup configuration is unavailable or invalid") from None
    try:
        wallet.verify_wallet()
    except Exception:
        raise SetupError("Circle wallet verification failed") from None
    reader = ChainReader(rpc, selected)
    try:
        _verify_usdc(reader)
    except SetupError:
        raise
    except Exception:
        raise SetupError("deployment or USDC verification failed") from None
    return config, reader, wallet, ContractCalls(selected.domain, "USDC", "2")
