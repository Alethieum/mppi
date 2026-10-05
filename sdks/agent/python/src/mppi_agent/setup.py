"""Installed Circle onboarding and client composition for official Arc testnet.

The SDK supplies reviewed public deployment/account pins. RPC observations are
compared with them, never promoted into trust anchors. Wallet custody remains
with Circle; the separate local session credential authorizes MPPI vouchers.
Sources and supported account version: docs/agent-quickstart.md.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mppi_protocol.amounts import uint64
from mppi_protocol.chain import ChainReader, CodePin, HttpJsonRpc
from mppi_protocol.contract import (
    ARC_USDC,
    ContractCalls,
    _decode_exact,
    _hash,
)
from mppi_protocol.deployment import profile_from_mapping, profile_to_mapping
from mppi_protocol.discovery import address_hex, parse_address, parse_document
from mppi_protocol.follower import ChainFollower
from mppi_protocol.registry import RegistryReader
from mppi_protocol.types import uint256

from .checkpoint import Checkpoint
from .circle import CircleAgentWallet
from .credentials import create_session_signer, load_session_signer, private_directory
from .onboarding import OnboardingError, OnboardingResult, onboard

if TYPE_CHECKING:
    from .client import Client

DEFAULT_CONFIG = Path(".mppi/agent.json")
DEFAULT_RPC_ENV = "ARC_RPC_URL_TESTNET"
# Public endpoint documented at docs.arc.io/arc/references/connect-to-arc.
DEFAULT_RPC = "https://rpc.blockdaemon.testnet.arc.io"
# circle.py:CircleAgentWallet gives each CLI command 240 seconds. Reserve another
# 60 seconds for wallet/code verification and reconciliation around that command;
# this operation budget does not extend chain freshness or collection deadlines.
_CIRCLE_OPERATION_TIMEOUT_MS = 300_000
_PROXY_HASH = bytes.fromhex("d09e34b2e51f09092e73e3ba1d520a42972496bf2a3bfdb22bb50b9729a9fd66")
_IMPLEMENTATION = CodePin(
    bytes.fromhex("895b96c92d1007872fe1f70606e6b7eeaebd620e"),
    bytes.fromhex("3692ce55057ce6c0da5f23b4fb8819fa6d5838758a5e88585b291fdccd36d0fa"),
)


def _path(config: Path, suffix: str) -> Path:
    return config.with_name(config.name + suffix)


def _profile() -> dict[str, Any]:
    return parse_document(files("mppi_agent").joinpath("arc-testnet.json").read_bytes())


def _rpc(env_name: str) -> HttpJsonRpc:
    if type(env_name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name) is None:
        raise OnboardingError("RPC configuration must name an environment variable")
    endpoint = os.environ.get(env_name)
    if endpoint is None and env_name == DEFAULT_RPC_ENV:
        endpoint = DEFAULT_RPC
    if not endpoint:
        raise OnboardingError("configure the selected Arc RPC environment variable")
    return HttpJsonRpc(endpoint)


def _record(value: object) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {
        "version",
        "kind",
        "profile",
        "wallet",
        "rpc_env",
        "signer_address",
        "initialized",
        "agent_id",
        "payments_initialized",
    }:
        raise OnboardingError("agent setup is missing or has unexpected fields")
    if (
        type(value["version"]) is not int
        or value["version"] != 1
        or value["kind"] != "mppi-agent-setup"
        or value["profile"] != _profile()
        or type(value["initialized"]) is not bool
        or type(value["payments_initialized"]) is not bool
    ):
        raise OnboardingError("agent setup does not match the supported deployment")
    parse_address(value["wallet"])
    if value["signer_address"] is not None:
        parse_address(value["signer_address"])
    elif value["initialized"]:
        raise OnboardingError("initialized agent setup has no fixed signer")
    if value["agent_id"] is not None:
        identifier = value["agent_id"]
        if type(identifier) is not str or re.fullmatch(r"0|[1-9][0-9]{0,77}", identifier) is None:
            raise OnboardingError("agent setup identity is invalid")
        uint256(int(identifier), field="agent identity")
    return value


def _reader(record: dict[str, Any]) -> ChainReader:
    return ChainReader(_rpc(record["rpc_env"]), profile_from_mapping(record["profile"]))


def _wallet(record: dict[str, Any], reader: ChainReader, config: Path) -> CircleAgentWallet:
    return CircleAgentWallet(
        chain=reader,
        wallet=CodePin(parse_address(record["wallet"]), _PROXY_HASH),
        implementation=_IMPLEMENTATION,
        checkpoint_path=_path(config, ".wallet.json"),
        allow_registration_activation=True,
    )


def _verify_usdc(reader: ChainReader) -> None:
    """Check fixed Arc USDC/2/6 and its EIP-712 separator at one canonical block."""
    block = reader.block()
    reader.verify_deployment(block)
    for function, output, expected in (
        ("name()", "string", "USDC"),
        ("version()", "string", "2"),
        ("decimals()", "uint8", 6),
    ):
        raw = reader._call(ARC_USDC, _hash(function.encode("ascii"))[:4], block)
        if _decode_exact([output], raw) != (expected,):
            raise OnboardingError("Arc USDC authorization domain differs from supported values")
    expected_separator = _hash(
        _hash(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")
        + _hash(b"USDC")
        + _hash(b"2")
        + reader.profile.domain.chain_id.to_bytes(32, "big")
        + ARC_USDC.rjust(32, b"\0")
    )
    separator = reader._call(ARC_USDC, _hash(b"DOMAIN_SEPARATOR()")[:4], block)
    if separator != expected_separator:
        raise OnboardingError("Arc USDC authorization domain separator is inconsistent")
    reader._check_canonical(block)


@dataclass(frozen=True, slots=True)
class SetupResult:
    config_path: Path
    wallet_address: bytes
    onboarding: OnboardingResult


def onboard_circle(
    wallet_address: str,
    *,
    config_path: Path = DEFAULT_CONFIG,
    rpc_env: str = DEFAULT_RPC_ENV,
    agent_id: int | None = None,
) -> SetupResult:
    """Prepare one supplied Circle wallet; reruns reconcile its original registration.

    Explicit invocation creates a separate private session credential exactly
    once. It never funds escrow, replenishes a wallet, or starts inference.
    """
    address = parse_address(wallet_address)
    _rpc(rpc_env)  # Validate local selection before creating any retained setup.
    if agent_id is not None:
        uint256(agent_id, field="agent identity")
    config = Path(config_path).absolute()
    with private_directory(config.parent, create=True):
        pass
    state = Checkpoint(config)
    try:
        saved = state.load()
        if saved is None:
            if any(
                _path(config, suffix).exists()
                for suffix in (
                    ".signer",
                    ".identity.json",
                    ".wallet.json",
                    ".payments.json",
                )
            ):
                raise OnboardingError("setup record is missing; restore it with its retained state")
            saved = {
                "version": 1,
                "kind": "mppi-agent-setup",
                "profile": _profile(),
                "wallet": address_hex(address),
                "rpc_env": rpc_env,
                "signer_address": None,
                "initialized": False,
                "agent_id": None,
                "payments_initialized": False,
            }
            # Validate the supplied account before binding new local state to it.
            # Invalid selection or unavailable RPC leaves no credential to replace.
            initial_reader = _reader(saved)
            initial_wallet = _wallet(saved, initial_reader, config)
            initial_wallet.verify_registration_wallet()
            _verify_usdc(initial_reader)
            state.save(saved)
        record = _record(saved)
        if record["wallet"] != address_hex(address) or record["rpc_env"] != rpc_env:
            raise OnboardingError("reuse the original wallet and RPC selection for this setup")
        if agent_id is not None:
            uint256(agent_id, field="agent identity")
            if record["agent_id"] is not None and record["agent_id"] != str(agent_id):
                raise OnboardingError("selected identity differs from retained agent setup")
        reader = _reader(record)
        key_path = _path(config, ".signer")
        if record["signer_address"] is None:
            signer = create_session_signer(key_path)
            record["signer_address"] = address_hex(signer.address)
            state.save(record)
        else:
            load_session_signer(key_path, expected_address=parse_address(record["signer_address"]))
        if record["initialized"]:
            for suffix in (".identity.json", ".wallet.json"):
                if not _path(config, suffix).is_file():
                    raise OnboardingError(
                        "retained onboarding state is missing; restore its backup"
                    )
        wallet = _wallet(record, reader, config)
        if not record["initialized"]:
            # Establish all retained records before any possible wallet submission.
            with wallet._journal():
                wallet._save()
            identity_state = Checkpoint(_path(config, ".identity.json"))
            try:
                if identity_state.load() is None:
                    identity_state.save(
                        {
                            "version": 1,
                            "kind": "mppi-agent-onboarding",
                            "profile": profile_to_mapping(reader.profile),
                            "wallet": address_hex(address),
                            "agent_id": None,
                            "registration": None,
                        }
                    )
            finally:
                identity_state.close()
            record["initialized"] = True
            state.save(record)
        wallet.verify_registration_wallet()
        _verify_usdc(reader)
        result = onboard(
            wallet=wallet,
            registry=RegistryReader(reader),
            config_path=_path(config, ".identity.json"),
            agent_id=int(record["agent_id"]) if record["agent_id"] is not None else agent_id,
        )
        if result.identity is not None:
            record["agent_id"] = str(result.identity.agent_id)
            state.save(record)  # Preserve the confirmed identity even if a later read fails.
            wallet.verify_wallet()
        return SetupResult(config, address, result)
    finally:
        state.close()


def client_from_config(
    provider: str | int,
    *,
    config_path: Path = DEFAULT_CONFIG,
    initial_session_tokens: int | None = None,
    ca_file: str | None = None,
) -> Client:
    """Compose the installed client from completed onboarding without new wallet writes.

    One stable payment checkpoint belongs to this setup. Its existing provider,
    signer and pending-operation guards apply unchanged. Funding is explicit.
    """
    from .client import Client

    if initial_session_tokens is not None:
        uint64(initial_session_tokens, field="initial_session_tokens", positive=True)
    config = Path(config_path).absolute()
    with private_directory(config.parent):
        pass
    state = Checkpoint(config)
    try:
        record = _record(state.load())
        if not record["initialized"] or record["agent_id"] is None:
            raise OnboardingError("complete mppi-agent onboard before creating a client")
        for suffix in (".identity.json", ".wallet.json"):
            if not _path(config, suffix).is_file():
                raise OnboardingError("retained agent state is missing; restore its backup")
        signer = load_session_signer(
            _path(config, ".signer"),
            expected_address=parse_address(record["signer_address"]),
        )
        reader = _reader(record)
        wallet = _wallet(record, reader, config)
        wallet.verify_wallet()
        _verify_usdc(reader)
        identity = RegistryReader(reader).read(int(record["agent_id"]))
        if identity.owner != wallet.address or identity.agent_wallet != wallet.address:
            raise OnboardingError("registered agent authority differs from configured wallet")
        payment_path = _path(config, ".payments.json")
        if record["payments_initialized"] and not payment_path.is_file():
            raise OnboardingError("payment checkpoint is missing; restore it before continuing")
        if not record["payments_initialized"]:
            payments = Checkpoint(payment_path)
            try:
                if payments.load() is None:
                    payments.save({"version": 1})
            finally:
                payments.close()
            record["payments_initialized"] = True
            state.save(record)
        return Client(
            provider,
            wallet=wallet,
            signer=signer,
            chain=ChainFollower(
                reader,
                max_age_ms=60_000,
                collection_margin_ms=30_000,
                poll_interval_ms=2_000,
            ),
            contract=ContractCalls(reader.profile.domain, "USDC", "2"),
            checkpoint_path=payment_path,
            signer_reference=str(_path(config, ".signer")),
            initial_session_tokens=initial_session_tokens,
            agent_id=identity.agent_id,
            ca_file=ca_file,
            timeout_ms=_CIRCLE_OPERATION_TIMEOUT_MS,
        )
    finally:
        state.close()
