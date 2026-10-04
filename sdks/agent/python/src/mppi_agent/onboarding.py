"""One resumable, agent-owned ERC-8004 registration; no wallet custody or funding.

The caller supplies a compatible wallet and verified registry reader. The local
record contains identity and exact submission intent, never credentials. A
pending registration is reconciled on rerun and cannot become another mint.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mppi_protocol.capabilities import TransactionWallet
from mppi_protocol.deployment import profile_to_mapping
from mppi_protocol.discovery import address_hex, parse_address
from mppi_protocol.errors import ValidationError
from mppi_protocol.registry import RegistryCalls, RegistryIdentity, RegistryReader
from mppi_protocol.types import OperationResult, OperationState, fixed_bytes, uint256

from .checkpoint import Checkpoint


class OnboardingError(RuntimeError):
    """Setup cannot finish; no implicit funding or replacement identity occurs."""


@dataclass(frozen=True, slots=True)
class OnboardingResult:
    identity: RegistryIdentity | None
    balance: int
    operation: OperationResult | None
    submitted: bool

    @property
    def complete(self) -> bool:
        return self.identity is not None


def _intent(result: OperationResult, call_data: bytes) -> dict[str, Any]:
    return {
        "operation_ref": result.operation_ref,
        "calldata": "0x" + call_data.hex(),
        "state": result.state.value,
        "wallet_operation_id": result.wallet_operation_id,
        "tx_hash": "0x" + result.tx_hash.hex() if result.tx_hash is not None else None,
    }


def _result(value: object, call_data: bytes) -> OperationResult:
    if (
        type(value) is not dict
        or set(value) != {"operation_ref", "calldata", "state", "wallet_operation_id", "tx_hash"}
        or value["calldata"] != "0x" + call_data.hex()
    ):
        raise OnboardingError("retained registration intent is inconsistent")
    try:
        tx = value["tx_hash"]
        if tx is not None and (type(tx) is not str or len(tx) != 66 or not tx.startswith("0x")):
            raise ValueError
        state = OperationState(value["state"])
        operation = OperationResult(
            value["operation_ref"],
            OperationState.UNKNOWN if state is OperationState.CONFIRMED else state,
            value["wallet_operation_id"],
            fixed_bytes(bytes.fromhex(tx[2:]), 32, field="tx_hash") if tx is not None else None,
        )
        if not operation.operation_ref or len(operation.operation_ref) > 256:
            raise ValueError
        if operation.wallet_operation_id is not None and (
            type(operation.wallet_operation_id) is not str
            or not operation.wallet_operation_id
            or len(operation.wallet_operation_id) > 256
        ):
            raise ValueError
        return operation
    except (ValueError, TypeError, ValidationError):
        raise OnboardingError("retained registration result is invalid") from None


def _merge(known: OperationResult, response: object) -> OperationResult:
    # An adapter outage/substitution cannot discard or replace known handles.
    if not isinstance(response, OperationResult) or response.operation_ref != known.operation_ref:
        return OperationResult(
            known.operation_ref, OperationState.UNKNOWN, known.wallet_operation_id, known.tx_hash
        )
    if response.wallet_operation_id is not None and (
        type(response.wallet_operation_id) is not str
        or not response.wallet_operation_id
        or len(response.wallet_operation_id) > 256
    ):
        return OperationResult(
            known.operation_ref, OperationState.UNKNOWN, known.wallet_operation_id, known.tx_hash
        )
    conflict = (
        known.tx_hash is not None
        and response.tx_hash is not None
        and known.tx_hash != response.tx_hash
    ) or (
        known.wallet_operation_id is not None
        and response.wallet_operation_id is not None
        and known.wallet_operation_id != response.wallet_operation_id
    )
    if conflict:
        return OperationResult(
            known.operation_ref, OperationState.UNKNOWN, known.wallet_operation_id, known.tx_hash
        )
    return OperationResult(
        known.operation_ref,
        OperationState.UNKNOWN if response.state is OperationState.CONFIRMED else response.state,
        response.wallet_operation_id or known.wallet_operation_id,
        response.tx_hash or known.tx_hash,
    )


def onboard(
    *,
    wallet: TransactionWallet,
    registry: RegistryReader,
    config_path: Path,
    agent_id: int | None = None,
) -> OnboardingResult:
    """Check funding/identity and complete one explicit registration invocation.

    Reuse ``config_path`` for this wallet and deployment. Supply ``agent_id``
    when adopting an existing identity. A pending result needs another invocation
    to query the original operation. This function never deposits or refills USDC.
    """
    address = fixed_bytes(wallet.address, 20, field="wallet address", nonzero=True)
    if agent_id is not None:
        uint256(agent_id, field="agent_id")
    chain = registry.chain
    calls = RegistryCalls(chain.profile.domain.chain_id, chain.profile.registry_address)
    prepared = calls.register(address)
    profile = profile_to_mapping(chain.profile)
    checkpoint = Checkpoint(config_path)
    try:
        record = checkpoint.load()
        if record is None:
            record = {
                "version": 1,
                "kind": "mppi-agent-onboarding",
                "profile": profile,
                "wallet": address_hex(address),
                "agent_id": None,
                "registration": None,
            }
        if set(record) != {"version", "kind", "profile", "wallet", "agent_id", "registration"}:
            raise OnboardingError("onboarding configuration has unexpected fields")
        if record["kind"] != "mppi-agent-onboarding" or record["profile"] != profile:
            raise OnboardingError("onboarding configuration belongs to another deployment")
        if parse_address(record["wallet"]) != address:
            raise OnboardingError("onboarding configuration belongs to another wallet")
        saved_id = record["agent_id"]
        if saved_id is not None:
            if (
                type(saved_id) is not str
                or len(saved_id) > 78
                or not saved_id.isascii()
                or not saved_id.isdecimal()
            ):
                raise OnboardingError("saved agent identity is invalid")
            selected = uint256(int(saved_id), field="agent_id")
            if str(selected) != saved_id or agent_id is not None and selected != agent_id:
                raise OnboardingError("agent identity conflicts with completed onboarding")
            agent_id = selected
        block = registry.verified_block()
        balance = registry.balance(address, block=block)
        if balance <= 0:
            raise OnboardingError("fund the selected wallet with USDC on Arc before onboarding")
        identity: RegistryIdentity | None
        # A requested external identity cannot erase an unresolved local mint.
        if agent_id is not None and (saved_id is not None or record["registration"] is None):
            if record["registration"] is not None:
                retained = _result(record["registration"], prepared.call.data)
                if record["registration"]["state"] != "confirmed" or retained.tx_hash is None:
                    raise OnboardingError(
                        "completed identity has no confirmed registration evidence"
                    )
            identity = registry.read(agent_id, block=block)
            if identity.owner != address or identity.agent_wallet != address:
                raise OnboardingError(
                    "agent identity owner and agentWallet must match the funding wallet"
                )
            if saved_id is None:
                record["agent_id"] = str(agent_id)
                checkpoint.save(record)
            return OnboardingResult(identity, balance, None, False)
        if record["registration"] is None:
            registry.simulate(prepared)
            result = OperationResult("register:" + secrets.token_hex(32), OperationState.UNKNOWN)
            record["registration"] = _intent(result, prepared.call.data)
            checkpoint.save(record)  # Before the first possible external write.
            submit = True
        else:
            result = _result(record["registration"], prepared.call.data)
            submit = False
        known = result
        if submit and wallet.address != address:
            record["registration"] = None
            checkpoint.save(record)  # No wallet operation has been invoked.
            raise OnboardingError("wallet authority changed before registration submission")
        try:
            response = (
                wallet.execute(prepared.call, operation_ref=result.operation_ref)
                if submit
                else wallet.reconcile(operation_ref=result.operation_ref)
            )
        except Exception:
            response = OperationResult(result.operation_ref, OperationState.UNKNOWN)
        result = _merge(known, response)
        record["registration"] = _intent(result, prepared.call.data)
        checkpoint.save(record)  # Preserve identifiers even if the RPC now fails.
        identity = None
        if result.tx_hash is not None:
            verified = registry.reconcile(
                prepared,
                operation_ref=result.operation_ref,
                tx_hash=result.tx_hash,
                wallet_operation_id=result.wallet_operation_id,
            )
            result, identity = verified.operation, verified.identity
            if identity is not None:
                if result.state is not OperationState.CONFIRMED:
                    raise OnboardingError("registration has no canonical confirmation")
                if agent_id is not None and agent_id != identity.agent_id:
                    raise OnboardingError(
                        "confirmed registration conflicts with requested identity"
                    )
                if identity.owner != address or identity.agent_wallet != address:
                    raise OnboardingError("confirmed identity differs from the funding wallet")
                record["agent_id"] = str(identity.agent_id)
            record["registration"] = _intent(result, prepared.call.data)
            if result.state is OperationState.FAILED:
                # A canonical failed transaction cannot have minted this ID.
                # A later explicit invocation may start a fresh registration.
                record["registration"] = None
            checkpoint.save(record)
        return OnboardingResult(identity, balance, result, submit)
    finally:
        checkpoint.close()
