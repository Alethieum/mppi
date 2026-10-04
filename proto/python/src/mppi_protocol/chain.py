"""Read-only Arc evidence and reconciliation of previously submitted MPPI calls.

RPC consistency is checked, not cryptographically attested. Select a trusted RPC
and verified deployment manifest. This module does not implement a chain follower,
provider storage, transaction resubmission or session readiness by itself.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Protocol, cast
from uuid import uuid4

from eth_abi.abi import encode

from .amounts import uint64
from .catalog import Catalog, parse_catalog
from .contract import (
    OperationKind,
    PreparedOperation,
    _decode_exact,
    _hash,
    decode_event,
    decode_result,
    encode_call,
    event_topic,
)
from .discovery import ProviderIdentity, _parse_document, address_hex, parse_address
from .errors import ValidationCode, ValidationError
from .http import MAX_RPC_RESPONSE_BYTES, HttpTransport, TransportError
from .signing import session_id as descriptor_id
from .signing import voucher_digest
from .types import (
    ChainObservation,
    ConfirmedSettlementRef,
    Domain,
    OperationResult,
    OperationState,
    PauseLevel,
    SessionDescriptor,
    SessionOperationOutcome,
    SessionStatus,
    fixed_bytes,
    uint256,
)

_IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"


class ChainEvidenceError(RuntimeError):
    """Missing or inconsistent chain evidence; payment authorization must stop."""


class Rpc(Protocol):
    def call(self, method: str, params: list[object]) -> object: ...


@dataclass(frozen=True, slots=True)
class HttpJsonRpc:
    endpoint: str = field(repr=False)
    transport: HttpTransport = field(
        default_factory=lambda: HttpTransport(max_bytes=MAX_RPC_RESPONSE_BYTES)
    )

    def call(self, method: str, params: list[object]) -> object:
        # This adapter only reads. It cannot turn reconciliation into a submit.
        if method not in {
            "eth_chainId",
            "eth_getBlockByNumber",
            "eth_getTransactionReceipt",
            "eth_getCode",
            "eth_getStorageAt",
            "eth_call",
            "eth_estimateGas",
        }:
            raise ChainEvidenceError("unsupported read-only RPC method")
        request_id = str(uuid4())
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            separators=(",", ":"),
        ).encode()
        response = _parse_document(
            self.transport.request(self.endpoint, body=payload), max_bytes=MAX_RPC_RESPONSE_BYTES
        )
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise ChainEvidenceError("RPC response identity mismatch")
        if "error" in response or "result" not in response:
            # No remote error data: it may contain private endpoint parameters.
            raise ChainEvidenceError("RPC request returned no usable result")
        return response["result"]


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ChainEvidenceError("RPC object missing")
    return cast(dict[str, Any], value)


def _quantity(value: object) -> int:
    if (
        type(value) is not str
        or re.fullmatch(r"0x(?:0|[1-9a-f][0-9a-f]*)", value) is None
        or len(value) > 66
    ):
        raise ChainEvidenceError("invalid RPC quantity")
    return int(value, 16)


def _data(value: object, length: int | None = None) -> bytes:
    if type(value) is not str or re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value) is None:
        raise ChainEvidenceError("invalid RPC bytes")
    data = bytes.fromhex(value[2:])
    if length is not None and len(data) != length:
        raise ChainEvidenceError("invalid RPC byte width")
    return data


@dataclass(frozen=True, slots=True)
class CodePin:
    address: bytes
    code_hash: bytes

    def __post_init__(self) -> None:
        fixed_bytes(self.address, 20, field="code address", nonzero=True)
        fixed_bytes(self.code_hash, 32, field="code hash", nonzero=True)


@dataclass(frozen=True, slots=True)
class DeploymentProfile:
    domain: Domain
    registry_address: bytes
    proxy: CodePin
    implementation: CodePin
    registry: CodePin
    registry_implementation: CodePin

    def __post_init__(self) -> None:
        if not isinstance(self.domain, Domain) or self.domain.chain_id not in (5042, 5042002):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "MPPI requires an Arc network profile"
            )
        fixed_bytes(self.registry_address, 20, field="registry_address", nonzero=True)
        if (
            self.proxy.address != self.domain.verifying_contract
            or self.registry.address != self.registry_address
        ):
            raise ValidationError(
                ValidationCode.INVALID_VALUE, "profile addresses and code pins differ"
            )


@dataclass(frozen=True, slots=True)
class Block:
    observation: ChainObservation
    timestamp: int
    parent_hash: bytes


@dataclass(frozen=True, slots=True)
class ChainSession:
    deposit: int
    settled_cumulative: int
    status: SessionStatus
    close_requested_at: int
    last_activity: int
    initial_rate_card_hash: bytes

    @property
    def exists(self) -> bool:
        return self.deposit > 0


@dataclass(frozen=True, slots=True)
class OpeningEvidence:
    descriptor: SessionDescriptor
    block: Block
    deposit: int
    initial_catalog: Catalog
    tx_hash: bytes


@dataclass(frozen=True, slots=True)
class ClosingEvidence:
    block: Block
    capture_amount: int
    refunded: int
    tx_hash: bytes


@dataclass(frozen=True, slots=True)
class SimulationEvidence:
    """Advisory execution/cost evidence at one canonical block, never a receipt."""

    block: Block
    gas_estimate: int
    gas_limit: int


class ChainReader:
    """Synchronous, block-pinned reads; use outside the payment event loop."""

    def __init__(self, rpc: Rpc, profile: DeploymentProfile) -> None:
        self.rpc = rpc
        self.profile = profile

    def _network(self) -> None:
        if _quantity(self.rpc.call("eth_chainId", [])) != self.profile.domain.chain_id:
            raise ChainEvidenceError("RPC chain differs from the selected deployment")

    def block(self, number: int | None = None) -> Block:
        self._network()
        if number is not None:
            uint64(number, field="block number")
        raw = _object(
            self.rpc.call(
                "eth_getBlockByNumber", ["latest" if number is None else hex(number), False]
            )
        )
        actual = _quantity(raw.get("number"))
        if number is not None and actual != number:
            raise ChainEvidenceError("canonical block number mismatch")
        timestamp = uint64(_quantity(raw.get("timestamp")), field="block timestamp")
        return Block(
            ChainObservation(
                self.profile.domain.chain_id,
                self.profile.domain.verifying_contract,
                actual,
                _data(raw.get("hash"), 32),
            ),
            timestamp,
            _data(raw.get("parentHash"), 32),
        )

    def _position(self, block: Block) -> dict[str, object]:
        observation = block.observation
        if (
            observation.chain_id != self.profile.domain.chain_id
            or observation.contract_address != self.profile.domain.verifying_contract
        ):
            raise ChainEvidenceError("block observation differs from selected deployment")
        return {"blockHash": "0x" + observation.block_hash.hex(), "requireCanonical": True}

    def _call(self, address: bytes, data: bytes, block: Block) -> bytes:
        result = self.rpc.call(
            "eth_call",
            [{"to": address_hex(address), "data": "0x" + data.hex()}, self._position(block)],
        )
        return _data(result)

    def verify_deployment(self, block: Block) -> None:
        """Compare selected manifest code/implementations and MPPI EIP-712 domain.

        Governance/initializer manifest assertions are deployment release checks;
        these reads cannot manufacture a trusted manifest from an unknown RPC.
        """
        self._network()
        for pin in (
            self.profile.proxy,
            self.profile.implementation,
            self.profile.registry,
            self.profile.registry_implementation,
        ):
            code = _data(
                self.rpc.call("eth_getCode", [address_hex(pin.address), self._position(block)])
            )
            if not code or _hash(code) != pin.code_hash:
                raise ChainEvidenceError("deployed code differs from release profile")
        for proxy, implementation in (
            (self.profile.proxy, self.profile.implementation),
            (self.profile.registry, self.profile.registry_implementation),
        ):
            value = _data(
                self.rpc.call(
                    "eth_getStorageAt",
                    [address_hex(proxy.address), _IMPLEMENTATION_SLOT, self._position(block)],
                ),
                32,
            )
            if value != b"\0" * 12 + implementation.address:
                raise ChainEvidenceError("proxy implementation differs from release profile")
        digest = decode_result(
            "voucherDigest",
            self._call(
                self.profile.domain.verifying_contract,
                encode_call("voucherDigest", (bytes(32), 0)),
                block,
            ),
        )[0]
        if digest != voucher_digest(self.profile.domain, bytes(32), 0):
            raise ChainEvidenceError("MPPI signing domain differs from release profile")
        self._check_canonical(block)

    def _check_canonical(self, block: Block) -> None:
        if self.block(block.observation.block_number) != block:
            raise ChainEvidenceError("canonical block evidence changed during observation")

    def session(self, session_id: bytes, block: Block) -> ChainSession:
        fixed_bytes(session_id, 32, field="session_id")
        result = decode_result(
            "getSession",
            self._call(
                self.profile.domain.verifying_contract,
                encode_call("getSession", (session_id,)),
                block,
            ),
        )[0]
        deposit, settled, status, closing, activity, initial_hash = result
        if (
            status not in (0, 1, 2)
            or settled > deposit
            or (
                deposit == 0
                and any((settled, status, closing, activity, initial_hash != bytes(32)))
            )
        ):
            raise ChainEvidenceError("inconsistent MPPI session state")
        return ChainSession(
            deposit,
            settled,
            (SessionStatus.OPEN, SessionStatus.CLOSING, SessionStatus.CLOSED)[status],
            closing,
            activity,
            initial_hash,
        )

    def pause_level(self, block: Block) -> PauseLevel:
        result = decode_result(
            "pauseLevel",
            self._call(
                self.profile.domain.verifying_contract, encode_call("pauseLevel", ()), block
            ),
        )[0]
        if result not in (0, 1, 2):
            raise ChainEvidenceError("invalid MPPI pause level")
        return PauseLevel(result)

    def identity(
        self, provider_id: int, block: Block, *, allow_unavailable_catalog: bool = False
    ) -> ProviderIdentity:
        uint256(provider_id, field="provider_id")

        def read(
            name: str, inputs: list[str], values: list[object], outputs: list[str]
        ) -> tuple[Any, ...]:
            signature = name + "(" + ",".join(inputs) + ")"
            data = _hash(signature.encode())[:4] + encode(inputs, values)
            return _decode_exact(outputs, self._call(self.profile.registry_address, data, block))

        owner = parse_address(read("ownerOf", ["uint256"], [provider_id], ["address"])[0])
        recipient = parse_address(
            read("getAgentWallet", ["uint256"], [provider_id], ["address"])[0]
        )
        uri = read("tokenURI", ["uint256"], [provider_id], ["string"])[0]
        raw_catalog = read(
            "getMetadata", ["uint256", "string"], [provider_id, "mppi.catalog"], ["bytes"]
        )[0]
        try:
            catalog: Catalog | None = parse_catalog(raw_catalog)
        except ValidationError:
            if not allow_unavailable_catalog:
                raise
            # Unusable offering is distinct from unavailable RPC/identity state.
            # Historical accounting and eligible exits must remain observable.
            catalog = None
        self._check_canonical(block)
        return ProviderIdentity(
            block.observation,
            self.profile.registry_address,
            provider_id,
            owner,
            recipient,
            uri,
            catalog,
        )

    def agent_wallet(self, agent_id: int, block: Block) -> bytes:
        """Registry identity binds the funding wallet, never its session signer."""
        uint256(agent_id, field="agent_id")
        data = _hash(b"getAgentWallet(uint256)")[:4] + encode(["uint256"], [agent_id])
        address = parse_address(
            _decode_exact(["address"], self._call(self.profile.registry_address, data, block))[0]
        )
        self._check_canonical(block)
        return address

    def _event(
        self,
        tx_hash: bytes,
        session_id: bytes,
        name: str,
        *,
        capture_amount: int | None = None,
        delta: int | None = None,
    ) -> tuple[Block, dict[str, Any]]:
        """Read one unambiguous MPPI event; outer transaction success is insufficient."""
        fixed_bytes(tx_hash, 32, field="tx_hash")
        fixed_bytes(session_id, 32, field="session_id")
        self._network()
        receipt = _object(self.rpc.call("eth_getTransactionReceipt", ["0x" + tx_hash.hex()]))
        block = self.block(_quantity(receipt.get("blockNumber")))
        if (
            _data(receipt.get("transactionHash"), 32) != tx_hash
            or _data(receipt.get("blockHash"), 32) != block.observation.block_hash
            or _quantity(receipt.get("status")) != 1
        ):
            raise ChainEvidenceError("successful canonical receipt required")
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise ChainEvidenceError("receipt logs missing")
        matches = []
        previous = -1
        tx_index = _quantity(receipt.get("transactionIndex"))
        for raw in logs:
            log = _object(raw)
            index = _quantity(log.get("logIndex"))
            if (
                index <= previous
                or log.get("removed") is not False
                or _data(log.get("transactionHash"), 32) != tx_hash
                or _quantity(log.get("transactionIndex")) != tx_index
                or _quantity(log.get("blockNumber")) != block.observation.block_number
                or _data(log.get("blockHash"), 32) != block.observation.block_hash
            ):
                raise ChainEvidenceError("event inclusion mismatch")
            previous = index
            if _data(log.get("address"), 20) != self.profile.domain.verifying_contract:
                continue
            if not isinstance(log.get("topics"), list):
                raise ChainEvidenceError("event topics missing")
            topics = tuple(_data(value, 32) for value in log["topics"])
            if not topics or topics[0] != event_topic(name):
                continue
            event = decode_event(name, topics, _data(log.get("data")))
            if (
                event["sessionId"] == session_id
                and (capture_amount is None or event.get("captureAmount") == capture_amount)
                and (delta is None or event.get("deltaPaid") == delta)
            ):
                matches.append(event)
        if len(matches) != 1:
            raise ChainEvidenceError("required MPPI event missing or ambiguous")
        self._check_canonical(block)
        return block, matches[0]

    def opening(
        self, tx_hash: bytes, session_id: bytes, *, accepted_catalog: Catalog | None = None
    ) -> OpeningEvidence:
        block, event = self._event(tx_hash, session_id, "SessionOpened")
        descriptor = SessionDescriptor(
            parse_address(event["agent"]),
            parse_address(event["sessionSigner"]),
            event["providerId"],
            parse_address(event["recipient"]),
            parse_address(event["operator"]),
            parse_address(event["token"]),
            event["salt"],
        )
        state = self.session(session_id, block)
        # A later transaction in this block may have replaced registry metadata.
        # The opening event commits to the exact accepted bytes by hash. Reparse
        # supplied bytes; constructing a Catalog object is not verification.
        catalog = (
            parse_catalog(accepted_catalog.canonical_bytes)
            if accepted_catalog is not None
            else self.identity(descriptor.provider_id, block).catalog
        )
        if (
            descriptor_id(self.profile.domain, descriptor) != session_id
            or not state.exists
            or event["deposit"] <= 0
            or state.deposit < event["deposit"]
            or state.initial_rate_card_hash != event["initialRateCardHash"]
            or catalog is None
            or catalog.rate_card_hash != event["initialRateCardHash"]
        ):
            raise ChainEvidenceError("opening descriptor, deposit or catalog mismatch")
        self._check_canonical(block)
        return OpeningEvidence(descriptor, block, event["deposit"], catalog, tx_hash)

    def simulate(self, operation: PreparedOperation, *, sender: bytes) -> SimulationEvidence:
        """Simulate exactly from the intended wallet before batch submission.

        Execution API ``eth_estimateGas`` takes a block number/tag, unlike
        EIP-1898 ``eth_call``. Recheck that height's hash after estimation; never
        silently fall back to latest. Both results remain advisory.
        """
        fixed_bytes(sender, 20, field="sender", nonzero=True)
        if (
            not isinstance(operation, PreparedOperation)
            or operation.call.chain_id != self.profile.domain.chain_id
            or operation.call.to != self.profile.domain.verifying_contract
        ):
            raise ChainEvidenceError("simulation differs from selected deployment")
        block = self.block()
        self.verify_deployment(block)
        raw = _object(
            self.rpc.call("eth_getBlockByNumber", [hex(block.observation.block_number), False])
        )
        if _data(raw.get("hash"), 32) != block.observation.block_hash:
            raise ChainEvidenceError("simulation block changed")
        gas_limit = _quantity(raw.get("gasLimit"))
        if gas_limit <= 0:
            raise ChainEvidenceError("simulation block has no gas capacity")
        call = {
            "from": address_hex(sender),
            "to": address_hex(operation.call.to),
            "data": "0x" + operation.call.data.hex(),
            "value": "0x0",
            "gas": hex(gas_limit),
        }
        if _data(self.rpc.call("eth_call", [call, self._position(block)])) != b"":
            raise ChainEvidenceError("MPPI simulation returned unexpected data")
        estimate = _quantity(
            self.rpc.call("eth_estimateGas", [call, hex(block.observation.block_number)])
        )
        if not 0 < estimate <= gas_limit:
            raise ChainEvidenceError("simulation estimate exceeds block capacity")
        self._check_canonical(block)
        return SimulationEvidence(block, estimate, gas_limit)

    def closing(self, tx_hash: bytes, session_id: bytes) -> ClosingEvidence:
        block, event = self._event(tx_hash, session_id, "SessionClosed")
        state = self.session(session_id, block)
        if (
            not state.exists
            or state.status is not SessionStatus.CLOSED
            or event["captureAmount"] != state.settled_cumulative
            or event["captureAmount"] + event["refunded"] != state.deposit
        ):
            raise ChainEvidenceError("closing event contradicts canonical state")
        self._check_canonical(block)
        return ClosingEvidence(block, event["captureAmount"], event["refunded"], tx_hash)

    def verify_settlement(
        self, reference: ConfirmedSettlementRef, descriptor: SessionDescriptor
    ) -> None:
        if descriptor_id(self.profile.domain, descriptor) != reference.session_id:
            raise ChainEvidenceError("settlement descriptor mismatch")
        block, event = self._event(
            reference.tx_hash,
            reference.session_id,
            "Settled",
            capture_amount=reference.settled_cumulative,
            delta=reference.delta,
        )
        state = self.session(reference.session_id, block)
        if (
            block.observation != reference.observation
            or parse_address(event["agent"]) != descriptor.agent
            or parse_address(event["recipient"]) != descriptor.recipient
            or event["newSettled"] != reference.settled_cumulative
            or event["captureAmount"] != reference.settled_cumulative
            or event["deltaPaid"] != reference.delta
            or event["cumulativeAmount"] < reference.settled_cumulative
            or not state.exists
            or state.settled_cumulative < reference.settled_cumulative
        ):
            raise ChainEvidenceError("settlement reference contradicts canonical event")
        self._check_canonical(block)

    def reconcile(
        self,
        operation: PreparedOperation,
        *,
        operation_ref: str,
        tx_hash: bytes | None,
        wallet_operation_id: str | None = None,
    ) -> OperationResult:
        """Re-read one original transaction; never submit or replace a call.

        A missing hash/receipt remains unknown/pending. Included outer success
        without MPPI evidence remains unknown. Batch confirmation describes the
        receipt; each item's coverage and attributable payment remain separate.
        """
        if (
            operation.call.chain_id != self.profile.domain.chain_id
            or operation.call.to != self.profile.domain.verifying_contract
        ):
            raise ChainEvidenceError("operation differs from selected deployment")
        if tx_hash is None:
            return OperationResult(operation_ref, OperationState.UNKNOWN, wallet_operation_id)
        fixed_bytes(tx_hash, 32, field="tx_hash")
        try:
            self._network()
            raw = self.rpc.call("eth_getTransactionReceipt", ["0x" + tx_hash.hex()])
            if raw is None:
                return OperationResult(
                    operation_ref, OperationState.PENDING, wallet_operation_id, tx_hash
                )
            receipt = _object(raw)
            if _data(receipt.get("transactionHash"), 32) != tx_hash:
                raise ChainEvidenceError("receipt transaction mismatch")
            block = self.block(_quantity(receipt.get("blockNumber")))
            if _data(receipt.get("blockHash"), 32) != block.observation.block_hash:
                raise ChainEvidenceError("receipt is not in the canonical block")
            status = _quantity(receipt.get("status"))
            if status not in (0, 1):
                raise ChainEvidenceError("invalid receipt status")
            if status == 0:
                self._check_canonical(block)
                return OperationResult(
                    operation_ref,
                    OperationState.FAILED,
                    wallet_operation_id,
                    tx_hash,
                    block.observation,
                )
            outcomes, proven = self._outcomes(operation, receipt, block, tx_hash)
            self._check_canonical(block)
            return OperationResult(
                operation_ref,
                OperationState.CONFIRMED if proven else OperationState.UNKNOWN,
                wallet_operation_id,
                tx_hash,
                block.observation,
                outcomes,
            )
        except (
            TransportError,
            ChainEvidenceError,
            ValidationError,
            ValueError,
            KeyError,
            TypeError,
        ):
            # Evidence unavailable/contradictory never becomes an economic fail.
            return OperationResult(
                operation_ref, OperationState.UNKNOWN, wallet_operation_id, tx_hash
            )

    def _outcomes(
        self, operation: PreparedOperation, receipt: dict[str, Any], block: Block, tx_hash: bytes
    ) -> tuple[tuple[SessionOperationOutcome, ...], bool]:
        name = {
            OperationKind.OPEN: "SessionOpened",
            OperationKind.TOP_UP: "ToppedUp",
            OperationKind.SETTLE: "Settled",
            OperationKind.SETTLE_BATCH: "Settled",
            OperationKind.CLOSE: "SessionClosed",
            OperationKind.WITHDRAW: "SessionClosed",
            OperationKind.REQUEST_CLOSE: "CloseRequested",
            OperationKind.RELEASE_IDLE: "IdleReleased",
        }[operation.kind]
        logs = receipt.get("logs")
        if not isinstance(logs, list):
            raise ChainEvidenceError("receipt logs missing")
        events: dict[bytes, list[dict[str, Any]]] = {}
        captures: dict[tuple[bytes, int], list[dict[str, Any]]] = {}
        wanted = set(operation.session_ids)
        previous_index = -1
        tx_index = _quantity(receipt.get("transactionIndex"))
        for raw in logs:
            log = _object(raw)
            index = _quantity(log.get("logIndex"))
            if (
                index <= previous_index
                or log.get("removed") is not False
                or _data(log.get("transactionHash"), 32) != tx_hash
                or _quantity(log.get("transactionIndex")) != tx_index
                or _quantity(log.get("blockNumber")) != block.observation.block_number
                or _data(log.get("blockHash"), 32) != block.observation.block_hash
            ):
                raise ChainEvidenceError("log inclusion or order mismatch")
            previous_index = index
            if _data(log.get("address"), 20) != self.profile.domain.verifying_contract:
                continue
            if not isinstance(log.get("topics"), list):
                raise ChainEvidenceError("log topics missing")
            topics = tuple(_data(value, 32) for value in log["topics"])
            if not topics or topics[0] != event_topic(name):
                continue
            decoded_event = decode_event(name, topics, _data(log.get("data")))
            session_id = decoded_event["sessionId"]
            if session_id not in wanted:
                # An ERC-4337 receipt may include other users' MPPI calls.
                continue
            if session_id in events and operation.kind is not OperationKind.SETTLE_BATCH:
                raise ChainEvidenceError("ambiguous repeated operation event")
            events.setdefault(session_id, []).append(decoded_event)
            if operation.kind is OperationKind.SETTLE_BATCH:
                captures.setdefault((session_id, decoded_event["captureAmount"]), []).append(
                    decoded_event
                )
        multiplicities: dict[tuple[bytes, int], int] = {}
        if operation.kind is OperationKind.SETTLE_BATCH:
            for sid, capture_target in zip(operation.session_ids, operation.amounts, strict=True):
                key = (sid, capture_target)
                multiplicities[key] = multiplicities.get(key, 0) + 1
        states: dict[bytes, ChainSession] = {}
        results = []
        proven = True
        for index, (session_id, descriptor) in enumerate(
            zip(operation.session_ids, operation.descriptors, strict=True)
        ):
            if session_id not in states:
                states[session_id] = self.session(session_id, block)
            state = states[session_id]
            candidates = events.get(session_id, [])
            event = candidates[0] if candidates else None
            target = operation.amounts[index] if operation.amounts else None
            settlement = None
            covered = None
            if operation.kind in (OperationKind.SETTLE, OperationKind.SETTLE_BATCH):
                assert target is not None
                covered = state.exists and state.settled_cumulative >= target
                if operation.kind is OperationKind.SETTLE_BATCH:
                    # The ABI deliberately emits no item index. Distinct targets
                    # can be matched, but identical repeated inputs cannot tell
                    # which item paid. Preserve ordered coverage without assigning
                    # the same payment to multiple ambiguous items.
                    matches = captures.get((session_id, target), [])
                    if len(matches) > 1:
                        raise ChainEvidenceError("ambiguous repeated capture event")
                    event = matches[0] if matches else None
                if event is not None:
                    if (
                        parse_address(event["agent"]) != descriptor.agent
                        or parse_address(event["recipient"]) != descriptor.recipient
                        or event["captureAmount"] != target
                        or event["newSettled"] != target
                        or event["cumulativeAmount"] < target
                        or not 0 < event["deltaPaid"] <= target
                        or not covered
                    ):
                        raise ChainEvidenceError(
                            "settlement event contradicts the intended target/state"
                        )
                    if multiplicities.get((session_id, target), 1) == 1:
                        settlement = ConfirmedSettlementRef(
                            block.observation, tx_hash, session_id, target, event["deltaPaid"]
                        )
                # No event can mean a no-op, skipped item or inner wallet failure.
                # State coverage is useful but never attributable to this call.
                elif operation.kind is OperationKind.SETTLE:
                    proven = False
            elif event is None or not state.exists:
                proven = False
            elif operation.kind is OperationKind.OPEN:
                expected = {
                    "agent": descriptor.agent,
                    "sessionSigner": descriptor.session_signer,
                    "recipient": descriptor.recipient,
                    "operator": descriptor.operator,
                    "token": descriptor.token,
                }
                if (
                    any(parse_address(event[key]) != value for key, value in expected.items())
                    or event["providerId"] != descriptor.provider_id
                    or event["salt"] != descriptor.salt
                    or event["deposit"] != target
                    or state.deposit < event["deposit"]
                    or event["initialRateCardHash"] != operation.expected_rate_card_hash
                    or state.initial_rate_card_hash != operation.expected_rate_card_hash
                ):
                    raise ChainEvidenceError("opening event contradicts descriptor/funding terms")
            elif operation.kind is OperationKind.TOP_UP:
                if (
                    event["amount"] != target
                    or event["newDeposit"] < event["amount"]
                    or state.deposit < event["newDeposit"]
                ):
                    raise ChainEvidenceError("top-up event contradicts the intended funding")
            elif operation.kind is OperationKind.REQUEST_CLOSE:
                if (
                    state.status not in (SessionStatus.CLOSING, SessionStatus.CLOSED)
                    or event["graceEnd"] != state.close_requested_at + 3600
                    or state.close_requested_at != block.timestamp
                ):
                    raise ChainEvidenceError("close request contradicts canonical state")
            elif operation.kind in (OperationKind.CLOSE, OperationKind.WITHDRAW):
                if (
                    state.status is not SessionStatus.CLOSED
                    or event["captureAmount"] != state.settled_cumulative
                    or event["captureAmount"] + event["refunded"] != state.deposit
                    or (operation.kind is OperationKind.CLOSE and event["captureAmount"] != target)
                ):
                    raise ChainEvidenceError("close/refund event contradicts canonical state")
            elif operation.kind is OperationKind.RELEASE_IDLE:
                if (
                    state.status is not SessionStatus.CLOSED
                    or event["amount"] != state.deposit - state.settled_cumulative
                ):
                    raise ChainEvidenceError("idle refund contradicts canonical state")
            results.append(SessionOperationOutcome(session_id, target, covered, settlement))
        if operation.kind is OperationKind.SETTLE_BATCH:
            proven = any(result.settlement_reference is not None for result in results)
        return tuple(results), proven
