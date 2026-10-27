"""Chain round trips of one agent opening, against a synthetic Arc node.

A round trip is one JSON-RPC HTTP request, and a batch is one. The Client, its
ChainFollower and ChainReader and the submit and reconcile path are the SDK's
own; the node, the wallet service, the provider's Control peer and its
discovery documents are stand-ins. Arc blocks are final, so no earlier block is
re-read (docs.arc.io/arc/concepts/deterministic-finality). Event bytes come
from the compiled contract ABI, not the decoder under test.
"""

import asyncio
import contextlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest
from eth_abi import decode, encode
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi.v1 import mppi_pb2 as pb
from mppi.v1 import mppi_pb2_grpc as services
from mppi_agent import Client, ClientError, PaymentError
from mppi_agent import client as client_module
from mppi_agent.discovery import ProviderDiscovery
from mppi_protocol import CatalogRow, Rates, publish_catalog
from mppi_protocol import chain as chain_module
from mppi_protocol.capabilities import FundingAuthorization, WalletCall
from mppi_protocol.chain import ChainEvidenceError, ChainReader, CodePin, DeploymentProfile
from mppi_protocol.circle import funding_typed_data
from mppi_protocol.contract import ContractCalls
from mppi_protocol.follower import ChainFollower
from mppi_protocol.signing import LocalSessionSigner, voucher_digest
from mppi_protocol.types import (
    Domain,
    OperationResult,
    OperationState,
    SessionDescriptor,
)
from mppi_protocol.wire import GRPC_OPTIONS, descriptor_to_wire

COMPILED_ABI = json.loads(
    (Path(__file__).resolve().parents[4] / "contracts/abi/MPPI.json").read_text()
)
CHAIN = 5042002
PROXY, IMPLEMENTATION = b"\x99" * 20, b"\x98" * 20
REGISTRY, REGISTRY_IMPLEMENTATION = b"\x97" * 20, b"\x96" * 20
MULTICALL3 = bytes.fromhex("ca11bde05977b3631167028862be2a173976ca11")
DOMAIN = Domain(CHAIN, PROXY)
AGENT = b"\x5a" * 20  # A smart account, as a Circle Agent Wallet is.
EOA = Account.from_key(b"\x22" * 32)  # Public unfunded fixture: an account without code.
OPERATOR, RECIPIENT = b"\x33" * 20, b"\x22" * 20
PROVIDER_ID, AGENT_ID = 7, 43
MODEL = "test/model"
CATALOG = publish_catalog((CatalogRow(MODEL, Rates(500_000, 500_000)),))
SIGNER = LocalSessionSigner(bytes.fromhex("11" * 32))  # Public unfunded fixture.
TX = b"\xaa" * 32
CODE = {
    address: bytes([number]) + b"test-runtime"
    for number, address in enumerate(
        (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION, MULTICALL3, AGENT), start=1
    )
}
IMPLEMENTATIONS = {PROXY: IMPLEMENTATION, REGISTRY: REGISTRY_IMPLEMENTATION}
PROFILE = DeploymentProfile(
    DOMAIN,
    REGISTRY,
    *(
        CodePin(address, keccak(CODE[address]))
        for address in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION)
    ),
)
DESCRIPTOR_TYPE = "(address,address,uint256,address,address,address,bytes32)"
OPEN_TYPES = [DESCRIPTOR_TYPE, "uint64", "bytes32", "(uint256,uint256,bytes32,bytes)"]
STATE_TYPE = "(uint64,uint64,uint8,uint64,uint64,bytes32)"
FIRST_HEAD = 100


def hx(value: bytes) -> str:
    return "0x" + value.hex()


def selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


def block_hash(number: int) -> bytes:
    return keccak(number.to_bytes(32, "big"))


def address(value: str) -> bytes:
    return bytes.fromhex(value[2:])


class ArcNode:
    """Final blocks one second apart, ending at ``head``; ``lagging`` makes
    the node serve an older head, as one load-balanced backend can.

    Each ``call`` and each ``call_many`` is one HTTP request; ``trips`` names
    the step each one served, and ``batches`` its methods and the address each
    reads (an eth_call's target, an eth_getCode's account)."""

    def __init__(self) -> None:
        # The newest block is a few seconds old, so evidence is fresh.
        self.base = int(time.time()) - FIRST_HEAD - 10
        self.head = FIRST_HEAD
        self.lagging: int | None = None
        self.wallet = AGENT  # The funding wallet the agent id binds.
        self.trips: list[str] = []
        self.batches: list[list[tuple[str, bytes | None]]] = []
        self.opened: tuple[SessionDescriptor, bytes, int, int] | None = None
        # Added to the deposit after the opening, as a top-up the funding
        # wallet made outside the SDK would be.
        self.topped = 0
        self.receipt: dict | None = None

    def call(self, method: str, params: list) -> object:
        return self.call_many([(method, params)])[0]

    def call_many(self, calls: list[tuple[str, list]]) -> list[object]:
        self.trips.append(self._step(calls))
        self.batches.append(
            [
                (
                    method,
                    address(params[0]["to"])
                    if method == "eth_call"
                    else address(params[0])
                    if method == "eth_getCode"
                    else None,
                )
                for method, params in calls
            ]
        )
        return [self._answer(method, params) for method, params in calls]

    def _step(self, calls: list[tuple[str, list]]) -> str:
        methods = {method for method, _ in calls}
        targets = {address(params[0]["to"]) for method, params in calls if method == "eth_call"}
        if "eth_getTransactionReceipt" in methods:
            return "receipt"
        if MULTICALL3 in targets:
            return "snapshot"
        if self.wallet in targets:
            return "funding"
        return "attachment"

    def block(self, number: int) -> dict:
        return {
            "number": hex(number),
            "hash": hx(block_hash(number)),
            "parentHash": hx(block_hash(number - 1)),
            "timestamp": hex(self.base + number),
        }

    def _answer(self, method: str, params: list) -> object:
        head = self.head if self.lagging is None else self.lagging
        if method == "eth_chainId":
            return hex(CHAIN)
        if method == "eth_getBlockByNumber":
            number = head if params[0] == "latest" else int(params[0], 16)
            return self.block(number) if number <= head else None
        if method == "eth_getTransactionReceipt":
            assert params == [hx(TX)]
            return self.receipt
        *arguments, position = params
        if position == "latest":
            number = head
        else:
            assert position["requireCanonical"] is True
            (number,) = [n for n in range(head + 1) if hx(block_hash(n)) == position["blockHash"]]
        if method == "eth_getCode":
            return hx(CODE.get(address(arguments[0]), b""))
        if method == "eth_getStorageAt":
            return hx(bytes(12) + IMPLEMENTATIONS[address(arguments[0])])
        assert method == "eth_call" and set(arguments[0]) == {"to", "data"}
        target, data = address(arguments[0]["to"]), bytes.fromhex(arguments[0]["data"][2:])
        if target == MULTICALL3:
            assert data[:4] == selector("aggregate3((address,bool,bytes)[])")
            (views,) = decode(["(address,bool,bytes)[]"], data[4:])
            answers = [(True, self._view(address(to), view, number)) for to, _, view in views]
            return hx(encode(["(bool,bytes)[]"], [answers]))
        return hx(self._view(target, data, number))

    def _view(self, target: bytes, data: bytes, number: int) -> bytes:
        call, arguments = data[:4], data[4:]
        if target == MULTICALL3:
            return {
                selector("getBlockNumber()"): encode(["uint256"], [number]),
                selector("getCurrentBlockTimestamp()"): encode(["uint256"], [self.base + number]),
                selector("getLastBlockHash()"): block_hash(number - 1),
            }[call]
        if target == PROXY:
            if call == selector("voucherDigest(bytes32,uint64)"):
                return voucher_digest(DOMAIN, bytes(32), 0)
            if call == selector("pauseLevel()"):
                return encode(["uint8"], [0])
            assert call == selector("getSession(bytes32)")
            (session_id,) = decode(["bytes32"], arguments)
            return encode([STATE_TYPE], [self.session(session_id, number)])
        if target == AGENT:  # ERC-1271 isValidSignature: this account accepts it.
            assert call == bytes.fromhex("1626ba7e")
            return bytes.fromhex("1626ba7e") + bytes(28)
        assert target != self.wallet  # An account without code is never called.
        assert target == REGISTRY
        if call == selector("getMetadata(uint256,string)"):
            assert decode(["uint256", "string"], arguments) == (PROVIDER_ID, "mppi.catalog")
            return encode(["bytes"], [CATALOG.canonical_bytes])
        (identifier,) = decode(["uint256"], arguments)
        if call == selector("getAgentWallet(uint256)"):
            return encode(
                ["address"], [{PROVIDER_ID: RECIPIENT, AGENT_ID: self.wallet}[identifier]]
            )
        assert identifier == PROVIDER_ID
        if call == selector("ownerOf(uint256)"):
            return encode(["address"], [OPERATOR])
        assert call == selector("tokenURI(uint256)")
        return encode(["string"], ["https://provider.example/discover"])

    def session(self, session_id: bytes, number: int) -> tuple:
        if self.opened is None or session_id != self.opened[1] or number < self.opened[3]:
            return (0, 0, 0, 0, 0, bytes(32))
        _, _, deposit, opened_at = self.opened
        return (deposit + self.topped, 0, 0, 0, self.base + opened_at, CATALOG.rate_card_hash)

    def open(self, call: WalletCall) -> None:
        """Execute ``open`` five blocks on and emit its SessionOpened."""
        assert call.to == PROXY and call.data[:4] == selector("open(" + ",".join(OPEN_TYPES) + ")")
        fields, deposit, rate_card_hash, _ = decode(OPEN_TYPES, call.data[4:])
        assert rate_card_hash == CATALOG.rate_card_hash
        descriptor = SessionDescriptor(
            address(fields[0]),
            address(fields[1]),
            fields[2],
            address(fields[3]),
            address(fields[4]),
            address(fields[5]),
            fields[6],
        )
        session_id = keccak(
            encode(
                ["address"] * 2 + ["uint256"] + ["address"] * 3 + ["bytes32", "address", "uint256"],
                [*fields, PROXY, CHAIN],
            )
        )
        self.head += 5
        self.opened = (descriptor, session_id, deposit, self.head)
        event = next(
            item
            for item in COMPILED_ABI
            if item["type"] == "event" and item["name"] == "SessionOpened"
        )
        values = {
            "sessionId": session_id,
            "agent": descriptor.agent,
            "sessionSigner": descriptor.session_signer,
            "providerId": descriptor.provider_id,
            "recipient": descriptor.recipient,
            "operator": descriptor.operator,
            "token": descriptor.token,
            "salt": descriptor.salt,
            "deposit": deposit,
            "initialRateCardHash": rate_card_hash,
        }
        fields_of = event["inputs"]
        inclusion = {
            "transactionHash": hx(TX),
            "transactionIndex": "0x0",
            "blockNumber": hex(self.head),
            "blockHash": hx(block_hash(self.head)),
        }
        log = inclusion | {
            "address": hx(PROXY),
            "topics": [
                hx(keccak(text="SessionOpened(" + ",".join(f["type"] for f in fields_of) + ")"))
            ]
            + [hx(encode([f["type"]], [values[f["name"]]])) for f in fields_of if f["indexed"]],
            "data": hx(
                encode(
                    [f["type"] for f in fields_of if not f["indexed"]],
                    [values[f["name"]] for f in fields_of if not f["indexed"]],
                )
            ),
            "logIndex": "0x0",
            "removed": False,
        }
        self.receipt = inclusion | {"status": "0x1", "logs": [log]}


class Wallet:
    """A funding wallet whose service returns once mined: a smart account, or
    with ``eoa`` an account without code that signs by ECDSA."""

    def __init__(self, node: ArcNode, *, eoa: bool = False) -> None:
        self.node, self.eoa = node, eoa

    @property
    def address(self) -> bytes:
        return bytes.fromhex(EOA.address[2:]) if self.eoa else AGENT

    def sign_funding_authorization(self, authorization: FundingAuthorization) -> bytes:
        if self.eoa:
            message = encode_typed_data(full_message=funding_typed_data(authorization))
            return bytes(EOA.sign_message(message).signature)
        return b"\x01" * 65  # This account's isValidSignature accepts it.

    def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
        self.node.open(call)
        return OperationResult(operation_ref, OperationState.PENDING, "wallet-1", TX)

    def reconcile(self, *, operation_ref: str) -> OperationResult:
        return OperationResult(operation_ref, OperationState.PENDING, "wallet-1", TX)


class Provider(services.ControlServicer):
    """The provider's Control: a challenge, then the first attachment, whose
    snapshot names a block two after the opening's, as its follower has read.
    Each attachment is the session's next generation."""

    def __init__(self, node: ArcNode) -> None:
        self.node = node
        self.generation = 0

    def Session(self, requests, context):
        nonce = b"n" * 32
        yield pb.ControlMessage(reconnect_challenge=pb.ReconnectChallenge(nonce=nonce))
        for message in requests:
            kind = message.WhichOneof("kind")
            if kind == "challenge_request":
                yield pb.ControlMessage(
                    challenge=pb.Challenge(
                        request_id=message.challenge_request.request_id,
                        model=message.challenge_request.model,
                        rate_card_hash=CATALOG.rate_card_hash,
                        provider_id=PROVIDER_ID.to_bytes(32, "big"),
                        contract=PROXY,
                        chain_id=CHAIN,
                        operator=OPERATOR,
                        cover_output_tokens=1_000,
                        suggest_session_tokens=1_000_000,
                    )
                )
            elif kind == "reconnect_request":
                assert self.node.opened is not None
                descriptor, session_id, deposit, _ = self.node.opened
                self.node.head += 2
                self.generation += 1
                yield pb.ControlMessage(
                    session_state=pb.SessionState(
                        session_id=session_id,
                        nonce=nonce,
                        control_generation=self.generation,
                        ready=True,
                        close_pending=False,
                        descriptor=descriptor_to_wire(descriptor),
                        meter_seq=0,
                        catalogs=[
                            pb.CatalogVersion(
                                rate_card_hash=CATALOG.rate_card_hash,
                                catalog=CATALOG.canonical_bytes,
                            )
                        ],
                        initial_rate_card_hash=CATALOG.rate_card_hash,
                        current_rate_card_hash=CATALOG.rate_card_hash,
                        deposit=deposit + self.node.topped,
                        settled_cumulative=0,
                        status=pb.SESSION_STATUS_OPEN,
                        chain_block_number=self.node.head,
                        chain_block_hash=block_hash(self.node.head),
                    )
                )


@contextlib.contextmanager
def harness(tmp_path, monkeypatch, node, wallet):
    """A fresh process's Client, its follower and the provider's Control peer."""
    monkeypatch.setattr(chain_module, "_MULTICALL3_CODE_HASH", keccak(CODE[MULTICALL3]))
    server = grpc.server(ThreadPoolExecutor(max_workers=4), options=GRPC_OPTIONS)
    services.add_ControlServicer_to_server(Provider(node), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    origin = f"https://127.0.0.1:{port}"
    monkeypatch.setattr(
        client_module,
        "discover",
        lambda identity, **_: ProviderDiscovery(identity, origin, b"", b"", (MODEL,)),
    )
    monkeypatch.setattr(
        "mppi_agent.client.grpc.aio.secure_channel",
        lambda target, credentials, *, options: grpc.aio.insecure_channel(target, options=options),
    )
    # No background refresh runs during the test: its first poll is a minute away.
    follower = ChainFollower(
        ChainReader(node, PROFILE),
        max_age_ms=60_000,
        collection_margin_ms=60_000,
        poll_interval_ms=60_000,
    )
    node.wallet = wallet.address
    client = Client(
        PROVIDER_ID,
        wallet=wallet,
        signer=SIGNER,
        chain=follower,
        contract=ContractCalls(DOMAIN, "USDC", "2"),
        checkpoint_path=tmp_path / "agent.json",
        signer_reference="fixture:public-session-key",
        initial_session_tokens=1_000,
        agent_id=AGENT_ID,
    )
    try:
        yield client, follower
    finally:
        client.close()
        server.stop(None).wait()


@pytest.fixture
def opening(request, tmp_path, monkeypatch):
    """A fresh process opens one session through its first attachment."""
    node = ArcNode()
    options = getattr(request, "param", {})
    node.topped = options.get("topped", 0)
    wallet = Wallet(node, eoa=options.get("wallet") == "eoa")
    with harness(tmp_path, monkeypatch, node, wallet) as (client, follower):
        try:
            opened = client.sessions.open(PROVIDER_ID, model=MODEL)
        except ClientError as error:
            opened = error
        yield SimpleNamespace(client=client, node=node, follower=follower, opened=opened)


@pytest.mark.parametrize(
    "opening", [{}, {"wallet": "eoa"}], ids=["smart_account", "eoa"], indirect=True
)
def test_a_fresh_opening_reads_the_chain_in_two_round_trips_or_three_for_a_smart_account(
    opening,
) -> None:
    """The pre-open snapshot, which also reads the registry's binding for the
    agent id and the funding wallet's code; for a smart account, its code and
    isValidSignature of the funding signature at the snapshot's block; and the
    opening's receipt, its own proof. An account without code is checked by
    ECDSA recovery with no read, as USDC checks it, and the first attachment
    checks the provider's snapshot against the opening's proof."""
    node = opening.node
    assert opening.opened.operation.state is OperationState.CONFIRMED
    assert opening.opened.session is not None and opening.opened.session.readiness.ready
    assert ("eth_getCode", node.wallet) in node.batches[0]
    # The intent names the block it was made at: its opening lands after it.
    assert opening.client._saved["funding_intent"]["block"] == FIRST_HEAD
    if node.wallet == AGENT:
        assert node.trips == ["snapshot", "funding", "receipt"]
        # USDC's rule at the snapshot's block; the binding came with the snapshot.
        assert node.batches[1] == [
            ("eth_getBlockByNumber", None),
            ("eth_getCode", AGENT),
            ("eth_call", AGENT),
        ]
    else:
        assert node.trips == ["snapshot", "receipt"]
    # Once open, a refresh no longer reads the funding wallet.
    asyncio.run_coroutine_threadsafe(opening.follower.refresh(), opening.client._loop).result(10)
    assert ("eth_getCode", node.wallet) not in node.batches[-1]
    assert opening.follower.current.funding is None


@pytest.mark.parametrize("lagging", range(FIRST_HEAD - 1, FIRST_HEAD + 8))
def test_after_the_first_attachment_a_lagging_read_is_lag_not_a_regression(
    opening, lagging
) -> None:
    """The opening's state is recorded on the follower's block 100, before
    the opening's 105, with the opening's block as its proof
    (ChainFollower.confirm); blocks before 105 hold no deposit. A backend
    serving a head before 105 is behind the proof: lag, and requests stay
    ready. Without the proving block it would read as a regression and hold
    requests at INCONSISTENT_STATE until a later refresh is accepted. From
    105 on, through the provider's block 107, the read is accepted."""
    session_id = opening.opened.session.session_id
    opening.node.lagging = lagging
    refresh = asyncio.run_coroutine_threadsafe(opening.follower.refresh(), opening.client._loop)
    with contextlib.suppress(ChainEvidenceError):  # A background poll keeps its evidence.
        refresh.result(timeout=10)
    readiness = opening.client.sessions.get(session_id).readiness
    assert readiness.ready, readiness.failures
    opening.node.lagging = None
    asyncio.run_coroutine_threadsafe(opening.follower.refresh(), opening.client._loop).result(10)
    assert opening.follower.current.block.observation.block_number == opening.node.head
    assert opening.client.sessions.get(session_id).readiness.ready


@pytest.mark.parametrize("opening", [{"topped": 5}], indirect=True)
def test_a_first_snapshot_that_differs_from_the_opening_s_proof_reads_the_chain_next(
    opening,
) -> None:
    """The funding wallet topped the new session up outside the SDK before
    its first attachment, so the provider's snapshot holds more than the
    opening proved: the attachment fails closed, having read nothing. The
    next open reads the session at the provider's block and attaches."""
    assert isinstance(opening.opened, ClientError)
    assert "differ from canonical state" in str(opening.opened)
    assert opening.node.trips == ["snapshot", "funding", "receipt"]
    reopened = opening.client.sessions.open(PROVIDER_ID, model=MODEL)
    assert reopened.session is not None and reopened.session.readiness.ready
    assert reopened.session.deposit == opening.node.opened[2] + 5
    assert opening.node.trips[-1] == "attachment"


class HeldNode(ArcNode):
    """Holds its next snapshot read until released, as a slow backend would."""

    def __init__(self) -> None:
        super().__init__()
        self.hold = False
        self.entered, self.release = threading.Event(), threading.Event()

    def call_many(self, calls: list[tuple[str, list]]) -> list[object]:
        if self.hold and self._step(calls) == "snapshot":
            self.hold = False
            self.entered.set()
            assert self.release.wait(10)
        return super().call_many(calls)


def test_an_opening_at_the_follower_s_own_block_outlives_a_refresh_begun_before_its_watch(
    tmp_path, monkeypatch
) -> None:
    """While the wallet submits the opening, mined at block 105, a background
    refresh accepts block 105 itself, and the next one, at 106, is still
    reading without the session when open() records the opening's proof at
    the follower's own block. That read is behind the proof: lag, and its
    repeat reads the session, so the first attachment finds the session in
    the follower's evidence. Before, the read replaced the proven state and
    the attachment raised a bare KeyError after funding."""
    node = HeldNode()
    held = {}

    class RacingWallet(Wallet):
        def execute(self, call: WalletCall, *, operation_ref: str) -> OperationResult:
            result = super().execute(call, operation_ref=operation_ref)
            loop = held["client"]._loop
            follower = held["follower"]
            asyncio.run_coroutine_threadsafe(follower.refresh(), loop).result(10)
            node.head += 1
            node.hold = True
            held["refresh"] = asyncio.run_coroutine_threadsafe(follower.refresh(), loop)
            assert node.entered.wait(10)
            return result

    with harness(tmp_path, monkeypatch, node, RacingWallet(node)) as (client, follower):
        held["client"], held["follower"] = client, follower
        confirm_opened, attach = client._confirm_opened, client._attach

        def confirm_then_release(ledger, outcome):
            proven = confirm_opened(ledger, outcome)
            assert proven is not None  # At the follower's own block, 105.
            assert follower.current.block.observation == outcome.observation
            node.release.set()
            return proven

        async def attach_after_the_held_refresh(*args, **kwargs):
            await asyncio.wrap_future(held["refresh"])
            return await attach(*args, **kwargs)

        client._confirm_opened = confirm_then_release
        client._attach = attach_after_the_held_refresh
        opened = client.sessions.open(PROVIDER_ID, model=MODEL)
        assert opened.operation.state is OperationState.CONFIRMED
        assert opened.session is not None and opened.session.readiness.ready
        assert follower.current.block.observation.block_number == 106
        assert opened.session.session_id in follower.current.sessions
        assert "attachment" not in node.trips  # Checked against its proof, unread.


def test_a_new_session_no_accepted_read_holds_attaches_on_its_opening_s_proof(
    tmp_path, monkeypatch
) -> None:
    """Should the follower's evidence not hold the new session at its first
    attachment, the opening's own proof stands for its state: the attachment
    is applied but not ready, and open() raises SESSION_NOT_READY /
    STATE_UNAVAILABLE instead of a bare KeyError after funding. The next open()
    reads the session at the provider's block and is ready."""
    node = ArcNode()
    with harness(tmp_path, monkeypatch, node, Wallet(node)) as (client, follower):
        attach = client._attach

        async def attach_without_the_session(*args, **kwargs):
            session_id = client._ledger.session_id
            follower.unwatch_session(session_id)  # Drops its proof as well.
            await follower.refresh()
            assert session_id not in follower.current.sessions
            follower.follow(sessions=(session_id,))
            return await attach(*args, **kwargs)

        client._attach = attach_without_the_session
        with pytest.raises(PaymentError) as failure:
            client.sessions.open(PROVIDER_ID, model=MODEL)
        assert failure.value.failure.reason == "STATE_UNAVAILABLE"
        assert client._attached_generation == 1  # Applied, and kept.
        saved = json.loads(client._checkpoint.path.read_text())
        assert saved["ledger"]["generation"] == 1
        client._attach = attach
        reopened = client.sessions.open(PROVIDER_ID, model=MODEL)
        assert reopened.session is not None and reopened.session.readiness.ready
        assert node.trips[-1] == "attachment"  # Read at the provider's block.
