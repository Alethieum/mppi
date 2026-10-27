"""Circle CLI adapter boundaries; no credentials, actual CLI or network calls.

Subprocess limit checks run a tiny local Python executable in the Apple test
container. Independent CLI fixtures use the published 1.1.4 output schema.
"""

import copy
import errno
import json
import os
import stat
import subprocess
import sys
import threading
import time
from dataclasses import replace
from uuid import uuid4

import pytest
from eth_abi import encode
from eth_account.messages import encode_typed_data
from eth_utils import keccak
from mppi_protocol import circle, local_state
from mppi_protocol.capabilities import FundingAuthorization, NotSubmittedError, WalletCall
from mppi_protocol.chain import ChainReader, CodePin, DeploymentProfile
from mppi_protocol.circle import (
    _MAX_JOURNAL_BYTES,
    CircleAgentWallet,
    CircleAgentWalletError,
    _digest,
)
from mppi_protocol.contract import ARC_USDC, ContractCalls
from mppi_protocol.deployment import profile_to_mapping
from mppi_protocol.errors import ValidationError
from mppi_protocol.local_state import Checkpoint
from mppi_protocol.operations import submit_operation
from mppi_protocol.signing import voucher_digest
from mppi_protocol.types import Domain, OperationState, SessionDescriptor

CHAIN = 5042002
PROXY = b"\x99" * 20
IMPLEMENTATION = b"\x98" * 20
REGISTRY = b"\x97" * 20
REGISTRY_IMPLEMENTATION = b"\x96" * 20
WALLET = b"\x11" * 20
TOKEN = bytes.fromhex("3600000000000000000000000000000000000000")
HASH = b"\xbb" * 32
TX = b"\xaa" * 32
ID = "11111111-2222-4333-8444-555555555555"
DOMAIN = Domain(CHAIN, PROXY)
CALL = WalletCall(CHAIN, PROXY, b"\x01\x23\x45\x67")


def hx(value):
    return "0x" + value.hex()


def whole(call, row):
    """``row`` as releases before the digest kept it: the whole call, no
    retirement mark, and before route selection no route."""
    excluded = ("call_digest", "retire_at", "route")
    kept = {key: value for key, value in row.items() if key not in excluded}
    return kept | {"call": {"chain_id": call.chain_id, "to": hx(call.to), "data": hx(call.data)}}


class Rpc:
    def __init__(self):
        self.calls = []
        self.code = {
            address: bytes([i]) + b"runtime"
            for i, address in enumerate(
                (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION), start=1
            )
        }
        self.chain = CHAIN

    def call(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "eth_chainId":
            return hex(self.chain)
        if method == "eth_getBlockByNumber":
            return {
                "number": "0x64",
                "hash": hx(HASH),
                "parentHash": hx(b"\xbc" * 32),
                "timestamp": "0x1234",
            }
        assert params[-1] == {"blockHash": hx(HASH), "requireCanonical": True}
        if method == "eth_getCode":
            return hx(self.code[bytes.fromhex(params[0][2:])])
        if method == "eth_getStorageAt":
            address = bytes.fromhex(params[0][2:])
            target = {PROXY: IMPLEMENTATION, REGISTRY: REGISTRY_IMPLEMENTATION}[address]
            return hx(bytes(12) + target)
        if method == "eth_call":
            assert params[0] == {
                "to": hx(PROXY),
                "data": hx(
                    keccak(text="voucherDigest(bytes32,uint64)")[:4]
                    + encode(["bytes32", "uint64"], [bytes(32), 0])
                ),
            }
            return hx(
                encode(["bytes32"], [voucher_digest(Domain(self.chain, PROXY), bytes(32), 0)])
            )
        raise AssertionError(method)


class Cli:
    def __init__(self, path):
        self.path = path
        self.blockchain = "ARC-TESTNET"
        self.calls = []
        self.version = b"1.1.4\n"
        self.wallets = [{"type": "agent", "address": hx(WALLET), "blockchain": "ARC-TESTNET"}]
        self.execute_error = False
        self.reply_changes = {}
        self.pages = [[]]
        self.signing_response = "0x" + "11" * 140
        self.signing_data = None
        self.before_submit = None

    def transaction(self, **changes):
        return {
            "id": ID,
            "state": "CONFIRMED",
            "blockchain": self.blockchain,
            "txHash": hx(TX),
            "sourceAddress": hx(WALLET),
            "destinationAddress": hx(WALLET),
            "operation": "CONTRACT_EXECUTION",
            "transactionType": "OUTBOUND",
            "abiFunctionSignature": "execute(address,uint256,bytes)",
            "abiParameters": [hx(PROXY), "0", hx(CALL.data)],
            "contractAddress": hx(WALLET),
            "createDate": "2026-09-27T00:00:00Z",
            **changes,
        }

    def __call__(self, args):
        self.calls.append(copy.deepcopy(args))
        if args == ["--version"]:
            return self.version
        assert args[-2:] == ["--output", "json"]
        args = args[:-2]
        if args[:2] == ["wallet", "list"]:
            assert args == ["wallet", "list", "--type", "agent", "--chain", self.blockchain]
            data = {"wallets": self.wallets}
        elif args[:2] == ["wallet", "execute"]:
            # Real journal bytes must already exist before the command starts.
            record = json.loads(self.path.read_text())
            row = record["operations"]["original-ref"]
            expected = [
                "wallet",
                "execute",
                "execute(address,uint256,bytes)",
                hx(PROXY),
                "0",
                hx(CALL.data),
                "--contract",
                hx(WALLET),
                "--address",
                hx(WALLET),
                "--chain",
                self.blockchain,
                "--idempotency-key",
                row["idempotency_key"],
            ]
            assert args == expected
            assert row["wallet_operation_id"] is None and row["state"] == "unknown"
            if self.before_submit:
                self.before_submit(row)
            if self.execute_error:
                raise CircleAgentWalletError("mock timeout")
            data = self.transaction(idempotencyKey=row["idempotency_key"], **self.reply_changes)
        elif args[:2] == ["transaction", "list"]:
            expected = [
                "transaction",
                "list",
                "--address",
                hx(WALLET),
                "--chain",
                self.blockchain,
                "--operation",
                "execute",
                "--tx-type",
                "outbound",
                "--limit",
                "50",
            ]
            assert args[: len(expected)] == expected
            cursor = args[-1] if "--cursor" in args else None
            if cursor is None:
                data = {"transactions": self.pages[0]}
            else:
                prior = next(
                    i for i, page in enumerate(self.pages) if page and page[-1]["id"] == cursor
                )
                data = {"transactions": self.pages[prior + 1]}
        elif args[:3] == ["wallet", "sign", "typed-data"]:
            assert args[4:] == ["--address", hx(WALLET), "--chain", self.blockchain]
            self.signing_data = json.loads(args[3])
            data = {"signature": self.signing_response}
        else:
            raise AssertionError(args)
        return json.dumps({"data": data}).encode()


@pytest.fixture
def context(tmp_path, monkeypatch):
    rpc = Rpc()
    pins = [
        CodePin(address, keccak(rpc.code[address]))
        for address in (PROXY, IMPLEMENTATION, REGISTRY, REGISTRY_IMPLEMENTATION)
    ]
    chain = ChainReader(rpc, DeploymentProfile(DOMAIN, REGISTRY, *pins))
    path = tmp_path / "wallet.json"
    cli = Cli(path)
    options = {"chain": chain, "address": WALLET, "checkpoint_path": path}
    wallet = CircleAgentWallet(**options)
    monkeypatch.setattr(wallet, "_run", cli)
    return wallet, rpc, cli, options


def test_exact_call_is_journaled_before_single_submit(context):
    wallet, _, cli, _ = context
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.PENDING  # Canonical chain confirmation remains separate.
    assert result.tx_hash == TX and result.wallet_operation_id == ID
    cli.pages = [[cli.transaction()]]
    repeated = wallet.execute(CALL, operation_ref="original-ref")
    assert repeated == result
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1
    with pytest.raises(ValidationError, match="reused"):
        wallet.execute(replace(CALL, data=b"different"), operation_ref="original-ref")


def test_restart_recovers_original_resource_with_reads_only(context, monkeypatch):
    wallet, _, cli, options = context
    original = wallet.execute(CALL, operation_ref="original-ref")
    cli.calls.clear()
    cli.pages = [[cli.transaction(state="COMPLETE")]]
    reopened = CircleAgentWallet(**options)
    monkeypatch.setattr(reopened, "_run", cli)
    assert reopened.execute(CALL, operation_ref="original-ref") == original
    assert all(args[:2] == ["transaction", "list"] for args in cli.calls)


def test_no_returned_handle_remains_unknown_even_with_matching_new_transactions(
    context, monkeypatch
):
    wallet, _, cli, options = context
    cli.execute_error = True
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN and result.tx_hash is None
    key = json.loads(cli.path.read_text())["operations"]["original-ref"]["idempotency_key"]
    cli.calls.clear()
    cli.execute_error = False
    cli.pages = [[cli.transaction()]]
    reopened = CircleAgentWallet(**options)
    monkeypatch.setattr(reopened, "_run", cli)
    assert reopened.execute(CALL, operation_ref="original-ref") == result
    assert reopened.reconcile(operation_ref="original-ref") == result
    assert cli.calls == []
    assert json.loads(cli.path.read_text())["operations"]["original-ref"]["idempotency_key"] == key


def test_a_lost_execute_result_keeps_the_adapters_own_cause(context):
    wallet, _, cli, _ = context
    cli.execute_error = True
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert (result.state, result.wallet_operation_id) == (OperationState.UNKNOWN, None)
    assert result.cause == "CircleAgentWalletError: mock timeout"


def test_a_failed_lookup_keeps_handles_and_the_adapters_own_cause(context, monkeypatch):
    wallet, _, cli, _ = context
    original = wallet.execute(CALL, operation_ref="original-ref")
    cli.pages = [[]]
    missing = wallet.reconcile(operation_ref="original-ref")
    assert (missing.state, missing.wallet_operation_id) == (OperationState.UNKNOWN, ID)
    assert missing.cause == (
        "CircleAgentWalletError: Circle history does not list the original transaction"
    )

    def refused(_arguments):
        raise CircleAgentWalletError("Circle command did not return success (AUTH_EXPIRED)")

    monkeypatch.setattr(wallet, "_run", refused)
    result = wallet.reconcile(operation_ref="original-ref")
    assert (result.wallet_operation_id, result.tx_hash) == (ID, original.tx_hash)
    assert result.cause == (
        "CircleAgentWalletError: Circle command did not return success (AUTH_EXPIRED)"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"sourceAddress": hx(PROXY)},
        {"blockchain": "ARC"},
        {"contractAddress": hx(PROXY)},
        {"destinationAddress": hx(PROXY)},
        {"operation": "TRANSFER"},
        {"transactionType": "INBOUND"},
        {"abiFunctionSignature": "other(address,uint256,bytes)"},
        {"abiParameters": [hx(PROXY), "1", hx(CALL.data)]},
        {"abiParameters": [hx(REGISTRY), "0", hx(CALL.data)]},
        {"abiParameters": [hx(PROXY), "0", "0x1234"]},
        {"abiParameters": [hx(PROXY), 0, hx(CALL.data)]},
        {"state": "INVENTED"},
        {"idempotencyKey": "wrong"},
    ],
)
def test_execute_response_must_bind_exact_intent(context, changes):
    wallet, _, cli, _ = context
    cli.reply_changes = changes
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.wallet_operation_id is None and result.tx_hash is None


@pytest.mark.parametrize(
    "problem", ["missing", "foreign_hash", "wrong_call", "duplicate", "malformed"]
)
def test_lookup_outage_and_conflicts_preserve_handles(context, problem):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    if problem == "missing":
        cli.pages = [[]]
    elif problem == "foreign_hash":
        cli.pages = [[cli.transaction(txHash=hx(b"\xdd" * 32))]]
    elif problem == "wrong_call":
        cli.pages = [[cli.transaction(abiParameters=[hx(PROXY), "0", "0x00"])]]
    elif problem == "duplicate":
        cli.pages = [[cli.transaction(), cli.transaction()]]
    else:
        cli.pages = [[{"id": ID}]]
    result = wallet.reconcile(operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.wallet_operation_id == ID and result.tx_hash == TX


def test_paginated_known_id_and_no_latest_match_heuristic(context):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    first = [cli.transaction(id=str(uuid4())) for _ in range(50)]
    cli.pages = [first, [cli.transaction(state="FAILED")]]
    result = wallet.reconcile(operation_ref="original-ref")
    assert result.state is OperationState.FAILED and result.wallet_operation_id == ID
    assert cli.calls[-1][-4:] == ["--cursor", first[-1]["id"], "--output", "json"]


def test_page_limit_fails_closed(context):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    wallet._max_pages = 1
    cli.pages = [[cli.transaction(id=str(uuid4())) for _ in range(50)], [cli.transaction()]]
    assert wallet.reconcile(operation_ref="original-ref").state is OperationState.UNKNOWN


@pytest.mark.parametrize(
    "problem",
    [
        "version",
        "account",
        "network",
        "duplicate",
        "malformed_address",
    ],
)
def test_cli_selection_checked_before_any_write(context, problem):
    wallet, rpc, cli, _ = context
    if problem == "version":
        cli.version = b"1.1.5\n"
    elif problem == "account":
        cli.wallets[0]["type"] = "local"
    elif problem == "network":
        cli.wallets[0]["blockchain"] = "ARC"
    elif problem == "duplicate":
        cli.wallets *= 2
    else:
        cli.wallets[0]["address"] = {}
    with pytest.raises((CircleAgentWalletError, RuntimeError)):
        wallet.execute(CALL, operation_ref="original-ref")
    assert not cli.path.exists() and rpc.calls == []
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)


def test_journal_failure_prevents_submission(context, monkeypatch):
    wallet, _, cli, _ = context

    def fail(_self, _):
        raise OSError("storage failure")

    monkeypatch.setattr(Checkpoint, "save", fail)
    with pytest.raises(NotSubmittedError) as failure:  # Circle was never asked.
        wallet.execute(CALL, operation_ref="original-ref")
    assert isinstance(failure.value.__cause__, OSError)
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)


def test_a_failed_wallet_check_before_the_journal_is_a_definite_not_submitted(context):
    """A check inside execute fails before the intent is journaled: Circle was
    never asked, so the operation definitely failed and keeps the error kind."""
    wallet, rpc, cli, options = context
    cli.version = b"1.1.5\n"  # verify_wallet rejects the CLI.
    descriptor = SessionDescriptor(
        WALLET, b"\x05" * 20, 42, b"\x06" * 20, b"\x07" * 20, ARC_USDC, b"\x08" * 32
    )
    operation = ContractCalls(DOMAIN, "USDC", "2").request_close(descriptor)
    result = submit_operation(
        wallet, operation, operation_ref="original-ref", chain=options["chain"]
    )
    assert result.state is OperationState.FAILED and result.tx_hash is None
    assert result.cause == "NotSubmittedError: CircleAgentWalletError"
    assert not any(args[:2] == ["wallet", "execute"] for args in cli.calls)
    assert wallet.reconcile(operation_ref="original-ref").state is OperationState.UNKNOWN


def test_funding_signature_bytes_preserved_and_typed_data_independent(context):
    wallet, _, cli, _ = context
    auth = FundingAuthorization(
        CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 5_510_000, 10, 20, b"\x33" * 32
    )
    signature = wallet.sign_funding_authorization(auth)
    assert signature == b"\x11" * 140  # Not an ECDSA 65-byte SessionSigner proof.
    message = encode_typed_data(full_message=cli.signing_data)
    expected = keccak(
        encode(
            ["bytes32", "address", "address", "uint256", "uint256", "uint256", "bytes32"],
            [
                keccak(
                    text="ReceiveWithAuthorization(address from,address to,uint256 value,uint256 validAfter,uint256 validBefore,bytes32 nonce)"
                ),
                WALLET,
                PROXY,
                5_510_000,
                10,
                20,
                b"\x33" * 32,
            ],
        )
    )
    assert message.body == expected
    assert not cli.path.exists()


@pytest.mark.parametrize("field,value", [("chain_id", 5042), ("owner", PROXY), ("token", PROXY)])
def test_signing_rejects_wrong_funding_authority_before_cli(context, field, value):
    wallet, _, cli, _ = context
    auth = FundingAuthorization(CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32)
    with pytest.raises(ValidationError):
        wallet.sign_funding_authorization(replace(auth, **{field: value}))
    assert cli.calls == []


@pytest.mark.parametrize("signature", ["0x", "0xzz", "0x11 22", "secret", "0x" + "11" * 65537])
def test_bad_signing_responses_rejected(context, signature):
    wallet, _, cli, _ = context
    cli.signing_response = signature
    auth = FundingAuthorization(CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32)
    with pytest.raises(CircleAgentWalletError):
        wallet.sign_funding_authorization(auth)


def test_restart_rejects_changed_profile_and_tampered_journal(context):
    wallet, _, cli, options = context
    wallet.execute(CALL, operation_ref="original-ref")
    with pytest.raises(CircleAgentWalletError):
        CircleAgentWallet(**(options | {"address": b"\x13" * 20}))
    record = json.loads(cli.path.read_text())
    row = record["operations"]["original-ref"]
    record["operations"]["original-ref"] = whole(replace(CALL, chain_id=5042), row)
    cli.path.write_text(json.dumps(record))
    with pytest.raises(CircleAgentWalletError):
        CircleAgentWallet(**options)


@pytest.mark.parametrize(
    "chain_id,blockchain,other_chain", [(5042, "ARC", 5042002), (5042002, "ARC-TESTNET", 5042)]
)
def test_circle_funding_intent_and_cli_selection_are_isolated_by_network(
    context, monkeypatch, chain_id, blockchain, other_chain
):
    _, rpc, cli, options = context
    rpc.chain = chain_id
    profile = options["chain"].profile
    chain = ChainReader(rpc, replace(profile, domain=replace(profile.domain, chain_id=chain_id)))
    wallet = CircleAgentWallet(**(options | {"chain": chain}))
    cli.blockchain = blockchain
    cli.wallets[0]["blockchain"] = blockchain
    monkeypatch.setattr(wallet, "_run", cli)
    authorization = FundingAuthorization(
        chain_id, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32
    )
    with pytest.raises(ValidationError, match="funding authorization mismatch"):
        wallet.sign_funding_authorization(replace(authorization, chain_id=other_chain))
    assert cli.calls == [] and rpc.calls == []
    assert wallet.sign_funding_authorization(authorization) == b"\x11" * 140
    assert cli.signing_data["domain"]["chainId"] == chain_id
    assert cli.calls[-1][-4:] == ["--chain", blockchain, "--output", "json"]
    assert not cli.path.exists()  # Signing alone does not create an operation or submit it.


def test_retained_circle_journal_cannot_be_reused_on_another_network(context):
    wallet, rpc, cli, options = context
    wallet.execute(CALL, operation_ref="original-ref")
    original = cli.path.read_bytes()
    before = list(rpc.calls), list(cli.calls)
    profile = options["chain"].profile
    mainnet = ChainReader(rpc, replace(profile, domain=replace(profile.domain, chain_id=5042)))
    with pytest.raises(CircleAgentWalletError, match="^Circle journal could not be validated$"):
        CircleAgentWallet(**(options | {"chain": mainnet}))
    assert cli.path.read_bytes() == original
    assert (rpc.calls, cli.calls) == before


@pytest.mark.parametrize(
    "script,expected",
    [
        ("import sys; sys.stdout.write('x' * 1048577)", "output limit"),
        ("import sys; sys.stderr.write('private-material-' * 6000)", "error-output limit"),
        ("import time; time.sleep(5)", "timed out"),
        ("import sys; sys.stderr.write('private-material'); sys.exit(7)", "success"),
        # CLI 1.1.4 prints its error as JSON on stdout and exits 1 (dist/index.js:732-750).
        (
            'print(\'{"error":{"code":"AUTH_EXPIRED","message":'
            '"private-material https://u:p@example.test"}}\'); exit(1)',
            r"^Circle command did not return success \(AUTH_EXPIRED\)$",
        ),
        (
            'print(\'{"error":{"code":"private-material"}}\'); exit(1)',
            "^Circle command did not return success$",
        ),
        ('print(\'{"error":{"code":{}}}\'); exit(1)', "^Circle command did not return success$"),
        ("print('private-material'); exit(1)", "^Circle command did not return success$"),
    ],
)
def test_actual_subprocess_bounds_and_redacted_errors(context, script, expected):
    _, _, _, options = context
    bounded = CircleAgentWallet(
        **(options | {"command": (sys.executable, "-c", script), "timeout_seconds": 0.3})
    )
    with pytest.raises(CircleAgentWalletError, match=expected) as error:
        bounded._run([])
    assert "private-material" not in str(error.value)


@pytest.mark.skipif(not hasattr(os, "WNOWAIT"), reason="needs waitid to hold a child unreaped")
@pytest.mark.parametrize(
    ("script", "expected"),
    [
        ("import sys; sys.stdout.write('x' * 1048577)", "output limit"),
        ("import time; time.sleep(0.6)", "timed out"),
    ],
)
def test_a_command_that_exits_before_its_group_is_stopped_keeps_its_own_error(
    context, monkeypatch, script, expected
):
    # The race a stop can lose: the command exits, unreaped, before its group
    # is signalled. macOS then refuses the signal (EPERM) and Linux delivers it
    # to the zombie; either way the command's own error stands, with no note.
    _, _, _, options = context
    signal_group = os.killpg

    def once_exited(group: int, number: int) -> None:
        deadline = time.monotonic() + 10
        while os.waitid(os.P_PID, group, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
            assert time.monotonic() < deadline, "the command never exited"
            time.sleep(0.01)
        signal_group(group, number)

    monkeypatch.setattr(os, "killpg", once_exited)
    bounded = CircleAgentWallet(
        **(options | {"command": (sys.executable, "-c", script), "timeout_seconds": 0.3})
    )
    with pytest.raises(CircleAgentWalletError, match=expected) as error:
        bounded._run([])
    assert getattr(error.value, "__notes__", []) == []


@pytest.mark.parametrize("refusal", [errno.EPERM, errno.EINVAL])
def test_a_stop_refused_while_the_command_lives_is_noted_on_its_own_error(
    context, monkeypatch, tmp_path, refusal
):
    # The command runs until it is released. An EPERM leaves it running when
    # the adapter checks, so the stop failed; any other refusal is a failure
    # outright. Either way the command's own error stands, noted.
    _, _, _, options = context
    release = tmp_path / "release"
    script = (
        "import os, time\n"
        "end = time.monotonic() + 30\n"
        f"while not os.path.exists({str(release)!r}) and time.monotonic() < end:\n"
        "    time.sleep(0.01)"
    )
    checked = []
    poll = subprocess.Popen.poll

    def refused(group: int, number: int) -> None:
        if refusal != errno.EPERM:
            release.touch()
        raise OSError(refusal, os.strerror(refusal))

    def observed(process: subprocess.Popen[bytes]) -> int | None:
        returncode = poll(process)
        checked.append(returncode)
        release.touch()
        return returncode

    monkeypatch.setattr(os, "killpg", refused)
    monkeypatch.setattr(subprocess.Popen, "poll", observed)
    bounded = CircleAgentWallet(
        **(options | {"command": (sys.executable, "-c", script), "timeout_seconds": 0.3})
    )
    try:
        with pytest.raises(CircleAgentWalletError, match="timed out") as error:
            bounded._run([])
    finally:
        release.touch()
    assert error.value.__notes__ == ["Circle command cleanup did not complete"]
    if refusal == errno.EPERM:
        assert checked == [None]


def test_actual_subprocess_arguments_never_evaluated_by_shell(context):
    _, _, _, options = context
    direct = CircleAgentWallet(
        **(
            options
            | {
                "command": (
                    sys.executable,
                    "-c",
                    "import json,sys; print(json.dumps(sys.argv[1:]))",
                )
            }
        )
    )
    arguments = ["$(touch /tmp/never-run)", "`echo private`", "; echo bad"]
    assert json.loads(direct._run(arguments)) == arguments


def test_scoped_journal_lock_is_released_between_operations(context):
    _, _, cli, options = context
    # Another process holds the journal past the execute's timeout: the
    # ordinary journal error, never a definite not-submitted, and Circle is
    # asked nothing.
    wallet = CircleAgentWallet(**(options | {"timeout_seconds": 0.3}))
    wallet._run = cli
    held = Checkpoint(options["checkpoint_path"])
    try:
        started = time.monotonic()
        with pytest.raises(ValidationError) as refused:
            wallet.execute(CALL, operation_ref="original-ref")
        assert time.monotonic() - started >= 0.3
    finally:
        held.close()
    assert not isinstance(refused.value, NotSubmittedError)
    assert cli.calls == []
    assert wallet.execute(CALL, operation_ref="original-ref").wallet_operation_id == ID
    next_owner = Checkpoint(options["checkpoint_path"])
    next_owner.close()


def held_for_a_while(path):
    """Another process holding the journal at ``path`` for 1.5 seconds, from
    when this returns."""
    holder = Checkpoint(path)
    release = threading.Timer(1.5, holder.close)
    release.start()
    return release


def test_execute_and_reconcile_wait_for_the_journal_up_to_their_own_timeout(context):
    wallet, _, cli, options = context
    # Another process holds the journal for longer than the one second other
    # records wait for, but within the CLI timeout (240 s by default).
    release = held_for_a_while(options["checkpoint_path"])
    executed = wallet.execute(CALL, operation_ref="original-ref")
    release.join()
    assert executed.state is OperationState.PENDING
    cli.pages = [[cli.transaction()]]
    release = held_for_a_while(options["checkpoint_path"])
    assert wallet.reconcile(operation_ref="original-ref") == executed
    release.join()
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


def kept_answer(options, cli):
    """An execute whose record step another process keeps out past the 0.3 s
    timeout of the wallet returned: UNKNOWN with Circle's transaction id and
    hash, which that wallet keeps for its next journal step."""
    wallet = CircleAgentWallet(**(options | {"timeout_seconds": 0.3}))
    wallet._run = cli
    path, held = options["checkpoint_path"], []
    # Another process takes the journal while Circle executes, and keeps it
    # past the execute's timeout.
    cli.before_submit = lambda row: held.append(Checkpoint(path))
    result = wallet.execute(CALL, operation_ref="original-ref")
    held.pop().close()
    cli.before_submit = None
    return wallet, result


def test_a_result_the_journal_cannot_take_is_returned_and_written_at_the_next_step(context):
    _, _, cli, options = context
    path = options["checkpoint_path"]
    wallet, result = kept_answer(options, cli)
    # Unknown, with Circle's transaction id and hash for the runtime to check.
    assert (result.state, result.wallet_operation_id, result.tx_hash) == (
        OperationState.UNKNOWN,
        ID,
        TX,
    )
    assert result.cause == "ValidationError"

    def row():
        return json.loads(path.read_text())["operations"]["original-ref"]

    assert (row()["wallet_operation_id"], row()["tx_hash"]) == (None, None)
    # The adapter's next journal step writes them, here a retire that names
    # no reference.
    wallet.retire(operation_refs=(), at=time.time())
    assert (row()["wallet_operation_id"], row()["tx_hash"]) == (ID, hx(TX))
    # So a later reconcile, even from another adapter object, finds the
    # transaction in Circle's history.
    reopened = CircleAgentWallet(**(options | {"require_existing": True}))
    reopened._run = cli
    cli.pages = [[cli.transaction(idempotencyKey=row()["idempotency_key"])]]
    found = reopened.reconcile(operation_ref="original-ref")
    assert (found.state, found.wallet_operation_id, found.tx_hash) == (
        OperationState.PENDING,
        ID,
        TX,
    )
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


@pytest.mark.parametrize(
    "script,needs_kill",
    [
        ("print('done')", False),
        ("import sys; sys.exit(7)", False),
        ("import time; time.sleep(5)", True),
        (
            "import sys,time; sys.stdout.write('x' * 1048577); sys.stdout.flush(); time.sleep(5)",
            True,
        ),
    ],
)
def test_process_cleanup_signals_before_reap_only_on_live_failure(
    context, monkeypatch, script, needs_kill
):
    _, _, _, options = context
    events = []
    killpg, wait = os.killpg, subprocess.Popen.wait

    def kill(pid, sig):
        events.append("kill")
        return killpg(pid, sig)

    def waited(process, *args, **kwargs):
        events.append("wait")
        return wait(process, *args, **kwargs)

    monkeypatch.setattr(os, "killpg", kill)
    monkeypatch.setattr(subprocess.Popen, "wait", waited)
    process = CircleAgentWallet(
        **(options | {"command": (sys.executable, "-c", script), "timeout_seconds": 0.3})
    )
    try:
        process._run([])
    except CircleAgentWalletError:
        pass
    if needs_kill:
        assert events[:2] == ["kill", "wait"]
    else:
        assert "kill" not in events and "wait" in events


@pytest.mark.parametrize("overflow", [False, True])
def test_custody_command_finishes_without_timeout_kill(context, monkeypatch, tmp_path, overflow):
    _, _, _, options = context
    marker = tmp_path / "original-finished"
    script = (
        "import pathlib,sys,time; "
        + ("sys.stdout.write('x' * 1048577); sys.stdout.flush(); " if overflow else "")
        + "time.sleep(0.4); pathlib.Path(sys.argv[1]).write_text('finished'); print('done')"
    )

    def unexpected_kill(*args):
        pytest.fail("an original custody command must not be killed")

    monkeypatch.setattr(os, "killpg", unexpected_kill)
    wallet = CircleAgentWallet(
        **(
            options
            | {
                "command": (sys.executable, "-c", script, str(marker)),
                "timeout_seconds": 0.1,
            }
        )
    )
    if overflow:
        with pytest.raises(CircleAgentWalletError, match="output limit"):
            wallet._run(["wallet", "execute"])
    else:
        assert wallet._run(["wallet", "execute"]) == b"done\n"
    assert marker.read_text() == "finished"


def test_open_existing_requires_original_journal_and_lock(context):
    _, _, _, options = context
    with pytest.raises(ValidationError):
        CircleAgentWallet(**(options | {"require_existing": True}))
    assert not options["checkpoint_path"].exists()


def test_hashless_service_failure_keeps_original_submission_unknown(context):
    wallet, _, cli, _ = context
    cli.reply_changes = {"state": "FAILED", "txHash": None}
    result = wallet.execute(CALL, operation_ref="original-ref")
    assert result.state is OperationState.UNKNOWN
    assert result.wallet_operation_id == ID and result.tx_hash is None
    cli.pages = [[cli.transaction(state="FAILED", txHash=None)]]
    assert wallet.execute(CALL, operation_ref="original-ref") == result
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


class Trips:
    """Counts round trips to the RPC: one per call, or one per batch."""

    def __init__(self, rpc):
        self.rpc, self.trips = rpc, 0

    def call(self, method, params):
        self.trips += 1
        return self.rpc.call(method, params)

    def call_many(self, calls):
        self.trips += 1
        return [self.rpc.call(method, params) for method, params in calls]


def test_wallet_check_reads_no_chain_and_selection_once(context, monkeypatch):
    _, rpc, cli, options = context
    trips = Trips(rpc)
    chain = ChainReader(trips, options["chain"].profile)
    wallet = CircleAgentWallet(**{**options, "chain": chain})
    monkeypatch.setattr(wallet, "_run", cli)
    wallet.verify_wallet()
    wallet.verify_wallet()
    # No account code or implementation is pinned; the CLI version and its
    # selection of this wallet cannot change for this object.
    assert trips.trips == 0 and rpc.calls == []
    assert cli.calls == [
        ["--version"],
        ["wallet", "list", "--type", "agent", "--chain", "ARC-TESTNET", "--output", "json"],
    ]


def test_the_cli_and_wallet_are_checked_once_until_a_command_fails(context, monkeypatch):
    # CLI 1.1.4 starts a Node process per command and has no long-lived mode.
    # Once checked, an opening runs only its sign and execute commands.
    wallet, _, cli, _ = context
    auth = FundingAuthorization(CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32)
    wallet.verify_wallet()
    wallet.sign_funding_authorization(auth)
    wallet.execute(CALL, operation_ref="original-ref")
    wallet.verify_wallet()
    assert [args[:2] for args in cli.calls] == [
        ["--version"],
        ["wallet", "list"],
        ["wallet", "sign"],
        ["wallet", "execute"],
    ]
    # A failed command (here an expired login) clears the check, so the next
    # operation checks the CLI and wallet again before anything else.
    cli.calls.clear()

    def expired(arguments):
        cli.calls.append(arguments)
        raise CircleAgentWalletError("Circle command did not return success (AUTH_REQUIRED)")

    monkeypatch.setattr(wallet, "_run", expired)
    assert wallet.reconcile(operation_ref="original-ref").state is OperationState.UNKNOWN
    monkeypatch.setattr(wallet, "_run", cli)
    wallet.sign_funding_authorization(auth)
    assert [args[:2] for args in cli.calls] == [
        ["transaction", "list"],
        ["--version"],
        ["wallet", "list"],
        ["wallet", "sign"],
    ]


def test_version_1_journal_keeps_its_operations_and_drops_account_pins(context):
    wallet, _, cli, options = context
    result = wallet.execute(CALL, operation_ref="original-ref")
    record = json.loads(cli.path.read_text())
    assert (record["version"], record["wallet"]) == (2, hx(WALLET))
    pins = {"address": hx(b"\x12" * 20), "code_hash": hx(b"\x01" * 32)}
    legacy = record | {
        "version": 1,
        "wallet": {"address": hx(WALLET), "code_hash": hx(b"\x02" * 32)},
        "implementation": pins,
        "operations": {"original-ref": whole(CALL, record["operations"]["original-ref"])},
    }
    cli.path.write_text(json.dumps(legacy))
    restored = CircleAgentWallet(**options)
    restored._run = cli
    cli.pages = [[cli.transaction()]]
    assert restored.reconcile(operation_ref="original-ref") == result
    assert json.loads(cli.path.read_text()) == record  # Saved as version 2, compact.
    cli.path.write_text(json.dumps(legacy | {"wallet": {"address": hx(b"\x13" * 20)}}))
    with pytest.raises(CircleAgentWalletError, match="^Circle journal could not be validated$"):
        CircleAgentWallet(**options)


def test_both_adapters_bind_a_call_by_the_same_digest():
    # The Rust adapter's tests check this vector too: both keep one journal.
    call = WalletCall(5042, b"\x07" * 20, bytes.fromhex("deadbeef"))
    assert _digest(call).hex() == "e4bdaec23c3fd8baf664d58d25c7b5f93f9ef58b77c4ce4902c6846b1289abf3"


def test_an_earlier_whole_call_record_keeps_binding_its_call_by_digest(context):
    wallet, _, cli, options = context
    result = wallet.execute(CALL, operation_ref="original-ref")
    current = json.loads(cli.path.read_text())
    earlier = copy.deepcopy(current)
    earlier["operations"]["original-ref"] = whole(CALL, current["operations"]["original-ref"])
    cli.path.write_text(json.dumps(earlier))
    raw = cli.path.read_bytes()
    restored = CircleAgentWallet(**(options | {"require_existing": True}))
    restored._run = cli
    assert cli.path.read_bytes() == raw, "opening rewrote it"
    with pytest.raises(ValidationError, match="reused"):
        restored.execute(replace(CALL, data=b"different"), operation_ref="original-ref")
    cli.pages = [[cli.transaction()]]
    assert restored.reconcile(operation_ref="original-ref") == result
    assert json.loads(cli.path.read_text()) == current, "the next save keeps only the digest"
    with pytest.raises(ValidationError, match="reused"):
        restored.execute(replace(CALL, data=b"different"), operation_ref="original-ref")
    assert restored.execute(CALL, operation_ref="original-ref") == result
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1
    # A record with both its call and a digest is neither.
    digest = current["operations"]["original-ref"]["call_digest"]
    earlier["operations"]["original-ref"]["call_digest"] = digest
    cli.path.write_text(json.dumps(earlier))
    with pytest.raises(CircleAgentWalletError, match="^Circle journal could not be validated$"):
        CircleAgentWallet(**options)


class Many(Cli):
    """Answers each reference's execute with its own transaction id, as Circle does."""

    def __init__(self, path):
        super().__init__(path)
        self.executed = 0
        self.lose = False  # Circle's answer is lost: no transaction id is known.
        self.fail = False  # Circle reports a failure, but no transaction hash.
        self.expired = set()  # Commands, such as "wallet list", refused for the login.

    def __call__(self, args):
        if " ".join(args[:2]) in self.expired:
            self.calls.append(copy.deepcopy(args))
            raise CircleAgentWalletError("Circle command did not return success (AUTH_REQUIRED)")
        if args[:2] != ["wallet", "execute"]:
            return super().__call__(args)
        self.calls.append(copy.deepcopy(args))
        if self.lose:
            raise CircleAgentWalletError("mock timeout")
        self.executed += 1
        transaction = self.transaction(
            id=f"{self.executed:08x}-1111-4111-8111-111111111111",
            idempotencyKey=args[args.index("--idempotency-key") + 1],
            abiParameters=args[3:6],
        )
        if self.fail:
            transaction |= {"state": "FAILED", "txHash": None}
        self.pages = [[transaction]]
        return json.dumps({"data": transaction}).encode()


@pytest.fixture
def many(context, monkeypatch):
    wallet, _, cli, options = context
    answers = Many(cli.path)
    monkeypatch.setattr(wallet, "_run", answers)
    return wallet, answers, options


def test_retired_records_leave_the_journal_so_operations_never_stop_at_a_count(many):
    wallet, cli, _ = many
    # 5,000 operations over 1,000 sessions of five, each session's retired once
    # it ends: past the 4,096 records the adapter once refused beyond.
    for session in range(1000):
        references = [f"settle:{session}:{n}" for n in range(5)]
        for reference in references:
            result = wallet.execute(CALL, operation_ref=reference)
            assert result.state is OperationState.PENDING, reference
        wallet.retire(operation_refs=references, at=time.time())
    assert cli.executed == 5000
    assert json.loads(cli.path.read_text())["operations"] == {}
    assert not wallet.near_capacity


def test_retire_keeps_every_record_whose_call_may_still_reach_circle(many):
    wallet, cli, _ = many
    wallet.execute(CALL, operation_ref="hashed")
    cli.lose = True
    lost = wallet.execute(CALL, operation_ref="lost")
    cli.lose, cli.fail = False, True
    hashless = wallet.execute(CALL, operation_ref="hashless")
    cli.fail = False
    now = time.time()
    wallet.retire(operation_refs=["hashed", "lost", "hashless", "unrecorded"], at=now)
    kept = json.loads(cli.path.read_text())["operations"]
    assert sorted(kept) == ["hashless", "lost"]
    # Each keeps its mark, so it is forgotten once it holds a hash.
    assert [row["retire_at"] for row in kept.values()] == [int(now)] * 2
    # Each still only reconciles its original intent.
    assert wallet.execute(CALL, operation_ref="lost") == lost
    assert wallet.execute(CALL, operation_ref="hashless") == hashless
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 3
    with pytest.raises(ValidationError, match="reused"):
        wallet.execute(replace(CALL, data=b"different"), operation_ref="lost")
    # The retired one answers as any reference with no record: unknown.
    retired = wallet.reconcile(operation_ref="hashed")
    assert (retired.state, retired.wallet_operation_id, retired.tx_hash) == (
        OperationState.UNKNOWN,
        None,
        None,
    )


def test_a_compaction_that_cannot_replace_the_journal_leaves_it_whole(many, monkeypatch):
    wallet, cli, options = many
    for reference in ("first", "second"):
        wallet.execute(CALL, operation_ref=reference)
    before = cli.path.read_bytes()

    def crashed(source, target):
        raise OSError("crashed before the rename")

    # The replacement is written and synced, never renamed over the journal.
    with monkeypatch.context() as crash:
        crash.setattr(local_state.os, "replace", crashed)
        with pytest.raises(ValidationError):
            wallet.retire(operation_refs=["first", "second"], at=time.time())
    assert cli.path.read_bytes() == before
    # A crash after writing a replacement but before its rename leaves only a
    # temporary file beside the journal, which nothing reads.
    (cli.path.parent / ".wallet.json.crashed").write_text('{"operations":')
    reopened = CircleAgentWallet(**(options | {"require_existing": True}))
    reopened._run = cli
    first = reopened.reconcile(operation_ref="first")
    assert first.wallet_operation_id == "00000001-1111-4111-8111-111111111111"
    reopened.retire(operation_refs=["first", "second"], at=time.time())
    assert json.loads(cli.path.read_text())["operations"] == {}
    assert cli.executed == 2


def grow(path, size):
    """Records added to the journal at ``path``, copies of its original-ref
    with their own keys and ids, until it is within one record of ``size``.
    Every one, original-ref included, is marked to retire in 2100, so none
    needs room for a mark."""
    journal = json.loads(path.read_text())
    template = journal["operations"]["original-ref"]
    template["retire_at"] = 4_102_444_800

    def encoded(value):
        return json.dumps(value, separators=(",", ":"))

    # Compact JSON: each further member adds ,"<reference>":<row>.
    total = len(encoded(journal))
    for n in range(1 << 20):
        row = template | {"idempotency_key": str(uuid4()), "wallet_operation_id": str(uuid4())}
        fixed = len(encoded(row)) + 4
        room = size - (total + fixed + 8)
        if room < 0:
            break
        reference = f"{n:08}" + "r" * min(room, 4000)
        journal["operations"][reference] = row
        total += fixed + len(reference)
    raw = encoded(journal)
    assert len(raw) == total
    path.write_text(raw)


def test_near_capacity_from_three_quarters_and_nothing_reaches_circle_past_the_bound(context):
    wallet, _, cli, _ = context
    wallet.execute(CALL, operation_ref="original-ref")
    assert not wallet.near_capacity
    grow(cli.path, _MAX_JOURNAL_BYTES // 4 * 3 + 1000)
    cli.pages = [[cli.transaction()]]
    wallet.reconcile(operation_ref="original-ref")
    assert wallet.near_capacity, "read at three quarters"
    filler = [
        ref for ref in json.loads(cli.path.read_text())["operations"] if ref != "original-ref"
    ]
    wallet.retire(operation_refs=filler, at=time.time())
    assert not wallet.near_capacity, "written below three quarters"
    # At the bound a new call's record does not fit: never sent to Circle.
    grow(cli.path, _MAX_JOURNAL_BYTES)
    raw = cli.path.read_bytes()
    with pytest.raises(NotSubmittedError) as failure:
        wallet.execute(CALL, operation_ref="over" * 250)
    assert str(failure.value.__cause__) == "Circle journal operation capacity reached"
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1
    assert cli.path.read_bytes() == raw
    assert wallet.near_capacity


def test_the_last_record_keeps_room_for_the_id_and_hash_circle_returns(many):
    wallet, cli, _ = many
    wallet.execute(CALL, operation_ref="original-ref")
    grow(cli.path, _MAX_JOURNAL_BYTES - 2000)
    size = len(cli.path.read_bytes())
    # A new member ,"<reference>":<row>, whose row has no id, hash or mark yet.
    row = {
        "route": "self",
        "call_digest": hx(bytes(32)),
        "idempotency_key": str(uuid4()),
        "wallet_operation_id": None,
        "tx_hash": None,
        "state": "unknown",
        "retire_at": None,
    }
    member = len(json.dumps(row, separators=(",", ":"))) + 4

    def reference(free):
        return "r" * (_MAX_JOURNAL_BYTES - free - size - member)

    # Room for the record, not for the id and hash Circle returns and a
    # retirement mark of up to 20 digits (34 + 64 + 16 bytes): unsent.
    with pytest.raises(NotSubmittedError):
        wallet.execute(CALL, operation_ref=reference(113))
    assert cli.executed == 1
    # Room for all three: sent, and its id and hash recorded, 16 bytes short of
    # the bound, which its mark may take.
    last = reference(114)
    result = wallet.execute(CALL, operation_ref=last)
    assert (result.wallet_operation_id, result.tx_hash) == (
        "00000002-1111-4111-8111-111111111111",
        TX,
    )
    assert len(cli.path.read_bytes()) == _MAX_JOURNAL_BYTES - 16
    wallet.retire(operation_refs=[last], at=time.time() + 86_400)
    assert len(cli.path.read_bytes()) <= _MAX_JOURNAL_BYTES


# Circle CLI 1.1.4 as a process of its own, enough for one agent wallet:
# argv[1] is a directory it shares with the test, and argv[2] "hold" makes its
# execute wait there until the test releases it.
FAKE_CLI = r"""
import json, os, sys, time, uuid
state, mode, args = sys.argv[1], sys.argv[2], sys.argv[3:]
wallet, chain = "0x" + "11" * 20, "ARC-TESTNET"
def reply(data):
    print(json.dumps({"data": data}))
    sys.exit(0)
if args == ["--version"]:
    print("1.1.4")
    sys.exit(0)
if args[:2] == ["wallet", "list"]:
    reply({"wallets": [{"type": "agent", "address": wallet, "blockchain": chain}]})
if args[:2] == ["wallet", "execute"]:
    if mode == "hold":
        open(os.path.join(state, "entered"), "w").close()
        deadline = time.monotonic() + 30
        while not os.path.exists(os.path.join(state, "release")):
            if time.monotonic() > deadline:
                sys.exit(3)
            time.sleep(0.01)
    transaction = {
        "id": str(uuid.uuid4()),
        "idempotencyKey": args[args.index("--idempotency-key") + 1],
        "state": "COMPLETE",
        "blockchain": chain,
        "operation": "CONTRACT_EXECUTION",
        "transactionType": "OUTBOUND",
        "sourceAddress": wallet,
        "contractAddress": wallet,
        "destinationAddress": wallet,
        "abiFunctionSignature": args[2],
        "abiParameters": args[3:6],
        "txHash": "0x" + os.urandom(32).hex(),
    }
    with open(os.path.join(state, transaction["id"] + ".json"), "w") as saved:
        json.dump(transaction, saved)
    reply(transaction)
if args[:2] == ["transaction", "list"]:
    names = sorted(name for name in os.listdir(state) if name.endswith(".json"))
    reply({"transactions": [json.load(open(os.path.join(state, name))) for name in names]})
sys.exit(2)
"""
# One adapter in a process of its own: it executes one call and prints the result.
DRIVER = r"""
import json, sys
from pathlib import Path
from mppi_protocol.capabilities import WalletCall
from mppi_protocol.chain import ChainReader
from mppi_protocol.circle import CircleAgentWallet
from mppi_protocol.deployment import profile_from_mapping
journal, profile, command, address, to, data, reference = sys.argv[1:]
class Unused:
    def call(self, method, params):
        raise AssertionError(method)
wallet = CircleAgentWallet(
    chain=ChainReader(Unused(), profile_from_mapping(json.loads(profile))),
    address=bytes.fromhex(address),
    checkpoint_path=Path(journal),
    command=tuple(json.loads(command)),
)
call = WalletCall(wallet._chain_id, bytes.fromhex(to), bytes.fromhex(data))
result = wallet.execute(call, operation_ref=reference)
print(json.dumps([result.state.value, result.wallet_operation_id, result.tx_hash.hex()]))
"""


def test_a_slow_circle_call_holds_no_journal_lock_and_every_row_survives(context, tmp_path):
    """Two processes share one journal: while one waits inside Circle's execute,
    the other saves, records and reconciles its own operation."""
    _, _, _, options = context
    state = tmp_path / "circle"
    state.mkdir()
    path = options["checkpoint_path"]
    command = [sys.executable, "-c", FAKE_CLI, str(state)]
    profile = json.dumps(profile_to_mapping(options["chain"].profile))
    arguments = [str(path), profile, json.dumps([*command, "hold"]), WALLET.hex()]
    slow = subprocess.Popen(
        [sys.executable, "-c", DRIVER, *arguments, PROXY.hex(), CALL.data.hex(), "slow"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 30
        while not (state / "entered").exists():
            assert slow.poll() is None and time.monotonic() < deadline, "never reached Circle"
            time.sleep(0.01)
        other = CircleAgentWallet(**(options | {"command": (*command, "go")}))
        quick = other.execute(CALL, operation_ref="quick")
        assert quick.state is OperationState.PENDING
        assert other.reconcile(operation_ref="quick") == quick
    finally:
        (state / "release").touch()
        output, error = slow.communicate(timeout=60)
    assert slow.returncode == 0, error.decode()
    state_value, identifier, tx = json.loads(output)
    assert state_value == OperationState.PENDING.value
    # Neither lost the other's record, and both hold their handles.
    rows = json.loads(path.read_text())["operations"]
    assert (rows["slow"]["wallet_operation_id"], rows["slow"]["tx_hash"]) == (identifier, "0x" + tx)
    assert (rows["quick"]["wallet_operation_id"], rows["quick"]["tx_hash"]) == (
        quick.wallet_operation_id,
        hx(quick.tx_hash),
    )
    assert identifier != quick.wallet_operation_id


class Clock:
    """The adapter's clocks, which the test moves together."""

    def __init__(self):
        self.elapsed = 0.0
        self.wall = time.time()

    def monotonic(self):
        return 1000.0 + self.elapsed

    def time(self):
        return self.wall + self.elapsed

    def advance(self, seconds):
        self.elapsed += seconds


@pytest.fixture
def clock(monkeypatch):
    moved = Clock()
    monkeypatch.setattr(circle, "time", moved)
    return moved


AUTHORIZATION = FundingAuthorization(
    CHAIN, TOKEN, "USDC", "2", WALLET, PROXY, 1, 10, 20, b"\x33" * 32
)


def verbs(cli):
    """The commands run since the last call, each by its first two words."""
    seen = [" ".join(args[:2]) for args in cli.calls]
    cli.calls.clear()
    return seen


def test_an_execute_checks_the_cli_and_wallet_unless_a_check_began_within_60_seconds(many, clock):
    # CLI 1.1.4 starts a Node process per command and has no long-lived mode.
    wallet, cli, _ = many
    wallet.verify_wallet()
    wallet.sign_funding_authorization(AUTHORIZATION)
    clock.advance(59)
    wallet.execute(CALL, operation_ref="first")
    assert verbs(cli) == ["--version", "wallet list", "wallet sign", "wallet execute"]
    # Once the check is 60 seconds old an execute checks first.
    clock.advance(1)
    wallet.execute(CALL, operation_ref="second")
    wallet.execute(CALL, operation_ref="third")
    assert verbs(cli) == ["--version", "wallet list", "wallet execute", "wallet execute"]
    # Signing and verify_wallet accept the last passing check whatever its age.
    clock.advance(3600)
    wallet.verify_wallet()
    wallet.sign_funding_authorization(AUTHORIZATION)
    assert verbs(cli) == ["wallet sign"]
    # A login lost before the check is due fails the execute's own check,
    # before its journal save: definitely not submitted.
    cli.expired = {"wallet list"}
    with pytest.raises(NotSubmittedError) as failure:
        wallet.execute(CALL, operation_ref="fourth")
    assert str(failure.value.__cause__) == "Circle command did not return success (AUTH_REQUIRED)"
    assert verbs(cli) == ["--version", "wallet list"]
    assert "fourth" not in json.loads(cli.path.read_text())["operations"]
    # A login lost within 60 seconds of a passing check reaches the CLI's own
    # refusal after the save: unknown, with no transaction id to reconcile.
    cli.expired = set()
    wallet.verify_wallet()
    cli.expired = {"wallet execute"}
    lost = wallet.execute(CALL, operation_ref="fifth")
    assert (lost.state, lost.wallet_operation_id, lost.tx_hash) == (
        OperationState.UNKNOWN,
        None,
        None,
    )
    assert lost.cause == (
        "CircleAgentWalletError: Circle command did not return success (AUTH_REQUIRED)"
    )
    assert verbs(cli) == ["--version", "wallet list", "wallet execute"]
    # That failure cleared the record, so the next signature checks first.
    cli.expired = set()
    wallet.sign_funding_authorization(AUTHORIZATION)
    assert verbs(cli) == ["--version", "wallet list", "wallet sign"]


@pytest.mark.parametrize("problem", ["version", "account", "missing"])
def test_a_refused_check_leaves_no_record_for_signing(many, clock, problem):
    wallet, cli, _ = many
    wallet.verify_wallet()
    clock.advance(60)
    # The execute's check is refused: another CLI version, or a listing
    # without this agent wallet. The earlier pass no longer counts.
    listed = copy.deepcopy(cli.wallets)
    if problem == "version":
        cli.version = b"1.1.5\n"
    elif problem == "account":
        cli.wallets[0]["type"] = "local"
    else:
        cli.wallets = []
    with pytest.raises(NotSubmittedError):
        wallet.execute(CALL, operation_ref="refused")
    cli.version, cli.wallets = b"1.1.4\n", listed
    verbs(cli)
    wallet.verify_wallet()
    wallet.sign_funding_authorization(AUTHORIZATION)
    assert verbs(cli) == ["--version", "wallet list", "wallet sign"]


def test_a_call_refused_under_an_unbound_reference_is_a_definite_failure(many):
    wallet, cli, options = many
    descriptor = SessionDescriptor(
        b"\x21" * 20, b"\x22" * 20, 7, b"\x23" * 20, WALLET, ARC_USDC, b"\x24" * 32
    )
    # A settlement of 137 sessions in one batch is over the CLI's call bound.
    batch = ContractCalls(DOMAIN, "USDC", "2").settle_batch(
        [(descriptor, 100, 50, b"\x1b" * 65)] * 137
    )
    assert len(batch.call.data) == 61_636 > circle._MAX_CALL_BYTES
    result = submit_operation(wallet, batch, operation_ref="settle:batch", chain=options["chain"])
    assert (result.state, result.wallet_operation_id, result.tx_hash) == (
        OperationState.FAILED,
        None,
        None,
    )
    assert result.cause == "NotSubmittedError: ValidationError"
    # Refused before any record of it was saved: never sent.
    for call, reference in [(replace(CALL, chain_id=5042), "settle:other"), (CALL, "r" * 4097)]:
        with pytest.raises(NotSubmittedError):
            wallet.execute(call, operation_ref=reference)
    assert cli.calls == [], "Circle was asked nothing"
    assert not cli.path.exists()
    # A reference that binds another call is still a refused reuse: that call
    # may have reached Circle.
    wallet.execute(CALL, operation_ref="bound")
    with pytest.raises(ValidationError, match="reused"):
        wallet.execute(batch.call, operation_ref="bound")
    assert cli.executed == 1


def test_a_save_that_landed_but_reported_failure_leaves_no_binding(many, monkeypatch):
    wallet, cli, _ = many
    sync, failing = os.fsync, []

    def fsync(fd):
        # Checkpoint.save syncs the directory only after os.replace.
        if failing and stat.S_ISDIR(os.fstat(fd).st_mode):
            failing.pop()
            raise OSError("directory sync failed")
        return sync(fd)

    monkeypatch.setattr(local_state.os, "fsync", fsync)
    # The intent is renamed into place, then syncing the directory fails.
    failing.append(True)
    with pytest.raises(NotSubmittedError):
        wallet.execute(CALL, operation_ref="close:x")
    assert json.loads(cli.path.read_text())["operations"] == {}, "the landed intent is gone"
    # So the reference may carry a newly prepared call, as a close does.
    prepared = replace(CALL, data=b"\xde\xad")
    assert wallet.execute(prepared, operation_ref="close:x").state is OperationState.PENDING
    assert cli.executed == 1
    # A journal that cannot be shown free of the intent proves nothing: the
    # removal is reported failed too, so the outcome is unknown.
    failing.extend([True, True])
    with pytest.raises(CircleAgentWalletError) as unknown:
        wallet.execute(CALL, operation_ref="close:y")
    assert not isinstance(unknown.value, NotSubmittedError)
    assert cli.executed == 1


def test_a_retirement_mark_survives_a_restart_and_the_first_save_after_it_forgets(many, clock):
    wallet, cli, options = many
    wallet.execute(CALL, operation_ref="retired")
    cli.lose = True
    wallet.execute(CALL, operation_ref="hashless")
    cli.lose = False
    due = clock.time() + 3600
    references = ["retired", "hashless", "unrecorded"]
    wallet.retire(operation_refs=references, at=due)
    # A later time keeps the earlier mark.
    wallet.retire(operation_refs=references, at=due + 60)
    rows = json.loads(cli.path.read_text())["operations"]
    assert [rows[reference]["retire_at"] for reference in ("retired", "hashless")] == [int(due)] * 2
    del wallet
    restarted = CircleAgentWallet(**(options | {"require_existing": True}))
    restarted._run = cli
    restarted.execute(CALL, operation_ref="before")
    assert "retired" in json.loads(cli.path.read_text())["operations"], "not yet due"
    clock.advance(3600)
    restarted.execute(CALL, operation_ref="after")
    rows = json.loads(cli.path.read_text())["operations"]
    # Due and holding a hash: forgotten. Without one it keeps its mark.
    assert sorted(rows) == ["after", "before", "hashless"]
    assert rows["hashless"]["retire_at"] == int(due)
    # A time that is not a Unix time in seconds is refused.
    for at in (-1, float("nan"), True, 2.0**64):
        with pytest.raises(ValidationError):
            restarted.retire(operation_refs=references, at=at)


def test_retire_skips_a_reference_no_record_can_have(many):
    wallet, cli, _ = many
    wallet.execute(CALL, operation_ref="done")
    wallet.retire(operation_refs=["", "r" * 4097, 7, "done"], at=time.time())
    assert json.loads(cli.path.read_text())["operations"] == {}


def test_a_reconcile_withdraws_a_retirement_mark(many, clock):
    wallet, cli, options = many
    executed = wallet.execute(CALL, operation_ref="superseded")
    due = clock.time() + 3600
    wallet.retire(operation_refs=["superseded"], at=due)

    def rows():
        return json.loads(cli.path.read_text())["operations"]

    assert rows()["superseded"]["retire_at"] == int(due)
    # A recovery from an older checkpoint looks the reference up again, so it
    # may be presented again: the lookup withdraws its mark.
    reopened = CircleAgentWallet(**(options | {"require_existing": True}))
    reopened._run = cli
    assert reopened.reconcile(operation_ref="superseded") == executed
    assert rows()["superseded"]["retire_at"] is None
    # Past the time it was marked for, a save keeps it.
    clock.advance(3600)
    reopened.execute(CALL, operation_ref="later")
    assert "superseded" in rows()
    # Marked anew, the first save from that time forgets it.
    reopened.retire(operation_refs=["superseded"], at=clock.time())
    assert "superseded" not in rows()
    assert cli.executed == 2


class FailingSaves:
    """Checkpoint.save refusing its next ``count`` calls, as a full disk does."""

    def __init__(self, monkeypatch, count):
        self.count, save = count, Checkpoint.save

        def failing(checkpoint, record):
            if self.count:
                self.count -= 1
                raise OSError("storage failure")
            return save(checkpoint, record)

        monkeypatch.setattr(Checkpoint, "save", failing)


def handles(path, reference="original-ref"):
    row = json.loads(path.read_text())["operations"][reference]
    return row["wallet_operation_id"], row["tx_hash"]


def kept_key(options):
    path = options["checkpoint_path"]
    return json.loads(path.read_text())["operations"]["original-ref"]["idempotency_key"]


def test_a_kept_answer_whose_save_fails_leaves_a_new_execute_definite(context, monkeypatch):
    _, _, cli, options = context
    wallet, _ = kept_answer(options, cli)
    # The disk refuses the next save, the one that would carry the kept answer.
    FailingSaves(monkeypatch, 1)
    executes = sum(args[:2] == ["wallet", "execute"] for args in cli.calls)
    # The journal held no record of the new reference and none was saved:
    # never sent, so a definite failure, as without a kept answer.
    with pytest.raises(NotSubmittedError):
        wallet.execute(replace(CALL, data=b"\x89\xab"), operation_ref="second-ref")
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == executes
    path = options["checkpoint_path"]
    assert "second-ref" not in json.loads(path.read_text())["operations"]
    # The next save that landed wrote the kept answer.
    assert handles(path) == (ID, hx(TX))


def test_a_failed_save_of_a_kept_answer_fails_no_step_and_the_next_save_writes_it(
    context, monkeypatch
):
    _, _, cli, options = context
    wallet, _ = kept_answer(options, cli)
    # The first save fails: the one a reconcile's first step makes for the
    # kept answer alone, since that step saves nothing of its own. The
    # reconcile goes on, with the kept answer's id, to Circle's history.
    FailingSaves(monkeypatch, 1)
    cli.pages = [[cli.transaction(idempotencyKey=kept_key(options))]]
    found = wallet.reconcile(operation_ref="original-ref")
    assert (found.state, found.wallet_operation_id, found.tx_hash) == (
        OperationState.PENDING,
        ID,
        TX,
    )
    # Its record step's save, the next that landed, wrote it.
    assert handles(options["checkpoint_path"]) == (ID, hx(TX))
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


def test_a_step_whose_own_save_fails_with_a_kept_answer_reports_its_own_failure(
    context, monkeypatch
):
    _, _, cli, options = context
    path = options["checkpoint_path"]
    wallet, _ = kept_answer(options, cli)
    # Every save fails. Before an intent is saved, an execute is definitely
    # not submitted.
    failing = FailingSaves(monkeypatch, 1000)
    with pytest.raises(NotSubmittedError):
        wallet.execute(replace(CALL, data=b"\x89\xab"), operation_ref="second-ref")
    assert "second-ref" not in json.loads(path.read_text())["operations"]
    # After Circle answered, a record step that cannot save is UNKNOWN with
    # the transaction id and hash: here a reconcile's, whose first step's
    # save of the kept answer fails too.
    cli.pages = [[cli.transaction(idempotencyKey=kept_key(options))]]
    found = wallet.reconcile(operation_ref="original-ref")
    assert (found.state, found.wallet_operation_id, found.tx_hash) == (
        OperationState.UNKNOWN,
        ID,
        TX,
    )
    assert found.cause == "OSError"
    # Still kept, and written once a save lands.
    assert handles(path) == (None, None)
    failing.count = 0
    wallet.retire(operation_refs=(), at=time.time())
    assert handles(path) == (ID, hx(TX))
    assert sum(args[:2] == ["wallet", "execute"] for args in cli.calls) == 1


def test_a_record_step_that_cannot_read_the_journal_keeps_circles_answer(context):
    wallet, _, cli, options = context
    path, readable = options["checkpoint_path"], []

    def unreadable(row):
        # While Circle executes, the journal gains a record no adapter accepts:
        # its idempotency key is not a version 4 UUID.
        readable.append(path.read_bytes())
        journal = json.loads(readable[0])
        invalid = row | {"idempotency_key": "11111111-1111-1111-8111-111111111111"}
        journal["operations"]["other-ref"] = invalid
        path.write_text(json.dumps(journal))

    cli.before_submit = unreadable
    result = wallet.execute(CALL, operation_ref="original-ref")
    # UNKNOWN, with Circle's transaction id and hash for the runtime to check.
    assert (result.state, result.wallet_operation_id, result.tx_hash) == (
        OperationState.UNKNOWN,
        ID,
        TX,
    )
    assert result.cause == "CircleAgentWalletError: Circle journal could not be validated"
    # While the journal stays unreadable every step fails, and the answer
    # stays kept.
    with pytest.raises(CircleAgentWalletError):
        wallet.retire(operation_refs=(), at=time.time())
    # Readable again, the next journal step writes it.
    path.write_bytes(readable[0])
    wallet.retire(operation_refs=(), at=time.time())
    assert handles(path) == (ID, hx(TX))


def test_a_merge_the_journal_refuses_still_returns_circles_answer(context):
    wallet, _, cli, options = context
    path = options["checkpoint_path"]

    def contradicted(row):
        # While Circle executes, another record in the journal takes the
        # transaction id Circle is about to report for this reference.
        journal = json.loads(path.read_bytes())
        other = row | {"idempotency_key": str(uuid4()), "wallet_operation_id": ID}
        journal["operations"]["other-ref"] = other
        path.write_text(json.dumps(journal))

    cli.before_submit = contradicted
    result = wallet.execute(CALL, operation_ref="original-ref")
    # UNKNOWN, with Circle's transaction id and hash for the runtime to check:
    # the journal contradicting them never discards them.
    assert (result.state, result.wallet_operation_id, result.tx_hash) == (
        OperationState.UNKNOWN,
        ID,
        TX,
    )
    assert result.cause == (
        "CircleAgentWalletError: Circle transaction already belongs to another intent"
    )
    # The next journal step still refuses them: they are never written over
    # the journal's own evidence.
    wallet.retire(operation_refs=(), at=time.time())
    assert handles(path) == (None, None)
    assert handles(path, "other-ref") == (ID, None)
