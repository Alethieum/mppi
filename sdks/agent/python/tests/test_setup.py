"""Public setup composition with wallet/chain boundaries replaced by local fixtures."""

import asyncio
import json
from contextlib import contextmanager
from dataclasses import replace
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import mppi_protocol.circle as shared_circle
import pytest
from eth_abi import encode
from eth_account.messages import encode_typed_data
from eth_utils import function_signature_to_4byte_selector
from mppi_agent import Client, cli, setup
from mppi_agent import client as client_module
from mppi_agent.checkpoint import Checkpoint
from mppi_agent.credentials import CredentialError, load_session_signer
from mppi_agent.onboarding import OnboardingError
from mppi_protocol.chain import Block, ChainEvidenceError
from mppi_protocol.contract import ARC_USDC
from mppi_protocol.deployment import profile_from_mapping, profile_to_mapping
from mppi_protocol.errors import ValidationError
from mppi_protocol.follower import ChainFollower
from mppi_protocol.types import (
    ChainObservation,
    OperationResult,
    OperationState,
    PauseLevel,
    SessionDescriptor,
)

ADDRESS = b"\x11" * 20
WALLET = "0x" + ADDRESS.hex()
TX = b"\x22" * 32
PROVIDER = "https://provider.example"
ERRORS = (OnboardingError, CredentialError, ValidationError)


class Boundaries:
    """Only registry registration can cross this fixture's write boundary."""

    def __init__(self, monkeypatch, tmp_path):
        tmp_path.chmod(0o700)
        self.config = tmp_path / "agent.json"
        self.identity = SimpleNamespace(agent_id=77, owner=ADDRESS, agent_wallet=ADDRESS)
        self.registration_state = OperationState.CONFIRMED
        self.balance = 1_000_000
        self.submissions = []
        self.reconciliations = []
        self.rpc_urls = []
        self.reads = []
        self.wallets = []
        self.wallet_pins = []
        self.usdc_name, self.usdc_version, self.usdc_decimals = "USDC", "2", 6
        self.separator = None
        owner = self

        class Reader:
            def __init__(self, rpc, profile):
                self.rpc, self.profile = rpc, profile

            def block(self):
                return ChainObservation(
                    self.profile.domain.chain_id,
                    self.profile.domain.verifying_contract,
                    10,
                    b"b" * 32,
                )

            def verify_deployment(self, block):
                assert block == self.block()

            def _check_canonical(self, block):
                assert block == self.block()

            def _call(self, to, data, block):
                assert to == ARC_USDC and block == self.block()
                # eth-account 0.14.0 messages.encode_typed_data returns its domain
                # separator in header; this is independent of setup's fixed hashing.
                separator = encode_typed_data(
                    domain_data={
                        "name": "USDC",
                        "version": "2",
                        "chainId": self.profile.domain.chain_id,
                        "verifyingContract": "0x" + ARC_USDC.hex(),
                    },
                    message_types={"Check": [{"name": "value", "type": "uint256"}]},
                    message_data={"value": 0},
                ).header
                # eth-utils 6.0.0 abi.py:function_signature_to_4byte_selector
                # handles ordinary token ABI signatures independently of MPPI's ABI.
                return {
                    function_signature_to_4byte_selector("name()"): encode(
                        ["string"], [owner.usdc_name]
                    ),
                    function_signature_to_4byte_selector("version()"): encode(
                        ["string"], [owner.usdc_version]
                    ),
                    function_signature_to_4byte_selector("decimals()"): encode(
                        ["uint8"], [owner.usdc_decimals]
                    ),
                    function_signature_to_4byte_selector("DOMAIN_SEPARATOR()"): (
                        separator if owner.separator is None else owner.separator
                    ),
                }[data]

        class Registry:
            def __init__(self, reader):
                self.chain = reader

            def verified_block(self):
                return self.chain.block()

            def balance(self, address, *, block):
                assert address == ADDRESS and block == self.chain.block()
                return owner.balance

            def read(self, agent_id, *, block=None):
                assert agent_id == owner.identity.agent_id
                owner.reads.append(agent_id)
                return owner.identity

            def simulate(self, operation):
                assert operation.call.to == self.chain.profile.registry_address

            def reconcile(self, operation, *, operation_ref, tx_hash, wallet_operation_id=None):
                assert operation.call.to == self.chain.profile.registry_address and tx_hash == TX
                return SimpleNamespace(
                    operation=OperationResult(
                        operation_ref,
                        owner.registration_state,
                        wallet_operation_id,
                        tx_hash,
                        self.chain.block(),
                    ),
                    identity=(
                        owner.identity
                        if owner.registration_state is OperationState.CONFIRMED
                        else None
                    ),
                )

        class Wallet:
            def __init__(self, *, chain, wallet, implementation, checkpoint_path, **kwargs):
                owner.wallets.append(wallet.address)
                owner.wallet_pins.append((wallet.code_hash, implementation))
                self.chain, self.address, self.path = chain, wallet.address, checkpoint_path
                self.journal = None

            @contextmanager
            def _journal(self):
                self.journal = Checkpoint(self.path)
                try:
                    yield
                finally:
                    self.journal.close()
                    self.journal = None

            def _save(self):
                self.journal.save({"version": 1, "operations": []})

            def verify_registration_wallet(self):
                pass

            def verify_wallet(self):
                pass

            def sign_funding_authorization(self, authorization):
                pytest.fail("setup/composition must never authorize escrow funding")

            def execute(self, call, *, operation_ref):
                assert call.to == self.chain.profile.registry_address
                identity = json.loads(owner.path(".identity.json").read_text())
                assert identity["registration"]["operation_ref"] == operation_ref
                assert identity["registration"]["calldata"] == "0x" + call.data.hex()
                owner.submissions.append(operation_ref)
                return OperationResult(
                    operation_ref, OperationState.PENDING, "wallet-operation", TX
                )

            def reconcile(self, *, operation_ref):
                assert operation_ref in owner.submissions
                owner.reconciliations.append(operation_ref)
                return OperationResult(
                    operation_ref, OperationState.PENDING, "wallet-operation", TX
                )

        def rpc(endpoint):
            self.rpc_urls.append(endpoint)
            return object()

        monkeypatch.setattr(setup, "HttpJsonRpc", rpc)
        monkeypatch.setattr(setup, "ChainReader", Reader)
        monkeypatch.setattr(setup, "RegistryReader", Registry)
        monkeypatch.setattr(setup, "CircleAgentWallet", Wallet)
        monkeypatch.setenv(setup.DEFAULT_RPC_ENV, "https://rpc.example/secret-rpc-token")

    def path(self, suffix):
        return self.config.with_name(self.config.name + suffix)

    def onboard(self, **kwargs):
        return setup.onboard_circle(WALLET, config_path=self.config, **kwargs)


@pytest.fixture
def boundary(monkeypatch, tmp_path):
    return Boundaries(monkeypatch, tmp_path)


@pytest.mark.parametrize(
    "field,value",
    [
        ("rpc_env", "BAD-NAME"),
        ("rpc_env", ""),
        ("rpc_env", 1),
        ("agent_id", True),
        ("agent_id", -1),
        ("agent_id", 2**256),
        ("agent_id", "0"),
        ("agent_id", 1.5),
    ],
)
def test_invalid_setup_arguments_fail_before_creating_any_state_or_wallet(boundary, field, value):
    with pytest.raises(ERRORS):
        boundary.onboard(**{field: value})
    assert not list(boundary.config.parent.iterdir())
    assert not boundary.wallets and not boundary.submissions


def test_fresh_rejected_wallet_leaves_no_setup_or_signer(boundary, monkeypatch):
    def reject(self):
        raise OnboardingError("unsupported supplied wallet")

    monkeypatch.setattr(setup.CircleAgentWallet, "verify_registration_wallet", reject)
    with pytest.raises(OnboardingError, match="unsupported"):
        boundary.onboard()
    assert not boundary.config.exists() and not boundary.path(".signer").exists()
    assert not boundary.submissions


def test_fresh_setup_constructs_reusable_public_client_without_private_integration(
    boundary, monkeypatch
):
    result = boundary.onboard()
    assert result.onboarding.complete and len(boundary.submissions) == 1
    signer = load_session_signer(boundary.path(".signer"))
    monkeypatch.setattr(
        setup, "onboard", lambda **kwargs: pytest.fail("client must only read identity")
    )
    monkeypatch.setattr(
        setup, "create_session_signer", lambda path: pytest.fail("client must load key")
    )
    for _ in range(2):
        with Client.from_config(
            PROVIDER, config_path=boundary.config, initial_session_tokens=5000
        ) as client:
            assert client.wallet.address == ADDRESS
            assert client.signer.address == signer.address
            assert client.agent_id == 77 and client.initial_session_tokens == 5000
            assert callable(client.chat.completions.create)
            assert client.chain.reader.profile.domain == client.contract.domain
    assert len(boundary.submissions) == 1 and not boundary.reconciliations
    setup_text = boundary.config.read_text()
    assert "secret-rpc-token" not in setup_text
    assert boundary.path(".signer").read_bytes().hex() not in setup_text
    assert json.loads(setup_text)["rpc_env"] == setup.DEFAULT_RPC_ENV


@pytest.mark.parametrize("read_seconds", [12, 25])
def test_builtin_follower_allows_read_latency_but_same_head_still_expires(
    boundary, monkeypatch, read_seconds
):
    boundary.onboard()
    epoch = 1_800_000_000
    clock = SimpleNamespace(wall=epoch, mono=0)

    class DelayedReader:
        def __init__(self, reader):
            self.profile = reader.profile
            self.head = Block(reader.block(), epoch, b"a" * 32)

        def block(self, number=None):
            assert number in (None, self.head.observation.block_number)
            return self.head

        def verify_deployment(self, block):
            assert block == self.head
            # Canonical verification takes time after choosing the pinned head.
            clock.wall += read_seconds
            clock.mono += read_seconds

        def pause_level(self, block):
            assert block == self.head
            return PauseLevel.NONE

    def follower(reader, **options):
        return ChainFollower(
            DelayedReader(reader),
            **options,
            monotonic=lambda: clock.mono,
            wall_clock=lambda: clock.wall,
        )

    monkeypatch.setattr(setup, "ChainFollower", follower)
    with Client.from_config(PROVIDER, config_path=boundary.config) as client:
        assert client.chain.poll_interval_ms == 2_000
        assert client.chain.collection_margin_ms == 30_000

        async def scenario():
            original = ChainFollower(
                client.chain.reader,
                max_age_ms=10_000,
                collection_margin_ms=30_000,
                monotonic=lambda: clock.mono,
                wall_clock=lambda: clock.wall,
            )
            try:
                with pytest.raises(ChainEvidenceError, match="unavailable or stale"):
                    await original.start()
                assert not original.readiness().ready
            finally:
                await original.stop()

            clock.wall, clock.mono = epoch, 0
            await client.chain.start()
            assert client.chain.readiness().ready
            assert clock.wall - client.chain.snapshot().block.timestamp == read_seconds

            # A just-completed observation cannot renew an unchanged old head.
            clock.wall, clock.mono = epoch + 60 - read_seconds, 60 - read_seconds
            with pytest.raises(ChainEvidenceError, match="unavailable or stale"):
                await client.chain.refresh()
            assert clock.wall == epoch + 60
            assert client.chain.current.observed_at == clock.mono
            assert client.chain.readiness().failures[0].reason == "CHAIN_STATE_STALE"

        client._sync(scenario())
    assert len(boundary.submissions) == 1 and not boundary.reconciliations


@pytest.mark.parametrize(
    "original_budget,seconds,expected",
    [
        (True, 14.3, OperationState.UNKNOWN),
        (False, 254.3, OperationState.PENDING),
        (False, 301, OperationState.UNKNOWN),
    ],
)
def test_builtin_wallet_operation_budget_allows_cli_wait_but_remains_bounded(
    boundary, monkeypatch, original_budget, seconds, expected
):
    boundary.onboard()
    with Client.from_config(PROVIDER, config_path=boundary.config) as client:
        if original_budget:
            client.timeout = 10  # Reproduce the previous composition's operation budget.
        descriptor = SessionDescriptor(
            ADDRESS, client.signer.address, 7, b"r" * 20, b"o" * 20, ARC_USDC, b"s" * 32
        )
        operation = client.contract.release_idle(descriptor)
        reference = "same-operation-through-timeout"
        calls = []
        real_to_thread = asyncio.to_thread

        async def scenario():
            loop = asyncio.get_running_loop()
            clock = SimpleNamespace(now=loop.time())

            async def delayed_wallet(function, *args, **kwargs):
                if function is not client_module.submit_operation:
                    return await real_to_thread(function, *args, **kwargs)
                calls.append((args, kwargs))
                result = loop.create_future()

                def complete():
                    if not result.done():
                        result.set_result(
                            OperationResult(reference, OperationState.PENDING, "original-wallet-id")
                        )

                # Python 3.12 asyncio loop.call_later/time and wait_for:
                # https://docs.python.org/3.12/library/asyncio-eventloop.html#delayed-callbacks
                # https://docs.python.org/3.12/library/asyncio-task.html#asyncio.wait_for
                # Advance their shared clock without sleeping or mocking wait_for.
                handle = loop.call_later(seconds, complete)
                loop.call_soon(setattr, clock, "now", clock.now + seconds)
                try:
                    return await result
                finally:
                    handle.cancel()

            with monkeypatch.context() as patch:
                patch.setattr(loop, "time", lambda: clock.now)
                patch.setattr(asyncio, "to_thread", delayed_wallet)
                outcome = await client._submit(operation, reference)
            assert outcome.state is expected and outcome.operation_ref == reference
            assert client._operations[reference] == (operation, outcome)
            assert len(calls) == 1
            assert calls[0][0] == (client.wallet, operation)
            assert calls[0][1]["operation_ref"] == reference
            assert calls[0][1]["chain"] is client.chain.reader
            assert outcome.wallet_operation_id == (
                "original-wallet-id" if expected is OperationState.PENDING else None
            )

        client._sync(scenario())
    assert len(boundary.submissions) == 1 and not boundary.reconciliations


def test_completed_rerun_keeps_signer_and_zero_adopted_identity(boundary, monkeypatch):
    boundary.identity.agent_id = 0
    boundary.onboard(agent_id=0)
    before = boundary.path(".signer").read_bytes()
    monkeypatch.setattr(
        setup, "create_session_signer", lambda path: pytest.fail("must load original")
    )
    result = boundary.onboard()
    assert result.onboarding.identity.agent_id == 0
    assert not boundary.submissions
    assert boundary.path(".signer").read_bytes() == before
    with Client.from_config(PROVIDER, config_path=boundary.config) as client:
        assert client.agent_id == 0 and client.initial_session_tokens is None
    assert not boundary.submissions


@pytest.mark.parametrize("state", [OperationState.PENDING, OperationState.UNKNOWN])
def test_pending_registration_reuses_original_checkpoint_and_submission(boundary, state):
    boundary.registration_state = state
    first = boundary.onboard()
    assert not first.onboarding.complete
    reference = first.onboarding.operation.operation_ref
    key = boundary.path(".signer").read_bytes()
    with pytest.raises(OnboardingError, match="complete"):
        Client.from_config(PROVIDER, config_path=boundary.config)
    boundary.registration_state = OperationState.CONFIRMED
    result = boundary.onboard()
    assert result.onboarding.complete
    assert result.onboarding.operation.operation_ref == reference
    assert boundary.submissions == [reference] and boundary.reconciliations == [reference]
    assert boundary.path(".signer").read_bytes() == key


@pytest.mark.parametrize("change", ["wallet", "rpc_env", "agent_id"])
def test_conflicting_onboarding_selection_rejects_before_new_wallet_write(boundary, change):
    boundary.onboard()
    before = boundary.config.read_bytes()
    values = {"wallet_address": WALLET, "config_path": boundary.config}
    values[change if change != "wallet" else "wallet_address"] = {
        "wallet": "0x" + (b"x" * 20).hex(),
        "rpc_env": "OTHER_RPC",
        "agent_id": 78,
    }[change]
    with pytest.raises(OnboardingError):
        setup.onboard_circle(**values)
    assert len(boundary.submissions) == 1
    assert boundary.config.read_bytes() == before


def test_replaced_signer_cannot_change_retained_payment_authority(boundary):
    boundary.onboard()
    before = boundary.config.read_bytes()
    boundary.path(".signer").write_bytes((2).to_bytes(32, "big"))
    for operation in (
        boundary.onboard,
        lambda: Client.from_config(PROVIDER, config_path=boundary.config),
    ):
        with pytest.raises(CredentialError):
            operation()
    assert boundary.config.read_bytes() == before
    assert len(boundary.submissions) == 1


@pytest.mark.parametrize("suffix", [".signer", ".identity.json", ".wallet.json", ".payments.json"])
def test_lost_retained_state_blocks_client_without_regeneration_or_wallet_write(boundary, suffix):
    boundary.onboard()
    with Client.from_config(PROVIDER, config_path=boundary.config):
        pass
    boundary.path(suffix).unlink()
    with pytest.raises(ERRORS):
        Client.from_config(PROVIDER, config_path=boundary.config)
    if suffix != ".payments.json":
        with pytest.raises(ERRORS):
            boundary.onboard()
    assert not boundary.path(suffix).exists()
    assert len(boundary.submissions) == 1


@pytest.mark.parametrize(
    "change", ["missing", "duplicate", "extra", "profile", "identity", "signer"]
)
def test_missing_or_malformed_setup_never_reconstructs_from_partial_state(boundary, change):
    boundary.onboard()
    signer = boundary.path(".signer").read_bytes()
    if change == "missing":
        boundary.config.unlink()
    elif change == "duplicate":
        boundary.config.write_text('{"version":1,"version":1}')
    else:
        record = json.loads(boundary.config.read_text())
        if change == "extra":
            record["unexpected"] = "value"
        elif change == "profile":
            record["profile"]["chain_id"] = 5042
        elif change == "signer":
            record["signer_address"] = None
        else:
            record["agent_id"] = "00"
        boundary.config.write_text(json.dumps(record))
    for operation in (
        boundary.onboard,
        lambda: Client.from_config(PROVIDER, config_path=boundary.config),
    ):
        with pytest.raises(ERRORS):
            operation()
    assert boundary.path(".signer").read_bytes() == signer
    assert len(boundary.submissions) == 1
    if change == "missing":
        assert not boundary.config.exists()


@pytest.mark.parametrize("field", ["owner", "agent_wallet"])
def test_client_rechecks_current_registered_authority(boundary, field):
    boundary.onboard()
    setattr(boundary.identity, field, b"x" * 20)
    with pytest.raises(OnboardingError, match="authority"):
        Client.from_config(PROVIDER, config_path=boundary.config)
    assert len(boundary.submissions) == 1


@pytest.mark.parametrize("change", ["name", "version", "decimals", "separator"])
def test_wrong_usdc_domain_or_precision_stops_before_registration(boundary, change):
    field, value = {
        "name": ("usdc_name", "Other Token"),
        "version": ("usdc_version", "3"),
        "decimals": ("usdc_decimals", 18),
        "separator": ("separator", b"x" * 32),
    }[change]
    setattr(boundary, field, value)
    with pytest.raises(OnboardingError, match="USDC"):
        boundary.onboard()
    assert not boundary.submissions
    assert not boundary.config.exists() and not boundary.path(".signer").exists()


def test_empty_wallet_cannot_complete_public_setup(boundary):
    boundary.balance = 0
    with pytest.raises(OnboardingError, match="fund"):
        boundary.onboard()
    assert not boundary.submissions
    with pytest.raises(OnboardingError, match="complete"):
        Client.from_config(PROVIDER, config_path=boundary.config)


def test_same_setup_excludes_concurrent_clients_and_releases_lock(boundary):
    boundary.onboard()
    with Client.from_config(PROVIDER, config_path=boundary.config):
        with pytest.raises(ValidationError, match="already in use"):
            Client.from_config(PROVIDER, config_path=boundary.config)
    with Client.from_config(PROVIDER, config_path=boundary.config):
        pass
    assert len(boundary.submissions) == 1


@pytest.mark.parametrize("testnet,chain_id", [(True, 5042002), (False, 5042)])
def test_bundled_profile_exactly_matches_public_deployment(testnet, chain_id):
    name = "arc-testnet.json" if testnet else "arc-mainnet.json"
    published = Path(__file__).resolve().parents[4] / "deployments" / name
    assert files("mppi_agent").joinpath(name).read_bytes() == published.read_bytes()
    assert setup._profile(testnet=testnet) == json.loads(published.read_bytes())
    assert setup._profile(testnet=testnet)["chain_id"] == chain_id


@pytest.mark.parametrize("complete,exit_code", [(True, 0), (False, 2)])
@pytest.mark.parametrize("explicit_config", [False, True])
@pytest.mark.parametrize("testnet", [True, False])
def test_builtin_cli_needs_no_wallet_factory_and_preserves_status(
    monkeypatch, tmp_path, capsys, complete, exit_code, explicit_config, testnet
):
    calls = []

    def onboard(wallet, **kwargs):
        calls.append((wallet, kwargs))
        return setup.SetupResult(
            kwargs["config_path"],
            ADDRESS,
            SimpleNamespace(
                complete=complete,
                identity=SimpleNamespace(agent_id=0) if complete else None,
                balance=1_000_000,
                submitted=not complete,
                operation=None if complete else OperationResult("original", OperationState.UNKNOWN),
            ),
        )

    monkeypatch.setattr(cli, "onboard_circle", onboard)
    monkeypatch.setattr(
        cli.importlib, "import_module", lambda name: pytest.fail("no factory needed")
    )
    network = "--testnet" if testnet else "--mainnet"
    default_rpc_env = setup.DEFAULT_RPC_ENV if testnet else setup.MAINNET_RPC_ENV
    args = ["onboard", network, "--wallet", WALLET, "--agent-id", "0"]
    selected = tmp_path / "agent.json" if explicit_config else setup.DEFAULT_CONFIG
    if explicit_config:
        args += ["--config", str(selected), "--rpc-env", "CUSTOM_RPC"]
    assert cli.main(args) == exit_code
    assert calls == [
        (
            WALLET,
            {
                "config_path": selected,
                "rpc_env": "CUSTOM_RPC" if explicit_config else default_rpc_env,
                "agent_id": 0,
                "testnet": testnet,
                "profile": None,
            },
        )
    ]
    output = capsys.readouterr()
    assert output.err == ""
    result = json.loads(output.out)
    assert result["complete"] is complete and result["config"] == str(selected)
    assert result["agent_id"] == ("0" if complete else None)


def mainnet_selection(monkeypatch):
    monkeypatch.setenv(setup.MAINNET_RPC_ENV, "https://mainnet.example/selected")
    return {"testnet": False}


@pytest.mark.parametrize("testnet,profile", [(False, "testnet"), (True, "mainnet"), (1, None)])
def test_network_profile_errors_precede_state_and_wallet(boundary, testnet, profile):
    if profile is not None:
        profile = profile_from_mapping(setup._profile(testnet=profile == "testnet"))
    with pytest.raises(OnboardingError):
        boundary.onboard(testnet=testnet, profile=profile)
    assert not list(boundary.config.parent.iterdir())
    assert not boundary.wallets and not boundary.rpc_urls and not boundary.submissions


def test_mainnet_never_infers_account_pins_from_testnet(boundary, monkeypatch):
    monkeypatch.delitem(shared_circle._ACCOUNT_PINS, 5042, raising=False)
    profile = profile_from_mapping(setup._profile())
    profile = replace(profile, domain=replace(profile.domain, chain_id=5042))
    with pytest.raises(OnboardingError, match="account pins are unavailable"):
        boundary.onboard(testnet=False, profile=profile, rpc_env=setup.MAINNET_RPC_ENV)
    assert not list(boundary.config.parent.iterdir())
    assert not boundary.wallets and not boundary.rpc_urls and not boundary.submissions


def test_mainnet_builtin_composition_uses_only_selected_profile_rpc_and_account_pins(
    boundary, monkeypatch
):
    options = mainnet_selection(monkeypatch)
    expected_profile = profile_from_mapping(setup._profile(testnet=False))
    assert boundary.onboard(**options).onboarding.complete
    assert boundary.onboard(**options).onboarding.complete
    with Client.from_config(PROVIDER, config_path=boundary.config, testnet=False) as client:
        assert client.contract.domain.chain_id == 5042
        assert client.chain.reader.profile == expected_profile
    assert set(boundary.wallet_pins) == {shared_circle._ACCOUNT_PINS[5042]}
    assert set(boundary.rpc_urls) == {"https://mainnet.example/selected"}
    assert len(boundary.submissions) == 1
    record = json.loads(boundary.config.read_bytes())
    assert record["profile"] == profile_to_mapping(expected_profile)
    assert record["rpc_env"] == setup.MAINNET_RPC_ENV


@pytest.mark.parametrize("testnet", [True, False])
@pytest.mark.parametrize("custom_endpoint", [False, True])
def test_omitted_rpc_selects_only_the_network_environment_or_public_default(
    boundary, monkeypatch, testnet, custom_endpoint
):
    selected = setup.DEFAULT_RPC_ENV if testnet else setup.MAINNET_RPC_ENV
    other = setup.MAINNET_RPC_ENV if testnet else setup.DEFAULT_RPC_ENV
    endpoint = "https://selected.example/rpc"
    monkeypatch.setenv(other, "https://wrong-network.example/rpc")
    if custom_endpoint:
        monkeypatch.setenv(selected, endpoint)
    else:
        monkeypatch.delenv(selected, raising=False)
        endpoint = (
            "https://rpc.blockdaemon.testnet.arc.io"
            if testnet
            else "https://rpc.blockdaemon.mainnet.arc.io"
        )
    assert boundary.onboard(testnet=testnet, rpc_env=None).onboarding.complete
    with Client.from_config(PROVIDER, config_path=boundary.config, testnet=testnet):
        pass
    assert set(boundary.rpc_urls) == {endpoint}
    assert json.loads(boundary.config.read_bytes())["rpc_env"] == selected
    assert len(boundary.submissions) == 1


@pytest.mark.parametrize("start_mainnet", [False, True])
def test_existing_setup_and_payment_state_cannot_switch_networks(
    boundary, monkeypatch, start_mainnet
):
    mainnet = mainnet_selection(monkeypatch)
    first, second = (mainnet, {}) if start_mainnet else ({}, mainnet)
    boundary.onboard(**first)
    client_options = {key: value for key, value in first.items() if key != "rpc_env"}
    with Client.from_config(PROVIDER, config_path=boundary.config, **client_options):
        pass
    retained = {path.name: path.read_bytes() for path in boundary.config.parent.iterdir()}
    with pytest.raises(OnboardingError, match="supported deployment"):
        boundary.onboard(**second)
    client_options = {key: value for key, value in second.items() if key != "rpc_env"}
    with pytest.raises(OnboardingError, match="supported deployment"):
        Client.from_config(PROVIDER, config_path=boundary.config, **client_options)
    assert {path.name: path.read_bytes() for path in boundary.config.parent.iterdir()} == retained
    assert len(boundary.submissions) == 1


@pytest.mark.parametrize("testnet", [True, False])
def test_explicit_rpc_environment_cannot_select_the_other_network(boundary, testnet):
    options = {
        "testnet": testnet,
        "rpc_env": setup.MAINNET_RPC_ENV if testnet else setup.DEFAULT_RPC_ENV,
    }
    with pytest.raises(OnboardingError, match="RPC environment selection"):
        boundary.onboard(**options)
    assert not boundary.rpc_urls and not boundary.wallets and not boundary.submissions
    assert not list(boundary.config.parent.iterdir())


def test_mainnet_builtin_cli_does_not_require_a_wallet_factory(monkeypatch, tmp_path):
    profile = profile_from_mapping(setup._profile())
    profile = replace(profile, domain=replace(profile.domain, chain_id=5042))
    calls = []
    monkeypatch.setattr(cli, "load_profile", lambda path: profile)
    monkeypatch.setattr(cli.importlib, "import_module", lambda name: pytest.fail("no factory"))

    def onboard(wallet, **options):
        calls.append((wallet, options))
        return setup.SetupResult(
            options["config_path"],
            ADDRESS,
            SimpleNamespace(
                complete=True,
                identity=SimpleNamespace(agent_id=77),
                balance=1_000_000,
                submitted=False,
                operation=None,
            ),
        )

    monkeypatch.setattr(cli, "onboard_circle", onboard)
    assert cli.main(["onboard", "--mainnet", "--profile", "trusted.json", "--wallet", WALLET]) == 0
    assert calls == [
        (
            WALLET,
            {
                "config_path": setup.DEFAULT_CONFIG,
                "rpc_env": setup.MAINNET_RPC_ENV,
                "agent_id": None,
                "testnet": False,
                "profile": profile,
            },
        )
    ]
