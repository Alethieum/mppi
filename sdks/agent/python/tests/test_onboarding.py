import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from mppi_agent.onboarding import OnboardingError, onboard
from mppi_protocol.chain import CodePin, DeploymentProfile
from mppi_protocol.errors import ValidationError
from mppi_protocol.types import ChainObservation, Domain, OperationResult, OperationState

ADDRESS = b"\x11" * 20
PROXY = b"\x22" * 20
REGISTRY = b"\x33" * 20
TX = b"\x44" * 32
OBSERVATION = ChainObservation(5042002, PROXY, 10, b"\x55" * 32)
PROFILE = DeploymentProfile(
    Domain(5042002, PROXY),
    REGISTRY,
    CodePin(PROXY, b"\x01" * 32),
    CodePin(b"\x02" * 20, b"\x03" * 32),
    CodePin(REGISTRY, b"\x04" * 32),
    CodePin(b"\x05" * 20, b"\x06" * 32),
)


class Registry:
    def __init__(self):
        self.chain = SimpleNamespace(profile=PROFILE)
        self.available = 1_000_000
        self.identity = SimpleNamespace(agent_id=77, owner=ADDRESS, agent_wallet=ADDRESS)
        self.result_state = OperationState.CONFIRMED
        self.reads = 0
        self.simulations = 0

    def verified_block(self):
        return OBSERVATION

    def balance(self, address, *, block):
        assert address == ADDRESS and block == OBSERVATION
        return self.available

    def read(self, agent_id, *, block):
        assert agent_id == 77 and block == OBSERVATION
        self.reads += 1
        return self.identity

    def simulate(self, operation):
        assert operation.call.to == REGISTRY
        self.simulations += 1

    def reconcile(self, operation, *, operation_ref, tx_hash, wallet_operation_id=None):
        assert operation.call.to == REGISTRY and tx_hash == TX
        return SimpleNamespace(
            operation=OperationResult(
                operation_ref, self.result_state, wallet_operation_id, tx_hash, OBSERVATION
            ),
            identity=self.identity if self.result_state is OperationState.CONFIRMED else None,
        )


class Wallet:
    address = ADDRESS

    def __init__(self, path):
        self.path = path
        self.calls = []
        self.queries = []
        self.offline = False

    def execute(self, call, *, operation_ref):
        retained = json.loads(self.path.read_text())
        assert retained["registration"]["operation_ref"] == operation_ref
        assert retained["registration"]["calldata"] == "0x" + call.data.hex()
        self.calls.append((call, operation_ref))
        if self.offline:
            raise OSError("sensitive upstream diagnostics must not be echoed")
        return OperationResult(operation_ref, OperationState.PENDING, "wallet-op", TX)

    def reconcile(self, *, operation_ref):
        self.queries.append(operation_ref)
        if self.offline:
            raise OSError("sensitive upstream diagnostics")
        return OperationResult(operation_ref, OperationState.PENDING, "wallet-op", TX)


def test_onboard_confirms_identity_and_complete_rerun_only_checks_current_state(tmp_path):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    first = onboard(wallet=wallet, registry=registry, config_path=path)
    assert first.complete and first.submitted and first.identity.agent_id == 77
    assert first.operation.state is OperationState.CONFIRMED
    before = path.read_bytes()
    second = onboard(wallet=wallet, registry=registry, config_path=path)
    assert second.complete and not second.submitted
    assert path.read_bytes() == before
    assert len(wallet.calls) == 1 and wallet.queries == []
    assert registry.reads == 1 and registry.simulations == 1
    assert path.stat().st_mode & 0o777 == 0o600


def test_lost_submission_response_reconciles_same_registration_without_resubmitting(tmp_path):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    wallet.offline = True
    first = onboard(wallet=wallet, registry=registry, config_path=path)
    assert not first.complete and first.operation.state is OperationState.UNKNOWN
    original = first.operation.operation_ref
    wallet.offline = False
    restored = Wallet(path)
    result = onboard(wallet=restored, registry=registry, config_path=path)
    assert result.complete and not result.submitted
    assert restored.calls == [] and restored.queries == [original]


def test_known_transaction_survives_wallet_outage_and_is_verified_directly(tmp_path):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    registry.result_state = OperationState.PENDING
    first = onboard(wallet=wallet, registry=registry, config_path=path)
    wallet.offline = True
    registry.result_state = OperationState.CONFIRMED
    second = onboard(wallet=wallet, registry=registry, config_path=path)
    assert second.complete and second.operation.tx_hash == TX
    assert second.operation.wallet_operation_id == "wallet-op"
    assert second.operation.operation_ref == first.operation.operation_ref
    assert len(wallet.calls) == 1


@pytest.mark.parametrize("state", (OperationState.PENDING, OperationState.UNKNOWN))
def test_uncertain_registration_remains_pending_across_repeated_invocations(tmp_path, state):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    registry.result_state = state
    one = onboard(wallet=wallet, registry=registry, config_path=path)
    two = onboard(wallet=wallet, registry=registry, config_path=path)
    assert not one.complete and not two.complete
    assert len(wallet.calls) == 1
    assert one.operation.operation_ref == two.operation.operation_ref


def test_only_canonical_failed_registration_can_be_replaced_by_next_explicit_invocation(tmp_path):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    registry.result_state = OperationState.FAILED
    failed = onboard(wallet=wallet, registry=registry, config_path=path)
    assert not failed.complete and failed.operation.state is OperationState.FAILED
    registry.result_state = OperationState.CONFIRMED
    completed = onboard(wallet=wallet, registry=registry, config_path=path)
    assert completed.complete and len(wallet.calls) == 2
    assert failed.operation.operation_ref != completed.operation.operation_ref


def test_existing_agent_id_needs_no_registration_or_wallet_signature(tmp_path):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    result = onboard(wallet=wallet, registry=registry, config_path=path, agent_id=77)
    assert result.complete and result.operation is None
    assert wallet.calls == wallet.queries == [] and registry.simulations == 0


@pytest.mark.parametrize("when", ("new", "rerun"))
def test_empty_wallet_never_completes_onboarding(tmp_path, when):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    if when == "rerun":
        onboard(wallet=wallet, registry=registry, config_path=path)
    before = len(wallet.calls)
    registry.available = 0
    with pytest.raises(OnboardingError, match="fund the selected wallet"):
        onboard(wallet=wallet, registry=registry, config_path=path)
    assert len(wallet.calls) == before


@pytest.mark.parametrize("field", ("owner", "agent_wallet"))
def test_completed_identity_transfer_or_wallet_rebinding_is_detected_on_rerun(tmp_path, field):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    onboard(wallet=wallet, registry=registry, config_path=path)
    before = path.read_bytes()
    setattr(registry.identity, field, b"\x66" * 20)
    with pytest.raises(OnboardingError, match="must match"):
        onboard(wallet=wallet, registry=registry, config_path=path)
    assert path.read_bytes() == before and len(wallet.calls) == 1


@pytest.mark.parametrize("change", ("wallet", "deployment", "identity", "extra", "calldata"))
def test_conflicting_saved_configuration_cannot_create_replacement_registration(tmp_path, change):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    onboard(wallet=wallet, registry=registry, config_path=path)
    data = json.loads(path.read_text())
    if change == "wallet":
        data["wallet"] = "0x" + (b"\x77" * 20).hex()
    elif change == "deployment":
        registry.chain.profile = replace(PROFILE, domain=Domain(5042, PROXY))
    elif change == "identity":
        data["agent_id"] = "077"
    elif change == "extra":
        data["unexpected"] = "value"
    else:
        data["registration"]["calldata"] = "0x00"
    path.write_text(json.dumps(data))
    with pytest.raises((OnboardingError, ValidationError)):
        onboard(wallet=wallet, registry=registry, config_path=path)
    assert len(wallet.calls) == 1


def test_second_process_cannot_register_through_same_locked_configuration(tmp_path):
    from mppi_agent.checkpoint import Checkpoint

    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()
    lock = Checkpoint(path)
    try:
        with pytest.raises(ValidationError, match="already in use"):
            onboard(wallet=wallet, registry=registry, config_path=path)
        assert wallet.calls == []
    finally:
        lock.close()


def test_wallet_rotation_during_simulation_cannot_submit_an_identity_for_another_wallet(tmp_path):
    path = tmp_path / "agent.json"
    wallet, registry = Wallet(path), Registry()

    def rotate(operation):
        wallet.address = b"\x88" * 20

    registry.simulate = rotate
    with pytest.raises(OnboardingError, match="authority changed"):
        onboard(wallet=wallet, registry=registry, config_path=path)
    assert wallet.calls == []
    assert json.loads(path.read_text())["registration"] is None
