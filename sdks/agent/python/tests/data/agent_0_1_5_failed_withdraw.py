"""Write agent-0.1.5-failed-withdraw.json with the released mppi-agent 0.1.5.

The checkpoint mppi-agent 0.1.5 (tag agent-v0.1.5) saves after a withdrawal
its wallet refused before submission: Client._exit sends the withdrawal under
its one fixed reference, withdraw-<session id hex>, the wallet raises
NotSubmittedError, and the journal holds that reference FAILED. Every write
goes through 0.1.5's own Client._exit, _submit, _persist, _record and
Checkpoint.save; only the wallet and the chain view are stand-ins, with the
public test values of tests/test_agent_runtime.py.

Run it with the released sources, from a checkout of this repository:

    git archive agent-v0.1.5 mppi | tar -x -C <dir>
    PYTHONPATH=<dir>/mppi/sdks/agent/python/src:<dir>/mppi/proto/python/src \\
        python -B agent_0_1_5_failed_withdraw.py <output path>
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from mppi_agent import client as client_module
from mppi_agent.checkpoint import Checkpoint
from mppi_agent.client import Client
from mppi_agent.state import Ledger
from mppi_protocol._observers import Observers
from mppi_protocol.amounts import Rates
from mppi_protocol.capabilities import FundingAuthorization, NotSubmittedError
from mppi_protocol.catalog import CatalogRow, publish_catalog
from mppi_protocol.chain import Block, ChainSession
from mppi_protocol.contract import ARC_USDC, ContractCalls, OperationKind
from mppi_protocol.signing import LocalSessionSigner
from mppi_protocol.types import (
    ChainObservation,
    Domain,
    OperationResult,
    OperationState,
    PauseLevel,
    SessionDescriptor,
    SessionStatus,
)

DOMAIN = Domain(5042002, b"p" * 20)
SIGNER = LocalSessionSigner(bytes.fromhex("11" * 32))
CATALOG = publish_catalog((CatalogRow("test/model", Rates(500_000, 500_000)),))
DESCRIPTOR = SessionDescriptor(
    b"a" * 20, SIGNER.address, 7, b"r" * 20, b"o" * 20, bytes.fromhex("36" + "00" * 19), b"s" * 32
)


class RefusingWallet:
    """A wallet whose own record proves the withdrawal never reached it."""

    address = DESCRIPTOR.agent

    def execute(self, call, *, operation_ref):
        raise NotSubmittedError("wallet service refused the call before submission")

    def reconcile(self, *, operation_ref):
        raise AssertionError("not reconciled")


async def main(path: Path) -> None:
    pyproject = Path(client_module.__file__).parents[2] / "pyproject.toml"
    assert 'version = "0.1.5"' in pyproject.read_text(), "run it with the 0.1.5 sources"
    contract = ContractCalls(DOMAIN, "USDC", "2")
    observation = ChainObservation(DOMAIN.chain_id, DOMAIN.verifying_contract, 10, b"b" * 32)
    client = object.__new__(Client)
    client._observers = Observers(None)
    client.signer, client.timeout = SIGNER, 1
    client.contract, client.wallet = contract, RefusingWallet()
    client.signer_reference = "fixture:public-test-key"
    ledger = Ledger(DOMAIN, DESCRIPTOR, CATALOG)
    ledger.generation = 1
    client._ledger = ledger
    client._saved = {"version": 2, "scope": client._scope(7)}
    auth = FundingAuthorization(
        DOMAIN.chain_id,
        ARC_USDC,
        "USDC",
        "2",
        DESCRIPTOR.agent,
        DOMAIN.verifying_contract,
        100,
        0,
        2_000_000_000,
        b"n" * 32,
    )
    opening = OperationResult(
        "open-x", OperationState.CONFIRMED, tx_hash=b"o" * 32, observation=observation
    )
    client._opening_ref = "open-x"
    client._operations = {
        "open-x": (contract.open(DESCRIPTOR, 100, CATALOG.rate_card_hash, auth, bytes(65)), opening)
    }
    client._closed_result = None
    client._outputs, client._receipted = {}, set()
    client._checkpoint, client._checkpoint_lock = Checkpoint(path), asyncio.Lock()
    client._open_lock, client._funding_lock = asyncio.Lock(), asyncio.Lock()
    client._operation_locks = {}
    client._local_start = AsyncMock()
    # Closing since block time 100, so the withdrawal is due an hour later.
    closing = ChainSession(100, 0, SessionStatus.CLOSING, 100, 100, CATALOG.rate_card_hash)
    current = SimpleNamespace(
        block=Block(observation, 100 + 3600, b"c" * 32),
        sessions={ledger.session_id: closing},
        pause_level=PauseLevel.NONE,
    )
    client.chain = SimpleNamespace(
        snapshot=lambda: current, reader=SimpleNamespace(profile=SimpleNamespace(domain=DOMAIN))
    )
    await client._persist()
    result = await client._exit(ledger.session_id, OperationKind.WITHDRAW)
    assert result.state is OperationState.FAILED, result
    assert result.operation_ref == "withdraw-" + ledger.session_id.hex()
    client._checkpoint.close()


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1])))
