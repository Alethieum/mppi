"""Public agent imports retain one shared wallet implementation."""

from mppi_agent.checkpoint import Checkpoint as AgentCheckpoint
from mppi_agent.circle import CircleAgentWallet as AgentWallet
from mppi_protocol.circle import CircleAgentWallet
from mppi_protocol.local_state import Checkpoint


def test_public_agent_imports_are_shared():
    assert AgentWallet is CircleAgentWallet
    assert AgentCheckpoint is Checkpoint
