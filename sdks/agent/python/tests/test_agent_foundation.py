import pytest
from mppi_agent import AgentSessions, Rates, funding_amount


def test_role_package_exposes_exact_funding_math() -> None:
    assert funding_amount(10000, Rates(1000000, 3000000)) == 30000
    with pytest.raises(TypeError):
        AgentSessions()  # type: ignore[misc]
