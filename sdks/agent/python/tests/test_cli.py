import json
from types import SimpleNamespace

import pytest
from mppi_agent import cli
from mppi_agent.onboarding import OnboardingError
from mppi_protocol.types import OperationResult, OperationState


def setup(monkeypatch, tmp_path, *, complete=True, chain_id=5042002):
    profile = SimpleNamespace(domain=SimpleNamespace(chain_id=chain_id))
    reader = object()
    wallet = SimpleNamespace(address=b"\x11" * 20)
    calls = []
    monkeypatch.setenv("MPPI_RPC_URL", "https://rpc.example/credential-not-for-output")
    monkeypatch.setattr(cli, "load_profile", lambda path: profile)
    monkeypatch.setattr(cli, "HttpJsonRpc", lambda url: reader)
    monkeypatch.setattr(cli, "ChainReader", lambda rpc, selected: (rpc, selected))
    monkeypatch.setattr(cli, "RegistryReader", lambda chain: chain)

    def factory(selected):
        assert selected is profile
        return wallet

    monkeypatch.setattr(
        cli.importlib, "import_module", lambda module: SimpleNamespace(wallet=factory)
    )

    def onboard(**kwargs):
        assert kwargs["wallet"] is wallet and kwargs["registry"] == (reader, profile)
        calls.append(kwargs)
        return SimpleNamespace(
            complete=complete,
            identity=SimpleNamespace(agent_id=0) if complete else None,
            balance=1_000_000,
            submitted=not complete,
            operation=None if complete else OperationResult("retained-ref", OperationState.UNKNOWN),
        )

    monkeypatch.setattr(cli, "onboard", onboard)
    args = [
        "onboard",
        "--profile",
        "profile.json",
        "--config",
        str(tmp_path / "agent.json"),
        "--wallet-factory",
        "integration:wallet",
    ]
    return args, calls


@pytest.mark.parametrize("complete,exit_code", [(True, 0), (False, 2)])
def test_cli_machine_result_preserves_zero_identity_and_unknown_state(
    monkeypatch, tmp_path, capsys, complete, exit_code
):
    args, calls = setup(monkeypatch, tmp_path, complete=complete)
    assert cli.main(args) == exit_code
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["complete"] is complete
    assert result["agent_id"] == ("0" if complete else None)
    assert result["operation_state"] == (None if complete else "unknown")
    assert len(calls) == 1 and output.err == ""
    assert "credential" not in output.out


@pytest.mark.parametrize("value", ["-1", "00", "true", "1e3", str(2**256)])
def test_invalid_identity_never_reaches_wallet_onboarding(monkeypatch, tmp_path, capsys, value):
    args, calls = setup(monkeypatch, tmp_path)
    assert cli.main(args + ["--agent-id", value]) == 1
    assert calls == [] and capsys.readouterr().out == ""


def test_missing_rpc_and_untrusted_factory_syntax_never_import(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path)
    monkeypatch.delenv("MPPI_RPC_URL")
    monkeypatch.setattr(
        cli.importlib, "import_module", lambda module: pytest.fail("must not import")
    )
    assert cli.main(args) == 1
    assert cli.main(args[:-1] + ["integration:wallet()"]) == 1
    assert calls == [] and "credential" not in capsys.readouterr().err


@pytest.mark.parametrize("source", ["wallet", "rpc", "onboard"])
def test_external_exceptions_are_redacted(monkeypatch, tmp_path, capsys, source):
    args, _ = setup(monkeypatch, tmp_path)

    def fail(*args, **kwargs):
        raise RuntimeError("api-key secret and remote transaction data")

    if source == "wallet":
        monkeypatch.setattr(cli.importlib, "import_module", fail)
    elif source == "rpc":
        monkeypatch.setattr(cli, "HttpJsonRpc", fail)
    else:
        monkeypatch.setattr(cli, "onboard", fail)
    assert cli.main(args) == 1
    output = capsys.readouterr()
    assert output.out == "" and "secret" not in output.err and "Traceback" not in output.err


def test_actionable_local_error_is_reported(monkeypatch, tmp_path, capsys):
    args, _ = setup(monkeypatch, tmp_path)

    def fail(**kwargs):
        raise OnboardingError("fund the selected wallet with USDC on Arc before onboarding")

    monkeypatch.setattr(cli, "onboard", fail)
    assert cli.main(args) == 1
    assert "fund the selected wallet" in capsys.readouterr().err


def test_testnet_requires_matching_profile_before_wallet_access(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path, chain_id=5042)
    monkeypatch.setattr(
        cli.importlib, "import_module", lambda module: pytest.fail("must not import wallet")
    )
    monkeypatch.setattr(cli, "HttpJsonRpc", lambda url: pytest.fail("must not open RPC"))
    assert cli.main(args + ["--testnet"]) == 1
    assert calls == []
    assert "requires an Arc testnet deployment profile" in capsys.readouterr().err


def test_testnet_keeps_explicit_profile_and_rpc_selection(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path)
    rpc_urls = []
    original_rpc = cli.HttpJsonRpc
    monkeypatch.setattr(
        cli, "HttpJsonRpc", lambda url: (rpc_urls.append(url), original_rpc(url))[1]
    )
    monkeypatch.setenv("ARC_RPC_URL_TESTNET", "https://testnet.example/explicit")
    assert cli.main(args + ["--testnet", "--rpc-env", "ARC_RPC_URL_TESTNET"]) == 0
    assert rpc_urls == ["https://testnet.example/explicit"]
    assert len(calls) == 1 and capsys.readouterr().err == ""


def test_testnet_does_not_supply_rpc_or_deployment_defaults(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path)
    monkeypatch.delenv("MPPI_RPC_URL")
    assert cli.main(args + ["--testnet"]) == 1
    assert calls == []
    with pytest.raises(SystemExit) as error:
        cli.main(
            ["onboard", "--testnet", "--config", "agent.json", "--wallet-factory", "own:wallet"]
        )
    assert error.value.code == 2
    assert "--profile" in capsys.readouterr().err


def test_mainnet_requires_matching_profile_before_wallet_or_rpc(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli.importlib, "import_module", lambda module: pytest.fail("must not import wallet")
    )
    monkeypatch.setattr(cli, "HttpJsonRpc", lambda url: pytest.fail("must not open RPC"))
    assert cli.main(args + ["--mainnet"]) == 1
    assert calls == []
    assert "requires an Arc mainnet deployment profile" in capsys.readouterr().err


def test_explicit_mainnet_uses_trusted_profile_and_rpc(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path, chain_id=5042)
    assert cli.main(args + ["--mainnet"]) == 0
    assert len(calls) == 1 and capsys.readouterr().err == ""


def test_network_flags_are_mutually_exclusive_before_wallet_access(monkeypatch, tmp_path, capsys):
    args, calls = setup(monkeypatch, tmp_path)
    with pytest.raises(SystemExit) as error:
        cli.main(args + ["--mainnet", "--testnet"])
    assert error.value.code == 2 and calls == []
    assert "not allowed" in capsys.readouterr().err


@pytest.mark.parametrize("network", ["--testnet", "--mainnet"])
def test_builtin_requires_wallet_before_setup(monkeypatch, capsys, network):
    monkeypatch.setattr(cli, "onboard_circle", lambda *args, **kw: pytest.fail("must not set up"))
    assert cli.main(["onboard", network]) == 1
    assert "--wallet ADDRESS" in capsys.readouterr().err
