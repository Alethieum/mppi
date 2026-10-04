"""Provider declarations are validated before publication or runtime startup."""

from copy import deepcopy
from pathlib import Path

import pytest
from mppi_protocol import MAX_UINT64
from mppi_provider.config import ConfigError, ProviderConfig


def configuration() -> dict[str, object]:
    return {
        "provider": {
            "name": "Independent Provider",
            "description": "Provider declarations",
            "public_origin": "HTTPS://Provider.Example:443/",
            "recipient": "0x" + "22" * 20,
            "services": [{"name": "OASF", "endpoint": "ipfs://example", "skills": ["chat"]}],
        },
        "identity": {"provider_id": "42", "wallet_address": "0x" + "11" * 20},
        "chain": {"network": "testnet", "max_age_ms": 1000},
        "upstream": {"url": "http://127.0.0.1:8080/custom/v1/", "api_key_env": "UPSTREAM_KEY"},
        "models": [
            {
                "model": "example/model",
                "description": "Text generation",
                "context_window": 128000,
                "max_output": 8192,
                "modalities": ["text"],
                "embeddings": False,
                "streaming": True,
                "tools": True,
                "structured_output": True,
                "rates": {"input": "300000", "output": "1200000"},
            }
        ],
        "payment": {"collection_margin_ms": 60000, "authorization_wait_ms": 10000},
        "settlement": {"interval_ms": 30000, "amount": "1000000"},
    }


def test_public_example_configuration_loads_without_side_effects() -> None:
    root = Path(__file__).resolve().parents[4]
    config = ProviderConfig.from_file(root / "examples/provider/mppi.toml")
    assert config.chain.chain_id == 5042002
    assert config.models[0].rates.cache == 30000
    assert config.upstream.completions_url == "http://127.0.0.1:8080/v1/chat/completions"


def set_field(values: dict[str, object], path: str, value: object) -> None:
    target = values
    parts = path.split(".")
    for part in parts[:-1]:
        target = target[int(part)] if isinstance(target, list) else target[part]  # type: ignore[index,assignment]
    target[parts[-1]] = value


def test_configuration_derives_exact_catalog_and_preserves_upstream_prefix() -> None:
    config = ProviderConfig.from_mapping(configuration())
    assert config.provider.public_origin == "https://provider.example"
    assert config.upstream.completions_url == "http://127.0.0.1:8080/custom/v1/chat/completions"
    assert config.identity.provider_id == 42
    assert config.chain.chain_id == 5042002
    assert config.payment.output_buffer_tokens == 1000
    assert config.payment.suggest_session_tokens == 1_000_000
    assert config.catalog.canonical_bytes == (
        b'[{"input":"300000","model":"example/model","output":"1200000"}]'
    )
    assert config.models[0].rates.cache is None
    assert config.listen is None


def test_programmatic_exact_uint64_and_zero_cache_are_preserved() -> None:
    values = configuration()
    set_field(values, "models.0.rates.input", MAX_UINT64)
    set_field(values, "models.0.rates.cache", 0)
    config = ProviderConfig.from_mapping(values)
    assert config.models[0].rates.input == MAX_UINT64
    assert config.models[0].rates.cache == 0
    assert b'"cache":"0"' in config.catalog.canonical_bytes


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("provider.name", ""),
        ("provider.public_origin", "https://provider.example/path"),
        ("provider.recipient", "0x" + "11" * 20),
        ("identity.provider_id", "01"),
        ("identity.provider_id", True),
        ("identity.wallet_id", "not-a-wallet-uuid"),
        ("chain.network", "another-chain"),
        ("chain.max_age_ms", 0),
        ("chain.max_age_ms", True),
        ("upstream.url", "https://secret:password@upstream.example/v1"),
        ("upstream.url", "http://upstream.example/v1?token=secret"),
        ("upstream.api_key_env", "NOT A VARIABLE"),
        ("models.0.max_output", 128001),
        ("models.0.streaming", 1),
        ("models.0.modalities", ["text", "text"]),
        ("models.0.modalities", ["unknown"]),
        ("models.0.rates.input", 1.0),
        ("models.0.rates.output", str(MAX_UINT64 + 1)),
        ("models.0.rates.cache", False),
        ("payment.authorization_wait_ms", 0),
        ("payment.authorization_wait_ms", 1 << 64),
        ("payment.output_buffer_tokens", 0),
        ("settlement.amount", "0"),
        ("settlement.units", -1),
    ],
)
def test_invalid_configuration_rejects_without_input_values(path: str, value: object) -> None:
    values = configuration()
    set_field(values, path, value)
    with pytest.raises(ConfigError) as error:
        ProviderConfig.from_mapping(values)
    assert "secret" not in str(error.value)
    assert "password" not in str(error.value)


def test_required_timings_and_unknown_fields_are_not_silently_defaulted() -> None:
    values = configuration()
    values["payment"] = {"collection_margin_ms": 1000}
    with pytest.raises(ConfigError, match="authorization_wait_ms"):
        ProviderConfig.from_mapping(values)
    values = configuration()
    values["storage"] = {"dsn": "secret"}
    with pytest.raises(ConfigError):
        ProviderConfig.from_mapping(values)


def test_duplicate_models_and_extra_mppi_service_reject() -> None:
    values = configuration()
    values["models"] = [deepcopy(values["models"][0]), deepcopy(values["models"][0])]  # type: ignore[index]
    with pytest.raises(ConfigError):
        ProviderConfig.from_mapping(values)
    values = configuration()
    set_field(values, "provider.services", [{"name": "mppi", "endpoint": "https://evil.example"}])
    with pytest.raises(ConfigError):
        ProviderConfig.from_mapping(values)


def test_config_is_a_snapshot_and_overrides_preserve_unspecified_fields() -> None:
    values = configuration()
    override: dict[str, object] = {"payment": {"output_buffer_tokens": 2048}}
    config = ProviderConfig.from_mapping(values, overrides=override)
    set_field(values, "models.0.rates.input", 999)
    set_field(values, "provider.services", [])
    assert config.models[0].rates.input == 300000
    assert config.payment.output_buffer_tokens == 2048
    assert config.payment.authorization_wait_ms == 10000
    assert config.provider.services[0].as_object()["skills"] == ["chat"]
    config.provider.services[0].as_object()["skills"] = []
    assert config.provider.services[0].as_object()["skills"] == ["chat"]


TOML = """
[provider]
name = "Test"
description = "Text"
public_origin = "https://provider.example"
recipient = "0x2222222222222222222222222222222222222222"
[identity]
provider_id = "42"
wallet_address = "0x1111111111111111111111111111111111111111"
[chain]
network = "testnet"
max_age_ms = 1000
[upstream]
url = "http://127.0.0.1:8080/v1"
[[models]]
model = "example/model"
description = "Test"
context_window = 10000
max_output = 1000
modalities = ["text"]
embeddings = false
streaming = true
tools = false
structured_output = false
rates = {input = "18446744073709551615", output = "0"}
[payment]
collection_margin_ms = 60000
authorization_wait_ms = 10000
[settlement]
interval_ms = 30000
[listen]
bind = "127.0.0.1:8443"
tls_mode = "direct"
tls_cert = "certificates/server.pem"
tls_key = "certificates/key.pem"
"""


def test_toml_amounts_and_file_paths_with_programmatic_override(tmp_path: Path) -> None:
    path = tmp_path / "mppi.toml"
    path.write_text(TOML)
    config = ProviderConfig.from_file(path, overrides={"listen": {"tls_key": "/run/key.pem"}})
    assert config.models[0].rates.input == MAX_UINT64
    assert config.listen is not None
    assert config.listen.tls_cert == tmp_path / "certificates/server.pem"
    assert config.listen.tls_key == Path("/run/key.pem")
    assert not (tmp_path / "certificates").exists()  # Validation does not provision files.


def test_toml_numeric_money_duplicate_keys_and_secret_errors_reject(tmp_path: Path) -> None:
    path = tmp_path / "mppi.toml"
    for raw in (
        TOML.replace('input = "18446744073709551615"', "input = 12"),
        TOML.replace('name = "Test"', 'name = "Test"\nname = "SECRET"'),
        'name = "SECRET\n',
    ):
        path.write_text(raw)
        with pytest.raises(ConfigError) as error:
            ProviderConfig.from_file(path)
        assert "SECRET" not in str(error.value)
        assert error.value.source == path


def test_toml_size_bound_precedes_parser(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import mppi_provider.config as module

    path = tmp_path / "large.toml"
    path.write_bytes(b"#" * (module.MAX_CONFIG_BYTES + 1))
    monkeypatch.setattr(module.tomllib, "loads", lambda _: pytest.fail("oversized input parsed"))
    with pytest.raises(ConfigError):
        ProviderConfig.from_file(path)


def test_listener_modes_remain_configuration_only() -> None:
    values = configuration()
    values["listen"] = {"bind": "0.0.0.0:8443", "tls_mode": "ingress", "tls_key": "secret.pem"}
    with pytest.raises(ConfigError):
        ProviderConfig.from_mapping(values)
    values["listen"] = {"bind": "0.0.0.0:8443", "tls_mode": "direct"}
    with pytest.raises(ConfigError):
        ProviderConfig.from_mapping(values)
    values["listen"] = {"bind": "0.0.0.0:8443", "tls_mode": "ingress"}
    assert ProviderConfig.from_mapping(values).listen is not None
