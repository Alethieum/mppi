"""Public discovery contains only verified identity bindings and declarations."""

import json
from dataclasses import replace

import pytest
from mppi_protocol import CatalogRow, Rates, publish_catalog
from mppi_protocol.discovery import ProviderIdentity, verify_models, verify_registration
from mppi_protocol.types import ChainObservation
from mppi_provider.config import (
    AdditionalService,
    ChainConfig,
    ConfigError,
    ModelConfig,
    PaymentConfig,
    ProviderConfig,
    ProviderDetails,
    ProviderIdentityConfig,
    SettlementConfig,
    UpstreamConfig,
)
from mppi_provider.discovery import DiscoveryDocuments


def declarations() -> tuple[ProviderConfig, ProviderIdentity]:
    config = ProviderConfig(
        provider=ProviderDetails(
            name="Public provider",
            description="Text models",
            public_origin="https://provider.example",
            recipient=b"r" * 20,
            services=(
                AdditionalService.from_mapping(
                    {"name": "OASF", "endpoint": "ipfs://card", "skills": ["chat"]}
                ),
            ),
        ),
        identity=ProviderIdentityConfig(provider_id=(1 << 256) - 1, wallet_address=b"o" * 20),
        chain=ChainConfig(network="testnet", max_age_ms=1000),
        upstream=UpstreamConfig(url="http://private.internal/v1", api_key_env="PRIVATE_API_KEY"),
        models=(
            ModelConfig(
                model="example/model",
                description="Text",
                context_window=10000,
                max_output=1000,
                modalities=("text",),
                embeddings=False,
                streaming=True,
                tools=False,
                structured_output=False,
                rates=Rates(input=1, output=2),
            ),
        ),
        payment=PaymentConfig(collection_margin_ms=60000, authorization_wait_ms=10000),
        settlement=SettlementConfig(interval_ms=30000),
    )
    identity = ProviderIdentity(
        observation=ChainObservation(
            chain_id=5042002, contract_address=b"c" * 20, block_number=10, block_hash=b"b" * 32
        ),
        registry_address=b"i" * 20,
        provider_id=config.identity.provider_id,
        owner=b"o" * 20,
        recipient=b"r" * 20,
        agent_uri="https://provider.example/discover",
        catalog=config.catalog,
    )
    return config, identity


def test_all_discovery_paths_bind_one_identity_without_private_configuration() -> None:
    config, identity = declarations()
    documents = DiscoveryDocuments(config, identity)
    primary = documents.get("/discover")
    assert primary is not None
    assert primary == documents.get("/.well-known/agent-registration.json")
    assert dict(primary.headers)["content-length"] == str(len(primary.body))
    assert dict(primary.headers)["content-type"] == "application/json; charset=utf-8"
    registration = json.loads(primary.body)
    assert registration["registrations"] == [
        {"agentId": (1 << 256) - 1, "agentRegistry": "eip155:5042002:0x" + (b"i" * 20).hex()}
    ]
    assert registration["services"][1]["skills"] == ["chat"]
    assert "image" not in registration
    assert "supportedTrust" not in registration
    assert verify_registration(primary.body, identity) == "https://provider.example"
    cards = documents.get("/models")
    assert cards is not None
    assert verify_models(cards.body, identity.catalog) == ("example/model",)
    assert "rates" not in json.loads(cards.body)["models"][0]
    for body in (primary.body, cards.body):
        assert b"private.internal" not in body
        assert b"PRIVATE_API_KEY" not in body
        assert b"collection_margin_ms" not in body
        assert b"wallet_id" not in body
    for path in ("/v1/chat/completions", "/discover/", "/unknown", "/models?token=secret"):
        assert documents.get(path) is None


@pytest.mark.parametrize(
    "field", ["provider_id", "owner", "recipient", "chain", "origin", "catalog"]
)
def test_mismatched_evidence_never_generates_an_active_document(field: str) -> None:
    config, identity = declarations()
    changes = {
        "provider_id": {"provider_id": 1},
        "owner": {"owner": b"x" * 20},
        "recipient": {"recipient": b"x" * 20},
        "chain": {"observation": replace(identity.observation, chain_id=5042)},
        "origin": {"agent_uri": "https://other.example/discover"},
        "catalog": {
            "catalog": publish_catalog([CatalogRow("example/model", Rates(input=1, output=3))])
        },
    }
    with pytest.raises(ConfigError):
        DiscoveryDocuments(config, replace(identity, **changes[field]))


def test_pending_local_prices_require_explicit_application_after_confirmation() -> None:
    config, identity = declarations()
    changed = replace(config, models=(replace(config.models[0], rates=Rates(input=3, output=4)),))
    with pytest.raises(ConfigError):
        DiscoveryDocuments(changed, identity)
    assert (
        DiscoveryDocuments(changed, replace(identity, catalog=changed.catalog)).get("/models")
        is not None
    )
    assert config.catalog.rates_for("example/model") == Rates(input=1, output=2)


def test_identity_origin_uses_the_same_normalization_as_the_receiver() -> None:
    config, identity = declarations()
    normalized = replace(identity, agent_uri="HTTPS://Provider.Example:443/discover")
    assert (
        DiscoveryDocuments(config, normalized).registration
        == DiscoveryDocuments(config, identity).registration
    )


def test_unpaired_unicode_cycles_and_non_json_extensions_reject() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)
    for value in ("\ud800", cyclic, float("nan"), object(), {1: "nonstring key"}):
        with pytest.raises(ConfigError):
            AdditionalService.from_mapping(
                {"name": "other", "endpoint": "email@example.org", "extension": value}
            )


def test_additional_service_size_is_checked_and_body_limit_is_aggregate() -> None:
    from mppi_protocol.discovery import MAX_DOCUMENT_BYTES

    with pytest.raises(ConfigError):
        AdditionalService.from_mapping(
            {"name": "other", "endpoint": "x", "extension": "x" * MAX_DOCUMENT_BYTES}
        )
    config, identity = declarations()
    extra = AdditionalService.from_mapping(
        {"name": "other", "endpoint": "x", "extension": "x" * (MAX_DOCUMENT_BYTES // 2)}
    )
    enlarged = replace(config, provider=replace(config.provider, services=(extra, extra)))
    with pytest.raises(ConfigError):
        DiscoveryDocuments(enlarged, identity)
