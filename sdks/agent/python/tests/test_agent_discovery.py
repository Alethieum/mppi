"""Agent trust checks with independent documents, without importing provider SDK."""

import json

import pytest
from mppi.v1.mppi_pb2 import Challenge
from mppi_agent.discovery import discover, verify_challenge
from mppi_protocol import CatalogRow, Rates, ValidationError, publish_catalog
from mppi_protocol.discovery import REGISTRATION_TYPE, ProviderIdentity
from mppi_protocol.types import ChainObservation


@pytest.fixture
def identity():
    return ProviderIdentity(
        ChainObservation(5042002, b"c" * 20, 1, b"b" * 32),
        b"i" * 20,
        2**200,
        b"o" * 20,
        b"r" * 20,
        "https://provider.example/discover",
        publish_catalog([CatalogRow("test/model", Rates(5, 10))]),
    )


def challenge_for(identity):
    return Challenge(
        request_id=b"q" * 32,
        model="test/model",
        rate_card_hash=identity.catalog.rate_card_hash,
        provider_id=identity.provider_id.to_bytes(32, "big"),
        contract=identity.observation.contract_address,
        chain_id=5042002,
        operator=identity.owner,
        cover_output_tokens=1000,
        suggest_session_tokens=1000000,
    )


def test_correlated_challenge(identity):
    verify_challenge(challenge_for(identity), identity, request_id=b"q" * 32, model="test/model")


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("request_id", b"z" * 32),
        ("model", "test/other"),
        ("rate_card_hash", b"z" * 32),
        ("provider_id", bytes(32)),
        ("contract", b"z" * 20),
        ("chain_id", 1),
        ("operator", b"z" * 20),
        ("cover_output_tokens", 0),
        ("suggest_session_tokens", 0),
    ],
)
def test_altered_or_absent_challenge_never_authorizes_funding(identity, field, bad):
    challenge = challenge_for(identity)
    setattr(challenge, field, bad)
    with pytest.raises(ValidationError):
        verify_challenge(challenge, identity, request_id=b"q" * 32, model="test/model")
    challenge.ClearField(field)
    with pytest.raises(ValidationError):
        verify_challenge(challenge, identity, request_id=b"q" * 32, model="test/model")


def test_agent_fetches_only_registry_bound_urls(identity):
    class IndependentPeer:
        def __init__(self):
            self.calls = []

        def request(self, url):
            self.calls.append(url)
            if url.endswith("/discover"):
                return json.dumps(
                    {
                        "type": REGISTRATION_TYPE,
                        "name": "Peer",
                        "description": "Independent",
                        "active": True,
                        "x402Support": False,
                        "registrations": [
                            {
                                "agentId": identity.provider_id,
                                "agentRegistry": "eip155:5042002:0x"
                                + identity.registry_address.hex(),
                            }
                        ],
                        "services": [
                            {
                                "name": "mppi",
                                "version": "1",
                                "endpoint": "https://provider.example",
                                "models": "https://provider.example/models",
                            }
                        ],
                    }
                ).encode()
            return b'{"models":[]}'

    peer = IndependentPeer()
    result = discover(identity, expected_origin="HTTPS://PROVIDER.EXAMPLE:443/", transport=peer)
    assert result.identity == identity
    assert peer.calls == ["https://provider.example/discover", "https://provider.example/models"]
    peer.calls.clear()
    with pytest.raises(ValidationError):
        discover(identity, expected_origin="https://evil.example", transport=peer)
    assert peer.calls == []
