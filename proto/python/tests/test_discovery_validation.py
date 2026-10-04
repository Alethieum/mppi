"""Independent hostile-document tests; no provider package is imported."""

import json
from dataclasses import replace

import pytest
from mppi_protocol import Catalog, CatalogRow, Rates, ValidationError, publish_catalog
from mppi_protocol.discovery import (
    REGISTRATION_TYPE,
    ProviderIdentity,
    normalize_origin,
    origin_from_discover,
    parse_document,
    verify_models,
    verify_registration,
)
from mppi_protocol.types import ChainObservation


@pytest.fixture
def identity():
    return ProviderIdentity(
        ChainObservation(5042002, b"c" * 20, 5, b"b" * 32),
        b"i" * 20,
        (1 << 256) - 1,
        b"o" * 20,
        b"r" * 20,
        "https://provider.example/discover",
        publish_catalog([CatalogRow("test/model", Rates(1, 2))]),
    )


def registration(identity):
    return {
        "type": REGISTRATION_TYPE,
        "name": "Test",
        "description": "Independent peer",
        "active": True,
        "x402Support": False,
        "registrations": [
            {
                "agentId": identity.provider_id,
                "agentRegistry": "eip155:5042002:0x" + (b"i" * 20).hex(),
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


def raw(value):
    return json.dumps(value).encode()


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        ("HTTPS://Provider.Example:443/", "https://provider.example"),
        ("https://provider.example:8443", "https://provider.example:8443"),
        ("https://[2001:0db8::1]:443/", "https://[2001:db8::1]"),
    ],
)
def test_origin_normalization(origin, expected):
    assert normalize_origin(origin) == expected


@pytest.mark.parametrize(
    "origin",
    [
        "http://provider.example",
        "https://user@provider.example",
        "https://provider.example?",
        "https://provider.example#",
        "https://provider.example/path",
        "https://provider.example//",
        "https://provider.example:\t443",
        " https://provider.example",
        "https://provider.example\\evil",
        "https://provider.example:0",
        "https://provider.example:",
        "https://provider.example:65536",
        "https://prov%69der.example",
        "https://provider..example",
        "https://provider.example.evil/path",
        "https://provider.example\n",
        "https://-bad.example",
        "https://prövider.example",
    ],
)
def test_ambiguous_origins_reject(origin):
    with pytest.raises(ValidationError):
        normalize_origin(origin)


def test_registry_uri_has_exact_discovery_path():
    assert (
        origin_from_discover("HTTPS://PROVIDER.EXAMPLE:443/discover") == "https://provider.example"
    )
    for uri in (
        "https://provider.example//discover",
        "https://provider.example/prefix/discover",
        "https://provider.example/discover?x=1",
        "https://provider.example/discover/",
    ):
        with pytest.raises(ValidationError):
            origin_from_discover(uri)


def test_registration_binds_exact_large_identity_and_allows_extensions(identity):
    doc = registration(identity)
    doc["custom"] = {"float": 0.1}
    doc["services"].append({"name": "OASF", "endpoint": "ipfs://example", "skills": ["chat"]})
    doc["registrations"].append({"agentId": 5, "agentRegistry": "eip155:1:0x" + (b"x" * 20).hex()})
    assert (
        verify_registration(raw(doc), identity, expected_origin="HTTPS://PROVIDER.EXAMPLE:443/")
        == "https://provider.example"
    )


@pytest.mark.parametrize(
    "fault",
    [
        "duplicate",
        "wrong_id",
        "float_id",
        "bool_id",
        "wrong_registry",
        "wrong_chain",
        "inactive",
        "service_count",
        "wrong_origin",
        "wrong_models",
        "version",
    ],
)
def test_identity_and_service_substitutions_reject(identity, fault):
    doc = registration(identity)
    entry = doc["registrations"][0]
    service = doc["services"][0]
    if fault == "duplicate":
        doc["registrations"].append(dict(entry))
    elif fault == "wrong_id":
        entry["agentId"] -= 1
    elif fault == "float_id":
        entry["agentId"] = float(entry["agentId"])
    elif fault == "bool_id":
        entry["agentId"] = True
    elif fault == "wrong_registry":
        entry["agentRegistry"] = "eip155:5042002:0x" + (b"j" * 20).hex()
    elif fault == "wrong_chain":
        entry["agentRegistry"] = "eip155:1:0x" + (b"i" * 20).hex()
    elif fault == "inactive":
        doc["active"] = False
    elif fault == "service_count":
        doc["services"].append(dict(service))
    elif fault == "wrong_origin":
        service["endpoint"] = "https://provider.example.evil"
    elif fault == "wrong_models":
        service["models"] = "https://provider.example.evil/models"
    elif fault == "version":
        service["version"] = 1
    with pytest.raises(ValidationError):
        verify_registration(raw(doc), identity)


@pytest.mark.parametrize(
    "document",
    [
        b'{"a":1,"\\u0061":2}',
        b"\xef\xbb\xbf{}",
        b'{"x":NaN}',
        b"[]",
        b'{"a": "\xff"}',
        b'{"x":' + b"[" * 128 + b"0" + b"]" * 128 + b"}",
    ],
)
def test_bounded_duplicate_safe_json(document):
    with pytest.raises(ValidationError):
        parse_document(document)
    with pytest.raises(ValidationError):
        parse_document(b'{"a":1}', max_bytes=2)


def test_catalog_record_cannot_substitute_rates_or_hash(identity):
    forged = Catalog(identity.catalog.rows, identity.catalog.canonical_bytes, bytes(32))
    with pytest.raises(ValidationError):
        replace(identity, catalog=forged)


def test_cards_are_advisory_and_cannot_add_models(identity):
    card = {
        "model": "test/model",
        "description": "test",
        "context_window": 100,
        "max_output": 10,
        "modalities": ["text"],
        "embeddings": False,
        "streaming": True,
        "tools": False,
        "structured_output": False,
        "rates": {"output": "0"},
    }
    assert verify_models(raw({"models": [card]}), identity.catalog) == ("test/model",)
    assert identity.catalog.rates_for("test/model").output == 2
    for changed in (
        {"model": "test/other"},
        {"max_output": 101},
        {"context_window": True},
        {"modalities": ["text", "text"]},
        {"tools": 1},
    ):
        with pytest.raises(ValidationError):
            verify_models(raw({"models": [card | changed]}), identity.catalog)
