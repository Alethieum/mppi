import json

import pytest
from mppi_protocol import chain
from mppi_protocol.deployment import load_profile, profile_from_mapping, profile_to_mapping
from mppi_protocol.errors import ValidationError


def value():
    def pin(n):
        return {
            "address": "0x" + bytes([n]).hex() * 20,
            "code_hash": "0x" + bytes([n + 1]).hex() * 32,
        }

    return {
        "version": 1,
        "chain_id": 5042002,
        "proxy": pin(1),
        "implementation": pin(3),
        "registry": pin(5),
        "registry_implementation": pin(7),
    }


def test_explicit_profile_roundtrip_and_bounded_file(tmp_path):
    data = value()
    profile = profile_from_mapping(data)
    assert profile_to_mapping(profile) == data
    path = tmp_path / "profile.json"
    path.write_text(json.dumps(data))
    assert load_profile(path) == profile


@pytest.mark.parametrize(
    "bad",
    (
        "extra",
        "boolean_version",
        "wrong_chain",
        "boolean_chain",
        "zero_hash",
        "missing_pin",
        "invalid_address",
        "whitespace_hash",
    ),
)
def test_malformed_profiles_reject_before_rpc(bad):
    data = value()
    if bad == "extra":
        data["rpc_secret"] = "not permitted"
    elif bad == "boolean_version":
        data["version"] = True
    elif bad == "wrong_chain":
        data["chain_id"] = 1
    elif bad == "boolean_chain":
        data["chain_id"] = True
    elif bad == "zero_hash":
        data["proxy"]["code_hash"] = "0x" + "00" * 32
    elif bad == "missing_pin":
        del data["registry_implementation"]
    elif bad == "invalid_address":
        data["registry"]["address"] = "bad"
    else:
        data["implementation"]["code_hash"] = "0x" + " " * 64
    with pytest.raises(ValidationError):
        profile_from_mapping(data)


def test_profile_json_rejects_duplicate_fields_and_oversized_file(tmp_path):
    path = tmp_path / "profile.json"
    path.write_text('{"version":1,"version":1}')
    with pytest.raises(ValidationError):
        load_profile(path)
    path.write_bytes(b" " * ((1 << 20) + 1))
    with pytest.raises(ValidationError):
        load_profile(path)


def test_multicall3_is_pinned_to_its_canonical_address_and_runtime():
    # test_chain swaps in its stand-in's hash; the released value is held here.
    assert chain._MULTICALL3.hex() == "ca11bde05977b3631167028862be2a173976ca11"
    assert chain._MULTICALL3_CODE_HASH.hex() == (
        "d5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891"
    )
