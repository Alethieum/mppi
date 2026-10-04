import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

VECTORS = Path(__file__).resolve().parents[3] / "testvectors"


@pytest.mark.parametrize("suite", ["catalog", "amounts", "lifecycle"])
def test_fixture_schema(suite: str) -> None:
    schema = json.loads((VECTORS / "schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    fixture = json.loads((VECTORS / f"{suite}.json").read_text())
    Draft202012Validator(schema).validate(fixture)


def test_amount_fixture_requires_operation_inputs() -> None:
    schema = json.loads((VECTORS / "schema.json").read_text())
    fixture = {
        "schemaVersion": 1,
        "suite": "amounts",
        "cases": [
            {"name": "missing funding input", "operation": "funding", "input": {}, "expect": "1"}
        ],
    }
    assert not Draft202012Validator(schema).is_valid(fixture)
