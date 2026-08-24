from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import cast

import pytest
from chaosagent_fixtures import (
    FIXTURE_V0_SCHEMA_VERSION,
    FixtureValidationError,
    canonicalize_fixture,
    digest_fixture,
    fixture_schema_v0,
    load_fixture,
    loads_fixture,
    validate_fixture,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = ROOT / "benchmarks/shipment-refund/fixtures/failed-shipment.v0.json"


def _document() -> dict[str, object]:
    return cast(dict[str, object], json.loads(FIXTURE_PATH.read_text(encoding="utf-8")))


def _rows(document: dict[str, object], name: str) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], document[name])


def test_golden_fixture_is_valid_immutable_and_has_expected_identity() -> None:
    fixture = load_fixture(FIXTURE_PATH)
    assert fixture.to_dict()["schema_version"] == FIXTURE_V0_SCHEMA_VERSION
    assert fixture.to_dict()["fixture_id"] == "fake-company.failed-shipment"
    assert fixture.digest == (
        "sha256:e5a8bd98d2255cb03c4e04aaaa9a7cdf3295e19e7c1913b88a2cf7016e6ca9fd"
    )
    assert fixture_schema_v0()["$id"] == "https://schemas.chaosagent.dev/fixture/v0/schema.json"
    with pytest.raises(FrozenInstanceError):
        setattr(fixture, "digest", "sha256:" + "0" * 64)


def test_malformed_unknown_and_missing_fields_fail_closed() -> None:
    with pytest.raises(FixtureValidationError, match="malformed JSON"):
        loads_fixture("{")
    document = _document()
    document["unknown"] = True
    with pytest.raises(FixtureValidationError, match="Additional properties"):
        validate_fixture(document)
    document = _document()
    del document["orders"]
    with pytest.raises(FixtureValidationError, match="required property"):
        validate_fixture(document)
    document = _document()
    document["schema_version"] = "chaosagent.fixture/v1"
    with pytest.raises(FixtureValidationError, match="unsupported version"):
        canonicalize_fixture(document)


def test_duplicate_ids_and_broken_cross_references_are_rejected() -> None:
    duplicate = _document()
    _rows(duplicate, "orders").append(deepcopy(_rows(duplicate, "orders")[0]))
    with pytest.raises(FixtureValidationError, match="duplicate order_id 'ORD-1007'"):
        validate_fixture(duplicate)

    broken = _document()
    _rows(broken, "shipments")[0]["order_id"] = "ORD-missing"
    with pytest.raises(FixtureValidationError, match="references missing order"):
        validate_fixture(broken)

    ownership = _document()
    _rows(ownership, "customers").append(
        {
            "customer_id": "CUS-999",
            "name": "Other Customer",
            "email": "other@example.invalid",
            "status": "active",
        }
    )
    _rows(ownership, "support_tickets")[0]["customer_id"] = "CUS-999"
    with pytest.raises(FixtureValidationError, match="does not own order"):
        validate_fixture(ownership)


def test_impossible_money_time_and_status_values_are_rejected() -> None:
    floating = _document()
    _rows(floating, "orders")[0]["total_minor"] = 12999.0
    with pytest.raises(FixtureValidationError, match="integer minor-unit"):
        validate_fixture(floating)

    negative = _document()
    _rows(negative, "payments")[0]["amount_minor"] = -1
    with pytest.raises(FixtureValidationError, match="minimum of 1"):
        validate_fixture(negative)

    future = _document()
    _rows(future, "shipments")[0]["updated_at"] = "2026-08-25T00:00:00Z"
    with pytest.raises(FixtureValidationError, match="after.*reference_time"):
        validate_fixture(future)

    status = _document()
    _rows(status, "orders")[0]["status"] = "mysterious"
    with pytest.raises(FixtureValidationError, match="not one of"):
        validate_fixture(status)

    excessive_refund = _document()
    _rows(excessive_refund, "refunds").append(
        {
            "refund_id": "REF-too-large",
            "payment_id": "PAY-1007",
            "order_id": "ORD-1007",
            "status": "pending",
            "amount_minor": 13000,
            "reason": "Invalid amount",
            "created_at": "2026-08-24T09:00:00Z",
        }
    )
    with pytest.raises(FixtureValidationError, match="non-failed total exceeds"):
        validate_fixture(excessive_refund)


def test_canonical_digest_ignores_json_format_keys_and_entity_array_order() -> None:
    document = _document()
    second_customer: dict[str, object] = {
        "status": "active",
        "email": "second@example.invalid",
        "name": "Second Customer",
        "customer_id": "CUS-999",
    }
    _rows(document, "customers").append(second_customer)
    canonical = digest_fixture(document)
    reordered = json.loads(json.dumps(document, indent=4, sort_keys=False))
    cast(list[object], reordered["customers"]).reverse()
    assert digest_fixture(reordered) == canonical

    changed = deepcopy(document)
    _rows(changed, "orders")[0]["total_minor"] = 13000
    assert digest_fixture(changed) != canonical


def test_loader_rejects_duplicate_json_object_keys_and_defensively_copies() -> None:
    with pytest.raises(FixtureValidationError, match="duplicate JSON object key"):
        loads_fixture('{"schema_version":"chaosagent.fixture/v0","schema_version":"x"}')
    fixture = load_fixture(FIXTURE_PATH)
    first = fixture.to_dict()
    _rows(first, "orders")[0]["status"] = "cancelled"
    assert _rows(fixture.to_dict(), "orders")[0]["status"] == "paid"
