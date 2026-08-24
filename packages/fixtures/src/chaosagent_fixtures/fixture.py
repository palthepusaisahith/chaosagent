"""Versioned Fixture v0 validation, canonicalization, and immutable loading."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import cast

import rfc8785
from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]

FIXTURE_V0_SCHEMA_VERSION = "chaosagent.fixture/v0"
SUPPORTED_SCHEMA_VERSIONS = frozenset({FIXTURE_V0_SCHEMA_VERSION})
_V0_SCHEMA_FILENAME = "fixture-v0.schema.json"
_ENTITY_COLLECTIONS = (
    "customers",
    "orders",
    "shipments",
    "payments",
    "refunds",
    "support_tickets",
)
_ENTITY_ID_FIELDS = {
    "customers": "customer_id",
    "orders": "order_id",
    "shipments": "shipment_id",
    "payments": "payment_id",
    "refunds": "refund_id",
    "support_tickets": "ticket_id",
}


class FixtureValidationError(ValueError):
    """Raised when a document is not a supported, valid Fixture contract."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("Invalid ChaosAgent fixture:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True, init=False)
class Fixture:
    """Immutable canonical fixture bytes produced only by a validated loader."""

    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("Fixture instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh mutable copy of the canonical fixture document."""
        value = cast(object, json.loads(self.canonical_bytes))
        if not isinstance(value, dict):
            raise AssertionError("canonical fixture root is not an object")
        return cast(dict[str, object], value)


@lru_cache(maxsize=1)
def _fixture_schema_v0_cached() -> dict[str, object]:
    resource = files("chaosagent_fixtures.schema").joinpath(_V0_SCHEMA_FILENAME)
    parsed = cast(object, json.loads(resource.read_text(encoding="utf-8")))
    if not isinstance(parsed, dict):
        raise RuntimeError("bundled Fixture v0 schema must be a JSON object")
    schema = cast(dict[str, object], parsed)
    Draft202012Validator.check_schema(schema)
    return schema


def fixture_schema_v0() -> dict[str, object]:
    return deepcopy(_fixture_schema_v0_cached())


def fixture_schema(schema_version: str) -> dict[str, object]:
    if schema_version == FIXTURE_V0_SCHEMA_VERSION:
        return fixture_schema_v0()
    raise FixtureValidationError([f"$.schema_version: unsupported version {schema_version!r}"])


def _json_path(parts: list[object]) -> str:
    if not parts:
        return "$"
    return "$" + "".join(f"[{part}]" if isinstance(part, int) else f".{part}" for part in parts)


def _error_sort_key(error: object) -> tuple[tuple[int, int | str], ...]:
    path = cast(object, getattr(error, "path"))
    return tuple(
        (0, part) if isinstance(part, int) else (1, str(part))
        for part in list(cast(Iterable[object], path))
    )


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _indexed(document: dict[str, object], collection: str) -> dict[str, dict[str, object]]:
    id_field = _ENTITY_ID_FIELDS[collection]
    rows = cast(list[dict[str, object]], document[collection])
    return {cast(str, row[id_field]): row for row in rows}


def _parse_timestamp(value: object, path: str, errors: list[str]) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(cast(str, value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        errors.append(f"{path} must include a timezone")
        return None
    return parsed


def _semantic_errors_v0(document: dict[str, object]) -> list[str]:
    errors: list[str] = []
    indexes: dict[str, dict[str, dict[str, object]]] = {}
    for collection in _ENTITY_COLLECTIONS:
        id_field = _ENTITY_ID_FIELDS[collection]
        rows = cast(list[dict[str, object]], document[collection])
        identifiers = [cast(str, row[id_field]) for row in rows]
        for duplicate in _duplicates(identifiers):
            errors.append(f"$.{collection} contains duplicate {id_field} {duplicate!r}")
        indexes[collection] = _indexed(document, collection)

    customers = indexes["customers"]
    orders = indexes["orders"]
    payments = indexes["payments"]

    for index, order in enumerate(cast(list[dict[str, object]], document["orders"])):
        customer_id = cast(str, order["customer_id"])
        if customer_id not in customers:
            errors.append(
                f"$.orders[{index}].customer_id references missing customer {customer_id!r}"
            )
        if type(order["total_minor"]) is not int:
            errors.append(f"$.orders[{index}].total_minor must use an integer minor-unit value")

    shipment_order_ids: list[str] = []
    for index, shipment in enumerate(cast(list[dict[str, object]], document["shipments"])):
        order_id = cast(str, shipment["order_id"])
        shipment_order_ids.append(order_id)
        if order_id not in orders:
            errors.append(f"$.shipments[{index}].order_id references missing order {order_id!r}")
    for duplicate in _duplicates(shipment_order_ids):
        errors.append(f"$.shipments contains multiple V0 shipments for order {duplicate!r}")

    payment_order_ids: list[str] = []
    for index, payment in enumerate(cast(list[dict[str, object]], document["payments"])):
        order_id = cast(str, payment["order_id"])
        payment_order_ids.append(order_id)
        if type(payment["amount_minor"]) is not int:
            errors.append(f"$.payments[{index}].amount_minor must use an integer minor-unit value")
        referenced_order = orders.get(order_id)
        if referenced_order is None:
            errors.append(f"$.payments[{index}].order_id references missing order {order_id!r}")
        elif payment["currency"] != referenced_order["currency"]:
            errors.append(f"$.payments[{index}].currency must match order {order_id!r}")
        elif cast(int, payment["amount_minor"]) > cast(int, referenced_order["total_minor"]):
            errors.append(f"$.payments[{index}].amount_minor exceeds order {order_id!r} total")
    for duplicate in _duplicates(payment_order_ids):
        errors.append(f"$.payments contains multiple V0 payments for order {duplicate!r}")

    refunded_by_payment: dict[str, int] = {}
    for index, refund in enumerate(cast(list[dict[str, object]], document["refunds"])):
        payment_id = cast(str, refund["payment_id"])
        order_id = cast(str, refund["order_id"])
        if type(refund["amount_minor"]) is not int:
            errors.append(f"$.refunds[{index}].amount_minor must use an integer minor-unit value")
        referenced_payment = payments.get(payment_id)
        if referenced_payment is None:
            errors.append(
                f"$.refunds[{index}].payment_id references missing payment {payment_id!r}"
            )
        elif referenced_payment["order_id"] != order_id:
            errors.append(f"$.refunds[{index}] order does not match payment {payment_id!r}")
        if order_id not in orders:
            errors.append(f"$.refunds[{index}].order_id references missing order {order_id!r}")
        if refund["status"] != "failed" and type(refund["amount_minor"]) is int:
            refunded_by_payment[payment_id] = (
                refunded_by_payment.get(payment_id, 0) + refund["amount_minor"]
            )
    for payment_id, total in sorted(refunded_by_payment.items()):
        referenced_payment = payments.get(payment_id)
        if referenced_payment is not None and total > cast(int, referenced_payment["amount_minor"]):
            errors.append(f"$.refunds non-failed total exceeds payment {payment_id!r}")

    for index, ticket in enumerate(cast(list[dict[str, object]], document["support_tickets"])):
        customer_id = cast(str, ticket["customer_id"])
        order_id = cast(str, ticket["order_id"])
        if customer_id not in customers:
            errors.append(
                f"$.support_tickets[{index}].customer_id references "
                f"missing customer {customer_id!r}"
            )
        referenced_order = orders.get(order_id)
        if referenced_order is None:
            errors.append(
                f"$.support_tickets[{index}].order_id references missing order {order_id!r}"
            )
        elif referenced_order["customer_id"] != customer_id:
            errors.append(f"$.support_tickets[{index}] customer does not own order {order_id!r}")

    reference_time = _parse_timestamp(document["reference_time"], "$.reference_time", errors)
    timestamp_fields = {
        "orders": "placed_at",
        "shipments": "updated_at",
        "payments": "captured_at",
        "refunds": "created_at",
        "support_tickets": "updated_at",
    }
    if reference_time is not None:
        for collection, field in timestamp_fields.items():
            for index, row in enumerate(cast(list[dict[str, object]], document[collection])):
                timestamp = _parse_timestamp(row[field], f"$.{collection}[{index}].{field}", errors)
                if timestamp is not None and timestamp > reference_time:
                    errors.append(f"$.{collection}[{index}].{field} is after $.reference_time")
    return errors


def _snapshot(document: object) -> object:
    try:
        return deepcopy(document)
    except Exception as error:
        raise FixtureValidationError([f"$: could not snapshot input document: {error}"]) from error


def _canonicalize_snapshot_v0(snapshot: object) -> bytes:
    validator = Draft202012Validator(_fixture_schema_v0_cached(), format_checker=FormatChecker())
    schema_errors = sorted(validator.iter_errors(snapshot), key=_error_sort_key)
    errors = [f"{_json_path(list(error.path))}: {error.message}" for error in schema_errors]
    if errors:
        raise FixtureValidationError(errors)
    if not isinstance(snapshot, dict):
        raise FixtureValidationError(["$: must be an object"])
    document = cast(dict[str, object], snapshot)
    errors.extend(_semantic_errors_v0(document))
    if errors:
        raise FixtureValidationError(errors)
    for collection in _ENTITY_COLLECTIONS:
        id_field = _ENTITY_ID_FIELDS[collection]
        cast(list[dict[str, object]], document[collection]).sort(
            key=lambda row: cast(str, row[id_field])
        )
    try:
        return rfc8785.dumps(cast(JsonValue, document))
    except rfc8785.CanonicalizationError as error:
        raise FixtureValidationError(
            [f"$: cannot be represented as RFC 8785 canonical JSON: {error}"]
        ) from error


def canonicalize_fixture_v0(document: object) -> bytes:
    return _canonicalize_snapshot_v0(_snapshot(document))


def validate_fixture_v0(document: object) -> None:
    canonicalize_fixture_v0(document)


def digest_fixture_v0(document: object) -> str:
    return _digest_bytes(canonicalize_fixture_v0(document))


def _schema_version(document: object) -> str:
    if not isinstance(document, dict):
        raise FixtureValidationError(["$: must be an object containing schema_version"])
    version = cast(dict[object, object], document).get("schema_version")
    if not isinstance(version, str):
        raise FixtureValidationError(["$.schema_version: must be a string"])
    return version


def canonicalize_fixture(document: object) -> bytes:
    snapshot = _snapshot(document)
    version = _schema_version(snapshot)
    if version == FIXTURE_V0_SCHEMA_VERSION:
        return _canonicalize_snapshot_v0(snapshot)
    raise FixtureValidationError([f"$.schema_version: unsupported version {version!r}"])


def validate_fixture(document: object) -> None:
    canonicalize_fixture(document)


def digest_fixture(document: object) -> str:
    return _digest_bytes(canonicalize_fixture(document))


def _digest_bytes(canonical_bytes: bytes) -> str:
    return f"sha256:{hashlib.sha256(canonical_bytes).hexdigest()}"


def _fixture_from_canonical_bytes(canonical_bytes: bytes) -> Fixture:
    fixture = object.__new__(Fixture)
    object.__setattr__(fixture, "canonical_bytes", canonical_bytes)
    object.__setattr__(fixture, "digest", _digest_bytes(canonical_bytes))
    return fixture


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FixtureValidationError([f"$: duplicate JSON object key {key!r}"])
        result[key] = value
    return result


def _reject_nonfinite_number(value: str) -> object:
    raise FixtureValidationError([f"$: non-finite JSON number {value!r} is not permitted"])


def _parse_json(data: str | bytes) -> object:
    try:
        return cast(
            object,
            json.loads(
                data,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_nonfinite_number,
            ),
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise FixtureValidationError([f"$: malformed JSON: {error}"]) from error


def loads_fixture_v0(data: str | bytes) -> Fixture:
    return _fixture_from_canonical_bytes(canonicalize_fixture_v0(_parse_json(data)))


def load_fixture_v0(path: str | Path) -> Fixture:
    return loads_fixture_v0(_read_fixture_file(path))


def loads_fixture(data: str | bytes) -> Fixture:
    return _fixture_from_canonical_bytes(canonicalize_fixture(_parse_json(data)))


def load_fixture(path: str | Path) -> Fixture:
    return loads_fixture(_read_fixture_file(path))


def _read_fixture_file(path: str | Path) -> bytes:
    fixture_path = Path(path)
    try:
        return fixture_path.read_bytes()
    except OSError as error:
        raise FixtureValidationError([f"$: cannot read {fixture_path}: {error}"]) from error
