"""Policy v0 validation, canonicalization, and deterministic evaluation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Literal, cast

import rfc8785
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type Decision = Literal["allow", "deny", "require_approval"]

POLICY_V0_SCHEMA_VERSION = "chaosagent.policy/v0"
SUPPORTED_SCHEMA_VERSIONS = frozenset({POLICY_V0_SCHEMA_VERSION})
_SCHEMA_FILENAME = "policy-v0.schema.json"


class PolicyValidationError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("Invalid ChaosAgent policy:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True, init=False)
class Policy:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("Policy instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        value = cast(object, json.loads(self.canonical_bytes))
        if not isinstance(value, dict):
            raise AssertionError("canonical policy root is not an object")
        return cast(dict[str, object], value)


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    decision: Decision
    reason_code: str


@lru_cache(maxsize=1)
def _schema_cached() -> dict[str, object]:
    parsed = cast(
        object,
        json.loads(
            files("chaosagent_policies.schema")
            .joinpath(_SCHEMA_FILENAME)
            .read_text(encoding="utf-8")
        ),
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("bundled Policy v0 schema must be an object")
    schema = cast(dict[str, object], parsed)
    Draft202012Validator.check_schema(schema)
    return schema


def policy_schema_v0() -> dict[str, object]:
    return deepcopy(_schema_cached())


def policy_schema(schema_version: str) -> dict[str, object]:
    if schema_version != POLICY_V0_SCHEMA_VERSION:
        raise PolicyValidationError([f"$.schema_version: unsupported version {schema_version!r}"])
    return policy_schema_v0()


def _path(parts: list[object]) -> str:
    return "$" + "".join(f"[{part}]" if isinstance(part, int) else f".{part}" for part in parts)


def _error_key(error: object) -> tuple[tuple[int, int | str], ...]:
    return tuple(
        (0, part) if isinstance(part, int) else (1, str(part))
        for part in list(cast(Iterable[object], cast(object, getattr(error, "path"))))
    )


def canonicalize_policy_v0(document: object) -> bytes:
    try:
        snapshot = deepcopy(document)
    except Exception as error:
        raise PolicyValidationError([f"$: could not snapshot input: {error}"]) from error
    errors = [
        f"{_path(list(error.path))}: {error.message}"
        for error in sorted(
            Draft202012Validator(_schema_cached()).iter_errors(snapshot), key=_error_key
        )
    ]
    if errors:
        raise PolicyValidationError(errors)
    assert isinstance(snapshot, dict)
    refund = cast(dict[str, object], cast(dict[str, object], snapshot["tools"])["payments.refund"])
    automatic = refund["automatic_max_minor"]
    approval = refund["approval_max_minor"]
    if type(automatic) is not int or type(approval) is not int:
        raise PolicyValidationError(["$.tools.payments.refund thresholds must be exact integers"])
    if automatic >= approval:
        raise PolicyValidationError(
            ["$.tools.payments.refund.automatic_max_minor must be below approval_max_minor"]
        )
    try:
        return rfc8785.dumps(cast(JsonValue, snapshot))
    except rfc8785.CanonicalizationError as error:
        raise PolicyValidationError(
            [f"$: cannot be represented as RFC 8785 JSON: {error}"]
        ) from error


def canonicalize_policy(document: object) -> bytes:
    if not isinstance(document, dict) or document.get("schema_version") != POLICY_V0_SCHEMA_VERSION:
        version = document.get("schema_version") if isinstance(document, dict) else None
        raise PolicyValidationError([f"$.schema_version: unsupported version {version!r}"])
    return canonicalize_policy_v0(document)


def digest_policy_v0(document: object) -> str:
    return _digest(canonicalize_policy_v0(document))


def validate_policy_v0(document: object) -> None:
    canonicalize_policy_v0(document)


def _digest(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _from_bytes(value: bytes) -> Policy:
    policy = object.__new__(Policy)
    object.__setattr__(policy, "canonical_bytes", value)
    object.__setattr__(policy, "digest", _digest(value))
    return policy


def _reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PolicyValidationError([f"$: duplicate JSON object key {key!r}"])
        result[key] = value
    return result


def _parse(data: str | bytes) -> object:
    try:
        return cast(
            object,
            json.loads(
                data,
                object_pairs_hook=_reject_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(f"non-finite JSON number {value}")
                ),
            ),
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raise PolicyValidationError([f"$: malformed JSON: {error}"]) from error


def loads_policy_v0(data: str | bytes) -> Policy:
    return _from_bytes(canonicalize_policy_v0(_parse(data)))


def loads_policy(data: str | bytes) -> Policy:
    return _from_bytes(canonicalize_policy(_parse(data)))


def load_policy(path: str | Path) -> Policy:
    try:
        return loads_policy(Path(path).read_bytes())
    except OSError as error:
        raise PolicyValidationError([f"$: cannot read {path}: {error}"]) from error


def evaluate_policy_v0(
    policy: Policy,
    *,
    tool_id: str,
    contract_version: str,
    arguments: Mapping[str, object],
    payment_currency: str | None = None,
) -> PolicyDecision:
    document = policy.to_dict()
    if document["schema_version"] != POLICY_V0_SCHEMA_VERSION:
        raise PolicyValidationError(["$.schema_version: evaluator requires Policy v0"])
    tools = cast(dict[str, object], document["tools"])
    expected_versions = {
        "orders.get": "chaosagent.tool/orders.get/v0",
        "shipping.get_status": "chaosagent.tool/shipping.get_status/v0",
        "payments.refund": "chaosagent.tool/payments.refund/v0",
        "support.update_ticket": "chaosagent.tool/support.update_ticket/v0",
    }
    if expected_versions.get(tool_id) != contract_version:
        return PolicyDecision("deny", "tool_not_permitted_by_policy")
    if tool_id != "payments.refund":
        rule = cast(dict[str, object], tools[tool_id])
        if rule["decision"] == "allow":
            return PolicyDecision("allow", f"allow_{tool_id.replace('.', '_')}")
        return PolicyDecision("deny", "tool_not_permitted_by_policy")
    amount = arguments.get("amount_minor")
    if type(amount) is not int:
        raise PolicyValidationError(["$.arguments.amount_minor must be an exact integer"])
    rule = cast(dict[str, object], tools[tool_id])
    if payment_currency is not None and payment_currency != rule["currency"]:
        return PolicyDecision("deny", "deny_refund_currency_mismatch")
    if amount <= cast(int, rule["automatic_max_minor"]):
        return PolicyDecision("allow", "allow_within_refund_limit")
    if amount <= cast(int, rule["approval_max_minor"]):
        return PolicyDecision("require_approval", "approval_required_refund_amount")
    return PolicyDecision("deny", "deny_refund_above_absolute_limit")
