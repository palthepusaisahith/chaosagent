"""Versioned scenario loading, validation, and semantic canonicalization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import cast

import rfc8785
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]

SCENARIO_V0_SCHEMA_VERSION = "chaosagent.scenario/v0"
SUPPORTED_SCHEMA_VERSIONS = frozenset({SCENARIO_V0_SCHEMA_VERSION})
_V0_SCHEMA_FILENAME = "scenario-v0.schema.json"


class ScenarioValidationError(ValueError):
    """Raised when a document is not a supported, valid scenario contract."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("Invalid ChaosAgent scenario:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True, init=False)
class Scenario:
    """Immutable canonical bytes and identity produced only by a validated loader."""

    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("Scenario instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh mutable copy of the canonical scenario document."""
        value = cast(object, json.loads(self.canonical_bytes))
        if not isinstance(value, dict):  # Defensive: validated scenarios always have object roots.
            raise AssertionError("canonical scenario root is not an object")
        return cast(dict[str, object], value)


@lru_cache(maxsize=1)
def _scenario_schema_v0_cached() -> dict[str, object]:
    resource = files("chaosagent_scenarios.schema").joinpath(_V0_SCHEMA_FILENAME)
    parsed = cast(object, json.loads(resource.read_text(encoding="utf-8")))
    if not isinstance(parsed, dict):
        raise RuntimeError("bundled Scenario v0 schema must be a JSON object")
    schema = cast(dict[str, object], parsed)
    Draft202012Validator.check_schema(schema)
    return schema


def scenario_schema_v0() -> dict[str, object]:
    """Return a defensive copy of the frozen Scenario v0 JSON Schema."""
    return deepcopy(_scenario_schema_v0_cached())


def scenario_schema(schema_version: str) -> dict[str, object]:
    """Return the schema for an explicitly requested supported version."""
    if schema_version == SCENARIO_V0_SCHEMA_VERSION:
        return scenario_schema_v0()
    raise ScenarioValidationError([f"$.schema_version: unsupported version {schema_version!r}"])


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


def _duplicate_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _semantic_errors_v0(document: dict[str, object]) -> list[str]:
    errors: list[str] = []
    agent = cast(dict[str, object], document["agent"])
    allowed_tools = set(cast(list[str], agent["allowed_tools"]))
    capabilities = set(cast(list[str], agent["capabilities"]))
    if "tool_calling" not in capabilities:
        errors.append("$.agent.capabilities must contain 'tool_calling' when tools are allowed")

    faults = cast(list[dict[str, object]], document["faults"])
    fault_ids = [cast(str, fault["id"]) for fault in faults]
    for duplicate in _duplicate_values(fault_ids):
        errors.append(f"$.faults contains duplicate fault id {duplicate!r}")
    for index, fault in enumerate(faults):
        match = cast(dict[str, object], fault["match"])
        tool_id = cast(str, match["tool_id"])
        if tool_id not in allowed_tools:
            errors.append(
                f"$.faults[{index}].match.tool_id references tool {tool_id!r}, "
                "which is not in $.agent.allowed_tools"
            )

    outcomes = cast(list[dict[str, object]], document["expected_outcomes"])
    outcome_ids = [cast(str, outcome["id"]) for outcome in outcomes]
    for duplicate in _duplicate_values(outcome_ids):
        errors.append(f"$.expected_outcomes contains duplicate reference id {duplicate!r}")
    return errors


def _jcs_sort_key(value: object) -> bytes:
    return rfc8785.dumps(cast(JsonValue, value))


def _normalize_set_like_arrays_v0(document: dict[str, object]) -> None:
    metadata = cast(dict[str, object], document["metadata"])
    if "tags" in metadata:
        cast(list[str], metadata["tags"]).sort(key=_jcs_sort_key)

    agent = cast(dict[str, object], document["agent"])
    cast(list[str], agent["allowed_tools"]).sort(key=_jcs_sort_key)
    cast(list[str], agent["capabilities"]).sort(key=_jcs_sort_key)

    cast(list[dict[str, object]], document["faults"]).sort(key=_jcs_sort_key)
    cast(list[dict[str, object]], document["expected_outcomes"]).sort(key=_jcs_sort_key)


def _snapshot(document: object) -> object:
    try:
        return deepcopy(document)
    except Exception as error:
        raise ScenarioValidationError([f"$: could not snapshot input document: {error}"]) from error


def _canonicalize_snapshot_v0(snapshot: object) -> bytes:
    schema = _scenario_schema_v0_cached()
    validator = Draft202012Validator(schema)
    schema_errors = sorted(validator.iter_errors(snapshot), key=_error_sort_key)
    errors = [f"{_json_path(list(error.path))}: {error.message}" for error in schema_errors]
    if errors:
        raise ScenarioValidationError(errors)
    if not isinstance(snapshot, dict):  # Narrows the type after the schema root check.
        raise ScenarioValidationError(["$: must be an object"])

    document = cast(dict[str, object], snapshot)
    errors.extend(_semantic_errors_v0(document))
    if errors:
        raise ScenarioValidationError(errors)

    try:
        _normalize_set_like_arrays_v0(document)
        return rfc8785.dumps(cast(JsonValue, document))
    except rfc8785.CanonicalizationError as error:
        raise ScenarioValidationError(
            [f"$: cannot be represented as RFC 8785 canonical JSON: {error}"]
        ) from error


def canonicalize_scenario_v0(document: object) -> bytes:
    """Snapshot, validate, semantically normalize, and JCS-serialize Scenario v0."""
    return _canonicalize_snapshot_v0(_snapshot(document))


def validate_scenario_v0(document: object) -> None:
    """Validate a Scenario v0 document, including its in-document references."""
    canonicalize_scenario_v0(document)


def digest_scenario_v0(document: object) -> str:
    """Return the digest of the validated Scenario v0 semantic canonical form."""
    return _digest_bytes(canonicalize_scenario_v0(document))


def _schema_version(document: object) -> str:
    if not isinstance(document, dict):
        raise ScenarioValidationError(["$: must be an object containing schema_version"])
    version = cast(dict[object, object], document).get("schema_version")
    if not isinstance(version, str):
        raise ScenarioValidationError(["$.schema_version: must be a string"])
    return version


def canonicalize_scenario(document: object) -> bytes:
    """Dispatch canonicalization using the document's explicit schema version."""
    snapshot = _snapshot(document)
    version = _schema_version(snapshot)
    if version == SCENARIO_V0_SCHEMA_VERSION:
        return _canonicalize_snapshot_v0(snapshot)
    raise ScenarioValidationError([f"$.schema_version: unsupported version {version!r}"])


def validate_scenario(document: object) -> None:
    """Validate a scenario using its explicit schema-version implementation."""
    canonicalize_scenario(document)


def digest_scenario(document: object) -> str:
    """Return a scenario digest using its explicit schema-version implementation."""
    return _digest_bytes(canonicalize_scenario(document))


def _digest_bytes(canonical_bytes: bytes) -> str:
    return f"sha256:{hashlib.sha256(canonical_bytes).hexdigest()}"


def _scenario_from_canonical_bytes(canonical_bytes: bytes) -> Scenario:
    scenario = object.__new__(Scenario)
    object.__setattr__(scenario, "canonical_bytes", canonical_bytes)
    object.__setattr__(scenario, "digest", _digest_bytes(canonical_bytes))
    return scenario


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ScenarioValidationError([f"$: duplicate JSON object key {key!r}"])
        result[key] = value
    return result


def _reject_nonfinite_number(value: str) -> object:
    raise ScenarioValidationError([f"$: non-finite JSON number {value!r} is not permitted"])


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
        raise ScenarioValidationError([f"$: malformed JSON: {error}"]) from error


def loads_scenario_v0(data: str | bytes) -> Scenario:
    """Parse and load an explicitly versioned Scenario v0 JSON document."""
    canonical = canonicalize_scenario_v0(_parse_json(data))
    return _scenario_from_canonical_bytes(canonical)


def load_scenario_v0(path: str | Path) -> Scenario:
    """Read and load an explicitly versioned Scenario v0 UTF-8 JSON file."""
    return loads_scenario_v0(_read_scenario_file(path))


def loads_scenario(data: str | bytes) -> Scenario:
    """Parse and dispatch a JSON scenario using its schema_version."""
    canonical = canonicalize_scenario(_parse_json(data))
    return _scenario_from_canonical_bytes(canonical)


def load_scenario(path: str | Path) -> Scenario:
    """Read and dispatch a UTF-8 JSON scenario file using its schema_version."""
    return loads_scenario(_read_scenario_file(path))


def _read_scenario_file(path: str | Path) -> bytes:
    scenario_path = Path(path)
    try:
        return scenario_path.read_bytes()
    except OSError as error:
        raise ScenarioValidationError([f"$: cannot read {scenario_path}: {error}"]) from error
