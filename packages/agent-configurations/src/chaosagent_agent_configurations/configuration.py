"""Strict Agent Configuration v0 validation and RFC 8785 identity."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from typing import cast

import rfc8785
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

AGENT_CONFIGURATION_V0_SCHEMA_VERSION = "chaosagent.agent-configuration/v0"


class AgentConfigurationValidationError(ValueError):
    """The configuration is malformed, unsupported, or non-canonicalizable."""


@dataclass(frozen=True, slots=True, init=False)
class AgentConfiguration:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("AgentConfiguration instances require validated loading")

    def to_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):
            raise AssertionError("validated Agent Configuration root is not an object")
        return cast(dict[str, object], value)


@lru_cache(maxsize=1)
def _schema() -> dict[str, object]:
    parsed = json.loads(
        files("chaosagent_agent_configurations.schema")
        .joinpath("agent-configuration-v0.schema.json")
        .read_text("utf-8")
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("Agent Configuration v0 schema is not an object")
    schema = cast(dict[str, object], parsed)
    Draft202012Validator.check_schema(schema)
    return schema


def agent_configuration_schema_v0() -> dict[str, object]:
    return deepcopy(_schema())


def canonicalize_agent_configuration(document: object) -> bytes:
    try:
        snapshot = deepcopy(document)
    except Exception as error:
        raise AgentConfigurationValidationError("configuration cannot be snapshotted") from error
    if not isinstance(snapshot, dict):
        raise AgentConfigurationValidationError("configuration must be an object")
    version = snapshot.get("schema_version")
    if version != AGENT_CONFIGURATION_V0_SCHEMA_VERSION:
        raise AgentConfigurationValidationError(f"unsupported schema_version {version!r}")
    errors = sorted(
        Draft202012Validator(_schema()).iter_errors(snapshot),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        validation_error = errors[0]
        path = "$" + "".join(f"[{part!r}]" for part in validation_error.absolute_path)
        raise AgentConfigurationValidationError(
            f"invalid configuration at {path}: {validation_error.message}"
        )
    accounting = cast(dict[str, object], snapshot["token_accounting"])
    if accounting["model"] != snapshot["model"]:
        raise AgentConfigurationValidationError(
            "token_accounting model must equal the configured model snapshot"
        )
    try:
        return rfc8785.dumps(snapshot)
    except rfc8785.CanonicalizationError as error:
        raise AgentConfigurationValidationError("configuration is not RFC 8785 JSON") from error


def digest_agent_configuration(document: object) -> str:
    return "sha256:" + hashlib.sha256(canonicalize_agent_configuration(document)).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AgentConfigurationValidationError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def loads_agent_configuration(data: str | bytes) -> AgentConfiguration:
    try:
        document = json.loads(data, object_pairs_hook=_reject_duplicate_keys)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise AgentConfigurationValidationError("malformed Agent Configuration JSON") from error
    canonical = canonicalize_agent_configuration(document)
    configuration = object.__new__(AgentConfiguration)
    object.__setattr__(configuration, "canonical_bytes", canonical)
    object.__setattr__(configuration, "digest", "sha256:" + hashlib.sha256(canonical).hexdigest())
    return configuration
