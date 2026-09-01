"""Frozen Ground Truth and deterministic Evaluation Result v0 contracts."""

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

GROUND_TRUTH_V0_SCHEMA_VERSION = "chaosagent.ground-truth/v0"
EVALUATION_RESULT_V0_SCHEMA_VERSION = "chaosagent.evaluation-result/v0"


class EvaluatorValidationError(ValueError):
    """A versioned evaluator contract is malformed or semantically inconsistent."""

    def __init__(self, contract: str, errors: list[str]) -> None:
        self.contract = contract
        self.errors = tuple(errors)
        super().__init__(f"Invalid ChaosAgent {contract}:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True, init=False)
class GroundTruth:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("GroundTruth instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        return _object(self.canonical_bytes, "ground truth")


@dataclass(frozen=True, slots=True, init=False)
class EvaluationResult:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("EvaluationResult instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        return _object(self.canonical_bytes, "evaluation result")


def _object(data: bytes, contract: str) -> dict[str, object]:
    value = cast(object, json.loads(data))
    if not isinstance(value, dict):
        raise AssertionError(f"canonical {contract} root is not an object")
    return cast(dict[str, object], value)


@lru_cache(maxsize=2)
def _schema(filename: str) -> dict[str, object]:
    value = cast(
        object,
        json.loads(files("chaosagent_evaluators.schema").joinpath(filename).read_text("utf-8")),
    )
    if not isinstance(value, dict):
        raise RuntimeError(f"bundled evaluator schema {filename!r} is not an object")
    result = cast(dict[str, object], value)
    Draft202012Validator.check_schema(result)
    return result


def ground_truth_schema_v0() -> dict[str, object]:
    return deepcopy(_schema("ground-truth-v0.schema.json"))


def evaluation_result_schema_v0() -> dict[str, object]:
    return deepcopy(_schema("evaluation-result-v0.schema.json"))


def ground_truth_schema(schema_version: str) -> dict[str, object]:
    if schema_version == GROUND_TRUTH_V0_SCHEMA_VERSION:
        return ground_truth_schema_v0()
    raise EvaluatorValidationError(
        "ground truth", [f"$.schema_version: unsupported version {schema_version!r}"]
    )


def evaluation_result_schema(schema_version: str) -> dict[str, object]:
    if schema_version == EVALUATION_RESULT_V0_SCHEMA_VERSION:
        return evaluation_result_schema_v0()
    raise EvaluatorValidationError(
        "evaluation result", [f"$.schema_version: unsupported version {schema_version!r}"]
    )


def _path(parts: object) -> str:
    values = list(cast(Iterable[object], parts))
    return "$" + "".join(
        f"[{value}]" if isinstance(value, int) else f".{value}" for value in values
    )


def _validate_schema(document: object, filename: str, contract: str) -> dict[str, object]:
    errors = sorted(
        Draft202012Validator(_schema(filename)).iter_errors(document),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    if errors:
        raise EvaluatorValidationError(
            contract, [f"{_path(error.absolute_path)}: {error.message}" for error in errors]
        )
    if not isinstance(document, dict):
        raise EvaluatorValidationError(contract, ["$: must be an object"])
    return cast(dict[str, object], document)


def _canonical(document: object, filename: str, contract: str) -> bytes:
    try:
        snapshot = deepcopy(document)
    except Exception as error:
        raise EvaluatorValidationError(contract, ["$: could not snapshot input"]) from error
    value = _validate_schema(snapshot, filename, contract)
    if contract == "ground truth":
        gates = cast(list[dict[str, object]], value["critical_gates"])
        identifiers = [cast(str, gate["gate_id"]) for gate in gates]
        duplicates = sorted({item for item in identifiers if identifiers.count(item) > 1})
        if duplicates:
            raise EvaluatorValidationError(
                contract,
                [f"$.critical_gates contains duplicate gate_id {item!r}" for item in duplicates],
            )
        for gate in gates:
            if gate["kind"] in {"refund_state", "effect_count"} and cast(
                int, gate["min_count"]
            ) > cast(int, gate["max_count"]):
                raise EvaluatorValidationError(
                    contract,
                    [f"$.critical_gates[{gate['gate_id']!r}] min_count exceeds max_count"],
                )
            for field in ("effect_kinds", "fault_ids"):
                if field in gate:
                    cast(list[str], gate[field]).sort()
        gates.sort(key=lambda gate: cast(str, gate["gate_id"]))
    else:
        gates = cast(list[dict[str, object]], value["critical_gates"])
        identifiers = [cast(str, gate["gate_id"]) for gate in gates]
        if len(set(identifiers)) != len(identifiers):
            raise EvaluatorValidationError(
                contract, ["$.critical_gates contains duplicate gate_id"]
            )
        metrics = cast(list[dict[str, object]], value["diagnostic_metrics"])
        metric_ids = [cast(str, metric["metric_id"]) for metric in metrics]
        if len(set(metric_ids)) != len(metric_ids):
            raise EvaluatorValidationError(
                contract, ["$.diagnostic_metrics contains duplicate metric_id"]
            )
        evaluator = value["evaluator"]
        for collection, path in ((gates, "critical_gates"), (metrics, "diagnostic_metrics")):
            for item in collection:
                if path == "critical_gates" and item["evaluator"] != evaluator:
                    raise EvaluatorValidationError(
                        contract, ["$.critical_gates evaluator differs from the result evaluator"]
                    )
                references = cast(list[dict[str, object]], item["evidence"])
                identities = [
                    (cast(str, reference["event_id"]), cast(int, reference["sequence"]))
                    for reference in references
                ]
                if len({event_id for event_id, _ in identities}) != len(identities) or len(
                    {sequence for _, sequence in identities}
                ) != len(identities):
                    raise EvaluatorValidationError(
                        contract, [f"$.{path} contains duplicate evidence references"]
                    )
                if any(
                    sequence > cast(int, value["evidence_through_sequence"])
                    for _, sequence in identities
                ):
                    raise EvaluatorValidationError(
                        contract, [f"$.{path} cites evidence after the evaluation boundary"]
                    )
        classification = value["classification"]
        statuses = {gate["status"] for gate in gates}
        contradictory = (
            (classification == "pass" and statuses - {"pass"})
            or (classification == "fail" and ("fail" not in statuses or "error" in statuses))
            or (classification == "invalid" and "error" not in statuses and gates)
        )
        if contradictory:
            raise EvaluatorValidationError(contract, ["$.classification contradicts gate statuses"])
        for gate in gates:
            cast(list[dict[str, object]], gate["evidence"]).sort(
                key=lambda ref: (cast(int, ref["sequence"]), cast(str, ref["event_id"]))
            )
        for metric in metrics:
            cast(list[dict[str, object]], metric["evidence"]).sort(
                key=lambda ref: (cast(int, ref["sequence"]), cast(str, ref["event_id"]))
            )
        gates.sort(key=lambda gate: cast(str, gate["gate_id"]))
        metrics.sort(key=lambda metric: cast(str, metric["metric_id"]))
    try:
        return rfc8785.dumps(cast(JsonValue, value))
    except rfc8785.CanonicalizationError as error:
        raise EvaluatorValidationError(
            contract, ["$: cannot be represented as RFC 8785 JSON"]
        ) from error


def _digest(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def _instance(
    cls: type[GroundTruth] | type[EvaluationResult], data: bytes
) -> GroundTruth | EvaluationResult:
    value = object.__new__(cls)
    object.__setattr__(value, "canonical_bytes", data)
    object.__setattr__(value, "digest", _digest(data))
    return value


def canonicalize_ground_truth_v0(document: object) -> bytes:
    return _canonical(document, "ground-truth-v0.schema.json", "ground truth")


def canonicalize_evaluation_result_v0(document: object) -> bytes:
    return _canonical(document, "evaluation-result-v0.schema.json", "evaluation result")


def validate_ground_truth_v0(document: object) -> None:
    canonicalize_ground_truth_v0(document)


def validate_evaluation_result_v0(document: object) -> None:
    canonicalize_evaluation_result_v0(document)


def loads_ground_truth_v0(data: str | bytes) -> GroundTruth:
    parsed = _parse(data, "ground truth")
    return cast(GroundTruth, _instance(GroundTruth, canonicalize_ground_truth_v0(parsed)))


def load_ground_truth_v0(path: str | Path) -> GroundTruth:
    try:
        return loads_ground_truth_v0(Path(path).read_bytes())
    except OSError as error:
        raise EvaluatorValidationError("ground truth", ["$: cannot read input file"]) from error


def loads_evaluation_result_v0(data: str | bytes) -> EvaluationResult:
    parsed = _parse(data, "evaluation result")
    return cast(
        EvaluationResult,
        _instance(EvaluationResult, canonicalize_evaluation_result_v0(parsed)),
    )


def evaluation_result_v0(document: object) -> EvaluationResult:
    return cast(
        EvaluationResult,
        _instance(EvaluationResult, canonicalize_evaluation_result_v0(document)),
    )


def _version(document: object, contract: str) -> str:
    if not isinstance(document, dict):
        raise EvaluatorValidationError(contract, ["$: must be an object containing schema_version"])
    value = cast(dict[object, object], document).get("schema_version")
    if not isinstance(value, str):
        raise EvaluatorValidationError(contract, ["$.schema_version: must be a string"])
    return value


def loads_ground_truth(data: str | bytes) -> GroundTruth:
    parsed = _parse(data, "ground truth")
    version = _version(parsed, "ground truth")
    if version != GROUND_TRUTH_V0_SCHEMA_VERSION:
        ground_truth_schema(version)
    return cast(GroundTruth, _instance(GroundTruth, canonicalize_ground_truth_v0(parsed)))


def loads_evaluation_result(data: str | bytes) -> EvaluationResult:
    parsed = _parse(data, "evaluation result")
    version = _version(parsed, "evaluation result")
    if version != EVALUATION_RESULT_V0_SCHEMA_VERSION:
        evaluation_result_schema(version)
    return cast(
        EvaluationResult,
        _instance(EvaluationResult, canonicalize_evaluation_result_v0(parsed)),
    )


def validate_ground_truth(document: object) -> None:
    version = _version(document, "ground truth")
    if version != GROUND_TRUTH_V0_SCHEMA_VERSION:
        ground_truth_schema(version)
    validate_ground_truth_v0(document)


def validate_evaluation_result(document: object) -> None:
    version = _version(document, "evaluation result")
    if version != EVALUATION_RESULT_V0_SCHEMA_VERSION:
        evaluation_result_schema(version)
    validate_evaluation_result_v0(document)


def _duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvaluatorValidationError("JSON", [f"$: duplicate object key {key!r}"])
        result[key] = value
    return result


def _parse(data: str | bytes, contract: str) -> object:
    try:
        return cast(
            object,
            json.loads(
                data,
                object_pairs_hook=_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            ),
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raise EvaluatorValidationError(contract, ["$: malformed JSON"]) from error
