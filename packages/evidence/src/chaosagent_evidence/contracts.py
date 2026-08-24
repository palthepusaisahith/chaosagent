"""Validated, immutable wrappers for versioned run evidence contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import cast

import rfc8785
from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]

RUN_EVENT_V0_SCHEMA_VERSION = "chaosagent.run-event/v0"
RUN_REPORT_V0_SCHEMA_VERSION = "chaosagent.run-report/v0"
_EVENT_SCHEMA_FILENAME = "run-event-v0.schema.json"
_REPORT_SCHEMA_FILENAME = "run-report-v0.schema.json"


class EvidenceValidationError(ValueError):
    """Raised when an event, report, or event stream violates its contract."""

    def __init__(self, contract: str, errors: list[str]) -> None:
        self.contract = contract
        self.errors = tuple(errors)
        super().__init__(f"Invalid ChaosAgent {contract}:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True, init=False)
class RunEvent:
    """Immutable canonical bytes produced only by a validated event loader."""

    canonical_bytes: bytes

    def __init__(self) -> None:
        raise TypeError("RunEvent instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        return _canonical_object(self.canonical_bytes, "run event")


@dataclass(frozen=True, slots=True, init=False)
class RunReport:
    """Immutable canonical bytes produced only by a validated report loader."""

    canonical_bytes: bytes

    def __init__(self) -> None:
        raise TypeError("RunReport instances must be created by a validated loader")

    def to_dict(self) -> dict[str, object]:
        return _canonical_object(self.canonical_bytes, "run report")


def _canonical_object(data: bytes, contract: str) -> dict[str, object]:
    parsed = cast(object, json.loads(data))
    if not isinstance(parsed, dict):
        raise AssertionError(f"canonical {contract} root is not an object")
    return cast(dict[str, object], parsed)


@lru_cache(maxsize=2)
def _schema_cached(filename: str) -> dict[str, object]:
    resource = files("chaosagent_evidence.schema").joinpath(filename)
    parsed = cast(object, json.loads(resource.read_text(encoding="utf-8")))
    if not isinstance(parsed, dict):
        raise RuntimeError(f"bundled schema {filename} must be a JSON object")
    schema = cast(dict[str, object], parsed)
    Draft202012Validator.check_schema(schema)
    return schema


def run_event_schema_v0() -> dict[str, object]:
    """Return a defensive copy of the frozen Run Event v0 JSON Schema."""
    return deepcopy(_schema_cached(_EVENT_SCHEMA_FILENAME))


def run_report_schema_v0() -> dict[str, object]:
    """Return a defensive copy of the frozen Run Report v0 JSON Schema."""
    return deepcopy(_schema_cached(_REPORT_SCHEMA_FILENAME))


def run_event_schema(schema_version: str) -> dict[str, object]:
    if schema_version == RUN_EVENT_V0_SCHEMA_VERSION:
        return run_event_schema_v0()
    raise EvidenceValidationError(
        "run event", [f"$.schema_version: unsupported version {schema_version!r}"]
    )


def run_report_schema(schema_version: str) -> dict[str, object]:
    if schema_version == RUN_REPORT_V0_SCHEMA_VERSION:
        return run_report_schema_v0()
    raise EvidenceValidationError(
        "run report", [f"$.schema_version: unsupported version {schema_version!r}"]
    )


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


def _snapshot(document: object, contract: str) -> object:
    try:
        return deepcopy(document)
    except Exception as error:
        raise EvidenceValidationError(
            contract, [f"$: could not snapshot input document: {error}"]
        ) from error


def _schema_errors(snapshot: object, filename: str) -> list[str]:
    validator = Draft202012Validator(_schema_cached(filename), format_checker=FormatChecker())
    failures = sorted(
        (
            leaf
            for failure in validator.iter_errors(snapshot)
            for leaf in _validation_error_leaves(failure)
        ),
        key=_error_sort_key,
    )
    return [
        f"{_json_path(list(cast(Iterable[object], getattr(error, 'path'))))}: "
        f"{getattr(error, 'message')}"
        for error in failures
    ]


def _validation_error_leaves(error: object) -> Iterable[object]:
    context = list(cast(Iterable[object], cast(object, getattr(error, "context"))))
    if not context:
        yield error
        return
    for child in context:
        yield from _validation_error_leaves(child)


def _jcs(document: object, contract: str) -> bytes:
    try:
        return rfc8785.dumps(cast(JsonValue, document))
    except rfc8785.CanonicalizationError as error:
        raise EvidenceValidationError(
            contract, [f"$: cannot be represented as RFC 8785 canonical JSON: {error}"]
        ) from error


def _digest_bytes(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def digest_payload_v0(payload: object) -> str:
    """Return the RFC 8785 SHA-256 digest used by Run Event v0 payloads."""
    snapshot = _snapshot(payload, "event payload")
    _normalize_payload_set_arrays_v0(snapshot)
    return _digest_bytes(_jcs(snapshot, "event payload"))


def _normalize_payload_set_arrays_v0(payload: object) -> None:
    if not isinstance(payload, dict):
        return
    related = cast(dict[object, object], payload).get("related_event_ids")
    if isinstance(related, list) and all(isinstance(item, str) for item in related):
        cast(list[str], related).sort()


def _canonicalize_event_snapshot_v0(snapshot: object) -> bytes:
    errors = _schema_errors(snapshot, _EVENT_SCHEMA_FILENAME)
    if errors:
        raise EvidenceValidationError("run event", errors)
    if not isinstance(snapshot, dict):
        raise EvidenceValidationError("run event", ["$: must be an object"])
    document = cast(dict[str, object], snapshot)
    _normalize_payload_set_arrays_v0(document["payload"])
    expected = digest_payload_v0(document["payload"])
    actual = cast(str, document["payload_digest"])
    if actual != expected:
        raise EvidenceValidationError(
            "run event", [f"$.payload_digest: does not match payload; expected {expected!r}"]
        )
    return _jcs(document, "run event")


def canonicalize_run_event_v0(document: object) -> bytes:
    """Snapshot, validate, and JCS-serialize a Run Event v0 document."""
    return _canonicalize_event_snapshot_v0(_snapshot(document, "run event"))


def validate_run_event_v0(document: object) -> None:
    canonicalize_run_event_v0(document)


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    repeated: set[str] = set()
    for value in values:
        if value in seen:
            repeated.add(value)
        seen.add(value)
    return sorted(repeated)


def validate_run_event_stream_v0(events: Sequence[object], *, complete: bool = False) -> None:
    """Validate one ordered run stream; sequence gaps are permitted in v0."""
    if not events:
        raise EvidenceValidationError("run event stream", ["$: must contain at least one event"])
    documents: list[dict[str, object]] = []
    for index, event in enumerate(events):
        try:
            canonical = canonicalize_run_event_v0(event)
        except EvidenceValidationError as error:
            raise EvidenceValidationError(
                "run event stream", [f"$[{index}]{item[1:]}" for item in error.errors]
            ) from error
        documents.append(_canonical_object(canonical, "run event"))

    errors: list[str] = []
    run_ids = [cast(str, event["run_id"]) for event in documents]
    if len(set(run_ids)) != 1:
        errors.append("$: all events must have the same run_id")
    for duplicate in _duplicates([cast(str, event["event_id"]) for event in documents]):
        errors.append(f"$: duplicate event_id {duplicate!r}")
    sequences = [cast(int, event["sequence"]) for event in documents]
    if complete and sequences[0] != 1:
        errors.append("$[0].sequence: a complete run stream must begin at sequence 1")
    for index in range(1, len(sequences)):
        if sequences[index] <= sequences[index - 1]:
            errors.append(
                f"$[{index}].sequence: must be greater than the preceding sequence "
                f"{sequences[index - 1]}"
            )

    event_by_id = {cast(str, event["event_id"]): event for event in documents}
    request_by_id: dict[str, dict[str, object]] = {}
    request_attempt_ids: list[str] = []
    result_attempt_ids: list[str] = []
    for index, event in enumerate(documents):
        event_type = cast(str, event["event_type"])
        payload = cast(dict[str, object], event["payload"])
        if event_type == "tool.requested":
            request_by_id[cast(str, event["event_id"])] = event
            request_attempt_ids.append(cast(str, payload["attempt_id"]))
        elif event_type == "tool.result":
            result_attempt_ids.append(cast(str, payload["attempt_id"]))
            request_id = cast(str, payload["request_event_id"])
            request = request_by_id.get(request_id)
            if request is None:
                errors.append(
                    f"$[{index}].payload.request_event_id references no preceding tool request"
                )
            else:
                request_payload = cast(dict[str, object], request["payload"])
                for field in ("logical_call_id", "attempt_id", "attempt_number", "tool_id"):
                    if payload[field] != request_payload[field]:
                        errors.append(
                            f"$[{index}].payload.{field} does not match "
                            f"request event {request_id!r}"
                        )

        if complete:
            reference_ids: list[str] = []
            causation = event.get("causation_event_id")
            if isinstance(causation, str):
                reference_ids.append(causation)
            related = payload.get("related_event_ids")
            if isinstance(related, list):
                reference_ids.extend(cast(list[str], related))
            for reference_id in reference_ids:
                referenced = event_by_id.get(reference_id)
                if referenced is None:
                    errors.append(f"$[{index}] references unknown event_id {reference_id!r}")
                elif cast(int, referenced["sequence"]) >= cast(int, event["sequence"]):
                    errors.append(f"$[{index}] references non-preceding event_id {reference_id!r}")

    for duplicate in _duplicates(request_attempt_ids):
        errors.append(f"$: duplicate tool request attempt_id {duplicate!r}")
    for duplicate in _duplicates(result_attempt_ids):
        errors.append(f"$: duplicate tool result attempt_id {duplicate!r}")
    if errors:
        raise EvidenceValidationError("run event stream", errors)


def _evidence_refs(report: dict[str, object]) -> Iterable[dict[str, object]]:
    fault = cast(dict[str, object], report["fault_observation"])
    for reference in cast(list[dict[str, object]], fault["evidence"]):
        yield reference
    for gate in cast(list[dict[str, object]], report["critical_gates"]):
        for reference in cast(list[dict[str, object]], gate["evidence"]):
            yield reference
    for metric in cast(list[dict[str, object]], report["diagnostic_metrics"]):
        for reference in cast(list[dict[str, object]], metric["evidence"]):
            yield reference


def _report_semantic_errors_v0(report: dict[str, object]) -> list[str]:
    errors: list[str] = []
    boundary = cast(dict[str, object], report["evidence_boundary"])
    first = cast(int, boundary["first_sequence"])
    last = cast(int, boundary["last_sequence"])
    count = cast(int, boundary["event_count"])
    if last < first:
        errors.append("$.evidence_boundary.last_sequence must be >= first_sequence")
    elif count > last - first + 1:
        errors.append("$.evidence_boundary.event_count exceeds the inclusive sequence range")

    provenance = cast(dict[str, object], report["provenance"])
    evaluated_through = provenance.get("evaluated_through_sequence")
    if isinstance(evaluated_through, int) and not first <= evaluated_through <= last:
        errors.append("$.provenance.evaluated_through_sequence is outside the evidence boundary")

    for reference in _evidence_refs(report):
        kind = cast(str, reference["kind"])
        if kind == "event":
            sequence = cast(int, reference["sequence"])
            if not first <= sequence <= last:
                errors.append(f"$: event evidence sequence {sequence} is outside the boundary")
        elif kind == "event_range":
            start = cast(int, reference["start_sequence"])
            end = cast(int, reference["end_sequence"])
            if start > end:
                errors.append(f"$: event evidence range {start}..{end} is reversed")
            elif start < first or end > last:
                errors.append(f"$: event evidence range {start}..{end} is outside the boundary")

    gates = cast(list[dict[str, object]], report["critical_gates"])
    for duplicate in _duplicates([cast(str, gate["gate_id"]) for gate in gates]):
        errors.append(f"$.critical_gates contains duplicate gate_id {duplicate!r}")
    metrics = cast(list[dict[str, object]], report["diagnostic_metrics"])
    for duplicate in _duplicates([cast(str, metric["metric_id"]) for metric in metrics]):
        errors.append(f"$.diagnostic_metrics contains duplicate metric_id {duplicate!r}")
    evaluators = cast(list[dict[str, object]], provenance["evaluator_revisions"])
    evaluator_keys = [f"{item['id']}\0{item['revision']}\0{item['digest']}" for item in evaluators]
    for duplicate in _duplicates(evaluator_keys):
        evaluator_id = duplicate.split("\0", maxsplit=1)[0]
        errors.append(f"$.provenance.evaluator_revisions contains duplicate {evaluator_id!r}")
    evaluator_key_set = set(evaluator_keys)
    for index, gate in enumerate(gates):
        evaluator = cast(dict[str, object], gate["evaluator"])
        key = f"{evaluator['id']}\0{evaluator['revision']}\0{evaluator['digest']}"
        if key not in evaluator_key_set:
            errors.append(
                f"$.critical_gates[{index}].evaluator is not listed in "
                "$.provenance.evaluator_revisions"
            )
    return errors


def _canonicalize_report_snapshot_v0(snapshot: object) -> bytes:
    errors = _schema_errors(snapshot, _REPORT_SCHEMA_FILENAME)
    if errors:
        raise EvidenceValidationError("run report", errors)
    if not isinstance(snapshot, dict):
        raise EvidenceValidationError("run report", ["$: must be an object"])
    report = cast(dict[str, object], snapshot)
    errors = _report_semantic_errors_v0(report)
    if errors:
        raise EvidenceValidationError("run report", errors)
    return _jcs(report, "run report")


def canonicalize_run_report_v0(document: object) -> bytes:
    """Snapshot, validate, and JCS-serialize a Run Report v0 document."""
    return _canonicalize_report_snapshot_v0(_snapshot(document, "run report"))


def validate_run_report_v0(document: object) -> None:
    canonicalize_run_report_v0(document)


def validate_run_report_with_events_v0(report: object, events: Sequence[object]) -> None:
    """Validate a final report against its complete, dependency-closed event stream."""
    report_document = _canonical_object(canonicalize_run_report_v0(report), "run report")
    validate_run_event_stream_v0(events, complete=True)
    event_documents = [
        _canonical_object(canonicalize_run_event_v0(event), "run event") for event in events
    ]

    errors: list[str] = []
    report_run_id = cast(str, report_document["run_id"])
    if any(cast(str, event["run_id"]) != report_run_id for event in event_documents):
        errors.append("$.run_id does not match every supplied event")

    boundary = cast(dict[str, object], report_document["evidence_boundary"])
    sequences = [cast(int, event["sequence"]) for event in event_documents]
    if sequences[0] != cast(int, boundary["first_sequence"]):
        errors.append("$.evidence_boundary.first_sequence does not match the supplied stream")
    if sequences[-1] != cast(int, boundary["last_sequence"]):
        errors.append("$.evidence_boundary.last_sequence does not match the supplied stream")
    if len(event_documents) != cast(int, boundary["event_count"]):
        errors.append("$.evidence_boundary.event_count does not match the supplied stream")

    event_by_id = {cast(str, event["event_id"]): event for event in event_documents}
    sequence_set = set(sequences)
    for reference in _evidence_refs(report_document):
        kind = cast(str, reference["kind"])
        if kind == "event":
            event_id = cast(str, reference["event_id"])
            referenced = event_by_id.get(event_id)
            if referenced is None:
                errors.append(f"$: event evidence references unknown event_id {event_id!r}")
            elif cast(int, referenced["sequence"]) != cast(int, reference["sequence"]):
                errors.append(f"$: event evidence sequence does not match event_id {event_id!r}")
        elif kind == "event_range":
            start = cast(int, reference["start_sequence"])
            end = cast(int, reference["end_sequence"])
            if not any(start <= sequence <= end for sequence in sequence_set):
                errors.append(f"$: event evidence range {start}..{end} contains no supplied event")
    if errors:
        raise EvidenceValidationError("run report and event stream", errors)


def _schema_version(document: object, contract: str) -> str:
    if not isinstance(document, dict):
        raise EvidenceValidationError(contract, ["$: must be an object containing schema_version"])
    version = cast(dict[object, object], document).get("schema_version")
    if not isinstance(version, str):
        raise EvidenceValidationError(contract, ["$.schema_version: must be a string"])
    return version


def canonicalize_run_event(document: object) -> bytes:
    snapshot = _snapshot(document, "run event")
    version = _schema_version(snapshot, "run event")
    if version == RUN_EVENT_V0_SCHEMA_VERSION:
        return _canonicalize_event_snapshot_v0(snapshot)
    raise EvidenceValidationError(
        "run event", [f"$.schema_version: unsupported version {version!r}"]
    )


def canonicalize_run_report(document: object) -> bytes:
    snapshot = _snapshot(document, "run report")
    version = _schema_version(snapshot, "run report")
    if version == RUN_REPORT_V0_SCHEMA_VERSION:
        return _canonicalize_report_snapshot_v0(snapshot)
    raise EvidenceValidationError(
        "run report", [f"$.schema_version: unsupported version {version!r}"]
    )


def validate_run_event(document: object) -> None:
    canonicalize_run_event(document)


def validate_run_report(document: object) -> None:
    canonicalize_run_report(document)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceValidationError(
                "JSON document", [f"$: duplicate JSON object key {key!r}"]
            )
        result[key] = value
    return result


def _reject_nonfinite_number(value: str) -> object:
    raise EvidenceValidationError(
        "JSON document", [f"$: non-finite JSON number {value!r} is not permitted"]
    )


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
        raise EvidenceValidationError("JSON document", [f"$: malformed JSON: {error}"]) from error


def _event_from_bytes(canonical: bytes) -> RunEvent:
    event = object.__new__(RunEvent)
    object.__setattr__(event, "canonical_bytes", canonical)
    return event


def _report_from_bytes(canonical: bytes) -> RunReport:
    report = object.__new__(RunReport)
    object.__setattr__(report, "canonical_bytes", canonical)
    return report


def loads_run_event_v0(data: str | bytes) -> RunEvent:
    return _event_from_bytes(canonicalize_run_event_v0(_parse_json(data)))


def loads_run_event(data: str | bytes) -> RunEvent:
    return _event_from_bytes(canonicalize_run_event(_parse_json(data)))


def loads_run_report_v0(data: str | bytes) -> RunReport:
    return _report_from_bytes(canonicalize_run_report_v0(_parse_json(data)))


def loads_run_report(data: str | bytes) -> RunReport:
    return _report_from_bytes(canonicalize_run_report(_parse_json(data)))


def _read(path: str | Path, contract: str) -> bytes:
    source = Path(path)
    try:
        return source.read_bytes()
    except OSError as error:
        raise EvidenceValidationError(contract, [f"$: cannot read {source}: {error}"]) from error


def load_run_event_v0(path: str | Path) -> RunEvent:
    return loads_run_event_v0(_read(path, "run event"))


def load_run_event(path: str | Path) -> RunEvent:
    return loads_run_event(_read(path, "run event"))


def load_run_report_v0(path: str | Path) -> RunReport:
    return loads_run_report_v0(_read(path, "run report"))


def load_run_report(path: str | Path) -> RunReport:
    return loads_run_report(_read(path, "run report"))
