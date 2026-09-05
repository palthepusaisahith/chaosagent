"""Deterministic bundle construction and PostgreSQL snapshot export."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

import rfc8785
from chaosagent_agent_configurations import AgentConfiguration, loads_agent_configuration
from chaosagent_evaluators import (
    EvaluationResult,
    GroundTruth,
    aggregate_campaign_v0,
    authenticated_campaign_plan,
    authenticated_campaign_trial,
    campaign_cohort_v0,
    loads_campaign_statistics_v0,
    loads_evaluation_result,
    loads_ground_truth,
)
from chaosagent_evaluators.service import load_authoritative_evaluation_snapshot
from chaosagent_evidence import (
    RunEvent,
    RunReport,
    digest_payload_v0,
    loads_run_event,
    loads_run_report,
)
from chaosagent_faults import compile_fault_plan_v0
from chaosagent_persistence import (
    PersistenceError,
    PersistenceRepository,
    RevisionReference,
    RunRecord,
)
from chaosagent_scenarios import Scenario, loads_scenario
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .contracts import (
    BUNDLE_FORMAT_V0,
    ExportBundle,
    ExportIntegrityError,
    expected_role_metadata_v0,
    export_manifest_v0,
)

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type Role = Literal[
    "scenario",
    "agent_configuration",
    "run_events",
    "run_report",
    "evaluation_results",
    "ground_truths",
    "campaign_plan",
    "campaign_statistics",
]

_TERMINAL = frozenset({"completed", "failed", "timed_out", "cancelled", "infra_error"})
_PROTECTED_POINTER_PARTS = frozenset(
    {
        "schema_version",
        "id",
        "run_id",
        "event_id",
        "report_id",
        "evaluation_id",
        "scenario_id",
        "agent_configuration_id",
        "revision",
        "digest",
        "payload_digest",
        "input_digest",
        "sequence",
        "evidence_through_sequence",
    }
)


@dataclass(frozen=True, slots=True)
class RedactionRule:
    role: Role
    json_pointer: str


@dataclass(frozen=True, slots=True)
class ApplicationProvenance:
    version: str
    repository_commit: str | None = None
    repository_dirty: bool | None = None


@dataclass(frozen=True, slots=True)
class _RunMaterial:
    run: RunRecord
    scenario: Scenario
    agent_configuration: AgentConfiguration | None
    events: tuple[RunEvent, ...]
    report: RunReport | None
    evaluations: tuple[EvaluationResult, ...]
    ground_truths: tuple[GroundTruth, ...]
    selected_fault_ids: tuple[str, ...]


def _sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _timestamp(value: datetime | None) -> str:
    current = datetime.now(UTC) if value is None else value
    if current.tzinfo is None or current.utcoffset() is None:
        raise ExportIntegrityError("exported_at must be timezone-aware")
    return current.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _ground_truths(values: Iterable[GroundTruth]) -> tuple[GroundTruth, ...]:
    try:
        snapshot = tuple(values)
    except Exception as error:
        raise ExportIntegrityError("ground_truths could not be read") from error
    if any(not isinstance(value, GroundTruth) for value in snapshot):
        raise ExportIntegrityError("ground_truths must contain validated Ground Truth values")
    return snapshot


def _redaction_rules(values: Iterable[RedactionRule]) -> tuple[RedactionRule, ...]:
    try:
        snapshot = tuple(values)
    except Exception as error:
        raise ExportIntegrityError("redaction_rules could not be read") from error
    if any(not isinstance(value, RedactionRule) for value in snapshot):
        raise ExportIntegrityError("redaction_rules must contain RedactionRule values")
    ordered = tuple(sorted(snapshot, key=lambda item: (item.role, item.json_pointer)))
    if len({(item.role, item.json_pointer) for item in ordered}) != len(ordered):
        raise ExportIntegrityError("redaction rules must be unique")
    return ordered


def _k_values(values: Iterable[int]) -> tuple[int, ...]:
    try:
        snapshot = tuple(values)
    except Exception as error:
        raise ExportIntegrityError("k_values could not be read") from error
    if any(not isinstance(value, int) or isinstance(value, bool) for value in snapshot):
        raise ExportIntegrityError("k_values must contain integers")
    return snapshot


def _export_request(
    engine: Engine,
    identifier: str,
    exported_at: datetime | None,
    application: ApplicationProvenance | None,
) -> None:
    if not isinstance(engine, Engine):
        raise ExportIntegrityError("export requires a SQLAlchemy Engine")
    if not isinstance(identifier, str) or not identifier:
        raise ExportIntegrityError("export identity must be a non-empty string")
    if exported_at is not None and not isinstance(exported_at, datetime):
        raise ExportIntegrityError("exported_at must be a datetime")
    if application is not None and not isinstance(application, ApplicationProvenance):
        raise ExportIntegrityError("application must be ApplicationProvenance")


def _revision(reference: RevisionReference) -> dict[str, object]:
    return {"id": reference.id, "revision": reference.revision, "digest": reference.digest}


def _run_prefix(run_id: str) -> str:
    return "runs/" + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]


def _pointer_parts(pointer: str) -> tuple[str, ...]:
    if not isinstance(pointer, str) or not pointer.startswith("/") or len(pointer) > 512:
        raise ExportIntegrityError("redaction rules require a bounded JSON Pointer")
    parts: list[str] = []
    for raw_part in pointer[1:].split("/"):
        decoded: list[str] = []
        index = 0
        while index < len(raw_part):
            character = raw_part[index]
            if character != "~":
                decoded.append(character)
                index += 1
                continue
            if index + 1 >= len(raw_part) or raw_part[index + 1] not in {"0", "1"}:
                raise ExportIntegrityError("redaction JSON Pointer is malformed")
            decoded.append("~" if raw_part[index + 1] == "0" else "/")
            index += 2
        parts.append("".join(decoded))
    result = tuple(parts)
    if not result:
        raise ExportIntegrityError("redaction JSON Pointer is malformed")
    if any(
        part in _PROTECTED_POINTER_PARTS or part.endswith("_id") or "digest" in part
        for part in result
    ):
        raise ExportIntegrityError(
            "redaction cannot alter identity, digest, or evidence-boundary fields"
        )
    return result


def _array_index(token: str, length: int) -> int:
    if token == "0":
        index = 0
    elif re.fullmatch(r"[1-9][0-9]*", token):
        index = int(token)
    else:
        raise ExportIntegrityError("redaction JSON Pointer has an invalid array index")
    if index >= length:
        raise ExportIntegrityError("redaction JSON Pointer array index is out of range")
    return index


def _redact(
    document: dict[str, object], pointers: Sequence[str]
) -> tuple[dict[str, object], set[str]]:
    value = deepcopy(document)
    applied: set[str] = set()
    for pointer in pointers:
        parts = _pointer_parts(pointer)
        current: object = value
        try:
            for part in parts[:-1]:
                if isinstance(current, dict):
                    current = cast(dict[str, object], current)[part]
                elif isinstance(current, list):
                    current = current[_array_index(part, len(current))]
                else:
                    raise KeyError(part)
            leaf = parts[-1]
            if isinstance(current, dict) and leaf in current:
                original = current[leaf]
                if not isinstance(original, str):
                    raise ExportIntegrityError("redaction targets must be string values")
                current[leaf] = "[REDACTED]"
                applied.add(pointer)
            elif isinstance(current, list):
                index = _array_index(leaf, len(current))
                original = current[index]
                if not isinstance(original, str):
                    raise ExportIntegrityError("redaction targets must be string values")
                current[index] = "[REDACTED]"
                applied.add(pointer)
        except KeyError:
            continue
    return value, applied


def _encode_documents(
    role: Role,
    documents: Sequence[dict[str, object]],
    rules: Sequence[RedactionRule],
    loader: Callable[[str | bytes], object],
    *,
    jsonl: bool,
) -> tuple[bytes, bool, str | None]:
    pointers = tuple(rule.json_pointer for rule in rules if rule.role == role)
    source_bytes = tuple(rfc8785.dumps(cast(JsonValue, item)) for item in documents)
    encoded: list[bytes] = []
    applied: set[str] = set()
    for document in documents:
        redacted, hits = _redact(document, pointers)
        if role == "run_events" and hits:
            redacted["payload_digest"] = digest_payload_v0(
                cast(dict[str, object], redacted["payload"])
            )
        validated = loader(rfc8785.dumps(cast(JsonValue, redacted)))
        canonical = cast(bytes, getattr(validated, "canonical_bytes"))
        encoded.append(canonical)
        applied.update(hits)
    if set(pointers) != applied:
        raise ExportIntegrityError(f"a configured {role} redaction path did not match any document")
    source = b"".join(item + b"\n" for item in source_bytes) if jsonl else source_bytes[0]
    output = b"".join(item + b"\n" for item in encoded) if jsonl else encoded[0]
    return output, bool(applied), _sha(source) if applied else None


def _file_entry(
    path: str,
    role: Role,
    data: bytes,
    *,
    redacted: bool = False,
    source_digest: str | None = None,
) -> dict[str, object]:
    media_type, canonical, source_classification = expected_role_metadata_v0(
        role, redacted=redacted
    )
    entry: dict[str, object] = {
        "path": path,
        "role": role,
        "media_type": media_type,
        "byte_length": len(data),
        "sha256": _sha(data),
        "canonical": canonical,
        "redacted": redacted,
        "source_classification": source_classification,
    }
    if source_digest is not None:
        entry["source_digest"] = source_digest
    return entry


def _provider_models(events: Sequence[RunEvent]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str | None], set[str]] = {}
    for event in events:
        document = event.to_dict()
        if document["event_type"] != "agent.step":
            continue
        model = cast(
            dict[str, object], cast(dict[str, object], document["payload"]).get("model", {})
        )
        provider = model.get("provider")
        requested = model.get("requested_model")
        if not isinstance(provider, str) or not isinstance(requested, str):
            continue
        resolved = model.get("resolved_model")
        key = (provider, requested, resolved if isinstance(resolved, str) else None)
        request_id = model.get("provider_request_id")
        grouped.setdefault(key, set())
        if isinstance(request_id, str):
            grouped[key].add(request_id)
    result: list[dict[str, object]] = []
    for (provider, requested, resolved), request_ids in sorted(grouped.items()):
        item: dict[str, object] = {
            "provider": provider,
            "requested_model": requested,
            "provider_request_ids": sorted(request_ids),
        }
        if resolved is not None:
            item["resolved_model"] = resolved
        result.append(item)
    return result


def _validate_event_stream(run_id: str, events: Sequence[RunEvent]) -> None:
    documents = [item.to_dict() for item in events]
    sequences = [cast(int, item["sequence"]) for item in documents]
    event_ids = [cast(str, item["event_id"]) for item in documents]
    if not documents or sequences != list(range(1, len(documents) + 1)):
        raise ExportIntegrityError(
            "authoritative event sequence is missing, duplicated, or unordered"
        )
    if len(set(event_ids)) != len(event_ids) or any(item["run_id"] != run_id for item in documents):
        raise ExportIntegrityError("authoritative event identities are inconsistent")


def _load_run(
    repository: PersistenceRepository, run_id: str, truths: tuple[GroundTruth, ...]
) -> _RunMaterial:
    run = repository.get_run(run_id)
    if run is None:
        raise ExportIntegrityError("Run does not exist")
    if run.status not in _TERMINAL:
        raise ExportIntegrityError("final Run bundles require a terminal Run")
    scenario_record = repository.get_scenario_revision(run.scenario.id, run.scenario.revision)
    if scenario_record is None or scenario_record.scenario.digest != run.scenario.digest:
        raise ExportIntegrityError("Run Scenario provenance is unavailable or corrupt")
    agent_record = repository.get_agent_configuration_reference(
        run.agent_configuration.id, run.agent_configuration.revision
    )
    if agent_record is None or agent_record.reference != run.agent_configuration:
        raise ExportIntegrityError("Run Agent Configuration provenance is unavailable or corrupt")
    event_records = repository.fetch_events(run_id)
    events = tuple(record.event for record in event_records)
    _validate_event_stream(run_id, events)
    report_record = repository.get_final_report(run_id)
    report = None if report_record is None else report_record.report
    evaluations: tuple[EvaluationResult, ...] = ()
    if run.status == "completed":
        evaluations = (load_authoritative_evaluation_snapshot(repository, run_id, truths).result,)
    membership = repository.get_campaign_membership(run_id)
    if membership is not None:
        plan = repository.get_campaign_plan(membership.campaign_id)
        if plan is None or plan.canonical_digest != membership.campaign_plan_digest:
            raise ExportIntegrityError("Run Campaign fault provenance is corrupt")
        selected = plan.selected_fault_ids
    elif run.fault_plan_digest is not None:
        compiled_plan = compile_fault_plan_v0(scenario_record.scenario)
        if compiled_plan.digest != run.fault_plan_digest:
            raise ExportIntegrityError("Run fault-plan digest cannot be reconstructed")
        selected = compiled_plan.selected_fault_ids
    else:
        selected = ()
    return _RunMaterial(
        run,
        scenario_record.scenario,
        agent_record.configuration,
        events,
        report,
        evaluations,
        truths,
        selected,
    )


def _add_run(
    material: _RunMaterial,
    files_by_path: dict[str, bytes],
    entries: list[dict[str, object]],
    rules: Sequence[RedactionRule],
    *,
    shared_provenance: bool,
) -> dict[str, object]:
    prefix = _run_prefix(material.run.run_id)
    scenario_path = "provenance/scenario.json"
    agent_path = "provenance/agent_configuration.json"
    if not shared_provenance or scenario_path not in files_by_path:
        data, redacted, source = _encode_documents(
            "scenario", (material.scenario.to_dict(),), rules, loads_scenario, jsonl=False
        )
        files_by_path[scenario_path] = data
        entries.append(
            _file_entry(scenario_path, "scenario", data, redacted=redacted, source_digest=source)
        )
    if material.agent_configuration is not None and (
        not shared_provenance or agent_path not in files_by_path
    ):
        data, redacted, source = _encode_documents(
            "agent_configuration",
            (material.agent_configuration.to_dict(),),
            rules,
            loads_agent_configuration,
            jsonl=False,
        )
        files_by_path[agent_path] = data
        entries.append(
            _file_entry(
                agent_path, "agent_configuration", data, redacted=redacted, source_digest=source
            )
        )
    event_path = f"{prefix}/events.jsonl"
    event_data, redacted, source = _encode_documents(
        "run_events",
        tuple(item.to_dict() for item in material.events),
        rules,
        loads_run_event,
        jsonl=True,
    )
    files_by_path[event_path] = event_data
    entries.append(
        _file_entry(
            event_path,
            "run_events",
            event_data,
            redacted=redacted,
            source_digest=source,
        )
    )
    evaluation_refs: list[dict[str, object]] = []
    if material.evaluations:
        evaluation_path = f"{prefix}/evaluation/results.jsonl"
        ordered = tuple(
            sorted(
                material.evaluations, key=lambda item: cast(str, item.to_dict()["evaluation_id"])
            )
        )
        data, redacted, source = _encode_documents(
            "evaluation_results",
            tuple(item.to_dict() for item in ordered),
            rules,
            loads_evaluation_result,
            jsonl=True,
        )
        files_by_path[evaluation_path] = data
        entries.append(
            _file_entry(
                evaluation_path,
                "evaluation_results",
                data,
                redacted=redacted,
                source_digest=source,
            )
        )
        truth_path = f"{prefix}/evaluation/ground-truths.jsonl"
        truths = tuple(sorted(material.ground_truths, key=lambda item: item.digest))
        truth_data, truth_redacted, truth_source = (
            _encode_documents(
                "ground_truths",
                tuple(item.to_dict() for item in truths),
                rules,
                loads_ground_truth,
                jsonl=True,
            )
            if truths
            else (b"", False, None)
        )
        files_by_path[truth_path] = truth_data
        entries.append(
            _file_entry(
                truth_path,
                "ground_truths",
                truth_data,
                redacted=truth_redacted,
                source_digest=truth_source,
            )
        )
        evaluation_refs = [
            {
                "evaluation_id": item.to_dict()["evaluation_id"],
                "digest": item.digest,
                "path": evaluation_path,
                "ground_truths_path": truth_path,
                "ground_truth_digests": [truth.digest for truth in truths],
            }
            for item in ordered
        ]
    report_info: dict[str, object] = {"status": "unavailable"}
    if material.report is not None:
        report_path = f"{prefix}/report.json"
        data, redacted, source = _encode_documents(
            "run_report", (material.report.to_dict(),), rules, loads_run_report, jsonl=False
        )
        files_by_path[report_path] = data
        entries.append(
            _file_entry(
                report_path,
                "run_report",
                data,
                redacted=redacted,
                source_digest=source,
            )
        )
        report_info = {
            "status": "available",
            "report_id": material.report.to_dict()["report_id"],
            "path": report_path,
        }
    fault_plan: dict[str, object] = (
        {"status": "unavailable"}
        if material.run.fault_plan_digest is None
        else {"status": "available", "digest": material.run.fault_plan_digest}
    )
    return {
        "run_id": material.run.run_id,
        "status": material.run.status,
        "scenario": _revision(material.run.scenario),
        "agent_configuration": _revision(material.run.agent_configuration),
        "agent_configuration_content": (
            "available" if material.agent_configuration is not None else "unavailable"
        ),
        "fault_seed": material.run.fault_seed,
        "selected_fault_ids": list(material.selected_fault_ids),
        "fault_plan": fault_plan,
        "provider_models": _provider_models(material.events),
        "events_path": event_path,
        "evaluations": evaluation_refs,
        "report": report_info,
    }


def _finish_bundle(
    *,
    export_kind: str,
    run_documents: list[dict[str, object]],
    files_by_path: dict[str, bytes],
    entries: list[dict[str, object]],
    rules: Sequence[RedactionRule],
    exported_at: datetime | None,
    application: ApplicationProvenance | None,
    campaign: dict[str, object] | None = None,
) -> ExportBundle:
    rule_documents = [
        {"role": rule.role, "json_pointer": rule.json_pointer, "replacement": "[REDACTED]"}
        for rule in sorted(rules, key=lambda item: (item.role, item.json_pointer))
    ]
    manifest_document: dict[str, object] = {
        "schema_version": "chaosagent.export-manifest/v0",
        "bundle_format_version": BUNDLE_FORMAT_V0,
        "export_kind": export_kind,
        "exported_at": _timestamp(exported_at),
        "redaction": {"status": "redacted" if rules else "unredacted", "rules": rule_documents},
        "runs": sorted(run_documents, key=lambda item: cast(str, item["run_id"])),
        "files": sorted(entries, key=lambda item: cast(str, item["path"])),
    }
    if application is not None:
        app: dict[str, object] = {"version": application.version}
        if application.repository_commit is not None:
            app["repository_commit"] = application.repository_commit
        if application.repository_dirty is not None:
            app["repository_dirty"] = application.repository_dirty
        manifest_document["application"] = app
    if campaign is not None:
        manifest_document["campaign"] = campaign
    manifest = export_manifest_v0(manifest_document)
    files_by_path["manifest.json"] = manifest.canonical_bytes
    from .contracts import checksum_index

    files_by_path["checksums.sha256"] = checksum_index(files_by_path)
    return ExportBundle(manifest, files_by_path)


def export_run_bundle(
    engine: Engine,
    run_id: str,
    *,
    ground_truths: Iterable[GroundTruth] = (),
    redaction_rules: Iterable[RedactionRule] = (),
    exported_at: datetime | None = None,
    application: ApplicationProvenance | None = None,
) -> ExportBundle:
    _export_request(engine, run_id, exported_at, application)
    truths = _ground_truths(ground_truths)
    rules = _redaction_rules(redaction_rules)
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
            with connection.begin():
                connection.execute(text("SET TRANSACTION READ ONLY"))
                with Session(bind=connection) as session:
                    material = _load_run(PersistenceRepository(session), run_id, truths)
    except ExportIntegrityError:
        raise
    except (PersistenceError, SQLAlchemyError, ValueError, TypeError, KeyError) as error:
        raise ExportIntegrityError("authoritative Run export snapshot is unavailable") from error
    files_by_path: dict[str, bytes] = {}
    entries: list[dict[str, object]] = []
    run_document = _add_run(material, files_by_path, entries, rules, shared_provenance=False)
    return _finish_bundle(
        export_kind="run",
        run_documents=[run_document],
        files_by_path=files_by_path,
        entries=entries,
        rules=rules,
        exported_at=exported_at,
        application=application,
    )


def export_campaign_bundle(
    engine: Engine,
    campaign_id: str,
    *,
    ground_truths: Iterable[GroundTruth] = (),
    k_values: Iterable[int] = (1,),
    redaction_rules: Iterable[RedactionRule] = (),
    exported_at: datetime | None = None,
    application: ApplicationProvenance | None = None,
) -> ExportBundle:
    _export_request(engine, campaign_id, exported_at, application)
    truths = _ground_truths(ground_truths)
    rules = _redaction_rules(redaction_rules)
    if any(rule.role in {"scenario", "run_events"} for rule in rules):
        raise ExportIntegrityError(
            "Campaign bundles require unredacted Scenario and Run events for fault authority"
        )
    requested_k_values = _k_values(k_values)
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
            with connection.begin():
                connection.execute(text("SET TRANSACTION READ ONLY"))
                with Session(bind=connection) as session:
                    repository = PersistenceRepository(session)
                    record = repository.get_campaign_plan(campaign_id)
                    if record is None:
                        raise ExportIntegrityError("Campaign does not exist")
                    plan = authenticated_campaign_plan(
                        repository,
                        campaign_id=record.campaign_id,
                        arm=record.arm,
                        selected_fault_ids=record.selected_fault_ids,
                        assignments=dict(record.assignments),
                    )
                    materials = tuple(
                        _load_run(repository, run_id, truths) for _, run_id in record.assignments
                    )
                    trials = tuple(
                        authenticated_campaign_trial(
                            repository, plan, item.run.run_id, ground_truths=truths
                        )
                        for item in materials
                    )
                    scenario_document = materials[0].scenario.to_dict()
                    available = tuple(
                        sorted(
                            cast(str, fault["id"])
                            for fault in cast(list[dict[str, object]], scenario_document["faults"])
                        )
                    )
                    cohort = campaign_cohort_v0(
                        campaign_id=record.campaign_id,
                        arm=record.arm,
                        scenario=_revision(record.scenario),
                        agent_configuration=_revision(record.agent_configuration),
                        available_fault_ids=available,
                        selected_fault_ids=record.selected_fault_ids,
                        planned_trials=record.planned_trials,
                        trials=trials,
                    )
                    statistics = aggregate_campaign_v0(cohort, k_values=requested_k_values)
    except ExportIntegrityError:
        raise
    except (PersistenceError, SQLAlchemyError, ValueError, TypeError, KeyError) as error:
        raise ExportIntegrityError(
            "authoritative Campaign export snapshot is unavailable"
        ) from error
    files_by_path: dict[str, bytes] = {}
    entries: list[dict[str, object]] = []
    run_documents = [
        _add_run(item, files_by_path, entries, rules, shared_provenance=True) for item in materials
    ]
    plan_document: dict[str, object] = {
        "schema_version": "chaosagent.campaign-plan/v0",
        "campaign_id": record.campaign_id,
        "arm": record.arm,
        "planned_trials": record.planned_trials,
        "scenario": _revision(record.scenario),
        "agent_configuration": _revision(record.agent_configuration),
        "selected_fault_ids": list(record.selected_fault_ids),
        "fault_plan_digest": record.fault_plan_digest,
        "assignments": [
            {"trial_index": index, "run_id": run_id} for index, run_id in record.assignments
        ],
    }
    plan_data = rfc8785.dumps(cast(JsonValue, plan_document))
    if _sha(plan_data) != record.canonical_digest:
        raise ExportIntegrityError("Campaign plan canonical digest is corrupt")
    plan_path = "campaign/plan.json"
    files_by_path[plan_path] = plan_data
    entries.append(_file_entry(plan_path, "campaign_plan", plan_data))
    stats_path = "campaign/statistics.json"
    stats_data, stats_redacted, stats_source = _encode_documents(
        "campaign_statistics",
        (statistics.to_dict(),),
        rules,
        loads_campaign_statistics_v0,
        jsonl=False,
    )
    files_by_path[stats_path] = stats_data
    entries.append(
        _file_entry(
            stats_path,
            "campaign_statistics",
            stats_data,
            redacted=stats_redacted,
            source_digest=stats_source,
        )
    )
    comparison_info: dict[str, object] = {"status": "unavailable"}
    campaign_document: dict[str, object] = {
        "campaign_id": record.campaign_id,
        "plan_digest": record.canonical_digest,
        "plan_path": plan_path,
        "statistics": {"status": "available", "digest": statistics.digest, "path": stats_path},
        "comparison": comparison_info,
    }
    return _finish_bundle(
        export_kind="campaign",
        run_documents=run_documents,
        files_by_path=files_by_path,
        entries=entries,
        rules=rules,
        exported_at=exported_at,
        application=application,
        campaign=campaign_document,
    )
