"""Offline validation for closed-set ChaosAgent export bundles."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import rfc8785
from chaosagent_agent_configurations import loads_agent_configuration
from chaosagent_evaluators import (
    loads_campaign_statistics_v0,
    loads_evaluation_result,
    loads_ground_truth,
)
from chaosagent_evidence import (
    loads_run_event,
    loads_run_report,
    validate_run_event_stream_v0,
    validate_run_report_with_events_v0,
)
from chaosagent_faults import (
    FaultEngine,
    FaultHistoryValidationError,
    authenticate_fault_history_v0,
    compile_fault_plan_v0,
)
from chaosagent_scenarios import loads_scenario

from .contracts import (
    ExportBundle,
    ExportIntegrityError,
    ExportValidationError,
    ValidationResult,
    expected_role_metadata_v0,
    loads_export_manifest_v0,
    parse_checksum_index,
    read_bundle_directory,
    snapshot_bundle_mapping_v0,
)

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExportValidationError(f"Campaign plan has duplicate JSON key {key!r}")
        result[key] = value
    return result


def _sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _json_lines(data: bytes, subject: str) -> list[bytes]:
    if not data:
        return []
    if not data.endswith(b"\n"):
        raise ExportValidationError(f"{subject} JSONL must end every record with LF")
    lines = data.splitlines()
    if any(not line for line in lines):
        raise ExportValidationError(f"{subject} JSONL contains a blank record")
    return lines


def _entry_by_path(document: dict[str, object]) -> dict[str, dict[str, object]]:
    return {
        cast(str, entry["path"]): entry
        for entry in cast(list[dict[str, object]], document["files"])
    }


def _canonical_file(data: bytes, role: str) -> tuple[list[dict[str, object]], bytes]:
    loaders = {
        "scenario": loads_scenario,
        "agent_configuration": loads_agent_configuration,
        "run_events": loads_run_event,
        "run_report": loads_run_report,
        "evaluation_results": loads_evaluation_result,
        "ground_truths": loads_ground_truth,
        "campaign_statistics": loads_campaign_statistics_v0,
    }
    if role == "campaign_plan":
        try:
            value = json.loads(data, object_pairs_hook=_reject_duplicate_keys)
        except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as error:
            raise ExportValidationError("Campaign plan JSON is malformed") from error
        if not isinstance(value, dict):
            raise ExportValidationError("Campaign plan must be a JSON object")
        canonical = rfc8785.dumps(value)
        return [cast(dict[str, object], value)], canonical
    loader = loaders.get(role)
    if loader is None:
        raise ExportValidationError("manifest contains an unsupported file role")
    jsonl = role in {"run_events", "evaluation_results", "ground_truths"}
    raw_documents = _json_lines(data, role) if jsonl else [data]
    documents: list[dict[str, object]] = []
    encoded: list[bytes] = []
    for raw in raw_documents:
        loaded = loader(raw)
        canonical = cast(bytes, getattr(loaded, "canonical_bytes"))
        value = cast(object, json.loads(canonical))
        if not isinstance(value, dict):
            raise ExportValidationError(f"{role} record must be an object")
        documents.append(cast(dict[str, object], value))
        encoded.append(canonical)
    expected = b"".join(item + b"\n" for item in encoded) if jsonl else encoded[0]
    return documents, expected


def _event_map(run_id: str, documents: list[dict[str, object]]) -> dict[str, int]:
    validate_run_event_stream_v0(documents, complete=True)
    sequences = [cast(int, item["sequence"]) for item in documents]
    event_ids = [cast(str, item["event_id"]) for item in documents]
    if sequences != list(range(1, len(documents) + 1)) or not documents:
        raise ExportIntegrityError("Run event sequence is missing, duplicated, or unordered")
    if len(set(event_ids)) != len(event_ids) or any(item["run_id"] != run_id for item in documents):
        raise ExportIntegrityError("Run event identities do not match the manifest")
    return dict(zip(event_ids, sequences, strict=True))


def _provider_models(documents: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str | None], set[str]] = {}
    for event in documents:
        if event["event_type"] != "agent.step":
            continue
        model = cast(dict[str, object], cast(dict[str, object], event["payload"]).get("model", {}))
        provider = model.get("provider")
        requested = model.get("requested_model")
        if not isinstance(provider, str) or not isinstance(requested, str):
            continue
        resolved_value = model.get("resolved_model")
        resolved = resolved_value if isinstance(resolved_value, str) else None
        key = (provider, requested, resolved)
        grouped.setdefault(key, set())
        request_id = model.get("provider_request_id")
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


def _validate_evidence_refs(
    document: dict[str, object], events: Mapping[str, int], boundary: int
) -> None:
    for collection_name in ("critical_gates", "diagnostic_metrics"):
        for item in cast(list[dict[str, object]], document[collection_name]):
            for reference in cast(list[dict[str, object]], item["evidence"]):
                event_id = cast(str, reference["event_id"])
                sequence = cast(int, reference["sequence"])
                if events.get(event_id) != sequence or sequence > boundary:
                    raise ExportIntegrityError(
                        "evaluation evidence reference is absent or out of bounds"
                    )


def _validate_relationships(
    manifest: dict[str, object], parsed: Mapping[str, list[dict[str, object]]]
) -> None:
    entries = _entry_by_path(manifest)
    expected_roles: dict[str, str] = {}
    authenticated_observations: dict[str, dict[str, tuple[str, ...]]] = {}
    event_maps: dict[str, dict[str, int]] = {}
    campaign_bundle = isinstance(manifest.get("campaign"), dict)
    scenario_paths = [path for path, entry in entries.items() if entry["role"] == "scenario"]
    agent_paths = [
        path for path, entry in entries.items() if entry["role"] == "agent_configuration"
    ]
    if len(scenario_paths) != 1 or len(agent_paths) > 1:
        raise ExportIntegrityError("bundle provenance file cardinality is invalid")
    scenario = parsed[scenario_paths[0]][0]
    if scenario_paths[0] != "provenance/scenario.json":
        raise ExportIntegrityError("Scenario is stored at a noncanonical bundle path")
    expected_roles[scenario_paths[0]] = "scenario"
    scenario_value = loads_scenario(rfc8785.dumps(cast(JsonValue, scenario)))
    scenario_entry = entries[scenario_paths[0]]
    agent = parsed[agent_paths[0]][0] if agent_paths else None
    agent_entry = entries[agent_paths[0]] if agent_paths else None
    if agent_paths:
        if agent_paths[0] != "provenance/agent_configuration.json":
            raise ExportIntegrityError(
                "Agent Configuration is stored at a noncanonical bundle path"
            )
        expected_roles[agent_paths[0]] = "agent_configuration"
    all_run_ids: set[str] = set()
    runs_by_id: dict[str, dict[str, object]] = {}
    for run in cast(list[dict[str, object]], manifest["runs"]):
        run_id = cast(str, run["run_id"])
        all_run_ids.add(run_id)
        runs_by_id[run_id] = run
        scenario_ref = cast(dict[str, object], run["scenario"])
        if (scenario["scenario_id"], scenario["revision"]) != (
            scenario_ref["id"],
            scenario_ref["revision"],
        ):
            raise ExportIntegrityError("Scenario file does not match a Run revision")
        scenario_identity = (
            cast(str, scenario_entry["source_digest"])
            if scenario_entry["redacted"]
            else _sha(rfc8785.dumps(cast(JsonValue, scenario)))
        )
        if scenario_identity != scenario_ref["digest"]:
            raise ExportIntegrityError("Scenario file does not match its authoritative digest")
        agent_ref = cast(dict[str, object], run["agent_configuration"])
        if run["agent_configuration_content"] == "available":
            if agent is None or agent_entry is None:
                raise ExportIntegrityError("Agent Configuration content is declared but absent")
            if (agent["agent_configuration_id"], agent["revision"]) != (
                agent_ref["id"],
                agent_ref["revision"],
            ):
                raise ExportIntegrityError("Agent Configuration file does not match a Run")
            agent_identity = (
                cast(str, agent_entry["source_digest"])
                if agent_entry["redacted"]
                else _sha(rfc8785.dumps(cast(JsonValue, agent)))
            )
            if agent_identity != agent_ref["digest"]:
                raise ExportIntegrityError("Agent Configuration digest does not match a Run")
        events_path = cast(str, run["events_path"])
        prefix = "runs/" + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]
        if events_path != f"{prefix}/events.jsonl":
            raise ExportIntegrityError("Run events are stored at a noncanonical bundle path")
        expected_roles[events_path] = "run_events"
        event_documents = parsed[events_path]
        event_map = _event_map(run_id, event_documents)
        event_maps[run_id] = event_map
        if _provider_models(event_documents) != run["provider_models"]:
            raise ExportIntegrityError("provider/model provenance does not match Run evidence")
        selected_fault_ids = cast(list[str], run["selected_fault_ids"])
        fault_plan = cast(dict[str, object], run["fault_plan"])
        if fault_plan["status"] == "available":
            scenario_fault_ids = {
                cast(str, item["id"]) for item in cast(list[dict[str, object]], scenario["faults"])
            }
            if not set(selected_fault_ids) <= scenario_fault_ids:
                raise ExportIntegrityError("selected faults are absent from the Scenario")
            if not scenario_entry["redacted"]:
                compiled = compile_fault_plan_v0(
                    scenario_value, selected_fault_ids=selected_fault_ids
                )
                if compiled.digest != fault_plan["digest"]:
                    raise ExportIntegrityError(
                        "compiled fault-plan provenance does not match Scenario"
                    )
        if campaign_bundle:
            if scenario_entry["redacted"] or entries[events_path]["redacted"]:
                raise ExportIntegrityError(
                    "Campaign fault authority requires unredacted Scenario and Run events"
                )
            fault_events = any(
                cast(str, item["event_type"]).startswith("fault.") for item in event_documents
            )
            if selected_fault_ids or fault_events:
                seed = run["fault_seed"]
                if isinstance(seed, bool) or not isinstance(seed, int):
                    raise ExportIntegrityError(
                        "Campaign fault authority requires a frozen Run seed"
                    )
                try:
                    compiled = compile_fault_plan_v0(
                        scenario_value, selected_fault_ids=selected_fault_ids
                    )
                    history = authenticate_fault_history_v0(
                        event_documents,
                        FaultEngine(compiled, run_seed=seed),
                        run_id=run_id,
                        scenario_digest=cast(str, scenario_ref["digest"]),
                        producer_component="tool-gateway",
                    )
                except (FaultHistoryValidationError, ValueError, TypeError) as error:
                    raise ExportIntegrityError(
                        "Campaign Run fault evidence is not authoritative"
                    ) from error
                authenticated_observations[run_id] = dict(history.observed_event_ids)
            else:
                authenticated_observations[run_id] = {}
        evaluations = cast(list[dict[str, object]], run["evaluations"])
        for reference in evaluations:
            path = cast(str, reference["path"])
            if path != f"{prefix}/evaluation/results.jsonl":
                raise ExportIntegrityError(
                    "Evaluation Results are stored at a noncanonical bundle path"
                )
            expected_roles[path] = "evaluation_results"
            candidates = [
                item
                for item in parsed[path]
                if item["evaluation_id"] == reference["evaluation_id"] and item["run_id"] == run_id
            ]
            if len(candidates) != 1:
                raise ExportIntegrityError(
                    "Evaluation Result file does not match its Run reference"
                )
            evaluation = candidates[0]
            if (
                not entries[path]["redacted"]
                and _sha(rfc8785.dumps(cast(JsonValue, evaluation))) != reference["digest"]
            ):
                raise ExportIntegrityError("Evaluation Result digest does not match the manifest")
            evaluation_boundary = cast(int, evaluation["evidence_through_sequence"])
            _validate_evidence_refs(evaluation, event_map, evaluation_boundary)
            truth_path = cast(str, reference["ground_truths_path"])
            if truth_path != f"{prefix}/evaluation/ground-truths.jsonl":
                raise ExportIntegrityError("Ground Truths are stored at a noncanonical bundle path")
            expected_roles[truth_path] = "ground_truths"
            truth_digests = sorted(
                _sha(rfc8785.dumps(cast(JsonValue, truth))) for truth in parsed[truth_path]
            )
            if not entries[truth_path]["redacted"] and truth_digests != cast(
                list[str], reference["ground_truth_digests"]
            ):
                raise ExportIntegrityError(
                    "Ground Truth content does not match Evaluation provenance"
                )
        report_info = cast(dict[str, object], run["report"])
        if report_info["status"] == "available":
            report_path = cast(str, report_info["path"])
            if report_path != f"{prefix}/report.json":
                raise ExportIntegrityError("Run Report is stored at a noncanonical bundle path")
            expected_roles[report_path] = "run_report"
            report = parsed[report_path][0]
            if report["run_id"] != run_id or report["report_id"] != report_info["report_id"]:
                raise ExportIntegrityError("Run Report identity does not match the manifest")
            if (
                report["scenario"] != run["scenario"]
                or report["agent_configuration"] != run["agent_configuration"]
                or report["run_status"] != run["status"]
            ):
                raise ExportIntegrityError("Run Report provenance contradicts the manifest")
            report_boundary = cast(dict[str, object], report["evidence_boundary"])
            if report_boundary["event_count"] != len(event_map) or report_boundary[
                "last_sequence"
            ] != len(event_map):
                raise ExportIntegrityError(
                    "Run Report evidence boundary does not match exported events"
                )
            validate_run_report_with_events_v0(report, event_documents)
    campaign = manifest.get("campaign")
    if isinstance(campaign, dict):
        plan_path = cast(str, campaign["plan_path"])
        if plan_path != "campaign/plan.json":
            raise ExportIntegrityError("Campaign plan is stored at a noncanonical bundle path")
        expected_roles[plan_path] = "campaign_plan"
        plan = parsed[plan_path][0]
        if plan.get("campaign_id") != campaign["campaign_id"]:
            raise ExportIntegrityError("Campaign plan identity does not match the manifest")
        if _sha(rfc8785.dumps(cast(JsonValue, plan))) != campaign["plan_digest"]:
            raise ExportIntegrityError("Campaign plan digest does not match the manifest")
        assignments = cast(list[dict[str, object]], plan.get("assignments", []))
        if {cast(str, item["run_id"]) for item in assignments} != all_run_ids:
            raise ExportIntegrityError("Campaign plan assignments do not match included Runs")
        plan_selected = plan.get("selected_fault_ids")
        if any(
            run["selected_fault_ids"] != plan_selected
            for run in cast(list[dict[str, object]], manifest["runs"])
        ):
            raise ExportIntegrityError("Campaign selected faults do not match included Runs")
        statistics = cast(dict[str, object], campaign["statistics"])
        if statistics["status"] == "available":
            stats_path = cast(str, statistics["path"])
            if stats_path != "campaign/statistics.json":
                raise ExportIntegrityError(
                    "Campaign statistics are stored at a noncanonical bundle path"
                )
            expected_roles[stats_path] = "campaign_statistics"
            stats = parsed[stats_path][0]
            if stats["campaign_id"] != campaign["campaign_id"]:
                raise ExportIntegrityError("Campaign statistics belong to another Campaign")
            if (
                not entries[stats_path]["redacted"]
                and _sha(rfc8785.dumps(cast(JsonValue, stats))) != statistics["digest"]
            ):
                raise ExportIntegrityError("Campaign statistics digest does not match the manifest")
            expected_projection = {
                "campaign_id": plan["campaign_id"],
                "arm": plan["arm"],
                "scenario": plan["scenario"],
                "agent_configuration": plan["agent_configuration"],
                "selected_fault_ids": plan["selected_fault_ids"],
                "planned_trials": plan["planned_trials"],
            }
            if any(stats[key] != value for key, value in expected_projection.items()):
                raise ExportIntegrityError("Campaign statistics contradict the Campaign plan")
            scenario_faults = cast(list[dict[str, object]], scenario["faults"])
            if stats["available_fault_ids"] != sorted(
                cast(str, fault["id"]) for fault in scenario_faults
            ):
                raise ExportIntegrityError("Campaign statistics contradict Scenario faults")
            assignment_by_run = {
                cast(str, item["run_id"]): cast(int, item["trial_index"]) for item in assignments
            }
            statistic_rows = cast(list[dict[str, object]], stats["runs"])
            if {cast(str, item["run_id"]) for item in statistic_rows} != all_run_ids:
                raise ExportIntegrityError("Campaign statistics Run membership is inconsistent")
            for row in statistic_rows:
                member_run_id = cast(str, row["run_id"])
                if row["trial_index"] != assignment_by_run[member_run_id]:
                    raise ExportIntegrityError(
                        "Campaign statistics trial assignment is inconsistent"
                    )
                run_manifest = runs_by_id[member_run_id]
                evaluation_refs = cast(list[dict[str, object]], run_manifest["evaluations"])
                matches = [
                    item
                    for item in evaluation_refs
                    if item["evaluation_id"] == row["evaluation_id"]
                    and item["digest"] == row["evaluation_digest"]
                ]
                if len(matches) != 1:
                    raise ExportIntegrityError(
                        "Campaign statistics Evaluation identity is inconsistent"
                    )
                evaluation_candidates = [
                    item
                    for item in parsed[cast(str, matches[0]["path"])]
                    if item["evaluation_id"] == row["evaluation_id"]
                ]
                if len(evaluation_candidates) != 1:
                    raise ExportIntegrityError("Campaign statistics Evaluation is absent")
                evaluation = evaluation_candidates[0]
                if any(
                    row[field] != evaluation[evaluation_field]
                    for field, evaluation_field in (
                        ("evaluation_input_digest", "input_digest"),
                        ("evidence_through_sequence", "evidence_through_sequence"),
                        ("classification", "classification"),
                    )
                ):
                    raise ExportIntegrityError(
                        "Campaign statistics contradict an Evaluation Result"
                    )
                boundary = cast(int, evaluation["evidence_through_sequence"])
                observed_faults = sorted(
                    fault_id
                    for fault_id, event_ids in authenticated_observations[
                        cast(str, row["run_id"])
                    ].items()
                    if any(
                        event_maps[cast(str, row["run_id"])][event_id] <= boundary
                        for event_id in event_ids
                    )
                )
                if row["observed_fault_ids"] != observed_faults:
                    raise ExportIntegrityError(
                        "Campaign statistics fault observations contradict Run evidence"
                    )
        comparison = cast(dict[str, object], campaign["comparison"])
        if comparison["status"] != "unavailable":
            raise ExportIntegrityError("Campaign comparison export is unsupported in bundle v0")
    if set(expected_roles) != set(entries):
        raise ExportIntegrityError("manifest contains an unreferenced or missing payload")
    if any(entries[path]["role"] != role for path, role in expected_roles.items()):
        raise ExportIntegrityError("manifest file roles contradict referenced artifacts")


def _validate_files(files_by_path: Mapping[str, bytes]) -> str:
    if set(files_by_path) < {"manifest.json", "checksums.sha256"}:
        raise ExportValidationError("bundle is missing manifest.json or checksums.sha256")
    manifest = loads_export_manifest_v0(files_by_path["manifest.json"])
    document = manifest.to_dict()
    if manifest.canonical_bytes != files_by_path["manifest.json"]:
        raise ExportValidationError("manifest.json is not canonical JSON")
    entries = _entry_by_path(document)
    expected_paths = set(entries) | {"manifest.json", "checksums.sha256"}
    if set(files_by_path) != expected_paths:
        raise ExportIntegrityError("bundle has missing or unexpected files")
    checksums = parse_checksum_index(files_by_path["checksums.sha256"])
    if set(checksums) != expected_paths - {"checksums.sha256"}:
        raise ExportIntegrityError("checksum index paths do not match bundle contents")
    for path, digest in checksums.items():
        if _sha(files_by_path[path]) != digest:
            raise ExportIntegrityError("checksum index does not match exact exported bytes")
    parsed: dict[str, list[dict[str, object]]] = {}
    for path, entry in entries.items():
        data = files_by_path[path]
        if len(data) != entry["byte_length"] or _sha(data) != entry["sha256"]:
            raise ExportIntegrityError(f"file metadata does not match exact bytes for {path!r}")
        role = cast(str, entry["role"])
        expected_media, expected_canonical, expected_classification = expected_role_metadata_v0(
            role, redacted=cast(bool, entry["redacted"])
        )
        if (
            entry["media_type"] != expected_media
            or entry["canonical"] is not expected_canonical
            or entry["source_classification"] != expected_classification
        ):
            raise ExportIntegrityError(f"file metadata contradicts role for {path!r}")
        documents, canonical = _canonical_file(data, role)
        if canonical != data:
            raise ExportIntegrityError(f"file declared canonical but is not canonical: {path!r}")
        parsed[path] = documents
    _validate_relationships(document, parsed)
    return manifest.digest


def validate_export_bundle(
    source: ExportBundle | Mapping[str, bytes] | str | Path,
) -> ValidationResult:
    """Validate a directory or immutable in-memory bundle without PostgreSQL."""
    try:
        if isinstance(source, ExportBundle):
            files_by_path = source.files()
        elif isinstance(source, str | Path):
            files_by_path = read_bundle_directory(source)
        elif isinstance(source, Mapping):
            files_by_path = snapshot_bundle_mapping_v0(cast(Mapping[object, object], source))
        else:
            raise ExportValidationError("bundle source must be a directory or byte mapping")
        digest = _validate_files(files_by_path)
        return ValidationResult(True, (), digest)
    except ExportValidationError as error:
        return ValidationResult(False, error.errors)
    except Exception:
        return ValidationResult(False, ("bundle validation failed on malformed input",))


def validate_export_bundle_or_raise(
    source: ExportBundle | Mapping[str, bytes] | str | Path,
) -> ValidationResult:
    result = validate_export_bundle(source)
    if not result.valid:
        raise ExportValidationError(list(result.errors))
    return result
