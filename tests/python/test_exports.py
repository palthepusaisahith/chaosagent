from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import ItemsView, Iterator, Mapping
from copy import deepcopy
from pathlib import Path
from typing import cast

import chaosagent_exports.contracts as export_contracts
import pytest
import rfc8785
from chaosagent_agent_configurations import loads_agent_configuration
from chaosagent_evidence import loads_run_event, loads_run_report
from chaosagent_exports import (
    ExportBundle,
    ExportValidationError,
    checksum_index,
    export_manifest_schema_v0,
    export_manifest_v0,
    loads_export_manifest_v0,
    parse_checksum_index,
    validate_export_bundle,
)
from chaosagent_exports.bundle import RedactionRule, _encode_documents, _redact, _redaction_rules
from chaosagent_faults import compile_fault_plan_v0
from chaosagent_scenarios import loads_scenario

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "benchmarks" / "shipment-refund" / "evidence" / "v0"
SCENARIO = (
    ROOT / "benchmarks" / "shipment-refund" / "scenarios" / "refund-ambiguous-timeout.v0.json"
)


def _sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _entry(path: str, role: str, data: bytes, *, derived: bool = False) -> dict[str, object]:
    return {
        "path": path,
        "role": role,
        "media_type": "application/x-ndjson" if path.endswith(".jsonl") else "application/json",
        "byte_length": len(data),
        "sha256": _sha(data),
        "canonical": True,
        "redacted": False,
        "source_classification": "derived" if derived else "authoritative",
    }


def _agent_configuration() -> object:
    return {
        "schema_version": "chaosagent.agent-configuration/v0",
        "agent_configuration_id": "scripted-agent",
        "revision": "1",
        "provider": "openai",
        "adapter": {"id": "openai-responses", "version": "v0"},
        "model": "gpt-4.1-2025-04-14",
        "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
        "token_accounting": {
            "schema_version": "chaosagent.token-accounting/v0",
            "schedule_id": "structural-rates",
            "revision": "2026-08-28",
            "model": "gpt-4.1-2025-04-14",
            "unit": "microusd",
            "tokens_per_rate_unit": 1000000,
            "rounding": "ceiling_per_response",
            "input_rate_microusd": 1000000,
            "cached_input_rate_microusd": 500000,
            "output_rate_microusd": 2000000,
        },
        "timeout_ms": 5000,
        "max_output_tokens": 256,
        "temperature": None,
        "parallel_tool_calls": True,
        "store": False,
        "max_retries": 0,
    }


def _bundle(*, include_agent_configuration: bool = False) -> ExportBundle:
    run_prefix = "runs/" + hashlib.sha256(b"run-refund-001").hexdigest()[:24]
    events_path = f"{run_prefix}/events.jsonl"
    report_path = f"{run_prefix}/report.json"
    scenario = loads_scenario(SCENARIO.read_bytes())
    fault_plan = compile_fault_plan_v0(scenario, selected_fault_ids=("refund-ack-lost",))
    report = loads_run_report((EVIDENCE / "run-report.json").read_bytes())
    event_paths = sorted(EVIDENCE.glob("[0-9][0-9][0-9]-*.json"))
    events = [loads_run_event(path.read_bytes()) for path in event_paths]
    event_data = b"".join(item.canonical_bytes + b"\n" for item in events)
    scenario_data = scenario.canonical_bytes
    report_data = report.canonical_bytes
    files = {
        "provenance/scenario.json": scenario_data,
        events_path: event_data,
        report_path: report_data,
    }
    report_document = report.to_dict()
    scenario_document = scenario.to_dict()
    file_entries: list[dict[str, object]] = [
        _entry("provenance/scenario.json", "scenario", scenario_data),
        _entry(events_path, "run_events", event_data),
        _entry(report_path, "run_report", report_data, derived=True),
    ]
    agent_reference = report_document["agent_configuration"]
    agent_content = "unavailable"
    report_info: dict[str, object] = {
        "status": "available",
        "report_id": report_document["report_id"],
        "path": report_path,
    }
    if include_agent_configuration:
        agent = loads_agent_configuration(json.dumps(_agent_configuration()))
        agent_path = "provenance/agent_configuration.json"
        files[agent_path] = agent.canonical_bytes
        file_entries.append(_entry(agent_path, "agent_configuration", agent.canonical_bytes))
        agent_document = agent.to_dict()
        agent_reference = {
            "id": agent_document["agent_configuration_id"],
            "revision": agent_document["revision"],
            "digest": agent.digest,
        }
        agent_content = "available"
        del files[report_path]
        file_entries = [entry for entry in file_entries if entry["path"] != report_path]
        report_info = {"status": "unavailable"}
    file_entries.sort(key=lambda entry: cast(str, entry["path"]))
    manifest = export_manifest_v0(
        {
            "schema_version": "chaosagent.export-manifest/v0",
            "bundle_format_version": "chaosagent.export-bundle/v0",
            "export_kind": "run",
            "exported_at": "2026-08-24T10:00:20.010Z",
            "redaction": {"status": "unredacted", "rules": []},
            "runs": [
                {
                    "run_id": "run-refund-001",
                    "status": "completed",
                    "scenario": {
                        "id": scenario_document["scenario_id"],
                        "revision": scenario_document["revision"],
                        "digest": scenario.digest,
                    },
                    "agent_configuration": agent_reference,
                    "agent_configuration_content": agent_content,
                    "fault_seed": None,
                    "selected_fault_ids": ["refund-ack-lost"],
                    "fault_plan": {"status": "available", "digest": fault_plan.digest},
                    "provider_models": [],
                    "events_path": events_path,
                    "evaluations": [],
                    "report": report_info,
                }
            ],
            "files": file_entries,
        }
    )
    files["manifest.json"] = manifest.canonical_bytes
    files["checksums.sha256"] = checksum_index(files)
    return ExportBundle(manifest, files)


def _rechecksum(files: dict[str, bytes]) -> dict[str, bytes]:
    files["checksums.sha256"] = checksum_index(
        {path: data for path, data in files.items() if path != "checksums.sha256"}
    )
    return files


def _reseal_payload(files: dict[str, bytes], path: str, data: bytes) -> dict[str, bytes]:
    files[path] = data
    document = cast(dict[str, object], json.loads(files["manifest.json"]))
    entry = next(
        item for item in cast(list[dict[str, object]], document["files"]) if item["path"] == path
    )
    entry["byte_length"] = len(data)
    entry["sha256"] = _sha(data)
    files["manifest.json"] = export_manifest_v0(document).canonical_bytes
    return _rechecksum(files)


def _reseal_manifest_unchecked(
    files: dict[str, bytes], document: dict[str, object]
) -> dict[str, bytes]:
    document.pop("manifest_digest", None)
    document.pop("export_id", None)
    identity = deepcopy(document)
    identity.pop("exported_at", None)
    digest = "sha256:" + hashlib.sha256(rfc8785.dumps(cast(JsonValue, identity))).hexdigest()
    document["manifest_digest"] = digest
    document["export_id"] = "export-" + digest.removeprefix("sha256:")[:32]
    files["manifest.json"] = rfc8785.dumps(cast(JsonValue, document))
    return _rechecksum(files)


def test_manifest_schema_is_strict_versioned_and_bundled() -> None:
    schema = export_manifest_schema_v0()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    document = _bundle().manifest.to_dict()
    document["unexpected"] = True
    with pytest.raises(ExportValidationError):
        export_manifest_v0(document)


def test_valid_bundle_is_offline_verifiable_and_immutable(tmp_path: Path) -> None:
    bundle = _bundle()
    result = validate_export_bundle(bundle)
    assert result.valid and result.manifest_digest == bundle.manifest.digest
    copied = bundle.files()
    copied["manifest.json"] = b"changed"
    assert validate_export_bundle(bundle).valid
    destination = bundle.write_directory(tmp_path / "bundle")
    assert validate_export_bundle(destination).valid


def test_repeated_bundle_and_zip_are_byte_deterministic() -> None:
    first = _bundle()
    second = _bundle()
    assert first.manifest.digest == second.manifest.digest
    assert first.files() == second.files()
    assert first.to_zip_bytes() == second.to_zip_bytes()


@pytest.mark.parametrize(
    "mutation",
    ["event", "report", "scenario", "missing", "extra", "swap", "checksum"],
)
def test_tampering_fails_closed(mutation: str) -> None:
    files = _bundle().files()
    run_prefix = "runs/" + hashlib.sha256(b"run-refund-001").hexdigest()[:24]
    events_path = f"{run_prefix}/events.jsonl"
    report_path = f"{run_prefix}/report.json"
    if mutation == "event":
        files[events_path] += b" "
    elif mutation == "report":
        files[report_path] = files[report_path].replace(
            b'"classification":"pass"', b'"classification":"fail"'
        )
    elif mutation == "scenario":
        files["provenance/scenario.json"] += b" "
    elif mutation == "missing":
        del files[report_path]
    elif mutation == "extra":
        files["unexpected.json"] = b"{}"
    elif mutation == "swap":
        files["provenance/scenario.json"], files[report_path] = (
            files[report_path],
            files["provenance/scenario.json"],
        )
    else:
        files["checksums.sha256"] = files["checksums.sha256"].replace(b"a", b"b", 1)
    assert not validate_export_bundle(files).valid


def test_manifest_digest_excludes_occurrence_timestamp_only() -> None:
    first = _bundle().manifest.to_dict()
    second = deepcopy(first)
    second["exported_at"] = "2027-01-01T00:00:00Z"
    assert export_manifest_v0(first).digest == export_manifest_v0(second).digest
    cast(list[dict[str, object]], second["runs"])[0]["fault_seed"] = 7
    assert export_manifest_v0(first).digest != export_manifest_v0(second).digest


@pytest.mark.parametrize(
    "value",
    [b"{", b'{"schema_version":"chaosagent.export-manifest/v0","x":NaN}', b"[[]"],
)
def test_malformed_manifest_is_sanitized(value: bytes) -> None:
    with pytest.raises(ExportValidationError):
        loads_export_manifest_v0(value)


def test_duplicate_manifest_and_checksum_paths_are_rejected() -> None:
    manifest = _bundle().manifest.canonical_bytes
    duplicate = manifest.replace(
        b'{"bundle_format_version"', b'{"schema_version":"x","bundle_format_version"'
    )
    with pytest.raises(ExportValidationError, match="duplicate JSON object key"):
        loads_export_manifest_v0(duplicate)
    checksum = b"0" * 64 + b"  manifest.json\n" + b"1" * 64 + b"  manifest.json\n"
    with pytest.raises(ExportValidationError, match="duplicate path"):
        parse_checksum_index(checksum)


@pytest.mark.parametrize(
    "path", ["../escape.json", "/absolute.json", "C:/drive.json", "\\\\server\\share.json"]
)
def test_unsafe_manifest_paths_are_rejected(path: str) -> None:
    document = _bundle().manifest.to_dict()
    cast(list[dict[str, object]], document["files"])[0]["path"] = path
    with pytest.raises(ExportValidationError):
        export_manifest_v0(document)


def test_manifest_content_changes_are_detected_even_with_fresh_checksums() -> None:
    files = _bundle().files()
    manifest = json.loads(files["manifest.json"])
    manifest["manifest_digest"] = "sha256:" + "f" * 64
    files["manifest.json"] = rfc8785.dumps(manifest)
    _rechecksum(files)
    result = validate_export_bundle(files)
    assert not result.valid and "manifest_digest" in result.errors[0]


@pytest.mark.parametrize("field", ["byte_length", "sha256"])
def test_manifest_file_projection_tampering_is_rejected(field: str) -> None:
    files = _bundle().files()
    document = cast(dict[str, object], json.loads(files["manifest.json"]))
    entry = cast(list[dict[str, object]], document["files"])[0]
    entry[field] = cast(int, entry[field]) + 1 if field == "byte_length" else "sha256:" + "f" * 64
    _reseal_manifest_unchecked(files, document)
    assert not validate_export_bundle(files).valid


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("media_type", "application/json"),
        ("source_classification", "metadata"),
        ("canonical", False),
    ],
)
def test_resealed_false_role_metadata_is_rejected(field: str, value: object) -> None:
    files = _bundle().files()
    document = cast(dict[str, object], json.loads(files["manifest.json"]))
    entry = next(
        item
        for item in cast(list[dict[str, object]], document["files"])
        if item["role"] == "run_events"
    )
    entry[field] = value
    _reseal_manifest_unchecked(files, document)
    assert not validate_export_bundle(files).valid


def test_noncanonical_payload_cannot_opt_out_of_canonical_validation() -> None:
    files = _bundle().files()
    path = "provenance/scenario.json"
    replacement = files[path] + b" "
    document = cast(dict[str, object], json.loads(files["manifest.json"]))
    entry = next(
        item for item in cast(list[dict[str, object]], document["files"]) if item["path"] == path
    )
    entry["canonical"] = False
    entry["byte_length"] = len(replacement)
    entry["sha256"] = _sha(replacement)
    files[path] = replacement
    _reseal_manifest_unchecked(files, document)
    assert not validate_export_bundle(files).valid


def test_resealed_role_and_path_substitution_is_rejected() -> None:
    files = _bundle().files()
    document = cast(dict[str, object], json.loads(files["manifest.json"]))
    entry = next(
        item
        for item in cast(list[dict[str, object]], document["files"])
        if item["role"] == "scenario"
    )
    entry["role"] = "run_report"
    entry["source_classification"] = "derived"
    files["manifest.json"] = export_manifest_v0(document).canonical_bytes
    _rechecksum(files)
    assert not validate_export_bundle(files).valid


@pytest.mark.parametrize("token", ["-1", "+1", "01", "99", "0x1"])
def test_redaction_rejects_invalid_or_out_of_range_array_indices(token: str) -> None:
    scenario = loads_scenario(SCENARIO.read_bytes())
    with pytest.raises(ExportValidationError):
        _encode_documents(
            "scenario",
            (scenario.to_dict(),),
            (RedactionRule("scenario", f"/agent/instructions/{token}"),),
            loads_scenario,
            jsonl=False,
        )


@pytest.mark.parametrize("pointer", ["/a/~2key", "/a/key~"])
def test_redaction_rejects_invalid_json_pointer_escapes(pointer: str) -> None:
    with pytest.raises(ExportValidationError):
        _redact({"a": {"~2key": "value", "key~": "value"}}, (pointer,))


def test_redaction_supports_strict_array_indices_and_pointer_escaping() -> None:
    redacted, applied = _redact(
        {
            "a": ["first", "second"],
            "keys": {"": "empty", "tilde~key": "one", "slash/key": "two"},
        },
        ("/a/0", "/a/1", "/keys/", "/keys/tilde~0key", "/keys/slash~1key"),
    )
    assert redacted == {
        "a": ["[REDACTED]", "[REDACTED]"],
        "keys": {
            "": "[REDACTED]",
            "tilde~key": "[REDACTED]",
            "slash/key": "[REDACTED]",
        },
    }
    assert applied == {
        "/a/0",
        "/a/1",
        "/keys/",
        "/keys/tilde~0key",
        "/keys/slash~1key",
    }


def test_duplicate_redaction_rules_fail_closed() -> None:
    rule = RedactionRule("scenario", "/metadata/description")
    with pytest.raises(ExportValidationError, match="unique"):
        _redaction_rules((rule, rule))


class _HostileBundleMapping(Mapping[str, bytes]):
    def __getitem__(self, key: str) -> bytes:
        raise RuntimeError("hostile getitem")

    def __iter__(self) -> Iterator[str]:
        raise RuntimeError("hostile iteration")

    def __len__(self) -> int:
        return 1

    def items(self) -> ItemsView[str, bytes]:
        raise RuntimeError("hostile items")


@pytest.mark.parametrize("kind", ["aggregate", "individual", "count"])
def test_mapping_bundle_resource_limits_fail_closed(
    monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    files = cast(Mapping[object, object], _bundle().files())
    if kind == "aggregate":
        monkeypatch.setattr(export_contracts, "_MAX_BUNDLE_BYTES", 1)
    elif kind == "individual":
        monkeypatch.setattr(export_contracts, "_MAX_FILE_BYTES", 1)
    else:
        monkeypatch.setattr(export_contracts, "_MAX_BUNDLE_FILES", 1)
    assert not validate_export_bundle(cast(Mapping[str, bytes], files)).valid


@pytest.mark.parametrize(
    "files",
    [
        {1: b"value"},
        {"manifest.json": "not-bytes"},
        _HostileBundleMapping(),
    ],
)
def test_mapping_bundle_types_and_hostile_behavior_are_sanitized(files: object) -> None:
    assert not validate_export_bundle(cast(Mapping[str, bytes], files)).valid


def test_campaign_comparison_is_not_part_of_single_campaign_bundle_v0() -> None:
    schema = export_manifest_schema_v0()
    definitions = cast(dict[str, object], schema["$defs"])
    campaign = cast(dict[str, object], definitions["campaign"])
    properties = cast(dict[str, object], campaign["properties"])
    comparison = cast(dict[str, object], properties["comparison"])
    status = cast(dict[str, object], cast(dict[str, object], comparison["properties"])["status"])
    assert status == {"const": "unavailable"}
    from chaosagent_exports import export_campaign_bundle

    assert "comparison" not in inspect.signature(export_campaign_bundle).parameters


@pytest.mark.parametrize("kind", ["malformed", "duplicate_key", "nonfinite"])
def test_corrupt_event_json_is_not_repaired(kind: str) -> None:
    files = _bundle().files()
    manifest = _bundle().manifest.to_dict()
    run = cast(list[dict[str, object]], manifest["runs"])[0]
    event_path = cast(str, run["events_path"])
    lines = files[event_path].splitlines(keepends=True)
    if kind == "malformed":
        lines[0] = b"{\n"
    elif kind == "duplicate_key":
        lines[0] = lines[0].replace(b"{", b'{"run_id":"duplicate",', 1)
    else:
        lines[0] = lines[0].replace(b'"sequence":1', b'"sequence":NaN', 1)
    _reseal_payload(files, event_path, b"".join(lines))
    assert not validate_export_bundle(files).valid


@pytest.mark.parametrize("value", [1, None, object(), {"manifest.json": object()}])
def test_offline_validator_sanitizes_unsupported_public_input(value: object) -> None:
    result = validate_export_bundle(cast(ExportBundle, value))
    assert not result.valid


def test_agent_configuration_substitution_is_rejected() -> None:
    bundle = _bundle(include_agent_configuration=True)
    assert validate_export_bundle(bundle).valid
    files = bundle.files()
    path = "provenance/agent_configuration.json"
    document = cast(dict[str, object], json.loads(files[path]))
    document["model"] = "gpt-4o-2024-08-06"
    cast(dict[str, object], document["token_accounting"])["model"] = "gpt-4o-2024-08-06"
    replacement = loads_agent_configuration(json.dumps(document)).canonical_bytes
    _reseal_payload(files, path, replacement)
    assert not validate_export_bundle(files).valid
