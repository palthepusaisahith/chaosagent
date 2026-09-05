"""Export Manifest v0 and immutable bundle primitives."""

from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import stat
import zipfile
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import cast

import rfc8785
from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]

EXPORT_MANIFEST_V0_SCHEMA_VERSION = "chaosagent.export-manifest/v0"
BUNDLE_FORMAT_V0 = "chaosagent.export-bundle/v0"
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_CHECKSUM_RE = re.compile(r"([0-9a-f]{64})  ([A-Za-z0-9._:-]+(?:/[A-Za-z0-9._:-]+)*)")
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_MAX_BUNDLE_BYTES = 128 * 1024 * 1024
_MAX_FILE_BYTES = 128 * 1024 * 1024
_MAX_BUNDLE_FILES = 50_002

_ROLE_METADATA_V0: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "scenario": ("application/json", "authoritative"),
        "agent_configuration": ("application/json", "authoritative"),
        "run_events": ("application/x-ndjson", "authoritative"),
        "run_report": ("application/json", "derived"),
        "evaluation_results": ("application/x-ndjson", "derived"),
        "ground_truths": ("application/x-ndjson", "metadata"),
        "campaign_plan": ("application/json", "authoritative"),
        "campaign_statistics": ("application/json", "derived"),
    }
)


class ExportValidationError(ValueError):
    """Sanitized public error for malformed manifests, bundles, and export input."""

    def __init__(self, errors: str | list[str]) -> None:
        values = [errors] if isinstance(errors, str) else errors
        self.errors = tuple(values)
        super().__init__("Invalid ChaosAgent export: " + "; ".join(values))


class ExportIntegrityError(ExportValidationError):
    """A durable source or an exported relationship is inconsistent."""


def expected_role_metadata_v0(role: str, *, redacted: bool) -> tuple[str, bool, str]:
    """Return the authoritative V0 media/canonical/classification policy."""
    try:
        media_type, source_classification = _ROLE_METADATA_V0[role]
    except KeyError as error:
        raise ExportValidationError("manifest contains an unsupported file role") from error
    return media_type, True, "derived" if redacted else source_classification


def snapshot_bundle_mapping_v0(files_by_path: Mapping[object, object]) -> dict[str, bytes]:
    """Defensively snapshot hostile mapping input within the V0 resource profile."""
    if not isinstance(files_by_path, Mapping):
        raise ExportValidationError("bundle source must be a byte mapping")
    copied: dict[str, bytes] = {}
    total = 0
    try:
        for raw_path, raw_data in files_by_path.items():
            if len(copied) >= _MAX_BUNDLE_FILES:
                raise ExportValidationError("bundle exceeds the v0 file-count limit")
            if type(raw_path) is not str:
                raise ExportValidationError("bundle paths must be strings")
            path = raw_path
            _safe_path(path)
            if path in copied:
                raise ExportValidationError("bundle contains a duplicate path")
            if type(raw_data) is not bytes:
                raise ExportValidationError("bundle payloads must be bytes")
            data = raw_data
            size = len(data)
            if size > _MAX_FILE_BYTES:
                raise ExportValidationError("bundle file exceeds the v0 size limit")
            total += size
            if total > _MAX_BUNDLE_BYTES:
                raise ExportValidationError("bundle exceeds the v0 size limit")
            copied[path] = data
    except ExportValidationError:
        raise
    except Exception as error:
        raise ExportValidationError("bundle files could not be snapshotted") from error
    return copied


@dataclass(frozen=True, slots=True, init=False)
class ExportManifest:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("ExportManifest instances require validated loading")

    def to_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):
            raise AssertionError("validated manifest root is not an object")
        return cast(dict[str, object], value)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool
    errors: tuple[str, ...]
    manifest_digest: str | None = None


@dataclass(frozen=True, slots=True, init=False)
class ExportBundle:
    """Immutable exact bundle bytes; callers only receive defensive copies."""

    manifest: ExportManifest
    _files: Mapping[str, bytes]

    def __init__(self, manifest: ExportManifest, files_by_path: Mapping[str, bytes]) -> None:
        if not isinstance(manifest, ExportManifest) or not isinstance(files_by_path, Mapping):
            raise ExportValidationError("bundle requires a validated manifest and byte mapping")
        copied = snapshot_bundle_mapping_v0(cast(Mapping[object, object], files_by_path))
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "_files", MappingProxyType(copied))

    def files(self) -> dict[str, bytes]:
        return dict(self._files)

    def to_zip_bytes(self) -> bytes:
        stream = io.BytesIO()
        with zipfile.ZipFile(
            stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9, strict_timestamps=True
        ) as archive:
            for path in sorted(self._files):
                info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(info, self._files[path], compress_type=zipfile.ZIP_DEFLATED)
        return stream.getvalue()

    def write_directory(self, destination: str | Path) -> Path:
        try:
            target = Path(destination)
        except Exception as error:
            raise ExportValidationError("destination is not a filesystem path") from error
        try:
            if target.exists() or target.is_symlink():
                raise ExportValidationError("destination must not already exist")
            requested_parent = target.parent
            if requested_parent.is_symlink():
                raise ExportValidationError("destination parent is not a safe directory")
            parent = requested_parent.resolve(strict=True)
            if not parent.is_dir():
                raise ExportValidationError("destination parent is not a safe directory")
            target.mkdir(mode=0o700)
            for logical_path in sorted(self._files):
                _safe_path(logical_path)
                output = target.joinpath(*PurePosixPath(logical_path).parts)
                output.parent.mkdir(parents=True, exist_ok=True)
                if output.is_symlink():
                    raise ExportValidationError("bundle output path resolves through a symlink")
                with output.open("xb") as handle:
                    handle.write(self._files[logical_path])
            return target
        except ExportValidationError:
            if target.exists() and target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            raise
        except OSError as error:
            if target.exists() and target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            raise ExportValidationError("bundle directory could not be written safely") from error


@lru_cache(maxsize=1)
def _schema() -> dict[str, object]:
    resource = files("chaosagent_exports.schema").joinpath("export-manifest-v0.schema.json")
    value = json.loads(resource.read_text("utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("bundled Export Manifest v0 schema is not an object")
    schema = cast(dict[str, object], value)
    Draft202012Validator.check_schema(schema)
    return schema


def export_manifest_schema_v0() -> dict[str, object]:
    return deepcopy(_schema())


def _duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExportValidationError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _parse_json(data: str | bytes, subject: str) -> object:
    if not isinstance(data, str | bytes):
        raise ExportValidationError(f"{subject} must be text or bytes")
    if len(data) > _MAX_MANIFEST_BYTES:
        raise ExportValidationError(f"{subject} exceeds the v0 size limit")
    try:
        return cast(
            object,
            json.loads(
                data,
                object_pairs_hook=_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            ),
        )
    except ExportValidationError:
        raise
    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ) as error:
        raise ExportValidationError(f"malformed {subject}") from error


def _identity_material(document: dict[str, object]) -> bytes:
    material = deepcopy(document)
    material.pop("manifest_digest", None)
    material.pop("export_id", None)
    material.pop("exported_at", None)
    return rfc8785.dumps(cast(JsonValue, material))


def _manifest_digest(document: dict[str, object]) -> str:
    return "sha256:" + hashlib.sha256(_identity_material(document)).hexdigest()


def _semantic_errors(document: dict[str, object]) -> list[str]:
    errors: list[str] = []
    runs = cast(list[dict[str, object]], document["runs"])
    run_ids = [cast(str, run["run_id"]) for run in runs]
    if run_ids != sorted(set(run_ids)):
        errors.append("runs must be uniquely ordered by run_id")
    entries = cast(list[dict[str, object]], document["files"])
    paths = [cast(str, entry["path"]) for entry in entries]
    if paths != sorted(set(paths)):
        errors.append("files must have unique lexically ordered paths")
    for path in paths:
        try:
            _safe_path(path)
        except ExportValidationError as error:
            errors.extend(error.errors)
    redaction = cast(dict[str, object], document["redaction"])
    rules = cast(list[dict[str, object]], redaction["rules"])
    if (redaction["status"] == "redacted") != bool(rules):
        errors.append("redaction status must agree with the configured rules")
    rule_keys = [(item["role"], item["json_pointer"]) for item in rules]
    if rule_keys != sorted(set(rule_keys)):
        errors.append("redaction rules must be unique and canonically ordered")
    redacted_paths = {entry["path"] for entry in entries if entry["redacted"]}
    if bool(redacted_paths) != (redaction["status"] == "redacted"):
        errors.append("redaction status must agree with redacted files")
    for entry in entries:
        expected_media, expected_canonical, expected_classification = expected_role_metadata_v0(
            cast(str, entry["role"]), redacted=cast(bool, entry["redacted"])
        )
        if (
            entry["media_type"] != expected_media
            or entry["canonical"] is not expected_canonical
            or entry["source_classification"] != expected_classification
        ):
            errors.append(
                f"file {entry['path']!r} metadata contradicts its role and redaction status"
            )
        if entry["redacted"]:
            if entry["source_classification"] != "derived" or "source_digest" not in entry:
                errors.append("redacted files must be derived and identify their source digest")
        elif "source_digest" in entry:
            errors.append("unredacted files must not declare source_digest")
    paths_set = set(paths)
    for run in runs:
        selected_fault_ids = cast(list[str], run["selected_fault_ids"])
        if selected_fault_ids != sorted(selected_fault_ids):
            errors.append(f"run {run['run_id']!r} selected fault IDs must be ordered")
        fault_plan = cast(dict[str, object], run["fault_plan"])
        if fault_plan["status"] == "unavailable" and selected_fault_ids:
            errors.append(f"run {run['run_id']!r} cannot select faults without a fault plan")
        evaluations = cast(list[dict[str, object]], run["evaluations"])
        evaluation_ids = [cast(str, item["evaluation_id"]) for item in evaluations]
        if evaluation_ids != sorted(set(evaluation_ids)):
            errors.append(f"run {run['run_id']!r} evaluations must be uniquely ordered")
        for evaluation in evaluations:
            digests = cast(list[str], evaluation["ground_truth_digests"])
            if digests != sorted(digests):
                errors.append("ground truth digests must be ordered")
        required = [cast(str, run["events_path"])]
        required.extend(cast(str, item["path"]) for item in evaluations)
        required.extend(cast(str, item["ground_truths_path"]) for item in evaluations)
        report = cast(dict[str, object], run["report"])
        if report["status"] == "available":
            required.append(cast(str, report["path"]))
        if not set(required) <= paths_set:
            errors.append(f"run {run['run_id']!r} references an absent file")
    expected_digest = _manifest_digest(document)
    if document["manifest_digest"] != expected_digest:
        errors.append("manifest_digest does not match the non-circular identity material")
    expected_id = "export-" + expected_digest.removeprefix("sha256:")[:32]
    if document["export_id"] != expected_id:
        errors.append("export_id does not match manifest_digest")
    return errors


def canonicalize_export_manifest_v0(document: object) -> bytes:
    try:
        snapshot = deepcopy(document)
    except Exception as error:
        raise ExportValidationError("manifest could not be snapshotted") from error
    try:
        schema_errors = sorted(
            Draft202012Validator(_schema(), format_checker=FormatChecker()).iter_errors(snapshot),
            key=lambda item: tuple(str(part) for part in item.absolute_path),
        )
    except ExportValidationError:
        raise
    except Exception as error:
        raise ExportValidationError("manifest schema validation failed") from error
    if schema_errors:
        raise ExportValidationError([error.message for error in schema_errors])
    if not isinstance(snapshot, dict):
        raise ExportValidationError("manifest must be an object")
    semantic_errors = _semantic_errors(cast(dict[str, object], snapshot))
    if semantic_errors:
        raise ExportValidationError(semantic_errors)
    try:
        return rfc8785.dumps(cast(JsonValue, snapshot))
    except (rfc8785.CanonicalizationError, TypeError) as error:
        raise ExportValidationError("manifest is not RFC 8785 representable") from error


def export_manifest_v0(document: object) -> ExportManifest:
    if not isinstance(document, dict):
        raise ExportValidationError("manifest must be an object")
    try:
        snapshot = deepcopy(document)
        snapshot.pop("manifest_digest", None)
        snapshot.pop("export_id", None)
        snapshot["manifest_digest"] = _manifest_digest(cast(dict[str, object], snapshot))
        snapshot["export_id"] = "export-" + cast(str, snapshot["manifest_digest"])[7:39]
    except ExportValidationError:
        raise
    except Exception as error:
        raise ExportValidationError("manifest identity could not be constructed") from error
    canonical = canonicalize_export_manifest_v0(snapshot)
    value = object.__new__(ExportManifest)
    object.__setattr__(value, "canonical_bytes", canonical)
    object.__setattr__(value, "digest", cast(str, snapshot["manifest_digest"]))
    return value


def loads_export_manifest_v0(data: str | bytes) -> ExportManifest:
    document = _parse_json(data, "Export Manifest JSON")
    canonical = canonicalize_export_manifest_v0(document)
    value = object.__new__(ExportManifest)
    object.__setattr__(value, "canonical_bytes", canonical)
    object.__setattr__(
        value, "digest", cast(str, cast(dict[str, object], document)["manifest_digest"])
    )
    return value


def _safe_path(value: str) -> None:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ExportValidationError("bundle contains an unsafe logical path")
    path = PurePosixPath(value)
    if path.is_absolute() or value.startswith("//") or re.match(r"^[A-Za-z]:", value):
        raise ExportValidationError("bundle contains an absolute logical path")
    if any(part in {"", ".", ".."} for part in path.parts) or path.as_posix() != value:
        raise ExportValidationError("bundle contains a non-normalized logical path")


def checksum_index(files_by_path: Mapping[str, bytes]) -> bytes:
    if not isinstance(files_by_path, Mapping):
        raise ExportValidationError("checksum input must be a byte mapping")
    lines = []
    try:
        for path in sorted(files_by_path):
            _safe_path(path)
            data = files_by_path[path]
            if not isinstance(data, bytes):
                raise ExportValidationError("checksum input values must be bytes")
            lines.append(f"{hashlib.sha256(data).hexdigest()}  {path}\n")
    except ExportValidationError:
        raise
    except Exception as error:
        raise ExportValidationError("checksum input could not be read") from error
    return "".join(lines).encode("ascii")


def parse_checksum_index(data: bytes) -> dict[str, str]:
    if not isinstance(data, bytes):
        raise ExportValidationError("checksum index must be bytes")
    if len(data) > _MAX_MANIFEST_BYTES:
        raise ExportValidationError("checksum index exceeds the v0 size limit")
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError as error:
        raise ExportValidationError("checksum index is not ASCII") from error
    result: dict[str, str] = {}
    previous = ""
    for line in text.splitlines(keepends=True):
        if not line.endswith("\n"):
            raise ExportValidationError("checksum index must use complete LF-terminated lines")
        match = _CHECKSUM_RE.fullmatch(line[:-1])
        if match is None:
            raise ExportValidationError("checksum index has a malformed line")
        digest, path = match.groups()
        _safe_path(path)
        if path in result:
            raise ExportValidationError("checksum index contains a duplicate path")
        if path <= previous:
            raise ExportValidationError("checksum index is not in lexical path order")
        result[path] = "sha256:" + digest
        previous = path
    return result


def read_bundle_directory(directory: str | Path) -> dict[str, bytes]:
    root = Path(directory)
    try:
        if root.is_symlink() or not root.is_dir():
            raise ExportValidationError("bundle path must be a non-symlink directory")
        result: dict[str, bytes] = {}
        total = 0
        for item in root.rglob("*"):
            mode = item.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise ExportValidationError("bundle directories and files must not be symlinks")
            if item.is_dir():
                continue
            if not stat.S_ISREG(mode):
                raise ExportValidationError("bundle contains a non-regular file")
            if len(result) >= _MAX_BUNDLE_FILES:
                raise ExportValidationError("bundle exceeds the v0 file-count limit")
            logical = item.relative_to(root).as_posix()
            _safe_path(logical)
            size = item.stat().st_size
            if size < 0 or size > _MAX_FILE_BYTES or total + size > _MAX_BUNDLE_BYTES:
                raise ExportValidationError("bundle exceeds the v0 size limit")
            data = item.read_bytes()
            total += len(data)
            if total > _MAX_BUNDLE_BYTES:
                raise ExportValidationError("bundle exceeds the v0 size limit")
            result[logical] = data
        return result
    except ExportValidationError:
        raise
    except OSError as error:
        raise ExportValidationError("bundle directory could not be read safely") from error
