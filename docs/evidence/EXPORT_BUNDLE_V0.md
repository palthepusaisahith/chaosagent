# Export Bundle v0

Export Bundle v0 is a portable, closed-set representation of one terminal Run or
one authoritative Campaign. It is evidence infrastructure, not storage, an API,
telemetry, or a replacement for Run Report v0.

## Layout

Every directory and deterministic ZIP contains `manifest.json`,
`checksums.sha256`, and only the payload paths listed by the manifest. Run
directories use the first 24 hexadecimal characters of `SHA-256(run_id)` rather
than a caller-controlled identifier as a filename.

```text
manifest.json
checksums.sha256
provenance/scenario.json
provenance/agent_configuration.json       # only when its v0 document is available
runs/<run-key>/events.jsonl
runs/<run-key>/report.json                 # only when persisted
runs/<run-key>/evaluation/results.jsonl    # completed evaluator Runs
runs/<run-key>/evaluation/ground-truths.jsonl
campaign/plan.json                         # Campaign bundles only
campaign/statistics.json                   # Campaign bundles only
```

Paths are normalized POSIX-relative paths. Absolute, drive-qualified, UNC,
backslash-containing, empty-segment, `.` and `..` paths are invalid. Directory
validation rejects symbolic links, non-regular files, missing files, and
unlisted files. Directory writing refuses an existing destination. ZIP entries
have lexical ordering, a 1980-01-01 timestamp, no ownership data, and a fixed
regular-file mode.

## Authority and identity

Persisted Run events, Scenario revisions, Agent Configuration revisions, and
Campaign plans are authoritative inputs. Run Reports, Evaluation Results,
Campaign Statistics, and redacted representations are derived summaries of
authenticated evidence. Manifest metadata describes this classification per
file. A missing Agent Configuration document is represented as `unavailable`;
its frozen `{id, revision, digest}` is still retained. No missing value is
fabricated.

`manifest_digest` is SHA-256 over RFC 8785 serialization after removing
`manifest_digest`, `export_id`, and `exported_at`. `export_id` is `export-` plus
the first 32 hex characters of that digest. Thus the export occurrence timestamp
is auditable but does not change the reproducibility identity. Every other
manifest field, including payload checksums and provenance, participates.

`checksums.sha256` contains lexically ordered GNU-style SHA-256 lines for
`manifest.json` and every listed payload file. It excludes itself, avoiding a
checksum cycle. The manifest lists payload files but not the two fixed
administrative files. A payload entry records exact byte length and SHA-256
independently of the checksum index.

JSON files are RFC 8785 bytes. JSONL files contain one canonical contract per
LF-terminated line. Events remain in authoritative sequence order; gaps,
duplicates, reordering, wrong Run IDs, and duplicate event IDs fail closed.
Evaluations are ordered by evaluation identity. Campaign Runs are ordered by Run
identity in the manifest while their plan retains authoritative trial ordering.

## Snapshot rule

The database exporters own a short PostgreSQL `REPEATABLE READ`, `READ ONLY`
transaction. They accept only terminal Runs; Campaign members must all be
terminal and evaluation-complete. This prevents mixing records observed at
different database snapshots and does not acquire or mutate a lease.
Partial/nonterminal exports are intentionally unsupported in v0.

## Redaction

Redaction is explicit and deterministic. Each rule names a logical file role and
an exact JSON Pointer; replacement is always `[REDACTED]`. Rules may replace
string leaves only. Identity, revision, digest, Run/event/evaluation IDs, and
evidence-boundary fields cannot be redacted. A rule that matches nothing fails
export. Modified contracts are revalidated, and event payload digests are
recomputed for the derived representation. Campaign bundles require unredacted
Scenario and Run Event files so their fault-observation chains remain
independently authenticatable; Run bundles may redact those roles.

Redacted files are labeled `derived`, include the SHA-256 of their unredacted
source bytes, and get checksums for their actual redacted bytes. Persisted data
is never changed. The source digest identifies only the original bytes claimed
by the manifest. Without the original bytes or a future publisher signature or
PKI, offline validation cannot prove that the undisclosed source had that
content.

## Offline validation

`validate_export_bundle()` accepts an immutable in-memory bundle, a byte
mapping, or a directory and returns a structured `ValidationResult`. It needs no
database. Validation covers the manifest schema and identity, closed file set,
paths, byte lengths, both checksum layers, canonical JSON or JSONL, all existing
semantic contract loaders, Run/Scenario/config/report/evaluator relationships,
event evidence references, Campaign plan membership, and Campaign statistics
identity. `validate_export_bundle_or_raise()` provides the same checks with a
sanitized `ExportValidationError` boundary.

The public boundary rejects duplicate JSON keys, non-finite numbers, deep
malformed JSON, unsafe paths, checksum ambiguity, oversized bundles, and
filesystem errors without exposing raw parser or filesystem exceptions. It never
executes bundle content.

## Limitations and deferred work

- No migration or export registry is introduced. Bundles are generated on
  demand.
- Campaign comparison export is omitted in v0. The single-Campaign bundle shape
  cannot authenticate both comparison sides; a future format may include both
  authoritative Campaign cohorts in one coherent snapshot.
- Export signatures, PKI, remote/object storage, retention automation, and
  background workers are deferred.
- Exact external dependency/container/SBOM provenance can be supplied later
  through compatible application metadata; unavailable values are omitted.
- Raw provider/model bodies are not persisted by current contracts and therefore
  are not exported.
- Issue #19 owns OpenTelemetry. Issue #20 owns REST/SSE. Issue #21 owns the
  dashboard/download UI. Issue #22 owns sandbox/container hardening.
