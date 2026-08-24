# Scenario Schema v0

Scenario v0 is the immutable, provider-neutral contract for stable ChaosAgent
experiment semantics. It describes what a scenario means. It does not describe
one invocation of that scenario.

The authoritative Draft 2020-12 JSON Schema is bundled at
`packages/scenarios/src/chaosagent_scenarios/schema/scenario-v0.schema.json`.
The shipment/refund file under `benchmarks/` is a structurally valid template
whose external references are deliberately unresolved.

## Scenario and Campaign boundary

Scenario v0 owns the fixture, task and instructions, tool/capability allowlist,
policy revision, named fault definitions, budgets, and expected ground-truth
references that define the experiment.

Scenario v0 does **not** own an agent-configuration revision, campaign seed,
trial count, baseline/faulted variants, pairing strategy, or derived trial
seeds. A later Campaign contract will freeze a Scenario revision and an Agent
Configuration revision, select fault definitions, and define invocation and
pairing details. No Campaign schema or behavior is implemented by Issue #3.

Changing Campaign data therefore will not change Scenario identity.

## Contract fields

- `schema_version` is exactly `chaosagent.scenario/v0`. Generic loaders dispatch
  on this value and reject unknown versions.
- `scenario_id` and `revision` identify an authored Scenario revision. Storage
  immutability is deferred; the Scenario digest covers the validated document
  itself.
- `metadata` contains a title, description, and optional tags.
- `fixture` is a structurally validated `{id, revision, digest}` reference.
- `agent.task` is the user goal, and `agent.instructions` is its ordered
  instruction sequence. `allowed_tools` and `capabilities` are allowlists, not
  agent-provider configuration.
- `policy` is a content-addressed policy revision reference. Scenario v0 does
  not provide arbitrary scenario-local policy overrides or define a policy
  language.
- `faults` is an unordered set of uniquely named declarative fault definitions
  available for selection by a future Campaign. V0 defines their JSON shape,
  supported kinds, match fields, phase vocabulary, activation parameters, and
  kind-specific parameter shapes. It does not select, compile, match, or execute
  them.
- `budgets` sets hard maximum steps, tool calls, wall time, and cost. Cost is an
  integer number of micro-US dollars to avoid currency rounding ambiguity.
- `expected_outcomes` is an unordered set of content-addressed ground-truth
  references. Evaluator definitions and execution are deferred.

All objects reject unknown properties. String and collection sizes, numeric
ranges, enum values, reference shapes, the phase vocabulary, and kind-specific
fault parameter shapes are bounded by the JSON Schema.

## Closed V0 tool vocabulary

Scenario v0 deliberately recognizes only the four tools approved for the V1
fake-company domain:

- `orders.get`
- `shipping.get_status`
- `payments.refund`
- `support.update_ticket`

The Scenario loader knows only these identifiers and whether a fault target is
present in the scenario's allowlist. It does not classify tools as mutable,
read-only, risky, idempotent, or eligible for a particular runtime behavior.

Expanding this closed vocabulary requires either a new Scenario schema version
or a future architecture decision to validate tool identifiers and capabilities
against a versioned catalog. Issue #3 does not implement that catalog.

## Validation responsibilities

The JSON Schema is authoritative for document structure, closed vocabularies,
field constraints, duplicate primitive set members, and kind-specific fault
parameter shapes.

The versioned Python V0 validator adds only invariants that standard Draft
2020-12 cannot express portably:

- logical fault IDs and expected-outcome IDs are unique; and
- every fault target is present in `agent.allowed_tools`.

It does not infer mutation behavior, interpret call ordinals or activation caps,
restrict fault kinds to particular runtime phases, reject unselected fault
definitions, or apply future fault-matcher rules.

Duplicate JSON object keys are rejected by the JSON loading APIs before schema
validation. A dictionary supplied directly to an API has already lost any
duplicate-key information and cannot be checked for it retroactively.

## Reference semantics

The fixture, policy, and expected-outcome reference shape is:

```json
{
  "id": "resource-family-id",
  "revision": "revision-id",
  "digest": "sha256:<64 lowercase hexadecimal characters>"
}
```

Issue #3 validates only this structure and digest syntax. It does not assert
that an external revision exists, resolve it, fetch its content, or verify that
the supplied digest matches that content. Those operations require future
revision contracts, catalogs, and stores. Scenario v0 intentionally does not
invent canonicalization rules for those not-yet-defined resource types.

The benchmark template uses an all-zero SHA-256 sentinel for every unresolved
external reference. That sentinel is not a verified content digest.

## Ordered and set-like arrays

Scenario v0 classifies every contract-owned array explicitly:

| Array                 | Semantics                       | Canonical handling                                        |
| --------------------- | ------------------------------- | --------------------------------------------------------- |
| `metadata.tags`       | Set-like                        | Reject duplicates; sort by each item's JCS bytes          |
| `agent.instructions`  | Ordered                         | Preserve authored order                                   |
| `agent.allowed_tools` | Set-like                        | Reject duplicates; sort by JCS bytes                      |
| `agent.capabilities`  | Set-like                        | Reject duplicates; sort by JCS bytes                      |
| `faults`              | Set-like, keyed by fault ID     | Reject duplicate IDs; sort by each definition's JCS bytes |
| `expected_outcomes`   | Set-like, keyed by reference ID | Reject duplicate IDs; sort by each reference's JCS bytes  |

Arrays nested inside a fault payload value are not contract-owned collections
and retain their authored order. Scenario v0 never sorts arrays globally.

The schema includes `x-chaosagent-array-semantics` annotations so another
implementation can reproduce these rules without inferring them from Python
code.

## Canonical representation and digest

Loading and canonicalization perform one logical pipeline:

1. Parse JSON while rejecting duplicate object keys and non-finite constants.
2. Take a defensive snapshot for direct in-memory API inputs.
3. Validate that exact snapshot with the V0 JSON Schema and V0 semantic rules.
4. Sort only the declared set-like arrays in the private validated snapshot.
5. Serialize it using RFC 8785 JSON Canonicalization Scheme (JCS).
6. Compute lowercase SHA-256 over those canonical UTF-8 bytes.

The digest format is:

```text
sha256:<64 lowercase hexadecimal characters>
```

The Scenario digest covers every Scenario v0 field after the explicitly defined
set normalization. It does not cover external referenced content, a Campaign, an
Agent Configuration, or values supplied later by a runner.

JCS removes insignificant whitespace, sorts object keys, canonicalizes JSON
escaping, and serializes numbers in its interoperable IEEE-754 domain.
Equivalent exponent spellings and negative zero/zero therefore canonicalize as
JCS defines. Integers outside JCS's exact interoperable range are rejected.

Canonicalization does not trim strings, change case, normalize Unicode, add
defaults, resolve references, or mutate the caller's document. Canonically
equivalent Unicode escape spellings represent the same parsed string, while
canonically composed and decomposed Unicode strings remain distinct because V0
does not apply Unicode normalization.

## Loading and immutability

Generic APIs (`load_scenario`, `loads_scenario`, `validate_scenario`,
`canonicalize_scenario`, and `digest_scenario`) inspect `schema_version` and
dispatch only to a registered implementation. Unknown versions fail closed.

Explicit V0 APIs have a `_v0` suffix, including `load_scenario_v0`,
`validate_scenario_v0`, and `scenario_schema_v0`. The generic `scenario_schema`
function requires an explicit version argument. This prevents a future v1 from
silently changing what an existing unversioned call means.

Loading returns a frozen `Scenario` whose constructor is not public. Canonical
bytes and the digest are produced together from the same validated snapshot.
`Scenario.to_dict()` reparses those immutable bytes and returns a fresh deep
copy, so mutations cannot affect the loaded Scenario.

## Versioning policy

V0's schema, semantic invariants, array classifications, and canonicalization
rules form one frozen validation profile. A change to field meaning, required
fields, accepted values, semantic invariants, set/ordered classification, or
canonical identity requires a new schema version and `$id`.

A future v1 must add a separate schema and implementation and register it with
the dispatcher. It must not modify V0 behavior or reinterpret stored V0 bytes.
No v1 or compatibility reader is implemented here.

## Deferred work

Issue #3 does not implement Campaign configuration, revision persistence,
external reference resolution, policy evaluation, a tool catalog, fault
selection/matching/execution, evaluator definitions/execution, events/reports,
agent execution, provider integration, or UI behavior.
