# Execution trust boundary v1

ChaosAgent uses a capability-restricted execution boundary. An agent can ask for
only the exact, versioned synthetic tools supplied in its immutable Scenario. It
is not given direct shell, Python, filesystem, network, database, environment,
Docker, or control-plane capabilities.

This is a description of implemented V1 controls, not a claim of complete
isolation. The provider-neutral loop and Tool Gateway currently execute as
trusted application code in the service process. There is no OS/container
boundary around that code in this repository. Consequently, arbitrary Python
adapters, arbitrary tools, user images, plugins, and remote MCP servers are not
supported. Adding any of them requires a separate isolation design and security
review; Docker alone is not a hostile-multitenant boundary.

## Authorities and untrusted data

Authority-bearing inputs are committed application code; PostgreSQL records
validated through their immutable contract loaders and relational bindings;
frozen Scenario, Fixture, Policy, Agent Configuration, and Ground Truth
revisions; current lease/fencing state; durable Policy/Approval records; and
server-created event, evaluation, report, cursor, and export identities. Trusted
operator configuration selects the hosted provider and supplies its credential.

Scenario-authored prose, synthetic customer/order/support text, fault payloads,
agent tool arguments, provider text and tool-call JSON, tool-result text, REST
identifiers and request bodies, raw evidence, and exported content are
untrusted. They remain data even when they resemble instructions, HTML, shell
commands, events, approvals, or evaluator results.

Applicable abuse classes are prompt and authority injection, tool-name
confusion, malformed JSON/schema input, duplicate/replay abuse, cross-Run
confusion, stale-worker writes, archive/path traversal, HTML/SSE/log injection,
secret leakage, and bounded resource exhaustion. Arbitrary URL SSRF, host mount
escape, Docker-socket abuse, and arbitrary-code escape are not reachable agent
features in V1 because no such capabilities are registered. They become
applicable if those excluded features are introduced.

## Capability and provider boundary

The runtime derives its tool specifications from the frozen Scenario and the
immutable in-repo registry. Dispatch uses the exact pair
`(tool_id, contract_version)`; there is no fuzzy lookup, case folding, dotted
attribute resolution, dynamic import, `eval`, `exec`, or shell interpolation.
The Gateway validates the call envelope and strict JSON Schema before acquiring
business authority. It then verifies Scenario authorization, Policy/Approval
binding, the current Run row, lease token, attempt generation, and database-time
expiry. Read handlers receive only a Run-bound read interface. Mutation handlers
produce pure intents; the Gateway owns the effect transaction.

The hosted-provider adapter is trusted code talking to an operator-configured
endpoint, but every provider response is untrusted. V1 bounds a response to 256
output items, 256 content items per message, 100,000 text characters, and 65,536
characters per tool-argument JSON document before runtime persistence. Duplicate
JSON keys, non-finite constants, malformed/deeply recursive JSON, unknown
function aliases, and oversized argument documents fail with sanitized provider
errors. The runtime then applies the authoritative Scenario tool schemas and its
128-tool-call step limit. Provider-created IDs, text, and JSON cannot select a
Run, Policy, Approval, event type, effect, evaluation, or terminal state.

Only the hosted-provider adapter has outbound-network behavior. Its base URL and
credential are operator configuration, not agent arguments. No V1 tool accepts a
URL and no generic fetcher exists, so agent-directed SSRF is not applicable. The
provider credential is neither placed in Scenario/Agent Configuration documents
nor product evidence. Paid network calls are disabled in tests.

## Durable authority and replay

Synthetic-company reads and writes include `run_id` in their repository
predicates. Effects, idempotency records, approvals, recovery markers, events,
and evaluations are revalidated against the same Run and frozen revisions.
Current lease ownership is checked under the Run lock with PostgreSQL time.
Stale tokens/attempts cannot append evidence or effects. Same-Run same-key
replay returns the durable result; a key bound to changed arguments conflicts;
equal keys in different Runs do not share authority.

Policy decisions and approvals are server-generated evidence with deterministic
identities and complete Run/Scenario/Policy/tool/request bindings. Text such as
`"approved": true` is not an approval. Business invariants are rechecked at
execution time, including after human approval. Evaluators authenticate the
event and state-evidence chain at a frozen sequence boundary; provider text that
says `PASS` or resembles `fault.applied` has no evaluator authority.

## Structured content, exports, and presentation

Event envelopes and event types are created by trusted code. Untrusted content
is nested inside typed payload fields and serialized with JSON. SSE uses exactly
one JSON `data:` line, so newline, NUL, HTML, and SSE-looking payload text stays
escaped and cannot create another frame. Operational telemetry uses bounded,
low-cardinality identifiers and never records raw prompts, tool arguments,
outputs, approval comments, provider responses, credentials, or database URLs.

Export logical paths are normalized relative POSIX paths. Absolute, drive/UNC,
backslash, NUL, empty, dot, and parent paths fail closed. Bundle readers reject
symlinks and non-regular files. File count, individual file size, aggregate
size, manifest size, checksums, canonical bytes, roles, and cross-document
bindings are verified before an offline bundle is accepted. Export text remains
data. The React dashboard renders raw evidence through React text nodes and
`JSON.stringify`; it does not use `dangerouslySetInnerHTML`. The raw inspector
is also capped at the newest 400 events.

## Resource limits and failure behavior

- The control plane rejects fixed or streamed request bodies above 1 MiB by
  default; operator configuration is constrained to 1 KiB–16 MiB.
- IDs, cursors, page sizes, Scenario collections, tool arguments, provider
  output, runtime steps/tool calls/time/cost, fault occurrences and delays,
  Campaign trials, SSE polling/page settings, and export inputs have explicit
  V0/V1 bounds at their authoritative layer.
- Limits reject input; identity or evidence is never silently truncated.
- Contract/parser failures use stable sanitized errors. Public errors omit SQL,
  driver details, stack traces, host paths, environment values, and secrets.
- Fault delays consume the existing wall-time/lease budget and are fenced again
  before authoritative post-delay evidence can commit.

## Abuse coverage

The following matrix identifies the primary regression proof. Several rows rely
on mature tests from earlier issues rather than duplicating them.

| Category                      | Applicable | Primary regression proof                                                                           |
| ----------------------------- | ---------- | -------------------------------------------------------------------------------------------------- |
| Unknown tool/capability       | Yes        | `test_adversarial_capability_names_fail_before_database_access`                                    |
| Malformed mutation arguments  | Yes        | `test_mutation_contracts_are_strict_and_use_integer_money`; Gateway PostgreSQL business-rule tests |
| Prompt injection remains data | Yes        | `test_prompt_injection_and_authority_spoofs_remain_provider_text`; frontend inert-evidence test    |
| Authoritative-event spoof     | Yes        | provider-text test plus Event/evaluator stream-validation tests                                    |
| Evaluator/PASS spoof          | Yes        | provider-text test and evaluator provenance/corruption tests                                       |
| Policy/Approval spoof         | Yes        | Issue #10 approval identity, provenance, cross-Run, stale, and TOCTOU tests                        |
| Cross-Run state/effects       | Yes        | `test_mutation_is_run_isolated_and_stale_worker_cannot_apply_after_reclaim`                        |
| Stale lease/fencing           | Yes        | Issue #6 lifecycle races and Gateway/runtime stale-worker tests                                    |
| Exactly-once replay           | Yes        | Issue #9/#15 concurrent and post-commit recovery tests                                             |
| Export path traversal         | Yes        | `test_unsafe_manifest_paths_are_rejected` and directory/symlink tests                              |
| Oversized/complex input       | Yes        | body-limit, export-limit, runtime-limit, and provider-bound tests                                  |
| Malformed/tampered API cursor | Yes        | `test_malformed_cursors_fail_closed` and PostgreSQL foreign-cursor test                            |
| SSE payload injection         | Yes        | `test_sse_payload_control_text_cannot_forge_frames`                                                |
| Provider malformed tool call  | Yes        | `test_provider_tool_argument_abuse_is_bounded_and_sanitized`                                       |
| Provider unknown tool call    | Yes        | `test_malformed_or_unauthorized_provider_output_fails_closed`                                      |
| Secret/error sanitization     | Yes        | provider, runtime, telemetry, control-plane, and export error tests                                |
| Malicious frontend evidence   | Yes        | `renders hostile raw evidence as inert text without executable DOM`                                |
| Tampered export bundle        | Yes        | `test_tampering_fails_closed` and manifest projection tests                                        |
| Timeout/delay abuse           | Yes        | fault-delay budget, post-delay fencing, and runtime timeout tests                                  |
| Unicode/control characters    | Yes        | exact tool-name test, SSE framing test, JCS Unicode tests, JSONB persistence profile               |
| Arbitrary URL/SSRF            | No         | no URL-bearing V1 agent capability exists                                                          |
| Host mount/socket escape      | No         | no agent-controlled container/image/mount/socket surface exists                                    |

## Known limitations

The current boundary prevents an untrusted model from selecting ambient process
capabilities, but trusted application code still shares a process. It is not a
defense against a malicious committed adapter/tool or a Python supply-chain
compromise. Provider traffic is not mediated by a repository-owned egress proxy.
Authentication/tenant isolation remains an explicitly documented control-plane
deployment integration point. PostgreSQL, provider, and telemetry transports
still depend on operator TLS/network configuration. Resource limits mitigate,
but cannot eliminate, CPU/memory pressure inside trusted parsers and libraries.
