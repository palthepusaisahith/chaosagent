# OpenTelemetry instrumentation v0

Issue #19 adds backend-neutral operational traces and metrics around the
existing Run, provider, Tool Gateway, fault, and deterministic-evaluator
boundaries. PostgreSQL Run Events, state evidence, effect rows, evaluation
results, and export bundles remain the authoritative product record.
OpenTelemetry data may be sampled, delayed, duplicated, truncated, dropped, or
unavailable and must never be used for authorization, fencing, replay,
evaluation, or recovery decisions.

## Bootstrap and deployment

Telemetry is disabled by default. A deployable process opts in explicitly:

```python
from chaosagent_telemetry import configure_telemetry

installation = configure_telemetry()
```

Configuration uses:

- `CHAOSAGENT_TELEMETRY_MODE=disabled|otlp` (default `disabled`)
- `OTEL_SERVICE_NAME` (default `chaosagent`)
- `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://127.0.0.1:4317`)
- `OTEL_EXPORTER_OTLP_INSECURE=true|false` (default `true` for local use)

Call `installation.shutdown()` during graceful process shutdown when an
installation was returned. The example
[`deploy/otel/collector-config.yaml`](../../deploy/otel/collector-config.yaml)
accepts OTLP on loopback and writes debug output. It is disposable development
configuration, not production deployment or a hosted backend. Production
operators own collector authentication, TLS, sampling, processors, exporters,
retention, and access control.

The bootstrap installs providers only in the ChaosAgent facade; it does not
replace process-global OpenTelemetry providers owned by a host application. The
production bootstrap always uses `BatchSpanProcessor` and
`PeriodicExportingMetricReader`, so network export completion is never required
for a product transaction to succeed. Arbitrary provider injection is available
only from the explicitly test-only `chaosagent_telemetry.testing` module.
Collector/exporter failures are isolated from domain execution. Invalid startup
configuration raises a sanitized error before work starts.

ChaosAgent permits one owned live installation per process. A second enabled
configuration attempt fails deterministically and retires the newly constructed
provider pair without disturbing the current installation. `shutdown()` is
idempotent, retires only the providers owned by that installation, disables the
facade, and permits a later fresh configuration. Selecting disabled mode also
retires the current ChaosAgent-owned installation. It never shuts down a host
application's global providers.

## Trace model

Stable `chaosagent.*` spans are:

- `chaosagent.run.execute`: one worker execution attempt, with Run ID, worker,
  lease attempt, and adapter identity;
- `chaosagent.model.invoke`: one provider invocation and agent step;
- `chaosagent.tool.execute`: one physical Gateway attempt, with tool contract,
  capability, logical call, and physical attempt number;
- `chaosagent.fault.match` and `chaosagent.fault.apply`: selection and
  application boundaries, separate from authoritative fault evidence; and
- `chaosagent.evaluator.execute`: one fenced deterministic evaluation, with gate
  outcomes represented as span events.

The telemetry package contains the isolated mapping to the evolving OTel GenAI
vocabulary. Model spans use `gen_ai.operation.name`, provider/model identity,
and token-usage attributes when the already-validated adapter output supplies
them. Tool spans use the standard operation, tool-name, and tool-type
attributes. ChaosAgent's versioned attributes carry the domain correlation that
has no stable upstream equivalent. Content-bearing GenAI attributes are never
populated.

Ambient W3C context establishes ordinary parent/child relationships within one
execution. A Run can resume on another worker or process, so v0 does **not**
hold a span open across leases and does not fabricate a single continuous parent
chain. Run ID and lease attempt correlate those honest attempt spans. Future
service transports can propagate W3C headers through their own boundary.

When a Run Event is created under an active, recording ChaosAgent span, its
already-versioned optional `trace_context` contains lowercase 32-hex `trace_id`
and 16-hex `span_id`. This is a diagnostic pointer only. It is not part of the
event payload digest, does not replace `correlation_id` or `causation_event_id`,
and is never trusted when replaying evidence. No Run Event v0 change or database
migration was required because this strict field was committed in Issue #4.
Non-recording, unsampled, invalid, zero-ID, or ended contexts never become
persisted correlation. If SDK context restoration fails, ChaosAgent's owned
context marker still prevents an ended Run span from correlating a later Run; a
legitimate recording host span remains eligible as an external parent.

## Privacy and content policy

V0 has no content-capture mode. The instrumentation does not record prompts,
model responses, tool arguments/results, fixture rows, approval comments,
database statements, arbitrary exception messages, credentials, headers, or
environment-variable values. Operational attributes contain bounded state,
catalog identifiers, revision/digest references, Run/logical-call identifiers,
and timing only. IDs can still be sensitive operational metadata; protect the
collector and backend accordingly.

Adding content capture later requires an explicit opt-in contract, redaction
policy, size limits, security review, and tests. Standard GenAI conventions are
still developing, so provider-specific mapping remains isolated behind this
package rather than frozen into product evidence.

## Metrics and cardinality

Counters and duration histograms cover Run attempts, model invocations, tool
attempts, fault decisions/applications/observations, evaluator executions, and
critical-gate outcomes. Metric dimensions are fixed allowlists: normalized
outcomes, known provider family, the four V1 tools, read/mutation capability,
approved fault kind/phase, and the single `critical` gate family. Unknown values
collapse to `other`. Run IDs, event IDs, Scenario IDs, logical calls, fault IDs,
model names, exception text, and other unbounded values are never metric labels.

Spans may contain identifiers needed to correlate one operation, because span
attributes are not metric dimensions. Dashboards should aggregate metrics and
use traces for individual investigations.

## Failure semantics and limitations

All instrumentation operations catch ordinary SDK/exporter failures at the
telemetry boundary. They do not catch `BaseException`, alter returned domain
values, commit transactions, add retries, or participate in lifecycle locks.
Span export occurs outside product correctness decisions. With telemetry
disabled, event documents remain semantically equivalent to the pre-Issue-#19
form because no trace field is injected.

Fault metrics describe operational matching/application attempts. Because
telemetry is deliberately non-authoritative, a metric can outlive a product
transaction that later rolls back; durable Run Events remain the source of
truth. The included Collector configuration was statically checked, but v0 does
not pin or execute a Collector binary as part of repository verification.

V0 intentionally does not add structured operational logging, tail sampling,
trace queries, a telemetry UI, REST/SSE propagation, alerting, deployment
orchestration, or backend-specific dashboards. Those belong to later roadmap or
operator work.
