# Control Plane API v1

The ChaosAgent control plane is a thin HTTP adapter over the authoritative
contracts and PostgreSQL services from Issues #3–#19. It does not implement a
second state machine, worker, policy engine, evaluator, statistics engine, or
export format. The API prefix is `/api/v1`; this transport version is
independent of the `v0` versions used by stored domain contracts.

## Running locally

Apply the existing migrations through `0010`, configure a non-production
PostgreSQL database and the Ground Truth revisions referenced by the Scenarios
you will evaluate or export, then start the ASGI factory from the repository
root:

```shell
export CHAOSAGENT_DATABASE_URL='postgresql+psycopg://user:password@127.0.0.1/chaosagent'
export CHAOSAGENT_GROUND_TRUTH_PATHS='benchmarks/shipment-refund/ground-truth/refund-once-and-close-ticket.v0.json'
uv run uvicorn chaosagent_control_plane.app:create_app_from_environment --factory
```

On Windows, separate multiple Ground Truth paths with `;`; Unix systems use `:`,
following the platform path separator. Generated OpenAPI is available at
`/openapi.json` and the framework documentation UI at `/docs`.

Optional configuration is deliberately bounded:

- `CHAOSAGENT_CORS_ORIGINS`: comma-separated explicit origins; empty by default.
- `CHAOSAGENT_SSE_PAGE_SIZE`: 1–1000, default 100.
- `CHAOSAGENT_SSE_POLL_SECONDS`: 0.01–10, default 0.5.
- `CHAOSAGENT_SSE_KEEPALIVE_SECONDS`: 0.1–300, default 15.
- `CHAOSAGENT_MAX_REQUEST_BODY_BYTES`: 1024–16777216, default 1048576.

## Resources and actions

The API exposes process health and PostgreSQL readiness; immutable Scenario and
Agent Configuration revision creation/read; queued Run creation/read and legal
queued cancellation; ordered Run Event reads and streams; deterministic
evaluation or stored Run Report reads; durable approval inspection/resolution;
authoritative Campaign planning/statistics/comparison; and deterministic Run or
Campaign export generation.

Creation retries use their durable identity. An identical Scenario, Agent
Configuration, Run, or Campaign request is naturally repeatable; the same
identity with different immutable content is a conflict. Approval resolution is
repeatable only for the same decision and actor. Cancellation is repeatable once
the Run is cancelled. Export requests include `exported_at`, so a retry with the
same frozen inputs produces the same archive bytes.

Run creation only creates queued work. This service does not claim Runs or run a
worker in an HTTP request.

## Errors

Errors have a bounded, stable envelope:

```json
{ "error": { "code": "run_not_found", "message": "The Run does not exist." } }
```

Request validation uses strict models with unknown fields rejected. The domain
contract loaders remain authoritative for complete document validation. Errors
never include SQL, connection strings, filesystem paths, provider credentials,
lease tokens, stack traces, or raw exception text.

## Event pages and cursors

`GET /api/v1/runs/{run_id}/events` reads immutable events in authoritative
`(run_id, sequence)` order with a bounded `limit`. `next_cursor` is an opaque,
versioned base64url token binding the Run ID, sequence, and globally unique
Event ID. Before use, a cursor is decoded, range-checked, and resolved against
the persisted event. Malformed, unknown, or foreign-Run cursors fail closed.
Pages select `sequence > cursor.sequence`; offsets and timestamps are not replay
positions.

## Replay-safe SSE

`GET /api/v1/runs/{run_id}/events/stream` emits persisted events as:

```text
id: <opaque event cursor>
event: <validated event type>
data: <JSON event document>
```

The standard `Last-Event-ID` header resumes strictly after its persisted event.
An equivalent `cursor` query parameter is available; disagreeing header and
query values are rejected. The implementation repeatedly performs bounded,
ordered PostgreSQL reads. It has no process-local authoritative queue, so a
process restart or an event committed between polls creates neither a gap nor a
duplicate. Rolled-back rows are never visible.

Every poll owns a short session/transaction; no transaction, row lock, or
database connection spans the client connection. Slow clients receive bounded
pages through ASGI backpressure. Disconnects stop subsequent polling. Keepalive
comments contain no `id`, are not product events, and have no evidentiary
meaning.

After delivering all currently committed evidence, the stream closes when the
authoritative Run state is one of the existing terminal states: `completed`,
`failed`, `timed_out`, `cancelled`, or `infra_error`. `evaluating` is active and
does not close the stream.

## Authority, privacy, and security boundaries

PostgreSQL contracts remain product truth. OpenTelemetry data, HTTP request
context, in-memory objects, and SSE delivery state are operational only. The API
does not expose lease tokens, credentials, environment configuration, or
provider secrets, and it never derives evaluation or Campaign outcomes from
telemetry. Export filenames are fixed response headers; clients cannot supply
host paths.

Existing policy, approval, capability, fencing, idempotency, fault, evaluation,
Campaign, and export validation stays in the established domain packages. A
resolved approval still authorizes only its exact durable binding, and mutation
execution still rechecks current business invariants.

User authentication, network perimeter controls, rate limiting, and deployment
authorization are intentionally not implemented in Issue #20. A network-facing
deployment must provide an appropriate authenticated boundary. CORS is disabled
unless explicit origins are configured and is not an authentication mechanism.

## Deferred work

Issue #21 owns the dashboard and frontend SSE client. The final V1 abuse and
capability-boundary model is documented in
[`EXECUTION_TRUST_BOUNDARY_V1.md`](../security/EXECUTION_TRUST_BOUNDARY_V1.md).
This package adds neither a worker daemon nor scheduler, WebSockets,
Redis/Kafka, object storage, export registry, Campaign-comparison bundle, or new
telemetry/evidence authority.
