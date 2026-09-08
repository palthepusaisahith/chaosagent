# Dashboard flagship flow

The ChaosAgent dashboard presents the Issue #20 control plane as a reliability
control room. It does not implement a second evaluator, event store, Run state
machine, or Campaign authority.

## Local development

Start PostgreSQL, apply migrations through `0010`, and run the control plane as
described in `docs/control-plane/CONTROL_PLANE_V1.md`. Then use the repository's
locked Node 22 and pnpm 10 toolchain:

```shell
pnpm install --frozen-lockfile
pnpm --filter @chaosagent/web dev
```

The Vite server runs at `http://127.0.0.1:5173` and proxies `/api` to
`http://127.0.0.1:8000`. `VITE_CONTROL_PLANE_URL` may identify another API
origin when that origin is explicitly allowed by control-plane CORS. No
credential is stored in browser storage.

## Workflow

- **Dashboard** explains the ambiguous-refund experiment and summarizes only
  Runs loaded or created in the current browser session. Issue #20 has no global
  Run-list endpoint, so the UI does not invent one.
- **Runs** accepts the exact Scenario and Agent Configuration IDs, revisions,
  and digests required by `POST /api/v1/runs`, or opens a known persisted Run.
- **Run detail** loads the current Run, every persisted event page, the report
  or evaluation result, and approvals. It starts SSE from the last opaque event
  cursor, re-reads authoritative Run state across that handoff, and drains the
  remaining REST evidence before closing a terminal stream. Events render in
  authoritative sequence order.
- **Campaigns** opens an exact Campaign, obtains Issue #17 statistics, links to
  member Runs, and requests the existing baseline-versus-faulted comparison.

The flagship causal strip appears only when matching evidence exists. It keeps
the committed refund distinct from the deliberately hidden acknowledgement and
shows exactly-once success only when the evaluator gate and authoritative
`refund.created` evidence agree within the evaluator's frozen evidence cutoff.
Request, effect, fault, result, and retry steps must share their persisted
causal identities. Missing steps remain explicitly unobserved.

## Live updates and evidence authority

Page hydration uses REST before SSE starts. The final event cursor is supplied
as the stream's `cursor` query parameter, which has the same replay semantics as
`Last-Event-ID`. The browser retains the last server cursor, deduplicates by
Event ID and sequence, and resumes with bounded exponential backoff after an
error. A terminal lifecycle event triggers a final REST reconciliation and
evidence drain before the stream closes. Keepalive comments do not create
product events. Human approval actions require an explicit responder ID; the
browser does not invent an authenticated identity.

The primary timeline and raw inspector are bounded to the latest 400 events; the
client still hydrates the complete persisted replay for evidence-derived
summaries. Unmounting closes EventSource and cancels pending reconnects.

PostgreSQL and validated Issue #3–#20 contracts remain authoritative. Browser
state, visual summaries, and SSE delivery are projections only. Raw prompt,
tool, and error strings render as inert React text; the UI uses no untrusted
HTML and does not expose database errors, lease tokens, provider secrets, or
environment configuration.

## Deferred

Issue #21 does not add Run or revision collection endpoints, a worker daemon,
frontend authentication, a mock production data plane, or a new migration. Raw
evidence abuse coverage and the process-level capability boundary are described
in
[`EXECUTION_TRUST_BOUNDARY_V1.md`](../security/EXECUTION_TRUST_BOUNDARY_V1.md).
