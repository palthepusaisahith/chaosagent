import { useState, type FormEvent, type ReactNode } from 'react';

import { ControlPlaneApiError } from './api';
import type {
  Approval,
  EvaluationClassification,
  JsonObject,
  ReportResponse,
  Run,
  RunCreateInput,
  RunEvent,
  RunStatus,
} from './domain';
import {
  deriveExactlyOnceProof,
  deriveFlagshipStory,
  eventCategory,
  eventSummary,
  reportClassification,
  reportGates,
} from './presentation';

export function Icon({
  name,
}: {
  name: 'pulse' | 'run' | 'campaign' | 'arrow' | 'shield';
}) {
  const paths = {
    pulse: 'M3 12h4l2-6 4 12 2-6h6',
    run: 'M8 5v14l11-7z',
    campaign: 'M4 18V8m8 10V4m8 14v-7',
    arrow: 'M5 12h14m-5-5 5 5-5 5',
    shield: 'M12 3 5 6v5c0 4.6 3 8.2 7 10 4-1.8 7-5.4 7-10V6z',
  } as const;
  return (
    <svg aria-hidden="true" className="icon" fill="none" viewBox="0 0 24 24">
      <path
        d={paths[name]}
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.8"
      />
    </svg>
  );
}

export function PageHeader({
  eyebrow,
  title,
  children,
}: {
  eyebrow: string;
  title: string;
  children?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
      </div>
      {children}
    </header>
  );
}

export function StatusBadge({
  value,
}: {
  value: RunStatus | EvaluationClassification | string;
}) {
  return (
    <span className={`status status--${value}`}>
      {value.replaceAll('_', ' ').toUpperCase()}
    </span>
  );
}

export function ErrorNotice({ error }: { error: unknown }) {
  const code =
    error instanceof ControlPlaneApiError ? error.code : 'unexpected_error';
  const message =
    error instanceof ControlPlaneApiError
      ? error.message
      : 'The request could not be completed.';
  return (
    <div className="notice notice--error" role="alert">
      <strong>{code.replaceAll('_', ' ')}</strong>
      <span>{message}</span>
    </div>
  );
}

export function LoadingState({
  label = 'Loading authoritative state…',
}: {
  label?: string;
}) {
  return (
    <div className="loading-state" role="status">
      <span className="loader" />
      <span>{label}</span>
    </div>
  );
}

export function EmptyState({ title, body }: { title: string; body: string }) {
  return (
    <div className="empty-state">
      <span className="empty-state__mark">◇</span>
      <h3>{title}</h3>
      <p>{body}</p>
    </div>
  );
}

interface RunFormProps {
  busy: boolean;
  onCreate: (input: RunCreateInput) => Promise<void>;
}

export function RunCreationForm({ busy, onCreate }: RunFormProps) {
  const [error, setError] = useState<unknown>(null);
  const [input, setInput] = useState({
    runId: '',
    scenarioId: 'shipment-refund.ambiguous-timeout',
    scenarioRevision: '1',
    scenarioDigest: '',
    agentId: '',
    agentRevision: '',
    agentDigest: '',
    createdBy: '',
  });
  const field = (key: keyof typeof input) => ({
    value: input[key],
    onChange: (event: React.ChangeEvent<HTMLInputElement>) =>
      setInput((current) => ({ ...current, [key]: event.target.value })),
  });
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setError(null);
    try {
      await onCreate({
        run_id: input.runId,
        scenario: {
          id: input.scenarioId,
          revision: input.scenarioRevision,
          digest: input.scenarioDigest,
        },
        agent_configuration: {
          id: input.agentId,
          revision: input.agentRevision,
          digest: input.agentDigest,
        },
        created_by: input.createdBy,
      });
    } catch (caught) {
      setError(caught);
    }
  };
  return (
    <form className="run-form panel" onSubmit={(event) => void submit(event)}>
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Immutable inputs</p>
          <h2>Queue a reliability run</h2>
        </div>
        <span className="quiet-chip">No hidden defaults</span>
      </div>
      <div className="form-grid">
        <Field label="Run ID" name="run-id" required {...field('runId')} />
        <Field
          label="Created by"
          name="created-by"
          required
          {...field('createdBy')}
        />
      </div>
      <fieldset>
        <legend>Scenario revision</legend>
        <div className="form-grid form-grid--reference">
          <Field
            label="Scenario ID"
            name="scenario-id"
            required
            {...field('scenarioId')}
          />
          <Field
            label="Revision"
            name="scenario-revision"
            required
            {...field('scenarioRevision')}
          />
          <Field
            label="SHA-256 digest"
            name="scenario-digest"
            placeholder="sha256:…"
            required
            {...field('scenarioDigest')}
          />
        </div>
      </fieldset>
      <fieldset>
        <legend>Agent Configuration revision</legend>
        <div className="form-grid form-grid--reference">
          <Field
            label="Configuration ID"
            name="agent-id"
            required
            {...field('agentId')}
          />
          <Field
            label="Revision"
            name="agent-revision"
            required
            {...field('agentRevision')}
          />
          <Field
            label="SHA-256 digest"
            name="agent-digest"
            placeholder="sha256:…"
            required
            {...field('agentDigest')}
          />
        </div>
      </fieldset>
      {error !== null && <ErrorNotice error={error} />}
      <div className="form-actions">
        <p>
          The control plane verifies every ID, revision, and digest before
          queuing.
        </p>
        <button
          className="button button--primary"
          disabled={busy}
          type="submit"
        >
          <Icon name="run" /> {busy ? 'Queuing…' : 'Start Run'}
        </button>
      </div>
    </form>
  );
}

function Field({
  label,
  name,
  ...props
}: React.InputHTMLAttributes<HTMLInputElement> & {
  label: string;
  name: string;
}) {
  return (
    <label className="field" htmlFor={name}>
      <span>{label}</span>
      <input id={name} name={name} {...props} />
    </label>
  );
}

export function RunHeader({
  run,
  report,
}: {
  run: Run;
  report: ReportResponse | null;
}) {
  const classification = reportClassification(report);
  return (
    <section className="run-hero panel">
      <div className="run-hero__identity">
        <p className="eyebrow">Run evidence room</p>
        <div className="run-title-row">
          <h1>{run.run_id}</h1>
          <StatusBadge value={run.status} />
          {classification !== null && <StatusBadge value={classification} />}
        </div>
        <p className="run-subtitle">
          Created {formatTime(run.created_at)} by {run.created_by} · attempt{' '}
          {run.attempt}
        </p>
      </div>
      <dl className="reference-grid">
        <Reference
          label="Scenario"
          value={`${run.scenario.id} @ ${run.scenario.revision}`}
          digest={run.scenario.digest}
        />
        <Reference
          label="Agent Configuration"
          value={`${run.agent_configuration.id} @ ${run.agent_configuration.revision}`}
          digest={run.agent_configuration.digest}
        />
        <Reference
          label="Fault seed"
          value={
            run.fault_seed === null ? 'Not selected' : String(run.fault_seed)
          }
        />
      </dl>
    </section>
  );
}

function Reference({
  label,
  value,
  digest,
}: {
  label: string;
  value: string;
  digest?: string;
}) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{value}</dd>
      {digest !== undefined && <DigestDisclosure digest={digest} />}
    </div>
  );
}

export function FlagshipStory({
  events,
  report,
}: {
  events: readonly RunEvent[];
  report: ReportResponse | null;
}) {
  const steps = deriveFlagshipStory(events, report);
  const applicable = steps.filter((step) => step.state === 'complete');
  if (applicable.length < 2) return null;
  return (
    <section className="story panel" aria-labelledby="story-title">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Causal chain</p>
          <h2 id="story-title">From ambiguous response to proven outcome</h2>
        </div>
        <span className="quiet-chip">Derived from persisted evidence</span>
      </div>
      <ol className="story-flow">
        {steps.map((step, index) => (
          <li
            className={
              step.state === 'complete'
                ? 'story-step story-step--complete'
                : 'story-step'
            }
            key={step.id}
          >
            <span className="story-step__number">{index + 1}</span>
            <div>
              <strong>{step.label}</strong>
              <span>
                {step.state === 'complete'
                  ? step.detail
                  : 'Not observed in this Run'}
              </span>
              {step.sequence !== null && <small>Event #{step.sequence}</small>}
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}

export function ExactlyOncePanel({
  events,
  report,
}: {
  events: readonly RunEvent[];
  report: ReportResponse | null;
}) {
  const proof = deriveExactlyOnceProof(events, report);
  if (proof.gates.length === 0) {
    return (
      <EmptyState
        title="Exactly-once proof pending"
        body="No authoritative exactly-once evaluator gate is available yet."
      />
    );
  }
  return (
    <section
      className={`proof-card ${proof.authoritative ? 'proof-card--pass' : 'proof-card--unproven'}`}
    >
      <div className="proof-card__seal">
        <Icon name="shield" />
      </div>
      <div>
        <p className="eyebrow">Authoritative business effect</p>
        <h2>
          {proof.authoritative
            ? 'Exactly one refund, proven'
            : 'Exactly-once not proven'}
        </h2>
        <div className="proof-metrics">
          <Metric
            label="Expected effects"
            value={String(proof.expectedEffects)}
          />
          <Metric
            label="Observed effects"
            value={String(proof.observedEffects)}
          />
          <Metric
            label="Evaluator gates"
            value={`${String(proof.gates.filter((gate) => gate.status === 'pass').length)}/${String(proof.gates.length)} PASS`}
          />
        </div>
        <p>
          {proof.duplicateDisposition === 'prevented_or_reconciled'
            ? 'A retry occurred, while authenticated state evidence neutered a duplicate business effect.'
            : 'The interface will not infer duplicate prevention from missing UI events.'}
        </p>
      </div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function EventTimeline({ events }: { events: readonly RunEvent[] }) {
  if (events.length === 0)
    return (
      <EmptyState
        title="No evidence yet"
        body="Persisted Run Events will appear here in authoritative sequence order."
      />
    );
  return (
    <ol className="timeline" aria-label="Run event timeline">
      {events.map((event) => {
        const category = eventCategory(event.event_type);
        return (
          <li
            className={`timeline-event timeline-event--${category}`}
            key={event.event_id}
            data-testid="timeline-event"
          >
            <div className="timeline-event__rail">
              <span>{event.sequence}</span>
            </div>
            <div className="timeline-event__card">
              <div className="timeline-event__meta">
                <span className="event-kind">{category}</span>
                <time dateTime={event.occurred_at}>
                  {formatTime(event.occurred_at)}
                </time>
                <code>{event.event_type}</code>
              </div>
              <strong>{eventSummary(event)}</strong>
              <p>Produced by {event.producer.component}</p>
              <details>
                <summary>Inspect structured evidence</summary>
                <pre>{safeJson(event)}</pre>
              </details>
            </div>
          </li>
        );
      })}
    </ol>
  );
}

export function EvaluationPanel({ report }: { report: ReportResponse | null }) {
  if (report === null)
    return (
      <EmptyState
        title="Evaluation not available"
        body="The deterministic evaluator has not produced a result for this Run."
      />
    );
  const classification = reportClassification(report) ?? 'invalid';
  const gates = reportGates(report);
  const metrics = report.document.diagnostic_metrics;
  return (
    <section className="evaluation-card">
      <div className="evaluation-card__verdict">
        <p className="eyebrow">Deterministic evaluation</p>
        <StatusBadge value={classification} />
        <p>{classificationCopy(classification)}</p>
        <span className="document-kind">
          {report.kind.replaceAll('_', ' ')}
        </span>
        <DigestDisclosure digest={report.digest} />
      </div>
      <div className="gate-list">
        {gates.map((gate) => (
          <div className="gate" key={gate.gate_id}>
            <StatusBadge value={gate.status} />
            <div>
              <strong>{humanize(gate.gate_id)}</strong>
              <span>
                {gate.reason_code === undefined
                  ? `${gate.evidence.length} evidence reference(s)`
                  : humanize(gate.reason_code)}
              </span>
            </div>
          </div>
        ))}
      </div>
      {metrics.length > 0 && (
        <div className="diagnostic-list">
          <h3>Diagnostics</h3>
          {metrics.slice(0, 8).map((metric) => (
            <div key={metric.metric_id}>
              <span>{humanize(metric.metric_id)}</span>
              <strong>
                {metric.value} {metric.unit}
              </strong>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export function ApprovalsPanel({
  approvals,
  busyId,
  onResolve,
}: {
  approvals: readonly Approval[];
  busyId: string | null;
  onResolve: (
    approval: Approval,
    result: 'approved' | 'denied',
    actorId: string,
  ) => Promise<void>;
}) {
  const [actorId, setActorId] = useState('');
  if (approvals.length === 0)
    return (
      <EmptyState
        title="No approval activity"
        body="This Run has no persisted approval requests."
      />
    );
  return (
    <div className="approval-list">
      <label className="field" htmlFor="approval-actor-id">
        <span>Human responder ID</span>
        <input
          id="approval-actor-id"
          maxLength={128}
          pattern="[A-Za-z0-9][A-Za-z0-9._:-]*"
          placeholder="Enter your actor ID"
          required
          value={actorId}
          onChange={(event) => setActorId(event.target.value)}
        />
      </label>
      {approvals.map((approval) => (
        <article className="approval-card" key={approval.approval_id}>
          <div>
            <StatusBadge value={approval.status} />
            <h3>{approval.tool_id}</h3>
            <p>
              {approval.policy.id} @ {approval.policy.revision}
            </p>
          </div>
          <dl>
            <dt>Approval ID</dt>
            <dd>{approval.approval_id}</dd>
            <dt>Requested</dt>
            <dd>{formatTime(approval.created_at)}</dd>
          </dl>
          {approval.status === 'pending' && (
            <div className="approval-actions">
              <button
                className="button button--quiet"
                disabled={
                  busyId === approval.approval_id || !validActorId(actorId)
                }
                onClick={() => void onResolve(approval, 'denied', actorId)}
              >
                Deny
              </button>
              <button
                className="button button--primary"
                disabled={
                  busyId === approval.approval_id || !validActorId(actorId)
                }
                onClick={() => void onResolve(approval, 'approved', actorId)}
              >
                Approve
              </button>
            </div>
          )}
        </article>
      ))}
    </div>
  );
}

function validActorId(value: string): boolean {
  return /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value);
}

export function RawEvidence({
  report,
  events,
}: {
  report: ReportResponse | null;
  events: readonly RunEvent[];
}) {
  const visibleEvents = events.length > 400 ? events.slice(-400) : events;
  return (
    <details className="raw-evidence panel">
      <summary>Raw evidence inspector</summary>
      <p>
        Immutable event documents and the final report are shown as inert text.
        {events.length > visibleEvents.length
          ? ` Displaying the latest 400 of ${String(events.length)} events to keep the inspector bounded.`
          : ''}
      </p>
      <pre>{safeJson({ events: visibleEvents, report })}</pre>
    </details>
  );
}

export function Panel({
  eyebrow,
  title,
  children,
  className = '',
}: {
  eyebrow: string;
  title: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`}>
      <div className="panel-heading">
        <div>
          <p className="eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
        </div>
      </div>
      {children}
    </section>
  );
}

export function formatTime(value: string): string {
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf())
    ? value
    : new Intl.DateTimeFormat(undefined, {
        dateStyle: 'medium',
        timeStyle: 'medium',
      }).format(parsed);
}

export function humanize(value: string): string {
  return value
    .replaceAll(/[._-]/g, ' ')
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return 'Unable to display structured evidence.';
  }
}

function shortDigest(value: string): string {
  return value.length > 24 ? `${value.slice(0, 16)}…${value.slice(-7)}` : value;
}

export function DigestDisclosure({ digest }: { digest: string }) {
  return (
    <details className="digest-disclosure">
      <summary>
        <code>{shortDigest(digest)}</code>
      </summary>
      <code>{digest}</code>
    </details>
  );
}

function classificationCopy(value: EvaluationClassification): string {
  if (value === 'pass')
    return 'All critical gates passed against authoritative evidence.';
  if (value === 'fail')
    return 'At least one critical business or safety gate failed.';
  if (value === 'invalid')
    return 'The evaluator input was missing or corrupt; this is not an ordinary failure.';
  return 'This Run has not been evaluated.';
}

export function objectEntries(value: JsonObject): [string, unknown][] {
  return Object.entries(value);
}
