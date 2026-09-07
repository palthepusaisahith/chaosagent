import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from 'react';

import { ControlPlaneClient } from './api';
import {
  ApprovalsPanel,
  DigestDisclosure,
  EmptyState,
  ErrorNotice,
  EvaluationPanel,
  EventTimeline,
  ExactlyOncePanel,
  FlagshipStory,
  Icon,
  LoadingState,
  PageHeader,
  Panel,
  RawEvidence,
  RunCreationForm,
  RunHeader,
  StatusBadge,
  humanize,
  objectEntries,
  safeJson,
} from './components';
import type {
  Approval,
  Campaign,
  CampaignComparisonResponse,
  CampaignDocumentResponse,
  JsonObject,
  ReportResponse,
  Run,
  RunCreateInput,
  RunEvent,
  RunStatus,
} from './domain';
import { readString, terminalStatuses } from './domain';
import { sortAndMergeEvents } from './presentation';
import { ReplaySafeRunStream } from './sse';

type Route =
  | { page: 'home' }
  | { page: 'runs' }
  | { page: 'run'; runId: string }
  | { page: 'campaigns' };

const defaultClient = new ControlPlaneClient();

export function App({
  client = defaultClient,
}: {
  client?: ControlPlaneClient;
}) {
  const [route, setRoute] = useState<Route>(() =>
    parseRoute(window.location.pathname),
  );
  const [knownRuns, setKnownRuns] = useState<Map<string, Run>>(() => new Map());
  useEffect(() => {
    const pop = () => setRoute(parseRoute(window.location.pathname));
    window.addEventListener('popstate', pop);
    return () => window.removeEventListener('popstate', pop);
  }, []);
  const navigate = useCallback((path: string) => {
    window.history.pushState(null, '', path);
    setRoute(parseRoute(path));
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);
  const remember = useCallback((run: Run) => {
    setKnownRuns((current) => new Map(current).set(run.run_id, run));
  }, []);
  return (
    <div className="app-shell">
      <Navigation route={route} navigate={navigate} />
      <main id="main-content">
        {route.page === 'home' && (
          <HomePage runs={[...knownRuns.values()]} navigate={navigate} />
        )}
        {route.page === 'runs' && (
          <RunsPage client={client} navigate={navigate} remember={remember} />
        )}
        {route.page === 'run' && (
          <RunDetailPage
            client={client}
            runId={route.runId}
            remember={remember}
          />
        )}
        {route.page === 'campaigns' && (
          <CampaignsPage client={client} navigate={navigate} />
        )}
      </main>
      <footer>
        <span>ChaosAgent</span>
        <span>
          PostgreSQL evidence is authoritative · dashboard state is a view
        </span>
      </footer>
    </div>
  );
}

function Navigation({
  route,
  navigate,
}: {
  route: Route;
  navigate: (path: string) => void;
}) {
  return (
    <header className="topbar">
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <button
        aria-label="Open ChaosAgent dashboard"
        className="brand"
        onClick={() => navigate('/')}
      >
        <span className="brand-mark">
          <Icon name="pulse" />
        </span>
        <span>
          <strong>ChaosAgent</strong>
          <small>Reliability control room</small>
        </span>
      </button>
      <nav aria-label="Primary navigation">
        <NavLink active={route.page === 'home'} href="/" navigate={navigate}>
          Dashboard
        </NavLink>
        <NavLink
          active={route.page === 'runs' || route.page === 'run'}
          href="/runs"
          navigate={navigate}
        >
          Runs
        </NavLink>
        <NavLink
          active={route.page === 'campaigns'}
          href="/campaigns"
          navigate={navigate}
        >
          Campaigns
        </NavLink>
      </nav>
      <span className="authority-indicator">
        <span /> Evidence-backed
      </span>
    </header>
  );
}

function NavLink({
  active,
  href,
  navigate,
  children,
}: {
  active: boolean;
  href: string;
  navigate: (path: string) => void;
  children: ReactNode;
}) {
  return (
    <a
      className={active ? 'nav-link nav-link--active' : 'nav-link'}
      href={href}
      onClick={(event) => {
        event.preventDefault();
        navigate(href);
      }}
    >
      {children}
    </a>
  );
}

function HomePage({
  runs,
  navigate,
}: {
  runs: Run[];
  navigate: (path: string) => void;
}) {
  const active = runs.filter((run) => !terminalStatuses.has(run.status));
  const faulted = runs.filter((run) => run.fault_seed !== null);
  return (
    <div className="page home-page">
      <section className="home-hero">
        <div>
          <p className="eyebrow">Reliability engineering for agents</p>
          <h1>
            Know what happens
            <br />
            after the tool <em>lies.</em>
          </h1>
          <p className="lede">
            Reliability evaluation for tool-using AI agents under real failure
            conditions.
          </p>
          <div className="hero-actions">
            <button
              className="button button--primary"
              onClick={() => navigate('/runs')}
            >
              <Icon name="run" /> Start a Run
            </button>
            <button
              className="button button--quiet"
              onClick={() => navigate('/campaigns')}
            >
              <Icon name="campaign" /> Compare Campaigns
            </button>
          </div>
        </div>
        <FlagshipPreview />
      </section>
      <section className="summary-strip" aria-label="Loaded run summary">
        <SummaryMetric
          label="Loaded Runs"
          value={String(runs.length)}
          note="This browser session"
        />
        <SummaryMetric
          label="Active"
          value={String(active.length)}
          note="Queued through evaluating"
        />
        <SummaryMetric
          label="Fault-selected"
          value={String(faulted.length)}
          note="With persisted fault seed"
        />
        <SummaryMetric
          label="Authority"
          value="DB"
          note="Append-only evidence"
        />
      </section>
      <section className="home-grid">
        <Panel eyebrow="Workspace" title="Loaded Runs" className="recent-runs">
          {runs.length === 0 ? (
            <EmptyState
              title="No Runs loaded"
              body="Issue #20 exposes lookup—not a global Run list. Open or create a Run to begin this truthful session view."
            />
          ) : (
            <div className="run-list">
              {runs
                .slice(-6)
                .reverse()
                .map((run) => (
                  <RunRow key={run.run_id} run={run} navigate={navigate} />
                ))}
            </div>
          )}
        </Panel>
        <Panel
          eyebrow="Flagship experiment"
          title="The lost refund acknowledgement"
        >
          <p className="feature-copy">
            A refund commits. Its acknowledgement is deliberately hidden. The
            agent must recover without creating a second refund—and prove it
            from authoritative state.
          </p>
          <div className="mini-sequence">
            <span>Commit</span>
            <Icon name="arrow" />
            <span className="mini-sequence__fault">Timeout</span>
            <Icon name="arrow" />
            <span>Recover</span>
            <Icon name="arrow" />
            <span className="mini-sequence__pass">Prove</span>
          </div>
        </Panel>
      </section>
    </div>
  );
}

function FlagshipPreview() {
  return (
    <div className="signal-card" aria-label="Flagship reliability signal">
      <div className="signal-card__top">
        <span>AMBIGUOUS REFUND</span>
        <span className="live-dot">REFERENCE FLOW</span>
      </div>
      <div className="signal-wave">
        <i />
        <i />
        <i />
        <i />
        <i />
        <i />
        <i />
      </div>
      <div className="signal-card__fault">
        <span>Mutation committed</span>
        <strong>ACK LOST</strong>
      </div>
      <div className="signal-card__result">
        <span className="seal">✓</span>
        <div>
          <strong>1 refund</strong>
          <small>exactly-once invariant preserved</small>
        </div>
        <StatusBadge value="pass" />
      </div>
    </div>
  );
}

function SummaryMetric({
  label,
  value,
  note,
}: {
  label: string;
  value: string;
  note: string;
}) {
  return (
    <article>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{note}</small>
    </article>
  );
}

function RunRow({
  run,
  navigate,
}: {
  run: Run;
  navigate: (path: string) => void;
}) {
  return (
    <button
      className="run-row"
      onClick={() => navigate(`/runs/${encodeURIComponent(run.run_id)}`)}
    >
      <span className="run-row__mark">
        <Icon name="pulse" />
      </span>
      <span>
        <strong>{run.run_id}</strong>
        <small>
          {run.scenario.id} @ {run.scenario.revision}
        </small>
      </span>
      <StatusBadge value={run.status} />
      <Icon name="arrow" />
    </button>
  );
}

function RunsPage({
  client,
  navigate,
  remember,
}: {
  client: ControlPlaneClient;
  navigate: (path: string) => void;
  remember: (run: Run) => void;
}) {
  const [busy, setBusy] = useState(false);
  const [lookupId, setLookupId] = useState('');
  const [lookupError, setLookupError] = useState<unknown>(null);
  const create = async (input: RunCreateInput) => {
    setBusy(true);
    try {
      const run = await client.createRun(input);
      remember(run);
      navigate(`/runs/${encodeURIComponent(run.run_id)}`);
    } finally {
      setBusy(false);
    }
  };
  const lookup = async (event: FormEvent) => {
    event.preventDefault();
    setLookupError(null);
    setBusy(true);
    try {
      const run = await client.getRun(lookupId);
      remember(run);
      navigate(`/runs/${encodeURIComponent(run.run_id)}`);
    } catch (error) {
      setLookupError(error);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="page">
      <PageHeader
        eyebrow="Run workspace"
        title="Start or inspect an experiment"
      />
      <div className="runs-layout">
        <RunCreationForm busy={busy} onCreate={create} />
        <aside className="panel lookup-card">
          <p className="eyebrow">Persisted history</p>
          <h2>Open a Run</h2>
          <p>
            Enter an exact Run ID. The control plane does not expose a global
            listing endpoint.
          </p>
          <form onSubmit={(event) => void lookup(event)}>
            <label className="field" htmlFor="lookup-run">
              <span>Run ID</span>
              <input
                id="lookup-run"
                required
                value={lookupId}
                onChange={(event) => setLookupId(event.target.value)}
              />
            </label>
            <button
              className="button button--dark"
              disabled={busy}
              type="submit"
            >
              Open evidence <Icon name="arrow" />
            </button>
          </form>
          {lookupError !== null && <ErrorNotice error={lookupError} />}
          <hr />
          <h3>What gets frozen</h3>
          <ul className="check-list">
            <li>Scenario ID, revision, digest</li>
            <li>Agent Configuration ID, revision, digest</li>
            <li>Fixture, policy, and fault plan resolved server-side</li>
          </ul>
        </aside>
      </div>
    </div>
  );
}

function RunDetailPage({
  client,
  runId,
  remember,
}: {
  client: ControlPlaneClient;
  runId: string;
  remember: (run: Run) => void;
}) {
  const [run, setRun] = useState<Run | null>(null);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [report, setReport] = useState<ReportResponse | null>(null);
  const [approvals, setApprovals] = useState<Approval[]>([]);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  const [streamState, setStreamState] = useState<
    'connected' | 'reconnecting' | 'stopped'
  >('stopped');
  const [streamError, setStreamError] = useState<string | null>(null);
  const [approvalBusy, setApprovalBusy] = useState<string | null>(null);
  const streamRef = useRef<ReplaySafeRunStream | null>(null);
  useEffect(() => {
    let active = true;
    let handoffCursor: string | null = null;
    let finalizing = false;
    let stream: ReplaySafeRunStream | null = null;
    const reconcileTerminal = async () => {
      if (!active || finalizing) return;
      finalizing = true;
      try {
        const [fresh, tail, finalReport, finalApprovals] = await Promise.all([
          client.getRun(runId),
          client.getAllEvents(runId, 200, handoffCursor),
          client.getReport(runId),
          client.getApprovals(runId),
        ]);
        if (!active) return;
        setRun(fresh);
        remember(fresh);
        setEvents((current) => sortAndMergeEvents(current, tail.events));
        setReport(finalReport);
        setApprovals(finalApprovals.approvals);
        if (terminalStatuses.has(fresh.status)) {
          stream?.stop();
        } else {
          finalizing = false;
        }
      } catch {
        if (active) {
          finalizing = false;
          setStreamError('Final reconciliation failed; reload to retry.');
        }
      }
    };
    const load = async () => {
      setLoading(true);
      setError(null);
      setEvents([]);
      setReport(null);
      setApprovals([]);
      try {
        const loadedRun = await client.getRun(runId);
        const replay = await client.getAllEvents(runId);
        const [loadedReport, loadedApprovals] = await Promise.all([
          client.getReport(runId),
          client.getApprovals(runId),
        ]);
        if (!active) return;
        setRun(loadedRun);
        remember(loadedRun);
        setEvents(sortAndMergeEvents([], replay.events));
        setReport(loadedReport);
        setApprovals(loadedApprovals.approvals);
        handoffCursor = replay.cursor;
        stream = new ReplaySafeRunStream(
          client.apiUrl(`/runs/${encodeURIComponent(runId)}/events/stream`),
          runId,
          undefined,
          {
            onEvent: (event) => {
              if (!active) return;
              setEvents((current) => sortAndMergeEvents(current, [event]));
              const state = readString(event.payload, 'state') as
                RunStatus | undefined;
              if (event.event_type === 'run.lifecycle') {
                if (state !== undefined && terminalStatuses.has(state)) {
                  void reconcileTerminal();
                } else {
                  void client
                    .getRun(runId)
                    .then((fresh) => {
                      if (active) {
                        setRun(fresh);
                        remember(fresh);
                      }
                    })
                    .catch(() => {
                      if (active)
                        setStreamError(
                          'Run status refresh failed; persisted events remain visible.',
                        );
                    });
                }
              }
            },
            onState: (state) => {
              if (active) setStreamState(state);
            },
            onError: (message) => {
              if (active) setStreamError(message);
            },
          },
          replay.cursor,
          replay.events,
        );
        streamRef.current = stream;
        stream.start();
        const reconciledRun = await client.getRun(runId);
        if (!active) return;
        setRun(reconciledRun);
        remember(reconciledRun);
        if (terminalStatuses.has(reconciledRun.status)) {
          await reconcileTerminal();
        }
      } catch (caught) {
        if (active) setError(caught);
      } finally {
        if (active) setLoading(false);
      }
    };
    void load();
    return () => {
      active = false;
      streamRef.current?.stop();
      streamRef.current = null;
    };
  }, [client, remember, runId]);
  const resolve = async (
    approval: Approval,
    result: 'approved' | 'denied',
    actorId: string,
  ) => {
    setApprovalBusy(approval.approval_id);
    setError(null);
    try {
      await client.resolveApproval(approval.approval_id, result, actorId);
      const fresh = await client.getApprovals(runId);
      setApprovals(fresh.approvals);
    } catch (caught) {
      setError(caught);
    } finally {
      setApprovalBusy(null);
    }
  };
  if (loading)
    return (
      <div className="page">
        <LoadingState />
      </div>
    );
  if (error !== null && run === null)
    return (
      <div className="page">
        <PageHeader eyebrow="Run evidence room" title="Run unavailable" />
        <ErrorNotice error={error} />
      </div>
    );
  if (run === null) return null;
  const visibleEvents = events.length > 400 ? events.slice(-400) : events;
  return (
    <div className="page run-page">
      <RunHeader run={run} report={report} />
      <div className="live-bar" role="status">
        <span className={`live-status live-status--${streamState}`} />
        <strong>
          {terminalStatuses.has(run.status)
            ? 'Persisted final state'
            : streamState === 'reconnecting'
              ? 'Live stream reconnecting'
              : 'Following persisted events'}
        </strong>
        <span>{events.length} events · ordered by sequence</span>
      </div>
      {streamError !== null && (
        <div className="notice notice--warning" role="status">
          <strong>Live view notice</strong>
          <span>{streamError} Existing persisted evidence is unchanged.</span>
        </div>
      )}
      {error !== null && <ErrorNotice error={error} />}
      <FlagshipStory events={events} report={report} />
      <div className="evidence-layout">
        <section className="panel timeline-panel">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Execution timeline</p>
              <h2>What happened, in order</h2>
            </div>
            <span className="quiet-chip">Sequence is authoritative</span>
          </div>
          {events.length > visibleEvents.length && (
            <p className="bounded-notice">
              Showing the latest 400 of {events.length} events. Raw evidence
              retains the complete replay.
            </p>
          )}
          <EventTimeline events={visibleEvents} />
        </section>
        <aside className="inspector-column">
          <ExactlyOncePanel events={events} report={report} />
          <Panel eyebrow="Evaluation" title="Critical gates">
            <EvaluationPanel report={report} />
          </Panel>
          <Panel eyebrow="Policy boundary" title="Approvals">
            <ApprovalsPanel
              approvals={approvals}
              busyId={approvalBusy}
              onResolve={resolve}
            />
          </Panel>
          <ExpectedActual report={report} />
        </aside>
      </div>
      <RawEvidence events={events} report={report} />
    </div>
  );
}

function ExpectedActual({ report }: { report: ReportResponse | null }) {
  const gates = report?.document.critical_gates ?? [];
  return (
    <Panel eyebrow="Expected vs observed" title="Verified outcomes">
      {gates.length === 0 ? (
        <EmptyState
          title="No verified outcomes"
          body="Expected and observed state appears after deterministic evaluation."
        />
      ) : (
        <div
          className="comparison-table"
          role="table"
          aria-label="Expected and observed gate outcomes"
        >
          <div role="row">
            <strong role="columnheader">Invariant</strong>
            <strong role="columnheader">Expected</strong>
            <strong role="columnheader">Verdict</strong>
          </div>
          {gates.slice(0, 8).map((gate) => (
            <div role="row" key={gate.gate_id}>
              <span role="cell">{humanize(gate.gate_id)}</span>
              <span role="cell">Satisfied</span>
              <span role="cell">
                <StatusBadge value={gate.status} />
              </span>
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

function CampaignsPage({
  client,
  navigate,
}: {
  client: ControlPlaneClient;
  navigate: (path: string) => void;
}) {
  const [campaignId, setCampaignId] = useState('');
  const [baselineId, setBaselineId] = useState('');
  const [faultedId, setFaultedId] = useState('');
  const [campaign, setCampaign] = useState<Campaign | null>(null);
  const [statistics, setStatistics] = useState<CampaignDocumentResponse | null>(
    null,
  );
  const [comparison, setComparison] =
    useState<CampaignComparisonResponse | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  const loadCampaign = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    setComparison(null);
    try {
      const [plan, stats] = await Promise.all([
        client.getCampaign(campaignId),
        client.getCampaignStatistics(campaignId),
      ]);
      setCampaign(plan);
      setStatistics(stats);
    } catch (caught) {
      setError(caught);
    } finally {
      setBusy(false);
    }
  };
  const compare = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      setComparison(await client.compareCampaigns(baselineId, faultedId));
    } catch (caught) {
      setError(caught);
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="page">
      <PageHeader
        eyebrow="Campaign reliability"
        title="Inspect cohorts and paired deltas"
      />
      {error !== null && <ErrorNotice error={error} />}
      <div className="campaign-layout">
        <Panel eyebrow="Authoritative cohort" title="Open Campaign">
          <form
            className="inline-form"
            onSubmit={(event) => void loadCampaign(event)}
          >
            <label className="field" htmlFor="campaign-id">
              <span>Campaign ID</span>
              <input
                id="campaign-id"
                required
                value={campaignId}
                onChange={(event) => setCampaignId(event.target.value)}
              />
            </label>
            <button className="button button--primary" disabled={busy}>
              Load
            </button>
          </form>
        </Panel>
        <Panel eyebrow="Paired comparison" title="Baseline vs faulted">
          <form
            className="comparison-form"
            onSubmit={(event) => void compare(event)}
          >
            <label className="field" htmlFor="baseline-id">
              <span>Baseline Campaign</span>
              <input
                id="baseline-id"
                required
                value={baselineId}
                onChange={(event) => setBaselineId(event.target.value)}
              />
            </label>
            <label className="field" htmlFor="faulted-id">
              <span>Faulted Campaign</span>
              <input
                id="faulted-id"
                required
                value={faultedId}
                onChange={(event) => setFaultedId(event.target.value)}
              />
            </label>
            <button className="button button--dark" disabled={busy}>
              Compare
            </button>
          </form>
        </Panel>
      </div>
      {campaign !== null && (
        <CampaignResult
          campaign={campaign}
          statistics={statistics}
          navigate={navigate}
        />
      )}
      {comparison !== null && <CampaignComparison response={comparison} />}
      {campaign === null && comparison === null && (
        <EmptyState
          title="No Campaign loaded"
          body="Enter an exact persisted Campaign identity. Aggregates are computed by the Issue #17 backend."
        />
      )}
    </div>
  );
}

function CampaignResult({
  campaign,
  statistics,
  navigate,
}: {
  campaign: Campaign;
  statistics: CampaignDocumentResponse | null;
  navigate: (path: string) => void;
}) {
  return (
    <section className="panel campaign-result">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">{campaign.arm} arm</p>
          <h2>{campaign.campaign_id}</h2>
          <DigestDisclosure digest={campaign.digest} />
        </div>
        <StatusBadge value={campaign.arm} />
      </div>
      <div className="campaign-summary">
        <SummaryMetric
          label="Planned trials"
          value={String(campaign.planned_trials)}
          note="Frozen membership"
        />
        <SummaryMetric
          label="Selected faults"
          value={String(campaign.selected_fault_ids.length)}
          note={campaign.selected_fault_ids.join(', ') || 'Baseline'}
        />
        <SummaryMetric
          label="Scenario"
          value={campaign.scenario.revision}
          note={campaign.scenario.id}
        />
      </div>
      <div className="trial-grid">
        {campaign.assignments.map((assignment) => (
          <button
            key={assignment.run_id}
            onClick={() =>
              navigate(`/runs/${encodeURIComponent(assignment.run_id)}`)
            }
          >
            <span>Trial {assignment.trial_index}</span>
            <strong>{assignment.run_id}</strong>
            <Icon name="arrow" />
          </button>
        ))}
      </div>
      {statistics !== null && (
        <div>
          <DigestDisclosure digest={statistics.digest} />
          <DocumentFacts
            title="Computed statistics"
            document={statistics.document}
          />
        </div>
      )}
    </section>
  );
}

function CampaignComparison({
  response,
}: {
  response: CampaignComparisonResponse;
}) {
  return (
    <section className="panel campaign-result">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">Authenticated comparison</p>
          <h2>
            {response.baseline_campaign_id} <span className="muted">vs</span>{' '}
            {response.faulted_campaign_id}
          </h2>
        </div>
        <span className="quiet-chip">Issue #17 statistics</span>
      </div>
      <DigestDisclosure digest={response.digest} />
      <DocumentFacts title="Paired results" document={response.document} />
    </section>
  );
}

function DocumentFacts({
  title,
  document,
}: {
  title: string;
  document: JsonObject;
}) {
  const facts = objectEntries(document)
    .filter(([, value]) =>
      ['string', 'number', 'boolean'].includes(typeof value),
    )
    .slice(0, 12);
  return (
    <div className="document-facts">
      <h3>{title}</h3>
      {facts.length === 0 ? (
        <details>
          <summary>Inspect aggregate document</summary>
          <pre>{safeJson(document)}</pre>
        </details>
      ) : (
        <dl>
          {facts.map(([key, value]) => (
            <div key={key}>
              <dt>{humanize(key)}</dt>
              <dd>{String(value)}</dd>
            </div>
          ))}
        </dl>
      )}
      <details>
        <summary>Raw aggregate document</summary>
        <pre>{safeJson(document)}</pre>
      </details>
    </div>
  );
}

function parseRoute(pathname: string): Route {
  const runMatch = /^\/runs\/([^/]+)\/?$/.exec(pathname);
  if (runMatch?.[1] !== undefined) {
    try {
      return { page: 'run', runId: decodeURIComponent(runMatch[1]) };
    } catch {
      return { page: 'runs' };
    }
  }
  if (/^\/runs\/?$/.test(pathname)) return { page: 'runs' };
  if (/^\/campaigns\/?$/.test(pathname)) return { page: 'campaigns' };
  return { page: 'home' };
}
