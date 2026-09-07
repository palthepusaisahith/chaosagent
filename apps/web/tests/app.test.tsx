import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { ControlPlaneClient } from '../src/api';
import { App } from '../src/app';
import { event, report, run } from './fixtures';

class BrowserEventSource extends EventTarget {
  public static instances: BrowserEventSource[] = [];
  public readonly url: string;
  public onerror: ((event: Event) => void) | null = null;
  public closed = false;

  public constructor(url: string | URL) {
    super();
    this.url = String(url);
    BrowserEventSource.instances.push(this);
  }

  public close(): void {
    this.closed = true;
  }
}

afterEach(() => {
  BrowserEventSource.instances = [];
  window.history.replaceState(null, '', '/');
  vi.unstubAllGlobals();
});

describe('Run detail live integration', () => {
  it('hydrates REST first, appends SSE evidence, and finalizes a terminal Run', async () => {
    window.history.replaceState(null, '', '/runs/run-refund-001');
    vi.stubGlobal('EventSource', BrowserEventSource);
    let runReads = 0;
    let reportReads = 0;
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith('/runs/run-refund-001')) {
        runReads += 1;
        return json(run({ status: runReads < 3 ? 'running' : 'completed' }));
      }
      if (url.includes('/events?'))
        return json({
          run_id: 'run-refund-001',
          events: [],
          next_cursor: null,
          has_more: false,
        });
      if (url.endsWith('/report')) {
        reportReads += 1;
        return reportReads === 1
          ? json(
              { error: { code: 'report_not_found', message: 'Not ready.' } },
              409,
            )
          : json(report());
      }
      if (url.endsWith('/approvals'))
        return json({ run_id: 'run-refund-001', approvals: [] });
      throw new Error(`Unexpected URL ${url}`);
    });

    render(<App client={new ControlPlaneClient('', fetcher)} />);

    await screen.findByText('Following persisted events');
    const source = BrowserEventSource.instances[0];
    expect(source).toBeDefined();
    const terminal = event(1, 'run.lifecycle', {
      state: 'completed',
      previous_state: 'evaluating',
    });
    source?.dispatchEvent(
      new MessageEvent('run.lifecycle', {
        data: JSON.stringify(terminal),
        lastEventId: 'cursor-terminal',
      }),
    );

    await waitFor(() =>
      expect(screen.getByText('Persisted final state')).toBeVisible(),
    );
    await waitFor(() =>
      expect(screen.getAllByText('PASS').length).toBeGreaterThan(0),
    );
    expect(source?.closed).toBe(true);
    expect(screen.getByText('Run completed')).toBeVisible();
  });

  it('shows a truthful empty dashboard without inventing recent Runs', () => {
    render(<App client={new ControlPlaneClient('', vi.fn<typeof fetch>())} />);
    expect(
      screen.getByRole('heading', { name: 'No Runs loaded' }),
    ).toBeVisible();
    expect(
      screen.getByText(/exposes lookup—not a global Run list/i),
    ).toBeVisible();
  });

  it('drains a terminal event committed during the REST-to-SSE handoff', async () => {
    window.history.replaceState(null, '', '/runs/run-refund-001');
    vi.stubGlobal('EventSource', BrowserEventSource);
    let runReads = 0;
    let eventReads = 0;
    let reportReads = 0;
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith('/runs/run-refund-001')) {
        runReads += 1;
        return json(run({ status: runReads === 1 ? 'running' : 'completed' }));
      }
      if (url.includes('/events?')) {
        eventReads += 1;
        return eventReads === 1
          ? json({
              run_id: 'run-refund-001',
              events: [
                event(1, 'run.lifecycle', {
                  state: 'running',
                  previous_state: 'provisioning',
                }),
              ],
              next_cursor: 'cursor-1',
              has_more: false,
            })
          : json({
              run_id: 'run-refund-001',
              events: [
                event(2, 'run.lifecycle', {
                  state: 'completed',
                  previous_state: 'evaluating',
                }),
              ],
              next_cursor: 'cursor-2',
              has_more: false,
            });
      }
      if (url.endsWith('/report')) {
        reportReads += 1;
        return reportReads === 1
          ? json(
              { error: { code: 'report_not_ready', message: 'Not ready.' } },
              409,
            )
          : json(report());
      }
      if (url.endsWith('/approvals'))
        return json({ run_id: 'run-refund-001', approvals: [] });
      throw new Error(`Unexpected URL ${url}`);
    });

    render(<App client={new ControlPlaneClient('', fetcher)} />);

    await screen.findByText('Persisted final state');
    expect(screen.getByText('Run completed')).toBeVisible();
    expect(BrowserEventSource.instances[0]?.closed).toBe(true);
    expect(eventReads).toBe(2);
  });

  it('renders a loading state while authoritative Run hydration is pending', () => {
    window.history.replaceState(null, '', '/runs/run-refund-001');
    const fetcher = vi.fn<typeof fetch>(() => new Promise(() => undefined));
    render(<App client={new ControlPlaneClient('', fetcher)} />);
    expect(screen.getByRole('status')).toHaveTextContent(
      'Loading authoritative state',
    );
  });

  it('loads exact Campaign membership and backend-produced statistics', async () => {
    window.history.replaceState(null, '', '/campaigns');
    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input);
      if (url.endsWith('/campaigns/campaign-1')) {
        return json({
          schema_version: 'chaosagent.campaign/v0',
          campaign_id: 'campaign-1',
          arm: 'faulted',
          planned_trials: 1,
          scenario: run().scenario,
          agent_configuration: run().agent_configuration,
          selected_fault_ids: ['refund-ack-lost'],
          fault_plan_digest: `sha256:${'a'.repeat(64)}`,
          assignments: [{ trial_index: 0, run_id: 'run-refund-001' }],
          digest: `sha256:${'a'.repeat(64)}`,
          created_at: '2026-09-06T10:00:00Z',
        });
      }
      if (url.includes('/campaigns/campaign-1/statistics?k=1')) {
        return json({
          campaign_id: 'campaign-1',
          digest: `sha256:${'b'.repeat(64)}`,
          document: { pass_rate: 1, valid_trials: 1 },
        });
      }
      throw new Error(`Unexpected URL ${url}`);
    });
    const user = userEvent.setup();
    render(<App client={new ControlPlaneClient('', fetcher)} />);
    await user.type(screen.getByLabelText('Campaign ID'), 'campaign-1');
    await user.click(screen.getByRole('button', { name: 'Load' }));
    expect(await screen.findByText('run-refund-001')).toBeVisible();
    expect(screen.getByText(/pass rate/i)).toBeVisible();
    expect(screen.getAllByText('1').length).toBeGreaterThan(0);
  });
});

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}
