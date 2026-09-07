import { describe, expect, it, vi } from 'vitest';

import { ControlPlaneApiError, ControlPlaneClient } from '../src/api';
import { event, run } from './fixtures';

describe('ControlPlaneClient', () => {
  it('sends every immutable Run identity input unchanged', async () => {
    const created = run({ status: 'queued' });
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify(created), {
        status: 201,
        headers: { 'content-type': 'application/json' },
      }),
    );
    const client = new ControlPlaneClient('https://control.example', fetcher);
    const input = {
      run_id: 'run-refund-001',
      scenario: {
        id: 'scenario-a',
        revision: '7',
        digest: `sha256:${'1'.repeat(64)}`,
      },
      agent_configuration: {
        id: 'agent-a',
        revision: '3',
        digest: `sha256:${'2'.repeat(64)}`,
      },
      created_by: 'dashboard-user',
    };

    await client.createRun(input);

    expect(fetcher).toHaveBeenCalledOnce();
    const [url, init] = fetcher.mock.calls[0] ?? [];
    expect(url).toBe('https://control.example/api/v1/runs');
    expect(init?.method).toBe('POST');
    expect(typeof init?.body).toBe('string');
    if (typeof init?.body !== 'string') throw new Error('expected JSON body');
    expect(JSON.parse(init.body)).toEqual(input);
  });

  it('renders only the sanitized API error envelope', async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        JSON.stringify({
          error: {
            code: 'run_not_found',
            message: 'The Run does not exist.',
          },
        }),
        { status: 404 },
      ),
    );
    const client = new ControlPlaneClient('', fetcher);
    await expect(client.getRun('missing')).rejects.toEqual(
      new ControlPlaneApiError(404, 'run_not_found', 'The Run does not exist.'),
    );
  });

  it('falls back to a bounded transport error when an error body is not JSON', async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response('postgres host=secret', { status: 500 }));
    const client = new ControlPlaneClient('', fetcher);
    await expect(client.getRun('run-1')).rejects.toMatchObject({
      code: 'request_failed',
      message: 'The control-plane request could not be completed.',
    });
  });

  it('hydrates every event page and preserves the final opaque cursor', async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        json({
          run_id: 'run-refund-001',
          events: [event(1, 'run.lifecycle')],
          next_cursor: 'cursor-1',
          has_more: true,
        }),
      )
      .mockResolvedValueOnce(
        json({
          run_id: 'run-refund-001',
          events: [event(2, 'agent.step')],
          next_cursor: 'cursor-2',
          has_more: false,
        }),
      );
    const client = new ControlPlaneClient('', fetcher);
    const replay = await client.getAllEvents('run-refund-001', 1);
    expect(replay.events.map((item) => item.sequence)).toEqual([1, 2]);
    expect(replay.cursor).toBe('cursor-2');
    expect(requestUrl(fetcher.mock.calls[1]?.[0])).toContain('cursor=cursor-1');
  });

  it('sends the exact user-provided approval actor and resolution', async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementation(() => Promise.resolve(json({})));
    const client = new ControlPlaneClient('', fetcher);
    await client.resolveApproval('approval-1', 'approved', 'reviewer-9');
    const [url, init] = fetcher.mock.calls[0] ?? [];
    expect(url).toBe('/api/v1/approvals/approval-1/resolve');
    expect(typeof init?.body).toBe('string');
    if (typeof init?.body !== 'string') throw new Error('expected JSON body');
    expect(JSON.parse(init.body)).toEqual({
      result: 'approved',
      actor_id: 'reviewer-9',
    });
    await client.resolveApproval('approval-1', 'denied', 'reviewer-10');
    const deniedInit = fetcher.mock.calls[1]?.[1];
    expect(typeof deniedInit?.body).toBe('string');
    if (typeof deniedInit?.body !== 'string')
      throw new Error('expected JSON body');
    expect(JSON.parse(deniedInit.body)).toEqual({
      result: 'denied',
      actor_id: 'reviewer-10',
    });
  });
});

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status });
}

function requestUrl(value: RequestInfo | URL | undefined): string {
  if (typeof value === 'string') return value;
  if (value instanceof URL) return value.toString();
  return value?.url ?? '';
}
