import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  ReplaySafeRunStream,
  type EventSourceLike,
  type EventSourceMessage,
} from '../src/sse';
import { event } from './fixtures';

class FakeEventSource implements EventSourceLike {
  public onerror: ((event: Event) => void) | null = null;
  public closed = false;
  private readonly listeners = new Map<
    string,
    ((event: EventSourceMessage) => void)[]
  >();

  public addEventListener(
    type: string,
    listener: (event: EventSourceMessage) => void,
  ): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  public close(): void {
    this.closed = true;
  }

  public emit(type: string, message: EventSourceMessage): void {
    for (const listener of this.listeners.get(type) ?? []) listener(message);
  }
}

describe('ReplaySafeRunStream', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('appends an incremental persisted event once', () => {
    const sources: FakeEventSource[] = [];
    const received = vi.fn();
    const stream = new ReplaySafeRunStream(
      '/api/v1/runs/run-refund-001/events/stream',
      'run-refund-001',
      () => {
        const source = new FakeEventSource();
        sources.push(source);
        return source;
      },
      { onEvent: received, onState: vi.fn(), onError: vi.fn() },
      'cursor-1',
      [event(1, 'run.lifecycle')],
    );
    stream.start();
    const next = event(2, 'agent.step');
    sources[0]?.emit('agent.step', {
      data: JSON.stringify(next),
      lastEventId: 'cursor-2',
    });
    sources[0]?.emit('agent.step', {
      data: JSON.stringify(next),
      lastEventId: 'cursor-2',
    });
    expect(received).toHaveBeenCalledOnce();
    expect(received).toHaveBeenCalledWith(next, 'cursor-2');
    stream.stop();
  });

  it('resumes from the latest server cursor after an error without duplicating evidence', async () => {
    const urls: string[] = [];
    const sources: FakeEventSource[] = [];
    const received = vi.fn();
    const states = vi.fn();
    const stream = new ReplaySafeRunStream(
      '/api/v1/runs/run-refund-001/events/stream',
      'run-refund-001',
      (url) => {
        urls.push(url);
        const source = new FakeEventSource();
        sources.push(source);
        return source;
      },
      { onEvent: received, onState: states, onError: vi.fn() },
      'cursor-1',
      [event(1, 'run.lifecycle')],
    );
    stream.start();
    const next = event(2, 'tool.requested');
    sources[0]?.emit('tool.requested', {
      data: JSON.stringify(next),
      lastEventId: 'cursor-2',
    });
    sources[0]?.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(1000);
    expect(urls).toHaveLength(2);
    expect(new URL(urls[1] ?? '').searchParams.get('cursor')).toBe('cursor-2');
    sources[1]?.emit('tool.requested', {
      data: JSON.stringify(next),
      lastEventId: 'cursor-2',
    });
    expect(received).toHaveBeenCalledOnce();
    expect(states).toHaveBeenCalledWith('reconnecting');
    stream.stop();
  });

  it('rejects malformed or foreign-Run stream events', () => {
    const source = new FakeEventSource();
    const received = vi.fn();
    const failed = vi.fn();
    const stream = new ReplaySafeRunStream(
      '/api/v1/runs/run-refund-001/events/stream',
      'run-refund-001',
      () => source,
      { onEvent: received, onState: vi.fn(), onError: failed },
      null,
    );
    stream.start();
    source.emit('agent.step', { data: 'not json', lastEventId: 'cursor-1' });
    source.emit('agent.step', {
      data: JSON.stringify(event(1, 'agent.step', {}, { run_id: 'run-other' })),
      lastEventId: 'cursor-1',
    });
    expect(received).not.toHaveBeenCalled();
    expect(failed).toHaveBeenCalledTimes(2);
    stream.stop();
  });

  it.each([
    [
      'missing payload',
      (value: Record<string, unknown>) => delete value.payload,
    ],
    [
      'null payload',
      (value: Record<string, unknown>) => (value.payload = null),
    ],
    [
      'string payload',
      (value: Record<string, unknown>) => (value.payload = 'bad'),
    ],
    [
      'missing producer',
      (value: Record<string, unknown>) => delete value.producer,
    ],
    [
      'malformed producer',
      (value: Record<string, unknown>) => (value.producer = { component: 7 }),
    ],
  ])('rejects %s without advancing the replay cursor', async (_, corrupt) => {
    const urls: string[] = [];
    const sources: FakeEventSource[] = [];
    const received = vi.fn();
    const failed = vi.fn();
    const stream = new ReplaySafeRunStream(
      '/api/v1/runs/run-refund-001/events/stream',
      'run-refund-001',
      (url) => {
        urls.push(url);
        const source = new FakeEventSource();
        sources.push(source);
        return source;
      },
      { onEvent: received, onState: vi.fn(), onError: failed },
      'cursor-1',
      [event(1, 'run.lifecycle')],
    );
    stream.start();
    const malformed: Record<string, unknown> = { ...event(2, 'agent.step') };
    corrupt(malformed);
    sources[0]?.emit('agent.step', {
      data: JSON.stringify(malformed),
      lastEventId: 'cursor-bad',
    });
    expect(received).not.toHaveBeenCalled();
    expect(failed).toHaveBeenCalledWith(
      'The live stream returned an invalid event envelope.',
    );
    sources[0]?.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(1000);
    expect(new URL(urls[1] ?? '').searchParams.get('cursor')).toBe('cursor-1');
    stream.stop();
  });

  it('stops without advancing when stream identity contradicts persisted sequence', () => {
    const source = new FakeEventSource();
    const received = vi.fn();
    const failed = vi.fn();
    const states = vi.fn();
    const stream = new ReplaySafeRunStream(
      '/api/v1/runs/run-refund-001/events/stream',
      'run-refund-001',
      () => source,
      { onEvent: received, onState: states, onError: failed },
      'cursor-1',
      [event(1, 'run.lifecycle')],
    );
    stream.start();
    source.emit('agent.step', {
      data: JSON.stringify(
        event(1, 'agent.step', {}, { event_id: 'contradictory-event' }),
      ),
      lastEventId: 'bad-cursor',
    });
    expect(received).not.toHaveBeenCalled();
    expect(failed).toHaveBeenCalledWith(
      'The live stream contradicted persisted event identity.',
    );
    expect(source.closed).toBe(true);
    expect(states).toHaveBeenLastCalledWith('stopped');
  });

  it('cleans up the EventSource and pending reconnect on stop', async () => {
    const source = new FakeEventSource();
    const factory = vi.fn(() => source);
    const states = vi.fn();
    const stream = new ReplaySafeRunStream(
      '/api/v1/runs/run-refund-001/events/stream',
      'run-refund-001',
      factory,
      { onEvent: vi.fn(), onState: states, onError: vi.fn() },
      null,
    );
    stream.start();
    source.onerror?.(new Event('error'));
    stream.stop();
    await vi.advanceTimersByTimeAsync(20_000);
    expect(factory).toHaveBeenCalledOnce();
    expect(source.closed).toBe(true);
    expect(states).toHaveBeenLastCalledWith('stopped');
  });
});
