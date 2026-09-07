import type { RunEvent } from './domain';

export interface EventSourceMessage {
  data: string;
  lastEventId: string;
}

export interface EventSourceLike {
  close(): void;
  addEventListener(
    type: string,
    listener: (event: EventSourceMessage) => void,
  ): void;
  onerror: ((event: Event) => void) | null;
}

export type EventSourceFactory = (url: string) => EventSourceLike;

export interface RunStreamCallbacks {
  onEvent: (event: RunEvent, cursor: string) => void;
  onState: (state: 'connected' | 'reconnecting' | 'stopped') => void;
  onError: (message: string) => void;
}

const EVENT_TYPES = [
  'run.lifecycle',
  'agent.step',
  'tool.requested',
  'tool.result',
  'fault.not_matched',
  'fault.matched',
  'fault.applied',
  'fault.observed',
  'state.evidence_recorded',
  'policy.decision',
  'approval.requested',
  'approval.resolved',
  'evaluation.started',
  'evaluation.result_recorded',
  'run.error',
] as const;

export class ReplaySafeRunStream {
  private source: EventSourceLike | null = null;
  private stopped = true;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly seenEventIds = new Map<string, number>();
  private readonly seenSequences = new Map<number, string>();

  public constructor(
    private readonly streamUrl: string,
    private readonly expectedRunId: string,
    private readonly factory: EventSourceFactory = (url) =>
      new EventSource(url),
    private readonly callbacks: RunStreamCallbacks,
    private cursor: string | null,
    persistedEvents: readonly RunEvent[] = [],
  ) {
    for (const event of persistedEvents) {
      this.seenEventIds.set(event.event_id, event.sequence);
      this.seenSequences.set(event.sequence, event.event_id);
    }
  }

  public start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  public stop(): void {
    this.stopped = true;
    this.source?.close();
    this.source = null;
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.callbacks.onState('stopped');
  }

  private connect(): void {
    if (this.stopped) return;
    const url = new URL(this.streamUrl, window.location.origin);
    if (this.cursor !== null) url.searchParams.set('cursor', this.cursor);
    this.source = this.factory(url.toString());
    this.callbacks.onState('connected');
    const receive = (
      expectedEventType: string,
      message: EventSourceMessage,
    ): void => {
      let event: RunEvent;
      try {
        const parsed: unknown = JSON.parse(message.data);
        if (!isRunEventEnvelope(parsed, this.expectedRunId, expectedEventType))
          throw new Error('invalid event envelope');
        event = parsed;
      } catch {
        this.callbacks.onError(
          'The live stream returned an invalid event envelope.',
        );
        return;
      }
      if (message.lastEventId.length === 0) {
        this.callbacks.onError(
          'The live stream returned an invalid event envelope.',
        );
        return;
      }
      const existingSequence = this.seenEventIds.get(event.event_id);
      const existingEventId = this.seenSequences.get(event.sequence);
      if (existingSequence !== undefined || existingEventId !== undefined) {
        if (
          existingSequence === event.sequence &&
          existingEventId === event.event_id
        ) {
          this.cursor = message.lastEventId;
          this.reconnectAttempt = 0;
          return;
        }
        this.callbacks.onError(
          'The live stream contradicted persisted event identity.',
        );
        this.stop();
        return;
      }
      this.cursor = message.lastEventId;
      this.reconnectAttempt = 0;
      this.seenEventIds.set(event.event_id, event.sequence);
      this.seenSequences.set(event.sequence, event.event_id);
      this.callbacks.onEvent(event, message.lastEventId);
    };
    for (const eventType of EVENT_TYPES) {
      this.source.addEventListener(eventType, (message) => {
        receive(eventType, message);
      });
    }
    this.source.onerror = () => {
      this.source?.close();
      this.source = null;
      if (this.stopped) return;
      this.callbacks.onState('reconnecting');
      const delay = Math.min(1_000 * 2 ** this.reconnectAttempt, 15_000);
      this.reconnectAttempt += 1;
      this.reconnectTimer = setTimeout(() => {
        this.connect();
      }, delay);
    };
  }
}

function isRunEventEnvelope(
  value: unknown,
  expectedRunId: string,
  expectedEventType: string,
): value is RunEvent {
  if (!isRecord(value)) return false;
  const producer = value.producer;
  const payload = value.payload;
  return (
    value.schema_version === 'chaosagent.run-event/v0' &&
    nonEmptyString(value.event_id) &&
    value.run_id === expectedRunId &&
    Number.isSafeInteger(value.sequence) &&
    typeof value.sequence === 'number' &&
    value.sequence >= 1 &&
    utcTimestamp(value.occurred_at) &&
    utcTimestamp(value.recorded_at) &&
    value.event_type === expectedEventType &&
    isRecord(producer) &&
    nonEmptyString(producer.component) &&
    (producer.instance_id === undefined ||
      nonEmptyString(producer.instance_id)) &&
    nonEmptyString(value.correlation_id) &&
    (value.causation_event_id === undefined ||
      nonEmptyString(value.causation_event_id)) &&
    isRecord(payload) &&
    typeof value.payload_digest === 'string' &&
    /^sha256:[0-9a-f]{64}$/.test(value.payload_digest)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function utcTimestamp(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.endsWith('Z') &&
    Number.isFinite(Date.parse(value))
  );
}
