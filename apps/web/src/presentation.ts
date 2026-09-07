import type {
  EvaluationClassification,
  GateResult,
  JsonObject,
  ReportResponse,
  RunEvent,
} from './domain';
import { readNumber, readString } from './domain';

export type EventCategory =
  | 'lifecycle'
  | 'model'
  | 'tool'
  | 'effect'
  | 'policy'
  | 'approval'
  | 'fault'
  | 'evaluation'
  | 'other';

export interface StoryStep {
  id:
    | 'request'
    | 'commit'
    | 'fault'
    | 'recovery'
    | 'dedupe'
    | 'proof'
    | 'verdict';
  label: string;
  detail: string;
  sequence: number | null;
  state: 'complete' | 'absent';
}

export interface ExactlyOnceProof {
  authoritative: boolean;
  expectedEffects: 1;
  observedEffects: number;
  duplicateDisposition: 'prevented_or_reconciled' | 'not_proven';
  gates: GateResult[];
}

export function eventCategory(eventType: string): EventCategory {
  if (eventType.startsWith('run.')) return 'lifecycle';
  if (eventType.startsWith('agent.') || eventType.startsWith('model.'))
    return 'model';
  if (eventType.startsWith('tool.')) return 'tool';
  if (eventType.startsWith('state.')) return 'effect';
  if (eventType.startsWith('policy.')) return 'policy';
  if (eventType.startsWith('approval.')) return 'approval';
  if (eventType.startsWith('fault.')) return 'fault';
  if (eventType.startsWith('evaluation.')) return 'evaluation';
  return 'other';
}

export function eventSummary(event: RunEvent): string {
  const payload = event.payload;
  const tool = readString(payload, 'tool_id');
  const outcome = readString(payload, 'outcome');
  const fault = readString(payload, 'fault_id');
  const decision = readString(payload, 'decision');
  const fact = readString(payload, 'fact_type');
  const phase = readString(payload, 'phase');
  const status =
    readString(payload, 'state') ??
    readString(payload, 'to_status') ??
    readString(payload, 'status');
  if (event.event_type === 'tool.requested')
    return `Requested ${tool ?? 'tool'}`;
  if (event.event_type === 'tool.result') {
    const duration = readNumber(payload, 'duration_ms');
    return `${tool ?? 'Tool'} returned ${outcome ?? 'a result'}${duration === undefined ? '' : ` in ${String(duration)} ms`}`;
  }
  if (event.event_type === 'state.evidence_recorded') {
    return `Authoritative state recorded: ${fact ?? 'business fact'}`;
  }
  if (event.event_type.startsWith('fault.')) {
    const verb =
      event.event_type.split('.')[1]?.replaceAll('_', ' ') ?? 'recorded';
    return `${fault ?? 'Fault'} ${verb}`;
  }
  if (event.event_type === 'policy.decision')
    return `Policy decision: ${decision ?? 'recorded'}`;
  if (event.event_type.startsWith('approval.'))
    return `Approval ${event.event_type.split('.')[1] ?? 'updated'}`;
  if (event.event_type.startsWith('evaluation.'))
    return `Evaluation ${phase ?? outcome ?? 'updated'}`;
  if (event.event_type.startsWith('run.'))
    return `Run ${status ?? event.event_type.split('.')[1] ?? 'updated'}`;
  if (event.event_type.startsWith('agent.'))
    return `Agent step ${phase ?? 'recorded'}`;
  return event.event_type.replaceAll('.', ' · ');
}

export function reportClassification(
  report: ReportResponse | null,
): EvaluationClassification | null {
  if (report === null) return null;
  const value = report.document.classification;
  return value;
}

export function reportGates(report: ReportResponse | null): GateResult[] {
  if (report === null || !Array.isArray(report.document.critical_gates))
    return [];
  return report.document.critical_gates;
}

export function deriveExactlyOnceProof(
  events: readonly RunEvent[],
  report: ReportResponse | null,
): ExactlyOnceProof {
  const gates = reportGates(report);
  const combinedGate =
    gates.find((item) =>
      [
        'single_refund_effect',
        'exactly_once_business_effect',
        'exactly_once_effect',
      ].includes(item.gate_id),
    ) ?? null;
  const refundStateGate =
    gates.find((item) => item.gate_id === 'required_refund_state') ?? null;
  const duplicateGate =
    gates.find((item) => item.gate_id === 'no_duplicate_refund_effect') ?? null;
  const proofGates =
    combinedGate === null
      ? [refundStateGate, duplicateGate].filter(
          (item): item is GateResult => item !== null,
        )
      : [combinedGate];
  const boundary = evidenceCutoff(report);
  const boundedEvents =
    boundary === null
      ? []
      : events.filter((event) => event.sequence <= boundary);
  const committed = boundedEvents.filter(
    (event) =>
      event.event_type === 'state.evidence_recorded' &&
      readString(event.payload, 'evidence_kind') === 'business_effect' &&
      readString(event.payload, 'fact_type') === 'refund.created',
  );
  const distinctEvidence = new Set(
    committed.map(
      (event) => readString(event.payload, 'evidence_id') ?? event.event_id,
    ),
  );
  const retries =
    matchingRefundRetry(refundRequests(boundedEvents)) !== undefined;
  const gatesProveExactlyOnce =
    combinedGate !== null
      ? combinedGate.status === 'pass' && combinedGate.evidence.length > 0
      : refundStateGate?.status === 'pass' &&
        refundStateGate.evidence.length > 0 &&
        duplicateGate?.status === 'pass' &&
        duplicateGate.evidence.length > 0;
  const effectEvent = committed[0];
  const gatesReferenceEffect =
    effectEvent !== undefined &&
    proofGates.length > 0 &&
    proofGates.every((gate) =>
      gate.evidence.some(
        (reference) =>
          reference.kind === 'event' &&
          reference.event_id === effectEvent.event_id &&
          reference.sequence === effectEvent.sequence,
      ),
    );
  const authoritative =
    reportClassification(report) === 'pass' &&
    boundary !== null &&
    gatesProveExactlyOnce &&
    gatesReferenceEffect &&
    distinctEvidence.size === 1;
  return {
    authoritative,
    expectedEffects: 1,
    observedEffects: distinctEvidence.size,
    duplicateDisposition:
      authoritative && retries ? 'prevented_or_reconciled' : 'not_proven',
    gates: proofGates,
  };
}

export function deriveFlagshipStory(
  events: readonly RunEvent[],
  report: ReportResponse | null,
): StoryStep[] {
  const ordered = [...events].sort(
    (left, right) => left.sequence - right.sequence,
  );
  const requests = refundRequests(ordered);
  const chain = selectRefundChain(ordered, requests);
  const { request, committed, fault, ambiguity, retry } = chain;
  const ambiguousAcknowledgement =
    request !== undefined &&
    committed !== undefined &&
    fault !== undefined &&
    ambiguity !== undefined;
  const proof = deriveExactlyOnceProof(ordered, report);
  const classification = reportClassification(report);
  const step = (
    id: StoryStep['id'],
    label: string,
    detail: string,
    event?: RunEvent,
    complete = event !== undefined,
  ): StoryStep => ({
    id,
    label,
    detail,
    sequence: event?.sequence ?? null,
    state: complete ? 'complete' : 'absent',
  });
  return [
    step(
      'request',
      'Refund requested',
      'The agent issued the mutation through the Tool Gateway.',
      request,
    ),
    step(
      'commit',
      'Refund committed',
      'Authoritative business-effect evidence exists.',
      committed,
    ),
    step(
      'fault',
      'Acknowledgement hidden',
      'The response failed after commit; the refund itself did not fail.',
      ambiguity,
      ambiguousAcknowledgement,
    ),
    step(
      'recovery',
      'Agent recovered',
      'A later physical attempt retried the same operation.',
      retry,
    ),
    step(
      'dedupe',
      'Duplicate effect blocked',
      'Retries occurred while authoritative evidence remained exactly one.',
      retry,
      retry !== undefined &&
        proof.authoritative &&
        proof.duplicateDisposition === 'prevented_or_reconciled',
    ),
    step(
      'proof',
      'One refund verified',
      'The deterministic exactly-once gate and state evidence agree.',
      committed,
      proof.authoritative,
    ),
    step(
      'verdict',
      verdictLabel(classification),
      verdictDetail(classification),
      undefined,
      classification !== null && classification !== 'not_evaluated',
    ),
  ];
}

interface RefundChain {
  request: RunEvent | undefined;
  committed: RunEvent | undefined;
  fault: RunEvent | undefined;
  ambiguity: RunEvent | undefined;
  retry: RunEvent | undefined;
}

function selectRefundChain(
  events: readonly RunEvent[],
  requests: readonly RunEvent[],
): RefundChain {
  let selected: RefundChain = {
    request: undefined,
    committed: undefined,
    fault: undefined,
    ambiguity: undefined,
    retry: undefined,
  };
  let selectedScore = -1;
  for (const request of requests) {
    const logicalCall = readString(request.payload, 'logical_call_id');
    const committed = events.find(
      (event) =>
        event.sequence > request.sequence &&
        event.event_type === 'state.evidence_recorded' &&
        readString(event.payload, 'evidence_kind') === 'business_effect' &&
        readString(event.payload, 'fact_type') === 'refund.created' &&
        relatedTo(event, request.event_id) &&
        event.correlation_id === logicalCall,
    );
    const fault =
      committed === undefined
        ? undefined
        : events.find(
            (event) =>
              event.sequence > committed.sequence &&
              event.event_type === 'fault.applied' &&
              relatedTo(event, request.event_id) &&
              event.correlation_id === logicalCall,
          );
    const ambiguity =
      fault === undefined
        ? undefined
        : events.find(
            (event) =>
              event.sequence > fault.sequence &&
              event.event_type === 'tool.result' &&
              readString(event.payload, 'tool_id') === 'payments.refund' &&
              readString(event.payload, 'request_event_id') ===
                request.event_id &&
              readString(event.payload, 'logical_call_id') === logicalCall &&
              event.correlation_id === logicalCall &&
              event.causation_event_id === fault.event_id &&
              (readString(event.payload, 'outcome') === 'unknown' ||
                readString(event.payload, 'outcome') === 'timed_out'),
          );
    const retry =
      ambiguity === undefined
        ? undefined
        : matchingRefundRetry(
            requests.filter(
              (candidate) => candidate.sequence > ambiguity.sequence,
            ),
            request,
          );
    const candidate = { request, committed, fault, ambiguity, retry };
    const score = [request, committed, fault, ambiguity, retry].filter(
      Boolean,
    ).length;
    if (score > selectedScore) {
      selected = candidate;
      selectedScore = score;
    }
  }
  return selected;
}

function relatedTo(event: RunEvent, eventId: string): boolean {
  const related = event.payload.related_event_ids;
  return Array.isArray(related) && related.includes(eventId);
}

function refundRequests(events: readonly RunEvent[]): RunEvent[] {
  return events.filter(
    (event) =>
      event.event_type === 'tool.requested' &&
      readString(event.payload, 'tool_id') === 'payments.refund',
  );
}

function matchingRefundRetry(
  requests: readonly RunEvent[],
  operation = requests[0],
): RunEvent | undefined {
  const first = operation;
  if (first === undefined) return undefined;
  const logicalCall = readString(first.payload, 'logical_call_id');
  const idempotencyDigest = readString(first.payload, 'idempotency_key_digest');
  return requests.find(
    (candidate) =>
      candidate.event_id !== first.event_id &&
      ((logicalCall !== undefined &&
        readString(candidate.payload, 'logical_call_id') === logicalCall) ||
        (idempotencyDigest !== undefined &&
          readString(candidate.payload, 'idempotency_key_digest') ===
            idempotencyDigest)),
  );
}

export function evidenceCutoff(report: ReportResponse | null): number | null {
  if (report === null) return null;
  if (
    report.kind === 'evaluation_result' &&
    report.document.schema_version === 'chaosagent.evaluation-result/v0' &&
    'evidence_through_sequence' in report.document
  ) {
    return positiveSafeInteger(report.document.evidence_through_sequence);
  }
  if (
    report.kind !== 'run_report' ||
    report.document.schema_version !== 'chaosagent.run-report/v0' ||
    !('evidence_boundary' in report.document)
  )
    return null;
  const boundary: unknown = report.document.evidence_boundary;
  if (
    typeof boundary !== 'object' ||
    boundary === null ||
    Array.isArray(boundary)
  )
    return null;
  return positiveSafeInteger((boundary as JsonObject).last_sequence);
}

function positiveSafeInteger(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 1
    ? value
    : null;
}

function verdictLabel(classification: EvaluationClassification | null): string {
  if (classification === 'pass') return 'Evaluation passed';
  if (classification === 'fail') return 'Evaluation failed';
  if (classification === 'invalid') return 'Evaluation invalid';
  return 'Evaluation pending';
}

function verdictDetail(
  classification: EvaluationClassification | null,
): string {
  if (classification === 'pass') return 'Every critical gate passed.';
  if (classification === 'fail') return 'At least one critical gate failed.';
  if (classification === 'invalid')
    return 'Authoritative evaluation input was insufficient or invalid.';
  return 'No authoritative evaluation verdict is available yet.';
}

export function sortAndMergeEvents(
  current: readonly RunEvent[],
  incoming: readonly RunEvent[],
): RunEvent[] {
  const byId = new Map(current.map((event) => [event.event_id, event]));
  const occupiedSequences = new Map(
    current.map((event) => [event.sequence, event.event_id]),
  );
  for (const event of incoming) {
    if (byId.has(event.event_id)) continue;
    if (occupiedSequences.has(event.sequence)) continue;
    byId.set(event.event_id, event);
    occupiedSequences.set(event.sequence, event.event_id);
  }
  return [...byId.values()].sort(
    (left, right) => left.sequence - right.sequence,
  );
}
