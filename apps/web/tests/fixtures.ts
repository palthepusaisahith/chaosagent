import type {
  Approval,
  GateResult,
  ReportResponse,
  Run,
  RunEvent,
} from '../src/domain';

const digest = `sha256:${'a'.repeat(64)}`;

export function run(overrides: Partial<Run> = {}): Run {
  return {
    run_id: 'run-refund-001',
    status: 'running',
    lifecycle_version: 3,
    attempt: 1,
    scenario: {
      id: 'shipment-refund.ambiguous-timeout',
      revision: '1',
      digest,
    },
    agent_configuration: { id: 'reference-agent', revision: '1', digest },
    fixture: { id: 'failed-shipment', revision: '1', digest },
    fault_seed: 42,
    fault_plan_digest: digest,
    lease_expires_at: '2026-09-06T10:05:00Z',
    heartbeat_at: '2026-09-06T10:00:00Z',
    created_at: '2026-09-06T09:59:00Z',
    created_by: 'test-user',
    ...overrides,
  };
}

export function event(
  sequence: number,
  eventType: string,
  payload: RunEvent['payload'] = {},
  overrides: Partial<RunEvent> = {},
): RunEvent {
  return {
    schema_version: 'chaosagent.run-event/v0',
    event_id: `event-${String(sequence)}`,
    run_id: 'run-refund-001',
    sequence,
    occurred_at: `2026-09-06T10:00:${String(sequence).padStart(2, '0')}Z`,
    recorded_at: `2026-09-06T10:00:${String(sequence).padStart(2, '0')}Z`,
    event_type: eventType,
    producer: { component: 'test-runtime' },
    correlation_id: 'test-correlation',
    payload,
    payload_digest: digest,
    ...overrides,
  };
}

export function gate(overrides: Partial<GateResult> = {}): GateResult {
  return {
    gate_id: 'single_refund_effect',
    status: 'pass',
    evaluator: { id: 'refund-effect-evaluator', revision: '1', digest },
    evidence: [{ kind: 'event', event_id: 'event-2', sequence: 2 }],
    ...overrides,
  };
}

export function report(
  classification: 'pass' | 'fail' | 'invalid' = 'pass',
  gates: GateResult[] = [
    gate({ gate_id: 'required_refund_state' }),
    gate({ gate_id: 'no_duplicate_refund_effect' }),
  ],
): ReportResponse {
  return {
    run_id: 'run-refund-001',
    kind: 'run_report',
    document_id: 'report-1',
    digest,
    inserted_at: '2026-09-06T10:01:00Z',
    document: {
      schema_version: 'chaosagent.run-report/v0',
      report_id: 'report-1',
      run_id: 'run-refund-001',
      generated_at: '2026-09-06T10:01:00Z',
      scenario: {
        id: 'shipment-refund.ambiguous-timeout',
        revision: '1',
        digest,
      },
      agent_configuration: { id: 'reference-agent', revision: '1', digest },
      run_status: classification === 'pass' ? 'completed' : 'failed',
      classification,
      evidence_boundary: {
        first_sequence: 1,
        last_sequence: 20,
        event_count: 20,
      },
      fault_observation: {
        status: 'observed',
        fault_ids: ['refund-ack-lost'],
        evidence: [{ kind: 'event', event_id: 'event-4', sequence: 4 }],
      },
      critical_gates: gates,
      diagnostic_metrics: [],
      totals: {},
      provenance: {},
    },
  };
}

export function flagshipEvents(): RunEvent[] {
  return [
    event(
      1,
      'tool.requested',
      {
        logical_call_id: 'refund-call',
        attempt_id: 'attempt-1',
        attempt_number: 1,
        tool_id: 'payments.refund',
        arguments_digest: digest,
        idempotency_key_digest: digest,
      },
      { correlation_id: 'refund-call' },
    ),
    event(
      2,
      'state.evidence_recorded',
      {
        evidence_id: 'effect-refund-1',
        evidence_kind: 'business_effect',
        fact_type: 'refund.created',
        subject: { type: 'order', id: 'ORDER-1' },
        related_event_ids: ['event-1'],
      },
      { correlation_id: 'refund-call' },
    ),
    event(
      3,
      'fault.applied',
      {
        fault_id: 'refund-ack-lost',
        activation_id: 'activation-1',
        related_event_ids: ['event-1'],
      },
      { correlation_id: 'refund-call' },
    ),
    event(
      4,
      'tool.result',
      {
        logical_call_id: 'refund-call',
        request_event_id: 'event-1',
        attempt_id: 'attempt-1',
        attempt_number: 1,
        tool_id: 'payments.refund',
        outcome: 'unknown',
        duration_ms: 5000,
        error_code: 'acknowledgement_timeout',
      },
      { correlation_id: 'refund-call', causation_event_id: 'event-3' },
    ),
    event(
      5,
      'tool.requested',
      {
        logical_call_id: 'refund-call',
        attempt_id: 'attempt-2',
        attempt_number: 2,
        tool_id: 'payments.refund',
        arguments_digest: digest,
        idempotency_key_digest: digest,
      },
      { correlation_id: 'refund-call' },
    ),
  ];
}

export function approval(overrides: Partial<Approval> = {}): Approval {
  return {
    approval_id: 'approval-1',
    run_id: 'run-refund-001',
    scenario: {
      id: 'shipment-refund.ambiguous-timeout',
      revision: '1',
      digest,
    },
    policy: { id: 'refund-policy', revision: '1', digest },
    tool_id: 'payments.refund',
    contract_version: 'v0',
    request_digest: digest,
    idempotency_key_digest: digest,
    logical_call_id: 'refund-call',
    requested_attempt_id: 'attempt-1',
    lease_attempt: 1,
    decision_id: 'decision-1',
    decision_event_id: 'decision-event-1',
    request_event_id: 'event-1',
    status: 'pending',
    created_at: '2026-09-06T10:00:00Z',
    resolved_at: null,
    actor_id: null,
    resolution_event_id: null,
    ...overrides,
  };
}
