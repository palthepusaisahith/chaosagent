export type JsonPrimitive = boolean | number | string | null;
export type JsonValue = JsonPrimitive | JsonValue[] | JsonObject;
declare const jsonObjectType: unique symbol;
export interface JsonObject extends Record<string, JsonValue> {
  readonly [jsonObjectType]?: never;
}

export interface RevisionReference {
  id: string;
  revision: string;
  digest: string;
}

export type RunStatus =
  | 'queued'
  | 'provisioning'
  | 'running'
  | 'evaluating'
  | 'completed'
  | 'failed'
  | 'timed_out'
  | 'cancelled'
  | 'infra_error';

export type EvaluationClassification =
  'pass' | 'fail' | 'invalid' | 'not_evaluated';

export interface Run {
  run_id: string;
  status: RunStatus;
  lifecycle_version: number;
  attempt: number;
  scenario: RevisionReference;
  agent_configuration: RevisionReference;
  fixture: RevisionReference | null;
  fault_seed: number | null;
  fault_plan_digest: string | null;
  lease_expires_at: string | null;
  heartbeat_at: string | null;
  created_at: string;
  created_by: string;
}

export interface RunCreateInput {
  run_id: string;
  scenario: RevisionReference;
  agent_configuration: RevisionReference;
  created_by: string;
}

export interface RunEvent {
  schema_version: string;
  event_id: string;
  run_id: string;
  sequence: number;
  occurred_at: string;
  recorded_at: string;
  event_type: string;
  producer: { component: string; instance_id?: string };
  correlation_id: string;
  causation_event_id?: string;
  trace_context?: JsonObject;
  payload: JsonObject;
  payload_digest: string;
}

export interface EventPage {
  run_id: string;
  events: RunEvent[];
  next_cursor: string | null;
  has_more: boolean;
}

export interface EvidenceReference {
  kind: 'artifact' | 'event' | 'event_range';
  event_id?: string;
  sequence?: number;
  start_sequence?: number;
  end_sequence?: number;
  artifact_id?: string;
  digest?: string;
  media_type?: string;
}

export interface GateResult {
  gate_id: string;
  status: 'pass' | 'fail' | 'not_applicable' | 'error';
  evaluator: RevisionReference;
  evidence: EvidenceReference[];
  reason_code?: string;
}

export interface DiagnosticMetric {
  metric_id: string;
  value: number;
  unit: string;
  interpretation:
    'lower_better' | 'higher_better' | 'informational' | 'neutral';
  evidence: EvidenceReference[];
}

export interface RunReportDocument {
  schema_version: string;
  report_id: string;
  run_id: string;
  generated_at: string;
  scenario: RevisionReference;
  agent_configuration: RevisionReference;
  run_status: Exclude<
    RunStatus,
    'queued' | 'provisioning' | 'running' | 'evaluating'
  >;
  classification: EvaluationClassification;
  evidence_boundary: {
    first_sequence: number;
    last_sequence: number;
    event_count: number;
  };
  fault_observation: {
    status:
      | 'not_applicable'
      | 'not_matched'
      | 'matched_not_applied'
      | 'applied_not_observed'
      | 'observed'
      | 'mixed';
    fault_ids: string[];
    evidence: EvidenceReference[];
  };
  critical_gates: GateResult[];
  diagnostic_metrics: DiagnosticMetric[];
  totals: JsonObject;
  provenance: JsonObject;
}

export interface EvaluationResultDocument {
  schema_version: string;
  evaluation_id: string;
  run_id: string;
  evaluator: RevisionReference;
  input_digest: string;
  classification: EvaluationClassification;
  critical_gates: GateResult[];
  diagnostic_metrics: DiagnosticMetric[];
  evidence_through_sequence: number;
  error_code?: string;
}

export interface ReportResponse {
  run_id: string;
  kind: 'run_report' | 'evaluation_result';
  document_id: string;
  digest: string;
  inserted_at: string | null;
  document: RunReportDocument | EvaluationResultDocument;
}

export interface Approval {
  approval_id: string;
  run_id: string;
  scenario: RevisionReference;
  policy: RevisionReference;
  tool_id: string;
  contract_version: string;
  request_digest: string;
  idempotency_key_digest: string;
  logical_call_id: string;
  requested_attempt_id: string;
  lease_attempt: number;
  decision_id: string;
  decision_event_id: string;
  request_event_id: string;
  status: 'pending' | 'approved' | 'denied';
  created_at: string;
  resolved_at: string | null;
  actor_id: string | null;
  resolution_event_id: string | null;
}

export interface ApprovalList {
  run_id: string;
  approvals: Approval[];
}

export interface CampaignAssignment {
  trial_index: number;
  run_id: string;
}

export interface Campaign {
  schema_version: string;
  campaign_id: string;
  arm: 'baseline' | 'faulted';
  planned_trials: number;
  scenario: RevisionReference;
  agent_configuration: RevisionReference;
  selected_fault_ids: string[];
  fault_plan_digest: string;
  assignments: CampaignAssignment[];
  digest: string;
  created_at: string;
}

export interface CampaignDocumentResponse {
  campaign_id: string;
  digest: string;
  document: JsonObject;
}

export interface CampaignComparisonResponse {
  baseline_campaign_id: string;
  faulted_campaign_id: string;
  digest: string;
  document: JsonObject;
}

export interface ApiErrorEnvelope {
  error: { code: string; message: string; details?: JsonObject | null };
}

export const terminalStatuses = new Set<RunStatus>([
  'completed',
  'failed',
  'timed_out',
  'cancelled',
  'infra_error',
]);

export function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function readString(
  object: JsonObject,
  key: string,
): string | undefined {
  const value = object[key];
  return typeof value === 'string' ? value : undefined;
}

export function readNumber(
  object: JsonObject,
  key: string,
): number | undefined {
  const value = object[key];
  return typeof value === 'number' && Number.isFinite(value)
    ? value
    : undefined;
}
