import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import {
  EmptyState,
  EvaluationPanel,
  EventTimeline,
  ExactlyOncePanel,
  FlagshipStory,
  ApprovalsPanel,
  RawEvidence,
  StatusBadge,
  RunCreationForm,
} from '../src/components';
import {
  deriveExactlyOnceProof,
  deriveFlagshipStory,
  evidenceCutoff,
  sortAndMergeEvents,
} from '../src/presentation';
import type { ReportResponse } from '../src/domain';
import { approval, event, flagshipEvents, gate, report } from './fixtures';

describe('evidence presentation', () => {
  it('renders hostile raw evidence as inert text without executable DOM', () => {
    const hostile =
      '<script>globalThis.pwned=true</script><a href="javascript:alert(1)">PASS</a>\n\nid: forged';
    const hostileEvent = event(1, 'agent.step', { text: hostile });
    const { container } = render(
      <RawEvidence events={[hostileEvent]} report={null} />,
    );
    expect(
      screen.getByText(/globalThis\.pwned=true/, { exact: false }),
    ).toBeInTheDocument();
    expect(container.querySelector('script')).toBeNull();
    expect(container.querySelector('a')).toBeNull();
    const serialized = container.querySelector('pre')?.textContent;
    expect(serialized).toContain('<script>globalThis.pwned=true</script>');
    expect(serialized).toContain('href=\\"javascript:alert(1)\\"');
    expect(serialized).toContain('\\n\\nid: forged');
  });

  it('constructs Run creation from every explicit immutable reference field', async () => {
    const create = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    render(<RunCreationForm busy={false} onCreate={create} />);
    await user.type(screen.getByLabelText('Run ID'), 'run-new');
    await user.type(screen.getByLabelText('Created by'), 'operator-7');
    await user.clear(screen.getByLabelText('Scenario ID'));
    await user.type(screen.getByLabelText('Scenario ID'), 'scenario-a');
    await user.clear(
      screen.getByLabelText('Revision', { selector: '#scenario-revision' }),
    );
    await user.type(
      screen.getByLabelText('Revision', { selector: '#scenario-revision' }),
      '7',
    );
    await user.type(
      screen.getByLabelText('SHA-256 digest', { selector: '#scenario-digest' }),
      `sha256:${'1'.repeat(64)}`,
    );
    await user.type(screen.getByLabelText('Configuration ID'), 'agent-a');
    await user.type(
      screen.getByLabelText('Revision', { selector: '#agent-revision' }),
      '3',
    );
    await user.type(
      screen.getByLabelText('SHA-256 digest', { selector: '#agent-digest' }),
      `sha256:${'2'.repeat(64)}`,
    );
    await user.click(screen.getByRole('button', { name: /start run/i }));
    expect(create).toHaveBeenCalledWith({
      run_id: 'run-new',
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
      created_by: 'operator-7',
    });
  });
  it('preserves authoritative sequence ordering while deduplicating replay', () => {
    const three = event(3, 'run.lifecycle');
    const one = event(1, 'run.lifecycle');
    const two = event(2, 'agent.step');
    expect(
      sortAndMergeEvents([three, one], [two, one]).map((item) => item.sequence),
    ).toEqual([1, 2, 3]);
  });

  it('keeps PASS, FAIL, and INVALID visually and textually distinct', () => {
    render(
      <div>
        <StatusBadge value="pass" />
        <StatusBadge value="fail" />
        <StatusBadge value="invalid" />
      </div>,
    );
    expect(screen.getByText('PASS')).toHaveClass('status--pass');
    expect(screen.getByText('FAIL')).toHaveClass('status--fail');
    expect(screen.getByText('INVALID')).toHaveClass('status--invalid');
  });

  it('describes an applied fault separately from a failed mutation', () => {
    render(
      <EventTimeline
        events={[event(1, 'fault.applied', { fault_id: 'refund-ack-lost' })]}
      />,
    );
    const row = screen.getByTestId('timeline-event');
    expect(row).toHaveClass('timeline-event--fault');
    expect(screen.getByText('refund-ack-lost applied')).toBeVisible();
    expect(screen.queryByText(/refund failed/i)).not.toBeInTheDocument();
  });

  it('proves exactly once only when evaluator and authoritative effect evidence agree', () => {
    const events = flagshipEvents();
    expect(deriveExactlyOnceProof(events, null).authoritative).toBe(false);
    expect(deriveExactlyOnceProof(events, report()).authoritative).toBe(true);
    expect(
      deriveExactlyOnceProof(
        [
          ...events,
          event(6, 'state.evidence_recorded', {
            evidence_id: 'effect-refund-2',
            evidence_kind: 'business_effect',
            fact_type: 'refund.created',
            subject: { type: 'order', id: 'ORDER-1' },
            related_event_ids: ['event-5'],
          }),
        ],
        report(),
      ).authoritative,
    ).toBe(false);
    expect(
      deriveExactlyOnceProof(
        [
          ...events,
          event(21, 'state.evidence_recorded', {
            evidence_id: 'effect-after-evaluation',
            evidence_kind: 'business_effect',
            fact_type: 'refund.created',
            subject: { type: 'order', id: 'ORDER-1' },
            related_event_ids: ['event-20'],
          }),
        ],
        report(),
      ).authoritative,
    ).toBe(true);
  });

  it('requires an overall PASS classification for exactly-once proof', () => {
    const events = flagshipEvents();
    expect(deriveExactlyOnceProof(events, report('fail')).authoritative).toBe(
      false,
    );
    expect(
      deriveExactlyOnceProof(events, report('invalid')).authoritative,
    ).toBe(false);
  });

  it('uses the frozen cutoff from both report document forms', () => {
    const runReport = report();
    const evaluationResult = evaluationResultReport(5);
    expect(evidenceCutoff(runReport)).toBe(20);
    expect(evidenceCutoff(evaluationResult)).toBe(5);
    const duplicate = event(6, 'state.evidence_recorded', {
      evidence_id: 'effect-refund-2',
      evidence_kind: 'business_effect',
      fact_type: 'refund.created',
      subject: { type: 'order', id: 'ORDER-1' },
      related_event_ids: ['event-1'],
    });
    expect(
      deriveExactlyOnceProof([...flagshipEvents(), duplicate], evaluationResult)
        .authoritative,
    ).toBe(true);
    expect(
      deriveExactlyOnceProof([...flagshipEvents(), duplicate], report())
        .authoritative,
    ).toBe(false);
  });

  it('does not render a fabricated proof without the evaluator gate', () => {
    render(
      <ExactlyOncePanel
        events={flagshipEvents()}
        report={report('pass', [gate({ gate_id: 'fault_observed' })])}
      />,
    );
    expect(screen.getByText('Exactly-once proof pending')).toBeVisible();
    expect(
      screen.queryByText('Exactly one refund, proven'),
    ).not.toBeInTheDocument();
  });

  it('renders only flagship steps supported by actual evidence', () => {
    const complete = deriveFlagshipStory(flagshipEvents(), report());
    expect(
      complete
        .filter((step) => step.state === 'complete')
        .map((step) => step.id),
    ).toEqual([
      'request',
      'commit',
      'fault',
      'recovery',
      'dedupe',
      'proof',
      'verdict',
    ]);
    const absent = deriveFlagshipStory(
      [event(1, 'tool.requested', { tool_id: 'orders.get' })],
      null,
    );
    expect(absent.every((step) => step.state === 'absent')).toBe(true);
    const { container } = render(<FlagshipStory events={[]} report={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('does not combine unrelated refund attempts into one causal story', () => {
    const events = flagshipEvents();
    const unrelatedEffect = event(2, 'state.evidence_recorded', {
      ...events[1]?.payload,
      related_event_ids: ['other-request'],
    });
    const mismatchedResult = event(
      4,
      'tool.result',
      { ...events[3]?.payload, request_event_id: 'other-request' },
      { correlation_id: 'refund-call', causation_event_id: 'event-3' },
    );
    const unrelatedFault = event(
      3,
      'fault.applied',
      { ...events[2]?.payload, related_event_ids: ['other-request'] },
      { correlation_id: 'refund-call' },
    );
    const effectSteps = deriveFlagshipStory(
      [events[0]!, unrelatedEffect, ...events.slice(2)],
      report(),
    );
    const resultSteps = deriveFlagshipStory(
      [...events.slice(0, 3), mismatchedResult, events[4]!],
      report(),
    );
    const faultSteps = deriveFlagshipStory(
      [events[0]!, events[1]!, unrelatedFault, ...events.slice(3)],
      report(),
    );
    const earlyRetrySteps = deriveFlagshipStory(
      [
        events[0]!,
        events[1]!,
        event(3, 'tool.requested', events[4]!.payload, {
          correlation_id: 'refund-call',
        }),
        event(4, 'fault.applied', events[2]!.payload, {
          correlation_id: 'refund-call',
        }),
        event(5, 'tool.result', events[3]!.payload, {
          correlation_id: 'refund-call',
          causation_event_id: 'event-4',
        }),
      ],
      report(),
    );
    expect(effectSteps.find((step) => step.id === 'commit')?.state).toBe(
      'absent',
    );
    expect(resultSteps.find((step) => step.id === 'fault')?.state).toBe(
      'absent',
    );
    expect(faultSteps.find((step) => step.id === 'fault')?.state).toBe(
      'absent',
    );
    expect(earlyRetrySteps.find((step) => step.id === 'recovery')?.state).toBe(
      'absent',
    );
  });

  it('labels PASS, FAIL, INVALID, and unevaluated stories truthfully', () => {
    expect(
      deriveFlagshipStory(flagshipEvents(), report('pass')).at(-1)?.label,
    ).toBe('Evaluation passed');
    expect(
      deriveFlagshipStory(flagshipEvents(), report('fail')).at(-1)?.label,
    ).toBe('Evaluation failed');
    expect(
      deriveFlagshipStory(flagshipEvents(), report('invalid')).at(-1)?.label,
    ).toBe('Evaluation invalid');
    expect(deriveFlagshipStory(flagshipEvents(), null).at(-1)?.label).toBe(
      'Evaluation pending',
    );
  });

  it('uses an explicit human actor for approval and denial', async () => {
    const resolve = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    const { rerender } = render(
      <ApprovalsPanel
        approvals={[approval()]}
        busyId={null}
        onResolve={resolve}
      />,
    );
    expect(screen.getByRole('button', { name: 'Approve' })).toBeDisabled();
    await user.type(screen.getByLabelText('Human responder ID'), 'reviewer-9');
    await user.click(screen.getByRole('button', { name: 'Approve' }));
    expect(resolve).toHaveBeenLastCalledWith(
      expect.objectContaining({ approval_id: 'approval-1' }),
      'approved',
      'reviewer-9',
    );
    rerender(
      <ApprovalsPanel
        approvals={[approval()]}
        busyId={null}
        onResolve={resolve}
      />,
    );
    await user.click(screen.getByRole('button', { name: 'Deny' }));
    expect(resolve).toHaveBeenLastCalledWith(
      expect.objectContaining({ approval_id: 'approval-1' }),
      'denied',
      'reviewer-9',
    );
  });

  it('renders classification-specific evaluator language', () => {
    const { rerender } = render(
      <EvaluationPanel report={report('fail', [gate({ status: 'fail' })])} />,
    );
    expect(screen.getByText(/at least one critical business/i)).toBeVisible();
    rerender(
      <EvaluationPanel
        report={report('invalid', [gate({ status: 'error' })])}
      />,
    );
    expect(screen.getByText(/not an ordinary failure/i)).toBeVisible();
    rerender(<EvaluationPanel report={null} />);
    expect(screen.getByText('Evaluation not available')).toBeVisible();
  });

  it('renders polished empty state copy', () => {
    render(<EmptyState title="No Runs loaded" body="Open a persisted Run." />);
    expect(
      screen.getByRole('heading', { name: 'No Runs loaded' }),
    ).toBeVisible();
  });
});

function evaluationResultReport(
  evidenceThroughSequence: number,
): ReportResponse {
  const base = report();
  return {
    ...base,
    kind: 'evaluation_result',
    document_id: 'evaluation-1',
    inserted_at: null,
    document: {
      schema_version: 'chaosagent.evaluation-result/v0',
      evaluation_id: 'evaluation-1',
      run_id: base.run_id,
      evaluator: {
        id: 'chaosagent.critical-evaluator',
        revision: 'v0',
        digest: base.digest,
      },
      input_digest: base.digest,
      classification: 'pass',
      critical_gates: base.document.critical_gates,
      diagnostic_metrics: [],
      evidence_through_sequence: evidenceThroughSequence,
    },
  };
}
