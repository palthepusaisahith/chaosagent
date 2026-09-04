# Campaign statistics and comparison v0

Issue #17 is a pure deterministic aggregation layer over Issue #16 evaluation
truth. It does not create Campaigns, schedule Runs, persist aggregates, build
Run Reports, export bundles, expose APIs, or implement UI behavior.

## Trust and membership boundary

`authenticated_campaign_plan` freezes a contiguous Run-to-index mapping in
PostgreSQL while every Run row is locked and still queued, before outcomes can
influence grouping. The immutable plan binds Campaign ID and arm, Scenario and
Agent Configuration, selected faults and the compiled fault-plan digest, planned
indexes, and Run identities. A Run cannot be rebound by another thread, process,
or restarted service, and one Campaign index cannot name two Runs.

`authenticated_campaign_trial` then accepts that plan, a persistence repository,
and a completed Run identity, not an `EvaluationInput`. It reconstructs the
Issue #16 input from the Run's immutable Scenario, Fixture, checkpoint, effects,
approvals, and Event stream, then requires the recomputed evaluation identity to
match the committed `evaluation.result_recorded` evidence and terminal lifecycle
causation. A caller-created semantically coherent snapshot therefore cannot
enter the public Campaign boundary.

The trial permanently binds its durable plan's Campaign ID and arm, planned
trial index, Run, Scenario and Agent Configuration revisions, selected faults,
evaluator identity, input digest, evidence boundary, and canonical Evaluation
Result. Trial integrity seals reject fabricated or mutated evaluation wrappers,
while Campaign authority is always reconstructed from PostgreSQL. Observed fault
IDs come only from passing `fault_observed` Ground Truth gates in that
reconstructed result.

`campaign_cohort_v0` freezes one arm's Campaign ID, role, Scenario and Agent
Configuration revision references, available and selected fault IDs, planned
trial count, and Run/trial-index membership. It rejects duplicate Run IDs,
duplicate indexes, mixed revisions, malformed evaluation wrappers, and selected
faults absent from the Scenario catalog. A trial bound to Campaign B cannot be
supplied to Campaign A. A comparison additionally rejects reuse of one Run
across arms, equal Campaign IDs, and requires exact Scenario and Agent
Configuration equality.

Issue #17 stores only the narrow immutable Campaign plan and Run/index
memberships required to authenticate aggregation. This is not Campaign
orchestration or an export manifest. A rolled-back caller transaction leaves no
membership, while committed authority survives process restart. Runtime and
Gateway entry points bind the exact compiled fault-plan digest onto the Run and
reject baseline/faulted assignment mismatches before execution. Arbitrary
aggregate JSON can be checked for contract arithmetic but is not treated as
proof of Run provenance.

## Counts and denominators

Every summary reports:

- `total_runs`: every frozen member;
- `valid_evaluated`: `pass + fail`;
- separate `pass`, `fail`, and `invalid` counts.

The pass-rate denominator is only `valid_evaluated`. Invalid Runs remain visible
but are never silently counted as failures or successes. A zero denominator is
represented as `unavailable` with `zero_denominator`; it is never encoded as
zero, NaN, or infinity.

Every nonempty binomial rate includes `n`, successes, a point estimate, and a
95% Wilson score interval. Calculations use Python `Decimal` with a frozen
`1.959963984540054` normal quantile and round half-even to twelve decimal
places. Durable numbers are decimal strings, avoiding platform-dependent JSON
float spellings. The empirical predetermined-group `pass_power_k` rate also
includes a Wilson interval over its valid groups; the exact combinatorial
`pass_at_k` estimator is not misrepresented as an independent binomial sample.

## pass@k and pass^k

`pass@k` is the exact finite-sample estimator

```text
1 - C(n - c, k) / C(n, k)
```

where `n` is valid evaluated Runs and `c` is PASS Runs. It is unavailable when
`k > n`; observations are never fabricated.

`pass_power_k` is the architecture's empirical `pass^k`. Group membership is
fixed by the planned zero-based index space: index `i` belongs to group
`floor(i / k)`. Missing Runs never compact the sequence or shift later members.
A complete group succeeds only when all members PASS. Groups with missing or
INVALID members are separately excluded and counted; an incomplete final planned
group is counted as discarded. The result is explicitly labeled
`predetermined_groups`, never confused with the independence-model estimate
`p_hat^k`.

## Fault conditioning and pairing

A faulted Run enters the conditioned denominator only when every selected fault
ID is established by authenticated Issue #16 fault-observation gates. Merely
declaring or selecting a fault is insufficient. Empty conditioned sets remain
explicitly unavailable.

Baseline and faulted arms pair only by the same frozen trial index after exact
Scenario and Agent Configuration compatibility checks. The comparison reports
improvements (`fail -> pass`), regressions (`pass -> fail`), unchanged valid
pairs, invalid pairs, missing indexes, and the fault-minus-baseline pass delta.
It also reports the same delta conditioned on observed faults. These are
controlled pairs, not claims of identical provider randomness or statistical
significance.

## Canonical results and warnings

Campaign Statistics v0 and Campaign Comparison v0 are strict Draft 2020-12
contracts. Unknown properties and unsupported versions fail closed. Arrays are
emitted in stable trial, `k`, fault-ID, and warning order. Semantic validation
recomputes counts, denominators, formulas, warnings, IDs, and paired deltas. RFC
8785 canonical bytes plus SHA-256 form their stable digest.

Warnings are deterministic and do not alter results. V0 warns for samples below
30, invalid evaluations/groups, incomplete groups, insufficient `k`, missing or
invalid pairs, and empty observed-fault sets. No significance claim or bootstrap
interval is produced for the single V1 task family.

## Deferred

Issue #18 owns manifests, checksums, archives, and export. Issues #19–#22 own
telemetry, REST/SSE, UI, and sandbox hardening. Campaign orchestration, derived
trial seeds, provider calls, Run Report finalization, bootstrap inference across
a multi-task suite, and model judging are absent.
