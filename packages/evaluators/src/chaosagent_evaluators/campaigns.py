"""Deterministic Campaign statistics and paired comparison for Issue #17."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from functools import lru_cache
from importlib.resources import files
from threading import Lock
from types import MappingProxyType
from typing import Literal, cast
from weakref import WeakKeyDictionary

import rfc8785
from chaosagent_faults import FaultRuleValidationError, compile_fault_plan_v0
from chaosagent_persistence import (
    CampaignMembershipConflictError,
    CampaignPlanRecord,
    PersistenceError,
    PersistenceRepository,
)
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from referencing import Registry, Resource
from sqlalchemy.exc import SQLAlchemyError

from .contracts import (
    EvaluationResult,
    EvaluatorValidationError,
    GroundTruth,
    loads_evaluation_result,
)
from .engine import EVALUATOR_REVISION, EvaluationInput, evaluate_critical_gates
from .service import load_authoritative_evaluation_snapshot

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type CampaignArm = Literal["baseline", "faulted"]

CAMPAIGN_STATISTICS_V0_SCHEMA_VERSION = "chaosagent.campaign-statistics/v0"
CAMPAIGN_COMPARISON_V0_SCHEMA_VERSION = "chaosagent.campaign-comparison/v0"
_SAFE_MAX = 9_007_199_254_740_991
_SMALL_SAMPLE_THRESHOLD = 30
_DECIMAL_QUANTUM = Decimal("0.000000000001")
_WILSON_Z_95 = Decimal("1.959963984540054")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CATALOG_ID_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
_REVISION_RE = re.compile(r"^[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


class CampaignValidationError(ValueError):
    """Campaign membership or aggregate output is malformed or contradictory."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = tuple(errors)
        super().__init__("Invalid ChaosAgent Campaign aggregation:\n- " + "\n- ".join(errors))


@dataclass(frozen=True, slots=True, init=False, eq=False, weakref_slot=True)
class CampaignTrial:
    """Opaque repository-authenticated Issue #16 result plus frozen membership."""

    campaign_id: str
    arm: CampaignArm
    run_id: str
    trial_index: int
    planned_trials: int
    scenario: Mapping[str, object]
    agent_configuration: Mapping[str, object]
    selected_fault_ids: tuple[str, ...]
    evaluation_input: EvaluationInput
    evaluation: EvaluationResult
    evaluation_id: str
    evaluation_input_digest: str
    evidence_through_sequence: int
    available_fault_ids: tuple[str, ...]
    observed_fault_ids: tuple[str, ...]

    def __init__(self) -> None:
        raise TypeError("CampaignTrial instances must be created by authenticated_campaign_trial")


@dataclass(frozen=True, slots=True, init=False, eq=False, weakref_slot=True)
class CampaignPlan:
    """Opaque pre-execution binding of queued Runs to one Campaign arm."""

    campaign_id: str
    arm: CampaignArm
    scenario: Mapping[str, object]
    agent_configuration: Mapping[str, object]
    selected_fault_ids: tuple[str, ...]
    assignments: tuple[tuple[int, str], ...]
    fault_plan_digest: str
    canonical_digest: str

    def __init__(self) -> None:
        raise TypeError("CampaignPlan instances must be created by authenticated_campaign_plan")


@dataclass(frozen=True, slots=True)
class CampaignCohort:
    """Frozen membership supplied to the pure Issue #17 aggregation layer."""

    campaign_id: str
    arm: CampaignArm
    scenario: Mapping[str, object]
    agent_configuration: Mapping[str, object]
    available_fault_ids: tuple[str, ...]
    selected_fault_ids: tuple[str, ...]
    planned_trials: int
    trials: tuple[CampaignTrial, ...]


@dataclass(frozen=True, slots=True, init=False)
class CampaignStatistics:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("CampaignStatistics instances must be created by aggregate_campaign_v0")

    def to_dict(self) -> dict[str, object]:
        return _canonical_object(self.canonical_bytes, "Campaign statistics")


@dataclass(frozen=True, slots=True, init=False)
class CampaignComparison:
    canonical_bytes: bytes
    digest: str

    def __init__(self) -> None:
        raise TypeError("CampaignComparison instances must be created by compare_campaigns_v0")

    def to_dict(self) -> dict[str, object]:
        return _canonical_object(self.canonical_bytes, "Campaign comparison")


_TRIAL_SEALS: WeakKeyDictionary[CampaignTrial, bytes] = WeakKeyDictionary()
_AUTHORITY_LOCK = Lock()


def authenticated_campaign_plan(
    repository: PersistenceRepository,
    *,
    campaign_id: str,
    arm: CampaignArm,
    selected_fault_ids: Iterable[str],
    assignments: Mapping[int, str],
) -> CampaignPlan:
    """Freeze queued Run membership before outcomes can influence indexes."""
    _identifier(campaign_id, "campaign_id")
    if not isinstance(arm, str) or arm not in {"baseline", "faulted"}:
        raise CampaignValidationError(["arm must be baseline or faulted"])
    selected = _fault_ids(selected_fault_ids, "selected_fault_ids")
    if (arm == "baseline" and selected) or (arm == "faulted" and not selected):
        raise CampaignValidationError(
            ["baseline selects no faults and faulted selects at least one fault"]
        )
    try:
        items = tuple(assignments.items())
    except Exception as error:
        raise CampaignValidationError(["Campaign assignments are malformed"]) from error
    if not items:
        raise CampaignValidationError(["authoritative Campaign plans require at least one Run"])
    if any(
        type(index) is not int
        or index < 0
        or not isinstance(run_id, str)
        or _ID_RE.fullmatch(run_id) is None
        for index, run_id in items
    ):
        raise CampaignValidationError(["Campaign assignments are malformed"])
    ordered = tuple(sorted(items))
    if [item[0] for item in ordered] != list(range(len(ordered))):
        raise CampaignValidationError(["Campaign planned indexes must be contiguous from zero"])
    run_ids = [item[1] for item in ordered]
    if len(set(run_ids)) != len(run_ids):
        raise CampaignValidationError(["Campaign assignments contain duplicate Runs"])
    try:
        existing = repository.get_campaign_plan(campaign_id)
        if existing is not None:
            if (
                existing.arm != arm
                or existing.selected_fault_ids != selected
                or existing.assignments != ordered
            ):
                raise CampaignValidationError(
                    ["Campaign membership conflicts with durable authority"]
                )
            return _plan_from_record(existing)
        first = repository.get_run(run_ids[0])
        if first is None:
            raise CampaignValidationError(["Campaign plans require existing Runs"])
        scenario_record = repository.get_scenario_revision(
            first.scenario.id, first.scenario.revision
        )
        if scenario_record is None or scenario_record.scenario.digest != first.scenario.digest:
            raise CampaignValidationError(["Campaign Scenario binding does not resolve"])
        scenario_document = scenario_record.scenario.to_dict()
        available = tuple(
            sorted(
                cast(str, fault["id"])
                for fault in cast(list[dict[str, object]], scenario_document["faults"])
            )
        )
        if not set(selected) <= set(available):
            raise CampaignValidationError(["selected faults are absent from the Scenario catalog"])
        fault_plan = compile_fault_plan_v0(scenario_record.scenario, selected_fault_ids=selected)
        record = repository.create_campaign_plan(
            campaign_id=campaign_id,
            arm=arm,
            selected_fault_ids=selected,
            fault_plan_digest=fault_plan.digest,
            assignments=ordered,
        )
    except CampaignMembershipConflictError as error:
        raise CampaignValidationError(
            ["Campaign membership conflicts with durable authority"]
        ) from error
    except CampaignValidationError:
        raise
    except (PersistenceError, SQLAlchemyError, FaultRuleValidationError, ValueError) as error:
        raise CampaignValidationError(["Campaign plan provenance is unavailable"]) from error
    return _plan_from_record(record)


def _plan_from_record(record: CampaignPlanRecord) -> CampaignPlan:
    plan = object.__new__(CampaignPlan)
    object.__setattr__(plan, "campaign_id", record.campaign_id)
    object.__setattr__(plan, "arm", record.arm)
    object.__setattr__(
        plan,
        "scenario",
        MappingProxyType(
            {
                "id": record.scenario.id,
                "revision": record.scenario.revision,
                "digest": record.scenario.digest,
            }
        ),
    )
    object.__setattr__(
        plan,
        "agent_configuration",
        MappingProxyType(
            {
                "id": record.agent_configuration.id,
                "revision": record.agent_configuration.revision,
                "digest": record.agent_configuration.digest,
            }
        ),
    )
    object.__setattr__(plan, "selected_fault_ids", record.selected_fault_ids)
    object.__setattr__(plan, "assignments", record.assignments)
    object.__setattr__(plan, "fault_plan_digest", record.fault_plan_digest)
    object.__setattr__(plan, "canonical_digest", record.canonical_digest)
    return plan


def authenticated_campaign_trial(
    repository: PersistenceRepository,
    plan: CampaignPlan,
    run_id: str,
    *,
    ground_truths: tuple[GroundTruth, ...],
) -> CampaignTrial:
    """Mint one trial from its pre-execution plan and completed Issue #16 Run.

    Arbitrary caller-created ``EvaluationInput`` objects are never accepted here.
    """
    if isinstance(run_id, EvaluationInput) or not isinstance(plan, CampaignPlan):
        raise CampaignValidationError(
            ["caller-created EvaluationInput is not authoritative Campaign evidence"]
        )
    if not isinstance(run_id, str):
        raise CampaignValidationError(["run_id must identify an authoritative persisted Run"])
    try:
        record = repository.get_campaign_plan(plan.campaign_id)
        membership = repository.get_campaign_membership(run_id)
        if record is None or membership is None:
            raise CampaignValidationError(["Campaign plan has no committed durable authority"])
        authoritative = _plan_from_record(record)
        if _plan_projection(plan) != _plan_projection(authoritative):
            raise CampaignValidationError(["Campaign plan authority binding is corrupt"])
        if (
            membership.campaign_id != record.campaign_id
            or membership.campaign_plan_digest != record.canonical_digest
            or membership.scenario != record.scenario
            or membership.agent_configuration != record.agent_configuration
        ):
            raise CampaignValidationError(["Campaign membership authority binding is corrupt"])
        assignment = {member_run_id: index for index, member_run_id in record.assignments}
        if run_id not in assignment or assignment[run_id] != membership.trial_index:
            raise CampaignValidationError(["Run is absent from the frozen Campaign plan"])
        snapshot = load_authoritative_evaluation_snapshot(repository, run_id, ground_truths)
        reconstructed_fault_plan = compile_fault_plan_v0(
            snapshot.evaluation_input.scenario,
            selected_fault_ids=record.selected_fault_ids,
        )
        if reconstructed_fault_plan.digest != record.fault_plan_digest:
            raise CampaignValidationError(["Campaign fault plan identity is corrupt"])
        if snapshot.run.fault_plan_digest != record.fault_plan_digest:
            raise CampaignValidationError(
                ["Run execution fault plan differs from its frozen Campaign assignment"]
            )
    except CampaignValidationError:
        raise
    except (
        PersistenceError,
        SQLAlchemyError,
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        raise CampaignValidationError(
            ["authoritative Campaign evaluation provenance is unavailable"]
        ) from error
    trial = _mint_campaign_trial(
        snapshot.evaluation_input,
        snapshot.result,
        campaign_id=plan.campaign_id,
        arm=plan.arm,
        trial_index=assignment[run_id],
        planned_trials=record.planned_trials,
        selected_fault_ids=record.selected_fault_ids,
        agent_configuration=authoritative.agent_configuration,
    )
    if dict(trial.scenario) != dict(plan.scenario):
        raise CampaignValidationError(["Run Scenario differs from its frozen Campaign plan"])
    if dict(trial.agent_configuration) != dict(plan.agent_configuration):
        raise CampaignValidationError(
            ["Run Agent Configuration differs from its frozen Campaign plan"]
        )
    return trial


def _mint_campaign_trial(
    evaluation_input: EvaluationInput,
    result: EvaluationResult,
    *,
    campaign_id: str,
    arm: CampaignArm,
    trial_index: int,
    planned_trials: int,
    selected_fault_ids: Iterable[str],
    agent_configuration: Mapping[str, object],
) -> CampaignTrial:
    """Internal mint used after authority verification and by pure numeric tests."""
    _identifier(campaign_id, "campaign_id")
    if not isinstance(arm, str) or arm not in {"baseline", "faulted"}:
        raise CampaignValidationError(["arm must be baseline or faulted"])
    if type(trial_index) is not int or not 0 <= trial_index <= _SAFE_MAX:
        raise CampaignValidationError(["trial_index must be a nonnegative JSON-safe integer"])
    if type(planned_trials) is not int or not 0 <= planned_trials <= _SAFE_MAX:
        raise CampaignValidationError(["planned_trials must be a nonnegative JSON-safe integer"])
    if trial_index >= planned_trials:
        raise CampaignValidationError(["trial_index is outside the frozen trial plan"])
    selected = _fault_ids(selected_fault_ids, "selected_fault_ids")
    if (arm == "baseline" and selected) or (arm == "faulted" and not selected):
        raise CampaignValidationError(
            ["baseline selects no faults and faulted selects at least one fault"]
        )
    agent = _revision_reference(agent_configuration, "agent_configuration")
    reconstructed = evaluate_critical_gates(evaluation_input)
    if (
        reconstructed.canonical_bytes != result.canonical_bytes
        or reconstructed.digest != result.digest
    ):
        raise CampaignValidationError(["authoritative evaluation snapshot binding is inconsistent"])
    result_document = result.to_dict()
    if result_document["run_id"] != evaluation_input.run_id:
        raise CampaignValidationError(["evaluation result does not bind its Run"])
    if result_document["evaluator"] != EVALUATOR_REVISION:
        raise CampaignValidationError(["evaluation result uses an unsupported evaluator revision"])
    scenario_document = evaluation_input.scenario.to_dict()
    available_fault_ids = tuple(
        sorted(
            cast(str, fault["id"])
            for fault in cast(list[dict[str, object]], scenario_document["faults"])
        )
    )
    if not set(selected) <= set(available_fault_ids):
        raise CampaignValidationError(["selected faults are absent from the Scenario catalog"])
    scenario = _revision_reference(
        {
            "id": scenario_document["scenario_id"],
            "revision": scenario_document["revision"],
            "digest": evaluation_input.scenario.digest,
        },
        "scenario",
    )
    observed: set[str] = set()
    if result_document["classification"] != "invalid":
        gate_results = {
            cast(str, gate["gate_id"]): gate
            for gate in cast(list[dict[str, object]], result_document["critical_gates"])
        }
        for truth in evaluation_input.ground_truths:
            for gate in cast(list[dict[str, object]], truth.to_dict()["critical_gates"]):
                if gate["kind"] != "fault_observed":
                    continue
                result_gate = gate_results.get(cast(str, gate["gate_id"]))
                if result_gate is not None and result_gate["status"] == "pass":
                    observed.update(cast(list[str], gate["fault_ids"]))
    value = object.__new__(CampaignTrial)
    object.__setattr__(value, "campaign_id", campaign_id)
    object.__setattr__(value, "arm", arm)
    object.__setattr__(value, "run_id", evaluation_input.run_id)
    object.__setattr__(value, "trial_index", trial_index)
    object.__setattr__(value, "planned_trials", planned_trials)
    object.__setattr__(value, "scenario", MappingProxyType(scenario))
    object.__setattr__(value, "agent_configuration", MappingProxyType(agent))
    object.__setattr__(value, "selected_fault_ids", selected)
    object.__setattr__(value, "evaluation_input", evaluation_input)
    object.__setattr__(value, "evaluation", result)
    object.__setattr__(value, "evaluation_id", cast(str, result_document["evaluation_id"]))
    object.__setattr__(value, "evaluation_input_digest", cast(str, result_document["input_digest"]))
    object.__setattr__(
        value,
        "evidence_through_sequence",
        cast(int, result_document["evidence_through_sequence"]),
    )
    object.__setattr__(value, "available_fault_ids", available_fault_ids)
    object.__setattr__(value, "observed_fault_ids", tuple(sorted(observed)))
    with _AUTHORITY_LOCK:
        _TRIAL_SEALS[value] = _trial_seal(value)
    return value


def campaign_cohort_v0(
    *,
    campaign_id: str,
    arm: CampaignArm,
    scenario: Mapping[str, object],
    agent_configuration: Mapping[str, object],
    available_fault_ids: Iterable[str],
    selected_fault_ids: Iterable[str],
    planned_trials: int,
    trials: Iterable[CampaignTrial],
) -> CampaignCohort:
    """Validate and freeze one Campaign arm's deterministic membership."""
    _identifier(campaign_id, "campaign_id")
    if not isinstance(arm, str) or arm not in {"baseline", "faulted"}:
        raise CampaignValidationError(["arm must be baseline or faulted"])
    scenario_ref = _revision_reference(scenario, "scenario")
    agent_ref = _revision_reference(agent_configuration, "agent_configuration")
    available = _fault_ids(available_fault_ids, "available_fault_ids")
    faults = _fault_ids(selected_fault_ids, "selected_fault_ids")
    if type(planned_trials) is not int or not 0 <= planned_trials <= _SAFE_MAX:
        raise CampaignValidationError(["planned_trials must be a nonnegative JSON-safe integer"])
    if not set(faults) <= set(available):
        raise CampaignValidationError(["selected faults are absent from the Scenario catalog"])
    if (arm == "baseline" and faults) or (arm == "faulted" and not faults):
        raise CampaignValidationError(
            ["baseline selects no faults and faulted selects at least one fault"]
        )
    snapshot = cast(tuple[CampaignTrial, ...], _iterable_snapshot(trials, "trials"))
    errors: list[str] = []
    run_ids: set[str] = set()
    indexes: set[int] = set()
    for trial in snapshot:
        try:
            _validate_trial(trial)
        except CampaignValidationError as error:
            errors.extend(error.errors)
            continue
        if trial.run_id in run_ids:
            errors.append(f"duplicate run_id {trial.run_id!r}")
        if trial.trial_index in indexes:
            errors.append(f"duplicate trial_index {trial.trial_index}")
        if dict(trial.scenario) != scenario_ref:
            errors.append(f"run {trial.run_id!r} has an incompatible Scenario revision")
        if dict(trial.agent_configuration) != agent_ref:
            errors.append(f"run {trial.run_id!r} has an incompatible Agent Configuration")
        if trial.available_fault_ids != available:
            errors.append(f"run {trial.run_id!r} has a different Scenario fault catalog")
        if trial.campaign_id != campaign_id:
            errors.append(f"run {trial.run_id!r} belongs to a different Campaign")
        if trial.arm != arm:
            errors.append(f"run {trial.run_id!r} belongs to a different Campaign arm")
        if trial.selected_fault_ids != faults:
            errors.append(f"run {trial.run_id!r} has a different selected-fault assignment")
        if trial.trial_index >= planned_trials:
            errors.append(f"run {trial.run_id!r} is outside the frozen trial plan")
        if trial.planned_trials != planned_trials:
            errors.append(f"run {trial.run_id!r} has a different frozen trial plan")
        run_ids.add(trial.run_id)
        indexes.add(trial.trial_index)
    if errors:
        raise CampaignValidationError(errors)
    return CampaignCohort(
        campaign_id,
        arm,
        MappingProxyType(scenario_ref),
        MappingProxyType(agent_ref),
        available,
        faults,
        planned_trials,
        tuple(sorted(snapshot, key=lambda item: (item.trial_index, item.run_id))),
    )


def aggregate_campaign_v0(
    cohort: CampaignCohort, *, k_values: Iterable[int] = (1,)
) -> CampaignStatistics:
    """Compute deterministic counts, rates, Wilson intervals, and reliability."""
    cohort = _validate_cohort(cohort)
    ks = _k_values(k_values)
    document = _statistics_document(cohort, ks)
    data = _canonical_contract(document, "campaign-statistics-v0.schema.json")
    return cast(CampaignStatistics, _wrapper(CampaignStatistics, data))


def compare_campaigns_v0(
    baseline: CampaignCohort,
    faulted: CampaignCohort,
    *,
    k_values: Iterable[int] = (1,),
) -> CampaignComparison:
    """Compare controlled baseline/fault pairs by immutable trial index."""
    baseline = _validate_cohort(baseline)
    faulted = _validate_cohort(faulted)
    if baseline.arm != "baseline" or faulted.arm != "faulted":
        raise CampaignValidationError(["comparison requires baseline then faulted cohorts"])
    if baseline.campaign_id == faulted.campaign_id:
        raise CampaignValidationError(["baseline and faulted Campaign IDs must differ"])
    if dict(baseline.scenario) != dict(faulted.scenario):
        raise CampaignValidationError(["paired Campaigns use incompatible Scenario revisions"])
    if dict(baseline.agent_configuration) != dict(faulted.agent_configuration):
        raise CampaignValidationError(
            ["paired Campaigns use incompatible Agent Configuration revisions"]
        )
    if baseline.available_fault_ids != faulted.available_fault_ids:
        raise CampaignValidationError(["paired Campaigns use incompatible fault catalogs"])
    if baseline.planned_trials != faulted.planned_trials:
        raise CampaignValidationError(["paired Campaigns use incompatible trial plans"])
    overlap = {item.run_id for item in baseline.trials} & {item.run_id for item in faulted.trials}
    if overlap:
        raise CampaignValidationError([f"Run is substituted across Campaigns: {min(overlap)!r}"])
    ks = _k_values(k_values)
    baseline_document = _statistics_document(baseline, ks)
    faulted_document = _statistics_document(faulted, ks)
    paired = _paired_document(baseline, faulted)
    warnings = sorted(
        set(cast(list[str], baseline_document["warnings"]))
        | set(cast(list[str], faulted_document["warnings"]))
        | set(cast(list[str], paired["warnings"]))
    )
    comparison_material = {
        "baseline_digest": _digest(rfc8785.dumps(cast(JsonValue, baseline_document))),
        "faulted_digest": _digest(rfc8785.dumps(cast(JsonValue, faulted_document))),
        "paired": paired,
    }
    comparison_id = (
        "comparison-"
        + hashlib.sha256(rfc8785.dumps(cast(JsonValue, comparison_material))).hexdigest()[:32]
    )
    document: dict[str, object] = {
        "schema_version": CAMPAIGN_COMPARISON_V0_SCHEMA_VERSION,
        "comparison_id": comparison_id,
        "baseline": baseline_document,
        "faulted": faulted_document,
        "paired": paired,
        "warnings": warnings,
    }
    data = _canonical_contract(document, "campaign-comparison-v0.schema.json")
    return cast(CampaignComparison, _wrapper(CampaignComparison, data))


def loads_campaign_statistics_v0(data: str | bytes) -> CampaignStatistics:
    canonical = _canonical_contract(_parse_json(data), "campaign-statistics-v0.schema.json")
    return cast(CampaignStatistics, _wrapper(CampaignStatistics, canonical))


def loads_campaign_comparison_v0(data: str | bytes) -> CampaignComparison:
    canonical = _canonical_contract(_parse_json(data), "campaign-comparison-v0.schema.json")
    return cast(CampaignComparison, _wrapper(CampaignComparison, canonical))


def campaign_statistics_schema_v0() -> dict[str, object]:
    return deepcopy(_schema("campaign-statistics-v0.schema.json"))


def campaign_comparison_schema_v0() -> dict[str, object]:
    return deepcopy(_schema("campaign-comparison-v0.schema.json"))


def _statistics_document(cohort: CampaignCohort, ks: tuple[int, ...]) -> dict[str, object]:
    indexed_classifications = [(item.trial_index, _classification(item)) for item in cohort.trials]
    classifications = [item[1] for item in indexed_classifications]
    counts = _counts(classifications)
    warnings = _count_warnings(counts)
    if len(cohort.trials) != cohort.planned_trials:
        warnings.add("missing_planned_trials")
    reliability = [
        _reliability(indexed_classifications, cohort.planned_trials, k, warnings) for k in ks
    ]
    observed_trials = tuple(
        item
        for item in cohort.trials
        if cohort.selected_fault_ids
        and set(cohort.selected_fault_ids) <= set(item.observed_fault_ids)
    )
    if cohort.selected_fault_ids:
        observed_counts = _counts([_classification(item) for item in observed_trials])
        observed: dict[str, object] = {
            "status": "available" if observed_trials else "empty",
            "required_fault_ids": list(cohort.selected_fault_ids),
            "unobserved_runs": len(cohort.trials) - len(observed_trials),
            "counts": observed_counts,
            "pass_rate": _binomial_rate(
                observed_counts["pass"], observed_counts["valid_evaluated"]
            ),
            "reliability": [
                _reliability(
                    [(item.trial_index, _classification(item)) for item in observed_trials],
                    cohort.planned_trials,
                    k,
                    warnings,
                )
                for k in ks
            ],
        }
        if not observed_trials:
            warnings.add("observed_fault_condition_empty")
        if len(observed_trials) != len(cohort.trials):
            warnings.add("unobserved_fault_trials")
    else:
        observed = {"status": "not_applicable", "required_fault_ids": []}
    material = {
        "campaign_id": cohort.campaign_id,
        "arm": cohort.arm,
        "scenario": dict(cohort.scenario),
        "agent_configuration": dict(cohort.agent_configuration),
        "available_fault_ids": list(cohort.available_fault_ids),
        "selected_fault_ids": list(cohort.selected_fault_ids),
        "planned_trials": cohort.planned_trials,
        "runs": [
            {
                "run_id": item.run_id,
                "trial_index": item.trial_index,
                "evaluation_digest": item.evaluation.digest,
                "evaluation_id": item.evaluation_id,
                "evaluation_input_digest": item.evaluation_input_digest,
                "evidence_through_sequence": item.evidence_through_sequence,
                "classification": _classification(item),
                "observed_fault_ids": list(item.observed_fault_ids),
            }
            for item in cohort.trials
        ],
    }
    statistics_id = (
        "statistics-" + hashlib.sha256(rfc8785.dumps(cast(JsonValue, material))).hexdigest()[:32]
    )
    return {
        "schema_version": CAMPAIGN_STATISTICS_V0_SCHEMA_VERSION,
        "statistics_id": statistics_id,
        **material,
        "counts": counts,
        "pass_rate": _binomial_rate(counts["pass"], counts["valid_evaluated"]),
        "reliability": reliability,
        "observed_fault_condition": observed,
        "warnings": sorted(warnings),
    }


def _counts(classifications: Sequence[str]) -> dict[str, int]:
    passed = classifications.count("pass")
    failed = classifications.count("fail")
    invalid = classifications.count("invalid")
    return {
        "total_runs": len(classifications),
        "valid_evaluated": passed + failed,
        "pass": passed,
        "fail": failed,
        "invalid": invalid,
    }


def _count_warnings(counts: Mapping[str, int]) -> set[str]:
    warnings: set[str] = set()
    if counts["valid_evaluated"] == 0:
        warnings.add("no_valid_evaluations")
    elif counts["valid_evaluated"] < _SMALL_SAMPLE_THRESHOLD:
        warnings.add("small_sample")
    if counts["invalid"]:
        warnings.add("invalid_evaluations_present")
    return warnings


def _binomial_rate(successes: int, n: int) -> dict[str, object]:
    if n == 0:
        return {"status": "unavailable", "n": 0, "successes": 0, "reason": "zero_denominator"}
    estimate = Fraction(successes, n)
    lower, upper = _wilson(successes, n)
    return {
        "status": "available",
        "n": n,
        "successes": successes,
        "estimate": _fraction_decimal(estimate),
        "wilson_95": {"lower": lower, "upper": upper},
    }


def _wilson(successes: int, n: int) -> tuple[str, str]:
    if n <= 0 or not 0 <= successes <= n:
        raise CampaignValidationError(["Wilson interval requires 0 <= successes <= n and n > 0"])
    with localcontext() as context:
        context.prec = 60
        n_value = Decimal(n)
        p = Decimal(successes) / n_value
        z2 = _WILSON_Z_95 * _WILSON_Z_95
        denominator = Decimal(1) + z2 / n_value
        center = (p + z2 / (Decimal(2) * n_value)) / denominator
        spread = (
            _WILSON_Z_95
            * (p * (Decimal(1) - p) / n_value + z2 / (Decimal(4) * n_value * n_value)).sqrt()
            / denominator
        )
        lower = max(Decimal(0), center - spread)
        upper = min(Decimal(1), center + spread)
        return _decimal_string(lower), _decimal_string(upper)


def _reliability(
    indexed_classifications: Sequence[tuple[int, str]],
    planned_trials: int,
    k: int,
    warnings: set[str],
) -> dict[str, object]:
    classifications = [item[1] for item in indexed_classifications]
    valid = [item for item in classifications if item != "invalid"]
    pass_count = valid.count("pass")
    n = len(valid)
    if k > n:
        pass_at: dict[str, object] = {
            "status": "insufficient_samples",
            "n": n,
            "successes": pass_count,
            "reason": "k_exceeds_valid_sample",
        }
        warnings.add("insufficient_pass_at_k_samples")
    else:
        denominator = math.comb(n, k)
        numerator = denominator - math.comb(n - pass_count, k)
        pass_at = {
            "status": "available",
            "n": n,
            "successes": pass_count,
            "estimate": _fraction_decimal(Fraction(numerator, denominator)),
        }
    complete_groups = planned_trials // k
    discarded = planned_trials % k
    by_index = dict(indexed_classifications)
    groups = [
        [by_index.get(index) for index in range(group * k, (group + 1) * k)]
        for group in range(complete_groups)
    ]
    present_groups = [group for group in groups if all(item is not None for item in group)]
    valid_groups = [group for group in present_groups if all(item != "invalid" for item in group)]
    successful_groups = sum(all(item == "pass" for item in group) for group in valid_groups)
    missing_groups = complete_groups - len(present_groups)
    invalid_groups = len(present_groups) - len(valid_groups)
    if not valid_groups:
        pass_power: dict[str, object] = {
            "status": "unavailable",
            "method": "predetermined_groups",
            "complete_groups": complete_groups,
            "valid_groups": 0,
            "successful_groups": 0,
            "invalid_groups": invalid_groups,
            "missing_groups": missing_groups,
            "discarded_runs": discarded,
            "reason": "zero_valid_groups",
        }
    else:
        lower, upper = _wilson(successful_groups, len(valid_groups))
        pass_power = {
            "status": "available",
            "method": "predetermined_groups",
            "complete_groups": complete_groups,
            "valid_groups": len(valid_groups),
            "successful_groups": successful_groups,
            "invalid_groups": invalid_groups,
            "missing_groups": missing_groups,
            "discarded_runs": discarded,
            "estimate": _fraction_decimal(Fraction(successful_groups, len(valid_groups))),
            "wilson_95": {"lower": lower, "upper": upper},
        }
    if discarded:
        warnings.add("incomplete_pass_power_group")
    if invalid_groups:
        warnings.add("invalid_pass_power_groups")
    if missing_groups:
        warnings.add("missing_pass_power_groups")
    return {"k": k, "pass_at_k": pass_at, "pass_power_k": pass_power}


def _paired_document(baseline: CampaignCohort, faulted: CampaignCohort) -> dict[str, object]:
    baseline_rows = [
        {
            "trial_index": item.trial_index,
            "classification": _classification(item),
            "observed_fault_ids": list(item.observed_fault_ids),
        }
        for item in baseline.trials
    ]
    faulted_rows = [
        {
            "trial_index": item.trial_index,
            "classification": _classification(item),
            "observed_fault_ids": list(item.observed_fault_ids),
        }
        for item in faulted.trials
    ]
    return _paired_rows(baseline_rows, faulted_rows, faulted.selected_fault_ids)


def _paired_rows(
    baseline_rows: Sequence[Mapping[str, object]],
    faulted_rows: Sequence[Mapping[str, object]],
    selected_fault_ids: Sequence[str],
) -> dict[str, object]:
    baseline_by_index = {cast(int, item["trial_index"]): item for item in baseline_rows}
    faulted_by_index = {cast(int, item["trial_index"]): item for item in faulted_rows}
    common = sorted(baseline_by_index.keys() & faulted_by_index.keys())
    missing_baseline = sorted(faulted_by_index.keys() - baseline_by_index.keys())
    missing_faulted = sorted(baseline_by_index.keys() - faulted_by_index.keys())
    valid_pairs: list[tuple[Mapping[str, object], Mapping[str, object]]] = []
    invalid_pairs = 0
    for index in common:
        pair = (baseline_by_index[index], faulted_by_index[index])
        if any(item["classification"] == "invalid" for item in pair):
            invalid_pairs += 1
        else:
            valid_pairs.append(pair)
    improved = sum(
        left["classification"] == "fail" and right["classification"] == "pass"
        for left, right in valid_pairs
    )
    regressed = sum(
        left["classification"] == "pass" and right["classification"] == "fail"
        for left, right in valid_pairs
    )
    unchanged = len(valid_pairs) - improved - regressed
    delta = _signed_rate(improved - regressed, len(valid_pairs))
    observed_pairs = [
        pair
        for pair in valid_pairs
        if set(selected_fault_ids) <= set(cast(Sequence[str], pair[1]["observed_fault_ids"]))
    ]
    observed_improved = sum(
        left["classification"] == "fail" and right["classification"] == "pass"
        for left, right in observed_pairs
    )
    observed_regressed = sum(
        left["classification"] == "pass" and right["classification"] == "fail"
        for left, right in observed_pairs
    )
    warnings: set[str] = set()
    if missing_baseline or missing_faulted:
        warnings.add("missing_pairs")
    if invalid_pairs:
        warnings.add("invalid_pairs_excluded")
    if len(valid_pairs) < _SMALL_SAMPLE_THRESHOLD:
        warnings.add("small_paired_sample")
    if not observed_pairs:
        warnings.add("observed_fault_pair_set_empty")
    return {
        "common_pairs": len(common),
        "valid_pairs": len(valid_pairs),
        "invalid_pairs": invalid_pairs,
        "improved": improved,
        "regressed": regressed,
        "unchanged": unchanged,
        "missing_baseline_trial_indexes": missing_baseline,
        "missing_faulted_trial_indexes": missing_faulted,
        "fault_minus_baseline_pass_delta": delta,
        "observed_fault_pairs": {
            "n": len(observed_pairs),
            "improved": observed_improved,
            "regressed": observed_regressed,
            "pass_delta": _signed_rate(observed_improved - observed_regressed, len(observed_pairs)),
        },
        "warnings": sorted(warnings),
    }


def _signed_rate(numerator: int, denominator: int) -> dict[str, object]:
    if denominator == 0:
        return {"status": "unavailable", "reason": "zero_denominator"}
    return {
        "status": "available",
        "numerator": numerator,
        "denominator": denominator,
        "estimate": _fraction_decimal(Fraction(numerator, denominator)),
    }


def _validate_trial(trial: CampaignTrial) -> None:
    try:
        _validate_trial_fields(trial)
    except CampaignValidationError:
        raise
    except Exception as error:
        raise CampaignValidationError(["Campaign member is malformed"]) from error


def _validate_trial_fields(trial: CampaignTrial) -> None:
    if not isinstance(trial, CampaignTrial):
        raise CampaignValidationError(["Campaign member is not an authenticated trial"])
    _identifier(trial.run_id, "run_id")
    if type(trial.trial_index) is not int or not 0 <= trial.trial_index <= _SAFE_MAX:
        raise CampaignValidationError(["trial_index is invalid"])
    scenario = _revision_reference(trial.scenario, "scenario")
    agent = _revision_reference(trial.agent_configuration, "agent_configuration")
    try:
        loaded = loads_evaluation_result(trial.evaluation.canonical_bytes)
        reconstructed = evaluate_critical_gates(trial.evaluation_input)
    except (AttributeError, EvaluatorValidationError, TypeError, ValueError) as error:
        raise CampaignValidationError(["trial Evaluation Result is malformed"]) from error
    with _AUTHORITY_LOCK:
        authority_matches = _TRIAL_SEALS.get(trial) == _trial_seal(trial)
    if (
        not authority_matches
        or trial.campaign_id == ""
        or trial.arm not in {"baseline", "faulted"}
        or loaded.digest != trial.evaluation.digest
        or loaded.to_dict()["run_id"] != trial.run_id
        or loaded.to_dict()["evaluator"] != EVALUATOR_REVISION
        or loaded.to_dict()["evaluation_id"] != trial.evaluation_id
        or loaded.to_dict()["input_digest"] != trial.evaluation_input_digest
        or loaded.to_dict()["evidence_through_sequence"] != trial.evidence_through_sequence
        or reconstructed.canonical_bytes != loaded.canonical_bytes
        or reconstructed.digest != loaded.digest
    ):
        raise CampaignValidationError(["trial authority or Evaluation Result binding is corrupt"])
    _identifier(trial.campaign_id, "campaign_id")
    selected = _fault_ids(trial.selected_fault_ids, "selected_fault_ids")
    if (
        selected != trial.selected_fault_ids
        or (trial.arm == "baseline" and selected)
        or (trial.arm == "faulted" and not selected)
    ):
        raise CampaignValidationError(["trial Campaign membership is malformed"])
    if tuple(sorted(trial.observed_fault_ids)) != trial.observed_fault_ids or any(
        _CATALOG_ID_RE.fullmatch(item) is None for item in trial.observed_fault_ids
    ):
        raise CampaignValidationError(["observed fault identities are malformed"])
    if tuple(sorted(trial.available_fault_ids)) != trial.available_fault_ids or any(
        _CATALOG_ID_RE.fullmatch(item) is None for item in trial.available_fault_ids
    ):
        raise CampaignValidationError(["available fault identities are malformed"])
    if not set(trial.observed_fault_ids) <= set(trial.available_fault_ids):
        raise CampaignValidationError(["observed faults are not defined by the Scenario"])
    if not scenario or not agent:
        raise CampaignValidationError(["trial revision references are unavailable"])


def _trial_seal(trial: CampaignTrial) -> bytes:
    try:
        document: dict[str, JsonValue] = {
            "campaign_id": trial.campaign_id,
            "arm": trial.arm,
            "run_id": trial.run_id,
            "trial_index": trial.trial_index,
            "planned_trials": trial.planned_trials,
            "scenario": cast(dict[str, JsonValue], dict(trial.scenario)),
            "agent_configuration": cast(dict[str, JsonValue], dict(trial.agent_configuration)),
            "selected_fault_ids": list(trial.selected_fault_ids),
            "evaluation_digest": trial.evaluation.digest,
            "evaluation_id": trial.evaluation_id,
            "evaluation_input_digest": trial.evaluation_input_digest,
            "evidence_through_sequence": trial.evidence_through_sequence,
            "available_fault_ids": list(trial.available_fault_ids),
            "observed_fault_ids": list(trial.observed_fault_ids),
        }
        return hashlib.sha256(rfc8785.dumps(document)).digest()
    except (AttributeError, TypeError, rfc8785.CanonicalizationError) as error:
        raise CampaignValidationError(["trial authority binding is malformed"]) from error


def _plan_projection(plan: CampaignPlan) -> dict[str, JsonValue]:
    try:
        return {
            "campaign_id": plan.campaign_id,
            "arm": plan.arm,
            "scenario": cast(dict[str, JsonValue], dict(plan.scenario)),
            "agent_configuration": cast(dict[str, JsonValue], dict(plan.agent_configuration)),
            "selected_fault_ids": list(plan.selected_fault_ids),
            "assignments": [
                {"trial_index": index, "run_id": run_id} for index, run_id in plan.assignments
            ],
            "fault_plan_digest": plan.fault_plan_digest,
            "canonical_digest": plan.canonical_digest,
        }
    except (AttributeError, TypeError, ValueError) as error:
        raise CampaignValidationError(["Campaign plan authority binding is malformed"]) from error


def _validate_cohort(cohort: CampaignCohort) -> CampaignCohort:
    if not isinstance(cohort, CampaignCohort):
        raise CampaignValidationError(["Campaign cohort is malformed"])
    try:
        return campaign_cohort_v0(
            campaign_id=cohort.campaign_id,
            arm=cohort.arm,
            scenario=cohort.scenario,
            agent_configuration=cohort.agent_configuration,
            available_fault_ids=cohort.available_fault_ids,
            selected_fault_ids=cohort.selected_fault_ids,
            planned_trials=cohort.planned_trials,
            trials=cohort.trials,
        )
    except CampaignValidationError:
        raise
    except Exception as error:
        raise CampaignValidationError(["Campaign cohort is malformed"]) from error


def _classification(trial: CampaignTrial) -> str:
    return cast(str, trial.evaluation.to_dict()["classification"])


def _k_values(values: Iterable[int]) -> tuple[int, ...]:
    snapshot = cast(tuple[int, ...], _iterable_snapshot(values, "k_values"))
    if not snapshot or any(type(item) is not int or not 1 <= item <= 1000 for item in snapshot):
        raise CampaignValidationError(["k_values must be unique integers between 1 and 1000"])
    if len(set(snapshot)) != len(snapshot):
        raise CampaignValidationError(["k_values must be unique integers between 1 and 1000"])
    return tuple(sorted(snapshot))


def _fault_ids(values: Iterable[str], field: str) -> tuple[str, ...]:
    snapshot = cast(tuple[str, ...], _iterable_snapshot(values, field))
    if any(
        not isinstance(item, str) or _CATALOG_ID_RE.fullmatch(item) is None for item in snapshot
    ):
        raise CampaignValidationError([f"{field} are malformed or duplicated"])
    if len(set(snapshot)) != len(snapshot):
        raise CampaignValidationError([f"{field} are malformed or duplicated"])
    return tuple(sorted(snapshot))


def _iterable_snapshot(values: object, field: str) -> tuple[object, ...]:
    if isinstance(values, str | bytes | bytearray | Mapping) or not isinstance(values, Iterable):
        raise CampaignValidationError([f"{field} must be an iterable of items"])
    try:
        return tuple(values)
    except Exception as error:
        raise CampaignValidationError([f"{field} could not be snapshotted"]) from error


def _fraction_decimal(value: Fraction) -> str:
    with localcontext() as context:
        context.prec = 60
        return _decimal_string(Decimal(value.numerator) / Decimal(value.denominator))


def _decimal_string(value: Decimal) -> str:
    return format(value.quantize(_DECIMAL_QUANTUM, rounding=ROUND_HALF_EVEN), "f")


def _identifier(value: object, field: str) -> None:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise CampaignValidationError([f"{field} is not a valid identifier"])


def _revision_reference(value: Mapping[str, object], field: str) -> dict[str, object]:
    try:
        snapshot = deepcopy(dict(value))
        valid = set(snapshot) == {"id", "revision", "digest"} and (
            isinstance(snapshot["id"], str)
            and _CATALOG_ID_RE.fullmatch(snapshot["id"]) is not None
            and isinstance(snapshot["revision"], str)
            and _REVISION_RE.fullmatch(snapshot["revision"]) is not None
            and isinstance(snapshot["digest"], str)
            and _DIGEST_RE.fullmatch(snapshot["digest"]) is not None
        )
    except Exception as error:
        raise CampaignValidationError([f"{field} could not be snapshotted"]) from error
    if not valid:
        raise CampaignValidationError([f"{field} is not a valid revision reference"])
    return snapshot


@lru_cache(maxsize=2)
def _schema(filename: str) -> dict[str, object]:
    value = json.loads(files("chaosagent_evaluators.schema").joinpath(filename).read_text("utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"bundled Campaign schema {filename!r} is not an object")
    result = cast(dict[str, object], value)
    Draft202012Validator.check_schema(result)
    return result


@lru_cache(maxsize=2)
def _validator(filename: str) -> Draft202012Validator:
    statistics = _schema("campaign-statistics-v0.schema.json")
    registry = Registry().with_resource(
        cast(str, statistics["$id"]),
        Resource.from_contents(statistics),
    )
    return Draft202012Validator(_schema(filename), registry=registry)


def _canonical_contract(document: object, filename: str) -> bytes:
    try:
        snapshot = deepcopy(document)
    except Exception as error:
        raise CampaignValidationError(["contract could not be snapshotted"]) from error
    errors = sorted(
        _validator(filename).iter_errors(snapshot),
        key=lambda item: tuple(str(part) for part in item.absolute_path),
    )
    if errors:
        raise CampaignValidationError([error.message for error in errors])
    if not isinstance(snapshot, dict):
        raise CampaignValidationError(["Campaign aggregate must be an object"])
    semantic_errors = (
        _statistics_semantic_errors(cast(dict[str, object], snapshot))
        if filename == "campaign-statistics-v0.schema.json"
        else _comparison_semantic_errors(cast(dict[str, object], snapshot))
    )
    if semantic_errors:
        raise CampaignValidationError(semantic_errors)
    try:
        return rfc8785.dumps(cast(JsonValue, snapshot))
    except (rfc8785.CanonicalizationError, TypeError) as error:
        raise CampaignValidationError(["contract is not RFC 8785 representable"]) from error


def _statistics_semantic_errors(document: dict[str, object]) -> list[str]:
    errors: list[str] = []
    runs = cast(list[dict[str, object]], document["runs"])
    run_ids = [cast(str, item["run_id"]) for item in runs]
    indexes = [cast(int, item["trial_index"]) for item in runs]
    if len(set(run_ids)) != len(run_ids):
        errors.append("runs contain duplicate Run identities")
    if len(set(indexes)) != len(indexes):
        errors.append("runs contain duplicate trial indexes")
    if runs != sorted(
        runs,
        key=lambda item: (cast(int, item["trial_index"]), cast(str, item["run_id"])),
    ):
        errors.append("runs are not in deterministic trial order")
    selected = cast(list[str], document["selected_fault_ids"])
    available = cast(list[str], document["available_fault_ids"])
    planned_trials = cast(int, document["planned_trials"])
    if available != sorted(available) or not set(selected) <= set(available):
        errors.append("selected faults are not a subset of the Scenario fault catalog")
    if selected != sorted(selected):
        errors.append("selected fault IDs are not in canonical order")
    if (document["arm"] == "baseline" and selected) or (
        document["arm"] == "faulted" and not selected
    ):
        errors.append("Campaign arm and selected faults contradict")
    classifications = [cast(str, item["classification"]) for item in runs]
    indexed_classifications = [
        (cast(int, item["trial_index"]), cast(str, item["classification"])) for item in runs
    ]
    if any(not set(cast(list[str], item["observed_fault_ids"])) <= set(available) for item in runs):
        errors.append("observed fault IDs are absent from the Scenario fault catalog")
    if any(
        cast(list[str], item["observed_fault_ids"])
        != sorted(cast(list[str], item["observed_fault_ids"]))
        for item in runs
    ):
        errors.append("observed fault IDs are not in canonical order")
    if any(index >= planned_trials for index in indexes):
        errors.append("Run trial indexes are outside the frozen trial plan")
    counts = _counts(classifications)
    if document["counts"] != counts:
        errors.append("Campaign counts contradict Run classifications")
    if document["pass_rate"] != _binomial_rate(counts["pass"], counts["valid_evaluated"]):
        errors.append("Campaign pass rate contradicts counts")
    reliability = cast(list[dict[str, object]], document["reliability"])
    ks = [cast(int, item["k"]) for item in reliability]
    if ks != sorted(set(ks)):
        errors.append("reliability k values are duplicated or unordered")
    warnings = _count_warnings(counts)
    if len(runs) != planned_trials:
        warnings.add("missing_planned_trials")
    expected_reliability = [
        _reliability(indexed_classifications, planned_trials, k, warnings) for k in ks
    ]
    if reliability != expected_reliability:
        errors.append("Campaign reliability statistics contradict Run classifications")
    observed = cast(dict[str, object], document["observed_fault_condition"])
    if selected:
        observed_rows = [
            item
            for item in runs
            if set(selected) <= set(cast(list[str], item["observed_fault_ids"]))
        ]
        observed_classifications = [cast(str, item["classification"]) for item in observed_rows]
        observed_counts = _counts(observed_classifications)
        expected_observed = {
            "status": "available" if observed_rows else "empty",
            "required_fault_ids": selected,
            "unobserved_runs": len(runs) - len(observed_rows),
            "counts": observed_counts,
            "pass_rate": _binomial_rate(
                observed_counts["pass"], observed_counts["valid_evaluated"]
            ),
            "reliability": [
                _reliability(
                    [
                        (
                            cast(int, item["trial_index"]),
                            cast(str, item["classification"]),
                        )
                        for item in observed_rows
                    ],
                    planned_trials,
                    k,
                    warnings,
                )
                for k in ks
            ],
        }
        if not observed_rows:
            warnings.add("observed_fault_condition_empty")
        if len(observed_rows) != len(runs):
            warnings.add("unobserved_fault_trials")
    else:
        expected_observed = {"status": "not_applicable", "required_fault_ids": []}
    if observed != expected_observed:
        errors.append("observed-fault conditioning contradicts authenticated Run rows")
    if document["warnings"] != sorted(warnings):
        errors.append("Campaign warnings contradict the deterministic warning rules")
    material = {
        "campaign_id": document["campaign_id"],
        "arm": document["arm"],
        "scenario": document["scenario"],
        "agent_configuration": document["agent_configuration"],
        "available_fault_ids": available,
        "selected_fault_ids": selected,
        "planned_trials": planned_trials,
        "runs": runs,
    }
    expected_id = (
        "statistics-" + hashlib.sha256(rfc8785.dumps(cast(JsonValue, material))).hexdigest()[:32]
    )
    if document["statistics_id"] != expected_id:
        errors.append("statistics_id does not match canonical Campaign membership")
    return errors


def _comparison_semantic_errors(document: dict[str, object]) -> list[str]:
    errors: list[str] = []
    baseline = document["baseline"]
    faulted = document["faulted"]
    if not isinstance(baseline, dict) or not isinstance(faulted, dict):
        return ["comparison Campaign summaries must be objects"]
    for label, summary in (("baseline", baseline), ("faulted", faulted)):
        schema_errors = sorted(
            _validator("campaign-statistics-v0.schema.json").iter_errors(summary),
            key=lambda item: tuple(str(part) for part in item.absolute_path),
        )
        if schema_errors:
            errors.append(f"{label} Campaign summary violates Campaign Statistics v0")
        else:
            errors.extend(f"{label}: {item}" for item in _statistics_semantic_errors(summary))
    if errors:
        return errors
    if baseline["arm"] != "baseline" or faulted["arm"] != "faulted":
        errors.append("comparison Campaign arms are reversed or malformed")
    if baseline["campaign_id"] == faulted["campaign_id"]:
        errors.append("baseline and faulted Campaign IDs must differ")
    if baseline["scenario"] != faulted["scenario"]:
        errors.append("comparison Scenario revisions differ")
    if baseline["agent_configuration"] != faulted["agent_configuration"]:
        errors.append("comparison Agent Configuration revisions differ")
    if baseline["available_fault_ids"] != faulted["available_fault_ids"]:
        errors.append("comparison fault catalogs differ")
    if baseline["planned_trials"] != faulted["planned_trials"]:
        errors.append("comparison trial plans differ")
    baseline_runs = cast(list[dict[str, object]], baseline["runs"])
    faulted_runs = cast(list[dict[str, object]], faulted["runs"])
    if {item["run_id"] for item in baseline_runs} & {item["run_id"] for item in faulted_runs}:
        errors.append("comparison substitutes a Run across Campaigns")
    expected_paired = _paired_rows(
        baseline_runs,
        faulted_runs,
        cast(list[str], faulted["selected_fault_ids"]),
    )
    if document["paired"] != expected_paired:
        errors.append("paired comparison contradicts Campaign Run outcomes")
    material = {
        "baseline_digest": _digest(rfc8785.dumps(cast(JsonValue, baseline))),
        "faulted_digest": _digest(rfc8785.dumps(cast(JsonValue, faulted))),
        "paired": document["paired"],
    }
    expected_id = (
        "comparison-" + hashlib.sha256(rfc8785.dumps(cast(JsonValue, material))).hexdigest()[:32]
    )
    if document["comparison_id"] != expected_id:
        errors.append("comparison_id does not match canonical summaries")
    expected_warnings = sorted(
        set(cast(list[str], baseline["warnings"]))
        | set(cast(list[str], faulted["warnings"]))
        | set(cast(list[str], cast(dict[str, object], document["paired"])["warnings"]))
    )
    if document["warnings"] != expected_warnings:
        errors.append("comparison warnings contradict component warnings")
    return errors


def _wrapper(
    cls: type[CampaignStatistics] | type[CampaignComparison], data: bytes
) -> CampaignStatistics | CampaignComparison:
    value = object.__new__(cls)
    object.__setattr__(value, "canonical_bytes", data)
    object.__setattr__(value, "digest", _digest(data))
    return value


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _canonical_object(data: bytes, contract: str) -> dict[str, object]:
    value = json.loads(data)
    if not isinstance(value, dict):
        raise AssertionError(f"canonical {contract} is not an object")
    return cast(dict[str, object], value)


def _duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CampaignValidationError([f"duplicate JSON object key {key!r}"])
        result[key] = value
    return result


def _parse_json(data: str | bytes) -> object:
    if not isinstance(data, str | bytes):
        raise CampaignValidationError(["Campaign JSON must be text or bytes"])
    try:
        return cast(
            object,
            json.loads(
                data,
                object_pairs_hook=_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            ),
        )
    except (
        json.JSONDecodeError,
        RecursionError,
        UnicodeDecodeError,
        TypeError,
        ValueError,
    ) as error:
        raise CampaignValidationError(["malformed Campaign JSON"]) from error
