from __future__ import annotations

import copy
import hashlib
from collections.abc import ItemsView, Iterator, Mapping
from pathlib import Path
from typing import Any, cast

import pytest
import rfc8785
from chaosagent_faults import (
    CompiledFaultPlan,
    CompiledFaultRule,
    FaultMatchContext,
    FaultRuleValidationError,
    compile_fault_plan_v0,
    match_fault_plan_v0,
)
from chaosagent_scenarios import (
    Scenario,
    load_scenario,
    loads_scenario,
)

ROOT = Path(__file__).resolve().parents[2]
AMBIGUOUS_SCENARIO = ROOT / "benchmarks/shipment-refund/scenarios/refund-ambiguous-timeout.v0.json"
READ_FAULT_SCENARIO = ROOT / "benchmarks/shipment-refund/scenarios/shipping-transient-error.v0.json"
KIND_PARAMETERS: dict[str, dict[str, object]] = {
    "delay": {"duration_ms": 1},
    "timeout": {"duration_ms": 1},
    "http_error": {"status": 503},
    "malformed_response": {"mode": "invalid_json"},
    "stale_field": {"json_pointer": "/status", "value": "pending"},
    "ambiguous_post_commit_timeout": {"duration_ms": 1},
    "auth_error": {"status": 401},
    "indirect_prompt_injection": {"content": "synthetic text"},
    "duplicate_response": {"deliveries": 2},
}
ALLOWED_PHASES: dict[str, set[str]] = {
    "delay": {"before_tool", "after_tool"},
    "timeout": {"before_tool", "after_tool"},
    "http_error": {"before_tool"},
    "malformed_response": {"after_tool"},
    "stale_field": {"after_tool"},
    "ambiguous_post_commit_timeout": {"after_commit"},
    "auth_error": {"before_tool"},
    "indirect_prompt_injection": {"after_tool"},
    "duplicate_response": {"after_tool"},
}
PHASES = ("before_tool", "after_commit", "after_tool")


class StatefulMapping(Mapping[str, object]):
    """Mapping whose items change after the first materialization."""

    def __init__(self, first: dict[str, object], later: dict[str, object]) -> None:
        self.first = first
        self.later = later
        self.items_calls = 0

    def __getitem__(self, key: str) -> object:
        return self.later[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.later)

    def __len__(self) -> int:
        return len(self.later)

    def items(self) -> ItemsView[str, object]:
        self.items_calls += 1
        source = self.first if self.items_calls == 1 else self.later
        return source.items()


def _digest(value: object) -> str:
    canonical = rfc8785.dumps(value)  # type: ignore[arg-type]
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _loads_document(document: dict[str, object]) -> Scenario:
    return loads_scenario(rfc8785.dumps(cast(Any, document)))


def _context(
    plan: CompiledFaultPlan,
    *,
    tool_id: str = "payments.refund",
    phase: str = "after_commit",
    call_ordinal: int = 1,
    attempt_number: int = 1,
    arguments: Mapping[str, object] | None = None,
    arguments_digest: str | None = None,
    prior: Mapping[str, int] | None = None,
    run_seed: int = 42,
) -> FaultMatchContext:
    actual_arguments: Mapping[str, object] = (
        {"order_id": "ORD-1007", "amount_minor": 4200} if arguments is None else arguments
    )
    return FaultMatchContext(
        run_id="run-fault-001",
        run_seed=run_seed,
        scenario_digest=plan.scenario_digest,
        tool_id=tool_id,
        phase=cast(object, phase),  # type: ignore[arg-type]
        logical_call_id="call-refund-001",
        physical_attempt_id="attempt-refund-001",
        attempt_number=attempt_number,
        call_ordinal=call_ordinal,
        arguments=actual_arguments,
        arguments_digest=(
            _digest(dict(actual_arguments)) if arguments_digest is None else arguments_digest
        ),
        prior_applied_occurrences={} if prior is None else prior,
    )


def _plan(path: Path = AMBIGUOUS_SCENARIO) -> CompiledFaultPlan:
    return compile_fault_plan_v0(load_scenario(path))


def test_flagship_rule_compiles_and_matches_exactly() -> None:
    plan = _plan()

    assert [rule.fault_id for rule in plan.rules] == ["refund-ack-lost"]
    rule = plan.rules[0]
    assert rule.kind == "ambiguous_post_commit_timeout"
    assert rule.phase == "after_commit"
    assert rule.parameters == {"duration_ms": 5000}

    decision = match_fault_plan_v0(plan, _context(plan))[0]
    assert decision.matched is True
    assert decision.reason == "matched"
    assert decision.selection_bucket_ppm == 42605
    assert decision.activation_id == (
        "activation-8201f08533a01b98ceff37b6067741dd979357532e35f14b555de799a6d44f3a"
    )


def test_read_fault_golden_rule_compiles_and_matches() -> None:
    plan = _plan(READ_FAULT_SCENARIO)
    arguments = {"order_id": "ORD-1007"}
    context = _context(
        plan,
        tool_id="shipping.get_status",
        phase="before_tool",
        arguments=arguments,
    )

    decision = match_fault_plan_v0(plan, context)[0]

    assert plan.rules[0].kind == "http_error"
    assert plan.rules[0].parameters == {"status": 503}
    assert decision.matched is True


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"tool_id": "orders.get"}, "tool_mismatch"),
        ({"phase": "before_tool"}, "phase_mismatch"),
        ({"call_ordinal": 2}, "call_ordinal_mismatch"),
        ({"arguments": {"order_id": "ORD-9999"}}, "argument_mismatch"),
        ({"prior": {"refund-ack-lost": 1}}, "activation_cap_reached"),
    ],
)
def test_nonmatching_reasons_are_deterministic(change: dict[str, object], reason: str) -> None:
    plan = _plan()
    decision = match_fault_plan_v0(plan, _context(plan, **change))[0]  # type: ignore[arg-type]

    assert decision.matched is False
    assert decision.reason == reason
    assert decision.selection_bucket_ppm is None
    assert decision.activation_id is None


def test_probability_selection_is_repeatable_and_sensitive_to_semantic_input() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    activation = cast(dict[str, object], fault["activation"])
    activation["probability_ppm"] = 500_000
    scenario = _loads_document(document)
    plan = compile_fault_plan_v0(scenario)

    first = match_fault_plan_v0(plan, _context(plan, run_seed=7))[0]
    repeated = match_fault_plan_v0(plan, _context(plan, run_seed=7))[0]
    changed = match_fault_plan_v0(plan, _context(plan, run_seed=8))[0]

    assert first == repeated
    assert first.selection_bucket_ppm == repeated.selection_bucket_ppm
    assert changed.selection_bucket_ppm != first.selection_bucket_ppm


def test_probability_bucket_is_stable_across_a_range_of_seeds() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    cast(dict[str, object], fault["activation"])["probability_ppm"] = 500_000
    plan = compile_fault_plan_v0(_loads_document(document))

    first_pass = [
        match_fault_plan_v0(plan, _context(plan, run_seed=seed))[0] for seed in range(100)
    ]
    second_pass = [
        match_fault_plan_v0(plan, _context(plan, run_seed=seed))[0] for seed in range(100)
    ]

    assert first_pass == second_pass
    assert all(
        decision.selection_bucket_ppm is not None and 0 <= decision.selection_bucket_ppm < 1_000_000
        for decision in first_pass
    )


def test_physical_attempt_identity_changes_activation_identity_not_selection() -> None:
    plan = _plan()
    first_context = _context(plan)
    second_context = copy.copy(first_context)
    object.__setattr__(second_context, "physical_attempt_id", "attempt-refund-002")

    first = match_fault_plan_v0(plan, first_context)[0]
    second = match_fault_plan_v0(plan, second_context)[0]

    assert first.selection_bucket_ppm == second.selection_bucket_ppm
    assert first.activation_id != second.activation_id


def test_probability_not_selected_is_explicit() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    cast(dict[str, object], fault["activation"])["probability_ppm"] = 1
    plan = compile_fault_plan_v0(_loads_document(document))

    decision = match_fault_plan_v0(plan, _context(plan))[0]

    assert decision.matched is False
    assert decision.reason == "probability_not_selected"
    assert decision.selection_bucket_ppm is not None


def test_probability_one_ppm_selects_exact_bucket_zero_vector() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    cast(dict[str, object], fault["activation"])["probability_ppm"] = 1
    plan = compile_fault_plan_v0(_loads_document(document))

    decision = match_fault_plan_v0(plan, _context(plan, run_seed=1_453_656))[0]

    assert decision.matched is True
    assert decision.selection_bucket_ppm == 0


def test_argument_predicate_uses_json_semantics_without_bool_integer_aliasing() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    match = cast(dict[str, object], fault["match"])
    match["argument_equals"] = {"amount_minor": 1}
    plan = compile_fault_plan_v0(_loads_document(document))

    decision = match_fault_plan_v0(plan, _context(plan, arguments={"amount_minor": True}))[0]

    assert decision.reason == "argument_mismatch"


@pytest.mark.parametrize(
    ("expected", "actual"),
    [
        (1, 1.0),
        (-0.0, 0),
        ("\u00e9", "é"),
    ],
)
def test_argument_predicate_uses_jcs_equivalence(expected: object, actual: object) -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    cast(dict[str, object], fault["match"])["argument_equals"] = {"value": expected}
    plan = compile_fault_plan_v0(_loads_document(document))

    decision = match_fault_plan_v0(plan, _context(plan, arguments={"value": actual}))[0]

    assert decision.matched is True


def test_argument_predicate_does_not_normalize_unicode() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    cast(dict[str, object], fault["match"])["argument_equals"] = {"value": "é"}
    plan = compile_fault_plan_v0(_loads_document(document))

    decision = match_fault_plan_v0(plan, _context(plan, arguments={"value": "e\u0301"}))[0]

    assert decision.reason == "argument_mismatch"


def test_argument_object_key_order_does_not_change_selection() -> None:
    plan = _plan()
    first = _context(plan, arguments={"order_id": "ORD-1007", "amount_minor": 4200})
    second = _context(plan, arguments={"amount_minor": 4200, "order_id": "ORD-1007"})

    assert match_fault_plan_v0(plan, first) == match_fault_plan_v0(plan, second)


@pytest.mark.parametrize(
    "value",
    [
        9_007_199_254_740_992,
        float("nan"),
        float("inf"),
        float("-inf"),
        "\ud800",
        object(),
    ],
)
def test_non_jcs_argument_values_fail_closed(value: object) -> None:
    plan = _plan()
    context = _context(plan)
    object.__setattr__(context, "arguments", {"order_id": value})
    object.__setattr__(context, "arguments_digest", "sha256:" + "0" * 64)

    with pytest.raises(FaultRuleValidationError, match="JSON|RFC 8785"):
        match_fault_plan_v0(plan, context)


def test_tuple_is_rejected_as_non_json_input() -> None:
    plan = _plan()
    context = _context(plan)
    object.__setattr__(context, "arguments", {"order_id": ("ORD-1007",)})
    object.__setattr__(context, "arguments_digest", "sha256:" + "0" * 64)

    with pytest.raises(FaultRuleValidationError, match="tuple"):
        match_fault_plan_v0(plan, context)


def test_selected_fault_ids_are_explicit_unique_and_fail_closed() -> None:
    scenario = load_scenario(AMBIGUOUS_SCENARIO)
    assert compile_fault_plan_v0(scenario, selected_fault_ids=[]).rules == ()

    with pytest.raises(FaultRuleValidationError, match="unknown selected fault ID"):
        compile_fault_plan_v0(scenario, selected_fault_ids=["does-not-exist"])
    with pytest.raises(FaultRuleValidationError, match="must be unique"):
        compile_fault_plan_v0(scenario, selected_fault_ids=["refund-ack-lost", "refund-ack-lost"])


@pytest.mark.parametrize(
    ("kind", "phase"),
    [(kind, phase) for kind in KIND_PARAMETERS for phase in PHASES],
)
def test_full_scenario_v0_kind_phase_matrix(kind: str, phase: str) -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    fault = cast(dict[str, object], cast(list[object], document["faults"])[0])
    fault["kind"] = kind
    fault["parameters"] = KIND_PARAMETERS[kind]
    cast(dict[str, object], fault["match"])["phase"] = phase

    scenario = _loads_document(document)
    if phase in ALLOWED_PHASES[kind]:
        plan = compile_fault_plan_v0(scenario)
        assert plan.rules[0].kind == kind
        assert plan.rules[0].phase == phase
    else:
        with pytest.raises(FaultRuleValidationError, match="cannot run at phase"):
            compile_fault_plan_v0(scenario)


def test_context_digest_and_scenario_binding_fail_closed() -> None:
    plan = _plan()
    context = _context(plan)

    wrong_digest = copy.copy(context)
    object.__setattr__(wrong_digest, "arguments_digest", "sha256:" + "0" * 64)
    with pytest.raises(FaultRuleValidationError, match="arguments_digest"):
        match_fault_plan_v0(plan, wrong_digest)

    wrong_scenario = copy.copy(context)
    object.__setattr__(wrong_scenario, "scenario_digest", "sha256:" + "1" * 64)
    with pytest.raises(FaultRuleValidationError, match="compiled Scenario"):
        match_fault_plan_v0(plan, wrong_scenario)

    with pytest.raises(FaultRuleValidationError, match="unselected fault ID"):
        match_fault_plan_v0(plan, _context(plan, prior={"fabricated-fault": 1}))


def test_non_json_argument_keys_fail_closed_without_string_coercion() -> None:
    plan = _plan()
    context = _context(plan)
    object.__setattr__(context, "arguments", {1: "not-a-json-object-key"})

    with pytest.raises(FaultRuleValidationError, match="non-string object key"):
        match_fault_plan_v0(plan, context)


def test_arguments_are_snapshotted_once_before_digest_and_predicate_matching() -> None:
    plan = _plan()
    first = {"order_id": "ORD-1007", "amount_minor": 4200}
    changing = StatefulMapping(first, {"order_id": "ORD-9999", "amount_minor": 4200})
    context = _context(plan, arguments=changing, arguments_digest=_digest(first))

    decision = match_fault_plan_v0(plan, context)[0]

    assert decision.matched is True
    assert changing.items_calls == 1


def test_activation_history_is_snapshotted_once_before_cap_lookup() -> None:
    plan = _plan()
    changing = StatefulMapping({"refund-ack-lost": 1}, {"refund-ack-lost": 0})
    context = _context(plan)
    object.__setattr__(context, "prior_applied_occurrences", changing)

    decision = match_fault_plan_v0(plan, context)[0]

    assert decision.reason == "activation_cap_reached"
    assert changing.items_calls == 1


@pytest.mark.parametrize("value", [True, -1, 9_007_199_254_740_992])
def test_invalid_seed_fails_closed(value: object) -> None:
    plan = _plan()
    context = _context(plan)
    object.__setattr__(context, "run_seed", value)

    with pytest.raises(FaultRuleValidationError, match="run_seed"):
        match_fault_plan_v0(plan, context)


def test_compiled_objects_cannot_be_constructed_directly() -> None:
    with pytest.raises(TypeError, match="compile_fault_plan_v0"):
        CompiledFaultRule()
    with pytest.raises(TypeError, match="compile_fault_plan_v0"):
        CompiledFaultPlan()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("fault_id", ""),
        ("tool_id", "invented.tool"),
        ("kind", "unknown"),
        ("phase", "before_tool"),
        ("parameters", {}),
        ("parameters", {"status": 503}),
        ("argument_equals", {"order_id": "ORD-1007"}),
        ("parameters", {"duration_ms": 5000}),
    ],
)
def test_mutated_compiled_rules_fail_integrity(field: str, value: object) -> None:
    plan = _plan()
    forged_rule = copy.copy(plan.rules[0])
    object.__setattr__(forged_rule, field, value)
    forged_plan = copy.copy(plan)
    object.__setattr__(forged_plan, "rules", (forged_rule,))

    with pytest.raises(
        FaultRuleValidationError, match="created by|integrity|malformed|immutable|kind/phase"
    ):
        match_fault_plan_v0(forged_plan, _context(plan))


def test_mutable_fabricated_rule_mappings_cannot_change_after_validation() -> None:
    plan = _plan()
    predicates: dict[str, object] = {"order_id": "ORD-1007"}
    parameters: dict[str, object] = {"duration_ms": 5000}
    forged_rule = copy.copy(plan.rules[0])
    object.__setattr__(forged_rule, "argument_equals", predicates)
    object.__setattr__(forged_rule, "parameters", parameters)
    forged_plan = copy.copy(plan)
    object.__setattr__(forged_plan, "rules", (forged_rule,))
    predicates["order_id"] = "ORD-9999"
    parameters["duration_ms"] = 1

    with pytest.raises(FaultRuleValidationError, match="created by|integrity|malformed|immutable"):
        match_fault_plan_v0(forged_plan, _context(plan))


def test_fabricated_plans_fail_closed() -> None:
    plan = _plan()

    wrong_digest = copy.copy(plan)
    object.__setattr__(wrong_digest, "scenario_digest", "sha256:" + "1" * 64)
    with pytest.raises(FaultRuleValidationError, match="created by|Scenario binding|integrity"):
        match_fault_plan_v0(wrong_digest, _context(plan))

    mutable_rules = copy.copy(plan)
    object.__setattr__(mutable_rules, "rules", list(plan.rules))
    with pytest.raises(FaultRuleValidationError, match="created by|rules must be immutable"):
        match_fault_plan_v0(mutable_rules, _context(plan))

    invalid_rule = copy.copy(plan.rules[0])
    object.__setattr__(invalid_rule, "fault_id", "")
    injected = copy.copy(plan)
    object.__setattr__(injected, "rules", (invalid_rule,))
    with pytest.raises(FaultRuleValidationError, match="created by|malformed|integrity"):
        match_fault_plan_v0(injected, _context(plan))


def test_standalone_rule_matcher_is_not_public() -> None:
    import chaosagent_faults

    assert not hasattr(chaosagent_faults, "match_fault_rule_v0")


def test_compiled_rules_and_nested_parameters_are_immutable() -> None:
    rule = _plan().rules[0]

    with pytest.raises(TypeError):
        rule.parameters["duration_ms"] = 1  # type: ignore[index]
    with pytest.raises(TypeError):
        rule.argument_equals["order_id"] = "ORD-9999"  # type: ignore[index]


def test_rule_order_does_not_depend_on_authored_set_order() -> None:
    document = load_scenario(AMBIGUOUS_SCENARIO).to_dict()
    first = cast(dict[str, object], cast(list[object], document["faults"])[0])
    second = copy.deepcopy(first)
    second["id"] = "another-refund-fault"
    cast(dict[str, object], second["match"])["call_ordinal"] = 2
    cast(list[object], document["faults"]).append(second)

    forward = compile_fault_plan_v0(_loads_document(document))
    cast(list[object], document["faults"]).reverse()
    reverse = compile_fault_plan_v0(_loads_document(document))

    assert forward.rules == reverse.rules
    assert [rule.fault_id for rule in forward.rules] == [
        "another-refund-fault",
        "refund-ack-lost",
    ]


def test_compiler_revalidates_frozen_scenario_digest() -> None:
    scenario = load_scenario(AMBIGUOUS_SCENARIO)
    object.__setattr__(scenario, "digest", "sha256:" + "0" * 64)

    with pytest.raises(FaultRuleValidationError, match="bytes and digest disagree"):
        compile_fault_plan_v0(scenario)
