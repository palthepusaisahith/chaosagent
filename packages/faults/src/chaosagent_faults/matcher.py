"""Pure deterministic Scenario v0 fault-rule compiler and matcher."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, cast
from weakref import WeakValueDictionary

import rfc8785
from chaosagent_scenarios import (
    Scenario,
    ScenarioValidationError,
    loads_scenario,
)

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type JsonScalar = None | bool | int | float | str
type FaultPhase = Literal["before_tool", "after_commit", "after_tool"]
type FaultKind = Literal[
    "delay",
    "timeout",
    "http_error",
    "malformed_response",
    "stale_field",
    "ambiguous_post_commit_timeout",
    "auth_error",
    "indirect_prompt_injection",
    "duplicate_response",
]
type FaultDecisionReason = Literal[
    "matched",
    "tool_mismatch",
    "phase_mismatch",
    "call_ordinal_mismatch",
    "argument_mismatch",
    "activation_cap_reached",
    "probability_not_selected",
]

FAULT_MATCHER_V0_ALGORITHM = "chaosagent.fault-matcher/sha256-v0"
_JSON_SAFE_INTEGER_MAX = 9_007_199_254_740_991
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CATALOG_ID_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
_REVISION_RE = re.compile(r"^[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*$")
_TOOL_IDS = frozenset(
    {"orders.get", "shipping.get_status", "payments.refund", "support.update_ticket"}
)
_COMPILED_TOKEN = object()
_MAPPING_PROXY_TYPE: type[object] = type(MappingProxyType({}))

_KINDS_BY_PHASE: Mapping[FaultPhase, frozenset[str]] = MappingProxyType(
    {
        "before_tool": frozenset({"delay", "timeout", "http_error", "auth_error"}),
        "after_commit": frozenset({"ambiguous_post_commit_timeout"}),
        "after_tool": frozenset(
            {
                "delay",
                "timeout",
                "malformed_response",
                "stale_field",
                "indirect_prompt_injection",
                "duplicate_response",
            }
        ),
    }
)


class FaultRuleValidationError(ValueError):
    """Raised when a Scenario fault cannot be compiled or context is malformed."""


@dataclass(frozen=True, slots=True, init=False, weakref_slot=True)
class CompiledFaultRule:
    """Immutable executable match semantics for one validated Scenario v0 rule."""

    fault_id: str
    scenario_digest: str
    kind: FaultKind
    tool_id: str
    phase: FaultPhase
    call_ordinal: int | None
    argument_equals: Mapping[str, JsonScalar]
    probability_ppm: int
    max_occurrences: int
    parameters: Mapping[str, object]
    _construction_token: object = field(repr=False, compare=False)
    _integrity_digest: str = field(repr=False)

    def __init__(self) -> None:
        raise TypeError("CompiledFaultRule instances must be created by compile_fault_plan_v0")


@dataclass(frozen=True, slots=True, init=False, weakref_slot=True)
class CompiledFaultPlan:
    """Immutable, deterministically ordered rules from one frozen Scenario."""

    scenario_id: str
    scenario_revision: str
    scenario_digest: str
    rules: tuple[CompiledFaultRule, ...]
    _construction_token: object = field(repr=False, compare=False)
    _integrity_digest: str = field(repr=False)

    def __init__(self) -> None:
        raise TypeError("CompiledFaultPlan instances must be created by compile_fault_plan_v0")

    @property
    def digest(self) -> str:
        """Return the deterministic identity of this validated compiled plan."""
        _validate_plan(self)
        return self._integrity_digest

    @property
    def selected_fault_ids(self) -> tuple[str, ...]:
        """Return the exact ordered fault assignment represented by this plan."""
        _validate_plan(self)
        return tuple(rule.fault_id for rule in self.rules)


@dataclass(frozen=True, slots=True)
class FaultMatchContext:
    """Provider-neutral facts for one physical attempt at one logical call."""

    run_id: str
    run_seed: int
    scenario_digest: str
    tool_id: str
    phase: FaultPhase
    logical_call_id: str
    physical_attempt_id: str
    attempt_number: int
    call_ordinal: int
    arguments: Mapping[str, object]
    arguments_digest: str
    prior_applied_occurrences: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class FaultDecision:
    """A reproducible match decision; application and observation are later concerns."""

    fault_id: str
    matched: bool
    reason: FaultDecisionReason
    selection_bucket_ppm: int | None
    activation_id: str | None


_COMPILED_RULE_REGISTRY: WeakValueDictionary[int, CompiledFaultRule] = WeakValueDictionary()
_COMPILED_PLAN_REGISTRY: WeakValueDictionary[int, CompiledFaultPlan] = WeakValueDictionary()


@dataclass(frozen=True, slots=True)
class _FaultMatchSnapshot:
    run_id: str
    run_seed: int
    scenario_digest: str
    tool_id: str
    phase: FaultPhase
    logical_call_id: str
    physical_attempt_id: str
    attempt_number: int
    call_ordinal: int
    arguments: Mapping[str, JsonValue]
    arguments_digest: str
    prior_applied_occurrences: Mapping[str, int]


def compile_fault_plan_v0(
    scenario: Scenario, *, selected_fault_ids: Iterable[str] | None = None
) -> CompiledFaultPlan:
    """Compile selected Scenario v0 declarations without adding Campaign semantics."""
    validated = _validated_scenario(scenario)
    document = validated.to_dict()
    if document.get("schema_version") != "chaosagent.scenario/v0":
        raise FaultRuleValidationError("fault matcher v0 requires Scenario v0")
    faults = cast(list[dict[str, object]], document["faults"])
    by_id = {cast(str, fault["id"]): fault for fault in faults}

    if selected_fault_ids is None:
        selected = set(by_id)
    else:
        selected_list = list(selected_fault_ids)
        if any(not isinstance(item, str) for item in selected_list):
            raise FaultRuleValidationError("selected fault IDs must be strings")
        selected = set(selected_list)
        if len(selected) != len(selected_list):
            raise FaultRuleValidationError("selected fault IDs must be unique")
        unknown = sorted(selected.difference(by_id))
        if unknown:
            raise FaultRuleValidationError(f"unknown selected fault ID(s): {', '.join(unknown)}")

    rules = tuple(
        _compile_rule(by_id[fault_id], scenario_digest=validated.digest)
        for fault_id in sorted(selected)
    )
    return _new_compiled_plan(
        cast(str, document["scenario_id"]),
        cast(str, document["revision"]),
        validated.digest,
        rules,
    )


def match_fault_plan_v0(
    plan: CompiledFaultPlan, context: FaultMatchContext
) -> tuple[FaultDecision, ...]:
    """Evaluate all selected rules in stable fault-ID order."""
    _validate_plan(plan)
    snapshot = _snapshot_context(context)
    _validate_context(plan, snapshot)
    return tuple(_match_fault_rule_v0(rule, snapshot) for rule in plan.rules)


def expected_fault_activation_id_v0(
    rule: CompiledFaultRule,
    *,
    run_seed: int,
    run_id: str,
    logical_call_id: str,
    physical_attempt_id: str,
    attempt_number: int,
    call_ordinal: int,
    arguments_digest: str,
) -> str | None:
    """Recompute one Issue #13 activation identity from persisted request facts."""
    _validate_compiled_rule(rule)
    for field_name, value in (
        ("run_id", run_id),
        ("logical_call_id", logical_call_id),
        ("physical_attempt_id", physical_attempt_id),
    ):
        if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
            raise FaultRuleValidationError(f"{field_name} is malformed")
    _require_nonnegative_integer(run_seed, "run_seed", maximum=_JSON_SAFE_INTEGER_MAX)
    _require_positive_integer(attempt_number, "attempt_number", maximum=_JSON_SAFE_INTEGER_MAX)
    _require_positive_integer(call_ordinal, "call_ordinal", maximum=1000)
    _require_digest(arguments_digest, "arguments_digest")
    material = _selection_material_values(
        rule,
        run_seed=run_seed,
        run_id=run_id,
        logical_call_id=logical_call_id,
        attempt_number=attempt_number,
        call_ordinal=call_ordinal,
        arguments_digest=arguments_digest,
    )
    if _selection_bucket(material) >= rule.probability_ppm:
        return None
    activation_material = material + b"\x00" + physical_attempt_id.encode("utf-8")
    return f"activation-{hashlib.sha256(activation_material).hexdigest()}"


def _match_fault_rule_v0(rule: CompiledFaultRule, context: _FaultMatchSnapshot) -> FaultDecision:
    """Evaluate one trusted compiled rule against one immutable context snapshot."""
    if context.scenario_digest != rule.scenario_digest:
        raise FaultRuleValidationError("match context does not reference the compiled Scenario")

    if context.tool_id != rule.tool_id:
        return _not_matched(rule, "tool_mismatch")
    if context.phase != rule.phase:
        return _not_matched(rule, "phase_mismatch")
    if rule.call_ordinal is not None and context.call_ordinal != rule.call_ordinal:
        return _not_matched(rule, "call_ordinal_mismatch")
    for name, expected in rule.argument_equals.items():
        if name not in context.arguments or not _json_equal(context.arguments[name], expected):
            return _not_matched(rule, "argument_mismatch")

    prior = context.prior_applied_occurrences.get(rule.fault_id, 0)
    if prior >= rule.max_occurrences:
        return _not_matched(rule, "activation_cap_reached")

    selection_material = _selection_material(rule, context)
    bucket = _selection_bucket(selection_material)
    if bucket >= rule.probability_ppm:
        return FaultDecision(
            fault_id=rule.fault_id,
            matched=False,
            reason="probability_not_selected",
            selection_bucket_ppm=bucket,
            activation_id=None,
        )
    activation_material = selection_material + b"\x00" + context.physical_attempt_id.encode("utf-8")
    return FaultDecision(
        fault_id=rule.fault_id,
        matched=True,
        reason="matched",
        selection_bucket_ppm=bucket,
        activation_id=f"activation-{hashlib.sha256(activation_material).hexdigest()}",
    )


def _validated_scenario(scenario: Scenario) -> Scenario:
    if not isinstance(scenario, Scenario):
        raise FaultRuleValidationError("scenario must be a validated Scenario")
    try:
        validated = loads_scenario(scenario.canonical_bytes)
    except (AttributeError, ScenarioValidationError, TypeError) as error:
        raise FaultRuleValidationError("scenario is not a valid frozen Scenario v0") from error
    if scenario.digest != validated.digest:
        raise FaultRuleValidationError("scenario canonical bytes and digest disagree")
    return validated


def _compile_rule(document: Mapping[str, object], *, scenario_digest: str) -> CompiledFaultRule:
    fault_id = cast(str, document["id"])
    kind = cast(FaultKind, document["kind"])
    match = cast(dict[str, object], document["match"])
    phase = cast(FaultPhase, match["phase"])
    if kind not in _KINDS_BY_PHASE[phase]:
        raise FaultRuleValidationError(
            f"fault {fault_id!r}: kind {kind!r} cannot run at phase {phase!r}"
        )

    activation = cast(dict[str, object], document["activation"])
    arguments = cast(dict[str, JsonScalar], match.get("argument_equals", {}))
    parameters = cast(dict[str, object], document["parameters"])
    rule = _new_compiled_rule(
        fault_id=fault_id,
        scenario_digest=scenario_digest,
        kind=kind,
        tool_id=cast(str, match["tool_id"]),
        phase=phase,
        call_ordinal=cast(int | None, match.get("call_ordinal")),
        argument_equals=cast(Mapping[str, JsonScalar], _freeze_compiled_json(arguments)),
        probability_ppm=cast(int, activation["probability_ppm"]),
        max_occurrences=cast(int, activation["max_occurrences"]),
        parameters=cast(Mapping[str, object], _freeze_compiled_json(parameters)),
    )
    _validate_compiled_rule(rule)
    return rule


def _new_compiled_rule(
    *,
    fault_id: str,
    scenario_digest: str,
    kind: FaultKind,
    tool_id: str,
    phase: FaultPhase,
    call_ordinal: int | None,
    argument_equals: Mapping[str, JsonScalar],
    probability_ppm: int,
    max_occurrences: int,
    parameters: Mapping[str, object],
) -> CompiledFaultRule:
    rule = object.__new__(CompiledFaultRule)
    for name, value in (
        ("fault_id", fault_id),
        ("scenario_digest", scenario_digest),
        ("kind", kind),
        ("tool_id", tool_id),
        ("phase", phase),
        ("call_ordinal", call_ordinal),
        ("argument_equals", argument_equals),
        ("probability_ppm", probability_ppm),
        ("max_occurrences", max_occurrences),
        ("parameters", parameters),
        ("_construction_token", _COMPILED_TOKEN),
    ):
        object.__setattr__(rule, name, value)
    object.__setattr__(rule, "_integrity_digest", _digest_compiled_rule(rule))
    _COMPILED_RULE_REGISTRY[id(rule)] = rule
    return rule


def _new_compiled_plan(
    scenario_id: str,
    scenario_revision: str,
    scenario_digest: str,
    rules: tuple[CompiledFaultRule, ...],
) -> CompiledFaultPlan:
    plan = object.__new__(CompiledFaultPlan)
    for name, value in (
        ("scenario_id", scenario_id),
        ("scenario_revision", scenario_revision),
        ("scenario_digest", scenario_digest),
        ("rules", rules),
        ("_construction_token", _COMPILED_TOKEN),
    ):
        object.__setattr__(plan, name, value)
    object.__setattr__(plan, "_integrity_digest", _digest_compiled_plan(plan))
    _COMPILED_PLAN_REGISTRY[id(plan)] = plan
    return plan


def _digest_compiled_rule(rule: CompiledFaultRule) -> str:
    document: dict[str, JsonValue] = {
        "fault_id": _require_catalog_id(rule.fault_id, "fault_id"),
        "scenario_digest": _require_digest(rule.scenario_digest, "scenario_digest"),
        "kind": _require_kind(rule.kind),
        "tool_id": _require_tool_id(rule.tool_id),
        "phase": _require_phase(rule.phase),
        "argument_equals": cast(dict[str, JsonValue], _thaw_compiled_json(rule.argument_equals)),
        "probability_ppm": _require_positive_integer(
            rule.probability_ppm, "probability_ppm", maximum=1_000_000
        ),
        "max_occurrences": _require_positive_integer(
            rule.max_occurrences, "max_occurrences", maximum=1000
        ),
        "parameters": cast(dict[str, JsonValue], _thaw_compiled_json(rule.parameters)),
    }
    if rule.call_ordinal is not None:
        document["call_ordinal"] = _require_positive_integer(
            rule.call_ordinal, "call_ordinal", maximum=1000
        )
    if rule.kind not in _KINDS_BY_PHASE[rule.phase]:
        raise FaultRuleValidationError("compiled fault kind/phase combination is invalid")
    return _digest_canonical(document)


def _digest_compiled_plan(plan: CompiledFaultPlan) -> str:
    scenario_id = _require_catalog_id(plan.scenario_id, "scenario_id")
    if (
        not isinstance(plan.scenario_revision, str)
        or _REVISION_RE.fullmatch(plan.scenario_revision) is None
    ):
        raise FaultRuleValidationError("compiled plan has a malformed Scenario revision")
    scenario_digest = _require_digest(plan.scenario_digest, "scenario_digest")
    fault_ids = [rule.fault_id for rule in plan.rules]
    if fault_ids != sorted(set(fault_ids)):
        raise FaultRuleValidationError("compiled rules must have unique fault IDs in sorted order")
    document: dict[str, JsonValue] = {
        "scenario_id": scenario_id,
        "scenario_revision": plan.scenario_revision,
        "scenario_digest": scenario_digest,
        "rules": [rule._integrity_digest for rule in plan.rules],
    }
    return _digest_canonical(document)


def _snapshot_context(context: FaultMatchContext) -> _FaultMatchSnapshot:
    if not isinstance(context, FaultMatchContext):
        raise FaultRuleValidationError("context must be FaultMatchContext")
    try:
        arguments = _snapshot_json_object(context.arguments, field="arguments")
        history_items = list(context.prior_applied_occurrences.items())
    except (AttributeError, RuntimeError, TypeError, ValueError) as error:
        if isinstance(error, FaultRuleValidationError):
            raise
        raise FaultRuleValidationError("context mappings could not be snapshotted") from error

    history: dict[str, int] = {}
    for fault_id, count in history_items:
        if fault_id in history:
            raise FaultRuleValidationError("prior_applied_occurrences contains duplicate fault IDs")
        if not isinstance(fault_id, str) or _CATALOG_ID_RE.fullmatch(fault_id) is None:
            raise FaultRuleValidationError(
                "prior_applied_occurrences contains a malformed fault ID"
            )
        history[fault_id] = _require_nonnegative_integer(
            count, "prior applied occurrence count", maximum=1000
        )

    snapshot = _FaultMatchSnapshot(
        run_id=context.run_id,
        run_seed=context.run_seed,
        scenario_digest=context.scenario_digest,
        tool_id=context.tool_id,
        phase=context.phase,
        logical_call_id=context.logical_call_id,
        physical_attempt_id=context.physical_attempt_id,
        attempt_number=context.attempt_number,
        call_ordinal=context.call_ordinal,
        arguments=cast(Mapping[str, JsonValue], _freeze_snapshot_json(arguments)),
        arguments_digest=context.arguments_digest,
        prior_applied_occurrences=MappingProxyType(history),
    )
    _validate_context_values(snapshot)
    return snapshot


def _validate_compiled_rule(rule: CompiledFaultRule) -> None:
    if not isinstance(rule, CompiledFaultRule):
        raise FaultRuleValidationError("rule must be compiled by the v0 compiler")
    try:
        if (
            rule._construction_token is not _COMPILED_TOKEN
            or _COMPILED_RULE_REGISTRY.get(id(rule)) is not rule
        ):
            raise FaultRuleValidationError("rule was not created by the v0 compiler")
        if not isinstance(rule.argument_equals, _MAPPING_PROXY_TYPE) or not _is_frozen_json(
            rule.argument_equals
        ):
            raise FaultRuleValidationError("compiled argument predicates must be immutable")
        if not isinstance(rule.parameters, _MAPPING_PROXY_TYPE) or not _is_frozen_json(
            rule.parameters
        ):
            raise FaultRuleValidationError("compiled parameters must be immutable")
        expected_integrity = _digest_compiled_rule(rule)
    except (AttributeError, TypeError, ValueError, rfc8785.CanonicalizationError) as error:
        raise FaultRuleValidationError("compiled fault rule is malformed") from error
    if rule._integrity_digest != expected_integrity:
        raise FaultRuleValidationError("compiled fault rule integrity check failed")


def _validate_plan(plan: CompiledFaultPlan) -> None:
    if not isinstance(plan, CompiledFaultPlan):
        raise FaultRuleValidationError("plan must be compiled by the v0 compiler")
    try:
        if (
            plan._construction_token is not _COMPILED_TOKEN
            or _COMPILED_PLAN_REGISTRY.get(id(plan)) is not plan
        ):
            raise FaultRuleValidationError("plan was not created by the v0 compiler")
        if not isinstance(plan.rules, tuple):
            raise FaultRuleValidationError("compiled plan rules must be immutable")
        for rule in plan.rules:
            _validate_compiled_rule(rule)
            if rule.scenario_digest != plan.scenario_digest:
                raise FaultRuleValidationError("compiled rule has the wrong Scenario binding")
        expected_integrity = _digest_compiled_plan(plan)
    except (AttributeError, TypeError, ValueError, rfc8785.CanonicalizationError) as error:
        if isinstance(error, FaultRuleValidationError):
            raise
        raise FaultRuleValidationError("compiled fault plan is malformed") from error
    if plan._integrity_digest != expected_integrity:
        raise FaultRuleValidationError("compiled fault plan integrity check failed")


def _validate_context(plan: CompiledFaultPlan, context: _FaultMatchSnapshot) -> None:
    if context.scenario_digest != plan.scenario_digest:
        raise FaultRuleValidationError("match context does not reference the compiled Scenario")
    selected_ids = {rule.fault_id for rule in plan.rules}
    unknown_counts = sorted(set(context.prior_applied_occurrences).difference(selected_ids))
    if unknown_counts:
        raise FaultRuleValidationError(
            "prior applied occurrence history contains unselected fault ID(s): "
            + ", ".join(unknown_counts)
        )


def _validate_context_values(context: _FaultMatchSnapshot) -> None:
    for field_name, value in (
        ("run_id", context.run_id),
        ("logical_call_id", context.logical_call_id),
        ("physical_attempt_id", context.physical_attempt_id),
    ):
        if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
            raise FaultRuleValidationError(f"{field_name} is malformed")
    if (
        not isinstance(context.scenario_digest, str)
        or _DIGEST_RE.fullmatch(context.scenario_digest) is None
    ):
        raise FaultRuleValidationError("scenario_digest is malformed")
    _require_nonnegative_integer(context.run_seed, "run_seed", maximum=_JSON_SAFE_INTEGER_MAX)
    _require_positive_integer(
        context.attempt_number, "attempt_number", maximum=_JSON_SAFE_INTEGER_MAX
    )
    _require_positive_integer(context.call_ordinal, "call_ordinal", maximum=1000)
    _require_phase(context.phase)
    _require_tool_id(context.tool_id)
    if not isinstance(context.arguments, Mapping):
        raise FaultRuleValidationError("arguments must be a JSON object")
    calculated_digest = _digest_frozen_json(context.arguments)
    if context.arguments_digest != calculated_digest:
        raise FaultRuleValidationError("arguments_digest does not match arguments")
    if not isinstance(context.prior_applied_occurrences, Mapping):
        raise FaultRuleValidationError("prior_applied_occurrences must be a mapping")
    for fault_id, count in context.prior_applied_occurrences.items():
        if not isinstance(fault_id, str) or _CATALOG_ID_RE.fullmatch(fault_id) is None:
            raise FaultRuleValidationError(
                "prior_applied_occurrences contains a malformed fault ID"
            )
        _require_nonnegative_integer(count, "prior applied occurrence count", maximum=1000)


def _selection_material(rule: CompiledFaultRule, context: _FaultMatchSnapshot) -> bytes:
    return _selection_material_values(
        rule,
        run_seed=context.run_seed,
        run_id=context.run_id,
        logical_call_id=context.logical_call_id,
        attempt_number=context.attempt_number,
        call_ordinal=context.call_ordinal,
        arguments_digest=context.arguments_digest,
    )


def _selection_material_values(
    rule: CompiledFaultRule,
    *,
    run_seed: int,
    run_id: str,
    logical_call_id: str,
    attempt_number: int,
    call_ordinal: int,
    arguments_digest: str,
) -> bytes:
    value: dict[str, JsonValue] = {
        "algorithm": FAULT_MATCHER_V0_ALGORITHM,
        "run_seed": run_seed,
        "run_id": run_id,
        "scenario_digest": rule.scenario_digest,
        "fault_id": rule.fault_id,
        "tool_id": rule.tool_id,
        "phase": rule.phase,
        "logical_call_id": logical_call_id,
        "attempt_number": attempt_number,
        "call_ordinal": call_ordinal,
        "arguments_digest": arguments_digest,
    }
    return rfc8785.dumps(value)


def _selection_bucket(material: bytes) -> int:
    sample = int.from_bytes(hashlib.sha256(material).digest(), "big")
    return sample * 1_000_000 // (1 << 256)


def _not_matched(rule: CompiledFaultRule, reason: FaultDecisionReason) -> FaultDecision:
    return FaultDecision(rule.fault_id, False, reason, None, None)


def _json_equal(left: object, right: object) -> bool:
    try:
        return rfc8785.dumps(_thaw_compiled_json(left)) == rfc8785.dumps(_thaw_compiled_json(right))
    except (rfc8785.CanonicalizationError, TypeError) as error:
        raise FaultRuleValidationError("arguments are outside the supported JSON domain") from error


def _digest_frozen_json(value: object) -> str:
    try:
        canonical = rfc8785.dumps(_thaw_compiled_json(value))
    except (rfc8785.CanonicalizationError, TypeError) as error:
        raise FaultRuleValidationError("arguments are outside the supported JSON domain") from error
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _freeze_compiled_json(value: object) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise FaultRuleValidationError("JSON object keys must be strings")
        return MappingProxyType(
            {cast(str, key): _freeze_compiled_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_compiled_json(item) for item in value)
    return value


def _freeze_snapshot_json(value: JsonValue) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_snapshot_json(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_snapshot_json(item) for item in value)
    return value


def _is_frozen_json(value: object) -> bool:
    if type(value) is _MAPPING_PROXY_TYPE:
        mapping = cast(Mapping[object, object], value)
        return all(isinstance(key, str) and _is_frozen_json(item) for key, item in mapping.items())
    if isinstance(value, tuple):
        return all(_is_frozen_json(item) for item in value)
    return value is None or isinstance(value, bool | int | float | str)


def _thaw_compiled_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise FaultRuleValidationError("JSON object keys must be strings")
        return {cast(str, key): _thaw_compiled_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_compiled_json(item) for item in value]
    if isinstance(value, list):
        raise FaultRuleValidationError("compiled JSON arrays must be immutable")
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise FaultRuleValidationError("value is outside the supported JSON domain")


def _snapshot_json_object(value: object, *, field: str) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise FaultRuleValidationError(f"{field} must be a JSON object")
    snapshot = _snapshot_json_mapping(value, field=field)
    _digest_canonical(snapshot)
    return snapshot


def _snapshot_json_mapping(value: Mapping[object, object], *, field: str) -> dict[str, JsonValue]:
    try:
        items = list(value.items())
    except (RuntimeError, TypeError, ValueError) as error:
        raise FaultRuleValidationError(f"{field} could not be snapshotted") from error
    result: dict[str, JsonValue] = {}
    for key, item in items:
        if not isinstance(key, str):
            raise FaultRuleValidationError(f"{field} contains a non-string object key")
        if key in result:
            raise FaultRuleValidationError(f"{field} contains duplicate object key {key!r}")
        result[key] = _snapshot_json_value(item, field=f"{field}.{key}")
    return result


def _snapshot_json_value(value: object, *, field: str) -> JsonValue:
    if isinstance(value, Mapping):
        return _snapshot_json_mapping(value, field=field)
    if isinstance(value, list):
        return [
            _snapshot_json_value(item, field=f"{field}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, tuple):
        raise FaultRuleValidationError(f"{field} contains a tuple, which is not a JSON array")
    if value is None or isinstance(value, bool | int | float | str):
        return value
    raise FaultRuleValidationError(f"{field} contains a value outside the JSON domain")


def _digest_canonical(value: JsonValue) -> str:
    try:
        canonical = rfc8785.dumps(value)
    except (rfc8785.CanonicalizationError, TypeError) as error:
        raise FaultRuleValidationError("value is outside the RFC 8785 JSON domain") from error
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _require_catalog_id(value: object, field: str) -> str:
    if not isinstance(value, str) or _CATALOG_ID_RE.fullmatch(value) is None:
        raise FaultRuleValidationError(f"{field} is malformed")
    return value


def _require_digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise FaultRuleValidationError(f"{field} is malformed")
    return value


def _require_kind(value: object) -> FaultKind:
    if not isinstance(value, str) or not any(value in kinds for kinds in _KINDS_BY_PHASE.values()):
        raise FaultRuleValidationError("fault kind is outside the Scenario v0 vocabulary")
    return cast(FaultKind, value)


def _require_phase(value: object) -> FaultPhase:
    if value not in _KINDS_BY_PHASE:
        raise FaultRuleValidationError("phase is outside the Scenario v0 vocabulary")
    return cast(FaultPhase, value)


def _require_tool_id(value: object) -> str:
    if not isinstance(value, str) or value not in _TOOL_IDS:
        raise FaultRuleValidationError("tool_id is outside the Scenario v0 vocabulary")
    return value


def _require_positive_integer(value: object, field: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise FaultRuleValidationError(f"{field} must be an integer from 1 through {maximum}")
    return value


def _require_nonnegative_integer(value: object, field: str, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise FaultRuleValidationError(f"{field} must be an integer from 0 through {maximum}")
    return value
