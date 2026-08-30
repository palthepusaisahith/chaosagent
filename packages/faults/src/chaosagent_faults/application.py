"""Deterministic Issue #14 application of trusted fault-match decisions."""

from __future__ import annotations

import time
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType
from typing import Literal, Protocol, cast
from weakref import WeakValueDictionary

from .matcher import (
    CompiledFaultPlan,
    CompiledFaultRule,
    FaultDecision,
    FaultMatchContext,
    FaultPhase,
    _validate_plan,
    expected_fault_activation_id_v0,
    match_fault_plan_v0,
)

type FaultFailureCode = Literal[
    "fault_timeout",
    "fault_http_429",
    "fault_http_503",
    "fault_auth_401",
    "fault_auth_403",
    "fault_malformed_response",
]


class FaultApplicationError(RuntimeError):
    """Raised when a trusted directive cannot be applied safely."""


class FaultSleeper(Protocol):
    """Small synchronous delay boundary used by production and deterministic tests."""

    def sleep_ms(self, duration_ms: int) -> None: ...


class BlockingFaultSleeper:
    """Production sleeper; a delay blocks only the current Gateway execution path."""

    def sleep_ms(self, duration_ms: int) -> None:
        time.sleep(duration_ms / 1000)


@dataclass(frozen=True, slots=True, weakref_slot=True)
class FaultSelection:
    """Matcher decisions and the trusted rules selected for one phase."""

    decisions: tuple[FaultDecision, ...]
    matched_rules: tuple[CompiledFaultRule, ...]
    reportable_not_matched: tuple[FaultDecision, ...]


@dataclass(frozen=True, slots=True)
class AppliedFault:
    """One directive that actually altered timing, execution, or observation."""

    rule: CompiledFaultRule
    activation_id: str


@dataclass(frozen=True, slots=True)
class FaultApplicationResult:
    """Provider-neutral observed result after applying one phase's directives."""

    applied: tuple[AppliedFault, ...]
    output: Mapping[str, object] | None
    failure_code: FaultFailureCode | None


class FaultEngine:
    """Narrow bridge from the pure matcher to deterministic fault application."""

    def __init__(
        self,
        plan: CompiledFaultPlan,
        *,
        run_seed: int,
        sleeper: FaultSleeper | None = None,
    ) -> None:
        if isinstance(run_seed, bool) or not isinstance(run_seed, int):
            raise ValueError("run_seed must be an exact integer")
        if not 0 <= run_seed <= 9_007_199_254_740_991:
            raise ValueError("run_seed must be a nonnegative JSON safe integer")
        self._plan = plan
        self._run_seed = run_seed
        self._sleeper = sleeper or BlockingFaultSleeper()
        self._issued: WeakValueDictionary[int, FaultSelection] = WeakValueDictionary()
        self._fingerprints: dict[int, tuple[object, ...]] = {}
        self._selection_lock = Lock()

    @property
    def scenario_digest(self) -> str:
        return self._plan.scenario_digest

    @property
    def scenario_id(self) -> str:
        return self._plan.scenario_id

    @property
    def scenario_revision(self) -> str:
        return self._plan.scenario_revision

    @property
    def rules(self) -> tuple[CompiledFaultRule, ...]:
        """Return the validated immutable rules used to authenticate history."""
        # Selection validates the plan as well, but history must fail closed before
        # it is allowed to influence a new match.
        _validate_plan(self._plan)
        return self._plan.rules

    def has_after_commit_rule(self, tool_id: str) -> bool:
        """Return whether the frozen plan can target this tool after commit."""
        return any(rule.tool_id == tool_id and rule.phase == "after_commit" for rule in self.rules)

    def authenticates_activation(
        self,
        rule: CompiledFaultRule,
        *,
        run_id: str,
        logical_call_id: str,
        physical_attempt_id: str,
        attempt_number: int,
        arguments_digest: str,
        activation_id: str,
        call_ordinal: int | None = None,
    ) -> bool:
        """Verify history using Issue #13's exact activation identity material."""
        ordinals = (
            (call_ordinal,)
            if call_ordinal is not None
            else (rule.call_ordinal,)
            if rule.call_ordinal is not None
            else tuple(range(1, 1001))
        )
        matches = 0
        for call_ordinal in ordinals:
            expected = expected_fault_activation_id_v0(
                rule,
                run_seed=self._run_seed,
                run_id=run_id,
                logical_call_id=logical_call_id,
                physical_attempt_id=physical_attempt_id,
                attempt_number=attempt_number,
                call_ordinal=call_ordinal,
                arguments_digest=arguments_digest,
            )
            if expected == activation_id:
                matches += 1
        return matches == 1

    def select(
        self,
        *,
        run_id: str,
        scenario_digest: str,
        tool_id: str,
        phase: FaultPhase,
        logical_call_id: str,
        physical_attempt_id: str,
        attempt_number: int,
        call_ordinal: int,
        arguments: Mapping[str, object],
        arguments_digest: str,
        prior_applied_occurrences: Mapping[str, int],
    ) -> FaultSelection:
        context = FaultMatchContext(
            run_id=run_id,
            run_seed=self._run_seed,
            scenario_digest=scenario_digest,
            tool_id=tool_id,
            phase=phase,
            logical_call_id=logical_call_id,
            physical_attempt_id=physical_attempt_id,
            attempt_number=attempt_number,
            call_ordinal=call_ordinal,
            arguments=arguments,
            arguments_digest=arguments_digest,
            prior_applied_occurrences=prior_applied_occurrences,
        )
        decisions = match_fault_plan_v0(self._plan, context)
        by_id = {rule.fault_id: rule for rule in self._plan.rules}
        matched = tuple(by_id[item.fault_id] for item in decisions if item.matched)
        reportable = tuple(
            item
            for item in decisions
            if not item.matched
            and by_id[item.fault_id].tool_id == tool_id
            and by_id[item.fault_id].phase == phase
        )
        selection = FaultSelection(decisions, matched, reportable)
        with self._selection_lock:
            for expired in self._fingerprints.keys() - self._issued.keys():
                self._fingerprints.pop(expired, None)
            self._issued[id(selection)] = selection
            self._fingerprints[id(selection)] = _selection_fingerprint(selection)
        return selection

    def apply_before(self, selection: FaultSelection) -> FaultApplicationResult:
        self._validate_selection(selection)
        applied: list[AppliedFault] = []
        failure: FaultFailureCode | None = None
        for rule in selection.matched_rules:
            activation_id = _activation_id(selection, rule)
            _validate_parameters(rule)
            if rule.kind == "delay":
                self._sleeper.sleep_ms(_duration(rule))
                applied.append(AppliedFault(rule, activation_id))
            elif failure is None and rule.kind in {"timeout", "http_error", "auth_error"}:
                failure = _failure_code(rule)
                applied.append(AppliedFault(rule, activation_id))
            elif rule.kind not in {"timeout", "http_error", "auth_error"}:
                raise FaultApplicationError("unsupported before_tool fault directive")
        return FaultApplicationResult(tuple(applied), None, failure)

    def apply_after(
        self, selection: FaultSelection, output: Mapping[str, object]
    ) -> FaultApplicationResult:
        self._validate_selection(selection)
        observed: dict[str, object] | None = deepcopy(dict(output))
        applied: list[AppliedFault] = []
        failure: FaultFailureCode | None = None
        transformed = False
        for rule in selection.matched_rules:
            activation_id = _activation_id(selection, rule)
            _validate_parameters(rule)
            if rule.kind == "delay":
                self._sleeper.sleep_ms(_duration(rule))
                applied.append(AppliedFault(rule, activation_id))
                continue
            if transformed:
                continue
            if rule.kind == "timeout":
                observed = None
                failure = "fault_timeout"
            elif rule.kind == "malformed_response":
                mode = cast(str, rule.parameters["mode"])
                observed = {
                    "_chaosagent_fault": {
                        "kind": "malformed_response",
                        "mode": mode,
                        "representation": "<invalid-json>" if mode == "invalid_json" else {},
                    }
                }
                failure = "fault_malformed_response"
            elif rule.kind == "stale_field":
                assert observed is not None
                _replace_json_pointer(
                    observed,
                    cast(str, rule.parameters["json_pointer"]),
                    deepcopy(rule.parameters["value"]),
                )
            elif rule.kind == "indirect_prompt_injection":
                assert observed is not None
                observed = {
                    "_chaosagent_fault": {
                        "kind": "indirect_prompt_injection",
                        "untrusted_content": cast(str, rule.parameters["content"]),
                    },
                    "response": observed,
                }
            elif rule.kind == "duplicate_response":
                assert observed is not None
                deliveries = cast(int, rule.parameters["deliveries"])
                observed = {
                    "_chaosagent_fault": {
                        "kind": "duplicate_response",
                        "deliveries": deliveries,
                    },
                    "responses": [deepcopy(observed) for _ in range(deliveries)],
                }
            else:
                raise FaultApplicationError("unsupported after_tool fault directive")
            transformed = True
            applied.append(AppliedFault(rule, activation_id))
        frozen = None if observed is None else cast(Mapping[str, object], _freeze_json(observed))
        return FaultApplicationResult(tuple(applied), frozen, failure)

    def apply_post_commit(self, selection: FaultSelection) -> FaultApplicationResult:
        """Replace only the acknowledgement of an already-established mutation."""
        self._validate_selection(selection)
        applied: list[AppliedFault] = []
        for rule in selection.matched_rules:
            _validate_parameters(rule)
            if rule.kind != "ambiguous_post_commit_timeout" or rule.phase != "after_commit":
                raise FaultApplicationError("unsupported after_commit fault directive")
            if not applied:
                applied.append(AppliedFault(rule, _activation_id(selection, rule)))
        return FaultApplicationResult(tuple(applied), None, "fault_timeout" if applied else None)

    def _validate_selection(self, selection: FaultSelection) -> None:
        if not isinstance(selection, FaultSelection):
            raise FaultApplicationError("fault selection is malformed")
        with self._selection_lock:
            issued = self._issued.get(id(selection))
            fingerprint = self._fingerprints.get(id(selection))
        if issued is not selection or fingerprint != _selection_fingerprint(selection):
            raise FaultApplicationError("fault selection was not issued by this engine")


def _activation_id(selection: FaultSelection, rule: CompiledFaultRule) -> str:
    matches = [
        item.activation_id
        for item in selection.decisions
        if item.matched and item.fault_id == rule.fault_id
    ]
    if len(matches) != 1 or matches[0] is None:
        raise FaultApplicationError("fault selection has an invalid activation binding")
    return matches[0]


def _selection_fingerprint(selection: FaultSelection) -> tuple[object, ...]:
    return (
        tuple(
            (
                decision.fault_id,
                decision.matched,
                decision.reason,
                decision.selection_bucket_ppm,
                decision.activation_id,
            )
            for decision in selection.decisions
        ),
        tuple(_rule_fingerprint(rule) for rule in selection.matched_rules),
        tuple(
            (
                decision.fault_id,
                decision.matched,
                decision.reason,
                decision.selection_bucket_ppm,
                decision.activation_id,
            )
            for decision in selection.reportable_not_matched
        ),
    )


def _rule_fingerprint(rule: CompiledFaultRule) -> tuple[object, ...]:
    return (
        id(rule),
        rule.fault_id,
        rule.scenario_digest,
        rule.kind,
        rule.tool_id,
        rule.phase,
        rule.call_ordinal,
        _json_fingerprint(rule.argument_equals),
        rule.probability_ppm,
        rule.max_occurrences,
        _json_fingerprint(rule.parameters),
    )


def _json_fingerprint(value: object) -> object:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _json_fingerprint(item)) for key, item in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_json_fingerprint(item) for item in value)
    return value


def _duration(rule: CompiledFaultRule) -> int:
    return cast(int, rule.parameters["duration_ms"])


def _failure_code(rule: CompiledFaultRule) -> FaultFailureCode:
    if rule.kind == "timeout":
        return "fault_timeout"
    status = cast(int, rule.parameters["status"])
    if rule.kind == "http_error":
        return cast(FaultFailureCode, f"fault_http_{status}")
    if rule.kind == "auth_error":
        return cast(FaultFailureCode, f"fault_auth_{status}")
    raise FaultApplicationError("fault does not define a failure code")


def _validate_parameters(rule: CompiledFaultRule) -> None:
    parameters = rule.parameters
    if rule.kind in {"delay", "timeout", "ambiguous_post_commit_timeout"}:
        if set(parameters) != {"duration_ms"}:
            raise FaultApplicationError("duration fault parameters are malformed")
        value = parameters["duration_ms"]
        if type(value) is not int or not 1 <= value <= 600_000:
            raise FaultApplicationError("duration_ms is outside the Scenario v0 range")
        return
    if rule.kind == "http_error":
        _require_exact_status(parameters, {429, 503})
        return
    if rule.kind == "auth_error":
        _require_exact_status(parameters, {401, 403})
        return
    if rule.kind == "malformed_response":
        if set(parameters) != {"mode"} or parameters.get("mode") not in {
            "invalid_json",
            "schema_violation",
        }:
            raise FaultApplicationError("malformed_response parameters are invalid")
        return
    if rule.kind == "stale_field":
        if set(parameters) != {"json_pointer", "value"}:
            raise FaultApplicationError("stale_field parameters are malformed")
        pointer = parameters["json_pointer"]
        if not isinstance(pointer, str) or not pointer.startswith("/"):
            raise FaultApplicationError("stale_field json_pointer is invalid")
        _validate_json(parameters["value"])
        return
    if rule.kind == "indirect_prompt_injection":
        content = parameters.get("content")
        if (
            set(parameters) != {"content"}
            or not isinstance(content, str)
            or not 1 <= len(content) <= 4000
        ):
            raise FaultApplicationError("prompt injection parameters are invalid")
        return
    if rule.kind == "duplicate_response":
        deliveries = parameters.get("deliveries")
        if (
            set(parameters) != {"deliveries"}
            or type(deliveries) is not int
            or not 2 <= deliveries <= 10
        ):
            raise FaultApplicationError("duplicate response parameters are invalid")
        return
    raise FaultApplicationError("fault kind is outside the Issue #14 application boundary")


def _require_exact_status(parameters: Mapping[str, object], allowed: set[int]) -> None:
    status = parameters.get("status")
    if set(parameters) != {"status"} or type(status) is not int or status not in allowed:
        raise FaultApplicationError("fault status parameters are invalid")


def _replace_json_pointer(document: dict[str, object], pointer: str, value: object) -> None:
    tokens = pointer[1:].split("/")
    if not tokens or any(token == "" for token in tokens):
        raise FaultApplicationError("stale_field must target an existing named response field")
    decoded = [token.replace("~1", "/").replace("~0", "~") for token in tokens]
    current: object = document
    for token in decoded[:-1]:
        if not isinstance(current, dict) or token not in current:
            raise FaultApplicationError("stale_field target does not exist")
        current = current[token]
    leaf = decoded[-1]
    if not isinstance(current, dict) or leaf not in current:
        raise FaultApplicationError("stale_field target does not exist")
    current[leaf] = value


def _validate_json(value: object) -> None:
    if value is None or isinstance(value, str | bool):
        return
    if type(value) is int:
        if not -9_007_199_254_740_991 <= value <= 9_007_199_254_740_991:
            raise FaultApplicationError("fault JSON integer is outside the safe range")
        return
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise FaultApplicationError("fault JSON number must be finite")
        return
    if isinstance(value, list | tuple):
        for item in value:
            _validate_json(item)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise FaultApplicationError("fault JSON object keys must be strings")
            _validate_json(item)
        return
    raise FaultApplicationError("fault parameter is not JSON representable")


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_json(item) for item in value)
    return value
