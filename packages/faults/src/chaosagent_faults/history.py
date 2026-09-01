"""Authoritative authentication of committed Issue #14 fault evidence history."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast

import rfc8785

from .application import FaultEngine
from .matcher import CompiledFaultRule


class FaultHistoryValidationError(ValueError):
    """Committed fault evidence is incomplete, contradictory, or fabricated."""


@dataclass(frozen=True, slots=True)
class AuthenticatedFaultHistory:
    counts: Mapping[str, int]
    observed_event_ids: Mapping[str, tuple[str, ...]]


def _payload(event: Mapping[str, object]) -> Mapping[str, object]:
    value = event.get("payload")
    if not isinstance(value, Mapping):
        raise FaultHistoryValidationError("fault evidence payload is malformed")
    return cast(Mapping[str, object], value)


def _same_json(left: object, right: object) -> bool:
    try:
        return rfc8785.dumps(left) == rfc8785.dumps(right)  # type: ignore[arg-type]
    except (rfc8785.CanonicalizationError, TypeError) as error:
        raise FaultHistoryValidationError("fault request arguments are malformed") from error


def authenticate_fault_history_v0(
    documents: Sequence[Mapping[str, object]],
    engine: FaultEngine,
    *,
    run_id: str,
    scenario_digest: str,
    producer_component: str | None = None,
    request_arguments: Mapping[str, Mapping[str, object]] | None = None,
    request_ordinals: Mapping[str, int] | None = None,
    ignore_request_event_id: str | None = None,
) -> AuthenticatedFaultHistory:
    """Authenticate complete matched/applied/result/observed chains once."""
    if engine.scenario_digest != scenario_digest:
        raise FaultHistoryValidationError("fault history has the wrong Scenario digest")
    rules = {rule.fault_id: rule for rule in engine.rules}
    counts = {fault_id: 0 for fault_id in rules}
    observed_ids: dict[str, list[str]] = {fault_id: [] for fault_id in rules}
    filtered: list[Mapping[str, object]] = []
    for document in documents:
        payload = _payload(document)
        related = payload.get("related_event_ids")
        result_request = payload.get("request_event_id")
        if ignore_request_event_id is not None and (
            document.get("event_id") == ignore_request_event_id
            or (isinstance(related, list) and ignore_request_event_id in related)
            or result_request == ignore_request_event_id
        ):
            continue
        filtered.append(document)
    by_id = {cast(str, item["event_id"]): item for item in filtered}
    if len(by_id) != len(filtered) or any(item.get("run_id") != run_id for item in filtered):
        raise FaultHistoryValidationError("fault history Run/event identity is incoherent")

    matched: dict[str, tuple[Mapping[str, object], Mapping[str, object], CompiledFaultRule]] = {}
    for document in filtered:
        if document.get("event_type") != "fault.matched":
            continue
        payload = _payload(document)
        fault_id = cast(str, payload["fault_id"])
        activation_id = cast(str, payload["activation_id"])
        related = cast(list[str], payload["related_event_ids"])
        rule = rules.get(fault_id)
        request = by_id.get(related[0]) if len(related) == 1 else None
        if (
            rule is None
            or activation_id in matched
            or request is None
            or request.get("event_type") != "tool.requested"
            or document.get("causation_event_id") != request.get("event_id")
            or document.get("correlation_id") != request.get("correlation_id")
            or document.get("producer") != request.get("producer")
        ):
            raise FaultHistoryValidationError("fault.matched history is incoherent")
        producer = document.get("producer")
        if producer_component is not None and (
            not isinstance(producer, Mapping) or producer.get("component") != producer_component
        ):
            raise FaultHistoryValidationError("fault history producer is not authoritative")
        request_payload = _payload(request)
        logical_id = cast(str, request_payload["logical_call_id"])
        request_id = cast(str, request["event_id"])
        ordinal = None if request_ordinals is None else request_ordinals.get(request_id)
        arguments = (
            None
            if request_arguments is None
            else request_arguments.get(cast(str, request["event_id"]))
        )
        if arguments is not None and any(
            name not in arguments or not _same_json(arguments[name], expected)
            for name, expected in rule.argument_equals.items()
        ):
            raise FaultHistoryValidationError("fault request arguments do not match the rule")
        if (
            (request_ordinals is not None and ordinal is None)
            or request_payload.get("tool_id") != rule.tool_id
            or (
                ordinal is not None
                and rule.call_ordinal is not None
                and ordinal != rule.call_ordinal
            )
            or request.get("correlation_id") != logical_id
            or not engine.authenticates_activation(
                rule,
                run_id=run_id,
                logical_call_id=logical_id,
                physical_attempt_id=cast(str, request_payload["attempt_id"]),
                attempt_number=cast(int, request_payload["attempt_number"]),
                call_ordinal=ordinal,
                arguments_digest=cast(str, request_payload["arguments_digest"]),
                activation_id=activation_id,
            )
            or cast(int, request["sequence"]) >= cast(int, document["sequence"])
        ):
            raise FaultHistoryValidationError("fault.matched request binding is incoherent")
        matched[activation_id] = (document, request, rule)

    applied: set[str] = set()
    for document in filtered:
        if document.get("event_type") != "fault.applied":
            continue
        payload = _payload(document)
        fault_id = cast(str, payload["fault_id"])
        activation_id = cast(str, payload["activation_id"])
        related = cast(list[str], payload["related_event_ids"])
        match_entry = matched.get(activation_id)
        if match_entry is None:
            raise FaultHistoryValidationError("fault.applied has no authenticated match")
        matched_event, request, rule = match_entry
        if (
            activation_id in applied
            or fault_id != rule.fault_id
            or set(related) != {request["event_id"], matched_event["event_id"]}
            or document.get("causation_event_id") != matched_event.get("event_id")
            or document.get("correlation_id") != matched_event.get("correlation_id")
            or document.get("producer") != matched_event.get("producer")
            or cast(int, matched_event["sequence"]) >= cast(int, document["sequence"])
        ):
            raise FaultHistoryValidationError("fault.applied history is incoherent")
        result_candidates = [
            item
            for item in filtered
            if item.get("event_type") == "tool.result"
            and _payload(item).get("request_event_id") == request["event_id"]
        ]
        observed_candidates = [
            item
            for item in filtered
            if item.get("event_type") == "fault.observed"
            and _payload(item).get("activation_id") == activation_id
        ]
        if len(result_candidates) != 1 or len(observed_candidates) != 1:
            raise FaultHistoryValidationError("fault application has no unique observed result")
        result = result_candidates[0]
        observed = observed_candidates[0]
        result_payload = _payload(result)
        observed_payload = _payload(observed)
        request_payload = _payload(request)
        request_applications = [
            item
            for item in filtered
            if item.get("event_type") == "fault.applied"
            and request["event_id"] in cast(list[str], _payload(item)["related_event_ids"])
        ]
        last_application = max(request_applications, key=lambda item: cast(int, item["sequence"]))
        if (
            result_payload.get("logical_call_id") != request_payload.get("logical_call_id")
            or result_payload.get("attempt_id") != request_payload.get("attempt_id")
            or result_payload.get("attempt_number") != request_payload.get("attempt_number")
            or result_payload.get("tool_id") != request_payload.get("tool_id")
            or observed_payload.get("fault_id") != fault_id
            or set(cast(list[str], observed_payload["related_event_ids"]))
            != {request["event_id"], document["event_id"], result["event_id"]}
            or observed.get("causation_event_id") != result.get("event_id")
            or observed.get("correlation_id") != request.get("correlation_id")
            or observed.get("producer") != request.get("producer")
            or result.get("causation_event_id") != last_application.get("event_id")
            or result.get("correlation_id") != request.get("correlation_id")
            or result.get("producer") != request.get("producer")
            or not (
                cast(int, document["sequence"])
                < cast(int, result["sequence"])
                < cast(int, observed["sequence"])
            )
        ):
            raise FaultHistoryValidationError("fault observation history is incoherent")
        applied.add(activation_id)
        counts[fault_id] += 1
        if counts[fault_id] > rule.max_occurrences:
            raise FaultHistoryValidationError("fault occurrence cap is exceeded")
        observed_ids[fault_id].append(cast(str, observed["event_id"]))

    if set(matched) != applied:
        raise FaultHistoryValidationError("fault history contains an orphan match")
    for document in filtered:
        if (
            document.get("event_type") == "fault.observed"
            and cast(str, _payload(document)["activation_id"]) not in applied
        ):
            raise FaultHistoryValidationError("fault.observed has no authoritative application")
    return AuthenticatedFaultHistory(
        MappingProxyType(counts),
        MappingProxyType({key: tuple(value) for key, value in observed_ids.items()}),
    )
