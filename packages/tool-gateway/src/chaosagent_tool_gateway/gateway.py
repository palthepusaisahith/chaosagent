"""Strict transactional tool boundary for the synthetic company."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from time import monotonic_ns
from types import MappingProxyType
from typing import Literal, Protocol, cast
from uuid import uuid4

from chaosagent_evidence import (
    EvidenceValidationError,
    RunEvent,
    digest_payload_v0,
    loads_run_event,
    validate_run_event_stream_v0,
)
from chaosagent_faults import (
    AppliedFault,
    FaultApplicationError,
    FaultEngine,
    FaultRuleValidationError,
    FaultSelection,
)
from chaosagent_persistence import (
    BusinessRuleViolationError,
    CompanyEffect,
    CompanyOrder,
    CompanyShipment,
    IdempotencyConflictError,
    LeaseExpiredError,
    LeaseIdentity,
    PersistenceError,
    PersistenceIntegrityError,
    PersistenceRepository,
    ReferenceNotFoundError,
    RevisionReference,
    StaleLeaseError,
    approval_identity,
)
from chaosagent_policies import PolicyValidationError, evaluate_policy_v0
from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type ToolErrorCode = Literal[
    "invalid_request",
    "unsupported_tool",
    "tool_not_allowed",
    "run_not_ready",
    "stale_lease",
    "entity_not_found",
    "business_rule_violation",
    "idempotency_conflict",
    "policy_denied",
    "approval_required",
    "approval_not_found",
    "approval_pending",
    "approval_denied",
    "approval_mismatch",
    "policy_integrity_error",
    "infrastructure_error",
    "fault_timeout",
    "fault_http_429",
    "fault_http_503",
    "fault_auth_401",
    "fault_auth_403",
    "fault_malformed_response",
]

TOOL_EVENT_SCHEMA_VERSION = "chaosagent.run-event/v0"
SCENARIO_V0_SCHEMA_VERSION = "chaosagent.scenario/v0"
ORDERS_GET_V0 = "chaosagent.tool/orders.get/v0"
SHIPPING_GET_STATUS_V0 = "chaosagent.tool/shipping.get_status/v0"
PAYMENTS_REFUND_V0 = "chaosagent.tool/payments.refund/v0"
SUPPORT_UPDATE_TICKET_V0 = "chaosagent.tool/support.update_ticket/v0"
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_CONTRACT_VERSION_RE = re.compile(r"^chaosagent\.tool/[a-z0-9._-]+/v[0-9]+$")
_ACTIVATION_ID_RE = re.compile(r"^activation-[0-9a-f]{64}$")
SCENARIO_V0_TOOL_VERSIONS: Mapping[str, str] = MappingProxyType(
    {
        "orders.get": ORDERS_GET_V0,
        "shipping.get_status": SHIPPING_GET_STATUS_V0,
        "payments.refund": PAYMENTS_REFUND_V0,
        "support.update_ticket": SUPPORT_UPDATE_TICKET_V0,
    }
)


class _LeaseLostDuringExecution(RuntimeError):
    pass


class _PolicyGateBlocked(RuntimeError):
    pass


@contextmanager
def _execute_when_authorized(error: ToolError | None) -> Iterator[None]:
    if error is not None:
        raise _PolicyGateBlocked
    yield


@dataclass(frozen=True, slots=True)
class ToolError:
    code: ToolErrorCode
    message: str


@dataclass(frozen=True, slots=True)
class ToolExecutionResult:
    """Provider-neutral result with recursively immutable JSON output."""

    tool_id: str
    contract_version: str
    outcome: Literal["succeeded", "failed", "denied"]
    output: Mapping[str, object] | None
    error: ToolError | None
    request_event_id: str | None
    result_event_id: str | None
    state_evidence_event_id: str | None
    policy_decision_event_id: str | None = None
    approval_id: str | None = None


class ReadOnlyCompanyState(Protocol):
    """Run-bound capability exposing only Issue #8 synthetic-company reads."""

    def get_order(self, order_id: str) -> CompanyOrder | None: ...

    def get_shipment_for_order(self, order_id: str) -> CompanyShipment | None: ...


class _RunBoundCompanyState:
    __slots__ = ("__repository", "__run_id")

    def __init__(self, repository: PersistenceRepository, run_id: str) -> None:
        self.__repository = repository
        self.__run_id = run_id

    def get_order(self, order_id: str) -> CompanyOrder | None:
        return self.__repository.get_company_order(self.__run_id, order_id)

    def get_shipment_for_order(self, order_id: str) -> CompanyShipment | None:
        return self.__repository.get_company_shipment_for_order(self.__run_id, order_id)


@dataclass(frozen=True, slots=True)
class RefundMutationIntent:
    """Pure validated intent; it carries no persistence capability."""

    order_id: str
    payment_id: str
    amount_minor: int
    reason: str


@dataclass(frozen=True, slots=True)
class SupportTicketMutationIntent:
    """Pure validated intent; it carries no persistence capability."""

    ticket_id: str
    status: str
    note: str


type MutationIntent = RefundMutationIntent | SupportTicketMutationIntent


type ReadToolHandler = Callable[
    [ReadOnlyCompanyState, Mapping[str, object]], Mapping[str, object] | None
]
type MutationToolHandler = Callable[[Mapping[str, object]], MutationIntent]
type ToolHandler = ReadToolHandler | MutationToolHandler


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    tool_id: str
    contract_version: str
    description: str
    capability: Literal["read", "mutation"]
    read_only: bool
    input_schema: Mapping[str, object]
    output_schema: Mapping[str, object]
    handler: ToolHandler


class ToolRegistry:
    """Immutable explicit registry keyed by exact tool ID and contract version."""

    def __init__(self, definitions: tuple[ToolDefinition, ...]) -> None:
        entries: dict[tuple[str, str], ToolDefinition] = {}
        for definition in definitions:
            if (
                not isinstance(definition.tool_id, str)
                or len(definition.tool_id) > 128
                or _NAME_RE.fullmatch(definition.tool_id) is None
            ):
                raise ValueError(f"invalid tool definition ID {definition.tool_id!r}")
            if (
                not isinstance(definition.contract_version, str)
                or len(definition.contract_version) > 256
                or _CONTRACT_VERSION_RE.fullmatch(definition.contract_version) is None
            ):
                raise ValueError(f"invalid tool contract version {definition.contract_version!r}")
            if not isinstance(definition.description, str) or not definition.description.strip():
                raise ValueError("tool definition description must not be empty")
            if definition.capability not in {"read", "mutation"}:
                raise ValueError("tool capability must be read or mutation")
            if (definition.capability == "read") != (definition.read_only is True):
                raise ValueError("tool capability and read_only metadata disagree")
            if not callable(definition.handler):
                raise ValueError("tool definition handler must be callable")
            frozen_definition = replace(
                definition,
                input_schema=cast(Mapping[str, object], _freeze_json(definition.input_schema)),
                output_schema=cast(Mapping[str, object], _freeze_json(definition.output_schema)),
            )
            Draft202012Validator.check_schema(_schema_dict(frozen_definition.input_schema))
            Draft202012Validator.check_schema(_schema_dict(frozen_definition.output_schema))
            key = (definition.tool_id, definition.contract_version)
            if key in entries:
                raise ValueError(f"duplicate tool definition {key!r}")
            entries[key] = frozen_definition
        self._entries = MappingProxyType(entries)

    @property
    def definitions(self) -> tuple[ToolDefinition, ...]:
        return tuple(self._entries[key] for key in sorted(self._entries))

    def resolve(self, tool_id: str, contract_version: str) -> ToolDefinition | None:
        return self._entries.get((tool_id, contract_version))

    def has_tool_id(self, tool_id: str) -> bool:
        return any(registered_id == tool_id for registered_id, _ in self._entries)


def _schema_resource(name: str) -> Mapping[str, object]:
    return cast(Mapping[str, object], _freeze_json(_schema_resource_cached(name)))


@lru_cache(maxsize=8)
def _schema_resource_cached(name: str) -> dict[str, object]:
    parsed = cast(
        object,
        json.loads(files("chaosagent_tool_gateway.schema").joinpath(name).read_text("utf-8")),
    )
    if not isinstance(parsed, dict):
        raise RuntimeError(f"bundled tool schema {name!r} is not an object")
    schema = cast(dict[str, object], parsed)
    Draft202012Validator.check_schema(schema)
    return schema


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _orders_get(
    company: ReadOnlyCompanyState, arguments: Mapping[str, object]
) -> dict[str, object] | None:
    order_id = cast(str, arguments["order_id"])
    order = company.get_order(order_id)
    if order is None:
        return None
    return {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "status": order.status,
        "total_minor": order.total_minor,
        "currency": order.currency,
        "placed_at": _timestamp(order.placed_at),
    }


def _shipping_get_status(
    company: ReadOnlyCompanyState, arguments: Mapping[str, object]
) -> dict[str, object] | None:
    # The approved architecture specifies lookup by order_id, not shipment_id.
    order_id = cast(str, arguments["order_id"])
    shipment = company.get_shipment_for_order(order_id)
    if shipment is None:
        return None
    return {
        "shipment_id": shipment.shipment_id,
        "order_id": shipment.order_id,
        "status": shipment.status,
        "carrier": shipment.carrier,
        "tracking_number": shipment.tracking_number,
        "updated_at": _timestamp(shipment.updated_at),
    }


def _payments_refund(arguments: Mapping[str, object]) -> RefundMutationIntent:
    return RefundMutationIntent(
        order_id=cast(str, arguments["order_id"]),
        payment_id=cast(str, arguments["payment_id"]),
        amount_minor=cast(int, arguments["amount_minor"]),
        reason=cast(str, arguments["reason"]),
    )


def _support_update_ticket(arguments: Mapping[str, object]) -> SupportTicketMutationIntent:
    return SupportTicketMutationIntent(
        ticket_id=cast(str, arguments["ticket_id"]),
        status=cast(str, arguments["status"]),
        note=cast(str, arguments["note"]),
    )


def default_tool_registry() -> ToolRegistry:
    """Return the frozen Scenario v0 tool catalog; no dynamic discovery occurs."""
    return ToolRegistry(
        (
            ToolDefinition(
                tool_id="orders.get",
                contract_version=ORDERS_GET_V0,
                description="Read one synthetic-company order by order ID.",
                capability="read",
                read_only=True,
                input_schema=_schema_resource("orders-get-v0.input.schema.json"),
                output_schema=_schema_resource("orders-get-v0.output.schema.json"),
                handler=_orders_get,
            ),
            ToolDefinition(
                tool_id="shipping.get_status",
                contract_version=SHIPPING_GET_STATUS_V0,
                description="Read synthetic shipment status by order ID.",
                capability="read",
                read_only=True,
                input_schema=_schema_resource("shipping-get-status-v0.input.schema.json"),
                output_schema=_schema_resource("shipping-get-status-v0.output.schema.json"),
                handler=_shipping_get_status,
            ),
            ToolDefinition(
                tool_id="payments.refund",
                contract_version=PAYMENTS_REFUND_V0,
                description="Create or replay one idempotent synthetic payment refund.",
                capability="mutation",
                read_only=False,
                input_schema=_schema_resource("payments-refund-v0.input.schema.json"),
                output_schema=_schema_resource("payments-refund-v0.output.schema.json"),
                handler=_payments_refund,
            ),
            ToolDefinition(
                tool_id="support.update_ticket",
                contract_version=SUPPORT_UPDATE_TICKET_V0,
                description="Apply or replay one idempotent synthetic support-ticket update.",
                capability="mutation",
                read_only=False,
                input_schema=_schema_resource("support-update-ticket-v0.input.schema.json"),
                output_schema=_schema_resource("support-update-ticket-v0.output.schema.json"),
                handler=_support_update_ticket,
            ),
        )
    )


class ToolGateway:
    """Authorize one tool attempt and atomically persist its evidence/effect."""

    def __init__(
        self,
        session: Session,
        *,
        registry: ToolRegistry | None = None,
        producer_component: str = "tool-gateway",
        producer_instance_id: str | None = None,
        fault_engine: FaultEngine | None = None,
    ) -> None:
        self._session = session
        self._repository = PersistenceRepository(session)
        self._registry = registry or default_tool_registry()
        _require_producer_component(producer_component)
        _require_optional_id(producer_instance_id, "producer_instance_id")
        self._producer_component = producer_component
        self._producer_instance_id = producer_instance_id
        self._fault_engine = fault_engine

    def execute(
        self,
        lease: object,
        *,
        tool_id: object,
        contract_version: object,
        arguments: object,
        logical_call_id: object,
        attempt_id: object,
        attempt_number: object = 1,
        call_ordinal: object | None = None,
        step_id: object | None = None,
        causation_event_id: object | None = None,
        approval_id: object | None = None,
    ) -> ToolExecutionResult:
        field_error = _validate_call_fields(
            lease,
            tool_id,
            contract_version,
            arguments,
            logical_call_id,
            attempt_id,
            attempt_number,
            call_ordinal,
            step_id,
            causation_event_id,
            approval_id,
        )
        result_tool_id = tool_id if isinstance(tool_id, str) else "<invalid>"
        result_version = contract_version if isinstance(contract_version, str) else "<invalid>"
        if field_error is not None:
            return self._rejected(
                result_tool_id,
                result_version,
                "invalid_request",
                f"invalid tool request: {field_error}",
            )
        assert isinstance(lease, LeaseIdentity)
        assert isinstance(tool_id, str)
        assert isinstance(contract_version, str)
        assert isinstance(logical_call_id, str)
        assert isinstance(attempt_id, str)
        assert isinstance(attempt_number, int) and not isinstance(attempt_number, bool)
        assert call_ordinal is None or (
            isinstance(call_ordinal, int) and not isinstance(call_ordinal, bool)
        )
        assert step_id is None or isinstance(step_id, str)
        assert causation_event_id is None or isinstance(causation_event_id, str)
        assert approval_id is None or isinstance(approval_id, str)
        if self._fault_engine is not None and call_ordinal is None:
            return self._rejected(
                tool_id,
                contract_version,
                "invalid_request",
                "fault-aware execution requires an explicit logical call ordinal",
            )
        effective_call_ordinal = 1 if call_ordinal is None else call_ordinal
        definition = self._registry.resolve(tool_id, contract_version)
        if definition is None:
            message = (
                "unsupported contract version for known tool"
                if self._registry.has_tool_id(tool_id)
                else "unsupported tool"
            )
            return self._rejected(tool_id, contract_version, "unsupported_tool", message)
        request_error = _validate_instance(arguments, definition.input_schema)
        if request_error is not None:
            return self._rejected(
                tool_id,
                contract_version,
                "invalid_request",
                f"invalid tool request: {request_error}",
            )
        if (
            tool_id == "payments.refund"
            and type(cast(dict[str, object], arguments).get("amount_minor")) is not int
        ):
            return self._rejected(
                tool_id,
                contract_version,
                "invalid_request",
                "invalid tool request: amount_minor must be an exact JSON integer",
            )
        arguments_snapshot = cast(dict[str, object], deepcopy(arguments))

        try:
            with self._session.begin_nested():
                try:
                    run = self._repository.lock_current_lease(lease)
                except (StaleLeaseError, LeaseExpiredError):
                    return self._rejected(
                        tool_id,
                        contract_version,
                        "stale_lease",
                        "caller does not hold the current unexpired Run lease",
                    )
                if run.status != "running":
                    return self._rejected(
                        tool_id,
                        contract_version,
                        "run_not_ready",
                        "Run is not in the running lifecycle state",
                    )
                scenario_record = self._repository.get_scenario_revision(
                    run.scenario.id, run.scenario.revision
                )
                if (
                    scenario_record is None
                    or scenario_record.scenario.digest != run.scenario.digest
                ):
                    raise ReferenceNotFoundError("Run Scenario binding does not resolve")
                if self._fault_engine is not None and (
                    self._fault_engine.scenario_id != run.scenario.id
                    or self._fault_engine.scenario_revision != run.scenario.revision
                    or self._fault_engine.scenario_digest != run.scenario.digest
                ):
                    raise PersistenceIntegrityError(
                        "fault engine does not match the authoritative Run Scenario"
                    )
                scenario = scenario_record.scenario.to_dict()
                agent = cast(dict[str, object], scenario["agent"])
                if tool_id not in cast(list[str], agent["allowed_tools"]):
                    return self._rejected(
                        tool_id,
                        contract_version,
                        "tool_not_allowed",
                        "tool is not permitted by the frozen Scenario",
                    )
                scenario_version = cast(str, scenario["schema_version"])
                expected_contract_version = (
                    SCENARIO_V0_TOOL_VERSIONS.get(tool_id)
                    if scenario_version == SCENARIO_V0_SCHEMA_VERSION
                    else None
                )
                if expected_contract_version != contract_version:
                    return self._rejected(
                        tool_id,
                        contract_version,
                        "unsupported_tool",
                        "tool contract version is not defined by the frozen Scenario version",
                    )
                if not self._repository.has_run_company_state(run.run_id):
                    return self._rejected(
                        tool_id,
                        contract_version,
                        "run_not_ready",
                        "Run-local synthetic company state is not initialized",
                    )
                policy_reference = cast(dict[str, object], scenario["policy"])
                policy_record = self._repository.get_policy_revision(
                    cast(str, policy_reference["id"]),
                    cast(str, policy_reference["revision"]),
                )
                if (
                    policy_record is None
                    or policy_record.policy.digest != policy_reference["digest"]
                ):
                    return self._rejected(
                        tool_id,
                        contract_version,
                        "policy_integrity_error",
                        "the frozen Scenario policy reference does not resolve",
                    )
                frozen_policy = RevisionReference(
                    cast(str, policy_reference["id"]),
                    cast(str, policy_reference["revision"]),
                    cast(str, policy_reference["digest"]),
                )

                request_event_id = _event_id()
                request_digest = digest_payload_v0(
                    {
                        "tool_id": tool_id,
                        "contract_version": contract_version,
                        "arguments": arguments_snapshot,
                    }
                )
                idempotency_key_digest: str | None = None
                if definition.capability == "mutation":
                    idempotency_key_digest = digest_payload_v0(
                        cast(str, arguments_snapshot["idempotency_key"])
                    )
                arguments_digest = digest_payload_v0(arguments_snapshot)
                request_payload: dict[str, object] = {
                    "logical_call_id": logical_call_id,
                    "attempt_id": attempt_id,
                    "attempt_number": attempt_number,
                    "tool_id": tool_id,
                    "arguments_digest": arguments_digest,
                }
                if step_id is not None:
                    request_payload["step_id"] = step_id
                if idempotency_key_digest is not None:
                    request_payload["idempotency_key_digest"] = idempotency_key_digest
                self._append_event(
                    run.run_id,
                    request_event_id,
                    "tool.requested",
                    request_payload,
                    correlation_id=logical_call_id,
                    causation_event_id=causation_event_id,
                )

                payment_currency: str | None = None
                if tool_id == "payments.refund":
                    payment = self._repository.get_company_payment(
                        run.run_id, cast(str, arguments_snapshot["payment_id"])
                    )
                    payment_currency = None if payment is None else payment.currency
                decision = evaluate_policy_v0(
                    policy_record.policy,
                    tool_id=tool_id,
                    contract_version=contract_version,
                    arguments=arguments_snapshot,
                    payment_currency=payment_currency,
                )
                decision_id = f"decision-{uuid4().hex}"
                policy_decision_event_id = _event_id()
                self._append_event(
                    run.run_id,
                    policy_decision_event_id,
                    "policy.decision",
                    {
                        "decision_id": decision_id,
                        "policy": {
                            "id": frozen_policy.id,
                            "revision": frozen_policy.revision,
                            "digest": frozen_policy.digest,
                        },
                        "decision": decision.decision,
                        "reason_code": decision.reason_code,
                        "logical_call_id": logical_call_id,
                    },
                    correlation_id=logical_call_id,
                    causation_event_id=request_event_id,
                )

                started = monotonic_ns()
                output: dict[str, object] | None = None
                tool_error: ToolError | None = None
                effect: CompanyEffect | None = None
                applied_faults: list[tuple[AppliedFault, str]] = []
                effective_approval_id: str | None = None
                result_causation_event_id = policy_decision_event_id
                if decision.decision == "deny":
                    tool_error = ToolError(
                        "policy_denied", "the frozen Policy denied this tool request"
                    )
                elif decision.decision == "require_approval":
                    assert idempotency_key_digest is not None
                    effective_approval_id = approval_identity(
                        run_id=run.run_id,
                        scenario_id=run.scenario.id,
                        scenario_revision=run.scenario.revision,
                        scenario_digest=run.scenario.digest,
                        policy_id=frozen_policy.id,
                        policy_revision=frozen_policy.revision,
                        policy_digest=frozen_policy.digest,
                        tool_id=tool_id,
                        contract_version=contract_version,
                        request_digest=request_digest,
                        idempotency_key_digest=idempotency_key_digest,
                    )
                    if approval_id is not None and approval_id != effective_approval_id:
                        tool_error = ToolError(
                            "approval_mismatch",
                            "approval does not authorize this exact frozen request",
                        )
                    else:
                        approval = self._repository.get_approval_request_for_authorization(
                            effective_approval_id,
                            run=run,
                            policy=frozen_policy,
                            tool_id=tool_id,
                            contract_version=contract_version,
                            request_digest=request_digest,
                            idempotency_key_digest=idempotency_key_digest,
                            arguments=arguments_snapshot,
                        )
                        if approval is None and approval_id is not None:
                            tool_error = ToolError(
                                "approval_not_found", "approval request was not found"
                            )
                        elif approval is None:
                            approval = self._repository.create_approval_request(
                                run=run,
                                policy=frozen_policy,
                                tool_id=tool_id,
                                contract_version=contract_version,
                                request_digest=request_digest,
                                idempotency_key_digest=idempotency_key_digest,
                                arguments=arguments_snapshot,
                                logical_call_id=logical_call_id,
                                requested_attempt_id=attempt_id,
                                lease_attempt=lease.attempt,
                                decision_id=decision_id,
                                decision_event_id=policy_decision_event_id,
                                request_event_id=_event_id(),
                                producer_component=self._producer_component,
                                producer_instance_id=self._producer_instance_id,
                            )
                            result_causation_event_id = approval.request_event_id
                            tool_error = ToolError(
                                "approval_required",
                                "human approval is required before execution",
                            )
                        elif approval.status == "pending":
                            result_causation_event_id = approval.request_event_id
                            tool_error = ToolError(
                                "approval_pending", "approval request is still pending"
                            )
                        elif approval.status == "denied":
                            assert approval.resolution_event_id is not None
                            result_causation_event_id = approval.resolution_event_id
                            tool_error = ToolError("approval_denied", "approval request was denied")
                        else:
                            assert approval.resolution_event_id is not None
                            result_causation_event_id = approval.resolution_event_id
                if tool_error is None:
                    before = self._select_faults(
                        run_id=run.run_id,
                        request_event_id=request_event_id,
                        scenario_digest=run.scenario.digest,
                        tool_id=tool_id,
                        phase="before_tool",
                        logical_call_id=logical_call_id,
                        attempt_id=attempt_id,
                        attempt_number=attempt_number,
                        call_ordinal=effective_call_ordinal,
                        arguments=arguments_snapshot,
                        arguments_digest=arguments_digest,
                    )
                    matched_event_ids = self._record_fault_selection(
                        run.run_id, request_event_id, logical_call_id, before
                    )
                    if before is None:
                        before_result = None
                    else:
                        assert self._fault_engine is not None
                        before_result = self._fault_engine.apply_before(before)
                    if before_result is not None:
                        applied_faults.extend(
                            self._record_applied_faults(
                                run.run_id,
                                request_event_id,
                                logical_call_id,
                                before_result.applied,
                                matched_event_ids,
                            )
                        )
                        if before_result.failure_code is not None:
                            tool_error = ToolError(
                                before_result.failure_code,
                                "tool execution was replaced by a declared fault",
                            )
                    try:
                        self._repository.lock_current_lease(lease)
                    except (StaleLeaseError, LeaseExpiredError) as error:
                        raise _LeaseLostDuringExecution from error
                try:
                    with self._session.begin_nested(), _execute_when_authorized(tool_error):
                        if definition.capability == "read":
                            read_handler = cast(ReadToolHandler, definition.handler)
                            read_output = read_handler(
                                _RunBoundCompanyState(self._repository, run.run_id),
                                arguments_snapshot,
                            )
                            output = None if read_output is None else dict(read_output)
                            if read_output is None:
                                tool_error = ToolError(
                                    "entity_not_found",
                                    "requested synthetic-company entity was not found",
                                )
                        else:
                            assert idempotency_key_digest is not None
                            mutation_handler = cast(MutationToolHandler, definition.handler)
                            intent = mutation_handler(arguments_snapshot)
                            effect = self._apply_mutation_intent(
                                intent,
                                run_id=run.run_id,
                                tool_id=tool_id,
                                contract_version=contract_version,
                                idempotency_key_digest=idempotency_key_digest,
                                request_digest=request_digest,
                                arguments=arguments_snapshot,
                                logical_call_id=logical_call_id,
                                attempt_id=attempt_id,
                                lease_attempt=lease.attempt,
                            )
                            effect = self._repository.verify_company_effect(
                                effect, expected_arguments=arguments_snapshot
                            )
                            output = dict(effect.result)
                        if output is not None:
                            output_error = _validate_instance(output, definition.output_schema)
                            if output_error is not None:
                                raise ValueError(
                                    f"handler output violated its contract: {output_error}"
                                )
                except _PolicyGateBlocked:
                    pass
                except IdempotencyConflictError:
                    output = None
                    effect = None
                    tool_error = ToolError(
                        "idempotency_conflict",
                        "idempotency key is already bound to a different request",
                    )
                except BusinessRuleViolationError:
                    output = None
                    effect = None
                    tool_error = ToolError(
                        "business_rule_violation",
                        "mutation was rejected by synthetic business rules",
                    )
                except ReferenceNotFoundError:
                    output = None
                    effect = None
                    tool_error = ToolError(
                        "entity_not_found",
                        "requested synthetic-company entity was not found",
                    )
                except Exception:
                    output = None
                    effect = None
                    tool_error = ToolError(
                        "infrastructure_error", "tool execution failed internally"
                    )
                if definition.capability == "read" and output is not None and tool_error is None:
                    try:
                        self._repository.lock_current_lease(lease)
                    except (StaleLeaseError, LeaseExpiredError) as error:
                        raise _LeaseLostDuringExecution from error
                    after = self._select_faults(
                        run_id=run.run_id,
                        request_event_id=request_event_id,
                        scenario_digest=run.scenario.digest,
                        tool_id=tool_id,
                        phase="after_tool",
                        logical_call_id=logical_call_id,
                        attempt_id=attempt_id,
                        attempt_number=attempt_number,
                        call_ordinal=effective_call_ordinal,
                        arguments=arguments_snapshot,
                        arguments_digest=arguments_digest,
                    )
                    after_matched_event_ids = self._record_fault_selection(
                        run.run_id, request_event_id, logical_call_id, after
                    )
                    if after is None:
                        after_result = None
                    else:
                        assert self._fault_engine is not None
                        after_result = self._fault_engine.apply_after(after, output)
                    if after_result is not None:
                        try:
                            self._repository.lock_current_lease(lease)
                        except (StaleLeaseError, LeaseExpiredError) as error:
                            raise _LeaseLostDuringExecution from error
                        applied_faults.extend(
                            self._record_applied_faults(
                                run.run_id,
                                request_event_id,
                                logical_call_id,
                                after_result.applied,
                                after_matched_event_ids,
                            )
                        )
                        output = (
                            None
                            if after_result.output is None
                            else cast(dict[str, object], _thaw_json(after_result.output))
                        )
                        if after_result.failure_code is not None:
                            tool_error = ToolError(
                                after_result.failure_code,
                                "tool observation was replaced by a declared fault",
                            )
                if effect is not None:
                    try:
                        self._repository.lock_current_lease(lease)
                    except (StaleLeaseError, LeaseExpiredError) as error:
                        raise _LeaseLostDuringExecution from error
                duration_ms = max(0, (monotonic_ns() - started) // 1_000_000)
                result_event_id = _event_id()
                result_payload: dict[str, object] = {
                    "logical_call_id": logical_call_id,
                    "request_event_id": request_event_id,
                    "attempt_id": attempt_id,
                    "attempt_number": attempt_number,
                    "tool_id": tool_id,
                    "outcome": (
                        "succeeded"
                        if tool_error is None
                        else "timed_out"
                        if tool_error.code == "fault_timeout"
                        else "failed"
                    ),
                    "duration_ms": duration_ms,
                }
                if output is not None:
                    result_payload["response_digest"] = digest_payload_v0(output)
                if tool_error is not None:
                    result_payload["error_code"] = tool_error.code
                if applied_faults:
                    result_causation_event_id = applied_faults[-1][1]
                self._append_event(
                    run.run_id,
                    result_event_id,
                    "tool.result",
                    result_payload,
                    correlation_id=logical_call_id,
                    causation_event_id=result_causation_event_id,
                )
                for applied, applied_event_id in applied_faults:
                    self._append_event(
                        run.run_id,
                        _event_id(),
                        "fault.observed",
                        {
                            "fault_id": applied.rule.fault_id,
                            "activation_id": applied.activation_id,
                            "related_event_ids": [
                                request_event_id,
                                applied_event_id,
                                result_event_id,
                            ],
                        },
                        correlation_id=logical_call_id,
                        causation_event_id=result_event_id,
                    )
                state_evidence_event_id: str | None = None
                if effect is not None and effect.newly_applied:
                    state_evidence_event_id = _event_id()
                    self._append_event(
                        run.run_id,
                        state_evidence_event_id,
                        "state.evidence_recorded",
                        {
                            "evidence_id": effect.effect_id,
                            "evidence_kind": "business_effect",
                            "fact_type": effect.effect_kind,
                            "subject": {
                                "type": effect.subject_type,
                                "id": effect.subject_id,
                            },
                            "related_event_ids": [request_event_id, result_event_id],
                        },
                        correlation_id=logical_call_id,
                        causation_event_id=result_event_id,
                    )
                return ToolExecutionResult(
                    tool_id=tool_id,
                    contract_version=contract_version,
                    outcome=self._result_outcome(tool_error),
                    output=None
                    if output is None
                    else cast(Mapping[str, object], _freeze_json(output)),
                    error=tool_error,
                    request_event_id=request_event_id,
                    result_event_id=result_event_id,
                    state_evidence_event_id=state_evidence_event_id,
                    policy_decision_event_id=policy_decision_event_id,
                    approval_id=effective_approval_id,
                )
        except _LeaseLostDuringExecution:
            return self._rejected(
                tool_id,
                contract_version,
                "stale_lease",
                "caller lost the Run lease before the mutation could commit",
            )
        except PolicyValidationError:
            return self._rejected(
                tool_id,
                contract_version,
                "policy_integrity_error",
                "the frozen Policy could not be evaluated",
            )
        except (
            EvidenceValidationError,
            FaultApplicationError,
            FaultRuleValidationError,
            PersistenceError,
            SQLAlchemyError,
        ):
            return self._rejected(
                tool_id,
                contract_version,
                "infrastructure_error",
                "tool evidence could not be persisted",
            )

    def _select_faults(
        self,
        *,
        run_id: str,
        request_event_id: str,
        scenario_digest: str,
        tool_id: str,
        phase: Literal["before_tool", "after_tool"],
        logical_call_id: str,
        attempt_id: str,
        attempt_number: int,
        call_ordinal: int,
        arguments: Mapping[str, object],
        arguments_digest: str,
    ) -> FaultSelection | None:
        if self._fault_engine is None:
            return None
        if self._fault_engine.scenario_digest != scenario_digest:
            raise FaultApplicationError("fault engine does not match the Run Scenario")
        history = self._authoritative_fault_history(
            run_id=run_id,
            scenario_digest=scenario_digest,
            current_request_event_id=request_event_id,
        )
        return self._fault_engine.select(
            run_id=run_id,
            scenario_digest=scenario_digest,
            tool_id=tool_id,
            phase=phase,
            logical_call_id=logical_call_id,
            physical_attempt_id=attempt_id,
            attempt_number=attempt_number,
            call_ordinal=call_ordinal,
            arguments=arguments,
            arguments_digest=arguments_digest,
            prior_applied_occurrences=history,
        )

    def _authoritative_fault_history(
        self,
        *,
        run_id: str,
        scenario_digest: str,
        current_request_event_id: str,
    ) -> dict[str, int]:
        """Authenticate complete committed fault chains before counting them."""
        assert self._fault_engine is not None
        if self._fault_engine.scenario_digest != scenario_digest:
            raise PersistenceIntegrityError("fault history has the wrong Scenario digest")
        records = self._repository.fetch_events(run_id)
        events = tuple(record.event for record in records)
        documents = [event.to_dict() for event in events]
        if events:
            try:
                validate_run_event_stream_v0(documents, complete=True)
            except EvidenceValidationError as error:
                raise PersistenceIntegrityError("Run Event history is incoherent") from error
        by_id = {cast(str, item["event_id"]): item for item in documents}
        rules = {rule.fault_id: rule for rule in self._fault_engine.rules}
        history = {fault_id: 0 for fault_id in rules}
        matched_activations: dict[str, str] = {}
        applied_activations: set[str] = set()

        for document in documents:
            if document["event_type"] != "fault.matched":
                continue
            payload = cast(dict[str, object], document["payload"])
            related = cast(list[str], payload["related_event_ids"])
            if current_request_event_id in related:
                continue
            fault_id = cast(str, payload["fault_id"])
            activation_id = cast(str, payload["activation_id"])
            rule = rules.get(fault_id)
            request = by_id.get(related[0]) if len(related) == 1 else None
            if (
                rule is None
                or rule.scenario_digest != scenario_digest
                or rule.phase == "after_commit"
                or _ACTIVATION_ID_RE.fullmatch(activation_id) is None
                or activation_id in matched_activations
                or request is None
                or request["event_type"] != "tool.requested"
                or document.get("causation_event_id") != request["event_id"]
                or document["correlation_id"] != request["correlation_id"]
                or document["producer"] != request["producer"]
                or cast(dict[str, object], document["producer"])["component"]
                != self._producer_component
            ):
                raise PersistenceIntegrityError("fault.matched history is incoherent")
            request_payload = cast(dict[str, object], request["payload"])
            if (
                request["run_id"] != run_id
                or request["correlation_id"] != request_payload["logical_call_id"]
                or request_payload["tool_id"] != rule.tool_id
                or not self._fault_engine.authenticates_activation(
                    rule,
                    run_id=run_id,
                    logical_call_id=cast(str, request_payload["logical_call_id"]),
                    physical_attempt_id=cast(str, request_payload["attempt_id"]),
                    attempt_number=cast(int, request_payload["attempt_number"]),
                    arguments_digest=cast(str, request_payload["arguments_digest"]),
                    activation_id=activation_id,
                )
                or cast(int, request["sequence"]) >= cast(int, document["sequence"])
            ):
                raise PersistenceIntegrityError("fault.matched request binding is incoherent")
            matched_activations[activation_id] = cast(str, document["event_id"])

        for document in documents:
            if document["event_type"] != "fault.applied":
                continue
            payload = cast(dict[str, object], document["payload"])
            related = cast(list[str], payload["related_event_ids"])
            if current_request_event_id in related:
                continue
            fault_id = cast(str, payload["fault_id"])
            activation_id = cast(str, payload["activation_id"])
            rule = rules.get(fault_id)
            matched_id = matched_activations.get(activation_id)
            matched = by_id.get(matched_id) if matched_id is not None else None
            request_ids = [
                item
                for item in related
                if (candidate := by_id.get(item)) is not None
                and candidate["event_type"] == "tool.requested"
            ]
            if (
                rule is None
                or matched is None
                or activation_id in applied_activations
                or matched_id not in related
                or len(request_ids) != 1
                or set(related) != {request_ids[0], matched_id}
                or document.get("causation_event_id") != matched_id
                or document["correlation_id"] != matched["correlation_id"]
                or document["producer"] != matched["producer"]
            ):
                raise PersistenceIntegrityError("fault.applied history is incoherent")
            matched_payload = cast(dict[str, object], matched["payload"])
            if (
                matched_payload["fault_id"] != fault_id
                or matched_payload["activation_id"] != activation_id
                or set(cast(list[str], matched_payload["related_event_ids"])) != {request_ids[0]}
                or cast(int, matched["sequence"]) >= cast(int, document["sequence"])
            ):
                raise PersistenceIntegrityError("fault application identity is incoherent")
            request = by_id[request_ids[0]]
            result_candidates = [
                item
                for item in documents
                if item["event_type"] == "tool.result"
                and cast(dict[str, object], item["payload"])["request_event_id"]
                == request["event_id"]
            ]
            observed_candidates = [
                item
                for item in documents
                if item["event_type"] == "fault.observed"
                and cast(dict[str, object], item["payload"])["activation_id"] == activation_id
            ]
            if len(result_candidates) != 1 or len(observed_candidates) != 1:
                raise PersistenceIntegrityError("fault application has no unique observed result")
            result = result_candidates[0]
            observed = observed_candidates[0]
            result_payload = cast(dict[str, object], result["payload"])
            observed_payload = cast(dict[str, object], observed["payload"])
            request_payload = cast(dict[str, object], request["payload"])
            request_applications = [
                item
                for item in documents
                if item["event_type"] == "fault.applied"
                and request_ids[0]
                in cast(list[str], cast(dict[str, object], item["payload"])["related_event_ids"])
            ]
            last_application = max(
                request_applications, key=lambda item: cast(int, item["sequence"])
            )
            if (
                result_payload["logical_call_id"] != request_payload["logical_call_id"]
                or result_payload["attempt_id"] != request_payload["attempt_id"]
                or result_payload["attempt_number"] != request_payload["attempt_number"]
                or result_payload["tool_id"] != request_payload["tool_id"]
                or observed_payload["fault_id"] != fault_id
                or set(cast(list[str], observed_payload["related_event_ids"]))
                != {
                    cast(str, request["event_id"]),
                    cast(str, document["event_id"]),
                    cast(str, result["event_id"]),
                }
                or observed.get("causation_event_id") != result["event_id"]
                or observed["correlation_id"] != request["correlation_id"]
                or observed["producer"] != request["producer"]
                or result.get("causation_event_id") != last_application["event_id"]
                or result["correlation_id"] != request["correlation_id"]
                or result["producer"] != request["producer"]
                or not (
                    cast(int, document["sequence"])
                    < cast(int, result["sequence"])
                    < cast(int, observed["sequence"])
                )
            ):
                raise PersistenceIntegrityError("fault observation history is incoherent")
            applied_activations.add(activation_id)
            history[fault_id] += 1

        for document in documents:
            if document["event_type"] != "fault.observed":
                continue
            payload = cast(dict[str, object], document["payload"])
            if current_request_event_id in cast(list[str], payload["related_event_ids"]):
                continue
            if cast(str, payload["activation_id"]) not in applied_activations:
                raise PersistenceIntegrityError("fault.observed has no authoritative application")
        return history

    def _record_fault_selection(
        self,
        run_id: str,
        request_event_id: str,
        logical_call_id: str,
        selection: FaultSelection | None,
    ) -> dict[str, str]:
        if selection is None:
            return {}
        for decision in selection.reportable_not_matched:
            self._append_event(
                run_id,
                _event_id(),
                "fault.not_matched",
                {"fault_id": decision.fault_id, "reason_code": decision.reason},
                correlation_id=logical_call_id,
                causation_event_id=request_event_id,
            )
        matched_event_ids: dict[str, str] = {}
        for rule in selection.matched_rules:
            activation_ids = [
                item.activation_id
                for item in selection.decisions
                if item.matched and item.fault_id == rule.fault_id
            ]
            if len(activation_ids) != 1 or activation_ids[0] is None:
                raise FaultApplicationError("matched fault has no unique activation identity")
            event_id = _event_id()
            self._append_event(
                run_id,
                event_id,
                "fault.matched",
                {
                    "fault_id": rule.fault_id,
                    "activation_id": activation_ids[0],
                    "related_event_ids": [request_event_id],
                },
                correlation_id=logical_call_id,
                causation_event_id=request_event_id,
            )
            matched_event_ids[rule.fault_id] = event_id
        return matched_event_ids

    def _record_applied_faults(
        self,
        run_id: str,
        request_event_id: str,
        logical_call_id: str,
        applied: tuple[AppliedFault, ...],
        matched_event_ids: Mapping[str, str],
    ) -> list[tuple[AppliedFault, str]]:
        records: list[tuple[AppliedFault, str]] = []
        for item in applied:
            matched_event_id = matched_event_ids.get(item.rule.fault_id)
            if matched_event_id is None:
                raise FaultApplicationError("applied fault has no matched evidence")
            event_id = _event_id()
            self._append_event(
                run_id,
                event_id,
                "fault.applied",
                {
                    "fault_id": item.rule.fault_id,
                    "activation_id": item.activation_id,
                    "related_event_ids": [request_event_id, matched_event_id],
                },
                correlation_id=logical_call_id,
                causation_event_id=matched_event_id,
            )
            records.append((item, event_id))
        return records

    def _apply_mutation_intent(
        self,
        intent: MutationIntent,
        *,
        run_id: str,
        tool_id: str,
        contract_version: str,
        idempotency_key_digest: str,
        request_digest: str,
        arguments: Mapping[str, object],
        logical_call_id: str,
        attempt_id: str,
        lease_attempt: int,
    ) -> CompanyEffect:
        """Translate a pure trusted handler intent into one repository mutation."""
        if tool_id == "payments.refund":
            if type(intent) is not RefundMutationIntent or (
                intent.order_id,
                intent.payment_id,
                intent.amount_minor,
                intent.reason,
            ) != (
                arguments["order_id"],
                arguments["payment_id"],
                arguments["amount_minor"],
                arguments["reason"],
            ):
                raise PersistenceIntegrityError("mutation handler returned an inconsistent intent")
            return self._repository.apply_refund_effect(
                run_id,
                order_id=intent.order_id,
                payment_id=intent.payment_id,
                amount_minor=intent.amount_minor,
                reason=intent.reason,
                contract_version=contract_version,
                idempotency_key_digest=idempotency_key_digest,
                request_digest=request_digest,
                logical_call_id=logical_call_id,
                attempt_id=attempt_id,
                lease_attempt=lease_attempt,
            )
        if tool_id == "support.update_ticket":
            if type(intent) is not SupportTicketMutationIntent or (
                intent.ticket_id,
                intent.status,
                intent.note,
            ) != (
                arguments["ticket_id"],
                arguments["status"],
                arguments["note"],
            ):
                raise PersistenceIntegrityError("mutation handler returned an inconsistent intent")
            return self._repository.apply_support_ticket_effect(
                run_id,
                ticket_id=intent.ticket_id,
                status=intent.status,
                note=intent.note,
                contract_version=contract_version,
                idempotency_key_digest=idempotency_key_digest,
                request_digest=request_digest,
                logical_call_id=logical_call_id,
                attempt_id=attempt_id,
                lease_attempt=lease_attempt,
            )
        raise PersistenceIntegrityError("mutation tool has no persistence implementation")

    def _append_event(
        self,
        run_id: str,
        event_id: str,
        event_type: Literal[
            "tool.requested",
            "tool.result",
            "state.evidence_recorded",
            "policy.decision",
            "fault.not_matched",
            "fault.matched",
            "fault.applied",
            "fault.observed",
        ],
        payload: dict[str, object],
        *,
        correlation_id: str,
        causation_event_id: str | None,
    ) -> None:
        observed = self._repository.database_time()
        producer: dict[str, object] = {"component": self._producer_component}
        if self._producer_instance_id is not None:
            producer["instance_id"] = self._producer_instance_id

        def event_factory(sequence: int) -> RunEvent:
            document: dict[str, object] = {
                "schema_version": TOOL_EVENT_SCHEMA_VERSION,
                "event_id": event_id,
                "run_id": run_id,
                "sequence": sequence,
                "occurred_at": _timestamp(observed),
                "recorded_at": _timestamp(observed),
                "event_type": event_type,
                "producer": producer,
                "correlation_id": correlation_id,
                "payload": payload,
                "payload_digest": digest_payload_v0(payload),
            }
            if causation_event_id is not None:
                document["causation_event_id"] = causation_event_id
            return loads_run_event(json.dumps(document))

        self._repository.append_event_allocated(run_id, event_factory)

    @staticmethod
    def _result_outcome(error: ToolError | None) -> Literal["succeeded", "failed", "denied"]:
        if error is None:
            return "succeeded"
        if error.code in {
            "policy_denied",
            "approval_required",
            "approval_not_found",
            "approval_pending",
            "approval_denied",
            "approval_mismatch",
        }:
            return "denied"
        return "failed"

    @staticmethod
    def _rejected(
        tool_id: str,
        contract_version: str,
        code: ToolErrorCode,
        message: str,
    ) -> ToolExecutionResult:
        return ToolExecutionResult(
            tool_id=tool_id,
            contract_version=contract_version,
            outcome=(
                "denied"
                if code
                in {
                    "tool_not_allowed",
                    "stale_lease",
                    "policy_denied",
                    "approval_required",
                    "approval_not_found",
                    "approval_pending",
                    "approval_denied",
                    "approval_mismatch",
                }
                else "failed"
            ),
            output=None,
            error=ToolError(code, message),
            request_event_id=None,
            result_event_id=None,
            state_evidence_event_id=None,
        )


def _validate_call_fields(
    lease: object,
    tool_id: object,
    contract_version: object,
    arguments: object,
    logical_call_id: object,
    attempt_id: object,
    attempt_number: object,
    call_ordinal: object,
    step_id: object | None,
    causation_event_id: object | None,
    approval_id: object | None,
) -> str | None:
    if not isinstance(lease, LeaseIdentity):
        return "lease credentials are malformed"
    if any(
        not isinstance(value, str) or _ID_RE.fullmatch(value) is None
        for value in (lease.run_id, lease.worker_id, lease.lease_token)
    ):
        return "lease credentials are malformed"
    if isinstance(lease.attempt, bool) or not isinstance(lease.attempt, int) or lease.attempt < 1:
        return "lease attempt generation is malformed"
    if not isinstance(tool_id, str) or len(tool_id) > 128 or _NAME_RE.fullmatch(tool_id) is None:
        return "tool_id is malformed"
    if (
        not isinstance(contract_version, str)
        or len(contract_version) > 256
        or _CONTRACT_VERSION_RE.fullmatch(contract_version) is None
    ):
        return "contract_version is malformed"
    if not isinstance(arguments, dict):
        return "arguments must be a JSON object"
    for name, value in (("logical_call_id", logical_call_id), ("attempt_id", attempt_id)):
        if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
            return f"{name} is malformed"
    for name, value in (
        ("step_id", step_id),
        ("causation_event_id", causation_event_id),
        ("approval_id", approval_id),
    ):
        if value is not None and (not isinstance(value, str) or _ID_RE.fullmatch(value) is None):
            return f"{name} is malformed"
    for name, value in (("attempt_number", attempt_number),):
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 1 <= value <= 9_007_199_254_740_991
        ):
            return f"{name} must be a positive JSON safe integer"
    if call_ordinal is not None and (
        isinstance(call_ordinal, bool)
        or not isinstance(call_ordinal, int)
        or not 1 <= call_ordinal <= 9_007_199_254_740_991
    ):
        return "call_ordinal must be a positive JSON safe integer"
    return None


def _validate_instance(instance: object, schema: Mapping[str, object]) -> str | None:
    errors = sorted(
        Draft202012Validator(_schema_dict(schema), format_checker=FormatChecker()).iter_errors(
            instance
        ),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if not errors:
        return None
    error = errors[0]
    path = "$" + "".join(
        f"[{part}]" if isinstance(part, int) else f".{part}" for part in error.absolute_path
    )
    return f"{path}: {error.message}"


def _event_id() -> str:
    return f"evt-{uuid4().hex}"


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_thaw_json(item) for item in value]
    return value


def _schema_dict(schema: Mapping[str, object]) -> dict[str, object]:
    return cast(dict[str, object], _thaw_json(schema))


def _require_producer_component(value: object) -> None:
    if not isinstance(value, str) or len(value) > 128 or _NAME_RE.fullmatch(value) is None:
        raise ValueError("producer_component must match the Event v0 name profile")


def _require_optional_id(value: object | None, field: str) -> None:
    if value is not None and (not isinstance(value, str) or _ID_RE.fullmatch(value) is None):
        raise ValueError(f"{field} must match the Event v0 identifier profile")
