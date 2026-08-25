"""Strict, transactional read-only tool boundary for the synthetic company."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
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
)
from chaosagent_persistence import (
    CompanyOrder,
    CompanyShipment,
    LeaseExpiredError,
    LeaseIdentity,
    PersistenceError,
    PersistenceRepository,
    ReferenceNotFoundError,
    StaleLeaseError,
)
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
    "infrastructure_error",
]

TOOL_EVENT_SCHEMA_VERSION = "chaosagent.run-event/v0"
SCENARIO_V0_SCHEMA_VERSION = "chaosagent.scenario/v0"
ORDERS_GET_V0 = "chaosagent.tool/orders.get/v0"
SHIPPING_GET_STATUS_V0 = "chaosagent.tool/shipping.get_status/v0"
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_CONTRACT_VERSION_RE = re.compile(r"^chaosagent\.tool/[a-z0-9._-]+/v[0-9]+$")
SCENARIO_V0_TOOL_VERSIONS: Mapping[str, str] = MappingProxyType(
    {
        "orders.get": ORDERS_GET_V0,
        "shipping.get_status": SHIPPING_GET_STATUS_V0,
    }
)


@dataclass(frozen=True, slots=True)
class ToolError:
    code: ToolErrorCode
    message: str


@dataclass(frozen=True, slots=True)
class ToolExecutionResult:
    """Provider-neutral result; output mappings are immutable and flat in v0."""

    tool_id: str
    contract_version: str
    outcome: Literal["succeeded", "failed", "denied"]
    output: Mapping[str, object] | None
    error: ToolError | None
    request_event_id: str | None
    result_event_id: str | None


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


type ToolHandler = Callable[[ReadOnlyCompanyState, Mapping[str, object]], dict[str, object] | None]


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    tool_id: str
    contract_version: str
    description: str
    capability: Literal["read"]
    read_only: Literal[True]
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
            if definition.capability != "read" or definition.read_only is not True:
                raise ValueError("Issue #8 registry accepts only read-only tool definitions")
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


@lru_cache(maxsize=4)
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


def default_tool_registry() -> ToolRegistry:
    """Return the fixed Issue #8 catalog; no dynamic discovery occurs."""
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
        )
    )


class ToolGateway:
    """Execute one read-only call and append coherent Event v0 evidence."""

    def __init__(
        self,
        session: Session,
        *,
        registry: ToolRegistry | None = None,
        producer_component: str = "tool-gateway",
        producer_instance_id: str | None = None,
    ) -> None:
        self._session = session
        self._repository = PersistenceRepository(session)
        self._registry = registry or default_tool_registry()
        _require_producer_component(producer_component)
        _require_optional_id(producer_instance_id, "producer_instance_id")
        self._producer_component = producer_component
        self._producer_instance_id = producer_instance_id

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
        step_id: object | None = None,
        causation_event_id: object | None = None,
    ) -> ToolExecutionResult:
        field_error = _validate_call_fields(
            lease,
            tool_id,
            contract_version,
            arguments,
            logical_call_id,
            attempt_id,
            attempt_number,
            step_id,
            causation_event_id,
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
        assert step_id is None or isinstance(step_id, str)
        assert causation_event_id is None or isinstance(causation_event_id, str)
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

                request_event_id = _event_id()
                request_payload: dict[str, object] = {
                    "logical_call_id": logical_call_id,
                    "attempt_id": attempt_id,
                    "attempt_number": attempt_number,
                    "tool_id": tool_id,
                    "arguments_digest": digest_payload_v0(arguments_snapshot),
                }
                if step_id is not None:
                    request_payload["step_id"] = step_id
                self._append_event(
                    run.run_id,
                    request_event_id,
                    "tool.requested",
                    request_payload,
                    correlation_id=logical_call_id,
                    causation_event_id=causation_event_id,
                )

                started = monotonic_ns()
                output: dict[str, object] | None = None
                tool_error: ToolError | None = None
                company = _RunBoundCompanyState(self._repository, run.run_id)
                try:
                    with self._session.begin_nested():
                        output = definition.handler(company, arguments_snapshot)
                        if output is None:
                            tool_error = ToolError(
                                "entity_not_found",
                                "requested synthetic-company entity was not found",
                            )
                        else:
                            output_error = _validate_instance(output, definition.output_schema)
                            if output_error is not None:
                                raise ValueError(
                                    f"handler output violated its contract: {output_error}"
                                )
                except Exception:
                    output = None
                    tool_error = ToolError(
                        "infrastructure_error", "tool execution failed internally"
                    )
                duration_ms = max(0, (monotonic_ns() - started) // 1_000_000)
                result_event_id = _event_id()
                result_payload: dict[str, object] = {
                    "logical_call_id": logical_call_id,
                    "request_event_id": request_event_id,
                    "attempt_id": attempt_id,
                    "attempt_number": attempt_number,
                    "tool_id": tool_id,
                    "outcome": "succeeded" if tool_error is None else "failed",
                    "duration_ms": duration_ms,
                }
                if output is not None:
                    result_payload["response_digest"] = digest_payload_v0(output)
                else:
                    assert tool_error is not None
                    result_payload["error_code"] = tool_error.code
                self._append_event(
                    run.run_id,
                    result_event_id,
                    "tool.result",
                    result_payload,
                    correlation_id=logical_call_id,
                    causation_event_id=request_event_id,
                )
                return ToolExecutionResult(
                    tool_id=tool_id,
                    contract_version=contract_version,
                    outcome="succeeded" if tool_error is None else "failed",
                    output=None if output is None else MappingProxyType(deepcopy(output)),
                    error=tool_error,
                    request_event_id=request_event_id,
                    result_event_id=result_event_id,
                )
        except (EvidenceValidationError, PersistenceError, SQLAlchemyError):
            return self._rejected(
                tool_id,
                contract_version,
                "infrastructure_error",
                "tool evidence could not be persisted",
            )

    def _append_event(
        self,
        run_id: str,
        event_id: str,
        event_type: Literal["tool.requested", "tool.result"],
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
    def _rejected(
        tool_id: str,
        contract_version: str,
        code: ToolErrorCode,
        message: str,
    ) -> ToolExecutionResult:
        return ToolExecutionResult(
            tool_id=tool_id,
            contract_version=contract_version,
            outcome="denied" if code in {"tool_not_allowed", "stale_lease"} else "failed",
            output=None,
            error=ToolError(code, message),
            request_event_id=None,
            result_event_id=None,
        )


def _validate_call_fields(
    lease: object,
    tool_id: object,
    contract_version: object,
    arguments: object,
    logical_call_id: object,
    attempt_id: object,
    attempt_number: object,
    step_id: object | None,
    causation_event_id: object | None,
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
    for name, value in (("step_id", step_id), ("causation_event_id", causation_event_id)):
        if value is not None and (not isinstance(value, str) or _ID_RE.fullmatch(value) is None):
            return f"{name} is malformed"
    if isinstance(attempt_number, bool) or attempt_number != 1:
        return "attempt_number must be 1; retries are deferred"
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
