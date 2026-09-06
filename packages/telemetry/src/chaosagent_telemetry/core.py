"""Failure-isolated telemetry API used by product-domain packages."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from math import isfinite
from threading import RLock
from time import monotonic
from typing import Final, Literal, Protocol, cast

from opentelemetry import metrics, trace
from opentelemetry.context import Context, attach, detach
from opentelemetry.metrics import Counter, Histogram, Meter, MeterProvider
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer, TracerProvider

INSTRUMENTATION_SCOPE: Final = "io.chaosagent"
INSTRUMENTATION_VERSION: Final = "0.1.0"

_lock = RLock()
_tracer: Tracer = trace.get_tracer(INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION)
_meter: Meter = metrics.get_meter(INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION)
_instruments: dict[str, Counter | Histogram] = {}
_enabled = False
_current_installation: TelemetryInstallation | None = None
_active_span: ContextVar[_OwnedSpanContext | None] = ContextVar(
    "chaosagent_active_span", default=None
)
_IDENTIFIER_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class ProviderHandle(Protocol):
    """Provider lifecycle shared by SDK tracer and meter providers."""

    def shutdown(self) -> object: ...


class TelemetryLifecycleError(RuntimeError):
    """Internal deterministic ownership conflict used by the bootstrap."""


@dataclass(slots=True, eq=False)
class TelemetryInstallation:
    """Explicit installation returned by bootstrap or tests."""

    tracer_provider: ProviderHandle
    meter_provider: ProviderHandle
    _closed: bool = False

    def shutdown(self) -> None:
        """Disable this installation and retire each owned provider once."""
        global _current_installation
        with _lock:
            if self._closed:
                return
            self._closed = True
            if _current_installation is self:
                _set_disabled_locked()
                _current_installation = None
        for provider in (self.meter_provider, self.tracer_provider):
            try:
                provider.shutdown()
            except Exception:
                # Export and shutdown are operational best effort, never product truth.
                continue


@dataclass(frozen=True, slots=True)
class _OwnedSpanContext:
    """An active span bound to its opaque installation generation and host parent."""

    span: Span
    installation: TelemetryInstallation
    external_parent: Context


def _install_providers(
    tracer_provider: ProviderHandle,
    meter_provider: ProviderHandle,
) -> TelemetryInstallation:
    """Install one owned provider pair without changing process-global providers."""
    global _tracer, _meter, _instruments, _enabled, _current_installation
    tracer = cast(TracerProvider, tracer_provider).get_tracer(
        INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION
    )
    meter = cast(MeterProvider, meter_provider).get_meter(
        INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION
    )
    installation = TelemetryInstallation(tracer_provider, meter_provider)
    with _lock:
        if _current_installation is not None:
            raise TelemetryLifecycleError("telemetry is already configured")
        _tracer = tracer
        _meter = meter
        _instruments = {}
        _enabled = True
        _current_installation = installation
    return installation


def _set_disabled_locked() -> None:
    global _tracer, _meter, _instruments, _enabled
    _tracer = trace.NoOpTracerProvider().get_tracer(INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION)
    _meter = metrics.NoOpMeterProvider().get_meter(INSTRUMENTATION_SCOPE, INSTRUMENTATION_VERSION)
    _instruments = {}
    _enabled = False


def _reset_telemetry() -> None:
    """Disable and retire only the provider pair owned by this facade."""
    global _current_installation
    with _lock:
        installation = _current_installation
        if installation is None:
            _set_disabled_locked()
            return
    installation.shutdown()


@dataclass(slots=True)
class SafeSpan:
    """A span whose observability operations cannot affect domain execution."""

    _span: Span | None

    def set_attribute(self, key: str, value: str | int | float | bool) -> None:
        if self._span is None:
            return
        try:
            self._span.set_attribute(key, value)
        except Exception:
            return

    def add_event(
        self,
        name: str,
        attributes: Mapping[str, str | int | float | bool] | None = None,
    ) -> None:
        if self._span is None:
            return
        try:
            self._span.add_event(name, attributes=dict(attributes or {}))
        except Exception:
            return

    def set_outcome(self, outcome: object) -> None:
        normalized = _bounded(outcome, _OUTCOMES)
        self.set_attribute("chaosagent.outcome", normalized)
        if self._span is None:
            return
        try:
            if normalized in _ERROR_OUTCOMES or normalized == "other":
                self._span.set_status(Status(StatusCode.ERROR))
            elif normalized in _SUCCESS_OUTCOMES:
                self._span.set_status(Status(StatusCode.OK))
        except Exception:
            return


@contextmanager
def span(
    name: str,
    *,
    kind: Literal["internal", "client"] = "internal",
    attributes: Mapping[str, str | int | float | bool] | None = None,
) -> Iterator[SafeSpan]:
    """Start a span safely while preserving all application exceptions."""
    current: Span | None = None
    token: Token[Context] | None = None
    active_token: Token[_OwnedSpanContext | None] | None = None
    previous_active = _active_span.get()
    with _lock:
        installation = _current_installation if _enabled else None
        tracer = _tracer
    parent_context, external_parent = _safe_parent_context(previous_active, installation)
    try:
        if installation is not None:
            span_kind = SpanKind.CLIENT if kind == "client" else SpanKind.INTERNAL
            current = tracer.start_span(
                name,
                context=parent_context,
                kind=span_kind,
                attributes=dict(attributes or {}),
            )
            try:
                token = attach(trace.set_span_in_context(current))
                active_token = _active_span.set(
                    _OwnedSpanContext(current, installation, external_parent)
                )
            except Exception:
                try:
                    current.end()
                except Exception:
                    pass
                current = None
    except Exception:
        current = None
        token = None
    handle = SafeSpan(current)
    try:
        yield handle
    except Exception:
        if current is not None:
            try:
                current.set_status(Status(StatusCode.ERROR))
            except Exception:
                pass
        raise
    finally:
        if active_token is not None:
            try:
                _active_span.reset(active_token)
            except Exception:
                _active_span.set(previous_active)
        if token is not None:
            try:
                detach(token)
            except Exception:
                try:
                    attach(parent_context)
                except Exception:
                    pass
        if current is not None:
            try:
                current.end()
            except Exception:
                pass


def current_trace_context() -> dict[str, str] | None:
    """Return strict W3C-sized identifiers for the currently recording context."""
    try:
        active = _active_span.get()
        with _lock:
            if (
                not _enabled
                or active is None
                or active.installation is not _current_installation
                or not active.span.is_recording()
            ):
                return None
            context = active.span.get_span_context()
            if not context.is_valid or context.trace_id == 0 or context.span_id == 0:
                return None
            return {
                "trace_id": f"{context.trace_id:032x}",
                "span_id": f"{context.span_id:016x}",
            }
    except Exception:
        return None


def set_current_span_attributes(
    attributes: Mapping[str, str | int | float | bool],
) -> None:
    """Safely enrich the active span after authoritative state is loaded."""
    try:
        active = _active_span.get()
        with _lock:
            if (
                not _enabled
                or active is None
                or active.installation is not _current_installation
                or not active.span.is_recording()
            ):
                return
            if not active.span.get_span_context().is_valid:
                return
            for key, value in attributes.items():
                active.span.set_attribute(key, value)
    except Exception:
        return


def safe_identifier(value: object) -> str:
    """Bound an operational identifier before it reaches a span attribute."""
    return (
        value
        if type(value) is str and len(value) <= 128 and _IDENTIFIER_RE.fullmatch(value)
        else "invalid"
    )


def inject_trace_context(document: dict[str, object]) -> None:
    """Add optional event correlation without changing payload or business identity."""
    try:
        context = current_trace_context()
        if context is not None:
            document["trace_context"] = context
    except Exception:
        return


@contextmanager
def timed() -> Iterator[list[float]]:
    """Return a mutable one-item duration result without wall-clock semantics."""
    try:
        started = monotonic()
    except Exception:
        started = None
    result = [0.0]
    try:
        yield result
    finally:
        try:
            elapsed = 0.0 if started is None else monotonic() - started
            result[0] = elapsed if isfinite(elapsed) and elapsed >= 0 else 0.0
        except Exception:
            result[0] = 0.0


_OUTCOMES: Final = frozenset(
    {
        "completed",
        "evaluation_ready",
        "waiting_for_approval",
        "failed",
        "infra_error",
        "invalid",
        "timed_out",
        "cancelled",
        "stale_lease",
        "run_not_ready",
        "approval_required",
        "denied",
        "not_found",
        "success",
        "error",
        "applied",
        "not_applied",
        "observed",
        "not_observed",
        "matched",
        "not_matched",
        "pass",
        "fail",
    }
)
_ERROR_OUTCOMES: Final = frozenset(
    {
        "failed",
        "infra_error",
        "invalid",
        "timed_out",
        "cancelled",
        "stale_lease",
        "run_not_ready",
        "denied",
        "error",
        "fail",
    }
)
_SUCCESS_OUTCOMES: Final = frozenset(
    {
        "completed",
        "evaluation_ready",
        "success",
        "applied",
        "observed",
        "matched",
        "pass",
    }
)
_PROVIDERS: Final = frozenset({"openai", "scripted"})
_TOOLS: Final = frozenset(
    {"orders.get", "shipping.get_status", "payments.refund", "support.update_ticket"}
)
_FAULT_KINDS: Final = frozenset(
    {
        "delay",
        "timeout",
        "http_error",
        "malformed_response",
        "stale_field",
        "auth_error",
        "indirect_prompt_injection",
        "duplicate_response",
        "ambiguous_post_commit_timeout",
    }
)
_PHASES: Final = frozenset({"before_tool", "after_tool", "after_commit"})
_GATES: Final = frozenset({"critical"})


def _safe_parent_context(
    active: _OwnedSpanContext | None,
    installation: TelemetryInstallation | None,
) -> tuple[Context, Context]:
    """Return a live same-generation parent and the original safe host parent."""
    try:
        if active is not None:
            external_parent = _recording_parent_context(active.external_parent)
            if active.installation is installation:
                context = active.span.get_span_context()
                if active.span.is_recording() and context.is_valid:
                    return trace.set_span_in_context(active.span, Context()), external_parent
            return external_parent, external_parent
        parent = trace.get_current_span()
        context = parent.get_span_context()
        if parent.is_recording() and context.is_valid:
            external_parent = trace.set_span_in_context(parent, Context())
            return external_parent, external_parent
    except Exception:
        pass
    empty = Context()
    return empty, empty


def _recording_parent_context(context: Context) -> Context:
    """Keep a captured host parent only while its actual span remains recording and valid."""
    try:
        parent = trace.get_current_span(context)
        span_context = parent.get_span_context()
        if parent.is_recording() and span_context.is_valid:
            return context
    except Exception:
        pass
    return Context()


def _bounded(value: object, allowed: frozenset[str]) -> str:
    return value if type(value) is str and value in allowed else "other"


def _instrument(name: str, *, histogram: bool = False) -> Counter | Histogram:
    with _lock:
        instrument = _instruments.get(name)
        if instrument is not None:
            return instrument
        if histogram:
            instrument = _meter.create_histogram(name, unit="s")
        else:
            instrument = _meter.create_counter(name, unit="{operation}")
        _instruments[name] = instrument
        return instrument


def _counter(name: str, attributes: Mapping[str, str]) -> None:
    try:
        instrument = _instrument(name)
        if isinstance(instrument, Counter):
            instrument.add(1, dict(attributes))
    except Exception:
        return


def _histogram(name: str, duration_seconds: object, attributes: Mapping[str, str]) -> None:
    try:
        instrument = _instrument(name, histogram=True)
        if isinstance(instrument, Histogram):
            duration = 0.0
            if (
                isinstance(duration_seconds, int | float)
                and not isinstance(duration_seconds, bool)
                and isfinite(duration_seconds)
                and duration_seconds >= 0
            ):
                duration = float(duration_seconds)
            instrument.record(duration, dict(attributes))
    except Exception:
        return


def record_run(outcome: object, duration_seconds: object) -> None:
    attributes = {"outcome": _bounded(outcome, _OUTCOMES)}
    _counter("chaosagent.run.executions", attributes)
    _histogram("chaosagent.run.duration", duration_seconds, attributes)


def record_model(provider: object, outcome: object, duration_seconds: object) -> None:
    attributes = {
        "provider": _bounded(provider, _PROVIDERS),
        "outcome": _bounded(outcome, _OUTCOMES),
    }
    _counter("chaosagent.model.invocations", attributes)
    _histogram("chaosagent.model.duration", duration_seconds, attributes)


def record_tool(
    tool_id: object, capability: object, outcome: object, duration_seconds: object
) -> None:
    attributes = {
        "tool": _bounded(tool_id, _TOOLS),
        "capability": (
            capability
            if type(capability) is str and capability in {"read", "mutation"}
            else "other"
        ),
        "outcome": _bounded(outcome, _OUTCOMES),
    }
    _counter("chaosagent.tool.invocations", attributes)
    _histogram("chaosagent.tool.duration", duration_seconds, attributes)


def record_fault(kind: object, phase: object, outcome: object) -> None:
    attributes = {
        "kind": _bounded(kind, _FAULT_KINDS),
        "phase": _bounded(phase, _PHASES),
        "outcome": _bounded(outcome, _OUTCOMES),
    }
    _counter("chaosagent.fault.decisions", attributes)


def record_evaluation(classification: object, duration_seconds: object) -> None:
    attributes = {"classification": _bounded(classification, _OUTCOMES)}
    _counter("chaosagent.evaluator.executions", attributes)
    _histogram("chaosagent.evaluator.duration", duration_seconds, attributes)


def record_gate(family: object, outcome: object) -> None:
    _counter(
        "chaosagent.evaluator.gates",
        {"family": _bounded(family, _GATES), "outcome": _bounded(outcome, _OUTCOMES)},
    )
