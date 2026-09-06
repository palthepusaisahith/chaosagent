"""Focused OpenTelemetry contract, privacy, and failure-isolation tests."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from threading import Barrier
from typing import Any, Protocol, cast

import pytest
from chaosagent_telemetry import (
    SafeSpan,
    TelemetryConfig,
    TelemetryConfigurationError,
    configure_telemetry,
    current_trace_context,
    inject_trace_context,
    record_evaluation,
    record_fault,
    record_gate,
    record_model,
    record_run,
    record_tool,
    span,
)
from chaosagent_telemetry.testing import install_test_providers, reset_test_telemetry
from opentelemetry import trace
from opentelemetry.context import attach, detach
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricExporter,
    MetricExportResult,
    MetricsData,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NonRecordingSpan, Span, SpanContext, TraceFlags


class _MetricPoint(Protocol):
    attributes: Mapping[str, object]


class _MetricPoints(Protocol):
    data_points: Sequence[_MetricPoint]


@contextmanager
def _recording() -> Iterator[tuple[InMemorySpanExporter, InMemoryMetricReader]]:
    spans = InMemorySpanExporter()
    traces = TracerProvider()
    traces.add_span_processor(SimpleSpanProcessor(spans))
    metrics = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[metrics])
    installation = install_test_providers(traces, meter)
    try:
        yield spans, metrics
    finally:
        installation.shutdown()


def test_default_disabled_context_does_not_mutate_event() -> None:
    reset_test_telemetry()
    document: dict[str, object] = {"event_id": "event-1"}
    external = TracerProvider()
    try:
        with external.get_tracer("external").start_as_current_span("external-span"):
            inject_trace_context(document)
            assert current_trace_context() is None
    finally:
        external.shutdown()
    assert document == {"event_id": "event-1"}


def test_active_span_injects_strict_trace_context_and_preserves_parentage() -> None:
    with _recording() as (exporter, _):
        with span("chaosagent.run.execute"):
            document: dict[str, object] = {}
            inject_trace_context(document)
            parent = cast(dict[str, str], document["trace_context"])
            with span("chaosagent.model.invoke"):
                child = current_trace_context()
                assert child is not None
                assert child["trace_id"] == parent["trace_id"]
                assert child["span_id"] != parent["span_id"]
        finished = exporter.get_finished_spans()
    assert [item.name for item in finished] == [
        "chaosagent.model.invoke",
        "chaosagent.run.execute",
    ]
    assert len(parent["trace_id"]) == 32
    assert len(parent["span_id"]) == 16
    assert re.fullmatch(r"[0-9a-f]{32}", parent["trace_id"])
    assert re.fullmatch(r"[0-9a-f]{16}", parent["span_id"])


def test_spans_and_metrics_do_not_capture_content_and_bound_dimensions() -> None:
    with _recording() as (_, reader):
        with span(
            "chaosagent.tool.execute",
            attributes={"chaosagent.tool.id": "orders.get"},
        ):
            record_tool("attacker-controlled-tool", "unexpected", "mystery", 0.5)
            record_model("attacker-provider", "mystery", 0.25)
        metric_data = reader.get_metrics_data()
        assert metric_data is not None
        attribute_sets = [
            dict(point.attributes)
            for resource in metric_data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            for point in cast(_MetricPoints, metric.data).data_points
        ]
    assert attribute_sets
    assert all("attacker-controlled-tool" not in values for values in attribute_sets)
    assert all("attacker-provider" not in values for values in attribute_sets)
    for forbidden_label in (
        "run_id",
        "event_id",
        "request_id",
        "logical_call_id",
        "trace_id",
        "span_id",
        "customer_id",
        "order_id",
        "payment_id",
        "ticket_id",
    ):
        assert all(forbidden_label not in attributes for attributes in attribute_sets)
    assert all(set(attributes.values()) <= {"other"} for attributes in attribute_sets)


def test_application_exception_is_preserved_without_telemetry_masking() -> None:
    with _recording() as (exporter, _):
        with pytest.raises(ValueError, match="domain failure"):
            with span("chaosagent.run.execute"):
                raise ValueError("domain failure")
        assert "domain failure" not in repr(exporter.get_finished_spans())


def test_returned_failure_uses_error_status_without_exception_content() -> None:
    with _recording() as (exporter, _):
        with span("chaosagent.tool.execute") as current:
            current.set_outcome("failed")
        finished = exporter.get_finished_spans()
    assert len(finished) == 1
    assert finished[0].status.status_code.name == "ERROR"
    assert finished[0].status.description is None


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, SystemExit, GeneratorExit])
def test_base_exceptions_are_not_swallowed(interrupt: type[BaseException]) -> None:
    with _recording():
        with pytest.raises(interrupt):
            with span("chaosagent.run.execute"):
                raise interrupt


def test_telemetry_start_failure_is_isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    from chaosagent_telemetry import core

    class BrokenTracer:
        def start_span(self, *_args: object, **_kwargs: object) -> object:
            raise RuntimeError("collector path is broken")

    monkeypatch.setattr(core, "_tracer", BrokenTracer())
    document: dict[str, object] = {}
    with span("chaosagent.run.execute") as current:
        current.set_attribute("safe", True)
        inject_trace_context(document)
    assert document == {}
    with pytest.raises(ValueError, match="domain failure"):
        with span("chaosagent.run.execute"):
            raise ValueError("domain failure")
    reset_test_telemetry()


def test_span_and_metric_operation_failures_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from chaosagent_telemetry import core

    class BrokenSpan:
        def set_attribute(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("attribute failure")

        def add_event(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("event failure")

    class BrokenMeter:
        def create_counter(self, *_args: object, **_kwargs: object) -> object:
            raise RuntimeError("metric failure")

    current = SafeSpan(cast(Span, BrokenSpan()))
    current.set_attribute("safe", True)
    current.add_event("safe")
    monkeypatch.setattr(core, "_meter", BrokenMeter())
    record_tool("orders.get", "read", "success", 0.1)
    reset_test_telemetry()


def test_failing_exporters_do_not_change_domain_result() -> None:
    class FailingSpanExporter(SpanExporter):
        def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
            assert spans
            return SpanExportResult.FAILURE

    class FailingMetricExporter(MetricExporter):
        def export(
            self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: object
        ) -> MetricExportResult:
            del metrics_data, timeout_millis, kwargs
            return MetricExportResult.FAILURE

        def force_flush(self, timeout_millis: float = 10_000) -> bool:
            del timeout_millis
            return False

        def shutdown(self, timeout_millis: float = 30_000, **kwargs: object) -> None:
            del timeout_millis, kwargs

    traces = TracerProvider()
    traces.add_span_processor(SimpleSpanProcessor(FailingSpanExporter()))
    reader = PeriodicExportingMetricReader(
        FailingMetricExporter(), export_interval_millis=3_600_000
    )
    meter = MeterProvider(metric_readers=[reader])
    installation = install_test_providers(traces, meter)
    try:
        domain_result = "unchanged"
        with span("chaosagent.run.execute"):
            record_tool("orders.get", "read", "success", 0.1)
        reader.force_flush()
        assert domain_result == "unchanged"
    finally:
        installation.shutdown()


def test_disabled_bootstrap_needs_no_collector() -> None:
    assert configure_telemetry(TelemetryConfig()) is None
    assert current_trace_context() is None


def test_invalid_environment_configuration_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CHAOSAGENT_TELEMETRY_MODE", "surprise")
    with pytest.raises(TelemetryConfigurationError, match="invalid telemetry mode"):
        TelemetryConfig.from_environment()


def test_invalid_explicit_configuration_fails_before_sdk_startup() -> None:
    with pytest.raises(TelemetryConfigurationError, match="invalid telemetry configuration"):
        configure_telemetry(TelemetryConfig(mode="otlp", endpoint=""))


def test_nonrecording_unsampled_invalid_and_ended_contexts_are_not_exposed() -> None:
    with _recording():
        contexts = (
            SpanContext(
                trace_id=1,
                span_id=2,
                is_remote=False,
                trace_flags=TraceFlags(1),
                trace_state=trace.DEFAULT_TRACE_STATE,
            ),
            SpanContext(
                trace_id=3,
                span_id=4,
                is_remote=True,
                trace_flags=TraceFlags(0),
                trace_state=trace.DEFAULT_TRACE_STATE,
            ),
            SpanContext(
                trace_id=0,
                span_id=0,
                is_remote=False,
                trace_flags=TraceFlags(0),
                trace_state=trace.DEFAULT_TRACE_STATE,
            ),
        )
        for context in contexts:
            token = attach(trace.set_span_in_context(NonRecordingSpan(context)))
            try:
                assert current_trace_context() is None
            finally:
                detach(token)
        with span("chaosagent.run.execute"):
            assert current_trace_context() is not None
        assert current_trace_context() is None


def test_detach_failure_cannot_leak_context_to_the_next_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_detach = detach
    calls = 0

    def fail_once(token: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("deliberate detach failure")
        actual_detach(cast(Any, token))

    monkeypatch.setattr("chaosagent_telemetry.core.detach", fail_once)
    with _recording() as (exporter, _):
        result = "unchanged"
        with span("run-a"):
            run_a = current_trace_context()
        assert result == "unchanged"
        with span("run-b"):
            run_b = current_trace_context()
        assert run_a is not None and run_b is not None
        assert run_a["trace_id"] != run_b["trace_id"]
        finished = exporter.get_finished_spans()
        assert next(item for item in finished if item.name == "run-b").parent is None


def test_detach_failure_preserves_exception_and_legitimate_external_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_detach = detach
    calls = 0

    def fail_once(token: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("deliberate detach failure")
        actual_detach(cast(Any, token))

    monkeypatch.setattr("chaosagent_telemetry.core.detach", fail_once)
    external = TracerProvider()
    try:
        with _recording() as (exporter, _):
            with external.get_tracer("host").start_as_current_span("host") as host:
                with pytest.raises(ValueError, match="domain failure"):
                    with span("run-a"):
                        raise ValueError("domain failure")
                assert trace.get_current_span().get_span_context() == host.get_span_context()
                with span("run-b"):
                    assert current_trace_context() is not None
            finished = exporter.get_finished_spans()
            host_id = host.get_span_context().span_id
            assert all(
                item.parent is not None and item.parent.span_id == host_id for item in finished
            )
    finally:
        external.shutdown()


def test_concurrent_contexts_do_not_cross_correlate() -> None:
    barrier = Barrier(2)

    def capture(name: str) -> dict[str, str]:
        with span(name):
            barrier.wait()
            context = current_trace_context()
            assert context is not None
            barrier.wait()
            return context

    with _recording():
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(capture, "run-a")
            second = executor.submit(capture, "run-b")
            first_context = first.result()
            second_context = second.result()
    assert first_context["trace_id"] != second_context["trace_id"]


@pytest.mark.parametrize(
    "config",
    [
        object(),
        {},
        TelemetryConfig(mode=cast(Any, [])),
        TelemetryConfig(mode="otlp", service_name=cast(Any, object())),
        TelemetryConfig(mode="otlp", endpoint=cast(Any, object())),
        TelemetryConfig(mode="otlp", insecure=cast(Any, "yes")),
    ],
)
def test_malformed_explicit_configuration_is_sanitized(config: object) -> None:
    with pytest.raises(TelemetryConfigurationError):
        configure_telemetry(cast(Any, config))


def test_malformed_metric_metadata_safely_noops() -> None:
    class HostileString(str):
        def __hash__(self) -> int:
            raise RuntimeError("hostile hash")

    hostile = cast(Any, [])
    record_run(hostile, hostile)
    record_model(hostile, hostile, hostile)
    record_tool(hostile, hostile, hostile, hostile)
    record_fault(hostile, hostile, hostile)
    record_evaluation(hostile, hostile)
    record_gate(hostile, hostile)
    hostile_string = cast(Any, HostileString("success"))
    record_run(hostile_string, 0.1)
    record_model(hostile_string, hostile_string, 0.1)
    record_tool(hostile_string, hostile_string, hostile_string, 0.1)
    record_fault(hostile_string, hostile_string, hostile_string)
    record_evaluation(hostile_string, 0.1)
    record_gate(hostile_string, hostile_string)


def test_installation_shutdown_is_idempotent_disables_and_allows_reenable() -> None:
    first_traces = TracerProvider()
    first_metrics = MeterProvider()
    first = install_test_providers(first_traces, first_metrics)
    with span("first"):
        assert current_trace_context() is not None
    first.shutdown()
    first.shutdown()
    with span("retired"):
        assert current_trace_context() is None

    second_traces = TracerProvider()
    second_metrics = MeterProvider()
    second = install_test_providers(second_traces, second_metrics)
    try:
        with span("second"):
            assert current_trace_context() is not None
    finally:
        second.shutdown()


def test_production_bootstrap_owns_async_processors_and_rejects_live_reconfigure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import chaosagent_telemetry as public_api
    import chaosagent_telemetry.bootstrap as bootstrap

    class TrackingSpanExporter(SpanExporter):
        def __init__(self) -> None:
            self.shutdown_calls = 0

        def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
            del spans
            return SpanExportResult.SUCCESS

        def shutdown(self) -> None:
            self.shutdown_calls += 1

    class TrackingMetricExporter(MetricExporter):
        def __init__(self) -> None:
            super().__init__()
            self.shutdown_calls = 0

        def export(
            self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: object
        ) -> MetricExportResult:
            del metrics_data, timeout_millis, kwargs
            return MetricExportResult.SUCCESS

        def force_flush(self, timeout_millis: float = 10_000) -> bool:
            del timeout_millis
            return True

        def shutdown(self, timeout_millis: float = 30_000, **kwargs: object) -> None:
            del timeout_millis, kwargs
            self.shutdown_calls += 1

    span_exporters: list[TrackingSpanExporter] = []
    metric_exporters: list[TrackingMetricExporter] = []
    span_processors: list[BatchSpanProcessor] = []
    metric_readers: list[PeriodicExportingMetricReader] = []

    def span_exporter_factory(**kwargs: object) -> TrackingSpanExporter:
        del kwargs
        exporter = TrackingSpanExporter()
        span_exporters.append(exporter)
        return exporter

    def metric_exporter_factory(**kwargs: object) -> TrackingMetricExporter:
        del kwargs
        exporter = TrackingMetricExporter()
        metric_exporters.append(exporter)
        return exporter

    def batch_processor_factory(exporter: SpanExporter) -> BatchSpanProcessor:
        processor = BatchSpanProcessor(exporter)
        span_processors.append(processor)
        return processor

    def periodic_reader_factory(exporter: MetricExporter) -> PeriodicExportingMetricReader:
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=3_600_000)
        metric_readers.append(reader)
        return reader

    monkeypatch.setattr(bootstrap, "OTLPSpanExporter", span_exporter_factory)
    monkeypatch.setattr(bootstrap, "OTLPMetricExporter", metric_exporter_factory)
    monkeypatch.setattr(bootstrap, "BatchSpanProcessor", batch_processor_factory)
    monkeypatch.setattr(bootstrap, "PeriodicExportingMetricReader", periodic_reader_factory)

    config = TelemetryConfig(mode="otlp")
    first = configure_telemetry(config)
    assert first is not None
    try:
        assert len(span_processors) == len(metric_readers) == 1
        assert not hasattr(public_api, "install_providers")
        with pytest.raises(TelemetryConfigurationError, match="already configured"):
            configure_telemetry(config)
        assert len(span_processors) == len(metric_readers) == 2
        assert span_exporters[0].shutdown_calls == metric_exporters[0].shutdown_calls == 0
        assert span_exporters[1].shutdown_calls == metric_exporters[1].shutdown_calls == 1
        with span("still-active-after-rejected-reconfigure"):
            assert current_trace_context() is not None

        first.shutdown()
        first.shutdown()
        assert span_exporters[0].shutdown_calls == metric_exporters[0].shutdown_calls == 1
        with span("after-shutdown"):
            assert current_trace_context() is None

        replacement = configure_telemetry(config)
        assert replacement is not None
        assert len(span_processors) == len(metric_readers) == 3
        assert configure_telemetry(TelemetryConfig()) is None
        assert span_exporters[2].shutdown_calls == metric_exporters[2].shutdown_calls == 1
        with span("after-disabled-reset"):
            assert current_trace_context() is None
    finally:
        reset_test_telemetry()


def test_retired_active_span_cannot_revive_after_reconfiguration() -> None:
    first_exporter = InMemorySpanExporter()
    first_traces = TracerProvider()
    first_traces.add_span_processor(SimpleSpanProcessor(first_exporter))
    first = install_test_providers(first_traces, MeterProvider())
    first_scope = span("installation-a")
    first_scope.__enter__()
    first_context = current_trace_context()
    assert first_context is not None
    first.shutdown()
    assert current_trace_context() is None
    with span("disabled"):
        assert current_trace_context() is None

    second_exporter = InMemorySpanExporter()
    second_traces = TracerProvider()
    second_traces.add_span_processor(SimpleSpanProcessor(second_exporter))
    second = install_test_providers(second_traces, MeterProvider())
    try:
        assert current_trace_context() is None
        with span("installation-b"):
            second_context = current_trace_context()
            assert second_context is not None
            event: dict[str, object] = {}
            inject_trace_context(event)
            assert event["trace_context"] == second_context
            with span("installation-b-child"):
                child_context = current_trace_context()
                assert child_context is not None
        assert second_context["trace_id"] != first_context["trace_id"]
        assert first_context not in event.values()
        finished = second_exporter.get_finished_spans()
        parent = next(item for item in finished if item.name == "installation-b").parent
        assert parent is None
        child = next(item for item in finished if item.name == "installation-b-child")
        root = next(item for item in finished if item.name == "installation-b")
        assert child.parent is not None and child.parent.span_id == root.context.span_id
    finally:
        first_scope.__exit__(None, None, None)
        second.shutdown()


def test_retired_span_falls_back_to_legitimate_external_parent() -> None:
    external = TracerProvider()
    first = install_test_providers(TracerProvider(), MeterProvider())
    try:
        with external.get_tracer("host").start_as_current_span("external") as external_span:
            first_scope = span("installation-a")
            first_scope.__enter__()
            second = None
            try:
                first_context = current_trace_context()
                assert first_context is not None
                first.shutdown()

                exporter = InMemorySpanExporter()
                second_traces = TracerProvider()
                second_traces.add_span_processor(SimpleSpanProcessor(exporter))
                second = install_test_providers(second_traces, MeterProvider())
                assert current_trace_context() is None
                with span("installation-b"):
                    second_context = current_trace_context()
                    assert second_context is not None
                finished = exporter.get_finished_spans()
                current = next(item for item in finished if item.name == "installation-b")
                assert current.parent is not None
                assert current.parent.span_id == external_span.get_span_context().span_id
                assert current.parent.span_id != int(first_context["span_id"], 16)
            finally:
                first_scope.__exit__(None, None, None)
                if second is not None:
                    second.shutdown()
    finally:
        first.shutdown()
        external.shutdown()


def test_retired_span_does_not_fall_back_to_ended_external_parent() -> None:
    external = TracerProvider()
    first = install_test_providers(TracerProvider(), MeterProvider())
    try:
        with external.get_tracer("host").start_as_current_span("external") as external_span:
            first_scope = span("installation-a")
            first_scope.__enter__()
            second = None
            try:
                external_span.end()
                first.shutdown()
                exporter = InMemorySpanExporter()
                second_traces = TracerProvider()
                second_traces.add_span_processor(SimpleSpanProcessor(exporter))
                second = install_test_providers(second_traces, MeterProvider())
                with span("installation-b"):
                    assert current_trace_context() is not None
                current = next(
                    item for item in exporter.get_finished_spans() if item.name == "installation-b"
                )
                assert current.parent is None
            finally:
                first_scope.__exit__(None, None, None)
                if second is not None:
                    second.shutdown()
    finally:
        first.shutdown()
        external.shutdown()


def test_copied_stale_contexts_from_multiple_generations_cannot_parent_current() -> None:
    first = install_test_providers(TracerProvider(), MeterProvider())
    first_scope = span("installation-a")
    first_scope.__enter__()
    first_context = current_trace_context()
    assert first_context is not None
    copied_first = copy_context()
    first.shutdown()

    second = install_test_providers(TracerProvider(), MeterProvider())
    second_scope = span("installation-b")
    second_scope.__enter__()
    second_context = current_trace_context()
    assert second_context is not None
    copied_second = copy_context()
    second.shutdown()

    exporter = InMemorySpanExporter()
    third_traces = TracerProvider()
    third_traces.add_span_processor(SimpleSpanProcessor(exporter))
    third = install_test_providers(third_traces, MeterProvider())

    def capture(name: str) -> dict[str, str]:
        assert current_trace_context() is None
        with span(name):
            context = current_trace_context()
            assert context is not None
            return context

    try:
        from_first = copied_first.run(capture, "installation-c-from-a")
        from_second = copied_second.run(capture, "installation-c-from-b")
        assert from_first["trace_id"] not in {
            first_context["trace_id"],
            second_context["trace_id"],
        }
        assert from_second["trace_id"] not in {
            first_context["trace_id"],
            second_context["trace_id"],
        }
        assert all(item.parent is None for item in exporter.get_finished_spans())
    finally:
        second_scope.__exit__(None, None, None)
        first_scope.__exit__(None, None, None)
        third.shutdown()


def test_detach_failure_cannot_bridge_retired_and_new_installations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_detach = detach
    calls = 0

    def fail_once(token: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("deliberate detach failure")
        actual_detach(cast(Any, token))

    monkeypatch.setattr("chaosagent_telemetry.core.detach", fail_once)
    first_exporter = InMemorySpanExporter()
    first_traces = TracerProvider()
    first_traces.add_span_processor(SimpleSpanProcessor(first_exporter))
    first = install_test_providers(first_traces, MeterProvider())
    with span("installation-a"):
        first_context = current_trace_context()
        assert first_context is not None
    first.shutdown()

    second_exporter = InMemorySpanExporter()
    second_traces = TracerProvider()
    second_traces.add_span_processor(SimpleSpanProcessor(second_exporter))
    second = install_test_providers(second_traces, MeterProvider())
    try:
        assert current_trace_context() is None
        with span("installation-b"):
            second_context = current_trace_context()
            assert second_context is not None
        assert second_context["trace_id"] != first_context["trace_id"]
        current = next(
            item for item in second_exporter.get_finished_spans() if item.name == "installation-b"
        )
        assert current.parent is None
    finally:
        second.shutdown()
