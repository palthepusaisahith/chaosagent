"""Explicit OpenTelemetry SDK bootstrap for deployable ChaosAgent processes."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal, cast

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .core import (
    TelemetryInstallation,
    TelemetryLifecycleError,
    _install_providers,
    _reset_telemetry,
)


class TelemetryConfigurationError(ValueError):
    """Sanitized invalid telemetry startup configuration."""


@dataclass(frozen=True, slots=True)
class TelemetryConfig:
    mode: Literal["disabled", "otlp"] = "disabled"
    service_name: str = "chaosagent"
    endpoint: str = "http://127.0.0.1:4317"
    insecure: bool = True

    @classmethod
    def from_environment(cls) -> TelemetryConfig:
        mode = os.environ.get("CHAOSAGENT_TELEMETRY_MODE", "disabled")
        if mode not in {"disabled", "otlp"}:
            raise TelemetryConfigurationError("invalid telemetry mode")
        service_name = os.environ.get("OTEL_SERVICE_NAME", "chaosagent").strip()
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317").strip()
        insecure_value = os.environ.get("OTEL_EXPORTER_OTLP_INSECURE", "true").lower()
        if not service_name or not endpoint or insecure_value not in {"true", "false"}:
            raise TelemetryConfigurationError("invalid telemetry configuration")
        return cls(
            mode=cast(Literal["disabled", "otlp"], mode),
            service_name=service_name,
            endpoint=endpoint,
            insecure=insecure_value == "true",
        )


def configure_telemetry(config: TelemetryConfig | None = None) -> TelemetryInstallation | None:
    """Configure OTLP explicitly; default operation remains dependency-free and disabled."""
    if config is not None and type(config) is not TelemetryConfig:
        raise TelemetryConfigurationError("invalid telemetry configuration")
    selected = TelemetryConfig.from_environment() if config is None else config
    if type(selected.mode) is not str or selected.mode not in {"disabled", "otlp"}:
        raise TelemetryConfigurationError("invalid telemetry mode")
    if selected.mode == "disabled":
        _reset_telemetry()
        return None
    if (
        type(selected.service_name) is not str
        or not selected.service_name.strip()
        or len(selected.service_name) > 255
        or type(selected.endpoint) is not str
        or not selected.endpoint.strip()
        or len(selected.endpoint) > 2_048
        or type(selected.insecure) is not bool
    ):
        raise TelemetryConfigurationError("invalid telemetry configuration")
    tracer_provider: TracerProvider | None = None
    meter_provider: MeterProvider | None = None
    try:
        resource = Resource.create({SERVICE_NAME: selected.service_name})
        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=selected.endpoint, insecure=selected.insecure)
            )
        )
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=selected.endpoint, insecure=selected.insecure)
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    except Exception:
        for provider in (meter_provider, tracer_provider):
            if provider is not None:
                try:
                    provider.shutdown()
                except Exception:
                    pass
        raise TelemetryConfigurationError("telemetry SDK initialization failed") from None
    assert tracer_provider is not None and meter_provider is not None
    try:
        return _install_providers(tracer_provider, meter_provider)
    except TelemetryLifecycleError:
        for provider in (meter_provider, tracer_provider):
            try:
                provider.shutdown()
            except Exception:
                pass
        raise TelemetryConfigurationError("telemetry is already configured") from None
