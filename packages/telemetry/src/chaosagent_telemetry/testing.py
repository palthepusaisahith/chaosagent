"""Explicit test-only provider installation hooks for telemetry verification."""

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider

from .core import TelemetryInstallation, _install_providers, _reset_telemetry


def install_test_providers(
    tracer_provider: TracerProvider, meter_provider: MeterProvider
) -> TelemetryInstallation:
    """Install arbitrary providers only for isolated tests."""
    return _install_providers(tracer_provider, meter_provider)


def reset_test_telemetry() -> None:
    """Retire the current test installation and restore the disabled facade."""
    _reset_telemetry()
