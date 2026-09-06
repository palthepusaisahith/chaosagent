"""Backend-neutral OpenTelemetry integration for ChaosAgent."""

from .bootstrap import (
    TelemetryConfig,
    TelemetryConfigurationError,
    configure_telemetry,
)
from .conventions import (
    model_operation_attributes,
    model_response_attributes,
    tool_operation_attributes,
)
from .core import (
    INSTRUMENTATION_SCOPE,
    INSTRUMENTATION_VERSION,
    SafeSpan,
    TelemetryInstallation,
    current_trace_context,
    inject_trace_context,
    record_evaluation,
    record_fault,
    record_gate,
    record_model,
    record_run,
    record_tool,
    safe_identifier,
    set_current_span_attributes,
    span,
    timed,
)

__all__ = [
    "INSTRUMENTATION_SCOPE",
    "INSTRUMENTATION_VERSION",
    "SafeSpan",
    "TelemetryConfig",
    "TelemetryConfigurationError",
    "TelemetryInstallation",
    "configure_telemetry",
    "current_trace_context",
    "inject_trace_context",
    "model_operation_attributes",
    "model_response_attributes",
    "record_evaluation",
    "record_fault",
    "record_gate",
    "record_model",
    "record_run",
    "record_tool",
    "safe_identifier",
    "set_current_span_attributes",
    "span",
    "timed",
    "tool_operation_attributes",
]
