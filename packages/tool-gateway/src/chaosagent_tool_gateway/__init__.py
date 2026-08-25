"""Read-only synthetic-company tool contracts, catalog, and gateway."""

from .gateway import (
    ORDERS_GET_V0,
    SCENARIO_V0_SCHEMA_VERSION,
    SCENARIO_V0_TOOL_VERSIONS,
    SHIPPING_GET_STATUS_V0,
    ReadOnlyCompanyState,
    ToolDefinition,
    ToolError,
    ToolErrorCode,
    ToolExecutionResult,
    ToolGateway,
    ToolRegistry,
    default_tool_registry,
)

__all__ = [
    "ORDERS_GET_V0",
    "ReadOnlyCompanyState",
    "SCENARIO_V0_SCHEMA_VERSION",
    "SCENARIO_V0_TOOL_VERSIONS",
    "SHIPPING_GET_STATUS_V0",
    "ToolDefinition",
    "ToolError",
    "ToolErrorCode",
    "ToolExecutionResult",
    "ToolGateway",
    "ToolRegistry",
    "default_tool_registry",
]
