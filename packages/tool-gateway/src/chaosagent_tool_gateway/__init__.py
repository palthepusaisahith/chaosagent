"""Synthetic-company tool contracts, capability boundaries, and gateway."""

from .gateway import (
    ORDERS_GET_V0,
    PAYMENTS_REFUND_V0,
    SCENARIO_V0_SCHEMA_VERSION,
    SCENARIO_V0_TOOL_VERSIONS,
    SHIPPING_GET_STATUS_V0,
    SUPPORT_UPDATE_TICKET_V0,
    ReadOnlyCompanyState,
    RefundMutationIntent,
    SupportTicketMutationIntent,
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
    "PAYMENTS_REFUND_V0",
    "RefundMutationIntent",
    "ReadOnlyCompanyState",
    "SupportTicketMutationIntent",
    "SCENARIO_V0_SCHEMA_VERSION",
    "SCENARIO_V0_TOOL_VERSIONS",
    "SHIPPING_GET_STATUS_V0",
    "SUPPORT_UPDATE_TICKET_V0",
    "ToolDefinition",
    "ToolError",
    "ToolErrorCode",
    "ToolExecutionResult",
    "ToolGateway",
    "ToolRegistry",
    "default_tool_registry",
]
