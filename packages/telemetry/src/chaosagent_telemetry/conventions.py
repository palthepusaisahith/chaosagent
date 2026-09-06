"""Isolated mapping to evolving OpenTelemetry GenAI semantic conventions."""

from __future__ import annotations

from typing import Final

# These are deliberately local string constants. The GenAI conventions are
# evolving, while this package's chaosagent.* vocabulary is versioned and stable.
GEN_AI_OPERATION_NAME: Final = "gen_ai.operation.name"
GEN_AI_PROVIDER_NAME: Final = "gen_ai.provider.name"
GEN_AI_REQUEST_MODEL: Final = "gen_ai.request.model"
GEN_AI_RESPONSE_MODEL: Final = "gen_ai.response.model"
GEN_AI_USAGE_INPUT_TOKENS: Final = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS: Final = "gen_ai.usage.output_tokens"
GEN_AI_TOOL_NAME: Final = "gen_ai.tool.name"
GEN_AI_TOOL_TYPE: Final = "gen_ai.tool.type"


def model_operation_attributes(provider: str) -> dict[str, str]:
    return {GEN_AI_OPERATION_NAME: "chat", GEN_AI_PROVIDER_NAME: provider}


def model_response_attributes(
    *,
    provider: str,
    requested_model: str,
    resolved_model: str | None,
    input_tokens: int | None,
    output_tokens: int | None,
) -> dict[str, str | int]:
    attributes: dict[str, str | int] = {
        GEN_AI_PROVIDER_NAME: provider,
        GEN_AI_REQUEST_MODEL: requested_model,
    }
    if resolved_model is not None:
        attributes[GEN_AI_RESPONSE_MODEL] = resolved_model
    if input_tokens is not None:
        attributes[GEN_AI_USAGE_INPUT_TOKENS] = input_tokens
    if output_tokens is not None:
        attributes[GEN_AI_USAGE_OUTPUT_TOKENS] = output_tokens
    return attributes


def tool_operation_attributes(tool_id: str) -> dict[str, str]:
    return {
        GEN_AI_OPERATION_NAME: "execute_tool",
        GEN_AI_TOOL_NAME: tool_id,
        GEN_AI_TOOL_TYPE: "function",
    }
