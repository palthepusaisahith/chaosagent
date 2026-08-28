"""Stateless translation between ChaosAgent and OpenAI Responses."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Protocol, cast

from chaosagent_agent_configurations import AgentConfiguration
from chaosagent_agent_runtime import (
    AgentContext,
    AgentOutput,
    AgentOutputValidationError,
    AgentProviderError,
    AgentProviderMetadata,
    AgentProviderTimeout,
    AgentToolCall,
    AgentUsage,
)
from openai import (
    APIConnectionError,
    APIResponseValidationError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    OpenAI,
    OpenAIError,
    RateLimitError,
)

_SAFE_INTEGER = 9_007_199_254_740_991
_PROVIDER_SCHEMA_DROPPED_KEYWORDS = frozenset({"$schema", "$id", "title", "minLength", "maxLength"})
_PROVIDER_SCHEMA_SUPPORTED_KEYWORDS = frozenset(
    {
        "$defs",
        "$ref",
        "additionalProperties",
        "anyOf",
        "description",
        "enum",
        "exclusiveMaximum",
        "exclusiveMinimum",
        "format",
        "items",
        "maximum",
        "maxItems",
        "minimum",
        "minItems",
        "multipleOf",
        "pattern",
        "properties",
        "required",
        "type",
    }
)


class _Responses(Protocol):
    def create(self, **kwargs: object) -> object: ...


class _Client(Protocol):
    @property
    def responses(self) -> _Responses: ...

    def with_options(self, **kwargs: object) -> _Client: ...


class OpenAIResponsesAdapter:
    """Synchronous, stateless Responses API implementation of AgentAdapter."""

    def __init__(
        self,
        configuration: AgentConfiguration,
        *,
        client: _Client | OpenAI | None = None,
        api_key: str | None = None,
    ) -> None:
        document = configuration.to_dict()
        self._configuration = configuration
        self._document = document
        self._accounting = cast(dict[str, object], document["token_accounting"])
        if client is None:
            credential = api_key if api_key is not None else os.environ.get("OPENAI_API_KEY")
            if not credential:
                raise AgentProviderError("OpenAI credentials are not configured")
            try:
                client = cast(_Client, OpenAI(api_key=credential, max_retries=0))
            except OpenAIError:
                raise AgentProviderError("OpenAI client configuration failed") from None
        self._client = cast(_Client, client)

    @property
    def adapter_id(self) -> str:
        return cast(str, self._document["agent_configuration_id"])

    @property
    def adapter_version(self) -> str:
        return cast(str, self._document["revision"])

    @property
    def configuration_digest(self) -> str:
        return self._configuration.digest

    @property
    def provider_name(self) -> str:
        return "openai"

    @property
    def requested_model(self) -> str:
        return cast(str, self._document["model"])

    def invoke(self, context: AgentContext) -> AgentOutput:
        remaining = context.remaining_wall_time_ms
        if type(remaining) is not int or remaining <= 0:
            raise AgentProviderTimeout("provider time budget is exhausted")
        timeout_seconds = min(cast(int, self._document["timeout_ms"]), remaining) / 1000
        request = self._request(context)
        try:
            response = self._client.with_options(
                timeout=timeout_seconds,
                max_retries=cast(int, self._document["max_retries"]),
            ).responses.create(**request)
        except APITimeoutError:
            raise AgentProviderTimeout("OpenAI request timed out") from None
        except (
            AuthenticationError,
            RateLimitError,
            APIConnectionError,
            APIResponseValidationError,
            APIStatusError,
            OpenAIError,
        ):
            raise AgentProviderError("OpenAI request failed") from None
        return self._translate_response(response, context)

    def _request(self, context: AgentContext) -> dict[str, object]:
        tools, _ = _tool_maps(context)
        request: dict[str, object] = {
            "model": self._document["model"],
            "instructions": _instructions(context),
            "input": _trajectory_input(context),
            "tools": tools,
            "tool_choice": "auto" if tools else "none",
            "parallel_tool_calls": self._document["parallel_tool_calls"],
            "max_output_tokens": self._document["max_output_tokens"],
            "store": self._document["store"],
        }
        if self._document["temperature"] is not None:
            request["temperature"] = self._document["temperature"]
        return request

    def _translate_response(self, response: object, context: AgentContext) -> AgentOutput:
        document = _response_document(response)
        if document.get("status") != "completed" or document.get("error") is not None:
            raise AgentProviderError("OpenAI returned an unsuccessful response")
        output = document.get("output")
        if not isinstance(output, list):
            raise AgentOutputValidationError("OpenAI response output is malformed")
        _, aliases = _tool_maps(context)
        text_parts: list[str] = []
        calls: list[AgentToolCall] = []
        for item in output:
            if not isinstance(item, dict) or not isinstance(item.get("type"), str):
                raise AgentOutputValidationError("OpenAI response item is malformed")
            item_type = item["type"]
            if item_type == "reasoning":
                raise AgentOutputValidationError(
                    "OpenAI returned reasoning state outside the configured compatibility profile"
                )
            if item_type == "message":
                _collect_message_text(item, text_parts)
                continue
            if item_type == "function_call":
                calls.append(_function_call(item, aliases))
                continue
            raise AgentOutputValidationError("OpenAI returned an unsupported output item")
        text = "".join(text_parts)
        if not text and not calls:
            raise AgentOutputValidationError("OpenAI returned no public text or tool calls")
        usage = _usage(document.get("usage"), self._accounting)
        response_id = document.get("id")
        resolved_model = document.get("model")
        if not isinstance(response_id, str) or not isinstance(resolved_model, str):
            raise AgentOutputValidationError("OpenAI response identity is malformed")
        metadata = AgentProviderMetadata(
            "openai",
            cast(str, self._document["model"]),
            resolved_model,
            response_id,
        )
        return AgentOutput(text, tuple(calls), not calls, usage, metadata)


def _response_document(response: object) -> dict[str, object]:
    if isinstance(response, Mapping):
        plain = _plain_json(response)
        if not isinstance(plain, dict):
            raise AgentOutputValidationError("OpenAI response object is malformed")
        return plain
    model_dump = getattr(response, "model_dump", None)
    if not callable(model_dump):
        raise AgentOutputValidationError("OpenAI response object is malformed")
    dumped = model_dump(mode="json")
    if not isinstance(dumped, dict):
        raise AgentOutputValidationError("OpenAI response object is malformed")
    return cast(dict[str, object], dumped)


def _tool_alias(tool_id: str) -> str:
    return "chaosagent_" + tool_id.replace(".", "__").replace("-", "_")


def _tool_maps(context: AgentContext) -> tuple[list[dict[str, object]], dict[str, object]]:
    tools: list[dict[str, object]] = []
    aliases: dict[str, object] = {}
    for tool in context.tools:
        alias = _tool_alias(tool.tool_id)
        if alias in aliases:
            raise AgentProviderError("authorized tool aliases collide")
        aliases[alias] = tool
        tools.append(
            {
                "type": "function",
                "name": alias,
                "description": tool.description,
                "parameters": _openai_tool_schema(tool.input_schema),
                "strict": True,
            }
        )
    return tools, aliases


def _instructions(context: AgentContext) -> str:
    lines = [
        "You are executing one ChaosAgent scenario. Use only the supplied functions.",
        f"Task: {context.task}",
        *context.instructions,
        (
            "Remaining budgets: "
            f"steps={context.remaining_steps}, tools={context.remaining_tool_calls}, "
            f"wall_ms={context.remaining_wall_time_ms}."
        ),
    ]
    return "\n".join(lines)


def _trajectory_input(context: AgentContext) -> list[dict[str, object]]:
    items: list[dict[str, object]] = [
        {"role": "user", "content": [{"type": "input_text", "text": context.task}]}
    ]
    call_ids: dict[str, str] = {}
    result_counts: dict[str, int] = {}
    for turn in context.trajectory:
        kind = turn.get("kind")
        if kind == "assistant":
            text = turn.get("text")
            if isinstance(text, str) and text:
                items.append(
                    {"role": "assistant", "content": [{"type": "output_text", "text": text}]}
                )
            calls = turn.get("tool_calls")
            if not isinstance(calls, list | tuple):
                raise AgentProviderError("durable trajectory is malformed")
            for call in calls:
                if not isinstance(call, Mapping):
                    raise AgentProviderError("durable trajectory is malformed")
                logical_id = call.get("logical_call_id")
                call_id = call.get("call_id")
                tool_id = call.get("tool_id")
                arguments = call.get("arguments")
                if not all(isinstance(value, str) for value in (logical_id, call_id, tool_id)):
                    raise AgentProviderError("durable trajectory is malformed")
                assert isinstance(logical_id, str)
                assert isinstance(call_id, str)
                assert isinstance(tool_id, str)
                call_ids[logical_id] = call_id
                items.append(
                    {
                        "type": "function_call",
                        "call_id": call_id,
                        "name": _tool_alias(tool_id),
                        "arguments": json.dumps(
                            _plain_json(arguments), sort_keys=True, separators=(",", ":")
                        ),
                    }
                )
            continue
        if kind != "tool":
            raise AgentProviderError("durable trajectory is malformed")
        logical = turn.get("logical_call_id")
        if not isinstance(logical, str) or logical not in call_ids:
            raise AgentProviderError("durable trajectory is malformed")
        count = result_counts.get(logical, 0)
        result_counts[logical] = count + 1
        outcome = {
            "outcome": turn.get("outcome"),
            "output": turn.get("output"),
            "error": turn.get("error"),
            "approval_id": turn.get("approval_id"),
        }
        encoded = json.dumps(_plain_json(outcome), sort_keys=True, separators=(",", ":"))
        if count == 0:
            items.append(
                {"type": "function_call_output", "call_id": call_ids[logical], "output": encoded}
            )
        else:
            items.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": "ChaosAgent retry/approval outcome: " + encoded,
                        }
                    ],
                }
            )
    return items


def _collect_message_text(item: dict[str, object], target: list[str]) -> None:
    if item.get("role") != "assistant" or not isinstance(item.get("content"), list):
        raise AgentOutputValidationError("OpenAI message output is malformed")
    for content in cast(list[object], item["content"]):
        if not isinstance(content, dict) or content.get("type") != "output_text":
            raise AgentOutputValidationError("OpenAI message content is unsupported")
        text = content.get("text")
        if not isinstance(text, str):
            raise AgentOutputValidationError("OpenAI text output is malformed")
        target.append(text)


def _function_call(item: dict[str, object], aliases: dict[str, object]) -> AgentToolCall:
    name = item.get("name")
    call_id = item.get("call_id")
    raw_arguments = item.get("arguments")
    if (
        not isinstance(name, str)
        or not isinstance(call_id, str)
        or not isinstance(raw_arguments, str)
    ):
        raise AgentOutputValidationError("OpenAI function call is malformed")
    tool = aliases.get(name)
    if tool is None:
        raise AgentOutputValidationError("OpenAI requested an unauthorized function")
    try:
        arguments = json.loads(raw_arguments)
    except json.JSONDecodeError as error:
        raise AgentOutputValidationError("OpenAI function arguments are malformed JSON") from error
    if not isinstance(arguments, dict):
        raise AgentOutputValidationError("OpenAI function arguments must be an object")
    return AgentToolCall(
        call_id,
        cast(str, getattr(tool, "tool_id")),
        cast(str, getattr(tool, "contract_version")),
        arguments,
    )


def _usage(value: object, accounting: dict[str, object]) -> AgentUsage:
    if value is None:
        return AgentUsage(None, None, None)
    if not isinstance(value, dict):
        raise AgentOutputValidationError("OpenAI usage is malformed")
    input_tokens = value.get("input_tokens")
    output_tokens = value.get("output_tokens")
    for count in (input_tokens, output_tokens):
        if count is not None and (type(count) is not int or not 0 <= count <= _SAFE_INTEGER):
            raise AgentOutputValidationError("OpenAI token usage is invalid")
    if input_tokens is None or output_tokens is None:
        return AgentUsage(cast(int | None, input_tokens), cast(int | None, output_tokens), None)
    assert isinstance(input_tokens, int) and isinstance(output_tokens, int)
    if set(value) != {
        "input_tokens",
        "input_tokens_details",
        "output_tokens",
        "output_tokens_details",
        "total_tokens",
    }:
        return AgentUsage(input_tokens, output_tokens, None)
    input_details = value.get("input_tokens_details")
    output_details = value.get("output_tokens_details")
    if not isinstance(input_details, dict) or not isinstance(output_details, dict):
        return AgentUsage(input_tokens, output_tokens, None)
    if set(input_details) != {"cached_tokens", "cache_write_tokens"} or set(output_details) != {
        "reasoning_tokens"
    }:
        return AgentUsage(input_tokens, output_tokens, None)
    cached_tokens = input_details.get("cached_tokens")
    cache_write_tokens = input_details.get("cache_write_tokens")
    reasoning_tokens = output_details.get("reasoning_tokens")
    for count in (cached_tokens, cache_write_tokens, reasoning_tokens):
        if type(count) is not int or not 0 <= count <= _SAFE_INTEGER:
            raise AgentOutputValidationError("OpenAI token usage details are invalid")
    assert isinstance(cached_tokens, int)
    assert isinstance(cache_write_tokens, int)
    assert isinstance(reasoning_tokens, int)
    if cached_tokens > input_tokens:
        raise AgentOutputValidationError("OpenAI cached token usage exceeds input usage")
    if cache_write_tokens != 0 or reasoning_tokens != 0:
        return AgentUsage(input_tokens, output_tokens, None)
    total_tokens = value.get("total_tokens")
    if type(total_tokens) is not int or total_tokens != input_tokens + output_tokens:
        return AgentUsage(input_tokens, output_tokens, None)
    uncached_tokens = input_tokens - cached_tokens
    weighted = (
        uncached_tokens * cast(int, accounting["input_rate_microusd"])
        + cached_tokens * cast(int, accounting["cached_input_rate_microusd"])
        + output_tokens * cast(int, accounting["output_rate_microusd"])
    )
    rate_unit = cast(int, accounting["tokens_per_rate_unit"])
    cost = (weighted + rate_unit - 1) // rate_unit
    if cost > _SAFE_INTEGER:
        raise AgentOutputValidationError("OpenAI accounted cost exceeds the supported range")
    return AgentUsage(input_tokens, output_tokens, cost)


def _openai_tool_schema(value: object) -> dict[str, object]:
    """Compile the authoritative Tool schema to OpenAI's strict supported subset."""
    compiled = _compile_schema_node(value, path="$")
    if not isinstance(compiled, dict):
        raise AgentProviderError("authorized tool schema root is invalid")
    if compiled.get("type") != "object" or "anyOf" in compiled:
        raise AgentProviderError("authorized tool schema root must be an object")
    return compiled


def _compile_schema_node(value: object, *, path: str) -> object:
    if isinstance(value, Mapping):
        source = dict(value)
        unsupported = (
            set(source) - _PROVIDER_SCHEMA_SUPPORTED_KEYWORDS - _PROVIDER_SCHEMA_DROPPED_KEYWORDS
        )
        if unsupported:
            raise AgentProviderError("authorized tool schema is not OpenAI strict-compatible")
        compiled: dict[str, object] = {}
        for key, item in source.items():
            if key in _PROVIDER_SCHEMA_DROPPED_KEYWORDS:
                continue
            if key in {"properties", "$defs"}:
                if not isinstance(item, Mapping):
                    raise AgentProviderError("authorized tool schema is malformed")
                compiled[key] = {
                    str(name): _compile_schema_node(child, path=f"{path}.{key}.{name}")
                    for name, child in item.items()
                }
            elif key in {"items"}:
                compiled[key] = _compile_schema_node(item, path=f"{path}.{key}")
            elif key == "anyOf":
                if not isinstance(item, list) or not item:
                    raise AgentProviderError("authorized tool schema is malformed")
                compiled[key] = [
                    _compile_schema_node(child, path=f"{path}.anyOf") for child in item
                ]
            else:
                compiled[key] = _plain_json(item)
        if "enum" in compiled and "type" not in compiled:
            enum_values = compiled["enum"]
            if (
                isinstance(enum_values, list)
                and enum_values
                and all(isinstance(item, str) for item in enum_values)
            ):
                compiled["type"] = "string"
            else:
                raise AgentProviderError("authorized tool enum has no representable strict type")
        if compiled.get("type") == "object":
            properties = compiled.get("properties")
            required = compiled.get("required")
            if (
                not isinstance(properties, dict)
                or not isinstance(required, list)
                or set(required) != set(properties)
                or compiled.get("additionalProperties") is not False
            ):
                raise AgentProviderError("authorized tool object is not OpenAI strict-compatible")
        reference = compiled.get("$ref")
        if reference is not None and (
            not isinstance(reference, str) or not reference.startswith("#/$defs/")
        ):
            raise AgentProviderError("authorized tool schema reference is unsupported")
        return compiled
    if isinstance(value, list):
        return [_compile_schema_node(item, path=path) for item in value]
    return value


def _plain_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain_json(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain_json(item) for item in value]
    return value
