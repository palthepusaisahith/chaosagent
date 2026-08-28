from __future__ import annotations

import json
import socket
from collections import deque
from dataclasses import replace
from typing import cast

import httpx2
import pytest
from chaosagent_agent_configurations import (
    AgentConfigurationValidationError,
    canonicalize_agent_configuration,
    loads_agent_configuration,
)
from chaosagent_agent_runtime import (
    AgentContext,
    AgentOutputValidationError,
    AgentProviderError,
    AgentProviderTimeout,
    AgentToolSpec,
)
from chaosagent_provider_openai import OpenAIResponsesAdapter
from openai import (
    APIConnectionError,
    APIResponseValidationError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    OpenAI,
    PermissionDeniedError,
    RateLimitError,
)
from openai.types.responses.response import Response


def _configuration() -> object:
    return {
        "schema_version": "chaosagent.agent-configuration/v0",
        "agent_configuration_id": "openai-test-agent",
        "revision": "r1",
        "provider": "openai",
        "adapter": {"id": "openai-responses", "version": "v0"},
        "model": "gpt-4.1-2025-04-14",
        "compatibility_profile": "openai-responses-stateless-non-reasoning/v0",
        "token_accounting": {
            "schema_version": "chaosagent.token-accounting/v0",
            "schedule_id": "test-openai-rates",
            "revision": "2026-08-28",
            "model": "gpt-4.1-2025-04-14",
            "unit": "microusd",
            "tokens_per_rate_unit": 1000000,
            "rounding": "ceiling_per_response",
            "input_rate_microusd": 1000000,
            "cached_input_rate_microusd": 500000,
            "output_rate_microusd": 2000000,
        },
        "timeout_ms": 5000,
        "max_output_tokens": 256,
        "temperature": None,
        "parallel_tool_calls": True,
        "store": False,
        "max_retries": 0,
    }


class FakeResponses:
    def __init__(self, outputs: deque[object]) -> None:
        self.outputs = outputs
        self.requests: list[dict[str, object]] = []

    def create(self, **kwargs: object) -> object:
        self.requests.append(dict(kwargs))
        value = self.outputs.popleft()
        if isinstance(value, Exception):
            raise value
        return value


class FakeClient:
    def __init__(self, *outputs: object) -> None:
        self.responses = FakeResponses(deque(outputs))
        self.options: list[dict[str, object]] = []

    def with_options(self, **kwargs: object) -> FakeClient:
        self.options.append(dict(kwargs))
        return self


def _response(*output: object, usage: object = None) -> dict[str, object]:
    return {
        "id": "resp_test_123",
        "model": "gpt-4.1-2025-04-14",
        "status": "completed",
        "error": None,
        "output": list(output),
        "usage": usage,
    }


def _message(text: str) -> dict[str, object]:
    return {
        "type": "message",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text}],
    }


def _call(name: str, call_id: str, arguments: str) -> dict[str, object]:
    return {"type": "function_call", "name": name, "call_id": call_id, "arguments": arguments}


def _tool(tool_id: str = "orders.get") -> AgentToolSpec:
    return AgentToolSpec(
        tool_id,
        "chaosagent.tool/orders.get/v0",
        "Read an order.",
        {
            "type": "object",
            "required": ["order_id"],
            "properties": {"order_id": {"type": "string"}},
            "additionalProperties": False,
        },
    )


def _context(*, trajectory: tuple[dict[str, object], ...] = ()) -> AgentContext:
    return AgentContext(
        "run-openai-test",
        "Inspect the order.",
        ("Follow policy.",),
        1,
        trajectory,
        (_tool(),),
        5,
        5,
        2500,
        1000,
        True,
    )


def _adapter(client: FakeClient) -> OpenAIResponsesAdapter:
    configuration = loads_agent_configuration(json.dumps(_configuration()))
    return OpenAIResponsesAdapter(configuration, client=client)


@pytest.fixture(autouse=True)
def _forbid_provider_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def blocked(*args: object, **kwargs: object) -> None:
        raise AssertionError("provider tests must not open network sockets")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)


def test_configuration_is_strict_versioned_canonical_and_secret_free() -> None:
    document = cast(dict[str, object], _configuration())
    reordered = dict(reversed(list(document.items())))
    assert canonicalize_agent_configuration(document) == canonicalize_agent_configuration(reordered)
    invalid = {**document, "api_key": "sk-secret"}
    with pytest.raises(AgentConfigurationValidationError):
        canonicalize_agent_configuration(invalid)
    with pytest.raises(AgentConfigurationValidationError):
        canonicalize_agent_configuration({**document, "schema_version": "v1"})
    mismatched_accounting = json.loads(json.dumps(document))
    mismatched_accounting["token_accounting"]["model"] = "gpt-4o-2024-08-06"
    with pytest.raises(AgentConfigurationValidationError, match="must equal"):
        canonicalize_agent_configuration(mismatched_accounting)

    changed_rate = json.loads(json.dumps(document))
    changed_rate["token_accounting"]["input_rate_microusd"] = 1000001
    assert canonicalize_agent_configuration(document) != canonicalize_agent_configuration(
        changed_rate
    )
    changed_profile = {**document, "compatibility_profile": "unsupported/v1"}
    changed_adapter = json.loads(json.dumps(document))
    changed_adapter["adapter"]["version"] = "v1"
    for unsupported in (changed_profile, changed_adapter):
        with pytest.raises(AgentConfigurationValidationError):
            canonicalize_agent_configuration(unsupported)


def test_text_only_response_is_final_and_records_safe_metadata() -> None:
    client = FakeClient(
        _response(
            _message("Done"),
            usage={
                "input_tokens": 10,
                "input_tokens_details": {"cached_tokens": 2, "cache_write_tokens": 0},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 12,
            },
        )
    )
    output = _adapter(client).invoke(_context())
    assert output.text == "Done" and output.final and output.tool_calls == ()
    assert output.usage.input_tokens == 10 and output.usage.output_tokens == 2
    assert output.usage.cost_microusd == 13
    assert output.provider_metadata is not None
    assert output.provider_metadata.provider_request_id == "resp_test_123"


@pytest.mark.parametrize("with_text", [False, True])
def test_one_tool_call_and_text_plus_tool_are_nonfinal(with_text: bool) -> None:
    items: list[object] = []
    if with_text:
        items.append(_message("I will inspect it."))
    items.append(_call("chaosagent_orders__get", "provider-call-1", '{"order_id":"ORD-1007"}'))
    output = _adapter(FakeClient(_response(*items))).invoke(_context())
    assert not output.final and len(output.tool_calls) == 1
    assert output.tool_calls[0].tool_id == "orders.get"
    assert output.tool_calls[0].call_id == "provider-call-1"


def test_multiple_tool_calls_preserve_provider_order() -> None:
    context = replace(_context(), tools=(_tool(), _tool("shipping.get_status")))
    client = FakeClient(
        _response(
            _call("chaosagent_orders__get", "call-a", '{"order_id":"ORD-1007"}'),
            _call("chaosagent_shipping__get_status", "call-b", '{"order_id":"ORD-1007"}'),
        )
    )
    output = _adapter(client).invoke(context)
    assert [call.call_id for call in output.tool_calls] == ["call-a", "call-b"]


@pytest.mark.parametrize(
    "response",
    [
        _response(_call("chaosagent_orders__get", "bad", "{")),
        _response(_call("unauthorized", "bad", "{}")),
        _response(),
        _response({"type": "web_search_call"}),
        {"id": "x", "model": "x", "status": "completed", "output": "bad"},
        object(),
    ],
)
def test_malformed_or_unauthorized_provider_output_fails_closed(response: object) -> None:
    with pytest.raises(AgentOutputValidationError):
        _adapter(FakeClient(response)).invoke(_context())


def _request() -> httpx2.Request:
    return httpx2.Request("POST", "https://api.openai.com/v1/responses")


def _status_error(kind: str) -> Exception:
    request = _request()
    if kind == "auth":
        return AuthenticationError(
            "secret auth detail", response=httpx2.Response(401, request=request), body=None
        )
    if kind == "rate":
        return RateLimitError(
            "secret rate detail", response=httpx2.Response(429, request=request), body=None
        )
    if kind == "connection":
        return APIConnectionError(message="secret host", request=request)
    if kind == "bad_request":
        return BadRequestError(
            "secret invalid request", response=httpx2.Response(400, request=request), body=None
        )
    if kind == "permission":
        return PermissionDeniedError(
            "secret permission", response=httpx2.Response(403, request=request), body=None
        )
    if kind == "validation":
        return APIResponseValidationError(
            response=httpx2.Response(200, request=request),
            body={"secret": "invalid response"},
            message="secret validation",
        )
    return InternalServerError(
        "secret server", response=httpx2.Response(500, request=request), body=None
    )


def test_provider_timeout_is_sanitized() -> None:
    with pytest.raises(AgentProviderTimeout, match="timed out") as raised:
        _adapter(FakeClient(APITimeoutError(_request()))).invoke(_context())
    assert "api.openai.com" not in str(raised.value)
    assert raised.value.__cause__ is None


@pytest.mark.parametrize(
    "kind", ["auth", "rate", "connection", "bad_request", "permission", "validation", "server"]
)
def test_provider_errors_are_sanitized(kind: str) -> None:
    with pytest.raises(AgentProviderError) as raised:
        _adapter(FakeClient(_status_error(kind))).invoke(_context())
    assert "secret" not in str(raised.value) and "api.openai.com" not in str(raised.value)
    assert raised.value.__cause__ is None


def test_missing_credentials_fails_without_constructing_network_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    configuration = loads_agent_configuration(json.dumps(_configuration()))
    with pytest.raises(AgentProviderError, match="not configured"):
        OpenAIResponsesAdapter(configuration)


def test_timeout_is_bounded_by_remaining_runtime_budget_and_zero_prevents_request() -> None:
    client = FakeClient(_response(_message("Done")))
    adapter = _adapter(client)
    adapter.invoke(_context())
    assert client.options == [{"timeout": 2.5, "max_retries": 0}]
    empty_client = FakeClient(_response(_message("unused")))
    with pytest.raises(AgentProviderTimeout):
        _adapter(empty_client).invoke(replace(_context(), remaining_wall_time_ms=0))
    assert empty_client.responses.requests == []


def test_request_exposes_only_exact_allowed_schemas_and_never_remote_state() -> None:
    client = FakeClient(_response(_message("Done")))
    context = _context()
    _adapter(client).invoke(context)
    request = client.responses.requests[0]
    tools = cast(list[dict[str, object]], request["tools"])
    provider_schema = cast(dict[str, object], tools[0]["parameters"])
    assert "$schema" not in provider_schema and "$id" not in provider_schema
    order_id = cast(
        dict[str, object], cast(dict[str, object], provider_schema["properties"])["order_id"]
    )
    assert "minLength" not in order_id and "maxLength" not in order_id
    assert order_id["type"] == "string"
    assert tools[0]["strict"] is True
    assert "previous_response_id" not in request and request["store"] is False


def test_trajectory_and_approval_retry_are_reconstructed_from_durable_values() -> None:
    assistant: dict[str, object] = {
        "kind": "assistant",
        "text": "Checking",
        "tool_calls": [
            {
                "logical_call_id": "logical-1",
                "call_id": "provider-call-1",
                "tool_id": "orders.get",
                "arguments": {"order_id": "ORD-1007"},
            }
        ],
    }
    wait: dict[str, object] = {
        "kind": "tool",
        "logical_call_id": "logical-1",
        "outcome": "denied",
        "output": None,
        "error": {"code": "approval_required"},
        "approval_id": "approval-1",
    }
    approved: dict[str, object] = {
        "kind": "tool",
        "logical_call_id": "logical-1",
        "outcome": "succeeded",
        "output": {"status": "ok"},
        "error": None,
        "approval_id": "approval-1",
    }
    client = FakeClient(_response(_message("Done")))
    _adapter(client).invoke(_context(trajectory=(assistant, wait, approved)))
    provider_input = cast(list[dict[str, object]], client.responses.requests[0]["input"])
    assert any(item.get("type") == "function_call_output" for item in provider_input)
    assert any("retry/approval outcome" in json.dumps(item) for item in provider_input)


@pytest.mark.parametrize(
    "usage",
    [None, {"input_tokens": None, "output_tokens": 2}, {"input_tokens": 3}],
)
def test_unknown_usage_remains_unknown_and_cost_is_never_fabricated(usage: object) -> None:
    output = _adapter(FakeClient(_response(_message("Done"), usage=usage))).invoke(_context())
    assert output.usage.cost_microusd is None


def test_accounting_rounds_up_with_the_exact_supported_usage_shape() -> None:
    usage = {
        "input_tokens": 1,
        "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
        "output_tokens": 0,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 1,
    }
    output = _adapter(FakeClient(_response(_message("Done"), usage=usage))).invoke(_context())
    assert output.usage.cost_microusd == 1


@pytest.mark.parametrize(
    "usage",
    [
        {
            "input_tokens": 1,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 1,
        },
        {
            "input_tokens": 1,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 1,
            "future_billed_tokens": 1,
        },
        {
            "input_tokens": 1,
            "input_tokens_details": {
                "cached_tokens": 0,
                "cache_write_tokens": 0,
                "future_cache_tokens": 1,
            },
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 1,
        },
        {
            "input_tokens": 1,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0, "future_tokens": 1},
            "total_tokens": 1,
        },
        {
            "input_tokens": 1,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
            "output_tokens": 1,
            "output_tokens_details": {"reasoning_tokens": 1},
            "total_tokens": 2,
        },
        {
            "input_tokens": 1,
            "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 1},
            "output_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 1,
        },
    ],
)
def test_incomplete_or_unsupported_usage_dimensions_make_cost_unknown(
    usage: dict[str, object],
) -> None:
    output = _adapter(FakeClient(_response(_message("Done"), usage=usage))).invoke(_context())
    assert output.usage.input_tokens == usage["input_tokens"]
    assert output.usage.output_tokens == usage["output_tokens"]
    assert output.usage.cost_microusd is None


def test_accounting_overflow_fails_closed() -> None:
    usage = {
        "input_tokens": 9_007_199_254_740_991,
        "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
        "output_tokens": 9_007_199_254_740_991,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 18_014_398_509_481_982,
    }
    with pytest.raises(AgentOutputValidationError, match="cost exceeds"):
        _adapter(FakeClient(_response(_message("Done"), usage=usage))).invoke(_context())


@pytest.mark.parametrize("unsupported_model", ["gpt-5.6", "gpt-4.1"])
def test_unsupported_models_are_rejected_by_frozen_configuration_before_client_use(
    unsupported_model: str,
) -> None:
    document = cast(dict[str, object], _configuration())
    document["model"] = unsupported_model
    cast(dict[str, object], document["token_accounting"])["model"] = unsupported_model
    with pytest.raises(AgentConfigurationValidationError):
        loads_agent_configuration(json.dumps(document))


def test_unexpected_reasoning_item_fails_closed() -> None:
    with pytest.raises(AgentOutputValidationError, match="reasoning state"):
        _adapter(FakeClient(_response({"type": "reasoning", "id": "rs_1", "summary": []}))).invoke(
            _context()
        )


def test_real_sdk_response_model_is_translated() -> None:
    response = Response.model_validate(
        {
            "id": "resp_sdk_object",
            "created_at": 0,
            "model": "gpt-4.1-2025-04-14",
            "object": "response",
            "output": [
                {
                    "id": "msg_sdk_object",
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "SDK object",
                            "annotations": [],
                        }
                    ],
                }
            ],
            "parallel_tool_calls": False,
            "tool_choice": "none",
            "tools": [],
            "status": "completed",
            "error": None,
            "usage": {
                "input_tokens": 3,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 5,
            },
        }
    )
    output = _adapter(FakeClient(response)).invoke(_context())
    assert output.text == "SDK object" and output.usage.cost_microusd == 7


def test_real_sdk_function_call_response_model_is_translated() -> None:
    response = Response.model_validate(
        {
            "id": "resp_sdk_function",
            "created_at": 0,
            "model": "gpt-4.1-2025-04-14",
            "object": "response",
            "output": [
                {
                    "id": "fc_sdk_object",
                    "type": "function_call",
                    "status": "completed",
                    "call_id": "provider-call-sdk",
                    "name": "chaosagent_orders__get",
                    "arguments": '{"order_id":"ORD-1007"}',
                }
            ],
            "parallel_tool_calls": False,
            "tool_choice": "auto",
            "tools": [],
            "status": "completed",
            "error": None,
            "usage": {
                "input_tokens": 3,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 5,
            },
        }
    )
    output = _adapter(FakeClient(response)).invoke(_context())
    assert output.tool_calls == (output.tool_calls[0],)
    assert output.tool_calls[0].call_id == "provider-call-sdk"
    assert output.tool_calls[0].arguments == {"order_id": "ORD-1007"}


def test_real_sdk_serializes_all_tool_schemas_without_network() -> None:
    from chaosagent_tool_gateway import default_tool_registry

    captured: list[httpx2.Request] = []

    def respond(request: httpx2.Request) -> httpx2.Response:
        captured.append(request)
        return httpx2.Response(
            200,
            request=request,
            json={
                "id": "resp_mock_transport",
                "created_at": 0,
                "model": "gpt-4.1-2025-04-14",
                "object": "response",
                "output": [
                    {
                        "id": "msg_mock_transport",
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {
                                "type": "output_text",
                                "text": "Serialized",
                                "annotations": [],
                            }
                        ],
                    }
                ],
                "parallel_tool_calls": False,
                "tool_choice": "auto",
                "tools": [],
                "status": "completed",
                "error": None,
                "usage": {
                    "input_tokens": 1,
                    "input_tokens_details": {
                        "cached_tokens": 0,
                        "cache_write_tokens": 0,
                    },
                    "output_tokens": 1,
                    "output_tokens_details": {"reasoning_tokens": 0},
                    "total_tokens": 2,
                },
            },
        )

    definitions = default_tool_registry().definitions
    tools = tuple(
        AgentToolSpec(
            definition.tool_id,
            definition.contract_version,
            definition.description,
            definition.input_schema,
        )
        for definition in definitions
    )
    with httpx2.Client(transport=httpx2.MockTransport(respond)) as http_client:
        client = OpenAI(api_key="test-only", http_client=http_client, max_retries=0)
        adapter = OpenAIResponsesAdapter(
            loads_agent_configuration(json.dumps(_configuration())), client=client
        )
        assert adapter.invoke(replace(_context(), tools=tools)).text == "Serialized"
    assert len(captured) == 1
    serialized = cast(dict[str, object], json.loads(captured[0].content))
    assert len(cast(list[object], serialized["tools"])) == 4
    assert serialized["store"] is False


def test_all_tool_schemas_compile_and_unsupported_schema_fails_before_request() -> None:
    from chaosagent_tool_gateway import default_tool_registry

    definitions = default_tool_registry().definitions
    original_schemas = [repr(item.input_schema) for item in definitions]
    tools = tuple(
        AgentToolSpec(
            definition.tool_id,
            definition.contract_version,
            definition.description,
            definition.input_schema,
        )
        for definition in definitions
    )
    client = FakeClient(_response(_message("Done")))
    _adapter(client).invoke(replace(_context(), tools=tools))
    compiled = cast(list[dict[str, object]], client.responses.requests[0]["tools"])
    assert [tool["name"] for tool in compiled] == [
        "chaosagent_orders__get",
        "chaosagent_payments__refund",
        "chaosagent_shipping__get_status",
        "chaosagent_support__update_ticket",
    ]
    assert all(tool["strict"] is True for tool in compiled)

    def assert_provider_subset(value: object) -> None:
        if isinstance(value, dict):
            assert not ({"$schema", "$id", "title", "minLength", "maxLength"} & set(value))
            if value.get("type") == "object":
                properties = cast(dict[str, object], value["properties"])
                assert set(cast(list[str], value["required"])) == set(properties)
                assert value["additionalProperties"] is False
            for child in value.values():
                assert_provider_subset(child)
        elif isinstance(value, list):
            for child in value:
                assert_provider_subset(child)

    for tool in compiled:
        assert_provider_subset(tool["parameters"])
    assert original_schemas == [repr(item.input_schema) for item in definitions]
    unsupported = AgentToolSpec(
        "orders.get",
        "chaosagent.tool/orders.get/v0",
        "bad",
        {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
            "not": {"type": "null"},
        },
    )
    blocked_client = FakeClient(_response(_message("unused")))
    with pytest.raises(AgentProviderError, match="strict-compatible"):
        _adapter(blocked_client).invoke(replace(_context(), tools=(unsupported,)))
    assert blocked_client.responses.requests == []


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "string"},
        {"type": "array", "items": {"type": "string"}},
        {
            "anyOf": [
                {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False,
                },
                {"type": "null"},
            ]
        },
        {"type": "object", "properties": {}, "required": []},
    ],
)
def test_non_object_union_or_malformed_tool_schema_root_fails_before_request(
    schema: dict[str, object],
) -> None:
    tool = AgentToolSpec("orders.get", "chaosagent.tool/orders.get/v0", "bad", schema)
    client = FakeClient(_response(_message("unused")))
    with pytest.raises(AgentProviderError):
        _adapter(client).invoke(replace(_context(), tools=(tool,)))
    assert client.responses.requests == []
