from __future__ import annotations

import json
from importlib.resources import files
from types import MappingProxyType

import pytest
from chaosagent_agent_runtime import (
    AgentContext,
    AgentOutput,
    AgentOutputValidationError,
    AgentToolCall,
    AgentUsage,
    ScriptedAgentAdapter,
    execution_checkpoint_schema_v0,
    validate_execution_checkpoint,
)


def _context(step: int = 1) -> AgentContext:
    return AgentContext(
        "run-1",
        "task",
        ("instruction",),
        step,
        (),
        (),
        3,
        3,
        1000,
        100,
        True,
    )


def test_scripted_adapter_is_keyed_to_durable_step_number() -> None:
    first = AgentOutput("one", usage=AgentUsage(cost_microusd=0))
    second = AgentOutput("two", final=True, usage=AgentUsage(cost_microusd=1))
    adapter = ScriptedAgentAdapter("scripted", "1", (first, second))

    assert adapter.invoke(_context(2)) == second
    assert adapter.invoke(_context(1)) == first


def test_scripted_adapter_can_emit_tool_calls_without_mutable_provider_state() -> None:
    call = AgentToolCall(
        "lookup",
        "orders.get",
        "chaosagent.tool/orders.get/v0",
        MappingProxyType({"order_id": "ORD-1007"}),
    )
    output = AgentOutput("Checking.", (call,), usage=AgentUsage(cost_microusd=0))
    adapter = ScriptedAgentAdapter("scripted", "1", (output,))

    assert adapter.invoke(_context()).tool_calls == (call,)


def test_checkpoint_schema_is_bundled_and_unknown_version_fails_closed() -> None:
    assert (
        files("chaosagent_agent_runtime.schema")
        .joinpath("execution-checkpoint-v0.schema.json")
        .is_file()
    )
    schema = execution_checkpoint_schema_v0()
    assert schema["$id"] == "https://schemas.chaosagent.dev/execution-checkpoint/v0/schema.json"
    with pytest.raises(AgentOutputValidationError, match="unsupported"):
        validate_execution_checkpoint({"schema_version": "chaosagent.execution-checkpoint/v1"})


def test_checkpoint_rejects_unknown_properties() -> None:
    document = {
        "schema_version": "chaosagent.execution-checkpoint/v0",
        "run_id": "run-1",
        "checkpoint_version": 1,
        "lease_attempt": 1,
        "last_event_sequence": 1,
        "adapter": {"id": "scripted", "version": "1"},
        "next_step_number": 1,
        "tool_attempts": 0,
        "active_wall_time_ms": 0,
        "known_cost_microusd": 0,
        "cost_complete": True,
        "status": "active",
        "trajectory": [],
        "pending_tool_calls": [],
        "final_answer": None,
        "reasoning": "must never be persisted",
    }
    with pytest.raises(AgentOutputValidationError, match="reasoning"):
        validate_execution_checkpoint(json.loads(json.dumps(document)))
