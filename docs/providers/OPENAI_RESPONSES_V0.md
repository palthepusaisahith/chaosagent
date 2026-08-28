# OpenAI Responses adapter v0

## Boundary and frozen configuration

Issue #12 adds `packages/providers-openai`, the only package that imports the
official OpenAI Python SDK. The provider-neutral runtime, checkpoints, Gateway,
Policy, approvals, and effects contain no SDK objects. `AgentProviderMetadata`
is limited to safe strings common to hosted providers: provider, requested and
resolved model, and provider request ID.

`chaosagent.agent-configuration/v0` is a strict immutable OpenAI-only contract.
It freezes configuration ID/revision, provider and adapter version, an exact
model snapshot, compatibility profile, token-accounting schedule, timeout,
maximum output tokens, optional temperature, parallel-call choice,
`store=false`, and `max_retries=0`. It has no arbitrary options object and
cannot contain credentials. RFC 8785 bytes and SHA-256 identify the whole
document, including its rates. PostgreSQL migration 0007 stores the canonical
JSONB document beside its digest; legacy placeholder rows remain valid only for
scripted adapters. A hosted adapter must present the exact digest frozen by its
Run.

V0 accepts only `gpt-4.1-2025-04-14` and `gpt-4o-2024-08-06` under
`openai-responses-stateless-non-reasoning/v0`. Moving aliases and models whose
correct multi-turn use requires opaque reasoning items are rejected at
configuration load. Adding a model or another compatibility profile is a
contract change, not an implicit SDK capability probe.

## Official API assumptions

The implementation uses `openai==3.0.0` and the synchronous Responses API.
OpenAI's current
[Responses API reference](https://developers.openai.com/api/reference/cli/resources/responses/methods/create)
defines ordered output items, custom function tools, `max_output_tokens`,
`parallel_tool_calls`, response IDs/models, and optional input/output token
usage. The
[function-calling guide](https://developers.openai.com/api/docs/guides/function-calling)
defines function calls with JSON argument strings and strict schemas. The
[official quickstart](https://platform.openai.com/docs/quickstart/make-your-first-api-request)
documents the official SDK and standard `OPENAI_API_KEY` environment behavior.

Calls are non-streaming and stateless (`store=false`); the adapter never uses
`previous_response_id` or a remote Conversation. SDK automatic retries are
disabled because retry orchestration is outside Issue #12. Per-call SDK timeout
is the smaller of configured timeout and remaining runtime wall time.

## Translation

The request contains the Scenario task/instructions, remaining budgets, a
reconstruction of the safe durable trajectory, and only the `AgentContext`
tools. ChaosAgent IDs containing dots are mapped to deterministic provider-safe
function aliases such as `orders.get` to `chaosagent_orders__get`; the reverse
map exists only for that invocation. Each Tool v0 input schema is defensively
compiled into the documented strict Structured Outputs subset and sent with
`strict=true`. Object properties must all be required and objects must reject
additional properties. Unsupported keywords fail before the request; descriptive
metadata and string-length constraints unsupported by this provider boundary are
omitted from the provider schema. The original Tool schemas are never mutated,
and the Gateway still applies the complete authoritative Tool v0 schema before
any handler can run. No built-in, MCP, SQL, filesystem, shell, or network tool
is exposed.

Previous assistant text, function requests, and first tool results become
Responses input items. Later physical outcomes for the same logical call (the
approval/retry case) become an explicit user-visible ChaosAgent outcome message,
because one provider function call cannot truthfully have two function outputs.
This representation uses only durable checkpoint values and does not invent or
require opaque provider state.

Response rules are deterministic:

- text without calls is final;
- one or more function calls are ordered and non-final, whether or not text is
  also present;
- reasoning items fail closed because the v0 compatibility profile excludes
  opaque reasoning-state continuation;
- empty, unsupported, malformed, or unauthorized output fails closed;
- function argument JSON must parse to an object and is never repaired;
- provider call IDs remain correlation labels; the runtime derives authoritative
  logical and physical identities.

## Errors, usage, and secrets

SDK timeouts map to `AgentProviderTimeout`. Authentication, rate limit,
connection, response-validation, and provider status failures map to the single
sanitized `AgentProviderError`. Raw SDK messages, requests, headers, bodies,
hosts, and stack traces never enter `AgentOutput`, checkpoints, or evidence.

Input, cached-input, and output token counts come only from the response usage
object. The immutable configuration embeds an explicit versioned accounting
schedule in micro-USD per one million tokens. For each response the adapter
computes

```text
ceil((uncached_input * input_rate
    + cached_input * cached_input_rate
    + output * output_rate) / 1_000_000)
```

using integer arithmetic, then the runtime accumulates those per-response
amounts. This is deterministic experiment accounting under the frozen schedule,
not a claim about an invoice or current public pricing. Missing main counts,
missing cached/cache-write/reasoning detail counts, unknown top-level or detail
dimensions, inconsistent totals, non-zero cache-write tokens, and non-zero
reasoning tokens yield unknown cost. Because Scenario v0 has a hard cost budget,
the runtime then fails closed with `cost_unavailable`; it never silently
substitutes zero for an absent or newly introduced billing dimension. Malformed,
negative, or unsafe integer values reject the provider output instead. The
PostgreSQL end-to-end tests use the production adapter and reported fake
response usage rather than overriding its cost.

Provider metadata is also evidence, not trusted decoration. Before persisting a
completed `agent.step`, the runtime binds its provider and requested model to
the active adapter; a mismatched adapter response fails as invalid output.
Resolved model and provider response ID remain reported provenance values.

Credentials are constructor/deployment inputs only. The default client reads
`OPENAI_API_KEY`; tests inject a fake client and install an outbound-socket
guard. SDK-object compatibility is exercised with locally constructed official
SDK response models. No normal or CI test contacts OpenAI or consumes paid
tokens.

## Deferred

Live smoke testing, retries/backoff, streaming, invoice reconciliation, new
accounting schedules, reasoning-state transport, automatic heartbeats, fault
injection, evaluation, Campaigns, telemetry, and UI remain deferred.
