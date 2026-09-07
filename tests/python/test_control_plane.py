"""Transport-only tests for API v1 validation, cursors, and SSE framing."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime
from typing import cast

import pytest
from chaosagent_control_plane import (
    ControlPlaneError,
    RunEventStream,
    SSEConfig,
    create_app,
    decode_cursor,
    encode_cursor,
)
from chaosagent_control_plane.service import ControlPlaneService
from chaosagent_evidence import loads_run_event
from chaosagent_persistence import RevisionReference, RunEventRecord, RunRecord, RunStatus
from fastapi.testclient import TestClient
from sqlalchemy import create_engine


def _offline_app() -> TestClient:
    engine = create_engine(
        "postgresql+psycopg://unused:unused@127.0.0.1:9/unused?connect_timeout=1"
    )
    return TestClient(create_app(engine), raise_server_exceptions=False)


def _event(run_id: str, sequence: int) -> RunEventRecord:
    payload: dict[str, object] = {
        "previous_state": "queued" if sequence == 1 else "running",
        "state": "cancelled" if sequence == 1 else "completed",
        "reason_code": "test",
    }
    document: dict[str, object] = {
        "schema_version": "chaosagent.run-event/v0",
        "event_id": f"event-{run_id}-{sequence}",
        "run_id": run_id,
        "sequence": sequence,
        "occurred_at": "2026-09-06T10:00:00.000Z",
        "recorded_at": "2026-09-06T10:00:00.000Z",
        "event_type": "run.lifecycle",
        "producer": {"component": "control-plane-test"},
        "correlation_id": run_id,
        "payload": payload,
    }
    from chaosagent_evidence import digest_payload_v0

    document["payload_digest"] = digest_payload_v0(payload)
    return RunEventRecord(loads_run_event(json.dumps(document)), datetime.now(UTC))


def _run(run_id: str, status: str) -> RunRecord:
    reference = RevisionReference("revision", "1", "sha256:" + "a" * 64)
    return RunRecord(
        run_id=run_id,
        scenario=reference,
        agent_configuration=reference,
        fixture=reference,
        fault_seed=None,
        fault_plan_digest=None,
        status=cast(RunStatus, status),
        lifecycle_version=1,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        heartbeat_at=None,
        attempt=0,
        created_at=datetime.now(UTC),
        created_by="test-suite",
    )


def test_health_openapi_and_secure_default_cors() -> None:
    with _offline_app() as client:
        response = client.get("/api/v1/health")
        assert response.status_code == 200 and response.json() == {"status": "ok"}
        schema = client.get("/openapi.json").json()
        assert "/api/v1/runs/{run_id}/events/stream" in schema["paths"]
        assert response.headers.get("access-control-allow-origin") is None

    paths = schema["paths"]
    stream = paths["/api/v1/runs/{run_id}/events/stream"]["get"]
    run_export = paths["/api/v1/runs/{run_id}/exports"]["post"]
    campaign_export = paths["/api/v1/campaigns/{campaign_id}/exports"]["post"]
    assert stream["responses"]["200"]["content"] == {"text/event-stream": {}}
    binary = {"application/zip": {"schema": {"type": "string", "format": "binary"}}}
    assert run_export["responses"]["200"]["content"] == binary
    assert campaign_export["responses"]["200"]["content"] == binary
    assert "ErrorResponse" in schema["components"]["schemas"]
    assert "HTTPValidationError" not in json.dumps(schema)
    for status in ("409", "413", "422", "500", "503"):
        reference = paths["/api/v1/runs"]["post"]["responses"][status]["content"][
            "application/json"
        ]["schema"]["$ref"]
        assert reference == "#/components/schemas/ErrorResponse"


def test_sse_database_preflight_runs_off_event_loop_and_fails_before_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_engine(
        "postgresql+psycopg://unused:unused@127.0.0.1:9/unused?connect_timeout=1"
    )
    application = create_app(engine)
    service = cast(ControlPlaneService, application.state.control_plane_service)
    called = False

    def preflight(_run_id: str, _cursor: str | None) -> None:
        nonlocal called
        called = True
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio.get_running_loop()
        raise ControlPlaneError(400, "invalid_event_cursor", "The event cursor is malformed.")

    monkeypatch.setattr(service, "preflight_event_stream", preflight)
    with TestClient(application, raise_server_exceptions=False) as client:
        response = client.get("/api/v1/runs/run-1/events/stream", params={"cursor": "bad"})
    assert called
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["error"]["code"] == "invalid_event_cursor"


def test_readiness_failure_is_sanitized() -> None:
    with _offline_app() as client:
        response = client.get("/api/v1/ready")
    assert response.status_code == 503
    assert response.json() == {
        "error": {"code": "postgres_unavailable", "message": "PostgreSQL is not ready."}
    }
    assert "127.0.0.1" not in response.text and "psycopg" not in response.text


def test_repository_failure_is_sanitized() -> None:
    with _offline_app() as client:
        response = client.get("/api/v1/runs/run-1")
    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "internal_error",
            "message": "The control-plane request could not be completed.",
        }
    }
    assert "127.0.0.1" not in response.text and "psycopg" not in response.text


def test_request_validation_and_body_limit_are_sanitized() -> None:
    with _offline_app() as client:
        invalid = client.post("/api/v1/runs", json={"unexpected": True})
        huge = client.post(
            "/api/v1/runs", content=b"x" * 1_048_577, headers={"content-type": "application/json"}
        )
        understated = client.post(
            "/api/v1/runs",
            content=iter((b"x" * 600_000, b"x" * 600_000)),
            headers={"content-type": "application/json", "content-length": "1"},
        )
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "invalid_request"
    assert "input" not in invalid.text
    assert huge.status_code == 413
    assert huge.json()["error"]["code"] == "request_body_too_large"
    assert understated.status_code == 413
    assert understated.json()["error"]["code"] == "request_body_too_large"


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-06 10:00:00Z",
        "20260906T100000+0000",
        "2026-09-06T10:00:00+00:00:30",
        "2026-09-06T10:00:00,5Z",
        "2026-09-06T10:00:00",
        "2026-09-06T10:00:00+05:60",
        "2026-09-06T10:00:00-05:60",
        "2026-09-06T10:00:00+24:00",
        "2026-09-06T10:00:00Zjunk",
        "2026-02-30T10:00:00Z",
    ],
)
def test_export_timestamp_rejects_non_rfc3339_profile(value: str) -> None:
    with pytest.raises(ControlPlaneError) as captured:
        ControlPlaneService._parse_exported_at(value)
    assert captured.value.status_code == 422
    assert captured.value.code == "invalid_export_timestamp"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-09-06T10:00:00Z", "2026-09-06T10:00:00+00:00"),
        ("2026-09-06T10:00:00.123456Z", "2026-09-06T10:00:00.123456+00:00"),
        ("2026-09-06T10:00:00+05:30", "2026-09-06T04:30:00+00:00"),
        ("2026-09-06T10:00:00+05:59", "2026-09-06T04:01:00+00:00"),
        ("2026-09-06T10:00:00-05:59", "2026-09-06T15:59:00+00:00"),
        ("2026-09-06T10:00:00-04:00", "2026-09-06T14:00:00+00:00"),
    ],
)
def test_export_timestamp_accepts_rfc3339_profile(value: str, expected: str) -> None:
    assert ControlPlaneService._parse_exported_at(value).isoformat() == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "v0.bad",
        "v1.bad!",
        "v1." + "a" * 601,
        "v1.WzFd",
        "v1." + base64.urlsafe_b64encode(b"run-1\n9007199254740992\nevent-1").rstrip(b"=").decode(),
    ],
)
def test_malformed_cursors_fail_closed(value: str) -> None:
    with pytest.raises(ControlPlaneError) as captured:
        decode_cursor(value)
    assert captured.value.code == "invalid_event_cursor"


def test_cursor_round_trip_is_stable_and_bound() -> None:
    encoded = encode_cursor("run-1", 42, "event-42")
    assert decode_cursor(encoded).run_id == "run-1"
    assert decode_cursor(encoded).sequence == 42
    assert encode_cursor("run-1", 42, "event-42") == encoded


def test_sse_frames_pages_without_gaps_and_closes_on_terminal() -> None:
    class FakeService:
        def poll_events(
            self, run_id: str, *, after_sequence: int, limit: int
        ) -> tuple[RunRecord, tuple[RunEventRecord, ...]]:
            assert run_id == "run-1" and limit == 1
            available = tuple(
                item
                for item in (_event("run-1", 1), _event("run-1", 2))
                if cast(int, item.event.to_dict()["sequence"]) > after_sequence
            )
            return _run(run_id, "completed"), available[:limit]

    async def collect() -> list[bytes]:
        streamer = RunEventStream(cast(ControlPlaneService, FakeService()), SSEConfig(page_size=1))

        async def connected() -> bool:
            return False

        return [frame async for frame in streamer.frames("run-1", None, connected)]

    frames = asyncio.run(collect())
    assert len(frames) == 2
    assert frames[0].startswith(b"id: v1.") and b"event: run.lifecycle" in frames[0]
    assert b'"sequence":1' in frames[0] and b'"sequence":2' in frames[1]


def test_sse_keepalive_has_no_event_identity() -> None:
    class FakeService:
        calls = 0

        def poll_events(
            self, run_id: str, *, after_sequence: int, limit: int
        ) -> tuple[RunRecord, tuple[RunEventRecord, ...]]:
            self.calls += 1
            return _run(run_id, "running"), ()

    service = FakeService()

    async def sleeper(_seconds: float) -> None:
        await asyncio.sleep(0.11)

    async def collect() -> list[bytes]:
        streamer = RunEventStream(
            cast(ControlPlaneService, service),
            SSEConfig(poll_interval_seconds=0.01, keepalive_seconds=0.1),
            sleeper=sleeper,
        )

        async def disconnected() -> bool:
            return service.calls >= 2

        return [frame async for frame in streamer.frames("run-1", None, disconnected)]

    frames = asyncio.run(collect())
    assert frames == [b": keepalive\n\n"]
    assert b"id:" not in frames[0] and b"event:" not in frames[0]


def test_evaluating_stream_remains_open_and_simultaneous_clients_are_independent() -> None:
    class FakeService:
        polls = 0

        def poll_events(
            self, run_id: str, *, after_sequence: int, limit: int
        ) -> tuple[RunRecord, tuple[RunEventRecord, ...]]:
            self.polls += 1
            records = () if after_sequence else (_event(run_id, 1),)
            return _run(run_id, "evaluating"), records

    service = FakeService()

    async def sleeper(_seconds: float) -> None:
        return None

    async def collect() -> tuple[list[bytes], list[bytes]]:
        async def one_client() -> list[bytes]:
            checks = 0

            async def disconnected() -> bool:
                nonlocal checks
                checks += 1
                return checks >= 3

            streamer = RunEventStream(
                cast(ControlPlaneService, service),
                SSEConfig(poll_interval_seconds=0.01),
                sleeper=sleeper,
            )
            return [frame async for frame in streamer.frames("run-1", None, disconnected)]

        first, second = await asyncio.gather(one_client(), one_client())
        return first, second

    first, second = asyncio.run(collect())
    assert len(first) == len(second) == 1
    assert b'"sequence":1' in first[0] and first == second
    assert service.polls == 2


def test_sse_disconnect_stops_before_database_poll() -> None:
    class FakeService:
        def poll_events(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("disconnected stream reached persistence")

    async def collect() -> list[bytes]:
        streamer = RunEventStream(cast(ControlPlaneService, FakeService()), SSEConfig())

        async def disconnected() -> bool:
            return True

        return [frame async for frame in streamer.frames("run-1", None, disconnected)]

    assert asyncio.run(collect()) == []
