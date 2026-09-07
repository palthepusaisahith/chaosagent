"""FastAPI transport for the ChaosAgent control plane."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, cast

from chaosagent_evaluators import GroundTruth
from chaosagent_persistence import create_postgres_engine
from fastapi import FastAPI, Header, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import Engine
from starlette.concurrency import run_in_threadpool
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .errors import ControlPlaneError
from .models import (
    AgentConfigurationCreateRequest,
    AgentConfigurationResponse,
    ApprovalListResponse,
    ApprovalResolutionResponse,
    ApprovalResolveRequest,
    CampaignComparisonResponse,
    CampaignCreateRequest,
    CampaignResponse,
    CampaignStatisticsResponse,
    CancellationResponse,
    ErrorResponse,
    EventPageResponse,
    ExportRequest,
    HealthResponse,
    ReadyResponse,
    ReportResponse,
    RunCreateRequest,
    RunResponse,
    ScenarioCreateRequest,
    ScenarioResponse,
)
from .service import ControlPlaneService, map_unhandled_error
from .settings import ControlPlaneSettings
from .sse import RunEventStream, SSEConfig

API_PREFIX = "/api/v1"
IdentifierPath = Annotated[
    str,
    Path(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"),
]
RevisionPath = Annotated[
    str,
    Path(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"),
]

_ERROR_DESCRIPTIONS = {
    400: "Malformed or conflicting request locator",
    404: "Requested resource was not found",
    409: "Request conflicts with authoritative state",
    413: "Request body exceeds the configured limit",
    422: "Request or domain contract validation failed",
    500: "Persisted state is inconsistent or processing failed",
    503: "Required persistence is unavailable",
}


def _error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {
        status_code: {
            "model": ErrorResponse,
            "description": _ERROR_DESCRIPTIONS[status_code],
        }
        for status_code in status_codes
    }


class RequestBodyLimitMiddleware:
    """Reject oversized fixed or streamed request bodies before domain parsing."""

    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        raw_length = headers.get(b"content-length")
        if raw_length is not None:
            try:
                if int(raw_length) > self.max_bytes:
                    await self._reject(send)
                    return
            except ValueError:
                await self._reject(send)
                return
        if scope.get("method") not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, send)
            return
        messages: list[Message] = []
        consumed = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                messages.append(message)
                break
            consumed += len(message.get("body", b""))
            if consumed > self.max_bytes:
                await self._reject(send)
                return
            messages.append(message)
            if not message.get("more_body", False):
                break
        index = 0

        async def replay_receive() -> Message:
            nonlocal index
            if index < len(messages):
                message = messages[index]
                index += 1
                return message
            return {"type": "http.disconnect"}

        await self.app(scope, replay_receive, send)

    @staticmethod
    async def _reject(send: Send) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": "request_body_too_large",
                    "message": "The request body exceeds the configured limit.",
                }
            },
            separators=(",", ":"),
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def create_app(
    engine: Engine,
    *,
    ground_truths: tuple[GroundTruth, ...] = (),
    sse_config: SSEConfig | None = None,
    cors_origins: tuple[str, ...] = (),
    max_request_body_bytes: int = 1_048_576,
) -> FastAPI:
    """Create an API adapter around a caller-owned database engine."""
    if type(max_request_body_bytes) is not int or not 1_024 <= max_request_body_bytes <= 16_777_216:
        raise ValueError("max_request_body_bytes is outside the safe range")
    service = ControlPlaneService(engine, ground_truths=ground_truths)
    stream = RunEventStream(service, sse_config or SSEConfig())
    application = FastAPI(
        title="ChaosAgent Control Plane",
        version="1.0.0",
        description="Provider-neutral REST and PostgreSQL-authoritative Run Event SSE.",
    )
    application.state.control_plane_service = service
    application.add_middleware(RequestBodyLimitMiddleware, max_bytes=max_request_body_bytes)
    if cors_origins:
        application.add_middleware(
            CORSMiddleware,
            allow_origins=list(cors_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST"],
            allow_headers=["Content-Type", "Last-Event-ID"],
        )

    @application.exception_handler(ControlPlaneError)
    async def control_plane_error_handler(
        _request: Request, error: ControlPlaneError
    ) -> JSONResponse:
        content: dict[str, object] = {"error": {"code": error.code, "message": error.message}}
        if error.details is not None:
            cast(dict[str, object], content["error"])["details"] = error.details
        return JSONResponse(status_code=error.status_code, content=content)

    @application.exception_handler(RequestValidationError)
    async def request_validation_error_handler(
        _request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "invalid_request",
                    "message": "The HTTP request is invalid.",
                }
            },
        )

    @application.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, error: Exception) -> JSONResponse:
        mapped = map_unhandled_error(error)
        return JSONResponse(
            status_code=mapped.status_code,
            content={"error": {"code": mapped.code, "message": mapped.message}},
        )

    @application.get(f"{API_PREFIX}/health", response_model=HealthResponse, tags=["system"])
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @application.get(
        f"{API_PREFIX}/ready",
        response_model=ReadyResponse,
        responses=_error_responses(500, 503),
        tags=["system"],
    )
    def ready() -> ReadyResponse:
        service.ready()
        return ReadyResponse(status="ready")

    @application.post(
        f"{API_PREFIX}/scenarios",
        response_model=ScenarioResponse,
        status_code=201,
        responses=_error_responses(409, 413, 422, 500, 503),
        tags=["scenarios"],
    )
    def create_scenario(request: ScenarioCreateRequest) -> ScenarioResponse:
        return service.create_scenario(request)

    @application.get(
        f"{API_PREFIX}/scenarios/{{scenario_id}}/revisions/{{revision}}",
        response_model=ScenarioResponse,
        responses=_error_responses(404, 422, 500, 503),
        tags=["scenarios"],
    )
    def get_scenario(scenario_id: IdentifierPath, revision: RevisionPath) -> ScenarioResponse:
        return service.get_scenario(scenario_id, revision)

    @application.post(
        f"{API_PREFIX}/agent-configs",
        response_model=AgentConfigurationResponse,
        status_code=201,
        responses=_error_responses(409, 413, 422, 500, 503),
        tags=["agent-configs"],
    )
    def create_agent_configuration(
        request: AgentConfigurationCreateRequest,
    ) -> AgentConfigurationResponse:
        return service.create_agent_configuration(request)

    @application.get(
        f"{API_PREFIX}/agent-configs/{{configuration_id}}/revisions/{{revision}}",
        response_model=AgentConfigurationResponse,
        responses=_error_responses(404, 409, 422, 500, 503),
        tags=["agent-configs"],
    )
    def get_agent_configuration(
        configuration_id: IdentifierPath, revision: RevisionPath
    ) -> AgentConfigurationResponse:
        return service.get_agent_configuration(configuration_id, revision)

    @application.post(
        f"{API_PREFIX}/runs",
        response_model=RunResponse,
        status_code=201,
        responses=_error_responses(409, 413, 422, 500, 503),
        tags=["runs"],
    )
    def create_run(request: RunCreateRequest) -> RunResponse:
        return service.create_run(request)

    @application.get(
        f"{API_PREFIX}/runs/{{run_id}}",
        response_model=RunResponse,
        responses=_error_responses(404, 422, 500, 503),
        tags=["runs"],
    )
    def get_run(run_id: IdentifierPath) -> RunResponse:
        return service.get_run(run_id)

    @application.post(
        f"{API_PREFIX}/runs/{{run_id}}/cancel",
        response_model=CancellationResponse,
        responses=_error_responses(404, 409, 422, 500, 503),
        tags=["runs"],
    )
    def cancel_run(run_id: IdentifierPath) -> CancellationResponse:
        return service.cancel_run(run_id)

    @application.get(
        f"{API_PREFIX}/runs/{{run_id}}/events",
        response_model=EventPageResponse,
        responses=_error_responses(400, 404, 422, 500, 503),
        tags=["events"],
    )
    def get_events(
        run_id: IdentifierPath,
        cursor: Annotated[str | None, Query(max_length=605)] = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> EventPageResponse:
        return service.event_page(run_id, cursor, limit)

    @application.get(
        f"{API_PREFIX}/runs/{{run_id}}/events/stream",
        response_class=StreamingResponse,
        responses={
            200: {
                "description": "Persisted Run Events in sequence order",
                "content": {"text/event-stream": {}},
            },
            **_error_responses(400, 404, 422, 500, 503),
        },
        tags=["events"],
    )
    async def stream_events(
        request: Request,
        run_id: IdentifierPath,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID", max_length=605)] = None,
        cursor: Annotated[str | None, Query(max_length=605)] = None,
    ) -> StreamingResponse:
        if last_event_id is not None and cursor is not None and last_event_id != cursor:
            raise ControlPlaneError(
                400, "conflicting_event_cursors", "Header and query cursors disagree."
            )
        selected = last_event_id if last_event_id is not None else cursor
        decoded = await run_in_threadpool(service.preflight_event_stream, run_id, selected)

        async def safe_frames() -> AsyncIterator[bytes]:
            try:
                async for frame in stream.frames(run_id, decoded, request.is_disconnected):
                    yield frame
            except Exception:
                return

        return StreamingResponse(
            safe_frames(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    @application.get(
        f"{API_PREFIX}/runs/{{run_id}}/report",
        response_model=ReportResponse,
        responses=_error_responses(404, 409, 422, 500, 503),
        tags=["reports"],
    )
    def get_report(run_id: IdentifierPath) -> ReportResponse:
        return service.get_report(run_id)

    @application.get(
        f"{API_PREFIX}/runs/{{run_id}}/approvals",
        response_model=ApprovalListResponse,
        responses=_error_responses(404, 422, 500, 503),
        tags=["approvals"],
    )
    def list_approvals(run_id: IdentifierPath) -> ApprovalListResponse:
        return ApprovalListResponse(run_id=run_id, approvals=service.list_approvals(run_id))

    @application.post(
        f"{API_PREFIX}/approvals/{{approval_id}}/resolve",
        response_model=ApprovalResolutionResponse,
        responses=_error_responses(404, 409, 413, 422, 500, 503),
        tags=["approvals"],
    )
    def resolve_approval(
        approval_id: IdentifierPath, request: ApprovalResolveRequest
    ) -> ApprovalResolutionResponse:
        approval, repeated = service.resolve_approval(
            approval_id, result=request.result, actor_id=request.actor_id
        )
        return ApprovalResolutionResponse(approval=approval, already_resolved=repeated)

    @application.post(
        f"{API_PREFIX}/campaigns",
        response_model=CampaignResponse,
        status_code=201,
        responses=_error_responses(409, 413, 422, 500, 503),
        tags=["campaigns"],
    )
    def create_campaign(request: CampaignCreateRequest) -> CampaignResponse:
        return service.create_campaign(request)

    @application.get(
        f"{API_PREFIX}/campaigns/{{campaign_id}}",
        response_model=CampaignResponse,
        responses=_error_responses(404, 422, 500, 503),
        tags=["campaigns"],
    )
    def get_campaign(campaign_id: IdentifierPath) -> CampaignResponse:
        return service.get_campaign(campaign_id)

    @application.get(
        f"{API_PREFIX}/campaigns/{{campaign_id}}/statistics",
        response_model=CampaignStatisticsResponse,
        responses=_error_responses(409, 422, 500, 503),
        tags=["campaigns"],
    )
    def campaign_statistics(
        campaign_id: IdentifierPath,
        k: Annotated[list[int], Query(min_length=1, max_length=32)] = [1],
    ) -> CampaignStatisticsResponse:
        statistics = service.campaign_statistics(campaign_id, tuple(k))
        return CampaignStatisticsResponse(
            campaign_id=campaign_id,
            digest=statistics.digest,
            document=statistics.to_dict(),
        )

    @application.get(
        f"{API_PREFIX}/campaigns/{{baseline_campaign_id}}/compare/{{faulted_campaign_id}}",
        response_model=CampaignComparisonResponse,
        responses=_error_responses(409, 422, 500, 503),
        tags=["campaigns"],
    )
    def campaign_comparison(
        baseline_campaign_id: IdentifierPath,
        faulted_campaign_id: IdentifierPath,
        k: Annotated[list[int], Query(min_length=1, max_length=32)] = [1],
    ) -> CampaignComparisonResponse:
        comparison = service.campaign_comparison(
            baseline_campaign_id, faulted_campaign_id, tuple(k)
        )
        return CampaignComparisonResponse(
            baseline_campaign_id=baseline_campaign_id,
            faulted_campaign_id=faulted_campaign_id,
            digest=comparison.digest,
            document=comparison.to_dict(),
        )

    @application.post(
        f"{API_PREFIX}/runs/{{run_id}}/exports",
        response_class=Response,
        responses={
            200: {
                "description": "Deterministic Run export bundle",
                "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}},
            },
            **_error_responses(404, 409, 413, 422, 500, 503),
        },
        tags=["exports"],
    )
    def run_export(run_id: IdentifierPath, request: ExportRequest) -> Response:
        data = service.export_run(run_id, request.exported_at)
        return Response(
            data,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="chaosagent-run-export.zip"'},
        )

    @application.post(
        f"{API_PREFIX}/campaigns/{{campaign_id}}/exports",
        response_class=Response,
        responses={
            200: {
                "description": "Deterministic Campaign export bundle",
                "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}},
            },
            **_error_responses(404, 409, 413, 422, 500, 503),
        },
        tags=["exports"],
    )
    def campaign_export(campaign_id: IdentifierPath, request: ExportRequest) -> Response:
        data = service.export_campaign(campaign_id, request.exported_at, tuple(request.k_values))
        return Response(
            data,
            media_type="application/zip",
            headers={
                "Content-Disposition": 'attachment; filename="chaosagent-campaign-export.zip"'
            },
        )

    return application


def create_app_from_environment() -> FastAPI:
    """Uvicorn factory that owns its configured Engine for process lifetime."""
    settings = ControlPlaneSettings.from_environment()
    engine = create_postgres_engine(settings.database_url)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> Any:
        yield
        engine.dispose()

    application = create_app(
        engine,
        ground_truths=settings.load_ground_truths(),
        sse_config=SSEConfig(
            page_size=settings.sse_page_size,
            poll_interval_seconds=settings.sse_poll_interval_seconds,
            keepalive_seconds=settings.sse_keepalive_seconds,
        ),
        cors_origins=settings.cors_origins,
        max_request_body_bytes=settings.max_request_body_bytes,
    )
    application.router.lifespan_context = lifespan
    return application
