"""Bounded startup configuration for the control-plane process."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from chaosagent_evaluators import GroundTruth, load_ground_truth_v0


@dataclass(frozen=True, slots=True)
class ControlPlaneSettings:
    database_url: str
    ground_truth_paths: tuple[Path, ...] = ()
    cors_origins: tuple[str, ...] = ()
    sse_page_size: int = 100
    sse_poll_interval_seconds: float = 0.5
    sse_keepalive_seconds: float = 15.0
    max_request_body_bytes: int = 1_048_576

    @classmethod
    def from_environment(cls) -> ControlPlaneSettings:
        database_url = os.environ.get("CHAOSAGENT_DATABASE_URL", "").strip()
        if not database_url:
            raise RuntimeError("CHAOSAGENT_DATABASE_URL is required")
        truth_value = os.environ.get("CHAOSAGENT_GROUND_TRUTH_PATHS", "")
        origin_value = os.environ.get("CHAOSAGENT_CORS_ORIGINS", "")
        try:
            page_size = int(os.environ.get("CHAOSAGENT_SSE_PAGE_SIZE", "100"))
            poll = float(os.environ.get("CHAOSAGENT_SSE_POLL_SECONDS", "0.5"))
            keepalive = float(os.environ.get("CHAOSAGENT_SSE_KEEPALIVE_SECONDS", "15"))
            body_bytes = int(os.environ.get("CHAOSAGENT_MAX_REQUEST_BODY_BYTES", "1048576"))
        except ValueError as error:
            raise RuntimeError("invalid control-plane numeric configuration") from error
        return cls(
            database_url=database_url,
            ground_truth_paths=tuple(
                Path(item.strip()) for item in truth_value.split(os.pathsep) if item.strip()
            ),
            cors_origins=tuple(item.strip() for item in origin_value.split(",") if item.strip()),
            sse_page_size=page_size,
            sse_poll_interval_seconds=poll,
            sse_keepalive_seconds=keepalive,
            max_request_body_bytes=body_bytes,
        )

    def load_ground_truths(self) -> tuple[GroundTruth, ...]:
        try:
            return tuple(load_ground_truth_v0(path) for path in self.ground_truth_paths)
        except Exception as error:
            raise RuntimeError("configured Ground Truth revisions could not be loaded") from error
