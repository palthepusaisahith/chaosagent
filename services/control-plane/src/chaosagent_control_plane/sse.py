"""Replay-safe SSE framing backed exclusively by persisted Run Events."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import cast

from chaosagent_persistence import TERMINAL_STATUSES
from starlette.concurrency import run_in_threadpool

from .cursor import EventCursor, encode_cursor
from .service import ControlPlaneService

_EVENT_TYPE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")


@dataclass(frozen=True, slots=True)
class SSEConfig:
    page_size: int = 100
    poll_interval_seconds: float = 0.5
    keepalive_seconds: float = 15.0

    def __post_init__(self) -> None:
        if type(self.page_size) is not int or not 1 <= self.page_size <= 1_000:
            raise ValueError("SSE page_size must be between 1 and 1000")
        for value, name, minimum, maximum in (
            (self.poll_interval_seconds, "poll_interval_seconds", 0.01, 10.0),
            (self.keepalive_seconds, "keepalive_seconds", 0.1, 300.0),
        ):
            if not isinstance(value, int | float) or isinstance(value, bool):
                raise ValueError(f"SSE {name} must be numeric")
            if not minimum <= float(value) <= maximum:
                raise ValueError(f"SSE {name} is outside the safe range")


class RunEventStream:
    """Incrementally polls immutable evidence without a connection-long transaction."""

    def __init__(
        self,
        service: ControlPlaneService,
        config: SSEConfig,
        *,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._service = service
        self._config = config
        self._sleep = sleeper

    async def frames(
        self,
        run_id: str,
        cursor: EventCursor | None,
        disconnected: Callable[[], Awaitable[bool]],
    ) -> AsyncIterator[bytes]:
        after_sequence = 0 if cursor is None else cursor.sequence
        last_output = monotonic()
        while True:
            if await disconnected():
                return
            run, records = await run_in_threadpool(
                self._service.poll_events,
                run_id,
                after_sequence=after_sequence,
                limit=self._config.page_size,
            )
            for record in records:
                document = record.event.to_dict()
                sequence = cast(int, document["sequence"])
                event_id = cast(str, document["event_id"])
                event_type = cast(str, document["event_type"])
                if _EVENT_TYPE_RE.fullmatch(event_type) is None:
                    return
                yield _event_frame(encode_cursor(run_id, sequence, event_id), event_type, document)
                after_sequence = sequence
                last_output = monotonic()
                if await disconnected():
                    return
            if len(records) == self._config.page_size:
                continue
            if run.status in TERMINAL_STATUSES:
                return
            if monotonic() - last_output >= self._config.keepalive_seconds:
                yield b": keepalive\n\n"
                last_output = monotonic()
            await self._sleep(self._config.poll_interval_seconds)


def _event_frame(cursor: str, event_type: str, document: dict[str, object]) -> bytes:
    data = json.dumps(document, ensure_ascii=False, separators=(",", ":"))
    return f"id: {cursor}\nevent: {event_type}\ndata: {data}\n\n".encode()
