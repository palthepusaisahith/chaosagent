"""Opaque, stable Run-event cursor encoding for polling and SSE replay."""

from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass

from .errors import bad_request

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CURSOR_RE = re.compile(r"^v1\.([A-Za-z0-9_-]{1,600})$")
_MAX_SEQUENCE = 9_007_199_254_740_991


@dataclass(frozen=True, slots=True)
class EventCursor:
    run_id: str
    sequence: int
    event_id: str


def encode_cursor(run_id: str, sequence: int, event_id: str) -> str:
    if (
        _IDENTIFIER_RE.fullmatch(run_id) is None
        or _IDENTIFIER_RE.fullmatch(event_id) is None
        or type(sequence) is not int
        or not 1 <= sequence <= _MAX_SEQUENCE
    ):
        raise bad_request("invalid_event_cursor", "The event cursor is malformed.")
    material = f"{run_id}\n{sequence}\n{event_id}".encode()
    encoded = base64.urlsafe_b64encode(material).rstrip(b"=").decode("ascii")
    return f"v1.{encoded}"


def decode_cursor(value: str) -> EventCursor:
    if not isinstance(value, str) or len(value) > 605:
        raise bad_request("invalid_event_cursor", "The event cursor is malformed.")
    match = _CURSOR_RE.fullmatch(value)
    if match is None:
        raise bad_request("invalid_event_cursor", "The event cursor is malformed.")
    encoded = match.group(1)
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        material = base64.b64decode(padded, altchars=b"-_", validate=True).decode("utf-8")
        run_id, raw_sequence, event_id = material.split("\n")
        sequence = int(raw_sequence)
    except (binascii.Error, UnicodeDecodeError, ValueError) as error:
        raise bad_request("invalid_event_cursor", "The event cursor is malformed.") from error
    if (
        _IDENTIFIER_RE.fullmatch(run_id) is None
        or _IDENTIFIER_RE.fullmatch(event_id) is None
        or not 1 <= sequence <= _MAX_SEQUENCE
        or encode_cursor(run_id, sequence, event_id) != value
    ):
        raise bad_request("invalid_event_cursor", "The event cursor is malformed.")
    return EventCursor(run_id, sequence, event_id)
