"""Deterministic, sanitized HTTP error boundary."""

from __future__ import annotations


class ControlPlaneError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


def bad_request(code: str, message: str) -> ControlPlaneError:
    return ControlPlaneError(400, code, message)


def not_found(code: str, message: str) -> ControlPlaneError:
    return ControlPlaneError(404, code, message)


def conflict(code: str, message: str) -> ControlPlaneError:
    return ControlPlaneError(409, code, message)


def unavailable(code: str, message: str) -> ControlPlaneError:
    return ControlPlaneError(503, code, message)


def internal_error() -> ControlPlaneError:
    return ControlPlaneError(
        500, "internal_error", "The control-plane request could not be completed."
    )
