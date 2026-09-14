"""Deterministic local orchestration for the ChaosAgent flagship experiment."""

from .runner import (
    FlagshipDemoError,
    FlagshipDemoResult,
    FlagshipProof,
    build_scripted_adapter,
    extract_flagship_proof,
    run_flagship_demo,
    script_manifest,
    script_manifest_digest,
)

__all__ = [
    "FlagshipDemoError",
    "FlagshipDemoResult",
    "FlagshipProof",
    "build_scripted_adapter",
    "extract_flagship_proof",
    "run_flagship_demo",
    "script_manifest",
    "script_manifest_digest",
]
