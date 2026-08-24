"""Load, validate, and canonically identify synthetic-company fixtures."""

from .fixture import (
    FIXTURE_V0_SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
    Fixture,
    FixtureValidationError,
    canonicalize_fixture,
    canonicalize_fixture_v0,
    digest_fixture,
    digest_fixture_v0,
    fixture_schema,
    fixture_schema_v0,
    load_fixture,
    load_fixture_v0,
    loads_fixture,
    loads_fixture_v0,
    validate_fixture,
    validate_fixture_v0,
)

__all__ = [
    "FIXTURE_V0_SCHEMA_VERSION",
    "SUPPORTED_SCHEMA_VERSIONS",
    "Fixture",
    "FixtureValidationError",
    "canonicalize_fixture",
    "canonicalize_fixture_v0",
    "digest_fixture",
    "digest_fixture_v0",
    "fixture_schema",
    "fixture_schema_v0",
    "load_fixture",
    "load_fixture_v0",
    "loads_fixture",
    "loads_fixture_v0",
    "validate_fixture",
    "validate_fixture_v0",
]
