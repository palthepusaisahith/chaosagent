# Shipment/refund Export Bundle v0 fixture

`tests/python/test_exports.py` assembles the committed shipment/refund Scenario,
eleven Run Event v0 documents, and Run Report v0 into a deterministic golden
Export Bundle v0. The test verifies the canonical manifest, exact JSONL
ordering, checksums, deterministic ZIP bytes, directory validation, and
representative tampering. The PostgreSQL integration tests build Run and
Campaign bundles from authenticated Issue #16/#17 state rather than checking in
fabricated database-derived identities.

This directory deliberately contains no placeholder manifest. A manifest is
meaningful only when its checksums and provenance correspond to the exact
exported files.
