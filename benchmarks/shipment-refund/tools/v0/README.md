# Read-only Tool v0 golden calls

`read-only-calls.json` is a structural example derived from the committed
`fake-company.failed-shipment` Fixture revision 1. It demonstrates the exact
lookup key and deterministic output of Issue #8. It is not a claim that a Run
occurred and contains no fabricated event IDs, timestamps, or persistence
references.

`mutation-calls.json` is likewise structural. It shows first application,
same-key replay, conflicting reuse, and support update/replay using real Fixture
v0 entity identities and one explicitly fictional Run identity. It is not an
event transcript and its database timestamp is intentionally described rather
than fabricated.
