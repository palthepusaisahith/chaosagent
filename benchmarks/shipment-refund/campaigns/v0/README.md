# Shipment/refund Campaign comparison v0

`comparison.structural.json` is a **structural golden**, not a record of six
executed Runs and not a published benchmark result. Its per-Run Evaluation
Result digests are deterministically derived test fixtures over the committed
Issue #16 flagship evaluation shape.

It demonstrates three controlled trial indexes per arm, observed-fault
conditioning for `refund-ack-lost`, finite-sample `pass@k`, predetermined-group
`pass^k`, Wilson intervals, and a paired improvement. The small-sample warnings
are intentional. The example Agent Configuration digest is structural and does
not claim a catalog-backed production revision.
