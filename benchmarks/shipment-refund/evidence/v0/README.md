# Shipment/refund evidence v0 golden documents

These files are structural contract examples, not records of an experiment that
actually ran. The numbered event files form one ordered example stream, and
`run-report.json` references that stream.

Event `payload_digest` values are real SHA-256 digests of each payload's RFC
8785 representation and are verified by the loader. The report's Scenario digest
is the real Scenario v0 semantic digest of the adjacent committed
shipment/refund example. By contrast, all-zero digests inside Agent
Configuration, evaluator, tool-argument, and idempotency-key references are
conspicuous unresolved sentinels for catalogs, content, and stores that do not
exist yet. They are syntax examples only and make no content-addressability or
resolution claim.

The example shows a refund request whose acknowledgement is lost after the
effect, a distinct authoritative business-effect observation, the separate
matched/applied/observed evidence, and a final report whose claims point to
events in the example stream. Unknown model usage and cost are omitted rather
than represented as zero. The files do not implement the tool, fault, run loop,
evaluator, report builder, state store, or persistence system.
