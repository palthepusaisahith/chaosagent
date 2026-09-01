# Shipment/refund Scenario v0 example

`refund-ambiguous-timeout.v0.json` is the committed structural revision 1 and is
left byte-for-byte semantically unchanged.
`refund-ambiguous-timeout.evaluated.v0.json` is revision 2; it references the
committed deterministic Fixture v0 document in `../fixtures` and the executable
Ground Truth v0 revision.

Its policy reference resolves to the committed Policy v0 document in
`../policies`. Revision 2's expected-outcome reference resolves to the
committed, content-addressed document in `../ground-truth`; revision 1 retains
its original clearly structural placeholder rather than silently changing an
immutable revision.

`shipping-transient-error.v0.json` is the structural Issue #14 pre-tool 503
example. Execution requires an explicitly compiled/selected fault plan and run
seed; merely storing the Scenario does not activate a Campaign.
