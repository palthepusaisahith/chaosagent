# Shipment/refund Scenario v0 template

`refund-ambiguous-timeout.v0.json` is a structurally valid authoring template,
not an executable or externally resolved scenario.

Its fixture, policy, and expected-outcome references use an all-zero SHA-256
sentinel. The sentinel satisfies Scenario v0's reference syntax only. It does
not claim that referenced content exists, that a catalog resolved it, or that
its digest was verified. Replace every sentinel with a catalog-issued digest
once the corresponding revision contracts and stores exist.
