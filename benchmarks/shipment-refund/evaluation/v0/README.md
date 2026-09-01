# Deterministic evaluation v0 example

`pass.structural.json` is the canonicalizable output of the pure evaluator for
the synthetic trajectory assembled by `tests/python/test_evaluators.py`. It is a
structural golden example, not evidence from an actually executed Run. Its event
IDs therefore identify that test fixture only and are not represented as a real
trace. The fault activation ID is nevertheless derived by the production Issue
#13 algorithm from the structural Run seed and frozen Scenario; it is not a
readability placeholder.
