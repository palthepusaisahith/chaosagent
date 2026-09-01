"""Deterministic compilation and matching of frozen Scenario v0 fault rules."""

from .application import (
    AppliedFault,
    BlockingFaultSleeper,
    FaultApplicationError,
    FaultApplicationResult,
    FaultEngine,
    FaultFailureCode,
    FaultSelection,
    FaultSleeper,
)
from .history import (
    AuthenticatedFaultHistory,
    FaultHistoryValidationError,
    authenticate_fault_history_v0,
)
from .matcher import (
    FAULT_MATCHER_V0_ALGORITHM,
    CompiledFaultPlan,
    CompiledFaultRule,
    FaultDecision,
    FaultDecisionReason,
    FaultKind,
    FaultMatchContext,
    FaultPhase,
    FaultRuleValidationError,
    compile_fault_plan_v0,
    match_fault_plan_v0,
)

__all__ = [
    "AppliedFault",
    "AuthenticatedFaultHistory",
    "BlockingFaultSleeper",
    "FAULT_MATCHER_V0_ALGORITHM",
    "CompiledFaultPlan",
    "CompiledFaultRule",
    "FaultDecision",
    "FaultDecisionReason",
    "FaultApplicationError",
    "FaultApplicationResult",
    "FaultEngine",
    "FaultFailureCode",
    "FaultHistoryValidationError",
    "FaultKind",
    "FaultMatchContext",
    "FaultPhase",
    "FaultRuleValidationError",
    "FaultSelection",
    "FaultSleeper",
    "authenticate_fault_history_v0",
    "compile_fault_plan_v0",
    "match_fault_plan_v0",
]
