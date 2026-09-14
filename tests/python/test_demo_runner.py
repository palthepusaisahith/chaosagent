from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from dataclasses import replace
from inspect import signature
from pathlib import Path
from typing import cast

import pytest
from alembic import command
from alembic.config import Config
from chaosagent_agent_runtime import AgentContext
from chaosagent_demo_runner import (
    FlagshipDemoError,
    FlagshipDemoResult,
    FlagshipProof,
    build_scripted_adapter,
    extract_flagship_proof,
    run_flagship_demo,
    script_manifest,
    script_manifest_digest,
)
from chaosagent_demo_runner.cli import main, render_success
from chaosagent_demo_runner.runner import load_flagship_assets
from chaosagent_evaluators import (
    AuthoritativeEvaluationSnapshot,
    load_authoritative_evaluation_snapshot,
)
from chaosagent_evidence import digest_payload_v0
from chaosagent_persistence import (
    ExecutionCheckpointRecord,
    PersistenceRepository,
    RunEventRecord,
    create_postgres_engine,
)
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_INI = ROOT / "packages" / "persistence" / "alembic.ini"
SCRIPT_DIGEST = "sha256:16da7e3daeac7a205f7e8241f5f602568f36f8acfd7c4eb47aa3439b216320d2"


def _plain(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return value


def test_script_manifest_is_stable_and_defensively_rebuilt() -> None:
    first = script_manifest()
    second = script_manifest()
    assert first == second
    assert script_manifest_digest() == SCRIPT_DIGEST
    cast(list[dict[str, object]], first["steps"])[0]["text"] = "tampered"
    assert script_manifest() == second
    assert script_manifest_digest() == SCRIPT_DIGEST


def test_scripted_adapter_has_the_exact_deterministic_sequence() -> None:
    adapter = build_scripted_adapter()
    outputs = tuple(
        adapter.invoke(
            AgentContext(
                "run-test",
                "task",
                (),
                step_number,
                (),
                (),
                10,
                10,
                10_000,
                10_000,
                True,
            )
        )
        for step_number in range(1, 7)
    )
    calls = [output.tool_calls[0] for output in outputs[:-1]]
    assert [(call.tool_id, call.contract_version) for call in calls] == [
        ("orders.get", "chaosagent.tool/orders.get/v0"),
        ("shipping.get_status", "chaosagent.tool/shipping.get_status/v0"),
        ("payments.refund", "chaosagent.tool/payments.refund/v0"),
        ("payments.refund", "chaosagent.tool/payments.refund/v0"),
        ("support.update_ticket", "chaosagent.tool/support.update_ticket/v0"),
    ]
    first_refund = calls[2]
    second_refund = calls[3]
    assert dict(first_refund.arguments) == dict(second_refund.arguments)
    assert first_refund.arguments["idempotency_key"] == "flagship-refund-v1"
    assert outputs[-1].final
    assert outputs[-1].text == "Refund confirmed exactly once."
    assert all(output.usage.cost_microusd == 0 for output in outputs)


def test_scenario_revision_three_preserves_verification_first_matcher() -> None:
    scenario = load_flagship_assets(ROOT).scenario.to_dict()
    assert scenario["revision"] == "3"
    faults = cast(list[dict[str, object]], scenario["faults"])
    assert len(faults) == 1
    match = cast(dict[str, object], faults[0]["match"])
    assert match == {
        "tool_id": "payments.refund",
        "phase": "after_commit",
        "argument_equals": {"order_id": "ORD-1007"},
    }


def test_success_output_uses_proof_values() -> None:
    proof = FlagshipProof(
        "run-demo",
        "event-cause",
        "event-effect",
        "event-fault",
        "event-ambiguous",
        "event-recovery",
        "event-recovery-result",
        1,
        "already_applied",
        "pass",
    )
    output = render_success(
        FlagshipDemoResult("run-demo", "http://127.0.0.1:5173/runs/run-demo", proof)
    )
    assert "authoritative refund effects = 1" in output
    assert "disposition already_applied" in output
    assert "VERDICT    PASS" in output


def test_proof_api_does_not_accept_independent_effect_records() -> None:
    assert tuple(signature(extract_flagship_proof).parameters) == (
        "snapshot",
        "event_records",
        "checkpoint",
    )


def test_cli_fails_cleanly_without_database_url(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("CHAOSAGENT_DATABASE_URL", raising=False)
    assert main([]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "CHAOSAGENT_DATABASE_URL is required" in captured.err


@pytest.fixture(scope="module")
def demo_database_url() -> Iterator[str]:
    database_url = os.environ.get("CHAOSAGENT_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("CHAOSAGENT_TEST_DATABASE_URL is not configured")
    if os.environ.get("CHAOSAGENT_ALLOW_DESTRUCTIVE_DATABASE_TESTS") != "1":
        raise RuntimeError("destructive PostgreSQL tests require explicit opt-in")
    if not (make_url(database_url).database or "").endswith("_test"):
        raise RuntimeError("PostgreSQL integration database name must end with '_test'")
    prior = os.environ.get("CHAOSAGENT_DATABASE_URL")
    os.environ["CHAOSAGENT_DATABASE_URL"] = database_url
    configuration = Config(str(ALEMBIC_INI))
    command.downgrade(configuration, "base")
    command.upgrade(configuration, "head")
    try:
        yield database_url
    finally:
        command.downgrade(configuration, "base")
        if prior is None:
            os.environ.pop("CHAOSAGENT_DATABASE_URL", None)
        else:
            os.environ["CHAOSAGENT_DATABASE_URL"] = prior


@pytest.mark.postgres
def test_real_flagship_path_and_repeat_invocation(demo_database_url: str) -> None:
    first = run_flagship_demo(demo_database_url, ROOT)
    second = run_flagship_demo(demo_database_url, ROOT)
    assert first.run_id != second.run_id
    assert first.proof.classification == second.proof.classification == "pass"
    assert first.proof.refund_effect_count == second.proof.refund_effect_count == 1
    assert (
        first.proof.recovery_disposition == second.proof.recovery_disposition == "already_applied"
    )

    engine = create_postgres_engine(demo_database_url)
    try:
        assets = load_flagship_assets(ROOT)
        with Session(engine) as session:
            repository = PersistenceRepository(session)
            scenario = repository.get_scenario_revision("shipment-refund.ambiguous-timeout", "3")
            assert scenario is not None and scenario.scenario.digest == assets.scenario.digest
            proof_material: dict[
                str,
                tuple[
                    AuthoritativeEvaluationSnapshot,
                    tuple[RunEventRecord, ...],
                    ExecutionCheckpointRecord,
                ],
            ] = {}
            for result in (first, second):
                run = repository.get_run(result.run_id)
                state = repository.get_run_company_state(result.run_id)
                events = repository.fetch_events(result.run_id)
                checkpoint = repository.get_execution_checkpoint(result.run_id)
                assert run is not None and run.status == "completed" and run.fault_seed == 1616
                assert state is not None and len(state.refunds) == 1
                assert checkpoint is not None
                assert repository.get_final_report(result.run_id) is None
                snapshot = load_authoritative_evaluation_snapshot(
                    repository, result.run_id, (assets.ground_truth,)
                )
                assert snapshot.result.to_dict()["classification"] == "pass"
                assert extract_flagship_proof(snapshot, events, checkpoint) == result.proof
                proof_material[result.run_id] = (snapshot, events, checkpoint)
                documents = [record.event.to_dict() for record in events]
                types = [document["event_type"] for document in documents]
                assert types.count("fault.matched") == 1
                assert types.count("fault.applied") == 1
                assert types.count("fault.observed") == 1
                assert types.count("state.evidence_recorded") == 2
                lifecycle_states = [
                    cast(dict[str, object], document["payload"]).get("state")
                    for document in documents
                    if document["event_type"] == "run.lifecycle"
                ]
                assert lifecycle_states == ["provisioning", "running", "evaluating", "completed"]
                ticket = next(item for item in state.support_tickets if item.ticket_id == "TKT-204")
                assert ticket.status == "closed" and "Refund" in ticket.note

                corrupted = cast(dict[str, object], _plain(checkpoint.document))
                trajectory = cast(list[dict[str, object]], corrupted["trajectory"])
                recovery = [
                    item
                    for item in trajectory
                    if item.get("kind") == "tool"
                    and item.get("request_event_id") == result.proof.recovery_request_event_id
                ]
                assert len(recovery) == 1
                cast(dict[str, object], recovery[0]["output"])["application"] = "applied"
                with pytest.raises(FlagshipDemoError, match="proof material"):
                    extract_flagship_proof(
                        snapshot,
                        events,
                        replace(
                            checkpoint,
                            document=corrupted,
                            document_digest=digest_payload_v0(corrupted),
                        ),
                    )

                fabricated = cast(dict[str, object], _plain(checkpoint.document))
                cast(list[dict[str, object]], fabricated["trajectory"])[0]["text"] = "fabricated"
                with pytest.raises(FlagshipDemoError, match="proof material"):
                    extract_flagship_proof(
                        snapshot,
                        events,
                        replace(
                            checkpoint,
                            document=fabricated,
                            document_digest=digest_payload_v0(fabricated),
                        ),
                    )

            first_snapshot, first_events, first_checkpoint = proof_material[first.run_id]
            _, second_events, second_checkpoint = proof_material[second.run_id]
            with pytest.raises(FlagshipDemoError, match="checkpoint binding"):
                extract_flagship_proof(first_snapshot, first_events, second_checkpoint)
            with pytest.raises(FlagshipDemoError, match="another Run"):
                extract_flagship_proof(first_snapshot, second_events, first_checkpoint)

            first_effects = repository.list_company_effects(first.run_id)
            second_effects = repository.list_company_effects(second.run_id)
            assert {item.run_id for item in first_effects} == {first.run_id}
            assert {item.run_id for item in second_effects} == {second.run_id}
            assert {item.effect_id for item in first_effects}.isdisjoint(
                item.effect_id for item in second_effects
            )
    finally:
        engine.dispose()


@pytest.mark.postgres
def test_explicit_run_id_conflict_fails_without_reuse(demo_database_url: str) -> None:
    first = run_flagship_demo(demo_database_url, ROOT, run_id="run-flagship-explicit")
    assert first.proof.classification == "pass"
    with pytest.raises(FlagshipDemoError, match="PostgreSQL rejected"):
        run_flagship_demo(demo_database_url, ROOT, run_id="run-flagship-explicit")
