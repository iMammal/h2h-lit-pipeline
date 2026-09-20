from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import h2h_lit.prior_survey_identity_adjudication as adjudication
from h2h_lit.external_retrieval_wave import (
    ExternalRetrievalWaveError,
    _exclusive_external_source_session,
)
from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    RecordOccurrence,
    RetrievalCompletionStatus,
    RetrievalRun,
    RetrievalRunKind,
    ReviewDataset,
    SourceQuery,
    canonicalize_occurrences,
)

STAMP = "2026-09-20T21:00:00+00:00"


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _write_json(path: Path, value: object) -> dict:
    content = _json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return {
        "path": path.name,
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _root_reference(root: Path, path: Path) -> dict:
    content = path.read_bytes()
    return {
        "path": path.relative_to(root).as_posix(),
        "path_scope": "repository_relative",
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _state_fixture(root: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict, str, Path, str]:
    dataset_path = root / "production" / "review_dataset.json"
    dataset_path.parent.mkdir(parents=True)
    dataset_path.write_bytes(b"immutable-registered-dataset\n")
    reconciliation_path = root / "production" / "reconciliation.json"
    reconciliation_path.write_bytes(
        _json_bytes(
            {
                "input_occurrences": 187446,
                "output_occurrences": 187446,
                "canonical_counts": {"after_approved_adjudication": 140959},
                "duplicate_decisions": {"effective_decisions": 187446},
                "related_versions": {"preserved_links": 50, "collapsed": False},
                "remaining_identity_review": {
                    "source_unresolved_identity_groups": 30,
                    "source_unresolved_occurrences": 71,
                },
                "identification_closed": False,
                "screening_changed": False,
                "prisma_changed": False,
            }
        )
    )
    dataset_ref = _root_reference(root, dataset_path)
    reconciliation_ref = _root_reference(root, reconciliation_path)
    state = {
        "schema_version": "1.0.0",
        "execution_id": "fixture",
        "status": "COMPLETE",
        "state_hash": "state-hash-fixture",
        "sources": {"fixture": {"status": "COMPLETE"}},
        "prior_survey_imports": {"EBK25": {"status": "AUTHORIZED"}},
        "identification_set_closed": False,
        "screening_executed": False,
        "prisma_generated": False,
        "corpus_modified": False,
        "final_global_deduplication_executed": True,
        "global_identification_merge": {
            "status": "COMPLETE_NOT_IDENTIFICATION_CLOSED",
            "dataset": dataset_ref,
            "reconciliation": reconciliation_ref,
        },
    }
    state_raw_hash = hashlib.sha256(_json_bytes(state)).hexdigest()

    review_dir = root / "review"
    review_groups = []
    application_groups = []
    occurrence_number = 0
    for group_number in range(30):
        group_size = 3 if group_number < 11 else 2
        occurrence_ids = []
        effective = []
        planned = []
        canonical_id = f"canonical:{group_number:02d}"
        survivor_id = f"occurrence:{occurrence_number:03d}"
        for _ in range(group_size):
            occurrence_id = f"occurrence:{occurrence_number:03d}"
            prior_id = f"dedupe:{occurrence_number:03d}"
            decision_id = f"confirm:{occurrence_number:03d}"
            outcome = "unique" if occurrence_id == survivor_id else "duplicate"
            prior = {
                "decision_id": prior_id,
                "occurrence_id": occurrence_id,
                "canonical_record_id": canonical_id,
                "survivor_occurrence_id": survivor_id,
                "outcome": outcome,
                "match_key": f"title:fixture {group_number}",
                "match_rule": "doi_first_title_fallback",
            }
            effective.append(prior)
            planned.append(
                {
                    **prior,
                    "decision_id": decision_id,
                    "match_rule": "adjudicated_prior_survey_identity_confirmation",
                    "supersedes_ids": [prior_id],
                    "original_match_rule": "doi_first_title_fallback",
                }
            )
            occurrence_ids.append(occurrence_id)
            occurrence_number += 1
        group_id = f"ebk25-candidate:{group_number:02d}"
        review_groups.append(
            {
                "provisional_identity_group_id": group_id,
                "canonical_id": canonical_id,
                "current_canonical_record": {
                    "canonical_id": canonical_id,
                    "survivor_occurrence_id": survivor_id,
                    "occurrence_ids": occurrence_ids,
                    "metadata": {
                        "prior_survey_identity_status": "UNRESOLVED",
                        "unresolved_prior_survey_occurrence_ids": occurrence_ids,
                    },
                },
                "effective_duplicate_decisions": effective,
            }
        )
        application_groups.append(
            {
                "group_id": group_id,
                "title": f"Fixture {group_number}",
                "proposed_outcome": "MERGE",
                "source_occurrence_count": group_size,
                "canonical_id_before": canonical_id,
                "canonical_id_after_dry_run": canonical_id,
                "survivor_occurrence_id_before": survivor_id,
                "survivor_occurrence_id_after_dry_run": survivor_id,
                "occurrence_ids_before": occurrence_ids,
                "occurrence_ids_after_dry_run": occurrence_ids,
                "occurrence_membership_delta": {"added": [], "deleted": [], "moved": []},
                "historical_status": {
                    "canonical_marker": "UNRESOLVED",
                    "unresolved_occurrence_ids": occurrence_ids,
                    "occurrence_identity_resolution": {
                        occurrence_id: "UNRESOLVED" for occurrence_id in occurrence_ids
                    },
                },
                "planned_current_status": "ADJUDICATED_CONFIRMED",
                "preserved_labels": {
                    "source_survey_membership": ["UNCONFIRMED"],
                    "our_star_eligibility": ["UNASSESSED"],
                    "prisma_counted": [False],
                    "production_eligible": [False],
                },
                "planned_duplicate_decisions": planned,
            }
        )
    assert occurrence_number == 71
    review_ref = _write_json(
        review_dir / "review_groups.json",
        {"groups": review_groups, "status": "STAGED_REVIEW_NOT_APPLIED"},
    )
    review_manifest_ref = _write_json(
        review_dir / "package_manifest.json",
        {"artifacts": {"review_groups": review_ref}},
    )

    proposal_dir = root / "proposal"
    artifacts = {
        "benchmark_specification_proposal": _write_json(
            proposal_dir / "benchmark.json", {"fixture": True}
        ),
        "bibliographic_lookup_evidence": _write_json(
            proposal_dir / "evidence.json", {"fixture": True}
        ),
        "identity_proposals": _write_json(proposal_dir / "proposals.json", {"fixture": True}),
        "identity_rationales": _write_json(proposal_dir / "rationales.json", {"fixture": True}),
    }
    proposal_manifest_ref = _write_json(
        proposal_dir / "package_manifest.json",
        {
            "artifact_class": "PRIOR_SURVEY_IDENTITY_REVIEW_PROPOSAL_PACKAGE",
            "schema_version": "2.0.0",
            "status": "STAGED_REVIEW_NOT_APPLIED",
            "artifacts": artifacts,
            "bindings": {
                "source_review_package": {
                    "path": review_dir.relative_to(root).as_posix(),
                    "package_manifest_sha256": review_manifest_ref["raw_sha256"],
                    "review_groups_sha256": review_ref["raw_sha256"],
                }
            },
        },
    )

    dry_dir = root / "dry-run"
    proposal_binding = {
        "path": proposal_dir.relative_to(root).as_posix(),
        "package_manifest_sha256": proposal_manifest_ref["raw_sha256"],
    }
    original_binding = {
        "package_manifest_sha256": review_manifest_ref["raw_sha256"],
        "review_groups_sha256": review_ref["raw_sha256"],
    }
    evidence_binding = {
        "path": (proposal_dir / artifacts["bibliographic_lookup_evidence"]["path"])
        .relative_to(root)
        .as_posix(),
        "raw_sha256": artifacts["bibliographic_lookup_evidence"]["raw_sha256"],
    }
    reconciliation_binding = {
        "path": reconciliation_ref["path"],
        "raw_sha256": reconciliation_ref["raw_sha256"],
    }
    state_binding = {
        "path": "production/execution_state.json",
        "raw_sha256": state_raw_hash,
        "embedded_state_hash": state["state_hash"],
    }
    application = {
        "schema_version": "1.0.0",
        "status": adjudication.DRY_RUN_STATUS,
        "application_supported_by_current_cli": False,
        "source_package": proposal_binding,
        "bindings": {
            "registered_dataset": dataset_ref,
            "execution_state": state_binding,
            "original_review_package": original_binding,
            "bibliographic_lookup_evidence": evidence_binding,
            "registered_reconciliation": reconciliation_binding,
        },
        "groups": application_groups,
        "invariant_totals": {
            "occurrences_before": 187446,
            "occurrences_after_dry_run": 187446,
            "canonical_records_before": 140959,
            "canonical_records_after_dry_run": 140959,
            "related_version_links_before": 50,
            "related_version_links_after_dry_run": 50,
            "effective_duplicate_decisions_before": 187446,
            "effective_duplicate_decisions_after_dry_run": 187446,
        },
    }
    application_ref = _write_json(dry_dir / "application.json", application)
    report_ref = _write_json(dry_dir / "report.json", {"fixture": True})
    dry_manifest_ref = _write_json(
        dry_dir / "package_manifest.json",
        {
            "artifact_class": adjudication.DRY_RUN_ARTIFACT_CLASS,
            "schema_version": "1.0.0",
            "status": adjudication.DRY_RUN_STATUS,
            "artifacts": {
                "application_dry_run": application_ref,
                "report": report_ref,
            },
            "bindings": {
                "proposal_package": proposal_binding,
                "registered_dataset": dataset_ref,
                "execution_state": state_binding,
                "original_review_package": original_binding,
                "bibliographic_lookup_evidence": {"raw_sha256": evidence_binding["raw_sha256"]},
                "registered_reconciliation": reconciliation_binding,
            },
            "counts": {
                "identity_groups": 30,
                "source_occurrences": 71,
                "planned_superseding_history_entries": 71,
                "occurrence_membership_changes": 0,
                "canonical_membership_changes": 0,
                "related_version_link_changes": 0,
            },
            "invariant_totals": adjudication.EXPECTED_TOTALS,
        },
    )
    monkeypatch.setattr(adjudication, "_implementation_bindings", lambda _root: [])
    return state, state_raw_hash, dry_dir, dry_manifest_ref["raw_sha256"]


def _small_review_dataset() -> ReviewDataset:
    run_id = "run:test"
    query_id = "query:test"
    run = RetrievalRun(
        run_id=run_id,
        kind=RetrievalRunKind.PRIMARY,
        query_plan_version="test",
        query_plan_hash="0" * 64,
        planned_query_ids=[query_id],
        source_query_ids=[query_id],
        retrieval_started_at=STAMP,
        retrieval_completed_at=STAMP,
        status=ProcessingStatus.OK,
        protocol_version="1.0.0",
        retrieval_cutoff_date="2026-09-20",
        completion_status=RetrievalCompletionStatus.COMPLETE,
    )
    query = SourceQuery(
        query_id=query_id,
        source_database="PriorSurveySeed",
        query_text="fixture",
        retrieval_started_at=STAMP,
        retrieval_ended_at=STAMP,
        status=ProcessingStatus.OK,
        run_id=run_id,
        result_count=2,
    )
    occurrences = [
        RecordOccurrence(
            occurrence_id=f"occurrence:{number}",
            source_query_id=query_id,
            source_identifier=str(number),
            retrieved_at=STAMP,
            record=LiteratureRecord(
                title="Same work",
                source_database="PriorSurveySeed",
                source_identifier=str(number),
                original_metadata={
                    "identity_resolution": "UNRESOLVED",
                    "source_survey_membership": "UNCONFIRMED",
                    "our_star_eligibility": "UNASSESSED",
                },
            ),
        )
        for number in range(2)
    ]
    provenance = DecisionProvenance(
        actor=DecisionActor("software", ActorType.SOFTWARE),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=STAMP,
    )
    canonical, decisions = canonicalize_occurrences(occurrences, provenance=provenance)
    canonical[0].metadata.update(
        {
            "prior_survey_identity_status": "UNRESOLVED",
            "unresolved_prior_survey_occurrence_ids": [item.occurrence_id for item in occurrences],
        }
    )
    dataset = ReviewDataset(
        schema_version="1.3.0",
        retrieval_runs=[run],
        source_queries=[query],
        occurrences=occurrences,
        canonical_records=canonical,
        duplicate_decisions=decisions,
    )
    dataset.validate()
    return dataset


def test_overlay_supersedes_without_changing_membership_or_history() -> None:
    dataset = _small_review_dataset()
    historical_markers = [
        item.record.original_metadata["identity_resolution"] for item in dataset.occurrences
    ]
    before_history = list(dataset.duplicate_decisions)
    overlay_decisions = []
    for previous in dataset.effective_duplicate_decisions():
        overlay_decisions.append(
            {
                "decision_id": f"confirm:{previous.occurrence_id}",
                "occurrence_id": previous.occurrence_id,
                "canonical_record_id": previous.canonical_record_id,
                "survivor_occurrence_id": previous.survivor_occurrence_id,
                "outcome": previous.outcome.value,
                "match_key": previous.match_key,
                "match_rule": "adjudicated_prior_survey_identity_confirmation",
                "provenance": {
                    "actor": {
                        "actor_id": "owner",
                        "actor_type": "adjudicator",
                    },
                    "authority": "adjudicated",
                    "scope": "prospective",
                    "protocol_version": "1.0.0",
                    "rubric_version": "1.0.0",
                    "created_at": STAMP,
                    "supersedes_ids": [previous.decision_id],
                },
            }
        )
    adjudication.append_identity_confirmation_overlay(
        dataset, {"superseding_duplicate_decisions": overlay_decisions}
    )
    assert len(dataset.duplicate_decisions) == len(before_history) + 2
    assert len(dataset.effective_duplicate_decisions()) == 2
    assert all(
        item.match_rule == "adjudicated_prior_survey_identity_confirmation"
        for item in dataset.effective_duplicate_decisions()
    )
    assert historical_markers == [
        item.record.original_metadata["identity_resolution"] for item in dataset.occurrences
    ]
    assert dataset.canonical_records[0].metadata["prior_survey_identity_status"] == ("UNRESOLVED")


def test_prepare_validate_authorize_is_idempotent_and_dataset_is_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, state_hash, dry_dir, dry_hash = _state_fixture(tmp_path, monkeypatch)
    dataset_path = tmp_path / state["global_identification_merge"]["dataset"]["path"]
    dataset_before = dataset_path.read_bytes()
    output_dir = tmp_path / "ready"
    result = adjudication.prepare_identity_confirmation_package(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256=state_hash,
        dry_run_package_dir=dry_dir,
        expected_dry_run_manifest_sha256=dry_hash,
        output_dir=output_dir,
        generated_at=STAMP,
    )
    validated = adjudication.validate_identity_confirmation_package(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256=state_hash,
        package_dir=output_dir,
        expected_manifest_sha256=result["package_manifest_sha256"],
    )
    assert len(validated.overlay["groups"]) == 30
    assert len(validated.overlay["superseding_duplicate_decisions"]) == 71
    state, first = adjudication.authorize_identity_confirmation_overlay(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256=state_hash,
        package_dir=output_dir,
        expected_manifest_sha256=result["package_manifest_sha256"],
        authorized_at=STAMP,
    )
    state, second = adjudication.authorize_identity_confirmation_overlay(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="ignored-after-idempotent-registration",
        package_dir=output_dir,
        expected_manifest_sha256=result["package_manifest_sha256"],
        authorized_at="2026-09-20T22:00:00+00:00",
    )
    assert first == second
    assert dataset_path.read_bytes() == dataset_before
    status = adjudication.validate_registered_identity_confirmation_overlay(
        root=tmp_path, state=state, verify_dataset_hash=True
    )
    assert status is not None
    assert status["current_adjudicated_confirmed_groups"] == 30
    assert state["identification_set_closed"] is False
    assert state["screening_executed"] is False
    assert state["prisma_generated"] is False


def test_tampering_and_stale_input_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, state_hash, dry_dir, dry_hash = _state_fixture(tmp_path, monkeypatch)
    output_dir = tmp_path / "ready"
    result = adjudication.prepare_identity_confirmation_package(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256=state_hash,
        dry_run_package_dir=dry_dir,
        expected_dry_run_manifest_sha256=dry_hash,
        output_dir=output_dir,
        generated_at=STAMP,
    )
    with pytest.raises(
        adjudication.PriorSurveyIdentityAdjudicationError, match="input state is stale"
    ):
        adjudication.validate_identity_confirmation_package(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="0" * 64,
            package_dir=output_dir,
            expected_manifest_sha256=result["package_manifest_sha256"],
        )
    overlay_path = output_dir / "identity_confirmation_overlay.json"
    overlay_path.write_bytes(overlay_path.read_bytes() + b" ")
    with pytest.raises(
        adjudication.PriorSurveyIdentityAdjudicationError,
        match="artifact (size|hash)",
    ):
        adjudication.validate_identity_confirmation_package(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256=state_hash,
            package_dir=output_dir,
            expected_manifest_sha256=result["package_manifest_sha256"],
        )


def test_locked_entrypoint_rejects_a_concurrent_session(tmp_path: Path) -> None:
    with (
        _exclusive_external_source_session(tmp_path),
        pytest.raises(ExternalRetrievalWaveError, match="already active"),
    ):
        adjudication.prepare_identity_confirmation_package_locked(
            root=tmp_path,
            dry_run_package_dir="unused",
            expected_dry_run_manifest_sha256="0" * 64,
            output_dir="unused",
        )


def test_overlay_state_reader_validates_pre_and_post_global_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import h2h_lit.arxiv_snapshot_integration as snapshot
    import h2h_lit.external_retrieval_wave as external
    import h2h_lit.global_identification_merge as global_merge
    import h2h_lit.prior_survey_integration as prior

    wave_path = tmp_path / external.WAVE_PATH
    preflight_path = tmp_path / external.PREFLIGHT_PATH
    state_path = tmp_path / external.EXECUTION_STATE_PATH
    wave_path.parent.mkdir(parents=True, exist_ok=True)
    wave_path.write_bytes(b"wave\n")
    preflight_path.write_bytes(b"preflight\n")
    state = {
        "state_hash": "fixture",
        "wave_manifest_hash": "wave-manifest",
        "planned_wave_raw_sha256": hashlib.sha256(wave_path.read_bytes()).hexdigest(),
        "preflight_raw_sha256": hashlib.sha256(preflight_path.read_bytes()).hexdigest(),
        "final_global_deduplication_executed": True,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_bytes(_json_bytes(state))

    class _Wave:
        @staticmethod
        def manifest_hash() -> str:
            return "wave-manifest"

    observed: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        external,
        "validate_persisted_external_preflight",
        lambda *, root: (_Wave(), {}),
    )
    monkeypatch.setattr(external, "_validate_embedded_hash", lambda *_args: None)
    monkeypatch.setattr(
        snapshot,
        "validate_authorized_snapshot_substitution",
        lambda *, root, state: observed.append(
            ("snapshot", state["final_global_deduplication_executed"])
        ),
    )
    monkeypatch.setattr(
        prior,
        "validate_authorized_prior_survey_imports",
        lambda *, root, state: observed.append(
            ("prior", state["final_global_deduplication_executed"])
        ),
    )
    monkeypatch.setattr(
        global_merge,
        "validate_registered_global_merge",
        lambda *, root, state: observed.append(
            ("global", state["final_global_deduplication_executed"])
        ),
    )
    loaded_path, raw, loaded = adjudication._load_production_state(tmp_path)
    assert loaded_path == state_path
    assert raw == state_path.read_bytes()
    assert loaded["final_global_deduplication_executed"] is True
    assert observed == [("snapshot", True), ("prior", False), ("global", True)]
