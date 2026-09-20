from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from h2h_lit.arxiv_snapshot_integration import (
    ArxivSnapshotIntegrationError,
    ValidatedSnapshotPackage,
    apply_snapshot_adjudications,
    build_amendment_v2,
    derive_api_lineage,
    merge_identification_datasets_with_snapshot_integration,
    validate_prepared_package,
)
from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    DedupeOutcome,
    RecordOccurrence,
    RetrievalCompletionStatus,
    RetrievalRun,
    RetrievalRunKind,
    ReviewDataset,
    SourceQuery,
    canonicalize_occurrences,
)

STAMP = "2026-09-19T21:12:05+00:00"
ROOT = Path(__file__).resolve().parents[1]
PREPARED_PACKAGE = (
    ROOT
    / "outputs/staging/"
    "arxiv-snapshot-v303-integration-prep-20260920T002244Z"
)


def _dataset(
    *, source: str, run_id: str, records: list[tuple[str, LiteratureRecord]]
) -> ReviewDataset:
    query_id = f"query:{run_id}"
    run = RetrievalRun(
        run_id=run_id,
        kind=RetrievalRunKind.PRIMARY,
        query_plan_version="test-v1",
        query_plan_hash="0" * 64,
        planned_query_ids=[query_id],
        source_query_ids=[query_id],
        retrieval_started_at=STAMP,
        retrieval_completed_at=STAMP,
        status=ProcessingStatus.OK,
        protocol_version="1.0.0",
        retrieval_cutoff_date="2026-09-19",
        completion_status=RetrievalCompletionStatus.COMPLETE,
    )
    query = SourceQuery(
        query_id=query_id,
        source_database=source,
        query_text="test",
        retrieval_started_at=STAMP,
        retrieval_ended_at=STAMP,
        status=ProcessingStatus.OK,
        run_id=run_id,
        result_count=len(records),
        completion_status=RetrievalCompletionStatus.COMPLETE,
    )
    occurrences = [
        RecordOccurrence(
            occurrence_id=occurrence_id,
            source_query_id=query_id,
            source_identifier=record.source_identifier or occurrence_id,
            retrieved_at=STAMP,
            record=record,
            metadata={"snapshot_line_number": index + 1},
        )
        for index, (occurrence_id, record) in enumerate(records)
    ]
    provenance = DecisionProvenance(
        actor=DecisionActor("test", ActorType.SOFTWARE),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=STAMP,
    )
    canonical, decisions = canonicalize_occurrences(
        occurrences, provenance=provenance
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


def _fixture() -> tuple[ReviewDataset, ReviewDataset, ValidatedSnapshotPackage]:
    existing = _dataset(
        source="SemanticScholar",
        run_id="existing-run",
        records=[
            (
                "existing-alpha",
                LiteratureRecord(
                    title="Published alpha",
                    doi="10.1000/alpha",
                    arxiv_id="1234.5678v2",
                    source_identifier="paper-alpha",
                    source_database="SemanticScholar",
                ),
            ),
            (
                "existing-beta",
                LiteratureRecord(
                    title="Published beta",
                    doi="10.1000/beta",
                    source_identifier="paper-beta",
                    source_database="SemanticScholar",
                ),
            ),
        ],
    )
    snapshot = _dataset(
        source="arXivSnapshotV303",
        run_id="arxiv-snapshot-v303:QF01",
        records=[
            (
                "snapshot-alpha-qf01",
                LiteratureRecord(
                    title="Alpha preprint",
                    arxiv_id="1234.5678",
                    source_identifier="1234.5678",
                    source_database="arXivSnapshotV303",
                ),
            ),
            (
                "snapshot-alpha-qf02",
                LiteratureRecord(
                    title="Alpha preprint",
                    arxiv_id="1234.5678",
                    source_identifier="1234.5678",
                    source_database="arXivSnapshotV303",
                ),
            ),
            (
                "snapshot-beta",
                LiteratureRecord(
                    title="Beta preprint",
                    arxiv_id="9999.0001",
                    source_identifier="9999.0001",
                    source_database="arXivSnapshotV303",
                ),
            ),
        ],
    )
    identities = [
        {
            "proposal_id": "same-alpha",
            "normalized_arxiv_id": "1234.5678",
            "proposed_disposition": "same_bibliographic_record",
            "decision_status": "PROPOSED_NOT_APPLIED",
            "conflicts": [],
            "generic_dedupe_rule_changed": False,
            "candidate_identity": {
                "arxiv_id": "1234.5678",
                "staging_candidate_id": "candidate-alpha",
                "source_line_number": 1,
            },
            "existing_identity": {
                "arxiv_id": "1234.5678v2",
                "canonical_id": "source-local-id-must-not-be-used",
            },
        }
    ]
    relationships = [
        {
            "proposal_id": "related-beta",
            "proposed_disposition": "related preprint/published version",
            "decision_status": "PROPOSED_NOT_APPLIED",
            "identity_scope": "bibliographic_record_only",
            "study_grouping_decided": False,
            "screening_or_eligibility_decided": False,
            "candidate_identity": {
                "arxiv_id": "9999.0001",
                "staging_candidate_id": "candidate-beta",
            },
            "existing_identity": {"identity_key": "doi:10.1000/beta"},
        }
    ]
    package = ValidatedSnapshotPackage(
        package_dir=Path("."),
        package_manifest={
            "artifacts": {
                "retrieval_method_amendment_v1.json": {
                    "raw_sha256": "amendment-v1-sha"
                }
            }
        },
        package_manifest_sha256="package-sha",
        import_manifest={},
        amendment_v1={
            "amendment_id": "arxiv-snapshot-v303-retrieval-method-amendment-v1",
            "replacement": {
                "preserved_api_attempt_count": 57,
                "preserved_api_raw_response_count": 34,
            },
        },
        snapshot_dataset=snapshot,
        identity_proposals=identities,
        relationship_proposals=relationships,
    )
    return existing, snapshot, package


def test_actual_global_merge_preserves_occurrences_and_adjudications() -> None:
    existing, snapshot, package = _fixture()
    merged = merge_identification_datasets_with_snapshot_integration(
        [existing, snapshot], package, created_at=STAMP
    )

    assert len(merged.occurrences) == 5
    assert len(merged.canonical_records) == 3
    alpha_occurrences = {
        "existing-alpha", "snapshot-alpha-qf01", "snapshot-alpha-qf02"
    }
    alpha = next(
        item
        for item in merged.canonical_records
        if item.survivor_occurrence_id == "existing-alpha"
    )
    assert set(alpha.occurrence_ids) == alpha_occurrences
    adjudications = [
        item
        for item in merged.effective_duplicate_decisions()
        if item.match_rule == "adjudicated_exact_normalized_arxiv_id"
    ]
    assert {item.occurrence_id for item in adjudications} == {
        "snapshot-alpha-qf01", "snapshot-alpha-qf02"
    }
    assert all(
        item.outcome is DedupeOutcome.DUPLICATE
        and item.provenance.authority is DecisionAuthority.ADJUDICATED
        and len(item.provenance.supersedes_ids) == 1
        for item in adjudications
    )
    assert len(merged.duplicate_decisions) == 7


def test_related_versions_remain_separate_and_links_survive_later_merge() -> None:
    existing, snapshot, package = _fixture()
    first = merge_identification_datasets_with_snapshot_integration(
        [existing, snapshot], package, created_at=STAMP
    )
    later = merge_identification_datasets_with_snapshot_integration(
        [first], package, created_at=STAMP
    )

    beta_records = [
        item
        for item in later.canonical_records
        if item.record.title in {"Published beta", "Beta preprint"}
    ]
    assert len(beta_records) == 2
    marker = next(
        run.metadata["arxiv_snapshot_v303_integration"]
        for run in later.retrieval_runs
        if "arxiv_snapshot_v303_integration" in run.metadata
    )
    assert marker["related_version_candidate_count"] == 1
    assert len(marker["related_version_links"]) == 1
    assert marker["study_grouping_decided"] is False
    assert marker["screening_or_eligibility_decided"] is False
    assert len(later.occurrences) == 5
    assert len(later.canonical_records) == 3


def test_conflicting_arxiv_targets_remain_terminal() -> None:
    existing, snapshot, package = _fixture()
    conflict = _dataset(
        source="EuropePMC",
        run_id="conflict-run",
        records=[
            (
                "existing-alpha-conflict",
                LiteratureRecord(
                    title="A different alpha",
                    doi="10.1000/different",
                    arxiv_id="1234.5678",
                    source_identifier="different",
                    source_database="EuropePMC",
                ),
            )
        ],
    )
    with pytest.raises(
        ArxivSnapshotIntegrationError, match="missing or conflicting merged target"
    ):
        merge_identification_datasets_with_snapshot_integration(
            [existing, conflict, snapshot], package, created_at=STAMP
        )


def test_unapproved_third_snapshot_occurrence_remains_terminal() -> None:
    existing, snapshot, package = _fixture()
    extra = _dataset(
        source="arXivSnapshotV303",
        run_id="arxiv-snapshot-v303:unexpected",
        records=[
            (
                "snapshot-alpha-unapproved-third",
                LiteratureRecord(
                    title="Another alpha occurrence",
                    arxiv_id="1234.5678v3",
                    source_identifier="1234.5678v3",
                    source_database="arXivSnapshotV303",
                ),
            )
        ],
    )
    with pytest.raises(
        ArxivSnapshotIntegrationError,
        match="approved snapshot occurrence identities changed",
    ):
        merge_identification_datasets_with_snapshot_integration(
            [existing, snapshot, extra], package, created_at=STAMP
        )


def test_tampered_adjudication_metadata_is_rejected() -> None:
    existing, snapshot, package = _fixture()
    merged = merge_identification_datasets_with_snapshot_integration(
        [existing, snapshot], package, created_at=STAMP
    )
    tampered = copy.deepcopy(merged)
    decision = next(
        item
        for item in tampered.duplicate_decisions
        if item.match_rule == "adjudicated_exact_normalized_arxiv_id"
    )
    decision.provenance.source_artifact_id = "tampered"
    with pytest.raises(
        ArxivSnapshotIntegrationError,
        match="identity adjudication provenance changed",
    ):
        merge_identification_datasets_with_snapshot_integration(
            [tampered], package, created_at=STAMP
        )


def test_direct_reapplication_refuses_partial_or_conflicting_state() -> None:
    existing, snapshot, package = _fixture()
    merged = merge_identification_datasets_with_snapshot_integration(
        [existing, snapshot], package, created_at=STAMP
    )
    with pytest.raises(
        ArxivSnapshotIntegrationError,
        match="already matches under the generic rule",
    ):
        apply_snapshot_adjudications(merged, package, created_at=STAMP)


def test_corrected_amendment_counts_episode_scoped_lineage() -> None:
    _, _, package = _fixture()
    lineage = {
        "attempt_events": 60,
        "saved_responses": 37,
        "episodes_counted_once": [],
    }
    amendment = build_amendment_v2(package, lineage=lineage)
    assert amendment["replacement"]["preserved_api_attempt_count"] == 60
    assert amendment["replacement"]["preserved_api_raw_response_count"] == 37
    assert amendment["supersedes_amendment_id"].endswith("-v1")
    assert amendment["correction"]["api_lineage_accounting"] == lineage


def test_corrected_amendment_refuses_unverified_totals() -> None:
    _, _, package = _fixture()
    with pytest.raises(
        ArxivSnapshotIntegrationError, match="corrected API lineage totals changed"
    ):
        build_amendment_v2(
            package,
            lineage={"attempt_events": 59, "saved_responses": 37},
        )


def test_real_prepared_package_and_episode_lineage_are_exactly_bound() -> None:
    if not PREPARED_PACKAGE.is_dir():
        pytest.skip("persisted integration package is not present")
    package = validate_prepared_package(PREPARED_PACKAGE)
    state_path = (
        ROOT
        / "outputs/production/star-external-retrieval-wave-001/execution/"
        "execution_state.json"
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    lineage = derive_api_lineage(root=ROOT, state=state)

    assert len(package.snapshot_dataset.occurrences) == 1333
    assert len(package.snapshot_dataset.canonical_records) == 958
    assert len(package.identity_proposals) == 424
    assert len(package.relationship_proposals) == 50
    assert lineage["attempt_events"] == 60
    assert lineage["saved_responses"] == 37
    assert [item["attempt_events"] for item in lineage["episodes_counted_once"]] == [
        15, 10, 7, 19, 6, 3
    ]
    assert [item["saved_responses"] for item in lineage["episodes_counted_once"]] == [
        7, 1, 1, 19, 6, 3
    ]


def test_real_package_refuses_changed_manifest_binding() -> None:
    if not PREPARED_PACKAGE.is_dir():
        pytest.skip("persisted integration package is not present")
    with pytest.raises(
        ArxivSnapshotIntegrationError, match="package manifest hash changed"
    ):
        validate_prepared_package(
            PREPARED_PACKAGE, expected_manifest_sha256="0" * 64
        )
