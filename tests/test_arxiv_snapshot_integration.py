from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from h2h_lit.arxiv_snapshot_integration import (
    DIRECT_IDENTITY_MATCH_RULE,
    PROPAGATED_IDENTITY_MATCH_RULE,
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


def _canonical_group_propagation_fixture(
    *,
    prior_arxiv_id: str | None = None,
    prior_identity_resolution: str = "PROVISIONAL",
) -> tuple[list[ReviewDataset], ValidatedSnapshotPackage]:
    title = "A Multi-scale Visual Analytics Approach for Exploring Biomedical Knowledge"
    doi = "10.1109/vahc53616.2021.00010"
    arxiv_id = "2109.06828"
    existing = _dataset(
        source="SemanticScholar",
        run_id="semantic-scholar-run",
        records=[
            (
                occurrence_id,
                LiteratureRecord(
                    title=title.replace("Multi-scale", "Multi-Scale"),
                    authors=["Fahd Husain", "Rosa Romero-Gómez"],
                    year=2021,
                    doi=doi,
                    arxiv_id=arxiv_id,
                    source_identifier="3b33acee6261b11aefb05ee631627707d3096004",
                    source_database="SemanticScholar",
                ),
            )
            for occurrence_id in (
                "semantic-alpha-qf01",
                "semantic-alpha-qf03",
                "semantic-alpha-qf05",
            )
        ],
    )
    snapshot = _dataset(
        source="arXivSnapshotV303",
        run_id="arxiv-snapshot-v303:2109.06828",
        records=[
            (
                occurrence_id,
                LiteratureRecord(
                    title=title,
                    authors=["Fahd Husain", "Rosa Romero-Gomez"],
                    year=2021,
                    arxiv_id=arxiv_id,
                    source_identifier=arxiv_id,
                    source_database="arXivSnapshotV303",
                ),
            )
            for occurrence_id in ("snapshot-2109-qf01", "snapshot-2109-qf03")
        ],
    )
    prior = _dataset(
        source="PriorSurveySeed",
        run_id="prior-survey-ebk25",
        records=[
            (
                occurrence_id,
                LiteratureRecord(
                    title=title,
                    authors=["Husain"],
                    year=2021,
                    arxiv_id=prior_arxiv_id if index == 0 else None,
                    source_identifier=f"EBK25:{occurrence_id}",
                    source_database="PriorSurveySeed",
                    original_metadata={
                        "identity_resolution": prior_identity_resolution,
                        "identity_conflict_status": "NO_RECORDED_METADATA_CONFLICT",
                        "source_survey_membership": "UNCONFIRMED",
                        "our_star_eligibility": "UNASSESSED",
                        "provisional_identity_group_id": "ebk25-candidate:2109.06828",
                    },
                ),
            )
            for index, occurrence_id in enumerate(
                ("ebk25-assignments-final-340", "ebk25-nicolas-wip-9")
            )
        ],
    )
    package = ValidatedSnapshotPackage(
        package_dir=Path("."),
        package_manifest={},
        package_manifest_sha256="package-sha",
        import_manifest={},
        amendment_v1={},
        snapshot_dataset=snapshot,
        identity_proposals=[
            {
                "proposal_id": "arxiv-id-link:57bf3d22f010f8f485e372c8",
                "normalized_arxiv_id": arxiv_id,
                "proposed_disposition": "same_bibliographic_record",
                "decision_status": "PROPOSED_NOT_APPLIED",
                "conflicts": [],
                "generic_dedupe_rule_changed": False,
                "candidate_identity": {
                    "arxiv_id": arxiv_id,
                    "staging_candidate_id": "snapshot-candidate:2a8f90acc5db191b70d042c0",
                    "source_line_number": 1529650,
                },
                "existing_identity": {"arxiv_id": arxiv_id},
            }
        ],
        relationship_proposals=[],
    )
    return [existing, snapshot, prior], package


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


def test_direct_adjudication_allows_snapshot_only_doi_group() -> None:
    arxiv_id = "2003.04655"
    existing = _dataset(
        source="SemanticScholar",
        run_id="existing-doi-asymmetric",
        records=[
            (
                "existing-2003",
                LiteratureRecord(
                    title="Lung Infection Quantification of COVID-19 in CT Images with Deep Learning",
                    arxiv_id=arxiv_id,
                    source_identifier="semantic-2003",
                    source_database="SemanticScholar",
                ),
            )
        ],
    )
    snapshot = _dataset(
        source="arXivSnapshotV303",
        run_id="arxiv-snapshot-v303:2003.04655",
        records=[
            (
                "snapshot-2003",
                LiteratureRecord(
                    title="Lung Infection Quantification of COVID-19 in CT Images with Deep Learning",
                    doi="10.1002/mp.14609",
                    arxiv_id=arxiv_id,
                    source_identifier=arxiv_id,
                    source_database="arXivSnapshotV303",
                ),
            )
        ],
    )
    package = ValidatedSnapshotPackage(
        package_dir=Path("."),
        package_manifest={},
        package_manifest_sha256="package-sha",
        import_manifest={},
        amendment_v1={},
        snapshot_dataset=snapshot,
        identity_proposals=[
            {
                "proposal_id": "arxiv-id-link:52c935c86fc26e42a588413e",
                "normalized_arxiv_id": arxiv_id,
                "candidate_identity": {
                    "arxiv_id": arxiv_id,
                    "source_line_number": 1255011,
                },
            }
        ],
        relationship_proposals=[],
    )

    merged = merge_identification_datasets_with_snapshot_integration(
        [existing, snapshot], package, created_at=STAMP
    )

    assert len(merged.canonical_records) == 1
    decision = next(
        item
        for item in merged.effective_duplicate_decisions()
        if item.occurrence_id == "snapshot-2003"
    )
    assert decision.match_rule == DIRECT_IDENTITY_MATCH_RULE
    assert decision.match_key == "arxiv:2003.04655"
    assert (
        merged.retrieval_runs[1]
        .metadata["arxiv_snapshot_v303_integration"]
        ["propagated_generic_title_group_occurrence_count"]
        == 0
    )
    merged.validate()


def test_arxiv_2109_group_propagation_preserves_all_seven_occurrences() -> None:
    datasets, package = _canonical_group_propagation_fixture()
    merged = merge_identification_datasets_with_snapshot_integration(
        datasets, package, created_at=STAMP
    )

    expected_occurrences = {
        "semantic-alpha-qf01",
        "semantic-alpha-qf03",
        "semantic-alpha-qf05",
        "snapshot-2109-qf01",
        "snapshot-2109-qf03",
        "ebk25-assignments-final-340",
        "ebk25-nicolas-wip-9",
    }
    assert {item.occurrence_id for item in merged.occurrences} == expected_occurrences
    assert len(merged.canonical_records) == 1
    target = merged.canonical_records[0]
    assert target.metadata["dedupe_key"] == "doi:10.1109/vahc53616.2021.00010"
    assert set(target.occurrence_ids) == expected_occurrences
    assert all(
        item.metadata.get("dedupe_key")
        != "title:a multiscale visual analytics approach for exploring biomedical knowledge"
        for item in merged.canonical_records
    )

    effective = {
        item.occurrence_id: item for item in merged.effective_duplicate_decisions()
    }
    assert set(effective) == expected_occurrences
    assert all(
        decision.canonical_record_id == target.canonical_id
        and decision.survivor_occurrence_id == target.survivor_occurrence_id
        for decision in effective.values()
    )
    assert {
        item.occurrence_id
        for item in effective.values()
        if item.match_rule == DIRECT_IDENTITY_MATCH_RULE
    } == {"snapshot-2109-qf01", "snapshot-2109-qf03"}
    propagated = {
        item.occurrence_id: item
        for item in effective.values()
        if item.match_rule == PROPAGATED_IDENTITY_MATCH_RULE
    }
    assert set(propagated) == {
        "ebk25-assignments-final-340",
        "ebk25-nicolas-wip-9",
    }
    all_decisions = {item.decision_id: item for item in merged.duplicate_decisions}
    occurrence_by_id = {item.occurrence_id: item for item in merged.occurrences}
    for occurrence_id, decision in propagated.items():
        assert len(decision.provenance.supersedes_ids) == 1
        previous = all_decisions[decision.provenance.supersedes_ids[0]]
        assert previous.occurrence_id == occurrence_id
        assert previous.match_rule == "doi_first_title_fallback"
        assert previous.match_key == decision.match_key
        assert decision.provenance.metadata == {
            "proposal_id": "arxiv-id-link:57bf3d22f010f8f485e372c8",
            "normalized_arxiv_id": "2109.06828",
            "candidate_source_line_number": 1529650,
            "target_resolved_after_global_merge": True,
            "generic_dedupe_rule_changed": False,
            "decision_application": "propagated_generic_title_group_membership",
            "propagated_from_canonical_id": previous.canonical_record_id,
            "propagated_from_decision_id": previous.decision_id,
            "propagated_from_match_key": previous.match_key,
            "propagated_from_match_rule": previous.match_rule,
            "identity_restrictions_preserved": True,
        }
        record_metadata = occurrence_by_id[occurrence_id].record.original_metadata
        assert record_metadata["identity_resolution"] == "PROVISIONAL"
        assert record_metadata["source_survey_membership"] == "UNCONFIRMED"
        assert record_metadata["our_star_eligibility"] == "UNASSESSED"
    marker = merged.retrieval_runs[1].metadata["arxiv_snapshot_v303_integration"]
    assert marker["propagated_generic_title_group_occurrence_count"] == 2
    assert marker["related_version_links"] == []
    merged.validate()


def test_group_propagation_rejects_contradictory_arxiv_identifier() -> None:
    datasets, package = _canonical_group_propagation_fixture(
        prior_arxiv_id="9999.0001"
    )

    with pytest.raises(
        ArxivSnapshotIntegrationError,
        match="generic title group has a contradictory arXiv identifier",
    ):
        merge_identification_datasets_with_snapshot_integration(
            datasets, package, created_at=STAMP
        )


def test_group_propagation_rejects_unresolved_identity_restriction() -> None:
    datasets, package = _canonical_group_propagation_fixture(
        prior_identity_resolution="UNRESOLVED"
    )

    with pytest.raises(
        ArxivSnapshotIntegrationError,
        match="generic title group has an unresolved identity restriction",
    ):
        merge_identification_datasets_with_snapshot_integration(
            datasets, package, created_at=STAMP
        )


def test_group_propagation_survives_snapshot_aware_remerge() -> None:
    datasets, package = _canonical_group_propagation_fixture()
    first = merge_identification_datasets_with_snapshot_integration(
        datasets, package, created_at=STAMP
    )
    later = merge_identification_datasets_with_snapshot_integration(
        [first], package, created_at=STAMP
    )

    assert len(later.occurrences) == 7
    assert len(later.canonical_records) == 1
    assert set(later.canonical_records[0].occurrence_ids) == {
        item.occurrence_id for item in later.occurrences
    }
    effective = later.effective_duplicate_decisions()
    assert sum(item.match_rule == DIRECT_IDENTITY_MATCH_RULE for item in effective) == 2
    assert sum(item.match_rule == PROPAGATED_IDENTITY_MATCH_RULE for item in effective) == 2
    assert all(
        item.record.original_metadata.get("identity_resolution") == "PROVISIONAL"
        for item in later.occurrences
        if item.record.source_database == "PriorSurveySeed"
    )
    later.validate()


def test_group_propagation_remerge_rejects_changed_provenance() -> None:
    datasets, package = _canonical_group_propagation_fixture()
    merged = merge_identification_datasets_with_snapshot_integration(
        datasets, package, created_at=STAMP
    )
    tampered = copy.deepcopy(merged)
    propagated = next(
        item
        for item in tampered.duplicate_decisions
        if item.match_rule == PROPAGATED_IDENTITY_MATCH_RULE
    )
    propagated.provenance.metadata["propagated_from_match_key"] = "title:changed"

    with pytest.raises(
        ArxivSnapshotIntegrationError,
        match="snapshot propagated identity provenance changed",
    ):
        merge_identification_datasets_with_snapshot_integration(
            [tampered], package, created_at=STAMP
        )


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
