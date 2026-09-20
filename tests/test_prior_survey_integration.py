from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import h2h_lit.prior_survey_integration as integration
from h2h_lit.artifact_import import merge_identification_datasets
from h2h_lit.prior_survey_integration import (
    PriorSurveyIntegrationError,
    authorization_state_fingerprint,
    authorize_prior_survey_package_locked,
    merge_identification_datasets_with_prior_surveys_and_snapshot,
    validate_authorized_prior_survey_imports,
    validate_prior_survey_package,
)


def _state() -> dict:
    return {
        "schema_version": "1.0.0",
        "execution_id": "wave",
        "status": "COMPLETE",
        "wave_manifest_hash": "wave-hash",
        "planned_wave_raw_sha256": "plan-hash",
        "preflight_raw_sha256": "preflight-hash",
        "sources": {"arXiv": {"status": "PAUSED_TRANSIENT_PROVIDER"}},
        "external_retrieval_completed_at_utc": "2026-09-20T00:00:00Z",
        "external_retrieval_cutoff_date": "2026-09-20",
        "prior_survey_seed_imported": False,
        "identification_set_closed": False,
        "final_global_deduplication_executed": False,
        "screening_executed": False,
        "prisma_generated": False,
        "corpus_modified": False,
    }


def _write_json(path: Path, value: object) -> dict:
    content = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return {
        "path": path.name,
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _write_package(
    root: Path,
    state: dict,
    *,
    seed_set_id: str = "EBK25",
    designation: str = "AUTHOR_SUPPLIED_REFERENCE_LEDGER",
    membership: str = "UNCONFIRMED",
    unresolved: bool = True,
    package_suffix: str = "",
    package_version: str = "test-v1",
) -> tuple[Path, str]:
    package = root / "staging" / f"{seed_set_id.lower()}{package_suffix}"
    package.mkdir(parents=True)
    source = root / "source.txt"
    source.write_text("source evidence\n", encoding="utf-8")
    entries = [
        {
            "entry_id": f"entry-{ordinal}",
            "ordinal": ordinal,
            "raw_citation": json.dumps({"title": "Shared title", "year": year}),
            "title": "Shared title" if seed_set_id == "EBK25" else f"Title {ordinal}",
            "authors": ["Author"],
            "year": year,
            "doi": None,
            "locator": f"Sheet!A{ordinal}",
            "source_survey_membership": membership,
            "our_star_eligibility": "UNASSESSED",
            "provisional_identity_group_id": (
                "group-1" if seed_set_id == "EBK25" else f"group-{ordinal}"
            ),
        }
        for ordinal, year in ((1, 2020), (2, 2021))
    ]
    import_manifest = {
        "schema_version": "1.1.0",
        "run_id": f"prior-survey:{seed_set_id}",
        "query_id": f"prior-survey-query:{seed_set_id}",
        "seed_set_id": seed_set_id,
        "seed_set_version": package_version,
        "originating_review": {"citation": "Test survey"},
        "extraction_method": "test evidence extraction",
        "curator_id": "test",
        "created_at": "2026-09-20T00:00:00+00:00",
        "imported_at": "2026-09-20T00:00:00+00:00",
        "expected_entry_count": 2,
        "source_designation": designation,
        "membership_status": membership,
        "star_eligibility_status": "UNASSESSED",
        "identity_status": (
            "PROVISIONAL_IDENTITIES"
            if seed_set_id == "EBK25"
            else "SUPPORTED_SOURCE_IDENTITIES"
        ),
        "entries": entries,
    }
    import_ref = _write_json(package / "import_manifest.json", import_manifest)
    groups = [
        {
            "group_id": "group-1",
            "occurrence_entry_ids": ["entry-1", "entry-2"],
            "identity_resolution": "UNRESOLVED" if unresolved else "SUPPORTED",
            "reason": "conflicting year evidence" if unresolved else "source ID",
            "source_survey_membership": membership,
        }
    ]
    if seed_set_id != "EBK25":
        groups = [
            {
                "group_id": f"group-{ordinal}",
                "occurrence_entry_ids": [f"entry-{ordinal}"],
                "identity_resolution": "SUPPORTED",
                "reason": "author source ID",
                "source_survey_membership": membership,
            }
            for ordinal in (1, 2)
        ]
    review_ref = _write_json(
        package / "identity_review.json",
        {
            "schema_version": "1.0.0",
            "status": "REVIEWED_WITH_UNCERTAINTY_PRESERVED",
            "seed_set_id": seed_set_id,
            "groups": groups,
        },
    )
    identities = 1 if seed_set_id == "EBK25" else 2
    manifest = {
        "schema_version": "1.0.0",
        "status": "READY_FOR_EXPLICIT_AUTHORIZATION",
        "package_id": f"package-{seed_set_id}",
        "package_version": package_version,
        "seed_set_id": seed_set_id,
        "source_designation": designation,
        "membership_status": membership,
        "identity_status": import_manifest["identity_status"],
        "star_eligibility_status": "UNASSESSED",
        "production_eligible": False,
        "prisma_counted": False,
        "production_state_binding": {
            "authorization_state_fingerprint": authorization_state_fingerprint(state)
        },
        "source_artifacts": [
            {
                "path": "source.txt",
                "path_scope": "repository_relative",
                "byte_size": source.stat().st_size,
                "raw_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            }
        ],
        "source_package_bindings": [],
        "import_manifest": import_ref,
        "identity_review": review_ref,
        "counts": {
            "source_occurrences": 2,
            "provisional_identity_groups": identities,
            "confirmed_memberships": identities if membership == "CONFIRMED_SURVEY_MEMBER" else 0,
            "unknown_memberships": identities if membership == "UNCONFIRMED" else 0,
            "background_references": 0,
            "unresolved_identity_groups": 1 if unresolved else 0,
            "occurrence_membership_counts": {membership: 2},
        },
        "global_merge_contract": {
            "helper": "merge_identification_datasets_with_prior_surveys_and_snapshot",
            "snapshot_identity_adjudications": 424,
            "snapshot_related_version_links": 50,
            "identification_closure_allowed": False,
        },
    }
    manifest_ref = _write_json(package / "package_manifest.json", manifest)
    return package, manifest_ref["raw_sha256"]


def test_qualified_ebk25_ledger_preserves_occurrences_and_uncertainty(tmp_path):
    state = _state()
    package, digest = _write_package(tmp_path, state)
    validated = validate_prior_survey_package(
        root=tmp_path,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        state=state,
    )

    assert len(validated.dataset.occurrences) == 2
    assert len(validated.dataset.canonical_records) == 1
    assert validated.dataset.canonical_records[0].metadata["identity_resolution"] == "UNRESOLVED"
    assert all(
        item.record.original_metadata["source_survey_membership"] == "UNCONFIRMED"
        for item in validated.dataset.occurrences
    )
    assert all(
        item.provenance.metadata["identity_authority"]
        == "PROVISIONAL_SOURCE_GROUPING_NOT_ADJUDICATED"
        for item in validated.dataset.duplicate_decisions
    )


def test_supported_jfr25_designation_preserves_confirmed_memberships(tmp_path):
    state = _state()
    package, digest = _write_package(
        tmp_path,
        state,
        seed_set_id="JFR25",
        designation="SUPPORTED_AUTHOR_COMPANION_MEMBERSHIP_LIST",
        membership="CONFIRMED_SURVEY_MEMBER",
        unresolved=False,
    )
    validated = validate_prior_survey_package(
        root=tmp_path,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        state=state,
    )

    assert len(validated.dataset.occurrences) == 2
    assert len(validated.dataset.canonical_records) == 2
    assert all(
        item.record.original_metadata["source_survey_membership"]
        == "CONFIRMED_SURVEY_MEMBER"
        for item in validated.dataset.occurrences
    )


def test_authorization_is_idempotent_only_after_provenance_validation(tmp_path):
    state = _state()
    package, digest = _write_package(tmp_path, state)
    state, first = authorize_prior_survey_package_locked(
        root=tmp_path,
        state=state,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        authorized_at="2026-09-20T01:00:00Z",
    )
    state, second = authorize_prior_survey_package_locked(
        root=tmp_path,
        state=state,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        authorized_at="2026-09-20T02:00:00Z",
    )

    assert first == second
    assert state["prior_survey_seed_imported"] is False
    assert state["identification_set_closed"] is False
    validate_authorized_prior_survey_imports(root=tmp_path, state=state)

    source_path = tmp_path / "source.txt"
    source_original = source_path.read_text(encoding="utf-8")
    source_path.write_text("tampered source\n", encoding="utf-8")
    with pytest.raises(PriorSurveyIntegrationError, match="bound artifact changed"):
        validate_authorized_prior_survey_imports(root=tmp_path, state=state)
    source_path.write_text(source_original, encoding="utf-8")

    dataset_path = tmp_path / first["dataset"]["path"]
    dataset_path.write_text("tampered", encoding="utf-8")
    with pytest.raises(PriorSurveyIntegrationError, match="bound artifact changed"):
        authorize_prior_survey_package_locked(
            root=tmp_path,
            state=state,
            package_dir=package,
            expected_package_manifest_sha256=digest,
            authorized_at="2026-09-20T03:00:00Z",
        )


def test_authorization_recovers_safe_matching_artifacts_before_state_registration(
    tmp_path,
):
    state = _state()
    package, digest = _write_package(tmp_path, state)
    state, first = authorize_prior_survey_package_locked(
        root=tmp_path,
        state=state,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        authorized_at="2026-09-20T01:00:00Z",
    )
    state.pop("prior_survey_imports")

    state, recovered = authorize_prior_survey_package_locked(
        root=tmp_path,
        state=state,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        authorized_at="2026-09-20T02:00:00Z",
    )

    assert recovered["authorized_at_utc"] == first["authorized_at_utc"]
    validate_authorized_prior_survey_imports(root=tmp_path, state=state)


def test_package_rejects_tampering_state_drift_and_conflicting_registration(tmp_path):
    state = _state()
    package, digest = _write_package(tmp_path, state)
    drifted = json.loads(json.dumps(state))
    drifted["sources"]["arXiv"]["status"] = "FAILED"
    with pytest.raises(PriorSurveyIntegrationError, match="production-state drift"):
        validate_prior_survey_package(
            root=tmp_path,
            package_dir=package,
            expected_package_manifest_sha256=digest,
            state=drifted,
        )

    import_path = package / "import_manifest.json"
    original = import_path.read_text(encoding="utf-8")
    import_path.write_text(original + " ", encoding="utf-8")
    with pytest.raises(PriorSurveyIntegrationError, match="bound artifact changed"):
        validate_prior_survey_package(
            root=tmp_path,
            package_dir=package,
            expected_package_manifest_sha256=digest,
            state=state,
        )
    import_path.write_text(original, encoding="utf-8")

    state, _ = authorize_prior_survey_package_locked(
        root=tmp_path,
        state=state,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        authorized_at="2026-09-20T01:00:00Z",
    )
    changed_package, changed = _write_package(
        tmp_path,
        state,
        package_suffix="-conflict",
        package_version="test-v2",
    )
    with pytest.raises(PriorSurveyIntegrationError, match="conflicting"):
        authorize_prior_survey_package_locked(
            root=tmp_path,
            state=state,
            package_dir=changed_package,
            expected_package_manifest_sha256=changed,
            authorized_at="2026-09-20T02:00:00Z",
        )


def test_global_merge_routes_through_snapshot_helper_and_marks_uncertainty(
    tmp_path, monkeypatch
):
    state = _state()
    package, digest = _write_package(tmp_path, state)
    dataset = validate_prior_survey_package(
        root=tmp_path,
        package_dir=package,
        expected_package_manifest_sha256=digest,
        state=state,
    ).dataset
    called = False

    def snapshot_merge(datasets, snapshot_package, *, created_at=None):
        nonlocal called
        called = True
        assert snapshot_package == "snapshot"
        return merge_identification_datasets(datasets)

    monkeypatch.setattr(
        integration,
        "merge_identification_datasets_with_snapshot_integration",
        snapshot_merge,
    )
    merged = merge_identification_datasets_with_prior_surveys_and_snapshot(
        [dataset], "snapshot"
    )

    assert called is True
    assert len(merged.occurrences) == 2
    assert merged.canonical_records[0].metadata["prior_survey_identity_status"] == "UNRESOLVED"
