from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from h2h_lit.sources.jfr25_seed import (
    EXPECTED_APPLICATION_COUNT,
    EXPECTED_RAW_ROW_COUNT,
    EXPECTED_SHARED_IDS,
    EXPECTED_STUDY_COUNT,
    EXPECTED_UNIQUE_MEMBER_COUNT,
    JFR25ExtractionError,
    build_raw_rows_artifact,
    extract_companion_arrays,
)

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = ROOT / "artifacts/prior_review_seeds/JFR25/snapshot-2026-09-02"
BUNDLE = SNAPSHOT / "companion_bundle_f173585a891c8de6.js"
RAW_ROWS = SNAPSHOT / "jfr25_raw_category_rows.json"
MANIFEST = ROOT / "config/star_retrieval_prerequisites_v1/seed_jfr25.json"

EXPECTED_ARTIFACT_HASHES = {
    "companion_landing_page": "7e39caee2c907dd28803a83ded9e72d0d0e43b9957ab5402649c0ab2e4543e3f",
    "membership_bundle": "d5df84441630b9a5576041475b2edb47b030f4ef46530f1a3283b2f5502d4707",
    "arxiv_metadata_and_version_history": (
        "ff2332c87f1ed2d38b3d12dc78a0f25182ce131c5dc1019cb212e2e6d5707eb7"
    ),
    "arxiv_v1_pdf_version_evidence": (
        "9952e02cae6f82b44da3cdef2ea4d66627fdba970c057423920c3358527b9059"
    ),
}


def _manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def test_manifest_preserves_exact_membership_reconciliation() -> None:
    manifest = _manifest()
    reconciliation = manifest["reconciliation"]
    assert reconciliation["application_rows"] == EXPECTED_APPLICATION_COUNT
    assert reconciliation["study_rows"] == EXPECTED_STUDY_COUNT
    assert reconciliation["raw_category_rows"] == EXPECTED_RAW_ROW_COUNT
    assert tuple(reconciliation["shared_source_ids"]) == EXPECTED_SHARED_IDS
    assert reconciliation["shared_source_id_count"] == len(EXPECTED_SHARED_IDS)
    assert reconciliation["unique_members"] == EXPECTED_UNIQUE_MEMBER_COUNT
    assert reconciliation["identity_key"] == "companion_site_explicit_source_id"
    assert reconciliation["fuzzy_or_title_matching_used"] is False

    entries = manifest["entries"]
    assert len(entries) == EXPECTED_UNIQUE_MEMBER_COUNT
    assert [entry["ordinal"] for entry in entries] == list(range(1, 139))
    assert len({entry["source_member_id"] for entry in entries}) == 138
    assert sum(len(entry["source_rows"]) for entry in entries) == 146


def test_raw_artifacts_and_deterministic_extraction_when_snapshot_is_available() -> None:
    if not BUNDLE.exists() or not RAW_ROWS.exists():
        pytest.skip("ignored acquisition snapshot is not present in this checkout")
    bundle = BUNDLE.read_bytes()
    applications_a, studies_a = extract_companion_arrays(bundle)
    applications_b, studies_b = extract_companion_arrays(bundle)
    assert applications_a == applications_b
    assert studies_a == studies_b

    expected = json.loads(RAW_ROWS.read_text(encoding="utf-8"))
    regenerated = build_raw_rows_artifact(
        applications_a,
        studies_a,
        source_bundle_sha256=hashlib.sha256(bundle).hexdigest(),
    )
    assert regenerated == expected
    assert len(regenerated["raw_category_rows"]) == 146


def test_acquisition_hashes_and_http_provenance_are_frozen() -> None:
    artifacts = {
        item["artifact_role"]: item for item in _manifest()["source_artifacts"]
    }
    assert {role: item["raw_sha256"] for role, item in artifacts.items()} == (
        EXPECTED_ARTIFACT_HASHES
    )
    assert all(item["http_status"] == 200 for item in artifacts.values())
    assert all(not Path(item["relative_path"]).is_absolute() for item in artifacts.values())
    assert all(".." not in Path(item["relative_path"]).parts for item in artifacts.values())
    if SNAPSHOT.exists():
        for item in artifacts.values():
            path = ROOT / item["relative_path"]
            assert hashlib.sha256(path.read_bytes()).hexdigest() == item["raw_sha256"]


def test_source_rows_preserve_metadata_without_star_decisions() -> None:
    manifest = _manifest()
    first_source_record = manifest["entries"][0]["source_rows"][0]["source_record"]
    assert {"id", "paper_bib_tag", "title", "authors", "year", "doi", "abstract"} <= set(
        first_source_record
    )
    assert any("tool_name" in row["source_record"] for entry in manifest["entries"] for row in entry["source_rows"])
    assert any("study_focus" in row["source_record"] for entry in manifest["entries"] for row in entry["source_rows"])

    serialized = MANIFEST.read_text(encoding="utf-8")
    for forbidden in (
        '"star_eligibility"',
        '"assistance_modes"',
        '"visualization_modalities"',
        '"task_annotations"',
        '"synthesis_priority"',
        '"corpus_membership"',
    ):
        assert forbidden not in serialized
    assert manifest["occurrences_created"] == 0


def test_version_and_licensing_qualifications_are_explicit() -> None:
    manifest = _manifest()
    assert manifest["source_version"]["companion_site_reported_last_update"] == "2025-12-14"
    assert manifest["source_version"]["arxiv_v1_submitted"] == "2025-01-15"
    assert "not asserted to be byte-identical" in manifest["source_version"]["qualification"]
    assert manifest["licensing"]["redistribution_permission"] == "UNRESOLVED"
    assert manifest["licensing"]["explicit_corpus_data_redistribution_license_observed"] is False


def test_count_drift_fails_closed_without_fuzzy_reconciliation() -> None:
    if not BUNDLE.exists():
        pytest.skip("ignored acquisition snapshot is not present in this checkout")
    applications, studies = extract_companion_arrays(BUNDLE.read_bytes())
    with pytest.raises(JFR25ExtractionError, match="application row count changed"):
        build_raw_rows_artifact(
            applications[:-1],
            studies,
            source_bundle_sha256="0" * 64,
        )
