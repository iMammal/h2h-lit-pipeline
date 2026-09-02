from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from h2h_lit.production_query_plan import (
    PRODUCTION_SELECTION,
    build_production_query_plan,
    load_production_query_plan,
)
from h2h_lit.production_wave import (
    REQUIRED_IDENTIFICATION_SOURCES_V2,
    REQUIRED_SUPPORT_SOURCES_V2,
    ProductionRetrievalWave,
    ProductionWaveStatus,
    compute_query_plan_hash,
    preflight_production_wave,
)
from h2h_lit.query_development import load_candidate_set, load_sizing_run

ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "config" / "star_production_query_plan_v1.json"
CANDIDATE_PATH = ROOT / "config" / "star_query_candidates_v0_4.json"
CONTROLS_PATH = ROOT / "config" / "star_query_semantic_controls_v0_3.json"
SENTINEL_PATH = ROOT / "config" / "star_query_sentinels_v0_1.json"
PLAN_HASH = "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"


def _plan():
    return load_production_query_plan(PLAN_PATH, root=ROOT)


def test_v0_4_checksum_manifest_matches_final_readable_artifacts() -> None:
    manifest = json.loads(
        (ROOT / "provenance" / "star_query_sizing_v0_4_checksum_manifest.json").read_text()
    )
    assert manifest["experiment_status"].endswith("after_bounded_resume")
    assert manifest["prior_checkpoint_provenance"]["status"].startswith("overwritten")
    assert manifest["immutable_artifact_storage"]["status"] == "unresolved"
    assert "do not preserve" in manifest["immutable_artifact_storage"]["limitation"]
    assert manifest["semantic_scholar_gate"]["state"] == "passed"
    assert manifest["containment_evaluation"]["count_containment"]["state"] == "passed"
    assert all(
        item["state"] == "passed"
        for item in manifest["containment_evaluation"]["sentinel_containment"]
    )
    for artifact in manifest["artifacts"]:
        path = ROOT / artifact["path"]
        assert path.stat().st_size == artifact["byte_size"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["raw_sha256"]
    final_run = load_sizing_run(
        ROOT / "outputs/query_sizing/star-query-sizing-v0-4-run-001/query_sizing_run.json"
    )
    by_role = {item["role"]: item for item in manifest["artifacts"]}
    assert final_run.run_hash() == by_role["final_completed_run"]["canonical_hash"]
    assert final_run.dry_run_plan_hash == by_role["dry_run_plan"]["canonical_hash"]


def test_production_plan_is_deterministic_and_matches_tracked_artifact() -> None:
    plan = _plan()
    rebuilt = build_production_query_plan(
        CANDIDATE_PATH,
        CONTROLS_PATH,
        SENTINEL_PATH,
        root=ROOT,
    )
    assert plan.plan_hash() == PLAN_HASH
    assert rebuilt.plan_hash() == PLAN_HASH
    assert rebuilt.to_json() == PLAN_PATH.read_text(encoding="utf-8")


def test_exact_five_family_freeze_and_comparators_are_inactive() -> None:
    data = _plan().payload
    assert len(data["families"]) == 5
    assert {
        item["family_id"]: item["production_variant"] for item in data["families"]
    } == PRODUCTION_SELECTION
    assert all(item["production_status"] == "active" for item in data["families"])
    assert data["families"][0]["historical_comparator_variants"] == ["anchored"]
    assert data["families"][1]["historical_comparator_variants"] == ["A", "B", "C", "D"]
    assert data["families"][2]["historical_comparator_variants"] == ["default"]
    active = {(item["family_id"], item["variant_id"]) for item in data["source_queries"]}
    assert active == set(PRODUCTION_SELECTION.items())


def test_frozen_expressions_and_inherited_vocabulary_are_unchanged() -> None:
    candidate = load_candidate_set(CANDIDATE_PATH)
    data = _plan().payload
    for family in data["families"]:
        expected = candidate.payload["families"][family["family_id"]]["variants"][
            family["production_variant"]
        ]
        assert family["conceptual_expression"] == expected
    assert data["families"][1]["conceptual_expression"] == (
        "{L} AND ({V} OR {S}) AND "
        "({A_HIGH} OR ({A_GENERIC} AND {ASSISTANCE_CONTEXT}))"
    )
    assert data["families"][2]["conceptual_expression"] == (
        "{L} AND ({S} OR {V_HIGH}) AND "
        "({R_HIGH} OR ({R_BROAD} AND {RELATIONAL_CONTEXT}))"
    )
    assert "sizing did not establish incremental recall" in data["families"][2][
        "methodological_purpose"
    ]


def test_source_roles_and_source_specific_mechanics_are_frozen() -> None:
    data = _plan().payload
    roles = {item["source"]: item for item in data["source_roles"]}
    assert roles["PubMed"]["role"] == "primary_identification"
    assert roles["EuropePMC"]["role"] == "primary_identification"
    assert roles["IEEEXplore"]["role"] == "required_supplemental_identification"
    assert roles["ACMDigitalLibrary"]["transport"] == "artifact_import"
    assert roles["SemanticScholar"]["mode"] == "bulk"
    assert roles["CrossRef"]["production_identification"] is False
    assert not any(item["source"] == "CrossRef" for item in data["source_queries"])

    pubmed = [item for item in data["source_queries"] if item["source"] == "PubMed"]
    assert len(pubmed) == 5
    assert all(item["request_specification"]["method"] == "POST" for item in pubmed)
    assert all(
        item["request_specification"]["headers"]["content-type"]
        == "application/x-www-form-urlencoded"
        for item in pubmed
    )
    assert all(
        item["request_specification"]["form"]["term"] == item["query_text"]
        for item in pubmed
    )
    assert all(
        re.search(r"\)[Title/Abstract]", item["query_text"]) is None for item in pubmed
    )

    semantic = [
        item for item in data["source_queries"] if item["source"] == "SemanticScholar"
    ]
    assert len(semantic) == 5
    assert all(item["mode"] == "bulk" for item in semantic)
    assert all(
        item["request_specification"]["semantic_control_gate"]
        == "bulk_boolean_semantics"
        for item in semantic
    )
    assert all(" + " in item["query_text"] for item in semantic)
    assert all(" AND " not in item["query_text"] for item in semantic)


def test_prerequisites_partitions_and_execution_boundaries_remain_explicit() -> None:
    data = _plan().payload
    assert data["partitions"] == []
    assert data["automatic_partitioning"] is False
    assert data["automatic_query_rewriting"] is False
    assert data["automatic_mode_switching"] is False
    assert all(value is False for value in data["execution_boundaries"].values())
    assert data["required_prior_survey_seed_manifests"] == [
        {"seed_set_id": item, "status": "required_unresolved", "manifest": None}
        for item in ("EBK25", "JFR25", "FP19")
    ]
    prerequisites = set(data["unresolved_prerequisites"])
    assert "IEEE_XPLORE_API_KEY" in prerequisites
    assert "acm_institutional_access_and_operator" in prerequisites
    assert {"EBK25_seed_manifest", "JFR25_seed_manifest", "FP19_seed_manifest"} <= prerequisites


def test_phase4a_schema_1_1_separates_identification_and_support_sources(tmp_path: Path) -> None:
    data = _plan().payload["phase4a_compatibility"]
    assert data["required_identification_sources"] == list(REQUIRED_IDENTIFICATION_SOURCES_V2)
    assert data["required_support_sources"] == list(REQUIRED_SUPPORT_SOURCES_V2)
    assert data["wave_instantiated"] is False

    wave = ProductionRetrievalWave(
        schema_version="1.1.0",
        wave_id="compatibility-only",
        wave_version="1.0.0",
        query_plan_version="star-production-query-plan-v1",
        query_plan_hash="",
        required_sources=list(REQUIRED_IDENTIFICATION_SOURCES_V2),
        support_sources=list(REQUIRED_SUPPORT_SOURCES_V2),
        query_families=[],
        status=ProductionWaveStatus.PLANNED,
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    report = preflight_production_wave(wave, manifest_root=tmp_path)
    codes = {item.code for item in report.issues}
    assert "UNSUPPORTED_SCHEMA" not in codes
    assert "REQUIRED_SOURCE_LIST_MISMATCH" not in codes
    assert "SUPPORT_SOURCE_LIST_MISMATCH" not in codes
    assert "CROSSREF_IDENTIFICATION_PROHIBITED" not in codes
    assert "MISSING_REQUIRED_SOURCE_QUERY" in codes
    assert report.ready is False


def test_historical_candidate_configs_remain_byte_identical() -> None:
    expected = {
        "star_query_candidates_v0_1.json": "3de17186d0c1fc50b5379819370cb4fefea699c6b9b7c6223cb3d8abe4cdfb02",
        "star_query_candidates_v0_2.json": "d58eef9a791474023061c38e4215e441ce789e12ff6a329dcbca501f2a94deac",
        "star_query_candidates_v0_3.json": "06f7d72da6b22d22ebf29923d465e49aec1df69e0cdf51487f24e4486e3a3b1c",
        "star_query_candidates_v0_4.json": "88bd3ba4c5ed3140d916b4ffa05f12a32df3d3ff1561d659bed291a4b56c0d46",
    }
    for name, digest in expected.items():
        assert hashlib.sha256((ROOT / "config" / name).read_bytes()).hexdigest() == digest
