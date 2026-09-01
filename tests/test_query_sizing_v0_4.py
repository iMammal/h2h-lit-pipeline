from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace

from h2h_lit.query_development import (
    SentinelDiagnosticOutcome,
    SizingGateStatus,
    SizingTransportStatus,
    load_candidate_set,
    load_sentinel_set,
)
from h2h_lit.query_sizing import build_sizing_dry_run, save_sizing_dry_run
from h2h_lit.query_sizing_live import (
    _evaluate_containment,
    load_validated_sizing_plan,
)

ROOT = Path(__file__).resolve().parents[1]
V1 = ROOT / "config" / "star_query_candidates_v0_1.json"
V2 = ROOT / "config" / "star_query_candidates_v0_2.json"
V3 = ROOT / "config" / "star_query_candidates_v0_3.json"
V4 = ROOT / "config" / "star_query_candidates_v0_4.json"
CONTROLS = ROOT / "config" / "star_query_semantic_controls_v0_3.json"
SENTINELS = ROOT / "config" / "star_query_sentinels_v0_1.json"
V4_HASH = "324903e8c62dbae656a429754f3d5695b174f2a36a9dc4de74aecd86e83a8600"


def _report() -> dict:
    return build_sizing_dry_run(
        V4,
        SENTINELS,
        run_id="star-query-sizing-v0-4-run-001",
        created_at="2026-09-01T22:00:00Z",
        semantic_control_config=CONTROLS,
    )


def _plan(tmp_path: Path):
    path = tmp_path / "dry_run.json"
    save_sizing_dry_run(_report(), path)
    return load_validated_sizing_plan(path, V4, SENTINELS)


def test_v0_3_checksum_manifest_matches_frozen_artifacts() -> None:
    manifest = json.loads(
        (ROOT / "provenance" / "star_query_sizing_v0_3_checksum_manifest.json").read_text()
    )
    assert manifest["executor_git_commit"] == "eda0d0daecdb3636245c9a7e87f679260af0537c"
    assert manifest["immutable_artifact_storage"]["status"] == "unresolved"
    assert "do not preserve" in manifest["immutable_artifact_storage"]["limitation"]
    for artifact in manifest["artifacts"]:
        path = ROOT / artifact["path"]
        assert path.stat().st_size == artifact["byte_size"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["raw_sha256"]


def test_v0_4_inherits_terms_and_preserves_frozen_families() -> None:
    v3 = load_candidate_set(V3)
    v4 = load_candidate_set(V4)
    assert v4.candidate_set_version == "0.4.0-preproduction-bounded"
    assert v4.candidate_set_hash() == V4_HASH
    assert v4.payload["blocks"] == v3.payload["blocks"]
    assert v4.payload["anchors"] == v3.payload["anchors"]
    for family, variant in (
        ("STAR-QF01-RELATIONAL-VIS", "unanchored"),
        ("STAR-QF04-NONDESKTOP-ENV", "default"),
        ("STAR-QF05-CONVERSATIONAL", "default"),
    ):
        assert v4.payload["families"][family]["variants"][variant] == (
            v3.payload["families"][family]["variants"][variant]
        )
    assert load_sentinel_set(SENTINELS).sentinel_set_hash() == (
        "1acc34ae05f0637bdfb5d3feebe2044197164ae4b6d6ffd0e043daa63bfd46a3"
    )


def test_v0_4_adds_only_approved_expressions_using_existing_blocks() -> None:
    v4 = load_candidate_set(V4)
    qf02_e = v4.payload["families"]["STAR-QF02-ASSISTED-VIS"]["variants"]["E"]
    qf03 = v4.payload["families"]["STAR-QF03-INTERACTIVE-SYSTEMS"]["variants"][
        "revised"
    ]
    assert qf02_e == (
        "{L} AND ({V} OR {S}) AND "
        "({A_HIGH} OR ({A_GENERIC} AND {ASSISTANCE_CONTEXT}))"
    )
    assert qf03 == (
        "{L} AND ({S} OR {V_HIGH}) AND "
        "({R_HIGH} OR ({R_BROAD} AND {RELATIONAL_CONTEXT}))"
    )
    placeholders = {"L", "V", "S", "A_HIGH", "A_GENERIC", "ASSISTANCE_CONTEXT"}
    assert placeholders == set(re.findall(r"\{([A-Z_]+)\}", qf02_e))
    assert v4.payload["blocks"] == load_candidate_set(V3).payload["blocks"]


def test_v0_4_bounded_plan_has_exact_matrix_and_boundaries(tmp_path: Path) -> None:
    report = _report()
    assert report == _report()
    specs = report["candidate_specifications"]
    assert len(specs) == 10
    assert len(report["sentinel_identity_specifications"]) == 20
    assert len(report["sentinel_diagnostic_specifications"]) == 34
    assert len(report["semantic_control_specifications"]) == 6
    assert len(report["containment_assertion_specifications"]) == 2
    assert not any(item["source"] == "CrossRef" for item in specs)
    assert not any(
        item["family_id"] in {
            "STAR-QF01-RELATIONAL-VIS",
            "STAR-QF04-NONDESKTOP-ENV",
            "STAR-QF05-CONVERSATIONAL",
        }
        for item in specs
    )
    assert {(item["variant_id"], item["source"]) for item in specs if item["family_id"].startswith("STAR-QF02")} == {
        ("E", "PubMed"),
        ("E", "EuropePMC"),
        ("E", "SemanticScholar"),
        ("E", "arXiv"),
        ("C", "SemanticScholar"),
        ("D", "SemanticScholar"),
    }
    assert "partition" not in json.dumps(specs).lower()
    assert all(
        value is False
        for key, value in report["non_production_invariants"].items()
        if key != "network_calls_performed"
    )
    assert report["non_production_invariants"]["network_calls_performed"] == 0
    requirements = {item["source"]: item for item in report["source_requirements"]}
    for source in ("IEEEXplore", "ACMDigitalLibrary"):
        assert requirements[source]["required_for_live_execution"] is False
        assert requirements[source]["status"] == (
            "orthogonal_pending_not_part_of_bounded_matrix"
        )
    plan = _plan(tmp_path)
    assert len(plan.containment_specs) == 2


def test_v0_4_preserves_pubmed_post_and_semantic_gate_order() -> None:
    report = _report()
    pubmed = [item for item in report["candidate_specifications"] if item["source"] == "PubMed"]
    assert len(pubmed) == 2
    assert all(item["request"]["method"] == "POST" for item in pubmed)
    assert all(item["request"]["params"] == {} for item in pubmed)
    assert all(
        item["request"]["headers"]
        == {"content-type": "application/x-www-form-urlencoded"}
        for item in pubmed
    )
    semantic = [
        item for item in report["candidate_specifications"] if item["source"] == "SemanticScholar"
    ]
    assert len(semantic) == 4
    assert report["semantic_control_provenance"]["gate"] == "bulk_boolean_semantics"
    assert all("bulk_boolean_semantics_control_required" in item["syntax_gates"] for item in semantic)


def test_containment_assertions_pass_and_fail_without_selecting_a_variant(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    subset_id = "candidate:STAR-QF02-ASSISTED-VIS:C:SemanticScholar"
    superset_id = "candidate:STAR-QF02-ASSISTED-VIS:D:SemanticScholar"
    observations = [
        SimpleNamespace(
            candidate_query_id=subset_id,
            transport_status=SizingTransportStatus.SUCCEEDED,
            reported_count=10,
        ),
        SimpleNamespace(
            candidate_query_id=superset_id,
            transport_status=SizingTransportStatus.SUCCEEDED,
            reported_count=20,
        ),
    ]
    sentinel_ids = ["sentinel:icave-2017", "sentinel:wang-et-al-2025", "sentinel:phenoflow-2025"]
    diagnostics = [
        SimpleNamespace(candidate_query_id=candidate, sentinel_id=sentinel, outcome=outcome)
        for sentinel in sentinel_ids
        for candidate, outcome in (
            (subset_id, SentinelDiagnosticOutcome.INDEXED_AND_MATCHED),
            (superset_id, SentinelDiagnosticOutcome.INDEXED_AND_MATCHED),
        )
    ]
    run = SimpleNamespace(observations=observations, sentinel_diagnostics=diagnostics)
    passed = _evaluate_containment(plan, run, "2026-09-01T22:01:00Z")
    assert passed.state is SizingGateStatus.PASSED
    assert passed.selection_status == "containment_supported"

    diagnostics[-1].outcome = SentinelDiagnosticOutcome.INDEXED_BUT_QUERY_MISSED
    failed = _evaluate_containment(plan, run, "2026-09-01T22:02:00Z")
    assert failed.state is SizingGateStatus.FAILED
    assert failed.selection_status == "unresolved"
    assert not any("freeze" in reason or "select" in reason for reason in failed.reasons)


def test_legacy_configs_and_hashes_remain_unchanged() -> None:
    expected = {
        V1: "4c642ff04c84c1e1534566d789278fdab21af9f75a57332fce04fa3751fe01bc",
        V2: "701bba1a7b40ba508b41df6a8d03d340449b5f67f8ed89ee9e9ad3dcf7cfeaf2",
        V3: "5add42cd86317a958951917ef5adcdbef3d70cf300f7c4f7511d9a0242ea0b5f",
    }
    for path, digest in expected.items():
        assert load_candidate_set(path).candidate_set_hash() == digest
