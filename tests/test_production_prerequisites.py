from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from h2h_lit.production_prerequisites import (
    ACM_FINAL_RECONCILIATION_PATH,
    EXPECTED_PLAN_HASH,
    EXPECTED_PLAN_RAW_SHA256,
    PRODUCTION_SELECTION,
    ProductionPrerequisiteError,
    ProductionPrerequisitePackage,
    build_prerequisite_payloads,
    load_prerequisite_package,
)
from h2h_lit.production_query_plan import load_production_query_plan

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_PATH = ROOT / "config/star_retrieval_prerequisites_v1.json"
CHILD_ROOT = ROOT / "config/star_retrieval_prerequisites_v1"
PLAN_PATH = ROOT / "config/star_production_query_plan_v1.json"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _package() -> ProductionPrerequisitePackage:
    return load_prerequisite_package(PACKAGE_PATH, root=ROOT)


def test_frozen_plan_and_prerequisite_package_hashes_validate() -> None:
    plan = load_production_query_plan(PLAN_PATH, root=ROOT)
    assert plan.payload["plan_version"] == "1.0.0"
    assert plan.plan_hash() == EXPECTED_PLAN_HASH
    assert _sha(PLAN_PATH) == EXPECTED_PLAN_RAW_SHA256

    package = _package()
    assert package.payload["package_version"] == "1.0.0"
    assert package.payload["package_hash"] == package.package_hash()
    assert package.payload["overall_status"] == "BLOCKED_EXTERNAL_INPUT"


def test_package_regenerates_deterministically_from_frozen_evidence() -> None:
    children, package = build_prerequisite_payloads(
        root=ROOT,
        plan_path=PLAN_PATH,
        generated_at="2026-09-02T00:54:40Z",
        ieee_credential_present=False,
    )
    tracked = json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
    assert package.to_json() == PACKAGE_PATH.read_text(encoding="utf-8")
    for filename, payload in children.items():
        assert json.dumps(payload, sort_keys=True, indent=2) + "\n" == (
            CHILD_ROOT / filename
        ).read_text(encoding="utf-8")
    assert tracked["package_hash"] == package.package_hash()
    assert tracked["updated_at"] == "2026-09-03T18:29:49Z"


def test_ieee_absent_credential_is_explicit_and_never_persisted() -> None:
    ieee = json.loads((CHILD_ROOT / "ieee_readiness.json").read_text())
    assert ieee["status"] == "BLOCKED_CREDENTIAL"
    assert ieee["credential"] == {
        "present": False,
        "required_name": "IEEE_XPLORE_API_KEY",
        "value_persisted": False,
    }
    assert ieee["verification_executed"] is False
    assert len(ieee["queries"]) == 5
    assert len(ieee["verification_requests"]) == 5
    serialized = json.dumps(ieee)
    assert "apikey=" not in serialized
    assert "api_key\":" not in serialized
    for item in ieee["verification_requests"]:
        assert item["request"]["credential_reference"] == "IEEE_XPLORE_API_KEY"
        assert item["request"]["params"]["max_records"] == 1
        assert item["request"]["params"]["start_record"] == 1
        assert len(item["request_hash"]) == 64


def test_ieee_queries_are_exact_plan_queries_and_policy_is_unresolved() -> None:
    plan = json.loads(PLAN_PATH.read_text())
    ieee = json.loads((CHILD_ROOT / "ieee_readiness.json").read_text())
    planned = [item for item in plan["source_queries"] if item["source"] == "IEEEXplore"]
    assert [item["query_text"] for item in ieee["queries"]] == [
        item["query_text"] for item in planned
    ]
    assert ieee["content_policy"]["external_hosted_llm_use"] == "unresolved"
    assert ieee["content_policy"]["abstract_not_required_as_model_input"] is True
    assert ieee["pagination"]["completion_proof"] == "ieee_totalfound_reconciled"
    assert ieee["expected_native_fields"][0] == "article_number"


def test_acm_spec_is_human_only_and_has_required_operator_evidence_fields() -> None:
    plan = json.loads(PLAN_PATH.read_text())
    acm = json.loads((CHILD_ROOT / "acm_operator_spec.json").read_text())
    assert acm["status"] == "RETRIEVAL_EVIDENCE_COMPLETE_NOT_IMPORTED"
    assert acm["operator"]["operator_id"] is None
    assert acm["operator"]["operator_id_required"] is True
    assert acm["workflow"]["scope"] == "ACM Publications"
    assert acm["workflow"]["fields"] == ["Title", "Abstract", "Author Keywords"]
    assert acm["workflow"]["filters"] == {}
    assert acm["workflow"]["browser_automation"] is False
    assert acm["workflow"]["scraping"] is False
    planned = [
        item for item in plan["source_queries"] if item["source"] == "ACMDigitalLibrary"
    ]
    assert [item["query"] for item in acm["queries"]] == [
        item["query_text"] for item in planned
    ]
    assert acm["final_reconciliation"]["path"] == ACM_FINAL_RECONCILIATION_PATH
    assert acm["operator"]["metadata_state"] == "LIMITED_NONBLOCKING_FOR_RETRIEVED_SET"
    assert acm["operator"]["retrieval_completeness_affected"] is False
    for item in acm["queries"]:
        sizing = item["sizing_search_evidence"]
        export = item["citation_export_evidence"]
        assert sizing["status"] == "FIELD_CHILD_EXECUTION_OBSERVATIONS_PRESERVED"
        assert set(sizing["field_counts"]) == {"title", "keyword", "abstract"}
        assert sizing["later_verification_is_completeness_gate"] is False
        assert export["status"] == "FIELD_CHILD_EXPORTS_RECONCILED_NOT_IMPORTED"
        assert export["field_union_unique_count"] > 0
        assert len(export["field_union_digest_sha256"]) == 64
        assert export["production_import_performed"] is False


@pytest.mark.parametrize("seed_id", ["EBK25", "FP19"])
def test_seed_manifest_is_prospective_unpopulated_and_infers_nothing(seed_id: str) -> None:
    seed = json.loads((CHILD_ROOT / f"seed_{seed_id.lower()}.json").read_text())
    assert seed["seed_set_id"] == seed_id
    assert seed["status"] == "UNPOPULATED_REQUIRES_CURATOR_INPUT"
    assert seed["originating_review"]["citation"] is None
    assert seed["originating_review"]["doi"] is None
    assert seed["curator"]["operator_id"] is None
    assert seed["extraction_method"] is None
    assert seed["entries"] == []
    assert seed["expected_entry_count"] is None
    assert seed["import_allowed"] is False
    assert seed["occurrences_created"] == 0
    assert seed["entry_schema"]["eligibility_or_taxonomy_fields_permitted"] is False


def test_jfr25_seed_is_populated_but_not_imported() -> None:
    seed = json.loads((CHILD_ROOT / "seed_jfr25.json").read_text())
    assert seed["seed_set_id"] == "JFR25"
    assert seed["status"] == "POPULATED_VALIDATED_NOT_IMPORTED"
    assert seed["expected_entry_count"] == 138
    assert len(seed["entries"]) == 138
    assert seed["import_allowed"] is True
    assert seed["occurrences_created"] == 0
    assert seed["reconciliation"]["fuzzy_or_title_matching_used"] is False
    assert seed["entry_schema"]["eligibility_or_taxonomy_fields_permitted"] is False


def test_source_windows_use_only_frozen_sizing_and_do_not_partition() -> None:
    windows = json.loads((CHILD_ROOT / "source_window_review.json").read_text())
    assert windows["derivation"] == (
        "frozen_v0_3_and_final_v0_4_sizing_plus_final_acm_retrieval_evidence"
    )
    assert windows["automatic_partitioning"] is False
    assert windows["known_overflows"] == []
    assert len(windows["items"]) == 30
    clear = [item for item in windows["items"] if item["state"] == "RESOLVED_CLEAR"]
    unknown = [item for item in windows["items"] if item["state"] == "UNKNOWN_UNSIZED"]
    acm = [
        item
        for item in windows["items"]
        if item["state"] == "RESOLVED_RETRIEVAL_EVIDENCE"
    ]
    assert len(clear) == 20
    assert len(acm) == 5
    assert len(unknown) == 5
    assert {item["source"] for item in unknown} == {"IEEEXplore"}
    assert {item["source"] for item in acm} == {"ACMDigitalLibrary"}
    assert all(item["evidence_path"] is None for item in unknown)
    assert all(item["evidence_path"].startswith("outputs/query_sizing/") for item in clear)
    assert all(item["evidence_path"] == ACM_FINAL_RECONCILIATION_PATH for item in acm)
    assert {
        (item["family_id"], item["variant_id"])
        for item in windows["items"]
    } == set(PRODUCTION_SELECTION.items())


def test_phase4a_contract_is_compatible_but_not_ready_and_crossref_is_support_only() -> None:
    package = _package().payload
    phase4a = package["phase4a_compatibility"]
    assert phase4a["production_plan_accepted"] is True
    assert phase4a["planning_contract_compatible"] is True
    assert phase4a["required_identification_sources"] == [
        "PubMed",
        "EuropePMC",
        "SemanticScholar",
        "arXiv",
        "IEEEXplore",
        "ACMDigitalLibrary",
        "PriorSurveySeed",
    ]
    assert phase4a["required_support_sources"] == ["CrossRef"]
    assert phase4a["crossref_identification_allowed"] is False
    assert phase4a["required_inputs_available"] is False
    assert phase4a["ready"] is False
    assert phase4a["wave_instantiated"] is False
    assert package["states"]["JFR25"] == "POPULATED_VALIDATED_NOT_IMPORTED"
    assert package["states"]["EBK25"] == "UNPOPULATED_REQUIRES_CURATOR_INPUT"
    assert package["states"]["FP19"] == "UNPOPULATED_REQUIRES_CURATOR_INPUT"
    assert package["states"]["acm"] == "RETRIEVAL_EVIDENCE_COMPLETE_NOT_IMPORTED"
    assert "ACM_OPERATOR_EVIDENCE_MISSING" not in phase4a["readiness_issues"]
    assert package["overall_status"] == "BLOCKED_EXTERNAL_INPUT"


def test_no_production_review_or_occurrence_state_is_created() -> None:
    package = _package().payload
    assert package["production_operations_created"] == []
    serialized = PACKAGE_PATH.read_text(encoding="utf-8")
    for forbidden in (
        '"retrieval_cutoff_date"',
        '"record_occurrences"',
        '"source_queries"',
        '"retrieval_runs"',
        '"corpus_memberships"',
        '"prisma"',
    ):
        assert forbidden not in serialized


def test_package_rejects_hash_mismatch_and_ready_claim() -> None:
    payload = json.loads(PACKAGE_PATH.read_text())
    payload["overall_status"] = "READY_FOR_WAVE_INSTANTIATION"
    with pytest.raises(ProductionPrerequisiteError, match="hash mismatch"):
        ProductionPrerequisitePackage(payload).validate(root=ROOT)


def test_package_paths_are_relative_and_traversal_safe() -> None:
    package = _package().payload
    assert not Path(package["production_query_plan"]["path"]).is_absolute()
    for item in package["artifacts"]:
        path = Path(item["path"])
        assert not path.is_absolute()
        assert ".." not in path.parts
