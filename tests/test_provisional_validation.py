from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from h2h_lit.provisional_validation import (
    PREFLIGHT_ARTIFACT_CLASS,
    ProvisionalValidationError,
    build_preflight,
    deterministic_identity_sample,
    load_validation_config,
    resolve_output_namespace,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/star_provisional_pipeline_validation_v1.json"


def test_config_preserves_nonproduction_boundaries() -> None:
    config = load_validation_config(CONFIG)

    assert config["run_id"].startswith("provisional:")
    assert config["production_import_allowed"] is False
    assert config["output_namespace"].startswith("outputs/provisional/")
    assert config["pubmed"]["complete_identity_enumeration_required"] is True
    assert config["pubmed"]["metadata_sample_size_per_family"] == 100
    assert config["pubmed"]["expected_request_count_without_retries"] == 10
    assert config["jfr25_rediscovery"]["create_seed_occurrences"] is False
    assert all(config["prohibited_effects"].values())


def test_preflight_distinguishes_enumeration_from_metadata_sampling() -> None:
    report = build_preflight(
        root=ROOT,
        config_path=CONFIG,
        verify_acm_artifacts=False,
        generated_at_utc="2026-09-03T12:00:00Z",
    )

    assert report["artifact_class"] == PREFLIGHT_ARTIFACT_CLASS
    assert report["classification"] == {
        "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
        "production_import_allowed": False,
        "production_completion_claimed": False,
        "retrieval_cutoff": None,
        "disposition": "DISCARD_ONLY",
    }
    assert report["pubmed_plan"]["query_count"] == 5
    assert report["pubmed_plan"]["expected_request_count_without_retries"] == 10
    for query in report["pubmed_plan"]["queries"]:
        enumeration = query["complete_identity_enumeration"]
        assert enumeration["semantic_state_when_reconciled"] == (
            "COMPLETE_IDENTITY_ENUMERATION"
        )
        assert enumeration["characterized_as_truncated_due_to_metadata_sampling"] is False
        assert query["metadata_acquisition"]["semantic_state"] == (
            "DETERMINISTIC_SUBSET_PLANNED"
        )
        assert query["expected_requests_without_retries"] == 2

    assert report["acm_plan"]["selected_artifact_accounted_record_count"] == 11664
    assert report["acm_plan"]["selected_artifact_malformed_record_count"] == 3
    assert report["acm_plan"]["provisional_occurrences_created"] == 0
    assert report["jfr25_plan"]["validated_member_count"] == 138
    assert report["jfr25_plan"]["members_with_normalized_doi"] == 128
    assert report["jfr25_plan"]["members_without_normalized_doi"] == 10
    assert report["safeguards"]["network_requests_made"] == 0
    assert report["safeguards"]["acm_provisional_import_performed"] is False
    assert report["screening_plan"]["corpus_memberships_created"] == 0

    material = dict(report)
    claimed_hash = material.pop("preflight_hash")
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    assert claimed_hash == hashlib.sha256(encoded).hexdigest()


def test_output_namespace_guard_fails_closed(tmp_path: Path) -> None:
    expected = resolve_output_namespace(
        tmp_path, "outputs/provisional/star-pipeline-validation-001"
    )
    assert expected == tmp_path / "outputs/provisional/star-pipeline-validation-001"

    with pytest.raises(ProvisionalValidationError, match="beneath outputs/provisional"):
        resolve_output_namespace(tmp_path, "outputs/production")
    with pytest.raises(ProvisionalValidationError, match="repository-relative"):
        resolve_output_namespace(tmp_path, "../outside")


def test_identity_sampling_is_order_independent_and_eligibility_blind() -> None:
    identities = ["pmid:10", "pmid:20", "pmid:30", "pmid:40"]
    forward = deterministic_identity_sample(identities, sample_size=2, salt="fixed")
    reverse = deterministic_identity_sample(
        list(reversed(identities)), sample_size=2, salt="fixed"
    )
    assert forward == reverse
    assert len(forward) == 2

    with pytest.raises(ProvisionalValidationError, match="unique universe"):
        deterministic_identity_sample(
            ["pmid:10", "pmid:10"], sample_size=1, salt="fixed"
        )
