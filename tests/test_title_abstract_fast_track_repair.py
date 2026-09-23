from __future__ import annotations

import json
from pathlib import Path

import pytest

from h2h_lit.openai_provider import _response_schema_for_version
from h2h_lit.title_abstract_fast_track_batch import CRITERION_KEYS
from h2h_lit.title_abstract_fast_track_repair import (
    EVIDENCED,
    NOT_EVIDENCED,
    audit_saved_run,
    evidence_units_for,
    validate_evidence_unit_payload,
)

RECORD = {
    "batch_order": 1,
    "batch_status": "UNPROCESSED_READY_NOT_LAUNCHED",
    "canonical_id": "canonical:test",
    "title": "Interactive network analysis for biology",
    "abstract": "First sentence. Second sentence has v2.0 details.",
}


def _payload(*, decision: str = "YES", evidence_id: str = "TITLE.1"):
    criteria = {}
    for key in CRITERION_KEYS:
        criteria[key] = {
            "decision": decision,
            "certainty": "UNCERTAIN" if decision == "UNCERTAIN" else "SUPPORTED",
            "evidence_status": EVIDENCED,
            "evidence_ids": [evidence_id],
            "rationale": f"Reason for {key}.",
        }
    return {"criteria": criteria, "overall_rationale": "Bounded test judgment."}


def test_evidence_units_are_deterministic_and_preserve_original_text():
    first = evidence_units_for(RECORD)
    second = evidence_units_for(RECORD)

    assert first == second
    assert [item["id"] for item in first] == ["TITLE.1", "ABSTRACT.001", "ABSTRACT.002"]
    for item in first:
        source = RECORD[item["source_field"]]
        assert source[item["start"]:item["end"]] == item["text"]


def test_provider_schema_uses_locally_checked_ids_without_unsupported_unique_items():
    schema = _response_schema_for_version("1.4.1")
    evidence_ids = schema["properties"]["criteria"]["properties"][
        "E1_life_science_application"
    ]["properties"]["evidence_ids"]

    assert "uniqueItems" not in evidence_ids


def test_valid_ids_attach_original_evidence_text_in_code():
    result = validate_evidence_unit_payload(_payload(), RECORD)

    evidence = result["criteria"][CRITERION_KEYS[0]]["evidence"][0]
    assert evidence["id"] == "TITLE.1"
    assert evidence["text"] == RECORD["title"]
    assert result["computed_outcome"] == "INCLUDE"


def test_not_evidenced_is_valid_for_uncertainty_and_requires_no_quote():
    payload = _payload()
    item = payload["criteria"]["E7_evidence_sufficiency"]
    item.update(
        decision="UNCERTAIN",
        certainty="UNCERTAIN",
        evidence_status=NOT_EVIDENCED,
        evidence_ids=[],
    )

    result = validate_evidence_unit_payload(payload, RECORD)

    assert result["computed_outcome"] == "UNCERTAIN"
    assert result["criteria"]["E7_evidence_sufficiency"]["evidence"] == []


def test_e7_yes_is_rejected_when_a_scientific_criterion_is_unresolved():
    payload = _payload()
    item = payload["criteria"]["E3_interactive_visual_analytics"]
    item.update(
        decision="UNCERTAIN",
        certainty="UNCERTAIN",
        evidence_status=NOT_EVIDENCED,
        evidence_ids=[],
    )

    with pytest.raises(ValueError, match="E7.YES is inconsistent"):
        validate_evidence_unit_payload(payload, RECORD)


def test_nonexistent_evidence_id_is_rejected():
    payload = _payload(evidence_id="ABSTRACT.999")
    with pytest.raises(ValueError, match="nonexistent IDs"):
        validate_evidence_unit_payload(payload, RECORD)


def test_yes_or_no_cannot_use_not_evidenced_status():
    payload = _payload()
    item = payload["criteria"]["E3_interactive_visual_analytics"]
    item.update(evidence_status=NOT_EVIDENCED, evidence_ids=[])
    with pytest.raises(ValueError, match="YES requires affirmative supplied evidence"):
        validate_evidence_unit_payload(payload, RECORD)


def test_real_saved_failures_are_classified_offline_if_present():
    root = Path("outputs/staging/title-abstract-fast-track-batch-v2-1-0-gpt-5-6-luna-20260923T084057Z")
    batch = Path("outputs/staging/title-abstract-fast-track-v2-1-0-20260922/next_batch_250.jsonl")
    if not root.exists() or not batch.exists():
        pytest.skip("archived stopped-run fixture is not present")

    audit, breakdown = audit_saved_run(root, batch)

    assert audit["nonexact_evidence_item_count"] == 17
    assert audit["classification_counts"] == {
        "ATTEMPT_TO_QUOTE_ABSENT_EVIDENCE": 2,
        "FORMATTING_OR_NORMALIZATION_DIFFERENCE": 4,
        "PARAPHRASE_OR_ABRIDGED_EXCERPT": 11,
    }
    assert len(audit["structural_defects"]) == 2
    assert breakdown["total"] == 61
    assert breakdown["missing_abstract"] == 25
    assert breakdown["uncertain_by_criterion_overlapping"] == {
        "E1": 8,
        "E2": 27,
        "E3": 56,
        "E4": 58,
        "E5": 49,
        "E7": 61,
    }
    assert breakdown["e7_only_uncertainty"] == 0
