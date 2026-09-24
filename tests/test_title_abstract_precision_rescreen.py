from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from h2h_lit.title_abstract_fast_track_batch import CRITERION_KEYS, Pricing, RunSettings, sha256_file
from h2h_lit.title_abstract_precision_rescreen import (
    OUTPUT_SCHEMA_VERSION,
    precision_response_schema,
    run_precision_rescreen,
    validate_precision_payload,
)


def _record(
    *,
    title: str = "Interactive clinical analysis system",
    abstract: str = (
        "Clinicians interact with a linked visual dashboard. "
        "Statistical analysis clusters patient trajectories and supports treatment decisions."
    ),
) -> dict:
    return {
        "batch_order": 1,
        "batch_status": "UNPROCESSED_READY_NOT_LAUNCHED",
        "canonical_id": "canonical:test",
        "title": title,
        "abstract": abstract,
        "prior_candidate_number": 1,
        "doi": None,
        "source_url": None,
    }


def _criterion(
    decision: str = "YES",
    *,
    evidence_ids: list[str] | None = None,
    evidence_status: str = "EVIDENCED",
) -> dict:
    return {
        "decision": decision,
        "certainty": "UNCERTAIN" if decision == "UNCERTAIN" else "SUPPORTED",
        "evidence_status": evidence_status,
        "evidence_ids": ["TITLE.1"] if evidence_ids is None else evidence_ids,
        "rationale": "The selected supplied evidence supports this judgment.",
    }


def _payload(
    *,
    scope: str = "QUALIFYING_RESEARCH_REPORT",
    integration: str = "SUPPORTED",
) -> dict:
    integration_missing = integration == "NOT_EVIDENCED_IN_SUPPLIED_METADATA"
    return {
        "criteria": {key: _criterion() for key in CRITERION_KEYS},
        "policy_assessment": {
            "document_scope": {
                "decision": scope,
                "evidence_status": "NOT_EVIDENCED_IN_SUPPLIED_METADATA" if scope == "UNCERTAIN" else "EVIDENCED",
                "evidence_ids": [] if scope == "UNCERTAIN" else ["TITLE.1"],
                "rationale": "The supplied title identifies the document scope.",
            },
            "integrated_workflow": {
                "decision": integration,
                "evidence_status": "NOT_EVIDENCED_IN_SUPPLIED_METADATA" if integration_missing else "EVIDENCED",
                "evidence_ids": [] if integration_missing else ["ABSTRACT.001", "ABSTRACT.002"],
                "rationale": "The supplied abstract establishes or fails to establish the integrated workflow.",
            },
        },
        "overall_rationale": "Prospective high-precision judgment from supplied metadata only.",
    }


def test_survey_exclusion_routes_to_h2h3_background():
    result = validate_precision_payload(
        _payload(scope="SURVEY_REVIEW_OVERVIEW"),
        _record(title="A survey of interactive clinical visualization systems"),
    )

    assert result["computed_outcome"] == "EXCLUDED"
    assert result["operational_disposition"] == "EXCLUDE"
    assert result["route"] == "H2H3_BACKGROUND"
    assert "SURVEY_REVIEW_OR_BROAD_OVERVIEW" in result["exclusion_reasons"]


def test_infrastructure_ambiguity_defers_without_inventing_a_negative():
    payload = _payload(integration="NOT_EVIDENCED_IN_SUPPLIED_METADATA")
    for key in (
        "E3_interactive_visual_analytics",
        "E4_computational_assistance",
        "E5_human_analytic_relationship",
        "E7_evidence_sufficiency",
    ):
        payload["criteria"][key] = _criterion(
            "UNCERTAIN",
            evidence_ids=[],
            evidence_status="NOT_EVIDENCED_IN_SUPPLIED_METADATA",
        )

    result = validate_precision_payload(
        payload,
        _record(
            title="Portable browser infrastructure",
            abstract="The server streams rendered images to remote clients.",
        ),
    )

    assert result["computed_outcome"] == "UNCERTAIN"
    assert result["operational_disposition"] == "DEFER"
    assert result["exclusion_reasons"] == []


def test_clearly_integrated_analysis_advances():
    result = validate_precision_payload(_payload(), _record())

    assert result["computed_outcome"] == "INCLUDE"
    assert result["operational_disposition"] == "ADVANCE_TO_FULL_REPORT_ASSESSMENT"
    assert result["route"] == "FULL_REPORT_ASSESSMENT"


def test_non_ml_computational_assistance_can_advance():
    result = validate_precision_payload(
        _payload(),
        _record(
            title="Interactive simulation and registration for surgical planning",
            abstract=(
                "Surgeons interact with registered anatomical visualizations. "
                "Optimization and simulation update the plan during analysis."
            ),
        ),
    )

    assert result["computed_outcome"] == "INCLUDE"


def test_health_administration_use_is_not_automatically_excluded():
    result = validate_precision_payload(
        _payload(),
        _record(
            title="Interactive health-system performance dashboard",
            abstract=(
                "Administrators inspect linked performance views. "
                "Statistical models identify service bottlenecks and support operational decisions."
            ),
        ),
    )

    assert result["computed_outcome"] == "INCLUDE"


def test_scientific_no_precedes_other_scientific_uncertainty():
    payload = _payload()
    payload["criteria"]["E1_life_science_application"] = _criterion("NO")
    payload["criteria"]["E3_interactive_visual_analytics"] = _criterion(
        "UNCERTAIN",
        evidence_ids=[],
        evidence_status="NOT_EVIDENCED_IN_SUPPLIED_METADATA",
    )

    result = validate_precision_payload(payload, _record())

    assert result["computed_outcome"] == "EXCLUDED"
    assert result["operational_disposition"] == "EXCLUDE"


def test_missing_evidence_cannot_use_fabricated_or_nonexistent_ids():
    payload = _payload(integration="NOT_EVIDENCED_IN_SUPPLIED_METADATA")
    payload["policy_assessment"]["integrated_workflow"]["evidence_ids"] = ["ABSTRACT.999"]

    with pytest.raises(ValueError, match="nonexistent IDs|requires an empty"):
        validate_precision_payload(payload, _record())

    payload = _payload()
    payload["criteria"]["E4_computational_assistance"]["evidence_ids"] = ["ABSTRACT.999"]
    with pytest.raises(ValueError, match="nonexistent IDs"):
        validate_precision_payload(payload, _record())


def test_schema_and_aggregation_require_evidence_ids():
    schema = precision_response_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["criteria", "policy_assessment", "overall_rationale"]
    payload = _payload()
    payload["policy_assessment"]["document_scope"]["evidence_ids"] = []

    with pytest.raises(ValueError, match="requires evidence IDs"):
        validate_precision_payload(payload, _record())


class _FakeProvider:
    def __init__(self):
        self.metadata = {}

    def generate(self, *, request_id, attempt_number, **_kwargs):
        self.metadata[(request_id, attempt_number)] = {
            "provider_model": "gpt-5.6-luna",
            "provider_service_tier": "default",
            "provider_usage": {
                "input_tokens": 120,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens": 80,
                "output_tokens_details": {"reasoning_tokens": 20},
            },
        }
        return json.dumps(_payload())

    def metadata_for(self, request_id, attempt_number):
        return self.metadata[(request_id, attempt_number)]


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def test_twenty_record_smoke_uses_campaign_ledger_and_writes_handoffs(tmp_path):
    records = []
    for index in range(1, 891):
        record = _record()
        record.update({
            "batch_order": index,
            "canonical_id": f"canonical:test{index:03d}",
            "prior_candidate_number": index,
        })
        records.append(record)
    batch = tmp_path / "batch.jsonl"
    batch.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    prompt = tmp_path / "prompt.md"
    prompt.write_text(
        "Prompt-Version: 2.2.0\nOutput-Schema-Version: 2.2.0\n",
        encoding="utf-8",
    )
    protocol = tmp_path / "protocol.json"
    _write_json(protocol, {"protocol_version": "2.0.0"})
    amendment = tmp_path / "amendment.json"
    _write_json(amendment, {"amendment_version": "2.2.0"})
    ledger = tmp_path / "ledger.json"
    _write_json(ledger, {
        "cumulative_conservative_cost_usd": 15.771293,
        "current_inflight_reserved_cost_usd": 0.0,
        "cumulative_usage": {
            "input_tokens": 10,
            "cached_input_tokens": 2,
            "cache_write_tokens": 1,
            "output_tokens": 5,
            "reasoning_tokens": 2,
        },
    })
    manifest = tmp_path / "manifest.json"
    _write_json(manifest, {
        "batch": {"sha256": sha256_file(batch), "records": 890, "unique_ids": 890},
        "counts": {
            "records": 890,
            "prior_250_effective_repair": 11,
            "overnight_25000": 879,
            "known_related_version_candidates": 5,
        },
        "campaign_ledger": {"sha256": sha256_file(ledger)},
        "prompt": {"sha256": sha256_file(prompt)},
        "protocol": {"sha256": sha256_file(protocol)},
        "amendment": {"sha256": sha256_file(amendment)},
        "source_bindings": {},
    })
    settings = RunSettings(
        provider_name="OpenAI",
        model="gpt-5.6-luna",
        hard_spending_cap_usd=50.0,
        pricing=Pricing(0.20, 0.02, 0.25, 1.20),
        reasoning_effort="medium",
        concurrency=4,
        retry_limit=1,
        smoke_count=20,
        service_tier="default",
    )

    report = run_precision_rescreen(
        batch_path=batch,
        batch_manifest_path=manifest,
        prompt_path=prompt,
        protocol_path=protocol,
        amendment_path=amendment,
        campaign_ledger_path=ledger,
        output_dir=tmp_path / "run",
        settings=settings,
        provider=_FakeProvider(),
        runtime_hours=1.0,
        drain_seconds=1.0,
    )

    assert report["smoke_test_passed"] is True
    assert report["counts"]["advance"] == 890
    assert report["stopping_reason"] == "BATCH_COMPLETE"
    with (tmp_path / "run" / "advance_candidates.csv").open(encoding="utf-8", newline="") as handle:
        assert len(list(csv.DictReader(handle))) == 890
    assert (tmp_path / "run" / "advance_candidates.xlsx").exists()
    assert (tmp_path / "run" / "unprocessed.csv").exists()
    assert report["cumulative_conservative_cost_usd"] > 15.771293
    assert OUTPUT_SCHEMA_VERSION == "2.2.0"
