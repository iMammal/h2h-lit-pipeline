from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from h2h_lit.title_abstract_fast_track_batch import CRITERION_KEYS, Pricing, RunSettings, sha256_file
from h2h_lit.title_abstract_remaining_campaign import (
    ANNOTATION_STATES,
    ASSISTANCE_MODES,
    TASKS,
    VISUALIZATION_MODALITIES,
    WORKFLOW_SUPPORT,
    run_remaining_campaign,
)


def criterion() -> dict:
    return {
        "decision": "YES", "certainty": "SUPPORTED", "evidence_status": "EVIDENCED",
        "evidence_ids": ["TITLE.1"], "rationale": "The supplied title supports this judgment.",
    }


def screening_payload() -> dict:
    return {
        "criteria": {key: criterion() for key in CRITERION_KEYS},
        "policy_assessment": {
            "document_scope": {
                "decision": "QUALIFYING_RESEARCH_REPORT", "evidence_status": "EVIDENCED",
                "evidence_ids": ["TITLE.1"], "rationale": "The title identifies a research report.",
            },
            "integrated_workflow": {
                "decision": "SUPPORTED", "evidence_status": "EVIDENCED",
                "evidence_ids": ["ABSTRACT.001"], "rationale": "The abstract supports an integrated workflow.",
            },
        },
        "overall_rationale": "Supported by supplied title and abstract evidence.",
    }


def coding_payload() -> dict:
    def rows(labels):
        return [{
            "label": label, "state": "UNCERTAIN",
            "evidence_status": "NOT_EVIDENCED_IN_SUPPLIED_METADATA", "evidence_ids": [],
            "rationale": "The supplied metadata does not resolve this label.",
        } for label in labels]
    return {
        "assistance_modes": rows(ASSISTANCE_MODES),
        "visualization_modalities": rows(VISUALIZATION_MODALITIES),
        "tasks": rows(TASKS),
        "workflow_support_review": {
            "decision": "UNKNOWN", "evidence_status": "NOT_EVIDENCED_IN_SUPPLIED_METADATA",
            "evidence_ids": [], "rationale": "The supplied metadata does not resolve workflow support.",
        },
        "overall_rationale": "Provisional coding from supplied metadata only.",
    }


class FakeProvider:
    def __init__(self):
        self.metadata = {}

    def generate(self, *, parameters, request_id, attempt_number, **_kwargs):
        self.metadata[(request_id, attempt_number)] = {
            "provider_model": "gpt-5.6-luna", "provider_service_tier": "default",
            "provider_usage": {
                "input_tokens": 100,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens": 100, "output_tokens_details": {"reasoning_tokens": 20},
            },
            "rate_limit_headers": {
                "x-ratelimit-limit-requests": "10000", "x-ratelimit-limit-tokens": "10000000",
            },
        }
        return json.dumps(
            coding_payload()
            if parameters["response_schema_version"] == "coding-1.0.0"
            else screening_payload()
        )

    def metadata_for(self, request_id, attempt_number):
        return self.metadata[(request_id, attempt_number)]


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_manifest_driven_continuation_counts_and_shared_budget(tmp_path):
    records = []
    for index in range(1, 21):
        records.append({
            "batch_order": index, "batch_status": "UNPROCESSED_READY_NOT_LAUNCHED",
            "canonical_id": f"canonical:continue{index:03d}", "title": "Visual clinical analysis",
            "abstract": "Clinicians inspect a visualization supported by statistical analysis.",
            "prior_candidate_number": None, "doi": None, "source_url": None,
        })
    coding_seed = [{**records[0], "canonical_id": "canonical:pendingcode", "coding_order": 8833}]
    queue = tmp_path / "queue.jsonl"
    seed = tmp_path / "seed.jsonl"
    write_jsonl(queue, records)
    write_jsonl(seed, coding_seed)
    config = tmp_path / "config.json"
    write_json(config, {"taxonomy": {
        "annotation_states": list(ANNOTATION_STATES), "assistance_modes": list(ASSISTANCE_MODES),
        "visualization_modalities": list(VISUALIZATION_MODALITIES), "tasks": list(TASKS),
        "workflow_support_review": list(WORKFLOW_SUPPORT),
    }})
    screening_prompt = tmp_path / "screening.md"
    coding_prompt = tmp_path / "coding.md"
    screening_prompt.write_text("Prompt-Version: 2.2.0\n", encoding="utf-8")
    coding_prompt.write_text("Prompt-Version: 1.0.0\n", encoding="utf-8")
    protocol = tmp_path / "protocol.json"
    amendment = tmp_path / "amendment.json"
    write_json(protocol, {"protocol_version": "2.0.0"})
    write_json(amendment, {"amendment_version": "2.2.0"})
    ledger = tmp_path / "ledger.json"
    write_json(ledger, {
        "cumulative_conservative_cost_usd": 194.47124293,
        "outstanding_reservations_usd": 0.0,
        "cumulative_usage": {key: 0 for key in (
            "input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "reasoning_tokens"
        )},
    })
    bindings = {
        "config": config, "screening_prompt": screening_prompt, "coding_prompt": coding_prompt,
        "protocol": protocol, "amendment": amendment,
    }
    manifest = tmp_path / "manifest.json"
    write_json(manifest, {
        "screening_queue": {"sha256": sha256_file(queue), "records": 20, "unique_ids": 20},
        "coding_seed": {"sha256": sha256_file(seed), "records": 1, "unique_ids": 1},
        "bindings": {name: {"sha256": sha256_file(path)} for name, path in bindings.items()},
    })
    settings = RunSettings(
        provider_name="OpenAI", model="gpt-5.6-luna", hard_spending_cap_usd=225.0,
        pricing=Pricing(0.20, 0.02, 0.25, 1.20), max_output_tokens=3000,
        reasoning_effort="medium", concurrency=4, timeout_seconds=10, retry_limit=1,
        smoke_count=20, service_tier="default",
    )
    report = run_remaining_campaign(
        queue_path=queue, coding_seed_path=seed, manifest_path=manifest, config_path=config,
        screening_prompt_path=screening_prompt, coding_prompt_path=coding_prompt,
        protocol_path=protocol, amendment_path=amendment, campaign_ledger_path=ledger,
        output_dir=tmp_path / "run", settings=settings, provider=FakeProvider(),
        runtime_hours=1, drain_seconds=1,
    )
    assert report["status"] == "COMPLETE"
    assert report["screening"]["queue_records"] == 20
    assert report["screening"]["counts"]["advance"] == 20
    assert report["coding"]["initial_seed_records"] == 1
    assert report["coding"]["counts"]["validated"] == 21
    assert report["cumulative_conservative_cost_usd"] > 194.47124293
    assert report["cumulative_conservative_cost_usd"] <= 225.0
    assert report["cumulative_usage"]["input_tokens"] == 4100
    assert report["cumulative_usage"]["output_tokens"] == 4100


def test_combined_csv_rejects_duplicate_ids(tmp_path):
    from scripts.build_title_abstract_combined_handoff import combine_csv

    fields = ["canonical_id", "title"]
    paths = []
    for index in range(2):
        path = tmp_path / f"part{index}.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerow({"canonical_id": "canonical:same", "title": "Same"})
        paths.append(path)
    with pytest.raises(ValueError, match="duplicate"):
        combine_csv(paths, tmp_path / "combined.csv")


def test_category_summary_counts_unique_papers_separately_from_assignments():
    from scripts.build_title_abstract_combined_handoff import sum_category_summaries

    result = sum_category_summaries(
        {"unique_coded_papers": 3, "multilabel_present_assignments": 8, "labels": {"x": 3},
         "workflow_support_review": {"SUPPORTED": 2, "UNKNOWN": 1, "UNSUPPORTED": 0}},
        {"unique_coded_papers": 2, "multilabel_present_assignments": 7, "labels": {"x": 2, "y": 1},
         "workflow_support_review": {"SUPPORTED": 1, "UNKNOWN": 0, "UNSUPPORTED": 1}},
    )
    assert result["unique_coded_papers"] == 5
    assert result["multilabel_present_assignments"] == 15
    assert result["labels"] == {"x": 5, "y": 1}
    assert result["workflow_support_review"]["UNSUPPORTED"] == 1


def test_e7_warning_summary_totals_only_outcome_rows(tmp_path):
    from scripts.build_title_abstract_combined_handoff import e7_warning_summary

    result_dir = tmp_path / "results"
    for index, (disposition, warning) in enumerate(
        [
            ("ADVANCE_TO_FULL_REPORT_ASSESSMENT", True),
            ("DEFER", False),
            ("EXCLUDE", True),
        ]
    ):
        record_dir = result_dir / f"record-{index}"
        record_dir.mkdir(parents=True)
        (record_dir / "result.json").write_text(
            json.dumps(
                {
                    "canonical_id": f"canonical:{index}",
                    "status": "VALIDATED",
                    "judgment": {"operational_disposition": disposition},
                    "e7_consistency_warning": warning,
                }
            ),
            encoding="utf-8",
        )

    summary = e7_warning_summary([result_dir])

    assert summary["total_valid_screenings"] == 3
    assert summary["total_e7_warnings"] == 2
    assert summary["advance_warning_check"] == (
        "1 of 1 ADVANCE records carry a nonfatal E7 warning"
    )
