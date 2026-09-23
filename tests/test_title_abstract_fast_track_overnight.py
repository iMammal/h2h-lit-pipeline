from __future__ import annotations

import json
from pathlib import Path

from h2h_lit.title_abstract_fast_track_batch import CRITERION_KEYS, Pricing, RunSettings, sha256_file
from h2h_lit.title_abstract_fast_track_overnight import Budget, run_overnight


class FakeProvider:
    def __init__(self):
        self.metadata = {}

    def generate(self, *, request_id, attempt_number, **_kwargs):
        criteria = {
            key: {
                "decision": "YES",
                "certainty": "SUPPORTED",
                "evidence_status": "EVIDENCED",
                "evidence_ids": ["TITLE.1"],
                "rationale": f"Title evidence supports {key}.",
            }
            for key in CRITERION_KEYS
        }
        self.metadata[(request_id, attempt_number)] = {
            "provider_model": "gpt-5.6-luna",
            "provider_service_tier": "default",
            "provider_usage": {
                "input_tokens": 100,
                "input_tokens_details": {"cached_tokens": 0, "cache_write_tokens": 0},
                "output_tokens": 50,
                "output_tokens_details": {"reasoning_tokens": 10},
            },
        }
        return json.dumps({"criteria": criteria, "overall_rationale": "All criteria supported."})

    def metadata_for(self, request_id, attempt_number):
        return self.metadata[(request_id, attempt_number)]


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def test_budget_counts_prior_cost_and_inflight_reservations():
    budget = Budget(1.0, 0.4)

    assert budget.reserve(0.5)
    assert not budget.reserve(0.2)
    assert budget.snapshot() == {"actual": 0.4, "reserved": 0.5, "cap": 1.0}
    budget.settle(0.5, 0.1)
    assert budget.snapshot() == {"actual": 0.5, "reserved": 0.0, "cap": 1.0}


def test_twenty_record_smoke_runs_through_shared_evidence_id_validator(tmp_path):
    records = [
        {
            "batch_order": index,
            "batch_status": "UNPROCESSED_READY_NOT_LAUNCHED",
            "canonical_id": f"canonical:test{index:03d}",
            "title": f"Interactive biological network system {index}",
            "abstract": "Interactive visual analysis supports biological investigation.",
            "doi": None,
            "source_url": None,
        }
        for index in range(1, 21)
    ]
    batch = tmp_path / "batch.jsonl"
    batch.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
    overlay = tmp_path / "overlay.json"
    _write_json(overlay, {"schema_version": "1.0.0"})
    protocol = tmp_path / "protocol.json"
    _write_json(protocol, {"protocol_version": "2.0.0"})
    amendment = tmp_path / "amendment.json"
    _write_json(amendment, {"amendment_version": "2.1.0"})
    prompt = tmp_path / "prompt.md"
    prompt.write_text("Prompt-Version: 2.1.2\n", encoding="utf-8")
    prior = tmp_path / "prior"
    prior.mkdir()
    _write_json(prior / "package_manifest.json", {"status": "COMPLETE"})
    _write_json(prior / "combined_coverage_report.json", {
        "counts": {"include": 11, "deferred": 172, "excluded": 63, "unresolved": 4},
        "cumulative_conservative_cost_usd": 0.34471121,
        "cumulative_usage": {
            "input_tokens": 10, "cached_input_tokens": 2, "cache_write_tokens": 1,
            "output_tokens": 5, "reasoning_tokens": 2,
        },
    })
    manifest = tmp_path / "manifest.json"
    _write_json(manifest, {
        "batch": {"sha256": sha256_file(batch), "records": 20},
        "frame": {"sha256": "corpus-test", "canonical_records": 20},
        "identity_overlay": {"sha256": sha256_file(overlay)},
        "prior_screening": {
            "repair_package_manifest": {"sha256": sha256_file(prior / "package_manifest.json")},
            "repair_report": {"sha256": sha256_file(prior / "combined_coverage_report.json")},
        },
    })
    settings = RunSettings(
        provider_name="OpenAI", model="gpt-5.6-luna", hard_spending_cap_usd=50.0,
        pricing=Pricing(0.20, 0.02, 0.25, 1.20), reasoning_effort="medium",
        concurrency=4, retry_limit=1, smoke_count=20, service_tier="default",
    )

    report = run_overnight(
        batch_path=batch, batch_manifest_path=manifest, prompt_path=prompt,
        protocol_path=protocol, amendment_path=amendment,
        identity_overlay_path=overlay, prior_repair_dir=prior,
        output_dir=tmp_path / "run", settings=settings, provider=FakeProvider(),
        runtime_hours=1.0, drain_seconds=1.0,
    )

    assert report["smoke_test_passed"] is True
    assert report["counts"] == {
        "include": 20, "deferred": 0, "excluded": 0,
        "failed": 0, "ambiguous": 0, "unprocessed": 0,
    }
    assert report["cumulative_conservative_cost_usd"] > 0.34471121
    assert (tmp_path / "run" / "clear_include_handoff.csv").read_text().count("\n") == 21
