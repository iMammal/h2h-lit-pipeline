from __future__ import annotations

import json
from typing import Any
from pathlib import Path

import pytest

from h2h_lit.title_abstract_fast_track_batch import (
    CRITERION_KEYS,
    Pricing,
    RunSettings,
    _Budget,
    _normalize_usage,
    _screen_record,
    _usage_cost,
    finalize_stopped_run,
    validate_model_payload,
)

RECORD = {
    "batch_order": 1,
    "batch_status": "UNPROCESSED_READY_NOT_LAUNCHED",
    "canonical_id": "canonical:test",
    "title": "Interactive network analysis for biology",
    "abstract": "Biologists interactively explore a network with algorithmic clustering to interpret results.",
    "doi": "10.1/test",
    "source_url": "https://example.test/record",
    "source_database": "fixture",
    "source_identifier": "fixture:1",
}


def _payload(decisions: dict[str, str] | None = None) -> dict[str, Any]:
    decisions = decisions or {}
    quote = "Interactive network analysis for biology"
    criteria = {}
    for key in CRITERION_KEYS:
        decision = decisions.get(key, "YES")
        criteria[key] = {
            "decision": decision,
            "certainty": "UNCERTAIN" if decision == "UNCERTAIN" else "SUPPORTED",
            "evidence": [
                {
                    "quote": quote,
                    "source_field": "title",
                    "locator": "input.title",
                    "claimed_start": 0,
                    "claimed_end": len(quote),
                }
            ],
            "rationale": f"Evidence for {key}.",
        }
    return {"criteria": criteria, "overall_rationale": "Bounded fixture judgment."}


class FakeProvider:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.metadata = {}

    def generate(self, **kwargs):
        self.calls.append(kwargs)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        self.metadata[(kwargs["request_id"], kwargs["attempt_number"])] = {
            "provider_usage": {"input_tokens": 100, "output_tokens": 50}
        }
        return response

    def metadata_for(self, request_id, attempt_number):
        return self.metadata.get((request_id, attempt_number), {})


def _settings(cap=10.0):
    return RunSettings(
        provider_name="OpenAI",
        model="authorized-model",
        hard_spending_cap_usd=cap,
        pricing=Pricing(1.0, 0.1, 1.25, 2.0),
        retry_limit=1,
    )


def test_strict_validation_recomputes_include_and_advance():
    result = validate_model_payload(_payload(), RECORD)

    assert result["computed_outcome"] == "INCLUDE"
    assert result["operational_disposition"] == "ADVANCE_TO_FULL_REPORT_ASSESSMENT"
    assert result["e6_status"] == "NOT_ASSESSED_AT_THIS_STAGE"


def test_scientific_no_precedes_uncertainty_and_derives_reason():
    result = validate_model_payload(
        _payload(
            {
                "E3_interactive_visual_analytics": "NO",
                "E5_human_analytic_relationship": "UNCERTAIN",
            }
        ),
        RECORD,
    )

    assert result["computed_outcome"] == "EXCLUDED"
    assert result["operational_disposition"] == "EXCLUDE"
    assert result["exclusion_reasons"] == ["NO_INTERACTIVE_VISUAL_ANALYTICS"]


def test_uncertainty_and_missing_abstract_defer_without_exclusion():
    uncertain = validate_model_payload(
        _payload({"E4_computational_assistance": "UNCERTAIN"}), RECORD
    )
    title_only = {**RECORD, "abstract": ""}
    title_only_result = validate_model_payload(_payload(), title_only)

    assert uncertain["computed_outcome"] == "UNCERTAIN"
    assert uncertain["operational_disposition"] == "DEFER"
    assert uncertain["exclusion_reasons"] == []
    assert title_only_result["computed_outcome"] == "INCLUDE"
    assert title_only_result["operational_disposition"] == "DEFER"


def test_invalid_extra_fields_and_nonverbatim_evidence_are_rejected():
    extra = _payload()
    extra["computed_outcome"] = "INCLUDE"
    with pytest.raises(ValueError, match="exactly criteria"):
        validate_model_payload(extra, RECORD)

    nonverbatim = _payload()
    nonverbatim["criteria"][CRITERION_KEYS[0]]["evidence"][0]["quote"] = "invented quote"
    with pytest.raises(ValueError, match="exact supplied-field substring"):
        validate_model_payload(nonverbatim, RECORD)


def test_failed_attempt_is_preserved_then_retry_validates(tmp_path: Path):
    provider = FakeProvider(["not-json", json.dumps(_payload())])
    result = _screen_record(
        RECORD,
        tmp_path,
        "prompt",
        "prompt-hash",
        _settings(),
        provider,
        _Budget(10.0),
        iter([0.0, 1.0, 2.0, 3.0]).__next__,
    )

    assert result["status"] == "VALIDATED"
    assert len(provider.calls) == 2
    assert json.loads((tmp_path / "canonical_test" / "attempt-001.json").read_text())["status"] == "INVALID"
    assert (tmp_path / "canonical_test" / "attempt-002.json").exists()


def test_resume_returns_create_only_result_without_duplicate_submission(tmp_path: Path):
    provider = FakeProvider([json.dumps(_payload())])
    first = _screen_record(
        RECORD,
        tmp_path,
        "prompt",
        "prompt-hash",
        _settings(),
        provider,
        _Budget(10.0),
        iter([0.0, 1.0]).__next__,
    )
    second = _screen_record(
        RECORD,
        tmp_path,
        "prompt",
        "prompt-hash",
        _settings(),
        provider,
        _Budget(10.0),
        iter([2.0, 3.0]).__next__,
    )

    assert first == second
    assert len(provider.calls) == 1


def test_resume_recovers_valid_attempt_written_before_final_without_resubmission(
        tmp_path: Path,
):
    provider = FakeProvider([json.dumps(_payload())])
    first = _screen_record(
        RECORD,
        tmp_path,
        "prompt",
        "prompt-hash",
        _settings(),
        provider,
        _Budget(10.0),
        iter([0.0, 1.0]).__next__,
    )
    (tmp_path / "canonical_test" / "result.json").unlink()
    no_call_provider = FakeProvider([])

    recovered = _screen_record(
        RECORD,
        tmp_path,
        "prompt",
        "prompt-hash",
        _settings(),
        no_call_provider,
        _Budget(10.0),
        iter([2.0, 3.0]).__next__,
    )

    assert recovered == first
    assert no_call_provider.calls == []


def test_orphaned_reservation_is_not_resubmitted(tmp_path: Path):
    record_dir = tmp_path / "canonical_test"
    record_dir.mkdir()
    (record_dir / "attempt-001.reservation.json").write_text("{}")
    provider = FakeProvider([json.dumps(_payload())])

    result = _screen_record(
        RECORD,
        tmp_path,
        "prompt",
        "prompt-hash",
        _settings(),
        provider,
        _Budget(10.0),
        iter([0.0, 1.0]).__next__,
    )

    assert result["failure_reason"] == "AMBIGUOUS_PRIOR_SUBMISSION_NOT_RETRIED"
    assert provider.calls == []


def test_budget_is_reserved_before_submission(tmp_path: Path):
    provider = FakeProvider([json.dumps(_payload())])
    result = _screen_record(
        RECORD,
        tmp_path,
        "long prompt" * 100,
        "prompt-hash",
        _settings(cap=0.000001),
        provider,
        _Budget(0.000001),
        iter([0.0, 1.0]).__next__,
    )

    assert result["status"] == "UNPROCESSED_BUDGET_STOP"
    assert provider.calls == []


def test_gpt56_cache_writes_and_reasoning_tokens_are_accounted_without_double_billing():
    usage = _normalize_usage(
        {
            "input_tokens": 1_000,
            "input_tokens_details": {
                "cached_tokens": 200,
                "cache_write_tokens": 300,
            },
            "output_tokens": 400,
            "output_tokens_details": {"reasoning_tokens": 250},
        }
    )
    pricing = Pricing(0.20, 0.02, 0.25, 1.20)

    assert usage["reasoning_tokens"] == 250
    assert _usage_cost(usage, pricing) == pytest.approx(0.000659)


def test_stopped_run_finalizes_orphaned_reservation_without_provider_call(tmp_path: Path):
    batch_path = tmp_path / "batch.jsonl"
    records = []
    for index in range(1, 251):
        records.append(
            {
                **RECORD,
                "batch_order": index,
                "canonical_id": f"canonical:test-{index}",
            }
        )
    batch_path.write_text("".join(json.dumps(item) + "\n" for item in records))
    output_dir = tmp_path / "run"
    record_dir = output_dir / "records" / "canonical_test-1"
    record_dir.mkdir(parents=True)
    (output_dir / "run_binding.json").write_text(
        json.dumps(
            {
                "status": "RUNNING",
                "model": "authorized-model",
                "prompt": {"sha256": "prompt-hash"},
                "parameters": {"reasoning_effort": "medium", "service_tier": "default"},
                "controls": {"hard_spending_cap_usd": 5.0},
            }
        )
    )
    (record_dir / "attempt-001.reservation.json").write_text(
        json.dumps(
            {
                "input_hash": "input-hash",
                "request_id": "request:test",
                "attempt_number": 1,
                "maximum_reserved_cost_usd": 0.01,
            }
        )
    )

    report = finalize_stopped_run(
        batch_path=batch_path,
        output_dir=output_dir,
        stopped_reason="systematic exact-quote validation failure",
    )

    result = json.loads((record_dir / "result.json").read_text())
    assert result["status"] == "FAILED"
    assert result["ambiguous_inflight_request"] is True
    assert report["counts"] == {
        "include": 0,
        "deferred": 0,
        "excluded": 0,
        "failed": 1,
        "unprocessed": 249,
    }
    assert report["estimated_billed_cost_usd"] == pytest.approx(0.01)
