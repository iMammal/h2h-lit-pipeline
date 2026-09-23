"""Narrow evidence-ID repair for the stopped fast-track 250-record batch."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from h2h_lit.inference import InferenceProvider
from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.title_abstract_fast_track_batch import (
    CRITERION_KEYS,
    EXCLUSION_REASONS,
    SHORT_KEYS,
    Pricing,
    RunSettings,
    _Budget,
    _maximum_attempt_cost,
    _normalize_usage,
    _usage_cost,
    load_jsonl,
    sha256_file,
    validate_batch,
)
from h2h_lit.title_abstract_screening import E6_STATUS, recompute_outcome

PROMPT_VERSION = "2.1.2"
OUTPUT_SCHEMA_VERSION = "1.4.1"
EVIDENCED = "EVIDENCED"
NOT_EVIDENCED = "NOT_EVIDENCED_IN_SUPPLIED_METADATA"
_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=\S)")


def evidence_unit_response_schema() -> dict[str, Any]:
    criterion = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decision": {"type": "string", "enum": ["YES", "NO", "UNCERTAIN"]},
            "certainty": {"type": "string", "enum": ["SUPPORTED", "UNCERTAIN"]},
            "evidence_status": {
                "type": "string",
                "enum": [EVIDENCED, NOT_EVIDENCED],
            },
            "evidence_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "rationale": {"type": "string", "minLength": 1},
        },
        "required": [
            "decision",
            "certainty",
            "evidence_status",
            "evidence_ids",
            "rationale",
        ],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "criteria": {
                "type": "object",
                "additionalProperties": False,
                "properties": {key: criterion for key in CRITERION_KEYS},
                "required": list(CRITERION_KEYS),
            },
            "overall_rationale": {"type": "string", "minLength": 1},
        },
        "required": ["criteria", "overall_rationale"],
    }


def evidence_units_for(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return deterministic IDs bound to unmodified title/abstract substrings."""

    units: list[dict[str, Any]] = []
    title = str(record.get("title", ""))
    if title:
        units.append(
            {
                "id": "TITLE.1",
                "source_field": "title",
                "start": 0,
                "end": len(title),
                "text": title,
            }
        )
    abstract = str(record.get("abstract", ""))
    start = 0
    sentence_number = 1
    for boundary in [*_SENTENCE_BOUNDARY.finditer(abstract), None]:
        raw_end = boundary.start() if boundary is not None else len(abstract)
        raw = abstract[start:raw_end]
        leading = len(raw) - len(raw.lstrip())
        trailing = len(raw.rstrip())
        unit_start = start + leading
        unit_end = start + trailing
        if unit_start < unit_end:
            units.append(
                {
                    "id": f"ABSTRACT.{sentence_number:03d}",
                    "source_field": "abstract",
                    "start": unit_start,
                    "end": unit_end,
                    "text": abstract[unit_start:unit_end],
                }
            )
            sentence_number += 1
        if boundary is not None:
            start = boundary.end()
    return units


def inference_input_for(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "canonical_record_id": record["canonical_id"],
        "evidence_units": evidence_units_for(record),
    }


def validate_evidence_unit_payload(payload: Any, record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {"criteria", "overall_rationale"}:
        raise ValueError("response must contain exactly criteria and overall_rationale")
    criteria = payload["criteria"]
    if not isinstance(criteria, dict) or set(criteria) != set(CRITERION_KEYS):
        raise ValueError("criteria must contain E1-E5 and E7 exactly once")
    overall = payload["overall_rationale"]
    if not isinstance(overall, str) or not overall.strip():
        raise ValueError("overall_rationale must be a non-empty string")

    units = evidence_units_for(record)
    by_id = {unit["id"]: unit for unit in units}
    responses: dict[str, str] = {}
    normalized: dict[str, Any] = {}
    expected_fields = {
        "decision",
        "certainty",
        "evidence_status",
        "evidence_ids",
        "rationale",
    }
    for key in CRITERION_KEYS:
        item = criteria[key]
        if not isinstance(item, dict) or set(item) != expected_fields:
            raise ValueError(f"{key} has invalid fields")
        decision = item["decision"]
        if decision not in {"YES", "NO", "UNCERTAIN"}:
            raise ValueError(f"{key}.decision is invalid")
        if key == "E7_evidence_sufficiency" and decision == "NO":
            raise ValueError("E7 must use UNCERTAIN, not NO, for insufficient evidence")
        expected_certainty = "UNCERTAIN" if decision == "UNCERTAIN" else "SUPPORTED"
        if item["certainty"] != expected_certainty:
            raise ValueError(f"{key}.certainty does not match its decision")
        if not isinstance(item["rationale"], str) or not item["rationale"].strip():
            raise ValueError(f"{key}.rationale must be non-empty")
        status = item["evidence_status"]
        if status not in {EVIDENCED, NOT_EVIDENCED}:
            raise ValueError(f"{key}.evidence_status is invalid")
        evidence_ids = item["evidence_ids"]
        if (
            not isinstance(evidence_ids, list)
            or any(not isinstance(value, str) for value in evidence_ids)
            or len(set(evidence_ids)) != len(evidence_ids)
        ):
            raise ValueError(f"{key}.evidence_ids must be a unique string list")
        unknown = sorted(set(evidence_ids) - set(by_id))
        if unknown:
            raise ValueError(f"{key}.evidence_ids contain nonexistent IDs: {unknown}")
        if status == EVIDENCED and not evidence_ids:
            raise ValueError(f"{key}.EVIDENCED requires at least one evidence ID")
        if status == NOT_EVIDENCED and evidence_ids:
            raise ValueError(f"{key}.NOT_EVIDENCED requires an empty evidence ID list")
        if decision in {"YES", "NO"} and status != EVIDENCED:
            raise ValueError(f"{key}.{decision} requires affirmative supplied evidence")
        responses[SHORT_KEYS[key]] = decision
        normalized[key] = {
            **item,
            "rationale": item["rationale"].strip(),
            "evidence": [dict(by_id[value]) for value in evidence_ids],
        }

    if responses["E7"] == "YES" and any(
        responses[key] == "UNCERTAIN" for key in ("E1", "E2", "E3", "E4", "E5")
    ):
        raise ValueError("E7.YES is inconsistent with an unresolved E1-E5 criterion")

    outcome = recompute_outcome(responses, E6_STATUS)
    abstract_missing = not str(record.get("abstract", "")).strip()
    if outcome == "EXCLUDED":
        disposition = "EXCLUDE"
    elif abstract_missing or outcome == "UNCERTAIN":
        disposition = "DEFER"
    else:
        disposition = "ADVANCE_TO_FULL_REPORT_ASSESSMENT"
    no_criteria = [key for key in ("E1", "E2", "E3", "E4", "E5") if responses[key] == "NO"]
    return {
        "criteria": normalized,
        "overall_rationale": overall.strip(),
        "responses": responses,
        "e6_status": E6_STATUS,
        "computed_outcome": outcome,
        "operational_disposition": disposition,
        "exclusion_reasons": [EXCLUSION_REASONS[key] for key in no_criteria],
    }


def audit_saved_run(original_run_dir: Path, batch_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Classify archived failures and summarize archived deferrals without mutation."""

    batch = {item["canonical_id"]: item for item in load_jsonl(batch_path)}
    classifications: list[dict[str, Any]] = []
    structural: list[dict[str, Any]] = []
    failed_rows = list(load_jsonl(original_run_dir / "failed.jsonl"))
    for row in failed_rows:
        if row.get("failure_reason") != "RETRIES_EXHAUSTED":
            continue
        record = batch[row["canonical_id"]]
        for attempt in row.get("attempts", []):
            raw = attempt.get("raw_response")
            if not isinstance(raw, str):
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            for criterion, item in payload.get("criteria", {}).items():
                decision = item.get("decision")
                certainty = item.get("certainty")
                expected = "UNCERTAIN" if decision == "UNCERTAIN" else "SUPPORTED"
                if certainty != expected:
                    structural.append(
                        {
                            "canonical_id": row["canonical_id"],
                            "attempt_number": attempt.get("attempt_number"),
                            "criterion": criterion,
                            "classification": "CERTAINTY_DECISION_MISMATCH",
                        }
                    )
                for evidence in item.get("evidence", []):
                    field = evidence.get("source_field")
                    quote = evidence.get("quote")
                    locator = evidence.get("locator")
                    if field not in {"title", "abstract"} or not isinstance(quote, str):
                        continue
                    if locator != f"input.{field}":
                        structural.append(
                            {
                                "canonical_id": row["canonical_id"],
                                "attempt_number": attempt.get("attempt_number"),
                                "criterion": criterion,
                                "classification": "LOCATOR_FIELD_MISMATCH",
                                "source_field": field,
                                "locator": locator,
                            }
                        )
                    source = str(record.get(field, ""))
                    if quote and quote in source:
                        continue
                    classification = _classify_nonexact_quote(quote, source)
                    classifications.append(
                        {
                            "canonical_id": row["canonical_id"],
                            "attempt_number": attempt.get("attempt_number"),
                            "criterion": criterion,
                            "source_field": field,
                            "classification": classification,
                            "quote": quote,
                        }
                    )
    counts: dict[str, int] = {}
    for item in classifications:
        name = item["classification"]
        counts[name] = counts.get(name, 0) + 1
    failure_audit = {
        "artifact_class": "saved_fast_track_failure_audit",
        "source_run": str(original_run_dir),
        "source_run_manifest_sha256": sha256_file(original_run_dir / "package_manifest.json"),
        "nonexact_evidence_item_count": len(classifications),
        "classification_counts": counts,
        "items": classifications,
        "structural_defects": structural,
        "decision_level_review": [
            {
                "canonical_id": "canonical:32e237f5080c4441eba950e5",
                "attempt_number": 1,
                "criterion": "E3_interactive_visual_analytics",
                "classification": "UNSUPPORTED_COMBINATION_INFERENCE",
                "handling": "REMAINS_REJECTED; NOT SALVAGED BY EVIDENCE-ID REPAIR",
            }
        ],
        "original_responses_modified": False,
    }

    deferred = list(load_jsonl(original_run_dir / "deferred.jsonl"))
    uncertain = {
        key: sum(row["judgment"]["responses"][key] == "UNCERTAIN" for row in deferred)
        for key in ("E1", "E2", "E3", "E4", "E5", "E7")
    }
    e7_only = sum(
        row["judgment"]["responses"]["E7"] == "UNCERTAIN"
        and all(row["judgment"]["responses"][key] == "YES" for key in ("E1", "E2", "E3", "E4", "E5"))
        for row in deferred
    )
    deferral_breakdown = {
        "artifact_class": "original_saved_deferral_breakdown",
        "total": len(deferred),
        "missing_abstract": sum(_abstract_missing(row) for row in deferred),
        "uncertain_by_criterion_overlapping": uncertain,
        "e7_only_uncertainty": e7_only,
        "interpretation": (
            "A required quote is incompatible with documenting absent evidence; "
            "E7 insufficiency is represented by an explicit not-evidenced status."
        ),
    }
    return failure_audit, deferral_breakdown


def _classify_nonexact_quote(quote: str, source: str) -> str:
    if not quote and not source:
        return "ATTEMPT_TO_QUOTE_ABSENT_EVIDENCE"
    if quote and _normalize_text(quote) in _normalize_text(source):
        return "FORMATTING_OR_NORMALIZATION_DIFFERENCE"
    quote_tokens = set(re.findall(r"[a-z0-9]+", quote.casefold()))
    source_tokens = set(re.findall(r"[a-z0-9]+", source.casefold()))
    overlap = len(quote_tokens & source_tokens) / max(1, len(quote_tokens))
    if "..." in quote or "…" in quote or overlap >= 0.6:
        return "PARAPHRASE_OR_ABRIDGED_EXCERPT"
    return "UNSUPPORTED_OR_NONEXISTENT_CLAIM"


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = re.sub(r"[‐‑‒–—―]", "-", normalized)
    return " ".join(normalized.split())


def verify_original_package(original_run_dir: Path) -> dict[str, Any]:
    manifest_path = original_run_dir / "package_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mismatches = []
    for name, binding in manifest.get("artifacts", {}).items():
        path = original_run_dir / name
        actual = sha256_file(path) if path.exists() else None
        if actual != binding.get("sha256"):
            mismatches.append({"path": name, "expected": binding.get("sha256"), "actual": actual})
    if mismatches:
        raise ValueError(f"original run artifact binding mismatch: {mismatches}")
    return {
        "path": str(original_run_dir),
        "package_manifest_sha256": sha256_file(manifest_path),
        "artifact_count": len(manifest.get("artifacts", {})),
        "verified": True,
    }


def ambiguous_reconciliation(original_run_dir: Path) -> list[dict[str, Any]]:
    """Use only saved provider IDs; store=false means local IDs are not retrievable responses."""

    binding = json.loads((original_run_dir / "run_binding.json").read_text(encoding="utf-8"))
    store = binding.get("parameters", {}).get("store")
    items = []
    for row in load_jsonl(original_run_dir / "failed.jsonl"):
        if row.get("failure_reason") != "INTERRUPTED_SYSTEMATIC_VALIDATION_STOP":
            continue
        record_dir = original_run_dir / "records" / row["canonical_id"].replace(":", "_")
        reservations = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(record_dir.glob("attempt-*.reservation.json"))
        ]
        attempts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(record_dir.glob("attempt-[0-9][0-9][0-9].json"))
        ]
        completed_numbers = {item.get("attempt_number") for item in attempts}
        outstanding = [item for item in reservations if item.get("attempt_number") not in completed_numbers]
        saved_provider_ids = [
            item.get("provider_metadata", {}).get("provider_response_id")
            for item in attempts
            if item.get("provider_metadata", {}).get("provider_response_id")
        ]
        items.append(
            {
                "canonical_id": row["canonical_id"],
                "outstanding_client_request_ids": [item.get("request_id") for item in outstanding],
                "saved_provider_response_ids_for_outstanding_attempts": [],
                "other_completed_attempt_provider_response_ids": saved_provider_ids,
                "store": store,
                "resolution": "UNRESOLVED_NOT_RESUBMITTED",
                "rationale": (
                    "No provider response ID was saved for the outstanding attempt; the local "
                    "client request ID is not a retrievable provider response ID."
                ),
            }
        )
    return items


def run_repair(
    *,
    batch_path: Path,
    original_run_dir: Path,
    output_dir: Path,
    prompt_path: Path,
    settings: RunSettings,
    provider: InferenceProvider,
    expected_batch_sha256: str,
    prior_repair_run_dir: Path | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    settings.validate()
    records = list(load_jsonl(batch_path))
    validate_batch(records)
    if sha256_file(batch_path) != expected_batch_sha256:
        raise ValueError("batch SHA-256 does not match the frozen original batch")
    original_binding = json.loads((original_run_dir / "run_binding.json").read_text(encoding="utf-8"))
    if original_binding["bindings"]["batch"]["sha256"] != expected_batch_sha256:
        raise ValueError("original run is not bound to the supplied batch")
    original_package = verify_original_package(original_run_dir)
    if output_dir.exists():
        raise FileExistsError("repair output directory already exists")
    output_dir.mkdir(parents=True)
    (output_dir / "records").mkdir()

    failure_audit, original_deferrals = audit_saved_run(original_run_dir, batch_path)
    reconciliation = ambiguous_reconciliation(original_run_dir)
    _write_json(output_dir / "offline_failure_audit.json", failure_audit)
    _write_json(output_dir / "original_deferral_breakdown.json", original_deferrals)
    _write_json(output_dir / "ambiguous_reconciliation.json", reconciliation)

    prompt = prompt_path.read_text(encoding="utf-8")
    if f"Prompt-Version: {PROMPT_VERSION}" not in prompt:
        raise ValueError("unexpected repair prompt version")
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    schema = evidence_unit_response_schema()
    original_report = json.loads((original_run_dir / "run_report.json").read_text(encoding="utf-8"))
    prior_cost = float(original_report["estimated_billed_cost_usd"])
    prior_usage = dict(original_report["usage"])
    prior_repair_binding = None
    if prior_repair_run_dir is not None:
        prior_manifest_path = prior_repair_run_dir / "package_manifest.json"
        prior_report = json.loads(
            (prior_repair_run_dir / "combined_coverage_report.json").read_text(encoding="utf-8")
        )
        prior_cost = float(prior_report["cumulative_conservative_cost_usd"])
        prior_usage = dict(prior_report["cumulative_usage"])
        prior_repair_binding = {
            "path": str(prior_repair_run_dir),
            "package_manifest_sha256": sha256_file(prior_manifest_path),
            "status": prior_report["status"],
            "conservative_cumulative_cost_usd": prior_cost,
        }
    if prior_cost > settings.hard_spending_cap_usd:
        raise ValueError("original conservative cost already exceeds the authorized cap")
    budget = _Budget(settings.hard_spending_cap_usd, initial_actual=prior_cost)

    failed_rows = list(load_jsonl(original_run_dir / "failed.jsonl"))
    retry_ids = {
        row["canonical_id"]
        for row in failed_rows
        if row.get("failure_reason") == "RETRIES_EXHAUSTED"
    }
    ambiguous_ids = {item["canonical_id"] for item in reconciliation}
    unprocessed_ids = {row["canonical_id"] for row in load_jsonl(original_run_dir / "unprocessed.jsonl")}
    candidates = [
        record
        for record in records
        if record["canonical_id"] in retry_ids | unprocessed_ids
        and record["canonical_id"] not in ambiguous_ids
    ]
    smoke_records = [record for record in candidates if record["canonical_id"] in retry_ids][:3]
    remaining = [record for record in candidates if record not in smoke_records]
    binding = {
        "artifact_class": "title_abstract_fast_track_evidence_id_repair_run",
        "status": "RUNNING",
        "base_amendment_version": "2.1.0",
        "repair_version": PROMPT_VERSION,
        "original_run": original_package,
        "prior_repair_run": prior_repair_binding,
        "batch": {"path": str(batch_path), "sha256": expected_batch_sha256, "records": 250},
        "prompt": {"path": str(prompt_path), "version": PROMPT_VERSION, "sha256": prompt_hash},
        "output_schema": {
            "version": OUTPUT_SCHEMA_VERSION,
            "sha256": _json_hash(schema),
            "schema": schema,
        },
        "provider": settings.provider_name,
        "model": settings.model,
        "parameters": {
            "max_output_tokens": settings.max_output_tokens,
            "reasoning_effort": settings.reasoning_effort,
            "response_schema_version": OUTPUT_SCHEMA_VERSION,
            "service_tier": settings.service_tier,
            "store": False,
            "structured_output": "title_abstract_fast_track_evidence_ids_v2_1_2",
            "verbosity": settings.verbosity,
        },
        "controls": {
            "hard_cumulative_spending_cap_usd": settings.hard_spending_cap_usd,
            "conservative_cost_before_this_repair_usd": prior_cost,
            "concurrency": settings.concurrency,
            "timeout_seconds": settings.timeout_seconds,
            "retry_limit": settings.retry_limit,
            "smoke_count": len(smoke_records),
        },
        "selection": {
            "retry_exhausted": len(retry_ids),
            "original_unprocessed": len(unprocessed_ids),
            "ambiguous_excluded_from_resubmission": len(ambiguous_ids),
            "repair_candidates": len(candidates),
        },
        "model_input_policy": "canonical_id_and_deterministic_title_abstract_evidence_units_only",
        "tools": [],
    }
    _create_json(output_dir / "run_binding.json", binding)

    smoke_results = _run_group(
        smoke_records,
        output_dir / "records",
        prompt,
        prompt_hash,
        settings,
        provider,
        budget,
        clock,
        concurrency=1,
    )
    smoke_passed = len(smoke_results) == len(smoke_records) and all(
        item.get("status") == "VALIDATED" for item in smoke_results
    )
    if smoke_passed:
        _run_group(
            remaining,
            output_dir / "records",
            prompt,
            prompt_hash,
            settings,
            provider,
            budget,
            clock,
            concurrency=settings.concurrency,
        )
    return _write_repair_outputs(
        records=records,
        original_run_dir=original_run_dir,
        output_dir=output_dir,
        binding=binding,
        smoke_passed=smoke_passed,
        reconciliation=reconciliation,
        prior_cost=prior_cost,
        prior_usage=prior_usage,
    )


def _run_group(
    records: list[dict[str, Any]],
    results_dir: Path,
    prompt: str,
    prompt_hash: str,
    settings: RunSettings,
    provider: InferenceProvider,
    budget: _Budget,
    clock: Callable[[], float],
    *,
    concurrency: int,
) -> list[dict[str, Any]]:
    outputs = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(
                _screen_record,
                record,
                results_dir,
                prompt,
                prompt_hash,
                settings,
                provider,
                budget,
                clock,
            ): record["canonical_id"]
            for record in records
        }
        for future in as_completed(futures):
            outputs.append(future.result())
    return outputs


def _screen_record(
    record: dict[str, Any],
    results_dir: Path,
    prompt: str,
    prompt_hash: str,
    settings: RunSettings,
    provider: InferenceProvider,
    budget: _Budget,
    clock: Callable[[], float],
) -> dict[str, Any]:
    record_dir = results_dir / record["canonical_id"].replace(":", "_")
    record_dir.mkdir(exist_ok=True)
    final_path = record_dir / "result.json"
    input_snapshot = inference_input_for(record)
    input_hash = _json_hash(input_snapshot)
    parameters = {
        "max_output_tokens": settings.max_output_tokens,
        "reasoning_effort": settings.reasoning_effort,
        "response_schema_version": OUTPUT_SCHEMA_VERSION,
        "service_tier": settings.service_tier,
        "store": False,
        "structured_output": "title_abstract_fast_track_evidence_ids_v2_1_2",
        "verbosity": settings.verbosity,
    }
    attempts = []
    for attempt_number in range(1, settings.retry_limit + 2):
        maximum_cost = _maximum_attempt_cost(prompt, input_snapshot, settings)
        if not budget.reserve(maximum_cost):
            result = _base_result(record, input_hash, prompt_hash, attempts)
            result.update(status="UNPROCESSED_BUDGET_STOP", failure_reason="CUMULATIVE_BUDGET_STOP")
            _create_json(final_path, result)
            return result
        request_id = _stable_id(
            "repair-v2.1.2", record["canonical_id"], input_hash, prompt_hash, str(attempt_number)
        )
        _create_json(
            record_dir / f"attempt-{attempt_number:03d}.reservation.json",
            {
                "request_id": request_id,
                "attempt_number": attempt_number,
                "maximum_reserved_cost_usd": maximum_cost,
                "input_hash": input_hash,
            },
        )
        started = clock()
        raw_response = ""
        metadata: dict[str, Any] = {}
        validated = None
        error = None
        try:
            raw_response = provider.generate(
                model=settings.model,
                prompt=prompt,
                input_snapshot=input_snapshot,
                parameters=parameters,
                request_id=request_id,
                attempt_number=attempt_number,
            )
            if not isinstance(raw_response, str):
                raise TypeError("provider response must be a string")
            validated = validate_evidence_unit_payload(json.loads(raw_response), record)
        except Exception as exc:  # noqa: BLE001 - saved response/error is required evidence
            error = f"{type(exc).__name__}: {exc}"
        try:
            if isinstance(provider, OpenAIResponsesProvider) or hasattr(provider, "metadata_for"):
                metadata = dict(provider.metadata_for(request_id, attempt_number))
        except Exception as exc:  # noqa: BLE001
            error = error or f"metadata error: {type(exc).__name__}: {exc}"
            validated = None
        latency = max(0.0, clock() - started)
        try:
            usage = _normalize_usage(metadata.get("provider_usage"))
        except (TypeError, ValueError) as exc:
            usage = _zero_usage()
            error = error or f"invalid provider usage: {exc}"
            validated = None
        actual_cost = _usage_cost(usage, settings.pricing) if any(usage.values()) else maximum_cost
        budget.settle(maximum_cost, actual_cost)
        attempt = {
            "request_id": request_id,
            "attempt_number": attempt_number,
            "status": "VALID" if validated is not None else "INVALID",
            "latency_seconds": latency,
            "usage": usage,
            "estimated_billed_cost_usd": actual_cost,
            "raw_response": raw_response,
            "validation_error": error,
            "provider_metadata": metadata,
        }
        attempts.append(attempt)
        _create_json(record_dir / f"attempt-{attempt_number:03d}.json", attempt)
        if validated is not None:
            result = _base_result(record, input_hash, prompt_hash, attempts)
            result.update(status="VALIDATED", judgment=validated)
            _create_json(final_path, result)
            return result
    result = _base_result(record, input_hash, prompt_hash, attempts)
    result.update(status="FAILED", failure_reason="RETRIES_EXHAUSTED")
    _create_json(final_path, result)
    return result


def _base_result(
    record: Mapping[str, Any],
    input_hash: str,
    prompt_hash: str,
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "canonical_id": record["canonical_id"],
        "batch_order": record["batch_order"],
        "title": record["title"],
        "abstract": record["abstract"],
        "input_hash": input_hash,
        "prompt_sha256": prompt_hash,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "attempts": attempts,
    }


def _write_repair_outputs(
    *,
    records: list[dict[str, Any]],
    original_run_dir: Path,
    output_dir: Path,
    binding: Mapping[str, Any],
    smoke_passed: bool,
    reconciliation: list[dict[str, Any]],
    prior_cost: float,
    prior_usage: Mapping[str, int],
) -> dict[str, Any]:
    by_id = {record["canonical_id"]: record for record in records}
    original_valid = []
    for group in ("include", "deferred", "excluded"):
        for row in load_jsonl(original_run_dir / f"{group}.jsonl"):
            original_valid.append(
                {
                    **row,
                    "result_version": "ORIGINAL_FAST_TRACK_V2_1_0_SCHEMA_1_3_0",
                    "supersession": "PRESERVED_UNCHANGED",
                }
            )
    repair_results = []
    for path in sorted((output_dir / "records").glob("*/result.json")):
        repair_results.append(json.loads(path.read_text(encoding="utf-8")))
    repair_by_id = {row["canonical_id"]: row for row in repair_results}
    combined = list(original_valid)
    for row in repair_results:
        if row.get("status") == "VALIDATED":
            combined.append(
                {
                    **row,
                    "result_version": "EVIDENCE_ID_REPAIR_V2_1_2_SCHEMA_1_4_1",
                    "supersedes_failed_result_in_original_run": row["canonical_id"]
                    in {item["canonical_id"] for item in load_jsonl(original_run_dir / "failed.jsonl")},
                }
            )
    unresolved_ids = {item["canonical_id"] for item in reconciliation}
    groups: dict[str, list[dict[str, Any]]] = {
        "include": [],
        "deferred": [],
        "excluded": [],
        "failed": [],
        "unresolved": [],
        "unprocessed": [],
    }
    for row in combined:
        disposition = row["judgment"]["operational_disposition"]
        groups[
            {
                "ADVANCE_TO_FULL_REPORT_ASSESSMENT": "include",
                "DEFER": "deferred",
                "EXCLUDE": "excluded",
            }[disposition]
        ].append(row)
    groups["failed"] = [row for row in repair_results if row.get("status") == "FAILED"]
    groups["unresolved"] = [
        {
            "canonical_id": canonical_id,
            "batch_order": by_id[canonical_id]["batch_order"],
            "title": by_id[canonical_id]["title"],
            "status": "UNRESOLVED_AMBIGUOUS_INTERRUPTED_REQUEST_NOT_RESUBMITTED",
        }
        for canonical_id in unresolved_ids
    ]
    covered_ids = {
        row["canonical_id"]
        for name in ("include", "deferred", "excluded", "failed", "unresolved")
        for row in groups[name]
    }
    groups["unprocessed"] = [
        {
            "canonical_id": record["canonical_id"],
            "batch_order": record["batch_order"],
            "title": record["title"],
            "status": repair_by_id.get(record["canonical_id"], {}).get(
                "status", "UNPROCESSED_REPAIR_NOT_RUN"
            ),
        }
        for record in records
        if record["canonical_id"] not in covered_ids
    ]
    for name, rows in groups.items():
        _write_jsonl(output_dir / f"combined_{name}.jsonl", rows)

    handoff_path = output_dir / "combined_clear_include_handoff.csv"
    with handoff_path.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "canonical_id",
            "title",
            "doi",
            "source_url",
            "reasons",
            "e6_status",
            "result_version",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in sorted(groups["include"], key=lambda value: value["batch_order"]):
            source = by_id[row["canonical_id"]]
            writer.writerow(
                {
                    "canonical_id": row["canonical_id"],
                    "title": row["title"],
                    "doi": source.get("doi") or "",
                    "source_url": source.get("source_url") or "",
                    "reasons": row["judgment"]["overall_rationale"],
                    "e6_status": E6_STATUS,
                    "result_version": row["result_version"],
                }
            )

    original_report = json.loads((original_run_dir / "run_report.json").read_text(encoding="utf-8"))
    repair_usage = _zero_usage()
    repair_cost = 0.0
    repair_latency = 0.0
    repair_requests = 0
    repair_retries = 0
    for row in repair_results:
        for attempt in row.get("attempts", []):
            repair_requests += 1
            repair_retries += int(attempt.get("attempt_number", 1) > 1)
            repair_cost += float(attempt.get("estimated_billed_cost_usd", 0.0))
            repair_latency += float(attempt.get("latency_seconds", 0.0))
            for key in repair_usage:
                repair_usage[key] += int(attempt.get("usage", {}).get(key, 0))
    cumulative_usage = {key: int(prior_usage.get(key, 0)) + repair_usage[key] for key in repair_usage}
    deferral_breakdown = _deferral_breakdown(groups["deferred"])
    _write_json(output_dir / "combined_deferral_breakdown.json", deferral_breakdown)
    consistency_observations = []
    for row in combined:
        responses = row["judgment"]["responses"]
        if responses["E7"] == "YES" and any(
            responses[key] == "UNCERTAIN" for key in ("E1", "E2", "E3", "E4", "E5")
        ):
            consistency_observations.append(
                {
                    "canonical_id": row["canonical_id"],
                    "batch_order": row["batch_order"],
                    "observation": "E7_YES_WITH_UNRESOLVED_SCIENTIFIC_CRITERION",
                    "handling": "PRESERVED_RESPONSE; FLAGGED; NOT_IN_CLEAR_INCLUDE_HANDOFF",
                    "result_version": row["result_version"],
                    "computed_outcome": row["judgment"]["computed_outcome"],
                    "operational_disposition": row["judgment"]["operational_disposition"],
                }
            )
    _write_json(output_dir / "post_run_consistency_observations.json", consistency_observations)
    report = {
        **binding,
        "status": "COMPLETE" if smoke_passed and not groups["unprocessed"] else "PARTIAL",
        "smoke_test_passed": smoke_passed,
        "counts": {name: len(rows) for name, rows in groups.items()},
        "original_valid_results_preserved": len(original_valid),
        "repair_validated": sum(row.get("status") == "VALIDATED" for row in repair_results),
        "original_usage": original_report["usage"],
        "usage_before_this_repair": dict(prior_usage),
        "repair_usage": repair_usage,
        "cumulative_usage": cumulative_usage,
        "original_conservative_cost_usd": original_report["estimated_billed_cost_usd"],
        "conservative_cost_before_this_repair_usd": prior_cost,
        "repair_estimated_cost_usd": repair_cost,
        "cumulative_conservative_cost_usd": prior_cost + repair_cost,
        "hard_cumulative_spending_cap_usd": binding["controls"]["hard_cumulative_spending_cap_usd"],
        "repair_requests": repair_requests,
        "repair_retries": repair_retries,
        "repair_aggregate_latency_seconds": repair_latency,
        "ambiguous_reconciliation": reconciliation,
        "deferral_breakdown": deferral_breakdown,
        "post_run_consistency_observation_count": len(consistency_observations),
        "accuracy_claim": "NOT_ASSESSED_BY_THIS_OPERATIONAL_RUN",
        "staging_only": True,
    }
    _write_json(output_dir / "combined_coverage_report.json", report)
    _write_report_markdown(output_dir / "combined_coverage_report.md", report)
    _write_manifest(output_dir, report)
    return report


def _deferral_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    uncertain = {
        key: sum(row["judgment"]["responses"][key] == "UNCERTAIN" for row in rows)
        for key in ("E1", "E2", "E3", "E4", "E5", "E7")
    }
    return {
        "total": len(rows),
        "missing_abstract": sum(_abstract_missing(row) for row in rows),
        "uncertain_by_criterion_overlapping": uncertain,
        "e7_only_uncertainty": sum(
            row["judgment"]["responses"]["E7"] == "UNCERTAIN"
            and all(row["judgment"]["responses"][key] == "YES" for key in ("E1", "E2", "E3", "E4", "E5"))
            for row in rows
        ),
    }


def _abstract_missing(row: Mapping[str, Any]) -> bool:
    if "abstract_missing" in row:
        return bool(row["abstract_missing"])
    return not str(row.get("abstract", "")).strip()


def _write_report_markdown(path: Path, report: Mapping[str, Any]) -> None:
    counts = report["counts"]
    text = f"""# Fast-track evidence-ID repair combined coverage

Status: `{report['status']}`

- Original valid results preserved unchanged: {report['original_valid_results_preserved']}.
- Repair-validated records: {report['repair_validated']}.
- Combined INCLUDE: {counts['include']}.
- Combined DEFER: {counts['deferred']}.
- Combined EXCLUDED: {counts['excluded']}.
- Failed: {counts['failed']}.
- Unresolved ambiguous interrupted requests: {counts['unresolved']}.
- Unprocessed: {counts['unprocessed']}.
- Repair requests: {report['repair_requests']} ({report['repair_retries']} retries).
- Repair estimated cost: USD {report['repair_estimated_cost_usd']:.6f}.
- Cumulative conservative cost: USD {report['cumulative_conservative_cost_usd']:.6f} of USD {report['hard_cumulative_spending_cap_usd']:.2f}.
- Repair aggregate latency: {report['repair_aggregate_latency_seconds']:.3f} seconds.

The four ambiguous interrupted requests were not resubmitted. This staging-only repair
does not assess screening accuracy and does not modify the original run or production state.
"""
    path.write_text(text, encoding="utf-8", newline="\n")


def _write_manifest(output_dir: Path, report: Mapping[str, Any]) -> None:
    names = [
        "run_binding.json",
        "offline_failure_audit.json",
        "original_deferral_breakdown.json",
        "ambiguous_reconciliation.json",
        "combined_coverage_report.json",
        "combined_coverage_report.md",
        "combined_deferral_breakdown.json",
        "post_run_consistency_observations.json",
        "combined_clear_include_handoff.csv",
        "combined_include.jsonl",
        "combined_deferred.jsonl",
        "combined_excluded.jsonl",
        "combined_failed.jsonl",
        "combined_unresolved.jsonl",
        "combined_unprocessed.jsonl",
    ]
    artifacts = {
        name: {"sha256": sha256_file(output_dir / name), "bytes": (output_dir / name).stat().st_size}
        for name in names
    }
    _write_json(
        output_dir / "package_manifest.json",
        {
            "artifact_class": "title_abstract_fast_track_evidence_id_repair_package",
            "repair_version": PROMPT_VERSION,
            "status": report["status"],
            "counts": report["counts"],
            "artifacts": artifacts,
            "staging_only": True,
        },
    )


def _zero_usage() -> dict[str, int]:
    return {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "cache_write_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
    }


def _stable_id(*parts: str) -> str:
    return "request:" + hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]


def _json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _create_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _write_jsonl(path: Path, values: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for value in sorted(values, key=lambda item: item["batch_order"]):
            handle.write(json.dumps(value, sort_keys=True, ensure_ascii=True) + "\n")
