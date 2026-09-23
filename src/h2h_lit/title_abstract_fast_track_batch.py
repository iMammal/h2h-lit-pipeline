"""Bounded, staging-only runner for the approved title/abstract fast track."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from h2h_lit.inference import InferenceProvider
from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.pilot5d import pilot5d_response_schema
from h2h_lit.title_abstract_screening import E6_STATUS, recompute_outcome

PROMPT_VERSION = "2.1.0"
OUTPUT_SCHEMA_VERSION = "1.3.0"
AMENDMENT_VERSION = "2.1.0"
CRITERION_KEYS = (
    "E1_life_science_application",
    "E2_relational_multiscale_relevance",
    "E3_interactive_visual_analytics",
    "E4_computational_assistance",
    "E5_human_analytic_relationship",
    "E7_evidence_sufficiency",
)
SHORT_KEYS = dict(zip(CRITERION_KEYS, ("E1", "E2", "E3", "E4", "E5", "E7"), strict=True))
EXCLUSION_REASONS = {
    "E1": "NO_LIFE_SCIENCE_APPLICATION",
    "E2": "NO_RELATIONAL_OR_MULTISCALE_RELEVANCE",
    "E3": "NO_INTERACTIVE_VISUAL_ANALYTICS",
    "E4": "NO_COMPUTATIONAL_ASSISTANCE",
    "E5": "NO_HUMAN_ANALYTIC_RELATIONSHIP",
}


@dataclass(frozen=True, slots=True)
class Pricing:
    input_usd_per_million: float
    cached_input_usd_per_million: float
    cache_write_usd_per_million: float
    output_usd_per_million: float

    def validate(self) -> None:
        if min(
                self.input_usd_per_million,
                self.cached_input_usd_per_million,
                self.cache_write_usd_per_million,
                self.output_usd_per_million,
        ) < 0:
            raise ValueError("pricing values must be non-negative")


@dataclass(frozen=True, slots=True)
class RunSettings:
    provider_name: str
    model: str
    hard_spending_cap_usd: float
    pricing: Pricing
    max_output_tokens: int = 2500
    reasoning_effort: str = "low"
    verbosity: str = "low"
    concurrency: int = 4
    timeout_seconds: float = 120.0
    retry_limit: int = 1
    smoke_count: int = 3
    service_tier: str = "default"

    def validate(self) -> None:
        if self.provider_name != "OpenAI":
            raise ValueError("only the configured OpenAI Responses provider is supported")
        if self.service_tier != "default":
            raise ValueError("this authorized run requires standard/default processing")
        if not self.model.strip():
            raise ValueError("exact model is required")
        if self.hard_spending_cap_usd <= 0:
            raise ValueError("hard spending cap must be positive")
        if self.max_output_tokens < 1 or self.concurrency < 1 or self.retry_limit < 0:
            raise ValueError("invalid execution bounds")
        if self.timeout_seconds <= 0 or self.smoke_count < 1:
            raise ValueError("invalid timeout or smoke count")
        self.pricing.validate()


class _Budget:
    def __init__(self, cap: float, *, initial_actual: float = 0.0):
        self.cap = cap
        self.actual = initial_actual
        self.reserved = 0.0
        self.lock = threading.Lock()

    def reserve(self, maximum: float) -> bool:
        with self.lock:
            if self.actual + self.reserved + maximum > self.cap + 1e-12:
                return False
            self.reserved += maximum
            return True

    def settle(self, maximum: float, actual: float) -> None:
        with self.lock:
            self.reserved -= maximum
            self.actual += actual
            if self.actual > self.cap + 1e-9:
                raise RuntimeError("provider usage exceeded conservatively reserved budget")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"batch line {line_number} is not an object")
            yield value


def validate_batch(records: list[dict[str, Any]], *, expected_count: int = 250) -> None:
    if len(records) != expected_count:
        raise ValueError(f"expected {expected_count} records, found {len(records)}")
    ids = [item.get("canonical_id") for item in records]
    if any(not isinstance(item, str) or not item for item in ids):
        raise ValueError("every record requires a canonical_id")
    if len(set(ids)) != len(ids):
        raise ValueError("batch contains duplicate canonical IDs")
    for index, record in enumerate(records, 1):
        if record.get("batch_order") != index:
            raise ValueError("batch order is not contiguous")
        if record.get("batch_status") != "UNPROCESSED_READY_NOT_LAUNCHED":
            raise ValueError("batch contains an unexpected prior status")
        if not isinstance(record.get("title"), str) or not isinstance(record.get("abstract"), str):
            raise ValueError("title and abstract must be strings")


def verify_execution_bindings(
        *,
        records: list[dict[str, Any]],
        batch_path: Path,
        expected_batch_sha256: str,
        batch_manifest_path: Path,
        sampling_manifest_path: Path,
        corpus_path: Path,
        protocol_path: Path,
        amendment_path: Path,
) -> dict[str, Any]:
    """Recheck frozen input and protocol bindings without loading the corpus."""

    batch_hash = sha256_file(batch_path)
    if batch_hash != expected_batch_sha256:
        raise ValueError("batch SHA-256 does not match the authorized binding")
    batch_manifest = json.loads(batch_manifest_path.read_text(encoding="utf-8"))
    if (
            batch_manifest.get("batch_sha256") != batch_hash
            or batch_manifest.get("batch_size") != len(records)
            or batch_manifest.get("frame", {}).get("path") != str(corpus_path)
    ):
        raise ValueError("batch manifest does not bind the supplied batch and corpus")

    sampling_manifest = json.loads(sampling_manifest_path.read_text(encoding="utf-8"))
    selected = sampling_manifest.get("selected_records")
    if not isinstance(selected, list):
        raise ValueError("sampling manifest has no selected_records")
    frozen_ids = {item.get("canonical_id") for item in selected if isinstance(item, dict)}
    batch_ids = {item["canonical_id"] for item in records}
    overlap = sorted(batch_ids & frozen_ids)
    if overlap:
        raise ValueError(f"batch overlaps frozen evaluation/calibration records: {overlap}")
    group_counts: dict[str, int] = {}
    for item in selected:
        group = item.get("selection_group")
        group_counts[group] = group_counts.get(group, 0) + 1
    if group_counts != {"challenge": 30, "uniform_random": 70, "calibration": 10}:
        raise ValueError("unexpected frozen sampling membership counts")

    corpus_hash = sha256_file(corpus_path)
    if corpus_hash != batch_manifest.get("frame", {}).get("sha256"):
        raise ValueError("registered corpus hash does not match the batch manifest")
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if protocol.get("protocol_version") != "2.0.0":
        raise ValueError("unexpected base protocol version")
    if protocol.get("assessed_criteria") != ["E1", "E2", "E3", "E4", "E5", "E7"]:
        raise ValueError("unexpected assessed criteria")
    if protocol.get("e6", {}).get("status") != E6_STATUS:
        raise ValueError("unexpected E6 protocol status")
    if amendment.get("amendment_version") != AMENDMENT_VERSION:
        raise ValueError("unexpected fast-track amendment version")

    return {
        "batch": {
            "path": str(batch_path),
            "sha256": batch_hash,
            "records": len(records),
            "unique_ids": len(batch_ids),
        },
        "batch_manifest": {
            "path": str(batch_manifest_path),
            "sha256": sha256_file(batch_manifest_path),
        },
        "sampling_manifest": {
            "path": str(sampling_manifest_path),
            "sha256": sha256_file(sampling_manifest_path),
            "frozen_counts": group_counts,
            "overlap": 0,
        },
        "registered_corpus": {
            "path": str(corpus_path),
            "sha256": corpus_hash,
            "canonical_records": batch_manifest["frame"]["canonical_records"],
            "loaded": False,
        },
        "base_protocol": {
            "path": str(protocol_path),
            "sha256": sha256_file(protocol_path),
            "version": protocol["protocol_version"],
        },
        "fast_track_amendment": {
            "path": str(amendment_path),
            "sha256": sha256_file(amendment_path),
            "version": amendment["amendment_version"],
        },
    }


def validate_model_payload(payload: Any, record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or set(payload) != {"criteria", "overall_rationale"}:
        raise ValueError("response must contain exactly criteria and overall_rationale")
    criteria = payload["criteria"]
    if not isinstance(criteria, dict) or set(criteria) != set(CRITERION_KEYS):
        raise ValueError("criteria must contain E1-E5 and E7 exactly once")
    rationale = payload["overall_rationale"]
    if not isinstance(rationale, str) or not rationale.strip():
        raise ValueError("overall_rationale must be a non-empty string")

    responses: dict[str, str] = {}
    normalized: dict[str, Any] = {}
    for key in CRITERION_KEYS:
        item = criteria[key]
        if not isinstance(item, dict) or set(item) != {
            "decision",
            "certainty",
            "evidence",
            "rationale",
        }:
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
        evidence = item["evidence"]
        if not isinstance(evidence, list) or not evidence:
            raise ValueError(f"{key}.evidence must contain at least one item")
        checked_evidence = []
        for evidence_item in evidence:
            checked_evidence.append(_validate_evidence(evidence_item, record, key))
        short_key = SHORT_KEYS[key]
        responses[short_key] = decision
        normalized[key] = {**item, "evidence": checked_evidence}

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
        "overall_rationale": rationale.strip(),
        "responses": responses,
        "e6_status": E6_STATUS,
        "computed_outcome": outcome,
        "operational_disposition": disposition,
        "exclusion_reasons": [EXCLUSION_REASONS[key] for key in no_criteria],
    }


def _validate_evidence(item: Any, record: Mapping[str, Any], criterion: str) -> dict[str, Any]:
    required = {"quote", "source_field", "locator", "claimed_start", "claimed_end"}
    if not isinstance(item, dict) or set(item) != required:
        raise ValueError(f"{criterion}.evidence item has invalid fields")
    field = item["source_field"]
    if field not in {"title", "abstract"}:
        raise ValueError(f"{criterion}.evidence source_field is invalid")
    if item["locator"] != f"input.{field}":
        raise ValueError(f"{criterion}.evidence locator does not match source_field")
    quote = item["quote"]
    source = str(record.get(field, ""))
    if not isinstance(quote, str) or not quote.strip() or quote not in source:
        raise ValueError(f"{criterion}.evidence quote is not an exact supplied-field substring")
    for offset in ("claimed_start", "claimed_end"):
        if item[offset] is not None and not isinstance(item[offset], int):
            raise ValueError(f"{criterion}.evidence {offset} must be integer or null")
    return dict(item)


def run_batch(
        *,
        batch_path: Path,
        output_dir: Path,
        prompt_path: Path,
        settings: RunSettings,
        provider: InferenceProvider,
        expected_batch_sha256: str,
        batch_manifest_path: Path,
        sampling_manifest_path: Path,
        corpus_path: Path,
        protocol_path: Path,
        amendment_path: Path,
        resume: bool = False,
        clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Run the fixed batch without constructing or mutating a ReviewDataset."""

    settings.validate()
    records = list(load_jsonl(batch_path))
    validate_batch(records)
    bindings = verify_execution_bindings(
        records=records,
        batch_path=batch_path,
        expected_batch_sha256=expected_batch_sha256,
        batch_manifest_path=batch_manifest_path,
        sampling_manifest_path=sampling_manifest_path,
        corpus_path=corpus_path,
        protocol_path=protocol_path,
        amendment_path=amendment_path,
    )
    prompt = prompt_path.read_text(encoding="utf-8")
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    if f"Prompt-Version: {PROMPT_VERSION}" not in prompt:
        raise ValueError("unexpected prompt version")

    if output_dir.exists() and not resume:
        raise FileExistsError("output directory already exists; use explicit resume")
    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "records"
    results_dir.mkdir(exist_ok=True)
    output_schema = pilot5d_response_schema()
    run_binding = {
        "artifact_class": "title_abstract_fast_track_batch_run",
        "status": "RUNNING",
        "amendment_version": AMENDMENT_VERSION,
        "bindings": bindings,
        "prompt": {"path": str(prompt_path), "version": PROMPT_VERSION, "sha256": prompt_hash},
        "output_schema": {
            "version": OUTPUT_SCHEMA_VERSION,
            "sha256": _json_hash(output_schema),
            "schema": output_schema,
        },
        "implementation": {
            "batch_module": {"path": __file__, "sha256": sha256_file(Path(__file__))},
            "provider_module": {
                "path": str(Path(__file__).with_name("openai_provider.py")),
                "sha256": sha256_file(Path(__file__).with_name("openai_provider.py")),
            },
        },
        "provider": settings.provider_name,
        "model": settings.model,
        "parameters": {
            "max_output_tokens": settings.max_output_tokens,
            "reasoning_effort": settings.reasoning_effort,
            "response_schema_version": OUTPUT_SCHEMA_VERSION,
            "service_tier": settings.service_tier,
            "store": False,
            "structured_output": "title_abstract_fast_track_v2_1_0",
            "verbosity": settings.verbosity,
        },
        "pricing_usd_per_million_tokens": {
            "input": settings.pricing.input_usd_per_million,
            "cached_input": settings.pricing.cached_input_usd_per_million,
            "cache_write": settings.pricing.cache_write_usd_per_million,
            "output": settings.pricing.output_usd_per_million,
        },
        "controls": {
            "hard_spending_cap_usd": settings.hard_spending_cap_usd,
            "concurrency": settings.concurrency,
            "timeout_seconds": settings.timeout_seconds,
            "retry_limit": settings.retry_limit,
            "smoke_count": settings.smoke_count,
        },
        "model_input_policy": "canonical_id_title_abstract_only_no_prior_judgments",
        "tools": [],
    }
    _create_or_match_json(output_dir / "run_binding.json", run_binding)
    existing = _load_existing_results(results_dir)
    prior_cost = sum(
        float(attempt.get("estimated_billed_cost_usd", 0.0))
        for result in existing.values()
        for attempt in result.get("attempts", [])
    )
    if prior_cost > settings.hard_spending_cap_usd + 1e-9:
        raise ValueError("persisted attempt cost exceeds the authorized spending cap")
    budget = _Budget(settings.hard_spending_cap_usd, initial_actual=prior_cost)

    pending = [record for record in records if record["canonical_id"] not in existing]
    smoke_pending = pending[: settings.smoke_count]
    smoke_results = _run_group(
        smoke_pending, results_dir, prompt, prompt_hash, settings, provider, budget, clock, concurrency=1
    )
    smoke_passed = all(item.get("status") == "VALIDATED" for item in smoke_results)
    if smoke_passed:
        remaining = pending[len(smoke_pending):]
        _run_group(
            remaining,
            results_dir,
            prompt,
            prompt_hash,
            settings,
            provider,
            budget,
            clock,
            concurrency=settings.concurrency,
        )

    report = _write_package_outputs(
        records=records,
        output_dir=output_dir,
        run_binding=run_binding,
        smoke_passed=smoke_passed,
        budget_cap=settings.hard_spending_cap_usd,
    )
    return report


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
    if not records:
        return []
    outputs: list[dict[str, Any]] = []
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
    if final_path.exists():
        return json.loads(final_path.read_text(encoding="utf-8"))

    input_snapshot = {
        "canonical_record_id": record["canonical_id"],
        "title": record["title"],
        "abstract": record["abstract"],
    }
    input_hash = _json_hash(input_snapshot)
    parameters = {
        "max_output_tokens": settings.max_output_tokens,
        "reasoning_effort": settings.reasoning_effort,
        "response_schema_version": OUTPUT_SCHEMA_VERSION,
        "service_tier": settings.service_tier,
        "store": False,
        "structured_output": "title_abstract_fast_track_v2_1_0",
        "verbosity": settings.verbosity,
    }
    attempts: list[dict[str, Any]] = []
    first_new_attempt: int | None = None
    for attempt_number in range(1, settings.retry_limit + 2):
        attempt_path = record_dir / f"attempt-{attempt_number:03d}.json"
        reservation_path = record_dir / f"attempt-{attempt_number:03d}.reservation.json"
        if attempt_path.exists():
            attempt = json.loads(attempt_path.read_text(encoding="utf-8"))
            attempts.append(attempt)
            if attempt.get("status") == "VALID":
                payload = json.loads(attempt["raw_response"])
                validated = validate_model_payload(payload, record)
                result = _validated_result(
                    record, input_hash, prompt_hash, validated, attempts
                )
                _create_json(final_path, result)
                return result
            continue
        if reservation_path.exists():
            result = _failed_result(
                record,
                input_hash,
                prompt_hash,
                attempts,
                "AMBIGUOUS_PRIOR_SUBMISSION_NOT_RETRIED",
            )
            _create_json(final_path, result)
            return result
        first_new_attempt = attempt_number
        break
    else:
        result = _failed_result(record, input_hash, prompt_hash, attempts, "RETRIES_EXHAUSTED")
        _create_json(final_path, result)
        return result

    if first_new_attempt is None:
        raise RuntimeError("failed to resolve the next create-only attempt number")
    for attempt_number in range(first_new_attempt, settings.retry_limit + 2):
        attempt_path = record_dir / f"attempt-{attempt_number:03d}.json"
        reservation_path = record_dir / f"attempt-{attempt_number:03d}.reservation.json"
        maximum_cost = _maximum_attempt_cost(prompt, input_snapshot, settings)
        if not budget.reserve(maximum_cost):
            return {
                "status": "UNPROCESSED_BUDGET_STOP",
                "canonical_id": record["canonical_id"],
                "batch_order": record["batch_order"],
            }
        request_id = _stable_id(record["canonical_id"], input_hash, prompt_hash, str(attempt_number))
        _create_json(
            reservation_path,
            {
                "request_id": request_id,
                "attempt_number": attempt_number,
                "maximum_reserved_cost_usd": maximum_cost,
                "input_hash": input_hash,
            },
        )
        started = clock()
        raw_response = ""
        provider_metadata: dict[str, Any] = {}
        error: str | None = None
        validated: dict[str, Any] | None = None
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
            payload = json.loads(raw_response)
            validated = validate_model_payload(payload, record)
        except Exception as exc:  # noqa: BLE001 - every failed attempt is evidence
            error = f"{type(exc).__name__}: {exc}"
        try:
            if isinstance(provider, OpenAIResponsesProvider):
                provider_metadata = provider.metadata_for(request_id, attempt_number)
            elif hasattr(provider, "metadata_for"):
                provider_metadata = dict(provider.metadata_for(request_id, attempt_number))
        except Exception as exc:  # noqa: BLE001 - metadata failure is part of the attempt
            provider_metadata = {"metadata_error": f"{type(exc).__name__}: {exc}"}
            error = error or provider_metadata["metadata_error"]
            validated = None
        latency = max(0.0, clock() - started)
        try:
            usage = _normalize_usage(provider_metadata.get("provider_usage"))
        except (TypeError, ValueError) as exc:
            usage = {
                "input_tokens": 0,
                "cached_input_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 0,
                "reasoning_tokens": 0,
            }
            error = error or f"invalid provider usage: {exc}"
            validated = None
        usage_reported = any(usage.values())
        actual_cost = (
            _usage_cost(usage, settings.pricing) if usage_reported else maximum_cost
        )
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
            "provider_metadata": provider_metadata,
        }
        _create_json(attempt_path, attempt)
        attempts.append(attempt)
        if validated is not None:
            result = _validated_result(record, input_hash, prompt_hash, validated, attempts)
            _create_json(final_path, result)
            return result
    result = _failed_result(record, input_hash, prompt_hash, attempts, "RETRIES_EXHAUSTED")
    _create_json(final_path, result)
    return result


def _validated_result(
        record: Mapping[str, Any],
        input_hash: str,
        prompt_hash: str,
        validated: dict[str, Any],
        attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "VALIDATED",
        "canonical_id": record["canonical_id"],
        "batch_order": record["batch_order"],
        "title": record["title"],
        "abstract_missing": not bool(str(record["abstract"]).strip()),
        "doi": record.get("doi"),
        "source_url": record.get("source_url"),
        "source_database": record.get("source_database"),
        "source_identifier": record.get("source_identifier"),
        "input_hash": input_hash,
        "prompt_hash": prompt_hash,
        "judgment": validated,
        "attempts": attempts,
    }


def _maximum_attempt_cost(prompt: str, snapshot: dict[str, Any], settings: RunSettings) -> float:
    serialized = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    conservative_input_tokens = len((prompt + serialized).encode("utf-8"))
    maximum_input_rate = max(
        settings.pricing.input_usd_per_million,
        settings.pricing.cache_write_usd_per_million,
    )
    return (
            conservative_input_tokens * maximum_input_rate
            + settings.max_output_tokens * settings.pricing.output_usd_per_million
    ) / 1_000_000


def _normalize_usage(value: Any) -> dict[str, int]:
    value = value if isinstance(value, dict) else {}
    input_tokens = int(value.get("input_tokens") or 0)
    output_tokens = int(value.get("output_tokens") or 0)
    input_details = value.get("input_tokens_details")
    cached = (
        int(input_details.get("cached_tokens") or 0)
        if isinstance(input_details, dict)
        else 0
    )
    cache_write = (
        int(input_details.get("cache_write_tokens") or 0)
        if isinstance(input_details, dict)
        else 0
    )
    output_details = value.get("output_tokens_details")
    reasoning = (
        int(output_details.get("reasoning_tokens") or 0)
        if isinstance(output_details, dict)
        else 0
    )
    if (
            min(input_tokens, output_tokens, cached, cache_write, reasoning) < 0
            or cached + cache_write > input_tokens
            or reasoning > output_tokens
    ):
        raise ValueError("provider returned invalid token usage")
    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": cached,
        "cache_write_tokens": cache_write,
        "output_tokens": output_tokens,
        "reasoning_tokens": reasoning,
    }


def _usage_cost(usage: Mapping[str, int], pricing: Pricing) -> float:
    uncached = (
            usage["input_tokens"]
            - usage["cached_input_tokens"]
            - usage["cache_write_tokens"]
    )
    return (
            uncached * pricing.input_usd_per_million
            + usage["cached_input_tokens"] * pricing.cached_input_usd_per_million
            + usage["cache_write_tokens"] * pricing.cache_write_usd_per_million
            + usage["output_tokens"] * pricing.output_usd_per_million
    ) / 1_000_000


def _failed_result(
        record: Mapping[str, Any],
        input_hash: str,
        prompt_hash: str,
        attempts: list[dict[str, Any]],
        reason: str,
) -> dict[str, Any]:
    return {
        "status": "FAILED",
        "failure_reason": reason,
        "canonical_id": record["canonical_id"],
        "batch_order": record["batch_order"],
        "title": record["title"],
        "input_hash": input_hash,
        "prompt_hash": prompt_hash,
        "attempts": attempts,
    }


def _load_existing_results(results_dir: Path) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for path in results_dir.glob("*/result.json"):
        value = json.loads(path.read_text(encoding="utf-8"))
        record_id = value.get("canonical_id")
        if not isinstance(record_id, str) or record_id in results:
            raise ValueError("invalid or duplicate persisted result")
        results[record_id] = value
    return results


def _write_package_outputs(
        *,
        records: list[dict[str, Any]],
        output_dir: Path,
        run_binding: dict[str, Any],
        smoke_passed: bool,
        budget_cap: float,
        stopped_reason: str | None = None,
) -> dict[str, Any]:
    results = _load_existing_results(output_dir / "records")
    groups: dict[str, list[dict[str, Any]]] = {
        "include": [],
        "deferred": [],
        "excluded": [],
        "failed": [],
        "unprocessed": [],
    }
    total_usage = {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "cache_write_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
    }
    total_cost = 0.0
    total_latency = 0.0
    completed_attempt_total = 0
    provider_service_tiers: dict[str, int] = {}
    provider_models: dict[str, int] = {}
    by_id = {item["canonical_id"]: item for item in records}
    for record in records:
        result = results.get(record["canonical_id"])
        if result is None:
            groups["unprocessed"].append(record)
            continue
        attempts = result.get("attempts", [])
        completed_attempt_total += len(attempts)
        for attempt in attempts:
            for key in total_usage:
                total_usage[key] += int(attempt.get("usage", {}).get(key, 0))
            total_cost += float(attempt.get("estimated_billed_cost_usd", 0.0))
            total_latency += float(attempt.get("latency_seconds", 0.0))
            metadata = attempt.get("provider_metadata", {})
            service_tier = metadata.get("provider_service_tier")
            provider_model = metadata.get("provider_model")
            if service_tier:
                provider_service_tiers[service_tier] = (
                        provider_service_tiers.get(service_tier, 0) + 1
                )
            if provider_model:
                provider_models[provider_model] = (
                        provider_models.get(provider_model, 0) + 1
                )
        if result["status"] != "VALIDATED":
            groups["failed"].append(result)
            continue
        disposition = result["judgment"]["operational_disposition"]
        target = {
            "ADVANCE_TO_FULL_REPORT_ASSESSMENT": "include",
            "DEFER": "deferred",
            "EXCLUDE": "excluded",
        }[disposition]
        groups[target].append(result)

    ambiguous_inflight = []
    reservations = sorted(
        (output_dir / "records").glob("*/attempt-*.reservation.json")
    )
    submitted_request_total = len(reservations)
    retry_total = sum(
        1
        for reservation_path in reservations
        if int(
            json.loads(reservation_path.read_text(encoding="utf-8")).get(
                "attempt_number", 1
            )
        )
        > 1
    )
    for reservation_path in reservations:
        attempt_path = reservation_path.with_name(
            reservation_path.name.replace(".reservation.json", ".json")
        )
        if attempt_path.exists():
            continue
        reservation = json.loads(reservation_path.read_text(encoding="utf-8"))
        total_cost += float(reservation.get("maximum_reserved_cost_usd", 0.0))
        ambiguous_inflight.append(
            {
                "record_directory": reservation_path.parent.name,
                "request_id": reservation.get("request_id"),
                "attempt_number": reservation.get("attempt_number"),
                "maximum_reserved_cost_usd": reservation.get(
                    "maximum_reserved_cost_usd", 0.0
                ),
                "billing_status": "UNKNOWN_COUNTED_AT_RESERVED_MAXIMUM",
            }
        )

    for name, rows in groups.items():
        _write_jsonl(output_dir / f"{name}.jsonl", rows)
    handoff_path = output_dir / "clear_include_handoff.csv"
    with handoff_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "canonical_id",
                "title",
                "doi",
                "source_url",
                "reasons",
                "e6_status",
            ],
        )
        writer.writeheader()
        for result in sorted(groups["include"], key=lambda item: item["batch_order"]):
            source = by_id[result["canonical_id"]]
            writer.writerow(
                {
                    "canonical_id": result["canonical_id"],
                    "title": result["title"],
                    "doi": source.get("doi") or "",
                    "source_url": source.get("source_url") or "",
                    "reasons": result["judgment"]["overall_rationale"],
                    "e6_status": E6_STATUS,
                }
            )
    report = {
        **run_binding,
        "status": (
            "STOPPED_SYSTEMATIC_VALIDATION_FAILURE"
            if stopped_reason
            else (
                "COMPLETE"
                if not groups["unprocessed"] and not groups["failed"]
                else "PARTIAL"
            )
        ),
        "stop_reason": stopped_reason,
        "smoke_test_passed": smoke_passed,
        "counts": {name: len(rows) for name, rows in groups.items()},
        "usage": total_usage,
        "estimated_billed_cost_usd": total_cost,
        "cost_accounting": {
            "ambiguous_inflight_requests": ambiguous_inflight,
            "ambiguous_inflight_counted_at_reserved_maximum": len(ambiguous_inflight),
            "actual_invoice_may_be_lower": bool(ambiguous_inflight),
        },
        "hard_spending_cap_usd": budget_cap,
        "aggregate_request_latency_seconds": total_latency,
        "submitted_request_total": submitted_request_total,
        "completed_attempt_total": completed_attempt_total,
        "retry_total": retry_total,
        "observed_provider_service_tiers": provider_service_tiers,
        "observed_provider_models": provider_models,
        "operational_readiness": (
            "READY_FOR_LARGER_BOUNDED_BATCH"
            if (
                    not stopped_reason
                    and smoke_passed
                    and not groups["failed"]
                    and not groups["unprocessed"]
            )
            else "NOT_YET_READY"
        ),
        "accuracy_claim": "NOT_ASSESSED_BY_THIS_OPERATIONAL_RUN",
    }
    _write_json(output_dir / "run_report.json", report)
    _write_run_report_markdown(output_dir / "run_report.md", report)
    _write_package_manifest(output_dir, report)
    return report


def finalize_stopped_run(
        *,
        batch_path: Path,
        output_dir: Path,
        stopped_reason: str,
) -> dict[str, Any]:
    """Finalize persisted attempts after an intentional stop without making provider calls."""

    records = list(load_jsonl(batch_path))
    validate_batch(records)
    run_binding = json.loads((output_dir / "run_binding.json").read_text(encoding="utf-8"))
    results_dir = output_dir / "records"
    prompt_hash = run_binding["prompt"]["sha256"]
    for record in records:
        record_dir = results_dir / record["canonical_id"].replace(":", "_")
        if not record_dir.exists() or (record_dir / "result.json").exists():
            continue
        attempts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(record_dir.glob("attempt-[0-9][0-9][0-9].json"))
        ]
        reservations = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(record_dir.glob("attempt-*.reservation.json"))
        ]
        input_hash = (
            reservations[-1].get("input_hash")
            if reservations
            else _json_hash(
                {
                    "canonical_record_id": record["canonical_id"],
                    "title": record["title"],
                    "abstract": record["abstract"],
                }
            )
        )
        valid_attempts = [attempt for attempt in attempts if attempt.get("status") == "VALID"]
        if valid_attempts:
            payload = json.loads(valid_attempts[-1]["raw_response"])
            validated = validate_model_payload(payload, record)
            result = _validated_result(
                record, input_hash, prompt_hash, validated, attempts
            )
        else:
            result = _failed_result(
                record,
                input_hash,
                prompt_hash,
                attempts,
                "INTERRUPTED_SYSTEMATIC_VALIDATION_STOP",
            )
            result["ambiguous_inflight_request"] = bool(
                reservations and len(reservations) > len(attempts)
            )
        _create_json(record_dir / "result.json", result)

    return _write_package_outputs(
        records=records,
        output_dir=output_dir,
        run_binding=run_binding,
        smoke_passed=True,
        budget_cap=float(run_binding["controls"]["hard_spending_cap_usd"]),
        stopped_reason=stopped_reason,
    )


def _write_run_report_markdown(path: Path, report: Mapping[str, Any]) -> None:
    counts = report["counts"]
    usage = report["usage"]
    text = f"""# Fast-track batch run report

Status: `{report['status']}`

- Model: `{report['model']}`; reasoning `{report['parameters']['reasoning_effort']}`; service tier `{report['parameters']['service_tier']}`.
- Stop reason: {report.get('stop_reason') or 'none'}.
- Validated clear INCLUDE handoff: {counts['include']}.
- Validated DEFER: {counts['deferred']}.
- Validated EXCLUDED: {counts['excluded']}.
- Failed: {counts['failed']}.
- Unprocessed: {counts['unprocessed']}.
- Input tokens: {usage['input_tokens']} ({usage['cached_input_tokens']} cached; {usage['cache_write_tokens']} cache-write).
- Output tokens: {usage['output_tokens']} ({usage['reasoning_tokens']} reasoning).
- Estimated billed cost: USD {report['estimated_billed_cost_usd']:.6f} of the USD {report['hard_spending_cap_usd']:.2f} hard cap.
- Submitted requests: {report['submitted_request_total']} ({report['retry_total']} retries); completed attempt responses: {report['completed_attempt_total']}.
- Aggregate request latency: {report['aggregate_request_latency_seconds']:.3f} seconds.
- Ambiguous in-flight requests counted at reserved maximum: {report['cost_accounting']['ambiguous_inflight_counted_at_reserved_maximum']}.
- Operational readiness: `{report['operational_readiness']}`. This run does not assess screening accuracy.

The stopped run is staging-only. Failed calls are not counted as screened. No larger batch was launched.
"""
    path.write_text(text, encoding="utf-8", newline="\n")


def _write_package_manifest(output_dir: Path, report: Mapping[str, Any]) -> None:
    artifact_names = [
        "run_binding.json",
        "run_report.json",
        "run_report.md",
        "clear_include_handoff.csv",
        "include.jsonl",
        "deferred.jsonl",
        "excluded.jsonl",
        "failed.jsonl",
        "unprocessed.jsonl",
    ]
    artifacts = {
        name: {"sha256": sha256_file(output_dir / name), "bytes": (output_dir / name).stat().st_size}
        for name in artifact_names
    }
    record_files = sorted(
        path for path in (output_dir / "records").glob("**/*") if path.is_file()
    )
    aggregate = hashlib.sha256()
    for path in record_files:
        relative = path.relative_to(output_dir).as_posix()
        aggregate.update(relative.encode("utf-8"))
        aggregate.update(b"\x00")
        aggregate.update(sha256_file(path).encode("ascii"))
        aggregate.update(b"\n")
    manifest = {
        "artifact_class": "title_abstract_fast_track_batch_staging_package",
        "status": report["status"],
        "model": report["model"],
        "amendment_version": report.get("amendment_version", AMENDMENT_VERSION),
        "counts": report["counts"],
        "artifacts": artifacts,
        "finalization_implementation": {
            "path": __file__,
            "sha256": sha256_file(Path(__file__)),
        },
        "per_record_file_count": len(record_files),
        "per_record_files_aggregate_sha256": aggregate.hexdigest(),
        "staging_only": True,
    }
    _write_json(output_dir / "package_manifest.json", manifest)


def _stable_id(*parts: str) -> str:
    return "request:" + hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]


def _json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _create_or_match_json(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8")) != value:
            raise ValueError(f"resume binding mismatch: {path}")
        return
    _create_json(path, value)


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


def _write_jsonl(path: Path, values: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for value in sorted(values, key=lambda item: item["batch_order"]):
            handle.write(json.dumps(value, sort_keys=True, ensure_ascii=True) + "\n")
