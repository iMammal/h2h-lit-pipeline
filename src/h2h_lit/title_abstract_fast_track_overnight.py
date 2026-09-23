"""Dedicated staging-only coordinator for the authorized overnight fast-track batch."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import signal
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from h2h_lit.inference import InferenceProvider
from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.title_abstract_fast_track_batch import (
    Pricing,
    RunSettings,
    _maximum_attempt_cost,
    _normalize_usage,
    _usage_cost,
    load_jsonl,
    sha256_file,
    validate_batch,
)
from h2h_lit.title_abstract_fast_track_repair import (
    OUTPUT_SCHEMA_VERSION,
    PROMPT_VERSION,
    _e7_yes_needs_consistency_warning,
    evidence_unit_response_schema,
    inference_input_for,
    validate_evidence_unit_payload,
)
from h2h_lit.title_abstract_screening import E6_STATUS

OVERNIGHT_RUNNER_VERSION = "1.0.0"
_USAGE_KEYS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_tokens",
    "output_tokens",
    "reasoning_tokens",
)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def stable_id(*parts: str) -> str:
    return "request:" + hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]


def create_json(path: Path, value: Any) -> None:
    payload = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)


def atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    os.replace(temporary, path)


class Budget:
    def __init__(self, cap: float, initial_actual: float):
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
                raise RuntimeError("provider usage exceeded conservatively reserved cumulative budget")

    def snapshot(self) -> dict[str, float]:
        with self.lock:
            return {"actual": self.actual, "reserved": self.reserved, "cap": self.cap}


class RuntimeWindow:
    def __init__(self, output_dir: Path, hours: float, drain_seconds: float):
        self.output_dir = output_dir
        self.limit_seconds = hours * 3600.0
        self.drain_seconds = drain_seconds
        self.started_monotonic: float | None = None
        self.started_at_utc: str | None = None
        self.lock = threading.Lock()

    def mark_first_request(self) -> None:
        with self.lock:
            if self.started_monotonic is not None:
                return
            self.started_monotonic = time.monotonic()
            self.started_at_utc = utc_now()
            create_json(
                self.output_dir / "runtime_window.json",
                {
                    "first_new_paid_request_at_utc": self.started_at_utc,
                    "runtime_limit_seconds": self.limit_seconds,
                    "drain_reserve_seconds": self.drain_seconds,
                },
            )

    def elapsed(self) -> float:
        with self.lock:
            return 0.0 if self.started_monotonic is None else time.monotonic() - self.started_monotonic

    def allow_new_request(self) -> bool:
        with self.lock:
            return self.started_monotonic is None or (
                time.monotonic() - self.started_monotonic
                < self.limit_seconds - self.drain_seconds
            )


class Progress:
    def __init__(
        self,
        output_dir: Path,
        records: list[dict[str, Any]],
        prior_report: Mapping[str, Any],
        initial_cost: float,
        initial_usage: Mapping[str, int],
    ):
        self.output_dir = output_dir
        self.records = records
        self.by_id = {row["canonical_id"]: row for row in records}
        self.prior_report = prior_report
        self.initial_cost = initial_cost
        self.initial_usage = {key: int(initial_usage.get(key, 0)) for key in _USAGE_KEYS}
        self.counts = {name: 0 for name in ("include", "deferred", "excluded", "failed", "ambiguous")}
        self.usage = {key: 0 for key in _USAGE_KEYS}
        self.cost = 0.0
        self.latency = 0.0
        self.requests = 0
        self.retries = 0
        self.warnings = 0
        self.completed_ids: set[str] = set()
        self.lock = threading.Lock()
        self.events_path = output_dir / "completion_events.jsonl"
        self.include_path = output_dir / "clear_include_handoff.csv"
        if not self.include_path.exists():
            with self.include_path.open("x", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=[
                    "batch_order", "canonical_id", "title", "doi", "source_url",
                    "abstract_status", "supporting_criterion_evidence", "overall_rationale",
                    "e6_status", "status",
                ])
                writer.writeheader()

    def add(self, result: Mapping[str, Any], *, persist_event: bool = True) -> None:
        record_id = str(result["canonical_id"])
        with self.lock:
            if record_id in self.completed_ids:
                return
            self.completed_ids.add(record_id)
            group = _result_group(result)
            if group != "unprocessed":
                self.counts[group] += 1
            for attempt in result.get("attempts", []):
                self.requests += 1
                self.retries += int(int(attempt.get("attempt_number", 1)) > 1)
                self.cost += float(attempt.get("estimated_billed_cost_usd", 0.0))
                self.latency += float(attempt.get("latency_seconds", 0.0))
                for key in _USAGE_KEYS:
                    self.usage[key] += int(attempt.get("usage", {}).get(key, 0))
            if result.get("e7_consistency_warning"):
                self.warnings += 1
            if persist_event:
                with self.events_path.open("a", encoding="utf-8", newline="\n") as handle:
                    handle.write(json.dumps({
                        "completed_at_utc": utc_now(),
                        "batch_order": result.get("batch_order"),
                        "canonical_id": record_id,
                        "status": result.get("status"),
                        "group": group,
                        "e7_consistency_warning": bool(result.get("e7_consistency_warning")),
                    }, sort_keys=True) + "\n")
            if group == "include" and persist_event:
                source = self.by_id[record_id]
                judgment = result["judgment"]
                with self.include_path.open("a", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=[
                        "batch_order", "canonical_id", "title", "doi", "source_url",
                        "abstract_status", "supporting_criterion_evidence", "overall_rationale",
                        "e6_status", "status",
                    ])
                    writer.writerow({
                        "batch_order": result["batch_order"],
                        "canonical_id": record_id,
                        "title": result["title"],
                        "doi": source.get("doi") or "",
                        "source_url": source.get("source_url") or "",
                        "abstract_status": "MISSING" if result["abstract_missing"] else "PRESENT",
                        "supporting_criterion_evidence": json.dumps(judgment["criteria"], sort_keys=True),
                        "overall_rationale": judgment["overall_rationale"],
                        "e6_status": E6_STATUS,
                        "status": "MODEL_NOMINATED_REQUIRES_FULL_REPORT_ASSESSMENT",
                    })

    def snapshot(self, budget: Budget, runtime: RuntimeWindow, stop_reason: str | None) -> dict[str, Any]:
        with self.lock:
            budget_state = budget.snapshot()
            processed = len(self.completed_ids)
            return {
                "updated_at_utc": utc_now(),
                "status": "RUNNING" if stop_reason is None else "STOPPING",
                "stop_reason": stop_reason,
                "batch_records": len(self.records),
                "completed_records": processed,
                "unprocessed_records": len(self.records) - processed,
                "counts": dict(self.counts),
                "new_run_usage": dict(self.usage),
                "cumulative_usage": {
                    key: self.initial_usage[key] + self.usage[key] for key in _USAGE_KEYS
                },
                "new_run_estimated_cost_usd": self.cost,
                "cumulative_conservative_cost_usd": budget_state["actual"],
                "current_inflight_reserved_cost_usd": budget_state["reserved"],
                "hard_cumulative_spending_cap_usd": budget_state["cap"],
                "requests": self.requests,
                "retries": self.retries,
                "aggregate_latency_seconds": self.latency,
                "e7_consistency_warnings": self.warnings,
                "elapsed_seconds_from_first_new_request": runtime.elapsed(),
                "throughput_records_per_hour": (
                    processed / runtime.elapsed() * 3600.0 if runtime.elapsed() > 0 else 0.0
                ),
                "historical_repair_counts": self.prior_report["counts"],
            }


def _result_group(result: Mapping[str, Any]) -> str:
    if result.get("status") == "AMBIGUOUS":
        return "ambiguous"
    if result.get("status") != "VALIDATED":
        return "unprocessed" if str(result.get("status", "")).startswith("UNPROCESSED") else "failed"
    return {
        "ADVANCE_TO_FULL_REPORT_ASSESSMENT": "include",
        "DEFER": "deferred",
        "EXCLUDE": "excluded",
    }[result["judgment"]["operational_disposition"]]


def _error_category(result: Mapping[str, Any]) -> str | None:
    for attempt in reversed(result.get("attempts", [])):
        metadata = attempt.get("provider_metadata", {})
        status = metadata.get("provider_error_status")
        text = " ".join(str(value) for value in (
            attempt.get("validation_error"), metadata.get("provider_error"),
            metadata.get("provider_error_response"),
        ) if value).casefold()
        if status in {401, 403} or "authentication" in text or "invalid api key" in text:
            return "AUTHENTICATION"
        if status == 402 or "billing" in text or "insufficient_quota" in text:
            return "BILLING"
        if status == 429 or "rate limit" in text:
            return "RATE_LIMIT"
        if status in {507, 529} or "storage" in text:
            return "STORAGE"
    return None


def _screen_record(
    record: dict[str, Any],
    records_dir: Path,
    prompt: str,
    prompt_hash: str,
    settings: RunSettings,
    provider: InferenceProvider,
    budget: Budget,
    runtime: RuntimeWindow,
) -> dict[str, Any]:
    record_dir = records_dir / record["canonical_id"].replace(":", "_")
    record_dir.mkdir(exist_ok=True)
    final_path = record_dir / "result.json"
    if final_path.exists():
        return json.loads(final_path.read_text(encoding="utf-8"))
    input_snapshot = inference_input_for(record)
    input_hash = json_hash(input_snapshot)
    parameters = {
        "max_output_tokens": settings.max_output_tokens,
        "reasoning_effort": settings.reasoning_effort,
        "response_schema_version": OUTPUT_SCHEMA_VERSION,
        "service_tier": settings.service_tier,
        "store": False,
        "structured_output": "title_abstract_fast_track_overnight_v2_1_2",
        "verbosity": settings.verbosity,
    }
    attempts: list[dict[str, Any]] = []
    for attempt_number in range(1, settings.retry_limit + 2):
        reservation_path = record_dir / f"attempt-{attempt_number:03d}.reservation.json"
        attempt_path = record_dir / f"attempt-{attempt_number:03d}.json"
        if reservation_path.exists() and not attempt_path.exists():
            result = {
                "status": "AMBIGUOUS", "failure_reason": "OUTSTANDING_RESERVATION_NOT_RESUBMITTED",
                "canonical_id": record["canonical_id"], "batch_order": record["batch_order"],
                "title": record["title"], "attempts": attempts,
            }
            create_json(final_path, result)
            return result
        if not runtime.allow_new_request():
            status = "UNPROCESSED_RUNTIME_STOP" if not attempts else "FAILED"
            result = {
                "status": status, "failure_reason": "RUNTIME_DRAIN_STOP_BEFORE_REQUEST",
                "canonical_id": record["canonical_id"], "batch_order": record["batch_order"],
                "title": record["title"], "attempts": attempts,
            }
            create_json(final_path, result)
            return result
        maximum = _maximum_attempt_cost(prompt, input_snapshot, settings)
        if not budget.reserve(maximum):
            status = "UNPROCESSED_BUDGET_STOP" if not attempts else "FAILED"
            result = {
                "status": status, "failure_reason": "CUMULATIVE_BUDGET_STOP_BEFORE_REQUEST",
                "canonical_id": record["canonical_id"], "batch_order": record["batch_order"],
                "title": record["title"], "attempts": attempts,
            }
            create_json(final_path, result)
            return result
        runtime.mark_first_request()
        request_id = stable_id("overnight-v1", record["canonical_id"], input_hash, prompt_hash, str(attempt_number))
        create_json(reservation_path, {
            "request_id": request_id, "attempt_number": attempt_number,
            "maximum_reserved_cost_usd": maximum, "input_hash": input_hash,
        })
        started = time.monotonic()
        raw_response = ""
        metadata: dict[str, Any] = {}
        validated = None
        error = None
        try:
            raw_response = provider.generate(
                model=settings.model, prompt=prompt, input_snapshot=input_snapshot,
                parameters=parameters, request_id=request_id, attempt_number=attempt_number,
            )
            validated = validate_evidence_unit_payload(json.loads(raw_response), record)
        except Exception as exc:  # noqa: BLE001 - persisted as run evidence
            error = f"{type(exc).__name__}: {exc}"
        try:
            if isinstance(provider, OpenAIResponsesProvider) or hasattr(provider, "metadata_for"):
                metadata = dict(provider.metadata_for(request_id, attempt_number))
        except Exception as exc:  # noqa: BLE001
            error = error or f"metadata error: {type(exc).__name__}: {exc}"
            validated = None
        try:
            usage = _normalize_usage(metadata.get("provider_usage"))
        except (TypeError, ValueError) as exc:
            usage = {key: 0 for key in _USAGE_KEYS}
            error = error or f"invalid provider usage: {exc}"
            validated = None
        actual = _usage_cost(usage, settings.pricing) if any(usage.values()) else maximum
        budget.settle(maximum, actual)
        attempt = {
            "request_id": request_id, "attempt_number": attempt_number,
            "status": "VALID" if validated is not None else "INVALID",
            "latency_seconds": max(0.0, time.monotonic() - started),
            "usage": usage, "estimated_billed_cost_usd": actual,
            "raw_response": raw_response, "validation_error": error,
            "provider_metadata": metadata,
        }
        create_json(attempt_path, attempt)
        attempts.append(attempt)
        if validated is not None:
            warning = _e7_yes_needs_consistency_warning(validated["responses"])
            result = {
                "status": "VALIDATED", "canonical_id": record["canonical_id"],
                "batch_order": record["batch_order"], "title": record["title"],
                "abstract_missing": not bool(str(record["abstract"]).strip()),
                "doi": record.get("doi"), "source_url": record.get("source_url"),
                "source_database": record.get("source_database"),
                "source_identifier": record.get("source_identifier"),
                "input_hash": input_hash, "prompt_hash": prompt_hash,
                "output_schema_version": OUTPUT_SCHEMA_VERSION, "judgment": validated,
                "e7_consistency_warning": warning, "attempts": attempts,
            }
            create_json(final_path, result)
            return result
        category = _error_category({"attempts": attempts})
        if category in {"AUTHENTICATION", "BILLING"}:
            break
    result = {
        "status": "FAILED", "failure_reason": "RETRIES_EXHAUSTED",
        "canonical_id": record["canonical_id"], "batch_order": record["batch_order"],
        "title": record["title"], "input_hash": input_hash,
        "prompt_hash": prompt_hash, "attempts": attempts,
    }
    create_json(final_path, result)
    return result


def _load_existing_results(records_dir: Path) -> dict[str, dict[str, Any]]:
    results = {}
    for path in records_dir.glob("*/result.json"):
        value = json.loads(path.read_text(encoding="utf-8"))
        record_id = value.get("canonical_id")
        if not isinstance(record_id, str) or record_id in results:
            raise ValueError("invalid or duplicate persisted overnight result")
        results[record_id] = value
    return results


def _append_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")


def run_overnight(
    *,
    batch_path: Path,
    batch_manifest_path: Path,
    prompt_path: Path,
    protocol_path: Path,
    amendment_path: Path,
    identity_overlay_path: Path,
    prior_repair_dir: Path,
    output_dir: Path,
    settings: RunSettings,
    provider: InferenceProvider,
    runtime_hours: float = 10.0,
    drain_seconds: float = 300.0,
    resume: bool = False,
) -> dict[str, Any]:
    settings.validate()
    if settings.smoke_count != 20 or settings.concurrency < 1 or settings.concurrency > 8:
        raise ValueError("overnight run requires smoke_count=20 and concurrency between 1 and 8")
    records = list(load_jsonl(batch_path))
    if len(records) > 25000:
        raise ValueError("authorized new-record limit exceeded")
    validate_batch(records, expected_count=len(records))
    manifest = json.loads(batch_manifest_path.read_text(encoding="utf-8"))
    if manifest["batch"]["sha256"] != sha256_file(batch_path) or manifest["batch"]["records"] != len(records):
        raise ValueError("overnight batch binding mismatch")
    if manifest["identity_overlay"]["sha256"] != sha256_file(identity_overlay_path):
        raise ValueError("identity overlay binding mismatch")
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    amendment = json.loads(amendment_path.read_text(encoding="utf-8"))
    if protocol.get("protocol_version") != "2.0.0" or amendment.get("amendment_version") != "2.1.0":
        raise ValueError("protocol/amendment binding mismatch")
    prompt = prompt_path.read_text(encoding="utf-8")
    if f"Prompt-Version: {PROMPT_VERSION}" not in prompt:
        raise ValueError("unexpected evidence-ID prompt version")
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    prior_manifest_path = prior_repair_dir / "package_manifest.json"
    prior_report_path = prior_repair_dir / "combined_coverage_report.json"
    if manifest["prior_screening"]["repair_package_manifest"]["sha256"] != sha256_file(prior_manifest_path):
        raise ValueError("prior repair package binding mismatch")
    if manifest["prior_screening"]["repair_report"]["sha256"] != sha256_file(prior_report_path):
        raise ValueError("prior repair report binding mismatch")
    prior_report = json.loads(prior_report_path.read_text(encoding="utf-8"))
    initial_cost = float(prior_report["cumulative_conservative_cost_usd"])
    initial_usage = prior_report["cumulative_usage"]
    if initial_cost > settings.hard_spending_cap_usd:
        raise ValueError("persisted cumulative cost already exceeds the authorized cap")

    if output_dir.exists() and not resume:
        raise FileExistsError("overnight output directory exists; explicit resume required")
    output_dir.mkdir(parents=True, exist_ok=True)
    records_dir = output_dir / "records"
    records_dir.mkdir(exist_ok=True)
    lock_handle = (output_dir / "budget_owner.lock").open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError("another worker owns the overnight spending ledger") from exc

    implementation_path = Path(__file__)
    binding = {
        "artifact_class": "title_abstract_fast_track_overnight_run",
        "runner_version": OVERNIGHT_RUNNER_VERSION,
        "status": "RUNNING",
        "created_at_utc": utc_now(),
        "batch": {"path": str(batch_path), "sha256": sha256_file(batch_path), "records": len(records)},
        "batch_manifest": {"path": str(batch_manifest_path), "sha256": sha256_file(batch_manifest_path)},
        "registered_corpus": manifest["frame"],
        "identity_overlay": manifest["identity_overlay"],
        "protocol": {"path": str(protocol_path), "sha256": sha256_file(protocol_path), "version": "2.0.0"},
        "amendment": {"path": str(amendment_path), "sha256": sha256_file(amendment_path), "version": "2.1.0"},
        "prompt": {"path": str(prompt_path), "sha256": prompt_hash, "version": PROMPT_VERSION},
        "schema": {"version": OUTPUT_SCHEMA_VERSION, "sha256": json_hash(evidence_unit_response_schema())},
        "implementation": {"path": str(implementation_path), "sha256": sha256_file(implementation_path)},
        "prior_repair": {"path": str(prior_repair_dir), "manifest_sha256": sha256_file(prior_manifest_path), "report_sha256": sha256_file(prior_report_path)},
        "provider": settings.provider_name, "model": settings.model,
        "parameters": {"reasoning_effort": settings.reasoning_effort, "service_tier": settings.service_tier, "max_output_tokens": settings.max_output_tokens, "verbosity": settings.verbosity, "store": False, "tools": []},
        "pricing_usd_per_million_tokens": {"input": settings.pricing.input_usd_per_million, "cached_input": settings.pricing.cached_input_usd_per_million, "cache_write": settings.pricing.cache_write_usd_per_million, "output": settings.pricing.output_usd_per_million},
        "controls": {"hard_cumulative_spending_cap_usd": settings.hard_spending_cap_usd, "starting_conservative_cost_usd": initial_cost, "runtime_hours": runtime_hours, "drain_seconds": drain_seconds, "new_record_limit": len(records), "smoke_count": settings.smoke_count, "concurrency": settings.concurrency, "retry_limit": settings.retry_limit, "timeout_seconds": settings.timeout_seconds, "rolling_failure_gate": "3 retry-exhausted records in 20 completions"},
        "model_input_policy": "canonical_id_and_deterministic_title_abstract_evidence_units_only",
        "staging_only": True,
    }
    binding_path = output_dir / "run_binding.json"
    if binding_path.exists():
        existing_binding = json.loads(binding_path.read_text(encoding="utf-8"))
        for key in ("batch", "prompt", "schema", "model", "parameters", "pricing_usd_per_million_tokens", "controls"):
            if existing_binding.get(key) != binding.get(key):
                raise ValueError(f"resume binding mismatch: {key}")
        binding = existing_binding
    else:
        create_json(binding_path, binding)

    existing = _load_existing_results(records_dir)
    current_persisted_cost = sum(
        float(attempt.get("estimated_billed_cost_usd", 0.0))
        for result in existing.values() for attempt in result.get("attempts", [])
    )
    budget = Budget(settings.hard_spending_cap_usd, initial_cost + current_persisted_cost)
    runtime = RuntimeWindow(output_dir, runtime_hours, drain_seconds)
    progress = Progress(output_dir, records, prior_report, initial_cost, initial_usage)
    for result in existing.values():
        progress.add(result, persist_event=False)

    stop_event = threading.Event()
    stop_reason: str | None = None
    rolling = deque(maxlen=20)
    persistent_categories = deque(maxlen=3)

    def request_stop(reason: str) -> None:
        nonlocal stop_reason
        if stop_reason is None:
            stop_reason = reason
            stop_event.set()
            print(json.dumps({"event": "stop_requested", "reason": reason, "at": utc_now()}), flush=True)

    def signal_stop(signum: int, _frame: Any) -> None:
        request_stop(f"SIGNAL_{signum}_GRACEFUL_DRAIN")

    previous_handlers = {
        signum: signal.signal(signum, signal_stop) for signum in (signal.SIGTERM, signal.SIGINT)
    }

    def register(result: dict[str, Any]) -> None:
        progress.add(result)
        rolling.append(result.get("status") == "FAILED" and result.get("failure_reason") == "RETRIES_EXHAUSTED")
        category = _error_category(result)
        if category in {"AUTHENTICATION", "BILLING"}:
            request_stop(f"{category}_FAILURE")
        if category in {"RATE_LIMIT", "STORAGE"}:
            persistent_categories.append(category)
            if len(persistent_categories) == 3 and len(set(persistent_categories)) == 1:
                request_stop(f"PERSISTENT_{category}_FAILURE")
        else:
            persistent_categories.clear()
        if len(rolling) == 20 and sum(rolling) >= 3:
            request_stop("ROLLING_VALIDATION_FAILURE_GATE")
        atomic_json(output_dir / "progress.json", progress.snapshot(budget, runtime, stop_reason))

    pending = [record for record in records if record["canonical_id"] not in existing]

    def run_phase(phase_records: list[dict[str, Any]], *, smoke: bool) -> list[dict[str, Any]]:
        iterator = iter(phase_records)
        futures: dict[Future[dict[str, Any]], dict[str, Any]] = {}
        completed: list[dict[str, Any]] = []
        concurrency = min(settings.concurrency, len(phase_records))
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            while True:
                while not stop_event.is_set() and len(futures) < concurrency:
                    if not runtime.allow_new_request():
                        request_stop("RUNTIME_LIMIT_DRAIN")
                        break
                    try:
                        record = next(iterator)
                    except StopIteration:
                        break
                    future = pool.submit(
                        _screen_record, record, records_dir, prompt, prompt_hash,
                        settings, provider, budget, runtime,
                    )
                    futures[future] = record
                if not futures:
                    break
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    futures.pop(future)
                    result = future.result()
                    completed.append(result)
                    register(result)
                    print(json.dumps({"event": "record_completed", "batch_order": result.get("batch_order"), "canonical_id": result.get("canonical_id"), "status": result.get("status"), "group": _result_group(result)}), flush=True)
                    if smoke and result.get("status") != "VALIDATED":
                        request_stop("SMOKE_TEST_VALIDATION_FAILURE")
        return completed

    try:
        smoke_records = pending[: settings.smoke_count]
        smoke_results = run_phase(smoke_records, smoke=True)
        smoke_passed = (
            len(smoke_results) == len(smoke_records) == settings.smoke_count
            and all(result.get("status") == "VALIDATED" for result in smoke_results)
            and stop_reason is None
        )
        if smoke_passed:
            print(json.dumps({"event": "smoke_passed", "records": settings.smoke_count}), flush=True)
            run_phase(pending[settings.smoke_count :], smoke=False)
        elif stop_reason is None:
            request_stop("SMOKE_TEST_INCOMPLETE")
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)

    final_results = _load_existing_results(records_dir)
    groups = {name: [] for name in ("include", "deferred", "excluded", "failed", "ambiguous", "unprocessed")}
    for record in records:
        result = final_results.get(record["canonical_id"])
        if result is None:
            groups["unprocessed"].append(record)
        else:
            groups[_result_group(result)].append(result)
    for name, rows in groups.items():
        _append_jsonl(output_dir / f"{name}.jsonl", rows)
    snapshot = progress.snapshot(budget, runtime, stop_reason)
    snapshot.update({
        "status": "COMPLETE" if not groups["unprocessed"] and not groups["failed"] and not groups["ambiguous"] else "STOPPED",
        "stopping_reason": stop_reason or ("BATCH_COMPLETE" if not groups["unprocessed"] else "UNKNOWN"),
        "smoke_test_passed": smoke_passed,
        "counts": {name: len(rows) for name, rows in groups.items()},
        "model_nominations_are_final_inclusions": False,
        "accuracy_assessed": False,
    })
    atomic_json(output_dir / "run_report.json", snapshot)
    report_text = (
        "# Overnight title/abstract screening report\n\n"
        f"Status: `{snapshot['status']}`\n\n"
        f"- Stop reason: `{snapshot['stopping_reason']}`.\n"
        f"- New validated INCLUDE candidates: {snapshot['counts']['include']}.\n"
        f"- New DEFER: {snapshot['counts']['deferred']}; EXCLUDED: {snapshot['counts']['excluded']}.\n"
        f"- Failed: {snapshot['counts']['failed']}; ambiguous: {snapshot['counts']['ambiguous']}; unprocessed: {snapshot['counts']['unprocessed']}.\n"
        f"- Cumulative conservative cost: USD {snapshot['cumulative_conservative_cost_usd']:.6f} of USD {snapshot['hard_cumulative_spending_cap_usd']:.2f}.\n"
        f"- INCLUDE handoff: `{progress.include_path}`.\n\n"
        "INCLUDE records are model-nominated candidates requiring full-report assessment, not final inclusions. Successful schema validation does not establish screening accuracy.\n"
    )
    (output_dir / "morning_report.md").write_text(report_text, encoding="utf-8", newline="\n")
    artifacts = {}
    for path in sorted(output_dir.iterdir()):
        if path.is_file() and path.name not in {"package_manifest.json", "budget_owner.lock"}:
            artifacts[path.name] = {"sha256": sha256_file(path), "bytes": path.stat().st_size}
    atomic_json(output_dir / "package_manifest.json", {
        "artifact_class": "title_abstract_fast_track_overnight_staging_package",
        "runner_version": OVERNIGHT_RUNNER_VERSION,
        "status": snapshot["status"], "counts": snapshot["counts"],
        "artifacts": artifacts, "staging_only": True,
    })
    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    lock_handle.close()
    return snapshot
