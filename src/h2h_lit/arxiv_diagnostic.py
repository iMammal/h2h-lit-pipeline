"""Isolated, provenance-bound arXiv request diagnostics.

This module deliberately does not use the production retrieval runner: diagnostic
responses must never create production records, occurrences, episodes, or PRISMA
counts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import CheckpointStore, atomic_write
from h2h_lit.external_retrieval_wave import (
    EXECUTION_STATE_PATH,
    EXTERNAL_SOURCE_SESSION_LOCK_PATH,
    OUTPUT_ROOT,
    _exclusive_external_source_session,
)
from h2h_lit.http import HttpClient, RequestsHttpClient
from h2h_lit.pagination import PageRequest, RateLimiter, redact_url
from h2h_lit.retrieval import RetrievalQuerySpec
from h2h_lit.sources.arxiv import API_URL, ArxivPaginator

DIAGNOSTIC_RUN_ID = "star-external-retrieval-wave-001:arXiv:diagnostic:small-page-001"
DIAGNOSTIC_RELATIVE_ROOT = "outputs/diagnostics/arxiv/arxiv-small-page-diagnostic-001"
DIAGNOSTIC_MANIFEST_NAME = "diagnostic_manifest.json"
PURPOSE = "diagnostic"
PRODUCTION_ELIGIBLE = False
MAXIMUM_REQUESTS = 2
PAGE_SIZE = 1
READ_TIMEOUT_SECONDS = 120.0
MINIMUM_REQUEST_INTERVAL_SECONDS = 3.0
CONTROL_QUERY = "all:electron"
QF01_PRODUCTION_QUERY_ID = "production:STAR-QF01-RELATIONAL-VIS:arXiv"
SORT_BY = "submittedDate"
SORT_ORDER = "ascending"

_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)(api[_-]?key|apikey|access[_-]?token|token[_-]?key|authorization|password)"
    r"(\s*[=:]\s*)([^&\s]+)"
)


class ArxivDiagnosticError(RuntimeError):
    """The isolated diagnostic cannot proceed without violating its contract."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _canonical_json(payload: Any) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _file_reference(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "byte_size": path.stat().st_size,
        "raw_sha256": _sha256(path),
    }


def _tree_snapshot(path: Path, root: Path) -> dict[str, Any]:
    files = []
    total_bytes = 0
    for item in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        size = item.stat().st_size
        total_bytes += size
        files.append(
            {
                "path": item.relative_to(root).as_posix(),
                "byte_size": size,
                "raw_sha256": _sha256(item),
            }
        )
    tree_hash = hashlib.sha256(_canonical_json(files)).hexdigest()
    return {
        "root": path.relative_to(root).as_posix(),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "tree_sha256": tree_hash,
        "files": files,
    }


def _snapshot_summary(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {key: snapshot[key] for key in ("root", "file_count", "total_bytes", "tree_sha256")}


def _changed_paths(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[str]:
    before_files = {item["path"]: item for item in before["files"]}
    after_files = {item["path"]: item for item in after["files"]}
    return sorted(
        path
        for path in before_files.keys() | after_files.keys()
        if before_files.get(path) != after_files.get(path)
    )


def _safe_fixed_path(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ArxivDiagnosticError(f"path escaped repository root: {relative}") from exc
    return candidate


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ArxivDiagnosticError(f"expected a JSON object: {path}")
    return payload


def _verify_production_is_idle(state: Mapping[str, Any]) -> None:
    sources = state.get("sources")
    if not isinstance(sources, dict):
        raise ArxivDiagnosticError("production execution state lacks source states")
    running = sorted(
        source
        for source, source_state in sources.items()
        if isinstance(source_state, dict) and source_state.get("status") == "RUNNING"
    )
    if running:
        raise ArxivDiagnosticError(
            f"production source session is active: {', '.join(running)}"
        )
    arxiv = sources.get("arXiv")
    if not isinstance(arxiv, dict):
        raise ArxivDiagnosticError("production execution state lacks arXiv state")
    if arxiv.get("active_episode_number") != 4:
        raise ArxivDiagnosticError("arXiv episode 4 is not the active production episode")
    if arxiv.get("status") != "PAUSED_PROVIDER_RATE_LIMIT":
        raise ArxivDiagnosticError("arXiv production state is not paused for provider rate limit")
    if arxiv.get("last_session_completed_at_utc") is None:
        raise ArxivDiagnosticError("arXiv production session has no completion timestamp")


def _load_qf01_binding(root: Path, state: Mapping[str, Any]) -> dict[str, Any]:
    arxiv = state["sources"]["arXiv"]
    reference = arxiv.get("checkpoint_dataset")
    if not isinstance(reference, dict):
        raise ArxivDiagnosticError("active arXiv checkpoint reference is absent")
    checkpoint_path = _safe_fixed_path(root, str(reference.get("path", "")))
    expected_root = _safe_fixed_path(root, f"{OUTPUT_ROOT}/execution/arXiv/")
    try:
        checkpoint_path.relative_to(expected_root)
    except ValueError as exc:
        raise ArxivDiagnosticError("active arXiv checkpoint escaped its production namespace") from exc
    if not checkpoint_path.is_file():
        raise ArxivDiagnosticError("active arXiv checkpoint does not exist")
    actual_reference = _file_reference(checkpoint_path, root)
    if actual_reference != reference:
        raise ArxivDiagnosticError("active arXiv checkpoint reference does not match disk")
    checkpoint = _load_json(checkpoint_path)
    runs = checkpoint.get("retrieval_runs", [])
    if len(runs) != 1 or runs[0].get("run_id") != arxiv.get("active_run_id"):
        raise ArxivDiagnosticError("active arXiv run ID does not match its checkpoint")
    matches = [
        query
        for query in checkpoint.get("source_queries", [])
        if query.get("metadata", {}).get("production_query_id") == QF01_PRODUCTION_QUERY_ID
    ]
    if len(matches) != 1:
        raise ArxivDiagnosticError("checkpoint does not contain exactly one production QF01 query")
    query = matches[0]
    if query.get("endpoint") != API_URL:
        raise ArxivDiagnosticError("checkpointed QF01 endpoint changed")
    if query.get("metadata", {}).get("request_timeout_seconds") != READ_TIMEOUT_SECONDS:
        raise ArxivDiagnosticError("checkpointed QF01 timeout changed")
    if not isinstance(query.get("query_version"), str) or not query["query_version"].strip():
        raise ArxivDiagnosticError("checkpointed QF01 query version is absent")
    return {
        "checkpoint_reference": actual_reference,
        "production_run_id": runs[0]["run_id"],
        "query_id": query["query_id"],
        "query_text": query["query_text"],
        "query_version": query.get("query_version"),
    }


def _build_request(query_text: str, query_version: str) -> PageRequest:
    spec = RetrievalQuerySpec(
        source_database="arXiv",
        query_text=query_text,
        query_version=query_version,
        limit=PAGE_SIZE,
        endpoint=API_URL,
        metadata={
            "request_timeout_seconds": READ_TIMEOUT_SECONDS,
            "sort_by": SORT_BY,
            "sort_order": SORT_ORDER,
        },
    )
    request = ArxivPaginator().build_request(spec, {"start": 0})
    expected_params = {
        "search_query": query_text,
        "start": 0,
        "max_results": PAGE_SIZE,
        "sortBy": SORT_BY,
        "sortOrder": SORT_ORDER,
    }
    if (
        request.method != "GET"
        or request.url != API_URL
        or request.params != expected_params
        or request.timeout != READ_TIMEOUT_SECONDS
        or request.headers
    ):
        raise ArxivDiagnosticError("arXiv diagnostic request construction changed")
    return request


def _relevant_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    named = {
        "content-length",
        "content-type",
        "date",
        "fastly-restarts",
        "retry-after",
        "server",
        "via",
        "x-cache",
        "x-cache-hits",
        "x-served-by",
        "x-timer",
    }
    return {
        str(key): str(value)
        for key, value in headers.items()
        if str(key).lower() in named or "ratelimit" in str(key).lower()
    }


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    lowered = name.lower()
    for key, value in headers.items():
        if str(key).lower() == lowered:
            return str(value)
    return None


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp lacks timezone")
    return parsed.astimezone(UTC)


def _retry_after_evidence(value: str | None, ended_at: str) -> dict[str, Any]:
    if value is None:
        return {
            "header_present": False,
            "value": None,
            "earliest_retry_at_utc": None,
            "interpretation": "absent",
            "automatically_scheduled": False,
        }
    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            retry_at = retry_at.astimezone(UTC)
            return {
                "header_present": True,
                "value": value,
                "earliest_retry_at_utc": retry_at.isoformat().replace("+00:00", "Z"),
                "interpretation": "http-date",
                "automatically_scheduled": False,
            }
        except (TypeError, ValueError, OverflowError):
            return {
                "header_present": True,
                "value": value,
                "earliest_retry_at_utc": None,
                "interpretation": "unparseable",
                "automatically_scheduled": False,
            }
    retry_at = _parse_utc(ended_at) + timedelta(seconds=max(0.0, seconds))
    return {
        "header_present": True,
        "value": value,
        "earliest_retry_at_utc": retry_at.isoformat().replace("+00:00", "Z"),
        "interpretation": "delay-seconds",
        "automatically_scheduled": False,
    }


def _sanitize_exception(exc: Exception) -> str:
    message = _SENSITIVE_ASSIGNMENT.sub(r"\1\2<redacted>", str(exc))
    message = re.sub(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@", r"\1<redacted>@", message)
    return f"{type(exc).__name__}: {message}"


def _save_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    atomic_write(path, _canonical_json(manifest))


def _interpret(attempts: list[dict[str, Any]]) -> dict[str, Any]:
    if len(attempts) == 1 and attempts[0].get("http_status") == 429:
        return {
            "classification": "CONTROL_RATE_LIMITED_SECOND_PROBE_NOT_ISSUED",
            "supports": (
                "the simple control request was itself rate-limited, which is consistent "
                "with an endpoint- or client-scope condition"
            ),
            "does_not_establish": (
                "the rate-limit scope or whether the production query would also fail now"
            ),
        }
    if len(attempts) < 2:
        return {
            "classification": "INCOMPLETE_DIAGNOSTIC",
            "supports": "only one probe produced evidence",
            "does_not_establish": "general versus request-specific failure",
        }
    control, production = attempts
    control_ok = control.get("http_status") is not None and 200 <= control["http_status"] < 300
    production_ok = (
        production.get("http_status") is not None
        and 200 <= production["http_status"] < 300
    )
    if control_ok and not production_ok:
        classification = "REQUEST_SPECIFIC_FAILURE_SUPPORTED"
        supports = "the endpoint accepted the control but rejected the production-derived request"
    elif control_ok and production_ok:
        classification = "PRIOR_FAILURE_NOT_REPRODUCED_AT_PAGE_SIZE_ONE"
        supports = "both page-size-one requests succeeded during this diagnostic"
    elif not control_ok and production_ok:
        classification = "TEMPORAL_OR_ORDER_EFFECT_INCONCLUSIVE"
        supports = "the later production-derived request succeeded after the control failed"
    else:
        classification = "GENERAL_OR_CLIENT_SCOPE_FAILURE_CONSISTENT"
        supports = "both distinct page-size-one requests failed"
    return {
        "classification": classification,
        "supports": supports,
        "does_not_establish": (
            "provider-wide scope, client/IP scope, historical root cause, or page-size causality"
        ),
    }


def run_arxiv_diagnostic(
    *,
    root: str | Path,
    authorize_live_diagnostic: bool,
    http: HttpClient | None = None,
    timestamp: Callable[[], str] = utc_now,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run at most two diagnostic-only arXiv requests with no retries."""

    if not authorize_live_diagnostic:
        raise ArxivDiagnosticError("live diagnostic requires explicit authorization")
    root_path = Path(root).resolve()
    production_root = _safe_fixed_path(root_path, OUTPUT_ROOT)
    state_path = _safe_fixed_path(root_path, EXECUTION_STATE_PATH)
    lock_path = _safe_fixed_path(root_path, EXTERNAL_SOURCE_SESSION_LOCK_PATH)
    diagnostic_root = _safe_fixed_path(root_path, DIAGNOSTIC_RELATIVE_ROOT)
    manifest_path = diagnostic_root / DIAGNOSTIC_MANIFEST_NAME
    if not production_root.is_dir() or not state_path.is_file():
        raise ArxivDiagnosticError("production retrieval evidence is absent")
    if not lock_path.is_file():
        raise ArxivDiagnosticError("production session lock is absent; refusing to create it")
    if diagnostic_root.exists():
        raise ArxivDiagnosticError("diagnostic namespace already exists; refusing to overwrite it")

    transport = http or RequestsHttpClient()
    limiter = RateLimiter(
        {"arXiv": MINIMUM_REQUEST_INTERVAL_SECONDS},
        clock=monotonic,
        sleep=sleep,
    )

    with _exclusive_external_source_session(root_path):
        before = _tree_snapshot(production_root, root_path)
        state = _load_json(state_path)
        _verify_production_is_idle(state)
        qf01 = _load_qf01_binding(root_path, state)
        protected_inputs = [
            _file_reference(state_path, root_path),
            qf01["checkpoint_reference"],
        ]
        for relative in (
            f"{OUTPUT_ROOT}/planned_wave.json",
            f"{OUTPUT_ROOT}/preflight.json",
            "config/star_production_query_plan_v1.json",
            "artifacts/acm_field_execution/queries.txt",
        ):
            path = _safe_fixed_path(root_path, relative)
            if path.is_file():
                protected_inputs.append(_file_reference(path, root_path))

        probes = [
            {
                "probe_id": "simple-control",
                "query_text": CONTROL_QUERY,
                "query_version": "arxiv-isolated-control-v1",
            },
            {
                "probe_id": "checkpointed-production-qf01-small-page",
                "query_text": qf01["query_text"],
                "query_version": qf01["query_version"],
                "source_query_id": qf01["query_id"],
            },
        ]
        requests = [
            (probe, _build_request(probe["query_text"], probe["query_version"]))
            for probe in probes
        ]
        started_at = timestamp()
        manifest: dict[str, Any] = {
            "schema_version": "1.0.0",
            "run_id": DIAGNOSTIC_RUN_ID,
            "purpose": PURPOSE,
            "production_eligible": PRODUCTION_ELIGIBLE,
            "production_records_created": False,
            "prisma_counted": False,
            "namespace": DIAGNOSTIC_RELATIVE_ROOT,
            "started_at_utc": started_at,
            "completed_at_utc": None,
            "status": "RUNNING",
            "request_budget": MAXIMUM_REQUESTS,
            "requests_made": 0,
            "automatic_retries": False,
            "minimum_request_interval_seconds": MINIMUM_REQUEST_INTERVAL_SECONDS,
            "client_behavior": {
                "transport": type(transport).__name__,
                "explicit_request_headers": {},
                "user_agent": None,
                "user_agent_provenance": (
                    "transport/session default; exact value is not exposed by the existing client"
                ),
            },
            "production_binding": {
                "production_run_id": qf01["production_run_id"],
                "production_query_id": QF01_PRODUCTION_QUERY_ID,
                "source_query_id": qf01["query_id"],
                "source_checkpoint": qf01["checkpoint_reference"],
            },
            "protected_inputs": protected_inputs,
            "attempts": [],
            "stop_reason": None,
            "interpretation": None,
            "production_integrity": {
                "before": _snapshot_summary(before),
                "after": None,
                "unchanged": None,
                "changed_paths": None,
            },
        }
        diagnostic_root.mkdir(parents=True, exist_ok=False)
        response_store = CheckpointStore(diagnostic_root / "evidence")
        _save_manifest(manifest_path, manifest)

        for index, (probe, request) in enumerate(requests, start=1):
            if manifest["requests_made"] >= MAXIMUM_REQUESTS:
                raise ArxivDiagnosticError("diagnostic request budget exhausted")
            pacing_delay = limiter.wait("arXiv")
            request_started_at = timestamp()
            monotonic_started = monotonic()
            evidence: dict[str, Any] = {
                "probe_index": index,
                "probe_id": probe["probe_id"],
                "started_at_utc": request_started_at,
                "ended_at_utc": None,
                "elapsed_seconds": None,
                "pacing_delay_seconds": pacing_delay,
                "automatic_retry_delay_seconds": None,
                "request": {
                    "method": request.method,
                    "endpoint": request.url,
                    "params": request.sanitized_params(),
                    "headers": request.sanitized_headers(),
                    "timeout_seconds": request.timeout,
                    "request_hash": request.request_hash(),
                    "source_query_id": probe.get("source_query_id"),
                },
                "http_status": None,
                "response_url": None,
                "actual_request_url": None,
                "response_body": None,
                "response_body_encoding": None,
                "response_body_sha256": None,
                "relevant_response_headers": {},
                "retry_after": None,
                "raw_response": None,
                "transport_exception": None,
            }
            manifest["attempts"].append(evidence)
            _save_manifest(manifest_path, manifest)
            response = None
            try:
                response = transport.get(
                    request.url,
                    params=request.params,
                    headers=request.headers or None,
                    timeout=request.timeout,
                )
            except Exception as exc:  # noqa: BLE001 - transport errors are evidence
                ended_at = timestamp()
                evidence["ended_at_utc"] = ended_at
                evidence["elapsed_seconds"] = round(monotonic() - monotonic_started, 6)
                evidence["transport_exception"] = _sanitize_exception(exc)
                evidence["retry_after"] = _retry_after_evidence(None, ended_at)
            manifest["requests_made"] += 1
            if response is not None:
                ended_at = timestamp()
                evidence["ended_at_utc"] = ended_at
                evidence["elapsed_seconds"] = round(monotonic() - monotonic_started, 6)
                relative_path, response_hash = response_store.save_response(
                    f"{DIAGNOSTIC_RUN_ID}:{probe['probe_id']}", response
                )
                body = bytes(response.content)
                evidence.update(
                    {
                        "http_status": int(response.status_code),
                        "response_url": redact_url(response.url),
                        "actual_request_url": redact_url(
                            getattr(response, "request_url", response.url)
                        ),
                        "response_body": body.decode("utf-8", errors="replace"),
                        "response_body_encoding": "utf-8-with-replacement",
                        "response_body_sha256": hashlib.sha256(body).hexdigest(),
                        "relevant_response_headers": _relevant_headers(
                            response.headers or {}
                        ),
                        "retry_after": _retry_after_evidence(
                            _header(response.headers or {}, "retry-after"), ended_at
                        ),
                        "raw_response": {
                            "path": f"evidence/{relative_path}",
                            "raw_sha256": response_hash,
                        },
                    }
                )
            _save_manifest(manifest_path, manifest)

            if index == 1 and evidence["http_status"] == 429:
                manifest["stop_reason"] = "FIRST_PROBE_HTTP_429"
                break
            if evidence["retry_after"]["header_present"]:
                manifest["stop_reason"] = "RETRY_AFTER_PRESENT_NO_AUTOMATIC_SCHEDULING"
                break

        if manifest["stop_reason"] is None:
            manifest["stop_reason"] = "DIAGNOSTIC_REQUEST_BUDGET_COMPLETE"
        manifest["interpretation"] = _interpret(manifest["attempts"])
        manifest["completed_at_utc"] = timestamp()
        manifest["status"] = "COMPLETE"

        after = _tree_snapshot(production_root, root_path)
        changed = _changed_paths(before, after)
        manifest["production_integrity"] = {
            "before": _snapshot_summary(before),
            "after": _snapshot_summary(after),
            "unchanged": not changed,
            "changed_paths": changed,
        }
        _save_manifest(manifest_path, manifest)
        if changed:
            raise ArxivDiagnosticError(
                "production artifacts changed during diagnostic: " + ", ".join(changed)
            )
        return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--authorize-live-diagnostic", action="store_true")
    args = parser.parse_args(argv)
    result = run_arxiv_diagnostic(
        root=args.root,
        authorize_live_diagnostic=args.authorize_live_diagnostic,
    )
    print(
        json.dumps(
            {
                "run_id": result["run_id"],
                "status": result["status"],
                "purpose": result["purpose"],
                "production_eligible": result["production_eligible"],
                "requests_made": result["requests_made"],
                "stop_reason": result["stop_reason"],
                "http_statuses": [item["http_status"] for item in result["attempts"]],
                "production_artifacts_unchanged": result["production_integrity"][
                    "unchanged"
                ],
                "manifest": f"{DIAGNOSTIC_RELATIVE_ROOT}/{DIAGNOSTIC_MANIFEST_NAME}",
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
