"""Live executor for the non-production STAR count/syntax-sizing plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.http import HttpClient, HttpResponse, RequestsHttpClient
from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.query_development import (
    SIZING_RUN_SCHEMA_VERSION,
    QuerySizingRun,
    SentinelCandidateMatchState,
    SentinelDiagnostic,
    SentinelDiagnosticOutcome,
    SentinelIdentityState,
    SentinelSourceIndexingState,
    SizingAttempt,
    SizingCountKind,
    SizingGateStatus,
    SizingObservation,
    SizingRunStatus,
    SizingSyntaxStatus,
    SizingTransportStatus,
    SizingWindowStatus,
    load_candidate_set,
    load_sentinel_set,
    load_sizing_run,
    save_sizing_run,
    sizing_request_hash,
)
from h2h_lit.query_sizing import canonical_json

AUTOMATED_SOURCES = {
    "PubMed",
    "EuropePMC",
    "SemanticScholar",
    "arXiv",
    "IEEEXplore",
    "CrossRef",
}
MANUAL_SOURCE = "ACMDigitalLibrary"
EXPECTED_CANDIDATE_COUNT = 62
DEFAULT_PLAN = Path("outputs/query_sizing/star-query-sizing-v0-1-run-001/dry_run.json")
DEFAULT_OUTPUT = DEFAULT_PLAN.with_name("query_sizing_run.json")
DEFAULT_REPORT = DEFAULT_PLAN.with_name("query_sizing_report.json")


class SizingPlanError(ValueError):
    """The approved dry-run plan cannot safely authorize live sizing."""


@dataclass(frozen=True, slots=True)
class ValidatedSizingPlan:
    payload: dict[str, Any]
    plan_hash: str
    candidate_specs: tuple[dict[str, Any], ...]
    diagnostic_specs: tuple[dict[str, Any], ...]


@dataclass(frozen=True, slots=True)
class ParsedEnvelope:
    count: int | None
    count_kind: SizingCountKind
    syntax_status: SizingSyntaxStatus
    translation: str | None = None
    warnings: tuple[str, ...] = ()
    gate_name: str | None = None
    gate_status: SizingGateStatus = SizingGateStatus.NOT_APPLICABLE


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def load_validated_sizing_plan(
    plan_path: str | Path,
    candidate_config: str | Path,
    sentinel_config: str | Path,
) -> ValidatedSizingPlan:
    """Validate the frozen plan and both referenced configuration hashes."""
    path = Path(plan_path)
    if not path.is_file():
        raise FileNotFoundError(f"approved sizing dry-run plan is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    claimed = payload.get("report_hash")
    actual = _hash_without_key(payload, "report_hash")
    if claimed != actual:
        raise SizingPlanError("dry-run plan report hash does not match its contents")

    candidate_set = load_candidate_set(candidate_config)
    sentinel_set = load_sentinel_set(sentinel_config)
    run = payload.get("run", {})
    expected = {
        "candidate_set_id": candidate_set.candidate_set_id,
        "candidate_set_version": candidate_set.candidate_set_version,
        "candidate_set_hash": candidate_set.candidate_set_hash(),
        "sentinel_set_id": sentinel_set.sentinel_set_id,
        "sentinel_set_version": sentinel_set.sentinel_set_version,
        "sentinel_set_hash": sentinel_set.sentinel_set_hash(),
    }
    mismatches = [key for key, value in expected.items() if run.get(key) != value]
    if mismatches:
        raise SizingPlanError(f"dry-run plan provenance mismatch: {', '.join(mismatches)}")
    if sentinel_set.candidate_set_hash != candidate_set.candidate_set_hash():
        raise SizingPlanError("sentinel set references a different candidate set")

    specs = tuple(payload.get("candidate_specifications", []))
    if len(specs) != EXPECTED_CANDIDATE_COUNT:
        raise SizingPlanError("approved dry-run plan must contain exactly 62 candidates")
    identifiers = [str(item.get("candidate_query_id")) for item in specs]
    if identifiers != list(run.get("planned_candidate_query_ids", [])):
        raise SizingPlanError("candidate plan order does not match run provenance")
    if len(identifiers) != len(set(identifiers)):
        raise SizingPlanError("candidate IDs in the dry-run plan are not unique")
    for spec in specs:
        source = spec.get("source")
        if source not in AUTOMATED_SOURCES | {MANUAL_SOURCE}:
            raise SizingPlanError(f"unsupported source in sizing plan: {source}")
        if sizing_request_hash(spec["request"]) != spec.get("request_hash"):
            raise SizingPlanError(f"request hash mismatch: {spec.get('candidate_query_id')}")
        if source == "SemanticScholar" and "/search/bulk" not in spec["request"]["url"]:
            raise SizingPlanError("Semantic Scholar sizing must remain in bulk mode")
        if "partition" in canonical_json(spec).lower():
            raise SizingPlanError("query sizing does not authorize partitioning")

    diagnostics = tuple(payload.get("sentinel_diagnostic_specifications", []))
    for item in diagnostics:
        if item.get("candidate_query_id") not in set(identifiers):
            raise SizingPlanError("sentinel diagnostic references an unplanned candidate")
        for key in ("identity_request", "match_request"):
            request = item[key]
            if sizing_request_hash(request) != item.get(f"{key}_hash"):
                raise SizingPlanError(f"sentinel {key} hash mismatch")
    return ValidatedSizingPlan(payload, actual, specs, diagnostics)


class LiveSizingExecutor:
    """Execute only requests serialized in a validated sizing dry-run plan."""

    def __init__(
        self,
        *,
        http: HttpClient,
        retry_policy: RetryPolicy | None = None,
        rate_limiter: RateLimiter | None = None,
        sleep: Callable[[float], None] = time.sleep,
        timestamp: Callable[[], str] = utc_now,
        timeout: float = 30.0,
    ) -> None:
        self.http = http
        self.retry_policy = retry_policy or RetryPolicy()
        self.rate_limiter = rate_limiter or RateLimiter()
        self.sleep = sleep
        self.timestamp = timestamp
        self.timeout = timeout

    def execute(
        self,
        plan: ValidatedSizingPlan,
        output_path: str | Path,
        report_path: str | Path,
        *,
        credentials: Mapping[str, str] | None = None,
        execute_diagnostics: bool = True,
    ) -> QuerySizingRun:
        output = Path(output_path)
        credentials = dict(credentials or {})
        run = self._load_or_create_run(plan, output)
        if run.status is SizingRunStatus.COMPLETED:
            report = build_comparison_report(run)
            atomic_write(Path(report_path), (canonical_json(report) + "\n").encode("utf-8"))
            return run
        spec_by_id = {item["candidate_query_id"]: item for item in plan.candidate_specs}
        observations = {item.candidate_query_id: item for item in run.observations}

        for candidate_id in run.planned_candidate_query_ids:
            current = observations[candidate_id]
            if _observation_terminal(current, self.retry_policy.max_attempts):
                continue
            spec = spec_by_id[candidate_id]
            if spec["source"] == MANUAL_SOURCE:
                current = replace(
                    current,
                    observed_at=self.timestamp(),
                    transport_status=SizingTransportStatus.PENDING_MANUAL,
                    response_status="PENDING_MANUAL",
                    warnings=[*current.warnings, "ACM sizing requires the approved human workflow"],
                )
            elif spec["source"] == "IEEEXplore" and not credentials.get(
                "IEEE_XPLORE_API_KEY"
            ):
                current = replace(
                    current,
                    observed_at=self.timestamp(),
                    transport_status=SizingTransportStatus.BLOCKED_CREDENTIAL,
                    response_status="BLOCKED_CREDENTIAL",
                    warnings=[*current.warnings, "credential required: IEEE_XPLORE_API_KEY"],
                )
            else:
                current = self._execute_observation(
                    spec, current, credentials=credentials, checkpoint=lambda item: self._checkpoint(
                        run, observations, item, output
                    ), pending_path=_pending_envelope_path(output, candidate_id)
                )
            observations[candidate_id] = current
            self._checkpoint(run, observations, current, output)

        run.observations = [observations[item] for item in run.planned_candidate_query_ids]
        if execute_diagnostics:
            run.sentinel_diagnostics = self._execute_diagnostics(
                plan, run, credentials, output
            )
        run.status = SizingRunStatus.COMPLETED
        run.completed_at = self.timestamp()
        save_sizing_run(output, run)
        report = build_comparison_report(run)
        atomic_write(Path(report_path), (canonical_json(report) + "\n").encode("utf-8"))
        return run

    def _load_or_create_run(
        self, plan: ValidatedSizingPlan, output: Path
    ) -> QuerySizingRun:
        plan_run = plan.payload["run"]
        if output.exists():
            run = load_sizing_run(output)
            if run.dry_run_plan_hash != plan.plan_hash:
                raise SizingPlanError("checkpoint references a different dry-run plan")
            if run.candidate_set_hash != plan_run["candidate_set_hash"]:
                raise SizingPlanError("checkpoint candidate-set hash mismatch")
            if run.sentinel_set_hash != plan_run["sentinel_set_hash"]:
                raise SizingPlanError("checkpoint sentinel-set hash mismatch")
            if run.status is SizingRunStatus.COMPLETED:
                return run
            return run

        observations = [_fresh_observation(spec, self.timestamp()) for spec in plan.candidate_specs]
        run = QuerySizingRun(
            schema_version=SIZING_RUN_SCHEMA_VERSION,
            sizing_run_id=plan_run["sizing_run_id"],
            candidate_set_id=plan_run["candidate_set_id"],
            candidate_set_version=plan_run["candidate_set_version"],
            candidate_set_hash=plan_run["candidate_set_hash"],
            sentinel_set_id=plan_run["sentinel_set_id"],
            sentinel_set_version=plan_run["sentinel_set_version"],
            sentinel_set_hash=plan_run["sentinel_set_hash"],
            dry_run_plan_hash=plan.plan_hash,
            status=SizingRunStatus.RUNNING,
            planned_candidate_query_ids=list(plan_run["planned_candidate_query_ids"]),
            created_at=plan_run["created_at"],
            started_at=self.timestamp(),
            observations=observations,
        )
        save_sizing_run(output, run)
        return run

    def _checkpoint(
        self,
        run: QuerySizingRun,
        observations: dict[str, SizingObservation],
        item: SizingObservation,
        output: Path,
    ) -> None:
        observations[item.candidate_query_id] = item
        run.observations = [observations[key] for key in run.planned_candidate_query_ids]
        save_sizing_run(output, run)

    def _execute_observation(
        self,
        spec: dict[str, Any],
        observation: SizingObservation,
        *,
        credentials: Mapping[str, str],
        checkpoint: Callable[[SizingObservation], None],
        pending_path: Path,
    ) -> SizingObservation:
        request = observation.request
        sent_request = {key: value for key, value in request.items() if key != "fallback"}
        attempt_number = len(observation.attempts) + 1
        retry_reason = _next_retry_reason(observation)
        replay = _load_pending_envelope(pending_path, observation, attempt_number)
        if replay is not None:
            return _successful_observation(
                observation,
                attempt_number,
                str(replay["started_at"]),
                str(replay["completed_at"]),
                retry_reason,
                int(replay["response_status"]),
                str(replay["response_hash"]),
                _envelope_from_dict(replay["envelope"]),
            )
        while attempt_number <= self.retry_policy.max_attempts:
            started = self.timestamp()
            try:
                self.rate_limiter.wait(observation.source)
                response = self._send(request, observation.source, credentials)
            except Exception as exc:  # noqa: BLE001 - transport implementations vary
                current = _failed_attempt(
                    observation,
                    attempt_number,
                    started,
                    self.timestamp(),
                    retry_reason,
                    f"transport_error:{type(exc).__name__}:{exc}",
                    attempt_request=sent_request,
                )
                checkpoint(current)
                observation = current
                if attempt_number >= self.retry_policy.max_attempts:
                    return observation
                self.sleep(self.retry_policy.delay(attempt_number))
                attempt_number += 1
                retry_reason = "transport_error"
                continue

            response_hash = hashlib.sha256(bytes(response.content)).hexdigest()
            status = int(response.status_code)
            if status in self.retry_policy.retry_statuses:
                current = _failed_attempt(
                    observation,
                    attempt_number,
                    started,
                    self.timestamp(),
                    retry_reason,
                    f"retryable_http_status:{status}",
                    response_status=status,
                    response_hash=response_hash,
                    attempt_request=sent_request,
                )
                checkpoint(current)
                observation = current
                if attempt_number >= self.retry_policy.max_attempts:
                    return observation
                self.sleep(
                    self.retry_policy.delay(attempt_number, response.headers.get("Retry-After"))
                )
                attempt_number += 1
                retry_reason = f"retryable_http_status:{status}"
                continue
            if status < 200 or status >= 300:
                return _terminal_http_failure(
                    observation,
                    attempt_number,
                    started,
                    self.timestamp(),
                    retry_reason,
                    status,
                    response_hash,
                    attempt_request=sent_request,
                )

            try:
                envelope = _parse_envelope(observation.source, response)
                if observation.source == "CrossRef" and envelope.count is None:
                    fallback = request.get("fallback")
                    if fallback is not None:
                        current = _failed_attempt(
                            observation,
                            attempt_number,
                            started,
                            self.timestamp(),
                            retry_reason,
                            "rows_0_unsupported_or_total_results_missing",
                            response_status=status,
                            response_hash=response_hash,
                            attempt_request=sent_request,
                        )
                        checkpoint(current)
                        observation = current
                        attempt_number += 1
                        retry_reason = "approved_rows_1_fallback"
                        started = self.timestamp()
                        fallback_response = self._send(fallback, observation.source, credentials)
                        response_hash = hashlib.sha256(bytes(fallback_response.content)).hexdigest()
                        if not 200 <= int(fallback_response.status_code) < 300:
                            return _terminal_http_failure(
                                observation,
                                attempt_number,
                                started,
                                self.timestamp(),
                                retry_reason,
                                int(fallback_response.status_code),
                                response_hash,
                                attempt_request=fallback,
                            )
                        envelope = _parse_envelope(observation.source, fallback_response)
                        status = int(fallback_response.status_code)
            except Exception as exc:  # noqa: BLE001 - malformed source envelopes are terminal
                return _parse_failure(
                    observation,
                    attempt_number,
                    started,
                    self.timestamp(),
                    retry_reason,
                    status,
                    response_hash,
                    exc,
                    attempt_request=sent_request,
                )
            completed = self.timestamp()
            _save_pending_envelope(
                pending_path,
                observation,
                attempt_number,
                started,
                completed,
                status,
                response_hash,
                envelope,
            )
            return _successful_observation(
                observation,
                attempt_number,
                started,
                completed,
                retry_reason,
                status,
                response_hash,
                envelope,
                attempt_request=(
                    request.get("fallback")
                    if retry_reason == "approved_rows_1_fallback"
                    else sent_request
                ),
            )
        return observation

    def _send(
        self,
        request: dict[str, Any],
        source: str,
        credentials: Mapping[str, str],
    ) -> HttpResponse:
        params = dict(request.get("params", {}))
        headers: dict[str, str] = {}
        if source == "IEEEXplore":
            params["apikey"] = credentials["IEEE_XPLORE_API_KEY"]
        elif source == "SemanticScholar" and credentials.get("SEMANTIC_SCHOLAR_API_KEY"):
            headers["x-api-key"] = credentials["SEMANTIC_SCHOLAR_API_KEY"]
        return self.http.get(
            request["url"], params=params, headers=headers, timeout=self.timeout
        )

    def _execute_diagnostics(
        self,
        plan: ValidatedSizingPlan,
        run: QuerySizingRun,
        credentials: Mapping[str, str],
        output: Path,
    ) -> list[SentinelDiagnostic]:
        completed = {
            (item.sentinel_id, item.source, item.candidate_query_id): item
            for item in run.sentinel_diagnostics
        }
        for spec in plan.diagnostic_specs:
            key = (spec["sentinel_id"], spec["source"], spec["candidate_query_id"])
            if key in completed:
                continue
            diagnostic = self._execute_diagnostic(spec, credentials)
            completed[key] = diagnostic
            run.sentinel_diagnostics = list(completed.values())
            save_sizing_run(output, run)
        return list(completed.values())

    def _execute_diagnostic(
        self, spec: dict[str, Any], credentials: Mapping[str, str]
    ) -> SentinelDiagnostic:
        source = spec["source"]
        if source == MANUAL_SOURCE:
            return _unsupported_diagnostic(spec, "ACM diagnostics require human execution")
        if source == "IEEEXplore" and not credentials.get("IEEE_XPLORE_API_KEY"):
            return _unsupported_diagnostic(spec, "credential required: IEEE_XPLORE_API_KEY")
        if not spec.get("doi"):
            return SentinelDiagnostic(
                sentinel_id=spec["sentinel_id"], source=source,
                candidate_query_id=spec["candidate_query_id"],
                outcome=SentinelDiagnosticOutcome.IDENTITY_UNRESOLVED,
                identity_state=SentinelIdentityState.UNRESOLVED,
                source_indexing_state=SentinelSourceIndexingState.UNKNOWN,
                candidate_match_state=SentinelCandidateMatchState.UNTESTED,
                warnings=["no stable DOI/native identifier was frozen for this sentinel"],
            )
        identity = spec["identity_request"]
        try:
            identity_response = self._send(identity, source, credentials)
            identity_hash = hashlib.sha256(bytes(identity_response.content)).hexdigest()
            identity_ids = _parse_identifiers(source, identity_response)
        except Exception as exc:  # noqa: BLE001
            return _unsupported_diagnostic(spec, f"identity probe failed:{type(exc).__name__}")
        if not identity_ids:
            return SentinelDiagnostic(
                sentinel_id=spec["sentinel_id"], source=source,
                candidate_query_id=spec["candidate_query_id"],
                outcome=SentinelDiagnosticOutcome.SOURCE_NOT_INDEXED,
                identity_state=SentinelIdentityState.RESOLVED,
                source_indexing_state=SentinelSourceIndexingState.NOT_INDEXED,
                candidate_match_state=SentinelCandidateMatchState.UNTESTED,
                request=identity, request_hash=spec["identity_request_hash"],
                response_hash=identity_hash,
            )
        match = spec["match_request"]
        try:
            match_response = self._send(match, source, credentials)
            match_hash = hashlib.sha256(bytes(match_response.content)).hexdigest()
            match_ids = _parse_identifiers(source, match_response)
        except Exception as exc:  # noqa: BLE001
            return _unsupported_diagnostic(spec, f"match probe failed:{type(exc).__name__}")
        if match_ids:
            return SentinelDiagnostic(
                sentinel_id=spec["sentinel_id"], source=source,
                candidate_query_id=spec["candidate_query_id"],
                outcome=SentinelDiagnosticOutcome.INDEXED_AND_MATCHED,
                identity_state=SentinelIdentityState.RESOLVED,
                source_indexing_state=SentinelSourceIndexingState.INDEXED,
                candidate_match_state=SentinelCandidateMatchState.MATCHED,
                request=match, request_hash=spec["match_request_hash"],
                response_hash=match_hash, identifier_results=sorted(set(match_ids)),
            )
        return SentinelDiagnostic(
            sentinel_id=spec["sentinel_id"], source=source,
            candidate_query_id=spec["candidate_query_id"],
            outcome=SentinelDiagnosticOutcome.INDEXED_BUT_QUERY_MISSED,
            identity_state=SentinelIdentityState.RESOLVED,
            source_indexing_state=SentinelSourceIndexingState.INDEXED,
            candidate_match_state=SentinelCandidateMatchState.MISSED,
            request=match, request_hash=spec["match_request_hash"], response_hash=match_hash,
        )


def _fresh_observation(spec: dict[str, Any], timestamp: str) -> SizingObservation:
    gate_name = None
    gate_status = SizingGateStatus.NOT_APPLICABLE
    if spec["source"] == "SemanticScholar":
        gate_name, gate_status = "bulk_boolean_semantics", SizingGateStatus.PENDING
    elif spec["source"] == "CrossRef":
        gate_name, gate_status = "identification_semantics", SizingGateStatus.UNRESOLVED
    return SizingObservation(
        observation_id=f"live-sizing:{hashlib.sha256(spec['candidate_query_id'].encode()).hexdigest()[:24]}",
        candidate_query_id=spec["candidate_query_id"], query_hash=spec["query_hash"],
        source=spec["source"], observed_at=timestamp, request=dict(spec["request"]),
        request_hash=spec["request_hash"], response_hash=None, reported_count=None,
        count_kind=SizingCountKind(spec["count_kind"]), hard_window=spec.get("hard_window"),
        window_status=SizingWindowStatus.UNKNOWN, syntax_status=SizingSyntaxStatus.UNTESTED,
        credential_reference=spec.get("credential_reference"), gate_name=gate_name,
        gate_status=gate_status,
    )


def _parse_envelope(source: str, response: HttpResponse) -> ParsedEnvelope:
    if source == "PubMed":
        root = ET.fromstring(response.content)
        errors = [node.text or "" for node in root.findall(".//ErrorList/*")]
        warnings = [node.text or "" for node in root.findall(".//WarningList/*")]
        translation = root.findtext(".//QueryTranslation")
        count = int(root.findtext(".//Count", "-1"))
        if count < 0:
            raise ValueError("PubMed response omitted Count")
        status = (
            SizingSyntaxStatus.REJECTED
            if errors
            else SizingSyntaxStatus.WARNING
            if warnings
            else SizingSyntaxStatus.ACCEPTED
        )
        return ParsedEnvelope(count, SizingCountKind.EXACT, status, translation, tuple(errors + warnings))
    if source == "EuropePMC":
        data = response.json()
        count = int(data["hitCount"])
        source_warnings = data.get("warnings", [])
        if isinstance(source_warnings, dict):
            source_warnings = [f"{key}:{value}" for key, value in source_warnings.items()]
        warnings = tuple(str(item) for item in source_warnings)
        return ParsedEnvelope(count, SizingCountKind.EXACT,
                              SizingSyntaxStatus.WARNING if warnings else SizingSyntaxStatus.ACCEPTED,
                              warnings=warnings)
    if source == "SemanticScholar":
        data = response.json()
        count = data.get("total")
        warnings = []
        gate = SizingGateStatus.PENDING
        warnings.append(f"continuation_token_present={bool(data.get('token'))}")
        if data.get("error") or data.get("errors"):
            gate = SizingGateStatus.FAILED
            warnings.append(str(data.get("error") or data.get("errors")))
        return ParsedEnvelope(int(count) if count is not None else None, SizingCountKind.ESTIMATED,
                              SizingSyntaxStatus.REJECTED if gate is SizingGateStatus.FAILED else SizingSyntaxStatus.ACCEPTED,
                              warnings=tuple(warnings), gate_name="bulk_boolean_semantics", gate_status=gate)
    if source == "arXiv":
        root = ET.fromstring(response.content)
        error_entries = [
            item.text or ""
            for item in root.findall(
                "{http://www.w3.org/2005/Atom}entry/"
                "{http://www.w3.org/2005/Atom}summary"
            )
            if "error" in (item.text or "").lower()
        ]
        total = root.findtext("{http://a9.com/-/spec/opensearch/1.1/}totalResults")
        if total is None:
            raise ValueError("arXiv feed omitted totalResults")
        return ParsedEnvelope(
            int(total),
            SizingCountKind.EXACT,
            SizingSyntaxStatus.REJECTED if error_entries else SizingSyntaxStatus.ACCEPTED,
            warnings=tuple(error_entries),
        )
    if source == "IEEEXplore":
        data = response.json()
        warnings = (f"totalsearched={data.get('totalsearched')}",)
        return ParsedEnvelope(int(data["totalfound"]), SizingCountKind.EXACT,
                              SizingSyntaxStatus.ACCEPTED, warnings=warnings)
    if source == "CrossRef":
        data = response.json()
        message = data.get("message", {})
        count = message.get("total-results")
        query_evidence = message.get("query")
        gate_status = (
            SizingGateStatus.FAILED
            if isinstance(query_evidence, dict) and query_evidence.get("search-terms")
            else SizingGateStatus.UNRESOLVED
        )
        warning = (
            "Crossref response identifies the request as search-terms/free-text"
            if gate_status is SizingGateStatus.FAILED
            else "Crossref identification semantics remain unresolved"
        )
        return ParsedEnvelope(int(count) if count is not None else None, SizingCountKind.EXACT,
                              SizingSyntaxStatus.ACCEPTED, gate_name="identification_semantics",
                              gate_status=gate_status, warnings=(warning,))
    raise ValueError(f"unsupported sizing source: {source}")


def _parse_identifiers(source: str, response: HttpResponse) -> list[str]:
    if int(response.status_code) < 200 or int(response.status_code) >= 300:
        return []
    if source == "PubMed":
        return [item.text for item in ET.fromstring(response.content).findall(".//Id") if item.text]
    if source == "EuropePMC":
        data = response.json()
        return [str(item[key]) for item in data.get("resultList", {}).get("result", [])
                for key in ("doi", "pmid", "id") if item.get(key)]
    if source == "SemanticScholar":
        return [str(item["paperId"]) for item in response.json().get("data", []) if item.get("paperId")]
    if source == "arXiv":
        return [item.text for item in ET.fromstring(response.content).findall(
            "{http://www.w3.org/2005/Atom}entry/{http://www.w3.org/2005/Atom}id") if item.text]
    if source == "IEEEXplore":
        return [str(item["article_number"]) for item in response.json().get("articles", [])
                if item.get("article_number")]
    if source == "CrossRef":
        data = response.json().get("message", {})
        items = data.get("items", [data])
        return [str(item["DOI"]) for item in items if isinstance(item, dict) and item.get("DOI")]
    return []


def _successful_observation(
    observation: SizingObservation, attempt_number: int, started: str, completed: str,
    retry_reason: str | None, status: int, response_hash: str, envelope: ParsedEnvelope,
    *, attempt_request: dict[str, Any] | None = None,
) -> SizingObservation:
    transport = (
        SizingTransportStatus.GATE_FAILED
        if envelope.gate_status is SizingGateStatus.FAILED
        else SizingTransportStatus.SUCCEEDED
    )
    actual_request = attempt_request or observation.request
    attempt = SizingAttempt(
        attempt_number, started, completed, actual_request, sizing_request_hash(actual_request),
        transport, status,
        attempt_number - 1 if attempt_number > 1 else None, retry_reason, response_hash,
        observation.credential_reference, list(envelope.warnings), [],
    )
    window = SizingWindowStatus.UNKNOWN
    if envelope.count is not None and observation.hard_window is not None:
        window = (SizingWindowStatus.OVERFLOW if envelope.count > observation.hard_window
                  else SizingWindowStatus.CLEAR)
    return replace(observation, observed_at=completed, response_hash=response_hash,
                   reported_count=envelope.count, count_kind=envelope.count_kind,
                   window_status=window, syntax_status=envelope.syntax_status,
                   transport_status=transport, response_status=status,
                   source_query_translation=envelope.translation,
                   warnings=[*observation.warnings, *envelope.warnings],
                   gate_name=envelope.gate_name or observation.gate_name,
                   gate_status=envelope.gate_status, attempts=[*observation.attempts, attempt])


def _failed_attempt(
    observation: SizingObservation, number: int, started: str, completed: str,
    retry_reason: str | None, error: str, *, response_status: int | None = None,
    response_hash: str | None = None,
    attempt_request: dict[str, Any] | None = None,
) -> SizingObservation:
    actual_request = attempt_request or observation.request
    attempt = SizingAttempt(
        number, started, completed, actual_request, sizing_request_hash(actual_request),
        SizingTransportStatus.FAILED, response_status,
        number - 1 if number > 1 else None, retry_reason, response_hash,
        observation.credential_reference, [], [error],
    )
    return replace(observation, observed_at=completed, response_hash=response_hash,
                   transport_status=SizingTransportStatus.FAILED,
                   response_status=response_status, attempts=[*observation.attempts, attempt])


def _terminal_http_failure(
    observation: SizingObservation, number: int, started: str, completed: str,
    retry_reason: str | None, status: int, response_hash: str,
    *, attempt_request: dict[str, Any] | None = None,
) -> SizingObservation:
    current = _failed_attempt(observation, number, started, completed, retry_reason,
                              f"terminal_http_status:{status}", response_status=status,
                              response_hash=response_hash, attempt_request=attempt_request)
    return replace(current, syntax_status=SizingSyntaxStatus.REJECTED)


def _parse_failure(
    observation: SizingObservation, number: int, started: str, completed: str,
    retry_reason: str | None, status: int, response_hash: str, error: Exception,
    *, attempt_request: dict[str, Any] | None = None,
) -> SizingObservation:
    current = _failed_attempt(observation, number, started, completed, retry_reason,
                              f"response_envelope_error:{type(error).__name__}:{error}",
                              response_status=status, response_hash=response_hash,
                              attempt_request=attempt_request)
    return replace(current, syntax_status=SizingSyntaxStatus.REJECTED)


def _next_retry_reason(observation: SizingObservation) -> str | None:
    if not observation.attempts:
        return None
    final = observation.attempts[-1]
    return final.errors[-1] if final.errors else "resumed_retry"


def _observation_terminal(observation: SizingObservation, max_attempts: int) -> bool:
    if observation.transport_status in {
        SizingTransportStatus.SUCCEEDED, SizingTransportStatus.BLOCKED_CREDENTIAL,
        SizingTransportStatus.PENDING_MANUAL, SizingTransportStatus.GATE_FAILED,
    }:
        return True
    if observation.syntax_status is SizingSyntaxStatus.REJECTED:
        return True
    return len(observation.attempts) >= max_attempts


def _unsupported_diagnostic(spec: dict[str, Any], warning: str) -> SentinelDiagnostic:
    return SentinelDiagnostic(
        sentinel_id=spec["sentinel_id"], source=spec["source"],
        candidate_query_id=spec["candidate_query_id"],
        outcome=SentinelDiagnosticOutcome.DIAGNOSTIC_UNSUPPORTED,
        identity_state=SentinelIdentityState.RESOLVED,
        source_indexing_state=SentinelSourceIndexingState.UNKNOWN,
        candidate_match_state=SentinelCandidateMatchState.UNSUPPORTED, warnings=[warning],
    )


def build_comparison_report(run: QuerySizingRun) -> dict[str, Any]:
    qf01_counts = _variant_counts(run, "STAR-QF01-RELATIONAL-VIS", ("anchored", "unanchored"))
    qf02_counts = _variant_counts(run, "STAR-QF02-ASSISTED-VIS", ("A", "B", "C", "D"))
    return {
        "report_kind": "non_production_query_sizing_comparison",
        "sizing_run_id": run.sizing_run_id,
        "run_hash": run.run_hash(),
        "dry_run_plan_hash": run.dry_run_plan_hash,
        "qf01": {
            source: {
                "counts": counts,
                "unanchored_minus_anchored": _difference(counts, "unanchored", "anchored"),
                "unanchored_to_anchored_ratio": _ratio(counts, "unanchored", "anchored"),
            }
            for source, counts in qf01_counts.items()
        },
        "qf02": {
            source: {
                "counts": counts,
                "source_semantics_validated": _source_semantics_validated(
                    run, "STAR-QF02-ASSISTED-VIS", source
                ),
                "expected_c_le_d_le_a_le_b": (
                    _ordered_qf02(counts)
                    if _source_semantics_validated(run, "STAR-QF02-ASSISTED-VIS", source)
                    else None
                ),
            }
            for source, counts in qf02_counts.items()
        },
        "summary": {
            "transport_failures": [item.candidate_query_id for item in run.observations
                                   if item.transport_status is SizingTransportStatus.FAILED],
            "syntax_failures": [item.candidate_query_id for item in run.observations
                                if item.syntax_status is SizingSyntaxStatus.REJECTED],
            "hard_window_overflows": [item.candidate_query_id for item in run.observations
                                      if item.window_status is SizingWindowStatus.OVERFLOW],
            "near_zero_counts": [item.candidate_query_id for item in run.observations
                                 if item.reported_count in {0, 1}],
            "gate_failures": [item.candidate_query_id for item in run.observations
                              if item.gate_status is SizingGateStatus.FAILED],
            "blocked_credentials": [item.candidate_query_id for item in run.observations
                                    if item.transport_status is SizingTransportStatus.BLOCKED_CREDENTIAL],
            "acm_pending_manual": sum(item.transport_status is SizingTransportStatus.PENDING_MANUAL
                                      for item in run.observations),
            "sentinel_misses": [f"{item.sentinel_id}:{item.source}:{item.candidate_query_id}"
                                for item in run.sentinel_diagnostics
                                if item.outcome is SentinelDiagnosticOutcome.INDEXED_BUT_QUERY_MISSED],
        },
        "automatic_selection_performed": False,
        "production_queries_frozen": False,
    }


def _variant_counts(
    run: QuerySizingRun, family: str, variants: tuple[str, ...]
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for item in run.observations:
        if family not in item.candidate_query_id:
            continue
        variant = next((value for value in variants if f":{value}:" in item.candidate_query_id), None)
        if variant:
            output.setdefault(item.source, {})[variant] = item.reported_count
    return output


def _difference(counts: Mapping[str, int | None], left: str, right: str) -> int | None:
    if counts.get(left) is None or counts.get(right) is None:
        return None
    return int(counts[left]) - int(counts[right])


def _ratio(counts: Mapping[str, int | None], numerator: str, denominator: str) -> float | None:
    if counts.get(numerator) is None or not counts.get(denominator):
        return None
    return int(counts[numerator]) / int(counts[denominator])


def _ordered_qf02(counts: Mapping[str, int | None]) -> bool | None:
    if any(counts.get(key) is None for key in ("A", "B", "C", "D")):
        return None
    return int(counts["C"]) <= int(counts["D"]) <= int(counts["A"]) <= int(counts["B"])


def _source_semantics_validated(run: QuerySizingRun, family: str, source: str) -> bool:
    relevant = [
        item
        for item in run.observations
        if family in item.candidate_query_id and item.source == source
    ]
    return bool(relevant) and all(
        item.gate_status in {SizingGateStatus.NOT_APPLICABLE, SizingGateStatus.PASSED}
        for item in relevant
    )


def _hash_without_key(payload: dict[str, Any], key: str) -> str:
    value = {name: item for name, item in payload.items() if name != key}
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _pending_envelope_path(output: Path, candidate_id: str) -> Path:
    digest = hashlib.sha256(candidate_id.encode("utf-8")).hexdigest()
    return output.parent / ".query_sizing_pending" / f"{digest}.json"


def _save_pending_envelope(
    path: Path,
    observation: SizingObservation,
    attempt_number: int,
    started_at: str,
    completed_at: str,
    response_status: int,
    response_hash: str,
    envelope: ParsedEnvelope,
) -> None:
    payload = {
        "candidate_query_id": observation.candidate_query_id,
        "request_hash": observation.request_hash,
        "attempt_number": attempt_number,
        "started_at": started_at,
        "completed_at": completed_at,
        "response_status": response_status,
        "response_hash": response_hash,
        "envelope": {
            "count": envelope.count,
            "count_kind": envelope.count_kind.value,
            "syntax_status": envelope.syntax_status.value,
            "translation": envelope.translation,
            "warnings": list(envelope.warnings),
            "gate_name": envelope.gate_name,
            "gate_status": envelope.gate_status.value,
        },
        "contains_bibliographic_records": False,
    }
    atomic_write(path, (canonical_json(payload) + "\n").encode("utf-8"))


def _load_pending_envelope(
    path: Path, observation: SizingObservation, attempt_number: int
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("candidate_query_id") != observation.candidate_query_id:
        raise SizingPlanError("pending response candidate mismatch")
    if payload.get("request_hash") != observation.request_hash:
        raise SizingPlanError("pending response request hash mismatch")
    if payload.get("attempt_number") != attempt_number:
        return None
    if payload.get("contains_bibliographic_records") is not False:
        raise SizingPlanError("pending sizing response contains forbidden record data")
    return payload


def _envelope_from_dict(data: Mapping[str, Any]) -> ParsedEnvelope:
    return ParsedEnvelope(
        count=data.get("count"),
        count_kind=SizingCountKind(data["count_kind"]),
        syntax_status=SizingSyntaxStatus(data["syntax_status"]),
        translation=data.get("translation"),
        warnings=tuple(data.get("warnings", [])),
        gate_name=data.get("gate_name"),
        gate_status=SizingGateStatus(data.get("gate_status", "not_applicable")),
    )


def _load_credentials() -> tuple[dict[str, str], list[str]]:
    names = ("IEEE_XPLORE_API_KEY", "SEMANTIC_SCHOLAR_API_KEY")
    values = {name: os.environ[name] for name in names if os.environ.get(name)}
    return values, sorted(values)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--candidate-config", type=Path,
                        default=Path("config/star_query_candidates_v0_1.json"))
    parser.add_argument("--sentinel-config", type=Path,
                        default=Path("config/star_query_sentinels_v0_1.json"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--authorize-live-sizing", action="store_true")
    args = parser.parse_args(argv)
    if not args.authorize_live_sizing:
        parser.error("live sizing requires explicit --authorize-live-sizing")
    plan = load_validated_sizing_plan(args.plan, args.candidate_config, args.sentinel_config)
    credentials, names = _load_credentials()
    print(f"Configured credential references: {', '.join(names) if names else 'none'}")
    executor = LiveSizingExecutor(http=RequestsHttpClient())
    executor.execute(plan, args.output, args.report, credentials=credentials)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
