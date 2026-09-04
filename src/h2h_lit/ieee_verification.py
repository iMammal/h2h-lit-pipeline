"""Bounded credential-gated verification of the five frozen IEEE requests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.http import HttpClient, RequestsHttpClient
from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.production_prerequisites import (
    EXPECTED_PLAN_HASH,
    EXPECTED_PLAN_RAW_SHA256,
    IEEE_VERIFICATION_MANIFEST_PATH,
    PRODUCTION_SELECTION,
    build_prerequisite_payloads,
    load_prerequisite_package,
    save_prerequisite_payloads,
)
from h2h_lit.production_query_plan import load_production_query_plan
from h2h_lit.sources.ieee_xplore import PAGINATOR

PLAN_PATH = "config/star_production_query_plan_v1.json"
PACKAGE_PATH = "config/star_retrieval_prerequisites_v1.json"
READINESS_PATH = "config/star_retrieval_prerequisites_v1/ieee_readiness.json"
CHILD_DIRECTORY = "config/star_retrieval_prerequisites_v1"
OUTPUT_ROOT = "artifacts/ieee_verification/star-ieee-verification-001"
RUN_MANIFEST_PATH = f"{OUTPUT_ROOT}/run_manifest.json"
CREDENTIAL_NAME = "IEEE_XPLORE_API_KEY"
MANIFEST_ID = "star-ieee-xplore-verification-2026-09-04"
MANIFEST_VERSION = "1.0.0"
MAX_ATTEMPTS = 2
EXPECTED_VERIFICATION_REQUEST_HASHES = {
    "STAR-QF01-RELATIONAL-VIS": (
        "3ee82d9733bb0d6fc70cc2b075e22c7e8fbb58cd9a65696b4ca758e0637d4051"
    ),
    "STAR-QF02-ASSISTED-VIS": (
        "a4cd1a3ea2ddc2421ad131a96b70e6bbf69fabac02dc088ab6395c386cf41c7a"
    ),
    "STAR-QF03-INTERACTIVE-SYSTEMS": (
        "b9adbbd796ffa173a7ef285727fdbdfacbf4c9a217cb3ef0921ccc74af9d11e7"
    ),
    "STAR-QF04-NONDESKTOP-ENV": (
        "f0e3c50d19e8aafd014196d37bbdc80b7711b4eaaa2b3228bf61203e22f4897b"
    ),
    "STAR-QF05-CONVERSATIONAL": (
        "9f34facee3dd66e06a82cd04de82d348d35929fb02744f318db7ff4f0accb4bf"
    ),
}
PROVIDER_ERROR_FIELDS = (
    "error",
    "errors",
    "error_message",
    "errorMessage",
    "error_code",
    "errorCode",
)
PROVIDER_WARNING_FIELDS = ("warning", "warnings")


class IeeeVerificationError(RuntimeError):
    """The bounded IEEE verification could not be proven complete and valid."""

    def __init__(
        self, message: str, *, observation: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.observation = observation


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def preflight_ieee_verification(*, root: str | Path) -> dict[str, Any]:
    """Validate all frozen local inputs without reading a credential or using HTTP."""

    root_path = Path(root).resolve()
    package = load_prerequisite_package(root_path / PACKAGE_PATH, root=root_path)
    plan_file = root_path / PLAN_PATH
    plan = load_production_query_plan(plan_file, root=root_path)
    if _sha256(plan_file.read_bytes()) != EXPECTED_PLAN_RAW_SHA256:
        raise IeeeVerificationError("frozen production-plan raw hash mismatch")
    if plan.plan_hash() != EXPECTED_PLAN_HASH:
        raise IeeeVerificationError("frozen production-plan canonical hash mismatch")

    readiness_file = root_path / READINESS_PATH
    readiness = json.loads(readiness_file.read_text(encoding="utf-8"))
    expected = _expected_requests(plan.payload)
    actual = readiness.get("verification_requests", [])
    frozen_keys = (
        "family_id",
        "query_id",
        "query_text_sha256",
        "request",
        "request_hash",
    )
    actual_frozen = [
        {key: item.get(key) for key in frozen_keys} for item in actual
    ]
    expected_frozen = [
        {key: item.get(key) for key in frozen_keys} for item in expected
    ]
    if len(actual) != len(expected) or actual_frozen != expected_frozen:
        raise IeeeVerificationError("IEEE readiness requests do not match the frozen plan")
    executed = readiness.get("verification_executed") is True
    if executed and readiness.get("status") not in {
        "VERIFIED_READY_FOR_RETRIEVAL",
        "VERIFIED_PARTITION_REVIEW_REQUIRED",
    }:
        raise IeeeVerificationError("IEEE executed readiness state is invalid")
    for item in actual:
        frozen_hash = EXPECTED_VERIFICATION_REQUEST_HASHES.get(item["family_id"])
        if item["request_hash"] != frozen_hash:
            raise IeeeVerificationError(
                f"frozen IEEE request hash mismatch for {item['family_id']}"
            )

    package_ref = next(
        item
        for item in package.payload["artifacts"]
        if item["artifact_id"] == "star-ieee-readiness-v1"
    )
    if (
        package_ref["raw_sha256"] != _sha256(readiness_file.read_bytes())
        or package_ref["canonical_hash"] != readiness["artifact_hash"]
    ):
        raise IeeeVerificationError("IEEE readiness package binding mismatch")
    return {
        "status": (
            "POST_EXECUTION_AUDIT_PASSED_NO_NETWORK"
            if executed
            else "PREFLIGHT_PASSED_NO_NETWORK"
        ),
        "production_query_plan": _file_reference(
            plan_file,
            root_path,
            plan.plan_hash(),
            plan.payload["plan_version"],
        ),
        "ieee_readiness": _file_reference(
            readiness_file,
            root_path,
            readiness["artifact_hash"],
            readiness["artifact_version"],
        ),
        "requests": actual,
        "request_count": len(actual),
        "maximum_http_attempts": len(actual) * MAX_ATTEMPTS,
        "credential_reference": CREDENTIAL_NAME,
        "credential_read": False,
        "network_used": False,
        "output_root": OUTPUT_ROOT,
        "provenance_path": IEEE_VERIFICATION_MANIFEST_PATH,
    }


def execute_ieee_verification(
    *,
    root: str | Path,
    http: HttpClient,
    credential: str,
    timestamp: Callable[[], str] = utc_now,
    sleep: Callable[[float], None] = time.sleep,
    retry_policy: RetryPolicy | None = None,
    rate_limiter: RateLimiter | None = None,
    finalize_prerequisites: bool = True,
) -> dict[str, Any]:
    """Execute only the frozen five-request verification and persist its evidence."""

    if not credential:
        raise IeeeVerificationError(f"{CREDENTIAL_NAME} is required")
    root_path = Path(root).resolve()
    output_root = _safe_output_path(root_path, OUTPUT_ROOT)
    provenance_path = _safe_output_path(root_path, IEEE_VERIFICATION_MANIFEST_PATH)
    if output_root.exists():
        raise IeeeVerificationError(f"verification output already exists: {OUTPUT_ROOT}")
    if provenance_path.exists():
        raise IeeeVerificationError(
            f"verification provenance already exists: {IEEE_VERIFICATION_MANIFEST_PATH}"
        )
    preflight = preflight_ieee_verification(root=root_path)
    policy = retry_policy or RetryPolicy(
        max_attempts=MAX_ATTEMPTS,
        base_delay_seconds=1.0,
        maximum_delay_seconds=10.0,
    )
    if policy.max_attempts > MAX_ATTEMPTS:
        raise IeeeVerificationError(
            f"IEEE verification permits at most {MAX_ATTEMPTS} attempts per request"
        )
    limiter = rate_limiter or RateLimiter()
    started_at = timestamp()
    observations: list[dict[str, Any]] = []
    failure: IeeeVerificationError | None = None

    for item in preflight["requests"]:
        try:
            observations.append(
                _execute_request(
                    root=root_path,
                    frozen=item,
                    hard_window=_hard_window_for(
                        root_path, item["family_id"]
                    ),
                    http=http,
                    credential=credential,
                    timestamp=timestamp,
                    sleep=sleep,
                    retry_policy=policy,
                    rate_limiter=limiter,
                )
            )
        except IeeeVerificationError as exc:
            if exc.observation is not None:
                observations.append(exc.observation)
            failure = exc
            break

    completed_at = timestamp()
    if failure is not None:
        run = {
            "schema_version": "1.0.0",
            "run_id": "star-ieee-verification-001",
            "status": "FAILED_CLOSED",
            "started_at_utc": started_at,
            "completed_at_utc": completed_at,
            "requests": observations,
            "failure": str(failure),
            "credential": {
                "required_name": CREDENTIAL_NAME,
                "present_at_execution": True,
                "value_persisted": False,
            },
            "downstream_side_effects": [],
        }
        atomic_write(
            root_path / RUN_MANIFEST_PATH, _pretty_json(run).encode("utf-8")
        )
        raise failure

    source_window_state = (
        "RESOLVED_CLEAR"
        if all(
            item["source_window"]["state"] == "RESOLVED_CLEAR"
            for item in observations
        )
        else "UNRESOLVED_PARTITION_REVIEW"
    )
    manifest: dict[str, Any] = {
        "schema_version": "1.0.0",
        "manifest_id": MANIFEST_ID,
        "manifest_version": MANIFEST_VERSION,
        "run_id": "star-ieee-verification-001",
        "status": (
            "VERIFIED_READY_FOR_RETRIEVAL"
            if source_window_state == "RESOLVED_CLEAR"
            else "VERIFIED_PARTITION_REVIEW_REQUIRED"
        ),
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "production_query_plan": preflight["production_query_plan"],
        "input_ieee_readiness": preflight["ieee_readiness"],
        "credential": {
            "required_name": CREDENTIAL_NAME,
            "present_at_execution": True,
            "verified": True,
            "value_persisted": False,
        },
        "api": {
            "endpoint": observations[0]["endpoint"],
            "transport": "official_metadata_api_https",
        },
        "request_policy": {
            "frozen_request_count": 5,
            "max_records_per_request": 1,
            "maximum_attempts_per_request": policy.max_attempts,
            "maximum_http_attempts": 5 * policy.max_attempts,
            "pagination_performed": False,
            "production_retrieval_performed": False,
        },
        "requests": observations,
        "source_window_state": source_window_state,
        "source_window_rule": (
            "compare_exact_provider_count_to_frozen_declared_hard_window; "
            "no_declared_hard_window_is_resolved_without_inventing_a_limit"
        ),
        "downstream_side_effects": [],
        "production_retrieval_started": False,
        "production_retrieval_wave_instantiated": False,
        "retrieval_cutoff_established": False,
    }
    manifest["manifest_hash"] = _manifest_hash(manifest)
    atomic_write(provenance_path, _pretty_json(manifest).encode("utf-8"))
    run = {
        "schema_version": "1.0.0",
        "run_id": manifest["run_id"],
        "status": manifest["status"],
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "tracked_manifest": _file_reference(
            provenance_path,
            root_path,
            manifest["manifest_hash"],
            manifest["manifest_version"],
        ),
        "requests": observations,
        "credential": {
            "required_name": CREDENTIAL_NAME,
            "present_at_execution": True,
            "verified": True,
            "value_persisted": False,
        },
        "downstream_side_effects": [],
    }
    atomic_write(root_path / RUN_MANIFEST_PATH, _pretty_json(run).encode("utf-8"))

    if source_window_state == "RESOLVED_CLEAR" and finalize_prerequisites:
        _finalize_prerequisites(root_path)
    return manifest


def _execute_request(
    *,
    root: Path,
    frozen: Mapping[str, Any],
    hard_window: int | None,
    http: HttpClient,
    credential: str,
    timestamp: Callable[[], str],
    sleep: Callable[[float], None],
    retry_policy: RetryPolicy,
    rate_limiter: RateLimiter,
) -> dict[str, Any]:
    request = dict(frozen["request"])
    if _hash_payload(request) != frozen["request_hash"]:
        raise IeeeVerificationError(
            f"request-hash mismatch before execution: {frozen['family_id']}",
            observation=_failed_request_observation(
                frozen, request, [], "REQUEST_HASH_MISMATCH"
            ),
        )
    attempts: list[dict[str, Any]] = []
    for attempt_number in range(1, retry_policy.max_attempts + 1):
        requested_at = timestamp()
        params = dict(request["params"])
        params["apikey"] = credential
        try:
            rate_limiter.wait("IEEEXplore")
            response = http.get(
                request["endpoint"], params=params, headers={}, timeout=30.0
            )
        except Exception as exc:  # noqa: BLE001 - transports are injected
            attempts.append(
                {
                    "attempt_number": attempt_number,
                    "requested_at_utc": requested_at,
                    "responded_at_utc": timestamp(),
                    "outcome": "RETRYABLE_TRANSPORT_FAILURE",
                    "failure_reason": f"transport_error:{type(exc).__name__}",
                    "http_status": None,
                    "response_artifact": None,
                }
            )
            if attempt_number == retry_policy.max_attempts:
                raise IeeeVerificationError(
                    f"IEEE transport failed for {frozen['family_id']}",
                    observation=_failed_request_observation(
                        frozen, request, attempts, "TRANSPORT_FAILURE"
                    ),
                ) from None
            sleep(retry_policy.delay(attempt_number))
            continue

        responded_at = timestamp()
        raw = bytes(response.content)
        if credential.encode("utf-8") in raw:
            raise IeeeVerificationError(
                f"provider response exposed credential for {frozen['family_id']}"
            )
        response_ref = _save_raw_response(
            root,
            frozen["family_id"],
            attempt_number,
            raw,
        )
        status = int(response.status_code)
        attempt = {
            "attempt_number": attempt_number,
            "requested_at_utc": requested_at,
            "responded_at_utc": responded_at,
            "http_status": status,
            "response_artifact": response_ref,
        }
        if status in retry_policy.retry_statuses:
            attempt.update(
                {
                    "outcome": "RETRYABLE_HTTP_FAILURE",
                    "failure_reason": f"retryable_http_status:{status}",
                }
            )
            attempts.append(attempt)
            if attempt_number == retry_policy.max_attempts:
                raise IeeeVerificationError(
                    f"IEEE retry limit reached for {frozen['family_id']}",
                    observation=_failed_request_observation(
                        frozen, request, attempts, "RETRY_LIMIT_REACHED"
                    ),
                )
            sleep(
                retry_policy.delay(
                    attempt_number, (response.headers or {}).get("Retry-After")
                )
            )
            continue
        if status in {401, 403}:
            attempt.update(
                {
                    "outcome": "TERMINAL_AUTHENTICATION_FAILURE",
                    "failure_reason": f"authentication_http_status:{status}",
                }
            )
            attempts.append(attempt)
            raise IeeeVerificationError(
                f"IEEE authentication failed for {frozen['family_id']} ({status})",
                observation=_failed_request_observation(
                    frozen, request, attempts, "AUTHENTICATION_FAILURE"
                ),
            )
        if status != 200:
            attempt.update(
                {
                    "outcome": "TERMINAL_HTTP_FAILURE",
                    "failure_reason": f"terminal_http_status:{status}",
                }
            )
            attempts.append(attempt)
            raise IeeeVerificationError(
                f"IEEE terminal HTTP failure for {frozen['family_id']} ({status})",
                observation=_failed_request_observation(
                    frozen, request, attempts, "TERMINAL_HTTP_FAILURE"
                ),
            )
        try:
            payload = response.json()
            provider_errors, provider_warnings = _provider_messages(payload)
            if provider_errors:
                raise IeeeVerificationError("provider reported an error")
            if not isinstance(payload, dict) or not isinstance(payload.get("articles"), list):
                raise IeeeVerificationError("unexpected IEEE response structure")
            parsed = PAGINATOR.parse_response(
                SimpleNamespace(query_text=request["params"]["querytext"], limit=1),
                {"start_record": 1},
                response,
            )
            if not parsed.total_is_exact or parsed.source_reported_total is None:
                raise IeeeVerificationError("IEEE exact total was not available")
            if parsed.incomplete_reason:
                raise IeeeVerificationError(parsed.incomplete_reason)
            provider_count = parsed.source_reported_total
        except IeeeVerificationError as exc:
            attempt.update(
                {
                    "outcome": "PROVIDER_OR_QUERY_FAILURE",
                    "failure_reason": str(exc),
                }
            )
            attempts.append(attempt)
            raise IeeeVerificationError(
                f"IEEE provider/query validation failed for {frozen['family_id']}: {exc}",
                observation=_failed_request_observation(
                    frozen, request, attempts, "PROVIDER_OR_QUERY_FAILURE"
                ),
            ) from None
        except Exception as exc:  # noqa: BLE001 - adapter failures must fail closed
            attempt.update(
                {
                    "outcome": "PARSER_FAILURE",
                    "failure_reason": f"parser_failure:{type(exc).__name__}",
                }
            )
            attempts.append(attempt)
            raise IeeeVerificationError(
                f"IEEE parser failure for {frozen['family_id']}: {type(exc).__name__}",
                observation=_failed_request_observation(
                    frozen, request, attempts, "PARSER_FAILURE"
                ),
            ) from None
        source_window = _source_window(provider_count, hard_window)
        attempt.update({"outcome": "SUCCEEDED", "failure_reason": None})
        attempts.append(attempt)
        return {
            "request_id": f"verify:{frozen['query_id']}",
            "family_id": frozen["family_id"],
            "query_id": frozen["query_id"],
            "query_text_sha256": frozen["query_text_sha256"],
            "request_hash": frozen["request_hash"],
            "method": request["method"],
            "endpoint": request["endpoint"],
            "frozen_params": request["params"],
            "credential_reference": CREDENTIAL_NAME,
            "credential_value_persisted": False,
            "execution_status": "SUCCEEDED",
            "attempts": attempts,
            "final_http_status": status,
            "provider_count": provider_count,
            "provider_total_field": parsed.metadata["total_field"],
            "provider_totalsearched": parsed.metadata.get("totalsearched"),
            "provider_errors": [],
            "provider_warnings": provider_warnings,
            "source_window": source_window,
        }
    raise AssertionError("bounded IEEE attempt loop exited unexpectedly")


def _failed_request_observation(
    frozen: Mapping[str, Any],
    request: Mapping[str, Any],
    attempts: list[dict[str, Any]],
    failure_reason: str,
) -> dict[str, Any]:
    return {
        "request_id": f"verify:{frozen['query_id']}",
        "family_id": frozen["family_id"],
        "query_id": frozen["query_id"],
        "query_text_sha256": frozen["query_text_sha256"],
        "request_hash": frozen["request_hash"],
        "method": request["method"],
        "endpoint": request["endpoint"],
        "frozen_params": request["params"],
        "credential_reference": CREDENTIAL_NAME,
        "credential_value_persisted": False,
        "execution_status": "FAILED_CLOSED",
        "attempts": attempts,
        "failure_reason": failure_reason,
    }


def _expected_requests(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    queries = [item for item in plan["source_queries"] if item["source"] == "IEEEXplore"]
    if [item["family_id"] for item in queries] != list(PRODUCTION_SELECTION):
        raise IeeeVerificationError("frozen IEEE family coverage changed")
    result = []
    for query in queries:
        params = dict(query["request_specification"]["params"])
        params.update({"max_records": 1, "start_record": 1})
        request = {
            "method": "GET",
            "endpoint": query["request_specification"]["endpoint"],
            "params": params,
            "credential_reference": CREDENTIAL_NAME,
        }
        result.append(
            {
                "family_id": query["family_id"],
                "query_id": query["query_id"],
                "query_text_sha256": query["query_text_sha256"],
                "request": request,
                "request_hash": _hash_payload(request),
                "execution_status": "NOT_EXECUTED",
            }
        )
    return result


def _hard_window_for(root: Path, family_id: str) -> int | None:
    plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    item = next(
        item
        for item in plan["source_queries"]
        if item["source"] == "IEEEXplore" and item["family_id"] == family_id
    )
    value = item.get("hard_window")
    return int(value) if value is not None else None


def _source_window(provider_count: int, hard_window: int | None) -> dict[str, Any]:
    if hard_window is None:
        return {
            "state": "RESOLVED_CLEAR",
            "status": "exact_total_no_declared_hard_window",
            "hard_window": None,
            "partitioning_invented": False,
        }
    if provider_count <= hard_window:
        return {
            "state": "RESOLVED_CLEAR",
            "status": "clear",
            "hard_window": hard_window,
            "partitioning_invented": False,
        }
    return {
        "state": "UNRESOLVED_PARTITION_REVIEW",
        "status": "overflow_requires_separate_partition_review",
        "hard_window": hard_window,
        "partitioning_invented": False,
    }


def _provider_messages(payload: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        raise IeeeVerificationError("IEEE response must be a JSON object")
    errors = [
        {"field": field, "value": payload[field]}
        for field in PROVIDER_ERROR_FIELDS
        if field in payload and payload[field] not in (None, "", [], {})
    ]
    warnings = [
        {"field": field, "value": payload[field]}
        for field in PROVIDER_WARNING_FIELDS
        if field in payload and payload[field] not in (None, "", [], {})
    ]
    return errors, warnings


def _save_raw_response(
    root: Path, family_id: str, attempt_number: int, raw: bytes
) -> dict[str, Any]:
    short_family = family_id.split("-", 2)[1]
    relative = Path(OUTPUT_ROOT) / "responses" / (
        f"{short_family}_attempt_{attempt_number:03d}.json"
    )
    path = _safe_output_path(root, relative.as_posix())
    atomic_write(path, raw)
    return {
        "path": path.relative_to(root).as_posix(),
        "byte_size": len(raw),
        "raw_sha256": _sha256(raw),
    }


def _finalize_prerequisites(root: Path) -> None:
    current = load_prerequisite_package(root / PACKAGE_PATH, root=root)
    children, package = build_prerequisite_payloads(
        root=root,
        plan_path=root / PLAN_PATH,
        generated_at=current.payload["generated_at"],
        ieee_credential_present=False,
    )
    save_prerequisite_payloads(
        root=root,
        child_directory=CHILD_DIRECTORY,
        package_path=PACKAGE_PATH,
        children=children,
        package=package,
    )


def _safe_output_path(root: Path, relative: str) -> Path:
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts:
        raise IeeeVerificationError("IEEE evidence paths must be relative and traversal-safe")
    path = (root / value).resolve()
    if not path.is_relative_to(root):
        raise IeeeVerificationError("IEEE evidence path escaped the repository root")
    return path


def _file_reference(
    path: Path, root: Path, canonical_hash: str, version: str
) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "path": path.relative_to(root).as_posix(),
        "version": version,
        "raw_sha256": _sha256(raw),
        "canonical_hash": canonical_hash,
        "byte_size": len(raw),
    }


def _manifest_hash(payload: Mapping[str, Any]) -> str:
    material = dict(payload)
    material.pop("manifest_hash", None)
    return _hash_payload(material)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    return _sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    )


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _pretty_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--authorize-live-ieee-verification", action="store_true")
    args = parser.parse_args(argv)
    preflight = preflight_ieee_verification(root=args.root)
    if not args.authorize_live_ieee_verification:
        print(
            json.dumps(
                {
                    "status": preflight["status"],
                    "request_count": preflight["request_count"],
                    "maximum_http_attempts": preflight["maximum_http_attempts"],
                    "request_hashes": [
                        {
                            "family_id": item["family_id"],
                            "request_hash": item["request_hash"],
                        }
                        for item in preflight["requests"]
                    ],
                    "credential_reference": CREDENTIAL_NAME,
                    "credential_read": False,
                    "network_used": False,
                    "output_root": OUTPUT_ROOT,
                    "provenance_path": IEEE_VERIFICATION_MANIFEST_PATH,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    credential = os.environ.get(CREDENTIAL_NAME, "")
    if not credential:
        parser.error(f"{CREDENTIAL_NAME} must be present for authorized execution")
    manifest = execute_ieee_verification(
        root=args.root,
        http=RequestsHttpClient(),
        credential=credential,
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "manifest_hash": manifest["manifest_hash"],
                "provider_counts": {
                    item["family_id"]: item["provider_count"]
                    for item in manifest["requests"]
                },
                "credential_reference": CREDENTIAL_NAME,
                "credential_value_persisted": False,
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
