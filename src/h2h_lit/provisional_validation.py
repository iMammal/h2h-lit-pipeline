"""Fail-closed planning and preflight for the isolated STAR validation run."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from h2h_lit.acm_field_execution import load_acm_final_reconciliation_manifest
from h2h_lit.bibtex_io import (
    parse_bibtex_with_diagnostics,
    record_from_bibtex_fields,
    split_bib_entries,
)
from h2h_lit.checkpoint import atomic_write
from h2h_lit.http import HttpClient, HttpResponse, RequestsHttpClient
from h2h_lit.normalize import dedupe_key, normalize_doi, normalize_title
from h2h_lit.pagination import RateLimiter, RetryPolicy, redact_url
from h2h_lit.production_query_plan import load_production_query_plan
from h2h_lit.sources.pubmed import EUTILS as PUBMED_EUTILS
from h2h_lit.sources.pubmed import parse_pubmed_fetch

SCHEMA_VERSION = "1.0.0"
PLAN_ID = "star-provisional-pipeline-validation-001"
PREFLIGHT_SCHEMA_VERSION = "1.0.0"
PREFLIGHT_ARTIFACT_CLASS = "PROVISIONAL_VALIDATION_PREFLIGHT_ONLY"
ALLOWED_OUTPUT_ROOT = Path("outputs/provisional")
EXPECTED_FAMILIES = (
    "STAR-QF01-RELATIONAL-VIS",
    "STAR-QF02-ASSISTED-VIS",
    "STAR-QF03-INTERACTIVE-SYSTEMS",
    "STAR-QF04-NONDESKTOP-ENV",
    "STAR-QF05-CONVERSATIONAL",
)
EXPECTED_AUTHORIZATION_FLAGS = {
    "pubmed": "--authorize-pubmed-execution",
    "acm": "--authorize-acm-provisional-import",
    "llm": "--authorize-llm-inference",
}
PUBMED_EXECUTION_ARTIFACT_CLASS = "PROVISIONAL_PUBMED_EXECUTION_ONLY"
OFFLINE_STAGE_ARTIFACT_CLASS = "PROVISIONAL_ACM_PUBMED_LOCAL_VALIDATION_ONLY"
SCREENING_ARTIFACT_CLASS = "PROVISIONAL_VALIDATION_ONLY"
SCREENING_SAMPLE_CLASS = "PROVISIONAL_STAGE5D_INFERENCE_SAMPLE"
SCREENING_RUN_CLASS = "PROVISIONAL_STAGE5D_ELIGIBILITY_ONLY"
PUBMED_SEARCH_ENDPOINT = PUBMED_EUTILS + "esearch.fcgi"
PUBMED_FETCH_ENDPOINT = PUBMED_EUTILS + "efetch.fcgi"


class ProvisionalValidationError(ValueError):
    """The provisional validation plan is unsafe or no longer reproducible."""


@dataclass(slots=True)
class _StoredHttpResponse:
    status_code: int
    headers: dict[str, str]
    content: bytes
    text: str
    url: str
    request_url: str

    def json(self) -> Any:
        return json.loads(self.text)

    def iter_content(self, chunk_size: int = 8192):
        for index in range(0, len(self.content), chunk_size):
            yield self.content[index : index + chunk_size]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _bound_file(root: Path, specification: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    relative = Path(str(specification["path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ProvisionalValidationError("bound paths must be repository-relative")
    path = root / relative
    raw = path.read_bytes()
    actual = _sha256_bytes(raw)
    if actual != specification["raw_sha256"]:
        raise ProvisionalValidationError(f"bound file hash changed: {relative.as_posix()}")
    return path, {
        "path": relative.as_posix(),
        "byte_size": len(raw),
        "raw_sha256": actual,
        **(
            {"canonical_hash": specification["canonical_hash"]}
            if "canonical_hash" in specification
            else {}
        ),
    }


def _validate_embedded_hash(payload: dict[str, Any], field: str) -> None:
    claimed = payload.get(field)
    material = dict(payload)
    material.pop(field, None)
    if claimed != _sha256_json(material):
        raise ProvisionalValidationError(f"embedded {field} is invalid")


def load_validation_config(path: str | Path) -> dict[str, Any]:
    """Load the additive nonproduction plan and reject weakened safeguards."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ProvisionalValidationError("unsupported provisional validation schema")
    if payload.get("plan_id") != PLAN_ID:
        raise ProvisionalValidationError("unexpected provisional validation plan ID")
    if not str(payload.get("run_id", "")).startswith("provisional:"):
        raise ProvisionalValidationError("provisional run IDs require the provisional prefix")
    if payload.get("production_import_allowed") is not False:
        raise ProvisionalValidationError("the provisional plan cannot permit production import")
    if payload.get("output_namespace") != (
        "outputs/provisional/star-pipeline-validation-001"
    ):
        raise ProvisionalValidationError("unexpected provisional output namespace")

    pubmed = payload.get("pubmed", {})
    if pubmed.get("complete_identity_enumeration_required") is not True:
        raise ProvisionalValidationError("complete PubMed identity enumeration is required")
    if pubmed.get("family_count") != len(EXPECTED_FAMILIES):
        raise ProvisionalValidationError("PubMed must cover all five frozen families")
    if pubmed.get("metadata_sample_size_per_family") != 100:
        raise ProvisionalValidationError("unexpected PubMed metadata sample size")
    if pubmed.get("metadata_fetch_batch_size") != 100:
        raise ProvisionalValidationError("unexpected PubMed metadata batch size")
    expected_requests = len(EXPECTED_FAMILIES) * (
        1
        + (
            pubmed["metadata_sample_size_per_family"]
            + pubmed["metadata_fetch_batch_size"]
            - 1
        )
        // pubmed["metadata_fetch_batch_size"]
    )
    if pubmed.get("expected_request_count_without_retries") != expected_requests:
        raise ProvisionalValidationError("PubMed request-count declaration is inconsistent")
    if pubmed.get("authorization_flag") != EXPECTED_AUTHORIZATION_FLAGS["pubmed"]:
        raise ProvisionalValidationError("unexpected PubMed authorization boundary")
    if payload.get("acm", {}).get("authorization_flag") != EXPECTED_AUTHORIZATION_FLAGS["acm"]:
        raise ProvisionalValidationError("unexpected ACM authorization boundary")
    if payload.get("screening", {}).get("authorization_flag") != (
        EXPECTED_AUTHORIZATION_FLAGS["llm"]
    ):
        raise ProvisionalValidationError("unexpected LLM authorization boundary")

    if payload.get("candidate_selection", {}).get("target_canonical_records") != 750:
        raise ProvisionalValidationError("unexpected provisional candidate target")
    if payload.get("screening", {}).get("proposal_sample_size") != 250:
        raise ProvisionalValidationError("unexpected proposal sample target")
    if payload.get("jfr25_rediscovery", {}).get("create_seed_occurrences") is not False:
        raise ProvisionalValidationError("JFR25 must remain comparison-only")
    prohibited = payload.get("prohibited_effects", {})
    if not prohibited or not all(value is True for value in prohibited.values()):
        raise ProvisionalValidationError("every production side effect must remain prohibited")
    return payload


def resolve_output_namespace(root: str | Path, configured: str) -> Path:
    """Resolve the one allowed ignored namespace and reject path escape/symlinks."""

    root_path = Path(root).resolve()
    relative = Path(configured)
    if relative.is_absolute() or ".." in relative.parts:
        raise ProvisionalValidationError("output namespace must be repository-relative")
    allowed = (root_path / ALLOWED_OUTPUT_ROOT).resolve()
    output = (root_path / relative).resolve()
    if output == allowed or allowed not in output.parents:
        raise ProvisionalValidationError("output must be beneath outputs/provisional")
    return output


def deterministic_identity_sample(
    identities: list[str], *, sample_size: int, salt: str
) -> list[str]:
    """Select identities without using provider order or record contents."""

    unique = set(identities)
    if len(unique) != len(identities):
        raise ProvisionalValidationError("identity sampling requires a unique universe")
    if sample_size < 0:
        raise ProvisionalValidationError("sample size cannot be negative")
    return sorted(
        identities,
        key=lambda identity: (
            _sha256_bytes(f"{salt}\x1f{identity}".encode()),
            identity,
        ),
    )[:sample_size]


def _selection_rows(
    identities: list[str], *, sample_size: int, config_hash: str, query_hash: str
) -> list[dict[str, Any]]:
    salt = f"{config_hash}\x1f{query_hash}"
    selected = deterministic_identity_sample(
        identities, sample_size=min(sample_size, len(identities)), salt=salt
    )
    return [
        {
            "selection_rank": rank,
            "pmid": pmid,
            "selection_sha256": _sha256_bytes(
                f"{config_hash}\x1f{query_hash}\x1f{pmid}".encode()
            ),
        }
        for rank, pmid in enumerate(selected, start=1)
    ]


def _parse_pubmed_enumeration(
    content: bytes, *, maximum_supported_count: int
) -> dict[str, Any]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise ProvisionalValidationError("PubMed ESearch returned malformed XML") from exc
    count_text = root.findtext(".//Count")
    if count_text is None or not count_text.isdigit():
        raise ProvisionalValidationError("PubMed ESearch Count is missing or malformed")
    provider_count = int(count_text)
    if provider_count > maximum_supported_count:
        raise ProvisionalValidationError(
            f"PubMed count {provider_count} exceeds the supported complete-ID window "
            f"{maximum_supported_count}"
        )
    pmids = [item.text.strip() for item in root.findall(".//Id") if item.text]
    if len(pmids) != provider_count:
        raise ProvisionalValidationError(
            f"PubMed returned {len(pmids)} PMIDs for provider Count {provider_count}"
        )
    if len(set(pmids)) != len(pmids):
        raise ProvisionalValidationError("PubMed complete identity enumeration contains duplicates")
    if any(not pmid.isdigit() for pmid in pmids):
        raise ProvisionalValidationError("PubMed returned a non-numeric PMID")
    errors = [
        "".join(item.itertext()).strip()
        for item in root.findall(".//ErrorList/*")
        if "".join(item.itertext()).strip()
    ]
    if errors:
        raise ProvisionalValidationError(
            "PubMed reported query errors: " + "; ".join(errors)
        )
    warnings = [
        "".join(item.itertext()).strip()
        for item in root.findall(".//WarningList/*")
        if "".join(item.itertext()).strip()
    ]
    return {
        "semantic_state": "COMPLETE_IDENTITY_ENUMERATION",
        "provider_reported_count": provider_count,
        "unique_pmid_count": len(pmids),
        "pmids_provider_order": pmids,
        "pmid_provider_order_sha256": _sha256_bytes("\n".join(pmids).encode()),
        "pmid_identity_set_sha256": _sha256_bytes(
            "\n".join(sorted(pmids)).encode()
        ),
        "query_translation": root.findtext(".//QueryTranslation"),
        "query_key": root.findtext(".//QueryKey"),
        "webenv_present": bool(root.findtext(".//WebEnv")),
        "warnings": warnings,
        "characterized_as_truncated_due_to_metadata_sampling": False,
    }


def _sanitize_response_headers(headers: dict[str, Any]) -> dict[str, str]:
    sensitive = {"authorization", "proxy-authorization", "set-cookie", "cookie"}
    return {
        str(key): "<redacted>" if str(key).lower() in sensitive else str(value)
        for key, value in headers.items()
    }


def _request_hash(
    *, method: str, url: str, params: dict[str, Any], headers: dict[str, str]
) -> str:
    return _sha256_json(
        {
            "method": method,
            "url": url,
            "params": params,
            "headers": headers,
        }
    )


def _persist_response(
    *,
    path: Path,
    response: HttpResponse,
    method: str,
    request_url: str,
    request_params: dict[str, Any],
    request_headers: dict[str, str],
) -> dict[str, Any]:
    content = bytes(response.content)
    payload = {
        "status_code": response.status_code,
        "response_headers": _sanitize_response_headers(response.headers or {}),
        "content_base64": base64.b64encode(content).decode("ascii"),
        "response_url": redact_url(response.url),
        "actual_request_url": redact_url(
            getattr(response, "request_url", response.url)
        ),
        "request": {
            "method": method,
            "url": request_url,
            "params": request_params,
            "headers": request_headers,
            "request_hash": _request_hash(
                method=method,
                url=request_url,
                params=request_params,
                headers=request_headers,
            ),
        },
        "raw_body_byte_size": len(content),
        "raw_body_sha256": _sha256_bytes(content),
    }
    encoded = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode()
    atomic_write(path, encoded)
    return {
        "relative_path": path.as_posix(),
        "byte_size": len(encoded),
        "sha256": _sha256_bytes(encoded),
        "raw_body_byte_size": len(content),
        "raw_body_sha256": payload["raw_body_sha256"],
        "status_code": response.status_code,
    }


def _artifact_binding(path: Path, *, output: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "relative_path": path.relative_to(output).as_posix(),
        "byte_size": len(raw),
        "sha256": _sha256_bytes(raw),
    }


def _load_persisted_response(
    *,
    path: Path,
    output: Path,
    method: str,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
) -> tuple[_StoredHttpResponse, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_request = {
        "method": method,
        "url": url,
        "params": params,
        "headers": headers,
        "request_hash": _request_hash(
            method=method, url=url, params=params, headers=headers
        ),
    }
    if payload.get("request") != expected_request:
        raise ProvisionalValidationError(f"persisted PubMed request changed: {path.name}")
    try:
        content = base64.b64decode(payload["content_base64"], validate=True)
    except (KeyError, ValueError) as exc:
        raise ProvisionalValidationError(
            f"persisted PubMed response is invalid: {path.name}"
        ) from exc
    if len(content) != payload.get("raw_body_byte_size") or _sha256_bytes(
        content
    ) != payload.get("raw_body_sha256"):
        raise ProvisionalValidationError(f"persisted PubMed response hash failed: {path.name}")
    status_code = payload.get("status_code")
    if not isinstance(status_code, int) or not 200 <= status_code < 300:
        raise ProvisionalValidationError(
            f"persisted PubMed response was not successful: {path.name}"
        )
    binding = {
        **_artifact_binding(path, output=output),
        "raw_body_byte_size": len(content),
        "raw_body_sha256": payload["raw_body_sha256"],
        "status_code": status_code,
    }
    return (
        _StoredHttpResponse(
            status_code=status_code,
            headers=dict(payload.get("response_headers", {})),
            content=content,
            text=content.decode("utf-8"),
            url=str(payload.get("response_url", url)),
            request_url=str(payload.get("actual_request_url", url)),
        ),
        binding,
    )


def _recovered_attempt(
    *,
    request_sequence: int,
    method: str,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    response_binding: dict[str, Any],
) -> dict[str, Any]:
    return {
        "request_sequence": request_sequence,
        "attempt_number": 1,
        "started_at_utc": None,
        "method": method,
        "url": url,
        "request_hash": _request_hash(
            method=method, url=url, params=params, headers=headers
        ),
        "rate_limit_delay_seconds": None,
        "transport_error": None,
        "response": response_binding,
        "status": "SUCCEEDED",
        "recovered_after_fail_closed_interruption": True,
        "timestamp_limitation": "request start was not persisted before interruption",
    }


def _parse_pubmed_metadata(content: bytes, *, query: str) -> list[Any]:
    """Parse PubMed metadata through the shared article/book-aware parser."""

    return parse_pubmed_fetch(content, query=query)


def _perform_pubmed_request(
    *,
    http: HttpClient,
    method: str,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    response_root: Path,
    response_stem: str,
    request_sequence: int,
    retry_policy: RetryPolicy,
    limiter: RateLimiter,
    retry_sleep: Callable[[float], None],
) -> tuple[HttpResponse, list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    for attempt_number in range(1, retry_policy.max_attempts + 1):
        rate_delay = limiter.wait("PubMed")
        started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        response: HttpResponse | None = None
        transport_error: str | None = None
        try:
            if method == "POST":
                response = http.post(
                    url,
                    data=params,
                    headers=headers or None,
                    timeout=30,
                )
            elif method == "GET":
                response = http.get(
                    url,
                    params=params,
                    headers=headers or None,
                    timeout=30,
                )
            else:
                raise ProvisionalValidationError(f"unsupported PubMed method: {method}")
        except Exception as exc:  # noqa: BLE001 - transport failures are provenance
            transport_error = f"{type(exc).__name__}: {exc}"

        attempt: dict[str, Any] = {
            "request_sequence": request_sequence,
            "attempt_number": attempt_number,
            "started_at_utc": started_at,
            "method": method,
            "url": url,
            "request_hash": _request_hash(
                method=method, url=url, params=params, headers=headers
            ),
            "rate_limit_delay_seconds": rate_delay,
            "transport_error": transport_error,
        }
        if response is not None:
            path = response_root / (
                f"{request_sequence:02d}_{response_stem}_attempt_{attempt_number:02d}.json"
            )
            binding = _persist_response(
                path=path,
                response=response,
                method=method,
                request_url=url,
                request_params=params,
                request_headers=headers,
            )
            binding["relative_path"] = path.relative_to(response_root.parent.parent).as_posix()
            attempt["response"] = binding
            attempt["status"] = (
                "SUCCEEDED" if 200 <= response.status_code < 300 else "HTTP_ERROR"
            )
            attempts.append(attempt)
            if 200 <= response.status_code < 300:
                return response, attempts
            retryable = response.status_code in retry_policy.retry_statuses
            if not retryable:
                raise ProvisionalValidationError(
                    f"PubMed returned non-retryable HTTP {response.status_code}"
                )
            retry_after = next(
                (
                    value
                    for key, value in (response.headers or {}).items()
                    if key.lower() == "retry-after"
                ),
                None,
            )
        else:
            attempt["status"] = "TRANSPORT_ERROR"
            attempts.append(attempt)
            retry_after = None

        if attempt_number < retry_policy.max_attempts:
            delay = retry_policy.delay(attempt_number, retry_after)
            attempt["retry_delay_seconds"] = delay
            retry_sleep(delay)
    raise ProvisionalValidationError(
        f"PubMed request attempts exhausted: sequence {request_sequence} {response_stem}"
    )


def _acm_bindings(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    family_unions: list[dict[str, Any]] = []
    for family in manifest["families"]:
        family_unions.append(
            {
                "family_id": family["family_id"],
                "unique_stable_identity_count": family["field_union"][
                    "unique_stable_identity_count"
                ],
                "stable_identity_union_digest_sha256": family["field_union"][
                    "stable_identity_union_digest_sha256"
                ],
            }
        )
        for child in family["children"]:
            for artifact in child["selected_artifacts"]:
                selected.append(
                    {
                        "family_id": family["family_id"],
                        "child_query_id": child["child_query_id"],
                        "field_key": child["field_key"],
                        "path": artifact["relative_path"],
                        "byte_size": artifact["byte_size"],
                        "raw_sha256": artifact["raw_sha256"],
                        "total_accounted_entry_count": artifact[
                            "total_accounted_entry_count"
                        ],
                        "malformed_entry_count": artifact["malformed_entry_count"],
                        "classification": artifact["classification"],
                    }
                )
    return selected, family_unions


def build_preflight(
    *,
    root: str | Path,
    config_path: str | Path,
    verify_acm_artifacts: bool = True,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Validate every input binding without retrieval, import, inference, or dedupe."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    config_raw = config_file.read_bytes()
    config = load_validation_config(config_file)
    config_hash = _sha256_bytes(config_raw)
    output = resolve_output_namespace(root_path, config["output_namespace"])

    bindings: dict[str, dict[str, Any]] = {}
    binding_paths: dict[str, Path] = {}
    for name, specification in config["bindings"].items():
        path, binding = _bound_file(root_path, specification)
        binding_paths[name] = path
        bindings[name] = binding

    production_plan = load_production_query_plan(
        binding_paths["production_query_plan"], root=root_path
    )
    expected_plan_hash = config["bindings"]["production_query_plan"]["canonical_hash"]
    if production_plan.plan_hash() != expected_plan_hash:
        raise ProvisionalValidationError("frozen production query-plan hash changed")
    pubmed_queries = [
        query
        for query in production_plan.payload["source_queries"]
        if query["source"] == "PubMed"
    ]
    if tuple(query["family_id"] for query in pubmed_queries) != EXPECTED_FAMILIES:
        raise ProvisionalValidationError("frozen PubMed family set or order changed")

    acm_manifest = load_acm_final_reconciliation_manifest(
        binding_paths["acm_final_reconciliation"],
        root=root_path,
        verify_artifacts=verify_acm_artifacts,
    )
    if acm_manifest["manifest_hash"] != config["bindings"][
        "acm_final_reconciliation"
    ]["canonical_hash"]:
        raise ProvisionalValidationError("ACM reconciliation canonical hash changed")
    if acm_manifest["status"] != config["acm"]["source_manifest_status_required"]:
        raise ProvisionalValidationError("ACM evidence is not in the required source state")
    if acm_manifest["readiness"]["production_import_performed"]:
        raise ProvisionalValidationError("ACM production import has already changed state")
    selected_acm, family_unions = _acm_bindings(acm_manifest)

    jfr25 = json.loads(binding_paths["jfr25_seed_manifest"].read_text(encoding="utf-8"))
    _validate_embedded_hash(jfr25, "artifact_hash")
    if jfr25["artifact_hash"] != config["bindings"]["jfr25_seed_manifest"][
        "canonical_hash"
    ]:
        raise ProvisionalValidationError("JFR25 canonical hash changed")
    if len(jfr25.get("entries", [])) != 138 or jfr25.get("occurrences_created") != 0:
        raise ProvisionalValidationError("JFR25 must remain a 138-member unimported comparison set")

    pubmed = config["pubmed"]
    requests_per_family = 1 + (
        pubmed["metadata_sample_size_per_family"]
        + pubmed["metadata_fetch_batch_size"]
        - 1
    ) // pubmed["metadata_fetch_batch_size"]
    timestamp = generated_at_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    report: dict[str, Any] = {
        "schema_version": PREFLIGHT_SCHEMA_VERSION,
        "artifact_class": PREFLIGHT_ARTIFACT_CLASS,
        "plan_id": config["plan_id"],
        "run_id": config["run_id"],
        "generated_at_utc": timestamp,
        "config": {
            "path": config_file.relative_to(root_path).as_posix(),
            "byte_size": len(config_raw),
            "raw_sha256": config_hash,
        },
        "classification": {
            "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
            "production_import_allowed": False,
            "production_completion_claimed": False,
            "retrieval_cutoff": None,
            "disposition": "DISCARD_ONLY",
        },
        "output_namespace": {
            "path": output.relative_to(root_path).as_posix(),
            "must_remain_beneath": ALLOWED_OUTPUT_ROOT.as_posix(),
            "standalone_production_review_dataset_permitted": False,
        },
        "bindings": bindings,
        "pubmed_plan": {
            "query_count": len(pubmed_queries),
            "queries": [
                {
                    "family_id": query["family_id"],
                    "production_query_id": query["query_id"],
                    "query_text_sha256": query["query_text_sha256"],
                    "enumeration_request_method": query["request_specification"][
                        "method"
                    ],
                    "complete_identity_enumeration": {
                        "required": True,
                        "semantic_state_when_reconciled": "COMPLETE_IDENTITY_ENUMERATION",
                        "characterized_as_truncated_due_to_metadata_sampling": False,
                        "maximum_supported_identity_count": pubmed[
                            "maximum_supported_identity_count_per_family"
                        ],
                    },
                    "metadata_acquisition": {
                        "semantic_state": "DETERMINISTIC_SUBSET_PLANNED",
                        "sample_size": pubmed["metadata_sample_size_per_family"],
                        "fetch_batch_size": pubmed["metadata_fetch_batch_size"],
                        "selection_method": pubmed["selection_method"],
                    },
                    "expected_requests_without_retries": requests_per_family,
                }
                for query in pubmed_queries
            ],
            "expected_request_count_without_retries": len(pubmed_queries)
            * requests_per_family,
            "execution_authorization_required": pubmed["authorization_flag"],
        },
        "acm_plan": {
            "source_manifest_status": acm_manifest["status"],
            "selected_artifact_count": len(selected_acm),
            "selected_artifact_accounted_record_count": sum(
                artifact["total_accounted_entry_count"] for artifact in selected_acm
            ),
            "selected_artifact_malformed_record_count": sum(
                artifact["malformed_entry_count"] for artifact in selected_acm
            ),
            "selected_artifacts": selected_acm,
            "family_unions": family_unions,
            "provisional_occurrences_created": 0,
            "execution_authorization_required": config["acm"]["authorization_flag"],
        },
        "candidate_plan": config["candidate_selection"],
        "screening_plan": {
            **config["screening"],
            "inference_attempts_created": 0,
            "screening_decisions_created": 0,
            "corpus_memberships_created": 0,
        },
        "jfr25_plan": {
            **config["jfr25_rediscovery"],
            "validated_member_count": len(jfr25["entries"]),
            "members_with_normalized_doi": sum(
                bool(entry.get("doi")) for entry in jfr25["entries"]
            ),
            "members_without_normalized_doi": sum(
                not entry.get("doi") for entry in jfr25["entries"]
            ),
            "occurrences_created": 0,
        },
        "authorization_boundaries": {
            "preflight": "NO_EXTERNAL_OR_IMPORT_AUTHORIZATION_REQUIRED",
            "pubmed_execution": EXPECTED_AUTHORIZATION_FLAGS["pubmed"],
            "acm_provisional_import": EXPECTED_AUTHORIZATION_FLAGS["acm"],
            "llm_inference": EXPECTED_AUTHORIZATION_FLAGS["llm"],
        },
        "safeguards": {
            "frozen_query_text_modified": False,
            "raw_evidence_modified": False,
            "network_requests_made": 0,
            "acm_provisional_import_performed": False,
            "pubmed_execution_performed": False,
            "llm_inference_performed": False,
            "normalization_or_deduplication_performed": False,
            "prisma_or_corpus_effects": False,
            "prohibited_effects": config["prohibited_effects"],
        },
        "planned_outputs_after_separate_authorization": [
            "run_manifest.json",
            "raw_responses/",
            "provisional_dataset_envelope.json",
            "sample_manifest.json",
            "diagnostics.json",
            "jfr25_rediscovery.json",
            "screening/preflight.json",
            "screening/report.json",
            "screening/review_table.csv",
            "screening/invalid_response_queue.json",
            "screening/human_validation_sample.csv",
            "screening/human_validation_sample_manifest.json",
        ],
    }
    report["preflight_hash"] = _sha256_json(report)
    return report


def _write_json(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    encoded = (json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode()
    atomic_write(path, encoded)
    return {
        "relative_path": path.as_posix(),
        "byte_size": len(encoded),
        "sha256": _sha256_bytes(encoded),
    }


def _verify_json_artifact(output: Path, binding: dict[str, Any]) -> dict[str, Any]:
    relative = Path(binding["relative_path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ProvisionalValidationError("PubMed artifact path escapes its output namespace")
    path = output / relative
    raw = path.read_bytes()
    if len(raw) != binding["byte_size"] or _sha256_bytes(raw) != binding["sha256"]:
        raise ProvisionalValidationError(f"PubMed artifact binding failed: {relative}")
    return json.loads(raw)


def validate_pubmed_execution_artifacts(
    *, root: str | Path, config_path: str | Path
) -> dict[str, Any]:
    """Verify the persisted provisional PubMed run without changing any state."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    config = load_validation_config(config_file)
    output = resolve_output_namespace(root_path, config["output_namespace"])
    execution_path = output / "pubmed/pubmed_execution.json"
    execution_raw = execution_path.read_bytes()
    execution = json.loads(execution_raw)
    _validate_embedded_hash(execution, "execution_hash")
    if execution.get("artifact_class") != PUBMED_EXECUTION_ARTIFACT_CLASS:
        raise ProvisionalValidationError("unexpected PubMed execution artifact class")
    if execution.get("config_raw_sha256") != _sha256_bytes(config_file.read_bytes()):
        raise ProvisionalValidationError("PubMed execution config binding changed")
    classification = execution.get("classification", {})
    if classification.get("retrieval_cutoff") is not None or any(
        classification.get(field) is not False
        for field in (
            "production_import_allowed",
            "production_completion_claimed",
            "production_retrieval_wave_instantiated",
        )
    ):
        raise ProvisionalValidationError("PubMed execution claims production state")
    if any(execution.get("downstream_effects", {}).values()):
        raise ProvisionalValidationError("PubMed execution claims a downstream effect")

    family_count = 0
    response_paths: set[str] = set()
    for family in execution.get("families", []):
        family_count += 1
        family_payload = _verify_json_artifact(output, family["family_artifact"])
        _validate_embedded_hash(family_payload, "manifest_hash")
        identity_payload = _verify_json_artifact(
            output, family["identity_manifest_artifact"]
        )
        _validate_embedded_hash(identity_payload, "manifest_hash")
        enumeration = identity_payload.get("enumeration", {})
        if enumeration.get("semantic_state") != "COMPLETE_IDENTITY_ENUMERATION":
            raise ProvisionalValidationError("PubMed identity enumeration is not complete")
        if enumeration.get("provider_reported_count") != len(
            enumeration.get("pmids_provider_order", [])
        ):
            raise ProvisionalValidationError("PubMed identity manifest count diverged")

    attempts = execution.get("request_accounting", {}).get("attempts", [])
    for attempt in attempts:
        response_binding = attempt.get("response")
        if response_binding is None:
            continue
        response_payload = _verify_json_artifact(output, response_binding)
        raw_body = base64.b64decode(response_payload["content_base64"], validate=True)
        if len(raw_body) != response_payload["raw_body_byte_size"] or _sha256_bytes(
            raw_body
        ) != response_payload["raw_body_sha256"]:
            raise ProvisionalValidationError("persisted PubMed raw response hash failed")
        response_paths.add(response_binding["relative_path"])

    accounting = execution.get("request_accounting", {})
    if family_count != len(EXPECTED_FAMILIES):
        raise ProvisionalValidationError("PubMed execution family count diverged")
    if accounting.get("logical_request_count") != config["pubmed"][
        "expected_request_count_without_retries"
    ]:
        raise ProvisionalValidationError("PubMed logical request count diverged")
    if accounting.get("actual_attempt_count") != len(attempts):
        raise ProvisionalValidationError("PubMed request-attempt accounting diverged")
    if len(response_paths) != sum(
        1 for attempt in attempts if attempt.get("response") is not None
    ):
        raise ProvisionalValidationError("PubMed raw response bindings are not unique")
    return {
        "status": "VERIFIED_PROVISIONAL_PUBMED_EXECUTION",
        "execution_path": (
            Path(config["output_namespace"])
            / execution_path.relative_to(output)
        ).as_posix(),
        "execution_file_byte_size": len(execution_raw),
        "execution_file_sha256": _sha256_bytes(execution_raw),
        "execution_hash": execution["execution_hash"],
        "family_artifact_count": family_count,
        "raw_response_artifact_count": len(response_paths),
        "logical_request_count": accounting["logical_request_count"],
        "actual_attempt_count": accounting["actual_attempt_count"],
        "retry_count": accounting["retry_count"],
    }


def _family_code(family_id: str) -> str:
    for code in ("QF01", "QF02", "QF03", "QF04", "QF05"):
        if code in family_id:
            return code
    raise ProvisionalValidationError(f"unexpected query family: {family_id}")


def _pubmed_overlap(enumerations: list[dict[str, Any]]) -> dict[str, Any]:
    by_family = {
        item["family_id"]: set(item["enumeration"]["pmids_provider_order"])
        for item in enumerations
    }
    all_pmids = set().union(*by_family.values())
    memberships = {
        pmid: sorted(family for family, values in by_family.items() if pmid in values)
        for pmid in sorted(all_pmids, key=int)
    }
    repeated = {
        pmid: families for pmid, families in memberships.items() if len(families) > 1
    }
    pairwise = [
        {
            "left_family_id": left,
            "right_family_id": right,
            "overlap_count": len(by_family[left] & by_family[right]),
        }
        for left, right in combinations(by_family, 2)
    ]
    return {
        "summed_family_identity_count": sum(len(values) for values in by_family.values()),
        "unique_pmid_count_across_families": len(all_pmids),
        "pmids_present_in_multiple_families_count": len(repeated),
        "pmids_present_in_multiple_families": repeated,
        "pairwise_overlap_counts": pairwise,
        "overlap_digest_sha256": _sha256_json(repeated),
    }


def _family_execution_summary(
    family_result: dict[str, Any], family_binding: dict[str, Any]
) -> dict[str, Any]:
    return {
        **{key: value for key, value in family_result.items() if key != "metadata_fetch"},
        "metadata_fetch": {
            key: value
            for key, value in family_result["metadata_fetch"].items()
            if key != "records"
        },
        "family_artifact": family_binding,
    }


def execute_pubmed_boundary(
    *,
    root: str | Path,
    config_path: str | Path,
    http: HttpClient,
    retry_policy: RetryPolicy | None = None,
    limiter: RateLimiter | None = None,
    retry_sleep: Callable[[float], None] = time.sleep,
) -> tuple[dict[str, Any], Path]:
    """Run only complete PMID enumeration plus deterministic sampled metadata fetches."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    config = load_validation_config(config_file)
    preflight = build_preflight(
        root=root_path, config_path=config_file, verify_acm_artifacts=True
    )
    output = resolve_output_namespace(root_path, config["output_namespace"])
    pubmed_root = output / "pubmed"
    execution_path = pubmed_root / "pubmed_execution.json"
    if execution_path.exists():
        raise FileExistsError(
            "provisional PubMed execution is already complete; use a new approved run namespace"
        )
    raw_response_root = pubmed_root / "raw_responses"
    identity_root = pubmed_root / "identity_manifests"
    family_root = pubmed_root / "families"
    if pubmed_root.exists():
        allowed_directories = {pubmed_root, raw_response_root, identity_root, family_root}
        if any(path not in allowed_directories for path in pubmed_root.rglob("*") if path.is_dir()):
            raise ProvisionalValidationError("unexpected directory in partial PubMed output")
        for path in (item for item in pubmed_root.rglob("*") if item.is_file()):
            relative = path.relative_to(pubmed_root).as_posix()
            valid = (
                re.fullmatch(r"identity_manifests/QF0[1-5]\.json", relative)
                or re.fullmatch(r"families/QF0[1-5]\.json", relative)
                or re.fullmatch(
                    r"raw_responses/(0[1-9]|10)_QF0[1-5]_(esearch|efetch)_attempt_0[1-3]\.json",
                    relative,
                )
            )
            if not valid:
                raise ProvisionalValidationError(
                    f"unexpected artifact in partial PubMed output: {relative}"
                )
    raw_response_root.mkdir(parents=True, exist_ok=True)
    identity_root.mkdir(exist_ok=True)
    family_root.mkdir(exist_ok=True)

    config_raw = config_file.read_bytes()
    config_hash = _sha256_bytes(config_raw)
    plan_path = root_path / config["bindings"]["production_query_plan"]["path"]
    plan = load_production_query_plan(plan_path, root=root_path)
    queries = [
        query for query in plan.payload["source_queries"] if query["source"] == "PubMed"
    ]
    if tuple(query["family_id"] for query in queries) != EXPECTED_FAMILIES:
        raise ProvisionalValidationError("frozen PubMed query family set changed")

    validated_requests: list[tuple[dict[str, Any], str, str, dict[str, Any], dict[str, str]]] = []
    for query in queries:
        request = query["request_specification"]
        method = request["method"]
        endpoint = request["endpoint"]
        form = dict(request["form"])
        headers = {str(key): str(value) for key, value in request["headers"].items()}
        if method != "POST" or endpoint != PUBMED_SEARCH_ENDPOINT:
            raise ProvisionalValidationError("frozen PubMed ESearch request shape changed")
        if form.get("term") != query["query_text"]:
            raise ProvisionalValidationError("PubMed form no longer contains the frozen query")
        if _sha256_bytes(query["query_text"].encode()) != query["query_text_sha256"]:
            raise ProvisionalValidationError("frozen PubMed query text hash mismatch")
        if form.get("retmax") != config["pubmed"][
            "maximum_supported_identity_count_per_family"
        ]:
            raise ProvisionalValidationError("PubMed identity enumeration window changed")
        if form.get("usehistory") != "y" or form.get("retmode") != "xml":
            raise ProvisionalValidationError("PubMed complete-enumeration controls changed")
        validated_requests.append((query, method, endpoint, form, headers))

    policy = retry_policy or RetryPolicy()
    rate_limiter = limiter or RateLimiter()
    all_attempts: list[dict[str, Any]] = []
    enumerations: list[dict[str, Any]] = []
    request_sequence = 0
    started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    for query, method, endpoint, form, headers in validated_requests:
        request_sequence += 1
        family_code = _family_code(query["family_id"])
        response_path = raw_response_root / (
            f"{request_sequence:02d}_{family_code}_esearch_attempt_01.json"
        )
        identity_path = identity_root / f"{family_code}.json"
        if response_path.exists():
            response, response_binding = _load_persisted_response(
                path=response_path,
                output=output,
                method=method,
                url=endpoint,
                params=form,
                headers=headers,
            )
            if not identity_path.exists():
                raise ProvisionalValidationError(
                    f"partial PubMed ESearch lacks its identity manifest: {family_code}"
                )
            identity_manifest = json.loads(identity_path.read_text(encoding="utf-8"))
            _validate_embedded_hash(identity_manifest, "manifest_hash")
            attempts = list(identity_manifest.get("request_attempts", []))
            if len(attempts) != 1 or attempts[0].get("response") != response_binding:
                raise ProvisionalValidationError(
                    f"partial PubMed ESearch provenance diverged: {family_code}"
                )
        else:
            if identity_path.exists():
                raise ProvisionalValidationError(
                    f"partial PubMed identity manifest lacks raw response: {family_code}"
                )
            response, attempts = _perform_pubmed_request(
                http=http,
                method=method,
                url=endpoint,
                params=form,
                headers=headers,
                response_root=raw_response_root,
                response_stem=f"{family_code}_esearch",
                request_sequence=request_sequence,
                retry_policy=policy,
                limiter=rate_limiter,
                retry_sleep=retry_sleep,
            )
        enumeration = _parse_pubmed_enumeration(
            bytes(response.content),
            maximum_supported_count=config["pubmed"][
                "maximum_supported_identity_count_per_family"
            ],
        )
        item = {
            "family_id": query["family_id"],
            "family_code": family_code,
            "production_query_id": query["query_id"],
            "query_text_sha256": query["query_text_sha256"],
            "enumeration": enumeration,
            "request_attempts": attempts,
        }
        if identity_path.exists():
            expected_identity = dict(identity_manifest)
            expected_identity.pop("manifest_hash", None)
            actual_identity = {
                "artifact_class": "PROVISIONAL_PUBMED_COMPLETE_IDENTITY_ENUMERATION",
                "run_id": config["run_id"],
                **item,
                "production_completion_claimed": False,
                "retrieval_cutoff": None,
            }
            if expected_identity != actual_identity:
                raise ProvisionalValidationError(
                    f"partial PubMed identity content diverged: {family_code}"
                )
            binding = _artifact_binding(identity_path, output=output)
        else:
            identity_manifest = {
                "artifact_class": "PROVISIONAL_PUBMED_COMPLETE_IDENTITY_ENUMERATION",
                "run_id": config["run_id"],
                **item,
                "production_completion_claimed": False,
                "retrieval_cutoff": None,
            }
            identity_manifest["manifest_hash"] = _sha256_json(identity_manifest)
            binding = _write_json(identity_path, identity_manifest)
            binding["relative_path"] = identity_path.relative_to(output).as_posix()
        item["identity_manifest_artifact"] = binding
        enumerations.append(item)
        all_attempts.extend(attempts)

    overlap = _pubmed_overlap(enumerations)
    selections: dict[str, list[dict[str, Any]]] = {}
    for item in enumerations:
        rows = _selection_rows(
            item["enumeration"]["pmids_provider_order"],
            sample_size=config["pubmed"]["metadata_sample_size_per_family"],
            config_hash=config_hash,
            query_hash=item["query_text_sha256"],
        )
        expected = min(
            config["pubmed"]["metadata_sample_size_per_family"],
            item["enumeration"]["provider_reported_count"],
        )
        if len(rows) != expected or (
            item["enumeration"]["provider_reported_count"] >= 100 and len(rows) != 100
        ):
            raise ProvisionalValidationError("PubMed deterministic sample size diverged")
        selections[item["family_id"]] = rows

    family_results: list[dict[str, Any]] = []
    for query, enumeration_item in zip(queries, enumerations, strict=True):
        family_id = query["family_id"]
        family_code = enumeration_item["family_code"]
        selection = selections[family_id]
        selected_pmids = [row["pmid"] for row in selection]
        request_sequence += 1
        fetch_params = {
            "db": "pubmed",
            "id": ",".join(selected_pmids),
            "retmode": "xml",
            "rettype": "abstract",
        }
        response_path = raw_response_root / (
            f"{request_sequence:02d}_{family_code}_efetch_attempt_01.json"
        )
        family_path = family_root / f"{family_code}.json"
        if response_path.exists():
            response, response_binding = _load_persisted_response(
                path=response_path,
                output=output,
                method="GET",
                url=PUBMED_FETCH_ENDPOINT,
                params=fetch_params,
                headers={},
            )
            if family_path.exists():
                prior_family = json.loads(family_path.read_text(encoding="utf-8"))
                _validate_embedded_hash(prior_family, "manifest_hash")
                attempts = list(
                    prior_family.get("metadata_fetch", {}).get("request_attempts", [])
                )
                if len(attempts) != 1 or attempts[0].get("response") != response_binding:
                    raise ProvisionalValidationError(
                        f"partial PubMed metadata provenance diverged: {family_code}"
                    )
                prior_selection = prior_family.get("metadata_selection", {})
                prior_fetch = prior_family.get("metadata_fetch", {})
                if (
                    prior_family.get("family_id") != family_id
                    or prior_family.get("query_text_sha256")
                    != query["query_text_sha256"]
                    or prior_family.get("identity_manifest_artifact")
                    != enumeration_item["identity_manifest_artifact"]
                    or prior_selection.get("selection_rows") != selection
                    or set(prior_fetch.get("returned_pmid_order", []))
                    != set(selected_pmids)
                    or prior_fetch.get("success_count") != len(selected_pmids)
                    or prior_fetch.get("failure_count") != 0
                ):
                    raise ProvisionalValidationError(
                        f"partial PubMed family content diverged: {family_code}"
                    )
                family_binding = _artifact_binding(family_path, output=output)
                all_attempts.extend(attempts)
                family_results.append(
                    _family_execution_summary(prior_family, family_binding)
                )
                continue
            else:
                attempts = [
                    _recovered_attempt(
                        request_sequence=request_sequence,
                        method="GET",
                        url=PUBMED_FETCH_ENDPOINT,
                        params=fetch_params,
                        headers={},
                        response_binding=response_binding,
                    )
                ]
        else:
            if family_path.exists():
                raise ProvisionalValidationError(
                    f"partial PubMed family artifact lacks raw response: {family_code}"
                )
            response, attempts = _perform_pubmed_request(
                http=http,
                method="GET",
                url=PUBMED_FETCH_ENDPOINT,
                params=fetch_params,
                headers={},
                response_root=raw_response_root,
                response_stem=f"{family_code}_efetch",
                request_sequence=request_sequence,
                retry_policy=policy,
                limiter=rate_limiter,
                retry_sleep=retry_sleep,
            )
        all_attempts.extend(attempts)
        try:
            records = _parse_pubmed_metadata(
                bytes(response.content), query=query["query_text"]
            )
        except ET.ParseError as exc:
            raise ProvisionalValidationError("PubMed EFetch returned malformed XML") from exc
        returned_pmids = [record.pmid or record.source_identifier for record in records]
        if any(not pmid for pmid in returned_pmids):
            raise ProvisionalValidationError("PubMed metadata record lacks a PMID")
        if len(returned_pmids) != len(set(returned_pmids)):
            raise ProvisionalValidationError("PubMed metadata response repeats a PMID")
        if set(returned_pmids) != set(selected_pmids):
            missing = sorted(set(selected_pmids) - set(returned_pmids), key=int)
            unexpected = sorted(set(returned_pmids) - set(selected_pmids), key=int)
            raise ProvisionalValidationError(
                f"PubMed metadata set mismatch; missing={missing!r}, unexpected={unexpected!r}"
            )
        missing_abstract_pmids = sorted(
            (
                record.pmid or str(record.source_identifier)
                for record in records
                if not record.abstract.strip()
            ),
            key=int,
        )
        family_result: dict[str, Any] = {
            "artifact_class": "PROVISIONAL_PUBMED_SAMPLED_METADATA",
            "run_id": config["run_id"],
            "family_id": family_id,
            "family_code": family_code,
            "production_query_id": query["query_id"],
            "query_text_sha256": query["query_text_sha256"],
            "complete_identity_enumeration": {
                key: value
                for key, value in enumeration_item["enumeration"].items()
                if key != "pmids_provider_order"
            },
            "identity_manifest_artifact": enumeration_item["identity_manifest_artifact"],
            "metadata_selection": {
                "state": "DETERMINISTIC_SUBSET_ACQUIRED",
                "selection_method": config["pubmed"]["selection_method"],
                "selection_count": len(selection),
                "selection_rows": selection,
                "selected_pmid_sequence_sha256": _sha256_bytes(
                    "\n".join(selected_pmids).encode()
                ),
            },
            "metadata_fetch": {
                "requested_count": len(selected_pmids),
                "success_count": len(records),
                "failure_count": 0,
                "returned_pmid_order": returned_pmids,
                "missing_abstract_count": len(missing_abstract_pmids),
                "missing_abstract_pmids": missing_abstract_pmids,
                "request_attempts": attempts,
                "records": [record.to_dict() for record in records],
            },
            "classification": {
                "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
                "production_completion_claimed": False,
                "retrieval_cutoff": None,
            },
        }
        family_result["manifest_hash"] = _sha256_json(family_result)
        family_binding = _write_json(family_path, family_result)
        family_binding["relative_path"] = family_path.relative_to(output).as_posix()
        family_results.append(_family_execution_summary(family_result, family_binding))

    selected_memberships: dict[str, list[str]] = {}
    for family_id, rows in selections.items():
        for row in rows:
            selected_memberships.setdefault(row["pmid"], []).append(family_id)
    selected_duplicates = {
        pmid: sorted(families)
        for pmid, families in sorted(selected_memberships.items(), key=lambda item: int(item[0]))
        if len(families) > 1
    }
    completed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    execution: dict[str, Any] = {
        "schema_version": "1.0.0",
        "artifact_class": PUBMED_EXECUTION_ARTIFACT_CLASS,
        "plan_id": config["plan_id"],
        "run_id": config["run_id"],
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "config_raw_sha256": config_hash,
        "frozen_production_query_plan_hash": plan.plan_hash(),
        "classification": {
            "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
            "production_import_allowed": False,
            "production_completion_claimed": False,
            "retrieval_cutoff": None,
            "production_retrieval_wave_instantiated": False,
            "disposition": "DISCARD_ONLY",
        },
        "request_accounting": {
            "logical_request_count": request_sequence,
            "actual_attempt_count": len(all_attempts),
            "retry_count": len(all_attempts) - request_sequence,
            "expected_logical_request_count": config["pubmed"][
                "expected_request_count_without_retries"
            ],
            "attempts": all_attempts,
        },
        "complete_identity_enumeration_overlap": overlap,
        "selected_sample_overlap": {
            "duplicate_pmid_count": len(selected_duplicates),
            "duplicate_pmids": selected_duplicates,
            "digest_sha256": _sha256_json(selected_duplicates),
        },
        "families": family_results,
        "downstream_effects": {
            "acm_imported": False,
            "cross_source_deduplication": False,
            "llm_inference": False,
            "screening": False,
            "jfr25_comparison": False,
            "prisma": False,
            "corpus_membership": False,
        },
        "pre_execution_preflight_hash": preflight["preflight_hash"],
    }
    if request_sequence != config["pubmed"]["expected_request_count_without_retries"]:
        raise ProvisionalValidationError("PubMed logical request accounting diverged")
    execution["execution_hash"] = _sha256_json(execution)
    execution_path = pubmed_root / "pubmed_execution.json"
    _write_json(execution_path, execution)
    return execution, execution_path


def _provisional_record(fields: dict[str, str]) -> dict[str, Any]:
    parsed = record_from_bibtex_fields(fields)
    return {
        "title": parsed.title,
        "abstract": parsed.abstract,
        "authors": parsed.authors,
        "year": parsed.year,
        "doi": parsed.doi,
        "source_identifier": fields.get("_key"),
        "source_database": "ACMDigitalLibrary",
        "source_url": parsed.source_url,
        "journal": parsed.journal,
    }


def _load_selected_acm_occurrences(
    *, root: Path, manifest: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    occurrences: list[dict[str, Any]] = []
    artifact_summaries: list[dict[str, Any]] = []
    for family in manifest["families"]:
        family_code = _family_code(family["family_id"])
        for child in family["children"]:
            for artifact in child["selected_artifacts"]:
                relative = Path(artifact["relative_path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise ProvisionalValidationError("ACM selected artifact path is unsafe")
                path = root / relative
                raw = path.read_bytes()
                if len(raw) != artifact["byte_size"] or _sha256_bytes(raw) != artifact[
                    "raw_sha256"
                ]:
                    raise ProvisionalValidationError(
                        f"ACM selected artifact binding failed: {relative}"
                    )
                text = raw.decode("utf-8")
                diagnostic = parse_bibtex_with_diagnostics(text)
                if (
                    diagnostic.physical_header_count != artifact["physical_header_count"]
                    or diagnostic.accounted_record_count
                    != artifact["total_accounted_entry_count"]
                    or len(diagnostic.issues) != artifact["malformed_entry_count"]
                ):
                    raise ProvisionalValidationError(
                        f"ACM parser accounting changed: {relative}"
                    )
                artifact_issue_count = 0
                for ordinal, raw_entry in enumerate(split_bib_entries(text), start=1):
                    item = parse_bibtex_with_diagnostics(raw_entry)
                    issue = item.issues[0] if item.issues else None
                    fields = item.entries[0] if item.entries else issue.partial_fields
                    if issue is not None:
                        artifact_issue_count += 1
                    native_id = fields.get("_key") or (issue.key if issue else None)
                    raw_entry_hash = _sha256_bytes(raw_entry.encode("utf-8"))
                    occurrence_id = "provisional-acm-occurrence:" + _sha256_bytes(
                        (
                            f"{artifact['raw_sha256']}\x1f{ordinal}\x1f"
                            f"{native_id or raw_entry_hash}"
                        ).encode()
                    )[:24]
                    record = _provisional_record(fields)
                    record["source_identifier"] = native_id
                    occurrences.append(
                        {
                            "occurrence_id": occurrence_id,
                            "route": "ACM",
                            "source_database": "ACMDigitalLibrary",
                            "family_id": family["family_id"],
                            "family_code": family_code,
                            "child_query_id": child["child_query_id"],
                            "field_key": child["field_key"],
                            "artifact_relative_path": relative.as_posix(),
                            "artifact_sha256": artifact["raw_sha256"],
                            "artifact_record_ordinal": ordinal,
                            "source_identifier": native_id,
                            "raw_entry_sha256": raw_entry_hash,
                            "malformed": issue is not None,
                            "parse_issue": (
                                {
                                    "code": issue.code,
                                    "message": issue.message,
                                    "brace_depth": issue.brace_depth,
                                    "native_id": issue.key,
                                }
                                if issue
                                else None
                            ),
                            "record": record,
                        }
                    )
                if artifact_issue_count != artifact["malformed_entry_count"]:
                    raise ProvisionalValidationError(
                        f"ACM malformed record count changed: {relative}"
                    )
                artifact_summaries.append(
                    {
                        "family_id": family["family_id"],
                        "child_query_id": child["child_query_id"],
                        "field_key": child["field_key"],
                        **artifact,
                        "provisional_occurrence_count": diagnostic.accounted_record_count,
                    }
                )
    if len(occurrences) != 11664:
        raise ProvisionalValidationError("ACM selected occurrence total changed")
    malformed = sum(item["malformed"] for item in occurrences)
    if malformed != 3:
        raise ProvisionalValidationError("ACM malformed occurrence total changed")
    return occurrences, {
        "selected_artifact_count": len(artifact_summaries),
        "selected_artifact_occurrence_count": len(occurrences),
        "malformed_but_identified_occurrence_count": malformed,
        "selected_artifacts": artifact_summaries,
        "nonselected_preserved_artifacts": manifest[
            "nonselected_preserved_bibtex_artifacts"
        ],
    }


def _load_sampled_pubmed_occurrences(
    *, output: Path, execution: dict[str, Any]
) -> list[dict[str, Any]]:
    occurrences: list[dict[str, Any]] = []
    for family in execution["families"]:
        payload = _verify_json_artifact(output, family["family_artifact"])
        _validate_embedded_hash(payload, "manifest_hash")
        family_code = family["family_code"]
        for rank, record in enumerate(payload["metadata_fetch"]["records"], start=1):
            pmid = str(record.get("pmid") or record.get("source_identifier") or "")
            if not pmid:
                raise ProvisionalValidationError("sampled PubMed record lacks PMID")
            occurrences.append(
                {
                    "occurrence_id": "provisional-pubmed-occurrence:"
                    + _sha256_bytes(
                        f"{family['family_id']}\x1f{pmid}".encode()
                    )[:24],
                    "route": "PubMed",
                    "source_database": "PubMed",
                    "family_id": family["family_id"],
                    "family_code": family_code,
                    "child_query_id": family["production_query_id"],
                    "field_key": None,
                    "artifact_relative_path": family["family_artifact"]["relative_path"],
                    "artifact_sha256": family["family_artifact"]["sha256"],
                    "artifact_record_ordinal": rank,
                    "source_identifier": pmid,
                    "raw_entry_sha256": _sha256_json(record),
                    "malformed": False,
                    "parse_issue": None,
                    "record": record,
                }
            )
    if len(occurrences) != 500:
        raise ProvisionalValidationError("PubMed sampled occurrence total changed")
    return occurrences


def _canonicalize_provisional(
    occurrences: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    decisions: list[dict[str, Any]] = []
    doi_titles: dict[str, set[str]] = {}
    unresolved: list[str] = []
    for occurrence in occurrences:
        record = occurrence["record"]
        key = dedupe_key(doi=record.get("doi"), title=record.get("title"))
        if key.startswith("doi:"):
            match_rule = "normalized_doi"
            title = normalize_title(record.get("title"))
            if title:
                doi_titles.setdefault(title, set()).add(key)
        elif key.startswith("title:"):
            match_rule = "exact_normalized_title_fallback"
        else:
            key = f"occurrence:{occurrence['occurrence_id']}"
            match_rule = "occurrence_fallback_missing_doi_and_title"
            unresolved.append(occurrence["occurrence_id"])
        canonical = by_key.get(key)
        outcome = "DUPLICATE" if canonical is not None else "UNIQUE"
        if canonical is None:
            canonical = {
                "canonical_id": "provisional-canonical:"
                + _sha256_bytes(key.encode())[:24],
                "dedupe_key": key,
                "match_basis": match_rule,
                "representative_record": record,
                "occurrences": [],
            }
            by_key[key] = canonical
        canonical["occurrences"].append(
            {key: value for key, value in occurrence.items() if key != "record"}
        )
        decisions.append(
            {
                "occurrence_id": occurrence["occurrence_id"],
                "canonical_id": canonical["canonical_id"],
                "outcome": outcome,
                "match_key": key,
                "match_rule": match_rule,
            }
        )
    canonical_records = list(by_key.values())
    title_ambiguities = [
        {
            "canonical_id": item["canonical_id"],
            "normalized_title": item["dedupe_key"].removeprefix("title:"),
            "doi_identity_count_with_same_title": len(
                doi_titles.get(item["dedupe_key"].removeprefix("title:"), set())
            ),
        }
        for item in canonical_records
        if item["dedupe_key"].startswith("title:")
        and doi_titles.get(item["dedupe_key"].removeprefix("title:"))
    ]
    route_sets = [
        tuple(sorted({item["route"] for item in canonical["occurrences"]}))
        for canonical in canonical_records
    ]
    family_sets = [
        tuple(sorted({item["family_code"] for item in canonical["occurrences"]}))
        for canonical in canonical_records
    ]
    route_patterns: dict[str, int] = {}
    family_patterns: dict[str, int] = {}
    for routes in route_sets:
        label = "+".join(routes)
        route_patterns[label] = route_patterns.get(label, 0) + 1
    for families in family_sets:
        label = "+".join(families)
        family_patterns[label] = family_patterns.get(label, 0) + 1
    duplicate_decisions = [item for item in decisions if item["outcome"] == "DUPLICATE"]
    stats = {
        "occurrence_count": len(occurrences),
        "canonical_identity_count": len(canonical_records),
        "duplicate_occurrence_count": len(duplicate_decisions),
        "doi_match_duplicate_count": sum(
            item["match_rule"] == "normalized_doi" for item in duplicate_decisions
        ),
        "title_fallback_duplicate_count": sum(
            item["match_rule"] == "exact_normalized_title_fallback"
            for item in duplicate_decisions
        ),
        "unresolved_missing_doi_and_title_count": len(unresolved),
        "unresolved_occurrence_ids": unresolved,
        "title_fallback_ambiguity_count": len(title_ambiguities),
        "title_fallback_ambiguities": title_ambiguities,
        "route_membership_patterns": dict(sorted(route_patterns.items())),
        "query_family_membership_patterns": dict(sorted(family_patterns.items())),
        "acm_pubmed_exact_identity_overlap": route_patterns.get("ACM+PubMed", 0),
    }
    return canonical_records, decisions, stats


def _build_validation_cohort(
    *, canonical_records: list[dict[str, Any]], config_hash: str, target: int
) -> dict[str, Any]:
    by_id = {item["canonical_id"]: item for item in canonical_records}
    strata: dict[str, dict[str, Any]] = {}
    selected_by: dict[str, list[str]] = {}
    for family_code in ("QF01", "QF02", "QF03", "QF04", "QF05"):
        for route in ("ACM", "PubMed"):
            stratum_id = f"{family_code}|{route}"
            eligible = [
                item["canonical_id"]
                for item in canonical_records
                if any(
                    occurrence["family_code"] == family_code
                    and occurrence["route"] == route
                    for occurrence in item["occurrences"]
                )
            ]
            ranked = sorted(
                eligible,
                key=lambda canonical_id: (
                    _sha256_bytes(
                        f"{config_hash}\x1f{stratum_id}\x1f{canonical_id}".encode()
                    ),
                    canonical_id,
                ),
            )
            if len(ranked) < 50:
                raise ProvisionalValidationError(
                    f"provisional stratum {stratum_id} has fewer than 50 identities"
                )
            chosen = ranked[:50]
            for canonical_id in chosen:
                selected_by.setdefault(canonical_id, []).append(stratum_id)
            strata[stratum_id] = {
                "eligible_identity_count": len(eligible),
                "quota": 50,
                "selected_canonical_ids": chosen,
                "selected_identity_digest_sha256": _sha256_bytes(
                    "\n".join(chosen).encode()
                ),
            }
    quota_union = set(selected_by)
    remaining = sorted(
        (canonical_id for canonical_id in by_id if canonical_id not in quota_union),
        key=lambda canonical_id: (
            _sha256_bytes(f"{config_hash}\x1f{canonical_id}".encode()),
            canonical_id,
        ),
    )
    if len(by_id) < target:
        raise ProvisionalValidationError("fewer than 750 provisional identities exist")
    final_ids = quota_union | set(remaining[: target - len(quota_union)])
    if len(final_ids) != target:
        raise ProvisionalValidationError("provisional cohort did not reach exactly 750")
    rows = [
        {
            "canonical_id": canonical_id,
            "global_selection_sha256": _sha256_bytes(
                f"{config_hash}\x1f{canonical_id}".encode()
            ),
            "selected_by_strata": sorted(selected_by.get(canonical_id, [])),
            "selected_as_global_fill": canonical_id not in quota_union,
            "representative_record": by_id[canonical_id]["representative_record"],
            "occurrences": by_id[canonical_id]["occurrences"],
        }
        for canonical_id in sorted(
            final_ids,
            key=lambda value: (
                _sha256_bytes(f"{config_hash}\x1f{value}".encode()),
                value,
            ),
        )
    ]
    return {
        "artifact_class": "PROVISIONAL_DETERMINISTIC_VALIDATION_COHORT",
        "target_count": target,
        "actual_count": len(rows),
        "selection_inputs": ["config_raw_sha256", "canonical_id", "stratum_id"],
        "content_or_eligibility_inspected_for_selection": False,
        "jfr25_membership_inspected_for_selection": False,
        "quota_union_count": len(quota_union),
        "global_fill_count": target - len(quota_union),
        "strata": strata,
        "cohort_identity_digest_sha256": _sha256_bytes(
            "\n".join(sorted(final_ids)).encode()
        ),
        "records": rows,
    }


def _jfr25_matches(
    *,
    entries: list[dict[str, Any]],
    canonical_records: list[dict[str, Any]],
    cohort_ids: set[str],
) -> list[dict[str, Any]]:
    doi_index: dict[str, str] = {}
    title_index: dict[str, list[str]] = {}
    by_id = {item["canonical_id"]: item for item in canonical_records}
    for item in canonical_records:
        record = item["representative_record"]
        doi = normalize_doi(record.get("doi"))
        title = normalize_title(record.get("title"))
        if doi:
            doi_index[doi] = item["canonical_id"]
        if title:
            title_index.setdefault(title, []).append(item["canonical_id"])
    results: list[dict[str, Any]] = []
    for entry in entries:
        doi = normalize_doi(entry.get("doi"))
        title = normalize_title(entry.get("title"))
        canonical_id: str | None = None
        method: str | None = None
        ambiguity = 0
        if doi:
            canonical_id = doi_index.get(doi)
            method = "exact_normalized_doi" if canonical_id else None
        elif title:
            candidates = title_index.get(title, [])
            ambiguity = len(candidates)
            if len(candidates) == 1:
                canonical_id = candidates[0]
                method = "unique_exact_normalized_title"
        canonical = by_id.get(canonical_id) if canonical_id else None
        memberships = (
            sorted(
                {
                    f"{item['family_code']}|{item['route']}"
                    for item in canonical["occurrences"]
                }
            )
            if canonical
            else []
        )
        results.append(
            {
                "entry_id": entry["entry_id"],
                "source_member_id": entry["source_member_id"],
                "doi": doi,
                "normalized_title": title,
                "match_status": "REDISCOVERED" if canonical_id else "NOT_REDISCOVERED",
                "match_method": method,
                "title_candidate_count_when_doi_unavailable": ambiguity if not doi else None,
                "canonical_id": canonical_id,
                "source_family_route_memberships": memberships,
                "in_750_cohort": canonical_id in cohort_ids if canonical_id else False,
            }
        )
    return results


def execute_offline_validation_stage(
    *, root: str | Path, config_path: str | Path
) -> tuple[dict[str, Any], Path]:
    """Import and compare only isolated, already-preserved local evidence."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    config = load_validation_config(config_file)
    preflight = build_preflight(
        root=root_path, config_path=config_file, verify_acm_artifacts=True
    )
    pubmed_verification = validate_pubmed_execution_artifacts(
        root=root_path, config_path=config_file
    )
    output = resolve_output_namespace(root_path, config["output_namespace"])
    offline_root = output / "offline"
    if offline_root.exists():
        raise FileExistsError("provisional offline stage already exists")

    acm_binding = config["bindings"]["acm_final_reconciliation"]
    acm_path, acm_file_binding = _bound_file(root_path, acm_binding)
    acm_manifest = load_acm_final_reconciliation_manifest(
        acm_path, root=root_path, verify_artifacts=True
    )
    acm_occurrences, acm_accounting = _load_selected_acm_occurrences(
        root=root_path, manifest=acm_manifest
    )
    pubmed_execution_path = output / "pubmed/pubmed_execution.json"
    pubmed_execution = json.loads(pubmed_execution_path.read_text(encoding="utf-8"))
    _validate_embedded_hash(pubmed_execution, "execution_hash")
    pubmed_occurrences = _load_sampled_pubmed_occurrences(
        output=output, execution=pubmed_execution
    )

    acm_canonical, _, acm_stats = _canonicalize_provisional(acm_occurrences)
    pubmed_canonical, _, pubmed_stats = _canonicalize_provisional(pubmed_occurrences)
    combined_canonical, decisions, combined_stats = _canonicalize_provisional(
        acm_occurrences + pubmed_occurrences
    )
    config_hash = _sha256_bytes(config_file.read_bytes())
    cohort = _build_validation_cohort(
        canonical_records=combined_canonical,
        config_hash=config_hash,
        target=config["candidate_selection"]["target_canonical_records"],
    )

    jfr_path, jfr_binding = _bound_file(
        root_path, config["bindings"]["jfr25_seed_manifest"]
    )
    jfr_manifest = json.loads(jfr_path.read_text(encoding="utf-8"))
    if (
        jfr_manifest.get("status") != "POPULATED_VALIDATED_NOT_IMPORTED"
        or len(jfr_manifest.get("entries", [])) != 138
        or jfr_manifest.get("occurrences_created") != 0
    ):
        raise ProvisionalValidationError("JFR25 comparison binding is not validated")
    matches = _jfr25_matches(
        entries=jfr_manifest["entries"],
        canonical_records=combined_canonical,
        cohort_ids={item["canonical_id"] for item in cohort["records"]},
    )
    metadata_rediscovered = sum(item["canonical_id"] is not None for item in matches)
    cohort_rediscovered = sum(item["in_750_cohort"] for item in matches)
    enumerated_unique = pubmed_execution["complete_identity_enumeration_overlap"][
        "unique_pmid_count_across_families"
    ]
    sampled_unique_pmids = len(
        {item["source_identifier"] for item in pubmed_occurrences}
    )

    base_classification = {
        "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
        "production_import_allowed": False,
        "production_completion_claimed": False,
        "retrieval_cutoff": None,
        "production_retrieval_wave_instantiated": False,
        "disposition": "DISCARD_ONLY",
    }
    acm_payload = {
        "artifact_class": "PROVISIONAL_ACM_IMPORT_ACCOUNTING",
        "classification": base_classification,
        "acm_manifest_binding": acm_file_binding,
        **acm_accounting,
        "actual_unique_canonical_acm_identity_count": len(acm_canonical),
        "canonicalization_statistics": acm_stats,
        "occurrences": acm_occurrences,
    }
    acm_payload["artifact_hash"] = _sha256_json(acm_payload)
    offline_root.mkdir(parents=True)
    acm_artifact = _write_json(offline_root / "acm_import_accounting.json", acm_payload)
    acm_artifact["relative_path"] = (
        offline_root / "acm_import_accounting.json"
    ).relative_to(output).as_posix()

    canonical_payload = {
        "artifact_class": "PROVISIONAL_LOCAL_CANONICALIZATION",
        "classification": base_classification,
        "rule": "normalized DOI first; exact normalized title fallback; occurrence fallback",
        "acm": acm_stats,
        "pubmed_sample": pubmed_stats,
        "combined": combined_stats,
        "canonical_records": combined_canonical,
        "duplicate_decisions": decisions,
    }
    canonical_payload["artifact_hash"] = _sha256_json(canonical_payload)
    canonical_artifact = _write_json(
        offline_root / "provisional_canonicalization.json", canonical_payload
    )
    canonical_artifact["relative_path"] = (
        offline_root / "provisional_canonicalization.json"
    ).relative_to(output).as_posix()

    cohort.update({"classification": base_classification, "config_raw_sha256": config_hash})
    cohort["artifact_hash"] = _sha256_json(cohort)
    cohort_artifact = _write_json(
        offline_root / "validation_cohort_750.json", cohort
    )
    cohort_artifact["relative_path"] = (
        offline_root / "validation_cohort_750.json"
    ).relative_to(output).as_posix()

    rediscovery = {
        "artifact_class": "PROVISIONAL_JFR25_REDISCOVERY_DIAGNOSTIC",
        "classification": base_classification,
        "jfr25_manifest_binding": jfr_binding,
        "jfr25_occurrences_created": 0,
        "terminology": "rediscovery diagnostic; not recall or precision",
        "matching_rules": {
            "doi_bearing_member": "exact normalized DOI only",
            "doi_unavailable_member": "unique exact normalized title only",
            "fuzzy_or_semantic_matching": False,
        },
        "full_provisional_acquisition_universe": {
            "acm_selected_occurrence_count": len(acm_occurrences),
            "acm_canonical_identity_count": len(acm_canonical),
            "complete_pubmed_enumerated_unique_pmid_count": enumerated_unique,
            "pubmed_metadata_bearing_unique_pmid_count": sampled_unique_pmids,
            "pubmed_enumeration_only_pmid_count": enumerated_unique
            - sampled_unique_pmids,
            "rediscovered_member_count_detectable_with_available_metadata": metadata_rediscovered,
            "limitation": (
                "enumeration-only PMIDs have no DOI/title metadata and cannot be assessed "
                "under the approved exact identity rules"
            ),
        },
        "combined_metadata_bearing_universe": {
            "canonical_identity_count": len(combined_canonical),
            "rediscovered_member_count": metadata_rediscovered,
        },
        "deterministic_750_cohort": {
            "canonical_identity_count": len(cohort["records"]),
            "rediscovered_member_count": cohort_rediscovered,
        },
        "members": matches,
    }
    rediscovery["artifact_hash"] = _sha256_json(rediscovery)
    rediscovery_artifact = _write_json(
        offline_root / "jfr25_rediscovery.json", rediscovery
    )
    rediscovery_artifact["relative_path"] = (
        offline_root / "jfr25_rediscovery.json"
    ).relative_to(output).as_posix()

    summary = {
        "schema_version": "1.0.0",
        "artifact_class": OFFLINE_STAGE_ARTIFACT_CLASS,
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "classification": base_classification,
        "preflight_hash": preflight["preflight_hash"],
        "pubmed_execution_verification": pubmed_verification,
        "inputs": {
            "config_raw_sha256": config_hash,
            "acm_final_reconciliation": acm_file_binding,
            "pubmed_execution_file_sha256": _sha256_bytes(
                pubmed_execution_path.read_bytes()
            ),
            "jfr25_seed_manifest": jfr_binding,
        },
        "results": {
            "raw_acm_selected_occurrences": len(acm_occurrences),
            "unique_acm_identities": len(acm_canonical),
            "pubmed_sampled_occurrences": len(pubmed_occurrences),
            "unique_pubmed_sampled_identities": len(pubmed_canonical),
            "acm_pubmed_exact_identity_overlap": combined_stats[
                "acm_pubmed_exact_identity_overlap"
            ],
            "combined_canonical_identity_count": len(combined_canonical),
            "doi_match_duplicate_count": combined_stats["doi_match_duplicate_count"],
            "title_fallback_duplicate_count": combined_stats[
                "title_fallback_duplicate_count"
            ],
            "unresolved_missing_identity_count": combined_stats[
                "unresolved_missing_doi_and_title_count"
            ],
            "cohort_count": len(cohort["records"]),
            "jfr25_metadata_universe_rediscovered": metadata_rediscovered,
            "jfr25_cohort_rediscovered": cohort_rediscovered,
        },
        "artifacts": {
            "acm_import_accounting": acm_artifact,
            "canonicalization": canonical_artifact,
            "validation_cohort": cohort_artifact,
            "jfr25_rediscovery": rediscovery_artifact,
        },
        "prohibited_effects": {
            "network_requests": 0,
            "llm_inference": False,
            "production_retrieval_cutoff": None,
            "production_retrieval_wave": False,
            "production_review_dataset": False,
            "authoritative_screening_or_adjudication": False,
            "prisma": False,
            "corpus_membership": False,
            "production_deduplication_or_identification_closure": False,
            "jfr25_occurrences": 0,
        },
    }
    summary["artifact_hash"] = _sha256_json(summary)
    summary_path = offline_root / "offline_stage_summary.json"
    _write_json(summary_path, summary)
    return summary, summary_path


def _screening_classification() -> dict[str, Any]:
    return {
        "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
        "artifact_state": SCREENING_ARTIFACT_CLASS,
        "production_import_allowed": False,
        "production_completion_claimed": False,
        "authoritative_screening_or_adjudication": False,
        "retrieval_cutoff": None,
        "production_retrieval_wave_instantiated": False,
        "disposition": "DISCARD_ONLY",
    }


def _verify_offline_stage_inputs(
    *, root: Path, config_path: Path
) -> tuple[dict[str, Any], Path, dict[str, Any], dict[str, Any]]:
    """Revalidate the 750 cohort and every artifact bound by its upstream summary."""

    config = load_validation_config(config_path)
    build_preflight(root=root, config_path=config_path, verify_acm_artifacts=True)
    validate_pubmed_execution_artifacts(root=root, config_path=config_path)
    output = resolve_output_namespace(root, config["output_namespace"])
    summary_path = output / "offline/offline_stage_summary.json"
    summary_raw = summary_path.read_bytes()
    summary = json.loads(summary_raw)
    _validate_embedded_hash(summary, "artifact_hash")
    if summary.get("artifact_class") != OFFLINE_STAGE_ARTIFACT_CLASS:
        raise ProvisionalValidationError("unexpected provisional offline-stage artifact")
    if summary.get("classification") != {
        "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
        "production_import_allowed": False,
        "production_completion_claimed": False,
        "retrieval_cutoff": None,
        "production_retrieval_wave_instantiated": False,
        "disposition": "DISCARD_ONLY",
    }:
        raise ProvisionalValidationError("offline stage no longer has discard-only classification")
    if any(summary.get("prohibited_effects", {}).get(key) not in (False, 0, None) for key in summary.get("prohibited_effects", {})):
        raise ProvisionalValidationError("offline stage claims a prohibited production effect")

    verified: dict[str, Any] = {}
    for name, binding in summary.get("artifacts", {}).items():
        payload = _verify_json_artifact(output, binding)
        _validate_embedded_hash(payload, "artifact_hash")
        verified[name] = payload
    if set(verified) != {
        "acm_import_accounting",
        "canonicalization",
        "validation_cohort",
        "jfr25_rediscovery",
    }:
        raise ProvisionalValidationError("offline-stage artifact bindings are incomplete")

    cohort = verified["validation_cohort"]
    records = cohort.get("records", [])
    identities = [str(item.get("canonical_id", "")) for item in records]
    if (
        cohort.get("artifact_class") != "PROVISIONAL_DETERMINISTIC_VALIDATION_COHORT"
        or cohort.get("actual_count") != 750
        or len(identities) != 750
        or len(set(identities)) != 750
    ):
        raise ProvisionalValidationError("the provisional validation cohort is not exactly 750 unique records")
    if cohort.get("cohort_identity_digest_sha256") != _sha256_bytes(
        "\n".join(sorted(identities)).encode()
    ):
        raise ProvisionalValidationError("the provisional cohort identity digest changed")
    canonical_ids = {
        item["canonical_id"] for item in verified["canonicalization"]["canonical_records"]
    }
    if not set(identities) <= canonical_ids:
        raise ProvisionalValidationError("cohort identities are absent from canonicalization")
    return (
        config,
        output,
        {
            "relative_path": summary_path.relative_to(output).as_posix(),
            "byte_size": len(summary_raw),
            "raw_sha256": _sha256_bytes(summary_raw),
            "artifact_hash": summary["artifact_hash"],
        },
        cohort,
    )


def _stage5d_model_configuration(
    *, root: Path, config: dict[str, Any]
) -> tuple[dict[str, Any], Any, dict[str, Any]]:
    from h2h_lit import inference as inference_core
    from h2h_lit.pilot import load_pilot_config
    from h2h_lit.pilot5d import load_pilot5d_config
    from h2h_lit.review import ScreeningStage

    stage5d_path, stage5d_binding = _bound_file(root, config["bindings"]["stage5d_config"])
    prompt_path, prompt_binding = _bound_file(root, config["bindings"]["stage5d_prompt"])
    stage5d = load_pilot5d_config(stage5d_path)
    if Path(stage5d["prompt_path"]).as_posix() != config["bindings"]["stage5d_prompt"]["path"]:
        raise ProvisionalValidationError("Stage 5D prompt binding changed")
    prompt = inference_core.load_prompt_artifact(
        prompt_path,
        stage=ScreeningStage.TITLE_ABSTRACT,
        version=stage5d["prompt_version"],
        output_schema_version=stage5d["output_schema_version"],
    )
    if prompt.content_hash != prompt_binding["raw_sha256"]:
        raise ProvisionalValidationError("Stage 5D prompt content hash changed")
    base = load_pilot_config(root / stage5d["selection_config"])
    model = json.loads(json.dumps(base["model"]))
    model["parameters"].update(stage5d["model_parameter_overrides"])
    if model["parameters"].get("response_schema_version") != config["screening"][
        "output_schema_version"
    ]:
        raise ProvisionalValidationError("Stage 5D schema binding changed")
    retry_limit = int(base["retry_limit_per_record"])
    if retry_limit != 1:
        raise ProvisionalValidationError("the frozen Stage 5D one-retry policy changed")
    return model, prompt, {
        "stage5d_config": stage5d_binding,
        "stage5d_prompt": prompt_binding,
        "retry_limit_per_record": retry_limit,
        "retry_conditions": list(stage5d["execution_controls"]["retry_conditions"]),
        "execution_controls": dict(stage5d["execution_controls"]),
    }


def _build_screening_sample_manifest(
    *, cohort: dict[str, Any], config_hash: str, prompt_hash: str, sample_size: int
) -> dict[str, Any]:
    identities = [item["canonical_id"] for item in cohort["records"]]
    salt = _sha256_bytes(
        f"{config_hash}\x1f{cohort['artifact_hash']}\x1f{prompt_hash}\x1fstage5d".encode()
    )
    selected = deterministic_identity_sample(
        identities, sample_size=sample_size, salt=salt
    )
    if len(selected) != sample_size:
        raise ProvisionalValidationError("Stage 5D sample did not reach exactly 250 records")
    rows = [
        {
            "selection_rank": rank,
            "canonical_id": canonical_id,
            "selection_sha256": _sha256_bytes(
                f"{salt}\x1f{canonical_id}".encode()
            ),
        }
        for rank, canonical_id in enumerate(selected, start=1)
    ]
    manifest: dict[str, Any] = {
        "schema_version": "1.0.0",
        "artifact_class": SCREENING_SAMPLE_CLASS,
        "classification": _screening_classification(),
        "cohort_artifact_hash": cohort["artifact_hash"],
        "cohort_identity_digest_sha256": cohort["cohort_identity_digest_sha256"],
        "selection_rule": "lowest_sha256_of_frozen_config_cohort_prompt_stage_and_canonical_identity",
        "selection_inputs": [
            "provisional_config_raw_sha256",
            "cohort_artifact_hash",
            "stage5d_prompt_hash",
            "literal_stage5d_salt",
            "canonical_id",
        ],
        "selection_salt_sha256": salt,
        "content_fields_inspected_for_selection": [],
        "source_or_family_inspected_for_selection": False,
        "jfr25_or_prior_llm_result_inspected_for_selection": False,
        "target_count": sample_size,
        "actual_count": len(rows),
        "records": rows,
        "ordered_identity_digest_sha256": _sha256_bytes(
            "\n".join(selected).encode()
        ),
    }
    manifest["artifact_hash"] = _sha256_json(manifest)
    return manifest


def build_screening_preflight(
    *,
    root: str | Path,
    config_path: str | Path,
    generated_at_utc: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    """Prepare and fully verify a no-call Stage 5D sample and exposure report."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    config, output, offline_binding, cohort = _verify_offline_stage_inputs(
        root=root_path, config_path=config_file
    )
    model, prompt, stage5d = _stage5d_model_configuration(root=root_path, config=config)
    config_raw = config_file.read_bytes()
    config_hash = _sha256_bytes(config_raw)
    sample = _build_screening_sample_manifest(
        cohort=cohort,
        config_hash=config_hash,
        prompt_hash=prompt.content_hash,
        sample_size=int(config["screening"]["proposal_sample_size"]),
    )
    by_id = {item["canonical_id"]: item for item in cohort["records"]}
    from h2h_lit.models import LiteratureRecord
    from h2h_lit.pilot5d import pilot5d_inference_input_for

    input_characters = 0
    missing_abstracts = 0
    for selected in sample["records"]:
        row = by_id[selected["canonical_id"]]
        record_data = row["representative_record"]
        record = SimpleNamespace(
            canonical_id=row["canonical_id"],
            record=LiteratureRecord(
                title=str(record_data.get("title") or ""),
                abstract=str(record_data.get("abstract") or ""),
                year=record_data.get("year"),
                doi=record_data.get("doi"),
                source_identifier=record_data.get("source_identifier"),
                original_metadata={},
            ),
        )
        inference_input = pilot5d_inference_input_for(
            record,
            pilot_execution_date=(generated_at_utc or datetime.now(UTC).isoformat())[:10],
        )
        inference_input.metadata.pop("doi", None)
        inference_input.metadata.pop("source_identifier", None)
        input_characters += len(prompt.content) + len(
            _canonical_json(inference_input.to_snapshot())
        )
        missing_abstracts += not bool(record.record.abstract.strip())

    minimum_calls = len(sample["records"])
    maximum_calls = minimum_calls * (stage5d["retry_limit_per_record"] + 1)
    approximate_first_input_tokens = (input_characters + 3) // 4
    approximate_max_input_tokens = approximate_first_input_tokens * 2
    output_token_maximum = maximum_calls * int(model["parameters"]["max_output_tokens"])
    prices = model["pricing_usd_per_million_tokens"]
    cost_upper = (
        approximate_max_input_tokens * prices["input"]
        + output_token_maximum * prices["output"]
    ) / 1_000_000
    timestamp = generated_at_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    preflight: dict[str, Any] = {
        "schema_version": "1.0.0",
        "artifact_class": "PROVISIONAL_STAGE5D_SCREENING_PREFLIGHT",
        "classification": _screening_classification(),
        "generated_at_utc": timestamp,
        "config_binding": {
            "path": config_file.relative_to(root_path).as_posix(),
            "byte_size": len(config_raw),
            "raw_sha256": config_hash,
        },
        "offline_stage_binding": offline_binding,
        "cohort_binding": {
            "relative_path": "offline/validation_cohort_750.json",
            "artifact_hash": cohort["artifact_hash"],
            "identity_digest_sha256": cohort["cohort_identity_digest_sha256"],
            "record_count": len(cohort["records"]),
        },
        "sample": {
            "record_count": len(sample["records"]),
            "artifact_hash": sample["artifact_hash"],
            "ordered_identity_digest_sha256": sample[
                "ordered_identity_digest_sha256"
            ],
            "missing_abstract_count": missing_abstracts,
        },
        "stage5d": stage5d,
        "model": model,
        "prompt": {
            "path": prompt.path,
            "version": prompt.version,
            "output_schema_version": prompt.output_schema_version,
            "sha256": prompt.content_hash,
        },
        "execution_exposure": {
            "expected_attempts_without_retries": minimum_calls,
            "maximum_attempts_with_one_retry_each": maximum_calls,
            "approximate_first_attempt_input_tokens": approximate_first_input_tokens,
            "approximate_maximum_input_tokens": approximate_max_input_tokens,
            "hard_maximum_output_tokens": output_token_maximum,
            "estimated_cost_upper_bound_usd": round(cost_upper, 6),
            "cost_assumptions": "four characters per input token, no cached-input discount, every record consumes its one retry and full output-token cap",
            "serial_runtime_planning_range_minutes": [25, 75],
            "runtime_assumption": "approximately 6-18 seconds per successful provider call; actual retries and provider latency govern",
        },
        "authorization": {
            "required_flag": config["screening"]["authorization_flag"],
            "model_calls_made": 0,
        },
        "safeguards": {
            "retrieval_requests": 0,
            "production_retrieval_wave": False,
            "production_review_dataset": False,
            "authoritative_screening_or_adjudication": False,
            "production_deduplication_or_identification_closure": False,
            "retrieval_cutoff": None,
            "prisma": False,
            "corpus_membership": False,
            "jfr25_ebk25_fp19_occurrences": 0,
            "full_text_screening": False,
        },
    }
    preflight["artifact_hash"] = _sha256_json(preflight)
    return preflight, sample, output


def write_screening_preflight(
    *, preflight: dict[str, Any], sample: dict[str, Any], output: Path
) -> tuple[Path, Path]:
    screening_root = output / "screening"
    sample_path = screening_root / "inference_sample_manifest.json"
    preflight_path = screening_root / "preflight.json"
    if sample_path.exists() or preflight_path.exists():
        raise FileExistsError("Stage 5D screening preflight already exists")
    screening_root.mkdir(parents=True, exist_ok=True)
    sample_artifact = _write_json(sample_path, sample)
    sample_artifact["relative_path"] = sample_path.relative_to(output).as_posix()
    preflight["sample_artifact"] = sample_artifact
    preflight.pop("artifact_hash", None)
    preflight["artifact_hash"] = _sha256_json(preflight)
    _write_json(preflight_path, preflight)
    return preflight_path, sample_path


def _stage5d_input_for_cohort_record(
    *, row: dict[str, Any], pilot_execution_date: str
) -> Any:
    from h2h_lit.models import LiteratureRecord
    from h2h_lit.pilot5d import pilot5d_inference_input_for

    data = row["representative_record"]
    record = SimpleNamespace(
        canonical_id=row["canonical_id"],
        record=LiteratureRecord(
            title=str(data.get("title") or ""),
            abstract=str(data.get("abstract") or ""),
            authors=list(data.get("authors") or []),
            year=data.get("year"),
            doi=data.get("doi"),
            source_identifier=data.get("source_identifier"),
            journal=data.get("journal"),
            original_metadata={},
        ),
    )
    inference_input = pilot5d_inference_input_for(
        record, pilot_execution_date=pilot_execution_date
    )
    inference_input.metadata.pop("doi", None)
    inference_input.metadata.pop("source_identifier", None)
    inference_input.metadata.update(
        {
            "source_identity_exposed_to_model": False,
            "query_family_or_route_exposed_to_model": False,
            "provisional_validation_only": True,
        }
    )
    return inference_input


def _serialize_stage5d_proposal(parsed: Any) -> dict[str, Any]:
    from h2h_lit.screening import derive_eligibility_status

    criteria: dict[str, Any] = {}
    for criterion, value in parsed.criterion_values.items():
        criteria[criterion.value] = {
            "decision": value.value,
            "rationale": parsed.criterion_rationales[criterion],
            "evidence": [
                {
                    "start": span.start,
                    "end": span.end,
                    "quote": span.quote,
                    "locator": span.locator,
                    "source_field": span.source_field,
                    "claimed_start": span.claimed_start,
                    "claimed_end": span.claimed_end,
                    "resolution_method": span.resolution_method,
                }
                for span in parsed.criterion_evidence[criterion]
            ],
        }
    status = derive_eligibility_status(parsed.criterion_values)
    return {
        "eligibility_status": status.value,
        "reporting_label": {
            "ELIGIBLE": "INCLUDE",
            "EXCLUDED": "EXCLUDE",
            "UNCERTAIN": "UNCERTAIN",
        }[status.value],
        "criteria": criteria,
        "primary_exclusion_reason": (
            parsed.primary_exclusion_reason.value
            if parsed.primary_exclusion_reason is not None
            else None
        ),
        "secondary_exclusion_reasons": [
            item.value for item in parsed.secondary_exclusion_reasons
        ],
        "overall_rationale": parsed.overall_rationale,
        "audit_flags": list(parsed.audit_flags),
        "requires_full_text_escalation": status.value == "UNCERTAIN",
    }


def _load_attempt_artifact(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    _validate_embedded_hash(payload, "artifact_hash")
    if payload.get("artifact_class") != "PROVISIONAL_STAGE5D_MODEL_ATTEMPT":
        raise ProvisionalValidationError("unexpected provisional model-attempt artifact")
    if payload.get("classification") != _screening_classification():
        raise ProvisionalValidationError("model attempt is not discard-only")
    return payload


def _run_stage5d_record(
    *,
    provider: Any,
    prompt: Any,
    model: dict[str, Any],
    run_id: str,
    row: dict[str, Any],
    pilot_execution_date: str,
    raw_root: Path,
    retry_limit: int = 1,
) -> list[dict[str, Any]]:
    """Run or resume one record using the frozen Stage 5D parser and retry rule."""

    from h2h_lit.pilot5d import parse_pilot5d_proposal

    inference_input = _stage5d_input_for_cohort_record(
        row=row, pilot_execution_date=pilot_execution_date
    )
    snapshot = inference_input.to_snapshot()
    input_hash = _sha256_json(snapshot)
    request_id = "provisional-inference-request:" + _sha256_bytes(
        f"{run_id}\x1f{input_hash}".encode()
    )[:24]
    request_slug = request_id.rsplit(":", 1)[-1]
    attempts: list[dict[str, Any]] = []
    for attempt_number in range(1, retry_limit + 2):
        path = raw_root / f"{request_slug}_attempt_{attempt_number:03d}.json"
        if path.exists():
            attempt = _load_attempt_artifact(path)
            if (
                attempt.get("request_id") != request_id
                or attempt.get("attempt_number") != attempt_number
                or attempt.get("input_hash") != input_hash
                or attempt.get("run_id") != run_id
            ):
                raise ProvisionalValidationError("persisted model attempt binding changed")
            attempts.append(attempt)
        else:
            started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            started_clock = time.monotonic()
            raw_response = ""
            parsed_response: dict[str, Any] | None = None
            proposal: dict[str, Any] | None = None
            failure_reason: str | None = None
            provider_metadata: dict[str, Any] = {}
            try:
                raw_response = provider.generate(
                    model=model["name"],
                    prompt=prompt.content,
                    input_snapshot=snapshot,
                    parameters=dict(model["parameters"]),
                    request_id=request_id,
                    attempt_number=attempt_number,
                )
                metadata_for = getattr(provider, "metadata_for", None)
                if callable(metadata_for):
                    provider_metadata = metadata_for(request_id, attempt_number)
                loaded = json.loads(raw_response)
                if not isinstance(loaded, dict):
                    raise TypeError("top-level response must be a JSON object")
                parsed_response = loaded
                proposal = _serialize_stage5d_proposal(
                    parse_pilot5d_proposal(loaded, inference_input)
                )
            except Exception as exc:  # noqa: BLE001 - failures are evidence
                metadata_for = getattr(provider, "metadata_for", None)
                if callable(metadata_for):
                    provider_metadata = metadata_for(request_id, attempt_number)
                prefix = (
                    "provider error"
                    if not raw_response
                    else "output validation error"
                )
                failure_reason = f"{prefix}: {type(exc).__name__}: {exc}"
            ended_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            attempt = {
                "schema_version": "1.0.0",
                "artifact_class": "PROVISIONAL_STAGE5D_MODEL_ATTEMPT",
                "classification": _screening_classification(),
                "run_id": run_id,
                "request_id": request_id,
                "attempt_id": f"{request_id}:attempt:{attempt_number}",
                "attempt_number": attempt_number,
                "retry_of_attempt_id": (
                    attempts[-1]["attempt_id"] if attempts else None
                ),
                "canonical_id": row["canonical_id"],
                "started_at_utc": started_at,
                "ended_at_utc": ended_at,
                "wall_clock_seconds": round(time.monotonic() - started_clock, 6),
                "input_hash": input_hash,
                "input_snapshot": snapshot,
                "model_visible_source_identity": False,
                "audit_source_provenance": {
                    "occurrences": row["occurrences"],
                    "representative_source_database": row["representative_record"].get(
                        "source_database"
                    ),
                },
                "prompt": {
                    "path": prompt.path,
                    "version": prompt.version,
                    "sha256": prompt.content_hash,
                    "output_schema_version": prompt.output_schema_version,
                },
                "model": model,
                "raw_response": raw_response,
                "parsed_response": parsed_response,
                "proposal": proposal,
                "validation_state": "VALID" if proposal is not None else "INVALID",
                "failure_reason": failure_reason,
                "provider_response": provider_metadata,
            }
            attempt["artifact_hash"] = _sha256_json(attempt)
            _write_json(path, attempt)
            attempts.append(attempt)
        if attempts[-1]["validation_state"] == "VALID":
            break
    return attempts


def _ranked_ids(ids: Iterable[str], *, salt: str) -> list[str]:
    return sorted(
        set(ids),
        key=lambda value: (_sha256_bytes(f"{salt}\x1f{value}".encode()), value),
    )


def _round_robin_records(
    records: list[dict[str, Any]], *, quota: int, salt: str, bucket: Callable[[dict[str, Any]], str]
) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in records:
        buckets[bucket(item)].append(item)
    for name, items in buckets.items():
        items.sort(
            key=lambda item: (
                _sha256_bytes(f"{salt}\x1f{name}\x1f{item['canonical_id']}".encode()),
                item["canonical_id"],
            )
        )
    chosen: list[dict[str, Any]] = []
    names = sorted(buckets)
    while len(chosen) < quota and any(buckets.values()):
        for name in names:
            if buckets[name] and len(chosen) < quota:
                chosen.append(buckets[name].pop(0))
    return chosen


def _build_human_validation_sample(
    *,
    proposals: list[dict[str, Any]],
    records_by_id: dict[str, dict[str, Any]],
    quotas: dict[str, int],
    salt: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in proposals:
        by_status[item["proposal"]["eligibility_status"]].append(item)
    selected: list[dict[str, Any]] = []
    initial_counts: dict[str, int] = {}
    for status in ("ELIGIBLE", "EXCLUDED", "UNCERTAIN"):
        available = by_status.get(status, [])
        quota = int(quotas[status])
        if status == "EXCLUDED":
            chosen = _round_robin_records(
                available,
                quota=min(quota, len(available)),
                salt=f"{salt}\x1fEXCLUDED",
                bucket=lambda item: str(
                    item["proposal"].get("primary_exclusion_reason") or "NO_REASON"
                ),
            )
        elif status == "UNCERTAIN":
            def uncertain_bucket(item: dict[str, Any]) -> str:
                criteria = sorted(
                    key.split("_", 1)[0]
                    for key, value in item["proposal"]["criteria"].items()
                    if value["decision"] == "UNCERTAIN"
                )
                missing = not bool(
                    records_by_id[item["canonical_id"]]["representative_record"]
                    .get("abstract", "")
                    .strip()
                )
                return f"{'|'.join(criteria) or 'NONE'}|missing_abstract={missing}"

            chosen = _round_robin_records(
                available,
                quota=min(quota, len(available)),
                salt=f"{salt}\x1fUNCERTAIN",
                bucket=uncertain_bucket,
            )
        else:
            chosen_ids = _ranked_ids(
                (item["canonical_id"] for item in available),
                salt=f"{salt}\x1fELIGIBLE",
            )[:quota]
            indexed = {item["canonical_id"]: item for item in available}
            chosen = [indexed[item] for item in chosen_ids]
        selected.extend(chosen)
        initial_counts[status] = len(chosen)

    target = sum(quotas.values())
    selected_ids = {item["canonical_id"] for item in selected}
    fill_pool = [
        item for item in proposals if item["canonical_id"] not in selected_ids
    ]
    fill_ids = _ranked_ids(
        (item["canonical_id"] for item in fill_pool), salt=f"{salt}\x1fredistribution"
    )[: max(0, target - len(selected))]
    fill_index = {item["canonical_id"]: item for item in fill_pool}
    selected.extend(fill_index[item] for item in fill_ids)
    selected.sort(
        key=lambda item: (
            _sha256_bytes(f"{salt}\x1ffinal\x1f{item['canonical_id']}".encode()),
            item["canonical_id"],
        )
    )
    final_counts = Counter(item["proposal"]["eligibility_status"] for item in selected)
    manifest = {
        "target_count": target,
        "actual_count": len(selected),
        "requested_status_quotas": quotas,
        "initial_quota_counts": initial_counts,
        "redistributed_count": len(fill_ids),
        "final_status_counts": dict(sorted(final_counts.items())),
        "excluded_selection": "round_robin_primary_exclusion_reason",
        "uncertain_selection": "round_robin_uncertain_criteria_and_missing_abstract_state",
        "terminal_invalid_responses_included": False,
        "blinded_csv_exposes_model_proposal": False,
        "ordered_identity_digest_sha256": _sha256_bytes(
            "\n".join(item["canonical_id"] for item in selected).encode()
        ),
    }
    return selected, manifest


def _screening_breakdown(
    *, finals: list[dict[str, Any]], records_by_id: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    families: dict[str, Counter[str]] = defaultdict(Counter)
    routes: dict[str, Counter[str]] = defaultdict(Counter)
    for item in finals:
        label = (
            item["proposal"]["eligibility_status"]
            if item.get("proposal")
            else "INVALID"
        )
        row = records_by_id[item["canonical_id"]]
        for family in sorted({occurrence["family_code"] for occurrence in row["occurrences"]}):
            families[family][label] += 1
        for route in sorted({occurrence["route"] for occurrence in row["occurrences"]}):
            routes[route][label] += 1
    return {
        "query_family_nonexclusive": {
            key: dict(sorted(value.items())) for key, value in sorted(families.items())
        },
        "route_nonexclusive": {
            key: dict(sorted(value.items())) for key, value in sorted(routes.items())
        },
    }


def execute_provisional_screening_stage(
    *, root: str | Path, config_path: str | Path, provider: Any
) -> tuple[dict[str, Any], Path]:
    """Run only isolated eligibility proposals against the persisted 250 sample."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    expected_preflight, expected_sample, output = build_screening_preflight(
        root=root_path, config_path=config_file
    )
    screening_root = output / "screening"
    preflight_path = screening_root / "preflight.json"
    sample_path = screening_root / "inference_sample_manifest.json"
    if not preflight_path.is_file() or not sample_path.is_file():
        raise ProvisionalValidationError("screening preflight must be persisted before inference")
    preflight_raw = preflight_path.read_bytes()
    sample_raw = sample_path.read_bytes()
    preflight = json.loads(preflight_raw)
    sample = json.loads(sample_raw)
    _validate_embedded_hash(preflight, "artifact_hash")
    _validate_embedded_hash(sample, "artifact_hash")
    sample_binding = preflight.get("sample_artifact", {})
    if (
        sample_binding.get("byte_size") != len(sample_raw)
        or sample_binding.get("sha256") != _sha256_bytes(sample_raw)
        or sample.get("artifact_hash") != expected_sample.get("artifact_hash")
        or sample.get("ordered_identity_digest_sha256")
        != expected_sample.get("ordered_identity_digest_sha256")
    ):
        raise ProvisionalValidationError("persisted Stage 5D sample binding changed")
    for field in ("config_binding", "offline_stage_binding", "cohort_binding", "model", "prompt", "stage5d"):
        if preflight.get(field) != expected_preflight.get(field):
            raise ProvisionalValidationError(f"persisted screening preflight changed: {field}")

    config, _, _, cohort = _verify_offline_stage_inputs(
        root=root_path, config_path=config_file
    )
    model, prompt, stage5d = _stage5d_model_configuration(root=root_path, config=config)
    records_by_id = {item["canonical_id"]: item for item in cohort["records"]}
    selected = [records_by_id[item["canonical_id"]] for item in sample["records"]]
    run_id = "provisional-stage5d:" + _sha256_bytes(
        f"{sample['artifact_hash']}\x1f{prompt.content_hash}\x1f{model['name']}\x1f{_sha256_json(model['parameters'])}".encode()
    )[:24]
    raw_root = screening_root / "raw_model_responses"
    raw_root.mkdir(parents=True, exist_ok=True)
    started_clock = time.monotonic()
    run_started_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    pilot_date = run_started_at[:10]
    all_attempts: list[dict[str, Any]] = []
    finals: list[dict[str, Any]] = []
    for row in selected:
        attempts = _run_stage5d_record(
            provider=provider,
            prompt=prompt,
            model=model,
            run_id=run_id,
            row=row,
            pilot_execution_date=pilot_date,
            raw_root=raw_root,
            retry_limit=stage5d["retry_limit_per_record"],
        )
        all_attempts.extend(attempts)
        finals.append(attempts[-1])
    run_ended_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    wall_seconds = round(time.monotonic() - started_clock, 6)

    valid_finals = [item for item in finals if item["validation_state"] == "VALID"]
    invalid_finals = [item for item in finals if item["validation_state"] != "VALID"]
    proposals = [
        {
            "canonical_id": item["canonical_id"],
            "request_id": item["request_id"],
            "final_attempt_id": item["attempt_id"],
            "attempt_count": item["attempt_number"],
            "proposal": item["proposal"],
            "audit_source_provenance": item["audit_source_provenance"],
        }
        for item in valid_finals
    ]
    status_counts = Counter(item["proposal"]["eligibility_status"] for item in proposals)
    primary_reasons = Counter(
        item["proposal"]["primary_exclusion_reason"]
        for item in proposals
        if item["proposal"]["primary_exclusion_reason"]
    )
    all_reasons: Counter[str] = Counter()
    uncertain_criteria: Counter[str] = Counter()
    for item in proposals:
        proposal = item["proposal"]
        for reason in [
            proposal["primary_exclusion_reason"],
            *proposal["secondary_exclusion_reasons"],
        ]:
            if reason:
                all_reasons[reason] += 1
        for criterion, value in proposal["criteria"].items():
            if value["decision"] == "UNCERTAIN":
                uncertain_criteria[criterion] += 1
    evidence_failures = [
        {
            "attempt_id": item["attempt_id"],
            "canonical_id": item["canonical_id"],
            "failure_reason": item["failure_reason"],
        }
        for item in all_attempts
        if item.get("failure_reason")
        and any(
            token in item["failure_reason"].casefold()
            for token in ("evidence", "quote", "locator", "substring", "rationale")
        )
    ]
    input_tokens = output_tokens = cached_tokens = 0
    for item in all_attempts:
        usage = item.get("provider_response", {}).get("provider_usage") or {}
        input_tokens += int(usage.get("input_tokens", 0) or 0)
        output_tokens += int(usage.get("output_tokens", 0) or 0)
        cached_tokens += int(
            (usage.get("input_tokens_details") or {}).get("cached_tokens", 0) or 0
        )
    prices = model["pricing_usd_per_million_tokens"]
    cost = (
        (input_tokens - cached_tokens) * prices["input"]
        + cached_tokens * prices["cached_input"]
        + output_tokens * prices["output"]
    ) / 1_000_000
    attempt_bindings = []
    for item in all_attempts:
        slug = item["request_id"].rsplit(":", 1)[-1]
        path = raw_root / f"{slug}_attempt_{item['attempt_number']:03d}.json"
        raw = path.read_bytes()
        attempt_bindings.append(
            {
                "attempt_id": item["attempt_id"],
                "relative_path": path.relative_to(output).as_posix(),
                "byte_size": len(raw),
                "sha256": _sha256_bytes(raw),
                "validation_state": item["validation_state"],
            }
        )

    base = {
        "schema_version": "1.0.0",
        "classification": _screening_classification(),
        "run_id": run_id,
        "sample_artifact_hash": sample["artifact_hash"],
        "prompt": preflight["prompt"],
        "model": model,
    }
    attempt_index = {
        **base,
        "artifact_class": "PROVISIONAL_STAGE5D_MODEL_ATTEMPT_INDEX",
        "attempt_count": len(all_attempts),
        "attempt_artifacts": attempt_bindings,
    }
    attempt_index["artifact_hash"] = _sha256_json(attempt_index)
    attempts_artifact = _write_json(screening_root / "model_attempts.json", attempt_index)

    proposal_payload = {
        **base,
        "artifact_class": "PROVISIONAL_STAGE5D_VALIDATED_PROPOSALS",
        "valid_final_proposal_count": len(proposals),
        "proposals": proposals,
    }
    proposal_payload["artifact_hash"] = _sha256_json(proposal_payload)
    proposals_artifact = _write_json(
        screening_root / "validated_proposals.json", proposal_payload
    )

    invalid_queue = {
        **base,
        "artifact_class": "PROVISIONAL_STAGE5D_TERMINAL_INVALID_QUEUE",
        "terminal_invalid_count": len(invalid_finals),
        "records": [
            {
                "canonical_id": item["canonical_id"],
                "request_id": item["request_id"],
                "final_attempt_id": item["attempt_id"],
                "attempt_count": item["attempt_number"],
                "failure_reason": item["failure_reason"],
            }
            for item in invalid_finals
        ],
    }
    invalid_queue["artifact_hash"] = _sha256_json(invalid_queue)
    invalid_artifact = _write_json(
        screening_root / "invalid_response_queue.json", invalid_queue
    )

    human_rows, human_detail = _build_human_validation_sample(
        proposals=proposals,
        records_by_id=records_by_id,
        quotas=config["human_validation_sample"]["status_quotas"],
        salt=_sha256_bytes(f"{sample['artifact_hash']}\x1fhuman-validation".encode()),
    )
    human_manifest = {
        **base,
        "artifact_class": "PROVISIONAL_BLINDED_HUMAN_VALIDATION_SAMPLE_MANIFEST",
        **human_detail,
        "records": [
            {
                "sample_order": index,
                "canonical_id": item["canonical_id"],
                "model_stratification_status": item["proposal"]["eligibility_status"],
                "primary_exclusion_reason_for_sampling": item["proposal"].get(
                    "primary_exclusion_reason"
                ),
                "uncertain_criteria_for_sampling": sorted(
                    criterion
                    for criterion, value in item["proposal"]["criteria"].items()
                    if value["decision"] == "UNCERTAIN"
                ),
            }
            for index, item in enumerate(human_rows, start=1)
        ],
        "blinding_instruction": "Do not provide this manifest to the human coder until the blinded CSV has been completed.",
    }
    human_manifest["artifact_hash"] = _sha256_json(human_manifest)
    human_manifest_artifact = _write_json(
        screening_root / "human_validation_sample_manifest.json", human_manifest
    )

    csv_buffer = io.StringIO(newline="")
    fieldnames = [
        "artifact_class",
        "production_import_allowed",
        "disposition",
        "sample_order",
        "canonical_id",
        "title",
        "abstract",
        "publication_year",
        "human_E1",
        "human_E2",
        "human_E3",
        "human_E4",
        "human_E5",
        "human_E6",
        "human_E7",
        "human_eligibility_status",
        "human_primary_exclusion_reason",
        "human_notes",
    ]
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for index, item in enumerate(human_rows, start=1):
        record = records_by_id[item["canonical_id"]]["representative_record"]
        writer.writerow(
            {
                "artifact_class": SCREENING_ARTIFACT_CLASS,
                "production_import_allowed": "false",
                "disposition": "DISCARD_ONLY",
                "sample_order": index,
                "canonical_id": item["canonical_id"],
                "title": record.get("title") or "",
                "abstract": record.get("abstract") or "",
                "publication_year": record.get("year") or "",
                "human_E1": "",
                "human_E2": "",
                "human_E3": "",
                "human_E4": "",
                "human_E5": "",
                "human_E6": "",
                "human_E7": "",
                "human_eligibility_status": "",
                "human_primary_exclusion_reason": "",
                "human_notes": "",
            }
        )
    csv_raw = csv_buffer.getvalue().encode("utf-8")
    csv_path = screening_root / "human_validation_sample.csv"
    atomic_write(csv_path, csv_raw)
    csv_artifact = {
        "relative_path": csv_path.relative_to(output).as_posix(),
        "byte_size": len(csv_raw),
        "sha256": _sha256_bytes(csv_raw),
        "model_proposal_status_column_present": False,
    }

    denomin = len(proposals) or 1
    report = {
        **base,
        "artifact_class": SCREENING_RUN_CLASS,
        "run_started_at_utc": run_started_at,
        "run_ended_at_utc": run_ended_at,
        "wall_clock_seconds": wall_seconds,
        "selected_record_count": len(selected),
        "total_model_attempts": len(all_attempts),
        "first_attempt_successes": sum(
            item["validation_state"] == "VALID" and item["attempt_number"] == 1
            for item in finals
        ),
        "retry_successes": sum(
            item["validation_state"] == "VALID" and item["attempt_number"] == 2
            for item in finals
        ),
        "terminal_invalid_or_unresolved_responses": len(invalid_finals),
        "valid_final_proposals": len(proposals),
        "proposal_distribution": {
            label: {
                "count": status_counts.get(status, 0),
                "percentage_among_valid": round(
                    100 * status_counts.get(status, 0) / denomin, 4
                ),
            }
            for status, label in (
                ("ELIGIBLE", "INCLUDE"),
                ("EXCLUDED", "EXCLUDE"),
                ("UNCERTAIN", "UNCERTAIN"),
            )
        },
        "primary_exclusion_reason_frequencies": dict(sorted(primary_reasons.items())),
        "all_exclusion_reason_frequencies": dict(sorted(all_reasons.items())),
        "uncertain_criterion_frequencies": dict(sorted(uncertain_criteria.items())),
        "full_text_escalation_count": status_counts.get("UNCERTAIN", 0),
        "missing_abstract_count": sum(
            not bool(row["representative_record"].get("abstract", "").strip())
            for row in selected
        ),
        "proposal_or_evidence_validation_failure_count": len(evidence_failures),
        "proposal_or_evidence_validation_failures": evidence_failures,
        "breakdowns": _screening_breakdown(
            finals=finals, records_by_id=records_by_id
        ),
        "usage": {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_tokens,
            "output_tokens": output_tokens,
            "estimated_cost_usd": round(cost, 6),
            "pricing_usd_per_million_tokens": prices,
        },
        "human_validation_sample": human_detail,
        "methodological_limitations": [
            "This balanced provisional validation sample is not a prevalence estimate for the retrieved literature.",
            "Every result is a non-authoritative Stage 5D eligibility proposal.",
            "E6 is deterministic and pilot-only; it does not establish a production retrieval cutoff.",
        ],
        "artifacts": {
            "attempt_index": attempts_artifact,
            "validated_proposals": proposals_artifact,
            "invalid_response_queue": invalid_artifact,
            "human_validation_sample_csv": csv_artifact,
            "human_validation_sample_manifest": human_manifest_artifact,
        },
        "prohibited_effects": {
            "retrieval_requests": 0,
            "production_retrieval_wave": False,
            "production_review_dataset": False,
            "authoritative_screening_or_adjudication": False,
            "production_deduplication_or_identification_closure": False,
            "retrieval_cutoff": None,
            "prisma": False,
            "corpus_membership": False,
            "seed_occurrences": 0,
        },
    }
    report["artifact_hash"] = _sha256_json(report)
    report_path = screening_root / "screening_report.json"
    _write_json(report_path, report)
    return report, report_path


def write_preflight(report: dict[str, Any], *, root: str | Path) -> Path:
    """Persist only the preflight inside its guarded ignored namespace."""

    root_path = Path(root).resolve()
    output = resolve_output_namespace(root_path, report["output_namespace"]["path"])
    path = output / "preflight.json"
    if path.exists():
        raise FileExistsError("preflight already exists; use a new provisional run namespace")
    encoded = (
        json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    atomic_write(path, encoded)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/star_provisional_pipeline_validation_v1.json"),
    )
    parser.add_argument("--output", type=Path)
    boundary = parser.add_mutually_exclusive_group(required=True)
    boundary.add_argument("--preflight", action="store_true")
    boundary.add_argument("--authorize-pubmed-execution", action="store_true")
    boundary.add_argument("--authorize-acm-provisional-import", action="store_true")
    boundary.add_argument("--screening-preflight", action="store_true")
    boundary.add_argument("--authorize-llm-inference", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    config = load_validation_config(
        args.config if args.config.is_absolute() else root / args.config
    )
    if args.output is not None:
        requested = args.output
        if requested.is_absolute():
            try:
                requested = requested.resolve().relative_to(root)
            except ValueError as exc:
                raise ProvisionalValidationError(
                    "output must be inside the repository"
                ) from exc
        if requested.as_posix() != config["output_namespace"]:
            raise ProvisionalValidationError("output must equal the configured namespace")
    if args.preflight:
        report = build_preflight(root=root, config_path=args.config)
        path = write_preflight(report, root=root)
        print(path.relative_to(root).as_posix())
        print(report["preflight_hash"])
        return 0

    if args.authorize_acm_provisional_import:
        summary, path = execute_offline_validation_stage(
            root=root, config_path=args.config
        )
        print(path.relative_to(root).as_posix())
        print(summary["artifact_hash"])
        return 0

    if args.screening_preflight:
        preflight, sample, output = build_screening_preflight(
            root=root, config_path=args.config
        )
        preflight_path, sample_path = write_screening_preflight(
            preflight=preflight, sample=sample, output=output
        )
        print(preflight_path.relative_to(root).as_posix())
        print(sample_path.relative_to(root).as_posix())
        print(preflight["artifact_hash"])
        return 0

    if args.authorize_llm_inference:
        from h2h_lit.openai_provider import OpenAIResponsesProvider

        report, path = execute_provisional_screening_stage(
            root=root,
            config_path=args.config,
            provider=OpenAIResponsesProvider.from_environment(),
        )
        print(path.relative_to(root).as_posix())
        print(report["artifact_hash"])
        return 0

    execution, path = execute_pubmed_boundary(
        root=root,
        config_path=args.config,
        http=RequestsHttpClient(),
    )
    print(path.relative_to(root).as_posix())
    print(execution["execution_hash"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
