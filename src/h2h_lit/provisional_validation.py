"""Fail-closed planning and preflight for the isolated STAR validation run."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import combinations
from pathlib import Path
from typing import Any

from h2h_lit.acm_field_execution import load_acm_final_reconciliation_manifest
from h2h_lit.checkpoint import atomic_write
from h2h_lit.http import HttpClient, HttpResponse, RequestsHttpClient
from h2h_lit.pagination import RateLimiter, RetryPolicy, redact_url
from h2h_lit.production_query_plan import load_production_query_plan
from h2h_lit.sources.common import make_record
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


def _element_text(element: ET.Element | None) -> str:
    return "" if element is None else "".join(element.itertext()).strip()


def _parse_pubmed_metadata(content: bytes, *, query: str) -> list[Any]:
    """Use the repository parser and account for valid PubmedBookArticle records."""

    records = parse_pubmed_fetch(content, query=query)
    root = ET.fromstring(content)
    for item in root.findall(".//PubmedBookArticle"):
        document = item.find("BookDocument")
        if document is None:
            continue
        pmid = document.findtext("PMID")
        title = _element_text(document.find(".//BookTitle"))
        abstract = " ".join(
            text
            for text in (
                _element_text(part) for part in document.findall(".//AbstractText")
            )
            if text
        )
        authors: list[str] = []
        for author in document.findall(".//AuthorList/Author"):
            last = author.findtext("LastName") or ""
            initials = author.findtext("Initials") or ""
            if last and initials:
                authors.append(f"{last}, {initials}")
            elif last:
                authors.append(last)
        doi = next(
            (
                identifier.text
                for identifier in document.findall(".//ArticleId")
                if (identifier.attrib.get("IdType") or "").lower() == "doi"
                and identifier.text
            ),
            None,
        )
        journal = (
            _element_text(document.find(".//CollectionTitle"))
            or _element_text(document.find(".//PublisherName"))
            or "PubMed"
        )
        records.append(
            make_record(
                title=title,
                abstract=abstract,
                authors=authors,
                year=document.findtext(".//Book/PubDate/Year"),
                doi=doi,
                pmid=pmid,
                source_identifier=pmid,
                source_database="PubMed",
                source_url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None,
                journal=journal,
                is_open_access=False,
                original_metadata={
                    "pmid": pmid,
                    "pubmed_record_type": "PubmedBookArticle",
                },
                source_query=query,
                stage="pubmed_search",
            )
        )
    return records


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
