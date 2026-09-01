"""Deterministic, non-production query-sizing dry-run planning."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.query_development import (
    CANDIDATE_V2_SCHEMA_VERSION,
    CANDIDATE_V3_SCHEMA_VERSION,
    SIZING_RUN_SCHEMA_VERSION,
    SIZING_RUN_V2_SCHEMA_VERSION,
    QuerySizingRun,
    SentinelPaper,
    SizingAttempt,
    SizingObservation,
    SizingRunStatus,
    SizingSyntaxStatus,
    SizingTransportStatus,
    SizingWindowStatus,
    load_candidate_set,
    load_semantic_control_set,
    load_sentinel_set,
    sizing_request_hash,
)

DRY_RUN_SCHEMA_VERSION = "1.0.0"
DRY_RUN_V2_SCHEMA_VERSION = "1.1.0"
DEFAULT_RUN_ID = "star-query-sizing-v0-1-run-001"

SOURCE_URLS = {
    "PubMed": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
    "EuropePMC": "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
    "SemanticScholar": "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
    "arXiv": "http://export.arxiv.org/api/query",
    "IEEEXplore": "https://ieeexploreapi.ieee.org/api/v1/search/articles",
    "CrossRef": "https://api.crossref.org/works",
}

INTERPRETATION_RULES = [
    "hard-window overflow prevents freezing a candidate as written",
    "syntax rejection or semantic rewrite requires review",
    "QF01 anchored/unanchored differences are diagnostic",
    "QF02 A/B/C/D count relationships are diagnostic",
    "high count below a source limit is not itself a defect",
    "heavy overlap is acceptable",
    "sentinel misses require diagnosis rather than automatic query expansion",
    "no partitioning is authorized",
    "no QF01 or QF02 winner is selected by this dry run",
]

NON_PRODUCTION_INVARIANTS = {
    "creates_record_occurrences": False,
    "creates_review_dataset": False,
    "creates_retrieval_run": False,
    "contributes_prisma_counts": False,
    "derives_e6": False,
    "runs_screening": False,
    "establishes_retrieval_cutoff": False,
    "creates_corpus_membership": False,
    "supports_partitioning": False,
    "network_calls_performed": 0,
}


def build_sizing_dry_run(
    candidate_config: str | Path,
    sentinel_config: str | Path,
    *,
    run_id: str = DEFAULT_RUN_ID,
    created_at: str | None = None,
    semantic_control_config: str | Path | None = None,
) -> dict[str, Any]:
    candidate_set = load_candidate_set(candidate_config)
    sentinel_set = load_sentinel_set(sentinel_config)
    is_versioned = candidate_set.payload["schema_version"] in {
        CANDIDATE_V2_SCHEMA_VERSION,
        CANDIDATE_V3_SCHEMA_VERSION,
    }
    if sentinel_set.candidate_set_hash != candidate_set.candidate_set_hash():
        compatibility = candidate_set.payload.get("sentinel_compatibility", {})
        if not is_versioned or (
            compatibility.get("sentinel_set_hash") != sentinel_set.sentinel_set_hash()
            or compatibility.get("original_candidate_set_hash")
            != sentinel_set.candidate_set_hash
            or compatibility.get("membership_and_expectations_unchanged") is not True
        ):
            raise ValueError("sentinel set does not reference a compatible candidate set")

    timestamp = created_at or sentinel_set.frozen_at
    candidates = candidate_set.render_all()
    expected_candidates = 54 if is_versioned else 62
    if len(candidates) != expected_candidates:
        raise ValueError(
            f"the candidate configuration must render exactly {expected_candidates} queries"
        )

    observations = [_planned_observation(item, timestamp) for item in candidates]
    run = QuerySizingRun(
        schema_version=(
            SIZING_RUN_V2_SCHEMA_VERSION if is_versioned else SIZING_RUN_SCHEMA_VERSION
        ),
        sizing_run_id=run_id,
        candidate_set_id=candidate_set.candidate_set_id,
        candidate_set_version=candidate_set.candidate_set_version,
        candidate_set_hash=candidate_set.candidate_set_hash(),
        sentinel_set_id=sentinel_set.sentinel_set_id,
        sentinel_set_version=sentinel_set.sentinel_set_version,
        sentinel_set_hash=sentinel_set.sentinel_set_hash(),
        status=SizingRunStatus.PLANNED,
        planned_candidate_query_ids=[item.candidate_query_id for item in candidates],
        created_at=timestamp,
        observations=observations,
    )
    run.validate()

    candidate_specs = [
        {
            "candidate_query_id": item.candidate_query_id,
            "query_hash": item.query_hash,
            "family_id": item.family_id,
            "variant_id": item.variant_id,
            "leading_candidate": item.leading_candidate,
            "source": item.source,
            "source_role": item.source_role,
            "count_kind": item.count_kind.value,
            "hard_window": item.hard_window,
            "syntax_gates": list(item.syntax_uncertainties),
            "request": observations[index].request,
            "request_hash": observations[index].request_hash,
            "credential_reference": observations[index].credential_reference,
            "parser_contract": item.parser_contract,
        }
        for index, item in enumerate(candidates)
    ]
    identity_specs: list[dict[str, Any]] = []
    if is_versioned:
        identity_specs = [
            _sentinel_identity_spec(source, sentinel)
            for source in candidate_set.payload["sources"]
            for sentinel in sentinel_set.entries
        ]
        identity_by_key = {
            (item["source"], item["sentinel_id"]): item["identity_resolution_id"]
            for item in identity_specs
        }
        diagnostics = [
            _sentinel_match_spec(
                item,
                sentinel,
                identity_by_key[(item.source, sentinel.sentinel_id)],
            )
            for item in candidates
            for sentinel in sentinel_set.entries
            if item.family_id in sentinel.diagnostic_family_ids
        ]
    else:
        diagnostics = [
            _sentinel_diagnostic_spec(item, sentinel)
            for item in candidates
            for sentinel in sentinel_set.entries
            if item.family_id in sentinel.diagnostic_family_ids
        ]
    semantic_controls: list[dict[str, Any]] = []
    semantic_control_provenance: dict[str, Any] | None = None
    if is_versioned:
        if semantic_control_config is None:
            raise ValueError("versioned dry runs require the frozen semantic-control config")
        controls = load_semantic_control_set(semantic_control_config)
        semantic_controls = [_semantic_control_spec(item) for item in controls.probes]
        semantic_control_provenance = {
            "control_set_id": controls.control_set_id,
            "control_set_version": controls.control_set_version,
            "control_set_hash": controls.control_set_hash(),
            "assertions": [item.to_dict() for item in controls.assertions],
            "gate": controls.production_identification_gate,
            "automatic_mode_switching": controls.automatic_mode_switching,
        }
    report = {
        "schema_version": (
            DRY_RUN_V2_SCHEMA_VERSION if is_versioned else DRY_RUN_SCHEMA_VERSION
        ),
        "report_kind": "non_production_query_sizing_dry_run",
        "run": run.to_dict(),
        "candidate_specifications": candidate_specs,
        "sentinel_identity_specifications": identity_specs,
        "sentinel_diagnostic_specifications": diagnostics,
        "semantic_control_specifications": semantic_controls,
        "semantic_control_provenance": semantic_control_provenance,
        "source_requirements": _source_requirements(is_v2=is_versioned),
        "interpretation_rules": list(INTERPRETATION_RULES),
        "non_production_invariants": dict(NON_PRODUCTION_INVARIANTS),
    }
    report["report_hash"] = _hash_without_report_hash(report)
    return report


def save_sizing_dry_run(report: dict[str, Any], path: str | Path) -> str:
    expected = _hash_without_report_hash(report)
    if report.get("report_hash") != expected:
        raise ValueError("dry-run report hash does not match its contents")
    content = canonical_json(report) + "\n"
    atomic_write(Path(path), content.encode("utf-8"))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def canonical_json(value: Any, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _planned_observation(candidate: Any, timestamp: str) -> SizingObservation:
    request = _execution_request(candidate.source, candidate.sizing_request)
    request_hash = sizing_request_hash(request)
    credential_reference = (
        "IEEE_XPLORE_API_KEY" if candidate.source == "IEEEXplore" else None
    )
    attempt = SizingAttempt(
        attempt_number=1,
        started_at=timestamp,
        completed_at=None,
        request=request,
        request_hash=request_hash,
        transport_status=SizingTransportStatus.PLANNED,
        credential_reference=credential_reference,
    )
    return SizingObservation(
        observation_id=_stable_id("sizing-observation", candidate.candidate_query_id),
        candidate_query_id=candidate.candidate_query_id,
        query_hash=candidate.query_hash,
        source=candidate.source,
        observed_at=timestamp,
        request=request,
        request_hash=request_hash,
        response_hash=None,
        reported_count=None,
        count_kind=candidate.count_kind,
        hard_window=candidate.hard_window,
        window_status=SizingWindowStatus.UNKNOWN,
        syntax_status=SizingSyntaxStatus.UNTESTED,
        transport_status=SizingTransportStatus.PLANNED,
        credential_reference=credential_reference,
        warnings=list(candidate.syntax_uncertainties),
        attempts=[attempt],
    )


def _execution_request(source: str, rendered: dict[str, Any]) -> dict[str, Any]:
    if source == "ACMDigitalLibrary":
        return dict(rendered)
    request = {
        "transport": "http",
        "method": rendered["method"],
        "url": SOURCE_URLS[source],
        "params": dict(rendered["params"]),
    }
    if "form" in rendered:
        request["form"] = dict(rendered["form"])
    if "headers" in rendered:
        request["headers"] = dict(rendered["headers"])
    if source == "CrossRef":
        request["fallback"] = {
            "transport": "http",
            "method": rendered["method"],
            "url": SOURCE_URLS[source],
            "params": dict(rendered["fallback_params"]),
            "condition": "rows_0_rejected_or_total_results_missing",
        }
    return request


def _sentinel_diagnostic_spec(candidate: Any, sentinel: SentinelPaper) -> dict[str, Any]:
    identity_request, match_request, support = _diagnostic_requests(candidate, sentinel)
    return {
        "diagnostic_plan_id": _stable_id(
            "sentinel-diagnostic",
            sentinel.sentinel_id,
            candidate.candidate_query_id,
        ),
        "sentinel_id": sentinel.sentinel_id,
        "source_identifier": sentinel.source_identifier,
        "doi": sentinel.doi,
        "source": candidate.source,
        "candidate_query_id": candidate.candidate_query_id,
        "query_hash": candidate.query_hash,
        "support_status": support,
        "execution_condition": "execute_match_probe_only_after_source_identity_resolution",
        "identity_request": identity_request,
        "identity_request_hash": sizing_request_hash(identity_request),
        "match_request": match_request,
        "match_request_hash": sizing_request_hash(match_request),
        "allowed_persisted_result": "identifier_only",
        "possible_outcomes": [
            "INDEXED_AND_MATCHED",
            "INDEXED_BUT_QUERY_MISSED",
            "SOURCE_NOT_INDEXED",
            "IDENTITY_UNRESOLVED",
            "DIAGNOSTIC_UNSUPPORTED",
        ],
    }


def _sentinel_identity_spec(source: str, sentinel: SentinelPaper) -> dict[str, Any]:
    request, support = _identity_request(source, sentinel)
    resolution_id = _stable_id("sentinel-identity", source, sentinel.sentinel_id)
    unresolved = sentinel.doi is None
    return {
        "identity_resolution_id": resolution_id,
        "sentinel_id": sentinel.sentinel_id,
        "source_identifier": sentinel.source_identifier,
        "doi": sentinel.doi,
        "source": source,
        "support_status": support,
        "identity_basis": "stable_doi" if not unresolved else "identity_unresolved",
        "execution_status": "identity_unresolved" if unresolved else "planned",
        "request": request,
        "request_hash": sizing_request_hash(request),
        "allowed_persisted_result": "identifier_only",
        "resolve_once_per_source_and_sentinel": True,
    }


def _sentinel_match_spec(
    candidate: Any,
    sentinel: SentinelPaper,
    identity_resolution_id: str,
) -> dict[str, Any]:
    _, match_request, support = _diagnostic_requests(candidate, sentinel)
    return {
        "diagnostic_plan_id": _stable_id(
            "sentinel-diagnostic", sentinel.sentinel_id, candidate.candidate_query_id
        ),
        "sentinel_id": sentinel.sentinel_id,
        "source": candidate.source,
        "candidate_query_id": candidate.candidate_query_id,
        "query_hash": candidate.query_hash,
        "support_status": support,
        "identity_resolution_id": identity_resolution_id,
        "execution_condition": "consume_frozen_source_sentinel_identity_resolution",
        "match_request": match_request,
        "match_request_hash": sizing_request_hash(match_request),
        "allowed_persisted_result": "identifier_only",
        "possible_outcomes": [
            "INDEXED_AND_MATCHED",
            "INDEXED_BUT_QUERY_MISSED",
            "SOURCE_NOT_INDEXED",
            "IDENTITY_UNRESOLVED",
            "DIAGNOSTIC_UNSUPPORTED",
        ],
    }


def _identity_request(
    source: str,
    sentinel: SentinelPaper,
) -> tuple[dict[str, Any], str]:
    doi = sentinel.doi
    title = sentinel.title
    if source == "PubMed":
        identity = f'"{doi}"[DOI]' if doi else f'"{title}"[Title]'
        return (
            _http(
                SOURCE_URLS[source],
                {"db": "pubmed", "term": identity, "retmax": 1, "retmode": "xml"},
            ),
            "identifier_constrained",
        )
    if source == "EuropePMC":
        identity = f'DOI:"{doi}"' if doi else f'TITLE:"{title}"'
        return (
            _http(
                SOURCE_URLS[source],
                {"format": "json", "pageSize": 1, "resultType": "lite", "query": identity},
            ),
            "identifier_constrained",
        )
    if source == "SemanticScholar":
        return (
            _http(
                SOURCE_URLS[source],
                {"limit": 1, "fields": "paperId", "sort": "paperId:asc", "query": f'"{title}"'},
            ),
            "bulk_boolean_semantics_gate_required",
        )
    if source == "arXiv":
        identity = f'all:"{doi}"' if doi else f'ti:"{title}"'
        return (
            _http(
                SOURCE_URLS[source],
                {
                    "start": 0,
                    "max_results": 1,
                    "sortBy": "submittedDate",
                    "sortOrder": "ascending",
                    "search_query": identity,
                },
            ),
            "identifier_constrained_after_identity_resolution",
        )
    if source == "IEEEXplore":
        identity_key = "doi" if doi else "article_title"
        return (
            _http(
                SOURCE_URLS[source],
                {
                    "format": "json",
                    "max_records": 1,
                    "start_record": 1,
                    "sort_field": "article_number",
                    "sort_order": "asc",
                    identity_key: doi or title,
                },
                credential_reference="IEEE_XPLORE_API_KEY",
            ),
            "identifier_constrained_count_only_no_abstract_use",
        )
    if source == "CrossRef":
        return (
            _http(
                f"{SOURCE_URLS[source]}/{doi}", {}
            )
            if doi
            else _http(SOURCE_URLS[source], {"query.title": title, "rows": 1}),
            "exact_identity_resolution_only",
        )
    if source == "ACMDigitalLibrary":
        identity = f'DOI "{doi}"' if doi else f'Title "{title}"'
        return (
            {
                "transport": "human_ui",
                "workflow": "advanced_search",
                "scope": "ACM Publications",
                "fields": ["Title", "Abstract", "Author Keywords"],
                "filters": {},
                "sort": "publicationDate asc",
                "citation_export": False,
                "query": identity,
            },
            "human_operator_required",
        )
    raise ValueError(f"unsupported identity source: {source}")


def _semantic_control_spec(probe: Any) -> dict[str, Any]:
    request = _http(
        SOURCE_URLS["SemanticScholar"],
        {
            "query": probe.expression,
            "limit": 1,
            "fields": "paperId",
            "sort": "paperId:asc",
        },
    )
    return {
        "control_query_id": f"semantic-control:{probe.probe_id}",
        "probe_id": probe.probe_id,
        "role": probe.role,
        "expression": probe.expression,
        "expression_hash": hashlib.sha256(probe.expression.encode("utf-8")).hexdigest(),
        "source": "SemanticScholar",
        "mode": "bulk",
        "request": request,
        "request_hash": sizing_request_hash(request),
        "creates_production_occurrences": False,
    }


def _diagnostic_requests(
    candidate: Any, sentinel: SentinelPaper
) -> tuple[dict[str, Any], dict[str, Any], str]:
    doi = sentinel.doi
    title = sentinel.title
    source = candidate.source
    if source == "PubMed":
        identity = f'"{doi}"[DOI]' if doi else f'"{title}"[Title]'
        match_fields = {
            "db": "pubmed",
            "term": f"({candidate.query_text}) AND ({identity})",
            "retmax": 1,
            "retmode": "xml",
        }
        match_request = (
            _http(
                SOURCE_URLS[source],
                {},
                method="POST",
                form=match_fields,
                headers={"content-type": "application/x-www-form-urlencoded"},
            )
            if candidate.sizing_request["method"] == "POST"
            else _http(SOURCE_URLS[source], match_fields)
        )
        return (
            _http(SOURCE_URLS[source], {"db": "pubmed", "term": identity, "retmax": 1, "retmode": "xml"}),
            match_request,
            "identifier_constrained",
        )
    if source == "EuropePMC":
        identity = f'DOI:"{doi}"' if doi else f'TITLE:"{title}"'
        params = {"format": "json", "pageSize": 1, "resultType": "lite"}
        return (
            _http(SOURCE_URLS[source], {**params, "query": identity}),
            _http(
                SOURCE_URLS[source],
                {**params, "query": f"({candidate.query_text}) AND ({identity})"},
            ),
            "identifier_constrained",
        )
    if source == "SemanticScholar":
        identity_query = f'"{title}"'
        params = {"limit": 1, "fields": "paperId", "sort": "paperId:asc"}
        conjunction = (
            "+"
            if candidate.sizing_request.get("params", {}).get("query") == candidate.query_text
            and candidate.sizing_request.get("endpoint") == "paper/search/bulk"
            and "bulk_boolean_semantics_control_required" in candidate.syntax_uncertainties
            else "AND"
        )
        return (
            _http(SOURCE_URLS[source], {**params, "query": identity_query}),
            _http(
                SOURCE_URLS[source],
                {**params, "query": f"({candidate.query_text}) {conjunction} {identity_query}"},
            ),
            "bulk_boolean_semantics_gate_required",
        )
    if source == "arXiv":
        identity = f'all:"{doi}"' if doi else f'ti:"{title}"'
        params = {"start": 0, "max_results": 1, "sortBy": "submittedDate", "sortOrder": "ascending"}
        return (
            _http(SOURCE_URLS[source], {**params, "search_query": identity}),
            _http(
                SOURCE_URLS[source],
                {**params, "search_query": f"({candidate.query_text}) AND ({identity})"},
            ),
            "identifier_constrained_after_identity_resolution",
        )
    if source == "IEEEXplore":
        identity_key = "doi" if doi else "article_title"
        identity_value = doi or title
        params = {
            "format": "json",
            "max_records": 1,
            "start_record": 1,
            "sort_field": "article_number",
            "sort_order": "asc",
        }
        return (
            _http(
                SOURCE_URLS[source],
                {**params, identity_key: identity_value},
                credential_reference="IEEE_XPLORE_API_KEY",
            ),
            _http(
                SOURCE_URLS[source],
                {**params, "querytext": candidate.query_text, identity_key: identity_value},
                credential_reference="IEEE_XPLORE_API_KEY",
            ),
            "identifier_constrained_count_only_no_abstract_use",
        )
    if source == "CrossRef":
        if doi:
            identity_request = _http(f"{SOURCE_URLS[source]}/{doi}", {})
            match_params = {"query": candidate.query_text, "filter": f"doi:{doi}", "rows": 0}
        else:
            identity_request = _http(SOURCE_URLS[source], {"query.title": title, "rows": 1})
            match_params = {"query": candidate.query_text, "query.title": title, "rows": 0}
        return (
            identity_request,
            _http(SOURCE_URLS[source], match_params),
            "identification_semantics_unresolved",
        )
    if source == "ACMDigitalLibrary":
        identity = f'DOI "{doi}"' if doi else f'Title "{title}"'
        common = {
            "transport": "human_ui",
            "workflow": "advanced_search",
            "scope": "ACM Publications",
            "fields": ["Title", "Abstract", "Author Keywords"],
            "filters": {},
            "sort": "publicationDate asc",
            "citation_export": False,
        }
        return (
            {**common, "query": identity},
            {**common, "query": f"({candidate.query_text}) AND ({identity})"},
            "human_operator_required",
        )
    raise ValueError(f"unsupported diagnostic source: {source}")


def _http(
    url: str,
    params: dict[str, Any],
    *,
    credential_reference: str | None = None,
    method: str = "GET",
    form: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "transport": "http",
        "method": method,
        "url": url,
        "params": params,
    }
    if form is not None:
        request["form"] = form
    if headers is not None:
        request["headers"] = headers
    if credential_reference:
        request["credential_reference"] = credential_reference
    return request


def _source_requirements(*, is_v2: bool = False) -> list[dict[str, Any]]:
    requirements = [
        {
            "source": "IEEEXplore",
            "kind": "credential",
            "reference": "IEEE_XPLORE_API_KEY",
            "required_for_live_execution": True,
        },
        {
            "source": "SemanticScholar",
            "kind": "credential",
            "reference": "SEMANTIC_SCHOLAR_API_KEY",
            "required_for_live_execution": False,
            "gate": "bulk_boolean_semantics",
        },
        {
            "source": "ACMDigitalLibrary",
            "kind": "human_operator",
            "reference": "ACM_OPERATOR_ID",
            "required_for_live_execution": True,
            "requirements": ["institutional_access", "UTC_timestamp", "query_evidence"],
        },
    ]
    requirements.append(
        {
            "source": "CrossRef",
            "kind": "non_identification_role" if is_v2 else "semantics_gate",
            "reference": None,
            "required_for_live_execution": not is_v2,
            "gate": None if is_v2 else "identification_semantics",
            "capabilities": (
                [
                    "doi_metadata_enrichment",
                    "exact_identity_resolution",
                    "deduplication_support",
                ]
                if is_v2
                else []
            ),
        }
    )
    return requirements


def _stable_id(prefix: str, *values: str) -> str:
    digest = hashlib.sha256("\x1f".join(values).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _hash_without_report_hash(report: dict[str, Any]) -> str:
    payload = {key: value for key, value in report.items() if key != "report_hash"}
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
