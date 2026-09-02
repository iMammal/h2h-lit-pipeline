"""Frozen production STAR query-plan loading and Phase 4A compatibility checks."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h2h_lit.production_wave import (
    REQUIRED_IDENTIFICATION_SOURCES_V2,
    REQUIRED_SUPPORT_SOURCES_V2,
    SOURCE_CONTRACTS,
)
from h2h_lit.query_development import (
    EXPECTED_FAMILIES,
    load_candidate_set,
    load_semantic_control_set,
    load_sentinel_set,
)

PRODUCTION_PLAN_SCHEMA_VERSION = "1.0.0"
PRODUCTION_SELECTION = {
    "STAR-QF01-RELATIONAL-VIS": "unanchored",
    "STAR-QF02-ASSISTED-VIS": "E",
    "STAR-QF03-INTERACTIVE-SYSTEMS": "revised",
    "STAR-QF04-NONDESKTOP-ENV": "default",
    "STAR-QF05-CONVERSATIONAL": "default",
}
IDENTIFICATION_DATABASES = (
    "PubMed",
    "EuropePMC",
    "SemanticScholar",
    "arXiv",
    "IEEEXplore",
    "ACMDigitalLibrary",
)


class ProductionQueryPlanError(ValueError):
    """Raised when the frozen production plan no longer matches its source artifacts."""


@dataclass(frozen=True, slots=True)
class ProductionQueryPlan:
    payload: dict[str, Any]

    def canonical_payload(self) -> dict[str, Any]:
        material = dict(self.payload)
        material.pop("plan_hash", None)
        return material

    def canonical_json(self) -> str:
        return _canonical_json(self.canonical_payload())

    def plan_hash(self) -> str:
        return _sha256(self.canonical_json().encode("utf-8"))

    def to_json(self) -> str:
        return json.dumps(self.payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"

    def validate(self, *, root: str | Path) -> None:
        root_path = Path(root)
        data = self.payload
        if data.get("schema_version") != PRODUCTION_PLAN_SCHEMA_VERSION:
            raise ProductionQueryPlanError("unsupported production query-plan schema")
        if data.get("status") != "production_frozen":
            raise ProductionQueryPlanError("production query plan must be frozen")
        if data.get("plan_hash") != self.plan_hash():
            raise ProductionQueryPlanError("production query-plan hash mismatch")
        if data.get("partitions") != [] or data.get("automatic_partitioning") is not False:
            raise ProductionQueryPlanError("production query plan cannot invent partitions")
        if any(data.get("execution_boundaries", {}).values()):
            raise ProductionQueryPlanError("query-plan freeze cannot perform production operations")

        candidate_ref = data["candidate_config"]
        candidate_path = root_path / candidate_ref["path"]
        candidate = load_candidate_set(candidate_path)
        _validate_file_reference(candidate_path, candidate_ref)
        if candidate.candidate_set_hash() != candidate_ref["canonical_hash"]:
            raise ProductionQueryPlanError("candidate canonical hash mismatch")

        control_ref = data["semantic_controls"]
        control_path = root_path / control_ref["path"]
        controls = load_semantic_control_set(control_path)
        _validate_file_reference(control_path, control_ref)
        if controls.control_set_hash() != control_ref["canonical_hash"]:
            raise ProductionQueryPlanError("semantic-control canonical hash mismatch")

        sentinel_ref = data["sentinel_set"]
        sentinel_path = root_path / sentinel_ref["path"]
        sentinels = load_sentinel_set(sentinel_path)
        _validate_file_reference(sentinel_path, sentinel_ref)
        if sentinels.sentinel_set_hash() != sentinel_ref["canonical_hash"]:
            raise ProductionQueryPlanError("sentinel canonical hash mismatch")

        families = data.get("families", [])
        if [item.get("family_id") for item in families] != list(EXPECTED_FAMILIES):
            raise ProductionQueryPlanError("production plan must contain exactly five families")
        actual_selection = {
            item["family_id"]: item["production_variant"] for item in families
        }
        if actual_selection != PRODUCTION_SELECTION:
            raise ProductionQueryPlanError("production family selection changed")
        for item in families:
            expected = candidate.payload["families"][item["family_id"]]["variants"][item["production_variant"]]
            if item["conceptual_expression"] != expected:
                raise ProductionQueryPlanError(
                    f"conceptual expression changed for {item['family_id']}"
                )
            if item.get("production_status") != "active":
                raise ProductionQueryPlanError("all five frozen families must be active")

        expected_queries = _render_production_queries(candidate)
        if data.get("source_queries") != expected_queries:
            raise ProductionQueryPlanError("source-specific rendered queries changed")
        _validate_source_roles(data)
        _validate_phase4a_contract(data)


def build_production_query_plan(
    candidate_path: str | Path,
    semantic_control_path: str | Path,
    sentinel_path: str | Path,
    *,
    root: str | Path,
) -> ProductionQueryPlan:
    root_path = Path(root)
    candidate_file = Path(candidate_path)
    controls_file = Path(semantic_control_path)
    sentinel_file = Path(sentinel_path)
    candidate = load_candidate_set(candidate_file)
    controls = load_semantic_control_set(controls_file)
    sentinels = load_sentinel_set(sentinel_file)

    families = []
    purposes = {
        "STAR-QF01-RELATIONAL-VIS": (
            "Primary assistance-neutral retrieval for explicit or derived relational "
            "life-science visualization."
        ),
        "STAR-QF02-ASSISTED-VIS": (
            "Primary retrieval for computationally assisted interactive visualization, "
            "with generic mechanisms constrained by an assistance-context anchor."
        ),
        "STAR-QF03-INTERACTIVE-SYSTEMS": (
            "Assistance-neutral vocabulary-compensation route for interactive life-science "
            "systems whose metadata may omit stronger visualization, assistance, immersive, "
            "or conversational terminology; sizing did not establish incremental recall."
        ),
        "STAR-QF04-NONDESKTOP-ENV": (
            "Complementary recall for non-desktop and immersive visualization environments."
        ),
        "STAR-QF05-CONVERSATIONAL": (
            "Complementary recall for natural-language, conversational, and agentic interfaces."
        ),
    }
    historical = {
        "STAR-QF01-RELATIONAL-VIS": ["anchored"],
        "STAR-QF02-ASSISTED-VIS": ["A", "B", "C", "D"],
        "STAR-QF03-INTERACTIVE-SYSTEMS": ["default"],
        "STAR-QF04-NONDESKTOP-ENV": [],
        "STAR-QF05-CONVERSATIONAL": [],
    }
    for family_id in EXPECTED_FAMILIES:
        variant = PRODUCTION_SELECTION[family_id]
        family = candidate.payload["families"][family_id]
        families.append(
            {
                "family_id": family_id,
                "production_status": "active",
                "production_variant": variant,
                "role": family["role"],
                "conceptual_expression": family["variants"][variant],
                "methodological_purpose": purposes[family_id],
                "historical_comparator_variants": historical[family_id],
            }
        )

    payload: dict[str, Any] = {
        "schema_version": PRODUCTION_PLAN_SCHEMA_VERSION,
        "plan_id": "h2h-star-production-query-plan",
        "plan_version": "1.0.0",
        "status": "production_frozen",
        "methodological_basis": "query_family_design_complete_after_bounded_v0_4",
        "candidate_config": _file_reference(
            candidate_file, root_path, candidate.candidate_set_version, candidate.candidate_set_hash()
        ),
        "semantic_controls": {
            **_file_reference(
                controls_file,
                root_path,
                controls.control_set_version,
                controls.control_set_hash(),
            ),
            "required_gate": "bulk_boolean_semantics",
            "gate_behavior": "fail_closed_before_semantic_scholar_candidates",
            "expressions": [probe.expression for probe in controls.probes],
        },
        "sentinel_set": _file_reference(
            sentinel_file,
            root_path,
            sentinels.sentinel_set_version,
            sentinels.sentinel_set_hash(),
        ),
        "term_block_provenance": {
            "source_candidate_hash": candidate.candidate_set_hash(),
            "blocks_and_anchors_hash": _sha256(
                _canonical_json(
                    {
                        "blocks": candidate.payload["blocks"],
                        "anchors": candidate.payload["anchors"],
                    }
                ).encode("utf-8")
            ),
            "inheritance_policy": "unchanged_from_v0_3_through_v0_4",
        },
        "families": families,
        "source_roles": _source_roles(),
        "source_queries": _render_production_queries(candidate),
        "required_prior_survey_seed_manifests": [
            {"seed_set_id": item, "status": "required_unresolved", "manifest": None}
            for item in ("EBK25", "JFR25", "FP19")
        ],
        "unresolved_prerequisites": [
            "IEEE_XPLORE_API_KEY",
            "ieee_production_query_sizing_or_verification",
            "acm_institutional_access_and_operator",
            "acm_query_sizing_and_export_evidence",
            "EBK25_seed_manifest",
            "JFR25_seed_manifest",
            "FP19_seed_manifest",
            "source_window_partition_review_if_any_final_query_exceeds_a_supported_window",
        ],
        "partitions": [],
        "automatic_partitioning": False,
        "automatic_query_rewriting": False,
        "automatic_mode_switching": False,
        "phase4a_compatibility": {
            "wave_schema_version": "1.1.0",
            "required_identification_sources": list(REQUIRED_IDENTIFICATION_SOURCES_V2),
            "required_support_sources": list(REQUIRED_SUPPORT_SOURCES_V2),
            "wave_instantiated": False,
            "readiness": "planned_inputs_unresolved",
        },
        "execution_boundaries": {
            "retrieval_executed": False,
            "sizing_executed": False,
            "screening_executed": False,
            "llm_executed": False,
            "prisma_generated": False,
            "e6_derived": False,
            "corpus_created": False,
        },
    }
    plan = ProductionQueryPlan(payload)
    payload["plan_hash"] = plan.plan_hash()
    plan = ProductionQueryPlan(payload)
    plan.validate(root=root_path)
    return plan


def load_production_query_plan(path: str | Path, *, root: str | Path) -> ProductionQueryPlan:
    plan = ProductionQueryPlan(json.loads(Path(path).read_text(encoding="utf-8")))
    plan.validate(root=root)
    return plan


def _render_production_queries(candidate: Any) -> list[dict[str, Any]]:
    rendered = candidate.render_selected(PRODUCTION_SELECTION, list(IDENTIFICATION_DATABASES))
    output = []
    for item in rendered:
        request = _production_request(item.source, item.query_text, item.sizing_request)
        output.append(
            {
                "query_id": f"production:{item.family_id}:{item.source}",
                "family_id": item.family_id,
                "variant_id": item.variant_id,
                "source": item.source,
                "query_text": item.query_text,
                "query_text_sha256": _sha256(item.query_text.encode("utf-8")),
                "field_restrictions": item.field_restrictions,
                "transport": "artifact_import" if item.source == "ACMDigitalLibrary" else "http",
                "mode": "bulk" if item.source == "SemanticScholar" else None,
                "request_specification": request,
                "request_specification_hash": _sha256(_canonical_json(request).encode("utf-8")),
                "hard_window": item.hard_window,
                "completeness": _completeness(item.source),
                "content_policy": (
                    {"abstract": "external_llm_use_unresolved"}
                    if item.source == "IEEEXplore"
                    else {}
                ),
            }
        )
    return output


def _production_request(source: str, query_text: str, sizing_request: dict[str, Any]) -> dict[str, Any]:
    if source == "PubMed":
        return {
            "method": "POST",
            "endpoint": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            "headers": {"content-type": "application/x-www-form-urlencoded"},
            "form": {
                "db": "pubmed",
                "term": query_text,
                "retmax": 10000,
                "retmode": "xml",
                "usehistory": "y",
            },
            "warning_provenance": ["ErrorList", "WarningList", "QueryTranslation"],
        }
    if source == "EuropePMC":
        return {
            "method": "GET",
            "endpoint": "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
            "params": {
                "query": query_text,
                "format": "json",
                "pageSize": 1000,
                "resultType": "core",
                "cursorMark": "*",
            },
        }
    if source == "SemanticScholar":
        return {
            "method": "GET",
            "endpoint": "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
            "params": {
                "query": query_text,
                "limit": 1000,
                "fields": (
                    "paperId,title,abstract,year,venue,url,externalIds,isOpenAccess,"
                    "authors,openAccessPdf"
                ),
                "sort": "paperId:asc",
            },
            "semantic_control_gate": "bulk_boolean_semantics",
        }
    if source == "arXiv":
        return {
            "method": "GET",
            "endpoint": "http://export.arxiv.org/api/query",
            "params": {
                "search_query": query_text,
                "start": 0,
                "max_results": 2000,
                "sortBy": "submittedDate",
                "sortOrder": "ascending",
            },
        }
    if source == "IEEEXplore":
        return {
            "method": "GET",
            "endpoint": "https://ieeexploreapi.ieee.org/api/v1/search/articles",
            "params": {
                "query_parameter": "querytext",
                "querytext": query_text,
                "format": "json",
                "max_records": 200,
                "start_record": 1,
                "sort_field": "article_number",
                "sort_order": "asc",
            },
            "credential_reference": "IEEE_XPLORE_API_KEY",
        }
    if source == "ACMDigitalLibrary":
        return {
            **sizing_request,
            "citation_export": True,
            "export_format": "BibTeX",
            "operator_evidence_required": True,
            "ui_reported_total": None,
            "expected_chunks": None,
        }
    raise ProductionQueryPlanError(f"unsupported production source {source}")


def _completeness(source: str) -> dict[str, Any]:
    contract = SOURCE_CONTRACTS[source]
    proofs = list(contract.completion_proofs)
    exact = contract.exact_total_required
    maximum = contract.maximum_supported_results
    if source == "SemanticScholar":
        proofs = ["semantic_scholar_bulk_token_exhausted"]
        exact = False
        maximum = None
    return {
        "adapter_id": contract.adapter_id,
        "adapter_version": contract.adapter_version,
        "pagination_strategy": contract.strategy,
        "completion_proofs": proofs,
        "exact_total_required": exact,
        "maximum_supported_results": maximum,
        "incomplete_or_truncated_cannot_finalize": True,
    }


def _source_roles() -> list[dict[str, Any]]:
    return [
        {"source": "PubMed", "role": "primary_identification", "transport": "http"},
        {"source": "EuropePMC", "role": "primary_identification", "transport": "http"},
        {
            "source": "IEEEXplore",
            "role": "required_supplemental_identification",
            "transport": "http",
        },
        {
            "source": "ACMDigitalLibrary",
            "role": "required_supplemental_identification",
            "transport": "artifact_import",
        },
        {
            "source": "SemanticScholar",
            "role": "supplemental_identification",
            "transport": "http",
            "mode": "bulk",
        },
        {"source": "arXiv", "role": "supplemental_identification", "transport": "http"},
        {
            "source": "CrossRef",
            "role": "enrichment_exact_identity_and_deduplication_support",
            "transport": "http",
            "production_identification": False,
        },
        {
            "source": "PriorSurveySeed",
            "role": "prior_survey_seed_identification",
            "transport": "artifact_import",
        },
    ]


def _validate_source_roles(data: dict[str, Any]) -> None:
    roles = {item["source"]: item for item in data.get("source_roles", [])}
    expected = {item["source"]: item for item in _source_roles()}
    if roles != expected:
        raise ProductionQueryPlanError("production source roles changed")
    if any(item["source"] == "CrossRef" for item in data["source_queries"]):
        raise ProductionQueryPlanError("Crossref cannot be a production identification query")
    semantic = [item for item in data["source_queries"] if item["source"] == "SemanticScholar"]
    if not semantic or any(
        item["mode"] != "bulk"
        or item["request_specification"].get("semantic_control_gate")
        != "bulk_boolean_semantics"
        for item in semantic
    ):
        raise ProductionQueryPlanError("Semantic Scholar must remain bulk and fail-closed")


def _validate_phase4a_contract(data: dict[str, Any]) -> None:
    compatibility = data.get("phase4a_compatibility", {})
    if compatibility.get("wave_schema_version") != "1.1.0":
        raise ProductionQueryPlanError("production plan requires Phase 4A schema 1.1.0")
    if compatibility.get("required_identification_sources") != list(
        REQUIRED_IDENTIFICATION_SOURCES_V2
    ):
        raise ProductionQueryPlanError("Phase 4A identification inventory changed")
    if compatibility.get("required_support_sources") != list(REQUIRED_SUPPORT_SOURCES_V2):
        raise ProductionQueryPlanError("Phase 4A support inventory changed")
    if compatibility.get("wave_instantiated") is not False:
        raise ProductionQueryPlanError("query-plan freeze cannot instantiate a wave")


def _file_reference(path: Path, root: Path, version: str, canonical_hash: str) -> dict[str, Any]:
    relative = path.resolve().relative_to(root.resolve()).as_posix()
    return {
        "path": relative,
        "version": version,
        "raw_sha256": _sha256(path.read_bytes()),
        "canonical_hash": canonical_hash,
        "byte_size": path.stat().st_size,
    }


def _validate_file_reference(path: Path, reference: dict[str, Any]) -> None:
    if not path.is_file():
        raise ProductionQueryPlanError(f"frozen input is missing: {path}")
    if path.stat().st_size != reference.get("byte_size"):
        raise ProductionQueryPlanError(f"frozen input byte size changed: {path}")
    if _sha256(path.read_bytes()) != reference.get("raw_sha256"):
        raise ProductionQueryPlanError(f"frozen input raw hash changed: {path}")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()
