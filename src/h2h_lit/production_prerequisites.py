"""Offline prerequisite package for a frozen production retrieval query plan."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h2h_lit.acm_field_execution import (
    ACM_RETRIEVAL_EVIDENCE_COMPLETE,
    load_acm_final_reconciliation_manifest,
)
from h2h_lit.checkpoint import atomic_write
from h2h_lit.production_query_plan import load_production_query_plan
from h2h_lit.production_wave import (
    REQUIRED_IDENTIFICATION_SOURCES_V2,
    REQUIRED_SUPPORT_SOURCES_V2,
    SOURCE_CONTRACTS,
)

PREREQUISITE_SCHEMA_VERSION = "1.0.0"
PREREQUISITE_PACKAGE_VERSION = "1.1.0"
EXPECTED_PLAN_VERSION = "1.0.0"
EXPECTED_PLAN_HASH = "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
EXPECTED_PLAN_RAW_SHA256 = (
    "b887d638e42f4909c1c8461dde733d758e5176d528ddccee4370211e14ed7451"
)
SEED_SET_IDS = ("EBK25", "JFR25", "FP19")
PRIOR_SURVEY_SOURCE = "PriorSurveySeed"
EXTERNAL_IDENTIFICATION_SOURCES = tuple(
    source
    for source in REQUIRED_IDENTIFICATION_SOURCES_V2
    if source != PRIOR_SURVEY_SOURCE
)
POST_CLOSURE_OPERATIONS = (
    "final_global_deduplication",
    "prisma_reconciliation",
    "screening",
    "corpus_freeze",
)
PRODUCTION_SELECTION = {
    "STAR-QF01-RELATIONAL-VIS": "unanchored",
    "STAR-QF02-ASSISTED-VIS": "E",
    "STAR-QF03-INTERACTIVE-SYSTEMS": "revised",
    "STAR-QF04-NONDESKTOP-ENV": "default",
    "STAR-QF05-CONVERSATIONAL": "default",
}
SIZING_EVIDENCE = {
    "STAR-QF01-RELATIONAL-VIS": (
        "outputs/query_sizing/star-query-sizing-v0-3-run-001/query_sizing_run.json",
        "unanchored",
    ),
    "STAR-QF02-ASSISTED-VIS": (
        "outputs/query_sizing/star-query-sizing-v0-4-run-001/query_sizing_run.json",
        "E",
    ),
    "STAR-QF03-INTERACTIVE-SYSTEMS": (
        "outputs/query_sizing/star-query-sizing-v0-4-run-001/query_sizing_run.json",
        "revised",
    ),
    "STAR-QF04-NONDESKTOP-ENV": (
        "outputs/query_sizing/star-query-sizing-v0-3-run-001/query_sizing_run.json",
        "default",
    ),
    "STAR-QF05-CONVERSATIONAL": (
        "outputs/query_sizing/star-query-sizing-v0-3-run-001/query_sizing_run.json",
        "default",
    ),
}
ACM_FINAL_RECONCILIATION_PATH = (
    "provenance/star_acm_field_execution_2026-09-03_final_reconciliation_manifest.json"
)


class ProductionPrerequisiteError(ValueError):
    """Raised when prerequisite evidence conflicts with the frozen production plan."""


@dataclass(frozen=True, slots=True)
class ProductionPrerequisitePackage:
    payload: dict[str, Any]

    def canonical_payload(self) -> dict[str, Any]:
        material = dict(self.payload)
        material.pop("package_hash", None)
        return material

    def package_hash(self) -> str:
        return _sha256(_canonical_json(self.canonical_payload()).encode("utf-8"))

    def to_json(self) -> str:
        return json.dumps(self.payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"

    def validate(self, *, root: str | Path) -> None:
        root_path = Path(root)
        data = self.payload
        if data.get("schema_version") != PREREQUISITE_SCHEMA_VERSION:
            raise ProductionPrerequisiteError("unsupported prerequisite package schema")
        if data.get("package_version") != PREREQUISITE_PACKAGE_VERSION:
            raise ProductionPrerequisiteError("unsupported prerequisite package version")
        if data.get("package_hash") != self.package_hash():
            raise ProductionPrerequisiteError("prerequisite package hash mismatch")
        if data.get("overall_status") == "READY_FOR_WAVE_INSTANTIATION":
            raise ProductionPrerequisiteError("incomplete package cannot report ready")
        if data.get("production_operations_created") != []:
            raise ProductionPrerequisiteError("prerequisite validation created production state")

        plan_ref = data["production_query_plan"]
        plan_path = root_path / _safe_relative_path(plan_ref["path"])
        _validate_file_reference(plan_path, plan_ref)
        plan = load_production_query_plan(plan_path, root=root_path)
        if plan.payload["plan_version"] != EXPECTED_PLAN_VERSION:
            raise ProductionPrerequisiteError("production plan version changed")
        if plan.plan_hash() != EXPECTED_PLAN_HASH:
            raise ProductionPrerequisiteError("production plan canonical hash changed")

        children: dict[str, dict[str, Any]] = {}
        for ref in data.get("artifacts", []):
            child_path = root_path / _safe_relative_path(ref["path"])
            _validate_file_reference(child_path, ref)
            child = json.loads(child_path.read_text(encoding="utf-8"))
            if _artifact_hash(child) != ref["canonical_hash"]:
                raise ProductionPrerequisiteError(
                    f"child canonical hash mismatch: {ref['path']}"
                )
            children[ref["artifact_id"]] = child

        _validate_ieee(children["star-ieee-readiness-v1"], plan.payload)
        _validate_acm(children["star-acm-operator-spec-v1"], plan.payload, root_path)
        for seed_id in SEED_SET_IDS:
            _validate_seed(children[f"star-seed-{seed_id.lower()}-v1"], seed_id)
        _validate_windows(children["star-source-window-review-v1"], plan.payload)
        _validate_phase4a_compatibility(
            data["phase4a_compatibility"], children=children
        )
        external = data["phase4a_compatibility"]["external_retrieval_execution"]
        if data["blocking_reasons"] != external["blocking_reasons"]:
            raise ProductionPrerequisiteError(
                "BLOCKED_EXTERNAL_INPUT reasons must be external-retrieval reasons"
            )
        if data["overall_status"] == "BLOCKED_EXTERNAL_INPUT" and external["ready"]:
            raise ProductionPrerequisiteError(
                "external-ready package cannot report BLOCKED_EXTERNAL_INPUT"
            )


def build_prerequisite_payloads(
    *,
    root: str | Path,
    plan_path: str | Path,
    generated_at: str,
    ieee_credential_present: bool,
) -> tuple[dict[str, dict[str, Any]], ProductionPrerequisitePackage]:
    """Build prospective artifacts using only frozen local plan and sizing evidence."""

    root_path = Path(root)
    plan_file = Path(plan_path)
    plan = load_production_query_plan(plan_file, root=root_path)
    _validate_expected_plan(plan_file, plan.payload, root_path)

    ieee = _build_ieee(plan.payload, generated_at, ieee_credential_present)
    acm_final = _load_final_acm_reconciliation(root_path)
    acm = _build_acm(plan.payload, generated_at, acm_final, root_path)
    seeds = {seed_id: _build_seed(seed_id, root_path) for seed_id in SEED_SET_IDS}
    windows = _build_source_windows(plan.payload, root_path, acm_final)
    children = {
        "ieee_readiness.json": _finalize_artifact(ieee),
        "acm_operator_spec.json": _finalize_artifact(acm),
        **{
            f"seed_{seed_id.lower()}.json": _finalize_artifact(payload)
            for seed_id, payload in seeds.items()
        },
        "source_window_review.json": _finalize_artifact(windows),
    }

    artifact_refs = []
    for filename, payload in children.items():
        content = _pretty_json(payload).encode("utf-8")
        artifact_refs.append(
            {
                "artifact_id": payload["artifact_id"],
                "path": f"config/star_retrieval_prerequisites_v1/{filename}",
                "raw_sha256": _sha256(content),
                "canonical_hash": payload["artifact_hash"],
                "byte_size": len(content),
                "status": payload["status"],
            }
        )

    external_gate = _build_external_retrieval_gate(ieee, acm, windows)
    closure_gate = _build_identification_closure_gate(seeds)
    readiness_issues = [
        *external_gate["blocking_issues"],
        *closure_gate["blocking_issues"],
    ]

    package_payload: dict[str, Any] = {
        "schema_version": PREREQUISITE_SCHEMA_VERSION,
        "package_id": "h2h-star-retrieval-prerequisites",
        "package_version": PREREQUISITE_PACKAGE_VERSION,
        "generated_at": generated_at,
        "updated_at": max(
            [generated_at]
            + ([str(acm_final["reconciled_at_utc"])] if acm_final else [])
            + [
                str(seed.get("acquired_at"))
                for seed in seeds.values()
                if seed.get("acquired_at")
            ]
        ),
        "production_query_plan": _file_reference(
            plan_file, root_path, plan.plan_hash(), plan.payload["plan_version"]
        ),
        "artifacts": artifact_refs,
        "states": {
            "ieee": ieee["status"],
            "acm": acm["status"],
            **{seed_id: seeds[seed_id]["status"] for seed_id in SEED_SET_IDS},
            "source_windows": windows["status"],
        },
        "overall_status": "BLOCKED_EXTERNAL_INPUT",
        "blocking_reasons": external_gate["blocking_reasons"],
        "phase4a_compatibility": {
            "wave_schema_version": "1.1.0",
            "production_plan_accepted": True,
            "required_identification_sources": list(REQUIRED_IDENTIFICATION_SOURCES_V2),
            "required_support_sources": list(REQUIRED_SUPPORT_SOURCES_V2),
            "crossref_identification_allowed": False,
            "planning_contract_compatible": True,
            "required_inputs_available": (
                external_gate["ready"]
                and closure_gate["all_required_seed_manifests_validated"]
            ),
            "ready": False,
            "wave_instantiated": False,
            "readiness_issues": readiness_issues,
            "external_retrieval_execution": external_gate,
            "identification_set_closure": closure_gate,
            "post_closure_operations": {
                "incremental_normalization_during_retrieval_allowed": True,
                "allowed": False,
                "blocked_operations": list(POST_CLOSURE_OPERATIONS),
                "requires_identification_set_closure": True,
                "requires_completed_external_retrieval": True,
            },
        },
        "production_operations_created": [],
    }
    package = ProductionPrerequisitePackage(package_payload)
    package_payload["package_hash"] = package.package_hash()
    package = ProductionPrerequisitePackage(package_payload)
    return children, package


def save_prerequisite_payloads(
    *,
    root: str | Path,
    child_directory: str | Path,
    package_path: str | Path,
    children: dict[str, dict[str, Any]],
    package: ProductionPrerequisitePackage,
) -> None:
    root_path = Path(root)
    child_root = root_path / _safe_relative_path(str(child_directory))
    for filename, payload in children.items():
        atomic_write(child_root / filename, _pretty_json(payload).encode("utf-8"))
    atomic_write(
        root_path / _safe_relative_path(str(package_path)),
        package.to_json().encode("utf-8"),
    )
    package.validate(root=root_path)


def load_prerequisite_package(
    path: str | Path, *, root: str | Path
) -> ProductionPrerequisitePackage:
    package = ProductionPrerequisitePackage(
        json.loads(Path(path).read_text(encoding="utf-8"))
    )
    package.validate(root=root)
    return package


def _build_external_retrieval_gate(
    ieee: dict[str, Any],
    acm: dict[str, Any],
    windows: dict[str, Any],
) -> dict[str, Any]:
    issues: list[str] = []
    reasons: list[str] = []
    if ieee["status"] == "BLOCKED_CREDENTIAL":
        issues.append("IEEE_BLOCKED_CREDENTIAL")
        reasons.append("IEEE_XPLORE_API_KEY is absent and IEEE verification has not run")
    elif not ieee["verification_executed"]:
        issues.append("IEEE_VERIFICATION_NOT_EXECUTED")
        reasons.append("IEEE verification has not run")
    if acm["status"] != ACM_RETRIEVAL_EVIDENCE_COMPLETE:
        issues.append("ACM_OPERATOR_EVIDENCE_MISSING")
        reasons.append("ACM operator/access, sizing, and export evidence are not supplied")
    if windows["unresolved_items"]:
        issues.append("SUPPLEMENTAL_SOURCE_WINDOWS_UNKNOWN")
        unresolved_sources = sorted(
            {item.rsplit(":", 1)[-1] for item in windows["unresolved_items"]}
        )
        reasons.append(
            f"{', '.join(unresolved_sources)} final-query source-window states remain unsized"
        )
    return {
        "status": "READY" if not issues else "BLOCKED_EXTERNAL_INPUT",
        "ready": not issues,
        "required_identification_sources": list(EXTERNAL_IDENTIFICATION_SOURCES),
        "required_support_sources": list(REQUIRED_SUPPORT_SOURCES_V2),
        "blocking_issues": issues,
        "blocking_reasons": reasons,
        "nonblocking_offline_identification_source": PRIOR_SURVEY_SOURCE,
    }


def _build_identification_closure_gate(
    seeds: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    states = {seed_id: seeds[seed_id]["status"] for seed_id in SEED_SET_IDS}
    validated = [
        seed_id
        for seed_id in SEED_SET_IDS
        if seeds[seed_id]["completeness_state"] == "POPULATED_VALIDATED"
        and seeds[seed_id]["import_allowed"]
    ]
    pending_manifests = [
        seed_id for seed_id in SEED_SET_IDS if seed_id not in validated
    ]
    imported = [
        seed_id
        for seed_id in SEED_SET_IDS
        if seeds[seed_id]["expected_entry_count"] is not None
        and seeds[seed_id]["occurrences_created"]
        == seeds[seed_id]["expected_entry_count"]
    ]
    pending_imports = [seed_id for seed_id in SEED_SET_IDS if seed_id not in imported]
    issues = []
    reasons = []
    if pending_manifests:
        issues.append("PRIOR_SURVEY_SEED_MANIFESTS_UNPOPULATED")
        reasons.append(
            f"{', '.join(pending_manifests)} require prospective curator-populated manifests"
        )
    if pending_imports:
        issues.append("PRIOR_SURVEY_SEED_IMPORTS_INCOMPLETE")
        reasons.append(
            f"{', '.join(pending_imports)} have not been imported into the identification set"
        )
    satisfied = not pending_manifests and not pending_imports
    return {
        "status": "READY" if satisfied else "BLOCKED_REQUIRED_IDENTIFICATION_INPUT",
        "ready": satisfied,
        "required_seed_set_ids": list(SEED_SET_IDS),
        "seed_states": states,
        "validated_seed_set_ids": validated,
        "pending_manifest_seed_set_ids": pending_manifests,
        "imported_seed_set_ids": imported,
        "pending_import_seed_set_ids": pending_imports,
        "all_required_seed_manifests_validated": not pending_manifests,
        "all_required_seed_imports_complete": not pending_imports,
        "blocking_issues": issues,
        "blocking_reasons": reasons,
        "full_wave_finalization_allowed": False,
    }


def _build_ieee(
    plan: dict[str, Any], generated_at: str, credential_present: bool
) -> dict[str, Any]:
    queries = _queries_for(plan, "IEEEXplore")
    verification_requests = []
    for query in queries:
        request = query["request_specification"]
        params = dict(request["params"])
        params["max_records"] = 1
        params["start_record"] = 1
        verification_request = {
            "method": "GET",
            "endpoint": request["endpoint"],
            "params": params,
            "credential_reference": "IEEE_XPLORE_API_KEY",
        }
        verification_requests.append(
            {
                "family_id": query["family_id"],
                "query_id": query["query_id"],
                "query_text_sha256": query["query_text_sha256"],
                "request": verification_request,
                "request_hash": _sha256(
                    _canonical_json(verification_request).encode("utf-8")
                ),
                "execution_status": "NOT_EXECUTED",
            }
        )
    return {
        "schema_version": PREREQUISITE_SCHEMA_VERSION,
        "artifact_id": "star-ieee-readiness-v1",
        "artifact_version": "1.0.0",
        "status": "READY_FOR_VERIFICATION" if credential_present else "BLOCKED_CREDENTIAL",
        "assessed_at": generated_at,
        "credential": {
            "required_name": "IEEE_XPLORE_API_KEY",
            "present": credential_present,
            "value_persisted": False,
        },
        "api": {
            "endpoint": "https://ieeexploreapi.ieee.org/api/v1/search/articles",
            "mode": "official_metadata_api",
            "transport": "http",
        },
        "production_family_ids": list(PRODUCTION_SELECTION),
        "queries": [
            {
                "family_id": item["family_id"],
                "query_id": item["query_id"],
                "query_text": item["query_text"],
                "query_text_sha256": item["query_text_sha256"],
                "request_specification": item["request_specification"],
                "request_specification_hash": item["request_specification_hash"],
            }
            for item in queries
        ],
        "verification_requests": verification_requests,
        "verification_executed": False,
        "pagination": {
            "strategy": "start_record",
            "start_record_base": 1,
            "page_size": 200,
            "maximum_page_size": 200,
            "completion_proof": "ieee_totalfound_reconciled",
            "exact_total_required": True,
            "no_silent_truncation": True,
        },
        "expected_native_fields": [
            "article_number",
            "doi",
            "title",
            "authors_in_order",
            "abstract",
            "publication_title",
            "publication_year_or_date",
            "publication_type",
            "publisher",
            "access_type",
            "isbn",
            "issn",
            "pages",
            "urls",
            "author_terms",
            "index_terms",
            "rank",
            "totalfound",
            "totalsearched",
        ],
        "quota_access": {
            "status": "UNVERIFIED",
            "quota": None,
            "verification_required": True,
        },
        "content_policy": {
            "metadata_identification": "operationally_allowed_pending_access_verification",
            "abstract_text": "external_llm_use_unresolved",
            "external_hosted_llm_use": "unresolved",
            "abstract_not_required_as_model_input": True,
            "text_provenance_separate_from_identification_provenance": True,
        },
        "artifact_hash": None,
    }


def _build_acm(
    plan: dict[str, Any],
    generated_at: str,
    final_reconciliation: dict[str, Any] | None,
    root: Path,
) -> dict[str, Any]:
    queries = _queries_for(plan, "ACMDigitalLibrary")
    payload = {
        "schema_version": PREREQUISITE_SCHEMA_VERSION,
        "artifact_id": "star-acm-operator-spec-v1",
        "artifact_version": "1.0.0",
        "status": "REQUIRES_OPERATOR_INPUT",
        "created_at": generated_at,
        "workflow": {
            "kind": "human_operated_advanced_search_and_citation_export",
            "browser_automation": False,
            "scraping": False,
            "hidden_or_undocumented_api": False,
            "scope": "ACM Publications",
            "fields": ["Title", "Abstract", "Author Keywords"],
            "filters": {},
            "sort": "publicationDate asc",
            "export_format": "BibTeX",
        },
        "operator": {
            "operator_id": None,
            "operator_id_required": True,
            "institutional_access_tier": None,
            "institutional_access_status": "UNRESOLVED",
        },
        "queries": [
            {
                "family_id": item["family_id"],
                "query_id": item["query_id"],
                "query": item["query_text"],
                "query_sha256": item["query_text_sha256"],
                "scope": "ACM Publications",
                "fields": ["Title", "Abstract", "Author Keywords"],
                "filters": {},
                "sort": "publicationDate asc",
                "sizing_search_evidence": {
                    "status": "NOT_PERFORMED",
                    "ui_reported_count": None,
                    "search_timestamp_utc": None,
                    "query_url": None,
                    "screenshot_relative_path": None,
                    "screenshot_sha256": None,
                },
                "citation_export_evidence": {
                    "status": "NOT_EXPORTED",
                    "export_format": "BibTeX",
                    "ui_reported_total": None,
                    "chunks": [],
                    "chunk_schema": {
                        "required": [
                            "chunk_id",
                            "first_record",
                            "last_record",
                            "artifact_relative_path",
                            "artifact_sha256",
                            "exported_at_utc",
                        ],
                        "relative_traversal_safe_paths_only": True,
                    },
                    "contiguous_range_coverage_required": True,
                    "ui_total_reconciliation_required": True,
                },
            }
            for item in queries
        ],
        "readiness_requirements": [
            "operator identity recorded",
            "institutional access status recorded",
            "each query has UTC sizing/search evidence and UI count",
            "each export has relative artifact paths and SHA-256 hashes",
            "exported contiguous ranges reconcile exactly to the UI total",
        ],
        "artifact_hash": None,
    }
    if final_reconciliation is None:
        return payload

    final_families = {
        family["family_id"]: family for family in final_reconciliation["families"]
    }
    payload["status"] = ACM_RETRIEVAL_EVIDENCE_COMPLETE
    payload["operator"]["metadata_state"] = "LIMITED_NONBLOCKING_FOR_RETRIEVED_SET"
    payload["operator"]["retrieval_completeness_affected"] = False
    payload["execution_model"] = "FIELD_DECOMPOSED_STABLE_IDENTITY_UNION"
    payload["final_reconciliation"] = {
        "path": ACM_FINAL_RECONCILIATION_PATH,
        "manifest_id": final_reconciliation["manifest_id"],
        "manifest_version": final_reconciliation["manifest_version"],
        "manifest_hash": final_reconciliation["manifest_hash"],
        "status": final_reconciliation["status"],
        "raw_sha256": _sha256((root / ACM_FINAL_RECONCILIATION_PATH).read_bytes()),
        "byte_size": (root / ACM_FINAL_RECONCILIATION_PATH).stat().st_size,
    }
    for query in payload["queries"]:
        family = final_families[query["family_id"]]
        query["sizing_search_evidence"] = {
            "status": "FIELD_CHILD_EXECUTION_OBSERVATIONS_PRESERVED",
            "field_counts": {
                child["field_key"]: child["execution_time_provider_observation"]["count"]
                for child in family["children"]
            },
            "evidence_path": ACM_FINAL_RECONCILIATION_PATH,
            "evidence_manifest_hash": final_reconciliation["manifest_hash"],
            "later_verification_is_completeness_gate": False,
        }
        query["citation_export_evidence"] = {
            "status": "FIELD_CHILD_EXPORTS_RECONCILED_NOT_IMPORTED",
            "export_format": "BibTeX",
            "field_union_unique_count": family["field_union"][
                "unique_stable_identity_count"
            ],
            "field_union_digest_sha256": family["field_union"][
                "stable_identity_union_digest_sha256"
            ],
            "evidence_path": ACM_FINAL_RECONCILIATION_PATH,
            "evidence_manifest_hash": final_reconciliation["manifest_hash"],
            "production_import_performed": False,
        }
    payload["readiness_requirements"] = [
        "frozen field-query syntax validated",
        "selected raw BibTeX artifacts bound by byte size and SHA-256",
        "every physical BibTeX header explicitly accounted",
        "year-partition and field unions reconciled by stable identity",
        "later provider observations retained without temporal-invariance gating",
        "affirmative operator/export failures superseded without deleting history",
    ]
    return payload


def _build_seed(seed_set_id: str, root: Path) -> dict[str, Any]:
    if seed_set_id == "JFR25":
        tracked = root / "config/star_retrieval_prerequisites_v1/seed_jfr25.json"
        if tracked.is_file():
            current = json.loads(tracked.read_text(encoding="utf-8"))
            if current.get("status") == "POPULATED_VALIDATED_NOT_IMPORTED":
                current.pop("artifact_hash", None)
                return current
    return {
        "schema_version": PREREQUISITE_SCHEMA_VERSION,
        "artifact_id": f"star-seed-{seed_set_id.lower()}-v1",
        "artifact_version": "1.0.0",
        "status": "UNPOPULATED_REQUIRES_CURATOR_INPUT",
        "source_role": "prior_survey_seed",
        "seed_set_id": seed_set_id,
        "seed_set_version": "1.0.0-prospective",
        "originating_review": {
            "citation": None,
            "doi": None,
            "repository_recovery_state": "UNRECOVERABLE_FROM_EXPLICIT_REPOSITORY_EVIDENCE",
        },
        "curator": {"operator_id": None, "required": True},
        "extraction_method": None,
        "source_artifact": {
            "relative_path": None,
            "sha256": None,
            "required": True,
        },
        "expected_entry_count": None,
        "completeness_state": "UNPOPULATED",
        "entries": [],
        "entry_schema": {
            "required": ["entry_id", "ordinal", "raw_citation"],
            "optional": [
                "doi",
                "title",
                "source_page",
                "source_table",
                "source_reference_locator",
            ],
            "eligibility_or_taxonomy_fields_permitted": False,
        },
        "import_allowed": False,
        "occurrences_created": 0,
        "artifact_hash": None,
    }


def _build_source_windows(
    plan: dict[str, Any],
    root: Path,
    final_acm_reconciliation: dict[str, Any] | None,
) -> dict[str, Any]:
    loaded_runs: dict[str, dict[str, Any]] = {}
    items = []
    for family_id, variant in PRODUCTION_SELECTION.items():
        evidence_path, evidence_variant = SIZING_EVIDENCE[family_id]
        if variant != evidence_variant:
            raise ProductionPrerequisiteError("sizing evidence variant does not match plan")
        if evidence_path not in loaded_runs:
            loaded_runs[evidence_path] = json.loads(
                (root / evidence_path).read_text(encoding="utf-8")
            )
        run = loaded_runs[evidence_path]
        observations = {
            item["candidate_query_id"]: item for item in run["observations"]
        }
        for source in ("PubMed", "EuropePMC", "SemanticScholar", "arXiv"):
            candidate_id = f"candidate:{family_id}:{variant}:{source}"
            observation = observations.get(candidate_id)
            if observation is None or observation.get("reported_count") is None:
                raise ProductionPrerequisiteError(
                    f"missing frozen sizing evidence for {candidate_id}"
                )
            if observation.get("transport_status") != "succeeded":
                raise ProductionPrerequisiteError(
                    f"frozen sizing evidence did not succeed for {candidate_id}"
                )
            items.append(
                {
                    "family_id": family_id,
                    "variant_id": variant,
                    "source": source,
                    "state": "RESOLVED_CLEAR",
                    "reported_count": observation["reported_count"],
                    "hard_window": observation.get("hard_window"),
                    "window_status": observation.get("window_status"),
                    "evidence_path": evidence_path,
                    "evidence_run_hash": _run_hash(run),
                    "candidate_query_id": candidate_id,
                }
            )
        for source in ("IEEEXplore", "ACMDigitalLibrary"):
            acm_family = (
                next(
                    item
                    for item in final_acm_reconciliation["families"]
                    if item["family_id"] == family_id
                )
                if source == "ACMDigitalLibrary" and final_acm_reconciliation
                else None
            )
            items.append(
                {
                    "family_id": family_id,
                    "variant_id": variant,
                    "source": source,
                    "state": (
                        "RESOLVED_RETRIEVAL_EVIDENCE"
                        if acm_family
                        else "UNKNOWN_UNSIZED"
                    ),
                    "reported_count": (
                        acm_family["field_union"]["unique_stable_identity_count"]
                        if acm_family
                        else None
                    ),
                    "hard_window": _plan_query(plan, family_id, source)["hard_window"],
                    "window_status": (
                        "field_decomposed_export_complete" if acm_family else "unknown"
                    ),
                    "evidence_path": (
                        ACM_FINAL_RECONCILIATION_PATH if acm_family else None
                    ),
                    "evidence_run_hash": (
                        final_acm_reconciliation["manifest_hash"]
                        if acm_family
                        else None
                    ),
                    "candidate_query_id": None,
                }
            )
    return {
        "schema_version": PREREQUISITE_SCHEMA_VERSION,
        "artifact_id": "star-source-window-review-v1",
        "artifact_version": "1.0.0",
        "status": "UNRESOLVED_SUPPLEMENTAL_SOURCES",
        "derivation": (
            "frozen_v0_3_and_final_v0_4_sizing_plus_final_acm_retrieval_evidence"
        ),
        "automatic_partitioning": False,
        "known_overflows": [],
        "unresolved_items": [
            f"{item['family_id']}:{item['source']}"
            for item in items
            if item["state"] == "UNKNOWN_UNSIZED"
        ],
        "items": items,
        "artifact_hash": None,
    }


def _validate_ieee(data: dict[str, Any], plan: dict[str, Any]) -> None:
    if data["credential"]["required_name"] != "IEEE_XPLORE_API_KEY":
        raise ProductionPrerequisiteError("IEEE credential reference changed")
    if data["credential"]["value_persisted"] is not False:
        raise ProductionPrerequisiteError("IEEE credential value cannot be persisted")
    if data["status"] == "BLOCKED_CREDENTIAL" and data["credential"]["present"]:
        raise ProductionPrerequisiteError("IEEE credential state is contradictory")
    expected = _queries_for(plan, "IEEEXplore")
    actual = data["queries"]
    if [item["query_text"] for item in actual] != [item["query_text"] for item in expected]:
        raise ProductionPrerequisiteError("IEEE production queries changed")
    if data["verification_executed"] is not False:
        raise ProductionPrerequisiteError("IEEE verification must remain unexecuted")


def _validate_acm(data: dict[str, Any], plan: dict[str, Any], root: Path) -> None:
    workflow = data["workflow"]
    if workflow["kind"] != "human_operated_advanced_search_and_citation_export":
        raise ProductionPrerequisiteError("ACM workflow must remain human operated")
    if workflow["browser_automation"] or workflow["scraping"]:
        raise ProductionPrerequisiteError("ACM automation is prohibited")
    if workflow["scope"] != "ACM Publications" or workflow["filters"] != {}:
        raise ProductionPrerequisiteError("ACM frozen scope or filters changed")
    expected = _queries_for(plan, "ACMDigitalLibrary")
    if [item["query"] for item in data["queries"]] != [
        item["query_text"] for item in expected
    ]:
        raise ProductionPrerequisiteError("ACM production queries changed")
    if data["operator"]["operator_id"] is not None:
        raise ProductionPrerequisiteError("ACM operator input was fabricated")
    if data["status"] == ACM_RETRIEVAL_EVIDENCE_COMPLETE:
        final = data.get("final_reconciliation", {})
        if final.get("path") != ACM_FINAL_RECONCILIATION_PATH:
            raise ProductionPrerequisiteError("ACM final reconciliation binding changed")
        final_path = root / _safe_relative_path(final["path"])
        if (
            len(final_path.read_bytes()) != final.get("byte_size")
            or _sha256(final_path.read_bytes()) != final.get("raw_sha256")
        ):
            raise ProductionPrerequisiteError("ACM final reconciliation bytes changed")
        final_manifest = load_acm_final_reconciliation_manifest(
            final_path, root=root, verify_artifacts=True
        )
        if final_manifest["manifest_hash"] != final.get("manifest_hash"):
            raise ProductionPrerequisiteError("ACM final reconciliation hash changed")
        if data["operator"].get("retrieval_completeness_affected") is not False:
            raise ProductionPrerequisiteError("ACM operator limitation semantics changed")
        if any(
            item["citation_export_evidence"].get("production_import_performed")
            for item in data["queries"]
        ):
            raise ProductionPrerequisiteError("ACM prerequisite performed production import")


def _validate_seed(data: dict[str, Any], seed_set_id: str) -> None:
    if data["seed_set_id"] != seed_set_id:
        raise ProductionPrerequisiteError("seed-set ID changed")
    if seed_set_id == "JFR25" and data["status"] == "POPULATED_VALIDATED_NOT_IMPORTED":
        reconciliation = data["reconciliation"]
        expected = {
            "application_rows": 87,
            "study_rows": 59,
            "raw_category_rows": 146,
            "shared_source_id_count": 8,
            "shared_source_ids": ["7", "22", "29", "35", "44", "52", "53", "93"],
            "unique_members": 138,
            "identity_key": "companion_site_explicit_source_id",
            "fuzzy_or_title_matching_used": False,
            "equation": "87 + 59 - 8 = 138",
        }
        if reconciliation != expected:
            raise ProductionPrerequisiteError("JFR25 membership reconciliation changed")
        entries = data["entries"]
        if data["expected_entry_count"] != 138 or len(entries) != 138:
            raise ProductionPrerequisiteError("JFR25 member count changed")
        if len({entry["source_member_id"] for entry in entries}) != 138:
            raise ProductionPrerequisiteError("JFR25 source IDs are not unique")
        if [entry["ordinal"] for entry in entries] != list(range(1, 139)):
            raise ProductionPrerequisiteError("JFR25 ordinals are not contiguous")
        if data["originating_review"]["arxiv_id"] != "2501.08500":
            raise ProductionPrerequisiteError("JFR25 review identity changed")
        if not data["import_allowed"] or data["occurrences_created"] != 0:
            raise ProductionPrerequisiteError("JFR25 import/occurrence state is invalid")
        forbidden = {
            "star_eligibility",
            "assistance_modes",
            "visualization_modalities",
            "task_annotations",
            "synthesis_priority",
            "corpus_membership",
        }
        if forbidden & _nested_keys(data):
            raise ProductionPrerequisiteError("JFR25 contains STAR decision fields")
        return
    if data["status"] != "UNPOPULATED_REQUIRES_CURATOR_INPUT":
        raise ProductionPrerequisiteError("seed manifest cannot be marked populated")
    if data["entries"] or data["expected_entry_count"] is not None:
        raise ProductionPrerequisiteError("seed members cannot be inferred")
    if data["originating_review"]["citation"] is not None:
        raise ProductionPrerequisiteError("seed citation cannot be inferred")
    if data["import_allowed"] or data["occurrences_created"] != 0:
        raise ProductionPrerequisiteError("unpopulated seed cannot create occurrences")


def _validate_windows(data: dict[str, Any], plan: dict[str, Any]) -> None:
    if data["automatic_partitioning"] or data["known_overflows"]:
        raise ProductionPrerequisiteError("source-window review invented a partition/overflow")
    expected_pairs = {
        (family_id, source)
        for family_id in PRODUCTION_SELECTION
        for source in (
            "PubMed",
            "EuropePMC",
            "SemanticScholar",
            "arXiv",
            "IEEEXplore",
            "ACMDigitalLibrary",
        )
    }
    actual_pairs = {(item["family_id"], item["source"]) for item in data["items"]}
    if actual_pairs != expected_pairs:
        raise ProductionPrerequisiteError("source-window coverage changed")
    for item in data["items"]:
        plan_item = _plan_query(plan, item["family_id"], item["source"])
        if item["variant_id"] != plan_item["variant_id"]:
            raise ProductionPrerequisiteError("source-window variant changed")


def _load_final_acm_reconciliation(root: Path) -> dict[str, Any] | None:
    path = root / ACM_FINAL_RECONCILIATION_PATH
    if not path.is_file():
        return None
    return load_acm_final_reconciliation_manifest(
        path, root=root, verify_artifacts=True
    )


def _validate_phase4a_compatibility(
    data: dict[str, Any], *, children: dict[str, dict[str, Any]]
) -> None:
    if data["required_identification_sources"] != list(
        REQUIRED_IDENTIFICATION_SOURCES_V2
    ):
        raise ProductionPrerequisiteError("Phase 4A identification inventory changed")
    if data["required_support_sources"] != list(REQUIRED_SUPPORT_SOURCES_V2):
        raise ProductionPrerequisiteError("Phase 4A support inventory changed")
    if data["crossref_identification_allowed"]:
        raise ProductionPrerequisiteError("Crossref cannot become identification")
    if data["ready"] or data["wave_instantiated"]:
        raise ProductionPrerequisiteError("prerequisite validation cannot instantiate a wave")
    for source in data["required_identification_sources"]:
        if source not in SOURCE_CONTRACTS:
            raise ProductionPrerequisiteError(f"Phase 4A source contract missing: {source}")

    external = data["external_retrieval_execution"]
    if external["required_identification_sources"] != list(
        EXTERNAL_IDENTIFICATION_SOURCES
    ):
        raise ProductionPrerequisiteError("external retrieval source inventory changed")
    if external["required_support_sources"] != list(REQUIRED_SUPPORT_SOURCES_V2):
        raise ProductionPrerequisiteError("external retrieval support inventory changed")
    if external["nonblocking_offline_identification_source"] != PRIOR_SURVEY_SOURCE:
        raise ProductionPrerequisiteError("prior-survey phase classification changed")
    if any("SEED" in issue for issue in external["blocking_issues"]):
        raise ProductionPrerequisiteError(
            "prior-survey seeds cannot block external retrieval execution"
        )
    expected_external = _build_external_retrieval_gate(
        children["star-ieee-readiness-v1"],
        children["star-acm-operator-spec-v1"],
        children["star-source-window-review-v1"],
    )
    if external != expected_external:
        raise ProductionPrerequisiteError("external retrieval gate is not evidence-derived")

    seeds = {
        seed_id: children[f"star-seed-{seed_id.lower()}-v1"]
        for seed_id in SEED_SET_IDS
    }
    closure = data["identification_set_closure"]
    expected_closure = _build_identification_closure_gate(seeds)
    if closure != expected_closure:
        raise ProductionPrerequisiteError(
            "identification closure gate is not seed-evidence-derived"
        )
    if closure["ready"] and not (
        closure["all_required_seed_manifests_validated"]
        and closure["all_required_seed_imports_complete"]
    ):
        raise ProductionPrerequisiteError(
            "identification closure cannot precede seed validation and import"
        )

    downstream = data["post_closure_operations"]
    if downstream != {
        "incremental_normalization_during_retrieval_allowed": True,
        "allowed": False,
        "blocked_operations": list(POST_CLOSURE_OPERATIONS),
        "requires_identification_set_closure": True,
        "requires_completed_external_retrieval": True,
    }:
        raise ProductionPrerequisiteError(
            "post-closure operation gate is inconsistent with identification closure"
        )
    if closure["full_wave_finalization_allowed"]:
        raise ProductionPrerequisiteError(
            "prerequisite validation cannot authorize full-wave finalization"
        )
    if data["required_inputs_available"] != (
        external["ready"] and closure["all_required_seed_manifests_validated"]
    ):
        raise ProductionPrerequisiteError("full-wave input readiness is inconsistent")
    expected_issues = [*external["blocking_issues"], *closure["blocking_issues"]]
    if data["readiness_issues"] != expected_issues:
        raise ProductionPrerequisiteError("full-wave readiness issues are inconsistent")


def _validate_expected_plan(
    plan_path: Path, plan: dict[str, Any], root: Path
) -> None:
    raw = plan_path.read_bytes()
    if plan.get("plan_version") != EXPECTED_PLAN_VERSION:
        raise ProductionPrerequisiteError("unexpected production plan version")
    if plan.get("plan_hash") != EXPECTED_PLAN_HASH:
        raise ProductionPrerequisiteError("unexpected production plan canonical hash")
    if _sha256(raw) != EXPECTED_PLAN_RAW_SHA256:
        raise ProductionPrerequisiteError("unexpected production plan raw hash")
    if plan_path.resolve() != (root / "config/star_production_query_plan_v1.json").resolve():
        raise ProductionPrerequisiteError("unexpected production plan path")


def _queries_for(plan: dict[str, Any], source: str) -> list[dict[str, Any]]:
    items = [item for item in plan["source_queries"] if item["source"] == source]
    if [item["family_id"] for item in items] != list(PRODUCTION_SELECTION):
        raise ProductionPrerequisiteError(f"{source} must have exactly five frozen queries")
    for item in items:
        if item["variant_id"] != PRODUCTION_SELECTION[item["family_id"]]:
            raise ProductionPrerequisiteError(f"{source} production variant changed")
    return items


def _plan_query(plan: dict[str, Any], family_id: str, source: str) -> dict[str, Any]:
    matches = [
        item
        for item in plan["source_queries"]
        if item["family_id"] == family_id and item["source"] == source
    ]
    if len(matches) != 1:
        raise ProductionPrerequisiteError("frozen plan query lookup is not unique")
    return matches[0]


def _run_hash(data: dict[str, Any]) -> str:
    material = dict(data)
    material.pop("run_hash", None)
    return _sha256(_canonical_json(material).encode("utf-8"))


def _finalize_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["artifact_hash"] = _artifact_hash(result)
    return result


def _artifact_hash(payload: dict[str, Any]) -> str:
    material = dict(payload)
    material.pop("artifact_hash", None)
    return _sha256(_canonical_json(material).encode("utf-8"))


def _file_reference(
    path: Path, root: Path, canonical_hash: str, version: str
) -> dict[str, Any]:
    relative = path.resolve().relative_to(root.resolve()).as_posix()
    raw = path.read_bytes()
    return {
        "path": relative,
        "version": version,
        "raw_sha256": _sha256(raw),
        "canonical_hash": canonical_hash,
        "byte_size": len(raw),
    }


def _validate_file_reference(path: Path, reference: dict[str, Any]) -> None:
    if not path.is_file():
        raise ProductionPrerequisiteError(f"referenced artifact missing: {path}")
    raw = path.read_bytes()
    if _sha256(raw) != reference["raw_sha256"]:
        raise ProductionPrerequisiteError(f"raw artifact hash mismatch: {path}")
    if len(raw) != reference["byte_size"]:
        raise ProductionPrerequisiteError(f"artifact byte size mismatch: {path}")


def _safe_relative_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ProductionPrerequisiteError("artifact paths must be traversal-safe and relative")
    return path


def _pretty_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _nested_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            key
            for item in value.values()
            for key in _nested_keys(item)
        }
    if isinstance(value, list):
        return {key for item in value for key in _nested_keys(item)}
    return set()
