"""Deterministic offline planning for the Phase 4A external-retrieval wave."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.acm_field_execution import load_acm_final_reconciliation_manifest
from h2h_lit.checkpoint import CheckpointStore, atomic_write
from h2h_lit.http import HttpClient, RequestsHttpClient
from h2h_lit.models import ProcessingStatus
from h2h_lit.pagination import PageRequest, RateLimiter, RetryPolicy, native_identifier
from h2h_lit.production_prerequisites import load_prerequisite_package
from h2h_lit.production_query_plan import load_production_query_plan
from h2h_lit.production_wave import (
    EXTERNAL_IDENTIFICATION_SOURCES_V2,
    EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
    REQUIRED_SUPPORT_SOURCES_V2,
    SOURCE_CONTRACTS,
    ArtifactKind,
    PaginationExpectation,
    ProductionQueryFamily,
    ProductionRetrievalWave,
    ProductionWaveStatus,
    RequiredArtifact,
    ResultWindowStatus,
    compute_query_plan_hash,
    preflight_production_wave,
    save_production_wave,
)
from h2h_lit.query_development import load_semantic_control_set
from h2h_lit.retrieval import (
    PAGINATED_SOURCE_ADAPTERS,
    RetrievalQuerySpec,
    _query_plan_hash,
    execute_paginated_retrieval_run,
    load_review_dataset,
    save_review_dataset,
)
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    RecordOccurrence,
    RetrievalAttemptStatus,
    RetrievalCompletionStatus,
    canonicalize_occurrences,
)
from h2h_lit.sources.acm_dl import import_acm_selected_reconciliation

PLAN_PATH = "config/star_production_query_plan_v1.json"
PREREQUISITE_PATH = "config/star_retrieval_prerequisites_v1.json"
IEEE_READINESS_PATH = "config/star_retrieval_prerequisites_v1/ieee_readiness.json"
ACM_READINESS_PATH = "config/star_retrieval_prerequisites_v1/acm_operator_spec.json"
SOURCE_WINDOWS_PATH = "config/star_retrieval_prerequisites_v1/source_window_review.json"
ACM_RECONCILIATION_PATH = (
    "provenance/star_acm_field_execution_2026-09-03_final_reconciliation_manifest.json"
)
OUTPUT_ROOT = "outputs/production/star-external-retrieval-wave-001"
WAVE_PATH = f"{OUTPUT_ROOT}/planned_wave.json"
PREFLIGHT_PATH = f"{OUTPUT_ROOT}/preflight.json"
WAVE_ID = "star-external-retrieval-wave-001"
WAVE_VERSION = "1.0.0"
READY_STATUS = "READY_FOR_EXTERNAL_RETRIEVAL_EXECUTION"
EXECUTION_ROOT = f"{OUTPUT_ROOT}/execution"
EXECUTION_STATE_PATH = f"{EXECUTION_ROOT}/execution_state.json"
IEEE_CREDENTIAL_NAME = "IEEE_XPLORE_API_KEY"
IEEE_DAILY_REQUEST_LIMIT = 200
IEEE_TOTAL_DRIFT_RECOVERY_STATUS = "PROVIDER_TOTAL_DRIFT_RECOVERY_READY_TO_RESUME"
IEEE_TOTAL_DRIFT_ERROR_PREFIX = "source exact total changed during pagination: "
IEEE_TOTAL_DRIFT_EXPECTED = (
    (16842, 16841, 6, 1199, 1200),
    (6061, 6060, 2, 399, 400),
    (9787, 9786, 2, 399, 400),
    (8410, 8409, 11, 2199, 2200),
    (4695, 4694, 2, 399, 400),
)
IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS = 23
IEEE_REPEATED_WINDOW_RECOVERY_STATUS = (
    "REPEATED_WINDOW_RECOVERY_READY_TO_RESUME"
)
IEEE_REPEATED_WINDOW_EXPECTED = (
    (25, 4993, 4797, 197, 4800),
    (10, 1997, 1799, 199, 1800),
    (11, 2195, 1998, 198, 2000),
    (20, 3997, 3799, 199, 3800),
    (21, 4197, 3999, 199, 4000),
)
IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES = (
    (
        16842, 16842, 16842, 16842, 16842, 16841,
        16845, 16845, 16845, 16845, 16845, 16845, 16845, 16845, 16845,
        16845, 16845, 16845, 16845, 16845, 16845, 16845, 16845, 16842, 16842,
    ),
    (6061, 6060, 6061, 6061, 6061, 6061, 6061, 6061, 6060, 6060),
    (9787, 9786, 9787, 9787, 9787, 9787, 9787, 9787, 9787, 9785, 9785),
    (
        8410, 8410, 8410, 8410, 8410, 8410, 8410, 8410, 8410, 8410,
        8409, 8411, 8411, 8411, 8411, 8411, 8411, 8411, 8410, 8410,
    ),
    (
        4695, 4694, 4696, 4696, 4696, 4696, 4696, 4696, 4696, 4696,
        4696, 4696, 4696, 4696, 4696, 4696, 4696, 4696, 4696, 4695, 4695,
    ),
)
IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS = 87
IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES = 82
IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS = 5
IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS = 92
IEEE_VERIFICATION_PATH = (
    "provenance/star_ieee_xplore_verification_2026-09-04_manifest.json"
)
SEMANTIC_CONTROL_PATH = "config/star_query_semantic_controls_v0_3.json"
SEMANTIC_CONTROL_GATE_PATH = (
    f"{EXECUTION_ROOT}/SemanticScholar/control_gate/control_gate.json"
)
PUBMED_TRANSPORT_RETRY_STATUS = "TRANSPORT_RETRY_AUTHORIZED_NOT_STARTED"
PUBMED_PARSER_RECOVERY_STATUS = "PARSER_RECOVERY_COMPLETE_READY_TO_RESUME"
ARXIV_RATE_LIMIT_RECOVERY_STATUS = "RATE_LIMIT_RECOVERY_READY_TO_RESUME"
ARXIV_RATE_LIMIT_EXPECTED_ATTEMPT_KINDS = (
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "HTTP_429"),
    ("HTTP_429", "HTTP_429", "HTTP_429"),
    ("HTTP_429", "HTTP_429", "HTTP_429"),
)
ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS = 15
ARXIV_RATE_LIMIT_EXPECTED_RESPONSES = 7
ARXIV_MIXED_RECOVERY_STATUS = (
    "MIXED_TRANSPORT_RATE_LIMIT_RECOVERY_READY_TO_RESUME"
)
ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256 = (
    "0fc8478938d772252304e8d0402cc41e125cb7b2e4a993bd4ffc42eb25cda4ae"
)
ARXIV_MIXED_EXPECTED_ATTEMPT_KINDS = (
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("HTTP_429",),
    (),
)
ARXIV_MIXED_EXPECTED_ATTEMPTS = 10
ARXIV_MIXED_EXPECTED_RESPONSES = 1
EUROPE_PMC_TERMINAL_RECOVERY_STATUS = "TERMINAL_SENTINEL_RECOVERY_COMPLETE"
EUROPE_PMC_TERMINAL_ERROR = (
    "PaginationError: Europe PMC returned an empty non-terminal cursor page"
)
EUROPE_PMC_RECOVERY_EXPECTED_COUNTS = (3972, 1500, 3629, 1209, 1717)
EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS = 27
PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS = (
    "33085405",
    "39836822",
    "38781405",
    "38484096",
    "37816103",
    "40198015",
)
TRANSPORT_ENVIRONMENT_FAILURE_TYPES = frozenset(
    {"ConnectionError", "ConnectTimeout", "ProxyError", "ReadTimeout", "SSLError", "Timeout"}
)


class ExternalRetrievalWaveError(ValueError):
    """Raised when an external-only production wave cannot pass offline preflight."""


def build_external_retrieval_wave(*, root: str | Path) -> ProductionRetrievalWave:
    """Bind the frozen external queries to a PLANNED schema-1.1 wave."""

    root_path = Path(root).resolve()
    plan_path = root_path / PLAN_PATH
    prerequisite_path = root_path / PREREQUISITE_PATH
    plan = load_production_query_plan(plan_path, root=root_path)
    package = load_prerequisite_package(prerequisite_path, root=root_path)
    phase = package.payload["phase4a_compatibility"]
    external_gate = phase["external_retrieval_execution"]
    closure_gate = phase["identification_set_closure"]
    if external_gate.get("status") != "READY" or external_gate.get("ready") is not True:
        raise ExternalRetrievalWaveError("external retrieval execution gate is not READY")
    if external_gate.get("required_identification_sources") != list(
        EXTERNAL_IDENTIFICATION_SOURCES_V2
    ):
        raise ExternalRetrievalWaveError("external source inventory changed")
    if package.payload["states"].get("ieee") != "VERIFIED_READY_FOR_RETRIEVAL":
        raise ExternalRetrievalWaveError("IEEE is not verified ready for retrieval")
    if package.payload["states"].get("acm") != (
        "RETRIEVAL_EVIDENCE_COMPLETE_NOT_IMPORTED"
    ):
        raise ExternalRetrievalWaveError("ACM retrieval evidence is not complete")
    if closure_gate.get("status") != "BLOCKED_REQUIRED_IDENTIFICATION_INPUT":
        raise ExternalRetrievalWaveError(
            "identification closure must remain blocked until seed completion"
        )
    if closure_gate.get("pending_manifest_seed_set_ids") != ["EBK25", "FP19"]:
        raise ExternalRetrievalWaveError("unexpected prior-survey seed readiness state")

    windows = _load_json(root_path / SOURCE_WINDOWS_PATH)
    window_by_query = {
        (item["family_id"], item["source"]): item for item in windows["items"]
    }
    acm_path = root_path / ACM_RECONCILIATION_PATH
    acm = load_acm_final_reconciliation_manifest(
        acm_path, root=root_path, verify_artifacts=False
    )
    acm_by_query = {item["parent_query_id"]: item for item in acm["families"]}
    roles = {item["source"]: item for item in plan.payload["source_roles"]}

    query_families = []
    for query in plan.payload["source_queries"]:
        source = query["source"]
        if source not in EXTERNAL_IDENTIFICATION_SOURCES_V2:
            continue
        window = window_by_query.get((query["family_id"], source))
        if window is None or not str(window.get("state", "")).startswith("RESOLVED_"):
            raise ExternalRetrievalWaveError(
                f"source window is not resolved for {query['query_id']}"
            )
        if query["query_text_sha256"] != _sha256(query["query_text"].encode("utf-8")):
            raise ExternalRetrievalWaveError(
                f"query-text hash mismatch for {query['query_id']}"
            )
        request_hash = _hash_payload(query["request_specification"])
        if query["request_specification_hash"] != request_hash:
            raise ExternalRetrievalWaveError(
                f"request-specification hash mismatch for {query['query_id']}"
            )
        role = roles.get(source)
        if role is None or role.get("transport") != query["transport"]:
            raise ExternalRetrievalWaveError(
                f"source-role binding mismatch for {query['query_id']}"
            )
        query_families.append(
            _query_family(
                query,
                reported_count=int(window["reported_count"]),
                acm_by_query=acm_by_query,
                acm_manifest_path=acm_path,
            )
        )

    expected_ids = [
        query["query_id"]
        for query in plan.payload["source_queries"]
        if query["source"] in EXTERNAL_IDENTIFICATION_SOURCES_V2
    ]
    if [item.query_family_id for item in query_families] != expected_ids:
        raise ExternalRetrievalWaveError("external query inventory is incomplete or reordered")
    if len(query_families) != 30:
        raise ExternalRetrievalWaveError("external wave must contain exactly 30 queries")

    wave = ProductionRetrievalWave(
        schema_version="1.1.0",
        wave_id=WAVE_ID,
        wave_version=WAVE_VERSION,
        query_plan_version=plan.payload["plan_version"],
        query_plan_hash="",
        required_sources=list(EXTERNAL_IDENTIFICATION_SOURCES_V2),
        support_sources=list(REQUIRED_SUPPORT_SOURCES_V2),
        query_families=query_families,
        status=ProductionWaveStatus.PLANNED,
        retrieval_cutoff_date=None,
        metadata={
            "execution_scope": EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
            "phase": "Phase 4A external identification retrieval",
            "deferred_identification_sources": ["PriorSurveySeed"],
            "deferred_seed_set_ids": ["EBK25", "JFR25", "FP19"],
            "identification_set_closure_allowed": False,
            "incremental_normalization_allowed": True,
            "final_global_deduplication_allowed": False,
            "retrieval_cutoff_established": False,
            "frozen_source_roles": plan.payload["source_roles"],
            "semantic_scholar_control_gate": {
                "required_gate": plan.payload["semantic_controls"]["required_gate"],
                "gate_behavior": plan.payload["semantic_controls"]["gate_behavior"],
                "control_set": _file_reference(
                    root_path / plan.payload["semantic_controls"]["path"], root_path
                ),
                "must_pass_before_candidate_requests": True,
            },
            "bindings": {
                "production_query_plan": {
                    **_file_reference(plan_path, root_path),
                    "canonical_hash": plan.plan_hash(),
                },
                "retrieval_prerequisites": {
                    **_file_reference(prerequisite_path, root_path),
                    "canonical_hash": package.package_hash(),
                },
                "ieee_readiness": _json_artifact_reference(
                    root_path / IEEE_READINESS_PATH, root_path, "artifact_hash"
                ),
                "acm_readiness": _json_artifact_reference(
                    root_path / ACM_READINESS_PATH, root_path, "artifact_hash"
                ),
                "source_windows": _json_artifact_reference(
                    root_path / SOURCE_WINDOWS_PATH, root_path, "artifact_hash"
                ),
                "acm_final_reconciliation": {
                    **_file_reference(acm_path, root_path),
                    "canonical_hash": acm["manifest_hash"],
                },
            },
            "production_operations": {
                "external_retrieval_executed": False,
                "acm_import_executed": False,
                "seed_import_executed": False,
                "identification_set_closed": False,
                "final_global_deduplication_executed": False,
                "prisma_generated": False,
                "screening_executed": False,
                "corpus_created": False,
            },
        },
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    return wave


def preflight_external_retrieval_wave(
    wave: ProductionRetrievalWave, *, root: str | Path
) -> dict[str, Any]:
    """Run the existing offline wave preflight plus phase-specific assertions."""

    root_path = Path(root).resolve()
    _verify_bound_files(wave, root_path)
    package = _load_json(root_path / PREREQUISITE_PATH)
    if package.get("package_hash") != wave.metadata["bindings"][
        "retrieval_prerequisites"
    ]["canonical_hash"]:
        raise ExternalRetrievalWaveError("prerequisite package hash changed")
    external = package.get("phase4a_compatibility", {}).get(
        "external_retrieval_execution", {}
    )
    if external.get("status") != "READY" or external.get("ready") is not True:
        raise ExternalRetrievalWaveError("external retrieval gate is no longer READY")
    report = preflight_production_wave(
        wave,
        manifest_root=root_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
    )
    if not report.ready or report.finalizable or report.execution_complete:
        raise ExternalRetrievalWaveError(
            "external wave did not reach a planned-only ready preflight"
        )
    if wave.retrieval_cutoff_date is not None:
        raise ExternalRetrievalWaveError("external preflight cannot establish a cutoff")
    burdens = _request_burdens(wave)
    acm = load_acm_final_reconciliation_manifest(
        root_path / ACM_RECONCILIATION_PATH,
        root=root_path,
        verify_artifacts=True,
    )
    selected_artifacts = [
        artifact
        for family in acm["families"]
        for child in family["children"]
        for artifact in child["selected_artifacts"]
    ]
    malformed = sum(item["malformed_entry_count"] for item in selected_artifacts)
    raw_occurrences = sum(item["total_accounted_entry_count"] for item in selected_artifacts)
    acm_unique_by_family = {
        item["family_id"]: item["field_union"]["unique_stable_identity_count"]
        for item in acm["families"]
    }
    return {
        "schema_version": "1.0.0",
        "artifact_id": "star-external-retrieval-wave-001-preflight",
        "status": READY_STATUS,
        "wave_id": wave.wave_id,
        "wave_manifest_hash": wave.manifest_hash(),
        "production_query_plan": wave.metadata["bindings"]["production_query_plan"],
        "retrieval_prerequisites": wave.metadata["bindings"][
            "retrieval_prerequisites"
        ],
        "external_gate": package["phase4a_compatibility"][
            "external_retrieval_execution"
        ],
        "identification_closure": package["phase4a_compatibility"][
            "identification_set_closure"
        ],
        "query_inventory": [
            {
                "query_id": item.query_family_id,
                "source": item.source_database,
                "query_sha256": _sha256(item.query_text.encode("utf-8")),
                "query_version": item.query_version,
                "transport_kind": item.transport_kind.value,
                "reported_count_evidence": item.native_parameters[
                    "observed_source_count"
                ],
                "frozen_source_role": next(
                    role["role"]
                    for role in wave.metadata["frozen_source_roles"]
                    if role["source"] == item.source_database
                ),
            }
            for item in wave.query_families
        ],
        "source_query_counts": {
            source: sum(
                item.source_database == source for item in wave.query_families
            )
            for source in wave.required_sources
        },
        "request_burden": burdens,
        "credentials": {
            "required_at_live_execution": ["IEEE_XPLORE_API_KEY"],
            "credential_values_read": False,
            "credential_values_persisted": False,
            "ieee_credential_previously_verified": True,
        },
        "acm_artifact_import": {
            "live_requests": 0,
            "manifest_path": ACM_RECONCILIATION_PATH,
            "manifest_hash": acm["manifest_hash"],
            "selected_artifact_count": len(selected_artifacts),
            "raw_selected_occurrence_count": raw_occurrences,
            "malformed_but_identified_record_count": malformed,
            "unique_identity_count_by_family": acm_unique_by_family,
            "import_executed": False,
            "selected_artifacts_only": True,
            "nonselected_artifacts_preserved_but_excluded": True,
        },
        "semantic_scholar": {
            **wave.metadata["semantic_scholar_control_gate"],
            "control_request_count": 6,
            "candidate_mode": "bulk",
            "completion_is_set_by": "semantic_scholar_bulk_token_exhausted",
        },
        "wave_preflight": report.to_dict(),
        "safeguards": {
            "planned_only": True,
            "network_used": False,
            "production_retrieval_executed": False,
            "production_retrieval_cutoff": None,
            "prior_survey_seed_imported": False,
            "identification_set_closed": False,
            "final_global_deduplication_executed": False,
            "prisma_generated": False,
            "screening_executed": False,
            "corpus_modified": False,
        },
    }


def save_external_retrieval_preflight(*, root: str | Path) -> dict[str, Any]:
    """Persist only deterministic PLANNED wave and offline preflight artifacts."""

    root_path = Path(root).resolve()
    wave = build_external_retrieval_wave(root=root_path)
    preflight = preflight_external_retrieval_wave(wave, root=root_path)
    wave_path = _safe_output_path(root_path, WAVE_PATH)
    preflight_path = _safe_output_path(root_path, PREFLIGHT_PATH)
    wave_file_sha256 = save_production_wave(wave_path, wave)
    preflight["wave_file"] = {
        **_file_reference(wave_path, root_path),
        "sha256_from_save": wave_file_sha256,
    }
    content = _pretty_json(preflight).encode("utf-8")
    atomic_write(preflight_path, content)
    return {
        **preflight,
        "preflight_file": {
            "path": PREFLIGHT_PATH,
            "byte_size": len(content),
            "raw_sha256": _sha256(content),
        },
    }


def _query_family(
    query: Mapping[str, Any],
    *,
    reported_count: int,
    acm_by_query: Mapping[str, dict[str, Any]],
    acm_manifest_path: Path,
) -> ProductionQueryFamily:
    source = str(query["source"])
    contract = SOURCE_CONTRACTS[source]
    completeness = query["completeness"]
    native_parameters = _native_parameters(query, reported_count=reported_count)
    required_artifact = None
    if source == "ACMDigitalLibrary":
        reconciled = acm_by_query.get(str(query["query_id"]))
        if reconciled is None:
            raise ExternalRetrievalWaveError(
                f"ACM reconciliation is missing {query['query_id']}"
            )
        reconciled_count = reconciled["field_union"]["unique_stable_identity_count"]
        if reconciled_count != reported_count:
            raise ExternalRetrievalWaveError(
                f"ACM window count differs from field union for {query['query_id']}"
            )
        required_artifact = RequiredArtifact(
            kind=ArtifactKind.ACM_EXPORT_MANIFEST,
            manifest_path=ACM_RECONCILIATION_PATH,
            manifest_sha256=_sha256(acm_manifest_path.read_bytes()),
            expected_total=reported_count,
            expected_chunks=[],
        )
    return ProductionQueryFamily(
        query_family_id=str(query["query_id"]),
        source_database=source,
        source_role=contract.role,
        identification_route=contract.route,
        transport_kind=contract.transport,
        adapter_id=str(completeness["adapter_id"]),
        adapter_version=str(completeness["adapter_version"]),
        query_version=f"1.0.0:{query['variant_id']}",
        query_text=str(query["query_text"]),
        native_parameters=native_parameters,
        pagination=PaginationExpectation(
            strategy=str(completeness["pagination_strategy"]),
            adapter_version=str(completeness["adapter_version"]),
            completion_proofs=list(completeness["completion_proofs"]),
            exact_total_required=bool(completeness["exact_total_required"]),
            maximum_supported_results=completeness["maximum_supported_results"],
        ),
        required_credentials=["api_key"] if source == "IEEEXplore" else [],
        content_policy=dict(query["content_policy"]),
        result_window_status=ResultWindowStatus.CLEAR,
        required_artifact=required_artifact,
    )


def _native_parameters(
    query: Mapping[str, Any], *, reported_count: int
) -> dict[str, Any]:
    source = query["source"]
    request = query["request_specification"]
    values: dict[str, Any]
    if source == "PubMed":
        values = {**request["form"], "page_size": 200}
    elif source == "EuropePMC":
        values = dict(request["params"])
    elif source == "SemanticScholar":
        values = {**request["params"], "mode": query["mode"]}
    elif source in {"arXiv", "IEEEXplore"}:
        values = dict(request["params"])
    elif source == "ACMDigitalLibrary":
        values = {
            "field_selections": request["fields"],
            "collection_scope": "acm_publications",
            "filters": request["filters"],
            "sort": request["sort"],
            "export_format": request["export_format"],
            "ui_reported_total": reported_count,
        }
    else:  # pragma: no cover - source inventory is validated before construction
        raise ExternalRetrievalWaveError(f"unsupported external source {source}")
    values["observed_source_count"] = reported_count
    values["frozen_request_specification_hash"] = query[
        "request_specification_hash"
    ]
    return values


def _request_burdens(wave: ProductionRetrievalWave) -> dict[str, Any]:
    page_sizes = {
        "PubMed": 200,
        "EuropePMC": 1000,
        "SemanticScholar": 1000,
        "arXiv": 2000,
        "IEEEXplore": 200,
    }
    by_source: dict[str, dict[str, Any]] = {}
    for source, page_size in page_sizes.items():
        families = []
        for item in wave.query_families:
            if item.source_database != source:
                continue
            count = int(item.native_parameters["observed_source_count"])
            pages = math.ceil(count / page_size)
            requests = pages + (1 if source == "PubMed" else 0)
            families.append(
                {
                    "query_id": item.query_family_id,
                    "reported_count_evidence": count,
                    "page_size": page_size,
                    "estimated_candidate_requests": requests,
                    "pubmed_identity_enumeration_requests": (
                        1 if source == "PubMed" else 0
                    ),
                }
            )
        candidate_requests = sum(
            item["estimated_candidate_requests"] for item in families
        )
        control_requests = 6 if source == "SemanticScholar" else 0
        by_source[source] = {
            "families": families,
            "estimated_candidate_requests": candidate_requests,
            "required_control_requests": control_requests,
            "estimated_total_requests": candidate_requests + control_requests,
            "estimate_basis": (
                "prior source-count evidence; bulk token exhaustion is authoritative"
                if source == "SemanticScholar"
                else "prior exact source-count evidence; live counts may change"
            ),
        }
    by_source["ACMDigitalLibrary"] = {
        "families": [],
        "estimated_candidate_requests": 0,
        "required_control_requests": 0,
        "estimated_total_requests": 0,
        "estimate_basis": "offline import from completed bound artifacts",
    }
    return {
        "by_source": by_source,
        "estimated_http_requests": sum(
            item["estimated_total_requests"] for item in by_source.values()
        ),
        "counts_are_planning_estimates_not_completion_proofs": True,
    }


def _safe_output_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ExternalRetrievalWaveError("output path must be traversal-safe and relative")
    resolved = (root / path).resolve()
    expected_root = (root / OUTPUT_ROOT).resolve()
    if not resolved.is_relative_to(expected_root):
        raise ExternalRetrievalWaveError("output escaped the external-wave namespace")
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _file_reference(path: Path, root: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "byte_size": len(raw),
        "raw_sha256": _sha256(raw),
    }


def _json_artifact_reference(
    path: Path, root: Path, canonical_hash_key: str
) -> dict[str, Any]:
    payload = _load_json(path)
    return {
        **_file_reference(path, root),
        "canonical_hash": payload[canonical_hash_key],
    }


def _verify_bound_files(wave: ProductionRetrievalWave, root: Path) -> None:
    bindings = wave.metadata.get("bindings", {})
    expected_names = {
        "production_query_plan",
        "retrieval_prerequisites",
        "ieee_readiness",
        "acm_readiness",
        "source_windows",
        "acm_final_reconciliation",
    }
    if set(bindings) != expected_names:
        raise ExternalRetrievalWaveError("external wave evidence bindings changed")
    for name, reference in bindings.items():
        value = Path(str(reference.get("path", "")))
        if value.is_absolute() or ".." in value.parts or not value.parts:
            raise ExternalRetrievalWaveError(f"unsafe evidence path for {name}")
        path = (root / value).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ExternalRetrievalWaveError(f"missing bound evidence for {name}")
        raw = path.read_bytes()
        if (
            len(raw) != reference.get("byte_size")
            or _sha256(raw) != reference.get("raw_sha256")
        ):
            raise ExternalRetrievalWaveError(f"bound evidence changed for {name}")


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


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_persisted_external_preflight(
    *, root: str | Path
) -> tuple[ProductionRetrievalWave, dict[str, Any]]:
    """Rebuild all frozen bindings and reject any persisted preflight drift."""

    root_path = Path(root).resolve()
    wave_path = root_path / WAVE_PATH
    preflight_path = root_path / PREFLIGHT_PATH
    expected_wave = build_external_retrieval_wave(root=root_path)
    expected_wave_bytes = (expected_wave.to_json() + "\n").encode("utf-8")
    if wave_path.read_bytes() != expected_wave_bytes:
        raise ExternalRetrievalWaveError("persisted planned wave differs from frozen inputs")
    preflight = _load_json(preflight_path)
    if preflight.get("status") != READY_STATUS:
        raise ExternalRetrievalWaveError("persisted external preflight is not ready")
    if preflight.get("wave_manifest_hash") != expected_wave.manifest_hash():
        raise ExternalRetrievalWaveError("persisted preflight wave hash mismatch")
    wave_ref = preflight.get("wave_file", {})
    if (
        wave_ref.get("path") != WAVE_PATH
        or wave_ref.get("byte_size") != len(expected_wave_bytes)
        or wave_ref.get("raw_sha256") != _sha256(expected_wave_bytes)
    ):
        raise ExternalRetrievalWaveError("persisted preflight wave-file binding mismatch")
    regenerated = preflight_external_retrieval_wave(expected_wave, root=root_path)
    for key in (
        "status",
        "wave_id",
        "wave_manifest_hash",
        "production_query_plan",
        "retrieval_prerequisites",
        "external_gate",
        "identification_closure",
        "query_inventory",
        "source_query_counts",
        "request_burden",
        "credentials",
        "acm_artifact_import",
        "semantic_scholar",
        "safeguards",
    ):
        if preflight.get(key) != regenerated.get(key):
            raise ExternalRetrievalWaveError(
                f"persisted preflight field changed: {key}"
            )
    return expected_wave, preflight


def authorize_arxiv_rate_limit_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize a fresh arXiv episode from the immutable timeout/429 failure."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["arXiv"]
    if source_state["status"] == ARXIV_RATE_LIMIT_RECOVERY_STATUS:
        _validate_authorized_arxiv_rate_limit_recovery(source_state, root_path, wave)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery requires the known terminal FAILED component"
        )
    if (
        source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or source_state.get("requests_this_session")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or source_state.get("pause_reason") is not None
    ):
        raise ExternalRetrievalWaveError(
            "arXiv source state is not the known timeout/rate-limit failure"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery cannot alter a closed external retrieval wave"
        )
    if source_state.get("execution_episodes"):
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery requires the original failed lineage"
        )

    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("failed arXiv state lacks a checkpoint binding")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    dataset = load_review_dataset(failed_checkpoint)
    recovery = _validate_failed_arxiv_rate_limit_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        root=root_path,
        wave=wave,
    )
    if source_state.get("failure_reason") != "; ".join(
        dataset.retrieval_runs[0].errors
    ):
        raise ExternalRetrievalWaveError("arXiv source/checkpoint failure reason changed")
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "arXiv"
    }
    recovered_at = timestamp()

    episode_1 = {
        "episode_number": 1,
        "episode_id": "arXiv-episode-001",
        "run_id": dataset.retrieval_runs[0].run_id,
        "status": "FAILED",
        "failure_classification": "TRANSPORT_TIMEOUT_AND_PROVIDER_RATE_LIMIT",
        "checkpoint_path": failed_checkpoint.parent.relative_to(root_path).as_posix(),
        "checkpoint_dataset": dict(checkpoint_reference),
        "attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
        "transport_timeout_count": 8,
        "http_429_count": ARXIV_RATE_LIMIT_EXPECTED_RESPONSES,
        "successful_http_response_count": 0,
        "occurrence_count": 0,
        "source_attempt_manifest_hash": _hash_payload(
            {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
        ),
        "raw_responses": recovery["raw_response_bindings"],
        "query_attempt_signatures": recovery["query_attempt_signatures"],
        "started_at_utc": source_state.get("last_session_started_at_utc"),
        "completed_at_utc": source_state.get("last_session_completed_at_utc"),
        "failure_reason": source_state.get("failure_reason"),
        "immutable": True,
    }

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/arXiv/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 checkpoint exists without valid state lineage"
        )

    run = dataset.retrieval_runs[0]
    dataset.retrieval_pages = []
    dataset.retrieval_attempts = []
    dataset.occurrences = []
    dataset.canonical_records = []
    dataset.duplicate_decisions = []
    run.retrieval_started_at = recovered_at
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.errors = [
        "offline arXiv rate-limit recovery complete; live resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata.pop("pause_metadata", None)
    run.metadata["offline_arxiv_rate_limit_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
        "source_raw_response_count": ARXIV_RATE_LIMIT_EXPECTED_RESPONSES,
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": recovery["raw_response_bindings"]}
        ),
        "restart_states": recovery["restart_states"],
        "network_used": False,
    }
    for query, restart in zip(
        dataset.source_queries, recovery["restart_states"], strict=True
    ):
        query.retrieval_started_at = recovered_at
        query.retrieval_ended_at = recovered_at
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.PLANNED
        query.page = None
        query.cursor = None
        query.result_count = 0
        query.errors = []
        query.page_ids = []
        query.source_reported_total = None
        query.total_is_exact = False
        query.completion_proof = None
        query.metadata.pop("pause_state", None)
        query.metadata.pop("pause_reason", None)
        query.metadata.pop("pause_metadata", None)
        query.metadata["offline_arxiv_rate_limit_recovery"] = {
            "source_episode_number": 1,
            "restart_state": dict(restart["request_state"]),
            "request_hash": restart["request_hash"],
        }
    dataset.validate()

    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError("arXiv recovery checkpoint hash disagreement")
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "arXiv episode-1 checkpoint changed during recovery"
        )
    _verify_arxiv_raw_response_bindings(
        root_path, recovery["raw_response_bindings"]
    )

    episode_2 = {
        "episode_number": 2,
        "episode_id": "arXiv-episode-002",
        "run_id": run.run_id,
        "status": ARXIV_RATE_LIMIT_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": "OFFLINE_ARXIV_TIMEOUT_AND_RATE_LIMIT_RECOVERY",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": episode_1["source_attempt_manifest_hash"],
        "source_attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
        "source_raw_responses": recovery["raw_response_bindings"],
        "restart_states": recovery["restart_states"],
        "network_used": False,
        "immutable": False,
    }
    source_state["execution_episodes"] = [episode_1, episode_2]
    source_state.update(
        {
            "status": ARXIV_RATE_LIMIT_RECOVERY_STATUS,
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": 0,
            "attempt_count": 0,
            "preserved_source_attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
            "preserved_source_raw_response_count": (
                ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
            ),
            "requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_RATE_LIMIT_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if {
        key: value for key, value in state["sources"].items() if key != "arXiv"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery changed another source component"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_arxiv_mixed_state_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize episode 3 from the exact response-free-timeout/429 pause."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["arXiv"]
    if source_state["status"] == ARXIV_MIXED_RECOVERY_STATUS:
        _validate_authorized_arxiv_mixed_state_recovery(
            source_state, root_path, wave
        )
        return state
    if (
        source_state.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
        or source_state.get("active_episode_number") != 2
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or source_state.get("requests_this_session")
        != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or source_state.get("pause_reason")
        != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or source_state.get("failure_reason") is not None
        or source_state.get("pause_metadata")
        != {
            "source_database": "arXiv",
            "http_status": 429,
            "retry_after_header_present": False,
            "retry_after": None,
        }
    ):
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery requires the exact episode-2 timeout/429 pause"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery cannot alter a closed external retrieval wave"
        )

    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery requires exactly two prior episodes"
        )
    episode_1, episode_2 = episodes
    _validate_arxiv_episode_1_provenance(
        episode_1=episode_1,
        episode_2=episode_2,
        root=root_path,
        wave=wave,
    )
    if (
        episode_2.get("episode_number") != 2
        or episode_2.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
        or episode_2.get("recovery_of_episode_number") != 1
        or episode_2.get("authorization_reason")
        != "OFFLINE_ARXIV_TIMEOUT_AND_RATE_LIMIT_RECOVERY"
        or episode_2.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or episode_2.get("frozen_query_plan_hash") != wave.query_plan_hash
        or episode_2.get("immutable") is not False
        or episode_2.get("network_used") is not False
        or episode_2.get("attempt_count") != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or episode_2.get("completed_query_count") != 0
        or episode_2.get("occurrence_count") != 0
        or episode_2.get("pause_reason")
        != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or episode_2.get("failure_reason") is not None
        or source_state.get("active_checkpoint_path")
        != episode_2.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != episode_2.get("checkpoint_dataset")
        or source_state.get("active_run_id") != episode_2.get("run_id")
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 execution lineage changed"
        )

    checkpoint_reference = episode_2.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("arXiv episode 2 lacks a checkpoint binding")
    if (
        checkpoint_reference.get("raw_sha256")
        != ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 checkpoint does not match the authorized production digest"
        )
    checkpoint_path = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(checkpoint_path, checkpoint_reference, root_path)
    checkpoint_bytes = checkpoint_path.read_bytes()
    checkpoint_payload = json.loads(checkpoint_bytes)
    dataset = load_review_dataset(checkpoint_path)
    recovery = _validate_arxiv_mixed_state_checkpoint(
        dataset=dataset,
        checkpoint_dir=checkpoint_path.parent,
        root=root_path,
        wave=wave,
    )
    if (
        dataset.retrieval_runs[0].run_id != episode_2.get("run_id")
        or recovery["restart_states"] != episode_2.get("restart_states")
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 frozen run/request identity changed"
        )
    episode_2_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": checkpoint_payload.get("retrieval_attempts", [])}
    )
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "arXiv"
    }
    recovered_at = timestamp()

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/arXiv/episodes/episode-003/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "arXiv episode-3 checkpoint exists without valid state lineage"
        )

    run = dataset.retrieval_runs[0]
    dataset.retrieval_pages = []
    dataset.retrieval_attempts = []
    dataset.occurrences = []
    dataset.canonical_records = []
    dataset.duplicate_decisions = []
    run.retrieval_started_at = recovered_at
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.errors = [
        "offline arXiv mixed transport/rate-limit recovery complete; live resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata.pop("pause_metadata", None)
    run.metadata.pop("session_request_count", None)
    provenance_bindings = [
        {
            "episode_number": 1,
            "checkpoint_dataset": dict(episode_1["checkpoint_dataset"]),
            "attempt_manifest_hash": episode_1["source_attempt_manifest_hash"],
            "raw_response_manifest_hash": _hash_payload(
                {"responses": episode_1["raw_responses"]}
            ),
        },
        {
            "episode_number": 2,
            "checkpoint_dataset": dict(checkpoint_reference),
            "attempt_manifest_hash": episode_2_attempt_manifest_hash,
            "raw_response_manifest_hash": _hash_payload(
                {"responses": recovery["raw_response_bindings"]}
            ),
        },
    ]
    run.metadata["offline_arxiv_mixed_state_recovery"] = {
        "recovery_episode_number": 3,
        "source_episode_number": 2,
        "source_episodes": provenance_bindings,
        "restart_states": recovery["restart_states"],
        "network_used": False,
    }
    for query, restart in zip(
        dataset.source_queries, recovery["restart_states"], strict=True
    ):
        query.retrieval_started_at = recovered_at
        query.retrieval_ended_at = recovered_at
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.PLANNED
        query.page = None
        query.cursor = None
        query.result_count = 0
        query.errors = []
        query.page_ids = []
        query.source_reported_total = None
        query.total_is_exact = False
        query.completion_proof = None
        query.metadata.pop("pause_state", None)
        query.metadata.pop("pause_reason", None)
        query.metadata.pop("pause_metadata", None)
        query.metadata["offline_arxiv_mixed_state_recovery"] = {
            "source_episode_number": 2,
            "restart_state": dict(restart["request_state"]),
            "request_hash": restart["request_hash"],
        }
    dataset.validate()

    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery checkpoint hash disagreement"
        )
    if checkpoint_path.read_bytes() != checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 checkpoint changed during recovery"
        )
    _verify_arxiv_raw_response_bindings(
        root_path, episode_1["raw_responses"]
    )
    _verify_arxiv_mixed_raw_response_bindings(
        root_path, recovery["raw_response_bindings"]
    )

    episode_2["immutable"] = True
    episode_3 = {
        "episode_number": 3,
        "episode_id": "arXiv-episode-003",
        "run_id": run.run_id,
        "status": ARXIV_MIXED_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": (
            "OFFLINE_ARXIV_MIXED_TRANSPORT_AND_RATE_LIMIT_RECOVERY"
        ),
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episodes": provenance_bindings,
        "source_attempt_count": (
            ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
            + ARXIV_MIXED_EXPECTED_ATTEMPTS
        ),
        "source_raw_response_count": (
            ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
            + ARXIV_MIXED_EXPECTED_RESPONSES
        ),
        "episode_2_attempt_manifest_hash": episode_2_attempt_manifest_hash,
        "episode_2_raw_responses": recovery["raw_response_bindings"],
        "episode_2_query_attempt_signatures": recovery[
            "query_attempt_signatures"
        ],
        "restart_states": recovery["restart_states"],
        "network_used": False,
        "immutable": False,
    }
    source_state["execution_episodes"].append(episode_3)
    source_state.update(
        {
            "status": ARXIV_MIXED_RECOVERY_STATUS,
            "active_episode_number": 3,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": 0,
            "attempt_count": 0,
            "preserved_source_attempt_count": (
                ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
                + ARXIV_MIXED_EXPECTED_ATTEMPTS
            ),
            "preserved_source_raw_response_count": (
                ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
                + ARXIV_MIXED_EXPECTED_RESPONSES
            ),
            "requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_MIXED_STATE_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    source_state.pop("pause_metadata", None)
    if {
        key: value for key, value in state["sources"].items() if key != "arXiv"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery changed another source component"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_pubmed_transport_retry(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize a new PubMed episode after a response-free transport failure."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["PubMed"]
    if source_state["status"] == PUBMED_TRANSPORT_RETRY_STATUS:
        _validate_authorized_pubmed_retry(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "PubMed transport retry requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "PubMed transport retry cannot alter a closed external retrieval wave"
        )

    checkpoint_path = source_state.get("checkpoint_dataset", {}).get("path")
    if not checkpoint_path:
        raise ExternalRetrievalWaveError("failed PubMed state lacks a checkpoint binding")
    checkpoint = _safe_output_path(root_path, checkpoint_path)
    _verify_file_reference(checkpoint, source_state["checkpoint_dataset"], root_path)
    dataset = load_review_dataset(checkpoint)
    _validate_response_free_pubmed_transport_failure(
        dataset=dataset,
        checkpoint_dir=checkpoint.parent,
        wave=wave,
    )

    episodes = source_state.setdefault("execution_episodes", [])
    if episodes:
        active_number = int(source_state.get("active_episode_number", len(episodes)))
        active = next(
            (item for item in episodes if item["episode_number"] == active_number), None
        )
        if active is None or active.get("status") != "FAILED":
            raise ExternalRetrievalWaveError(
                "PubMed retry episode lineage does not match the failed component"
            )
        active["checkpoint_dataset"] = dict(source_state["checkpoint_dataset"])
        active["immutable"] = True
    else:
        active_number = 1
        run = dataset.retrieval_runs[0]
        episodes.append(
            {
                "episode_number": active_number,
                "episode_id": "PubMed-episode-001",
                "run_id": run.run_id,
                "status": "FAILED",
                "failure_classification": (
                    "TRANSPORT_ENVIRONMENT_FAILURE_BEFORE_PROVIDER_RESPONSE"
                ),
                "checkpoint_path": checkpoint.parent.relative_to(root_path).as_posix(),
                "checkpoint_dataset": dict(source_state["checkpoint_dataset"]),
                "attempt_count": len(dataset.retrieval_attempts),
                "successful_http_response_count": 0,
                "raw_provider_response_count": 0,
                "occurrence_count": 0,
                "canonical_record_count": 0,
                "request_hashes": sorted(
                    {item.request_hash for item in dataset.retrieval_attempts}
                ),
                "started_at_utc": source_state.get("last_session_started_at_utc"),
                "completed_at_utc": source_state.get("last_session_completed_at_utc"),
                "immutable": True,
            }
        )

    next_number = active_number + 1
    retry_checkpoint_path = (
        f"{EXECUTION_ROOT}/PubMed/episodes/episode-{next_number:03d}/checkpoint"
    )
    authorized_at = timestamp()
    retry_episode = {
        "episode_number": next_number,
        "episode_id": f"PubMed-episode-{next_number:03d}",
        "run_id": f"{WAVE_ID}:PubMed:episode-{next_number:03d}",
        "status": PUBMED_TRANSPORT_RETRY_STATUS,
        "retry_of_episode_number": active_number,
        "authorization_reason": (
            "TRANSPORT_ENVIRONMENT_FAILURE_BEFORE_PROVIDER_RESPONSE"
        ),
        "authorized_at_utc": authorized_at,
        "checkpoint_path": retry_checkpoint_path,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "prior_episode_checkpoint_sha256": source_state["checkpoint_dataset"][
            "raw_sha256"
        ],
        "immutable": False,
    }
    episodes.append(retry_episode)
    source_state.update(
        {
            "status": PUBMED_TRANSPORT_RETRY_STATUS,
            "active_episode_number": next_number,
            "active_run_id": retry_episode["run_id"],
            "active_checkpoint_path": retry_checkpoint_path,
            "checkpoint_path": retry_checkpoint_path,
            "checkpoint_dataset": None,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": 0,
            "attempt_count": 0,
            "requests_this_session": 0,
            "pause_reason": None,
            "failure_reason": None,
            "last_session_started_at_utc": None,
            "last_session_completed_at_utc": None,
        }
    )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_pubmed_parser_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Reparse immutable PubMed episode-2 responses into a resumable checkpoint."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["PubMed"]
    if source_state["status"] == PUBMED_PARSER_RECOVERY_STATUS:
        _validate_authorized_pubmed_parser_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery cannot alter a closed external retrieval wave"
        )

    episodes = source_state.get("execution_episodes", [])
    if source_state.get("active_episode_number") != 2 or len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery is restricted to failed execution episode 2"
        )
    failed_episode = next(
        (item for item in episodes if item.get("episode_number") == 2), None
    )
    if (
        failed_episode is None
        or failed_episode.get("status") != "FAILED"
        or not failed_episode.get("immutable")
    ):
        raise ExternalRetrievalWaveError(
            "PubMed episode 2 is not preserved as an immutable failed episode"
        )
    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference or failed_episode.get("checkpoint_dataset") != checkpoint_reference:
        raise ExternalRetrievalWaveError("PubMed episode-2 checkpoint lineage changed")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_checkpoint_dir = failed_checkpoint.parent
    dataset = load_review_dataset(failed_checkpoint)
    recovered_at = timestamp()
    recovery = _reparse_failed_pubmed_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint_dir,
        wave=wave,
        recovered_at=recovered_at,
    )

    next_number = 3
    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/PubMed/episodes/episode-{next_number:03d}/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "PubMed parser-recovery checkpoint already exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = []
    for relative_path, expected_hash in recovery["raw_response_references"]:
        response_path = Path(relative_path)
        if (
            response_path.is_absolute()
            or ".." in response_path.parts
            or not response_path.parts
            or response_path.parts[0] != "responses"
        ):
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 raw response path is unsafe"
            )
        source = failed_checkpoint_dir / response_path
        raw = source.read_bytes()
        if _sha256(raw) != expected_hash:
            raise ExternalRetrievalWaveError(
                f"PubMed episode-2 raw response hash mismatch: {relative_path}"
            )
        destination = recovery_checkpoint_dir / response_path
        atomic_write(destination, raw)
        if destination.read_bytes() != raw:
            raise ExternalRetrievalWaveError(
                f"PubMed recovery raw-response copy mismatch: {relative_path}"
            )
        raw_bindings.append(
            {
                "episode_2_path": source.relative_to(root_path).as_posix(),
                "recovery_copy_path": destination.relative_to(root_path).as_posix(),
                "byte_size": len(raw),
                "raw_sha256": expected_hash,
            }
        )

    run = dataset.retrieval_runs[0]
    run.metadata["offline_parser_recovery"] = {
        "recovery_episode_number": next_number,
        "source_episode_number": 2,
        "source_checkpoint": dict(checkpoint_reference),
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "recovered_pmids": list(recovery["recovered_pmids"]),
        "remaining_efetch_request_count": recovery[
            "remaining_efetch_request_count"
        ],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError("PubMed recovery checkpoint hash disagreement")
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError("PubMed episode-2 checkpoint changed during recovery")
    for binding in raw_bindings:
        source = root_path / binding["episode_2_path"]
        raw = source.read_bytes()
        if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 raw response changed during recovery"
            )

    recovery_episode = {
        "episode_number": next_number,
        "episode_id": f"PubMed-episode-{next_number:03d}",
        "run_id": run.run_id,
        "status": PUBMED_PARSER_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": "OFFLINE_PUBMED_XML_PARSER_CORRECTION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_raw_responses": raw_bindings,
        "recovered_pmids": list(recovery["recovered_pmids"]),
        "already_fetched_occurrence_count": len(dataset.occurrences),
        "remaining_efetch_request_count": recovery[
            "remaining_efetch_request_count"
        ],
        "remaining_efetch_batches": recovery["remaining_efetch_batches"],
        "network_used": False,
        "immutable": False,
    }
    episodes.append(recovery_episode)
    source_state.update(
        {
            "status": PUBMED_PARSER_RECOVERY_STATUS,
            "active_episode_number": next_number,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": sum(
                query.completion_status is RetrievalCompletionStatus.COMPLETE
                for query in dataset.source_queries
            ),
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": 0,
            "pause_reason": "OFFLINE_PARSER_RECOVERY_COMPLETE; LIVE_EFETCH_RESUME_REQUIRED",
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_europe_pmc_terminal_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Reconcile Europe PMC terminal sentinels from immutable stored responses."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["EuropePMC"]
    if source_state["status"] == "COMPLETE":
        _validate_authorized_europe_pmc_terminal_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "Europe PMC terminal recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery cannot alter a closed external retrieval wave"
        )
    if source_state.get("execution_episodes"):
        raise ExternalRetrievalWaveError(
            "Europe PMC terminal recovery requires the original failed lineage"
        )

    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("Europe PMC failed checkpoint is absent")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    source_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    )
    dataset = load_review_dataset(failed_checkpoint)
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "EuropePMC"
    }
    recovered_at = timestamp()
    recovery = _reparse_failed_europe_pmc_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        wave=wave,
        recovered_at=recovered_at,
    )

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/EuropePMC/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery checkpoint exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = []
    for relative_path, expected_hash in recovery["raw_response_references"]:
        response_path = Path(relative_path)
        if (
            response_path.is_absolute()
            or ".." in response_path.parts
            or not response_path.parts
            or response_path.parts[0] != "responses"
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC raw response path is unsafe"
            )
        source = failed_checkpoint.parent / response_path
        raw = source.read_bytes()
        if _sha256(raw) != expected_hash:
            raise ExternalRetrievalWaveError(
                f"Europe PMC raw response hash mismatch: {relative_path}"
            )
        destination = recovery_checkpoint_dir / response_path
        atomic_write(destination, raw)
        if destination.read_bytes() != raw:
            raise ExternalRetrievalWaveError(
                f"Europe PMC recovery response copy mismatch: {relative_path}"
            )
        raw_bindings.append(
            {
                "failed_episode_path": source.relative_to(root_path).as_posix(),
                "recovery_copy_path": destination.relative_to(root_path).as_posix(),
                "byte_size": len(raw),
                "raw_sha256": expected_hash,
            }
        )

    run = dataset.retrieval_runs[0]
    run.metadata["offline_terminal_sentinel_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "query_occurrence_counts": recovery["query_occurrence_counts"],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery checkpoint hash disagreement"
        )
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "Europe PMC failed checkpoint changed during recovery"
        )
    for binding in raw_bindings:
        source = root_path / binding["failed_episode_path"]
        raw = source.read_bytes()
        if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "Europe PMC failed raw response changed during recovery"
            )

    failed_episode = {
        "episode_number": 1,
        "episode_id": "EuropePMC-episode-001",
        "run_id": run.run_id,
        "status": "FAILED",
        "failure_classification": (
            "TERMINAL_SENTINEL_PARSER_REJECTION_AFTER_EXACT_COUNT"
        ),
        "started_at_utc": source_state.get("last_session_started_at_utc"),
        "completed_at_utc": source_state.get("last_session_completed_at_utc"),
        "checkpoint_path": source_state["checkpoint_path"],
        "checkpoint_dataset": dict(checkpoint_reference),
        "attempt_count": source_state["attempt_count"],
        "occurrence_count": source_state["occurrence_count"],
        "completed_query_count": source_state["completed_query_count"],
        "failure_reason": source_state["failure_reason"],
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "immutable": True,
    }
    recovery_episode = {
        "episode_number": 2,
        "episode_id": "EuropePMC-episode-002",
        "run_id": run.run_id,
        "status": EUROPE_PMC_TERMINAL_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": "OFFLINE_REPEATED_CURSOR_TERMINAL_RECONCILIATION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_responses": raw_bindings,
        "query_occurrence_counts": recovery["query_occurrence_counts"],
        "occurrence_count": len(dataset.occurrences),
        "attempt_count": len(dataset.retrieval_attempts),
        "completed_query_count": 5,
        "network_used": False,
        "immutable": True,
    }
    source_state.update(
        {
            "status": "COMPLETE",
            "execution_episodes": [failed_episode, recovery_episode],
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 5,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": 0,
            "pause_reason": None,
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if {
        key: value for key, value in state["sources"].items() if key != "EuropePMC"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery changed another source component"
        )
    _finalize_execution_state(state, recovered_at)
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery unexpectedly closed the external retrieval wave"
        )
    _save_execution_state(state_path, state)
    return state


def authorize_ieee_total_drift_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Rebuild failed IEEE pagination as a mutable-total resumable lineage."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["IEEEXplore"]
    if source_state["status"] == IEEE_TOTAL_DRIFT_RECOVERY_STATUS:
        _validate_authorized_ieee_total_drift_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery cannot alter a closed external retrieval wave"
        )
    if source_state.get("execution_episodes"):
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery requires the original failed lineage"
        )

    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("IEEE failed checkpoint is absent")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    source_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    )
    dataset = load_review_dataset(failed_checkpoint)
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "IEEEXplore"
    }
    quota_before = json.loads(
        json.dumps(source_state.get("ieee_quota"), sort_keys=True)
    )
    recovered_at = timestamp()
    recovery = _reparse_failed_ieee_total_drift_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        wave=wave,
        recovered_at=recovered_at,
    )

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/IEEEXplore/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "IEEE recovery checkpoint exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = _copy_recovery_raw_responses(
        root=root_path,
        source_checkpoint_dir=failed_checkpoint.parent,
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=recovery["raw_response_references"],
        source_path_key="failed_episode_path",
        error_prefix="IEEE",
    )

    run = dataset.retrieval_runs[0]
    run.metadata["offline_provider_total_drift_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError("IEEE recovery checkpoint hash disagreement")
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "IEEE failed checkpoint changed during recovery"
        )
    _verify_recovery_raw_bindings(root_path, raw_bindings, "failed_episode_path")

    failed_episode = {
        "episode_number": 1,
        "episode_id": "IEEEXplore-episode-001",
        "run_id": run.run_id,
        "status": "FAILED",
        "failure_classification": "MUTABLE_PROVIDER_TOTAL_REJECTED_AS_INCONSISTENT",
        "started_at_utc": source_state.get("last_session_started_at_utc"),
        "completed_at_utc": source_state.get("last_session_completed_at_utc"),
        "checkpoint_path": source_state["checkpoint_path"],
        "checkpoint_dataset": dict(checkpoint_reference),
        "attempt_count": source_state["attempt_count"],
        "occurrence_count": source_state["occurrence_count"],
        "completed_query_count": source_state["completed_query_count"],
        "failure_reason": source_state["failure_reason"],
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "immutable": True,
    }
    recovery_episode = {
        "episode_number": 2,
        "episode_id": "IEEEXplore-episode-002",
        "run_id": run.run_id,
        "status": IEEE_TOTAL_DRIFT_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": "OFFLINE_MUTABLE_PROVIDER_TOTAL_RECONCILIATION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_responses": raw_bindings,
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "known_daily_calls_preserved": quota_before["known_calls_after_session"],
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": IEEE_TOTAL_DRIFT_RECOVERY_STATUS,
            "execution_episodes": [failed_episode, recovery_episode],
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": 0,
            "pause_reason": "OFFLINE_TOTAL_DRIFT_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED",
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if source_state.get("ieee_quota") != quota_before:
        raise ExternalRetrievalWaveError("IEEE recovery changed the daily quota ledger")
    if {
        key: value for key, value in state["sources"].items() if key != "IEEEXplore"
    } != other_sources_before:
        raise ExternalRetrievalWaveError("IEEE recovery changed another source component")
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_ieee_repeated_window_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Exclude proven repeated IEEE windows in a new offline episode."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["IEEEXplore"]
    if source_state["status"] == IEEE_REPEATED_WINDOW_RECOVERY_STATUS:
        _validate_authorized_ieee_repeated_window_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery cannot alter a closed retrieval wave"
        )

    episodes = source_state.get("execution_episodes", [])
    if (
        len(episodes) != 2
        or source_state.get("active_episode_number") != 2
        or any(not episode.get("immutable") for episode in episodes)
        or episodes[0].get("episode_number") != 1
        or episodes[1].get("episode_number") != 2
        or episodes[1].get("status") != "FAILED"
    ):
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery requires immutable failed episodes 1 and 2"
        )
    for episode in episodes:
        reference = episode.get("checkpoint_dataset")
        if not reference:
            raise ExternalRetrievalWaveError("IEEE episode checkpoint is absent")
        _verify_file_reference(
            _safe_output_path(root_path, reference["path"]), reference, root_path
        )

    quota_before = json.loads(
        json.dumps(source_state.get("ieee_quota"), sort_keys=True)
    )
    if (
        not quota_before
        or quota_before.get("daily_limit") != IEEE_DAILY_REQUEST_LIMIT
        or quota_before.get("known_calls_after_session")
        != IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS
    ):
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery quota evidence changed"
        )
    checkpoint_reference = source_state.get("checkpoint_dataset")
    if checkpoint_reference != episodes[1].get("checkpoint_dataset"):
        raise ExternalRetrievalWaveError("IEEE active episode-2 checkpoint changed")
    failed_checkpoint = _safe_output_path(root_path, checkpoint_reference["path"])
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    source_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    )
    dataset = load_review_dataset(failed_checkpoint)
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "IEEEXplore"
    }
    recovered_at = timestamp()
    recovery = _reparse_failed_ieee_repeated_window_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        root=root_path,
        wave=wave,
        recovered_at=recovered_at,
    )

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/IEEEXplore/episodes/episode-003/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 checkpoint exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = _copy_recovery_raw_responses(
        root=root_path,
        source_checkpoint_dir=failed_checkpoint.parent,
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=recovery["raw_response_references"],
        source_path_key="episode_2_path",
        error_prefix="IEEE repeated-window",
    )
    binding_by_source = {
        binding["episode_2_path"]: binding for binding in raw_bindings
    }
    rejected_bindings = [
        binding_by_source[item["episode_2_response_path"]]
        for item in recovery["rejection_evidence"]
    ]

    run = dataset.retrieval_runs[0]
    run.metadata["offline_repeated_window_recovery"] = {
        "recovery_episode_number": 3,
        "source_episode_number": 2,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_attempt_count": IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS,
        "retained_page_count": recovery["retained_page_count"],
        "rejected_page_count": len(recovery["rejection_evidence"]),
        "quota_only_rejected_attempt_count": len(recovery["rejection_evidence"]),
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "rejected_response_manifest_hash": _hash_payload(
            {"responses": rejected_bindings}
        ),
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "rejection_evidence": recovery["rejection_evidence"],
        "known_daily_calls_preserved": quota_before["known_calls_after_session"],
        "quota_day_utc": quota_before["quota_day_utc"],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 recovery checkpoint hash disagreement"
        )
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "IEEE episode-2 checkpoint changed during recovery"
        )
    _verify_recovery_raw_bindings(root_path, raw_bindings, "episode_2_path")
    for episode in episodes:
        reference = episode["checkpoint_dataset"]
        _verify_file_reference(
            _safe_output_path(root_path, reference["path"]), reference, root_path
        )

    recovery_episode = {
        "episode_number": 3,
        "episode_id": "IEEEXplore-episode-003",
        "run_id": run.run_id,
        "status": IEEE_REPEATED_WINDOW_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": "OFFLINE_REPEATED_PROVIDER_WINDOW_RECONCILIATION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_attempt_count": IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS,
        "source_raw_responses": raw_bindings,
        "rejected_raw_responses": rejected_bindings,
        "rejection_evidence": recovery["rejection_evidence"],
        "retained_page_count": recovery["retained_page_count"],
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "known_daily_calls_preserved": quota_before["known_calls_after_session"],
        "remaining_daily_calls": (
            IEEE_DAILY_REQUEST_LIMIT - quota_before["known_calls_after_session"]
        ),
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": IEEE_REPEATED_WINDOW_RECOVERY_STATUS,
            "execution_episodes": [*episodes, recovery_episode],
            "active_episode_number": 3,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_attempt_count": IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS,
            "rejected_page_count": len(recovery["rejection_evidence"]),
            "requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_REPEATED_WINDOW_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if source_state.get("ieee_quota") != quota_before:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery changed the daily quota ledger"
        )
    if {
        key: value for key, value in state["sources"].items() if key != "IEEEXplore"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery changed another source component"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def execute_external_source_session(
    *,
    root: str | Path,
    source: str,
    http: HttpClient | None,
    resume: bool,
    ieee_credential: str = "",
    quota_day_utc: str | None = None,
    timestamp: Callable[[], str] = utc_now,
    retry_policy: RetryPolicy | None = None,
    rate_limiter: RateLimiter | None = None,
    retry_sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Execute or resume exactly one authorized external source component."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    if source not in wave.required_sources:
        raise ExternalRetrievalWaveError(f"source is not in the external wave: {source}")
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    state = (
        _load_execution_state(state_path, root_path, wave, preflight)
        if state_path.exists()
        else _initial_execution_state(root_path, wave, preflight, timestamp())
    )
    source_state = state["sources"][source]
    if source_state["status"] == "COMPLETE":
        return state

    started_at = timestamp()
    source_state.update(
        {
            "status": "RUNNING",
            "last_session_started_at_utc": started_at,
            "last_session_completed_at_utc": None,
            "pause_reason": None,
            "failure_reason": None,
        }
    )
    source_state.pop("pause_metadata", None)
    _sync_active_retry_episode(source_state)
    _save_execution_state(state_path, state)
    if source == "ACMDigitalLibrary":
        _execute_acm_import(root_path, wave, state, source_state, timestamp)
        _finalize_execution_state(state, timestamp())
        _save_execution_state(state_path, state)
        return state
    if http is None:
        raise ExternalRetrievalWaveError(f"HTTP client is required for {source}")
    if source == "IEEEXplore" and not ieee_credential:
        source_state["status"] = "BLOCKED_CREDENTIAL"
        source_state["failure_reason"] = f"{IEEE_CREDENTIAL_NAME} is absent"
        _save_execution_state(state_path, state)
        raise ExternalRetrievalWaveError(f"{IEEE_CREDENTIAL_NAME} is required")

    if source == "SemanticScholar":
        gate = _execute_semantic_control_gate(
            root=root_path,
            http=http,
            resume=resume,
            timestamp=timestamp,
            retry_policy=retry_policy or RetryPolicy(),
            rate_limiter=rate_limiter,
            retry_sleep=retry_sleep,
        )
        source_state["semantic_control_gate"] = {
            "status": gate["status"],
            "manifest_path": SEMANTIC_CONTROL_GATE_PATH,
            "manifest_hash": gate["manifest_hash"],
        }
        if gate["status"] != "PASSED":
            source_state["status"] = "BLOCKED_SEMANTIC_CONTROL_GATE"
            source_state["failure_reason"] = (
                f"Semantic Scholar control gate is {gate['status']}"
            )
            source_state["candidate_request_count"] = 0
            source_state["last_session_completed_at_utc"] = timestamp()
            _save_execution_state(state_path, state)
            return state

    checkpoint_relative = source_state.get(
        "active_checkpoint_path", f"{EXECUTION_ROOT}/{source}/checkpoint"
    )
    if not str(checkpoint_relative).startswith(f"{EXECUTION_ROOT}/{source}/"):
        raise ExternalRetrievalWaveError("source checkpoint escaped its component namespace")
    checkpoint_dir = _safe_output_path(root_path, str(checkpoint_relative))
    checkpoint_exists = (checkpoint_dir / "review_dataset.json").exists()
    if checkpoint_exists and not resume:
        raise ExternalRetrievalWaveError(
            f"{source} checkpoint exists; pass --resume to continue"
        )
    if resume and not checkpoint_exists:
        raise ExternalRetrievalWaveError(
            f"{source} has no checkpoint to resume"
        )
    ieee_mutable_total_mode = (
        source == "IEEEXplore"
        and _ieee_total_drift_recovery_active(source_state)
    )
    specs = _source_query_specs(
        wave,
        source,
        ieee_credential=ieee_credential,
        ieee_mutable_total_mode=ieee_mutable_total_mode,
    )
    request_budget = None
    quota = None
    if source == "IEEEXplore":
        quota_day = quota_day_utc or datetime.now(UTC).date().isoformat()
        used = _ieee_calls_on_day(root_path, checkpoint_dir, quota_day)
        request_budget = max(0, IEEE_DAILY_REQUEST_LIMIT - used)
        quota = {
            "quota_day_utc": quota_day,
            "daily_limit": IEEE_DAILY_REQUEST_LIMIT,
            "known_calls_before_session": used,
            "session_request_budget": request_budget,
        }

    before_attempts = _checkpoint_attempt_count(checkpoint_dir)
    dataset = execute_paginated_retrieval_run(
        run_id=source_state.get("active_run_id", f"{WAVE_ID}:{source}"),
        queries=specs,
        http_clients={source: http},
        checkpoint_dir=checkpoint_dir,
        resume=resume,
        timestamp=timestamp,
        software_version=WAVE_VERSION,
        query_plan_version=wave.query_plan_hash,
        retry_policy=retry_policy or RetryPolicy(),
        rate_limiter=rate_limiter,
        retry_sleep=retry_sleep,
        request_budget=request_budget,
        pause_status_codes=(
            frozenset({429})
            if source in {"arXiv", "IEEEXplore"}
            else frozenset()
        ),
        resumable_transport_exhaustion_sources=(
            frozenset({"arXiv"}) if source == "arXiv" else frozenset()
        ),
    )
    after_attempts = len(dataset.retrieval_attempts)
    requests_this_session = after_attempts - before_attempts
    run = dataset.retrieval_runs[0]
    ieee_terminal_reconciliation = None
    if (
        source == "IEEEXplore"
        and ieee_mutable_total_mode
        and run.completion_status is RetrievalCompletionStatus.COMPLETE
    ):
        ieee_terminal_reconciliation = _ieee_terminal_reconciliation(dataset)
        run.metadata["ieee_terminal_reconciliation"] = ieee_terminal_reconciliation
        CheckpointStore(checkpoint_dir).save_dataset(dataset)
    pause_state = run.metadata.get("pause_state")
    if run.completion_status is RetrievalCompletionStatus.COMPLETE:
        status = "COMPLETE"
        failure_reason = None
        pause_reason = None
    elif pause_state == "REQUEST_BUDGET_EXHAUSTED":
        status = "PAUSED_DAILY_QUOTA" if source == "IEEEXplore" else "PAUSED"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "PROVIDER_QUOTA_EXHAUSTED":
        status = "PAUSED_PROVIDER_QUOTA"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "PROVIDER_RATE_LIMIT":
        status = "PAUSED_PROVIDER_RATE_LIMIT"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "TRANSIENT_TRANSPORT_EXHAUSTED":
        status = "PAUSED_TRANSIENT_TRANSPORT"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    else:
        status = "FAILED"
        failure_reason = "; ".join(run.errors)
        pause_reason = None
    source_state.update(
        {
            "status": status,
            "checkpoint_path": checkpoint_dir.relative_to(root_path).as_posix(),
            "checkpoint_dataset": _file_reference(
                checkpoint_dir / "review_dataset.json", root_path
            ),
            "completed_query_count": sum(
                item.completion_status is RetrievalCompletionStatus.COMPLETE
                for item in dataset.source_queries
            ),
            "total_query_count": len(dataset.source_queries),
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": requests_this_session,
            "pause_reason": pause_reason,
            "failure_reason": failure_reason,
            "last_session_completed_at_utc": timestamp(),
        }
    )
    if run.metadata.get("pause_metadata") is not None:
        source_state["pause_metadata"] = dict(run.metadata["pause_metadata"])
    else:
        source_state.pop("pause_metadata", None)
    if quota is not None:
        source_state["ieee_quota"] = {
            **quota,
            "requests_this_session": requests_this_session,
            "known_calls_after_session": quota["known_calls_before_session"]
            + requests_this_session,
        }
    if ieee_terminal_reconciliation is not None:
        source_state["terminal_reconciliation"] = ieee_terminal_reconciliation
    _sync_active_retry_episode(source_state)
    _finalize_execution_state(state, timestamp())
    _save_execution_state(state_path, state)
    return state


def _source_query_specs(
    wave: ProductionRetrievalWave,
    source: str,
    *,
    ieee_credential: str,
    ieee_mutable_total_mode: bool = False,
) -> list[RetrievalQuerySpec]:
    specs = []
    for family in wave.query_families:
        if family.source_database != source:
            continue
        parameters = family.native_parameters
        limit = int(
            parameters.get("page_size")
            or parameters.get("pageSize")
            or parameters.get("limit")
            or parameters.get("max_results")
            or parameters.get("max_records")
        )
        metadata = {
            "production_query_id": family.query_family_id,
            "frozen_request_specification_hash": parameters[
                "frozen_request_specification_hash"
            ],
            "content_policy": family.content_policy,
        }
        fields = []
        endpoint = None
        if source == "EuropePMC":
            metadata["result_type"] = parameters["resultType"]
            endpoint = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        elif source == "SemanticScholar":
            fields = str(parameters["fields"]).split(",")
            metadata["sort"] = parameters["sort"]
            endpoint = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
        elif source == "arXiv":
            endpoint = "http://export.arxiv.org/api/query"
        elif source == "IEEEXplore":
            metadata.update(
                {
                    "query_parameter": parameters["query_parameter"],
                    "sort_field": parameters["sort_field"],
                    "sort_order": parameters["sort_order"],
                    "mutable_provider_totals": ieee_mutable_total_mode,
                }
            )
            endpoint = "https://ieeexploreapi.ieee.org/api/v1/search/articles"
        specs.append(
            RetrievalQuerySpec(
                source_database=source,
                query_text=family.query_text,
                query_version=family.query_version,
                limit=limit,
                endpoint=endpoint,
                fields=fields,
                metadata=metadata,
                pagination_mode="bulk" if source == "SemanticScholar" else None,
                credentials={"api_key": ieee_credential}
                if source == "IEEEXplore"
                else {},
            )
        )
    if len(specs) != 5:
        raise ExternalRetrievalWaveError(f"{source} must have exactly five queries")
    return specs


def _execute_acm_import(
    root: Path,
    wave: ProductionRetrievalWave,
    state: dict[str, Any],
    source_state: dict[str, Any],
    timestamp: Callable[[], str],
) -> None:
    query_texts = {
        item.query_family_id: item.query_text
        for item in wave.query_families
        if item.source_database == "ACMDigitalLibrary"
    }
    datasets = import_acm_selected_reconciliation(
        root / ACM_RECONCILIATION_PATH,
        root=root,
        query_text_by_parent=query_texts,
        query_version=wave.query_plan_version,
        run_id_prefix=f"{WAVE_ID}:ACMDigitalLibrary",
        software_version=WAVE_VERSION,
    )
    family_files = []
    occurrence_count = 0
    malformed_count = 0
    for family_id, dataset in datasets.items():
        suffix = family_id.removeprefix("STAR-").split("-", 1)[0]
        path = _safe_output_path(
            root, f"{EXECUTION_ROOT}/ACMDigitalLibrary/{suffix}_review_dataset.json"
        )
        save_review_dataset(path, dataset)
        occurrence_count += len(dataset.occurrences)
        malformed_count += sum(
            bool(item.metadata.get("malformed_but_identified"))
            for item in dataset.occurrences
        )
        family_files.append(
            {
                "family_id": family_id,
                "dataset": _file_reference(path, root),
                "occurrence_count": len(dataset.occurrences),
                "canonical_identity_count": len(dataset.canonical_records),
            }
        )
    if occurrence_count != 11664 or malformed_count != 3:
        raise ExternalRetrievalWaveError("ACM production import accounting changed")
    source_state.update(
        {
            "status": "COMPLETE",
            "completed_query_count": 5,
            "total_query_count": 5,
            "occurrence_count": occurrence_count,
            "malformed_but_identified_count": malformed_count,
            "family_datasets": family_files,
            "network_request_count": 0,
            "last_session_completed_at_utc": timestamp(),
        }
    )
    state["acm_live_search_performed"] = False


def _execute_semantic_control_gate(
    *,
    root: Path,
    http: HttpClient,
    resume: bool,
    timestamp: Callable[[], str],
    retry_policy: RetryPolicy,
    rate_limiter: RateLimiter | None,
    retry_sleep: Callable[[float], None],
) -> dict[str, Any]:
    path = _safe_output_path(root, SEMANTIC_CONTROL_GATE_PATH)
    control_path = root / SEMANTIC_CONTROL_PATH
    controls = load_semantic_control_set(control_path)
    if path.exists():
        manifest = _load_json(path)
        _validate_embedded_hash(manifest, "manifest_hash")
        if manifest["control_set"]["raw_sha256"] != _sha256(
            control_path.read_bytes()
        ):
            raise ExternalRetrievalWaveError("Semantic control binding changed")
        if manifest["status"] in {"PASSED", "FAILED"}:
            return manifest
        if not resume:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar control checkpoint exists; pass --resume"
            )
    else:
        if resume:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar control gate has no checkpoint to resume"
            )
        manifest = {
            "schema_version": "1.0.0",
            "gate": "bulk_boolean_semantics",
            "status": "RUNNING",
            "control_set": {
                **_file_reference(control_path, root),
                "canonical_hash": controls.control_set_hash(),
            },
            "controls": [],
            "assertions": [assertion.to_dict() for assertion in controls.assertions],
            "candidate_queries_executed": False,
            "manifest_hash": None,
        }
    store = CheckpointStore(path.parent)
    limiter = rate_limiter or RateLimiter()
    observations = {item["probe_id"]: item for item in manifest["controls"]}
    for probe in controls.probes:
        observation = observations.get(probe.probe_id)
        if observation and observation["status"] == "SUCCEEDED":
            continue
        if observation is None:
            request = PageRequest(
                "GET",
                "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
                params={
                    "query": probe.expression,
                    "limit": 1,
                    "fields": "paperId",
                    "sort": "paperId:asc",
                },
                state={"probe_id": probe.probe_id},
            )
            observation = {
                "probe_id": probe.probe_id,
                "expression": probe.expression,
                "expression_sha256": _sha256(probe.expression.encode("utf-8")),
                "request": {
                    "method": request.method,
                    "url": request.url,
                    "params": request.sanitized_params(),
                },
                "request_hash": request.request_hash(),
                "status": "RUNNING",
                "reported_count": None,
                "attempts": [],
            }
            manifest["controls"].append(observation)
            observations[probe.probe_id] = observation
            _save_hashed_json(path, manifest, "manifest_hash")
        request = PageRequest(
            observation["request"]["method"],
            observation["request"]["url"],
            params=dict(observation["request"]["params"]),
            state={"probe_id": probe.probe_id},
        )
        while len(observation["attempts"]) < retry_policy.max_attempts:
            attempt_number = len(observation["attempts"]) + 1
            attempt = {
                "attempt_number": attempt_number,
                "started_at_utc": timestamp(),
                "status": "STARTED",
                "request_hash": request.request_hash(),
                "response": None,
                "error": None,
            }
            observation["attempts"].append(attempt)
            _save_hashed_json(path, manifest, "manifest_hash")
            try:
                delay = limiter.wait("SemanticScholar")
                response = http.get(
                    request.url,
                    params=request.params,
                    timeout=request.timeout,
                )
                attempt_id = (
                    f"semantic-control-{probe.probe_id}-attempt-{attempt_number:03d}"
                )
                response_path, response_hash = store.save_response(attempt_id, response)
                attempt["response"] = {
                    "status": response.status_code,
                    "path": response_path,
                    "sha256": response_hash,
                    "byte_size": (path.parent / response_path).stat().st_size,
                }
                attempt["rate_limit_delay_seconds"] = delay
                if not 200 <= response.status_code < 300:
                    raise ValueError(f"HTTP {response.status_code}")
                payload = response.json()
                if not isinstance(payload, dict) or "total" not in payload:
                    raise ValueError("Semantic control response omitted total")
                count = int(payload["total"])
                if count < 0 or payload.get("error") or payload.get("errors"):
                    raise ValueError("Semantic control response is invalid")
                observation["reported_count"] = count
                observation["status"] = "SUCCEEDED"
                attempt["status"] = "SUCCEEDED"
                attempt["completed_at_utc"] = timestamp()
                _save_hashed_json(path, manifest, "manifest_hash")
                break
            except Exception as exc:  # noqa: BLE001 - persist every control failure
                attempt["status"] = "FAILED"
                attempt["completed_at_utc"] = timestamp()
                attempt["error"] = f"{type(exc).__name__}: {exc}"
                if attempt_number < retry_policy.max_attempts:
                    delay = retry_policy.delay(attempt_number)
                    attempt["retry_delay_seconds"] = delay
                    _save_hashed_json(path, manifest, "manifest_hash")
                    retry_sleep(delay)
                else:
                    observation["status"] = "UNRESOLVED"
                    _save_hashed_json(path, manifest, "manifest_hash")

    unresolved = [
        item["probe_id"]
        for item in manifest["controls"]
        if item["status"] != "SUCCEEDED"
    ]
    assertion_results = []
    failed = []
    counts = {item["probe_id"]: item["reported_count"] for item in manifest["controls"]}
    if not unresolved:
        for assertion in manifest["assertions"]:
            left = int(counts[assertion["left_probe_id"]])
            right = int(counts[assertion["right_probe_id"]])
            relation = assertion["relation"]
            passed = {
                "less_than_or_equal": left <= right,
                "greater_than_or_equal": left >= right,
                "equal": left == right,
            }[relation]
            result = {
                **assertion,
                "left_count": left,
                "right_count": right,
                "passed": passed,
            }
            assertion_results.append(result)
            if not passed:
                failed.append(assertion["assertion_id"])
    manifest["assertion_results"] = assertion_results
    manifest["unresolved_control_ids"] = unresolved
    manifest["failed_assertion_ids"] = failed
    manifest["status"] = "UNRESOLVED" if unresolved else "FAILED" if failed else "PASSED"
    manifest["candidate_queries_executed"] = False
    _save_hashed_json(path, manifest, "manifest_hash")
    return manifest


def _initial_execution_state(
    root: Path,
    wave: ProductionRetrievalWave,
    preflight: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "execution_id": WAVE_ID,
        "status": "RUNNING",
        "created_at_utc": created_at,
        "updated_at_utc": created_at,
        "wave_manifest_hash": wave.manifest_hash(),
        "planned_wave_raw_sha256": _sha256(
            _safe_output_path(root, WAVE_PATH).read_bytes()
        ),
        "preflight_raw_sha256": _sha256(
            _safe_output_path(root, PREFLIGHT_PATH).read_bytes()
        ),
        "sources": {
            source: {
                "status": "NOT_STARTED",
                "completed_query_count": 0,
                "total_query_count": 5,
            }
            for source in wave.required_sources
        },
        "external_retrieval_completed_at_utc": None,
        "external_retrieval_cutoff_date": None,
        "acm_live_search_performed": False,
        "prior_survey_seed_imported": False,
        "identification_set_closed": False,
        "final_global_deduplication_executed": False,
        "prisma_generated": False,
        "screening_executed": False,
        "corpus_modified": False,
        "state_hash": None,
    }


def _load_execution_state(
    path: Path,
    root: Path,
    wave: ProductionRetrievalWave,
    preflight: dict[str, Any],
) -> dict[str, Any]:
    state = _load_json(path)
    _validate_embedded_hash(state, "state_hash")
    if (
        state.get("wave_manifest_hash") != wave.manifest_hash()
        or state.get("planned_wave_raw_sha256")
        != _sha256(_safe_output_path(root, WAVE_PATH).read_bytes())
        or state.get("preflight_raw_sha256")
        != _sha256(_safe_output_path(root, PREFLIGHT_PATH).read_bytes())
    ):
        raise ExternalRetrievalWaveError("execution checkpoint frozen-wave hash mismatch")
    return state


def _reparse_failed_ieee_total_drift_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery requires exactly one run"
        )
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "IEEE checkpoint is not an eligible total-drift failure"
        )
    if len(dataset.retrieval_attempts) != IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE failed attempt count changed")
    expected_occurrences = sum(item[3] for item in IEEE_TOTAL_DRIFT_EXPECTED)
    if len(dataset.occurrences) != expected_occurrences:
        raise ExternalRetrievalWaveError("IEEE failed occurrence count changed")

    specs = _source_query_specs(
        wave,
        "IEEEXplore",
        ieee_credential="offline-recovery-redacted",
        ieee_mutable_total_mode=True,
    )
    failed_query_plan_hash = run.query_plan_hash
    recovery_query_plan_hash = _query_plan_hash(specs)
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("IEEE query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["IEEEXplore"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    continuation_plan = []
    total_histories: dict[str, list[int]] = {}

    for query, spec, expected in zip(
        dataset.source_queries, specs, IEEE_TOTAL_DRIFT_EXPECTED, strict=True
    ):
        initial_total, final_total, page_count, occurrence_count, next_start = expected
        expected_error = (
            f"{IEEE_TOTAL_DRIFT_ERROR_PREFIX}[{final_total}, {initial_total}]"
        )
        if (
            query.source_database != "IEEEXplore"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.status is not ProcessingStatus.FAILED
            or query.errors != [expected_error]
        ):
            raise ExternalRetrievalWaveError(
                "IEEE recovery refused changed query/failure provenance"
            )
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if (
            len(pages) != page_count
            or [page.ordinal for page in pages] != list(range(page_count))
        ):
            raise ExternalRetrievalWaveError(
                "IEEE persisted page lineage is incomplete or unordered"
            )

        expected_start = 1
        query_identifiers: set[str] = set()
        observed_totals = []
        for page in pages:
            if (
                page.strategy != adapter.strategy
                or page.adapter_version != adapter.version
                or page.request_state != {"start_record": expected_start}
                or page.status is not RetrievalCompletionStatus.COMPLETE
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE persisted start_record/page lineage changed"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = sorted(
                (
                    attempt
                    for attempt in dataset.retrieval_attempts
                    if attempt.page_id == page.page_id
                ),
                key=lambda item: item.attempt_number,
            )
            if (
                len(attempts) != 1
                or page.attempt_ids != [attempts[0].attempt_id]
                or attempts[0].attempt_number != 1
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE recovery requires one zero-retry attempt per page"
                )
            attempt = attempts[0]
            if (
                attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                or attempt.response_status != 200
                or attempt.error is not None
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE recovery requires exact successful request lineage"
                )
            if attempt.raw_response_path in seen_response_paths:
                raise ExternalRetrievalWaveError("IEEE raw response path is reused")
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE raw response hash/read failure: {exc}"
                ) from exc
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_references.append(
                (attempt.raw_response_path, attempt.raw_response_hash)
            )
            try:
                parsed = adapter.parse_response(spec, page.request_state, response)
            except Exception as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE offline total-drift parse failed: {type(exc).__name__}: {exc}"
                ) from exc
            if (
                parsed.incomplete_reason
                or parsed.raw_item_count != len(parsed.records)
                or parsed.raw_item_count < 1
                or parsed.raw_item_count > 200
                or parsed.total_is_exact
                or parsed.terminal
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE persisted page is not a valid nonterminal recovery page"
                )
            page_occurrences = sorted(
                (
                    occurrence
                    for occurrence in dataset.occurrences
                    if occurrence.retrieval_page_id == page.page_id
                ),
                key=lambda item: item.source_rank or 0,
            )
            if (
                len(page_occurrences) != parsed.raw_item_count
                or [item.source_identifier for item in page_occurrences]
                != parsed.native_identifiers
                or page.returned_item_count != parsed.raw_item_count
                or page.native_identifiers != parsed.native_identifiers
                or len(set(parsed.native_identifiers)) != len(parsed.native_identifiers)
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE persisted record/page accounting changed"
                )
            overlap = query_identifiers.intersection(parsed.native_identifiers)
            if overlap:
                raise ExternalRetrievalWaveError(
                    "IEEE stable identity overlaps across persisted pages"
                )
            query_identifiers.update(parsed.native_identifiers)
            observed_totals.append(int(parsed.source_reported_total))
            persisted_next_start = int(page.next_state["start_record"])
            if persisted_next_start != expected_start + parsed.raw_item_count:
                raise ExternalRetrievalWaveError(
                    "IEEE response created a pagination gap or overlap"
                )
            expected_start = persisted_next_start

            prior_error = page.metadata.pop("completion_error", None)
            if prior_error:
                page.metadata["prior_completion_error"] = prior_error
            page.total_is_exact = False
            page.metadata.update(parsed.metadata)
            page.metadata["offline_provider_total_drift_recovery"] = True
            page.metadata["raw_response_reused_without_network"] = True

        if (
            expected_start != next_start
            or len(query_identifiers) != occurrence_count
            or query.result_count != occurrence_count
            or observed_totals[0] != initial_total
            or observed_totals[-1] != final_total
            or any(value != initial_total for value in observed_totals[:-1])
        ):
            raise ExternalRetrievalWaveError(
                "IEEE observed total/count/continuation evidence changed"
            )
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.RUNNING
        query.completion_proof = None
        query.source_reported_total = final_total
        query.total_is_exact = False
        query.errors = []
        query.retrieval_ended_at = recovered_at
        query.metadata["provider_total_observations"] = observed_totals
        query.metadata["provider_total_observation_range"] = [
            min(observed_totals),
            max(observed_totals),
        ]
        query.metadata["next_start_record"] = next_start
        query.metadata["offline_provider_total_drift_recovery"] = True
        query.metadata["mutable_provider_totals"] = True
        total_histories[spec.metadata["production_query_id"]] = observed_totals
        continuation_plan.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "next_start_record": next_start,
                "persisted_page_count": page_count,
                "persisted_occurrence_count": occurrence_count,
                "observed_provider_totals": observed_totals,
            }
        )

    if len(raw_response_references) != IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE raw-response accounting changed")
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.query_plan_hash = recovery_query_plan_hash
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        "offline IEEE provider-total-drift recovery complete; live pagination resume pending"
    ]
    run.metadata["provider_total_semantics"] = "MUTABLE_PAGINATION_OBSERVATION"
    run.metadata["network_requests_during_recovery"] = 0
    run.metadata["source_raw_response_count"] = len(raw_response_references)
    run.metadata["failed_episode_query_plan_hash"] = failed_query_plan_hash
    run.metadata["recovery_query_plan_hash"] = recovery_query_plan_hash
    dataset.validate()
    return {
        "raw_response_references": raw_response_references,
        "continuation_plan": continuation_plan,
        "provider_total_histories": total_histories,
    }


def _reparse_failed_ieee_repeated_window_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery requires exactly one run"
        )
    run = dataset.retrieval_runs[0]
    specs = _source_query_specs(
        wave,
        "IEEEXplore",
        ieee_credential="offline-recovery-redacted",
        ieee_mutable_total_mode=True,
    )
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_hash != _query_plan_hash(specs)
        or len(dataset.source_queries) != len(specs)
    ):
        raise ExternalRetrievalWaveError(
            "IEEE checkpoint is not an eligible repeated-window failure"
        )
    if len(dataset.retrieval_attempts) != IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE episode-2 attempt count changed")
    if len(IEEE_REPEATED_WINDOW_EXPECTED) != len(specs) or len(
        IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES
    ) != len(specs):
        raise ExternalRetrievalWaveError("IEEE repeated-window signature is incomplete")

    adapter = PAGINATED_SOURCE_ADAPTERS["IEEEXplore"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    rejected_page_ids: set[str] = set()
    rejected_attempt_ids: set[str] = set()
    rejected_occurrence_ids: set[str] = set()
    rejection_evidence = []
    continuation_plan = []
    total_histories: dict[str, list[int]] = {}
    retained_page_count = 0

    for query, spec, expected, expected_history in zip(
        dataset.source_queries,
        specs,
        IEEE_REPEATED_WINDOW_EXPECTED,
        IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES,
        strict=True,
    ):
        page_count, occurrence_count, rejected_start, rejected_count, next_start = expected
        if (
            query.source_database != "IEEEXplore"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.status is not ProcessingStatus.FAILED
            or query.result_count != occurrence_count
        ):
            raise ExternalRetrievalWaveError(
                "IEEE repeated-window recovery refused changed query provenance"
            )
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if (
            len(pages) != page_count
            or len(pages) < 2
            or [page.ordinal for page in pages] != list(range(page_count))
        ):
            raise ExternalRetrievalWaveError(
                "IEEE episode-2 page lineage is incomplete or unordered"
            )

        parsed_pages = []
        observed_totals = []
        query_identifiers: set[str] = set()
        for page_index, page in enumerate(pages):
            if (
                page.strategy != adapter.strategy
                or page.adapter_version != adapter.version
                or page.status is not RetrievalCompletionStatus.COMPLETE
                or (
                    page_index
                    and page.request_state != pages[page_index - 1].next_state
                )
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE episode-2 pagination ordering or request state changed"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = sorted(
                (
                    attempt
                    for attempt in dataset.retrieval_attempts
                    if attempt.page_id == page.page_id
                ),
                key=lambda item: item.attempt_number,
            )
            if (
                len(attempts) != 1
                or page.attempt_ids != [attempts[0].attempt_id]
                or attempts[0].attempt_number != 1
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE repeated-window recovery requires one attempt per page"
                )
            attempt = attempts[0]
            if (
                attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                or attempt.response_status != 200
                or attempt.error is not None
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE repeated-window recovery requires exact request identity"
                )
            if attempt.raw_response_path in seen_response_paths:
                raise ExternalRetrievalWaveError("IEEE raw response path is reused")
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE repeated-window raw response hash/read failure: {exc}"
                ) from exc
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_references.append(
                (attempt.raw_response_path, attempt.raw_response_hash)
            )
            try:
                parsed = adapter.parse_response(spec, page.request_state, response)
            except Exception as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE repeated-window offline parse failed: {type(exc).__name__}: {exc}"
                ) from exc
            page_occurrences = sorted(
                (
                    occurrence
                    for occurrence in dataset.occurrences
                    if occurrence.retrieval_page_id == page.page_id
                ),
                key=lambda item: item.source_rank or 0,
            )
            if (
                parsed.incomplete_reason
                or parsed.raw_item_count != len(parsed.records)
                or parsed.raw_item_count < 1
                or parsed.raw_item_count > spec.limit
                or parsed.total_is_exact
                or len(page_occurrences) != parsed.raw_item_count
                or [item.source_identifier for item in page_occurrences]
                != parsed.native_identifiers
                or page.returned_item_count != parsed.raw_item_count
                or page.native_identifiers != parsed.native_identifiers
                or len(set(parsed.native_identifiers)) != len(parsed.native_identifiers)
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE repeated-window page or occurrence accounting changed"
                )
            parsed_pages.append((page, attempt, parsed, page_occurrences))
            observed_totals.append(int(parsed.source_reported_total))
            if page_index:
                prior_page, _, prior_parsed, _ = parsed_pages[-2]
                expected_legacy_start = (
                    int(prior_page.request_state["start_record"])
                    + prior_parsed.raw_item_count
                )
                if int(page.request_state["start_record"]) != expected_legacy_start:
                    raise ExternalRetrievalWaveError(
                        "IEEE episode-2 lineage contains an unexplained pagination gap"
                    )

        previous_page, _, previous_parsed, _ = parsed_pages[-2]
        rejected_page, rejected_attempt, rejected_parsed, rejected_occurrences = (
            parsed_pages[-1]
        )
        previous_start = int(previous_page.request_state["start_record"])
        actual_rejected_start = int(rejected_page.request_state["start_record"])
        calculated_boundary = previous_start + int(spec.limit)
        expected_error = (
            "source repeated native identifiers across pages: "
            f"{sorted(rejected_parsed.native_identifiers)!r}"
        )
        if (
            query.errors != [expected_error]
            or rejected_page.metadata.get("completion_error") != expected_error
            or previous_parsed.raw_item_count != rejected_count
            or rejected_parsed.raw_item_count != rejected_count
            or actual_rejected_start != rejected_start
            or actual_rejected_start
            != previous_start + previous_parsed.raw_item_count
            or calculated_boundary != next_start
            or actual_rejected_start >= calculated_boundary
            or previous_start // int(spec.limit)
            != actual_rejected_start // int(spec.limit)
            or previous_parsed.native_identifiers
            != rejected_parsed.native_identifiers
            or previous_parsed.source_reported_total
            != rejected_parsed.source_reported_total
            or [record.original_metadata for record in previous_parsed.records]
            != [record.original_metadata for record in rejected_parsed.records]
            or previous_parsed.terminal
            or rejected_parsed.terminal
        ):
            raise ExternalRetrievalWaveError(
                "IEEE malformed repeated-window rejection signature"
            )
        if observed_totals != list(expected_history):
            raise ExternalRetrievalWaveError(
                "IEEE complete provider-total history changed"
            )

        for page, _, parsed, _ in parsed_pages[:-1]:
            overlap = query_identifiers.intersection(parsed.native_identifiers)
            if overlap:
                raise ExternalRetrievalWaveError(
                    "IEEE retained pages contain native-identifier overlap"
                )
            query_identifiers.update(parsed.native_identifiers)
        if len(query_identifiers) != occurrence_count - rejected_count:
            raise ExternalRetrievalWaveError(
                "IEEE retained occurrence accounting changed"
            )

        rejected_page_ids.add(rejected_page.page_id)
        rejected_attempt_ids.add(rejected_attempt.attempt_id)
        rejected_occurrence_ids.update(
            occurrence.occurrence_id for occurrence in rejected_occurrences
        )
        previous_page.next_state = {"start_record": calculated_boundary}
        previous_page.metadata["recovered_next_start_record"] = calculated_boundary
        rejection_evidence.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "preceding_page_id": previous_page.page_id,
                "rejected_page_id": rejected_page.page_id,
                "rejected_attempt_id": rejected_attempt.attempt_id,
                "rejected_request_hash": rejected_attempt.request_hash,
                "episode_2_response_path": (
                    checkpoint_dir / rejected_attempt.raw_response_path
                ).relative_to(root).as_posix(),
                "raw_response_hash": rejected_attempt.raw_response_hash,
                "preceding_start_record": previous_start,
                "rejected_start_record": actual_rejected_start,
                "returned_item_count": rejected_count,
                "corrected_next_start_record": calculated_boundary,
                "native_identifier_manifest_hash": _hash_payload(
                    {"native_identifiers": rejected_parsed.native_identifiers}
                ),
                "source_reported_total": rejected_parsed.source_reported_total,
                "terminal_error": expected_error,
                "classification": "REJECTED_WHOLE_PROVIDER_WINDOW_REPETITION",
            }
        )
        retained_page_count += len(pages) - 1
        query.page_ids = [page.page_id for page in pages[:-1]]
        query.result_count = occurrence_count - rejected_count
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.RUNNING
        query.completion_proof = None
        query.errors = []
        query.retrieval_ended_at = recovered_at
        query.total_is_exact = False
        query.metadata["provider_total_observations"] = observed_totals
        query.metadata["provider_total_observation_range"] = [
            min(observed_totals),
            max(observed_totals),
        ]
        query.metadata["recovery_retained_page_count"] = len(pages) - 1
        query.metadata["next_start_record"] = calculated_boundary
        query.metadata["offline_repeated_window_recovery"] = True
        query.metadata["mutable_provider_totals"] = True
        total_histories[spec.metadata["production_query_id"]] = observed_totals
        continuation_plan.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "next_start_record": calculated_boundary,
                "retained_page_count": len(pages) - 1,
                "retained_occurrence_count": occurrence_count - rejected_count,
                "rejected_page_count": 1,
                "observed_provider_totals": observed_totals,
            }
        )

    if (
        len(raw_response_references) != IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS
        or len(rejection_evidence) != IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS
        or retained_page_count != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
    ):
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery aggregate accounting changed"
        )

    dataset.retrieval_pages = [
        page for page in dataset.retrieval_pages if page.page_id not in rejected_page_ids
    ]
    dataset.retrieval_attempts = [
        attempt
        for attempt in dataset.retrieval_attempts
        if attempt.attempt_id not in rejected_attempt_ids
    ]
    dataset.occurrences = [
        occurrence
        for occurrence in dataset.occurrences
        if occurrence.occurrence_id not in rejected_occurrence_ids
    ]
    dataset.duplicate_decisions = [
        decision
        for decision in dataset.duplicate_decisions
        if decision.occurrence_id not in rejected_occurrence_ids
    ]
    for canonical in dataset.canonical_records:
        canonical.occurrence_ids = [
            occurrence_id
            for occurrence_id in canonical.occurrence_ids
            if occurrence_id not in rejected_occurrence_ids
        ]
    dataset.canonical_records = [
        canonical for canonical in dataset.canonical_records if canonical.occurrence_ids
    ]
    if any(
        canonical.survivor_occurrence_id in rejected_occurrence_ids
        for canonical in dataset.canonical_records
    ):
        raise ExternalRetrievalWaveError(
            "IEEE rejected occurrence was a canonical survivor"
        )

    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        "offline IEEE repeated-window recovery complete; live pagination resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata["provider_total_semantics"] = "MUTABLE_PAGINATION_OBSERVATION"
    run.metadata["network_requests_during_recovery"] = 0
    dataset.validate()
    return {
        "raw_response_references": raw_response_references,
        "continuation_plan": continuation_plan,
        "provider_total_histories": total_histories,
        "rejection_evidence": rejection_evidence,
        "retained_page_count": retained_page_count,
    }


def _copy_recovery_raw_responses(
    *,
    root: Path,
    source_checkpoint_dir: Path,
    recovery_checkpoint_dir: Path,
    raw_response_references: list[tuple[str, str]],
    source_path_key: str,
    error_prefix: str,
) -> list[dict[str, Any]]:
    bindings = []
    for relative_path, expected_hash in raw_response_references:
        response_path = Path(relative_path)
        if (
            response_path.is_absolute()
            or ".." in response_path.parts
            or not response_path.parts
            or response_path.parts[0] != "responses"
        ):
            raise ExternalRetrievalWaveError(
                f"{error_prefix} raw response path is unsafe"
            )
        source = source_checkpoint_dir / response_path
        raw = source.read_bytes()
        if _sha256(raw) != expected_hash:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} raw response hash mismatch: {relative_path}"
            )
        destination = recovery_checkpoint_dir / response_path
        atomic_write(destination, raw)
        if destination.read_bytes() != raw:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} recovery response copy mismatch: {relative_path}"
            )
        bindings.append(
            {
                source_path_key: source.relative_to(root).as_posix(),
                "recovery_copy_path": destination.relative_to(root).as_posix(),
                "byte_size": len(raw),
                "raw_sha256": expected_hash,
            }
        )
    return bindings


def _verify_recovery_raw_bindings(
    root: Path, bindings: list[dict[str, Any]], source_path_key: str
) -> None:
    for binding in bindings:
        for key in (source_path_key, "recovery_copy_path"):
            path = _safe_output_path(root, binding[key])
            raw = path.read_bytes()
            if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
                raise ExternalRetrievalWaveError(
                    "recovery raw-response binding changed"
                )


def _validate_authorized_ieee_total_drift_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError("IEEE recovery episode lineage changed")
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or not failed.get("immutable")
        or recovered.get("episode_number") != 2
        or recovered.get("status") != IEEE_TOTAL_DRIFT_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError(
            "authorized IEEE total-drift recovery lineage changed"
        )
    source_checkpoint = _safe_output_path(
        root, recovered["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(
        source_checkpoint, recovered["source_episode_checkpoint"], root
    )
    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    bindings = recovered.get("source_raw_responses", [])
    if len(bindings) != IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE recovery raw-response manifest changed")
    _verify_recovery_raw_bindings(root, bindings, "failed_episode_path")


def _validate_authorized_ieee_repeated_window_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 3:
        raise ExternalRetrievalWaveError("IEEE episode-3 recovery lineage changed")
    first, second, recovered = episodes
    if (
        first.get("episode_number") != 1
        or second.get("episode_number") != 2
        or not first.get("immutable")
        or not second.get("immutable")
        or second.get("status") != "FAILED"
        or recovered.get("episode_number") != 3
        or recovered.get("status") != IEEE_REPEATED_WINDOW_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 2
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or recovered.get("retained_page_count")
        != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
        or len(recovered.get("rejection_evidence", []))
        != IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS
        or recovered.get("known_daily_calls_preserved")
        != IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS
        or source_state.get("ieee_quota", {}).get("known_calls_after_session")
        != IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS
        or source_state.get("active_episode_number") != 3
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError(
            "authorized IEEE episode-3 recovery lineage changed"
        )
    for episode in (first, second):
        checkpoint = _safe_output_path(
            root, episode["checkpoint_dataset"]["path"]
        )
        _verify_file_reference(checkpoint, episode["checkpoint_dataset"], root)
    source_checkpoint = _safe_output_path(
        root, recovered["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(
        source_checkpoint, recovered["source_episode_checkpoint"], root
    )
    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    bindings = recovered.get("source_raw_responses", [])
    if (
        len(bindings) != IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS
        or len(recovered.get("rejected_raw_responses", []))
        != IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS
    ):
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 raw-response manifest changed"
        )
    _verify_recovery_raw_bindings(root, bindings, "episode_2_path")
    dataset = load_review_dataset(recovery_checkpoint)
    if (
        len(dataset.retrieval_pages) != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
        or len(dataset.retrieval_attempts)
        != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
        or [
            query.metadata.get("next_start_record")
            for query in dataset.source_queries
        ]
        != [item[4] for item in IEEE_REPEATED_WINDOW_EXPECTED]
    ):
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 continuation checkpoint changed"
        )


def _ieee_total_drift_recovery_active(source_state: Mapping[str, Any]) -> bool:
    active_number = source_state.get("active_episode_number")
    active = next(
        (
            item
            for item in source_state.get("execution_episodes", [])
            if item.get("episode_number") == active_number
        ),
        None,
    )
    return bool(
        active
        and (
            (
                active.get("episode_number") == 2
                and active.get("recovery_of_episode_number") == 1
                and active.get("authorization_reason")
                == "OFFLINE_MUTABLE_PROVIDER_TOTAL_RECONCILIATION"
            )
            or (
                active.get("episode_number") == 3
                and active.get("recovery_of_episode_number") == 2
                and active.get("authorization_reason")
                == "OFFLINE_REPEATED_PROVIDER_WINDOW_RECONCILIATION"
            )
        )
    )


def _ieee_terminal_reconciliation(dataset: Any) -> dict[str, Any]:
    families = []
    for query in dataset.source_queries:
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        identifiers = [
            identifier for page in pages for identifier in page.native_identifiers
        ]
        duplicate_count = len(identifiers) - len(set(identifiers))
        if (
            query.completion_status is not RetrievalCompletionStatus.COMPLETE
            or not pages
            or not pages[-1].terminal
            or duplicate_count
        ):
            raise ExternalRetrievalWaveError(
                "IEEE terminal reconciliation requires complete overlap-free pagination"
            )
        current_observations = [
            int(page.metadata["provider_total_observation"]) for page in pages
        ]
        retained_page_count = query.metadata.get("recovery_retained_page_count")
        if retained_page_count is None:
            observations = current_observations
        else:
            preserved_observations = list(
                query.metadata.get("provider_total_observations", [])
            )
            if (
                not isinstance(retained_page_count, int)
                or retained_page_count < 1
                or retained_page_count > len(current_observations)
                or len(preserved_observations) < retained_page_count
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE recovered provider-total history is malformed"
                )
            observations = [
                *preserved_observations,
                *current_observations[retained_page_count:],
            ]
        query.metadata["provider_total_observations"] = observations
        query.metadata["provider_total_observation_range"] = [
            min(observations),
            max(observations),
        ]
        query.metadata.pop("next_start_record", None)
        final_total = observations[-1]
        unique_count = len(set(identifiers))
        difference = unique_count - final_total
        drift_span = max(observations) - min(observations)
        families.append(
            {
                "production_query_id": query.metadata["production_query_id"],
                "unique_retrieved_identities": unique_count,
                "page_count": len(pages),
                "observed_provider_totals": observations,
                "provider_total_range": [min(observations), max(observations)],
                "duplicate_identities_across_pages": duplicate_count,
                "final_provider_total": final_total,
                "retrieved_minus_final_provider_total": difference,
                "discrepancy_explainable_by_observed_index_drift": (
                    abs(difference) <= drift_span
                ),
                "snapshot_equivalent_completeness_claimed": False,
                "completion_basis": (
                    "OFFSET_PAGINATION_EXHAUSTED_AGAINST_CURRENT_PAGE_TOTAL"
                ),
                "limitation": (
                    "mutable provider totals prevent a snapshot-equivalent completeness claim"
                ),
            }
        )
    return {
        "status": "TERMINAL_OFFSET_EXHAUSTION_RECONCILED_WITH_INDEX_DRIFT_LIMITATION",
        "families": families,
        "snapshot_equivalent_completeness_claimed": False,
    }


def _reparse_failed_europe_pmc_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "Europe PMC terminal recovery requires exactly one run"
        )
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "Europe PMC checkpoint is not an eligible terminal-sentinel failure"
        )
    if len(dataset.retrieval_attempts) != EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError(
            "Europe PMC failed attempt count changed"
        )
    if len(dataset.occurrences) != sum(EUROPE_PMC_RECOVERY_EXPECTED_COUNTS):
        raise ExternalRetrievalWaveError(
            "Europe PMC failed occurrence count changed"
        )

    specs = _source_query_specs(wave, "EuropePMC", ieee_credential="")
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("Europe PMC query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["EuropePMC"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    recovered_counts: dict[str, int] = {}
    failed_query_count = 0

    for query_index, (query, spec) in enumerate(
        zip(dataset.source_queries, specs, strict=True)
    ):
        expected_count = EUROPE_PMC_RECOVERY_EXPECTED_COUNTS[query_index]
        if (
            query.source_database != "EuropePMC"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC frozen query/request binding changed"
            )
        was_failed = query.completion_status is RetrievalCompletionStatus.FAILED
        if was_failed:
            failed_query_count += 1
            if query.errors != [EUROPE_PMC_TERMINAL_ERROR]:
                raise ExternalRetrievalWaveError(
                    "Europe PMC recovery refused a non-terminal-sentinel failure"
                )
        elif (
            query.completion_status is not RetrievalCompletionStatus.COMPLETE
            or query.status is not ProcessingStatus.OK
            or query.errors
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC recovery found an unsupported query state"
            )

        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if not pages or [page.ordinal for page in pages] != list(range(len(pages))):
            raise ExternalRetrievalWaveError(
                "Europe PMC page lineage is incomplete or unordered"
            )
        expected_cursor = "*"
        cumulative_count = 0
        exact_hit_count: int | None = None
        for page in pages:
            if (
                page.strategy != adapter.strategy
                or page.adapter_version != adapter.version
                or page.request_state != {"cursor_mark": expected_cursor}
            ):
                raise ExternalRetrievalWaveError(
                    "Europe PMC persisted pagination/request lineage changed"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = sorted(
                (
                    attempt
                    for attempt in dataset.retrieval_attempts
                    if attempt.page_id == page.page_id
                ),
                key=lambda item: item.attempt_number,
            )
            if (
                not attempts
                or [attempt.attempt_id for attempt in attempts] != page.attempt_ids
                or [attempt.attempt_number for attempt in attempts]
                != list(range(1, len(attempts) + 1))
            ):
                raise ExternalRetrievalWaveError(
                    "Europe PMC attempt lineage is incomplete or unordered"
                )

            parsed_attempts = []
            parse_state = {
                "cursor_mark": expected_cursor,
                "retrieved_count": cumulative_count,
                "expected_hit_count": exact_hit_count,
            }
            for attempt in attempts:
                if (
                    attempt.response_status is None
                    or not 200 <= attempt.response_status < 300
                    or not attempt.raw_response_path
                    or not attempt.raw_response_hash
                    or attempt.request_method != request.method
                    or attempt.request_url != request.url
                    or attempt.request_params != request.sanitized_params()
                    or attempt.request_headers != request.sanitized_headers()
                    or attempt.request_hash != request.request_hash()
                ):
                    raise ExternalRetrievalWaveError(
                        "Europe PMC recovery requires exact successful HTTP request lineage"
                    )
                if attempt.status is RetrievalAttemptStatus.FAILED:
                    if attempt.error != EUROPE_PMC_TERMINAL_ERROR:
                        raise ExternalRetrievalWaveError(
                            "Europe PMC recovery refused a non-parser attempt failure"
                        )
                elif attempt.status is not RetrievalAttemptStatus.SUCCEEDED:
                    raise ExternalRetrievalWaveError(
                        "Europe PMC recovery found an unsupported attempt state"
                    )
                if attempt.raw_response_path in seen_response_paths:
                    raise ExternalRetrievalWaveError(
                        "Europe PMC raw response path is reused"
                    )
                try:
                    response = response_store.load_response(
                        attempt.raw_response_path, attempt.raw_response_hash
                    )
                except (OSError, ValueError) as exc:
                    raise ExternalRetrievalWaveError(
                        f"Europe PMC raw response hash/read failure: {exc}"
                    ) from exc
                seen_response_paths.add(attempt.raw_response_path)
                raw_response_references.append(
                    (attempt.raw_response_path, attempt.raw_response_hash)
                )
                try:
                    parsed = adapter.parse_response(spec, parse_state, response)
                except Exception as exc:
                    raise ExternalRetrievalWaveError(
                        "Europe PMC offline terminal recovery parse failed: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                parsed_attempts.append(parsed)

            parsed = parsed_attempts[-1]
            signatures = {
                _hash_payload(
                    {
                        "raw_item_count": item.raw_item_count,
                        "native_identifiers": item.native_identifiers,
                        "next_state": item.next_state,
                        "terminal": item.terminal,
                        "source_reported_total": item.source_reported_total,
                    }
                )
                for item in parsed_attempts
            }
            if len(signatures) != 1:
                raise ExternalRetrievalWaveError(
                    "Europe PMC retry responses disagree for one page"
                )
            page_occurrences = sorted(
                (
                    occurrence
                    for occurrence in dataset.occurrences
                    if occurrence.retrieval_page_id == page.page_id
                ),
                key=lambda item: item.source_rank or 0,
            )
            if (
                len(page_occurrences) != parsed.raw_item_count
                or [item.source_identifier for item in page_occurrences]
                != parsed.native_identifiers
                or page.returned_item_count != parsed.raw_item_count
                or page.native_identifiers != parsed.native_identifiers
            ):
                raise ExternalRetrievalWaveError(
                    "Europe PMC persisted record/page accounting changed"
                )
            if parsed.source_reported_total != expected_count or not parsed.total_is_exact:
                raise ExternalRetrievalWaveError(
                    "Europe PMC exact provider hitCount changed"
                )

            parsed_legacy_next_state = (
                {"cursor_mark": parsed.next_state["cursor_mark"]}
                if parsed.next_state is not None
                else None
            )
            final_attempt = attempts[-1]
            if page.status is RetrievalCompletionStatus.COMPLETE:
                if (
                    page.next_state != parsed_legacy_next_state
                    or page.source_reported_total != parsed.source_reported_total
                    or page.total_is_exact != parsed.total_is_exact
                    or page.terminal != parsed.terminal
                    or page.completion_proof != parsed.completion_proof
                    or final_attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                ):
                    raise ExternalRetrievalWaveError(
                        "Europe PMC completed-page evidence changed"
                    )
            else:
                if (
                    not was_failed
                    or page is not pages[-1]
                    or page.status is not RetrievalCompletionStatus.FAILED
                    or not parsed.terminal
                    or not parsed.metadata.get("repeated_cursor_terminal_sentinel")
                ):
                    raise ExternalRetrievalWaveError(
                        "Europe PMC recovery found a non-terminal failed page"
                    )
                prior_completion_error = page.metadata.get("completion_error")
                page.status = RetrievalCompletionStatus.COMPLETE
                page.source_reported_total = parsed.source_reported_total
                page.total_is_exact = parsed.total_is_exact
                page.terminal = parsed.terminal
                page.completion_proof = parsed.completion_proof
                page.next_state = parsed_legacy_next_state
                page.metadata = {
                    **parsed.metadata,
                    "offline_terminal_sentinel_recovery": True,
                    "raw_response_reused_without_network": True,
                    "prior_completion_error": prior_completion_error,
                }
                final_attempt.metadata = {
                    **final_attempt.metadata,
                    "offline_terminal_sentinel_recovery": True,
                    "prior_error": final_attempt.error,
                    "raw_response_reused_without_network": True,
                }
                final_attempt.status = RetrievalAttemptStatus.SUCCEEDED
                final_attempt.error = None
            cumulative_count += parsed.raw_item_count
            exact_hit_count = parsed.source_reported_total
            if parsed.next_state is not None:
                expected_cursor = str(parsed.next_state["cursor_mark"])
            elif page is not pages[-1]:
                raise ExternalRetrievalWaveError(
                    "Europe PMC terminal page is not last in its query lineage"
                )

        if not pages[-1].terminal or pages[-1].completion_proof != (
            "europe_pmc_cursor_exhausted"
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC query lacks a verified terminal page"
            )
        if cumulative_count != expected_count:
            raise ExternalRetrievalWaveError(
                "Europe PMC cumulative count does not match exact hitCount"
            )
        if was_failed:
            query.status = ProcessingStatus.OK
            query.completion_status = RetrievalCompletionStatus.COMPLETE
            query.completion_proof = pages[-1].completion_proof
            query.result_count = cumulative_count
            query.source_reported_total = expected_count
            query.total_is_exact = True
            query.errors = []
            query.retrieval_ended_at = recovered_at
            query.metadata["offline_terminal_sentinel_recovery"] = True
        elif (
            query.result_count != cumulative_count
            or query.source_reported_total != expected_count
            or not query.total_is_exact
            or query.completion_proof != pages[-1].completion_proof
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC completed QF02 accounting changed"
            )
        recovered_counts[spec.metadata["production_query_id"]] = cumulative_count

    if failed_query_count != 4:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery expected exactly four sentinel-rejected queries"
        )
    if len(raw_response_references) != EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError(
            "Europe PMC raw-response accounting changed"
        )
    run.status = ProcessingStatus.OK
    run.completion_status = RetrievalCompletionStatus.COMPLETE
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = recovered_at[:10]
    run.errors = []
    run.metadata["offline_terminal_sentinel_recovery"] = True
    run.metadata["network_requests_during_recovery"] = 0
    run.metadata["source_raw_response_count"] = len(raw_response_references)
    dataset.validate()
    return {
        "raw_response_references": raw_response_references,
        "query_occurrence_counts": recovered_counts,
    }


def _validate_authorized_europe_pmc_terminal_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery episode lineage changed"
        )
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or not failed.get("immutable")
        or recovered.get("episode_number") != 2
        or recovered.get("status") != EUROPE_PMC_TERMINAL_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or not recovered.get("immutable")
        or recovered.get("network_used") is not False
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError(
            "authorized Europe PMC terminal-recovery lineage changed"
        )
    source_checkpoint = _safe_output_path(
        root, recovered["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(
        source_checkpoint, recovered["source_episode_checkpoint"], root
    )
    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    for binding in recovered.get("source_raw_responses", []):
        for key in ("failed_episode_path", "recovery_copy_path"):
            path = _safe_output_path(root, binding[key])
            raw = path.read_bytes()
            if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
                raise ExternalRetrievalWaveError(
                    "authorized Europe PMC recovery raw-response binding changed"
                )


def _reparse_failed_pubmed_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError("PubMed parser recovery requires exactly one run")
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "PubMed episode-2 run is not an eligible parser-only failure"
        )
    if not dataset.occurrences or not dataset.retrieval_attempts:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery requires persisted fetched records and responses"
        )
    if any(
        item.status is not ProcessingStatus.FAILED
        or not item.errors
        or any("PubMed EFetch PMID sequence" not in error for error in item.errors)
        for item in dataset.source_queries
    ):
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery refused for a non-sequence parser failure"
        )

    specs = _source_query_specs(wave, "PubMed", ieee_credential="")
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("PubMed episode-2 query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["PubMed"]
    response_store = CheckpointStore(checkpoint_dir)
    old_identifiers = {item.source_identifier for item in dataset.occurrences}
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    reconstructed_occurrences: list[RecordOccurrence] = []
    remaining_batches: list[dict[str, Any]] = []
    historical_request_hashes = {
        attempt.request_hash for attempt in dataset.retrieval_attempts
    }

    for query, spec in zip(dataset.source_queries, specs, strict=True):
        if (
            query.source_database != "PubMed"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 frozen query/request binding changed"
            )
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if not pages or pages[0].request_state != adapter.initial_state(spec):
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 pagination does not start with frozen ESearch"
            )
        expected_state = adapter.initial_state(spec)
        source_rank = 0
        for page in pages:
            if page.request_state != expected_state:
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 persisted pagination state sequence changed"
                )
            if page.adapter_version not in {"2.0.0", adapter.version}:
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 adapter version is not eligible for parser recovery"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = [
                attempt
                for attempt in dataset.retrieval_attempts
                if attempt.page_id == page.page_id
            ]
            if not attempts:
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 page lacks persisted attempt provenance"
                )
            for attempt in attempts:
                if (
                    attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                    or attempt.response_status is None
                    or not 200 <= attempt.response_status < 300
                    or not attempt.raw_response_path
                    or not attempt.raw_response_hash
                    or attempt.request_method != request.method
                    or attempt.request_hash != request.request_hash()
                ):
                    raise ExternalRetrievalWaveError(
                        "PubMed parser recovery requires successful hash-bound responses"
                    )
                if attempt.raw_response_path in seen_response_paths:
                    raise ExternalRetrievalWaveError(
                        "PubMed episode-2 raw response path is reused"
                    )
                _load_pubmed_recovery_response(
                    response_store,
                    attempt.raw_response_path,
                    attempt.raw_response_hash,
                )
                seen_response_paths.add(attempt.raw_response_path)
                raw_response_references.append(
                    (attempt.raw_response_path, attempt.raw_response_hash)
                )
            attempt = attempts[-1]
            response = _load_pubmed_recovery_response(
                response_store,
                str(attempt.raw_response_path),
                str(attempt.raw_response_hash),
            )
            try:
                parsed = adapter.parse_response(spec, request.state, response)
            except Exception as exc:
                raise ExternalRetrievalWaveError(
                    f"PubMed episode-2 offline parse failed: {type(exc).__name__}: {exc}"
                ) from exc
            if parsed.incomplete_reason or parsed.raw_item_count != len(parsed.records):
                raise ExternalRetrievalWaveError(
                    parsed.incomplete_reason
                    or "PubMed recovery parser record accounting mismatch"
                )
            expected_pmids = list(request.state.get("batch_pmids") or [])
            returned_pmids = [record.pmid for record in parsed.records]
            if expected_pmids and returned_pmids != expected_pmids:
                raise ExternalRetrievalWaveError(
                    "PubMed recovery returned a missing, unexpected, or reordered PMID"
                )

            prior_adapter_version = page.adapter_version
            page.adapter_version = adapter.version
            page.returned_item_count = parsed.raw_item_count
            page.native_identifiers = list(parsed.native_identifiers)
            page.next_state = parsed.next_state
            page.source_reported_total = parsed.source_reported_total
            page.total_is_exact = parsed.total_is_exact
            page.terminal = parsed.terminal
            page.completion_proof = parsed.completion_proof
            page.truncated = parsed.truncated
            page.truncation_reason = parsed.truncation_reason
            page.status = RetrievalCompletionStatus.COMPLETE
            page.metadata = {
                **parsed.metadata,
                "offline_parser_recovery": True,
                "recovered_from_adapter_version": prior_adapter_version,
                "raw_response_reused_without_network": True,
            }
            page.occurrence_ids = []
            for rank, record in enumerate(parsed.records, start=1):
                source_rank += 1
                source_identifier = native_identifier(record, source_rank)
                occurrence = RecordOccurrence(
                    occurrence_id=_retrieval_stable_id(
                        "occurrence", page.page_id, str(rank), source_identifier
                    ),
                    source_query_id=query.query_id,
                    source_identifier=source_identifier,
                    retrieved_at=attempt.ended_at or recovered_at,
                    record=record,
                    source_rank=source_rank,
                    page=page.ordinal,
                    cursor=_pubmed_state_cursor(page.request_state),
                    raw_payload_hash=_hash_payload(record.original_metadata),
                    metadata={
                        "source_identifier_missing": record.source_identifier is None,
                        "parser_incomplete": bool(
                            record.original_metadata.get("parser_incomplete")
                        ),
                        "offline_parser_recovery": True,
                    },
                    retrieval_page_id=page.page_id,
                )
                reconstructed_occurrences.append(occurrence)
                page.occurrence_ids.append(occurrence.occurrence_id)
            expected_state = parsed.next_state

        search_page = pages[0]
        pmids = list(search_page.next_state.get("pmids") or []) if search_page.next_state else []
        if expected_state is None:
            query.status = ProcessingStatus.OK
            query.completion_status = RetrievalCompletionStatus.COMPLETE
            query.completion_proof = pages[-1].completion_proof
        else:
            if expected_state.get("phase") != "fetch":
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 remaining state is not EFetch"
                )
            next_index = int(expected_state.get("index", -1))
            if next_index < 0 or next_index >= len(pmids):
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 remaining EFetch index is invalid"
                )
            for start in range(next_index, len(pmids), spec.limit):
                batch = pmids[start : start + spec.limit]
                batch_state = {"phase": "fetch", "pmids": pmids, "index": start}
                batch_request = adapter.build_request(spec, batch_state)
                if batch_request.request_hash() in historical_request_hashes:
                    raise ExternalRetrievalWaveError(
                        "PubMed recovery planned a remaining batch that was already requested"
                    )
                remaining_batches.append(
                    {
                        "production_query_id": spec.metadata["production_query_id"],
                        "start_index": start,
                        "end_index_exclusive": start + len(batch),
                        "pmids": batch,
                        "pmid_manifest_sha256": _hash_payload({"pmids": batch}),
                        "request_hash": batch_request.request_hash(),
                    }
                )
            query.status = ProcessingStatus.PARTIAL
            query.completion_status = RetrievalCompletionStatus.RUNNING
            query.completion_proof = None
        query.errors = []
        query.result_count = source_rank
        query.source_reported_total = len(pmids)
        query.total_is_exact = True
        query.retrieval_ended_at = recovered_at

    reconstructed_identifiers = {
        item.source_identifier for item in reconstructed_occurrences
    }
    recovered_pmids = tuple(
        pmid
        for pmid in PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS
        if pmid in reconstructed_identifiers and pmid not in old_identifiers
    )
    if recovered_pmids != PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS or (
        reconstructed_identifiers - old_identifiers
    ) != set(PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS):
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery did not recover exactly the six known book PMIDs"
        )
    record_by_pmid = {
        occurrence.record.pmid: occurrence.record for occurrence in reconstructed_occurrences
    }
    if any(
        record_by_pmid[pmid].original_metadata.get("pubmed_record_type")
        != "PubmedBookArticle"
        for pmid in recovered_pmids
    ):
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery PMID is not a PubmedBookArticle"
        )

    dataset.occurrences = reconstructed_occurrences
    if not remaining_batches:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery found no remaining live EFetch work"
        )
    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.external_retrieval_wave.pubmed_parser_recovery",
            actor_type=ActorType.SOFTWARE,
            metadata={"software_version": WAVE_VERSION},
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=run.retrieval_started_at,
        metadata={
            "run_id": run.run_id,
            "rule": "doi_first_title_fallback",
            "offline_parser_recovery": True,
        },
    )
    dataset.canonical_records, dataset.duplicate_decisions = canonicalize_occurrences(
        dataset.occurrences, provenance=provenance
    )
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_cutoff_date = None
    run.retrieval_completed_at = recovered_at
    run.errors = ["offline parser recovery complete; remaining PubMed EFetch batches pending"]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata["parser_recovery_source_response_count"] = len(
        raw_response_references
    )
    run.metadata["remaining_efetch_request_count"] = len(remaining_batches)
    dataset.validate()
    return {
        "recovered_pmids": recovered_pmids,
        "raw_response_references": raw_response_references,
        "remaining_efetch_request_count": len(remaining_batches),
        "remaining_efetch_batches": remaining_batches,
    }


def _validate_authorized_pubmed_parser_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    active = next(
        (
            item
            for item in episodes
            if item.get("episode_number") == source_state.get("active_episode_number")
        ),
        None,
    )
    if (
        active is None
        or active.get("episode_number") != 3
        or active.get("status") != PUBMED_PARSER_RECOVERY_STATUS
        or active.get("recovery_of_episode_number") != 2
        or source_state.get("active_checkpoint_path") != active.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError("authorized PubMed parser recovery lineage changed")
    prior = next(
        (item for item in episodes if item.get("episode_number") == 2), None
    )
    if prior is None or not prior.get("immutable") or prior.get("status") != "FAILED":
        raise ExternalRetrievalWaveError("PubMed episode 2 is not preserved")
    prior_checkpoint = _safe_output_path(
        root, active["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(prior_checkpoint, active["source_episode_checkpoint"], root)
    recovery_checkpoint = _safe_output_path(
        root, active["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, active["checkpoint_dataset"], root)
    for binding in active.get("source_raw_responses", []):
        for key in ("episode_2_path", "recovery_copy_path"):
            path = _safe_output_path(root, binding[key])
            raw = path.read_bytes()
            if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
                raise ExternalRetrievalWaveError(
                    "authorized PubMed parser recovery raw-response binding changed"
                )


def _load_pubmed_recovery_response(
    store: CheckpointStore, relative_path: str, expected_hash: str
) -> Any:
    try:
        return store.load_response(relative_path, expected_hash)
    except (OSError, ValueError) as exc:
        raise ExternalRetrievalWaveError(
            f"PubMed episode-2 raw response hash/read failure: {exc}"
        ) from exc


def _retrieval_stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _pubmed_state_cursor(state: Mapping[str, Any]) -> str | None:
    return str(state["index"]) if state.get("index") is not None else None


def _validate_response_free_pubmed_transport_failure(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
) -> None:
    if dataset.occurrences or dataset.canonical_records or dataset.duplicate_decisions:
        raise ExternalRetrievalWaveError(
            "PubMed transport retry refused because records were already imported"
        )
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError("PubMed failure checkpoint must contain one run")
    run = dataset.retrieval_runs[0]
    if run.completion_status is not RetrievalCompletionStatus.FAILED:
        raise ExternalRetrievalWaveError("PubMed checkpoint is not terminal FAILED")
    if run.query_plan_version != wave.query_plan_hash:
        raise ExternalRetrievalWaveError("PubMed failed episode query-plan binding changed")
    if not dataset.retrieval_attempts:
        raise ExternalRetrievalWaveError("PubMed failed episode contains no attempts")
    if any(
        attempt.status is not RetrievalAttemptStatus.FAILED
        or attempt.response_status is not None
        or attempt.raw_response_path is not None
        or attempt.raw_response_hash is not None
        or attempt.response_url is not None
        for attempt in dataset.retrieval_attempts
    ):
        raise ExternalRetrievalWaveError(
            "PubMed transport retry refused because an HTTP response exists"
        )
    response_dir = checkpoint_dir / "responses"
    if response_dir.exists() and any(response_dir.iterdir()):
        raise ExternalRetrievalWaveError(
            "PubMed transport retry refused because raw provider responses exist"
        )
    for attempt in dataset.retrieval_attempts:
        error_type = str(attempt.error or "").partition(":")[0]
        if error_type not in TRANSPORT_ENVIRONMENT_FAILURE_TYPES:
            raise ExternalRetrievalWaveError(
                "PubMed transport retry refused for a non-transport failure"
            )

    specs = _source_query_specs(wave, "PubMed", ieee_credential="")
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("PubMed failed episode query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["PubMed"]
    for query, spec in zip(dataset.source_queries, specs, strict=True):
        if (
            query.source_database != "PubMed"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "PubMed failed episode frozen query/request binding changed"
            )
        pages = [
            page for page in dataset.retrieval_pages if page.source_query_id == query.query_id
        ]
        if len(pages) != 1 or pages[0].request_state != adapter.initial_state(spec):
            raise ExternalRetrievalWaveError(
                "PubMed failed episode does not contain only its initial ESearch page"
            )
        expected_request = adapter.build_request(spec, pages[0].request_state)
        attempts = [
            attempt
            for attempt in dataset.retrieval_attempts
            if attempt.page_id == pages[0].page_id
        ]
        if not attempts or any(
            attempt.request_method != "POST"
            or attempt.request_hash != expected_request.request_hash()
            for attempt in attempts
        ):
            raise ExternalRetrievalWaveError(
                "PubMed failed episode ESearch request hash/method changed"
            )


def _validate_arxiv_episode_1_provenance(
    *,
    episode_1: Mapping[str, Any],
    episode_2: Mapping[str, Any],
    root: Path,
    wave: ProductionRetrievalWave,
) -> None:
    if (
        episode_1.get("episode_number") != 1
        or episode_1.get("status") != "FAILED"
        or episode_1.get("immutable") is not True
        or episode_1.get("attempt_count") != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or episode_1.get("transport_timeout_count") != 8
        or episode_1.get("http_429_count") != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or episode_1.get("successful_http_response_count") != 0
        or episode_1.get("occurrence_count") != 0
        or episode_2.get("source_episode_checkpoint")
        != episode_1.get("checkpoint_dataset")
        or episode_2.get("source_attempt_manifest_hash")
        != episode_1.get("source_attempt_manifest_hash")
        or episode_2.get("source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or episode_2.get("source_raw_responses")
        != episode_1.get("raw_responses")
        or episode_2.get("restart_states") is None
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 provenance changed")
    checkpoint_reference = episode_1.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("arXiv episode 1 lacks a checkpoint binding")
    checkpoint = _safe_output_path(root, str(checkpoint_reference["path"]))
    _verify_file_reference(checkpoint, checkpoint_reference, root)
    payload = _load_json(checkpoint)
    if episode_1.get("source_attempt_manifest_hash") != _hash_payload(
        {"retrieval_attempts": payload.get("retrieval_attempts", [])}
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 attempt manifest changed")
    validated = _validate_failed_arxiv_rate_limit_checkpoint(
        dataset=load_review_dataset(checkpoint),
        checkpoint_dir=checkpoint.parent,
        root=root,
        wave=wave,
    )
    if (
        episode_1.get("raw_responses") != validated["raw_response_bindings"]
        or episode_1.get("query_attempt_signatures")
        != validated["query_attempt_signatures"]
        or episode_2.get("restart_states") != validated["restart_states"]
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 evidence manifest changed")
    _verify_arxiv_raw_response_bindings(root, episode_1["raw_responses"])


def _validate_arxiv_mixed_state_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    if (
        dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or any(
            page.status is RetrievalCompletionStatus.COMPLETE
            or page.returned_item_count
            or page.occurrence_ids
            or page.native_identifiers
            for page in dataset.retrieval_pages
        )
        or any(
            attempt.status is RetrievalAttemptStatus.SUCCEEDED
            or (
                attempt.response_status is not None
                and 200 <= attempt.response_status < 300
            )
            for attempt in dataset.retrieval_attempts
        )
    ):
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery refused because a successful page or occurrence exists"
        )
    try:
        dataset.validate()
    except ValueError as exc:
        raise ExternalRetrievalWaveError(
            f"arXiv episode-2 checkpoint validation failed: {exc}"
        ) from exc
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery requires exactly one retrieval run"
        )
    run = dataset.retrieval_runs[0]
    expected_pause_metadata = {
        "source_database": "arXiv",
        "http_status": 429,
        "retry_after_header_present": False,
        "retry_after": None,
    }
    if (
        run.status is not ProcessingStatus.PARTIAL
        or run.completion_status is not RetrievalCompletionStatus.RUNNING
        or run.retrieval_cutoff_date is not None
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.planned_query_ids != [query.query_id for query in dataset.source_queries]
        or run.source_query_ids != run.planned_query_ids
        or run.errors != ["PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"]
        or run.metadata.get("pause_state") != "PROVIDER_RATE_LIMIT"
        or run.metadata.get("pause_reason")
        != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or run.metadata.get("pause_metadata") != expected_pause_metadata
        or run.metadata.get("session_request_count")
        != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_pages) != 4
        or len(dataset.retrieval_attempts) != ARXIV_MIXED_EXPECTED_ATTEMPTS
    ):
        raise ExternalRetrievalWaveError(
            "arXiv checkpoint is not the exact episode-2 mixed pause"
        )

    adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_bindings: list[dict[str, Any]] = []
    query_attempt_signatures: list[dict[str, Any]] = []
    restart_states: list[dict[str, Any]] = []
    seen_attempt_ids: set[str] = set()
    seen_response_paths: set[str] = set()

    for family_index, (query, spec, expected_kinds) in enumerate(
        zip(
            dataset.source_queries,
            specs,
            ARXIV_MIXED_EXPECTED_ATTEMPT_KINDS,
            strict=True,
        )
    ):
        expected_completion = (
            RetrievalCompletionStatus.FAILED
            if family_index < 3
            else (
                RetrievalCompletionStatus.RUNNING
                if family_index == 3
                else RetrievalCompletionStatus.PLANNED
            )
        )
        expected_status = (
            ProcessingStatus.FAILED
            if family_index < 3
            else ProcessingStatus.PARTIAL
        )
        if (
            query.source_database != "arXiv"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not expected_completion
            or query.status is not expected_status
            or query.result_count != 0
            or query.source_reported_total is not None
            or query.total_is_exact
            or query.completion_proof is not None
        ):
            raise ExternalRetrievalWaveError(
                "arXiv mixed recovery refused changed frozen query/state provenance"
            )
        expected_errors = (
            ["retrieval attempts exhausted before a response"]
            if family_index < 3
            else []
        )
        if query.errors != expected_errors:
            raise ExternalRetrievalWaveError(
                "arXiv mixed recovery refused changed query failure signature"
            )
        pages = [
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == query.query_id
        ]
        if family_index == 4:
            if pages or query.page_ids:
                raise ExternalRetrievalWaveError(
                    "arXiv QF05 must remain unattempted and planned"
                )
            request = adapter.build_request(spec, adapter.initial_state(spec))
            attempts: list[Any] = []
        else:
            if len(pages) != 1 or query.page_ids != [pages[0].page_id]:
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery requires one initial page for QF01-QF04"
                )
            page = pages[0]
            expected_page_status = (
                RetrievalCompletionStatus.FAILED
                if family_index < 3
                else RetrievalCompletionStatus.RUNNING
            )
            if (
                page.ordinal != 0
                or page.request_state != {"start": 0}
                or page.status is not expected_page_status
                or page.next_state is not None
                or page.source_reported_total is not None
                or page.returned_item_count != 0
                or page.occurrence_ids
                or page.native_identifiers
                or page.terminal
                or page.truncated
                or (
                    family_index < 3
                    and page.metadata.get("completion_error")
                    != "retrieval attempts exhausted before a response"
                )
                or (family_index == 3 and page.metadata)
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery refused changed initial-page lineage"
                )
            request = adapter.build_request(spec, page.request_state)
            attempt_by_id = {
                item.attempt_id: item for item in dataset.retrieval_attempts
            }
            try:
                attempts = [attempt_by_id[item] for item in page.attempt_ids]
            except KeyError as exc:
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery attempt lineage is incomplete"
                ) from exc
        if (
            len(attempts) != len(expected_kinds)
            or [item.attempt_number for item in attempts]
            != list(range(1, len(attempts) + 1))
        ):
            raise ExternalRetrievalWaveError(
                "arXiv mixed recovery attempt count/order changed"
            )
        observed_kinds: list[str] = []
        for index, attempt in enumerate(attempts):
            if (
                attempt.attempt_id in seen_attempt_ids
                or attempt.page_id != page.page_id
                or attempt.status is not RetrievalAttemptStatus.FAILED
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
                or attempt.retry_of_attempt_id
                != (attempts[index - 1].attempt_id if index else None)
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery refused changed request hash/method/lineage"
                )
            seen_attempt_ids.add(attempt.attempt_id)
            if attempt.response_status is None:
                failure_type = str(attempt.error or "").partition(":")[0]
                if (
                    failure_type != "ReadTimeout"
                    or attempt.raw_response_path is not None
                    or attempt.raw_response_hash is not None
                    or attempt.response_headers
                    or attempt.response_url is not None
                    or attempt.actual_request_url is not None
                    or attempt.metadata
                ):
                    raise ExternalRetrievalWaveError(
                        "arXiv mixed recovery encountered non-signature transport failure"
                    )
                observed_kinds.append(failure_type)
                continue
            if (
                family_index != 3
                or attempt.response_status != 429
                or attempt.error != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.raw_response_path in seen_response_paths
                or attempt.metadata.get("provider_pause")
                != expected_pause_metadata
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery encountered non-signature HTTP failure"
                )
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"arXiv episode-2 raw response hash/read failure: {exc}"
                ) from exc
            if (
                response.status_code != 429
                or response.headers != attempt.response_headers
                or any(
                    str(key).lower() == "retry-after"
                    for key in response.headers
                )
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv episode-2 persisted response metadata changed"
                )
            response_path = checkpoint_dir / attempt.raw_response_path
            raw = response_path.read_bytes()
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_bindings.append(
                {
                    "episode_2_path": response_path.relative_to(root).as_posix(),
                    "attempt_id": attempt.attempt_id,
                    "byte_size": len(raw),
                    "raw_sha256": attempt.raw_response_hash,
                    "http_status": 429,
                    "retry_after_header_present": False,
                    "retry_after": None,
                }
            )
            observed_kinds.append("HTTP_429")
        if tuple(observed_kinds) != expected_kinds:
            raise ExternalRetrievalWaveError(
                "arXiv episode-2 mixed attempt signature changed"
            )
        query_attempt_signatures.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "attempt_kinds": observed_kinds,
                "request_hash": request.request_hash(),
            }
        )
        restart_states.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "request_state": adapter.initial_state(spec),
                "max_results": spec.limit,
                "request_hash": request.request_hash(),
            }
        )

    if len(seen_attempt_ids) != ARXIV_MIXED_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("arXiv episode-2 contains unbound attempts")
    response_files = {
        path.relative_to(checkpoint_dir).as_posix()
        for path in response_store.responses.iterdir()
        if path.is_file()
    }
    if (
        len(raw_response_bindings) != ARXIV_MIXED_EXPECTED_RESPONSES
        or response_files != seen_response_paths
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 raw-response manifest is missing or has unbound files"
        )
    return {
        "raw_response_bindings": raw_response_bindings,
        "query_attempt_signatures": query_attempt_signatures,
        "restart_states": restart_states,
    }


def _verify_arxiv_mixed_raw_response_bindings(
    root: Path, bindings: list[dict[str, Any]]
) -> None:
    if len(bindings) != ARXIV_MIXED_EXPECTED_RESPONSES:
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 raw-response manifest changed"
        )
    for binding in bindings:
        path = _safe_output_path(root, str(binding.get("episode_2_path", "")))
        if not path.is_file():
            raise ExternalRetrievalWaveError("arXiv episode-2 raw response is missing")
        raw = path.read_bytes()
        if (
            len(raw) != binding.get("byte_size")
            or _sha256(raw) != binding.get("raw_sha256")
            or binding.get("http_status") != 429
            or binding.get("retry_after_header_present") is not False
            or binding.get("retry_after") is not None
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-2 raw response changed during recovery"
            )


def _validate_failed_arxiv_rate_limit_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    if (
        dataset.occurrences
        or any(
            page.status is RetrievalCompletionStatus.COMPLETE
            or page.returned_item_count
            or page.occurrence_ids
            or page.native_identifiers
            for page in dataset.retrieval_pages
        )
        or any(
            attempt.status is RetrievalAttemptStatus.SUCCEEDED
            or (
                attempt.response_status is not None
                and 200 <= attempt.response_status < 300
            )
            for attempt in dataset.retrieval_attempts
        )
    ):
        raise ExternalRetrievalWaveError(
            "arXiv recovery refused because a successful page or occurrence exists"
        )
    try:
        dataset.validate()
    except ValueError as exc:
        raise ExternalRetrievalWaveError(
            f"arXiv failed checkpoint validation failed: {exc}"
        ) from exc
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery requires exactly one retrieval run"
        )
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.planned_query_ids != [query.query_id for query in dataset.source_queries]
        or run.source_query_ids != run.planned_query_ids
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_pages) != 5
        or len(dataset.retrieval_attempts) != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
    ):
        raise ExternalRetrievalWaveError(
            "arXiv checkpoint is not the known timeout/rate-limit failure"
        )
    if (
        dataset.canonical_records
        or dataset.duplicate_decisions
    ):
        raise ExternalRetrievalWaveError(
            "arXiv recovery refused because a successful page or occurrence exists"
        )

    adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_bindings = []
    query_attempt_signatures = []
    restart_states = []
    seen_response_paths: set[str] = set()
    expected_run_errors = []

    for query, spec, expected_kinds in zip(
        dataset.source_queries,
        specs,
        ARXIV_RATE_LIMIT_EXPECTED_ATTEMPT_KINDS,
        strict=True,
    ):
        if (
            query.source_database != "arXiv"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.status is not ProcessingStatus.FAILED
            or query.result_count != 0
            or len(query.errors) != 1
        ):
            raise ExternalRetrievalWaveError(
                "arXiv recovery refused changed frozen query/failure provenance"
            )
        pages = [
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == query.query_id
        ]
        if (
            len(pages) != 1
            or query.page_ids != [pages[0].page_id]
            or pages[0].ordinal != 0
            or pages[0].request_state != {"start": 0}
            or pages[0].status is not RetrievalCompletionStatus.FAILED
            or pages[0].next_state is not None
            or pages[0].source_reported_total is not None
            or pages[0].terminal
            or pages[0].truncated
            or pages[0].metadata.get("completion_error") != query.errors[0]
        ):
            raise ExternalRetrievalWaveError(
                "arXiv recovery refused changed initial-page failure lineage"
            )
        page = pages[0]
        request = adapter.build_request(spec, page.request_state)
        attempts = [
            attempt
            for attempt_id in page.attempt_ids
            for attempt in dataset.retrieval_attempts
            if attempt.attempt_id == attempt_id
        ]
        if len(attempts) != 3 or [item.attempt_number for item in attempts] != [1, 2, 3]:
            raise ExternalRetrievalWaveError(
                "arXiv recovery requires exactly three ordered attempts per family"
            )
        observed_kinds = []
        for index, attempt in enumerate(attempts):
            if (
                attempt.page_id != page.page_id
                or attempt.status is not RetrievalAttemptStatus.FAILED
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
                or attempt.retry_of_attempt_id
                != (attempts[index - 1].attempt_id if index else None)
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv recovery refused changed request hash/method/lineage"
                )
            if attempt.response_status is None:
                failure_type = str(attempt.error or "").partition(":")[0]
                if (
                    failure_type != "ReadTimeout"
                    or attempt.raw_response_path is not None
                    or attempt.raw_response_hash is not None
                    or attempt.response_headers
                    or attempt.response_url is not None
                ):
                    raise ExternalRetrievalWaveError(
                        "arXiv recovery encountered non-signature transport failure"
                    )
                observed_kinds.append(failure_type)
                continue
            if (
                attempt.response_status != 429
                or not str(attempt.error or "").startswith("HTTP 429 from ")
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.raw_response_path in seen_response_paths
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv recovery encountered non-signature HTTP failure"
                )
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"arXiv raw response hash/read failure: {exc}"
                ) from exc
            if (
                response.status_code != 429
                or response.headers != attempt.response_headers
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv persisted response metadata changed"
                )
            response_path = checkpoint_dir / attempt.raw_response_path
            raw = response_path.read_bytes()
            retry_after = next(
                (
                    str(value)
                    for key, value in response.headers.items()
                    if str(key).lower() == "retry-after"
                ),
                None,
            )
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_bindings.append(
                {
                    "episode_1_path": response_path.relative_to(root).as_posix(),
                    "attempt_id": attempt.attempt_id,
                    "byte_size": len(raw),
                    "raw_sha256": attempt.raw_response_hash,
                    "http_status": 429,
                    "retry_after_header_present": retry_after is not None,
                    "retry_after": retry_after,
                }
            )
            observed_kinds.append("HTTP_429")
        if tuple(observed_kinds) != expected_kinds:
            raise ExternalRetrievalWaveError(
                "arXiv timeout/429 attempt signature changed"
            )
        if expected_kinds[-1] == "ReadTimeout":
            if query.errors != ["retrieval attempts exhausted before a response"]:
                raise ExternalRetrievalWaveError(
                    "arXiv transport exhaustion failure signature changed"
                )
        elif not query.errors[0].startswith("HTTP 429 from "):
            raise ExternalRetrievalWaveError(
                "arXiv HTTP 429 failure signature changed"
            )
        expected_run_errors.append(f"{query.query_id}: {query.errors[0]}")
        query_attempt_signatures.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "attempt_kinds": observed_kinds,
                "request_hash": request.request_hash(),
            }
        )
        restart_states.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "request_state": adapter.initial_state(spec),
                "max_results": spec.limit,
                "request_hash": request.request_hash(),
            }
        )

    if run.errors != expected_run_errors:
        raise ExternalRetrievalWaveError("arXiv run failure manifest changed")
    response_files = {
        path.relative_to(checkpoint_dir).as_posix()
        for path in response_store.responses.iterdir()
        if path.is_file()
    }
    if (
        len(raw_response_bindings) != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or response_files != seen_response_paths
    ):
        raise ExternalRetrievalWaveError(
            "arXiv raw-response artifact manifest is missing or has unbound files"
        )
    return {
        "raw_response_bindings": raw_response_bindings,
        "query_attempt_signatures": query_attempt_signatures,
        "restart_states": restart_states,
    }


def _verify_arxiv_raw_response_bindings(
    root: Path, bindings: list[dict[str, Any]]
) -> None:
    if len(bindings) != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES:
        raise ExternalRetrievalWaveError("arXiv recovery raw-response manifest changed")
    for binding in bindings:
        path = _safe_output_path(root, str(binding.get("episode_1_path", "")))
        raw = path.read_bytes()
        if (
            not path.is_file()
            or len(raw) != binding.get("byte_size")
            or _sha256(raw) != binding.get("raw_sha256")
            or binding.get("http_status") != 429
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-1 raw response changed during recovery"
            )


def _validate_authorized_arxiv_rate_limit_recovery(
    source_state: dict[str, Any], root: Path, wave: ProductionRetrievalWave
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError("arXiv episode-2 recovery lineage changed")
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or not failed.get("immutable")
        or failed.get("attempt_count") != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or failed.get("http_429_count") != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or recovered.get("episode_number") != 2
        or recovered.get("status") != ARXIV_RATE_LIMIT_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != recovered.get("checkpoint_dataset")
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != 0
        or source_state.get("requests_this_session") != 0
        or source_state.get("preserved_source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or source_state.get("preserved_source_raw_response_count")
        != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or recovered.get("source_episode_checkpoint")
        != failed.get("checkpoint_dataset")
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 recovery lineage changed")
    failed_checkpoint = _safe_output_path(
        root, failed["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(failed_checkpoint, failed["checkpoint_dataset"], root)
    failed_payload = _load_json(failed_checkpoint)
    if failed.get("source_attempt_manifest_hash") != _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 attempt manifest changed")
    failed_dataset = load_review_dataset(failed_checkpoint)
    validated = _validate_failed_arxiv_rate_limit_checkpoint(
        dataset=failed_dataset,
        checkpoint_dir=failed_checkpoint.parent,
        root=root,
        wave=wave,
    )
    if (
        recovered.get("source_raw_responses")
        != validated["raw_response_bindings"]
        or recovered.get("restart_states") != validated["restart_states"]
        or failed.get("query_attempt_signatures")
        != validated["query_attempt_signatures"]
    ):
        raise ExternalRetrievalWaveError("arXiv recovery evidence manifest changed")
    _verify_arxiv_raw_response_bindings(root, recovered["source_raw_responses"])

    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(
        recovery_checkpoint, recovered["checkpoint_dataset"], root
    )
    dataset = load_review_dataset(recovery_checkpoint)
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if (
        len(dataset.retrieval_runs) != 1
        or dataset.retrieval_runs[0].completion_status
        is not RetrievalCompletionStatus.RUNNING
        or dataset.retrieval_runs[0].query_plan_hash != _query_plan_hash(specs)
        or dataset.retrieval_pages
        or dataset.retrieval_attempts
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or [query.query_id for query in dataset.source_queries]
        != [item["query_id"] for item in recovered["restart_states"]]
        or any(
            query.completion_status is not RetrievalCompletionStatus.PLANNED
            or query.status is not ProcessingStatus.PARTIAL
            or query.result_count != 0
            or query.page_ids
            or query.errors
            for query in dataset.source_queries
        )
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 restart checkpoint changed")
    dataset.validate()


def _validate_authorized_arxiv_mixed_state_recovery(
    source_state: dict[str, Any], root: Path, wave: ProductionRetrievalWave
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 3:
        raise ExternalRetrievalWaveError("arXiv episode-3 recovery lineage changed")
    episode_1, episode_2, recovered = episodes
    _validate_arxiv_episode_1_provenance(
        episode_1=episode_1,
        episode_2=episode_2,
        root=root,
        wave=wave,
    )
    if (
        episode_2.get("episode_number") != 2
        or episode_2.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
        or episode_2.get("immutable") is not True
        or episode_2.get("attempt_count") != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or episode_2.get("completed_query_count") != 0
        or episode_2.get("occurrence_count") != 0
        or recovered.get("episode_number") != 3
        or recovered.get("status") != ARXIV_MIXED_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 2
        or recovered.get("authorization_reason")
        != "OFFLINE_ARXIV_MIXED_TRANSPORT_AND_RATE_LIMIT_RECOVERY"
        or recovered.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or recovered.get("frozen_query_plan_hash") != wave.query_plan_hash
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or recovered.get("source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        + ARXIV_MIXED_EXPECTED_ATTEMPTS
        or recovered.get("source_raw_response_count")
        != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        + ARXIV_MIXED_EXPECTED_RESPONSES
        or source_state.get("active_episode_number") != 3
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != recovered.get("checkpoint_dataset")
        or source_state.get("active_run_id") != recovered.get("run_id")
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != 0
        or source_state.get("requests_this_session") != 0
        or source_state.get("pause_reason")
        != "OFFLINE_MIXED_STATE_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
        or source_state.get("failure_reason") is not None
        or source_state.get("pause_metadata") is not None
        or source_state.get("preserved_source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        + ARXIV_MIXED_EXPECTED_ATTEMPTS
        or source_state.get("preserved_source_raw_response_count")
        != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        + ARXIV_MIXED_EXPECTED_RESPONSES
    ):
        raise ExternalRetrievalWaveError("arXiv episode-3 recovery lineage changed")

    episode_2_reference = episode_2.get("checkpoint_dataset")
    if (
        not episode_2_reference
        or episode_2_reference.get("raw_sha256")
        != ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 checkpoint binding changed")
    episode_2_checkpoint = _safe_output_path(
        root, str(episode_2_reference["path"])
    )
    _verify_file_reference(episode_2_checkpoint, episode_2_reference, root)
    episode_2_payload = _load_json(episode_2_checkpoint)
    expected_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": episode_2_payload.get("retrieval_attempts", [])}
    )
    episode_2_dataset = load_review_dataset(episode_2_checkpoint)
    validated = _validate_arxiv_mixed_state_checkpoint(
        dataset=episode_2_dataset,
        checkpoint_dir=episode_2_checkpoint.parent,
        root=root,
        wave=wave,
    )
    if (
        recovered.get("episode_2_attempt_manifest_hash")
        != expected_attempt_manifest_hash
        or recovered.get("episode_2_raw_responses")
        != validated["raw_response_bindings"]
        or recovered.get("episode_2_query_attempt_signatures")
        != validated["query_attempt_signatures"]
        or recovered.get("restart_states") != validated["restart_states"]
        or episode_2_dataset.retrieval_runs[0].run_id != episode_2.get("run_id")
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 recovery evidence changed")
    _verify_arxiv_mixed_raw_response_bindings(
        root, recovered["episode_2_raw_responses"]
    )

    expected_source_episodes = [
        {
            "episode_number": 1,
            "checkpoint_dataset": dict(episode_1["checkpoint_dataset"]),
            "attempt_manifest_hash": episode_1["source_attempt_manifest_hash"],
            "raw_response_manifest_hash": _hash_payload(
                {"responses": episode_1["raw_responses"]}
            ),
        },
        {
            "episode_number": 2,
            "checkpoint_dataset": dict(episode_2_reference),
            "attempt_manifest_hash": expected_attempt_manifest_hash,
            "raw_response_manifest_hash": _hash_payload(
                {"responses": validated["raw_response_bindings"]}
            ),
        },
    ]
    if recovered.get("source_episodes") != expected_source_episodes:
        raise ExternalRetrievalWaveError("arXiv recovery provenance binding changed")

    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    dataset = load_review_dataset(recovery_checkpoint)
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    recovery_metadata = dataset.retrieval_runs[0].metadata.get(
        "offline_arxiv_mixed_state_recovery", {}
    )
    if (
        len(dataset.retrieval_runs) != 1
        or dataset.retrieval_runs[0].completion_status
        is not RetrievalCompletionStatus.RUNNING
        or dataset.retrieval_runs[0].query_plan_hash != _query_plan_hash(specs)
        or recovery_metadata.get("source_episodes") != expected_source_episodes
        or recovery_metadata.get("restart_states") != validated["restart_states"]
        or recovery_metadata.get("network_used") is not False
        or dataset.retrieval_pages
        or dataset.retrieval_attempts
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or [query.query_id for query in dataset.source_queries]
        != [item["query_id"] for item in validated["restart_states"]]
        or any(
            query.completion_status is not RetrievalCompletionStatus.PLANNED
            or query.status is not ProcessingStatus.PARTIAL
            or query.result_count != 0
            or query.page_ids
            or query.errors
            or query.metadata.get("offline_arxiv_mixed_state_recovery", {}).get(
                "restart_state"
            )
            != {"start": 0}
            for query in dataset.source_queries
        )
    ):
        raise ExternalRetrievalWaveError("arXiv episode-3 restart checkpoint changed")
    dataset.validate()


def _validate_authorized_pubmed_retry(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    active_number = source_state.get("active_episode_number")
    if not episodes or active_number is None:
        raise ExternalRetrievalWaveError("authorized PubMed retry lacks episode lineage")
    active = next(
        (item for item in episodes if item["episode_number"] == active_number), None
    )
    if (
        active is None
        or active.get("status") != PUBMED_TRANSPORT_RETRY_STATUS
        or source_state.get("active_checkpoint_path") != active.get("checkpoint_path")
        or source_state.get("active_run_id") != active.get("run_id")
    ):
        raise ExternalRetrievalWaveError("authorized PubMed retry lineage changed")
    if _safe_output_path(root, active["checkpoint_path"]).exists():
        raise ExternalRetrievalWaveError(
            "authorized PubMed retry checkpoint already exists; use normal --resume"
        )
    previous = next(
        (
            item
            for item in episodes
            if item["episode_number"] == active["retry_of_episode_number"]
        ),
        None,
    )
    if previous is None or not previous.get("immutable"):
        raise ExternalRetrievalWaveError("prior PubMed failure episode is not preserved")
    checkpoint = _safe_output_path(root, previous["checkpoint_dataset"]["path"])
    _verify_file_reference(checkpoint, previous["checkpoint_dataset"], root)


def _sync_active_retry_episode(source_state: dict[str, Any]) -> None:
    active_number = source_state.get("active_episode_number")
    if active_number is None:
        return
    active = next(
        (
            item
            for item in source_state.get("execution_episodes", [])
            if item["episode_number"] == active_number
        ),
        None,
    )
    if active is None:
        raise ExternalRetrievalWaveError("active retry episode is missing")
    active.update(
        {
            "status": source_state["status"],
            "started_at_utc": source_state.get("last_session_started_at_utc"),
            "completed_at_utc": source_state.get("last_session_completed_at_utc"),
            "checkpoint_dataset": source_state.get("checkpoint_dataset"),
            "attempt_count": source_state.get("attempt_count", 0),
            "occurrence_count": source_state.get("occurrence_count", 0),
            "completed_query_count": source_state.get("completed_query_count", 0),
            "failure_reason": source_state.get("failure_reason"),
            "pause_reason": source_state.get("pause_reason"),
            "immutable": source_state["status"] in {"COMPLETE", "FAILED"},
        }
    )


def _verify_file_reference(
    path: Path, reference: Mapping[str, Any], root: Path
) -> None:
    if (
        not path.is_file()
        or path.relative_to(root).as_posix() != reference.get("path")
    ):
        raise ExternalRetrievalWaveError("checkpoint file binding changed")
    raw = path.read_bytes()
    if (
        len(raw) != reference.get("byte_size")
        or _sha256(raw) != reference.get("raw_sha256")
    ):
        raise ExternalRetrievalWaveError("checkpoint file hash/size changed")


def _finalize_execution_state(state: dict[str, Any], completed_at: str) -> None:
    if all(item["status"] == "COMPLETE" for item in state["sources"].values()):
        state["status"] = "COMPLETE"
        state["external_retrieval_completed_at_utc"] = completed_at
        state["external_retrieval_cutoff_date"] = completed_at[:10]
    else:
        state["status"] = "RUNNING"
        state["external_retrieval_completed_at_utc"] = None
        state["external_retrieval_cutoff_date"] = None


def _save_execution_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at_utc"] = max(
        str(state.get("updated_at_utc") or ""),
        str(
            max(
                (
                    item.get("last_session_completed_at_utc")
                    or item.get("last_session_started_at_utc")
                    or state["created_at_utc"]
                    for item in state["sources"].values()
                ),
                default=state["created_at_utc"],
            )
        ),
    )
    _save_hashed_json(path, state, "state_hash")


def _save_hashed_json(path: Path, payload: dict[str, Any], hash_key: str) -> None:
    material = dict(payload)
    material.pop(hash_key, None)
    payload[hash_key] = _hash_payload(material)
    atomic_write(path, _pretty_json(payload).encode("utf-8"))


def _validate_embedded_hash(payload: dict[str, Any], hash_key: str) -> None:
    material = dict(payload)
    claimed = material.pop(hash_key, None)
    if claimed != _hash_payload(material):
        raise ExternalRetrievalWaveError(f"{hash_key} mismatch")


def _checkpoint_attempt_count(checkpoint_dir: Path) -> int:
    path = checkpoint_dir / "review_dataset.json"
    return len(load_review_dataset(path).retrieval_attempts) if path.is_file() else 0


def _ieee_calls_on_day(root: Path, checkpoint_dir: Path, quota_day: str) -> int:
    verification = _load_json(root / IEEE_VERIFICATION_PATH)
    verification_calls = sum(
        attempt["requested_at_utc"][:10] == quota_day
        for request in verification["requests"]
        for attempt in request["attempts"]
    )
    checkpoint_path = checkpoint_dir / "review_dataset.json"
    retrieval_calls = 0
    if checkpoint_path.is_file():
        dataset = load_review_dataset(checkpoint_path)
        retrieval_calls = sum(
            attempt.started_at[:10] == quota_day
            for attempt in dataset.retrieval_attempts
        )
        recovery = dataset.retrieval_runs[0].metadata.get(
            "offline_repeated_window_recovery", {}
        )
        if recovery.get("quota_day_utc") == quota_day:
            retrieval_calls += int(
                recovery.get("quota_only_rejected_attempt_count", 0)
            )
    return verification_calls + retrieval_calls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--source", choices=list(EXTERNAL_IDENTIFICATION_SOURCES_V2))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--authorize-live-external-retrieval", action="store_true")
    parser.add_argument("--authorize-transport-retry-reset", action="store_true")
    parser.add_argument("--authorize-pubmed-parser-recovery", action="store_true")
    parser.add_argument(
        "--authorize-europe-pmc-terminal-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-ieee-total-drift-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-ieee-repeated-window-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-arxiv-rate-limit-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-arxiv-mixed-state-recovery", action="store_true"
    )
    args = parser.parse_args(argv)
    if args.authorize_arxiv_mixed_state_recovery:
        if args.source != "arXiv":
            parser.error(
                "mixed-state recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.resume
        ):
            parser.error(
                "arXiv mixed-state recovery is a separate offline authorization boundary"
            )
        state = authorize_arxiv_mixed_state_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][2]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "restart_states": active["restart_states"],
                    "preserved_attempt_count": active["source_attempt_count"],
                    "preserved_raw_response_count": active[
                        "source_raw_response_count"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_arxiv_rate_limit_recovery:
        if args.source != "arXiv":
            parser.error(
                "rate-limit recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "arXiv rate-limit recovery is a separate offline authorization boundary"
            )
        state = authorize_arxiv_rate_limit_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "restart_states": active["restart_states"],
                    "preserved_attempt_count": active["source_attempt_count"],
                    "preserved_raw_response_count": len(
                        active["source_raw_responses"]
                    ),
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_ieee_repeated_window_recovery:
        if args.source != "IEEEXplore":
            parser.error(
                "repeated-window recovery is supported only for --source IEEEXplore"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "IEEE repeated-window recovery is a separate offline authorization boundary"
            )
        state = authorize_ieee_repeated_window_recovery(root=args.root)
        source_state = state["sources"]["IEEEXplore"]
        active = source_state["execution_episodes"][2]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "IEEEXplore",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "continuation_plan": active["continuation_plan"],
                    "retained_page_count": active["retained_page_count"],
                    "rejected_page_count": len(active["rejection_evidence"]),
                    "known_daily_calls_preserved": active[
                        "known_daily_calls_preserved"
                    ],
                    "remaining_daily_calls": active["remaining_daily_calls"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_ieee_total_drift_recovery:
        if args.source != "IEEEXplore":
            parser.error(
                "provider-total recovery is supported only for --source IEEEXplore"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "IEEE provider-total recovery is a separate offline authorization boundary"
            )
        state = authorize_ieee_total_drift_recovery(root=args.root)
        source_state = state["sources"]["IEEEXplore"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "IEEEXplore",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "continuation_plan": active["continuation_plan"],
                    "known_daily_calls_preserved": active[
                        "known_daily_calls_preserved"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_europe_pmc_terminal_recovery:
        if args.source != "EuropePMC":
            parser.error(
                "terminal recovery is supported only for --source EuropePMC"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "Europe PMC terminal recovery is a separate offline authorization boundary"
            )
        state = authorize_europe_pmc_terminal_recovery(root=args.root)
        source_state = state["sources"]["EuropePMC"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "EuropePMC",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "query_occurrence_counts": active[
                        "query_occurrence_counts"
                    ],
                    "occurrence_count": source_state["occurrence_count"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_pubmed_parser_recovery:
        if args.source != "PubMed":
            parser.error("parser recovery is supported only for --source PubMed")
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error("parser recovery is a separate offline authorization boundary")
        state = authorize_pubmed_parser_recovery(root=args.root)
        source_state = state["sources"]["PubMed"]
        active = next(
            item
            for item in source_state["execution_episodes"]
            if item["episode_number"] == source_state["active_episode_number"]
        )
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "PubMed",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state["active_episode_number"],
                    "recovered_pmids": active["recovered_pmids"],
                    "already_fetched_occurrence_count": active[
                        "already_fetched_occurrence_count"
                    ],
                    "remaining_efetch_request_count": active[
                        "remaining_efetch_request_count"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_transport_retry_reset:
        if args.source != "PubMed":
            parser.error("transport retry reset is supported only for --source PubMed")
        if (
            args.authorize_live_external_retrieval
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "transport retry reset is a separate offline authorization boundary"
            )
        state = authorize_pubmed_transport_retry(root=args.root)
        source_state = state["sources"]["PubMed"]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "PubMed",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state["active_episode_number"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_live_external_retrieval:
        if not args.source:
            parser.error("--source is required for authorized execution")
        credential = (
            os.environ.get(IEEE_CREDENTIAL_NAME, "")
            if args.source == "IEEEXplore"
            else ""
        )
        state = execute_external_source_session(
            root=args.root,
            source=args.source,
            http=None
            if args.source == "ACMDigitalLibrary"
            else RequestsHttpClient(),
            resume=args.resume,
            ieee_credential=credential,
        )
        source_state = state["sources"][args.source]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": args.source,
                    "source_status": source_state["status"],
                    "completed_query_count": source_state[
                        "completed_query_count"
                    ],
                    "total_query_count": source_state["total_query_count"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "credential_value_persisted": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return (
            0
            if source_state["status"]
            in {
                "COMPLETE",
                "PAUSED_DAILY_QUOTA",
                "PAUSED_PROVIDER_QUOTA",
                "PAUSED_PROVIDER_RATE_LIMIT",
                "PAUSED_TRANSIENT_TRANSPORT",
            }
            else 2
        )
    result = save_external_retrieval_preflight(root=args.root)
    print(
        json.dumps(
            {
                "status": result["status"],
                "wave_id": result["wave_id"],
                "wave_path": result["wave_file"]["path"],
                "wave_sha256": result["wave_file"]["raw_sha256"],
                "preflight_path": result["preflight_file"]["path"],
                "preflight_sha256": result["preflight_file"]["raw_sha256"],
                "query_count": len(result["query_inventory"]),
                "estimated_http_requests": result["request_burden"][
                    "estimated_http_requests"
                ],
                "network_used": False,
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
