"""Deterministic offline planning for the Phase 4A external-retrieval wave."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from h2h_lit.acm_field_execution import load_acm_final_reconciliation_manifest
from h2h_lit.checkpoint import atomic_write
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args(argv)
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
