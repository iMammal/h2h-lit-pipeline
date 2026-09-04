"""Offline importer for human-generated ACM Digital Library BibTeX exports."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from h2h_lit.artifact_import import (
    ArtifactChunk,
    ArtifactImportPlan,
    ArtifactItem,
    build_artifact_review_dataset,
    stable_id,
)
from h2h_lit.bibtex_io import (
    parse_bibtex_with_diagnostics,
    parse_entry_fields,
    record_from_bibtex_fields,
    split_bib_entries,
)
from h2h_lit.models import LiteratureRecord, ProcessingStatus, ProvenanceEvent, ProvenanceKind
from h2h_lit.pagination import malformed_identifier, redact_url
from h2h_lit.review import IdentificationRoute, ReviewDataset

SOURCE_DATABASE = "ACMDigitalLibrary"


def import_acm_selected_reconciliation(
    manifest_path: str | Path,
    *,
    root: str | Path,
    query_text_by_parent: dict[str, str],
    query_version: str,
    run_id_prefix: str,
    protocol_version: str = "1.0.0",
    rubric_version: str = "1.0.0",
    software_version: str | None = None,
) -> dict[str, ReviewDataset]:
    """Import only final-selected ACM artifacts, retaining field-level provenance."""

    from h2h_lit.acm_field_execution import (  # local import avoids module cycle
        load_acm_final_reconciliation_manifest,
    )

    root_path = Path(root).resolve()
    path = Path(manifest_path).resolve()
    manifest = load_acm_final_reconciliation_manifest(
        path, root=root_path, verify_artifacts=True
    )
    datasets: dict[str, ReviewDataset] = {}
    for family in manifest["families"]:
        parent_query_id = family["parent_query_id"]
        query_text = query_text_by_parent.get(parent_query_id)
        if query_text is None:
            raise ValueError(f"missing frozen ACM query text for {parent_query_id}")
        chunks: list[ArtifactChunk] = []
        next_record = 1
        malformed_total = 0
        for child in family["children"]:
            for artifact in child["selected_artifacts"]:
                relative = _relative_artifact_path(artifact["relative_path"])
                artifact_path = root_path / relative
                raw = artifact_path.read_bytes()
                raw_hash = hashlib.sha256(raw).hexdigest()
                if (
                    len(raw) != artifact["byte_size"]
                    or raw_hash != artifact["raw_sha256"]
                ):
                    raise ValueError(f"ACM selected artifact binding changed: {relative}")
                text = raw.decode("utf-8")
                diagnostic = parse_bibtex_with_diagnostics(text)
                if (
                    diagnostic.physical_header_count
                    != artifact["physical_header_count"]
                    or diagnostic.accounted_record_count
                    != artifact["total_accounted_entry_count"]
                    or len(diagnostic.issues) != artifact["malformed_entry_count"]
                ):
                    raise ValueError(f"ACM parser accounting changed: {relative}")
                items: list[ArtifactItem] = []
                artifact_malformed = 0
                for rank, raw_entry in enumerate(split_bib_entries(text), start=1):
                    item, item_error = _entry_item(
                        raw_entry,
                        rank=rank,
                        query=query_text,
                        imported_at=manifest["reconciled_at_utc"],
                    )
                    if item_error:
                        artifact_malformed += 1
                        item.metadata.update(
                            {
                                "malformed_but_identified": True,
                                "parse_warning": item_error,
                            }
                        )
                    item.metadata.update(
                        {
                            "family_id": family["family_id"],
                            "parent_query_id": parent_query_id,
                            "child_query_id": child["child_query_id"],
                            "field_key": child["field_key"],
                            "final_reconciliation_manifest_hash": manifest[
                                "manifest_hash"
                            ],
                        }
                    )
                    items.append(item)
                if artifact_malformed != artifact["malformed_entry_count"]:
                    raise ValueError(f"ACM malformed accounting changed: {relative}")
                malformed_total += artifact_malformed
                first_record = next_record
                last_record = first_record + len(items) - 1
                chunks.append(
                    ArtifactChunk(
                        chunk_id=(
                            f"{child['field_key']}:"
                            f"{hashlib.sha256(relative.encode()).hexdigest()[:16]}"
                        ),
                        ordinal=len(chunks),
                        first_record=first_record,
                        last_record=last_record,
                        relative_path=relative,
                        artifact_hash=raw_hash,
                        items=items,
                        metadata={
                            "child_query_id": child["child_query_id"],
                            "field_key": child["field_key"],
                            "malformed_but_identified_count": artifact_malformed,
                            "declared_artifact_sha256": artifact["raw_sha256"],
                        },
                    )
                )
                next_record = last_record + 1
        raw_occurrence_count = next_record - 1
        dataset = build_artifact_review_dataset(
            ArtifactImportPlan(
                run_id=f"{run_id_prefix}:{family['family_id']}",
                query_id=parent_query_id,
                source_database=SOURCE_DATABASE,
                query_text=query_text,
                query_version=query_version,
                manifest_hash=manifest["manifest_hash"],
                started_at=min(
                    child["execution_time_provider_observation"]["observed_at_utc"]
                    for child in family["children"]
                ),
                completed_at=manifest["reconciled_at_utc"],
                reported_total=raw_occurrence_count,
                operator_id="UNRECORDED_OPERATOR_ID",
                chunks=chunks,
                identification_route=IdentificationRoute.DATABASE,
                fields=["Title", "Author Keywords", "Abstract"],
                filters={},
                metadata={
                    "workflow": "field_decomposed_selected_artifact_import",
                    "collection_scope": "acm_publications",
                    "sort": "publicationDate asc",
                    "export_format": "BibTeX",
                    "family_id": family["family_id"],
                    "field_union_unique_stable_identity_count": family[
                        "field_union"
                    ]["unique_stable_identity_count"],
                    "field_union_identity_digest_sha256": family["field_union"][
                        "stable_identity_union_digest_sha256"
                    ],
                    "malformed_but_identified_count": malformed_total,
                    "calibration_superseded_and_error_attempts_imported": False,
                    "final_reconciliation_manifest_path": path.relative_to(
                        root_path
                    ).as_posix(),
                },
            ),
            protocol_version=protocol_version,
            rubric_version=rubric_version,
            software_version=software_version,
        )
        if len(dataset.occurrences) != raw_occurrence_count:
            raise ValueError("ACM selected occurrence accounting diverged")
        if sum(
            bool(item.metadata.get("malformed_but_identified"))
            for item in dataset.occurrences
        ) != malformed_total:
            raise ValueError("ACM malformed identity accounting diverged")
        datasets[family["family_id"]] = dataset
    if len(datasets) != 5:
        raise ValueError("ACM selected import must cover exactly five families")
    return datasets


def import_acm_bibtex_manifest(
    manifest_path: str | Path,
    *,
    protocol_version: str = "1.0.0",
    rubric_version: str = "1.0.0",
    software_version: str | None = None,
) -> ReviewDataset:
    path = Path(manifest_path)
    raw_manifest = path.read_bytes()
    manifest_hash = hashlib.sha256(raw_manifest).hexdigest()
    manifest = json.loads(raw_manifest)
    _validate_manifest(manifest)
    chunks: list[ArtifactChunk] = []
    for ordinal, declared in enumerate(manifest["chunks"]):
        relative = _relative_artifact_path(declared["artifact_path"])
        artifact_path = path.parent / relative
        artifact_hash: str | None = None
        items: list[ArtifactItem] = []
        error: str | None = None
        try:
            raw = artifact_path.read_bytes()
            artifact_hash = hashlib.sha256(raw).hexdigest()
            if artifact_hash != declared["sha256"]:
                error = "artifact SHA-256 does not match the manifest"
            text = raw.decode("utf-8")
            raw_entries = split_bib_entries(text)
            for rank, raw_entry in enumerate(raw_entries, start=1):
                item, item_error = _entry_item(
                    raw_entry,
                    rank=rank,
                    query=manifest["query_text"],
                    imported_at=manifest["imported_at"],
                )
                items.append(item)
                error = error or item_error
            if not raw_entries and raw.strip():
                error = error or "artifact contains no parseable BibTeX entries"
        except (OSError, UnicodeDecodeError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        chunks.append(
            ArtifactChunk(
                chunk_id=str(declared["chunk_id"]),
                ordinal=ordinal,
                first_record=int(declared["first_record"]),
                last_record=int(declared["last_record"]),
                relative_path=relative,
                artifact_hash=artifact_hash,
                items=items,
                error=error,
                metadata={
                    "exported_at": declared["exported_at"],
                    "declared_sha256": declared["sha256"],
                    "validation_status": "valid" if error is None else "invalid",
                },
            )
        )
    evidence = manifest.get("operator_evidence", {})
    query_url = evidence.get("query_url")
    evidence_artifacts: list[dict[str, Any]] = []
    evidence_errors: list[str] = []
    for item in evidence.get("artifacts", []):
        relative = _relative_artifact_path(item["path"])
        preserved = {**item, "path": relative}
        try:
            actual_hash = hashlib.sha256((path.parent / relative).read_bytes()).hexdigest()
            preserved["actual_sha256"] = actual_hash
            if actual_hash != item.get("sha256"):
                evidence_errors.append(f"operator evidence {relative} SHA-256 mismatch")
        except OSError as exc:
            evidence_errors.append(f"operator evidence {relative}: {type(exc).__name__}: {exc}")
        evidence_artifacts.append(preserved)
    plan = ArtifactImportPlan(
        run_id=str(manifest["run_id"]),
        query_id=str(manifest["query_id"]),
        source_database=SOURCE_DATABASE,
        query_text=str(manifest["query_text"]),
        query_version=str(manifest["query_version"]),
        manifest_hash=manifest_hash,
        started_at=str(manifest["search_executed_at"]),
        completed_at=str(manifest["imported_at"]),
        reported_total=int(manifest["ui_reported_total"]),
        operator_id=str(manifest["operator_id"]),
        chunks=chunks,
        identification_route=IdentificationRoute.DATABASE,
        fields=list(manifest["field_selections"]),
        filters=dict(manifest["filters"]),
        metadata={
            "workflow": "advanced_search_citation_export",
            "collection_scope": manifest["collection_scope"],
            "sort": manifest["sort"],
            "export_format": manifest["export_format"],
            "access_tier": manifest.get("access_tier"),
            "query_url": redact_url(query_url) if query_url else None,
            "operator_evidence_artifacts": evidence_artifacts,
        },
        errors=evidence_errors,
    )
    return build_artifact_review_dataset(
        plan,
        protocol_version=protocol_version,
        rubric_version=rubric_version,
        software_version=software_version,
    )


def _validate_manifest(manifest: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "run_id",
        "query_id",
        "query_text",
        "query_version",
        "field_selections",
        "collection_scope",
        "filters",
        "search_executed_at",
        "imported_at",
        "ui_reported_total",
        "sort",
        "export_format",
        "operator_id",
        "chunks",
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"ACM manifest missing required fields: {missing}")
    if manifest["schema_version"] != "1.0.0":
        raise ValueError("unsupported ACM manifest schema version")
    if manifest["collection_scope"] not in {"acm_publications", "acm_guide"}:
        raise ValueError("ACM collection_scope must be acm_publications or acm_guide")
    if str(manifest["export_format"]).lower() != "bibtex":
        raise ValueError("Phase 3 ACM imports support BibTeX only")
    if not str(manifest["query_text"]).strip() or not manifest["field_selections"]:
        raise ValueError("ACM query text and field selections are required")
    if not str(manifest["operator_id"]).strip() or not str(manifest["sort"]).strip():
        raise ValueError("ACM operator and sort provenance are required")
    if int(manifest["ui_reported_total"]) < 0:
        raise ValueError("ACM UI-reported total cannot be negative")
    chunks = manifest["chunks"]
    if not isinstance(chunks, list) or (not chunks and manifest["ui_reported_total"] != 0):
        raise ValueError("ACM non-empty searches require declared export chunks")
    for chunk in chunks:
        chunk_required = {
            "chunk_id",
            "first_record",
            "last_record",
            "artifact_path",
            "sha256",
            "exported_at",
        }
        if chunk_required - chunk.keys():
            raise ValueError("ACM export chunk has insufficient provenance")
        if int(chunk["first_record"]) < 1 or int(chunk["last_record"]) < int(
            chunk["first_record"]
        ):
            raise ValueError("ACM export chunk range is invalid")


def _entry_item(
    raw: str, *, rank: int, query: str, imported_at: str
) -> tuple[ArtifactItem, str | None]:
    fields = parse_entry_fields(raw)
    balanced = raw.rstrip().endswith("}") and raw.count("{") == raw.count("}")
    if fields and balanced:
        record = record_from_bibtex_fields(fields)
        record.source_database = SOURCE_DATABASE
        record.source_identifier = fields.get("_key")
        record.original_metadata = {**fields, "raw_bibtex": raw}
        for event in record.provenance:
            event.timestamp = imported_at
        record.provenance.append(
            ProvenanceEvent(
                kind=ProvenanceKind.SOURCE_DERIVED,
                stage="acm_dl_bibtex_import",
                source_database=SOURCE_DATABASE,
                source_query=query,
                source_identifier=fields.get("_key"),
                timestamp=imported_at,
            )
        )
        identifier = fields.get("_key") or malformed_identifier(raw, rank)
        return ArtifactItem(identifier, record, raw), None
    identifier = fields.get("_key") if fields else None
    identifier = identifier or malformed_identifier(raw, rank)
    record = LiteratureRecord(
        title=fields.get("title", "") if fields else "",
        source_identifier=identifier,
        source_database=SOURCE_DATABASE,
        original_metadata={
            "raw_bibtex": raw,
            "parser_incomplete": True,
            "parser_error": "malformed or unbalanced BibTeX entry",
            "partial_fields": fields,
        },
        provenance=[
            ProvenanceEvent(
                kind=ProvenanceKind.SOURCE_DERIVED,
                stage="acm_dl_bibtex_import",
                status=ProcessingStatus.PARTIAL,
                source_database=SOURCE_DATABASE,
                source_query=query,
                source_identifier=identifier,
                errors=["malformed or unbalanced BibTeX entry"],
                timestamp=imported_at,
            )
        ],
    )
    return ArtifactItem(identifier, record, raw, {"parser_incomplete": True}), (
        "artifact contains malformed BibTeX entries"
    )


def _relative_artifact_path(value: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("artifact paths must be relative and may not traverse parents")
    return path.as_posix()


def opaque_artifact_id(artifact_hash: str) -> str:
    return stable_id("acm-artifact", artifact_hash)
