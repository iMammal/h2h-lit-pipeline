"""Versioned prior-survey seed manifest importer."""

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
from h2h_lit.models import LiteratureRecord, ProvenanceEvent, ProvenanceKind
from h2h_lit.normalize import normalize_doi
from h2h_lit.review import IdentificationRoute, ReviewDataset

SOURCE_DATABASE = "PriorSurveySeed"


def import_seed_manifest(
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
    seed_set_id = str(manifest["seed_set_id"])
    items = [
        _entry_item(
            entry,
            seed_set_id=seed_set_id,
            manifest_hash=manifest_hash,
            extraction_method=str(manifest["extraction_method"]),
            curator_id=str(manifest["curator_id"]),
            imported_at=str(manifest["imported_at"]),
        )
        for entry in manifest["entries"]
    ]
    chunk = ArtifactChunk(
        chunk_id=f"{seed_set_id}:{manifest['seed_set_version']}",
        ordinal=0,
        first_record=1,
        last_record=len(items),
        relative_path=path.name,
        artifact_hash=manifest_hash,
        items=items,
        metadata={
            "seed_set_id": seed_set_id,
            "seed_set_version": manifest["seed_set_version"],
            "manifest_hash": manifest_hash,
        },
    )
    origin = dict(manifest["originating_review"])
    plan = ArtifactImportPlan(
        run_id=str(manifest["run_id"]),
        query_id=str(manifest["query_id"]),
        source_database=SOURCE_DATABASE,
        query_text=f"prior survey seed set {seed_set_id}",
        query_version=str(manifest["seed_set_version"]),
        manifest_hash=manifest_hash,
        started_at=str(manifest["created_at"]),
        completed_at=str(manifest["imported_at"]),
        reported_total=int(manifest["expected_entry_count"]),
        operator_id=str(manifest["curator_id"]),
        chunks=[chunk],
        identification_route=IdentificationRoute.PRIOR_SURVEY_SEED,
        fields=["raw_citation", "title", "doi", "locator"],
        metadata={
            "source_role": IdentificationRoute.PRIOR_SURVEY_SEED.value,
            "seed_set_id": seed_set_id,
            "seed_set_version": manifest["seed_set_version"],
            "originating_review": origin,
            "extraction_method": manifest["extraction_method"],
            "export_format": "seed_manifest_json",
            "artifact_hash": manifest_hash,
        },
    )
    return build_artifact_review_dataset(
        plan,
        protocol_version=protocol_version,
        rubric_version=rubric_version,
        software_version=software_version,
    )


def _entry_item(
    entry: dict[str, Any],
    *,
    seed_set_id: str,
    manifest_hash: str,
    extraction_method: str,
    curator_id: str,
    imported_at: str,
) -> ArtifactItem:
    entry_id = str(entry["entry_id"])
    source_identifier = f"{seed_set_id}:{entry_id}"
    doi = normalize_doi(entry.get("doi"))
    metadata = {
        **entry,
        "seed_set_id": seed_set_id,
        "manifest_hash": manifest_hash,
        "source_role": IdentificationRoute.PRIOR_SURVEY_SEED.value,
        "extraction_method": extraction_method,
        "curator_id": curator_id,
    }
    record = LiteratureRecord(
        title=str(entry.get("title") or ""),
        abstract=str(entry.get("abstract") or ""),
        authors=list(entry.get("authors") or []),
        year=entry.get("year"),
        doi=doi,
        source_identifier=source_identifier,
        source_database=SOURCE_DATABASE,
        journal=entry.get("venue"),
        original_metadata=metadata,
        provenance=[
            ProvenanceEvent(
                kind=ProvenanceKind.HUMAN_DECISION,
                stage="prior_survey_seed_manifest_import",
                source_database=SOURCE_DATABASE,
                source_identifier=source_identifier,
                input_id=manifest_hash,
                metadata={
                    "seed_set_id": seed_set_id,
                    "entry_id": entry_id,
                    "ordinal": entry["ordinal"],
                    "locator": entry.get("locator"),
                    "extraction_method": extraction_method,
                    "curator_id": curator_id,
                },
                timestamp=imported_at,
            )
        ],
    )
    return ArtifactItem(
        source_identifier=source_identifier,
        record=record,
        raw_payload=entry,
        metadata={
            "seed_set_id": seed_set_id,
            "seed_entry_id": entry_id,
            "seed_ordinal": entry["ordinal"],
        },
    )


def _validate_manifest(manifest: dict[str, Any]) -> None:
    required = {
        "schema_version",
        "run_id",
        "query_id",
        "seed_set_id",
        "seed_set_version",
        "originating_review",
        "extraction_method",
        "curator_id",
        "created_at",
        "imported_at",
        "expected_entry_count",
        "entries",
    }
    missing = sorted(required - manifest.keys())
    if missing:
        raise ValueError(f"seed manifest missing required fields: {missing}")
    if manifest["schema_version"] != "1.0.0":
        raise ValueError("unsupported seed manifest schema version")
    if str(manifest["seed_set_id"]).upper() in {"EBK25", "JFR25", "FP19"} and not manifest[
        "entries"
    ]:
        raise ValueError("historical seed sets require explicit prospective entries")
    origin = manifest["originating_review"]
    if not isinstance(origin, dict) or not str(origin.get("citation") or "").strip():
        raise ValueError("seed manifest requires the originating review citation")
    if not str(manifest["extraction_method"]).strip() or not str(
        manifest["curator_id"]
    ).strip():
        raise ValueError("seed extraction method and curator are required")
    entries = manifest["entries"]
    if not entries:
        raise ValueError("seed manifests require explicit prospective entries")
    if int(manifest["expected_entry_count"]) != len(entries):
        raise ValueError("seed manifest entry count does not match expected_entry_count")
    ids = [str(entry.get("entry_id") or "") for entry in entries]
    ordinals = [entry.get("ordinal") for entry in entries]
    if not all(ids) or len(ids) != len(set(ids)):
        raise ValueError("seed entry IDs must be present and unique")
    if ordinals != list(range(1, len(entries) + 1)):
        raise ValueError("seed entry ordinals must be ordered and contiguous from one")
    for entry in entries:
        if not str(entry.get("raw_citation") or "").strip():
            raise ValueError("seed entries require raw citation text")
        if not (str(entry.get("title") or "").strip() or normalize_doi(entry.get("doi"))):
            raise ValueError("seed entries require title or DOI metadata")


def seed_query_id(seed_set_id: str, seed_set_version: str) -> str:
    return stable_id("seed-query", seed_set_id, seed_set_version)
