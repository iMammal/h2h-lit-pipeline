"""Shared non-HTTP retrieval provenance for curated artifact imports."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    IdentificationRoute,
    RecordOccurrence,
    RetrievalAttempt,
    RetrievalAttemptStatus,
    RetrievalCompletionStatus,
    RetrievalPage,
    RetrievalRun,
    RetrievalRunKind,
    RetrievalTransportKind,
    ReviewDataset,
    SourceQuery,
    canonicalize_occurrences,
)


@dataclass(slots=True)
class ArtifactItem:
    source_identifier: str
    record: LiteratureRecord
    raw_payload: Any
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ArtifactChunk:
    chunk_id: str
    ordinal: int
    first_record: int
    last_record: int
    relative_path: str
    artifact_hash: str | None
    items: list[ArtifactItem]
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ArtifactImportPlan:
    run_id: str
    query_id: str
    source_database: str
    query_text: str
    query_version: str
    manifest_hash: str
    started_at: str
    completed_at: str
    reported_total: int
    operator_id: str
    chunks: list[ArtifactChunk]
    identification_route: IdentificationRoute = IdentificationRoute.DATABASE
    fields: list[str] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    content_policy: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def build_artifact_review_dataset(
    plan: ArtifactImportPlan,
    *,
    protocol_version: str = "1.0.0",
    rubric_version: str = "1.0.0",
    software_version: str | None = None,
) -> ReviewDataset:
    """Represent an import using the normal run/query/page/attempt lineage."""

    pages: list[RetrievalPage] = []
    attempts: list[RetrievalAttempt] = []
    occurrences: list[RecordOccurrence] = []
    errors = list(plan.errors)
    expected_start = 1
    ordered_chunks = sorted(plan.chunks, key=lambda item: item.ordinal)
    for chunk in ordered_chunks:
        if chunk.ordinal != len(pages):
            errors.append("artifact chunk ordinals must be contiguous from zero")
        if chunk.first_record != expected_start:
            errors.append(
                f"artifact range is missing or overlapping at record {expected_start}"
            )
        declared_count = max(0, chunk.last_record - chunk.first_record + 1)
        if declared_count != len(chunk.items):
            errors.append(
                f"artifact {chunk.chunk_id} declared {declared_count} records but imported "
                f"{len(chunk.items)}"
            )
        if chunk.error:
            errors.append(f"artifact {chunk.chunk_id}: {chunk.error}")
        expected_start = chunk.last_record + 1
        page_id = stable_id("artifact-page", plan.query_id, chunk.chunk_id)
        attempt_id = stable_id("artifact-attempt", page_id, "1")
        succeeded = chunk.error is None and chunk.artifact_hash is not None
        page_status = (
            RetrievalCompletionStatus.COMPLETE
            if succeeded
            else RetrievalCompletionStatus.FAILED
        )
        occurrence_ids: list[str] = []
        for local_rank, item in enumerate(chunk.items, start=1):
            source_rank = chunk.first_record + local_rank - 1
            occurrence_id = stable_id(
                "occurrence", page_id, str(local_rank), item.source_identifier
            )
            occurrences.append(
                RecordOccurrence(
                    occurrence_id=occurrence_id,
                    source_query_id=plan.query_id,
                    source_identifier=item.source_identifier,
                    retrieved_at=plan.completed_at,
                    record=item.record,
                    source_rank=source_rank,
                    page=chunk.ordinal,
                    cursor=f"{chunk.first_record}-{chunk.last_record}",
                    raw_payload_hash=payload_hash(item.raw_payload),
                    metadata={**item.metadata, "artifact_chunk_id": chunk.chunk_id},
                    retrieval_page_id=page_id,
                )
            )
            occurrence_ids.append(occurrence_id)
        pages.append(
            RetrievalPage(
                page_id=page_id,
                source_query_id=plan.query_id,
                ordinal=chunk.ordinal,
                strategy="artifact_range",
                adapter_version="1.0.0",
                request_state={
                    "first_record": chunk.first_record,
                    "last_record": chunk.last_record,
                },
                status=page_status,
                attempt_ids=[attempt_id],
                next_state=(
                    {
                        "first_record": ordered_chunks[chunk.ordinal + 1].first_record,
                        "last_record": ordered_chunks[chunk.ordinal + 1].last_record,
                    }
                    if chunk.ordinal + 1 < len(ordered_chunks)
                    else None
                ),
                returned_item_count=len(chunk.items),
                occurrence_ids=occurrence_ids,
                source_reported_total=plan.reported_total,
                total_is_exact=True,
                terminal=chunk.ordinal == len(ordered_chunks) - 1,
                completion_proof=(
                    "artifact_ranges_and_total_reconciled"
                    if chunk.ordinal == len(ordered_chunks) - 1 and not errors
                    else None
                ),
                native_identifiers=[item.source_identifier for item in chunk.items],
                metadata=dict(chunk.metadata),
            )
        )
        request_payload = {
            "transport_kind": RetrievalTransportKind.ARTIFACT_IMPORT.value,
            "artifact_hash": chunk.artifact_hash,
            "first_record": chunk.first_record,
            "last_record": chunk.last_record,
            "format": plan.metadata.get("export_format"),
        }
        attempts.append(
            RetrievalAttempt(
                attempt_id=attempt_id,
                page_id=page_id,
                attempt_number=1,
                started_at=plan.completed_at,
                ended_at=plan.completed_at,
                status=(
                    RetrievalAttemptStatus.SUCCEEDED
                    if succeeded
                    else RetrievalAttemptStatus.FAILED
                ),
                request_method="IMPORT",
                request_url=f"artifact:{chunk.chunk_id}",
                request_params=request_payload,
                request_headers={},
                request_hash=payload_hash(request_payload),
                error=chunk.error if not succeeded else None,
                metadata={"validation_status": "valid" if succeeded else "invalid"},
                transport_kind=RetrievalTransportKind.ARTIFACT_IMPORT,
                artifact_path=chunk.relative_path,
                artifact_hash=chunk.artifact_hash,
                operator_id=plan.operator_id,
            )
        )

    if expected_start - 1 != plan.reported_total:
        errors.append(
            f"imported range ends at {expected_start - 1}, not reported total "
            f"{plan.reported_total}"
        )
    if len(occurrences) != plan.reported_total:
        errors.append(
            f"imported {len(occurrences)} occurrences, not reported total {plan.reported_total}"
        )
    errors = list(dict.fromkeys(errors))
    complete = not errors
    query = SourceQuery(
        query_id=plan.query_id,
        source_database=plan.source_database,
        query_text=plan.query_text,
        retrieval_started_at=plan.started_at,
        retrieval_ended_at=plan.completed_at,
        status=ProcessingStatus.OK if complete else ProcessingStatus.FAILED,
        run_id=plan.run_id,
        query_version=plan.query_version,
        result_count=len(occurrences),
        fields=list(plan.fields),
        filters=dict(plan.filters),
        software_version=software_version,
        errors=errors,
        metadata={**plan.metadata, "manifest_hash": plan.manifest_hash},
        completion_status=(
            RetrievalCompletionStatus.COMPLETE
            if complete
            else RetrievalCompletionStatus.FAILED
        ),
        page_ids=[page.page_id for page in pages],
        source_reported_total=plan.reported_total,
        total_is_exact=True,
        completion_proof="artifact_import_reconciled" if complete else None,
        identification_route=plan.identification_route,
        content_policy=dict(plan.content_policy),
    )
    run = RetrievalRun(
        run_id=plan.run_id,
        kind=RetrievalRunKind.PRIMARY,
        query_plan_version=plan.query_version,
        query_plan_hash=plan.manifest_hash,
        planned_query_ids=[plan.query_id],
        source_query_ids=[plan.query_id],
        retrieval_started_at=plan.started_at,
        retrieval_completed_at=plan.completed_at,
        status=ProcessingStatus.OK if complete else ProcessingStatus.FAILED,
        protocol_version=protocol_version,
        retrieval_cutoff_date=utc_date(plan.completed_at) if complete else None,
        software_version=software_version,
        errors=errors,
        metadata={"rubric_version": rubric_version, "transport_kind": "artifact_import"},
        completion_status=(
            RetrievalCompletionStatus.COMPLETE
            if complete
            else RetrievalCompletionStatus.FAILED
        ),
    )
    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.artifact_import",
            actor_type=ActorType.SOFTWARE,
            metadata={"software_version": software_version},
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version=protocol_version,
        rubric_version=rubric_version,
        created_at=plan.completed_at,
        metadata={"run_id": plan.run_id, "rule": "doi_first_title_fallback"},
    )
    canonical, decisions = canonicalize_occurrences(occurrences, provenance=provenance)
    dataset = ReviewDataset(
        schema_version="1.3.0",
        retrieval_runs=[run],
        source_queries=[query],
        retrieval_pages=pages,
        retrieval_attempts=attempts,
        occurrences=occurrences,
        canonical_records=canonical,
        duplicate_decisions=decisions,
    )
    dataset.validate()
    return dataset


def merge_identification_datasets(
    datasets: list[ReviewDataset],
    *,
    protocol_version: str = "1.0.0",
    rubric_version: str = "1.0.0",
    software_version: str | None = None,
) -> ReviewDataset:
    """Merge completed identification components and rerun ordinary canonicalization."""

    if not datasets:
        raise ValueError("at least one identification dataset is required")
    for dataset in datasets:
        dataset.validate()
    merged = ReviewDataset(
        schema_version="1.3.0",
        retrieval_runs=[item for dataset in datasets for item in dataset.retrieval_runs],
        source_queries=[item for dataset in datasets for item in dataset.source_queries],
        retrieval_pages=[item for dataset in datasets for item in dataset.retrieval_pages],
        retrieval_attempts=[item for dataset in datasets for item in dataset.retrieval_attempts],
        occurrences=[item for dataset in datasets for item in dataset.occurrences],
    )
    created_at = max(run.retrieval_completed_at for run in merged.retrieval_runs)
    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.artifact_import.merge",
            actor_type=ActorType.SOFTWARE,
            metadata={"software_version": software_version},
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version=protocol_version,
        rubric_version=rubric_version,
        created_at=created_at,
        metadata={
            "run_ids": [run.run_id for run in merged.retrieval_runs],
            "rule": "doi_first_title_fallback",
        },
    )
    merged.canonical_records, merged.duplicate_decisions = canonicalize_occurrences(
        merged.occurrences, provenance=provenance
    )
    merged.validate()
    return merged


def payload_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def stable_id(*parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{parts[0]}-{digest}"


def utc_date(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("artifact timestamps must include a UTC offset")
    return parsed.astimezone(UTC).date().isoformat()
