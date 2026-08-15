"""Deterministic record deduplication."""

from __future__ import annotations

from collections.abc import Iterable

from h2h_lit.models import LiteratureRecord, ProvenanceEvent, ProvenanceKind, ProcessingStatus
from h2h_lit.normalize import dedupe_key


def record_key(record: LiteratureRecord | dict[str, object]) -> str:
    if isinstance(record, LiteratureRecord):
        return dedupe_key(doi=record.doi, title=record.title)
    return dedupe_key(
        doi=str(record.get("doi") or "") or None,
        title=str(record.get("title") or "") or None,
    )


def deduplicate_records(records: Iterable[LiteratureRecord]) -> list[LiteratureRecord]:
    """Preserve first occurrence by DOI, falling back to normalized title."""

    seen: set[str] = set()
    unique: list[LiteratureRecord] = []
    for record in records:
        key = record_key(record)
        if not key:
            record.add_event(
                ProvenanceEvent(
                    kind=ProvenanceKind.DETERMINISTIC,
                    stage="dedupe",
                    status=ProcessingStatus.SKIPPED,
                    errors=["missing DOI and title"],
                )
            )
            continue
        if key in seen:
            record.add_event(
                ProvenanceEvent(
                    kind=ProvenanceKind.DETERMINISTIC,
                    stage="dedupe",
                    status=ProcessingStatus.SKIPPED,
                    metadata={"dedupe_key": key, "reason": "duplicate"},
                )
            )
            continue
        seen.add(key)
        record.add_event(
            ProvenanceEvent(
                kind=ProvenanceKind.DETERMINISTIC,
                stage="dedupe",
                metadata={"dedupe_key": key},
            )
        )
        unique.append(record)
    return unique

