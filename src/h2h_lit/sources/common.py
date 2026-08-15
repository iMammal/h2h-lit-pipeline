"""Shared helpers for literature source adapters."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from html import unescape
from typing import Any

from h2h_lit.models import LiteratureRecord, ProcessingStatus, ProvenanceEvent, ProvenanceKind
from h2h_lit.normalize import normalize_doi


def retrieval_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def strip_markup(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(value))
    return re.sub(r"\s+", " ", unescape(text)).strip()


def source_event(
    *,
    stage: str,
    source_database: str,
    source_query: str,
    source_identifier: str | None = None,
    source_url: str | None = None,
    status: ProcessingStatus = ProcessingStatus.OK,
    errors: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> ProvenanceEvent:
    return ProvenanceEvent(
        kind=ProvenanceKind.SOURCE_DERIVED,
        stage=stage,
        status=status,
        source_database=source_database,
        source_query=source_query,
        source_identifier=source_identifier,
        source_url=source_url,
        errors=errors or [],
        metadata=metadata or {},
    )


def make_record(
    *,
    title: str,
    abstract: str = "",
    authors: list[str] | None = None,
    year: str | int | None = None,
    doi: str | None = None,
    pmid: str | None = None,
    arxiv_id: str | None = None,
    source_identifier: str | None = None,
    source_database: str,
    source_url: str | None = None,
    pdf_url: str | None = None,
    journal: str | None = None,
    is_open_access: bool | None = None,
    original_metadata: dict[str, Any] | None = None,
    source_query: str,
    stage: str,
) -> LiteratureRecord:
    record = LiteratureRecord(
        title=strip_markup(title),
        abstract=strip_markup(abstract),
        authors=authors or [],
        year=year,
        doi=normalize_doi(doi),
        pmid=pmid,
        arxiv_id=arxiv_id,
        source_identifier=source_identifier,
        source_database=source_database,
        source_url=source_url,
        pdf_url=pdf_url,
        journal=strip_markup(journal),
        is_open_access=is_open_access,
        original_metadata=original_metadata or {},
    )
    record.add_event(
        source_event(
            stage=stage,
            source_database=source_database,
            source_query=source_query,
            source_identifier=source_identifier,
            source_url=source_url,
        )
    )
    return record


def first_year(parts: dict[str, Any]) -> int | None:
    for key in ("published-print", "published-online", "published", "created", "issued"):
        date_parts = (parts.get(key) or {}).get("date-parts") if isinstance(parts.get(key), dict) else None
        if date_parts and date_parts[0]:
            return date_parts[0][0]
    return None

