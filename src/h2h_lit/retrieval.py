"""Offline-capable retrieval orchestration for persisted review datasets."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.http import HttpClient, HttpResponse
from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    RecordOccurrence,
    RetrievalRun,
    RetrievalRunKind,
    ReviewDataset,
    SourceQuery,
    canonicalize_occurrences,
)
from h2h_lit.sources.arxiv import API_URL as ARXIV_API_URL
from h2h_lit.sources.arxiv import search_arxiv
from h2h_lit.sources.common import retrieval_timestamp
from h2h_lit.sources.crossref import SEARCH_URL as CROSSREF_SEARCH_URL
from h2h_lit.sources.crossref import search_crossref
from h2h_lit.sources.europe_pmc import SEARCH_URL as EUROPE_PMC_SEARCH_URL
from h2h_lit.sources.europe_pmc import search_europe_pmc
from h2h_lit.sources.pubmed import EUTILS as PUBMED_EUTILS
from h2h_lit.sources.pubmed import search_pubmed
from h2h_lit.sources.semantic_scholar import SEARCH_URL as SEMANTIC_SCHOLAR_SEARCH_URL
from h2h_lit.sources.semantic_scholar import search_semantic_scholar

SourceAdapter = Callable[..., list[LiteratureRecord]]
TimestampFactory = Callable[[], str]


SOURCE_ADAPTERS: dict[str, SourceAdapter] = {
    "PubMed": search_pubmed,
    "EuropePMC": search_europe_pmc,
    "CrossRef": search_crossref,
    "SemanticScholar": search_semantic_scholar,
    "arXiv": search_arxiv,
}

SOURCE_ENDPOINTS = {
    "PubMed": PUBMED_EUTILS,
    "EuropePMC": EUROPE_PMC_SEARCH_URL,
    "CrossRef": CROSSREF_SEARCH_URL,
    "SemanticScholar": SEMANTIC_SCHOLAR_SEARCH_URL,
    "arXiv": ARXIV_API_URL,
}


@dataclass(slots=True)
class RetrievalQuerySpec:
    """One source request whose outcome must remain visible in provenance."""

    source_database: str
    query_text: str
    query_version: str
    limit: int = 50
    page: int | None = None
    cursor: str | None = None
    endpoint: str | None = None
    fields: list[str] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class SourceRequestError(RuntimeError):
    """A source returned a non-success HTTP response."""

    def __init__(self, status_code: int, url: str):
        super().__init__(f"HTTP {status_code} from {url}")
        self.status_code = status_code
        self.url = url


class _StatusCheckingHttpClient:
    def __init__(self, client: HttpClient):
        self._client = client
        self.status_codes: list[int] = []
        self.response_urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> HttpResponse:
        response = self._client.get(url, **kwargs)
        self.status_codes.append(response.status_code)
        self.response_urls.append(response.url)
        if not 200 <= response.status_code < 300:
            raise SourceRequestError(response.status_code, response.url or url)
        return response


def execute_retrieval_run(
    *,
    run_id: str,
    queries: Iterable[RetrievalQuerySpec],
    http_clients: Mapping[str, HttpClient],
    timestamp: TimestampFactory = retrieval_timestamp,
    software_version: str | None = None,
    protocol_version: str = "1.0.0",
    rubric_version: str = "1.0.0",
    query_plan_version: str = "retrieval-query-plan-v1",
    adapters: Mapping[str, SourceAdapter] = SOURCE_ADAPTERS,
    adapter_options: Mapping[str, Mapping[str, Any]] | None = None,
) -> ReviewDataset:
    """Execute injected adapters and preserve every request and returned occurrence."""

    if not run_id.strip():
        raise ValueError("run_id must not be empty")

    query_specs = list(queries)
    if not query_specs:
        raise ValueError("retrieval runs require at least one planned source query")
    if not query_plan_version.strip():
        raise ValueError("query_plan_version must not be empty")

    options = adapter_options or {}
    source_queries: list[SourceQuery] = []
    occurrences: list[RecordOccurrence] = []
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    planned_query_ids = [
        _query_id(run_id, request_index, spec)
        for request_index, spec in enumerate(query_specs)
    ]

    for request_index, spec in enumerate(query_specs):
        started_at = timestamp()
        first_timestamp = first_timestamp or started_at
        query_id = planned_query_ids[request_index]
        tracking_client: _StatusCheckingHttpClient | None = None
        records: list[LiteratureRecord] = []
        errors: list[str] = []
        status = ProcessingStatus.OK

        try:
            adapter = adapters[spec.source_database]
            tracking_client = _StatusCheckingHttpClient(http_clients[spec.source_database])
            records = adapter(
                spec.query_text,
                limit=spec.limit,
                http=tracking_client,
                **dict(options.get(spec.source_database, {})),
            )
        except Exception as exc:  # noqa: BLE001 - failed requests are persisted as data
            status = ProcessingStatus.FAILED
            errors.append(f"{type(exc).__name__}: {exc}")

        ended_at = timestamp()
        last_timestamp = ended_at
        query_metadata = dict(spec.metadata)
        query_metadata.update(
            {
                "request_index": request_index,
                "response_status_codes": tracking_client.status_codes if tracking_client else [],
                "response_urls": tracking_client.response_urls if tracking_client else [],
                "empty_result": status is ProcessingStatus.OK and not records,
            }
        )
        source_query = SourceQuery(
            query_id=query_id,
            source_database=spec.source_database,
            query_text=spec.query_text,
            retrieval_started_at=started_at,
            retrieval_ended_at=ended_at,
            status=status,
            run_id=run_id,
            endpoint=spec.endpoint or SOURCE_ENDPOINTS.get(spec.source_database),
            query_version=spec.query_version,
            page=spec.page,
            cursor=spec.cursor,
            result_count=len(records),
            fields=list(spec.fields),
            filters={**spec.filters, "limit": spec.limit},
            software_version=software_version,
            errors=errors,
            metadata=query_metadata,
        )
        source_queries.append(source_query)

        for source_rank, record in enumerate(records, start=1):
            source_identifier = _source_identifier(record, source_rank)
            occurrences.append(
                RecordOccurrence(
                    occurrence_id=_stable_id(
                        "occurrence", query_id, str(source_rank), source_identifier
                    ),
                    source_query_id=query_id,
                    source_identifier=source_identifier,
                    retrieved_at=ended_at,
                    record=record,
                    source_rank=source_rank,
                    page=spec.page,
                    cursor=spec.cursor,
                    raw_payload_hash=_payload_hash(record.original_metadata),
                    metadata={
                        "source_identifier_missing": record.source_identifier is None,
                    },
                )
            )

    dedupe_provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.retrieval",
            actor_type=ActorType.SOFTWARE,
            metadata={"software_version": software_version},
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version=protocol_version,
        rubric_version=rubric_version,
        created_at=last_timestamp or timestamp(),
        metadata={"run_id": run_id, "rule": "doi_first_title_fallback"},
    )
    canonical_records, duplicate_decisions = canonicalize_occurrences(
        occurrences,
        provenance=dedupe_provenance,
    )
    failed_queries = [
        query for query in source_queries if query.status is ProcessingStatus.FAILED
    ]
    if not failed_queries:
        run_status = ProcessingStatus.OK
    elif len(failed_queries) == len(source_queries):
        run_status = ProcessingStatus.FAILED
    else:
        run_status = ProcessingStatus.PARTIAL
    completed_at = last_timestamp or timestamp()
    retrieval_run = RetrievalRun(
        run_id=run_id,
        kind=RetrievalRunKind.PRIMARY,
        query_plan_version=query_plan_version,
        query_plan_hash=_query_plan_hash(query_specs),
        planned_query_ids=planned_query_ids,
        source_query_ids=[query.query_id for query in source_queries],
        retrieval_started_at=first_timestamp or completed_at,
        retrieval_completed_at=completed_at,
        status=run_status,
        protocol_version=protocol_version,
        retrieval_cutoff_date=_utc_date(completed_at) if run_status is ProcessingStatus.OK else None,
        software_version=software_version,
        errors=[
            f"{query.query_id}: {error}"
            for query in failed_queries
            for error in query.errors
        ],
        metadata={"rubric_version": rubric_version},
    )
    dataset = ReviewDataset(
        schema_version="1.1.0",
        retrieval_runs=[retrieval_run],
        source_queries=source_queries,
        occurrences=occurrences,
        canonical_records=canonical_records,
        duplicate_decisions=duplicate_decisions,
    )
    dataset.validate()
    return dataset


def save_review_dataset(path: str | Path, dataset: ReviewDataset) -> str:
    """Persist canonical JSON and return its SHA-256 content digest."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    content = dataset.to_json() + "\n"
    destination.write_text(content, encoding="utf-8", newline="\n")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load_review_dataset(path: str | Path) -> ReviewDataset:
    return ReviewDataset.from_json(Path(path).read_text(encoding="utf-8"))


def _source_identifier(record: LiteratureRecord, source_rank: int) -> str:
    return (
        record.source_identifier
        or record.doi
        or record.pmid
        or record.arxiv_id
        or f"missing-source-id:{source_rank}"
    )


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _query_id(run_id: str, request_index: int, spec: RetrievalQuerySpec) -> str:
    return _stable_id(
        "query",
        run_id,
        str(request_index),
        spec.source_database,
        spec.query_version,
        spec.query_text,
        str(spec.page),
        str(spec.cursor),
    )


def _query_plan_hash(specs: list[RetrievalQuerySpec]) -> str:
    payload = [
        {
            "source_database": spec.source_database,
            "query_text": spec.query_text,
            "query_version": spec.query_version,
            "limit": spec.limit,
            "page": spec.page,
            "cursor": spec.cursor,
            "endpoint": spec.endpoint,
            "fields": list(spec.fields),
            "filters": dict(spec.filters),
            "metadata": dict(spec.metadata),
        }
        for spec in specs
    ]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _utc_date(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("retrieval timestamps must be ISO 8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("retrieval timestamps must include a UTC offset")
    return parsed.astimezone(UTC).date().isoformat()


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"
