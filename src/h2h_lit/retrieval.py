"""Offline-capable retrieval orchestration for persisted review datasets."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import CheckpointStore, atomic_write
from h2h_lit.http import HttpClient, HttpResponse, RequestsHttpClient
from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.pagination import (
    PaginatedSourceAdapter,
    PaginationError,
    RateLimiter,
    RetryPolicy,
    native_identifier,
    redact_url,
)
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    RecordOccurrence,
    RetrievalAttempt,
    RetrievalAttemptStatus,
    RetrievalCompletionStatus,
    RetrievalPage,
    RetrievalRun,
    RetrievalRunKind,
    ReviewDataset,
    SourceQuery,
    canonicalize_occurrences,
)
from h2h_lit.sources.arxiv import (
    API_URL as ARXIV_API_URL,
)
from h2h_lit.sources.arxiv import (
    PAGINATOR as ARXIV_PAGINATOR,
)
from h2h_lit.sources.arxiv import (
    search_arxiv,
)
from h2h_lit.sources.common import retrieval_timestamp
from h2h_lit.sources.crossref import (
    PAGINATOR as CROSSREF_PAGINATOR,
)
from h2h_lit.sources.crossref import (
    SEARCH_URL as CROSSREF_SEARCH_URL,
)
from h2h_lit.sources.crossref import (
    search_crossref,
)
from h2h_lit.sources.europe_pmc import (
    PAGINATOR as EUROPE_PMC_PAGINATOR,
)
from h2h_lit.sources.europe_pmc import (
    SEARCH_URL as EUROPE_PMC_SEARCH_URL,
)
from h2h_lit.sources.europe_pmc import (
    search_europe_pmc,
)
from h2h_lit.sources.ieee_xplore import ABSTRACT_CONTENT_POLICY
from h2h_lit.sources.ieee_xplore import PAGINATOR as IEEE_XPLORE_PAGINATOR
from h2h_lit.sources.ieee_xplore import SEARCH_URL as IEEE_XPLORE_SEARCH_URL
from h2h_lit.sources.pubmed import (
    EUTILS as PUBMED_EUTILS,
)
from h2h_lit.sources.pubmed import (
    PAGINATOR as PUBMED_PAGINATOR,
)
from h2h_lit.sources.pubmed import (
    search_pubmed,
)
from h2h_lit.sources.semantic_scholar import (
    PAGINATOR as SEMANTIC_SCHOLAR_PAGINATOR,
)
from h2h_lit.sources.semantic_scholar import (
    SEARCH_URL as SEMANTIC_SCHOLAR_SEARCH_URL,
)
from h2h_lit.sources.semantic_scholar import (
    search_semantic_scholar,
)

SourceAdapter = Callable[..., list[LiteratureRecord]]
TimestampFactory = Callable[[], str]

RESPONSE_FREE_TRANSIENT_TRANSPORT_ERRORS = frozenset(
    {
        "ConnectionError",
        "ConnectTimeout",
        "ProxyError",
        "ReadTimeout",
        "SSLError",
        "Timeout",
    }
)


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
    "IEEEXplore": IEEE_XPLORE_SEARCH_URL,
}

PAGINATED_SOURCE_ADAPTERS: dict[str, PaginatedSourceAdapter] = {
    "PubMed": PUBMED_PAGINATOR,
    "EuropePMC": EUROPE_PMC_PAGINATOR,
    "CrossRef": CROSSREF_PAGINATOR,
    "SemanticScholar": SEMANTIC_SCHOLAR_PAGINATOR,
    "arXiv": ARXIV_PAGINATOR,
    "IEEEXplore": IEEE_XPLORE_PAGINATOR,
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
    pagination_mode: str | None = None
    credentials: dict[str, str] = field(default_factory=dict, repr=False)


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
            completion_status=(
                RetrievalCompletionStatus.FAILED
                if status is ProcessingStatus.FAILED
                else RetrievalCompletionStatus.COMPLETE
            ),
            completion_proof="legacy_single_request_completed"
            if status is ProcessingStatus.OK
            else None,
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
        completion_status=(
            RetrievalCompletionStatus.COMPLETE
            if run_status is ProcessingStatus.OK
            else RetrievalCompletionStatus.FAILED
        ),
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


def execute_paginated_retrieval_run(
    *,
    run_id: str,
    queries: Iterable[RetrievalQuerySpec],
    http_clients: Mapping[str, HttpClient],
    checkpoint_dir: str | Path,
    resume: bool = False,
    timestamp: TimestampFactory = retrieval_timestamp,
    software_version: str | None = None,
    protocol_version: str = "1.0.0",
    rubric_version: str = "1.0.0",
    query_plan_version: str = "retrieval-query-plan-v2",
    adapters: Mapping[str, PaginatedSourceAdapter] = PAGINATED_SOURCE_ADAPTERS,
    retry_policy: RetryPolicy | None = None,
    rate_limiter: RateLimiter | None = None,
    retry_sleep: Callable[[float], None] = time.sleep,
    request_budget: int | None = None,
    pause_status_codes: frozenset[int] = frozenset(),
    resumable_transport_exhaustion_sources: frozenset[str] = frozenset(),
) -> ReviewDataset:
    """Execute a complete, checkpointed retrieval wave without scientific filtering."""

    if not run_id.strip():
        raise ValueError("run_id must not be empty")
    specs = list(queries)
    if not specs:
        raise ValueError("retrieval runs require at least one planned source query")
    if any(spec.limit < 1 for spec in specs):
        raise ValueError("retrieval page sizes must be positive")
    if not query_plan_version.strip():
        raise ValueError("query_plan_version must not be empty")
    if request_budget is not None and request_budget < 0:
        raise ValueError("request budget cannot be negative")
    retry_policy = retry_policy or RetryPolicy()
    for spec in specs:
        if spec.source_database not in adapters:
            raise ValueError(f"unsupported paginated source: {spec.source_database}")
        if spec.source_database not in http_clients:
            raise ValueError(f"missing HTTP client for {spec.source_database}")
        if (
            spec.source_database == "IEEEXplore"
            and isinstance(http_clients[spec.source_database], RequestsHttpClient)
            and not spec.credentials.get("api_key")
        ):
            raise ValueError("IEEE Xplore Metadata API credential is required for live retrieval")
        if spec.page is not None or spec.cursor is not None:
            raise ValueError(
                "paginated runs must start from adapter initial state or a persisted checkpoint"
            )
        if spec.source_database == "SemanticScholar" and spec.pagination_mode not in {
            "relevance",
            "bulk",
        }:
            raise ValueError("Semantic Scholar mode must be explicitly frozen per query")
        page_limit = _maximum_page_size(spec)
        if spec.limit > page_limit:
            raise ValueError(
                f"{spec.source_database} page size {spec.limit} exceeds supported maximum "
                f"{page_limit}"
            )
        reserved = _reserved_request_fields(spec.source_database)
        conflicts = reserved.intersection(spec.filters)
        if conflicts:
            raise ValueError(
                f"{spec.source_database} filters cannot override orchestrated request fields: "
                f"{sorted(conflicts)!r}"
            )

    store = CheckpointStore(checkpoint_dir)
    plan_hash = _query_plan_hash(specs)
    query_ids = [_query_id(run_id, index, spec) for index, spec in enumerate(specs)]
    if resume:
        if not store.dataset_path.exists():
            raise FileNotFoundError("resume requested but no retrieval checkpoint exists")
        dataset = load_review_dataset(store.dataset_path)
        if len(dataset.retrieval_runs) != 1 or dataset.retrieval_runs[0].run_id != run_id:
            raise ValueError("checkpoint retrieval run ID does not match")
        run = dataset.retrieval_runs[0]
        if run.query_plan_hash != plan_hash or run.query_plan_version != query_plan_version:
            raise ValueError("checkpoint query plan/version does not match the requested run")
        if run.planned_query_ids != query_ids:
            raise ValueError("checkpoint planned query manifest does not match")
        if run.completion_status is RetrievalCompletionStatus.COMPLETE:
            return dataset
        if run.completion_status in {
            RetrievalCompletionStatus.FAILED,
            RetrievalCompletionStatus.TRUNCATED,
        }:
            return dataset
    else:
        if store.dataset_path.exists():
            raise FileExistsError("checkpoint already exists; use resume=True or a new run path")
        started_at = timestamp()
        source_queries = [
            SourceQuery(
                query_id=query_ids[index],
                source_database=spec.source_database,
                query_text=spec.query_text,
                retrieval_started_at=started_at,
                retrieval_ended_at=started_at,
                status=ProcessingStatus.PARTIAL,
                run_id=run_id,
                endpoint=spec.endpoint or SOURCE_ENDPOINTS.get(spec.source_database),
                query_version=spec.query_version,
                result_count=0,
                fields=list(spec.fields),
                filters={**spec.filters, "page_size": spec.limit},
                software_version=software_version,
                metadata={
                    **spec.metadata,
                    "request_index": index,
                    "pagination_mode": spec.pagination_mode,
                    "credential_names": sorted(spec.credentials),
                },
                completion_status=RetrievalCompletionStatus.PLANNED,
                content_policy=(
                    {"abstract": ABSTRACT_CONTENT_POLICY}
                    if spec.source_database == "IEEEXplore"
                    else dict(spec.metadata.get("content_policy", {}))
                ),
            )
            for index, spec in enumerate(specs)
        ]
        retrieval_run = RetrievalRun(
            run_id=run_id,
            kind=RetrievalRunKind.PRIMARY,
            query_plan_version=query_plan_version,
            query_plan_hash=plan_hash,
            planned_query_ids=query_ids,
            source_query_ids=query_ids,
            retrieval_started_at=started_at,
            retrieval_completed_at=started_at,
            status=ProcessingStatus.PARTIAL,
            protocol_version=protocol_version,
            software_version=software_version,
            errors=["retrieval wave incomplete"],
            metadata={"rubric_version": rubric_version},
            completion_status=RetrievalCompletionStatus.RUNNING,
        )
        dataset = ReviewDataset(
            schema_version="1.2.0",
            retrieval_runs=[retrieval_run],
            source_queries=source_queries,
        )
        store.save_dataset(dataset)

    run = dataset.retrieval_runs[0]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata.pop("pause_metadata", None)
    run.metadata.pop("session_request_count", None)
    requests_made = 0
    attempt_record_baselines: dict[str, int] = {}
    ordinary_attempt_baselines: dict[str, int] = {}
    limiter = rate_limiter
    if limiter is None:
        live = any(
            isinstance(http_clients[spec.source_database], RequestsHttpClient)
            for spec in specs
        )
        limiter = RateLimiter() if live else RateLimiter({})

    for spec, query_id in zip(specs, query_ids, strict=True):
        query = next(item for item in dataset.source_queries if item.query_id == query_id)
        if query.completion_status is RetrievalCompletionStatus.COMPLETE:
            continue
        if query.completion_status in {
            RetrievalCompletionStatus.FAILED,
            RetrievalCompletionStatus.TRUNCATED,
        }:
            continue
        query.completion_status = RetrievalCompletionStatus.RUNNING
        query.metadata.pop("pause_state", None)
        query.metadata.pop("pause_reason", None)
        query.metadata.pop("pause_metadata", None)
        adapter = adapters[spec.source_database]
        existing_query_pages = [
            item for item in dataset.retrieval_pages if item.source_query_id == query_id
        ]
        if any(
            item.adapter_version != adapter.version or item.strategy != adapter.strategy
            for item in existing_query_pages
        ):
            raise ValueError("checkpoint adapter version/strategy does not match")

        while query.completion_status is RetrievalCompletionStatus.RUNNING:
            query_pages = sorted(
                (item for item in dataset.retrieval_pages if item.source_query_id == query_id),
                key=lambda item: item.ordinal,
            )
            if query_pages and query_pages[-1].status is RetrievalCompletionStatus.RUNNING:
                page = query_pages[-1]
                state = dict(page.request_state)
            else:
                state = (
                    adapter.initial_state(spec)
                    if not query_pages
                    else dict(query_pages[-1].next_state or {})
                )
                if not state:
                    _fail_query(query, "non-terminal page omitted its next pagination state")
                    break
                state_fingerprint = _payload_hash(state)
                if any(_payload_hash(item.request_state) == state_fingerprint for item in query_pages):
                    _fail_query(query, "pagination state repeated before completion")
                    break
                ordinal = len(query_pages)
                page = RetrievalPage(
                    page_id=_stable_id("page", query_id, str(ordinal), state_fingerprint),
                    source_query_id=query_id,
                    ordinal=ordinal,
                    strategy=adapter.strategy,
                    adapter_version=adapter.version,
                    request_state=state,
                    status=RetrievalCompletionStatus.RUNNING,
                )
                dataset.retrieval_pages.append(page)
                query.page_ids.append(page.page_id)
                _touch(dataset, query, timestamp())
                _save_checkpoint(store, dataset, protocol_version, rubric_version, software_version)

            request = adapter.build_request(spec, state)
            existing_attempts = [
                item for item in dataset.retrieval_attempts if item.page_id == page.page_id
            ]
            response: HttpResponse | None = None
            if existing_attempts and existing_attempts[-1].status is RetrievalAttemptStatus.STARTED:
                interrupted = existing_attempts[-1]
                if interrupted.request_hash != request.request_hash():
                    _fail_page(page, query, "resumed page request hash changed")
                    break
                if interrupted.raw_response_path and interrupted.raw_response_hash:
                    response = store.load_response(
                        interrupted.raw_response_path, interrupted.raw_response_hash
                    )
                else:
                    interrupted.status = RetrievalAttemptStatus.FAILED
                    interrupted.ended_at = timestamp()
                    interrupted.error = "interrupted before response persistence"

            attempt_records_before_invocation = attempt_record_baselines.setdefault(
                page.page_id, len(existing_attempts)
            )
            ordinary_attempts_before_invocation = ordinary_attempt_baselines.setdefault(
                page.page_id, _ordinary_attempt_count(existing_attempts)
            )

            while response is None and _attempt_budget_remaining(
                existing_attempts,
                ordinary_attempts_before_invocation,
                retry_policy.max_attempts,
                per_invocation=(
                    spec.source_database
                    in resumable_transport_exhaustion_sources
                ),
            ):
                if request_budget is not None and requests_made >= request_budget:
                    return _pause_retrieval(
                        store,
                        dataset,
                        query,
                        pause_state="REQUEST_BUDGET_EXHAUSTED",
                        reason=(
                            f"invocation request budget {request_budget} exhausted before "
                            "the next provider request"
                        ),
                        requests_made=requests_made,
                        timestamp=timestamp,
                        protocol_version=protocol_version,
                        rubric_version=rubric_version,
                        software_version=software_version,
                    )
                attempt_number = len(existing_attempts) + 1
                attempt_id = _stable_id("attempt", page.page_id, str(attempt_number))
                rate_delay = limiter.wait(spec.source_database)
                attempt = RetrievalAttempt(
                    attempt_id=attempt_id,
                    page_id=page.page_id,
                    attempt_number=attempt_number,
                    started_at=timestamp(),
                    status=RetrievalAttemptStatus.STARTED,
                    request_method=request.method,
                    request_url=request.url,
                    request_params=request.sanitized_params(),
                    request_headers=request.sanitized_headers(),
                    request_hash=request.request_hash(),
                    retry_of_attempt_id=existing_attempts[-1].attempt_id
                    if existing_attempts
                    else None,
                    rate_limit_delay_seconds=rate_delay,
                )
                dataset.retrieval_attempts.append(attempt)
                page.attempt_ids.append(attempt_id)
                existing_attempts.append(attempt)
                _touch(dataset, query, attempt.started_at)
                _save_checkpoint(store, dataset, protocol_version, rubric_version, software_version)
                try:
                    requests_made += 1
                    if request.method.upper() == "GET":
                        raw_response = http_clients[spec.source_database].get(
                            request.url,
                            params=request.params,
                            headers=request.headers or None,
                            timeout=request.timeout,
                        )
                    elif request.method.upper() == "POST":
                        raw_response = http_clients[spec.source_database].post(
                            request.url,
                            data=request.params,
                            headers=request.headers or None,
                            timeout=request.timeout,
                        )
                    else:
                        raise ValueError(
                            f"unsupported paginated request method {request.method!r}"
                        )
                    relative_path, response_hash = store.save_response(attempt_id, raw_response)
                    attempt.raw_response_path = relative_path
                    attempt.raw_response_hash = response_hash
                    attempt.response_status = raw_response.status_code
                    attempt.actual_request_url = redact_url(
                        getattr(raw_response, "request_url", raw_response.url)
                    )
                    attempt.response_url = redact_url(raw_response.url)
                    attempt.response_headers = _sanitized_response_headers(raw_response.headers)
                    _touch(dataset, query, timestamp())
                    _save_checkpoint(
                        store, dataset, protocol_version, rubric_version, software_version
                    )
                    response = store.load_response(relative_path, response_hash)
                except Exception as exc:  # noqa: BLE001 - every transport failure is provenance
                    attempt.status = RetrievalAttemptStatus.FAILED
                    attempt.ended_at = timestamp()
                    attempt.error = f"{type(exc).__name__}: {exc}"
                    if _attempt_budget_remaining(
                        existing_attempts,
                        ordinary_attempts_before_invocation,
                        retry_policy.max_attempts,
                        per_invocation=(
                            spec.source_database
                            in resumable_transport_exhaustion_sources
                        ),
                    ):
                        invocation_attempt_number = (
                            _ordinary_attempt_count(existing_attempts)
                            - ordinary_attempts_before_invocation
                        )
                        delay = retry_policy.delay(
                            invocation_attempt_number
                            if spec.source_database
                            in resumable_transport_exhaustion_sources
                            else attempt_number
                        )
                        attempt.retry_delay_seconds = delay
                        retry_sleep(delay)
                    _touch(dataset, query, attempt.ended_at)
                    _save_checkpoint(
                        store, dataset, protocol_version, rubric_version, software_version
                    )

            if response is None:
                invocation_attempts = existing_attempts[
                    attempt_records_before_invocation:
                ]
                if (
                    spec.source_database
                    in resumable_transport_exhaustion_sources
                    and len(invocation_attempts) == retry_policy.max_attempts
                    and all(
                        _is_response_free_transient_transport_failure(item)
                        for item in invocation_attempts
                    )
                    and all(
                        _is_response_free_transient_transport_failure(item)
                        or _is_provider_rate_limit_pause_attempt(item)
                        for item in existing_attempts
                    )
                ):
                    return _pause_retrieval(
                        store,
                        dataset,
                        query,
                        pause_state="TRANSIENT_TRANSPORT_EXHAUSTED",
                        reason="TRANSIENT_TRANSPORT_EXHAUSTED_NO_RESPONSE",
                        pause_metadata={
                            "source_database": spec.source_database,
                            "response_received": False,
                            "attempts_this_invocation": len(invocation_attempts),
                            "maximum_attempts_per_invocation": (
                                retry_policy.max_attempts
                            ),
                            "failure_types": [
                                str(item.error or "").partition(":")[0]
                                for item in invocation_attempts
                            ],
                        },
                        requests_made=requests_made,
                        timestamp=timestamp,
                        protocol_version=protocol_version,
                        rubric_version=rubric_version,
                        software_version=software_version,
                    )
                _fail_page(page, query, "retrieval attempts exhausted before a response")
                break

            attempt = existing_attempts[-1]
            if response.status_code in pause_status_codes:
                retry_after = _header(response.headers, "retry-after")
                arxiv_rate_limit = spec.source_database == "arXiv"
                attempt.status = RetrievalAttemptStatus.FAILED
                attempt.ended_at = timestamp()
                attempt.error = (
                    f"PROVIDER_RATE_LIMIT_PAUSED_HTTP_{response.status_code}"
                    if arxiv_rate_limit
                    else f"PROVIDER_QUOTA_EXHAUSTED_HTTP_{response.status_code}"
                )
                pause_metadata = {
                    "source_database": spec.source_database,
                    "http_status": response.status_code,
                    "retry_after_header_present": retry_after is not None,
                    "retry_after": retry_after,
                }
                attempt.metadata["provider_pause"] = dict(pause_metadata)
                _touch(dataset, query, attempt.ended_at)
                return _pause_retrieval(
                    store,
                    dataset,
                    query,
                    pause_state=(
                        "PROVIDER_RATE_LIMIT"
                        if arxiv_rate_limit
                        else "PROVIDER_QUOTA_EXHAUSTED"
                    ),
                    reason=attempt.error,
                    pause_metadata=pause_metadata,
                    requests_made=requests_made,
                    timestamp=timestamp,
                    protocol_version=protocol_version,
                    rubric_version=rubric_version,
                    software_version=software_version,
                )
            if not 200 <= response.status_code < 300:
                attempt.status = RetrievalAttemptStatus.FAILED
                attempt.ended_at = timestamp()
                attempt.error = f"HTTP {response.status_code} from {response.url}"
                retryable = response.status_code in retry_policy.retry_statuses
                if retryable and len(existing_attempts) < retry_policy.max_attempts:
                    retry_after = _header(response.headers, "retry-after")
                    delay = retry_policy.delay(attempt.attempt_number, retry_after)
                    attempt.retry_delay_seconds = delay
                    retry_sleep(delay)
                    response = None
                    _touch(dataset, query, attempt.ended_at)
                    _save_checkpoint(
                        store, dataset, protocol_version, rubric_version, software_version
                    )
                    continue
                _fail_page(page, query, attempt.error)
                break

            try:
                parsed = adapter.parse_response(spec, request.state, response)
                if parsed.raw_item_count != len(parsed.records):
                    raise PaginationError(
                        "parsed record count does not account for every raw returned item"
                    )
            except Exception as exc:  # noqa: BLE001 - invalid pages may be retried verbatim
                attempt.status = RetrievalAttemptStatus.FAILED
                attempt.ended_at = timestamp()
                attempt.error = f"{type(exc).__name__}: {exc}"
                if len(existing_attempts) < retry_policy.max_attempts:
                    delay = retry_policy.delay(attempt.attempt_number)
                    attempt.retry_delay_seconds = delay
                    retry_sleep(delay)
                    response = None
                    _touch(dataset, query, attempt.ended_at)
                    _save_checkpoint(
                        store, dataset, protocol_version, rubric_version, software_version
                    )
                    continue
                _fail_page(page, query, attempt.error)
                break

            previous_query_occurrences = [
                item for item in dataset.occurrences if item.source_query_id == query_id
            ]
            for rank, record in enumerate(parsed.records, start=1):
                source_rank = len(previous_query_occurrences) + rank
                source_identifier = native_identifier(record, source_rank)
                occurrence = RecordOccurrence(
                    occurrence_id=_stable_id(
                        "occurrence", page.page_id, str(rank), source_identifier
                    ),
                    source_query_id=query_id,
                    source_identifier=source_identifier,
                    retrieved_at=attempt.ended_at or timestamp(),
                    record=record,
                    source_rank=source_rank,
                    page=page.ordinal,
                    cursor=_state_cursor(state),
                    raw_payload_hash=_payload_hash(record.original_metadata),
                    metadata={
                        "source_identifier_missing": record.source_identifier is None,
                        "parser_incomplete": bool(
                            record.original_metadata.get("parser_incomplete")
                        ),
                    },
                    retrieval_page_id=page.page_id,
                )
                dataset.occurrences.append(occurrence)
                page.occurrence_ids.append(occurrence.occurrence_id)

            page.returned_item_count = parsed.raw_item_count
            page.native_identifiers = list(parsed.native_identifiers)
            page.next_state = parsed.next_state
            page.source_reported_total = parsed.source_reported_total
            page.total_is_exact = parsed.total_is_exact
            page.terminal = parsed.terminal
            page.completion_proof = parsed.completion_proof
            page.truncated = parsed.truncated
            page.truncation_reason = parsed.truncation_reason
            page.metadata = dict(parsed.metadata)
            page.status = (
                RetrievalCompletionStatus.TRUNCATED
                if parsed.truncated
                else RetrievalCompletionStatus.COMPLETE
            )
            attempt.status = RetrievalAttemptStatus.SUCCEEDED
            attempt.ended_at = timestamp()

            consistency_error = _pagination_consistency_error(dataset, query, page)
            consistency_error = parsed.incomplete_reason or consistency_error
            query.result_count = sum(
                item.returned_item_count
                for item in dataset.retrieval_pages
                if item.source_query_id == query_id
            )
            if parsed.source_reported_total is not None:
                query.source_reported_total = parsed.source_reported_total
                query.total_is_exact = parsed.total_is_exact
            if consistency_error:
                _fail_page(page, query, consistency_error, preserve_page_status=True)
            elif parsed.truncated:
                query.status = ProcessingStatus.PARTIAL
                query.completion_status = RetrievalCompletionStatus.TRUNCATED
                query.errors.append(parsed.truncation_reason or "source result window truncated")
            elif parsed.terminal:
                query.status = ProcessingStatus.OK
                query.completion_status = RetrievalCompletionStatus.COMPLETE
                query.completion_proof = parsed.completion_proof
                query.retrieval_ended_at = attempt.ended_at

            _touch(dataset, query, attempt.ended_at)
            _save_checkpoint(store, dataset, protocol_version, rubric_version, software_version)

    completed_at = timestamp()
    run.retrieval_completed_at = completed_at
    incomplete = [
        item
        for item in dataset.source_queries
        if item.completion_status is not RetrievalCompletionStatus.COMPLETE
    ]
    if not incomplete:
        run.status = ProcessingStatus.OK
        run.completion_status = RetrievalCompletionStatus.COMPLETE
        run.retrieval_cutoff_date = _utc_date(completed_at)
        run.errors = []
        run.metadata.pop("pause_state", None)
        run.metadata.pop("pause_reason", None)
        run.metadata["session_request_count"] = requests_made
    else:
        run.status = (
            ProcessingStatus.FAILED
            if len(incomplete) == len(dataset.source_queries)
            else ProcessingStatus.PARTIAL
        )
        run.completion_status = (
            RetrievalCompletionStatus.TRUNCATED
            if any(
                item.completion_status is RetrievalCompletionStatus.TRUNCATED
                for item in incomplete
            )
            else RetrievalCompletionStatus.FAILED
        )
        run.retrieval_cutoff_date = None
        run.errors = [
            f"{item.query_id}: {error}" for item in incomplete for error in item.errors
        ] or ["retrieval wave incomplete"]
    _save_checkpoint(store, dataset, protocol_version, rubric_version, software_version)
    dataset.validate()
    return dataset


def _ordinary_attempt_count(attempts: list[RetrievalAttempt]) -> int:
    return sum(
        not str(item.error or "").startswith(
            ("PROVIDER_QUOTA_EXHAUSTED_HTTP_", "PROVIDER_RATE_LIMIT_PAUSED_HTTP_")
        )
        for item in attempts
    )


def _attempt_budget_remaining(
    attempts: list[RetrievalAttempt],
    attempts_before_invocation: int,
    maximum_attempts: int,
    *,
    per_invocation: bool,
) -> bool:
    ordinary_attempts = _ordinary_attempt_count(attempts)
    if per_invocation:
        ordinary_attempts -= attempts_before_invocation
    return ordinary_attempts < maximum_attempts


def _is_response_free_transient_transport_failure(
    attempt: RetrievalAttempt,
) -> bool:
    failure_type = str(attempt.error or "").partition(":")[0]
    return (
        attempt.status is RetrievalAttemptStatus.FAILED
        and failure_type in RESPONSE_FREE_TRANSIENT_TRANSPORT_ERRORS
        and attempt.response_status is None
        and attempt.raw_response_path is None
        and attempt.raw_response_hash is None
        and attempt.response_url is None
        and not attempt.response_headers
    )


def _is_provider_rate_limit_pause_attempt(attempt: RetrievalAttempt) -> bool:
    return (
        attempt.status is RetrievalAttemptStatus.FAILED
        and attempt.response_status == 429
        and str(attempt.error or "").startswith(
            "PROVIDER_RATE_LIMIT_PAUSED_HTTP_"
        )
        and attempt.raw_response_path is not None
        and attempt.raw_response_hash is not None
    )


def _pause_retrieval(
    store: CheckpointStore,
    dataset: ReviewDataset,
    query: SourceQuery,
    *,
    pause_state: str,
    reason: str,
    pause_metadata: Mapping[str, Any] | None = None,
    requests_made: int,
    timestamp: TimestampFactory,
    protocol_version: str,
    rubric_version: str,
    software_version: str | None,
) -> ReviewDataset:
    paused_at = timestamp()
    query.status = ProcessingStatus.PARTIAL
    query.completion_status = RetrievalCompletionStatus.RUNNING
    query.retrieval_ended_at = paused_at
    query.metadata["pause_state"] = pause_state
    query.metadata["pause_reason"] = reason
    if pause_metadata is not None:
        query.metadata["pause_metadata"] = dict(pause_metadata)
    run = dataset.retrieval_runs[0]
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_cutoff_date = None
    run.retrieval_completed_at = paused_at
    run.errors = [reason]
    run.metadata["pause_state"] = pause_state
    run.metadata["pause_reason"] = reason
    if pause_metadata is not None:
        run.metadata["pause_metadata"] = dict(pause_metadata)
    run.metadata["session_request_count"] = requests_made
    _save_checkpoint(
        store, dataset, protocol_version, rubric_version, software_version
    )
    dataset.validate()
    return dataset


def _touch(dataset: ReviewDataset, query: SourceQuery, value: str | None) -> None:
    if value is None:
        return
    query.retrieval_ended_at = value
    dataset.retrieval_runs[0].retrieval_completed_at = value


def _maximum_page_size(spec: RetrievalQuerySpec) -> int:
    if spec.source_database == "SemanticScholar":
        return 100 if spec.pagination_mode == "relevance" else 1000
    return {
        "PubMed": 10_000,
        "EuropePMC": 1000,
        "CrossRef": 1000,
        "arXiv": 2000,
        "IEEEXplore": 200,
    }[spec.source_database]


def _reserved_request_fields(source_database: str) -> set[str]:
    return {
        "PubMed": {"db", "term", "retmax", "retstart", "retmode", "usehistory", "api_key"},
        "EuropePMC": {"query", "format", "pageSize", "cursorMark", "resultType"},
        "CrossRef": {"query", "rows", "cursor"},
        "SemanticScholar": {"query", "limit", "offset", "token", "fields", "sort"},
        "arXiv": {"search_query", "start", "max_results", "sortBy", "sortOrder"},
        "IEEEXplore": {
            "apikey",
            "api_key",
            "format",
            "max_records",
            "start_record",
            "sort_field",
            "sort_order",
            "querytext",
            "meta_data",
            "article_title",
            "abstract",
        },
    }[source_database]


def _save_checkpoint(
    store: CheckpointStore,
    dataset: ReviewDataset,
    protocol_version: str,
    rubric_version: str,
    software_version: str | None,
) -> None:
    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.retrieval",
            actor_type=ActorType.SOFTWARE,
            metadata={"software_version": software_version},
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version=protocol_version,
        rubric_version=rubric_version,
        created_at=dataset.retrieval_runs[0].retrieval_started_at,
        metadata={
            "run_id": dataset.retrieval_runs[0].run_id,
            "rule": "doi_first_title_fallback",
        },
    )
    dataset.canonical_records, dataset.duplicate_decisions = canonicalize_occurrences(
        dataset.occurrences, provenance=provenance
    )
    store.save_dataset(dataset)


def _fail_query(query: SourceQuery, error: str) -> None:
    query.status = ProcessingStatus.FAILED
    query.completion_status = RetrievalCompletionStatus.FAILED
    if error not in query.errors:
        query.errors.append(error)


def _fail_page(
    page: RetrievalPage,
    query: SourceQuery,
    error: str,
    *,
    preserve_page_status: bool = False,
) -> None:
    if not preserve_page_status:
        page.status = RetrievalCompletionStatus.FAILED
    page.metadata["completion_error"] = error
    _fail_query(query, error)


def _pagination_consistency_error(
    dataset: ReviewDataset,
    query: SourceQuery,
    page: RetrievalPage,
) -> str | None:
    prior_pages = [
        item
        for item in dataset.retrieval_pages
        if item.source_query_id == query.query_id and item.page_id != page.page_id
    ]
    prior_ids = {identifier for item in prior_pages for identifier in item.native_identifiers}
    overlap = prior_ids.intersection(page.native_identifiers)
    if overlap:
        return f"source repeated native identifiers across pages: {sorted(overlap)!r}"
    exact_totals = {
        item.source_reported_total
        for item in [*prior_pages, page]
        if item.total_is_exact and item.source_reported_total is not None
    }
    if len(exact_totals) > 1:
        return f"source exact total changed during pagination: {sorted(exact_totals)!r}"
    arxiv_updates = {
        item.metadata.get("feed_updated")
        for item in [*prior_pages, page]
        if item.metadata.get("feed_updated")
    }
    if len(arxiv_updates) > 1:
        return "arXiv feed snapshot changed during pagination"
    if page.terminal and not page.truncated and page.total_is_exact:
        occurrence_count = sum(
            item.returned_item_count
            for item in [*prior_pages, page]
        )
        if occurrence_count != page.source_reported_total:
            return (
                f"terminal occurrence count {occurrence_count} does not match exact source total "
                f"{page.source_reported_total}"
            )
    if not page.terminal and page.next_state is None:
        return "non-terminal page did not provide next pagination state"
    return None


def _state_cursor(state: dict[str, Any]) -> str | None:
    for key in ("cursor", "cursor_mark", "token"):
        if state.get(key) is not None:
            return str(state[key])
    if state.get("start") is not None:
        return str(state["start"])
    if state.get("index") is not None:
        return str(state["index"])
    if state.get("start_record") is not None:
        return str(state["start_record"])
    return None


def _sanitized_response_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    sensitive = {"authorization", "proxy-authorization", "set-cookie", "cookie"}
    return {
        str(key): "<redacted>" if str(key).lower() in sensitive else str(value)
        for key, value in headers.items()
    }


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    for key, value in headers.items():
        if str(key).lower() == name.lower():
            return str(value)
    return None


def save_review_dataset(path: str | Path, dataset: ReviewDataset) -> str:
    """Persist canonical JSON and return its SHA-256 content digest."""

    destination = Path(path)
    content = dataset.to_json() + "\n"
    atomic_write(destination, content.encode("utf-8"))
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
            "pagination_mode": spec.pagination_mode,
            "credential_names": sorted(spec.credentials),
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
