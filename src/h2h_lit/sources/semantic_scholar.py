"""Semantic Scholar source adapter."""

from __future__ import annotations

from typing import Any

from h2h_lit.http import HttpClient
from h2h_lit.pagination import (
    PageRequest,
    PaginationError,
    ParsedPage,
    malformed_identifier,
    native_identifier,
)
from h2h_lit.sources.common import make_record

SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"
BULK_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"


def search_semantic_scholar(
    query: str,
    *,
    limit: int = 50,
    http: HttpClient,
    api_key: str | None = None,
) -> list:
    headers = {"x-api-key": api_key} if api_key else None
    response = http.get(
        SEARCH_URL,
        params={
            "query": query,
            "limit": limit,
            "fields": "title,abstract,year,venue,url,externalIds,isOpenAccess,authors,openAccessPdf",
        },
        headers=headers,
        timeout=30,
    )
    return parse_semantic_scholar_response(response.json(), query=query)


def parse_semantic_scholar_response(payload: dict[str, Any], *, query: str) -> list:
    records = []
    for paper in payload.get("data") or []:
        external_ids = paper.get("externalIds") or {}
        oapdf = paper.get("openAccessPdf") or {}
        records.append(
            make_record(
                title=paper.get("title", ""),
                abstract=paper.get("abstract", ""),
                authors=[a.get("name", "") for a in paper.get("authors") or [] if a.get("name")],
                year=paper.get("year"),
                doi=external_ids.get("DOI"),
                arxiv_id=external_ids.get("ArXiv"),
                source_identifier=paper.get("paperId") or external_ids.get("DOI") or paper.get("url"),
                source_database="SemanticScholar",
                source_url=paper.get("url"),
                pdf_url=oapdf.get("url"),
                journal=paper.get("venue") or "SemanticScholar",
                is_open_access=paper.get("isOpenAccess"),
                original_metadata=paper,
                source_query=query,
                stage="semantic_scholar_search",
            )
        )
    return records


def _semantic_item(item: Any, *, query: str, rank: int):
    if isinstance(item, dict):
        try:
            return parse_semantic_scholar_response({"data": [item]}, query=query)[0]
        except Exception as exc:  # noqa: BLE001 - preserve malformed source items
            raw_item = {**item, "parser_incomplete": True, "parser_error": str(exc)}
    else:
        raw_item = {"raw_item": item, "parser_incomplete": True}
    return make_record(
            title="",
            source_identifier=malformed_identifier(item, rank),
            source_database="SemanticScholar",
            original_metadata=raw_item,
            source_query=query,
            stage="semantic_scholar_search",
        )


class SemanticScholarPaginator:
    source_database = "SemanticScholar"
    strategy = "explicit-relevance-or-bulk"
    version = "2.0.0"
    relevance_window = 1000

    def _mode(self, spec: Any) -> str:
        mode = spec.pagination_mode
        if mode not in {"relevance", "bulk"}:
            raise ValueError(
                "Semantic Scholar queries require pagination_mode='relevance' or 'bulk'"
            )
        return mode

    def initial_state(self, spec: Any) -> dict[str, Any]:
        mode = self._mode(spec)
        return {"mode": mode, "offset": 0} if mode == "relevance" else {"mode": mode}

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest:
        mode = self._mode(spec)
        fields = spec.fields or [
            "title", "abstract", "year", "venue", "url", "externalIds",
            "isOpenAccess", "authors", "openAccessPdf",
        ]
        params: dict[str, Any] = dict(spec.filters)
        params.update({
            "query": spec.query_text,
            "limit": spec.limit,
            "fields": ",".join(fields),
        })
        if mode == "relevance":
            params["offset"] = state["offset"]
            endpoint = spec.endpoint or SEARCH_URL
        else:
            if state.get("token"):
                params["token"] = state["token"]
            params["sort"] = spec.metadata.get("sort", "paperId:asc")
            endpoint = spec.endpoint or BULK_SEARCH_URL
        api_key = spec.credentials.get("api_key")
        headers = {"x-api-key": api_key} if api_key else {}
        return PageRequest("GET", endpoint, params=params, headers=headers, state=state)

    def parse_response(self, spec: Any, state: dict[str, Any], response: Any) -> ParsedPage:
        payload = response.json()
        items = payload.get("data") or []
        if not isinstance(items, list):
            raise PaginationError("Semantic Scholar data must be a list")
        records = [
            _semantic_item(item, query=spec.query_text, rank=rank)
            for rank, item in enumerate(items, 1)
        ]
        mode = self._mode(spec)
        total = payload.get("total")
        total_value = int(total) if total is not None else None
        if mode == "relevance" and total_value is not None and total_value > self.relevance_window:
            return ParsedPage(
                records=records,
                raw_item_count=len(items),
                next_state=None,
                terminal=True,
                source_reported_total=total_value,
                total_is_exact=True,
                truncated=True,
                truncation_reason=(
                    f"Semantic Scholar relevance total {total_value} exceeds supported "
                    f"window {self.relevance_window}"
                ),
                native_identifiers=[
                    native_identifier(item, rank) for rank, item in enumerate(records, 1)
                ],
            )
        next_value = payload.get("next") if mode == "relevance" else payload.get("token")
        terminal = next_value is None
        if not terminal and not items:
            raise PaginationError("Semantic Scholar returned an empty non-terminal page")
        if mode == "relevance":
            next_state = {"mode": mode, "offset": int(next_value)} if not terminal else None
            proof = "semantic_scholar_relevance_next_exhausted"
        else:
            if next_value == state.get("token"):
                raise PaginationError("Semantic Scholar repeated a bulk token")
            next_state = {"mode": mode, "token": next_value} if not terminal else None
            proof = "semantic_scholar_bulk_token_exhausted"
        return ParsedPage(
            records=records,
            raw_item_count=len(items),
            next_state=next_state,
            terminal=terminal,
            completion_proof=proof if terminal else None,
            source_reported_total=total_value,
            total_is_exact=mode == "relevance" and total_value is not None,
            native_identifiers=[native_identifier(item, rank) for rank, item in enumerate(records, 1)],
            metadata={"mode": mode, "total_is_estimate": mode == "bulk"},
        )


PAGINATOR = SemanticScholarPaginator()
