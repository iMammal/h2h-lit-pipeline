"""Semantic Scholar source adapter."""

from __future__ import annotations

from typing import Any

from h2h_lit.http import HttpClient
from h2h_lit.sources.common import make_record

SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"


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

