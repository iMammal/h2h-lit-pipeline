"""CrossRef source adapter."""

from __future__ import annotations

from typing import Any

from h2h_lit.http import HttpClient
from h2h_lit.sources.common import first_year, make_record, strip_markup

SEARCH_URL = "https://api.crossref.org/works"


def search_crossref(query: str, *, limit: int = 50, http: HttpClient) -> list:
    response = http.get(SEARCH_URL, params={"query": query, "rows": limit}, timeout=30)
    return parse_crossref_response(response.json(), query=query)


def _authors(item: dict[str, Any]) -> list[str]:
    out = []
    for author in item.get("author") or []:
        family = author.get("family") or ""
        given = author.get("given") or ""
        if family and given:
            out.append(f"{family}, {given}")
        elif family or given:
            out.append(family or given)
    return out


def _pdf_url(item: dict[str, Any]) -> str | None:
    for link in item.get("link") or []:
        if (link.get("content-type") or "").lower() == "application/pdf":
            return link.get("URL")
    return None


def parse_crossref_response(payload: dict[str, Any], *, query: str) -> list:
    records = []
    for item in ((payload.get("message") or {}).get("items") or []):
        titles = item.get("title") or []
        if not titles:
            continue
        doi = item.get("DOI") or None
        records.append(
            make_record(
                title=titles[0],
                abstract=strip_markup(item.get("abstract", "")),
                authors=_authors(item),
                year=first_year(item),
                doi=doi,
                source_identifier=doi,
                source_database="CrossRef",
                source_url=f"https://doi.org/{doi}" if doi else item.get("URL"),
                pdf_url=_pdf_url(item),
                journal=(item.get("container-title") or ["CrossRef"])[0],
                is_open_access=False,
                original_metadata=item,
                source_query=query,
                stage="crossref_search",
            )
        )
    return records

