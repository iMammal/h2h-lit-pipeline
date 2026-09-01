"""Europe PMC source adapter."""

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

SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"


def search_europe_pmc(query: str, *, limit: int = 50, http: HttpClient) -> list:
    response = http.get(SEARCH_URL, params={"query": query, "format": "json", "pageSize": limit}, timeout=30)
    return parse_europe_pmc_response(response.json(), query=query)


def _pdf_url(item: dict[str, Any]) -> str | None:
    urls = ((item.get("fullTextUrlList") or {}).get("fullTextUrl") or [])
    for entry in urls:
        url = entry.get("url") or ""
        style = (entry.get("documentStyle") or "").lower()
        site = (entry.get("site") or "").lower()
        if url and (style == "pdf" or url.lower().endswith(".pdf") or "pdf" in site):
            return url
    return None


def parse_europe_pmc_response(payload: dict[str, Any], *, query: str) -> list:
    records = []
    for item in ((payload.get("resultList") or {}).get("result") or []):
        doi = item.get("doi") or None
        source_url = f"https://doi.org/{doi}" if doi else item.get("fullTextUrl")
        records.append(
            make_record(
                title=item.get("title", ""),
                abstract=item.get("abstractText", ""),
                authors=[item["authorString"]] if item.get("authorString") else [],
                year=item.get("pubYear"),
                doi=doi,
                pmid=item.get("pmid"),
                source_identifier=item.get("id") or item.get("pmid") or doi,
                source_database="EuropePMC",
                source_url=source_url,
                pdf_url=_pdf_url(item),
                journal=item.get("journalTitle") or "EuropePMC",
                is_open_access=(item.get("isOpenAccess") == "Y"),
                original_metadata=item,
                source_query=query,
                stage="europe_pmc_search",
            )
        )
    return records


def _europe_pmc_item(item: Any, *, query: str, rank: int):
    if isinstance(item, dict):
        try:
            return parse_europe_pmc_response(
                {"resultList": {"result": [item]}}, query=query
            )[0]
        except Exception as exc:  # noqa: BLE001 - preserve malformed source items
            raw_item = {**item, "parser_incomplete": True, "parser_error": str(exc)}
    else:
        raw_item = {"raw_item": item, "parser_incomplete": True}
    return make_record(
            title="",
            source_identifier=malformed_identifier(item, rank),
            source_database="EuropePMC",
            original_metadata=raw_item,
            source_query=query,
            stage="europe_pmc_search",
        )


class EuropePmcPaginator:
    source_database = "EuropePMC"
    strategy = "cursor-mark"
    version = "2.0.0"

    def initial_state(self, spec: Any) -> dict[str, Any]:
        return {"cursor_mark": "*"}

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest:
        params = dict(spec.filters)
        params.update({
            "query": spec.query_text,
            "format": "json",
            "pageSize": min(spec.limit, 1000),
            "cursorMark": state["cursor_mark"],
            "resultType": spec.metadata.get("result_type", "core"),
        })
        return PageRequest("GET", spec.endpoint or SEARCH_URL, params=params, state=state)

    def parse_response(self, spec: Any, state: dict[str, Any], response: Any) -> ParsedPage:
        payload = response.json()
        items = ((payload.get("resultList") or {}).get("result") or [])
        if not isinstance(items, list):
            raise PaginationError("Europe PMC resultList.result must be a list")
        records = [
            _europe_pmc_item(item, query=spec.query_text, rank=rank)
            for rank, item in enumerate(items, 1)
        ]
        total = payload.get("hitCount")
        total = int(total) if total is not None else None
        next_cursor = payload.get("nextCursorMark")
        if next_cursor == state["cursor_mark"] and items:
            raise PaginationError("Europe PMC repeated a non-terminal cursor")
        terminal = not next_cursor
        if not terminal and not items:
            raise PaginationError("Europe PMC returned an empty non-terminal cursor page")
        return ParsedPage(
            records=records,
            raw_item_count=len(items),
            next_state={"cursor_mark": next_cursor} if not terminal else None,
            terminal=terminal,
            completion_proof="europe_pmc_cursor_exhausted" if terminal else None,
            source_reported_total=total,
            total_is_exact=total is not None,
            native_identifiers=[native_identifier(item, rank) for rank, item in enumerate(records, 1)],
            metadata={"next_page_url": payload.get("nextPageUrl")},
        )


PAGINATOR = EuropePmcPaginator()
