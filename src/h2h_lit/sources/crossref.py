"""CrossRef source adapter."""

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
        doi = item.get("DOI") or None
        records.append(
            make_record(
                title=titles[0] if titles else "",
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


def _crossref_item(item: Any, *, query: str, rank: int):
    if isinstance(item, dict):
        try:
            return parse_crossref_response({"message": {"items": [item]}}, query=query)[0]
        except Exception as exc:  # noqa: BLE001 - preserve malformed source items
            raw_item = {**item, "parser_incomplete": True, "parser_error": str(exc)}
    else:
        raw_item = {"raw_item": item, "parser_incomplete": True}
    return make_record(
            title="",
            source_identifier=malformed_identifier(item, rank),
            source_database="CrossRef",
            original_metadata=raw_item,
            source_query=query,
            stage="crossref_search",
        )


class CrossrefPaginator:
    source_database = "CrossRef"
    strategy = "cursor"
    version = "2.0.0"

    def initial_state(self, spec: Any) -> dict[str, Any]:
        return {"cursor": "*"}

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest:
        params: dict[str, Any] = dict(spec.filters)
        params.update({
            "query": spec.query_text,
            "rows": spec.limit,
            "cursor": state["cursor"],
        })
        return PageRequest("GET", spec.endpoint or SEARCH_URL, params=params, state=state)

    def parse_response(self, spec: Any, state: dict[str, Any], response: Any) -> ParsedPage:
        payload = response.json()
        message = payload.get("message") or {}
        items = message.get("items") or []
        if not isinstance(items, list):
            raise PaginationError("Crossref message.items must be a list")
        records = [
            _crossref_item(item, query=spec.query_text, rank=rank)
            for rank, item in enumerate(items, 1)
        ]
        next_cursor = message.get("next-cursor")
        terminal = len(items) < spec.limit
        if not terminal and not next_cursor:
            raise PaginationError("Crossref omitted next-cursor from a full page")
        if not terminal and next_cursor == state["cursor"]:
            raise PaginationError("Crossref repeated a non-terminal cursor")
        total = message.get("total-results")
        return ParsedPage(
            records=records,
            raw_item_count=len(items),
            next_state={"cursor": next_cursor} if not terminal else None,
            terminal=terminal,
            completion_proof="crossref_short_page" if terminal else None,
            source_reported_total=int(total) if total is not None else None,
            total_is_exact=total is not None,
            native_identifiers=[native_identifier(item, rank) for rank, item in enumerate(records, 1)],
            metadata={
                "message_type": message.get("type"),
                "message_version": message.get("version"),
            },
        )


PAGINATOR = CrossrefPaginator()
