"""arXiv source adapter."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import quote_plus

from h2h_lit.http import HttpClient
from h2h_lit.pagination import PageRequest, PaginationError, ParsedPage, native_identifier
from h2h_lit.sources.common import make_record

API_URL = "http://export.arxiv.org/api/query"
ATOM = "{http://www.w3.org/2005/Atom}"
OPEN_SEARCH = "{http://a9.com/-/spec/opensearch/1.1/}"


def search_arxiv(query: str, *, limit: int = 50, http: HttpClient) -> list:
    encoded = quote_plus(query)
    response = http.get(f"{API_URL}?search_query={encoded}&start=0&max_results={limit}", timeout=30)
    return parse_arxiv_response(response.content, query=query)


def _text(entry: ET.Element, tag: str) -> str:
    return entry.findtext(f"{ATOM}{tag}") or ""


def _links(entry: ET.Element) -> list[dict[str, str]]:
    return [link.attrib for link in entry.findall(f"{ATOM}link")]


def _abs_url(entry: ET.Element) -> str | None:
    for link in _links(entry):
        if link.get("rel") == "alternate":
            return link.get("href")
    return _text(entry, "id") or None


def _pdf_url(abs_url: str | None) -> str | None:
    if not abs_url or "arxiv.org" not in abs_url:
        return None
    if "/pdf/" in abs_url:
        return abs_url if abs_url.endswith(".pdf") else abs_url + ".pdf"
    if "/abs/" in abs_url:
        out = abs_url.replace("/abs/", "/pdf/")
        return out if out.endswith(".pdf") else out + ".pdf"
    return None


def parse_arxiv_response(content: bytes, *, query: str) -> list:
    root = ET.fromstring(content)
    records = []
    for entry in root.findall(f"{ATOM}entry"):
        abs_url = _abs_url(entry)
        arxiv_id = (abs_url or _text(entry, "id")).rstrip("/").split("/")[-1]
        authors = [a.findtext(f"{ATOM}name") or "" for a in entry.findall(f"{ATOM}author")]
        authors = [a for a in authors if a]
        published = _text(entry, "published")
        records.append(
            make_record(
                title=_text(entry, "title"),
                abstract=_text(entry, "summary"),
                authors=authors,
                year=published[:4] if published else None,
                arxiv_id=arxiv_id,
                source_identifier=arxiv_id,
                source_database="arXiv",
                source_url=abs_url,
                pdf_url=_pdf_url(abs_url),
                journal="arXiv",
                is_open_access=True,
                original_metadata={"id": _text(entry, "id"), "links": _links(entry)},
                source_query=query,
                stage="arxiv_search",
            )
        )
    return records


class ArxivPaginator:
    source_database = "arXiv"
    strategy = "start-max-results"
    version = "2.0.0"
    maximum_results = 30_000
    maximum_page_size = 2_000

    def initial_state(self, spec: Any) -> dict[str, Any]:
        return {"start": 0}

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest:
        params: dict[str, Any] = dict(spec.filters)
        params.update({
            "search_query": spec.query_text,
            "start": int(state["start"]),
            "max_results": min(spec.limit, self.maximum_page_size),
            "sortBy": spec.metadata.get("sort_by", "submittedDate"),
            "sortOrder": spec.metadata.get("sort_order", "ascending"),
        })
        return PageRequest("GET", spec.endpoint or API_URL, params=params, state=state)

    def parse_response(self, spec: Any, state: dict[str, Any], response: Any) -> ParsedPage:
        root = ET.fromstring(response.content)
        entries = root.findall(f"{ATOM}entry")
        if len(entries) == 1 and _text(entries[0], "title").strip().lower() == "error":
            raise PaginationError(f"arXiv returned an error feed: {_text(entries[0], 'summary')}")
        records = parse_arxiv_response(response.content, query=spec.query_text)
        total_text = root.findtext(f"{OPEN_SEARCH}totalResults")
        start_text = root.findtext(f"{OPEN_SEARCH}startIndex")
        page_size_text = root.findtext(f"{OPEN_SEARCH}itemsPerPage")
        total = int(total_text) if total_text is not None else None
        confirmed_start = int(start_text) if start_text is not None else int(state["start"])
        if confirmed_start != int(state["start"]):
            raise PaginationError("arXiv startIndex does not match the requested start")
        if total is not None and total > self.maximum_results:
            return ParsedPage(
                records=records,
                raw_item_count=len(entries),
                next_state=None,
                terminal=True,
                source_reported_total=total,
                total_is_exact=True,
                truncated=True,
                truncation_reason=(
                    f"arXiv total {total} exceeds supported window {self.maximum_results}"
                ),
                native_identifiers=[
                    native_identifier(item, rank) for rank, item in enumerate(records, 1)
                ],
            )
        next_start = confirmed_start + len(entries)
        terminal = (total is not None and next_start >= total) or len(entries) < min(
            spec.limit, self.maximum_page_size
        )
        if not terminal and not entries:
            raise PaginationError("arXiv returned an empty non-terminal page")
        return ParsedPage(
            records=records,
            raw_item_count=len(entries),
            next_state={"start": next_start} if not terminal else None,
            terminal=terminal,
            completion_proof="arxiv_exact_total_reached" if terminal and total is not None else (
                "arxiv_short_page" if terminal else None
            ),
            source_reported_total=total,
            total_is_exact=total is not None,
            native_identifiers=[native_identifier(item, rank) for rank, item in enumerate(records, 1)],
            metadata={
                "feed_updated": root.findtext(f"{ATOM}updated"),
                "items_per_page": int(page_size_text) if page_size_text is not None else None,
            },
        )


PAGINATOR = ArxivPaginator()
