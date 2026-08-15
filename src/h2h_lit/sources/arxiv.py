"""arXiv source adapter."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

from h2h_lit.http import HttpClient
from h2h_lit.sources.common import make_record

API_URL = "http://export.arxiv.org/api/query"
ATOM = "{http://www.w3.org/2005/Atom}"


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

