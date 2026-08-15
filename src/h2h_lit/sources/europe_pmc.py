"""Europe PMC source adapter."""

from __future__ import annotations

from typing import Any

from h2h_lit.http import HttpClient
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

