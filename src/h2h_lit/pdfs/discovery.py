"""OA PDF candidate discovery.

Ported from the strongest historical `downloader.ipynb` behavior, separated from
PDF transfer so resolution and download can be independently tested.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote_plus

from h2h_lit.http import HttpClient
from h2h_lit.models import LiteratureRecord, ProcessingStatus, ProvenanceEvent, ProvenanceKind
from h2h_lit.normalize import normalize_doi


@dataclass(frozen=True, slots=True)
class PdfCandidate:
    url: str
    source: str
    priority: int
    reason: str


@dataclass(slots=True)
class PdfDiscoveryResult:
    candidates: list[PdfCandidate] = field(default_factory=list)
    events: list[ProvenanceEvent] = field(default_factory=list)

    @property
    def urls(self) -> list[str]:
        return [candidate.url for candidate in self.candidates]


def is_open_access(record: LiteratureRecord | dict[str, Any]) -> bool:
    """Interpret historical OA state from records or parsed BibTeX fields."""

    if isinstance(record, LiteratureRecord):
        return bool(record.is_open_access or record.pdf_url)
    note = str(record.get("note") or "")
    if "openaccess" in note.lower():
        return "openaccess: true" in note.lower()
    return bool(record.get("oa") or record.get("is_open_access") or record.get("pdf_url"))


def arxiv_pdf_from_url(url: str | None) -> str | None:
    value = (url or "").strip()
    if not value or "arxiv.org" not in value:
        return None
    if "/pdf/" in value:
        return value if value.lower().endswith(".pdf") else value + ".pdf"
    if "/abs/" in value:
        pdf = value.replace("/abs/", "/pdf/")
        return pdf if pdf.lower().endswith(".pdf") else pdf + ".pdf"
    return None


def semantic_scholar_pdf_for_doi(doi: str | None, *, http: HttpClient) -> tuple[str | None, ProvenanceEvent]:
    normalized = normalize_doi(doi)
    url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{quote_plus(normalized or '')}"
    event = _event("semantic_scholar_pdf_resolution", source_database="SemanticScholar", source_identifier=normalized)
    if not normalized:
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("missing DOI")
        return None, event
    try:
        response = http.get(url, params={"fields": "isOpenAccess,openAccessPdf,url,externalIds"}, timeout=30)
        event.metadata.update({"http_status": response.status_code, "url": url})
        if response.status_code != 200:
            event.status = ProcessingStatus.FAILED
            event.errors.append(f"HTTP {response.status_code}")
            return None, event
        pdf = ((response.json() or {}).get("openAccessPdf") or {}).get("url") or None
        event.status = ProcessingStatus.OK if pdf else ProcessingStatus.SKIPPED
        if not pdf:
            event.errors.append("no openAccessPdf.url")
        return pdf, event
    except Exception as exc:
        event.status = ProcessingStatus.FAILED
        event.errors.append(str(exc))
        return None, event


def unpaywall_pdf_for_doi(
    doi: str | None,
    *,
    http: HttpClient,
    email: str = "example@example.com",
) -> tuple[str | None, ProvenanceEvent]:
    normalized = normalize_doi(doi)
    url = f"https://api.unpaywall.org/v2/{quote_plus(normalized or '')}"
    event = _event("unpaywall_pdf_resolution", source_database="Unpaywall", source_identifier=normalized)
    if not normalized:
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("missing DOI")
        return None, event
    try:
        response = http.get(url, params={"email": email}, timeout=30)
        event.metadata.update({"http_status": response.status_code, "url": url})
        if response.status_code != 200:
            event.status = ProcessingStatus.FAILED
            event.errors.append(f"HTTP {response.status_code}")
            return None, event
        data = response.json() or {}
        locations = []
        best = data.get("best_oa_location")
        if best:
            locations.append(best)
        locations.extend(data.get("oa_locations") or [])
        for location in locations:
            pdf = (location or {}).get("url_for_pdf") or ""
            if pdf:
                return pdf, event
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("no url_for_pdf")
        return None, event
    except Exception as exc:
        event.status = ProcessingStatus.FAILED
        event.errors.append(str(exc))
        return None, event


def europe_pmc_pdf_for_doi(doi: str | None, *, http: HttpClient) -> tuple[str | None, ProvenanceEvent]:
    normalized = normalize_doi(doi)
    event = _event("europe_pmc_pdf_resolution", source_database="EuropePMC", source_identifier=normalized)
    if not normalized:
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("missing DOI")
        return None, event
    try:
        response = http.get(
            "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
            params={"query": f"DOI:{normalized}", "format": "json", "pageSize": 1},
            timeout=30,
        )
        event.metadata["http_status"] = response.status_code
        if response.status_code != 200:
            event.status = ProcessingStatus.FAILED
            event.errors.append(f"HTTP {response.status_code}")
            return None, event
        results = (((response.json() or {}).get("resultList") or {}).get("result") or [])
        if not results:
            event.status = ProcessingStatus.SKIPPED
            event.errors.append("no results")
            return None, event
        item = results[0]
        pmcid = (item.get("pmcid") or "").strip()
        if pmcid:
            return f"https://europepmc.org/articles/{pmcid}/pdf", event
        urls = ((item.get("fullTextUrlList") or {}).get("fullTextUrl") or [])
        for entry in urls:
            url = entry.get("url") or ""
            style = (entry.get("documentStyle") or "").lower()
            site = (entry.get("site") or "").lower()
            if url and (url.lower().endswith(".pdf") or style == "pdf" or "pdf" in site):
                return url, event
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("no pdf link")
        return None, event
    except Exception as exc:
        event.status = ProcessingStatus.FAILED
        event.errors.append(str(exc))
        return None, event


def pubmed_pmc_pdf_for_doi(doi: str | None, *, http: HttpClient) -> tuple[str | None, ProvenanceEvent]:
    normalized = normalize_doi(doi)
    event = _event("pubmed_pmc_pdf_resolution", source_database="PubMed/PMC", source_identifier=normalized)
    if not normalized:
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("missing DOI")
        return None, event
    try:
        esearch = http.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
            params={"db": "pubmed", "term": f"{normalized}[DOI]", "retmode": "json"},
            timeout=30,
        )
        event.metadata["pubmed_status"] = esearch.status_code
        if esearch.status_code != 200:
            event.status = ProcessingStatus.FAILED
            event.errors.append(f"PubMed HTTP {esearch.status_code}")
            return None, event
        ids = (((esearch.json() or {}).get("esearchresult") or {}).get("idlist") or [])
        if not ids:
            event.status = ProcessingStatus.SKIPPED
            event.errors.append("no PMID for DOI")
            return None, event
        elink = http.get(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi",
            params={"dbfrom": "pubmed", "db": "pmc", "id": ids[0], "retmode": "json"},
            timeout=30,
        )
        event.metadata["pmc_status"] = elink.status_code
        if elink.status_code != 200:
            event.status = ProcessingStatus.FAILED
            event.errors.append(f"PMC HTTP {elink.status_code}")
            return None, event
        for linkset in (elink.json() or {}).get("linksets") or []:
            for db in (linkset or {}).get("linksetdbs") or []:
                if db.get("dbto") != "pmc":
                    continue
                links = db.get("links") or []
                if links:
                    value = str(links[0])
                    pmcid = f"PMC{value}" if value.isdigit() else value
                    return f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/", event
        event.status = ProcessingStatus.SKIPPED
        event.errors.append("no PMC link")
        return None, event
    except Exception as exc:
        event.status = ProcessingStatus.FAILED
        event.errors.append(str(exc))
        return None, event


def discover_pdf_candidates(
    record: LiteratureRecord,
    *,
    http: HttpClient,
    unpaywall_email: str = "example@example.com",
) -> PdfDiscoveryResult:
    """Return de-duplicated candidate URLs in historical priority order."""

    result = PdfDiscoveryResult()
    raw: list[PdfCandidate] = []
    source = (record.source_database or "").lower()

    def add(url: str | None, source_name: str, reason: str):
        if url:
            raw.append(PdfCandidate(url=url, source=source_name, priority=len(raw), reason=reason))

    if "arxiv" in source or "arxiv.org" in (record.source_url or "").lower():
        add(arxiv_pdf_from_url(record.source_url), "arXiv", "source-specific arXiv URL")
    if "europepmc" in source:
        url, event = europe_pmc_pdf_for_doi(record.doi, http=http)
        result.events.append(event)
        add(url, "EuropePMC", "source-specific DOI lookup")
    if "pubmed" in source:
        url, event = pubmed_pmc_pdf_for_doi(record.doi, http=http)
        result.events.append(event)
        add(url, "PubMed/PMC", "source-specific DOI lookup")
    if "semantic" in source:
        url, event = semantic_scholar_pdf_for_doi(record.doi, http=http)
        result.events.append(event)
        add(url, "SemanticScholar", "source-specific DOI lookup")

    if record.pdf_url:
        add(record.pdf_url, record.source_database or "record", "record pdf_url")
    elif (record.source_url or "").lower().endswith(".pdf"):
        add(record.source_url, record.source_database or "record", "direct source_url pdf")

    for resolver, source_name in (
        (semantic_scholar_pdf_for_doi, "SemanticScholar"),
        (lambda d, *, http: unpaywall_pdf_for_doi(d, http=http, email=unpaywall_email), "Unpaywall"),
        (pubmed_pmc_pdf_for_doi, "PubMed/PMC"),
        (europe_pmc_pdf_for_doi, "EuropePMC"),
    ):
        url, event = resolver(record.doi, http=http)
        result.events.append(event)
        add(url, source_name, "fallback DOI lookup")

    seen: set[str] = set()
    for candidate in raw:
        if candidate.url in seen:
            continue
        seen.add(candidate.url)
        result.candidates.append(candidate)
    result.events.append(
        _event(
            "pdf_candidate_ordering",
            source_database=record.source_database,
            source_identifier=record.doi or record.source_identifier,
            metadata={"candidate_count": len(result.candidates), "urls": result.urls},
        )
    )
    return result


def _event(
    stage: str,
    *,
    source_database: str | None,
    source_identifier: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ProvenanceEvent:
    return ProvenanceEvent(
        kind=ProvenanceKind.DETERMINISTIC,
        stage=stage,
        source_database=source_database,
        source_identifier=source_identifier,
        metadata=metadata or {},
    )

