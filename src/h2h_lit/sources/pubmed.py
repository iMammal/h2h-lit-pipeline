"""PubMed source adapter."""

from __future__ import annotations

import xml.etree.ElementTree as ET

from h2h_lit.http import HttpClient
from h2h_lit.sources.common import make_record

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"


def search_pubmed(query: str, *, limit: int = 50, http: HttpClient) -> list:
    search = http.get(
        EUTILS + "esearch.fcgi",
        params={"db": "pubmed", "term": query, "retmax": limit, "retmode": "xml"},
        timeout=30,
    )
    root = ET.fromstring(search.content)
    pmids = [el.text for el in root.findall(".//Id") if el.text]
    if not pmids:
        return []

    fetched = http.get(
        EUTILS + "efetch.fcgi",
        params={"db": "pubmed", "id": ",".join(pmids), "retmode": "xml", "rettype": "abstract"},
        timeout=30,
    )
    return parse_pubmed_fetch(fetched.content, query=query)


def parse_pubmed_fetch(content: bytes, *, query: str) -> list:
    root = ET.fromstring(content)
    records = []
    for article in root.findall(".//PubmedArticle"):
        pmid = article.findtext(".//PMID")
        title = article.findtext(".//ArticleTitle") or ""
        abstract = " ".join(t.text or "" for t in article.findall(".//AbstractText"))
        year = article.findtext(".//PubDate/Year")
        journal = article.findtext(".//Journal/Title") or "PubMed"
        doi = None
        for aid in article.findall(".//ArticleId"):
            if (aid.attrib.get("IdType") or "").lower() == "doi":
                doi = aid.text
                break
        authors = []
        for author in article.findall(".//Author"):
            last = author.findtext("LastName") or ""
            initials = author.findtext("Initials") or ""
            if last and initials:
                authors.append(f"{last}, {initials}")
            elif last:
                authors.append(last)
        records.append(
            make_record(
                title=title,
                abstract=abstract,
                authors=authors,
                year=year,
                doi=doi,
                pmid=pmid,
                source_identifier=pmid,
                source_database="PubMed",
                source_url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None,
                journal=journal,
                is_open_access=False,
                original_metadata={"pmid": pmid},
                source_query=query,
                stage="pubmed_search",
            )
        )
    return records

