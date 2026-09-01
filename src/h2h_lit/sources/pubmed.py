"""PubMed source adapter."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from h2h_lit.http import HttpClient
from h2h_lit.pagination import PageRequest, PaginationError, ParsedPage, native_identifier
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


class PubMedPaginator:
    source_database = "PubMed"
    strategy = "esearch-id-manifest-efetch-batches"
    version = "2.0.0"
    maximum_results = 10_000

    def initial_state(self, spec: Any) -> dict[str, Any]:
        return {"phase": "search"}

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest:
        if state["phase"] == "search":
            params: dict[str, Any] = dict(spec.filters)
            params.update({
                "db": "pubmed",
                "term": spec.query_text,
                "retmax": self.maximum_results,
                "retmode": "xml",
                "usehistory": "y",
            })
            if spec.credentials.get("api_key"):
                params["api_key"] = spec.credentials["api_key"]
            return PageRequest("GET", EUTILS + "esearch.fcgi", params=params, state=state)
        ids = list(state["pmids"])
        index = int(state["index"])
        batch = ids[index : index + spec.limit]
        return PageRequest(
            "GET",
            EUTILS + "efetch.fcgi",
            params={
                "db": "pubmed",
                "id": ",".join(batch),
                "retmode": "xml",
                "rettype": "abstract",
            },
            state={**state, "batch_pmids": batch},
        )

    def parse_response(
        self, spec: Any, state: dict[str, Any], response: Any
    ) -> ParsedPage:
        root = ET.fromstring(response.content)
        if state["phase"] == "search":
            count_text = root.findtext(".//Count")
            pmids = [item.text for item in root.findall(".//Id") if item.text]
            count = int(count_text) if count_text is not None else len(pmids)
            metadata = {
                "query_translation": root.findtext(".//QueryTranslation"),
                "query_key": root.findtext(".//QueryKey"),
                "webenv": root.findtext(".//WebEnv"),
                "pmid_manifest": pmids,
            }
            if count > self.maximum_results:
                return ParsedPage(
                    records=[],
                    raw_item_count=0,
                    next_state=None,
                    terminal=True,
                    source_reported_total=count,
                    total_is_exact=True,
                    truncated=True,
                    truncation_reason=(
                        f"PubMed result count {count} exceeds supported window "
                        f"{self.maximum_results}"
                    ),
                    metadata=metadata,
                )
            if len(pmids) != count:
                raise PaginationError(
                    f"PubMed returned {len(pmids)} IDs for an exact Count of {count}"
                )
            if not pmids:
                return ParsedPage(
                    records=[],
                    raw_item_count=0,
                    next_state=None,
                    terminal=True,
                    completion_proof="pubmed_exact_zero_count",
                    source_reported_total=0,
                    total_is_exact=True,
                    metadata=metadata,
                )
            return ParsedPage(
                records=[],
                raw_item_count=0,
                next_state={"phase": "fetch", "pmids": pmids, "index": 0},
                terminal=False,
                source_reported_total=count,
                total_is_exact=True,
                metadata=metadata,
            )

        expected = list(state.get("batch_pmids") or [])
        records = parse_pubmed_fetch(response.content, query=spec.query_text)
        for index, record in enumerate(records):
            if not record.pmid and index < len(expected):
                record.pmid = expected[index]
                record.source_identifier = expected[index]
                record.original_metadata["parser_incomplete"] = True
                record.original_metadata["inferred_pmid_from_requested_batch_position"] = True
        returned = [record.pmid for record in records]
        incomplete_reason = None
        if returned != expected:
            incomplete_reason = (
                f"PubMed EFetch PMID sequence {returned!r} does not match requested {expected!r}"
            )
        next_index = int(state["index"]) + len(expected)
        terminal = next_index == len(state["pmids"])
        return ParsedPage(
            records=records,
            raw_item_count=len(records),
            next_state=None
            if terminal
            else {"phase": "fetch", "pmids": list(state["pmids"]), "index": next_index},
            terminal=terminal,
            completion_proof="pubmed_exact_id_manifest_fetched" if terminal else None,
            source_reported_total=len(state["pmids"]),
            total_is_exact=True,
            incomplete_reason=incomplete_reason,
            native_identifiers=[native_identifier(item, rank) for rank, item in enumerate(records, 1)],
            metadata={"requested_pmids": expected},
        )


PAGINATOR = PubMedPaginator()
