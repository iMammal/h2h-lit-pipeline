import json

import pytest

from h2h_lit.sources.arxiv import search_arxiv
from h2h_lit.sources.crossref import search_crossref
from h2h_lit.sources.europe_pmc import search_europe_pmc
from h2h_lit.sources.pubmed import parse_pubmed_fetch, search_pubmed
from h2h_lit.sources.semantic_scholar import search_semantic_scholar
from tests.fake_http import FakeHttp, FakeResponse


def test_pubmed_search_parses_esearch_and_efetch():
    esearch = b"<eSearchResult><IdList><Id>123</Id></IdList></eSearchResult>"
    efetch = b"""
    <PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID>123</PMID>
          <Article>
            <Journal><Title>Journal Name</Title></Journal>
            <ArticleTitle>PubMed Paper</ArticleTitle>
            <Abstract><AbstractText>PubMed abstract.</AbstractText></Abstract>
            <AuthorList><Author><LastName>Smith</LastName><Initials>A</Initials></Author></AuthorList>
            <Journal><JournalIssue><PubDate><Year>2024</Year></PubDate></JournalIssue></Journal>
          </Article>
        </MedlineCitation>
        <PubmedData><ArticleIdList><ArticleId IdType="doi">10.1000/PubMed</ArticleId></ArticleIdList></PubmedData>
      </PubmedArticle>
    </PubmedArticleSet>
    """
    http = FakeHttp([FakeResponse(content=esearch), FakeResponse(content=efetch)])
    records = search_pubmed("network", http=http)
    assert records[0].title == "PubMed Paper"
    assert records[0].doi == "10.1000/pubmed"
    assert records[0].source_database == "PubMed"
    assert len(http.calls) == 2


def test_pubmed_empty_search_does_not_fetch():
    http = FakeHttp([FakeResponse(content=b"<eSearchResult><IdList /></eSearchResult>")])
    assert search_pubmed("missing", http=http) == []
    assert len(http.calls) == 1


def test_pubmed_fetch_parses_book_article_without_inventing_journal_metadata():
    content = b"""
    <PubmedArticleSet>
      <PubmedBookArticle>
        <BookDocument>
          <PMID>33085405</PMID>
          <ArticleIdList><ArticleId IdType="doi">10.1000/book</ArticleId></ArticleIdList>
          <Book>
            <Publisher><PublisherName>Evidence Press</PublisherName></Publisher>
            <BookTitle>Clinical Evidence Handbook</BookTitle>
            <CollectionTitle>Evidence Reports</CollectionTitle>
            <PubDate><Year>2020</Year></PubDate>
            <AuthorList><Author><LastName>Ng</LastName><Initials>A</Initials></Author></AuthorList>
          </Book>
          <Abstract><AbstractText>Book <i>abstract</i>.</AbstractText></Abstract>
        </BookDocument>
      </PubmedBookArticle>
    </PubmedArticleSet>
    """

    record = parse_pubmed_fetch(content, query="frozen query")[0]

    assert record.pmid == "33085405"
    assert record.title == "Clinical Evidence Handbook"
    assert record.abstract == "Book abstract."
    assert record.authors == ["Ng, A"]
    assert record.year == "2020"
    assert record.doi == "10.1000/book"
    assert record.journal == ""
    assert record.original_metadata == {
        "pmid": "33085405",
        "pubmed_record_type": "PubmedBookArticle",
        "book_title": "Clinical Evidence Handbook",
        "collection_title": "Evidence Reports",
        "publisher_name": "Evidence Press",
    }


def test_pubmed_fetch_preserves_mixed_article_book_provider_order():
    content = b"""
    <PubmedArticleSet>
      <PubmedArticle><MedlineCitation><PMID>100</PMID><Article>
        <ArticleTitle>Article first</ArticleTitle>
      </Article></MedlineCitation></PubmedArticle>
      <PubmedBookArticle><BookDocument><PMID>200</PMID><Book>
        <BookTitle>Book second</BookTitle>
      </Book></BookDocument></PubmedBookArticle>
      <PubmedArticle><MedlineCitation><PMID>300</PMID><Article>
        <ArticleTitle>Article third</ArticleTitle>
      </Article></MedlineCitation></PubmedArticle>
    </PubmedArticleSet>
    """

    records = parse_pubmed_fetch(content, query="frozen query")

    assert [record.pmid for record in records] == ["100", "200", "300"]
    assert [record.original_metadata["pubmed_record_type"] for record in records] == [
        "PubmedArticle",
        "PubmedBookArticle",
        "PubmedArticle",
    ]


def test_pubmed_fetch_preserves_incomplete_book_record_without_fabricated_fields():
    content = b"""
    <PubmedArticleSet><PubmedBookArticle><BookDocument>
      <PMID>39836822</PMID>
    </BookDocument></PubmedBookArticle></PubmedArticleSet>
    """

    record = parse_pubmed_fetch(content, query="frozen query")[0]

    assert record.pmid == "39836822"
    assert record.title == ""
    assert record.abstract == ""
    assert record.authors == []
    assert record.year is None
    assert record.doi is None
    assert record.journal == ""
    assert record.original_metadata["pubmed_record_type"] == "PubmedBookArticle"


def test_europe_pmc_search_preserves_pdf_metadata():
    payload = {
        "resultList": {
            "result": [
                {
                    "id": "E1",
                    "title": "Europe PMC Paper",
                    "abstractText": "Abstract",
                    "doi": "10.1000/epmc",
                    "pubYear": "2023",
                    "authorString": "A. Smith",
                    "journalTitle": "EPMC Journal",
                    "isOpenAccess": "Y",
                    "fullTextUrlList": {
                        "fullTextUrl": [{"url": "https://example.test/paper.pdf", "documentStyle": "pdf"}]
                    },
                }
            ]
        }
    }
    records = search_europe_pmc("query", http=FakeHttp([FakeResponse(payload=payload)]))
    assert records[0].pdf_url == "https://example.test/paper.pdf"
    assert records[0].is_open_access is True
    assert records[0].original_metadata["id"] == "E1"


def test_crossref_search_parses_authors_year_and_pdf_link():
    payload = {
        "message": {
            "items": [
                {
                    "title": ["CrossRef Paper"],
                    "abstract": "<p>CrossRef abstract.</p>",
                    "DOI": "10.1000/cross",
                    "published-online": {"date-parts": [[2022, 1, 1]]},
                    "author": [{"family": "Nguyen", "given": "B"}],
                    "container-title": ["Venue"],
                    "link": [{"content-type": "application/pdf", "URL": "https://x.test/a.pdf"}],
                }
            ]
        }
    }
    records = search_crossref("query", http=FakeHttp([FakeResponse(payload=payload)]))
    assert records[0].authors == ["Nguyen, B"]
    assert records[0].year == 2022
    assert records[0].abstract == "CrossRef abstract."
    assert records[0].pdf_url == "https://x.test/a.pdf"


def test_crossref_preserves_raw_item_without_a_title():
    payload = {"message": {"items": [{"DOI": "10.1000/no-title"}]}}

    records = search_crossref("query", http=FakeHttp([FakeResponse(payload=payload)]))

    assert len(records) == 1
    assert records[0].title == ""
    assert records[0].doi == "10.1000/no-title"


def test_semantic_scholar_search_passes_api_key_header_and_parses_open_access_pdf():
    payload = {
        "data": [
            {
                "paperId": "S2",
                "title": "Semantic Scholar Paper",
                "abstract": "Abstract",
                "year": 2021,
                "venue": "S2 Venue",
                "url": "https://sem.test/paper",
                "externalIds": {"DOI": "10.1000/s2", "ArXiv": "2101.00001"},
                "isOpenAccess": True,
                "authors": [{"name": "Ada Lovelace"}],
                "openAccessPdf": {"url": "https://sem.test/paper.pdf"},
            }
        ]
    }
    http = FakeHttp([FakeResponse(payload=payload)])
    records = search_semantic_scholar("query", http=http, api_key="fake-key")
    assert http.calls[0]["headers"] == {"x-api-key": "fake-key"}
    assert records[0].pdf_url == "https://sem.test/paper.pdf"
    assert records[0].arxiv_id == "2101.00001"


def test_arxiv_search_parses_atom_and_pdf_url():
    atom = b"""
    <feed xmlns="http://www.w3.org/2005/Atom">
      <entry>
        <id>http://arxiv.org/abs/2401.00001v1</id>
        <title>arXiv Paper</title>
        <summary>arXiv abstract.</summary>
        <published>2024-01-01T00:00:00Z</published>
        <author><name>Paper Author</name></author>
        <link rel="alternate" href="http://arxiv.org/abs/2401.00001v1" />
      </entry>
    </feed>
    """
    records = search_arxiv("query", http=FakeHttp([FakeResponse(content=atom)]))
    assert records[0].source_database == "arXiv"
    assert records[0].pdf_url == "http://arxiv.org/pdf/2401.00001v1.pdf"
    assert records[0].is_open_access is True


def test_malformed_json_propagates_parse_error_for_caller_policy():
    http = FakeHttp([FakeResponse(payload=json.JSONDecodeError("bad", "x", 0))])
    with pytest.raises(json.JSONDecodeError):
        search_crossref("query", http=http)


def test_rate_limited_response_is_visible_to_adapter_caller():
    http = FakeHttp([FakeResponse(status_code=429, payload={"message": "rate limited"})])
    records = search_europe_pmc("query", http=http)
    assert records == []
    assert http.calls[0]["url"].startswith("https://www.ebi.ac.uk")
