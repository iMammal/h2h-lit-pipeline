from pathlib import Path

from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.pdfs.discovery import (
    PdfCandidate,
    arxiv_pdf_from_url,
    discover_pdf_candidates,
    europe_pmc_pdf_for_doi,
    is_open_access,
    semantic_scholar_pdf_for_doi,
    unpaywall_pdf_for_doi,
)
from h2h_lit.pdfs.download import download_first_pdf, extract_pdf_url_from_html, fetch_pdf_bytes
from tests.fake_http import FakeHttp, FakeResponse


def test_oa_state_interpretation_from_record_and_bib_fields():
    assert is_open_access(LiteratureRecord(title="A", is_open_access=True)) is True
    assert is_open_access({"note": "Source: CrossRef, OpenAccess: True"}) is True
    assert is_open_access({"note": "Source: CrossRef, OpenAccess: False"}) is False


def test_arxiv_pdf_resolution_from_abs_url():
    assert arxiv_pdf_from_url("https://arxiv.org/abs/2401.00001") == "https://arxiv.org/pdf/2401.00001.pdf"
    assert arxiv_pdf_from_url("https://example.test/paper") is None


def test_semantic_scholar_pdf_resolution_success_and_missing():
    ok = FakeHttp([FakeResponse(payload={"openAccessPdf": {"url": "https://s2.test/paper.pdf"}})])
    url, event = semantic_scholar_pdf_for_doi("10.1000/s2", http=ok)
    assert url == "https://s2.test/paper.pdf"
    assert event.status == ProcessingStatus.OK

    missing = FakeHttp([FakeResponse(payload={"openAccessPdf": None})])
    url, event = semantic_scholar_pdf_for_doi("10.1000/s2", http=missing)
    assert url is None
    assert event.status == ProcessingStatus.SKIPPED


def test_unpaywall_pdf_resolution_rate_limit_and_success():
    limited = FakeHttp([FakeResponse(status_code=429, payload={"error": "rate limit"})])
    url, event = unpaywall_pdf_for_doi("10.1000/u", http=limited)
    assert url is None
    assert event.status == ProcessingStatus.FAILED
    assert event.errors == ["HTTP 429"]

    ok = FakeHttp([FakeResponse(payload={"best_oa_location": {"url_for_pdf": "https://upw.test/a.pdf"}})])
    url, event = unpaywall_pdf_for_doi("10.1000/u", http=ok, email="person@example.test")
    assert url == "https://upw.test/a.pdf"
    assert ok.calls[0]["params"]["email"] == "person@example.test"


def test_europe_pmc_resolution_uses_pmcid_before_full_text_urls():
    payload = {"resultList": {"result": [{"pmcid": "PMC123", "fullTextUrlList": {"fullTextUrl": []}}]}}
    url, event = europe_pmc_pdf_for_doi("10.1000/e", http=FakeHttp([FakeResponse(payload=payload)]))
    assert url == "https://europepmc.org/articles/PMC123/pdf"
    assert event.status == ProcessingStatus.OK


def test_discover_pdf_candidates_orders_source_specific_direct_then_fallbacks():
    record = LiteratureRecord(
        title="S2 paper",
        doi="10.1000/s2",
        source_database="SemanticScholar",
        source_url="https://sem.test/paper",
        pdf_url="https://record.test/direct.pdf",
    )
    http = FakeHttp(
        [
            FakeResponse(payload={"openAccessPdf": {"url": "https://s2.test/source.pdf"}}),
            FakeResponse(payload={"openAccessPdf": {"url": "https://s2.test/source.pdf"}}),
            FakeResponse(payload={"best_oa_location": {"url_for_pdf": "https://upw.test/a.pdf"}}),
            FakeResponse(payload={"esearchresult": {"idlist": []}}),
            FakeResponse(payload={"resultList": {"result": []}}),
        ]
    )
    result = discover_pdf_candidates(record, http=http)
    assert result.urls == [
        "https://s2.test/source.pdf",
        "https://record.test/direct.pdf",
        "https://upw.test/a.pdf",
    ]


def test_extract_pdf_url_from_html_supports_relative_links():
    html = '<html><a href="/files/paper.pdf">PDF</a></html>'
    assert extract_pdf_url_from_html(html, "https://publisher.test/page") == "https://publisher.test/files/paper.pdf"


def test_fetch_pdf_bytes_validates_magic_not_only_content_type():
    false_pdf = FakeHttp([FakeResponse(headers={"content-type": "application/pdf"}, content=b"not a pdf")])
    content, event = fetch_pdf_bytes("https://x.test/fake.pdf", http=false_pdf)
    assert content is None
    assert "missing %PDF- magic" in event.errors[0]

    valid_pdf = FakeHttp([FakeResponse(headers={"content-type": "application/octet-stream"}, content=b"%PDF-1.4\nbody")])
    content, event = fetch_pdf_bytes("https://x.test/real.pdf", http=valid_pdf)
    assert content == b"%PDF-1.4\nbody"
    assert event.status == ProcessingStatus.OK


def test_fetch_pdf_bytes_follows_landing_page_pdf_link():
    http = FakeHttp(
        [
            FakeResponse(
                headers={"content-type": "text/html"},
                content=b'<html><a href="paper.pdf">download</a></html>',
                url="https://publisher.test/article",
            ),
            FakeResponse(headers={"content-type": "application/pdf"}, content=b"%PDF-1.7\nbody"),
        ]
    )
    content, event = fetch_pdf_bytes("https://publisher.test/article", http=http)
    assert content == b"%PDF-1.7\nbody"
    assert event.output_id == "https://publisher.test/paper.pdf"


def test_download_first_pdf_tries_candidates_until_success(tmp_path: Path):
    candidates = [
        PdfCandidate("https://x.test/bad", "test", 0, "bad"),
        PdfCandidate("https://x.test/good", "test", 1, "good"),
    ]
    http = FakeHttp(
        [
            FakeResponse(headers={"content-type": "application/pdf"}, content=b"bad"),
            FakeResponse(headers={"content-type": "application/pdf"}, content=b"%PDF-1.7\nok"),
        ]
    )
    result = download_first_pdf(candidates, output_dir=tmp_path, filename_base="Paper / Name", http=http)
    assert result.status == ProcessingStatus.OK
    assert result.chosen_url == "https://x.test/good"
    assert result.output_path.read_bytes() == b"%PDF-1.7\nok"


def test_download_first_pdf_reports_exhausted_candidates(tmp_path: Path):
    candidates = [PdfCandidate("https://x.test/bad", "test", 0, "bad")]
    http = FakeHttp([FakeResponse(status_code=404, payload={})])
    result = download_first_pdf(candidates, output_dir=tmp_path, filename_base="Missing", http=http)
    assert result.status == ProcessingStatus.FAILED
    assert "HTTP 404" in result.errors

