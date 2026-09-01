from __future__ import annotations

from collections.abc import Callable

import pytest

from h2h_lit.models import ProcessingStatus
from h2h_lit.prisma import reconcile_prisma
from h2h_lit.retrieval import (
    RetrievalQuerySpec,
    execute_retrieval_run,
    load_review_dataset,
    save_review_dataset,
)
from h2h_lit.review import DedupeOutcome, RetrievalRunKind
from tests.fake_http import FakeHttp, FakeResponse

PUBMED_SEARCH = b"<eSearchResult><IdList><Id>123</Id></IdList></eSearchResult>"
PUBMED_FETCH = b"""
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>123</PMID>
      <Article>
        <Journal><Title>Journal Name</Title></Journal>
        <ArticleTitle>Shared Paper</ArticleTitle>
        <Abstract><AbstractText>PubMed abstract.</AbstractText></Abstract>
        <Journal><JournalIssue><PubDate><Year>2024</Year></PubDate></JournalIssue></Journal>
      </Article>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList><ArticleId IdType="doi">10.1000/shared</ArticleId></ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
"""


def _crossref_payload(doi: str = "10.1000/shared") -> dict:
    return {
        "message": {
            "items": [
                {
                    "title": ["Shared Paper"],
                    "abstract": "CrossRef abstract.",
                    "DOI": doi,
                    "published-online": {"date-parts": [[2024, 1, 1]]},
                    "container-title": ["Venue"],
                }
            ]
        }
    }


def _clock(*values: str) -> Callable[[], str]:
    iterator = iter(values)
    return iterator.__next__


def test_multi_source_records_preserve_occurrences_and_deduplicate_by_doi():
    dataset = execute_retrieval_run(
        run_id="run:2026-08-30",
        queries=[
            RetrievalQuerySpec(
                source_database="PubMed",
                query_text="network visual analytics",
                query_version="pubmed-v1",
                page=1,
            ),
            RetrievalQuerySpec(
                source_database="CrossRef",
                query_text="network visual analytics",
                query_version="crossref-v1",
                page=1,
                cursor="cursor-a",
            ),
        ],
        http_clients={
            "PubMed": FakeHttp(
                [FakeResponse(content=PUBMED_SEARCH), FakeResponse(content=PUBMED_FETCH)]
            ),
            "CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload())]),
        },
        timestamp=_clock(
            "2026-08-30T10:00:00+00:00",
            "2026-08-30T10:00:01+00:00",
            "2026-08-30T10:00:02+00:00",
            "2026-08-30T10:00:03+00:00",
        ),
        software_version="test-version",
    )

    assert len(dataset.source_queries) == 2
    assert len(dataset.occurrences) == 2
    assert len(dataset.canonical_records) == 1
    assert len(dataset.duplicate_decisions) == 2
    assert {item.outcome for item in dataset.duplicate_decisions} == {
        DedupeOutcome.UNIQUE,
        DedupeOutcome.DUPLICATE,
    }
    assert dataset.canonical_records[0].occurrence_ids == [
        item.occurrence_id for item in dataset.occurrences
    ]
    assert dataset.source_queries[0].query_version == "pubmed-v1"
    assert dataset.source_queries[1].cursor == "cursor-a"
    assert dataset.occurrences[0].source_identifier == "123"
    assert dataset.occurrences[1].record.doi == "10.1000/shared"
    assert dataset.occurrences[1].record.title == "Shared Paper"
    run = dataset.retrieval_runs[0]
    assert run.kind is RetrievalRunKind.PRIMARY
    assert run.status is ProcessingStatus.OK
    assert run.retrieval_cutoff_date == "2026-08-30"
    assert run.planned_query_ids == run.source_query_ids
    assert run.source_query_ids == [query.query_id for query in dataset.source_queries]


def test_failed_source_and_empty_page_remain_in_run_provenance():
    dataset = execute_retrieval_run(
        run_id="run:failures",
        queries=[
            RetrievalQuerySpec(
                source_database="EuropePMC",
                query_text="query",
                query_version="epmc-v1",
                page=3,
                cursor="failed-cursor",
            ),
            RetrievalQuerySpec(
                source_database="PubMed",
                query_text="no matches",
                query_version="pubmed-v1",
                page=4,
                cursor="empty-cursor",
            ),
        ],
        http_clients={
            "EuropePMC": FakeHttp(
                [FakeResponse(status_code=503, payload={"error": "unavailable"})]
            ),
            "PubMed": FakeHttp(
                [FakeResponse(content=b"<eSearchResult><IdList /></eSearchResult>")]
            ),
        },
        timestamp=_clock(
            "2026-08-30T10:00:00+00:00",
            "2026-08-30T10:00:01+00:00",
            "2026-08-30T10:00:02+00:00",
            "2026-08-30T10:00:03+00:00",
        ),
    )

    failed, empty = dataset.source_queries
    assert failed.status is ProcessingStatus.FAILED
    assert failed.result_count == 0
    assert failed.page == 3
    assert failed.cursor == "failed-cursor"
    assert failed.errors == [
        "SourceRequestError: HTTP 503 from https://example.test/response"
    ]
    assert failed.metadata["response_status_codes"] == [503]
    assert empty.status is ProcessingStatus.OK
    assert empty.result_count == 0
    assert empty.metadata["empty_result"] is True
    assert empty.page == 4
    assert empty.cursor == "empty-cursor"
    assert dataset.occurrences == []
    assert dataset.canonical_records == []
    assert dataset.retrieval_runs[0].status is ProcessingStatus.PARTIAL
    assert dataset.retrieval_runs[0].retrieval_cutoff_date is None

    report = reconcile_prisma(dataset)
    assert report.source_query_count == 2
    assert report.failed_query_count == 1
    assert report.empty_result_query_count == 1
    assert report.records_identified == 0
    assert report.records_after_deduplication == 0


def test_persisted_dataset_is_deterministic_and_round_trips(tmp_path):
    dataset = execute_retrieval_run(
        run_id="run:persisted",
        queries=[
            RetrievalQuerySpec(
                source_database="CrossRef",
                query_text="query",
                query_version="crossref-v2",
                fields=["title", "DOI"],
                filters={"type": "journal-article"},
            )
        ],
        http_clients={
            "CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload("10.1000/one"))])
        },
        timestamp=_clock(
            "2026-08-30T10:00:00+00:00",
            "2026-08-30T10:00:01+00:00",
        ),
    )
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    first_hash = save_review_dataset(first, dataset)
    second_hash = save_review_dataset(second, dataset)
    restored = load_review_dataset(first)

    assert first_hash == second_hash
    assert first.read_bytes() == second.read_bytes()
    assert restored.to_json() == dataset.to_json()
    assert restored.source_queries[0].fields == ["title", "DOI"]
    assert restored.source_queries[0].filters == {
        "limit": 50,
        "type": "journal-article",
    }
    assert restored.retrieval_runs == dataset.retrieval_runs


def test_repeated_runs_have_distinct_query_and_occurrence_lineage():
    query = RetrievalQuerySpec(
        source_database="CrossRef",
        query_text="same query",
        query_version="crossref-v1",
    )
    first = execute_retrieval_run(
        run_id="run:first",
        queries=[query],
        http_clients={"CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload())])},
        timestamp=_clock(
            "2026-08-30T10:00:00+00:00",
            "2026-08-30T10:00:01+00:00",
        ),
    )
    second = execute_retrieval_run(
        run_id="run:second",
        queries=[query],
        http_clients={"CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload())])},
        timestamp=_clock(
            "2026-08-30T10:00:00+00:00",
            "2026-08-30T10:00:01+00:00",
        ),
    )

    assert first.source_queries[0].run_id == "run:first"
    assert second.source_queries[0].run_id == "run:second"
    assert first.source_queries[0].query_id != second.source_queries[0].query_id
    assert first.occurrences[0].occurrence_id != second.occurrences[0].occurrence_id


def test_prisma_counts_reconcile_from_stored_occurrences_and_decisions():
    dataset = execute_retrieval_run(
        run_id="run:prisma",
        queries=[
            RetrievalQuerySpec("PubMed", "query", "pubmed-v1"),
            RetrievalQuerySpec("CrossRef", "query", "crossref-v1"),
        ],
        http_clients={
            "PubMed": FakeHttp(
                [FakeResponse(content=PUBMED_SEARCH), FakeResponse(content=PUBMED_FETCH)]
            ),
            "CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload())]),
        },
        timestamp=_clock(
            "2026-08-30T23:59:58+00:00",
            "2026-08-30T23:59:59+00:00",
            "2026-08-31T00:00:00+00:00",
            "2026-08-31T00:00:01+00:00",
        ),
    )

    report = reconcile_prisma(dataset)

    assert report.run_ids == ["run:prisma"]
    assert report.records_by_source == {"CrossRef": 1, "PubMed": 1}
    assert report.records_identified == 2
    assert report.duplicate_records_removed == 1
    assert report.records_after_deduplication == 1
    assert report.records_identified - report.duplicate_records_removed == 1
    assert report.reconciled is True

    dataset.source_queries[0].result_count = 2
    with pytest.raises(ValueError, match="result_count=2 but has 1 occurrences"):
        reconcile_prisma(dataset)


def test_retrieval_cutoff_is_final_utc_day_of_complete_wave():
    dataset = execute_retrieval_run(
        run_id="run:spans-days",
        queries=[RetrievalQuerySpec("CrossRef", "query", "crossref-v1")],
        http_clients={
            "CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload())])
        },
        timestamp=_clock(
            "2026-08-30T23:59:59-05:00",
            "2026-08-31T05:00:01+00:00",
        ),
        query_plan_version="production-plan-v1",
    )

    run = dataset.retrieval_runs[0]
    assert run.retrieval_cutoff_date == "2026-08-31"
    assert run.query_plan_version == "production-plan-v1"
    assert len(run.query_plan_hash) == 64


def test_successful_run_manifest_cannot_omit_a_planned_query():
    dataset = execute_retrieval_run(
        run_id="run:manifest",
        queries=[RetrievalQuerySpec("CrossRef", "query", "crossref-v1")],
        http_clients={
            "CrossRef": FakeHttp([FakeResponse(payload=_crossref_payload())])
        },
        timestamp=_clock(
            "2026-08-30T10:00:00+00:00",
            "2026-08-30T10:00:01+00:00",
        ),
    )
    dataset.retrieval_runs[0].source_query_ids.clear()

    with pytest.raises(ValueError, match="source query manifest disagrees"):
        dataset.validate()
