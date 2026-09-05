from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from h2h_lit.models import ProcessingStatus
from h2h_lit.pagination import RetryPolicy
from h2h_lit.prisma import reconcile_prisma
from h2h_lit.retrieval import RetrievalQuerySpec, execute_paginated_retrieval_run
from h2h_lit.review import RetrievalAttemptStatus, RetrievalCompletionStatus
from h2h_lit.sources.crossref import PAGINATOR as CROSSREF_PAGINATOR
from tests.fake_http import FakeHttp, FakeResponse


class Clock:
    def __init__(self):
        self.value = datetime(2026, 8, 31, tzinfo=UTC)

    def __call__(self) -> str:
        result = self.value.isoformat()
        self.value += timedelta(seconds=1)
        return result


def _crossref_item(identifier: str, *, title: str | None = None) -> dict:
    item = {"DOI": identifier, "container-title": ["Venue"]}
    if title is not None:
        item["title"] = [title]
    return item


def _crossref_page(items: list[object], *, total: int, cursor: str = "next") -> dict:
    return {
        "message": {
            "items": items,
            "total-results": total,
            "next-cursor": cursor,
            "type": "work-list",
            "version": "1.0.0",
        }
    }


def _run(tmp_path, source: str, responses, spec: RetrievalQuerySpec, **kwargs):
    return execute_paginated_retrieval_run(
        run_id=f"run:{source.lower()}",
        queries=[spec],
        http_clients={source: FakeHttp(responses)},
        checkpoint_dir=tmp_path / source,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=kwargs.pop("max_attempts", 1)),
        retry_sleep=lambda _: None,
        **kwargs,
    )


def test_pubmed_freezes_id_manifest_and_fetches_deterministic_batches(tmp_path):
    search = b"""
    <eSearchResult><Count>3</Count><QueryKey>1</QueryKey><WebEnv>env</WebEnv>
      <IdList><Id>1</Id><Id>2</Id><Id>3</Id></IdList>
      <QueryTranslation>cells[All Fields]</QueryTranslation>
    </eSearchResult>"""

    def fetch(*pmids: str) -> bytes:
        articles = "".join(
            f"<PubmedArticle><MedlineCitation><PMID>{pmid}</PMID><Article>"
            f"<ArticleTitle>Paper {pmid}</ArticleTitle></Article></MedlineCitation>"
            f"</PubmedArticle>" for pmid in pmids
        )
        return f"<PubmedArticleSet>{articles}</PubmedArticleSet>".encode()

    dataset_http = FakeHttp(
        [FakeResponse(content=search), FakeResponse(content=fetch("1", "2")),
         FakeResponse(content=fetch("3"))]
    )
    dataset = execute_paginated_retrieval_run(
        run_id="run:pubmed",
        queries=[RetrievalQuerySpec("PubMed", "cells", "pubmed-v2", limit=2)],
        http_clients={"PubMed": dataset_http},
        checkpoint_dir=tmp_path / "PubMed",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        retry_sleep=lambda _: None,
    )

    assert [page.returned_item_count for page in dataset.retrieval_pages] == [0, 2, 1]
    assert dataset.source_queries[0].source_reported_total == 3
    assert dataset.source_queries[0].completion_proof == "pubmed_exact_id_manifest_fetched"
    assert [item.source_identifier for item in dataset.occurrences] == ["1", "2", "3"]
    assert dataset.retrieval_runs[0].retrieval_cutoff_date == "2026-08-31"
    assert [call["method"] for call in dataset_http.calls] == ["POST", "GET", "GET"]


def test_europe_pmc_cursor_chain_is_complete_and_auditable(tmp_path):
    pages = [
        {
            "hitCount": 3,
            "nextCursorMark": "cursor-2",
            "nextPageUrl": "https://example.test/page2",
            "resultList": {"result": [{"id": "E1", "title": "One"}, {"id": "E2"}]},
        },
        {
            "hitCount": 3,
            "resultList": {"result": [{"id": "E3", "title": "Three"}]},
        },
    ]
    http = FakeHttp([FakeResponse(payload=item) for item in pages])
    dataset = execute_paginated_retrieval_run(
        run_id="run:epmc",
        queries=[RetrievalQuerySpec("EuropePMC", "cells", "epmc-v2", limit=2)],
        http_clients={"EuropePMC": http},
        checkpoint_dir=tmp_path / "epmc",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert [call["params"]["cursorMark"] for call in http.calls] == ["*", "cursor-2"]
    assert dataset.source_queries[0].result_count == 3
    assert dataset.retrieval_pages[-1].completion_proof == "europe_pmc_cursor_exhausted"


def test_crossref_preserves_incomplete_and_malformed_raw_items(tmp_path):
    dataset = _run(
        tmp_path,
        "CrossRef",
        [FakeResponse(payload=_crossref_page([
            _crossref_item("10.1/one"), "unparsed scalar"
        ], total=2))],
        RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=3),
    )

    assert len(dataset.occurrences) == 2
    assert dataset.occurrences[0].record.title == ""
    assert dataset.occurrences[1].metadata["parser_incomplete"] is True
    assert dataset.retrieval_pages[0].returned_item_count == 2


def test_crossref_cursor_chain_records_exact_sent_requests(tmp_path):
    http = FakeHttp([
        FakeResponse(payload=_crossref_page([
            _crossref_item("10.1/one", title="One"),
            _crossref_item("10.1/two", title="Two"),
        ], total=3, cursor="cursor-2")),
        FakeResponse(payload=_crossref_page([
            _crossref_item("10.1/three", title="Three")
        ], total=3, cursor="unused")),
    ])
    dataset = execute_paginated_retrieval_run(
        run_id="run:crossref-pages",
        queries=[RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)],
        http_clients={"CrossRef": http},
        checkpoint_dir=tmp_path / "crossref-pages",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert [call["params"]["cursor"] for call in http.calls] == ["*", "cursor-2"]
    assert [attempt.request_params["cursor"] for attempt in dataset.retrieval_attempts] == [
        "*", "cursor-2"
    ]
    assert all(attempt.actual_request_url.startswith("https://api.crossref.org/works?")
               for attempt in dataset.retrieval_attempts)
    assert dataset.source_queries[0].completion_status is RetrievalCompletionStatus.COMPLETE


def test_semantic_scholar_requires_frozen_mode_and_redacts_api_key(tmp_path):
    spec = RetrievalQuerySpec(
        "SemanticScholar", "cells", "s2-v2", limit=2,
        pagination_mode="relevance", credentials={"api_key": "not-persisted"}
    )
    pages = [
        {"total": 3, "next": 2, "data": [
            {"paperId": "S1", "title": "One"}, {"paperId": "S2", "title": "Two"}
        ]},
        {"total": 3, "data": [{"paperId": "S3", "title": "Three"}]},
    ]
    http = FakeHttp([FakeResponse(payload=item) for item in pages])
    dataset = execute_paginated_retrieval_run(
        run_id="run:s2",
        queries=[spec],
        http_clients={"SemanticScholar": http},
        checkpoint_dir=tmp_path / "s2",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert [call["params"]["offset"] for call in http.calls] == [0, 2]
    assert http.calls[0]["headers"] == {"x-api-key": "not-persisted"}
    assert dataset.retrieval_attempts[0].request_headers == {"x-api-key": "<redacted>"}
    assert "not-persisted" not in (tmp_path / "s2" / "review_dataset.json").read_text()

    with pytest.raises(ValueError, match="mode must be explicitly frozen"):
        execute_paginated_retrieval_run(
            run_id="run:s2-missing-mode",
            queries=[RetrievalQuerySpec("SemanticScholar", "cells", "s2-v2")],
            http_clients={"SemanticScholar": FakeHttp([])},
            checkpoint_dir=tmp_path / "s2-missing-mode",
        )

    with pytest.raises(ValueError, match="must start from adapter initial state"):
        execute_paginated_retrieval_run(
            run_id="run:caller-cursor",
            queries=[RetrievalQuerySpec(
                "CrossRef", "cells", "crossref-v2", cursor="caller-supplied"
            )],
            http_clients={"CrossRef": FakeHttp([])},
            checkpoint_dir=tmp_path / "caller-cursor",
        )


def test_semantic_scholar_bulk_uses_returned_tokens_without_switching_modes(tmp_path):
    http = FakeHttp([
        FakeResponse(payload={
            "total": 5000, "token": "token-2",
            "data": [{"paperId": "S1", "title": "One"}],
        }),
        FakeResponse(payload={
            "total": 5001, "data": [{"paperId": "S2", "title": "Two"}],
        }),
    ])
    dataset = execute_paginated_retrieval_run(
        run_id="run:s2-bulk",
        queries=[RetrievalQuerySpec(
            "SemanticScholar", "cells", "s2-bulk-v1", limit=1, pagination_mode="bulk"
        )],
        http_clients={"SemanticScholar": http},
        checkpoint_dir=tmp_path / "s2-bulk",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert http.calls[0]["url"].endswith("/paper/search/bulk")
    assert "token" not in http.calls[0]["params"]
    assert http.calls[1]["params"]["token"] == "token-2"
    assert dataset.retrieval_pages[-1].completion_proof == (
        "semantic_scholar_bulk_token_exhausted"
    )
    assert dataset.source_queries[0].total_is_exact is False


def _arxiv_feed(*ids: str, total: int, start: int) -> bytes:
    entries = "".join(
        f"<entry><id>http://arxiv.org/abs/{identifier}</id><title>{identifier}</title>"
        f"<summary>Abstract</summary></entry>" for identifier in ids
    )
    return f"""<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
      <updated>2026-08-31T00:00:00Z</updated>
      <opensearch:totalResults>{total}</opensearch:totalResults>
      <opensearch:startIndex>{start}</opensearch:startIndex>
      <opensearch:itemsPerPage>{len(ids)}</opensearch:itemsPerPage>{entries}</feed>""".encode()


def test_arxiv_uses_response_confirmed_offsets_and_stable_snapshot(tmp_path):
    http = FakeHttp([
        FakeResponse(content=_arxiv_feed("a1", "a2", total=3, start=0)),
        FakeResponse(content=_arxiv_feed("a3", total=3, start=2)),
    ])
    dataset = execute_paginated_retrieval_run(
        run_id="run:arxiv",
        queries=[RetrievalQuerySpec("arXiv", "cells", "arxiv-v2", limit=2)],
        http_clients={"arXiv": http},
        checkpoint_dir=tmp_path / "arxiv",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert [call["params"]["start"] for call in http.calls] == [0, 2]
    assert dataset.source_queries[0].source_reported_total == 3
    assert dataset.source_queries[0].completion_proof == "arxiv_exact_total_reached"


@pytest.mark.parametrize(
    ("headers", "present", "value"),
    [({}, False, None), ({"Retry-After": "120"}, True, "120")],
)
def test_arxiv_429_pauses_once_and_records_retry_after_evidence(
    tmp_path, headers, present, value
):
    http = FakeHttp(
        [FakeResponse(status_code=429, headers=headers, content=b"Rate exceeded.")]
    )
    dataset = execute_paginated_retrieval_run(
        run_id=f"run:arxiv-rate-limit:{present}",
        queries=[RetrievalQuerySpec("arXiv", "cells", "arxiv-v2", limit=2)],
        http_clients={"arXiv": http},
        checkpoint_dir=tmp_path / f"arxiv-rate-limit-{present}",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=3),
        retry_sleep=lambda _: pytest.fail("arXiv 429 must pause without retrying"),
        pause_status_codes=frozenset({429}),
    )

    assert len(http.calls) == 1
    run = dataset.retrieval_runs[0]
    query = dataset.source_queries[0]
    attempt = dataset.retrieval_attempts[0]
    expected = {
        "source_database": "arXiv",
        "http_status": 429,
        "retry_after_header_present": present,
        "retry_after": value,
    }
    assert run.completion_status is RetrievalCompletionStatus.RUNNING
    assert run.metadata["pause_state"] == "PROVIDER_RATE_LIMIT"
    assert run.metadata["pause_metadata"] == expected
    assert query.metadata["pause_metadata"] == expected
    assert attempt.metadata["provider_pause"] == expected
    assert attempt.error == "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
    assert attempt.raw_response_path and attempt.raw_response_hash


def test_arxiv_rate_limit_pause_resumes_same_frozen_initial_request(tmp_path):
    checkpoint = tmp_path / "arxiv-rate-limit-resume"
    spec = RetrievalQuerySpec("arXiv", "cells", "arxiv-v2", limit=2)
    execute_paginated_retrieval_run(
        run_id="run:arxiv-rate-limit-resume",
        queries=[spec],
        http_clients={"arXiv": FakeHttp([FakeResponse(status_code=429)])},
        checkpoint_dir=checkpoint,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        pause_status_codes=frozenset({429}),
    )
    resumed_http = FakeHttp(
        [FakeResponse(content=_arxiv_feed("a1", total=1, start=0))]
    )
    completed = execute_paginated_retrieval_run(
        run_id="run:arxiv-rate-limit-resume",
        queries=[spec],
        http_clients={"arXiv": resumed_http},
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        pause_status_codes=frozenset({429}),
    )

    assert resumed_http.calls[0]["params"]["start"] == 0
    assert [item.attempt_number for item in completed.retrieval_attempts] == [1, 2]
    assert completed.retrieval_runs[0].completion_status is (
        RetrievalCompletionStatus.COMPLETE
    )


def test_arxiv_successful_response_validation_remains_fail_closed(tmp_path):
    malformed = execute_paginated_retrieval_run(
        run_id="run:arxiv-malformed-after-rate-limit-change",
        queries=[RetrievalQuerySpec("arXiv", "cells", "arxiv-v2", limit=2)],
        http_clients={"arXiv": FakeHttp([FakeResponse(content=b"not XML")])},
        checkpoint_dir=tmp_path / "arxiv-malformed-after-rate-limit-change",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        pause_status_codes=frozenset({429}),
    )

    assert malformed.retrieval_runs[0].completion_status is (
        RetrievalCompletionStatus.FAILED
    )
    assert malformed.source_queries[0].completion_status is (
        RetrievalCompletionStatus.FAILED
    )
    assert "ParseError" in malformed.retrieval_attempts[0].error


@pytest.mark.parametrize("failure_kind", ["cross_page_duplicate", "pagination_mismatch"])
def test_arxiv_identity_and_pagination_validation_remain_fail_closed(
    tmp_path, failure_kind
):
    if failure_kind == "cross_page_duplicate":
        responses = [
            FakeResponse(content=_arxiv_feed("a1", "a2", total=3, start=0)),
            FakeResponse(content=_arxiv_feed("a2", total=3, start=2)),
        ]
    else:
        responses = [FakeResponse(content=_arxiv_feed("a1", total=1, start=1))]
    dataset = execute_paginated_retrieval_run(
        run_id=f"run:arxiv-validation:{failure_kind}",
        queries=[RetrievalQuerySpec("arXiv", "cells", "arxiv-v2", limit=2)],
        http_clients={"arXiv": FakeHttp(responses)},
        checkpoint_dir=tmp_path / f"arxiv-validation-{failure_kind}",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        pause_status_codes=frozenset({429}),
    )

    assert dataset.retrieval_runs[0].completion_status is (
        RetrievalCompletionStatus.FAILED
    )
    errors = dataset.source_queries[0].errors
    assert errors
    if failure_kind == "cross_page_duplicate":
        assert "repeated native identifiers across pages" in errors[0]
    else:
        assert "startIndex does not match" in errors[0]


@pytest.mark.parametrize(
    ("source", "response", "spec"),
    [
        (
            "PubMed",
            FakeResponse(content=b"<eSearchResult><Count>10001</Count><IdList /></eSearchResult>"),
            RetrievalQuerySpec("PubMed", "broad", "pubmed-v2"),
        ),
        (
            "SemanticScholar",
            FakeResponse(payload={"total": 1001, "data": [{"paperId": "S1"}]}),
            RetrievalQuerySpec(
                "SemanticScholar", "broad", "s2-v2", pagination_mode="relevance"
            ),
        ),
        (
            "arXiv",
            FakeResponse(content=_arxiv_feed("a1", total=30001, start=0)),
            RetrievalQuerySpec("arXiv", "broad", "arxiv-v2"),
        ),
    ],
)
def test_unsupported_result_windows_are_truncated_without_cutoff(
    tmp_path, source, response, spec
):
    dataset = _run(tmp_path, source, [response], spec)

    query = dataset.source_queries[0]
    run = dataset.retrieval_runs[0]
    assert query.completion_status is RetrievalCompletionStatus.TRUNCATED
    assert run.completion_status is RetrievalCompletionStatus.TRUNCATED
    assert run.retrieval_cutoff_date is None
    assert query.errors


def test_retry_preserves_failed_attempt_and_reuses_identical_page_state(tmp_path):
    sleeps: list[float] = []
    http = FakeHttp([
        FakeResponse(status_code=429, headers={"Retry-After": "4"}, payload={"error": "rate"}),
        FakeResponse(payload=_crossref_page([], total=0)),
    ])
    dataset = execute_paginated_retrieval_run(
        run_id="run:retry",
        queries=[RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)],
        http_clients={"CrossRef": http},
        checkpoint_dir=tmp_path / "retry",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=1),
        retry_sleep=sleeps.append,
    )

    assert [item.status for item in dataset.retrieval_attempts] == [
        RetrievalAttemptStatus.FAILED, RetrievalAttemptStatus.SUCCEEDED
    ]
    assert dataset.retrieval_attempts[1].retry_of_attempt_id == dataset.retrieval_attempts[0].attempt_id
    assert dataset.retrieval_attempts[0].request_hash == dataset.retrieval_attempts[1].request_hash
    assert dataset.retrieval_attempts[0].response_headers["Retry-After"] == "4"
    assert sleeps == [4.0]


def test_exact_total_mismatch_fails_completion_and_cannot_set_cutoff(tmp_path):
    dataset = _run(
        tmp_path,
        "CrossRef",
        [FakeResponse(payload=_crossref_page([
            _crossref_item("10.1/one", title="One")
        ], total=2))],
        RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2),
    )

    assert dataset.source_queries[0].completion_status is RetrievalCompletionStatus.FAILED
    assert "does not match exact source total" in dataset.source_queries[0].errors[0]
    assert dataset.retrieval_runs[0].retrieval_cutoff_date is None


def test_query_plan_change_refuses_resume(tmp_path):
    checkpoint = tmp_path / "plan-change"
    original = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    execute_paginated_retrieval_run(
        run_id="run:plan-change",
        queries=[original],
        http_clients={"CrossRef": FakeHttp([
            FakeResponse(payload=_crossref_page([], total=0))
        ])},
        checkpoint_dir=checkpoint,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    with pytest.raises(ValueError, match="query plan/version does not match"):
        execute_paginated_retrieval_run(
            run_id="run:plan-change",
            queries=[RetrievalQuerySpec("CrossRef", "changed", "crossref-v2", limit=2)],
            http_clients={"CrossRef": FakeHttp([])},
            checkpoint_dir=checkpoint,
            resume=True,
            timestamp=Clock(),
            retry_policy=RetryPolicy(max_attempts=1),
        )


def test_corrupted_raw_response_is_rejected_during_resume(tmp_path):
    checkpoint = tmp_path / "corrupt-response"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    adapter = InterruptingParser()
    with pytest.raises(KeyboardInterrupt):
        execute_paginated_retrieval_run(
            run_id="run:corrupt-response",
            queries=[spec],
            http_clients={"CrossRef": FakeHttp([
                FakeResponse(payload=_crossref_page([], total=0))
            ])},
            checkpoint_dir=checkpoint,
            timestamp=Clock(),
            adapters={"CrossRef": adapter},
            retry_policy=RetryPolicy(max_attempts=1),
        )
    response_file = next((checkpoint / "responses").iterdir())
    response_file.write_text("corrupted", encoding="utf-8")

    with pytest.raises(ValueError, match="hash mismatch"):
        execute_paginated_retrieval_run(
            run_id="run:corrupt-response",
            queries=[spec],
            http_clients={"CrossRef": FakeHttp([])},
            checkpoint_dir=checkpoint,
            resume=True,
            timestamp=Clock(),
            adapters={"CrossRef": adapter},
            retry_policy=RetryPolicy(max_attempts=1),
        )


class InterruptBeforeResponse:
    def get(self, url, **kwargs):
        raise KeyboardInterrupt


class InterruptOnSecondRequest(FakeHttp):
    def get(self, url, **kwargs):
        if len(self.calls) == 1:
            self.calls.append({"url": url, **kwargs})
            raise KeyboardInterrupt
        return super().get(url, **kwargs)


def test_resume_retries_started_attempt_without_skipping_page(tmp_path):
    checkpoint = tmp_path / "resume-before-response"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    with pytest.raises(KeyboardInterrupt):
        execute_paginated_retrieval_run(
            run_id="run:resume-before",
            queries=[spec],
            http_clients={"CrossRef": InterruptBeforeResponse()},
            checkpoint_dir=checkpoint,
            timestamp=Clock(),
            retry_policy=RetryPolicy(max_attempts=2),
            retry_sleep=lambda _: None,
        )

    resumed_http = FakeHttp([FakeResponse(payload=_crossref_page([], total=0))])
    dataset = execute_paginated_retrieval_run(
        run_id="run:resume-before",
        queries=[spec],
        http_clients={"CrossRef": resumed_http},
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=2),
        retry_sleep=lambda _: None,
    )

    assert len(resumed_http.calls) == 1
    assert len(dataset.retrieval_attempts) == 2
    assert len(dataset.retrieval_pages) == 1
    assert dataset.retrieval_runs[0].status is ProcessingStatus.OK


def test_resume_after_committed_page_does_not_repeat_or_skip_records(tmp_path):
    checkpoint = tmp_path / "resume-second-page"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    first_page = FakeResponse(payload=_crossref_page([
        _crossref_item("10.1/one", title="One"),
        _crossref_item("10.1/two", title="Two"),
    ], total=3, cursor="cursor-2"))
    interrupted_http = InterruptOnSecondRequest([first_page])
    with pytest.raises(KeyboardInterrupt):
        execute_paginated_retrieval_run(
            run_id="run:resume-second",
            queries=[spec],
            http_clients={"CrossRef": interrupted_http},
            checkpoint_dir=checkpoint,
            timestamp=Clock(),
            retry_policy=RetryPolicy(max_attempts=2),
            retry_sleep=lambda _: None,
        )

    resumed_http = FakeHttp([FakeResponse(payload=_crossref_page([
        _crossref_item("10.1/three", title="Three")
    ], total=3))])
    dataset = execute_paginated_retrieval_run(
        run_id="run:resume-second",
        queries=[spec],
        http_clients={"CrossRef": resumed_http},
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=2),
        retry_sleep=lambda _: None,
    )

    assert resumed_http.calls[0]["params"]["cursor"] == "cursor-2"
    assert [item.source_identifier for item in dataset.occurrences] == [
        "10.1/one", "10.1/two", "10.1/three"
    ]
    assert len({item.occurrence_id for item in dataset.occurrences}) == 3


def test_request_budget_pauses_cleanly_and_resume_completes(tmp_path):
    checkpoint = tmp_path / "budgeted"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    first_http = FakeHttp(
        [
            FakeResponse(
                payload=_crossref_page(
                    [
                        _crossref_item("10.1/one", title="One"),
                        _crossref_item("10.1/two", title="Two"),
                    ],
                    total=3,
                    cursor="cursor-2",
                )
            )
        ]
    )
    paused = execute_paginated_retrieval_run(
        run_id="run:budgeted",
        queries=[spec],
        http_clients={"CrossRef": first_http},
        checkpoint_dir=checkpoint,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        request_budget=1,
    )

    assert len(first_http.calls) == 1
    assert paused.retrieval_runs[0].completion_status is RetrievalCompletionStatus.RUNNING
    assert paused.retrieval_runs[0].retrieval_cutoff_date is None
    assert paused.retrieval_runs[0].metadata["pause_state"] == (
        "REQUEST_BUDGET_EXHAUSTED"
    )

    second_http = FakeHttp(
            [
                FakeResponse(
                    payload=_crossref_page(
                        [_crossref_item("10.1/three", title="Three")], total=3
                    )
                )
            ]
    )
    completed = execute_paginated_retrieval_run(
        run_id="run:budgeted",
        queries=[spec],
        http_clients={"CrossRef": second_http},
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        request_budget=1,
    )
    assert completed.retrieval_runs[0].completion_status is RetrievalCompletionStatus.COMPLETE
    assert len(completed.occurrences) == 3
    assert len(second_http.calls) == 1


def test_provider_quota_response_is_resumable_not_terminal_failure(tmp_path):
    checkpoint = tmp_path / "provider-quota"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=1)
    paused = execute_paginated_retrieval_run(
        run_id="run:provider-quota",
        queries=[spec],
        http_clients={"CrossRef": FakeHttp([FakeResponse(status_code=429)])},
        checkpoint_dir=checkpoint,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        pause_status_codes=frozenset({429}),
    )
    assert paused.retrieval_runs[0].metadata["pause_state"] == (
        "PROVIDER_QUOTA_EXHAUSTED"
    )
    assert paused.source_queries[0].completion_status is RetrievalCompletionStatus.RUNNING

    completed = execute_paginated_retrieval_run(
        run_id="run:provider-quota",
        queries=[spec],
        http_clients={
            "CrossRef": FakeHttp([FakeResponse(payload=_crossref_page([], total=0))])
        },
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        pause_status_codes=frozenset({429}),
    )
    assert completed.retrieval_runs[0].completion_status is (
        RetrievalCompletionStatus.COMPLETE
    )
    assert [item.attempt_number for item in completed.retrieval_attempts] == [1, 2]


class InterruptingParser:
    source_database = CROSSREF_PAGINATOR.source_database
    strategy = CROSSREF_PAGINATOR.strategy
    version = CROSSREF_PAGINATOR.version

    def __init__(self):
        self.interrupt = True

    def initial_state(self, spec):
        return CROSSREF_PAGINATOR.initial_state(spec)

    def build_request(self, spec, state):
        return CROSSREF_PAGINATOR.build_request(spec, state)

    def parse_response(self, spec, state, response):
        if self.interrupt:
            self.interrupt = False
            raise KeyboardInterrupt
        return CROSSREF_PAGINATOR.parse_response(spec, state, response)


class ChangedAdapter(InterruptingParser):
    version = "changed-version"


def test_resume_refuses_changed_adapter_version(tmp_path):
    checkpoint = tmp_path / "changed-adapter"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    with pytest.raises(KeyboardInterrupt):
        execute_paginated_retrieval_run(
            run_id="run:changed-adapter",
            queries=[spec],
            http_clients={"CrossRef": InterruptBeforeResponse()},
            checkpoint_dir=checkpoint,
            timestamp=Clock(),
            retry_policy=RetryPolicy(max_attempts=2),
        )

    with pytest.raises(ValueError, match="adapter version/strategy"):
        execute_paginated_retrieval_run(
            run_id="run:changed-adapter",
            queries=[spec],
            http_clients={"CrossRef": FakeHttp([])},
            checkpoint_dir=checkpoint,
            resume=True,
            timestamp=Clock(),
            adapters={"CrossRef": ChangedAdapter()},
            retry_policy=RetryPolicy(max_attempts=2),
        )


def test_resume_replays_persisted_response_without_network_call(tmp_path):
    checkpoint = tmp_path / "resume-after-response"
    spec = RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2)
    adapter = InterruptingParser()
    with pytest.raises(KeyboardInterrupt):
        execute_paginated_retrieval_run(
            run_id="run:resume-after",
            queries=[spec],
            http_clients={"CrossRef": FakeHttp([
                FakeResponse(payload=_crossref_page([], total=0))
            ])},
            checkpoint_dir=checkpoint,
            timestamp=Clock(),
            adapters={"CrossRef": adapter},
            retry_policy=RetryPolicy(max_attempts=1),
        )

    no_network = FakeHttp([])
    dataset = execute_paginated_retrieval_run(
        run_id="run:resume-after",
        queries=[spec],
        http_clients={"CrossRef": no_network},
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(),
        adapters={"CrossRef": adapter},
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert no_network.calls == []
    assert dataset.retrieval_attempts[0].status is RetrievalAttemptStatus.SUCCEEDED
    assert dataset.retrieval_runs[0].completion_status is RetrievalCompletionStatus.COMPLETE


def test_prisma_adds_retrieval_completeness_without_changing_arithmetic(tmp_path):
    dataset = _run(
        tmp_path,
        "CrossRef",
        [FakeResponse(payload=_crossref_page([
            _crossref_item("10.1/one", title="One")
        ], total=1))],
        RetrievalQuerySpec("CrossRef", "cells", "crossref-v2", limit=2),
    )

    report = reconcile_prisma(dataset)

    assert report.records_identified == 1
    assert report.records_after_deduplication == 1
    assert report.retrieval_runs_by_completion == {"complete": 1}
    assert report.source_queries_by_completion == {"complete": 1}
    assert report.retrieval_page_count == 1
    assert report.retrieval_attempt_count == 1
    assert report.failed_retrieval_attempt_count == 0
