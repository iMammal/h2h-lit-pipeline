from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from h2h_lit.http import RequestsHttpClient
from h2h_lit.pagination import RetryPolicy
from h2h_lit.retrieval import RetrievalQuerySpec, execute_paginated_retrieval_run
from h2h_lit.review import RetrievalCompletionStatus
from tests.fake_http import FakeHttp, FakeResponse

FIXTURES = Path(__file__).parent / "fixtures" / "ieee"


class Clock:
    def __init__(self, second: int = 0):
        self.value = datetime(2026, 8, 31, 12, 0, second, tzinfo=UTC)

    def __call__(self) -> str:
        result = self.value.isoformat()
        self.value += timedelta(seconds=1)
        return result


def _payload(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _spec() -> RetrievalQuerySpec:
    return RetrievalQuerySpec(
        "IEEEXplore",
        '(("visual analytics") AND (biology))',
        "ieee-production-query-v1",
        limit=2,
        fields=["article_number", "doi", "title", "abstract"],
        filters={"publication_year": "2010_2026"},
        metadata={
            "query_parameter": "querytext",
            "sort_field": "article_number",
            "sort_order": "asc",
        },
        credentials={"api_key": "offline-test-key"},
    )


def test_ieee_multi_page_total_reconciliation_and_provenance(tmp_path):
    http = FakeHttp(
        [FakeResponse(payload=_payload("page1.json")), FakeResponse(payload=_payload("page2.json"))]
    )
    dataset = execute_paginated_retrieval_run(
        run_id="run:ieee",
        queries=[_spec()],
        http_clients={"IEEEXplore": http},
        checkpoint_dir=tmp_path / "ieee",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert [call["params"]["start_record"] for call in http.calls] == [1, 3]
    assert all(call["params"]["max_records"] == 2 for call in http.calls)
    assert dataset.source_queries[0].source_reported_total == 3
    assert dataset.source_queries[0].completion_proof == "ieee_totalfound_reconciled"
    assert dataset.retrieval_pages[0].metadata["totalsearched"] == 100
    assert dataset.retrieval_attempts[0].raw_response_hash
    assert dataset.retrieval_attempts[0].request_params["querytext"] == _spec().query_text
    assert dataset.retrieval_attempts[0].request_params["sort_field"] == "article_number"
    assert dataset.retrieval_attempts[0].request_params["apikey"] == "<redacted>"
    assert [item.source_identifier for item in dataset.occurrences] == ["1001", "1002", "1003"]
    assert dataset.occurrences[0].record.authors == ["A. Author", "B. Author"]
    assert dataset.source_queries[0].content_policy == {
        "abstract": "external_llm_use_unresolved"
    }
    assert dataset.occurrences[0].record.original_metadata["text_field_provenance"][
        "abstract"
    ]["content_policy"] == "external_llm_use_unresolved"
    persisted = (tmp_path / "ieee" / "review_dataset.json").read_text()
    assert "offline-test-key" not in persisted
    assert "<redacted>" in persisted


def test_ieee_exact_total_mismatch_and_malformed_items_cannot_complete(tmp_path):
    mismatch = _payload("page2.json")
    mismatch["totalfound"] = 2
    dataset = execute_paginated_retrieval_run(
        run_id="run:ieee-mismatch",
        queries=[_spec()],
        http_clients={"IEEEXplore": FakeHttp([FakeResponse(payload=mismatch)])},
        checkpoint_dir=tmp_path / "mismatch",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )
    assert dataset.retrieval_runs[0].completion_status is RetrievalCompletionStatus.FAILED
    assert dataset.retrieval_runs[0].retrieval_cutoff_date is None

    malformed = execute_paginated_retrieval_run(
        run_id="run:ieee-malformed",
        queries=[_spec()],
        http_clients={"IEEEXplore": FakeHttp([FakeResponse(payload=_payload("malformed.json"))])},
        checkpoint_dir=tmp_path / "malformed",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )
    assert len(malformed.occurrences) == 2
    assert all(item.metadata["parser_incomplete"] for item in malformed.occurrences)
    assert malformed.source_queries[0].completion_status is RetrievalCompletionStatus.FAILED


def test_ieee_opt_in_mutable_totals_record_multiple_changes_and_exhaustion(
    tmp_path,
):
    spec = _spec()
    spec.metadata["mutable_provider_totals"] = True
    responses = [
        {
            "total_records": 6,
            "articles": [
                {"article_number": "1", "title": "One"},
                {"article_number": "2", "title": "Two"},
            ],
        },
        {
            "total_records": 7,
            "articles": [
                {"article_number": "3", "title": "Three"},
                {"article_number": "4", "title": "Four"},
            ],
        },
        {
            "total_records": 5,
            "articles": [{"article_number": "5", "title": "Five"}],
        },
    ]

    dataset = execute_paginated_retrieval_run(
        run_id="run:ieee-mutable-totals",
        queries=[spec],
        http_clients={
            "IEEEXplore": FakeHttp(
                [FakeResponse(payload=payload) for payload in responses]
            )
        },
        checkpoint_dir=tmp_path / "mutable-totals",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert dataset.retrieval_runs[0].completion_status is RetrievalCompletionStatus.COMPLETE
    assert [
        page.metadata["provider_total_observation"]
        for page in dataset.retrieval_pages
    ] == [6, 7, 5]
    assert all(not page.total_is_exact for page in dataset.retrieval_pages)
    assert dataset.source_queries[0].total_is_exact is False
    assert dataset.source_queries[0].completion_proof == (
        "ieee_current_total_exhaustion_observed"
    )
    assert [item.source_identifier for item in dataset.occurrences] == [
        "1",
        "2",
        "3",
        "4",
        "5",
    ]


def test_ieee_short_nonterminal_page_advances_to_next_request_window(tmp_path):
    spec = _spec()
    spec.metadata["mutable_provider_totals"] = True
    responses = [
        {
            "total_records": 6,
            "articles": [
                {"article_number": "1", "title": "One"},
                {"article_number": "2", "title": "Two"},
            ],
        },
        {
            "total_records": 7,
            "articles": [{"article_number": "3", "title": "Three"}],
        },
        {
            "total_records": 5,
            "articles": [{"article_number": "5", "title": "Five"}],
        },
    ]
    http = FakeHttp([FakeResponse(payload=payload) for payload in responses])

    dataset = execute_paginated_retrieval_run(
        run_id="run:ieee-short-window",
        queries=[spec],
        http_clients={"IEEEXplore": http},
        checkpoint_dir=tmp_path / "short-window",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    assert [call["params"]["start_record"] for call in http.calls] == [1, 3, 5]
    assert [page.returned_item_count for page in dataset.retrieval_pages] == [2, 1, 1]
    assert dataset.retrieval_pages[1].next_state == {"start_record": 5}
    assert dataset.retrieval_pages[1].terminal is False
    assert dataset.retrieval_pages[2].next_state is None
    assert dataset.retrieval_pages[2].terminal is True
    assert [
        page.metadata["provider_total_observation"]
        for page in dataset.retrieval_pages
    ] == [6, 7, 5]
    assert dataset.retrieval_runs[0].completion_status is RetrievalCompletionStatus.COMPLETE


def test_ieee_retry_and_interrupted_checkpoint_resume(tmp_path):
    retry_http = FakeHttp(
        [
            FakeResponse(payload={"error": "rate"}, status_code=429, headers={"Retry-After": "0"}),
            FakeResponse(payload={"articles": [], "totalfound": 0, "totalsearched": 0}),
        ]
    )
    retried = execute_paginated_retrieval_run(
        run_id="run:ieee-retry",
        queries=[_spec()],
        http_clients={"IEEEXplore": retry_http},
        checkpoint_dir=tmp_path / "retry",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0),
        retry_sleep=lambda _: None,
    )
    assert len(retried.retrieval_attempts) == 2
    assert retried.retrieval_attempts[1].retry_of_attempt_id == retried.retrieval_attempts[0].attempt_id

    class InterruptedHttp:
        def get(self, *args, **kwargs):
            raise KeyboardInterrupt

    checkpoint = tmp_path / "resume"
    with pytest.raises(KeyboardInterrupt):
        execute_paginated_retrieval_run(
            run_id="run:ieee-resume",
            queries=[_spec()],
            http_clients={"IEEEXplore": InterruptedHttp()},
            checkpoint_dir=checkpoint,
            timestamp=Clock(),
            retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0),
        )
    resumed = execute_paginated_retrieval_run(
        run_id="run:ieee-resume",
        queries=[_spec()],
        http_clients={
            "IEEEXplore": FakeHttp(
                [FakeResponse(payload={"articles": [], "totalfound": 0, "totalsearched": 0})]
            )
        },
        checkpoint_dir=checkpoint,
        resume=True,
        timestamp=Clock(second=30),
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0),
        retry_sleep=lambda _: None,
    )
    assert resumed.retrieval_runs[0].completion_status is RetrievalCompletionStatus.COMPLETE
    assert len(resumed.retrieval_attempts) == 2


def test_ieee_live_transport_fails_at_missing_credential_boundary(tmp_path):
    spec = _spec()
    spec.credentials = {}
    with pytest.raises(ValueError, match="credential is required"):
        execute_paginated_retrieval_run(
            run_id="run:ieee-no-credential",
            queries=[spec],
            http_clients={"IEEEXplore": RequestsHttpClient(session=object())},
            checkpoint_dir=tmp_path / "no-credential",
        )


def test_ieee_page_size_and_request_fields_are_frozen(tmp_path):
    spec = _spec()
    spec.limit = 201
    with pytest.raises(ValueError, match="exceeds supported maximum"):
        execute_paginated_retrieval_run(
            run_id="run:ieee-too-large",
            queries=[spec],
            http_clients={"IEEEXplore": FakeHttp([])},
            checkpoint_dir=tmp_path / "too-large",
        )
