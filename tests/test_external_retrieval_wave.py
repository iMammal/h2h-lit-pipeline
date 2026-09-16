from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import h2h_lit.external_retrieval_wave as external_module
import h2h_lit.sources.pubmed as pubmed_module
from h2h_lit.external_retrieval_wave import (
    ACM_RECONCILIATION_PATH,
    ARXIV_MIXED_RECOVERY_STATUS,
    ARXIV_RATE_LIMIT_RECOVERY_STATUS,
    ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
    ARXIV_TRANSPORT_POLICY_RECOVERY_STATUS,
    EUROPE_PMC_TERMINAL_RECOVERY_STATUS,
    IEEE_REPEATED_WINDOW_RECOVERY_STATUS,
    IEEE_TOTAL_DRIFT_RECOVERY_STATUS,
    PREFLIGHT_PATH,
    PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS,
    PUBMED_PARSER_RECOVERY_STATUS,
    READY_STATUS,
    SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS,
    SEMANTIC_CONTROL_5XX_RECOVERY_STATUS,
    SEMANTIC_CONTROL_GATE_PATH,
    SEMANTIC_CONTROL_RECOVERY_GATE_PATH,
    SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS,
    WAVE_PATH,
    ExternalRetrievalWaveError,
    _safe_output_path,
    authorize_arxiv_episode_3_state_reconciliation,
    authorize_arxiv_mixed_state_recovery,
    authorize_arxiv_rate_limit_recovery,
    authorize_arxiv_transport_policy_recovery,
    authorize_europe_pmc_terminal_recovery,
    authorize_ieee_repeated_window_recovery,
    authorize_ieee_total_drift_recovery,
    authorize_pubmed_parser_recovery,
    authorize_pubmed_transport_retry,
    authorize_semantic_scholar_candidate_5xx_recovery,
    authorize_semantic_scholar_control_5xx_recovery,
    authorize_semantic_scholar_native_id_overlap_recovery,
    build_external_retrieval_wave,
    execute_external_source_session,
    preflight_external_retrieval_wave,
    validate_persisted_external_preflight,
)
from h2h_lit.pagination import (
    PaginationError,
    ParsedPage,
    RateLimiter,
    RetryPolicy,
    native_identifier,
)
from h2h_lit.production_prerequisites import (
    EXPECTED_PLAN_HASH,
    EXPECTED_PLAN_RAW_SHA256,
)
from h2h_lit.production_wave import (
    EXTERNAL_IDENTIFICATION_SOURCES_V2,
    ProductionWaveStatus,
    compute_query_plan_hash,
)
from h2h_lit.review import RetrievalAttemptStatus, RetrievalCompletionStatus
from h2h_lit.sources.europe_pmc import (
    EuropePmcPaginator,
    parse_europe_pmc_response,
)
from h2h_lit.sources.ieee_xplore import IeeeXplorePaginator
from tests.fake_http import FakeHttp, FakeResponse

ROOT = Path(__file__).resolve().parents[1]


class Clock:
    def __init__(self):
        self.value = datetime(2026, 9, 4, 12, tzinfo=UTC)

    def __call__(self) -> str:
        value = self.value.isoformat().replace("+00:00", "Z")
        self.value += timedelta(seconds=1)
        return value


@pytest.fixture(scope="module")
def external_wave():
    return build_external_retrieval_wave(root=ROOT)


@pytest.fixture(scope="module")
def external_preflight(external_wave):
    return preflight_external_retrieval_wave(external_wave, root=ROOT)


def test_external_wave_binds_exact_frozen_inventory_and_is_planned_only(
    external_wave,
) -> None:
    wave = external_wave

    assert wave.status is ProductionWaveStatus.PLANNED
    assert wave.retrieval_cutoff_date is None
    assert wave.required_sources == list(EXTERNAL_IDENTIFICATION_SOURCES_V2)
    assert wave.support_sources == ["CrossRef"]
    assert len(wave.query_families) == 30
    assert {
        item.source_database for item in wave.query_families
    } == set(EXTERNAL_IDENTIFICATION_SOURCES_V2)
    assert wave.query_plan_hash == compute_query_plan_hash(wave)
    assert wave.metadata["bindings"]["production_query_plan"] == {
        "path": "config/star_production_query_plan_v1.json",
        "byte_size": 205457,
        "raw_sha256": EXPECTED_PLAN_RAW_SHA256,
        "canonical_hash": EXPECTED_PLAN_HASH,
    }
    assert wave.metadata["deferred_seed_set_ids"] == ["EBK25", "JFR25", "FP19"]
    assert wave.metadata["identification_set_closure_allowed"] is False
    assert wave.metadata["production_operations"] == {
        "external_retrieval_executed": False,
        "acm_import_executed": False,
        "seed_import_executed": False,
        "identification_set_closed": False,
        "final_global_deduplication_executed": False,
        "prisma_generated": False,
        "screening_executed": False,
        "corpus_created": False,
    }


def test_external_preflight_reaches_ready_and_binds_acm_without_import(
    external_preflight,
) -> None:
    report = external_preflight

    assert report["status"] == READY_STATUS
    assert report["production_query_plan"]["raw_sha256"] == EXPECTED_PLAN_RAW_SHA256
    assert report["production_query_plan"]["path"].endswith(
        "star_production_query_plan_v1.json"
    )
    assert report["wave_preflight"]["ready"] is True
    assert report["wave_preflight"]["execution_complete"] is False
    assert report["wave_preflight"]["finalizable"] is False
    assert report["acm_artifact_import"] == {
        "live_requests": 0,
        "manifest_path": ACM_RECONCILIATION_PATH,
        "manifest_hash": (
            "c159e0b2b6dc7991d0eab55ed5b395817d524c0b8245011db3d083870f3f4e12"
        ),
        "selected_artifact_count": 25,
        "raw_selected_occurrence_count": 11664,
        "malformed_but_identified_record_count": 3,
        "unique_identity_count_by_family": {
            "STAR-QF01-RELATIONAL-VIS": 1949,
            "STAR-QF02-ASSISTED-VIS": 1689,
            "STAR-QF03-INTERACTIVE-SYSTEMS": 1995,
            "STAR-QF04-NONDESKTOP-ENV": 2456,
            "STAR-QF05-CONVERSATIONAL": 3477,
        },
        "import_executed": False,
        "selected_artifacts_only": True,
        "nonselected_artifacts_preserved_but_excluded": True,
    }
    assert report["safeguards"]["network_used"] is False
    assert report["safeguards"]["production_retrieval_cutoff"] is None


def test_request_burden_is_explicit_and_semantic_controls_fail_closed(
    external_preflight,
) -> None:
    report = external_preflight

    burden = report["request_burden"]
    assert burden["estimated_http_requests"] == 467
    assert {
        source: item["estimated_total_requests"]
        for source, item in burden["by_source"].items()
    } == {
        "PubMed": 62,
        "EuropePMC": 14,
        "SemanticScholar": 148,
        "arXiv": 11,
        "IEEEXplore": 232,
        "ACMDigitalLibrary": 0,
    }
    assert report["semantic_scholar"]["required_gate"] == "bulk_boolean_semantics"
    assert report["semantic_scholar"]["control_request_count"] == 6
    assert report["semantic_scholar"]["must_pass_before_candidate_requests"] is True


def test_output_paths_cannot_escape_the_dedicated_namespace() -> None:
    expected = ROOT / "outputs/production/star-external-retrieval-wave-001/wave.json"
    assert _safe_output_path(
        ROOT, "outputs/production/star-external-retrieval-wave-001/wave.json"
    ) == expected
    with pytest.raises(ExternalRetrievalWaveError):
        _safe_output_path(ROOT, "outputs/production/unrelated.json")


def test_plan_canonical_hash_is_bound_separately_from_wave_query_hash(
    external_preflight,
) -> None:
    report = external_preflight
    assert EXPECTED_PLAN_HASH == (
        "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
    )
    assert report["wave_manifest_hash"] != EXPECTED_PLAN_HASH


def _install_isolated_runtime(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    monkeypatch.setattr(
        external_module,
        "validate_persisted_external_preflight",
        lambda **_: (external_wave, external_preflight),
    )
    preflight_path = tmp_path / PREFLIGHT_PATH
    preflight_path.parent.mkdir(parents=True, exist_ok=True)
    preflight_path.write_text(json.dumps(external_preflight), encoding="utf-8")
    wave_path = tmp_path / WAVE_PATH
    wave_path.write_text(
        json.dumps(external_wave.to_dict(), sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _arxiv_feed(identifier: str) -> bytes:
    return f"""<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
      <updated>2026-09-04T00:00:00Z</updated>
      <opensearch:totalResults>1</opensearch:totalResults>
      <opensearch:startIndex>0</opensearch:startIndex>
      <opensearch:itemsPerPage>1</opensearch:itemsPerPage>
      <entry><id>http://arxiv.org/abs/{identifier}</id><title>{identifier}</title>
      <summary>Abstract</summary></entry></feed>""".encode()


class ReadTimeout(Exception):
    """Deterministic stand-in matching the persisted requests exception name."""


def _failed_arxiv_rate_limit_episode(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    failures = [
        *[ReadTimeout("timed out") for _ in range(3)],
        *[ReadTimeout("timed out") for _ in range(3)],
        ReadTimeout("timed out"),
        ReadTimeout("timed out"),
        FakeResponse(status_code=429, content=b"Rate exceeded."),
        *[
            FakeResponse(status_code=429, content=b"Rate exceeded.")
            for _ in range(3)
        ],
        *[
            FakeResponse(status_code=429, content=b"Rate exceeded.")
            for _ in range(3)
        ],
    ]
    original_execute = external_module.execute_paginated_retrieval_run

    def execute_with_legacy_429_failure(**kwargs):
        kwargs["pause_status_codes"] = frozenset()
        kwargs["resumable_transport_exhaustion_sources"] = frozenset()
        return original_execute(**kwargs)

    monkeypatch.setattr(
        external_module,
        "execute_paginated_retrieval_run",
        execute_with_legacy_429_failure,
    )
    clock = Clock()
    failed = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp(failures),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    monkeypatch.setattr(
        external_module,
        "execute_paginated_retrieval_run",
        original_execute,
    )
    assert failed["sources"]["arXiv"]["status"] == "FAILED"
    assert failed["sources"]["arXiv"]["attempt_count"] == 15
    return failed, clock


def _paused_arxiv_mixed_episode(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_arxiv_rate_limit_recovery(root=tmp_path, timestamp=clock)
    failures = [
        *[ReadTimeout("timed out") for _ in range(3)],
        *[ReadTimeout("timed out") for _ in range(3)],
        *[ReadTimeout("timed out") for _ in range(3)],
        FakeResponse(status_code=429, content=b"Rate exceeded."),
    ]
    original_execute = external_module.execute_paginated_retrieval_run

    def execute_with_legacy_terminal_timeouts(**kwargs):
        kwargs["resumable_transport_exhaustion_sources"] = frozenset()
        return original_execute(**kwargs)

    monkeypatch.setattr(
        external_module,
        "execute_paginated_retrieval_run",
        execute_with_legacy_terminal_timeouts,
    )
    paused = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp(failures),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    monkeypatch.setattr(
        external_module,
        "execute_paginated_retrieval_run",
        original_execute,
    )
    source_state = paused["sources"]["arXiv"]
    assert source_state["status"] == "PAUSED_PROVIDER_RATE_LIMIT"
    assert source_state["attempt_count"] == 10
    assert source_state["occurrence_count"] == 0
    monkeypatch.setattr(
        external_module,
        "ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256",
        source_state["checkpoint_dataset"]["raw_sha256"],
    )
    return paused, clock


def test_arxiv_rate_limit_recovery_creates_fresh_episode_and_preserves_evidence(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    arxiv_before = failed["sources"]["arXiv"]
    checkpoint_ref = arxiv_before["checkpoint_dataset"]
    checkpoint_path = tmp_path / checkpoint_ref["path"]
    checkpoint_bytes = checkpoint_path.read_bytes()
    response_dir = checkpoint_path.parent / "responses"
    response_bytes = {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    }
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in failed["sources"].items()
        if key != "arXiv"
    }

    recovered = authorize_arxiv_rate_limit_recovery(
        root=tmp_path, timestamp=clock
    )

    arxiv = recovered["sources"]["arXiv"]
    assert arxiv["status"] == ARXIV_RATE_LIMIT_RECOVERY_STATUS
    assert arxiv["active_episode_number"] == 2
    assert arxiv["attempt_count"] == 0
    assert arxiv["occurrence_count"] == 0
    assert arxiv["preserved_source_attempt_count"] == 15
    assert arxiv["preserved_source_raw_response_count"] == 7
    episode_1, episode_2 = arxiv["execution_episodes"]
    assert episode_1["immutable"] is True
    assert episode_1["attempt_count"] == 15
    assert episode_1["transport_timeout_count"] == 8
    assert episode_1["http_429_count"] == 7
    assert [item["attempt_kinds"] for item in episode_1["query_attempt_signatures"]] == [
        ["ReadTimeout", "ReadTimeout", "ReadTimeout"],
        ["ReadTimeout", "ReadTimeout", "ReadTimeout"],
        ["ReadTimeout", "ReadTimeout", "HTTP_429"],
        ["HTTP_429", "HTTP_429", "HTTP_429"],
        ["HTTP_429", "HTTP_429", "HTTP_429"],
    ]
    assert episode_2["immutable"] is False
    assert episode_2["network_used"] is False
    assert [item["request_state"] for item in episode_2["restart_states"]] == [
        {"start": 0}
    ] * 5
    assert [item["max_results"] for item in episode_2["restart_states"]] == [
        2000
    ] * 5
    assert all(
        item["retry_after_header_present"] is False
        and item["retry_after"] is None
        for item in episode_2["source_raw_responses"]
    )
    assert checkpoint_path.read_bytes() == checkpoint_bytes
    assert {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    } == response_bytes
    assert {
        key: value for key, value in recovered["sources"].items() if key != "arXiv"
    } == other_sources_before
    assert recovered["external_retrieval_cutoff_date"] is None

    active = external_module.load_review_dataset(
        tmp_path / arxiv["checkpoint_dataset"]["path"]
    )
    assert len(active.source_queries) == 5
    assert active.retrieval_pages == []
    assert active.retrieval_attempts == []
    assert active.occurrences == []
    assert all(
        query.completion_status is external_module.RetrievalCompletionStatus.PLANNED
        and query.result_count == 0
        and query.page_ids == []
        for query in active.source_queries
    )

    repeated = authorize_arxiv_rate_limit_recovery(
        root=tmp_path, timestamp=clock
    )
    assert repeated == recovered


def test_arxiv_rate_limit_recovery_refuses_response_hash_corruption(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / failed["sources"]["arXiv"]["checkpoint_dataset"]["path"]
    response = next((checkpoint.parent / "responses").iterdir())
    response.write_bytes(response.read_bytes() + b"corrupt")

    with pytest.raises(ExternalRetrievalWaveError, match="hash/read failure"):
        authorize_arxiv_rate_limit_recovery(root=tmp_path, timestamp=clock)


@pytest.mark.parametrize("drift_kind", ["query", "request_hash"])
def test_arxiv_rate_limit_recovery_refuses_query_or_request_hash_drift(
    tmp_path, monkeypatch, external_wave, external_preflight, drift_kind
) -> None:
    failed, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / failed["sources"]["arXiv"]["checkpoint_dataset"]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    if drift_kind == "query":
        dataset.source_queries[0].query_text += " changed"
    else:
        dataset.retrieval_attempts[0].request_hash = "0" * 64
    monkeypatch.setattr(external_module, "load_review_dataset", lambda _: dataset)

    with pytest.raises(ExternalRetrievalWaveError, match="query|request|retries"):
        authorize_arxiv_rate_limit_recovery(root=tmp_path, timestamp=clock)


def test_arxiv_rate_limit_recovery_refuses_any_successful_page_or_occurrence(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / failed["sources"]["arXiv"]["checkpoint_dataset"]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    dataset.occurrences.append(object())
    monkeypatch.setattr(external_module, "load_review_dataset", lambda _: dataset)

    with pytest.raises(
        ExternalRetrievalWaveError, match="successful page or occurrence exists"
    ):
        authorize_arxiv_rate_limit_recovery(root=tmp_path, timestamp=clock)


def test_arxiv_episode_2_live_resume_starts_all_families_at_zero(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_arxiv_rate_limit_recovery(root=tmp_path, timestamp=clock)
    http = FakeHttp(
        [FakeResponse(content=_arxiv_feed(f"family-{index}")) for index in range(5)]
    )

    completed = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )

    assert [call["params"]["start"] for call in http.calls] == [0] * 5
    assert [call["params"]["max_results"] for call in http.calls] == [2000] * 5
    arxiv = completed["sources"]["arXiv"]
    assert arxiv["status"] == "COMPLETE"
    assert arxiv["completed_query_count"] == 5
    assert arxiv["occurrence_count"] == 5
    assert arxiv["execution_episodes"][0]["immutable"] is True
    assert arxiv["execution_episodes"][1]["immutable"] is True


def test_arxiv_episode_2_repeated_429_pauses_and_malformed_feed_fails_closed(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _failed_arxiv_rate_limit_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_arxiv_rate_limit_recovery(root=tmp_path, timestamp=clock)
    paused = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp(
            [FakeResponse(status_code=429, headers={"Retry-After": "90"})]
        ),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: pytest.fail("429 must pause without spinning"),
    )
    arxiv = paused["sources"]["arXiv"]
    assert arxiv["status"] == "PAUSED_PROVIDER_RATE_LIMIT"
    assert arxiv["requests_this_session"] == 1
    assert arxiv["pause_metadata"]["retry_after_header_present"] is True
    assert arxiv["pause_metadata"]["retry_after"] == "90"

    failed = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp([FakeResponse(content=b"not XML")]),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert failed["sources"]["arXiv"]["status"] == "FAILED"
    assert "ParseError" in failed["sources"]["arXiv"]["failure_reason"]


def test_arxiv_mixed_state_recovery_creates_episode_3_and_preserves_evidence(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    paused, clock = _paused_arxiv_mixed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    arxiv_before = paused["sources"]["arXiv"]
    episode_checkpoint_bytes = {
        episode["episode_number"]: (
            tmp_path / episode["checkpoint_dataset"]["path"]
        ).read_bytes()
        for episode in arxiv_before["execution_episodes"]
    }
    response_bytes = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for episode in arxiv_before["execution_episodes"]
        for path in (
            tmp_path / episode["checkpoint_path"] / "responses"
        ).glob("*.json")
    }
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in paused["sources"].items()
        if key != "arXiv"
    }

    recovered = authorize_arxiv_mixed_state_recovery(
        root=tmp_path, timestamp=clock
    )

    arxiv = recovered["sources"]["arXiv"]
    assert arxiv["status"] == ARXIV_MIXED_RECOVERY_STATUS
    assert arxiv["active_episode_number"] == 3
    assert arxiv["attempt_count"] == 0
    assert arxiv["occurrence_count"] == 0
    assert arxiv["preserved_source_attempt_count"] == 25
    assert arxiv["preserved_source_raw_response_count"] == 8
    episode_1, episode_2, episode_3 = arxiv["execution_episodes"]
    assert episode_1["immutable"] is True
    assert episode_2["immutable"] is True
    assert episode_2["attempt_count"] == 10
    assert episode_2["status"] == "PAUSED_PROVIDER_RATE_LIMIT"
    assert episode_3["immutable"] is False
    assert episode_3["network_used"] is False
    assert episode_3["source_attempt_count"] == 25
    assert episode_3["source_raw_response_count"] == 8
    assert [
        item["attempt_kinds"]
        for item in episode_3["episode_2_query_attempt_signatures"]
    ] == [
        ["ReadTimeout", "ReadTimeout", "ReadTimeout"],
        ["ReadTimeout", "ReadTimeout", "ReadTimeout"],
        ["ReadTimeout", "ReadTimeout", "ReadTimeout"],
        ["HTTP_429"],
        [],
    ]
    assert [item["request_state"] for item in episode_3["restart_states"]] == [
        {"start": 0}
    ] * 5
    assert [item["max_results"] for item in episode_3["restart_states"]] == [
        2000
    ] * 5
    assert len(episode_3["episode_2_raw_responses"]) == 1
    assert (
        episode_3["episode_2_raw_responses"][0][
            "retry_after_header_present"
        ]
        is False
    )
    assert episode_3["episode_2_raw_responses"][0]["retry_after"] is None
    assert {
        episode["episode_number"]: (
            tmp_path / episode["checkpoint_dataset"]["path"]
        ).read_bytes()
        for episode in arxiv["execution_episodes"][:2]
    } == episode_checkpoint_bytes
    assert {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for episode in arxiv["execution_episodes"][:2]
        for path in (
            tmp_path / episode["checkpoint_path"] / "responses"
        ).glob("*.json")
    } == response_bytes
    assert {
        key: value for key, value in recovered["sources"].items() if key != "arXiv"
    } == other_sources_before
    assert recovered["external_retrieval_cutoff_date"] is None

    active = external_module.load_review_dataset(
        tmp_path / arxiv["checkpoint_dataset"]["path"]
    )
    assert active.retrieval_pages == []
    assert active.retrieval_attempts == []
    assert active.occurrences == []
    assert all(
        query.completion_status is external_module.RetrievalCompletionStatus.PLANNED
        and query.result_count == 0
        and query.page_ids == []
        for query in active.source_queries
    )

    repeated = authorize_arxiv_mixed_state_recovery(
        root=tmp_path, timestamp=clock
    )
    assert repeated == recovered


def test_arxiv_mixed_state_recovery_refuses_response_hash_corruption(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    paused, clock = _paused_arxiv_mixed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / paused["sources"]["arXiv"]["checkpoint_dataset"]["path"]
    response = next((checkpoint.parent / "responses").iterdir())
    response.write_bytes(response.read_bytes() + b"corrupt")

    with pytest.raises(ExternalRetrievalWaveError, match="hash/read failure"):
        authorize_arxiv_mixed_state_recovery(root=tmp_path, timestamp=clock)


@pytest.mark.parametrize(
    "drift_kind",
    ["signature", "query", "request_hash", "successful_page", "occurrence"],
)
def test_arxiv_mixed_state_recovery_refuses_signature_query_or_data_drift(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    drift_kind,
) -> None:
    paused, clock = _paused_arxiv_mixed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    episode_2_path = (
        tmp_path / paused["sources"]["arXiv"]["checkpoint_dataset"]["path"]
    ).resolve()
    original_load = external_module.load_review_dataset

    def load_with_drift(path):
        dataset = original_load(path)
        if Path(path).resolve() != episode_2_path:
            return dataset
        if drift_kind == "signature":
            dataset.retrieval_attempts[0].error = "ConnectionError: changed"
        elif drift_kind == "query":
            dataset.source_queries[0].query_text += " changed"
        elif drift_kind == "request_hash":
            dataset.retrieval_attempts[0].request_hash = "0" * 64
        elif drift_kind == "successful_page":
            dataset.retrieval_pages[0].status = (
                external_module.RetrievalCompletionStatus.COMPLETE
            )
        else:
            dataset.occurrences.append(object())
        return dataset

    monkeypatch.setattr(external_module, "load_review_dataset", load_with_drift)
    with pytest.raises(
        ExternalRetrievalWaveError,
        match="signature|query|request|successful page|frozen",
    ):
        authorize_arxiv_mixed_state_recovery(root=tmp_path, timestamp=clock)


def test_arxiv_episode_3_live_resume_starts_all_families_at_zero(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _paused_arxiv_mixed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_arxiv_mixed_state_recovery(root=tmp_path, timestamp=clock)
    http = FakeHttp(
        [FakeResponse(content=_arxiv_feed(f"family-{index}")) for index in range(5)]
    )

    completed = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )

    assert [call["params"]["start"] for call in http.calls] == [0] * 5
    assert [call["params"]["max_results"] for call in http.calls] == [2000] * 5
    arxiv = completed["sources"]["arXiv"]
    assert arxiv["status"] == "COMPLETE"
    assert arxiv["completed_query_count"] == 5
    assert arxiv["occurrence_count"] == 5
    assert [item["immutable"] for item in arxiv["execution_episodes"]] == [
        True,
        True,
        True,
    ]


def test_arxiv_live_transport_pause_is_resumable_without_terminal_query_skip(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    clock = Clock()
    first_http = FakeHttp([ReadTimeout("timed out") for _ in range(3)])

    paused = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=first_http,
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    arxiv = paused["sources"]["arXiv"]
    assert arxiv["status"] == "PAUSED_TRANSIENT_TRANSPORT"
    assert arxiv["requests_this_session"] == 3
    checkpoint = external_module.load_review_dataset(
        tmp_path / arxiv["checkpoint_dataset"]["path"]
    )
    assert checkpoint.source_queries[0].completion_status is (
        external_module.RetrievalCompletionStatus.RUNNING
    )
    assert all(
        query.completion_status is external_module.RetrievalCompletionStatus.PLANNED
        for query in checkpoint.source_queries[1:]
    )

    resumed_http = FakeHttp(
        [FakeResponse(content=_arxiv_feed(f"family-{index}")) for index in range(5)]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=resumed_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    assert len(resumed_http.calls) == 5
    assert [call["params"]["start"] for call in resumed_http.calls] == [0] * 5
    assert completed["sources"]["arXiv"]["status"] == "COMPLETE"


def _stale_arxiv_episode_3_state(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _, clock = _paused_arxiv_mixed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_arxiv_mixed_state_recovery(root=tmp_path, timestamp=clock)
    execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp([FakeResponse(status_code=429)]),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    state_after_four = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp([ReadTimeout("timed out") for _ in range(3)]),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    stale_source = json.loads(
        json.dumps(state_after_four["sources"]["arXiv"], sort_keys=True)
    )
    state_after_seven = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=FakeHttp([ReadTimeout("timed out again") for _ in range(3)]),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=1),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    actual_reference = state_after_seven["sources"]["arXiv"][
        "checkpoint_dataset"
    ]
    state_after_seven["sources"]["arXiv"] = stale_source
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    external_module._save_execution_state(state_path, state_after_seven)
    monkeypatch.setattr(
        external_module,
        "ARXIV_EPISODE_3_STALE_CHECKPOINT_SHA256",
        stale_source["checkpoint_dataset"]["raw_sha256"],
    )
    monkeypatch.setattr(
        external_module,
        "ARXIV_EPISODE_3_STALE_CHECKPOINT_SIZE",
        stale_source["checkpoint_dataset"]["byte_size"],
    )
    monkeypatch.setattr(
        external_module,
        "ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SHA256",
        actual_reference["raw_sha256"],
    )
    monkeypatch.setattr(
        external_module,
        "ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SIZE",
        actual_reference["byte_size"],
    )
    return state_after_seven, stale_source, actual_reference, clock


def _reconciled_arxiv_episode_3_state(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _, _, _, clock = _stale_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    state = authorize_arxiv_episode_3_state_reconciliation(
        root=tmp_path, timestamp=clock
    )
    return state, clock


def test_external_source_session_lock_fails_fast_before_state_load(
    tmp_path, monkeypatch
) -> None:
    state_loaded = False

    def unexpected_state_load(*args, **kwargs):
        nonlocal state_loaded
        state_loaded = True
        raise AssertionError("state must not load while another session holds the lock")

    monkeypatch.setattr(external_module, "_load_execution_state", unexpected_state_load)
    with external_module._exclusive_external_source_session(tmp_path), pytest.raises(
        ExternalRetrievalWaveError,
        match="another external-source session is already active",
    ):
        execute_external_source_session(
            root=tmp_path,
            source="arXiv",
            http=FakeHttp([]),
            resume=True,
        )
    assert state_loaded is False


def test_external_source_session_lock_is_released_after_exception(tmp_path) -> None:
    with pytest.raises(
        RuntimeError, match="deliberate"
    ), external_module._exclusive_external_source_session(tmp_path):
        raise RuntimeError("deliberate")
    with external_module._exclusive_external_source_session(tmp_path):
        pass


def test_external_source_session_lock_excludes_another_process(tmp_path) -> None:
    script = """
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / "src"))
from h2h_lit.external_retrieval_wave import (
    ExternalRetrievalWaveError,
    _exclusive_external_source_session,
)
try:
    with _exclusive_external_source_session(Path(sys.argv[1])):
        pass
except ExternalRetrievalWaveError:
    raise SystemExit(23)
raise SystemExit(0)
"""
    with external_module._exclusive_external_source_session(tmp_path):
        result = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path)],
            check=False,
            capture_output=True,
            text=True,
        )
    assert result.returncode == 23


def test_arxiv_episode_3_state_reconciliation_refreshes_only_stale_summary(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, stale_source, actual_reference, clock = _stale_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint_path = tmp_path / actual_reference["path"]
    checkpoint_bytes = checkpoint_path.read_bytes()
    response_bytes = {
        path.name: path.read_bytes()
        for path in sorted((checkpoint_path.parent / "responses").iterdir())
    }
    other_sources = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "arXiv"
    }

    reconciled = authorize_arxiv_episode_3_state_reconciliation(
        root=tmp_path, timestamp=clock
    )

    arxiv = reconciled["sources"]["arXiv"]
    episode = arxiv["execution_episodes"][2]
    checkpoint = external_module.load_review_dataset(checkpoint_path)
    assert arxiv["checkpoint_dataset"] == actual_reference
    assert episode["checkpoint_dataset"] == actual_reference
    assert arxiv["attempt_count"] == episode["attempt_count"] == 7
    assert arxiv["requests_this_session"] == 3
    assert arxiv["last_session_started_at_utc"] == (
        checkpoint.retrieval_attempts[4].started_at
    )
    assert arxiv["last_session_completed_at_utc"] == (
        checkpoint.retrieval_attempts[6].ended_at
    )
    evidence = arxiv["state_reconciliation"]
    assert episode["state_reconciliation"] == evidence
    assert evidence["classification"] == "CONCURRENT_EXECUTION_STATE_LOST_UPDATE"
    assert evidence["prior_checkpoint_dataset"] == stale_source[
        "checkpoint_dataset"
    ]
    assert evidence["reconciled_checkpoint_dataset"] == actual_reference
    assert evidence["appended_attempt_numbers"] == [5, 6, 7]
    assert evidence["persisted_response_hashes"] == [
        checkpoint.retrieval_attempts[0].raw_response_hash
    ]
    assert evidence["network_used"] is False
    assert evidence["checkpoint_modified"] is False
    assert checkpoint_path.read_bytes() == checkpoint_bytes
    assert {
        path.name: path.read_bytes()
        for path in sorted((checkpoint_path.parent / "responses").iterdir())
    } == response_bytes
    assert {
        key: value for key, value in reconciled["sources"].items() if key != "arXiv"
    } == other_sources

    repeated = authorize_arxiv_episode_3_state_reconciliation(
        root=tmp_path, timestamp=clock
    )
    assert repeated == reconciled


@pytest.mark.parametrize(
    "drift_kind",
    ["state_reference", "query", "request_hash", "attempt", "response"],
)
def test_arxiv_episode_3_state_reconciliation_refuses_unexpected_drift(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    drift_kind,
) -> None:
    state, _, actual_reference, clock = _stale_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    checkpoint_path = tmp_path / actual_reference["path"]
    if drift_kind == "state_reference":
        state["sources"]["arXiv"]["checkpoint_dataset"]["raw_sha256"] = "0" * 64
        state["sources"]["arXiv"]["execution_episodes"][2][
            "checkpoint_dataset"
        ]["raw_sha256"] = "0" * 64
        external_module._save_execution_state(state_path, state)
    elif drift_kind == "response":
        response = next((checkpoint_path.parent / "responses").iterdir())
        response.write_bytes(response.read_bytes() + b"corrupt")
    else:
        original_load = external_module.load_review_dataset

        def load_with_drift(path):
            dataset = original_load(path)
            if Path(path).resolve() != checkpoint_path.resolve():
                return dataset
            if drift_kind == "query":
                dataset.source_queries[0].query_text += " changed"
            elif drift_kind == "request_hash":
                dataset.retrieval_attempts[0].request_hash = "0" * 64
            else:
                dataset.retrieval_attempts[-1].error = "ConnectionError: changed"
            return dataset

        monkeypatch.setattr(external_module, "load_review_dataset", load_with_drift)

    with pytest.raises(
        (ExternalRetrievalWaveError, ValueError),
        match="arXiv|request|retries|response hash",
    ):
        authorize_arxiv_episode_3_state_reconciliation(
            root=tmp_path, timestamp=clock
        )


def test_arxiv_transport_policy_recovery_creates_bound_episode_four(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    parent_state, clock = _reconciled_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    arxiv_before = parent_state["sources"]["arXiv"]
    historical_files = {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for episode in arxiv_before["execution_episodes"]
        for path in (
            tmp_path / episode["checkpoint_dataset"]["path"]
        ).parent.rglob("*")
        if path.is_file()
    }
    other_sources = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in parent_state["sources"].items()
        if key != "arXiv"
    }

    recovered = authorize_arxiv_transport_policy_recovery(
        root=tmp_path, timestamp=clock
    )

    arxiv = recovered["sources"]["arXiv"]
    assert arxiv["status"] == ARXIV_TRANSPORT_POLICY_RECOVERY_STATUS
    assert arxiv["active_episode_number"] == 4
    assert arxiv["attempt_count"] == arxiv["occurrence_count"] == 0
    assert arxiv["completed_query_count"] == 0
    assert arxiv["transport_policy"] == {
        "read_timeout_seconds": ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
        "maximum_attempts_per_invocation": 3,
    }
    assert len(arxiv["execution_episodes"]) == 4
    assert [item["immutable"] for item in arxiv["execution_episodes"]] == [
        True,
        True,
        True,
        False,
    ]
    episode_4 = arxiv["execution_episodes"][3]
    assert episode_4["recovery_of_episode_number"] == 3
    assert episode_4["parent_checkpoint_dataset"] == arxiv_before[
        "checkpoint_dataset"
    ]
    assert episode_4["old_transport_policy"] == {
        "read_timeout_seconds": 30.0,
        "maximum_attempts_per_invocation": 3,
    }
    assert episode_4["transport_policy"] == arxiv["transport_policy"]
    assert len(episode_4["request_identity_changes"]) == 5
    assert all(
        item["request_state"] == {"start": 0}
        and item["max_results"] == 2000
        and item["old_timeout_seconds"] == 30.0
        and item["new_timeout_seconds"] == 120.0
        and item["old_request_hash"] != item["new_request_hash"]
        for item in episode_4["request_identity_changes"]
    )
    assert episode_4["restart_states"] == [
        {
            "production_query_id": item["production_query_id"],
            "query_id": item["query_id"],
            "request_state": {"start": 0},
            "max_results": 2000,
            "request_hash": item["new_request_hash"],
        }
        for item in episode_4["request_identity_changes"]
    ]
    checkpoint = external_module.load_review_dataset(
        tmp_path / arxiv["checkpoint_dataset"]["path"]
    )
    assert checkpoint.retrieval_pages == []
    assert checkpoint.retrieval_attempts == []
    assert checkpoint.occurrences == []
    assert checkpoint.canonical_records == []
    assert all(
        query.completion_status
        is external_module.RetrievalCompletionStatus.PLANNED
        and query.filters == {"page_size": 2000}
        and query.metadata["request_timeout_seconds"] == 120.0
        for query in checkpoint.source_queries
    )
    assert {
        path.relative_to(tmp_path).as_posix(): path.read_bytes()
        for episode in arxiv["execution_episodes"][:3]
        for path in (
            tmp_path / episode["checkpoint_dataset"]["path"]
        ).parent.rglob("*")
        if path.is_file()
    } == historical_files
    assert {
        key: value for key, value in recovered["sources"].items() if key != "arXiv"
    } == other_sources
    assert recovered["external_retrieval_cutoff_date"] is None


def test_arxiv_transport_policy_recovery_lock_contention_fails_before_mutation(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    parent_state, clock = _reconciled_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    before = state_path.read_bytes()

    with external_module._exclusive_external_source_session(tmp_path), pytest.raises(
        ExternalRetrievalWaveError,
        match="another external-source session is already active",
    ):
        authorize_arxiv_transport_policy_recovery(
            root=tmp_path, timestamp=clock
        )

    assert state_path.read_bytes() == before
    assert parent_state["sources"]["arXiv"]["active_episode_number"] == 3


def test_arxiv_recovered_timeout_is_persisted_across_bounded_resumes(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _reconciled_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_arxiv_transport_policy_recovery(root=tmp_path, timestamp=clock)
    first_http = FakeHttp([ReadTimeout("timed out") for _ in range(3)])

    paused = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=first_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    assert len(first_http.calls) == 3
    assert {call["timeout"] for call in first_http.calls} == {120.0}
    assert paused["sources"]["arXiv"]["status"] == (
        "PAUSED_TRANSIENT_TRANSPORT"
    )
    assert paused["sources"]["arXiv"]["transport_policy"][
        "read_timeout_seconds"
    ] == 120.0

    resumed_http = FakeHttp(
        [FakeResponse(content=_arxiv_feed(f"family-{index}")) for index in range(5)]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=resumed_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    assert len(resumed_http.calls) == 5
    assert {call["timeout"] for call in resumed_http.calls} == {120.0}
    assert completed["sources"]["arXiv"]["status"] == "COMPLETE"


def test_arxiv_recovered_episode_refuses_a_different_attempt_bound(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _reconciled_arxiv_episode_3_state(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    recovered = authorize_arxiv_transport_policy_recovery(
        root=tmp_path, timestamp=clock
    )
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    before = state_path.read_bytes()

    with pytest.raises(
        ExternalRetrievalWaveError,
        match="requires exactly three attempts",
    ):
        execute_external_source_session(
            root=tmp_path,
            source="arXiv",
            http=FakeHttp([]),
            resume=True,
            retry_policy=RetryPolicy(max_attempts=2),
        )

    assert state_path.read_bytes() == before
    assert recovered["sources"]["arXiv"]["active_episode_number"] == 4


def _ieee_page(identifier: str, *, total: int) -> dict:
    return {
        "total_records": total,
        "articles": [{"article_number": identifier, "title": identifier}],
    }


def _ieee_drift_page(
    family: int, *, start_record: int, count: int, total: int, overlap: bool = False
) -> dict:
    identifiers = [
        f"F{family}-{index}"
        for index in range(start_record, start_record + count)
    ]
    if overlap:
        identifiers[0] = f"F{family}-1"
    return {
        "total_records": total,
        "articles": [
            {"article_number": identifier, "title": identifier}
            for identifier in identifiers
        ],
    }


def _failed_ieee_total_drift_episode(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    *,
    cross_page_overlap: bool = False,
):
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    source_query_specs = external_module._source_query_specs

    def small_ieee_specs(*args, **kwargs):
        specs = source_query_specs(*args, **kwargs)
        if args[1] == "IEEEXplore":
            for spec in specs:
                spec.limit = 2
        return specs

    monkeypatch.setattr(external_module, "_source_query_specs", small_ieee_specs)
    monkeypatch.setattr(
        external_module,
        "IEEE_TOTAL_DRIFT_EXPECTED",
        tuple((6, 5, 2, 3, 4) for _ in range(5)),
    )
    monkeypatch.setattr(
        external_module, "IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS", 10
    )
    monkeypatch.setattr(
        external_module,
        "_ieee_calls_on_day",
        lambda _root, checkpoint_dir, _day: (
            5 + external_module._checkpoint_attempt_count(checkpoint_dir)
        ),
    )
    responses = []
    for family in range(1, 6):
        responses.extend(
            [
                FakeResponse(
                    payload=_ieee_drift_page(
                        family, start_record=1, count=2, total=6
                    )
                ),
                FakeResponse(
                    payload=_ieee_drift_page(
                        family,
                        start_record=3,
                        count=1,
                        total=5,
                        overlap=cross_page_overlap and family == 1,
                    )
                ),
            ]
        )
    clock = Clock()
    fixed_adapter = external_module.PAGINATED_SOURCE_ADAPTERS["IEEEXplore"]
    monkeypatch.setitem(
        external_module.PAGINATED_SOURCE_ADAPTERS,
        "IEEEXplore",
        LegacyReturnedCountIeeePaginator(),
    )
    failed = execute_external_source_session(
        root=tmp_path,
        source="IEEEXplore",
        http=FakeHttp(responses),
        resume=False,
        ieee_credential="offline-test-key",
        quota_day_utc="2026-09-04",
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    monkeypatch.setitem(
        external_module.PAGINATED_SOURCE_ADAPTERS,
        "IEEEXplore",
        fixed_adapter,
    )
    assert failed["sources"]["IEEEXplore"]["status"] == "FAILED"
    return failed, clock


class LegacyReturnedCountIeeePaginator(IeeeXplorePaginator):
    """Model the paginator used to create the immutable episode-2 failure."""

    def parse_response(self, spec, state, response):
        parsed = super().parse_response(spec, state, response)
        next_start = int(state["start_record"]) + parsed.raw_item_count
        parsed.terminal = next_start > int(parsed.source_reported_total)
        parsed.next_state = None if parsed.terminal else {"start_record": next_start}
        parsed.completion_proof = (
            "ieee_current_total_exhaustion_observed" if parsed.terminal else None
        )
        return parsed


def _failed_ieee_repeated_window_episode(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
):
    _, clock = _failed_ieee_total_drift_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_ieee_total_drift_recovery(root=tmp_path, timestamp=clock)
    monkeypatch.setattr(
        external_module,
        "IEEE_REPEATED_WINDOW_EXPECTED",
        tuple((4, 5, 5, 1, 6) for _ in range(5)),
    )
    monkeypatch.setattr(
        external_module,
        "IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES",
        tuple((6, 5, 7, 7) for _ in range(5)),
    )
    monkeypatch.setattr(
        external_module, "IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS", 20
    )
    monkeypatch.setattr(
        external_module, "IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES", 15
    )
    monkeypatch.setattr(
        external_module, "IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS", 92
    )
    monkeypatch.setattr(
        external_module,
        "_ieee_calls_on_day",
        lambda _root, checkpoint_dir, _day: (
            72 + external_module._checkpoint_attempt_count(checkpoint_dir)
        ),
    )
    fixed_adapter = external_module.PAGINATED_SOURCE_ADAPTERS["IEEEXplore"]
    monkeypatch.setitem(
        external_module.PAGINATED_SOURCE_ADAPTERS,
        "IEEEXplore",
        LegacyReturnedCountIeeePaginator(),
    )
    responses = []
    for family in range(1, 6):
        repeated = _ieee_drift_page(
            family, start_record=4, count=1, total=7
        )
        responses.extend(
            [FakeResponse(payload=repeated), FakeResponse(payload=repeated)]
        )
    http = FakeHttp(responses)
    episode_2 = execute_external_source_session(
        root=tmp_path,
        source="IEEEXplore",
        http=http,
        resume=True,
        ieee_credential="offline-test-key",
        quota_day_utc="2026-09-04",
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    monkeypatch.setitem(
        external_module.PAGINATED_SOURCE_ADAPTERS,
        "IEEEXplore",
        fixed_adapter,
    )
    assert episode_2["sources"]["IEEEXplore"]["status"] == "FAILED"
    assert [call["params"]["start_record"] for call in http.calls] == [
        4,
        5,
    ] * 5
    assert episode_2["sources"]["IEEEXplore"]["ieee_quota"][
        "known_calls_after_session"
    ] == 92
    return episode_2, clock


def _rewrite_active_ieee_checkpoint(tmp_path, state, dataset):
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    checkpoint_path = tmp_path / state["sources"]["IEEEXplore"][
        "checkpoint_dataset"
    ]["path"]
    external_module.save_review_dataset(checkpoint_path, dataset)
    reference = external_module._file_reference(checkpoint_path, tmp_path)
    state["sources"]["IEEEXplore"]["checkpoint_dataset"] = reference
    state["sources"]["IEEEXplore"]["execution_episodes"][-1][
        "checkpoint_dataset"
    ] = reference
    external_module._save_execution_state(state_path, state)


class LegacyEuropePmcPaginator:
    source_database = "EuropePMC"
    strategy = "cursor-mark"
    version = "2.0.0"

    def initial_state(self, spec):
        return {"cursor_mark": "*"}

    def build_request(self, spec, state):
        return EuropePmcPaginator().build_request(spec, state)

    def parse_response(self, spec, state, response):
        payload = response.json()
        items = ((payload.get("resultList") or {}).get("result") or [])
        records = parse_europe_pmc_response(
            {"resultList": {"result": items}}, query=spec.query_text
        )
        total = payload.get("hitCount")
        total = int(total) if total is not None else None
        next_cursor = payload.get("nextCursorMark")
        if next_cursor == state["cursor_mark"] and items:
            raise PaginationError("Europe PMC repeated a non-terminal cursor")
        terminal = not next_cursor
        if not terminal and not items:
            raise PaginationError(
                "Europe PMC returned an empty non-terminal cursor page"
            )
        return ParsedPage(
            records=records,
            raw_item_count=len(items),
            next_state={"cursor_mark": next_cursor} if not terminal else None,
            terminal=terminal,
            completion_proof=(
                "europe_pmc_cursor_exhausted" if terminal else None
            ),
            source_reported_total=total,
            total_is_exact=total is not None,
            native_identifiers=[
                native_identifier(record, rank)
                for rank, record in enumerate(records, 1)
            ],
            metadata={"next_page_url": payload.get("nextPageUrl")},
        )


def _europe_pmc_record_page(identifier: str, *, total: int = 1) -> dict:
    return {
        "hitCount": total,
        "nextCursorMark": f"terminal-{identifier}",
        "resultList": {
            "result": [{"id": identifier, "title": f"Title {identifier}"}]
        },
    }


def _europe_pmc_terminal_page(
    identifier: str, *, repeated: bool, total: int = 1
) -> dict:
    payload = {"hitCount": total, "resultList": {"result": []}}
    if repeated:
        payload["nextCursorMark"] = f"terminal-{identifier}"
    return payload


def _failed_europe_pmc_terminal_episode(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    *,
    provider_counts=(1, 1, 1, 1, 1),
):
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    monkeypatch.setattr(
        external_module,
        "EUROPE_PMC_RECOVERY_EXPECTED_COUNTS",
        (1, 1, 1, 1, 1),
    )
    monkeypatch.setattr(
        external_module, "EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS", 10
    )
    legacy = LegacyEuropePmcPaginator()
    monkeypatch.setitem(
        external_module.PAGINATED_SOURCE_ADAPTERS, "EuropePMC", legacy
    )
    responses = []
    for index in range(1, 6):
        identifier = f"E{index}"
        total = provider_counts[index - 1]
        responses.extend(
            [
                FakeResponse(
                    payload=_europe_pmc_record_page(identifier, total=total)
                ),
                FakeResponse(
                    payload=_europe_pmc_terminal_page(
                        identifier, repeated=index != 2, total=total
                    )
                ),
            ]
        )
    clock = Clock()
    failed = execute_external_source_session(
        root=tmp_path,
        source="EuropePMC",
        http=FakeHttp(responses),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    monkeypatch.setitem(
        external_module.PAGINATED_SOURCE_ADAPTERS,
        "EuropePMC",
        EuropePmcPaginator(),
    )
    assert failed["sources"]["EuropePMC"]["status"] == "FAILED"
    assert failed["sources"]["EuropePMC"]["completed_query_count"] == 1
    return failed, clock


def _failed_pubmed_episode(
    tmp_path, monkeypatch, external_wave, external_preflight, *, failures=None
):
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    clock = Clock()
    queued = failures or [ConnectionError("DNS resolution failed") for _ in range(15)]
    state = execute_external_source_session(
        root=tmp_path,
        source="PubMed",
        http=FakeHttp(queued),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert state["sources"]["PubMed"]["status"] == "FAILED"
    return state, clock


def _pubmed_search(pmid: str) -> bytes:
    return (
        "<eSearchResult><Count>1</Count><QueryKey>1</QueryKey>"
        f"<WebEnv>env-{pmid}</WebEnv><IdList><Id>{pmid}</Id></IdList>"
        "</eSearchResult>"
    ).encode()


def _pubmed_fetch(pmid: str) -> bytes:
    return f"""<PubmedArticleSet><PubmedArticle><MedlineCitation>
      <PMID>{pmid}</PMID><Article><ArticleTitle>Title {pmid}</ArticleTitle>
      <Abstract><AbstractText>Abstract {pmid}</AbstractText></Abstract>
      <Journal><Title>Journal</Title><JournalIssue><PubDate><Year>2026</Year>
      </PubDate></JournalIssue></Journal></Article></MedlineCitation>
      <PubmedData><ArticleIdList><ArticleId IdType="doi">10.1000/{pmid}</ArticleId>
      </ArticleIdList></PubmedData></PubmedArticle></PubmedArticleSet>""".encode()


def _pubmed_search_many(pmids: list[str]) -> bytes:
    identifiers = "".join(f"<Id>{pmid}</Id>" for pmid in pmids)
    return (
        f"<eSearchResult><Count>{len(pmids)}</Count><QueryKey>1</QueryKey>"
        f"<WebEnv>env-{pmids[0]}</WebEnv><IdList>{identifiers}</IdList>"
        "</eSearchResult>"
    ).encode()


def _pubmed_article_fragment(pmid: str) -> str:
    return f"""<PubmedArticle><MedlineCitation><PMID>{pmid}</PMID><Article>
      <ArticleTitle>Title {pmid}</ArticleTitle>
      <Abstract><AbstractText>Abstract {pmid}</AbstractText></Abstract>
      <Journal><Title>Journal</Title><JournalIssue><PubDate><Year>2026</Year>
      </PubDate></JournalIssue></Journal></Article></MedlineCitation>
      <PubmedData><ArticleIdList><ArticleId IdType="doi">10.1000/{pmid}</ArticleId>
      </ArticleIdList></PubmedData></PubmedArticle>"""


def _pubmed_book_fragment(pmid: str) -> str:
    return f"""<PubmedBookArticle><BookDocument><PMID>{pmid}</PMID>
      <Book><BookTitle>Book {pmid}</BookTitle><PubDate><Year>2026</Year></PubDate></Book>
      <Abstract><AbstractText>Abstract {pmid}</AbstractText></Abstract>
      </BookDocument></PubmedBookArticle>"""


def _pubmed_mixed_fetch(pmids: list[str], book_pmids: set[str]) -> bytes:
    entries = "".join(
        _pubmed_book_fragment(pmid)
        if pmid in book_pmids
        else _pubmed_article_fragment(pmid)
        for pmid in pmids
    )
    return f"<PubmedArticleSet>{entries}</PubmedArticleSet>".encode()


def _failed_parser_pubmed_episode(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _, clock = _failed_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_pubmed_transport_retry(root=tmp_path, timestamp=clock)
    qf01 = [str(50_000_000 + index) for index in range(199)]
    qf01.insert(1, PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS[0])
    qf01.append("59999999")
    family_pmids = [
        qf01,
        ["60000001", PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS[1]],
        [
            "60000002",
            PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS[2],
            PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS[3],
        ],
        ["60000003", PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS[4]],
        ["60000004", PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS[5]],
    ]
    book_pmids = set(PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS)
    responses = []
    for pmids in family_pmids:
        fetched = pmids[:200]
        responses.extend(
            [
                FakeResponse(content=_pubmed_search_many(pmids)),
                FakeResponse(content=_pubmed_mixed_fetch(fetched, book_pmids)),
            ]
        )

    corrected_parser = pubmed_module.parse_pubmed_fetch

    def legacy_article_only_parser(content: bytes, *, query: str):
        return [
            record
            for record in corrected_parser(content, query=query)
            if record.original_metadata["pubmed_record_type"] == "PubmedArticle"
        ]

    monkeypatch.setattr(
        pubmed_module, "parse_pubmed_fetch", legacy_article_only_parser
    )
    failed = execute_external_source_session(
        root=tmp_path,
        source="PubMed",
        http=FakeHttp(responses),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    monkeypatch.setattr(pubmed_module, "parse_pubmed_fetch", corrected_parser)
    assert failed["sources"]["PubMed"]["status"] == "FAILED"
    assert failed["sources"]["PubMed"]["active_episode_number"] == 2
    return failed, clock, family_pmids


def test_dns_only_pubmed_failure_authorizes_new_immutable_episode(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    original_ref = failed["sources"]["PubMed"]["checkpoint_dataset"]
    original = tmp_path / original_ref["path"]
    original_bytes = original.read_bytes()
    other_sources_before = {
        key: dict(value)
        for key, value in failed["sources"].items()
        if key != "PubMed"
    }

    reset = authorize_pubmed_transport_retry(root=tmp_path, timestamp=clock)
    pubmed = reset["sources"]["PubMed"]
    assert pubmed["status"] == "TRANSPORT_RETRY_AUTHORIZED_NOT_STARTED"
    assert pubmed["active_episode_number"] == 2
    assert len(pubmed["execution_episodes"]) == 2
    assert pubmed["execution_episodes"][0]["immutable"] is True
    assert pubmed["execution_episodes"][0]["attempt_count"] == 15
    assert pubmed["execution_episodes"][0]["checkpoint_dataset"] == original_ref
    assert pubmed["execution_episodes"][1]["retry_of_episode_number"] == 1
    assert original.read_bytes() == original_bytes
    assert hashlib.sha256(original_bytes).hexdigest() == original_ref["raw_sha256"]
    assert {
        key: value for key, value in reset["sources"].items() if key != "PubMed"
    } == other_sources_before
    assert reset["external_retrieval_cutoff_date"] is None

    repeated = authorize_pubmed_transport_retry(root=tmp_path, timestamp=clock)
    assert repeated == reset


def test_pubmed_transport_retry_refuses_any_http_response(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _failed_pubmed_episode(
        tmp_path,
        monkeypatch,
        external_wave,
        external_preflight,
        failures=[FakeResponse(status_code=503) for _ in range(15)],
    )
    with pytest.raises(ExternalRetrievalWaveError, match="HTTP response exists"):
        authorize_pubmed_transport_retry(root=tmp_path, timestamp=Clock())


@pytest.mark.parametrize("failure_kind", ["provider_query", "authentication", "parser"])
def test_pubmed_transport_retry_refuses_provider_query_auth_and_parser_failures(
    tmp_path, monkeypatch, external_wave, external_preflight, failure_kind
) -> None:
    if failure_kind == "provider_query":
        failures = [FakeResponse(status_code=400) for _ in range(15)]
    elif failure_kind == "authentication":
        failures = [FakeResponse(status_code=401) for _ in range(15)]
    else:
        failures = [FakeResponse(content=b"not XML") for _ in range(15)]
    _failed_pubmed_episode(
        tmp_path,
        monkeypatch,
        external_wave,
        external_preflight,
        failures=failures,
    )
    with pytest.raises(ExternalRetrievalWaveError, match="HTTP response exists"):
        authorize_pubmed_transport_retry(root=tmp_path, timestamp=Clock())


def test_pubmed_transport_retry_refuses_imported_occurrence(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, _ = _failed_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / state["sources"]["PubMed"]["checkpoint_dataset"]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    dataset.occurrences.append(object())
    monkeypatch.setattr(external_module, "load_review_dataset", lambda _: dataset)
    with pytest.raises(ExternalRetrievalWaveError, match="records were already imported"):
        authorize_pubmed_transport_retry(root=tmp_path, timestamp=Clock())


def test_pubmed_transport_retry_refuses_changed_frozen_request_hash(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, _ = _failed_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / state["sources"]["PubMed"]["checkpoint_dataset"]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    for attempt in dataset.retrieval_attempts:
        attempt.request_hash = "0" * 64
    monkeypatch.setattr(external_module, "load_review_dataset", lambda _: dataset)
    with pytest.raises(ExternalRetrievalWaveError, match="request hash/method changed"):
        authorize_pubmed_transport_retry(root=tmp_path, timestamp=Clock())


def test_pubmed_transport_retry_refuses_nontransport_failure(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _failed_pubmed_episode(
        tmp_path,
        monkeypatch,
        external_wave,
        external_preflight,
        failures=[ValueError("provider query/parser failure") for _ in range(15)],
    )
    with pytest.raises(ExternalRetrievalWaveError, match="non-transport failure"):
        authorize_pubmed_transport_retry(root=tmp_path, timestamp=Clock())


def test_authorized_pubmed_second_episode_completes_after_environment_recovery(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    original_ref = failed["sources"]["PubMed"]["checkpoint_dataset"]
    original_bytes = (tmp_path / original_ref["path"]).read_bytes()
    authorize_pubmed_transport_retry(root=tmp_path, timestamp=clock)
    responses = []
    for index in range(1, 6):
        pmid = str(1000 + index)
        responses.extend(
            [
                FakeResponse(content=_pubmed_search(pmid)),
                FakeResponse(content=_pubmed_fetch(pmid)),
            ]
        )

    completed = execute_external_source_session(
        root=tmp_path,
        source="PubMed",
        http=FakeHttp(responses),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    pubmed = completed["sources"]["PubMed"]
    assert pubmed["status"] == "COMPLETE"
    assert pubmed["completed_query_count"] == 5
    assert pubmed["occurrence_count"] == 5
    assert pubmed["attempt_count"] == 10
    assert pubmed["execution_episodes"][0]["status"] == "FAILED"
    assert pubmed["execution_episodes"][0]["immutable"] is True
    assert pubmed["execution_episodes"][1]["status"] == "COMPLETE"
    assert pubmed["execution_episodes"][1]["immutable"] is True
    assert pubmed["execution_episodes"][1]["run_id"].endswith("episode-002")
    assert (tmp_path / original_ref["path"]).read_bytes() == original_bytes
    assert completed["external_retrieval_cutoff_date"] is None


def test_pubmed_parser_recovery_preserves_episode_2_and_recovers_exact_six_books(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock, family_pmids = _failed_parser_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    pubmed_before = failed["sources"]["PubMed"]
    episode_2_before = json.loads(
        json.dumps(pubmed_before["execution_episodes"][1], sort_keys=True)
    )
    checkpoint_ref = pubmed_before["checkpoint_dataset"]
    checkpoint_path = tmp_path / checkpoint_ref["path"]
    checkpoint_bytes = checkpoint_path.read_bytes()
    response_dir = checkpoint_path.parent / "responses"
    raw_responses_before = {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    }
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in failed["sources"].items()
        if key != "PubMed"
    }

    recovered = authorize_pubmed_parser_recovery(root=tmp_path, timestamp=clock)
    pubmed = recovered["sources"]["PubMed"]
    assert pubmed["status"] == PUBMED_PARSER_RECOVERY_STATUS
    assert pubmed["active_episode_number"] == 3
    assert pubmed["execution_episodes"][1] == episode_2_before
    episode_3 = pubmed["execution_episodes"][2]
    assert episode_3["recovery_of_episode_number"] == 2
    assert episode_3["recovered_pmids"] == list(
        PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS
    )
    assert episode_3["remaining_efetch_request_count"] == 1
    assert episode_3["remaining_efetch_batches"][0]["pmids"] == ["59999999"]
    assert episode_3["network_used"] is False
    assert checkpoint_path.read_bytes() == checkpoint_bytes
    assert {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    } == raw_responses_before
    assert {
        key: value for key, value in recovered["sources"].items() if key != "PubMed"
    } == other_sources_before
    assert recovered["external_retrieval_cutoff_date"] is None

    dataset = external_module.load_review_dataset(
        tmp_path / pubmed["checkpoint_dataset"]["path"]
    )
    recovered_ids = {occurrence.source_identifier for occurrence in dataset.occurrences}
    assert set(PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS) <= recovered_ids
    assert len(dataset.occurrences) == sum(min(len(pmids), 200) for pmids in family_pmids)
    assert len(dataset.retrieval_attempts) == len(raw_responses_before)
    assert sum(
        query.completion_status.name == "COMPLETE" for query in dataset.source_queries
    ) == 4
    for binding in episode_3["source_raw_responses"]:
        original = (tmp_path / binding["episode_2_path"]).read_bytes()
        copied = (tmp_path / binding["recovery_copy_path"]).read_bytes()
        assert copied == original
        assert hashlib.sha256(original).hexdigest() == binding["raw_sha256"]

    repeated = authorize_pubmed_parser_recovery(root=tmp_path, timestamp=clock)
    assert repeated == recovered


def test_pubmed_parser_recovery_resume_requests_only_unfetched_batch(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock, _ = _failed_parser_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_pubmed_parser_recovery(root=tmp_path, timestamp=clock)
    http = FakeHttp([FakeResponse(content=_pubmed_fetch("59999999"))])

    completed = execute_external_source_session(
        root=tmp_path,
        source="PubMed",
        http=http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )

    assert len(http.calls) == 1
    assert http.calls[0]["method"] == "GET"
    assert http.calls[0]["params"]["id"] == "59999999"
    assert completed["sources"]["PubMed"]["status"] == "COMPLETE"
    assert completed["sources"]["ACMDigitalLibrary"]["status"] == "NOT_STARTED"
    assert completed["external_retrieval_cutoff_date"] is None


def test_pubmed_parser_recovery_refuses_raw_response_hash_mismatch(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock, _ = _failed_parser_pubmed_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / failed["sources"]["PubMed"]["checkpoint_dataset"]["path"]
    response = next((checkpoint.parent / "responses").iterdir())
    response.write_bytes(response.read_bytes() + b"corrupt")

    with pytest.raises(ExternalRetrievalWaveError, match="hash mismatch"):
        authorize_pubmed_parser_recovery(root=tmp_path, timestamp=clock)


def test_ieee_total_drift_recovery_preserves_failure_and_builds_continuations(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_ieee_total_drift_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    ieee_before = failed["sources"]["IEEEXplore"]
    checkpoint_ref = ieee_before["checkpoint_dataset"]
    checkpoint_path = tmp_path / checkpoint_ref["path"]
    checkpoint_bytes = checkpoint_path.read_bytes()
    response_dir = checkpoint_path.parent / "responses"
    raw_responses_before = {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    }
    quota_before = json.loads(json.dumps(ieee_before["ieee_quota"], sort_keys=True))
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in failed["sources"].items()
        if key != "IEEEXplore"
    }

    recovered = authorize_ieee_total_drift_recovery(
        root=tmp_path, timestamp=clock
    )

    ieee = recovered["sources"]["IEEEXplore"]
    assert ieee["status"] == IEEE_TOTAL_DRIFT_RECOVERY_STATUS
    assert ieee["completed_query_count"] == 0
    assert ieee["occurrence_count"] == 15
    assert ieee["attempt_count"] == 10
    assert ieee["requests_this_session"] == 0
    assert ieee["ieee_quota"] == quota_before
    assert ieee["active_episode_number"] == 2
    assert ieee["execution_episodes"][0]["status"] == "FAILED"
    assert ieee["execution_episodes"][0]["immutable"] is True
    episode_2 = ieee["execution_episodes"][1]
    assert episode_2["status"] == IEEE_TOTAL_DRIFT_RECOVERY_STATUS
    assert episode_2["recovery_of_episode_number"] == 1
    assert episode_2["network_used"] is False
    assert episode_2["known_daily_calls_preserved"] == 15
    assert [
        item["next_start_record"] for item in episode_2["continuation_plan"]
    ] == [4, 4, 4, 4, 4]
    assert checkpoint_path.read_bytes() == checkpoint_bytes
    assert {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    } == raw_responses_before
    assert {
        key: value for key, value in recovered["sources"].items()
        if key != "IEEEXplore"
    } == other_sources_before
    assert recovered["external_retrieval_cutoff_date"] is None

    dataset = external_module.load_review_dataset(
        tmp_path / ieee["checkpoint_dataset"]["path"]
    )
    run = dataset.retrieval_runs[0]
    assert run.completion_status.name == "RUNNING"
    assert run.query_plan_hash == run.metadata["recovery_query_plan_hash"]
    assert (
        run.metadata["failed_episode_query_plan_hash"]
        != run.metadata["recovery_query_plan_hash"]
    )
    assert all(not page.total_is_exact for page in dataset.retrieval_pages)
    assert all(
        query.completion_status.name == "RUNNING"
        and query.metadata["mutable_provider_totals"] is True
        for query in dataset.source_queries
    )
    for binding in episode_2["source_raw_responses"]:
        original = (tmp_path / binding["failed_episode_path"]).read_bytes()
        copied = (tmp_path / binding["recovery_copy_path"]).read_bytes()
        assert copied == original
        assert hashlib.sha256(original).hexdigest() == binding["raw_sha256"]

    repeated = authorize_ieee_total_drift_recovery(
        root=tmp_path, timestamp=clock
    )
    assert repeated == recovered


def test_ieee_total_drift_resume_starts_at_continuations_and_reconciles(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_ieee_total_drift_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    failed_checkpoint = (
        tmp_path / failed["sources"]["IEEEXplore"]["checkpoint_dataset"]["path"]
    )
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    authorize_ieee_total_drift_recovery(root=tmp_path, timestamp=clock)
    http = FakeHttp(
        [
            FakeResponse(
                payload=_ieee_drift_page(
                    family, start_record=4, count=2, total=5
                )
            )
            for family in range(1, 6)
        ]
    )

    completed = execute_external_source_session(
        root=tmp_path,
        source="IEEEXplore",
        http=http,
        resume=True,
        ieee_credential="offline-test-key",
        quota_day_utc="2026-09-04",
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )

    assert [call["params"]["start_record"] for call in http.calls] == [4] * 5
    ieee = completed["sources"]["IEEEXplore"]
    assert ieee["status"] == "COMPLETE"
    assert ieee["completed_query_count"] == 5
    assert ieee["occurrence_count"] == 25
    assert ieee["attempt_count"] == 15
    assert ieee["requests_this_session"] == 5
    assert ieee["ieee_quota"]["known_calls_before_session"] == 15
    assert ieee["ieee_quota"]["known_calls_after_session"] == 20
    assert ieee["execution_episodes"][0]["status"] == "FAILED"
    assert ieee["execution_episodes"][0]["immutable"] is True
    assert ieee["execution_episodes"][1]["status"] == "COMPLETE"
    assert ieee["execution_episodes"][1]["immutable"] is True
    reconciliation = ieee["terminal_reconciliation"]
    assert reconciliation["snapshot_equivalent_completeness_claimed"] is False
    assert all(
        family["observed_provider_totals"] == [6, 5, 5]
        and family["duplicate_identities_across_pages"] == 0
        and family["retrieved_minus_final_provider_total"] == 0
        and family["discrepancy_explainable_by_observed_index_drift"] is True
        for family in reconciliation["families"]
    )
    assert failed_checkpoint.read_bytes() == failed_checkpoint_bytes
    assert completed["external_retrieval_cutoff_date"] is None

    dataset = external_module.load_review_dataset(
        tmp_path / ieee["checkpoint_dataset"]["path"]
    )
    assert all(
        query.metadata["provider_total_observations"] == [6, 5, 5]
        and "next_start_record" not in query.metadata
        for query in dataset.source_queries
    )


def test_ieee_total_drift_recovery_refuses_cross_page_identity_overlap(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _failed_ieee_total_drift_episode(
        tmp_path,
        monkeypatch,
        external_wave,
        external_preflight,
        cross_page_overlap=True,
    )

    with pytest.raises(
        ExternalRetrievalWaveError,
        match="refused changed query/failure provenance",
    ):
        authorize_ieee_total_drift_recovery(root=tmp_path, timestamp=Clock())


def test_ieee_total_drift_recovery_refuses_raw_response_hash_mismatch(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_ieee_total_drift_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = (
        tmp_path / failed["sources"]["IEEEXplore"]["checkpoint_dataset"]["path"]
    )
    response = next((checkpoint.parent / "responses").iterdir())
    response.write_bytes(response.read_bytes() + b"corrupt")

    with pytest.raises(ExternalRetrievalWaveError, match="hash/read failure"):
        authorize_ieee_total_drift_recovery(root=tmp_path, timestamp=clock)


def test_ieee_repeated_window_recovery_builds_episode_3_without_refunding_calls(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    episode_2_state, clock = _failed_ieee_repeated_window_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    ieee_before = episode_2_state["sources"]["IEEEXplore"]
    immutable_checkpoints = {
        episode["episode_number"]: (
            tmp_path / episode["checkpoint_dataset"]["path"]
        ).read_bytes()
        for episode in ieee_before["execution_episodes"]
    }
    episode_2_checkpoint = tmp_path / ieee_before["checkpoint_dataset"]["path"]
    episode_2_responses = {
        path.name: path.read_bytes()
        for path in sorted((episode_2_checkpoint.parent / "responses").iterdir())
    }

    recovered = authorize_ieee_repeated_window_recovery(
        root=tmp_path, timestamp=clock
    )

    ieee = recovered["sources"]["IEEEXplore"]
    assert ieee["status"] == IEEE_REPEATED_WINDOW_RECOVERY_STATUS
    assert ieee["active_episode_number"] == 3
    assert ieee["ieee_quota"]["known_calls_after_session"] == 92
    assert ieee["requests_this_session"] == 0
    assert ieee["preserved_source_attempt_count"] == 20
    assert ieee["rejected_page_count"] == 5
    assert [episode["immutable"] for episode in ieee["execution_episodes"]] == [
        True,
        True,
        False,
    ]
    assert [episode["status"] for episode in ieee["execution_episodes"][:2]] == [
        "FAILED",
        "FAILED",
    ]
    episode_3 = ieee["execution_episodes"][2]
    assert episode_3["retained_page_count"] == 15
    assert len(episode_3["rejection_evidence"]) == 5
    assert len(episode_3["source_raw_responses"]) == 20
    assert len(episode_3["rejected_raw_responses"]) == 5
    assert episode_3["known_daily_calls_preserved"] == 92
    assert episode_3["remaining_daily_calls"] == 108
    assert [
        item["next_start_record"] for item in episode_3["continuation_plan"]
    ] == [6, 6, 6, 6, 6]
    assert all(
        item["classification"] == "REJECTED_WHOLE_PROVIDER_WINDOW_REPETITION"
        for item in episode_3["rejection_evidence"]
    )
    assert recovered["external_retrieval_cutoff_date"] is None

    for episode in ieee["execution_episodes"][:2]:
        path = tmp_path / episode["checkpoint_dataset"]["path"]
        assert path.read_bytes() == immutable_checkpoints[episode["episode_number"]]
    assert {
        path.name: path.read_bytes()
        for path in sorted((episode_2_checkpoint.parent / "responses").iterdir())
    } == episode_2_responses
    for binding in episode_3["source_raw_responses"]:
        source = tmp_path / binding["episode_2_path"]
        copied = tmp_path / binding["recovery_copy_path"]
        assert copied.read_bytes() == source.read_bytes()
        assert hashlib.sha256(source.read_bytes()).hexdigest() == binding["raw_sha256"]

    dataset = external_module.load_review_dataset(
        tmp_path / ieee["checkpoint_dataset"]["path"]
    )
    assert len(dataset.retrieval_pages) == 15
    assert len(dataset.retrieval_attempts) == 15
    assert len(dataset.occurrences) == 20
    assert [query.result_count for query in dataset.source_queries] == [4] * 5
    assert [
        query.metadata["next_start_record"] for query in dataset.source_queries
    ] == [6] * 5
    assert all(
        query.metadata["provider_total_observations"] == [6, 5, 7, 7]
        and query.metadata["recovery_retained_page_count"] == 3
        for query in dataset.source_queries
    )
    for query in dataset.source_queries:
        identifiers = [
            identifier
            for page in dataset.retrieval_pages
            if page.source_query_id == query.query_id
            for identifier in page.native_identifiers
        ]
        assert len(identifiers) == len(set(identifiers))

    repeated = authorize_ieee_repeated_window_recovery(
        root=tmp_path, timestamp=clock
    )
    assert repeated == recovered


def test_ieee_episode_3_live_resume_begins_at_corrected_boundaries(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _failed_ieee_repeated_window_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_ieee_repeated_window_recovery(root=tmp_path, timestamp=clock)
    monkeypatch.setattr(external_module, "_ieee_calls_on_day", lambda *_: 92)
    http = FakeHttp(
        [
            FakeResponse(
                payload=_ieee_drift_page(
                    family, start_record=6, count=2, total=7
                )
            )
            for family in range(1, 6)
        ]
    )

    completed = execute_external_source_session(
        root=tmp_path,
        source="IEEEXplore",
        http=http,
        resume=True,
        ieee_credential="offline-test-key",
        quota_day_utc="2026-09-04",
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )

    assert [call["params"]["start_record"] for call in http.calls] == [6] * 5
    ieee = completed["sources"]["IEEEXplore"]
    assert ieee["status"] == "COMPLETE"
    assert ieee["ieee_quota"]["known_calls_before_session"] == 92
    assert ieee["ieee_quota"]["known_calls_after_session"] == 97
    assert [episode["immutable"] for episode in ieee["execution_episodes"]] == [
        True,
        True,
        True,
    ]
    assert all(
        family["observed_provider_totals"] == [6, 5, 7, 7, 7]
        for family in ieee["terminal_reconciliation"]["families"]
    )


def test_ieee_repeated_window_recovery_refuses_response_hash_corruption(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, clock = _failed_ieee_repeated_window_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / state["sources"]["IEEEXplore"][
        "checkpoint_dataset"
    ]["path"]
    response = next((checkpoint.parent / "responses").iterdir())
    response.write_bytes(response.read_bytes() + b"corrupt")

    with pytest.raises(ExternalRetrievalWaveError, match="hash/read failure"):
        authorize_ieee_repeated_window_recovery(root=tmp_path, timestamp=clock)


def test_ieee_repeated_window_recovery_refuses_nonidentical_overlap(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, clock = _failed_ieee_repeated_window_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / state["sources"]["IEEEXplore"][
        "checkpoint_dataset"
    ]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    rejected_page = dataset.retrieval_pages[3]
    attempt = next(
        item for item in dataset.retrieval_attempts if item.page_id == rejected_page.page_id
    )
    response_path = checkpoint.parent / attempt.raw_response_path
    envelope = json.loads(response_path.read_text(encoding="utf-8"))
    payload = json.loads(base64.b64decode(envelope["content_base64"]))
    payload["articles"].append({"article_number": "not-the-same-page", "title": "X"})
    envelope["content_base64"] = base64.b64encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).decode()
    encoded = (json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n").encode()
    response_path.write_bytes(encoded)
    attempt.raw_response_hash = hashlib.sha256(encoded).hexdigest()
    _rewrite_active_ieee_checkpoint(tmp_path, state, dataset)

    with pytest.raises(
        ExternalRetrievalWaveError, match="page or occurrence accounting changed"
    ):
        authorize_ieee_repeated_window_recovery(root=tmp_path, timestamp=clock)


def test_ieee_repeated_window_recovery_refuses_malformed_rejection_signature(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, clock = _failed_ieee_repeated_window_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / state["sources"]["IEEEXplore"][
        "checkpoint_dataset"
    ]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    dataset.source_queries[0].errors = ["unsupported rejection signature"]
    _rewrite_active_ieee_checkpoint(tmp_path, state, dataset)

    with pytest.raises(
        ExternalRetrievalWaveError, match="malformed repeated-window rejection signature"
    ):
        authorize_ieee_repeated_window_recovery(root=tmp_path, timestamp=clock)


def test_ieee_repeated_window_recovery_refuses_unexplained_pagination_gap(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    state, clock = _failed_ieee_repeated_window_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = tmp_path / state["sources"]["IEEEXplore"][
        "checkpoint_dataset"
    ]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    query = dataset.source_queries[0]
    pages = [
        page for page in dataset.retrieval_pages if page.source_query_id == query.query_id
    ]
    preceding, rejected = pages[-2:]
    preceding.next_state = {"start_record": 7}
    rejected.request_state = {"start_record": 7}
    attempt = next(
        item for item in dataset.retrieval_attempts if item.page_id == rejected.page_id
    )
    spec = external_module._source_query_specs(
        external_wave,
        "IEEEXplore",
        ieee_credential="offline-recovery-redacted",
        ieee_mutable_total_mode=True,
    )[0]
    request = IeeeXplorePaginator().build_request(spec, rejected.request_state)
    attempt.request_params = request.sanitized_params()
    attempt.request_headers = request.sanitized_headers()
    attempt.request_hash = request.request_hash()
    _rewrite_active_ieee_checkpoint(tmp_path, state, dataset)

    with pytest.raises(
        ExternalRetrievalWaveError, match="unexplained pagination gap"
    ):
        authorize_ieee_repeated_window_recovery(root=tmp_path, timestamp=clock)


def test_europe_pmc_terminal_recovery_preserves_failure_and_reconstructs_all_queries(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_europe_pmc_terminal_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    europe_before = failed["sources"]["EuropePMC"]
    checkpoint_ref = europe_before["checkpoint_dataset"]
    checkpoint_path = tmp_path / checkpoint_ref["path"]
    checkpoint_bytes = checkpoint_path.read_bytes()
    response_dir = checkpoint_path.parent / "responses"
    raw_responses_before = {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    }
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in failed["sources"].items()
        if key != "EuropePMC"
    }

    recovered = authorize_europe_pmc_terminal_recovery(
        root=tmp_path, timestamp=clock
    )

    europe = recovered["sources"]["EuropePMC"]
    assert europe["status"] == "COMPLETE"
    assert europe["completed_query_count"] == 5
    assert europe["occurrence_count"] == 5
    assert europe["requests_this_session"] == 0
    assert europe["active_episode_number"] == 2
    assert europe["execution_episodes"][0]["status"] == "FAILED"
    assert europe["execution_episodes"][0]["immutable"] is True
    episode_2 = europe["execution_episodes"][1]
    assert episode_2["status"] == EUROPE_PMC_TERMINAL_RECOVERY_STATUS
    assert episode_2["recovery_of_episode_number"] == 1
    assert episode_2["network_used"] is False
    assert len(episode_2["source_raw_responses"]) == 10
    assert checkpoint_path.read_bytes() == checkpoint_bytes
    assert {
        path.name: path.read_bytes() for path in sorted(response_dir.iterdir())
    } == raw_responses_before
    assert {
        key: value for key, value in recovered["sources"].items() if key != "EuropePMC"
    } == other_sources_before
    assert recovered["external_retrieval_cutoff_date"] is None

    dataset = external_module.load_review_dataset(
        tmp_path / europe["checkpoint_dataset"]["path"]
    )
    assert len(dataset.occurrences) == 5
    assert len(dataset.retrieval_attempts) == 10
    assert all(
        query.completion_status.name == "COMPLETE"
        for query in dataset.source_queries
    )
    terminal_pages = [page for page in dataset.retrieval_pages if page.terminal]
    assert len(terminal_pages) == 5
    assert sum(
        page.metadata.get("repeated_cursor_terminal_sentinel") is True
        for page in terminal_pages
    ) == 4
    for binding in episode_2["source_raw_responses"]:
        original = (tmp_path / binding["failed_episode_path"]).read_bytes()
        copied = (tmp_path / binding["recovery_copy_path"]).read_bytes()
        assert copied == original
        assert hashlib.sha256(original).hexdigest() == binding["raw_sha256"]

    repeated = authorize_europe_pmc_terminal_recovery(
        root=tmp_path, timestamp=clock
    )
    assert repeated == recovered


def test_europe_pmc_terminal_recovery_refuses_provider_count_mismatch(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock = _failed_europe_pmc_terminal_episode(
        tmp_path,
        monkeypatch,
        external_wave,
        external_preflight,
        provider_counts=(2, 1, 1, 1, 1),
    )

    with pytest.raises(ExternalRetrievalWaveError, match="provider hitCount changed"):
        authorize_europe_pmc_terminal_recovery(root=tmp_path, timestamp=clock)


def test_europe_pmc_terminal_recovery_refuses_raw_response_hash_mismatch(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    failed, clock = _failed_europe_pmc_terminal_episode(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint = (
        tmp_path / failed["sources"]["EuropePMC"]["checkpoint_dataset"]["path"]
    )
    response = next((checkpoint.parent / "responses").iterdir())
    response.write_bytes(response.read_bytes() + b"corrupt")

    with pytest.raises(ExternalRetrievalWaveError, match="hash/read failure"):
        authorize_europe_pmc_terminal_recovery(root=tmp_path, timestamp=clock)


def test_source_execution_is_idempotent_after_complete(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    http = FakeHttp(
        [FakeResponse(content=_arxiv_feed(f"test.{index}")) for index in range(5)]
    )
    state = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=http,
        resume=False,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert state["sources"]["arXiv"]["status"] == "COMPLETE"
    assert state["external_retrieval_cutoff_date"] is None
    assert len(http.calls) == 5

    no_calls = FakeHttp([])
    repeated = execute_external_source_session(
        root=tmp_path,
        source="arXiv",
        http=no_calls,
        resume=True,
        timestamp=Clock(),
    )
    assert repeated["sources"]["arXiv"]["status"] == "COMPLETE"
    assert no_calls.calls == []


def _install_semantic_runtime(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    control_target = tmp_path / "config/star_query_semantic_controls_v0_3.json"
    control_target.parent.mkdir(parents=True, exist_ok=True)
    control_target.write_bytes(
        (ROOT / "config/star_query_semantic_controls_v0_3.json").read_bytes()
    )


def _passing_semantic_controls() -> list[FakeResponse]:
    return [
        FakeResponse(payload={"total": count})
        for count in [100, 80, 20, 160, 30, 30]
    ]


def _semantic_candidate_responses() -> list[FakeResponse]:
    return [
        FakeResponse(
            payload={
                "total": 1,
                "data": [{"paperId": f"S{index}", "title": f"Paper {index}"}],
            }
        )
        for index in range(1, 6)
    ]


def _semantic_500() -> FakeResponse:
    return FakeResponse(
        status_code=500,
        payload={"message": "Internal Server Error"},
    )


def _blocked_semantic_control_5xx_gate(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    responses = [
        _semantic_500(),
        FakeResponse(payload={"total": 943168}),
        _semantic_500(),
        _semantic_500(),
        FakeResponse(payload={"total": 955441}),
        FakeResponse(payload={"total": 14246}),
        _semantic_500(),
        _semantic_500(),
        _semantic_500(),
    ]
    clock = Clock()
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=FakeHttp(responses),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert paused["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_TRANSIENT_PROVIDER"
    )

    gate_path = tmp_path / SEMANTIC_CONTROL_GATE_PATH
    gate = json.loads(gate_path.read_text())
    gate["status"] = "UNRESOLVED"
    gate.pop("pause_state")
    gate.pop("pause_reason")
    gate.pop("pause_metadata")
    gate["controls"][-1]["status"] = "UNRESOLVED"
    gate["controls"][-1]["attempts"][-1].pop("provider_pause")
    gate["assertion_results"] = []
    gate["failed_assertion_ids"] = []
    gate["unresolved_control_ids"] = ["a-or-b"]
    external_module._save_hashed_json(gate_path, gate, "manifest_hash")
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CONTROL_BLOCKED_MANIFEST_RAW_SHA256",
        hashlib.sha256(gate_path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CONTROL_BLOCKED_MANIFEST_LOGICAL_HASH",
        gate["manifest_hash"],
    )

    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    state = json.loads(state_path.read_text())
    source = state["sources"]["SemanticScholar"]
    source["status"] = "BLOCKED_SEMANTIC_CONTROL_GATE"
    source["failure_reason"] = "Semantic Scholar control gate is UNRESOLVED"
    source["pause_reason"] = None
    source.pop("pause_metadata", None)
    source.pop("control_requests_this_session", None)
    source["candidate_request_count"] = 0
    source["semantic_control_gate"] = {
        "status": "UNRESOLVED",
        "manifest_path": SEMANTIC_CONTROL_GATE_PATH,
        "manifest_hash": gate["manifest_hash"],
    }
    external_module._save_execution_state(state_path, state)
    return state, clock


def _failed_semantic_candidate_5xx_checkpoint(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CONTROL_RECOVERY_GATE_PATH",
        SEMANTIC_CONTROL_GATE_PATH,
    )
    original_executor = external_module.execute_paginated_retrieval_run

    def terminal_5xx_executor(**kwargs):
        kwargs["resumable_provider_5xx_exhaustion_sources"] = frozenset()
        return original_executor(**kwargs)

    monkeypatch.setattr(
        external_module,
        "execute_paginated_retrieval_run",
        terminal_5xx_executor,
    )
    candidate_responses = [
        FakeResponse(
            payload={
                "total": 1,
                "data": [{"paperId": "S1", "title": "QF01"}],
            }
        ),
        FakeResponse(
            payload={
                "total": 2,
                "token": "token-qf02",
                "data": [{"paperId": "S2", "title": "QF02 first"}],
            }
        ),
        _semantic_500(),
        _semantic_500(),
        _semantic_500(),
        FakeResponse(
            payload={
                "total": 2,
                "token": "token-qf03",
                "data": [{"paperId": "S3", "title": "QF03 first"}],
            }
        ),
        _semantic_500(),
        _semantic_500(),
        _semantic_500(),
        FakeResponse(
            payload={
                "total": 1,
                "data": [{"paperId": "S4", "title": "QF04"}],
            }
        ),
        FakeResponse(
            payload={
                "total": 1,
                "data": [{"paperId": "S5", "title": "QF05"}],
            }
        ),
    ]
    clock = Clock()
    failed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=FakeHttp([*_passing_semantic_controls(), *candidate_responses]),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert failed["sources"]["SemanticScholar"]["status"] == "FAILED"
    monkeypatch.setattr(
        external_module,
        "execute_paginated_retrieval_run",
        original_executor,
    )

    checkpoint = (
        tmp_path
        / external_module.EXECUTION_ROOT
        / "SemanticScholar/checkpoint/review_dataset.json"
    )
    control = tmp_path / SEMANTIC_CONTROL_GATE_PATH
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_CHECKPOINT_SHA256",
        hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_CONTROL_SHA256",
        hashlib.sha256(control.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_CONTROL_LOGICAL_HASH",
        json.loads(control.read_text())["manifest_hash"],
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_COUNTS",
        (
            ("COMPLETE", 1, 1),
            ("FAILED", 2, 1),
            ("FAILED", 2, 1),
            ("COMPLETE", 1, 1),
            ("COMPLETE", 1, 1),
        ),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_CONTINUATION_TOKENS",
        (None, "token-qf02", "token-qf03", None, None),
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_CANDIDATE_5XX_EXPECTED_ATTEMPTS", 11
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_CANDIDATE_5XX_EXPECTED_RESPONSES", 11
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_SUCCESSFUL_PAGES",
        5,
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_CANDIDATE_5XX_EXPECTED_TOTAL_PAGES", 7
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_HTTP_STATUSES",
        {200: 5, 500: 6},
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_CANDIDATE_5XX_EXPECTED_OCCURRENCES", 5
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_CANONICAL_RECORDS",
        5,
    )

    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    state = json.loads(state_path.read_text())
    source = state["sources"]["SemanticScholar"]
    source.update(
        {
            "status": "RUNNING",
            "completed_query_count": 2,
            "attempt_count": 8,
            "occurrence_count": 4,
            "checkpoint_dataset": {
                "path": checkpoint.relative_to(tmp_path).as_posix(),
                "byte_size": 1,
                "raw_sha256": "stale-pointer",
            },
            "last_session_completed_at_utc": None,
        }
    )
    source["failure_reason"] = None
    external_module._save_execution_state(state_path, state)
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_CANDIDATE_5XX_EXPECTED_STALE_STATE_SHA256",
        hashlib.sha256(state_path.read_bytes()).hexdigest(),
    )
    return state, clock, checkpoint


def _failed_semantic_native_id_overlap_episode_2(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    duplicate_id = "zz-overlap-native-id"
    duplicate_doi = "10.1000/overlap"
    second_duplicate_id = "zzzz-second-overlap-native-id"

    def duplicate_record(author_name: str) -> dict:
        return {
            "paperId": duplicate_id,
            "externalIds": {"DOI": duplicate_doi},
            "title": "Provider repeated paper",
            "authors": [
                {"authorId": "a1", "name": "First Author"},
                {"authorId": "a2", "name": "Second Author"},
                {"authorId": "a3", "name": author_name},
            ],
            "venue": "Test Venue",
            "year": 2026,
            "url": f"https://example.test/{duplicate_id}",
            "abstract": "Same abstract",
            "isOpenAccess": False,
            "openAccessPdf": None,
        }

    qf03_responses = []
    for ordinal in range(43):
        if ordinal == 2:
            data = [
                duplicate_record("J. Example"),
                {"paperId": "id-0002b", "title": "Page 2 tail"},
            ]
        elif ordinal == 3:
            data = [
                _second_semantic_overlap_record(second_duplicate_id),
                {"paperId": "id-0003b", "title": "Page 3 tail"},
            ]
        elif ordinal == 42:
            data = [
                duplicate_record("Jennifer Example"),
                {"paperId": "zzz-tail-0042", "title": "Page 42 tail"},
            ]
        else:
            data = [
                {
                    "paperId": f"id-{ordinal:04d}",
                    "title": f"QF03 page {ordinal}",
                }
            ]
        qf03_responses.append(
            FakeResponse(
                payload={
                    "total": 1_000 + ordinal,
                    "token": f"token-{ordinal + 1}",
                    "data": data,
                }
            )
        )
    candidate_responses = [
        FakeResponse(payload={"total": 1, "data": [{"paperId": "qf01"}]}),
        FakeResponse(payload={"total": 1, "data": [{"paperId": "qf02"}]}),
        *qf03_responses,
        FakeResponse(payload={"total": 1, "data": [{"paperId": "qf04"}]}),
        FakeResponse(payload={"total": 1, "data": [{"paperId": "qf05"}]}),
    ]
    clock = Clock()
    failed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=FakeHttp([*_passing_semantic_controls(), *candidate_responses]),
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert failed["sources"]["SemanticScholar"]["status"] == "FAILED"

    source_root = (
        tmp_path / external_module.EXECUTION_ROOT / "SemanticScholar"
    )
    original_checkpoint_dir = source_root / "checkpoint"
    episode_2_checkpoint_dir = source_root / "episodes/episode-002/checkpoint"
    episode_2_checkpoint_dir.parent.mkdir(parents=True)
    original_checkpoint_dir.rename(episode_2_checkpoint_dir)
    checkpoint = episode_2_checkpoint_dir / "review_dataset.json"
    dataset = external_module.load_review_dataset(checkpoint)
    qf03 = dataset.source_queries[2]
    qf03_pages = sorted(
        (
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == qf03.query_id
        ),
        key=lambda page: page.ordinal,
    )
    pair = sorted(
        (
            occurrence
            for occurrence in dataset.occurrences
            if occurrence.source_identifier == duplicate_id
        ),
        key=lambda occurrence: occurrence.page,
    )
    attempts = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    expected_occurrences = []
    for occurrence, author_name in zip(
        pair, ["J. Example", "Jennifer Example"], strict=True
    ):
        page = next(
            page
            for page in qf03_pages
            if page.page_id == occurrence.retrieval_page_id
        )
        attempt = next(
            attempts[attempt_id]
            for attempt_id in page.attempt_ids
            if attempts[attempt_id].status is RetrievalAttemptStatus.SUCCEEDED
        )
        expected_occurrences.append(
            {
                "ordinal": page.ordinal,
                "page_id": page.page_id,
                "occurrence_id": occurrence.occurrence_id,
                "raw_payload_hash": occurrence.raw_payload_hash,
                "response_path": attempt.raw_response_path,
                "response_sha256": attempt.raw_response_hash,
                "provider_total": page.source_reported_total,
                "author_name": author_name,
            }
        )
    canonical = next(
        item
        for item in dataset.canonical_records
        if pair[0].occurrence_id in item.occurrence_ids
    )
    parent_reference = external_module._file_reference(checkpoint, tmp_path)
    error = f"source repeated native identifiers across pages: ['{duplicate_id}']"
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SHA256",
        parent_reference["raw_sha256"],
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SIZE",
        parent_reference["byte_size"],
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID", duplicate_id
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID", qf03.query_id
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_NATIVE_ID_OVERLAP_ERROR", error
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES",
        tuple(expected_occurrences),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_CANONICAL_ID",
        canonical.canonical_id,
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_DEDUPE_KEY",
        f"doi:{duplicate_doi}",
    )
    qf_counts = tuple(
        (
            query.completion_status.value,
            len(
                [
                    page
                    for page in dataset.retrieval_pages
                    if page.source_query_id == query.query_id
                ]
            ),
            query.result_count,
        )
        for query in dataset.source_queries
    )
    monkeypatch.setattr(
        external_module, "SEMANTIC_NATIVE_ID_OVERLAP_QF_COUNTS", qf_counts
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_ATTEMPTS",
        len(dataset.retrieval_attempts),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_PAGES",
        len(dataset.retrieval_pages),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_OCCURRENCES",
        len(dataset.occurrences),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_CANONICAL_RECORDS",
        len(dataset.canonical_records),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_QF03_DISTINCT_IDS",
        len({item for page in qf03_pages for item in page.native_identifiers}),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_ORDERING_ANOMALIES",
        (
            {
                "page_ordinal": 2,
                "left_index": 0,
                "left_paper_id": duplicate_id,
                "right_paper_id": "id-0002b",
            },
            {
                "page_ordinal": 3,
                "left_index": 0,
                "left_paper_id": second_duplicate_id,
                "right_paper_id": "id-0003b",
            },
        ),
    )

    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    state = json.loads(state_path.read_text())
    source = state["sources"]["SemanticScholar"]
    checkpoint_relative = episode_2_checkpoint_dir.relative_to(tmp_path).as_posix()
    episode_1 = {
        "episode_number": 1,
        "episode_id": "SemanticScholar-episode-001",
        "run_id": dataset.retrieval_runs[0].run_id,
        "status": "FAILED",
        "immutable": True,
    }
    episode_2 = {
        "episode_number": 2,
        "episode_id": "SemanticScholar-episode-002",
        "run_id": dataset.retrieval_runs[0].run_id,
        "status": "FAILED",
        "checkpoint_path": checkpoint_relative,
        "checkpoint_dataset": parent_reference,
        "failure_reason": f"{qf03.query_id}: {error}",
        "immutable": True,
    }
    source.update(
        {
            "status": "FAILED",
            "execution_episodes": [episode_1, episode_2],
            "active_episode_number": 2,
            "active_run_id": dataset.retrieval_runs[0].run_id,
            "active_checkpoint_path": checkpoint_relative,
            "checkpoint_path": checkpoint_relative,
            "checkpoint_dataset": parent_reference,
            "completed_query_count": 4,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "failure_reason": f"{qf03.query_id}: {error}",
        }
    )
    external_module._save_execution_state(state_path, state)
    return state, clock, checkpoint, duplicate_id


def _second_semantic_overlap_record(paper_id: str) -> dict:
    return {
        "paperId": paper_id,
        "externalIds": {"DOI": "10.1000/second-overlap"},
        "title": "Provider repeated second paper",
        "authors": [{"authorId": "b1", "name": "Same Author"}],
        "venue": "Test Venue",
        "year": 2025,
        "url": f"https://example.test/{paper_id}",
        "abstract": "Second overlap abstract",
        "isOpenAccess": True,
        "openAccessPdf": {"url": "https://example.test/second.pdf"},
    }


def _failed_semantic_native_id_overlap_episode_3(
    tmp_path, monkeypatch, external_wave, external_preflight
):
    _, clock, episode_2_checkpoint, first_duplicate_id = (
        _failed_semantic_native_id_overlap_episode_2(
            tmp_path, monkeypatch, external_wave, external_preflight
        )
    )
    authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    second_duplicate_id = "zzzz-second-overlap-native-id"
    responses = []
    for ordinal in range(43, 55):
        data = (
            [_second_semantic_overlap_record(second_duplicate_id)]
            if ordinal == 54
            else [
                {
                    "paperId": f"zzza-id-{ordinal:04d}",
                    "title": f"QF03 page {ordinal}",
                }
            ]
        )
        responses.append(
            FakeResponse(
                payload={
                    "total": 1_000 + ordinal,
                    "token": f"token-{ordinal + 1}",
                    "data": data,
                }
            )
        )
    failed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=FakeHttp(responses),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    source = failed["sources"]["SemanticScholar"]
    assert source["status"] == "FAILED"
    assert source["active_episode_number"] == 3
    episode_3_checkpoint = tmp_path / source["checkpoint_dataset"]["path"]
    dataset = external_module.load_review_dataset(episode_3_checkpoint)
    qf03 = dataset.source_queries[2]
    pages = sorted(
        (
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == qf03.query_id
        ),
        key=lambda page: page.ordinal,
    )
    pair = sorted(
        (
            occurrence
            for occurrence in dataset.occurrences
            if occurrence.source_identifier == second_duplicate_id
        ),
        key=lambda occurrence: occurrence.page,
    )
    assert [occurrence.page for occurrence in pair] == [3, 54]
    attempts = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    expected_occurrences = []
    for occurrence in pair:
        page = next(
            page
            for page in pages
            if page.page_id == occurrence.retrieval_page_id
        )
        attempt = next(
            attempts[attempt_id]
            for attempt_id in page.attempt_ids
            if attempts[attempt_id].status is RetrievalAttemptStatus.SUCCEEDED
        )
        expected_occurrences.append(
            {
                "ordinal": page.ordinal,
                "page_id": page.page_id,
                "occurrence_id": occurrence.occurrence_id,
                "raw_payload_hash": occurrence.raw_payload_hash,
                "response_path": attempt.raw_response_path,
                "response_sha256": attempt.raw_response_hash,
                "provider_total": page.source_reported_total,
            }
        )
    canonical = next(
        item
        for item in dataset.canonical_records
        if pair[0].occurrence_id in item.occurrence_ids
    )
    parent_reference = external_module._file_reference(
        episode_3_checkpoint, tmp_path
    )
    error = (
        "source repeated native identifiers across pages: "
        f"['{second_duplicate_id}']"
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SHA256",
        parent_reference["raw_sha256"],
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SIZE",
        parent_reference["byte_size"],
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PAPER_ID",
        second_duplicate_id,
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_ERROR",
        error,
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_OCCURRENCES",
        tuple(expected_occurrences),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_CANONICAL_ID",
        canonical.canonical_id,
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_DEDUPE_KEY",
        "doi:10.1000/second-overlap",
    )
    qf_counts = tuple(
        (
            query.completion_status.value,
            len(
                [
                    page
                    for page in dataset.retrieval_pages
                    if page.source_query_id == query.query_id
                ]
            ),
            query.result_count,
        )
        for query in dataset.source_queries
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_QF_COUNTS",
        qf_counts,
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_ATTEMPTS",
        len(dataset.retrieval_attempts),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_PAGES",
        len(dataset.retrieval_pages),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_OCCURRENCES",
        len(dataset.occurrences),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_CANONICAL_RECORDS",
        len(dataset.canonical_records),
    )
    monkeypatch.setattr(
        external_module,
        "SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_QF03_DISTINCT_IDS",
        len({native_id for page in pages for native_id in page.native_identifiers}),
    )
    return (
        failed,
        clock,
        episode_2_checkpoint,
        episode_3_checkpoint,
        first_duplicate_id,
        second_duplicate_id,
    )


def test_semantic_control_5xx_exhaustion_pauses_with_fresh_resume_budget(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    final_response = _semantic_500()
    final_response.headers = {"Retry-After": "17"}
    first_http = FakeHttp([_semantic_500(), _semantic_500(), final_response])
    clock = Clock()
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    source = paused["sources"]["SemanticScholar"]
    assert len(first_http.calls) == 3
    assert source["status"] == "PAUSED_TRANSIENT_PROVIDER"
    assert source["candidate_request_count"] == 0
    assert source["pause_metadata"] == {
        "source_database": "SemanticScholar",
        "probe_id": "atomic-a",
        "http_statuses": [500, 500, 500],
        "retry_after_header_present": True,
        "retry_after": "17",
        "attempts_this_invocation": 3,
        "maximum_attempts_per_invocation": 3,
    }

    second_http = FakeHttp([_semantic_500() for _ in range(3)])
    paused_again = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=second_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert len(second_http.calls) == 3
    assert paused_again["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_TRANSIENT_PROVIDER"
    )
    gate = json.loads((tmp_path / SEMANTIC_CONTROL_GATE_PATH).read_text())
    assert [
        attempt["attempt_number"] for attempt in gate["controls"][0]["attempts"]
    ] == [1, 2, 3, 4, 5, 6]
    assert all(
        attempt["response"]["sha256"]
        for attempt in gate["controls"][0]["attempts"]
    )


def test_semantic_candidate_5xx_exhaustion_maps_to_resumable_source_pause(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    clock = Clock()
    first_http = FakeHttp(
        [*_passing_semantic_controls(), *[_semantic_500() for _ in range(3)]]
    )
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    source = paused["sources"]["SemanticScholar"]
    assert source["status"] == "PAUSED_TRANSIENT_PROVIDER"
    assert source["completed_query_count"] == 0
    assert source["requests_this_session"] == 3
    assert source["pause_reason"] == "RETRYABLE_PROVIDER_5XX_EXHAUSTED"
    assert source["pause_metadata"]["http_statuses"] == [500, 500, 500]

    resumed_http = FakeHttp(_semantic_candidate_responses())
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=resumed_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert len(resumed_http.calls) == 5


def test_semantic_control_5xx_recovery_preserves_evidence_and_resumes_in_order(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    blocked, clock = _blocked_semantic_control_5xx_gate(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    gate_path = tmp_path / SEMANTIC_CONTROL_GATE_PATH
    gate_bytes = gate_path.read_bytes()
    response_bytes = {
        path.name: path.read_bytes()
        for path in sorted((gate_path.parent / "responses").iterdir())
    }
    other_sources = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in blocked["sources"].items()
        if key != "SemanticScholar"
    }

    recovered = authorize_semantic_scholar_control_5xx_recovery(
        root=tmp_path, timestamp=clock
    )
    source = recovered["sources"]["SemanticScholar"]
    assert source["status"] == SEMANTIC_CONTROL_5XX_RECOVERY_STATUS
    assert source["candidate_request_count"] == 0
    assert source["semantic_control_gate"]["manifest_path"] == (
        SEMANTIC_CONTROL_RECOVERY_GATE_PATH
    )
    assert source["control_gate_recovery"]["retained_successful_control_ids"] == [
        "atomic-a",
        "atomic-b",
        "a-and-b",
    ]
    assert source["control_gate_recovery"]["reopened_control_id"] == "a-or-b"
    assert source["control_gate_recovery"]["unattempted_control_ids"] == [
        "grouped-left",
        "grouped-right",
    ]
    assert source["control_gate_recovery"]["source_response_count"] == 9
    assert gate_path.read_bytes() == gate_bytes
    assert {
        name: (gate_path.parent / "responses" / name).read_bytes()
        for name in response_bytes
    } == response_bytes
    assert {
        key: value
        for key, value in recovered["sources"].items()
        if key != "SemanticScholar"
    } == other_sources
    assert recovered["external_retrieval_cutoff_date"] is None
    assert (
        authorize_semantic_scholar_control_5xx_recovery(
            root=tmp_path, timestamp=clock
        )
        == recovered
    )

    paused_http = FakeHttp([_semantic_500() for _ in range(3)])
    paused_again = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=paused_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert len(paused_http.calls) == 3
    assert all(
        call["params"]["query"] == "visualization | biology"
        for call in paused_http.calls
    )
    assert paused_again["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_TRANSIENT_PROVIDER"
    )
    active_gate_path = tmp_path / SEMANTIC_CONTROL_RECOVERY_GATE_PATH
    active_gate = json.loads(active_gate_path.read_text())
    assert [item["probe_id"] for item in active_gate["controls"]] == [
        "atomic-a",
        "atomic-b",
        "a-and-b",
        "a-or-b",
    ]

    live_http = FakeHttp(
        [
            FakeResponse(payload={"total": 1883934}),
            FakeResponse(payload={"total": 86875}),
            FakeResponse(payload={"total": 86875}),
            *_semantic_candidate_responses(),
        ]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=live_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert len(live_http.calls) == 8
    assert [call["params"]["query"] for call in live_http.calls[:3]] == [
        "visualization | biology",
        "visualization + (biology | interactive)",
        "(visualization + biology) | (visualization + interactive)",
    ]
    final_gate = json.loads(active_gate_path.read_text())
    assert final_gate["status"] == "PASSED"
    assert final_gate["failed_assertion_ids"] == []
    assert all(item["passed"] for item in final_gate["assertion_results"])
    assert [
        len(item["attempts"]) for item in final_gate["controls"][:3]
    ] == [2, 3, 1]
    assert gate_path.read_bytes() == gate_bytes
    for name, content in response_bytes.items():
        assert (gate_path.parent / "responses" / name).read_bytes() == content


def test_semantic_candidate_5xx_recovery_preserves_and_resumes_only_failed_qfs(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    before, clock, checkpoint = _failed_semantic_candidate_5xx_checkpoint(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    checkpoint_bytes = checkpoint.read_bytes()
    response_bytes = {
        path.name: path.read_bytes()
        for path in sorted((checkpoint.parent / "responses").iterdir())
    }
    other_sources = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in before["sources"].items()
        if key != "SemanticScholar"
    }

    recovered = authorize_semantic_scholar_candidate_5xx_recovery(
        root=tmp_path, timestamp=clock
    )
    source = recovered["sources"]["SemanticScholar"]
    assert source["status"] == SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS
    assert source["active_episode_number"] == 2
    assert source["completed_query_count"] == 3
    assert source["requests_this_session"] == 0
    assert source["checkpoint_dataset"] != before["sources"][
        "SemanticScholar"
    ]["checkpoint_dataset"]
    assert source["checkpoint_dataset"] == source["execution_episodes"][1][
        "checkpoint_dataset"
    ]
    assert source["execution_episodes"][0]["immutable"] is True
    assert source["execution_episodes"][1]["immutable"] is False
    assert checkpoint.read_bytes() == checkpoint_bytes
    assert {
        name: (checkpoint.parent / "responses" / name).read_bytes()
        for name in response_bytes
    } == response_bytes
    assert {
        key: value
        for key, value in recovered["sources"].items()
        if key != "SemanticScholar"
    } == other_sources
    assert recovered["external_retrieval_cutoff_date"] is None

    active_checkpoint = tmp_path / source["checkpoint_dataset"]["path"]
    active = external_module.load_review_dataset(active_checkpoint)
    assert [query.completion_status.value for query in active.source_queries] == [
        "complete",
        "running",
        "running",
        "complete",
        "complete",
    ]
    assert [
        page.request_state
        for page in active.retrieval_pages
        if page.status.value == "running"
    ] == [
        {"mode": "bulk", "token": "token-qf02"},
        {"mode": "bulk", "token": "token-qf03"},
    ]
    assert len(active.retrieval_attempts) == 11
    assert len(active.occurrences) == 5
    assert len(source["execution_episodes"][1]["source_raw_responses"]) == 11
    assert all(
        (tmp_path / binding["recovery_copy_path"]).read_bytes()
        == (tmp_path / binding["episode_1_path"]).read_bytes()
        for binding in source["execution_episodes"][1]["source_raw_responses"]
    )
    assert (
        authorize_semantic_scholar_candidate_5xx_recovery(
            root=tmp_path, timestamp=clock
        )
        == recovered
    )

    http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 2,
                    "data": [{"paperId": "S2b", "title": "QF02 second"}],
                }
            ),
            FakeResponse(
                payload={
                    "total": 2,
                    "data": [{"paperId": "S3b", "title": "QF03 second"}],
                }
            ),
        ]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert len(http.calls) == 2
    assert http.calls[0]["params"]["token"] == "token-qf02"
    assert http.calls[1]["params"]["token"] == "token-qf03"


def test_semantic_native_id_overlap_recovery_preserves_parent_and_is_idempotent(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    before, clock, parent_checkpoint, duplicate_id = (
        _failed_semantic_native_id_overlap_episode_2(
            tmp_path, monkeypatch, external_wave, external_preflight
        )
    )
    parent_bytes = parent_checkpoint.read_bytes()
    response_bytes = {
        path.name: path.read_bytes()
        for path in sorted((parent_checkpoint.parent / "responses").iterdir())
    }
    parent_episodes = json.loads(
        json.dumps(
            before["sources"]["SemanticScholar"]["execution_episodes"],
            sort_keys=True,
        )
    )
    other_sources = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in before["sources"].items()
        if key != "SemanticScholar"
    }

    recovered = authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    source = recovered["sources"]["SemanticScholar"]
    active = source["execution_episodes"][2]
    assert source["status"] == SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS
    assert source["active_episode_number"] == 3
    assert source["completed_query_count"] == 4
    assert source["requests_this_session"] == 0
    assert source["execution_episodes"][:2] == parent_episodes
    assert active["recovery_of_episode_number"] == 2
    assert active["network_used"] is False
    assert active["continuation_state"]["next_page_ordinal"] == 43
    provenance = active["adjudication_provenance"]
    assert provenance["adjudicated_native_id"] == duplicate_id
    assert len(provenance["adjudicated_occurrences"]) == 2
    assert provenance["provider_ordering_anomalies"] == [
        dict(item)
        for item in external_module.SEMANTIC_NATIVE_ID_OVERLAP_ORDERING_ANOMALIES
    ]
    assert provenance["provider_completeness"] == "UNPROVEN"
    assert provenance["generic_duplicate_validation_changed"] is False
    assert "third occurrence" in provenance["exception_scope"]
    assert parent_checkpoint.read_bytes() == parent_bytes
    assert {
        name: (parent_checkpoint.parent / "responses" / name).read_bytes()
        for name in response_bytes
    } == response_bytes
    assert {
        key: value
        for key, value in recovered["sources"].items()
        if key != "SemanticScholar"
    } == other_sources
    child_checkpoint = tmp_path / source["checkpoint_dataset"]["path"]
    child = external_module.load_review_dataset(child_checkpoint)
    assert child.source_queries[2].completion_status is RetrievalCompletionStatus.RUNNING
    page_42 = next(
        page
        for page in child.retrieval_pages
        if page.source_query_id == child.source_queries[2].query_id
        and page.ordinal == 42
    )
    assert page_42.status is RetrievalCompletionStatus.COMPLETE
    assert "completion_error" not in page_42.metadata
    assert len(
        [
            occurrence
            for occurrence in child.occurrences
            if occurrence.source_identifier == duplicate_id
        ]
    ) == 2
    assert (
        authorize_semantic_scholar_native_id_overlap_recovery(
            root=tmp_path, timestamp=clock
        )
        == recovered
    )


def test_semantic_native_id_overlap_recovery_continues_and_later_resumes(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock, _, _ = _failed_semantic_native_id_overlap_episode_2(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    first_http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 1_100,
                    "token": "token-44",
                    "data": [{"paperId": "id-0043", "title": "Page 43"}],
                }
            ),
            FakeResponse(
                payload={
                    "total": 1_101,
                    "token": "token-45",
                    "data": [{"paperId": "id-0044", "title": "Page 44"}],
                }
            ),
            FakeResponse(status_code=429, content=b"Rate limited"),
        ]
    )
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert paused["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_PROVIDER_RATE_LIMIT"
    )
    assert [call["params"]["token"] for call in first_http.calls] == [
        "token-43",
        "token-44",
        "token-45",
    ]
    second_http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 1_102,
                    "data": [{"paperId": "id-0045", "title": "Page 45"}],
                }
            )
        ]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=second_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert [call["params"]["token"] for call in second_http.calls] == [
        "token-45"
    ]
    checkpoint = tmp_path / completed["sources"]["SemanticScholar"][
        "checkpoint_dataset"
    ]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    qf03_pages = [
        page
        for page in dataset.retrieval_pages
        if page.source_query_id == dataset.source_queries[2].query_id
    ]
    assert [page.ordinal for page in qf03_pages] == list(range(46))


def test_semantic_native_id_overlap_recovery_keeps_new_overlap_terminal(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock, _, duplicate_id = _failed_semantic_native_id_overlap_episode_2(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 1_100,
                    "data": [
                        {"paperId": duplicate_id, "title": "Third occurrence"}
                    ],
                }
            )
        ]
    )
    failed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    source = failed["sources"]["SemanticScholar"]
    assert source["status"] == "FAILED"
    assert duplicate_id in source["failure_reason"]
    checkpoint = tmp_path / source["checkpoint_dataset"]["path"]
    dataset = external_module.load_review_dataset(checkpoint)
    assert len(
        [
            occurrence
            for occurrence in dataset.occurrences
            if occurrence.source_identifier == duplicate_id
        ]
    ) == 3


@pytest.mark.parametrize(
    ("tamper_location", "error"),
    [
        ("state", "adjudication provenance changed"),
        ("checkpoint", "checkpoint provenance changed"),
    ],
)
def test_semantic_native_id_overlap_recovery_rejects_tampered_provenance_before_io(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    tamper_location,
    error,
) -> None:
    _, clock, _, _ = _failed_semantic_native_id_overlap_episode_2(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    recovered = authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    source = recovered["sources"]["SemanticScholar"]
    if tamper_location == "state":
        source["execution_episodes"][2]["adjudication_provenance"][
            "exception_scope"
        ] = "all overlaps"
    else:
        checkpoint = tmp_path / source["checkpoint_dataset"]["path"]
        payload = json.loads(checkpoint.read_text())
        payload["retrieval_runs"][0]["metadata"][
            "offline_semantic_native_id_overlap_recovery"
        ]["exception_scope"] = "all overlaps"
        external_module.atomic_write(
            checkpoint,
            (
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode(),
        )
        reference = external_module._file_reference(checkpoint, tmp_path)
        source["checkpoint_dataset"] = reference
        source["execution_episodes"][2]["checkpoint_dataset"] = reference
    external_module._save_execution_state(state_path, recovered)
    state_bytes = state_path.read_bytes()
    http = FakeHttp([])
    with pytest.raises(ExternalRetrievalWaveError, match=error):
        execute_external_source_session(
            root=tmp_path,
            source="SemanticScholar",
            http=http,
            resume=True,
            timestamp=clock,
        )
    assert http.calls == []
    assert state_path.read_bytes() == state_bytes


def test_semantic_native_id_overlap_authorization_obeys_shared_lock(
    tmp_path, monkeypatch
) -> None:
    state_loaded = False

    def unexpected_state_load(*args, **kwargs):
        nonlocal state_loaded
        state_loaded = True
        raise AssertionError("authorization must acquire the lock before state load")

    monkeypatch.setattr(external_module, "_load_execution_state", unexpected_state_load)
    with external_module._exclusive_external_source_session(tmp_path), pytest.raises(
        ExternalRetrievalWaveError,
        match="another external-source session is already active",
    ):
        authorize_semantic_scholar_native_id_overlap_recovery(root=tmp_path)
    assert state_loaded is False


def test_semantic_native_id_overlap_recovery_cli_is_offline_and_source_scoped(
    tmp_path, monkeypatch, capsys
) -> None:
    calls = []
    state = {
        "status": "RUNNING",
        "sources": {
            "SemanticScholar": {
                "status": SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS,
                "active_episode_number": 3,
                "completed_query_count": 4,
                "checkpoint_dataset": {"path": "child", "raw_sha256": "child"},
                "execution_episodes": [
                    {},
                    {},
                    {
                        "episode_number": 3,
                        "parent_checkpoint_dataset": {
                            "path": "parent",
                            "raw_sha256": "parent",
                        },
                        "adjudication_provenance": {
                            "adjudicated_occurrences": ["first", "second"],
                            "provider_completeness": "UNPROVEN",
                        },
                        "continuation_state": {"next_page_ordinal": 43},
                    },
                ],
            }
        },
    }

    def authorize(*, root):
        calls.append(root)
        return state

    monkeypatch.setattr(
        external_module,
        "authorize_semantic_scholar_native_id_overlap_recovery",
        authorize,
    )
    assert external_module.main(
        [
            "--root",
            str(tmp_path),
            "--source",
            "SemanticScholar",
            "--authorize-semantic-scholar-native-id-overlap-recovery",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert calls == [tmp_path]
    assert output["active_episode_number"] == 3
    assert output["continuation"] == {"next_page_ordinal": 43}
    assert output["provider_completeness"] == "UNPROVEN"
    assert output["network_used"] is False


def test_semantic_second_native_id_overlap_recovery_preserves_parents_and_is_idempotent(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    before, clock, episode_2_checkpoint, episode_3_checkpoint, first_id, second_id = (
        _failed_semantic_native_id_overlap_episode_3(
            tmp_path, monkeypatch, external_wave, external_preflight
        )
    )
    parent_episodes = json.loads(
        json.dumps(
            before["sources"]["SemanticScholar"]["execution_episodes"],
            sort_keys=True,
        )
    )
    historical_files = {
        path: path.read_bytes()
        for checkpoint in (episode_2_checkpoint, episode_3_checkpoint)
        for path in [checkpoint, *sorted((checkpoint.parent / "responses").iterdir())]
    }
    other_sources = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in before["sources"].items()
        if key != "SemanticScholar"
    }

    recovered = authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    source = recovered["sources"]["SemanticScholar"]
    active = source["execution_episodes"][3]
    assert source["status"] == SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS
    assert source["active_episode_number"] == 4
    assert source["occurrence_count"] == len(
        external_module.load_review_dataset(
            tmp_path / source["checkpoint_dataset"]["path"]
        ).occurrences
    )
    assert source["completed_query_count"] == 4
    assert source["execution_episodes"][:3] == parent_episodes
    assert all(episode["immutable"] for episode in source["execution_episodes"][1:3])
    assert active["recovery_of_episode_number"] == 3
    assert active["network_used"] is False
    assert active["continuation_state"]["next_page_ordinal"] == 55
    provenance = active["adjudication_provenance"]
    assert provenance["adjudicated_native_ids"] == [first_id, second_id]
    assert len(provenance["adjudications"]) == 2
    assert len(provenance["adjudicated_occurrences"]) == 4
    assert provenance["provider_completeness"] == "UNPROVEN"
    assert provenance["generic_duplicate_validation_changed"] is False
    assert "third occurrence of either paperId" in provenance["exception_scope"]
    assert all(path.read_bytes() == content for path, content in historical_files.items())
    assert {
        key: value
        for key, value in recovered["sources"].items()
        if key != "SemanticScholar"
    } == other_sources
    child = external_module.load_review_dataset(
        tmp_path / source["checkpoint_dataset"]["path"]
    )
    assert child.source_queries[2].completion_status is RetrievalCompletionStatus.RUNNING
    page_54 = next(
        page
        for page in child.retrieval_pages
        if page.source_query_id == child.source_queries[2].query_id
        and page.ordinal == 54
    )
    assert "completion_error" not in page_54.metadata
    assert {
        native_id: len(
            [
                occurrence
                for occurrence in child.occurrences
                if occurrence.source_identifier == native_id
            ]
        )
        for native_id in (first_id, second_id)
    } == {first_id: 2, second_id: 2}
    assert (
        authorize_semantic_scholar_native_id_overlap_recovery(
            root=tmp_path, timestamp=clock
        )
        == recovered
    )


def test_semantic_second_native_id_overlap_recovery_continues_and_later_resumes(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _, clock, _, _, _, _ = _failed_semantic_native_id_overlap_episode_3(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    first_http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 1_200,
                    "token": "token-56",
                    "data": [{"paperId": "id-0055", "title": "Page 55"}],
                }
            ),
            FakeResponse(
                payload={
                    "total": 1_201,
                    "token": "token-57",
                    "data": [{"paperId": "id-0056", "title": "Page 56"}],
                }
            ),
            FakeResponse(status_code=429, content=b"Rate limited"),
        ]
    )
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert paused["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_PROVIDER_RATE_LIMIT"
    )
    assert [call["params"]["token"] for call in first_http.calls] == [
        "token-55",
        "token-56",
        "token-57",
    ]
    second_http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 1_202,
                    "data": [{"paperId": "id-0057", "title": "Page 57"}],
                }
            )
        ]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=second_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert [call["params"]["token"] for call in second_http.calls] == ["token-57"]
    dataset = external_module.load_review_dataset(
        tmp_path
        / completed["sources"]["SemanticScholar"]["checkpoint_dataset"]["path"]
    )
    qf03_pages = [
        page
        for page in dataset.retrieval_pages
        if page.source_query_id == dataset.source_queries[2].query_id
    ]
    assert [page.ordinal for page in qf03_pages] == list(range(58))


@pytest.mark.parametrize("overlap_kind", ["first", "second", "new"])
def test_semantic_second_native_id_overlap_recovery_keeps_new_overlaps_terminal(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    overlap_kind,
) -> None:
    _, clock, _, _, first_id, second_id = (
        _failed_semantic_native_id_overlap_episode_3(
            tmp_path, monkeypatch, external_wave, external_preflight
        )
    )
    authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    if overlap_kind == "new":
        native_id = "new-cross-page-overlap"
        responses = [
            FakeResponse(
                payload={
                    "total": 1_200,
                    "token": "token-56",
                    "data": [{"paperId": native_id, "title": "First copy"}],
                }
            ),
            FakeResponse(
                payload={
                    "total": 1_201,
                    "data": [{"paperId": native_id, "title": "Second copy"}],
                }
            ),
        ]
    else:
        native_id = first_id if overlap_kind == "first" else second_id
        responses = [
            FakeResponse(
                payload={
                    "total": 1_200,
                    "data": [{"paperId": native_id, "title": "Third copy"}],
                }
            )
        ]
    failed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=FakeHttp(responses),
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    source = failed["sources"]["SemanticScholar"]
    assert source["status"] == "FAILED"
    assert native_id in source["failure_reason"]


@pytest.mark.parametrize("tamper_location", ["state", "checkpoint", "prior"])
def test_semantic_second_native_id_overlap_recovery_rejects_tampered_provenance(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    tamper_location,
) -> None:
    _, clock, _, _, _, _ = _failed_semantic_native_id_overlap_episode_3(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    recovered = authorize_semantic_scholar_native_id_overlap_recovery(
        root=tmp_path, timestamp=clock
    )
    source = recovered["sources"]["SemanticScholar"]
    if tamper_location == "prior":
        source["execution_episodes"][2]["adjudication_provenance"][
            "exception_scope"
        ] = "all overlaps"
    elif tamper_location == "state":
        source["execution_episodes"][3]["adjudication_provenance"][
            "exception_scope"
        ] = "all overlaps"
    else:
        checkpoint = tmp_path / source["checkpoint_dataset"]["path"]
        payload = json.loads(checkpoint.read_text())
        payload["retrieval_runs"][0]["metadata"][
            "offline_semantic_native_id_overlap_recovery_episode_4"
        ]["exception_scope"] = "all overlaps"
        external_module.atomic_write(
            checkpoint,
            (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        )
        reference = external_module._file_reference(checkpoint, tmp_path)
        source["checkpoint_dataset"] = reference
        source["execution_episodes"][3]["checkpoint_dataset"] = reference
    state_path = tmp_path / external_module.EXECUTION_STATE_PATH
    external_module._save_execution_state(state_path, recovered)
    state_bytes = state_path.read_bytes()
    http = FakeHttp([])
    with pytest.raises(ExternalRetrievalWaveError, match="provenance changed"):
        execute_external_source_session(
            root=tmp_path,
            source="SemanticScholar",
            http=http,
            resume=True,
            timestamp=clock,
        )
    assert http.calls == []
    assert state_path.read_bytes() == state_bytes

    with pytest.raises(SystemExit):
        external_module.main(
            [
                "--source",
                "arXiv",
                "--authorize-semantic-scholar-native-id-overlap-recovery",
            ]
        )
    with pytest.raises(SystemExit):
        external_module.main(
            [
                "--source",
                "SemanticScholar",
                "--resume",
                "--authorize-semantic-scholar-native-id-overlap-recovery",
            ]
        )


@pytest.mark.parametrize(
    "drift",
    ["response", "query", "request", "token", "completed-family"],
)
def test_semantic_candidate_5xx_recovery_refuses_corruption_and_drift(
    tmp_path, monkeypatch, external_wave, external_preflight, drift
) -> None:
    _, clock, checkpoint = _failed_semantic_candidate_5xx_checkpoint(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    if drift == "response":
        response = next((checkpoint.parent / "responses").iterdir())
        response.write_bytes(response.read_bytes() + b"corrupt")
    else:
        payload = json.loads(checkpoint.read_text())
        if drift == "query":
            payload["source_queries"][0]["query_text"] += " changed"
        elif drift == "request":
            payload["retrieval_attempts"][0]["request_hash"] = "changed"
        elif drift == "token":
            failed_page = next(
                page
                for page in payload["retrieval_pages"]
                if page["source_query_id"]
                == payload["source_queries"][1]["query_id"]
                and page["status"] == "failed"
            )
            failed_page["request_state"]["token"] = "changed"
        else:
            payload["source_queries"][0]["result_count"] = 2
        external_module.atomic_write(
            checkpoint,
            (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
        )
        monkeypatch.setattr(
            external_module,
            "SEMANTIC_CANDIDATE_5XX_EXPECTED_CHECKPOINT_SHA256",
            hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
        )

    with pytest.raises(ExternalRetrievalWaveError):
        authorize_semantic_scholar_candidate_5xx_recovery(
            root=tmp_path, timestamp=clock
        )


@pytest.mark.parametrize("corruption", ["response", "query", "candidate"])
def test_semantic_control_5xx_recovery_refuses_drift(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    corruption,
) -> None:
    state, clock = _blocked_semantic_control_5xx_gate(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    gate_path = tmp_path / SEMANTIC_CONTROL_GATE_PATH
    if corruption == "response":
        response = next((gate_path.parent / "responses").iterdir())
        response.write_bytes(response.read_bytes() + b"corrupt")
        error = "response hash/size changed"
    elif corruption == "query":
        gate = json.loads(gate_path.read_text())
        gate["controls"][0]["expression"] = "changed"
        external_module._save_hashed_json(gate_path, gate, "manifest_hash")
        monkeypatch.setattr(
            external_module,
            "SEMANTIC_CONTROL_BLOCKED_MANIFEST_RAW_SHA256",
            hashlib.sha256(gate_path.read_bytes()).hexdigest(),
        )
        monkeypatch.setattr(
            external_module,
            "SEMANTIC_CONTROL_BLOCKED_MANIFEST_LOGICAL_HASH",
            gate["manifest_hash"],
        )
        error = "attempt/query signature changed"
    else:
        state_path = tmp_path / external_module.EXECUTION_STATE_PATH
        state["sources"]["SemanticScholar"]["candidate_request_count"] = 1
        external_module._save_execution_state(state_path, state)
        error = "source state is not the known blocked"

    with pytest.raises(ExternalRetrievalWaveError, match=error):
        authorize_semantic_scholar_control_5xx_recovery(
            root=tmp_path, timestamp=clock
        )


@pytest.mark.parametrize(
    ("headers", "present", "value"),
    [({}, False, None), ({"Retry-After": "120"}, True, "120")],
)
def test_semantic_control_429_pauses_with_retry_after_and_resumes(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    headers,
    present,
    value,
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    first_http = FakeHttp(
        [FakeResponse(status_code=429, headers=headers, content=b"Rate limited")]
    )
    clock = Clock()
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: pytest.fail("control 429 must pause immediately"),
    )

    source = paused["sources"]["SemanticScholar"]
    expected = {
        "source_database": "SemanticScholar",
        "probe_id": "atomic-a",
        "http_status": 429,
        "retry_after_header_present": present,
        "retry_after": value,
    }
    assert len(first_http.calls) == 1
    assert source["status"] == "PAUSED_PROVIDER_RATE_LIMIT"
    assert source["pause_metadata"] == expected
    gate_path = tmp_path / source["semantic_control_gate"]["manifest_path"]
    gate = json.loads(gate_path.read_text())
    assert gate["pause_metadata"] == expected
    attempt = gate["controls"][0]["attempts"][0]
    assert attempt["response"]["retry_after_header_present"] is present
    assert attempt["response"]["retry_after"] == value
    assert (gate_path.parent / attempt["response"]["path"]).is_file()

    resumed_http = FakeHttp(
        [*_passing_semantic_controls(), *_semantic_candidate_responses()]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=resumed_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=3),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )

    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert len(resumed_http.calls) == 11
    resumed_gate = json.loads(gate_path.read_text())
    assert resumed_gate["status"] == "PASSED"
    assert len(resumed_gate["controls"][0]["attempts"]) == 2


def test_semantic_control_transport_exhaustion_gets_fresh_resume_budget(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    first_http = FakeHttp([ReadTimeout("timed out") for _ in range(2)])
    clock = Clock()
    paused = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    source = paused["sources"]["SemanticScholar"]
    assert len(first_http.calls) == 2
    assert source["status"] == "PAUSED_TRANSIENT_TRANSPORT"
    assert source["pause_metadata"]["attempts_this_invocation"] == 2

    second_http = FakeHttp([ReadTimeout("timed out again") for _ in range(2)])
    paused_again = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=second_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    assert len(second_http.calls) == 2
    assert paused_again["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_TRANSIENT_TRANSPORT"
    )
    gate_path = (
        tmp_path
        / paused_again["sources"]["SemanticScholar"]["semantic_control_gate"][
            "manifest_path"
        ]
    )
    gate = json.loads(gate_path.read_text())
    assert [item["attempt_number"] for item in gate["controls"][0]["attempts"]] == [
        1,
        2,
        3,
        4,
    ]


def test_semantic_resume_skips_successful_controls_and_candidate_pages(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    first_http = FakeHttp(
        [
            _passing_semantic_controls()[0],
            FakeResponse(status_code=429, content=b"Rate limited"),
        ]
    )
    clock = Clock()
    paused_control = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=first_http,
        resume=False,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert paused_control["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_PROVIDER_RATE_LIMIT"
    )

    remaining_controls = _passing_semantic_controls()[1:]
    candidate_http = FakeHttp(
        [
            *remaining_controls,
            FakeResponse(
                payload={
                    "total": 2,
                    "token": "qf01-next",
                    "data": [{"paperId": "S1", "title": "Paper 1"}],
                }
            ),
            FakeResponse(status_code=429, content=b"Rate limited"),
        ]
    )
    paused_candidate = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=candidate_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert paused_candidate["sources"]["SemanticScholar"]["status"] == (
        "PAUSED_PROVIDER_RATE_LIMIT"
    )
    assert len(candidate_http.calls) == 7

    final_http = FakeHttp(
        [
            FakeResponse(
                payload={
                    "total": 2,
                    "data": [{"paperId": "S2", "title": "Paper 2"}],
                }
            ),
            *_semantic_candidate_responses()[1:],
        ]
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=final_http,
        resume=True,
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert completed["sources"]["SemanticScholar"]["status"] == "COMPLETE"
    assert len(final_http.calls) == 5
    assert final_http.calls[0]["params"]["token"] == "qf01-next"
    gate_path = tmp_path / SEMANTIC_CONTROL_GATE_PATH
    gate = json.loads(gate_path.read_text())
    assert len(gate["controls"][0]["attempts"]) == 1


def test_semantic_control_failure_executes_zero_candidate_queries(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    counts = [100, 100, 50, 150, 70, 60]
    http = FakeHttp([FakeResponse(payload={"total": count}) for count in counts])
    state = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=http,
        resume=False,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: None,
    )
    source = state["sources"]["SemanticScholar"]
    assert source["status"] == "BLOCKED_SEMANTIC_CONTROL_GATE"
    assert source["semantic_control_gate"]["status"] == "FAILED"
    assert source["candidate_request_count"] == 0
    assert len(http.calls) == 6
    assert not (
        tmp_path
        / "outputs/production/star-external-retrieval-wave-001/execution/"
        "SemanticScholar/checkpoint/review_dataset.json"
    ).exists()


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (FakeResponse(status_code=400, content=b"Invalid query"), "HTTP 400"),
        (FakeResponse(status_code=401, content=b"Unauthorized"), "HTTP 401"),
        (FakeResponse(status_code=403, content=b"Forbidden"), "HTTP 403"),
        (FakeResponse(payload={"data": []}), "response omitted total"),
    ],
)
def test_semantic_control_permanent_response_failures_remain_unresolved(
    tmp_path,
    monkeypatch,
    external_wave,
    external_preflight,
    response,
    error,
) -> None:
    _install_semantic_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    http = FakeHttp([response])
    state = execute_external_source_session(
        root=tmp_path,
        source="SemanticScholar",
        http=http,
        resume=False,
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=3),
        rate_limiter=RateLimiter({}),
        retry_sleep=lambda _: pytest.fail("permanent control failures must not retry"),
    )

    source = state["sources"]["SemanticScholar"]
    assert source["status"] == "BLOCKED_SEMANTIC_CONTROL_GATE"
    assert source["semantic_control_gate"]["status"] == "UNRESOLVED"
    assert len(http.calls) == 1
    gate = json.loads((tmp_path / SEMANTIC_CONTROL_GATE_PATH).read_text())
    assert error in gate["controls"][0]["attempts"][0]["error"]
    assert gate["controls"][0]["attempts"][0]["response"]["sha256"]


def test_ieee_daily_quota_stops_and_resumes_without_repeating_pages(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    known_calls = iter([199, 0])
    monkeypatch.setattr(
        external_module, "_ieee_calls_on_day", lambda *_: next(known_calls)
    )
    source_query_specs = external_module._source_query_specs

    def one_record_ieee_specs(*args, **kwargs):
        specs = source_query_specs(*args, **kwargs)
        if args[1] == "IEEEXplore":
            for spec in specs:
                spec.limit = 1
        return specs

    monkeypatch.setattr(
        external_module, "_source_query_specs", one_record_ieee_specs
    )
    clock = Clock()
    first_http = FakeHttp([FakeResponse(payload=_ieee_page("A1", total=2))])
    paused = execute_external_source_session(
        root=tmp_path,
        source="IEEEXplore",
        http=first_http,
        resume=False,
        ieee_credential="secret-not-persisted",
        quota_day_utc="2026-09-04",
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert paused["sources"]["IEEEXplore"]["status"] == "PAUSED_DAILY_QUOTA"
    assert paused["sources"]["IEEEXplore"]["requests_this_session"] == 1

    responses = [FakeResponse(payload=_ieee_page("A2", total=2))]
    responses.extend(
        FakeResponse(payload=_ieee_page(f"B{index}", total=1))
        for index in range(2, 6)
    )
    completed = execute_external_source_session(
        root=tmp_path,
        source="IEEEXplore",
        http=FakeHttp(responses),
        resume=True,
        ieee_credential="secret-not-persisted",
        quota_day_utc="2026-09-05",
        timestamp=clock,
        retry_policy=RetryPolicy(max_attempts=1),
        rate_limiter=RateLimiter({}),
    )
    assert completed["sources"]["IEEEXplore"]["status"] == "COMPLETE"
    checkpoint = (
        tmp_path
        / "outputs/production/star-external-retrieval-wave-001/execution/"
        "IEEEXplore/checkpoint/review_dataset.json"
    )
    assert "secret-not-persisted" not in checkpoint.read_text()
    assert len(json.loads(checkpoint.read_text())["retrieval_attempts"]) == 6


def test_ieee_credential_absence_fails_before_any_request(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    http = FakeHttp([])
    with pytest.raises(ExternalRetrievalWaveError, match="IEEE_XPLORE_API_KEY"):
        execute_external_source_session(
            root=tmp_path,
            source="IEEEXplore",
            http=http,
            resume=False,
            ieee_credential="",
            timestamp=Clock(),
        )
    assert http.calls == []


def test_persisted_wave_hash_mismatch_fails_before_execution(
    tmp_path, monkeypatch, external_wave
) -> None:
    monkeypatch.setattr(
        external_module,
        "build_external_retrieval_wave",
        lambda **_: external_wave,
    )
    wave_path = tmp_path / WAVE_PATH
    wave_path.parent.mkdir(parents=True, exist_ok=True)
    wave_path.write_text("{}\n", encoding="utf-8")
    (tmp_path / PREFLIGHT_PATH).write_text("{}\n", encoding="utf-8")
    with pytest.raises(ExternalRetrievalWaveError, match="planned wave differs"):
        validate_persisted_external_preflight(root=tmp_path)
