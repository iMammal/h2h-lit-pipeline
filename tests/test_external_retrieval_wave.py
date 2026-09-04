from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import h2h_lit.external_retrieval_wave as external_module
from h2h_lit.external_retrieval_wave import (
    ACM_RECONCILIATION_PATH,
    PREFLIGHT_PATH,
    READY_STATUS,
    WAVE_PATH,
    ExternalRetrievalWaveError,
    _safe_output_path,
    authorize_pubmed_transport_retry,
    build_external_retrieval_wave,
    execute_external_source_session,
    preflight_external_retrieval_wave,
    validate_persisted_external_preflight,
)
from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.production_prerequisites import (
    EXPECTED_PLAN_HASH,
    EXPECTED_PLAN_RAW_SHA256,
)
from h2h_lit.production_wave import (
    EXTERNAL_IDENTIFICATION_SOURCES_V2,
    ProductionWaveStatus,
    compute_query_plan_hash,
)
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


def _ieee_page(identifier: str, *, total: int) -> dict:
    return {
        "total_records": total,
        "articles": [{"article_number": identifier, "title": identifier}],
    }


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


def test_semantic_control_failure_executes_zero_candidate_queries(
    tmp_path, monkeypatch, external_wave, external_preflight
) -> None:
    _install_isolated_runtime(
        tmp_path, monkeypatch, external_wave, external_preflight
    )
    control_target = tmp_path / "config/star_query_semantic_controls_v0_3.json"
    control_target.parent.mkdir(parents=True, exist_ok=True)
    control_target.write_bytes((ROOT / "config/star_query_semantic_controls_v0_3.json").read_bytes())
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
