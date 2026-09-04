from __future__ import annotations

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
