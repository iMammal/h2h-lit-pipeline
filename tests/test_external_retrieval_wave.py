from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import h2h_lit.external_retrieval_wave as external_module
import h2h_lit.sources.pubmed as pubmed_module
from h2h_lit.external_retrieval_wave import (
    ACM_RECONCILIATION_PATH,
    EUROPE_PMC_TERMINAL_RECOVERY_STATUS,
    IEEE_TOTAL_DRIFT_RECOVERY_STATUS,
    PREFLIGHT_PATH,
    PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS,
    PUBMED_PARSER_RECOVERY_STATUS,
    READY_STATUS,
    WAVE_PATH,
    ExternalRetrievalWaveError,
    _safe_output_path,
    authorize_europe_pmc_terminal_recovery,
    authorize_ieee_total_drift_recovery,
    authorize_pubmed_parser_recovery,
    authorize_pubmed_transport_retry,
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
from h2h_lit.sources.europe_pmc import (
    EuropePmcPaginator,
    parse_europe_pmc_response,
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
        tuple((5, 4, 2, 3, 4) for _ in range(5)),
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
                        family, start_record=1, count=2, total=5
                    )
                ),
                FakeResponse(
                    payload=_ieee_drift_page(
                        family,
                        start_record=3,
                        count=1,
                        total=4,
                        overlap=cross_page_overlap and family == 1,
                    )
                ),
            ]
        )
    clock = Clock()
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
    assert failed["sources"]["IEEEXplore"]["status"] == "FAILED"
    return failed, clock


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
        family["observed_provider_totals"] == [5, 4, 5]
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
        query.metadata["provider_total_observations"] == [5, 4, 5]
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
