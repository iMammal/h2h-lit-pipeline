from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from h2h_lit.query_development import (
    CANDIDATE_SCHEMA_VERSION,
    SENTINEL_ACCURACY_INTERPRETATION,
    SENTINEL_PURPOSE,
    SENTINEL_SET_SCHEMA_VERSION,
    SIZING_RUN_SCHEMA_VERSION,
    CandidateConfigurationError,
    CandidateSet,
    QuerySizingRun,
    SentinelPaper,
    SentinelPaperSet,
    SizingCountKind,
    SizingObservation,
    SizingRunStatus,
    SizingSyntaxStatus,
    SizingWindowStatus,
    load_candidate_set,
    load_sentinel_set,
    load_sizing_run,
    save_sentinel_set,
    save_sizing_run,
    sizing_request_hash,
)

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_CONFIG = ROOT / "config" / "star_query_candidates_v0_1.json"


def _candidate_set() -> CandidateSet:
    return load_candidate_set(CANDIDATE_CONFIG)


def _observation(candidate_query_id: str, *, count: int = 17) -> SizingObservation:
    request = {
        "method": "GET",
        "endpoint": "https://example.invalid/count",
        "params": {"query": "candidate expression", "limit": 1},
    }
    return SizingObservation(
        observation_id="observation-001",
        candidate_query_id=candidate_query_id,
        query_hash="a" * 64,
        source="PubMed",
        observed_at="2026-09-01T12:00:00Z",
        request=request,
        request_hash=sizing_request_hash(request),
        response_hash="b" * 64,
        reported_count=count,
        count_kind=SizingCountKind.EXACT,
        hard_window=10_000,
        window_status=SizingWindowStatus.CLEAR,
        syntax_status=SizingSyntaxStatus.ACCEPTED,
    )


def test_candidate_set_encodes_approved_architecture() -> None:
    candidate_set = _candidate_set()
    payload = candidate_set.payload

    assert payload["schema_version"] == CANDIDATE_SCHEMA_VERSION
    assert payload["methodological_status"] == "candidate_not_production_frozen"
    assert payload["no_partitioning_authorized"] is True
    assert tuple(payload["families"]) == (
        "STAR-QF01-RELATIONAL-VIS",
        "STAR-QF02-ASSISTED-VIS",
        "STAR-QF03-INTERACTIVE-SYSTEMS",
        "STAR-QF04-NONDESKTOP-ENV",
        "STAR-QF05-CONVERSATIONAL",
    )
    assert all(value is None for value in payload["retrieval_filters"].values())
    assert candidate_set.candidate_set_hash() == _candidate_set().candidate_set_hash()


def test_qf01_qf02_comparators_and_retrieval_only_anchor() -> None:
    candidate_set = _candidate_set()
    payload = candidate_set.payload

    assert "X" not in payload["anchors"]
    assistance_context = payload["anchors"]["ASSISTANCE_CONTEXT"]
    assert assistance_context["purpose"] == "retrieval_context_only"
    assert set(assistance_context["prohibited_interpretations"]) == {
        "eligibility_evidence",
        "classification_evidence",
    }
    qf01 = payload["families"]["STAR-QF01-RELATIONAL-VIS"]
    assert set(qf01["variants"]) == {"anchored", "unanchored"}
    qf02 = payload["families"]["STAR-QF02-ASSISTED-VIS"]
    assert set(qf02["variants"]) == {"A", "B", "C", "D"}
    assert qf02["leading_candidate"] == "D"

    rendered = {item.candidate_query_id: item for item in candidate_set.render_all()}
    anchored = rendered["candidate:STAR-QF01-RELATIONAL-VIS:anchored:PubMed"]
    unanchored = rendered["candidate:STAR-QF01-RELATIONAL-VIS:unanchored:PubMed"]
    qf02_d = rendered["candidate:STAR-QF02-ASSISTED-VIS:D:PubMed"]
    assert anchored.query_text != unanchored.query_text
    assert "relational data" in anchored.query_text
    assert "relational data" not in unanchored.query_text
    assert "machine learning" in qf02_d.query_text
    assert "user-guided" in qf02_d.query_text


def test_source_renderings_are_sizing_only_and_source_specific() -> None:
    queries = _candidate_set().render_all()
    assert len(queries) == 62
    assert len({item.candidate_query_id for item in queries}) == len(queries)
    assert all(item.methodological_status == "candidate_not_production_frozen" for item in queries)
    assert all(not item.creates_production_occurrences for item in queries)
    assert all(not item.establishes_retrieval_cutoff for item in queries)

    by_id = {item.candidate_query_id: item for item in queries}
    prefix = "candidate:STAR-QF03-INTERACTIVE-SYSTEMS:default:"
    pubmed = by_id[prefix + "PubMed"]
    europe_pmc = by_id[prefix + "EuropePMC"]
    semantic_scholar = by_id[prefix + "SemanticScholar"]
    arxiv = by_id[prefix + "arXiv"]
    ieee = by_id[prefix + "IEEEXplore"]
    acm = by_id[prefix + "ACMDigitalLibrary"]

    assert "[Title/Abstract]" in pubmed.query_text
    assert pubmed.sizing_request["params"]["retmax"] == 0
    assert "TITLE_ABS:" in europe_pmc.query_text
    assert europe_pmc.sizing_request["params"]["pageSize"] == 1
    assert semantic_scholar.sizing_request["params"]["limit"] == 1
    assert semantic_scholar.syntax_uncertainties == ["bulk_boolean_semantics_unverified"]
    assert arxiv.sizing_request["params"]["max_results"] == 1
    assert ieee.sizing_request["params"]["max_records"] == 1
    assert "apikey" not in ieee.sizing_request["params"]
    assert acm.sizing_request["transport"] == "human_ui"
    assert acm.sizing_request["scope"] == "ACM Publications"
    assert acm.sizing_request["citation_export"] is False

    crossref = [item for item in queries if item.source == "CrossRef"]
    assert all(item.family_id != "STAR-QF03-INTERACTIVE-SYSTEMS" for item in crossref)
    assert all("provisional" in item.source_role for item in crossref)
    assert all(item.syntax_uncertainties for item in crossref)


def test_rendering_and_query_hashes_are_deterministic() -> None:
    first = _candidate_set().render_all()
    second = _candidate_set().render_all()

    assert first == second
    assert [item.query_hash for item in first] == [item.query_hash for item in second]


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("no_partitioning_authorized",), False),
        (("retrieval_filters", "language"), "English"),
        (("sources", "SemanticScholar", "pagination_mode"), "relevance"),
        (("sources", "CrossRef", "role"), "primary_identification"),
    ],
)
def test_candidate_configuration_rejects_unapproved_changes(
    path: tuple[str, ...], value: object
) -> None:
    data = copy.deepcopy(_candidate_set().payload)
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(CandidateConfigurationError):
        CandidateSet(data).validate()


def test_sizing_run_round_trip_is_isolated_and_deterministic(tmp_path: Path) -> None:
    candidate_id = "candidate:STAR-QF01-RELATIONAL-VIS:anchored:PubMed"
    observation = _observation(candidate_id)
    run = QuerySizingRun(
        schema_version=SIZING_RUN_SCHEMA_VERSION,
        sizing_run_id="star-sizing-v0-1-run-001",
        candidate_set_id="h2h-star-five-family-query-candidates",
        candidate_set_version="0.1.0-preproduction",
        candidate_set_hash=_candidate_set().candidate_set_hash(),
        status=SizingRunStatus.COMPLETED,
        planned_candidate_query_ids=[candidate_id],
        created_at="2026-09-01T12:00:00Z",
        observations=[observation],
        completed_at="2026-09-01T12:01:00Z",
    )
    path = tmp_path / "sizing_run.json"

    save_sizing_run(path, run)
    loaded = load_sizing_run(path)
    persisted = json.loads(path.read_text(encoding="utf-8"))

    assert loaded.to_dict() == run.to_dict()
    assert loaded.run_hash() == run.run_hash()
    assert loaded.creates_production_occurrences is False
    assert loaded.contributes_prisma_counts is False
    assert loaded.establishes_retrieval_cutoff is False
    assert loaded.derives_e6 is False
    assert "occurrences" not in persisted
    assert "prisma" not in persisted
    assert "retrieval_cutoff" not in persisted
    assert "e6" not in persisted


def test_sizing_rejects_production_effects_and_incomplete_completion() -> None:
    observation = _observation("candidate-1")
    with pytest.raises(ValueError, match="every planned query"):
        QuerySizingRun(
            schema_version=SIZING_RUN_SCHEMA_VERSION,
            sizing_run_id="run-1",
            candidate_set_id="set-1",
            candidate_set_version="v1",
            candidate_set_hash="a" * 64,
            status=SizingRunStatus.COMPLETED,
            planned_candidate_query_ids=["candidate-1", "candidate-2"],
            created_at="2026-09-01T12:00:00Z",
            observations=[observation],
            completed_at="2026-09-01T12:01:00Z",
        ).validate()

    with pytest.raises(ValueError, match="cannot affect production"):
        QuerySizingRun(
            schema_version=SIZING_RUN_SCHEMA_VERSION,
            sizing_run_id="run-2",
            candidate_set_id="set-1",
            candidate_set_version="v1",
            candidate_set_hash="a" * 64,
            status=SizingRunStatus.PLANNED,
            planned_candidate_query_ids=["candidate-1"],
            created_at="2026-09-01T12:00:00Z",
            contributes_prisma_counts=True,
        ).validate()


@pytest.mark.parametrize(
    "request_payload",
    [
        {"returned_records": [{"title": "must not be retained"}]},
        {"api_key": "secret-value"},
        {"authorization": "Bearer secret-value"},
    ],
)
def test_sizing_request_rejects_records_and_secrets(
    request_payload: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        sizing_request_hash(request_payload)


def test_sizing_observation_rejects_wrong_window_classification() -> None:
    request = {"method": "GET", "params": {"query": "candidate", "limit": 1}}
    observation = SizingObservation(
        observation_id="observation-001",
        candidate_query_id="candidate-1",
        query_hash="a" * 64,
        source="PubMed",
        observed_at="2026-09-01T12:00:00Z",
        request=request,
        request_hash=sizing_request_hash(request),
        response_hash=None,
        reported_count=101,
        count_kind=SizingCountKind.EXACT,
        hard_window=100,
        window_status=SizingWindowStatus.CLEAR,
        syntax_status=SizingSyntaxStatus.ACCEPTED,
    )
    with pytest.raises(ValueError, match="window status"):
        observation.validate()


def test_sentinel_set_is_frozen_and_has_no_gold_semantics(tmp_path: Path) -> None:
    sentinel_set = SentinelPaperSet(
        schema_version=SENTINEL_SET_SCHEMA_VERSION,
        sentinel_set_id="star-query-sentinels-v0-1",
        sentinel_set_version="0.1.0-pre-sizing",
        candidate_set_hash=_candidate_set().candidate_set_hash(),
        frozen_at="2026-09-01T11:00:00Z",
        entries=[
            SentinelPaper(
                sentinel_id="sentinel-001",
                title="A known bibliographic sentinel",
                doi="10.1234/example",
                source_identifier="neutral-sentinel-001",
                diagnostic_family_ids=["STAR-QF01-RELATIONAL-VIS"],
            )
        ],
        purpose=SENTINEL_PURPOSE,
        accuracy_interpretation=SENTINEL_ACCURACY_INTERPRETATION,
    )
    path = tmp_path / "sentinels.json"

    save_sentinel_set(path, sentinel_set)
    loaded = load_sentinel_set(path)
    assert loaded.to_dict() == sentinel_set.to_dict()
    assert loaded.sentinel_set_hash() == sentinel_set.sentinel_set_hash()

    data = copy.deepcopy(sentinel_set.to_dict())
    data["entries"][0]["gold_label"] = "eligible"
    with pytest.raises(ValueError, match="gold_label"):
        SentinelPaperSet.from_dict(data)


def test_sentinel_set_rejects_unfrozen_or_unknown_family() -> None:
    with pytest.raises(ValueError, match="frozen prospectively"):
        SentinelPaperSet(
            schema_version=SENTINEL_SET_SCHEMA_VERSION,
            sentinel_set_id="sentinels",
            sentinel_set_version="v1",
            candidate_set_hash="a" * 64,
            frozen_at="",
            entries=[
                SentinelPaper(
                    sentinel_id="sentinel-1",
                    title="Title",
                    diagnostic_family_ids=["STAR-QF01-RELATIONAL-VIS"],
                )
            ],
        ).validate()

    with pytest.raises(ValueError, match="unknown query family"):
        SentinelPaperSet(
            schema_version=SENTINEL_SET_SCHEMA_VERSION,
            sentinel_set_id="sentinels",
            sentinel_set_version="v1",
            candidate_set_hash="a" * 64,
            frozen_at="2026-09-01T11:00:00Z",
            entries=[
                SentinelPaper(
                    sentinel_id="sentinel-1",
                    title="Title",
                    diagnostic_family_ids=["UNKNOWN"],
                )
            ],
        ).validate()
