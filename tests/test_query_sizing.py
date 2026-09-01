from __future__ import annotations

import copy
import json
import socket
from pathlib import Path

import pytest

from h2h_lit.query_development import (
    LEGACY_SIZING_RUN_SCHEMA_VERSION,
    AcmOperatorEvidence,
    QuerySizingRun,
    SentinelCandidateMatchState,
    SentinelDiagnostic,
    SentinelDiagnosticOutcome,
    SentinelIdentityState,
    SentinelSourceIndexingState,
    SizingAttempt,
    SizingCountKind,
    SizingObservation,
    SizingRunStatus,
    SizingSyntaxStatus,
    SizingTransportStatus,
    SizingWindowStatus,
    load_candidate_set,
    load_sentinel_set,
    load_sizing_run,
    save_sizing_run,
    sizing_request_hash,
    validate_sentinel_revision,
)
from h2h_lit.query_sizing import build_sizing_dry_run, save_sizing_dry_run
from h2h_lit.query_sizing_cli import main

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_CONFIG = ROOT / "config" / "star_query_candidates_v0_1.json"
SENTINEL_CONFIG = ROOT / "config" / "star_query_sentinels_v0_1.json"
EXPECTED_CANDIDATE_HASH = "4c642ff04c84c1e1534566d789278fdab21af9f75a57332fce04fa3751fe01bc"
EXPECTED_SENTINEL_HASH = "1acc34ae05f0637bdfb5d3feebe2044197164ae4b6d6ffd0e043daa63bfd46a3"


def _request() -> dict[str, object]:
    return {
        "transport": "http",
        "method": "GET",
        "url": "https://example.invalid/count",
        "params": {"query": "candidate", "limit": 1},
    }


def _run() -> QuerySizingRun:
    candidates = load_candidate_set(CANDIDATE_CONFIG)
    sentinels = load_sentinel_set(SENTINEL_CONFIG)
    candidate = candidates.render_all()[0]
    request = _request()
    request_hash = sizing_request_hash(request)
    attempt = SizingAttempt(
        attempt_number=1,
        started_at="2026-09-01T13:18:46Z",
        completed_at=None,
        request=request,
        request_hash=request_hash,
        transport_status=SizingTransportStatus.PLANNED,
    )
    observation = SizingObservation(
        observation_id="observation-001",
        candidate_query_id=candidate.candidate_query_id,
        query_hash=candidate.query_hash,
        source=candidate.source,
        observed_at="2026-09-01T13:18:46Z",
        request=request,
        request_hash=request_hash,
        response_hash=None,
        reported_count=None,
        count_kind=SizingCountKind.EXACT,
        hard_window=10_000,
        window_status=SizingWindowStatus.UNKNOWN,
        syntax_status=SizingSyntaxStatus.UNTESTED,
        transport_status=SizingTransportStatus.PLANNED,
        attempts=[attempt],
    )
    return QuerySizingRun(
        schema_version="1.1.0",
        sizing_run_id="star-query-sizing-v0-1-run-001",
        candidate_set_id=candidates.candidate_set_id,
        candidate_set_version=candidates.candidate_set_version,
        candidate_set_hash=candidates.candidate_set_hash(),
        sentinel_set_id=sentinels.sentinel_set_id,
        sentinel_set_version=sentinels.sentinel_set_version,
        sentinel_set_hash=sentinels.sentinel_set_hash(),
        status=SizingRunStatus.PLANNED,
        planned_candidate_query_ids=[candidate.candidate_query_id],
        created_at="2026-09-01T13:18:46Z",
        observations=[observation],
    )


def test_frozen_sentinel_membership_order_and_hash() -> None:
    sentinel_set = load_sentinel_set(SENTINEL_CONFIG)

    assert [entry.source_identifier for entry in sentinel_set.entries] == [
        "biowheel-2017",
        "icave-2017",
        "aegis-2018",
        "dtbia-2025",
        "wang-et-al-2025",
        "phenoflow-2025",
    ]
    assert sentinel_set.candidate_set_hash == EXPECTED_CANDIDATE_HASH
    assert sentinel_set.sentinel_set_hash() == EXPECTED_SENTINEL_HASH
    assert sentinel_set.accuracy_interpretation == "prohibited"
    assert sentinel_set.recall_interpretation == "prohibited"
    assert sentinel_set.contributes_prisma_counts is False
    assert sentinel_set.creates_occurrences is False
    assert sentinel_set.creates_corpus_membership is False


def test_sentinel_mutation_requires_new_version_and_hash() -> None:
    original = load_sentinel_set(SENTINEL_CONFIG)
    mutated = copy.deepcopy(original)
    mutated.entries[0] = copy.deepcopy(mutated.entries[0])
    object.__setattr__(mutated.entries[0], "title", "Changed title")

    with pytest.raises(ValueError, match="new version"):
        validate_sentinel_revision(original, mutated)

    mutated.sentinel_set_version = "0.1.1-pre-sizing"
    validate_sentinel_revision(original, mutated)
    assert mutated.sentinel_set_hash() != original.sentinel_set_hash()


def test_dry_run_renders_exact_candidate_matrix_and_gates() -> None:
    report = build_sizing_dry_run(CANDIDATE_CONFIG, SENTINEL_CONFIG)
    specs = report["candidate_specifications"]

    assert len(specs) == 62
    assert len({item["candidate_query_id"] for item in specs}) == 62
    assert len(report["sentinel_diagnostic_specifications"]) == 164
    assert not any(
        item["source"] == "CrossRef"
        and item["family_id"] == "STAR-QF03-INTERACTIVE-SYSTEMS"
        for item in specs
    )
    semantic = [item for item in specs if item["source"] == "SemanticScholar"]
    assert len(semantic) == 9
    assert all(item["syntax_gates"] == ["bulk_boolean_semantics_unverified"] for item in semantic)
    crossref = [item for item in specs if item["source"] == "CrossRef"]
    assert len(crossref) == 8
    assert all(
        item["syntax_gates"] == ["general_query_identification_semantics_unverified"]
        for item in crossref
    )


def test_dry_run_requests_and_report_are_deterministic() -> None:
    first = build_sizing_dry_run(CANDIDATE_CONFIG, SENTINEL_CONFIG)
    second = build_sizing_dry_run(CANDIDATE_CONFIG, SENTINEL_CONFIG)

    assert first == second
    assert first["report_hash"] == second["report_hash"]
    for specification in first["candidate_specifications"]:
        assert sizing_request_hash(specification["request"]) == specification["request_hash"]


def test_dry_run_preserves_source_specific_minimum_requests() -> None:
    report = build_sizing_dry_run(CANDIDATE_CONFIG, SENTINEL_CONFIG)
    specs = report["candidate_specifications"]
    by_source = {}
    for item in specs:
        by_source.setdefault(item["source"], item)

    assert by_source["PubMed"]["request"]["params"]["retmax"] == 0
    assert by_source["EuropePMC"]["request"]["params"]["pageSize"] == 1
    assert by_source["SemanticScholar"]["request"]["params"] == {
        "query": by_source["SemanticScholar"]["request"]["params"]["query"],
        "limit": 1,
        "fields": "paperId",
        "sort": "paperId:asc",
    }
    assert by_source["arXiv"]["request"]["params"]["max_results"] == 1
    assert by_source["IEEEXplore"]["request"]["params"]["max_records"] == 1
    assert by_source["IEEEXplore"]["credential_reference"] == "IEEE_XPLORE_API_KEY"
    assert by_source["CrossRef"]["request"]["params"]["rows"] == 0
    assert by_source["CrossRef"]["request"]["fallback"]["params"]["rows"] == 1
    assert by_source["ACMDigitalLibrary"]["request"]["transport"] == "human_ui"
    assert by_source["ACMDigitalLibrary"]["request"]["citation_export"] is False


def test_attempt_retry_lineage_and_round_trip() -> None:
    request = _request()
    request_hash = sizing_request_hash(request)
    first = SizingAttempt(
        attempt_number=1,
        started_at="2026-09-01T14:00:00Z",
        completed_at="2026-09-01T14:00:01Z",
        request=request,
        request_hash=request_hash,
        transport_status=SizingTransportStatus.FAILED,
        response_status=429,
        response_hash="a" * 64,
        credential_reference="IEEE_XPLORE_API_KEY",
        errors=["rate limited"],
    )
    second = SizingAttempt(
        attempt_number=2,
        started_at="2026-09-01T14:00:02Z",
        completed_at="2026-09-01T14:00:03Z",
        request=request,
        request_hash=request_hash,
        transport_status=SizingTransportStatus.SUCCEEDED,
        response_status=200,
        retry_of_attempt_number=1,
        retry_reason="HTTP 429",
        response_hash="b" * 64,
        credential_reference="IEEE_XPLORE_API_KEY",
    )

    assert SizingAttempt.from_dict(first.to_dict()) == first
    assert SizingAttempt.from_dict(second.to_dict()) == second

    invalid = copy.deepcopy(second)
    object.__setattr__(invalid, "retry_of_attempt_number", None)
    with pytest.raises(ValueError, match="retry lineage"):
        invalid.validate()


def test_credentials_are_references_and_secret_values_are_rejected() -> None:
    request = _request()
    attempt = SizingAttempt(
        attempt_number=1,
        started_at="2026-09-01T14:00:00Z",
        completed_at=None,
        request=request,
        request_hash=sizing_request_hash(request),
        transport_status=SizingTransportStatus.PLANNED,
        credential_reference="IEEE_XPLORE_API_KEY",
    )
    attempt.validate()

    with pytest.raises(ValueError, match="persisted secret"):
        sizing_request_hash({"params": {"apikey": "secret-value"}})
    with pytest.raises(ValueError, match="credential value"):
        secret_query = "api_" + "key=" + "secret-value"
        sizing_request_hash({"url": f"https://example.invalid/?{secret_query}"})


def test_acm_evidence_requires_safe_relative_artifacts() -> None:
    evidence = AcmOperatorEvidence(
        operator_id="operator-001",
        observed_at="2026-09-01T14:00:00Z",
        ui_rendered_query="candidate query",
        ui_reported_count=42,
        artifact_path="evidence/acm/qf01.png",
        artifact_hash="a" * 64,
        institutional_access_tier="institutional",
    )
    assert AcmOperatorEvidence.from_dict(evidence.to_dict()) == evidence

    for path in ("/tmp/qf01.png", "../qf01.png", "evidence\\qf01.png"):
        invalid = copy.deepcopy(evidence)
        object.__setattr__(invalid, "artifact_path", path)
        with pytest.raises(ValueError, match="artifact paths"):
            invalid.validate()


def test_sentinel_diagnostic_states_remain_distinct() -> None:
    missed = SentinelDiagnostic(
        sentinel_id="sentinel:icave-2017",
        source="PubMed",
        candidate_query_id="candidate-1",
        outcome=SentinelDiagnosticOutcome.INDEXED_BUT_QUERY_MISSED,
        identity_state=SentinelIdentityState.RESOLVED,
        source_indexing_state=SentinelSourceIndexingState.INDEXED,
        candidate_match_state=SentinelCandidateMatchState.MISSED,
    )
    not_indexed = SentinelDiagnostic(
        sentinel_id="sentinel:icave-2017",
        source="PubMed",
        candidate_query_id="candidate-1",
        outcome=SentinelDiagnosticOutcome.SOURCE_NOT_INDEXED,
        identity_state=SentinelIdentityState.RESOLVED,
        source_indexing_state=SentinelSourceIndexingState.NOT_INDEXED,
        candidate_match_state=SentinelCandidateMatchState.UNTESTED,
    )
    missed.validate()
    not_indexed.validate()
    assert missed.to_dict() != not_indexed.to_dict()

    invalid = copy.deepcopy(missed)
    object.__setattr__(invalid, "source_indexing_state", SentinelSourceIndexingState.NOT_INDEXED)
    with pytest.raises(ValueError, match="do not match outcome"):
        invalid.validate()


def test_sizing_round_trip_and_legacy_loading(tmp_path: Path) -> None:
    run = _run()
    path = tmp_path / "sizing-run.json"
    save_sizing_run(path, run)
    assert load_sizing_run(path).to_dict() == run.to_dict()
    assert load_sizing_run(path).run_hash() == run.run_hash()

    legacy = run.to_dict()
    legacy["schema_version"] = LEGACY_SIZING_RUN_SCHEMA_VERSION
    legacy["sentinel_set_id"] = None
    legacy["sentinel_set_version"] = None
    legacy["sentinel_set_hash"] = None
    QuerySizingRun.from_dict(legacy).validate()


def test_dry_run_has_no_production_effects_or_partitioning() -> None:
    report = build_sizing_dry_run(CANDIDATE_CONFIG, SENTINEL_CONFIG)
    invariants = report["non_production_invariants"]

    assert invariants["network_calls_performed"] == 0
    assert all(value is False for key, value in invariants.items() if key != "network_calls_performed")
    run = report["run"]
    assert run["creates_production_occurrences"] is False
    assert run["contributes_prisma_counts"] is False
    assert run["derives_e6"] is False
    assert run["creates_review_dataset"] is False
    assert run["creates_retrieval_run"] is False
    assert run["supports_partitioning"] is False


def test_dry_run_save_is_deterministic(tmp_path: Path) -> None:
    report = build_sizing_dry_run(CANDIDATE_CONFIG, SENTINEL_CONFIG)
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    first_hash = save_sizing_dry_run(report, first)
    second_hash = save_sizing_dry_run(report, second)
    assert first_hash == second_hash
    assert first.read_bytes() == second.read_bytes()


def test_dry_run_cli_performs_zero_network_activity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail_socket(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"network socket attempted: {args!r} {kwargs!r}")

    monkeypatch.setattr(socket, "socket", fail_socket)
    output = tmp_path / "dry-run.json"
    result = main(
        [
            "--candidate-config",
            str(CANDIDATE_CONFIG),
            "--sentinel-config",
            str(SENTINEL_CONFIG),
            "--output",
            str(output),
        ]
    )

    assert result == 0
    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted["non_production_invariants"]["network_calls_performed"] == 0
