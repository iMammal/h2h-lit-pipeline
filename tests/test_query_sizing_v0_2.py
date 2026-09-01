from __future__ import annotations

import hashlib
import json
import re
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.query_development import (
    QuerySizingRun,
    SentinelDiagnosticOutcome,
    SentinelIdentityResolutionStatus,
    SizingGateStatus,
    SizingRunStatus,
    SizingSyntaxStatus,
    evaluate_semantic_control_counts,
    load_candidate_set,
    load_semantic_control_set,
    load_sentinel_set,
    load_sizing_run,
)
from h2h_lit.query_sizing import build_sizing_dry_run, save_sizing_dry_run
from h2h_lit.query_sizing_cli import main as sizing_cli_main
from h2h_lit.query_sizing_live import (
    LiveSizingExecutor,
    ValidatedSizingPlan,
    _parse_envelope,
    load_validated_sizing_plan,
)

ROOT = Path(__file__).resolve().parents[1]
V1_CANDIDATES = ROOT / "config" / "star_query_candidates_v0_1.json"
V2_CANDIDATES = ROOT / "config" / "star_query_candidates_v0_2.json"
SENTINELS = ROOT / "config" / "star_query_sentinels_v0_1.json"
CONTROLS = ROOT / "config" / "star_query_semantic_controls_v0_2.json"
FIXTURES = ROOT / "tests" / "fixtures" / "query_sizing"
V1_OUTPUT = ROOT / "outputs" / "query_sizing" / "star-query-sizing-v0-1-run-001"

V1_RAW_HASH = "3de17186d0c1fc50b5379819370cb4fefea699c6b9b7c6223cb3d8abe4cdfb02"
V1_CANONICAL_HASH = "4c642ff04c84c1e1534566d789278fdab21af9f75a57332fce04fa3751fe01bc"
V2_CANONICAL_HASH = "701bba1a7b40ba508b41df6a8d03d340449b5f67f8ed89ee9e9ad3dcf7cfeaf2"
SENTINEL_HASH = "1acc34ae05f0637bdfb5d3feebe2044197164ae4b6d6ffd0e043daa63bfd46a3"


@dataclass(slots=True)
class FakeResponse:
    status_code: int
    content: bytes
    headers: dict[str, str]

    def json(self) -> Any:
        return json.loads(self.content)


def _v2_report() -> dict[str, Any]:
    return build_sizing_dry_run(
        V2_CANDIDATES,
        SENTINELS,
        run_id="star-query-sizing-v0-2-run-001",
        created_at="2026-09-01T18:00:00Z",
        semantic_control_config=CONTROLS,
    )


def _executor(http: Any, *, attempts: int = 1) -> LiveSizingExecutor:
    counter = iter(range(1000))
    return LiveSizingExecutor(
        http=http,
        retry_policy=RetryPolicy(max_attempts=attempts, base_delay_seconds=0),
        rate_limiter=RateLimiter(minimum_intervals={}),
        sleep=lambda _: None,
        timestamp=lambda: f"2026-09-01T18:00:{next(counter):05d}Z",
    )


def test_v0_1_bytes_and_v0_2_conceptual_terms_are_unchanged() -> None:
    assert hashlib.sha256(V1_CANDIDATES.read_bytes()).hexdigest() == V1_RAW_HASH
    v1 = load_candidate_set(V1_CANDIDATES)
    v2 = load_candidate_set(V2_CANDIDATES)

    assert v1.candidate_set_hash() == V1_CANONICAL_HASH
    assert v2.candidate_set_hash() == V2_CANONICAL_HASH
    assert v2.payload["blocks"] == v1.payload["blocks"]
    assert v2.payload["anchors"] == v1.payload["anchors"]
    assert v2.payload["families"] == v1.payload["families"]


def test_pubmed_uses_leaf_level_title_abstract_scope_and_preserves_grouping() -> None:
    v1 = load_candidate_set(V1_CANDIDATES)
    v2 = load_candidate_set(V2_CANDIDATES)
    v1_queries = {
        (item.family_id, item.variant_id): item
        for item in v1.render_all()
        if item.source == "PubMed"
    }
    for item in (query for query in v2.render_all() if query.source == "PubMed"):
        historical = v1_queries[(item.family_id, item.variant_id)].query_text
        expression = historical[1 : -len(")[Title/Abstract]")]
        leaf_count = len(
            [
                token
                for token in re.findall(r'"[^"\n]+"|\(|\)|\bAND\b|\bOR\b|[^\s()]+', expression)
                if token not in {"(", ")", "AND", "OR"}
            ]
        )
        assert item.query_text.count("[Title/Abstract]") == leaf_count
        assert ")[Title/Abstract]" not in item.query_text
        assert item.query_text.count("(") == expression.count("(")
        assert item.query_text.count(")") == expression.count(")")


def test_pubmed_structured_messages_and_exact_translation_are_preserved() -> None:
    response = FakeResponse(
        200,
        (FIXTURES / "pubmed_structured_messages.xml").read_bytes(),
        {},
    )
    parsed = _parse_envelope(
        "PubMed",
        response,
        parser_contract="pubmed_esearch_structured_messages_v0_2",
    )

    assert parsed.count == 37
    assert parsed.syntax_status is SizingSyntaxStatus.WARNING
    assert parsed.translation == (
        "(biology[Title/Abstract]) AND (visualization[Title/Abstract])"
    )
    assert parsed.source_messages == {
        "errors": {"PhraseNotFound": ['"unmatched historical phrase"']},
        "warnings": {
            "QuotedPhraseNotFound": ['"another unmatched phrase"'],
            "OutputMessage": ["No items found for one leaf term."],
        },
    }


def test_arxiv_v0_2_uses_structural_errors_only_and_drops_bibliographic_text() -> None:
    content = (FIXTURES / "arxiv_positional_error.xml").read_bytes()
    response = FakeResponse(200, content, {})

    parsed = _parse_envelope(
        "arXiv",
        response,
        parser_contract="arxiv_structural_error_feed_v0_2",
    )
    historical = _parse_envelope("arXiv", response)

    assert parsed.count == 1350
    assert parsed.syntax_status is SizingSyntaxStatus.ACCEPTED
    assert parsed.warnings == ()
    assert "positional error" not in json.dumps(parsed.__dict__ if hasattr(parsed, "__dict__") else str(parsed))
    assert historical.syntax_status is SizingSyntaxStatus.REJECTED


def test_v0_2_plan_has_expected_matrix_identity_and_control_counts() -> None:
    report = _v2_report()
    candidates = report["candidate_specifications"]
    identities = report["sentinel_identity_specifications"]
    diagnostics = report["sentinel_diagnostic_specifications"]
    controls = report["semantic_control_specifications"]

    assert len(candidates) == 54
    assert len(identities) == 42
    assert len(diagnostics) == 144
    assert len(controls) == 6
    assert len({(item["source"], item["sentinel_id"]) for item in identities}) == 42
    assert not any(item["source"] == "CrossRef" for item in candidates)
    assert sum(item["source"] == "CrossRef" for item in identities) == 6
    assert all(item["mode"] == "bulk" for item in controls)
    assert all("partition" not in json.dumps(item).lower() for item in candidates)


def test_v0_2_cli_and_plan_validation_are_offline_and_deterministic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_socket(*args: object, **kwargs: object) -> None:
        raise AssertionError(f"network socket attempted: {args!r} {kwargs!r}")

    monkeypatch.setattr(socket, "socket", fail_socket)
    output = tmp_path / "dry-run-v0.2.json"
    assert sizing_cli_main(
        [
            "--candidate-config",
            str(V2_CANDIDATES),
            "--sentinel-config",
            str(SENTINELS),
            "--semantic-control-config",
            str(CONTROLS),
            "--run-id",
            "star-query-sizing-v0-2-run-001",
            "--created-at",
            "2026-09-01T18:00:00Z",
            "--output",
            str(output),
        ]
    ) == 0
    plan = load_validated_sizing_plan(output, V2_CANDIDATES, SENTINELS)
    assert len(plan.candidate_specs) == 54
    assert len(plan.identity_specs) == 42
    assert len(plan.diagnostic_specs) == 144
    assert len(plan.semantic_control_specs) == 6

    second = tmp_path / "dry-run-v0.2-second.json"
    save_sizing_dry_run(_v2_report(), second)
    assert output.read_bytes() == second.read_bytes()


def test_source_sentinel_identity_is_resolved_once_and_reused(tmp_path: Path) -> None:
    report = _v2_report()
    matching = [
        item
        for item in report["sentinel_diagnostic_specifications"]
        if item["source"] == "PubMed" and item["sentinel_id"] == "sentinel:icave-2017"
    ][:2]
    resolution_id = matching[0]["identity_resolution_id"]
    assert all(item["identity_resolution_id"] == resolution_id for item in matching)
    identity = next(
        item
        for item in report["sentinel_identity_specifications"]
        if item["identity_resolution_id"] == resolution_id
    )

    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def get(self, url: str, *, params=None, **kwargs):
            self.calls.append(dict(params or {}))
            return FakeResponse(
                200,
                b"<eSearchResult><Count>1</Count><IdList><Id>123</Id></IdList></eSearchResult>",
                {},
            )

    plan_run = report["run"]
    plan = ValidatedSizingPlan(
        report,
        report["report_hash"],
        (),
        tuple(matching),
        (identity,),
        (),
    )
    run = QuerySizingRun(
        schema_version="1.2.0",
        sizing_run_id=plan_run["sizing_run_id"],
        candidate_set_id=plan_run["candidate_set_id"],
        candidate_set_version=plan_run["candidate_set_version"],
        candidate_set_hash=plan_run["candidate_set_hash"],
        sentinel_set_id=plan_run["sentinel_set_id"],
        sentinel_set_version=plan_run["sentinel_set_version"],
        sentinel_set_hash=plan_run["sentinel_set_hash"],
        status=SizingRunStatus.PLANNED,
        planned_candidate_query_ids=[item["candidate_query_id"] for item in matching],
        created_at=plan_run["created_at"],
    )
    client = Client()
    diagnostics = _executor(client)._execute_diagnostics(
        plan, run, {}, tmp_path / "run.json"
    )

    assert len(run.sentinel_identity_resolutions) == 1
    assert run.sentinel_identity_resolutions[0].status is (
        SentinelIdentityResolutionStatus.RESOLVED_INDEXED
    )
    assert len(run.sentinel_identity_resolutions[0].attempts) == 1
    assert len(client.calls) == 3  # one identity lookup, then two candidate-match probes
    assert all(item.outcome is SentinelDiagnosticOutcome.INDEXED_AND_MATCHED for item in diagnostics)


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (RuntimeError("offline transport failure"), SentinelIdentityResolutionStatus.TRANSPORT_FAILURE),
        (FakeResponse(200, b"not XML", {}), SentinelIdentityResolutionStatus.PARSER_FAILURE),
    ],
)
def test_identity_failures_never_become_source_not_indexed(
    response: Exception | FakeResponse,
    expected: SentinelIdentityResolutionStatus,
) -> None:
    report = _v2_report()
    spec = next(
        item
        for item in report["sentinel_identity_specifications"]
        if item["source"] == "PubMed" and item["doi"]
    )

    class Client:
        def get(self, *args, **kwargs):
            if isinstance(response, Exception):
                raise response
            return response

    resolution = _executor(Client())._execute_identity_resolution(spec, {})
    assert resolution.status is expected
    assert resolution.status is not SentinelIdentityResolutionStatus.CONCLUSIVELY_NOT_INDEXED


def test_semantic_scholar_controls_are_deterministic_and_gate_failures() -> None:
    controls = load_semantic_control_set(CONTROLS)
    assert controls.automatic_mode_switching is False
    assert controls.control_set_hash() == (
        "574585774db8f780173490dfc1769a982f5f272d35992261e9103059ee2f1694"
    )
    passing = {
        "atomic-a": 100,
        "atomic-b": 80,
        "a-and-b": 20,
        "a-or-b": 160,
        "grouped-left": 30,
        "grouped-right": 30,
    }
    assert evaluate_semantic_control_counts(controls, passing) == (
        SizingGateStatus.PASSED,
        [],
    )
    failing = {**passing, "a-and-b": 120, "grouped-right": 31}
    status, failures = evaluate_semantic_control_counts(controls, failing)
    assert status is SizingGateStatus.FAILED
    assert failures == ["and-not-greater-than-a", "and-not-greater-than-b", "equivalent-grouping-count"]


def test_crossref_role_and_manual_credential_boundaries_are_explicit() -> None:
    candidate_set = load_candidate_set(V2_CANDIDATES)
    crossref = candidate_set.payload["sources"]["CrossRef"]
    report = _v2_report()

    assert crossref["identification_sizing_enabled"] is False
    assert set(crossref["capabilities"]) == {
        "doi_metadata_enrichment",
        "exact_identity_resolution",
        "deduplication_support",
    }
    observations = report["run"]["observations"]
    assert sum(item["source"] == "IEEEXplore" for item in observations) == 9
    assert all(
        item["credential_reference"] == "IEEE_XPLORE_API_KEY"
        for item in observations
        if item["source"] == "IEEEXplore"
    )
    assert sum(item["source"] == "ACMDigitalLibrary" for item in observations) == 9
    assert all(
        item["request"]["transport"] == "human_ui"
        for item in observations
        if item["source"] == "ACMDigitalLibrary"
    )


def test_sentinel_artifact_is_reused_unchanged_and_dtbia_remains_unresolved() -> None:
    sentinels = load_sentinel_set(SENTINELS)
    report = _v2_report()

    assert sentinels.sentinel_set_hash() == SENTINEL_HASH
    assert [item.source_identifier for item in sentinels.entries] == [
        "biowheel-2017",
        "icave-2017",
        "aegis-2018",
        "dtbia-2025",
        "wang-et-al-2025",
        "phenoflow-2025",
    ]
    dtbia = [
        item
        for item in report["sentinel_identity_specifications"]
        if item["sentinel_id"] == "sentinel:dtbia-2025"
    ]
    assert len(dtbia) == 7
    assert all(item["execution_status"] == "identity_unresolved" for item in dtbia)


def test_v0_2_dry_run_is_non_production_and_v0_1_artifacts_still_load() -> None:
    report = _v2_report()
    invariants = report["non_production_invariants"]

    assert invariants["network_calls_performed"] == 0
    assert all(
        value is False
        for key, value in invariants.items()
        if key != "network_calls_performed"
    )
    assert build_sizing_dry_run(V1_CANDIDATES, SENTINELS)["run"]["schema_version"] == "1.1.0"
    if (V1_OUTPUT / "query_sizing_run.json").is_file():
        historical = load_sizing_run(V1_OUTPUT / "query_sizing_run.json")
        assert historical.schema_version == "1.1.0"
        assert historical.sentinel_identity_resolutions == []


def test_v0_1_checksum_manifest_records_ignored_storage_limitation() -> None:
    manifest = json.loads(
        (ROOT / "provenance" / "star_query_sizing_v0_1_checksum_manifest.json").read_text()
    )
    assert manifest["executor_git_commit"] == "1b75090de2097c8c910dc28ad57c6178313adc3f"
    assert manifest["immutable_artifact_storage"]["status"] == "unresolved"
    assert "do not preserve" in manifest["immutable_artifact_storage"]["limitation"]
    tracked = {item["path"]: item for item in manifest["artifacts"]}
    assert tracked["config/star_query_candidates_v0_1.json"]["raw_sha256"] == V1_RAW_HASH
