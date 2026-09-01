from __future__ import annotations

import copy
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.query_development import (
    SentinelDiagnosticOutcome,
    SizingGateStatus,
    SizingTransportStatus,
    load_sizing_run,
)
from h2h_lit.query_sizing import canonical_json
from h2h_lit.query_sizing_live import (
    LiveSizingExecutor,
    SizingPlanError,
    build_comparison_report,
    load_validated_sizing_plan,
)

ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = (
    ROOT
    / "outputs"
    / "query_sizing"
    / "star-query-sizing-v0-2-run-001"
    / "dry_run.json"
)
V2_CANDIDATES = ROOT / "config" / "star_query_candidates_v0_2.json"
SENTINELS = ROOT / "config" / "star_query_sentinels_v0_1.json"
EXPECTED_RAW_HASH = "b087382131b050f4c7537dcba6862795789db3b3f14ab5f5ad9026693e719e57"
EXPECTED_PLAN_HASH = "8e670a14848aa7cfa7562c53bfeb0ee545cd03078f4687ca0c42039ba9ac11a9"

PASSING_COUNTS = {
    "visualization": 100,
    "biology": 80,
    "visualization AND biology": 20,
    "visualization OR biology": 160,
    "visualization AND (biology OR interactive)": 30,
    "(visualization AND biology) OR (visualization AND interactive)": 30,
}


@dataclass(slots=True)
class FakeResponse:
    status_code: int
    content: bytes
    headers: dict[str, str]

    def json(self) -> Any:
        return json.loads(self.content)


class SemanticControlClient:
    def __init__(
        self,
        counts: dict[str, int] | None = None,
        *,
        fail_query: str | None = None,
        retry_status: int | None = None,
    ) -> None:
        self.counts = dict(counts or PASSING_COUNTS)
        self.fail_query = fail_query
        self.retry_status = retry_status
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, *, params=None, headers=None, **kwargs):
        params = dict(params or {})
        self.calls.append((url, params))
        if "eutils.ncbi" in url:
            return FakeResponse(
                200,
                b"<eSearchResult><Count>17</Count><QueryTranslation>frozen</QueryTranslation></eSearchResult>",
                {},
            )
        query = str(params.get("query", ""))
        if query == self.fail_query:
            if self.retry_status is not None:
                return FakeResponse(
                    self.retry_status,
                    b'{"error":"temporary"}',
                    {"Retry-After": "2"},
                )
            raise TimeoutError("mocked control timeout")
        if query in self.counts:
            body = {
                "total": self.counts[query],
                "token": "not-persisted-token",
                "data": [
                    {
                        "paperId": "SHOULD_NOT_PERSIST",
                        "title": "ARBITRARY RETURNED PAPER CONTENT",
                    }
                ],
            }
            return FakeResponse(200, json.dumps(body).encode(), {})
        body = {
            "total": 11,
            "token": "candidate-token",
            "data": [{"paperId": "CANDIDATE-PAPER-ID"}],
        }
        return FakeResponse(200, json.dumps(body).encode(), {})


def _plan():
    return load_validated_sizing_plan(PLAN_PATH, V2_CANDIDATES, SENTINELS)


def _subset_plan(plan, *, include_pubmed: bool = False, one_semantic: bool = False):
    semantic = [item for item in plan.candidate_specs if item["source"] == "SemanticScholar"]
    selected = semantic[:1] if one_semantic else semantic
    if include_pubmed:
        selected = [
            next(item for item in plan.candidate_specs if item["source"] == "PubMed"),
            *selected,
        ]
    identifiers = [item["candidate_query_id"] for item in selected]
    payload = copy.deepcopy(plan.payload)
    payload["candidate_specifications"] = selected
    payload["run"]["planned_candidate_query_ids"] = identifiers
    payload["run"]["observations"] = [
        item
        for item in payload["run"]["observations"]
        if item["candidate_query_id"] in identifiers
    ]
    diagnostics = tuple(
        item
        for item in plan.diagnostic_specs
        if item["candidate_query_id"] in identifiers
    )
    identity_ids = {item["identity_resolution_id"] for item in diagnostics}
    identities = tuple(
        item
        for item in plan.identity_specs
        if item["identity_resolution_id"] in identity_ids
    )
    return replace(
        plan,
        payload=payload,
        candidate_specs=tuple(selected),
        diagnostic_specs=diagnostics,
        identity_specs=identities,
    )


def _executor(client: Any, *, attempts: int = 3) -> LiveSizingExecutor:
    counter = iter(range(100_000))
    return LiveSizingExecutor(
        http=client,
        retry_policy=RetryPolicy(max_attempts=attempts, base_delay_seconds=0),
        rate_limiter=RateLimiter(minimum_intervals={}),
        sleep=lambda _: None,
        timestamp=lambda: f"2026-09-01T19:00:{next(counter):05d}Z",
    )


def _run(
    tmp_path: Path,
    client: SemanticControlClient,
    *,
    plan=None,
    execute_diagnostics: bool = False,
    attempts: int = 3,
):
    selected = plan or _subset_plan(_plan())
    return _executor(client, attempts=attempts).execute(
        selected,
        tmp_path / "run.json",
        tmp_path / "report.json",
        execute_diagnostics=execute_diagnostics,
    )


def _control_queries(client: SemanticControlClient) -> list[str]:
    return [
        str(params["query"])
        for url, params in client.calls
        if "semanticscholar" in url and str(params.get("query", "")) in PASSING_COUNTS
    ]


def test_frozen_plan_hashes_and_control_integrity() -> None:
    import hashlib

    assert hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest() == EXPECTED_RAW_HASH
    plan = _plan()
    assert plan.plan_hash == EXPECTED_PLAN_HASH
    assert len(plan.semantic_control_specs) == 6
    assert all(item["mode"] == "bulk" for item in plan.semantic_control_specs)


def test_passing_controls_release_all_semantic_candidates_and_link_gate(
    tmp_path: Path,
) -> None:
    client = SemanticControlClient()
    run = _run(tmp_path, client)

    assert len(run.semantic_control_observations) == 6
    assert all(
        item.transport_status is SizingTransportStatus.SUCCEEDED
        for item in run.semantic_control_observations
    )
    assert run.semantic_control_gate is not None
    assert run.semantic_control_gate.state is SizingGateStatus.PASSED
    assert len(run.semantic_control_gate.assertion_results) == 5
    assert all(item.passed for item in run.semantic_control_gate.assertion_results)
    semantic = [item for item in run.observations if item.source == "SemanticScholar"]
    assert len(semantic) == 9
    assert all(item.attempts for item in semantic)
    assert all(item.gate_status is SizingGateStatus.PASSED for item in semantic)
    assert all(
        item.gate_evaluation_id == run.semantic_control_gate.evaluation_id
        for item in semantic
    )
    assert _control_queries(client) == list(PASSING_COUNTS)
    assert len(client.calls) == 15
    persisted = (tmp_path / "run.json").read_text()
    pending = "\n".join(
        path.read_text()
        for path in (tmp_path / ".query_sizing_pending").glob("semantic-control-*.json")
    )
    assert "SHOULD_NOT_PERSIST" not in persisted + pending
    assert "ARBITRARY RETURNED PAPER CONTENT" not in persisted + pending
    assert "not-persisted-token" not in persisted + pending

    report = build_comparison_report(run)["semantic_controls"]
    assert report["planned"] == 6
    assert report["completed"] == 6
    assert len(report["candidate_requests_executed"]) == 9
    assert report["candidate_requests_blocked"] == []


def test_failed_assertion_blocks_nine_candidates_but_not_other_sources(
    tmp_path: Path,
) -> None:
    counts = {**PASSING_COUNTS, "visualization AND biology": 120}
    client = SemanticControlClient(counts)
    run = _run(
        tmp_path,
        client,
        plan=_subset_plan(_plan(), include_pubmed=True),
    )

    assert run.semantic_control_gate is not None
    assert run.semantic_control_gate.state is SizingGateStatus.FAILED
    assert run.semantic_control_gate.reasons == [
        "assertion_failed:and-not-greater-than-a",
        "assertion_failed:and-not-greater-than-b",
    ]
    semantic = [item for item in run.observations if item.source == "SemanticScholar"]
    pubmed = next(item for item in run.observations if item.source == "PubMed")
    assert all(item.transport_status is SizingTransportStatus.BLOCKED_GATE for item in semantic)
    assert all(not item.attempts for item in semantic)
    assert pubmed.transport_status is SizingTransportStatus.SUCCEEDED
    assert len(_control_queries(client)) == 6
    assert len(client.calls) == 7


def test_unresolved_control_exhausts_retries_and_blocks_candidates(
    tmp_path: Path,
) -> None:
    client = SemanticControlClient(fail_query="visualization", retry_status=429)
    run = _run(tmp_path, client, attempts=3)

    assert run.semantic_control_gate is not None
    assert run.semantic_control_gate.state is SizingGateStatus.UNRESOLVED
    failed = next(
        item for item in run.semantic_control_observations if item.probe_id == "atomic-a"
    )
    assert len(failed.attempts) == 3
    assert all(item.retry_after == "2" for item in failed.attempts)
    assert all(
        item.transport_status is SizingTransportStatus.BLOCKED_GATE
        for item in run.observations
    )
    assert all(not item.attempts for item in run.observations)


def test_invalid_control_count_is_unresolved_and_not_retried(tmp_path: Path) -> None:
    class MissingCountClient(SemanticControlClient):
        def get(self, url: str, *, params=None, headers=None, **kwargs):
            params = dict(params or {})
            query = str(params.get("query", ""))
            if query == "visualization":
                self.calls.append((url, params))
                return FakeResponse(200, b'{"data":[]}', {})
            return super().get(url, params=params, headers=headers, **kwargs)

    client = MissingCountClient()
    run = _run(tmp_path, client)
    assert run.semantic_control_gate is not None
    assert run.semantic_control_gate.state is SizingGateStatus.UNRESOLVED
    failed = next(
        item for item in run.semantic_control_observations if item.probe_id == "atomic-a"
    )
    assert failed.failure_state == "parser_failure"
    assert len(failed.attempts) == 1
    assert all(
        item.transport_status is SizingTransportStatus.BLOCKED_GATE
        for item in run.observations
    )


def test_resume_does_not_reissue_committed_controls(tmp_path: Path) -> None:
    class CrashAfterThreeControls(LiveSizingExecutor):
        crashed = False

        def _checkpoint_semantic_control(self, run, plan, by_id, output):
            super()._checkpoint_semantic_control(run, plan, by_id, output)
            completed = sum(
                item.transport_status is SizingTransportStatus.SUCCEEDED
                for item in by_id.values()
            )
            if completed == 3 and not self.crashed:
                self.crashed = True
                raise RuntimeError("simulated interruption after three controls")

    client = SemanticControlClient()
    plan = _subset_plan(_plan(), one_semantic=True)
    base = _executor(client)
    crashing = CrashAfterThreeControls(
        http=client,
        retry_policy=base.retry_policy,
        rate_limiter=base.rate_limiter,
        sleep=base.sleep,
        timestamp=base.timestamp,
    )
    with pytest.raises(RuntimeError, match="three controls"):
        crashing.execute(
            plan,
            tmp_path / "run.json",
            tmp_path / "report.json",
            execute_diagnostics=False,
        )
    assert len(_control_queries(client)) == 3

    _executor(client).execute(
        plan,
        tmp_path / "run.json",
        tmp_path / "report.json",
        execute_diagnostics=False,
    )
    assert len(_control_queries(client)) == 6


def test_pending_control_response_replays_without_transport(tmp_path: Path) -> None:
    class CrashBeforeControlCommit(LiveSizingExecutor):
        crashed = False

        def _checkpoint_semantic_control(self, run, plan, by_id, output):
            completed = sum(
                item.transport_status is SizingTransportStatus.SUCCEEDED
                for item in by_id.values()
            )
            if completed == 1 and not self.crashed:
                self.crashed = True
                raise RuntimeError("simulated precommit interruption")
            super()._checkpoint_semantic_control(run, plan, by_id, output)

    client = SemanticControlClient()
    plan = _subset_plan(_plan(), one_semantic=True)
    base = _executor(client)
    crashing = CrashBeforeControlCommit(
        http=client,
        retry_policy=base.retry_policy,
        rate_limiter=base.rate_limiter,
        sleep=base.sleep,
        timestamp=base.timestamp,
    )
    with pytest.raises(RuntimeError, match="precommit"):
        crashing.execute(
            plan,
            tmp_path / "run.json",
            tmp_path / "report.json",
            execute_diagnostics=False,
        )
    assert len(_control_queries(client)) == 1

    _executor(client).execute(
        plan,
        tmp_path / "run.json",
        tmp_path / "report.json",
        execute_diagnostics=False,
    )
    assert len(_control_queries(client)) == 6


def test_candidates_cannot_execute_before_persisted_pass(tmp_path: Path) -> None:
    class CrashAfterGateCommit(LiveSizingExecutor):
        crashed = False

        def _checkpoint_semantic_control(self, run, plan, by_id, output):
            super()._checkpoint_semantic_control(run, plan, by_id, output)
            if run.semantic_control_gate is not None and not self.crashed:
                self.crashed = True
                raise RuntimeError("simulated interruption after gate commit")

    client = SemanticControlClient()
    plan = _subset_plan(_plan(), one_semantic=True)
    base = _executor(client)
    executor = CrashAfterGateCommit(
        http=client,
        retry_policy=base.retry_policy,
        rate_limiter=base.rate_limiter,
        sleep=base.sleep,
        timestamp=base.timestamp,
    )
    with pytest.raises(RuntimeError, match="gate commit"):
        executor.execute(
            plan,
            tmp_path / "run.json",
            tmp_path / "report.json",
            execute_diagnostics=False,
        )
    checkpoint = load_sizing_run(tmp_path / "run.json")
    assert checkpoint.semantic_control_gate is not None
    assert checkpoint.semantic_control_gate.state is SizingGateStatus.PASSED
    assert len(client.calls) == 6


def test_resume_after_all_controls_commit_evaluates_gate_without_reissue(
    tmp_path: Path,
) -> None:
    class CrashBeforeGateEvaluation(LiveSizingExecutor):
        crashed = False

        def _checkpoint_semantic_control(self, run, plan, by_id, output):
            super()._checkpoint_semantic_control(run, plan, by_id, output)
            completed = sum(
                item.transport_status is SizingTransportStatus.SUCCEEDED
                for item in by_id.values()
            )
            if completed == 6 and run.semantic_control_gate is None and not self.crashed:
                self.crashed = True
                raise RuntimeError("simulated interruption before gate evaluation")

    client = SemanticControlClient()
    plan = _subset_plan(_plan(), one_semantic=True)
    base = _executor(client)
    crashing = CrashBeforeGateEvaluation(
        http=client,
        retry_policy=base.retry_policy,
        rate_limiter=base.rate_limiter,
        sleep=base.sleep,
        timestamp=base.timestamp,
    )
    with pytest.raises(RuntimeError, match="before gate evaluation"):
        crashing.execute(
            plan,
            tmp_path / "run.json",
            tmp_path / "report.json",
            execute_diagnostics=False,
        )
    assert len(_control_queries(client)) == 6
    checkpoint = load_sizing_run(tmp_path / "run.json")
    assert len(checkpoint.semantic_control_observations) == 6
    assert checkpoint.semantic_control_gate is None

    resumed = _executor(client).execute(
        plan,
        tmp_path / "run.json",
        tmp_path / "report.json",
        execute_diagnostics=False,
    )
    assert len(_control_queries(client)) == 6
    assert resumed.semantic_control_gate is not None
    assert resumed.semantic_control_gate.state is SizingGateStatus.PASSED
    assert resumed.observations[0].attempts


def test_gate_failure_prevents_semantic_match_but_not_identity_resolution(
    tmp_path: Path,
) -> None:
    counts = {**PASSING_COUNTS, "visualization AND biology": 120}
    client = SemanticControlClient(counts)
    plan = _subset_plan(_plan(), one_semantic=True)
    run = _run(tmp_path, client, plan=plan, execute_diagnostics=True)

    semantic_diagnostics = [
        item for item in run.sentinel_diagnostics if item.source == "SemanticScholar"
    ]
    assert semantic_diagnostics
    assert all(
        item.outcome is SentinelDiagnosticOutcome.DIAGNOSTIC_UNSUPPORTED
        for item in semantic_diagnostics
    )
    assert not any(
        item.outcome is SentinelDiagnosticOutcome.SOURCE_NOT_INDEXED
        for item in semantic_diagnostics
    )
    assert any(
        item.source == "SemanticScholar" for item in run.sentinel_identity_resolutions
    )


def test_changed_control_or_request_hash_fails_closed(tmp_path: Path) -> None:
    payload = json.loads(PLAN_PATH.read_text())
    payload["semantic_control_specifications"][0]["request"]["params"]["query"] = "changed"
    changed = tmp_path / "changed-plan.json"
    changed.write_text(canonical_json(payload) + "\n")
    with pytest.raises(SizingPlanError, match="report hash"):
        load_validated_sizing_plan(changed, V2_CANDIDATES, SENTINELS)

    payload["report_hash"] = _hash_without_report_hash(payload)
    changed.write_text(canonical_json(payload) + "\n")
    with pytest.raises(SizingPlanError, match="frozen expression|request hash"):
        load_validated_sizing_plan(changed, V2_CANDIDATES, SENTINELS)


def _hash_without_report_hash(payload: dict[str, Any]) -> str:
    import hashlib

    material = {key: value for key, value in payload.items() if key != "report_hash"}
    return hashlib.sha256(canonical_json(material).encode()).hexdigest()
