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
from h2h_lit.query_sizing import build_sizing_dry_run, save_sizing_dry_run
from h2h_lit.query_sizing_live import (
    LiveSizingExecutor,
    SizingPlanError,
    load_validated_sizing_plan,
)

ROOT = Path(__file__).resolve().parents[1]
CANDIDATES = ROOT / "config" / "star_query_candidates_v0_1.json"
SENTINELS = ROOT / "config" / "star_query_sentinels_v0_1.json"


@dataclass(slots=True)
class FakeResponse:
    status_code: int
    content: bytes
    headers: dict[str, str]
    url: str = "https://example.invalid"

    @property
    def text(self) -> str:
        return self.content.decode("utf-8")

    def json(self) -> Any:
        return json.loads(self.content)


class SourceEnvelopeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str]]] = []
        self.pubmed_statuses: list[int] = []

    def get(self, url: str, *, params=None, headers=None, **kwargs):
        params = dict(params or {})
        headers = dict(headers or {})
        self.calls.append((url, params, headers))
        if "eutils.ncbi" in url:
            status = self.pubmed_statuses.pop(0) if self.pubmed_statuses else 200
            if status != 200:
                return FakeResponse(status, b"rate limited", {"Retry-After": "2"}, url)
            if int(params.get("retmax", 0)) == 0:
                body = b"<eSearchResult><Count>17</Count><QueryTranslation>frozen</QueryTranslation></eSearchResult>"
            else:
                body = b"<eSearchResult><Count>1</Count><IdList><Id>123</Id></IdList></eSearchResult>"
            return FakeResponse(200, body, {}, url)
        if "europepmc" in url:
            body = {"hitCount": 13, "resultList": {"result": [{"doi": "10.1/test"}]}}
        elif "semanticscholar" in url:
            body = {"total": 11, "token": "next", "data": [{"paperId": "S2"}]}
        elif "arxiv" in url:
            xml = (
                '<feed xmlns="http://www.w3.org/2005/Atom" '
                'xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">'
                "<opensearch:totalResults>30001</opensearch:totalResults>"
                "<entry><id>arxiv:1</id></entry></feed>"
            )
            return FakeResponse(200, xml.encode(), {}, url)
        elif "ieeexplore" in url:
            body = {"totalfound": 7, "totalsearched": 99, "articles": [{"article_number": "42"}]}
        elif "crossref" in url:
            body = {"message": {"total-results": 19, "items": [{"DOI": "10.1/test"}]}}
        else:  # pragma: no cover
            raise AssertionError(url)
        return FakeResponse(200, json.dumps(body).encode(), {}, url)


def _plan(tmp_path: Path):
    path = tmp_path / "dry_run.json"
    save_sizing_dry_run(
        build_sizing_dry_run(
            CANDIDATES, SENTINELS, created_at="2026-09-01T00:00:00Z"
        ),
        path,
    )
    return path, load_validated_sizing_plan(path, CANDIDATES, SENTINELS)


def _executor(client: Any) -> LiveSizingExecutor:
    counter = iter(range(10_000))
    return LiveSizingExecutor(
        http=client,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0),
        rate_limiter=RateLimiter(minimum_intervals={}),
        sleep=lambda _: None,
        timestamp=lambda: f"2026-09-01T00:00:{next(counter):05d}Z",
    )


def _subset_plan(plan, *, sources: set[str] | None = None, candidate_ids=None):
    selected = [
        item
        for item in plan.candidate_specs
        if (sources is None or item["source"] in sources)
        and (candidate_ids is None or item["candidate_query_id"] in set(candidate_ids))
    ]
    payload = copy.deepcopy(plan.payload)
    ids = [item["candidate_query_id"] for item in selected]
    payload["candidate_specifications"] = selected
    payload["run"]["planned_candidate_query_ids"] = ids
    payload["run"]["observations"] = [
        item for item in payload["run"]["observations"] if item["candidate_query_id"] in ids
    ]
    diagnostics = tuple(
        item for item in plan.diagnostic_specs if item["candidate_query_id"] in ids
    )
    return replace(plan, payload=payload, candidate_specs=tuple(selected),
                   diagnostic_specs=diagnostics)


def test_plan_hash_and_config_hashes_are_verified_before_transport(tmp_path: Path) -> None:
    path, _ = _plan(tmp_path)
    payload = json.loads(path.read_text())
    payload["candidate_specifications"][0]["request"]["params"]["retmax"] = 1
    path.write_text(json.dumps(payload))

    with pytest.raises(SizingPlanError, match="report hash"):
        load_validated_sizing_plan(path, CANDIDATES, SENTINELS)


def test_executes_one_observation_per_candidate_and_keeps_acm_manual(tmp_path: Path) -> None:
    _, plan = _plan(tmp_path)
    client = SourceEnvelopeClient()
    run = _executor(client).execute(
        plan,
        tmp_path / "run.json",
        tmp_path / "report.json",
        credentials={},
        execute_diagnostics=False,
    )

    assert len(run.observations) == 62
    assert len({item.candidate_query_id for item in run.observations}) == 62
    assert sum(item.transport_status is SizingTransportStatus.PENDING_MANUAL
               for item in run.observations) == 9
    assert sum(item.transport_status is SizingTransportStatus.BLOCKED_CREDENTIAL
               for item in run.observations) == 9
    assert all("apikey" not in params for _, params, _ in client.calls)
    assert all(item.reported_count is None for item in run.observations
               if item.source == "ACMDigitalLibrary")


def test_resume_does_not_repeat_completed_requests(tmp_path: Path) -> None:
    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={plan.candidate_specs[0]["candidate_query_id"]})
    client = SourceEnvelopeClient()
    executor = _executor(client)
    output = tmp_path / "run.json"
    report = tmp_path / "report.json"
    executor.execute(plan, output, report, execute_diagnostics=False)
    first_count = len(client.calls)
    first_run = output.read_bytes()
    first_report = report.read_bytes()
    executor.execute(plan, output, report, execute_diagnostics=False)
    assert len(client.calls) == first_count
    assert output.read_bytes() == first_run
    assert report.read_bytes() == first_report


def test_retry_after_and_attempt_lineage_are_preserved(tmp_path: Path) -> None:
    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={next(
        item["candidate_query_id"] for item in plan.candidate_specs if item["source"] == "PubMed"
    )})
    client = SourceEnvelopeClient()
    client.pubmed_statuses = [429, 200]
    delays: list[float] = []
    executor = _executor(client)
    executor.sleep = delays.append
    run = executor.execute(plan, tmp_path / "run.json", tmp_path / "report.json",
                           execute_diagnostics=False)
    retried = next(item for item in run.observations if item.source == "PubMed")
    assert [item.response_status for item in retried.attempts] == [429, 200]
    assert retried.attempts[1].retry_of_attempt_number == 1
    assert delays[0] == 2.0


def test_permanent_syntax_failure_is_not_retried(tmp_path: Path) -> None:
    class SyntaxFailureClient(SourceEnvelopeClient):
        def get(self, url: str, **kwargs):
            if "eutils.ncbi" in url:
                self.calls.append((url, dict(kwargs.get("params") or {}), {}))
                return FakeResponse(400, b"invalid query", {}, url)
            return super().get(url, **kwargs)

    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={next(
        item["candidate_query_id"] for item in plan.candidate_specs if item["source"] == "PubMed"
    )})
    client = SyntaxFailureClient()
    run = _executor(client).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    pubmed = [item for item in run.observations if item.source == "PubMed"]
    assert all(len(item.attempts) == 1 for item in pubmed)
    assert all(item.syntax_status.value == "rejected" for item in pubmed)


def test_response_envelope_is_replayed_after_precommit_interruption(tmp_path: Path) -> None:
    class CrashAfterResponseExecutor(LiveSizingExecutor):
        crashed = False

        def _checkpoint(self, run, observations, item, output):
            if item.transport_status is SizingTransportStatus.SUCCEEDED and not self.crashed:
                self.crashed = True
                raise RuntimeError("simulated interruption after response persistence")
            return super()._checkpoint(run, observations, item, output)

    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={next(
        item["candidate_query_id"] for item in plan.candidate_specs if item["source"] == "PubMed"
    )})
    client = SourceEnvelopeClient()
    base = _executor(client)
    crashing = CrashAfterResponseExecutor(
        http=client,
        retry_policy=base.retry_policy,
        rate_limiter=base.rate_limiter,
        sleep=base.sleep,
        timestamp=base.timestamp,
    )
    output = tmp_path / "run.json"
    with pytest.raises(RuntimeError, match="simulated interruption"):
        crashing.execute(plan, output, tmp_path / "report.json", execute_diagnostics=False)

    _executor(client).execute(plan, output, tmp_path / "report.json", execute_diagnostics=False)
    assert len(client.calls) == 1


def test_source_envelopes_preserve_warnings_gates_and_windows(tmp_path: Path) -> None:
    _, plan = _plan(tmp_path)
    one_per_source = {
        next(item["candidate_query_id"] for item in plan.candidate_specs if item["source"] == source)
        for source in ("PubMed", "SemanticScholar", "CrossRef", "arXiv", "IEEEXplore")
    }
    plan = _subset_plan(plan, candidate_ids=one_per_source)
    run = _executor(SourceEnvelopeClient()).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json",
        credentials={"IEEE_XPLORE_API_KEY": "not-persisted"},
        execute_diagnostics=False,
    )
    pubmed = next(item for item in run.observations if item.source == "PubMed")
    semantic = next(item for item in run.observations if item.source == "SemanticScholar")
    crossref = next(item for item in run.observations if item.source == "CrossRef")
    arxiv = next(item for item in run.observations if item.source == "arXiv")
    ieee = next(item for item in run.observations if item.source == "IEEEXplore")
    assert pubmed.source_query_translation == "frozen"
    assert semantic.gate_status is SizingGateStatus.PENDING
    assert crossref.gate_status is SizingGateStatus.UNRESOLVED
    assert arxiv.window_status.value == "overflow"
    assert "totalsearched=99" in ieee.warnings
    assert "not-persisted" not in (tmp_path / "run.json").read_text()


def test_semantic_boolean_error_is_gate_failure(tmp_path: Path) -> None:
    class SemanticErrorClient(SourceEnvelopeClient):
        def get(self, url: str, **kwargs):
            if "semanticscholar" in url:
                return FakeResponse(200, b'{"error":"Boolean syntax rejected"}', {}, url)
            return super().get(url, **kwargs)

    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={next(
        item["candidate_query_id"] for item in plan.candidate_specs
        if item["source"] == "SemanticScholar"
    )})
    run = _executor(SemanticErrorClient()).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    semantic = [item for item in run.observations if item.source == "SemanticScholar"]
    assert all(item.transport_status is SizingTransportStatus.GATE_FAILED for item in semantic)
    assert all(item.gate_status is SizingGateStatus.FAILED for item in semantic)


def test_crossref_rows_zero_uses_only_approved_fallback(tmp_path: Path) -> None:
    class CrossrefFallbackClient(SourceEnvelopeClient):
        def get(self, url: str, *, params=None, **kwargs):
            if "crossref" in url and params.get("rows") == 0:
                self.calls.append((url, dict(params), dict(kwargs.get("headers") or {})))
                return FakeResponse(200, b'{"message":{}}', {}, url)
            return super().get(url, params=params, **kwargs)

    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={next(
        item["candidate_query_id"] for item in plan.candidate_specs if item["source"] == "CrossRef"
    )})
    client = CrossrefFallbackClient()
    run = _executor(client).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    crossref = [item for item in run.observations if item.source == "CrossRef"]
    assert all(item.reported_count == 19 for item in crossref)
    rows = [params["rows"] for url, params, _ in client.calls if "crossref" in url]
    assert 0 in rows and 1 in rows


def test_crossref_free_text_evidence_fails_identification_semantics_gate(
    tmp_path: Path,
) -> None:
    class CrossrefFreeTextClient(SourceEnvelopeClient):
        def get(self, url: str, **kwargs):
            if "crossref" in url:
                body = {
                    "message": {
                        "total-results": 19,
                        "query": {"search-terms": "the exact frozen candidate string"},
                    }
                }
                return FakeResponse(200, json.dumps(body).encode(), {}, url)
            return super().get(url, **kwargs)

    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={next(
        item["candidate_query_id"] for item in plan.candidate_specs if item["source"] == "CrossRef"
    )})
    run = _executor(CrossrefFreeTextClient()).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    observation = run.observations[0]
    assert observation.reported_count == 19
    assert observation.gate_status is SizingGateStatus.FAILED
    assert observation.transport_status is SizingTransportStatus.GATE_FAILED


def test_sentinel_diagnostics_distinguish_unresolved_miss_and_not_indexed(
    tmp_path: Path,
) -> None:
    _, plan = _plan(tmp_path)
    selected = []
    for sentinel in ("sentinel:biowheel-2017", "sentinel:icave-2017", "sentinel:dtbia-2025"):
        selected.append(next(item for item in plan.diagnostic_specs
                             if item["source"] == "PubMed" and item["sentinel_id"] == sentinel))
    plan = replace(plan, diagnostic_specs=tuple(selected))
    plan = _subset_plan(
        plan, candidate_ids={item["candidate_query_id"] for item in selected}
    )
    plan = replace(plan, diagnostic_specs=tuple(selected))

    class DiagnosticClient(SourceEnvelopeClient):
        def get(self, url: str, *, params=None, headers=None, **kwargs):
            term = str((params or {}).get("term", ""))
            self.calls.append((url, dict(params or {}), dict(headers or {})))
            if "10.1093/gigascience/gix054" in term or " AND " in term:
                body = b"<eSearchResult><Count>0</Count><IdList /></eSearchResult>"
            else:
                body = b"<eSearchResult><Count>1</Count><IdList><Id>123</Id></IdList></eSearchResult>"
            return FakeResponse(200, body, {}, url)

    client = DiagnosticClient()
    run = _executor(client).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json",
        credentials={"IEEE_XPLORE_API_KEY": "secret"}, execute_diagnostics=True,
    )
    outcomes = {item.outcome for item in run.sentinel_diagnostics}
    assert outcomes == {
        SentinelDiagnosticOutcome.IDENTITY_UNRESOLVED,
        SentinelDiagnosticOutcome.INDEXED_BUT_QUERY_MISSED,
        SentinelDiagnosticOutcome.SOURCE_NOT_INDEXED,
    }


def test_checkpoint_round_trip_is_deterministic_and_has_no_production_state(
    tmp_path: Path,
) -> None:
    _, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={plan.candidate_specs[0]["candidate_query_id"]})
    output = tmp_path / "run.json"
    run = _executor(SourceEnvelopeClient()).execute(
        plan, output, tmp_path / "report.json", execute_diagnostics=False
    )
    loaded = load_sizing_run(output)
    assert loaded.to_json() == run.to_json()
    payload = json.loads(output.read_text())
    forbidden = {"record_occurrences", "prisma", "e6", "retrieval_cutoff", "records"}
    assert forbidden.isdisjoint(payload)


def test_resume_refuses_changed_plan_hash(tmp_path: Path) -> None:
    path, plan = _plan(tmp_path)
    plan = _subset_plan(plan, candidate_ids={plan.candidate_specs[0]["candidate_query_id"]})
    output = tmp_path / "run.json"
    _executor(SourceEnvelopeClient()).execute(
        plan, output, tmp_path / "report.json", execute_diagnostics=False
    )
    payload = json.loads(path.read_text())
    payload["interpretation_rules"] = [*payload["interpretation_rules"], "changed"]
    payload["report_hash"] = "0" * 64
    changed = copy.deepcopy(plan)
    object.__setattr__(changed, "plan_hash", "0" * 64)
    with pytest.raises(SizingPlanError, match="different dry-run plan"):
        _executor(SourceEnvelopeClient()).execute(
            changed, output, tmp_path / "report.json", execute_diagnostics=False
        )
