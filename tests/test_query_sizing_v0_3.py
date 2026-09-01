from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.query_development import (
    SizingGateStatus,
    SizingSyntaxStatus,
    SizingTransportStatus,
    _render_semantic_scholar_bulk,
    load_candidate_set,
    load_semantic_control_set,
    load_sentinel_set,
    sizing_request_hash,
)
from h2h_lit.query_sizing import build_sizing_dry_run, save_sizing_dry_run
from h2h_lit.query_sizing_live import LiveSizingExecutor, load_validated_sizing_plan

ROOT = Path(__file__).resolve().parents[1]
V1_CANDIDATES = ROOT / "config" / "star_query_candidates_v0_1.json"
V2_CANDIDATES = ROOT / "config" / "star_query_candidates_v0_2.json"
V3_CANDIDATES = ROOT / "config" / "star_query_candidates_v0_3.json"
V2_CONTROLS = ROOT / "config" / "star_query_semantic_controls_v0_2.json"
V3_CONTROLS = ROOT / "config" / "star_query_semantic_controls_v0_3.json"
SENTINELS = ROOT / "config" / "star_query_sentinels_v0_1.json"
FIXTURES = ROOT / "tests" / "fixtures" / "query_sizing"
V2_OUTPUT = ROOT / "outputs" / "query_sizing" / "star-query-sizing-v0-2-run-001"

V3_CANDIDATE_HASH = "5add42cd86317a958951917ef5adcdbef3d70cf300f7c4f7511d9a0242ea0b5f"
V3_CONTROL_HASH = "18570dba1111bbb0367f28af602c6a02d9bf6b4c34e9e1083a76c1fb3b8e5e64"
SENTINEL_HASH = "1acc34ae05f0637bdfb5d3feebe2044197164ae4b6d6ffd0e043daa63bfd46a3"


@dataclass(slots=True)
class FakeResponse:
    status_code: int
    content: bytes
    headers: dict[str, str]
    url: str = "https://example.invalid"

    def json(self) -> Any:
        return json.loads(self.content)


def _v3_report() -> dict[str, Any]:
    return build_sizing_dry_run(
        V3_CANDIDATES,
        SENTINELS,
        run_id="star-query-sizing-v0-3-run-001",
        created_at="2026-09-01T21:00:00Z",
        semantic_control_config=V3_CONTROLS,
    )


def _v3_plan(tmp_path: Path):
    path = tmp_path / "dry-run.json"
    save_sizing_dry_run(_v3_report(), path)
    return load_validated_sizing_plan(path, V3_CANDIDATES, SENTINELS)


def _subset_plan(plan, *, source: str, keep_controls: bool):
    selected = [item for item in plan.candidate_specs if item["source"] == source]
    identifiers = [item["candidate_query_id"] for item in selected]
    payload = copy.deepcopy(plan.payload)
    payload["candidate_specifications"] = selected
    payload["run"]["planned_candidate_query_ids"] = identifiers
    payload["run"]["observations"] = [
        item
        for item in payload["run"]["observations"]
        if item["candidate_query_id"] in identifiers
    ]
    if not keep_controls:
        payload["semantic_control_specifications"] = []
        payload["semantic_control_provenance"] = None
    return replace(
        plan,
        payload=payload,
        candidate_specs=tuple(selected),
        diagnostic_specs=(),
        identity_specs=(),
        semantic_control_specs=(plan.semantic_control_specs if keep_controls else ()),
    )


def _executor(client: Any) -> LiveSizingExecutor:
    counter = iter(range(100_000))
    return LiveSizingExecutor(
        http=client,
        retry_policy=RetryPolicy(max_attempts=1, base_delay_seconds=0),
        rate_limiter=RateLimiter(minimum_intervals={}),
        sleep=lambda _: None,
        timestamp=lambda: f"2026-09-01T21:00:{next(counter):05d}Z",
    )


def test_v0_2_checksum_manifest_matches_immutable_artifacts() -> None:
    manifest = json.loads(
        (ROOT / "provenance" / "star_query_sizing_v0_2_checksum_manifest.json").read_text()
    )
    for artifact in manifest["artifacts"]:
        path = ROOT / artifact["path"]
        assert path.stat().st_size == artifact["byte_size"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == artifact["raw_sha256"]
    assert manifest["immutable_artifact_storage"]["status"] == "unresolved"
    assert manifest["immutable_artifact_storage"]["locator"] is None


def test_v0_3_versions_hashes_and_conceptual_content_are_frozen() -> None:
    v1 = load_candidate_set(V1_CANDIDATES)
    v2 = load_candidate_set(V2_CANDIDATES)
    v3 = load_candidate_set(V3_CANDIDATES)
    controls = load_semantic_control_set(V3_CONTROLS)

    assert v3.candidate_set_version == "0.3.0-preproduction"
    assert v3.candidate_set_hash() == V3_CANDIDATE_HASH
    assert controls.control_set_version == "0.3.0-preproduction"
    assert controls.control_set_hash() == V3_CONTROL_HASH
    assert v3.payload["blocks"] == v2.payload["blocks"] == v1.payload["blocks"]
    assert v3.payload["anchors"] == v2.payload["anchors"] == v1.payload["anchors"]
    assert v3.payload["families"] == v2.payload["families"] == v1.payload["families"]
    assert load_sentinel_set(SENTINELS).sentinel_set_hash() == SENTINEL_HASH


def test_pubmed_post_preserves_every_v0_2_rendered_query_character() -> None:
    v2 = {
        (item.family_id, item.variant_id): item
        for item in load_candidate_set(V2_CANDIDATES).render_all()
        if item.source == "PubMed"
    }
    v3 = {
        (item.family_id, item.variant_id): item
        for item in load_candidate_set(V3_CANDIDATES).render_all()
        if item.source == "PubMed"
    }
    assert v3.keys() == v2.keys()
    for key, item in v3.items():
        historical = v2[key]
        assert item.query_text == historical.query_text
        assert item.sizing_request == {
            "method": "POST",
            "endpoint": "esearch.fcgi",
            "params": {},
            "form": historical.sizing_request["params"],
            "headers": {"content-type": "application/x-www-form-urlencoded"},
        }

    plan = _v3_report()
    requests = [
        item["request"]
        for item in plan["candidate_specifications"]
        if item["source"] == "PubMed"
    ]
    assert all(item["method"] == "POST" for item in requests)
    assert all(item["params"] == {} for item in requests)
    assert all(item["url"].endswith("/esearch.fcgi") for item in requests)
    assert all(set(item["form"]) == {"db", "term", "retmax", "retmode"} for item in requests)
    assert all(
        item["headers"] == {"content-type": "application/x-www-form-urlencoded"}
        for item in requests
    )


def test_post_request_hash_is_deterministic_method_sensitive_and_secret_safe() -> None:
    form = {"db": "pubmed", "term": "biology[Title/Abstract]", "retmax": 0, "retmode": "xml"}
    post = {
        "transport": "http",
        "method": "POST",
        "url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        "params": {},
        "form": form,
    }
    get = {**post, "method": "GET", "params": form}
    get.pop("form")
    assert sizing_request_hash(post) == sizing_request_hash(copy.deepcopy(post))
    assert sizing_request_hash(post) != sizing_request_hash(get)
    with pytest.raises(ValueError, match="secret"):
        sizing_request_hash({**post, "form": {**form, "api_key": "do-not-persist"}})


def test_semantic_scholar_renderer_uses_ast_and_preserves_phrase_content() -> None:
    expression = 'alpha AND ("research AND development" OR beta) AND NOT gamma'
    assert _render_semantic_scholar_bulk(expression) == (
        '(alpha + ("research AND development" | beta) + -gamma)'
    )
    assert _render_semantic_scholar_bulk("A OR B AND C") == "(A | (B + C))"


def test_v0_3_controls_preserve_v0_2_logic_with_documented_expressions() -> None:
    v2 = load_semantic_control_set(V2_CONTROLS)
    v3 = load_semantic_control_set(V3_CONTROLS)
    assert [item.to_dict() for item in v3.assertions] == [
        item.to_dict() for item in v2.assertions
    ]
    assert [item.expression for item in v3.probes] == [
        "visualization",
        "biology",
        "visualization + biology",
        "visualization | biology",
        "visualization + (biology | interactive)",
        "(visualization + biology) | (visualization + interactive)",
    ]
    report = _v3_report()
    assert [item["expression"] for item in report["semantic_control_specifications"]] == [
        item.expression for item in v3.probes
    ]


def test_v0_3_plan_matrix_boundaries_and_symbolic_candidates() -> None:
    report = _v3_report()
    assert len(report["candidate_specifications"]) == 54
    assert len(report["sentinel_identity_specifications"]) == 42
    assert len(report["sentinel_diagnostic_specifications"]) == 144
    assert len(report["semantic_control_specifications"]) == 6
    assert not any(
        item["source"] == "CrossRef" for item in report["candidate_specifications"]
    )
    semantic = [
        item for item in report["candidate_specifications"] if item["source"] == "SemanticScholar"
    ]
    assert len(semantic) == 9
    assert all(" + " in item["request"]["params"]["query"] for item in semantic)
    assert all("partition" not in json.dumps(item).lower() for item in report["candidate_specifications"])


def test_pubmed_structured_messages_are_transport_independent() -> None:
    from h2h_lit.query_sizing_live import _parse_envelope

    parsed = _parse_envelope(
        "PubMed",
        FakeResponse(200, (FIXTURES / "pubmed_structured_messages.xml").read_bytes(), {}),
        parser_contract="pubmed_esearch_structured_messages_v0_2",
    )
    assert parsed.syntax_status is SizingSyntaxStatus.WARNING
    assert parsed.translation == (
        "(biology[Title/Abstract]) AND (visualization[Title/Abstract])"
    )
    assert "QuotedPhraseNotFound" in parsed.source_messages["warnings"]


class PubMedPostClient:
    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.posts: list[dict[str, Any]] = []

    def post(self, url: str, *, data=None, params=None, headers=None, **kwargs):
        self.posts.append(
            {
                "url": url,
                "data": dict(data or {}),
                "params": dict(params or {}),
                "headers": dict(headers or {}),
            }
        )
        if self.status == 414:
            return FakeResponse(414, b"", {}, url)
        return FakeResponse(
            200,
            b"<eSearchResult><Count>17</Count><QueryTranslation>exact</QueryTranslation></eSearchResult>",
            {},
            url,
        )

    def get(self, *args, **kwargs):  # pragma: no cover - POST plan must not call this
        raise AssertionError("v0.3 PubMed candidate used GET")


def test_post_checkpoint_replay_avoids_retransmission(tmp_path: Path) -> None:
    class CrashBeforeCommit(LiveSizingExecutor):
        crashed = False

        def _checkpoint(self, run, observations, item, output):
            if item.transport_status is SizingTransportStatus.SUCCEEDED and not self.crashed:
                self.crashed = True
                raise RuntimeError("post response persisted before observation commit")
            return super()._checkpoint(run, observations, item, output)

    plan = _subset_plan(_v3_plan(tmp_path), source="PubMed", keep_controls=False)
    plan = replace(plan, candidate_specs=plan.candidate_specs[:1])
    candidate_id = plan.candidate_specs[0]["candidate_query_id"]
    plan.payload["candidate_specifications"] = list(plan.candidate_specs)
    plan.payload["run"]["planned_candidate_query_ids"] = [candidate_id]
    plan.payload["run"]["observations"] = [
        item
        for item in plan.payload["run"]["observations"]
        if item["candidate_query_id"] == candidate_id
    ]
    client = PubMedPostClient()
    base = _executor(client)
    crashing = CrashBeforeCommit(
        http=client,
        retry_policy=base.retry_policy,
        rate_limiter=base.rate_limiter,
        sleep=base.sleep,
        timestamp=base.timestamp,
    )
    with pytest.raises(RuntimeError, match="post response persisted"):
        crashing.execute(plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False)

    _executor(client).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    assert len(client.posts) == 1
    assert client.posts[0]["data"] == plan.candidate_specs[0]["request"]["form"]
    assert client.posts[0]["headers"] == {
        "content-type": "application/x-www-form-urlencoded"
    }


def test_post_414_is_transport_failure_with_untested_query_syntax(tmp_path: Path) -> None:
    plan = _subset_plan(_v3_plan(tmp_path), source="PubMed", keep_controls=False)
    plan = replace(plan, candidate_specs=plan.candidate_specs[:1])
    candidate_id = plan.candidate_specs[0]["candidate_query_id"]
    plan.payload["candidate_specifications"] = list(plan.candidate_specs)
    plan.payload["run"]["planned_candidate_query_ids"] = [candidate_id]
    plan.payload["run"]["observations"] = [
        item
        for item in plan.payload["run"]["observations"]
        if item["candidate_query_id"] == candidate_id
    ]
    run = _executor(PubMedPostClient(414)).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    observation = run.observations[0]
    assert observation.transport_status is SizingTransportStatus.FAILED
    assert observation.syntax_status is SizingSyntaxStatus.UNTESTED
    assert observation.attempts[0].errors == ["request_target_too_long:414"]


def test_failed_v0_3_semantic_controls_block_all_candidates(tmp_path: Path) -> None:
    counts = {
        "visualization": 100,
        "biology": 80,
        "visualization + biology": 20,
        "visualization | biology": 10,
        "visualization + (biology | interactive)": 30,
        "(visualization + biology) | (visualization + interactive)": 30,
    }

    class Client:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get(self, url: str, *, params=None, **kwargs):
            query = str((params or {})["query"])
            self.calls.append(query)
            return FakeResponse(200, json.dumps({"total": counts[query]}).encode(), {}, url)

    plan = _subset_plan(_v3_plan(tmp_path), source="SemanticScholar", keep_controls=True)
    client = Client()
    run = _executor(client).execute(
        plan, tmp_path / "run.json", tmp_path / "report.json", execute_diagnostics=False
    )
    assert run.semantic_control_gate is not None
    assert run.semantic_control_gate.state is SizingGateStatus.FAILED
    assert len(client.calls) == 6
    assert all(
        item.transport_status is SizingTransportStatus.BLOCKED_GATE
        for item in run.observations
    )


def test_v0_1_and_v0_2_remain_loadable_with_original_hashes() -> None:
    assert load_candidate_set(V1_CANDIDATES).candidate_set_hash() == (
        "4c642ff04c84c1e1534566d789278fdab21af9f75a57332fce04fa3751fe01bc"
    )
    assert load_candidate_set(V2_CANDIDATES).candidate_set_hash() == (
        "701bba1a7b40ba508b41df6a8d03d340449b5f67f8ed89ee9e9ad3dcf7cfeaf2"
    )
    assert hashlib.sha256((V2_OUTPUT / "dry_run.json").read_bytes()).hexdigest() == (
        "b087382131b050f4c7537dcba6862795789db3b3f14ab5f5ad9026693e719e57"
    )
