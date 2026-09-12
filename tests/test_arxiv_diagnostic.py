from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from h2h_lit.arxiv_diagnostic import (
    CONTROL_QUERY,
    DIAGNOSTIC_RELATIVE_ROOT,
    QF01_PRODUCTION_QUERY_ID,
    run_arxiv_diagnostic,
)
from h2h_lit.external_retrieval_wave import (
    EXECUTION_STATE_PATH,
    EXTERNAL_SOURCE_SESSION_LOCK_PATH,
    OUTPUT_ROOT,
)
from tests.fake_http import FakeHttp, FakeResponse


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class Timestamps:
    def __init__(self) -> None:
        self.value = datetime(2026, 9, 11, 20, tzinfo=UTC)

    def __call__(self) -> str:
        result = self.value.isoformat().replace("+00:00", "Z")
        self.value += timedelta(seconds=1)
        return result


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _reference(path: Path, root: Path) -> dict:
    content = path.read_bytes()
    return {
        "path": path.relative_to(root).as_posix(),
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _root(tmp_path: Path) -> tuple[Path, str]:
    qf01 = 'all:(("life science") AND ("visual analytics"))'
    checkpoint_path = (
        tmp_path
        / OUTPUT_ROOT
        / "execution/arXiv/episodes/episode-004/checkpoint/review_dataset.json"
    )
    _write_json(
        checkpoint_path,
        {
            "retrieval_runs": [{"run_id": "production:arxiv"}],
            "source_queries": [
                {
                    "query_id": "query:qf01",
                    "query_text": qf01,
                    "query_version": "frozen-v1",
                    "endpoint": "http://export.arxiv.org/api/query",
                    "metadata": {
                        "production_query_id": QF01_PRODUCTION_QUERY_ID,
                        "request_timeout_seconds": 120.0,
                    },
                }
            ],
        },
    )
    state_path = tmp_path / EXECUTION_STATE_PATH
    _write_json(
        state_path,
        {
            "sources": {
                "arXiv": {
                    "status": "PAUSED_PROVIDER_RATE_LIMIT",
                    "active_episode_number": 4,
                    "active_run_id": "production:arxiv",
                    "last_session_completed_at_utc": "2026-09-11T19:21:49Z",
                    "checkpoint_dataset": _reference(checkpoint_path, tmp_path),
                },
                "Other": {"status": "COMPLETE"},
            }
        },
    )
    lock_path = tmp_path / EXTERNAL_SOURCE_SESSION_LOCK_PATH
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch()
    return tmp_path, qf01


def test_first_probe_429_stops_before_production_query(tmp_path: Path) -> None:
    root, _ = _root(tmp_path)
    before = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in (root / OUTPUT_ROOT).rglob("*")
        if path.is_file()
    }
    http = FakeHttp(
        [
            FakeResponse(
                status_code=429,
                headers={"Retry-After": "60", "Content-Type": "text/plain"},
                content=b"Rate exceeded.",
            )
        ]
    )
    clock = Clock()

    result = run_arxiv_diagnostic(
        root=root,
        authorize_live_diagnostic=True,
        http=http,
        timestamp=Timestamps(),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert result["requests_made"] == 1
    assert result["stop_reason"] == "FIRST_PROBE_HTTP_429"
    assert result["attempts"][0]["request"]["params"]["search_query"] == CONTROL_QUERY
    assert result["attempts"][0]["retry_after"] == {
        "header_present": True,
        "value": "60",
        "earliest_retry_at_utc": "2026-09-11T20:01:02Z",
        "interpretation": "delay-seconds",
        "automatically_scheduled": False,
    }
    assert result["production_integrity"]["unchanged"] is True
    after = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in (root / OUTPUT_ROOT).rglob("*")
        if path.is_file()
    }
    assert after == before


def test_two_probes_are_sequential_small_and_never_retried(tmp_path: Path) -> None:
    root, qf01 = _root(tmp_path)
    http = FakeHttp(
        [
            FakeResponse(status_code=200, content=b"<feed />"),
            FakeResponse(status_code=500, content=b"provider error"),
        ]
    )
    clock = Clock()

    result = run_arxiv_diagnostic(
        root=root,
        authorize_live_diagnostic=True,
        http=http,
        timestamp=Timestamps(),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert len(http.calls) == 2
    assert result["requests_made"] == 2
    assert result["stop_reason"] == "DIAGNOSTIC_REQUEST_BUDGET_COMPLETE"
    assert result["attempts"][1]["pacing_delay_seconds"] == 3.0
    assert [call["params"]["search_query"] for call in http.calls] == [CONTROL_QUERY, qf01]
    assert all(call["params"]["start"] == 0 for call in http.calls)
    assert all(call["params"]["max_results"] == 1 for call in http.calls)
    assert all(call["params"]["sortBy"] == "submittedDate" for call in http.calls)
    assert all(call["params"]["sortOrder"] == "ascending" for call in http.calls)
    assert all(call["timeout"] == 120.0 for call in http.calls)
    assert all(call["headers"] is None for call in http.calls)
    assert result["interpretation"]["classification"] == "REQUEST_SPECIFIC_FAILURE_SUPPORTED"
    assert result["production_eligible"] is False
    assert result["production_records_created"] is False
    assert result["prisma_counted"] is False
    assert (root / DIAGNOSTIC_RELATIVE_ROOT / "diagnostic_manifest.json").is_file()
