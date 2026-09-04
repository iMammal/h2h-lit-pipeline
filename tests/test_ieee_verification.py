from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from h2h_lit.ieee_verification import (
    CREDENTIAL_NAME,
    EXPECTED_VERIFICATION_REQUEST_HASHES,
    IeeeVerificationError,
    execute_ieee_verification,
    preflight_ieee_verification,
)
from h2h_lit.pagination import RateLimiter, RetryPolicy

ROOT = Path(__file__).resolve().parents[1]
_PREFLIGHT: dict[str, Any] | None = None


@dataclass
class _Response:
    payload: Any
    status_code: int = 200

    def __post_init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.content = json.dumps(self.payload, sort_keys=True).encode("utf-8")
        self.text = self.content.decode("utf-8")
        self.url = "https://ieeexploreapi.ieee.org/api/v1/search/articles"

    def json(self) -> Any:
        return self.payload


class _Http:
    def __init__(self, responses: list[_Response]):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> _Response:
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


def _payload(total: int) -> dict[str, Any]:
    return {
        "total_records": total,
        "total_searched": 7_000_000,
        "articles": [
            {
                "article_number": str(total + 100),
                "title": f"Result {total}",
            }
        ]
        if total
        else [],
    }


def _execute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    responses: list[_Response],
    *,
    credential: str = "unit-test-secret",
) -> tuple[dict[str, Any], _Http]:
    import h2h_lit.ieee_verification as module

    preflight = _preflight()
    monkeypatch.setattr(module, "preflight_ieee_verification", lambda **_: preflight)
    monkeypatch.setattr(module, "_hard_window_for", lambda *_: None)
    http = _Http(responses)
    manifest = execute_ieee_verification(
        root=tmp_path,
        http=http,
        credential=credential,
        timestamp=lambda: "2026-09-04T12:00:00Z",
        sleep=lambda _: None,
        retry_policy=RetryPolicy(max_attempts=2, base_delay_seconds=0),
        rate_limiter=RateLimiter(minimum_intervals={"IEEEXplore": 0}),
        finalize_prerequisites=False,
    )
    return manifest, http


def _preflight() -> dict[str, Any]:
    global _PREFLIGHT
    if _PREFLIGHT is None:
        _PREFLIGHT = preflight_ieee_verification(root=ROOT)
    return deepcopy(_PREFLIGHT)


def test_preflight_binds_frozen_plan_readiness_and_exact_five_requests() -> None:
    result = _preflight()
    assert result["status"] == "POST_EXECUTION_AUDIT_PASSED_NO_NETWORK"
    assert result["request_count"] == 5
    assert result["maximum_http_attempts"] == 10
    assert result["credential_reference"] == CREDENTIAL_NAME
    assert result["credential_read"] is False
    assert result["network_used"] is False
    assert {
        item["family_id"]: item["request_hash"] for item in result["requests"]
    } == EXPECTED_VERIFICATION_REQUEST_HASHES
    assert all(item["request"]["params"]["max_records"] == 1 for item in result["requests"])


def test_success_executes_five_requests_and_never_persists_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    counts = [11, 22, 33, 44, 55]
    manifest, http = _execute(
        tmp_path, monkeypatch, [_Response(_payload(count)) for count in counts]
    )
    assert manifest["status"] == "VERIFIED_READY_FOR_RETRIEVAL"
    assert manifest["source_window_state"] == "RESOLVED_CLEAR"
    assert [item["provider_count"] for item in manifest["requests"]] == counts
    assert len(http.calls) == 5
    assert all(call["params"]["max_records"] == 1 for call in http.calls)
    assert all(call["params"]["start_record"] == 1 for call in http.calls)
    assert all(call["params"]["apikey"] == "unit-test-secret" for call in http.calls)
    assert "unit-test-secret" not in json.dumps(manifest)
    assert manifest["credential"] == {
        "required_name": CREDENTIAL_NAME,
        "present_at_execution": True,
        "verified": True,
        "value_persisted": False,
    }
    for item in manifest["requests"]:
        assert item["attempts"][-1]["outcome"] == "SUCCEEDED"
        raw = item["attempts"][-1]["response_artifact"]
        path = tmp_path / raw["path"]
        assert path.is_file()
        assert path.stat().st_size == raw["byte_size"]


def test_missing_credential_fails_before_preflight_or_http(tmp_path: Path) -> None:
    with pytest.raises(IeeeVerificationError, match=CREDENTIAL_NAME):
        execute_ieee_verification(root=tmp_path, http=_Http([]), credential="")


@pytest.mark.parametrize("status", [401, 403])
def test_authentication_failure_is_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    with pytest.raises(IeeeVerificationError, match="authentication failed"):
        _execute(tmp_path, monkeypatch, [_Response({"error": "denied"}, status)])
    run = json.loads(
        (tmp_path / "artifacts/ieee_verification/star-ieee-verification-001/run_manifest.json")
        .read_text(encoding="utf-8")
    )
    assert run["status"] == "FAILED_CLOSED"
    assert run["requests"][0]["attempts"][0]["outcome"] == (
        "TERMINAL_AUTHENTICATION_FAILURE"
    )


def test_provider_error_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(IeeeVerificationError, match="provider reported an error"):
        _execute(
            tmp_path,
            monkeypatch,
            [_Response({"error_message": "query rejected", "articles": []})],
        )


@pytest.mark.parametrize(
    "payload",
    [
        ["not", "an", "object"],
        {"total_records": "not-an-integer", "articles": []},
        {"total_records": 4, "articles": "not-a-list"},
        {"articles": []},
    ],
)
def test_malformed_response_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: Any
) -> None:
    with pytest.raises(IeeeVerificationError, match="provider/query validation|parser failure"):
        _execute(tmp_path, monkeypatch, [_Response(payload)])


def test_request_hash_mismatch_fails_before_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import h2h_lit.ieee_verification as module

    preflight = _preflight()
    preflight["requests"][0]["request_hash"] = "0" * 64
    monkeypatch.setattr(module, "preflight_ieee_verification", lambda **_: preflight)
    monkeypatch.setattr(module, "_hard_window_for", lambda *_: None)
    http = _Http([])
    with pytest.raises(IeeeVerificationError, match="request-hash mismatch"):
        execute_ieee_verification(
            root=tmp_path,
            http=http,
            credential="unit-test-secret",
            timestamp=lambda: "2026-09-04T12:00:00Z",
            finalize_prerequisites=False,
        )
    assert http.calls == []


def test_retry_is_bounded_and_accounted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    responses = [_Response({}, 503), _Response(_payload(11))]
    responses.extend(_Response(_payload(value)) for value in [22, 33, 44, 55])
    manifest, http = _execute(tmp_path, monkeypatch, responses)
    attempts = manifest["requests"][0]["attempts"]
    assert [item["outcome"] for item in attempts] == [
        "RETRYABLE_HTTP_FAILURE",
        "SUCCEEDED",
    ]
    assert len(http.calls) == 6


def test_count_window_handling_never_invents_partitioning() -> None:
    from h2h_lit.ieee_verification import _source_window

    assert _source_window(50_000, None) == {
        "state": "RESOLVED_CLEAR",
        "status": "exact_total_no_declared_hard_window",
        "hard_window": None,
        "partitioning_invented": False,
    }
    assert _source_window(1_000, 1_000)["state"] == "RESOLVED_CLEAR"
    overflow = _source_window(1_001, 1_000)
    assert overflow["state"] == "UNRESOLVED_PARTITION_REVIEW"
    assert overflow["partitioning_invented"] is False
