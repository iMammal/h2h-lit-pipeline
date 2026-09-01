from __future__ import annotations

from h2h_lit.http import RequestsHttpClient
from h2h_lit.pagination import PageRequest, RateLimiter, RetryPolicy


def test_page_request_hash_is_deterministic_and_redacts_credentials():
    first = PageRequest(
        "GET",
        "https://example.test/search",
        params={"query": "cells", "cursor": "*"},
        headers={"x-api-key": "secret", "Accept": "application/json"},
        state={"cursor": "*"},
    )
    second = PageRequest(
        "GET",
        "https://example.test/search",
        params={"cursor": "*", "query": "cells"},
        headers={"Accept": "application/json", "x-api-key": "different-secret"},
        state={"cursor": "*"},
    )

    assert first.sanitized_headers()["x-api-key"] == "<redacted>"
    assert first.request_hash() == second.request_hash()


def test_rate_limiter_and_retry_after_are_deterministic():
    times = iter([0.0, 0.25, 1.0])
    sleeps: list[float] = []
    limiter = RateLimiter({"source": 1.0}, clock=lambda: next(times), sleep=sleeps.append)

    assert limiter.wait("source") == 0.0
    assert limiter.wait("source") == 0.75
    assert sleeps == [0.75]
    assert RetryPolicy(base_delay_seconds=1, maximum_delay_seconds=10).delay(2, "5") == 5


class SessionResponse:
    status_code = 200
    content = b'{"ok":true}'
    text = '{"ok":true}'
    url = "https://example.test/final"
    request = type("Request", (), {"url": "https://example.test/search?q=cells"})()

    def __init__(self):
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return {"ok": True}

    def iter_content(self, chunk_size=8192):
        yield self.content


class Session:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return SessionResponse()


def test_live_transport_wraps_actual_prepared_request_without_calling_network():
    session = Session()
    client = RequestsHttpClient(session=session)

    response = client.get("https://example.test/search", params={"q": "cells"}, timeout=30.0)

    assert session.calls[0][1]["params"] == {"q": "cells"}
    assert response.request_url == "https://example.test/search?q=cells"
    assert response.json() == {"ok": True}
