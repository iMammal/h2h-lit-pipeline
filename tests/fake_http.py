from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode


@dataclass
class FakeResponse:
    status_code: int = 200
    headers: dict[str, str] | None = None
    content: bytes = b""
    payload: Any = None
    text: str = ""
    url: str = "https://example.test/response"
    request_url: str | None = None

    def __post_init__(self):
        self.headers = self.headers or {}
        if not self.text and self.content:
            self.text = self.content.decode("utf-8", errors="ignore")

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload

    def iter_content(self, chunk_size: int = 8192):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._request("POST", url, **kwargs)

    def _request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"No fake response queued for {url}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if response.request_url is None:
            params = kwargs.get("params") or kwargs.get("data") or {}
            response.request_url = f"{url}?{urlencode(params, doseq=True)}" if params else url
        return response
