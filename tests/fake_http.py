from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class FakeResponse:
    status_code: int = 200
    headers: dict[str, str] | None = None
    content: bytes = b""
    payload: Any = None
    text: str = ""
    url: str = "https://example.test/response"

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
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"No fake response queued for {url}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

