"""HTTP protocol types used by adapters.

Phase 2 keeps external services mocked by requiring callers to inject an HTTP client.
No adapter constructs a live network client by default.
"""

from __future__ import annotations

from typing import Any, Protocol


class HttpResponse(Protocol):
    status_code: int
    headers: dict[str, str]
    content: bytes
    text: str
    url: str

    def json(self) -> Any: ...

    def iter_content(self, chunk_size: int = 8192): ...


class HttpClient(Protocol):
    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | float | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
    ) -> HttpResponse: ...

