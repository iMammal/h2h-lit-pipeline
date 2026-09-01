"""Injected and live HTTP transports used by retrieval adapters."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
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
        timeout: float | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
    ) -> HttpResponse: ...


@dataclass(slots=True)
class RequestsHttpResponse:
    """Small stable response wrapper that also exposes the actual prepared URL."""

    _response: Any

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def headers(self) -> dict[str, str]:
        return dict(self._response.headers)

    @property
    def content(self) -> bytes:
        return self._response.content

    @property
    def text(self) -> str:
        return self._response.text

    @property
    def url(self) -> str:
        return self._response.url

    @property
    def request_url(self) -> str:
        return self._response.request.url

    def json(self) -> Any:
        return self._response.json()

    def iter_content(self, chunk_size: int = 8192) -> Iterator[bytes]:
        return self._response.iter_content(chunk_size=chunk_size)


class RequestsHttpClient:
    """Live requests transport; construction performs no network activity."""

    is_live_transport = True

    def __init__(self, session: Any | None = None):
        if session is None:
            try:
                import requests
            except ImportError as exc:
                raise RuntimeError(
                    "live HTTP transport requires the project's requests dependency"
                ) from exc
            session = requests.Session()
        self.session = session

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
    ) -> RequestsHttpResponse:
        response = self.session.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
            stream=stream,
            allow_redirects=allow_redirects,
        )
        return RequestsHttpResponse(response)
