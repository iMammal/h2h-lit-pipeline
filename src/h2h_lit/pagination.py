"""Source-independent pagination, retry, and rate-limit contracts."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from h2h_lit.http import HttpResponse
from h2h_lit.models import LiteratureRecord


@dataclass(frozen=True, slots=True)
class PageRequest:
    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    state: dict[str, Any] = field(default_factory=dict)

    def sanitized_headers(self) -> dict[str, str]:
        return {
            key: ("<redacted>" if key.lower() in {"authorization", "x-api-key"} else value)
            for key, value in self.headers.items()
        }

    def sanitized_params(self) -> dict[str, Any]:
        return {
            key: "<redacted>" if _sensitive_key(key) else value
            for key, value in self.params.items()
        }

    def request_hash(self) -> str:
        payload = {
            "method": self.method,
            "url": self.url,
            "params": self.sanitized_params(),
            "headers": self.sanitized_headers(),
            "timeout": self.timeout,
            "state": self.state,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class ParsedPage:
    records: list[LiteratureRecord]
    raw_item_count: int
    next_state: dict[str, Any] | None
    terminal: bool
    completion_proof: str | None = None
    source_reported_total: int | None = None
    total_is_exact: bool = False
    truncated: bool = False
    truncation_reason: str | None = None
    incomplete_reason: str | None = None
    native_identifiers: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class PaginatedSourceAdapter(Protocol):
    source_database: str
    strategy: str
    version: str

    def initial_state(self, spec: Any) -> dict[str, Any]: ...

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest: ...

    def parse_response(
        self,
        spec: Any,
        state: dict[str, Any],
        response: HttpResponse,
    ) -> ParsedPage: ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    maximum_delay_seconds: float = 60.0
    retry_statuses: frozenset[int] = frozenset({408, 429, 500, 502, 503, 504})

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.base_delay_seconds < 0 or self.maximum_delay_seconds < 0:
            raise ValueError("retry delays cannot be negative")

    def delay(self, attempt_number: int, retry_after: str | None = None) -> float:
        calculated = min(
            self.maximum_delay_seconds,
            self.base_delay_seconds * (2 ** max(0, attempt_number - 1)),
        )
        if retry_after is None:
            return calculated
        try:
            return max(calculated, float(retry_after))
        except ValueError:
            return calculated


DEFAULT_MINIMUM_INTERVALS = {
    "PubMed": 1.0 / 3.0,
    "EuropePMC": 1.0,
    "CrossRef": 1.0,
    "SemanticScholar": 1.0,
    "arXiv": 3.0,
}


class RateLimiter:
    """Sequential source limiter with injectable clock/sleeper for offline tests."""

    def __init__(
        self,
        minimum_intervals: dict[str, float] | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.minimum_intervals = dict(
            DEFAULT_MINIMUM_INTERVALS if minimum_intervals is None else minimum_intervals
        )
        self._clock = clock
        self._sleep = sleep
        self._last_request: dict[str, float] = {}

    def wait(self, source_database: str) -> float:
        now = self._clock()
        previous = self._last_request.get(source_database)
        interval = self.minimum_intervals.get(source_database, 0.0)
        delay = max(0.0, interval - (now - previous)) if previous is not None else 0.0
        if delay:
            self._sleep(delay)
            now = self._clock()
        self._last_request[source_database] = now
        return delay


class PaginationError(RuntimeError):
    """A page cannot be parsed or continued reproducibly."""


def redact_url(url: str) -> str:
    if not url:
        return url
    parts = urlsplit(url)
    query = urlencode(
        [
            (key, "<redacted>" if _sensitive_key(key) else value)
            for key, value in parse_qsl(parts.query, keep_blank_values=True)
        ]
    )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def _sensitive_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return normalized in {"api_key", "apikey", "access_token", "token_key"}


def native_identifier(record: LiteratureRecord, fallback_rank: int) -> str:
    return (
        record.source_identifier
        or record.doi
        or record.pmid
        or record.arxiv_id
        or f"missing-source-id:{fallback_rank}"
    )


def malformed_identifier(raw_item: Any, rank: int) -> str:
    encoded = json.dumps(
        raw_item, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    )
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
    return f"malformed-item:{digest}:{rank}"
