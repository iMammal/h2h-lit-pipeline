"""Atomic retrieval checkpoint and raw-response persistence."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h2h_lit.http import HttpResponse
from h2h_lit.pagination import redact_url


@dataclass(slots=True)
class StoredResponse:
    status_code: int
    headers: dict[str, str]
    content: bytes
    url: str
    request_url: str

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.content.decode("utf-8"))

    def iter_content(self, chunk_size: int = 8192):
        for index in range(0, len(self.content), chunk_size):
            yield self.content[index : index + chunk_size]


class CheckpointStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.responses = self.root / "responses"
        self.dataset_path = self.root / "review_dataset.json"
        self.responses.mkdir(parents=True, exist_ok=True)

    def save_dataset(self, dataset: Any) -> str:
        content = dataset.to_json() + "\n"
        atomic_write(self.dataset_path, content.encode("utf-8"))
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def save_response(self, attempt_id: str, response: HttpResponse) -> tuple[str, str]:
        content = bytes(response.content)
        if not content:
            try:
                content = json.dumps(
                    response.json(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ).encode("utf-8")
            except Exception:  # noqa: BLE001 - an empty non-JSON response remains empty
                content = b""
        payload = {
            "status_code": response.status_code,
            "headers": _sanitized_headers(response.headers or {}),
            "content_base64": base64.b64encode(content).decode("ascii"),
            "url": redact_url(response.url),
            "request_url": redact_url(getattr(response, "request_url", response.url)),
        }
        encoded = (
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        filename = hashlib.sha256(attempt_id.encode("utf-8")).hexdigest() + ".json"
        path = self.responses / filename
        atomic_write(path, encoded)
        return str(path.relative_to(self.root)), digest

    def load_response(self, relative_path: str, expected_hash: str) -> StoredResponse:
        path = self.root / relative_path
        encoded = path.read_bytes()
        if hashlib.sha256(encoded).hexdigest() != expected_hash:
            raise ValueError(f"checkpoint response hash mismatch: {relative_path}")
        payload = json.loads(encoded)
        return StoredResponse(
            status_code=int(payload["status_code"]),
            headers=dict(payload.get("headers", {})),
            content=base64.b64decode(payload.get("content_base64", "")),
            url=payload.get("url", ""),
            request_url=payload.get("request_url", payload.get("url", "")),
        )


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _sanitized_headers(headers: dict[str, Any]) -> dict[str, str]:
    sensitive = {"authorization", "proxy-authorization", "set-cookie", "cookie"}
    return {
        str(key): "<redacted>" if str(key).lower() in sensitive else str(value)
        for key, value in headers.items()
    }
