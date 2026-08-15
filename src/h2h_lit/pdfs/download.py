"""PDF transfer and validation."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin

from h2h_lit.http import HttpClient, HttpResponse
from h2h_lit.models import ProcessingStatus, ProvenanceEvent, ProvenanceKind
from h2h_lit.normalize import sanitize_filename
from h2h_lit.pdfs.discovery import PdfCandidate


@dataclass(slots=True)
class PdfTransferResult:
    status: ProcessingStatus
    output_path: Path | None = None
    chosen_url: str | None = None
    events: list[ProvenanceEvent] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def looks_like_pdf_response(response: HttpResponse, content: bytes) -> bool:
    content_type = (response.headers.get("content-type") or "").lower()
    return "application/pdf" in content_type or content.startswith(b"%PDF-")


def extract_pdf_url_from_html(html: str, base_url: str) -> str | None:
    if not html:
        return None
    match = re.search(r'href=[\'"]([^\'"]+\.pdf(?:\?[^\'"]*)?)[\'"]', html, flags=re.IGNORECASE)
    if not match:
        match = re.search(r'href=[\'"]([^\'"]+(?:pdf|download)[^\'"]*)[\'"]', html, flags=re.IGNORECASE)
    if not match:
        return None
    href = match.group(1).strip()
    if href.startswith(("http://", "https://")):
        return href
    if href.startswith("//"):
        return "https:" + href
    return urljoin(base_url, href)


def fetch_pdf_bytes(url: str, *, http: HttpClient, timeout: int = 45) -> tuple[bytes | None, ProvenanceEvent]:
    event = ProvenanceEvent(kind=ProvenanceKind.DETERMINISTIC, stage="pdf_fetch", source_url=url)
    try:
        response = http.get(url, stream=True, timeout=timeout, allow_redirects=True)
        content = response.content
        event.metadata.update(
            {
                "http_status": response.status_code,
                "content_type": response.headers.get("content-type", ""),
                "final_url": response.url,
                "bytes": len(content),
            }
        )
        if response.status_code != 200:
            event.status = ProcessingStatus.FAILED
            event.errors.append(f"HTTP {response.status_code}")
            return None, event
        if looks_like_pdf_response(response, content) and content.startswith(b"%PDF-"):
            return content, event
        if looks_like_pdf_response(response, content) and not content.startswith(b"%PDF-"):
            event.status = ProcessingStatus.FAILED
            event.errors.append("false PDF response: missing %PDF- magic")
            return None, event

        content_type = (response.headers.get("content-type") or "").lower()
        if "text/html" in content_type or content.lstrip().lower().startswith((b"<!doctype html", b"<html")):
            html = content.decode("utf-8", errors="ignore")
            pdf_url = extract_pdf_url_from_html(html, response.url or url)
            if pdf_url and pdf_url != url:
                nested_content, nested_event = fetch_pdf_bytes(pdf_url, http=http, timeout=timeout)
                nested_event.input_id = url
                event.output_id = pdf_url
                if nested_content:
                    return nested_content, event
                event.errors.extend(nested_event.errors)
        event.status = ProcessingStatus.FAILED
        event.errors.append("response was not a valid PDF")
        return None, event
    except Exception as exc:
        event.status = ProcessingStatus.FAILED
        event.errors.append(str(exc))
        return None, event


def download_first_pdf(
    candidates: list[PdfCandidate],
    *,
    output_dir: str | Path,
    filename_base: str,
    http: HttpClient,
) -> PdfTransferResult:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    out_path = output_root / f"{sanitize_filename(filename_base)}.pdf"
    result = PdfTransferResult(status=ProcessingStatus.FAILED, output_path=out_path)
    for candidate in candidates:
        content, event = fetch_pdf_bytes(candidate.url, http=http)
        event.metadata.update({"candidate_source": candidate.source, "candidate_reason": candidate.reason})
        result.events.append(event)
        if content:
            out_path.write_bytes(content)
            result.status = ProcessingStatus.OK
            result.chosen_url = candidate.url
            return result
        result.errors.extend(event.errors)
    if not candidates:
        result.errors.append("no candidates")
    return result

