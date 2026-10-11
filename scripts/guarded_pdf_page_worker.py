#!/usr/bin/env python3
"""Extract text from only explicitly requested PDF pages.

This worker is intentionally small. It is launched and monitored by
``guarded_pdf_extract.py`` and must never enumerate a document to discover pages.
Page numbers are one-based.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pypdf import PdfReader


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_pages(raw: str) -> list[int]:
    pages = [int(value.strip()) for value in raw.split(",") if value.strip()]
    if not pages or any(page < 1 for page in pages):
        raise argparse.ArgumentTypeError("pages must be positive one-based integers")
    if len(pages) != len(set(pages)):
        raise argparse.ArgumentTypeError("duplicate pages are not allowed")
    if len(pages) > 2:
        raise argparse.ArgumentTypeError("a guarded batch may contain at most two pages")
    return pages


def append_event(path: Path, event: str, **fields: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"timestamp_utc": utc_now(), "event": event, **fields}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--pages", required=True, type=parse_pages)
    parser.add_argument("--max-chars-per-page", type=int, default=200_000)
    parser.add_argument(
        "--test-sleep-seconds",
        type=float,
        default=0.0,
        help=argparse.SUPPRESS,
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    append_event(
        args.events,
        "worker_started",
        pid=os.getpid(),
        requested_pages=args.pages,
        source=str(args.pdf),
    )
    if args.test_sleep_seconds > 0:
        append_event(args.events, "test_sleep_started", seconds=args.test_sleep_seconds)
        time.sleep(args.test_sleep_seconds)

    extracted: list[dict[str, Any]] = []
    try:
        # Passing an already-open binary handle avoids PdfReader's Path branch, which
        # copies the entire PDF into a BytesIO. Index only requested pages. Do not call
        # len(reader.pages), enumerate(reader.pages), or scan the document for terms.
        with args.pdf.open("rb") as source_handle:
            reader = PdfReader(source_handle)
            for page_number in args.pages:
                append_event(args.events, "extract_started", pdf_page=page_number)
                text = reader.pages[page_number - 1].extract_text() or ""
                text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
                truncated = len(text) > args.max_chars_per_page
                retained_text = text[: args.max_chars_per_page]
                extracted.append(
                    {
                        "pdf_page": page_number,
                        "text": retained_text,
                        "text_characters": len(text),
                        "text_sha256": text_hash,
                        "truncated": truncated,
                    }
                )
                append_event(
                    args.events,
                    "extract_completed",
                    pdf_page=page_number,
                    text_characters=len(text),
                    text_sha256=text_hash,
                    truncated=truncated,
                )
            reader.close()
    except BaseException as exc:
        append_event(
            args.events,
            "worker_failed",
            error_type=type(exc).__name__,
            error_message=str(exc)[:1000],
        )
        raise

    payload = {
        "schema_version": "guarded-pdf-page-extraction-v1",
        "source": str(args.pdf),
        "requested_pages": args.pages,
        "extracted_pages": extracted,
        "completed_utc": utc_now(),
    }
    atomic_write_json(args.output, payload)
    append_event(args.events, "worker_completed", extracted_pages=args.pages)
    print(json.dumps({"status": "complete", "pages": args.pages, "output": str(args.output)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
