"""Normalization helpers recovered from historical notebook behavior."""

from __future__ import annotations

import re
import unicodedata

DOI_RE = re.compile(r"(?<!\w)(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)


def normalize_doi(doi: str | None) -> str | None:
    """Normalize a DOI string or DOI-bearing URL.

    Based on the historical downloader `_norm_doi` behavior: strip common DOI
    prefixes/URLs, locate the first DOI-shaped token, trim punctuation, and lower-case.
    """

    if not doi:
        return None
    value = str(doi).strip()
    value = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^doi\s*:\s*", "", value, flags=re.IGNORECASE)
    value = value.strip().strip(".").strip()
    match = DOI_RE.search(value)
    if not match:
        return None
    return match.group(1).rstrip(".,;").lower()


def normalize_title(title: str | None) -> str:
    """Normalize a title for deterministic comparison."""

    if not title:
        return ""
    value = unicodedata.normalize("NFKC", str(title))
    value = value.replace("{", "").replace("}", "")
    value = re.sub(r"\s+", " ", value).strip().lower()
    value = re.sub(r"[^\w\s]+", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def dedupe_key(*, doi: str | None = None, title: str | None = None) -> str:
    """Return the historical DOI-first dedupe key, falling back to normalized title."""

    normalized_doi = normalize_doi(doi)
    if normalized_doi:
        return f"doi:{normalized_doi}"
    normalized = normalize_title(title)
    return f"title:{normalized}" if normalized else ""


def sanitize_filename(value: str | None, max_len: int = 180) -> str:
    """Sanitize text for filesystem paths, preserving historical truncation behavior."""

    if not value:
        return "untitled"
    cleaned = re.sub(r'[\\/*?:"<>|]', "", str(value))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len] or "untitled"
