"""Small BibTeX parser/writer compatible with the historical notebooks."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from h2h_lit.models import LiteratureRecord, ProvenanceEvent, ProvenanceKind
from h2h_lit.normalize import normalize_doi

FIELD_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_-]*)\s*=")


def escape_bibtex(text: object) -> str:
    if text is None:
        return ""
    return str(text).replace("{", "\\{").replace("}", "\\}").replace("\n", " ").strip()


def split_bib_entries(text: str) -> list[str]:
    """Split BibTeX text into raw entries using balanced braces.

    Ported from the historical downloader `_split_bib_entries` behavior, but kept
    independent of the downloader.
    """

    entries: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        at = text.find("@", i)
        if at == -1:
            break
        brace = text.find("{", at)
        if brace == -1:
            break
        depth = 1
        j = brace + 1
        while j < n and depth:
            if text[j] == "{":
                depth += 1
            elif text[j] == "}":
                depth -= 1
            j += 1
        if depth == 0:
            entries.append(text[at:j])
            i = j
        else:
            entries.append(text[at:])
            break
    return entries


def parse_note(note: str | None) -> dict[str, object]:
    """Parse the lightweight note metadata used by historical generated BibTeX."""

    out: dict[str, object] = {}
    if not note:
        return out
    for part in re.split(r",\s*", str(note)):
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        key = key.strip().lower().replace(" ", "_")
        value = value.strip()
        if value.lower() == "true":
            out[key] = True
        elif value.lower() == "false":
            out[key] = False
        else:
            out[key] = value
    return out


def _parse_braced_value(body: str, index: int) -> tuple[str, int]:
    depth = 1
    start = index + 1
    i = start
    while i < len(body) and depth:
        if body[i] == "{":
            depth += 1
        elif body[i] == "}":
            depth -= 1
        i += 1
    return body[start : i - 1].strip(), i


def _parse_quoted_value(body: str, index: int) -> tuple[str, int]:
    i = index + 1
    start = i
    while i < len(body):
        if body[i] == '"' and body[i - 1] != "\\":
            break
        i += 1
    return body[start:i].strip(), i + 1


def parse_entry_fields(entry_text: str) -> dict[str, str]:
    """Parse one BibTeX entry into lower-case fields plus `_type` and `_key`."""

    header = re.match(r"@\s*([A-Za-z]+)\s*\{\s*([^,\s]+)\s*,", entry_text, re.DOTALL)
    if not header:
        return {}
    fields: dict[str, str] = {
        "_type": header.group(1).strip(),
        "_key": header.group(2).strip(),
    }
    body = entry_text[header.end() :].rstrip().rstrip("}")
    i = 0
    while i < len(body):
        match = FIELD_RE.search(body, i)
        if not match:
            break
        key = match.group(1).lower()
        i = match.end()
        while i < len(body) and body[i] in " \t\r\n":
            i += 1
        if i >= len(body):
            break
        if body[i] == "{":
            value, i = _parse_braced_value(body, i)
        elif body[i] == '"':
            value, i = _parse_quoted_value(body, i)
        else:
            start = i
            while i < len(body) and body[i] not in ",\n\r}":
                i += 1
            value = body[start:i].strip()
        fields[key] = value
    return fields


def parse_bibtex(text: str) -> list[dict[str, str]]:
    return [fields for raw in split_bib_entries(text) if (fields := parse_entry_fields(raw))]


def record_from_bibtex_fields(fields: dict[str, str]) -> LiteratureRecord:
    note_info = parse_note(fields.get("note"))
    authors = [
        a.strip()
        for a in re.split(r"\s+and\s+", fields.get("author", ""))
        if a.strip() and a.strip().lower() != "unknown"
    ]
    source = str(note_info.get("source") or fields.get("journal") or "").strip() or None
    record = LiteratureRecord(
        title=fields.get("title", "").replace("\\{", "{").replace("\\}", "}"),
        abstract=fields.get("abstract", "").replace("\\{", "{").replace("\\}", "}"),
        authors=authors,
        year=fields.get("year") or None,
        doi=normalize_doi(fields.get("doi")),
        source_database=source,
        source_url=fields.get("url") or None,
        pdf_url=fields.get("pdf_url") or None,
        journal=fields.get("journal") or None,
        is_open_access=note_info.get("openaccess")
        if isinstance(note_info.get("openaccess"), bool)
        else None,
        original_metadata=fields.copy(),
    )
    record.add_event(
        ProvenanceEvent(
            kind=ProvenanceKind.SOURCE_DERIVED,
            stage="bibtex_parse",
            source_database=source,
            source_url=record.source_url,
            source_identifier=fields.get("_key"),
        )
    )
    if "llm_classification" in fields:
        record.annotations["llm_classification"] = fields["llm_classification"]
    return record


def records_from_bibtex(text: str) -> list[LiteratureRecord]:
    return [record_from_bibtex_fields(fields) for fields in parse_bibtex(text)]


def to_bibtex(entries: Iterable[LiteratureRecord | dict[str, object]]) -> str:
    """Write records using the historical H2HLitFetcher field schema."""

    bibs: list[str] = []
    for i, entry in enumerate(entries):
        if isinstance(entry, LiteratureRecord):
            data = entry.to_dict()
            source = entry.source_database or "Unknown"
            oa = entry.is_open_access if entry.is_open_access is not None else False
        else:
            data = dict(entry)
            source = str(data.get("source") or data.get("source_database") or "Unknown")
            oa = bool(data.get("oa", data.get("is_open_access", False)))

        title = escape_bibtex(data.get("title", ""))
        abstract = escape_bibtex(data.get("abstract", ""))
        year = data.get("year") or "n.d."
        doi = normalize_doi(str(data.get("doi") or "")) or ""
        journal = escape_bibtex(data.get("journal") or source or "Unknown")
        authors_value = data.get("authors") or data.get("author") or "Unknown"
        if isinstance(authors_value, list):
            authors = " and ".join(str(a) for a in authors_value) or "Unknown"
        else:
            authors = str(authors_value) or "Unknown"
        url = data.get("url") or data.get("source_url") or ""

        bibs.append(
            f"""@article{{entry{i},
  title   = {{{title}}},
  author  = {{{escape_bibtex(authors)}}},
  journal = {{{journal}}},
  year    = {{{year}}},
  url     = {{{url}}},
  doi     = {{{doi}}},
  note    = {{Source: {source}, OpenAccess: {oa}}},
  abstract= {{{abstract}}}
}}"""
        )
    return "\n\n".join(bibs)


def save_bib(entries: Iterable[LiteratureRecord | dict[str, object]], out_path: str | Path) -> Path:
    path = Path(out_path)
    path.write_text(to_bibtex(entries), encoding="utf-8")
    return path

