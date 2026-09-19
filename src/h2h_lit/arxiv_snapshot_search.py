"""Provisional, offline search of a pinned arXiv metadata snapshot.

This workflow is deliberately separate from production retrieval. It evaluates the
five frozen arXiv query expressions with an explicit local matching policy and does
not claim equivalence with arXiv's API index or contribute records to PRISMA counts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unicodedata
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, TypeAlias

SNAPSHOT_VERSION = 303
SNAPSHOT_PUBLICATION_DATE = "2026-09-12"
SNAPSHOT_SOURCE_URL = (
    "https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/303"
)
EXPECTED_ARCHIVE_SIZE = 1_831_379_597
EXPECTED_ARCHIVE_SHA256 = "c567b1caaf52686299c7b92ddf96eb93492808dc4d15031b8bd3292d772179a5"
EXPECTED_SNAPSHOT_SIZE = 5_527_749_370
EXPECTED_ARCHIVE_MEMBER = "arxiv-metadata-oai-snapshot.json"
EXPECTED_PLAN_HASH = "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
MATCHES_FILENAME = "matches.jsonl"
MANIFEST_FILENAME = "manifest.json"

EXPECTED_ARXIV_QUERY_HASHES = {
    "production:STAR-QF01-RELATIONAL-VIS:arXiv": (
        "3a6d0977b01eee38d41100e1f33919944617bd2e43e2d567317afcc4c4d56166"
    ),
    "production:STAR-QF02-ASSISTED-VIS:arXiv": (
        "a7b4c935d569ed9446eb099137891e1ddb26ebf25a944aa65eea6794b460fad7"
    ),
    "production:STAR-QF03-INTERACTIVE-SYSTEMS:arXiv": (
        "27bab7ca13fd8a4ad6c65485e29b66cd84c6db5386ddf7b1cfa27ea84a22f2a4"
    ),
    "production:STAR-QF04-NONDESKTOP-ENV:arXiv": (
        "95267a35ac7b7ac298685e4681b44d9c643e9995c0068a3abb4c5ef6603174d4"
    ),
    "production:STAR-QF05-CONVERSATIONAL:arXiv": (
        "c75245326a80f076b9ad77763bf66982f9e3d0f365175c5ed56444ba4bfbb312"
    ),
}

SEARCHED_SNAPSHOT_FIELDS = (
    "id",
    "title",
    "authors",
    "abstract",
    "comments",
    "journal-ref",
    "categories",
    "report-no",
)

MATCHING_POLICY: dict[str, Any] = {
    "policy_id": "arxiv-snapshot-local-all-fields-v1",
    "api_equivalence": "NOT_CLAIMED",
    "all_field_mapping": list(SEARCHED_SNAPSHOT_FIELDS),
    "boolean_operators": ["AND", "OR"],
    "boolean_precedence": ["parentheses", "AND", "OR"],
    "case": "unicode_casefold",
    "unicode_normalization": "NFKC",
    "tokenization": "maximal_unicode_alphanumeric_sequences",
    "punctuation": "token_boundary",
    "hyphens": "token_boundary",
    "unquoted_terms": "whole_normalized_token",
    "quoted_phrases": "contiguous_normalized_tokens_within_one_field",
    "cross_field_boolean_matches": True,
    "phrase_crosses_fields": False,
    "stemming": False,
    "wildcards": False,
    "fuzzy_matching": False,
    "synonyms": False,
    "markup_interpretation": False,
    "unsupported_syntax": "FAIL_CLOSED",
    "result_order": "snapshot_source_order",
}


class ArxivSnapshotSearchError(RuntimeError):
    """The isolated snapshot workflow cannot satisfy its provenance contract."""


@dataclass(frozen=True, slots=True)
class SnapshotQuery:
    query_id: str
    family_id: str
    variant_id: str
    query_text: str
    query_text_sha256: str
    request_specification_hash: str
    expression: Expression


@dataclass(frozen=True, slots=True)
class Term:
    tokens: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BooleanNode:
    operator: str
    children: tuple[Expression, ...]


Expression: TypeAlias = Term | BooleanNode
Token: TypeAlias = tuple[str, str | tuple[str, ...]]


@dataclass(slots=True)
class SearchDocument:
    fields: tuple[str, ...]
    token_set: frozenset[str]

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> SearchDocument:
        normalized_fields: list[str] = []
        tokens: set[str] = set()
        for field_name in SEARCHED_SNAPSHOT_FIELDS:
            for value in _scalar_texts(record.get(field_name)):
                field_tokens = normalize_tokens(value)
                if not field_tokens:
                    continue
                tokens.update(field_tokens)
                normalized_fields.append(f" {' '.join(field_tokens)} ")
        return cls(tuple(normalized_fields), frozenset(tokens))

    def matches(self, term: Term) -> bool:
        if len(term.tokens) == 1:
            return term.tokens[0] in self.token_set
        phrase = f" {' '.join(term.tokens)} "
        return any(phrase in field for field in self.fields)


@dataclass(slots=True)
class ScanStats:
    record_count: int
    union_match_count: int
    per_query_counts: dict[str, int]
    source_sha256: str
    source_bytes: int
    matches_sha256: str
    matches_bytes: int
    observed_date_ranges: dict[str, dict[str, str | None]]
    null_or_missing_field_counts: dict[str, int]
    invalid_version_date_count: int


class _QueryParser:
    def __init__(self, query_text: str):
        if not query_text.startswith("all:"):
            raise ArxivSnapshotSearchError("snapshot query must use the all: field prefix")
        self.tokens = _tokenize_query(query_text[4:])
        self.position = 0

    def parse(self) -> Expression:
        if not self.tokens:
            raise ArxivSnapshotSearchError("empty snapshot query")
        expression = self._parse_or()
        if self.position != len(self.tokens):
            raise ArxivSnapshotSearchError("unexpected trailing query syntax")
        return expression

    def _parse_or(self) -> Expression:
        children = [self._parse_and()]
        while self._accept("operator", "OR"):
            children.append(self._parse_and())
        return _combine("OR", children)

    def _parse_and(self) -> Expression:
        children = [self._parse_primary()]
        while self._accept("operator", "AND"):
            children.append(self._parse_primary())
        return _combine("AND", children)

    def _parse_primary(self) -> Expression:
        if self._accept("punctuation", "("):
            expression = self._parse_or()
            if not self._accept("punctuation", ")"):
                raise ArxivSnapshotSearchError("unclosed query parenthesis")
            return expression
        if self.position >= len(self.tokens):
            raise ArxivSnapshotSearchError("query ended where a term was required")
        token_type, token_value = self.tokens[self.position]
        if token_type != "term" or not isinstance(token_value, tuple):
            raise ArxivSnapshotSearchError("query operator appeared where a term was required")
        self.position += 1
        return Term(token_value)

    def _accept(self, token_type: str, value: str) -> bool:
        if self.position >= len(self.tokens):
            return False
        actual_type, actual_value = self.tokens[self.position]
        if actual_type == token_type and actual_value == value:
            self.position += 1
            return True
        return False


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _canonical_json(payload: Any) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _pretty_json(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode(
        "utf-8"
    )


def matching_policy_hash() -> str:
    return hashlib.sha256(_canonical_json(MATCHING_POLICY)).hexdigest()


def normalize_tokens(value: str) -> tuple[str, ...]:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    separated = "".join(character if character.isalnum() else " " for character in normalized)
    return tuple(separated.split())


def parse_query(query_text: str) -> Expression:
    return _QueryParser(query_text).parse()


def expression_matches(
    expression: Expression,
    document: SearchDocument,
    memo: dict[Term, bool] | None = None,
) -> bool:
    if isinstance(expression, Term):
        if memo is None:
            return document.matches(expression)
        if expression not in memo:
            memo[expression] = document.matches(expression)
        return memo[expression]
    if expression.operator == "AND":
        return all(expression_matches(child, document, memo) for child in expression.children)
    if expression.operator == "OR":
        return any(expression_matches(child, document, memo) for child in expression.children)
    raise ArxivSnapshotSearchError(f"unsupported Boolean operator: {expression.operator}")


def load_frozen_arxiv_queries(
    *, root: str | Path, plan_path: str | Path
) -> tuple[str, list[SnapshotQuery]]:
    root_path = Path(root).resolve()
    candidate = Path(plan_path)
    if not candidate.is_absolute():
        candidate = root_path / candidate
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArxivSnapshotSearchError("unable to load the frozen production plan") from exc
    if not isinstance(payload, dict):
        raise ArxivSnapshotSearchError("frozen production plan must be a JSON object")
    material = dict(payload)
    recorded_plan_hash = material.pop("plan_hash", None)
    calculated_plan_hash = hashlib.sha256(_canonical_json(material)).hexdigest()
    if recorded_plan_hash != calculated_plan_hash or calculated_plan_hash != EXPECTED_PLAN_HASH:
        raise ArxivSnapshotSearchError("frozen production plan hash changed")

    source_queries = [item for item in payload.get("source_queries", []) if item.get("source") == "arXiv"]
    if len(source_queries) != len(EXPECTED_ARXIV_QUERY_HASHES):
        raise ArxivSnapshotSearchError("frozen arXiv query count changed")
    actual_hashes = {item.get("query_id"): item.get("query_text_sha256") for item in source_queries}
    if actual_hashes != EXPECTED_ARXIV_QUERY_HASHES:
        raise ArxivSnapshotSearchError("frozen arXiv query identities or hashes changed")
    if any(item.get("field_restrictions") != ["all"] for item in source_queries):
        raise ArxivSnapshotSearchError("frozen arXiv field restrictions changed")
    for item in source_queries:
        if hashlib.sha256(item["query_text"].encode("utf-8")).hexdigest() != item[
            "query_text_sha256"
        ]:
            raise ArxivSnapshotSearchError("frozen arXiv query text hash mismatch")
        if hashlib.sha256(_canonical_json(item["request_specification"])).hexdigest() != item[
            "request_specification_hash"
        ]:
            raise ArxivSnapshotSearchError("frozen arXiv request specification hash mismatch")

    queries = [
        SnapshotQuery(
            query_id=item["query_id"],
            family_id=item["family_id"],
            variant_id=item["variant_id"],
            query_text=item["query_text"],
            query_text_sha256=item["query_text_sha256"],
            request_specification_hash=item["request_specification_hash"],
            expression=parse_query(item["query_text"]),
        )
        for item in source_queries
    ]
    return calculated_plan_hash, queries


def scan_snapshot(
    source: BinaryIO,
    matches: BinaryIO,
    queries: Sequence[SnapshotQuery],
) -> ScanStats:
    source_digest = hashlib.sha256()
    matches_digest = hashlib.sha256()
    source_bytes = 0
    matches_bytes = 0
    record_count = 0
    union_match_count = 0
    per_query_counts = {query.query_id: 0 for query in queries}
    missing_counts = {field_name: 0 for field_name in SEARCHED_SNAPSHOT_FIELDS}
    date_values: dict[str, list[str | None]] = {
        "update_date": [None, None],
        "first_version_created": [None, None],
        "all_versions_created": [None, None],
    }
    invalid_version_dates = 0

    for line_number, raw_line in enumerate(source, 1):
        source_digest.update(raw_line)
        source_bytes += len(raw_line)
        if not raw_line.strip():
            raise ArxivSnapshotSearchError(f"blank JSONL record at line {line_number}")
        try:
            record = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArxivSnapshotSearchError(
                f"invalid JSONL record at line {line_number}: {exc}"
            ) from exc
        if not isinstance(record, dict):
            raise ArxivSnapshotSearchError(f"non-object JSONL record at line {line_number}")
        _validate_record(record, line_number)
        record_count += 1

        for field_name in SEARCHED_SNAPSHOT_FIELDS:
            if record.get(field_name) in (None, "", []):
                missing_counts[field_name] += 1
        _observe_date(date_values["update_date"], record.get("update_date"))
        versions = record.get("versions", [])
        parsed_version_dates = [_parsed_version_date(version.get("created")) for version in versions]
        invalid_version_dates += sum(created is None for created in parsed_version_dates)
        if parsed_version_dates:
            first_created = parsed_version_dates[0]
            if first_created is None:
                pass
            else:
                _observe_date(date_values["first_version_created"], first_created)
        for created in parsed_version_dates:
            if created is not None:
                _observe_date(date_values["all_versions_created"], created)

        document = SearchDocument.from_record(record)
        match_memo: dict[Term, bool] = {}
        matching_query_ids = [
            query.query_id
            for query in queries
            if expression_matches(query.expression, document, match_memo)
        ]
        if not matching_query_ids:
            continue
        union_match_count += 1
        for query_id in matching_query_ids:
            per_query_counts[query_id] += 1
        envelope = {
            "arxiv_id": record["id"],
            "matching_query_ids": matching_query_ids,
            "source_record": record,
            "source_record_provenance": {
                "line_number": line_number,
                "raw_line_sha256": hashlib.sha256(raw_line).hexdigest(),
                "snapshot_version": SNAPSHOT_VERSION,
            },
        }
        encoded = _canonical_json(envelope) + b"\n"
        matches.write(encoded)
        matches_digest.update(encoded)
        matches_bytes += len(encoded)

    return ScanStats(
        record_count=record_count,
        union_match_count=union_match_count,
        per_query_counts=per_query_counts,
        source_sha256=source_digest.hexdigest(),
        source_bytes=source_bytes,
        matches_sha256=matches_digest.hexdigest(),
        matches_bytes=matches_bytes,
        observed_date_ranges={
            name: {"minimum": values[0], "maximum": values[1]}
            for name, values in date_values.items()
        },
        null_or_missing_field_counts=missing_counts,
        invalid_version_date_count=invalid_version_dates,
    )


def run_snapshot_search(
    *,
    root: str | Path,
    plan_path: str | Path,
    archive_path: str | Path,
    snapshot_path: str | Path,
    output_dir: str | Path,
    code_commit: str | None = None,
    timestamp: Callable[[], str] = utc_now,
    expected_archive_size: int = EXPECTED_ARCHIVE_SIZE,
    expected_archive_sha256: str = EXPECTED_ARCHIVE_SHA256,
    expected_snapshot_size: int = EXPECTED_SNAPSHOT_SIZE,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    archive = Path(archive_path).resolve()
    snapshot = Path(snapshot_path).resolve()
    output = Path(output_dir).resolve()
    _validate_output_location(root_path, output, archive, snapshot)
    _validate_regular_file(archive, expected_archive_size, "snapshot archive")
    _validate_regular_file(snapshot, expected_snapshot_size, "snapshot JSONL")
    _validate_archive(archive, expected_snapshot_size)
    if code_commit is None:
        code_commit = _clean_git_commit(root_path)

    plan_hash, queries = load_frozen_arxiv_queries(root=root_path, plan_path=plan_path)
    archive_before = _stat_identity(archive)
    snapshot_before = _stat_identity(snapshot)
    archive_hash = _sha256_file(archive)
    if archive_hash != expected_archive_sha256:
        raise ArxivSnapshotSearchError("snapshot archive SHA-256 mismatch")
    started_at = timestamp()

    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.mkdir()
    except FileExistsError as exc:
        raise ArxivSnapshotSearchError(f"output namespace already exists: {output}") from exc

    temporary_matches = output / f".{MATCHES_FILENAME}.tmp"
    final_matches = output / MATCHES_FILENAME
    try:
        with snapshot.open("rb") as source, temporary_matches.open("xb") as matches:
            scan = scan_snapshot(source, matches, queries)
            matches.flush()
            os.fsync(matches.fileno())
        if scan.source_bytes != expected_snapshot_size:
            raise ArxivSnapshotSearchError("snapshot byte count changed during scan")
        if _stat_identity(snapshot) != snapshot_before:
            raise ArxivSnapshotSearchError("snapshot JSONL changed during scan")
        if _stat_identity(archive) != archive_before:
            raise ArxivSnapshotSearchError("snapshot archive changed during scan")
        temporary_matches.replace(final_matches)

        completed_at = timestamp()
        manifest: dict[str, Any] = {
            "schema_version": "1.0.0",
            "purpose": "provisional_arxiv_metadata_snapshot_search",
            "status": "COMPLETE",
            "production_eligible": False,
            "prisma_counted": False,
            "api_equivalence": "NOT_CLAIMED",
            "provider_completeness": "UNPROVEN",
            "snapshot_scan_complete": True,
            "started_at_utc": started_at,
            "completed_at_utc": completed_at,
            "code_commit": code_commit,
            "snapshot": {
                "dataset": "Cornell-University/arxiv",
                "version": SNAPSHOT_VERSION,
                "source_url": SNAPSHOT_SOURCE_URL,
                "publication_date": SNAPSHOT_PUBLICATION_DATE,
                "publication_date_is_coverage_evidence": False,
                "archive": {
                    "path": str(archive),
                    "byte_size": archive_before[2],
                    "raw_sha256": archive_hash,
                    "member": EXPECTED_ARCHIVE_MEMBER,
                },
                "jsonl": {
                    "path": str(snapshot),
                    "byte_size": scan.source_bytes,
                    "raw_sha256": scan.source_sha256,
                },
            },
            "frozen_query_plan": {
                "path": str(Path(plan_path)),
                "plan_hash": plan_hash,
                "queries": [
                    {
                        "query_id": query.query_id,
                        "family_id": query.family_id,
                        "variant_id": query.variant_id,
                        "query_text": query.query_text,
                        "query_text_sha256": query.query_text_sha256,
                        "request_specification_hash": query.request_specification_hash,
                    }
                    for query in queries
                ],
            },
            "matching_policy": MATCHING_POLICY,
            "matching_policy_sha256": matching_policy_hash(),
            "result_order": {
                "stored_order": "snapshot_source_order",
                "frozen_api_sort_by": "submittedDate",
                "frozen_api_sort_order": "ascending",
                "api_sort_reproduced": False,
            },
            "counts": {
                "source_records": scan.record_count,
                "union_matches": scan.union_match_count,
                "per_query_matches": scan.per_query_counts,
                "null_or_missing_search_fields": scan.null_or_missing_field_counts,
                "invalid_version_dates": scan.invalid_version_date_count,
            },
            "observed_date_ranges": scan.observed_date_ranges,
            "results": {
                "path": MATCHES_FILENAME,
                "byte_size": scan.matches_bytes,
                "raw_sha256": scan.matches_sha256,
            },
        }
        _atomic_write(output / MANIFEST_FILENAME, _pretty_json(manifest))
        return manifest
    except BaseException:
        shutil.rmtree(output, ignore_errors=True)
        raise


def _tokenize_query(expression: str) -> list[Token]:
    tokens: list[Token] = []
    position = 0
    while position < len(expression):
        character = expression[position]
        if character.isspace():
            position += 1
            continue
        if character in "()":
            tokens.append(("punctuation", character))
            position += 1
            continue
        if character == '"':
            closing = expression.find('"', position + 1)
            if closing < 0:
                raise ArxivSnapshotSearchError("unclosed quoted phrase")
            raw_value = expression[position + 1 : closing]
            normalized = normalize_tokens(raw_value)
            if not normalized:
                raise ArxivSnapshotSearchError("quoted phrase has no searchable tokens")
            tokens.append(("term", normalized))
            position = closing + 1
            continue
        end = position
        while end < len(expression) and not expression[end].isspace() and expression[end] not in "()":
            end += 1
        raw_value = expression[position:end]
        if raw_value in {"AND", "OR"}:
            tokens.append(("operator", raw_value))
        elif raw_value == "ANDNOT":
            raise ArxivSnapshotSearchError("ANDNOT is outside the approved matching policy")
        else:
            if any(marker in raw_value for marker in (":", "*", "?")):
                raise ArxivSnapshotSearchError(
                    f"unsupported snapshot query token: {raw_value}"
                )
            normalized = normalize_tokens(raw_value)
            if len(normalized) != 1:
                raise ArxivSnapshotSearchError(
                    f"unquoted snapshot term must normalize to one token: {raw_value}"
                )
            tokens.append(("term", normalized))
        position = end
    return tokens


def _combine(operator: str, children: list[Expression]) -> Expression:
    if len(children) == 1:
        return children[0]
    flattened: list[Expression] = []
    for child in children:
        if isinstance(child, BooleanNode) and child.operator == operator:
            flattened.extend(child.children)
        else:
            flattened.append(child)
    return BooleanNode(operator, tuple(flattened))


def _scalar_texts(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _scalar_texts(item)


def _validate_record(record: Mapping[str, Any], line_number: int) -> None:
    if not isinstance(record.get("id"), str) or not record["id"].strip():
        raise ArxivSnapshotSearchError(f"record at line {line_number} lacks an arXiv id")
    scalar_fields = set(SEARCHED_SNAPSHOT_FIELDS) | {
        "submitter",
        "doi",
        "license",
        "update_date",
    }
    for field_name in scalar_fields:
        value = record.get(field_name)
        if value is not None and not isinstance(value, str):
            raise ArxivSnapshotSearchError(
                f"record at line {line_number} has non-string {field_name}"
            )
    versions = record.get("versions")
    if not isinstance(versions, list) or any(not isinstance(item, dict) for item in versions):
        raise ArxivSnapshotSearchError(
            f"record at line {line_number} has invalid versions metadata"
        )
    authors_parsed = record.get("authors_parsed")
    if authors_parsed is not None and not isinstance(authors_parsed, list):
        raise ArxivSnapshotSearchError(
            f"record at line {line_number} has invalid authors_parsed metadata"
        )


def _parsed_version_date(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _observe_date(bounds: list[str | None], value: Any) -> None:
    if not isinstance(value, str) or not value:
        return
    if bounds[0] is None or value < bounds[0]:
        bounds[0] = value
    if bounds[1] is None or value > bounds[1]:
        bounds[1] = value


def _validate_regular_file(path: Path, expected_size: int, label: str) -> None:
    if not path.is_file():
        raise ArxivSnapshotSearchError(f"{label} is missing or not a regular file: {path}")
    actual_size = path.stat().st_size
    if actual_size != expected_size:
        raise ArxivSnapshotSearchError(
            f"{label} size mismatch: expected {expected_size}, found {actual_size}"
        )


def _validate_archive(path: Path, expected_member_size: int) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
    except (OSError, zipfile.BadZipFile) as exc:
        raise ArxivSnapshotSearchError(f"invalid snapshot archive: {path}") from exc
    if len(members) != 1:
        raise ArxivSnapshotSearchError("snapshot archive must contain exactly one member")
    member = members[0]
    member_path = PurePosixPath(member.filename)
    if (
        member.filename != EXPECTED_ARCHIVE_MEMBER
        or member.is_dir()
        or member_path.is_absolute()
        or ".." in member_path.parts
        or "\\" in member.filename
    ):
        raise ArxivSnapshotSearchError("snapshot archive contains an unexpected path")
    if member.file_size != expected_member_size:
        raise ArxivSnapshotSearchError("snapshot archive member size mismatch")


def _validate_output_location(
    root: Path, output: Path, archive: Path, snapshot: Path
) -> None:
    production_root = (root / "outputs/production").resolve()
    if output == production_root or production_root in output.parents:
        raise ArxivSnapshotSearchError("snapshot output cannot be written under outputs/production")
    if output in archive.parents or output in snapshot.parents:
        raise ArxivSnapshotSearchError("snapshot output cannot contain a source artifact")
    if output.exists():
        raise ArxivSnapshotSearchError(f"output namespace already exists: {output}")


def _stat_identity(path: Path) -> tuple[int, int, int, int]:
    status = path.stat()
    return status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, content: bytes) -> None:
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


def _clean_git_commit(root: Path) -> str:
    try:
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ArxivSnapshotSearchError("unable to establish code commit provenance") from exc
    if status.strip():
        raise ArxivSnapshotSearchError("remote execution checkout must be clean")
    return commit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--plan", default="config/star_production_query_plan_v1.json")
    parser.add_argument("--archive", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--snapshot-version", required=True, type=int)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    if args.snapshot_version != SNAPSHOT_VERSION:
        parser.error(f"only snapshot version {SNAPSHOT_VERSION} is authorized")
    if args.source_url != SNAPSHOT_SOURCE_URL:
        parser.error("snapshot source URL does not match the authorized version")

    manifest = run_snapshot_search(
        root=args.root,
        plan_path=args.plan,
        archive_path=args.archive,
        snapshot_path=args.snapshot,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "production_eligible": manifest["production_eligible"],
                "prisma_counted": manifest["prisma_counted"],
                "source_records": manifest["counts"]["source_records"],
                "union_matches": manifest["counts"]["union_matches"],
                "manifest": str(Path(args.output_dir) / MANIFEST_FILENAME),
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
