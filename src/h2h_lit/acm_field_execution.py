"""Prospective field-decomposed ACM execution and union provenance.

This module is intentionally upstream of review-corpus construction. It validates
human-operated ACM evidence and reconciles field exports without creating review,
screening, PRISMA, E6, or corpus objects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path
from typing import Any

from h2h_lit.bibtex_io import BibtexParseResult, parse_bibtex_with_diagnostics
from h2h_lit.normalize import normalize_doi
from h2h_lit.production_query_plan import load_production_query_plan

SCHEMA_VERSION = "1.0.0"
CONTRACT_ID = "star-acm-field-decomposed-execution"
CONTRACT_VERSION = "1.0.0"
FIELDS = (("title", "Title"), ("keyword", "Keyword"), ("abstract", "Abstract"))
ACM_SCOPE = "ACM Publications"
ACM_COLLECTION = "Full-Text Collection"
ACM_EXPORT_FILTER = "ACM Content: DL"
ACM_SORT = "publicationDate asc"
ACM_EXPORT_RECORD_CEILING = 1000
EXPORT_PARTITION_CONTRACT_SCHEMA_VERSION = "1.0.0"
EXPORT_PARTITION_CONTRACT_ID = "star-acm-publication-year-export-partition"
EXPORT_PARTITION_CONTRACT_VERSION = "1.0.0"
EXPECTED_PLAN_HASH = "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
EXPECTED_PLAN_RAW_SHA256 = "b887d638e42f4909c1c8461dde733d758e5176d528ddccee4370211e14ed7451"
QUERY_SYNTAX_MANIFEST_SCHEMA_VERSION = "1.0.0"
QUERY_SYNTAX_MANIFEST_VERSION = "1.0.0"
QUERY_SYNTAX_MANIFEST_ID = "star-acm-field-execution-2026-09-02-query-syntax"
EXECUTION_METHOD_COMMIT = "1913d7390a527af5fbdb8b45b30672deabe295e2"
QUERY_SYNTAX_VALID = "VALID"
QUERY_SYNTAX_INVALID = "INVALID"
QUERY_SYNTAX_COMPLETE = "SYNTAX_COUNT_EVIDENCE_VALIDATED"
QUERY_SYNTAX_INCOMPLETE = "COMPLETED_CAPTURE_WITH_VALIDATION_FAILURE"
FINAL_RECONCILIATION_SCHEMA_VERSION = "1.0.0"
FINAL_RECONCILIATION_MANIFEST_VERSION = "1.0.0"
FINAL_RECONCILIATION_MANIFEST_ID = (
    "star-acm-field-execution-2026-09-03-final-reconciliation"
)
ACM_RETRIEVAL_EVIDENCE_COMPLETE = "RETRIEVAL_EVIDENCE_COMPLETE_NOT_IMPORTED"
ACM_FINAL_RECONCILIATION_AT_UTC = "2026-09-03T18:29:49Z"
QUERY_SYNTAX_FILENAME = re.compile(
    r"(?P<family>QF[0-9]{2})_(?P<field>title|keyword|abstract)_query_syntax_csv"
)
ACM_TIMESTAMP = re.compile(
    r"(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2}) at "
    r"(?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2}) (?P<zone>PDT|PST)"
)
ACM_TIMEZONE_OFFSETS = {"PDT": -7, "PST": -8}
OPERATOR_EVIDENCE_LIMITATIONS = {
    "query_url_or_screenshot_evidence": "NOT_SUPPLIED",
    "operator_identity": "NOT_SUPPLIED",
    "institutional_access_provenance": "NOT_SUPPLIED",
    "citation_exports": "NOT_PERFORMED",
    "field_unions": "NOT_COMPUTED",
    "readiness": "NOT_READY",
}
QUERY_SYNTAX_PRODUCTION_SIDE_EFFECTS = {
    "citation_export_created": False,
    "field_union_created": False,
    "screening_executed": False,
    "prisma_generated": False,
    "e6_derived": False,
    "llm_executed": False,
    "corpus_membership_created": False,
}

FINAL_SELECTED_EXPORTS = {
    "QF01": {
        "title": ("QF01/title/QF01_title_000001-000008.bib",),
        "keyword": ("QF01/keyword/QF01_keyword_000001-000028.bib",),
        "abstract": (
            "QF01/abstract/QF01_abstract_1974-2016.bib",
            "QF01/abstract/QF01_abstract_2017-2026.bib",
        ),
    },
    "QF02": {
        "title": ("QF02/title/QF02_title_000001-000018.bib",),
        "keyword": ("QF02/keyword/QF02_keyword_000001-000016.bib",),
        "abstract": (
            "QF02/abstract/QF02_abstract_1967-2024.bib",
            "QF02/abstract/QF02_abstract_2025-2026.bib",
        ),
    },
    "QF03": {
        "title": ("QF03/title/QF03_title_000001-000004.bib",),
        "keyword": ("QF03/keyword/QF03_keyword_000001-000012.bib",),
        "abstract": (
            "QF03/abstract/QF03_abstract_1971-2018.bib",
            "QF03/abstract/QF03_abstract_2019-2019.bib",
            "QF03/abstract/QF03_abstract_2020-2026.bib",
        ),
    },
    "QF04": {
        "title": ("QF04/title/QF04_title_000001-000023.bib",),
        "keyword": ("QF04/keyword/QF04_keyword_000001-000034.bib",),
        "abstract": (
            "QF04/abstract/QF04_abstract_1978-2018.bib",
            "QF04/abstract/QF04_abstract_2019-2024.bib",
            "QF04/abstract/QF04_abstract_2025-2026.bib",
        ),
    },
    "QF05": {
        "title": ("QF05/title/QF05_title_000001-000019.bib",),
        "keyword": ("QF05/keyword/QF05_keyword_000001-000024.bib",),
        "abstract": (
            "QF05/abstract/QF05_abstract_1972-2017.bib",
            "QF05/abstract/QF05_abstract_2018-2023.bib",
            "QF05/abstract/QF05_abstract_2024-2024.bib",
            "QF05/abstract/QF05_abstract_2025-2025.bib",
            "QF05/abstract/QF05_abstract_2026-2026.bib",
        ),
    },
}

FINAL_ABSTRACT_RANGES = {
    "QF01": ((1974, 2016), (2017, 2026)),
    "QF02": ((1967, 2024), (2025, 2026)),
    "QF03": ((1971, 2018), (2019, 2019), (2020, 2026)),
    "QF04": ((1978, 2018), (2019, 2024), (2025, 2026)),
    "QF05": ((1972, 2017), (2018, 2023), (2024, 2024), (2025, 2025), (2026, 2026)),
}

FINAL_NONSELECTED_EXPORTS = (
    (
        "QF01/abstract/QF01_abstract_000001-001931.bib",
        "CALIBRATION_ONLY",
        "ACM Export All ceiling calibration; never eligible for the partition union",
    ),
    (
        "QF01/keyword/QF01_keyword_000001-000020.bib",
        "SUPERSEDED_INCOMPLETE_OPERATOR_EXPORT",
        "affirmative first-page-only export error superseded by the 28-record Export All artifact",
    ),
    (
        "QF02/abstract/QF02_abstract_2025-2025_SHORT.bib",
        "SUPERSEDED_DUPLICATE_EXPORT",
        "same 768-record stable-identity set as the selected 2025-2026 operator-labeled artifact",
    ),
    (
        "QF05/abstract/QF05_abstract_2024-2025.bib",
        "SUPERSEDED_OVERLAPPING_EXPLORATORY_EXPORT",
        "overlaps the selected single-year artifacts and is preserved outside the production union",
    ),
)

FINAL_SCREENSHOT_OBSERVATIONS = (
    ("QF01/QF01_abstract_1974-2016.png", "QF01", (), 1974, 2016, 950, "2026-09-03T17:23:20Z"),
    ("QF01/QF01_abstract_2017-2026.png", "QF01", (), 2017, 2026, 982, "2026-09-03T17:24:11Z"),
    ("QF03/QF03_abstract_1971-2018.png", "QF03", (), 1971, 2018, 951, "2026-09-03T17:19:36Z"),
    ("QF03/QF03_abstract_2019-2019.png", "QF03", (), 2019, 2019, 78, "2026-09-03T17:14:53Z"),
    ("QF03/QF03_abstract_2020-2026.png", "QF03", (), 2020, 2026, 955, "2026-09-03T17:20:49Z"),
    ("QF03/QF03_abstract_all_dates.png", "QF03", (), None, None, 1983, "2026-09-03T18:27:50Z"),
    ("QF04/QF04_abstract_1978-2018.png", "QF04", (), 1978, 2018, 999, "2026-09-03T17:13:17Z"),
    ("QF04/QF04_abstract_2019-2024.png", "QF04", (), 2019, 2024, 988, "2026-09-03T17:12:38Z"),
    ("QF04/QF04_abstract_2025-2026.png", "QF04", (), 2025, 2026, 446, "2026-09-03T17:12:03Z"),
    ("QF04/QF04_abstract_all_dates.png", "QF04", (), None, None, 2433, "2026-09-03T18:28:43Z"),
    ("QF05/QF05_abstract_1972-2017.png", "QF05", (), 1972, 2017, 908, "2026-09-03T17:10:10Z"),
    ("QF05/QF05_abstract_2018-2023.png", "QF05", (), 2018, 2023, 858, "2026-09-03T17:06:03Z"),
    ("QF05/QF05_abstract_2023-2024.png", "QF05", (), 2023, 2024, 678, "2026-09-03T17:05:38Z"),
    ("QF05/QF05_abstract_2024-2024.png", "QF05", (), 2024, 2024, 459, "2026-09-03T17:05:11Z"),
    ("QF05/QF05_abstract_2025-2025.png", "QF05", (), 2025, 2025, 668, "2026-09-03T17:04:27Z"),
    ("QF05/QF05_abstract_2026-2026.png", "QF05", (), 2026, 2026, 563, "2026-09-03T17:00:52Z"),
    ("QF05/QF05_abstract_all_dates.png", "QF05", (), None, None, 3456, "2026-09-03T18:29:49Z"),
    ("ambiguous/abstract_1967-2024_count_000905_attempt_001.png", None, ("QF01", "QF02"), 1967, 2024, 905, "2026-09-03T18:18:21Z"),
    ("ambiguous/abstract_2025-2026_count_000769_attempt_001.png", None, ("QF01", "QF02"), 2025, 2026, 769, "2026-09-03T18:19:24Z"),
    ("ambiguous/abstract_all_dates_count_001674_attempt_001.png", None, ("QF01", "QF02"), None, None, 1674, "2026-09-03T18:17:17Z"),
    ("ambiguous/abstract_all_dates_count_001931_attempt_001.png", None, ("QF01", "QF02"), None, None, 1931, "2026-09-03T18:20:30Z"),
)


class AcmFieldExecutionError(ValueError):
    """Raised when ACM field evidence or union provenance is incomplete or inconsistent."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _validate_sha256(value: str, label: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise AcmFieldExecutionError(f"{label} must be a lowercase SHA-256")


def _validate_relative_path(value: str) -> None:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise AcmFieldExecutionError("artifact paths must be relative and traversal-safe")


def child_query_id(parent_query_id: str, field_key: str) -> str:
    if field_key not in {item[0] for item in FIELDS}:
        raise AcmFieldExecutionError(f"unsupported ACM field {field_key}")
    return f"{parent_query_id}:field:{field_key}"


def field_query(field_label: str, scientific_query: str) -> str:
    return f"{field_label}:({scientific_query})"


@dataclass(frozen=True, slots=True)
class AcmQuerySyntaxEvidence:
    query_name: str
    search_run_date: str
    reported_count: int
    exported_syntax: str


@dataclass(frozen=True, slots=True)
class AcmExportArtifactReference:
    artifact_relative_path: str
    artifact_sha256: str
    byte_size: int
    first_record: int
    last_record: int

    def validate(self) -> None:
        _validate_relative_path(self.artifact_relative_path)
        _validate_sha256(self.artifact_sha256, "ACM export artifact hash")
        if self.byte_size < 0:
            raise AcmFieldExecutionError("ACM export artifact byte size cannot be negative")
        if self.first_record < 1 or self.last_record < self.first_record:
            raise AcmFieldExecutionError("ACM export artifact range is invalid")


@dataclass(frozen=True, slots=True)
class AcmYearPartitionArtifactReference:
    """One raw BibTeX artifact produced for an inclusive publication-year range."""

    artifact_relative_path: str
    artifact_sha256: str
    byte_size: int
    parsed_entry_count: int

    def validate(self) -> None:
        _validate_relative_path(self.artifact_relative_path)
        _validate_sha256(self.artifact_sha256, "ACM year-partition artifact hash")
        if self.byte_size < 0:
            raise AcmFieldExecutionError("ACM year-partition artifact size cannot be negative")
        if self.parsed_entry_count < 0:
            raise AcmFieldExecutionError("ACM parsed-entry count cannot be negative")


@dataclass(frozen=True, slots=True)
class AcmRawFieldHit:
    child_query_id: str
    field_key: str
    row_ordinal: int
    raw_entry_sha256: str
    acm_native_id: str | None = None
    doi: str | None = None

    def validate(self) -> None:
        if self.field_key not in {item[0] for item in FIELDS}:
            raise AcmFieldExecutionError(f"unsupported ACM field {self.field_key}")
        if self.row_ordinal < 1:
            raise AcmFieldExecutionError("ACM raw-hit ordinals must be positive")
        _validate_sha256(self.raw_entry_sha256, "ACM raw-entry hash")
        if not (self.acm_native_id and self.acm_native_id.strip()) and not normalize_doi(self.doi):
            raise AcmFieldExecutionError(
                "ACM union requires a stable ACM native identifier or valid DOI for every hit"
            )

    def identity_tokens(self) -> tuple[str, ...]:
        self.validate()
        tokens: list[str] = []
        if self.acm_native_id and self.acm_native_id.strip():
            tokens.append(f"acm:{self.acm_native_id.strip()}")
        normalized = normalize_doi(self.doi)
        if normalized:
            tokens.append(f"doi:{normalized}")
        return tuple(tokens)


@dataclass(frozen=True, slots=True)
class AcmYearPartitionEvidence:
    """Count and parsed export evidence for one inclusive operational year range."""

    partition_id: str
    child_query_id: str
    field_key: str
    from_year: int
    to_year: int
    ui_reported_count: int
    artifact: AcmYearPartitionArtifactReference
    hits: tuple[AcmRawFieldHit, ...]
    ui_count_observed_at_utc: str
    exported_at_utc: str
    operator_sort: str | None = None

    def validate(self, child_spec: dict[str, Any]) -> None:
        if not self.partition_id.strip():
            raise AcmFieldExecutionError("ACM year partition requires an ID")
        if self.child_query_id != child_spec["child_query_id"]:
            raise AcmFieldExecutionError("ACM year partition is linked to the wrong child")
        if self.field_key != child_spec["field_key"]:
            raise AcmFieldExecutionError("ACM year partition field does not match the child")
        if self.from_year > self.to_year:
            raise AcmFieldExecutionError("ACM year partition has an invalid inclusive range")
        if self.ui_reported_count < 0:
            raise AcmFieldExecutionError("ACM year-partition UI count cannot be negative")
        if self.ui_reported_count > ACM_EXPORT_RECORD_CEILING:
            raise AcmFieldExecutionError("ACM year-partition UI count exceeds export ceiling")
        if not self.ui_count_observed_at_utc.strip() or not self.exported_at_utc.strip():
            raise AcmFieldExecutionError("ACM year partition requires count and export timestamps")
        self.artifact.validate()
        if self.artifact.parsed_entry_count != self.ui_reported_count:
            raise AcmFieldExecutionError(
                "ACM year-partition parsed-entry count does not reconcile to partition UI count"
            )
        if len(self.hits) != self.artifact.parsed_entry_count:
            raise AcmFieldExecutionError(
                "ACM year-partition stable-identity rows do not reconcile to parsed entries"
            )
        if [hit.row_ordinal for hit in self.hits] != list(range(1, len(self.hits) + 1)):
            raise AcmFieldExecutionError("ACM year-partition row ordinals must be contiguous")
        for hit in self.hits:
            hit.validate()
            if hit.child_query_id != self.child_query_id or hit.field_key != self.field_key:
                raise AcmFieldExecutionError("ACM year-partition hit is linked to the wrong child")


@dataclass(frozen=True, slots=True)
class AcmChildPartitionUnionResult:
    """Set-based completeness result for one field child's year partitions."""

    child_query_id: str
    complete: bool
    state: str
    unfiltered_ui_count: int
    partition_ui_count_sum: int
    partition_count: int
    unique_union_count: int | None
    ordering_used_for_completeness: bool = False


@dataclass(frozen=True, slots=True)
class AcmFieldExportEvidence:
    child_query_id: str
    field_key: str
    operator_evidence: AcmQuerySyntaxEvidence
    artifacts: tuple[AcmExportArtifactReference, ...]
    hits: tuple[AcmRawFieldHit, ...]
    exported_at_utc: str

    def validate(self, child_spec: dict[str, Any]) -> None:
        if self.child_query_id != child_spec["child_query_id"]:
            raise AcmFieldExecutionError("ACM export child-query ID does not match contract")
        if self.field_key != child_spec["field_key"]:
            raise AcmFieldExecutionError("ACM export field does not match contract")
        if not self.exported_at_utc.strip():
            raise AcmFieldExecutionError("ACM export requires a UTC timestamp")
        validate_acm_query_syntax(self.operator_evidence, child_spec)
        if not self.artifacts:
            raise AcmFieldExecutionError("ACM field export requires artifact references")
        for artifact in self.artifacts:
            artifact.validate()
        expected_ranges = sorted(
            (artifact.first_record, artifact.last_record) for artifact in self.artifacts
        )
        next_record = 1
        for first_record, last_record in expected_ranges:
            if first_record != next_record:
                raise AcmFieldExecutionError("ACM export ranges must be contiguous and non-overlapping")
            next_record = last_record + 1
        if next_record - 1 != self.operator_evidence.reported_count:
            raise AcmFieldExecutionError("ACM export ranges do not reconcile to UI count")
        if len(self.hits) != self.operator_evidence.reported_count:
            raise AcmFieldExecutionError("ACM raw field hits do not reconcile to UI count")
        for hit in self.hits:
            hit.validate()
            if hit.child_query_id != self.child_query_id or hit.field_key != self.field_key:
                raise AcmFieldExecutionError("ACM raw hit is linked to the wrong child query")


def reconcile_acm_year_partitions(
    child_spec: dict[str, Any],
    unfiltered_operator_evidence: AcmQuerySyntaxEvidence,
    *,
    supported_year_from: int,
    supported_year_to: int,
    partitions: Iterable[AcmYearPartitionEvidence],
) -> AcmChildPartitionUnionResult:
    """Validate and reconcile a complete, set-based year-partition export.

    Publication years are operational provider filters only.  The accepted unfiltered
    field query remains the scientific result definition, and provider ordering is
    deliberately excluded from every completeness decision.
    """

    validate_acm_query_syntax(unfiltered_operator_evidence, child_spec)
    if supported_year_from > supported_year_to:
        raise AcmFieldExecutionError("ACM supported publication-year domain is invalid")
    items = sorted(partitions, key=lambda item: (item.from_year, item.to_year))
    if not items:
        raise AcmFieldExecutionError("ACM year partition plan cannot be empty")
    if len({item.partition_id for item in items}) != len(items):
        raise AcmFieldExecutionError("ACM year partition IDs must be unique")

    expected_from = supported_year_from
    for item in items:
        item.validate(child_spec)
        if item.from_year != expected_from:
            raise AcmFieldExecutionError(
                "ACM year partitions must be disjoint and collectively exhaustive"
            )
        expected_from = item.to_year + 1
    if expected_from != supported_year_to + 1:
        raise AcmFieldExecutionError(
            "ACM year partitions must be disjoint and collectively exhaustive"
        )

    partition_count_sum = sum(item.ui_reported_count for item in items)
    unfiltered_count = unfiltered_operator_evidence.reported_count
    if partition_count_sum != unfiltered_count:
        return AcmChildPartitionUnionResult(
            child_query_id=child_spec["child_query_id"],
            complete=False,
            state="UNRESOLVED_UNDATED_OR_UNREPRESENTED_RECORDS",
            unfiltered_ui_count=unfiltered_count,
            partition_ui_count_sum=partition_count_sum,
            partition_count=len(items),
            unique_union_count=None,
        )

    parent: dict[str, str] = {}

    def find(token: str) -> str:
        parent.setdefault(token, token)
        while parent[token] != token:
            parent[token] = parent[parent[token]]
            token = parent[token]
        return token

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        first, second = sorted((left_root, right_root))
        parent[second] = first

    hit_tokens: list[tuple[str, tuple[str, ...]]] = []
    for item in items:
        for hit in item.hits:
            tokens = hit.identity_tokens()
            for token in tokens:
                find(token)
            for token in tokens[1:]:
                union(tokens[0], token)
            hit_tokens.append((item.partition_id, tokens))

    component_partitions: dict[str, set[str]] = {}
    for partition_id, tokens in hit_tokens:
        component_partitions.setdefault(find(tokens[0]), set()).add(partition_id)
    if any(len(partition_ids) > 1 for partition_ids in component_partitions.values()):
        raise AcmFieldExecutionError("ACM stable identity overlaps across year partitions")

    unique_union_count = len({find(tokens[0]) for _, tokens in hit_tokens})
    if unique_union_count != unfiltered_count:
        raise AcmFieldExecutionError(
            "ACM year-partition unique union does not reconcile to unfiltered child count"
        )
    return AcmChildPartitionUnionResult(
        child_query_id=child_spec["child_query_id"],
        complete=True,
        state="COMPLETE_SET_RECONCILED",
        unfiltered_ui_count=unfiltered_count,
        partition_ui_count_sum=partition_count_sum,
        partition_count=len(items),
        unique_union_count=unique_union_count,
    )


@dataclass(frozen=True, slots=True)
class AcmParentUnionResult:
    parent_query_id: str
    parent_query_hash: str
    complete: bool
    child_query_ids: tuple[str, ...]
    per_field_raw_counts: dict[str, int]
    raw_hit_total: int
    unique_union_count: int
    overlap_counts: dict[str, int]
    members: tuple[dict[str, Any], ...]
    union_hash: str

    def canonical_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("union_hash", None)
        return payload

    def validate(self) -> None:
        _validate_sha256(self.parent_query_hash, "parent ACM query hash")
        _validate_sha256(self.union_hash, "ACM union hash")
        if not self.complete:
            raise AcmFieldExecutionError("an ACM parent union cannot be emitted incomplete")
        if self.raw_hit_total != sum(self.per_field_raw_counts.values()):
            raise AcmFieldExecutionError("ACM raw-hit total does not reconcile")
        if self.unique_union_count != sum(self.overlap_counts.values()):
            raise AcmFieldExecutionError("ACM overlap counts do not reconcile to union count")
        if self.unique_union_count != len(self.members):
            raise AcmFieldExecutionError("ACM union member count does not reconcile")
        if self.union_hash != _sha256_text(_canonical_json(self.canonical_payload())):
            raise AcmFieldExecutionError("ACM parent union hash mismatch")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)


def parse_acm_query_syntax_csv(raw: bytes | str) -> list[AcmQuerySyntaxEvidence]:
    """Parse ACM's one-line Export Query Syntax CSV, including unquoted comma counts."""

    text = raw.decode("utf-8-sig") if isinstance(raw, bytes) else raw.lstrip("\ufeff")
    lines = [line.rstrip("\r") for line in text.splitlines() if line.strip()]
    expected_header = "ACM DL: Query Name,Search Run Date,Search Result Count,Query Syntax"
    if not lines or lines[0] != expected_header:
        raise AcmFieldExecutionError("unsupported ACM Export Query Syntax CSV header")
    evidence: list[AcmQuerySyntaxEvidence] = []
    row_pattern = re.compile(
        r"^(?P<name>[^,]*),(?P<date>[^,]*),(?P<count>[0-9][0-9,]*),"
        r"(?P<syntax>.*)$"
    )
    for line in lines[1:]:
        match = row_pattern.fullmatch(line)
        if match is None:
            raise AcmFieldExecutionError("malformed ACM Export Query Syntax row")
        count_text = match.group("count")
        if not re.fullmatch(r"(?:0|[1-9][0-9]*|[1-9][0-9]{0,2}(?:,[0-9]{3})+)", count_text):
            raise AcmFieldExecutionError("malformed ACM result count")
        evidence.append(
            AcmQuerySyntaxEvidence(
                query_name=match.group("name"),
                search_run_date=match.group("date"),
                reported_count=int(count_text.replace(",", "")),
                exported_syntax=match.group("syntax"),
            )
        )
    if not evidence:
        raise AcmFieldExecutionError("ACM Export Query Syntax CSV contains no evidence row")
    return evidence


def validate_acm_query_syntax(
    evidence: AcmQuerySyntaxEvidence, child_spec: dict[str, Any]
) -> None:
    """Require one exact field wrapper and the frozen full-text collection scope."""

    expected_query = child_spec["child_query_text"]
    rendered_query, exported_filter = _exported_syntax_parts(evidence.exported_syntax)
    wrappers = re.findall(r"(?i)\b(?:title|keyword|abstract):\(", rendered_query)
    if len(wrappers) != 1:
        raise AcmFieldExecutionError("ACM child evidence must contain exactly one field wrapper")
    expected_wrapper = f"{child_spec['field_label']}:("
    if wrappers[0] != expected_wrapper:
        raise AcmFieldExecutionError("ACM child evidence contains the wrong field wrapper")
    if rendered_query != expected_query:
        raise AcmFieldExecutionError("ACM child evidence does not contain the exact frozen query")
    if exported_filter != ACM_EXPORT_FILTER:
        raise AcmFieldExecutionError("ACM evidence must use the Full-Text Collection")


def _exported_syntax_parts(syntax: str) -> tuple[str, str]:
    match = re.fullmatch(
        r'"?query"?\s*:\s*\{(?P<query>.*)\}\s*\t\s*'
        r'"?filter"?\s*:\s*\{(?P<filter>[^}]*)\}\s*',
        syntax,
    )
    if match is None:
        raise AcmFieldExecutionError("ACM exported syntax has an unsupported structure")
    return match.group("query"), match.group("filter")


def normalize_acm_search_timestamp(value: str) -> str:
    """Normalize ACM's exported US Pacific timestamp while retaining it verbatim elsewhere."""

    match = ACM_TIMESTAMP.fullmatch(value)
    if match is None:
        raise AcmFieldExecutionError("unsupported ACM search timestamp")
    offset = timezone(timedelta(hours=ACM_TIMEZONE_OFFSETS[match.group("zone")]))
    local = datetime.strptime(
        f"{match.group('date')} {match.group('time')}", "%Y-%m-%d %H:%M:%S"
    ).replace(tzinfo=offset)
    return local.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _family_code(family_id: str) -> str:
    match = re.search(r"QF[0-9]{2}", family_id)
    if match is None:
        raise AcmFieldExecutionError(f"ACM family has no QF code: {family_id}")
    return match.group()


def _expected_query_names(family_code: str, field_key: str) -> tuple[str, str]:
    stem = f"{family_code}_{field_key}_query_syntax"
    return f"{stem}.csv", f"{stem}_csv"


def _syntax_validation_result(
    evidence: AcmQuerySyntaxEvidence,
    child_spec: dict[str, Any],
    *,
    expected_query_names: tuple[str, ...],
) -> dict[str, Any]:
    checks = {
        "query_name_matches_child": evidence.query_name in expected_query_names,
        "exactly_one_expected_field_wrapper": False,
        "no_other_field_wrapper": False,
        "exact_frozen_scientific_expression": False,
        "required_collection_filter": False,
        "ui_count_parsed": isinstance(evidence.reported_count, int)
        and evidence.reported_count >= 0,
        "timestamp_normalized_to_utc": False,
    }
    reasons: list[str] = []
    if not checks["query_name_matches_child"]:
        reasons.append(
            "query name does not match expected child names "
            f"{', '.join(expected_query_names)}"
        )
    try:
        normalize_acm_search_timestamp(evidence.search_run_date)
        checks["timestamp_normalized_to_utc"] = True
    except AcmFieldExecutionError as exc:
        reasons.append(str(exc))

    try:
        rendered_query, exported_filter = _exported_syntax_parts(evidence.exported_syntax)
    except AcmFieldExecutionError as exc:
        reasons.append(str(exc))
    else:
        wrappers = re.findall(r"(?i)\b(?:title|keyword|abstract):\(", rendered_query)
        expected_wrapper = f"{child_spec['field_label']}:("
        checks["exactly_one_expected_field_wrapper"] = (
            len(wrappers) == 1 and wrappers[0] == expected_wrapper
        )
        checks["no_other_field_wrapper"] = len(wrappers) == 1
        checks["exact_frozen_scientific_expression"] = (
            rendered_query == child_spec["child_query_text"]
        )
        checks["required_collection_filter"] = exported_filter == ACM_EXPORT_FILTER
        if len(wrappers) != 1:
            reasons.append("ACM child evidence must contain exactly one field wrapper")
        elif wrappers[0] != expected_wrapper:
            reasons.append("ACM child evidence contains the wrong field wrapper")
        if not checks["exact_frozen_scientific_expression"]:
            escaped_quotes = rendered_query.count('\\"')
            diagnostic_unescaped = rendered_query.replace('\\"', '"')
            if escaped_quotes and diagnostic_unescaped == child_spec["child_query_text"]:
                reasons.append(
                    "stored query contains "
                    f"{escaped_quotes} literal backslashes before phrase quotes; "
                    "exact frozen query required"
                )
            else:
                reasons.append("ACM child evidence does not contain the exact frozen query")
        if not checks["required_collection_filter"]:
            reasons.append("ACM evidence must use the Full-Text Collection")

    state = QUERY_SYNTAX_VALID if all(checks.values()) else QUERY_SYNTAX_INVALID
    return {
        "state": state,
        "reason": None if state == QUERY_SYNTAX_VALID else "; ".join(reasons),
        "checks": checks,
    }


def build_acm_query_syntax_manifest(
    contract_path: str | Path,
    syntax_directory: str | Path,
    *,
    root: str | Path,
) -> dict[str, Any]:
    """Ingest exactly one historical query-syntax attempt for each frozen ACM child."""

    root_path = Path(root).resolve()
    contract_file = Path(contract_path).resolve()
    syntax_path = Path(syntax_directory).resolve()
    try:
        contract_relative = contract_file.relative_to(root_path).as_posix()
        syntax_relative = syntax_path.relative_to(root_path).as_posix()
    except ValueError as exc:
        raise AcmFieldExecutionError("ACM evidence inputs must be inside the repository root") from exc
    if not syntax_path.is_dir():
        raise AcmFieldExecutionError("ACM query-syntax artifact directory is missing")

    contract = load_acm_field_execution_contract(contract_file, root=root_path)
    observed_files: dict[tuple[str, str], Path] = {}
    for path in sorted(syntax_path.iterdir(), key=lambda item: item.name):
        if not path.is_file():
            raise AcmFieldExecutionError("ACM query-syntax directory must contain only files")
        match = QUERY_SYNTAX_FILENAME.fullmatch(path.name)
        if match is None:
            raise AcmFieldExecutionError(f"unexpected ACM query-syntax artifact: {path.name}")
        key = (match.group("family"), match.group("field"))
        if key in observed_files:
            raise AcmFieldExecutionError(f"duplicate ACM query-syntax artifact for {key}")
        observed_files[key] = path

    expected_keys = {
        (_family_code(family["family_id"]), child["field_key"])
        for family in contract["families"]
        for child in family["children"]
    }
    if set(observed_files) != expected_keys:
        missing = sorted(expected_keys - set(observed_files))
        unexpected = sorted(set(observed_files) - expected_keys)
        raise AcmFieldExecutionError(
            f"ACM query-syntax directory does not match contract; missing={missing}, "
            f"unexpected={unexpected}"
        )

    valid_attempts = 0
    invalid_attempts = 0
    families: list[dict[str, Any]] = []
    for family in contract["families"]:
        family_code = _family_code(family["family_id"])
        children: list[dict[str, Any]] = []
        for child in family["children"]:
            path = observed_files[(family_code, child["field_key"])]
            raw = path.read_bytes()
            evidence_rows = parse_acm_query_syntax_csv(raw)
            if len(evidence_rows) != 1:
                raise AcmFieldExecutionError(
                    f"ACM query-syntax artifact must contain exactly one row: {path.name}"
                )
            evidence = evidence_rows[0]
            expected_names = _expected_query_names(family_code, child["field_key"])
            validation = _syntax_validation_result(
                evidence, child, expected_query_names=expected_names
            )
            if validation["state"] == QUERY_SYNTAX_VALID:
                valid_attempts += 1
                accepted_attempt_number: int | None = 1
                completion_state = QUERY_SYNTAX_COMPLETE
            else:
                invalid_attempts += 1
                accepted_attempt_number = None
                completion_state = "REQUIRES_CORRECTED_ATTEMPT"
            attempt = {
                "attempt_number": 1,
                "supersedes_attempt_number": None,
                "superseded_by_attempt_number": None,
                "family_id": family["family_id"],
                "child_query_id": child["child_query_id"],
                "field_key": child["field_key"],
                "field_label": child["field_label"],
                "artifact": {
                    "relative_path": path.relative_to(root_path).as_posix(),
                    "byte_size": len(raw),
                    "raw_sha256": _sha256_bytes(raw),
                    "availability": "AVAILABLE",
                },
                "query_name": evidence.query_name,
                "acm_search_run_date_verbatim": evidence.search_run_date,
                "normalized_search_timestamp_utc": normalize_acm_search_timestamp(
                    evidence.search_run_date
                ),
                "parsed_ui_reported_count": evidence.reported_count,
                "validation": validation,
            }
            children.append(
                {
                    "child_query_id": child["child_query_id"],
                    "field_key": child["field_key"],
                    "field_label": child["field_label"],
                    "accepted_attempt_number": accepted_attempt_number,
                    "completion_state": completion_state,
                    "attempts": [attempt],
                }
            )
        families.append(
            {
                "family_id": family["family_id"],
                "parent_query_id": family["parent_query_id"],
                "children": children,
            }
        )

    manifest: dict[str, Any] = {
        "schema_version": QUERY_SYNTAX_MANIFEST_SCHEMA_VERSION,
        "manifest_id": QUERY_SYNTAX_MANIFEST_ID,
        "manifest_version": QUERY_SYNTAX_MANIFEST_VERSION,
        "manifest_status": (
            QUERY_SYNTAX_COMPLETE if invalid_attempts == 0 else QUERY_SYNTAX_INCOMPLETE
        ),
        "execution_method_commit": EXECUTION_METHOD_COMMIT,
        "contract": {
            "path": contract_relative,
            "version": contract["contract_version"],
            "contract_hash": contract["contract_hash"],
            "raw_sha256": _sha256_bytes(contract_file.read_bytes()),
            "byte_size": contract_file.stat().st_size,
        },
        "source_directory": syntax_relative,
        "ignored_raw_artifacts": True,
        "immutable_artifact_storage": {
            "status": "unresolved",
            "locator": None,
            "repository_policy": "raw operator artifacts are under the ignored artifacts directory",
            "limitation": "checksums detect mutation but do not preserve ignored artifact bytes",
        },
        "families": families,
        "validation_summary": {
            "expected_child_count": len(expected_keys),
            "observed_artifact_count": len(observed_files),
            "valid_attempt_count": valid_attempts,
            "invalid_attempt_count": invalid_attempts,
            "all_children_have_accepted_attempt": invalid_attempts == 0,
        },
        "operator_evidence_limitations": dict(OPERATOR_EVIDENCE_LIMITATIONS),
        "production_side_effects": dict(QUERY_SYNTAX_PRODUCTION_SIDE_EFFECTS),
    }
    manifest["manifest_hash"] = _sha256_text(_canonical_json(manifest))
    validate_acm_query_syntax_manifest(manifest, root=root_path, verify_artifacts=True)
    return manifest


def reconcile_acm_query_syntax_manifest(
    existing_manifest: dict[str, Any],
    contract_path: str | Path,
    syntax_directory: str | Path,
    *,
    root: str | Path,
) -> dict[str, Any]:
    """Append changed same-path bytes as new attempts without reconstructing overwritten bytes."""

    prior_material = dict(existing_manifest)
    prior_hash = prior_material.pop("manifest_hash", None)
    if prior_hash != _sha256_text(_canonical_json(prior_material)):
        raise AcmFieldExecutionError("existing ACM query-syntax manifest hash mismatch")

    current = build_acm_query_syntax_manifest(
        contract_path, syntax_directory, root=root
    )
    reconciled = json.loads(json.dumps(existing_manifest))
    current_children = {
        child["child_query_id"]: child
        for family in current["families"]
        for child in family["children"]
    }
    changed_children: list[str] = []
    for family in reconciled["families"]:
        for child in family["children"]:
            current_child = current_children[child["child_query_id"]]
            latest = child["attempts"][-1]
            latest["artifact"].setdefault("availability", "AVAILABLE")
            observed = current_child["attempts"][0]
            if (
                latest["artifact"]["raw_sha256"] == observed["artifact"]["raw_sha256"]
                and latest["artifact"]["byte_size"] == observed["artifact"]["byte_size"]
            ):
                continue
            next_number = latest["attempt_number"] + 1
            latest["superseded_by_attempt_number"] = next_number
            latest["artifact"]["availability"] = "OVERWRITTEN_UNAVAILABLE"
            new_attempt = json.loads(json.dumps(observed))
            new_attempt["attempt_number"] = next_number
            new_attempt["supersedes_attempt_number"] = latest["attempt_number"]
            new_attempt["superseded_by_attempt_number"] = None
            child["attempts"].append(new_attempt)
            changed_children.append(child["child_query_id"])

    accepted_count = 0
    valid_attempt_count = 0
    invalid_attempt_count = 0
    artifact_count = 0
    for family in reconciled["families"]:
        for child in family["children"]:
            for attempt in child["attempts"]:
                artifact_count += 1
                if attempt["validation"]["state"] == QUERY_SYNTAX_VALID:
                    valid_attempt_count += 1
                else:
                    invalid_attempt_count += 1
            latest = child["attempts"][-1]
            if latest["validation"]["state"] == QUERY_SYNTAX_VALID:
                child["accepted_attempt_number"] = latest["attempt_number"]
                child["completion_state"] = QUERY_SYNTAX_COMPLETE
                accepted_count += 1
            else:
                child["accepted_attempt_number"] = None
                child["completion_state"] = "REQUIRES_CORRECTED_ATTEMPT"

    expected_child_count = sum(
        len(family["children"]) for family in reconciled["families"]
    )
    reconciled["validation_summary"] = {
        "expected_child_count": expected_child_count,
        "observed_artifact_count": artifact_count,
        "valid_attempt_count": valid_attempt_count,
        "invalid_attempt_count": invalid_attempt_count,
        "all_children_have_accepted_attempt": accepted_count == expected_child_count,
    }
    reconciled["manifest_status"] = (
        QUERY_SYNTAX_COMPLETE
        if accepted_count == expected_child_count
        else QUERY_SYNTAX_INCOMPLETE
    )
    if changed_children and "reconciliation" not in reconciled:
        reconciled["reconciliation"] = {
            "previous_manifest_hash": prior_hash,
            "changed_child_query_ids": changed_children,
            "overwritten_bytes_reconstructed": False,
        }
    reconciled.pop("manifest_hash", None)
    reconciled["manifest_hash"] = _sha256_text(_canonical_json(reconciled))
    validate_acm_query_syntax_manifest(reconciled, root=root, verify_artifacts=True)
    return reconciled


def validate_acm_query_syntax_manifest(
    manifest: dict[str, Any],
    *,
    root: str | Path,
    verify_artifacts: bool = False,
) -> None:
    """Validate manifest structure and optionally re-read every ignored raw artifact."""

    material = dict(manifest)
    claimed_hash = material.pop("manifest_hash", None)
    if claimed_hash != _sha256_text(_canonical_json(material)):
        raise AcmFieldExecutionError("ACM query-syntax manifest hash mismatch")
    if manifest.get("schema_version") != QUERY_SYNTAX_MANIFEST_SCHEMA_VERSION:
        raise AcmFieldExecutionError("unsupported ACM query-syntax manifest schema")
    if manifest.get("manifest_id") != QUERY_SYNTAX_MANIFEST_ID:
        raise AcmFieldExecutionError("unexpected ACM query-syntax manifest ID")
    if manifest.get("manifest_version") != QUERY_SYNTAX_MANIFEST_VERSION:
        raise AcmFieldExecutionError("unsupported ACM query-syntax manifest version")
    if manifest.get("execution_method_commit") != EXECUTION_METHOD_COMMIT:
        raise AcmFieldExecutionError("ACM execution-method commit changed")
    _validate_relative_path(manifest["source_directory"])
    if manifest.get("ignored_raw_artifacts") is not True:
        raise AcmFieldExecutionError("ACM raw operator artifacts must remain ignored")
    expected_storage = {
        "status": "unresolved",
        "locator": None,
        "repository_policy": "raw operator artifacts are under the ignored artifacts directory",
        "limitation": "checksums detect mutation but do not preserve ignored artifact bytes",
    }
    if manifest.get("immutable_artifact_storage") != expected_storage:
        raise AcmFieldExecutionError("ACM immutable artifact-storage limitation changed")
    if manifest.get("operator_evidence_limitations") != OPERATOR_EVIDENCE_LIMITATIONS:
        raise AcmFieldExecutionError("ACM operator-evidence limitations changed")
    if manifest.get("production_side_effects") != QUERY_SYNTAX_PRODUCTION_SIDE_EFFECTS:
        raise AcmFieldExecutionError("ACM query-syntax evidence created production effects")

    root_path = Path(root).resolve()
    contract_ref = manifest["contract"]
    _validate_relative_path(contract_ref["path"])
    contract_path = root_path / contract_ref["path"]
    contract_raw = contract_path.read_bytes()
    if len(contract_raw) != contract_ref["byte_size"]:
        raise AcmFieldExecutionError("ACM contract byte size changed")
    if _sha256_bytes(contract_raw) != contract_ref["raw_sha256"]:
        raise AcmFieldExecutionError("ACM contract raw hash changed")
    contract = load_acm_field_execution_contract(contract_path, root=root_path)
    if contract_ref["version"] != contract["contract_version"]:
        raise AcmFieldExecutionError("ACM contract version changed")
    if contract_ref["contract_hash"] != contract["contract_hash"]:
        raise AcmFieldExecutionError("ACM contract canonical hash changed")

    expected_families = {family["family_id"]: family for family in contract["families"]}
    actual_families = manifest.get("families", [])
    if len(actual_families) != len(expected_families) or {
        family["family_id"] for family in actual_families
    } != set(expected_families):
        raise AcmFieldExecutionError("ACM query-syntax manifest family coverage changed")
    accepted_count = 0
    valid_attempt_count = 0
    invalid_attempt_count = 0
    artifact_count = 0
    artifact_path_bindings: dict[str, str] = {}
    for family_item in actual_families:
        family = expected_families.get(family_item["family_id"])
        if family is None or family_item["parent_query_id"] != family["parent_query_id"]:
            raise AcmFieldExecutionError("ACM query-syntax manifest family binding changed")
        expected_children = {child["child_query_id"]: child for child in family["children"]}
        child_items = family_item.get("children", [])
        if len(child_items) != len(expected_children) or {
            child["child_query_id"] for child in child_items
        } != set(expected_children):
            raise AcmFieldExecutionError("ACM query-syntax manifest child coverage changed")
        for child_item in child_items:
            child = expected_children.get(child_item["child_query_id"])
            if child is None:
                raise AcmFieldExecutionError("ACM query-syntax manifest child binding changed")
            if (child_item["field_key"], child_item["field_label"]) != (
                child["field_key"],
                child["field_label"],
            ):
                raise AcmFieldExecutionError("ACM query-syntax manifest field binding changed")
            attempts = child_item.get("attempts", [])
            if not attempts:
                raise AcmFieldExecutionError("ACM query-syntax child must retain every attempt")
            expected_numbers = list(range(1, len(attempts) + 1))
            if [attempt["attempt_number"] for attempt in attempts] != expected_numbers:
                raise AcmFieldExecutionError("ACM query-syntax attempt numbers must be contiguous")
            for index, attempt in enumerate(attempts):
                number = index + 1
                expected_prior = number - 1 if number > 1 else None
                expected_next = number + 1 if number < len(attempts) else None
                if attempt["supersedes_attempt_number"] != expected_prior:
                    raise AcmFieldExecutionError("ACM query-syntax supersession history is broken")
                if attempt["superseded_by_attempt_number"] != expected_next:
                    raise AcmFieldExecutionError("ACM query-syntax supersession history is broken")
                if (
                    attempt["family_id"] != family["family_id"]
                    or attempt["child_query_id"] != child["child_query_id"]
                    or attempt["field_key"] != child["field_key"]
                    or attempt["field_label"] != child["field_label"]
                ):
                    raise AcmFieldExecutionError("ACM query-syntax attempt binding changed")
                artifact = attempt["artifact"]
                _validate_relative_path(artifact["relative_path"])
                _validate_sha256(artifact["raw_sha256"], "ACM query-syntax artifact hash")
                if artifact["byte_size"] < 0:
                    raise AcmFieldExecutionError("ACM query-syntax artifact size is invalid")
                availability = artifact.get("availability")
                if availability not in {"AVAILABLE", "OVERWRITTEN_UNAVAILABLE"}:
                    raise AcmFieldExecutionError("ACM query-syntax artifact availability is invalid")
                prior_binding = artifact_path_bindings.get(artifact["relative_path"])
                if prior_binding is not None and prior_binding != child["child_query_id"]:
                    raise AcmFieldExecutionError(
                        "ACM query-syntax artifact path is reused by another child"
                    )
                artifact_path_bindings[artifact["relative_path"]] = child["child_query_id"]
                if availability == "OVERWRITTEN_UNAVAILABLE" and expected_next is None:
                    raise AcmFieldExecutionError(
                        "unavailable ACM query-syntax bytes require a superseding attempt"
                    )
                artifact_count += 1
                validation = attempt["validation"]
                checks = validation.get("checks", {})
                expected_check_keys = {
                    "query_name_matches_child",
                    "exactly_one_expected_field_wrapper",
                    "no_other_field_wrapper",
                    "exact_frozen_scientific_expression",
                    "required_collection_filter",
                    "ui_count_parsed",
                    "timestamp_normalized_to_utc",
                }
                if set(checks) != expected_check_keys or not all(
                    isinstance(value, bool) for value in checks.values()
                ):
                    raise AcmFieldExecutionError("ACM query-syntax validation checks changed")
                if not isinstance(attempt["parsed_ui_reported_count"], int) or (
                    attempt["parsed_ui_reported_count"] < 0
                ):
                    raise AcmFieldExecutionError("ACM query-syntax UI count is invalid")
                if attempt["normalized_search_timestamp_utc"] != normalize_acm_search_timestamp(
                    attempt["acm_search_run_date_verbatim"]
                ):
                    raise AcmFieldExecutionError("ACM normalized search timestamp changed")
                if validation["state"] == QUERY_SYNTAX_VALID:
                    if not all(checks.values()) or validation["reason"] is not None:
                        raise AcmFieldExecutionError("ACM valid syntax state is inconsistent")
                    valid_attempt_count += 1
                elif validation["state"] == QUERY_SYNTAX_INVALID:
                    if all(checks.values()) or not validation["reason"]:
                        raise AcmFieldExecutionError("ACM invalid syntax state is inconsistent")
                    invalid_attempt_count += 1
                else:
                    raise AcmFieldExecutionError("unsupported ACM query-syntax validation state")
                if verify_artifacts and availability == "AVAILABLE":
                    artifact_path = root_path / artifact["relative_path"]
                    raw = artifact_path.read_bytes()
                    if len(raw) != artifact["byte_size"]:
                        raise AcmFieldExecutionError("ACM query-syntax artifact byte size changed")
                    if _sha256_bytes(raw) != artifact["raw_sha256"]:
                        raise AcmFieldExecutionError("ACM query-syntax artifact hash changed")
                    rows = parse_acm_query_syntax_csv(raw)
                    if len(rows) != 1:
                        raise AcmFieldExecutionError(
                            "ACM query-syntax artifact must contain exactly one evidence row"
                        )
                    evidence = rows[0]
                    expected_names = _expected_query_names(
                        _family_code(family["family_id"]), child["field_key"]
                    )
                    reproduced = _syntax_validation_result(
                        evidence, child, expected_query_names=expected_names
                    )
                    observed_fields = {
                        "query_name": evidence.query_name,
                        "acm_search_run_date_verbatim": evidence.search_run_date,
                        "normalized_search_timestamp_utc": normalize_acm_search_timestamp(
                            evidence.search_run_date
                        ),
                        "parsed_ui_reported_count": evidence.reported_count,
                        "validation": reproduced,
                    }
                    if any(attempt[key] != value for key, value in observed_fields.items()):
                        raise AcmFieldExecutionError(
                            "ACM query-syntax manifest does not match raw artifact"
                        )

            accepted = child_item["accepted_attempt_number"]
            latest = attempts[-1]
            if latest["validation"]["state"] == QUERY_SYNTAX_VALID:
                if accepted != latest["attempt_number"]:
                    raise AcmFieldExecutionError("ACM accepted attempt must be latest valid attempt")
                if child_item["completion_state"] != QUERY_SYNTAX_COMPLETE:
                    raise AcmFieldExecutionError("ACM valid child completion state changed")
                accepted_count += 1
            else:
                if accepted is not None or child_item["completion_state"] != (
                    "REQUIRES_CORRECTED_ATTEMPT"
                ):
                    raise AcmFieldExecutionError("ACM invalid child cannot have an accepted attempt")

    expected_child_count = sum(len(family["children"]) for family in contract["families"])
    summary = {
        "expected_child_count": expected_child_count,
        "observed_artifact_count": artifact_count,
        "valid_attempt_count": valid_attempt_count,
        "invalid_attempt_count": invalid_attempt_count,
        "all_children_have_accepted_attempt": accepted_count == expected_child_count,
    }
    if manifest.get("validation_summary") != summary:
        raise AcmFieldExecutionError("ACM query-syntax validation summary changed")
    expected_status = (
        QUERY_SYNTAX_COMPLETE
        if accepted_count == expected_child_count
        else QUERY_SYNTAX_INCOMPLETE
    )
    if manifest.get("manifest_status") != expected_status:
        raise AcmFieldExecutionError("ACM query-syntax manifest readiness is inconsistent")


def load_acm_query_syntax_manifest(
    path: str | Path,
    *,
    root: str | Path,
    verify_artifacts: bool = False,
) -> dict[str, Any]:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_acm_query_syntax_manifest(
        manifest, root=root, verify_artifacts=verify_artifacts
    )
    return manifest


def query_syntax_manifest_json(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def _bound_file(path: Path, root: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "relative_path": path.relative_to(root).as_posix(),
        "byte_size": len(raw),
        "raw_sha256": _sha256_bytes(raw),
    }


def _stable_identity_components(
    rows: Iterable[dict[str, str | None]],
) -> tuple[tuple[str, ...], dict[str, set[str]]]:
    parent: dict[str, str] = {}

    def find(token: str) -> str:
        parent.setdefault(token, token)
        while parent[token] != token:
            parent[token] = parent[parent[token]]
            token = parent[token]
        return token

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        first, second = sorted((left_root, right_root))
        parent[second] = first

    material = list(rows)
    row_tokens: list[tuple[str, tuple[str, ...]]] = []
    for row in material:
        tokens: list[str] = []
        if row["native_id"]:
            tokens.append(f"acm:{row['native_id']}")
        normalized_doi = normalize_doi(row["doi"])
        if normalized_doi:
            tokens.append(f"doi:{normalized_doi}")
        if not tokens:
            raise AcmFieldExecutionError("final ACM evidence contains no stable identity")
        for token in tokens:
            find(token)
        for token in tokens[1:]:
            union(tokens[0], token)
        row_tokens.append((str(row["artifact_path"]), tuple(tokens)))

    components: dict[str, set[str]] = {}
    component_artifacts: dict[str, set[str]] = {}
    for artifact_path, tokens in row_tokens:
        root_token = find(tokens[0])
        components.setdefault(root_token, set()).update(tokens)
        component_artifacts.setdefault(root_token, set()).add(artifact_path)
    canonical = tuple(
        sorted(
            next(
                (token for token in sorted(tokens) if token.startswith("acm:")),
                min(tokens),
            )
            for tokens in components.values()
        )
    )
    artifacts_by_canonical = {
        next(
            (token for token in sorted(components[root_token]) if token.startswith("acm:")),
            min(components[root_token]),
        ): paths
        for root_token, paths in component_artifacts.items()
    }
    return canonical, artifacts_by_canonical


def _diagnostic_bibtex_evidence(
    path: Path, root: Path
) -> tuple[dict[str, Any], list[dict[str, str | None]]]:
    raw = path.read_bytes()
    result: BibtexParseResult = parse_bibtex_with_diagnostics(raw.decode("utf-8"))
    if result.accounted_record_count != result.physical_header_count:
        raise AcmFieldExecutionError("ACM physical and parser record accounting diverged")

    rows: list[dict[str, str | None]] = []
    years: list[int] = []
    year_counts: dict[str, int] = {}
    native_ids: set[str] = set()
    normalized_dois: set[str] = set()
    missing_year_count = 0
    missing_identity_count = 0
    for fields in result.entries:
        native_id = fields.get("_key")
        doi = normalize_doi(fields.get("doi"))
        if native_id:
            native_ids.add(native_id)
        if doi:
            normalized_dois.add(doi)
        if not native_id and not doi:
            missing_identity_count += 1
        year = fields.get("year")
        if year and year.isdigit():
            numeric_year = int(year)
            years.append(numeric_year)
            year_counts[year] = year_counts.get(year, 0) + 1
        else:
            missing_year_count += 1
        rows.append(
            {
                "artifact_path": path.relative_to(root).as_posix(),
                "native_id": native_id,
                "doi": doi,
                "field_key": None,
            }
        )
    issues = []
    for issue in result.issues:
        native_id = issue.key
        doi = normalize_doi(issue.partial_fields.get("doi"))
        if native_id:
            native_ids.add(native_id)
        if doi:
            normalized_dois.add(doi)
        if not native_id and not doi:
            missing_identity_count += 1
        year = issue.partial_fields.get("year")
        if year and year.isdigit():
            numeric_year = int(year)
            years.append(numeric_year)
            year_counts[year] = year_counts.get(year, 0) + 1
        else:
            missing_year_count += 1
        rows.append(
            {
                "artifact_path": path.relative_to(root).as_posix(),
                "native_id": native_id,
                "doi": doi,
                "field_key": None,
            }
        )
        issues.append(
            {
                "ordinal": issue.ordinal,
                "code": issue.code,
                "native_id": native_id,
                "brace_depth": issue.brace_depth,
            }
        )
    if missing_identity_count:
        raise AcmFieldExecutionError("ACM final retrieval contains missing stable identities")
    stable_identities, _ = _stable_identity_components(rows)
    evidence = {
        **_bound_file(path, root),
        "physical_header_count": result.physical_header_count,
        "successfully_parsed_entry_count": len(result.entries),
        "malformed_entry_count": len(result.issues),
        "total_accounted_entry_count": result.accounted_record_count,
        "unique_native_id_count": len(native_ids),
        "unique_normalized_doi_count": len(normalized_dois),
        "unique_stable_identity_count": len(stable_identities),
        "stable_identity_digest_sha256": _sha256_text("\n".join(stable_identities)),
        "missing_stable_identity_count": missing_identity_count,
        "publication_year_minimum": min(years) if years else None,
        "publication_year_maximum": max(years) if years else None,
        "publication_year_counts": dict(sorted(year_counts.items())),
        "missing_or_unparseable_year_count": missing_year_count,
        "parse_issues": issues,
    }
    return evidence, rows


def _referenced_manifest(path: Path, root: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    reference = _bound_file(path, root)
    for key in ("manifest_id", "manifest_version", "manifest_hash"):
        if key in payload:
            reference[key] = payload[key]
    for key in ("contract_id", "contract_version", "contract_hash"):
        if key in payload:
            reference[key] = payload[key]
    return reference


def build_acm_final_reconciliation_manifest(*, root: str | Path) -> dict[str, Any]:
    """Build the final offline ACM retrieval-evidence reconciliation.

    The strict prospective partition contract remains unchanged.  This retrospective
    audit distinguishes the retrieved artifact set from later observations of ACM's
    mutable index; later counts are never substituted into execution-time gates.
    """

    root_path = Path(root).resolve()
    bibtex_root = root_path / "artifacts/acm_field_execution/2026-09-02/bibtex"
    screenshot_root = (
        root_path / "artifacts/acm_field_execution/2026-09-03/ui_count_evidence"
    )
    field_contract_path = root_path / "config/star_acm_field_execution_contract_v1.json"
    partition_contract_path = root_path / "config/star_acm_export_partition_contract_v1.json"
    plan_path = root_path / "config/star_production_query_plan_v1.json"
    syntax_manifest_path = (
        root_path
        / "provenance/star_acm_field_execution_2026-09-02_query_syntax_manifest.json"
    )
    calibration_manifest_path = (
        root_path
        / "provenance/star_acm_field_execution_2026-09-02_bulk_export_calibration_manifest.json"
    )
    field_contract = load_acm_field_execution_contract(field_contract_path, root=root_path)
    load_acm_export_partition_contract(partition_contract_path, root=root_path)
    syntax_manifest = load_acm_query_syntax_manifest(
        syntax_manifest_path, root=root_path, verify_artifacts=True
    )
    syntax_children = {
        child["child_query_id"]: child
        for family in syntax_manifest["families"]
        for child in family["children"]
    }

    selected_paths = {
        path
        for fields in FINAL_SELECTED_EXPORTS.values()
        for paths in fields.values()
        for path in paths
    }
    nonselected_paths = {item[0] for item in FINAL_NONSELECTED_EXPORTS}
    observed_paths = {
        path.relative_to(bibtex_root).as_posix() for path in bibtex_root.rglob("*.bib")
    }
    if selected_paths | nonselected_paths != observed_paths:
        raise AcmFieldExecutionError("final ACM BibTeX inventory classification is incomplete")
    if selected_paths & nonselected_paths:
        raise AcmFieldExecutionError("an ACM BibTeX artifact has conflicting classifications")

    parsed_cache: dict[
        str, tuple[dict[str, Any], list[dict[str, str | None]]]
    ] = {}

    def parsed(relative_path: str) -> tuple[dict[str, Any], list[dict[str, str | None]]]:
        if relative_path not in parsed_cache:
            parsed_cache[relative_path] = _diagnostic_bibtex_evidence(
                bibtex_root / relative_path, root_path
            )
        return parsed_cache[relative_path]

    families: list[dict[str, Any]] = []
    for family in field_contract["families"]:
        family_code = _family_code(family["family_id"])
        child_results: list[dict[str, Any]] = []
        parent_rows: list[dict[str, str | None]] = []
        for child in family["children"]:
            field_key = child["field_key"]
            paths = FINAL_SELECTED_EXPORTS[family_code][field_key]
            ranges = FINAL_ABSTRACT_RANGES[family_code] if field_key == "abstract" else ()
            if ranges and len(ranges) != len(paths):
                raise AcmFieldExecutionError("ACM final partition path/range count differs")
            if ranges and any(
                current[1] + 1 != following[0]
                for current, following in pairwise(ranges)
            ):
                raise AcmFieldExecutionError(
                    "ACM final publication-year ranges are not disjoint and exhaustive"
                )
            artifact_items: list[dict[str, Any]] = []
            rows: list[dict[str, str | None]] = []
            for index, relative_path in enumerate(paths):
                artifact, artifact_rows = parsed(relative_path)
                if artifact["total_accounted_entry_count"] > ACM_EXPORT_RECORD_CEILING:
                    raise AcmFieldExecutionError(
                        "selected ACM artifact exceeds the demonstrated export ceiling"
                    )
                item = dict(artifact)
                item["classification"] = "SELECTED_RETRIEVAL_ARTIFACT"
                item["operator_label_not_authoritative"] = True
                if ranges:
                    item["operator_publication_date_filter"] = {
                        "from_year": ranges[index][0],
                        "to_year": ranges[index][1],
                        "inclusive": True,
                    }
                artifact_items.append(item)
                for row in artifact_rows:
                    copied = dict(row)
                    copied["field_key"] = field_key
                    rows.append(copied)
                    parent_rows.append(copied)
            identities, artifacts_by_identity = _stable_identity_components(rows)
            overlaps = sorted(
                identity
                for identity, artifact_paths in artifacts_by_identity.items()
                if len(artifact_paths) > 1
            )
            if overlaps:
                raise AcmFieldExecutionError(
                    "selected ACM year partitions overlap by stable identity"
                )
            syntax_child = syntax_children[child["child_query_id"]]
            accepted_number = syntax_child["accepted_attempt_number"]
            accepted_attempt = next(
                attempt
                for attempt in syntax_child["attempts"]
                if attempt["attempt_number"] == accepted_number
            )
            execution_count = accepted_attempt["parsed_ui_reported_count"]
            difference = len(identities) - execution_count
            comparison_state = (
                "RECONCILES"
                if difference == 0
                else "PROVIDER_INDEX_STATE_DISCREPANCY_NO_AFFIRMATIVE_FAILURE"
            )
            child_results.append(
                {
                    "child_query_id": child["child_query_id"],
                    "field_key": field_key,
                    "field_label": child["field_label"],
                    "frozen_child_query_sha256": child["child_query_sha256"],
                    "accepted_syntax_attempt_number": accepted_number,
                    "execution_time_provider_observation": {
                        "count": execution_count,
                        "observed_at_utc": accepted_attempt[
                            "normalized_search_timestamp_utc"
                        ],
                        "query_syntax_artifact_sha256": accepted_attempt["artifact"][
                            "raw_sha256"
                        ],
                    },
                    "selected_artifacts": artifact_items,
                    "retrieved_set": {
                        "total_accounted_record_count": sum(
                            item["total_accounted_entry_count"]
                            for item in artifact_items
                        ),
                        "malformed_record_count": sum(
                            item["malformed_entry_count"] for item in artifact_items
                        ),
                        "unique_stable_identity_count": len(identities),
                        "stable_identity_union_digest_sha256": _sha256_text(
                            "\n".join(identities)
                        ),
                        "cross_artifact_stable_identity_overlap_count": len(overlaps),
                    },
                    "count_comparison": {
                        "retrieved_minus_execution_observation": difference,
                        "state": comparison_state,
                        "blocks_retrieval_completeness": False,
                    },
                    "retrieval_completeness_state": "COMPLETE_RETRIEVED_SET",
                }
            )

        parent_identities, _ = _stable_identity_components(parent_rows)
        overlap_counts = {
            "title_only": 0,
            "keyword_only": 0,
            "abstract_only": 0,
            "title_keyword": 0,
            "title_abstract": 0,
            "keyword_abstract": 0,
            "triple_overlap": 0,
        }
        identity_fields: dict[str, set[str]] = {}
        # All preserved records have native IDs, so map each field row through its
        # canonical ACM token. DOI fallback remains available in component creation.
        for row in parent_rows:
            native = row["native_id"]
            if native:
                identity_fields.setdefault(f"acm:{native}", set()).add(
                    str(row["field_key"])
                )
        overlap_names = {
            frozenset({"title"}): "title_only",
            frozenset({"keyword"}): "keyword_only",
            frozenset({"abstract"}): "abstract_only",
            frozenset({"title", "keyword"}): "title_keyword",
            frozenset({"title", "abstract"}): "title_abstract",
            frozenset({"keyword", "abstract"}): "keyword_abstract",
            frozenset({"title", "keyword", "abstract"}): "triple_overlap",
        }
        for fields in identity_fields.values():
            overlap_counts[overlap_names[frozenset(fields)]] += 1
        if sum(overlap_counts.values()) != len(parent_identities):
            raise AcmFieldExecutionError("ACM parent field-overlap accounting diverged")
        families.append(
            {
                "family_id": family["family_id"],
                "parent_query_id": family["parent_query_id"],
                "children": child_results,
                "field_union": {
                    "unique_stable_identity_count": len(parent_identities),
                    "stable_identity_union_digest_sha256": _sha256_text(
                        "\n".join(parent_identities)
                    ),
                    "overlap_counts": overlap_counts,
                    "state": "COMPLETE_SET_RECONCILED_NOT_IMPORTED",
                },
            }
        )

    nonselected = []
    for relative_path, classification, reason in FINAL_NONSELECTED_EXPORTS:
        artifact, rows = parsed(relative_path)
        item = dict(artifact)
        item.update({"classification": classification, "reason": reason})
        if relative_path.endswith("QF01_keyword_000001-000020.bib"):
            selected_rows = parsed("QF01/keyword/QF01_keyword_000001-000028.bib")[1]
            old_ids, _ = _stable_identity_components(rows)
            new_ids, _ = _stable_identity_components(selected_rows)
            item["stable_identity_set_relationship"] = "STRICT_SUBSET_OF_SELECTED_EXPORT"
            item["selected_export_additional_identity_count"] = len(
                set(new_ids) - set(old_ids)
            )
            item["affirmative_operator_or_export_failure"] = True
            item["failure_resolved_by_superseding_artifact"] = True
        elif relative_path.endswith("QF02_abstract_2025-2025_SHORT.bib"):
            selected_rows = parsed("QF02/abstract/QF02_abstract_2025-2026.bib")[1]
            old_ids, _ = _stable_identity_components(rows)
            new_ids, _ = _stable_identity_components(selected_rows)
            item["stable_identity_set_relationship"] = (
                "IDENTICAL_TO_SELECTED_EXPORT"
                if old_ids == new_ids
                else "DIFFERS_FROM_SELECTED_EXPORT"
            )
            item["affirmative_operator_or_export_failure"] = False
        else:
            item["affirmative_operator_or_export_failure"] = False
        nonselected.append(item)

    screenshots = []
    for (
        relative_path,
        family_code,
        candidate_codes,
        from_year,
        to_year,
        count,
        observed_at,
    ) in FINAL_SCREENSHOT_OBSERVATIONS:
        path = screenshot_root / relative_path
        item = {
            **_bound_file(path, root_path),
            "evidence_role": "SUBSEQUENT_PROVIDER_VERIFICATION_OBSERVATION",
            "classification_method": "manual_visual_inspection",
            "filename_used_as_authoritative_metadata": False,
            "family_id": (
                next(
                    family["family_id"]
                    for family in field_contract["families"]
                    if _family_code(family["family_id"]) == family_code
                )
                if family_code
                else None
            ),
            "candidate_family_codes": list(candidate_codes),
            "field_key": "abstract",
            "publication_date_filter": (
                {"state": "ALL_DATES", "from_year": None, "to_year": None}
                if from_year is None
                else {
                    "state": "FILTERED_INCLUSIVE",
                    "from_year": from_year,
                    "to_year": to_year,
                }
            ),
            "displayed_result_count": count,
            "observed_at_utc": observed_at,
            "family_mapping_state": "UNAMBIGUOUS" if family_code else "AMBIGUOUS",
            "used_as_retrieval_completeness_gate": False,
        }
        screenshots.append(item)

    manifest: dict[str, Any] = {
        "schema_version": FINAL_RECONCILIATION_SCHEMA_VERSION,
        "manifest_id": FINAL_RECONCILIATION_MANIFEST_ID,
        "manifest_version": FINAL_RECONCILIATION_MANIFEST_VERSION,
        "status": ACM_RETRIEVAL_EVIDENCE_COMPLETE,
        "reconciled_at_utc": ACM_FINAL_RECONCILIATION_AT_UTC,
        "methodology": {
            "search_specification": (
                "frozen production query expressions plus accepted ACM field syntax"
            ),
            "retrieved_record_set": (
                "selected raw BibTeX artifacts reconciled by stable identity"
            ),
            "prospective_same_snapshot_partition_gates_preserved": True,
            "later_provider_observations_are_completeness_gates": False,
            "provider_index_temporal_invariance_required": False,
            "count_differences_without_affirmative_failure": (
                "documented provider/index-state discrepancies"
            ),
            "affirmative_operator_or_export_failure_blocks_selection": True,
            "export_order_used_for_completeness": False,
        },
        "bindings": {
            "production_query_plan": _referenced_manifest(plan_path, root_path),
            "field_execution_contract": _referenced_manifest(
                field_contract_path, root_path
            ),
            "export_partition_contract": _referenced_manifest(
                partition_contract_path, root_path
            ),
            "query_syntax_manifest": _referenced_manifest(
                syntax_manifest_path, root_path
            ),
            "bulk_export_calibration_manifest": _referenced_manifest(
                calibration_manifest_path, root_path
            ),
            "diagnostic_bibtex_parser_commit": (
                "9f7c2e808631fe803e9b98050d77c54e23938f46"
            ),
        },
        "families": families,
        "nonselected_preserved_bibtex_artifacts": nonselected,
        "subsequent_verification_screenshots": screenshots,
        "limitations": {
            "operator_identity": "NOT_RECORDED",
            "institutional_access_tier": "NOT_RECORDED",
            "exact_bibtex_export_timestamps": "NOT_RECOVERABLE",
            "ambiguous_screenshot_family_count": sum(
                item["family_mapping_state"] == "AMBIGUOUS" for item in screenshots
            ),
            "limitations_block_retrieved_set_completeness": False,
        },
        "readiness": {
            "acm_retrieval_evidence_complete": True,
            "manual_acm_interaction_required": False,
            "production_import_performed": False,
            "global_production_wave_ready": False,
            "next_acm_transition": "PRODUCTION_ARTIFACT_IMPORT_WHEN_AUTHORIZED",
        },
        "production_side_effects": {
            "retrieval_run_created": False,
            "record_occurrence_created": False,
            "screening_executed": False,
            "prisma_generated": False,
            "e6_derived": False,
            "llm_executed": False,
            "corpus_membership_created": False,
        },
    }
    if len(families) != 5 or sum(len(item["children"]) for item in families) != 15:
        raise AcmFieldExecutionError("final ACM reconciliation must cover 15 field children")
    if any(manifest["production_side_effects"].values()):
        raise AcmFieldExecutionError("final ACM reconciliation created production state")
    manifest["manifest_hash"] = _sha256_text(_canonical_json(manifest))
    return manifest


def validate_acm_final_reconciliation_manifest(
    manifest: dict[str, Any], *, root: str | Path, verify_artifacts: bool = False
) -> None:
    material = dict(manifest)
    claimed_hash = material.pop("manifest_hash", None)
    if claimed_hash != _sha256_text(_canonical_json(material)):
        raise AcmFieldExecutionError("ACM final reconciliation manifest hash mismatch")
    if manifest.get("schema_version") != FINAL_RECONCILIATION_SCHEMA_VERSION:
        raise AcmFieldExecutionError("unsupported ACM final reconciliation schema")
    if manifest.get("manifest_id") != FINAL_RECONCILIATION_MANIFEST_ID:
        raise AcmFieldExecutionError("unexpected ACM final reconciliation manifest ID")
    if manifest.get("status") != ACM_RETRIEVAL_EVIDENCE_COMPLETE:
        raise AcmFieldExecutionError("ACM final retrieval evidence is not complete")
    if not manifest.get("readiness", {}).get("acm_retrieval_evidence_complete"):
        raise AcmFieldExecutionError("ACM final readiness state is inconsistent")
    if manifest["readiness"].get("production_import_performed"):
        raise AcmFieldExecutionError("ACM reconciliation cannot perform production import")
    if any(manifest.get("production_side_effects", {}).values()):
        raise AcmFieldExecutionError("ACM final reconciliation created production state")
    if verify_artifacts:
        rebuilt = build_acm_final_reconciliation_manifest(root=root)
        if manifest != rebuilt:
            raise AcmFieldExecutionError(
                "ACM final reconciliation does not match preserved raw evidence"
            )


def load_acm_final_reconciliation_manifest(
    path: str | Path, *, root: str | Path, verify_artifacts: bool = False
) -> dict[str, Any]:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_acm_final_reconciliation_manifest(
        manifest, root=root, verify_artifacts=verify_artifacts
    )
    return manifest


def final_reconciliation_manifest_json(manifest: dict[str, Any]) -> str:
    return json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def build_acm_parent_union(
    family_spec: dict[str, Any], field_exports: Iterable[AcmFieldExportEvidence]
) -> AcmParentUnionResult:
    """Validate three complete field exports and build their deterministic identity union."""

    child_specs = {item["child_query_id"]: item for item in family_spec["children"]}
    exports = list(field_exports)
    if len(exports) != len(child_specs) or {item.child_query_id for item in exports} != set(
        child_specs
    ):
        raise AcmFieldExecutionError("ACM parent requires exactly its three field exports")
    for item in exports:
        item.validate(child_specs[item.child_query_id])

    hits = [hit for export in exports for hit in export.hits]
    parent: dict[str, str] = {}

    def find(token: str) -> str:
        parent.setdefault(token, token)
        while parent[token] != token:
            parent[token] = parent[parent[token]]
            token = parent[token]
        return token

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        first, second = sorted((left_root, right_root))
        parent[second] = first

    for hit in hits:
        tokens = hit.identity_tokens()
        for token in tokens:
            find(token)
        for token in tokens[1:]:
            union(tokens[0], token)

    components: dict[str, dict[str, Any]] = {}
    for hit in hits:
        tokens = hit.identity_tokens()
        root = find(tokens[0])
        component = components.setdefault(root, {"tokens": set(), "fields": set(), "hits": []})
        component["tokens"].update(tokens)
        component["fields"].add(hit.field_key)
        component["hits"].append(
            {
                "child_query_id": hit.child_query_id,
                "field_key": hit.field_key,
                "row_ordinal": hit.row_ordinal,
                "raw_entry_sha256": hit.raw_entry_sha256,
            }
        )

    overlap_keys = {
        frozenset({"title"}): "title_only",
        frozenset({"keyword"}): "keyword_only",
        frozenset({"abstract"}): "abstract_only",
        frozenset({"title", "keyword"}): "title_keyword",
        frozenset({"title", "abstract"}): "title_abstract",
        frozenset({"keyword", "abstract"}): "keyword_abstract",
        frozenset({"title", "keyword", "abstract"}): "triple_overlap",
    }
    overlap_counts = {value: 0 for value in overlap_keys.values()}
    members: list[dict[str, Any]] = []
    for component in components.values():
        fields = frozenset(component["fields"])
        overlap_counts[overlap_keys[fields]] += 1
        tokens = sorted(component["tokens"])
        canonical = next((token for token in tokens if token.startswith("acm:")), tokens[0])
        members.append(
            {
                "canonical_identity": canonical,
                "identity_tokens": tokens,
                "fields": sorted(fields),
                "raw_hits": sorted(
                    component["hits"],
                    key=lambda item: (
                        item["field_key"], item["row_ordinal"], item["raw_entry_sha256"]
                    ),
                ),
            }
        )
    members.sort(key=lambda item: item["canonical_identity"])
    per_field = {
        field_key: sum(1 for hit in hits if hit.field_key == field_key)
        for field_key, _ in FIELDS
    }
    payload = {
        "parent_query_id": family_spec["parent_query_id"],
        "parent_query_hash": family_spec["parent_query_hash"],
        "complete": True,
        "child_query_ids": tuple(item["child_query_id"] for item in family_spec["children"]),
        "per_field_raw_counts": per_field,
        "raw_hit_total": len(hits),
        "unique_union_count": len(members),
        "overlap_counts": overlap_counts,
        "members": tuple(members),
    }
    result = AcmParentUnionResult(
        **payload,
        union_hash=_sha256_text(_canonical_json(payload)),
    )
    result.validate()
    return result


def build_acm_field_execution_contract(
    production_plan_path: str | Path, *, root: str | Path
) -> dict[str, Any]:
    root_path = Path(root)
    plan_path = Path(production_plan_path)
    plan = load_production_query_plan(plan_path, root=root_path)
    if plan.plan_hash() != EXPECTED_PLAN_HASH:
        raise AcmFieldExecutionError("unexpected production plan canonical hash")
    if _sha256_bytes(plan_path.read_bytes()) != EXPECTED_PLAN_RAW_SHA256:
        raise AcmFieldExecutionError("unexpected production plan raw hash")

    families: list[dict[str, Any]] = []
    acm_queries = [
        item for item in plan.payload["source_queries"] if item["source"] == "ACMDigitalLibrary"
    ]
    for parent in acm_queries:
        children = []
        for field_key, field_label in FIELDS:
            rendered = field_query(field_label, parent["query_text"])
            children.append(
                {
                    "child_query_id": child_query_id(parent["query_id"], field_key),
                    "field_key": field_key,
                    "field_label": field_label,
                    "child_query_text": rendered,
                    "child_query_sha256": _sha256_text(rendered),
                    "execution_status": "REQUIRES_OPERATOR_INPUT",
                    "request_specification": {
                        "workflow": "advanced_search",
                        "scope": ACM_SCOPE,
                        "collection": ACM_COLLECTION,
                        "field": field_label,
                        "filters": {},
                        "sort": ACM_SORT,
                        "transport": "human_ui_artifact_import",
                    },
                    "operator_evidence": {
                        "export_query_syntax_csv_required": True,
                        "exact_single_field_wrapper_required": True,
                        "ui_reported_count_required": True,
                        "utc_timestamp_required": True,
                        "query_url_or_screenshot_evidence_required": True,
                    },
                    "export_artifact_references": [],
                    "completion": {
                        "citation_export_required": True,
                        "format": "BibTeX",
                        "contiguous_chunk_ranges_required": True,
                        "ui_total_reconciliation_required": True,
                    },
                }
            )
        families.append(
            {
                "family_id": parent["family_id"],
                "parent_query_id": parent["query_id"],
                "parent_query_hash": parent["request_specification_hash"],
                "scientific_query_text": parent["query_text"],
                "scientific_query_text_sha256": parent["query_text_sha256"],
                "parent_combined_semantics": (
                    "Title:(Q) OR Keyword:(Q) OR Abstract:(Q)"
                ),
                "decomposed_union_semantics": (
                    "union(Title:(Q), Keyword:(Q), Abstract:(Q))"
                ),
                "children": children,
                "parent_completion": {
                    "all_three_children_required": True,
                    "all_child_exports_reconciled_required": True,
                    "stable_identity_hierarchy": ["ACM native ID", "normalized DOI"],
                    "missing_stable_identity_blocks_union": True,
                    "overlap_categories": [
                        "title_only",
                        "keyword_only",
                        "abstract_only",
                        "title_keyword",
                        "title_abstract",
                        "keyword_abstract",
                        "triple_overlap",
                    ],
                },
            }
        )

    calibration_path = root_path / "artifacts/H2H_QF01_csv"
    calibration = {
        "kind": "historical_combined_query_calibration",
        "family_id": "STAR-QF01-RELATIONAL-VIS",
        "artifact_relative_path": "artifacts/H2H_QF01_csv",
        "artifact_raw_sha256": (
            _sha256_bytes(calibration_path.read_bytes()) if calibration_path.exists() else None
        ),
        "artifact_byte_size": calibration_path.stat().st_size if calibration_path.exists() else None,
        "artifact_git_status": "ignored_untracked",
        "search_run_date": "2026-09-02 at 17:26:03 PDT",
        "ui_reported_count": 1948,
        "query_form": "combined_three_field_or",
        "rewritten": False,
        "durable_storage": "unresolved",
    }
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "contract_version": CONTRACT_VERSION,
        "status": "PROSPECTIVE_NOT_EXECUTED",
        "production_plan": {
            "path": plan_path.relative_to(root_path).as_posix(),
            "version": plan.payload["plan_version"],
            "canonical_hash": plan.plan_hash(),
            "raw_sha256": _sha256_bytes(plan_path.read_bytes()),
        },
        "execution_contract": {
            "scope": ACM_SCOPE,
            "collection": ACM_COLLECTION,
            "exported_filter_syntax": ACM_EXPORT_FILTER,
            "filters": {},
            "sort": ACM_SORT,
            "transport": "human_ui_artifact_import",
            "browser_automation": False,
            "network_execution_performed": False,
            "scientific_query_change": False,
            "parent_result_definition": "deterministic_set_union_of_three_field_children",
        },
        "families": families,
        "historical_calibration_evidence": [calibration],
        "production_side_effects": {
            "review_dataset_created": False,
            "retrieval_run_created": False,
            "source_query_created": False,
            "record_occurrence_created": False,
            "prisma_generated": False,
            "e6_derived": False,
            "screening_executed": False,
            "llm_executed": False,
            "corpus_membership_created": False,
        },
    }
    payload["contract_hash"] = _sha256_text(_canonical_json(payload))
    validate_acm_field_execution_contract(payload, root=root_path)
    return payload


def validate_acm_field_execution_contract(payload: dict[str, Any], *, root: str | Path) -> None:
    material = dict(payload)
    claimed_hash = material.pop("contract_hash", None)
    if claimed_hash != _sha256_text(_canonical_json(material)):
        raise AcmFieldExecutionError("ACM field execution contract hash mismatch")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise AcmFieldExecutionError("unsupported ACM field execution contract schema")
    if payload.get("status") != "PROSPECTIVE_NOT_EXECUTED":
        raise AcmFieldExecutionError("ACM field execution contract must remain prospective")
    plan_ref = payload["production_plan"]
    plan_path = Path(root) / plan_ref["path"]
    plan = load_production_query_plan(plan_path, root=root)
    if plan.plan_hash() != plan_ref["canonical_hash"] or plan.plan_hash() != EXPECTED_PLAN_HASH:
        raise AcmFieldExecutionError("ACM contract production-plan hash mismatch")
    if _sha256_bytes(plan_path.read_bytes()) != plan_ref["raw_sha256"]:
        raise AcmFieldExecutionError("ACM contract production-plan raw hash mismatch")
    if len(payload.get("families", [])) != 5:
        raise AcmFieldExecutionError("ACM contract must contain exactly five parent families")
    plan_queries = {
        item["query_id"]: item
        for item in plan.payload["source_queries"]
        if item["source"] == "ACMDigitalLibrary"
    }
    for family in payload["families"]:
        parent = plan_queries.get(family["parent_query_id"])
        if parent is None:
            raise AcmFieldExecutionError("ACM contract contains an unknown parent query")
        if family["parent_query_hash"] != parent["request_specification_hash"]:
            raise AcmFieldExecutionError("ACM parent query hash changed")
        if family["scientific_query_text"] != parent["query_text"]:
            raise AcmFieldExecutionError("ACM scientific query text changed")
        if family["scientific_query_text_sha256"] != parent["query_text_sha256"]:
            raise AcmFieldExecutionError("ACM scientific query text hash changed")
        if len(family.get("children", [])) != 3:
            raise AcmFieldExecutionError("ACM parent must define exactly three children")
        for child, (field_key, field_label) in zip(family["children"], FIELDS, strict=True):
            expected_id = child_query_id(parent["query_id"], field_key)
            expected_query = field_query(field_label, parent["query_text"])
            if child["child_query_id"] != expected_id:
                raise AcmFieldExecutionError("ACM child-query ID changed")
            if child["field_key"] != field_key or child["field_label"] != field_label:
                raise AcmFieldExecutionError("ACM child field changed")
            if child["child_query_text"] != expected_query:
                raise AcmFieldExecutionError("ACM child query changed")
            if child["child_query_sha256"] != _sha256_text(expected_query):
                raise AcmFieldExecutionError("ACM child query hash mismatch")
            if child.get("request_specification") != {
                "workflow": "advanced_search",
                "scope": ACM_SCOPE,
                "collection": ACM_COLLECTION,
                "field": field_label,
                "filters": {},
                "sort": ACM_SORT,
                "transport": "human_ui_artifact_import",
            }:
                raise AcmFieldExecutionError("ACM child execution settings changed")
    if any(payload.get("production_side_effects", {}).values()):
        raise AcmFieldExecutionError("ACM contract construction cannot create production state")


def load_acm_field_execution_contract(path: str | Path, *, root: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_acm_field_execution_contract(payload, root=root)
    return payload


def contract_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def build_acm_export_partition_contract(
    field_contract_path: str | Path, *, root: str | Path
) -> dict[str, Any]:
    """Build the subordinate, prospective ACM publication-year export contract."""

    root_path = Path(root).resolve()
    parent_path = Path(field_contract_path).resolve()
    parent_contract = load_acm_field_execution_contract(parent_path, root=root_path)
    model = {
        "partition_dimension": "Publication Date year",
        "range_semantics": "inclusive_from_and_to_year",
        "range_selection": "prospective_from_observed_provider_counts",
        "provider_supported_year_domain_must_be_recorded": True,
        "ranges_disjoint_required": True,
        "ranges_collectively_exhaustive_required": True,
        "maximum_partition_ui_count": ACM_EXPORT_RECORD_CEILING,
        "partition_ui_count_required_before_export_acceptance": True,
        "partition_ui_count_sum_must_equal_unfiltered_child_count": True,
        "count_sum_mismatch_state": "UNRESOLVED_UNDATED_OR_UNREPRESENTED_RECORDS",
        "parsed_bibtex_count_must_equal_partition_ui_count": True,
        "stable_identity_overlap_across_partitions_allowed": False,
        "unique_union_count_must_equal_unfiltered_child_count": True,
        "completeness_semantics": "set_based_stable_identity_union",
        "export_order_used_for_completeness": False,
        "operator_sort_may_be_recorded_as_metadata": True,
        "scientific_query_change": False,
        "eligibility_filter": False,
    }
    families = []
    for family in parent_contract["families"]:
        children = []
        for child in family["children"]:
            children.append(
                {
                    "child_query_id": child["child_query_id"],
                    "field_key": child["field_key"],
                    "field_label": child["field_label"],
                    "frozen_child_query_sha256": child["child_query_sha256"],
                    "unfiltered_count_evidence": "accepted_query_syntax_attempt_required",
                    "export_partition_model": dict(model),
                    "partition_plan_status": "REQUIRES_PROSPECTIVE_PROVIDER_COUNTS",
                    "partitions": [],
                }
            )
        families.append(
            {
                "family_id": family["family_id"],
                "parent_query_id": family["parent_query_id"],
                "children": children,
            }
        )
    payload: dict[str, Any] = {
        "schema_version": EXPORT_PARTITION_CONTRACT_SCHEMA_VERSION,
        "contract_id": EXPORT_PARTITION_CONTRACT_ID,
        "contract_version": EXPORT_PARTITION_CONTRACT_VERSION,
        "status": "PROSPECTIVE_NOT_EXECUTED",
        "parent_field_execution_contract": {
            "path": parent_path.relative_to(root_path).as_posix(),
            "version": parent_contract["contract_version"],
            "contract_hash": parent_contract["contract_hash"],
            "raw_sha256": _sha256_bytes(parent_path.read_bytes()),
            "byte_size": parent_path.stat().st_size,
        },
        "scope": {
            "purpose": "provider_export_transport_partitioning_only",
            "scientific_query_preserved_exactly": True,
            "publication_year_is_scientific_search_term": False,
            "publication_year_is_eligibility_filter": False,
        },
        "provider_constraint": {
            "empirically_observed_bulk_bibtex_ceiling": ACM_EXPORT_RECORD_CEILING,
            "positional_chunking_permitted": False,
            "publication_date_sort_reliable_for_completeness": False,
        },
        "families": families,
        "calibration_evidence_policy": {
            "manifest_path": (
                "provenance/"
                "star_acm_field_execution_2026-09-02_bulk_export_calibration_manifest.json"
            ),
            "production_partition_eligible": False,
            "raw_artifact_must_remain_unchanged": True,
        },
        "production_side_effects": {
            "field_execution_ready": False,
            "retrieval_run_created": False,
            "record_occurrence_created": False,
            "screening_executed": False,
            "prisma_generated": False,
            "e6_derived": False,
            "llm_executed": False,
            "corpus_membership_created": False,
        },
    }
    payload["contract_hash"] = _sha256_text(_canonical_json(payload))
    validate_acm_export_partition_contract(payload, root=root_path)
    return payload


def validate_acm_export_partition_contract(
    payload: dict[str, Any], *, root: str | Path
) -> None:
    material = dict(payload)
    claimed_hash = material.pop("contract_hash", None)
    if claimed_hash != _sha256_text(_canonical_json(material)):
        raise AcmFieldExecutionError("ACM export-partition contract hash mismatch")
    if payload.get("schema_version") != EXPORT_PARTITION_CONTRACT_SCHEMA_VERSION:
        raise AcmFieldExecutionError("unsupported ACM export-partition contract schema")
    if payload.get("contract_id") != EXPORT_PARTITION_CONTRACT_ID:
        raise AcmFieldExecutionError("unexpected ACM export-partition contract ID")
    if payload.get("contract_version") != EXPORT_PARTITION_CONTRACT_VERSION:
        raise AcmFieldExecutionError("unsupported ACM export-partition contract version")
    if payload.get("status") != "PROSPECTIVE_NOT_EXECUTED":
        raise AcmFieldExecutionError("ACM export-partition contract must remain prospective")

    root_path = Path(root).resolve()
    parent_ref = payload["parent_field_execution_contract"]
    _validate_relative_path(parent_ref["path"])
    parent_path = root_path / parent_ref["path"]
    raw = parent_path.read_bytes()
    if len(raw) != parent_ref["byte_size"] or _sha256_bytes(raw) != parent_ref["raw_sha256"]:
        raise AcmFieldExecutionError("parent ACM field contract bytes changed")
    parent_contract = load_acm_field_execution_contract(parent_path, root=root_path)
    if (
        parent_ref["version"] != parent_contract["contract_version"]
        or parent_ref["contract_hash"] != parent_contract["contract_hash"]
    ):
        raise AcmFieldExecutionError("parent ACM field contract binding changed")

    expected_families = {family["family_id"]: family for family in parent_contract["families"]}
    actual_families = payload.get("families", [])
    if {family["family_id"] for family in actual_families} != set(expected_families):
        raise AcmFieldExecutionError("ACM export-partition family coverage changed")
    for family_item in actual_families:
        family = expected_families[family_item["family_id"]]
        if family_item["parent_query_id"] != family["parent_query_id"]:
            raise AcmFieldExecutionError("ACM export-partition parent binding changed")
        expected_children = {child["child_query_id"]: child for child in family["children"]}
        children = family_item.get("children", [])
        if {child["child_query_id"] for child in children} != set(expected_children):
            raise AcmFieldExecutionError("ACM export-partition child coverage changed")
        for child_item in children:
            child = expected_children[child_item["child_query_id"]]
            if child_item["frozen_child_query_sha256"] != child["child_query_sha256"]:
                raise AcmFieldExecutionError("ACM export partition changed a frozen child query")
            if child_item["partitions"] or child_item["partition_plan_status"] != (
                "REQUIRES_PROSPECTIVE_PROVIDER_COUNTS"
            ):
                raise AcmFieldExecutionError("prospective ACM partition contract contains execution data")
            model = child_item["export_partition_model"]
            if (
                model["maximum_partition_ui_count"] != ACM_EXPORT_RECORD_CEILING
                or model["export_order_used_for_completeness"] is not False
                or model["scientific_query_change"] is not False
                or model["eligibility_filter"] is not False
            ):
                raise AcmFieldExecutionError("ACM export-partition safeguards changed")
    if any(payload.get("production_side_effects", {}).values()):
        raise AcmFieldExecutionError("ACM export-partition contract created production state")


def load_acm_export_partition_contract(
    path: str | Path, *, root: str | Path
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_acm_export_partition_contract(payload, root=root)
    return payload


def export_partition_contract_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the offline ACM field execution contract")
    parser.add_argument("--production-plan", required=True)
    parser.add_argument("--root", default=".")
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    payload = build_acm_field_execution_contract(args.production_plan, root=args.root)
    Path(args.output).write_text(contract_json(payload), encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
