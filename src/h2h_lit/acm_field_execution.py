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
from pathlib import Path
from typing import Any

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
EXPECTED_PLAN_HASH = "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
EXPECTED_PLAN_RAW_SHA256 = "b887d638e42f4909c1c8461dde733d758e5176d528ddccee4370211e14ed7451"


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
    syntax = evidence.exported_syntax
    match = re.fullmatch(
        r'"?query"?\s*:\s*\{(?P<query>.*)\}\s*\t\s*'
        r'"?filter"?\s*:\s*\{(?P<filter>[^}]*)\}\s*',
        syntax,
    )
    if match is None:
        raise AcmFieldExecutionError("ACM exported syntax has an unsupported structure")
    rendered_query = match.group("query")
    wrappers = re.findall(r"(?i)\b(?:title|keyword|abstract):\(", rendered_query)
    if len(wrappers) != 1:
        raise AcmFieldExecutionError("ACM child evidence must contain exactly one field wrapper")
    expected_wrapper = f"{child_spec['field_label']}:("
    if wrappers[0] != expected_wrapper:
        raise AcmFieldExecutionError("ACM child evidence contains the wrong field wrapper")
    if rendered_query != expected_query:
        raise AcmFieldExecutionError("ACM child evidence does not contain the exact frozen query")
    if match.group("filter") != ACM_EXPORT_FILTER:
        raise AcmFieldExecutionError("ACM evidence must use the Full-Text Collection")


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
