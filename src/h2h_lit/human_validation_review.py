"""Blinded, append-only human-validation review workspaces.

This module deliberately separates four concerns:

* immutable record-level packets containing no model proposal;
* assignment state, including explicit reassignment history;
* create-only reviewer returns stored below reviewer-specific paths; and
* reconciliation that reports coverage and disagreement without adjudicating it.

The initial workspace may be prepared without inventing reviewers or deadlines.  Such
records receive an ``UNASSIGNED`` primary slot and reconcile as missing reviews.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write

SCHEMA_VERSION = "1.0.0"
IMPLEMENTATION_VERSION = "human-validation-review-workspace-v1"
WORKSPACE_CLASS = "BLINDED_HUMAN_VALIDATION_REVIEW_WORKSPACE"
PACKET_CLASS = "BLINDED_HUMAN_REVIEW_PACKET"
LEDGER_CLASS = "HUMAN_REVIEW_ASSIGNMENT_LEDGER"
RETURN_CLASS = "BLINDED_HUMAN_RETURNED_REVIEW"
RECONCILIATION_CLASS = "HUMAN_REVIEW_RECONCILIATION_REPORT"

CRITERIA = (
    {
        "criterion_id": "E1",
        "label": "Life-science application",
        "question": (
            "Is the reported system applied to analysis, understanding, monitoring, "
            "decision-making, discovery, or scientific practice in an in-scope life-science, "
            "biomedical, clinical, health, neuroscience, ecology, or related domain?"
        ),
    },
    {
        "criterion_id": "E2",
        "label": "Relational, derived-structure, or multiscale relevance",
        "question": (
            "Does the system analyze explicit relationships or relationships derived from "
            "spatial, temporal, multivariate, image-derived, lineage, similarity, or other "
            "multiscale data?"
        ),
    },
    {
        "criterion_id": "E3",
        "label": "Interactive visual analytics",
        "question": (
            "Does a human use an interactive or analytically meaningful visual representation "
            "to inspect, understand, validate, steer, compare, interpret, or act on results?"
        ),
    },
    {
        "criterion_id": "E4",
        "label": "Substantive computational assistance",
        "question": (
            "Does a nontrivial computational mechanism operate inside the interactive visual-"
            "analytics workflow, beyond ordinary rendering or literal direct manipulation?"
        ),
    },
    {
        "criterion_id": "E5",
        "label": "Human analytic relationship",
        "question": (
            "Does a human meaningfully inspect, direct, validate, interpret, collaborate with, "
            "supervise, correct, or make decisions from the assisted process?"
        ),
    },
    {
        "criterion_id": "E6",
        "label": "Administrative scope",
        "question": (
            "Is the record within the documented retrieval end date, supported by English full "
            "text, and an eligible research-paper type under the frozen protocol?"
        ),
    },
    {
        "criterion_id": "E7",
        "label": "Evidence sufficiency",
        "question": (
            "Does the reviewed evidence identify a candidate system and support a defensible "
            "determination for E1-E6?"
        ),
    },
)

TRI_STATES = ("YES", "NO", "UNCERTAIN")
ELIGIBILITY_STATES = ("ELIGIBLE", "EXCLUDED", "UNCERTAIN")
ASSIGNMENT_STATES = ("UNASSIGNED", "PLANNED", "ASSIGNED", "RETURNED", "CANCELLED")
EXCLUSION_REASONS = (
    "EX_AFTER_RETRIEVAL_END_DATE",
    "EX_NON_ENGLISH_FULL_TEXT",
    "EX_INELIGIBLE_DOCUMENT_TYPE",
    "EX_NO_LIFE_SCIENCE_APPLICATION",
    "EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE",
    "EX_NO_INTERACTIVE_VISUAL_ANALYTICS",
    "EX_NO_QUALIFYING_ASSISTANCE",
    "EX_NO_HUMAN_ANALYTIC_RELATIONSHIP",
    "EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION",
    "EX_OTHER_PROTOCOL_REASON",
)


class HumanValidationReviewError(ValueError):
    """Raised when a review workspace, assignment, or return drifts."""


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode()


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _within_root(root: Path, value: str | Path) -> Path:
    candidate = Path(value)
    path = candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise HumanValidationReviewError(f"path escapes repository: {value}") from exc
    return path


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise HumanValidationReviewError(f"artifact is outside repository: {path}") from exc


def _reference(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": _relative(path, root),
        "path_scope": "repository_relative",
        "byte_size": path.stat().st_size,
        "raw_sha256": _sha256_file(path),
    }


def _local_reference(path: Path, package_dir: Path) -> dict[str, Any]:
    return {
        "path": path.resolve().relative_to(package_dir.resolve()).as_posix(),
        "byte_size": path.stat().st_size,
        "raw_sha256": _sha256_file(path),
    }


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise HumanValidationReviewError(f"refusing to overwrite review artifact: {path}")
        return
    atomic_write(path, content)


def _write_json_once(path: Path, value: Any) -> None:
    _write_once(path, _json_bytes(value))


def _artifact_hash(value: Mapping[str, Any]) -> str:
    material = dict(value)
    material.pop("artifact_hash", None)
    return _canonical_hash(material)


def _with_artifact_hash(value: dict[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result["artifact_hash"] = _artifact_hash(result)
    return result


def _verify_artifact_hash(value: Mapping[str, Any]) -> None:
    if value.get("artifact_hash") != _artifact_hash(value):
        raise HumanValidationReviewError("embedded artifact hash changed")


def _contains_forbidden_blinded_key(value: Any) -> bool:
    forbidden = {
        "model",
        "model_proposal",
        "model_stratification_status",
        "primary_exclusion_reason_for_sampling",
        "uncertain_criteria_for_sampling",
        "prior_human_judgment",
    }
    if isinstance(value, Mapping):
        return any(
            str(key) in forbidden or _contains_forbidden_blinded_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_forbidden_blinded_key(item) for item in value)
    return False


def _slug_hash(value: str, length: int = 20) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:length]


def _reviewer_directory(reviewer_id: str) -> str:
    safe = re.sub(r"[^a-z0-9._-]+", "-", reviewer_id.casefold()).strip("-._")
    safe = safe[:32] or "reviewer"
    return f"{safe}-{_slug_hash(reviewer_id, 10)}"


def _criteria_contract() -> dict[str, Any]:
    return {
        "criterion_states": list(TRI_STATES),
        "criteria": list(CRITERIA),
        "aggregate_states": list(ELIGIBILITY_STATES),
        "aggregate_rule": {
            "ELIGIBLE": "E1-E7 are all YES.",
            "EXCLUDED": "At least one criterion is conclusively NO.",
            "UNCERTAIN": "No criterion is NO and at least one is UNCERTAIN.",
        },
        "exclusion_reasons": list(EXCLUSION_REASONS),
        "full_text_escalation_rule": (
            "Escalate when no criterion is NO and at least one criterion is UNCERTAIN. A "
            "conclusive NO produces EXCLUDED even when another criterion is UNCERTAIN. "
            "Discretionary escalation of an excluded record requires a separate rationale; "
            "acquisition failure does not itself justify exclusion."
        ),
    }


def _response_template(record_id: str) -> dict[str, Any]:
    return {
        "record_id": record_id,
        "criteria": {
            item["criterion_id"]: {
                "decision": None,
                "evidence_quote": "",
                "evidence_locator": "",
                "rationale": "",
            }
            for item in CRITERIA
        },
        "eligibility_status": None,
        "primary_exclusion_reason": None,
        "secondary_exclusion_reasons": [],
        "full_text_escalation_required": None,
        "excluded_record_escalation_rationale": "",
        "confidence": None,
        "notes": "",
    }


def recompute_aggregate_outcome(decisions: Mapping[str, Any]) -> str:
    """Return the protocol aggregate from a complete E1-E7 decision mapping.

    A returned workbook or JSON response may contain a blank or stale calculated value.
    Callers must use this result as authoritative and treat any supplied aggregate only as
    a consistency check.
    """

    expected_criteria = {item["criterion_id"] for item in CRITERIA}
    if set(decisions) != expected_criteria:
        raise HumanValidationReviewError("aggregate recomputation requires complete E1-E7")
    values = [decisions[criterion_id] for criterion_id in sorted(expected_criteria)]
    if any(value not in TRI_STATES for value in values):
        raise HumanValidationReviewError("aggregate recomputation found invalid criterion decision")
    if "NO" in values:
        return "EXCLUDED"
    if all(value == "YES" for value in values):
        return "ELIGIBLE"
    return "UNCERTAIN"


def _load_source_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise HumanValidationReviewError("blinded review source is empty")
    required = {"canonical_id", "title", "abstract", "publication_year"}
    if not required.issubset(rows[0]):
        raise HumanValidationReviewError("blinded review source columns changed")
    seen: set[str] = set()
    for row in rows:
        record_id = str(row.get("canonical_id") or "").strip()
        if not record_id or record_id in seen:
            raise HumanValidationReviewError("record IDs must be nonempty and unique")
        seen.add(record_id)
        for key, value in row.items():
            if key.startswith("human_") and str(value or "").strip():
                raise HumanValidationReviewError("source already contains human judgments")
    return rows


def _load_assignment_plan(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"assignments": []}
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict) or not isinstance(value.get("assignments"), list):
        raise HumanValidationReviewError("assignment plan must contain an assignments list")
    return value


def _validate_due_date(value: Any, *, required: bool) -> str | None:
    if value in (None, ""):
        if required:
            raise HumanValidationReviewError("assigned review requires an ISO due date")
        return None
    text = str(value)
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise HumanValidationReviewError(f"invalid assignment due date: {text}") from exc
    return text


def _assignment_rows(
    *, records: list[dict[str, Any]], plan: Mapping[str, Any], workspace_id: str
) -> list[dict[str, Any]]:
    record_ids = [str(item["canonical_id"]) for item in records]
    known = set(record_ids)
    planned_by_record: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in plan.get("assignments", []):
        if not isinstance(item, Mapping):
            raise HumanValidationReviewError("assignment plan row must be an object")
        record_id = str(item.get("record_id") or "")
        if record_id not in known:
            raise HumanValidationReviewError(f"assignment references unknown record: {record_id}")
        planned_by_record[record_id].append(item)

    assignments: list[dict[str, Any]] = []
    seen_slots: set[tuple[str, str]] = set()
    for record_id in record_ids:
        rows = planned_by_record.get(record_id) or [
            {"slot_id": "primary", "reviewer_id": None, "due_date": None, "status": "UNASSIGNED"}
        ]
        for raw in rows:
            slot_id = str(raw.get("slot_id") or "primary")
            if (record_id, slot_id) in seen_slots:
                raise HumanValidationReviewError("assignment slots must be unique per record")
            seen_slots.add((record_id, slot_id))
            status = str(raw.get("status") or "ASSIGNED")
            if status not in ASSIGNMENT_STATES:
                raise HumanValidationReviewError(f"unsupported assignment status: {status}")
            reviewer_id = raw.get("reviewer_id")
            reviewer_id = str(reviewer_id).strip() if reviewer_id not in (None, "") else None
            if status in {"PLANNED", "ASSIGNED", "RETURNED"} and not reviewer_id:
                raise HumanValidationReviewError(f"{status} assignment requires reviewer_id")
            if status == "UNASSIGNED" and reviewer_id is not None:
                raise HumanValidationReviewError("UNASSIGNED row cannot name a reviewer")
            due_date = _validate_due_date(
                raw.get("due_date"), required=status in {"ASSIGNED", "RETURNED"}
            )
            history = raw.get("reassignment_history", [])
            if not isinstance(history, list):
                raise HumanValidationReviewError("reassignment_history must be a list")
            for index, event in enumerate(history, start=1):
                if not isinstance(event, Mapping) or event.get("sequence") != index:
                    raise HumanValidationReviewError("reassignment history sequence changed")
                for key in ("from_reviewer_id", "to_reviewer_id", "changed_at_utc", "reason"):
                    if not str(event.get(key) or "").strip():
                        raise HumanValidationReviewError(f"reassignment event requires {key}")
            if history and history[-1].get("to_reviewer_id") != reviewer_id:
                raise HumanValidationReviewError(
                    "current reviewer disagrees with reassignment history"
                )
            assignment_id = "assignment:" + _slug_hash(
                f"{workspace_id}\x1f{record_id}\x1f{slot_id}", 24
            )
            assignments.append(
                {
                    "assignment_id": assignment_id,
                    "slot_id": slot_id,
                    "record_id": record_id,
                    "reviewer_id": reviewer_id,
                    "due_date": due_date,
                    "status": status,
                    "reassignment_history": [dict(item) for item in history],
                }
            )
    return assignments


def _ledger_csv(assignments: Iterable[Mapping[str, Any]]) -> bytes:
    buffer = io.StringIO(newline="")
    fields = (
        "assignment_id",
        "slot_id",
        "reviewer_id",
        "record_id",
        "due_date",
        "status",
        "reassignment_count",
        "reassignment_history_json",
    )
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for item in assignments:
        history = item.get("reassignment_history", [])
        writer.writerow(
            {
                **{key: item.get(key) or "" for key in fields[:6]},
                "reassignment_count": len(history),
                "reassignment_history_json": json.dumps(
                    history, sort_keys=True, separators=(",", ":")
                ),
            }
        )
    return buffer.getvalue().encode()


def _return_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Blinded human returned review",
        "artifact_class": RETURN_CLASS,
        "schema_version": SCHEMA_VERSION,
        "create_only": True,
        "required": [
            "assignment_id",
            "reviewer_id",
            "record_id",
            "submitted_at_utc",
            "response",
        ],
        "response_contract": _response_template("<stable-record-id>"),
        "allowed_values": {
            "criterion_decision": list(TRI_STATES),
            "eligibility_status": list(ELIGIBILITY_STATES),
            "primary_exclusion_reason": list(EXCLUSION_REASONS),
            "confidence": ["LOW", "MEDIUM", "HIGH"],
        },
        "storage_rule": (
            "Each submission is written below a reviewer-specific directory using a unique "
            "submission ID. Existing files are never overwritten."
        ),
    }


def _initial_reconciliation(
    *,
    workspace_id: str,
    record_ids: list[str],
    assignments: list[dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    assignment_by_record: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for assignment in assignments:
        assignment_by_record[assignment["record_id"]].append(assignment)
    return _with_artifact_hash(
        {
            "artifact_class": RECONCILIATION_CLASS,
            "schema_version": SCHEMA_VERSION,
            "workspace_id": workspace_id,
            "generated_at_utc": generated_at,
            "counts": {
                "records_total": len(record_ids),
                "missing_reviews": len(record_ids),
                "single_reviewed": 0,
                "independently_double_reviewed": 0,
                "double_review_agreements": 0,
                "disagreements": 0,
            },
            "category_semantics": {
                "missing_reviews": "No valid returned review for the record.",
                "single_reviewed": "Exactly one valid returned review.",
                "independently_double_reviewed": (
                    "At least two valid reviews from distinct reviewer IDs; disagreements are a subset."
                ),
                "disagreements": (
                    "Independent reviews differ on any E1-E7 decision, aggregate eligibility, "
                    "or primary exclusion reason."
                ),
            },
            "records": [
                {
                    "record_id": record_id,
                    "category": "MISSING_REVIEW",
                    "valid_review_count": 0,
                    "assigned_reviewers": sorted(
                        item["reviewer_id"]
                        for item in assignment_by_record[record_id]
                        if item.get("reviewer_id")
                    ),
                    "review_ids": [],
                    "disagreement_fields": [],
                }
                for record_id in record_ids
            ],
        }
    )


def prepare_review_workspace(
    *,
    root: str | Path,
    source_csv: str | Path,
    rubric_path: str | Path,
    output_dir: str | Path,
    generated_at: str,
    assignment_plan: str | Path | None = None,
    source_manifest: str | Path | None = None,
) -> dict[str, Any]:
    """Prepare immutable packets, a ledger, return contract, and baseline reconciliation."""

    root_path = Path(root).resolve()
    source_path = _within_root(root_path, source_csv)
    rubric = _within_root(root_path, rubric_path)
    output = _within_root(root_path, output_dir)
    plan_path = _within_root(root_path, assignment_plan) if assignment_plan else None
    source_manifest_path = _within_root(root_path, source_manifest) if source_manifest else None
    rows = _load_source_rows(source_path)
    source_reference = _reference(source_path, root_path)
    rubric_reference = _reference(rubric, root_path)
    plan = _load_assignment_plan(plan_path)
    record_ids = [str(row["canonical_id"]) for row in rows]
    workspace_id = "human-review-workspace:" + _slug_hash(
        f"{source_reference['raw_sha256']}\x1f{rubric_reference['raw_sha256']}",
        24,
    )
    output.mkdir(parents=True, exist_ok=True)
    packet_dir = output / "packets"
    packet_references: list[dict[str, Any]] = []
    packet_by_record: dict[str, dict[str, str]] = {}
    contract = _criteria_contract()
    for row in rows:
        record_id = str(row["canonical_id"])
        row_material = {
            "record_id": record_id,
            "sample_order": int(row.get("sample_order") or 0),
            "title": row.get("title") or "",
            "abstract": row.get("abstract") or "",
            "publication_year": row.get("publication_year") or None,
        }
        packet_id = "review-packet:" + _slug_hash(
            f"{workspace_id}\x1f{_canonical_hash(row_material)}", 24
        )
        packet = _with_artifact_hash(
            {
                "artifact_class": PACKET_CLASS,
                "schema_version": SCHEMA_VERSION,
                "workspace_id": workspace_id,
                "packet_id": packet_id,
                "blinded": True,
                "blinding_statement": (
                    "Model proposals, sampling strata, prior human judgments, and reviewer "
                    "identities are not included."
                ),
                "record": row_material,
                "criteria_contract": contract,
                "structured_response": _response_template(record_id),
                "rubric_binding": rubric_reference,
                "source_row_sha256": _canonical_hash(row_material),
            }
        )
        packet_path = packet_dir / f"record-{_slug_hash(record_id)}.json"
        _write_json_once(packet_path, packet)
        reference = _local_reference(packet_path, output)
        packet_references.append({"record_id": record_id, "packet_id": packet_id, **reference})
        packet_by_record[record_id] = {
            "packet_id": packet_id,
            "packet_path": reference["path"],
        }

    assignments = _assignment_rows(records=rows, plan=plan, workspace_id=workspace_id)
    for assignment in assignments:
        assignment.update(packet_by_record[assignment["record_id"]])
    ledger = _with_artifact_hash(
        {
            "artifact_class": LEDGER_CLASS,
            "schema_version": SCHEMA_VERSION,
            "workspace_id": workspace_id,
            "generated_at_utc": generated_at,
            "assignments": assignments,
        }
    )
    ledger_path = output / "assignment_ledger.json"
    ledger_csv_path = output / "assignment_ledger.csv"
    _write_json_once(ledger_path, ledger)
    _write_once(ledger_csv_path, _ledger_csv(assignments))
    schema_path = output / "returned_reviews" / "RETURN_SCHEMA.json"
    _write_json_once(schema_path, _return_schema())
    reconciliation = _initial_reconciliation(
        workspace_id=workspace_id,
        record_ids=record_ids,
        assignments=assignments,
        generated_at=generated_at,
    )
    reconciliation_path = output / "reconciliation_report.json"
    _write_json_once(reconciliation_path, reconciliation)
    manifest = {
        "artifact_class": WORKSPACE_CLASS,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "workspace_id": workspace_id,
        "generated_at_utc": generated_at,
        "status": (
            "PREPARED_NOT_ASSIGNED_NOT_REVIEWED"
            if all(item["status"] == "UNASSIGNED" for item in assignments)
            else "PREPARED_ASSIGNMENTS_NOT_REVIEWED"
        ),
        "bindings": {
            "blinded_source_csv": source_reference,
            "blinded_sample_manifest": (
                _reference(source_manifest_path, root_path) if source_manifest_path else None
            ),
            "frozen_rubric": rubric_reference,
            "assignment_plan": _reference(plan_path, root_path) if plan_path else None,
            "implementation": _reference(
                root_path / "src/h2h_lit/human_validation_review.py", root_path
            ),
        },
        "counts": {
            "records": len(rows),
            "packets": len(packet_references),
            "assignments": len(assignments),
            "unassigned": sum(item["status"] == "UNASSIGNED" for item in assignments),
            "returned_reviews": 0,
        },
        "artifacts": {
            "assignment_ledger_json": _local_reference(ledger_path, output),
            "assignment_ledger_csv": _local_reference(ledger_csv_path, output),
            "return_schema": _local_reference(schema_path, output),
            "reconciliation_report": _local_reference(reconciliation_path, output),
            "packets": packet_references,
        },
        "invariants": {
            "record_ids_stable": True,
            "packets_blinded": True,
            "returned_reviews_create_only": True,
            "reviewer_returns_separate": True,
            "reconciliation_is_non_adjudicative": True,
            "production_import_allowed": False,
        },
    }
    manifest_path = output / "package_manifest.json"
    _write_json_once(manifest_path, manifest)
    return {
        "workspace_id": workspace_id,
        "package_dir": _relative(output, root_path),
        "package_manifest_sha256": _sha256_file(manifest_path),
        "counts": manifest["counts"],
        "reconciliation_counts": reconciliation["counts"],
    }


def _verify_local_reference(package_dir: Path, reference: Mapping[str, Any]) -> Path:
    path = (package_dir / str(reference.get("path") or "")).resolve()
    try:
        path.relative_to(package_dir)
    except ValueError as exc:
        raise HumanValidationReviewError("workspace artifact escapes package") from exc
    if not path.is_file() or path.stat().st_size != reference.get("byte_size"):
        raise HumanValidationReviewError("workspace artifact path or size changed")
    if _sha256_file(path) != reference.get("raw_sha256"):
        raise HumanValidationReviewError("workspace artifact hash changed")
    return path


def validate_review_workspace(
    *, root: str | Path, package_dir: str | Path, expected_manifest_sha256: str
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    package = _within_root(root_path, package_dir)
    manifest_path = package / "package_manifest.json"
    if _sha256_file(manifest_path) != expected_manifest_sha256:
        raise HumanValidationReviewError("workspace manifest hash changed")
    manifest = json.loads(manifest_path.read_bytes())
    if (
        manifest.get("artifact_class") != WORKSPACE_CLASS
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("implementation_version") != IMPLEMENTATION_VERSION
    ):
        raise HumanValidationReviewError("workspace contract changed")
    for binding in manifest.get("bindings", {}).values():
        if binding is None:
            continue
        path = _within_root(root_path, str(binding.get("path") or ""))
        if (
            not path.is_file()
            or path.stat().st_size != binding.get("byte_size")
            or _sha256_file(path) != binding.get("raw_sha256")
        ):
            raise HumanValidationReviewError("workspace source binding changed")
    artifacts = manifest.get("artifacts", {})
    for key in (
        "assignment_ledger_json",
        "assignment_ledger_csv",
        "return_schema",
        "reconciliation_report",
    ):
        _verify_local_reference(package, artifacts.get(key, {}))
    packets = artifacts.get("packets", [])
    if len(packets) != manifest.get("counts", {}).get("packets"):
        raise HumanValidationReviewError("workspace packet count changed")
    for reference in packets:
        packet_path = _verify_local_reference(package, reference)
        packet = json.loads(packet_path.read_bytes())
        _verify_artifact_hash(packet)
        if (
            packet.get("artifact_class") != PACKET_CLASS
            or packet.get("blinded") is not True
            or packet.get("record", {}).get("record_id") != reference.get("record_id")
            or _contains_forbidden_blinded_key(packet)
        ):
            raise HumanValidationReviewError("blinded packet contract changed")
    ledger_path = _verify_local_reference(package, artifacts["assignment_ledger_json"])
    ledger = json.loads(ledger_path.read_bytes())
    _verify_artifact_hash(ledger)
    if ledger.get("workspace_id") != manifest.get("workspace_id"):
        raise HumanValidationReviewError("assignment ledger workspace changed")
    return manifest


def _validate_response(response: Mapping[str, Any], record_id: str) -> None:
    if response.get("record_id") != record_id:
        raise HumanValidationReviewError("returned response record ID changed")
    criteria = response.get("criteria")
    if not isinstance(criteria, Mapping) or set(criteria) != {
        item["criterion_id"] for item in CRITERIA
    }:
        raise HumanValidationReviewError("returned response criterion set changed")
    decisions: dict[str, str] = {}
    for criterion_id in sorted(criteria):
        item = criteria[criterion_id]
        if not isinstance(item, Mapping) or item.get("decision") not in TRI_STATES:
            raise HumanValidationReviewError("invalid returned criterion decision")
        decisions[criterion_id] = str(item["decision"])
        for field in ("evidence_quote", "evidence_locator", "rationale"):
            if not isinstance(item.get(field), str):
                raise HumanValidationReviewError(f"criterion {field} must be text")
    expected = recompute_aggregate_outcome(decisions)
    if response.get("eligibility_status") != expected:
        raise HumanValidationReviewError("aggregate eligibility disagrees with E1-E7")
    primary = response.get("primary_exclusion_reason")
    if expected == "EXCLUDED" and primary not in EXCLUSION_REASONS:
        raise HumanValidationReviewError("excluded review requires a primary reason")
    if expected != "EXCLUDED" and primary is not None:
        raise HumanValidationReviewError("non-excluded review cannot have a primary reason")
    secondary = response.get("secondary_exclusion_reasons")
    if not isinstance(secondary, list) or any(item not in EXCLUSION_REASONS for item in secondary):
        raise HumanValidationReviewError("invalid secondary exclusion reasons")
    escalation = response.get("full_text_escalation_required")
    if escalation not in (True, False):
        raise HumanValidationReviewError("full_text_escalation_required must be boolean")
    escalation_rationale = response.get("excluded_record_escalation_rationale", "")
    if not isinstance(escalation_rationale, str):
        raise HumanValidationReviewError("excluded-record escalation rationale must be text")
    if expected == "UNCERTAIN" and escalation is not True:
        raise HumanValidationReviewError("uncertain review requires full-text escalation")
    if expected == "ELIGIBLE" and escalation is not False:
        raise HumanValidationReviewError("eligible review cannot require full-text escalation")
    if expected == "EXCLUDED" and escalation is True and not escalation_rationale.strip():
        raise HumanValidationReviewError(
            "discretionary escalation of an excluded review requires an explicit rationale"
        )
    if not (expected == "EXCLUDED" and escalation is True) and escalation_rationale.strip():
        raise HumanValidationReviewError(
            "excluded-record escalation rationale is only valid for an escalated exclusion"
        )
    if response.get("confidence") not in ("LOW", "MEDIUM", "HIGH"):
        raise HumanValidationReviewError("confidence must be LOW, MEDIUM, or HIGH")
    if not isinstance(response.get("notes"), str):
        raise HumanValidationReviewError("review notes must be text")


def write_returned_review(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    assignment_id: str,
    reviewer_id: str,
    submitted_at: str,
    response: Mapping[str, Any],
) -> dict[str, Any]:
    """Write one create-only return below a reviewer-specific directory."""

    root_path = Path(root).resolve()
    package = _within_root(root_path, package_dir)
    manifest = validate_review_workspace(
        root=root_path,
        package_dir=package,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    ledger_ref = manifest["artifacts"]["assignment_ledger_json"]
    ledger = json.loads(_verify_local_reference(package, ledger_ref).read_bytes())
    assignment = next(
        (item for item in ledger["assignments"] if item["assignment_id"] == assignment_id), None
    )
    if assignment is None:
        raise HumanValidationReviewError("returned review references unknown assignment")
    if assignment.get("reviewer_id") != reviewer_id:
        raise HumanValidationReviewError("returned reviewer does not own assignment")
    if assignment.get("status") not in {"ASSIGNED", "RETURNED"}:
        raise HumanValidationReviewError("assignment is not open for a returned review")
    record_id = assignment["record_id"]
    _validate_response(response, record_id)
    payload = {
        "artifact_class": RETURN_CLASS,
        "schema_version": SCHEMA_VERSION,
        "workspace_id": manifest["workspace_id"],
        "workspace_manifest_sha256": expected_manifest_sha256,
        "assignment_id": assignment_id,
        "reviewer_id": reviewer_id,
        "record_id": record_id,
        "packet_id": assignment["packet_id"],
        "submitted_at_utc": submitted_at,
        "response": dict(response),
    }
    submission_id = "returned-review:" + _slug_hash(_canonical_hash(payload), 24)
    payload["submission_id"] = submission_id
    payload = _with_artifact_hash(payload)
    destination = (
        package
        / "returned_reviews"
        / _reviewer_directory(reviewer_id)
        / f"{_slug_hash(submission_id, 32)}.json"
    )
    if destination.exists():
        raise HumanValidationReviewError(
            "returned review already exists; never overwrite a judgment"
        )
    _write_json_once(destination, payload)
    return {
        "submission_id": submission_id,
        "path": _relative(destination, root_path),
        "raw_sha256": _sha256_file(destination),
    }


def _judgment_signature(review: Mapping[str, Any]) -> dict[str, Any]:
    response = review["response"]
    return {
        "criteria": {
            key: response["criteria"][key]["decision"] for key in sorted(response["criteria"])
        },
        "eligibility_status": response["eligibility_status"],
        "primary_exclusion_reason": response["primary_exclusion_reason"],
    }


def reconcile_returned_reviews(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    generated_at: str,
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    """Reconcile create-only returns without choosing a winning judgment."""

    root_path = Path(root).resolve()
    package = _within_root(root_path, package_dir)
    manifest = validate_review_workspace(
        root=root_path,
        package_dir=package,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    ledger_path = _verify_local_reference(package, manifest["artifacts"]["assignment_ledger_json"])
    ledger = json.loads(ledger_path.read_bytes())
    assignments = {item["assignment_id"]: item for item in ledger["assignments"]}
    by_record: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen_assignments: set[str] = set()
    returns_root = package / "returned_reviews"
    for path in sorted(returns_root.glob("*/*.json")):
        review = json.loads(path.read_bytes())
        _verify_artifact_hash(review)
        if review.get("artifact_class") != RETURN_CLASS:
            raise HumanValidationReviewError("returned review artifact class changed")
        assignment = assignments.get(review.get("assignment_id"))
        if assignment is None:
            raise HumanValidationReviewError("returned review assignment changed")
        if review["assignment_id"] in seen_assignments:
            raise HumanValidationReviewError("multiple returns exist for one assignment")
        seen_assignments.add(review["assignment_id"])
        if (
            review.get("workspace_manifest_sha256") != expected_manifest_sha256
            or review.get("workspace_id") != manifest["workspace_id"]
            or review.get("reviewer_id") != assignment.get("reviewer_id")
            or review.get("record_id") != assignment.get("record_id")
            or review.get("packet_id") != assignment.get("packet_id")
            or path.parent.name != _reviewer_directory(str(review.get("reviewer_id")))
        ):
            raise HumanValidationReviewError("returned review binding changed")
        _validate_response(review["response"], assignment["record_id"])
        by_record[assignment["record_id"]].append(review)

    record_ids = [item["record_id"] for item in manifest["artifacts"]["packets"]]
    records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for record_id in record_ids:
        reviews = by_record.get(record_id, [])
        reviewers = [item["reviewer_id"] for item in reviews]
        if len(reviewers) != len(set(reviewers)):
            raise HumanValidationReviewError("independent reviews require distinct reviewer IDs")
        disagreement_fields: list[str] = []
        if len(reviews) >= 2:
            signatures = [_judgment_signature(item) for item in reviews]
            first = signatures[0]
            for criterion_id in sorted(first["criteria"]):
                if len({item["criteria"][criterion_id] for item in signatures}) > 1:
                    disagreement_fields.append(criterion_id)
            for field in ("eligibility_status", "primary_exclusion_reason"):
                if len({item[field] for item in signatures}) > 1:
                    disagreement_fields.append(field)
            category = (
                "INDEPENDENT_DOUBLE_REVIEW_DISAGREEMENT"
                if disagreement_fields
                else "INDEPENDENT_DOUBLE_REVIEW_AGREEMENT"
            )
            counts["independently_double_reviewed"] += 1
            counts["disagreements" if disagreement_fields else "double_review_agreements"] += 1
        elif len(reviews) == 1:
            category = "SINGLE_REVIEWED"
            counts["single_reviewed"] += 1
        else:
            category = "MISSING_REVIEW"
            counts["missing_reviews"] += 1
        records.append(
            {
                "record_id": record_id,
                "category": category,
                "valid_review_count": len(reviews),
                "reviewer_ids": sorted(reviewers),
                "review_ids": sorted(item["submission_id"] for item in reviews),
                "disagreement_fields": disagreement_fields,
            }
        )
    report = _with_artifact_hash(
        {
            "artifact_class": RECONCILIATION_CLASS,
            "schema_version": SCHEMA_VERSION,
            "workspace_id": manifest["workspace_id"],
            "workspace_manifest_sha256": expected_manifest_sha256,
            "generated_at_utc": generated_at,
            "counts": {
                "records_total": len(record_ids),
                "missing_reviews": counts["missing_reviews"],
                "single_reviewed": counts["single_reviewed"],
                "independently_double_reviewed": counts["independently_double_reviewed"],
                "double_review_agreements": counts["double_review_agreements"],
                "disagreements": counts["disagreements"],
            },
            "category_semantics": {
                "disagreements_are_subset_of_independently_double_reviewed": True,
                "reconciliation_selects_winner": False,
            },
            "records": records,
        }
    )
    destination = (
        _within_root(root_path, output_path)
        if output_path
        else package / "reconciliation" / f"reconciliation-{_slug_hash(generated_at)}.json"
    )
    if destination.exists():
        raise HumanValidationReviewError("reconciliation output already exists")
    _write_json_once(destination, report)
    return {
        "path": _relative(destination, root_path),
        "raw_sha256": _sha256_file(destination),
        "counts": report["counts"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--prepare-review-workspace", action="store_true")
    modes.add_argument("--write-returned-review", action="store_true")
    modes.add_argument("--reconcile-returned-reviews", action="store_true")
    parser.add_argument("--source-csv", type=Path)
    parser.add_argument("--source-manifest", type=Path)
    parser.add_argument("--rubric", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--generated-at")
    parser.add_argument("--assignment-plan", type=Path)
    parser.add_argument("--package-dir", type=Path)
    parser.add_argument("--package-manifest-sha256")
    parser.add_argument("--assignment-id")
    parser.add_argument("--reviewer-id")
    parser.add_argument("--submitted-at")
    parser.add_argument("--response-json", type=Path)
    parser.add_argument("--reconciliation-output", type=Path)
    args = parser.parse_args(argv)
    if args.prepare_review_workspace:
        if not all((args.source_csv, args.rubric, args.output_dir, args.generated_at)):
            parser.error(
                "preparation requires --source-csv, --rubric, --output-dir, and --generated-at"
            )
        result = prepare_review_workspace(
            root=args.root,
            source_csv=args.source_csv,
            rubric_path=args.rubric,
            output_dir=args.output_dir,
            generated_at=args.generated_at,
            assignment_plan=args.assignment_plan,
            source_manifest=args.source_manifest,
        )
    elif args.write_returned_review:
        if not all(
            (
                args.package_dir,
                args.package_manifest_sha256,
                args.assignment_id,
                args.reviewer_id,
                args.submitted_at,
                args.response_json,
            )
        ):
            parser.error(
                "return writing requires --package-dir, --package-manifest-sha256, "
                "--assignment-id, --reviewer-id, --submitted-at, and --response-json"
            )
        response_path = _within_root(Path(args.root).resolve(), args.response_json)
        result = write_returned_review(
            root=args.root,
            package_dir=args.package_dir,
            expected_manifest_sha256=args.package_manifest_sha256,
            assignment_id=args.assignment_id,
            reviewer_id=args.reviewer_id,
            submitted_at=args.submitted_at,
            response=json.loads(response_path.read_bytes()),
        )
    else:
        if not all((args.package_dir, args.package_manifest_sha256, args.generated_at)):
            parser.error(
                "reconciliation requires --package-dir, --package-manifest-sha256, "
                "and --generated-at"
            )
        result = reconcile_returned_reviews(
            root=args.root,
            package_dir=args.package_dir,
            expected_manifest_sha256=args.package_manifest_sha256,
            generated_at=args.generated_at,
            output_path=args.reconciliation_output,
        )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
