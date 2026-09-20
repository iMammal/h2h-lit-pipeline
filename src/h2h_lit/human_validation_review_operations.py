"""Append-only operational extensions for blinded human review workspaces.

The v1 workspace package and its bound implementation remain immutable.  This module
adds three operations around that package without rewriting it:

* create-only corrections linked to a reviewer's own prior submission;
* create-only assignment-ledger revisions for explicit release to Morris; and
* reconciliation that distinguishes submission history from independent reviewers.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from h2h_lit.human_validation_review import (
    RETURN_CLASS,
    HumanValidationReviewError,
    _canonical_hash,
    _judgment_signature,
    _relative,
    _reviewer_directory,
    _sha256_file,
    _slug_hash,
    _validate_response,
    _verify_artifact_hash,
    _verify_local_reference,
    _with_artifact_hash,
    _within_root,
    _write_json_once,
    validate_review_workspace,
)

OPERATIONS_SCHEMA_VERSION = "1.1.0"
LEDGER_REVISION_CLASS = "HUMAN_REVIEW_ASSIGNMENT_LEDGER_REVISION"
HISTORY_RECONCILIATION_CLASS = "HUMAN_REVIEW_HISTORY_RECONCILIATION_REPORT"
MORRIS_REVIEWER_ID = "morris"


def _load_workspace(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    assignment_ledger_revision: str | Path | None = None,
) -> tuple[Path, Path, dict[str, Any], dict[str, Any], str]:
    root_path = Path(root).resolve()
    package = _within_root(root_path, package_dir)
    manifest = validate_review_workspace(
        root=root_path,
        package_dir=package,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    base_ledger_path = _verify_local_reference(
        package, manifest["artifacts"]["assignment_ledger_json"]
    )
    base_ledger_sha256 = _sha256_file(base_ledger_path)
    if assignment_ledger_revision is None:
        return (
            root_path,
            package,
            manifest,
            json.loads(base_ledger_path.read_bytes()),
            base_ledger_sha256,
        )

    revision_path = _within_root(root_path, assignment_ledger_revision)
    revision = json.loads(revision_path.read_bytes())
    _verify_artifact_hash(revision)
    if (
        revision.get("artifact_class") != LEDGER_REVISION_CLASS
        or revision.get("schema_version") != OPERATIONS_SCHEMA_VERSION
        or revision.get("workspace_id") != manifest["workspace_id"]
        or revision.get("workspace_manifest_sha256") != expected_manifest_sha256
        or revision.get("base_assignment_ledger_sha256") != base_ledger_sha256
    ):
        raise HumanValidationReviewError("assignment-ledger revision binding changed")
    if not isinstance(revision.get("assignments"), list):
        raise HumanValidationReviewError("assignment-ledger revision has no assignments")
    return root_path, package, manifest, revision, _sha256_file(revision_path)


def _validate_return_binding(
    *,
    review: Mapping[str, Any],
    path: Path,
    assignment: Mapping[str, Any],
    manifest: Mapping[str, Any],
    expected_manifest_sha256: str,
) -> None:
    _verify_artifact_hash(review)
    if review.get("artifact_class") != RETURN_CLASS:
        raise HumanValidationReviewError("returned review artifact class changed")
    if (
        review.get("workspace_manifest_sha256") != expected_manifest_sha256
        or review.get("workspace_id") != manifest["workspace_id"]
        or review.get("reviewer_id") != assignment.get("reviewer_id")
        or review.get("record_id") != assignment.get("record_id")
        or review.get("packet_id") != assignment.get("packet_id")
        or path.parent.name != _reviewer_directory(str(review.get("reviewer_id")))
    ):
        raise HumanValidationReviewError("returned review binding changed")
    _validate_response(review["response"], str(assignment["record_id"]))


def _load_return_history(
    *,
    package: Path,
    manifest: Mapping[str, Any],
    ledger: Mapping[str, Any],
    expected_manifest_sha256: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    assignments = {
        str(item["assignment_id"]): item for item in ledger.get("assignments", [])
    }
    submissions: dict[str, dict[str, Any]] = {}
    by_assignment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for path in sorted((package / "returned_reviews").glob("*/*.json")):
        review = json.loads(path.read_bytes())
        assignment = assignments.get(str(review.get("assignment_id") or ""))
        if not isinstance(assignment, Mapping):
            raise HumanValidationReviewError("returned review assignment changed")
        _validate_return_binding(
            review=review,
            path=path,
            assignment=assignment,
            manifest=manifest,
            expected_manifest_sha256=expected_manifest_sha256,
        )
        submission_id = str(review.get("submission_id") or "")
        if not submission_id or submission_id in submissions:
            raise HumanValidationReviewError("returned review submission ID changed")
        submissions[submission_id] = review
        by_assignment[str(review["assignment_id"])].append(review)

    for assignment_id, reviews in by_assignment.items():
        review_ids = {str(item["submission_id"]) for item in reviews}
        children: Counter[str] = Counter()
        roots = 0
        for review in reviews:
            parent = review.get("corrects_submission_id")
            if parent in (None, ""):
                roots += 1
                continue
            parent_review = submissions.get(str(parent))
            if (
                parent_review is None
                or parent not in review_ids
                or parent_review.get("assignment_id") != assignment_id
                or parent_review.get("reviewer_id") != review.get("reviewer_id")
                or parent_review.get("record_id") != review.get("record_id")
            ):
                raise HumanValidationReviewError("correction target binding changed")
            children[str(parent)] += 1
        if roots != 1 or any(count != 1 for count in children.values()):
            raise HumanValidationReviewError("returned review correction history branched")
        terminals = review_ids - set(children)
        if len(terminals) != 1:
            raise HumanValidationReviewError("returned review correction history has no terminal")
        visited: set[str] = set()
        cursor = next(iter(terminals))
        while cursor:
            if cursor in visited:
                raise HumanValidationReviewError("returned review correction history contains a cycle")
            visited.add(cursor)
            cursor = str(submissions[cursor].get("corrects_submission_id") or "")
        if visited != review_ids:
            raise HumanValidationReviewError("returned review correction history is disconnected")
    return submissions, by_assignment


def _terminal_review(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    corrected_ids = {
        str(item["corrects_submission_id"])
        for item in reviews
        if item.get("corrects_submission_id")
    }
    terminals = [item for item in reviews if item["submission_id"] not in corrected_ids]
    if len(terminals) != 1:
        raise HumanValidationReviewError("assignment does not have one effective review")
    return terminals[0]


def write_returned_review_revision(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    assignment_id: str,
    reviewer_id: str,
    submitted_at: str,
    response: Mapping[str, Any],
    corrects_submission_id: str | None = None,
    assignment_ledger_revision: str | Path | None = None,
) -> dict[str, Any]:
    """Create an original return or append a correction to the same reviewer's chain."""

    root_path, package, manifest, ledger, ledger_sha256 = _load_workspace(
        root=root,
        package_dir=package_dir,
        expected_manifest_sha256=expected_manifest_sha256,
        assignment_ledger_revision=assignment_ledger_revision,
    )
    assignments = {item["assignment_id"]: item for item in ledger["assignments"]}
    assignment = assignments.get(assignment_id)
    if not isinstance(assignment, Mapping):
        raise HumanValidationReviewError("returned review references unknown assignment")
    if assignment.get("reviewer_id") != reviewer_id:
        raise HumanValidationReviewError("returned reviewer does not own assignment")
    if assignment.get("status") not in {"ASSIGNED", "RETURNED", "PLANNED"}:
        raise HumanValidationReviewError("assignment is not open for a returned review")
    _validate_response(response, str(assignment["record_id"]))
    submissions, by_assignment = _load_return_history(
        package=package,
        manifest=manifest,
        ledger=ledger,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    prior = by_assignment.get(assignment_id, [])
    if prior:
        latest = _terminal_review(prior)
        if corrects_submission_id != latest["submission_id"]:
            raise HumanValidationReviewError(
                "correction must name the current terminal submission for this assignment"
            )
        sequence = len(prior) + 1
    else:
        if corrects_submission_id is not None:
            raise HumanValidationReviewError("original return cannot correct another submission")
        sequence = 1
    if corrects_submission_id and corrects_submission_id not in submissions:
        raise HumanValidationReviewError("correction target does not exist")

    payload = {
        "artifact_class": RETURN_CLASS,
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "workspace_id": manifest["workspace_id"],
        "workspace_manifest_sha256": expected_manifest_sha256,
        "assignment_ledger_sha256": ledger_sha256,
        "assignment_id": assignment_id,
        "reviewer_id": reviewer_id,
        "record_id": assignment["record_id"],
        "packet_id": assignment["packet_id"],
        "submitted_at_utc": submitted_at,
        "revision_sequence": sequence,
        "corrects_submission_id": corrects_submission_id,
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
        raise HumanValidationReviewError("returned review already exists")
    _write_json_once(destination, payload)
    return {
        "submission_id": submission_id,
        "corrects_submission_id": corrects_submission_id,
        "revision_sequence": sequence,
        "path": _relative(destination, root_path),
        "raw_sha256": _sha256_file(destination),
    }


def release_unfinished_assignment_to_morris(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    assignment_id: str,
    changed_at: str,
    reason: str,
    output_path: str | Path,
    assignment_ledger_revision: str | Path | None = None,
) -> dict[str, Any]:
    """Create a ledger revision releasing unfinished work to Morris without overwriting state."""

    root_path, package, manifest, ledger, ledger_sha256 = _load_workspace(
        root=root,
        package_dir=package_dir,
        expected_manifest_sha256=expected_manifest_sha256,
        assignment_ledger_revision=assignment_ledger_revision,
    )
    assignments = [dict(item) for item in ledger["assignments"]]
    target = next((item for item in assignments if item["assignment_id"] == assignment_id), None)
    if target is None:
        raise HumanValidationReviewError("release references unknown assignment")
    current_reviewer = target.get("reviewer_id")
    if not current_reviewer:
        raise HumanValidationReviewError("unassigned work cannot be released")
    if current_reviewer == MORRIS_REVIEWER_ID:
        raise HumanValidationReviewError("assignment is already held by Morris")
    if target.get("status") not in {"PLANNED", "ASSIGNED"}:
        raise HumanValidationReviewError("only unfinished open work can be released")
    if not reason.strip():
        raise HumanValidationReviewError("release requires a reason")
    _, by_assignment = _load_return_history(
        package=package,
        manifest=manifest,
        ledger=ledger,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    if by_assignment.get(assignment_id):
        raise HumanValidationReviewError(
            "completed judgments are preserved; an assignment with returns cannot be released"
        )
    history = [dict(item) for item in target.get("reassignment_history", [])]
    history.append(
        {
            "sequence": len(history) + 1,
            "action": "RELEASED_AND_REASSIGNED",
            "from_reviewer_id": current_reviewer,
            "to_reviewer_id": MORRIS_REVIEWER_ID,
            "changed_at_utc": changed_at,
            "reason": reason.strip(),
            "unfinished_work_only": True,
        }
    )
    target.update(
        {
            "reviewer_id": MORRIS_REVIEWER_ID,
            "due_date": None,
            "status": "PLANNED",
            "release_state": "RELEASED_TO_MORRIS_AWAITING_DUE_DATE",
            "reassignment_history": history,
        }
    )
    revision = _with_artifact_hash(
        {
            "artifact_class": LEDGER_REVISION_CLASS,
            "schema_version": OPERATIONS_SCHEMA_VERSION,
            "workspace_id": manifest["workspace_id"],
            "workspace_manifest_sha256": expected_manifest_sha256,
            "base_assignment_ledger_sha256": ledger_sha256,
            "generated_at_utc": changed_at,
            "status": "CREATE_ONLY_LEDGER_REVISION",
            "assignments": assignments,
        }
    )
    destination = _within_root(root_path, output_path)
    if destination.exists():
        raise HumanValidationReviewError("assignment-ledger revision already exists")
    _write_json_once(destination, revision)
    return {
        "path": _relative(destination, root_path),
        "raw_sha256": _sha256_file(destination),
        "assignment_id": assignment_id,
        "reviewer_id": MORRIS_REVIEWER_ID,
        "status": "PLANNED",
    }


def _disagreement_fields(reviews: list[Mapping[str, Any]]) -> list[str]:
    if len(reviews) < 2:
        return []
    signatures = [_judgment_signature(item) for item in reviews]
    result: list[str] = []
    for criterion_id in sorted(str(key) for key in signatures[0]["criteria"]):
        if len({item["criteria"][criterion_id] for item in signatures}) > 1:
            result.append(criterion_id)
    for field in ("eligibility_status", "primary_exclusion_reason"):
        if len({item[field] for item in signatures}) > 1:
            result.append(field)
    return result


def reconcile_review_history(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    generated_at: str,
    output_path: str | Path,
    assignment_ledger_revision: str | Path | None = None,
) -> dict[str, Any]:
    """Reconcile histories while counting distinct reviewer identities only once."""

    root_path, package, manifest, ledger, ledger_sha256 = _load_workspace(
        root=root,
        package_dir=package_dir,
        expected_manifest_sha256=expected_manifest_sha256,
        assignment_ledger_revision=assignment_ledger_revision,
    )
    submissions, by_assignment = _load_return_history(
        package=package,
        manifest=manifest,
        ledger=ledger,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    effective_by_record: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for reviews in by_assignment.values():
        terminal = _terminal_review(reviews)
        effective_by_record[str(terminal["record_id"])].append(terminal)

    record_ids = [item["record_id"] for item in manifest["artifacts"]["packets"]]
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for record_id in record_ids:
        effective = effective_by_record.get(record_id, [])
        by_reviewer: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for review in effective:
            by_reviewer[str(review["reviewer_id"])].append(review)
        representatives = [
            max(items, key=lambda item: (str(item["submitted_at_utc"]), item["submission_id"]))
            for items in by_reviewer.values()
        ]
        representatives.sort(key=lambda item: str(item["reviewer_id"]))
        disagreements = _disagreement_fields(representatives)
        repeat_count = len(effective) - len(representatives)
        counts["same_reviewer_repeat_reviews"] += repeat_count
        if len(representatives) >= 2:
            category = (
                "INDEPENDENT_DOUBLE_REVIEW_DISAGREEMENT"
                if disagreements
                else "INDEPENDENT_DOUBLE_REVIEW_AGREEMENT"
            )
            counts["independently_double_reviewed"] += 1
            counts["disagreements" if disagreements else "double_review_agreements"] += 1
        elif representatives:
            category = "SINGLE_REVIEWED"
            counts["single_reviewed"] += 1
        else:
            category = "MISSING_REVIEW"
            counts["missing_reviews"] += 1
        record_history = [
            review
            for reviews in by_assignment.values()
            for review in reviews
            if review["record_id"] == record_id
        ]
        rows.append(
            {
                "record_id": record_id,
                "category": category,
                "submission_history_count": len(record_history),
                "effective_assignment_review_count": len(effective),
                "independent_reviewer_count": len(representatives),
                "same_reviewer_repeat_review_count": repeat_count,
                "reviewer_ids": sorted(by_reviewer),
                "submission_ids": sorted(item["submission_id"] for item in record_history),
                "effective_submission_ids": sorted(
                    item["submission_id"] for item in effective
                ),
                "independent_submission_ids": sorted(
                    item["submission_id"] for item in representatives
                ),
                "disagreement_fields": disagreements,
            }
        )

    effective_count = sum(len(items) for items in effective_by_record.values())
    report = _with_artifact_hash(
        {
            "artifact_class": HISTORY_RECONCILIATION_CLASS,
            "schema_version": OPERATIONS_SCHEMA_VERSION,
            "workspace_id": manifest["workspace_id"],
            "workspace_manifest_sha256": expected_manifest_sha256,
            "assignment_ledger_sha256": ledger_sha256,
            "generated_at_utc": generated_at,
            "counts": {
                "records_total": len(record_ids),
                "missing_reviews": counts["missing_reviews"],
                "single_reviewed": counts["single_reviewed"],
                "independently_double_reviewed": counts[
                    "independently_double_reviewed"
                ],
                "double_review_agreements": counts["double_review_agreements"],
                "disagreements": counts["disagreements"],
                "returned_submissions": len(submissions),
                "effective_assignment_reviews": effective_count,
                "superseded_corrections": len(submissions) - effective_count,
                "same_reviewer_repeat_reviews": counts["same_reviewer_repeat_reviews"],
            },
            "category_semantics": {
                "independence_key": "reviewer_id",
                "corrections_count_as_additional_reviews": False,
                "same_reviewer_repeats_count_as_independent": False,
                "morris_repeats_count_as_independent": False,
                "disagreements_compare_distinct_reviewer_ids_only": True,
                "reconciliation_selects_winner": False,
            },
            "records": rows,
        }
    )
    destination = _within_root(root_path, output_path)
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
    modes.add_argument("--write-returned-review-revision", action="store_true")
    modes.add_argument("--release-unfinished-to-morris", action="store_true")
    modes.add_argument("--reconcile-review-history", action="store_true")
    parser.add_argument("--package-dir", type=Path, required=True)
    parser.add_argument("--package-manifest-sha256", required=True)
    parser.add_argument("--assignment-ledger-revision", type=Path)
    parser.add_argument("--assignment-id")
    parser.add_argument("--reviewer-id")
    parser.add_argument("--submitted-at")
    parser.add_argument("--response-json", type=Path)
    parser.add_argument("--corrects-submission-id")
    parser.add_argument("--changed-at")
    parser.add_argument("--reason")
    parser.add_argument("--generated-at")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    common = {
        "root": args.root,
        "package_dir": args.package_dir,
        "expected_manifest_sha256": args.package_manifest_sha256,
        "assignment_ledger_revision": args.assignment_ledger_revision,
    }
    if args.write_returned_review_revision:
        if not all((args.assignment_id, args.reviewer_id, args.submitted_at, args.response_json)):
            parser.error("return revision requires assignment, reviewer, time, and response")
        response_path = _within_root(Path(args.root).resolve(), args.response_json)
        result = write_returned_review_revision(
            **common,
            assignment_id=args.assignment_id,
            reviewer_id=args.reviewer_id,
            submitted_at=args.submitted_at,
            response=json.loads(response_path.read_bytes()),
            corrects_submission_id=args.corrects_submission_id,
        )
    elif args.release_unfinished_to_morris:
        if not all((args.assignment_id, args.changed_at, args.reason, args.output)):
            parser.error("release requires assignment, time, reason, and output")
        result = release_unfinished_assignment_to_morris(
            **common,
            assignment_id=args.assignment_id,
            changed_at=args.changed_at,
            reason=args.reason,
            output_path=args.output,
        )
    else:
        if not all((args.generated_at, args.output)):
            parser.error("reconciliation requires time and output")
        result = reconcile_review_history(
            **common,
            generated_at=args.generated_at,
            output_path=args.output,
        )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
