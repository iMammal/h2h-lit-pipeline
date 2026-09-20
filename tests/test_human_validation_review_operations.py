from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

import h2h_lit.human_validation_review as base
import h2h_lit.human_validation_review_operations as operations

STAMP = "2026-09-20T23:30:00Z"


def _response(record_id: str, *, e3: str = "YES", notes: str = "") -> dict:
    decisions = {f"E{index}": "YES" for index in range(1, 8)}
    decisions["E3"] = e3
    excluded = e3 == "NO"
    return {
        "record_id": record_id,
        "criteria": {
            key: {
                "decision": value,
                "evidence_quote": f"evidence-{key}",
                "evidence_locator": "abstract",
                "rationale": f"rationale-{key}",
            }
            for key, value in decisions.items()
        },
        "eligibility_status": "EXCLUDED" if excluded else "ELIGIBLE",
        "primary_exclusion_reason": (
            "EX_NO_INTERACTIVE_VISUAL_ANALYTICS" if excluded else None
        ),
        "secondary_exclusion_reasons": [],
        "full_text_escalation_required": False,
        "confidence": "HIGH",
        "notes": notes,
    }


def _prepare(tmp_path: Path, assignments: list[dict]) -> tuple[str, Path, dict]:
    record_id = "stable-record:01"
    source = tmp_path / "sample.csv"
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_order",
                "canonical_id",
                "title",
                "abstract",
                "publication_year",
                "human_E1",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sample_order": 1,
                "canonical_id": record_id,
                "title": "A stable title",
                "abstract": "A stable abstract",
                "publication_year": "2025",
                "human_E1": "",
            }
        )
    rubric = tmp_path / "rubric.md"
    rubric.write_text("# Frozen rubric\n", encoding="utf-8")
    implementation = tmp_path / "src" / "h2h_lit" / "human_validation_review.py"
    implementation.parent.mkdir(parents=True)
    assert base.__file__ is not None
    implementation.write_bytes(Path(base.__file__).read_bytes())
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"assignments": assignments}), encoding="utf-8")
    output = tmp_path / "workspace"
    prepared = base.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=output,
        generated_at=STAMP,
        assignment_plan=plan,
    )
    return record_id, output, prepared


def _assignment_ids(output: Path) -> dict[str, str]:
    ledger = json.loads((output / "assignment_ledger.json").read_bytes())
    return {item["slot_id"]: item["assignment_id"] for item in ledger["assignments"]}


def test_corrections_are_append_only_and_only_terminal_revision_is_effective(
    tmp_path: Path,
) -> None:
    record_id, output, prepared = _prepare(
        tmp_path,
        [
            {
                "record_id": "stable-record:01",
                "slot_id": "primary",
                "reviewer_id": "reviewer-alpha",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            }
        ],
    )
    assignment_id = _assignment_ids(output)["primary"]
    original = operations.write_returned_review_revision(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignment_id,
        reviewer_id="reviewer-alpha",
        submitted_at="2026-09-21T10:00:00Z",
        response=_response(record_id, notes="original"),
    )
    correction = operations.write_returned_review_revision(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignment_id,
        reviewer_id="reviewer-alpha",
        submitted_at="2026-09-21T11:00:00Z",
        response=_response(record_id, notes="corrected"),
        corrects_submission_id=original["submission_id"],
    )
    assert Path(tmp_path / original["path"]).is_file()
    assert Path(tmp_path / correction["path"]).is_file()
    assert original["path"] != correction["path"]

    result = operations.reconcile_review_history(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        generated_at="2026-09-21T12:00:00Z",
        output_path=tmp_path / "history-reconciliation.json",
    )
    assert result["counts"]["returned_submissions"] == 2
    assert result["counts"]["effective_assignment_reviews"] == 1
    assert result["counts"]["superseded_corrections"] == 1
    assert result["counts"]["single_reviewed"] == 1


def test_repeated_morris_reviews_are_not_independent_double_review(
    tmp_path: Path,
) -> None:
    record_id, output, prepared = _prepare(
        tmp_path,
        [
            {
                "record_id": "stable-record:01",
                "slot_id": "primary",
                "reviewer_id": "morris",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            },
            {
                "record_id": "stable-record:01",
                "slot_id": "secondary",
                "reviewer_id": "morris",
                "due_date": "2026-10-02",
                "status": "ASSIGNED",
            },
        ],
    )
    for index, assignment_id in enumerate(_assignment_ids(output).values(), start=1):
        operations.write_returned_review_revision(
            root=tmp_path,
            package_dir=output,
            expected_manifest_sha256=prepared["package_manifest_sha256"],
            assignment_id=assignment_id,
            reviewer_id="morris",
            submitted_at=f"2026-09-22T1{index}:00:00Z",
            response=_response(record_id, e3="NO" if index == 2 else "YES"),
        )
    result = operations.reconcile_review_history(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        generated_at="2026-09-22T15:00:00Z",
        output_path=tmp_path / "morris-reconciliation.json",
    )
    assert result["counts"]["single_reviewed"] == 1
    assert result["counts"]["independently_double_reviewed"] == 0
    assert result["counts"]["disagreements"] == 0
    assert result["counts"]["same_reviewer_repeat_reviews"] == 1
    report = json.loads((tmp_path / "morris-reconciliation.json").read_bytes())
    assert report["records"][0]["independent_reviewer_count"] == 1


def test_release_to_morris_is_create_only_and_rejects_completed_work(
    tmp_path: Path,
) -> None:
    record_id, output, prepared = _prepare(
        tmp_path,
        [
            {
                "record_id": "stable-record:01",
                "slot_id": "primary",
                "reviewer_id": "reviewer-alpha",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            }
        ],
    )
    assignment_id = _assignment_ids(output)["primary"]
    original_ledger = (output / "assignment_ledger.json").read_bytes()
    revision_path = tmp_path / "release-to-morris.json"
    operations.release_unfinished_assignment_to_morris(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignment_id,
        changed_at="2026-09-23T10:00:00Z",
        reason="Reviewer explicitly released unfinished work.",
        output_path=revision_path,
    )
    assert (output / "assignment_ledger.json").read_bytes() == original_ledger
    revision = json.loads(revision_path.read_bytes())
    revised = revision["assignments"][0]
    assert revised["reviewer_id"] == "morris"
    assert revised["due_date"] is None
    assert revised["reassignment_history"][-1]["action"] == "RELEASED_AND_REASSIGNED"

    operations.write_returned_review_revision(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignment_id,
        reviewer_id="reviewer-alpha",
        submitted_at="2026-09-23T11:00:00Z",
        response=_response(record_id),
    )
    with pytest.raises(
        base.HumanValidationReviewError,
        match="completed judgments are preserved",
    ):
        operations.release_unfinished_assignment_to_morris(
            root=tmp_path,
            package_dir=output,
            expected_manifest_sha256=prepared["package_manifest_sha256"],
            assignment_id=assignment_id,
            changed_at="2026-09-23T12:00:00Z",
            reason="Too late.",
            output_path=tmp_path / "late-release.json",
        )
