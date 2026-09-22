from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

import h2h_lit.human_validation_review as review

STAMP = "2026-09-20T23:00:00Z"


def _write_source(path: Path, count: int = 3) -> list[str]:
    record_ids = [f"stable-record:{index:02d}" for index in range(count)]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_order",
                "canonical_id",
                "title",
                "abstract",
                "publication_year",
                "human_E1",
                "human_eligibility_status",
            ],
        )
        writer.writeheader()
        for index, record_id in enumerate(record_ids, start=1):
            writer.writerow(
                {
                    "sample_order": index,
                    "canonical_id": record_id,
                    "title": f"Title {index}",
                    "abstract": f"Abstract {index}",
                    "publication_year": 2020 + index,
                    "human_E1": "",
                    "human_eligibility_status": "",
                }
            )
    return record_ids


def _setup_root(tmp_path: Path, count: int = 3) -> tuple[list[str], Path, Path]:
    source = tmp_path / "input" / "sample.csv"
    records = _write_source(source, count)
    rubric = tmp_path / "rubric.md"
    rubric.write_text("# Frozen rubric\n", encoding="utf-8")
    implementation = tmp_path / "src" / "h2h_lit" / "human_validation_review.py"
    implementation.parent.mkdir(parents=True)
    implementation.write_bytes(Path(review.__file__).read_bytes())
    return records, source, rubric


def _response(record_id: str, *, e3: str = "YES") -> dict:
    decisions = {f"E{index}": "YES" for index in range(1, 8)}
    decisions["E3"] = e3
    status = "ELIGIBLE" if all(value == "YES" for value in decisions.values()) else "EXCLUDED"
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
        "eligibility_status": status,
        "primary_exclusion_reason": (
            None if status == "ELIGIBLE" else "EX_NO_INTERACTIVE_VISUAL_ANALYTICS"
        ),
        "secondary_exclusion_reasons": [],
        "full_text_escalation_required": False,
        "confidence": "HIGH",
        "notes": "",
    }


def _prepare(
    tmp_path: Path, *, plan: dict | None = None, count: int = 3
) -> tuple[list[str], Path, dict]:
    records, source, rubric = _setup_root(tmp_path, count)
    plan_path = None
    if plan is not None:
        plan_path = tmp_path / "assignment-plan.json"
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
    output = tmp_path / "workspace"
    result = review.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=output,
        generated_at=STAMP,
        assignment_plan=plan_path,
    )
    return records, output, result


def test_default_workspace_is_blinded_unassigned_and_missing(tmp_path: Path) -> None:
    records, output, result = _prepare(tmp_path)

    assert result["counts"] == {
        "records": 3,
        "packets": 3,
        "assignments": 3,
        "unassigned": 3,
        "returned_reviews": 0,
    }
    assert result["reconciliation_counts"] == {
        "records_total": 3,
        "missing_reviews": 3,
        "single_reviewed": 0,
        "independently_double_reviewed": 0,
        "double_review_agreements": 0,
        "disagreements": 0,
    }
    manifest = review.validate_review_workspace(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=result["package_manifest_sha256"],
    )
    assert manifest["status"] == "PREPARED_NOT_ASSIGNED_NOT_REVIEWED"
    packets = [
        json.loads(path.read_bytes()) for path in sorted((output / "packets").glob("*.json"))
    ]
    assert {item["record"]["record_id"] for item in packets} == set(records)
    assert all(item["blinded"] is True for item in packets)
    assert all("model_stratification_status" not in json.dumps(item) for item in packets)
    assert all(
        set(item["structured_response"]["criteria"]) == {f"E{i}" for i in range(1, 8)}
        for item in packets
    )
    ledger = json.loads((output / "assignment_ledger.json").read_bytes())
    assert all(item["reviewer_id"] is None for item in ledger["assignments"])
    assert all(item["due_date"] is None for item in ledger["assignments"])
    assert all(item["status"] == "UNASSIGNED" for item in ledger["assignments"])


def test_separate_returns_reconcile_single_double_disagreement_and_missing(
    tmp_path: Path,
) -> None:
    record_ids, source, rubric = _setup_root(tmp_path)
    plan = {
        "assignments": [
            {
                "record_id": record_ids[0],
                "slot_id": "primary",
                "reviewer_id": "reviewer-alpha",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            },
            {
                "record_id": record_ids[0],
                "slot_id": "secondary",
                "reviewer_id": "reviewer-beta",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            },
            {
                "record_id": record_ids[1],
                "slot_id": "primary",
                "reviewer_id": "reviewer-alpha",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            },
        ]
    }
    plan_path = tmp_path / "assignment-plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    output = tmp_path / "workspace"
    prepared = review.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=output,
        generated_at=STAMP,
        assignment_plan=plan_path,
    )
    ledger = json.loads((output / "assignment_ledger.json").read_bytes())
    assignments = {
        (item["record_id"], item["reviewer_id"]): item["assignment_id"]
        for item in ledger["assignments"]
        if item["reviewer_id"]
    }
    alpha_first = review.write_returned_review(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignments[(record_ids[0], "reviewer-alpha")],
        reviewer_id="reviewer-alpha",
        submitted_at="2026-09-22T12:00:00Z",
        response=_response(record_ids[0]),
    )
    beta_first = review.write_returned_review(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignments[(record_ids[0], "reviewer-beta")],
        reviewer_id="reviewer-beta",
        submitted_at="2026-09-22T13:00:00Z",
        response=_response(record_ids[0], e3="NO"),
    )
    review.write_returned_review(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        assignment_id=assignments[(record_ids[1], "reviewer-alpha")],
        reviewer_id="reviewer-alpha",
        submitted_at="2026-09-22T14:00:00Z",
        response=_response(record_ids[1]),
    )

    assert Path(alpha_first["path"]).parent != Path(beta_first["path"]).parent
    result = review.reconcile_returned_reviews(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=prepared["package_manifest_sha256"],
        generated_at="2026-09-23T00:00:00Z",
    )
    assert result["counts"] == {
        "records_total": 3,
        "missing_reviews": 1,
        "single_reviewed": 1,
        "independently_double_reviewed": 1,
        "double_review_agreements": 0,
        "disagreements": 1,
    }
    report = json.loads((tmp_path / result["path"]).read_bytes())
    rows = {item["record_id"]: item for item in report["records"]}
    assert rows[record_ids[0]]["category"] == "INDEPENDENT_DOUBLE_REVIEW_DISAGREEMENT"
    assert rows[record_ids[0]]["disagreement_fields"] == [
        "E3",
        "eligibility_status",
        "primary_exclusion_reason",
    ]
    assert rows[record_ids[1]]["category"] == "SINGLE_REVIEWED"
    assert rows[record_ids[2]]["category"] == "MISSING_REVIEW"


def test_returned_review_is_create_only_and_validates_aggregate(tmp_path: Path) -> None:
    records, source, rubric = _setup_root(tmp_path, 1)
    plan = {
        "assignments": [
            {
                "record_id": records[0],
                "reviewer_id": "reviewer-alpha",
                "due_date": "2026-10-01",
                "status": "ASSIGNED",
            }
        ]
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    output = tmp_path / "workspace"
    prepared = review.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=output,
        generated_at=STAMP,
        assignment_plan=plan_path,
    )
    assignment = json.loads((output / "assignment_ledger.json").read_bytes())["assignments"][0]
    kwargs = {
        "root": tmp_path,
        "package_dir": output,
        "expected_manifest_sha256": prepared["package_manifest_sha256"],
        "assignment_id": assignment["assignment_id"],
        "reviewer_id": "reviewer-alpha",
        "submitted_at": "2026-09-22T12:00:00Z",
        "response": _response(records[0]),
    }
    review.write_returned_review(**kwargs)
    with pytest.raises(review.HumanValidationReviewError, match="never overwrite"):
        review.write_returned_review(**kwargs)

    invalid = _response(records[0])
    invalid["eligibility_status"] = "UNCERTAIN"
    with pytest.raises(review.HumanValidationReviewError, match="aggregate eligibility"):
        review._validate_response(invalid, records[0])


def _response_with_decisions(record_id: str, decisions: dict[str, str]) -> dict:
    outcome = review.recompute_aggregate_outcome(decisions)
    return {
        "record_id": record_id,
        "criteria": {
            key: {
                "decision": value,
                "evidence_quote": f"evidence-{key}",
                "evidence_locator": "title/abstract",
                "rationale": f"rationale-{key}",
            }
            for key, value in decisions.items()
        },
        "eligibility_status": outcome,
        "primary_exclusion_reason": (
            "EX_NO_LIFE_SCIENCE_APPLICATION" if outcome == "EXCLUDED" else None
        ),
        "secondary_exclusion_reasons": [],
        "full_text_escalation_required": outcome == "UNCERTAIN",
        "excluded_record_escalation_rationale": "",
        "confidence": "MEDIUM",
        "notes": "",
    }


def test_aggregate_no_precedes_uncertain_without_mandatory_escalation() -> None:
    decisions = {f"E{index}": "YES" for index in range(1, 8)}
    decisions["E1"] = "NO"
    decisions["E6"] = "UNCERTAIN"
    response = _response_with_decisions("stable-record:01", decisions)

    assert response["eligibility_status"] == "EXCLUDED"
    assert response["full_text_escalation_required"] is False
    review._validate_response(response, "stable-record:01")


def test_aggregate_uncertainty_without_no_requires_escalation() -> None:
    decisions = {f"E{index}": "YES" for index in range(1, 8)}
    decisions["E6"] = "UNCERTAIN"
    response = _response_with_decisions("stable-record:02", decisions)

    assert response["eligibility_status"] == "UNCERTAIN"
    assert response["full_text_escalation_required"] is True
    review._validate_response(response, "stable-record:02")


def test_aggregate_all_yes_is_eligible_without_escalation() -> None:
    decisions = {f"E{index}": "YES" for index in range(1, 8)}
    response = _response_with_decisions("stable-record:03", decisions)

    assert response["eligibility_status"] == "ELIGIBLE"
    assert response["full_text_escalation_required"] is False
    review._validate_response(response, "stable-record:03")


def test_aggregate_rejects_incomplete_responses() -> None:
    decisions = {f"E{index}": "YES" for index in range(1, 7)}
    with pytest.raises(review.HumanValidationReviewError, match="complete E1-E7"):
        review.recompute_aggregate_outcome(decisions)


def test_discretionary_escalation_of_excluded_record_requires_separate_rationale() -> None:
    decisions = {f"E{index}": "YES" for index in range(1, 8)}
    decisions["E1"] = "NO"
    response = _response_with_decisions("stable-record:04", decisions)
    response["full_text_escalation_required"] = True
    with pytest.raises(review.HumanValidationReviewError, match="explicit rationale"):
        review._validate_response(response, "stable-record:04")

    response["excluded_record_escalation_rationale"] = "Resolve a conflicting source."
    review._validate_response(response, "stable-record:04")


def test_reassignment_history_and_tampering_are_checked(tmp_path: Path) -> None:
    records, source, rubric = _setup_root(tmp_path, 1)
    plan = {
        "assignments": [
            {
                "record_id": records[0],
                "reviewer_id": "reviewer-beta",
                "due_date": "2026-10-03",
                "status": "ASSIGNED",
                "reassignment_history": [
                    {
                        "sequence": 1,
                        "from_reviewer_id": "reviewer-alpha",
                        "to_reviewer_id": "reviewer-beta",
                        "changed_at_utc": "2026-09-21T12:00:00Z",
                        "reason": "availability",
                    }
                ],
            }
        ]
    }
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    output = tmp_path / "workspace"
    prepared = review.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=output,
        generated_at=STAMP,
        assignment_plan=plan_path,
    )
    ledger = json.loads((output / "assignment_ledger.json").read_bytes())
    assert (
        ledger["assignments"][0]["reassignment_history"][0]["from_reviewer_id"] == "reviewer-alpha"
    )
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(review.HumanValidationReviewError, match="source binding"):
        review.validate_review_workspace(
            root=tmp_path,
            package_dir=output,
            expected_manifest_sha256=prepared["package_manifest_sha256"],
        )


def test_source_with_existing_human_judgment_is_rejected(tmp_path: Path) -> None:
    _records, source, rubric = _setup_root(tmp_path, 1)
    raw = source.read_text(encoding="utf-8")
    source.write_text(raw.replace(",,\n", ",YES,ELIGIBLE\n"), encoding="utf-8")
    with pytest.raises(review.HumanValidationReviewError, match="human judgments"):
        review.prepare_review_workspace(
            root=tmp_path,
            source_csv=source,
            rubric_path=rubric,
            output_dir=tmp_path / "workspace",
            generated_at=STAMP,
        )


def test_workspace_manifest_hash_is_full_file_sha256(tmp_path: Path) -> None:
    _records, output, result = _prepare(tmp_path, count=1)
    assert (
        result["package_manifest_sha256"]
        == hashlib.sha256((output / "package_manifest.json").read_bytes()).hexdigest()
    )


def test_packet_identity_is_independent_of_assignment_plan(tmp_path: Path) -> None:
    records, source, rubric = _setup_root(tmp_path, 1)
    first = review.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=tmp_path / "unassigned",
        generated_at=STAMP,
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "assignments": [
                    {
                        "record_id": records[0],
                        "reviewer_id": "reviewer-alpha",
                        "due_date": "2026-10-01",
                        "status": "ASSIGNED",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    second = review.prepare_review_workspace(
        root=tmp_path,
        source_csv=source,
        rubric_path=rubric,
        output_dir=tmp_path / "assigned",
        generated_at=STAMP,
        assignment_plan=plan_path,
    )
    first_packet = next((tmp_path / "unassigned" / "packets").glob("*.json"))
    second_packet = next((tmp_path / "assigned" / "packets").glob("*.json"))

    assert first["workspace_id"] == second["workspace_id"]
    assert first_packet.read_bytes() == second_packet.read_bytes()
