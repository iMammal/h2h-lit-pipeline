from __future__ import annotations

import pytest

from h2h_lit.title_abstract_screening import (
    E6_STATUS,
    TitleAbstractScreeningError,
    recommend_fast_track_disposition,
    recommend_next_action,
    recompute_outcome,
)


def answers(**overrides: str) -> dict[str, str]:
    result = {criterion: "YES" for criterion in ("E1", "E2", "E3", "E4", "E5", "E7")}
    result.update(overrides)
    return result


@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        (answers(), "INCLUDE"),
        (answers(E5="NO"), "EXCLUDED"),
        (answers(E5="UNCERTAIN"), "UNCERTAIN"),
        (answers(E2="NO", E5="UNCERTAIN"), "EXCLUDED"),
        (answers(E7="NO"), "UNCERTAIN"),
        (answers(E7="UNCERTAIN"), "UNCERTAIN"),
    ],
)
def test_approved_aggregate(responses: dict[str, str], expected: str) -> None:
    assert recompute_outcome(responses) == expected


def test_e6_is_deferred_not_yes() -> None:
    with pytest.raises(TitleAbstractScreeningError, match="E6 must be"):
        recompute_outcome(answers(), "YES")


@pytest.mark.parametrize("bad", [{"E1": "YES"}, answers(E3=""), answers(E3="MAYBE")])
def test_incomplete_or_invalid_responses_are_rejected(bad: dict[str, str]) -> None:
    with pytest.raises(TitleAbstractScreeningError):
        recompute_outcome(bad)


def test_routing_precedence() -> None:
    assert recommend_next_action(
        answers(E2="NO"), abstract_missing=True
    ) == "NONE"
    assert recommend_next_action(
        answers(), abstract_missing=True
    ) == "METADATA_RECOVERY"
    assert recommend_next_action(
        answers(E7="NO"), abstract_missing=False
    ) == "FULL_TEXT_ASSESSMENT"
    assert recommend_next_action(
        answers(E2="NO"), abstract_missing=True, evidence_conflict=True
    ) == "EVIDENCE_RECONCILIATION"
    assert recommend_next_action(
        {"E1": "YES"}, abstract_missing=False
    ) == "TARGETED_SECOND_REVIEW"
    assert recommend_next_action(
        answers(), abstract_missing=False, targeted_second_review_requested=True
    ) == "TARGETED_SECOND_REVIEW"
    assert recommend_next_action(answers(), abstract_missing=False) == "FULL_TEXT_ASSESSMENT"


def test_literal_deferred_e6_constant() -> None:
    assert E6_STATUS == "NOT_ASSESSED_AT_THIS_STAGE"


@pytest.mark.parametrize(
    ("responses", "abstract_missing", "evidence_conflict", "full_report_available", "expected"),
    [
        (answers(), False, False, True, "ADVANCE_TO_FULL_REPORT_ASSESSMENT"),
        (answers(E3="NO"), False, False, True, "EXCLUDE"),
        (answers(E7="UNCERTAIN"), False, False, True, "DEFER"),
        (answers(), True, False, True, "DEFER"),
        (answers(), False, True, True, "DEFER"),
        (answers(), False, False, False, "DEFER"),
    ],
)
def test_fast_track_disposition_is_separate_from_scientific_outcome(
    responses: dict[str, str],
    abstract_missing: bool,
    evidence_conflict: bool,
    full_report_available: bool,
    expected: str,
) -> None:
    assert recommend_fast_track_disposition(
        responses,
        abstract_missing=abstract_missing,
        evidence_conflict=evidence_conflict,
        full_report_available=full_report_available,
    ) == expected


def test_fast_track_rejects_incomplete_responses() -> None:
    with pytest.raises(TitleAbstractScreeningError):
        recommend_fast_track_disposition(
            {"E1": "YES"}, abstract_missing=False, full_report_available=True
        )
