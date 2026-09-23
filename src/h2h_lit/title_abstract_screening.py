"""Approved E6-deferred title/abstract screening logic."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROTOCOL_VERSION = "2.0.0"
RETURN_SCHEMA_VERSION = "2.0.0"
ASSESSED_CRITERIA = ("E1", "E2", "E3", "E4", "E5", "E7")
SCIENTIFIC_CRITERIA = ("E1", "E2", "E3", "E4", "E5")
TRI_STATES = frozenset({"YES", "NO", "UNCERTAIN"})
E6_STATUS = "NOT_ASSESSED_AT_THIS_STAGE"
NEXT_ACTIONS = frozenset(
    {
        "NONE",
        "METADATA_RECOVERY",
        "EVIDENCE_RECONCILIATION",
        "TARGETED_SECOND_REVIEW",
        "FULL_TEXT_ASSESSMENT",
    }
)
FAST_TRACK_DISPOSITIONS = frozenset(
    {"EXCLUDE", "ADVANCE_TO_FULL_REPORT_ASSESSMENT", "DEFER"}
)


class TitleAbstractScreeningError(ValueError):
    """Raised when an approved-protocol return is incomplete or invalid."""


def load_protocol_contract(path: str | Path) -> dict[str, Any]:
    contract = json.loads(Path(path).read_text(encoding="utf-8"))
    if contract.get("protocol_version") != PROTOCOL_VERSION:
        raise TitleAbstractScreeningError("unexpected protocol version")
    if contract.get("return_schema_version") != RETURN_SCHEMA_VERSION:
        raise TitleAbstractScreeningError("unexpected return schema version")
    if tuple(contract.get("assessed_criteria", ())) != ASSESSED_CRITERIA:
        raise TitleAbstractScreeningError("unexpected assessed-criteria binding")
    if contract.get("e6", {}).get("status") != E6_STATUS:
        raise TitleAbstractScreeningError("unexpected E6 binding")
    return contract


def validate_responses(responses: dict[str, str], e6_status: str = E6_STATUS) -> None:
    if e6_status != E6_STATUS:
        raise TitleAbstractScreeningError(f"E6 must be {E6_STATUS}")
    if set(responses) != set(ASSESSED_CRITERIA):
        raise TitleAbstractScreeningError("complete E1-E5 and E7 responses are required")
    invalid = {criterion: responses[criterion] for criterion in ASSESSED_CRITERIA if responses[criterion] not in TRI_STATES}
    if invalid:
        raise TitleAbstractScreeningError(f"invalid criterion responses: {invalid}")


def recompute_outcome(responses: dict[str, str], e6_status: str = E6_STATUS) -> str:
    """Recompute outcome without trusting a workbook formula or cached value."""

    validate_responses(responses, e6_status)
    if any(responses[criterion] == "NO" for criterion in SCIENTIFIC_CRITERIA):
        return "EXCLUDED"
    if any(responses[criterion] == "UNCERTAIN" for criterion in SCIENTIFIC_CRITERIA):
        return "UNCERTAIN"
    if responses["E7"] in {"NO", "UNCERTAIN"}:
        return "UNCERTAIN"
    return "INCLUDE"


def recommend_next_action(
    responses: dict[str, str],
    *,
    e6_status: str = E6_STATUS,
    abstract_missing: bool,
    evidence_conflict: bool = False,
    targeted_second_review_requested: bool = False,
) -> str:
    """Apply approved routing precedence independently of the aggregate field."""

    if evidence_conflict:
        return "EVIDENCE_RECONCILIATION"
    try:
        outcome = recompute_outcome(responses, e6_status)
    except TitleAbstractScreeningError:
        return "TARGETED_SECOND_REVIEW"
    if outcome == "EXCLUDED":
        return "NONE"
    if abstract_missing:
        return "METADATA_RECOVERY"
    if targeted_second_review_requested:
        return "TARGETED_SECOND_REVIEW"
    return "FULL_TEXT_ASSESSMENT"


def recommend_fast_track_disposition(
    responses: dict[str, str],
    *,
    e6_status: str = E6_STATUS,
    abstract_missing: bool,
    evidence_conflict: bool = False,
    full_report_available: bool,
) -> str:
    """Map a valid v2 scientific result to the prospective fast-track disposition.

    This does not reinterpret the v2 screening outcome.  It only applies the
    time-bounded operational amendment after independently recomputing that outcome.
    """

    outcome = recompute_outcome(responses, e6_status)
    if evidence_conflict:
        return "DEFER"
    if outcome == "EXCLUDED":
        return "EXCLUDE"
    if abstract_missing or outcome == "UNCERTAIN" or not full_report_available:
        return "DEFER"
    return "ADVANCE_TO_FULL_REPORT_ASSESSMENT"


def formula_from_template(template: str, row: int) -> str:
    if not isinstance(row, int) or row < 1:
        raise TitleAbstractScreeningError("row must be a positive integer")
    return template.replace("{row}", str(row))
