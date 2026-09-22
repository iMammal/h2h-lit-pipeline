"""Memory-bounded validation for returned title/abstract review workbooks."""

from __future__ import annotations

import argparse
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from h2h_lit.human_validation_review import (
    EXCLUSION_REASONS,
    TRI_STATES,
    HumanValidationReviewError,
    recompute_aggregate_outcome,
)

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
_CELL_REF = re.compile(r"^([A-Z]+)[0-9]+$")


def _column_index(cell_ref: str) -> int:
    match = _CELL_REF.match(cell_ref)
    if match is None:
        raise HumanValidationReviewError(f"invalid workbook cell reference: {cell_ref}")
    result = 0
    for character in match.group(1):
        result = result * 26 + ord(character) - ord("A") + 1
    return result


def _shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        handle = archive.open("xl/sharedStrings.xml")
    except KeyError:
        return []
    values: list[str] = []
    with handle:
        for event, element in ET.iterparse(handle, events=("end",)):
            if element.tag == f"{{{_MAIN_NS}}}si":
                values.append("".join(node.text or "" for node in element.iter(f"{{{_MAIN_NS}}}t")))
                element.clear()
    return values


def _worksheet_paths(archive: zipfile.ZipFile) -> dict[str, str]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    targets = {
        relation.attrib["Id"]: relation.attrib["Target"].lstrip("/")
        for relation in relationships.findall(f"{{{_PKG_REL_NS}}}Relationship")
    }
    result: dict[str, str] = {}
    for sheet in workbook.findall(f".//{{{_MAIN_NS}}}sheet"):
        relationship_id = sheet.attrib[f"{{{_REL_NS}}}id"]
        target = targets[relationship_id]
        result[sheet.attrib["name"]] = target if target.startswith("xl/") else f"xl/{target}"
    return result


def _cell_payload(cell: ET.Element, shared_strings: list[str]) -> dict[str, Any]:
    value_type = cell.attrib.get("t")
    formula_node = cell.find(f"{{{_MAIN_NS}}}f")
    value_node = cell.find(f"{{{_MAIN_NS}}}v")
    if value_type == "inlineStr":
        value: Any = "".join(
            node.text or "" for node in cell.iter(f"{{{_MAIN_NS}}}t")
        )
    elif value_node is None or value_node.text in (None, ""):
        value = None
    elif value_type == "s":
        value = shared_strings[int(value_node.text)]
    elif value_type in {"str", "e"}:
        value = value_node.text
    elif value_type == "b":
        value = value_node.text == "1"
    else:
        raw = value_node.text
        try:
            value = int(raw)
        except ValueError:
            try:
                value = float(raw)
            except ValueError:
                value = raw
    return {
        "value": value,
        "formula": None if formula_node is None else formula_node.text,
    }


def _iter_rows(
    archive: zipfile.ZipFile, sheet_path: str, shared_strings: list[str]
) -> Iterator[tuple[int, dict[int, dict[str, Any]]]]:
    with archive.open(sheet_path) as handle:
        for event, element in ET.iterparse(handle, events=("end",)):
            if element.tag != f"{{{_MAIN_NS}}}row":
                continue
            cells: dict[int, dict[str, Any]] = {}
            for cell in element.findall(f"{{{_MAIN_NS}}}c"):
                cells[_column_index(cell.attrib["r"])] = _cell_payload(cell, shared_strings)
            yield int(element.attrib["r"]), cells
            element.clear()


def _text(cells: dict[int, dict[str, Any]], column: int) -> str:
    value = cells.get(column, {}).get("value")
    return "" if value is None else str(value).strip()


def validate_review_workbook(path: str | Path) -> dict[str, Any]:
    """Recompute workbook outcomes and report cache and escalation inconsistencies.

    Formula cache values are captured for diagnostics only. They never determine the
    aggregate outcome or whether a row is complete.
    """

    workbook_path = Path(path)
    with zipfile.ZipFile(workbook_path) as archive:
        shared_strings = _shared_strings(archive)
        worksheets = _worksheet_paths(archive)
        if "Instructions" not in worksheets or "Reviews" not in worksheets:
            raise HumanValidationReviewError(
                "review workbook must contain Instructions and Reviews sheets"
            )

        reviewer_id = ""
        for row_number, cells in _iter_rows(
            archive, worksheets["Instructions"], shared_strings
        ):
            if row_number == 3 and _text(cells, 1) == "Reviewer ID":
                reviewer_id = _text(cells, 2)
                break

        header_columns: dict[str, int] = {}
        row_reports: list[dict[str, Any]] = []
        for row_number, cells in _iter_rows(archive, worksheets["Reviews"], shared_strings):
            if row_number == 3:
                header_columns = {
                    _text(cells, column): column for column in cells if _text(cells, column)
                }
                continue
            if row_number < 4:
                continue
            record_id = _text(cells, header_columns.get("Stable record ID", 1))
            if not record_id:
                continue
            criteria = {
                criterion: _text(cells, header_columns.get(criterion, 0))
                for criterion in (f"E{index}" for index in range(1, 8))
            }
            problems: list[str] = []
            if any(value not in TRI_STATES for value in criteria.values()):
                computed_outcome = None
                problems.append("INCOMPLETE_OR_INVALID_CRITERIA")
            else:
                computed_outcome = recompute_aggregate_outcome(criteria)

            outcome_column = header_columns.get("Eligibility status", 12)
            outcome_cell = cells.get(outcome_column, {})
            cached_outcome = outcome_cell.get("value")
            if cached_outcome in (None, ""):
                cache_status = "BLANK"
            elif computed_outcome is not None and str(cached_outcome) == computed_outcome:
                cache_status = "MATCH"
            else:
                cache_status = "STALE_OR_INCORRECT"

            exclusion_reason = _text(
                cells, header_columns.get("Primary exclusion reason", 13)
            )
            escalation = _text(cells, header_columns.get("Full-text escalation", 14))
            escalation_rationale = _text(
                cells,
                header_columns.get("Excluded-record escalation rationale", 0),
            )
            if computed_outcome == "EXCLUDED":
                if exclusion_reason not in EXCLUSION_REASONS:
                    problems.append("EXCLUDED_WITHOUT_VALID_PRIMARY_REASON")
                if escalation == "YES" and not escalation_rationale:
                    problems.append("EXCLUDED_ESCALATION_WITHOUT_EXPLICIT_RATIONALE")
            elif computed_outcome == "UNCERTAIN" and escalation != "YES":
                problems.append("UNCERTAIN_WITHOUT_REQUIRED_ESCALATION")
            elif computed_outcome == "ELIGIBLE" and escalation == "YES":
                problems.append("ELIGIBLE_WITH_ESCALATION")
            if escalation not in {"YES", "NO"}:
                problems.append("INVALID_OR_MISSING_ESCALATION")

            row_reports.append(
                {
                    "row": row_number,
                    "record_id": record_id,
                    "criteria": criteria,
                    "computed_outcome": computed_outcome,
                    "formula": outcome_cell.get("formula"),
                    "cached_outcome": cached_outcome,
                    "cache_status": cache_status,
                    "primary_exclusion_reason": exclusion_reason or None,
                    "full_text_escalation": escalation or None,
                    "excluded_record_escalation_rationale": escalation_rationale,
                    "inconsistencies": problems,
                }
            )

    outcome_counts: dict[str, int] = {}
    cache_counts: dict[str, int] = {}
    inconsistency_counts: dict[str, int] = {}
    for row in row_reports:
        outcome = row["computed_outcome"] or "INVALID"
        outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
        cache = row["cache_status"]
        cache_counts[cache] = cache_counts.get(cache, 0) + 1
        for problem in row["inconsistencies"]:
            inconsistency_counts[problem] = inconsistency_counts.get(problem, 0) + 1
    return {
        "workbook": str(workbook_path.resolve()),
        "reviewer_id": reviewer_id,
        "records": len(row_reports),
        "computed_outcome_counts": outcome_counts,
        "formula_cache_counts": cache_counts,
        "inconsistency_counts": inconsistency_counts,
        "rows": row_reports,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workbook", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = validate_review_workbook(args.workbook)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
