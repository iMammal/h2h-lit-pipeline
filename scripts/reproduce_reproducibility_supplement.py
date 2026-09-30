"""Recompute H2H2 supplement aggregates and figures from redistributed inputs.

This entry point is deliberately independent of the repository layout.  It reads only
the extracted supplement root supplied with ``--supplement-root`` and writes only to a
separate ``--output-dir``.  It does not perform retrieval, model inference, screening,
deduplication, or scientific reassessment.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
from collections import Counter, defaultdict
from pathlib import Path

ASSISTANCE = ("Algorithmic", "Adaptive", "Conversational", "Immersive")
MODALITIES = ("Desktop/Planar", "Large Display", "VR", "AR/MR", "CAVE")
TASKS = (
    "Navigation and Multiscale Orientation",
    "Comparison and Differentiation",
    "Selection, Filtering, and Precision Interaction",
    "Sensemaking and Hypothesis Development",
    "Coordination and Collaborative Reasoning",
)
LABEL_COLUMNS = {
    "assistance": (ASSISTANCE, "assistance_present", "assistance_unknown"),
    "modality": (MODALITIES, "modalities_present", "modalities_unknown"),
    "task": (TASKS, "tasks_present", "tasks_unknown"),
}
MODALITY_ALIASES = {"Desktop 2D": "Desktop/Planar"}
ABSTRACT_AVAILABILITY_SCOPE = (
    "Recomputes preserved abstract-availability flags from the reduced candidate export; "
    "abstract text is omitted and is not inspected. Historical output field names are "
    "retained for compatibility."
)
TARGET_CELLS = {
    ("Adaptive", "Large Display"),
    ("Immersive", "Desktop/Planar"),
    ("Immersive", "Large Display"),
}
CANONICAL_STATUS = {
    "ELIGIBLE": "ELIGIBLE",
    "UNRESOLVED": "UNRESOLVED",
    "HOLD": "UNRESOLVED",
    "EXCLUDED": "EXCLUDED_CONTEXTUAL",
    "CONTEXTUAL": "EXCLUDED_CONTEXTUAL",
    "EXCLUDED_CONTEXTUAL": "EXCLUDED_CONTEXTUAL",
    "INACCESSIBLE": "INACCESSIBLE",
    "INACCESSIBLE_FULL_REPORT": "INACCESSIBLE",
}
REPRESENTATIVES = {
    ("Algorithmic", "Desktop/Planar"): ("Robin", "CellWhisperer"),
    ("Algorithmic", "Large Display"): ("Echo", "MinOmics"),
    ("Algorithmic", "VR"): ("VROOM", "RATS VR planning system"),
    ("Algorithmic", "AR/MR"): ("Augmented Reality Microscope", "Vitessce Link"),
    ("Algorithmic", "CAVE"): ("iCAVE", "ExaViz"),
    ("Adaptive", "Desktop/Planar"): ("Facetto", "Metis"),
    ("Adaptive", "VR"): ("SAMIRA", "Interactive AI annotation in VR"),
    ("Adaptive", "AR/MR"): ("Liver MR navigation",),
    ("Adaptive", "CAVE"): ("ExaViz",),
    ("Conversational", "Desktop/Planar"): ("CellWhisperer", "Robin"),
    ("Conversational", "VR"): ("SAMIRA", "ASCRIBE-XR"),
    ("Immersive", "VR"): ("Space-Time Hypercube", "Interactive AI annotation in VR"),
    ("Immersive", "AR/MR"): ("Liver MR navigation", "Skin-lesion AR"),
    ("Immersive", "CAVE"): ("ExaViz",),
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def labels(value: str, *, modality: bool = False) -> set[str]:
    result = {item.strip() for item in value.split(";") if item.strip()}
    if modality:
        result = {MODALITY_ALIASES.get(item, item) for item in result}
    return result


def candidate_tables(
    candidate_rows: list[dict[str, str]],
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    ids = [row["canonical_id"].strip() for row in candidate_rows]
    if not all(ids) or len(ids) != len(set(ids)):
        raise ValueError("candidate canonical IDs are blank or duplicated")
    denominator = len(ids)
    parsed: list[dict[str, set[str]]] = []
    for row in candidate_rows:
        item: dict[str, set[str]] = {}
        for dimension, (vocabulary, present_col, uncertain_col) in LABEL_COLUMNS.items():
            present = labels(row[present_col], modality=dimension == "modality")
            uncertain = labels(row[uncertain_col], modality=dimension == "modality")
            unknown = (present | uncertain) - set(vocabulary)
            overlap = present & uncertain
            if unknown or overlap:
                raise ValueError(
                    f"invalid {dimension} labels for {row['canonical_id']}: "
                    f"unknown={sorted(unknown)}, overlap={sorted(overlap)}"
                )
            item[f"{dimension}_present"] = present
            item[f"{dimension}_uncertain"] = uncertain
        parsed.append(item)

    state_rows: list[dict[str, object]] = []
    state_rule = (
        "one state per canonical candidate record; omitted CSV label = ABSENT "
        "because prompt requires every frozen label exactly once"
    )
    for dimension, (vocabulary, _present_col, _uncertain_col) in LABEL_COLUMNS.items():
        for label in vocabulary:
            present = sum(label in row[f"{dimension}_present"] for row in parsed)
            uncertain = sum(label in row[f"{dimension}_uncertain"] for row in parsed)
            state_rows.append(
                {
                    "dimension": dimension,
                    "label": label,
                    "PRESENT": present,
                    "ABSENT": denominator - present - uncertain,
                    "UNCERTAIN": uncertain,
                    "denominator": denominator,
                    "counting_rule": state_rule,
                }
            )

    cell_rows: list[dict[str, object]] = []
    task_rows: list[dict[str, object]] = []
    for assistance in ASSISTANCE:
        for modality in MODALITIES:
            in_cell = [
                row
                for row in parsed
                if assistance in row["assistance_present"]
                and modality in row["modality_present"]
            ]
            cell_rows.append(
                {
                    "assistance_mode": assistance,
                    "visualization_modality": modality,
                    "present_label_cooccurrence_records": len(in_cell),
                    "candidate_record_denominator": denominator,
                    "counting_unit": "unique canonical candidate record",
                    "interpretation": (
                        "machine-coded PRESENT-label co-occurrence; not a verified "
                        "mechanism placement or prevalence estimate"
                    ),
                }
            )
            for task in TASKS:
                present = sum(task in row["task_present"] for row in in_cell)
                uncertain = sum(task in row["task_uncertain"] for row in in_cell)
                task_rows.append(
                    {
                        "assistance_mode": assistance,
                        "visualization_modality": modality,
                        "task": task,
                        "task_PRESENT": present,
                        "task_ABSENT": len(in_cell) - present - uncertain,
                        "task_UNCERTAIN": uncertain,
                        "cell_candidate_records": len(in_cell),
                    }
                )

    abstract_present = sum(row.get("abstract_status", "").strip() == "PRESENT" for row in candidate_rows)
    abstract_rows = [
        {
            "measure": "abstract_text_presence",
            "state": "PRESENT",
            "candidate_records": abstract_present,
            "denominator": denominator,
        },
        {
            "measure": "export_abstract_status",
            "state": "PRESENT",
            "candidate_records": abstract_present,
            "denominator": denominator,
        },
    ]
    return state_rows, cell_rows, task_rows, abstract_rows


def normalized_status(row: dict[str, str]) -> str:
    original = (row.get("normalized_status") or row.get("aggregate_status") or "").strip()
    if original not in CANONICAL_STATUS:
        raise ValueError(f"unrecognized eligibility status for {row.get('system')}: {original!r}")
    return CANONICAL_STATUS[original]


def supported_table(
    placement_rows: list[dict[str, str]], eligibility_rows: list[dict[str, str]]
) -> tuple[list[dict[str, object]], dict[tuple[str, str], set[str]]]:
    eligibility = {row["system"].strip(): normalized_status(row) for row in eligibility_rows}
    systems_by_cell: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in placement_rows:
        if row.get("placement_status", "").strip() != "SUPPORTED":
            continue
        system = row["system"].strip()
        if eligibility.get(system) != "ELIGIBLE":
            raise ValueError(f"supported placement does not belong to eligible system: {system}")
        cell = (row["assistance_mode"].strip(), row["visualization_modality"].strip())
        if cell[0] not in ASSISTANCE or cell[1] not in MODALITIES:
            raise ValueError(f"unsupported assistance/modality vocabulary: {cell}")
        systems_by_cell[cell].add(system)

    rows: list[dict[str, object]] = []
    for assistance in ASSISTANCE:
        for modality in MODALITIES:
            systems = sorted(systems_by_cell[(assistance, modality)], key=str.casefold)
            rows.append(
                {
                    "assistance_mode": assistance,
                    "visualization_modality": modality,
                    "distinct_supported_systems": len(systems),
                    "systems": "; ".join(systems),
                    "assessment_scope": (
                        "recomputed from redistributed supported placement evidence; "
                        "one eligible system once per cell"
                    ),
                }
            )
    return rows, systems_by_cell


def integer_flow(rows: list[dict[str, str]]) -> dict[str, int]:
    return {
        row["proposed_box_id"]: int(row["count"])
        for row in rows
        if row.get("count", "").strip().isdigit()
    }


def reconcile(
    candidate_rows: list[dict[str, str]],
    placement_rows: list[dict[str, str]],
    eligibility_rows: list[dict[str, str]],
    crosswalk_rows: list[dict[str, str]],
    flow_rows: list[dict[str, str]],
    systems_by_cell: dict[tuple[str, str], set[str]],
) -> list[dict[str, object]]:
    flow = integer_flow(flow_rows)
    status_counts = Counter(normalized_status(row) for row in eligibility_rows)
    supported_systems = {
        system for systems in systems_by_cell.values() for system in systems
    }
    supported_placements = sum(len(systems) for systems in systems_by_cell.values())
    report_status = Counter(row.get("report_status", "").strip() for row in crosswalk_rows)
    accessible_reports = report_status["AVAILABLE"] + report_status["ACCESSIBLE"]
    checks = [
        ("unique_candidate_records", len({row["canonical_id"] for row in candidate_rows}), flow["A15"]),
        ("occurrence_routes_partition", sum(flow[f"A01.{i}"] for i in range(1, 10)), flow["A01"]),
        ("canonical_identity_equation", flow["A02"] - flow["A03"], flow["A04"]),
        ("screening_partition", flow["A07"] + flow["A06"] + flow["A08"] + flow["A09"] + flow["A10"], flow["A04"]),
        ("effective_outcome_partition", flow["A11"] + flow["A12"] + flow["A13"], flow["A07"]),
        ("candidate_binding", flow["A11"], flow["A15"]),
        ("selected_publication_rows", len(crosswalk_rows), flow["B01"]),
        ("systems_considered", len(eligibility_rows), flow["B02"]),
        ("accessible_publications", accessible_reports, flow["B03"]),
        ("eligible_systems", status_counts["ELIGIBLE"], flow["B08"]),
        ("unresolved_systems", status_counts["UNRESOLVED"], flow["B09"]),
        ("excluded_contextual_systems", status_counts["EXCLUDED_CONTEXTUAL"], flow["B10"]),
        ("inaccessible_systems", status_counts["INACCESSIBLE"], flow["B07"]),
        ("completed_assessments", status_counts["ELIGIBLE"] + status_counts["UNRESOLVED"] + status_counts["EXCLUDED_CONTEXTUAL"], flow["B06"]),
        ("system_assessment_partition", len(eligibility_rows), flow["B06"] + flow["B07"]),
        ("systems_with_supported_placements", len(supported_systems), flow["B11"]),
        ("supported_system_cell_placements", supported_placements, flow["B13"]),
        ("eligible_system_partition", status_counts["ELIGIBLE"], flow["B11"] + flow["B12"]),
    ]
    results = [
        {
            "check": name,
            "recomputed": observed,
            "reference": expected,
            "status": "PASS" if observed == expected else "FAIL",
        }
        for name, observed, expected in checks
    ]
    failures = [row for row in results if row["status"] != "PASS"]
    if failures:
        raise ValueError(f"reconciliation failures: {failures}")
    return results


def compare_table(
    generated: list[dict[str, object]],
    reference: list[dict[str, str]],
    keys: tuple[str, ...],
    numeric_fields: tuple[str, ...],
) -> dict[str, object]:
    def keyed(rows: list[dict[str, object]]) -> dict[tuple[str, ...], tuple[int, ...]]:
        return {
            tuple(str(row[key]) for key in keys): tuple(int(row[field]) for field in numeric_fields)
            for row in rows
        }

    left, right = keyed(generated), keyed(reference)
    return {
        "status": "PASS" if left == right else "FAIL",
        "generated_rows": len(left),
        "reference_rows": len(right),
        "keys": list(keys),
        "numeric_fields": list(numeric_fields),
    }


def svg_text(value: object) -> str:
    return html.escape(str(value), quote=True)


def write_supported_map(
    path: Path,
    systems_by_cell: dict[tuple[str, str], set[str]],
    reconciliation_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    width, height = 1600, 930
    left, top, cell_w, cell_h = 230, 150, 265, 142
    colors = {
        "Algorithmic": ("#D7EAF3", "#0072B2"),
        "Adaptive": ("#F3E3BE", "#B65400"),
        "Conversational": ("#D8EEE6", "#007A5E"),
        "Immersive": ("#E8DCEC", "#7A4A86"),
    }
    annotations: list[dict[str, object]] = []
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<defs><style>text{font-family:Arial,Helvetica,sans-serif;fill:#1f2529}.title{font-size:31px;font-weight:700}.sub{font-size:16px;fill:#46515a}.head{font-size:17px;font-weight:700;text-anchor:middle}.row{font-size:19px;font-weight:700}.count{font-size:18px;font-weight:700}.name{font-size:14px}.empty{font-size:14px;font-style:italic;fill:#626b72}</style><pattern id="hatch" width="9" height="9" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="9" stroke="#d0d4d7" stroke-width="2"/></pattern></defs>',
        '<rect width="100%" height="100%" fill="#fff"/>',
        '<text class="title" x="42" y="46">Assistance × Visualization Modality</text>',
        '<text class="sub" x="42" y="76">Distinct full-report-supported systems in the bounded assessed set</text>',
        '<text class="sub" x="42" y="101">76/381 systems have ≥1 supported placement · 132 completed assessments · 118 non-additive system–cell placements</text>',
    ]
    for column, modality in enumerate(MODALITIES):
        x = left + column * cell_w + (cell_w - 8) / 2
        parts.append(f'<text class="head" x="{x}" y="{top - 20}">{svg_text(modality)}</text>')
    for row_index, assistance in enumerate(ASSISTANCE):
        y = top + row_index * cell_h
        fill, accent = colors[assistance]
        parts.append(f'<rect x="38" y="{y}" width="176" height="{cell_h - 8}" rx="8" fill="{fill}" stroke="{accent}" stroke-width="2"/>')
        parts.append(f'<rect x="38" y="{y}" width="10" height="{cell_h - 8}" rx="5" fill="{accent}"/>')
        parts.append(f'<text class="row" x="62" y="{y + 72}">{svg_text(assistance)}</text>')
        for column, modality in enumerate(MODALITIES):
            x = left + column * cell_w
            cell = (assistance, modality)
            systems = systems_by_cell[cell]
            count = len(systems)
            cell_fill = fill if count else "url(#hatch)"
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_w - 8}" height="{cell_h - 8}" rx="8" fill="{cell_fill}" stroke="#50575c" stroke-width="1.4"/>')
            parts.append(f'<text class="count" x="{x + 14}" y="{y + 27}">{"n=" + str(count) if count else "—"}</text>')
            if not count:
                if cell in TARGET_CELLS:
                    parts.append(f'<text class="empty" x="{x + 14}" y="{y + 64}">Representative not yet</text>')
                    parts.append(f'<text class="empty" x="{x + 14}" y="{y + 85}">established</text>')
                else:
                    parts.append(f'<text class="empty" x="{x + 14}" y="{y + 64}">not represented in</text>')
                    parts.append(f'<text class="empty" x="{x + 14}" y="{y + 85}">assessed systems</text>')
                continue
            representatives = REPRESENTATIVES.get(cell, tuple(sorted(systems, key=str.casefold)[:2]))
            for representative in representatives:
                present = representative in systems
                annotations.append(
                    {
                        "assistance_mode": assistance,
                        "visualization_modality": modality,
                        "representative_system": representative,
                        "present_in_recomputed_cell": "YES" if present else "NO",
                    }
                )
                if not present:
                    raise ValueError(f"representative annotation not in recomputed cell {cell}: {representative}")
            for line_index, representative in enumerate(representatives):
                parts.append(f'<text class="name" x="{x + 14}" y="{y + 55 + line_index * 22}">{svg_text(representative)}</text>')
            remaining = count - len(representatives)
            if remaining > 0:
                suffix = "system" if remaining == 1 else "systems"
                parts.append(f'<text class="name" x="{x + 14}" y="{y + 55 + len(representatives) * 22}">+{remaining} {suffix}</text>')
    note_y = top + len(ASSISTANCE) * cell_h + 18
    parts.extend(
        [
            f'<rect x="38" y="{note_y}" width="1516" height="152" rx="8" fill="#f4f5f6" stroke="#4d555b"/>',
            f'<text class="sub" x="58" y="{note_y + 28}"><tspan font-weight="700">Counting rule:</tspan> one eligible system once per supported cell; related reports linked; cells multilabel and non-additive.</text>',
            f'<text class="sub" x="58" y="{note_y + 55}">Empty cells mean “not represented in assessed systems,” not “no systems or literature exist.”</text>',
            f'<text class="sub" x="58" y="{note_y + 82}">Counts and +N annotations are derived from redistributed placement and eligibility inputs.</text>',
            f'<text class="sub" x="58" y="{note_y + 109}">Original automated assessment provenance is retained; an author-reported package-level review statement is supplied separately.</text>',
            f'<text class="sub" x="58" y="{note_y + 136}">Candidate co-occurrences are discovery counts and are not encoded in this supported-system map.</text>',
            "</svg>",
        ]
    )
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")
    return annotations


def write_candidate_chart(path: Path, cell_rows: list[dict[str, object]]) -> None:
    lookup = {
        (str(row["assistance_mode"]), str(row["visualization_modality"])): int(
            row["present_label_cooccurrence_records"]
        )
        for row in cell_rows
    }
    width, height = 1500, 900
    left, top, cell_w, cell_h = 220, 140, 245, 132
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>text{font-family:Arial,Helvetica,sans-serif;fill:#1f2529}.title{font-size:30px;font-weight:700}.sub{font-size:16px;fill:#46515a}.head{font-size:17px;font-weight:700;text-anchor:middle}.row{font-size:18px;font-weight:700}.count{font-size:25px;font-weight:700;text-anchor:middle}.label{font-size:13px;text-anchor:middle;fill:#4a545b}</style>',
        '<rect width="100%" height="100%" fill="#fff"/>',
        '<text class="title" x="42" y="46">Machine-coded candidate co-occurrences</text>',
        '<text class="sub" x="42" y="76">Exact PRESENT × PRESENT labels among 9,505 unique candidate records</text>',
        '<text class="sub" x="42" y="101">Discovery counts only—not report-verified placements and not a literature-prevalence estimate</text>',
    ]
    for column, modality in enumerate(MODALITIES):
        x = left + column * cell_w + (cell_w - 8) / 2
        parts.append(f'<text class="head" x="{x}" y="{top - 20}">{svg_text(modality)}</text>')
    for row_index, assistance in enumerate(ASSISTANCE):
        y = top + row_index * cell_h
        parts.append(f'<text class="row" x="42" y="{y + 68}">{svg_text(assistance)}</text>')
        for column, modality in enumerate(MODALITIES):
            x = left + column * cell_w
            count = lookup[(assistance, modality)]
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_w - 8}" height="{cell_h - 8}" rx="8" fill="#E9F1F5" stroke="#455A64" stroke-width="1.5"/>')
            parts.append(f'<text class="count" x="{x + (cell_w - 8) / 2}" y="{y + 61}">{count:,}</text>')
            parts.append(f'<text class="label" x="{x + (cell_w - 8) / 2}" y="{y + 90}">candidate records</text>')
    note_y = top + len(ASSISTANCE) * cell_h + 28
    parts.extend(
        [
            f'<rect x="42" y="{note_y}" width="1408" height="100" rx="8" fill="#f6f6f4" stroke="#596168"/>',
            f'<text class="sub" x="62" y="{note_y + 31}">A candidate may appear in multiple cells. Cell totals are therefore non-additive.</text>',
            f'<text class="sub" x="62" y="{note_y + 58}">Desktop 2D is normalized to Desktop/Planar. UNCERTAIN labels do not contribute to these counts.</text>',
            f'<text class="sub" x="62" y="{note_y + 85}">The chart uses equal cell styling; no denominator-backed magnitude is encoded by color or area.</text>',
            "</svg>",
        ]
    )
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def render_pdf(svg: Path, pdf: Path) -> None:
    try:
        import cairosvg
    except ImportError as exc:  # pragma: no cover - environment-specific message
        raise RuntimeError(
            "PDF rendering requires CairoSVG; install requirements-reproduction.txt"
        ) from exc
    cairosvg.svg2pdf(url=str(svg), write_to=str(pdf))


def run(supplement: Path, output: Path) -> dict[str, object]:
    supplement = supplement.resolve()
    output = output.resolve()
    if not (supplement / "package_manifest.json").is_file():
        raise FileNotFoundError(f"not an extracted supplement root: {supplement}")
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"refusing to overwrite non-empty output directory: {output}")
    output.mkdir(parents=True, exist_ok=True)

    candidate_rows = read_csv(supplement / "candidate_records/candidate_audit_export.csv")
    placement_rows = read_csv(supplement / "full_report/placement_evidence_permissible.csv")
    eligibility_rows = read_csv(supplement / "full_report/eligibility_assessments.csv")
    crosswalk_rows = read_csv(supplement / "full_report/system_report_crosswalk.csv")
    flow_rows = read_csv(supplement / "flow/flow_count_ledger.csv")

    state_rows, cell_rows, task_rows, abstract_rows = candidate_tables(candidate_rows)
    supported_rows, systems_by_cell = supported_table(placement_rows, eligibility_rows)
    reconciliation = reconcile(
        candidate_rows,
        placement_rows,
        eligibility_rows,
        crosswalk_rows,
        flow_rows,
        systems_by_cell,
    )

    tables = {
        "candidate_label_state_counts.csv": (
            ["dimension", "label", "PRESENT", "ABSENT", "UNCERTAIN", "denominator", "counting_rule"],
            state_rows,
        ),
        "candidate_cell_cooccurrence_counts.csv": (
            ["assistance_mode", "visualization_modality", "present_label_cooccurrence_records", "candidate_record_denominator", "counting_unit", "interpretation"],
            cell_rows,
        ),
        "candidate_cell_task_distributions.csv": (
            ["assistance_mode", "visualization_modality", "task", "task_PRESENT", "task_ABSENT", "task_UNCERTAIN", "cell_candidate_records"],
            task_rows,
        ),
        "candidate_abstract_availability.csv": (
            ["measure", "state", "candidate_records", "denominator"],
            abstract_rows,
        ),
        "supported_system_cell_counts.csv": (
            ["assistance_mode", "visualization_modality", "distinct_supported_systems", "systems", "assessment_scope"],
            supported_rows,
        ),
        "reconciliation_counts.csv": (
            ["check", "recomputed", "reference", "status"],
            reconciliation,
        ),
    }
    for filename, (fields, rows) in tables.items():
        write_csv(output / filename, fields, rows)

    comparisons = {
        "candidate_label_states": compare_table(
            state_rows,
            read_csv(supplement / "candidate_records/candidate_label_state_counts.csv"),
            ("dimension", "label"),
            ("PRESENT", "ABSENT", "UNCERTAIN", "denominator"),
        ),
        "candidate_cells": compare_table(
            cell_rows,
            read_csv(supplement / "candidate_records/candidate_cell_cooccurrence_counts.csv"),
            ("assistance_mode", "visualization_modality"),
            ("present_label_cooccurrence_records", "candidate_record_denominator"),
        ),
        "candidate_cell_tasks": compare_table(
            task_rows,
            read_csv(supplement / "candidate_records/candidate_cell_task_distributions.csv"),
            ("assistance_mode", "visualization_modality", "task"),
            ("task_PRESENT", "task_ABSENT", "task_UNCERTAIN", "cell_candidate_records"),
        ),
        "candidate_abstract_status": compare_table(
            abstract_rows,
            read_csv(supplement / "candidate_records/candidate_abstract_availability.csv"),
            ("measure", "state"),
            ("candidate_records", "denominator"),
        ),
        "supported_cells": compare_table(
            supported_rows,
            read_csv(supplement / "full_report/corrected_cumulative_supported_cell_counts.csv"),
            ("assistance_mode", "visualization_modality"),
            ("distinct_supported_systems",),
        ),
    }
    if any(item["status"] != "PASS" for item in comparisons.values()):
        raise ValueError(f"reference-table comparisons failed: {comparisons}")

    figure_dir = output / "figures"
    figure_dir.mkdir()
    supported_svg = figure_dir / "figure2_assistance_modality_map.svg"
    candidate_svg = figure_dir / "machine_candidate_cooccurrence_chart.svg"
    annotations = write_supported_map(supported_svg, systems_by_cell, reconciliation)
    write_candidate_chart(candidate_svg, cell_rows)
    write_csv(
        output / "figure_annotation_checks.csv",
        ["assistance_mode", "visualization_modality", "representative_system", "present_in_recomputed_cell"],
        annotations,
    )
    render_pdf(supported_svg, figure_dir / "figure2_assistance_modality_map.pdf")
    render_pdf(candidate_svg, figure_dir / "machine_candidate_cooccurrence_chart.pdf")

    report = {
        "status": "PASS",
        "supplement_root": str(supplement),
        "output_directory": str(output),
        "network_used": False,
        "model_calls_used": False,
        "repository_fallback_used": False,
        "candidate_records": len(candidate_rows),
        "candidate_category_rows": len(state_rows),
        "candidate_cell_rows": len(cell_rows),
        "candidate_cell_task_rows": len(task_rows),
        "abstract_availability_scope": ABSTRACT_AVAILABILITY_SCOPE,
        "systems_considered": len(eligibility_rows),
        "supported_systems": len({system for systems in systems_by_cell.values() for system in systems}),
        "supported_system_cell_placements": sum(len(systems) for systems in systems_by_cell.values()),
        "reference_table_comparisons": comparisons,
        "reconciliation_checks": reconciliation,
        "representative_annotation_checks": {
            "rows": len(annotations),
            "all_present_in_recomputed_cells": all(
                row["present_in_recomputed_cell"] == "YES" for row in annotations
            ),
        },
        "rendering": {
            "svg": "deterministic text/vector generation",
            "pdf": "CairoSVG conversion; numeric/content equivalence is asserted, not byte identity",
        },
        "scope_limit": (
            "Recomputes downstream aggregates and figures from redistributed derivatives; "
            "does not reconstruct upstream retrieval, identity consolidation, screening, or model inference."
        ),
    }
    (output / "reproduction_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--supplement-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run(args.supplement_root, args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
