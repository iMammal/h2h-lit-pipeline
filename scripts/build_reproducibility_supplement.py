"""Build and verify the bounded H2H2 CGF reproducibility supplement.

Only preserved derived artifacts are read. The registered multi-gigabyte dataset is
hash-checked by streaming but is never deserialized.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

PACKAGE_VERSION = "h2h2-cgf-reproducibility-supplement-v3-20260930"
RELEASE_VERSION = "3.0.0"
ZENODO_CREATOR_NAME = "Chukhman, Morris"
DEVELOPMENT_TOOL_DISCLOSURE = (
    "ChatGPT and Codex were used as development tools; they are not creators."
)
EXPECTED_COUNTS = {
    "source_occurrences": 187446,
    "canonical_records": 140959,
    "valid_screenings": 140810,
    "advance": 9505,
    "defer": 83258,
    "excluded": 48047,
    "protected": 110,
    "failures": 35,
    "ambiguous": 4,
    "selected_report_rows": 384,
    "systems_considered": 381,
    "accessible_reports": 135,
    "completed_assessments": 132,
    "eligible": 80,
    "unresolved": 19,
    "excluded_contextual": 33,
    "supported_systems": 76,
    "supported_placements": 118,
}

CRITICAL_HASHES = {
    "outputs/staging/title-abstract-remaining-campaign-completion-audit-20260924-v1/combined_categorized_candidates.csv": "08ecc6167773cc6e1853c88f66b243b7e467c6eb3a68d08b13da42189e37790b",
    "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/review_dataset.json": "44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2",
    "outputs/production/star-external-retrieval-wave-001/execution/PriorSurveyIdentityAdjudication/v1/identity_confirmation_overlay.json": "c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d",
    "outputs/staging/manuscript-input-20260925/STAR-editable-source-expanded-map-20260925.zip": "ed8c302bf4bc2d29a44620ee57c1c73ba7e939b98104994707851cbf0cd95d8f",
}


@dataclass(frozen=True)
class CopySpec:
    source: str
    destination: str
    relationship: str
    source_version: str


COPY_SPECS = (
    CopySpec(
        "LICENSE",
        "LICENSE",
        "MIT license for Morris Chukhman's original pipeline code and scripts",
        "2026-09-30",
    ),
    CopySpec(
        "LICENSES/CC-BY-4.0.txt",
        "LICENSES/CC-BY-4.0.txt",
        "CC BY 4.0 legal code for original research materials within stated scope",
        "4.0",
    ),
    CopySpec(
        "LICENSE_SCOPE.md",
        "LICENSE_SCOPE.md",
        "license scope, attribution, exclusions, and separate-authorization notice",
        "2026-09-30",
    ),
    CopySpec(
        "pyproject.toml",
        "environment/pyproject.toml",
        "declared installation requirements and project metadata",
        "0.1.0",
    ),
    CopySpec(
        "README.md",
        "documentation/REPOSITORY_README.md",
        "release documentation",
        "documentation release",
    ),
    CopySpec(
        "docs/reproducibility.md",
        "documentation/reproducibility.md",
        "reproduction procedure",
        "documentation release",
    ),
    CopySpec(
        "docs/reproducibility_artifact_inventory.md",
        "documentation/reproducibility_artifact_inventory.md",
        "human-readable inventory",
        "documentation release",
    ),
    CopySpec(
        "docs/revised_star_protocol.md",
        "protocols/revised_star_protocol.md",
        "frozen scientific protocol",
        "1.0.0",
    ),
    CopySpec(
        "docs/revised_star_coding_rubric.md",
        "protocols/revised_star_coding_rubric.md",
        "frozen coding rubric",
        "1.0.0",
    ),
    CopySpec(
        "docs/title_abstract_screening_protocol_amendment_v2_0_0.md",
        "protocols/title_abstract_screening_protocol_amendment_v2_0_0.md",
        "screening amendment",
        "2.0.0",
    ),
    CopySpec(
        "docs/title_abstract_screening_fast_track_amendment_v2_1_0.md",
        "protocols/title_abstract_screening_fast_track_amendment_v2_1_0.md",
        "screening amendment",
        "2.1.0",
    ),
    CopySpec(
        "config/title_abstract_screening_precision_amendment_v2_2_0.json",
        "protocols/title_abstract_screening_precision_amendment_v2_2_0.json",
        "precision amendment",
        "2.2.0",
    ),
    CopySpec(
        "config/title_abstract_screening_return_schema_v2_0_0.json",
        "schemas/title_abstract_screening_return_schema_v2_0_0.json",
        "human return schema",
        "2.0.0",
    ),
    CopySpec(
        "config/title_abstract_remaining_campaign_v1_0_0.json",
        "screening/title_abstract_remaining_campaign_v1_0_0.json",
        "terminal screening/coding settings",
        "1.0.0",
    ),
    CopySpec(
        "prompts/title_abstract_precision_rescreen_v2_2_0.md",
        "prompts/title_abstract_precision_rescreen_v2_2_0.md",
        "executed screening prompt",
        "2.2.0",
    ),
    CopySpec(
        "prompts/title_abstract_taxonomy_coding_v1_0_0.md",
        "prompts/title_abstract_taxonomy_coding_v1_0_0.md",
        "executed coding prompt",
        "1.0.0",
    ),
    CopySpec(
        "config/star_production_query_plan_v1.json",
        "queries/star_production_query_plan_v1.json",
        "exact provider-native query plan",
        "1.0.0",
    ),
    CopySpec(
        "config/star_acm_field_execution_contract_v1.json",
        "queries/star_acm_field_execution_contract_v1.json",
        "ACM execution contract",
        "1.0.0",
    ),
    CopySpec(
        "config/star_acm_export_partition_contract_v1.json",
        "queries/star_acm_export_partition_contract_v1.json",
        "ACM export contract",
        "1.0.0",
    ),
    CopySpec(
        "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/input_inventory.json",
        "identification/global_input_inventory.json",
        "registered input bindings",
        "global-identification-merge-v1",
    ),
    CopySpec(
        "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/reconciliation.json",
        "identification/global_reconciliation.json",
        "identification counts",
        "1.0.0",
    ),
    CopySpec(
        "outputs/production/star-external-retrieval-wave-001/execution/PriorSurveyIdentityAdjudication/v1/identity_confirmation_overlay.json",
        "identification/identity_confirmation_overlay.json",
        "exact identity adjudication overlay",
        "1.0.0",
    ),
    CopySpec(
        "outputs/production/star-external-retrieval-wave-001/execution/arXivSnapshotV303/retrieval_method_amendment_v2.json",
        "identification/arxiv_snapshot_substitution.json",
        "arXiv retrieval substitution",
        "2",
    ),
    CopySpec(
        "outputs/staging/title-abstract-remaining-campaign-completion-audit-20260924-v1/combined_report.json",
        "screening/terminal_combined_report.json",
        "terminal candidate and remaining-queue counts",
        "2026-09-24",
    ),
    CopySpec(
        "outputs/staging/title-abstract-remaining-campaign-completion-audit-20260924-v1/package_manifest.json",
        "screening/terminal_package_manifest.json",
        "terminal artifact hashes",
        "2026-09-24",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/candidate_label_state_counts.csv",
        "candidate_records/candidate_label_state_counts.csv",
        "complete 9,505-record machine label-state counts",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/candidate_cell_cooccurrence_counts.csv",
        "candidate_records/candidate_cell_cooccurrence_counts.csv",
        "machine-coded Assistance × Modality co-occurrences",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/candidate_cell_task_distributions.csv",
        "candidate_records/candidate_cell_task_distributions.csv",
        "machine-coded task distributions within cells",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/candidate_abstract_availability.csv",
        "candidate_records/candidate_abstract_availability.csv",
        "aggregate of preserved candidate abstract-availability flags; omitted abstract text is not inspected",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/candidate_exact_duplicate_groups.csv",
        "candidate_records/candidate_exact_duplicate_groups.csv",
        "candidate-record duplicate audit",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/candidate_census_summary.md",
        "candidate_records/candidate_census_summary.md",
        "readable candidate-census interpretation",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/candidate-census-full-report-expansion-v1-20260925/build_census_and_freeze_queue.py",
        "historical_generators/build_candidate_census.py",
        "historical candidate-census generator; requires restricted source artifacts",
        "candidate-census-v1",
    ),
    CopySpec(
        "outputs/staging/title-abstract-remaining-campaign-v1-20260924/remaining_campaign_manifest.json",
        "screening/remaining_campaign_manifest.json",
        "remaining-corpus queue and protected-record binding",
        "1.0.0",
    ),
    CopySpec(
        "outputs/staging/title-abstract-fast-track-batch-v2-1-2-repair-gpt-5-6-luna-20260923T093018Z/combined_coverage_report.json",
        "screening/fast_track_repair_combined_coverage_report.json",
        "repaired initial/overnight screening lineage",
        "2.1.2",
    ),
    CopySpec(
        "outputs/staging/title-abstract-fast-track-overnight-completion-audit-20260923-v1/completion_audit.json",
        "screening/fast_track_overnight_completion_audit.json",
        "terminal overnight completion audit",
        "2026-09-23",
    ),
    CopySpec(
        "outputs/staging/title-abstract-precision-rescreen-completion-audit-20260924-v1/remote-results/run_report.json",
        "screening/precision_rescreen_run_report.json",
        "superseding precision outcomes",
        "2.2.0",
    ),
    CopySpec(
        "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json",
        "human_review/sampling_manifest.json",
        "frozen human-review sampling",
        "2026-09-20",
    ),
    CopySpec(
        "outputs/staging/title-abstract-screening-v2-return-morris-20260922-v2/pilot_return_validation_report.md",
        "human_review/morris_primary_return_validation.md",
        "completed review limitations and counts",
        "v2",
    ),
    CopySpec(
        "outputs/staging/target-cell-complete-pass-v1-20260926/corrected_cumulative_denominators.csv",
        "full_report/corrected_cumulative_denominators.csv",
        "full-report denominators",
        "target-cell-complete-pass-v1",
    ),
    CopySpec(
        "outputs/staging/target-cell-complete-pass-v1-20260926/corrected_cumulative_supported_cell_counts.csv",
        "full_report/corrected_cumulative_supported_cell_counts.csv",
        "supported system-cell counts",
        "target-cell-complete-pass-v1",
    ),
    CopySpec(
        "outputs/staging/target-cell-complete-pass-v1-20260926/generate_target_map.mjs",
        "historical_generators/generate_target_map.mjs",
        "historical Figure 2 generator; retained for provenance and not portable",
        "target-cell-complete-pass-v1",
    ),
    CopySpec(
        "outputs/staging/target-cell-complete-pass-v1-20260926/render_figure.py",
        "historical_generators/render_figure.py",
        "historical Figure 2 render helper; retained for provenance and not portable",
        "target-cell-complete-pass-v1",
    ),
    CopySpec(
        "outputs/staging/study-selection-flow-count-ledger-v1-20260926/flow_count_ledger.csv",
        "flow/flow_count_ledger.csv",
        "manuscript flow counts",
        "v1-20260926",
    ),
    CopySpec(
        "outputs/staging/study-selection-flow-count-ledger-v1-20260926/flow_connections.csv",
        "flow/flow_connections.csv",
        "flow relationships",
        "v1-20260926",
    ),
    CopySpec(
        "outputs/staging/study-selection-flow-count-ledger-v1-20260926/unit_crosswalk.csv",
        "flow/unit_crosswalk.csv",
        "report/system/candidate crosswalk",
        "v1-20260926",
    ),
    CopySpec(
        "outputs/staging/study-selection-flow-count-ledger-v1-20260926/reconciliation_report.md",
        "flow/reconciliation_report.md",
        "count equations and limitations",
        "v1-20260926",
    ),
    CopySpec(
        "outputs/staging/study-selection-flow-count-ledger-v1-20260926/proposed_diagram_structure.md",
        "flow/proposed_diagram_structure.md",
        "two-panel flow outline",
        "v1-20260926",
    ),
    CopySpec(
        "outputs/staging/study-selection-flow-count-ledger-v1-20260926/build_flow_ledger.py",
        "historical_generators/build_flow_ledger.py",
        "historical flow-ledger generator; requires restricted source artifacts",
        "v1-20260926",
    ),
)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_text(args: list[str], cwd: Path) -> str:
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def logical_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        return sum(1 for _ in reader)


def copy_bound(root: Path, supplement: Path, spec: CopySpec) -> dict[str, object]:
    source = root / spec.source
    if not source.is_file():
        raise FileNotFoundError(spec.source)
    destination = supplement / spec.destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    return {
        "relative_path": spec.destination,
        "bytes": destination.stat().st_size,
        "sha256": sha256_file(destination),
        "source_path": spec.source,
        "source_sha256": sha256_file(source),
        "source_version": spec.source_version,
        "relationship": spec.relationship,
        "redistribution_status": "INCLUDED",
    }


def write_candidate_derivative(root: Path, destination: Path) -> tuple[int, set[str]]:
    source = (
        root
        / "outputs/staging/title-abstract-remaining-campaign-completion-audit-20260924-v1/combined_categorized_candidates.csv"
    )
    keep = [
        "candidate_origin",
        "coding_stage_run",
        "canonical_id",
        "title",
        "doi",
        "source_url",
        "abstract_status",
        "assistance_present",
        "assistance_unknown",
        "modalities_present",
        "modalities_unknown",
        "tasks_present",
        "tasks_unknown",
        "workflow_support_review",
        "provisional_title_abstract_coding",
        "human_category_review",
        "human_final_eligibility",
        "human_notes",
    ]
    destination.parent.mkdir(parents=True, exist_ok=True)
    identifiers: set[str] = set()
    count = 0
    with (
        source.open("r", encoding="utf-8-sig", newline="") as src,
        destination.open("w", encoding="utf-8", newline="") as dst,
    ):
        reader = csv.DictReader(src)
        missing = [name for name in keep if name not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"candidate export missing columns: {missing}")
        writer = csv.DictWriter(dst, fieldnames=keep)
        writer.writeheader()
        for row in reader:
            canonical_id = row["canonical_id"]
            if canonical_id in identifiers:
                raise ValueError(f"duplicate canonical_id: {canonical_id}")
            identifiers.add(canonical_id)
            writer.writerow({name: row.get(name, "") for name in keep})
            count += 1
    return count, identifiers


def excerpt_if_permissible(value: str, limit: int = 25) -> str:
    if not value:
        return ""
    if len(value.split()) <= limit:
        return value
    return "[OMITTED: source passage exceeds the supplement excerpt limit; use the report locator]"


def release_excerpt_fields(value: str, limit: int = 25) -> dict[str, object]:
    """Apply the release-only editorial excerpt rule without changing source evidence."""
    words = len(value.split()) if value else 0
    if not value:
        treatment = "NO_SOURCE_PASSAGE"
    elif words <= limit:
        treatment = "RETAINED_AT_OR_BELOW_EDITORIAL_25_WORD_CAP"
    else:
        treatment = "OMITTED_ABOVE_EDITORIAL_25_WORD_CAP"
    return {
        "evidence_passage": excerpt_if_permissible(value, limit),
        "original_evidence_word_count": words,
        "excerpt_treatment": treatment,
    }


def normalized_eligibility_status(row: dict[str, str]) -> str:
    """Apply the validated normalized-status/aggregate-status fallback and aliases."""
    value = (row.get("normalized_status") or row.get("aggregate_status") or "").strip()
    aliases = {
        "ELIGIBLE": "ELIGIBLE",
        "UNRESOLVED": "UNRESOLVED",
        "HOLD": "UNRESOLVED",
        "EXCLUDED": "EXCLUDED_CONTEXTUAL",
        "CONTEXTUAL": "EXCLUDED_CONTEXTUAL",
        "EXCLUDED_CONTEXTUAL": "EXCLUDED_CONTEXTUAL",
        "INACCESSIBLE": "INACCESSIBLE",
        "INACCESSIBLE_FULL_REPORT": "INACCESSIBLE",
    }
    if value not in aliases:
        raise ValueError(f"unrecognized eligibility status for {row.get('system')}: {value!r}")
    return aliases[value]


def copy_csv_derivative(source: Path, destination: Path, transform=None, extra_fields=()) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with (
        source.open("r", encoding="utf-8-sig", newline="") as src,
        destination.open("w", encoding="utf-8", newline="") as dst,
    ):
        reader = csv.DictReader(src)
        fields = list(reader.fieldnames or []) + list(extra_fields)
        writer = csv.DictWriter(dst, fieldnames=fields)
        writer.writeheader()
        for row in reader:
            if transform:
                row = transform(row)
            writer.writerow(row)
            count += 1
    return count


def derive_execution_summary(root: Path, destination: Path) -> None:
    source = (
        root / "outputs/production/star-external-retrieval-wave-001/execution/execution_state.json"
    )
    state = json.loads(source.read_text(encoding="utf-8"))
    sources = {}
    for name, item in state["sources"].items():
        sources[name] = {
            key: item.get(key)
            for key in (
                "status",
                "occurrence_count",
                "completed_query_count",
                "total_query_count",
                "active_episode_number",
                "active_checkpoint_path",
                "failure_reason",
                "pause_reason",
                "network_request_count",
                "last_session_started_at_utc",
                "last_session_completed_at_utc",
            )
        }
        if item.get("checkpoint_dataset"):
            sources[name]["checkpoint_dataset"] = item["checkpoint_dataset"]
        if item.get("family_datasets"):
            sources[name]["family_datasets"] = item["family_datasets"]
    output = {
        "artifact_class": "reduced_executed_retrieval_summary",
        "source_artifact": str(source.relative_to(root)),
        "source_sha256": sha256_file(source),
        "created_at_utc": state.get("created_at_utc"),
        "updated_at_utc": state.get("updated_at_utc"),
        "external_retrieval_cutoff_date": state.get("external_retrieval_cutoff_date"),
        "status": state.get("status"),
        "identification_set_closed": state.get("identification_set_closed"),
        "sources": sources,
        "qualification": "Reduced derivative; excludes raw responses, headers, attempts, and credentials.",
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_denominators(path: Path) -> dict[str, int]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return {row["denominator"]: int(row["count"]) for row in csv.DictReader(handle)}


def validate_source_counts(root: Path, candidate_ids: set[str]) -> dict[str, object]:
    reconciliation = json.loads(
        (
            root
            / "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/reconciliation.json"
        ).read_text(encoding="utf-8")
    )
    denominators = read_denominators(
        root
        / "outputs/staging/target-cell-complete-pass-v1-20260926/corrected_cumulative_denominators.csv"
    )
    flow_rows = {}
    with (
        root / "outputs/staging/study-selection-flow-count-ledger-v1-20260926/flow_count_ledger.csv"
    ).open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            flow_rows[row["proposed_box_id"]] = int(row["count"])
    observed = {
        "source_occurrences": reconciliation["input_occurrences"],
        "canonical_records": reconciliation["canonical_counts"]["after_approved_adjudication"],
        "valid_screenings": flow_rows["A07"],
        "advance": flow_rows["A11"],
        "defer": flow_rows["A12"],
        "excluded": flow_rows["A13"],
        "protected": flow_rows["A06"],
        "failures": flow_rows["A08"],
        "ambiguous": flow_rows["A09"],
        "selected_report_rows": flow_rows["B01"],
        "systems_considered": denominators["systems_considered"],
        "accessible_reports": denominators["accessible_full_reports"],
        "completed_assessments": denominators["completed_eligibility_assessments"],
        "eligible": denominators["eligible_systems"],
        "unresolved": denominators["unresolved_systems"],
        "excluded_contextual": denominators["excluded_or_contextual_systems"],
        "supported_systems": denominators["systems_with_supported_placements"],
        "supported_placements": denominators["supported_system_cell_placements"],
    }
    errors = {
        key: {"expected": EXPECTED_COUNTS[key], "observed": value}
        for key, value in observed.items()
        if EXPECTED_COUNTS[key] != value
    }
    if len(candidate_ids) != EXPECTED_COUNTS["advance"]:
        errors["candidate_unique_ids"] = {
            "expected": EXPECTED_COUNTS["advance"],
            "observed": len(candidate_ids),
        }
    if (
        sum(
            (
                observed["valid_screenings"],
                observed["protected"],
                observed["failures"],
                observed["ambiguous"],
            )
        )
        != observed["canonical_records"]
    ):
        errors["screening_partition"] = "does not close"
    if (
        sum((observed["advance"], observed["defer"], observed["excluded"]))
        != observed["valid_screenings"]
    ):
        errors["outcome_partition"] = "does not close"
    if (
        sum((observed["eligible"], observed["unresolved"], observed["excluded_contextual"]))
        != observed["completed_assessments"]
    ):
        errors["completed_partition"] = "does not close"
    if errors:
        raise ValueError(f"count reconciliation failed: {errors}")
    return {"status": "PASS", "observed": observed, "equations_closed": True}


def manifest_entry(
    path: Path,
    supplement: Path,
    *,
    source_path: str,
    source_sha256: str,
    source_version: str,
    relationship: str,
    status: str = "INCLUDED",
) -> dict[str, object]:
    return {
        "relative_path": str(path.relative_to(supplement)),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "source_path": source_path,
        "source_sha256": source_sha256,
        "source_version": source_version,
        "relationship": relationship,
        "redistribution_status": status,
    }


def write_inventory_csv(destination: Path, rows: Iterable[dict[str, object]]) -> None:
    fields = [
        "relative_path",
        "bytes",
        "sha256",
        "source_path",
        "source_sha256",
        "source_version",
        "relationship",
        "redistribution_status",
    ]
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_static_materials(
    supplement: Path, branch: str, research_commit: str, release_commit: str
) -> dict[str, str]:
    files = {
        "README.md": """# H2H2 CGF reproducibility supplement

This package supports bounded offline recomputation of H2H2 candidate-label aggregates,
supported system-cell counts, eligibility/flow equations, and two vector figures. It
also supports audit of preserved search, screening, identity, and full-report decisions
and provenance. It does not reconstruct upstream record-level retrieval, identity
consolidation, or screening from aggregate summaries.

## Clean-extraction reproduction

From the extracted `supplement` directory, with Python 3.11+ and CairoSVG 2.9.1:

```bash
python reproduce.py --supplement-root . --output-dir ../reproduced
```

The command reads only the supplied supplement root and writes only to the separate
output directory. It recomputes 14 category distributions, all 20 candidate cells, all
100 cell-task distributions, supported system-cell counts, reconciliation equations,
and both SVG/PDF figures. Numeric outputs are checked against packaged reference tables.
PDF content is reproducible, but byte identity can vary with renderer versions.

The abstract-availability comparison reproduces preserved availability flags from the
reduced candidate export. Abstract text is omitted and is not inspected; historical
output field names are retained for compatibility.

The candidate landscape is machine-coded and is not a verified literature-prevalence
estimate. Original automated full-report assessment provenance remains in every row.
Morris Chukhman reports having reviewed the AI-generated material; see
`AUTHOR_REVIEW_STATEMENT.md`. This package-level statement is not row-level approval,
independent review, or a blinded validation study. Technical reproducibility does not
establish independent classification accuracy.

Morris Chukhman is the sole human creator of the original software and supplement.
ChatGPT and Codex were used as development tools and are not creators.

## Licensing

Morris Chukhman's original bundled code and scripts are licensed under MIT. His original
documentation, protocols, annotations, diagrams, and research derivatives are licensed
under CC BY 4.0. See `LICENSE`, `LICENSES/CC-BY-4.0.txt`, and `LICENSE_SCOPE.md`.
These licenses do not cover coauthored or collaborator-owned work, third-party excerpts,
screenshots, metadata, reports, dependencies, or material carrying another notice.
""",
        "DATA_DICTIONARY.md": """# Data dictionary

- **source occurrence**: one record returned/imported through one source route before consolidation.
- **canonical record**: identity group after DOI-first, title-fallback consolidation and approved overlay adjudication.
- **candidate record**: one terminal ADVANCE canonical identity with provisional machine taxonomy coding.
- **report row**: one selected publication/report identity; related versions remain separate rows.
- **system**: persisted system group used for full-report eligibility assessment.
- **system-cell placement**: one supported Assistance × Modality combination for one eligible system; multilabel and non-additive.

`candidate_audit_export.csv` omits abstracts, evidence text, and coding rationale.
The abstract-availability table and reproduction check use only the preserved
`abstract_status` flags in that reduced export. They do not inspect omitted abstract
text. Historical measure and field names are retained for compatibility.
Both redistributed placement tables retain mechanisms, identifiers, hashes, and
locators but replace source passages longer than 25 words with explicit omission
markers. The 25-word cap is an editorial packaging rule, not a legal threshold or a
claim that shorter excerpts are automatically redistributable. Full reports are not
included.

`normalized_status` in `eligibility_assessments.csv` is populated for every row by the
validated rule: use the original normalized value when present, otherwise normalize the
aggregate status. `original_normalized_status` preserves the source field.
""",
        "AVAILABILITY_AND_REDISTRIBUTION.md": """# Availability and redistribution statement

Included materials are author-generated protocols, software inputs, reduced derivatives,
aggregate ledgers, and figure sources. Excluded materials include copyrighted full-text
reports; raw provider responses and internal BibTeX exports with unresolved redistribution
terms; the registered corpus; manuscript source; review workbooks; credentials, signed
URLs, authentication headers, private correspondence, caches, and environments.

Excluded artifacts remain hash-bound. Morris Chukhman's original bundled code and
scripts are MIT-licensed. His original documentation, protocols, annotations, diagrams,
and research derivatives are CC BY 4.0-licensed. These grants apply only to material he
owns and may license. They do not relicense coauthored or collaborator-owned material,
third-party excerpts, screenshots, bibliographic/provider metadata, full reports,
dependencies, or material with a separate notice. Preserve existing notices and obtain
separate authorization for separately owned contributions.
""",
        "HISTORICAL_GENERATORS.md": """# Historical generators

Files under `historical_generators/` are preserved for provenance. They are not the
advertised clean-extraction workflow: their original paths bind to restricted or
undistributed artifacts, and some assume the repository's ignored `outputs/staging/`
tree. Use `reproduce.py` for portable offline reproduction from this package.
""",
        "requirements-reproduction.txt": """CairoSVG==2.9.1
""",
        "AUTHOR_REVIEW_STATEMENT.md": """# Author-reported review statement

Morris Chukhman reports that he reviewed the AI-generated material used in the
full-report synthesis. The release derivative preserves the original automated
assessment authority recorded for each row. This package-level statement does not
invent row-level approval dates, a second or independent reviewer, or a blinded
validation study. Scientifically unresolved boundaries remain unresolved.
""",
        "RELEASE_NOTES_DRAFT.md": f"""# Release notes

H2H2 CGF reproducibility supplement 3.0.0 (2026-09-30).

Research implementation checkpoint: `{research_commit}`. Documentation/release commit:
`{release_commit}` on `{branch}`.

This replacement preserves the scientific decisions and v2 reproduction workflow while
adding explicit licensing. Morris Chukhman's original bundled code and scripts use MIT;
his original documentation, protocols, annotations, diagrams, and research derivatives
use CC BY 4.0. The included scope statement excludes coauthored, collaborator-owned, and
third-party material from those grants. Original automated authority remains row-level,
while Morris Chukhman's review is recorded separately as an author-reported statement.

Morris Chukhman is the sole human creator of the original software and supplement.
ChatGPT and Codex were used as development tools and are not creators. No ORCID,
affiliation, DOI, or acceptance status is supplied or inferred. Existing third-party and
separately owned material exclusions remain unchanged.
""",
        "MANUSCRIPT_AVAILABILITY_PARAGRAPH.md": """A versioned reproducibility supplement accompanies this work. It contains exact search
and screening protocols, provider-native query definitions, reduced record-level coding
exports, corrected system/report and assistance–modality evidence matrices, flow-count
ledgers, and a portable offline script that recomputes the reported candidate-label
aggregates, supported system-cell counts, reconciliation equations, and vector figures
from included inputs. Copyrighted full texts, raw provider responses whose
redistribution terms are unresolved, institutional-access material, credentials, and
the registered multi-gigabyte corpus are excluded; their local provenance is retained
through identifiers, locators, and cryptographic hashes. The package audits preserved
decisions but does not rerun mutable retrieval or model services, and technical
reproducibility does not establish independent classification accuracy. Morris
Chukhman's original bundled software is available under MIT and his original research
materials under CC BY 4.0; separately owned and third-party material is excluded from
those grants as specified in the package's license-scope statement.
""",
        "MANUAL_RELEASE_HANDOFF.md": f"""# Manual release handoff

Verified branch: `{branch}`

Research checkpoint: `{research_commit}`

Documentation/release commit: `{release_commit}`

Inspect first, then run from the repository root:

```bash
git status --short --branch
git show --stat --oneline {release_commit}
git push origin {branch}
git tag -a h2h2-cgf-reproducibility-v3.0.0 {release_commit} -m 'H2H2 CGF reproducibility supplement v3.0.0'
git push origin h2h2-cgf-reproducibility-v3.0.0
```

Create the GitHub release from that tag and upload the ZIP plus external `SHA256SUMS`.
Upload the same two files to Zenodo using `ZENODO_METADATA_DRAFT.json` as the reviewed
metadata source. Do not claim availability until publication is complete.
""",
        "REVIEWER_REPRODUCIBILITY_COVERAGE.md": """# Reviewer reproducibility coverage

## Supported by this package

- versioned methodology and symmetric high-precision eligibility policy;
- auditable two-panel study-selection counts with explicit unit changes;
- prior-survey, IEEE Xplore, ACM DL, arXiv-substitution, and other source provenance;
- exact query strings, fields, request forms, dates, partitions, and provider limits;
- screening/coding prompts, schemas, model settings, amendments, and continuation lineage;
- criterion-level rubric and exclusion-reason taxonomy;
- completed author-review evidence with exposure and independence limitations;
- explicit separation of machine proposals, author judgments, and synthesis interpretation;
- corrected full-report/system denominators and supported map placements;
- executable aggregate and figure reproduction from publicly included inputs.

## Still unresolved

- public publication, DOI, and acceptance status;
- ORCID and affiliation are not supplied and must not be inferred;
- separate authorization for any coauthored or collaborator-owned material intended for
  distribution beyond the currently scoped Morris Chukhman contributions;
- an independent blinded estimate of classification accuracy;
- identification closure (the registered state explicitly remains open);
- redistribution of the registered corpus, raw provider responses, internal BibTeX, and
  copyrighted full reports;
- several terminal manifest-named outcome CSVs absent from the local completion package;
- remaining genuinely unresolved scientific boundaries; package-level author review is
  recorded separately without rewriting automated row-level authority.

The flow ledger is ready for figure authoring but is not a claim of PRISMA compliance.
""",
    }
    for name, content in files.items():
        (supplement / name).write_text(content, encoding="utf-8")
    metadata = {
        "title": "From Hairballs to Hypotheses: H2H2 CGF reproducibility supplement",
        "upload_type": "dataset",
        "description": "Protocols, reduced audit exports, corrected synthesis matrices, flow ledgers, and figure-generation inputs for the H2H2 CGF submission.",
        "creators": [
            {
                "name": ZENODO_CREATOR_NAME,
            }
        ],
        "publication_date": "2026-09-30",
        "version": RELEASE_VERSION,
        "license": "cc-by-4.0",
        "development_tools_disclosure": DEVELOPMENT_TOOL_DISCLOSURE,
        "license_scope": {
            "record_original_research_materials": "CC-BY-4.0",
            "bundled_original_pipeline_code_and_scripts": "MIT",
            "copyright_holder_for_licensed_contributions": "Morris Chukhman",
            "exclusions": (
                "Coauthored or collaborator-owned material and third-party excerpts, "
                "screenshots, metadata, reports, dependencies, and separately noticed material"
            ),
            "details": "LICENSE_SCOPE.md",
        },
        "metadata_not_supplied": ["ORCID", "affiliation", "DOI", "acceptance status"],
    }
    (supplement / "ZENODO_METADATA_DRAFT.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    files["ZENODO_METADATA_DRAFT.json"] = ""
    return {name: "generated release material" for name in files}


def add_generated(
    entries: list[dict[str, object]], path: Path, supplement: Path, relationship: str
) -> None:
    entries.append(
        manifest_entry(
            path,
            supplement,
            source_path="derived by scripts/build_reproducibility_supplement.py",
            source_sha256="derived",
            source_version=PACKAGE_VERSION,
            relationship=relationship,
        )
    )


def deterministic_zip(source_dir: Path, archive_path: Path) -> None:
    with zipfile.ZipFile(
        archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for path in sorted(p for p in source_dir.rglob("*") if p.is_file()):
            relative = Path(source_dir.name) / path.relative_to(source_dir)
            info = zipfile.ZipInfo(str(relative), date_time=(2026, 9, 30, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(
                info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9
            )


def verify_manifest(supplement: Path) -> dict[str, object]:
    manifest = json.loads((supplement / "package_manifest.json").read_text(encoding="utf-8"))
    errors = []
    listed = {entry["relative_path"] for entry in manifest["files"]}
    for entry in manifest["files"]:
        path = supplement / entry["relative_path"]
        if not path.is_file():
            errors.append(f"missing:{entry['relative_path']}")
            continue
        if path.stat().st_size != entry["bytes"]:
            errors.append(f"size:{entry['relative_path']}")
        if sha256_file(path) != entry["sha256"]:
            errors.append(f"hash:{entry['relative_path']}")
    actual = {
        str(path.relative_to(supplement))
        for path in supplement.rglob("*")
        if path.is_file() and path.name != "package_manifest.json"
    }
    if actual != listed:
        errors.append(
            f"manifest_coverage:missing={sorted(actual - listed)}:stale={sorted(listed - actual)}"
        )
    candidate_rows = logical_csv_rows(supplement / "candidate_records/candidate_audit_export.csv")
    placement_rows = logical_csv_rows(supplement / "full_report/placement_evidence_permissible.csv")
    if candidate_rows != EXPECTED_COUNTS["advance"]:
        errors.append("candidate_row_count")
    if placement_rows != EXPECTED_COUNTS["supported_placements"]:
        errors.append("placement_row_count")
    with (
        supplement / "candidate_records/candidate_audit_export.csv"
    ).open("r", encoding="utf-8-sig", newline="") as handle:
        candidate_ids = [row["canonical_id"] for row in csv.DictReader(handle)]
    if len(candidate_ids) != len(set(candidate_ids)):
        errors.append("candidate_id_uniqueness")
    excerpt_rows = 0
    omitted_rows = 0
    for relative in (
        "full_report/placement_evidence_permissible.csv",
        "full_report/corrected_cumulative_placement_matrix.csv",
    ):
        with (supplement / relative).open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                excerpt_rows += 1
                passage = row.get("evidence_passage", "")
                if len(passage.split()) > 25:
                    errors.append(f"excerpt_over_cap:{relative}:{row.get('placement_id')}")
                if row.get("excerpt_treatment") == "OMITTED_ABOVE_EDITORIAL_25_WORD_CAP":
                    omitted_rows += 1
    blank_normalized = 0
    with (
        supplement / "full_report/eligibility_assessments.csv"
    ).open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if not row.get("normalized_status", "").strip():
                blank_normalized += 1
    if blank_normalized:
        errors.append(f"blank_normalized_status:{blank_normalized}")
    license_scope_status = "NOT_APPLICABLE"
    release_metadata_status = "NOT_APPLICABLE"
    if manifest.get("package_id") == PACKAGE_VERSION:
        license_scope_status = "PASS"
        release_metadata_status = "PASS"
        required_license_files = (
            "LICENSE",
            "LICENSES/CC-BY-4.0.txt",
            "LICENSE_SCOPE.md",
        )
        for relative in required_license_files:
            if not (supplement / relative).is_file():
                errors.append(f"missing_license_file:{relative}")
                license_scope_status = "FAIL"
        metadata = json.loads(
            (supplement / "ZENODO_METADATA_DRAFT.json").read_text(encoding="utf-8")
        )
        if metadata.get("license") != "cc-by-4.0":
            errors.append("zenodo_license")
            license_scope_status = "FAIL"
        if metadata.get("version") != RELEASE_VERSION:
            errors.append("zenodo_release_version")
            release_metadata_status = "FAIL"
        if metadata.get("creators") != [{"name": ZENODO_CREATOR_NAME}]:
            errors.append("zenodo_creators")
            release_metadata_status = "FAIL"
        if metadata.get("development_tools_disclosure") != DEVELOPMENT_TOOL_DISCLOSURE:
            errors.append("zenodo_development_tools_disclosure")
            release_metadata_status = "FAIL"
        scope = (supplement / "LICENSE_SCOPE.md").read_text(encoding="utf-8")
        for required_text in ("MIT License", "CC BY 4.0", "Material not relicensed"):
            if required_text not in scope:
                errors.append(f"license_scope_text:{required_text}")
                license_scope_status = "FAIL"
    return {
        "status": "PASS" if not errors else "FAIL",
        "manifest_entries": len(manifest["files"]),
        "manifest_coverage_files": len(actual),
        "candidate_rows": candidate_rows,
        "candidate_unique_ids": len(set(candidate_ids)),
        "placement_rows": placement_rows,
        "excerpt_rows_checked": excerpt_rows,
        "omitted_excerpt_rows": omitted_rows,
        "blank_normalized_statuses": blank_normalized,
        "license_scope_status": license_scope_status,
        "release_metadata_status": release_metadata_status,
        "errors": errors,
    }


def build(root: Path, output_root: Path, research_commit: str) -> tuple[Path, Path, Path]:
    if output_root.exists():
        raise FileExistsError(f"refusing to overwrite existing output: {output_root}")
    for relative, expected in CRITICAL_HASHES.items():
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(relative)
        observed = sha256_file(path)
        if observed != expected:
            raise ValueError(f"critical hash mismatch for {relative}: {observed}")

    output_root.mkdir(parents=True)
    supplement = output_root / "supplement"
    supplement.mkdir()
    release_commit = run_text(["git", "rev-parse", "HEAD"], root)
    branch = run_text(["git", "branch", "--show-current"], root)
    entries = [copy_bound(root, supplement, spec) for spec in COPY_SPECS]

    portable_source = root / "scripts/reproduce_reproducibility_supplement.py"
    portable_destination = supplement / "reproduce.py"
    shutil.copyfile(portable_source, portable_destination)
    entries.append(
        manifest_entry(
            portable_destination,
            supplement,
            source_path=str(portable_source.relative_to(root)),
            source_sha256=sha256_file(portable_source),
            source_version=PACKAGE_VERSION,
            relationship="portable clean-extraction aggregate and figure reproducer",
        )
    )

    candidate_path = supplement / "candidate_records/candidate_audit_export.csv"
    candidate_count, candidate_ids = write_candidate_derivative(root, candidate_path)
    add_generated(
        entries, candidate_path, supplement, "abstract-free terminal candidate derivative"
    )

    source_base = root / "outputs/staging/target-cell-complete-pass-v1-20260926"
    placement_path = supplement / "full_report/placement_evidence_permissible.csv"

    def placement_transform(row):
        row.update(release_excerpt_fields(row.get("evidence_passage", "")))
        row["report_distributed"] = "NO"
        return row

    placement_count = copy_csv_derivative(
        source_base / "corrected_cumulative_supported_evidence_matrix.csv",
        placement_path,
        placement_transform,
        ("original_evidence_word_count", "excerpt_treatment", "report_distributed"),
    )
    add_generated(
        entries,
        placement_path,
        supplement,
        "supported placement derivative with editorially bounded excerpts",
    )

    cumulative_placement_path = supplement / "full_report/corrected_cumulative_placement_matrix.csv"
    cumulative_placement_count = copy_csv_derivative(
        source_base / "corrected_cumulative_placement_matrix.csv",
        cumulative_placement_path,
        placement_transform,
        ("original_evidence_word_count", "excerpt_treatment", "report_distributed"),
    )
    add_generated(
        entries,
        cumulative_placement_path,
        supplement,
        "complete placement-decision derivative with editorially bounded excerpts",
    )

    crosswalk_path = supplement / "full_report/system_report_crosswalk.csv"

    def crosswalk_transform(row):
        row["report_distributed"] = "NO"
        return row

    crosswalk_count = copy_csv_derivative(
        source_base / "corrected_cumulative_system_report_crosswalk.csv",
        crosswalk_path,
        crosswalk_transform,
        ("report_distributed",),
    )
    add_generated(entries, crosswalk_path, supplement, "report/system identity crosswalk")

    eligibility_path = supplement / "full_report/eligibility_assessments.csv"

    def eligibility_transform(row):
        row["original_normalized_status"] = row.get("normalized_status", "")
        row["normalized_status"] = normalized_eligibility_status(row)
        row["normalization_rule"] = (
            "original normalized_status when present; otherwise aggregate_status; "
            "legacy aliases mapped to four validated outcome classes"
        )
        return row

    eligibility_count = copy_csv_derivative(
        source_base / "corrected_cumulative_eligibility_assessments.csv",
        eligibility_path,
        eligibility_transform,
        ("original_normalized_status", "normalization_rule"),
    )
    add_generated(entries, eligibility_path, supplement, "system eligibility matrix")

    execution_path = supplement / "identification/executed_retrieval_summary.json"
    derive_execution_summary(root, execution_path)
    add_generated(entries, execution_path, supplement, "reduced executed retrieval state")

    for seed, version in (
        ("JFR25", "2026-09-20-supported-members-v1"),
        ("EBK25", "2026-09-20-qualified-ledger-v1"),
        ("FP19", "2026-09-20-reference-list-candidates-v2"),
    ):
        source = (
            root
            / f"outputs/production/star-external-retrieval-wave-001/execution/PriorSurveySeed/{seed}/{version}/authorization.json"
        )
        destination = supplement / f"identification/prior_survey_{seed.lower()}_authorization.json"
        shutil.copyfile(source, destination)
        entries.append(
            manifest_entry(
                destination,
                supplement,
                source_path=str(source.relative_to(root)),
                source_sha256=sha256_file(source),
                source_version=version,
                relationship="executed prior-survey import authorization",
            )
        )

    count_validation = validate_source_counts(root, candidate_ids)
    count_validation.update(
        {
            "candidate_rows": candidate_count,
            "placement_rows": placement_count,
            "cumulative_placement_rows": cumulative_placement_count,
            "crosswalk_rows": crosswalk_count,
            "eligibility_rows": eligibility_count,
        }
    )

    generated = write_static_materials(supplement, branch, research_commit, release_commit)
    for name, relationship in generated.items():
        add_generated(entries, supplement / name, supplement, relationship)

    # The portable reproducer requires only a supplement-root marker while staging.
    # The final complete manifest replaces this temporary file below.
    (supplement / "package_manifest.json").write_text("{}\n", encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="h2h2-repro-figures-") as tmp:
        reproduction_output = Path(tmp) / "reproduced"
        subprocess.run(
            [
                sys.executable,
                str(portable_destination),
                "--supplement-root",
                str(supplement),
                "--output-dir",
                str(reproduction_output),
            ],
            cwd=supplement,
            check=True,
            capture_output=True,
            text=True,
        )
        for name in (
            "figure2_assistance_modality_map.svg",
            "figure2_assistance_modality_map.pdf",
            "machine_candidate_cooccurrence_chart.svg",
            "machine_candidate_cooccurrence_chart.pdf",
        ):
            destination = supplement / "figures" / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(reproduction_output / "figures" / name, destination)
            add_generated(entries, destination, supplement, "portable reproduced vector figure")
        annotation_checks = supplement / "figures/figure_annotation_checks.csv"
        shutil.copyfile(reproduction_output / "figure_annotation_checks.csv", annotation_checks)
        add_generated(
            entries,
            annotation_checks,
            supplement,
            "representative annotations checked against recomputed supported cells",
        )

    environment = {
        "artifact_class": "release_preparation_environment",
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "node": run_text(["node", "--version"], root),
        "npm": run_text(["npm", "--version"], root),
        "git": run_text(["git", "--version"], root),
        "declared_requirements": "pyproject.toml",
        "qualification": "Current validation environment, not every historical run environment.",
    }
    environment_path = supplement / "environment_record.json"
    environment_path.write_text(json.dumps(environment, indent=2) + "\n", encoding="utf-8")
    add_generated(entries, environment_path, supplement, "validation environment")

    inventory_path = supplement / "artifact_inventory.csv"
    write_inventory_csv(
        inventory_path, sorted(entries, key=lambda item: str(item["relative_path"]))
    )
    add_generated(entries, inventory_path, supplement, "machine-readable package inventory")

    validation_path = supplement / "validation_report.json"
    validation = {
        "status": "PASS",
        "research_implementation_commit": research_commit,
        "documentation_release_commit": release_commit,
        "branch": branch,
        "count_reconciliation": count_validation,
        "critical_input_hashes": CRITICAL_HASHES,
        "registered_corpus_deserialized": False,
        "network_used": False,
        "model_calls_used": False,
    }
    validation_path.write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    add_generated(entries, validation_path, supplement, "focused build validation")

    manifest = {
        "package_id": PACKAGE_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "research_implementation_commit": research_commit,
        "documentation_release_commit": release_commit,
        "branch": branch,
        "manifest_scope": "All payload files except package_manifest.json itself; avoids a self-referential checksum.",
        "files": sorted(entries, key=lambda item: str(item["relative_path"])),
        "external_restricted": [
            {
                "path": path,
                "sha256": digest,
                "reason": "not redistributed; see availability statement",
            }
            for path, digest in CRITICAL_HASHES.items()
            if path.endswith(("review_dataset.json", ".zip"))
        ],
    }
    manifest_path = supplement / "package_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    verification = verify_manifest(supplement)
    if verification["status"] != "PASS":
        raise ValueError(f"package verification failed: {verification}")

    archive = output_root / f"{PACKAGE_VERSION}.zip"
    deterministic_zip(supplement, archive)
    checksums = output_root / "SHA256SUMS"
    checksums.write_text(f"{sha256_file(archive)}  {archive.name}\n", encoding="utf-8")

    with tempfile.TemporaryDirectory(prefix="h2h2-repro-verify-") as tmp:
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(tmp)
        extracted_supplement = Path(tmp) / supplement.name
        clean_verification = verify_manifest(extracted_supplement)
        if clean_verification["status"] != "PASS":
            raise ValueError(f"clean extraction verification failed: {clean_verification}")
        clean_reproduction_output = Path(tmp) / "reproduced"
        completed = subprocess.run(
            [
                sys.executable,
                str(extracted_supplement / "reproduce.py"),
                "--supplement-root",
                str(extracted_supplement),
                "--output-dir",
                str(clean_reproduction_output),
            ],
            cwd=tmp,
            check=True,
            capture_output=True,
            text=True,
        )
        clean_reproduction = json.loads(completed.stdout)
        if clean_reproduction["status"] != "PASS":
            raise ValueError(f"clean extraction reproduction failed: {clean_reproduction}")

    outer_validation = {
        "status": "PASS",
        "archive": archive.name,
        "archive_bytes": archive.stat().st_size,
        "archive_sha256": sha256_file(archive),
        "clean_extraction": clean_verification,
        "clean_extraction_reproduction": clean_reproduction,
        "supplement_manifest_sha256": sha256_file(manifest_path),
    }
    (output_root / "release_validation_report.json").write_text(
        json.dumps(outer_validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return supplement, archive, checksums


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--research-commit")
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args()
    if args.verify is None and (args.output_root is None or args.research_commit is None):
        parser.error("build mode requires --output-root and --research-commit")
    return args


def main() -> int:
    args = parse_args()
    if args.verify is not None:
        result = verify_manifest(args.verify.resolve())
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "PASS" else 1
    root = args.repository_root.resolve()
    output_root = args.output_root if args.output_root.is_absolute() else root / args.output_root
    supplement, archive, checksums = build(root, output_root, args.research_commit)
    print(
        json.dumps(
            {
                "status": "PASS",
                "supplement": str(supplement),
                "archive": str(archive),
                "archive_sha256": sha256_file(archive),
                "checksums": str(checksums),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
