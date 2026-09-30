import csv
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts" / "build_reproducibility_supplement.py"
SPEC = importlib.util.spec_from_file_location("build_reproducibility_supplement", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

REPRODUCE_SCRIPT = (
    Path(__file__).parents[1] / "scripts" / "reproduce_reproducibility_supplement.py"
)
REPRODUCE_SPEC = importlib.util.spec_from_file_location(
    "reproduce_reproducibility_supplement", REPRODUCE_SCRIPT
)
REPRODUCE = importlib.util.module_from_spec(REPRODUCE_SPEC)
assert REPRODUCE_SPEC.loader is not None
sys.modules[REPRODUCE_SPEC.name] = REPRODUCE
REPRODUCE_SPEC.loader.exec_module(REPRODUCE)


def test_excerpt_limit_preserves_short_and_omits_long() -> None:
    short = "one two three"
    long = " ".join(f"w{i}" for i in range(26))
    assert MODULE.excerpt_if_permissible(short) == short
    assert MODULE.excerpt_if_permissible(long).startswith("[OMITTED:")
    fields = MODULE.release_excerpt_fields(long)
    assert fields["original_evidence_word_count"] == 26
    assert fields["excerpt_treatment"] == "OMITTED_ABOVE_EDITORIAL_25_WORD_CAP"


def test_eligibility_normalization_preserves_validated_aliases() -> None:
    assert MODULE.normalized_eligibility_status(
        {"system": "a", "normalized_status": "", "aggregate_status": "ELIGIBLE"}
    ) == "ELIGIBLE"
    assert MODULE.normalized_eligibility_status(
        {"system": "b", "normalized_status": "UNRESOLVED", "aggregate_status": "HOLD"}
    ) == "UNRESOLVED"
    assert MODULE.normalized_eligibility_status(
        {"system": "c", "normalized_status": "", "aggregate_status": "HOLD"}
    ) == "UNRESOLVED"
    assert MODULE.normalized_eligibility_status(
        {"system": "d", "normalized_status": "", "aggregate_status": "CONTEXTUAL"}
    ) == "EXCLUDED_CONTEXTUAL"


def test_release_license_scope_is_explicit() -> None:
    root = Path(__file__).parents[1]
    assert MODULE.PACKAGE_VERSION == "h2h2-cgf-reproducibility-supplement-v3-20260930"
    assert "MIT License" in (root / "LICENSE").read_text(encoding="utf-8")
    scope = (root / "LICENSE_SCOPE.md").read_text(encoding="utf-8")
    assert "CC BY 4.0" in scope
    assert "Material not relicensed" in scope
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    assert 'license = { file = "LICENSE" }' in pyproject
    assert 'authors = [{ name = "Morris Chukhman" }]' in pyproject


def test_final_release_creator_and_tool_metadata(tmp_path: Path) -> None:
    MODULE.write_static_materials(tmp_path, "main", "research", "release")
    metadata = json.loads(
        (tmp_path / "ZENODO_METADATA_DRAFT.json").read_text(encoding="utf-8")
    )
    assert metadata["version"] == "3.0.0"
    assert metadata["creators"] == [{"name": "Chukhman, Morris"}]
    assert "ChatGPT and Codex" in metadata["development_tools_disclosure"]
    assert "creator" not in " ".join(metadata["metadata_not_supplied"]).lower()
    release_notes = (tmp_path / "RELEASE_NOTES_DRAFT.md").read_text(encoding="utf-8")
    assert "sole human creator" in release_notes
    assert "additional creators" not in release_notes


def test_abstract_availability_scope_is_explicit() -> None:
    assert "preserved abstract-availability flags" in REPRODUCE.ABSTRACT_AVAILABILITY_SCOPE
    assert "abstract text is omitted and is not inspected" in REPRODUCE.ABSTRACT_AVAILABILITY_SCOPE


def test_candidate_recomputation_normalizes_desktop_and_uses_exhaustive_omission() -> None:
    row = {
        "canonical_id": "canonical:1",
        "abstract_status": "PRESENT",
        "assistance_present": "Algorithmic",
        "assistance_unknown": "Adaptive",
        "modalities_present": "Desktop 2D",
        "modalities_unknown": "",
        "tasks_present": "Comparison and Differentiation",
        "tasks_unknown": "",
    }
    states, cells, tasks, abstracts = REPRODUCE.candidate_tables([row])
    state = {(item["dimension"], item["label"]): item for item in states}
    assert state[("assistance", "Algorithmic")]["PRESENT"] == 1
    assert state[("assistance", "Adaptive")]["UNCERTAIN"] == 1
    assert state[("assistance", "Conversational")]["ABSENT"] == 1
    cell = {
        (item["assistance_mode"], item["visualization_modality"]): item for item in cells
    }
    assert cell[("Algorithmic", "Desktop/Planar")][
        "present_label_cooccurrence_records"
    ] == 1
    assert len(tasks) == 100
    assert abstracts[0]["candidate_records"] == 1


def test_verify_manifest_and_logical_csv_rows(tmp_path: Path) -> None:
    candidate_dir = tmp_path / "candidate_records"
    report_dir = tmp_path / "full_report"
    candidate_dir.mkdir()
    report_dir.mkdir()
    candidate = candidate_dir / "candidate_audit_export.csv"
    placement = report_dir / "placement_evidence_permissible.csv"
    cumulative = report_dir / "corrected_cumulative_placement_matrix.csv"
    eligibility = report_dir / "eligibility_assessments.csv"
    with candidate.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["canonical_id", "title"])
        for index in range(MODULE.EXPECTED_COUNTS["advance"]):
            writer.writerow(
                [f"canonical:{index:024x}", "multiline\ntitle" if index == 0 else "title"]
            )
    with placement.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["placement_id", "evidence_passage", "excerpt_treatment"])
        for index in range(MODULE.EXPECTED_COUNTS["supported_placements"]):
            writer.writerow([f"P{index:03d}", "short passage", "RETAINED_AT_OR_BELOW_EDITORIAL_25_WORD_CAP"])
    with cumulative.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["placement_id", "evidence_passage", "excerpt_treatment"])
        for index in range(125):
            writer.writerow([f"C{index:03d}", "[OMITTED: source passage exceeds the supplement excerpt limit; use the report locator]", "OMITTED_ABOVE_EDITORIAL_25_WORD_CAP"])
    with eligibility.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["system", "normalized_status"])
        for index in range(MODULE.EXPECTED_COUNTS["systems_considered"]):
            writer.writerow([f"system-{index:03d}", "INACCESSIBLE"])

    files = []
    for path in (candidate, placement, cumulative, eligibility):
        files.append(
            {
                "relative_path": str(path.relative_to(tmp_path)),
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    (tmp_path / "package_manifest.json").write_text(json.dumps({"files": files}), encoding="utf-8")
    result = MODULE.verify_manifest(tmp_path)
    assert result["status"] == "PASS"
    assert result["candidate_rows"] == 9505
    assert result["candidate_unique_ids"] == 9505
    assert result["placement_rows"] == 118
    assert result["blank_normalized_statuses"] == 0
