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


def test_excerpt_limit_preserves_short_and_omits_long() -> None:
    short = "one two three"
    long = " ".join(f"w{i}" for i in range(26))
    assert MODULE.excerpt_if_permissible(short) == short
    assert MODULE.excerpt_if_permissible(long).startswith("[OMITTED:")


def test_verify_manifest_and_logical_csv_rows(tmp_path: Path) -> None:
    candidate_dir = tmp_path / "candidate_records"
    report_dir = tmp_path / "full_report"
    candidate_dir.mkdir()
    report_dir.mkdir()
    candidate = candidate_dir / "candidate_audit_export.csv"
    placement = report_dir / "placement_evidence_permissible.csv"
    with candidate.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["canonical_id", "title"])
        for index in range(MODULE.EXPECTED_COUNTS["advance"]):
            writer.writerow(
                [f"canonical:{index:024x}", "multiline\ntitle" if index == 0 else "title"]
            )
    with placement.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["placement_id"])
        for index in range(MODULE.EXPECTED_COUNTS["supported_placements"]):
            writer.writerow([f"P{index:03d}"])

    files = []
    for path in (candidate, placement):
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
    assert result["placement_rows"] == 118
