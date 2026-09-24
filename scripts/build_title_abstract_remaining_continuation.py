"""Freeze a create-only continuation from a terminal remaining-campaign run."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} is not an object")
            yield value


def write_create_only(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    write_create_only(
        path,
        b"".join(
            (json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n").encode("utf-8")
            for row in rows
        ),
    )


def freeze_continuation(
    *,
    parent_deployment: Path,
    output_dir: Path,
    config_path: Path,
    screening_prompt_path: Path,
    coding_prompt_path: Path,
    protocol_path: Path,
    amendment_path: Path,
) -> dict[str, Any]:
    parent_run = parent_deployment / "results" / "run-v1"
    report_path = parent_run / "run_report.json"
    terminal_path = parent_run / "terminal_status.json"
    binding_path = parent_run / "run_binding.json"
    original_manifest_path = parent_deployment / "inputs" / "remaining_campaign_manifest.json"
    original_queue_path = parent_deployment / "inputs" / "remaining_screening_queue.jsonl"
    original_coding_seed_path = parent_deployment / "inputs" / "coding_seed_646.jsonl"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    original_manifest = json.loads(original_manifest_path.read_text(encoding="utf-8"))
    if report.get("status") != "STOPPED" or terminal.get("status") != "STOPPED":
        raise ValueError("parent run is not terminal")
    if report.get("stopping_reason") != "RUNTIME_LIMIT_DRAIN":
        raise ValueError("unexpected parent stopping reason")
    if int(report["screening"]["completed_records"]) != 116853:
        raise ValueError("parent screening completion count drift")
    if int(report["screening"]["counts"]["failed"]) != 33:
        raise ValueError("parent failed count drift")
    if int(report["coding"]["completed_records"]) != 8832:
        raise ValueError("parent coding completion count drift")
    if float(report.get("outstanding_reservations_usd", 0.0)) > 1e-9:
        raise ValueError("parent has a material unresolved reservation")
    if original_manifest["screening_queue"]["sha256"] != sha256_file(original_queue_path):
        raise ValueError("original screening queue binding drift")
    if original_manifest["coding_seed"]["sha256"] != sha256_file(original_coding_seed_path):
        raise ValueError("original coding seed binding drift")

    pending_path = parent_run / "screening_unprocessed.jsonl"
    pending_rows = list(load_jsonl(pending_path))
    pending_ids = [row.get("canonical_id") for row in pending_rows]
    if len(pending_rows) != 9315 or len(set(pending_ids)) != len(pending_ids):
        raise ValueError("pending screening membership drift")
    expected_order = []
    pending_set = set(pending_ids)
    for row in load_jsonl(original_queue_path):
        if row.get("canonical_id") in pending_set:
            expected_order.append(row.get("canonical_id"))
    if expected_order != pending_ids:
        raise ValueError("pending screening order drift")
    if any(row.get("batch_status") != "UNPROCESSED_READY_NOT_LAUNCHED" for row in pending_rows):
        raise ValueError("pending queue contains a non-ready record")

    uncoded_path = parent_run / "uncoded_survivors.jsonl"
    reported_uncoded_rows = list(load_jsonl(uncoded_path))
    reported_uncoded_ids = {row.get("canonical_id") for row in reported_uncoded_rows}
    if len(reported_uncoded_rows) != 2 or len(reported_uncoded_ids) != 2:
        raise ValueError("reported terminal uncoded membership drift")
    initial_seed_rows = list(load_jsonl(original_coding_seed_path))
    initial_seed_by_id = {row["canonical_id"]: row for row in initial_seed_rows}
    if len(initial_seed_by_id) != 646:
        raise ValueError("initial coding seed membership drift")
    import csv

    with (parent_run / "screening_advance.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        advance_ids = [row["canonical_id"] for row in csv.DictReader(handle)]
    candidate_ids = set(initial_seed_by_id) | set(advance_ids)
    if len(advance_ids) != 8190 or len(candidate_ids) != 8836:
        raise ValueError("parent candidate universe drift")
    with (parent_run / "categorized_candidates.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        coded_ids = {row["canonical_id"] for row in csv.DictReader(handle)}
    if len(coded_ids) != 8832 or not coded_ids.issubset(candidate_ids):
        raise ValueError("parent coded membership drift")
    pending_coding_ids = candidate_ids - coded_ids
    if len(pending_coding_ids) != 4 or not reported_uncoded_ids.issubset(
        pending_coding_ids
    ):
        raise ValueError("effective pending coding membership drift")
    source_by_id = dict(initial_seed_by_id)
    missing_queue_ids = pending_coding_ids - set(source_by_id)
    for row in load_jsonl(original_queue_path):
        if row["canonical_id"] in missing_queue_ids:
            source_by_id[row["canonical_id"]] = row
    if not pending_coding_ids.issubset(source_by_id):
        raise ValueError("original evidence is missing for a pending coding candidate")
    reported_by_id = {
        row["canonical_id"]: row for row in reported_uncoded_rows
    }
    next_order = max(int(row.get("coding_order") or 0) for row in reported_uncoded_rows)
    uncoded_rows = []
    for record_id in sorted(
        pending_coding_ids,
        key=lambda value: (int(source_by_id[value].get("batch_order") or 0), value),
    ):
        if record_id in reported_by_id:
            uncoded_rows.append(reported_by_id[record_id])
            continue
        next_order += 1
        uncoded_rows.append({
            **source_by_id[record_id],
            "coding_order": next_order,
            "coding_source": "new_remaining_screening_advance_smoke_reconciliation",
        })
    uncoded_ids = [row["canonical_id"] for row in uncoded_rows]
    for record_id in uncoded_ids:
        record_dir = parent_run / "coding_records" / str(record_id).replace(":", "_")
        if (record_dir / "result.json").exists():
            raise ValueError(f"pending coding record already has a result: {record_id}")
        for reservation in record_dir.glob("*.reservation.json") if record_dir.exists() else ():
            if not reservation.with_name(reservation.name.replace(".reservation.json", ".json")).exists():
                raise ValueError(f"pending coding record has an unresolved request: {record_id}")

    output_dir.mkdir(parents=True, exist_ok=False)
    queue_out = output_dir / "continuation_screening_9315.jsonl"
    coding_out = output_dir / "continuation_coding_seed_4.jsonl"
    write_jsonl(queue_out, pending_rows)
    write_jsonl(coding_out, uncoded_rows)
    bindings = {
        "config": config_path,
        "screening_prompt": screening_prompt_path,
        "coding_prompt": coding_prompt_path,
        "protocol": protocol_path,
        "amendment": amendment_path,
    }
    manifest = {
        "artifact_class": "title_abstract_remaining_campaign_continuation_inputs",
        "schema_version": "1.0.0",
        "status": "FROZEN_READY_FOR_AUTHORIZED_CONTINUATION",
        "screening_queue": {
            "path": str(queue_out), "sha256": sha256_file(queue_out),
            "records": len(pending_rows), "unique_ids": len(set(pending_ids)),
        },
        "coding_seed": {
            "path": str(coding_out), "sha256": sha256_file(coding_out),
            "records": len(uncoded_rows), "unique_ids": len(set(uncoded_ids)),
        },
        "partition": {
            "original_queue_records": 126168,
            "parent_completed_screening_records": 116853,
            "parent_valid_screening_records": 116820,
            "preserved_parent_failures_not_retried": 33,
            "continuation_screening_records": len(pending_rows),
            "parent_candidate_records": 8836,
            "parent_completed_coding_records": 8832,
            "continuation_coding_seed_records": len(uncoded_rows),
            "reconciliation_total": 116853 + len(pending_rows),
        },
        "parent_run": {
            "deployment": str(parent_deployment),
            "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
            "terminal_status": {"path": str(terminal_path), "sha256": sha256_file(terminal_path)},
            "run_binding": {"path": str(binding_path), "sha256": sha256_file(binding_path)},
            "screening_unprocessed": {"path": str(pending_path), "sha256": sha256_file(pending_path)},
            "uncoded_survivors": {"path": str(uncoded_path), "sha256": sha256_file(uncoded_path)},
            "terminal_uncoded_file_records": len(reported_uncoded_rows),
            "effective_uncoded_records_after_smoke_reconciliation": len(uncoded_rows),
            "cumulative_conservative_cost_usd": report["cumulative_conservative_cost_usd"],
            "outstanding_reservations_usd": report["outstanding_reservations_usd"],
        },
        "original_bindings": {
            "input_manifest": {"path": str(original_manifest_path), "sha256": sha256_file(original_manifest_path)},
            "screening_queue": {"path": str(original_queue_path), "sha256": sha256_file(original_queue_path)},
            "coding_seed": {"path": str(original_coding_seed_path), "sha256": sha256_file(original_coding_seed_path)},
            "corpus": original_manifest["frame"],
            "identity_overlay": original_manifest["identity_overlay"],
            "order": original_manifest["order"],
        },
        "bindings": {
            name: {"path": str(path), "sha256": sha256_file(path)} for name, path in bindings.items()
        },
        "constraints": {
            "parent_results_preserved": True,
            "parent_failures_retried": False,
            "ambiguous_requests_resubmitted": False,
            "smoke_advance_queue_omission_reconciled": True,
            "scientific_policy_changed": False,
            "staging_only": True,
        },
    }
    manifest_path = output_dir / "continuation_manifest.json"
    write_create_only(
        manifest_path,
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-deployment", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--screening-prompt", type=Path, required=True)
    parser.add_argument("--coding-prompt", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--amendment", type=Path, required=True)
    args = parser.parse_args()
    manifest = freeze_continuation(
        parent_deployment=args.parent_deployment, output_dir=args.output_dir,
        config_path=args.config, screening_prompt_path=args.screening_prompt,
        coding_prompt_path=args.coding_prompt, protocol_path=args.protocol,
        amendment_path=args.amendment,
    )
    print(json.dumps({"screening": manifest["screening_queue"], "coding": manifest["coding_seed"], "partition": manifest["partition"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
