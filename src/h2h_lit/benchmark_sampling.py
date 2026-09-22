"""Memory-bounded staging draw for the title/abstract benchmark pilot.

The registered global ReviewDataset is several gigabytes.  This module deliberately
does not call ``load_review_dataset``.  It streams the ``canonical_records`` array,
retains only bounded top-k candidate heaps plus canonical IDs, and reads the small
``related_version_links`` array separately.  Model annotations, eligibility decisions,
and human screening outputs are neither loaded nor used for selection.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
import io
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.human_validation_review import prepare_review_workspace
from h2h_lit.prior_survey_identity_adjudication import (
    _load_production_state,
    validate_registered_identity_confirmation_overlay,
)

SCHEMA_VERSION = "1.0.0"
IMPLEMENTATION_VERSION = "title-abstract-benchmark-sampler-v1"
DEFAULT_SPEC = "config/title_abstract_benchmark_staging_v1.json"
RUBRIC_PATH = "docs/revised_star_coding_rubric.md"
EXPECTED_CUTOFF = "2026-09-20"

MODALITY_ASSISTANCE_RE = re.compile(
    r"\b(?:virtual reality|augmented reality|mixed reality|immersive|cave|"
    r"head[- ]mounted|wall[- ]?display|tiled display|large display|conversational|"
    r"chatbot|large language model|llms?|natural language|mixed[- ]initiative|"
    r"recommender|adaptive guidance|agentic)\b",
    re.IGNORECASE,
)
DOCUMENT_BOUNDARY_RE = re.compile(
    r"\b(?:survey|review|bibliometric|systematic review|scoping review|protocol|"
    r"framework|poster|demo(?:nstration)?|workshop|position paper|editorial)\b",
    re.IGNORECASE,
)
WORD_RE = re.compile(r"\b\w+\b", re.UNICODE)


class BenchmarkSamplingError(RuntimeError):
    """Raised when frozen bindings or sampling invariants drift."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode()


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise BenchmarkSamplingError(f"refusing to overwrite staging artifact: {path}")
        return
    atomic_write(path, content)


def _reference(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "path_scope": "repository_relative",
        "byte_size": path.stat().st_size,
        "raw_sha256": _sha256_file(path),
    }


def _iter_json_array(path: Path, key: str, *, chunk_size: int = 1024 * 1024) -> Iterator[Any]:
    """Yield one value at a time from a compact top-level JSON array."""

    marker = json.dumps(key, ensure_ascii=True) + ":["
    decoder = json.JSONDecoder()
    with path.open("r", encoding="utf-8") as handle:
        buffer = ""
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                raise BenchmarkSamplingError(f"top-level array not found: {key}")
            buffer += chunk
            position = buffer.find(marker)
            if position >= 0:
                buffer = buffer[position + len(marker) :]
                break
            buffer = buffer[-(len(marker) - 1) :]

        while True:
            buffer = buffer.lstrip()
            while buffer.startswith(","):
                buffer = buffer[1:].lstrip()
            if buffer.startswith("]"):
                return
            try:
                value, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                chunk = handle.read(chunk_size)
                if not chunk:
                    raise BenchmarkSamplingError(f"unterminated top-level array: {key}")
                buffer += chunk
                continue
            yield value
            buffer = buffer[end:]


def _rank(seed: str, purpose: str, canonical_id: str) -> int:
    material = f"{seed}\x1f{purpose}\x1f{canonical_id}".encode()
    return int.from_bytes(hashlib.sha256(material).digest(), "big")


def _rank_hex(seed: str, purpose: str, canonical_id: str) -> str:
    return hashlib.sha256(f"{seed}\x1f{purpose}\x1f{canonical_id}".encode()).hexdigest()


def _bounded_add(
    heap: list[tuple[int, str, dict[str, Any]]],
    *,
    limit: int,
    rank: int,
    record: dict[str, Any],
) -> None:
    item = (-rank, str(record["canonical_id"]), record)
    if len(heap) < limit:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


def _heap_records(heap: Iterable[tuple[int, str, dict[str, Any]]]) -> list[dict[str, Any]]:
    return [item[2] for item in sorted(heap, key=lambda item: (-item[0], item[1]))]


def _record_summary(item: Mapping[str, Any], overlay_ids: set[str]) -> dict[str, Any]:
    record = item.get("record", {})
    metadata = item.get("metadata", {})
    canonical_id = str(item.get("canonical_id") or "")
    if not canonical_id.startswith("canonical:"):
        raise BenchmarkSamplingError("registered canonical ID changed")
    abstract = str(record.get("abstract") or "").strip()
    return {
        "canonical_id": canonical_id,
        "title": str(record.get("title") or "").strip(),
        "abstract": abstract,
        "publication_year": record.get("year"),
        "source_database": str(record.get("source_database") or ""),
        "source_identifier": str(record.get("source_identifier") or ""),
        "provenance_source_databases": sorted(
            {
                str(event.get("source_database") or "")
                for event in record.get("provenance", [])
                if isinstance(event, Mapping) and event.get("source_database")
            }
        ),
        "effective_prior_survey_identity_status": (
            "ADJUDICATED_CONFIRMED"
            if canonical_id in overlay_ids
            else metadata.get("prior_survey_identity_status")
        ),
        "historical_prior_survey_identity_status": metadata.get(
            "prior_survey_identity_status"
        ),
    }


def _challenge_stratum(record: Mapping[str, Any]) -> str | None:
    source_values = {
        str(record.get("source_database") or ""),
        *[str(value) for value in record.get("provenance_source_databases", [])],
    }
    source_identifier = str(record.get("source_identifier") or "")
    if "PriorSurveySeed" in source_values or re.match(r"^(?:EBK25|FP19|JFR25):", source_identifier):
        return "prior_survey_route"
    abstract = str(record.get("abstract") or "").strip()
    if not abstract:
        return "missing_abstract"
    if len(WORD_RE.findall(abstract)) <= 79:
        return "sparse_abstract"
    text = f"{record.get('title') or ''}\n{abstract}"
    if MODALITY_ASSISTANCE_RE.search(text):
        return "uncommon_modality_or_assistance_cue"
    if DOCUMENT_BOUNDARY_RE.search(str(record.get("title") or "")):
        return "document_type_boundary_cue"
    return None


def _related_components(links: Iterable[Mapping[str, Any]]) -> tuple[dict[str, str], list[list[str]]]:
    parent: dict[str, str] = {}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)

    for link in links:
        left = str(link.get("existing_canonical_id") or "")
        right = str(link.get("snapshot_canonical_id") or "")
        if left and right:
            union(left, right)
    groups: dict[str, list[str]] = defaultdict(list)
    for value in parent:
        groups[find(value)].append(value)
    components = [sorted(values) for values in groups.values()]
    components.sort()
    mapping = {value: values[0] for values in components for value in values}
    return mapping, components


def _csv_bytes(records: list[Mapping[str, Any]]) -> bytes:
    buffer = io.StringIO(newline="")
    fields = ("sample_order", "canonical_id", "title", "abstract", "publication_year")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for index, record in enumerate(records, start=1):
        writer.writerow(
            {
                "sample_order": index,
                "canonical_id": record["canonical_id"],
                "title": record.get("title") or "",
                "abstract": record.get("abstract") or "",
                "publication_year": record.get("publication_year") or "",
            }
        )
    return buffer.getvalue().encode()


def _assignment_plan(records: list[Mapping[str, Any]], reviewer_id: str | None) -> dict[str, Any]:
    if reviewer_id is None:
        return {"assignments": []}
    return {
        "assignments": [
            {
                "record_id": item["canonical_id"],
                "slot_id": "primary",
                "reviewer_id": reviewer_id,
                "due_date": None,
                "status": "PLANNED",
                "reassignment_history": [],
            }
            for item in records
        ]
    }


def prepare_staging_benchmark(
    *,
    root: str | Path,
    output_dir: str | Path,
    generated_at: str,
    spec_path: str | Path = DEFAULT_SPEC,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    output = (
        (root_path / output_dir).resolve()
        if not Path(output_dir).is_absolute()
        else Path(output_dir).resolve()
    )
    try:
        output.relative_to(root_path)
    except ValueError as exc:
        raise BenchmarkSamplingError("output directory escapes repository") from exc
    specification_path = (root_path / spec_path).resolve()
    spec = json.loads(specification_path.read_bytes())
    seed = str(spec["seed"])

    _state_path, _state_raw, state = _load_production_state(root_path)
    if state.get("external_retrieval_cutoff_date") != EXPECTED_CUTOFF:
        raise BenchmarkSamplingError("E6 retrieval cutoff binding changed")
    overlay_validation = validate_registered_identity_confirmation_overlay(
        root=root_path, state=state, verify_dataset_hash=False
    )
    if overlay_validation is None:
        raise BenchmarkSamplingError("registered identity-confirmation overlay is absent")
    dataset_reference = state["global_identification_merge"]["dataset"]
    dataset_path = root_path / dataset_reference["path"]
    overlay_registration = state["prior_survey_identity_confirmation"]
    overlay_path = root_path / overlay_registration["overlay"]["path"]
    overlay = json.loads(overlay_path.read_bytes())
    overlay_ids = {str(item["canonical_record_id"]) for item in overlay["groups"]}
    if len(overlay_ids) != 30:
        raise BenchmarkSamplingError("effective overlay group count changed")

    dataset_hash = _sha256_file(dataset_path)
    if dataset_hash != dataset_reference["raw_sha256"]:
        raise BenchmarkSamplingError("registered dataset hash changed")
    if dataset_hash != spec["frame"]["registered_dataset_sha256"]:
        raise BenchmarkSamplingError("sampling specification dataset binding changed")
    overlay_hash = _sha256_file(overlay_path)
    if overlay_hash != spec["frame"]["identity_overlay_sha256"]:
        raise BenchmarkSamplingError("sampling specification overlay binding changed")

    challenge_heaps: dict[str, list[tuple[int, str, dict[str, Any]]]] = {
        str(item["stratum_id"]): [] for item in spec["challenge_strata"]
    }
    challenge_counts: Counter[str] = Counter()
    random_heap: list[tuple[int, str, dict[str, Any]]] = []
    calibration_heap: list[tuple[int, str, dict[str, Any]]] = []
    canonical_ids: list[str] = []

    for raw in _iter_json_array(dataset_path, "canonical_records"):
        record = _record_summary(raw, overlay_ids)
        canonical_id = record["canonical_id"]
        canonical_ids.append(canonical_id)
        stratum = _challenge_stratum(record)
        if stratum is not None:
            challenge_counts[stratum] += 1
            _bounded_add(
                challenge_heaps[stratum],
                limit=6,
                rank=_rank(seed, f"challenge:{stratum}", canonical_id),
                record=record,
            )
        _bounded_add(
            random_heap,
            limit=256,
            rank=_rank(seed, "uniform-random-evaluation", canonical_id),
            record=record,
        )
        _bounded_add(
            calibration_heap,
            limit=512,
            rank=_rank(seed, "calibration", canonical_id),
            record=record,
        )

    if len(canonical_ids) != int(spec["frame"]["canonical_records"]):
        raise BenchmarkSamplingError("registered canonical count changed")
    if len(set(canonical_ids)) != len(canonical_ids):
        raise BenchmarkSamplingError("registered canonical IDs are not unique")
    shortfalls = {
        stratum: 6 - challenge_counts[stratum]
        for stratum in challenge_heaps
        if challenge_counts[stratum] < 6
    }
    if shortfalls:
        raise BenchmarkSamplingError(f"challenge-stratum shortfall: {shortfalls}")

    related_links = list(_iter_json_array(dataset_path, "related_version_links"))
    related_map, related_components = _related_components(related_links)

    challenge: list[dict[str, Any]] = []
    for stratum in spec["overlap_priority"]:
        for record in _heap_records(challenge_heaps[stratum]):
            challenge.append({**record, "selection_group": "challenge", "challenge_stratum": stratum})
    challenge_ids = {item["canonical_id"] for item in challenge}
    random_records = [
        {**item, "selection_group": "uniform_random", "challenge_stratum": None}
        for item in _heap_records(random_heap)
        if item["canonical_id"] not in challenge_ids
    ][:70]
    if len(random_records) != 70:
        raise BenchmarkSamplingError("bounded random heap did not supply 70 records")
    evaluation = challenge + random_records
    evaluation_ids = {item["canonical_id"] for item in evaluation}

    evaluation_components = {
        related_map[record_id] for record_id in evaluation_ids if record_id in related_map
    }
    calibration = [
        {**item, "selection_group": "calibration", "challenge_stratum": None}
        for item in _heap_records(calibration_heap)
        if item["canonical_id"] not in evaluation_ids
        and related_map.get(item["canonical_id"]) not in evaluation_components
    ][:10]
    if len(calibration) != 10:
        raise BenchmarkSamplingError("bounded calibration heap did not supply 10 records")

    evaluation.sort(key=lambda item: (_rank(seed, "blinded-packet-order", item["canonical_id"]), item["canonical_id"]))
    calibration.sort(key=lambda item: (_rank(seed, "calibration-packet-order", item["canonical_id"]), item["canonical_id"]))
    secondary_a = sorted(
        evaluation,
        key=lambda item: (_rank(seed, "secondary-a", item["canonical_id"]), item["canonical_id"]),
    )[:25]
    secondary_a_ids = {item["canonical_id"] for item in secondary_a}
    secondary_b = sorted(
        [item for item in evaluation if item["canonical_id"] not in secondary_a_ids],
        key=lambda item: (_rank(seed, "secondary-b", item["canonical_id"]), item["canonical_id"]),
    )[:25]

    frame_hasher = hashlib.sha256()
    for line in (
        IMPLEMENTATION_VERSION,
        dataset_hash,
        overlay_hash,
        EXPECTED_CUTOFF,
        *sorted(canonical_ids),
    ):
        frame_hasher.update(line.encode())
        frame_hasher.update(b"\n")
    frame_hash = frame_hasher.hexdigest()

    output.mkdir(parents=True, exist_ok=True)
    source_dir = output / "blinded_sources"
    internal_dir = output / "internal"
    source_dir.mkdir(parents=True, exist_ok=True)
    internal_dir.mkdir(parents=True, exist_ok=True)
    packet_sets = {
        "primary_100": evaluation,
        "secondary_a_25": secondary_a,
        "secondary_b_25": secondary_b,
        "calibration_10": calibration,
    }
    source_paths: dict[str, Path] = {}
    for name, records in packet_sets.items():
        path = source_dir / f"{name}.csv"
        _write_once(path, _csv_bytes(records))
        source_paths[name] = path

    plans = {
        "primary_100": _assignment_plan(evaluation, "morris"),
        "secondary_a_25": _assignment_plan(secondary_a, None),
        "secondary_b_25": _assignment_plan(secondary_b, None),
        "calibration_10": _assignment_plan(calibration, None),
    }
    plan_paths: dict[str, Path | None] = {}
    for name, plan in plans.items():
        if not plan["assignments"]:
            plan_paths[name] = None
            continue
        path = internal_dir / f"{name}_assignment_plan.json"
        _write_once(path, _json_bytes(plan))
        plan_paths[name] = path

    manifest = {
        "artifact_class": "TITLE_ABSTRACT_BENCHMARK_STAGING_MANIFEST",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "STAGED_NOT_REVIEWED_NOT_IMPORTED",
        "generated_at_utc": generated_at,
        "e6_binding": {
            "retrieval_cutoff_date": EXPECTED_CUTOFF,
            "source": "registered external-retrieval wave",
            "identification_set_closed": bool(state.get("identification_set_closed")),
            "pilot_interpretation": (
                "This pilot is frozen to the September 20, 2026 retrieval cutoff. It does "
                "not establish final paper eligibility or close identification."
            ),
        },
        "frame": {
            "frame_sha256": frame_hash,
            "canonical_records": len(canonical_ids),
            "registered_dataset": _reference(dataset_path, root_path),
            "identity_overlay": _reference(overlay_path, root_path),
            "overlay_effective_status": {
                "adjudicated_confirmed_groups": len(overlay_ids),
                "open_identity_groups": 0,
            },
        },
        "sampling_specification": _reference(specification_path, root_path),
        "seed": seed,
        "selection_algorithm": spec["selection_algorithm"],
        "challenge": {
            "overlap_priority": spec["overlap_priority"],
            "shortfall_handling": spec["shortfall_handling"],
            "available_exclusive_stratum_counts": dict(sorted(challenge_counts.items())),
            "selected_per_stratum": dict(Counter(item["challenge_stratum"] for item in challenge)),
        },
        "counts": {
            "evaluation_total": len(evaluation),
            "uniform_random_evaluation": len(random_records),
            "challenge_evaluation": len(challenge),
            "calibration": len(calibration),
            "secondary_a": len(secondary_a),
            "secondary_b": len(secondary_b),
            "secondary_overlap": len(secondary_a_ids & {item["canonical_id"] for item in secondary_b}),
            "related_version_links": len(related_links),
            "related_version_components": len(related_components),
        },
        "related_version_policy": spec["related_version_policy"],
        "related_version_components": related_components,
        "secondary_selection": {
            "secondary_a_purpose": "secondary-a",
            "secondary_b_purpose": "secondary-b-after-removing-secondary-a",
            "reviewer_identities": None,
            "deadlines": None,
            "completed_independent_reviews": 0,
        },
        "selected_records": [
            {
                **item,
                "selection_rank_sha256": _rank_hex(
                    seed,
                    (
                        f"challenge:{item['challenge_stratum']}"
                        if item["selection_group"] == "challenge"
                        else item["selection_group"].replace("uniform_random", "uniform-random-evaluation")
                    ),
                    item["canonical_id"],
                ),
            }
            for item in evaluation + calibration
        ],
        "secondary_record_ids": {
            "secondary_a_25": [item["canonical_id"] for item in secondary_a],
            "secondary_b_25": [item["canonical_id"] for item in secondary_b],
        },
        "blinding": {
            "evaluation_workbooks_exclude": [
                "model outputs",
                "challenge strata",
                "selection ranks",
                "expected answers",
                "prior human judgments",
            ],
            "internal_manifest_only": True,
        },
        "state_effects": {
            "model_calls": 0,
            "emails": 0,
            "production_screening_imports": 0,
            "identification_closed": False,
            "prisma_changed": False,
        },
    }
    manifest_path = internal_dir / "sampling_manifest.json"
    _write_once(manifest_path, _json_bytes(manifest))

    workspace_results: dict[str, Any] = {}
    for name, records in packet_sets.items():
        workspace_results[name] = prepare_review_workspace(
            root=root_path,
            source_csv=source_paths[name],
            rubric_path=root_path / RUBRIC_PATH,
            output_dir=output / "review_workspaces" / name,
            generated_at=generated_at,
            assignment_plan=plan_paths[name],
            source_manifest=manifest_path,
        )

    result = {
        "output_dir": str(output),
        "sampling_manifest": str(manifest_path),
        "frame_sha256": frame_hash,
        "counts": manifest["counts"],
        "available_exclusive_stratum_counts": dict(sorted(challenge_counts.items())),
        "workspaces": workspace_results,
    }
    _write_once(output / "staging_result.json", _json_bytes(result))
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--generated-at", required=True)
    parser.add_argument("--spec", default=DEFAULT_SPEC)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = prepare_staging_benchmark(
        root=args.root,
        output_dir=args.output_dir,
        generated_at=args.generated_at,
        spec_path=args.spec,
    )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
