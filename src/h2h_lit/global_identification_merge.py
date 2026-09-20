"""Provenance-bound orchestration for the final identification-data merge."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tempfile
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, BinaryIO

from h2h_lit import review as review_model
from h2h_lit.arxiv_snapshot_integration import (
    INTEGRATION_MARKER,
    ValidatedSnapshotPackage,
    validate_applied_snapshot_adjudications,
    validate_authorized_snapshot_substitution,
    validate_prepared_package,
)
from h2h_lit.checkpoint import atomic_write
from h2h_lit.prior_survey_integration import (
    merge_identification_datasets_with_prior_surveys_and_snapshot,
    validate_authorized_prior_survey_imports,
)
from h2h_lit.prior_survey_source_relocation import (
    ValidatedSourceRelocations,
    prepare_source_relocation_package,
    relocation_validation_boundary,
    validate_source_relocation_manifest,
)
from h2h_lit.retrieval import load_review_dataset
from h2h_lit.review import ReviewDataset

SCHEMA_VERSION = "1.0.0"
IMPLEMENTATION_VERSION = "global-identification-merge-v1"
EXPECTED_EXTERNAL_SOURCES = {
    "ACMDigitalLibrary",
    "EuropePMC",
    "IEEEXplore",
    "PubMed",
    "SemanticScholar",
    "arXiv",
}
CHECKPOINT_SOURCES = ("EuropePMC", "IEEEXplore", "PubMed", "SemanticScholar")
EXPECTED_PRIOR_SURVEYS = {"JFR25", "EBK25", "FP19"}
PRODUCTION_OUTPUT = (
    "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1"
)
GIB = 1024**3
MIN_AVAILABLE_MEMORY_BYTES = 22 * GIB


class GlobalIdentificationMergeError(RuntimeError):
    """Raised when merge inputs, resources, or persisted evidence drift."""


@dataclass(frozen=True, slots=True)
class ResourceAvailability:
    physical_memory_bytes: int
    available_memory_bytes: int
    free_disk_bytes: int


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode("utf-8")


def _json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _within_root(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise GlobalIdentificationMergeError(
            f"global-merge path escapes repository: {value}"
        ) from exc
    return resolved


def _file_reference(path: Path, root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        persisted = resolved.relative_to(root).as_posix()
        scope = "repository_relative"
    except ValueError:
        persisted = str(resolved)
        scope = "absolute_read_only"
    return {
        "path": persisted,
        "path_scope": scope,
        "byte_size": resolved.stat().st_size,
        "raw_sha256": _sha256_file(resolved),
    }


def _resolve_reference(root: Path, reference: Mapping[str, Any]) -> Path:
    raw = Path(str(reference.get("path") or ""))
    scope = reference.get("path_scope", "repository_relative")
    if scope == "repository_relative":
        path = _within_root(root, raw)
    elif scope == "absolute_read_only" and raw.is_absolute():
        path = raw.resolve()
    else:
        raise GlobalIdentificationMergeError("invalid global-merge reference scope")
    if not path.is_file():
        raise GlobalIdentificationMergeError(f"bound input is missing: {path}")
    if path.stat().st_size != reference.get("byte_size"):
        raise GlobalIdentificationMergeError(f"bound input size changed: {path}")
    if _sha256_file(path) != reference.get("raw_sha256"):
        raise GlobalIdentificationMergeError(f"bound input hash changed: {path}")
    return path


def _state_binding_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": state.get("schema_version"),
        "execution_id": state.get("execution_id"),
        "status": state.get("status"),
        "wave_manifest_hash": state.get("wave_manifest_hash"),
        "planned_wave_raw_sha256": state.get("planned_wave_raw_sha256"),
        "preflight_raw_sha256": state.get("preflight_raw_sha256"),
        "sources": state.get("sources"),
        "prior_survey_imports": state.get("prior_survey_imports"),
        "external_retrieval_completed_at_utc": state.get("external_retrieval_completed_at_utc"),
        "external_retrieval_cutoff_date": state.get("external_retrieval_cutoff_date"),
        "identification_set_closed": state.get("identification_set_closed"),
        "screening_executed": state.get("screening_executed"),
        "prisma_generated": state.get("prisma_generated"),
        "corpus_modified": state.get("corpus_modified"),
    }


def _implementation_bindings(root: Path) -> list[dict[str, Any]]:
    paths = (
        "src/h2h_lit/global_identification_merge.py",
        "src/h2h_lit/prior_survey_integration.py",
        "src/h2h_lit/prior_survey_source_relocation.py",
        "src/h2h_lit/arxiv_snapshot_integration.py",
        "src/h2h_lit/artifact_import.py",
        "src/h2h_lit/review.py",
        "src/h2h_lit/external_retrieval_wave.py",
    )
    return [
        {**_file_reference(root / path, root), "role": "merge-implementation"} for path in paths
    ]


def _dataset_input(
    *,
    input_id: str,
    role: str,
    source: str,
    reference: Mapping[str, Any],
    expected_occurrences: int,
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "input_id": input_id,
        "role": role,
        "source": source,
        "dataset": dict(reference),
        "expected_occurrences": expected_occurrences,
        "authorization_sha256": _json_hash(authorization),
    }


def build_global_merge_plan(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    source_relocation_binding: Mapping[str, Any] | None = None,
    prior_survey_validator: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Select and bind only current authoritative identification datasets."""

    root_path = Path(root).resolve()
    if state.get("status") != "COMPLETE":
        raise GlobalIdentificationMergeError("external retrieval is not complete")
    if state.get("identification_set_closed") is not False:
        raise GlobalIdentificationMergeError("identification is already closed")
    if any(
        state.get(key) is not False
        for key in ("screening_executed", "prisma_generated", "corpus_modified")
    ):
        raise GlobalIdentificationMergeError(
            "global merge requires unscreened, uncounted identification state"
        )
    sources = state.get("sources", {})
    if set(sources) != EXPECTED_EXTERNAL_SOURCES:
        raise GlobalIdentificationMergeError("registered external-source inventory changed")
    try:
        validate_authorized_snapshot_substitution(root=root_path, state=state)
        (prior_survey_validator or validate_authorized_prior_survey_imports)(
            root=root_path, state=state
        )
    except (RuntimeError, ValueError) as exc:
        raise GlobalIdentificationMergeError(str(exc)) from exc

    inputs: list[dict[str, Any]] = []
    acm = sources["ACMDigitalLibrary"]
    if acm.get("status") != "COMPLETE" or len(acm.get("family_datasets", [])) != 5:
        raise GlobalIdentificationMergeError("ACM authoritative family inventory changed")
    for family in sorted(acm["family_datasets"], key=lambda item: item["family_id"]):
        inputs.append(
            _dataset_input(
                input_id=f"external:ACMDigitalLibrary:{family['family_id']}",
                role="authoritative_external_query_family",
                source="ACMDigitalLibrary",
                reference=family["dataset"],
                expected_occurrences=int(family["occurrence_count"]),
                authorization=family,
            )
        )

    for source_name in CHECKPOINT_SOURCES:
        source = sources[source_name]
        if source.get("status") != "COMPLETE" or not source.get("checkpoint_dataset"):
            raise GlobalIdentificationMergeError(
                f"{source_name} lacks a current authoritative checkpoint"
            )
        inputs.append(
            _dataset_input(
                input_id=f"external:{source_name}:active",
                role="authoritative_external_checkpoint",
                source=source_name,
                reference=source["checkpoint_dataset"],
                expected_occurrences=int(source["occurrence_count"]),
                authorization=source,
            )
        )

    arxiv = sources["arXiv"]
    substitution = arxiv.get("approved_snapshot_substitution")
    if (
        arxiv.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or not isinstance(substitution, dict)
        or substitution.get("status") != "APPROVED_COMPLETE_REPLACEMENT_ROUTE"
    ):
        raise GlobalIdentificationMergeError("authorized arXiv snapshot replacement is missing")
    inputs.append(
        _dataset_input(
            input_id="external:arXivSnapshotV303:authorized-replacement",
            role="authorized_snapshot_replacement",
            source="arXivSnapshotV303",
            reference=substitution["snapshot_checkpoint"],
            expected_occurrences=int(substitution["counts"]["query_membership_occurrences"]),
            authorization=substitution,
        )
    )

    prior = state.get("prior_survey_imports", {})
    if set(prior) != EXPECTED_PRIOR_SURVEYS:
        raise GlobalIdentificationMergeError(
            "authorized prior-survey inventory must be exactly JFR25, EBK25, and FP19"
        )
    for seed_set_id in sorted(prior):
        registration = prior[seed_set_id]
        if registration.get("status") != "AUTHORIZED_IMPORTED_NOT_GLOBALLY_MERGED":
            raise GlobalIdentificationMergeError(f"{seed_set_id} is not ready for global merging")
        item = _dataset_input(
            input_id=f"prior-survey:{seed_set_id}",
            role="authorized_prior_survey",
            source=f"PriorSurveySeed/{seed_set_id}",
            reference=registration["dataset"],
            expected_occurrences=int(registration["counts"]["source_occurrences"]),
            authorization=registration,
        )
        item["source_contract_counts"] = dict(registration["counts"])
        inputs.append(item)

    for item in inputs:
        _resolve_reference(root_path, item["dataset"])
    plan = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "execution_state_binding": {
            "path": (
                "outputs/production/star-external-retrieval-wave-001/execution/execution_state.json"
            ),
            "raw_sha256": execution_state_raw_sha256,
            "content_fingerprint": _json_hash(_state_binding_payload(state)),
        },
        "inputs": inputs,
        "input_dataset_count": len(inputs),
        "input_occurrence_count": sum(int(item["expected_occurrences"]) for item in inputs),
        "authorization_bindings": {
            "snapshot_substitution_sha256": _json_hash(substitution),
            "prior_survey_registrations_sha256": _json_hash(prior),
        },
        "implementation_bindings": _implementation_bindings(root_path),
        "selection_policy": {
            "historical_recovery_checkpoints_included": False,
            "staging_datasets_included": False,
            "arxiv_api_checkpoint_included": False,
            "acm_current_family_datasets_included": 5,
            "current_external_checkpoints_included": list(CHECKPOINT_SOURCES),
            "authorized_snapshot_replacement_included": True,
            "authorized_prior_surveys_included": sorted(EXPECTED_PRIOR_SURVEYS),
        },
    }
    if source_relocation_binding is not None:
        if (
            source_relocation_binding.get("authoritative_execution_state_sha256")
            != execution_state_raw_sha256
            or source_relocation_binding.get("production_state_modified") is not False
        ):
            raise GlobalIdentificationMergeError(
                "source-relocation binding differs from authoritative state"
            )
        plan["source_relocation"] = dict(source_relocation_binding)
    plan["plan_sha256"] = _json_hash(plan)
    return plan


def _physical_memory_bytes() -> int:
    for pages_key in ("SC_PHYS_PAGES", "SC_AVPHYS_PAGES"):
        try:
            pages = int(os.sysconf(pages_key))
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
        except (OSError, ValueError):
            continue
        if pages > 0 and page_size > 0:
            return pages * page_size
    if platform.system() == "Darwin":
        try:
            output = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True)
            return int(output.strip())
        except (OSError, subprocess.CalledProcessError, ValueError) as exc:
            raise GlobalIdentificationMergeError("cannot determine physical memory safely") from exc
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    raise GlobalIdentificationMergeError("cannot determine physical memory safely")


def _available_memory_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
        raise GlobalIdentificationMergeError("Linux MemAvailable is missing from /proc/meminfo")
    if platform.system() == "Darwin":
        try:
            output = subprocess.check_output(["vm_stat"], text=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            raise GlobalIdentificationMergeError(
                "cannot determine available memory safely"
            ) from exc
        first_line, *stat_lines = output.splitlines()
        marker = "page size of "
        if marker not in first_line:
            raise GlobalIdentificationMergeError("unexpected vm_stat output")
        page_size = int(first_line.split(marker, 1)[1].split()[0])
        available_pages = 0
        accepted = {"Pages free", "Pages inactive", "Pages speculative"}
        for line in stat_lines:
            if ":" not in line:
                continue
            name, raw_value = line.split(":", 1)
            if name in accepted:
                available_pages += int(raw_value.strip().rstrip("."))
        if available_pages <= 0:
            raise GlobalIdentificationMergeError(
                "vm_stat did not report conservative available pages"
            )
        return available_pages * page_size
    raise GlobalIdentificationMergeError("cannot determine available memory safely")


def probe_resources(output_parent: Path) -> ResourceAvailability:
    existing = output_parent.resolve()
    while not existing.exists():
        existing = existing.parent
    return ResourceAvailability(
        physical_memory_bytes=_physical_memory_bytes(),
        available_memory_bytes=_available_memory_bytes(),
        free_disk_bytes=shutil.disk_usage(existing).free,
    )


def resource_preflight(
    plan: Mapping[str, Any], availability: ResourceAvailability
) -> dict[str, Any]:
    input_bytes = sum(int(item["dataset"]["byte_size"]) for item in plan["inputs"])
    required_memory = input_bytes * 6 + GIB
    required_disk = input_bytes * 2 + GIB
    installed_memory_sufficient = availability.physical_memory_bytes >= required_memory
    available_memory_sufficient = availability.available_memory_bytes >= MIN_AVAILABLE_MEMORY_BYTES
    return {
        "input_serialized_bytes": input_bytes,
        "estimated_peak_memory_bytes": required_memory,
        "required_free_disk_bytes": required_disk,
        "physical_memory_bytes": availability.physical_memory_bytes,
        "available_memory_bytes": availability.available_memory_bytes,
        "minimum_available_memory_bytes": MIN_AVAILABLE_MEMORY_BYTES,
        "free_disk_bytes": availability.free_disk_bytes,
        "installed_memory_sufficient": installed_memory_sufficient,
        "available_memory_sufficient": available_memory_sufficient,
        "memory_sufficient": (installed_memory_sufficient and available_memory_sufficient),
        "disk_sufficient": availability.free_disk_bytes >= required_disk,
        "estimate_qualification": (
            "Conservative estimate for simultaneous Python ReviewDataset objects, "
            "canonicalization indexes, and bounded streaming serialization; launch "
            "also requires at least 22 GiB reported actually available."
        ),
    }


def _write_json_once(path: Path, value: Any) -> dict[str, Any]:
    content = _json_bytes(value)
    if path.exists():
        if path.read_bytes() != content:
            raise GlobalIdentificationMergeError(
                f"refusing to overwrite changed global-merge artifact: {path}"
            )
    else:
        atomic_write(path, content)
    return {
        "path": path.name,
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _record_resource_preflight_once(path: Path, value: Mapping[str, Any]) -> None:
    """Keep the first resource observation while permitting safe later recovery."""

    if path.exists():
        existing = json.loads(path.read_bytes())
        if existing.get("plan_sha256") != value.get("plan_sha256"):
            raise GlobalIdentificationMergeError(
                "existing resource preflight belongs to a different input plan"
            )
        return
    atomic_write(path, _json_bytes(value))


class _HashingWriter:
    def __init__(self, handle: BinaryIO):
        self.handle = handle
        self.digest = hashlib.sha256()
        self.byte_size = 0

    def write(self, content: bytes) -> None:
        self.handle.write(content)
        self.digest.update(content)
        self.byte_size += len(content)


def _stream_dataset(path: Path, dataset: ReviewDataset, plan_sha256: str) -> dict[str, Any]:
    """Write one dataset atomically without materializing another giant JSON value."""

    if path.exists():
        return {
            "path": path.name,
            "byte_size": path.stat().st_size,
            "raw_sha256": _sha256_file(path),
        }
    temporary = path.parent / f".{path.name}.{plan_sha256}.tmp"
    if temporary.exists():
        temporary.unlink()
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            writer = _HashingWriter(handle)
            writer.write(b"{")
            first_field = True
            for item in sorted(fields(ReviewDataset), key=lambda value: value.name):
                if not first_field:
                    writer.write(b",")
                first_field = False
                writer.write(json.dumps(item.name).encode("ascii"))
                writer.write(b":")
                value = getattr(dataset, item.name)
                if isinstance(value, list):
                    writer.write(b"[")
                    for index, element in enumerate(value):
                        if index:
                            writer.write(b",")
                        writer.write(
                            json.dumps(
                                review_model._serialize(element),
                                sort_keys=True,
                                separators=(",", ":"),
                                ensure_ascii=True,
                            ).encode("utf-8")
                        )
                    writer.write(b"]")
                else:
                    writer.write(
                        json.dumps(
                            review_model._serialize(value),
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=True,
                        ).encode("utf-8")
                    )
            writer.write(b"}\n")
            handle.flush()
            os.fsync(handle.fileno())
            reference = {
                "path": path.name,
                "byte_size": writer.byte_size,
                "raw_sha256": writer.digest.hexdigest(),
            }
        if path.exists():
            existing_hash = _sha256_file(path)
            if (
                path.stat().st_size != reference["byte_size"]
                or existing_hash != reference["raw_sha256"]
            ):
                raise GlobalIdentificationMergeError(
                    "concurrent global-merge output conflicts with staged result"
                )
            temporary.unlink()
        else:
            os.replace(temporary, path)
        return reference
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _source_key(occurrence: Any) -> str:
    if occurrence.record.source_database == "PriorSurveySeed":
        seed = occurrence.record.original_metadata.get("seed_set_id")
        return f"PriorSurveySeed/{seed}"
    return occurrence.record.source_database


def _item_hashes(datasets: list[ReviewDataset], field_name: str, item_id: str) -> dict[str, str]:
    result: dict[str, str] = {}
    item_count = 0
    for dataset in datasets:
        for item in getattr(dataset, field_name):
            item_count += 1
            identifier = str(getattr(item, item_id))
            payload = review_model._serialize(item)
            if field_name == "occurrences":
                payload.get("metadata", {}).pop("identity_adjudication", None)
            elif field_name == "retrieval_runs":
                payload.get("metadata", {}).pop(INTEGRATION_MARKER, None)
            digest = _json_hash(payload)
            if identifier in result and result[identifier] != digest:
                raise GlobalIdentificationMergeError(
                    f"conflicting {field_name} identity across inputs: {identifier}"
                )
            result[identifier] = digest
    if len(result) != item_count:
        raise GlobalIdentificationMergeError(
            f"duplicate {field_name} identities across selected inputs"
        )
    return result


def _source_evidence_hashes(
    datasets: list[ReviewDataset],
) -> dict[str, dict[str, str]]:
    inventories = (
        ("occurrences", "occurrence_id"),
        ("retrieval_runs", "run_id"),
        ("source_queries", "query_id"),
        ("retrieval_pages", "page_id"),
        ("retrieval_attempts", "attempt_id"),
    )
    return {
        field_name: _item_hashes(datasets, field_name, item_id)
        for field_name, item_id in inventories
    }


def reconcile_global_merge(
    *,
    inputs: list[ReviewDataset],
    merged: ReviewDataset,
    snapshot_package: ValidatedSnapshotPackage,
    plan: Mapping[str, Any],
    source_evidence_hashes: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    """Prove occurrence, provenance, adjudication, and uncertainty preservation."""

    merged.validate()
    validate_applied_snapshot_adjudications(merged, snapshot_package)
    expected_occurrences = sum(len(dataset.occurrences) for dataset in inputs)
    if expected_occurrences != plan["input_occurrence_count"]:
        raise GlobalIdentificationMergeError("loaded occurrence count differs from plan")
    if len(merged.occurrences) != expected_occurrences:
        raise GlobalIdentificationMergeError("global merge discarded occurrences")
    inventory_checks = (
        ("occurrences", "occurrence_id"),
        ("retrieval_runs", "run_id"),
        ("source_queries", "query_id"),
        ("retrieval_pages", "page_id"),
        ("retrieval_attempts", "attempt_id"),
    )
    provenance_counts: dict[str, int] = {}
    for field_name, item_id in inventory_checks:
        before = dict(source_evidence_hashes[field_name])
        after = _item_hashes([merged], field_name, item_id)
        if before != after:
            raise GlobalIdentificationMergeError(
                f"global merge changed {field_name} content or identity inventory"
            )
        provenance_counts[field_name] = len(after)

    input_sources = Counter(
        _source_key(occurrence) for dataset in inputs for occurrence in dataset.occurrences
    )
    output_sources = Counter(_source_key(item) for item in merged.occurrences)
    if input_sources != output_sources:
        raise GlobalIdentificationMergeError("per-source occurrence counts changed")

    prior_input = Counter(
        (
            _source_key(item),
            item.record.original_metadata.get("source_survey_membership"),
            item.record.original_metadata.get("our_star_eligibility"),
        )
        for dataset in inputs
        for item in dataset.occurrences
        if item.record.source_database == "PriorSurveySeed"
    )
    prior_output = Counter(
        (
            _source_key(item),
            item.record.original_metadata.get("source_survey_membership"),
            item.record.original_metadata.get("our_star_eligibility"),
        )
        for item in merged.occurrences
        if item.record.source_database == "PriorSurveySeed"
    )
    if prior_input != prior_output:
        raise GlobalIdentificationMergeError(
            "prior-survey membership or eligibility labels changed"
        )

    generic = [
        item
        for item in merged.duplicate_decisions
        if item.provenance.actor.actor_id == "h2h_lit.artifact_import.merge"
    ]
    adjudicated = [
        item
        for item in merged.duplicate_decisions
        if item.match_rule == "adjudicated_exact_normalized_arxiv_id"
    ]
    effective = merged.effective_duplicate_decisions()
    if len(generic) != len(merged.occurrences) or len(effective) != len(merged.occurrences):
        raise GlobalIdentificationMergeError("duplicate-decision coverage changed")
    if len({item.provenance.metadata.get("proposal_id") for item in adjudicated}) != len(
        snapshot_package.identity_proposals
    ):
        raise GlobalIdentificationMergeError(
            "approved arXiv identity adjudication coverage changed"
        )

    markers = [
        run.metadata[INTEGRATION_MARKER]
        for run in merged.retrieval_runs
        if INTEGRATION_MARKER in run.metadata
    ]
    if len(markers) != 1:
        raise GlobalIdentificationMergeError("snapshot relationship marker changed")
    related_links = markers[0].get("related_version_links", [])
    if len(related_links) != len(snapshot_package.relationship_proposals):
        raise GlobalIdentificationMergeError("related-version links changed")

    expected_unresolved_groups = sum(
        int(item.get("source_contract_counts", {}).get("unresolved_identity_groups", 0))
        for item in plan["inputs"]
    )
    unresolved_source_groups: dict[str, set[str]] = {}
    for dataset in inputs:
        for occurrence in dataset.occurrences:
            metadata = occurrence.record.original_metadata
            if metadata.get("identity_resolution") != "UNRESOLVED":
                continue
            group_id = metadata.get("provisional_identity_group_id")
            if not group_id:
                raise GlobalIdentificationMergeError(
                    "unresolved prior-survey occurrence lacks its source group marker"
                )
            unresolved_source_groups.setdefault(str(group_id), set()).add(occurrence.occurrence_id)
    if len(unresolved_source_groups) != expected_unresolved_groups:
        raise GlobalIdentificationMergeError(
            "unresolved prior-survey source-group count differs from authorization"
        )
    unresolved = [
        item
        for item in merged.canonical_records
        if item.metadata.get("prior_survey_identity_status") == "UNRESOLVED"
    ]
    marked_unresolved_occurrences = {
        occurrence_id
        for item in unresolved
        for occurrence_id in item.metadata.get("unresolved_prior_survey_occurrence_ids", [])
    }
    source_unresolved_occurrences = {
        occurrence_id
        for occurrence_ids in unresolved_source_groups.values()
        for occurrence_id in occurrence_ids
    }
    if marked_unresolved_occurrences != source_unresolved_occurrences:
        raise GlobalIdentificationMergeError(
            "global merge changed unresolved prior-survey occurrence markers"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_sha256": plan["plan_sha256"],
        "input_occurrences": expected_occurrences,
        "output_occurrences": len(merged.occurrences),
        "occurrences_by_source": dict(sorted(output_sources.items())),
        "provenance_identity_counts": provenance_counts,
        "canonical_counts": {
            "generic_before_adjudication": len({item.canonical_record_id for item in generic}),
            "after_approved_adjudication": len(merged.canonical_records),
        },
        "duplicate_decisions": {
            "generic_history_entries": len(generic),
            "adjudicated_history_entries": len(adjudicated),
            "all_history_entries": len(merged.duplicate_decisions),
            "effective_decisions": len(effective),
            "approved_identity_proposals": len(snapshot_package.identity_proposals),
        },
        "related_versions": {
            "proposal_links": len(snapshot_package.relationship_proposals),
            "preserved_links": len(related_links),
            "collapsed": False,
        },
        "prior_survey_labels": [
            {
                "source": source,
                "membership": membership,
                "star_eligibility": eligibility,
                "occurrences": count,
            }
            for (source, membership, eligibility), count in sorted(prior_output.items())
        ],
        "remaining_identity_review": {
            "source_unresolved_identity_groups": len(unresolved_source_groups),
            "source_unresolved_identity_groups_sha256": _json_hash(
                {key: sorted(value) for key, value in sorted(unresolved_source_groups.items())}
            ),
            "source_unresolved_occurrences": len(source_unresolved_occurrences),
            "merged_canonical_records_carrying_unresolved_markers": len(unresolved),
            "related_version_candidates": len(
                {
                    item["candidate_identity"]["staging_candidate_id"]
                    for item in snapshot_package.relationship_proposals
                }
            ),
            "related_version_links": len(related_links),
            "study_grouping_decided": False,
        },
        "identification_closed": False,
        "screening_changed": False,
        "prisma_changed": False,
    }


def _load_inputs(root: Path, plan: Mapping[str, Any]) -> list[ReviewDataset]:
    datasets: list[ReviewDataset] = []
    for item in plan["inputs"]:
        path = _resolve_reference(root, item["dataset"])
        dataset = load_review_dataset(path)
        if len(dataset.occurrences) != item["expected_occurrences"]:
            raise GlobalIdentificationMergeError(
                f"loaded occurrence count changed for {item['input_id']}"
            )
        datasets.append(dataset)
    return datasets


def _snapshot_package(root: Path, state: Mapping[str, Any]) -> ValidatedSnapshotPackage:
    reference = state["sources"]["arXiv"]["approved_snapshot_substitution"]["package"]
    return validate_prepared_package(
        root / reference["path"],
        expected_manifest_sha256=reference["manifest_sha256"],
    )


def validate_staged_merge_package(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    state: Mapping[str, Any],
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    package_path = _within_root(root_path, package_dir)
    manifest_path = package_path / "package_manifest.json"
    if not manifest_path.is_file() or _sha256_file(manifest_path) != expected_manifest_sha256:
        raise GlobalIdentificationMergeError("staged merge manifest hash changed")
    manifest = json.loads(manifest_path.read_bytes())
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("status") != "STAGED_GLOBAL_MERGE_NOT_REGISTERED"
        or manifest.get("implementation_version") != IMPLEMENTATION_VERSION
        or manifest.get("identification_closed") is not False
        or manifest.get("screening_changed") is not False
        or manifest.get("prisma_changed") is not False
    ):
        raise GlobalIdentificationMergeError("staged merge contract changed")
    if manifest["plan"]["state_content_fingerprint"] != _json_hash(_state_binding_payload(state)):
        raise GlobalIdentificationMergeError("staged merge input state drifted")
    for key in ("dataset", "reconciliation", "input_inventory"):
        reference = manifest[key]
        path = package_path / reference["path"]
        if (
            not path.is_file()
            or path.stat().st_size != reference["byte_size"]
            or _sha256_file(path) != reference["raw_sha256"]
        ):
            raise GlobalIdentificationMergeError(f"staged merge {key} artifact changed")
    for reference in manifest["implementation_bindings"]:
        _resolve_reference(root_path, reference)
    relocation_binding = manifest.get("source_relocation")
    if relocation_binding is not None:
        validated_relocation = validate_source_relocation_manifest(
            root=root_path,
            state=state,
            execution_state_raw_sha256=manifest["prepared_from_execution_state_sha256"],
            manifest_path=relocation_binding["manifest"]["path"],
            expected_manifest_sha256=relocation_binding["manifest"]["raw_sha256"],
        )
        if validated_relocation.binding(root_path) != relocation_binding:
            raise GlobalIdentificationMergeError("staged merge source-relocation binding changed")
    return manifest


def prepare_staged_global_merge(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    output_dir: str | Path,
    preflight_only: bool = False,
    availability: ResourceAvailability | None = None,
    source_relocation_binding: Mapping[str, Any] | None = None,
    prior_survey_validator: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Prepare a bound staging merge without mutating production state."""

    root_path = Path(root).resolve()
    output = _within_root(root_path, output_dir)
    output.mkdir(parents=True, exist_ok=True)
    plan = build_global_merge_plan(
        root=root_path,
        state=state,
        execution_state_raw_sha256=execution_state_raw_sha256,
        source_relocation_binding=source_relocation_binding,
        prior_survey_validator=prior_survey_validator,
    )
    manifest_path = output / "package_manifest.json"
    if manifest_path.is_file():
        digest = _sha256_file(manifest_path)
        manifest = validate_staged_merge_package(
            root=root_path,
            package_dir=output,
            expected_manifest_sha256=digest,
            state=state,
        )
        return {"manifest": manifest, "manifest_sha256": digest, "idempotent": True}
    resources = resource_preflight(plan, availability or probe_resources(output.parent))
    preflight = {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "RESOURCE_PREFLIGHT_PASSED"
            if resources["memory_sufficient"] and resources["disk_sufficient"]
            else "RESOURCE_PREFLIGHT_BLOCKED"
        ),
        "plan_sha256": plan["plan_sha256"],
        "input_dataset_count": plan["input_dataset_count"],
        "input_occurrence_count": plan["input_occurrence_count"],
        "resources": resources,
        "production_state_modified": False,
    }
    _record_resource_preflight_once(output / "resource_preflight.json", preflight)
    inventory_reference = _write_json_once(output / "input_inventory.json", plan)
    if preflight_only:
        return {"preflight": preflight, "plan": plan}
    if not resources["memory_sufficient"] or not resources["disk_sufficient"]:
        raise GlobalIdentificationMergeError(
            "resource preflight refused the full in-memory global merge"
        )

    snapshot_package = _snapshot_package(root_path, state)
    datasets = _load_inputs(root_path, plan)
    source_evidence_hashes = _source_evidence_hashes(datasets)
    output_dataset_path = output / "review_dataset.json"
    if output_dataset_path.is_file():
        merged = load_review_dataset(output_dataset_path)
    else:
        merged = merge_identification_datasets_with_prior_surveys_and_snapshot(
            datasets, snapshot_package
        )
    reconciliation = reconcile_global_merge(
        inputs=datasets,
        merged=merged,
        snapshot_package=snapshot_package,
        plan=plan,
        source_evidence_hashes=source_evidence_hashes,
    )
    dataset_reference = _stream_dataset(output_dataset_path, merged, plan["plan_sha256"])
    reconciliation_reference = _write_json_once(output / "reconciliation.json", reconciliation)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": "STAGED_GLOBAL_MERGE_NOT_REGISTERED",
        "implementation_version": IMPLEMENTATION_VERSION,
        "prepared_from_execution_state_sha256": execution_state_raw_sha256,
        "plan": {
            "raw_sha256": plan["plan_sha256"],
            "state_content_fingerprint": plan["execution_state_binding"]["content_fingerprint"],
        },
        "input_inventory": inventory_reference,
        "dataset": dataset_reference,
        "reconciliation": reconciliation_reference,
        "implementation_bindings": plan["implementation_bindings"],
        "counts": reconciliation,
        "identification_closed": False,
        "screening_changed": False,
        "prisma_changed": False,
        "production_state_modified": False,
    }
    if source_relocation_binding is not None:
        manifest["source_relocation"] = dict(source_relocation_binding)
    manifest_reference = _write_json_once(manifest_path, manifest)
    return {
        "manifest": manifest,
        "manifest_sha256": manifest_reference["raw_sha256"],
        "idempotent": False,
    }


def _copy_once(
    source: Path,
    destination: Path,
    expected: Mapping[str, Any],
    root: Path,
) -> dict[str, Any]:
    if destination.exists():
        if (
            destination.stat().st_size != expected["byte_size"]
            or _sha256_file(destination) != expected["raw_sha256"]
        ):
            raise GlobalIdentificationMergeError(
                f"refusing to overwrite changed production artifact: {destination}"
            )
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", dir=destination.parent
        )
        try:
            digest = hashlib.sha256()
            size = 0
            with source.open("rb") as input_handle, os.fdopen(descriptor, "wb") as output_handle:
                for block in iter(lambda: input_handle.read(1024 * 1024), b""):
                    output_handle.write(block)
                    digest.update(block)
                    size += len(block)
                output_handle.flush()
                os.fsync(output_handle.fileno())
            if size != expected["byte_size"] or digest.hexdigest() != expected["raw_sha256"]:
                raise GlobalIdentificationMergeError(
                    "staged artifact changed during production copy"
                )
            os.replace(temporary_name, destination)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise
    return {
        "path": destination.relative_to(root).as_posix(),
        "path_scope": "repository_relative",
        "byte_size": destination.stat().st_size,
        "raw_sha256": _sha256_file(destination),
    }


def validate_registered_global_merge(*, root: Path, state: Mapping[str, Any]) -> None:
    registration = state.get("global_identification_merge")
    if registration is None:
        return
    if (
        registration.get("status") != "COMPLETE_NOT_IDENTIFICATION_CLOSED"
        or state.get("final_global_deduplication_executed") is not True
        or state.get("identification_set_closed") is not False
        or state.get("screening_executed") is not False
        or state.get("prisma_generated") is not False
    ):
        raise GlobalIdentificationMergeError("registered global merge state changed")
    for key in ("authorization", "dataset", "reconciliation", "input_inventory"):
        _resolve_reference(root, registration[key])


def authorize_production_global_merge(
    *,
    root: str | Path,
    state: dict[str, Any],
    package_dir: str | Path,
    expected_manifest_sha256: str,
    authorized_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Register a validated staged merge; caller must hold the shared lock."""

    root_path = Path(root).resolve()
    if state.get("global_identification_merge") is not None:
        validate_registered_global_merge(root=root_path, state=state)
        registration = state["global_identification_merge"]
        registered_manifest = registration["package_manifest"]
        requested_manifest = _within_root(root_path, package_dir) / ("package_manifest.json")
        registered_manifest_path = _resolve_reference(root_path, registered_manifest)
        if (
            registered_manifest.get("raw_sha256") != expected_manifest_sha256
            or registered_manifest_path != requested_manifest.resolve()
        ):
            raise GlobalIdentificationMergeError(
                "repeated global-merge authorization binding changed"
            )
        return state, dict(registration)
    if state.get("final_global_deduplication_executed") is not False:
        raise GlobalIdentificationMergeError("global merge state drifted")
    manifest = validate_staged_merge_package(
        root=root_path,
        package_dir=package_dir,
        expected_manifest_sha256=expected_manifest_sha256,
        state=state,
    )
    package_path = _within_root(root_path, package_dir)
    output = root_path / PRODUCTION_OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    dataset = _copy_once(
        package_path / manifest["dataset"]["path"],
        output / "review_dataset.json",
        manifest["dataset"],
        root_path,
    )
    reconciliation = _copy_once(
        package_path / manifest["reconciliation"]["path"],
        output / "reconciliation.json",
        manifest["reconciliation"],
        root_path,
    )
    inventory = _copy_once(
        package_path / manifest["input_inventory"]["path"],
        output / "input_inventory.json",
        manifest["input_inventory"],
        root_path,
    )
    package_manifest = {
        "path": (package_path / "package_manifest.json").relative_to(root_path).as_posix(),
        "path_scope": "repository_relative",
        "byte_size": (package_path / "package_manifest.json").stat().st_size,
        "raw_sha256": expected_manifest_sha256,
    }
    authorization = {
        "schema_version": SCHEMA_VERSION,
        "status": "COMPLETE_NOT_IDENTIFICATION_CLOSED",
        "authorized_at_utc": authorized_at,
        "package_manifest": package_manifest,
        "dataset": dataset,
        "reconciliation": reconciliation,
        "input_inventory": inventory,
        "identification_closed": False,
        "screening_changed": False,
        "prisma_changed": False,
    }
    authorization_path = output / "authorization.json"
    authorization_ref = _write_json_once(authorization_path, authorization)
    registration = {
        **authorization,
        "authorization": {
            **authorization_ref,
            "path": authorization_path.relative_to(root_path).as_posix(),
            "path_scope": "repository_relative",
        },
    }
    for key in ("dataset", "reconciliation", "input_inventory"):
        registration[key] = {
            **registration[key],
            "path": (root_path / registration[key]["path"]).relative_to(root_path).as_posix(),
            "path_scope": "repository_relative",
        }
    state["global_identification_merge"] = registration
    state["final_global_deduplication_executed"] = True
    validate_registered_global_merge(root=root_path, state=state)
    return state, registration


def _load_production_state(
    root: Path,
    *,
    state_loader: Callable[..., dict[str, Any]] | None = None,
) -> tuple[Path, bytes, dict[str, Any]]:
    from h2h_lit.external_retrieval_wave import (
        EXECUTION_STATE_PATH,
        _load_execution_state,
        _safe_output_path,
        validate_persisted_external_preflight,
    )

    wave, preflight = validate_persisted_external_preflight(root=root)
    state_path = _safe_output_path(root, EXECUTION_STATE_PATH)
    raw = state_path.read_bytes()
    loader = state_loader or _load_execution_state
    return state_path, raw, loader(state_path, root, wave, preflight)


def _validate_requested_source_relocation(
    *,
    root: Path,
    manifest_path: str | Path,
    expected_manifest_sha256: str,
) -> ValidatedSourceRelocations:
    from h2h_lit.external_retrieval_wave import (
        EXECUTION_STATE_PATH,
        _safe_output_path,
        validate_persisted_external_preflight,
    )

    validate_persisted_external_preflight(root=root)
    state_path = _safe_output_path(root, EXECUTION_STATE_PATH)
    raw = state_path.read_bytes()
    provisional_state = json.loads(raw)
    return validate_source_relocation_manifest(
        root=root,
        state=provisional_state,
        execution_state_raw_sha256=hashlib.sha256(raw).hexdigest(),
        manifest_path=manifest_path,
        expected_manifest_sha256=expected_manifest_sha256,
    )


def prepare_global_merge_locked(
    *,
    root: str | Path,
    output_dir: str | Path,
    preflight_only: bool = False,
    availability: ResourceAvailability | None = None,
    source_relocation_manifest: str | Path | None = None,
    source_relocation_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    from h2h_lit.external_retrieval_wave import _exclusive_external_source_session

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        if (source_relocation_manifest is None) != (source_relocation_manifest_sha256 is None):
            raise GlobalIdentificationMergeError(
                "source relocation requires both manifest path and SHA-256"
            )
        relocations = None
        if source_relocation_manifest is not None:
            relocations = _validate_requested_source_relocation(
                root=root_path,
                manifest_path=source_relocation_manifest,
                expected_manifest_sha256=str(source_relocation_manifest_sha256),
            )
        boundary = relocation_validation_boundary(relocations) if relocations else None
        if boundary is None:
            state_path, raw, state = _load_production_state(root_path)
        else:
            state_path, raw, state = _load_production_state(
                root_path, state_loader=boundary.execution_state_loader
            )
        result = prepare_staged_global_merge(
            root=root_path,
            state=state,
            execution_state_raw_sha256=hashlib.sha256(raw).hexdigest(),
            output_dir=output_dir,
            preflight_only=preflight_only,
            availability=availability,
            source_relocation_binding=(relocations.binding(root_path) if relocations else None),
            prior_survey_validator=(
                boundary.prior_survey_validator
                if boundary is not None
                else validate_authorized_prior_survey_imports
            ),
        )
        if state_path.read_bytes() != raw:
            raise GlobalIdentificationMergeError("staging global merge changed production state")
        return result


def prepare_source_relocation_package_locked(
    *, root: str | Path, output_dir: str | Path
) -> dict[str, Any]:
    from h2h_lit.external_retrieval_wave import _exclusive_external_source_session

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        state_path, raw, state = _load_production_state(root_path)
        result = prepare_source_relocation_package(
            root=root_path,
            state=state,
            execution_state_raw_sha256=hashlib.sha256(raw).hexdigest(),
            output_dir=output_dir,
        )
        if state_path.read_bytes() != raw:
            raise GlobalIdentificationMergeError(
                "source-relocation preparation changed production state"
            )
        return result


def authorize_global_merge_locked(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    authorized_at: str,
) -> dict[str, Any]:
    from h2h_lit.external_retrieval_wave import (
        _exclusive_external_source_session,
        _save_execution_state,
    )

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        state_path, _raw, state = _load_production_state(root_path)
        sources_before = json.loads(json.dumps(state["sources"], sort_keys=True))
        prior_before = json.loads(json.dumps(state["prior_survey_imports"], sort_keys=True))
        state, registration = authorize_production_global_merge(
            root=root_path,
            state=state,
            package_dir=package_dir,
            expected_manifest_sha256=expected_manifest_sha256,
            authorized_at=authorized_at,
        )
        if state["sources"] != sources_before or state["prior_survey_imports"] != prior_before:
            raise GlobalIdentificationMergeError(
                "global merge registration changed input source state"
            )
        if any(
            state.get(key) is not False
            for key in (
                "identification_set_closed",
                "screening_executed",
                "prisma_generated",
                "corpus_modified",
            )
        ):
            raise GlobalIdentificationMergeError(
                "global merge registration changed a closure or screening gate"
            )
        _save_execution_state(state_path, state)
        validate_registered_global_merge(root=root_path, state=state)
        return registration


def _utc_now() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--prepare-global-identification-merge", action="store_true")
    modes.add_argument("--prepare-prior-survey-source-relocation", action="store_true")
    modes.add_argument("--authorize-global-identification-merge", action="store_true")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--package-dir", type=Path)
    parser.add_argument("--package-manifest-sha256")
    parser.add_argument("--source-relocation-manifest", type=Path)
    parser.add_argument("--source-relocation-manifest-sha256")
    args = parser.parse_args(argv)
    if args.prepare_global_identification_merge:
        if args.output_dir is None:
            parser.error("--output-dir is required for staging")
        result = prepare_global_merge_locked(
            root=args.root,
            output_dir=args.output_dir,
            preflight_only=args.preflight_only,
            source_relocation_manifest=args.source_relocation_manifest,
            source_relocation_manifest_sha256=(args.source_relocation_manifest_sha256),
        )
    elif args.prepare_prior_survey_source_relocation:
        if args.preflight_only:
            parser.error("--preflight-only is only valid for global-merge staging")
        if args.output_dir is None:
            parser.error("--output-dir is required for source relocation")
        if args.source_relocation_manifest or args.source_relocation_manifest_sha256:
            parser.error("source relocation inputs are not valid while preparing them")
        result = prepare_source_relocation_package_locked(
            root=args.root,
            output_dir=args.output_dir,
        )
    else:
        if args.preflight_only:
            parser.error("--preflight-only is only valid for staging")
        if args.package_dir is None or not args.package_manifest_sha256:
            parser.error("--package-dir and --package-manifest-sha256 are required")
        if args.source_relocation_manifest or args.source_relocation_manifest_sha256:
            parser.error("source relocation is staging-only")
        result = authorize_global_merge_locked(
            root=args.root,
            package_dir=args.package_dir,
            expected_manifest_sha256=args.package_manifest_sha256,
            authorized_at=_utc_now(),
        )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
