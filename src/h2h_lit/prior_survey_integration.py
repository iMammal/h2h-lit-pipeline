"""Provenance-bound authorization for qualified prior-survey evidence."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h2h_lit.arxiv_snapshot_integration import (
    ValidatedSnapshotPackage,
    merge_identification_datasets_with_snapshot_integration,
)
from h2h_lit.checkpoint import atomic_write
from h2h_lit.retrieval import load_review_dataset
from h2h_lit.review import ReviewDataset
from h2h_lit.sources.prior_survey_seed import import_seed_manifest

PRODUCTION_RELATIVE_ROOT = (
    "outputs/production/star-external-retrieval-wave-001/execution/PriorSurveySeed"
)
PACKAGE_SCHEMA_VERSION = "1.0.0"
AUTHORIZED_STATUS = "AUTHORIZED_IMPORTED_NOT_GLOBALLY_MERGED"
READY_STATUS = "READY_FOR_EXPLICIT_AUTHORIZATION"
ALLOWED_SEED_SET_IDS = {"EBK25", "JFR25", "FP19"}
ALLOWED_DESIGNATIONS = {
    "SUPPORTED_AUTHOR_COMPANION_MEMBERSHIP_LIST",
    "AUTHOR_SUPPLIED_REFERENCE_LEDGER",
    "PUBLISHED_SURVEY_BIBLIOGRAPHY_RECONSTRUCTION",
}


class PriorSurveyIntegrationError(ValueError):
    """Raised when qualified seed evidence or authorization lineage changes."""


@dataclass(frozen=True, slots=True)
class ValidatedPriorSurveyPackage:
    package_dir: Path
    package_manifest: dict[str, Any]
    package_manifest_sha256: str
    import_manifest_path: Path
    import_manifest_sha256: str
    identity_review_path: Path
    identity_review_sha256: str
    dataset: ReviewDataset

    @property
    def seed_set_id(self) -> str:
        return str(self.package_manifest["seed_set_id"])


def authorization_state_fingerprint(state: Mapping[str, Any]) -> str:
    """Hash external retrieval and closure state, excluding seed registrations."""

    material = {
        "schema_version": state.get("schema_version"),
        "execution_id": state.get("execution_id"),
        "status": state.get("status"),
        "wave_manifest_hash": state.get("wave_manifest_hash"),
        "planned_wave_raw_sha256": state.get("planned_wave_raw_sha256"),
        "preflight_raw_sha256": state.get("preflight_raw_sha256"),
        "sources": state.get("sources"),
        "external_retrieval_completed_at_utc": state.get(
            "external_retrieval_completed_at_utc"
        ),
        "external_retrieval_cutoff_date": state.get(
            "external_retrieval_cutoff_date"
        ),
        "identification_set_closed": state.get("identification_set_closed"),
        "final_global_deduplication_executed": state.get(
            "final_global_deduplication_executed"
        ),
        "screening_executed": state.get("screening_executed"),
        "prisma_generated": state.get("prisma_generated"),
        "corpus_modified": state.get("corpus_modified"),
    }
    return _json_hash(material)


def validate_prior_survey_package(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_package_manifest_sha256: str,
    state: Mapping[str, Any],
) -> ValidatedPriorSurveyPackage:
    """Validate a complete staging package without mutating it or production."""

    root_path = Path(root).resolve()
    package_path = _within_root(root_path, package_dir)
    manifest_path = package_path / "package_manifest.json"
    manifest_raw = manifest_path.read_bytes()
    manifest_hash = hashlib.sha256(manifest_raw).hexdigest()
    if manifest_hash != expected_package_manifest_sha256:
        raise PriorSurveyIntegrationError("prior-survey package manifest hash changed")
    manifest = json.loads(manifest_raw)
    if (
        manifest.get("schema_version") != PACKAGE_SCHEMA_VERSION
        or manifest.get("status") != READY_STATUS
    ):
        raise PriorSurveyIntegrationError("prior-survey package is not authorization-ready")
    seed_set_id = str(manifest.get("seed_set_id") or "")
    if seed_set_id not in ALLOWED_SEED_SET_IDS:
        raise PriorSurveyIntegrationError("unsupported prior-survey seed set")
    if manifest.get("source_designation") not in ALLOWED_DESIGNATIONS:
        raise PriorSurveyIntegrationError("unsupported prior-survey source designation")
    if manifest.get("star_eligibility_status") != "UNASSESSED":
        raise PriorSurveyIntegrationError("prior-survey package changed STAR eligibility")
    if manifest.get("production_eligible") is not False or manifest.get(
        "prisma_counted"
    ) is not False:
        raise PriorSurveyIntegrationError(
            "prior-survey package cannot pre-authorize production eligibility or PRISMA"
        )
    binding = manifest.get("production_state_binding", {})
    if binding.get("authorization_state_fingerprint") != authorization_state_fingerprint(
        state
    ):
        raise PriorSurveyIntegrationError("unexpected production-state drift")

    for reference in manifest.get("source_artifacts", []):
        _verify_source_artifact(root_path, reference)
    for reference in manifest.get("source_package_bindings", []):
        _verify_source_artifact(root_path, reference)
    for reference in manifest.get("implementation_bindings", []):
        _verify_source_artifact(root_path, reference)

    import_path, import_hash = _verify_package_artifact(
        package_path, manifest.get("import_manifest", {})
    )
    identity_path, identity_hash = _verify_package_artifact(
        package_path, manifest.get("identity_review", {})
    )
    import_manifest = json.loads(import_path.read_bytes())
    identity_review = json.loads(identity_path.read_bytes())
    _validate_import_bindings(manifest, import_manifest, identity_review)
    dataset = import_seed_manifest(import_path)
    _annotate_qualified_dataset(
        dataset,
        package_manifest=manifest,
        package_manifest_sha256=manifest_hash,
        identity_review=identity_review,
        identity_review_sha256=identity_hash,
    )
    _validate_counts(dataset, manifest, identity_review)
    dataset.validate()
    return ValidatedPriorSurveyPackage(
        package_dir=package_path,
        package_manifest=manifest,
        package_manifest_sha256=manifest_hash,
        import_manifest_path=import_path,
        import_manifest_sha256=import_hash,
        identity_review_path=identity_path,
        identity_review_sha256=identity_hash,
        dataset=dataset,
    )


def authorize_prior_survey_package_locked(
    *,
    root: str | Path,
    state: dict[str, Any],
    package_dir: str | Path,
    expected_package_manifest_sha256: str,
    authorized_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Authorize one import; the caller must hold the shared exclusive lock."""

    root_path = Path(root).resolve()
    _require_open_identification_state(state)
    package = validate_prior_survey_package(
        root=root_path,
        package_dir=package_dir,
        expected_package_manifest_sha256=expected_package_manifest_sha256,
        state=state,
    )
    registry = state.setdefault("prior_survey_imports", {})
    existing = registry.get(package.seed_set_id)
    if existing is not None:
        validate_authorized_prior_survey_imports(root=root_path, state=state)
        if (
            existing.get("package_manifest_sha256")
            != package.package_manifest_sha256
            or existing.get("source_designation")
            != package.package_manifest["source_designation"]
        ):
            raise PriorSurveyIntegrationError(
                "conflicting prior-survey source registration"
            )
        return state, dict(existing)

    version = str(package.package_manifest["package_version"])
    if not re.fullmatch(r"[A-Za-z0-9._-]+", version):
        raise PriorSurveyIntegrationError("unsafe prior-survey package version")
    output_dir = root_path / PRODUCTION_RELATIVE_ROOT / package.seed_set_id / version
    dataset_path = output_dir / "review_dataset.json"
    authorization_path = output_dir / "authorization.json"

    dataset_bytes = (package.dataset.to_json() + "\n").encode("utf-8")
    _write_once(dataset_path, dataset_bytes)
    dataset_reference = _file_reference(dataset_path, root_path)
    counts = dict(package.package_manifest["counts"])
    if authorization_path.is_file():
        existing_authorization = json.loads(authorization_path.read_bytes())
        if (
            existing_authorization.get("seed_set_id") != package.seed_set_id
            or existing_authorization.get("package_manifest", {}).get("raw_sha256")
            != package.package_manifest_sha256
            or existing_authorization.get("dataset") != dataset_reference
        ):
            raise PriorSurveyIntegrationError(
                "existing prior-survey authorization artifact has conflicting provenance"
            )
        authorized_at = str(existing_authorization.get("authorized_at_utc") or "")
        if not authorized_at:
            raise PriorSurveyIntegrationError(
                "existing prior-survey authorization timestamp is missing"
            )
    authorization = {
        "schema_version": "1.0.0",
        "status": AUTHORIZED_STATUS,
        "seed_set_id": package.seed_set_id,
        "package_version": version,
        "authorized_at_utc": authorized_at,
        "package_manifest": {
            "path": package.package_dir.relative_to(root_path).as_posix()
            + "/package_manifest.json",
            "raw_sha256": package.package_manifest_sha256,
        },
        "import_manifest": {
            **_file_reference(package.import_manifest_path, root_path),
        },
        "identity_review": {
            **_file_reference(package.identity_review_path, root_path),
        },
        "dataset": dataset_reference,
        "source_designation": package.package_manifest["source_designation"],
        "membership_status": package.package_manifest["membership_status"],
        "identity_status": package.package_manifest["identity_status"],
        "star_eligibility_status": "UNASSESSED",
        "counts": counts,
        "global_merge_contract": package.package_manifest["global_merge_contract"],
        "identification_closure_changed": False,
        "screening_changed": False,
        "prisma_changed": False,
    }
    authorization_bytes = _pretty_json(authorization)
    _write_once(authorization_path, authorization_bytes)
    registry_entry = {
        "status": AUTHORIZED_STATUS,
        "seed_set_id": package.seed_set_id,
        "package_version": version,
        "source_designation": authorization["source_designation"],
        "membership_status": authorization["membership_status"],
        "identity_status": authorization["identity_status"],
        "package_manifest_path": authorization["package_manifest"]["path"],
        "package_manifest_sha256": package.package_manifest_sha256,
        "authorization": _file_reference(authorization_path, root_path),
        "dataset": dataset_reference,
        "counts": counts,
        "authorized_at_utc": authorized_at,
    }
    registry[package.seed_set_id] = registry_entry
    validate_authorized_prior_survey_imports(root=root_path, state=state)
    return state, registry_entry


def validate_authorized_prior_survey_imports(
    *, root: str | Path, state: Mapping[str, Any]
) -> None:
    """Validate every registered import on every later state load."""

    root_path = Path(root).resolve()
    for seed_set_id, entry in state.get("prior_survey_imports", {}).items():
        if seed_set_id not in ALLOWED_SEED_SET_IDS:
            raise PriorSurveyIntegrationError(
                "authorized prior-survey registry contains an unknown source"
            )
        authorization_path = root_path / entry["authorization"]["path"]
        _verify_reference(authorization_path, entry["authorization"], root_path)
        authorization = json.loads(authorization_path.read_bytes())
        if (
            authorization.get("status") != AUTHORIZED_STATUS
            or authorization.get("seed_set_id") != seed_set_id
            or authorization.get("package_manifest", {}).get("raw_sha256")
            != entry.get("package_manifest_sha256")
            or authorization.get("source_designation")
            != entry.get("source_designation")
            or authorization.get("counts") != entry.get("counts")
        ):
            raise PriorSurveyIntegrationError(
                "authorized prior-survey registry provenance changed"
            )
        dataset_path = root_path / entry["dataset"]["path"]
        _verify_reference(dataset_path, entry["dataset"], root_path)
        dataset = load_review_dataset(dataset_path)
        package_manifest_path = root_path / authorization["package_manifest"]["path"]
        validated_package = validate_prior_survey_package(
            root=root_path,
            package_dir=package_manifest_path.parent,
            expected_package_manifest_sha256=entry["package_manifest_sha256"],
            state=state,
        )
        regenerated = (validated_package.dataset.to_json() + "\n").encode("utf-8")
        if hashlib.sha256(regenerated).hexdigest() != entry["dataset"]["raw_sha256"]:
            raise PriorSurveyIntegrationError(
                "authorized prior-survey dataset no longer matches bound package"
            )
        counts = entry["counts"]
        if (
            len(dataset.occurrences) != counts["source_occurrences"]
            or len(dataset.canonical_records) != counts["provisional_identity_groups"]
        ):
            raise PriorSurveyIntegrationError(
                "authorized prior-survey dataset counts changed"
            )


def merge_identification_datasets_with_prior_surveys_and_snapshot(
    datasets: list[ReviewDataset],
    snapshot_package: ValidatedSnapshotPackage,
    *,
    created_at: str | None = None,
) -> ReviewDataset:
    """Use the mandatory snapshot-aware merge and retain seed uncertainty markers."""

    occurrence_count = sum(len(dataset.occurrences) for dataset in datasets)
    merged = merge_identification_datasets_with_snapshot_integration(
        datasets, snapshot_package, created_at=created_at
    )
    if len(merged.occurrences) != occurrence_count:
        raise PriorSurveyIntegrationError(
            "global merge discarded source occurrence evidence"
        )
    occurrence_by_id = {item.occurrence_id: item for item in merged.occurrences}
    for canonical in merged.canonical_records:
        unresolved = [
            occurrence_id
            for occurrence_id in canonical.occurrence_ids
            if occurrence_by_id[occurrence_id].record.original_metadata.get(
                "identity_resolution"
            )
            == "UNRESOLVED"
        ]
        if unresolved:
            canonical.metadata["prior_survey_identity_status"] = "UNRESOLVED"
            canonical.metadata["unresolved_prior_survey_occurrence_ids"] = unresolved
    merged.validate()
    return merged


def _validate_import_bindings(
    package: Mapping[str, Any],
    manifest: Mapping[str, Any],
    identity_review: Mapping[str, Any],
) -> None:
    for key in (
        "seed_set_id",
        "source_designation",
        "membership_status",
        "identity_status",
        "star_eligibility_status",
    ):
        if manifest.get(key) != package.get(key):
            raise PriorSurveyIntegrationError(
                f"prior-survey import manifest {key} binding changed"
            )
    if manifest.get("schema_version") != "1.1.0":
        raise PriorSurveyIntegrationError("qualified import manifest schema changed")
    if identity_review.get("seed_set_id") != package.get("seed_set_id"):
        raise PriorSurveyIntegrationError("identity review seed binding changed")
    if identity_review.get("status") != "REVIEWED_WITH_UNCERTAINTY_PRESERVED":
        raise PriorSurveyIntegrationError("identity review status changed")


def _annotate_qualified_dataset(
    dataset: ReviewDataset,
    *,
    package_manifest: Mapping[str, Any],
    package_manifest_sha256: str,
    identity_review: Mapping[str, Any],
    identity_review_sha256: str,
) -> None:
    unresolved_groups = {
        item["group_id"]
        for item in identity_review.get("groups", [])
        if item.get("identity_resolution") == "UNRESOLVED"
    }
    shared = {
        "package_manifest_sha256": package_manifest_sha256,
        "source_designation": package_manifest["source_designation"],
        "membership_status": package_manifest["membership_status"],
        "identity_status": package_manifest["identity_status"],
        "star_eligibility_status": "UNASSESSED",
        "identity_review_sha256": identity_review_sha256,
        "global_merge_contract": package_manifest["global_merge_contract"],
    }
    for query in dataset.source_queries:
        query.metadata.update(shared)
    for run in dataset.retrieval_runs:
        run.metadata.update(shared)
    for occurrence in dataset.occurrences:
        metadata = occurrence.record.original_metadata
        group_id = metadata.get("provisional_identity_group_id")
        metadata["identity_resolution"] = (
            "UNRESOLVED" if group_id in unresolved_groups else "PROVISIONAL"
        )
        metadata["authorization_package_sha256"] = package_manifest_sha256
    for canonical in dataset.canonical_records:
        group_ids = {
            dataset_occurrence.record.original_metadata.get(
                "provisional_identity_group_id"
            )
            for dataset_occurrence in dataset.occurrences
            if dataset_occurrence.occurrence_id in canonical.occurrence_ids
        }
        canonical.metadata["identity_status"] = "PROVISIONAL"
        if group_ids & unresolved_groups:
            canonical.metadata["identity_resolution"] = "UNRESOLVED"
    for decision in dataset.duplicate_decisions:
        decision.provenance.metadata.update(
            {
                "identity_authority": "PROVISIONAL_SOURCE_GROUPING_NOT_ADJUDICATED",
                "package_manifest_sha256": package_manifest_sha256,
            }
        )


def _validate_counts(
    dataset: ReviewDataset,
    package: Mapping[str, Any],
    identity_review: Mapping[str, Any],
) -> None:
    counts = package.get("counts", {})
    if (
        len(dataset.occurrences) != counts.get("source_occurrences")
        or len(dataset.canonical_records) != counts.get("provisional_identity_groups")
    ):
        raise PriorSurveyIntegrationError("prior-survey package count binding changed")
    occurrence_memberships = Counter(
        item.record.original_metadata.get("source_survey_membership")
        for item in dataset.occurrences
    )
    expected_occurrence_memberships = counts.get("occurrence_membership_counts", {})
    if dict(sorted(occurrence_memberships.items())) != dict(
        sorted(expected_occurrence_memberships.items())
    ):
        raise PriorSurveyIntegrationError(
            "prior-survey occurrence membership counts changed"
        )
    groups = identity_review.get("groups", [])
    unresolved = sum(
        item.get("identity_resolution") == "UNRESOLVED" for item in groups
    )
    if unresolved != counts.get("unresolved_identity_groups"):
        raise PriorSurveyIntegrationError("unresolved identity count changed")
    group_memberships = Counter(
        item.get("source_survey_membership") for item in groups
    )
    expected_group_counts = {
        "CONFIRMED_SURVEY_MEMBER": counts.get("confirmed_memberships", 0),
        "SUPPORTED_SURVEY_MEMBER": counts.get("supported_memberships", 0),
        "UNCONFIRMED": counts.get("unknown_memberships", 0),
        "BACKGROUND_REFERENCE": counts.get("background_references", 0),
    }
    if any(
        group_memberships.get(status, 0) != expected
        for status, expected in expected_group_counts.items()
    ):
        raise PriorSurveyIntegrationError(
            "prior-survey group membership counts changed"
        )


def _require_open_identification_state(state: Mapping[str, Any]) -> None:
    if state.get("status") != "COMPLETE":
        raise PriorSurveyIntegrationError("external retrieval is not complete")
    forbidden = {
        "identification_set_closed": False,
        "final_global_deduplication_executed": False,
        "screening_executed": False,
        "prisma_generated": False,
        "corpus_modified": False,
    }
    if any(state.get(key) is not expected for key, expected in forbidden.items()):
        raise PriorSurveyIntegrationError(
            "prior-survey authorization requires open, unmodified identification state"
        )


def _verify_package_artifact(
    package_dir: Path, reference: Mapping[str, Any]
) -> tuple[Path, str]:
    relative = Path(str(reference.get("path") or ""))
    if relative.is_absolute() or ".." in relative.parts:
        raise PriorSurveyIntegrationError("unsafe prior-survey package artifact path")
    path = (package_dir / relative).resolve()
    if package_dir not in path.parents:
        raise PriorSurveyIntegrationError("prior-survey package artifact escaped package")
    digest = _verify_bytes(path, reference)
    return path, digest


def _verify_source_artifact(root: Path, reference: Mapping[str, Any]) -> None:
    scope = reference.get("path_scope")
    raw_path = Path(str(reference.get("path") or ""))
    if scope == "repository_relative":
        path = (root / raw_path).resolve()
        if root not in path.parents:
            raise PriorSurveyIntegrationError("source artifact escaped repository")
    elif scope == "absolute_read_only":
        if not raw_path.is_absolute():
            raise PriorSurveyIntegrationError("absolute source artifact path required")
        path = raw_path.resolve()
    else:
        raise PriorSurveyIntegrationError("unsupported source artifact path scope")
    _verify_bytes(path, reference)


def _verify_bytes(path: Path, reference: Mapping[str, Any]) -> str:
    if not path.is_file():
        raise PriorSurveyIntegrationError(f"bound artifact is missing: {path}")
    digest = _sha256_file(path)
    if path.stat().st_size != reference.get("byte_size") or digest != reference.get(
        "raw_sha256"
    ):
        raise PriorSurveyIntegrationError(f"bound artifact changed: {path}")
    return digest


def _verify_reference(
    path: Path, reference: Mapping[str, Any], root: Path
) -> None:
    if path.resolve().relative_to(root).as_posix() != reference.get("path"):
        raise PriorSurveyIntegrationError("authorized artifact path binding changed")
    _verify_bytes(path, reference)


def _within_root(root: Path, value: str | Path) -> Path:
    path = Path(value)
    resolved = (root / path).resolve() if not path.is_absolute() else path.resolve()
    if root != resolved and root not in resolved.parents:
        raise PriorSurveyIntegrationError("prior-survey package must be within repository")
    if not resolved.is_dir():
        raise PriorSurveyIntegrationError("prior-survey package directory is missing")
    return resolved


def _write_once(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise PriorSurveyIntegrationError(
                f"refusing to overwrite changed authorization artifact: {path}"
            )
        return
    atomic_write(path, content)


def _file_reference(path: Path, root: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "path": path.resolve().relative_to(root).as_posix(),
        "byte_size": len(raw),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _pretty_json(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
