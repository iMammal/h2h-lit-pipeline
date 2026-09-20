"""Hash-bound overlay for post-merge prior-survey identity confirmations.

The overlay is intentionally separate from the registered global-merge dataset.  Generic
execution-state loads do not validate this extension; consumers that use the adjudicated
status must call :func:`validate_registered_identity_confirmation_overlay` explicitly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.review import DuplicateDecision, ReviewDataset

SCHEMA_VERSION = "1.0.0"
IMPLEMENTATION_VERSION = "prior-survey-identity-adjudication-v1"
DRY_RUN_ARTIFACT_CLASS = "PRIOR_SURVEY_IDENTITY_CONFIRMATION_DRY_RUN_PACKAGE"
DRY_RUN_STATUS = "OFFLINE_DRY_RUN_NOT_AUTHORIZED_NOT_APPLIED"
READY_STATUS = "READY_FOR_EXPLICIT_IDENTITY_CONFIRMATION_AUTHORIZATION"
REGISTERED_STATUS = "AUTHORIZED_IDENTITY_CONFIRMATIONS_NOT_IDENTIFICATION_CLOSED"
STATE_KEY = "prior_survey_identity_confirmation"
PRODUCTION_RELATIVE_ROOT = (
    "outputs/production/star-external-retrieval-wave-001/execution/"
    "PriorSurveyIdentityAdjudication/v1"
)
EXPECTED_GROUPS = 30
EXPECTED_OCCURRENCES = 71
EXPECTED_TOTALS = {
    "occurrences": 187_446,
    "canonical_records": 140_959,
    "related_version_links": 50,
    "effective_duplicate_decisions": 187_446,
}
PRESERVED_STATE_FLAGS = {
    "identification_set_closed": False,
    "screening_executed": False,
    "prisma_generated": False,
    "corpus_modified": False,
    "final_global_deduplication_executed": True,
}


class PriorSurveyIdentityAdjudicationError(ValueError):
    """Raised when an identity-confirmation input, overlay, or registration drifts."""


@dataclass(frozen=True, slots=True)
class ValidatedIdentityConfirmationPackage:
    package_dir: Path
    manifest: dict[str, Any]
    manifest_sha256: str
    overlay: dict[str, Any]
    overlay_sha256: str


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _within_root(root: Path, value: str | Path) -> Path:
    candidate = Path(value)
    resolved = (root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise PriorSurveyIdentityAdjudicationError(
            f"identity-adjudication path escapes repository: {value}"
        ) from exc
    return resolved


def _relative_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise PriorSurveyIdentityAdjudicationError(
            f"identity-adjudication artifact is outside repository: {path}"
        ) from exc


def _file_reference(path: Path, root: Path, *, role: str | None = None) -> dict[str, Any]:
    reference: dict[str, Any] = {
        "path": _relative_path(path, root),
        "path_scope": "repository_relative",
        "byte_size": path.stat().st_size,
        "raw_sha256": _sha256_file(path),
    }
    if role is not None:
        reference["role"] = role
    return reference


def _verify_reference(
    root: Path,
    reference: Mapping[str, Any],
    *,
    verify_hash: bool = True,
) -> Path:
    if reference.get("path_scope", "repository_relative") != "repository_relative":
        raise PriorSurveyIdentityAdjudicationError("unsupported artifact path scope")
    path = _within_root(root, str(reference.get("path") or ""))
    if not path.is_file():
        raise PriorSurveyIdentityAdjudicationError(f"bound artifact is missing: {path}")
    if "byte_size" in reference and path.stat().st_size != reference.get("byte_size"):
        raise PriorSurveyIdentityAdjudicationError(f"bound artifact size changed: {path}")
    if verify_hash and _sha256_file(path) != reference.get("raw_sha256"):
        raise PriorSurveyIdentityAdjudicationError(f"bound artifact hash changed: {path}")
    return path


def _verify_package_artifact(package_dir: Path, reference: Mapping[str, Any]) -> tuple[Path, str]:
    path = (package_dir / str(reference.get("path") or "")).resolve()
    try:
        path.relative_to(package_dir)
    except ValueError as exc:
        raise PriorSurveyIdentityAdjudicationError(
            "package artifact escapes package directory"
        ) from exc
    if not path.is_file() or path.stat().st_size != reference.get("byte_size"):
        raise PriorSurveyIdentityAdjudicationError("package artifact size or path changed")
    digest = _sha256_file(path)
    if digest != reference.get("raw_sha256"):
        raise PriorSurveyIdentityAdjudicationError("package artifact hash changed")
    return path, digest


def _write_json_once(path: Path, value: Any) -> dict[str, Any]:
    content = _json_bytes(value)
    if path.exists():
        if path.read_bytes() != content:
            raise PriorSurveyIdentityAdjudicationError(
                f"refusing to overwrite changed identity-adjudication artifact: {path}"
            )
    else:
        atomic_write(path, content)
    return {
        "path": path.name,
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _copy_once(
    source: Path, destination: Path, expected: Mapping[str, Any], root: Path
) -> dict[str, Any]:
    if _sha256_file(source) != expected.get("raw_sha256"):
        raise PriorSurveyIdentityAdjudicationError("source overlay hash changed")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if _sha256_file(destination) != expected.get("raw_sha256"):
            raise PriorSurveyIdentityAdjudicationError(
                "refusing to overwrite changed registered overlay"
            )
    else:
        temporary = destination.with_name(f".{destination.name}.tmp")
        if temporary.exists():
            temporary.unlink()
        shutil.copyfile(source, temporary)
        temporary.replace(destination)
    return _file_reference(destination, root)


def _assert_preserved_state(state: Mapping[str, Any]) -> None:
    for key, expected in PRESERVED_STATE_FLAGS.items():
        if state.get(key) is not expected:
            raise PriorSurveyIdentityAdjudicationError(
                f"identity-adjudication requires {key}={expected!r}"
            )
    registration = state.get("global_identification_merge")
    if not isinstance(registration, Mapping):
        raise PriorSurveyIdentityAdjudicationError("registered global merge is missing")
    if registration.get("status") != "COMPLETE_NOT_IDENTIFICATION_CLOSED":
        raise PriorSurveyIdentityAdjudicationError("registered global merge status changed")


def _verify_proposal_package(root: Path, manifest_reference: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = _verify_reference(root, manifest_reference)
    manifest = json.loads(manifest_path.read_bytes())
    if (
        manifest.get("artifact_class") != "PRIOR_SURVEY_IDENTITY_REVIEW_PROPOSAL_PACKAGE"
        or manifest.get("schema_version") != "2.0.0"
        or manifest.get("status") != "STAGED_REVIEW_NOT_APPLIED"
    ):
        raise PriorSurveyIdentityAdjudicationError("proposal package contract changed")
    package_dir = manifest_path.parent
    for reference in manifest.get("artifacts", {}).values():
        _verify_package_artifact(package_dir, reference)
    source = manifest.get("bindings", {}).get("source_review_package", {})
    source_manifest_path = _within_root(
        root, Path(str(source.get("path") or "")) / "package_manifest.json"
    )
    if _sha256_file(source_manifest_path) != source.get("package_manifest_sha256"):
        raise PriorSurveyIdentityAdjudicationError("source review manifest changed")
    review_manifest = json.loads(source_manifest_path.read_bytes())
    review_reference = review_manifest.get("artifacts", {}).get("review_groups", {})
    review_path, review_hash = _verify_package_artifact(
        source_manifest_path.parent, review_reference
    )
    if review_hash != source.get("review_groups_sha256"):
        raise PriorSurveyIdentityAdjudicationError("source review groups changed")
    return {
        "manifest": manifest,
        "manifest_path": manifest_path,
        "review_groups": json.loads(review_path.read_bytes()),
        "review_groups_path": review_path,
    }


def _validate_groups(
    application: Mapping[str, Any], review_groups: Mapping[str, Any]
) -> list[dict[str, Any]]:
    groups = application.get("groups")
    if not isinstance(groups, list) or len(groups) != EXPECTED_GROUPS:
        raise PriorSurveyIdentityAdjudicationError("identity group count changed")
    source_groups = review_groups.get("groups")
    if not isinstance(source_groups, list) or len(source_groups) != EXPECTED_GROUPS:
        raise PriorSurveyIdentityAdjudicationError("source review group count changed")
    source_by_id = {str(item.get("provisional_identity_group_id")): item for item in source_groups}
    if len(source_by_id) != EXPECTED_GROUPS:
        raise PriorSurveyIdentityAdjudicationError("source review group IDs are not unique")

    decisions: list[dict[str, Any]] = []
    seen_groups: set[str] = set()
    seen_occurrences: set[str] = set()
    seen_decisions: set[str] = set()
    for group in groups:
        group_id = str(group.get("group_id") or "")
        if group_id in seen_groups or group_id not in source_by_id:
            raise PriorSurveyIdentityAdjudicationError("identity group set changed")
        seen_groups.add(group_id)
        source = source_by_id[group_id]
        source_canonical = source.get("current_canonical_record", {})
        expected_occurrences = list(source_canonical.get("occurrence_ids", []))
        if (
            group.get("proposed_outcome") != "MERGE"
            or group.get("planned_current_status") != "ADJUDICATED_CONFIRMED"
            or group.get("canonical_id_before") != source.get("canonical_id")
            or group.get("canonical_id_after_dry_run") != source.get("canonical_id")
            or group.get("survivor_occurrence_id_before")
            != source_canonical.get("survivor_occurrence_id")
            or group.get("survivor_occurrence_id_after_dry_run")
            != source_canonical.get("survivor_occurrence_id")
            or set(group.get("occurrence_ids_before", [])) != set(expected_occurrences)
            or set(group.get("occurrence_ids_after_dry_run", [])) != set(expected_occurrences)
            or any(
                group.get("occurrence_membership_delta", {}).get(key)
                for key in (
                    "added",
                    "deleted",
                    "moved",
                )
            )
        ):
            raise PriorSurveyIdentityAdjudicationError(
                f"identity group changes canonical membership: {group_id}"
            )
        historical = group.get("historical_status", {})
        preserved_labels = group.get("preserved_labels", {})
        if (
            historical.get("canonical_marker") != "UNRESOLVED"
            or set(historical.get("unresolved_occurrence_ids", [])) != set(expected_occurrences)
            or preserved_labels.get("source_survey_membership") != ["UNCONFIRMED"]
            or preserved_labels.get("our_star_eligibility") != ["UNASSESSED"]
            or preserved_labels.get("prisma_counted") != [False]
            or preserved_labels.get("production_eligible") != [False]
        ):
            raise PriorSurveyIdentityAdjudicationError(
                f"historical uncertainty or protected labels changed: {group_id}"
            )
        effective = {
            item["occurrence_id"]: item for item in source.get("effective_duplicate_decisions", [])
        }
        planned = group.get("planned_duplicate_decisions", [])
        if len(planned) != len(expected_occurrences):
            raise PriorSurveyIdentityAdjudicationError(
                f"planned decision count changed: {group_id}"
            )
        for item in planned:
            occurrence_id = str(item.get("occurrence_id") or "")
            prior = effective.get(occurrence_id)
            if occurrence_id in seen_occurrences or prior is None:
                raise PriorSurveyIdentityAdjudicationError(
                    "planned occurrence set is duplicated or changed"
                )
            seen_occurrences.add(occurrence_id)
            decision_id = str(item.get("decision_id") or "")
            if decision_id in seen_decisions:
                raise PriorSurveyIdentityAdjudicationError("decision IDs are not unique")
            seen_decisions.add(decision_id)
            if (
                item.get("canonical_record_id") != prior.get("canonical_record_id")
                or item.get("survivor_occurrence_id") != prior.get("survivor_occurrence_id")
                or item.get("outcome") != prior.get("outcome")
                or item.get("match_key") != prior.get("match_key")
                or item.get("supersedes_ids") != [prior.get("decision_id")]
                or item.get("match_rule") != "adjudicated_prior_survey_identity_confirmation"
            ):
                raise PriorSurveyIdentityAdjudicationError(
                    f"planned decision does not exactly supersede current decision: {occurrence_id}"
                )
            decisions.append(dict(item))
    if set(source_by_id) != seen_groups or len(seen_occurrences) != EXPECTED_OCCURRENCES:
        raise PriorSurveyIdentityAdjudicationError("30-group/71-occurrence coverage changed")
    return decisions


def _validate_dry_run(
    *,
    root: Path,
    dry_run_package_dir: str | Path,
    expected_manifest_sha256: str,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    verify_dataset_hash: bool,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    package_dir = _within_root(root, dry_run_package_dir)
    manifest_path = package_dir / "package_manifest.json"
    if _sha256_file(manifest_path) != expected_manifest_sha256:
        raise PriorSurveyIdentityAdjudicationError("dry-run package manifest hash changed")
    manifest = json.loads(manifest_path.read_bytes())
    if (
        manifest.get("artifact_class") != DRY_RUN_ARTIFACT_CLASS
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("status") != DRY_RUN_STATUS
    ):
        raise PriorSurveyIdentityAdjudicationError("dry-run package contract changed")
    artifacts = manifest.get("artifacts", {})
    application_path, _ = _verify_package_artifact(
        package_dir, artifacts.get("application_dry_run", {})
    )
    _verify_package_artifact(package_dir, artifacts.get("report", {}))
    application = json.loads(application_path.read_bytes())
    if (
        application.get("status") != DRY_RUN_STATUS
        or application.get("application_supported_by_current_cli") is not False
        or manifest.get("counts")
        != {
            "identity_groups": 30,
            "source_occurrences": 71,
            "planned_superseding_history_entries": 71,
            "occurrence_membership_changes": 0,
            "canonical_membership_changes": 0,
            "related_version_link_changes": 0,
        }
        or manifest.get("invariant_totals") != EXPECTED_TOTALS
    ):
        raise PriorSurveyIdentityAdjudicationError("dry-run invariant contract changed")
    application_totals = application.get("invariant_totals", {})
    if (
        application_totals.get("occurrences_before") != EXPECTED_TOTALS["occurrences"]
        or application_totals.get("occurrences_after_dry_run") != EXPECTED_TOTALS["occurrences"]
        or application_totals.get("canonical_records_before")
        != EXPECTED_TOTALS["canonical_records"]
        or application_totals.get("canonical_records_after_dry_run")
        != EXPECTED_TOTALS["canonical_records"]
        or application_totals.get("related_version_links_before")
        != EXPECTED_TOTALS["related_version_links"]
        or application_totals.get("related_version_links_after_dry_run")
        != EXPECTED_TOTALS["related_version_links"]
        or application_totals.get("effective_duplicate_decisions_before")
        != EXPECTED_TOTALS["effective_duplicate_decisions"]
        or application_totals.get("effective_duplicate_decisions_after_dry_run")
        != EXPECTED_TOTALS["effective_duplicate_decisions"]
    ):
        raise PriorSurveyIdentityAdjudicationError("dry-run before/after totals changed")
    _assert_preserved_state(state)
    state_binding = manifest.get("bindings", {}).get("execution_state", {})
    if state_binding.get("raw_sha256") != execution_state_raw_sha256 or state_binding.get(
        "embedded_state_hash"
    ) != state.get("state_hash"):
        raise PriorSurveyIdentityAdjudicationError("dry-run execution-state binding is stale")

    global_merge = state["global_identification_merge"]
    dataset_binding = manifest.get("bindings", {}).get("registered_dataset", {})
    registered_dataset = global_merge.get("dataset", {})
    if any(
        dataset_binding.get(key) != registered_dataset.get(key)
        for key in ("path", "byte_size", "raw_sha256")
    ):
        raise PriorSurveyIdentityAdjudicationError("registered dataset binding changed")
    dataset_path = _verify_reference(root, dataset_binding, verify_hash=False)
    reconciliation_binding = manifest.get("bindings", {}).get("registered_reconciliation", {})
    if any(
        reconciliation_binding.get(key) != global_merge.get("reconciliation", {}).get(key)
        for key in ("path", "raw_sha256")
    ):
        raise PriorSurveyIdentityAdjudicationError("registered reconciliation binding changed")
    reconciliation_path = _verify_reference(root, reconciliation_binding)
    reconciliation = json.loads(reconciliation_path.read_bytes())
    if (
        reconciliation.get("input_occurrences") != EXPECTED_TOTALS["occurrences"]
        or reconciliation.get("output_occurrences") != EXPECTED_TOTALS["occurrences"]
        or reconciliation.get("canonical_counts", {}).get("after_approved_adjudication")
        != EXPECTED_TOTALS["canonical_records"]
        or reconciliation.get("duplicate_decisions", {}).get("effective_decisions")
        != EXPECTED_TOTALS["effective_duplicate_decisions"]
        or reconciliation.get("related_versions", {}).get("preserved_links")
        != EXPECTED_TOTALS["related_version_links"]
        or reconciliation.get("related_versions", {}).get("collapsed") is not False
        or reconciliation.get("remaining_identity_review", {}).get(
            "source_unresolved_identity_groups"
        )
        != EXPECTED_GROUPS
        or reconciliation.get("remaining_identity_review", {}).get("source_unresolved_occurrences")
        != EXPECTED_OCCURRENCES
        or reconciliation.get("identification_closed") is not False
        or reconciliation.get("screening_changed") is not False
        or reconciliation.get("prisma_changed") is not False
    ):
        raise PriorSurveyIdentityAdjudicationError("registered reconciliation invariants changed")

    proposal = manifest.get("bindings", {}).get("proposal_package", {})
    proposal_manifest = _within_root(
        root, Path(str(proposal.get("path") or "")) / "package_manifest.json"
    )
    proposal_reference = {
        "path": _relative_path(proposal_manifest, root),
        "path_scope": "repository_relative",
        "byte_size": proposal_manifest.stat().st_size,
        "raw_sha256": proposal.get("package_manifest_sha256"),
    }
    proposal_data = _verify_proposal_package(root, proposal_reference)
    application_source = application.get("source_package", {})
    application_bindings = application.get("bindings", {})
    if (
        application_source.get("path") != proposal.get("path")
        or application_source.get("package_manifest_sha256")
        != proposal.get("package_manifest_sha256")
        or any(
            application_bindings.get("registered_dataset", {}).get(key) != dataset_binding.get(key)
            for key in ("path", "byte_size", "raw_sha256")
        )
        or application_bindings.get("bibliographic_lookup_evidence", {}).get("raw_sha256")
        != manifest.get("bindings", {}).get("bibliographic_lookup_evidence", {}).get("raw_sha256")
        or application_bindings.get("original_review_package")
        != manifest.get("bindings", {}).get("original_review_package")
    ):
        raise PriorSurveyIdentityAdjudicationError("dry-run source bindings disagree")
    decisions = _validate_groups(application, proposal_data["review_groups"])
    if verify_dataset_hash and _sha256_file(dataset_path) != dataset_binding.get("raw_sha256"):
        raise PriorSurveyIdentityAdjudicationError("registered dataset hash changed")
    return manifest, application, decisions


def _implementation_bindings(root: Path) -> list[dict[str, Any]]:
    paths = (
        "src/h2h_lit/prior_survey_identity_adjudication.py",
        "src/h2h_lit/review.py",
        "src/h2h_lit/global_identification_merge.py",
        "src/h2h_lit/external_retrieval_wave.py",
    )
    return [
        _file_reference(root / path, root, role="identity-overlay-implementation") for path in paths
    ]


def _overlay_decision(
    item: Mapping[str, Any],
    *,
    created_at: str,
    proposal_manifest_sha256: str,
    supporting_evidence_sha256: str,
) -> dict[str, Any]:
    return {
        "decision_id": item["decision_id"],
        "occurrence_id": item["occurrence_id"],
        "canonical_record_id": item["canonical_record_id"],
        "survivor_occurrence_id": item["survivor_occurrence_id"],
        "outcome": item["outcome"],
        "match_key": item["match_key"],
        "match_rule": item["match_rule"],
        "provenance": {
            "actor": {
                "actor_id": "project-owner-confirmed-identity-proposals-v2",
                "actor_type": "adjudicator",
                "display_name": None,
                "metadata": {"activation_requires_explicit_authorization": True},
            },
            "authority": "adjudicated",
            "scope": "prospective",
            "protocol_version": SCHEMA_VERSION,
            "rubric_version": SCHEMA_VERSION,
            "created_at": created_at,
            "supersedes_ids": list(item["supersedes_ids"]),
            "source_artifact_id": proposal_manifest_sha256,
            "metadata": {
                "identity_confirmation_only": True,
                "supporting_evidence_sha256": supporting_evidence_sha256,
                "original_match_rule": item.get("original_match_rule"),
                "historical_unresolved_markers_retained": True,
            },
        },
    }


def prepare_identity_confirmation_package(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    dry_run_package_dir: str | Path,
    expected_dry_run_manifest_sha256: str,
    output_dir: str | Path,
    generated_at: str,
    verify_dataset_hash: bool = True,
) -> dict[str, Any]:
    """Create an authorization-ready overlay without changing production state."""

    root_path = Path(root).resolve()
    state_before = json.loads(json.dumps(state, sort_keys=True))
    dry_manifest, application, planned = _validate_dry_run(
        root=root_path,
        dry_run_package_dir=dry_run_package_dir,
        expected_manifest_sha256=expected_dry_run_manifest_sha256,
        state=state,
        execution_state_raw_sha256=execution_state_raw_sha256,
        verify_dataset_hash=verify_dataset_hash,
    )
    proposal_binding = dry_manifest["bindings"]["proposal_package"]
    evidence_hash = dry_manifest["bindings"]["bibliographic_lookup_evidence"]["raw_sha256"]
    decisions = [
        _overlay_decision(
            item,
            created_at=generated_at,
            proposal_manifest_sha256=proposal_binding["package_manifest_sha256"],
            supporting_evidence_sha256=evidence_hash,
        )
        for item in planned
    ]
    overlay = {
        "artifact_class": "PRIOR_SURVEY_IDENTITY_CONFIRMATION_OVERLAY",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "generated_at_utc": generated_at,
        "status": READY_STATUS,
        "authority": "PENDING_EXPLICIT_PRODUCTION_AUTHORIZATION",
        "source_dry_run": {
            "path": _relative_path(_within_root(root_path, dry_run_package_dir), root_path),
            "package_manifest_sha256": expected_dry_run_manifest_sha256,
        },
        "bindings": application["bindings"],
        "proposal_package": proposal_binding,
        "supporting_evidence_sha256": evidence_hash,
        "historical_status": {
            "unresolved_groups_retained": EXPECTED_GROUPS,
            "unresolved_occurrence_markers_retained": EXPECTED_OCCURRENCES,
        },
        "current_status_if_authorized": {
            "adjudicated_confirmed_groups": EXPECTED_GROUPS,
            "open_identity_groups": 0,
        },
        "groups": [
            {
                "group_id": group["group_id"],
                "canonical_record_id": group["canonical_id_before"],
                "survivor_occurrence_id": group["survivor_occurrence_id_before"],
                "occurrence_ids": group["occurrence_ids_before"],
                "historical_status": group["historical_status"],
                "current_status_if_authorized": "ADJUDICATED_CONFIRMED",
                "decision_ids": [
                    item["decision_id"] for item in group["planned_duplicate_decisions"]
                ],
            }
            for group in application["groups"]
        ],
        "superseding_duplicate_decisions": decisions,
        "counts": {
            "identity_groups": EXPECTED_GROUPS,
            "source_occurrences": EXPECTED_OCCURRENCES,
            "superseding_decision_entries": EXPECTED_OCCURRENCES,
        },
        "invariant_totals": EXPECTED_TOTALS,
        "preserved": {
            "registered_dataset_immutable": True,
            "occurrence_membership_changes": 0,
            "canonical_membership_changes": 0,
            "survivor_changes": 0,
            "survey_membership_changes": 0,
            "star_eligibility_changes": 0,
            "related_version_link_changes": 0,
            "historical_unresolved_markers_retained": True,
        },
    }
    output_path = _within_root(root_path, output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    overlay_ref = _write_json_once(output_path / "identity_confirmation_overlay.json", overlay)
    report = {
        "artifact_class": "PRIOR_SURVEY_IDENTITY_CONFIRMATION_VALIDATION_REPORT",
        "schema_version": SCHEMA_VERSION,
        "status": READY_STATUS,
        "generated_at_utc": generated_at,
        "validated": {
            "dry_run_manifest_sha256": expected_dry_run_manifest_sha256,
            "dataset_stream_hash_verified": verify_dataset_hash,
            "groups": EXPECTED_GROUPS,
            "occurrences": EXPECTED_OCCURRENCES,
            "superseding_decisions": EXPECTED_OCCURRENCES,
            "production_state_changed": False,
            "registered_dataset_changed": False,
        },
        "validation_boundary": (
            "Only overlay-aware consumers call the overlay validator; the generic "
            "external execution-state loader remains unchanged."
        ),
        "benchmark_staffing_correction": {
            "single_human": (
                "One blinded human reviews every evaluation record; no second coder "
                "or independent adjudicator is assumed. Retained uncertainty remains "
                "unresolved or is handled later under a separately authorized process."
            ),
            "optional_intra_reviewer_consistency": (
                "The same human may repeat a prospectively selected blinded subset "
                "after a frozen washout interval; report this separately as "
                "intra-reviewer consistency, not independent replication."
            ),
            "two_human": (
                "Two humans independently review each record, blinded to model outputs "
                "and each other; disagreement resolution requires a separately stated "
                "consensus or adjudication procedure."
            ),
        },
    }
    report_ref = _write_json_once(output_path / "validation_report.json", report)
    dry_manifest_path = _within_root(root_path, Path(dry_run_package_dir) / "package_manifest.json")
    manifest = {
        "artifact_class": "PRIOR_SURVEY_IDENTITY_CONFIRMATION_AUTHORIZATION_PACKAGE",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "generated_at_utc": generated_at,
        "status": READY_STATUS,
        "artifacts": {"overlay": overlay_ref, "validation_report": report_ref},
        "source_bindings": {
            "dry_run_package_manifest": _file_reference(dry_manifest_path, root_path),
            "execution_state_raw_sha256": execution_state_raw_sha256,
            "execution_state_embedded_hash": state.get("state_hash"),
            "registered_dataset": dry_manifest["bindings"]["registered_dataset"],
            "proposal_package": proposal_binding,
            "bibliographic_lookup_evidence": dry_manifest["bindings"][
                "bibliographic_lookup_evidence"
            ],
            "original_review_package": dry_manifest["bindings"]["original_review_package"],
            "registered_reconciliation": dry_manifest["bindings"]["registered_reconciliation"],
        },
        "implementation_bindings": _implementation_bindings(root_path),
        "counts": overlay["counts"],
        "invariant_totals": EXPECTED_TOTALS,
        "state_effects_if_authorized": {
            "registered_dataset_changed": False,
            "overlay_registration_added": True,
            "identification_closed": False,
            "screening_changed": False,
            "prisma_changed": False,
            "source_registrations_changed": False,
            "prior_survey_registrations_changed": False,
        },
    }
    manifest_ref = _write_json_once(output_path / "package_manifest.json", manifest)
    if state != state_before:
        raise PriorSurveyIdentityAdjudicationError("staging changed production state")
    return {
        "status": READY_STATUS,
        "package_dir": _relative_path(output_path, root_path),
        "package_manifest_sha256": manifest_ref["raw_sha256"],
        "overlay_sha256": overlay_ref["raw_sha256"],
        "counts": overlay["counts"],
    }


def validate_identity_confirmation_package(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    verify_dataset_hash: bool = True,
) -> ValidatedIdentityConfirmationPackage:
    root_path = Path(root).resolve()
    _assert_preserved_state(state)
    package_path = _within_root(root_path, package_dir)
    manifest_path = package_path / "package_manifest.json"
    manifest_hash = _sha256_file(manifest_path)
    if manifest_hash != expected_manifest_sha256:
        raise PriorSurveyIdentityAdjudicationError("overlay package manifest hash changed")
    manifest = json.loads(manifest_path.read_bytes())
    if (
        manifest.get("artifact_class") != "PRIOR_SURVEY_IDENTITY_CONFIRMATION_AUTHORIZATION_PACKAGE"
        or manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("implementation_version") != IMPLEMENTATION_VERSION
        or manifest.get("status") != READY_STATUS
        or manifest.get("counts")
        != {
            "identity_groups": EXPECTED_GROUPS,
            "source_occurrences": EXPECTED_OCCURRENCES,
            "superseding_decision_entries": EXPECTED_OCCURRENCES,
        }
        or manifest.get("invariant_totals") != EXPECTED_TOTALS
    ):
        raise PriorSurveyIdentityAdjudicationError("overlay package contract changed")
    bindings = manifest.get("source_bindings", {})
    if bindings.get("execution_state_raw_sha256") != execution_state_raw_sha256 or bindings.get(
        "execution_state_embedded_hash"
    ) != state.get("state_hash"):
        raise PriorSurveyIdentityAdjudicationError("overlay package input state is stale")
    for reference in manifest.get("implementation_bindings", []):
        _verify_reference(root_path, reference)
    dry_reference = bindings.get("dry_run_package_manifest", {})
    _verify_reference(root_path, dry_reference)
    dry_package_dir = _within_root(root_path, str(dry_reference["path"])).parent
    dry_manifest, _application, planned = _validate_dry_run(
        root=root_path,
        dry_run_package_dir=dry_package_dir,
        expected_manifest_sha256=str(dry_reference["raw_sha256"]),
        state=state,
        execution_state_raw_sha256=execution_state_raw_sha256,
        verify_dataset_hash=verify_dataset_hash,
    )
    overlay_path, overlay_hash = _verify_package_artifact(
        package_path, manifest.get("artifacts", {}).get("overlay", {})
    )
    _verify_package_artifact(
        package_path, manifest.get("artifacts", {}).get("validation_report", {})
    )
    overlay = json.loads(overlay_path.read_bytes())
    if (
        overlay.get("status") != READY_STATUS
        or overlay.get("counts") != manifest.get("counts")
        or overlay.get("invariant_totals") != EXPECTED_TOTALS
        or len(overlay.get("groups", [])) != EXPECTED_GROUPS
        or len(overlay.get("superseding_duplicate_decisions", [])) != EXPECTED_OCCURRENCES
        or overlay.get("preserved", {}).get("historical_unresolved_markers_retained") is not True
    ):
        raise PriorSurveyIdentityAdjudicationError("identity overlay contract changed")
    expected_by_id = {item["decision_id"]: item for item in planned}
    actual_decisions = overlay["superseding_duplicate_decisions"]
    if len({item.get("decision_id") for item in actual_decisions}) != EXPECTED_OCCURRENCES:
        raise PriorSurveyIdentityAdjudicationError("overlay decision IDs changed")
    for item in actual_decisions:
        expected = expected_by_id.get(item.get("decision_id"))
        if expected is None:
            raise PriorSurveyIdentityAdjudicationError("overlay decision set changed")
        for key in (
            "occurrence_id",
            "canonical_record_id",
            "survivor_occurrence_id",
            "outcome",
            "match_key",
            "match_rule",
        ):
            if item.get(key) != expected.get(key):
                raise PriorSurveyIdentityAdjudicationError("overlay decision target changed")
        provenance = item.get("provenance", {})
        if (
            provenance.get("authority") != "adjudicated"
            or provenance.get("scope") != "prospective"
            or provenance.get("supersedes_ids") != expected.get("supersedes_ids")
            or provenance.get("source_artifact_id")
            != dry_manifest["bindings"]["proposal_package"]["package_manifest_sha256"]
        ):
            raise PriorSurveyIdentityAdjudicationError("overlay provenance changed")
        DuplicateDecision.from_dict(item)
    return ValidatedIdentityConfirmationPackage(
        package_dir=package_path,
        manifest=manifest,
        manifest_sha256=manifest_hash,
        overlay=overlay,
        overlay_sha256=overlay_hash,
    )


def append_identity_confirmation_overlay(
    dataset: ReviewDataset, overlay: Mapping[str, Any]
) -> None:
    """Append a validated overlay to an already loaded dataset in a capable consumer."""

    before = {
        "occurrences": len(dataset.occurrences),
        "canonical": len(dataset.canonical_records),
        "effective": len(dataset.effective_duplicate_decisions()),
        "memberships": list(dataset.corpus_memberships),
    }
    current = {item.occurrence_id: item for item in dataset.effective_duplicate_decisions()}
    additions = [
        DuplicateDecision.from_dict(item)
        for item in overlay.get("superseding_duplicate_decisions", [])
    ]
    for item in additions:
        previous = current.get(item.occurrence_id)
        if previous is None or item.provenance.supersedes_ids != [previous.decision_id]:
            raise PriorSurveyIdentityAdjudicationError(
                "overlay does not supersede the current same-occurrence decision"
            )
        if (
            item.canonical_record_id != previous.canonical_record_id
            or item.survivor_occurrence_id != previous.survivor_occurrence_id
            or item.outcome != previous.outcome
            or item.match_key != previous.match_key
        ):
            raise PriorSurveyIdentityAdjudicationError(
                "overlay changes an existing canonical assignment"
            )
    dataset.duplicate_decisions.extend(additions)
    dataset.validate()
    after = {
        "occurrences": len(dataset.occurrences),
        "canonical": len(dataset.canonical_records),
        "effective": len(dataset.effective_duplicate_decisions()),
        "memberships": list(dataset.corpus_memberships),
    }
    if after != before:
        raise PriorSurveyIdentityAdjudicationError(
            "identity overlay changed dataset membership or effective counts"
        )


def validate_registered_identity_confirmation_overlay(
    *, root: str | Path, state: Mapping[str, Any], verify_dataset_hash: bool = False
) -> dict[str, Any] | None:
    """Validate the extension for an overlay-aware state consumer.

    This function is deliberately not called by the generic execution-state loader.
    Set ``verify_dataset_hash`` when a fresh streamed verification of the large immutable
    dataset is required; reference and size checks are always performed.
    """

    registration = state.get(STATE_KEY)
    if registration is None:
        return None
    if not isinstance(registration, Mapping) or registration.get("status") != REGISTERED_STATUS:
        raise PriorSurveyIdentityAdjudicationError("registered identity overlay status changed")
    _assert_preserved_state(state)
    root_path = Path(root).resolve()
    package_path = _verify_reference(root_path, registration.get("package_manifest", {}))
    overlay_path = _verify_reference(root_path, registration.get("overlay", {}))
    authorization_path = _verify_reference(root_path, registration.get("authorization", {}))
    dataset_reference = registration.get("registered_dataset", {})
    dataset_path = _verify_reference(root_path, dataset_reference, verify_hash=verify_dataset_hash)
    global_dataset = state["global_identification_merge"]["dataset"]
    if any(
        dataset_reference.get(key) != global_dataset.get(key)
        for key in ("path", "byte_size", "raw_sha256")
    ):
        raise PriorSurveyIdentityAdjudicationError("overlay dataset binding changed")
    authorization = json.loads(authorization_path.read_bytes())
    if (
        authorization.get("status") != REGISTERED_STATUS
        or authorization.get("package_manifest", {}).get("raw_sha256")
        != registration.get("package_manifest", {}).get("raw_sha256")
        or authorization.get("overlay", {}).get("raw_sha256")
        != registration.get("overlay", {}).get("raw_sha256")
        or authorization.get("registered_dataset", {}).get("raw_sha256")
        != dataset_reference.get("raw_sha256")
        or authorization.get("counts")
        != {
            "identity_groups": EXPECTED_GROUPS,
            "source_occurrences": EXPECTED_OCCURRENCES,
            "superseding_decision_entries": EXPECTED_OCCURRENCES,
        }
    ):
        raise PriorSurveyIdentityAdjudicationError("overlay authorization changed")
    overlay = json.loads(overlay_path.read_bytes())
    if (
        len(overlay.get("groups", [])) != EXPECTED_GROUPS
        or len(overlay.get("superseding_duplicate_decisions", [])) != EXPECTED_OCCURRENCES
    ):
        raise PriorSurveyIdentityAdjudicationError("registered overlay coverage changed")
    return {
        "status": REGISTERED_STATUS,
        "package_manifest_path": _relative_path(package_path, root_path),
        "overlay_path": _relative_path(overlay_path, root_path),
        "authorization_path": _relative_path(authorization_path, root_path),
        "dataset_path": _relative_path(dataset_path, root_path),
        "dataset_hash_streamed": verify_dataset_hash,
        "historical_unresolved_groups": EXPECTED_GROUPS,
        "historical_unresolved_occurrence_markers": EXPECTED_OCCURRENCES,
        "current_adjudicated_confirmed_groups": EXPECTED_GROUPS,
        "current_open_identity_groups": 0,
    }


def authorize_identity_confirmation_overlay(
    *,
    root: str | Path,
    state: dict[str, Any],
    execution_state_raw_sha256: str,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    authorized_at: str,
    verify_dataset_hash: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Register an overlay; the caller must hold the shared production lock."""

    root_path = Path(root).resolve()
    existing = state.get(STATE_KEY)
    if existing is not None:
        validate_registered_identity_confirmation_overlay(
            root=root_path, state=state, verify_dataset_hash=verify_dataset_hash
        )
        requested_manifest = _within_root(root_path, package_dir) / "package_manifest.json"
        registered_manifest = _verify_reference(root_path, existing.get("package_manifest", {}))
        if (
            existing.get("package_manifest", {}).get("raw_sha256") != expected_manifest_sha256
            or registered_manifest != requested_manifest.resolve()
        ):
            raise PriorSurveyIdentityAdjudicationError(
                "repeated overlay authorization binding changed"
            )
        return state, dict(existing)
    sources_before = json.loads(json.dumps(state.get("sources"), sort_keys=True))
    prior_before = json.loads(json.dumps(state.get("prior_survey_imports"), sort_keys=True))
    global_before = json.loads(json.dumps(state.get("global_identification_merge"), sort_keys=True))
    validated = validate_identity_confirmation_package(
        root=root_path,
        state=state,
        execution_state_raw_sha256=execution_state_raw_sha256,
        package_dir=package_dir,
        expected_manifest_sha256=expected_manifest_sha256,
        verify_dataset_hash=verify_dataset_hash,
    )
    output = root_path / PRODUCTION_RELATIVE_ROOT
    output.mkdir(parents=True, exist_ok=True)
    source_overlay = validated.package_dir / validated.manifest["artifacts"]["overlay"]["path"]
    overlay_reference = _copy_once(
        source_overlay,
        output / "identity_confirmation_overlay.json",
        validated.manifest["artifacts"]["overlay"],
        root_path,
    )
    package_reference = _file_reference(validated.package_dir / "package_manifest.json", root_path)
    registered_dataset = dict(state["global_identification_merge"]["dataset"])
    authorization = {
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": REGISTERED_STATUS,
        "authorized_at_utc": authorized_at,
        "package_manifest": package_reference,
        "overlay": overlay_reference,
        "registered_dataset": registered_dataset,
        "counts": validated.manifest["counts"],
        "historical_status": {
            "unresolved_groups_retained": EXPECTED_GROUPS,
            "unresolved_occurrence_markers_retained": EXPECTED_OCCURRENCES,
        },
        "current_status": {
            "adjudicated_confirmed_groups": EXPECTED_GROUPS,
            "open_identity_groups": 0,
        },
        "identification_closed": False,
        "screening_changed": False,
        "prisma_changed": False,
        "registered_dataset_changed": False,
    }
    authorization_path = output / "authorization.json"
    authorization_reference = _write_json_once(authorization_path, authorization)
    authorization_reference = {
        **authorization_reference,
        "path": _relative_path(authorization_path, root_path),
        "path_scope": "repository_relative",
    }
    registration = {**authorization, "authorization": authorization_reference}
    state[STATE_KEY] = registration
    if (
        state.get("sources") != sources_before
        or state.get("prior_survey_imports") != prior_before
        or state.get("global_identification_merge") != global_before
    ):
        raise PriorSurveyIdentityAdjudicationError(
            "overlay registration changed an existing source authorization"
        )
    _assert_preserved_state(state)
    validate_registered_identity_confirmation_overlay(
        root=root_path, state=state, verify_dataset_hash=False
    )
    return state, registration


def _load_production_state(root: Path) -> tuple[Path, bytes, dict[str, Any]]:
    """Load the post-global-merge state through its two authorization boundaries.

    Prior-survey packages were authorized before final global deduplication and bind that
    value as false.  The global registration legitimately changes it to true.  The generic
    loader currently replays the prior-survey fingerprint against the post-merge value, so
    this overlay-specific reader validates the immutable raw state first, validates those
    inputs at their pre-global boundary, and validates the global registration separately.
    """

    from h2h_lit.arxiv_snapshot_integration import (
        validate_authorized_snapshot_substitution,
    )
    from h2h_lit.external_retrieval_wave import (
        EXECUTION_STATE_PATH,
        PREFLIGHT_PATH,
        WAVE_PATH,
        _safe_output_path,
        _validate_embedded_hash,
        validate_persisted_external_preflight,
    )
    from h2h_lit.global_identification_merge import validate_registered_global_merge
    from h2h_lit.prior_survey_integration import validate_authorized_prior_survey_imports

    wave, _preflight = validate_persisted_external_preflight(root=root)
    state_path = _safe_output_path(root, EXECUTION_STATE_PATH)
    raw = state_path.read_bytes()
    state = json.loads(raw)
    _validate_embedded_hash(state, "state_hash")
    if (
        state.get("wave_manifest_hash") != wave.manifest_hash()
        or state.get("planned_wave_raw_sha256") != _sha256_file(_safe_output_path(root, WAVE_PATH))
        or state.get("preflight_raw_sha256")
        != _sha256_file(_safe_output_path(root, PREFLIGHT_PATH))
    ):
        raise PriorSurveyIdentityAdjudicationError("execution checkpoint frozen-wave hash mismatch")
    validate_authorized_snapshot_substitution(root=root, state=state)
    pre_global_state = json.loads(json.dumps(state, sort_keys=True))
    pre_global_state["final_global_deduplication_executed"] = False
    validate_authorized_prior_survey_imports(root=root, state=pre_global_state)
    validate_registered_global_merge(root=root, state=state)
    return state_path, raw, state


def prepare_identity_confirmation_package_locked(
    *,
    root: str | Path,
    dry_run_package_dir: str | Path,
    expected_dry_run_manifest_sha256: str,
    output_dir: str | Path,
    generated_at: str | None = None,
) -> dict[str, Any]:
    from h2h_lit.external_retrieval_wave import _exclusive_external_source_session

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        state_path, raw, state = _load_production_state(root_path)
        result = prepare_identity_confirmation_package(
            root=root_path,
            state=state,
            execution_state_raw_sha256=hashlib.sha256(raw).hexdigest(),
            dry_run_package_dir=dry_run_package_dir,
            expected_dry_run_manifest_sha256=expected_dry_run_manifest_sha256,
            output_dir=output_dir,
            generated_at=generated_at or _utc_now(),
        )
        if state_path.read_bytes() != raw:
            raise PriorSurveyIdentityAdjudicationError(
                "identity-overlay staging changed production state"
            )
        return result


def authorize_identity_confirmation_overlay_locked(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_manifest_sha256: str,
    authorized_at: str | None = None,
) -> dict[str, Any]:
    from h2h_lit.external_retrieval_wave import (
        _exclusive_external_source_session,
        _save_execution_state,
    )

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        state_path, raw, state = _load_production_state(root_path)
        state, registration = authorize_identity_confirmation_overlay(
            root=root_path,
            state=state,
            execution_state_raw_sha256=hashlib.sha256(raw).hexdigest(),
            package_dir=package_dir,
            expected_manifest_sha256=expected_manifest_sha256,
            authorized_at=authorized_at or _utc_now(),
        )
        _save_execution_state(state_path, state)
        _state_path, _new_raw, reloaded = _load_production_state(root_path)
        validate_registered_identity_confirmation_overlay(
            root=root_path, state=reloaded, verify_dataset_hash=False
        )
        return registration


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("."))
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--prepare-identity-confirmation-overlay", action="store_true")
    modes.add_argument("--authorize-identity-confirmation-overlay", action="store_true")
    parser.add_argument("--dry-run-package-dir", type=Path)
    parser.add_argument("--dry-run-package-manifest-sha256")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--package-dir", type=Path)
    parser.add_argument("--package-manifest-sha256")
    args = parser.parse_args(argv)
    if args.prepare_identity_confirmation_overlay:
        if (
            args.dry_run_package_dir is None
            or not args.dry_run_package_manifest_sha256
            or args.output_dir is None
        ):
            parser.error(
                "preparation requires --dry-run-package-dir, "
                "--dry-run-package-manifest-sha256, and --output-dir"
            )
        result = prepare_identity_confirmation_package_locked(
            root=args.root,
            dry_run_package_dir=args.dry_run_package_dir,
            expected_dry_run_manifest_sha256=args.dry_run_package_manifest_sha256,
            output_dir=args.output_dir,
        )
    else:
        if args.package_dir is None or not args.package_manifest_sha256:
            parser.error("authorization requires --package-dir and --package-manifest-sha256")
        result = authorize_identity_confirmation_overlay_locked(
            root=args.root,
            package_dir=args.package_dir,
            expected_manifest_sha256=args.package_manifest_sha256,
        )
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
