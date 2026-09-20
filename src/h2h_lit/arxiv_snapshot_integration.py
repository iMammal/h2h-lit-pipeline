"""Provenance-bound integration of the provisional arXiv snapshot search.

The generic DOI-first/title-fallback canonicalizer remains authoritative.  This
module adds an explicit, package-bound adjudication layer after that ordinary
merge and records related-version proposals without collapsing those records.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.artifact_import import merge_identification_datasets
from h2h_lit.checkpoint import atomic_write
from h2h_lit.dedupe import record_key
from h2h_lit.retrieval import load_review_dataset, save_review_dataset
from h2h_lit.review import (
    ActorType,
    CanonicalRecord,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    DedupeOutcome,
    DuplicateDecision,
    ReviewDataset,
)

PACKAGE_MANIFEST_SHA256 = (
    "be9c1fc4cb71a3bad959143e90b6ae1fa490239e389a2818110d713550c7edd7"
)
PACKAGE_ID = "arxiv-snapshot-v303-integration-preparation-v1"
SOURCE_DATASET_SHA256 = (
    "03eeb74453679e66ed6ac1b4e6fa47ab5a8ff128de16d4a54592035c0e964348"
)
SOURCE_ARCHIVE_SHA256 = (
    "c567b1caaf52686299c7b92ddf96eb93492808dc4d15031b8bd3292d772179a5"
)
QUERY_PLAN_SHA256 = (
    "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
)
MATCHING_POLICY_SHA256 = (
    "c429952095f2ae70fc5436090aa64ce6788b048d27b2e372f6c663470e19fa92"
)
SOURCE_STAGING_MANIFEST_SHA256 = (
    "d0cdd0c88e6adbf15f1055c57703d8491aaedf6adc2b6288c465f8caf268def3"
)
PARENT_EXECUTION_STATE_SHA256 = (
    "b61ddc1242a4decbb85562d697dbc6a02fe388b068c96136debf9571183014f3"
)
PARENT_EXECUTION_STATE_HASH = (
    "968cbb5abec3df73a470dde0c815edc08ae2fa13b01fb8a647de9ed0ac9e7fd5"
)
EPISODE_6_CHECKPOINT_SHA256 = (
    "7791d85c94cdfb0550c8583a2cb4ab4c0dd3ee0a86bcd4ea424f803bf864c3c4"
)
EXPECTED_COUNTS = {
    "snapshot_records": 958,
    "query_memberships": 1333,
    "arxiv_id_only_links": 424,
    "ambiguous_candidates": 45,
    "ambiguous_identity_pairs": 50,
}
EXPECTED_QUERY_MEMBERSHIPS = {
    "production:STAR-QF01-RELATIONAL-VIS:arXiv": 271,
    "production:STAR-QF02-ASSISTED-VIS:arXiv": 405,
    "production:STAR-QF03-INTERACTIVE-SYSTEMS:arXiv": 154,
    "production:STAR-QF04-NONDESKTOP-ENV:arXiv": 92,
    "production:STAR-QF05-CONVERSATIONAL:arXiv": 411,
}
EXPECTED_EPISODE_ATTEMPTS = [15, 10, 7, 19, 6, 3]
EXPECTED_EPISODE_RESPONSES = [7, 1, 1, 19, 6, 3]
INTEGRATION_OUTPUT = (
    "outputs/production/star-external-retrieval-wave-001/execution/"
    "arXivSnapshotV303"
)
INTEGRATION_MARKER = "arxiv_snapshot_v303_integration"
DIRECT_IDENTITY_MATCH_RULE = "adjudicated_exact_normalized_arxiv_id"
PROPAGATED_IDENTITY_MATCH_RULE = "propagated_adjudicated_generic_title_group"


class ArxivSnapshotIntegrationError(RuntimeError):
    """Raised when a package, lineage, identity, or state binding changes."""


@dataclass(slots=True)
class ValidatedSnapshotPackage:
    package_dir: Path
    package_manifest: dict[str, Any]
    package_manifest_sha256: str
    import_manifest: dict[str, Any]
    amendment_v1: dict[str, Any]
    snapshot_dataset: ReviewDataset
    identity_proposals: list[dict[str, Any]]
    relationship_proposals: list[dict[str, Any]]


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return _sha256_bytes(encoded.encode("utf-8"))


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ArxivSnapshotIntegrationError(f"expected JSON object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ArxivSnapshotIntegrationError(
                    f"expected JSON object at {path}:{line_number}"
                )
            values.append(value)
    return values


def _contained_file(base: Path, relative: str) -> Path:
    path = (base / relative).resolve()
    try:
        path.relative_to(base.resolve())
    except ValueError as exc:
        raise ArxivSnapshotIntegrationError(
            f"artifact path escapes package: {relative}"
        ) from exc
    if not path.is_file():
        raise ArxivSnapshotIntegrationError(f"required artifact missing: {relative}")
    return path


def _resolve_repository_input(root: Path, value: str | Path) -> Path:
    path = Path(value)
    path = (root / path).resolve() if not path.is_absolute() else path.resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ArxivSnapshotIntegrationError(
            "snapshot integration input must remain inside the repository"
        ) from exc
    return path


def _validate_reference(base: Path, reference: Mapping[str, Any]) -> Path:
    relative = reference.get("path")
    if not isinstance(relative, str) or not relative:
        raise ArxivSnapshotIntegrationError("artifact reference path is missing")
    path = _contained_file(base, relative)
    size = path.stat().st_size
    digest = _sha256_file(path)
    if size != reference.get("byte_size") or digest != reference.get("raw_sha256"):
        raise ArxivSnapshotIntegrationError(
            f"artifact size/hash changed: {relative}"
        )
    return path


def _validate_package_manifest(
    package_dir: Path, *, expected_manifest_sha256: str
) -> dict[str, Any]:
    manifest_path = package_dir / "package_manifest.json"
    if not manifest_path.is_file():
        raise ArxivSnapshotIntegrationError("snapshot package manifest is missing")
    manifest_sha256 = _sha256_file(manifest_path)
    if manifest_sha256 != expected_manifest_sha256:
        raise ArxivSnapshotIntegrationError("snapshot package manifest hash changed")
    manifest = _read_json(manifest_path)
    if (
        manifest.get("package_id") != PACKAGE_ID
        or manifest.get("status") != "OFFLINE_PREPARATION_COMPLETE_NOT_APPLIED"
        or manifest.get("counts") != EXPECTED_COUNTS
        or manifest.get("production_eligible") is not False
        or manifest.get("prisma_counted") is not False
        or manifest.get("production_state_modified") is not False
    ):
        raise ArxivSnapshotIntegrationError("snapshot package contract changed")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ArxivSnapshotIntegrationError("snapshot package artifact map is missing")
    for name, reference in artifacts.items():
        if not isinstance(reference, dict) or reference.get("path") != name:
            raise ArxivSnapshotIntegrationError("snapshot artifact manifest changed")
        _validate_reference(package_dir, reference)
    return manifest


def validate_prepared_package(
    package_dir: str | Path,
    *,
    expected_manifest_sha256: str = PACKAGE_MANIFEST_SHA256,
) -> ValidatedSnapshotPackage:
    """Validate every package binding and its exact identity proposals."""

    package_path = Path(package_dir).resolve()
    manifest = _validate_package_manifest(
        package_path, expected_manifest_sha256=expected_manifest_sha256
    )
    import_manifest = _read_json(package_path / "import_package/import_package_manifest.json")
    if (
        import_manifest.get("package_id")
        != "arxiv-snapshot-v303-provisional-import-v1"
        or import_manifest.get("status") != "VALIDATED_OFFLINE_NOT_APPLIED"
        or import_manifest.get("snapshot_sha256") != SOURCE_DATASET_SHA256
        or import_manifest.get("query_plan_sha256") != QUERY_PLAN_SHA256
        or import_manifest.get("matching_policy_sha256")
        != MATCHING_POLICY_SHA256
        or import_manifest.get("source_record_count")
        != EXPECTED_COUNTS["snapshot_records"]
        or import_manifest.get("query_membership_occurrence_count")
        != EXPECTED_COUNTS["query_memberships"]
        or import_manifest.get("snapshot_internal_canonical_count")
        != EXPECTED_COUNTS["snapshot_records"]
    ):
        raise ArxivSnapshotIntegrationError("snapshot import manifest changed")
    bindings = import_manifest.get("bindings", {})
    if (
        bindings.get("execution_state_hash") != PARENT_EXECUTION_STATE_HASH
        or bindings.get("execution_state_sha256") != PARENT_EXECUTION_STATE_SHA256
        or bindings.get("source_staging_manifest_sha256")
        != SOURCE_STAGING_MANIFEST_SHA256
    ):
        raise ArxivSnapshotIntegrationError("snapshot import state binding changed")

    query_packages = import_manifest.get("query_packages")
    if not isinstance(query_packages, list) or len(query_packages) != 5:
        raise ArxivSnapshotIntegrationError("snapshot query package inventory changed")
    observed_memberships = {
        str(item.get("production_query_id")): item.get("membership_count")
        for item in query_packages
    }
    if observed_memberships != EXPECTED_QUERY_MEMBERSHIPS:
        raise ArxivSnapshotIntegrationError("snapshot query identities/counts changed")

    amendment = _read_json(package_path / "retrieval_method_amendment_v1.json")
    if (
        amendment.get("amendment_id")
        != "arxiv-snapshot-v303-retrieval-method-amendment-v1"
        or amendment.get("status") != "PROPOSED_NOT_APPLIED"
        or amendment.get("snapshot", {}).get("version") != 303
        or amendment.get("snapshot", {}).get("jsonl", {}).get("raw_sha256")
        != SOURCE_DATASET_SHA256
        or amendment.get("snapshot", {}).get("archive", {}).get("raw_sha256")
        != SOURCE_ARCHIVE_SHA256
        or amendment.get("matching_policy_sha256") != MATCHING_POLICY_SHA256
        or amendment.get("frozen_scientific_queries", {}).get("plan_hash")
        != QUERY_PLAN_SHA256
        or amendment.get("qualifications", {}).get("api_equivalence")
        != "NOT_CLAIMED"
        or amendment.get("qualifications", {}).get("provider_completeness")
        != "UNPROVEN"
    ):
        raise ArxivSnapshotIntegrationError("retrieval-method amendment changed")
    frozen_queries = amendment["frozen_scientific_queries"].get("queries")
    if not isinstance(frozen_queries, list) or len(frozen_queries) != 5:
        raise ArxivSnapshotIntegrationError("frozen scientific query inventory changed")
    if {
        item.get("query_id"): item.get("query_text_sha256") for item in frozen_queries
    } != {
        item.get("production_query_id"): item.get("query_text_sha256")
        for item in query_packages
    }:
        raise ArxivSnapshotIntegrationError("frozen query hashes changed")

    dataset_path = _validate_reference(
        package_path, import_manifest["merged_snapshot_dataset"]
    )
    dataset = load_review_dataset(dataset_path)
    dataset.validate()
    if (
        len(dataset.occurrences) != EXPECTED_COUNTS["query_memberships"]
        or len(dataset.canonical_records) != EXPECTED_COUNTS["snapshot_records"]
        or len(dataset.source_queries) != 5
        or {item.record.arxiv_id for item in dataset.occurrences}
        != {item.source_identifier for item in dataset.occurrences}
        or len({item.source_identifier for item in dataset.occurrences})
        != EXPECTED_COUNTS["snapshot_records"]
    ):
        raise ArxivSnapshotIntegrationError("snapshot review dataset counts changed")
    query_counts: dict[str, int] = {}
    query_by_id = {item.query_id: item for item in dataset.source_queries}
    for occurrence in dataset.occurrences:
        production_query_id = occurrence.metadata.get("frozen_production_query_id")
        if not isinstance(production_query_id, str):
            raise ArxivSnapshotIntegrationError(
                "snapshot occurrence query identity changed"
            )
        query_counts[production_query_id] = query_counts.get(production_query_id, 0) + 1
        if (
            occurrence.metadata.get("snapshot_version") != 303
            or occurrence.metadata.get("matching_policy_sha256")
            != MATCHING_POLICY_SHA256
            or occurrence.metadata.get("production_eligible") is not False
            or occurrence.metadata.get("prisma_counted") is not False
            or not occurrence.metadata.get("snapshot_raw_line_sha256")
            or not occurrence.metadata.get("snapshot_line_number")
        ):
            raise ArxivSnapshotIntegrationError(
                "snapshot occurrence provenance changed"
            )
    if query_counts != EXPECTED_QUERY_MEMBERSHIPS:
        raise ArxivSnapshotIntegrationError("snapshot occurrence memberships changed")
    for query_package in query_packages:
        query = query_by_id.get(query_package.get("snapshot_query_id"))
        if (
            query is None
            or query.query_text != next(
                item["query_text"]
                for item in frozen_queries
                if item["query_id"] == query_package["production_query_id"]
            )
            or query.metadata.get("frozen_query_text_sha256")
            != query_package.get("query_text_sha256")
        ):
            raise ArxivSnapshotIntegrationError("snapshot query provenance changed")

    identities = _read_jsonl(package_path / "arxiv_id_identity_link_proposals.jsonl")
    if len(identities) != EXPECTED_COUNTS["arxiv_id_only_links"]:
        raise ArxivSnapshotIntegrationError("arXiv-ID adjudication count changed")
    candidate_ids: set[str] = set()
    normalized_ids: set[str] = set()
    proposal_ids: set[str] = set()
    for proposal in identities:
        candidate = proposal.get("candidate_identity", {})
        normalized = proposal.get("normalized_arxiv_id")
        proposal_id = proposal.get("proposal_id")
        if (
            proposal.get("proposed_disposition") != "same_bibliographic_record"
            or proposal.get("decision_status") != "PROPOSED_NOT_APPLIED"
            or proposal.get("conflicts") != []
            or proposal.get("generic_dedupe_rule_changed") is not False
            or not isinstance(normalized, str)
            or not normalized
            or not isinstance(proposal_id, str)
            or not proposal_id
            or _normalize_arxiv_id(candidate.get("arxiv_id")) != normalized
            or _normalize_arxiv_id(
                proposal.get("existing_identity", {}).get("arxiv_id")
            )
            != normalized
        ):
            raise ArxivSnapshotIntegrationError("arXiv-ID proposal changed")
        normalized_id = normalized
        candidate_ids.add(str(candidate.get("staging_candidate_id")))
        normalized_ids.add(normalized_id)
        proposal_ids.add(proposal_id)
    if not (
        len(candidate_ids) == len(normalized_ids) == len(proposal_ids) == len(identities)
    ):
        raise ArxivSnapshotIntegrationError("arXiv-ID proposals are not one-to-one")
    if normalized_ids - {str(item.source_identifier) for item in dataset.occurrences}:
        raise ArxivSnapshotIntegrationError("arXiv-ID proposal lacks snapshot record")

    relationships = _read_jsonl(package_path / "ambiguous_adjudication_proposals.jsonl")
    relationship_candidates = {
        item.get("candidate_identity", {}).get("staging_candidate_id")
        for item in relationships
    }
    if (
        len(relationships) != EXPECTED_COUNTS["ambiguous_identity_pairs"]
        or len(relationship_candidates) != EXPECTED_COUNTS["ambiguous_candidates"]
        or any(
            item.get("proposed_disposition")
            != "related preprint/published version"
            or item.get("decision_status") != "PROPOSED_NOT_APPLIED"
            or item.get("identity_scope") != "bibliographic_record_only"
            or item.get("study_grouping_decided") is not False
            or item.get("screening_or_eligibility_decided") is not False
            for item in relationships
        )
    ):
        raise ArxivSnapshotIntegrationError("related-version proposals changed")

    return ValidatedSnapshotPackage(
        package_dir=package_path,
        package_manifest=manifest,
        package_manifest_sha256=expected_manifest_sha256,
        import_manifest=import_manifest,
        amendment_v1=amendment,
        snapshot_dataset=dataset,
        identity_proposals=identities,
        relationship_proposals=relationships,
    )


def _normalize_arxiv_id(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    normalized = re.sub(r"^https?://arxiv\.org/(?:abs|pdf)/", "", normalized)
    normalized = normalized.removesuffix(".pdf")
    normalized = re.sub(r"v\d+$", "", normalized)
    return normalized or None


def _validate_state_hash(state: Mapping[str, Any]) -> None:
    material = dict(state)
    claimed = material.pop("state_hash", None)
    if claimed != _json_hash(material):
        raise ArxivSnapshotIntegrationError("execution state embedded hash changed")


def derive_api_lineage(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    expected_state_sha256: str = PARENT_EXECUTION_STATE_SHA256,
) -> dict[str, Any]:
    """Derive episode-scoped request/response totals exactly once per episode."""

    root_path = Path(root).resolve()
    state_path = root_path / (
        "outputs/production/star-external-retrieval-wave-001/execution/"
        "execution_state.json"
    )
    if not state_path.is_file() or _sha256_file(state_path) != expected_state_sha256:
        raise ArxivSnapshotIntegrationError("active execution-state binding changed")
    persisted = _read_json(state_path)
    if persisted != state:
        raise ArxivSnapshotIntegrationError("supplied and persisted execution state differ")
    _validate_state_hash(state)
    source = state.get("sources", {}).get("arXiv", {})
    episodes = source.get("execution_episodes")
    if (
        source.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or source.get("active_episode_number") != 6
        or source.get("occurrence_count") != 0
        or source.get("completed_query_count") != 0
        or not isinstance(episodes, list)
        or [item.get("episode_number") for item in episodes] != list(range(1, 7))
    ):
        raise ArxivSnapshotIntegrationError("arXiv API state drifted")

    episode_rows: list[dict[str, Any]] = []
    for episode, expected_attempts, expected_responses in zip(
        episodes,
        EXPECTED_EPISODE_ATTEMPTS,
        EXPECTED_EPISODE_RESPONSES,
        strict=True,
    ):
        reference = episode.get("checkpoint_dataset", {})
        checkpoint = (root_path / str(reference.get("path", ""))).resolve()
        try:
            checkpoint.relative_to(root_path)
        except ValueError as exc:
            raise ArxivSnapshotIntegrationError(
                "episode checkpoint escapes repository"
            ) from exc
        if (
            not checkpoint.is_file()
            or checkpoint.stat().st_size != reference.get("byte_size")
            or _sha256_file(checkpoint) != reference.get("raw_sha256")
        ):
            raise ArxivSnapshotIntegrationError("episode checkpoint binding changed")
        dataset = load_review_dataset(checkpoint)
        dataset.validate()
        response_attempts = [
            item
            for item in dataset.retrieval_attempts
            if item.raw_response_path and item.raw_response_hash
        ]
        if (
            len(dataset.retrieval_attempts) != expected_attempts
            or len(response_attempts) != expected_responses
            or dataset.occurrences
        ):
            raise ArxivSnapshotIntegrationError("episode evidence counts changed")
        response_hashes: list[str] = []
        for attempt in response_attempts:
            response_relative = attempt.raw_response_path
            response_hash = attempt.raw_response_hash
            if not response_relative or not response_hash:
                raise ArxivSnapshotIntegrationError(
                    "raw response reference is incomplete"
                )
            response_path = (checkpoint.parent / response_relative).resolve()
            try:
                response_path.relative_to(checkpoint.parent.resolve())
            except ValueError as exc:
                raise ArxivSnapshotIntegrationError(
                    "raw response path escapes checkpoint"
                ) from exc
            if (
                not response_path.is_file()
                or _sha256_file(response_path) != response_hash
            ):
                raise ArxivSnapshotIntegrationError("raw response evidence changed")
            response_hashes.append(response_hash)
        episode_rows.append(
            {
                "episode_number": episode["episode_number"],
                "status": episode["status"],
                "attempt_events": len(dataset.retrieval_attempts),
                "saved_responses": len(response_attempts),
                "checkpoint": dict(reference),
                "raw_response_hashes": response_hashes,
            }
        )

    latest = episodes[-1]
    latest_dataset = load_review_dataset(
        root_path / latest["checkpoint_dataset"]["path"]
    )
    if (
        latest["checkpoint_dataset"].get("raw_sha256")
        != EPISODE_6_CHECKPOINT_SHA256
        or [item.response_status for item in latest_dataset.retrieval_attempts]
        != [500, 500, 500]
        or source.get("attempt_count") != 3
        or source.get("preserved_source_attempt_count") != 57
        or source.get("preserved_source_raw_response_count") != 34
    ):
        raise ArxivSnapshotIntegrationError("episode-6 lineage changed")
    lineage = latest.get("recovery_provenance", {}).get(
        "historical_lineage_counts", {}
    )
    if (
        lineage.get("preserved_total_attempts") != 57
        or lineage.get("preserved_total_raw_responses") != 34
    ):
        raise ArxivSnapshotIntegrationError("episode-6 parent lineage changed")
    return {
        "accounting_unit": "persisted attempt events and saved responses per episode",
        "episode_scoped_attempt_ids_may_repeat": True,
        "episodes_counted_once": episode_rows,
        "attempt_events": sum(item["attempt_events"] for item in episode_rows),
        "saved_responses": sum(item["saved_responses"] for item in episode_rows),
        "accepted_api_occurrences": 0,
        "episode_6_http_statuses": [500, 500, 500],
    }


def build_amendment_v2(
    package: ValidatedSnapshotPackage,
    *,
    lineage: Mapping[str, Any],
) -> dict[str, Any]:
    amendment = json.loads(json.dumps(package.amendment_v1))
    amendment["schema_version"] = "1.1.0"
    amendment["amendment_id"] = "arxiv-snapshot-v303-retrieval-method-amendment-v2"
    amendment["supersedes_amendment_id"] = package.amendment_v1["amendment_id"]
    amendment["correction"] = {
        "reason": (
            "Version 1 reported the preserved pre-episode-6 lineage rather than "
            "the cumulative lineage after episode 6."
        ),
        "amendment_v1_sha256": package.package_manifest.get("artifacts", {})
        .get("retrieval_method_amendment_v1.json", {})
        .get("raw_sha256"),
        "package_manifest_sha256": package.package_manifest_sha256,
        "api_lineage_accounting": dict(lineage),
    }
    replacement = amendment["replacement"]
    replacement["preserved_api_attempt_count"] = lineage["attempt_events"]
    replacement["preserved_api_raw_response_count"] = lineage["saved_responses"]
    if (
        replacement["preserved_api_attempt_count"] != 60
        or replacement["preserved_api_raw_response_count"] != 37
    ):
        raise ArxivSnapshotIntegrationError("corrected API lineage totals changed")
    return amendment


def _amendment_markdown(amendment: Mapping[str, Any]) -> str:
    lineage = amendment["correction"]["api_lineage_accounting"]
    return (
        "# arXiv retrieval-method amendment v2\n\n"
        "This amendment supersedes the lineage-accounting fields in v1; all "
        "scientific queries, snapshot bindings, matching-policy qualifications, "
        "and method limitations are unchanged.\n\n"
        "- API route status: `PAUSED_TRANSIENT_PROVIDER` (preserved, not COMPLETE)\n"
        f"- Persisted API attempt events: {lineage['attempt_events']}\n"
        f"- Saved API responses: {lineage['saved_responses']}\n"
        "- Accepted API occurrences: 0\n"
        "- Episode 6: three saved HTTP 500 responses\n"
        "- Replacement route: local search of arXiv snapshot v303 for this wave\n"
        "- API equivalence: NOT_CLAIMED\n"
        "- Provider completeness: UNPROVEN\n"
        "- Production/PRISMA application: NOT YET APPLIED\n\n"
        "Counts are episode-scoped persisted events. Attempt identifiers can recur "
        "across recovery episodes, so each immutable episode checkpoint is counted "
        "once rather than globally deduplicating attempt IDs.\n"
    )


def prepare_integration_dry_run(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    package_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Validate bindings and write a fresh, non-production staging report."""

    root_path = Path(root).resolve()
    output_path = Path(output_dir)
    if not output_path.is_absolute():
        output_path = (root_path / output_path).resolve()
    try:
        output_path.relative_to(root_path)
    except ValueError as exc:
        raise ArxivSnapshotIntegrationError(
            "dry-run output must remain inside the repository"
        ) from exc
    if output_path.exists():
        raise ArxivSnapshotIntegrationError("dry-run output namespace already exists")

    package = validate_prepared_package(
        _resolve_repository_input(root_path, package_dir)
    )
    lineage = derive_api_lineage(root=root_path, state=state)
    amendment = build_amendment_v2(package, lineage=lineage)
    prepared_projection = _read_json(package.package_dir / "offline_merge_dry_run.json")
    binding = prepared_projection.get("active_state_binding", {})
    if (
        binding.get("raw_sha256") != PARENT_EXECUTION_STATE_SHA256
        or binding.get("state_hash") != PARENT_EXECUTION_STATE_HASH
        or sum(
            int(item.get("occurrence_count", 0))
            for item in state.get("sources", {}).values()
        )
        != prepared_projection.get("evidence_counts", {}).get("existing_occurrences")
    ):
        raise ArxivSnapshotIntegrationError("dry-run input dataset binding changed")
    generic = prepared_projection["generic_doi_title_dry_run"]
    identity = prepared_projection["proposed_identity_aware_dry_run"]
    projected = (
        generic["existing_provisional_global_canonical_keys"]
        + generic["snapshot_keys_added"]
        - identity["one_to_one_nonconflicting_links"]
    )
    if projected != identity[
        "provisional_combined_canonical_count_if_same-record_proposals_applied"
    ]:
        raise ArxivSnapshotIntegrationError("bound projection arithmetic changed")

    output_path.mkdir(parents=True)
    amendment_json = output_path / "retrieval_method_amendment_v2.json"
    amendment_md = output_path / "retrieval_method_amendment_v2.md"
    report_path = output_path / "snapshot_integration_dry_run.json"
    atomic_write(
        amendment_json,
        (json.dumps(amendment, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )
    atomic_write(amendment_md, _amendment_markdown(amendment).encode("utf-8"))
    report = {
        "schema_version": "1.0.0",
        "status": "VALIDATED_OFFLINE_NOT_APPLIED",
        "package_manifest_sha256": package.package_manifest_sha256,
        "execution_state_sha256": PARENT_EXECUTION_STATE_SHA256,
        "execution_state_hash": PARENT_EXECUTION_STATE_HASH,
        "api_lineage": lineage,
        "occurrence_evidence": {
            "snapshot_query_memberships": len(package.snapshot_dataset.occurrences),
            "snapshot_record_identities": len(package.snapshot_dataset.canonical_records),
            "same_record_adjudications": len(package.identity_proposals),
            "related_version_candidates": len(
                {
                    item["candidate_identity"]["staging_candidate_id"]
                    for item in package.relationship_proposals
                }
            ),
            "related_version_links": len(package.relationship_proposals),
        },
        "bound_projection": {
            "existing_occurrences": prepared_projection["evidence_counts"][
                "existing_occurrences"
            ],
            "combined_occurrences": prepared_projection["evidence_counts"][
                "combined_occurrences_if_imported"
            ],
            "generic_provisional_canonical_records": generic[
                "provisional_combined_canonical_count"
            ],
            "identity_aware_provisional_canonical_records": projected,
            "universal_expected_count": False,
        },
        "production_state_modified": False,
        "production_eligible": False,
        "prisma_counted": False,
        "identification_set_closed": False,
        "prior_survey_seed_still_required": True,
    }
    atomic_write(
        report_path,
        (json.dumps(report, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )
    manifest = {
        "schema_version": "1.0.0",
        "status": "VALIDATED_OFFLINE_NOT_APPLIED",
        "artifacts": {
            item.name: {
                "path": item.name,
                "byte_size": item.stat().st_size,
                "raw_sha256": _sha256_file(item),
            }
            for item in (amendment_json, amendment_md, report_path)
        },
    }
    manifest_path = output_path / "dry_run_manifest.json"
    atomic_write(
        manifest_path,
        (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode("utf-8"),
    )
    report["output_dir"] = output_path.relative_to(root_path).as_posix()
    report["dry_run_manifest_sha256"] = _sha256_file(manifest_path)
    return report


def _effective_decisions(dataset: ReviewDataset) -> dict[str, DuplicateDecision]:
    superseded = {
        previous
        for decision in dataset.duplicate_decisions
        if decision.provenance.scope is DecisionScope.PROSPECTIVE
        for previous in decision.provenance.supersedes_ids
    }
    return {
        decision.occurrence_id: decision
        for decision in dataset.duplicate_decisions
        if decision.provenance.scope is DecisionScope.PROSPECTIVE
        and decision.decision_id not in superseded
    }


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _candidate_ids_by_arxiv(
    dataset: ReviewDataset, package: ValidatedSnapshotPackage
) -> dict[str, list[str]]:
    approved = {
        str(item["normalized_arxiv_id"]): item for item in package.identity_proposals
    }
    result: dict[str, list[str]] = {key: [] for key in approved}
    for occurrence in dataset.occurrences:
        if occurrence.record.source_database != "arXivSnapshotV303":
            continue
        normalized = _normalize_arxiv_id(occurrence.record.arxiv_id)
        if normalized is not None and normalized in result:
            result[normalized].append(occurrence.occurrence_id)
    expected: dict[str, set[str]] = {key: set() for key in approved}
    for occurrence in package.snapshot_dataset.occurrences:
        normalized = _normalize_arxiv_id(occurrence.record.arxiv_id)
        if normalized is not None and normalized in expected:
            expected[normalized].add(occurrence.occurrence_id)
    if any(
        not occurrence_ids or set(occurrence_ids) != expected[normalized]
        for normalized, occurrence_ids in result.items()
    ):
        raise ArxivSnapshotIntegrationError(
            "approved snapshot occurrence identities changed after merge"
        )
    return result


def _target_canonical_by_arxiv(
    dataset: ReviewDataset,
    package: ValidatedSnapshotPackage,
    effective: Mapping[str, DuplicateDecision],
) -> dict[str, CanonicalRecord]:
    requested = {str(item["normalized_arxiv_id"]) for item in package.identity_proposals}
    canonical_by_id = {item.canonical_id: item for item in dataset.canonical_records}
    found: dict[str, set[str]] = {key: set() for key in requested}
    for occurrence in dataset.occurrences:
        if occurrence.record.source_database == "arXivSnapshotV303":
            continue
        normalized = _normalize_arxiv_id(occurrence.record.arxiv_id)
        if normalized is not None and normalized in found:
            found[normalized].add(effective[occurrence.occurrence_id].canonical_record_id)
    if any(len(targets) != 1 for targets in found.values()):
        raise ArxivSnapshotIntegrationError(
            "approved arXiv identity has a missing or conflicting merged target"
        )
    return {
        key: canonical_by_id[next(iter(targets))] for key, targets in found.items()
    }


def _relationship_links(
    dataset: ReviewDataset, package: ValidatedSnapshotPackage
) -> list[dict[str, Any]]:
    canonicals_by_key: dict[str, list[CanonicalRecord]] = {}
    for canonical in dataset.canonical_records:
        key = record_key(canonical.record)
        if key:
            canonicals_by_key.setdefault(key, []).append(canonical)
    links: list[dict[str, Any]] = []
    for proposal in package.relationship_proposals:
        existing = proposal["existing_identity"]
        identity_key = existing.get("identity_key")
        targets = canonicals_by_key.get(identity_key, [])
        if len(targets) != 1:
            raise ArxivSnapshotIntegrationError(
                "related-version proposal target is missing or conflicting after merge"
            )
        candidate_arxiv = _normalize_arxiv_id(
            proposal["candidate_identity"].get("arxiv_id")
        )
        candidates = [
            item
            for item in dataset.canonical_records
            if item.record.source_database == "arXivSnapshotV303"
            and _normalize_arxiv_id(item.record.arxiv_id) == candidate_arxiv
        ]
        if len(candidates) != 1 or candidates[0].canonical_id == targets[0].canonical_id:
            raise ArxivSnapshotIntegrationError(
                "related-version records were collapsed or became ambiguous"
            )
        links.append(
            {
                "proposal_id": proposal["proposal_id"],
                "relationship": "related preprint/published version",
                "identity_scope": "bibliographic_record_only",
                "snapshot_canonical_id": candidates[0].canonical_id,
                "existing_canonical_id": targets[0].canonical_id,
                "study_grouping_decided": False,
                "screening_or_eligibility_decided": False,
            }
        )
    return sorted(links, key=lambda item: item["proposal_id"])


def apply_snapshot_adjudications(
    dataset: ReviewDataset,
    package: ValidatedSnapshotPackage,
    *,
    created_at: str,
) -> ReviewDataset:
    """Apply only the 424 exact identity decisions after ordinary merging."""

    dataset.validate()
    effective = _effective_decisions(dataset)
    candidate_occurrences = _candidate_ids_by_arxiv(dataset, package)
    targets = _target_canonical_by_arxiv(dataset, package, effective)
    canonical_by_id = {item.canonical_id: item for item in dataset.canonical_records}
    occurrence_by_id = {item.occurrence_id: item for item in dataset.occurrences}
    proposal_by_arxiv = {
        str(item["normalized_arxiv_id"]): item for item in package.identity_proposals
    }
    planned_groups: list[
        tuple[str, dict[str, Any], str, str, list[str], set[str]]
    ] = []
    claimed_groups: dict[str, tuple[str, str]] = {}
    for normalized_id, occurrence_ids in candidate_occurrences.items():
        target = targets[normalized_id]
        proposal = proposal_by_arxiv[normalized_id]
        candidate_ids = set(occurrence_ids)
        old_canonical_ids = list(
            dict.fromkeys(effective[item].canonical_record_id for item in occurrence_ids)
        )
        for old_canonical_id in old_canonical_ids:
            old_canonical = canonical_by_id[old_canonical_id]
            if old_canonical.canonical_id == target.canonical_id:
                raise ArxivSnapshotIntegrationError(
                    "arXiv-ID-only proposal already matches under the generic rule"
                )
            claimed = claimed_groups.get(old_canonical.canonical_id)
            if claimed is not None and claimed != (normalized_id, target.canonical_id):
                raise ArxivSnapshotIntegrationError(
                    "a generic canonical group has competing approved identity targets"
                )
            claimed_groups[old_canonical.canonical_id] = (
                normalized_id,
                target.canonical_id,
            )
            group_occurrence_ids = list(old_canonical.occurrence_ids)
            direct_ids = candidate_ids.intersection(group_occurrence_ids)
            if not direct_ids:
                raise ArxivSnapshotIntegrationError(
                    "approved snapshot occurrence left its generic canonical group"
                )
            group_key = str(old_canonical.metadata.get("dedupe_key") or "")
            propagated_ids = set(group_occurrence_ids) - direct_ids
            if propagated_ids and not group_key.startswith("title:"):
                raise ArxivSnapshotIntegrationError(
                    "arXiv-ID-only propagation requires a generic title group"
                )
            target_key = record_key(target.record)
            for member_id in group_occurrence_ids:
                member = occurrence_by_id[member_id]
                previous = effective[member_id]
                member_arxiv_id = _normalize_arxiv_id(member.record.arxiv_id)
                member_key = record_key(member.record)
                identity_resolution = member.record.original_metadata.get(
                    "identity_resolution"
                )
                identity_conflict = member.record.original_metadata.get(
                    "identity_conflict_status"
                )
                if previous.canonical_record_id != old_canonical.canonical_id:
                    raise ArxivSnapshotIntegrationError(
                        "generic canonical membership changed during adjudication"
                    )
                if (
                    previous.match_rule != "doi_first_title_fallback"
                    or previous.match_key != group_key
                    or member_key != group_key
                ):
                    raise ArxivSnapshotIntegrationError(
                        "arXiv-ID-only propagation requires current generic title evidence"
                    )
                if member_arxiv_id not in {None, normalized_id}:
                    raise ArxivSnapshotIntegrationError(
                        "generic title group has a contradictory arXiv identifier"
                    )
                if (
                    member_id not in direct_ids
                    and member.record.doi
                    and member_key != target_key
                ):
                    raise ArxivSnapshotIntegrationError(
                        "generic title group has a contradictory DOI identifier"
                    )
                if member_id not in direct_ids and identity_resolution == "UNRESOLVED":
                    raise ArxivSnapshotIntegrationError(
                        "generic title group has an unresolved identity restriction"
                    )
                if member_id not in direct_ids and identity_conflict not in {
                    None,
                    "",
                    "NO_RECORDED_METADATA_CONFLICT",
                }:
                    raise ArxivSnapshotIntegrationError(
                        "generic title group has an explicit identity conflict"
                    )
            planned_groups.append(
                (
                    normalized_id,
                    proposal,
                    target.canonical_id,
                    old_canonical.canonical_id,
                    group_occurrence_ids,
                    direct_ids,
                )
            )

    source_group_ids = {item[3] for item in planned_groups}
    if any(item[2] in source_group_ids for item in planned_groups):
        raise ArxivSnapshotIntegrationError(
            "approved identity targets form competing canonical-group transfers"
        )

    propagated_count = 0
    for (
        normalized_id,
        proposal,
        target_canonical_id,
        old_canonical_id,
        group_occurrence_ids,
        direct_ids,
    ) in planned_groups:
        target = canonical_by_id[target_canonical_id]
        old_canonical = canonical_by_id[old_canonical_id]
        old_canonical.occurrence_ids.clear()
        target.occurrence_ids.extend(group_occurrence_ids)
        for occurrence_id in group_occurrence_ids:
            previous = effective[occurrence_id]
            direct = occurrence_id in direct_ids
            provenance_metadata = {
                "proposal_id": proposal["proposal_id"],
                "normalized_arxiv_id": normalized_id,
                "candidate_source_line_number": proposal["candidate_identity"][
                    "source_line_number"
                ],
                "target_resolved_after_global_merge": True,
                "generic_dedupe_rule_changed": False,
                "decision_application": (
                    "direct_arxiv_id_adjudication"
                    if direct
                    else "propagated_generic_title_group_membership"
                ),
            }
            if not direct:
                provenance_metadata.update(
                    {
                        "propagated_from_canonical_id": old_canonical_id,
                        "propagated_from_decision_id": previous.decision_id,
                        "propagated_from_match_key": previous.match_key,
                        "propagated_from_match_rule": previous.match_rule,
                        "identity_restrictions_preserved": True,
                    }
                )
            decision = DuplicateDecision(
                decision_id=_stable_id(
                    "dedupe-adjudication",
                    package.package_manifest_sha256,
                    proposal["proposal_id"],
                    occurrence_id,
                    target.canonical_id,
                ),
                occurrence_id=occurrence_id,
                canonical_record_id=target.canonical_id,
                survivor_occurrence_id=target.survivor_occurrence_id,
                outcome=DedupeOutcome.DUPLICATE,
                match_key=(f"arxiv:{normalized_id}" if direct else previous.match_key),
                match_rule=(
                    DIRECT_IDENTITY_MATCH_RULE
                    if direct
                    else PROPAGATED_IDENTITY_MATCH_RULE
                ),
                provenance=DecisionProvenance(
                    actor=DecisionActor(
                        actor_id="h2h_lit.arxiv_snapshot_integration",
                        actor_type=ActorType.ADJUDICATOR,
                    ),
                    authority=DecisionAuthority.ADJUDICATED,
                    scope=DecisionScope.PROSPECTIVE,
                    protocol_version="1.0.0",
                    rubric_version="arxiv-snapshot-identity-v1",
                    created_at=created_at,
                    supersedes_ids=[previous.decision_id],
                    source_artifact_id=package.package_manifest_sha256,
                    metadata=provenance_metadata,
                ),
            )
            dataset.duplicate_decisions.append(decision)
            effective[occurrence_id] = decision
            metadata_key = (
                "identity_adjudication"
                if direct
                else "identity_adjudication_propagation"
            )
            occurrence_by_id[occurrence_id].metadata[metadata_key] = {
                "decision_id": decision.decision_id,
                "proposal_id": proposal["proposal_id"],
                "package_manifest_sha256": package.package_manifest_sha256,
                "normalized_arxiv_id": normalized_id,
            }
            if not direct:
                occurrence_by_id[occurrence_id].metadata[metadata_key].update(
                    {
                        "propagated_from_canonical_id": old_canonical_id,
                        "propagated_from_decision_id": previous.decision_id,
                        "propagated_from_match_key": previous.match_key,
                    }
                )
                propagated_count += 1

    dataset.canonical_records = [
        item for item in dataset.canonical_records if item.occurrence_ids
    ]
    links = _relationship_links(dataset, package)
    snapshot_runs = sorted(
        (
            item
            for item in dataset.retrieval_runs
            if item.run_id.startswith("arxiv-snapshot-v303:")
        ),
        key=lambda item: item.run_id,
    )
    expected_snapshot_runs = {
        item.run_id for item in package.snapshot_dataset.retrieval_runs
    }
    if not snapshot_runs or not expected_snapshot_runs.issubset(
        {item.run_id for item in snapshot_runs}
    ):
        raise ArxivSnapshotIntegrationError("snapshot retrieval-run inventory changed")
    snapshot_runs[0].metadata[INTEGRATION_MARKER] = {
        "package_manifest_sha256": package.package_manifest_sha256,
        "same_record_adjudication_count": len(package.identity_proposals),
        "propagated_generic_title_group_occurrence_count": propagated_count,
        "related_version_candidate_count": len(
            {
                item["candidate_identity"]["staging_candidate_id"]
                for item in package.relationship_proposals
            }
        ),
        "related_version_links": links,
        "study_grouping_decided": False,
        "screening_or_eligibility_decided": False,
        "provider_completeness": "UNPROVEN",
        "api_equivalence": "NOT_CLAIMED",
    }
    dataset.validate()
    return dataset


def validate_applied_snapshot_adjudications(
    dataset: ReviewDataset, package: ValidatedSnapshotPackage
) -> None:
    """Reject changed adjudication/relationship provenance before another merge."""

    markers = [
        run.metadata[INTEGRATION_MARKER]
        for run in dataset.retrieval_runs
        if INTEGRATION_MARKER in run.metadata
    ]
    if not markers:
        return
    if len(markers) != 1:
        raise ArxivSnapshotIntegrationError("snapshot integration marker changed")
    marker = markers[0]
    if (
        marker.get("package_manifest_sha256") != package.package_manifest_sha256
        or marker.get("same_record_adjudication_count")
        != len(package.identity_proposals)
        or not isinstance(
            marker.get("propagated_generic_title_group_occurrence_count"), int
        )
        or len(marker.get("related_version_links", []))
        != len(package.relationship_proposals)
        or marker.get("provider_completeness") != "UNPROVEN"
        or marker.get("api_equivalence") != "NOT_CLAIMED"
        or marker.get("study_grouping_decided") is not False
        or marker.get("screening_or_eligibility_decided") is not False
    ):
        raise ArxivSnapshotIntegrationError(
            "snapshot integration marker provenance changed"
        )
    effective = _effective_decisions(dataset)
    candidates = _candidate_ids_by_arxiv(dataset, package)
    proposals = {
        item["normalized_arxiv_id"]: item for item in package.identity_proposals
    }
    occurrence_by_id = {item.occurrence_id: item for item in dataset.occurrences}
    resolved_target_by_arxiv: dict[str, str] = {}
    for normalized_id, occurrence_ids in candidates.items():
        proposal = proposals[normalized_id]
        approved_identity = proposal["existing_identity"]
        approved_occurrence_id = str(
            approved_identity.get("survivor_occurrence_id") or ""
        )
        resolved_target_canonical_ids = {
            effective[item.occurrence_id].canonical_record_id
            for item in dataset.occurrences
            if item.record.source_database != "arXivSnapshotV303"
            and _normalize_arxiv_id(item.record.arxiv_id) == normalized_id
        }
        if len(resolved_target_canonical_ids) != 1:
            raise ArxivSnapshotIntegrationError(
                "snapshot adjudication target identity changed"
            )
        resolved_target_canonical_id = next(iter(resolved_target_canonical_ids))
        if approved_occurrence_id:
            approved_occurrence = occurrence_by_id.get(approved_occurrence_id)
            approved_key = record_key(approved_identity)
            if (
                approved_occurrence is None
                or approved_occurrence.record.source_database == "arXivSnapshotV303"
                or _normalize_arxiv_id(approved_occurrence.record.arxiv_id)
                != normalized_id
                or (
                    approved_identity.get("source_database")
                    and approved_occurrence.record.source_database
                    != approved_identity["source_database"]
                )
                or (approved_key and record_key(approved_occurrence.record) != approved_key)
                or effective[approved_occurrence_id].canonical_record_id
                != resolved_target_canonical_id
            ):
                raise ArxivSnapshotIntegrationError(
                    "approved snapshot adjudication target evidence changed"
                )
        resolved_target_by_arxiv[normalized_id] = resolved_target_canonical_id
        for occurrence_id in occurrence_ids:
            decision = effective[occurrence_id]
            metadata = occurrence_by_id[occurrence_id].metadata.get(
                "identity_adjudication", {}
            )
            if (
                decision.match_rule != DIRECT_IDENTITY_MATCH_RULE
                or decision.match_key != f"arxiv:{normalized_id}"
                or decision.provenance.authority is not DecisionAuthority.ADJUDICATED
                or decision.provenance.source_artifact_id
                != package.package_manifest_sha256
                or decision.provenance.metadata.get("proposal_id")
                != proposal["proposal_id"]
                or decision.provenance.metadata.get("decision_application")
                != "direct_arxiv_id_adjudication"
                or metadata.get("decision_id") != decision.decision_id
                or metadata.get("proposal_id") != proposal["proposal_id"]
                or metadata.get("package_manifest_sha256")
                != package.package_manifest_sha256
                or metadata.get("normalized_arxiv_id") != normalized_id
            ):
                raise ArxivSnapshotIntegrationError(
                    "snapshot identity adjudication provenance changed"
                )
            survivor = occurrence_by_id[decision.survivor_occurrence_id]
            if (
                survivor.record.source_database == "arXivSnapshotV303"
                or decision.canonical_record_id != resolved_target_canonical_id
            ):
                raise ArxivSnapshotIntegrationError(
                    "snapshot adjudication target identity changed"
                )
    all_decisions = {item.decision_id: item for item in dataset.duplicate_decisions}
    propagated = [
        item
        for item in effective.values()
        if item.match_rule == PROPAGATED_IDENTITY_MATCH_RULE
    ]
    if len(propagated) != marker["propagated_generic_title_group_occurrence_count"]:
        raise ArxivSnapshotIntegrationError(
            "snapshot propagated identity decision count changed"
        )
    for decision in propagated:
        provenance = decision.provenance
        metadata = provenance.metadata
        normalized_id = metadata.get("normalized_arxiv_id")
        proposal = proposals.get(normalized_id)
        occurrence = occurrence_by_id[decision.occurrence_id]
        occurrence_metadata = occurrence.metadata.get(
            "identity_adjudication_propagation", {}
        )
        previous_id = metadata.get("propagated_from_decision_id")
        previous = all_decisions.get(previous_id)
        if (
            proposal is None
            or occurrence.record.source_database == "arXivSnapshotV303"
            or provenance.authority is not DecisionAuthority.ADJUDICATED
            or provenance.source_artifact_id != package.package_manifest_sha256
            or provenance.supersedes_ids != [previous_id]
            or metadata.get("proposal_id") != proposal["proposal_id"]
            or metadata.get("decision_application")
            != "propagated_generic_title_group_membership"
            or metadata.get("identity_restrictions_preserved") is not True
            or previous is None
            or previous.occurrence_id != decision.occurrence_id
            or previous.match_rule != "doi_first_title_fallback"
            or previous.match_key != decision.match_key
            or metadata.get("propagated_from_canonical_id")
            != previous.canonical_record_id
            or metadata.get("propagated_from_match_key") != previous.match_key
            or metadata.get("propagated_from_match_rule") != previous.match_rule
            or occurrence_metadata.get("decision_id") != decision.decision_id
            or occurrence_metadata.get("proposal_id") != proposal["proposal_id"]
            or occurrence_metadata.get("package_manifest_sha256")
            != package.package_manifest_sha256
            or occurrence_metadata.get("normalized_arxiv_id") != normalized_id
            or occurrence_metadata.get("propagated_from_decision_id") != previous_id
            or occurrence_metadata.get("propagated_from_canonical_id")
            != previous.canonical_record_id
            or occurrence_metadata.get("propagated_from_match_key")
            != decision.match_key
        ):
            raise ArxivSnapshotIntegrationError(
                "snapshot propagated identity provenance changed"
            )
        survivor = occurrence_by_id[decision.survivor_occurrence_id]
        if (
            survivor.record.source_database == "arXivSnapshotV303"
            or decision.canonical_record_id
            != resolved_target_by_arxiv.get(normalized_id)
        ):
            raise ArxivSnapshotIntegrationError(
                "snapshot propagated identity target changed"
            )
    expected_proposal_ids = {
        item["proposal_id"] for item in package.relationship_proposals
    }
    links = marker["related_version_links"]
    if (
        {item.get("proposal_id") for item in links} != expected_proposal_ids
        or any(
            item.get("relationship") != "related preprint/published version"
            or item.get("study_grouping_decided") is not False
            or item.get("screening_or_eligibility_decided") is not False
            for item in links
        )
    ):
        raise ArxivSnapshotIntegrationError(
            "snapshot related-version provenance changed"
        )
    dataset.validate()


def merge_identification_datasets_with_snapshot_integration(
    datasets: list[ReviewDataset],
    package: ValidatedSnapshotPackage,
    *,
    created_at: str | None = None,
) -> ReviewDataset:
    """Run the actual generic merge, then reapply bound snapshot decisions."""

    for dataset in datasets:
        validate_applied_snapshot_adjudications(dataset, package)
    merged = merge_identification_datasets(datasets)
    timestamp = created_at or max(
        run.retrieval_completed_at for run in merged.retrieval_runs
    )
    return apply_snapshot_adjudications(merged, package, created_at=timestamp)


def _validate_amendment_v2(
    path: Path, package: ValidatedSnapshotPackage, lineage: Mapping[str, Any]
) -> tuple[dict[str, Any], str]:
    amendment = _read_json(path)
    expected = build_amendment_v2(package, lineage=lineage)
    if amendment != expected:
        raise ArxivSnapshotIntegrationError("retrieval-method amendment v2 changed")
    return amendment, _sha256_file(path)


def authorize_snapshot_substitution(
    *,
    root: str | Path,
    state: dict[str, Any],
    package_dir: str | Path,
    amendment_v2_path: str | Path,
    timestamp: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Prepare production artifacts/state; caller must hold and persist the lock."""

    root_path = Path(root).resolve()
    source_state = state.get("sources", {}).get("arXiv", {})
    existing = source_state.get("approved_snapshot_substitution")
    if existing is not None:
        validate_authorized_snapshot_substitution(root=root_path, state=state)
        return state
    if _sha256_file(
        root_path
        / "outputs/production/star-external-retrieval-wave-001/execution/execution_state.json"
    ) != PARENT_EXECUTION_STATE_SHA256:
        raise ArxivSnapshotIntegrationError("unexpected production-state drift")
    if (
        state.get("identification_set_closed") is not False
        or state.get("final_global_deduplication_executed") is not False
        or state.get("prisma_generated") is not False
        or state.get("screening_executed") is not False
        or state.get("prior_survey_seed_imported") is not False
    ):
        raise ArxivSnapshotIntegrationError(
            "snapshot integration requires the open pre-screening identification state"
        )
    if any(
        item.get("status") == "RUNNING" for item in state.get("sources", {}).values()
    ):
        raise ArxivSnapshotIntegrationError(
            "cannot integrate while an external-source session is RUNNING"
        )

    package_path = _resolve_repository_input(root_path, package_dir)
    package = validate_prepared_package(package_path)
    lineage = derive_api_lineage(root=root_path, state=state)
    amendment_path = _resolve_repository_input(root_path, amendment_v2_path)
    amendment, amendment_sha256 = _validate_amendment_v2(
        amendment_path, package, lineage
    )
    output_root = root_path / INTEGRATION_OUTPUT
    if output_root.exists():
        raise ArxivSnapshotIntegrationError(
            "snapshot production artifacts exist without valid state provenance"
        )
    output_root.mkdir(parents=True)
    checkpoint_path = output_root / "checkpoint/review_dataset.json"
    checkpoint_path.parent.mkdir(parents=True)
    save_review_dataset(checkpoint_path, package.snapshot_dataset)
    saved_sha256 = _sha256_file(checkpoint_path)
    if saved_sha256 != package.import_manifest["merged_snapshot_dataset"]["raw_sha256"]:
        raise ArxivSnapshotIntegrationError("saved snapshot dataset changed")
    amendment_output = output_root / "retrieval_method_amendment_v2.json"
    shutil.copyfile(amendment_path, amendment_output)
    authorized_at = (timestamp or (lambda: datetime.now(UTC).isoformat()))()
    api_binding = {
        "status": source_state["status"],
        "active_episode_number": source_state["active_episode_number"],
        "checkpoint_dataset": source_state["checkpoint_dataset"],
        "episode_count": len(source_state["execution_episodes"]),
        "cumulative_attempt_events": lineage["attempt_events"],
        "cumulative_saved_responses": lineage["saved_responses"],
        "accepted_occurrences": 0,
    }
    substitution = {
        "schema_version": "1.0.0",
        "status": "APPROVED_COMPLETE_REPLACEMENT_ROUTE",
        "authorized_at_utc": authorized_at,
        "scope": "star-external-retrieval-wave-001/arXiv",
        "package": {
            "path": package_path.relative_to(root_path).as_posix(),
            "manifest_sha256": package.package_manifest_sha256,
        },
        "retrieval_method_amendment": {
            "path": amendment_output.relative_to(root_path).as_posix(),
            "raw_sha256": amendment_sha256,
            "amendment_id": amendment["amendment_id"],
        },
        "snapshot_checkpoint": {
            "path": checkpoint_path.relative_to(root_path).as_posix(),
            "byte_size": checkpoint_path.stat().st_size,
            "raw_sha256": saved_sha256,
        },
        "api_route": api_binding,
        "counts": {
            "query_membership_occurrences": 1333,
            "snapshot_record_identities": 958,
            "same_record_adjudications": 424,
            "related_version_candidates": 45,
            "related_version_links": 50,
        },
        "identity_proposals_sha256": _sha256_file(
            package.package_dir / "arxiv_id_identity_link_proposals.jsonl"
        ),
        "relationship_proposals_sha256": _sha256_file(
            package.package_dir / "ambiguous_adjudication_proposals.jsonl"
        ),
        "identity_decisions": {
            "status": "AUTHORIZED_PENDING_GLOBAL_MERGE",
            "count": 424,
            "application": (
                "merge_identification_datasets_with_snapshot_integration"
            ),
            "targets_resolved_after_global_merge": True,
        },
        "related_version_relationships": {
            "status": "PRESERVED_SEPARATE_NOT_DEDUPED",
            "candidate_count": 45,
            "link_count": 50,
            "study_grouping_decided": False,
            "screening_or_eligibility_decided": False,
        },
        "generic_dedupe_rule_changed": False,
        "api_status_preserved": True,
        "api_history_preserved": True,
        "provider_completeness": "UNPROVEN",
        "api_equivalence": "NOT_CLAIMED",
        "identification_set_closed": False,
        "prior_survey_seed_still_required": True,
        "prisma_counted": False,
        "screening_decisions_created": False,
    }
    source_state["approved_snapshot_substitution"] = substitution
    validate_authorized_snapshot_substitution(root=root_path, state=state)
    return state


def validate_authorized_snapshot_substitution(
    *, root: str | Path, state: Mapping[str, Any]
) -> None:
    """Validate an existing authorization before later use or idempotent return."""

    root_path = Path(root).resolve()
    source = state.get("sources", {}).get("arXiv", {})
    substitution = source.get("approved_snapshot_substitution")
    if substitution is None:
        return
    if (
        substitution.get("status") != "APPROVED_COMPLETE_REPLACEMENT_ROUTE"
        or source.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or source.get("active_episode_number") != 6
        or source.get("occurrence_count") != 0
        or state.get("identification_set_closed") is not False
        or state.get("prior_survey_seed_imported") is not False
        or substitution.get("provider_completeness") != "UNPROVEN"
        or substitution.get("api_equivalence") != "NOT_CLAIMED"
        or substitution.get("generic_dedupe_rule_changed") is not False
        or substitution.get("identity_decisions")
        != {
            "status": "AUTHORIZED_PENDING_GLOBAL_MERGE",
            "count": 424,
            "application": "merge_identification_datasets_with_snapshot_integration",
            "targets_resolved_after_global_merge": True,
        }
        or substitution.get("related_version_relationships")
        != {
            "status": "PRESERVED_SEPARATE_NOT_DEDUPED",
            "candidate_count": 45,
            "link_count": 50,
            "study_grouping_decided": False,
            "screening_or_eligibility_decided": False,
        }
        or substitution.get("counts")
        != {
            "query_membership_occurrences": 1333,
            "snapshot_record_identities": 958,
            "same_record_adjudications": 424,
            "related_version_candidates": 45,
            "related_version_links": 50,
        }
    ):
        raise ArxivSnapshotIntegrationError(
            "authorized snapshot substitution provenance changed"
        )
    api = substitution.get("api_route", {})
    if (
        api.get("status") != source.get("status")
        or api.get("active_episode_number") != source.get("active_episode_number")
        or api.get("checkpoint_dataset") != source.get("checkpoint_dataset")
        or api.get("episode_count") != len(source.get("execution_episodes", []))
        or api.get("cumulative_attempt_events") != 60
        or api.get("cumulative_saved_responses") != 37
        or api.get("accepted_occurrences") != 0
    ):
        raise ArxivSnapshotIntegrationError("preserved arXiv API lineage changed")
    package_ref = substitution.get("package", {})
    package = validate_prepared_package(root_path / str(package_ref.get("path", "")))
    if package_ref.get("manifest_sha256") != package.package_manifest_sha256:
        raise ArxivSnapshotIntegrationError("authorized package binding changed")
    for key in ("retrieval_method_amendment", "snapshot_checkpoint"):
        reference = substitution.get(key, {})
        path = (root_path / str(reference.get("path", ""))).resolve()
        try:
            path.relative_to(root_path)
        except ValueError as exc:
            raise ArxivSnapshotIntegrationError(
                "authorized artifact escapes repository"
            ) from exc
        if (
            not path.is_file()
            or _sha256_file(path) != reference.get("raw_sha256")
            or (
                "byte_size" in reference
                and path.stat().st_size != reference.get("byte_size")
            )
        ):
            raise ArxivSnapshotIntegrationError(
                "authorized snapshot integration artifact changed"
            )
