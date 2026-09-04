"""Fail-closed planning and preflight for the isolated STAR validation run."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from h2h_lit.acm_field_execution import load_acm_final_reconciliation_manifest
from h2h_lit.checkpoint import atomic_write
from h2h_lit.production_query_plan import load_production_query_plan

SCHEMA_VERSION = "1.0.0"
PLAN_ID = "star-provisional-pipeline-validation-001"
PREFLIGHT_SCHEMA_VERSION = "1.0.0"
PREFLIGHT_ARTIFACT_CLASS = "PROVISIONAL_VALIDATION_PREFLIGHT_ONLY"
ALLOWED_OUTPUT_ROOT = Path("outputs/provisional")
EXPECTED_FAMILIES = (
    "STAR-QF01-RELATIONAL-VIS",
    "STAR-QF02-ASSISTED-VIS",
    "STAR-QF03-INTERACTIVE-SYSTEMS",
    "STAR-QF04-NONDESKTOP-ENV",
    "STAR-QF05-CONVERSATIONAL",
)
EXPECTED_AUTHORIZATION_FLAGS = {
    "pubmed": "--authorize-pubmed-execution",
    "acm": "--authorize-acm-provisional-import",
    "llm": "--authorize-llm-inference",
}


class ProvisionalValidationError(ValueError):
    """The provisional validation plan is unsafe or no longer reproducible."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(_canonical_json(value).encode("utf-8"))


def _bound_file(root: Path, specification: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    relative = Path(str(specification["path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ProvisionalValidationError("bound paths must be repository-relative")
    path = root / relative
    raw = path.read_bytes()
    actual = _sha256_bytes(raw)
    if actual != specification["raw_sha256"]:
        raise ProvisionalValidationError(f"bound file hash changed: {relative.as_posix()}")
    return path, {
        "path": relative.as_posix(),
        "byte_size": len(raw),
        "raw_sha256": actual,
        **(
            {"canonical_hash": specification["canonical_hash"]}
            if "canonical_hash" in specification
            else {}
        ),
    }


def _validate_embedded_hash(payload: dict[str, Any], field: str) -> None:
    claimed = payload.get(field)
    material = dict(payload)
    material.pop(field, None)
    if claimed != _sha256_json(material):
        raise ProvisionalValidationError(f"embedded {field} is invalid")


def load_validation_config(path: str | Path) -> dict[str, Any]:
    """Load the additive nonproduction plan and reject weakened safeguards."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ProvisionalValidationError("unsupported provisional validation schema")
    if payload.get("plan_id") != PLAN_ID:
        raise ProvisionalValidationError("unexpected provisional validation plan ID")
    if not str(payload.get("run_id", "")).startswith("provisional:"):
        raise ProvisionalValidationError("provisional run IDs require the provisional prefix")
    if payload.get("production_import_allowed") is not False:
        raise ProvisionalValidationError("the provisional plan cannot permit production import")
    if payload.get("output_namespace") != (
        "outputs/provisional/star-pipeline-validation-001"
    ):
        raise ProvisionalValidationError("unexpected provisional output namespace")

    pubmed = payload.get("pubmed", {})
    if pubmed.get("complete_identity_enumeration_required") is not True:
        raise ProvisionalValidationError("complete PubMed identity enumeration is required")
    if pubmed.get("family_count") != len(EXPECTED_FAMILIES):
        raise ProvisionalValidationError("PubMed must cover all five frozen families")
    if pubmed.get("metadata_sample_size_per_family") != 100:
        raise ProvisionalValidationError("unexpected PubMed metadata sample size")
    if pubmed.get("metadata_fetch_batch_size") != 100:
        raise ProvisionalValidationError("unexpected PubMed metadata batch size")
    expected_requests = len(EXPECTED_FAMILIES) * (
        1
        + (
            pubmed["metadata_sample_size_per_family"]
            + pubmed["metadata_fetch_batch_size"]
            - 1
        )
        // pubmed["metadata_fetch_batch_size"]
    )
    if pubmed.get("expected_request_count_without_retries") != expected_requests:
        raise ProvisionalValidationError("PubMed request-count declaration is inconsistent")
    if pubmed.get("authorization_flag") != EXPECTED_AUTHORIZATION_FLAGS["pubmed"]:
        raise ProvisionalValidationError("unexpected PubMed authorization boundary")
    if payload.get("acm", {}).get("authorization_flag") != EXPECTED_AUTHORIZATION_FLAGS["acm"]:
        raise ProvisionalValidationError("unexpected ACM authorization boundary")
    if payload.get("screening", {}).get("authorization_flag") != (
        EXPECTED_AUTHORIZATION_FLAGS["llm"]
    ):
        raise ProvisionalValidationError("unexpected LLM authorization boundary")

    if payload.get("candidate_selection", {}).get("target_canonical_records") != 750:
        raise ProvisionalValidationError("unexpected provisional candidate target")
    if payload.get("screening", {}).get("proposal_sample_size") != 250:
        raise ProvisionalValidationError("unexpected proposal sample target")
    if payload.get("jfr25_rediscovery", {}).get("create_seed_occurrences") is not False:
        raise ProvisionalValidationError("JFR25 must remain comparison-only")
    prohibited = payload.get("prohibited_effects", {})
    if not prohibited or not all(value is True for value in prohibited.values()):
        raise ProvisionalValidationError("every production side effect must remain prohibited")
    return payload


def resolve_output_namespace(root: str | Path, configured: str) -> Path:
    """Resolve the one allowed ignored namespace and reject path escape/symlinks."""

    root_path = Path(root).resolve()
    relative = Path(configured)
    if relative.is_absolute() or ".." in relative.parts:
        raise ProvisionalValidationError("output namespace must be repository-relative")
    allowed = (root_path / ALLOWED_OUTPUT_ROOT).resolve()
    output = (root_path / relative).resolve()
    if output == allowed or allowed not in output.parents:
        raise ProvisionalValidationError("output must be beneath outputs/provisional")
    return output


def deterministic_identity_sample(
    identities: list[str], *, sample_size: int, salt: str
) -> list[str]:
    """Select identities without using provider order or record contents."""

    unique = set(identities)
    if len(unique) != len(identities):
        raise ProvisionalValidationError("identity sampling requires a unique universe")
    if sample_size < 0:
        raise ProvisionalValidationError("sample size cannot be negative")
    return sorted(
        identities,
        key=lambda identity: (
            _sha256_bytes(f"{salt}\x1f{identity}".encode()),
            identity,
        ),
    )[:sample_size]


def _acm_bindings(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    selected: list[dict[str, Any]] = []
    family_unions: list[dict[str, Any]] = []
    for family in manifest["families"]:
        family_unions.append(
            {
                "family_id": family["family_id"],
                "unique_stable_identity_count": family["field_union"][
                    "unique_stable_identity_count"
                ],
                "stable_identity_union_digest_sha256": family["field_union"][
                    "stable_identity_union_digest_sha256"
                ],
            }
        )
        for child in family["children"]:
            for artifact in child["selected_artifacts"]:
                selected.append(
                    {
                        "family_id": family["family_id"],
                        "child_query_id": child["child_query_id"],
                        "field_key": child["field_key"],
                        "path": artifact["relative_path"],
                        "byte_size": artifact["byte_size"],
                        "raw_sha256": artifact["raw_sha256"],
                        "total_accounted_entry_count": artifact[
                            "total_accounted_entry_count"
                        ],
                        "malformed_entry_count": artifact["malformed_entry_count"],
                        "classification": artifact["classification"],
                    }
                )
    return selected, family_unions


def build_preflight(
    *,
    root: str | Path,
    config_path: str | Path,
    verify_acm_artifacts: bool = True,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    """Validate every input binding without retrieval, import, inference, or dedupe."""

    root_path = Path(root).resolve()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = root_path / config_file
    config_raw = config_file.read_bytes()
    config = load_validation_config(config_file)
    config_hash = _sha256_bytes(config_raw)
    output = resolve_output_namespace(root_path, config["output_namespace"])

    bindings: dict[str, dict[str, Any]] = {}
    binding_paths: dict[str, Path] = {}
    for name, specification in config["bindings"].items():
        path, binding = _bound_file(root_path, specification)
        binding_paths[name] = path
        bindings[name] = binding

    production_plan = load_production_query_plan(
        binding_paths["production_query_plan"], root=root_path
    )
    expected_plan_hash = config["bindings"]["production_query_plan"]["canonical_hash"]
    if production_plan.plan_hash() != expected_plan_hash:
        raise ProvisionalValidationError("frozen production query-plan hash changed")
    pubmed_queries = [
        query
        for query in production_plan.payload["source_queries"]
        if query["source"] == "PubMed"
    ]
    if tuple(query["family_id"] for query in pubmed_queries) != EXPECTED_FAMILIES:
        raise ProvisionalValidationError("frozen PubMed family set or order changed")

    acm_manifest = load_acm_final_reconciliation_manifest(
        binding_paths["acm_final_reconciliation"],
        root=root_path,
        verify_artifacts=verify_acm_artifacts,
    )
    if acm_manifest["manifest_hash"] != config["bindings"][
        "acm_final_reconciliation"
    ]["canonical_hash"]:
        raise ProvisionalValidationError("ACM reconciliation canonical hash changed")
    if acm_manifest["status"] != config["acm"]["source_manifest_status_required"]:
        raise ProvisionalValidationError("ACM evidence is not in the required source state")
    if acm_manifest["readiness"]["production_import_performed"]:
        raise ProvisionalValidationError("ACM production import has already changed state")
    selected_acm, family_unions = _acm_bindings(acm_manifest)

    jfr25 = json.loads(binding_paths["jfr25_seed_manifest"].read_text(encoding="utf-8"))
    _validate_embedded_hash(jfr25, "artifact_hash")
    if jfr25["artifact_hash"] != config["bindings"]["jfr25_seed_manifest"][
        "canonical_hash"
    ]:
        raise ProvisionalValidationError("JFR25 canonical hash changed")
    if len(jfr25.get("entries", [])) != 138 or jfr25.get("occurrences_created") != 0:
        raise ProvisionalValidationError("JFR25 must remain a 138-member unimported comparison set")

    pubmed = config["pubmed"]
    requests_per_family = 1 + (
        pubmed["metadata_sample_size_per_family"]
        + pubmed["metadata_fetch_batch_size"]
        - 1
    ) // pubmed["metadata_fetch_batch_size"]
    timestamp = generated_at_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    report: dict[str, Any] = {
        "schema_version": PREFLIGHT_SCHEMA_VERSION,
        "artifact_class": PREFLIGHT_ARTIFACT_CLASS,
        "plan_id": config["plan_id"],
        "run_id": config["run_id"],
        "generated_at_utc": timestamp,
        "config": {
            "path": config_file.relative_to(root_path).as_posix(),
            "byte_size": len(config_raw),
            "raw_sha256": config_hash,
        },
        "classification": {
            "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
            "production_import_allowed": False,
            "production_completion_claimed": False,
            "retrieval_cutoff": None,
            "disposition": "DISCARD_ONLY",
        },
        "output_namespace": {
            "path": output.relative_to(root_path).as_posix(),
            "must_remain_beneath": ALLOWED_OUTPUT_ROOT.as_posix(),
            "standalone_production_review_dataset_permitted": False,
        },
        "bindings": bindings,
        "pubmed_plan": {
            "query_count": len(pubmed_queries),
            "queries": [
                {
                    "family_id": query["family_id"],
                    "production_query_id": query["query_id"],
                    "query_text_sha256": query["query_text_sha256"],
                    "enumeration_request_method": query["request_specification"][
                        "method"
                    ],
                    "complete_identity_enumeration": {
                        "required": True,
                        "semantic_state_when_reconciled": "COMPLETE_IDENTITY_ENUMERATION",
                        "characterized_as_truncated_due_to_metadata_sampling": False,
                        "maximum_supported_identity_count": pubmed[
                            "maximum_supported_identity_count_per_family"
                        ],
                    },
                    "metadata_acquisition": {
                        "semantic_state": "DETERMINISTIC_SUBSET_PLANNED",
                        "sample_size": pubmed["metadata_sample_size_per_family"],
                        "fetch_batch_size": pubmed["metadata_fetch_batch_size"],
                        "selection_method": pubmed["selection_method"],
                    },
                    "expected_requests_without_retries": requests_per_family,
                }
                for query in pubmed_queries
            ],
            "expected_request_count_without_retries": len(pubmed_queries)
            * requests_per_family,
            "execution_authorization_required": pubmed["authorization_flag"],
        },
        "acm_plan": {
            "source_manifest_status": acm_manifest["status"],
            "selected_artifact_count": len(selected_acm),
            "selected_artifact_accounted_record_count": sum(
                artifact["total_accounted_entry_count"] for artifact in selected_acm
            ),
            "selected_artifact_malformed_record_count": sum(
                artifact["malformed_entry_count"] for artifact in selected_acm
            ),
            "selected_artifacts": selected_acm,
            "family_unions": family_unions,
            "provisional_occurrences_created": 0,
            "execution_authorization_required": config["acm"]["authorization_flag"],
        },
        "candidate_plan": config["candidate_selection"],
        "screening_plan": {
            **config["screening"],
            "inference_attempts_created": 0,
            "screening_decisions_created": 0,
            "corpus_memberships_created": 0,
        },
        "jfr25_plan": {
            **config["jfr25_rediscovery"],
            "validated_member_count": len(jfr25["entries"]),
            "members_with_normalized_doi": sum(
                bool(entry.get("doi")) for entry in jfr25["entries"]
            ),
            "members_without_normalized_doi": sum(
                not entry.get("doi") for entry in jfr25["entries"]
            ),
            "occurrences_created": 0,
        },
        "authorization_boundaries": {
            "preflight": "NO_EXTERNAL_OR_IMPORT_AUTHORIZATION_REQUIRED",
            "pubmed_execution": EXPECTED_AUTHORIZATION_FLAGS["pubmed"],
            "acm_provisional_import": EXPECTED_AUTHORIZATION_FLAGS["acm"],
            "llm_inference": EXPECTED_AUTHORIZATION_FLAGS["llm"],
        },
        "safeguards": {
            "frozen_query_text_modified": False,
            "raw_evidence_modified": False,
            "network_requests_made": 0,
            "acm_provisional_import_performed": False,
            "pubmed_execution_performed": False,
            "llm_inference_performed": False,
            "normalization_or_deduplication_performed": False,
            "prisma_or_corpus_effects": False,
            "prohibited_effects": config["prohibited_effects"],
        },
        "planned_outputs_after_separate_authorization": [
            "run_manifest.json",
            "raw_responses/",
            "provisional_dataset_envelope.json",
            "sample_manifest.json",
            "diagnostics.json",
            "jfr25_rediscovery.json",
            "screening/preflight.json",
            "screening/report.json",
            "screening/review_table.csv",
            "screening/invalid_response_queue.json",
            "screening/human_validation_sample.csv",
            "screening/human_validation_sample_manifest.json",
        ],
    }
    report["preflight_hash"] = _sha256_json(report)
    return report


def write_preflight(report: dict[str, Any], *, root: str | Path) -> Path:
    """Persist only the preflight inside its guarded ignored namespace."""

    root_path = Path(root).resolve()
    output = resolve_output_namespace(root_path, report["output_namespace"]["path"])
    path = output / "preflight.json"
    if path.exists():
        raise FileExistsError("preflight already exists; use a new provisional run namespace")
    encoded = (
        json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    atomic_write(path, encoded)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/star_provisional_pipeline_validation_v1.json"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    if not args.preflight:
        parser.error("only the offline --preflight boundary is enabled by this command")

    root = args.root.resolve()
    config = load_validation_config(
        args.config if args.config.is_absolute() else root / args.config
    )
    if args.output is not None:
        requested = args.output
        if requested.is_absolute():
            try:
                requested = requested.resolve().relative_to(root)
            except ValueError as exc:
                raise ProvisionalValidationError(
                    "output must be inside the repository"
                ) from exc
        if requested.as_posix() != config["output_namespace"]:
            raise ProvisionalValidationError("output must equal the configured namespace")
    report = build_preflight(root=root, config_path=args.config)
    path = write_preflight(report, root=root)
    print(path.relative_to(root).as_posix())
    print(report["preflight_hash"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
