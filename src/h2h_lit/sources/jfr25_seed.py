"""Deterministic extraction of the JFR25 author-companion corpus."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.normalize import normalize_doi

EXPECTED_APPLICATION_COUNT = 87
EXPECTED_STUDY_COUNT = 59
EXPECTED_SHARED_IDS = ("7", "22", "29", "35", "44", "52", "53", "93")
EXPECTED_RAW_ROW_COUNT = 146
EXPECTED_UNIQUE_MEMBER_COUNT = 138

_ARRAY_PATTERN = re.compile(r"e\.v\(JSON\.parse\(('(?:\\.|[^'\\])*')\)\)")


class JFR25ExtractionError(ValueError):
    """Raised when the frozen companion artifact does not meet its contract."""


def extract_companion_arrays(bundle: bytes) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract the two source arrays without interpreting their classifications."""

    text = bundle.decode("utf-8")
    decoded: list[list[dict[str, Any]]] = []
    for match in _ARRAY_PATTERN.finditer(text):
        serialized = ast.literal_eval(match.group(1))
        value = json.loads(serialized)
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            decoded.append(value)

    applications = [items for items in decoded if items and "tool_name" in items[0]]
    studies = [items for items in decoded if items and "study_focus" in items[0]]
    if len(applications) != 1 or len(studies) != 1:
        raise JFR25ExtractionError("expected one application and one study array")
    _validate_membership(applications[0], studies[0])
    return applications[0], studies[0]


def build_raw_rows_artifact(
    applications: list[dict[str, Any]],
    studies: list[dict[str, Any]],
    *,
    source_bundle_sha256: str,
) -> dict[str, Any]:
    """Preserve all category rows in their original source order."""

    _validate_membership(applications, studies)
    rows = [
        {
            "category": category,
            "category_ordinal": ordinal,
            "source_id": str(item["id"]),
            "source_record": item,
        }
        for category, items in (("application", applications), ("study", studies))
        for ordinal, item in enumerate(items, start=1)
    ]
    payload = {
        "schema_version": "1.0.0",
        "artifact_id": "jfr25-author-companion-raw-category-rows",
        "source_bundle_sha256": source_bundle_sha256,
        "reconciliation": _reconciliation(applications, studies),
        "raw_category_rows": rows,
    }
    payload["artifact_hash"] = _artifact_hash(payload)
    return payload


def build_seed_manifest(
    applications: list[dict[str, Any]],
    studies: list[dict[str, Any]],
    *,
    acquired_at: str,
    source_artifacts: list[dict[str, Any]],
    raw_rows_reference: dict[str, Any],
) -> dict[str, Any]:
    """Build the populated prerequisite manifest without creating occurrences."""

    _validate_membership(applications, studies)
    by_id: dict[str, list[dict[str, Any]]] = {}
    member_order: list[str] = []
    for category, items in (("application", applications), ("study", studies)):
        for category_ordinal, item in enumerate(items, start=1):
            source_id = str(item["id"])
            if source_id not in by_id:
                member_order.append(source_id)
                by_id[source_id] = []
            by_id[source_id].append(
                {
                    "category": category,
                    "category_ordinal": category_ordinal,
                    "source_record": item,
                }
            )

    entries = []
    for ordinal, source_id in enumerate(member_order, start=1):
        source_rows = by_id[source_id]
        first = source_rows[0]["source_record"]
        raw_doi = _string_or_none(first.get("doi"))
        citation_fields = {
            "authors": first.get("authors"),
            "doi": first.get("doi"),
            "title": first.get("title"),
            "year": first.get("year"),
        }
        entries.append(
            {
                "entry_id": f"jfr25-source-{source_id}",
                "ordinal": ordinal,
                "source_member_id": source_id,
                "source_category_membership": [row["category"] for row in source_rows],
                "source_bibliography_tags": [
                    row["source_record"].get("paper_bib_tag") for row in source_rows
                ],
                "raw_citation": _canonical_json(citation_fields),
                "raw_citation_format": "canonical_json_source_fields",
                "title": first.get("title"),
                "raw_authors": first.get("authors"),
                "year": first.get("year"),
                "raw_doi": raw_doi,
                "doi": normalize_doi(raw_doi),
                "source_rows": source_rows,
            }
        )

    payload: dict[str, Any] = {
        "schema_version": "1.0.0",
        "artifact_id": "star-seed-jfr25-v1",
        "artifact_version": "1.1.0",
        "status": "POPULATED_VALIDATED_NOT_IMPORTED",
        "source_role": "prior_survey_seed",
        "seed_set_id": "JFR25",
        "seed_set_version": "1.0.0-prospective",
        "originating_review": {
            "citation": (
                "Lucas Joos et al. Visual Network Analysis in Immersive "
                "Environments: A Survey. arXiv:2501.08500."
            ),
            "doi": "10.48550/arXiv.2501.08500",
            "arxiv_id": "2501.08500",
            "repository_recovery_state": "RECOVERED_FROM_AUTHOR_COMPANION_RESOURCE",
        },
        "curator": {
            "operator_id": None,
            "required_for_manual_membership_changes": True,
            "state": "DETERMINISTIC_AUTHOR_SOURCE_EXTRACTION_COMPLETE",
            "membership_inference_performed": False,
        },
        "extraction_method": (
            "exact extraction of author-site application and study JSON arrays; "
            "membership reconciled only by explicit source id"
        ),
        "source_artifact": next(
            item for item in source_artifacts if item["artifact_role"] == "membership_bundle"
        ),
        "source_artifacts": source_artifacts,
        "raw_category_rows_artifact": raw_rows_reference,
        "source_version": {
            "companion_site_reported_last_update": "2025-12-14",
            "arxiv_v1_submitted": "2025-01-15",
            "arxiv_v2_submitted": "2025-09-10",
            "qualification": (
                "The acquired companion site reports a later update than arXiv v1; "
                "the 138-member site corpus is not asserted to be byte-identical to "
                "the arXiv v1 corpus."
            ),
        },
        "acquired_at": acquired_at,
        "expected_entry_count": EXPECTED_UNIQUE_MEMBER_COUNT,
        "completeness_state": "POPULATED_VALIDATED",
        "reconciliation": _reconciliation(applications, studies),
        "entries": entries,
        "entry_schema": {
            "required": [
                "entry_id",
                "ordinal",
                "source_member_id",
                "raw_citation",
                "source_category_membership",
                "source_rows",
            ],
            "normalized_fields": ["doi"],
            "eligibility_or_taxonomy_fields_permitted": False,
        },
        "licensing": {
            "explicit_corpus_data_redistribution_license_observed": False,
            "redistribution_permission": "UNRESOLVED",
            "internal_provenance_preservation": "PRESERVED_FOR_REPRODUCIBILITY",
        },
        "import_allowed": True,
        "occurrences_created": 0,
        "artifact_hash": None,
    }
    payload["artifact_hash"] = _artifact_hash(payload)
    return payload


def artifact_record(
    *,
    artifact_role: str,
    url: str,
    retrieved_at_utc: str,
    body_path: Path,
    headers_path: Path,
    root: Path,
) -> dict[str, Any]:
    """Describe one already-acquired raw response without issuing network traffic."""

    body = body_path.read_bytes()
    headers = headers_path.read_bytes()
    status, response_headers = _parse_final_headers(headers)
    return {
        "artifact_role": artifact_role,
        "url": url,
        "retrieved_at_utc": retrieved_at_utc,
        "http_status": status,
        "mime_type": _header_value(response_headers, "content-type"),
        "byte_size": len(body),
        "raw_sha256": _sha256(body),
        "relative_path": _relative(body_path, root),
        "response_headers": response_headers,
        "headers_artifact": {
            "relative_path": _relative(headers_path, root),
            "byte_size": len(headers),
            "raw_sha256": _sha256(headers),
        },
    }


def _validate_membership(
    applications: list[dict[str, Any]], studies: list[dict[str, Any]]
) -> None:
    if len(applications) != EXPECTED_APPLICATION_COUNT:
        raise JFR25ExtractionError("application row count changed")
    if len(studies) != EXPECTED_STUDY_COUNT:
        raise JFR25ExtractionError("study row count changed")
    application_ids = [str(item.get("id")) for item in applications]
    study_ids = [str(item.get("id")) for item in studies]
    if len(set(application_ids)) != len(application_ids):
        raise JFR25ExtractionError("duplicate source id within application array")
    if len(set(study_ids)) != len(study_ids):
        raise JFR25ExtractionError("duplicate source id within study array")
    shared = tuple(sorted(set(application_ids) & set(study_ids), key=int))
    if shared != EXPECTED_SHARED_IDS:
        raise JFR25ExtractionError("shared source IDs changed")
    if len(set(application_ids) | set(study_ids)) != EXPECTED_UNIQUE_MEMBER_COUNT:
        raise JFR25ExtractionError("unique corpus count changed")


def _reconciliation(
    applications: list[dict[str, Any]], studies: list[dict[str, Any]]
) -> dict[str, Any]:
    application_ids = {str(item["id"]) for item in applications}
    study_ids = {str(item["id"]) for item in studies}
    shared = sorted(application_ids & study_ids, key=int)
    return {
        "application_rows": len(applications),
        "study_rows": len(studies),
        "raw_category_rows": len(applications) + len(studies),
        "shared_source_id_count": len(shared),
        "shared_source_ids": shared,
        "unique_members": len(application_ids | study_ids),
        "identity_key": "companion_site_explicit_source_id",
        "fuzzy_or_title_matching_used": False,
        "equation": "87 + 59 - 8 = 138",
    }


def _parse_final_headers(raw: bytes) -> tuple[int, list[dict[str, str]]]:
    blocks = re.split(rb"\r?\n\r?\n", raw.strip())
    block = next((item for item in reversed(blocks) if item.startswith(b"HTTP/")), b"")
    lines = block.decode("iso-8859-1").splitlines()
    if not lines:
        raise JFR25ExtractionError("HTTP response headers are missing")
    parts = lines[0].split()
    if len(parts) < 2 or not parts[1].isdigit():
        raise JFR25ExtractionError("HTTP status is malformed")
    headers = []
    for line in lines[1:]:
        if not line or ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers.append({"name": name.strip().lower(), "value": value.strip()})
    return int(parts[1]), headers


def _header_value(headers: list[dict[str, str]], name: str) -> str | None:
    values = [item["value"] for item in headers if item["name"] == name]
    return values[-1] if values else None


def _relative(path: Path, root: Path) -> str:
    resolved = path.resolve().relative_to(root.resolve())
    if ".." in resolved.parts:
        raise JFR25ExtractionError("artifact path is not traversal-safe")
    return resolved.as_posix()


def _string_or_none(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _artifact_hash(payload: dict[str, Any]) -> str:
    material = dict(payload)
    material.pop("artifact_hash", None)
    return _sha256(_canonical_json(material).encode("utf-8"))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _pretty_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode()


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--raw-rows-output", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    parser.add_argument("--acquired-at", required=True)
    parser.add_argument("--artifact-spec", action="append", default=[])
    args = parser.parse_args()

    root = args.root.resolve()
    applications, studies = extract_companion_arrays(args.bundle.read_bytes())
    bundle_sha = _sha256(args.bundle.read_bytes())
    raw_rows = build_raw_rows_artifact(
        applications, studies, source_bundle_sha256=bundle_sha
    )
    atomic_write(args.raw_rows_output, _pretty_json(raw_rows))

    artifacts = []
    for serialized in args.artifact_spec:
        spec = json.loads(serialized)
        artifacts.append(
            artifact_record(
                artifact_role=spec["artifact_role"],
                url=spec["url"],
                retrieved_at_utc=spec["retrieved_at_utc"],
                body_path=root / spec["relative_path"],
                headers_path=root / spec["headers_relative_path"],
                root=root,
            )
        )
    raw_rows_bytes = args.raw_rows_output.read_bytes()
    raw_rows_reference = {
        "relative_path": _relative(args.raw_rows_output, root),
        "raw_sha256": _sha256(raw_rows_bytes),
        "canonical_hash": raw_rows["artifact_hash"],
        "byte_size": len(raw_rows_bytes),
    }
    manifest = build_seed_manifest(
        applications,
        studies,
        acquired_at=args.acquired_at,
        source_artifacts=artifacts,
        raw_rows_reference=raw_rows_reference,
    )
    atomic_write(args.manifest_output, _pretty_json(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
