"""Exact-hash, staging-only relocation of prior-survey source evidence."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import FunctionType
from typing import Any

from h2h_lit import prior_survey_integration
from h2h_lit.checkpoint import atomic_write

SCHEMA_VERSION = "1.0.0"
STATUS = "STAGING_ONLY_EXACT_SOURCE_RELOCATION"


class PriorSurveySourceRelocationError(RuntimeError):
    """Raised when a staging relocation changes an authorized source binding."""


@dataclass(frozen=True, slots=True)
class ValidatedSourceRelocations:
    manifest_path: Path
    manifest_sha256: str
    manifest: dict[str, Any]
    replacements: dict[str, dict[str, Any]]

    def binding(self, root: Path) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "status": STATUS,
            "manifest": _file_reference(self.manifest_path, root),
            "authoritative_execution_state_sha256": self.manifest[
                "authoritative_execution_state_sha256"
            ],
            "entry_count": len(self.replacements),
            "entries_sha256": _json_hash(self.manifest["relocations"]),
            "production_state_modified": False,
        }


@dataclass(frozen=True, slots=True)
class RelocationValidationBoundary:
    """Call-local validators whose private globals contain exact-path substitutions."""

    prior_survey_validator: Callable[..., None]
    execution_state_loader: Callable[..., dict[str, Any]]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _within_root(root: Path, value: str | Path) -> Path:
    raw = Path(value)
    path = (root / raw).resolve() if not raw.is_absolute() else raw.resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise PriorSurveySourceRelocationError(
            f"relocation path escapes repository: {value}"
        ) from exc
    return path


def _staging_path(root: Path, value: str | Path) -> Path:
    path = _within_root(root, value)
    relative = path.relative_to(root)
    if relative.parts[:2] != ("outputs", "staging"):
        raise PriorSurveySourceRelocationError(
            "source relocation must remain under outputs/staging"
        )
    return path


def _file_reference(path: Path, root: Path) -> dict[str, Any]:
    path = path.resolve()
    return {
        "path": path.relative_to(root).as_posix(),
        "path_scope": "repository_relative",
        "byte_size": path.stat().st_size,
        "raw_sha256": _sha256_file(path),
    }


def _verify_file(path: Path, reference: Mapping[str, Any]) -> None:
    if not path.is_file():
        raise PriorSurveySourceRelocationError(f"relocated evidence is missing: {path}")
    if path.stat().st_size != reference.get("byte_size"):
        raise PriorSurveySourceRelocationError(f"relocated evidence size changed: {path}")
    if _sha256_file(path) != reference.get("raw_sha256"):
        raise PriorSurveySourceRelocationError(f"relocated evidence hash changed: {path}")


def _authorized_absolute_references(
    *, root: Path, state: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for seed_set_id, registration in sorted(state.get("prior_survey_imports", {}).items()):
        manifest_relative = Path(str(registration.get("package_manifest_path") or ""))
        manifest_path = _within_root(root, manifest_relative)
        expected_manifest_sha256 = registration.get("package_manifest_sha256")
        if not manifest_path.is_file() or _sha256_file(manifest_path) != expected_manifest_sha256:
            raise PriorSurveySourceRelocationError(
                f"authorized {seed_set_id} package manifest changed"
            )
        package = json.loads(manifest_path.read_bytes())
        for field in (
            "source_artifacts",
            "source_package_bindings",
            "implementation_bindings",
        ):
            for reference in package.get(field, []):
                if reference.get("path_scope") != "absolute_read_only":
                    continue
                original_path = str(reference.get("path") or "")
                if not Path(original_path).is_absolute() or original_path in result:
                    raise PriorSurveySourceRelocationError(
                        "authorized absolute source references are invalid or duplicated"
                    )
                result[original_path] = {
                    "seed_set_id": seed_set_id,
                    "package_manifest_path": manifest_relative.as_posix(),
                    "package_manifest_sha256": expected_manifest_sha256,
                    "package_field": field,
                    "original_reference": dict(reference),
                }
    return result


def prepare_source_relocation_package(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Copy exact authorized bytes into a fresh, repository-relative staging package."""

    root_path = Path(root).resolve()
    output = _staging_path(root_path, output_dir)
    if output.exists():
        raise PriorSurveySourceRelocationError(
            "refusing to overwrite a source-relocation namespace"
        )
    absolute_references = _authorized_absolute_references(root=root_path, state=state)
    if not absolute_references:
        raise PriorSurveySourceRelocationError(
            "no authorized absolute source references require relocation"
        )
    evidence_dir = output / "source-evidence"
    evidence_dir.mkdir(parents=True)
    relocations: list[dict[str, Any]] = []
    try:
        for original_path, binding in sorted(absolute_references.items()):
            original_reference = binding["original_reference"]
            source = Path(original_path)
            _verify_file(source, original_reference)
            suffix = "".join(source.suffixes)
            destination = evidence_dir / (
                f"{binding['seed_set_id']}-{original_reference['raw_sha256']}{suffix}"
            )
            with source.open("rb") as input_handle, destination.open("xb") as output_handle:
                shutil.copyfileobj(input_handle, output_handle, length=1024 * 1024)
                output_handle.flush()
                os.fsync(output_handle.fileno())
            relocated_reference = _file_reference(destination, root_path)
            if (
                relocated_reference["byte_size"] != original_reference["byte_size"]
                or relocated_reference["raw_sha256"] != original_reference["raw_sha256"]
            ):
                raise PriorSurveySourceRelocationError(
                    "copied source evidence does not match its authorization"
                )
            relocations.append(
                {
                    **binding,
                    "relocated_reference": relocated_reference,
                }
            )
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "status": STATUS,
            "authoritative_execution_state_sha256": execution_state_raw_sha256,
            "relocations": relocations,
            "production_state_modified": False,
            "source_packages_modified": False,
            "source_evidence_rewritten": False,
        }
        manifest_path = output / "relocation_manifest.json"
        atomic_write(manifest_path, _json_bytes(manifest))
    except BaseException:
        shutil.rmtree(output, ignore_errors=True)
        raise
    return {
        "manifest": manifest,
        "manifest_path": manifest_path.relative_to(root_path).as_posix(),
        "manifest_sha256": _sha256_file(manifest_path),
    }


def validate_source_relocation_manifest(
    *,
    root: str | Path,
    state: Mapping[str, Any],
    execution_state_raw_sha256: str,
    manifest_path: str | Path,
    expected_manifest_sha256: str,
) -> ValidatedSourceRelocations:
    """Bind every authorized absolute reference to exactly identical staged bytes."""

    root_path = Path(root).resolve()
    path = _staging_path(root_path, manifest_path)
    if not path.is_file() or _sha256_file(path) != expected_manifest_sha256:
        raise PriorSurveySourceRelocationError("source-relocation manifest hash changed")
    manifest = json.loads(path.read_bytes())
    if (
        manifest.get("schema_version") != SCHEMA_VERSION
        or manifest.get("status") != STATUS
        or manifest.get("authoritative_execution_state_sha256") != execution_state_raw_sha256
        or manifest.get("production_state_modified") is not False
        or manifest.get("source_packages_modified") is not False
        or manifest.get("source_evidence_rewritten") is not False
    ):
        raise PriorSurveySourceRelocationError("source-relocation provenance changed")
    authorized = _authorized_absolute_references(root=root_path, state=state)
    observed: dict[str, dict[str, Any]] = {}
    for entry in manifest.get("relocations", []):
        original_reference = entry.get("original_reference", {})
        original_path = str(original_reference.get("path") or "")
        if original_path in observed or original_path not in authorized:
            raise PriorSurveySourceRelocationError(
                "source-relocation inventory contains an unexpected reference"
            )
        expected = authorized[original_path]
        if any(entry.get(key) != expected[key] for key in expected):
            raise PriorSurveySourceRelocationError(
                "source-relocation authorization binding changed"
            )
        relocated = entry.get("relocated_reference", {})
        if relocated.get("path_scope") != "repository_relative":
            raise PriorSurveySourceRelocationError("relocated evidence must be repository-relative")
        relocated_path = _staging_path(root_path, str(relocated.get("path") or ""))
        _verify_file(relocated_path, relocated)
        if relocated.get("byte_size") != original_reference.get("byte_size") or relocated.get(
            "raw_sha256"
        ) != original_reference.get("raw_sha256"):
            raise PriorSurveySourceRelocationError(
                "relocated evidence differs from the authorized source bytes"
            )
        observed[original_path] = dict(relocated)
    if set(observed) != set(authorized):
        raise PriorSurveySourceRelocationError("source-relocation inventory is incomplete")
    return ValidatedSourceRelocations(
        manifest_path=path,
        manifest_sha256=expected_manifest_sha256,
        manifest=manifest,
        replacements=observed,
    )


def _function_with_isolated_globals(
    function: Callable[..., Any], replacements: Mapping[str, Any]
) -> Callable[..., Any]:
    """Clone one function with a private globals mapping; never mutate its module."""

    isolated_globals = dict(function.__globals__)
    isolated_globals.update(replacements)
    cloned = FunctionType(
        function.__code__,
        isolated_globals,
        name=function.__name__,
        argdefs=function.__defaults__,
        closure=function.__closure__,
    )
    cloned.__kwdefaults__ = function.__kwdefaults__
    cloned.__annotations__ = dict(function.__annotations__)
    return cloned


def relocation_validation_boundary(
    relocations: ValidatedSourceRelocations,
) -> RelocationValidationBoundary:
    """Build isolated validators for one provenance-bound staging operation.

    Only the cloned call graph sees the exact authorized path replacements. The
    imported modules, ``pathlib``, and builtins are not patched or mutated.
    """

    original_verifier = prior_survey_integration._verify_source_artifact

    def verify_with_relocation(root: Path, reference: Mapping[str, Any]) -> None:
        original_path = str(reference.get("path") or "")
        replacement = relocations.replacements.get(original_path)
        if reference.get("path_scope") == "absolute_read_only" and replacement:
            if reference.get("byte_size") != replacement.get("byte_size") or reference.get(
                "raw_sha256"
            ) != replacement.get("raw_sha256"):
                raise PriorSurveySourceRelocationError(
                    "runtime source reference differs from relocation authorization"
                )
            original_verifier(root, replacement)
            return
        original_verifier(root, reference)

    package_validator = _function_with_isolated_globals(
        prior_survey_integration.validate_prior_survey_package,
        {"_verify_source_artifact": verify_with_relocation},
    )
    prior_validator = _function_with_isolated_globals(
        prior_survey_integration.validate_authorized_prior_survey_imports,
        {"validate_prior_survey_package": package_validator},
    )

    # Imported lazily to avoid a module cycle: external_retrieval_wave imports
    # prior_survey_integration and the global orchestrator imports this module.
    from h2h_lit import external_retrieval_wave

    state_loader = _function_with_isolated_globals(
        external_retrieval_wave._load_execution_state,
        {"validate_authorized_prior_survey_imports": prior_validator},
    )
    return RelocationValidationBoundary(
        prior_survey_validator=prior_validator,
        execution_state_loader=state_loader,
    )
