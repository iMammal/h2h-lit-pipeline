from __future__ import annotations

import builtins
import hashlib
import json
from pathlib import Path

import pytest

from h2h_lit import external_retrieval_wave, prior_survey_integration
from h2h_lit.prior_survey_source_relocation import (
    PriorSurveySourceRelocationError,
    prepare_source_relocation_package,
    relocation_validation_boundary,
    validate_source_relocation_manifest,
)


def _fixture(tmp_path: Path) -> tuple[dict, Path, dict, bytes]:
    source = tmp_path / "outside-package" / "author-source.xlsx"
    source.parent.mkdir()
    source_bytes = b"unchanged author-supplied source evidence\n"
    source.write_bytes(source_bytes)
    original_reference = {
        "path": str(source),
        "path_scope": "absolute_read_only",
        "byte_size": len(source_bytes),
        "raw_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "role": "author-supplied-workbook",
    }
    package_path = tmp_path / "outputs/staging/prior-package/EBK25/v1/package_manifest.json"
    package_path.parent.mkdir(parents=True)
    package = {
        "seed_set_id": "EBK25",
        "source_artifacts": [original_reference],
        "source_package_bindings": [],
        "implementation_bindings": [],
    }
    package_bytes = (json.dumps(package, sort_keys=True) + "\n").encode()
    package_path.write_bytes(package_bytes)
    state = {
        "prior_survey_imports": {
            "EBK25": {
                "package_manifest_path": package_path.relative_to(tmp_path).as_posix(),
                "package_manifest_sha256": hashlib.sha256(package_bytes).hexdigest(),
            }
        }
    }
    return state, source, original_reference, package_bytes


def _prepare(tmp_path: Path):
    state, source, original_reference, package_bytes = _fixture(tmp_path)
    state_before = json.dumps(state, sort_keys=True)
    output = tmp_path / "outputs/staging/source-relocation-v1"
    result = prepare_source_relocation_package(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
        output_dir=output,
    )
    assert json.dumps(state, sort_keys=True) == state_before
    package_path = tmp_path / state["prior_survey_imports"]["EBK25"]["package_manifest_path"]
    assert package_path.read_bytes() == package_bytes
    validated = validate_source_relocation_manifest(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
        manifest_path=result["manifest_path"],
        expected_manifest_sha256=result["manifest_sha256"],
    )
    return state, source, original_reference, output, result, validated


def test_preparation_preserves_packages_and_binds_exact_source_bytes(tmp_path):
    state, source, original, _output, _result, validated = _prepare(tmp_path)

    assert len(validated.replacements) == 1
    replacement = validated.replacements[str(source)]
    assert replacement["byte_size"] == original["byte_size"]
    assert replacement["raw_sha256"] == original["raw_sha256"]
    assert replacement["path_scope"] == "repository_relative"
    assert validated.binding(tmp_path)["production_state_modified"] is False
    assert state["prior_survey_imports"]["EBK25"]["package_manifest_sha256"]


def test_call_local_validation_uses_copy_without_patching_process_state(tmp_path):
    _state, source, original, _output, _result, validated = _prepare(tmp_path)
    unavailable = source.with_suffix(".unavailable")
    source.rename(unavailable)
    verifier_before = prior_survey_integration._verify_source_artifact
    loader_before = external_retrieval_wave._load_execution_state
    path_open_before = Path.open
    builtin_open_before = builtins.open

    with pytest.raises(prior_survey_integration.PriorSurveyIntegrationError):
        prior_survey_integration._verify_source_artifact(tmp_path, original)
    boundary = relocation_validation_boundary(validated)

    package_validator = boundary.prior_survey_validator.__globals__["validate_prior_survey_package"]
    relocated_verifier = package_validator.__globals__["_verify_source_artifact"]
    relocated_verifier(tmp_path, original)
    assert prior_survey_integration._verify_source_artifact is verifier_before
    assert external_retrieval_wave._load_execution_state is loader_before
    assert Path.open is path_open_before
    assert builtins.open is builtin_open_before
    with pytest.raises(prior_survey_integration.PriorSurveyIntegrationError):
        prior_survey_integration._verify_source_artifact(tmp_path, original)


def test_call_local_validation_keeps_unmapped_paths_strict_and_needs_no_restoration(
    tmp_path,
):
    _state, _source, original, _output, _result, validated = _prepare(tmp_path)
    boundary = relocation_validation_boundary(validated)
    package_validator = boundary.prior_survey_validator.__globals__["validate_prior_survey_package"]
    relocated_verifier = package_validator.__globals__["_verify_source_artifact"]
    strict_verifier = prior_survey_integration._verify_source_artifact
    strict_loader = external_retrieval_wave._load_execution_state
    unmapped = {**original, "path": str(tmp_path / "never-authorized.xlsx")}

    with pytest.raises(prior_survey_integration.PriorSurveyIntegrationError):
        relocated_verifier(tmp_path, unmapped)
    assert prior_survey_integration._verify_source_artifact is strict_verifier

    bad_mapped = {**original, "raw_sha256": "0" * 64}
    with pytest.raises(PriorSurveySourceRelocationError, match="differs"):
        relocated_verifier(tmp_path, bad_mapped)
    assert prior_survey_integration._verify_source_artifact is strict_verifier
    assert external_retrieval_wave._load_execution_state is strict_loader


def test_relocation_rejects_tampered_copy_and_manifest(tmp_path):
    state, _source, _original, output, result, validated = _prepare(tmp_path)
    relocated = tmp_path / next(iter(validated.replacements.values()))["path"]
    relocated.write_bytes(b"tampered")
    with pytest.raises(PriorSurveySourceRelocationError, match="size changed|hash changed"):
        validate_source_relocation_manifest(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            manifest_path=result["manifest_path"],
            expected_manifest_sha256=result["manifest_sha256"],
        )

    manifest_path = output / "relocation_manifest.json"
    manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
    with pytest.raises(PriorSurveySourceRelocationError, match="manifest hash changed"):
        validate_source_relocation_manifest(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            manifest_path=result["manifest_path"],
            expected_manifest_sha256=result["manifest_sha256"],
        )


def test_relocation_rejects_state_and_authorization_drift(tmp_path):
    state, _source, _original, _output, result, _validated = _prepare(tmp_path)
    with pytest.raises(PriorSurveySourceRelocationError, match="provenance changed"):
        validate_source_relocation_manifest(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="different-state",
            manifest_path=result["manifest_path"],
            expected_manifest_sha256=result["manifest_sha256"],
        )

    package_path = tmp_path / state["prior_survey_imports"]["EBK25"]["package_manifest_path"]
    package_path.write_bytes(package_path.read_bytes() + b" ")
    with pytest.raises(PriorSurveySourceRelocationError, match="package manifest changed"):
        validate_source_relocation_manifest(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            manifest_path=result["manifest_path"],
            expected_manifest_sha256=result["manifest_sha256"],
        )


def test_relocation_namespace_is_fresh_and_staging_only(tmp_path):
    state, _source, _original, _package = _fixture(tmp_path)
    outside = tmp_path / "portable-evidence"
    with pytest.raises(PriorSurveySourceRelocationError, match="outputs/staging"):
        prepare_source_relocation_package(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            output_dir=outside,
        )

    output = tmp_path / "outputs/staging/source-relocation-v1"
    output.mkdir(parents=True)
    with pytest.raises(PriorSurveySourceRelocationError, match="overwrite"):
        prepare_source_relocation_package(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            output_dir=output,
        )
