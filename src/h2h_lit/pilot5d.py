"""Stage 5D eligibility-only controlled pilot, isolated from taxonomy coding."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from h2h_lit import inference as inference_core
from h2h_lit.pilot5b import (
    _MODEL_CRITERIA,
    _derive_e6,
    _derive_exclusion_reasons,
    _parse_quote_evidence,
)
from h2h_lit.pilot5c import (
    _GENERIC_EVIDENCE,
    _SUBSTANTIVE_CRITERIA,
    original_source_artifact_path_for,
    pilot5c_inference_input_for,
    pilot5c_response_schema,
)
from h2h_lit.review import EligibilityCriterion, ScreeningStage, TriState
from h2h_lit.screening import derive_eligibility_status

PILOT5D_PROMPT = Path(
    "prompts/revised_star_title_abstract_screening_pilot5d_v1_3_0.md"
)
PILOT5D_PROMPT_VERSION = "1.3.0"
PILOT5D_OUTPUT_SCHEMA_VERSION = "1.3.0"
PILOT5D_CONFIG = Path("config/stage5d_pilot_v1.json")
PILOT5D_OUTPUT_DIR = Path("outputs/stage5d_eligibility_only_v1")


def load_pilot5d_config(path: str | Path = PILOT5D_CONFIG) -> dict[str, Any]:
    resolved = inference_core._resolve_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    for key, expected in (
        ("config_version", PILOT5D_PROMPT_VERSION),
        ("prompt_version", PILOT5D_PROMPT_VERSION),
        ("output_schema_version", PILOT5D_OUTPUT_SCHEMA_VERSION),
    ):
        if payload.get(key) != expected:
            raise ValueError(f"unsupported Pilot 5D {key}")
    return payload


def pilot5d_inference_input_for(
    record: Any,
    *,
    pilot_execution_date: str,
) -> inference_core.InferenceInput:
    """Preserve 5C's field-addressable, opaque-provenance eligibility input."""

    base = pilot5c_inference_input_for(
        record, pilot_execution_date=pilot_execution_date
    )
    metadata = dict(base.metadata)
    metadata["input_policy"] = "pilot5d_eligibility_only_neutral_provenance"
    return replace(base, metadata=metadata)


def parse_pilot5d_proposal(
    payload: dict[str, Any],
    inference_input: inference_core.InferenceInput,
) -> inference_core._ParsedProposal:
    """Validate only E1-E5/E7 and derive all downstream eligibility state in software."""

    inference_core._require_exact_keys(
        payload,
        {"criteria", "overall_rationale"},
        "response",
    )
    criteria_payload = inference_core._require_dict(payload["criteria"], "criteria")
    by_value = {criterion.value: criterion for criterion in _MODEL_CRITERIA}
    inference_core._require_exact_keys(criteria_payload, set(by_value), "criteria")

    values: dict[EligibilityCriterion, TriState] = {}
    evidence: dict[
        EligibilityCriterion, tuple[inference_core._EvidenceSpan, ...]
    ] = {}
    rationales: dict[EligibilityCriterion, str] = {}
    for value, criterion in by_value.items():
        item = inference_core._require_dict(criteria_payload[value], value)
        inference_core._require_exact_keys(
            item, {"decision", "certainty", "evidence", "rationale"}, value
        )
        decision = inference_core._strict_enum(
            TriState, item["decision"], f"{value}.decision"
        )
        inference_core._validate_certainty(decision.value, item["certainty"], value)
        if (
            criterion is EligibilityCriterion.EVIDENCE_SUFFICIENCY
            and decision is TriState.NO
        ):
            raise ValueError(
                "E7 cannot be NO in a machine title/abstract proposal; use UNCERTAIN "
                "and escalate to full text"
            )
        values[criterion] = decision
        evidence[criterion] = _parse_quote_evidence(
            item["evidence"], inference_input, value, required=True
        )
        rationales[criterion] = inference_core._require_rationale(
            item["rationale"], value
        )

    e6_value, e6_span, e6_rationale, e6_reasons = _derive_e6(inference_input)
    values[EligibilityCriterion.ADMINISTRATIVE_SCOPE] = e6_value
    evidence[EligibilityCriterion.ADMINISTRATIVE_SCOPE] = (e6_span,)
    rationales[EligibilityCriterion.ADMINISTRATIVE_SCOPE] = e6_rationale
    reasons = _derive_exclusion_reasons(values, e6_reasons)
    derive_eligibility_status(values)
    return inference_core._ParsedProposal(
        criterion_values=values,
        criterion_evidence=evidence,
        criterion_rationales=rationales,
        assistance_modes=(),
        visualization_modalities=(),
        tasks=(),
        primary_exclusion_reason=reasons[0] if reasons else None,
        secondary_exclusion_reasons=tuple(reasons[1:]),
        overall_rationale=inference_core._require_rationale(
            payload["overall_rationale"], "overall_rationale"
        ),
        audit_flags=_eligibility_audit_flags(evidence),
    )


def _eligibility_audit_flags(
    evidence: dict[EligibilityCriterion, tuple[inference_core._EvidenceSpan, ...]],
) -> tuple[dict[str, Any], ...]:
    """Keep the 5C eligibility-evidence audits without taxonomy-dependent flags."""

    flags: list[dict[str, Any]] = []
    signatures: dict[tuple[str | None, int, int, str], list[str]] = {}
    for criterion in _MODEL_CRITERIA:
        for span in evidence[criterion]:
            signatures.setdefault(
                (span.source_field, span.start, span.end, span.quote), []
            ).append(criterion.value)
    reused = [
        {"criteria": sorted(labels), "quote": signature[3], "source_field": signature[0]}
        for signature, labels in signatures.items()
        if len(set(labels)) >= 3
    ]
    if reused:
        flags.append(
            {
                "code": "AUDIT_CROSS_CRITERION_EVIDENCE_REUSE",
                "message": "One evidence span supports several criterion decisions; review substantive fit.",
                "instances": sorted(reused, key=lambda item: (item["quote"], item["criteria"])),
            }
        )

    generic: list[dict[str, str]] = []
    for criterion in _SUBSTANTIVE_CRITERIA:
        for span in evidence[criterion]:
            normalized = " ".join(span.quote.casefold().split()).strip(" .,:;!?()[]{}\"'")
            if len(span.quote.strip()) <= 24 or normalized in _GENERIC_EVIDENCE:
                generic.append({"criterion": criterion.value, "quote": span.quote})
    if generic:
        flags.append(
            {
                "code": "AUDIT_GENERIC_OR_SHORT_CRITERION_EVIDENCE",
                "message": "Very short or generic evidence requires human review for criterion-specific support.",
                "instances": sorted(generic, key=lambda item: (item["criterion"], item["quote"])),
            }
        )
    return tuple(flags)


def pilot5d_response_schema() -> dict[str, Any]:
    """Schema 1.3.0 deliberately accepts eligibility evidence only."""

    schema = pilot5c_response_schema()
    for key in ("assistance_modes", "visualization_modalities", "tasks"):
        schema["properties"].pop(key)
    schema["required"] = [
        key
        for key in schema["required"]
        if key not in {"assistance_modes", "visualization_modalities", "tasks"}
    ]
    return schema


def prepare_pilot5d(
    *,
    config_path: str | Path = PILOT5D_CONFIG,
    historical_root: str | Path | None = None,
    output_dir: str | Path = PILOT5D_OUTPUT_DIR,
    created_at: str | None = None,
) -> tuple[Any, dict[str, Any], dict[str, Any], Any]:
    """Create an isolated no-call eligibility-only pilot preflight."""

    from h2h_lit.models import utc_now_iso
    from h2h_lit.pilot import PilotPaths, build_pilot_dataset, load_pilot_config
    from h2h_lit.retrieval import save_review_dataset

    config = load_pilot5d_config(config_path)
    timestamp = created_at or utc_now_iso()
    dataset, manifest = build_pilot_dataset(
        config_path=config["selection_config"],
        historical_root=historical_root,
        created_at=timestamp,
    )
    manifest.update(
        {
            "pilot_version": PILOT5D_PROMPT_VERSION,
            "selection_config_unchanged_from_stage5a": config["selection_config"],
            "pilot5d_execution_controls": dict(config["execution_controls"]),
        }
    )
    paths = PilotPaths.under(output_dir)
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    save_review_dataset(paths.review_dataset, dataset)
    _write_json(paths.selection_manifest, manifest)
    prompt = inference_core.load_prompt_artifact(
        config["prompt_path"],
        stage=ScreeningStage.TITLE_ABSTRACT,
        version=config["prompt_version"],
        output_schema_version=config["output_schema_version"],
    )
    base = load_pilot_config(config["selection_config"])
    model = json.loads(json.dumps(base["model"]))
    model["parameters"].update(config["model_parameter_overrides"])
    retries = int(base["retry_limit_per_record"])
    preflight = {
        "pilot_version": config["config_version"],
        "provider": model["provider"],
        "model": model["name"],
        "parameters": model["parameters"],
        "prompt": {
            "path": prompt.path,
            "version": prompt.version,
            "sha256": prompt.content_hash,
            "output_schema_version": prompt.output_schema_version,
        },
        "selection_config": config["selection_config"],
        "canonical_records": len(dataset.canonical_records),
        "maximum_model_calls": len(dataset.canonical_records) * (retries + 1),
        "execution_controls": config["execution_controls"],
        "output_paths": {
            "review_dataset": str(paths.review_dataset),
            "selection_manifest": str(paths.selection_manifest),
            "report": str(paths.report),
            "review_table": str(paths.review_table),
        },
        "live_calls_made": 0,
    }
    return dataset, manifest, preflight, paths


def run_live_pilot5d(
    *,
    provider: inference_core.InferenceProvider,
    config_path: str | Path = PILOT5D_CONFIG,
    historical_root: str | Path | None = None,
    output_dir: str | Path = PILOT5D_OUTPUT_DIR,
    created_at: str | None = None,
) -> tuple[Any, dict[str, Any], Any]:
    """Execute a future 5D run only when a caller explicitly supplies a provider."""

    from h2h_lit.models import utc_now_iso
    from h2h_lit.openai_provider import OpenAIResponsesProvider
    from h2h_lit.pilot import (
        _retry_disposition,
        _stable_id,
        _write_json as write_json,
        _write_review_table,
        build_pilot_report,
        load_pilot_config,
    )
    from h2h_lit.retrieval import save_review_dataset

    timestamp = created_at or utc_now_iso()
    dataset, manifest, _, paths = prepare_pilot5d(
        config_path=config_path,
        historical_root=historical_root,
        output_dir=output_dir,
        created_at=timestamp,
    )
    config = load_pilot5d_config(config_path)
    base = load_pilot_config(config["selection_config"])
    model = json.loads(json.dumps(base["model"]))
    model["parameters"].update(config["model_parameter_overrides"])
    run = inference_core.register_inference_run(
        dataset,
        stage=ScreeningStage.TITLE_ABSTRACT,
        prompt_path=config["prompt_path"],
        provider=model["provider"],
        model=model["name"],
        parameters=model["parameters"],
        created_at=timestamp,
        prompt_version=config["prompt_version"],
        output_schema_version=config["output_schema_version"],
        run_id=_stable_id("inference-run-pilot5d", manifest["config_hash"], timestamp),
    )
    save_review_dataset(paths.review_dataset, dataset)
    retry_limit = int(base["retry_limit_per_record"])
    for record in dataset.canonical_records:
        inference_input = pilot5d_inference_input_for(
            record, pilot_execution_date=timestamp[:10]
        )
        for _ in range(retry_limit + 1):
            attempt = inference_core.run_inference_attempt(
                dataset,
                run_id=run.run_id,
                inference_input=inference_input,
                provider=provider,
            )
            if isinstance(provider, OpenAIResponsesProvider):
                attempt.metadata["provider_response"] = provider.metadata_for(
                    attempt.request_id, attempt.attempt_number
                )
            attempt.metadata["source_provenance"] = {
                "opaque_artifact_id": inference_input.source_artifact_id,
                "original_artifact_path": original_source_artifact_path_for(record),
                "model_input_path_redacted": True,
            }
            retryable, retry_reason = _retry_disposition(attempt)
            retry_permitted = retryable and attempt.attempt_number <= retry_limit
            attempt.metadata["pilot5d_execution_controls"] = {
                **config["execution_controls"],
                "pilot_administrative_observation_cutoff": timestamp[:10],
                "retry_condition_met": retryable,
                "retry_permitted_by_budget": retry_permitted,
                "retry_reason": retry_reason,
            }
            save_review_dataset(paths.review_dataset, dataset)
            if not retry_permitted:
                break
    report_config = json.loads(json.dumps(base))
    report_config["config_version"] = config["config_version"]
    report_config["model"] = model
    report = build_pilot_report(dataset, config=report_config, manifest=manifest)
    write_json(paths.report, report)
    _write_review_table(paths.review_table, report["review_rows"])
    return dataset, report, paths


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
