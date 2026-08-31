"""Pilot 5B evidence repair and deterministic administrative screening controls."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from h2h_lit import inference as inference_core
from h2h_lit.review import (
    AnnotationState,
    AssistanceMode,
    EligibilityCriterion,
    EvidenceSource,
    ExclusionReason,
    ScreeningStage,
    TaskCategory,
    TriState,
    VisualizationModality,
)
from h2h_lit.screening import (
    EXCLUSION_REASON_ORDER,
    REASON_CRITERIA,
    derive_eligibility_status,
)

PILOT5B_PROMPT = Path(
    "prompts/revised_star_title_abstract_screening_pilot5b_v1_1_0.md"
)
PILOT5B_PROMPT_VERSION = "1.1.0"
PILOT5B_OUTPUT_SCHEMA_VERSION = "1.1.0"
PILOT5B_CONFIG = Path("config/stage5b_pilot_v1.json")
PILOT5B_OUTPUT_DIR = Path("outputs/stage5b_live_pilot_v1")

_MODEL_CRITERIA = tuple(
    criterion
    for criterion in EligibilityCriterion
    if criterion is not EligibilityCriterion.ADMINISTRATIVE_SCOPE
)
_ADMIN_REASON_KEYS = {
    "publication_date": ExclusionReason.AFTER_RETRIEVAL_END_DATE,
    "language": ExclusionReason.NON_ENGLISH_FULL_TEXT,
    "document_type": ExclusionReason.INELIGIBLE_DOCUMENT_TYPE,
}
_ELIGIBLE_DOCUMENT_TYPES = {
    "conference_paper",
    "journal_article",
    "preprint",
    "research_paper",
    "workshop_paper",
}
_INELIGIBLE_DOCUMENT_TYPES = {
    "book",
    "book_chapter",
    "editorial",
    "poster_abstract",
    "thesis",
}


def load_pilot5b_config(path: str | Path = PILOT5B_CONFIG) -> dict[str, Any]:
    resolved = inference_core._resolve_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if payload.get("config_version") != PILOT5B_PROMPT_VERSION:
        raise ValueError("unsupported Pilot 5B config version")
    if payload.get("prompt_version") != PILOT5B_PROMPT_VERSION:
        raise ValueError("Pilot 5B prompt/config version mismatch")
    if payload.get("output_schema_version") != PILOT5B_OUTPUT_SCHEMA_VERSION:
        raise ValueError("Pilot 5B schema/config version mismatch")
    return payload


def prepare_pilot5b(
    *,
    config_path: str | Path = PILOT5B_CONFIG,
    historical_root: str | Path | None = None,
    output_dir: str | Path = PILOT5B_OUTPUT_DIR,
    created_at: str | None = None,
) -> tuple[Any, dict[str, Any], dict[str, Any], Any]:
    """Write a separate no-call Pilot 5B dataset and auditable preflight."""

    from h2h_lit.models import utc_now_iso
    from h2h_lit.pilot import PilotPaths, build_pilot_dataset, load_pilot_config
    from h2h_lit.retrieval import save_review_dataset

    config = load_pilot5b_config(config_path)
    timestamp = created_at or utc_now_iso()
    dataset, manifest = build_pilot_dataset(
        config_path=config["selection_config"],
        historical_root=historical_root,
        created_at=timestamp,
    )
    manifest["pilot_version"] = PILOT5B_PROMPT_VERSION
    manifest["selection_config_unchanged_from_stage5a"] = config["selection_config"]
    manifest["pilot5b_execution_controls"] = dict(config["execution_controls"])
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


def run_live_pilot5b(
    *,
    provider: inference_core.InferenceProvider,
    config_path: str | Path = PILOT5B_CONFIG,
    historical_root: str | Path | None = None,
    output_dir: str | Path = PILOT5B_OUTPUT_DIR,
    created_at: str | None = None,
) -> tuple[Any, dict[str, Any], Any]:
    """Execute the explicitly requested future 5B run; callers control live access."""

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
    dataset, manifest, _, paths = prepare_pilot5b(
        config_path=config_path,
        historical_root=historical_root,
        output_dir=output_dir,
        created_at=timestamp,
    )
    config = load_pilot5b_config(config_path)
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
        run_id=_stable_id("inference-run-pilot5b", manifest["config_hash"], timestamp),
    )
    save_review_dataset(paths.review_dataset, dataset)
    retry_limit = int(base["retry_limit_per_record"])
    for record in dataset.canonical_records:
        inference_input = pilot5b_inference_input_for(
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
            retryable, retry_reason = _retry_disposition(attempt)
            retry_permitted = retryable and attempt.attempt_number <= retry_limit
            attempt.metadata["pilot5b_execution_controls"] = {
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


def pilot5b_inference_input_for(
    record: Any,
    *,
    pilot_execution_date: str,
) -> inference_core.InferenceInput:
    """Build a field-addressable input and explicit pilot-only E6 metadata."""

    title = record.record.title.strip()
    abstract = record.record.abstract.strip()
    original = record.record.original_metadata
    document_type = _document_type_status(original.get("_type"))
    language = _language_status(original.get("language"))
    source_file = original.get("source_file")
    full_text_status = (
        "verified_available"
        if source_file and inference_core._resolve_path(source_file).is_file()
        else "unknown"
    )
    administrative = {
        "document_type": document_type,
        "full_text": full_text_status,
        "language": language,
        "pilot_administrative_observation_cutoff": pilot_execution_date,
        "pilot_only": True,
        "production_retrieval_cutoff_status": "not_established_by_pilot",
        "publication_date": _publication_date_status(
            record.record.year,
            pilot_execution_date,
            original.get("publication_date"),
        ),
        "publication_year": record.record.year,
    }

    parts: list[str] = []
    fields: dict[str, dict[str, Any]] = {}
    for field_name, heading, value, locator in (
        ("title", "TITLE", title, "input.title"),
        ("abstract", "ABSTRACT", abstract, "input.abstract"),
    ):
        prefix = f"{heading}:\n"
        start = sum(len(part) for part in parts) + len(prefix)
        parts.append(prefix + value + "\n\n")
        fields[field_name] = {
            "locator": locator,
            "start": start,
            "end": start + len(value),
        }

    admin_text = json.dumps(
        administrative, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    prefix = "ADMINISTRATIVE_METADATA:\n"
    admin_start = sum(len(part) for part in parts) + len(prefix)
    parts.append(prefix + admin_text)
    fields["administrative_metadata"] = {
        "locator": "input.administrative_metadata",
        "start": admin_start,
        "end": admin_start + len(admin_text),
    }
    artifact_path = source_file or original.get("pilot_selection", {}).get("path")
    return inference_core.InferenceInput(
        canonical_record_id=record.canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        text="".join(parts),
        source_location="title_abstract",
        source_artifact_id=str(artifact_path) if artifact_path else None,
        metadata={
            "title": title,
            "doi": record.record.doi,
            "source_identifier": record.record.source_identifier,
            "input_policy": "pilot5b_field_addressable_title_abstract",
            "evidence_fields": fields,
            "pilot5b_administrative": administrative,
            "e6_decision_scope": "pilot_only",
        },
    )


def parse_pilot5b_proposal(
    payload: dict[str, Any],
    inference_input: inference_core.InferenceInput,
) -> inference_core._ParsedProposal:
    """Validate schema 1.1.0 and replace model offsets/outcomes deterministically."""

    inference_core._require_exact_keys(
        payload,
        {
            "criteria",
            "assistance_modes",
            "visualization_modalities",
            "tasks",
            "primary_exclusion_reason",
            "secondary_exclusion_reasons",
            "overall_rationale",
        },
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

    assistance = _parse_dimension(
        payload["assistance_modes"],
        [item.value for item in AssistanceMode],
        inference_input,
        "assistance_modes",
    )
    modalities = _parse_dimension(
        payload["visualization_modalities"],
        [item.value for item in VisualizationModality],
        inference_input,
        "visualization_modalities",
    )
    tasks = _parse_dimension(
        payload["tasks"],
        [item.value for item in TaskCategory],
        inference_input,
        "tasks",
    )
    _reject_indiscriminate_criterion_evidence(evidence)

    expected_reasons = _derive_exclusion_reasons(values, e6_reasons)
    expected_primary = expected_reasons[0] if expected_reasons else None
    expected_secondary = tuple(expected_reasons[1:])
    supplied_primary = (
        None
        if payload["primary_exclusion_reason"] is None
        else inference_core._strict_enum(
            ExclusionReason,
            payload["primary_exclusion_reason"],
            "primary_exclusion_reason",
        )
    )
    secondary_payload = payload["secondary_exclusion_reasons"]
    if not isinstance(secondary_payload, list):
        raise TypeError("secondary_exclusion_reasons must be a list")
    supplied_secondary = tuple(
        inference_core._strict_enum(
            ExclusionReason, item, "secondary_exclusion_reasons"
        )
        for item in secondary_payload
    )
    if supplied_primary != expected_primary or supplied_secondary != expected_secondary:
        raise ValueError(
            "exclusion-semantic violation: model exclusion fields do not equal the "
            "deterministic Stage 3 derivation"
        )

    # Exercise the Stage 3 aggregate rule here; materialization invokes it again.
    derive_eligibility_status(values)
    return inference_core._ParsedProposal(
        criterion_values=values,
        criterion_evidence=evidence,
        criterion_rationales=rationales,
        assistance_modes=assistance,
        visualization_modalities=modalities,
        tasks=tasks,
        primary_exclusion_reason=expected_primary,
        secondary_exclusion_reasons=expected_secondary,
        overall_rationale=inference_core._require_rationale(
            payload["overall_rationale"], "overall_rationale"
        ),
    )


def pilot5b_response_schema() -> dict[str, Any]:
    evidence = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "quote": {"type": "string"},
            "source_field": {"type": "string", "enum": ["title", "abstract"]},
            "locator": {"type": "string"},
            "claimed_start": {"type": ["integer", "null"]},
            "claimed_end": {"type": ["integer", "null"]},
        },
        "required": [
            "quote",
            "source_field",
            "locator",
            "claimed_start",
            "claimed_end",
        ],
    }
    evidence_array = {"type": "array", "items": evidence}
    criterion = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decision": {"type": "string", "enum": ["YES", "NO", "UNCERTAIN"]},
            "certainty": {"type": "string", "enum": ["SUPPORTED", "UNCERTAIN"]},
            "evidence": {**evidence_array, "minItems": 1},
            "rationale": {"type": "string", "minLength": 1},
        },
        "required": ["decision", "certainty", "evidence", "rationale"],
    }

    def dimension(labels: list[str]) -> dict[str, Any]:
        return {
            "type": "array",
            "minItems": len(labels),
            "maxItems": len(labels),
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "label": {"type": "string", "enum": labels},
                    "state": {
                        "type": "string",
                        "enum": ["PRESENT", "ABSENT", "UNCERTAIN"],
                    },
                    "certainty": {
                        "type": "string",
                        "enum": ["SUPPORTED", "UNCERTAIN"],
                    },
                    "evidence": evidence_array,
                    "rationale": {"type": "string", "minLength": 1},
                },
                "required": ["label", "state", "certainty", "evidence", "rationale"],
            },
        }

    reasons = [item.value for item in ExclusionReason]
    criteria = {item.value: criterion for item in _MODEL_CRITERIA}
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "criteria": {
                "type": "object",
                "additionalProperties": False,
                "properties": criteria,
                "required": list(criteria),
            },
            "assistance_modes": dimension([item.value for item in AssistanceMode]),
            "visualization_modalities": dimension(
                [item.value for item in VisualizationModality]
            ),
            "tasks": dimension([item.value for item in TaskCategory]),
            "primary_exclusion_reason": {
                "type": ["string", "null"],
                "enum": [*reasons, None],
            },
            "secondary_exclusion_reasons": {
                "type": "array",
                "items": {"type": "string", "enum": reasons},
            },
            "overall_rationale": {"type": "string", "minLength": 1},
        },
        "required": [
            "criteria",
            "assistance_modes",
            "visualization_modalities",
            "tasks",
            "primary_exclusion_reason",
            "secondary_exclusion_reasons",
            "overall_rationale",
        ],
    }


def _parse_dimension(
    payload: Any,
    allowed_labels: list[str],
    inference_input: inference_core.InferenceInput,
    context: str,
) -> tuple[inference_core._CodedValue, ...]:
    if not isinstance(payload, list):
        raise TypeError(f"{context} must be a list")
    by_label: dict[str, inference_core._CodedValue] = {}
    for index, raw_item in enumerate(payload):
        item_context = f"{context}[{index}]"
        item = inference_core._require_dict(raw_item, item_context)
        inference_core._require_exact_keys(
            item,
            {"label", "state", "certainty", "evidence", "rationale"},
            item_context,
        )
        label = item["label"]
        if type(label) is not str or label not in allowed_labels:
            raise ValueError(
                f"{item_context}.label uses an unsupported frozen vocabulary value"
            )
        if label in by_label:
            raise ValueError(f"{context} contains duplicate label {label}")
        state = inference_core._strict_enum(
            AnnotationState, item["state"], f"{item_context}.state"
        )
        inference_core._validate_certainty(state.value, item["certainty"], item_context)
        by_label[label] = inference_core._CodedValue(
            label=label,
            state=state,
            evidence=_parse_quote_evidence(
                item["evidence"],
                inference_input,
                item_context,
                required=state in {AnnotationState.PRESENT, AnnotationState.UNCERTAIN},
            ),
            rationale=inference_core._require_rationale(
                item["rationale"], item_context
            ),
        )
    if set(by_label) != set(allowed_labels):
        missing = sorted(set(allowed_labels) - set(by_label))
        raise ValueError(
            f"{context} must code every frozen label exactly once; missing={missing}"
        )
    return tuple(by_label[label] for label in allowed_labels)


def _parse_quote_evidence(
    payload: Any,
    inference_input: inference_core.InferenceInput,
    context: str,
    *,
    required: bool,
) -> tuple[inference_core._EvidenceSpan, ...]:
    if not isinstance(payload, list):
        raise TypeError(f"{context}.evidence must be a list")
    if required and not payload:
        raise ValueError(f"{context} requires at least one evidence quote")
    return tuple(
        _resolve_quote(raw_span, inference_input, f"{context}.evidence[{index}]")
        for index, raw_span in enumerate(payload)
    )


def _resolve_quote(
    raw_span: Any,
    inference_input: inference_core.InferenceInput,
    context: str,
) -> inference_core._EvidenceSpan:
    span = inference_core._require_dict(raw_span, context)
    inference_core._require_exact_keys(
        span,
        {"quote", "source_field", "locator", "claimed_start", "claimed_end"},
        context,
    )
    quote = span["quote"]
    source_field = span["source_field"]
    locator = span["locator"]
    if type(quote) is not str or not quote:
        raise ValueError(f"{context}.quote must be a non-empty verbatim string")
    if type(source_field) is not str or source_field not in {"title", "abstract"}:
        raise ValueError(f"{context}.source_field must be title or abstract")
    if type(locator) is not str or not locator:
        raise ValueError(f"{context}.locator must be a non-empty string")
    for key in ("claimed_start", "claimed_end"):
        if span[key] is not None and type(span[key]) is not int:
            raise TypeError(f"{context}.{key} must be an integer or null")

    fields = inference_input.metadata.get("evidence_fields")
    if not isinstance(fields, dict) or source_field not in fields:
        raise ValueError(f"{context} source field is absent from the persisted input metadata")
    field = fields[source_field]
    base_locator = field.get("locator")
    if type(base_locator) is not str:
        raise ValueError(f"{context} source-field locator metadata is invalid")
    occurrence = None
    if locator == base_locator:
        pass
    elif locator.startswith(f"{base_locator}#occurrence="):
        suffix = locator.removeprefix(f"{base_locator}#occurrence=")
        if not suffix.isdigit() or int(suffix) < 1:
            raise ValueError(f"{context}.locator has an invalid occurrence selector")
        occurrence = int(suffix)
    else:
        raise ValueError(f"{context}.locator does not address {source_field}")

    start = field.get("start")
    end = field.get("end")
    if type(start) is not int or type(end) is not int or start < 0 or end < start:
        raise ValueError(f"{context} source-field bounds are invalid")
    field_text = inference_input.text[start:end]
    matches: list[int] = []
    cursor = 0
    while True:
        match = field_text.find(quote, cursor)
        if match < 0:
            break
        matches.append(match)
        cursor = match + 1
    if not matches:
        raise ValueError(f"{context}.quote has zero exact matches in {source_field}")
    if occurrence is None:
        if len(matches) != 1:
            raise ValueError(
                f"{context}.quote is ambiguous in {source_field}; provide an occurrence locator"
            )
        selected = matches[0]
    else:
        if occurrence > len(matches):
            raise ValueError(f"{context}.locator selects a nonexistent quote occurrence")
        selected = matches[occurrence - 1]
    canonical_start = start + selected
    return inference_core._EvidenceSpan(
        start=canonical_start,
        end=canonical_start + len(quote),
        quote=quote,
        locator=locator,
        source_field=source_field,
        raw_quote=quote,
        claimed_start=span["claimed_start"],
        claimed_end=span["claimed_end"],
        resolution_method="unique_exact_substring"
        if occurrence is None
        else "locator_disambiguated_exact_substring",
    )


def _derive_e6(
    inference_input: inference_core.InferenceInput,
) -> tuple[
    TriState,
    inference_core._EvidenceSpan,
    str,
    tuple[ExclusionReason, ...],
]:
    admin = inference_input.metadata.get("pilot5b_administrative")
    fields = inference_input.metadata.get("evidence_fields")
    if not isinstance(admin, dict) or not isinstance(fields, dict):
        raise ValueError("Pilot 5B E6 requires persisted structured administrative metadata")
    if admin.get("pilot_only") is not True:
        raise ValueError("Pilot 5B E6 metadata must be explicitly marked pilot-only")
    admin_field = fields.get("administrative_metadata")
    if not isinstance(admin_field, dict):
        raise ValueError("Pilot 5B E6 administrative locator metadata is missing")
    start, end = admin_field.get("start"), admin_field.get("end")
    locator = admin_field.get("locator")
    if type(start) is not int or type(end) is not int or type(locator) is not str:
        raise ValueError("Pilot 5B E6 administrative locator metadata is invalid")

    components = {
        "publication_date": _status_value(admin.get("publication_date")),
        "language": _status_value(admin.get("language")),
        "document_type": _status_value(admin.get("document_type")),
        "full_text": _status_value(admin.get("full_text")),
    }
    if TriState.NO in components.values():
        decision = TriState.NO
    elif all(value is TriState.YES for value in components.values()):
        decision = TriState.YES
    else:
        decision = TriState.UNCERTAIN
    reasons = tuple(
        reason
        for key, reason in _ADMIN_REASON_KEYS.items()
        if components[key] is TriState.NO
    )
    return (
        decision,
        inference_core._EvidenceSpan(
            start=start,
            end=end,
            quote=inference_input.text[start:end],
            locator=locator,
            source_field="administrative_metadata",
            resolution_method="deterministic_structured_metadata",
            source=EvidenceSource.METADATA,
        ),
        "Pilot-only E6 derived deterministically from publication-date, language, "
        "document-type, and full-text administrative states.",
        reasons,
    )


def _status_value(value: Any) -> TriState:
    if value in {"within_scope", "english", "eligible", "verified_available"}:
        return TriState.YES
    if value in {"after_cutoff", "non_english", "ineligible"}:
        return TriState.NO
    return TriState.UNCERTAIN


def _derive_exclusion_reasons(
    values: dict[EligibilityCriterion, TriState],
    e6_reasons: tuple[ExclusionReason, ...],
) -> list[ExclusionReason]:
    reasons = list(e6_reasons)
    for reason in EXCLUSION_REASON_ORDER:
        supported = REASON_CRITERIA[reason]
        if (
            len(supported) == 1
            and (criterion := next(iter(supported)))
            not in {
                EligibilityCriterion.ADMINISTRATIVE_SCOPE,
                EligibilityCriterion.EVIDENCE_SUFFICIENCY,
            }
            and values[criterion] is TriState.NO
        ):
            reasons.append(reason)
    rank = {reason: index for index, reason in enumerate(EXCLUSION_REASON_ORDER)}
    return sorted(set(reasons), key=rank.__getitem__)


def _reject_indiscriminate_criterion_evidence(
    evidence: dict[EligibilityCriterion, tuple[inference_core._EvidenceSpan, ...]],
) -> None:
    signatures = {
        tuple((span.source_field, span.start, span.end, span.quote) for span in evidence[item])
        for item in _MODEL_CRITERIA
    }
    if len(signatures) == 1:
        raise ValueError(
            "criterion-specific evidence cannot reuse one identical span for every criterion"
        )


def _publication_date_status(
    year: Any,
    cutoff: str,
    publication_date: Any = None,
) -> str:
    if (
        type(publication_date) is str
        and len(publication_date) == 10
        and publication_date[4] == "-"
        and publication_date[7] == "-"
    ):
        return "within_scope" if publication_date <= cutoff else "after_cutoff"
    try:
        cutoff_year = int(cutoff[:4])
        publication_year = int(year)
    except (TypeError, ValueError):
        return "unknown"
    if publication_year < cutoff_year:
        return "within_scope"
    if publication_year > cutoff_year:
        return "after_cutoff"
    return "unknown"


def _language_status(value: Any) -> str:
    if type(value) is not str or not value.strip():
        return "unknown"
    normalized = value.strip().casefold()
    if normalized in {"n/a", "not recorded", "unknown", "unspecified"}:
        return "unknown"
    if normalized in {"en", "eng", "english"}:
        return "english"
    return "non_english"


def _document_type_status(value: Any) -> str:
    if type(value) is not str or not value.strip():
        return "unknown"
    normalized = value.strip().casefold().replace(" ", "_")
    if normalized in _ELIGIBLE_DOCUMENT_TYPES or normalized == "article":
        return "eligible"
    if normalized in _INELIGIBLE_DOCUMENT_TYPES:
        return "ineligible"
    return "unknown"
