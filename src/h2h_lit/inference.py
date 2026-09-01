"""Offline LLM proposal infrastructure with strict frozen-rubric validation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol

from h2h_lit.models import utc_now_iso
from h2h_lit.review import (
    ActorType,
    AnnotationDimension,
    AnnotationState,
    AssistanceMode,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    DimensionAnnotation,
    EligibilityCriterion,
    EligibilityStatus,
    EvidenceReference,
    EvidenceSource,
    ExclusionReason,
    InferenceAttempt,
    InferenceRun,
    InferenceValidationStatus,
    ReviewDataset,
    ScreeningStage,
    TaskCategory,
    TriState,
    VisualizationModality,
)
from h2h_lit.screening import ScreeningSubmission, record_screening_decision

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TITLE_ABSTRACT_PROMPT = Path("prompts/revised_star_title_abstract_screening_v1_0_0.md")
FULL_TEXT_PROMPT = Path("prompts/revised_star_full_text_screening_v1_0_0.md")
PROMPT_VERSION = "1.0.0"
OUTPUT_SCHEMA_VERSION = "1.0.0"

TimestampFactory = Callable[[], str]


class InferenceProvider(Protocol):
    def generate(
        self,
        *,
        model: str,
        prompt: str,
        input_snapshot: dict[str, Any],
        parameters: dict[str, Any],
        request_id: str,
        attempt_number: int,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class PromptArtifact:
    name: str
    version: str
    stage: ScreeningStage
    path: str
    content: str
    content_hash: str
    output_schema_version: str


@dataclass(slots=True)
class InferenceInput:
    canonical_record_id: str
    stage: ScreeningStage
    text: str
    source_location: str
    source_artifact_id: str | None = None
    prior_screening_decision_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "canonical_record_id": self.canonical_record_id,
            "stage": self.stage.value,
            "text": self.text,
            "source_location": self.source_location,
            "source_artifact_id": self.source_artifact_id,
            "prior_screening_decision_id": self.prior_screening_decision_id,
            "metadata": self.metadata,
        }


class MockInferenceProvider:
    """Queued deterministic responses for offline tests and development."""

    def __init__(self, responses: Sequence[str | Exception]):
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def generate(self, **kwargs: Any) -> str:
        self.calls.append(dict(kwargs))
        if not self._responses:
            raise AssertionError("no mocked inference response remains")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


@dataclass(frozen=True, slots=True)
class _EvidenceSpan:
    start: int
    end: int
    quote: str
    locator: str
    source_field: str | None = None
    raw_quote: str | None = None
    claimed_start: int | None = None
    claimed_end: int | None = None
    resolution_method: str | None = None
    source: EvidenceSource | None = None


@dataclass(frozen=True, slots=True)
class _CodedValue:
    label: str
    state: AnnotationState
    evidence: tuple[_EvidenceSpan, ...]
    rationale: str


@dataclass(frozen=True, slots=True)
class _ParsedProposal:
    criterion_values: dict[EligibilityCriterion, TriState]
    criterion_evidence: dict[EligibilityCriterion, tuple[_EvidenceSpan, ...]]
    criterion_rationales: dict[EligibilityCriterion, str]
    assistance_modes: tuple[_CodedValue, ...]
    visualization_modalities: tuple[_CodedValue, ...]
    tasks: tuple[_CodedValue, ...]
    primary_exclusion_reason: ExclusionReason | None
    secondary_exclusion_reasons: tuple[ExclusionReason, ...]
    overall_rationale: str
    audit_flags: tuple[dict[str, Any], ...] = ()


def load_prompt_artifact(
    path: str | Path,
    *,
    stage: ScreeningStage,
    name: str | None = None,
    version: str = PROMPT_VERSION,
    output_schema_version: str = OUTPUT_SCHEMA_VERSION,
) -> PromptArtifact:
    prompt_path = _resolve_path(path)
    content = prompt_path.read_text(encoding="utf-8")
    required_markers = [
        f"Prompt-Version: {version}",
        f"Stage: {stage.value}",
        f"Output-Schema-Version: {output_schema_version}",
    ]
    missing = [marker for marker in required_markers if marker not in content]
    if missing:
        raise ValueError(f"prompt artifact is missing metadata: {missing}")
    return PromptArtifact(
        name=name or prompt_path.stem,
        version=version,
        stage=stage,
        path=_portable_path(prompt_path),
        content=content,
        content_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
        output_schema_version=output_schema_version,
    )


def register_inference_run(
    dataset: ReviewDataset,
    *,
    stage: ScreeningStage,
    prompt_path: str | Path,
    provider: str,
    model: str,
    parameters: Mapping[str, Any],
    created_at: str,
    prompt_name: str | None = None,
    prompt_version: str = PROMPT_VERSION,
    output_schema_version: str = OUTPUT_SCHEMA_VERSION,
    run_id: str | None = None,
) -> InferenceRun:
    artifact = load_prompt_artifact(
        prompt_path,
        stage=stage,
        name=prompt_name,
        version=prompt_version,
        output_schema_version=output_schema_version,
    )
    parameters_copy = dict(parameters)
    generated_run_id = run_id or _stable_id(
        "inference-run",
        stage.value,
        artifact.content_hash,
        provider,
        model,
        _canonical_json(parameters_copy),
        created_at,
    )
    run = InferenceRun(
        run_id=generated_run_id,
        stage=stage,
        provider=provider,
        model=model,
        parameters=parameters_copy,
        prompt_name=artifact.name,
        prompt_version=artifact.version,
        prompt_hash=artifact.content_hash,
        prompt_path=artifact.path,
        created_at=created_at,
        output_schema_version=artifact.output_schema_version,
        metadata={"protocol_version": "1.0.0", "rubric_version": "1.0.0"},
    )
    dataset.inference_runs.append(run)
    try:
        dataset.validate()
    except Exception:
        dataset.inference_runs.pop()
        raise
    return run


def run_inference_attempt(
    dataset: ReviewDataset,
    *,
    run_id: str,
    inference_input: InferenceInput,
    provider: InferenceProvider,
    timestamp: TimestampFactory = utc_now_iso,
) -> InferenceAttempt:
    """Run one injected attempt, preserving invalid output without creating proposals."""

    run = next((item for item in dataset.inference_runs if item.run_id == run_id), None)
    if run is None:
        raise ValueError(f"unknown inference run: {run_id}")
    if not any(
        item.canonical_id == inference_input.canonical_record_id
        for item in dataset.canonical_records
    ):
        raise ValueError(f"unknown canonical record: {inference_input.canonical_record_id}")

    snapshot = inference_input.to_snapshot()
    input_hash = _json_hash(snapshot)
    request_id = _stable_id("inference-request", run.run_id, input_hash)
    previous_attempts = sorted(
        [item for item in dataset.inference_attempts if item.request_id == request_id],
        key=lambda item: item.attempt_number,
    )
    attempt_number = len(previous_attempts) + 1
    retry_of = previous_attempts[-1].attempt_id if previous_attempts else None
    attempt_id = _stable_id("inference-attempt", request_id, str(attempt_number))
    started_at = timestamp()
    ended_at = started_at
    raw_response = ""
    parsed_response: dict[str, Any] | None = None
    errors = _validate_attempt_input(dataset, run, inference_input, previous_attempts)
    proposal_id: str | None = None
    annotation_ids: list[str] = []

    prompt_content = ""
    if not errors:
        try:
            artifact = load_prompt_artifact(
                run.prompt_path,
                stage=run.stage,
                name=run.prompt_name,
                version=run.prompt_version,
                output_schema_version=run.output_schema_version,
            )
            if artifact.content_hash != run.prompt_hash:
                errors.append("prompt hash does not match the registered inference run")
            else:
                prompt_content = artifact.content
        except (OSError, ValueError) as exc:
            errors.append(f"prompt artifact error: {exc}")

    parsed: _ParsedProposal | None = None
    if not errors:
        try:
            response = provider.generate(
                model=run.model,
                prompt=prompt_content,
                input_snapshot=snapshot,
                parameters=dict(run.parameters),
                request_id=request_id,
                attempt_number=attempt_number,
            )
            if not isinstance(response, str):
                raise TypeError("provider response must be a string")
            raw_response = response
        except Exception as exc:  # noqa: BLE001 - provider failures are persisted attempts
            errors.append(f"provider error: {type(exc).__name__}: {exc}")

    if not errors:
        try:
            loaded = json.loads(raw_response)
            if not isinstance(loaded, dict):
                raise TypeError("top-level response must be a JSON object")
            parsed_response = loaded
            if run.output_schema_version == OUTPUT_SCHEMA_VERSION:
                parsed = _parse_proposal(loaded, inference_input)
            elif run.output_schema_version == "1.1.0":
                from h2h_lit.pilot5b import parse_pilot5b_proposal

                parsed = parse_pilot5b_proposal(loaded, inference_input)
            elif run.output_schema_version == "1.2.0":
                from h2h_lit.pilot5c import parse_pilot5c_proposal

                parsed = parse_pilot5c_proposal(loaded, inference_input)
            else:
                raise ValueError(
                    f"unsupported inference output schema: {run.output_schema_version}"
                )
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            errors.append(f"output validation error: {exc}")

    if parsed is not None and not errors:
        try:
            proposal_id, annotation_ids = _materialize_proposal(
                dataset,
                run=run,
                attempt_id=attempt_id,
                request_id=request_id,
                inference_input=inference_input,
                input_hash=input_hash,
                parsed=parsed,
                created_at=timestamp(),
            )
        except (ValueError, KeyError) as exc:
            errors.append(f"proposal validation error: {exc}")

    ended_at = timestamp()
    attempt = InferenceAttempt(
        attempt_id=attempt_id,
        request_id=request_id,
        run_id=run.run_id,
        canonical_record_id=inference_input.canonical_record_id,
        stage=run.stage,
        attempt_number=attempt_number,
        started_at=started_at,
        ended_at=ended_at,
        input_hash=input_hash,
        input_snapshot=snapshot,
        raw_response=raw_response,
        validation_status=(
            InferenceValidationStatus.INVALID if errors else InferenceValidationStatus.VALID
        ),
        parsed_response=parsed_response,
        validation_errors=errors,
        retry_of_attempt_id=retry_of,
        prior_screening_decision_id=inference_input.prior_screening_decision_id,
        screening_decision_id=proposal_id,
        annotation_ids=annotation_ids,
        metadata={
            "prompt_hash": run.prompt_hash,
            **({"audit_flags": list(parsed.audit_flags)} if parsed else {}),
        },
    )
    dataset.inference_attempts.append(attempt)
    try:
        dataset.validate()
    except Exception:
        dataset.inference_attempts.pop()
        raise
    return attempt


def _validate_attempt_input(
    dataset: ReviewDataset,
    run: InferenceRun,
    inference_input: InferenceInput,
    previous_attempts: list[InferenceAttempt],
) -> list[str]:
    errors: list[str] = []
    if inference_input.stage is not run.stage:
        errors.append("input stage does not match inference run stage")
    if not inference_input.text:
        errors.append("input text must not be empty")
    if not inference_input.source_location.strip():
        errors.append("input source location must not be empty")
    if previous_attempts and previous_attempts[-1].validation_status is InferenceValidationStatus.VALID:
        errors.append("a valid inference request cannot be retried")

    if run.stage is ScreeningStage.TITLE_ABSTRACT:
        if inference_input.prior_screening_decision_id is not None:
            errors.append("title/abstract input cannot link a prior screening decision")
        return errors

    prior_id = inference_input.prior_screening_decision_id
    prior = next(
        (item for item in dataset.screening_decisions if item.decision_id == prior_id),
        None,
    )
    if prior is None:
        errors.append("full-text input requires an existing prior screening decision")
    elif (
        prior.canonical_record_id != inference_input.canonical_record_id
        or prior.stage is not ScreeningStage.TITLE_ABSTRACT
        or prior.status is not EligibilityStatus.UNCERTAIN
        or prior.provenance.scope is not DecisionScope.PROSPECTIVE
    ):
        errors.append("full-text input must link the record's uncertain title/abstract decision")
    return errors


def _parse_proposal(
    payload: dict[str, Any],
    inference_input: InferenceInput,
) -> _ParsedProposal:
    _require_exact_keys(
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

    criteria_payload = _require_dict(payload["criteria"], "criteria")
    criterion_by_value = {item.value: item for item in EligibilityCriterion}
    _require_exact_keys(criteria_payload, set(criterion_by_value), "criteria")
    criterion_values: dict[EligibilityCriterion, TriState] = {}
    criterion_evidence: dict[EligibilityCriterion, tuple[_EvidenceSpan, ...]] = {}
    criterion_rationales: dict[EligibilityCriterion, str] = {}
    for value, criterion in criterion_by_value.items():
        item = _require_dict(criteria_payload[value], value)
        _require_exact_keys(item, {"decision", "certainty", "evidence", "rationale"}, value)
        decision = _strict_enum(TriState, item["decision"], f"{value}.decision")
        _validate_certainty(decision.value, item["certainty"], value)
        evidence = _parse_evidence(item["evidence"], inference_input, value, required=True)
        criterion_values[criterion] = decision
        criterion_evidence[criterion] = evidence
        criterion_rationales[criterion] = _require_rationale(item["rationale"], value)

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

    primary_value = payload["primary_exclusion_reason"]
    primary = (
        None
        if primary_value is None
        else _strict_enum(ExclusionReason, primary_value, "primary_exclusion_reason")
    )
    secondary_payload = payload["secondary_exclusion_reasons"]
    if not isinstance(secondary_payload, list):
        raise TypeError("secondary_exclusion_reasons must be a list")
    secondary = tuple(
        _strict_enum(ExclusionReason, item, "secondary_exclusion_reasons")
        for item in secondary_payload
    )
    overall_rationale = _require_rationale(payload["overall_rationale"], "overall_rationale")
    return _ParsedProposal(
        criterion_values=criterion_values,
        criterion_evidence=criterion_evidence,
        criterion_rationales=criterion_rationales,
        assistance_modes=assistance,
        visualization_modalities=modalities,
        tasks=tasks,
        primary_exclusion_reason=primary,
        secondary_exclusion_reasons=secondary,
        overall_rationale=overall_rationale,
    )


def _parse_dimension(
    payload: Any,
    allowed_labels: list[str],
    inference_input: InferenceInput,
    context: str,
) -> tuple[_CodedValue, ...]:
    if not isinstance(payload, list):
        raise TypeError(f"{context} must be a list")
    by_label: dict[str, _CodedValue] = {}
    for index, raw_item in enumerate(payload):
        item_context = f"{context}[{index}]"
        item = _require_dict(raw_item, item_context)
        _require_exact_keys(
            item, {"label", "state", "certainty", "evidence", "rationale"}, item_context
        )
        label = item["label"]
        if type(label) is not str or label not in allowed_labels:
            raise ValueError(f"{item_context}.label uses an unsupported frozen vocabulary value")
        if label in by_label:
            raise ValueError(f"{context} contains duplicate label {label}")
        state = _strict_enum(AnnotationState, item["state"], f"{item_context}.state")
        _validate_certainty(state.value, item["certainty"], item_context)
        evidence = _parse_evidence(
            item["evidence"],
            inference_input,
            item_context,
            required=state in {AnnotationState.PRESENT, AnnotationState.UNCERTAIN},
        )
        by_label[label] = _CodedValue(
            label=label,
            state=state,
            evidence=evidence,
            rationale=_require_rationale(item["rationale"], item_context),
        )
    if set(by_label) != set(allowed_labels):
        missing = sorted(set(allowed_labels) - set(by_label))
        raise ValueError(f"{context} must code every frozen label exactly once; missing={missing}")
    return tuple(by_label[label] for label in allowed_labels)


def _parse_evidence(
    payload: Any,
    inference_input: InferenceInput,
    context: str,
    *,
    required: bool,
) -> tuple[_EvidenceSpan, ...]:
    if not isinstance(payload, list):
        raise TypeError(f"{context}.evidence must be a list")
    if required and not payload:
        raise ValueError(f"{context} requires at least one evidence span")
    spans: list[_EvidenceSpan] = []
    for index, raw_span in enumerate(payload):
        span_context = f"{context}.evidence[{index}]"
        span = _require_dict(raw_span, span_context)
        _require_exact_keys(span, {"start", "end", "quote", "locator"}, span_context)
        start = span["start"]
        end = span["end"]
        quote = span["quote"]
        locator = span["locator"]
        if type(start) is not int or type(end) is not int:
            raise TypeError(f"{span_context} offsets must be integers")
        if type(quote) is not str or type(locator) is not str:
            raise TypeError(f"{span_context} quote and locator must be strings")
        if start < 0 or end <= start or end > len(inference_input.text):
            raise ValueError(f"{span_context} offsets are outside the input text")
        if inference_input.text[start:end] != quote:
            raise ValueError(f"{span_context} quote does not match the input snapshot")
        if locator != inference_input.source_location:
            raise ValueError(f"{span_context} locator does not match the input source location")
        spans.append(_EvidenceSpan(start=start, end=end, quote=quote, locator=locator))
    return tuple(spans)


def _materialize_proposal(
    dataset: ReviewDataset,
    *,
    run: InferenceRun,
    attempt_id: str,
    request_id: str,
    inference_input: InferenceInput,
    input_hash: str,
    parsed: _ParsedProposal,
    created_at: str,
) -> tuple[str, list[str]]:
    spans = _all_spans(parsed)
    existing_evidence = {item.evidence_id: item for item in dataset.evidence}
    evidence_ids: dict[_EvidenceSpan, str] = {}
    appended_evidence: list[EvidenceReference] = []
    for span in spans:
        evidence_id = _stable_id(
            "evidence",
            inference_input.canonical_record_id,
            input_hash,
            span.locator,
            str(span.start),
            str(span.end),
            span.quote,
        )
        evidence_ids[span] = evidence_id
        if evidence_id not in existing_evidence:
            evidence = EvidenceReference(
                evidence_id=evidence_id,
                canonical_record_id=inference_input.canonical_record_id,
                source=span.source
                or (
                    EvidenceSource.TITLE_ABSTRACT
                    if run.stage is ScreeningStage.TITLE_ABSTRACT
                    else EvidenceSource.FULL_TEXT
                ),
                locator=f"{span.locator}:{span.start}-{span.end}",
                quote=span.quote,
                artifact_id=inference_input.source_artifact_id,
                content_hash=input_hash,
                metadata={
                    "start": span.start,
                    "end": span.end,
                    "request_id": request_id,
                    "attempt_id": attempt_id,
                    **(
                        {"source_field": span.source_field}
                        if span.source_field is not None
                        else {}
                    ),
                    **(
                        {"raw_model_quote": span.raw_quote}
                        if span.raw_quote is not None
                        else {}
                    ),
                    **(
                        {"model_claimed_start": span.claimed_start}
                        if span.claimed_start is not None
                        else {}
                    ),
                    **(
                        {"model_claimed_end": span.claimed_end}
                        if span.claimed_end is not None
                        else {}
                    ),
                    **(
                        {"resolution_method": span.resolution_method}
                        if span.resolution_method is not None
                        else {}
                    ),
                },
            )
            dataset.evidence.append(evidence)
            appended_evidence.append(evidence)

    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id=f"llm:{run.provider}:{run.model}",
            actor_type=ActorType.LLM,
            metadata={"run_id": run.run_id, "request_id": request_id},
        ),
        authority=DecisionAuthority.PROPOSED,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=created_at,
        supersedes_ids=(
            [inference_input.prior_screening_decision_id]
            if inference_input.prior_screening_decision_id
            else []
        ),
        source_artifact_id=inference_input.source_artifact_id,
        metadata={
            "inference_run_id": run.run_id,
            "inference_attempt_id": attempt_id,
            "prompt_name": run.prompt_name,
            "prompt_version": run.prompt_version,
            "prompt_hash": run.prompt_hash,
            "input_hash": input_hash,
            **(
                {
                    "derived_screening": {
                        "actor_id": "software:h2h-lit-pipeline:stage3-derivation",
                        "actor_type": ActorType.SOFTWARE.value,
                        "authority": DecisionAuthority.DETERMINISTIC.value,
                        "rule": "stage3_eligibility_and_exclusion_derivation",
                        "primary_exclusion_reason": (
                            parsed.primary_exclusion_reason.value
                            if parsed.primary_exclusion_reason is not None
                            else None
                        ),
                        "secondary_exclusion_reasons": [
                            reason.value
                            for reason in parsed.secondary_exclusion_reasons
                        ],
                    },
                    "human_review_audit": {
                        "actor_id": "software:h2h-lit-pipeline:stage5c-audit",
                        "actor_type": ActorType.SOFTWARE.value,
                        "authority": DecisionAuthority.DETERMINISTIC.value,
                        "flags": list(parsed.audit_flags),
                    },
                }
                if run.output_schema_version == "1.2.0"
                else {}
            ),
        },
    )

    screening_result = None
    appended_annotations: list[DimensionAnnotation] = []
    try:
        screening_result = record_screening_decision(
            dataset,
            ScreeningSubmission(
                canonical_record_id=inference_input.canonical_record_id,
                stage=run.stage,
                criterion_values=parsed.criterion_values,
                criterion_evidence_ids={
                    criterion: [evidence_ids[span] for span in evidence]
                    for criterion, evidence in parsed.criterion_evidence.items()
                },
                criterion_rationales=parsed.criterion_rationales,
                provenance=provenance,
                primary_exclusion_reason=parsed.primary_exclusion_reason,
                secondary_exclusion_reasons=list(parsed.secondary_exclusion_reasons),
                notes=parsed.overall_rationale,
            ),
            finalize_membership=False,
        )
        prior_annotations = _prior_annotations(dataset, inference_input)
        for dimension, values in [
            (AnnotationDimension.ASSISTANCE_MODE, parsed.assistance_modes),
            (AnnotationDimension.VISUALIZATION_MODALITY, parsed.visualization_modalities),
            (AnnotationDimension.TASK, parsed.tasks),
        ]:
            for item in values:
                prior_id = prior_annotations.get((dimension, item.label))
                annotation_provenance = replace(
                    provenance,
                    supersedes_ids=[prior_id] if prior_id else [],
                )
                annotation = DimensionAnnotation(
                    annotation_id=_stable_id(
                        "annotation", attempt_id, dimension.value, item.label
                    ),
                    canonical_record_id=inference_input.canonical_record_id,
                    dimension=dimension,
                    value=item.label,
                    state=item.state,
                    provenance=annotation_provenance,
                    evidence_ids=[evidence_ids[span] for span in item.evidence],
                    rationale=item.rationale,
                )
                dataset.annotations.append(annotation)
                appended_annotations.append(annotation)
        dataset.validate()
    except Exception:
        for _ in appended_annotations:
            dataset.annotations.pop()
        if screening_result is not None:
            dataset.screening_decisions.pop()
        for _ in appended_evidence:
            dataset.evidence.pop()
        raise

    return screening_result.decision.decision_id, [
        item.annotation_id for item in appended_annotations
    ]


def _prior_annotations(
    dataset: ReviewDataset,
    inference_input: InferenceInput,
) -> dict[tuple[AnnotationDimension, str], str]:
    prior_id = inference_input.prior_screening_decision_id
    if prior_id is None:
        return {}
    attempt = next(
        (
            item
            for item in dataset.inference_attempts
            if item.screening_decision_id == prior_id
            and item.validation_status is InferenceValidationStatus.VALID
        ),
        None,
    )
    if attempt is None:
        return {}
    annotations = {item.annotation_id: item for item in dataset.annotations}
    return {
        (annotations[annotation_id].dimension, annotations[annotation_id].value): annotation_id
        for annotation_id in attempt.annotation_ids
    }


def _all_spans(parsed: _ParsedProposal) -> list[_EvidenceSpan]:
    ordered: list[_EvidenceSpan] = []
    seen: set[_EvidenceSpan] = set()
    groups = [
        *parsed.criterion_evidence.values(),
        *(item.evidence for item in parsed.assistance_modes),
        *(item.evidence for item in parsed.visualization_modalities),
        *(item.evidence for item in parsed.tasks),
    ]
    for group in groups:
        for span in group:
            if span not in seen:
                seen.add(span)
                ordered.append(span)
    return ordered


def _validate_certainty(state: str, certainty: Any, context: str) -> None:
    if type(certainty) is not str or certainty not in {"SUPPORTED", "UNCERTAIN"}:
        raise ValueError(f"{context}.certainty must be SUPPORTED or UNCERTAIN")
    expected = "UNCERTAIN" if state == "UNCERTAIN" else "SUPPORTED"
    if certainty != expected:
        raise ValueError(f"{context}.certainty is inconsistent with {state}")


def _strict_enum(enum_type: type[Any], value: Any, context: str) -> Any:
    if type(value) is not str:
        raise TypeError(f"{context} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"{context} uses unsupported value {value!r}") from exc


def _require_dict(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError(f"{context} must be an object")
    if any(type(key) is not str for key in value):
        raise TypeError(f"{context} keys must be strings")
    return value


def _require_exact_keys(value: dict[str, Any], expected: set[str], context: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f"{context} keys do not match schema; missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def _require_rationale(value: Any, context: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{context}.rationale must be a non-empty string")
    return value


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _portable_path(path: Path) -> str:
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _json_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"
