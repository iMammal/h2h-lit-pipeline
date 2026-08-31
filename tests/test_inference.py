from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from h2h_lit.inference import (
    FULL_TEXT_PROMPT,
    TITLE_ABSTRACT_PROMPT,
    InferenceInput,
    MockInferenceProvider,
    load_prompt_artifact,
    register_inference_run,
    run_inference_attempt,
)
from h2h_lit.models import LiteratureRecord
from h2h_lit.retrieval import load_review_dataset, save_review_dataset
from h2h_lit.review import (
    ActorType,
    AnnotationState,
    AssistanceMode,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    EligibilityCriterion,
    EligibilityStatus,
    InferenceValidationStatus,
    RecordOccurrence,
    ReviewDataset,
    ScreeningStage,
    SourceQuery,
    TaskCategory,
    TriState,
    VisualizationModality,
    canonicalize_occurrences,
)
from h2h_lit.screening import ScreeningSubmission, record_screening_decision

TEXT = (
    "Life science analysts use an interactive network visualization with clustering "
    "to compare cells."
)
FULL_TEXT = (
    "The biomedical system lets analysts inspect a multiscale lineage visualization. "
    "Clustering guides interactive comparison and hypothesis development."
)


def _clock() -> Callable[[], str]:
    counter = 0

    def timestamp() -> str:
        nonlocal counter
        counter += 1
        return f"time-{counter:03d}"

    return timestamp


def _provenance(
    actor_type: ActorType,
    authority: DecisionAuthority,
    created_at: str,
    *,
    supersedes: list[str] | None = None,
) -> DecisionProvenance:
    return DecisionProvenance(
        actor=DecisionActor(actor_id=f"{actor_type.value}:{created_at}", actor_type=actor_type),
        authority=authority,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=created_at,
        supersedes_ids=list(supersedes or []),
    )


def _dataset() -> ReviewDataset:
    query = SourceQuery(
        query_id="query:inference",
        source_database="MockSource",
        query_text="mocked inference record",
        query_version="mock-v1",
        retrieval_started_at="retrieval-start",
        retrieval_ended_at="retrieval-end",
        result_count=1,
    )
    occurrence = RecordOccurrence(
        occurrence_id="occurrence:inference",
        source_query_id=query.query_id,
        source_identifier="mock:1",
        retrieved_at="retrieval-end",
        record=LiteratureRecord(
            title="Mocked paper",
            abstract=TEXT,
            doi="10.1000/inference",
            source_database="MockSource",
            source_identifier="mock:1",
        ),
    )
    canonical, dedupe = canonicalize_occurrences(
        [occurrence],
        provenance=_provenance(
            ActorType.SOFTWARE, DecisionAuthority.DETERMINISTIC, "dedupe-time"
        ),
    )
    dataset = ReviewDataset(
        source_queries=[query],
        occurrences=[occurrence],
        canonical_records=canonical,
        duplicate_decisions=dedupe,
    )
    dataset.validate()
    return dataset


def _span(text: str, locator: str) -> dict[str, object]:
    return {"start": 0, "end": len(text), "quote": text, "locator": locator}


def _response_payload(
    text: str,
    locator: str,
    *,
    criterion_overrides: dict[EligibilityCriterion, TriState] | None = None,
    primary_exclusion_reason: str | None = None,
) -> dict[str, object]:
    overrides = criterion_overrides or {}
    evidence = [_span(text, locator)]
    criteria = {}
    for criterion in EligibilityCriterion:
        decision = overrides.get(criterion, TriState.YES)
        criteria[criterion.value] = {
            "decision": decision.value,
            "certainty": (
                "UNCERTAIN" if decision is TriState.UNCERTAIN else "SUPPORTED"
            ),
            "evidence": evidence,
            "rationale": f"Mocked rationale for {criterion.value}.",
        }

    def dimension_items(
        labels: list[str],
        present: set[str],
    ) -> list[dict[str, object]]:
        return [
            {
                "label": label,
                "state": (
                    AnnotationState.PRESENT.value
                    if label in present
                    else AnnotationState.ABSENT.value
                ),
                "certainty": "SUPPORTED",
                "evidence": evidence if label in present else [],
                "rationale": f"Mocked rationale for {label}.",
            }
            for label in labels
        ]

    return {
        "criteria": criteria,
        "assistance_modes": dimension_items(
            [item.value for item in AssistanceMode],
            {AssistanceMode.ALGORITHMIC.value, AssistanceMode.ADAPTIVE.value},
        ),
        "visualization_modalities": dimension_items(
            [item.value for item in VisualizationModality],
            {VisualizationModality.DESKTOP_2D.value},
        ),
        "tasks": dimension_items(
            [item.value for item in TaskCategory],
            {TaskCategory.COMPARISON_DIFFERENTIATION.value},
        ),
        "primary_exclusion_reason": primary_exclusion_reason,
        "secondary_exclusion_reasons": [],
        "overall_rationale": "Mocked auditable overall rationale.",
    }


def _register_title_run(dataset: ReviewDataset):
    return register_inference_run(
        dataset,
        stage=ScreeningStage.TITLE_ABSTRACT,
        prompt_path=TITLE_ABSTRACT_PROMPT,
        provider="mock-provider",
        model="mock-model-v1",
        parameters={"temperature": 0, "seed": 7},
        created_at="run-created",
    )


def test_prompt_artifacts_are_versioned_hashed_and_stage_specific():
    title = load_prompt_artifact(TITLE_ABSTRACT_PROMPT, stage=ScreeningStage.TITLE_ABSTRACT)
    full = load_prompt_artifact(FULL_TEXT_PROMPT, stage=ScreeningStage.FULL_TEXT)

    assert title.version == "1.0.0"
    assert full.version == "1.0.0"
    assert len(title.content_hash) == 64
    assert len(full.content_hash) == 64
    assert title.content_hash != full.content_hash
    assert "E7_evidence_sufficiency" in title.content
    assert "Coordination and Collaborative Reasoning" in full.content


def test_valid_title_abstract_output_creates_only_proposed_artifacts_and_round_trips(
    tmp_path: Path,
):
    dataset = _dataset()
    run = _register_title_run(dataset)
    payload = _response_payload(TEXT, "title_abstract")
    provider = MockInferenceProvider([json.dumps(payload)])
    inference_input = InferenceInput(
        canonical_record_id=dataset.canonical_records[0].canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        text=TEXT,
        source_location="title_abstract",
        source_artifact_id="artifact:metadata:1",
    )

    attempt = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=inference_input,
        provider=provider,
        timestamp=_clock(),
    )

    assert attempt.validation_status is InferenceValidationStatus.VALID
    assert attempt.attempt_number == 1
    assert attempt.raw_response == json.dumps(payload)
    assert attempt.parsed_response == payload
    assert attempt.screening_decision_id is not None
    assert len(attempt.annotation_ids) == 14
    assert len(dataset.evidence) == 1
    assert dataset.evidence[0].quote == TEXT
    assert dataset.evidence[0].locator == f"title_abstract:0-{len(TEXT)}"
    proposal = dataset.screening_decisions[0]
    assert proposal.provenance.actor.actor_type is ActorType.LLM
    assert proposal.provenance.authority is DecisionAuthority.PROPOSED
    assert dataset.corpus_memberships == []

    first = tmp_path / "inference.json"
    second = tmp_path / "inference-again.json"
    assert save_review_dataset(first, dataset) == save_review_dataset(second, dataset)
    restored = load_review_dataset(first)
    assert restored.to_json() == dataset.to_json()
    assert restored.inference_runs[0].prompt_hash == run.prompt_hash
    assert restored.inference_attempts[0].input_snapshot["text"] == TEXT


def test_malformed_json_is_preserved_and_valid_retry_keeps_failed_attempt():
    dataset = _dataset()
    run = _register_title_run(dataset)
    valid_response = json.dumps(_response_payload(TEXT, "title_abstract"))
    provider = MockInferenceProvider(["{not-json", valid_response])
    inference_input = InferenceInput(
        canonical_record_id=dataset.canonical_records[0].canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        text=TEXT,
        source_location="title_abstract",
    )
    clock = _clock()

    first = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=inference_input,
        provider=provider,
        timestamp=clock,
    )
    second = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=inference_input,
        provider=provider,
        timestamp=clock,
    )

    assert first.validation_status is InferenceValidationStatus.INVALID
    assert first.raw_response == "{not-json"
    assert first.parsed_response is None
    assert first.screening_decision_id is None
    assert second.validation_status is InferenceValidationStatus.VALID
    assert second.attempt_number == 2
    assert second.request_id == first.request_id
    assert second.retry_of_attempt_id == first.attempt_id
    assert len(dataset.inference_attempts) == 2
    assert len(dataset.screening_decisions) == 1
    encoded = dataset.to_json()
    assert ReviewDataset.from_json(encoded).to_json() == encoded


def test_valid_exclusion_remains_a_machine_proposal_without_membership():
    dataset = _dataset()
    run = _register_title_run(dataset)
    payload = _response_payload(
        TEXT,
        "title_abstract",
        criterion_overrides={EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO},
        primary_exclusion_reason="EX_NO_LIFE_SCIENCE_APPLICATION",
    )

    attempt = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=InferenceInput(
            canonical_record_id=dataset.canonical_records[0].canonical_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            text=TEXT,
            source_location="title_abstract",
        ),
        provider=MockInferenceProvider([json.dumps(payload)]),
        timestamp=_clock(),
    )

    proposal = dataset.screening_decisions[0]
    assert attempt.validation_status is InferenceValidationStatus.VALID
    assert proposal.status is EligibilityStatus.EXCLUDED
    assert proposal.provenance.authority is DecisionAuthority.PROPOSED
    assert dataset.corpus_memberships == []


@pytest.mark.parametrize(
    ("invalid_case", "error_fragment"),
    [
        ("vocabulary", "unsupported frozen vocabulary"),
        ("missing_evidence", "requires at least one evidence span"),
        ("certainty", "certainty must be SUPPORTED or UNCERTAIN"),
    ],
)
def test_invalid_structured_outputs_do_not_create_proposals(
    invalid_case: str,
    error_fragment: str,
):
    dataset = _dataset()
    run = _register_title_run(dataset)
    payload = _response_payload(TEXT, "title_abstract")
    if invalid_case == "vocabulary":
        payload["assistance_modes"][0]["label"] = "Generative"  # type: ignore[index]
    elif invalid_case == "missing_evidence":
        payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value][  # type: ignore[index]
            "evidence"
        ] = []
    else:
        payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value][  # type: ignore[index]
            "certainty"
        ] = "HIGH"
    provider = MockInferenceProvider([json.dumps(payload)])

    attempt = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=InferenceInput(
            canonical_record_id=dataset.canonical_records[0].canonical_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            text=TEXT,
            source_location="title_abstract",
        ),
        provider=provider,
        timestamp=_clock(),
    )

    assert attempt.validation_status is InferenceValidationStatus.INVALID
    assert any(error_fragment in error for error in attempt.validation_errors)
    assert attempt.parsed_response == payload
    assert dataset.screening_decisions == []
    assert dataset.annotations == []
    assert dataset.evidence == []


def test_full_text_proposal_links_and_supersedes_uncertain_title_proposal():
    dataset = _dataset()
    title_run = _register_title_run(dataset)
    title_payload = _response_payload(
        TEXT,
        "title_abstract",
        criterion_overrides={EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN},
    )
    title_attempt = run_inference_attempt(
        dataset,
        run_id=title_run.run_id,
        inference_input=InferenceInput(
            canonical_record_id=dataset.canonical_records[0].canonical_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            text=TEXT,
            source_location="title_abstract",
        ),
        provider=MockInferenceProvider([json.dumps(title_payload)]),
        timestamp=_clock(),
    )
    full_run = register_inference_run(
        dataset,
        stage=ScreeningStage.FULL_TEXT,
        prompt_path=FULL_TEXT_PROMPT,
        provider="mock-provider",
        model="mock-model-v1",
        parameters={"temperature": 0},
        created_at="full-run-created",
    )
    full_attempt = run_inference_attempt(
        dataset,
        run_id=full_run.run_id,
        inference_input=InferenceInput(
            canonical_record_id=dataset.canonical_records[0].canonical_id,
            stage=ScreeningStage.FULL_TEXT,
            text=FULL_TEXT,
            source_location="full_text:pages-1-4",
            source_artifact_id="artifact:full-text:1",
            prior_screening_decision_id=title_attempt.screening_decision_id,
        ),
        provider=MockInferenceProvider(
            [json.dumps(_response_payload(FULL_TEXT, "full_text:pages-1-4"))]
        ),
        timestamp=_clock(),
    )

    assert full_attempt.validation_status is InferenceValidationStatus.VALID
    assert full_attempt.prior_screening_decision_id == title_attempt.screening_decision_id
    assert dataset.effective_screening_decisions()[0].decision_id == (
        full_attempt.screening_decision_id
    )
    assert len(dataset.screening_decisions) == 2
    assert len(dataset.effective_screening_decisions()) == 1
    assert len(dataset.annotations) == 28
    effective_annotations = [
        item
        for item in dataset.annotations
        if not any(
            item.annotation_id in candidate.provenance.supersedes_ids
            for candidate in dataset.annotations
        )
    ]
    assert len(effective_annotations) == 14
    assert dataset.corpus_memberships == []


def test_full_text_attempt_without_prior_uncertain_decision_is_invalid_without_calling_provider():
    dataset = _dataset()
    full_run = register_inference_run(
        dataset,
        stage=ScreeningStage.FULL_TEXT,
        prompt_path=FULL_TEXT_PROMPT,
        provider="mock-provider",
        model="mock-model-v1",
        parameters={},
        created_at="full-run-created",
    )
    provider = MockInferenceProvider([json.dumps(_response_payload(FULL_TEXT, "full_text"))])

    attempt = run_inference_attempt(
        dataset,
        run_id=full_run.run_id,
        inference_input=InferenceInput(
            canonical_record_id=dataset.canonical_records[0].canonical_id,
            stage=ScreeningStage.FULL_TEXT,
            text=FULL_TEXT,
            source_location="full_text",
        ),
        provider=provider,
        timestamp=_clock(),
    )

    assert attempt.validation_status is InferenceValidationStatus.INVALID
    assert any("requires an existing prior" in error for error in attempt.validation_errors)
    assert provider.calls == []
    assert dataset.screening_decisions == []


def test_human_decision_can_supersede_valid_machine_proposal():
    dataset = _dataset()
    run = _register_title_run(dataset)
    attempt = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=InferenceInput(
            canonical_record_id=dataset.canonical_records[0].canonical_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            text=TEXT,
            source_location="title_abstract",
        ),
        provider=MockInferenceProvider(
            [json.dumps(_response_payload(TEXT, "title_abstract"))]
        ),
        timestamp=_clock(),
    )
    machine = dataset.screening_decisions[0]
    human = record_screening_decision(
        dataset,
        ScreeningSubmission(
            canonical_record_id=machine.canonical_record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            criterion_values={item.criterion: item.value for item in machine.criteria},
            criterion_evidence_ids={
                item.criterion: list(item.evidence_ids) for item in machine.criteria
            },
            criterion_rationales={
                item.criterion: "Human reviewed the cited input evidence."
                for item in machine.criteria
            },
            provenance=_provenance(
                ActorType.HUMAN,
                DecisionAuthority.DECIDED,
                "human-review",
                supersedes=[machine.decision_id],
            ),
        ),
        finalize_membership=True,
    )

    assert attempt.validation_status is InferenceValidationStatus.VALID
    assert len(dataset.screening_decisions) == 2
    assert dataset.effective_screening_decisions() == [human.decision]
    assert human.corpus_membership is not None
    assert human.corpus_membership.status is EligibilityStatus.ELIGIBLE
    assert machine.provenance.authority is DecisionAuthority.PROPOSED
