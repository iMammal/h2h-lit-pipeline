from __future__ import annotations

import json
import pytest

from h2h_lit.inference import (
    InferenceInput,
    MockInferenceProvider,
    load_prompt_artifact,
    register_inference_run,
    run_inference_attempt,
)
from h2h_lit.models import LiteratureRecord
from h2h_lit.pilot5b import (
    PILOT5B_OUTPUT_SCHEMA_VERSION,
    PILOT5B_PROMPT,
    PILOT5B_PROMPT_VERSION,
    parse_pilot5b_proposal,
    pilot5b_response_schema,
)
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
    ExclusionReason,
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

TITLE = "Life-science visual analytics"
ABSTRACT = (
    "Cells form relational networks. Analysts use interactive visualizations. "
    "Clustering assists exploration. Analysts interpret results. "
    "A candidate system is described."
)
CRITERION_QUOTES = {
    EligibilityCriterion.LIFE_SCIENCE_APPLICATION: "Cells",
    EligibilityCriterion.RELATIONAL_MULTISCALE_RELEVANCE: "relational networks",
    EligibilityCriterion.INTERACTIVE_VISUAL_ANALYTICS: "interactive visualizations",
    EligibilityCriterion.COMPUTATIONAL_ASSISTANCE: "Clustering assists exploration",
    EligibilityCriterion.HUMAN_ANALYTIC_RELATIONSHIP: "Analysts interpret results",
    EligibilityCriterion.EVIDENCE_SUFFICIENCY: "A candidate system is described",
}


def _input(
    *,
    title: str = TITLE,
    abstract: str = ABSTRACT,
    administrative: dict[str, object] | None = None,
) -> InferenceInput:
    admin = administrative or {
        "publication_date": "within_scope",
        "language": "unknown",
        "document_type": "eligible",
        "full_text": "unknown",
        "pilot_administrative_observation_cutoff": "2026-08-31",
        "pilot_only": True,
        "production_retrieval_cutoff_status": "not_established_by_pilot",
    }
    prefix_title = "TITLE:\n"
    prefix_abstract = "\n\nABSTRACT:\n"
    prefix_admin = "\n\nADMINISTRATIVE_METADATA:\n"
    admin_text = json.dumps(admin, sort_keys=True, separators=(",", ":"))
    text = prefix_title + title + prefix_abstract + abstract + prefix_admin + admin_text
    title_start = len(prefix_title)
    abstract_start = title_start + len(title) + len(prefix_abstract)
    admin_start = abstract_start + len(abstract) + len(prefix_admin)
    return InferenceInput(
        canonical_record_id="canonical:pilot5b",
        stage=ScreeningStage.TITLE_ABSTRACT,
        text=text,
        source_location="title_abstract",
        metadata={
            "evidence_fields": {
                "title": {
                    "locator": "input.title",
                    "start": title_start,
                    "end": title_start + len(title),
                },
                "abstract": {
                    "locator": "input.abstract",
                    "start": abstract_start,
                    "end": abstract_start + len(abstract),
                },
                "administrative_metadata": {
                    "locator": "input.administrative_metadata",
                    "start": admin_start,
                    "end": admin_start + len(admin_text),
                },
            },
            "pilot5b_administrative": admin,
            "e6_decision_scope": "pilot_only",
        },
    )


def _evidence(
    quote: str,
    *,
    source_field: str = "abstract",
    locator: str | None = None,
    claimed_start: int | None = None,
    claimed_end: int | None = None,
) -> dict[str, object]:
    return {
        "quote": quote,
        "source_field": source_field,
        "locator": locator or f"input.{source_field}",
        "claimed_start": claimed_start,
        "claimed_end": claimed_end,
    }


def _payload(
    *,
    decisions: dict[EligibilityCriterion, TriState] | None = None,
    primary: ExclusionReason | None = None,
    secondary: list[ExclusionReason] | None = None,
) -> dict[str, object]:
    decisions = decisions or {}
    criteria = {}
    for criterion, quote in CRITERION_QUOTES.items():
        decision = decisions.get(criterion, TriState.YES)
        criteria[criterion.value] = {
            "decision": decision.value,
            "certainty": "UNCERTAIN" if decision is TriState.UNCERTAIN else "SUPPORTED",
            "evidence": [_evidence(quote)],
            "rationale": f"Criterion-specific rationale for {criterion.value}.",
        }

    def dimension(labels: list[str]) -> list[dict[str, object]]:
        return [
            {
                "label": label,
                "state": AnnotationState.ABSENT.value,
                "certainty": "SUPPORTED",
                "evidence": [],
                "rationale": f"No supported {label} evidence.",
            }
            for label in labels
        ]

    return {
        "criteria": criteria,
        "assistance_modes": dimension([item.value for item in AssistanceMode]),
        "visualization_modalities": dimension(
            [item.value for item in VisualizationModality]
        ),
        "tasks": dimension([item.value for item in TaskCategory]),
        "primary_exclusion_reason": primary.value if primary else None,
        "secondary_exclusion_reasons": [item.value for item in secondary or []],
        "overall_rationale": "Auditable Pilot 5B proposal.",
    }


def _dataset() -> ReviewDataset:
    query = SourceQuery(
        query_id="query:pilot5b",
        source_database="fixture",
        query_text="fixture",
        retrieval_started_at="time",
        retrieval_ended_at="time",
    )
    occurrence = RecordOccurrence(
        occurrence_id="occurrence:pilot5b",
        source_query_id=query.query_id,
        source_identifier="fixture:1",
        retrieved_at="time",
        record=LiteratureRecord(title=TITLE, abstract=ABSTRACT),
    )
    provenance = DecisionProvenance(
        actor=DecisionActor("software:test", ActorType.SOFTWARE),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at="time",
    )
    records, decisions = canonicalize_occurrences([occurrence], provenance=provenance)
    dataset = ReviewDataset(
        source_queries=[query],
        occurrences=[occurrence],
        canonical_records=records,
        duplicate_decisions=decisions,
    )
    dataset.canonical_records[0].canonical_id = "canonical:pilot5b"
    for decision in dataset.duplicate_decisions:
        decision.canonical_record_id = "canonical:pilot5b"
    dataset.validate()
    return dataset


def test_correct_quote_ignores_wrong_claimed_offsets_and_derives_canonical_offsets():
    inference_input = _input()
    payload = _payload()
    item = payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]
    item["evidence"] = [
        _evidence("Cells", claimed_start=9999, claimed_end=10004)
    ]

    parsed = parse_pilot5b_proposal(payload, inference_input)
    span = parsed.criterion_evidence[EligibilityCriterion.LIFE_SCIENCE_APPLICATION][0]

    expected = inference_input.text.index("Cells")
    assert (span.start, span.end) == (expected, expected + len("Cells"))
    assert (span.claimed_start, span.claimed_end) == (9999, 10004)
    assert span.resolution_method == "unique_exact_substring"


def test_repeated_quote_requires_and_accepts_occurrence_locator():
    repeated_abstract = ABSTRACT + " repeat then repeat"
    inference_input = _input(abstract=repeated_abstract)
    payload = _payload()
    item = payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]
    item["evidence"] = [_evidence("repeat")]
    with pytest.raises(ValueError, match="ambiguous"):
        parse_pilot5b_proposal(payload, inference_input)

    item["evidence"] = [_evidence("repeat", locator="input.abstract#occurrence=2")]
    parsed = parse_pilot5b_proposal(payload, inference_input)
    span = parsed.criterion_evidence[EligibilityCriterion.LIFE_SCIENCE_APPLICATION][0]
    field_start = inference_input.metadata["evidence_fields"]["abstract"]["start"]
    assert span.start == field_start + repeated_abstract.rindex("repeat")
    assert span.resolution_method == "locator_disambiguated_exact_substring"


@pytest.mark.parametrize("quote", ["", "not present verbatim"])
def test_empty_and_zero_match_quotes_are_rejected(quote: str):
    payload = _payload()
    item = payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]
    item["evidence"] = [_evidence(quote)]

    with pytest.raises(ValueError, match="non-empty|zero exact matches"):
        parse_pilot5b_proposal(payload, _input())


def test_unicode_substitution_is_not_normalized_or_fuzzy_matched():
    inference_input = _input(abstract=ABSTRACT.replace("interactive", "inter\u2011active"))
    payload = _payload()

    with pytest.raises(ValueError, match="zero exact matches"):
        parse_pilot5b_proposal(payload, inference_input)


def test_e6_is_deterministic_and_uncertain_with_incomplete_metadata():
    parsed = parse_pilot5b_proposal(_payload(), _input())
    e6 = EligibilityCriterion.ADMINISTRATIVE_SCOPE

    assert parsed.criterion_values[e6] is TriState.UNCERTAIN
    assert parsed.criterion_evidence[e6][0].source.value == "metadata"
    assert parsed.criterion_evidence[e6][0].resolution_method == (
        "deterministic_structured_metadata"
    )


def test_exclusion_status_and_reasons_are_derived_and_model_must_agree():
    decisions = {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO}
    reason = ExclusionReason.NO_LIFE_SCIENCE_APPLICATION
    parsed = parse_pilot5b_proposal(
        _payload(decisions=decisions, primary=reason), _input()
    )

    assert parsed.primary_exclusion_reason is reason
    assert parsed.secondary_exclusion_reasons == ()

    with pytest.raises(ValueError, match="exclusion-semantic violation"):
        parse_pilot5b_proposal(_payload(decisions=decisions), _input())


def test_e6_exclusion_precedes_scientific_exclusion_using_stage3_order():
    administrative = {
        "publication_date": "after_cutoff",
        "language": "english",
        "document_type": "eligible",
        "full_text": "verified_available",
        "pilot_administrative_observation_cutoff": "2026-08-31",
        "pilot_only": True,
        "production_retrieval_cutoff_status": "not_established_by_pilot",
    }
    decisions = {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO}
    parsed = parse_pilot5b_proposal(
        _payload(
            decisions=decisions,
            primary=ExclusionReason.AFTER_RETRIEVAL_END_DATE,
            secondary=[ExclusionReason.NO_LIFE_SCIENCE_APPLICATION],
        ),
        _input(administrative=administrative),
    )

    assert parsed.criterion_values[EligibilityCriterion.ADMINISTRATIVE_SCOPE] is TriState.NO
    assert parsed.primary_exclusion_reason is ExclusionReason.AFTER_RETRIEVAL_END_DATE
    assert parsed.secondary_exclusion_reasons == (
        ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
    )


def test_missing_evidence_and_certainty_inconsistency_remain_invalid():
    missing = _payload()
    missing["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value][
        "evidence"
    ] = []
    with pytest.raises(ValueError, match="requires at least one evidence"):
        parse_pilot5b_proposal(missing, _input())

    inconsistent = _payload()
    inconsistent["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value][
        "certainty"
    ] = "UNCERTAIN"
    with pytest.raises(ValueError, match="inconsistent"):
        parse_pilot5b_proposal(inconsistent, _input())


def test_prompt_and_validator_reject_indiscriminate_generic_evidence_reuse():
    prompt = load_prompt_artifact(
        PILOT5B_PROMPT,
        stage=ScreeningStage.TITLE_ABSTRACT,
        version=PILOT5B_PROMPT_VERSION,
        output_schema_version=PILOT5B_OUTPUT_SCHEMA_VERSION,
    )
    assert "criterion-specific, substantively supportive evidence" in prompt.content
    assert "Do not reuse one generic" in prompt.content

    payload = _payload()
    for item in payload["criteria"].values():
        item["evidence"] = [_evidence("Cells")]
    with pytest.raises(ValueError, match="criterion-specific evidence"):
        parse_pilot5b_proposal(payload, _input())


def test_schema_1_1_attempt_materializes_canonical_and_claimed_offset_provenance():
    dataset = _dataset()
    run = register_inference_run(
        dataset,
        stage=ScreeningStage.TITLE_ABSTRACT,
        prompt_path=PILOT5B_PROMPT,
        provider="mock",
        model="mock",
        parameters={"response_schema_version": PILOT5B_OUTPUT_SCHEMA_VERSION},
        created_at="run-time",
        prompt_version=PILOT5B_PROMPT_VERSION,
        output_schema_version=PILOT5B_OUTPUT_SCHEMA_VERSION,
    )
    inference_input = _input()
    payload = _payload()
    item = payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]
    item["evidence"] = [
        _evidence("Cells", claimed_start=700, claimed_end=705)
    ]
    attempt = run_inference_attempt(
        dataset,
        run_id=run.run_id,
        inference_input=inference_input,
        provider=MockInferenceProvider([json.dumps(payload)]),
        timestamp=lambda: "attempt-time",
    )

    assert attempt.validation_status is InferenceValidationStatus.VALID
    assert dataset.corpus_memberships == []
    screen = dataset.screening_decisions[0]
    assert screen.status is EligibilityStatus.UNCERTAIN
    e1_id = next(
        item.evidence_ids[0]
        for item in screen.criteria
        if item.criterion is EligibilityCriterion.LIFE_SCIENCE_APPLICATION
    )
    evidence = next(item for item in dataset.evidence if item.evidence_id == e1_id)
    canonical_start = inference_input.text.index("Cells")
    assert evidence.metadata["start"] == canonical_start
    assert evidence.metadata["model_claimed_start"] == 700
    assert evidence.metadata["model_claimed_end"] == 705
    assert evidence.metadata["raw_model_quote"] == "Cells"


def test_schema_excludes_model_authored_e6_and_uses_quote_locators():
    schema = pilot5b_response_schema()
    criteria = schema["properties"]["criteria"]
    assert EligibilityCriterion.ADMINISTRATIVE_SCOPE.value not in criteria["required"]
    evidence = criteria["properties"][
        EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value
    ]["properties"]["evidence"]["items"]
    assert set(evidence["required"]) == {
        "quote",
        "source_field",
        "locator",
        "claimed_start",
        "claimed_end",
    }
