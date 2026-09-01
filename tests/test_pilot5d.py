from __future__ import annotations

import json
from types import SimpleNamespace

from h2h_lit.inference import (
    InferenceInput,
    MockInferenceProvider,
    register_inference_run,
    run_inference_attempt,
)
from h2h_lit.models import LiteratureRecord
from h2h_lit.pilot5c import pilot5c_response_schema
from h2h_lit.pilot5d import (
    PILOT5D_OUTPUT_SCHEMA_VERSION,
    PILOT5D_PROMPT,
    PILOT5D_PROMPT_VERSION,
    load_pilot5d_config,
    parse_pilot5d_proposal,
    pilot5d_inference_input_for,
    pilot5d_response_schema,
)
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    EligibilityCriterion,
    ExclusionReason,
    InferenceValidationStatus,
    RecordOccurrence,
    ReviewDataset,
    ScreeningStage,
    SourceQuery,
    TriState,
    canonicalize_occurrences,
)

TITLE = "Life-science relationship visualization"
ABSTRACT = (
    "Life science network researchers use interactive visual analytics. "
    "Algorithmic clustering assists analysts exploring relationships. "
    "The candidate system supports review."
)
QUOTES = {
    EligibilityCriterion.LIFE_SCIENCE_APPLICATION: "Life science",
    EligibilityCriterion.RELATIONAL_MULTISCALE_RELEVANCE: "network researchers",
    EligibilityCriterion.INTERACTIVE_VISUAL_ANALYTICS: "interactive visual analytics",
    EligibilityCriterion.COMPUTATIONAL_ASSISTANCE: "Algorithmic clustering assists analysts",
    EligibilityCriterion.HUMAN_ANALYTIC_RELATIONSHIP: "analysts exploring relationships",
    EligibilityCriterion.EVIDENCE_SUFFICIENCY: "candidate system",
}


def _input() -> InferenceInput:
    administrative = {
        "publication_date": "within_scope",
        "language": "unknown",
        "document_type": "eligible",
        "full_text": "unknown",
        "pilot_administrative_observation_cutoff": "2026-09-01",
        "pilot_only": True,
        "production_retrieval_cutoff_status": "not_established_by_pilot",
    }
    admin_text = json.dumps(administrative, sort_keys=True, separators=(",", ":"))
    title_prefix = "TITLE:\n"
    abstract_prefix = "\n\nABSTRACT:\n"
    admin_prefix = "\n\nADMINISTRATIVE_METADATA:\n"
    text = title_prefix + TITLE + abstract_prefix + ABSTRACT + admin_prefix + admin_text
    title_start = len(title_prefix)
    abstract_start = title_start + len(TITLE) + len(abstract_prefix)
    admin_start = abstract_start + len(ABSTRACT) + len(admin_prefix)
    return InferenceInput(
        canonical_record_id="canonical:pilot5d",
        stage=ScreeningStage.TITLE_ABSTRACT,
        text=text,
        source_location="title_abstract",
        source_artifact_id="provenance:artifact:test",
        metadata={
            "evidence_fields": {
                "title": {"locator": "input.title", "start": title_start, "end": title_start + len(TITLE)},
                "abstract": {"locator": "input.abstract", "start": abstract_start, "end": abstract_start + len(ABSTRACT)},
                "administrative_metadata": {"locator": "input.administrative_metadata", "start": admin_start, "end": admin_start + len(admin_text)},
            },
            "pilot5b_administrative": administrative,
            "e6_decision_scope": "pilot_only",
        },
    )


def _evidence(quote: str, *, claimed_start: int | None = None) -> dict[str, object]:
    return {
        "quote": quote,
        "source_field": "abstract",
        "locator": "input.abstract",
        "claimed_start": claimed_start,
        "claimed_end": None,
    }


def _payload(
    *, decisions: dict[EligibilityCriterion, TriState] | None = None
) -> dict[str, object]:
    decisions = decisions or {}
    criteria: dict[str, object] = {}
    for criterion, quote in QUOTES.items():
        decision = decisions.get(criterion, TriState.YES)
        criteria[criterion.value] = {
            "decision": decision.value,
            "certainty": "UNCERTAIN" if decision is TriState.UNCERTAIN else "SUPPORTED",
            "evidence": [_evidence(quote)],
            "rationale": f"Criterion-specific rationale for {criterion.value}.",
        }
    return {"criteria": criteria, "overall_rationale": "Auditable Stage 5D proposal."}


def _dataset() -> ReviewDataset:
    query = SourceQuery(query_id="query:pilot5d", source_database="fixture", query_text="fixture", retrieval_started_at="time", retrieval_ended_at="time")
    occurrence = RecordOccurrence(occurrence_id="occurrence:pilot5d", source_query_id=query.query_id, source_identifier="fixture:1", retrieved_at="time", record=LiteratureRecord(title=TITLE, abstract=ABSTRACT))
    provenance = DecisionProvenance(actor=DecisionActor("software:test", ActorType.SOFTWARE), authority=DecisionAuthority.DETERMINISTIC, scope=DecisionScope.PROSPECTIVE, protocol_version="1.0.0", rubric_version="1.0.0", created_at="time")
    records, decisions = canonicalize_occurrences([occurrence], provenance=provenance)
    dataset = ReviewDataset(source_queries=[query], occurrences=[occurrence], canonical_records=records, duplicate_decisions=decisions)
    dataset.canonical_records[0].canonical_id = "canonical:pilot5d"
    for decision in dataset.duplicate_decisions:
        decision.canonical_record_id = "canonical:pilot5d"
    dataset.validate()
    return dataset


def test_schema_and_parser_are_eligibility_only():
    schema = pilot5d_response_schema()
    forbidden = {"assistance_modes", "visualization_modalities", "tasks", "primary_exclusion_reason", "secondary_exclusion_reasons"}

    assert forbidden.isdisjoint(schema["properties"])
    assert forbidden.isdisjoint(schema["required"])
    payload = _payload()
    payload["assistance_modes"] = []
    try:
        parse_pilot5d_proposal(payload, _input())
    except ValueError as exc:
        assert "extra=['assistance_modes']" in str(exc)
    else:
        raise AssertionError("taxonomy output must be rejected by the 5D parser")


def test_taxonomy_absence_materializes_an_eligibility_proposal_without_annotations():
    dataset = _dataset()
    run = register_inference_run(dataset, stage=ScreeningStage.TITLE_ABSTRACT, prompt_path=PILOT5D_PROMPT, provider="mock", model="mock", parameters={"response_schema_version": PILOT5D_OUTPUT_SCHEMA_VERSION}, created_at="run-time", prompt_version=PILOT5D_PROMPT_VERSION, output_schema_version=PILOT5D_OUTPUT_SCHEMA_VERSION)
    payload = _payload(decisions={EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO})
    payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]["evidence"] = [_evidence("Life science", claimed_start=9999)]
    attempt = run_inference_attempt(dataset, run_id=run.run_id, inference_input=_input(), provider=MockInferenceProvider([json.dumps(payload)]), timestamp=lambda: "attempt-time")

    assert attempt.validation_status is InferenceValidationStatus.VALID
    assert attempt.annotation_ids == []
    assert dataset.annotations == []
    decision = dataset.screening_decisions[0]
    assert decision.primary_exclusion_reason is ExclusionReason.NO_LIFE_SCIENCE_APPLICATION
    assert decision.provenance.metadata["derived_screening"]["actor_id"] == "software:h2h-lit-pipeline:stage3-derivation"
    assert decision.provenance.metadata["human_review_audit"]["actor_id"] == "software:h2h-lit-pipeline:stage5d-eligibility-audit"
    e1 = next(item for item in decision.criteria if item.criterion is EligibilityCriterion.LIFE_SCIENCE_APPLICATION)
    evidence = next(item for item in dataset.evidence if item.evidence_id == e1.evidence_ids[0])
    assert evidence.metadata["start"] == _input().text.index("Life science")
    assert evidence.metadata["model_claimed_start"] == 9999
    assert evidence.metadata["resolution_method"] == "unique_exact_substring"


def test_malformed_eligibility_evidence_remains_invalid():
    dataset = _dataset()
    run = register_inference_run(dataset, stage=ScreeningStage.TITLE_ABSTRACT, prompt_path=PILOT5D_PROMPT, provider="mock", model="mock", parameters={"response_schema_version": PILOT5D_OUTPUT_SCHEMA_VERSION}, created_at="run-time", prompt_version=PILOT5D_PROMPT_VERSION, output_schema_version=PILOT5D_OUTPUT_SCHEMA_VERSION)
    payload = _payload()
    payload["criteria"][EligibilityCriterion.COMPUTATIONAL_ASSISTANCE.value]["evidence"] = [_evidence("paraphrased assistance")]
    attempt = run_inference_attempt(dataset, run_id=run.run_id, inference_input=_input(), provider=MockInferenceProvider([json.dumps(payload)]), timestamp=lambda: "attempt-time")

    assert attempt.validation_status is InferenceValidationStatus.INVALID
    assert "zero exact matches" in attempt.validation_errors[0]
    assert dataset.screening_decisions == []


def test_opaque_provenance_is_model_visible_without_historical_path_or_taxonomy_fields():
    record = SimpleNamespace(
        canonical_id="canonical:historical",
        record=SimpleNamespace(
            title=TITLE,
            abstract=ABSTRACT,
            year=2024,
            doi=None,
            source_identifier="historical:key",
            original_metadata={"pilot_selection": {"path": "BroadSearches2/Conversational/Conversational.bib"}},
        ),
    )
    inference_input = pilot5d_inference_input_for(record, pilot_execution_date="2026-09-01")
    snapshot = json.dumps(inference_input.to_snapshot(), sort_keys=True)

    assert inference_input.source_artifact_id.startswith("provenance:artifact:")
    assert "BroadSearches2" not in snapshot
    assert "assistance_modes" not in snapshot
    assert "visualization_modalities" not in snapshot
    assert "tasks" not in snapshot


def test_selection_and_stage5c_schema_remain_isolated():
    config = load_pilot5d_config()
    stage5c_schema = pilot5c_response_schema()

    assert config["selection_config"] == "config/stage5_pilot_v1.json"
    assert {"assistance_modes", "visualization_modalities", "tasks"} <= set(stage5c_schema["properties"])
    assert {"assistance_modes", "visualization_modalities", "tasks"} <= set(stage5c_schema["required"])
