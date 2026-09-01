from __future__ import annotations

import json
from types import SimpleNamespace

from h2h_lit.inference import (
    InferenceInput,
    MockInferenceProvider,
    load_prompt_artifact,
    register_inference_run,
    run_inference_attempt,
)
from h2h_lit.models import LiteratureRecord
from h2h_lit.pilot5c import (
    PILOT5C_OUTPUT_SCHEMA_VERSION,
    PILOT5C_PROMPT,
    PILOT5C_PROMPT_VERSION,
    original_source_artifact_path_for,
    parse_pilot5c_proposal,
    pilot5c_inference_input_for,
    pilot5c_response_schema,
)
from h2h_lit.review import (
    AnnotationState,
    AssistanceMode,
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
    TaskCategory,
    TriState,
    VisualizationModality,
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


def _input(*, title: str = TITLE, abstract: str = ABSTRACT) -> InferenceInput:
    admin = {
        "publication_date": "within_scope",
        "language": "unknown",
        "document_type": "eligible",
        "full_text": "unknown",
        "pilot_administrative_observation_cutoff": "2026-08-31",
        "pilot_only": True,
        "production_retrieval_cutoff_status": "not_established_by_pilot",
    }
    admin_text = json.dumps(admin, sort_keys=True, separators=(",", ":"))
    title_prefix = "TITLE:\n"
    abstract_prefix = "\n\nABSTRACT:\n"
    admin_prefix = "\n\nADMINISTRATIVE_METADATA:\n"
    text = title_prefix + title + abstract_prefix + abstract + admin_prefix + admin_text
    title_start = len(title_prefix)
    abstract_start = title_start + len(title) + len(abstract_prefix)
    admin_start = abstract_start + len(abstract) + len(admin_prefix)
    return InferenceInput(
        canonical_record_id="canonical:pilot5c",
        stage=ScreeningStage.TITLE_ABSTRACT,
        text=text,
        source_location="title_abstract",
        source_artifact_id="provenance:artifact:test",
        metadata={
            "evidence_fields": {
                "title": {"locator": "input.title", "start": title_start, "end": title_start + len(title)},
                "abstract": {"locator": "input.abstract", "start": abstract_start, "end": abstract_start + len(abstract)},
                "administrative_metadata": {"locator": "input.administrative_metadata", "start": admin_start, "end": admin_start + len(admin_text)},
            },
            "pilot5b_administrative": admin,
            "e6_decision_scope": "pilot_only",
        },
    )


def _evidence(quote: str, *, source_field: str = "abstract") -> dict[str, object]:
    return {"quote": quote, "source_field": source_field, "locator": f"input.{source_field}", "claimed_start": None, "claimed_end": None}


def _payload(*, decisions: dict[EligibilityCriterion, TriState] | None = None) -> dict[str, object]:
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

    def dimensions(labels: list[str]) -> list[dict[str, object]]:
        return [{"label": label, "state": AnnotationState.ABSENT.value, "certainty": "SUPPORTED", "evidence": [], "rationale": f"No supported {label} evidence."} for label in labels]

    return {
        "criteria": criteria,
        "assistance_modes": dimensions([item.value for item in AssistanceMode]),
        "visualization_modalities": dimensions([item.value for item in VisualizationModality]),
        "tasks": dimensions([item.value for item in TaskCategory]),
        "overall_rationale": "Auditable Stage 5C proposal.",
    }


def _codes(parsed: object) -> set[str]:
    return {item["code"] for item in parsed.audit_flags}


def _dataset() -> ReviewDataset:
    query = SourceQuery(query_id="query:pilot5c", source_database="fixture", query_text="fixture", retrieval_started_at="time", retrieval_ended_at="time")
    occurrence = RecordOccurrence(occurrence_id="occurrence:pilot5c", source_query_id=query.query_id, source_identifier="fixture:1", retrieved_at="time", record=LiteratureRecord(title=TITLE, abstract=ABSTRACT))
    provenance = DecisionProvenance(actor=DecisionActor("software:test", ActorType.SOFTWARE), authority=DecisionAuthority.DETERMINISTIC, scope=DecisionScope.PROSPECTIVE, protocol_version="1.0.0", rubric_version="1.0.0", created_at="time")
    records, decisions = canonicalize_occurrences([occurrence], provenance=provenance)
    dataset = ReviewDataset(source_queries=[query], occurrences=[occurrence], canonical_records=records, duplicate_decisions=decisions)
    dataset.canonical_records[0].canonical_id = "canonical:pilot5c"
    for decision in dataset.duplicate_decisions:
        decision.canonical_record_id = "canonical:pilot5c"
    dataset.validate()
    return dataset


def test_neutral_provenance_redacts_historical_path_from_model_snapshot():
    record = SimpleNamespace(
        canonical_id="canonical:historical",
        record=SimpleNamespace(
            title=TITLE,
            abstract=ABSTRACT,
            year=2024,
            doi=None,
            source_identifier="historical:key",
            original_metadata={"pilot_selection": {"path": "BroadSearches2/sorted__fetchALL12000_IMMERSIVE_VR.bib"}},
        ),
    )
    inference_input = pilot5c_inference_input_for(record, pilot_execution_date="2026-08-31")
    snapshot = json.dumps(inference_input.to_snapshot(), sort_keys=True)

    assert inference_input.source_artifact_id.startswith("provenance:artifact:")
    assert "BroadSearches2" not in snapshot
    assert original_source_artifact_path_for(record) == "BroadSearches2/sorted__fetchALL12000_IMMERSIVE_VR.bib"


def test_visual_analytics_reused_for_e2_e3_e5_is_valid_but_audited():
    title = "Visual Analytics for life-science systems"
    inference_input = _input(title=title)
    payload = _payload()
    for criterion in (
        EligibilityCriterion.RELATIONAL_MULTISCALE_RELEVANCE,
        EligibilityCriterion.INTERACTIVE_VISUAL_ANALYTICS,
        EligibilityCriterion.HUMAN_ANALYTIC_RELATIONSHIP,
    ):
        payload["criteria"][criterion.value]["evidence"] = [_evidence("Visual Analytics", source_field="title")]

    parsed = parse_pilot5c_proposal(payload, inference_input)

    assert "AUDIT_CROSS_CRITERION_EVIDENCE_REUSE" in _codes(parsed)
    assert "AUDIT_GENERIC_OR_SHORT_CRITERION_EVIDENCE" in _codes(parsed)


def test_wang_title_reuse_is_an_audit_signal_not_an_invalid_proposal():
    title = "Can LLMs Bridge Domain and Visualization? A Case Study on High-Dimension Data Visualization in Single-Cell Transcriptomics"
    abstract = "Single-cell biology is studied with interactive visual analytics. Algorithmic methods assist analysts. A candidate system is described."
    inference_input = _input(title=title, abstract=abstract)
    payload = _payload()
    payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]["evidence"] = [_evidence("Single-cell biology")]
    payload["criteria"][EligibilityCriterion.COMPUTATIONAL_ASSISTANCE.value]["evidence"] = [_evidence("Algorithmic methods assist analysts")]
    payload["criteria"][EligibilityCriterion.EVIDENCE_SUFFICIENCY.value]["evidence"] = [_evidence("candidate system")]
    reused = "Can LLMs Bridge Domain and Visualization?"
    for criterion in (
        EligibilityCriterion.RELATIONAL_MULTISCALE_RELEVANCE,
        EligibilityCriterion.INTERACTIVE_VISUAL_ANALYTICS,
        EligibilityCriterion.HUMAN_ANALYTIC_RELATIONSHIP,
    ):
        payload["criteria"][criterion.value]["evidence"] = [_evidence(reused, source_field="title")]

    parsed = parse_pilot5c_proposal(payload, inference_input)

    assert "AUDIT_CROSS_CRITERION_EVIDENCE_REUSE" in _codes(parsed)


def test_hololens_e4_no_with_immersive_present_is_valid_but_audited():
    title = "Visualization of molecular structures using HoloLens-based augmented reality"
    abstract = "Life science molecular networks are viewed in an interactive HoloLens display. Users inspect structures manually. The candidate system is described."
    inference_input = _input(title=title, abstract=abstract)
    payload = _payload(decisions={EligibilityCriterion.COMPUTATIONAL_ASSISTANCE: TriState.NO})
    payload["criteria"][EligibilityCriterion.LIFE_SCIENCE_APPLICATION.value]["evidence"] = [_evidence("Life science molecular")]
    payload["criteria"][EligibilityCriterion.RELATIONAL_MULTISCALE_RELEVANCE.value]["evidence"] = [_evidence("molecular networks")]
    payload["criteria"][EligibilityCriterion.INTERACTIVE_VISUAL_ANALYTICS.value]["evidence"] = [_evidence("interactive HoloLens display")]
    payload["criteria"][EligibilityCriterion.COMPUTATIONAL_ASSISTANCE.value]["evidence"] = [_evidence("Users inspect structures manually")]
    payload["criteria"][EligibilityCriterion.HUMAN_ANALYTIC_RELATIONSHIP.value]["evidence"] = [_evidence("Users inspect structures manually")]
    payload["criteria"][EligibilityCriterion.EVIDENCE_SUFFICIENCY.value]["evidence"] = [_evidence("candidate system")]
    immersive = next(item for item in payload["assistance_modes"] if item["label"] == AssistanceMode.IMMERSIVE.value)
    immersive.update({"state": AnnotationState.PRESENT.value, "certainty": "SUPPORTED", "evidence": [_evidence("interactive HoloLens display")], "rationale": "The display is immersive."})

    parsed = parse_pilot5c_proposal(payload, inference_input)

    assert {"AUDIT_ASSISTANCE_PRESENT_WITH_E4_NO", "AUDIT_IMMERSIVE_DISPLAY_ONLY"} <= _codes(parsed)


def test_icave_foundational_case_reuse_is_a_non_dispositive_audit_flag():
    title = "iCAVE: an open source tool for visualizing biomolecular networks in 3D"
    inference_input = _input(title=title, abstract="iCAVE visualizes biomolecular networks in an immersive environment.")
    payload = _payload()
    for criterion in payload["criteria"].values():
        criterion["evidence"] = [_evidence("iCAVE", source_field="title")]

    parsed = parse_pilot5c_proposal(payload, inference_input)

    assert "AUDIT_CROSS_CRITERION_EVIDENCE_REUSE" in _codes(parsed)
    assert "AUDIT_GENERIC_OR_SHORT_CRITERION_EVIDENCE" in _codes(parsed)


def test_stage3_derives_exclusions_and_provenance_without_model_fields():
    dataset = _dataset()
    run = register_inference_run(dataset, stage=ScreeningStage.TITLE_ABSTRACT, prompt_path=PILOT5C_PROMPT, provider="mock", model="mock", parameters={"response_schema_version": PILOT5C_OUTPUT_SCHEMA_VERSION}, created_at="run-time", prompt_version=PILOT5C_PROMPT_VERSION, output_schema_version=PILOT5C_OUTPUT_SCHEMA_VERSION)
    payload = _payload(decisions={EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO})
    attempt = run_inference_attempt(dataset, run_id=run.run_id, inference_input=_input(), provider=MockInferenceProvider([json.dumps(payload)]), timestamp=lambda: "attempt-time")

    assert attempt.validation_status is InferenceValidationStatus.VALID
    decision = dataset.screening_decisions[0]
    assert decision.primary_exclusion_reason is ExclusionReason.NO_LIFE_SCIENCE_APPLICATION
    derived = decision.provenance.metadata["derived_screening"]
    assert derived["actor_id"] == "software:h2h-lit-pipeline:stage3-derivation"
    assert derived["primary_exclusion_reason"] == ExclusionReason.NO_LIFE_SCIENCE_APPLICATION.value
    assert "human_review_audit" in decision.provenance.metadata


def test_prompt_and_schema_remove_model_authored_derived_fields():
    prompt = load_prompt_artifact(PILOT5C_PROMPT, stage=ScreeningStage.TITLE_ABSTRACT, version=PILOT5C_PROMPT_VERSION, output_schema_version=PILOT5C_OUTPUT_SCHEMA_VERSION)
    schema = pilot5c_response_schema()

    assert "substantively supports the proposition" in prompt.content
    assert "software derives aggregate" in prompt.content
    assert "eligibility and exclusion reasons from E1-E7" in prompt.content
    assert "primary/secondary exclusion reasons" in prompt.content
    assert "primary_exclusion_reason" not in schema["properties"]
    assert "secondary_exclusion_reasons" not in schema["properties"]
