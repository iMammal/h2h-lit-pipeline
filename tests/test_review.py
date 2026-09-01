import pytest

from h2h_lit.models import LiteratureRecord, ProcessingStatus, ProvenanceEvent, ProvenanceKind
from h2h_lit.review import (
    ActorType,
    AnnotationDimension,
    AnnotationState,
    AssistanceMode,
    CanonicalRecord,
    CorpusMembership,
    CriterionDecision,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    DedupeOutcome,
    DimensionAnnotation,
    EligibilityCriterion,
    EligibilityStatus,
    EvidenceReference,
    EvidenceSource,
    ExclusionReason,
    RecordOccurrence,
    RetrievalCompletionStatus,
    ReviewDataset,
    ScreeningDecision,
    ScreeningStage,
    SourceQuery,
    SynthesisPriority,
    SynthesisPriorityDecision,
    TaskCategory,
    TriState,
    VisualizationModality,
    canonicalize_occurrences,
)

NOW = "2026-08-30T12:00:00+00:00"


def test_legacy_failed_source_query_infers_failed_completion_status():
    query = SourceQuery.from_dict(
        {
            "query_id": "query:legacy:failed",
            "source_database": "PubMed",
            "query_text": "legacy",
            "retrieval_started_at": NOW,
            "retrieval_ended_at": NOW,
            "status": ProcessingStatus.FAILED.value,
            "errors": ["historical failure"],
        }
    )

    assert query.completion_status is RetrievalCompletionStatus.FAILED


def _actor(actor_type: ActorType) -> DecisionActor:
    return DecisionActor(actor_id=f"actor:{actor_type.value}", actor_type=actor_type)


def _provenance(
    actor_type: ActorType,
    authority: DecisionAuthority,
    *,
    scope: DecisionScope = DecisionScope.PROSPECTIVE,
    supersedes: list[str] | None = None,
) -> DecisionProvenance:
    return DecisionProvenance(
        actor=_actor(actor_type),
        authority=authority,
        scope=scope,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=NOW,
        supersedes_ids=list(supersedes or []),
    )


def _query() -> SourceQuery:
    return SourceQuery(
        query_id="query:pubmed:1",
        source_database="PubMed",
        query_text="visual analytics AND life science",
        retrieval_started_at=NOW,
        retrieval_ended_at=NOW,
        fields=["title", "abstract"],
        filters={"language": "none_at_retrieval"},
    )


def _occurrence(
    occurrence_id: str,
    *,
    title: str,
    doi: str | None,
    source_identifier: str,
) -> RecordOccurrence:
    return RecordOccurrence(
        occurrence_id=occurrence_id,
        source_query_id="query:pubmed:1",
        source_identifier=source_identifier,
        retrieved_at=NOW,
        record=LiteratureRecord(
            title=title,
            abstract="Evidence abstract.",
            doi=doi,
            source_database="PubMed",
            source_identifier=source_identifier,
            original_metadata={"source_id": source_identifier},
            provenance=[
                ProvenanceEvent(
                    kind=ProvenanceKind.SOURCE_DERIVED,
                    stage="source_query",
                    source_database="PubMed",
                    source_identifier=source_identifier,
                )
            ],
        ),
    )


def _criteria(
    evidence_id: str,
    overrides: dict[EligibilityCriterion, TriState] | None = None,
) -> list[CriterionDecision]:
    overrides = overrides or {}
    return [
        CriterionDecision(
            criterion=criterion,
            value=overrides.get(criterion, TriState.YES),
            evidence_ids=[evidence_id],
        )
        for criterion in EligibilityCriterion
    ]


def _canonicalized_dataset() -> tuple[ReviewDataset, CanonicalRecord]:
    occurrences = [
        _occurrence(
            "occurrence:pubmed:1",
            title="Assisted Visual Analytics",
            doi="10.1000/example",
            source_identifier="1",
        ),
        _occurrence(
            "occurrence:crossref:1",
            title="Assisted Visual Analytics",
            doi="https://doi.org/10.1000/example",
            source_identifier="10.1000/example",
        ),
    ]
    queries = [_query()]
    queries.append(
        SourceQuery(
            query_id="query:crossref:1",
            source_database="CrossRef",
            query_text="visual analytics AND life science",
            retrieval_started_at=NOW,
            retrieval_ended_at=NOW,
        )
    )
    occurrences[1].source_query_id = "query:crossref:1"
    canonical, decisions = canonicalize_occurrences(
        occurrences,
        provenance=_provenance(ActorType.SOFTWARE, DecisionAuthority.DETERMINISTIC),
    )
    dataset = ReviewDataset(
        source_queries=queries,
        occurrences=occurrences,
        canonical_records=canonical,
        duplicate_decisions=decisions,
    )
    return dataset, canonical[0]


def test_canonicalization_preserves_every_occurrence_and_identifies_survivor():
    dataset, canonical = _canonicalized_dataset()

    dataset.validate()

    assert len(dataset.occurrences) == 2
    assert canonical.occurrence_ids == ["occurrence:pubmed:1", "occurrence:crossref:1"]
    assert len(dataset.duplicate_decisions) == 2
    duplicate = next(
        item for item in dataset.duplicate_decisions if item.outcome is DedupeOutcome.DUPLICATE
    )
    assert duplicate.survivor_occurrence_id == "occurrence:pubmed:1"
    assert duplicate.canonical_record_id == canonical.canonical_id


def test_canonicalization_uses_occurrence_fallback_without_dropping_missing_keys():
    occurrence = _occurrence(
        "occurrence:pubmed:missing",
        title="",
        doi=None,
        source_identifier="missing",
    )
    canonical, decisions = canonicalize_occurrences(
        [occurrence],
        provenance=_provenance(ActorType.SOFTWARE, DecisionAuthority.DETERMINISTIC),
    )
    dataset = ReviewDataset(
        source_queries=[_query()],
        occurrences=[occurrence],
        canonical_records=canonical,
        duplicate_decisions=decisions,
    )

    dataset.validate()

    assert canonical[0].occurrence_ids == [occurrence.occurrence_id]
    assert decisions[0].match_rule == "occurrence_fallback_missing_doi_and_title"


def test_round_trip_is_deterministic_and_human_adjudication_supersedes_machine():
    dataset, canonical = _canonicalized_dataset()
    evidence = EvidenceReference(
        evidence_id="evidence:abstract:1",
        canonical_record_id=canonical.canonical_id,
        source=EvidenceSource.TITLE_ABSTRACT,
        locator="abstract",
        quote="Interactive life-science visual analytics with computational guidance.",
    )
    dataset.evidence.append(evidence)

    machine = ScreeningDecision(
        decision_id="screen:machine",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        criteria=_criteria(
            evidence.evidence_id,
            {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN},
        ),
        status=EligibilityStatus.UNCERTAIN,
        provenance=_provenance(ActorType.LLM, DecisionAuthority.PROPOSED),
    )
    human = ScreeningDecision(
        decision_id="screen:human",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.FULL_TEXT,
        criteria=_criteria(evidence.evidence_id),
        status=EligibilityStatus.ELIGIBLE,
        provenance=_provenance(
            ActorType.HUMAN,
            DecisionAuthority.DECIDED,
            supersedes=[machine.decision_id],
        ),
    )
    adjudicated = ScreeningDecision(
        decision_id="screen:adjudicated",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.FULL_TEXT,
        criteria=_criteria(evidence.evidence_id),
        status=EligibilityStatus.ELIGIBLE,
        provenance=_provenance(
            ActorType.ADJUDICATOR,
            DecisionAuthority.ADJUDICATED,
            supersedes=[human.decision_id],
        ),
    )
    historical = ScreeningDecision(
        decision_id="screen:historical",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        criteria=_criteria(
            evidence.evidence_id,
            {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO},
        ),
        status=EligibilityStatus.EXCLUDED,
        primary_exclusion_reason=ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
        provenance=_provenance(
            ActorType.LLM,
            DecisionAuthority.PROPOSED,
            scope=DecisionScope.HISTORICAL,
        ),
    )
    dataset.screening_decisions.extend([historical, machine, human, adjudicated])
    dataset.corpus_memberships.append(
        CorpusMembership(
            decision_id="membership:adjudicated",
            canonical_record_id=canonical.canonical_id,
            status=EligibilityStatus.ELIGIBLE,
            screening_decision_id=adjudicated.decision_id,
            provenance=_provenance(
                ActorType.ADJUDICATOR,
                DecisionAuthority.ADJUDICATED,
            ),
        )
    )
    dataset.annotations.extend(
        [
            DimensionAnnotation(
                annotation_id="annotation:algorithmic",
                canonical_record_id=canonical.canonical_id,
                dimension=AnnotationDimension.ASSISTANCE_MODE,
                value=AssistanceMode.ALGORITHMIC.value,
                state=AnnotationState.PRESENT,
                evidence_ids=[evidence.evidence_id],
                provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
            ),
            DimensionAnnotation(
                annotation_id="annotation:desktop",
                canonical_record_id=canonical.canonical_id,
                dimension=AnnotationDimension.VISUALIZATION_MODALITY,
                value=VisualizationModality.DESKTOP_2D.value,
                state=AnnotationState.PRESENT,
                evidence_ids=[evidence.evidence_id],
                provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
            ),
            DimensionAnnotation(
                annotation_id="annotation:task",
                canonical_record_id=canonical.canonical_id,
                dimension=AnnotationDimension.TASK,
                value=TaskCategory.SENSEMAKING_HYPOTHESIS.value,
                state=AnnotationState.PRESENT,
                evidence_ids=[evidence.evidence_id],
                provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
            ),
        ]
    )
    dataset.synthesis_priorities.append(
        SynthesisPriorityDecision(
            decision_id="priority:core",
            canonical_record_id=canonical.canonical_id,
            priority=SynthesisPriority.CORE,
            evidence_ids=[evidence.evidence_id],
            provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
        )
    )

    encoded = dataset.to_json()
    restored = ReviewDataset.from_json(encoded)

    assert restored.to_json() == encoded
    assert restored.to_dict() == dataset.to_dict()
    assert [item.decision_id for item in restored.effective_screening_decisions()] == [
        adjudicated.decision_id
    ]


def test_validation_rejects_an_unaccounted_occurrence():
    dataset, _ = _canonicalized_dataset()
    dataset.duplicate_decisions.pop()

    with pytest.raises(ValueError, match="every occurrence needs one dedupe decision"):
        dataset.validate()


def test_validation_rejects_duplicate_without_canonical_survivor():
    dataset, _ = _canonicalized_dataset()
    duplicate = next(
        item for item in dataset.duplicate_decisions if item.outcome is DedupeOutcome.DUPLICATE
    )
    duplicate.survivor_occurrence_id = "occurrence:missing"

    with pytest.raises(ValueError, match="missing survivor occurrence"):
        dataset.validate()


def test_excluded_screening_requires_explicit_reason():
    dataset, canonical = _canonicalized_dataset()
    evidence = EvidenceReference(
        evidence_id="evidence:metadata:1",
        canonical_record_id=canonical.canonical_id,
        source=EvidenceSource.METADATA,
        locator="record",
    )
    dataset.evidence.append(evidence)
    dataset.screening_decisions.append(
        ScreeningDecision(
            decision_id="screen:excluded",
            canonical_record_id=canonical.canonical_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            criteria=_criteria(
                evidence.evidence_id,
                {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO},
            ),
            status=EligibilityStatus.EXCLUDED,
            provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
        )
    )

    with pytest.raises(ValueError, match="primary exclusion reason"):
        dataset.validate()


def test_frozen_annotation_vocab_rejects_mobile_as_a_modality():
    dataset, canonical = _canonicalized_dataset()
    dataset.annotations.append(
        DimensionAnnotation(
            annotation_id="annotation:mobile",
            canonical_record_id=canonical.canonical_id,
            dimension=AnnotationDimension.VISUALIZATION_MODALITY,
            value="Mobile",
            state=AnnotationState.PRESENT,
            rationale="Phone form factor is not a STAR modality.",
            provenance=_provenance(ActorType.LLM, DecisionAuthority.PROPOSED),
        )
    )

    with pytest.raises(ValueError, match="invalid visualization_modality value"):
        dataset.validate()


def test_synthesis_priority_requires_eligible_membership():
    dataset, canonical = _canonicalized_dataset()
    evidence = EvidenceReference(
        evidence_id="evidence:metadata:excluded",
        canonical_record_id=canonical.canonical_id,
        source=EvidenceSource.METADATA,
        locator="record",
    )
    dataset.evidence.append(evidence)
    screen = ScreeningDecision(
        decision_id="screen:excluded",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.FULL_TEXT,
        criteria=_criteria(
            evidence.evidence_id,
            {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO},
        ),
        status=EligibilityStatus.EXCLUDED,
        primary_exclusion_reason=ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
        provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
    )
    dataset.screening_decisions.append(screen)
    dataset.corpus_memberships.append(
        CorpusMembership(
            decision_id="membership:excluded",
            canonical_record_id=canonical.canonical_id,
            status=EligibilityStatus.EXCLUDED,
            screening_decision_id=screen.decision_id,
            provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
        )
    )
    dataset.synthesis_priorities.append(
        SynthesisPriorityDecision(
            decision_id="priority:invalid",
            canonical_record_id=canonical.canonical_id,
            priority=SynthesisPriority.CONTEXTUAL,
            rationale="Priority cannot make an excluded paper eligible.",
            provenance=_provenance(ActorType.HUMAN, DecisionAuthority.DECIDED),
        )
    )

    with pytest.raises(ValueError, match="requires effective eligible membership"):
        dataset.validate()


def test_effective_membership_cannot_reference_a_superseded_screening_decision():
    dataset, canonical = _canonicalized_dataset()
    evidence = EvidenceReference(
        evidence_id="evidence:membership:1",
        canonical_record_id=canonical.canonical_id,
        source=EvidenceSource.FULL_TEXT,
        locator="full text",
    )
    dataset.evidence.append(evidence)
    machine = ScreeningDecision(
        decision_id="screen:machine:uncertain",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        criteria=_criteria(
            evidence.evidence_id,
            {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN},
        ),
        status=EligibilityStatus.UNCERTAIN,
        provenance=_provenance(ActorType.LLM, DecisionAuthority.PROPOSED),
    )
    human = ScreeningDecision(
        decision_id="screen:human:eligible",
        canonical_record_id=canonical.canonical_id,
        stage=ScreeningStage.FULL_TEXT,
        criteria=_criteria(evidence.evidence_id),
        status=EligibilityStatus.ELIGIBLE,
        provenance=_provenance(
            ActorType.HUMAN,
            DecisionAuthority.DECIDED,
            supersedes=[machine.decision_id],
        ),
    )
    dataset.screening_decisions.extend([machine, human])
    dataset.corpus_memberships.append(
        CorpusMembership(
            decision_id="membership:stale",
            canonical_record_id=canonical.canonical_id,
            status=EligibilityStatus.UNCERTAIN,
            screening_decision_id=machine.decision_id,
            provenance=_provenance(ActorType.LLM, DecisionAuthority.PROPOSED),
        )
    )

    with pytest.raises(
        ValueError, match="LLM proposals cannot create authoritative corpus membership"
    ):
        dataset.validate()
