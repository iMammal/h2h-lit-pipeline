from __future__ import annotations

from pathlib import Path

import pytest

from h2h_lit.models import LiteratureRecord
from h2h_lit.prisma import reconcile_prisma
from h2h_lit.retrieval import load_review_dataset, save_review_dataset
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    EligibilityCriterion,
    EligibilityStatus,
    EvidenceReference,
    EvidenceSource,
    ExclusionReason,
    RecordOccurrence,
    ReviewDataset,
    ScreeningStage,
    SourceQuery,
    SynthesisPriority,
    SynthesisPriorityDecision,
    TriState,
    canonicalize_occurrences,
)
from h2h_lit.screening import (
    ScreeningSubmission,
    derive_eligibility_status,
    finalize_corpus_membership,
    record_screening_decision,
)


def _provenance(
    actor_type: ActorType,
    authority: DecisionAuthority,
    created_at: str,
    *,
    supersedes: list[str] | None = None,
) -> DecisionProvenance:
    return DecisionProvenance(
        actor=DecisionActor(
            actor_id=f"{actor_type.value}:{created_at}",
            actor_type=actor_type,
        ),
        authority=authority,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=created_at,
        supersedes_ids=list(supersedes or []),
    )


def _dataset(record_count: int = 1) -> ReviewDataset:
    query = SourceQuery(
        query_id="query:stage3",
        source_database="MockSource",
        query_text="mocked records",
        query_version="mock-v1",
        retrieval_started_at="retrieval-start",
        retrieval_ended_at="retrieval-end",
        result_count=record_count,
    )
    occurrences = [
        RecordOccurrence(
            occurrence_id=f"occurrence:{index}",
            source_query_id=query.query_id,
            source_identifier=f"source:{index}",
            retrieved_at="retrieval-end",
            source_rank=index,
            record=LiteratureRecord(
                title=f"Paper {index}",
                doi=f"10.1000/stage3-{index}",
                source_database="MockSource",
                source_identifier=f"source:{index}",
            ),
        )
        for index in range(1, record_count + 1)
    ]
    canonical, dedupe = canonicalize_occurrences(
        occurrences,
        provenance=_provenance(
            ActorType.SOFTWARE,
            DecisionAuthority.DETERMINISTIC,
            "dedupe-time",
        ),
    )
    dataset = ReviewDataset(
        source_queries=[query],
        occurrences=occurrences,
        canonical_records=canonical,
        duplicate_decisions=dedupe,
    )
    dataset.validate()
    return dataset


def _add_evidence(
    dataset: ReviewDataset,
    canonical_record_id: str,
    suffix: str,
    source: EvidenceSource,
) -> str:
    evidence_id = f"evidence:{suffix}:{canonical_record_id}"
    dataset.evidence.append(
        EvidenceReference(
            evidence_id=evidence_id,
            canonical_record_id=canonical_record_id,
            source=source,
            locator=suffix,
            quote="Mocked evidence for offline screening.",
        )
    )
    return evidence_id


def _values(
    overrides: dict[EligibilityCriterion, TriState] | None = None,
) -> dict[EligibilityCriterion, TriState]:
    overrides = overrides or {}
    return {
        criterion: overrides.get(criterion, TriState.YES)
        for criterion in EligibilityCriterion
    }


def _submission(
    *,
    canonical_record_id: str,
    stage: ScreeningStage,
    evidence_id: str,
    provenance: DecisionProvenance,
    values: dict[EligibilityCriterion, TriState] | None = None,
    primary_reason: ExclusionReason | None = None,
    secondary_reasons: list[ExclusionReason] | None = None,
    technical_failure_criteria: list[EligibilityCriterion] | None = None,
    technical_errors: list[str] | None = None,
) -> ScreeningSubmission:
    criterion_values = values or _values()
    return ScreeningSubmission(
        canonical_record_id=canonical_record_id,
        stage=stage,
        criterion_values=criterion_values,
        criterion_evidence_ids={
            criterion: [evidence_id] for criterion in EligibilityCriterion
        },
        provenance=provenance,
        primary_exclusion_reason=primary_reason,
        secondary_exclusion_reasons=list(secondary_reasons or []),
        technical_failure_criteria=list(technical_failure_criteria or []),
        technical_errors=list(technical_errors or []),
    )


def test_uncertainty_escalates_then_human_supersedes_and_persists(tmp_path: Path):
    dataset = _dataset()
    record_id = dataset.canonical_records[0].canonical_id
    abstract_evidence = _add_evidence(
        dataset, record_id, "abstract", EvidenceSource.TITLE_ABSTRACT
    )
    full_text_evidence = _add_evidence(
        dataset, record_id, "full-text", EvidenceSource.FULL_TEXT
    )
    machine = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=abstract_evidence,
            values=_values(
                {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN}
            ),
            provenance=_provenance(
                ActorType.LLM, DecisionAuthority.PROPOSED, "machine-time"
            ),
        ),
    )

    assert machine.decision.status is EligibilityStatus.UNCERTAIN
    assert machine.full_text_required is True
    assert machine.corpus_membership is None

    human = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.FULL_TEXT,
            evidence_id=full_text_evidence,
            provenance=_provenance(
                ActorType.HUMAN,
                DecisionAuthority.DECIDED,
                "human-time",
                supersedes=[machine.decision.decision_id],
            ),
        ),
        finalize_membership=True,
    )

    assert human.decision.status is EligibilityStatus.ELIGIBLE
    assert human.corpus_membership is not None
    assert human.corpus_membership.screening_decision_id == human.decision.decision_id
    assert len(dataset.screening_decisions) == 2
    assert dataset.effective_screening_decisions() == [human.decision]

    first = tmp_path / "screened.json"
    second = tmp_path / "screened-again.json"
    assert save_review_dataset(first, dataset) == save_review_dataset(second, dataset)
    restored = load_review_dataset(first)
    assert restored.to_json() == dataset.to_json()
    assert restored.screening_decisions[0].criteria[0].evidence_ids == [abstract_evidence]


def test_conflicting_human_decisions_require_adjudication_before_membership():
    dataset = _dataset()
    record_id = dataset.canonical_records[0].canonical_id
    evidence_id = _add_evidence(dataset, record_id, "abstract", EvidenceSource.TITLE_ABSTRACT)

    include = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence_id,
            provenance=_provenance(
                ActorType.HUMAN, DecisionAuthority.DECIDED, "coder-one"
            ),
        ),
    )
    exclude = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence_id,
            values=_values(
                {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO}
            ),
            primary_reason=ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
            provenance=_provenance(
                ActorType.HUMAN, DecisionAuthority.DECIDED, "coder-two"
            ),
        ),
    )

    with pytest.raises(ValueError, match="one effective prospective screening"):
        finalize_corpus_membership(dataset, canonical_record_id=record_id)

    adjudicated = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence_id,
            provenance=_provenance(
                ActorType.ADJUDICATOR,
                DecisionAuthority.ADJUDICATED,
                "adjudication-time",
                supersedes=[include.decision.decision_id, exclude.decision.decision_id],
            ),
        ),
        finalize_membership=True,
    )

    assert dataset.effective_screening_decisions() == [adjudicated.decision]
    assert adjudicated.corpus_membership is not None
    assert adjudicated.corpus_membership.provenance.authority is DecisionAuthority.ADJUDICATED


def test_llm_proposal_cannot_create_membership_and_human_can_supersede_it():
    dataset = _dataset()
    record_id = dataset.canonical_records[0].canonical_id
    evidence_id = _add_evidence(dataset, record_id, "abstract", EvidenceSource.TITLE_ABSTRACT)
    with pytest.raises(
        ValueError, match="LLM proposals cannot create authoritative corpus membership"
    ):
        record_screening_decision(
            dataset,
            _submission(
                canonical_record_id=record_id,
                stage=ScreeningStage.TITLE_ABSTRACT,
                evidence_id=evidence_id,
                provenance=_provenance(
                    ActorType.LLM, DecisionAuthority.PROPOSED, "machine-proposal"
                ),
            ),
            finalize_membership=True,
        )

    machine = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence_id,
            provenance=_provenance(
                ActorType.LLM, DecisionAuthority.PROPOSED, "machine-proposal"
            ),
        ),
    )
    human = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence_id,
            values=_values(
                {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO}
            ),
            primary_reason=ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
            provenance=_provenance(
                ActorType.HUMAN,
                DecisionAuthority.DECIDED,
                "human-review",
                supersedes=[machine.decision.decision_id],
            ),
        ),
        finalize_membership=True,
    )

    assert len(dataset.screening_decisions) == 2
    assert len(dataset.corpus_memberships) == 1
    assert dataset.effective_screening_decisions() == [human.decision]
    assert dataset.effective_corpus_memberships() == [human.corpus_membership]
    assert human.corpus_membership is not None
    assert machine.corpus_membership is None
    assert human.corpus_membership.provenance.supersedes_ids == []


def test_excluded_record_requires_complete_ordered_frozen_reasons():
    dataset = _dataset()
    record_id = dataset.canonical_records[0].canonical_id
    evidence_id = _add_evidence(dataset, record_id, "abstract", EvidenceSource.TITLE_ABSTRACT)
    values = _values(
        {
            EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO,
            EligibilityCriterion.COMPUTATIONAL_ASSISTANCE: TriState.NO,
        }
    )
    base = {
        "canonical_record_id": record_id,
        "stage": ScreeningStage.TITLE_ABSTRACT,
        "evidence_id": evidence_id,
        "values": values,
        "provenance": _provenance(
            ActorType.HUMAN, DecisionAuthority.DECIDED, "exclude-time"
        ),
    }

    with pytest.raises(ValueError, match="primary frozen exclusion reason"):
        record_screening_decision(dataset, _submission(**base))

    result = record_screening_decision(
        dataset,
        _submission(
            **base,
            primary_reason=ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
            secondary_reasons=[ExclusionReason.NO_QUALIFYING_ASSISTANCE],
        ),
        finalize_membership=True,
    )

    assert result.decision.status is EligibilityStatus.EXCLUDED
    assert result.corpus_membership is not None
    assert result.corpus_membership.status is EligibilityStatus.EXCLUDED


def test_parser_failure_remains_uncertain_and_cannot_enable_priority():
    dataset = _dataset()
    record_id = dataset.canonical_records[0].canonical_id
    abstract_evidence = _add_evidence(
        dataset, record_id, "abstract", EvidenceSource.TITLE_ABSTRACT
    )
    failure_evidence = _add_evidence(dataset, record_id, "parser", EvidenceSource.OTHER)
    machine = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=abstract_evidence,
            values=_values(
                {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN}
            ),
            provenance=_provenance(
                ActorType.LLM, DecisionAuthority.PROPOSED, "machine-time"
            ),
        ),
    )
    human_provenance = _provenance(
        ActorType.HUMAN,
        DecisionAuthority.DECIDED,
        "human-time",
        supersedes=[machine.decision.decision_id],
    )
    unresolved = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_id,
            stage=ScreeningStage.FULL_TEXT,
            evidence_id=failure_evidence,
            values=_values(
                {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN}
            ),
            provenance=human_provenance,
            technical_failure_criteria=[EligibilityCriterion.EVIDENCE_SUFFICIENCY],
            technical_errors=["parser failed on mocked PDF"],
        ),
    )

    assert unresolved.decision.status is EligibilityStatus.UNCERTAIN
    assert unresolved.decision.technical_errors == ["parser failed on mocked PDF"]
    with pytest.raises(ValueError, match="unresolved decisions cannot be finalized"):
        finalize_corpus_membership(dataset, canonical_record_id=record_id)

    bad_submission = _submission(
        canonical_record_id=record_id,
        stage=ScreeningStage.FULL_TEXT,
        evidence_id=failure_evidence,
        values=_values({EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.NO}),
        primary_reason=ExclusionReason.INSUFFICIENT_EVIDENCE_AFTER_ESCALATION,
        provenance=_provenance(
            ActorType.HUMAN,
            DecisionAuthority.DECIDED,
            "bad-failure-time",
            supersedes=[unresolved.decision.decision_id],
        ),
        technical_failure_criteria=[EligibilityCriterion.EVIDENCE_SUFFICIENCY],
        technical_errors=["PDF unavailable"],
    )
    with pytest.raises(ValueError, match="failures must remain UNCERTAIN"):
        record_screening_decision(dataset, bad_submission)

    dataset.synthesis_priorities.append(
        SynthesisPriorityDecision(
            decision_id="priority:premature",
            canonical_record_id=record_id,
            priority=SynthesisPriority.CORE,
            evidence_ids=[failure_evidence],
            provenance=_provenance(
                ActorType.HUMAN, DecisionAuthority.DECIDED, "priority-time"
            ),
        )
    )
    with pytest.raises(ValueError, match="requires effective eligible membership"):
        dataset.validate()


def test_prisma_screening_counts_reconcile_from_effective_stored_state():
    dataset = _dataset(4)
    record_ids = [record.canonical_id for record in dataset.canonical_records]
    evidence = {
        record_id: _add_evidence(
            dataset, record_id, "abstract", EvidenceSource.TITLE_ABSTRACT
        )
        for record_id in record_ids
    }

    record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_ids[0],
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence[record_ids[0]],
            provenance=_provenance(
                ActorType.HUMAN, DecisionAuthority.DECIDED, "eligible-one"
            ),
        ),
        finalize_membership=True,
    )
    record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_ids[1],
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence[record_ids[1]],
            values=_values(
                {EligibilityCriterion.LIFE_SCIENCE_APPLICATION: TriState.NO}
            ),
            primary_reason=ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
            provenance=_provenance(
                ActorType.HUMAN, DecisionAuthority.DECIDED, "excluded-one"
            ),
        ),
        finalize_membership=True,
    )
    record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_ids[2],
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence[record_ids[2]],
            values=_values(
                {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN}
            ),
            provenance=_provenance(
                ActorType.LLM, DecisionAuthority.PROPOSED, "unresolved-one"
            ),
        ),
    )
    escalated = record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_ids[3],
            stage=ScreeningStage.TITLE_ABSTRACT,
            evidence_id=evidence[record_ids[3]],
            values=_values(
                {EligibilityCriterion.EVIDENCE_SUFFICIENCY: TriState.UNCERTAIN}
            ),
            provenance=_provenance(
                ActorType.LLM, DecisionAuthority.PROPOSED, "escalated-machine"
            ),
        ),
    )
    full_text_evidence = _add_evidence(
        dataset, record_ids[3], "full-text", EvidenceSource.FULL_TEXT
    )
    record_screening_decision(
        dataset,
        _submission(
            canonical_record_id=record_ids[3],
            stage=ScreeningStage.FULL_TEXT,
            evidence_id=full_text_evidence,
            provenance=_provenance(
                ActorType.HUMAN,
                DecisionAuthority.DECIDED,
                "eligible-two",
                supersedes=[escalated.decision.decision_id],
            ),
        ),
        finalize_membership=True,
    )

    report = reconcile_prisma(dataset)

    assert report.records_after_deduplication == 4
    assert report.records_screened == 4
    assert report.title_abstract_screened == 4
    assert report.full_text_assessed == 1
    assert report.eligible_records == 2
    assert report.excluded_records == 1
    assert report.unresolved_records == 1
    assert report.excluded_by_primary_reason == {
        ExclusionReason.NO_LIFE_SCIENCE_APPLICATION.value: 1
    }
    assert report.effective_screening_decisions_by_authority == {
        DecisionAuthority.DECIDED.value: 3,
        DecisionAuthority.PROPOSED.value: 1,
    }
    assert report.effective_memberships_by_authority == {
        DecisionAuthority.DECIDED.value: 3
    }
    assert (
        report.eligible_records + report.excluded_records + report.unresolved_records
        == report.records_screened
    )


def test_aggregate_status_is_derived_only_from_e1_through_e7():
    assert derive_eligibility_status(_values()) is EligibilityStatus.ELIGIBLE
    assert (
        derive_eligibility_status(
            _values({EligibilityCriterion.COMPUTATIONAL_ASSISTANCE: TriState.NO})
        )
        is EligibilityStatus.EXCLUDED
    )
    assert (
        derive_eligibility_status(
            _values({EligibilityCriterion.COMPUTATIONAL_ASSISTANCE: TriState.UNCERTAIN})
        )
        is EligibilityStatus.UNCERTAIN
    )
