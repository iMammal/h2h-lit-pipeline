"""Deterministic offline screening orchestration for the frozen STAR rubric."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace

from h2h_lit.review import (
    ActorType,
    CorpusMembership,
    CriterionDecision,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    EligibilityCriterion,
    EligibilityStatus,
    ExclusionReason,
    ReviewDataset,
    ScreeningDecision,
    ScreeningStage,
    TriState,
)

EXCLUSION_REASON_ORDER = (
    ExclusionReason.AFTER_RETRIEVAL_END_DATE,
    ExclusionReason.NON_ENGLISH_FULL_TEXT,
    ExclusionReason.INELIGIBLE_DOCUMENT_TYPE,
    ExclusionReason.NO_LIFE_SCIENCE_APPLICATION,
    ExclusionReason.NO_RELATIONAL_OR_MULTISCALE_RELEVANCE,
    ExclusionReason.NO_INTERACTIVE_VISUAL_ANALYTICS,
    ExclusionReason.NO_QUALIFYING_ASSISTANCE,
    ExclusionReason.NO_HUMAN_ANALYTIC_RELATIONSHIP,
    ExclusionReason.INSUFFICIENT_EVIDENCE_AFTER_ESCALATION,
    ExclusionReason.OTHER_PROTOCOL_REASON,
)

REASON_CRITERIA: dict[ExclusionReason, set[EligibilityCriterion]] = {
    ExclusionReason.AFTER_RETRIEVAL_END_DATE: {EligibilityCriterion.ADMINISTRATIVE_SCOPE},
    ExclusionReason.NON_ENGLISH_FULL_TEXT: {EligibilityCriterion.ADMINISTRATIVE_SCOPE},
    ExclusionReason.INELIGIBLE_DOCUMENT_TYPE: {EligibilityCriterion.ADMINISTRATIVE_SCOPE},
    ExclusionReason.NO_LIFE_SCIENCE_APPLICATION: {
        EligibilityCriterion.LIFE_SCIENCE_APPLICATION
    },
    ExclusionReason.NO_RELATIONAL_OR_MULTISCALE_RELEVANCE: {
        EligibilityCriterion.RELATIONAL_MULTISCALE_RELEVANCE
    },
    ExclusionReason.NO_INTERACTIVE_VISUAL_ANALYTICS: {
        EligibilityCriterion.INTERACTIVE_VISUAL_ANALYTICS
    },
    ExclusionReason.NO_QUALIFYING_ASSISTANCE: {
        EligibilityCriterion.COMPUTATIONAL_ASSISTANCE
    },
    ExclusionReason.NO_HUMAN_ANALYTIC_RELATIONSHIP: {
        EligibilityCriterion.HUMAN_ANALYTIC_RELATIONSHIP
    },
    ExclusionReason.INSUFFICIENT_EVIDENCE_AFTER_ESCALATION: {
        EligibilityCriterion.EVIDENCE_SUFFICIENCY
    },
    ExclusionReason.OTHER_PROTOCOL_REASON: set(EligibilityCriterion),
}


@dataclass(slots=True)
class ScreeningSubmission:
    canonical_record_id: str
    stage: ScreeningStage
    criterion_values: Mapping[EligibilityCriterion, TriState]
    criterion_evidence_ids: Mapping[EligibilityCriterion, list[str]]
    provenance: DecisionProvenance
    criterion_rationales: Mapping[EligibilityCriterion, str] = field(default_factory=dict)
    primary_exclusion_reason: ExclusionReason | None = None
    secondary_exclusion_reasons: list[ExclusionReason] = field(default_factory=list)
    technical_failure_criteria: list[EligibilityCriterion] = field(default_factory=list)
    technical_errors: list[str] = field(default_factory=list)
    notes: str = ""
    decision_id: str | None = None


@dataclass(frozen=True, slots=True)
class ScreeningResult:
    decision: ScreeningDecision
    corpus_membership: CorpusMembership | None
    full_text_required: bool


def derive_eligibility_status(
    criterion_values: Mapping[EligibilityCriterion, TriState],
) -> EligibilityStatus:
    """Apply the frozen aggregate rule without scores or taxonomy inputs."""

    if set(criterion_values) != set(EligibilityCriterion):
        raise ValueError("aggregate eligibility requires exactly E1-E7")
    values = list(criterion_values.values())
    if any(not isinstance(value, TriState) for value in values):
        raise ValueError("criterion values must use YES, NO, or UNCERTAIN")
    if TriState.NO in values:
        return EligibilityStatus.EXCLUDED
    if all(value is TriState.YES for value in values):
        return EligibilityStatus.ELIGIBLE
    return EligibilityStatus.UNCERTAIN


def record_screening_decision(
    dataset: ReviewDataset,
    submission: ScreeningSubmission,
    *,
    finalize_membership: bool = False,
) -> ScreeningResult:
    """Append a screening decision and optionally update formal membership atomically."""

    if not any(
        record.canonical_id == submission.canonical_record_id
        for record in dataset.canonical_records
    ):
        raise ValueError(f"unknown canonical record: {submission.canonical_record_id}")
    if submission.provenance.scope is not DecisionScope.PROSPECTIVE:
        raise ValueError("Stage 3 orchestration accepts prospective decisions only")

    criteria = _build_criteria(submission)
    status = derive_eligibility_status(submission.criterion_values)
    _validate_exclusion_reasons(submission, status)
    _validate_technical_failures(submission)
    _validate_stage_lineage(dataset, submission)

    decision_id = submission.decision_id or _screening_id(submission, status)
    decision = ScreeningDecision(
        decision_id=decision_id,
        canonical_record_id=submission.canonical_record_id,
        stage=submission.stage,
        criteria=criteria,
        status=status,
        provenance=submission.provenance,
        primary_exclusion_reason=submission.primary_exclusion_reason,
        secondary_exclusion_reasons=list(submission.secondary_exclusion_reasons),
        technical_failure_criteria=list(submission.technical_failure_criteria),
        technical_errors=list(submission.technical_errors),
        notes=submission.notes,
    )

    dataset.screening_decisions.append(decision)
    membership: CorpusMembership | None = None
    try:
        if finalize_membership:
            membership = _append_membership(dataset, decision)
        dataset.validate()
    except Exception:
        if membership is not None:
            dataset.corpus_memberships.pop()
        dataset.screening_decisions.pop()
        raise

    return ScreeningResult(
        decision=decision,
        corpus_membership=membership,
        full_text_required=(
            submission.stage is ScreeningStage.TITLE_ABSTRACT
            and status is EligibilityStatus.UNCERTAIN
        ),
    )


def finalize_corpus_membership(
    dataset: ReviewDataset,
    *,
    canonical_record_id: str,
) -> CorpusMembership:
    """Finalize membership from the sole effective resolved prospective decision."""

    candidates = [
        decision
        for decision in dataset.effective_screening_decisions()
        if decision.canonical_record_id == canonical_record_id
    ]
    if len(candidates) != 1:
        raise ValueError("membership requires one effective prospective screening decision")
    membership = _append_membership(dataset, candidates[0])
    try:
        dataset.validate()
    except Exception:
        dataset.corpus_memberships.pop()
        raise
    return membership


def _build_criteria(submission: ScreeningSubmission) -> list[CriterionDecision]:
    if set(submission.criterion_values) != set(EligibilityCriterion):
        raise ValueError("screening submissions must code E1-E7 exactly once")
    if set(submission.criterion_evidence_ids) != set(EligibilityCriterion):
        raise ValueError("every criterion requires an evidence-reference list")

    criteria: list[CriterionDecision] = []
    for criterion in EligibilityCriterion:
        evidence_ids = list(submission.criterion_evidence_ids[criterion])
        if not evidence_ids:
            raise ValueError(f"criterion {criterion.value} requires evidence references")
        criteria.append(
            CriterionDecision(
                criterion=criterion,
                value=submission.criterion_values[criterion],
                evidence_ids=evidence_ids,
                rationale=submission.criterion_rationales.get(criterion, ""),
            )
        )
    return criteria


def _validate_exclusion_reasons(
    submission: ScreeningSubmission,
    status: EligibilityStatus,
) -> None:
    reasons = [
        reason
        for reason in [
            submission.primary_exclusion_reason,
            *submission.secondary_exclusion_reasons,
        ]
        if reason is not None
    ]
    if status is not EligibilityStatus.EXCLUDED:
        if reasons:
            raise ValueError("only excluded records may have exclusion reasons")
        return
    if submission.primary_exclusion_reason is None:
        raise ValueError("excluded records require a primary frozen exclusion reason")
    if len(reasons) != len(set(reasons)):
        raise ValueError("exclusion reasons must not contain duplicates")

    reason_rank = {reason: index for index, reason in enumerate(EXCLUSION_REASON_ORDER)}
    if reason_rank[submission.primary_exclusion_reason] != min(reason_rank[item] for item in reasons):
        raise ValueError("primary exclusion reason must follow the frozen reason order")

    no_criteria = {
        criterion
        for criterion, value in submission.criterion_values.items()
        if value is TriState.NO
    }
    covered: set[EligibilityCriterion] = set()
    for reason in reasons:
        supported = REASON_CRITERIA[reason].intersection(no_criteria)
        if not supported:
            raise ValueError(f"exclusion reason {reason.value} has no supporting NO criterion")
        covered.update(supported)
    if not no_criteria.issubset(covered):
        raise ValueError("every NO criterion requires a corresponding exclusion reason")
    if ExclusionReason.OTHER_PROTOCOL_REASON in reasons and not submission.notes.strip():
        raise ValueError("EX_OTHER_PROTOCOL_REASON requires an explanation")
    if ExclusionReason.INSUFFICIENT_EVIDENCE_AFTER_ESCALATION in reasons and (
        submission.stage is not ScreeningStage.FULL_TEXT
        or submission.provenance.actor.actor_type
        not in {ActorType.HUMAN, ActorType.ADJUDICATOR}
    ):
        raise ValueError(
            "insufficient evidence exclusion requires full-text escalation and human review"
        )


def _validate_technical_failures(submission: ScreeningSubmission) -> None:
    technical_criteria = set(submission.technical_failure_criteria)
    if len(technical_criteria) != len(submission.technical_failure_criteria):
        raise ValueError("technical failure criteria must not contain duplicates")
    if technical_criteria and not submission.technical_errors:
        raise ValueError("technical failures require preserved error details")
    if submission.technical_errors and not technical_criteria:
        raise ValueError("technical errors must identify affected criteria")
    if any(
        submission.criterion_values[criterion] is not TriState.UNCERTAIN
        for criterion in technical_criteria
    ):
        raise ValueError("retrieval, PDF, and parser failures must remain UNCERTAIN")


def _validate_stage_lineage(dataset: ReviewDataset, submission: ScreeningSubmission) -> None:
    if submission.stage is not ScreeningStage.FULL_TEXT:
        return

    prior = {
        decision.decision_id: decision
        for decision in dataset.screening_decisions
        if decision.canonical_record_id == submission.canonical_record_id
        and decision.provenance.scope is submission.provenance.scope
    }
    predecessors = [
        prior[decision_id]
        for decision_id in submission.provenance.supersedes_ids
        if decision_id in prior
    ]
    if not predecessors:
        raise ValueError("full-text screening must supersede an earlier screening decision")
    if not any(
        decision.stage is ScreeningStage.FULL_TEXT
        or (
            decision.stage is ScreeningStage.TITLE_ABSTRACT
            and decision.status is EligibilityStatus.UNCERTAIN
        )
        for decision in predecessors
    ):
        raise ValueError("full-text escalation requires an uncertain title/abstract predecessor")


def _append_membership(
    dataset: ReviewDataset,
    decision: ScreeningDecision,
) -> CorpusMembership:
    if (
        decision.provenance.authority is DecisionAuthority.PROPOSED
        or decision.provenance.actor.actor_type is ActorType.LLM
    ):
        raise ValueError("LLM proposals cannot create authoritative corpus membership")
    if decision.status is EligibilityStatus.UNCERTAIN:
        raise ValueError("unresolved decisions cannot be finalized as corpus membership")
    effective = [
        item
        for item in dataset.effective_screening_decisions()
        if item.canonical_record_id == decision.canonical_record_id
    ]
    if len(effective) != 1 or effective[0].decision_id != decision.decision_id:
        raise ValueError("membership requires one effective prospective screening decision")

    previous_memberships = [
        item
        for item in dataset.effective_corpus_memberships()
        if item.canonical_record_id == decision.canonical_record_id
    ]
    provenance = replace(
        decision.provenance,
        supersedes_ids=[item.decision_id for item in previous_memberships],
        metadata={
            **decision.provenance.metadata,
            "screening_decision_id": decision.decision_id,
        },
    )
    membership = CorpusMembership(
        decision_id=_stable_id("membership", decision.decision_id),
        canonical_record_id=decision.canonical_record_id,
        status=decision.status,
        screening_decision_id=decision.decision_id,
        provenance=provenance,
    )
    dataset.corpus_memberships.append(membership)
    return membership


def _screening_id(
    submission: ScreeningSubmission,
    status: EligibilityStatus,
) -> str:
    payload = {
        "record": submission.canonical_record_id,
        "stage": submission.stage.value,
        "criteria": {
            criterion.value: submission.criterion_values[criterion].value
            for criterion in EligibilityCriterion
        },
        "evidence": {
            criterion.value: list(submission.criterion_evidence_ids[criterion])
            for criterion in EligibilityCriterion
        },
        "rationales": {
            criterion.value: submission.criterion_rationales.get(criterion, "")
            for criterion in EligibilityCriterion
        },
        "status": status.value,
        "primary_exclusion_reason": (
            submission.primary_exclusion_reason.value
            if submission.primary_exclusion_reason
            else None
        ),
        "secondary_exclusion_reasons": [
            reason.value for reason in submission.secondary_exclusion_reasons
        ],
        "technical_failure_criteria": [
            criterion.value for criterion in submission.technical_failure_criteria
        ],
        "technical_errors": list(submission.technical_errors),
        "notes": submission.notes,
        "actor": submission.provenance.actor.actor_id,
        "authority": submission.provenance.authority.value,
        "protocol_version": submission.provenance.protocol_version,
        "rubric_version": submission.provenance.rubric_version,
        "created_at": submission.provenance.created_at,
        "supersedes": list(submission.provenance.supersedes_ids),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return _stable_id("screening", encoded)


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"
