"""Deterministic production E6 derivation from structured administrative metadata."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from h2h_lit.models import ProcessingStatus
from h2h_lit.review import (
    ActorType,
    AdministrativeDocumentType,
    AdministrativeScopeDecision,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    ExclusionReason,
    PublicationDatePrecision,
    ReviewDataset,
    TriState,
)


@dataclass(slots=True)
class AdministrativeScopeSubmission:
    canonical_record_id: str
    retrieval_run_id: str
    publication_date: str | None
    publication_date_precision: PublicationDatePrecision
    full_text_language: str | None
    full_text_available: bool | None
    document_type: AdministrativeDocumentType
    qualifying_system_evidence: TriState
    evidence_ids: list[str]
    provenance: DecisionProvenance
    metadata: dict[str, Any] = field(default_factory=dict)
    decision_id: str | None = None


def record_administrative_scope_decision(
    dataset: ReviewDataset,
    submission: AdministrativeScopeSubmission,
) -> AdministrativeScopeDecision:
    """Derive, append, and validate a prospective software-authored E6 decision."""

    if submission.provenance.scope is not DecisionScope.PROSPECTIVE:
        raise ValueError("production E6 decisions must be prospective")
    if (
        submission.provenance.actor.actor_type is not ActorType.SOFTWARE
        or submission.provenance.authority is not DecisionAuthority.DETERMINISTIC
    ):
        raise ValueError("production E6 decisions require deterministic software provenance")
    if not any(
        record.canonical_id == submission.canonical_record_id
        for record in dataset.canonical_records
    ):
        raise ValueError(f"unknown canonical record: {submission.canonical_record_id}")
    run = next(
        (item for item in dataset.retrieval_runs if item.run_id == submission.retrieval_run_id),
        None,
    )
    if run is None:
        raise ValueError(f"unknown retrieval run: {submission.retrieval_run_id}")
    if run.status is not ProcessingStatus.OK or not run.retrieval_cutoff_date:
        raise ValueError("production E6 requires a successfully completed retrieval wave")

    states = derive_administrative_states(
        publication_date=submission.publication_date,
        publication_date_precision=submission.publication_date_precision,
        retrieval_cutoff_date=run.retrieval_cutoff_date,
        full_text_language=submission.full_text_language,
        full_text_available=submission.full_text_available,
        document_type=submission.document_type,
        qualifying_system_evidence=submission.qualifying_system_evidence,
    )
    date_state = states["date"]
    language_state = states["language"]
    document_type_state = states["document_type"]
    full_text_state = states["full_text"]
    component_values = list(states.values())
    value = (
        TriState.NO
        if TriState.NO in component_values
        else TriState.YES
        if all(item is TriState.YES for item in component_values)
        else TriState.UNCERTAIN
    )
    reasons: list[ExclusionReason] = []
    if date_state is TriState.NO:
        reasons.append(ExclusionReason.AFTER_RETRIEVAL_END_DATE)
    if language_state is TriState.NO:
        reasons.append(ExclusionReason.NON_ENGLISH_FULL_TEXT)
    if document_type_state is TriState.NO:
        reasons.append(ExclusionReason.INELIGIBLE_DOCUMENT_TYPE)

    decision = AdministrativeScopeDecision(
        decision_id=submission.decision_id or _decision_id(submission, value, reasons),
        canonical_record_id=submission.canonical_record_id,
        retrieval_run_id=submission.retrieval_run_id,
        publication_date=submission.publication_date,
        publication_date_precision=submission.publication_date_precision,
        full_text_language=submission.full_text_language,
        full_text_available=submission.full_text_available,
        document_type=submission.document_type,
        qualifying_system_evidence=submission.qualifying_system_evidence,
        date_state=date_state,
        language_state=language_state,
        document_type_state=document_type_state,
        full_text_state=full_text_state,
        value=value,
        exclusion_reasons=reasons,
        evidence_ids=list(submission.evidence_ids),
        provenance=submission.provenance,
        metadata={
            **submission.metadata,
            "retrieval_cutoff_date": run.retrieval_cutoff_date,
            "derivation": "structured_production_administrative_metadata_v1",
        },
    )
    dataset.administrative_scope_decisions.append(decision)
    try:
        dataset.validate()
    except Exception:
        dataset.administrative_scope_decisions.pop()
        raise
    return decision


def derive_administrative_states(
    *,
    publication_date: str | None,
    publication_date_precision: PublicationDatePrecision,
    retrieval_cutoff_date: str,
    full_text_language: str | None,
    full_text_available: bool | None,
    document_type: AdministrativeDocumentType,
    qualifying_system_evidence: TriState,
) -> dict[str, TriState]:
    """Derive all E6 components without accepting model-authored component states."""

    return {
        "date": _derive_date_state(
            publication_date,
            publication_date_precision,
            retrieval_cutoff_date,
        ),
        "language": _derive_language_state(full_text_language, full_text_available),
        "document_type": _derive_document_type_state(
            document_type, qualifying_system_evidence
        ),
        "full_text": (
            TriState.YES if full_text_available is True else TriState.UNCERTAIN
        ),
    }


def _derive_date_state(
    publication_date: str | None,
    precision: PublicationDatePrecision,
    cutoff: str,
) -> TriState:
    if publication_date is None:
        if precision is not PublicationDatePrecision.UNKNOWN:
            raise ValueError("missing publication date requires unknown precision")
        return TriState.UNCERTAIN
    if precision is PublicationDatePrecision.UNKNOWN:
        raise ValueError("a normalized publication date requires declared precision")
    if not publication_date.strip():
        raise ValueError("publication date must not be empty")
    cutoff_date = date.fromisoformat(cutoff)
    try:
        if precision is PublicationDatePrecision.DAY:
            earliest = latest = date.fromisoformat(publication_date)
        elif precision is PublicationDatePrecision.MONTH:
            year_text, month_text = publication_date.split("-")
            year, month = int(year_text), int(month_text)
            earliest = date(year, month, 1)
            latest = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
            latest = date.fromordinal(latest.toordinal() - 1)
        else:
            year = int(publication_date)
            earliest, latest = date(year, 1, 1), date(year, 12, 31)
    except (TypeError, ValueError) as exc:
        raise ValueError("publication date does not match its declared precision") from exc
    if latest <= cutoff_date:
        return TriState.YES
    if earliest > cutoff_date:
        return TriState.NO
    return TriState.UNCERTAIN


def _derive_language_state(language: str | None, available: bool | None) -> TriState:
    if available is not True or language is None or not language.strip():
        return TriState.UNCERTAIN
    normalized = language.strip().casefold()
    return TriState.YES if normalized in {"en", "eng", "english"} else TriState.NO


def _derive_document_type_state(
    document_type: AdministrativeDocumentType,
    qualifying_system_evidence: TriState,
) -> TriState:
    if document_type in {
        AdministrativeDocumentType.JOURNAL_ARTICLE,
        AdministrativeDocumentType.CONFERENCE_PAPER,
        AdministrativeDocumentType.WORKSHOP_PAPER,
        AdministrativeDocumentType.PREPRINT,
    }:
        return TriState.YES
    if document_type in {
        AdministrativeDocumentType.SURVEY,
        AdministrativeDocumentType.CONCEPTUAL,
    }:
        return qualifying_system_evidence
    if document_type is AdministrativeDocumentType.OTHER:
        return TriState.NO
    return TriState.UNCERTAIN


def _decision_id(
    submission: AdministrativeScopeSubmission,
    value: TriState,
    reasons: list[ExclusionReason],
) -> str:
    payload = {
        "canonical_record_id": submission.canonical_record_id,
        "retrieval_run_id": submission.retrieval_run_id,
        "publication_date": submission.publication_date,
        "publication_date_precision": submission.publication_date_precision.value,
        "full_text_language": submission.full_text_language,
        "full_text_available": submission.full_text_available,
        "document_type": submission.document_type.value,
        "qualifying_system_evidence": submission.qualifying_system_evidence.value,
        "evidence_ids": list(submission.evidence_ids),
        "value": value.value,
        "exclusion_reasons": [reason.value for reason in reasons],
        "created_at": submission.provenance.created_at,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]
    return f"administrative:{digest}"
