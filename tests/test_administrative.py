from __future__ import annotations

from collections.abc import Callable

import pytest

from h2h_lit.administrative import (
    AdministrativeScopeSubmission,
    record_administrative_scope_decision,
)
from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.retrieval import (
    RetrievalQuerySpec,
    execute_retrieval_run,
    load_review_dataset,
    save_review_dataset,
)
from h2h_lit.review import (
    ActorType,
    AdministrativeDocumentType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    EvidenceReference,
    EvidenceSource,
    ExclusionReason,
    PublicationDatePrecision,
    TriState,
)
from tests.fake_http import FakeHttp


def _clock(*values: str) -> Callable[[], str]:
    iterator = iter(values)
    return iterator.__next__


def _adapter(query: str, *, limit: int, http: object) -> list[LiteratureRecord]:
    del query, limit, http
    return [
        LiteratureRecord(
            title="Administrative test record",
            abstract="An offline fixture.",
            doi="10.1000/administrative",
            source_database="MockSource",
            source_identifier="mock:1",
            original_metadata={"native_document_type": "journal-article"},
        )
    ]


def _dataset():
    dataset = execute_retrieval_run(
        run_id="retrieval:production-primary-v1",
        queries=[RetrievalQuerySpec("MockSource", "frozen query", "mock-query-v1")],
        http_clients={"MockSource": FakeHttp([])},
        adapters={"MockSource": _adapter},
        timestamp=_clock(
            "2026-08-30T23:59:59+00:00",
            "2026-08-31T00:00:01+00:00",
        ),
        query_plan_version="production-search-v1",
    )
    record_id = dataset.canonical_records[0].canonical_id
    evidence_id = "evidence:administrative:mock-1"
    dataset.evidence.append(
        EvidenceReference(
            evidence_id=evidence_id,
            canonical_record_id=record_id,
            source=EvidenceSource.METADATA,
            locator="canonical_record.administrative_metadata",
            quote=None,
            metadata={"source_occurrence_ids": dataset.canonical_records[0].occurrence_ids},
        )
    )
    return dataset, record_id, evidence_id


def _provenance(created_at: str = "2026-08-31T00:00:02+00:00") -> DecisionProvenance:
    return DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.administrative",
            actor_type=ActorType.SOFTWARE,
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=created_at,
    )


def _submission(record_id: str, evidence_id: str, **overrides):
    values = {
        "canonical_record_id": record_id,
        "retrieval_run_id": "retrieval:production-primary-v1",
        "publication_date": "2026-08-30",
        "publication_date_precision": PublicationDatePrecision.DAY,
        "full_text_language": "English",
        "full_text_available": True,
        "document_type": AdministrativeDocumentType.JOURNAL_ARTICLE,
        "qualifying_system_evidence": TriState.YES,
        "evidence_ids": [evidence_id],
        "provenance": _provenance(),
    }
    values.update(overrides)
    return AdministrativeScopeSubmission(**values)


def test_production_e6_is_derived_from_successful_run_cutoff_and_round_trips(tmp_path):
    dataset, record_id, evidence_id = _dataset()

    decision = record_administrative_scope_decision(
        dataset, _submission(record_id, evidence_id)
    )

    assert decision.value is TriState.YES
    assert decision.date_state is TriState.YES
    assert decision.language_state is TriState.YES
    assert decision.document_type_state is TriState.YES
    assert decision.full_text_state is TriState.YES
    assert decision.exclusion_reasons == []
    assert decision.metadata["retrieval_cutoff_date"] == "2026-08-31"

    path = tmp_path / "administrative.json"
    save_review_dataset(path, dataset)
    restored = load_review_dataset(path)
    assert restored.to_json() == dataset.to_json()
    assert restored.administrative_scope_decisions[0] == decision


def test_coarse_date_overlapping_cutoff_remains_uncertain():
    dataset, record_id, evidence_id = _dataset()

    decision = record_administrative_scope_decision(
        dataset,
        _submission(
            record_id,
            evidence_id,
            publication_date="2026",
            publication_date_precision=PublicationDatePrecision.YEAR,
        ),
    )

    assert decision.date_state is TriState.UNCERTAIN
    assert decision.value is TriState.UNCERTAIN
    assert decision.exclusion_reasons == []


def test_after_cutoff_and_ineligible_type_have_deterministic_reasons():
    dataset, record_id, evidence_id = _dataset()

    decision = record_administrative_scope_decision(
        dataset,
        _submission(
            record_id,
            evidence_id,
            publication_date="2026-09-01",
            document_type=AdministrativeDocumentType.OTHER,
        ),
    )

    assert decision.value is TriState.NO
    assert decision.exclusion_reasons == [
        ExclusionReason.AFTER_RETRIEVAL_END_DATE,
        ExclusionReason.INELIGIBLE_DOCUMENT_TYPE,
    ]


def test_missing_full_text_metadata_escalates_instead_of_excluding():
    dataset, record_id, evidence_id = _dataset()

    decision = record_administrative_scope_decision(
        dataset,
        _submission(
            record_id,
            evidence_id,
            full_text_language=None,
            full_text_available=False,
        ),
    )

    assert decision.language_state is TriState.UNCERTAIN
    assert decision.full_text_state is TriState.UNCERTAIN
    assert decision.value is TriState.UNCERTAIN
    assert decision.exclusion_reasons == []


def test_missing_publication_date_requires_explicit_unknown_precision():
    dataset, record_id, evidence_id = _dataset()

    decision = record_administrative_scope_decision(
        dataset,
        _submission(
            record_id,
            evidence_id,
            publication_date=None,
            publication_date_precision=PublicationDatePrecision.UNKNOWN,
        ),
    )
    assert decision.date_state is TriState.UNCERTAIN

    other_dataset, other_record_id, other_evidence_id = _dataset()
    with pytest.raises(ValueError, match="missing publication date requires unknown precision"):
        record_administrative_scope_decision(
            other_dataset,
            _submission(
                other_record_id,
                other_evidence_id,
                publication_date=None,
                publication_date_precision=PublicationDatePrecision.DAY,
            ),
        )


@pytest.mark.parametrize(
    ("qualifying_system_evidence", "expected"),
    [
        (TriState.YES, TriState.YES),
        (TriState.NO, TriState.NO),
        (TriState.UNCERTAIN, TriState.UNCERTAIN),
    ],
)
def test_survey_document_type_follows_independent_qualifying_system_evidence(
    qualifying_system_evidence: TriState,
    expected: TriState,
):
    dataset, record_id, evidence_id = _dataset()

    decision = record_administrative_scope_decision(
        dataset,
        _submission(
            record_id,
            evidence_id,
            document_type=AdministrativeDocumentType.SURVEY,
            qualifying_system_evidence=qualifying_system_evidence,
        ),
    )

    assert decision.document_type_state is expected


def test_incomplete_run_cannot_supply_production_e6_cutoff():
    dataset, record_id, evidence_id = _dataset()
    run = dataset.retrieval_runs[0]
    run.status = ProcessingStatus.PARTIAL
    run.retrieval_cutoff_date = None
    run.errors = ["fixture failure"]

    with pytest.raises(ValueError, match="successfully completed retrieval wave"):
        record_administrative_scope_decision(
            dataset, _submission(record_id, evidence_id)
        )


def test_serialized_e6_component_states_cannot_disagree_with_structured_inputs():
    dataset, record_id, evidence_id = _dataset()
    decision = record_administrative_scope_decision(
        dataset, _submission(record_id, evidence_id)
    )
    decision.date_state = TriState.NO
    decision.value = TriState.NO
    decision.exclusion_reasons = [ExclusionReason.AFTER_RETRIEVAL_END_DATE]

    with pytest.raises(ValueError, match="component states are not deterministically derived"):
        dataset.validate()
