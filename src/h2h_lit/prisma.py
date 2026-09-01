"""PRISMA counts derived from persisted retrieval and screening provenance."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from h2h_lit.models import ProcessingStatus
from h2h_lit.review import (
    DecisionScope,
    DedupeOutcome,
    EligibilityStatus,
    ReviewDataset,
    ScreeningStage,
)


@dataclass(frozen=True, slots=True)
class PrismaReconciliation:
    run_ids: list[str]
    retrieval_runs_by_completion: dict[str, int]
    source_query_count: int
    source_queries_by_status: dict[str, int]
    source_queries_by_completion: dict[str, int]
    source_queries_by_source: dict[str, int]
    source_queries_by_run: dict[str, int]
    empty_result_query_count: int
    failed_query_count: int
    truncated_query_count: int
    retrieval_page_count: int
    retrieval_attempt_count: int
    failed_retrieval_attempt_count: int
    records_identified: int
    records_by_source: dict[str, int]
    records_by_run: dict[str, int]
    records_by_identification_route: dict[str, int]
    records_by_prior_survey_seed_set: dict[str, int]
    duplicate_records_removed: int
    records_after_deduplication: int
    records_screened: int
    title_abstract_screened: int
    full_text_assessed: int
    eligible_records: int
    excluded_records: int
    unresolved_records: int
    excluded_by_primary_reason: dict[str, int]
    effective_screening_decisions_by_authority: dict[str, int]
    effective_memberships_by_authority: dict[str, int]
    reconciled: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )


def reconcile_prisma(dataset: ReviewDataset) -> PrismaReconciliation:
    """Derive counts and reject any non-reconciling identification lineage."""

    dataset.validate()
    queries = {query.query_id: query for query in dataset.source_queries}
    decisions = dataset.effective_duplicate_decisions()

    status_counts = {status.value: 0 for status in ProcessingStatus}
    completion_counts: dict[str, int] = {}
    query_source_counts: dict[str, int] = {}
    query_run_counts: dict[str, int] = {}
    for query in dataset.source_queries:
        status_counts[query.status.value] += 1
        _increment(completion_counts, query.completion_status.value)
        _increment(query_source_counts, query.source_database)
        _increment(query_run_counts, query.run_id or "<unassigned>")

    record_source_counts: dict[str, int] = {}
    record_run_counts: dict[str, int] = {}
    record_route_counts: dict[str, int] = {}
    seed_set_counts: dict[str, int] = {}
    record_query_counts = {query_id: 0 for query_id in queries}
    for occurrence in dataset.occurrences:
        query = queries[occurrence.source_query_id]
        record_query_counts[query.query_id] += 1
        _increment(record_source_counts, query.source_database)
        _increment(record_run_counts, query.run_id or "<unassigned>")
        _increment(record_route_counts, query.identification_route.value)
        if query.identification_route.value == "prior_survey_seed":
            seed_set_id = str(query.metadata.get("seed_set_id") or "<unspecified>")
            _increment(seed_set_counts, seed_set_id)

    records_identified = len(dataset.occurrences)
    duplicates = sum(
        decision.outcome is DedupeOutcome.DUPLICATE for decision in decisions
    )
    unique_survivors = sum(
        decision.outcome is DedupeOutcome.UNIQUE for decision in decisions
    )
    records_after_deduplication = len(dataset.canonical_records)

    prospective_screens = [
        decision
        for decision in dataset.screening_decisions
        if decision.provenance.scope is DecisionScope.PROSPECTIVE
    ]
    screened_ids = {decision.canonical_record_id for decision in prospective_screens}
    title_abstract_ids = {
        decision.canonical_record_id
        for decision in prospective_screens
        if decision.stage is ScreeningStage.TITLE_ABSTRACT
    }
    full_text_ids = {
        decision.canonical_record_id
        for decision in prospective_screens
        if decision.stage is ScreeningStage.FULL_TEXT
    }
    effective_screens = dataset.effective_screening_decisions()
    screen_authorities: dict[str, int] = {}
    for decision in effective_screens:
        _increment(screen_authorities, decision.provenance.authority.value)

    effective_memberships = dataset.effective_corpus_memberships()
    membership_authorities: dict[str, int] = {}
    eligible_ids: set[str] = set()
    excluded_ids: set[str] = set()
    excluded_by_reason: dict[str, int] = {}
    screens_by_id = {decision.decision_id: decision for decision in dataset.screening_decisions}
    for membership in effective_memberships:
        _increment(membership_authorities, membership.provenance.authority.value)
        if membership.status is EligibilityStatus.ELIGIBLE:
            eligible_ids.add(membership.canonical_record_id)
        elif membership.status is EligibilityStatus.EXCLUDED:
            excluded_ids.add(membership.canonical_record_id)
            screen = screens_by_id[membership.screening_decision_id]
            if screen.primary_exclusion_reason is None:
                raise ValueError("excluded PRISMA records require a primary exclusion reason")
            _increment(excluded_by_reason, screen.primary_exclusion_reason.value)
    unresolved_ids = screened_ids - eligible_ids - excluded_ids

    if len(decisions) != records_identified:
        raise ValueError("PRISMA reconciliation requires one effective decision per occurrence")
    if sum(record_source_counts.values()) != records_identified:
        raise ValueError("source occurrence counts do not sum to records identified")
    if duplicates + unique_survivors != records_identified:
        raise ValueError("deduplication outcomes do not sum to records identified")
    if unique_survivors != records_after_deduplication:
        raise ValueError("unique survivors do not equal canonical records after deduplication")
    if records_identified - duplicates != records_after_deduplication:
        raise ValueError("identified minus duplicates does not equal records after deduplication")
    if not eligible_ids.union(excluded_ids).issubset(screened_ids):
        raise ValueError("corpus membership cannot precede screening")
    if len(screened_ids) != len(eligible_ids) + len(excluded_ids) + len(unresolved_ids):
        raise ValueError("screened records do not reconcile to eligible/excluded/unresolved")
    if sum(excluded_by_reason.values()) != len(excluded_ids):
        raise ValueError("excluded reasons do not reconcile to excluded records")

    run_ids = sorted({query.run_id for query in dataset.source_queries if query.run_id})
    run_completion_counts: dict[str, int] = {}
    for run in dataset.retrieval_runs:
        _increment(run_completion_counts, run.completion_status.value)
    return PrismaReconciliation(
        run_ids=run_ids,
        retrieval_runs_by_completion=_sorted_counts(run_completion_counts),
        source_query_count=len(dataset.source_queries),
        source_queries_by_status=_sorted_counts(status_counts),
        source_queries_by_completion=_sorted_counts(completion_counts),
        source_queries_by_source=_sorted_counts(query_source_counts),
        source_queries_by_run=_sorted_counts(query_run_counts),
        empty_result_query_count=sum(
            query.status is ProcessingStatus.OK and record_query_counts[query.query_id] == 0
            for query in dataset.source_queries
        ),
        failed_query_count=status_counts[ProcessingStatus.FAILED.value],
        truncated_query_count=completion_counts.get("truncated", 0),
        retrieval_page_count=len(dataset.retrieval_pages),
        retrieval_attempt_count=len(dataset.retrieval_attempts),
        failed_retrieval_attempt_count=sum(
            attempt.status.value == "failed" for attempt in dataset.retrieval_attempts
        ),
        records_identified=records_identified,
        records_by_source=_sorted_counts(record_source_counts),
        records_by_run=_sorted_counts(record_run_counts),
        records_by_identification_route=_sorted_counts(record_route_counts),
        records_by_prior_survey_seed_set=_sorted_counts(seed_set_counts),
        duplicate_records_removed=duplicates,
        records_after_deduplication=records_after_deduplication,
        records_screened=len(screened_ids),
        title_abstract_screened=len(title_abstract_ids),
        full_text_assessed=len(full_text_ids),
        eligible_records=len(eligible_ids),
        excluded_records=len(excluded_ids),
        unresolved_records=len(unresolved_ids),
        excluded_by_primary_reason=_sorted_counts(excluded_by_reason),
        effective_screening_decisions_by_authority=_sorted_counts(screen_authorities),
        effective_memberships_by_authority=_sorted_counts(membership_authorities),
        reconciled=True,
    )


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items()))
