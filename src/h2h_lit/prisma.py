"""PRISMA identification and deduplication counts derived from review provenance."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from h2h_lit.models import ProcessingStatus
from h2h_lit.review import DedupeOutcome, ReviewDataset


@dataclass(frozen=True, slots=True)
class PrismaReconciliation:
    run_ids: list[str]
    source_query_count: int
    source_queries_by_status: dict[str, int]
    source_queries_by_source: dict[str, int]
    source_queries_by_run: dict[str, int]
    empty_result_query_count: int
    failed_query_count: int
    records_identified: int
    records_by_source: dict[str, int]
    records_by_run: dict[str, int]
    duplicate_records_removed: int
    records_after_deduplication: int
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
    query_source_counts: dict[str, int] = {}
    query_run_counts: dict[str, int] = {}
    for query in dataset.source_queries:
        status_counts[query.status.value] += 1
        _increment(query_source_counts, query.source_database)
        _increment(query_run_counts, query.run_id or "<unassigned>")

    record_source_counts: dict[str, int] = {}
    record_run_counts: dict[str, int] = {}
    record_query_counts = {query_id: 0 for query_id in queries}
    for occurrence in dataset.occurrences:
        query = queries[occurrence.source_query_id]
        record_query_counts[query.query_id] += 1
        _increment(record_source_counts, query.source_database)
        _increment(record_run_counts, query.run_id or "<unassigned>")

    records_identified = len(dataset.occurrences)
    duplicates = sum(
        decision.outcome is DedupeOutcome.DUPLICATE for decision in decisions
    )
    unique_survivors = sum(
        decision.outcome is DedupeOutcome.UNIQUE for decision in decisions
    )
    records_after_deduplication = len(dataset.canonical_records)

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

    run_ids = sorted({query.run_id for query in dataset.source_queries if query.run_id})
    return PrismaReconciliation(
        run_ids=run_ids,
        source_query_count=len(dataset.source_queries),
        source_queries_by_status=_sorted_counts(status_counts),
        source_queries_by_source=_sorted_counts(query_source_counts),
        source_queries_by_run=_sorted_counts(query_run_counts),
        empty_result_query_count=sum(
            query.status is ProcessingStatus.OK and record_query_counts[query.query_id] == 0
            for query in dataset.source_queries
        ),
        failed_query_count=status_counts[ProcessingStatus.FAILED.value],
        records_identified=records_identified,
        records_by_source=_sorted_counts(record_source_counts),
        records_by_run=_sorted_counts(record_run_counts),
        duplicate_records_removed=duplicates,
        records_after_deduplication=records_after_deduplication,
        reconciled=True,
    )


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _sorted_counts(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items()))
