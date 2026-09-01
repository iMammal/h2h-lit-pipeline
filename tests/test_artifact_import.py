from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from h2h_lit.artifact_import import merge_identification_datasets
from h2h_lit.pagination import RetryPolicy
from h2h_lit.prisma import reconcile_prisma
from h2h_lit.retrieval import RetrievalQuerySpec, execute_paginated_retrieval_run
from h2h_lit.review import (
    DedupeOutcome,
    IdentificationRoute,
    RetrievalCompletionStatus,
    RetrievalTransportKind,
    ReviewDataset,
)
from h2h_lit.sources.acm_dl import import_acm_bibtex_manifest
from h2h_lit.sources.prior_survey_seed import import_seed_manifest
from tests.fake_http import FakeHttp, FakeResponse

FIXTURES = Path(__file__).parent / "fixtures"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_acm_manifest(tmp_path: Path, chunks: list[dict], *, total: int = 3) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    evidence_path = tmp_path / "search.png"
    evidence_path.write_bytes(b"operator screenshot evidence")
    fixture_dir = FIXTURES / "acm"
    for chunk in chunks:
        source = fixture_dir / chunk["artifact_path"]
        target = tmp_path / chunk["artifact_path"]
        target.write_bytes(source.read_bytes())
        chunk.setdefault("sha256", _sha(source))
        chunk.setdefault("exported_at", "2026-08-30T12:00:00+00:00")
    manifest = {
        "schema_version": "1.0.0",
        "run_id": "run:acm",
        "query_id": "query:acm",
        "query_text": 'Abstract:"visual analytics" AND biology',
        "query_version": "acm-query-v1",
        "field_selections": ["Abstract", "Title"],
        "collection_scope": "acm_publications",
        "filters": {"content_type": ["research_article"]},
        "search_executed_at": "2026-08-30T11:00:00+00:00",
        "imported_at": "2026-08-30T13:00:00+00:00",
        "ui_reported_total": total,
        "sort": "publicationDate asc",
        "export_format": "BibTeX",
        "operator_id": "operator-1",
        "access_tier": "institutional",
        "operator_evidence": {
            "query_url": "https://dl.acm.org/action/doSearch?token_key=secret&query=test",
            "artifacts": [{"path": "search.png", "sha256": _sha(evidence_path)}],
        },
        "chunks": chunks,
    }
    path = tmp_path / "acm_manifest.json"
    path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    return path


def test_acm_bibtex_multi_chunk_import_reconciles_and_preserves_operator(tmp_path):
    path = _write_acm_manifest(
        tmp_path,
        [
            {"chunk_id": "1-2", "first_record": 1, "last_record": 2,
             "artifact_path": "chunk_1_2.bib"},
            {"chunk_id": "3-3", "first_record": 3, "last_record": 3,
             "artifact_path": "chunk_3_3.bib"},
        ],
    )
    dataset = import_acm_bibtex_manifest(path)

    assert len(dataset.occurrences) == 3
    assert dataset.source_queries[0].completion_status is RetrievalCompletionStatus.COMPLETE
    assert all(
        attempt.transport_kind is RetrievalTransportKind.ARTIFACT_IMPORT
        and attempt.response_status is None
        and attempt.operator_id == "operator-1"
        for attempt in dataset.retrieval_attempts
    )
    assert [attempt.artifact_path for attempt in dataset.retrieval_attempts] == [
        "chunk_1_2.bib", "chunk_3_3.bib"
    ]
    serialized = dataset.to_json()
    assert str(tmp_path) not in serialized
    assert "secret" not in serialized
    assert "%3Credacted%3E" in serialized
    assert ReviewDataset.from_json(serialized).to_json() == serialized


@pytest.mark.parametrize(
    ("chunks", "match"),
    [
        ([{"chunk_id": "1-2", "first_record": 1, "last_record": 2,
           "artifact_path": "chunk_1_2.bib"},
          {"chunk_id": "4-4", "first_record": 4, "last_record": 4,
           "artifact_path": "chunk_3_3.bib"}], "missing or overlapping"),
        ([{"chunk_id": "1-2", "first_record": 1, "last_record": 2,
           "artifact_path": "chunk_1_2.bib"},
          {"chunk_id": "2-2", "first_record": 2, "last_record": 2,
           "artifact_path": "chunk_3_3.bib"}], "missing or overlapping"),
    ],
)
def test_acm_missing_or_overlapping_ranges_cannot_finalize(tmp_path, chunks, match):
    dataset = import_acm_bibtex_manifest(_write_acm_manifest(tmp_path, chunks))
    assert dataset.retrieval_runs[0].completion_status is RetrievalCompletionStatus.FAILED
    assert any(match in error for error in dataset.retrieval_runs[0].errors)
    assert dataset.retrieval_runs[0].retrieval_cutoff_date is None


def test_acm_malformed_or_absent_artifact_is_accounted_and_incomplete(tmp_path):
    malformed = import_acm_bibtex_manifest(
        _write_acm_manifest(
            tmp_path,
            [{"chunk_id": "1-1", "first_record": 1, "last_record": 1,
              "artifact_path": "malformed.bib"}],
            total=1,
        )
    )
    assert len(malformed.occurrences) == 1
    assert malformed.occurrences[0].metadata["parser_incomplete"] is True
    assert malformed.source_queries[0].completion_status is RetrievalCompletionStatus.FAILED

    missing_manifest = _write_acm_manifest(
        tmp_path / "missing",
        [{"chunk_id": "1-1", "first_record": 1, "last_record": 1,
          "artifact_path": "chunk_3_3.bib"}],
        total=1,
    )
    (missing_manifest.parent / "chunk_3_3.bib").unlink()
    missing = import_acm_bibtex_manifest(missing_manifest)
    assert missing.retrieval_attempts[0].status.value == "failed"
    assert missing.retrieval_runs[0].retrieval_cutoff_date is None


def test_acm_ui_total_and_operator_evidence_must_reconcile(tmp_path):
    total_mismatch = import_acm_bibtex_manifest(
        _write_acm_manifest(
            tmp_path / "total",
            [
                {"chunk_id": "1-2", "first_record": 1, "last_record": 2,
                 "artifact_path": "chunk_1_2.bib"},
                {"chunk_id": "3-3", "first_record": 3, "last_record": 3,
                 "artifact_path": "chunk_3_3.bib"},
            ],
            total=4,
        )
    )
    assert total_mismatch.retrieval_runs[0].completion_status is RetrievalCompletionStatus.FAILED
    assert any("reported total 4" in error for error in total_mismatch.retrieval_runs[0].errors)

    manifest_path = _write_acm_manifest(
        tmp_path / "evidence",
        [{"chunk_id": "1-1", "first_record": 1, "last_record": 1,
          "artifact_path": "chunk_3_3.bib"}],
        total=1,
    )
    (manifest_path.parent / "search.png").unlink()
    missing_evidence = import_acm_bibtex_manifest(manifest_path)
    assert missing_evidence.retrieval_runs[0].completion_status is RetrievalCompletionStatus.FAILED
    assert any("operator evidence" in error for error in missing_evidence.retrieval_runs[0].errors)


def test_seed_manifests_preserve_multiple_occurrences_and_prisma_route_counts():
    a = import_seed_manifest(FIXTURES / "seeds" / "seed_a.json")
    b = import_seed_manifest(FIXTURES / "seeds" / "seed_b.json")
    merged = merge_identification_datasets([a, b])
    prisma = reconcile_prisma(merged)

    assert len(merged.occurrences) == 2
    assert len(merged.canonical_records) == 1
    assert sum(
        decision.outcome is DedupeOutcome.DUPLICATE
        for decision in merged.duplicate_decisions
    ) == 1
    assert all(
        query.identification_route is IdentificationRoute.PRIOR_SURVEY_SEED
        for query in merged.source_queries
    )
    assert prisma.records_by_identification_route == {"prior_survey_seed": 2}
    assert prisma.records_by_prior_survey_seed_set == {"SEED-A": 1, "SEED-B": 1}


def test_seed_and_api_occurrences_use_normal_cross_source_deduplication(tmp_path):
    seed = import_seed_manifest(FIXTURES / "seeds" / "seed_a.json")

    class Clock:
        def __init__(self):
            self.value = datetime(2026, 8, 3, tzinfo=UTC)

        def __call__(self):
            value = self.value.isoformat()
            self.value += timedelta(seconds=1)
            return value

    api = execute_paginated_retrieval_run(
        run_id="run:crossref-shared",
        queries=[RetrievalQuerySpec("CrossRef", "shared", "crossref-v2", limit=5)],
        http_clients={
            "CrossRef": FakeHttp([FakeResponse(payload={"message": {
                "items": [{"DOI": "10.1000/shared", "title": ["Shared DOI Paper"]}],
                "total-results": 1,
                "next-cursor": "unused",
            }})])
        },
        checkpoint_dir=tmp_path / "api",
        timestamp=Clock(),
        retry_policy=RetryPolicy(max_attempts=1),
    )
    merged = merge_identification_datasets([seed, api])
    prisma = reconcile_prisma(merged)
    assert len(merged.occurrences) == 2
    assert len(merged.canonical_records) == 1
    assert prisma.records_by_identification_route == {
        "database": 1,
        "prior_survey_seed": 1,
    }


def test_seed_manifest_requires_explicit_complete_prospective_entries(tmp_path):
    manifest = json.loads((FIXTURES / "seeds" / "seed_a.json").read_text())
    manifest["expected_entry_count"] = 2
    path = tmp_path / "partial.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="expected_entry_count"):
        import_seed_manifest(path)


def test_artifact_import_serialization_and_hashing_are_deterministic(tmp_path):
    path = _write_acm_manifest(
        tmp_path,
        [
            {"chunk_id": "1-2", "first_record": 1, "last_record": 2,
             "artifact_path": "chunk_1_2.bib"},
            {"chunk_id": "3-3", "first_record": 3, "last_record": 3,
             "artifact_path": "chunk_3_3.bib"},
        ],
    )
    first = import_acm_bibtex_manifest(path)
    second = import_acm_bibtex_manifest(path)
    assert first.to_json() == second.to_json()
    assert first.source_queries[0].metadata["manifest_hash"] == hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def test_historical_stage5_review_datasets_remain_round_trip_compatible():
    root = Path(__file__).parents[1] / "outputs"
    paths = [
        root / "stage5_live_pilot_v1" / "review_dataset.json",
        root / "stage5b_live_pilot_v1" / "review_dataset.json",
        root / "stage5c_live_preprod_v1" / "review_dataset.json",
        root / "stage5d_live_pilot_v1" / "review_dataset.json",
    ]
    loaded = 0
    for path in paths:
        if not path.exists():
            continue
        dataset = ReviewDataset.from_json(path.read_text(encoding="utf-8"))
        round_trip = ReviewDataset.from_json(dataset.to_json())
        assert len(round_trip.occurrences) == len(dataset.occurrences)
        assert all(
            query.identification_route is IdentificationRoute.DATABASE
            for query in round_trip.source_queries
        )
        assert all(
            attempt.transport_kind is RetrievalTransportKind.HTTP
            for attempt in round_trip.retrieval_attempts
        )
        loaded += 1
    assert loaded == 4


def test_acm_manifest_rejects_absolute_paths_and_insufficient_provenance(tmp_path):
    manifest_path = _write_acm_manifest(
        tmp_path,
        [{"chunk_id": "1-1", "first_record": 1, "last_record": 1,
          "artifact_path": "chunk_3_3.bib"}],
        total=1,
    )
    manifest = json.loads(manifest_path.read_text())
    manifest["chunks"][0]["artifact_path"] = "/private/acm/export.bib"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="relative"):
        import_acm_bibtex_manifest(manifest_path)

    manifest.pop("operator_id")
    manifest["chunks"][0]["artifact_path"] = "chunk_3_3.bib"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required fields"):
        import_acm_bibtex_manifest(manifest_path)
