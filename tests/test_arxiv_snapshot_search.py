from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from h2h_lit.arxiv_snapshot_search import (
    EXPECTED_ARXIV_QUERY_HASHES,
    EXPECTED_PLAN_HASH,
    MANIFEST_FILENAME,
    MATCHES_FILENAME,
    ArxivSnapshotSearchError,
    SearchDocument,
    SnapshotQuery,
    _validate_archive,
    expression_matches,
    load_frozen_arxiv_queries,
    matching_policy_hash,
    normalize_tokens,
    parse_query,
    run_snapshot_search,
    scan_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]
PLAN = ROOT / "config/star_production_query_plan_v1.json"


def _record(
    arxiv_id: str,
    *,
    title: str = "",
    abstract: str = "",
    comments: str | None = None,
    update_date: str = "2026-01-02",
) -> dict[str, object]:
    return {
        "id": arxiv_id,
        "submitter": "A Submitter",
        "authors": "An Author",
        "title": title,
        "comments": comments,
        "journal-ref": None,
        "doi": None,
        "report-no": None,
        "categories": "cs.HC q-bio.QM",
        "license": None,
        "abstract": abstract,
        "versions": [{"version": "v1", "created": "Mon, 1 Jan 2024 00:00:00 GMT"}],
        "update_date": update_date,
        "authors_parsed": [["Author", "An", ""]],
    }


def _query(query_id: str, text: str) -> SnapshotQuery:
    return SnapshotQuery(
        query_id=query_id,
        family_id=query_id,
        variant_id="test",
        query_text=text,
        query_text_sha256=hashlib.sha256(text.encode()).hexdigest(),
        request_specification_hash="test-request",
        expression=parse_query(text),
    )


def _jsonl(records: list[dict[str, object]]) -> bytes:
    return b"".join(
        json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        for record in records
    )


def _write_archive(path: Path, snapshot: bytes, member: str = "arxiv-metadata-oai-snapshot.json"):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, snapshot)


def test_matching_policy_handles_case_punctuation_hyphens_and_fields():
    expression = parse_query('all:("life science" AND visualization)')

    cross_field = SearchDocument.from_record(
        _record("one", title="LIFE-science", abstract="Interactive Visualization")
    )
    split_phrase = SearchDocument.from_record(
        _record("two", title="life", abstract="science visualization")
    )

    assert expression_matches(expression, cross_field)
    assert not expression_matches(expression, split_phrase)
    assert normalize_tokens("Human–in_the/Loop") == ("human", "in", "the", "loop")


def test_matching_policy_uses_whole_tokens_and_boolean_precedence():
    expression = parse_query("all:(alpha OR beta AND gamma)")

    assert expression_matches(expression, SearchDocument.from_record(_record("one", title="alpha")))
    assert not expression_matches(
        expression, SearchDocument.from_record(_record("two", title="beta only"))
    )
    assert expression_matches(
        expression, SearchDocument.from_record(_record("three", title="beta gamma"))
    )
    assert not expression_matches(
        parse_query("all:visualization"),
        SearchDocument.from_record(_record("four", title="visualizations")),
    )


@pytest.mark.parametrize(
    "query",
    [
        "title:biology",
        "all:biology*",
        "all:biology?",
        "all:(biology ANDNOT medicine)",
        "all:(biology AND)",
        'all:"unterminated',
    ],
)
def test_matching_policy_rejects_unsupported_or_malformed_syntax(query: str):
    with pytest.raises(ArxivSnapshotSearchError):
        parse_query(query)


def test_frozen_plan_loads_exact_five_arxiv_queries():
    plan_hash, queries = load_frozen_arxiv_queries(root=ROOT, plan_path=PLAN)

    assert plan_hash == EXPECTED_PLAN_HASH
    assert {query.query_id: query.query_text_sha256 for query in queries} == (
        EXPECTED_ARXIV_QUERY_HASHES
    )
    assert len(queries) == 5


def test_streaming_scan_evaluates_all_queries_and_preserves_one_source_record():
    payload = _jsonl(
        [
            _record("one", title="Biology visualization"),
            _record("two", title="Unrelated subject", update_date="2025-12-31"),
        ]
    )
    output = io.BytesIO()

    stats = scan_snapshot(
        io.BytesIO(payload),
        output,
        [
            _query("Q1", "all:(biology AND visualization)"),
            _query("Q2", 'all:("visual analytics" OR visualization)'),
        ],
    )

    assert stats.record_count == 2
    assert stats.union_match_count == 1
    assert stats.per_query_counts == {"Q1": 1, "Q2": 1}
    assert stats.source_sha256 == hashlib.sha256(payload).hexdigest()
    assert stats.source_bytes == len(payload)
    assert stats.observed_date_ranges["update_date"] == {
        "minimum": "2025-12-31",
        "maximum": "2026-01-02",
    }
    saved = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(saved) == 1
    assert saved[0]["matching_query_ids"] == ["Q1", "Q2"]
    assert saved[0]["source_record"] == _record("one", title="Biology visualization")
    assert saved[0]["source_record_provenance"]["line_number"] == 1


def test_run_writes_provisional_manifest_and_results_without_changing_sources(tmp_path: Path):
    snapshot_bytes = _jsonl(
        [
            _record(
                "one",
                title="Biological network visual analytics",
                abstract="An interactive exploration system for life science.",
            )
        ]
    )
    snapshot = tmp_path / "snapshot.json"
    archive = tmp_path / "snapshot.zip"
    output = tmp_path / "isolated-run"
    snapshot.write_bytes(snapshot_bytes)
    _write_archive(archive, snapshot_bytes)
    archive_before = archive.read_bytes()
    snapshot_before = snapshot.read_bytes()

    manifest = run_snapshot_search(
        root=ROOT,
        plan_path=PLAN,
        archive_path=archive,
        snapshot_path=snapshot,
        output_dir=output,
        code_commit="a" * 40,
        timestamp=iter(["2026-09-19T10:00:00.000Z", "2026-09-19T10:00:01.000Z"]).__next__,
        expected_archive_size=archive.stat().st_size,
        expected_archive_sha256=hashlib.sha256(archive_before).hexdigest(),
        expected_snapshot_size=len(snapshot_bytes),
    )

    assert manifest["status"] == "COMPLETE"
    assert manifest["production_eligible"] is False
    assert manifest["prisma_counted"] is False
    assert manifest["api_equivalence"] == "NOT_CLAIMED"
    assert manifest["provider_completeness"] == "UNPROVEN"
    assert manifest["snapshot"]["publication_date_is_coverage_evidence"] is False
    assert manifest["matching_policy_sha256"] == matching_policy_hash()
    assert manifest["frozen_query_plan"]["plan_hash"] == EXPECTED_PLAN_HASH
    assert len(manifest["frozen_query_plan"]["queries"]) == 5
    assert manifest["counts"]["source_records"] == 1
    assert manifest["counts"]["union_matches"] == 1
    assert manifest["results"]["raw_sha256"] == hashlib.sha256(
        (output / MATCHES_FILENAME).read_bytes()
    ).hexdigest()
    assert json.loads((output / MANIFEST_FILENAME).read_text()) == manifest
    assert archive.read_bytes() == archive_before
    assert snapshot.read_bytes() == snapshot_before


def test_run_refuses_existing_namespace_before_reading_sources(tmp_path: Path):
    output = tmp_path / "existing"
    output.mkdir()

    with pytest.raises(ArxivSnapshotSearchError, match="already exists"):
        run_snapshot_search(
            root=ROOT,
            plan_path=PLAN,
            archive_path=tmp_path / "absent.zip",
            snapshot_path=tmp_path / "absent.json",
            output_dir=output,
            code_commit="a" * 40,
        )


def test_run_refuses_production_output_namespace(tmp_path: Path):
    with pytest.raises(ArxivSnapshotSearchError, match="outputs/production"):
        run_snapshot_search(
            root=ROOT,
            plan_path=PLAN,
            archive_path=tmp_path / "absent.zip",
            snapshot_path=tmp_path / "absent.json",
            output_dir=ROOT / "outputs/production/snapshot-test",
            code_commit="a" * 40,
        )


def test_archive_validation_rejects_unexpected_member_path(tmp_path: Path):
    archive = tmp_path / "bad.zip"
    _write_archive(archive, b"{}\n", member="../arxiv-metadata-oai-snapshot.json")

    with pytest.raises(ArxivSnapshotSearchError, match="unexpected path"):
        _validate_archive(archive, 3)


def test_scan_rejects_invalid_record_without_saving_partial_match():
    output = io.BytesIO()

    with pytest.raises(ArxivSnapshotSearchError, match="lacks an arXiv id"):
        scan_snapshot(io.BytesIO(b'{"title":"biology"}\n'), output, [])
    assert output.getvalue() == b""
