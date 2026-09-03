from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from h2h_lit.acm_field_execution import (
    ACM_EXPORT_FILTER,
    AcmExportArtifactReference,
    AcmFieldExecutionError,
    AcmFieldExportEvidence,
    AcmQuerySyntaxEvidence,
    AcmRawFieldHit,
    build_acm_field_execution_contract,
    build_acm_parent_union,
    contract_json,
    load_acm_field_execution_contract,
    parse_acm_query_syntax_csv,
    validate_acm_query_syntax,
)

ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "config/star_production_query_plan_v1.json"
CONTRACT_PATH = ROOT / "config/star_acm_field_execution_contract_v1.json"
PLAN_RAW_SHA256 = "b887d638e42f4909c1c8461dde733d758e5176d528ddccee4370211e14ed7451"


def _contract() -> dict:
    return load_acm_field_execution_contract(CONTRACT_PATH, root=ROOT)


def _syntax(child: dict, count: int) -> AcmQuerySyntaxEvidence:
    return AcmQuerySyntaxEvidence(
        query_name=child["child_query_id"],
        search_run_date="2026-09-03 at 00:00:00 UTC",
        reported_count=count,
        exported_syntax=(
            f'"query": {{{child["child_query_text"]}}}\t'
            f'"filter": {{{ACM_EXPORT_FILTER}}}'
        ),
    )


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def test_contract_is_deterministic_and_preserves_frozen_plan() -> None:
    assert hashlib.sha256(PLAN_PATH.read_bytes()).hexdigest() == PLAN_RAW_SHA256
    tracked = _contract()
    rebuilt = build_acm_field_execution_contract(PLAN_PATH, root=ROOT)
    assert rebuilt == tracked
    assert contract_json(rebuilt) == CONTRACT_PATH.read_text(encoding="utf-8")
    assert tracked["production_plan"]["canonical_hash"] == (
        "856ef04518bc26941275cf6b60a793814fe18ff6b0b80dd24571252a7161e091"
    )


def test_exactly_five_parents_and_fifteen_deterministic_children() -> None:
    contract = _contract()
    assert len(contract["families"]) == 5
    children = [child for family in contract["families"] for child in family["children"]]
    assert len(children) == 15
    assert len({child["child_query_id"] for child in children}) == 15
    for family in contract["families"]:
        assert [child["field_key"] for child in family["children"]] == [
            "title",
            "keyword",
            "abstract",
        ]
        for child in family["children"]:
            assert child["child_query_text"] == (
                f'{child["field_label"]}:({family["scientific_query_text"]})'
            )
            assert child["child_query_sha256"] == _hash(child["child_query_text"])
            assert family["scientific_query_text_sha256"] == _hash(
                family["scientific_query_text"]
            )
            assert child["request_specification"] == {
                "workflow": "advanced_search",
                "scope": "ACM Publications",
                "collection": "Full-Text Collection",
                "field": child["field_label"],
                "filters": {},
                "sort": "publicationDate asc",
                "transport": "human_ui_artifact_import",
            }


def test_csv_import_handles_acm_unquoted_thousands_separator() -> None:
    raw = (
        "ACM DL: Query Name,Search Run Date,Search Result Count,Query Syntax\n"
        'H2H_QF01.csv,2026-09-02 at 17:26:03 PDT,1,948,"query": '
        '{Title:(visualization)}\t"filter": {ACM Content: DL}\n'
    )
    evidence = parse_acm_query_syntax_csv(raw)
    assert len(evidence) == 1
    assert evidence[0].reported_count == 1948
    assert evidence[0].query_name == "H2H_QF01.csv"
    assert evidence[0].exported_syntax.startswith('"query": {Title:(')


def test_exact_single_field_operator_evidence_passes() -> None:
    child = _contract()["families"][0]["children"][0]
    validate_acm_query_syntax(_syntax(child, 1), child)


def test_keyword_evidence_with_abstract_clause_fails_closed() -> None:
    child = _contract()["families"][0]["children"][1]
    evidence = _syntax(child, 1)
    evidence = AcmQuerySyntaxEvidence(
        query_name=evidence.query_name,
        search_run_date=evidence.search_run_date,
        reported_count=1,
        exported_syntax=(
            f'"query": {{{child["child_query_text"]} OR '
            f'Abstract:({child["child_query_text"]})}}\t'
            f'"filter": {{{ACM_EXPORT_FILTER}}}'
        ),
    )
    with pytest.raises(AcmFieldExecutionError, match="exactly one field wrapper"):
        validate_acm_query_syntax(evidence, child)


def test_wrong_or_all_field_rendering_fails_closed() -> None:
    child = _contract()["families"][4]["children"][0]
    wrong = AcmQuerySyntaxEvidence(
        query_name="bad",
        search_run_date="2026-09-03 at 00:00:00 UTC",
        reported_count=0,
        exported_syntax=(
            '"query": {All: (title:( All: keyword:(}\t"filter": {ACM Content: DL}'
        ),
    )
    with pytest.raises(AcmFieldExecutionError, match="exactly one field wrapper"):
        validate_acm_query_syntax(wrong, child)


def _field_export(child: dict, identities: list[tuple[str, str | None]]) -> AcmFieldExportEvidence:
    hits = tuple(
        AcmRawFieldHit(
            child_query_id=child["child_query_id"],
            field_key=child["field_key"],
            row_ordinal=index,
            raw_entry_sha256=_hash(f'{child["field_key"]}:{index}:{native_id}'),
            acm_native_id=native_id,
            doi=doi,
        )
        for index, (native_id, doi) in enumerate(identities, start=1)
    )
    count = len(hits)
    return AcmFieldExportEvidence(
        child_query_id=child["child_query_id"],
        field_key=child["field_key"],
        operator_evidence=_syntax(child, count),
        artifacts=(
            AcmExportArtifactReference(
                artifact_relative_path=f'exports/{child["field_key"]}.bib',
                artifact_sha256=_hash(f'artifact:{child["field_key"]}'),
                byte_size=100,
                first_record=1,
                last_record=count,
            ),
        ),
        hits=hits,
        exported_at_utc="2026-09-03T00:00:00Z",
    )


def test_three_child_union_is_boolean_equivalent_and_preserves_overlaps() -> None:
    family = _contract()["families"][0]
    children = family["children"]
    exports = [
        _field_export(
            children[0], [("a", None), ("b1", "10.1234/b"), ("c", None), ("d", None)]
        ),
        _field_export(
            children[1], [("b2", "10.1234/b"), ("c", None), ("e", None), ("g", None)]
        ),
        _field_export(children[2], [("c", None), ("d", None), ("f", None), ("g", None)]),
    ]
    result = build_acm_parent_union(family, exports)
    assert result.complete is True
    assert result.raw_hit_total == 12
    assert result.unique_union_count == 7
    assert result.per_field_raw_counts == {"title": 4, "keyword": 4, "abstract": 4}
    assert result.overlap_counts == {
        "title_only": 1,
        "keyword_only": 1,
        "abstract_only": 1,
        "title_keyword": 1,
        "title_abstract": 1,
        "keyword_abstract": 1,
        "triple_overlap": 1,
    }
    separate_union = {
        token
        for export in exports
        for hit in export.hits
        for token in hit.identity_tokens()
        if token.startswith("acm:")
    }
    assert {token for item in result.members for token in item["identity_tokens"] if token in separate_union} == separate_union
    assert result.to_dict()["union_hash"] == result.union_hash


def test_parent_cannot_complete_with_missing_or_unreconciled_child() -> None:
    family = _contract()["families"][0]
    exports = [_field_export(child, [(child["field_key"], None)]) for child in family["children"]]
    with pytest.raises(AcmFieldExecutionError, match="exactly its three field exports"):
        build_acm_parent_union(family, exports[:2])

    bad = exports[0]
    bad = AcmFieldExportEvidence(
        child_query_id=bad.child_query_id,
        field_key=bad.field_key,
        operator_evidence=AcmQuerySyntaxEvidence(
            query_name=bad.operator_evidence.query_name,
            search_run_date=bad.operator_evidence.search_run_date,
            reported_count=2,
            exported_syntax=bad.operator_evidence.exported_syntax,
        ),
        artifacts=bad.artifacts,
        hits=bad.hits,
        exported_at_utc=bad.exported_at_utc,
    )
    with pytest.raises(AcmFieldExecutionError, match="ranges do not reconcile"):
        build_acm_parent_union(family, [bad, *exports[1:]])


def test_artifact_paths_and_stable_identity_fail_closed() -> None:
    with pytest.raises(AcmFieldExecutionError, match="relative and traversal-safe"):
        AcmExportArtifactReference(
            artifact_relative_path="../escape.bib",
            artifact_sha256=_hash("x"),
            byte_size=1,
            first_record=1,
            last_record=1,
        ).validate()
    with pytest.raises(AcmFieldExecutionError, match="stable ACM native identifier"):
        AcmRawFieldHit(
            child_query_id="child",
            field_key="title",
            row_ordinal=1,
            raw_entry_sha256=_hash("x"),
        ).validate()


def test_contract_has_no_production_or_methodological_side_effects() -> None:
    contract = _contract()
    assert all(value is False for value in contract["production_side_effects"].values())
    assert contract["execution_contract"]["network_execution_performed"] is False
    assert contract["execution_contract"]["scientific_query_change"] is False
    assert contract["execution_contract"]["filters"] == {}
    assert contract["historical_calibration_evidence"][0]["rewritten"] is False
    serialized = json.dumps(contract)
    for forbidden in ('"eligibility"', '"screening_decision"', '"corpus_membership"'):
        assert forbidden not in serialized
