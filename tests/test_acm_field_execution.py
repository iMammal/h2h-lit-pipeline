from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from h2h_lit.acm_field_execution import (
    ACM_EXPORT_FILTER,
    EXECUTION_METHOD_COMMIT,
    OPERATOR_EVIDENCE_LIMITATIONS,
    QUERY_SYNTAX_COMPLETE,
    QUERY_SYNTAX_INCOMPLETE,
    QUERY_SYNTAX_PRODUCTION_SIDE_EFFECTS,
    AcmExportArtifactReference,
    AcmFieldExecutionError,
    AcmFieldExportEvidence,
    AcmQuerySyntaxEvidence,
    AcmRawFieldHit,
    build_acm_field_execution_contract,
    build_acm_parent_union,
    build_acm_query_syntax_manifest,
    contract_json,
    load_acm_field_execution_contract,
    load_acm_query_syntax_manifest,
    normalize_acm_search_timestamp,
    parse_acm_query_syntax_csv,
    query_syntax_manifest_json,
    validate_acm_query_syntax,
    validate_acm_query_syntax_manifest,
)

ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "config/star_production_query_plan_v1.json"
CONTRACT_PATH = ROOT / "config/star_acm_field_execution_contract_v1.json"
QUERY_SYNTAX_ROOT = ROOT / "artifacts/acm_field_execution/2026-09-02/query_syntax"
QUERY_SYNTAX_MANIFEST_PATH = (
    ROOT / "provenance/star_acm_field_execution_2026-09-02_query_syntax_manifest.json"
)
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


def _manifest_attempts(manifest: dict) -> list[dict]:
    return [
        attempt
        for family in manifest["families"]
        for child in family["children"]
        for attempt in child["attempts"]
    ]


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


def test_query_syntax_manifest_is_deterministic_and_verifies_all_raw_artifacts() -> None:
    tracked = load_acm_query_syntax_manifest(
        QUERY_SYNTAX_MANIFEST_PATH, root=ROOT, verify_artifacts=True
    )
    rebuilt = build_acm_query_syntax_manifest(
        CONTRACT_PATH, QUERY_SYNTAX_ROOT, root=ROOT
    )
    assert query_syntax_manifest_json(rebuilt) == QUERY_SYNTAX_MANIFEST_PATH.read_text(
        encoding="utf-8"
    )
    assert rebuilt == tracked
    assert tracked["execution_method_commit"] == EXECUTION_METHOD_COMMIT
    assert tracked["manifest_status"] == QUERY_SYNTAX_INCOMPLETE
    assert tracked["validation_summary"] == {
        "expected_child_count": 15,
        "observed_artifact_count": 15,
        "valid_attempt_count": 14,
        "invalid_attempt_count": 1,
        "all_children_have_accepted_attempt": False,
    }
    assert tracked["operator_evidence_limitations"] == OPERATOR_EVIDENCE_LIMITATIONS
    assert tracked["production_side_effects"] == QUERY_SYNTAX_PRODUCTION_SIDE_EFFECTS
    assert all(value is False for value in tracked["production_side_effects"].values())


def test_query_syntax_manifest_binds_all_counts_sizes_and_raw_hashes() -> None:
    manifest = load_acm_query_syntax_manifest(
        QUERY_SYNTAX_MANIFEST_PATH, root=ROOT, verify_artifacts=True
    )
    expected = {
        "QF01_title_query_syntax_csv": (
            8,
            1712,
            "682ca5628ff6b9b2d17024b926769954c354e9ca8e6e30b9b0835189f537b558",
        ),
        "QF01_keyword_query_syntax_csv": (
            48,
            1787,
            "0f9a07aac22c954ad1c699b55565496ac05c9120088c48040152ff311919d28e",
        ),
        "QF01_abstract_query_syntax_csv": (
            1931,
            1722,
            "8807e8085d6f84d489922e4ed272da67698c1d521e95442453f7a4fee6a799ea",
        ),
        "QF02_title_query_syntax_csv": (
            18,
            3011,
            "db1edc08ad24141719828971e64516015cda19c9e5d0a73543f3cc6b03a77b6d",
        ),
        "QF02_keyword_query_syntax_csv": (
            16,
            3015,
            "dec43b59bb0ff72b30bb5c7779afcdf1c5c3c898487721a2c127f25ad4905f37",
        ),
        "QF02_abstract_query_syntax_csv": (
            1673,
            3020,
            "c0d00f388c5ba90d8a41cb8783f788bfb76e443999d02cce88543e9d51b54e3b",
        ),
        "QF03_title_query_syntax_csv": (
            4,
            2253,
            "ea0b8fb2e134d5dd8c5721d6a4010483f40530c580644b78936150cffc48f308",
        ),
        "QF03_keyword_query_syntax_csv": (
            12,
            2258,
            "ebe91a54114999da6392118dbfd7bedf298aa1c456453bcede002018f1967bc3",
        ),
        "QF03_abstract_query_syntax_csv": (
            1983,
            2263,
            "1476df7e871606e97a6d91551f34aadca8bf0106d02e74f9fa94f776443d1f7e",
        ),
        "QF04_title_query_syntax_csv": (
            23,
            2365,
            "0beb2636e81cc4fff25d2b046bd1c74d0d9f34482283bc8b1069b6d6e8950733",
        ),
        "QF04_keyword_query_syntax_csv": (
            34,
            2369,
            "89177437e377614ba0234a12cd8aa205ba1c41b6392492a4a8da2cba265f393f",
        ),
        "QF04_abstract_query_syntax_csv": (
            2432,
            2374,
            "f6535ed358beae8bfb5eb514a68741108034055368477a3050a012dfaf411100",
        ),
        "QF05_title_query_syntax_csv": (
            19,
            2495,
            "b7de6f005668a5604d3f77ed5395f2f754e61fa6119268334cf508c2e386a58e",
        ),
        "QF05_keyword_query_syntax_csv": (
            24,
            2499,
            "4b75c28758745f3b7a0b9af2d8a0432c7cb747714a368054177638cdcae7ea42",
        ),
        "QF05_abstract_query_syntax_csv": (
            3454,
            2504,
            "d5bc5f7d01ebbbed5a6fde02e229bf490d66d2523a1ff1f43a65421776ee844f",
        ),
    }
    observed = {
        Path(attempt["artifact"]["relative_path"]).name: (
            attempt["parsed_ui_reported_count"],
            attempt["artifact"]["byte_size"],
            attempt["artifact"]["raw_sha256"],
        )
        for attempt in _manifest_attempts(manifest)
    }
    assert observed == expected
    assert all(attempt["attempt_number"] == 1 for attempt in _manifest_attempts(manifest))
    assert all(
        attempt["normalized_search_timestamp_utc"].endswith("Z")
        for attempt in _manifest_attempts(manifest)
    )
    assert normalize_acm_search_timestamp("2026-09-02 at 19:04:29 PDT") == (
        "2026-09-03T02:04:29Z"
    )


def test_qf01_keyword_attempt_one_remains_invalid_without_unescaping() -> None:
    manifest = load_acm_query_syntax_manifest(
        QUERY_SYNTAX_MANIFEST_PATH, root=ROOT, verify_artifacts=True
    )
    child = manifest["families"][0]["children"][1]
    attempt = child["attempts"][0]
    raw = (ROOT / attempt["artifact"]["relative_path"]).read_bytes()
    assert b'\\"life science\\"' in raw
    assert attempt["validation"]["state"] == "INVALID"
    assert attempt["validation"]["checks"]["exact_frozen_scientific_expression"] is False
    assert attempt["validation"]["reason"] == (
        "stored query contains 70 literal backslashes before phrase quotes; "
        "exact frozen query required"
    )
    assert child["accepted_attempt_number"] is None
    assert child["completion_state"] == "REQUIRES_CORRECTED_ATTEMPT"
    assert attempt["supersedes_attempt_number"] is None
    assert attempt["superseded_by_attempt_number"] is None


def test_manifest_schema_retains_failed_attempt_when_a_later_attempt_supersedes_it() -> None:
    manifest = deepcopy(
        load_acm_query_syntax_manifest(QUERY_SYNTAX_MANIFEST_PATH, root=ROOT)
    )
    child = manifest["families"][0]["children"][1]
    failed = child["attempts"][0]
    failed["superseded_by_attempt_number"] = 2
    corrected = deepcopy(failed)
    corrected["attempt_number"] = 2
    corrected["supersedes_attempt_number"] = 1
    corrected["superseded_by_attempt_number"] = None
    corrected["artifact"] = {
        "relative_path": (
            "artifacts/acm_field_execution/future/query_syntax/"
            "QF01_keyword_query_syntax_csv_attempt_2"
        ),
        "byte_size": 1,
        "raw_sha256": _hash("future corrected evidence"),
    }
    corrected["validation"] = {
        "state": "VALID",
        "reason": None,
        "checks": {key: True for key in failed["validation"]["checks"]},
    }
    child["attempts"].append(corrected)
    child["accepted_attempt_number"] = 2
    child["completion_state"] = QUERY_SYNTAX_COMPLETE
    manifest["manifest_status"] = QUERY_SYNTAX_COMPLETE
    manifest["validation_summary"] = {
        "expected_child_count": 15,
        "observed_artifact_count": 16,
        "valid_attempt_count": 15,
        "invalid_attempt_count": 1,
        "all_children_have_accepted_attempt": True,
    }
    manifest.pop("manifest_hash")
    canonical = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    manifest["manifest_hash"] = _hash(canonical)
    validate_acm_query_syntax_manifest(manifest, root=ROOT)
    assert child["attempts"][0]["validation"]["state"] == "INVALID"


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
