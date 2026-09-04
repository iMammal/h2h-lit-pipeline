from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from h2h_lit.artifact_import import merge_identification_datasets
from h2h_lit.models import ProcessingStatus
from h2h_lit.production_wave import (
    EXTERNAL_IDENTIFICATION_SOURCES_V2,
    EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
    REQUIRED_PRODUCTION_SOURCES,
    SOURCE_CONTRACTS,
    ArtifactKind,
    PaginationExpectation,
    ProductionQueryFamily,
    ProductionRetrievalWave,
    ProductionWavePreflightError,
    ProductionWaveStatus,
    RequiredArtifact,
    ResultWindowStatus,
    compute_query_plan_hash,
    load_production_wave,
    preflight_production_wave,
    save_production_wave,
)
from h2h_lit.review import (
    RetrievalCompletionStatus,
    RetrievalRun,
    RetrievalRunKind,
    SourceQuery,
)
from h2h_lit.sources.acm_dl import import_acm_bibtex_manifest
from h2h_lit.sources.prior_survey_seed import import_seed_manifest

FIXTURES = Path(__file__).parent / "fixtures"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_artifacts(root: Path) -> tuple[RequiredArtifact, RequiredArtifact]:
    root.mkdir(parents=True, exist_ok=True)
    acm_chunk = root / "acm_chunk.bib"
    acm_chunk.write_bytes((FIXTURES / "acm" / "chunk_3_3.bib").read_bytes())
    chunk = {
        "chunk_id": "1-1",
        "first_record": 1,
        "last_record": 1,
        "artifact_path": acm_chunk.name,
        "sha256": _sha(acm_chunk),
        "exported_at": "2026-08-30T12:00:00+00:00",
    }
    acm_manifest = {
        "schema_version": "1.0.0",
        "run_id": "component:acm",
        "query_id": "query:acm",
        "query_text": "ACM frozen query",
        "query_version": "acm-query-v1",
        "field_selections": ["Title", "Abstract"],
        "collection_scope": "acm_publications",
        "filters": {"content_type": ["research_article"]},
        "search_executed_at": "2026-08-30T11:00:00+00:00",
        "imported_at": "2026-08-30T13:00:00+00:00",
        "ui_reported_total": 1,
        "sort": "publicationDate asc",
        "export_format": "BibTeX",
        "operator_id": "operator-1",
        "chunks": [chunk],
    }
    acm_path = root / "acm_manifest.json"
    acm_path.write_text(json.dumps(acm_manifest, sort_keys=True), encoding="utf-8")
    expected_chunk = {key: chunk[key] for key in (
        "chunk_id", "first_record", "last_record", "artifact_path", "sha256"
    )}
    acm_requirement = RequiredArtifact(
        kind=ArtifactKind.ACM_EXPORT_MANIFEST,
        manifest_path=acm_path.name,
        manifest_sha256=_sha(acm_path),
        expected_total=1,
        expected_chunks=[expected_chunk],
    )

    seed_payload = json.loads((FIXTURES / "seeds" / "seed_a.json").read_text())
    seed_payload["run_id"] = "component:seed-a"
    seed_payload["query_id"] = "query:seed-a"
    seed_path = root / "seed_a.json"
    seed_path.write_text(json.dumps(seed_payload, sort_keys=True), encoding="utf-8")
    seed_requirement = RequiredArtifact(
        kind=ArtifactKind.PRIOR_SURVEY_SEED_MANIFEST,
        manifest_path=seed_path.name,
        manifest_sha256=_sha(seed_path),
        expected_total=1,
        seed_set_id="SEED-A",
        seed_set_version="1.0.0",
    )
    return acm_requirement, seed_requirement


def _native_parameters(source: str) -> dict:
    return {
        "PubMed": {"db": "pubmed", "retmode": "xml", "usehistory": "y", "page_size": 200},
        "EuropePMC": {"format": "json", "resultType": "core", "pageSize": 500},
        "CrossRef": {"rows": 500},
        "SemanticScholar": {
            "mode": "relevance",
            "limit": 100,
            "fields": ["title", "abstract", "year", "externalIds"],
        },
        "arXiv": {"max_results": 500, "sortBy": "submittedDate", "sortOrder": "ascending"},
        "IEEEXplore": {
            "query_parameter": "querytext",
            "format": "json",
            "max_records": 200,
            "sort_field": "article_number",
            "sort_order": "asc",
        },
        "ACMDigitalLibrary": {
            "field_selections": ["Title", "Abstract"],
            "collection_scope": "acm_publications",
            "filters": {"content_type": ["research_article"]},
            "sort": "publicationDate asc",
            "export_format": "BibTeX",
            "ui_reported_total": 1,
        },
        "PriorSurveySeed": {
            "seed_set_id": "SEED-A",
            "seed_set_version": "1.0.0",
            "expected_entry_count": 1,
        },
    }[source]


def _family(
    source: str,
    *,
    acm: RequiredArtifact,
    seed: RequiredArtifact,
) -> ProductionQueryFamily:
    contract = SOURCE_CONTRACTS[source]
    proofs = list(contract.completion_proofs)
    exact_total = contract.exact_total_required
    maximum = contract.maximum_supported_results
    if source == "SemanticScholar":
        proofs = ["semantic_scholar_relevance_next_exhausted"]
        exact_total = True
        maximum = 1000
    artifact = acm if source == "ACMDigitalLibrary" else seed if source == "PriorSurveySeed" else None
    return ProductionQueryFamily(
        query_family_id=f"production:{source.lower()}:primary",
        source_database=source,
        source_role=contract.role,
        identification_route=contract.route,
        transport_kind=contract.transport,
        adapter_id=contract.adapter_id,
        adapter_version=contract.adapter_version,
        query_version=(
            "acm-query-v1" if source == "ACMDigitalLibrary" else f"{source.lower()}-query-v1"
        ),
        query_text=(
            "ACM frozen query"
            if source == "ACMDigitalLibrary"
            else "prior survey seed set SEED-A"
            if source == "PriorSurveySeed"
            else f"{source} frozen production query"
        ),
        native_parameters=_native_parameters(source),
        pagination=PaginationExpectation(
            strategy=contract.strategy,
            adapter_version=contract.adapter_version,
            completion_proofs=proofs,
            exact_total_required=exact_total,
            maximum_supported_results=maximum,
        ),
        required_credentials=["api_key"] if source == "IEEEXplore" else [],
        content_policy=(
            {"abstract": "external_llm_use_unresolved"}
            if source == "IEEEXplore"
            else {}
        ),
        required_artifact=artifact,
    )


def _wave(root: Path, *, status: ProductionWaveStatus = ProductionWaveStatus.READY):
    acm, seed = _write_artifacts(root)
    wave = ProductionRetrievalWave(
        schema_version="1.0.0",
        wave_id="h2h-star-production-wave-1",
        wave_version="1.0.0",
        query_plan_version="production-query-plan-v1",
        query_plan_hash="",
        required_sources=list(REQUIRED_PRODUCTION_SOURCES),
        query_families=[
            _family(source, acm=acm, seed=seed) for source in REQUIRED_PRODUCTION_SOURCES
        ],
        status=status,
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    return wave


def _codes(exc: ProductionWavePreflightError) -> set[str]:
    return {item.code for item in exc.report.issues}


def _completed_execution(wave: ProductionRetrievalWave, root: Path):
    by_source = {item.source_database: item for item in wave.query_families}
    acm_artifact = by_source["ACMDigitalLibrary"].required_artifact
    seed_artifact = by_source["PriorSurveySeed"].required_artifact
    assert acm_artifact is not None and seed_artifact is not None
    acm = import_acm_bibtex_manifest(root / acm_artifact.manifest_path)
    seed = import_seed_manifest(root / seed_artifact.manifest_path)
    dataset = merge_identification_datasets([acm, seed])
    for query in dataset.source_queries:
        query.run_id = wave.wave_id
        query.metadata["query_family_id"] = by_source[query.source_database].query_family_id
    for family in wave.query_families:
        if family.transport_kind.value != "http":
            continue
        dataset.source_queries.append(
            SourceQuery(
                query_id=f"executed:{family.query_family_id}",
                source_database=family.source_database,
                query_text=family.query_text,
                retrieval_started_at="2026-09-01T00:00:00+00:00",
                retrieval_ended_at="2026-09-01T00:01:00+00:00",
                status=ProcessingStatus.OK,
                run_id=wave.wave_id,
                query_version=family.query_version,
                result_count=0,
                metadata={"query_family_id": family.query_family_id},
                completion_status=RetrievalCompletionStatus.COMPLETE,
                source_reported_total=0,
                total_is_exact=family.pagination.exact_total_required,
                completion_proof=family.pagination.completion_proofs[0],
                identification_route=family.identification_route,
                content_policy=dict(family.content_policy),
            )
        )
    query_ids = [item.query_id for item in dataset.source_queries]
    dataset.retrieval_runs = [
        RetrievalRun(
            run_id=wave.wave_id,
            kind=RetrievalRunKind.PRIMARY,
            query_plan_version=wave.query_plan_version,
            query_plan_hash=wave.query_plan_hash,
            planned_query_ids=query_ids,
            source_query_ids=query_ids,
            retrieval_started_at="2026-08-01T00:00:00+00:00",
            retrieval_completed_at="2026-09-01T01:00:00+00:00",
            status=ProcessingStatus.OK,
            protocol_version="1.0.0",
            retrieval_cutoff_date="2026-09-01",
            completion_status=RetrievalCompletionStatus.COMPLETE,
        )
    ]
    dataset.validate()
    return dataset


def test_complete_wave_plan_is_ready_but_not_executed_or_finalizable(tmp_path):
    wave = _wave(tmp_path)
    report = preflight_production_wave(
        wave,
        manifest_root=tmp_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
    )
    assert report.planning_complete is True
    assert report.required_inputs_available is True
    assert report.ready is True
    assert report.execution_complete is False
    assert report.finalizable is False
    assert report.recommended_status is ProductionWaveStatus.READY
    assert wave.retrieval_cutoff_date is None


def test_external_scope_defers_seed_without_weakening_full_wave_contract(tmp_path):
    wave = _wave(tmp_path, status=ProductionWaveStatus.PLANNED)
    wave.schema_version = "1.1.0"
    wave.required_sources = list(EXTERNAL_IDENTIFICATION_SOURCES_V2)
    wave.support_sources = ["CrossRef"]
    wave.query_families = [
        item
        for item in wave.query_families
        if item.source_database not in {"CrossRef", "PriorSurveySeed"}
    ]
    wave.metadata = {
        "execution_scope": EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
        "deferred_identification_sources": ["PriorSurveySeed"],
        "identification_set_closure_allowed": False,
    }
    wave.query_plan_hash = compute_query_plan_hash(wave)

    report = preflight_production_wave(
        wave,
        manifest_root=tmp_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
    )
    assert report.ready is True
    assert report.execution_complete is False
    assert report.finalizable is False
    assert all(
        item.source_database != "PriorSurveySeed" for item in wave.query_families
    )


def test_external_scope_requires_explicit_identification_closure_guard(tmp_path):
    wave = _wave(tmp_path, status=ProductionWaveStatus.PLANNED)
    wave.schema_version = "1.1.0"
    wave.required_sources = list(EXTERNAL_IDENTIFICATION_SOURCES_V2)
    wave.support_sources = ["CrossRef"]
    wave.query_families = [
        item
        for item in wave.query_families
        if item.source_database not in {"CrossRef", "PriorSurveySeed"}
    ]
    wave.metadata = {
        "execution_scope": EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
        "deferred_identification_sources": ["PriorSurveySeed"],
        "identification_set_closure_allowed": True,
    }
    wave.query_plan_hash = compute_query_plan_hash(wave)

    report = preflight_production_wave(
        wave,
        manifest_root=tmp_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
    )
    assert report.ready is False
    assert "EXTERNAL_SCOPE_CLOSURE_GUARD_MISSING" in {
        item.code for item in report.issues
    }


def test_incomplete_wave_reports_every_missing_required_source(tmp_path):
    wave = _wave(tmp_path, status=ProductionWaveStatus.INCOMPLETE)
    wave.query_families = wave.query_families[:-2]
    wave.query_plan_hash = compute_query_plan_hash(wave)
    report = preflight_production_wave(
        wave,
        manifest_root=tmp_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
    )
    assert report.planning_complete is False
    messages = [item.message for item in report.issues if item.code == "MISSING_REQUIRED_SOURCE_QUERY"]
    assert messages == [
        "required source ACMDigitalLibrary has no query family",
        "required source PriorSurveySeed has no query family",
    ]


@pytest.mark.parametrize(
    ("source", "filename"),
    [("ACMDigitalLibrary", "acm_manifest.json"), ("PriorSurveySeed", "seed_a.json")],
)
def test_ready_refuses_missing_acm_or_seed_manifest(tmp_path, source, filename):
    wave = _wave(tmp_path)
    (tmp_path / filename).unlink()
    with pytest.raises(ProductionWavePreflightError) as captured:
        preflight_production_wave(
            wave,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert "MISSING_REQUIRED_ARTIFACT" in _codes(captured.value)
    assert any(item.query_family_id.endswith(f"{source.lower()}:primary")
               for item in captured.value.report.issues)


def test_ready_refuses_missing_ieee_credentials(tmp_path):
    wave = _wave(tmp_path)
    with pytest.raises(ProductionWavePreflightError) as captured:
        preflight_production_wave(wave, manifest_root=tmp_path)
    assert "MISSING_CONFIGURED_CREDENTIAL" in _codes(captured.value)
    assert "credential-value" not in wave.to_json()


def test_unresolved_partition_requirement_blocks_ready(tmp_path):
    wave = _wave(tmp_path)
    pubmed = wave.query_families[0]
    wave.query_families[0] = replace(
        pubmed,
        result_window_status=ResultWindowStatus.UNRESOLVED,
        unresolved_requirement="known result count exceeds 10,000; no approved partition plan",
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    with pytest.raises(ProductionWavePreflightError) as captured:
        preflight_production_wave(
            wave,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert "UNRESOLVED_RESULT_WINDOW" in _codes(captured.value)


def test_query_plan_hash_mismatch_blocks_ready(tmp_path):
    wave = _wave(tmp_path)
    wave.query_plan_hash = "0" * 64
    with pytest.raises(ProductionWavePreflightError) as captured:
        preflight_production_wave(
            wave,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert "QUERY_PLAN_HASH_MISMATCH" in _codes(captured.value)


def test_duplicate_query_family_ids_and_source_role_mismatch_block_ready(tmp_path):
    wave = _wave(tmp_path)
    duplicate_id = wave.query_families[0].query_family_id
    wave.query_families[1] = replace(
        wave.query_families[1], query_family_id=duplicate_id, source_role="prior_review"
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    with pytest.raises(ProductionWavePreflightError) as captured:
        preflight_production_wave(
            wave,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert {"DUPLICATE_QUERY_FAMILY_ID", "SOURCE_ROLE_MISMATCH"}.issubset(
        _codes(captured.value)
    )


def test_semantic_scholar_and_ieee_native_parameters_are_mandatory(tmp_path):
    wave = _wave(tmp_path)
    semantic_index = list(REQUIRED_PRODUCTION_SOURCES).index("SemanticScholar")
    ieee_index = list(REQUIRED_PRODUCTION_SOURCES).index("IEEEXplore")
    semantic = wave.query_families[semantic_index]
    ieee = wave.query_families[ieee_index]
    wave.query_families[semantic_index] = replace(
        semantic, native_parameters={**semantic.native_parameters, "mode": None}
    )
    wave.query_families[ieee_index] = replace(
        ieee, native_parameters={**ieee.native_parameters, "max_records": 201}
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    with pytest.raises(ProductionWavePreflightError) as captured:
        preflight_production_wave(
            wave,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert {"SEMANTIC_SCHOLAR_MODE_MISSING", "IEEE_PAGE_SIZE_INVALID"}.issubset(
        _codes(captured.value)
    )


def test_manifest_serialization_and_hashing_are_deterministic(tmp_path):
    wave = _wave(tmp_path)
    serialized = wave.to_json()
    loaded = ProductionRetrievalWave.from_json(serialized)
    assert loaded.to_json() == serialized
    assert loaded.manifest_hash() == wave.manifest_hash()
    assert compute_query_plan_hash(loaded) == wave.query_plan_hash
    assert "api_key" in serialized
    assert "credential-value" not in serialized
    path = tmp_path / "wave.json"
    first_hash = save_production_wave(path, wave)
    first_bytes = path.read_bytes()
    second_hash = save_production_wave(path, load_production_wave(path))
    assert path.read_bytes() == first_bytes
    assert first_hash == second_hash


def test_cutoff_and_finalized_status_require_full_execution_dataset(tmp_path):
    ready = _wave(tmp_path)
    ready.retrieval_cutoff_date = "2026-09-01"
    with pytest.raises(ProductionWavePreflightError) as premature:
        preflight_production_wave(
            ready,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert "PREMATURE_RETRIEVAL_CUTOFF" in _codes(premature.value)

    finalized = _wave(tmp_path, status=ProductionWaveStatus.FINALIZED)
    finalized.retrieval_cutoff_date = "2026-09-01"
    with pytest.raises(ProductionWavePreflightError) as missing_execution:
        preflight_production_wave(
            finalized,
            manifest_root=tmp_path,
            configured_credentials={"IEEEXplore": {"api_key"}},
        )
    assert missing_execution.value.report.execution_complete is False
    assert missing_execution.value.report.finalizable is False


def test_finalized_status_accepts_only_matching_complete_full_wave(tmp_path):
    wave = _wave(tmp_path, status=ProductionWaveStatus.FINALIZED)
    wave.retrieval_cutoff_date = "2026-09-01"
    execution = _completed_execution(wave, tmp_path)
    report = preflight_production_wave(
        wave,
        manifest_root=tmp_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
        execution_dataset=execution,
    )
    assert report.execution_complete is True
    assert report.finalizable is True
    assert report.recommended_status is ProductionWaveStatus.FINALIZED
