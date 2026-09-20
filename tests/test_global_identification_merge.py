from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import h2h_lit.global_identification_merge as global_merge
from h2h_lit.arxiv_snapshot_integration import ValidatedSnapshotPackage
from h2h_lit.external_retrieval_wave import (
    ExternalRetrievalWaveError,
    _exclusive_external_source_session,
)
from h2h_lit.models import LiteratureRecord, ProcessingStatus
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    RecordOccurrence,
    RetrievalCompletionStatus,
    RetrievalRun,
    RetrievalRunKind,
    ReviewDataset,
    SourceQuery,
    canonicalize_occurrences,
)

STAMP = "2026-09-20T05:00:00+00:00"


def _dataset(
    *, source: str, run_id: str, records: list[tuple[str, LiteratureRecord]]
) -> ReviewDataset:
    query_id = f"query:{run_id}"
    run = RetrievalRun(
        run_id=run_id,
        kind=RetrievalRunKind.PRIMARY,
        query_plan_version="test-v1",
        query_plan_hash="0" * 64,
        planned_query_ids=[query_id],
        source_query_ids=[query_id],
        retrieval_started_at=STAMP,
        retrieval_completed_at=STAMP,
        status=ProcessingStatus.OK,
        protocol_version="1.0.0",
        retrieval_cutoff_date="2026-09-20",
        completion_status=RetrievalCompletionStatus.COMPLETE,
    )
    query = SourceQuery(
        query_id=query_id,
        source_database=source,
        query_text="test",
        retrieval_started_at=STAMP,
        retrieval_ended_at=STAMP,
        status=ProcessingStatus.OK,
        run_id=run_id,
        result_count=len(records),
        completion_status=RetrievalCompletionStatus.COMPLETE,
    )
    occurrences = [
        RecordOccurrence(
            occurrence_id=occurrence_id,
            source_query_id=query_id,
            source_identifier=record.source_identifier or occurrence_id,
            retrieved_at=STAMP,
            record=record,
        )
        for occurrence_id, record in records
    ]
    provenance = DecisionProvenance(
        actor=DecisionActor("test", ActorType.SOFTWARE),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=STAMP,
    )
    canonical, decisions = canonicalize_occurrences(occurrences, provenance=provenance)
    dataset = ReviewDataset(
        schema_version="1.3.0",
        retrieval_runs=[run],
        source_queries=[query],
        occurrences=occurrences,
        canonical_records=canonical,
        duplicate_decisions=decisions,
    )
    dataset.validate()
    return dataset


def _record(
    source: str,
    identifier: str,
    title: str,
    *,
    doi: str | None = None,
    arxiv_id: str | None = None,
    metadata: dict | None = None,
) -> LiteratureRecord:
    return LiteratureRecord(
        title=title,
        doi=doi,
        arxiv_id=arxiv_id,
        source_identifier=identifier,
        source_database=source,
        original_metadata=metadata or {},
    )


def _write_dataset(root: Path, name: str, dataset: ReviewDataset) -> dict:
    path = root / "datasets" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (dataset.to_json() + "\n").encode()
    path.write_bytes(content)
    return {
        "path": path.relative_to(root).as_posix(),
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _write_authorization(root: Path, name: str) -> dict:
    path = root / "authorizations" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (json.dumps({"source": name}, sort_keys=True) + "\n").encode()
    path.write_bytes(content)
    return {
        "path": path.relative_to(root).as_posix(),
        "byte_size": len(content),
        "raw_sha256": hashlib.sha256(content).hexdigest(),
    }


def _fixture(tmp_path: Path, monkeypatch) -> tuple[dict, ValidatedSnapshotPackage, int]:
    existing = _dataset(
        source="SemanticScholar",
        run_id="semantic-run",
        records=[
            (
                "existing-alpha",
                _record(
                    "SemanticScholar",
                    "alpha",
                    "Published alpha",
                    doi="10.1000/alpha",
                    arxiv_id="1234.5678v2",
                ),
            ),
            (
                "existing-beta",
                _record(
                    "SemanticScholar",
                    "beta",
                    "Published beta",
                    doi="10.1000/beta",
                ),
            ),
        ],
    )
    snapshot = _dataset(
        source="arXivSnapshotV303",
        run_id="arxiv-snapshot-v303:QF01",
        records=[
            (
                "snapshot-alpha-qf01",
                _record(
                    "arXivSnapshotV303",
                    "1234.5678",
                    "Alpha preprint",
                    arxiv_id="1234.5678",
                ),
            ),
            (
                "snapshot-alpha-qf02",
                _record(
                    "arXivSnapshotV303",
                    "1234.5678",
                    "Alpha preprint",
                    arxiv_id="1234.5678",
                ),
            ),
            (
                "snapshot-beta",
                _record(
                    "arXivSnapshotV303",
                    "9999.0001",
                    "Beta preprint",
                    arxiv_id="9999.0001",
                ),
            ),
        ],
    )
    identities = [
        {
            "proposal_id": "same-alpha",
            "normalized_arxiv_id": "1234.5678",
            "candidate_identity": {
                "arxiv_id": "1234.5678",
                "staging_candidate_id": "candidate-alpha",
                "source_line_number": 1,
            },
            "existing_identity": {"arxiv_id": "1234.5678v2"},
        }
    ]
    relationships = [
        {
            "proposal_id": "related-beta",
            "candidate_identity": {
                "arxiv_id": "9999.0001",
                "staging_candidate_id": "candidate-beta",
            },
            "existing_identity": {"identity_key": "doi:10.1000/beta"},
        }
    ]
    package = ValidatedSnapshotPackage(
        package_dir=tmp_path,
        package_manifest={},
        package_manifest_sha256="snapshot-package-sha",
        import_manifest={},
        amendment_v1={},
        snapshot_dataset=snapshot,
        identity_proposals=identities,
        relationship_proposals=relationships,
    )

    sources: dict[str, dict] = {}
    acm_families = []
    for ordinal in range(1, 6):
        dataset = _dataset(
            source="ACMDigitalLibrary",
            run_id=f"acm-qf{ordinal}",
            records=[
                (
                    f"acm-{ordinal}",
                    _record(
                        "ACMDigitalLibrary",
                        f"acm-{ordinal}",
                        f"ACM work {ordinal}",
                    ),
                )
            ],
        )
        acm_families.append(
            {
                "family_id": f"QF{ordinal:02d}",
                "occurrence_count": 1,
                "dataset": _write_dataset(tmp_path, f"acm-{ordinal}", dataset),
            }
        )
    sources["ACMDigitalLibrary"] = {
        "status": "COMPLETE",
        "occurrence_count": 5,
        "family_datasets": acm_families,
    }
    for source in ("EuropePMC", "IEEEXplore", "PubMed"):
        dataset = _dataset(
            source=source,
            run_id=f"{source}-run",
            records=[
                (
                    f"{source}-occurrence",
                    _record(source, f"{source}-id", f"{source} work"),
                )
            ],
        )
        sources[source] = {
            "status": "COMPLETE",
            "occurrence_count": 1,
            "checkpoint_dataset": _write_dataset(tmp_path, source.lower(), dataset),
            "execution_episodes": [{"historical": True}],
        }
    sources["SemanticScholar"] = {
        "status": "COMPLETE",
        "occurrence_count": 2,
        "checkpoint_dataset": _write_dataset(tmp_path, "semantic", existing),
        "execution_episodes": [{"historical": True}],
    }
    snapshot_ref = _write_dataset(tmp_path, "snapshot", snapshot)
    sources["arXiv"] = {
        "status": "PAUSED_TRANSIENT_PROVIDER",
        "occurrence_count": 0,
        "checkpoint_dataset": _write_dataset(
            tmp_path,
            "arxiv-api-zero",
            _dataset(source="arXiv", run_id="arxiv-api", records=[]),
        ),
        "approved_snapshot_substitution": {
            "status": "APPROVED_COMPLETE_REPLACEMENT_ROUTE",
            "snapshot_checkpoint": snapshot_ref,
            "counts": {"query_membership_occurrences": 3},
            "package": {"path": "snapshot-package", "manifest_sha256": "sha"},
        },
    }

    prior: dict[str, dict] = {}
    for seed in ("JFR25", "EBK25", "FP19"):
        membership = "UNCONFIRMED" if seed == "EBK25" else "CONFIRMED_SURVEY_MEMBER"
        metadata = {
            "seed_set_id": seed,
            "source_survey_membership": membership,
            "our_star_eligibility": "UNASSESSED",
        }
        if seed == "EBK25":
            metadata["identity_resolution"] = "UNRESOLVED"
            metadata["provisional_identity_group_id"] = "ebk25-candidate:unresolved"
        dataset = _dataset(
            source="PriorSurveySeed",
            run_id=f"prior-{seed}",
            records=[
                (
                    f"prior-{seed}-occurrence",
                    _record(
                        "PriorSurveySeed",
                        f"{seed}:1",
                        f"{seed} work",
                        metadata=metadata,
                    ),
                )
            ],
        )
        prior[seed] = {
            "status": "AUTHORIZED_IMPORTED_NOT_GLOBALLY_MERGED",
            "dataset": _write_dataset(tmp_path, f"prior-{seed}", dataset),
            "authorization": _write_authorization(tmp_path, seed),
            "counts": {
                "source_occurrences": 1,
                "unresolved_identity_groups": 1 if seed == "EBK25" else 0,
            },
        }
    state = {
        "schema_version": "1.0.0",
        "execution_id": "wave",
        "status": "COMPLETE",
        "wave_manifest_hash": "wave",
        "planned_wave_raw_sha256": "plan",
        "preflight_raw_sha256": "preflight",
        "sources": sources,
        "prior_survey_imports": prior,
        "external_retrieval_completed_at_utc": STAMP,
        "external_retrieval_cutoff_date": "2026-09-20",
        "prior_survey_seed_imported": False,
        "identification_set_closed": False,
        "final_global_deduplication_executed": False,
        "screening_executed": False,
        "prisma_generated": False,
        "corpus_modified": False,
    }
    monkeypatch.setattr(global_merge, "validate_authorized_snapshot_substitution", lambda **_: None)
    monkeypatch.setattr(global_merge, "validate_authorized_prior_survey_imports", lambda **_: None)
    monkeypatch.setattr(global_merge, "_implementation_bindings", lambda _root: [])
    monkeypatch.setattr(global_merge, "_snapshot_package", lambda *_: package)
    expected = 5 + 3 + 2 + 3 + 3
    return state, package, expected


def _prepare(tmp_path: Path, monkeypatch) -> tuple[dict, dict, Path]:
    state, _package, expected = _fixture(tmp_path, monkeypatch)
    output = tmp_path / "staging" / "global-merge"
    result = global_merge.prepare_staged_global_merge(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
        output_dir=output,
        availability=global_merge.ResourceAvailability(99 * 1024**3, 99 * 1024**3, 99 * 1024**3),
    )
    assert result["manifest"]["counts"]["output_occurrences"] == expected
    return state, result, output


def test_plan_selects_only_authoritative_registered_inputs(tmp_path, monkeypatch):
    state, _package, expected = _fixture(tmp_path, monkeypatch)
    plan = global_merge.build_global_merge_plan(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
    )

    assert plan["input_dataset_count"] == 13
    assert plan["input_occurrence_count"] == expected
    assert (
        sum(item["role"] == "authoritative_external_query_family" for item in plan["inputs"]) == 5
    )
    assert all("historical" not in item["dataset"]["path"] for item in plan["inputs"])
    assert all("arxiv-api-zero" not in item["dataset"]["path"] for item in plan["inputs"])
    assert {
        item["source"] for item in plan["inputs"] if item["role"] == "authorized_prior_survey"
    } == {
        "PriorSurveySeed/JFR25",
        "PriorSurveySeed/EBK25",
        "PriorSurveySeed/FP19",
    }


def test_plan_binds_staging_only_source_relocation(tmp_path, monkeypatch):
    state, _package, _expected = _fixture(tmp_path, monkeypatch)
    binding = {
        "status": "STAGING_ONLY_EXACT_SOURCE_RELOCATION",
        "authoritative_execution_state_sha256": "state-sha",
        "production_state_modified": False,
    }
    plan = global_merge.build_global_merge_plan(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
        source_relocation_binding=binding,
    )
    assert plan["source_relocation"] == binding

    changed = {**binding, "authoritative_execution_state_sha256": "changed"}
    with pytest.raises(
        global_merge.GlobalIdentificationMergeError,
        match="differs from authoritative state",
    ):
        global_merge.build_global_merge_plan(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            source_relocation_binding=changed,
        )


def test_staging_merge_preserves_occurrences_adjudications_and_uncertainty(tmp_path, monkeypatch):
    _state, result, output = _prepare(tmp_path, monkeypatch)
    reconciliation = result["manifest"]["counts"]
    merged = global_merge.load_review_dataset(output / "review_dataset.json")

    assert reconciliation["input_occurrences"] == reconciliation["output_occurrences"]
    assert reconciliation["duplicate_decisions"]["approved_identity_proposals"] == 1
    assert reconciliation["duplicate_decisions"]["adjudicated_history_entries"] == 2
    assert reconciliation["related_versions"] == {
        "proposal_links": 1,
        "preserved_links": 1,
        "collapsed": False,
    }
    assert reconciliation["remaining_identity_review"] == {
        "source_unresolved_identity_groups": 1,
        "source_unresolved_identity_groups_sha256": global_merge._json_hash(
            {"ebk25-candidate:unresolved": ["prior-EBK25-occurrence"]}
        ),
        "source_unresolved_occurrences": 1,
        "merged_canonical_records_carrying_unresolved_markers": 1,
        "related_version_candidates": 1,
        "related_version_links": 1,
        "study_grouping_decided": False,
    }
    assert len(merged.effective_duplicate_decisions()) == len(merged.occurrences)
    beta = [
        item
        for item in merged.canonical_records
        if item.record.title in {"Published beta", "Beta preprint"}
    ]
    assert len(beta) == 2
    ebk = next(
        item
        for item in merged.occurrences
        if item.record.original_metadata.get("seed_set_id") == "EBK25"
    )
    assert ebk.record.original_metadata["source_survey_membership"] == "UNCONFIRMED"
    assert ebk.record.original_metadata["our_star_eligibility"] == "UNASSESSED"


@pytest.mark.parametrize("changed_field", ["occurrence", "source_query"])
def test_reconciliation_rejects_changed_source_evidence(tmp_path, monkeypatch, changed_field):
    state, package, _expected = _fixture(tmp_path, monkeypatch)
    plan = global_merge.build_global_merge_plan(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
    )
    inputs = global_merge._load_inputs(tmp_path, plan)
    evidence_hashes = global_merge._source_evidence_hashes(inputs)
    merged = global_merge.merge_identification_datasets_with_prior_surveys_and_snapshot(
        inputs, package
    )
    if changed_field == "occurrence":
        merged.occurrences[0].record.original_metadata["unexpected"] = True
    else:
        merged.source_queries[0].query_text = "changed"

    with pytest.raises(
        global_merge.GlobalIdentificationMergeError,
        match="content or identity inventory",
    ):
        global_merge.reconcile_global_merge(
            inputs=inputs,
            merged=merged,
            snapshot_package=package,
            plan=plan,
            source_evidence_hashes=evidence_hashes,
        )


def test_staging_is_idempotent_and_recovers_after_manifest_interruption(tmp_path, monkeypatch):
    state, first, output = _prepare(tmp_path, monkeypatch)
    original_merge = global_merge.merge_identification_datasets_with_prior_surveys_and_snapshot
    monkeypatch.setattr(
        global_merge,
        "merge_identification_datasets_with_prior_surveys_and_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("reran merge")),
    )
    second = global_merge.prepare_staged_global_merge(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
        output_dir=output,
        availability=global_merge.ResourceAvailability(100 * 1024**3, 100 * 1024**3, 100 * 1024**3),
    )
    assert second["idempotent"] is True
    assert second["manifest_sha256"] == first["manifest_sha256"]
    monkeypatch.setattr(
        global_merge,
        "merge_identification_datasets_with_prior_surveys_and_snapshot",
        original_merge,
    )

    recovery_root = tmp_path / "recovery"
    recovery_root.mkdir()
    recovery_state, _package, _expected = _fixture(recovery_root, monkeypatch)
    recovery_output = recovery_root / "staging" / "global-merge"
    original_write = global_merge._write_json_once

    def interrupt(path, value):
        if path.name == "reconciliation.json":
            raise RuntimeError("simulated interruption")
        return original_write(path, value)

    monkeypatch.setattr(global_merge, "_write_json_once", interrupt)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        global_merge.prepare_staged_global_merge(
            root=recovery_root,
            state=recovery_state,
            execution_state_raw_sha256="recovery-state",
            output_dir=recovery_output,
            availability=global_merge.ResourceAvailability(
                100 * 1024**3, 100 * 1024**3, 100 * 1024**3
            ),
        )
    assert (recovery_output / "review_dataset.json").is_file()
    assert not (recovery_output / "package_manifest.json").exists()
    monkeypatch.setattr(global_merge, "_write_json_once", original_write)
    recovered = global_merge.prepare_staged_global_merge(
        root=recovery_root,
        state=recovery_state,
        execution_state_raw_sha256="recovery-state",
        output_dir=recovery_output,
        availability=global_merge.ResourceAvailability(100 * 1024**3, 100 * 1024**3, 100 * 1024**3),
    )
    assert recovered["manifest"]["status"] == "STAGED_GLOBAL_MERGE_NOT_REGISTERED"


def test_resource_and_input_drift_refuse_before_merge(tmp_path, monkeypatch):
    state, _package, _expected = _fixture(tmp_path, monkeypatch)
    output = tmp_path / "staging" / "global-merge"
    with pytest.raises(global_merge.GlobalIdentificationMergeError, match="resource preflight"):
        global_merge.prepare_staged_global_merge(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
            output_dir=output,
            availability=global_merge.ResourceAvailability(1, 1, 1),
        )
    first_input = state["sources"]["ACMDigitalLibrary"]["family_datasets"][0]["dataset"]
    (tmp_path / first_input["path"]).write_text("tampered", encoding="utf-8")
    with pytest.raises(global_merge.GlobalIdentificationMergeError, match="size changed"):
        global_merge.build_global_merge_plan(
            root=tmp_path,
            state=state,
            execution_state_raw_sha256="state-sha",
        )


def test_resource_guard_requires_22_gib_actually_available(tmp_path, monkeypatch):
    state, _package, _expected = _fixture(tmp_path, monkeypatch)
    plan = global_merge.build_global_merge_plan(
        root=tmp_path,
        state=state,
        execution_state_raw_sha256="state-sha",
    )
    below = global_merge.resource_preflight(
        plan,
        global_merge.ResourceAvailability(
            100 * 1024**3,
            global_merge.MIN_AVAILABLE_MEMORY_BYTES - 1,
            100 * 1024**3,
        ),
    )
    at_threshold = global_merge.resource_preflight(
        plan,
        global_merge.ResourceAvailability(
            100 * 1024**3,
            global_merge.MIN_AVAILABLE_MEMORY_BYTES,
            100 * 1024**3,
        ),
    )

    assert below["installed_memory_sufficient"] is True
    assert below["available_memory_sufficient"] is False
    assert below["memory_sufficient"] is False
    assert at_threshold["available_memory_sufficient"] is True
    assert at_threshold["memory_sufficient"] is True


def test_production_registration_is_idempotent_without_closing_identification(
    tmp_path, monkeypatch
):
    state, result, output = _prepare(tmp_path, monkeypatch)
    state, first = global_merge.authorize_production_global_merge(
        root=tmp_path,
        state=state,
        package_dir=output,
        expected_manifest_sha256=result["manifest_sha256"],
        authorized_at=STAMP,
    )
    state, second = global_merge.authorize_production_global_merge(
        root=tmp_path,
        state=state,
        package_dir=output,
        expected_manifest_sha256=result["manifest_sha256"],
        authorized_at="later",
    )

    assert first == second
    assert state["final_global_deduplication_executed"] is True
    assert state["identification_set_closed"] is False
    assert state["screening_executed"] is False
    assert state["prisma_generated"] is False
    with pytest.raises(
        global_merge.GlobalIdentificationMergeError,
        match="repeated global-merge authorization binding changed",
    ):
        global_merge.authorize_production_global_merge(
            root=tmp_path,
            state=state,
            package_dir=output,
            expected_manifest_sha256="changed",
            authorized_at="later",
        )


def test_production_authorization_accepts_relocation_bound_return_without_redirecting_paths(
    tmp_path, monkeypatch
):
    state, _result, output = _prepare(tmp_path, monkeypatch)
    relocation_binding = {
        "schema_version": "1.0.0",
        "status": "STAGING_ONLY_EXACT_SOURCE_RELOCATION",
        "manifest": {
            "path": "outputs/staging/relocation/relocation_manifest.json",
            "path_scope": "repository_relative",
            "byte_size": 1,
            "raw_sha256": "a" * 64,
        },
        "authoritative_execution_state_sha256": "state-sha",
        "entry_count": 1,
        "entries_sha256": "b" * 64,
        "production_state_modified": False,
    }
    manifest_path = output / "package_manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["source_relocation"] = relocation_binding
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    class ValidatedRelocation:
        def binding(self, _root):
            return relocation_binding

    monkeypatch.setattr(
        global_merge,
        "validate_source_relocation_manifest",
        lambda **_: ValidatedRelocation(),
    )
    strict_prior_validator = global_merge.validate_authorized_prior_survey_imports
    validated = global_merge.validate_staged_merge_package(
        root=tmp_path,
        package_dir=output,
        expected_manifest_sha256=manifest_sha256,
        state=state,
    )
    state, registration = global_merge.authorize_production_global_merge(
        root=tmp_path,
        state=state,
        package_dir=output,
        expected_manifest_sha256=manifest_sha256,
        authorized_at=STAMP,
    )

    assert validated["source_relocation"] == relocation_binding
    assert registration["status"] == "COMPLETE_NOT_IDENTIFICATION_CLOSED"
    assert state["identification_set_closed"] is False
    assert global_merge.validate_authorized_prior_survey_imports is strict_prior_validator


def test_global_merge_uses_shared_lock_before_state_load(tmp_path, monkeypatch):
    loaded = False

    def unexpected_load(_root):
        nonlocal loaded
        loaded = True
        raise AssertionError("state loaded while lock was held")

    monkeypatch.setattr(global_merge, "_load_production_state", unexpected_load)
    with (
        _exclusive_external_source_session(tmp_path),
        pytest.raises(
            ExternalRetrievalWaveError,
            match="another external-source session is already active",
        ),
    ):
        global_merge.prepare_global_merge_locked(
            root=tmp_path,
            output_dir=tmp_path / "staging",
            preflight_only=True,
        )
    assert loaded is False
