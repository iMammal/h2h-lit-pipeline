"""Deterministic local sample construction and Stage 5 pilot reporting."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h2h_lit.bibtex_io import parse_bibtex, record_from_bibtex_fields
from h2h_lit.inference import (
    FULL_TEXT_PROMPT,
    TITLE_ABSTRACT_PROMPT,
    InferenceInput,
    InferenceProvider,
    load_prompt_artifact,
    register_inference_run,
    run_inference_attempt,
)
from h2h_lit.models import LiteratureRecord, ProcessingStatus, utc_now_iso
from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.retrieval import save_review_dataset
from h2h_lit.review import (
    ActorType,
    AnnotationDimension,
    AnnotationState,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    EligibilityCriterion,
    EligibilityStatus,
    InferenceValidationStatus,
    RecordOccurrence,
    ReviewDataset,
    ScreeningStage,
    SourceQuery,
    TriState,
    canonicalize_occurrences,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = Path("config/stage5_pilot_v1.json")
DEFAULT_OUTPUT_DIR = Path("outputs/stage5_live_pilot_v1")
REVIEW_DATASET_NAME = "review_dataset.json"
SELECTION_MANIFEST_NAME = "pilot_selection_manifest.json"
REPORT_NAME = "pilot_report.json"
REVIEW_TABLE_NAME = "pilot_review_table.csv"


@dataclass(frozen=True, slots=True)
class PilotPaths:
    output_dir: Path
    review_dataset: Path
    selection_manifest: Path
    report: Path
    review_table: Path

    @classmethod
    def under(cls, output_dir: str | Path) -> PilotPaths:
        root = Path(output_dir)
        return cls(
            output_dir=root,
            review_dataset=root / REVIEW_DATASET_NAME,
            selection_manifest=root / SELECTION_MANIFEST_NAME,
            report=root / REPORT_NAME,
            review_table=root / REVIEW_TABLE_NAME,
        )


def load_pilot_config(path: str | Path = DEFAULT_CONFIG) -> dict[str, Any]:
    resolved = _resolve_project_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if payload.get("config_version") != "1.0.0":
        raise ValueError("unsupported Stage 5 pilot config version")
    return payload


def build_pilot_dataset(
    *,
    config_path: str | Path = DEFAULT_CONFIG,
    historical_root: str | Path | None = None,
    created_at: str,
) -> tuple[ReviewDataset, dict[str, Any]]:
    """Build the local pilot without consulting historical labels as current truth."""

    config_file = _resolve_project_path(config_path)
    config = load_pilot_config(config_file)
    root = Path(
        historical_root
        or os.environ.get(config["historical_root_environment"], "")
        or config["historical_root_default"]
    )
    foundational_path = _resolve_project_path(config["foundational_source"])
    if not root.is_dir():
        raise FileNotFoundError(f"historical root is not available: {root}")
    if not foundational_path.is_file():
        raise FileNotFoundError(f"foundational source is not available: {foundational_path}")

    selections: list[dict[str, Any]] = []
    queries: list[SourceQuery] = []
    occurrences: list[RecordOccurrence] = []
    foundational = json.loads(foundational_path.read_text(encoding="utf-8"))
    papers = {item["id"]: item for item in foundational["papers"]}
    foundational_specs = config["foundational_papers"]
    foundational_query_id = _stable_id("source-query", _portable(foundational_path))
    queries.append(
        SourceQuery(
            query_id=foundational_query_id,
            source_database="local_foundational_graph",
            query_text="Stage 5 curated foundational paper selection",
            query_version=config["config_version"],
            retrieval_started_at=created_at,
            retrieval_ended_at=created_at,
            status=ProcessingStatus.OK,
            run_id=f"stage5-pilot:{config['config_version']}",
            endpoint=_portable(foundational_path),
            result_count=len(foundational_specs),
            fields=["title", "authors", "year", "doi", "local_full_text_path"],
            software_version="0.1.0",
            metadata={"mode": "local_read_only", "source_hash": _file_hash(foundational_path)},
        )
    )
    for rank, spec in enumerate(foundational_specs, start=1):
        paper_id = spec["paper_id"]
        if paper_id not in papers:
            raise ValueError(f"unknown foundational paper ID in pilot config: {paper_id}")
        paper = papers[paper_id]
        source_file = paper.get("metadata", {}).get("source_file")
        record = LiteratureRecord(
            title=paper["title"],
            authors=list(paper.get("authors", [])),
            year=paper.get("year"),
            doi=paper.get("doi"),
            source_identifier=paper_id,
            source_database="local_foundational_graph",
            original_metadata={
                "pilot_selection": dict(spec),
                "source_file": source_file,
                "source_graph_paper": paper,
            },
        )
        occurrence = _occurrence(
            query_id=foundational_query_id,
            source_path=foundational_path,
            source_identifier=paper_id,
            record=record,
            rank=rank,
            created_at=created_at,
        )
        occurrences.append(occurrence)
        selections.append(_selection_entry(occurrence, spec, foundational_path))

    by_path: dict[str, list[dict[str, Any]]] = {}
    for spec in config["bibtex_records"]:
        by_path.setdefault(spec["path"], []).append(spec)
    for relative_path, specs in by_path.items():
        source_path = root / relative_path
        if not source_path.is_file():
            raise FileNotFoundError(f"pilot BibTeX source is not available: {source_path}")
        fields_by_key = {
            item["_key"]: item
            for item in parse_bibtex(source_path.read_text(encoding="utf-8", errors="replace"))
        }
        query_id = _stable_id("source-query", str(source_path))
        queries.append(
            SourceQuery(
                query_id=query_id,
                source_database="local_historical_bibtex",
                query_text=f"Stage 5 curated BibTeX keys from {relative_path}",
                query_version=config["config_version"],
                retrieval_started_at=created_at,
                retrieval_ended_at=created_at,
                status=ProcessingStatus.OK,
                run_id=f"stage5-pilot:{config['config_version']}",
                endpoint=str(source_path),
                result_count=len(specs),
                fields=["title", "abstract", "authors", "year", "doi", "document_type"],
                software_version="0.1.0",
                metadata={"mode": "local_read_only", "source_hash": _file_hash(source_path)},
            )
        )
        for rank, spec in enumerate(specs, start=1):
            key = spec["key"]
            fields = fields_by_key.get(key)
            if fields is None:
                raise ValueError(f"BibTeX key {key!r} is absent from {source_path}")
            record = record_from_bibtex_fields(fields)
            for event in record.provenance:
                event.timestamp = created_at
            record.title = _plain_bibtex(record.title)
            record.abstract = _plain_bibtex(record.abstract)
            record.source_identifier = key
            record.original_metadata["pilot_selection"] = dict(spec)
            occurrence = _occurrence(
                query_id=query_id,
                source_path=source_path,
                source_identifier=key,
                record=record,
                rank=rank,
                created_at=created_at,
            )
            occurrences.append(occurrence)
            selections.append(_selection_entry(occurrence, spec, source_path))

    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="software:h2h-lit-pipeline:stage5-pilot",
            actor_type=ActorType.SOFTWARE,
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=created_at,
        metadata={"rule": "doi_first_title_fallback", "pilot_config": _portable(config_file)},
    )
    canonical, duplicate_decisions = canonicalize_occurrences(
        occurrences,
        provenance=provenance,
    )
    dataset = ReviewDataset(
        source_queries=queries,
        occurrences=occurrences,
        canonical_records=canonical,
        duplicate_decisions=duplicate_decisions,
    )
    dataset.validate()
    occurrence_to_canonical = {
        occurrence_id: record.canonical_id
        for record in canonical
        for occurrence_id in record.occurrence_ids
    }
    for item in selections:
        item["canonical_record_id"] = occurrence_to_canonical[item["occurrence_id"]]
    manifest = {
        "config_version": config["config_version"],
        "config_path": _portable(config_file),
        "config_hash": _file_hash(config_file),
        "created_at": created_at,
        "selection_rule": (
            "Explicit version-controlled IDs/keys, preserving config order; historical strata "
            "are sampling intent only and are not current eligibility or taxonomy truth."
        ),
        "selected_occurrence_count": len(occurrences),
        "canonical_record_count": len(canonical),
        "selections": selections,
    }
    return dataset, manifest


def inference_input_for(record: Any) -> InferenceInput:
    metadata = record.record.original_metadata
    source_file = metadata.get("source_file")
    local_full_text = bool(source_file and _resolve_project_path(source_file).is_file())
    document_type = metadata.get("_type", "not recorded")
    language = metadata.get("language", "not recorded")
    if local_full_text:
        full_text = "local file available (content not supplied at title/abstract stage)"
    elif record.record.is_open_access is True:
        full_text = "open access reported historically; local availability not verified"
    else:
        full_text = "not recorded"
    abstract = record.record.abstract.strip() or "[not available in local bibliographic record]"
    input_text = "\n".join(
        [
            f"TITLE: {record.record.title.strip()}",
            f"ABSTRACT: {abstract}",
            f"YEAR: {record.record.year or 'not recorded'}",
            f"DOCUMENT_TYPE: {document_type}",
            f"LANGUAGE: {language}",
            f"FULL_TEXT_AVAILABILITY: {full_text}",
            "RETRIEVAL_END_DATE: not yet established (pilot precedes regenerated retrieval)",
            f"SOURCE: {record.record.source_database or 'not recorded'}",
        ]
    )
    artifact_path = source_file or metadata.get("pilot_selection", {}).get("path")
    return InferenceInput(
        canonical_record_id=record.canonical_id,
        stage=ScreeningStage.TITLE_ABSTRACT,
        text=input_text,
        source_location="title_abstract",
        source_artifact_id=str(artifact_path) if artifact_path else None,
        metadata={
            "title": record.record.title,
            "doi": record.record.doi,
            "source_identifier": record.record.source_identifier,
            "input_policy": "title_abstract_plus_administrative_metadata",
        },
    )


def run_live_pilot(
    *,
    provider: InferenceProvider,
    config_path: str | Path = DEFAULT_CONFIG,
    historical_root: str | Path | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    created_at: str | None = None,
) -> tuple[ReviewDataset, dict[str, Any], PilotPaths]:
    timestamp = created_at or utc_now_iso()
    paths = PilotPaths.under(output_dir)
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    config = load_pilot_config(config_path)
    dataset, manifest = build_pilot_dataset(
        config_path=config_path,
        historical_root=historical_root,
        created_at=timestamp,
    )
    _write_json(paths.selection_manifest, manifest)
    run = register_inference_run(
        dataset,
        stage=ScreeningStage.TITLE_ABSTRACT,
        prompt_path=TITLE_ABSTRACT_PROMPT,
        provider=config["model"]["provider"],
        model=config["model"]["name"],
        parameters=config["model"]["parameters"],
        created_at=timestamp,
        run_id=_stable_id("inference-run", manifest["config_hash"], timestamp),
    )
    save_review_dataset(paths.review_dataset, dataset)

    retry_limit = int(config["retry_limit_per_record"])
    for record in dataset.canonical_records:
        inference_input = inference_input_for(record)
        for _ in range(retry_limit + 1):
            attempt = run_inference_attempt(
                dataset,
                run_id=run.run_id,
                inference_input=inference_input,
                provider=provider,
            )
            if isinstance(provider, OpenAIResponsesProvider):
                attempt.metadata["provider_response"] = provider.metadata_for(
                    attempt.request_id, attempt.attempt_number
                )
            save_review_dataset(paths.review_dataset, dataset)
            if attempt.validation_status is InferenceValidationStatus.VALID:
                break

    report = build_pilot_report(dataset, config=config, manifest=manifest)
    _write_json(paths.report, report)
    _write_review_table(paths.review_table, report["review_rows"])
    return dataset, report, paths


def prepare_pilot(
    *,
    config_path: str | Path = DEFAULT_CONFIG,
    historical_root: str | Path | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    created_at: str | None = None,
) -> tuple[ReviewDataset, dict[str, Any], dict[str, Any], PilotPaths]:
    """Materialize the deterministic sample and a no-call preflight report."""

    timestamp = created_at or utc_now_iso()
    paths = PilotPaths.under(output_dir)
    dataset, manifest = build_pilot_dataset(
        config_path=config_path,
        historical_root=historical_root,
        created_at=timestamp,
    )
    config = load_pilot_config(config_path)
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    save_review_dataset(paths.review_dataset, dataset)
    _write_json(paths.selection_manifest, manifest)
    prompt = load_prompt_artifact(
        TITLE_ABSTRACT_PROMPT, stage=ScreeningStage.TITLE_ABSTRACT
    )
    full_text_prompt = load_prompt_artifact(
        FULL_TEXT_PROMPT, stage=ScreeningStage.FULL_TEXT
    )
    retries = int(config["retry_limit_per_record"])
    max_calls = len(dataset.canonical_records) * (retries + 1)
    estimated_input_tokens = 0
    for record in dataset.canonical_records:
        snapshot = inference_input_for(record).to_snapshot()
        characters = len(prompt.content) + len(
            json.dumps(snapshot, sort_keys=True, ensure_ascii=True)
        )
        estimated_input_tokens += (characters + 3) // 4
    estimated_input_tokens *= retries + 1
    maximum_output_tokens = max_calls * int(config["model"]["parameters"]["max_output_tokens"])
    prices = config["model"]["pricing_usd_per_million_tokens"]
    max_cost = (
        estimated_input_tokens * prices["input"]
        + maximum_output_tokens * prices["output"]
    ) / 1_000_000
    preflight = {
        "provider": config["model"]["provider"],
        "model": config["model"]["name"],
        "parameters": config["model"]["parameters"],
        "prompt": {
            "path": prompt.path,
            "version": prompt.version,
            "sha256": prompt.content_hash,
        },
        "full_text_prompt_not_invoked": {
            "path": full_text_prompt.path,
            "version": full_text_prompt.version,
            "sha256": full_text_prompt.content_hash,
        },
        "sample_source": (
            "Explicit IDs/keys from the foundational source graph and read-only historical "
            "BibTeX files; config order is authoritative."
        ),
        "selection_rule": manifest["selection_rule"],
        "selected_occurrences": manifest["selected_occurrence_count"],
        "canonical_records": manifest["canonical_record_count"],
        "retry_limit_per_record": retries,
        "maximum_model_calls": max_calls,
        "estimated_token_exposure": {
            "input_tokens_approximate": estimated_input_tokens,
            "output_tokens_hard_maximum": maximum_output_tokens,
        },
        "estimated_cost_exposure_usd": {
            "upper_bound_approximate": round(max_cost, 4),
            "pricing": prices,
            "assumption": "No cached-input discount; four characters per input token.",
        },
        "output_paths": {
            "review_dataset": str(paths.review_dataset),
            "selection_manifest": str(paths.selection_manifest),
            "report": str(paths.report),
            "review_table": str(paths.review_table),
        },
        "live_calls_made": 0,
    }
    return dataset, manifest, preflight, paths


def build_pilot_report(
    dataset: ReviewDataset,
    *,
    config: dict[str, Any],
    manifest: dict[str, Any],
) -> dict[str, Any]:
    attempts_by_record: dict[str, list[Any]] = {}
    for attempt in dataset.inference_attempts:
        attempts_by_record.setdefault(attempt.canonical_record_id, []).append(attempt)
    screens = {item.decision_id: item for item in dataset.screening_decisions}
    annotations = {item.annotation_id: item for item in dataset.annotations}

    final_attempts = []
    review_rows: list[dict[str, Any]] = []
    criterion_uncertain = Counter()
    statuses = Counter()
    assistance = Counter()
    modalities = Counter()
    tasks = Counter()
    overlap = Counter()
    escalation: list[dict[str, str]] = []
    for record in dataset.canonical_records:
        record_attempts = sorted(
            attempts_by_record.get(record.canonical_id, []), key=lambda item: item.attempt_number
        )
        final = record_attempts[-1] if record_attempts else None
        if final is not None:
            final_attempts.append(final)
        screen = screens.get(final.screening_decision_id) if final else None
        uncertain_codes: list[str] = []
        present_assistance: list[str] = []
        present_modalities: list[str] = []
        present_tasks: list[str] = []
        if screen is not None:
            statuses[screen.status.value] += 1
            for criterion in screen.criteria:
                if criterion.value is TriState.UNCERTAIN:
                    criterion_uncertain[criterion.criterion.value] += 1
                    uncertain_codes.append(criterion.criterion.value.split("_", 1)[0])
            for annotation_id in final.annotation_ids:
                item = annotations[annotation_id]
                if item.state is not AnnotationState.PRESENT:
                    continue
                if item.dimension is AnnotationDimension.ASSISTANCE_MODE:
                    assistance[item.value] += 1
                    present_assistance.append(item.value)
                elif item.dimension is AnnotationDimension.VISUALIZATION_MODALITY:
                    modalities[item.value] += 1
                    present_modalities.append(item.value)
                else:
                    tasks[item.value] += 1
                    present_tasks.append(item.value)
            overlap[" + ".join(sorted(present_assistance)) or "none"] += 1
            if screen.status is EligibilityStatus.UNCERTAIN:
                escalation.append(
                    {"canonical_record_id": record.canonical_id, "title": record.record.title}
                )
        review_rows.append(
            {
                "canonical_record_id": record.canonical_id,
                "title": record.record.title,
                "final_validation": final.validation_status.value if final else "not_run",
                "attempts": len(record_attempts),
                "eligibility_proposal": screen.status.value if screen else "INVALID",
                "uncertain_criteria": "|".join(uncertain_codes),
                "assistance_present": "|".join(present_assistance),
                "modalities_present": "|".join(present_modalities),
                "tasks_present": "|".join(present_tasks),
            }
        )

    valid_final = sum(
        item.validation_status is InferenceValidationStatus.VALID for item in final_attempts
    )
    valid_attempts = sum(
        item.validation_status is InferenceValidationStatus.VALID
        for item in dataset.inference_attempts
    )
    invalid_errors = Counter(
        error
        for attempt in dataset.inference_attempts
        if attempt.validation_status is InferenceValidationStatus.INVALID
        for error in attempt.validation_errors
    )
    retried_records = sum(len(items) > 1 for items in attempts_by_record.values())
    evidence_failures = [
        {
            "attempt_id": attempt.attempt_id,
            "record_id": attempt.canonical_record_id,
            "errors": attempt.validation_errors,
        }
        for attempt in dataset.inference_attempts
        if any(
            token in error.lower()
            for error in attempt.validation_errors
            for token in ("evidence", "offset", "quote", "locator")
        )
    ]
    input_tokens = output_tokens = cached_tokens = 0
    for attempt in dataset.inference_attempts:
        usage = attempt.metadata.get("provider_response", {}).get("provider_usage") or {}
        input_tokens += int(usage.get("input_tokens", 0) or 0)
        output_tokens += int(usage.get("output_tokens", 0) or 0)
        details = usage.get("input_tokens_details") or {}
        cached_tokens += int(details.get("cached_tokens", 0) or 0)
    prices = config["model"]["pricing_usd_per_million_tokens"]
    estimated_cost = (
        (input_tokens - cached_tokens) * prices["input"]
        + cached_tokens * prices["cached_input"]
        + output_tokens * prices["output"]
    ) / 1_000_000
    valid_denominator = valid_final or 1
    return {
        "pilot_config_version": config["config_version"],
        "selection_manifest_hash": _json_hash(manifest),
        "model": config["model"],
        "records": len(dataset.canonical_records),
        "attempts": len(dataset.inference_attempts),
        "valid_attempts": valid_attempts,
        "invalid_attempts": len(dataset.inference_attempts) - valid_attempts,
        "attempt_valid_response_rate": (
            valid_attempts / len(dataset.inference_attempts)
            if dataset.inference_attempts
            else 0.0
        ),
        "valid_final_responses": valid_final,
        "invalid_final_responses": len(final_attempts) - valid_final,
        "valid_response_rate": valid_final / len(final_attempts) if final_attempts else 0.0,
        "retry_rate": retried_records / len(dataset.canonical_records),
        "eligibility_proposal_distribution": dict(sorted(statuses.items())),
        "criterion_uncertainty": {
            criterion.value: {
                "count": criterion_uncertain[criterion.value],
                "rate_among_valid": criterion_uncertain[criterion.value] / valid_denominator,
            }
            for criterion in EligibilityCriterion
        },
        "assistance_mode_frequencies": dict(sorted(assistance.items())),
        "assistance_mode_overlap": dict(sorted(overlap.items())),
        "visualization_modality_frequencies": dict(sorted(modalities.items())),
        "task_frequencies": dict(sorted(tasks.items())),
        "evidence_span_validation_failures": evidence_failures,
        "validation_failure_frequencies": dict(sorted(invalid_errors.items())),
        "full_text_escalation_count": len(escalation),
        "full_text_escalation_records": escalation,
        "usage": {
            "input_tokens": input_tokens,
            "cached_input_tokens": cached_tokens,
            "output_tokens": output_tokens,
            "estimated_cost_usd": round(estimated_cost, 6),
        },
        "methodological_constraints": [
            (
                "The regenerated retrieval end date does not yet exist; E6 must remain uncertain "
                "when that date is necessary for a determination."
            ),
            "Pilot selection strata are challenge-set intent, not human gold labels.",
            "No LLM proposal creates corpus membership.",
        ],
        "prompt_rubric_failure_modes": [
            (
                "E6 publication-date evaluation cannot be completed until the regenerated "
                "retrieval run establishes its end date; the pilot input exposes that value as "
                "not yet established rather than silently choosing a cutoff."
            ),
            *(
                ["One or more provider outputs failed the frozen Stage 4 validator."]
                if invalid_errors
                else []
            ),
        ],
        "review_rows": review_rows,
    }


def _occurrence(
    *,
    query_id: str,
    source_path: Path,
    source_identifier: str,
    record: LiteratureRecord,
    rank: int,
    created_at: str,
) -> RecordOccurrence:
    raw_hash = _json_hash(record.original_metadata)
    return RecordOccurrence(
        occurrence_id=_stable_id("occurrence", str(source_path), source_identifier),
        source_query_id=query_id,
        source_identifier=source_identifier,
        retrieved_at=created_at,
        record=record,
        source_rank=rank,
        raw_payload_hash=raw_hash,
        metadata={"local_source_path": str(source_path), "read_only": True},
    )


def _selection_entry(
    occurrence: RecordOccurrence,
    spec: dict[str, Any],
    source_path: Path,
) -> dict[str, Any]:
    return {
        "occurrence_id": occurrence.occurrence_id,
        "source_identifier": occurrence.source_identifier,
        "title": occurrence.record.title,
        "selection_stratum": spec["selection_stratum"],
        "source_path": str(source_path),
        "source_hash": _file_hash(source_path),
    }


def _write_review_table(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _resolve_project_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def _portable(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _plain_bibtex(value: str) -> str:
    return value.replace("{", "").replace("}", "").replace("\\_", "_").strip()


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_hash(payload: Any) -> str:
    content = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"
