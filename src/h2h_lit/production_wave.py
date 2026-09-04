"""Offline manifest and preflight gates for a frozen production retrieval wave."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write
from h2h_lit.models import ProcessingStatus
from h2h_lit.review import (
    IdentificationRoute,
    RetrievalCompletionStatus,
    RetrievalTransportKind,
    ReviewDataset,
)
from h2h_lit.sources.acm_dl import import_acm_bibtex_manifest
from h2h_lit.sources.arxiv import PAGINATOR as ARXIV_PAGINATOR
from h2h_lit.sources.crossref import PAGINATOR as CROSSREF_PAGINATOR
from h2h_lit.sources.europe_pmc import PAGINATOR as EUROPE_PMC_PAGINATOR
from h2h_lit.sources.ieee_xplore import ABSTRACT_CONTENT_POLICY
from h2h_lit.sources.ieee_xplore import PAGINATOR as IEEE_XPLORE_PAGINATOR
from h2h_lit.sources.prior_survey_seed import import_seed_manifest
from h2h_lit.sources.pubmed import PAGINATOR as PUBMED_PAGINATOR
from h2h_lit.sources.semantic_scholar import PAGINATOR as SEMANTIC_SCHOLAR_PAGINATOR

REQUIRED_PRODUCTION_SOURCES = (
    "PubMed",
    "EuropePMC",
    "CrossRef",
    "SemanticScholar",
    "arXiv",
    "IEEEXplore",
    "ACMDigitalLibrary",
    "PriorSurveySeed",
)

REQUIRED_IDENTIFICATION_SOURCES_V2 = (
    "PubMed",
    "EuropePMC",
    "SemanticScholar",
    "arXiv",
    "IEEEXplore",
    "ACMDigitalLibrary",
    "PriorSurveySeed",
)
REQUIRED_SUPPORT_SOURCES_V2 = ("CrossRef",)
EXTERNAL_IDENTIFICATION_SOURCES_V2 = tuple(
    source
    for source in REQUIRED_IDENTIFICATION_SOURCES_V2
    if source != "PriorSurveySeed"
)
EXTERNAL_RETRIEVAL_EXECUTION_SCOPE = "external_retrieval"


class ProductionWaveStatus(str, Enum):
    PLANNED = "planned"
    READY = "ready"
    INCOMPLETE = "incomplete"
    FINALIZED = "finalized"


class ResultWindowStatus(str, Enum):
    CLEAR = "clear"
    UNKNOWN_UNTIL_EXECUTION = "unknown_until_execution"
    UNRESOLVED = "unresolved"


class ArtifactKind(str, Enum):
    ACM_EXPORT_MANIFEST = "acm_export_manifest"
    PRIOR_SURVEY_SEED_MANIFEST = "prior_survey_seed_manifest"


@dataclass(frozen=True, slots=True)
class PaginationExpectation:
    strategy: str
    adapter_version: str
    completion_proofs: list[str]
    exact_total_required: bool
    maximum_supported_results: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PaginationExpectation:
        return cls(
            strategy=str(data["strategy"]),
            adapter_version=str(data["adapter_version"]),
            completion_proofs=list(data["completion_proofs"]),
            exact_total_required=bool(data["exact_total_required"]),
            maximum_supported_results=data.get("maximum_supported_results"),
        )


@dataclass(frozen=True, slots=True)
class RequiredArtifact:
    kind: ArtifactKind
    manifest_path: str
    manifest_sha256: str
    expected_total: int
    expected_chunks: list[dict[str, Any]] = field(default_factory=list)
    seed_set_id: str | None = None
    seed_set_version: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RequiredArtifact:
        return cls(
            kind=ArtifactKind(data["kind"]),
            manifest_path=str(data["manifest_path"]),
            manifest_sha256=str(data["manifest_sha256"]),
            expected_total=int(data["expected_total"]),
            expected_chunks=[dict(item) for item in data.get("expected_chunks", [])],
            seed_set_id=data.get("seed_set_id"),
            seed_set_version=data.get("seed_set_version"),
        )


@dataclass(frozen=True, slots=True)
class ProductionQueryFamily:
    query_family_id: str
    source_database: str
    source_role: str
    identification_route: IdentificationRoute
    transport_kind: RetrievalTransportKind
    adapter_id: str
    adapter_version: str
    query_version: str
    query_text: str
    native_parameters: dict[str, Any]
    pagination: PaginationExpectation
    required_credentials: list[str] = field(default_factory=list)
    content_policy: dict[str, str] = field(default_factory=dict)
    result_window_status: ResultWindowStatus = ResultWindowStatus.UNKNOWN_UNTIL_EXECUTION
    unresolved_requirement: str | None = None
    required_artifact: RequiredArtifact | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProductionQueryFamily:
        artifact = data.get("required_artifact")
        return cls(
            query_family_id=str(data["query_family_id"]),
            source_database=str(data["source_database"]),
            source_role=str(data["source_role"]),
            identification_route=IdentificationRoute(data["identification_route"]),
            transport_kind=RetrievalTransportKind(data["transport_kind"]),
            adapter_id=str(data["adapter_id"]),
            adapter_version=str(data["adapter_version"]),
            query_version=str(data["query_version"]),
            query_text=str(data["query_text"]),
            native_parameters=dict(data["native_parameters"]),
            pagination=PaginationExpectation.from_dict(data["pagination"]),
            required_credentials=list(data.get("required_credentials", [])),
            content_policy=dict(data.get("content_policy", {})),
            result_window_status=ResultWindowStatus(
                data.get(
                    "result_window_status",
                    ResultWindowStatus.UNKNOWN_UNTIL_EXECUTION.value,
                )
            ),
            unresolved_requirement=data.get("unresolved_requirement"),
            required_artifact=RequiredArtifact.from_dict(artifact) if artifact else None,
        )


@dataclass(slots=True)
class ProductionRetrievalWave:
    schema_version: str
    wave_id: str
    wave_version: str
    query_plan_version: str
    query_plan_hash: str
    required_sources: list[str]
    query_families: list[ProductionQueryFamily]
    status: ProductionWaveStatus
    retrieval_cutoff_date: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    support_sources: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = _serialize(self)
        if self.schema_version == "1.0.0":
            payload.pop("support_sources", None)
        return payload

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )

    def manifest_hash(self) -> str:
        return hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProductionRetrievalWave:
        return cls(
            schema_version=str(data["schema_version"]),
            wave_id=str(data["wave_id"]),
            wave_version=str(data["wave_version"]),
            query_plan_version=str(data["query_plan_version"]),
            query_plan_hash=str(data["query_plan_hash"]),
            required_sources=list(data["required_sources"]),
            query_families=[
                ProductionQueryFamily.from_dict(item) for item in data["query_families"]
            ],
            status=ProductionWaveStatus(data["status"]),
            retrieval_cutoff_date=data.get("retrieval_cutoff_date"),
            metadata=dict(data.get("metadata", {})),
            support_sources=list(data.get("support_sources", [])),
        )

    @classmethod
    def from_json(cls, text: str) -> ProductionRetrievalWave:
        return cls.from_dict(json.loads(text))


@dataclass(frozen=True, slots=True)
class PreflightIssue:
    code: str
    message: str
    query_family_id: str | None = None


@dataclass(frozen=True, slots=True)
class ProductionWavePreflight:
    wave_id: str
    query_plan_hash: str
    planning_complete: bool
    required_inputs_available: bool
    execution_complete: bool
    ready: bool
    finalizable: bool
    recommended_status: ProductionWaveStatus
    issues: list[PreflightIssue]

    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )


class ProductionWavePreflightError(ValueError):
    def __init__(self, report: ProductionWavePreflight):
        self.report = report
        summary = "; ".join(f"{item.code}: {item.message}" for item in report.issues)
        super().__init__(summary or "production retrieval wave did not pass preflight")


@dataclass(frozen=True, slots=True)
class _SourceContract:
    role: str
    route: IdentificationRoute
    transport: RetrievalTransportKind
    adapter_id: str
    adapter_version: str
    strategy: str
    completion_proofs: tuple[str, ...]
    exact_total_required: bool
    required_parameter_keys: frozenset[str]
    maximum_supported_results: int | None = None
    artifact_kind: ArtifactKind | None = None


SOURCE_CONTRACTS: dict[str, _SourceContract] = {
    "PubMed": _SourceContract(
        "database_search", IdentificationRoute.DATABASE, RetrievalTransportKind.HTTP,
        "PubMedPaginator", PUBMED_PAGINATOR.version, PUBMED_PAGINATOR.strategy,
        ("pubmed_exact_id_manifest_fetched", "pubmed_exact_zero_count"), True,
        frozenset({"db", "retmode", "usehistory", "page_size"}),
        PUBMED_PAGINATOR.maximum_results,
    ),
    "EuropePMC": _SourceContract(
        "database_search", IdentificationRoute.DATABASE, RetrievalTransportKind.HTTP,
        "EuropePmcPaginator", EUROPE_PMC_PAGINATOR.version, EUROPE_PMC_PAGINATOR.strategy,
        ("europe_pmc_cursor_exhausted",), True,
        frozenset({"format", "resultType", "pageSize"}),
    ),
    "CrossRef": _SourceContract(
        "database_search", IdentificationRoute.DATABASE, RetrievalTransportKind.HTTP,
        "CrossrefPaginator", CROSSREF_PAGINATOR.version, CROSSREF_PAGINATOR.strategy,
        ("crossref_short_page",), True, frozenset({"rows"}),
    ),
    "SemanticScholar": _SourceContract(
        "database_search", IdentificationRoute.DATABASE, RetrievalTransportKind.HTTP,
        "SemanticScholarPaginator", SEMANTIC_SCHOLAR_PAGINATOR.version,
        SEMANTIC_SCHOLAR_PAGINATOR.strategy,
        (
            "semantic_scholar_relevance_next_exhausted",
            "semantic_scholar_bulk_token_exhausted",
        ),
        False, frozenset({"mode", "limit", "fields"}),
    ),
    "arXiv": _SourceContract(
        "database_search", IdentificationRoute.DATABASE, RetrievalTransportKind.HTTP,
        "ArxivPaginator", ARXIV_PAGINATOR.version, ARXIV_PAGINATOR.strategy,
        ("arxiv_exact_total_reached", "arxiv_short_page"), True,
        frozenset({"max_results", "sortBy", "sortOrder"}),
        ARXIV_PAGINATOR.maximum_results,
    ),
    "IEEEXplore": _SourceContract(
        "database_search", IdentificationRoute.DATABASE, RetrievalTransportKind.HTTP,
        "IeeeXplorePaginator", IEEE_XPLORE_PAGINATOR.version,
        IEEE_XPLORE_PAGINATOR.strategy, ("ieee_totalfound_reconciled",), True,
        frozenset(
            {"query_parameter", "format", "max_records", "sort_field", "sort_order"}
        ),
    ),
    "ACMDigitalLibrary": _SourceContract(
        "database_search", IdentificationRoute.DATABASE,
        RetrievalTransportKind.ARTIFACT_IMPORT, "AcmBibtexArtifactImporter", "1.0.0",
        "artifact_range", ("artifact_import_reconciled",), True,
        frozenset(
            {
                "field_selections", "collection_scope", "filters", "sort",
                "export_format", "ui_reported_total",
            }
        ),
        artifact_kind=ArtifactKind.ACM_EXPORT_MANIFEST,
    ),
    "PriorSurveySeed": _SourceContract(
        "prior_review", IdentificationRoute.PRIOR_SURVEY_SEED,
        RetrievalTransportKind.ARTIFACT_IMPORT, "PriorSurveySeedManifestImporter", "1.0.0",
        "artifact_range", ("artifact_import_reconciled",), True,
        frozenset({"seed_set_id", "seed_set_version", "expected_entry_count"}),
        artifact_kind=ArtifactKind.PRIOR_SURVEY_SEED_MANIFEST,
    ),
}


def compute_query_plan_hash(wave: ProductionRetrievalWave) -> str:
    payload = {
        "schema_version": wave.schema_version,
        "wave_id": wave.wave_id,
        "wave_version": wave.wave_version,
        "query_plan_version": wave.query_plan_version,
        "required_sources": wave.required_sources,
        "query_families": _serialize(wave.query_families),
    }
    if wave.schema_version == "1.1.0":
        payload["support_sources"] = wave.support_sources
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def save_production_wave(path: str | Path, wave: ProductionRetrievalWave) -> str:
    """Atomically persist canonical JSON and return the persisted content hash."""

    content = wave.to_json() + "\n"
    atomic_write(Path(path), content.encode("utf-8"))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load_production_wave(path: str | Path) -> ProductionRetrievalWave:
    return ProductionRetrievalWave.from_json(Path(path).read_text(encoding="utf-8"))


def preflight_production_wave(
    wave: ProductionRetrievalWave,
    *,
    manifest_root: str | Path,
    configured_credentials: Mapping[str, Iterable[str]] | None = None,
    execution_dataset: ReviewDataset | None = None,
) -> ProductionWavePreflight:
    """Validate planning, required inputs, and optional full-wave execution evidence."""

    root = Path(manifest_root)
    credentials = {
        source: set(names) for source, names in (configured_credentials or {}).items()
    }
    planning_issues: list[PreflightIssue] = []
    input_issues: list[PreflightIssue] = []
    execution_issues: list[PreflightIssue] = []
    artifact_validation_cache: dict[Path, dict[str, Any]] = {}

    _validate_wave_identity(wave, planning_issues)
    _validate_query_families(wave, planning_issues)
    for family in wave.query_families:
        _validate_credentials(family, credentials, input_issues)
        _validate_required_artifact(
            family, root, input_issues, artifact_validation_cache
        )

    planning_complete = not planning_issues
    required_inputs_available = not input_issues
    ready = planning_complete and required_inputs_available
    execution_complete = False
    if execution_dataset is not None:
        _validate_execution(wave, execution_dataset, execution_issues)
        execution_complete = not execution_issues
    finalizable = ready and execution_complete
    issues = [*planning_issues, *input_issues, *execution_issues]
    recommended = (
        ProductionWaveStatus.FINALIZED
        if finalizable
        else ProductionWaveStatus.READY
        if ready
        else ProductionWaveStatus.INCOMPLETE
    )
    report = ProductionWavePreflight(
        wave_id=wave.wave_id,
        query_plan_hash=compute_query_plan_hash(wave),
        planning_complete=planning_complete,
        required_inputs_available=required_inputs_available,
        execution_complete=execution_complete,
        ready=ready,
        finalizable=finalizable,
        recommended_status=recommended,
        issues=issues,
    )
    if wave.status is ProductionWaveStatus.READY and not ready:
        raise ProductionWavePreflightError(report)
    if wave.status is ProductionWaveStatus.FINALIZED and not finalizable:
        raise ProductionWavePreflightError(report)
    if wave.status is not ProductionWaveStatus.FINALIZED and wave.retrieval_cutoff_date:
        cutoff_issue = PreflightIssue(
            "PREMATURE_RETRIEVAL_CUTOFF",
            "only a fully executed and finalized wave may have a retrieval cutoff",
        )
        report = _with_issue(report, cutoff_issue)
        raise ProductionWavePreflightError(report)
    return report


def _validate_wave_identity(
    wave: ProductionRetrievalWave, issues: list[PreflightIssue]
) -> None:
    for name, value in (
        ("wave_id", wave.wave_id),
        ("wave_version", wave.wave_version),
        ("query_plan_version", wave.query_plan_version),
    ):
        if not value.strip():
            issues.append(PreflightIssue("MISSING_WAVE_IDENTITY", f"{name} is required"))
    if wave.schema_version not in {"1.0.0", "1.1.0"}:
        issues.append(
            PreflightIssue(
                "UNSUPPORTED_SCHEMA", "wave schema_version must be 1.0.0 or 1.1.0"
            )
        )
    execution_scope = wave.metadata.get("execution_scope")
    if execution_scope not in {None, EXTERNAL_RETRIEVAL_EXECUTION_SCOPE}:
        issues.append(
            PreflightIssue(
                "UNSUPPORTED_EXECUTION_SCOPE",
                f"unsupported execution scope {execution_scope!r}",
            )
        )
    external_scope = (
        wave.schema_version == "1.1.0"
        and execution_scope == EXTERNAL_RETRIEVAL_EXECUTION_SCOPE
    )
    expected_sources = (
        EXTERNAL_IDENTIFICATION_SOURCES_V2
        if external_scope
        else REQUIRED_IDENTIFICATION_SOURCES_V2
        if wave.schema_version == "1.1.0"
        else REQUIRED_PRODUCTION_SOURCES
    )
    if wave.required_sources != list(expected_sources):
        issues.append(
            PreflightIssue(
                "REQUIRED_SOURCE_LIST_MISMATCH",
                "required_sources must contain the exact ordered production source inventory",
            )
        )
    if external_scope:
        if wave.metadata.get("deferred_identification_sources") != [
            "PriorSurveySeed"
        ]:
            issues.append(
                PreflightIssue(
                    "EXTERNAL_SCOPE_DEFERRED_SOURCE_MISMATCH",
                    "external-retrieval scope must defer only PriorSurveySeed",
                )
            )
        if wave.metadata.get("identification_set_closure_allowed") is not False:
            issues.append(
                PreflightIssue(
                    "EXTERNAL_SCOPE_CLOSURE_GUARD_MISSING",
                    "external-retrieval scope must prohibit identification-set closure",
                )
            )
        if wave.status is ProductionWaveStatus.FINALIZED:
            issues.append(
                PreflightIssue(
                    "EXTERNAL_SCOPE_FINALIZATION_PROHIBITED",
                    "an external-only wave cannot represent full-wave finalization",
                )
            )
    expected_support = (
        list(REQUIRED_SUPPORT_SOURCES_V2) if wave.schema_version == "1.1.0" else []
    )
    if wave.support_sources != expected_support:
        issues.append(
            PreflightIssue(
                "SUPPORT_SOURCE_LIST_MISMATCH",
                "support_sources must match the schema-specific frozen support inventory",
            )
        )
    computed = compute_query_plan_hash(wave)
    if wave.query_plan_hash != computed:
        issues.append(
            PreflightIssue(
                "QUERY_PLAN_HASH_MISMATCH",
                f"declared query-plan hash {wave.query_plan_hash!r} does not match {computed}",
            )
        )


def _validate_query_families(
    wave: ProductionRetrievalWave, issues: list[PreflightIssue]
) -> None:
    identifiers = [family.query_family_id for family in wave.query_families]
    duplicates = sorted({item for item in identifiers if identifiers.count(item) > 1})
    if duplicates:
        issues.append(
            PreflightIssue(
                "DUPLICATE_QUERY_FAMILY_ID",
                f"query-family IDs must be unique: {duplicates}",
            )
        )
    covered = {family.source_database for family in wave.query_families}
    for source in wave.required_sources:
        if source not in covered:
            issues.append(
                PreflightIssue(
                    "MISSING_REQUIRED_SOURCE_QUERY",
                    f"required source {source} has no query family",
                )
            )
    if wave.schema_version == "1.1.0" and any(
        family.source_database == "CrossRef" for family in wave.query_families
    ):
        issues.append(
            PreflightIssue(
                "CROSSREF_IDENTIFICATION_PROHIBITED",
                "Crossref is support-only and cannot have a production identification query",
            )
        )
    for family in wave.query_families:
        _validate_query_family(family, issues)


def _validate_query_family(
    family: ProductionQueryFamily, issues: list[PreflightIssue]
) -> None:
    family_id = family.query_family_id
    contract = SOURCE_CONTRACTS.get(family.source_database)
    if contract is None:
        issues.append(
            PreflightIssue(
                "UNSUPPORTED_SOURCE", f"unsupported source {family.source_database}", family_id
            )
        )
        return
    for name, value in (
        ("query_family_id", family.query_family_id),
        ("query_version", family.query_version),
        ("query_text", family.query_text),
    ):
        if not value.strip():
            issues.append(
                PreflightIssue("MISSING_QUERY_PROVENANCE", f"{name} is required", family_id)
            )
    comparisons = (
        ("SOURCE_ROLE_MISMATCH", "source_role", family.source_role, contract.role),
        (
            "IDENTIFICATION_ROUTE_MISMATCH",
            "identification_route",
            family.identification_route,
            contract.route,
        ),
        (
            "TRANSPORT_KIND_MISMATCH",
            "transport_kind",
            family.transport_kind,
            contract.transport,
        ),
        ("ADAPTER_ID_MISMATCH", "adapter_id", family.adapter_id, contract.adapter_id),
        (
            "ADAPTER_VERSION_MISMATCH",
            "adapter_version",
            family.adapter_version,
            contract.adapter_version,
        ),
        (
            "PAGINATION_STRATEGY_MISMATCH",
            "pagination.strategy",
            family.pagination.strategy,
            contract.strategy,
        ),
        (
            "PAGINATION_VERSION_MISMATCH",
            "pagination.adapter_version",
            family.pagination.adapter_version,
            contract.adapter_version,
        ),
    )
    for code, name, actual, expected in comparisons:
        if actual != expected:
            issues.append(
                PreflightIssue(code, f"{name}={actual!r}, expected {expected!r}", family_id)
            )
    expected_proofs = contract.completion_proofs
    expected_exact_total = contract.exact_total_required
    expected_maximum = contract.maximum_supported_results
    if family.source_database == "SemanticScholar":
        mode = family.native_parameters.get("mode")
        if mode == "relevance":
            expected_proofs = ("semantic_scholar_relevance_next_exhausted",)
            expected_exact_total = True
            expected_maximum = SEMANTIC_SCHOLAR_PAGINATOR.relevance_window
        elif mode == "bulk":
            expected_proofs = ("semantic_scholar_bulk_token_exhausted",)
            expected_exact_total = False
            expected_maximum = None
    if set(family.pagination.completion_proofs) != set(expected_proofs):
        issues.append(
            PreflightIssue(
                "COMPLETION_PROOF_MISMATCH",
                "declared completion proofs do not match the installed adapter contract",
                family_id,
            )
        )
    if family.pagination.exact_total_required != expected_exact_total:
        issues.append(
            PreflightIssue(
                "TOTAL_SEMANTICS_MISMATCH",
                "declared exact-total requirement does not match the source contract",
                family_id,
            )
        )
    if family.pagination.maximum_supported_results != expected_maximum:
        issues.append(
            PreflightIssue(
                "RESULT_WINDOW_MISMATCH",
                "declared supported result window does not match the adapter",
                family_id,
            )
        )
    missing_parameters = sorted(
        contract.required_parameter_keys - family.native_parameters.keys()
    )
    if missing_parameters:
        issues.append(
            PreflightIssue(
                "MISSING_SOURCE_PARAMETERS",
                f"missing frozen native parameters: {missing_parameters}",
                family_id,
            )
        )
    _validate_native_parameter_values(family, issues)
    if family.source_database == "SemanticScholar":
        _validate_semantic_scholar(family, issues)
    if family.source_database == "IEEEXplore":
        _validate_ieee(family, issues)
    if family.result_window_status is ResultWindowStatus.UNRESOLVED:
        issues.append(
            PreflightIssue(
                "UNRESOLVED_RESULT_WINDOW",
                family.unresolved_requirement
                or "query requires an unapproved partition/truncation resolution",
                family_id,
            )
        )
    if family.unresolved_requirement and family.result_window_status is not ResultWindowStatus.UNRESOLVED:
        issues.append(
            PreflightIssue(
                "INCONSISTENT_WINDOW_DECLARATION",
                "unresolved_requirement requires result_window_status=unresolved",
                family_id,
            )
        )
    if contract.artifact_kind is None and family.required_artifact is not None:
        issues.append(
            PreflightIssue(
                "UNEXPECTED_REQUIRED_ARTIFACT",
                "HTTP query families cannot declare an import artifact",
                family_id,
            )
        )
    if contract.artifact_kind is not None and (
        family.required_artifact is None
        or family.required_artifact.kind is not contract.artifact_kind
    ):
        issues.append(
            PreflightIssue(
                "MISSING_REQUIRED_ARTIFACT_DECLARATION",
                f"source requires {contract.artifact_kind.value}",
                family_id,
            )
        )


def _validate_semantic_scholar(
    family: ProductionQueryFamily, issues: list[PreflightIssue]
) -> None:
    mode = family.native_parameters.get("mode")
    if mode not in {"relevance", "bulk"}:
        issues.append(
            PreflightIssue(
                "SEMANTIC_SCHOLAR_MODE_MISSING",
                "Semantic Scholar mode must be frozen as relevance or bulk",
                family.query_family_id,
            )
        )
        return
    expected = (
        "semantic_scholar_relevance_next_exhausted"
        if mode == "relevance"
        else "semantic_scholar_bulk_token_exhausted"
    )
    if expected not in family.pagination.completion_proofs:
        issues.append(
            PreflightIssue(
                "SEMANTIC_SCHOLAR_MODE_PROOF_MISMATCH",
                f"mode {mode} requires completion proof {expected}",
                family.query_family_id,
            )
        )


def _validate_native_parameter_values(
    family: ProductionQueryFamily, issues: list[PreflightIssue]
) -> None:
    family_id = family.query_family_id
    parameters = family.native_parameters
    required = SOURCE_CONTRACTS[family.source_database].required_parameter_keys
    empty = sorted(
        key
        for key in required
        if parameters.get(key) is None
        or parameters.get(key) == ""
        or parameters.get(key) == []
    )
    if empty:
        issues.append(
            PreflightIssue(
                "INCOMPLETE_SOURCE_PARAMETERS",
                f"required native parameters have empty values: {empty}",
                family_id,
            )
        )
    limits = {
        "PubMed": ("page_size", 10_000),
        "EuropePMC": ("pageSize", 1000),
        "CrossRef": ("rows", 1000),
        "arXiv": ("max_results", 2000),
    }
    if family.source_database in limits:
        key, maximum = limits[family.source_database]
        try:
            value = int(parameters.get(key))
        except (TypeError, ValueError):
            value = 0
        if not 1 <= value <= maximum:
            issues.append(
                PreflightIssue(
                    "SOURCE_PAGE_SIZE_INVALID",
                    f"{key} must be between 1 and {maximum}",
                    family_id,
                )
            )
    if family.source_database == "SemanticScholar":
        maximum = 100 if parameters.get("mode") == "relevance" else 1000
        try:
            limit = int(parameters.get("limit"))
        except (TypeError, ValueError):
            limit = 0
        if not 1 <= limit <= maximum:
            issues.append(
                PreflightIssue(
                    "SOURCE_PAGE_SIZE_INVALID",
                    f"Semantic Scholar limit must be between 1 and {maximum} for the frozen mode",
                    family_id,
                )
            )
    if family.source_database == "ACMDigitalLibrary":
        if parameters.get("collection_scope") not in {"acm_publications", "acm_guide"}:
            issues.append(
                PreflightIssue(
                    "ACM_SCOPE_INVALID", "ACM collection scope is not frozen", family_id
                )
            )
        if str(parameters.get("export_format") or "").lower() != "bibtex":
            issues.append(
                PreflightIssue(
                    "ACM_EXPORT_FORMAT_INVALID", "ACM export format must be BibTeX", family_id
                )
            )
        try:
            total = int(parameters.get("ui_reported_total"))
        except (TypeError, ValueError):
            total = -1
        if total < 0:
            issues.append(
                PreflightIssue(
                    "ACM_UI_TOTAL_INVALID", "ACM UI total must be non-negative", family_id
                )
            )
    if family.source_database == "PriorSurveySeed":
        try:
            total = int(parameters.get("expected_entry_count"))
        except (TypeError, ValueError):
            total = 0
        if total < 1:
            issues.append(
                PreflightIssue(
                    "SEED_ENTRY_COUNT_INVALID",
                    "seed manifests require at least one explicit entry",
                    family_id,
                )
            )


def _validate_ieee(
    family: ProductionQueryFamily, issues: list[PreflightIssue]
) -> None:
    parameters = family.native_parameters
    if parameters.get("query_parameter") not in {
        "querytext", "meta_data", "article_title", "abstract"
    }:
        issues.append(
            PreflightIssue(
                "IEEE_QUERY_PARAMETER_INVALID",
                "IEEE query_parameter must be explicitly supported",
                family.query_family_id,
            )
        )
    try:
        max_records = int(parameters.get("max_records"))
    except (TypeError, ValueError):
        max_records = 0
    if not 1 <= max_records <= 200:
        issues.append(
            PreflightIssue(
                "IEEE_PAGE_SIZE_INVALID",
                "IEEE max_records must be between 1 and 200",
                family.query_family_id,
            )
        )
    if family.content_policy.get("abstract") != ABSTRACT_CONTENT_POLICY:
        issues.append(
            PreflightIssue(
                "IEEE_CONTENT_POLICY_MISSING",
                f"IEEE abstract policy must be {ABSTRACT_CONTENT_POLICY}",
                family.query_family_id,
            )
        )
    if "api_key" not in family.required_credentials:
        issues.append(
            PreflightIssue(
                "IEEE_CREDENTIAL_REQUIREMENT_MISSING",
                "IEEE query must declare api_key as an external credential requirement",
                family.query_family_id,
            )
        )


def _validate_credentials(
    family: ProductionQueryFamily,
    credentials: Mapping[str, set[str]],
    issues: list[PreflightIssue],
) -> None:
    available = credentials.get(family.source_database, set())
    missing = sorted(set(family.required_credentials) - available)
    if missing:
        issues.append(
            PreflightIssue(
                "MISSING_CONFIGURED_CREDENTIAL",
                f"configured credential names are missing: {missing}",
                family.query_family_id,
            )
        )


def _validate_required_artifact(
    family: ProductionQueryFamily,
    root: Path,
    issues: list[PreflightIssue],
    validation_cache: dict[Path, dict[str, Any]],
) -> None:
    required = family.required_artifact
    if required is None:
        return
    try:
        relative = _safe_relative_path(required.manifest_path)
    except ValueError as exc:
        issues.append(
            PreflightIssue("INVALID_ARTIFACT_PATH", str(exc), family.query_family_id)
        )
        return
    path = root / relative
    try:
        raw = path.read_bytes()
    except OSError as exc:
        issues.append(
            PreflightIssue(
                "MISSING_REQUIRED_ARTIFACT",
                f"{relative}: {type(exc).__name__}: {exc}",
                family.query_family_id,
            )
        )
        return
    actual_hash = hashlib.sha256(raw).hexdigest()
    if actual_hash != required.manifest_sha256:
        issues.append(
            PreflightIssue(
                "ARTIFACT_HASH_MISMATCH",
                f"{relative} hash does not match the frozen plan",
                family.query_family_id,
            )
        )
        return
    try:
        payload = json.loads(raw)
        if required.kind is ArtifactKind.ACM_EXPORT_MANIFEST:
            _validate_acm_artifact(
                family,
                required,
                path,
                payload,
                root,
                issues,
                validation_cache,
            )
        else:
            _validate_seed_artifact(family, required, path, payload, issues)
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        issues.append(
            PreflightIssue(
                "INVALID_REQUIRED_ARTIFACT",
                f"{relative}: {type(exc).__name__}: {exc}",
                family.query_family_id,
            )
        )


def _validate_acm_artifact(
    family: ProductionQueryFamily,
    required: RequiredArtifact,
    path: Path,
    payload: dict[str, Any],
    root: Path,
    issues: list[PreflightIssue],
    validation_cache: dict[Path, dict[str, Any]],
) -> None:
    if payload.get("manifest_id") == (
        "star-acm-field-execution-2026-09-03-final-reconciliation"
    ):
        _validate_acm_final_reconciliation_artifact(
            family, required, path, root, issues, validation_cache
        )
        return
    dataset = import_acm_bibtex_manifest(path)
    if dataset.retrieval_runs[0].completion_status is not RetrievalCompletionStatus.COMPLETE:
        raise ValueError("ACM import does not satisfy artifact completion checks")
    comparisons = {
        "query_text": family.query_text,
        "query_version": family.query_version,
        "ui_reported_total": required.expected_total,
        "field_selections": family.native_parameters.get("field_selections"),
        "collection_scope": family.native_parameters.get("collection_scope"),
        "filters": family.native_parameters.get("filters"),
        "sort": family.native_parameters.get("sort"),
        "export_format": family.native_parameters.get("export_format"),
    }
    for key, expected in comparisons.items():
        if payload.get(key) != expected:
            issues.append(
                PreflightIssue(
                    "ACM_ARTIFACT_PLAN_MISMATCH",
                    f"ACM manifest {key} does not match the frozen query family",
                    family.query_family_id,
                )
            )
    actual_chunks = [
        {
            key: item[key]
            for key in (
                "chunk_id", "first_record", "last_record", "artifact_path", "sha256"
            )
        }
        for item in payload.get("chunks", [])
    ]
    if actual_chunks != required.expected_chunks:
        issues.append(
            PreflightIssue(
                "ACM_CHUNK_PLAN_MISMATCH",
                "ACM export chunks do not match the frozen ranges and hashes",
                family.query_family_id,
            )
        )


def _validate_acm_final_reconciliation_artifact(
    family: ProductionQueryFamily,
    required: RequiredArtifact,
    path: Path,
    root: Path,
    issues: list[PreflightIssue],
    validation_cache: dict[Path, dict[str, Any]],
) -> None:
    # Import lazily to avoid the production-query-plan/production-wave import cycle.
    from h2h_lit.acm_field_execution import (
        load_acm_final_reconciliation_manifest,
    )

    manifest = validation_cache.get(path)
    if manifest is None:
        manifest = load_acm_final_reconciliation_manifest(
            path, root=root, verify_artifacts=True
        )
        validation_cache[path] = manifest
    matches = [
        item
        for item in manifest.get("families", [])
        if item.get("parent_query_id") == family.query_family_id
    ]
    if len(matches) != 1:
        issues.append(
            PreflightIssue(
                "ACM_ARTIFACT_PLAN_MISMATCH",
                "ACM reconciliation must contain exactly one matching parent query",
                family.query_family_id,
            )
        )
        return
    reconciled_family = matches[0]
    field_union = reconciled_family.get("field_union", {})
    if (
        field_union.get("state") != "COMPLETE_SET_RECONCILED_NOT_IMPORTED"
        or field_union.get("unique_stable_identity_count") != required.expected_total
    ):
        issues.append(
            PreflightIssue(
                "ACM_ARTIFACT_TOTAL_MISMATCH",
                "ACM field union is incomplete or differs from the frozen expectation",
                family.query_family_id,
            )
        )
    if {item.get("field_key") for item in reconciled_family.get("children", [])} != {
        "title",
        "keyword",
        "abstract",
    }:
        issues.append(
            PreflightIssue(
                "ACM_ARTIFACT_FIELD_MISMATCH",
                "ACM reconciliation must contain title, keyword, and abstract children",
                family.query_family_id,
            )
        )
    if required.expected_chunks:
        issues.append(
            PreflightIssue(
                "ACM_CHUNK_PLAN_MISMATCH",
                "field-decomposed ACM reconciliation binds selected artifacts internally",
                family.query_family_id,
            )
        )
    expected_parameters = {
        "field_selections": ["Title", "Abstract", "Author Keywords"],
        "collection_scope": "acm_publications",
        "filters": {},
        "sort": "publicationDate asc",
        "export_format": "BibTeX",
        "ui_reported_total": required.expected_total,
    }
    for key, expected in expected_parameters.items():
        if family.native_parameters.get(key) != expected:
            issues.append(
                PreflightIssue(
                    "ACM_ARTIFACT_PLAN_MISMATCH",
                    f"ACM wave parameter {key} does not match reconciled evidence",
                    family.query_family_id,
                )
            )


def _validate_seed_artifact(
    family: ProductionQueryFamily,
    required: RequiredArtifact,
    path: Path,
    payload: dict[str, Any],
    issues: list[PreflightIssue],
) -> None:
    dataset = import_seed_manifest(path)
    if dataset.retrieval_runs[0].completion_status is not RetrievalCompletionStatus.COMPLETE:
        raise ValueError("seed import does not satisfy manifest completion checks")
    comparisons = {
        "seed_set_id": required.seed_set_id,
        "seed_set_version": required.seed_set_version,
        "expected_entry_count": required.expected_total,
    }
    for key, expected in comparisons.items():
        if payload.get(key) != expected or family.native_parameters.get(key) != expected:
            issues.append(
                PreflightIssue(
                    "SEED_MANIFEST_PLAN_MISMATCH",
                    f"seed manifest {key} does not match the frozen query family",
                    family.query_family_id,
                )
            )


def _validate_execution(
    wave: ProductionRetrievalWave,
    dataset: ReviewDataset,
    issues: list[PreflightIssue],
) -> None:
    try:
        dataset.validate()
    except ValueError as exc:
        issues.append(PreflightIssue("INVALID_EXECUTION_DATASET", str(exc)))
        return
    matching_runs = [run for run in dataset.retrieval_runs if run.run_id == wave.wave_id]
    if len(matching_runs) != 1:
        issues.append(
            PreflightIssue(
                "MISSING_FULL_WAVE_RUN",
                "execution dataset must contain exactly one run matching wave_id",
            )
        )
        return
    run = matching_runs[0]
    if run.query_plan_hash != wave.query_plan_hash:
        issues.append(
            PreflightIssue(
                "EXECUTION_PLAN_HASH_MISMATCH",
                "executed retrieval run query-plan hash differs from the frozen wave",
            )
        )
    if (
        run.status is not ProcessingStatus.OK
        or run.completion_status is not RetrievalCompletionStatus.COMPLETE
        or not run.retrieval_cutoff_date
    ):
        issues.append(
            PreflightIssue(
                "EXECUTION_WAVE_INCOMPLETE",
                "full wave run is not complete or has no successful-wave cutoff",
            )
        )
    families = {item.query_family_id: item for item in wave.query_families}
    queries_by_family: dict[str, list[Any]] = {}
    for query in dataset.source_queries:
        if query.run_id != run.run_id:
            continue
        family_id = query.metadata.get("query_family_id")
        if family_id:
            queries_by_family.setdefault(str(family_id), []).append(query)
        else:
            issues.append(
                PreflightIssue(
                    "UNMAPPED_EXECUTED_QUERY",
                    f"executed query {query.query_id} lacks query_family_id provenance",
                )
            )
    for family_id, family in families.items():
        queries = queries_by_family.get(family_id, [])
        if len(queries) != 1:
            issues.append(
                PreflightIssue(
                    "EXECUTION_QUERY_FAMILY_MISSING",
                    "execution must contain exactly one query for the frozen family",
                    family_id,
                )
            )
            continue
        query = queries[0]
        if (
            query.source_database != family.source_database
            or query.identification_route is not family.identification_route
        ):
            issues.append(
                PreflightIssue(
                    "EXECUTION_SOURCE_PROVENANCE_MISMATCH",
                    "executed source/identification route differs from the frozen family",
                    family_id,
                )
            )
        if (
            query.status is not ProcessingStatus.OK
            or query.completion_status is not RetrievalCompletionStatus.COMPLETE
            or query.completion_proof not in family.pagination.completion_proofs
        ):
            issues.append(
                PreflightIssue(
                    "EXECUTION_COMPLETION_PROOF_MISSING",
                    "query did not complete with an approved source-specific proof",
                    family_id,
                )
            )
        if family.pagination.exact_total_required and not query.total_is_exact:
            issues.append(
                PreflightIssue(
                    "EXECUTION_EXACT_TOTAL_MISSING",
                    "query requires an exact source total",
                    family_id,
                )
            )
        if family.required_artifact is not None and (
            query.result_count != family.required_artifact.expected_total
            or query.source_reported_total != family.required_artifact.expected_total
        ):
            issues.append(
                PreflightIssue(
                    "EXECUTION_ARTIFACT_TOTAL_MISMATCH",
                    "artifact-import result and source totals must match the frozen expectation",
                    family_id,
                )
            )
        pages = [page for page in dataset.retrieval_pages if page.source_query_id == query.query_id]
        if any(page.truncated for page in pages):
            issues.append(
                PreflightIssue(
                    "EXECUTION_TRUNCATED", "executed query contains a truncated page", family_id
                )
            )
        page_ids = {page.page_id for page in pages}
        attempts = [
            attempt for attempt in dataset.retrieval_attempts if attempt.page_id in page_ids
        ]
        if any(attempt.transport_kind is not family.transport_kind for attempt in attempts):
            issues.append(
                PreflightIssue(
                    "EXECUTION_TRANSPORT_MISMATCH",
                    "executed attempts use a transport different from the frozen family",
                    family_id,
                )
            )
    if set(queries_by_family) - set(families):
        issues.append(
            PreflightIssue(
                "UNPLANNED_EXECUTED_QUERY_FAMILY",
                "execution dataset contains query families absent from the frozen wave",
            )
        )
    if wave.status is ProductionWaveStatus.FINALIZED and (
        wave.retrieval_cutoff_date != run.retrieval_cutoff_date
    ):
        issues.append(
            PreflightIssue(
                "FINALIZED_CUTOFF_MISMATCH",
                "manifest cutoff must equal the completed full-wave run cutoff",
            )
        )


def _safe_relative_path(value: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not value.strip():
        raise ValueError("artifact manifest paths must be non-empty relative paths")
    return path.as_posix()


def _serialize(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {item.name: _serialize(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    return value


def _with_issue(
    report: ProductionWavePreflight, issue: PreflightIssue
) -> ProductionWavePreflight:
    return ProductionWavePreflight(
        wave_id=report.wave_id,
        query_plan_hash=report.query_plan_hash,
        planning_complete=False,
        required_inputs_available=report.required_inputs_available,
        execution_complete=report.execution_complete,
        ready=False,
        finalizable=False,
        recommended_status=ProductionWaveStatus.INCOMPLETE,
        issues=[*report.issues, issue],
    )
