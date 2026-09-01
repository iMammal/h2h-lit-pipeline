"""Offline candidate-query rendering and non-production sizing provenance."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from h2h_lit.checkpoint import atomic_write

CANDIDATE_SCHEMA_VERSION = "0.1.0"
SIZING_RUN_SCHEMA_VERSION = "1.0.0"
SENTINEL_SET_SCHEMA_VERSION = "1.0.0"
EXPECTED_FAMILIES = (
    "STAR-QF01-RELATIONAL-VIS",
    "STAR-QF02-ASSISTED-VIS",
    "STAR-QF03-INTERACTIVE-SYSTEMS",
    "STAR-QF04-NONDESKTOP-ENV",
    "STAR-QF05-CONVERSATIONAL",
)
SENTINEL_PURPOSE = "syntax_and_recall_diagnostics_only"
SENTINEL_ACCURACY_INTERPRETATION = "prohibited"

_FORBIDDEN_SIZING_KEYS = {
    "corpus_membership",
    "e6",
    "occurrences",
    "prisma",
    "record_occurrences",
    "records",
    "retrieval_cutoff",
    "retrieval_cutoff_date",
    "returned_records",
}
_FORBIDDEN_SENTINEL_KEYS = {
    "accuracy_label",
    "classification",
    "eligibility",
    "expected_decision",
    "gold_label",
}


class CandidateConfigurationError(ValueError):
    """Raised when a candidate artifact violates the approved architecture."""


class SizingSyntaxStatus(str, Enum):
    UNTESTED = "untested"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    AMBIGUOUS = "ambiguous"
    WARNING = "warning"


class SizingWindowStatus(str, Enum):
    UNKNOWN = "unknown"
    CLEAR = "clear"
    OVERFLOW = "overflow"


class SizingCountKind(str, Enum):
    EXACT = "exact"
    ESTIMATED = "estimated"
    UI_REPORTED = "ui_reported"
    UNAVAILABLE = "unavailable"


class SizingRunStatus(str, Enum):
    PLANNED = "planned"
    RUNNING = "running"
    COMPLETED = "completed"
    INVALID = "invalid"


@dataclass(frozen=True, slots=True)
class CandidateQuery:
    candidate_query_id: str
    candidate_set_id: str
    candidate_set_version: str
    candidate_set_hash: str
    family_id: str
    family_role: str
    variant_id: str
    leading_candidate: bool
    source: str
    source_role: str
    query_text: str
    query_hash: str
    field_restrictions: list[str]
    sizing_request: dict[str, Any]
    syntax_uncertainties: list[str]
    hard_window: int | None
    count_kind: SizingCountKind
    methodological_status: str = "candidate_not_production_frozen"
    creates_production_occurrences: bool = False
    establishes_retrieval_cutoff: bool = False

    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)


@dataclass(slots=True)
class CandidateSet:
    payload: dict[str, Any]

    def validate(self) -> None:
        data = self.payload
        if data.get("schema_version") != CANDIDATE_SCHEMA_VERSION:
            raise CandidateConfigurationError("unsupported candidate-query schema version")
        if data.get("methodological_status") != "candidate_not_production_frozen":
            raise CandidateConfigurationError("candidate queries cannot be production-frozen")
        if data.get("no_partitioning_authorized") is not True:
            raise CandidateConfigurationError("query partitioning must remain unauthorized")
        if tuple(data.get("families", {})) != EXPECTED_FAMILIES:
            raise CandidateConfigurationError("candidate set must contain the approved five families")
        if any(value is not None for value in data.get("retrieval_filters", {}).values()):
            raise CandidateConfigurationError("candidate retrieval filters must all be unset")
        self._validate_blocks()
        self._validate_anchors()
        self._validate_families()
        self._validate_sources()

    def _validate_blocks(self) -> None:
        blocks = self.payload.get("blocks", {})
        if set(blocks) != {"L", "R", "V", "A", "S", "M", "C"}:
            raise CandidateConfigurationError("term blocks must be exactly L/R/V/A/S/M/C")
        required = {
            "label",
            "high_specificity",
            "recall_expansion",
            "dangerously_generic",
            "loss_if_removed",
            "noise_if_unanchored",
        }
        for block_id, block in blocks.items():
            missing = required - set(block)
            if missing:
                raise CandidateConfigurationError(
                    f"block {block_id} is missing required fields: {sorted(missing)}"
                )
            terms = [*block["high_specificity"], *block["recall_expansion"]]
            if not terms or len(terms) != len(set(terms)):
                raise CandidateConfigurationError(f"block {block_id} terms must be unique")

    def _validate_anchors(self) -> None:
        anchors = self.payload.get("anchors", {})
        if "X" in anchors or "ASSISTANCE_CONTEXT" not in anchors:
            raise CandidateConfigurationError(
                "the assistance anchor must be named ASSISTANCE_CONTEXT, not X"
            )
        for name, anchor in anchors.items():
            if anchor.get("purpose") != "retrieval_context_only":
                raise CandidateConfigurationError(f"anchor {name} is not retrieval-only")
            if not anchor.get("terms"):
                raise CandidateConfigurationError(f"anchor {name} has no terms")
        prohibited = set(anchors["ASSISTANCE_CONTEXT"].get("prohibited_interpretations", []))
        if prohibited != {"eligibility_evidence", "classification_evidence"}:
            raise CandidateConfigurationError(
                "ASSISTANCE_CONTEXT must prohibit eligibility/classification evidence semantics"
            )

    def _validate_families(self) -> None:
        families = self.payload["families"]
        if set(families[EXPECTED_FAMILIES[0]]["variants"]) != {"anchored", "unanchored"}:
            raise CandidateConfigurationError("QF01 requires anchored/unanchored comparators")
        qf02 = families[EXPECTED_FAMILIES[1]]
        if set(qf02["variants"]) != {"A", "B", "C", "D"}:
            raise CandidateConfigurationError("QF02 requires A/B/C/D sizing comparators")
        if qf02.get("leading_candidate") != "D":
            raise CandidateConfigurationError("QF02-D must remain the leading candidate")
        templates = " ".join(
            template
            for family in families.values()
            for template in family.get("variants", {}).values()
        )
        if "{X}" in templates:
            raise CandidateConfigurationError("family templates cannot use the retired X name")

    def _validate_sources(self) -> None:
        sources = self.payload.get("sources", {})
        expected = {
            "PubMed",
            "EuropePMC",
            "SemanticScholar",
            "arXiv",
            "IEEEXplore",
            "ACMDigitalLibrary",
            "CrossRef",
        }
        if set(sources) != expected:
            raise CandidateConfigurationError("candidate source registry is incomplete")
        if sources["SemanticScholar"].get("pagination_mode") != "bulk":
            raise CandidateConfigurationError("Semantic Scholar candidate mode must be bulk")
        if sources["SemanticScholar"].get("validation_gate") != "bulk_boolean_semantics":
            raise CandidateConfigurationError("Semantic Scholar Boolean validation gate is required")
        crossref = sources["CrossRef"]
        if "provisional" not in crossref.get("role", ""):
            raise CandidateConfigurationError("Crossref identification must remain provisional")
        if EXPECTED_FAMILIES[2] in crossref.get("families", []):
            raise CandidateConfigurationError("Crossref cannot execute QF03")
        for source, spec in sources.items():
            if not set(spec.get("families", [])).issubset(EXPECTED_FAMILIES):
                raise CandidateConfigurationError(f"source {source} references an unknown family")

    @property
    def candidate_set_id(self) -> str:
        return str(self.payload["candidate_set_id"])

    @property
    def candidate_set_version(self) -> str:
        return str(self.payload["candidate_set_version"])

    def canonical_json(self) -> str:
        self.validate()
        return _canonical_json(self.payload)

    def candidate_set_hash(self) -> str:
        return _sha256(self.canonical_json())

    def render_all(self) -> list[CandidateQuery]:
        self.validate()
        expansions = _expansions(self.payload)
        output: list[CandidateQuery] = []
        families = self.payload["families"]
        for source, source_spec in self.payload["sources"].items():
            for family_id, family in families.items():
                if family_id not in source_spec["families"]:
                    continue
                for variant_id, template in family["variants"].items():
                    expression = _expand_template(template, expansions)
                    output.append(
                        _render_source_query(
                            self,
                            source,
                            source_spec,
                            family_id,
                            family,
                            variant_id,
                            expression,
                        )
                    )
        return output


@dataclass(frozen=True, slots=True)
class SizingObservation:
    observation_id: str
    candidate_query_id: str
    query_hash: str
    source: str
    observed_at: str
    request: dict[str, Any]
    request_hash: str
    response_hash: str | None
    reported_count: int | None
    count_kind: SizingCountKind
    hard_window: int | None
    window_status: SizingWindowStatus
    syntax_status: SizingSyntaxStatus
    source_query_translation: str | None = None
    warnings: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.reported_count is not None and self.reported_count < 0:
            raise ValueError("reported sizing count cannot be negative")
        if self.hard_window is not None and self.hard_window < 1:
            raise ValueError("hard sizing window must be positive")
        expected = SizingWindowStatus.UNKNOWN
        if self.reported_count is not None and self.hard_window is not None:
            expected = (
                SizingWindowStatus.OVERFLOW
                if self.reported_count > self.hard_window
                else SizingWindowStatus.CLEAR
            )
        if self.window_status is not expected:
            raise ValueError(
                f"window status {self.window_status.value} does not match count/window evidence"
            )
        _reject_forbidden_keys(self.request, _FORBIDDEN_SIZING_KEYS)
        _reject_secrets(self.request)
        if _sha256(_canonical_json(self.request)) != self.request_hash:
            raise ValueError("sizing request hash does not match canonical request")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SizingObservation:
        return cls(
            observation_id=str(data["observation_id"]),
            candidate_query_id=str(data["candidate_query_id"]),
            query_hash=str(data["query_hash"]),
            source=str(data["source"]),
            observed_at=str(data["observed_at"]),
            request=dict(data["request"]),
            request_hash=str(data["request_hash"]),
            response_hash=data.get("response_hash"),
            reported_count=data.get("reported_count"),
            count_kind=SizingCountKind(data["count_kind"]),
            hard_window=data.get("hard_window"),
            window_status=SizingWindowStatus(data["window_status"]),
            syntax_status=SizingSyntaxStatus(data["syntax_status"]),
            source_query_translation=data.get("source_query_translation"),
            warnings=list(data.get("warnings", [])),
        )


@dataclass(slots=True)
class QuerySizingRun:
    schema_version: str
    sizing_run_id: str
    candidate_set_id: str
    candidate_set_version: str
    candidate_set_hash: str
    status: SizingRunStatus
    planned_candidate_query_ids: list[str]
    created_at: str
    observations: list[SizingObservation] = field(default_factory=list)
    completed_at: str | None = None
    purpose: str = "count_and_syntax_sizing_only"
    creates_production_occurrences: bool = False
    contributes_prisma_counts: bool = False
    establishes_retrieval_cutoff: bool = False
    derives_e6: bool = False

    def validate(self) -> None:
        if self.schema_version != SIZING_RUN_SCHEMA_VERSION:
            raise ValueError("unsupported query-sizing schema version")
        if self.purpose != "count_and_syntax_sizing_only":
            raise ValueError("query sizing has an invalid purpose")
        if any(
            (
                self.creates_production_occurrences,
                self.contributes_prisma_counts,
                self.establishes_retrieval_cutoff,
                self.derives_e6,
            )
        ):
            raise ValueError("query sizing cannot affect production review state")
        if len(self.planned_candidate_query_ids) != len(set(self.planned_candidate_query_ids)):
            raise ValueError("planned sizing candidate IDs must be unique")
        observation_ids = [item.observation_id for item in self.observations]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("sizing observation IDs must be unique")
        by_candidate = [item.candidate_query_id for item in self.observations]
        if len(by_candidate) != len(set(by_candidate)):
            raise ValueError("a sizing run permits one observation per candidate query")
        unknown = set(by_candidate) - set(self.planned_candidate_query_ids)
        if unknown:
            raise ValueError(f"observations reference unplanned candidate queries: {sorted(unknown)}")
        for observation in self.observations:
            observation.validate()
        if self.status is SizingRunStatus.COMPLETED:
            if set(by_candidate) != set(self.planned_candidate_query_ids):
                raise ValueError("completed sizing run does not account for every planned query")
            if not self.completed_at:
                raise ValueError("completed sizing run requires completed_at")
        _reject_forbidden_keys(_serialize(self), _FORBIDDEN_SIZING_KEYS)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    def run_hash(self) -> str:
        return _sha256(self.to_json())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QuerySizingRun:
        return cls(
            schema_version=str(data["schema_version"]),
            sizing_run_id=str(data["sizing_run_id"]),
            candidate_set_id=str(data["candidate_set_id"]),
            candidate_set_version=str(data["candidate_set_version"]),
            candidate_set_hash=str(data["candidate_set_hash"]),
            status=SizingRunStatus(data["status"]),
            planned_candidate_query_ids=list(data["planned_candidate_query_ids"]),
            created_at=str(data["created_at"]),
            observations=[SizingObservation.from_dict(item) for item in data["observations"]],
            completed_at=data.get("completed_at"),
            purpose=str(data.get("purpose", "count_and_syntax_sizing_only")),
            creates_production_occurrences=bool(data.get("creates_production_occurrences", False)),
            contributes_prisma_counts=bool(data.get("contributes_prisma_counts", False)),
            establishes_retrieval_cutoff=bool(data.get("establishes_retrieval_cutoff", False)),
            derives_e6=bool(data.get("derives_e6", False)),
        )

    @classmethod
    def from_json(cls, text: str) -> QuerySizingRun:
        return cls.from_dict(json.loads(text))


@dataclass(frozen=True, slots=True)
class SentinelPaper:
    sentinel_id: str
    title: str
    diagnostic_family_ids: list[str]
    doi: str | None = None
    source_identifier: str | None = None


@dataclass(slots=True)
class SentinelPaperSet:
    schema_version: str
    sentinel_set_id: str
    sentinel_set_version: str
    candidate_set_hash: str
    frozen_at: str
    entries: list[SentinelPaper]
    purpose: str = SENTINEL_PURPOSE
    accuracy_interpretation: str = SENTINEL_ACCURACY_INTERPRETATION

    def validate(self) -> None:
        if self.schema_version != SENTINEL_SET_SCHEMA_VERSION:
            raise ValueError("unsupported sentinel-set schema version")
        if self.purpose != SENTINEL_PURPOSE:
            raise ValueError("sentinels may only support syntax/recall diagnostics")
        if self.accuracy_interpretation != SENTINEL_ACCURACY_INTERPRETATION:
            raise ValueError("sentinel sets must prohibit gold-label/accuracy interpretation")
        if not self.frozen_at:
            raise ValueError("sentinel set must be frozen prospectively before sizing")
        if not self.entries:
            raise ValueError("sentinel set must contain explicit papers")
        identifiers = [item.sentinel_id for item in self.entries]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("sentinel IDs must be unique")
        for entry in self.entries:
            if not entry.title.strip() or not entry.diagnostic_family_ids:
                raise ValueError("sentinels require title and diagnostic family IDs")
            if not set(entry.diagnostic_family_ids).issubset(EXPECTED_FAMILIES):
                raise ValueError("sentinel references an unknown query family")
        _reject_forbidden_keys(_serialize(self), _FORBIDDEN_SENTINEL_KEYS)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    def sentinel_set_hash(self) -> str:
        return _sha256(self.to_json())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SentinelPaperSet:
        _reject_forbidden_keys(data, _FORBIDDEN_SENTINEL_KEYS)
        return cls(
            schema_version=str(data["schema_version"]),
            sentinel_set_id=str(data["sentinel_set_id"]),
            sentinel_set_version=str(data["sentinel_set_version"]),
            candidate_set_hash=str(data["candidate_set_hash"]),
            frozen_at=str(data["frozen_at"]),
            entries=[SentinelPaper(**item) for item in data["entries"]],
            purpose=str(data.get("purpose", SENTINEL_PURPOSE)),
            accuracy_interpretation=str(
                data.get("accuracy_interpretation", SENTINEL_ACCURACY_INTERPRETATION)
            ),
        )


def load_candidate_set(path: str | Path) -> CandidateSet:
    candidate_set = CandidateSet(json.loads(Path(path).read_text(encoding="utf-8")))
    candidate_set.validate()
    return candidate_set


def save_sizing_run(path: str | Path, run: QuerySizingRun) -> str:
    content = run.to_json() + "\n"
    atomic_write(Path(path), content.encode("utf-8"))
    return _sha256(content)


def load_sizing_run(path: str | Path) -> QuerySizingRun:
    run = QuerySizingRun.from_json(Path(path).read_text(encoding="utf-8"))
    run.validate()
    return run


def save_sentinel_set(path: str | Path, sentinel_set: SentinelPaperSet) -> str:
    content = sentinel_set.to_json() + "\n"
    atomic_write(Path(path), content.encode("utf-8"))
    return _sha256(content)


def load_sentinel_set(path: str | Path) -> SentinelPaperSet:
    sentinel_set = SentinelPaperSet.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
    sentinel_set.validate()
    return sentinel_set


def sizing_request_hash(request: dict[str, Any]) -> str:
    _reject_forbidden_keys(request, _FORBIDDEN_SIZING_KEYS)
    _reject_secrets(request)
    return _sha256(_canonical_json(request))


def _expansions(payload: dict[str, Any]) -> dict[str, str]:
    blocks = payload["blocks"]
    anchors = payload["anchors"]
    interaction = _disjunction(anchors["INTERACTION"]["terms"])
    environment = _disjunction(anchors["ENVIRONMENT_CONTEXT"]["terms"])
    conversational = _disjunction(anchors["CONVERSATIONAL_CONTEXT"]["terms"])
    high = {name: _disjunction(value["high_specificity"]) for name, value in blocks.items()}
    broad = {name: _disjunction(value["recall_expansion"]) for name, value in blocks.items()}
    return {
        "L": _or(high["L"], broad["L"]),
        "R_HIGH": high["R"],
        "R_BROAD": broad["R"],
        "V": _or(high["V"], _and(broad["V"], interaction)),
        "A_HIGH": high["A"],
        "A_GENERIC": broad["A"],
        "S": _or(high["S"], _and(broad["S"], interaction)),
        "M": _or(high["M"], _and(broad["M"], environment)),
        "C": _or(high["C"], _and(broad["C"], conversational)),
        "ASSISTANCE_CONTEXT": _disjunction(anchors["ASSISTANCE_CONTEXT"]["terms"]),
        "RELATIONAL_CONTEXT": _disjunction(anchors["RELATIONAL_CONTEXT"]["terms"]),
    }


def _expand_template(template: str, expansions: dict[str, str]) -> str:
    output = template
    for name in sorted(expansions, key=len, reverse=True):
        output = output.replace("{" + name + "}", expansions[name])
    unresolved = re.findall(r"\{[A-Z_]+\}", output)
    if unresolved:
        raise CandidateConfigurationError(f"unresolved query placeholders: {sorted(unresolved)}")
    return " ".join(output.split())


def _render_source_query(
    candidate_set: CandidateSet,
    source: str,
    source_spec: dict[str, Any],
    family_id: str,
    family: dict[str, Any],
    variant_id: str,
    expression: str,
) -> CandidateQuery:
    syntax = source_spec["syntax"]
    query_text = expression
    fields: list[str] = []
    uncertainties: list[str] = []
    request: dict[str, Any]
    if syntax == "pubmed_title_abstract":
        query_text = f"({expression})[Title/Abstract]"
        fields = ["Title", "Abstract"]
        request = {
            "method": "GET",
            "endpoint": "esearch.fcgi",
            "params": {"db": "pubmed", "term": query_text, "retmax": 0, "retmode": "xml"},
        }
    elif syntax == "europe_pmc_title_abstract":
        query_text = f"TITLE_ABS:({expression})"
        fields = ["Title", "Abstract"]
        request = {
            "method": "GET",
            "endpoint": "search",
            "params": {"query": query_text, "format": "json", "pageSize": 1, "resultType": "core"},
        }
    elif source == "SemanticScholar":
        uncertainties.append("bulk_boolean_semantics_unverified")
        request = {
            "method": "GET",
            "endpoint": "paper/search/bulk",
            "params": {"query": query_text, "limit": 1, "fields": "paperId", "sort": "paperId:asc"},
        }
    elif syntax == "arxiv_all_fields":
        query_text = f"all:({expression})"
        fields = ["all"]
        request = {
            "method": "GET",
            "endpoint": "api/query",
            "params": {
                "search_query": query_text,
                "start": 0,
                "max_results": 1,
                "sortBy": "submittedDate",
                "sortOrder": "ascending",
            },
        }
    elif syntax == "ieee_querytext":
        request = {
            "method": "GET",
            "endpoint": "api/v1/search/articles",
            "params": {
                "querytext": query_text,
                "format": "json",
                "max_records": 1,
                "start_record": 1,
                "sort_field": "article_number",
                "sort_order": "asc",
            },
        }
    elif syntax == "acm_advanced_search_ui":
        fields = ["Title", "Abstract", "Author Keywords"]
        uncertainties.append("nested_boolean_ui_semantics_unverified")
        request = {
            "transport": "human_ui",
            "workflow": "advanced_search",
            "query": query_text,
            "scope": "ACM Publications",
            "fields": fields,
            "filters": {},
            "sort": "publicationDate asc",
            "citation_export": False,
        }
    elif source == "CrossRef":
        uncertainties.append("general_query_identification_semantics_unverified")
        request = {
            "method": "GET",
            "endpoint": "works",
            "params": {"query": query_text, "rows": 0},
            "fallback_params": {"query": query_text, "rows": 1},
        }
    else:  # pragma: no cover - guarded by candidate-set source validation
        raise CandidateConfigurationError(f"unsupported sizing source: {source}")
    query_hash = _sha256(
        _canonical_json({"query_text": query_text, "fields": fields, "request": request})
    )
    candidate_id = f"candidate:{family_id}:{variant_id}:{source}"
    return CandidateQuery(
        candidate_query_id=candidate_id,
        candidate_set_id=candidate_set.candidate_set_id,
        candidate_set_version=candidate_set.candidate_set_version,
        candidate_set_hash=candidate_set.candidate_set_hash(),
        family_id=family_id,
        family_role=str(family["role"]),
        variant_id=variant_id,
        leading_candidate=family.get("leading_candidate") == variant_id,
        source=source,
        source_role=str(source_spec["role"]),
        query_text=query_text,
        query_hash=query_hash,
        field_restrictions=fields,
        sizing_request=request,
        syntax_uncertainties=uncertainties,
        hard_window=source_spec["sizing"].get("hard_window"),
        count_kind=SizingCountKind(source_spec["sizing"]["count_kind"]),
    )


def _disjunction(terms: list[str]) -> str:
    if not terms:
        raise CandidateConfigurationError("query term group cannot be empty")
    return "(" + " OR ".join(terms) + ")"


def _or(left: str, right: str) -> str:
    return f"({left} OR {right})"


def _and(left: str, right: str) -> str:
    return f"({left} AND {right})"


def _serialize(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: _serialize(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _reject_forbidden_keys(value: Any, forbidden: set[str]) -> None:
    if isinstance(value, dict):
        overlap = forbidden.intersection(str(key).lower() for key in value)
        if overlap:
            raise ValueError(f"non-production artifact contains forbidden fields: {sorted(overlap)}")
        for item in value.values():
            _reject_forbidden_keys(item, forbidden)
    elif isinstance(value, list):
        for item in value:
            _reject_forbidden_keys(item, forbidden)


def _reject_secrets(value: Any) -> None:
    sensitive = {"api_key", "apikey", "authorization", "token"}
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in sensitive and item not in {None, "", "<redacted>"}:
                raise ValueError(f"sizing request contains persisted secret field: {key}")
            _reject_secrets(item)
    elif isinstance(value, list):
        for item in value:
            _reject_secrets(item)
