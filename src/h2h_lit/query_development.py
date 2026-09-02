"""Offline candidate-query rendering and non-production sizing provenance."""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any

from h2h_lit.checkpoint import atomic_write

CANDIDATE_SCHEMA_VERSION = "0.1.0"
CANDIDATE_V2_SCHEMA_VERSION = "0.2.0"
CANDIDATE_V3_SCHEMA_VERSION = "0.3.0"
CANDIDATE_V4_SCHEMA_VERSION = "0.4.0"
SIZING_RUN_SCHEMA_VERSION = "1.1.0"
SIZING_RUN_V2_SCHEMA_VERSION = "1.2.0"
SIZING_RUN_V4_SCHEMA_VERSION = "1.3.0"
LEGACY_SIZING_RUN_SCHEMA_VERSION = "1.0.0"
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
SENTINEL_RECALL_INTERPRETATION = "prohibited"
SENTINEL_MISS_POLICY = "diagnosis_only"
SENTINEL_MUTATION_POLICY = "new_version_and_hash_required"

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


class SizingTransportStatus(str, Enum):
    PLANNED = "planned"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NOT_APPLICABLE = "not_applicable"
    BLOCKED_CREDENTIAL = "blocked_credential"
    PENDING_MANUAL = "pending_manual"
    GATE_FAILED = "gate_failed"
    BLOCKED_GATE = "blocked_gate"


class SizingGateStatus(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    UNRESOLVED = "unresolved"


class SentinelDiagnosticOutcome(str, Enum):
    INDEXED_AND_MATCHED = "INDEXED_AND_MATCHED"
    INDEXED_BUT_QUERY_MISSED = "INDEXED_BUT_QUERY_MISSED"
    SOURCE_NOT_INDEXED = "SOURCE_NOT_INDEXED"
    IDENTITY_UNRESOLVED = "IDENTITY_UNRESOLVED"
    DIAGNOSTIC_UNSUPPORTED = "DIAGNOSTIC_UNSUPPORTED"


class SentinelIdentityState(str, Enum):
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"


class SentinelSourceIndexingState(str, Enum):
    INDEXED = "indexed"
    NOT_INDEXED = "not_indexed"
    UNKNOWN = "unknown"


class SentinelCandidateMatchState(str, Enum):
    MATCHED = "matched"
    MISSED = "missed"
    UNTESTED = "untested"
    UNSUPPORTED = "unsupported"


class SentinelIdentityResolutionStatus(str, Enum):
    RESOLVED_INDEXED = "resolved_indexed"
    CONCLUSIVELY_NOT_INDEXED = "conclusively_not_indexed"
    IDENTITY_UNRESOLVED = "identity_unresolved"
    TRANSPORT_FAILURE = "transport_failure"
    PARSER_FAILURE = "parser_failure"
    DIAGNOSTIC_UNSUPPORTED = "diagnostic_unsupported"
    BLOCKED_CREDENTIAL = "blocked_credential"


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
    parser_contract: str = "legacy_v0_1"
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
        if data.get("schema_version") not in {
            CANDIDATE_SCHEMA_VERSION,
            CANDIDATE_V2_SCHEMA_VERSION,
            CANDIDATE_V3_SCHEMA_VERSION,
            CANDIDATE_V4_SCHEMA_VERSION,
        }:
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
        if self.payload["schema_version"] == CANDIDATE_V4_SCHEMA_VERSION:
            if set(qf02["variants"]) != {"A", "B", "C", "D", "E"}:
                raise CandidateConfigurationError("v0.4 QF02 requires A/B/C/D/E definitions")
            if qf02.get("leading_candidate") is not None:
                raise CandidateConfigurationError("v0.4 cannot automatically select QF02-E")
            qf03 = families[EXPECTED_FAMILIES[2]]
            if set(qf03["variants"]) != {"default", "revised"}:
                raise CandidateConfigurationError(
                    "v0.4 QF03 requires historical and revised definitions"
                )
            self._validate_bounded_v4()
        else:
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

    def _validate_bounded_v4(self) -> None:
        approved = [
            (item.get("family_id"), item.get("variant_id"))
            for item in self.payload.get("approved_production_freeze", [])
        ]
        if approved != [
            (EXPECTED_FAMILIES[0], "unanchored"),
            (EXPECTED_FAMILIES[3], "default"),
            (EXPECTED_FAMILIES[4], "default"),
        ]:
            raise CandidateConfigurationError("v0.4 production-freeze declarations changed")
        matrix = self.payload.get("bounded_matrix", [])
        expanded = [
            (item.get("family_id"), item.get("variant_id"), source)
            for item in matrix
            for source in item.get("sources", [])
        ]
        expected_sources = ["PubMed", "EuropePMC", "SemanticScholar", "arXiv"]
        expected = [
            (EXPECTED_FAMILIES[1], "E", source) for source in expected_sources
        ] + [
            (EXPECTED_FAMILIES[2], "revised", source) for source in expected_sources
        ] + [
            (EXPECTED_FAMILIES[1], variant, "SemanticScholar")
            for variant in ("C", "D")
        ]
        if expanded != expected:
            raise CandidateConfigurationError("v0.4 bounded sizing matrix changed")
        assertions = self.payload.get("containment_assertions", [])
        if [item.get("kind") for item in assertions] != [
            "candidate_count_less_than_or_equal",
            "sentinel_match_implication",
        ]:
            raise CandidateConfigurationError("v0.4 containment assertions changed")

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
        if self.payload["schema_version"] == CANDIDATE_SCHEMA_VERSION:
            if "provisional" not in crossref.get("role", ""):
                raise CandidateConfigurationError("Crossref identification must remain provisional")
            if EXPECTED_FAMILIES[2] in crossref.get("families", []):
                raise CandidateConfigurationError("Crossref cannot execute QF03")
        else:
            if crossref.get("families"):
                raise CandidateConfigurationError(
                    "Crossref cannot execute identification candidates in v0.2"
                )
            if crossref.get("role") != "enrichment_identity_and_deduplication_support":
                raise CandidateConfigurationError("Crossref v0.2 role is not explicit")
            capabilities = set(crossref.get("capabilities", []))
            if capabilities != {
                "doi_metadata_enrichment",
                "exact_identity_resolution",
                "deduplication_support",
            }:
                raise CandidateConfigurationError("Crossref v0.2 capabilities are incomplete")
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
        if self.payload["schema_version"] == CANDIDATE_V4_SCHEMA_VERSION:
            for matrix_item in self.payload["bounded_matrix"]:
                family_id = matrix_item["family_id"]
                variant_id = matrix_item["variant_id"]
                family = families[family_id]
                expression = _expand_template(family["variants"][variant_id], expansions)
                for source in matrix_item["sources"]:
                    output.append(
                        _render_source_query(
                            self,
                            source,
                            self.payload["sources"][source],
                            family_id,
                            family,
                            variant_id,
                            expression,
                        )
                    )
            return output
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

    def render_selected(
        self,
        selections: dict[str, str],
        sources: list[str],
    ) -> list[CandidateQuery]:
        """Render an explicit family/variant freeze without changing candidate state."""

        self.validate()
        if set(selections) != set(EXPECTED_FAMILIES):
            raise CandidateConfigurationError(
                "production selection must contain exactly the approved five families"
            )
        expansions = _expansions(self.payload)
        output: list[CandidateQuery] = []
        for family_id in EXPECTED_FAMILIES:
            family = self.payload["families"][family_id]
            variant_id = selections[family_id]
            if variant_id not in family["variants"]:
                raise CandidateConfigurationError(
                    f"unknown production variant {family_id}:{variant_id}"
                )
            expression = _expand_template(family["variants"][variant_id], expansions)
            for source in sources:
                source_spec = self.payload["sources"].get(source)
                if source_spec is None:
                    raise CandidateConfigurationError(f"unknown production source: {source}")
                if family_id not in source_spec.get("families", []):
                    raise CandidateConfigurationError(
                        f"source {source} does not support production family {family_id}"
                    )
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
class SizingAttempt:
    attempt_number: int
    started_at: str
    completed_at: str | None
    request: dict[str, Any]
    request_hash: str
    transport_status: SizingTransportStatus
    response_status: str | int | None = None
    retry_of_attempt_number: int | None = None
    retry_reason: str | None = None
    response_hash: str | None = None
    credential_reference: str | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    retry_after: str | None = None

    def validate(self) -> None:
        if self.attempt_number < 1:
            raise ValueError("sizing attempt numbers must be positive")
        if self.attempt_number == 1 and self.retry_of_attempt_number is not None:
            raise ValueError("the first sizing attempt cannot retry an earlier attempt")
        if self.attempt_number > 1:
            if self.retry_of_attempt_number != self.attempt_number - 1:
                raise ValueError("sizing retry lineage must reference the preceding attempt")
            if not self.retry_reason:
                raise ValueError("retried sizing attempts require a retry reason")
        if self.transport_status is SizingTransportStatus.PLANNED and self.completed_at:
            raise ValueError("planned sizing attempts cannot have completed_at")
        _validate_request(self.request, self.request_hash)
        _validate_hash(self.response_hash, "response hash")
        _validate_credential_reference(self.credential_reference)

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SizingAttempt:
        return cls(
            attempt_number=int(data["attempt_number"]),
            started_at=str(data["started_at"]),
            completed_at=data.get("completed_at"),
            request=dict(data["request"]),
            request_hash=str(data["request_hash"]),
            transport_status=SizingTransportStatus(data["transport_status"]),
            response_status=data.get("response_status"),
            retry_of_attempt_number=data.get("retry_of_attempt_number"),
            retry_reason=data.get("retry_reason"),
            response_hash=data.get("response_hash"),
            credential_reference=data.get("credential_reference"),
            retry_after=data.get("retry_after"),
            warnings=list(data.get("warnings", [])),
            errors=list(data.get("errors", [])),
        )


@dataclass(frozen=True, slots=True)
class AcmOperatorEvidence:
    operator_id: str
    observed_at: str
    ui_rendered_query: str
    ui_reported_count: int
    query_url: str | None = None
    artifact_path: str | None = None
    artifact_hash: str | None = None
    institutional_access_tier: str | None = None

    def validate(self) -> None:
        if not self.operator_id.strip() or not self.observed_at.strip():
            raise ValueError("ACM evidence requires operator ID and UTC timestamp")
        if not self.ui_rendered_query.strip():
            raise ValueError("ACM evidence requires the UI-rendered query")
        if self.ui_reported_count < 0:
            raise ValueError("ACM UI-reported counts cannot be negative")
        if self.artifact_path is not None:
            _validate_relative_path(self.artifact_path)
            if self.artifact_hash is None:
                raise ValueError("ACM evidence artifacts require a SHA-256 hash")
        _validate_hash(self.artifact_hash, "ACM artifact hash")
        if self.query_url:
            _reject_secrets({"query_url": self.query_url})

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AcmOperatorEvidence:
        return cls(
            operator_id=str(data["operator_id"]),
            observed_at=str(data["observed_at"]),
            ui_rendered_query=str(data["ui_rendered_query"]),
            ui_reported_count=int(data["ui_reported_count"]),
            query_url=data.get("query_url"),
            artifact_path=data.get("artifact_path"),
            artifact_hash=data.get("artifact_hash"),
            institutional_access_tier=data.get("institutional_access_tier"),
        )


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
    transport_status: SizingTransportStatus = SizingTransportStatus.PLANNED
    response_status: str | int | None = None
    credential_reference: str | None = None
    gate_name: str | None = None
    gate_status: SizingGateStatus = SizingGateStatus.NOT_APPLICABLE
    gate_evaluation_id: str | None = None
    source_query_translation: str | None = None
    source_messages: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    attempts: list[SizingAttempt] = field(default_factory=list)
    acm_operator_evidence: AcmOperatorEvidence | None = None

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
        _validate_request(self.request, self.request_hash)
        _validate_hash(self.response_hash, "response hash")
        _validate_credential_reference(self.credential_reference)
        if self.gate_status is not SizingGateStatus.NOT_APPLICABLE and not self.gate_name:
            raise ValueError("sizing gate status requires a gate name")
        for expected, attempt in enumerate(self.attempts, start=1):
            attempt.validate()
            if attempt.attempt_number != expected:
                raise ValueError("sizing attempts must be contiguous and ordered")
            if attempt.credential_reference != self.credential_reference:
                raise ValueError("attempt credential reference must match its observation")
        if self.attempts:
            final = self.attempts[-1]
            if final.transport_status is not self.transport_status:
                raise ValueError("observation transport status must match its final attempt")
            if final.response_hash != self.response_hash:
                raise ValueError("observation response hash must match its final attempt")
        if self.acm_operator_evidence is not None:
            if self.source != "ACMDigitalLibrary":
                raise ValueError("ACM operator evidence is only valid for ACM observations")
            self.acm_operator_evidence.validate()

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
            transport_status=SizingTransportStatus(
                data.get("transport_status", SizingTransportStatus.PLANNED.value)
            ),
            response_status=data.get("response_status"),
            credential_reference=data.get("credential_reference"),
            gate_name=data.get("gate_name"),
            gate_status=SizingGateStatus(
                data.get("gate_status", SizingGateStatus.NOT_APPLICABLE.value)
            ),
            gate_evaluation_id=data.get("gate_evaluation_id"),
            source_query_translation=data.get("source_query_translation"),
            source_messages={
                str(kind): {
                    str(category): [str(item) for item in values]
                    for category, values in categories.items()
                }
                for kind, categories in data.get("source_messages", {}).items()
            },
            warnings=list(data.get("warnings", [])),
            attempts=[SizingAttempt.from_dict(item) for item in data.get("attempts", [])],
            acm_operator_evidence=(
                AcmOperatorEvidence.from_dict(data["acm_operator_evidence"])
                if data.get("acm_operator_evidence") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class SentinelDiagnostic:
    sentinel_id: str
    source: str
    candidate_query_id: str
    outcome: SentinelDiagnosticOutcome
    identity_state: SentinelIdentityState
    source_indexing_state: SentinelSourceIndexingState
    candidate_match_state: SentinelCandidateMatchState
    request: dict[str, Any] | None = None
    request_hash: str | None = None
    response_hash: str | None = None
    identifier_results: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def validate(self) -> None:
        expected = {
            SentinelDiagnosticOutcome.INDEXED_AND_MATCHED: (
                SentinelIdentityState.RESOLVED,
                SentinelSourceIndexingState.INDEXED,
                SentinelCandidateMatchState.MATCHED,
            ),
            SentinelDiagnosticOutcome.INDEXED_BUT_QUERY_MISSED: (
                SentinelIdentityState.RESOLVED,
                SentinelSourceIndexingState.INDEXED,
                SentinelCandidateMatchState.MISSED,
            ),
            SentinelDiagnosticOutcome.SOURCE_NOT_INDEXED: (
                SentinelIdentityState.RESOLVED,
                SentinelSourceIndexingState.NOT_INDEXED,
                SentinelCandidateMatchState.UNTESTED,
            ),
            SentinelDiagnosticOutcome.IDENTITY_UNRESOLVED: (
                SentinelIdentityState.UNRESOLVED,
                SentinelSourceIndexingState.UNKNOWN,
                SentinelCandidateMatchState.UNTESTED,
            ),
            SentinelDiagnosticOutcome.DIAGNOSTIC_UNSUPPORTED: (
                SentinelIdentityState.RESOLVED,
                SentinelSourceIndexingState.UNKNOWN,
                SentinelCandidateMatchState.UNSUPPORTED,
            ),
        }[self.outcome]
        actual = (
            self.identity_state,
            self.source_indexing_state,
            self.candidate_match_state,
        )
        if actual != expected:
            raise ValueError(f"sentinel diagnostic states do not match outcome {self.outcome.value}")
        if self.request is None and self.request_hash is not None:
            raise ValueError("sentinel request hash requires a request")
        if self.request is not None:
            if self.request_hash is None:
                raise ValueError("sentinel diagnostic requests require a request hash")
            _validate_request(self.request, self.request_hash)
        _validate_hash(self.response_hash, "sentinel response hash")
        if self.outcome is SentinelDiagnosticOutcome.INDEXED_AND_MATCHED:
            if not self.identifier_results:
                raise ValueError("matched sentinel diagnostics require identifier-only results")
        elif self.identifier_results:
            raise ValueError("unmatched sentinel diagnostics cannot preserve identifier results")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SentinelDiagnostic:
        return cls(
            sentinel_id=str(data["sentinel_id"]),
            source=str(data["source"]),
            candidate_query_id=str(data["candidate_query_id"]),
            outcome=SentinelDiagnosticOutcome(data["outcome"]),
            identity_state=SentinelIdentityState(data["identity_state"]),
            source_indexing_state=SentinelSourceIndexingState(
                data["source_indexing_state"]
            ),
            candidate_match_state=SentinelCandidateMatchState(
                data["candidate_match_state"]
            ),
            request=dict(data["request"]) if data.get("request") is not None else None,
            request_hash=data.get("request_hash"),
            response_hash=data.get("response_hash"),
            identifier_results=list(data.get("identifier_results", [])),
            warnings=list(data.get("warnings", [])),
            errors=list(data.get("errors", [])),
        )


@dataclass(frozen=True, slots=True)
class SentinelIdentityResolution:
    resolution_id: str
    sentinel_id: str
    source: str
    status: SentinelIdentityResolutionStatus
    request: dict[str, Any] | None = None
    request_hash: str | None = None
    response_hash: str | None = None
    identifier_results: list[str] = field(default_factory=list)
    attempts: list[SizingAttempt] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.request is None and self.request_hash is not None:
            raise ValueError("sentinel identity request hash requires a request")
        if self.request is not None:
            if self.request_hash is None:
                raise ValueError("sentinel identity requests require a request hash")
            _validate_request(self.request, self.request_hash)
        _validate_hash(self.response_hash, "sentinel identity response hash")
        for expected, attempt in enumerate(self.attempts, start=1):
            attempt.validate()
            if attempt.attempt_number != expected:
                raise ValueError("sentinel identity attempts must be contiguous and ordered")
        if self.status is SentinelIdentityResolutionStatus.RESOLVED_INDEXED:
            if not self.identifier_results:
                raise ValueError("indexed sentinel identity requires identifier results")
        elif self.identifier_results:
            raise ValueError("unresolved/not-indexed identity cannot retain identifiers")
        if self.status is SentinelIdentityResolutionStatus.CONCLUSIVELY_NOT_INDEXED:
            if self.response_hash is None or not self.attempts:
                raise ValueError("not-indexed identity requires a successful response proof")
            if self.attempts[-1].transport_status is not SizingTransportStatus.SUCCEEDED:
                raise ValueError("failed identity lookup cannot mean source not indexed")
        if self.status in {
            SentinelIdentityResolutionStatus.TRANSPORT_FAILURE,
            SentinelIdentityResolutionStatus.PARSER_FAILURE,
        } and not self.errors:
            raise ValueError("identity failures require an explicit error")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SentinelIdentityResolution:
        return cls(
            resolution_id=str(data["resolution_id"]),
            sentinel_id=str(data["sentinel_id"]),
            source=str(data["source"]),
            status=SentinelIdentityResolutionStatus(data["status"]),
            request=dict(data["request"]) if data.get("request") is not None else None,
            request_hash=data.get("request_hash"),
            response_hash=data.get("response_hash"),
            identifier_results=list(data.get("identifier_results", [])),
            attempts=[SizingAttempt.from_dict(item) for item in data.get("attempts", [])],
            warnings=list(data.get("warnings", [])),
            errors=list(data.get("errors", [])),
        )


@dataclass(frozen=True, slots=True)
class SemanticControlProbe:
    probe_id: str
    expression: str
    role: str


@dataclass(frozen=True, slots=True)
class SemanticControlAssertion:
    assertion_id: str
    left_probe_id: str
    relation: str
    right_probe_id: str

    def validate(self) -> None:
        if self.relation not in {"less_than_or_equal", "greater_than_or_equal", "equal"}:
            raise ValueError("unsupported semantic-control relation")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)


@dataclass(slots=True)
class SemanticControlSet:
    schema_version: str
    control_set_id: str
    control_set_version: str
    target_source: str
    endpoint_mode: str
    purpose: str
    term_rationale: str
    probes: list[SemanticControlProbe]
    assertions: list[SemanticControlAssertion]
    production_identification_gate: str
    automatic_mode_switching: bool = False
    creates_production_occurrences: bool = False
    contributes_prisma_counts: bool = False

    def validate(self) -> None:
        if self.schema_version != "1.0.0":
            raise ValueError("unsupported semantic-control schema version")
        if self.target_source != "SemanticScholar" or self.endpoint_mode != "bulk":
            raise ValueError("semantic controls are frozen for Semantic Scholar bulk only")
        if self.purpose != "endpoint_boolean_semantics_only":
            raise ValueError("semantic controls cannot carry review methodology semantics")
        if not self.term_rationale.strip():
            raise ValueError("semantic controls require a prospective term rationale")
        if self.production_identification_gate != "bulk_boolean_semantics":
            raise ValueError("semantic controls must govern the bulk Boolean gate")
        if any(
            (
                self.automatic_mode_switching,
                self.creates_production_occurrences,
                self.contributes_prisma_counts,
            )
        ):
            raise ValueError("semantic controls cannot switch modes or affect production")
        probe_ids = [probe.probe_id for probe in self.probes]
        if len(probe_ids) != 6 or len(probe_ids) != len(set(probe_ids)):
            raise ValueError("semantic controls require six unique predeclared probes")
        known = set(probe_ids)
        for assertion in self.assertions:
            assertion.validate()
            if {assertion.left_probe_id, assertion.right_probe_id} - known:
                raise ValueError("semantic-control assertion references an unknown probe")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    def control_set_hash(self) -> str:
        return _sha256(_canonical_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticControlSet:
        return cls(
            schema_version=str(data["schema_version"]),
            control_set_id=str(data["control_set_id"]),
            control_set_version=str(data["control_set_version"]),
            target_source=str(data["target_source"]),
            endpoint_mode=str(data["endpoint_mode"]),
            purpose=str(data["purpose"]),
            term_rationale=str(data["term_rationale"]),
            probes=[SemanticControlProbe(**item) for item in data["probes"]],
            assertions=[SemanticControlAssertion(**item) for item in data["assertions"]],
            production_identification_gate=str(data["production_identification_gate"]),
            automatic_mode_switching=bool(data.get("automatic_mode_switching", False)),
            creates_production_occurrences=bool(
                data.get("creates_production_occurrences", False)
            ),
            contributes_prisma_counts=bool(data.get("contributes_prisma_counts", False)),
        )


@dataclass(frozen=True, slots=True)
class SemanticControlObservation:
    control_query_id: str
    probe_id: str
    source: str
    observed_at: str
    request: dict[str, Any]
    request_hash: str
    transport_status: SizingTransportStatus
    response_status: str | int | None = None
    response_hash: str | None = None
    reported_count: int | None = None
    count_kind: SizingCountKind = SizingCountKind.ESTIMATED
    syntax_status: SizingSyntaxStatus = SizingSyntaxStatus.UNTESTED
    attempts: list[SizingAttempt] = field(default_factory=list)
    failure_state: str | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.source != "SemanticScholar":
            raise ValueError("semantic controls are only valid for Semantic Scholar")
        _validate_request(self.request, self.request_hash)
        _validate_hash(self.response_hash, "semantic-control response hash")
        if self.reported_count is not None and self.reported_count < 0:
            raise ValueError("semantic-control counts cannot be negative")
        for expected, attempt in enumerate(self.attempts, start=1):
            attempt.validate()
            if attempt.attempt_number != expected:
                raise ValueError("semantic-control attempts must be contiguous and ordered")
            if attempt.request_hash != self.request_hash:
                raise ValueError("semantic-control retry changed the frozen request")
        if self.transport_status is SizingTransportStatus.SUCCEEDED:
            if self.reported_count is None or not self.attempts:
                raise ValueError("successful semantic control requires a completed count")
            if self.attempts[-1].transport_status is not SizingTransportStatus.SUCCEEDED:
                raise ValueError("semantic-control status must match its final attempt")
        if self.transport_status is SizingTransportStatus.FAILED and not self.failure_state:
            raise ValueError("failed semantic control requires an explicit failure state")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticControlObservation:
        return cls(
            control_query_id=str(data["control_query_id"]),
            probe_id=str(data["probe_id"]),
            source=str(data["source"]),
            observed_at=str(data["observed_at"]),
            request=dict(data["request"]),
            request_hash=str(data["request_hash"]),
            transport_status=SizingTransportStatus(data["transport_status"]),
            response_status=data.get("response_status"),
            response_hash=data.get("response_hash"),
            reported_count=data.get("reported_count"),
            count_kind=SizingCountKind(data.get("count_kind", "estimated")),
            syntax_status=SizingSyntaxStatus(data.get("syntax_status", "untested")),
            attempts=[SizingAttempt.from_dict(item) for item in data.get("attempts", [])],
            failure_state=data.get("failure_state"),
            warnings=list(data.get("warnings", [])),
            errors=list(data.get("errors", [])),
        )


@dataclass(frozen=True, slots=True)
class SemanticAssertionResult:
    assertion_id: str
    left_probe_id: str
    relation: str
    right_probe_id: str
    left_count: int
    right_count: int
    passed: bool

    def validate(self) -> None:
        if self.relation not in {"less_than_or_equal", "greater_than_or_equal", "equal"}:
            raise ValueError("unsupported semantic-control assertion relation")
        expected = {
            "less_than_or_equal": self.left_count <= self.right_count,
            "greater_than_or_equal": self.left_count >= self.right_count,
            "equal": self.left_count == self.right_count,
        }[self.relation]
        if self.passed is not expected:
            raise ValueError("semantic-control assertion result is inconsistent")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticAssertionResult:
        return cls(
            assertion_id=str(data["assertion_id"]),
            left_probe_id=str(data["left_probe_id"]),
            relation=str(data["relation"]),
            right_probe_id=str(data["right_probe_id"]),
            left_count=int(data["left_count"]),
            right_count=int(data["right_count"]),
            passed=bool(data["passed"]),
        )


@dataclass(frozen=True, slots=True)
class SemanticControlGateEvaluation:
    evaluation_id: str
    gate_name: str
    state: SizingGateStatus
    evaluated_at: str
    dry_run_plan_hash: str
    control_query_ids: list[str]
    assertion_results: list[SemanticAssertionResult] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.gate_name != "bulk_boolean_semantics":
            raise ValueError("semantic-control evaluation has an unexpected gate")
        if self.state not in {
            SizingGateStatus.PASSED,
            SizingGateStatus.FAILED,
            SizingGateStatus.UNRESOLVED,
        }:
            raise ValueError("semantic-control gate must be PASS, FAIL, or UNRESOLVED")
        _validate_hash(self.dry_run_plan_hash, "semantic-control dry-run plan hash")
        if len(self.control_query_ids) != 6 or len(set(self.control_query_ids)) != 6:
            raise ValueError("semantic-control gate requires the six frozen controls")
        for result in self.assertion_results:
            result.validate()
        if self.state is SizingGateStatus.PASSED:
            if self.reasons or not self.assertion_results:
                raise ValueError("passing semantic-control gate requires passed assertions")
            if not all(item.passed for item in self.assertion_results):
                raise ValueError("passing semantic-control gate has a failed assertion")
        elif not self.reasons:
            raise ValueError("failed/unresolved semantic-control gates require reasons")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticControlGateEvaluation:
        return cls(
            evaluation_id=str(data["evaluation_id"]),
            gate_name=str(data["gate_name"]),
            state=SizingGateStatus(data["state"]),
            evaluated_at=str(data["evaluated_at"]),
            dry_run_plan_hash=str(data["dry_run_plan_hash"]),
            control_query_ids=list(data["control_query_ids"]),
            assertion_results=[
                SemanticAssertionResult.from_dict(item)
                for item in data.get("assertion_results", [])
            ],
            reasons=list(data.get("reasons", [])),
        )


@dataclass(frozen=True, slots=True)
class ContainmentAssertionResult:
    assertion_id: str
    kind: str
    state: SizingGateStatus
    source: str
    subset_candidate_query_id: str
    superset_candidate_query_id: str
    subset_count: int | None = None
    superset_count: int | None = None
    evaluated_sentinel_ids: list[str] = field(default_factory=list)
    failed_sentinel_ids: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.kind not in {
            "candidate_count_less_than_or_equal",
            "sentinel_match_implication",
        }:
            raise ValueError("unsupported containment assertion kind")
        if self.source != "SemanticScholar":
            raise ValueError("v0.4 containment assertions are Semantic Scholar-only")
        if self.state not in {
            SizingGateStatus.PASSED,
            SizingGateStatus.FAILED,
            SizingGateStatus.UNRESOLVED,
        }:
            raise ValueError("containment assertion must be passed, failed, or unresolved")
        if self.kind == "candidate_count_less_than_or_equal":
            if self.subset_count is None or self.superset_count is None:
                if self.state is not SizingGateStatus.UNRESOLVED:
                    raise ValueError("resolved count containment requires both counts")
            elif (self.subset_count <= self.superset_count) != (
                self.state is SizingGateStatus.PASSED
            ):
                raise ValueError("count containment result is inconsistent")
        if self.state is SizingGateStatus.PASSED and (
            self.failed_sentinel_ids or self.reasons
        ):
            raise ValueError("passing containment assertions cannot have failures")
        if self.state is not SizingGateStatus.PASSED and not self.reasons:
            raise ValueError("failed/unresolved containment assertions require reasons")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContainmentAssertionResult:
        return cls(
            assertion_id=str(data["assertion_id"]),
            kind=str(data["kind"]),
            state=SizingGateStatus(data["state"]),
            source=str(data["source"]),
            subset_candidate_query_id=str(data["subset_candidate_query_id"]),
            superset_candidate_query_id=str(data["superset_candidate_query_id"]),
            subset_count=data.get("subset_count"),
            superset_count=data.get("superset_count"),
            evaluated_sentinel_ids=list(data.get("evaluated_sentinel_ids", [])),
            failed_sentinel_ids=list(data.get("failed_sentinel_ids", [])),
            reasons=list(data.get("reasons", [])),
        )


@dataclass(frozen=True, slots=True)
class ContainmentEvaluation:
    evaluation_id: str
    state: SizingGateStatus
    selection_status: str
    evaluated_at: str
    dry_run_plan_hash: str
    assertion_results: list[ContainmentAssertionResult]
    reasons: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.state not in {
            SizingGateStatus.PASSED,
            SizingGateStatus.FAILED,
            SizingGateStatus.UNRESOLVED,
        }:
            raise ValueError("containment evaluation has an invalid state")
        if self.selection_status not in {"containment_supported", "unresolved"}:
            raise ValueError("containment selection status is invalid")
        _validate_hash(self.dry_run_plan_hash, "containment dry-run plan hash")
        for result in self.assertion_results:
            result.validate()
        if self.state is SizingGateStatus.PASSED:
            if self.selection_status != "containment_supported" or self.reasons:
                raise ValueError("passing containment must support containment semantics")
        elif self.selection_status != "unresolved" or not self.reasons:
            raise ValueError("failed containment must leave selection semantics unresolved")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return _serialize(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContainmentEvaluation:
        return cls(
            evaluation_id=str(data["evaluation_id"]),
            state=SizingGateStatus(data["state"]),
            selection_status=str(data["selection_status"]),
            evaluated_at=str(data["evaluated_at"]),
            dry_run_plan_hash=str(data["dry_run_plan_hash"]),
            assertion_results=[
                ContainmentAssertionResult.from_dict(item)
                for item in data.get("assertion_results", [])
            ],
            reasons=list(data.get("reasons", [])),
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
    sentinel_set_id: str | None = None
    sentinel_set_version: str | None = None
    sentinel_set_hash: str | None = None
    dry_run_plan_hash: str | None = None
    started_at: str | None = None
    observations: list[SizingObservation] = field(default_factory=list)
    semantic_control_observations: list[SemanticControlObservation] = field(
        default_factory=list
    )
    semantic_control_gate: SemanticControlGateEvaluation | None = None
    containment_evaluation: ContainmentEvaluation | None = None
    sentinel_identity_resolutions: list[SentinelIdentityResolution] = field(
        default_factory=list
    )
    sentinel_diagnostics: list[SentinelDiagnostic] = field(default_factory=list)
    completed_at: str | None = None
    purpose: str = "count_and_syntax_sizing_only"
    creates_production_occurrences: bool = False
    contributes_prisma_counts: bool = False
    establishes_retrieval_cutoff: bool = False
    derives_e6: bool = False
    creates_review_dataset: bool = False
    creates_retrieval_run: bool = False
    creates_corpus_membership: bool = False
    runs_screening: bool = False
    supports_partitioning: bool = False

    def validate(self) -> None:
        if self.schema_version not in {
            LEGACY_SIZING_RUN_SCHEMA_VERSION,
            SIZING_RUN_SCHEMA_VERSION,
            SIZING_RUN_V2_SCHEMA_VERSION,
            SIZING_RUN_V4_SCHEMA_VERSION,
        }:
            raise ValueError("unsupported query-sizing schema version")
        if self.schema_version in {
            SIZING_RUN_SCHEMA_VERSION,
            SIZING_RUN_V2_SCHEMA_VERSION,
            SIZING_RUN_V4_SCHEMA_VERSION,
        }:
            if not all(
                (self.sentinel_set_id, self.sentinel_set_version, self.sentinel_set_hash)
            ):
                raise ValueError("current query-sizing runs require sentinel-set provenance")
            _validate_hash(self.sentinel_set_hash, "sentinel-set hash")
        _validate_hash(self.dry_run_plan_hash, "dry-run plan hash")
        if self.purpose != "count_and_syntax_sizing_only":
            raise ValueError("query sizing has an invalid purpose")
        if any(
            (
                self.creates_production_occurrences,
                self.contributes_prisma_counts,
                self.establishes_retrieval_cutoff,
                self.derives_e6,
                self.creates_review_dataset,
                self.creates_retrieval_run,
                self.creates_corpus_membership,
                self.runs_screening,
                self.supports_partitioning,
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
        control_ids = []
        for control in self.semantic_control_observations:
            control.validate()
            control_ids.append(control.control_query_id)
        if len(control_ids) != len(set(control_ids)):
            raise ValueError("semantic-control observations must have unique IDs")
        if self.semantic_control_gate is not None:
            self.semantic_control_gate.validate()
            if set(self.semantic_control_gate.control_query_ids) != set(control_ids):
                raise ValueError("semantic-control gate does not cover committed controls")
            if self.semantic_control_gate.dry_run_plan_hash != self.dry_run_plan_hash:
                raise ValueError("semantic-control gate references another dry-run plan")
        if self.schema_version not in {
            SIZING_RUN_V2_SCHEMA_VERSION,
            SIZING_RUN_V4_SCHEMA_VERSION,
        } and (
            control_ids or self.semantic_control_gate is not None
        ):
            raise ValueError("semantic-control execution provenance requires sizing v1.2")
        if self.semantic_control_gate is not None:
            for observation in self.observations:
                if observation.gate_evaluation_id is None:
                    continue
                if observation.gate_evaluation_id != self.semantic_control_gate.evaluation_id:
                    raise ValueError("candidate references another semantic-control gate")
        identity_keys = []
        for resolution in self.sentinel_identity_resolutions:
            resolution.validate()
            identity_keys.append((resolution.source, resolution.sentinel_id))
        if len(identity_keys) != len(set(identity_keys)):
            raise ValueError("sentinel identity is resolved once per source and sentinel")
        if self.schema_version not in {
            SIZING_RUN_V2_SCHEMA_VERSION,
            SIZING_RUN_V4_SCHEMA_VERSION,
        } and identity_keys:
            raise ValueError("source-level sentinel identity provenance requires sizing v1.2")
        known_sentinels = set()
        diagnostic_keys = []
        for diagnostic in self.sentinel_diagnostics:
            diagnostic.validate()
            diagnostic_keys.append(
                (diagnostic.sentinel_id, diagnostic.source, diagnostic.candidate_query_id)
            )
            known_sentinels.add(diagnostic.sentinel_id)
            if diagnostic.candidate_query_id not in self.planned_candidate_query_ids:
                raise ValueError("sentinel diagnostic references an unplanned candidate query")
        if len(diagnostic_keys) != len(set(diagnostic_keys)):
            raise ValueError("sentinel diagnostics must be unique per sentinel/source/query")
        if known_sentinels and not self.sentinel_set_id:
            raise ValueError("sentinel diagnostics require run-level sentinel-set provenance")
        if self.containment_evaluation is not None:
            if self.schema_version != SIZING_RUN_V4_SCHEMA_VERSION:
                raise ValueError("containment evaluation requires sizing v1.3")
            self.containment_evaluation.validate()
            if self.containment_evaluation.dry_run_plan_hash != self.dry_run_plan_hash:
                raise ValueError("containment evaluation references another dry-run plan")
        if (
            self.status in {SizingRunStatus.RUNNING, SizingRunStatus.COMPLETED}
            and not self.started_at
        ):
            raise ValueError("running/completed sizing runs require started_at")
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
            sentinel_set_id=data.get("sentinel_set_id"),
            sentinel_set_version=data.get("sentinel_set_version"),
            sentinel_set_hash=data.get("sentinel_set_hash"),
            dry_run_plan_hash=data.get("dry_run_plan_hash"),
            status=SizingRunStatus(data["status"]),
            planned_candidate_query_ids=list(data["planned_candidate_query_ids"]),
            created_at=str(data["created_at"]),
            started_at=data.get("started_at"),
            observations=[SizingObservation.from_dict(item) for item in data["observations"]],
            semantic_control_observations=[
                SemanticControlObservation.from_dict(item)
                for item in data.get("semantic_control_observations", [])
            ],
            semantic_control_gate=(
                SemanticControlGateEvaluation.from_dict(data["semantic_control_gate"])
                if data.get("semantic_control_gate") is not None
                else None
            ),
            containment_evaluation=(
                ContainmentEvaluation.from_dict(data["containment_evaluation"])
                if data.get("containment_evaluation") is not None
                else None
            ),
            sentinel_identity_resolutions=[
                SentinelIdentityResolution.from_dict(item)
                for item in data.get("sentinel_identity_resolutions", [])
            ],
            sentinel_diagnostics=[
                SentinelDiagnostic.from_dict(item)
                for item in data.get("sentinel_diagnostics", [])
            ],
            completed_at=data.get("completed_at"),
            purpose=str(data.get("purpose", "count_and_syntax_sizing_only")),
            creates_production_occurrences=bool(data.get("creates_production_occurrences", False)),
            contributes_prisma_counts=bool(data.get("contributes_prisma_counts", False)),
            establishes_retrieval_cutoff=bool(data.get("establishes_retrieval_cutoff", False)),
            derives_e6=bool(data.get("derives_e6", False)),
            creates_review_dataset=bool(data.get("creates_review_dataset", False)),
            creates_retrieval_run=bool(data.get("creates_retrieval_run", False)),
            creates_corpus_membership=bool(data.get("creates_corpus_membership", False)),
            runs_screening=bool(data.get("runs_screening", False)),
            supports_partitioning=bool(data.get("supports_partitioning", False)),
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
    provenance: dict[str, Any] = field(default_factory=dict)


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
    recall_interpretation: str = SENTINEL_RECALL_INTERPRETATION
    representative_sample: bool = False
    contributes_prisma_counts: bool = False
    creates_occurrences: bool = False
    creates_corpus_membership: bool = False
    miss_policy: str = SENTINEL_MISS_POLICY
    mutation_policy: str = SENTINEL_MUTATION_POLICY
    diagnostic_states: list[str] = field(
        default_factory=lambda: [item.value for item in SentinelDiagnosticOutcome]
    )

    def validate(self) -> None:
        if self.schema_version != SENTINEL_SET_SCHEMA_VERSION:
            raise ValueError("unsupported sentinel-set schema version")
        if self.purpose != SENTINEL_PURPOSE:
            raise ValueError("sentinels may only support syntax/recall diagnostics")
        if self.accuracy_interpretation != SENTINEL_ACCURACY_INTERPRETATION:
            raise ValueError("sentinel sets must prohibit gold-label/accuracy interpretation")
        if self.recall_interpretation != SENTINEL_RECALL_INTERPRETATION:
            raise ValueError("sentinel sets must prohibit recall estimation")
        if any(
            (
                self.representative_sample,
                self.contributes_prisma_counts,
                self.creates_occurrences,
                self.creates_corpus_membership,
            )
        ):
            raise ValueError("sentinel sets cannot affect or represent the review corpus")
        if self.miss_policy != SENTINEL_MISS_POLICY:
            raise ValueError("sentinel misses must trigger diagnosis only")
        if self.mutation_policy != SENTINEL_MUTATION_POLICY:
            raise ValueError("sentinel mutations require a new version and hash")
        if self.diagnostic_states != [item.value for item in SentinelDiagnosticOutcome]:
            raise ValueError("sentinel diagnostic states must use the frozen vocabulary")
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
            provenance = entry.provenance
            if not provenance.get("source_artifact") or not provenance.get("source_paper_id"):
                raise ValueError("sentinels require versioned foundational provenance")
            _validate_relative_path(str(provenance["source_artifact"]))
            _validate_hash(provenance.get("source_artifact_hash"), "source artifact hash")
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
            recall_interpretation=str(
                data.get("recall_interpretation", SENTINEL_RECALL_INTERPRETATION)
            ),
            representative_sample=bool(data.get("representative_sample", False)),
            contributes_prisma_counts=bool(data.get("contributes_prisma_counts", False)),
            creates_occurrences=bool(data.get("creates_occurrences", False)),
            creates_corpus_membership=bool(data.get("creates_corpus_membership", False)),
            miss_policy=str(data.get("miss_policy", SENTINEL_MISS_POLICY)),
            mutation_policy=str(data.get("mutation_policy", SENTINEL_MUTATION_POLICY)),
            diagnostic_states=list(
                data.get(
                    "diagnostic_states",
                    [item.value for item in SentinelDiagnosticOutcome],
                )
            ),
        )


def load_candidate_set(path: str | Path) -> CandidateSet:
    config_path = Path(path)
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    payload = _resolve_candidate_payload(raw, config_path.parent)
    candidate_set = CandidateSet(payload)
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


def load_semantic_control_set(path: str | Path) -> SemanticControlSet:
    controls = SemanticControlSet.from_dict(
        json.loads(Path(path).read_text(encoding="utf-8"))
    )
    controls.validate()
    return controls


def evaluate_semantic_control_counts(
    controls: SemanticControlSet,
    counts: dict[str, int],
) -> tuple[SizingGateStatus, list[str]]:
    controls.validate()
    expected = {probe.probe_id for probe in controls.probes}
    if set(counts) != expected or any(value < 0 for value in counts.values()):
        return SizingGateStatus.PENDING, ["semantic_control_counts_incomplete"]
    failures: list[str] = []
    for assertion in controls.assertions:
        left = counts[assertion.left_probe_id]
        right = counts[assertion.right_probe_id]
        passed = {
            "less_than_or_equal": left <= right,
            "greater_than_or_equal": left >= right,
            "equal": left == right,
        }[assertion.relation]
        if not passed:
            failures.append(assertion.assertion_id)
    return (
        (SizingGateStatus.FAILED, failures)
        if failures
        else (SizingGateStatus.PASSED, [])
    )


def validate_sentinel_revision(
    previous: SentinelPaperSet,
    current: SentinelPaperSet,
) -> None:
    previous.validate()
    current.validate()
    if previous.to_dict()["entries"] != current.to_dict()["entries"]:
        if previous.sentinel_set_version == current.sentinel_set_version:
            raise ValueError("sentinel membership/expectation changes require a new version")
        if previous.sentinel_set_hash() == current.sentinel_set_hash():
            raise ValueError("sentinel revisions require a new hash")


def sizing_request_hash(request: dict[str, Any]) -> str:
    _validate_request(request)
    return _sha256(_canonical_json(request))


def _resolve_candidate_payload(raw: dict[str, Any], directory: Path) -> dict[str, Any]:
    schema_version = raw.get("schema_version")
    if schema_version not in {
        CANDIDATE_V2_SCHEMA_VERSION,
        CANDIDATE_V3_SCHEMA_VERSION,
        CANDIDATE_V4_SCHEMA_VERSION,
    }:
        return raw
    inheritance = raw.get("inherits")
    if not isinstance(inheritance, dict):
        raise CandidateConfigurationError("versioned candidate config requires frozen inheritance")
    relative = str(inheritance.get("relative_path", ""))
    _validate_relative_path(relative)
    base_path = directory / relative
    base = load_candidate_set(base_path)
    expected_hash = inheritance.get("candidate_set_hash")
    if base.candidate_set_hash() != expected_hash:
        raise CandidateConfigurationError("inherited candidate-set hash mismatch")
    payload = deepcopy(base.payload)
    payload["schema_version"] = schema_version
    payload["candidate_set_version"] = str(raw["candidate_set_version"])
    payload["base_candidate_set_hash"] = str(expected_hash)
    payload["sentinel_compatibility"] = deepcopy(raw["sentinel_compatibility"])
    for source, overrides in raw.get("source_overrides", {}).items():
        if source not in payload["sources"]:
            raise CandidateConfigurationError(f"unknown source override: {source}")
        payload["sources"][source].update(deepcopy(overrides))
    for family_id, overrides in raw.get("family_overrides", {}).items():
        if family_id not in payload["families"]:
            raise CandidateConfigurationError(f"unknown family override: {family_id}")
        family = payload["families"][family_id]
        additions = deepcopy(overrides.get("additional_variants", {}))
        overlap = set(additions) & set(family["variants"])
        if overlap:
            raise CandidateConfigurationError(
                f"family override replaces historical variants: {sorted(overlap)}"
            )
        family["variants"].update(additions)
        for key, value in overrides.items():
            if key != "additional_variants":
                family[key] = deepcopy(value)
    for key in (
        "approved_production_freeze",
        "bounded_matrix",
        "containment_assertions",
        "historical_evidence",
    ):
        if key in raw:
            payload[key] = deepcopy(raw[key])
    if "no_partitioning_authorized" in raw:
        payload["no_partitioning_authorized"] = raw["no_partitioning_authorized"]
    return payload


def _validate_request(request: dict[str, Any], expected_hash: str | None = None) -> None:
    _reject_forbidden_keys(request, _FORBIDDEN_SIZING_KEYS)
    _reject_secrets(request)
    if request.get("transport") == "http":
        method = request.get("method")
        if method not in {"GET", "POST"}:
            raise ValueError("HTTP sizing requests must use GET or POST")
        form = request.get("form")
        if method == "GET" and form is not None:
            raise ValueError("GET sizing requests cannot carry a form body")
        if method == "POST" and not isinstance(form, dict):
            raise ValueError("POST sizing requests require a form body")
        if not isinstance(request.get("params", {}), dict):
            raise ValueError("HTTP sizing request parameters must be an object")
    if expected_hash is not None and _sha256(_canonical_json(request)) != expected_hash:
        raise ValueError("sizing request hash does not match canonical request")


def _validate_hash(value: str | None, label: str) -> None:
    if value is not None and re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise ValueError(f"{label} must be a SHA-256 hexadecimal digest")


def _validate_credential_reference(value: str | None) -> None:
    if value is None:
        return
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", value) is None:
        raise ValueError("credential references must be opaque uppercase names")


def _validate_relative_path(value: str) -> None:
    if not value or "\\" in value:
        raise ValueError("artifact paths must be non-empty POSIX relative paths")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or value == ".":
        raise ValueError("artifact paths must be relative and may not traverse parents")


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
        "V_HIGH": high["V"],
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
    elif syntax == "pubmed_title_abstract_leaf_scoped":
        query_text = _render_pubmed_title_abstract(expression)
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
    elif syntax == "semantic_scholar_bulk_symbolic_boolean":
        query_text = _render_semantic_scholar_bulk(expression)
        uncertainties.append("bulk_boolean_semantics_control_required")
        request = {
            "method": "GET",
            "endpoint": "paper/search/bulk",
            "params": {
                "query": query_text,
                "limit": 1,
                "fields": "paperId",
                "sort": "paperId:asc",
            },
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
    if (
        source == "PubMed"
        and source_spec.get("sizing_transport") == "form_post"
    ):
        request = {
            "method": "POST",
            "endpoint": "esearch.fcgi",
            "params": {},
            "form": dict(request["params"]),
            "headers": {"content-type": "application/x-www-form-urlencoded"},
        }
        query_hash = _sha256(
            _canonical_json({"query_text": query_text, "fields": fields, "request": request})
        )
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
        parser_contract=str(source_spec.get("parser_contract", "legacy_v0_1")),
    )


def _disjunction(terms: list[str]) -> str:
    if not terms:
        raise CandidateConfigurationError("query term group cannot be empty")
    return "(" + " OR ".join(terms) + ")"


def _render_pubmed_title_abstract(expression: str) -> str:
    tokens = re.findall(r'"[^"\n]+"|\(|\)|\bAND\b|\bOR\b|[^\s()]+', expression)
    if not tokens or "".join(tokens).replace(" ", "") != expression.replace(" ", ""):
        raise CandidateConfigurationError("PubMed expression contains an unsupported token")
    output = [
        token if token in {"(", ")", "AND", "OR"} else f"{token}[Title/Abstract]"
        for token in tokens
    ]
    rendered = " ".join(output)
    return rendered.replace("( ", "(").replace(" )", ")")


@dataclass(frozen=True, slots=True)
class _BooleanExpressionNode:
    kind: str
    value: str | None = None
    children: tuple[_BooleanExpressionNode, ...] = ()


class _BooleanExpressionParser:
    def __init__(self, expression: str):
        token_pattern = re.compile(
            r'"[^"\n]+"|\(|\)|\bAND\b|\bOR\b|\bNOT\b|[^\s()]+'
        )
        self.tokens = token_pattern.findall(expression + " ")
        if not self.tokens or "".join(self.tokens).replace(" ", "") != expression.replace(
            " ", ""
        ):
            raise CandidateConfigurationError("Boolean expression contains an unsupported token")
        self.index = 0

    def parse(self) -> _BooleanExpressionNode:
        node = self._parse_or()
        if self.index != len(self.tokens):
            raise CandidateConfigurationError("Boolean expression has trailing tokens")
        return node

    def _parse_or(self) -> _BooleanExpressionNode:
        children = [self._parse_and()]
        while self._peek() == "OR":
            self.index += 1
            children.append(self._parse_and())
        return self._combine("or", children)

    def _parse_and(self) -> _BooleanExpressionNode:
        children = [self._parse_unary()]
        while self._peek() == "AND":
            self.index += 1
            children.append(self._parse_unary())
        return self._combine("and", children)

    def _parse_unary(self) -> _BooleanExpressionNode:
        if self._peek() == "NOT":
            self.index += 1
            return _BooleanExpressionNode("not", children=(self._parse_unary(),))
        return self._parse_primary()

    def _parse_primary(self) -> _BooleanExpressionNode:
        token = self._peek()
        if token is None:
            raise CandidateConfigurationError("Boolean expression ended unexpectedly")
        if token == "(":
            self.index += 1
            node = self._parse_or()
            if self._peek() != ")":
                raise CandidateConfigurationError("Boolean expression has unbalanced parentheses")
            self.index += 1
            return node
        if token in {"AND", "OR", ")"}:
            raise CandidateConfigurationError(f"unexpected Boolean token: {token}")
        self.index += 1
        return _BooleanExpressionNode("leaf", value=token)

    def _peek(self) -> str | None:
        return self.tokens[self.index] if self.index < len(self.tokens) else None

    @staticmethod
    def _combine(kind: str, children: list[_BooleanExpressionNode]) -> _BooleanExpressionNode:
        return (
            children[0]
            if len(children) == 1
            else _BooleanExpressionNode(kind, children=tuple(children))
        )


def _render_semantic_scholar_bulk(expression: str) -> str:
    return _render_semantic_scholar_node(_BooleanExpressionParser(expression).parse())


def _render_semantic_scholar_node(node: _BooleanExpressionNode) -> str:
    if node.kind == "leaf":
        if node.value is None:  # pragma: no cover - construction invariant
            raise CandidateConfigurationError("Boolean leaf is empty")
        return node.value
    if node.kind == "not":
        return "-" + _render_semantic_scholar_node(node.children[0])
    operator = " + " if node.kind == "and" else " | "
    return (
        "("
        + operator.join(_render_semantic_scholar_node(item) for item in node.children)
        + ")"
    )


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
    elif isinstance(value, str) and re.search(
        r"(?:api_?key|apikey|authorization|access_token|token)=[^&\s<]+",
        value,
        flags=re.IGNORECASE,
    ):
        raise ValueError("sizing provenance contains a credential value")
