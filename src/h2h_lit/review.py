"""Prospective provenance and review-decision substrate for the revised STAR."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from typing import Any, TypeVar

from h2h_lit.dedupe import record_key
from h2h_lit.models import LiteratureRecord, ProcessingStatus


class ActorType(str, Enum):
    SOFTWARE = "software"
    LLM = "llm"
    HUMAN = "human"
    ADJUDICATOR = "adjudicator"


class DecisionAuthority(str, Enum):
    DETERMINISTIC = "deterministic"
    PROPOSED = "proposed"
    DECIDED = "decided"
    ADJUDICATED = "adjudicated"


class DecisionScope(str, Enum):
    PROSPECTIVE = "prospective"
    HISTORICAL = "historical"


class DedupeOutcome(str, Enum):
    UNIQUE = "unique"
    DUPLICATE = "duplicate"


class TriState(str, Enum):
    YES = "YES"
    NO = "NO"
    UNCERTAIN = "UNCERTAIN"


class EligibilityStatus(str, Enum):
    ELIGIBLE = "ELIGIBLE"
    EXCLUDED = "EXCLUDED"
    UNCERTAIN = "UNCERTAIN"


class ScreeningStage(str, Enum):
    TITLE_ABSTRACT = "title_abstract"
    FULL_TEXT = "full_text"


class InferenceValidationStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"


class EligibilityCriterion(str, Enum):
    LIFE_SCIENCE_APPLICATION = "E1_life_science_application"
    RELATIONAL_MULTISCALE_RELEVANCE = "E2_relational_multiscale_relevance"
    INTERACTIVE_VISUAL_ANALYTICS = "E3_interactive_visual_analytics"
    COMPUTATIONAL_ASSISTANCE = "E4_computational_assistance"
    HUMAN_ANALYTIC_RELATIONSHIP = "E5_human_analytic_relationship"
    ADMINISTRATIVE_SCOPE = "E6_administrative_scope"
    EVIDENCE_SUFFICIENCY = "E7_evidence_sufficiency"


class ExclusionReason(str, Enum):
    AFTER_RETRIEVAL_END_DATE = "EX_AFTER_RETRIEVAL_END_DATE"
    NON_ENGLISH_FULL_TEXT = "EX_NON_ENGLISH_FULL_TEXT"
    INELIGIBLE_DOCUMENT_TYPE = "EX_INELIGIBLE_DOCUMENT_TYPE"
    NO_LIFE_SCIENCE_APPLICATION = "EX_NO_LIFE_SCIENCE_APPLICATION"
    NO_RELATIONAL_OR_MULTISCALE_RELEVANCE = "EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE"
    NO_INTERACTIVE_VISUAL_ANALYTICS = "EX_NO_INTERACTIVE_VISUAL_ANALYTICS"
    NO_QUALIFYING_ASSISTANCE = "EX_NO_QUALIFYING_ASSISTANCE"
    NO_HUMAN_ANALYTIC_RELATIONSHIP = "EX_NO_HUMAN_ANALYTIC_RELATIONSHIP"
    INSUFFICIENT_EVIDENCE_AFTER_ESCALATION = "EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION"
    OTHER_PROTOCOL_REASON = "EX_OTHER_PROTOCOL_REASON"


class AnnotationDimension(str, Enum):
    ASSISTANCE_MODE = "assistance_mode"
    VISUALIZATION_MODALITY = "visualization_modality"
    TASK = "task"


class AnnotationState(str, Enum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    UNCERTAIN = "UNCERTAIN"


class AssistanceMode(str, Enum):
    ALGORITHMIC = "Algorithmic"
    ADAPTIVE = "Adaptive"
    CONVERSATIONAL = "Conversational"
    IMMERSIVE = "Immersive"


class VisualizationModality(str, Enum):
    DESKTOP_2D = "Desktop 2D"
    LARGE_DISPLAY = "Large Display"
    VR = "VR"
    AR_MR = "AR/MR"
    CAVE = "CAVE"


class TaskCategory(str, Enum):
    NAVIGATION_MULTISCALE_ORIENTATION = "Navigation and Multiscale Orientation"
    COMPARISON_DIFFERENTIATION = "Comparison and Differentiation"
    SELECTION_FILTERING_PRECISION = "Selection, Filtering, and Precision Interaction"
    SENSEMAKING_HYPOTHESIS = "Sensemaking and Hypothesis Development"
    COORDINATION_COLLABORATIVE_REASONING = "Coordination and Collaborative Reasoning"


class SynthesisPriority(str, Enum):
    CORE = "CORE"
    SUPPORTING = "SUPPORTING"
    CONTEXTUAL = "CONTEXTUAL"


class EvidenceSource(str, Enum):
    METADATA = "metadata"
    TITLE_ABSTRACT = "title_abstract"
    FULL_TEXT = "full_text"
    HISTORICAL_ARTIFACT = "historical_artifact"
    OTHER = "other"


@dataclass(slots=True)
class DecisionActor:
    actor_id: str
    actor_type: ActorType
    display_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DecisionActor:
        return cls(
            actor_id=data["actor_id"],
            actor_type=ActorType(data["actor_type"]),
            display_name=data.get("display_name"),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(slots=True)
class DecisionProvenance:
    actor: DecisionActor
    authority: DecisionAuthority
    scope: DecisionScope
    protocol_version: str
    rubric_version: str
    created_at: str
    supersedes_ids: list[str] = field(default_factory=list)
    source_artifact_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DecisionProvenance:
        return cls(
            actor=DecisionActor.from_dict(data["actor"]),
            authority=DecisionAuthority(data["authority"]),
            scope=DecisionScope(data["scope"]),
            protocol_version=data["protocol_version"],
            rubric_version=data["rubric_version"],
            created_at=data["created_at"],
            supersedes_ids=list(data.get("supersedes_ids", [])),
            source_artifact_id=data.get("source_artifact_id"),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(slots=True)
class SourceQuery:
    query_id: str
    source_database: str
    query_text: str
    retrieval_started_at: str
    retrieval_ended_at: str
    status: ProcessingStatus = ProcessingStatus.OK
    run_id: str | None = None
    endpoint: str | None = None
    query_version: str | None = None
    page: int | None = None
    cursor: str | None = None
    result_count: int | None = None
    fields: list[str] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)
    software_version: str | None = None
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceQuery:
        payload = dict(data)
        payload["status"] = ProcessingStatus(payload.get("status", ProcessingStatus.OK.value))
        return cls(**payload)


@dataclass(slots=True)
class RecordOccurrence:
    occurrence_id: str
    source_query_id: str
    source_identifier: str
    retrieved_at: str
    record: LiteratureRecord
    source_rank: int | None = None
    page: int | None = None
    cursor: str | None = None
    raw_payload_hash: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RecordOccurrence:
        payload = dict(data)
        payload["record"] = LiteratureRecord.from_dict(payload["record"])
        return cls(**payload)


@dataclass(slots=True)
class CanonicalRecord:
    canonical_id: str
    survivor_occurrence_id: str
    occurrence_ids: list[str]
    record: LiteratureRecord
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CanonicalRecord:
        payload = dict(data)
        payload["record"] = LiteratureRecord.from_dict(payload["record"])
        return cls(**payload)


@dataclass(slots=True)
class DuplicateDecision:
    decision_id: str
    occurrence_id: str
    canonical_record_id: str
    survivor_occurrence_id: str
    outcome: DedupeOutcome
    match_key: str
    match_rule: str
    provenance: DecisionProvenance

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DuplicateDecision:
        payload = dict(data)
        payload["outcome"] = DedupeOutcome(payload["outcome"])
        payload["provenance"] = DecisionProvenance.from_dict(payload["provenance"])
        return cls(**payload)


@dataclass(slots=True)
class InferenceRun:
    run_id: str
    stage: ScreeningStage
    provider: str
    model: str
    parameters: dict[str, Any]
    prompt_name: str
    prompt_version: str
    prompt_hash: str
    prompt_path: str
    created_at: str
    output_schema_version: str = "1.0.0"
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> InferenceRun:
        payload = dict(data)
        payload["stage"] = ScreeningStage(payload["stage"])
        return cls(**payload)


@dataclass(slots=True)
class InferenceAttempt:
    attempt_id: str
    request_id: str
    run_id: str
    canonical_record_id: str
    stage: ScreeningStage
    attempt_number: int
    started_at: str
    ended_at: str
    input_hash: str
    input_snapshot: dict[str, Any]
    raw_response: str
    validation_status: InferenceValidationStatus
    parsed_response: dict[str, Any] | None = None
    validation_errors: list[str] = field(default_factory=list)
    retry_of_attempt_id: str | None = None
    prior_screening_decision_id: str | None = None
    screening_decision_id: str | None = None
    annotation_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> InferenceAttempt:
        payload = dict(data)
        payload["stage"] = ScreeningStage(payload["stage"])
        payload["validation_status"] = InferenceValidationStatus(
            payload["validation_status"]
        )
        return cls(**payload)


@dataclass(slots=True)
class EvidenceReference:
    evidence_id: str
    canonical_record_id: str
    source: EvidenceSource
    locator: str
    quote: str | None = None
    artifact_id: str | None = None
    content_hash: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EvidenceReference:
        payload = dict(data)
        payload["source"] = EvidenceSource(payload["source"])
        return cls(**payload)


@dataclass(slots=True)
class CriterionDecision:
    criterion: EligibilityCriterion
    value: TriState
    evidence_ids: list[str] = field(default_factory=list)
    rationale: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CriterionDecision:
        return cls(
            criterion=EligibilityCriterion(data["criterion"]),
            value=TriState(data["value"]),
            evidence_ids=list(data.get("evidence_ids", [])),
            rationale=data.get("rationale", ""),
        )


@dataclass(slots=True)
class ScreeningDecision:
    decision_id: str
    canonical_record_id: str
    stage: ScreeningStage
    criteria: list[CriterionDecision]
    status: EligibilityStatus
    provenance: DecisionProvenance
    primary_exclusion_reason: ExclusionReason | None = None
    secondary_exclusion_reasons: list[ExclusionReason] = field(default_factory=list)
    technical_failure_criteria: list[EligibilityCriterion] = field(default_factory=list)
    technical_errors: list[str] = field(default_factory=list)
    notes: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScreeningDecision:
        return cls(
            decision_id=data["decision_id"],
            canonical_record_id=data["canonical_record_id"],
            stage=ScreeningStage(data["stage"]),
            criteria=[CriterionDecision.from_dict(item) for item in data["criteria"]],
            status=EligibilityStatus(data["status"]),
            provenance=DecisionProvenance.from_dict(data["provenance"]),
            primary_exclusion_reason=(
                ExclusionReason(data["primary_exclusion_reason"])
                if data.get("primary_exclusion_reason")
                else None
            ),
            secondary_exclusion_reasons=[
                ExclusionReason(value) for value in data.get("secondary_exclusion_reasons", [])
            ],
            technical_failure_criteria=[
                EligibilityCriterion(value)
                for value in data.get("technical_failure_criteria", [])
            ],
            technical_errors=list(data.get("technical_errors", [])),
            notes=data.get("notes", ""),
        )


@dataclass(slots=True)
class CorpusMembership:
    decision_id: str
    canonical_record_id: str
    status: EligibilityStatus
    screening_decision_id: str
    provenance: DecisionProvenance

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CorpusMembership:
        payload = dict(data)
        payload["status"] = EligibilityStatus(payload["status"])
        payload["provenance"] = DecisionProvenance.from_dict(payload["provenance"])
        return cls(**payload)


@dataclass(slots=True)
class DimensionAnnotation:
    annotation_id: str
    canonical_record_id: str
    dimension: AnnotationDimension
    value: str
    state: AnnotationState
    provenance: DecisionProvenance
    evidence_ids: list[str] = field(default_factory=list)
    system_id: str | None = None
    rationale: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DimensionAnnotation:
        return cls(
            annotation_id=data["annotation_id"],
            canonical_record_id=data["canonical_record_id"],
            dimension=AnnotationDimension(data["dimension"]),
            value=data["value"],
            state=AnnotationState(data["state"]),
            provenance=DecisionProvenance.from_dict(data["provenance"]),
            evidence_ids=list(data.get("evidence_ids", [])),
            system_id=data.get("system_id"),
            rationale=data.get("rationale", ""),
        )


@dataclass(slots=True)
class SynthesisPriorityDecision:
    decision_id: str
    canonical_record_id: str
    priority: SynthesisPriority
    provenance: DecisionProvenance
    evidence_ids: list[str] = field(default_factory=list)
    rationale: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SynthesisPriorityDecision:
        return cls(
            decision_id=data["decision_id"],
            canonical_record_id=data["canonical_record_id"],
            priority=SynthesisPriority(data["priority"]),
            provenance=DecisionProvenance.from_dict(data["provenance"]),
            evidence_ids=list(data.get("evidence_ids", [])),
            rationale=data.get("rationale", ""),
        )


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


DecisionT = TypeVar("DecisionT")


def _effective_decisions(items: Iterable[DecisionT]) -> list[DecisionT]:
    prospective = [
        item for item in items if item.provenance.scope is DecisionScope.PROSPECTIVE  # type: ignore[attr-defined]
    ]
    superseded = {
        decision_id
        for item in prospective
        for decision_id in item.provenance.supersedes_ids  # type: ignore[attr-defined]
    }
    return [item for item in prospective if _item_decision_id(item) not in superseded]


@dataclass(slots=True)
class ReviewDataset:
    schema_version: str = "1.0.0"
    source_queries: list[SourceQuery] = field(default_factory=list)
    occurrences: list[RecordOccurrence] = field(default_factory=list)
    canonical_records: list[CanonicalRecord] = field(default_factory=list)
    duplicate_decisions: list[DuplicateDecision] = field(default_factory=list)
    inference_runs: list[InferenceRun] = field(default_factory=list)
    inference_attempts: list[InferenceAttempt] = field(default_factory=list)
    evidence: list[EvidenceReference] = field(default_factory=list)
    screening_decisions: list[ScreeningDecision] = field(default_factory=list)
    corpus_memberships: list[CorpusMembership] = field(default_factory=list)
    annotations: list[DimensionAnnotation] = field(default_factory=list)
    synthesis_priorities: list[SynthesisPriorityDecision] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return _serialize(self)

    def to_json(self) -> str:
        self.validate()
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReviewDataset:
        return cls(
            schema_version=data.get("schema_version", "1.0.0"),
            source_queries=[SourceQuery.from_dict(item) for item in data.get("source_queries", [])],
            occurrences=[RecordOccurrence.from_dict(item) for item in data.get("occurrences", [])],
            canonical_records=[
                CanonicalRecord.from_dict(item) for item in data.get("canonical_records", [])
            ],
            duplicate_decisions=[
                DuplicateDecision.from_dict(item) for item in data.get("duplicate_decisions", [])
            ],
            inference_runs=[
                InferenceRun.from_dict(item) for item in data.get("inference_runs", [])
            ],
            inference_attempts=[
                InferenceAttempt.from_dict(item)
                for item in data.get("inference_attempts", [])
            ],
            evidence=[EvidenceReference.from_dict(item) for item in data.get("evidence", [])],
            screening_decisions=[
                ScreeningDecision.from_dict(item) for item in data.get("screening_decisions", [])
            ],
            corpus_memberships=[
                CorpusMembership.from_dict(item) for item in data.get("corpus_memberships", [])
            ],
            annotations=[
                DimensionAnnotation.from_dict(item) for item in data.get("annotations", [])
            ],
            synthesis_priorities=[
                SynthesisPriorityDecision.from_dict(item)
                for item in data.get("synthesis_priorities", [])
            ],
        )

    @classmethod
    def from_json(cls, text: str) -> ReviewDataset:
        dataset = cls.from_dict(json.loads(text))
        dataset.validate()
        return dataset

    def effective_screening_decisions(self) -> list[ScreeningDecision]:
        return _effective_decisions(self.screening_decisions)

    def effective_duplicate_decisions(self) -> list[DuplicateDecision]:
        return _effective_decisions(self.duplicate_decisions)

    def effective_corpus_memberships(self) -> list[CorpusMembership]:
        return _effective_decisions(self.corpus_memberships)

    def validate(self) -> None:
        queries = _unique_by_id(self.source_queries, "query_id", "source query")
        occurrences = _unique_by_id(self.occurrences, "occurrence_id", "occurrence")
        canonical = _unique_by_id(self.canonical_records, "canonical_id", "canonical record")
        evidence = _unique_by_id(self.evidence, "evidence_id", "evidence reference")

        occurrence_counts = {query_id: 0 for query_id in queries}
        for occurrence in self.occurrences:
            if occurrence.source_query_id not in queries:
                raise ValueError(
                    f"occurrence {occurrence.occurrence_id} references missing source query "
                    f"{occurrence.source_query_id}"
                )
            query = queries[occurrence.source_query_id]
            occurrence_counts[occurrence.source_query_id] += 1
            if occurrence.page != query.page or occurrence.cursor != query.cursor:
                raise ValueError(
                    f"occurrence {occurrence.occurrence_id} page/cursor disagrees with its query"
                )

        for query in self.source_queries:
            actual_count = occurrence_counts[query.query_id]
            if query.result_count is not None and query.result_count != actual_count:
                raise ValueError(
                    f"source query {query.query_id} result_count={query.result_count} "
                    f"but has {actual_count} occurrences"
                )
            if query.status is ProcessingStatus.FAILED:
                if actual_count:
                    raise ValueError("failed source queries cannot contain occurrences")
                if not query.errors:
                    raise ValueError("failed source queries must preserve at least one error")

        for item in self.evidence:
            if item.canonical_record_id not in canonical:
                raise ValueError(f"evidence {item.evidence_id} references missing canonical record")

        self._validate_decision_histories()
        self._validate_deduplication(occurrences, canonical)
        self._validate_screening(canonical, evidence)
        self._validate_inference(canonical)
        self._validate_membership(canonical)
        self._validate_annotations(canonical, evidence)
        self._validate_priorities(canonical, evidence)

    def _validate_decision_histories(self) -> None:
        collections: list[tuple[str, list[Any], Callable[[Any], tuple[Any, ...]]]] = [
            ("duplicate", self.duplicate_decisions, lambda item: (item.occurrence_id,)),
            ("screening", self.screening_decisions, lambda item: (item.canonical_record_id,)),
            ("membership", self.corpus_memberships, lambda item: (item.canonical_record_id,)),
            (
                "annotation",
                self.annotations,
                lambda item: (
                    item.canonical_record_id,
                    item.system_id,
                    item.dimension.value,
                    item.value,
                ),
            ),
            ("priority", self.synthesis_priorities, lambda item: (item.canonical_record_id,)),
        ]
        all_ids: set[str] = set()
        for name, items, identity in collections:
            by_id = _unique_by_id(items, _decision_id_field(name), f"{name} decision")
            overlap = all_ids.intersection(by_id)
            if overlap:
                raise ValueError(f"decision IDs are not globally unique: {sorted(overlap)}")
            all_ids.update(by_id)
            _validate_history(name, items, identity)

    def _validate_deduplication(
        self,
        occurrences: dict[str, RecordOccurrence],
        canonical: dict[str, CanonicalRecord],
    ) -> None:
        effective = _effective_decisions(self.duplicate_decisions)
        decisions_by_occurrence: dict[str, list[DuplicateDecision]] = {}
        for decision in effective:
            decisions_by_occurrence.setdefault(decision.occurrence_id, []).append(decision)

        if set(decisions_by_occurrence) != set(occurrences):
            missing = sorted(set(occurrences) - set(decisions_by_occurrence))
            extra = sorted(set(decisions_by_occurrence) - set(occurrences))
            raise ValueError(f"every occurrence needs one dedupe decision; missing={missing}, extra={extra}")
        if any(len(items) != 1 for items in decisions_by_occurrence.values()):
            raise ValueError("every occurrence must have exactly one effective dedupe decision")

        assigned: dict[str, str] = {}
        for occurrence_id, items in decisions_by_occurrence.items():
            decision = items[0]
            if decision.canonical_record_id not in canonical:
                raise ValueError(f"dedupe decision {decision.decision_id} has no canonical survivor")
            if decision.survivor_occurrence_id not in occurrences:
                raise ValueError(f"dedupe decision {decision.decision_id} has missing survivor occurrence")
            if decision.outcome is DedupeOutcome.UNIQUE and decision.survivor_occurrence_id != occurrence_id:
                raise ValueError("unique dedupe decisions must identify their own occurrence as survivor")
            if decision.outcome is DedupeOutcome.DUPLICATE and decision.survivor_occurrence_id == occurrence_id:
                raise ValueError("duplicate dedupe decisions must identify a different survivor occurrence")
            assigned[occurrence_id] = decision.canonical_record_id

        survivor_decisions = {item.occurrence_id: item for item in effective}
        for decision in effective:
            survivor = survivor_decisions[decision.survivor_occurrence_id]
            if survivor.outcome is not DedupeOutcome.UNIQUE:
                raise ValueError(f"survivor {survivor.occurrence_id} is not marked unique")
            if survivor.canonical_record_id != decision.canonical_record_id:
                raise ValueError("duplicate and survivor must resolve to the same canonical record")

        listed: dict[str, str] = {}
        for item in self.canonical_records:
            if item.survivor_occurrence_id not in item.occurrence_ids:
                raise ValueError(f"canonical record {item.canonical_id} omits its survivor occurrence")
            for occurrence_id in item.occurrence_ids:
                if occurrence_id not in occurrences:
                    raise ValueError(f"canonical record {item.canonical_id} lists missing occurrence")
                if occurrence_id in listed:
                    raise ValueError(f"occurrence {occurrence_id} appears in multiple canonical records")
                listed[occurrence_id] = item.canonical_id
        if listed != assigned:
            raise ValueError("canonical occurrence lists disagree with effective dedupe decisions")

    def _validate_screening(
        self,
        canonical: dict[str, CanonicalRecord],
        evidence: dict[str, EvidenceReference],
    ) -> None:
        for decision in self.screening_decisions:
            if decision.canonical_record_id not in canonical:
                raise ValueError(f"screening decision {decision.decision_id} has missing record")
            criteria = {item.criterion for item in decision.criteria}
            if len(criteria) != len(decision.criteria) or criteria != set(EligibilityCriterion):
                raise ValueError(f"screening decision {decision.decision_id} must code E1-E7 once")
            for item in decision.criteria:
                if not item.evidence_ids and not item.rationale:
                    raise ValueError(f"criterion {item.criterion.value} needs evidence or rationale")
                self._validate_evidence_ids(
                    item.evidence_ids, decision.canonical_record_id, evidence, decision.decision_id
                )
            criterion_values = {item.criterion: item.value for item in decision.criteria}
            technical_criteria = set(decision.technical_failure_criteria)
            if len(technical_criteria) != len(decision.technical_failure_criteria):
                raise ValueError("technical failure criteria must not contain duplicates")
            if technical_criteria and not decision.technical_errors:
                raise ValueError("technical failure criteria require preserved technical errors")
            if decision.technical_errors and not technical_criteria:
                raise ValueError("technical errors must identify the affected criteria")
            if any(criterion_values[item] is not TriState.UNCERTAIN for item in technical_criteria):
                raise ValueError("technically blocked criteria must remain UNCERTAIN")
            values = [item.value for item in decision.criteria]
            if decision.status is EligibilityStatus.ELIGIBLE and any(
                value is not TriState.YES for value in values
            ):
                raise ValueError("eligible screening decisions require YES for every criterion")
            if decision.status is EligibilityStatus.EXCLUDED and TriState.NO not in values:
                raise ValueError("excluded screening decisions require at least one NO criterion")
            if decision.status is EligibilityStatus.UNCERTAIN and (
                TriState.NO in values or TriState.UNCERTAIN not in values
            ):
                raise ValueError("uncertain screening requires no NO and at least one UNCERTAIN")
            if decision.status is EligibilityStatus.EXCLUDED:
                if decision.primary_exclusion_reason is None:
                    raise ValueError("excluded screening decisions need a primary exclusion reason")
                reasons = [
                    decision.primary_exclusion_reason,
                    *decision.secondary_exclusion_reasons,
                ]
                if ExclusionReason.OTHER_PROTOCOL_REASON in reasons and not decision.notes.strip():
                    raise ValueError("EX_OTHER_PROTOCOL_REASON requires an explanation")
            elif decision.primary_exclusion_reason is not None or decision.secondary_exclusion_reasons:
                raise ValueError("only excluded screening decisions may carry exclusion reasons")

    def _validate_membership(self, canonical: dict[str, CanonicalRecord]) -> None:
        screens = {item.decision_id: item for item in self.screening_decisions}
        for item in self.corpus_memberships:
            if item.canonical_record_id not in canonical:
                raise ValueError(f"membership {item.decision_id} has missing canonical record")
            screen = screens.get(item.screening_decision_id)
            if screen is None:
                raise ValueError(f"membership {item.decision_id} has missing screening decision")
            if screen.provenance.scope is not item.provenance.scope:
                raise ValueError("membership and screening decision scopes must agree")
            if screen.canonical_record_id != item.canonical_record_id or screen.status is not item.status:
                raise ValueError("corpus membership must agree with its screening decision")
        effective_memberships = _effective_decisions(self.corpus_memberships)
        _require_one_effective_per_key(
            "corpus membership", effective_memberships, lambda item: item.canonical_record_id
        )
        effective_screens: dict[str, list[ScreeningDecision]] = {}
        for screen in self.effective_screening_decisions():
            effective_screens.setdefault(screen.canonical_record_id, []).append(screen)
        for membership in effective_memberships:
            candidates = effective_screens.get(membership.canonical_record_id, [])
            if len(candidates) != 1 or candidates[0].decision_id != membership.screening_decision_id:
                raise ValueError(
                    "effective corpus membership requires one effective prospective screening decision"
                )
            if membership.status is EligibilityStatus.UNCERTAIN:
                raise ValueError("unresolved screening cannot create effective corpus membership")

    def _validate_inference(self, canonical: dict[str, CanonicalRecord]) -> None:
        runs = _unique_by_id(self.inference_runs, "run_id", "inference run")
        attempts = _unique_by_id(
            self.inference_attempts, "attempt_id", "inference attempt"
        )
        screens = {item.decision_id: item for item in self.screening_decisions}
        annotations = {item.annotation_id: item for item in self.annotations}
        request_numbers: set[tuple[str, int]] = set()

        for run in self.inference_runs:
            if not all(
                [
                    run.provider.strip(),
                    run.model.strip(),
                    run.prompt_name.strip(),
                    run.prompt_version.strip(),
                    run.prompt_hash.strip(),
                ]
            ):
                raise ValueError(f"inference run {run.run_id} has incomplete model/prompt metadata")

        for attempt in self.inference_attempts:
            run = runs.get(attempt.run_id)
            if run is None:
                raise ValueError(f"inference attempt {attempt.attempt_id} has missing run")
            if attempt.canonical_record_id not in canonical:
                raise ValueError(f"inference attempt {attempt.attempt_id} has missing record")
            if attempt.stage is not run.stage:
                raise ValueError("inference attempt stage must match its run")
            if attempt.request_id != _stable_id(
                "inference-request", attempt.run_id, attempt.input_hash
            ):
                raise ValueError("inference request ID is not stable for its run and input")
            if attempt.attempt_id != _stable_id(
                "inference-attempt", attempt.request_id, str(attempt.attempt_number)
            ):
                raise ValueError("inference attempt ID is not stable for its request number")
            if attempt.attempt_number < 1:
                raise ValueError("inference attempt numbers start at one")
            request_number = (attempt.request_id, attempt.attempt_number)
            if request_number in request_numbers:
                raise ValueError("inference request attempt numbers must be unique")
            request_numbers.add(request_number)
            if _json_hash(attempt.input_snapshot) != attempt.input_hash:
                raise ValueError(f"inference attempt {attempt.attempt_id} input hash mismatch")

            if attempt.attempt_number == 1 and attempt.retry_of_attempt_id is not None:
                raise ValueError("first inference attempts cannot be retries")
            if attempt.attempt_number > 1:
                previous = attempts.get(attempt.retry_of_attempt_id or "")
                if previous is None:
                    raise ValueError("retried inference attempts must reference an earlier attempt")
                if (
                    previous.request_id != attempt.request_id
                    or previous.attempt_number != attempt.attempt_number - 1
                    or previous.run_id != attempt.run_id
                    or previous.canonical_record_id != attempt.canonical_record_id
                    or previous.input_hash != attempt.input_hash
                ):
                    raise ValueError("retry lineage must follow the same request in order")

            if attempt.validation_status is InferenceValidationStatus.INVALID:
                if not attempt.validation_errors:
                    raise ValueError("invalid inference attempts require validation errors")
                if attempt.screening_decision_id or attempt.annotation_ids:
                    raise ValueError("invalid inference attempts cannot create proposals")
                continue

            if attempt.stage is ScreeningStage.TITLE_ABSTRACT:
                if attempt.prior_screening_decision_id is not None:
                    raise ValueError("title/abstract proposals cannot have a prior screening link")
            elif attempt.prior_screening_decision_id is None:
                raise ValueError("full-text proposals require a prior screening link")
            if attempt.validation_errors or attempt.parsed_response is None:
                raise ValueError("valid inference attempts need parsed output without errors")
            screen = screens.get(attempt.screening_decision_id or "")
            if screen is None:
                raise ValueError("valid inference attempts require a screening proposal")
            if (
                screen.canonical_record_id != attempt.canonical_record_id
                or screen.stage is not attempt.stage
                or screen.provenance.actor.actor_type is not ActorType.LLM
                or screen.provenance.authority is not DecisionAuthority.PROPOSED
            ):
                raise ValueError("inference screening outputs must remain LLM proposals")
            if attempt.stage is ScreeningStage.FULL_TEXT and (
                attempt.prior_screening_decision_id not in screen.provenance.supersedes_ids
            ):
                raise ValueError("full-text proposal does not supersede its prior screening decision")
            if len(attempt.annotation_ids) != len(set(attempt.annotation_ids)):
                raise ValueError("inference annotation IDs must not contain duplicates")
            expected_annotations = (
                set()
                if run.output_schema_version == "1.3.0"
                else {
                    *(
                        (AnnotationDimension.ASSISTANCE_MODE, item.value)
                        for item in AssistanceMode
                    ),
                    *(
                        (AnnotationDimension.VISUALIZATION_MODALITY, item.value)
                        for item in VisualizationModality
                    ),
                    *((AnnotationDimension.TASK, item.value) for item in TaskCategory),
                }
            )
            actual_annotations: set[tuple[AnnotationDimension, str]] = set()
            for annotation_id in attempt.annotation_ids:
                annotation = annotations.get(annotation_id)
                if annotation is None:
                    raise ValueError("valid inference attempt references a missing annotation")
                if (
                    annotation.canonical_record_id != attempt.canonical_record_id
                    or annotation.provenance.actor.actor_type is not ActorType.LLM
                    or annotation.provenance.authority is not DecisionAuthority.PROPOSED
                ):
                    raise ValueError("inference annotations must remain LLM proposals")
                actual_annotations.add((annotation.dimension, annotation.value))
            if actual_annotations != expected_annotations:
                raise ValueError("valid inference attempts must code every frozen label")

    def _validate_annotations(
        self,
        canonical: dict[str, CanonicalRecord],
        evidence: dict[str, EvidenceReference],
    ) -> None:
        allowed = {
            AnnotationDimension.ASSISTANCE_MODE: {item.value for item in AssistanceMode},
            AnnotationDimension.VISUALIZATION_MODALITY: {
                item.value for item in VisualizationModality
            },
            AnnotationDimension.TASK: {item.value for item in TaskCategory},
        }
        for item in self.annotations:
            if item.canonical_record_id not in canonical:
                raise ValueError(f"annotation {item.annotation_id} has missing canonical record")
            if item.value not in allowed[item.dimension]:
                raise ValueError(f"invalid {item.dimension.value} value: {item.value}")
            if item.state in {AnnotationState.PRESENT, AnnotationState.UNCERTAIN} and not (
                item.evidence_ids or item.rationale
            ):
                raise ValueError(f"annotation {item.annotation_id} needs evidence or rationale")
            self._validate_evidence_ids(
                item.evidence_ids, item.canonical_record_id, evidence, item.annotation_id
            )
        _require_one_effective_per_key(
            "annotation",
            _effective_decisions(self.annotations),
            lambda item: (item.canonical_record_id, item.system_id, item.dimension, item.value),
        )

    def _validate_priorities(
        self,
        canonical: dict[str, CanonicalRecord],
        evidence: dict[str, EvidenceReference],
    ) -> None:
        memberships = {
            item.canonical_record_id: item for item in self.effective_corpus_memberships()
        }
        for item in self.synthesis_priorities:
            if item.canonical_record_id not in canonical:
                raise ValueError(f"priority {item.decision_id} has missing canonical record")
            if not item.evidence_ids and not item.rationale:
                raise ValueError(f"priority {item.decision_id} needs evidence or rationale")
            self._validate_evidence_ids(
                item.evidence_ids, item.canonical_record_id, evidence, item.decision_id
            )
            if item.provenance.scope is DecisionScope.PROSPECTIVE:
                membership = memberships.get(item.canonical_record_id)
                if membership is None or membership.status is not EligibilityStatus.ELIGIBLE:
                    raise ValueError("synthesis priority requires effective eligible membership")
        _require_one_effective_per_key(
            "synthesis priority",
            _effective_decisions(self.synthesis_priorities),
            lambda item: item.canonical_record_id,
        )

    @staticmethod
    def _validate_evidence_ids(
        evidence_ids: Iterable[str],
        canonical_record_id: str,
        evidence: dict[str, EvidenceReference],
        owner_id: str,
    ) -> None:
        for evidence_id in evidence_ids:
            item = evidence.get(evidence_id)
            if item is None:
                raise ValueError(f"{owner_id} references missing evidence {evidence_id}")
            if item.canonical_record_id != canonical_record_id:
                raise ValueError(f"{owner_id} references evidence for a different record")


def canonicalize_occurrences(
    occurrences: Iterable[RecordOccurrence],
    *,
    provenance: DecisionProvenance,
) -> tuple[list[CanonicalRecord], list[DuplicateDecision]]:
    """Canonicalize without discarding any source occurrence."""

    occurrence_list = list(occurrences)
    _unique_by_id(occurrence_list, "occurrence_id", "occurrence")
    by_key: dict[str, CanonicalRecord] = {}
    canonical_records: list[CanonicalRecord] = []
    decisions: list[DuplicateDecision] = []

    for occurrence in occurrence_list:
        key = record_key(occurrence.record)
        match_rule = "doi_first_title_fallback"
        if not key:
            key = f"occurrence:{occurrence.occurrence_id}"
            match_rule = "occurrence_fallback_missing_doi_and_title"

        canonical = by_key.get(key)
        if canonical is None:
            canonical = CanonicalRecord(
                canonical_id=_stable_id("canonical", key),
                survivor_occurrence_id=occurrence.occurrence_id,
                occurrence_ids=[occurrence.occurrence_id],
                record=LiteratureRecord.from_dict(occurrence.record.to_dict()),
                metadata={"dedupe_key": key},
            )
            by_key[key] = canonical
            canonical_records.append(canonical)
            outcome = DedupeOutcome.UNIQUE
        else:
            canonical.occurrence_ids.append(occurrence.occurrence_id)
            outcome = DedupeOutcome.DUPLICATE

        decisions.append(
            DuplicateDecision(
                decision_id=_stable_id("dedupe", occurrence.occurrence_id, canonical.canonical_id),
                occurrence_id=occurrence.occurrence_id,
                canonical_record_id=canonical.canonical_id,
                survivor_occurrence_id=canonical.survivor_occurrence_id,
                outcome=outcome,
                match_key=key,
                match_rule=match_rule,
                provenance=DecisionProvenance.from_dict(_serialize(provenance)),
            )
        )

    return canonical_records, decisions


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _decision_id_field(name: str) -> str:
    return "annotation_id" if name == "annotation" else "decision_id"


def _item_decision_id(item: Any) -> str:
    return getattr(item, "decision_id", getattr(item, "annotation_id", ""))


def _unique_by_id(items: Iterable[Any], attribute: str, label: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items:
        item_id = getattr(item, attribute)
        if not item_id:
            raise ValueError(f"{label} ID must not be empty")
        if item_id in result:
            raise ValueError(f"duplicate {label} ID: {item_id}")
        result[item_id] = item
    return result


def _validate_history(
    name: str,
    items: list[Any],
    identity: Callable[[Any], tuple[Any, ...]],
) -> None:
    id_attribute = _decision_id_field(name)
    by_id = {getattr(item, id_attribute): item for item in items}
    authority_rank = {
        DecisionAuthority.DETERMINISTIC: 1,
        DecisionAuthority.PROPOSED: 1,
        DecisionAuthority.DECIDED: 2,
        DecisionAuthority.ADJUDICATED: 3,
    }
    actor_authority = {
        ActorType.SOFTWARE: DecisionAuthority.DETERMINISTIC,
        ActorType.LLM: DecisionAuthority.PROPOSED,
        ActorType.HUMAN: DecisionAuthority.DECIDED,
        ActorType.ADJUDICATOR: DecisionAuthority.ADJUDICATED,
    }

    for item in items:
        item_id = getattr(item, id_attribute)
        provenance = item.provenance
        expected = actor_authority[provenance.actor.actor_type]
        if provenance.authority is not expected:
            raise ValueError(
                f"{name} decision {item_id} has actor/authority mismatch: "
                f"{provenance.actor.actor_type.value}/{provenance.authority.value}"
            )
        for previous_id in provenance.supersedes_ids:
            previous = by_id.get(previous_id)
            if previous is None:
                raise ValueError(f"{name} decision {item_id} supersedes missing {previous_id}")
            if identity(previous) != identity(item):
                raise ValueError(f"{name} decision {item_id} supersedes a different subject")
            if previous.provenance.scope is not provenance.scope:
                raise ValueError("historical and prospective decision histories must remain separate")
            if authority_rank[provenance.authority] < authority_rank[previous.provenance.authority]:
                raise ValueError("a lower-authority decision cannot supersede a higher-authority decision")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(item_id: str) -> None:
        if item_id in visiting:
            raise ValueError(f"cycle in {name} decision history")
        if item_id in visited:
            return
        visiting.add(item_id)
        item = by_id[item_id]
        for previous_id in item.provenance.supersedes_ids:
            visit(previous_id)
        visiting.remove(item_id)
        visited.add(item_id)

    for item_id in by_id:
        visit(item_id)


def _require_one_effective_per_key(
    label: str,
    items: Iterable[Any],
    key: Callable[[Any], Any],
) -> None:
    counts: dict[Any, int] = {}
    for item in items:
        item_key = key(item)
        counts[item_key] = counts.get(item_key, 0) + 1
    duplicates = [item_key for item_key, count in counts.items() if count > 1]
    if duplicates:
        raise ValueError(f"multiple effective {label} decisions for {duplicates}")
