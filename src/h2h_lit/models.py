"""Serializable data and provenance models for the H2H literature pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class ProvenanceKind(str, Enum):
    """Methodological category for a record or pipeline event."""

    SOURCE_DERIVED = "source_derived"
    DETERMINISTIC = "deterministic"
    HEURISTIC = "heuristic"
    LLM_DERIVED = "llm_derived"
    HUMAN_DECISION = "human_decision"
    GENERATED_OUTPUT = "generated_output"


class ProcessingStatus(str, Enum):
    """Common processing status values."""

    OK = "ok"
    SKIPPED = "skipped"
    FAILED = "failed"
    PARTIAL = "partial"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_dict(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _clean_dict(asdict(value))
    if isinstance(value, dict):
        return {k: _clean_dict(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_clean_dict(v) for v in value]
    return value


@dataclass(slots=True)
class InferenceMetadata:
    """Metadata for a model-derived annotation."""

    provider: str
    model: str
    prompt_name: str
    prompt_version: str
    parameters: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict(self)


@dataclass(slots=True)
class ProvenanceEvent:
    """One pipeline event applied to a record or artifact."""

    kind: ProvenanceKind
    stage: str
    status: ProcessingStatus = ProcessingStatus.OK
    timestamp: str = field(default_factory=utc_now_iso)
    software_version: str | None = None
    source_database: str | None = None
    source_query: str | None = None
    source_identifier: str | None = None
    source_url: str | None = None
    input_id: str | None = None
    output_id: str | None = None
    model: str | None = None
    provider: str | None = None
    prompt_name: str | None = None
    prompt_version: str | None = None
    inference_parameters: dict[str, Any] = field(default_factory=dict)
    classification: str | None = None
    reasoning: str | None = None
    retries: int = 0
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProvenanceEvent:
        payload = dict(data)
        payload["kind"] = ProvenanceKind(payload["kind"])
        payload["status"] = ProcessingStatus(payload.get("status", ProcessingStatus.OK.value))
        return cls(**payload)


@dataclass(slots=True)
class LiteratureRecord:
    """Bibliographic record plus transparent provenance metadata."""

    title: str
    abstract: str = ""
    authors: list[str] = field(default_factory=list)
    year: str | int | None = None
    doi: str | None = None
    pmid: str | None = None
    arxiv_id: str | None = None
    source_identifier: str | None = None
    source_database: str | None = None
    source_url: str | None = None
    pdf_url: str | None = None
    journal: str | None = None
    is_open_access: bool | None = None
    original_metadata: dict[str, Any] = field(default_factory=dict)
    provenance: list[ProvenanceEvent] = field(default_factory=list)
    annotations: dict[str, Any] = field(default_factory=dict)

    def dedupe_key(self) -> str:
        from h2h_lit.normalize import dedupe_key

        return dedupe_key(doi=self.doi, title=self.title)

    def add_event(self, event: ProvenanceEvent) -> None:
        self.provenance.append(event)

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LiteratureRecord:
        payload = dict(data)
        payload["provenance"] = [
            event if isinstance(event, ProvenanceEvent) else ProvenanceEvent.from_dict(event)
            for event in payload.get("provenance", [])
        ]
        return cls(**payload)


@dataclass(slots=True)
class LLMAnnotation:
    """An explicit model-derived annotation attached to a literature record."""

    label: str
    value: Any
    inference: InferenceMetadata
    reasoning: str | None = None
    status: ProcessingStatus = ProcessingStatus.OK

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict(self)
