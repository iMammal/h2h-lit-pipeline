"""Typed inputs and configuration for deterministic CytoCave exports."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

UNKNOWN = "unknown"


class NodeType(str, Enum):
    """H2H node types represented in a CytoCave graph."""

    PAPER = "paper"
    MECHANISM = "mechanism"


class RelationType(str, Enum):
    """Initial visible relation classes for an H2H graph."""

    MEMBERSHIP = "paper_mechanism_membership"
    STRONGLY_RELATED = "strongly_related_mechanisms"
    PAPER_LINEAGE = "paper_lineage_extension"
    CITATION = "citation"
    WEAK_SEMANTIC_SIMILARITY = "weak_semantic_similarity"


@dataclass(frozen=True, slots=True)
class PaperNode:
    """A paper anchor whose position is derived from its mechanism nodes."""

    id: str
    title: str
    authors: tuple[str, ...] = ()
    year: int | None = None
    doi: str | None = None
    target: str = UNKNOWN
    display_type: str = "paper"
    glyph_size: float | None = None
    glyph_aspect: tuple[float, float, float] | None = None
    classification_status: str = "provisional"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MechanismNode:
    """One distinct assistance mechanism attached to a paper."""

    id: str
    paper_id: str
    name: str
    description: str = ""
    delegation: str = UNKNOWN
    mediation: str = UNKNOWN
    target: str = UNKNOWN
    agency_direction: str = UNKNOWN
    display_type: str = UNKNOWN
    interaction_modalities: tuple[str, ...] = ()
    adaptability: str = UNKNOWN
    glyph_size: float | None = None
    glyph_aspect: tuple[float, float, float] | None = None
    classification_status: str = "provisional"
    classification_evidence: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GraphRelation:
    """A semantic relation whose original direction is retained in the manifest."""

    source_id: str
    target_id: str
    relation_type: str
    weight: float | None = None
    directed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class H2HCytoCaveGraph:
    """Small paper/mechanism graph accepted by the offline exporter."""

    papers: tuple[PaperNode, ...]
    mechanisms: tuple[MechanismNode, ...]
    publication_order: tuple[str, ...] = ()
    relations: tuple[GraphRelation, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RelationStyle:
    """Allowed range and default renderer weight for one relation class."""

    minimum: float
    maximum: float
    default: float


def default_relation_styles() -> dict[str, RelationStyle]:
    """Return independent defaults so callers can safely customize them."""

    return {
        RelationType.MEMBERSHIP.value: RelationStyle(0.80, 1.00, 0.90),
        RelationType.STRONGLY_RELATED.value: RelationStyle(0.70, 0.85, 0.78),
        RelationType.PAPER_LINEAGE.value: RelationStyle(0.40, 0.60, 0.50),
        RelationType.CITATION.value: RelationStyle(0.10, 0.30, 0.20),
        RelationType.WEAK_SEMANTIC_SIMILARITY.value: RelationStyle(0.02, 0.15, 0.08),
    }


def default_glyph_lookup() -> dict[str, str]:
    """Return the renderer-independent glyph mapping recorded in the sidecar."""

    return {
        "paper": "sphere",
        UNKNOWN: "sphere",
        "VM-DESKTOP2D": "cube",
        "VM-DESKTOP3D": "dodecahedron",
        "VM-LARGE_DISPLAY": "box",
        "VM-MOBILE_TABLET": "cylinder",
        "VM-VR": "icosahedron",
        "VM-AR_MR": "tetrahedron",
        "VM-CAVE": "octahedron",
        "VM-PHYSICAL_HAPTIC": "torus",
        "VM-NONVISUAL_OR_UNSPECIFIED": "sphere",
        "VM-OTHER": "sphere",
    }


@dataclass(frozen=True, slots=True)
class CytoCaveExportConfig:
    """File names, visual mappings, and relation policy for one export."""

    dataset_folder: str
    subject_id: str
    atlas_suffix: str
    coordinate_name: str = "H2HDelegationMediation"
    publication_plane_spacing: float = 1.0
    unknown_coordinate: float = -1.0
    default_glyph_size: float = 1.0
    paper_glyph_size: float = 1.35
    default_glyph_aspect: tuple[float, float, float] = (1.0, 1.0, 1.0)
    glyph_lookup: dict[str, str] = field(default_factory=default_glyph_lookup)
    glyph_size_lookup: dict[str, float] = field(default_factory=dict)
    glyph_aspect_lookup: dict[str, tuple[float, float, float]] = field(default_factory=dict)
    relation_styles: dict[str, RelationStyle] = field(default_factory=default_relation_styles)
