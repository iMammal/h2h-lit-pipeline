"""Deterministic, offline export of H2H graphs to the CytoCave ingest contract."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from h2h_lit.cytocave.models import (
    UNKNOWN,
    CytoCaveExportConfig,
    GraphRelation,
    H2HCytoCaveGraph,
    MechanismNode,
    NodeType,
    PaperNode,
    RelationStyle,
    RelationType,
)

SIDECAR_SCHEMA_VERSION = "h2h-cytocave-sidecar-v1"
EXPORTER_VERSION = "1.1"
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
_DELEGATION_COORDINATES = {f"D{value}": float(value) for value in range(5)}
_MEDIATION_COORDINATES = {f"M{value}": float(value) for value in range(5)}
_LUT_FIELDS = (
    "label",
    "Anatomy",
    "region_name",
    "hemisphere",
    "NodeType",
    "StableID",
    "PaperID",
    "PublicationID",
    "PublicationOrder",
    "PublicationPlaneZ",
    "MechanismID",
    "Delegation",
    "Mediation",
    "Target",
    "AgencyDirection",
    "DisplayType",
    "InteractionModality",
    "Adaptability",
    "ClassificationStatus",
    "ClassificationEvidence",
    "Glyph",
    "GlyphSize",
    "GlyphAspectX",
    "GlyphAspectY",
    "GlyphAspectZ",
    "CoordinateX",
    "CoordinateY",
    "CoordinateZ",
    "CoordinateBasis",
    "HemisphereCompatibilityOnly",
)


class CytoCaveExportError(ValueError):
    """Raised when a graph cannot produce a contract-valid CytoCave dataset."""


@dataclass(frozen=True, slots=True)
class CytoCaveArtifactPaths:
    """Paths written by :func:`export_cytocave_dataset`."""

    index: Path
    topology: Path
    edges: Path
    lut: Path
    manifest: Path


@dataclass(frozen=True, slots=True)
class _ResolvedNode:
    stable_id: str
    node_type: str
    region_name: str
    paper_id: str
    publication_order: int
    publication_plane_z: float
    mechanism_id: str | None
    delegation: str | None
    mediation: str | None
    target: str
    agency_direction: str
    display_type: str
    interaction_modalities: tuple[str, ...]
    adaptability: str
    classification_status: str
    classification_evidence: dict[str, tuple[dict[str, Any], ...]]
    glyph: str
    glyph_size: float
    glyph_aspect: tuple[float, float, float]
    coordinates: tuple[float, float, float]
    coordinate_basis: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _ResolvedRelation:
    source_id: str
    target_id: str
    relation_type: str
    weight: float
    directed: bool
    generated: bool
    metadata: dict[str, Any]


def export_cytocave_dataset(
    graph: H2HCytoCaveGraph,
    output_root: str | Path,
    config: CytoCaveExportConfig,
) -> CytoCaveArtifactPaths:
    """Write a deterministic CytoCave-ready ``data/`` tree and H2H sidecar.

    The returned root can be copied into a CytoCave installation without changing
    CytoCave. The LUT is written at ``data/LookupTable_<atlas>.csv`` because the
    current loader does not resolve it from inside the dataset folder.
    """

    _validate_config(config)
    papers, mechanisms = _validate_graph(graph, config)
    publication_planes = _resolve_publication_planes(
        papers,
        graph.publication_order,
        config.publication_plane_spacing,
    )
    relations = _resolve_relations(papers, mechanisms, graph.relations, config)

    connected_ids = {
        endpoint for relation in relations for endpoint in (relation.source_id, relation.target_id)
    }
    resolved_nodes = _resolve_nodes(
        papers,
        mechanisms,
        connected_ids,
        publication_planes,
        config,
    )
    if resolved_nodes and not relations:
        raise CytoCaveExportError(
            "A headerless sparse triple file cannot establish dimensions for a graph "
            "with nodes but no visible edges"
        )

    node_index = {node.stable_id: index for index, node in enumerate(resolved_nodes)}
    _validate_sparse_dimension(resolved_nodes, relations, node_index)

    stem = config.subject_id
    network_name = f"{stem}_edges.csv"
    topology_name = f"{stem}_topology.csv"
    manifest_name = f"{stem}_manifest.json"
    lut_name = f"LookupTable_{config.atlas_suffix}.csv"

    index_text = _write_csv(
        [["subjectID", "network", "topology"], [config.subject_id, network_name, topology_name]],
        delimiter=",",
    )
    topology_text = _topology_csv(resolved_nodes, config)
    edge_text, emitted_pairs = _edge_csv(relations, node_index)
    lut_text = _lut_csv(resolved_nodes)

    file_contents = {
        "index": index_text,
        "topology": topology_text,
        "edges": edge_text,
        "lut": lut_text,
    }
    manifest = _build_manifest(
        graph=graph,
        config=config,
        nodes=resolved_nodes,
        relations=relations,
        node_index=node_index,
        emitted_pairs=emitted_pairs,
        filenames={
            "index": "index.txt",
            "topology": topology_name,
            "edges": network_name,
            "lut": f"../{lut_name}",
            "manifest": manifest_name,
        },
        file_contents=file_contents,
    )
    manifest_text = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n"

    root = Path(output_root)
    data_directory = root / "data"
    dataset_directory = data_directory / config.dataset_folder
    dataset_directory.mkdir(parents=True, exist_ok=True)
    data_directory.mkdir(parents=True, exist_ok=True)

    paths = CytoCaveArtifactPaths(
        index=dataset_directory / "index.txt",
        topology=dataset_directory / topology_name,
        edges=dataset_directory / network_name,
        lut=data_directory / lut_name,
        manifest=dataset_directory / manifest_name,
    )
    for path, content in (
        (paths.index, index_text),
        (paths.topology, topology_text),
        (paths.edges, edge_text),
        (paths.lut, lut_text),
        (paths.manifest, manifest_text),
    ):
        path.write_text(content, encoding="utf-8", newline="")
    return paths


def graph_from_dict(data: dict[str, Any]) -> H2HCytoCaveGraph:
    """Construct an exporter graph from a JSON-compatible dictionary."""

    def aspect(value: Any) -> tuple[float, float, float] | None:
        return tuple(value) if value is not None else None  # type: ignore[return-value]

    papers = tuple(
        PaperNode(
            id=item["id"],
            title=item["title"],
            authors=tuple(item.get("authors", ())),
            year=item.get("year"),
            doi=item.get("doi"),
            target=item.get("target", UNKNOWN),
            display_type=item.get("display_type", "paper"),
            glyph_size=item.get("glyph_size"),
            glyph_aspect=aspect(item.get("glyph_aspect")),
            classification_status=item.get("classification_status", "provisional"),
            metadata=dict(item.get("metadata", {})),
        )
        for item in data.get("papers", ())
    )
    mechanisms = tuple(
        MechanismNode(
            id=item["id"],
            paper_id=item["paper_id"],
            name=item["name"],
            description=item.get("description", ""),
            delegation=item.get("delegation", UNKNOWN),
            mediation=item.get("mediation", UNKNOWN),
            target=item.get("target", UNKNOWN),
            agency_direction=item.get("agency_direction", UNKNOWN),
            display_type=item.get("display_type", UNKNOWN),
            interaction_modalities=tuple(item.get("interaction_modalities", ())),
            adaptability=item.get("adaptability", UNKNOWN),
            glyph_size=item.get("glyph_size"),
            glyph_aspect=aspect(item.get("glyph_aspect")),
            classification_status=item.get("classification_status", "provisional"),
            classification_evidence={
                field_name: tuple(evidence_items)
                for field_name, evidence_items in item.get("classification_evidence", {}).items()
            },
            metadata=dict(item.get("metadata", {})),
        )
        for item in data.get("mechanisms", ())
    )
    relations = tuple(
        GraphRelation(
            source_id=item["source_id"],
            target_id=item["target_id"],
            relation_type=item["relation_type"],
            weight=item.get("weight"),
            directed=bool(item.get("directed", False)),
            metadata=dict(item.get("metadata", {})),
        )
        for item in data.get("relations", ())
    )
    return H2HCytoCaveGraph(
        papers=papers,
        mechanisms=mechanisms,
        publication_order=tuple(data.get("publication_order", ())),
        relations=relations,
        metadata=dict(data.get("metadata", {})),
    )


def load_graph_json(path: str | Path) -> H2HCytoCaveGraph:
    """Load a hand-curated graph without performing any network operations."""

    with Path(path).open(encoding="utf-8") as stream:
        data = json.load(stream)
    if not isinstance(data, dict):
        raise CytoCaveExportError("Graph JSON must contain one object")
    return graph_from_dict(data)


def _validate_config(config: CytoCaveExportConfig) -> None:
    for name, value in (
        ("dataset_folder", config.dataset_folder),
        ("subject_id", config.subject_id),
        ("atlas_suffix", config.atlas_suffix),
        ("coordinate_name", config.coordinate_name),
    ):
        if not value or not _SAFE_NAME.fullmatch(value):
            raise CytoCaveExportError(f"{name} must match {_SAFE_NAME.pattern!r}")
    for name, value in (
        ("publication_plane_spacing", config.publication_plane_spacing),
        ("unknown_coordinate", config.unknown_coordinate),
        ("default_glyph_size", config.default_glyph_size),
        ("paper_glyph_size", config.paper_glyph_size),
    ):
        _require_finite(value, name)
    if config.default_glyph_size <= 0 or config.paper_glyph_size <= 0:
        raise CytoCaveExportError("Glyph sizes must be positive")
    if config.publication_plane_spacing <= 0:
        raise CytoCaveExportError("publication_plane_spacing must be positive")
    _validate_aspect(config.default_glyph_aspect, "default_glyph_aspect")
    if RelationType.MEMBERSHIP.value not in config.relation_styles:
        raise CytoCaveExportError("relation_styles must configure paper-mechanism membership")
    for relation_type, style in config.relation_styles.items():
        _validate_relation_style(relation_type, style)


def _validate_graph(
    graph: H2HCytoCaveGraph, config: CytoCaveExportConfig
) -> tuple[dict[str, PaperNode], dict[str, MechanismNode]]:
    papers = {paper.id: paper for paper in graph.papers}
    mechanisms = {mechanism.id: mechanism for mechanism in graph.mechanisms}
    if len(papers) != len(graph.papers):
        raise CytoCaveExportError("Paper IDs must be unique")
    if len(mechanisms) != len(graph.mechanisms):
        raise CytoCaveExportError("Mechanism IDs must be unique")
    overlap = set(papers) & set(mechanisms)
    if overlap:
        raise CytoCaveExportError(f"Node IDs must be globally unique: {sorted(overlap)!r}")
    if not papers:
        raise CytoCaveExportError("At least one paper is required")
    for paper in papers.values():
        _require_text(paper.id, "paper.id")
        _require_text(paper.title, f"paper {paper.id!r} title")
        _require_text(
            paper.classification_status,
            f"paper {paper.id!r} classification_status",
        )
        _validate_visual_values(paper, config)
    for mechanism in mechanisms.values():
        _require_text(mechanism.id, "mechanism.id")
        _require_text(mechanism.name, f"mechanism {mechanism.id!r} name")
        if mechanism.paper_id not in papers:
            raise CytoCaveExportError(
                f"Mechanism {mechanism.id!r} references unknown paper {mechanism.paper_id!r}"
            )
        if mechanism.delegation not in {*_DELEGATION_COORDINATES, UNKNOWN}:
            raise CytoCaveExportError(f"Unsupported Delegation code {mechanism.delegation!r}")
        if mechanism.mediation not in {*_MEDIATION_COORDINATES, UNKNOWN}:
            raise CytoCaveExportError(f"Unsupported Mediation code {mechanism.mediation!r}")
        _require_text(
            mechanism.agency_direction,
            f"mechanism {mechanism.id!r} agency_direction",
        )
        _require_text(
            mechanism.adaptability,
            f"mechanism {mechanism.id!r} adaptability",
        )
        _require_text(
            mechanism.classification_status,
            f"mechanism {mechanism.id!r} classification_status",
        )
        for modality in mechanism.interaction_modalities:
            _require_text(modality, f"mechanism {mechanism.id!r} interaction modality")
        for field_name, evidence_items in mechanism.classification_evidence.items():
            _require_text(field_name, f"mechanism {mechanism.id!r} evidence field")
            if not evidence_items:
                raise CytoCaveExportError(
                    f"Evidence list {field_name!r} for mechanism {mechanism.id!r} is empty"
                )
            if not all(isinstance(item, dict) and item for item in evidence_items):
                raise CytoCaveExportError(
                    f"Evidence for mechanism {mechanism.id!r} must contain non-empty objects"
                )
        _validate_visual_values(mechanism, config)
    return papers, mechanisms


def _resolve_publication_planes(
    papers: dict[str, PaperNode],
    publication_order: tuple[str, ...],
    spacing: float,
) -> dict[str, tuple[int, float]]:
    if len(set(publication_order)) != len(publication_order):
        raise CytoCaveExportError("publication_order must contain each paper ID exactly once")

    ordered_ids = set(publication_order)
    paper_ids = set(papers)
    missing = sorted(paper_ids - ordered_ids)
    unknown = sorted(ordered_ids - paper_ids)
    if missing or unknown:
        raise CytoCaveExportError(
            "publication_order must exactly match paper IDs; "
            f"missing={missing!r}, unknown={unknown!r}"
        )

    center = (len(publication_order) - 1) / 2
    return {
        paper_id: (index + 1, (index - center) * spacing)
        for index, paper_id in enumerate(publication_order)
    }


def _validate_visual_values(node: PaperNode | MechanismNode, config: CytoCaveExportConfig) -> None:
    _require_text(node.target, f"node {node.id!r} target")
    _require_text(node.display_type, f"node {node.id!r} display_type")
    if node.glyph_size is not None:
        _require_finite(node.glyph_size, f"node {node.id!r} glyph_size")
        if node.glyph_size <= 0:
            raise CytoCaveExportError(f"Node {node.id!r} glyph_size must be positive")
    if node.glyph_aspect is not None:
        _validate_aspect(node.glyph_aspect, f"node {node.id!r} glyph_aspect")
    if node.display_type in config.glyph_size_lookup:
        size = config.glyph_size_lookup[node.display_type]
        _require_finite(size, f"glyph_size_lookup[{node.display_type!r}]")
        if size <= 0:
            raise CytoCaveExportError("Configured glyph sizes must be positive")
    if node.display_type in config.glyph_aspect_lookup:
        _validate_aspect(
            config.glyph_aspect_lookup[node.display_type],
            f"glyph_aspect_lookup[{node.display_type!r}]",
        )


def _resolve_relations(
    papers: dict[str, PaperNode],
    mechanisms: dict[str, MechanismNode],
    supplied: Iterable[GraphRelation],
    config: CytoCaveExportConfig,
) -> list[_ResolvedRelation]:
    relations = [
        _relation_with_weight(
            GraphRelation(
                source_id=mechanism.paper_id,
                target_id=mechanism.id,
                relation_type=RelationType.MEMBERSHIP.value,
            ),
            config,
            generated=True,
        )
        for mechanism in mechanisms.values()
    ]
    all_node_ids = set(papers) | set(mechanisms)
    for relation in supplied:
        if relation.relation_type == RelationType.MEMBERSHIP.value:
            raise CytoCaveExportError("Membership relations are generated from mechanism.paper_id")
        if relation.source_id not in all_node_ids or relation.target_id not in all_node_ids:
            raise CytoCaveExportError(
                f"Relation endpoints must reference existing nodes: "
                f"{relation.source_id!r}, {relation.target_id!r}"
            )
        if relation.source_id == relation.target_id:
            raise CytoCaveExportError(f"Self edge is not allowed for {relation.source_id!r}")
        relations.append(_relation_with_weight(relation, config, generated=False))

    seen_pairs: dict[tuple[str, str], str] = {}
    for relation in relations:
        pair = tuple(sorted((relation.source_id, relation.target_id)))
        if pair in seen_pairs:
            raise CytoCaveExportError(
                f"Multiple visible relations for node pair {pair!r} would overwrite sparse entries"
            )
        seen_pairs[pair] = relation.relation_type
    return sorted(relations, key=lambda item: (item.source_id, item.target_id, item.relation_type))


def _relation_with_weight(
    relation: GraphRelation, config: CytoCaveExportConfig, *, generated: bool
) -> _ResolvedRelation:
    style = config.relation_styles.get(relation.relation_type)
    if style is None:
        raise CytoCaveExportError(f"No relation style configured for {relation.relation_type!r}")
    weight = style.default if relation.weight is None else relation.weight
    _require_finite(weight, f"weight for {relation.relation_type!r}")
    if weight <= 0:
        raise CytoCaveExportError("Visible edge weights must be positive")
    if not style.minimum <= weight <= style.maximum:
        raise CytoCaveExportError(
            f"Weight {weight} for {relation.relation_type!r} is outside "
            f"[{style.minimum}, {style.maximum}]"
        )
    return _ResolvedRelation(
        source_id=relation.source_id,
        target_id=relation.target_id,
        relation_type=relation.relation_type,
        weight=weight,
        directed=relation.directed,
        generated=generated,
        metadata=relation.metadata,
    )


def _resolve_nodes(
    papers: dict[str, PaperNode],
    mechanisms: dict[str, MechanismNode],
    connected_ids: set[str],
    publication_planes: dict[str, tuple[int, float]],
    config: CytoCaveExportConfig,
) -> list[_ResolvedNode]:
    mechanism_coordinates = {
        mechanism.id: (
            _DELEGATION_COORDINATES.get(mechanism.delegation, config.unknown_coordinate),
            _MEDIATION_COORDINATES.get(mechanism.mediation, config.unknown_coordinate),
            publication_planes[mechanism.paper_id][1],
        )
        for mechanism in mechanisms.values()
    }
    by_paper: dict[str, list[MechanismNode]] = {paper_id: [] for paper_id in papers}
    for mechanism in mechanisms.values():
        by_paper[mechanism.paper_id].append(mechanism)

    nodes: list[_ResolvedNode] = []
    for paper in papers.values():
        publication_order, publication_plane_z = publication_planes[paper.id]
        children = by_paper[paper.id]
        if children:
            x = sum(mechanism_coordinates[item.id][0] for item in children) / len(children)
            y = sum(mechanism_coordinates[item.id][1] for item in children) / len(children)
            basis = "mechanism_centroid"
        else:
            x = config.unknown_coordinate
            y = config.unknown_coordinate
            basis = "unknown_no_mechanisms"
        nodes.append(
            _resolved_node(
                stable_id=paper.id,
                node_type=NodeType.PAPER.value,
                region_name=paper.title,
                paper_id=paper.id,
                publication_order=publication_order,
                publication_plane_z=publication_plane_z,
                mechanism_id=None,
                delegation=None,
                mediation=None,
                target=paper.target,
                agency_direction=UNKNOWN,
                display_type=paper.display_type,
                interaction_modalities=(),
                adaptability=UNKNOWN,
                classification_status=paper.classification_status,
                classification_evidence={},
                glyph_size=paper.glyph_size,
                glyph_aspect=paper.glyph_aspect,
                coordinates=(x, y, publication_plane_z),
                coordinate_basis=basis,
                metadata={
                    "authors": list(paper.authors),
                    "doi": paper.doi,
                    "year": paper.year,
                    **paper.metadata,
                },
                config=config,
            )
        )
    for mechanism in mechanisms.values():
        publication_order, publication_plane_z = publication_planes[mechanism.paper_id]
        basis = (
            "unknown_classification_coordinate"
            if mechanism.delegation == UNKNOWN or mechanism.mediation == UNKNOWN
            else "approved_classification_coordinate"
        )
        nodes.append(
            _resolved_node(
                stable_id=mechanism.id,
                node_type=NodeType.MECHANISM.value,
                region_name=mechanism.name,
                paper_id=mechanism.paper_id,
                publication_order=publication_order,
                publication_plane_z=publication_plane_z,
                mechanism_id=mechanism.id,
                delegation=mechanism.delegation,
                mediation=mechanism.mediation,
                target=mechanism.target,
                agency_direction=mechanism.agency_direction,
                display_type=mechanism.display_type,
                interaction_modalities=mechanism.interaction_modalities,
                adaptability=mechanism.adaptability,
                classification_status=mechanism.classification_status,
                classification_evidence=mechanism.classification_evidence,
                glyph_size=mechanism.glyph_size,
                glyph_aspect=mechanism.glyph_aspect,
                coordinates=mechanism_coordinates[mechanism.id],
                coordinate_basis=basis,
                metadata={"description": mechanism.description, **mechanism.metadata},
                config=config,
            )
        )

    type_rank = {NodeType.PAPER.value: 0, NodeType.MECHANISM.value: 1}
    return sorted(
        nodes,
        key=lambda node: (
            0 if node.stable_id not in connected_ids else 1,
            type_rank[node.node_type],
            node.stable_id,
        ),
    )


def _resolved_node(
    *,
    stable_id: str,
    node_type: str,
    region_name: str,
    paper_id: str,
    publication_order: int,
    publication_plane_z: float,
    mechanism_id: str | None,
    delegation: str | None,
    mediation: str | None,
    target: str,
    agency_direction: str,
    display_type: str,
    interaction_modalities: tuple[str, ...],
    adaptability: str,
    classification_status: str,
    classification_evidence: dict[str, tuple[dict[str, Any], ...]],
    glyph_size: float | None,
    glyph_aspect: tuple[float, float, float] | None,
    coordinates: tuple[float, float, float],
    coordinate_basis: str,
    metadata: dict[str, Any],
    config: CytoCaveExportConfig,
) -> _ResolvedNode:
    default_size = (
        config.paper_glyph_size if node_type == NodeType.PAPER.value else config.default_glyph_size
    )
    resolved_size = glyph_size or config.glyph_size_lookup.get(display_type, default_size)
    resolved_aspect = glyph_aspect or config.glyph_aspect_lookup.get(
        display_type, config.default_glyph_aspect
    )
    return _ResolvedNode(
        stable_id=stable_id,
        node_type=node_type,
        region_name=region_name,
        paper_id=paper_id,
        publication_order=publication_order,
        publication_plane_z=publication_plane_z,
        mechanism_id=mechanism_id,
        delegation=delegation,
        mediation=mediation,
        target=target,
        agency_direction=agency_direction,
        display_type=display_type,
        interaction_modalities=interaction_modalities,
        adaptability=adaptability,
        classification_status=classification_status,
        classification_evidence=classification_evidence,
        glyph=config.glyph_lookup.get(display_type, config.glyph_lookup.get(UNKNOWN, "sphere")),
        glyph_size=resolved_size,
        glyph_aspect=resolved_aspect,
        coordinates=coordinates,
        coordinate_basis=coordinate_basis,
        metadata={key: value for key, value in metadata.items() if value is not None},
    )


def _validate_sparse_dimension(
    nodes: list[_ResolvedNode],
    relations: list[_ResolvedRelation],
    node_index: dict[str, int],
) -> None:
    if not nodes:
        return
    largest_endpoint = max(
        node_index[endpoint]
        for relation in relations
        for endpoint in (relation.source_id, relation.target_id)
    )
    if largest_endpoint != len(nodes) - 1:
        raise CytoCaveExportError("Node ordering failed to expose the full sparse-matrix dimension")


def _topology_csv(nodes: list[_ResolvedNode], config: CytoCaveExportConfig) -> str:
    rows: list[list[Any]] = [["label", config.coordinate_name, "", ""]]
    rows.extend(
        [str(index + 1), *(_format_number(value) for value in node.coordinates)]
        for index, node in enumerate(nodes)
    )
    return _write_csv(rows, delimiter=",")


def _edge_csv(
    relations: list[_ResolvedRelation], node_index: dict[str, int]
) -> tuple[str, dict[tuple[str, str, str], list[list[int]]]]:
    rows: list[list[Any]] = []
    emitted_pairs: dict[tuple[str, str, str], list[list[int]]] = {}
    ordered = sorted(
        relations,
        key=lambda relation: (
            min(node_index[relation.source_id], node_index[relation.target_id]),
            max(node_index[relation.source_id], node_index[relation.target_id]),
            relation.relation_type,
        ),
    )
    for relation in ordered:
        source = node_index[relation.source_id]
        target = node_index[relation.target_id]
        pairs = [[source, target], [target, source]]
        emitted_pairs[(relation.source_id, relation.target_id, relation.relation_type)] = pairs
        rows.extend([[left, right, _format_number(relation.weight)] for left, right in pairs])
    return _write_csv(rows, delimiter=","), emitted_pairs


def _lut_csv(nodes: list[_ResolvedNode]) -> str:
    rows: list[list[Any]] = [list(_LUT_FIELDS)]
    for index, node in enumerate(nodes):
        x, y, z = node.coordinates
        aspect_x, aspect_y, aspect_z = node.glyph_aspect
        rows.append(
            [
                str(index + 1),
                node.target,
                node.region_name,
                "left" if index % 2 == 0 else "right",
                node.node_type,
                node.stable_id,
                node.paper_id,
                node.paper_id,
                str(node.publication_order),
                _format_number(node.publication_plane_z),
                node.mechanism_id or "",
                node.delegation or "not_applicable_paper_centroid",
                node.mediation or "not_applicable_paper_centroid",
                node.target,
                node.agency_direction,
                node.display_type,
                "|".join(node.interaction_modalities) or "IM-UNSPECIFIED",
                node.adaptability,
                node.classification_status,
                json.dumps(
                    node.classification_evidence,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                node.glyph,
                _format_number(node.glyph_size),
                _format_number(aspect_x),
                _format_number(aspect_y),
                _format_number(aspect_z),
                _format_number(x),
                _format_number(y),
                _format_number(z),
                node.coordinate_basis,
                "true",
            ]
        )
    return _write_csv(rows, delimiter=";")


def _build_manifest(
    *,
    graph: H2HCytoCaveGraph,
    config: CytoCaveExportConfig,
    nodes: list[_ResolvedNode],
    relations: list[_ResolvedRelation],
    node_index: dict[str, int],
    emitted_pairs: dict[tuple[str, str, str], list[list[int]]],
    filenames: dict[str, str],
    file_contents: dict[str, str],
) -> dict[str, Any]:
    relation_manifest = []
    for relation in relations:
        key = (relation.source_id, relation.target_id, relation.relation_type)
        relation_manifest.append(
            {
                "directed": relation.directed,
                "emitted_symmetric_pairs": emitted_pairs[key],
                "generated_membership": relation.generated,
                "metadata": relation.metadata,
                "original_source_edge_id": node_index[relation.source_id],
                "original_source_stable_id": relation.source_id,
                "original_target_edge_id": node_index[relation.target_id],
                "original_target_stable_id": relation.target_id,
                "relation_type": relation.relation_type,
                "weight": relation.weight,
            }
        )
    node_manifest = []
    for edge_id, node in enumerate(nodes):
        node_manifest.append(
            {
                "classification": {
                    "delegation": node.delegation,
                    "mediation": node.mediation,
                    "target": node.target,
                    "agency_direction": node.agency_direction,
                    "adaptability": node.adaptability,
                    "classification_evidence": node.classification_evidence,
                    "classification_status": node.classification_status,
                    "interaction_modalities": list(node.interaction_modalities),
                },
                "coordinate_basis": node.coordinate_basis,
                "coordinates": {
                    "x_delegation": node.coordinates[0],
                    "y_epistemic_mediation": node.coordinates[1],
                    "z": node.coordinates[2],
                },
                "edge_id": edge_id,
                "label": str(edge_id + 1),
                "mechanism_id": node.mechanism_id,
                "metadata": node.metadata,
                "node_type": node.node_type,
                "paper_id": node.paper_id,
                "publication": {
                    "PublicationID": node.paper_id,
                    "PublicationOrder": node.publication_order,
                    "PublicationPlaneZ": node.publication_plane_z,
                },
                "region_name": node.region_name,
                "stable_id": node.stable_id,
                "visual": {
                    "display_type": node.display_type,
                    "glyph": node.glyph,
                    "glyph_aspect_x": node.glyph_aspect[0],
                    "glyph_aspect_y": node.glyph_aspect[1],
                    "glyph_aspect_z": node.glyph_aspect[2],
                    "glyph_size": node.glyph_size,
                },
            }
        )
    return {
        "dataset": {
            "atlas_suffix": config.atlas_suffix,
            "dataset_folder": config.dataset_folder,
            "graph_metadata": graph.metadata,
            "subject_id": config.subject_id,
            "publication_order": {
                "centered": True,
                "entries": [
                    {
                        "PublicationID": paper_id,
                        "PublicationOrder": index + 1,
                        "PublicationPlaneZ": (index - (len(graph.publication_order) - 1) / 2)
                        * config.publication_plane_spacing,
                        "title": next(
                            paper.title for paper in graph.papers if paper.id == paper_id
                        ),
                    }
                    for index, paper_id in enumerate(graph.publication_order)
                ],
                "spacing": config.publication_plane_spacing,
                "status": graph.metadata.get("publication_order_status", "provisional"),
            },
        },
        "exporter_version": EXPORTER_VERSION,
        "files": {
            name: {
                "path": filenames[name],
                **({"sha256": _sha256(file_contents[name])} if name in file_contents else {}),
            }
            for name in ("index", "topology", "edges", "lut", "manifest")
        },
        "nodes": node_manifest,
        "relation_types": {
            relation_type: {
                "default": style.default,
                "maximum": style.maximum,
                "minimum": style.minimum,
            }
            for relation_type, style in sorted(config.relation_styles.items())
        },
        "relations": relation_manifest,
        "renderer_compatibility": {
            "anatomy_mirrors_target": True,
            "edge_direction": "symmetric_pairs_for_visibility",
            "hemisphere": {
                "allowed_values": ["left", "right"],
                "assignment": "alternating_topology_row_parity",
                "scientific_meaning": False,
            },
            "sparse_matrix_dimension": {
                "guarantee": "maximum topology row index occurs in an emitted edge",
                "node_count": len(nodes),
            },
        },
        "schema_version": SIDECAR_SCHEMA_VERSION,
        "visual_mappings": {
            "color": {
                "default_field": "Target",
                "lut_compatibility_field": "Anatomy",
                "unknown_value": UNKNOWN,
            },
            "coordinates": {
                "coordinate_system": config.coordinate_name,
                "mechanism_x": "Delegation",
                "mechanism_y": "Epistemic Mediation",
                "mechanism_z": "PublicationPlaneZ",
                "paper_position": "mechanism_xy_centroid_on_publication_plane",
                "paper_z": "PublicationPlaneZ",
                "publication_plane_spacing": config.publication_plane_spacing,
                "unknown_coordinate": config.unknown_coordinate,
            },
            "future_aspect_ratio": {"fields": ["GlyphAspectX", "GlyphAspectY", "GlyphAspectZ"]},
            "shape": {
                "default_field": "DisplayType",
                "glyph_field": "Glyph",
                "glyph_lookup": dict(sorted(config.glyph_lookup.items())),
            },
            "size": {"default_field": "GlyphSize"},
            "supplemental_collaboration_fields": [
                "AgencyDirection",
                "Adaptability",
                "InteractionModality",
                "ClassificationStatus",
                "ClassificationEvidence",
            ],
        },
    }


def _write_csv(rows: Iterable[Iterable[Any]], *, delimiter: str) -> str:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, delimiter=delimiter, lineterminator="\n")
    writer.writerows(rows)
    return stream.getvalue()


def _format_number(value: float) -> str:
    return format(value, ".12g")


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _validate_relation_style(relation_type: str, style: RelationStyle) -> None:
    for name, value in (
        ("minimum", style.minimum),
        ("maximum", style.maximum),
        ("default", style.default),
    ):
        _require_finite(value, f"{relation_type}.{name}")
    if style.minimum <= 0 or style.maximum < style.minimum:
        raise CytoCaveExportError(f"Invalid relation range for {relation_type!r}")
    if not style.minimum <= style.default <= style.maximum:
        raise CytoCaveExportError(f"Default relation weight is out of range for {relation_type!r}")


def _validate_aspect(aspect: tuple[float, float, float], name: str) -> None:
    if len(aspect) != 3:
        raise CytoCaveExportError(f"{name} must contain exactly three values")
    for value in aspect:
        _require_finite(value, name)
        if value <= 0:
            raise CytoCaveExportError(f"{name} values must be positive")


def _require_finite(value: float, name: str) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise CytoCaveExportError(f"{name} must be a finite number")


def _require_text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise CytoCaveExportError(f"{name} must be non-empty text")
