from __future__ import annotations

import csv
import json
import math
from itertools import pairwise
from pathlib import Path

import pytest

from h2h_lit.cytocave import (
    UNKNOWN,
    CytoCaveExportConfig,
    CytoCaveExportError,
    GraphRelation,
    H2HCytoCaveGraph,
    MechanismNode,
    PaperNode,
    RelationType,
    export_cytocave_dataset,
    load_graph_json,
)

REQUIRED_LUT_COLUMNS = {
    "label",
    "Anatomy",
    "region_name",
    "hemisphere",
    "PublicationID",
    "PublicationOrder",
    "PublicationPlaneZ",
}
DEMO_ROOT = Path("examples/cytocave/foundational_agency")


def _config(**overrides: object) -> CytoCaveExportConfig:
    values = {
        "dataset_folder": "test_dataset",
        "subject_id": "case001",
        "atlas_suffix": "h2h_test",
    }
    values.update(overrides)
    return CytoCaveExportConfig(**values)  # type: ignore[arg-type]


def _small_graph(*, include_isolated: bool = False) -> H2HCytoCaveGraph:
    papers = [PaperNode(id="paper-b", title="Paper B")]
    if include_isolated:
        papers.append(PaperNode(id="zzz-isolated-paper", title="Isolated paper"))
    return H2HCytoCaveGraph(
        papers=tuple(papers),
        mechanisms=(
            MechanismNode(
                id="mechanism-b",
                paper_id="paper-b",
                name="Mechanism B",
                delegation="D2",
                mediation="M3",
                target="T-VIEW",
                display_type="VM-VR",
            ),
        ),
        publication_order=tuple(paper.id for paper in papers),
    )


def _read_csv(path: Path, delimiter: str = ",") -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.reader(stream, delimiter=delimiter))


def _read_manifest(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _demo_graph() -> H2HCytoCaveGraph:
    return load_graph_json(DEMO_ROOT / "source_graph.json")


def test_stable_node_ordering_ids_and_sparse_dimension_guard(tmp_path: Path) -> None:
    graph = _small_graph(include_isolated=True)
    first = export_cytocave_dataset(graph, tmp_path / "first", _config())
    second = export_cytocave_dataset(
        H2HCytoCaveGraph(
            papers=tuple(reversed(graph.papers)),
            mechanisms=tuple(reversed(graph.mechanisms)),
            publication_order=graph.publication_order,
        ),
        tmp_path / "second",
        _config(),
    )

    first_manifest = _read_manifest(first.manifest)
    second_manifest = _read_manifest(second.manifest)
    first_nodes = first_manifest["nodes"]
    second_nodes = second_manifest["nodes"]
    assert [node["stable_id"] for node in first_nodes] == [
        "zzz-isolated-paper",
        "paper-b",
        "mechanism-b",
    ]
    assert first_nodes == second_nodes
    assert [node["edge_id"] for node in first_nodes] == [0, 1, 2]

    edges = _read_csv(first.edges)
    endpoint_ids = {int(row[column]) for row in edges for column in (0, 1)}
    assert max(endpoint_ids) == len(first_nodes) - 1
    assert 0 not in endpoint_ids


def test_lut_coverage_required_columns_and_compatibility_values(tmp_path: Path) -> None:
    paths = export_cytocave_dataset(_small_graph(), tmp_path, _config())
    topology = _read_csv(paths.topology)
    lut = _read_csv(paths.lut, delimiter=";")
    header = lut[0]
    records = [dict(zip(header, row, strict=True)) for row in lut[1:]]

    assert REQUIRED_LUT_COLUMNS <= set(header)
    assert {row[0] for row in topology[1:]} == {record["label"] for record in records}
    assert all(record["Anatomy"] for record in records)
    assert all(record["region_name"] for record in records)
    assert {record["hemisphere"] for record in records} <= {"left", "right"}
    assert all(record["HemisphereCompatibilityOnly"] == "true" for record in records)


def test_topology_has_one_valid_coordinate_block_and_centroid(tmp_path: Path) -> None:
    paths = export_cytocave_dataset(_small_graph(), tmp_path, _config())
    topology = _read_csv(paths.topology)

    assert topology[0] == ["label", "H2HDelegationMediation", "", ""]
    assert all(len(row) == 4 for row in topology)
    assert all(math.isfinite(float(value)) for row in topology[1:] for value in row[1:])
    assert topology[1][1:] == ["2", "3", "0"]
    assert topology[2][1:] == ["2", "3", "0"]


def test_edges_are_headerless_symmetric_positive_and_without_self_edges(tmp_path: Path) -> None:
    paths = export_cytocave_dataset(_small_graph(), tmp_path, _config())
    rows = _read_csv(paths.edges)
    triples = {(int(source), int(target), float(weight)) for source, target, weight in rows}

    assert rows[0][0] != "source"
    assert all(len(row) == 3 for row in rows)
    assert all(source != target for source, target, _ in triples)
    assert all(math.isfinite(weight) and weight > 0 for _, _, weight in triples)
    assert triples == {(1, 0, 0.9), (0, 1, 0.9)}


def test_edge_weight_ranges_by_relation_type(tmp_path: Path) -> None:
    papers = tuple(PaperNode(id=f"p{index}", title=f"Paper {index}") for index in range(1, 6))
    mechanisms = tuple(
        MechanismNode(id=f"m{index}", paper_id=f"p{index}", name=f"Mechanism {index}")
        for index in range(1, 6)
    )
    graph = H2HCytoCaveGraph(
        papers=papers,
        mechanisms=mechanisms,
        publication_order=tuple(paper.id for paper in papers),
        relations=(
            GraphRelation("m1", "m2", RelationType.STRONGLY_RELATED.value, 0.74),
            GraphRelation("p1", "p3", RelationType.PAPER_LINEAGE.value, 0.42, True),
            GraphRelation("p2", "p4", RelationType.CITATION.value, 0.12, True),
            GraphRelation("m3", "m5", RelationType.WEAK_SEMANTIC_SIMILARITY.value, 0.03),
        ),
    )
    paths = export_cytocave_dataset(graph, tmp_path, _config())
    manifest = _read_manifest(paths.manifest)
    styles = manifest["relation_types"]

    seen_types = set()
    for relation in manifest["relations"]:
        relation_type = relation["relation_type"]
        seen_types.add(relation_type)
        style = styles[relation_type]
        assert style["minimum"] <= relation["weight"] <= style["maximum"]
        assert relation["emitted_symmetric_pairs"][0] == list(
            reversed(relation["emitted_symmetric_pairs"][1])
        )
    assert seen_types == {relation_type.value for relation_type in RelationType}


def test_visual_mapping_metadata_and_lut_fields(tmp_path: Path) -> None:
    config = _config(
        glyph_lookup={"paper": "anchor", UNKNOWN: "unknown-glyph", "VM-VR": "vr-glyph"},
        glyph_size_lookup={"VM-VR": 2.25},
        glyph_aspect_lookup={"VM-VR": (2.0, 1.0, 0.5)},
    )
    paths = export_cytocave_dataset(_small_graph(), tmp_path, config)
    manifest = _read_manifest(paths.manifest)
    visual = manifest["visual_mappings"]
    mechanism = next(node for node in manifest["nodes"] if node["node_type"] == "mechanism")

    assert visual["color"]["default_field"] == "Target"
    assert visual["shape"]["default_field"] == "DisplayType"
    assert visual["size"]["default_field"] == "GlyphSize"
    assert visual["future_aspect_ratio"]["fields"] == [
        "GlyphAspectX",
        "GlyphAspectY",
        "GlyphAspectZ",
    ]
    assert mechanism["visual"] == {
        "display_type": "VM-VR",
        "glyph": "vr-glyph",
        "glyph_aspect_x": 2.0,
        "glyph_aspect_y": 1.0,
        "glyph_aspect_z": 0.5,
        "glyph_size": 2.25,
    }

    lut = _read_csv(paths.lut, delimiter=";")
    mechanism_record = next(
        dict(zip(lut[0], row, strict=True)) for row in lut[1:] if "mechanism-b" in row
    )
    assert mechanism_record["Target"] == "T-VIEW"
    assert mechanism_record["DisplayType"] == "VM-VR"
    assert mechanism_record["Glyph"] == "vr-glyph"
    assert mechanism_record["GlyphSize"] == "2.25"
    assert mechanism_record["GlyphAspectX"] == "2"
    assert mechanism_record["GlyphAspectY"] == "1"
    assert mechanism_record["GlyphAspectZ"] == "0.5"
    assert mechanism_record["AgencyDirection"] == "unknown"
    assert mechanism_record["Adaptability"] == "unknown"
    assert mechanism_record["ClassificationStatus"] == "provisional"
    assert "ClassificationEvidence" in mechanism_record


def test_demo_paper_mechanism_decomposition() -> None:
    graph = _demo_graph()
    mechanism_counts = {
        paper.id: sum(mechanism.paper_id == paper.id for mechanism in graph.mechanisms)
        for paper in graph.papers
    }

    assert len(graph.papers) == 11
    assert len(graph.mechanisms) == 26
    assert all(mechanism.paper_id in mechanism_counts for mechanism in graph.mechanisms)
    assert all(count >= 1 for count in mechanism_counts.values())
    assert mechanism_counts == {
        "horvitz-1999": 3,
        "domova-vrotsou-2023": 4,
        "holter-elassady-2024": 1,
        "monadjemi-et-al-2026": 1,
        "icave-2017": 2,
        "biowheel-2017": 2,
        "fathomnet-2022": 3,
        "dtbia-2025": 2,
        "aegis-2018": 2,
        "wang-et-al-2025": 3,
        "phenoflow-2025": 3,
    }


def test_demo_assignments_are_provisional_or_unknown_with_evidence() -> None:
    graph = _demo_graph()
    scalar_fields = (
        "delegation",
        "mediation",
        "target",
        "agency_direction",
        "display_type",
        "adaptability",
    )

    assert all(paper.classification_status == "provisional" for paper in graph.papers)
    assert all(mechanism.classification_status == "provisional" for mechanism in graph.mechanisms)
    assert any(mechanism.delegation == UNKNOWN for mechanism in graph.mechanisms)
    assert any(mechanism.mediation == UNKNOWN for mechanism in graph.mechanisms)
    assert any(mechanism.target == UNKNOWN for mechanism in graph.mechanisms)

    for mechanism in graph.mechanisms:
        for field_name in scalar_fields:
            if getattr(mechanism, field_name) != UNKNOWN:
                assert mechanism.classification_evidence.get(field_name), (
                    mechanism.id,
                    field_name,
                )
        if mechanism.interaction_modalities:
            assert mechanism.classification_evidence.get("interaction_modalities")
        if any(getattr(mechanism, field_name) == UNKNOWN for field_name in scalar_fields):
            assert mechanism.classification_evidence.get("uncertainty")


def test_demo_publication_planes_coordinates_and_metadata_agree(tmp_path: Path) -> None:
    graph = _demo_graph()
    spacing = 1.5
    paths = export_cytocave_dataset(
        graph,
        tmp_path,
        _config(publication_plane_spacing=spacing),
    )
    manifest = _read_manifest(paths.manifest)
    nodes = manifest["nodes"]
    mechanism_nodes = [node for node in nodes if node["node_type"] == "mechanism"]
    paper_nodes = [node for node in nodes if node["node_type"] == "paper"]
    topology = _read_csv(paths.topology)
    lut = _read_csv(paths.lut, delimiter=";")
    lut_records = {
        record["label"]: record
        for record in (dict(zip(lut[0], row, strict=True)) for row in lut[1:])
    }

    assert len(graph.publication_order) == len(graph.papers) == 11
    assert len(set(graph.publication_order)) == len(graph.publication_order)
    assert set(graph.publication_order) == {paper.id for paper in graph.papers}

    publication_entries = manifest["dataset"]["publication_order"]
    assert publication_entries["status"] == "provisional"
    assert publication_entries["spacing"] == spacing
    expected_planes = {
        paper_id: (order, (order - 6) * spacing)
        for order, paper_id in enumerate(graph.publication_order, start=1)
    }
    assert [entry["PublicationID"] for entry in publication_entries["entries"]] == list(
        graph.publication_order
    )
    assert [entry["PublicationPlaneZ"] for entry in publication_entries["entries"]] == [
        plane for _, plane in expected_planes.values()
    ]

    for node in mechanism_nodes:
        delegation = node["classification"]["delegation"]
        mediation = node["classification"]["mediation"]
        expected_x = -1.0 if delegation == UNKNOWN else float(delegation[1:])
        expected_y = -1.0 if mediation == UNKNOWN else float(mediation[1:])
        expected_order, expected_z = expected_planes[node["paper_id"]]
        assert node["coordinates"] == {
            "x_delegation": expected_x,
            "y_epistemic_mediation": expected_y,
            "z": expected_z,
        }
        assert node["publication"] == {
            "PublicationID": node["paper_id"],
            "PublicationOrder": expected_order,
            "PublicationPlaneZ": expected_z,
        }

    for paper in paper_nodes:
        members = [node for node in mechanism_nodes if node["paper_id"] == paper["paper_id"]]
        expected_order, expected_z = expected_planes[paper["paper_id"]]
        assert {node["coordinates"]["z"] for node in members} == {expected_z}
        assert paper["coordinates"] == {
            "x_delegation": sum(node["coordinates"]["x_delegation"] for node in members)
            / len(members),
            "y_epistemic_mediation": sum(
                node["coordinates"]["y_epistemic_mediation"] for node in members
            )
            / len(members),
            "z": expected_z,
        }
        assert paper["publication"]["PublicationOrder"] == expected_order

    assert len({node["coordinates"]["z"] for node in paper_nodes}) == len(paper_nodes)
    z_values = sorted(node["coordinates"]["z"] for node in paper_nodes)
    assert sum(z_values) == pytest.approx(0.0)
    assert all(right - left == pytest.approx(spacing) for left, right in pairwise(z_values))

    for topology_row in topology[1:]:
        label, x, y, z = topology_row
        manifest_node = next(node for node in nodes if node["label"] == label)
        lut_record = lut_records[label]
        assert [float(x), float(y), float(z)] == pytest.approx(
            [
                manifest_node["coordinates"]["x_delegation"],
                manifest_node["coordinates"]["y_epistemic_mediation"],
                manifest_node["coordinates"]["z"],
            ]
        )
        assert lut_record["PublicationID"] == manifest_node["publication"]["PublicationID"]
        assert (
            int(lut_record["PublicationOrder"]) == manifest_node["publication"]["PublicationOrder"]
        )
        assert (
            float(lut_record["PublicationPlaneZ"])
            == manifest_node["publication"]["PublicationPlaneZ"]
        )
        assert float(lut_record["CoordinateZ"]) == float(z)


@pytest.mark.parametrize(
    ("publication_order", "message"),
    [
        (("paper-b", "paper-b"), "exactly once"),
        ((), "missing"),
        (("paper-b", "unknown-paper"), "unknown"),
    ],
)
def test_rejects_invalid_publication_order(
    tmp_path: Path,
    publication_order: tuple[str, ...],
    message: str,
) -> None:
    graph = _small_graph()
    with pytest.raises(CytoCaveExportError, match=message):
        export_cytocave_dataset(
            H2HCytoCaveGraph(
                papers=graph.papers,
                mechanisms=graph.mechanisms,
                publication_order=publication_order,
            ),
            tmp_path,
            _config(),
        )


def test_demo_relation_bands_and_contract_metadata(tmp_path: Path) -> None:
    paths = export_cytocave_dataset(_demo_graph(), tmp_path, _config())
    manifest = _read_manifest(paths.manifest)
    styles = manifest["relation_types"]
    relation_counts: dict[str, int] = {}

    for relation in manifest["relations"]:
        relation_type = relation["relation_type"]
        relation_counts[relation_type] = relation_counts.get(relation_type, 0) + 1
        assert styles[relation_type]["minimum"] <= relation["weight"]
        assert relation["weight"] <= styles[relation_type]["maximum"]
        assert relation["emitted_symmetric_pairs"][0] == list(
            reversed(relation["emitted_symmetric_pairs"][1])
        )

    assert relation_counts == {
        RelationType.MEMBERSHIP.value: 26,
        RelationType.STRONGLY_RELATED.value: 17,
        RelationType.PAPER_LINEAGE.value: 3,
        RelationType.CITATION.value: 2,
        RelationType.WEAK_SEMANTIC_SIMILARITY.value: 2,
    }
    assert manifest["visual_mappings"]["color"]["default_field"] == "Target"
    assert manifest["visual_mappings"]["shape"]["default_field"] == "DisplayType"
    assert (
        "ClassificationEvidence" in manifest["visual_mappings"]["supplemental_collaboration_fields"]
    )


def test_citation_direction_is_preserved_while_csv_is_symmetric(tmp_path: Path) -> None:
    graph = H2HCytoCaveGraph(
        papers=(PaperNode("citing", "Citing"), PaperNode("cited", "Cited")),
        mechanisms=(
            MechanismNode("citing-m", "citing", "Citing mechanism"),
            MechanismNode("cited-m", "cited", "Cited mechanism"),
        ),
        publication_order=("citing", "cited"),
        relations=(GraphRelation("citing", "cited", RelationType.CITATION.value, directed=True),),
    )
    paths = export_cytocave_dataset(graph, tmp_path, _config())
    manifest = _read_manifest(paths.manifest)
    citation = next(
        relation
        for relation in manifest["relations"]
        if relation["relation_type"] == RelationType.CITATION.value
    )

    assert citation["directed"] is True
    assert citation["original_source_stable_id"] == "citing"
    assert citation["original_target_stable_id"] == "cited"
    assert citation["emitted_symmetric_pairs"][0] == list(
        reversed(citation["emitted_symmetric_pairs"][1])
    )


def test_rejects_self_edges_and_out_of_range_weights(tmp_path: Path) -> None:
    base = _small_graph()
    with pytest.raises(CytoCaveExportError, match="Self edge"):
        export_cytocave_dataset(
            H2HCytoCaveGraph(
                papers=base.papers,
                mechanisms=base.mechanisms,
                publication_order=base.publication_order,
                relations=(
                    GraphRelation("paper-b", "paper-b", RelationType.CITATION.value, directed=True),
                ),
            ),
            tmp_path / "self",
            _config(),
        )
    with pytest.raises(CytoCaveExportError, match="outside"):
        export_cytocave_dataset(
            H2HCytoCaveGraph(
                papers=(PaperNode("p1", "P1"), PaperNode("p2", "P2")),
                mechanisms=(
                    MechanismNode("m1", "p1", "M1"),
                    MechanismNode("m2", "p2", "M2"),
                ),
                publication_order=("p1", "p2"),
                relations=(GraphRelation("p1", "p2", RelationType.CITATION.value, weight=0.8),),
            ),
            tmp_path / "range",
            _config(),
        )


def test_repeated_exports_are_byte_identical(tmp_path: Path) -> None:
    first = export_cytocave_dataset(_small_graph(), tmp_path / "first", _config())
    second = export_cytocave_dataset(_small_graph(), tmp_path / "second", _config())

    for field in ("index", "topology", "edges", "lut", "manifest"):
        assert getattr(first, field).read_bytes() == getattr(second, field).read_bytes()


def test_committed_demo_artifacts_match_fresh_export(tmp_path: Path) -> None:
    graph = _demo_graph()
    config = CytoCaveExportConfig(
        dataset_folder="H2H_Foundational_Agency",
        subject_id="foundational_agency",
        atlas_suffix="h2h_foundational_agency",
    )
    fresh = export_cytocave_dataset(graph, tmp_path, config)
    committed = {
        "index": DEMO_ROOT / "data/H2H_Foundational_Agency/index.txt",
        "topology": DEMO_ROOT / "data/H2H_Foundational_Agency/foundational_agency_topology.csv",
        "edges": DEMO_ROOT / "data/H2H_Foundational_Agency/foundational_agency_edges.csv",
        "lut": DEMO_ROOT / "data/LookupTable_h2h_foundational_agency.csv",
        "manifest": DEMO_ROOT / "data/H2H_Foundational_Agency/foundational_agency_manifest.json",
    }
    for field, path in committed.items():
        assert path.read_bytes() == getattr(fresh, field).read_bytes()
