"""Command-line entry point for offline CytoCave export."""

from __future__ import annotations

import argparse
from pathlib import Path

from h2h_lit.cytocave import CytoCaveExportConfig, export_cytocave_dataset, load_graph_json


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graph", type=Path, help="Hand-curated H2H graph JSON")
    parser.add_argument("output_root", type=Path, help="Root in which data/ will be written")
    parser.add_argument("--dataset-folder", required=True)
    parser.add_argument("--subject-id", required=True)
    parser.add_argument("--atlas-suffix", required=True)
    parser.add_argument("--publication-plane-spacing", type=float, default=1.0)
    parser.add_argument("--unknown-coordinate", type=float, default=-1.0)
    args = parser.parse_args()

    paths = export_cytocave_dataset(
        load_graph_json(args.graph),
        args.output_root,
        CytoCaveExportConfig(
            dataset_folder=args.dataset_folder,
            subject_id=args.subject_id,
            atlas_suffix=args.atlas_suffix,
            publication_plane_spacing=args.publication_plane_spacing,
            unknown_coordinate=args.unknown_coordinate,
        ),
    )
    for path in (paths.index, paths.topology, paths.edges, paths.lut, paths.manifest):
        print(path)


if __name__ == "__main__":
    main()
