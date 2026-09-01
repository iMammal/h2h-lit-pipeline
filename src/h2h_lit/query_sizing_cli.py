"""Build a deterministic offline STAR query-sizing dry-run report."""

from __future__ import annotations

import argparse
from pathlib import Path

from h2h_lit.query_sizing import (
    DEFAULT_RUN_ID,
    build_sizing_dry_run,
    canonical_json,
    save_sizing_dry_run,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-config",
        default="config/star_query_candidates_v0_1.json",
    )
    parser.add_argument(
        "--sentinel-config",
        default="config/star_query_sentinels_v0_1.json",
    )
    parser.add_argument(
        "--semantic-control-config",
        help="Required only for a v0.2 candidate configuration.",
    )
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument(
        "--created-at",
        help="Explicit UTC provenance timestamp; defaults to the sentinel freeze timestamp.",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args(argv)

    report = build_sizing_dry_run(
        args.candidate_config,
        args.sentinel_config,
        run_id=args.run_id,
        created_at=args.created_at,
        semantic_control_config=args.semantic_control_config,
    )
    if args.output:
        save_sizing_dry_run(report, args.output)
    else:
        print(canonical_json(report, pretty=not args.compact))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    raise SystemExit(main())
