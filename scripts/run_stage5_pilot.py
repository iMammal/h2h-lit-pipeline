"""Prepare or execute the bounded Stage 5 live pilot."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.pilot import (
    DEFAULT_CONFIG,
    DEFAULT_OUTPUT_DIR,
    prepare_pilot,
    run_live_pilot,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--historical-root", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--run-live",
        action="store_true",
        help="Call the configured live provider; without this flag only preflight files are written.",
    )
    parser.add_argument("--api-key-environment", default="OPENAI_API_KEY")
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    args = parser.parse_args()

    _, _, preflight, _ = prepare_pilot(
        config_path=args.config,
        historical_root=args.historical_root,
        output_dir=args.output_dir,
    )
    print(json.dumps(preflight, indent=2, sort_keys=True))
    if not args.run_live:
        return 0

    provider = OpenAIResponsesProvider.from_environment(
        variable=args.api_key_environment,
        timeout_seconds=args.timeout_seconds,
    )
    _, report, paths = run_live_pilot(
        provider=provider,
        config_path=args.config,
        historical_root=args.historical_root,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "report": str(paths.report),
                "review_dataset": str(paths.review_dataset),
                "summary": {
                    "records": report["records"],
                    "attempts": report["attempts"],
                    "valid_response_rate": report["valid_response_rate"],
                    "full_text_escalation_count": report["full_text_escalation_count"],
                },
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
