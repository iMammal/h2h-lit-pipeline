"""Run the bounded evidence-ID repair for the stopped 250-record fast-track batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.title_abstract_fast_track_batch import Pricing, RunSettings
from h2h_lit.title_abstract_fast_track_repair import run_repair


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, required=True)
    parser.add_argument("--batch-sha256", required=True)
    parser.add_argument("--original-run-dir", type=Path, required=True)
    parser.add_argument("--prior-repair-run-dir", type=Path)
    parser.add_argument("--prompt", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--provider", choices=["OpenAI"], required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--hard-spending-cap-usd", type=float, required=True)
    parser.add_argument("--input-usd-per-million", type=float, required=True)
    parser.add_argument("--cached-input-usd-per-million", type=float, required=True)
    parser.add_argument("--cache-write-usd-per-million", type=float, required=True)
    parser.add_argument("--output-usd-per-million", type=float, required=True)
    parser.add_argument("--max-output-tokens", type=int, default=2500)
    parser.add_argument("--reasoning-effort", default="medium")
    parser.add_argument("--verbosity", default="low")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--retry-limit", type=int, default=1)
    parser.add_argument("--api-key-environment", default="OPENAI_API_KEY")
    args = parser.parse_args()

    settings = RunSettings(
        provider_name=args.provider,
        model=args.model,
        hard_spending_cap_usd=args.hard_spending_cap_usd,
        pricing=Pricing(
            input_usd_per_million=args.input_usd_per_million,
            cached_input_usd_per_million=args.cached_input_usd_per_million,
            cache_write_usd_per_million=args.cache_write_usd_per_million,
            output_usd_per_million=args.output_usd_per_million,
        ),
        max_output_tokens=args.max_output_tokens,
        reasoning_effort=args.reasoning_effort,
        verbosity=args.verbosity,
        concurrency=args.concurrency,
        timeout_seconds=args.timeout_seconds,
        retry_limit=args.retry_limit,
        smoke_count=3,
        service_tier="default",
    )
    provider = OpenAIResponsesProvider.from_environment(
        variable=args.api_key_environment,
        timeout_seconds=args.timeout_seconds,
    )
    report = run_repair(
        batch_path=args.batch,
        original_run_dir=args.original_run_dir,
        output_dir=args.output_dir,
        prompt_path=args.prompt,
        settings=settings,
        provider=provider,
        expected_batch_sha256=args.batch_sha256,
        prior_repair_run_dir=args.prior_repair_run_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
