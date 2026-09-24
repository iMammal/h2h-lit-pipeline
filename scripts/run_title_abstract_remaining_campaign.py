"""Run the authorized remaining-corpus screening and provisional coding campaign."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.title_abstract_fast_track_batch import Pricing, RunSettings
from h2h_lit.title_abstract_remaining_campaign import run_remaining_campaign


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("queue", "coding-seed", "manifest", "config", "screening-prompt", "coding-prompt", "protocol", "amendment", "campaign-ledger", "output-dir"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--hard-spending-cap-usd", type=float, required=True)
    parser.add_argument("--input-usd-per-million", type=float, required=True)
    parser.add_argument("--cached-input-usd-per-million", type=float, required=True)
    parser.add_argument("--cache-write-usd-per-million", type=float, required=True)
    parser.add_argument("--output-usd-per-million", type=float, required=True)
    parser.add_argument("--reasoning-effort", default="medium")
    parser.add_argument("--service-tier", default="default")
    parser.add_argument("--max-output-tokens", type=int, default=3000)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--retry-limit", type=int, default=1)
    parser.add_argument("--runtime-hours", type=float, default=12.0)
    parser.add_argument("--drain-seconds", type=float, default=300.0)
    parser.add_argument("--api-key-environment", default="OPENAI_API_KEY")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    settings = RunSettings(
        provider_name="OpenAI", model=args.model, hard_spending_cap_usd=args.hard_spending_cap_usd,
        pricing=Pricing(args.input_usd_per_million, args.cached_input_usd_per_million, args.cache_write_usd_per_million, args.output_usd_per_million),
        max_output_tokens=args.max_output_tokens, reasoning_effort=args.reasoning_effort, verbosity="low",
        concurrency=4, timeout_seconds=args.timeout_seconds, retry_limit=args.retry_limit, smoke_count=20,
        service_tier=args.service_tier,
    )
    provider = OpenAIResponsesProvider.from_environment(variable=args.api_key_environment, timeout_seconds=args.timeout_seconds)
    report = run_remaining_campaign(
        queue_path=args.queue, coding_seed_path=args.coding_seed, manifest_path=args.manifest,
        config_path=args.config, screening_prompt_path=args.screening_prompt, coding_prompt_path=args.coding_prompt,
        protocol_path=args.protocol, amendment_path=args.amendment, campaign_ledger_path=args.campaign_ledger,
        output_dir=args.output_dir, settings=settings, provider=provider, runtime_hours=args.runtime_hours,
        drain_seconds=args.drain_seconds, resume=args.resume,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
