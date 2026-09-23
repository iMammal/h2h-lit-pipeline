"""Launch the authorized staging-only overnight title/abstract screening batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from h2h_lit.openai_provider import OpenAIResponsesProvider
from h2h_lit.title_abstract_fast_track_batch import Pricing, RunSettings
from h2h_lit.title_abstract_fast_track_overnight import run_overnight


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, required=True)
    parser.add_argument("--batch-manifest", type=Path, required=True)
    parser.add_argument("--prompt", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--amendment", type=Path, required=True)
    parser.add_argument("--identity-overlay", type=Path, required=True)
    parser.add_argument("--prior-repair-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--hard-spending-cap-usd", type=float, required=True)
    parser.add_argument("--input-usd-per-million", type=float, required=True)
    parser.add_argument("--cached-input-usd-per-million", type=float, required=True)
    parser.add_argument("--cache-write-usd-per-million", type=float, required=True)
    parser.add_argument("--output-usd-per-million", type=float, required=True)
    parser.add_argument("--reasoning-effort", default="medium")
    parser.add_argument("--service-tier", default="default")
    parser.add_argument("--max-output-tokens", type=int, default=2500)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--retry-limit", type=int, default=1)
    parser.add_argument("--smoke-count", type=int, default=20)
    parser.add_argument("--runtime-hours", type=float, default=10.0)
    parser.add_argument("--drain-seconds", type=float, default=300.0)
    parser.add_argument("--api-key-environment", default="OPENAI_API_KEY")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    settings = RunSettings(
        provider_name="OpenAI", model=args.model,
        hard_spending_cap_usd=args.hard_spending_cap_usd,
        pricing=Pricing(
            input_usd_per_million=args.input_usd_per_million,
            cached_input_usd_per_million=args.cached_input_usd_per_million,
            cache_write_usd_per_million=args.cache_write_usd_per_million,
            output_usd_per_million=args.output_usd_per_million,
        ),
        max_output_tokens=args.max_output_tokens,
        reasoning_effort=args.reasoning_effort, verbosity="low",
        concurrency=args.concurrency, timeout_seconds=args.timeout_seconds,
        retry_limit=args.retry_limit, smoke_count=args.smoke_count,
        service_tier=args.service_tier,
    )
    provider = OpenAIResponsesProvider.from_environment(
        variable=args.api_key_environment, timeout_seconds=args.timeout_seconds,
    )
    report = run_overnight(
        batch_path=args.batch, batch_manifest_path=args.batch_manifest,
        prompt_path=args.prompt, protocol_path=args.protocol,
        amendment_path=args.amendment, identity_overlay_path=args.identity_overlay,
        prior_repair_dir=args.prior_repair_dir, output_dir=args.output_dir,
        settings=settings, provider=provider, runtime_hours=args.runtime_hours,
        drain_seconds=args.drain_seconds, resume=args.resume,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
