#!/usr/bin/env python3
"""Supervise a bounded PDF page-extraction subprocess on macOS.

The RSS ceiling is a monitored stop condition, not a hard memory limit. The supervisor
terminates only the exact child PID that it created.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GIB = 1024**3
MIB = 1024**2


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_pages(raw: str) -> list[int]:
    pages = [int(value.strip()) for value in raw.split(",") if value.strip()]
    if not pages or any(page < 1 for page in pages):
        raise argparse.ArgumentTypeError("pages must be positive one-based integers")
    if len(pages) != len(set(pages)):
        raise argparse.ArgumentTypeError("duplicate pages are not allowed")
    if len(pages) > 2:
        raise argparse.ArgumentTypeError("a guarded batch may contain at most two pages")
    return pages


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def command_output(command: list[str], timeout: float = 5.0) -> str:
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return completed.stdout


def system_sample(path: Path) -> dict[str, Any]:
    memory_text = command_output(["memory_pressure"])
    memory_match = re.search(r"System-wide memory free percentage:\s*(\d+)%", memory_text)
    if memory_match is None:
        raise RuntimeError("could not parse memory_pressure output")

    swap_text = command_output(["sysctl", "vm.swapusage"])
    swap_match = re.search(
        r"total = ([\d.]+)M\s+used = ([\d.]+)M\s+free = ([\d.]+)M",
        swap_text,
    )
    if swap_match is None:
        raise RuntimeError("could not parse vm.swapusage output")

    disk = os.statvfs(path)
    return {
        "timestamp_utc": utc_now(),
        "memory_free_percent": int(memory_match.group(1)),
        "swap_total_mib": float(swap_match.group(1)),
        "swap_used_mib": float(swap_match.group(2)),
        "swap_free_mib": float(swap_match.group(3)),
        "disk_free_bytes": disk.f_bavail * disk.f_frsize,
    }


def child_rss_bytes(pid: int) -> int | None:
    completed = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        check=False,
        capture_output=True,
        text=True,
        timeout=2.0,
    )
    raw = completed.stdout.strip()
    if not raw:
        return None
    return int(raw.splitlines()[0].strip()) * 1024


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def terminate_exact_child(process: subprocess.Popen[str]) -> dict[str, Any]:
    result: dict[str, Any] = {"target_pid": process.pid, "sigterm_sent": False, "sigkill_sent": False}
    if process.poll() is not None:
        return result
    try:
        os.kill(process.pid, signal.SIGTERM)
        result["sigterm_sent"] = True
    except ProcessLookupError:
        return result
    try:
        process.wait(timeout=2.0)
        return result
    except subprocess.TimeoutExpired:
        pass
    try:
        os.kill(process.pid, signal.SIGKILL)
        result["sigkill_sent"] = True
    except ProcessLookupError:
        return result
    process.wait(timeout=2.0)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--checkpoint", required=True, type=Path)
    parser.add_argument("--pages", required=True, type=parse_pages)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--rss-limit-mib", type=float, default=384.0)
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.25)
    parser.add_argument("--system-poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--minimum-memory-free-percent", type=int, default=30)
    parser.add_argument("--stop-memory-free-percent", type=int, default=25)
    parser.add_argument("--minimum-disk-free-gib", type=float, default=25.0)
    parser.add_argument("--stop-disk-free-gib", type=float, default=20.0)
    parser.add_argument("--maximum-preflight-swap-growth-mib", type=float, default=64.0)
    parser.add_argument("--stop-swap-growth-mib", type=float, default=512.0)
    parser.add_argument("--preflight-delay-seconds", type=float, default=1.0)
    parser.add_argument("--max-chars-per-page", type=int, default=200_000)
    parser.add_argument("--child-test-sleep-seconds", type=float, default=0.0, help=argparse.SUPPRESS)
    return parser


def checkpoint_base(args: argparse.Namespace, started: str) -> dict[str, Any]:
    return {
        "schema_version": "guarded-pdf-extraction-checkpoint-v1",
        "batch_id": args.batch_id,
        "source": str(args.pdf),
        "requested_pages": args.pages,
        "started_utc": started,
        "limits": {
            "rss_limit_mib": args.rss_limit_mib,
            "timeout_seconds": args.timeout_seconds,
            "stop_memory_free_percent": args.stop_memory_free_percent,
            "stop_swap_growth_mib": args.stop_swap_growth_mib,
            "stop_disk_free_gib": args.stop_disk_free_gib,
        },
    }


def write_incomplete(
    args: argparse.Namespace,
    checkpoint: dict[str, Any],
    reason: str,
    **fields: Any,
) -> int:
    checkpoint.update(
        {
            "status": "incomplete",
            "reason": reason,
            "scientific_evidence_status": "NOT_ASSESSED_EXTRACTION_INCOMPLETE",
            "completed_utc": utc_now(),
            **fields,
        }
    )
    atomic_write_json(args.checkpoint, checkpoint)
    print(json.dumps({"status": "incomplete", "reason": reason, "checkpoint": str(args.checkpoint)}))
    return 2


def main() -> int:
    args = build_parser().parse_args()
    started = utc_now()
    checkpoint = checkpoint_base(args, started)
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    args.events.parent.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    try:
        preflight_one = system_sample(args.pdf.parent)
        time.sleep(args.preflight_delay_seconds)
        preflight_two = system_sample(args.pdf.parent)
    except BaseException as exc:
        return write_incomplete(
            args,
            checkpoint,
            "preflight_monitoring_unavailable",
            monitoring_error=f"{type(exc).__name__}: {exc}",
        )
    checkpoint["preflight_samples"] = [preflight_one, preflight_two]

    preflight_reason = None
    if min(preflight_one["memory_free_percent"], preflight_two["memory_free_percent"]) < args.minimum_memory_free_percent:
        preflight_reason = "preflight_memory_pressure"
    elif min(preflight_one["disk_free_bytes"], preflight_two["disk_free_bytes"]) < args.minimum_disk_free_gib * GIB:
        preflight_reason = "preflight_disk_floor"
    elif preflight_two["swap_used_mib"] - preflight_one["swap_used_mib"] > args.maximum_preflight_swap_growth_mib:
        preflight_reason = "preflight_swap_growth"
    if preflight_reason:
        return write_incomplete(args, checkpoint, preflight_reason)

    worker = Path(__file__).with_name("guarded_pdf_page_worker.py")
    command = [
        sys.executable,
        str(worker),
        "--pdf",
        str(args.pdf),
        "--output",
        str(args.output),
        "--events",
        str(args.events),
        "--pages",
        ",".join(str(page) for page in args.pages),
        "--max-chars-per-page",
        str(args.max_chars_per_page),
    ]
    if args.child_test_sleep_seconds > 0:
        command.extend(["--test-sleep-seconds", str(args.child_test_sleep_seconds)])
    checkpoint["child_command"] = command

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    checkpoint["child_pid"] = process.pid
    started_monotonic = time.monotonic()
    last_system_poll = 0.0
    peak_rss = 0
    samples: list[dict[str, Any]] = []
    stop_reason: str | None = None
    monitoring_error: str | None = None

    while process.poll() is None:
        elapsed = time.monotonic() - started_monotonic
        try:
            rss = child_rss_bytes(process.pid)
        except BaseException as exc:
            monitoring_error = f"{type(exc).__name__}: {exc}"
            stop_reason = "child_rss_monitoring_unavailable"
            break
        if rss is not None:
            peak_rss = max(peak_rss, rss)
        sample: dict[str, Any] = {
            "elapsed_seconds": round(elapsed, 3),
            "child_rss_bytes": rss,
        }
        if elapsed - last_system_poll >= args.system_poll_interval_seconds:
            try:
                current_system = system_sample(args.pdf.parent)
            except BaseException as exc:
                monitoring_error = f"{type(exc).__name__}: {exc}"
                stop_reason = "system_monitoring_unavailable"
                samples.append(sample)
                break
            sample["system"] = current_system
            last_system_poll = elapsed
            if current_system["memory_free_percent"] < args.stop_memory_free_percent:
                stop_reason = "system_memory_pressure"
            elif current_system["swap_used_mib"] - preflight_two["swap_used_mib"] > args.stop_swap_growth_mib:
                stop_reason = "system_swap_growth"
            elif current_system["disk_free_bytes"] < args.stop_disk_free_gib * GIB:
                stop_reason = "system_disk_floor"
        samples.append(sample)
        if stop_reason:
            break
        if rss is not None and rss > args.rss_limit_mib * MIB:
            stop_reason = "child_rss_limit"
            break
        if elapsed > args.timeout_seconds:
            stop_reason = "timeout"
            break
        time.sleep(args.poll_interval_seconds)

    termination = None
    if stop_reason:
        termination = terminate_exact_child(process)
    stdout, stderr = process.communicate(timeout=5.0)
    elapsed_total = time.monotonic() - started_monotonic
    events = read_events(args.events)
    completed_pages = [event["pdf_page"] for event in events if event.get("event") == "extract_completed"]
    checkpoint.update(
        {
            "monitor_samples": samples,
            "peak_child_rss_bytes": peak_rss,
            "elapsed_seconds": round(elapsed_total, 3),
            "child_exit_code": process.returncode,
            "child_stdout": stdout[-4000:],
            "child_stderr": stderr[-8000:],
            "events_path": str(args.events),
            "child_events": events,
            "completed_pages": completed_pages,
            "termination": termination,
        }
    )
    if monitoring_error:
        checkpoint["monitoring_error"] = monitoring_error
    try:
        checkpoint["postflight_sample"] = system_sample(args.pdf.parent)
    except BaseException as exc:
        checkpoint["postflight_error"] = f"{type(exc).__name__}: {exc}"

    if stop_reason:
        return write_incomplete(args, checkpoint, stop_reason)
    if process.returncode != 0:
        return write_incomplete(args, checkpoint, "child_error")
    if not args.output.exists():
        return write_incomplete(args, checkpoint, "missing_output")

    output = json.loads(args.output.read_text(encoding="utf-8"))
    output_pages = [item["pdf_page"] for item in output.get("extracted_pages", [])]
    if output.get("requested_pages") != args.pages or output_pages != args.pages:
        return write_incomplete(args, checkpoint, "page_selection_mismatch")
    if any(item.get("truncated") for item in output["extracted_pages"]):
        checkpoint["output"] = {
            "path": str(args.output),
            "sha256": sha256_file(args.output),
            "bytes": args.output.stat().st_size,
        }
        return write_incomplete(args, checkpoint, "text_truncated")

    checkpoint.update(
        {
            "status": "complete",
            "reason": None,
            "scientific_evidence_status": "EXTRACTION_COMPLETE_UNREVIEWED",
            "completed_utc": utc_now(),
            "output": {
                "path": str(args.output),
                "sha256": sha256_file(args.output),
                "bytes": args.output.stat().st_size,
            },
        }
    )
    atomic_write_json(args.checkpoint, checkpoint)
    print(json.dumps({"status": "complete", "checkpoint": str(args.checkpoint)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
