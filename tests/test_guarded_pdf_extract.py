from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject


ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR = ROOT / "scripts" / "guarded_pdf_extract.py"


def make_three_page_pdf(path: Path) -> None:
    writer = PdfWriter()
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    font_reference = writer._add_object(font)
    for page_number in range(1, 4):
        page = writer.add_blank_page(width=300, height=300)
        page[NameObject("/Resources")] = DictionaryObject(
            {
                NameObject("/Font"): DictionaryObject(
                    {NameObject("/F1"): font_reference}
                )
            }
        )
        content = DecodedStreamObject()
        content.set_data(
            f"BT /F1 12 Tf 72 200 Td (SYNTHETIC_PAGE_{page_number}_TOKEN) Tj ET".encode()
        )
        page[NameObject("/Contents")] = writer._add_object(content)
    with path.open("wb") as handle:
        writer.write(handle)


def run_supervisor(
    directory: Path,
    pdf: Path,
    *,
    batch_id: str,
    extra: list[str] | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path, Path, Path]:
    output = directory / f"{batch_id}.output.json"
    events = directory / f"{batch_id}.events.jsonl"
    checkpoint = directory / f"{batch_id}.checkpoint.json"
    command = [
        sys.executable,
        str(SUPERVISOR),
        "--pdf",
        str(pdf),
        "--output",
        str(output),
        "--events",
        str(events),
        "--checkpoint",
        str(checkpoint),
        "--pages",
        "2",
        "--batch-id",
        batch_id,
        "--preflight-delay-seconds",
        "0.05",
        "--minimum-memory-free-percent",
        "0",
        "--minimum-disk-free-gib",
        "0",
    ]
    if extra:
        command.extend(extra)
    completed = subprocess.run(command, capture_output=True, text=True, timeout=20)
    return completed, output, events, checkpoint


def test_only_requested_page_undergoes_text_extraction(tmp_path: Path) -> None:
    pdf = tmp_path / "synthetic-three-pages.pdf"
    make_three_page_pdf(pdf)
    completed, output, events, checkpoint = run_supervisor(
        tmp_path, pdf, batch_id="selection"
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["requested_pages"] == [2]
    assert [item["pdf_page"] for item in payload["extracted_pages"]] == [2]
    assert "SYNTHETIC_PAGE_2_TOKEN" in payload["extracted_pages"][0]["text"]
    assert "SYNTHETIC_PAGE_1_TOKEN" not in payload["extracted_pages"][0]["text"]
    assert "SYNTHETIC_PAGE_3_TOKEN" not in payload["extracted_pages"][0]["text"]

    event_rows = [json.loads(line) for line in events.read_text().splitlines()]
    started = [row["pdf_page"] for row in event_rows if row["event"] == "extract_started"]
    finished = [row["pdf_page"] for row in event_rows if row["event"] == "extract_completed"]
    assert started == [2]
    assert finished == [2]
    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert state["status"] == "complete"
    assert state["scientific_evidence_status"] == "EXTRACTION_COMPLETE_UNREVIEWED"


def test_timeout_is_incomplete_not_negative_evidence(tmp_path: Path) -> None:
    pdf = tmp_path / "synthetic-three-pages.pdf"
    make_three_page_pdf(pdf)
    completed, output, _, checkpoint = run_supervisor(
        tmp_path,
        pdf,
        batch_id="timeout",
        extra=["--timeout-seconds", "0.2", "--child-test-sleep-seconds", "2"],
    )

    assert completed.returncode == 2
    assert not output.exists()
    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert state["status"] == "incomplete"
    assert state["reason"] == "timeout"
    assert state["scientific_evidence_status"] == "NOT_ASSESSED_EXTRACTION_INCOMPLETE"
    assert state["termination"]["target_pid"] == state["child_pid"]


def test_rss_stop_is_incomplete_not_negative_evidence(tmp_path: Path) -> None:
    pdf = tmp_path / "synthetic-three-pages.pdf"
    make_three_page_pdf(pdf)
    completed, output, _, checkpoint = run_supervisor(
        tmp_path,
        pdf,
        batch_id="rss-stop",
        extra=["--rss-limit-mib", "1", "--child-test-sleep-seconds", "2"],
    )

    assert completed.returncode == 2
    assert not output.exists()
    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert state["status"] == "incomplete"
    assert state["reason"] == "child_rss_limit"
    assert state["scientific_evidence_status"] == "NOT_ASSESSED_EXTRACTION_INCOMPLETE"
    assert state["termination"]["target_pid"] == state["child_pid"]
