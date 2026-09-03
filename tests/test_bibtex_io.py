from pathlib import Path

import pytest

from h2h_lit.bibtex_io import (
    BibtexParseError,
    parse_bibtex,
    parse_bibtex_with_diagnostics,
    parse_note,
    records_from_bibtex,
    split_bib_entries,
    to_bibtex,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample_results.bib"
MALFORMED_ACM_FIXTURE = Path(__file__).parent / "fixtures" / "acm_malformed_keywords.bib"


def test_parse_note_source_and_open_access():
    assert parse_note("Source: EuropePMC, OpenAccess: True") == {
        "source": "EuropePMC",
        "openaccess": True,
    }


def test_parse_bibtex_entries_and_fields():
    fields = parse_bibtex(FIXTURE.read_text(encoding="utf-8"))
    assert len(fields) == 3
    assert fields[0]["_key"] == "entry0"
    assert fields[0]["title"] == "Example Network Visualization Paper"
    assert fields[2]["title"] == "A Second {Biology} Visualization Tool"


def test_records_from_bibtex_preserve_source_metadata():
    records = records_from_bibtex(FIXTURE.read_text(encoding="utf-8"))
    assert records[0].doi == "10.1234/example.1"
    assert records[0].source_database == "EuropePMC"
    assert records[0].is_open_access is True
    assert records[0].provenance[0].stage == "bibtex_parse"


def test_to_bibtex_uses_historical_schema():
    records = records_from_bibtex(FIXTURE.read_text(encoding="utf-8"))
    text = to_bibtex(records[:1])
    assert "@article{entry0" in text
    assert "note    = {Source: EuropePMC, OpenAccess: True}" in text
    assert "abstract= {A short abstract" in text


def test_malformed_acm_entry_is_flagged_without_swallowing_later_records():
    text = MALFORMED_ACM_FIXTURE.read_text(encoding="utf-8")
    raw_entries = split_bib_entries(text)
    result = parse_bibtex_with_diagnostics(text)

    assert len(raw_entries) == 4
    assert result.physical_header_count == 4
    assert len(result.entries) == 3
    assert len(result.issues) == 1
    assert result.accounted_record_count == result.physical_header_count
    assert [entry["_key"] for entry in result.entries] == ["before", "after", "consecutive"]

    issue = result.issues[0]
    assert issue.ordinal == 2
    assert issue.code == "UNBALANCED_BRACES"
    assert issue.brace_depth == 1
    assert issue.key == "10.1145/3805712.3808367"
    assert issue.partial_fields["title"].startswith("ESCOMIC:")
    assert issue.raw_entry == raw_entries[1]
    assert "keywords = {{explainable information retrieval" in issue.raw_entry
    assert "@article{after" not in issue.raw_entry


def test_default_parser_fails_explicitly_and_exposes_diagnostics():
    text = MALFORMED_ACM_FIXTURE.read_text(encoding="utf-8")
    with pytest.raises(BibtexParseError) as caught:
        parse_bibtex(text)
    assert caught.value.result.accounted_record_count == 4
    assert caught.value.result.issues[0].key == "10.1145/3805712.3808367"


def test_valid_multiline_nested_quoted_and_consecutive_entries_are_unchanged():
    text = '''@article{one,
  title = {A {Nested} Title},
  abstract = {First line
second {nested} line},
  note = "quoted {braces} remain balanced"
}
@article{two,
  title = "Second {Title}",
  year = {2026}
}'''
    result = parse_bibtex_with_diagnostics(text)
    assert result.physical_header_count == 2
    assert result.accounted_record_count == 2
    assert result.issues == ()
    assert list(result.entries) == parse_bibtex(text)
    assert result.entries[0]["abstract"] == "First line\nsecond {nested} line"
    assert result.entries[0]["note"] == "quoted {braces} remain balanced"
