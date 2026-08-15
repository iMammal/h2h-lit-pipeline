from pathlib import Path

from h2h_lit.bibtex_io import parse_bibtex, parse_note, records_from_bibtex, to_bibtex


FIXTURE = Path(__file__).parent / "fixtures" / "sample_results.bib"


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

