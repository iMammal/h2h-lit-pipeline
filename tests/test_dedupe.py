from pathlib import Path

from h2h_lit.bibtex_io import records_from_bibtex
from h2h_lit.dedupe import deduplicate_records


FIXTURE = Path(__file__).parent / "fixtures" / "sample_results.bib"


def test_deduplicate_records_prefers_first_doi_match():
    records = records_from_bibtex(FIXTURE.read_text(encoding="utf-8"))
    unique = deduplicate_records(records)
    assert [r.title for r in unique] == [
        "Example Network Visualization Paper",
        "A Second {Biology} Visualization Tool",
    ]
    assert unique[0].provenance[-1].metadata["dedupe_key"] == "doi:10.1234/example.1"

