from h2h_lit.normalize import dedupe_key, normalize_doi, normalize_title, sanitize_filename


def test_normalize_doi_from_url_and_prefix():
    assert normalize_doi("https://doi.org/10.1234/ABC.Def.") == "10.1234/abc.def"
    assert normalize_doi("doi:10.5555/Test(1)") == "10.5555/test(1)"
    assert normalize_doi("not a doi") is None


def test_normalize_title_removes_bibtex_braces_and_punctuation():
    assert normalize_title("  A {Biology}: Visualization Tool! ") == "a biology visualization tool"


def test_dedupe_key_prefers_doi():
    assert dedupe_key(doi="DOI:10.1000/ABC", title="Different") == "doi:10.1000/abc"
    assert dedupe_key(doi=None, title="Example Paper") == "title:example paper"


def test_sanitize_filename_preserves_historical_constraints():
    assert sanitize_filename('A/B:C* "Paper"?') == "ABC Paper"
    assert sanitize_filename("") == "untitled"

