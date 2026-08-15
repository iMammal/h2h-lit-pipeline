# Migration Map

This file records historical implementation origins and their clean-package destinations.

## Initial Checkpoint

- Historical `downloader.ipynb`
  - `_norm_doi` -> `src/h2h_lit/normalize.py`
  - `_split_bib_entries`, `_parse_entry_fields`, `_parse_note` -> `src/h2h_lit/bibtex_io.py`
- Historical `H2HLitFetcher.ipynb`
  - `escape_bibtex`, `to_bibtex`, `save_bib` behavior -> `src/h2h_lit/bibtex_io.py`
  - DOI/title deduplication behavior -> `src/h2h_lit/dedupe.py`
- Shared historical behavior
  - filename sanitization -> `src/h2h_lit/normalize.py`
  - DOI-first duplicate keys, title fallback -> `src/h2h_lit/normalize.py` and `src/h2h_lit/dedupe.py`
- Current `H2H STAR 3/preprocess.ipynb`
  - JSON conversion and full-text analysis are deferred to later checkpoints.

## Phase 2 Source Adapters

- Historical `H2HLitFetcher.ipynb`
  - `fetch_pubmed` -> `src/h2h_lit/sources/pubmed.py`
  - `fetch_europe_pmc` -> `src/h2h_lit/sources/europe_pmc.py`
  - `fetch_crossref` -> `src/h2h_lit/sources/crossref.py`
  - `fetch_semantic_scholar` -> `src/h2h_lit/sources/semantic_scholar.py`
  - `fetch_arxiv` -> `src/h2h_lit/sources/arxiv.py`
  - source-specific `pdf_url` preservation -> canonical `LiteratureRecord.pdf_url`
  - source dictionaries -> `LiteratureRecord.original_metadata`

Phase 2 source adapters require an injected HTTP client and are tested with mocks only.

## Phase 2 OA PDF Acquisition

- Historical committed `downloader.ipynb`
  - `_is_open_access` / note interpretation -> `src/h2h_lit/pdfs/discovery.py::is_open_access`
  - `_semantic_scholar_pdf_for_doi` -> `src/h2h_lit/pdfs/discovery.py::semantic_scholar_pdf_for_doi`
  - `_unpaywall_pdf_for_doi` -> `src/h2h_lit/pdfs/discovery.py::unpaywall_pdf_for_doi`
  - `_europe_pmc_pdf_for_doi` -> `src/h2h_lit/pdfs/discovery.py::europe_pmc_pdf_for_doi`
  - `_pubmed_pmc_pdf_for_doi` -> `src/h2h_lit/pdfs/discovery.py::pubmed_pmc_pdf_for_doi`
  - `_arxiv_pdf_from_url` -> `src/h2h_lit/pdfs/discovery.py::arxiv_pdf_from_url`
  - candidate ordering/de-duplication in `download_oa_pdfs_from_bib` -> `discover_pdf_candidates`
  - `_is_pdf_response` -> `src/h2h_lit/pdfs/download.py::looks_like_pdf_response`
  - `_extract_pdf_url_from_html` -> `src/h2h_lit/pdfs/download.py::extract_pdf_url_from_html`
  - `_download_pdf` transfer/validation behavior -> `fetch_pdf_bytes` and `download_first_pdf`

The clean implementation intentionally separates candidate discovery from transfer and
requires injected HTTP clients for both. No live PDF requests are made in Phase 2 tests.
