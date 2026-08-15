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
