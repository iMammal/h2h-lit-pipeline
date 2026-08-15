# Migration Map

This file records historical implementation origins and their clean-package destinations.

## Initial Checkpoint

- Historical `downloader.ipynb`
  - `_norm_doi` -> `src/h2h_lit/normalize.py`
  - `_split_bib_entries`, `_parse_entry_fields`, `_parse_note` -> `src/h2h_lit/bibtex_io.py`
- Historical `H2HLitFetcher.ipynb`
  - `escape_bibtex`, `to_bibtex`, `save_bib` behavior -> `src/h2h_lit/bibtex_io.py`
  - DOI/title deduplication behavior -> `src/h2h_lit/dedupe.py`
- Current `H2H STAR 3/preprocess.ipynb`
  - JSON conversion and full-text analysis are deferred to later checkpoints.

