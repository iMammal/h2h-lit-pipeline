# Data Model

The core models are serializable Python dataclasses designed to round-trip to
JSON/JSONL/CSV and BibTeX-adjacent metadata without requiring a database.

Methodological categories are explicit:

- `source_derived`: bibliographic/API/PDF metadata.
- `deterministic`: normalization, deduplication, parsing, conversion.
- `heuristic`: keyword and off-topic filters.
- `llm_derived`: relevance, assistance type, modality, STAR analysis, reasoning.
- `human_decision`: manual review and task assignment.
- `generated_output`: downstream tables, JSON, reports, and figures.

The primary record type is `LiteratureRecord`; pipeline operations attach
`ProvenanceEvent` entries rather than silently mutating source-derived facts.
