# Architecture

The package separates source-derived data, deterministic transformations, heuristic
transformations, LLM-derived annotations, human/reviewer decisions, and generated
analytical outputs.

Source adapters normalize service responses into `LiteratureRecord` while preserving
service-specific metadata in `original_metadata` and provenance events. They accept an
injected HTTP client and do not perform live network calls by default.

PDF acquisition is split into candidate discovery and transfer:

- discovery resolves candidate PDF URLs from DOI/source metadata;
- transfer validates and writes PDF bytes;
- both stages emit provenance/status/error information.
