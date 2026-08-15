# AGENTS.md

Operational handoff for future coding/research agents working in this repository.

## 1. Mission

This repository is a clean, reproducible reconstruction of an organically developed
Jupyter-based literature-research pipeline supporting the STAR:

**From Hairballs to Hypotheses: AI-Assisted Visual Analytics in the Life Sciences**

This is not merely a generic paper downloader. The project exists to support a
scientifically transparent and reproducible literature-review methodology, including:

- literature discovery;
- bibliographic normalization;
- deduplication;
- corpus provenance;
- screening;
- LLM-assisted classification;
- human validation/adjudication;
- open-access full-text acquisition;
- structured full-text analysis;
- reproducible synthesis artifacts.

## 2. Historical Source Material

The archaeological workspace is:

```text
/Users/morris/PyCharmMiscProject
```

It contains historical notebooks, generated corpora, outputs, and credential-contaminated
Git history. Treat it as **read-only source/provenance material** unless the user
explicitly authorizes otherwise.

Important historical sources include:

- `H2HLitFetcher.ipynb`
- Git-committed historical `downloader.ipynb`
- `H2HLitFetcher12000v1.ipynb`
- `H2H STAR 3/preprocess.ipynb`
- `H2H STAR 3/distribute_tasks.py`
- `H2H STAR 3/distribute_tasks_2.py`

Historical `notebook.ipynb` was byte-identical to the committed downloader notebook and
is not an independent implementation lineage.

`speechify.ipynb` is unrelated to this research pipeline. It is a personal utility for
sending pasted text to OpenAI's Speech API and should not be migrated into this
repository.

## 3. Archaeological Findings

- Git initial commit in the historical workspace:
  `c1757aa11027e45cc1e411c1559b9ec27052f898`
- Initial commit date: January 12, 2026.
- The destructive notebook-zeroing event occurred later in the working tree.
- Historical `downloader.ipynb` is recoverable from Git.
- Its meaningful implementation scale is approximately 2,027 source-code lines across
  29 code cells, not 120k lines of Python.
- The strongest OA/PDF-acquisition implementation came from this committed downloader.
- `H2HLitFetcher.ipynb` contains the strongest search/query/classification lineage.
- `preprocess.ipynb` contains downstream RDF/PDF/full-text/STAR-analysis logic.

The clean implementation is synthesized from multiple historical sources rather than
treating one notebook as canonical.

## 4. Clean-Repository Status

Repository:

```text
/Users/morris/PyCharmProjects/h2h-lit-pipeline
```

Current reconstructed Git commits at the Phase 2 checkpoint:

```text
f7aefa0 Initialize clean project skeleton
34d5d38 Add provenance data models
e2f51c9 Add normalization BibTeX and dedupe core
df56c0c Add prompt artifact skeleton
2d07adc Add mocked literature source adapters
21334be Add mocked OA PDF acquisition
```

The working tree was clean at the Phase 2 checkpoint.

## 5. Implemented Components

### Deterministic Core

- canonical data/provenance models;
- DOI normalization;
- title normalization;
- dedupe keys;
- filename sanitization;
- BibTeX splitting/parsing/writing;
- note parsing;
- DOI-first/title-fallback deduplication.

### Literature Sources

Mock/injected-client implementations exist for:

- PubMed;
- Europe PMC;
- CrossRef;
- Semantic Scholar;
- arXiv.

All adapters return the canonical `LiteratureRecord` representation and preserve
source-specific provenance through `original_metadata` and provenance events.

### OA PDF Acquisition

OA means **Open Access**.

Recovered/migrated capabilities include:

- DOI-based OA discovery;
- Semantic Scholar;
- Unpaywall;
- Europe PMC;
- PubMed/PMC;
- arXiv;
- direct PDF candidates;
- ordered fallback;
- candidate de-duplication;
- PDF content-type validation;
- `%PDF-` magic validation;
- separation between discovery and transfer;
- acquisition provenance/errors.

No production live HTTP client has yet been enabled.

## 6. Current Test State

Phase 2 checkpoint:

```text
28 passed in 0.06s
```

Tests are offline/mocked. No live literature APIs, LLM APIs, or paper downloads were
invoked during reconstruction. The clean repository secret scan returned no matches.

## 7. Safety And Provenance Rules

Future agents must follow these rules:

1. Do not modify the historical `PyCharmMiscProject`.
2. Do not copy historical credentials into the clean repository.
3. Do not print credential values if discovered.
4. Do not use the historical Git history as the history of the clean repository.
5. Preserve historical behavior before improving it when methodology is uncertain.
6. Clearly distinguish historical behavior, inferred behavior, and newly designed behavior.
7. Do not silently alter scientific inclusion/classification methodology.
8. Do not make live external API or paid LLM calls unless explicitly authorized.
9. Maintain coherent incremental Git commits.
10. Keep tests offline by default through injected/mocked dependencies.

## 8. Known Technical Ambiguities

- DOI punctuation/parenthesis extraction has edge cases and should receive additional tests.
- The current BibTeX parser intentionally reproduces permissive historical behavior and is
  not a complete BibTeX implementation.
- Retry/backoff/rate-limit policy is not yet implemented.
- Production HTTP wiring is not yet implemented.
- Historical prompt text has not yet been migrated into the placeholder prompt files.
- LLM classification and STAR full-text analysis have not yet been migrated.
- The historical Desktop/Algorithmic corpus path appears to have used an asymmetric
  filtering rule and must not automatically become the revised methodology.

## 9. Reviewer-Driven Methodological Requirements

The reconstruction now directly supports the **CGF Fast-Track Major Revision**
methodology. The full reviewer-response matrix exists outside this repository and should
be provided to the next session.

Acceptance-critical literature methodology requirements:

- substantive methodology section in the main paper;
- PRISMA-style corpus flow with auditable counts;
- provenance for prior-survey seed papers;
- supplemental IEEE Xplore and ACM DL searches;
- exact search/query strings, dates, fields, filters, and API constraints;
- explicit LLM classification procedure;
- model/provider/version/settings/prompt provenance;
- explicit relevance/inclusion rubric;
- exclusion-reason taxonomy;
- human validation/audit of LLM classification;
- separation of machine assistance from author interpretation;
- symmetric corpus eligibility policy.

PRISMA-style flow means auditable counts showing records retrieved from each source,
deduplicated, screened, excluded with reasons, classified/audited, full-text eligible,
and included in the final synthesis corpus.

The software should eventually emit the evidence needed to construct this flow, rather
than relying on manually reconstructed counts.

## 10. Methodological Provenance Target

Desired per-record event chain:

```text
record discovered
    -> source database/service
    -> exact query/query-family
    -> retrieval timestamp/run
    -> normalized
    -> duplicate/unique decision
    -> screening decision
    -> exclusion reason if excluded
    -> LLM classification
    -> model + prompt version + settings
    -> human audit/adjudication
    -> eligibility decision
    -> narrative-synthesis selection
    -> full-text acquisition/analysis
```

The design need not use this exact storage representation, but these transitions must
become auditable.

## 11. Important Distinction: Eligibility Vs Synthesis Sampling

The historical asymmetric filtering issue is important. The revised methodology should
likely distinguish:

- **eligible corpus**
- **narrative synthesis / prioritization sample**

Do not reproduce a cell-specific threshold such as Desktop/Algorithmic `2019+` and
relevance `>=4` as a universal corpus eligibility decision unless this is explicitly
justified and approved.

The goal is comparable denominators across assistance x modality cells.

## 12. Reviewer Conceptual Concerns For Future Schema Design

Manuscript rewriting is outside the immediate coding task, but future software/schema
work should preserve room for these conceptual requirements:

- the assistance axis likely needs to represent locus/system initiative/mediation of
  analytical agency, not merely "AI technology";
- old techniques such as dimensionality reduction must not be represented as novel AI
  merely because they are computational;
- assistance relationships and visualization modalities need logically independent
  definitions;
- LLM classifications will need definitions clear enough for human inter-rater validation;
- task, modality, assistance, evaluation, and provenance should remain distinguishable
  fields in the data model.

Do not implement a new conceptual taxonomy solely from this summary. Preserve the ability
to revise labels once the manuscript conceptual model is frozen.

## 13. Recommended Next Phase

The next high-reasoning session should begin in analysis/plan mode, not immediately edit
code.

First task:

**Phase 3A - Methodology and LLM Archaeology**

Inspect the historical notebooks and generated artifacts to reconstruct:

- every historical literature-classification prompt;
- prompt variants/evolution;
- model names/providers;
- model parameters where available;
- input fields supplied to the model;
- expected/actual output schema;
- relevance-score definitions;
- assistance classifications;
- modality classifications;
- off-topic logic;
- retry/error handling;
- human/manual override behavior;
- generated `llm_classification`;
- generated `LLM_Reasoning`;
- paths between raw harvested records and final/sorted `.bib` files.

Also reconstruct the historical corpus stages sufficiently to determine what PRISMA
counts can be recovered versus what must be regenerated.

## 14. Required Stop Point For Next Session

Before modifying LLM/classification methodology, first produce a report containing:

1. historical prompt inventory;
2. classification lineage;
3. historical corpus state-transition model;
4. recoverable PRISMA stages/count evidence;
5. methodological inconsistencies;
6. reviewer requirement -> software/provenance mapping;
7. proposed revised data/provenance schema;
8. proposed human-validation workflow;
9. proposed implementation plan.

Then stop for approval.

## 15. Bootstrap Commands

Establish current repository state:

```bash
cd /Users/morris/PyCharmProjects/h2h-lit-pipeline
git status
git log --oneline --decorate -10
git ls-files
```

Test command known to work in the current Codex runtime:

```bash
/Users/morris/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m pytest
```

This uses the Codex runtime, not a stable project-local virtual environment. For normal
local development, create a project venv as described in `docs/development.md`.

Before starting Phase 3A, read:

- `README.md`
- `docs/architecture.md`
- `docs/migration.md`
- `docs/provenance.md`
- `docs/llm_methodology.md`

