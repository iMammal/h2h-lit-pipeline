# H2H Literature Pipeline

Reproducible literature-identification, machine-assisted screening, coding, and
full-report synthesis support for the CGF submission **From Hairballs to Hypotheses:
AI-Assisted Visual Analytics in the Life Sciences** (H2H2).

This repository contains the implementation and versioned protocols. Large research
artifacts are intentionally kept outside Git and are distributed, when rights permit,
as a separately checksummed supplement. The pipeline records machine proposals and
author assessments as distinct layers: technical reproducibility of a model-assisted
workflow does **not** establish independent classification accuracy.

## Reproducibility checkpoint

The release-preparation audit is documented in
[docs/reproducibility.md](docs/reproducibility.md). The authoritative artifact inventory,
including historical and restricted materials, is
[docs/reproducibility_artifact_inventory.md](docs/reproducibility_artifact_inventory.md).

The terminal artifacts used for manuscript-facing totals are:

- identification: the registered global merge and identity-confirmation overlay;
- screening: the effective terminal screening/continuation lineage and its final
  9,505-record categorized-candidate export;
- full reports: `target-cell-complete-pass-v1-20260926`, which contains the corrected
  cumulative eligibility, report/system, and placement matrices;
- flow counts: `study-selection-flow-count-ledger-v1-20260926`.

Earlier plans, pilot runs, progress snapshots, and the January manuscript remain
provenance. They do not override the terminal artifacts above.

## Workflow and artifact index

| Stage | Primary contract or artifact | Manuscript-facing output |
|---|---|---|
| Search design | `config/star_production_query_plan_v1.json` | Exact provider-native queries, fields, limits, and query families |
| Executed retrieval | `outputs/production/star-external-retrieval-wave-001/execution/` | Source-occurrence counts and execution provenance |
| Identity | `GlobalIdentificationMerge/v1/` plus `PriorSurveyIdentityAdjudication/v1/` | 187,446 occurrences and 140,959 canonical records |
| Screening | terminal title/abstract completion packages | 140,810 valid outcomes and the effective ADVANCE/DEFER/EXCLUDED partition |
| Candidate coding | `combined_categorized_candidates.csv` | 9,505 machine-coded candidate records |
| Human review | frozen benchmark and validated Morris return packages | Pilot evidence and its independence/exposure limitations |
| Full-report synthesis | `target-cell-complete-pass-v1-20260926/` | 381 systems considered, 132 completed assessments, 76 mapped systems, 118 placements |
| Study-selection flow | `study-selection-flow-count-ledger-v1-20260926/` | Diagram-ready counts, connections, equations, and unit crosswalk |
| Assistance × modality map | target-cell package generators and corrected matrices | Figure 2 SVG/PDF and supported-cell counts |

The ignored `outputs/` tree is not part of a GitHub source archive. Preserve the
supplement archive and its external `SHA256SUMS` beside a release; do not assume a
GitHub-to-Zenodo integration captures release attachments.

## Installation

The package metadata requires Python 3.11 or newer. Historical runs used the exact
model/provider settings saved in their manifests; the repository does not invent
unrecorded historical dependency pins.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
```

The public package metadata currently declares the project license as
`Proprietary`. No repository-wide open-source or data license is inferred. Resolve the
code, metadata, and supplement licensing choices before publication.

## Bounded offline reproduction

These commands use saved artifacts and make no network or model calls:

```bash
python scripts/build_reproducibility_supplement.py \
  --repository-root . \
  --output-root outputs/staging/h2h2-cgf-reproducibility-release-v1-20260930 \
  --research-commit 27cf867f598041f846f2ce57c6a736a89539dc98

python scripts/build_reproducibility_supplement.py \
  --verify outputs/staging/h2h2-cgf-reproducibility-release-v1-20260930/supplement

python -m pytest tests/test_reproducibility_supplement.py
```

The builder validates logical CSV records, stable-ID uniqueness, checkpoint equations,
source hashes, manifest completeness, and a clean extraction of the archive. It does
not reopen the registered multi-gigabyte corpus.

## Optional live services

Live retrieval and inference are optional reruns, not reproduction of the archived
results. They can change as providers, indexes, models, and access rights change.
Consult the source plan and command help before use. Depending on the selected sources,
credentials may include `IEEE_XPLORE_API_KEY`, `SEMANTIC_SCHOLAR_API_KEY`,
`UNPAYWALL_EMAIL`, and `OPENAI_API_KEY`; ACM export may require institutional access and
manual UI work. Live OpenAI inference incurs cost and must be explicitly authorized.
Never commit credentials, authentication headers, signed URLs, or raw institutional
downloads.

## Availability boundaries

The curated supplement excludes copyrighted full-text PDFs, raw provider responses
whose redistribution terms are not established, internal BibTeX exports of uncertain
rights, credentials, private correspondence, caches, and environments. It includes
reduced audit exports, exact query/protocol artifacts, checksummed bindings to excluded
inputs, corrected aggregate ledgers, and figure-generation inputs. See the
reproducibility guide for the resulting limits on end-to-end regeneration.
