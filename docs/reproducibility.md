# H2H2 CGF reproducibility guide

## Scope

This guide reproduces reported aggregates from preserved inputs and responses. It does
not rerun mutable literature services or paid models, claim exhaustive screening, or
turn automated judgments into independent human ground truth.

The audited research-implementation checkpoint is
`27cf867f598041f846f2ce57c6a736a89539dc98`; the first release-preparation commit is
`c920d20f1ef912e04addb959cff0eeb78d1a5352`. Cached remote-tracking state is not proof of
the current public GitHub state because no network fetch is part of this offline task.
The previously observed public checkpoint
`869f0cd932df282e0c6accc4bf0067813da198e2` is an ancestor of the research checkpoint.

## Precedence

Use this order when artifacts conflict:

1. `target-cell-complete-pass-v1-20260926` for cumulative full-report/system/placement
   state;
2. `study-selection-flow-count-ledger-v1-20260926` for manuscript flow counts and unit
   changes;
3. the terminal 9,505-row categorized-candidate export for candidate coding;
4. terminal screening completion/precision reports for effective screening outcomes;
5. registered production merge and identity overlay for identification;
6. frozen protocol 1.0.0, followed by explicitly scoped amendments 2.0.0, 2.1.0, and
   2.2.0 for the runs each governs.

Progress snapshots, partial exports, pilot outputs, proposals, and pre-execution plans
are historical unless a terminal artifact explicitly binds them. Empty early prior-
survey seed manifests are not evidence of completed imports.

## Units and reconciled equations

Counts are deliberately separated by unit:

```text
187,446 source occurrences
  -> 140,959 canonical records

140,959 canonical records
  = 140,810 valid effective outcomes
    + 110 protected records
    + 35 technical failures
    + 4 ambiguous requests
    + 0 genuinely unprocessed

140,810 valid effective outcomes
  = 9,505 ADVANCE
    + 83,258 DEFER
    + 48,047 EXCLUDED

384 selected publication/report rows
  -> 381 systems considered
  = 132 completed system assessments + 249 without a completed assessment

132 completed system assessments
  = 80 eligible + 19 unresolved + 33 excluded/contextual

80 eligible systems
  = 76 systems with supported placements + 4 without a supported placement

76 mapped systems -> 118 multilabel system-cell placements
```

Background designations (10,460) are a subset of exclusions, not another outcome.
Access attempts, report identities, system identities, completed system assessments,
and system-cell placements are not interchangeable. Cell totals are non-additive.

## Repository build and clean-extraction reproduction

From a checkout containing the preserved ignored artifacts:

```bash
python scripts/build_reproducibility_supplement.py \
  --repository-root . \
  --output-root outputs/staging/h2h2-cgf-reproducibility-release-v3-20260930 \
  --research-commit 27cf867f598041f846f2ce57c6a736a89539dc98

python scripts/build_reproducibility_supplement.py \
  --verify outputs/staging/h2h2-cgf-reproducibility-release-v3-20260930/supplement
```

The first command creates a fresh directory and refuses to overwrite an existing
package. The second validates the package manifest and CSV invariants. The build also
extracts the ZIP into a temporary directory and executes its portable reproducer without
relying on private absolute paths.

An external reader can run the advertised workflow using only the extracted package:

```bash
python -m pip install -r supplement/requirements-reproduction.txt
python supplement/reproduce.py \
  --supplement-root supplement \
  --output-dir reproduced
```

Python 3.11 or newer is required. CairoSVG 2.9.1 is the documented SVG-to-PDF rendering
dependency; PDF bytes may vary with renderer/library versions even when counts and
vector content agree. The entry point refuses to overwrite a non-empty output directory.

The authoritative manuscript input is
`outputs/staging/manuscript-input-20260925/STAR-editable-source-expanded-map-20260925.zip`
(SHA-256 `ed8c302bf4bc2d29a44620ee57c1c73ba7e939b98104994707851cbf0cd95d8f`).
It remains unchanged and is not redistributed by the supplement.

## What can be regenerated

The package supports executable offline recomputation of:

- 14 PRESENT/ABSENT/UNCERTAIN candidate category distributions;
- all 20 candidate Assistance × Modality PRESENT co-occurrences;
- all 100 candidate cell-task distributions;
- supported system-cell counts and the supported-system map;
- normalized eligibility outcomes and the identification/screening/full-report equations;
- the machine candidate co-occurrence vector figure.

It additionally supports audit of exact provider/query definitions, preserved executed
source counts, screening lineage, system/report identities, and placement evidence. It
does not reconstruct upstream record-level retrieval, deduplication, or effective
screening decisions from aggregate reports.

Rerunning retrieval or model screening requires excluded inputs, access, credentials,
and potentially paid services. Exact external-service reruns are not expected to
reproduce archived results.
Search indexes, API behavior, access entitlements, and model deployments can change.
The saved responses and full registered dataset remain locally hash-bound, but are not
redistributed where rights are uncertain.

## Human-review evidence

The frozen benchmark contains 100 primary records, 25 + 25 optional secondary records,
and 10 calibration records. The validated Morris primary return contains 100 complete
E1-E5/E7 judgments (5 INCLUDE, 41 UNCERTAIN, 54 EXCLUDED); one evidence-conflict record
is held from scoring, leaving 99 currently scorable records. E6 was not assessed at that
stage.

The record selection and protocol-development process exposed some records and model
judgments. The return is evidence of completed author review, not an independent blinded
accuracy benchmark. The earlier 40-record provisional sample is workflow/calibration
material only and is not bound to the registered corpus.

Morris Chukhman separately reports having reviewed the AI-generated full-report
synthesis material. Original automated row-level authority is preserved. This
package-level statement does not create row-level approval dates, an independent second
reviewer, or a blinded validation study; scientifically unresolved boundaries remain
unresolved.

## Environment record

The repository declares Python `>=3.11`, `pydantic>=2.8`, and `requests>=2.32`, with
`pytest>=8.2` and `ruff>=0.6` as development dependencies. The release-preparation
validation environment was macOS with Python 3.14.0, Node.js 24.19.0, npm 11.17.0,
pytest 9.1.1, requests 2.34.2, and ruff 0.16.4. The active environment did not contain
Pydantic at audit time; the supplement builder therefore uses only the Python standard
library. These current versions are not asserted as historical run versions.

Model/provider/version/settings for executed screening and coding are preserved in the
run configs and reports. The terminal remaining campaign records OpenAI Responses API,
`gpt-5.6-luna`, reasoning effort `medium`, default service tier, and no tools. Earlier
runs retain their own bindings and must not be silently relabeled.

## Rights and availability

Morris Chukhman has authorized the MIT License for his original pipeline code and
scripts, and Creative Commons Attribution 4.0 International for his original
documentation, protocols, annotations, diagrams, and research derivatives. The
repository's `LICENSE`, `LICENSES/CC-BY-4.0.txt`, and `LICENSE_SCOPE.md` files define the
scope and attribution. The supplement carries the same license files. Zenodo metadata
uses CC BY 4.0 for the record's original research materials and identifies bundled
original code as MIT-licensed.

These licenses apply only to material Morris Chukhman owns and has authority to license.
They do not establish exclusive ownership of coauthored material and do not relicense
third-party excerpts, screenshots, bibliographic or provider metadata, full reports,
dependencies, or material with a separate notice. Collaborator-created manuscript
assets, annotations, code, data, or protocols require the relevant rights holder's
authorization unless an existing license already permits distribution. Git authorship
is provenance, not a legal ownership determination.

No DOI, ORCID, acceptance status, complete creator list, or collaborator authorization
is inferred from the local evidence. These remain publication metadata or rights-clearance
decisions where applicable.

The supplement omits full-text reports, raw provider responses, abstract-bearing exports,
internal BibTeX files of uncertain redistribution rights, and private or credentialed
material. Hashes, report identifiers, provenance paths, evidence locators, and short
retained excerpts remain sufficient to audit the reported aggregate calculations,
but they do not permit a clean-room reader to repeat every full-report judgment without
lawful access to the cited reports.

For release packaging, all redistributed copies of source passages apply an editorial
25-word cap and explicit omission markers while retaining identifiers, locators, and
source hashes. The cap is a packaging policy, not a legal permission threshold; word
count alone does not establish redistribution rights.
