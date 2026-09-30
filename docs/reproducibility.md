# H2H2 CGF reproducibility guide

## Scope

This guide reproduces reported aggregates from preserved inputs and responses. It does
not rerun mutable literature services or paid models, claim exhaustive screening, or
turn automated judgments into independent human ground truth.

The audited research-implementation checkpoint is
`27cf867f598041f846f2ce57c6a736a89539dc98`. At audit time, local `main` and the cached
`origin/main` reference pointed to that commit. No fetch was performed, so this is not
evidence of the current public GitHub state. The previously observed public checkpoint
`869f0cd932df282e0c6accc4bf0067813da198e2` is an ancestor, six commits behind the
audited local checkpoint.

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

## Offline build and verification

From a checkout containing the preserved ignored artifacts:

```bash
python scripts/build_reproducibility_supplement.py \
  --repository-root . \
  --output-root outputs/staging/h2h2-cgf-reproducibility-release-v1-20260930 \
  --research-commit 27cf867f598041f846f2ce57c6a736a89539dc98

python scripts/build_reproducibility_supplement.py \
  --verify outputs/staging/h2h2-cgf-reproducibility-release-v1-20260930/supplement
```

The first command creates a fresh directory and refuses to overwrite an existing
package. The second validates the package manifest and CSV invariants. The build also
extracts the ZIP into a temporary directory and verifies it without relying on private
absolute paths.

The authoritative manuscript input is
`outputs/staging/manuscript-input-20260925/STAR-editable-source-expanded-map-20260925.zip`
(SHA-256 `ed8c302bf4bc2d29a44620ee57c1c73ba7e939b98104994707851cbf0cd95d8f`).
It remains unchanged and is not redistributed by the supplement.

## What can be regenerated

The package supports offline verification of:

- provider/query-family definitions and executed source counts;
- identification and screening equations;
- the 9,505 unique candidate identities and their machine-coded labels, using a reduced
  export that omits abstracts;
- full-report/system denominators and eligibility outcomes;
- supported assistance × modality counts and the published map from corrected inputs;
- flow-ledger tables and the two-panel diagram structure.

Exact external-service reruns are not expected to reproduce archived retrieval results.
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

The repository currently declares `Proprietary` in `pyproject.toml`. No DOI, ORCID,
acceptance status, or open license is established by the available local evidence.
Before publication, the authors must choose and record licenses separately for code,
original documentation, and redistributed metadata/evidence.

The supplement omits full-text reports, raw provider responses, abstract-bearing exports,
internal BibTeX files of uncertain redistribution rights, and private or credentialed
material. Hashes, report identifiers, provenance paths, evidence locators, and short
permissible excerpts remain sufficient to audit the reported aggregate calculations,
but they do not permit a clean-room reader to repeat every full-report judgment without
lawful access to the cited reports.
