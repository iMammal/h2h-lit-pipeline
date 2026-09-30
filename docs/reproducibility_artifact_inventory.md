# Reproducibility artifact inventory

Status labels are release decisions, not statements about whether a local file exists.
`INCLUDED` means the curated supplement contains the file or a documented reduced
derivative. `EXTERNAL/RESTRICTED` means a hash-bound local prerequisite is intentionally
not redistributed. `MISSING` means the terminal manifest references an artifact that is
not present locally. `HISTORICAL/SUPERSEDED` means the artifact is preserved but does not
control current totals.

| Material | Authoritative path or binding | Status | Release treatment |
|---|---|---|---|
| Exact production query families | `config/star_production_query_plan_v1.json` | INCLUDED | Exact provider-native strings, fields, request forms, limits, and provider constraints |
| ACM execution contracts | `config/star_acm_field_execution_contract_v1.json`; `config/star_acm_export_partition_contract_v1.json` | INCLUDED | UI/export constraints; executed ACM family dataset bindings are summarized separately |
| Executed retrieval state | `outputs/production/star-external-retrieval-wave-001/execution/execution_state.json` | INCLUDED | Reduced execution summary; raw state remains hash-bound locally |
| Registered source inputs | `GlobalIdentificationMerge/v1/input_inventory.json` | INCLUDED | Dataset paths, sizes, hashes, roles, and source counts; datasets themselves excluded |
| Global identity reconciliation | `GlobalIdentificationMerge/v1/reconciliation.json` | INCLUDED | Controls occurrence/canonical totals and related-version count |
| Registered 3.08 GB corpus | `GlobalIdentificationMerge/v1/review_dataset.json`; SHA-256 `44e418…15b2` | EXTERNAL/RESTRICTED | Not copied or deserialized; redistribution rights and package size are prohibitive |
| Identity overlay | `PriorSurveyIdentityAdjudication/v1/identity_confirmation_overlay.json`; SHA-256 `c287b6…251d` | INCLUDED | Exact adjudication overlay |
| arXiv snapshot substitution | `arXivSnapshotV303/retrieval_method_amendment_v2.json` | INCLUDED | Documents substitution after live-provider failures |
| Prior-survey imports | executed EBK25/FP19/JFR25 authorization records | INCLUDED | Authorization/provenance records only; source workbooks remain restricted |
| Raw provider responses | source-specific `responses/` trees | EXTERNAL/RESTRICTED | Not redistributed; terms and embedded metadata were not cleared |
| Raw ACM/IEEE and internal BibTeX exports | local execution/import evidence | EXTERNAL/RESTRICTED | Not required for aggregate reproduction; rights uncertain |
| Frozen protocol and coding rubric | `docs/revised_star_protocol.md`; `docs/revised_star_coding_rubric.md` | INCLUDED | Authoritative version 1.0.0 |
| Screening amendments | protocol 2.0.0, fast-track 2.1.0, precision 2.2.0 | INCLUDED | Scope-specific precedence preserved |
| Final screening/coding prompts and settings | precision/coding prompts and remaining-campaign config | INCLUDED | Exact local text/settings; run reports preserve executed lineage |
| Screening response schema | `config/title_abstract_screening_return_schema_v2_0_0.json` | INCLUDED | E6-deferred human-return schema |
| Terminal 9,505 candidate export | `combined_categorized_candidates.csv`; SHA-256 `08ecc6…90b` | INCLUDED | Reduced derivative omits abstracts/evidence text; IDs and labels retained |
| Complete candidate census | `candidate-census-full-report-expansion-v1-20260925` label, cell, task, abstract, and duplicate tables | INCLUDED | Machine-coded discovery landscape remains separate from supported full-report counts |
| Terminal manifest-named outcome CSVs | several `combined_*.csv` files named in completion manifest | MISSING | Counts remain bound to terminal JSON reports; absent files are not represented as zero |
| 40-record provisional validation sample | `outputs/provisional/star-pipeline-validation-001/` | HISTORICAL/SUPERSEDED | Workflow/calibration only; no registered-corpus binding |
| Frozen human-review packets | `outputs/title-abstract-benchmark-v1-20260920/` | INCLUDED | Sampling manifest and instructions; XLSX packets excluded from public supplement |
| Completed Morris review | `title-abstract-screening-v2-return-morris-20260922-v2` | INCLUDED | Validation report and reduced result; exposure/independence limitations retained |
| Earlier full-report waves | anchor/v2/expanded/targeted/sparse-cell packages | HISTORICAL/SUPERSEDED | Preserved as wave provenance; corrected cumulative terminal matrices control |
| Corrected cumulative full-report state | `target-cell-complete-pass-v1-20260926` | INCLUDED | Reduced system/report, eligibility, placement, and count tables |
| Copyrighted full reports and extracted text | wave-specific `reports/` and `extracted/` | EXTERNAL/RESTRICTED | Not redistributed; hashes/identifiers/locators retained |
| Figure 2 outputs and generators | target-cell package SVG/PDF plus `generate_target_map.mjs` and inputs | INCLUDED | Raster preview omitted; SVG/PDF and source retained |
| Flow diagram ledger | `study-selection-flow-count-ledger-v1-20260926` | INCLUDED | Counts, relationships, unit crosswalk, structure, and builder |
| Current manuscript ZIP | `STAR-editable-source-expanded-map-20260925.zip`; SHA-256 `ed8c30…5d8f` | EXTERNAL/RESTRICTED | Bound as current source, preserved unchanged, not included |
| January manuscript and anchor draft | earlier manuscript/synthesis artifacts | HISTORICAL/SUPERSEDED | May explain history but must not be cited as current manuscript source |
| Credentials, signed URLs, headers, caches, environments | various local-only locations | EXTERNAL/RESTRICTED | Always excluded |
| Project DOI, author ORCIDs, acceptance status | no verified local record | MISSING | Must be supplied by authors; never inferred |
| Release license decision | `pyproject.toml` says `Proprietary` | MISSING | Resolve code/data/document licenses before public release |

The generated supplement contains a machine-readable `artifact_inventory.csv` with
full hashes, source versions, relationships, and qualifications for every included or
bound item.
