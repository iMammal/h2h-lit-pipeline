# Audit of the existing 40-record human-validation sample

## Conclusion

The existing 40 records are suitable for testing the review workflow and, with strict
separation from reported evaluation metrics, for reviewer/rubric calibration. They are
not suitable as an independent evaluation set, a prevalence sample, or the planned
100-record benchmark.

The sample was selected after model inference and contains 21 model-stratified
`EXCLUDED` records and 19 model-stratified `UNCERTAIN` records. No model-stratified
`ELIGIBLE` records were available for the requested quota, so the 13-record shortfall was
redistributed. This construction is useful for exercising exclusions and uncertainty,
but it is not independent of the evaluated model and does not represent the target
corpus distribution.

## Source and selection lineage

1. The source lineage is the disposable provisional validation run
   `star-pipeline-validation-001`, not the registered production review dataset.
2. A 750-record cohort was built from provisional ACM and PubMed material across five
   query families. The intended design used 50 records per family-and-route stratum.
   Overlap produced a 497-record quota union and a deterministic global fill added 253
   records. Selection used the configuration hash, provisional canonical ID, and stratum
   ID. Content, eligibility, and JFR25 membership were not inspected for that selection.
3. A 250-record inference sample was selected from the 750-record cohort by the lowest
   SHA-256 of the frozen configuration, cohort, prompt, stage salt, and provisional
   canonical identity. Content, source/family, JFR25 membership, and prior LLM results
   were not inspected for this step.
4. Those 250 records were processed with OpenAI `gpt-5-mini-2025-08-07`, prompt
   `revised_star_title_abstract_screening_pilot5d_v1_3_0.md`, output schema 1.3.0.
5. The 40-record human sample was then selected from valid model proposals. The intended
   quotas were 13 `ELIGIBLE`, 13 `EXCLUDED`, and 14 `UNCERTAIN`. The initial eligible
   count was zero. Deterministic redistribution produced the final 21 `EXCLUDED` and 19
   `UNCERTAIN` records. Exclusions were selected round-robin by the model's primary
   exclusion reason; uncertain records were selected round-robin by uncertain criteria
   and missing-abstract state.

Key bindings:

- 750-record cohort artifact hash:
  `8073b0da19a1f3d6c07438e892ee2277c1510d01ed9f31b50ffce0fb2bed68fc`
- 250-record inference sample artifact hash:
  `2f16b6beddaf1dea733723c0a216c1c33e191cc16b2aee0c00a179bf5b2bb5bb`
- 40-record CSV SHA-256:
  `72f4fe6e0efe8bc7f606b8f548c18f72eb7659c8072b2f1330a95313b3b2dcd9`
- 40-record sampling-manifest SHA-256:
  `3b74be0485c731abb370203d4d83c8b347622bc2cf48decfb7d4283a1a936c11`
- Stage 5D run ID: `provisional-stage5d:08548768d0eaf468e8ad63bf`

## Relationship to the registered corpus

The registered production review dataset contains 140,959 canonical records and is
bound to raw SHA-256
`44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2`.

The 40-record sample has no binding to that dataset, no stored production-canonical
crosswalk, and no demonstrated sampling frame over those 140,959 records. Its IDs use
the `provisional-canonical:` namespace; the registered dataset uses the `canonical:`
namespace. The provisional run explicitly states `DISCARD_ONLY`,
`production_import_allowed: false`, and `production_review_dataset: false`.

Therefore, the available evidence does not establish that the 40 records are a subset
of the registered corpus. Individual works could overlap bibliographically, but that
would require a separately reviewed identity crosswalk; it must not be inferred from
these IDs or from shared retrieval concepts.

## Prior use and blinding

- The source CSV's human E1-E7, status, reason, and notes fields are blank.
- The canonical review workspace has no returned review files. Its baseline
  reconciliation reports 40 missing reviews, zero single reviews, and zero independent
  double reviews.
- Repository evidence does not show completed human review, human calibration,
  prompt-tuning use, or reported model evaluation based on these 40 records. This is an
  evidence statement, not proof that no off-repository viewing occurred.
- The records have already received model proposals because those proposals determined
  the 40-record selection strata.
- The blinded CSV and record packets expose stable provisional IDs, titles, abstracts,
  publication years, criteria, and empty response fields. They do not expose model
  proposal status, model output, sampling reason, expected label, or another reviewer's
  judgment.
- The sampling manifest is unblinded. It exposes model-stratification status and, by
  record, either the sampling exclusion reason or uncertain criteria. It must remain
  internal until blinded reviews are complete.

## Suitability

| Intended use | Assessment | Reason |
|---|---|---|
| Workflow testing | Suitable | Stable IDs, 40 blinded packets, all criteria, missing-review baseline, and no completed returns exercise assignment, partial return, correction, and reconciliation paths. |
| Reviewer/rubric calibration | Conditionally suitable | The model-conditioned mix deliberately exercises exclusions and uncertainty. Keep it disjoint from reported evaluation, do not expose the manifest during coding, and describe it as calibration material rather than a performance sample. |
| Independent model evaluation | Not suitable | Selection occurred after this model's predictions, contains no model-eligible stratum, is not a probability sample, and lacks a binding to the registered corpus. |
| Planned 100-record benchmark | Not supported | It has 40 records, a different provisional sampling frame, and a different model-conditioned purpose. No evidence authorizes relabeling it as the planned benchmark. |

## Canonical workspace preservation

The canonical workspace remains
`outputs/staging/human-validation-review-workspace-v1-20260920T230844Z` with package
manifest SHA-256
`db8a93a6c1949a3e74e035dfa80312fc05e03e906a11c4077ded75fd29aa1d83`.
Its 40 assignment rows remain unassigned with null reviewer IDs and null due dates.

