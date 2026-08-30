# Revised STAR Protocol

Status: **FROZEN for implementation**

Version: **1.0.0**

Frozen: **2026-08-30**

Companion coding rubric: [revised_star_coding_rubric.md](revised_star_coding_rubric.md)

This document is the authoritative methodological contract for regenerating the
literature corpus for the CGF revision of *From Hairballs to Hypotheses*. The companion
rubric is authoritative for operational coding. Historical prompts, classifications,
relevance scores, corpora, and the provisional D/M/Target materials remain provenance;
they do not override these documents.

## 1. Review Population and Evidence Roles

1. The unit of formal corpus eligibility is a paper reporting or analyzing one or more
   systems. Individual assistance mechanisms may be extracted for coding, but a
   mechanism is not an independent corpus member.
2. Every deduplicated record is screened under the same eligibility criteria. No date,
   relevance, assistance-mode, visualization-modality, task, or venue rule may be
   applied to only one classification cell.
3. Formal eligible-corpus membership requires a life-science application. Clinical and
   biomedical work are in scope. Surgical work is neither categorically included nor
   categorically excluded; it is evaluated under the same criteria as every other
   record.
4. Non-life-science work cannot enter the formal eligible corpus solely because its
   methods may transfer to life sciences. It may be retained as named seed evidence,
   background evidence, conceptual grounding, or transferable design context.
5. The scientific scope includes explicit networks and relationships derived from
   spatial, temporal, multivariate, image-derived, lineage, similarity, or other
   multiscale life-science data.
6. Eligible work must contain an interactive visual-analytics relationship among a
   human analyst, a visual representation, and a substantive computational assistance
   mechanism. Static presentation alone and ordinary direct manipulation alone do not
   qualify.

## 2. Administrative Scope

1. There is no lower publication-date cutoff. The documented retrieval end date of a
   regenerated search run is its only upper temporal boundary. Each run must preserve
   that date and timestamp in provenance.
2. The regenerated formal corpus requires English-language full text.
3. Journal, conference, workshop, and preprint research papers are permitted.
4. Surveys and conceptual works normally serve as background or seed evidence. They may
   enter the formal corpus only when they independently satisfy every eligibility
   criterion, including reporting or analyzing a qualifying interactive system rather
   than merely summarizing other eligible systems.
5. Venue or publication community is not an eligibility criterion.

## 3. Eligibility Decisions

1. Screening records criterion-level `YES`, `NO`, or `UNCERTAIN` decisions with evidence.
2. A record is eligible only when every required criterion is `YES`.
3. A conclusive `NO` produces an exclusion with one primary reason and optional
   secondary reasons.
4. If no criterion is `NO` and at least one criterion is `UNCERTAIN`, the record remains
   uncertain and is escalated to full text. Missing metadata, missing abstracts,
   retrieval failures, parser failures, or invalid model output cannot become scientific
   exclusions.
5. Uncertainty remaining after documented full-text escalation is sent to human review.
   Insufficient evidence is an exclusion reason only after that escalation is recorded.
6. Duplicate is a deduplication and provenance state, not an exclusion reason.

## 4. Classification Dimensions

### Assistance modes

The four submitted assistance modes are retained:

- `Algorithmic`
- `Adaptive`
- `Conversational`
- `Immersive`

They are overlapping, multilabel modes. A paper or system may receive any supported
combination. `Immersive` assistance means computational assistance that exploits
properties of an immersive environment, such as spatialization, embodiment, navigation,
or multisensory cues, as part of the analytic mechanism. Merely presenting a
visualization in VR, AR/MR, or CAVE does not establish Immersive assistance.

### Visualization modalities

The five submitted visualization modalities are retained:

- `Desktop 2D`
- `Large Display`
- `VR`
- `AR/MR`
- `CAVE`

Visualization modality is multilabel and is determined by how the visualization
environment is used analytically, not by device form factor. `Desktop 2D` is retained as
the manuscript-facing name; its operational meaning is **Planar 2D**. A phone or tablet
showing a conventional planar chart, network view, dashboard, spreadsheet, or similar
screen-based visualization is therefore `Desktop 2D`. A phone or tablet providing
spatially registered augmentation is `AR/MR`. A device used only as a controller does
not create a modality label. Coordinated analytic visualizations genuinely presented
across multiple environments may receive multiple labels.

Phone, tablet, desktop monitor, wall display, head-mounted display, and similar physical
forms may be recorded in a separate optional device-form-factor field. Mobile and Tablet
are not STAR visualization modalities.

### Tasks

The exact five submitted task categories are retained and coded as a multilabel,
cross-cutting synthesis lens:

- `Navigation and Multiscale Orientation`
- `Comparison and Differentiation`
- `Selection, Filtering, and Precision Interaction`
- `Sensemaking and Hypothesis Development`
- `Coordination and Collaborative Reasoning`

Task labels describe supported analytic intent. They do not determine eligibility and
are not treated as another mutually exclusive taxonomy axis.

### Locus of analytic agency and D/M/Target

Locus of analytic agency is an interpretive synthesis concept used to discuss how human
and computational initiative changes across systems. D/M/Target coding remains
exploratory and supplemental. It cannot change eligibility, formal corpus membership,
assistance-mode labels, visualization-modality labels, task labels, or synthesis
priority.

## 5. Corpus Membership and Synthesis Priority

1. `Eligible` and `Excluded` determine formal corpus membership.
2. `Core`, `Supporting`, and `Contextual` are assigned only after eligibility and govern
   synthesis priority, not membership.
3. Non-life-science seed or background evidence remains outside this priority scheme
   because it is outside the formal eligible corpus.
4. Historical relevance scores, historical `Off-Topic` decisions, single-label
   assistance/modality buckets, and historical corpus membership are immutable
   provenance only. They are not current eligibility or priority decisions.

## 6. Machine and Human Authority

1. LLM outputs are versioned proposed decisions, never authoritative ground truth.
2. Every proposed decision must preserve the rubric version, input snapshot, model and
   prompt provenance, criterion-level values, evidence locations, rationale, uncertainty,
   and technical status.
3. Human decisions supersede machine proposals for the dimensions reviewed. Human
   disagreement is resolved by a recorded adjudication; earlier decisions remain linked
   as provenance rather than being overwritten.
4. Historical machine and human annotations remain immutable and are explicitly marked
   historical.
5. Interpretive synthesis and claims about locus of analytic agency remain author work.

## 7. Human Validation Status

The final human-validation sample design is **not frozen in version 1.0.0**. A pilot will
be conducted on the regenerated corpus before the sample is selected. The design will
then be registered prospectively, before validation results are inspected, using:

- regenerated corpus size;
- assistance-mode, modality, task, and eligibility class balance;
- observed uncertainty and error prevalence in the pilot;
- rare-class coverage requirements; and
- available independent-coder and adjudication workload.

The registered design must specify the sample frame, selection probabilities or census
rules, random seed where applicable, coder independence and visibility of machine
outputs, adjudication procedure, missing-review handling, and planned validation
metrics.

## 8. Change Log from the Submitted STAR

### Preserved from the submission

- Life-science visual analytics centered on explicit or derived relational and
  multiscale structure.
- Clinical and biomedical applications within scope.
- Four assistance modes: Algorithmic, Adaptive, Conversational, and Immersive.
- Immersive assistance requires more than use of an immersive display.
- Five visualization modalities: Desktop 2D, Large Display, VR, AR/MR, and CAVE.
- Exact five task categories recovered from Sections 3.3.1-3.3.5.
- Assistance modes may overlap.
- LLM support is non-authoritative and authors retain interpretive authority.

### Operational clarifications

- Criterion-level `YES`/`NO`/`UNCERTAIN`, evidence requirements, and full-text
  escalation.
- Life-science application required for formal membership; transferable non-life-science
  work retained outside the formal corpus as seed/background evidence.
- No blanket surgical exclusion.
- English-language full text and the permitted research-paper document types.
- Explicit exclusion codes and separation of duplicate/technical states from exclusion.
- `Core`/`Supporting`/`Contextual` post-eligibility priority anchors.
- Desktop 2D operationalized as Planar 2D; device form factor stored separately.
- Multilabel modality and task coding for genuinely multi-environment or multi-task
  systems.
- D/M/Target restricted to supplemental analysis.

### Methodological repairs made in response to review

- Removed the Desktop 2D/Algorithmic-only 2019-present and relevance 4-5 corpus filter.
- Replaced single-label assistance and modality bucket assignment with independent
  multilabel coding.
- Separated eligible-corpus membership from synthesis priority.
- Reclassified historical relevance scores and classifications as provenance rather
  than current truth.
- Prevented model, parser, retrieval, and missing-evidence failures from silently
  becoming scientific exclusions.
- Made human supersession and adjudication explicit while deferring the final validation
  sample design until after a prospectively bounded pilot.
