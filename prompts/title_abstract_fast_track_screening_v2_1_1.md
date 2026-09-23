# STAR Title/Abstract Screening — Fast Track Evidence-ID Repair v2.1.1

Prompt-Version: 2.1.1
Protocol-Version: 2.0.0
Amendment-Version: 2.1.0
Stage: title_abstract
Output-Schema-Version: 1.4.0

You are proposing a title/abstract screening judgment. Use only the supplied
`evidence_units`, which reproduce the supplied title and abstract as deterministic
field/sentence units. Do not browse, retrieve metadata, use outside knowledge, or infer
from prior decisions. This is not a final paper-inclusion decision.

Assess E1–E5 and E7 as `YES`, `NO`, or `UNCERTAIN`. Do not assess E6: it is
`NOT_ASSESSED_AT_THIS_STAGE` and will be verified during full-report assessment.
Software independently recomputes the aggregate outcome and operational disposition.

## Criteria

- `E1_life_science_application`: YES only for a supported life-science application,
  including biomedical, clinical, health, neuroscience, ecology, marine science,
  biology, or bioinformatics. Exclusively non-life-science work is NO. If the supplied
  units do not establish the application, use UNCERTAIN.
- `E2_relational_multiscale_relevance`: YES for explicit networks/relationships or
  relationships derived from spatial, temporal, multivariate, image-derived, lineage,
  similarity, or other multiscale life-science data. Use NO only when supplied evidence
  supports failure; otherwise use UNCERTAIN.
- `E3_interactive_visual_analytics`: YES when humans use an interactive, analytically
  meaningful visual representation. Static presentation or an explicitly noninteractive
  workflow is NO. Unstated interaction is UNCERTAIN, not NO.
- `E4_computational_assistance`: YES for substantive computation inside the interactive
  workflow, such as transformation, organization, adaptation, recommendation,
  initiation, explanation, or computational use of immersive properties. Ordinary
  rendering, pan, zoom, click, literal manual selection, parameter entry, or lookup is
  insufficient. Use NO only when failure is supported; otherwise use UNCERTAIN.
- `E5_human_analytic_relationship`: YES when humans meaningfully inspect, direct,
  validate, interpret, supervise, correct, collaborate with, or decide from the assisted
  process. Fully offline computation without that relationship is NO. If the relationship
  is not established, use UNCERTAIN.
- `E7_evidence_sufficiency`: YES only when the supplied evidence identifies a candidate
  system and supports defensible E1–E5 determinations. Incomplete or missing evidence is
  UNCERTAIN. Do not use NO for E7.

A scientific NO must be supported by affirmative supplied evidence of failure. Missing
evidence is never affirmative evidence of scientific failure. A missing abstract normally
requires UNCERTAIN judgments and operational deferral unless the title explicitly supports
an individual criterion; a promising title does not establish all criteria.

## Evidence-unit contract

For each criterion return:

- `evidence_status`: `EVIDENCED` when one or more supplied units support the judgment,
  or `NOT_EVIDENCED_IN_SUPPLIED_METADATA` when the relevant evidence is absent;
- `evidence_ids`: only exact IDs from `evidence_units`. Use at least one ID with
  `EVIDENCED`; use an empty list with `NOT_EVIDENCED_IN_SUPPLIED_METADATA`;
- `rationale`: explain how the selected units support the decision, or what evidence is
  missing. Do not put quotations in the rationale and do not invent an evidence ID.

YES and NO require `EVIDENCED`. UNCERTAIN may be `EVIDENCED` when supplied units show
partial evidence, or `NOT_EVIDENCED_IN_SUPPLIED_METADATA` when the relevant evidence is
absent. E7 UNCERTAIN normally uses `NOT_EVIDENCED_IN_SUPPLIED_METADATA`; the absence of a
unit is the limitation and does not itself need a quotation. Software attaches the exact
original unit text after validating the selected IDs.

Use `SUPPORTED` certainty with YES or NO and `UNCERTAIN` certainty with UNCERTAIN.
Return one JSON object and no surrounding prose. Include exactly `criteria` and
`overall_rationale`. Within `criteria`, include E1–E5 and E7 exactly once. Do not include
E6, aggregate outcome, disposition, exclusion reasons, quotations, taxonomy labels, or
previous judgments.
