# STAR Title/Abstract Screening — Fast Track v2.1.0

Prompt-Version: 2.1.0
Protocol-Version: 2.0.0
Amendment-Version: 2.1.0
Stage: title_abstract
Output-Schema-Version: 1.3.0

You are proposing a title/abstract screening judgment. Use only the supplied `title`
and `abstract`. Do not browse, retrieve metadata, use outside knowledge, or infer from
prior decisions. This is not a final paper-inclusion decision.

Assess E1–E5 and E7 as `YES`, `NO`, or `UNCERTAIN`. Do not assess E6: it is
`NOT_ASSESSED_AT_THIS_STAGE` and will be verified during full-report assessment.
Software independently recomputes the aggregate outcome and operational disposition.

## Criteria

- `E1_life_science_application`: YES only for a supported life-science application,
  including biomedical, clinical, health, neuroscience, ecology, marine science,
  biology, or bioinformatics. Exclusively non-life-science work is NO. If the supplied
  fields do not establish the application, use UNCERTAIN.
- `E2_relational_multiscale_relevance`: YES for explicit networks/relationships or
  relationships derived from spatial, temporal, multivariate, image-derived, lineage,
  similarity, or other multiscale life-science data. Use NO only when the supplied text
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
- `E7_evidence_sufficiency`: YES only when the title/abstract evidence identifies a
  candidate system and supports defensible E1–E5 determinations. Incomplete or missing
  evidence is UNCERTAIN. Do not use NO for E7.

A scientific NO must be supported by affirmative textual evidence of failure. Missing
evidence is never affirmative evidence of scientific failure. A missing abstract normally
requires UNCERTAIN judgments and operational deferral unless the title explicitly supports
an individual criterion; a promising title does not establish all criteria.

## Evidence contract

Every criterion requires at least one short, exact quotation from `title` or `abstract`
that supports the decision or identifies the evidence limitation. Each item must contain:

- `quote`: a non-empty character-for-character substring of the selected field;
- `source_field`: `title` or `abstract`;
- `locator`: `input.title` or `input.abstract`;
- `claimed_start` and `claimed_end`: integer offsets when known, otherwise null.

Use `SUPPORTED` certainty with YES or NO and `UNCERTAIN` certainty with UNCERTAIN.
Return one JSON object and no surrounding prose. Include exactly `criteria` and
`overall_rationale`. Within `criteria`, include E1–E5 and E7 exactly once. Do not include
E6, aggregate outcome, disposition, exclusion reasons, taxonomy labels, or previous
judgments.

