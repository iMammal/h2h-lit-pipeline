# Revised STAR Title/Abstract Screening - Stage 5D Eligibility-only Control

Prompt-Version: 1.3.0
Protocol-Version: 1.0.0
Rubric-Version: 1.0.0
Stage: title_abstract
Output-Schema-Version: 1.3.0

You are proposing a title/abstract eligibility-screening decision for the revised STAR.
This is a machine proposal, not corpus membership. Use only the supplied title and
abstract. Do not use outside knowledge or turn missing text, retrieval, parsing, schema,
or model failure into scientific exclusion.

## Eligibility criteria

Code the six model-assessed criteria as `YES`, `NO`, or `UNCERTAIN`. Software, not the
model, derives E6 from persisted administrative metadata, and software derives aggregate
eligibility and exclusion reasons from E1-E7. Do not output E6, an aggregate decision,
or exclusion reasons.

- `E1_life_science_application`: YES only for a supported life-science application,
  including biomedical, clinical, health, neuroscience, ecology, marine science,
  biology, or bioinformatics. Surgical work uses the same rule and is not excluded as a
  category. Exclusively non-life-science work is NO; unclear application is UNCERTAIN.
- `E2_relational_multiscale_relevance`: YES for explicit networks/relationships or
  relationships derived from spatial, temporal, multivariate, image-derived, lineage,
  similarity, or other multiscale life-science data. NO only when absence is supported;
  otherwise UNCERTAIN.
- `E3_interactive_visual_analytics`: YES when humans use an interactive, analytically
  meaningful visual representation. Static presentation or no qualifying visual
  interaction is NO; unclear interaction is UNCERTAIN.
- `E4_computational_assistance`: YES for substantive computation inside the interactive
  workflow, such as transformation, organization, adaptation, recommendation,
  initiation, explanation, or computational use of immersive properties. Ordinary
  rendering, pan, zoom, click, literal manual selection, parameter entry, or lookup is
  insufficient. Technology terms alone are insufficient evidence.
- `E5_human_analytic_relationship`: YES when humans meaningfully inspect, direct,
  validate, interpret, supervise, correct, collaborate with, or decide from the assisted
  process. Fully offline computation without that relationship is NO.
- `E7_evidence_sufficiency`: YES only when title/abstract evidence identifies a candidate
  system and supports defensible E1-E5 determinations. Incomplete evidence is UNCERTAIN
  and requires escalation. Never return E7 NO at title/abstract machine screening.

## Criterion-specific evidence contract

Every criterion requires evidence that substantively supports the proposition represented
by that criterion. Exact substring matching is necessary but not sufficient. A generic
title fragment, a generic phrase such as `visual analytics`, or a technology name alone
does not establish E2, E3, E4, E5, or E7. For example, use a passage identifying an
interactive analytic action for E3, substantive computation inside the workflow for E4,
and a human role in the process for E5.

A quotation may support more than one decision only when that same passage substantively
establishes each of those decisions. Do not reuse it merely because it is an exact
substring or because the JSON schema requires evidence.

Each evidence item must contain a non-empty, character-for-character verbatim quote from
exactly one persisted source field, `title` or `abstract`. Do not paraphrase, normalize,
repair punctuation, substitute Unicode characters, or quote administrative metadata.
Use locator `input.title` or `input.abstract` when the exact quote occurs once in that
field. If the quote occurs more than once in the selected field, disambiguate it with a
one-based occurrence locator such as `input.abstract#occurrence=2`. The pipeline finds
the exact quote and derives canonical offsets. `claimed_start` and `claimed_end` are
provenance only and are never authoritative.

Use certainty `SUPPORTED` for YES/NO and `UNCERTAIN` for UNCERTAIN. Do not emit
probabilities or another certainty state.

## Required JSON

Return one JSON object and no prose. Include criteria E1-E5 and E7 exactly once; do not
include E6, aggregate eligibility, primary/secondary exclusion reasons, assistance modes,
visualization modalities, task classifications, or any other derived outcome.

```json
{
  "criteria": {
    "E1_life_science_application": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim life-science application evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E2_relational_multiscale_relevance": {"decision": "UNCERTAIN", "certainty": "UNCERTAIN", "evidence": [{"quote": "verbatim relational or multiscale evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E3_interactive_visual_analytics": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim interactive analytic-action evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E4_computational_assistance": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim substantive computation evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E5_human_analytic_relationship": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim human analytic-role evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E7_evidence_sufficiency": {"decision": "UNCERTAIN", "certainty": "UNCERTAIN", "evidence": [{"quote": "verbatim evidence-limit or candidate-system evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."}
  },
  "overall_rationale": "..."
}
```
