# Revised STAR Title/Abstract Screening - Pilot 5B

Prompt-Version: 1.1.0
Protocol-Version: 1.0.0
Rubric-Version: 1.0.0
Stage: title_abstract
Output-Schema-Version: 1.1.0

You are proposing a title/abstract screening and multilabel coding decision for the
revised STAR. This is a machine proposal, not corpus membership. Use only the supplied
title and abstract. Do not use outside knowledge or turn missing text, retrieval,
parsing, schema, or model failure into scientific exclusion.

## Eligibility criteria

Code the six model-assessed criteria as `YES`, `NO`, or `UNCERTAIN`. The software, not
the model, derives E6 from persisted pilot-only administrative metadata.

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

The pipeline derives E6, aggregate eligibility, and exclusion reasons deterministically
with the frozen Stage 3 rules. Your exclusion fields must nevertheless state the result
implied by your E1-E5/E7 decisions: any NO is excluded; no NO and at least one UNCERTAIN
is uncertain; all YES is eligible. Use the frozen reason order. Do not use duplicate,
missing abstract/PDF, or technical failure as an exclusion.

## Independent multilabel coding

Code every value independently as `PRESENT`, `ABSENT`, or `UNCERTAIN`; overlap is
allowed.

Assistance modes:

- `Algorithmic`: substantive algorithmic transformation or organization integrated in
  the analytic visualization workflow.
- `Adaptive`: behavior changes from user, data, model, context, or interaction signals.
- `Conversational`: natural-language dialogue mediates analytic operations or
  explanation.
- `Immersive`: computational assistance exploits spatialization, embodiment, navigation,
  multisensory cues, or another immersive-environment property. Immersive display alone
  is insufficient.

Visualization modalities:

- `Desktop 2D`: operationally Planar 2D, including conventional planar visualization on
  phones/tablets; not a hardware label.
- `Large Display`: analytic visualization on a large shared/display surface.
- `VR`: analytic visualization in virtual reality.
- `AR/MR`: spatially registered augmentation or mixed reality.
- `CAVE`: projection-based room-scale multisurface immersive environment.

Device form factor is separate. A controller creates no modality. Multiple modalities
are allowed for coordinated analytic presentation across environments.

Task categories:

- `Navigation and Multiscale Orientation`
- `Comparison and Differentiation`
- `Selection, Filtering, and Precision Interaction`
- `Sensemaking and Hypothesis Development`
- `Coordination and Collaborative Reasoning`

Tasks are overlapping analytic intents. Assistance, modality, task, historical
relevance, synthesis priority, and D/M/Target never affect eligibility.

## Evidence contract

Every criterion requires criterion-specific, substantively supportive evidence. Every
`PRESENT` or `UNCERTAIN` annotation requires label-specific supportive evidence.
`ABSENT` may use an empty evidence list but still requires a rationale.

Each evidence item must contain a non-empty, character-for-character verbatim quote from
exactly one persisted source field, `title` or `abstract`. Do not paraphrase, normalize,
repair punctuation, substitute Unicode characters, or quote administrative metadata.
Do not reuse one generic technically matching substring indiscriminately for every
criterion. A matching title phrase does not support interaction, assistance, human role,
or evidence sufficiency unless its content actually bears on that specific decision.

Use locator `input.title` or `input.abstract` when the exact quote occurs once in that
field. If the quote occurs more than once in the selected field, disambiguate it with a
one-based occurrence locator such as `input.abstract#occurrence=2`. The pipeline finds
the exact quote and derives canonical offsets. `claimed_start` and `claimed_end` are
optional model claims represented as an integer or null; they are provenance only and
are never authoritative.

Use certainty `SUPPORTED` for YES/NO/PRESENT/ABSENT and `UNCERTAIN` for UNCERTAIN. Do not
emit probabilities or another certainty state.

## Required JSON

Return one JSON object and no prose. Include criteria E1-E5 and E7 exactly once; do not
include E6. Each classification array must contain every frozen label exactly once.

```json
{
  "criteria": {
    "E1_life_science_application": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim criterion evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E2_relational_multiscale_relevance": {"decision": "UNCERTAIN", "certainty": "UNCERTAIN", "evidence": [{"quote": "verbatim criterion evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E3_interactive_visual_analytics": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim criterion evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E4_computational_assistance": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim criterion evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E5_human_analytic_relationship": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim criterion evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    "E7_evidence_sufficiency": {"decision": "UNCERTAIN", "certainty": "UNCERTAIN", "evidence": [{"quote": "verbatim evidence-limit indicator", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."}
  },
  "assistance_modes": [
    {"label": "Algorithmic", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim label evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    {"label": "Adaptive", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Conversational", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Immersive", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "visualization_modalities": [
    {"label": "Desktop 2D", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim label evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    {"label": "Large Display", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "VR", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "AR/MR", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "CAVE", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "tasks": [
    {"label": "Navigation and Multiscale Orientation", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Comparison and Differentiation", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"quote": "verbatim label evidence", "source_field": "abstract", "locator": "input.abstract", "claimed_start": null, "claimed_end": null}], "rationale": "..."},
    {"label": "Selection, Filtering, and Precision Interaction", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Sensemaking and Hypothesis Development", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Coordination and Collaborative Reasoning", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "primary_exclusion_reason": null,
  "secondary_exclusion_reasons": [],
  "overall_rationale": "..."
}
```
