# Revised STAR Title/Abstract Screening

Prompt-Version: 1.0.0
Protocol-Version: 1.0.0
Rubric-Version: 1.0.0
Stage: title_abstract
Output-Schema-Version: 1.0.0

You are proposing a title/abstract screening and coding decision for the revised STAR.
Your output is a machine proposal, not authoritative corpus membership. Use only the
provided input snapshot. Do not use outside knowledge, infer missing facts, or convert a
retrieval, parser, schema, or missing-text failure into scientific exclusion.

## Eligibility criteria

Code every criterion as exactly `YES`, `NO`, or `UNCERTAIN`.

- `E1_life_science_application`: YES only for a supported life-science application.
  Clinical, biomedical, and surgical work use the same rule. Exclusively non-life-science
  work is NO; unclear application is UNCERTAIN.
- `E2_relational_multiscale_relevance`: YES for explicit relationships/networks or
  relationships derived from spatial, temporal, multivariate, image-derived, lineage,
  similarity, or other multiscale life-science data. Unsupported or unclear relevance is
  UNCERTAIN.
- `E3_interactive_visual_analytics`: YES only when humans use an interactive,
  analytically meaningful visual representation. Static presentation or no qualifying
  visual interaction is NO; unclear interaction is UNCERTAIN.
- `E4_computational_assistance`: YES only for substantive computation inside the
  interactive analytic workflow. Ordinary rendering, pan, zoom, click, literal manual
  selection, direct parameter entry, or ordinary lookup alone is NO. Technology labels
  such as AI, intelligent, adaptive, or automatic are insufficient evidence.
- `E5_human_analytic_relationship`: YES when humans meaningfully inspect, direct,
  validate, interpret, supervise, correct, collaborate with, or decide from the assisted
  process. Fully offline computation without such a relationship is NO.
- `E6_administrative_scope`: YES only when the paper is no later than the documented
  retrieval end date, English full text is available, and it is a journal, conference,
  workshop, or preprint research paper. There is no lower date cutoff. Surveys and
  conceptual works qualify only if they independently satisfy the system criteria.
- `E7_evidence_sufficiency`: YES only when the available evidence supports a defensible
  E1-E6 determination for at least one candidate system. Missing or incomplete evidence
  at this stage is UNCERTAIN, not NO.

Aggregate semantics are fixed: any NO means EXCLUDED; all YES means ELIGIBLE; otherwise
UNCERTAIN. An UNCERTAIN title/abstract result requires full-text escalation. Supply one
primary frozen exclusion code and supported secondary codes for a proposed exclusion:
`EX_AFTER_RETRIEVAL_END_DATE`, `EX_NON_ENGLISH_FULL_TEXT`,
`EX_INELIGIBLE_DOCUMENT_TYPE`, `EX_NO_LIFE_SCIENCE_APPLICATION`,
`EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE`,
`EX_NO_INTERACTIVE_VISUAL_ANALYTICS`, `EX_NO_QUALIFYING_ASSISTANCE`,
`EX_NO_HUMAN_ANALYTIC_RELATIONSHIP`,
`EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION`, or `EX_OTHER_PROTOCOL_REASON`.
Duplicate, missing abstract/PDF, and retrieval/parser/model failure are not exclusions.

## Independent multilabel coding

Code every value independently as `PRESENT`, `ABSENT`, or `UNCERTAIN`. Overlap is allowed.

Assistance modes:

- `Algorithmic`: substantive algorithmic transformation or organization in the workflow.
- `Adaptive`: behavior changes from user, data, model, context, or interaction signals.
- `Conversational`: natural-language dialogue mediates analytic operations or explanation.
- `Immersive`: computational assistance exploits immersive properties such as
  spatialization, embodiment, navigation, or multisensory cues. Immersive display alone
  is insufficient.

Visualization modalities:

- `Desktop 2D`: operationally Planar 2D, including conventional planar visualization on
  phones or tablets; it is not restricted to desktop hardware.
- `Large Display`: analytic visualization presented on a large shared/display surface.
- `VR`: analytic visualization in virtual reality.
- `AR/MR`: spatially registered augmentation or mixed reality.
- `CAVE`: projection-based room-scale multisurface immersive environment.

Device form factor is not modality. A controller does not create a modality. Coordinated
analytic presentation across supported environments may receive multiple labels.

Task categories:

- `Navigation and Multiscale Orientation`
- `Comparison and Differentiation`
- `Selection, Filtering, and Precision Interaction`
- `Sensemaking and Hypothesis Development`
- `Coordination and Collaborative Reasoning`

Tasks describe supported analytic intent and are overlapping. None of these labels,
historical relevance, synthesis priority, or D/M/Target may affect eligibility.

## Evidence and certainty

Every criterion requires at least one exact evidence span. Every `PRESENT` or `UNCERTAIN`
classification requires at least one exact evidence span. `ABSENT` may use an empty span
list but still requires a rationale. A span uses zero-based character offsets into the
provided input text; `quote` must equal `input_text[start:end]`, and `locator` must equal
the supplied source location. Use `SUPPORTED` certainty with YES/NO/PRESENT/ABSENT and
`UNCERTAIN` certainty with UNCERTAIN. Do not output probabilities or other certainty
labels.

## Required JSON

Return one JSON object and no surrounding prose. Use exactly these top-level keys:

```json
{
  "criteria": {
    "E1_life_science_application": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    "E2_relational_multiscale_relevance": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    "E3_interactive_visual_analytics": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    "E4_computational_assistance": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    "E5_human_analytic_relationship": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    "E6_administrative_scope": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    "E7_evidence_sufficiency": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."}
  },
  "assistance_modes": [
    {"label": "Algorithmic", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    {"label": "Adaptive", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Conversational", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Immersive", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "visualization_modalities": [
    {"label": "Desktop 2D", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    {"label": "Large Display", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "VR", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "AR/MR", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "CAVE", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "tasks": [
    {"label": "Navigation and Multiscale Orientation", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Comparison and Differentiation", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "title_abstract"}], "rationale": "..."},
    {"label": "Selection, Filtering, and Precision Interaction", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Sensemaking and Hypothesis Development", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Coordination and Collaborative Reasoning", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "primary_exclusion_reason": null,
  "secondary_exclusion_reasons": [],
  "overall_rationale": "..."
}
```

The three classification arrays must contain each frozen vocabulary value exactly once.
Replace illustrative spans and rationales with exact input-grounded values.
