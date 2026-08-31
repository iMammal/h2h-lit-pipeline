# Revised STAR Full-Text Screening and Coding

Prompt-Version: 1.0.0
Protocol-Version: 1.0.0
Rubric-Version: 1.0.0
Stage: full_text
Output-Schema-Version: 1.0.0

You are proposing a full-text screening and coding decision for the revised STAR. Your
proposal must remain linked to the supplied earlier UNCERTAIN title/abstract decision.
It is not authoritative corpus membership. Use only the supplied full-text input
snapshot and source location. Do not use outside knowledge or silently fill missing text.
Retrieval, PDF, OCR, parser, schema, or model failure leaves affected criteria UNCERTAIN
and is never itself a scientific exclusion.

## Eligibility criteria

Code every criterion as exactly `YES`, `NO`, or `UNCERTAIN`.

- `E1_life_science_application`: YES only for a supported life-science application.
  Clinical, biomedical, and surgical work use the same rule. Exclusively non-life-science
  work is NO.
- `E2_relational_multiscale_relevance`: YES for explicit relationships/networks or
  relationships derived from spatial, temporal, multivariate, image-derived, lineage,
  similarity, or other multiscale life-science data.
- `E3_interactive_visual_analytics`: YES only when humans use an interactive,
  analytically meaningful visual representation. Static presentation alone is NO.
- `E4_computational_assistance`: YES only for substantive computation inside the
  interactive analytic workflow. Ordinary rendering/direct manipulation/lookup alone is
  NO, and AI-related terminology alone is not evidence.
- `E5_human_analytic_relationship`: YES when humans meaningfully inspect, direct,
  validate, interpret, supervise, correct, collaborate with, or decide from the assisted
  process. Fully offline computation without that relationship is NO.
- `E6_administrative_scope`: YES only when the paper is no later than the documented
  retrieval end date, English full text is available, and it is a journal, conference,
  workshop, or preprint research paper. There is no lower date cutoff. Surveys and
  conceptual works qualify only when independently satisfying the system criteria.
- `E7_evidence_sufficiency`: YES when full-text evidence supports a defensible E1-E6
  determination for at least one candidate system. Remaining uncertainty is sent to
  human review. `EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION` may be finalized only by a
  human/adjudicated decision, so an LLM proposal must not turn technical failure into NO.

Aggregate semantics are fixed: any NO means EXCLUDED; all YES means ELIGIBLE; otherwise
UNCERTAIN. Proposed exclusion codes are limited to:
`EX_AFTER_RETRIEVAL_END_DATE`, `EX_NON_ENGLISH_FULL_TEXT`,
`EX_INELIGIBLE_DOCUMENT_TYPE`, `EX_NO_LIFE_SCIENCE_APPLICATION`,
`EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE`,
`EX_NO_INTERACTIVE_VISUAL_ANALYTICS`, `EX_NO_QUALIFYING_ASSISTANCE`,
`EX_NO_HUMAN_ANALYTIC_RELATIONSHIP`,
`EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION`, or `EX_OTHER_PROTOCOL_REASON`.
Duplicate and technical failures are not exclusions.

## Independent multilabel coding

Code every value independently as `PRESENT`, `ABSENT`, or `UNCERTAIN`; overlap is allowed.

Assistance modes:

- `Algorithmic`: substantive algorithmic transformation or organization.
- `Adaptive`: behavior changes from user, data, model, context, or interaction signals.
- `Conversational`: natural-language dialogue mediates analytics or explanation.
- `Immersive`: computation exploits immersive spatialization, embodiment, navigation, or
  multisensory properties as assistance; immersive display alone is insufficient.

Visualization modalities:

- `Desktop 2D`: operationally Planar 2D on any conventional screen, including phone or
  tablet; physical desktop hardware is not required.
- `Large Display`: analytic visualization on a large shared/display surface.
- `VR`: analytic visualization in virtual reality.
- `AR/MR`: spatially registered augmentation or mixed reality.
- `CAVE`: projection-based room-scale multisurface immersive environment.

Device form factor is separate. Controllers do not create modalities. Coordinated
analytic environments may receive multiple supported modalities.

Task categories:

- `Navigation and Multiscale Orientation`
- `Comparison and Differentiation`
- `Selection, Filtering, and Precision Interaction`
- `Sensemaking and Hypothesis Development`
- `Coordination and Collaborative Reasoning`

Tasks are overlapping analytic intents. Assistance, modality, task, historical relevance,
synthesis priority, and D/M/Target cannot change eligibility.

## Evidence and certainty

Every criterion requires one or more exact spans from the supplied full text. Every
`PRESENT` or `UNCERTAIN` classification requires exact spans. `ABSENT` may use no span but
requires a rationale. Spans use zero-based character offsets; `quote` must equal
`input_text[start:end]`, and `locator` must equal the supplied source location. Use only
`SUPPORTED` certainty with YES/NO/PRESENT/ABSENT and `UNCERTAIN` certainty with UNCERTAIN.

## Required JSON

Return one JSON object and no prose, using exactly these top-level keys and shapes:

```json
{
  "criteria": {
    "E1_life_science_application": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    "E2_relational_multiscale_relevance": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    "E3_interactive_visual_analytics": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    "E4_computational_assistance": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    "E5_human_analytic_relationship": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    "E6_administrative_scope": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    "E7_evidence_sufficiency": {"decision": "YES", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."}
  },
  "assistance_modes": [
    {"label": "Algorithmic", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    {"label": "Adaptive", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Conversational", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Immersive", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "visualization_modalities": [
    {"label": "Desktop 2D", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    {"label": "Large Display", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "VR", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "AR/MR", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "CAVE", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "tasks": [
    {"label": "Navigation and Multiscale Orientation", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Comparison and Differentiation", "state": "PRESENT", "certainty": "SUPPORTED", "evidence": [{"start": 0, "end": 10, "quote": "exact text", "locator": "full_text"}], "rationale": "..."},
    {"label": "Selection, Filtering, and Precision Interaction", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Sensemaking and Hypothesis Development", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."},
    {"label": "Coordination and Collaborative Reasoning", "state": "ABSENT", "certainty": "SUPPORTED", "evidence": [], "rationale": "..."}
  ],
  "primary_exclusion_reason": null,
  "secondary_exclusion_reasons": [],
  "overall_rationale": "..."
}
```

The criteria object must contain all E1-E7 keys. Each classification array must contain
every frozen value exactly once. Replace illustrative spans and rationales with exact
input-grounded values.
