# STAR title/abstract category coding v1.0.0

Prompt-Version: 1.0.0
Taxonomy-Version: frozen-stage5c-1.2.0
Output-Schema-Version: coding-1.0.0
Stage: provisional_title_abstract_category_coding

Code the supplied candidate using only its deterministic title/abstract `evidence_units`.
Do not browse, retrieve metadata, use prior screening rationales, or infer from outside
knowledge. These are provisional abstract-based categories, not verified full-report
extractions or final paper-eligibility decisions.

Return every frozen label exactly once with state `PRESENT`, `ABSENT`, or `UNCERTAIN`.
Overlap is allowed. Do not balance categories or force a label to PRESENT. Use
`UNCERTAIN` when the supplied evidence does not resolve a label.

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
  phones or tablets; this is not a hardware label.
- `Large Display`: analytic visualization on a large shared or display surface.
- `VR`: analytic visualization in virtual reality.
- `AR/MR`: spatially registered augmentation or mixed reality.
- `CAVE`: projection-based room-scale multisurface immersive environment.

Device form factor is separate. A controller creates no modality. Multiple modalities
may be PRESENT for coordinated analytic presentation across environments.

Task categories:

- `Navigation and Multiscale Orientation`
- `Comparison and Differentiation`
- `Selection, Filtering, and Precision Interaction`
- `Sensemaking and Hypothesis Development`
- `Coordination and Collaborative Reasoning`

Tasks are overlapping analytic intents. Assistance, modality, and task labels do not
change the preserved screening result.

For each label return `state`, `evidence_status`, `evidence_ids`, and `rationale`.
`PRESENT` requires `EVIDENCED` and at least one exact supplied evidence ID. `ABSENT` may
use evidenced contradiction or `NOT_EVIDENCED_IN_SUPPLIED_METADATA` with an empty ID
list. `UNCERTAIN` may cite partial evidence or use the explicit absent-evidence status.
Never invent, duplicate, paraphrase, or normalize an evidence ID. Explanations remain
separate from the attached source text.

Also return `workflow_support_review`:

- `SUPPORTED`: the supplied evidence supports the qualifying integrated human-analysis,
  meaningful-visualization, and substantive-computational-assistance workflow.
- `UNSUPPORTED`: supplied evidence affirmatively contradicts that qualifying workflow.
- `UNKNOWN`: supplied evidence does not resolve the issue.

`SUPPORTED` and `UNSUPPORTED` require valid evidence IDs. `UNKNOWN` may use explicit
absent-evidence status. This is a review flag only and never silently changes the earlier
screening decision.

Return exactly `assistance_modes`, `visualization_modalities`, `tasks`,
`workflow_support_review`, and `overall_rationale` as one JSON object with no prose.
