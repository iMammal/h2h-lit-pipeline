# STAR Title/Abstract Screening — High-Precision Candidate Pass v2.2.0

Prompt-Version: 2.2.0
Protocol-Version: 2.0.0
Amendment-Version: 2.2.0
Stage: title_abstract_precision_rescreen
Output-Schema-Version: 2.2.0

You are proposing a prospective, higher-precision title/abstract screening judgment for
an existing candidate. Use only the supplied `evidence_units`, which reproduce the
original supplied title and abstract as deterministic field/sentence units. Do not
browse, retrieve metadata, use outside knowledge, infer from prior decisions, or assume
that a previous model outcome was correct. This is not a final paper-inclusion decision.

Assess E1–E5 and E7 as `YES`, `NO`, or `UNCERTAIN`. Do not assess E6: it is
`NOT_ASSESSED_AT_THIS_STAGE` and will be verified during full-report assessment.
Software independently recomputes the scientific outcome, operational disposition, and
H2H3/background routing.

## Preserved criteria

- `E1_life_science_application`: YES only for a supported life-science application,
  including biomedical, clinical, health, neuroscience, ecology, marine science,
  biology, bioinformatics, monitoring, operations, or health administration.
  Exclusively non-life-science work is NO. If the application is not established, use
  UNCERTAIN.
- `E2_relational_multiscale_relevance`: YES for explicit networks/relationships or
  relationships derived from spatial, temporal, multivariate, image-derived, lineage,
  similarity, or other multiscale life-science data. Use NO only when supplied evidence
  supports failure; otherwise use UNCERTAIN.
- `E3_interactive_visual_analytics`: YES when humans use visual interaction or an
  analytically meaningful visual representation within the analytical workflow. Static
  presentation or an explicitly noninteractive workflow is NO. Unstated analytical use
  is UNCERTAIN, not NO.
- `E4_computational_assistance`: YES for substantive computation integrated into that
  workflow. AI or machine learning is not mandatory. Qualifying computation may include
  clustering, statistical analysis, simulation, optimization, registration,
  recommendation, explanation, transformation, or similar methods. Infrastructure,
  data hosting, browser construction, ordinary rendering, streaming performance, pan,
  zoom, click, literal selection, parameter entry, lookup, or generic downstream support
  does not alone establish E4 in the required workflow.
- `E5_human_analytic_relationship`: YES when humans meaningfully inspect, direct,
  validate, interpret, supervise, correct, collaborate with, or decide from the assisted
  process in the same workflow. Fully offline computation without that relationship is
  NO. If the relationship is not established, use UNCERTAIN.
- `E7_evidence_sufficiency`: YES only when the supplied evidence identifies the specific
  candidate system or workflow and supports a defensible aggregate screening
  determination. E7 does not require every E1–E5 criterion to be resolved: a supported
  scientific NO may establish exclusion even when another scientific criterion is
  UNCERTAIN. Without a scientific NO, unresolved scientific evidence prevents
  advancement even if E7 is YES. Incomplete or missing evidence is UNCERTAIN. Do not use
  NO for E7.

## Prospective high-precision policy assessment

Also return `policy_assessment` with exactly these two fields:

1. `document_scope`
   - `QUALIFYING_RESEARCH_REPORT`: the supplied evidence describes or evaluates a
     specific system or workflow. The report need not introduce a new system.
   - `SURVEY_REVIEW_OVERVIEW`: the supplied evidence identifies a survey, review, or
     broad overview. These records are excluded from the current synthesis and retained
     separately for H2H3/background. Do not mine their references.
   - `UNCERTAIN`: the supplied evidence does not establish document scope.
2. `integrated_workflow`
   - `SUPPORTED`: supplied evidence explicitly connects a human analytical task, visual
     interaction or an analytically meaningful visual representation, and substantive
     computational assistance in the same workflow.
   - `CONTRADICTED`: supplied evidence affirmatively shows that the required connection
     is absent or incompatible with the described workflow.
   - `NOT_EVIDENCED_IN_SUPPLIED_METADATA`: the connection is missing or ambiguous. Use
     this for infrastructure, hosting, browser construction, rendering/streaming
     performance, generic downstream support, AI plus unrelated visualization, or a
     chatbot plus static explanatory imagery when explicit integration is not supplied.

Missing or ambiguous evidence produces DEFER, not an invented negative judgment. Clear
contradictory evidence may support EXCLUDED. Advance only clearly supported candidates.
There is no INCLUDE quota, target yield, or category balancing.

## Evidence-unit contract

For every criterion and each policy-assessment field return:

- `evidence_status`: `EVIDENCED` when selected supplied units support the judgment, or
  `NOT_EVIDENCED_IN_SUPPLIED_METADATA` when the relevant evidence is absent;
- `evidence_ids`: only exact IDs from `evidence_units`; use at least one ID with
  `EVIDENCED` and an empty list with `NOT_EVIDENCED_IN_SUPPLIED_METADATA`;
- `rationale`: explain the judgment or the missing evidence. Keep explanation separate
  from evidence and do not invent evidence IDs.

YES and NO require `EVIDENCED`. UNCERTAIN may use partial supplied evidence or explicit
absent-evidence status. `QUALIFYING_RESEARCH_REPORT`, `SURVEY_REVIEW_OVERVIEW`,
`SUPPORTED`, and `CONTRADICTED` require `EVIDENCED`. `NOT_EVIDENCED_IN_SUPPLIED_METADATA`
requires absent-evidence status and an empty ID list.

Use `SUPPORTED` certainty with YES or NO and `UNCERTAIN` certainty with UNCERTAIN.
Return one JSON object and no surrounding prose. Include exactly `criteria`,
`policy_assessment`, and `overall_rationale`. Do not include E6, aggregate outcome,
disposition, routing, exclusion reasons, quotations, previous judgments, or expected
answers.
