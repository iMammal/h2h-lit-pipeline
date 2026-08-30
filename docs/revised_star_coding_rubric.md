# Revised STAR Coding Rubric

Status: **FROZEN for implementation**

Version: **1.0.0**

Frozen: **2026-08-30**

Authoritative protocol: [revised_star_protocol.md](revised_star_protocol.md)

This rubric operationalizes formal corpus screening and STAR classification for the CGF
revision of *From Hairballs to Hypotheses*. Apply it symmetrically to every deduplicated
record. Historical annotations and the separate provisional D/M/Target rubric are not
inputs to current decisions, although they must be preserved as provenance.

## 1. Coding Units and Status Values

The screening unit is a paper. When a paper reports multiple systems, eligibility is
established if at least one identified system jointly satisfies all required criteria.
Record the qualifying system or systems. Assistance mechanisms may be extracted within
systems to support classification, but mechanisms are not independent corpus members.

Use these criterion and annotation states:

- `YES`, `NO`, `UNCERTAIN` for eligibility criteria;
- `PRESENT`, `ABSENT`, `UNCERTAIN` for each assistance mode, modality, and task;
- `ELIGIBLE`, `EXCLUDED`, `UNCERTAIN` for the aggregate screening decision; and
- `CORE`, `SUPPORTING`, `CONTEXTUAL` for post-eligibility synthesis priority.

Do not force a positive or negative label when the reviewed evidence is insufficient.

## 2. Formal Eligibility Rubric

### E1 - Life-science application

**YES:** The reported system is applied to analysis, understanding, monitoring,
decision-making, discovery, or scientific practice in life sciences, biomedical or
clinical science, health, neuroscience, ecology, marine science, biology,
bioinformatics, or a closely related domain.

Clinical work is in scope. Surgical work is assessed under the same rubric: a surgical
system may qualify when it supports an in-scope interactive analytic workflow, and it
may fail when it is only training, teleoperation, procedural guidance, or presentation
without the required analytic relationship. There is no blanket surgical exclusion.

**NO:** The reported application is exclusively outside life sciences. Transferable
non-life-science work may be retained as seed or background evidence but is not a formal
eligible-corpus member.

**UNCERTAIN:** The application domain or intended analytic use is not clear from the
available evidence.

### E2 - Relational, derived-structure, or multiscale relevance

**YES:** The system analyzes explicit networks or relationships, or relationships
derived from spatial, temporal, multivariate, image-derived, lineage, similarity, or
other multiscale life-science data. The representation need not be a node-link diagram.

**NO:** The work does not address explicit or derived relationships, multiscale
structure, or the corresponding analytic problem.

**UNCERTAIN:** The data are potentially relational or multiscale, but the reported
structure and analytic role are unclear.

### E3 - Interactive visual-analytics component

**YES:** Humans use an interactive or analytically meaningful visual representation to
inspect, understand, validate, steer, compare, interpret, or act on data or computational
results.

**NO:** Visual material is solely static illustration, presentation, communication, or
artistic output, or the reported workflow contains no qualifying visual analytic
interaction.

**UNCERTAIN:** A visualization is mentioned but its role or interactivity is not
described adequately.

### E4 - Substantive computational assistance

**YES:** At least one computational mechanism contributes nontrivially inside the
interactive visual-analytics workflow by transforming or organizing the analytic
substrate, adapting interaction, recommending or initiating an action, mediating an
explanation, or exploiting immersive-environment properties as assistance.

Qualifying examples include clustering, dimensionality reduction, graph computation,
layout, aggregation, automatic annotation, ranking, recommendation, prediction,
classification, adaptive behavior, natural-language analytic operations, explanation,
workflow orchestration, and computationally guided immersive interaction.

**NO:** The system provides only ordinary rendering, pan, zoom, click, literal manual
selection, direct parameter entry, static display, ordinary database lookup, or another
interaction without substantive computational assistance.

**UNCERTAIN:** Automation or intelligence is claimed, but the actual behavior and its
role inside the analytic workflow are not evidenced.

Technology terms such as "AI," "intelligent," "LLM-powered," "agentic," "adaptive,"
or "automatic" do not establish E4 by themselves.

### E5 - Human analytic relationship

**YES:** Humans meaningfully inspect, direct, validate, interpret, collaborate with,
supervise, correct, or make decisions from the assisted computational process.

**NO:** The reported computation is fully offline or automated with no meaningful human
analytic relationship to its results.

**UNCERTAIN:** The human role is not described adequately.

### E6 - Administrative scope

All parts must be satisfied:

- **Date:** no lower cutoff; the publication is available no later than the documented
  retrieval end date of the regenerated search run.
- **Language:** English-language full text is available for formal-corpus review.
- **Document type:** journal, conference, workshop, or preprint research paper.

A survey or conceptual work receives `YES` for document type only when it independently
reports or analyzes a system satisfying E1-E5. Otherwise retain it as seed/background
evidence outside the formal corpus.

Use `UNCERTAIN` while date, language, document type, or full-text availability is still
being resolved.

### E7 - Evidence sufficiency

**YES:** The reviewed evidence identifies at least one candidate system and supports a
defensible determination for E1-E6.

**NO:** After documented full-text escalation and human review where required, the
available evidence remains insufficient to establish eligibility.

**UNCERTAIN:** The title, abstract, metadata, or currently available text plausibly
indicates relevance but cannot support a defensible decision.

### Aggregate decision

- `ELIGIBLE`: E1-E7 are all `YES`.
- `EXCLUDED`: at least one criterion is conclusively `NO`.
- `UNCERTAIN`: no criterion is `NO` and at least one criterion is `UNCERTAIN`.

Assign the aggregate decision only after recording criterion evidence. Eligibility does
not depend on assistance-mode, modality, task, D/M/Target, historical relevance, or
synthesis-priority values.

## 3. Full-Text Escalation

Escalate a title/abstract decision to full text when:

- any required eligibility criterion is `UNCERTAIN`;
- a proposed exclusion rests on a missing or incomplete abstract;
- assistance cannot be distinguished from ordinary rendering or interaction;
- an immersive display cannot be distinguished from Immersive assistance;
- life-science application, relational/multiscale relevance, or human analytic role is
  ambiguous;
- language or document type cannot be confirmed; or
- available sources conflict.

Record each acquisition attempt and its result. Retrieval or parsing failure leaves the
scientific decision uncertain. After reasonable documented attempts, send unresolved
records to human review. Use insufficient evidence as an exclusion only after this path
is complete.

## 4. Exclusion Reasons

Record one primary exclusion reason and any supported secondary reasons. For records
with multiple `NO` criteria, use the first decisive reason in the order below as primary;
retain the rest as secondary.

| Code | Definition |
|---|---|
| `EX_AFTER_RETRIEVAL_END_DATE` | Publication falls after the documented retrieval end date. |
| `EX_NON_ENGLISH_FULL_TEXT` | English-language full text is not available. |
| `EX_INELIGIBLE_DOCUMENT_TYPE` | Document is not an eligible research-paper type and does not independently satisfy the system criteria. |
| `EX_NO_LIFE_SCIENCE_APPLICATION` | E1 is `NO`; transferable work may remain background evidence. |
| `EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE` | E2 is `NO`. |
| `EX_NO_INTERACTIVE_VISUAL_ANALYTICS` | E3 is `NO`, including presentation-only or static-only work. |
| `EX_NO_QUALIFYING_ASSISTANCE` | E4 is `NO`. |
| `EX_NO_HUMAN_ANALYTIC_RELATIONSHIP` | E5 is `NO`. |
| `EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION` | E7 is `NO` after documented escalation. |
| `EX_OTHER_PROTOCOL_REASON` | A versioned free-text explanation is required. |

The following are not exclusion reasons:

- duplicate or merged record;
- missing abstract;
- unavailable PDF before escalation is complete;
- retrieval, parser, schema, or model failure;
- invalid or unrecognized LLM output;
- low historical relevance score; or
- historical `Off-Topic` or corpus membership.

## 5. Assistance-Mode Coding

Code all four modes independently for each qualifying system. A paper-level mode set is
the union of `PRESENT` modes across its qualifying systems. Do not choose a primary mode,
do not force a tie-break, and do not default unclear systems to Algorithmic.

For each `PRESENT` or `UNCERTAIN` value, record the system/mechanism, evidence location,
relevant input or signal, computational behavior, output or action, human role/control,
and analytic consequence.

### Algorithmic

Automated computational analysis is integrated into the visualization pipeline and
operates on data, structure, representation, or analytic candidates.

**Positive indicators:** graph mining, clustering, dimensionality reduction, layout,
topology reduction, embeddings, aggregation, prediction, classification, automatic
annotation, ranking, or recommendation that shapes what the analyst can inspect.

**Counterexamples:** ordinary rendering, direct manipulation alone, or computation whose
results are not connected to the interactive analytic workflow.

Algorithmic may overlap with Adaptive, Conversational, and Immersive.

### Adaptive

The system dynamically tailors visualization, interaction, guidance, or analytic action
using user behavior, task context, data characteristics, interaction history, or inferred
intent.

**Positive indicators:** user modeling, personalization, context-responsive views,
intent-aware selection, proactive guidance, or co-adaptive behavior.

**Counterexamples:** biological adaptation, a one-time algorithm choice, or literal
execution of a direct command without system-selected adaptation.

Adaptive may overlap with Algorithmic, Conversational, and Immersive.

### Conversational

Natural language or dialogue is interpreted as part of the analytic interaction so that
users can steer analysis through queries, explanations, recommendations, or multi-step
plans.

**Positive indicators:** typed or spoken analytic queries, multi-turn dialogue,
language-grounded visualization operations, conversational explanations, or agentic
language orchestration of analysis.

**Counterexamples:** offline NLP preprocessing, a generated label shown without dialogue,
or speech used only as a button-equivalent command channel.

Conversational may overlap with Algorithmic, Adaptive, and Immersive.

### Immersive

Computational assistance exploits properties of an immersive environment, such as
spatialization, embodiment, navigation, tracked spatial context, or multisensory cues, as
an active analytic mechanism.

**Positive indicators:** computationally guided spatial navigation, intent-aware
viewpoint or scale control, gaze/gesture/spatial-signal fusion, spatially grounded
conversational assistance, analytic haptic guidance, or attention guidance whose
mechanism depends on immersive-environment properties.

**Counterexamples:** presenting the same visualization in an HMD, AR display, or CAVE;
stereoscopy alone; direct tracked-controller manipulation; or spatial layout without a
computational assistance mechanism.

Immersive normally occurs in VR, AR/MR, or CAVE and may overlap with every other mode.
Immersive display is necessary context for this mode but is not sufficient evidence.

## 6. Visualization-Modality Coding

Code the environment in which the analytic visualization is presented and used, not the
physical form factor of a device. Code every modality supported by evidence; do not
select one by precedence.

### Desktop 2D - operationally Planar 2D

`Desktop 2D` is the manuscript-facing historical label. Its frozen operational meaning
is **Planar 2D**: an analytic visualization presented through a conventional planar
screen-based environment, including charts, dashboards, spreadsheets, network views,
2D visualizations, and rotatable 3D or pseudo-3D views shown on a planar screen.

Examples:

- a desktop monitor showing a network dashboard: `Desktop 2D`;
- a phone showing a conventional chart: `Desktop 2D`;
- a tablet showing a spreadsheet or planar network view: `Desktop 2D`; and
- a standard monitor showing a rotatable 3D scatterplot: `Desktop 2D`.

Phone, tablet, and desktop computer are device form factors, not separate modalities.

### Large Display

An analytically used wall-sized, tiled, powerwall, or comparably large shared display
where scale, shared visibility, physical navigation, reach, or collaboration contributes
to the analytic environment. Screen size alone is insufficient when the device is used
only as a conventional single-user planar monitor.

### VR

An immersive synthetic environment, normally presented through a head-mounted display,
in which the analyst is perceptually situated inside the virtual analytic environment.

### AR/MR

Analytic information is spatially registered with or overlaid onto the physical
environment. A phone or tablet used in a Pokemon GO-like spatial augmentation is
`AR/MR`, not `Desktop 2D`.

### CAVE

A projection-based, room-scale, multi-surface immersive environment such as a CAVE or
CAVE-like installation.

### Controller and multi-environment rules

- A phone or tablet used primarily as input for a wall visualization is not a new
  modality; code `Large Display` and record the phone/tablet as a controller form factor.
- A tablet used primarily as input for a CAVE is not `Desktop 2D`; code `CAVE`.
- If a system presents coordinated analytic visualizations in both a planar interface
  and VR, code both `Desktop 2D` and `VR` when both presentations are evidenced.
- If a system merely configures or launches another environment from a planar device,
  do not add `Desktop 2D` unless analytic visualization is also presented there.
- Do not introduce `Mobile`, `Tablet`, `Desktop 3D`, `Physical/Haptic`, `Other`, or
  `Unspecified` as STAR visualization modalities. Missing evidence is represented by the
  annotation state `UNCERTAIN`, not by a sixth category.

### Optional device-form-factor attribute

Device form factor is descriptive and cannot change a STAR modality. When useful, record
one or more of:

- `PHONE`
- `TABLET`
- `DESKTOP_MONITOR`
- `LAPTOP_SCREEN`
- `WALL_OR_TILED_DISPLAY`
- `HMD`
- `PROJECTION_ROOM`
- `PHYSICAL_ARTIFACT`
- `OTHER`
- `UNSPECIFIED`

Also record `CONTROLLER_ONLY` when a device supplies input but does not present the
analytic visualization.

## 7. Task Coding

Tasks are a multilabel synthesis lens. Code each task independently. A task is `PRESENT`
only when the system supports the corresponding analyst intent or action; a general
claim that a system "supports analysis" is insufficient. Task labels do not affect
eligibility.

### Navigation and Multiscale Orientation

Traversing hierarchical, spatial, temporal, or semantic structures while maintaining
context across levels of abstraction, such as overview-to-detail or cohort-to-individual
transitions.

### Comparison and Differentiation

Contrasting conditions, timepoints, cohorts, entities, models, or aligned
representations, and identifying similarities, differences, outliers, or structural
shifts while preserving meaningful correspondence.

### Selection, Filtering, and Precision Interaction

Specifying subsets and constraints, including selecting clusters, filtering cohorts,
thresholding relationships, isolating regions, or stabilizing precise analytic
interaction, to reduce complexity and express intent.

### Sensemaking and Hypothesis Development

Constructing and revising mental models, contextualizing observations, externalizing
interpretations, forming or refining questions, and evaluating candidate explanations or
testable hypotheses.

### Coordination and Collaborative Reasoning

Establishing common ground, dividing analytic labor, maintaining shared reference,
integrating partial findings, supporting handoffs, and negotiating a verifiable shared
interpretation.

Process awareness and provenance are evaluation or workflow properties unless the
evidence shows that they directly support one or more of these five task intents.

## 8. Post-Eligibility Synthesis Priority

Assign exactly one priority after a paper is `ELIGIBLE`. Priority cannot change corpus
membership.

### CORE

The qualifying system and assistance relationship are central contributions of the
paper, directly address the STAR's life-science relational/multiscale problem, and
provide substantive evidence for assistance, modality, task, agency, or evaluation
synthesis.

### SUPPORTING

The paper is eligible and materially informs one or more synthesis questions, but the
qualifying system is narrower, secondary to another contribution, or provides limited
evidence for comparative synthesis.

### CONTEXTUAL

The paper is eligible but primarily contributes in-scope historical context, framing,
evaluation context, or an adjacent system perspective rather than direct central
evidence for the main synthesis claims.

An excluded non-life-science paper may be retained as `BACKGROUND_EVIDENCE` or
`SEED_EVIDENCE`; those are evidence roles, not synthesis priorities.

Historical 1-5 relevance scores must be preserved with their historical prompt and run
provenance. They cannot initialize or determine current priority.

## 9. Decision Authority and Provenance

### Machine proposals

An LLM annotation is a proposed decision. Preserve at minimum:

- record and system identifiers;
- rubric and taxonomy versions;
- actor type `LLM` and inference-run identifier;
- exact reviewed input fields or content hash;
- criterion or dimension being coded;
- proposed value, evidence location, and concise rationale;
- uncertainty or confidence when collected; and
- raw/parsed response, validation status, retries, and technical failures where retained.

A failed attempt cannot produce a scientific decision.

### Human decisions and adjudication

Human reviewers use the same criterion and annotation structures and cite reviewed
evidence. A human decision supersedes the corresponding machine proposal. Independent
human disagreement is resolved by a recorded adjudication that links every prior
decision and states the resolution rationale. Superseded decisions remain immutable.

Historical decisions are never silently rewritten as current decisions. Derived corpus
artifacts must identify the effective current decision and retain links to all historical
and superseded annotations.

## 10. Supplemental D/M/Target Boundary

D/M/Target may be coded for exploratory mechanism-level analysis under a separately
versioned supplemental rubric. It cannot:

- establish or reverse eligibility;
- add or remove a formal corpus member;
- establish an assistance mode, visualization modality, or task;
- determine `CORE`, `SUPPORTING`, or `CONTEXTUAL`; or
- be presented as the STAR's primary classification framework.

Locus of analytic agency is synthesized interpretively from reviewed evidence and the
frozen STAR dimensions; it is not computed as a D/M/Target eligibility taxonomy.

## 11. Human-Validation Design Deferred

This rubric freezes what will be coded, not the final validation sample. Conduct a
bounded pilot first. Then register the sampling and coder design prospectively before
examining validation results, using regenerated corpus size, class balance, observed
uncertainty/error prevalence, rare-class coverage, and coder/adjudicator workload.

The final design must state whether any strata receive a census, how the remaining
sample is selected and weighted, coder independence, whether coders can see machine
proposals, adjudication rules, missing-review handling, and planned metrics.
