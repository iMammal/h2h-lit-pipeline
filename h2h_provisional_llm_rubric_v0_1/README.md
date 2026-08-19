# H2H Provisional Multi-Stage LLM Classification Rubric v0.1

Status: **PROVISIONAL — advisor review required before freezing or using for the prospective corpus**

This prompt set supports the proposed mechanism-level **Analytic Agency** coding framework for the revised H2H STAR.

## Recommended architecture

Use **seven prompt stages**. They do not require seven different model products; they are seven logically separate LLM tasks that may be run with different model sizes/effort levels.

1. `01_eligibility_screen.md` — high-recall title/abstract screening.
2. `02_mechanism_extraction.md` — full-text extraction of distinct human-computational analytic mechanisms.
3. `03_delegation_coding.md` — independent D0-D4 coding.
4. `04_mediation_coding.md` — independent M0-M4 coding.
5. `05_analytic_target_coding.md` — independent multi-label target coding.
6. `06_modality_and_adaptability_coding.md` — visualization modality, interaction modality, and provisional adaptability.
7. `07_consistency_adjudication.md` — cross-check contradictions and unsupported inferences.

A subsequent **human validation/adjudication stage** is required.

## Why separate the stages?

Separate calls reduce label leakage. For example:

- an LLM should not infer D3 just because a mechanism uses an LLM;
- VR should not imply high Mediation;
- "adaptive" should not automatically imply high Adaptability;
- "supports hypothesis generation" should not imply M4 unless the system itself frames the hypothesis.

## Core concepts

- **Delegation:** Who determines what analytic action happens next?
- **Mediation:** How deeply does computation intervene in transforming evidence into analytic understanding?
- **Analytic Target:** What part of the analytic process does the mechanism act upon?
- **Visualization Modality:** Through what perceptual/display environment is information presented?
- **Interaction Modality:** Through what channel does the analyst communicate intent?
- **Adaptability:** Provisional descriptor for whether behavior changes with user, context, history, or interaction.

## General evidence discipline

- Classify **mechanisms**, not papers.
- Do not classify marketing terms.
- Prefer explicit behavioral evidence.
- Do not infer autonomy from "AI", "LLM", "agent", "intelligent", "adaptive", or "automatic".
- Do not infer M4 from "supports hypothesis generation".
- Do not infer machine prioritization from "supports anomaly detection" unless the machine identifies/prioritizes anomalies.
- Use the **lowest code directly supported by evidence** when ambiguous.
- Every D1+ and M2+ code must cite direct evidence.
- Preserve uncertainty rather than forcing a score.
