# Prompt 05 — Analytic Target Coding v0.1

Status: **PROVISIONAL**

## Core question

**What part of the analytic process does this computational mechanism act upon?**

Analytic Target is categorical and may be multi-label. It is not ordinal.

## Targets

### T-DATA — Data
Observations, records, features, samples, annotations, corpora, or data access.

### T-MODEL — Analysis / Model
Analytical/computational models or transformations.

### T-VIEW — Visualization / View
External visual representation: chart, layout, encoding, spatial organization.

### T-KNOWLEDGE — Knowledge / Decision
Interpretation, explanation, scientific/clinical judgment, conclusion, hypothesis, or decision support.

For v0.1, Knowledge and Decision remain combined. Flag cases where this is inadequate.

### T-WORKFLOW — Workflow / Process
Sequencing, orchestration, tool selection, provenance, or trajectory of analysis.

## Multi-label rule

Use multiple targets only when the same indivisible mechanism directly acts on more than one target. Otherwise prefer mechanism decomposition.

## Output JSON

```json
{
  "paper_id": "",
  "mechanism_id": "",
  "analytic_target": {
    "primary_target": "T-DATA | T-MODEL | T-VIEW | T-KNOWLEDGE | T-WORKFLOW | UNCERTAIN",
    "secondary_targets": [],
    "evidence": [],
    "reasoning_summary": "",
    "knowledge_decision_split_flag": false,
    "confidence": "LOW | MEDIUM | HIGH"
  }
}
```

## Tie-breaks

- Data vs Model: input substrate versus analytical transformation/model.
- Model vs View: computation/model versus external representation.
- View vs Knowledge: what is shown versus what it means.
- Knowledge vs Workflow: interpretation/decision versus organizing what analysis happens next.
