# Prompt 04 — Mediation Coding v0.1

Status: **PROVISIONAL**

## Core question

**How deeply does computation intervene in transforming evidence into analytic understanding?**

## Definition

Analytic mediation is the extent to which computational processes intervene between evidence and the analyst's understanding.

## Scale

### M0 — Execute
Performs an explicitly requested operation without materially shaping evidence or meaning.

### M1 — Structure
Transforms or organizes the evidence through which the analyst encounters the phenomenon.

Examples: dimensionality reduction, graph layout, aggregation, edge bundling, clustering used to structure a view.

### M2 — Prioritize
Influences which evidence, candidate, region, or analytic possibility should receive attention or review first.

Examples: ranked candidates, anomaly surfacing, recommended views, likely-animal video segments.

### M3 — Interpret
Contributes a semantic claim about what evidence means.

Examples: diagnosis, semantic cluster label, biological explanation, analytic interpretation.

### M4 — Frame
Influences what question, hypothesis, analytic objective, or problem formulation should be pursued.

## Evidence rules

- Do not infer M3/M4 from phrases such as "supports interpretation" or "supports hypothesis generation".
- Mediation and Delegation are independent.
- High Delegation does not imply deep Mediation.
- Use the deepest level directly evidenced.

## Output JSON

```json
{
  "paper_id": "",
  "mechanism_id": "",
  "mediation": {
    "code": "M0 | M1 | M2 | M3 | M4 | UNCERTAIN",
    "evidence": [],
    "reasoning_summary": "",
    "counterevidence": [],
    "confidence": "LOW | MEDIUM | HIGH"
  }
}
```

## Tie-breaks

- M0 vs M1: does computation substantively restructure evidence?
- M1 vs M2: does it make some evidence/options preferential for attention?
- M2 vs M3: does it make a claim about meaning?
- M3 vs M4: does it interpret evidence, or influence what question/hypothesis to pursue?
