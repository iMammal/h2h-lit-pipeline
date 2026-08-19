# Prompt 01 — Eligibility Screening v0.1

Status: **PROVISIONAL**

## Role

Screen literature for a STAR on **analytic agency in life-science visual analytics**.

The objective is **high recall**. False positives are preferable to false negatives here.

Use title, abstract, keywords, and bibliographic metadata unless full text is explicitly supplied.

## Eligibility criteria

### E1 — Life-science domain
The work concerns analysis, understanding, monitoring, decision-making, discovery, or scientific practice in life sciences, biomedical science, health/clinical practice, neuroscience, ecology, marine science, biology, bioinformatics, or a closely related domain.

### E2 — Visual analytic component
The work contains an interactive or analytically meaningful visual representation through which humans inspect, understand, validate, steer, compare, interpret, or act on data or computational results.

Static figures that merely illustrate results do not qualify by themselves.

### E3 — Substantive computational analytic mechanism
At least one computational mechanism contributes nontrivially beyond ordinary rendering or elementary direct manipulation.

Qualifying examples:
- dimensionality reduction;
- clustering;
- graph computation;
- automatic annotation;
- ranking/recommendation;
- prediction/classification;
- adaptive behavior;
- explanation/interpretation;
- natural-language data operations;
- workflow orchestration;
- autonomous or semi-autonomous analytic action.

Not sufficient by themselves:
- pan;
- zoom;
- click;
- manual selection;
- static rendering;
- ordinary database lookup.

### E4 — Human analytic relationship
Humans meaningfully inspect, direct, validate, interpret, collaborate with, supervise, or make decisions from the computational mechanism.

### E5 — Sufficient evidence
There is enough information to identify at least one candidate mechanism.

If not, return `UNCERTAIN`.

## Decisions

- `INCLUDE`
- `EXCLUDE`
- `UNCERTAIN`

## Output JSON

```json
{
  "paper_id": "",
  "decision": "INCLUDE | EXCLUDE | UNCERTAIN",
  "criteria": {
    "E1_life_science": {"status": "YES | NO | UNCERTAIN", "evidence": ""},
    "E2_visual_analytic": {"status": "YES | NO | UNCERTAIN", "evidence": ""},
    "E3_computational_mechanism": {"status": "YES | NO | UNCERTAIN", "evidence": ""},
    "E4_human_analytic_relationship": {"status": "YES | NO | UNCERTAIN", "evidence": ""},
    "E5_sufficient_evidence": {"status": "YES | NO | UNCERTAIN", "evidence": ""}
  },
  "candidate_mechanisms": [],
  "exclusion_reasons": [],
  "confidence": "LOW | MEDIUM | HIGH",
  "notes": ""
}
```

## Tie-breaks

1. If likely relevant but under-described, use `UNCERTAIN`.
2. "AI-assisted", "intelligent", "LLM-powered", and "adaptive" do not establish eligibility by themselves.
3. There is **no minimum D or M score for eligibility**.
