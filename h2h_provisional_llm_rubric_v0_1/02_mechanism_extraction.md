# Prompt 02 — Mechanism Extraction v0.1

Status: **PROVISIONAL**

## Role

Given an eligible or potentially eligible paper, identify distinct **human-computational analytic mechanisms**.

The coding unit is:

**paper → system/tool → mechanism**

Do not assign D, M, Target, or Modality scores in this step.

## Mechanism definition

A mechanism is a distinct computational behavior that changes the analytic process and can be separately evidenced.

Examples:
- LLM maps a natural-language cohort request to an executable query;
- model ranks candidate images for annotation;
- algorithm performs dimensionality reduction for visual exploration;
- recommender suggests the next visualization;
- CNN generates a diagnostic prediction;
- adaptive system changes a view based on user behavior;
- robotic tracker chooses control actions to maintain observation.

Split mechanisms when:
- human-machine allocation differs;
- epistemic effect differs;
- analytic target differs materially;
- one behavior could exist without the other.

## Evidence requirements

For every mechanism capture:
- evidence quote or precise paraphrase;
- page/section;
- human action;
- system action;
- input;
- output;
- analytic effect;
- whether demonstrated or merely claimed.

## Output JSON

```json
{
  "paper_id": "",
  "system_name": "",
  "mechanisms": [
    {
      "mechanism_id": "",
      "name": "",
      "description": "",
      "human_role": "",
      "system_role": "",
      "inputs": [],
      "outputs": [],
      "analytic_effect": "",
      "evidence": [],
      "dependencies_on_other_mechanisms": [],
      "uncertainties": []
    }
  ],
  "confidence": "LOW | MEDIUM | HIGH"
}
```

## Tie-breaks

1. Prefer fewer well-evidenced mechanisms over many speculative ones.
2. Do not combine retrieval and interpretation merely because the same LLM performs both.
3. Do not treat speech input itself as an analytic mechanism unless it transforms analysis.
