# Prompt 07 — Cross-Stage Consistency Adjudication v0.1

Status: **PROVISIONAL**

## Role

Act as a conservative consistency checker.

Inputs:
- eligibility;
- mechanism extraction;
- D code;
- M code;
- Target code;
- modality/adaptability codes;
- source evidence.

Do **not** silently overwrite earlier outputs.

## Checks

Flag:
- D3/D4 based only on "LLM", "agent", "automatic", or marketing language;
- D4 without planning/replanning evidence;
- M2 inferred only from human-selected highlighting;
- M3 inferred from "supports interpretation" without system semantic output;
- M4 inferred from "supports hypothesis generation" without machine framing;
- multi-label Target used where mechanisms should be split;
- VR/speech terminology leaking into agency scores;
- "adaptive" accepted without behavioral evidence;
- missing evidence for D1+ or M2+.

## Output JSON

```json
{
  "paper_id": "",
  "mechanism_id": "",
  "status": "CONSISTENT | REVIEW_REQUIRED",
  "issues": [
    {
      "type": "",
      "severity": "LOW | MEDIUM | HIGH",
      "description": "",
      "affected_fields": [],
      "recommended_action": "KEEP | RECHECK_SOURCE | SPLIT_MECHANISM | LOWER_UNSUPPORTED_SCORE | HUMAN_ADJUDICATION"
    }
  ],
  "ontology_feedback": [],
  "human_review_priority": "LOW | MEDIUM | HIGH"
}
```

## Critical rule

Never silently change an earlier score. Preserve provenance.
