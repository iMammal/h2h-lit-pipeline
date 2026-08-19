# Prompt 03 — Delegation Coding v0.1

Status: **PROVISIONAL**

## Core question

**Who determines what analytic action happens next?**

## Definition

Delegation is the degree to which authority for selecting and sequencing analytic actions is transferred from the human analyst to the computational system.

## Scale

### D0 — Commanded
Human chooses the specific analytic action; system executes it.

### D1 — Advisory
System proposes, predicts, ranks, or recommends; human decides whether to use it.

### D2 — Bounded Initiative
System may select or initiate local analytic actions within a human-defined objective or constraint set.

### D3 — Subgoal Delegation
Human specifies an analytic subgoal; system chooses and sequences multiple operations to achieve it.

### D4 — Goal Delegation
Human specifies a broad objective; system substantially plans, executes, evaluates, and revises an analytic workflow.

## Evidence rules

- Do not infer delegation from AI/LLM/agent/automatic/adaptive terminology.
- Complex models can be D0.
- Code the highest level **directly demonstrated**.
- If evidence is ambiguous, choose the lower supported level.

## Output JSON

```json
{
  "paper_id": "",
  "mechanism_id": "",
  "delegation": {
    "code": "D0 | D1 | D2 | D3 | D4 | UNCERTAIN",
    "evidence": [],
    "reasoning_summary": "",
    "counterevidence": [],
    "confidence": "LOW | MEDIUM | HIGH"
  }
}
```

## Tie-breaks

- D0 vs D1: did the system propose something the human could accept/reject?
- D1 vs D2: can the system initiate/choose a local action without approval for that exact action?
- D2 vs D3: did the human specify an outcome/subgoal while the system chose a sequence of operations?
- D3 vs D4: bounded subgoal versus broad objective with substantial planning/replanning.
