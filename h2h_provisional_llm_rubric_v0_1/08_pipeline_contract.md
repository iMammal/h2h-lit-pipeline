# Pipeline Contract v0.1

Status: **PROVISIONAL**

## Recommended execution order

```text
RAW RETRIEVAL RECORD
        |
        v
01 Eligibility
        |
        +---- EXCLUDE --> preserve reason/provenance
        |
        +---- UNCERTAIN --> full text / human-priority queue
        |
        v
Likely eligible paper
        |
        v
02 Mechanism Extraction
        |
        +------ mechanism_1
        |         |
        |         +--> 03 Delegation
        |         +--> 04 Mediation
        |         +--> 05 Analytic Target
        |         +--> 06 Modality + Adaptability
        |
        +------ mechanism_2 ...
                  |
                  v
        07 Consistency Adjudication
                  |
                  v
          Human validation/adjudication
                  |
                  v
          Versioned canonical record
```

## Model allocation suggestion

- **Stage 01:** cheaper/faster model, conservative high-recall prompt.
- **Stage 02:** stronger long-context model for full text.
- **Stages 03-06:** same strong model may be used in separate calls. During rubric development, keep calls separate to reduce label anchoring.
- **Stage 07:** strong reasoning model as an audit layer, not a substitute for human validation.

## Later optimization

After the rubric is frozen and empirically validated, D/M/T/Modality may be combined into fewer calls **only if testing shows no meaningful loss of reliability**.

## Required provenance

Record:
- protocol version;
- rubric version;
- prompt name/version;
- model/provider/version;
- model settings;
- timestamp;
- source document/hash;
- evidence spans;
- uncertainty;
- human adjudication.
