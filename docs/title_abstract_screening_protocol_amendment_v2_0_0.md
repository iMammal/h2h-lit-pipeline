# Title/Abstract Screening Protocol Amendment 2.0.0

Status: panel-approved procedure for the September 20, 2026 benchmark pilot. This amendment does not establish final paper eligibility and does not change the registered identification corpus or frozen sampling frame.

## Approval and scope

The panel approved the E6-deferred proposal on September 21, 2026. No panel identities or individual votes were recorded. This amendment governs only title/abstract screening returns under protocol `title-abstract-screening-e6-deferred` version `2.0.0` and return schema `title-abstract-screening-return` version `2.0.0`.

The frozen rubric remains preserved. This document is an explicit, versioned amendment rather than a silent edit to that rubric.

## Changes from the frozen title/abstract procedure

- E1-E5 remain the scientific screening criteria.
- E6 is deferred to administrative verification and must be recorded as `NOT_ASSESSED_AT_THIS_STAGE`. It is never inferred to be YES and does not enter the title/abstract aggregate.
- E7 asks whether the available title/abstract evidence is sufficient to support the E1-E5 judgments at this stage. E7=NO or UNCERTAIN expresses unresolved evidence; it is not itself a scientific exclusion.
- EXCLUDED requires at least one defensible NO among E1-E5, even if another response is UNCERTAIN.
- UNCERTAIN applies when E1-E5 contain no NO and at least one assessed response is unresolved, including E7=NO or UNCERTAIN.
- INCLUDE requires YES for E1-E5 and E7. It means retain for further assessment, not final inclusion.

Explicit title evidence may support an individual criterion when no abstract is present. A promising title must not be treated as evidence that every criterion is satisfied. Missing abstracts are routed to metadata recovery before a human full-text task is considered.

## Separate routing field

The aggregate outcome and next action are distinct. The deterministic precedence is:

1. Evidence conflict -> `EVIDENCE_RECONCILIATION`.
2. Incomplete or invalid responses -> `TARGETED_SECOND_REVIEW`.
3. EXCLUDED -> `NONE`, unless evidence reconciliation already applies.
4. Non-excluded and abstract missing -> `METADATA_RECOVERY`.
5. Prospective targeted-review request -> `TARGETED_SECOND_REVIEW`.
6. Otherwise -> `FULL_TEXT_ASSESSMENT`.

This ordering preserves scientific-NO exclusion precedence while preventing a missing abstract alone from automatically creating a human full-text task.

## Evidence and independence

The reviewed Rumi workbooks are protocol-development evidence with access to prior judgments. They are not independent reviews, are excluded from model-comparison metrics, and are not imported by this amendment. Panel approval of the procedure does not approve every record-level judgment, including the two title-only INCLUDE calls.

The row-15 title/abstract mismatch for `canonical:7d25df89fed662b381109a2e` requires evidence reconciliation and a new review after reconciliation. Original source responses and benchmark evidence remain unchanged; any later recovery must be versioned and clearly distinguished from the original title/abstract-only evidence.

## Reproducibility bindings

- Retrieval cutoff: `2026-09-20`.
- Registered corpus SHA-256: `44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2`.
- Identity-overlay SHA-256: `c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d`.
- Frozen sampling-manifest SHA-256: `a2fd6e54c6a955eadbd4ea63d9996b9bbb8ec29c65024a733eefdd5f13747249`.
- Reviewed draft workbook SHA-256: `27de0cdef2ad1933ff798a61eaa29c508dfc2946c14060f4d3b39feeba83453d`.

The reviewed draft was created before checkpoint portability edits to its builder. Its draft manifest does not record a builder hash. The current builder hash is `fb5edb8d96233428f158bde240766ee3c4e4e6f3a514c436d47d7b06add5f10a`; the generation-time implementation hash recorded during checkpoint review was `843c0a0984c8d41923dc74c0d36e25844b9fb813cd23feb24791988fa45186ce`. The draft artifacts are therefore preserved with their original bindings and are not regenerated or rebound.

Identification remains open. This amendment does not close identification, change PRISMA counts, redraw the sample, or modify bibliographic identities.
