# Full-report selection authority

Status: **author-confirmed 2026-10-09**

This record documents authority for the six full-report assessment sets that contain
20, 18, 50, 200, 24, and 72 publication/report identities. It distinguishes the person
who requested and bounded the work from the actor that nominated particular candidates,
software that ranked or reconciled records, and later review of evidence and reported
placements.

It does not change the frozen queues, assessment ledgers, or cumulative crosswalk, and it
does not assert that every completed queue received a separate pre-assessment author
review.

## Retrospective author confirmation

On 9 October 2026, Morris Chukhman retrospectively confirmed that:

- he issued the selection instructions preserved in the recovered session records;
- he authorized the stated priorities, queue bounds, and three target cells;
- Codex selected candidates within those instructions, including purposive choices where
  the instructions did not determine an exact row set;
- his review concerned the resulting evidence and reported placements; and
- this confirmation does not represent a separate pre-assessment review of every frozen
  queue.

This retrospective confirmation supplements rather than replaces the contemporaneous
session and artifact evidence. The intellectual origins of the twelve-anchor shortlist
and the provisional 25-row sparse-cell shortlist remain unresolved.

## Contemporaneous session evidence

The recovered local Codex session exports are outside the repository and are not release
artifacts. Timestamps below are UTC.

| Event | Timestamp | Message or file event | Local session evidence |
|---|---:|---|---|
| Twelve named anchors and bounded replacement authority | 2026-09-24 23:25:08 | `msg_01a0d5bc-b398-7472-80b0-b58552d0c66e` | `~/.codex/sessions/2026/09/20/rollout-2026-09-20T19-01-03-01a0c144-254c-7901-9df5-10e28823b777.jsonl:9369` |
| Codex records PeCaX as the replacement | 2026-09-24 23:56:08 | file-change event | same session, line 10305 |
| Initial Assistance x Modality map request | 2026-09-25 21:39:52 | `msg_01a0da82-ae7a-7443-aa28-8c094bc77844` | `~/.codex/sessions/2026/09/25/rollout-2026-09-25T16-39-49-01a0da82-a2cf-7221-80ba-c3fed5439351.jsonl:9` |
| Codex creates the initial placement data | 2026-09-25 21:56:53 | tool/file event | same session, line 499 |
| Up-to-40 expansion request | 2026-09-26 00:06:16 | `msg_01a0db08-b514-7330-803f-b97a9f046473` | same session, line 872 |
| Codex writes the exact Q01--Q18 queue | 2026-09-26 00:15:17 | tool/file event | same session, line 1155 |
| Up-to-60 expansion request | 2026-09-26 02:01:12 | `msg_01a0db71-f00e-7a51-9c11-ba8b0cdb0c88` | same session, line 2208 |
| Codex writes the hard-coded E01--E50 selection | 2026-09-26 02:07:51 | tool/file event | same session, line 2472 |
| Maximum-200 queue and 140-primary-order request | 2026-09-26 03:32:31 | `msg_01a0dbc5-89dd-7560-9c05-a91efe07e1b0` | same session, line 3380 |
| Provisional-shortlist reconciliation request | 2026-09-26 05:22:23 | `msg_01a0dc2a-2224-7f62-b0c1-284d8cd22f82` | same session, line 4581 |
| User supplies the two sparse-cell input files | 2026-09-26 05:53:08 | `msg_01a0dc46-468d-7311-a744-d3dcba9fc81d` | same session, line 4665 |
| Three-cell complete-pass authorization | 2026-09-26 15:41:14 | `msg_01a0de60-b4f7-75a1-8993-0608f0db1158` | same session, line 5318 |

The selection packages are beneath the Git-ignored `outputs/` tree. Git history therefore
does not identify their selectors; the recovered session events, frozen artifacts, and
executable scripts provide the relevant contemporaneous evidence.

## Authority by assessment set

| Set | Morris Chukhman's documented authority | Candidate nomination or selection | Software role | Approval boundary |
|---:|---|---|---|---|
| 20 | Requested the map, supplied the twelve titles operationally, authorized use of assessed coauthor evidence, and authorized at most one replacement per unsupported anchor slot | Codex chose PeCaX as the replacement and chose the seven coauthor-source additions that completed the set | Identity resolution and artifact materialization; no deterministic 20-entry selector | No separate review of the resulting 20 before assessment is asserted |
| 18 | Authorized priorities and a maximum of 40 additional systems | Codex purposively chose and ordered Q01--Q18 | Queue serialization only | No separate review of Q01--Q18 before assessment is asserted |
| 50 | Authorized a maximum of 60 and approximate sparse-cell, comparison/evaluation, and contrast priorities without mandatory quotas | Codex chose and ordered the hard-coded E01--E50 list after consulting the computed shortlist | Deterministic preliminary ranking, then validation and materialization of the Codex-authored list | No separate review of E01--E50 before assessment is asserted |
| 200 | Authorized at most 200 new candidate identities, with positions 1--140 as the primary order and 141--200 as ordered reserves | Deterministic software selected the first 200 under the implemented ordering | Exclusions, scoring, ordering, cutoff, and queue-role assignment | Rule authorization is documented; no row-by-row pre-assessment review is asserted |
| 24 | Authorized use of the supplied provisional shortlist, a maximum of 30, and the sparse-cell priorities | The upstream nominator of the 25 rows is unknown; software removed one related report, leaving 24 in preserved priority order | Terminal-export rebinding and canonical-ID, DOI, title, alias, and related-version reconciliation | Use of the shortlist and reconciliation was authorized; its authorship and any prior row-by-row review remain unknown |
| 72 | Selected and authorized complete assessment of the three target cells | Software enumerated every exact-`PRESENT` candidate in the cells; 72 newly represented systems remained after prior-system and related-report reconciliation | Exhaustive enumeration, grouping, and reconciliation; scores affected access order only | The cells and complete-pass rule were authorized; 72 was an outcome, not a quota |

The three author-selected target cells were:

- Immersive x Desktop/Planar;
- Immersive x Large Display; and
- Adaptive x Large Display.

## Queue bounds and resulting sizes

| Set | Authorized bound or rule | Result |
|---:|---|---:|
| 20 | No numerical 20-entry limit was stated | 20 systems resulted from the anchor/replacement and coauthor-evidence construction |
| 18 | At most 40 additional systems | Codex froze 18 |
| 50 | At most 60 additional systems | Codex froze 50 |
| 200 | At most 200 selected identities; positions 1--140 primary, 141--200 reserves | Software froze 200 |
| 24 | At most 30; supplied plan allowed up to three candidates per sparse cell | Supplied 25 reconciled deterministically to 24 |
| 72 | Complete enumeration of three specified cells; no inclusion cap | 72 new systems resulted after reconciliation |

## Correction to the 200-entry audit description

The original `queue_selection_audit.json` is preserved unchanged. Its `tie_break` text
mentions rationale class, descending score, and canonical ID but omits two earlier sort
strata implemented by `build_census_and_freeze_queue.py`.

The executable sort tuple is authoritative. Its complete order is:

1. `high_precision_signal`: `YES` before `NO`;
2. `selection_rationale_class`: sparse/empty cell, stronger evaluation/comparison,
   mechanism contrast, then contrast/limitation/negative;
3. `oa_likely_from_saved_doi`: `YES` before `NO`;
4. descending `ranking_score`; and
5. ascending `canonical_id` as the final tie-breaker.

The dated correction overlay is
`outputs/staging/candidate-census-full-report-expansion-v1-20260925/queue_selection_audit_correction_20261009.json`.
It changes descriptive audit metadata only: the original queue, ordering, roles, and
assessments were not regenerated or changed.

## Preserved unknowns

- The intellectual origin of the twelve-anchor shortlist is unknown. The session proves
  that Morris supplied it operationally, not whether he originated every nomination.
- The creator or creators of `followup_plan.md` and `provisional_queue.csv`, including the
  exact 25 nominations, remain unknown. Browser metadata showing download from
  `chatgpt.com` identifies a transfer channel, not authorship.
- No separate pre-assessment author approval is inferred for the exact 20-, 18-, 50-, or
  24-entry results.
- Morris's later review of evidence and reported placements does not convert automated
  assessment rows into independently or contemporaneously human-approved judgments.
