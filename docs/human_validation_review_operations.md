# Human-review operational extensions

The committed v1 review workspace and the canonical `230844Z` package remain immutable.
Operational changes are represented by new files outside the package manifest rather
than by editing the packet set, base assignment ledger, returns, or baseline
reconciliation.

## Supported behavior

- A record may have separate `primary` and `secondary` assignment slots. Independent
  review requires distinct reviewer IDs.
- Returns may be partial. Reconciliation classifies each record from the valid returns
  currently present and leaves the remainder missing.
- A reviewer correction is a new create-only returned-review file. It must name the
  current terminal submission in that same reviewer's assignment chain. The earlier
  judgment is preserved and remains auditable.
- Unfinished assigned work can be explicitly released and reassigned to reviewer ID
  `morris`. The operation creates a new assignment-ledger revision with a
  `RELEASED_AND_REASSIGNED` event. It does not overwrite the base ledger. A completed
  assignment cannot be released or reassigned by this operation.
- Reconciliation retains every submission ID, identifies the effective terminal review
  for each assignment, and counts independence by unique reviewer ID. Corrections and
  multiple assignments completed by the same reviewer do not create additional
  independent reviews. In particular, repeated reviews by `morris` remain a single
  independent reviewer and cannot create an independent-double-review disagreement.

## Commands

Use `python -m h2h_lit.human_validation_review_operations` with the package path and the
exact canonical package-manifest SHA-256. The available modes are:

- `--write-returned-review-revision`
- `--release-unfinished-to-morris`
- `--reconcile-review-history`

Corrections additionally require `--corrects-submission-id`. Release creates a separate
ledger revision at `--output`; later operations can bind to it through
`--assignment-ledger-revision`. Reconciliation also writes a new report at `--output`.

Do not use these commands to populate the current canonical workspace until reviewer
assignments and deadlines have been approved.

