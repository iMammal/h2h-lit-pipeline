# Human Validation Review Workflow

This workflow prepares blinded human-review materials without changing corpus membership,
screening status, PRISMA counts, model outputs, or the production review dataset. The current
prepared workspace is based on the 40-record provisional validation sample and is not a
production screening or adjudication artifact.

## Artifacts

Each workspace contains:

- `packets/record-<hash>.json`: one immutable, reviewer-neutral packet per stable record ID;
- `assignment_ledger.json`: authoritative assignments and complete reassignment histories;
- `assignment_ledger.csv`: compact operational view of the same ledger;
- `returned_reviews/RETURN_SCHEMA.json`: structured return contract;
- `returned_reviews/<reviewer-hash>/<submission-hash>.json`: create-only judgments;
- `reconciliation_report.json`: baseline coverage report; and
- `reconciliation/reconciliation-<hash>.json`: later reconciliation snapshots.

Packets include title, abstract, publication year, the frozen E1-E7 criteria, allowed values,
aggregate eligibility rules, exclusion reasons, and a structured response template. They do
not include model proposals, sampling strata, prior human judgments, or reviewer identities.

## Assignment Plan

Do not invent reviewer identities or deadlines. Prepare a new, hash-bound workspace after an
assignment plan has been approved. Packet identities remain stable when only assignments
change.

```json
{
  "assignments": [
    {
      "record_id": "provisional-canonical:...",
      "slot_id": "primary",
      "reviewer_id": "reviewer-alpha",
      "due_date": "2026-10-01",
      "status": "ASSIGNED",
      "reassignment_history": []
    }
  ]
}
```

An independently double-reviewed record has two assignment rows with distinct `slot_id` and
`reviewer_id` values. A reassigned slot retains its slot and assignment ID and appends an event:

```json
{
  "sequence": 1,
  "from_reviewer_id": "reviewer-alpha",
  "to_reviewer_id": "reviewer-beta",
  "changed_at_utc": "2026-09-25T12:00:00Z",
  "reason": "availability"
}
```

## Commands

Use the PyCharm-configured interpreter. These examples assume `PYTHONPATH=src`.

Prepare a workspace:

```bash
python -m h2h_lit.human_validation_review \
  --root . \
  --prepare-review-workspace \
  --source-csv outputs/provisional/star-pipeline-validation-001/screening/human_validation_sample.csv \
  --source-manifest outputs/provisional/star-pipeline-validation-001/screening/human_validation_sample_manifest.json \
  --rubric docs/revised_star_coding_rubric.md \
  --assignment-plan path/to/approved-assignment-plan.json \
  --output-dir outputs/staging/human-validation-review-workspace-v1-<timestamp> \
  --generated-at <UTC-timestamp>
```

Write one reviewer return from a completed response JSON object:

```bash
python -m h2h_lit.human_validation_review \
  --root . \
  --write-returned-review \
  --package-dir <workspace> \
  --package-manifest-sha256 <manifest-sha256> \
  --assignment-id <assignment-id> \
  --reviewer-id <reviewer-id> \
  --submitted-at <UTC-timestamp> \
  --response-json <completed-response.json>
```

The writer refuses unknown assignments, reviewer mismatches, logically inconsistent aggregate
decisions, and every attempt to overwrite an existing returned judgment.

Reconcile returns:

```bash
python -m h2h_lit.human_validation_review \
  --root . \
  --reconcile-returned-reviews \
  --package-dir <workspace> \
  --package-manifest-sha256 <manifest-sha256> \
  --generated-at <UTC-timestamp>
```

Reconciliation reports missing, single-reviewed, independently double-reviewed, agreement,
and disagreement counts. Disagreements are a subset of independently double-reviewed records.
The report identifies differing E1-E7, aggregate-eligibility, and primary-exclusion fields but
does not select a winning judgment or adjudicate the record.
