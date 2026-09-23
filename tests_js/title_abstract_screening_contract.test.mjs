import assert from "node:assert/strict";
import test from "node:test";

import {
  E6_STATUS,
  nextActionFormula,
  outcomeFormula,
  recommendFastTrackDisposition,
  recommendNextAction,
  recomputeOutcome,
} from "../scripts/title_abstract_screening_contract.mjs";

const answers = (overrides = {}) => ({
  E1: "YES", E2: "YES", E3: "YES", E4: "YES", E5: "YES", E7: "YES", ...overrides,
});

test("approved aggregation includes E5 and treats E7 insufficiency as uncertainty", () => {
  assert.equal(recomputeOutcome(answers()), "INCLUDE");
  assert.equal(recomputeOutcome(answers({ E5: "NO" })), "EXCLUDED");
  assert.equal(recomputeOutcome(answers({ E5: "UNCERTAIN" })), "UNCERTAIN");
  assert.equal(recomputeOutcome(answers({ E2: "NO", E5: "UNCERTAIN" })), "EXCLUDED");
  assert.equal(recomputeOutcome(answers({ E7: "NO" })), "UNCERTAIN");
});

test("E6 is literal and deferred", () => {
  assert.equal(E6_STATUS, "NOT_ASSESSED_AT_THIS_STAGE");
  assert.throws(() => recomputeOutcome(answers(), "YES"));
});

test("routing applies conflict, exclusion, metadata, and second-review precedence", () => {
  assert.equal(recommendNextAction(answers({ E2: "NO" }), { abstractMissing: true }), "NONE");
  assert.equal(recommendNextAction(answers(), { abstractMissing: true }), "METADATA_RECOVERY");
  assert.equal(recommendNextAction(answers(), { evidenceConflict: true }), "EVIDENCE_RECONCILIATION");
  assert.equal(recommendNextAction({ E1: "YES" }), "TARGETED_SECOND_REVIEW");
  assert.equal(recommendNextAction(answers(), { targetedSecondReviewRequested: true }), "TARGETED_SECOND_REVIEW");
});

test("workbook formulas include E5 and exclude E6", () => {
  const outcome = outcomeFormula(4);
  assert.match(outcome, /E4:I4/);
  assert.match(outcome, /K4/);
  assert.doesNotMatch(outcome, /J4/);
  const action = nextActionFormula(4);
  assert.match(action, /METADATA_RECOVERY/);
  assert.match(action, /EVIDENCE_RECONCILIATION/);
});

test("fast-track disposition is operational and preserves scientific precedence", () => {
  assert.equal(
    recommendFastTrackDisposition(answers(), { fullReportAvailable: true }),
    "ADVANCE_TO_FULL_REPORT_ASSESSMENT",
  );
  assert.equal(
    recommendFastTrackDisposition(answers({ E3: "NO" }), { fullReportAvailable: true }),
    "EXCLUDE",
  );
  assert.equal(
    recommendFastTrackDisposition(answers({ E7: "UNCERTAIN" }), { fullReportAvailable: true }),
    "DEFER",
  );
  assert.equal(
    recommendFastTrackDisposition(answers(), { abstractMissing: true, fullReportAvailable: true }),
    "DEFER",
  );
  assert.equal(
    recommendFastTrackDisposition(answers(), { evidenceConflict: true, fullReportAvailable: true }),
    "DEFER",
  );
  assert.equal(
    recommendFastTrackDisposition(answers(), { fullReportAvailable: false }),
    "DEFER",
  );
  assert.throws(() => recommendFastTrackDisposition({ E1: "YES" }, { fullReportAvailable: true }));
});
