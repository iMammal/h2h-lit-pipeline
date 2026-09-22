import test from "node:test";
import assert from "node:assert/strict";

import {
  DEFERRED_E6_VALUE,
  draftOutcomeFormula,
  recomputeDraftOutcome,
  validateDeferredE6,
} from "../scripts/e6_deferred_draft_formula.mjs";

const allYes = { E1: "YES", E2: "YES", E3: "YES", E4: "YES", E5: "YES", E7: "YES" };

test("all YES retains the record for further assessment", () => {
  assert.equal(recomputeDraftOutcome(allYes), "INCLUDE");
});

test("an E5-only NO excludes", () => {
  assert.equal(recomputeDraftOutcome({ ...allYes, E5: "NO" }), "EXCLUDED");
});

test("an E5-only UNCERTAIN advances as uncertain", () => {
  assert.equal(recomputeDraftOutcome({ ...allYes, E5: "UNCERTAIN" }), "UNCERTAIN");
});

test("NO takes precedence over UNCERTAIN", () => {
  assert.equal(
    recomputeDraftOutcome({ ...allYes, E2: "UNCERTAIN", E5: "NO" }),
    "EXCLUDED",
  );
});

test("E6 is explicitly deferred and is outside the aggregate", () => {
  assert.doesNotThrow(() => validateDeferredE6(DEFERRED_E6_VALUE));
  assert.equal(recomputeDraftOutcome(allYes), "INCLUDE");
  assert.throws(() => validateDeferredE6("YES"), /E6 must remain exactly/);
});

test("incomplete responses are rejected", () => {
  const incomplete = { ...allYes };
  delete incomplete.E5;
  assert.throws(() => recomputeDraftOutcome(incomplete), /complete E1-E5 and E7/);
});

test("invalid responses are rejected", () => {
  assert.throws(
    () => recomputeDraftOutcome({ ...allYes, E5: "MAYBE" }),
    /invalid draft criterion response/,
  );
});

test("saved-workbook formula includes E5, excludes E6, and requires six YES values", () => {
  const formula = draftOutcomeFormula(15);
  assert.match(formula, /COUNTIF\(E15:I15,"NO"\)/);
  assert.match(formula, /COUNTIF\(K15,"NO"\)/);
  assert.doesNotMatch(formula, /J15/);
  assert.match(formula, /=6/);
});
