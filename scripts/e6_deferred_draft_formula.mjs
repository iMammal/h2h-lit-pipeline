export const REQUIRED_DRAFT_CRITERIA = Object.freeze(["E1", "E2", "E3", "E4", "E5", "E7"]);
export const TRI_STATES = Object.freeze(["YES", "NO", "UNCERTAIN"]);
export const DEFERRED_E6_VALUE = "Not assessed at this stage";

export function recomputeDraftOutcome(decisions) {
  const keys = Object.keys(decisions).sort();
  const expected = [...REQUIRED_DRAFT_CRITERIA].sort();
  if (JSON.stringify(keys) !== JSON.stringify(expected)) {
    throw new Error("draft aggregate requires complete E1-E5 and E7 responses");
  }
  const values = REQUIRED_DRAFT_CRITERIA.map((criterion) => decisions[criterion]);
  const invalid = values.filter((value) => !TRI_STATES.includes(value));
  if (invalid.length) {
    throw new Error(`invalid draft criterion response: ${invalid.join(", ")}`);
  }
  if (values.includes("NO")) return "EXCLUDED";
  if (values.includes("UNCERTAIN")) return "UNCERTAIN";
  return "INCLUDE";
}

export function draftOutcomeFormula(row) {
  if (!Number.isInteger(row) || row < 1) throw new Error("row must be a positive integer");
  return `=IF(COUNTIF(E${row}:I${row},"NO")+COUNTIF(K${row},"NO")>0,"EXCLUDED",IF(COUNTIF(E${row}:I${row},"UNCERTAIN")+COUNTIF(K${row},"UNCERTAIN")>0,"UNCERTAIN",IF(COUNTIF(E${row}:I${row},"YES")+COUNTIF(K${row},"YES")=6,"INCLUDE","")))`;
}

export function validateDeferredE6(value) {
  if (value !== DEFERRED_E6_VALUE) {
    throw new Error(`E6 must remain exactly ${JSON.stringify(DEFERRED_E6_VALUE)}`);
  }
}
