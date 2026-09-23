import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const contractPath = path.join(repoRoot, "config/title_abstract_screening_protocol_v2_0_0.json");
export const contract = Object.freeze(JSON.parse(fs.readFileSync(contractPath, "utf8")));
export const E6_STATUS = contract.e6.status;
export const ASSESSED_CRITERIA = Object.freeze([...contract.assessed_criteria]);
export const SCIENTIFIC_CRITERIA = Object.freeze([...contract.scientific_criteria]);
export const TRI_STATES = Object.freeze([...contract.criterion_states]);

export function recomputeOutcome(responses, e6Status = E6_STATUS) {
  if (e6Status !== E6_STATUS) throw new Error(`E6 must be ${E6_STATUS}`);
  if (JSON.stringify(Object.keys(responses).sort()) !== JSON.stringify([...ASSESSED_CRITERIA].sort())) {
    throw new Error("complete E1-E5 and E7 responses are required");
  }
  if (ASSESSED_CRITERIA.some((criterion) => !TRI_STATES.includes(responses[criterion]))) {
    throw new Error("invalid criterion response");
  }
  if (SCIENTIFIC_CRITERIA.some((criterion) => responses[criterion] === "NO")) return "EXCLUDED";
  if (SCIENTIFIC_CRITERIA.some((criterion) => responses[criterion] === "UNCERTAIN")) return "UNCERTAIN";
  if (["NO", "UNCERTAIN"].includes(responses.E7)) return "UNCERTAIN";
  return "INCLUDE";
}

export function recommendNextAction(responses, options = {}) {
  const {
    e6Status = E6_STATUS,
    abstractMissing = false,
    evidenceConflict = false,
    targetedSecondReviewRequested = false,
  } = options;
  if (evidenceConflict) return "EVIDENCE_RECONCILIATION";
  let outcome;
  try {
    outcome = recomputeOutcome(responses, e6Status);
  } catch {
    return "TARGETED_SECOND_REVIEW";
  }
  if (outcome === "EXCLUDED") return "NONE";
  if (abstractMissing) return "METADATA_RECOVERY";
  if (targetedSecondReviewRequested) return "TARGETED_SECOND_REVIEW";
  return "FULL_TEXT_ASSESSMENT";
}

export function recommendFastTrackDisposition(responses, options = {}) {
  const {
    e6Status = E6_STATUS,
    abstractMissing = false,
    evidenceConflict = false,
    fullReportAvailable = false,
  } = options;
  const outcome = recomputeOutcome(responses, e6Status);
  if (evidenceConflict) return "DEFER";
  if (outcome === "EXCLUDED") return "EXCLUDE";
  if (abstractMissing || outcome === "UNCERTAIN" || !fullReportAvailable) return "DEFER";
  return "ADVANCE_TO_FULL_REPORT_ASSESSMENT";
}

export function outcomeFormula(row) {
  return contract.formula_templates.outcome.replaceAll("{row}", String(row));
}

export function nextActionFormula(row) {
  return contract.formula_templates.next_action.replaceAll("{row}", String(row));
}
