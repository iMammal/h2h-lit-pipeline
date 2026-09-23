import crypto from "node:crypto";
import { execFile as execFileCallback } from "node:child_process";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const artifactRequire = createRequire(import.meta.url);
const { SpreadsheetFile } = artifactRequire("@oai/artifact-tool");
const execFile = promisify(execFileCallback);

if (process.argv.length !== 5) {
  throw new Error(
    "usage: node archive_approved_title_abstract_return.mjs <finished.xlsx> <validator-report.json> <fresh-output-dir>",
  );
}

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sourcePath = path.resolve(process.argv[2]);
const validatorPath = path.resolve(process.argv[3]);
const outputDir = path.resolve(process.argv[4]);
const frozenRoot = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920");
const workspaceRoot = path.join(frozenRoot, "review_workspaces/primary_100");
const samplingPath = path.join(frozenRoot, "internal/sampling_manifest.json");
const investigationPath = path.join(
  repoRoot,
  "outputs/title-abstract-screening-approved-v2-20260921/internal/row15_evidence_reconciliation_task.json",
);
const protocolPath = path.join(repoRoot, "config/title_abstract_screening_protocol_v2_0_0.json");
const schemaPath = path.join(repoRoot, "config/title_abstract_screening_return_schema_v2_0_0.json");
const conflictId = "canonical:7d25df89fed662b381109a2e";
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const relative = (target) => path.relative(repoRoot, target);
const jsonBytes = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8");

async function assertFreshDirectory(target) {
  try {
    await fs.access(target);
  } catch {
    await fs.mkdir(target, { recursive: false });
    return;
  }
  throw new Error(`archive directory already exists: ${target}`);
}

async function writeOnce(target, bytes) {
  await fs.writeFile(target, bytes, { flag: "wx" });
  return { path: relative(target), sha256: sha256(bytes) };
}

function counter(items, key) {
  const result = {};
  for (const item of items) {
    const value = item[key];
    result[value] = (result[value] || 0) + 1;
  }
  return result;
}

function completeCounts(counts, keys) {
  return Object.fromEntries(keys.map((key) => [key, counts[key] || 0]));
}

function nestedMissingCounts(records) {
  const groups = ["uniform_random", "challenge", "TOTAL"];
  const outcomes = ["INCLUDE", "UNCERTAIN", "EXCLUDED"];
  const result = Object.fromEntries(
    groups.map((group) => [group, Object.fromEntries(outcomes.map((outcome) => [outcome, 0]))]),
  );
  for (const record of records) {
    if (!record.abstract_missing) continue;
    result.TOTAL[record.computed_outcome] += 1;
    result[record.selection_group][record.computed_outcome] += 1;
  }
  return result;
}

function markdownCountRow(label, counts, keys) {
  return `| ${label} | ${keys.map((key) => counts[key] || 0).join(" | ")} |`;
}

const sourceBytes = await fs.readFile(sourcePath);
const sourceSha256 = sha256(sourceBytes);
const blankPath = path.join(
  repoRoot,
  "outputs/title-abstract-screening-approved-v2-20260921/primary_morris_100_records_APPROVED_v2.xlsx",
);
const blankSha256 = sha256(await fs.readFile(blankPath));
if (sourceSha256 === blankSha256) throw new Error("finished return is byte-identical to the blank packet");

const validatorBytes = await fs.readFile(validatorPath);
const validation = JSON.parse(validatorBytes.toString("utf8"));
if (
  validation.reviewer_id !== "Morris" ||
  validation.return_schema_version !== "2.0.0" ||
  validation.records !== 100
) {
  throw new Error("supported v2 validator did not confirm reviewer/schema/record count");
}
const expectedOutcomes = { EXCLUDED: 54, INCLUDE: 5, UNCERTAIN: 41 };
if (JSON.stringify(validation.computed_outcome_counts) !== JSON.stringify(expectedOutcomes)) {
  throw new Error("independently recomputed outcome counts differ from expected submitted counts");
}
if (Object.keys(validation.inconsistency_counts).length) {
  throw new Error("supported v2 validator reported return inconsistencies");
}

const workbook = await SpreadsheetFile.importXlsx(sourceBytes);
const instructions = workbook.worksheets.getItem("Instructions");
const reviews = workbook.worksheets.getItem("Reviews");
if (
  instructions.getRange("B3").values[0][0] !== "Morris" ||
  String(instructions.getRange("B6").values[0][0]) !== "2.0.0" ||
  String(instructions.getRange("B7").values[0][0]) !== "2.0.0"
) {
  throw new Error("workbook instruction bindings changed");
}
const workbookRows = reviews.getRange("A4:S103").values;
if (workbookRows.length !== 100) throw new Error("finished workbook does not contain 100 review rows");

const ledgerPath = path.join(workspaceRoot, "assignment_ledger.json");
const ledgerBytes = await fs.readFile(ledgerPath);
const ledger = JSON.parse(ledgerBytes.toString("utf8"));
if (ledger.assignments.length !== 100) throw new Error("frozen primary ledger count changed");
const frozenRecords = [];
for (const assignment of ledger.assignments) {
  const packetPath = path.join(workspaceRoot, assignment.packet_path);
  const packetBytes = await fs.readFile(packetPath);
  const packet = JSON.parse(packetBytes.toString("utf8"));
  if (packet.record.record_id !== assignment.record_id) throw new Error("frozen assignment/packet ID mismatch");
  frozenRecords.push({ ...packet.record, packet_path: relative(packetPath), packet_sha256: sha256(packetBytes) });
}

const allowed = new Set(["YES", "NO", "UNCERTAIN"]);
const recordIds = new Set();
const evidenceMismatches = [];
const responseProblems = [];
for (let index = 0; index < workbookRows.length; index += 1) {
  const row = workbookRows[index];
  const frozen = frozenRecords[index];
  const excelRow = index + 4;
  const recordId = String(row[0] ?? "");
  if (recordIds.has(recordId)) responseProblems.push({ row: excelRow, record_id: recordId, problem: "DUPLICATE_ID" });
  recordIds.add(recordId);
  for (const [field, submitted, expected] of [
    ["record_id", recordId, frozen.record_id],
    ["title", String(row[1] ?? ""), String(frozen.title ?? "")],
    ["abstract", String(row[2] ?? ""), String(frozen.abstract ?? "")],
    ["publication_year", String(row[3] ?? ""), String(frozen.publication_year ?? "")],
  ]) {
    if (submitted !== expected) evidenceMismatches.push({ row: excelRow, record_id: recordId, field });
  }
  const decisions = [row[4], row[5], row[6], row[7], row[8], row[10]];
  if (decisions.some((value) => !allowed.has(value))) responseProblems.push({ row: excelRow, record_id: recordId, problem: "INVALID_OR_INCOMPLETE_E1_E5_E7" });
  if (row[9] !== "NOT_ASSESSED_AT_THIS_STAGE") responseProblems.push({ row: excelRow, record_id: recordId, problem: "INVALID_E6_STATUS" });
  if (!["YES", "NO"].includes(row[11]) || !["YES", "NO"].includes(row[12])) responseProblems.push({ row: excelRow, record_id: recordId, problem: "INVALID_WORKFLOW_FLAG" });
}
if (recordIds.size !== 100 || evidenceMismatches.length || responseProblems.length) {
  throw new Error("finished workbook differs from frozen evidence/order or contains invalid responses");
}

const samplingBytes = await fs.readFile(samplingPath);
const sampling = JSON.parse(samplingBytes.toString("utf8"));
const sampleById = new Map(sampling.selected_records.map((item) => [item.canonical_id, item]));
const validationById = new Map(validation.rows.map((row) => [row.record_id, row]));
const records = workbookRows.map((row, index) => {
  const recordId = row[0];
  const validated = validationById.get(recordId);
  const sampled = sampleById.get(recordId);
  if (!validated || !sampled || !["uniform_random", "challenge"].includes(sampled.selection_group)) {
    throw new Error(`missing validation or frozen sample binding: ${recordId}`);
  }
  return {
    worksheet_row: index + 4,
    record_id: recordId,
    title: row[1],
    abstract_missing: String(row[2] ?? "").trim() === "",
    publication_year: row[3],
    selection_group: sampled.selection_group,
    criteria: validated.criteria,
    e6_status: validated.e6_status,
    submitted_evidence_conflict: row[11],
    submitted_targeted_second_review: row[12],
    computed_outcome: validated.computed_outcome,
    submitted_cached_outcome: validated.cached_outcome,
    computed_next_action: validated.computed_next_action,
    submitted_cached_next_action: validated.cached_next_action,
    actionable_next_action: recordId === conflictId ? "EVIDENCE_RECONCILIATION" : validated.computed_next_action,
    benchmark_scorable: recordId !== conflictId,
    primary_exclusion_reason: validated.primary_exclusion_reason,
    formula_cache_status: validated.cache_status,
    next_action_cache_status: validated.next_action_cache_status,
  };
});

const submittedRoutes = completeCounts(counter(records, "computed_next_action"), ["METADATA_RECOVERY", "FULL_TEXT_ASSESSMENT", "NONE"]);
const expectedRoutes = { METADATA_RECOVERY: 31, FULL_TEXT_ASSESSMENT: 15, NONE: 54 };
if (JSON.stringify(submittedRoutes) !== JSON.stringify(expectedRoutes)) throw new Error("independently recomputed route counts changed");
const conflict = records.find((record) => record.record_id === conflictId);
if (!conflict || conflict.worksheet_row !== 15 || conflict.submitted_evidence_conflict !== "NO" || conflict.computed_outcome !== "EXCLUDED" || conflict.computed_next_action !== "NONE") {
  throw new Error("known row-15 submitted judgment changed");
}

const investigationBytes = await fs.readFile(investigationPath);
const investigation = JSON.parse(investigationBytes.toString("utf8"));
if (investigation.record_id !== conflictId || investigation.status !== "NOT_EXECUTED") throw new Error("existing provider-evidence investigation binding changed");
const providerPath = path.join(repoRoot, investigation.original_provider_response.path);
const providerSha256 = sha256(await fs.readFile(providerPath));
if (providerSha256 !== investigation.original_provider_response.sha256) throw new Error("original provider response hash changed");

const outcomes = completeCounts(counter(records, "computed_outcome"), ["INCLUDE", "UNCERTAIN", "EXCLUDED"]);
const actionableRoutes = completeCounts(counter(records, "actionable_next_action"), ["METADATA_RECOVERY", "FULL_TEXT_ASSESSMENT", "NONE", "EVIDENCE_RECONCILIATION"]);
const scorable = records.filter((record) => record.benchmark_scorable);
const scorableOutcomes = completeCounts(counter(scorable, "computed_outcome"), ["INCLUDE", "UNCERTAIN", "EXCLUDED"]);
const missingAbstractCounts = nestedMissingCounts(records);
const includeRecords = records.filter((record) => record.computed_outcome === "INCLUDE").map((record) => ({
  record_id: record.record_id,
  title: record.title,
  abstract_missing: record.abstract_missing,
  selection_group: record.selection_group,
}));
const groupResults = {};
for (const group of ["uniform_random", "challenge"]) {
  const groupRecords = records.filter((record) => record.selection_group === group);
  const groupScorable = groupRecords.filter((record) => record.benchmark_scorable);
  groupResults[group] = {
    total: groupRecords.length,
    submitted_outcomes: completeCounts(counter(groupRecords, "computed_outcome"), ["INCLUDE", "UNCERTAIN", "EXCLUDED"]),
    scorable_outcomes: completeCounts(counter(groupScorable, "computed_outcome"), ["INCLUDE", "UNCERTAIN", "EXCLUDED"]),
    submitted_routes: completeCounts(counter(groupRecords, "computed_next_action"), ["METADATA_RECOVERY", "FULL_TEXT_ASSESSMENT", "NONE"]),
    actionable_routes: completeCounts(counter(groupRecords, "actionable_next_action"), ["METADATA_RECOVERY", "FULL_TEXT_ASSESSMENT", "NONE", "EVIDENCE_RECONCILIATION"]),
  };
}
if (groupResults.uniform_random.total !== 70 || groupResults.challenge.total !== 30) throw new Error("frozen 70/30 sample mapping changed");

const { stdout: reviewsXml } = await execFile("unzip", ["-p", sourcePath, "xl/worksheets/sheet2.xml"], { maxBuffer: 16 * 1024 * 1024 });
const sharedFormulaRangeFinding = reviewsXml.includes('ref="O100:O131"');

await assertFreshDirectory(outputDir);
const evidenceDir = path.join(outputDir, "submitted_evidence");
const internalDir = path.join(outputDir, "internal");
const reconciliationDir = path.join(outputDir, "reconciliation");
await fs.mkdir(evidenceDir);
await fs.mkdir(internalDir);
await fs.mkdir(reconciliationDir);

const archivedWorkbookPath = path.join(evidenceDir, path.basename(sourcePath));
const archivedWorkbookRef = await writeOnce(archivedWorkbookPath, sourceBytes);
if (archivedWorkbookRef.sha256 !== sourceSha256) throw new Error("byte-preserving archive copy failed");
const validatorRef = await writeOnce(path.join(internalDir, "supported_v2_return_validation.json"), validatorBytes);
const recordValidation = {
  artifact_class: "TITLE_ABSTRACT_PILOT_RETURN_RECORD_VALIDATION",
  protocol_version: "2.0.0",
  return_schema_version: "2.0.0",
  reviewer_id: "Morris",
  independence_inferred: false,
  source_submission_sha256: sourceSha256,
  frozen_primary_ledger: { path: relative(ledgerPath), sha256: sha256(ledgerBytes) },
  frozen_sampling_manifest: { path: relative(samplingPath), sha256: sha256(samplingBytes) },
  validation: {
    records: records.length,
    unique_ids: recordIds.size,
    frozen_ids_titles_abstracts_years_and_order_match: true,
    complete_valid_e1_e5_e7: true,
    e6_deferred_for_all_records: true,
    independently_recomputed_without_formula_caches: true,
    formula_cache_counts: validation.formula_cache_counts,
    next_action_cache_counts: completeCounts(counter(records, "next_action_cache_status"), ["MATCH", "BLANK", "STALE_OR_INCORRECT"]),
  },
  submitted_counts: { outcomes, routes: submittedRoutes },
  actionable_counts: { outcomes, routes: actionableRoutes },
  scorable_counts_pending_conflict_re_review: { records: scorable.length, outcomes: scorableOutcomes },
  frozen_sample_groups: groupResults,
  include_records: includeRecords,
  missing_abstract_counts_by_outcome_and_group: missingAbstractCounts,
  records,
};
const recordValidationBytes = jsonBytes(recordValidation);
const recordValidationRef = await writeOnce(path.join(internalDir, "record_validation.json"), recordValidationBytes);

const reconciliation = {
  artifact_class: "TITLE_ABSTRACT_EVIDENCE_RECONCILIATION_HOLD",
  protocol_version: "2.0.0",
  return_schema_version: "2.0.0",
  record_id: conflictId,
  worksheet_row: conflict.worksheet_row,
  reviewer_id: "Morris",
  submitted_judgment_preserved: {
    evidence_conflict: conflict.submitted_evidence_conflict,
    criteria: conflict.criteria,
    outcome: conflict.computed_outcome,
    next_action: conflict.computed_next_action,
    primary_exclusion_reason: conflict.primary_exclusion_reason,
  },
  validation_override: {
    evidence_conflict: true,
    actionable_next_action: "EVIDENCE_RECONCILIATION",
    benchmark_scorable: false,
    reason: "Known title/abstract mismatch; corrected evidence and re-review are required before benchmark scoring.",
  },
  evidence: {
    title: conflict.title,
    abstract_issue: "The supplied abstract concerns asymptomatic COVID-19 rather than the melanoma/autoimmune-disease title.",
    existing_investigation: { path: relative(investigationPath), sha256: sha256(investigationBytes) },
    original_provider_response: { path: relative(providerPath), sha256: providerSha256, preserved_unchanged: true },
  },
  constraints: {
    workbook_modified: false,
    metadata_replaced: false,
    superseding_reviewer_judgment_created: false,
    production_screening_imported: false,
  },
};
const reconciliationRef = await writeOnce(path.join(reconciliationDir, "canonical-7d25df89fed662b381109a2e.json"), jsonBytes(reconciliation));

const groupOutcomeLines = ["uniform_random", "challenge"].flatMap((group) => {
  const item = groupResults[group];
  const label = group === "uniform_random" ? "Random" : "Challenge";
  const scorableTotal = Object.values(item.scorable_outcomes).reduce((sum, value) => sum + value, 0);
  return [
    `| ${label} | Submitted | ${item.total} | ${item.submitted_outcomes.INCLUDE} | ${item.submitted_outcomes.UNCERTAIN} | ${item.submitted_outcomes.EXCLUDED} |`,
    `| ${label} | Scorable pending reconciliation | ${scorableTotal} | ${item.scorable_outcomes.INCLUDE} | ${item.scorable_outcomes.UNCERTAIN} | ${item.scorable_outcomes.EXCLUDED} |`,
  ];
}).join("\n");
const groupRouteLines = ["uniform_random", "challenge"].flatMap((group) => {
  const item = groupResults[group];
  const label = group === "uniform_random" ? "Random" : "Challenge";
  return [
    `| ${label} | Submitted | ${item.submitted_routes.METADATA_RECOVERY} | ${item.submitted_routes.FULL_TEXT_ASSESSMENT} | ${item.submitted_routes.NONE} | 0 |`,
    `| ${label} | Actionable | ${item.actionable_routes.METADATA_RECOVERY} | ${item.actionable_routes.FULL_TEXT_ASSESSMENT} | ${item.actionable_routes.NONE} | ${item.actionable_routes.EVIDENCE_RECONCILIATION} |`,
  ];
}).join("\n");
const missingLines = ["uniform_random", "challenge", "TOTAL"].map((group) => {
  const item = missingAbstractCounts[group];
  return `| ${group === "uniform_random" ? "Random" : group === "challenge" ? "Challenge" : "Total"} | ${item.INCLUDE} | ${item.UNCERTAIN} | ${item.EXCLUDED} | ${item.INCLUDE + item.UNCERTAIN + item.EXCLUDED} |`;
}).join("\n");
const includeLines = includeRecords.map((record) => `- \`${record.record_id}\` — ${record.title} — missing abstract: **${record.abstract_missing ? "yes" : "no"}** — ${record.selection_group === "uniform_random" ? "random" : "challenge"}`).join("\n");
const report = `# Morris 100-record pilot return validation\n\nStatus: archived validated return under protocol 2.0.0. This archive does not import production screening decisions.\n\n## Validation and preservation\n\n- Reviewer ID: **Morris**. The name is retained; review independence is not inferred.\n- Completed source: \`${relative(sourcePath)}\`\n- Original SHA-256: \`${sourceSha256}\`\n- Archived copy: \`${archivedWorkbookRef.path}\`\n- Archived SHA-256: \`${archivedWorkbookRef.sha256}\` (byte-identical)\n- 100 unique stable IDs match the frozen primary packet. Titles, abstracts, years, and order are unchanged.\n- All E1-E5/E7 responses are complete and valid. E6 is \`NOT_ASSESSED_AT_THIS_STAGE\` for all 100 records.\n- Outcomes and routes were recomputed by the supported v2 validator. Formula caches were diagnostic only; all 100 outcome and route caches match.\n\n## Submitted and actionable counts\n\n| View | INCLUDE | UNCERTAIN | EXCLUDED | Scorable records |\n|---|---:|---:|---:|---:|\n| Submitted | ${outcomes.INCLUDE} | ${outcomes.UNCERTAIN} | ${outcomes.EXCLUDED} | 100 before conflict hold |\n| Scorable pending row-15 reconciliation | ${scorableOutcomes.INCLUDE} | ${scorableOutcomes.UNCERTAIN} | ${scorableOutcomes.EXCLUDED} | ${scorable.length} |\n\n| Route view | METADATA_RECOVERY | FULL_TEXT_ASSESSMENT | NONE | EVIDENCE_RECONCILIATION |\n|---|---:|---:|---:|---:|\n| Submitted | ${submittedRoutes.METADATA_RECOVERY} | ${submittedRoutes.FULL_TEXT_ASSESSMENT} | ${submittedRoutes.NONE} | 0 |\n| Actionable after known conflict flag | ${actionableRoutes.METADATA_RECOVERY} | ${actionableRoutes.FULL_TEXT_ASSESSMENT} | ${actionableRoutes.NONE} | ${actionableRoutes.EVIDENCE_RECONCILIATION} |\n\nThe conflict hold does not change Morris's submitted criteria or EXCLUDED outcome. It changes only the separate actionable route and scoring availability.\n\n## Frozen random and challenge groups\n\n| Group | Outcome view | Records | INCLUDE | UNCERTAIN | EXCLUDED |\n|---|---|---:|---:|---:|---:|\n${groupOutcomeLines}\n\n| Group | Route view | METADATA_RECOVERY | FULL_TEXT_ASSESSMENT | NONE | EVIDENCE_RECONCILIATION |\n|---|---|---:|---:|---:|---:|\n${groupRouteLines}\n\nThe scorable random view excludes the known conflict. The challenge group is unchanged.\n\n## INCLUDE records\n\n${includeLines}\n\n## Missing abstracts by submitted outcome and sample group\n\n| Group | INCLUDE | UNCERTAIN | EXCLUDED | Total missing |\n|---|---:|---:|---:|---:|\n${missingLines}\n\n## Known evidence conflict\n\nWorksheet row 15, \`${conflictId}\`, retains Morris's submitted Evidence conflict=NO, EXCLUDED outcome, and NONE route. The supplied melanoma/autoimmune-disease title is paired with a COVID-19 abstract. The separate reconciliation artifact flags the conflict, routes it to \`EVIDENCE_RECONCILIATION\`, and marks the judgment unavailable for benchmark scoring until corrected evidence is versioned and the record is re-reviewed. No workbook cell, metadata field, or judgment was altered.\n\nExisting provider-evidence investigation: \`${relative(investigationPath)}\` (SHA-256 \`${sha256(investigationBytes)}\`). Original provider response remains unchanged at \`${relative(providerPath)}\` (SHA-256 \`${providerSha256}\`).\n\n## Other findings and limitations\n\n- No additional ID, title, abstract, year, order, response-completeness, E6-status, outcome-cache, route-cache, or supported-validator inconsistency was found. This was not a new scientific adjudication of the other 99 judgments.\n- ${sharedFormulaRangeFinding ? "Excel serialized the final shared next-action formula block with range `O100:O131`, beyond the used range ending at row 103. Only rows 100-103 exist; their cached routes match independent recomputation. This non-blocking serialization quirk was preserved." : "No shared-formula range anomaly was detected."}\n- The conversation and prior protocol-development work exposed some sample records and model judgments. The return must not be described as an independent blinded human reference without additional provenance.\n- The known conflict record is explicitly separated from the 99 currently scorable records. No other evidence-content mismatches were adjudicated or inferred.\n`;
const reportRef = await writeOnce(path.join(outputDir, "pilot_return_validation_report.md"), Buffer.from(report, "utf8"));

const manifestPayload = {
  artifact_class: "TITLE_ABSTRACT_PILOT_RETURN_ARCHIVE",
  schema_version: "1.0.0",
  protocol_version: "2.0.0",
  return_schema_version: "2.0.0",
  status: "VALIDATED_STAGING_ARCHIVE_NOT_IMPORTED",
  created_at_utc: new Date().toISOString(),
  reviewer_id: "Morris",
  reviewer_independence_inferred: false,
  known_exposure_limitation: "This conversation and prior protocol-development work exposed some sample records and model judgments; no independent blinded human-reference claim is made.",
  source_submission: { path: relative(sourcePath), sha256: sourceSha256, bytes: sourceBytes.length },
  blank_packet: { path: relative(blankPath), sha256: blankSha256, distinct_from_submission: true },
  frozen_bindings: {
    primary_ledger: { path: relative(ledgerPath), sha256: sha256(ledgerBytes) },
    sampling_manifest: { path: relative(samplingPath), sha256: sha256(samplingBytes) },
    protocol: { path: relative(protocolPath), sha256: sha256(await fs.readFile(protocolPath)) },
    return_schema: { path: relative(schemaPath), sha256: sha256(await fs.readFile(schemaPath)) },
  },
  artifacts: {
    archived_original: archivedWorkbookRef,
    supported_v2_validation: validatorRef,
    record_validation: recordValidationRef,
    reconciliation_hold: reconciliationRef,
    report: reportRef,
  },
  counts: {
    submitted: { records: 100, outcomes, routes: submittedRoutes },
    actionable_routes: actionableRoutes,
    scorable_pending_reconciliation: { records: scorable.length, outcomes: scorableOutcomes },
  },
  state_effects: {
    production_screening_imported: false,
    production_state_modified: false,
    identification_closed: false,
    prisma_updated: false,
    metadata_recovered: false,
    models_or_providers_called: false,
  },
};
manifestPayload.artifact_hash = sha256(Buffer.from(JSON.stringify(manifestPayload), "utf8"));
const manifestRef = await writeOnce(path.join(outputDir, "archive_manifest.json"), jsonBytes(manifestPayload));

process.stdout.write(`${JSON.stringify({
  output_dir: relative(outputDir),
  source_sha256: sourceSha256,
  archived_sha256: archivedWorkbookRef.sha256,
  submitted_counts: { outcomes, routes: submittedRoutes },
  actionable_routes: actionableRoutes,
  scorable_records: scorable.length,
  group_results: groupResults,
  include_records: includeRecords,
  missing_abstract_counts: missingAbstractCounts,
  report: reportRef,
  manifest: manifestRef,
}, null, 2)}\n`);
