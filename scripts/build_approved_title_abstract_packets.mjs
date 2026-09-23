import crypto from "node:crypto";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const artifactRequire = createRequire(import.meta.url);
const { SpreadsheetFile, Workbook } = artifactRequire("@oai/artifact-tool");

import {
  ASSESSED_CRITERIA,
  E6_STATUS,
  contract,
  nextActionFormula,
  outcomeFormula,
} from "./title_abstract_screening_contract.mjs";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const frozenRoot = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920");
const outputDir = path.resolve(process.argv[2] || path.join(repoRoot, "outputs/title-abstract-screening-approved-v2-20260921"));
const internalDir = path.join(outputDir, "internal");
const previewDir = path.join(internalDir, "workbook_previews");
const postSavePreviewDir = path.join(internalDir, "post_save_visual_verification");
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const fontFamily = "Arial";

const expectedBindings = {
  retrieval_cutoff: "2026-09-20",
  corpus_sha256: "44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2",
  overlay_sha256: "c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d",
  sampling_manifest_sha256: "a2fd6e54c6a955eadbd4ea63d9996b9bbb8ec29c65024a733eefdd5f13747249",
};

const packetSpecs = [
  { key: "primary_100", file: "primary_morris_100_records_APPROVED_v2.xlsx", title: "Approved primary title/abstract review", reviewer: "Morris", role: "Primary", note: "Primary 100-record evaluation packet. Blank approved-procedure return; no prior judgments are included." },
  { key: "secondary_a_25", file: "optional_secondary_a_25_records_APPROVED_v2.xlsx", title: "Approved optional secondary review A", reviewer: "", role: "Optional secondary", note: "Prospectively selected subset of the same evaluation 100. Reviewer and deadline remain unassigned; this is not a completed independent review." },
  { key: "secondary_b_25", file: "optional_secondary_b_25_records_APPROVED_v2.xlsx", title: "Approved optional secondary review B", reviewer: "", role: "Optional secondary", note: "Prospectively selected subset of the same evaluation 100. Reviewer and deadline remain unassigned; this is not a completed independent review." },
  { key: "calibration_10", file: "calibration_10_records_APPROVED_v2.xlsx", title: "Approved title/abstract calibration", reviewer: "", role: "Calibration", note: "Separate calibration packet. Preserve it outside evaluation metrics." },
];

const criteria = [
  ["E1", "Life-science application", "Is the reported system applied to analysis, understanding, monitoring, decision-making, discovery, or scientific practice in an in-scope life-science, biomedical, clinical, health, neuroscience, ecology, or related domain?"],
  ["E2", "Relational, derived-structure, or multiscale relevance", "Does the system analyze explicit relationships or relationships derived from spatial, temporal, multivariate, image-derived, lineage, similarity, or other multiscale data?"],
  ["E3", "Interactive visual analytics", "Does a human use an interactive or analytically meaningful visual representation to inspect, understand, validate, steer, compare, interpret, or act on results?"],
  ["E4", "Substantive computational assistance", "Does a nontrivial computational mechanism operate inside the interactive visual-analytics workflow, beyond ordinary rendering or literal direct manipulation?"],
  ["E5", "Human analytic relationship", "Does a human meaningfully inspect, direct, validate, interpret, collaborate with, supervise, correct, or make decisions from the assisted process?"],
  ["E6", "Administrative scope", "Deferred to administrative verification. Do not assess at this title/abstract stage."],
  ["E7", "Evidence sufficiency", "Does the available title/abstract evidence support a defensible determination for E1-E5 at this stage? A NO or UNCERTAIN means unresolved evidence, not scientific exclusion."],
];
const exclusionReasons = [
  "EX_NO_LIFE_SCIENCE_APPLICATION",
  "EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE",
  "EX_NO_INTERACTIVE_VISUAL_ANALYTICS",
  "EX_NO_QUALIFYING_ASSISTANCE",
  "EX_NO_HUMAN_ANALYTIC_RELATIONSHIP",
  "EX_OTHER_PROTOCOL_REASON",
];

async function loadWorkspace(spec) {
  const workspace = path.join(frozenRoot, "review_workspaces", spec.key);
  const ledgerPath = path.join(workspace, "assignment_ledger.json");
  const ledgerBytes = await fs.readFile(ledgerPath);
  const ledger = JSON.parse(ledgerBytes.toString("utf8"));
  const records = [];
  for (const assignment of ledger.assignments) {
    const packetPath = path.join(workspace, assignment.packet_path);
    const packetBytes = await fs.readFile(packetPath);
    const packet = JSON.parse(packetBytes.toString("utf8"));
    if (packet.record.record_id !== assignment.record_id) throw new Error(`assignment/packet ID mismatch: ${assignment.record_id}`);
    records.push({
      ...packet.record,
      source_row_sha256: packet.source_row_sha256,
      packet_path: path.relative(repoRoot, packetPath),
      packet_sha256: sha256(packetBytes),
      assignment_id: assignment.assignment_id,
      reviewer_id: assignment.reviewer_id,
      due_date: assignment.due_date,
    });
  }
  const reviewers = [...new Set(ledger.assignments.map((item) => item.reviewer_id).filter(Boolean))];
  if (spec.key === "primary_100" && (reviewers.length !== 1 || reviewers[0].toLowerCase() !== "morris")) throw new Error("primary reviewer binding changed");
  if (spec.key !== "primary_100" && reviewers.length) throw new Error(`${spec.key} must remain unassigned`);
  return { records, ledgerPath: path.relative(repoRoot, ledgerPath), ledgerSha256: sha256(ledgerBytes) };
}

function styleHeader(range, fill = "#4472C4") {
  range.format = { fill, font: { name: fontFamily, size: 10, bold: true, color: "#FFFFFF" }, wrapText: true, verticalAlignment: "center", horizontalAlignment: "center", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
}

async function renderChecks(workbook, spec, endRow, baseDir) {
  await fs.mkdir(baseDir, { recursive: true });
  for (const [sheetName, range, suffix] of [
    ["Instructions", "A1:D27", "instructions"],
    ["Reviews", `A1:S${Math.min(endRow, 12)}`, "reviews_top"],
    ["Reviews", `A${Math.max(4, endRow - 8)}:S${endRow}`, "reviews_bottom"],
  ]) {
    const blob = await workbook.render({ sheetName, range, scale: 1, format: "png" });
    await fs.writeFile(path.join(baseDir, `${spec.key}_${suffix}.png`), new Uint8Array(await blob.arrayBuffer()));
  }
}

async function buildWorkbook(spec, records) {
  const workbook = Workbook.create();
  const instructions = workbook.worksheets.add("Instructions");
  const reviews = workbook.worksheets.add("Reviews");
  instructions.showGridLines = false;
  reviews.showGridLines = false;
  const instructionRows = [
    [spec.title, "", "", ""],
    ["Panel-approved E6-deferred procedure, bound to the September 20, 2026 retrieval cutoff. INCLUDE retains a record for further assessment; it is not final inclusion.", "", "", ""],
    ["Reviewer ID", spec.reviewer, "", ""],
    ["Assignment role", spec.role, "", ""],
    ["Deadline", "", "", ""],
    ["Protocol version", contract.protocol_version, "", ""],
    ["Return schema version", contract.return_schema_version, "", ""],
    ["Packet note", spec.note, "", ""],
    ["How to complete the packet", "", "", ""],
    ["1", "Review only the supplied title and abstract. Explicit title evidence may support an individual criterion; do not infer that a promising title establishes every criterion.", "", ""],
    ["2", "Choose YES, NO, or UNCERTAIN for E1-E5 and E7. E6 is fixed as NOT_ASSESSED_AT_THIS_STAGE and must not be changed.", "", ""],
    ["3", "A defensible scientific NO in E1-E5 produces EXCLUDED even when another response is UNCERTAIN. E7=NO or UNCERTAIN expresses unresolved evidence and produces UNCERTAIN when E1-E5 contain no NO.", "", ""],
    ["4", "INCLUDE requires YES for E1-E5 and E7. It means retain for further assessment, not final paper inclusion.", "", ""],
    ["5", "Keep criterion responses, the calculated screening outcome, and the calculated next action separate. Do not replace formulas.", "", ""],
    ["6", "If the supplied title and abstract are mismatched or otherwise conflicted, mark Evidence conflict YES. Evidence reconciliation takes precedence over other routes.", "", ""],
    ["7", "A non-excluded record with no abstract routes to metadata recovery before human full-text assessment. Missing abstracts do not automatically make every criterion unknowable.", "", ""],
    ["8", "Use Targeted second review only when a second title/abstract assessment is prospectively needed. Incomplete or invalid returns are also routed there by validation.", "", ""],
    ["9", "Do not add model outputs, sampling strata, expected answers, or another reviewer's judgments. Save and return a separate copy.", "", ""],
    ["", "", "", ""],
    ["Criterion", "Label", "Question", "Allowed response"],
    ...criteria.map((item) => [item[0], item[1], item[2], item[0] === "E6" ? E6_STATUS : "YES / NO / UNCERTAIN"]),
  ];
  instructions.getRange(`A1:D${instructionRows.length}`).values = instructionRows;
  instructions.mergeCells("A1:D1");
  instructions.mergeCells("A2:D2");
  instructions.mergeCells("B8:D8");
  instructions.mergeCells("A9:D9");
  for (let row = 10; row <= 18; row += 1) instructions.mergeCells(`B${row}:D${row}`);
  instructions.getRange(`A1:D${instructionRows.length}`).format.font = { name: fontFamily, size: 10, color: "#1F1F1F" };
  instructions.getRange("A1:D1").format = { fill: "#1F4E78", font: { name: fontFamily, size: 18, bold: true, color: "#FFFFFF" } };
  instructions.getRange("A2:D2").format = { font: { name: fontFamily, size: 10, italic: true, color: "#595959" }, wrapText: true };
  instructions.getRange("A3:A8").format = { fill: "#D9EAF7", font: { name: fontFamily, size: 10, bold: true, color: "#1F1F1F" } };
  instructions.getRange("B3:B8").format = { fill: "#FFF2CC", font: { name: fontFamily, size: 10 }, wrapText: true, borders: { preset: "all", style: "thin", color: "#C9B458" } };
  instructions.getRange("A9:D9").format = { fill: "#D9EAF7", font: { name: fontFamily, size: 10, bold: true } };
  instructions.getRange("A10:A18").format = { font: { name: fontFamily, size: 10, bold: true, color: "#1F4E78" }, horizontalAlignment: "center" };
  instructions.getRange("B10:D18").format.wrapText = true;
  styleHeader(instructions.getRange("A20:D20"));
  instructions.getRange(`A21:D${instructionRows.length}`).format = { font: { name: fontFamily, size: 10 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  instructions.getRange(`A21:A${instructionRows.length}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 10, bold: true, color: "#1F4E78" }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  instructions.getRange("A1:D1").format.rowHeight = 30;
  instructions.getRange("A2:D2").format.rowHeight = 44;
  instructions.getRange("A8:D8").format.rowHeight = 42;
  instructions.getRange("A10:D18").format.rowHeight = 58;
  instructions.getRange("A12:D12").format.rowHeight = 76;
  instructions.getRange(`A21:D${instructionRows.length}`).format.rowHeight = 64;
  instructions.getRange("A:A").format.columnWidth = 22;
  instructions.getRange("B:B").format.columnWidth = 44;
  instructions.getRange("C:C").format.columnWidth = 88;
  instructions.getRange("D:D").format.columnWidth = 30;
  instructions.freezePanes.freezeRows(2);

  const headers = ["Stable record ID", "Title", "Abstract", "Year", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "Evidence conflict", "Targeted second review", "Screening outcome", "Next action", "Primary exclusion reason", "Confidence", "Uncertainty / missing evidence", "Notes"];
  reviews.getRange("A1:S1").values = [["Approved E6-deferred blinded review records", ...Array(18).fill("")]];
  reviews.getRange("A2:S2").values = [["Enter responses only in blue cells. E6 remains deferred. Missing abstracts route to metadata recovery before human full-text assessment. INCLUDE is not final inclusion.", ...Array(18).fill("")]];
  reviews.getRange("A3:S3").values = [headers];
  const startRow = 4;
  const endRow = startRow + records.length - 1;
  reviews.getRange(`A${startRow}:S${endRow}`).values = records.map((record) => [record.record_id, record.title || "", record.abstract || "", record.publication_year ? Number(record.publication_year) : null, "", "", "", "", "", E6_STATUS, "", "", "", null, null, "", "", "", ""]);
  for (let row = startRow; row <= endRow; row += 1) {
    reviews.getRange(`N${row}`).formulas = [[outcomeFormula(row)]];
    reviews.getRange(`O${row}`).formulas = [[nextActionFormula(row)]];
  }
  reviews.getRange("A1:S1").format = { fill: "#1F4E78", font: { name: fontFamily, size: 16, bold: true, color: "#FFFFFF" } };
  reviews.getRange("A2:S2").format = { font: { name: fontFamily, size: 10, italic: true, color: "#595959" }, wrapText: true };
  styleHeader(reviews.getRange("A3:S3"));
  reviews.getRange(`A${startRow}:D${endRow}`).format = { fill: "#F2F2F2", font: { name: fontFamily, size: 9 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`A${startRow}:A${endRow}`).format.font = { name: fontFamily, size: 9, bold: true, color: "#1F4E78" };
  reviews.getRange(`E${startRow}:I${endRow}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 9 }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`J${startRow}:J${endRow}`).format = { fill: "#E7E6E6", font: { name: fontFamily, size: 8, italic: true, color: "#595959" }, horizontalAlignment: "center", wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`K${startRow}:M${endRow}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 9 }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`N${startRow}:O${endRow}`).format = { fill: "#E2F0D9", font: { name: fontFamily, size: 9 }, horizontalAlignment: "center", wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`P${startRow}:S${endRow}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 9 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`E${startRow}:I${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] } };
  reviews.getRange(`J${startRow}:J${endRow}`).dataValidation = { rule: { type: "list", values: [E6_STATUS] } };
  reviews.getRange(`K${startRow}:K${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] } };
  reviews.getRange(`L${startRow}:M${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO"] } };
  reviews.getRange(`P${startRow}:P${endRow}`).dataValidation = { rule: { type: "list", values: exclusionReasons } };
  reviews.getRange(`Q${startRow}:Q${endRow}`).dataValidation = { rule: { type: "list", values: ["LOW", "MEDIUM", "HIGH"] } };
  reviews.getRange(`D${startRow}:D${endRow}`).format.numberFormat = "0";
  for (const [column, width] of Object.entries({ A: 30, B: 48, C: 100, D: 10, E: 9, F: 9, G: 9, H: 9, I: 9, J: 29, K: 9, L: 18, M: 20, N: 18, O: 25, P: 38, Q: 14, R: 42, S: 42 })) reviews.getRange(`${column}:${column}`).format.columnWidth = width;
  reviews.getRange("A1:S1").format.rowHeight = 28;
  reviews.getRange("A2:S2").format.rowHeight = 44;
  reviews.getRange("A3:S3").format.rowHeight = 52;
  reviews.getRange(`A${startRow}:S${endRow}`).format.rowHeight = 86;
  reviews.freezePanes.freezeRows(3);
  reviews.freezePanes.freezeColumns(4);
  const table = reviews.tables.add(`A3:S${endRow}`, true, `${spec.key.replaceAll("_", "")}ApprovedReviews`);
  table.style = "TableStyleMedium2";

  const inspect = await workbook.inspect({ kind: "table", range: `Reviews!A1:S${Math.min(endRow, 10)}`, include: "values,formulas", tableMaxRows: 10, tableMaxCols: 19, summary: "approved packet layout and formulas" });
  const errors = await workbook.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!", options: { useRegex: true, maxResults: 100 }, summary: "formula error scan" });
  await renderChecks(workbook, spec, endRow, previewDir);
  const outputPath = path.join(outputDir, spec.file);
  const xlsx = await SpreadsheetFile.exportXlsx(workbook);
  await xlsx.save(outputPath);
  const outputBytes = await fs.readFile(outputPath);
  const saved = await SpreadsheetFile.importXlsx(outputBytes);
  await renderChecks(saved, spec, endRow, postSavePreviewDir);
  const savedReviews = saved.worksheets.getItem("Reviews");
  const ids = savedReviews.getRange(`A${startRow}:A${endRow}`).values.flat();
  const e6Values = savedReviews.getRange(`J${startRow}:J${endRow}`).values.flat();
  const formulas = savedReviews.getRange(`N${startRow}:O${endRow}`).formulas;
  if (JSON.stringify(ids) !== JSON.stringify(records.map((record) => record.record_id))) throw new Error(`${spec.key}: record order changed`);
  if (e6Values.some((value) => value !== E6_STATUS)) throw new Error(`${spec.key}: E6 is not explicitly deferred`);
  if (formulas.some((row, index) => !row[0].includes(`E${index + 4}:I${index + 4}`) || row[0].includes(`J${index + 4}`))) throw new Error(`${spec.key}: outcome formula binding failed`);
  const savedErrors = await saved.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!", options: { useRegex: true, maxResults: 100 }, summary: "post-save formula error scan" });
  await fs.writeFile(path.join(internalDir, `${spec.key}.inspect.ndjson`), `${inspect.ndjson}\n${errors.ndjson}\n${savedErrors.ndjson}\n`);
  return { key: spec.key, file: spec.file, path: path.relative(repoRoot, outputPath), records: records.length, ids: records.map((record) => record.record_id), sha256: sha256(outputBytes), formula_error_scan: savedErrors.ndjson };
}

await fs.mkdir(internalDir, { recursive: true });
const samplingPath = path.join(frozenRoot, "internal/sampling_manifest.json");
const samplingBytes = await fs.readFile(samplingPath);
if (sha256(samplingBytes) !== expectedBindings.sampling_manifest_sha256) throw new Error("frozen sampling manifest binding changed");
const sampling = JSON.parse(samplingBytes.toString("utf8"));
if (sampling.e6_binding?.retrieval_cutoff_date !== expectedBindings.retrieval_cutoff) throw new Error("retrieval cutoff binding changed");
const workspaces = new Map();
for (const spec of packetSpecs) workspaces.set(spec.key, await loadWorkspace(spec));
if (workspaces.get("primary_100").records.length !== 100 || workspaces.get("secondary_a_25").records.length !== 25 || workspaces.get("secondary_b_25").records.length !== 25 || workspaces.get("calibration_10").records.length !== 10) throw new Error("frozen packet counts changed");
const primaryIds = new Set(workspaces.get("primary_100").records.map((record) => record.record_id));
for (const key of ["secondary_a_25", "secondary_b_25"]) if (workspaces.get(key).records.some((record) => !primaryIds.has(record.record_id))) throw new Error(`${key} is no longer a subset of the primary 100`);
const calibrationIds = new Set(workspaces.get("calibration_10").records.map((record) => record.record_id));
if ([...calibrationIds].some((id) => primaryIds.has(id))) throw new Error("calibration/evaluation separation changed");

const results = [];
for (const spec of packetSpecs) results.push(await buildWorkbook(spec, workspaces.get(spec.key).records));
const allUniqueRecords = [...workspaces.get("primary_100").records, ...workspaces.get("calibration_10").records];
const metadataQueue = allUniqueRecords.filter((record) => !String(record.abstract || "").trim()).map((record) => ({
  record_id: record.record_id,
  route: "METADATA_RECOVERY",
  status: "PREPARED_NOT_EXECUTED",
  sample: calibrationIds.has(record.record_id) ? "calibration_10" : "evaluation_100",
  title: record.title,
  original_abstract: "",
  original_packet_path: record.packet_path,
  original_packet_sha256: record.packet_sha256,
  rule: "Check versioned registered metadata recovery before considering a human full-text task; preserve original benchmark evidence.",
}));
if (metadataQueue.length !== 37) throw new Error(`expected 37 unique missing-abstract records, found ${metadataQueue.length}`);
await fs.writeFile(path.join(internalDir, "metadata_recovery_queue.json"), `${JSON.stringify({ artifact_class: "PREPARED_METADATA_RECOVERY_QUEUE", status: "NOT_EXECUTED", protocol_version: contract.protocol_version, records: metadataQueue }, null, 2)}\n`);

const row15Id = "canonical:7d25df89fed662b381109a2e";
const row15 = workspaces.get("primary_100").records.find((record) => record.record_id === row15Id);
if (!row15) throw new Error("row-15 record missing from primary assignment");
const rawProviderPath = path.join(repoRoot, "outputs/production/star-external-retrieval-wave-001/execution/SemanticScholar/episodes/episode-004/checkpoint/responses/7a0110fbcafddba5beef7d86edc10b135bacbeed7a75cde2d2812a8cd0e3e8a1.json");
const rawProviderBytes = await fs.readFile(rawProviderPath);
const reconciliationTask = {
  artifact_class: "PREPARED_EVIDENCE_RECONCILIATION_TASK",
  status: "NOT_EXECUTED",
  protocol_version: contract.protocol_version,
  record_id: row15Id,
  assignment_position: workspaces.get("primary_100").records.findIndex((record) => record.record_id === row15Id) + 1,
  route: "EVIDENCE_RECONCILIATION",
  re_review_required_after_reconciliation: true,
  original_evidence: { title: row15.title, abstract: row15.abstract, packet_path: row15.packet_path, packet_sha256: row15.packet_sha256 },
  original_provider_response: { path: path.relative(repoRoot, rawProviderPath), sha256: sha256(rawProviderBytes), preserved_unchanged: true },
  instruction: "Reconcile the authoritative title/abstract pairing without replacing the original benchmark evidence. Any recovered evidence must be separately versioned before re-review.",
};
await fs.writeFile(path.join(internalDir, "row15_evidence_reconciliation_task.json"), `${JSON.stringify(reconciliationTask, null, 2)}\n`);

const manifest = {
  artifact_class: "APPROVED_E6_DEFERRED_REVIEW_PACKET_SET",
  status: "BLANK_REVIEW_MATERIALS_NOT_DISTRIBUTED",
  generated_at_utc: new Date().toISOString(),
  protocol_version: contract.protocol_version,
  return_schema_version: contract.return_schema_version,
  bindings: expectedBindings,
  reviewed_draft: { path: "outputs/e6-deferred-panel-proposal-draft-v2-20260921", preserved_not_regenerated: true, workbook_sha256: "27de0cdef2ad1933ff798a61eaa29c508dfc2946c14060f4d3b39feeba83453d", generation_time_builder_sha256: "843c0a0984c8d41923dc74c0d36e25844b9fb813cd23feb24791988fa45186ce", current_portable_builder_sha256: "fb5edb8d96233428f158bde240766ee3c4e4e6f3a514c436d47d7b06add5f10a" },
  criteria: ASSESSED_CRITERIA,
  e6_status: E6_STATUS,
  rumi_protocol_development_evidence: { independent_review: false, included_in_model_comparison_metrics: false, record_level_judgments_approved_by_panel: false },
  workspaces: Object.fromEntries([...workspaces.entries()].map(([key, value]) => [key, { ledger_path: value.ledgerPath, ledger_sha256: value.ledgerSha256, records: value.records.length }])),
  workbooks: results,
  prepared_tasks: { metadata_recovery_records: metadataQueue.length, metadata_recovery_executed: false, evidence_reconciliation_records: 1, evidence_reconciliation_executed: false },
  prohibited_content_checks: { prior_judgments_included: false, model_outputs_included: false, sampling_strata_included: false, expected_answers_included: false },
};
await fs.writeFile(path.join(internalDir, "approved_packet_manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
await fs.writeFile(path.join(outputDir, "reviewer_instructions.md"), `# Approved E6-deferred title/abstract screening\n\nProtocol version: \`${contract.protocol_version}\`  \nReturn schema version: \`${contract.return_schema_version}\`  \nRetrieval cutoff: \`${contract.retrieval_cutoff}\`\n\nReview only the title and abstract supplied in the workbook. Complete E1-E5 and E7; E6 remains \`${E6_STATUS}\`. A defensible scientific NO in E1-E5 produces EXCLUDED. With no E1-E5 NO, unresolved evidence—including E7 NO or UNCERTAIN—produces UNCERTAIN. Six YES responses produce INCLUDE, meaning retain for further assessment rather than final inclusion.\n\nExplicit title evidence may support an individual judgment, but a promising title does not establish every criterion. A non-excluded missing-abstract record routes to metadata recovery before human full-text assessment. Mark evidence conflicts separately; reconciliation takes precedence and is followed by re-review. Keep responses, aggregate outcome, and next action distinct. Do not add model outputs, sampling strata, expected answers, or prior judgments.\n\nThe optional secondary packets are prospectively selected assignments only; reviewer identities and deadlines remain unassigned. The calibration packet is separate from evaluation metrics.\n`, "utf8");
process.stdout.write(`${JSON.stringify({ outputDir, protocolVersion: contract.protocol_version, returnSchemaVersion: contract.return_schema_version, workbooks: results.map(({ file, records, sha256: digest }) => ({ file, records, sha256: digest })), metadataRecoveryRecords: metadataQueue.length, reconciliationRecord: row15Id }, null, 2)}\n`);
