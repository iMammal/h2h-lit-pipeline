import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

if (!process.argv[2]) {
  throw new Error("usage: node build_benchmark_review_workbooks.mjs <source-dir> [output-dir]");
}
const sourceDir = path.resolve(process.argv[2]);
const outputDir = path.resolve(process.argv[3] || process.argv[2]);

const fontFamily = "Arial";
const criteria = [
  ["E1", "Life-science application", "Is the reported system applied to analysis, understanding, monitoring, decision-making, discovery, or scientific practice in an in-scope life-science, biomedical, clinical, health, neuroscience, ecology, or related domain?"],
  ["E2", "Relational, derived-structure, or multiscale relevance", "Does the system analyze explicit relationships or relationships derived from spatial, temporal, multivariate, image-derived, lineage, similarity, or other multiscale data?"],
  ["E3", "Interactive visual analytics", "Does a human use an interactive or analytically meaningful visual representation to inspect, understand, validate, steer, compare, interpret, or act on results?"],
  ["E4", "Substantive computational assistance", "Does a nontrivial computational mechanism operate inside the interactive visual-analytics workflow, beyond ordinary rendering or literal direct manipulation?"],
  ["E5", "Human analytic relationship", "Does a human meaningfully inspect, direct, validate, interpret, collaborate with, supervise, correct, or make decisions from the assisted process?"],
  ["E6", "Administrative scope", "Is the record available by the September 20, 2026 retrieval cutoff, supported by English full text, and an eligible research-paper type under the frozen protocol?"],
  ["E7", "Evidence sufficiency", "Does the reviewed evidence identify a candidate system and support a defensible determination for E1-E6?"],
];
const exclusionReasons = [
  "EX_AFTER_RETRIEVAL_END_DATE",
  "EX_NON_ENGLISH_FULL_TEXT",
  "EX_INELIGIBLE_DOCUMENT_TYPE",
  "EX_NO_LIFE_SCIENCE_APPLICATION",
  "EX_NO_RELATIONAL_OR_MULTISCALE_RELEVANCE",
  "EX_NO_INTERACTIVE_VISUAL_ANALYTICS",
  "EX_NO_QUALIFYING_ASSISTANCE",
  "EX_NO_HUMAN_ANALYTIC_RELATIONSHIP",
  "EX_INSUFFICIENT_EVIDENCE_AFTER_ESCALATION",
  "EX_OTHER_PROTOCOL_REASON",
];

const packetSpecs = [
  { key: "primary_100", file: "primary_morris_100_records.xlsx", title: "Primary title-and-abstract review", reviewer: "Morris", role: "Primary", note: "Primary 100-record pilot packet." },
  { key: "secondary_a_25", file: "optional_secondary_a_25_records.xlsx", title: "Optional secondary title-and-abstract review A", reviewer: "", role: "Optional secondary", note: "Reviewer identity and deadline are unassigned. This packet does not represent a completed independent review." },
  { key: "secondary_b_25", file: "optional_secondary_b_25_records.xlsx", title: "Optional secondary title-and-abstract review B", reviewer: "", role: "Optional secondary", note: "Reviewer identity and deadline are unassigned. This packet does not represent a completed independent review." },
  { key: "calibration_10", file: "calibration_10_records.xlsx", title: "Title-and-abstract calibration", reviewer: "", role: "Calibration", note: "Separate calibration material. Do not include these records in evaluation metrics." },
];

async function loadPackets(key) {
  const dir = path.join(sourceDir, "review_workspaces", key, "packets");
  const names = (await fs.readdir(dir)).filter((name) => name.endsWith(".json"));
  const packets = [];
  for (const name of names) packets.push(JSON.parse(await fs.readFile(path.join(dir, name), "utf8")));
  packets.sort((a, b) => a.record.sample_order - b.record.sample_order);
  return packets.map((packet) => packet.record);
}

function styleHeader(range, fill = "#4472C4") {
  range.format = {
    fill,
    font: { name: fontFamily, size: 10, bold: true, color: "#FFFFFF" },
    wrapText: true,
    verticalAlignment: "center",
    horizontalAlignment: "center",
    borders: { preset: "all", style: "thin", color: "#D9E2F3" },
  };
}

async function buildWorkbook(spec, records) {
  const workbook = Workbook.create();
  const instructions = workbook.worksheets.add("Instructions");
  const reviews = workbook.worksheets.add("Reviews");
  instructions.showGridLines = false;
  reviews.showGridLines = false;

  const instructionRows = [
    [spec.title, "", "", ""],
    ["Title/abstract pilot bound to the September 20, 2026 retrieval cutoff.", "", "", ""],
    ["Reviewer ID", spec.reviewer, "", ""],
    ["Assignment role", spec.role, "", ""],
    ["Deadline", "", "", ""],
    ["Packet note", spec.note, "", ""],
    ["How to complete the packet", "", "", ""],
    ["1", "Review only the title and abstract shown in the Reviews sheet. A missing abstract does not make every criterion unknowable: use explicit title evidence where it supports an individual judgment.", "", ""],
    ["2", "Choose YES, NO, or UNCERTAIN for every E1-E7 field.", "", ""],
    ["3", "The eligibility status is calculated from E1-E7. Any defensible NO produces EXCLUDED, even when another criterion is UNCERTAIN. Do not replace the formula.", "", ""],
    ["4", "If there is no NO and at least one criterion is UNCERTAIN, mark full-text escalation YES and describe the missing evidence. The record advances to further/full-text review.", "", ""],
    ["5", "An excluded record does not require escalation only because E6 or another criterion is unknown. If you still escalate an excluded record, mark YES and enter a specific reason in Excluded-record escalation rationale.", "", ""],
    ["6", "Title/abstract screening does not establish final inclusion. Do not add model outputs, sampling strata, expected answers, or another reviewer's judgment.", "", ""],
    ["7", "Keep criterion responses, the calculated aggregate outcome, and escalation separate. Describe uncertainty or missing evidence in its own field.", "", ""],
    ["8", "Save and return your own copy. Do not merge another reviewer's judgments into it.", "", ""],
    ["", "", "", ""],
    ["Criterion", "Label", "Question", "Allowed response"],
    ...criteria.map((item) => [item[0], item[1], item[2], "YES / NO / UNCERTAIN"]),
  ];
  instructions.getRange(`A1:D${instructionRows.length}`).values = instructionRows;
  instructions.getRange(`A1:D${instructionRows.length}`).format.font = { name: fontFamily, size: 10, color: "#1F1F1F" };
  instructions.getRange("A1:D1").format = { fill: "#1F4E78", font: { name: fontFamily, size: 18, bold: true, color: "#FFFFFF" } };
  instructions.getRange("A2:D2").format = { font: { name: fontFamily, size: 10, italic: true, color: "#595959" } };
  instructions.getRange("A3:A6").format = { fill: "#D9EAF7", font: { name: fontFamily, size: 10, bold: true, color: "#1F1F1F" } };
  instructions.getRange("B3:B6").format = { fill: "#FFF2CC", font: { name: fontFamily, size: 10 }, wrapText: true, borders: { preset: "all", style: "thin", color: "#C9B458" } };
  instructions.getRange("A7:D7").format = { fill: "#D9EAF7", font: { name: fontFamily, size: 10, bold: true } };
  instructions.getRange("A8:A15").format = { font: { name: fontFamily, size: 10, bold: true, color: "#1F4E78" }, horizontalAlignment: "center" };
  instructions.getRange("B8:D15").format.wrapText = true;
  styleHeader(instructions.getRange("A17:D17"));
  instructions.getRange(`A18:D${instructionRows.length}`).format = { font: { name: fontFamily, size: 10 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  instructions.getRange(`A18:A${instructionRows.length}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 10, bold: true, color: "#1F4E78" }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  instructions.getRange("A1:D1").format.rowHeight = 28;
  instructions.getRange("A2:D2").format.rowHeight = 24;
  instructions.getRange("A6:D6").format.rowHeight = 34;
  instructions.getRange("A8:D15").format.rowHeight = 48;
  instructions.getRange("A8:D8").format.rowHeight = 70;
  instructions.getRange("A10:D12").format.rowHeight = 70;
  instructions.getRange(`A18:D${instructionRows.length}`).format.rowHeight = 56;
  instructions.getRange("A:A").format.columnWidth = 18;
  instructions.getRange("B:B").format.columnWidth = 38;
  instructions.getRange("C:C").format.columnWidth = 88;
  instructions.getRange("D:D").format.columnWidth = 22;
  instructions.freezePanes.freezeRows(2);

  const headers = ["Stable record ID", "Title", "Abstract", "Year", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "Eligibility status", "Primary exclusion reason", "Full-text escalation", "Excluded-record escalation rationale", "Confidence", "Uncertainty / missing evidence", "Notes"];
  reviews.getRange("A1:R1").values = [["Blinded review records", ...Array(17).fill("")]];
  reviews.getRange("A2:R2").values = [["Enter responses only in blue cells. Any NO makes the calculated outcome EXCLUDED. If there is no NO, uncertain cases advance to further/full-text review. Title/abstract screening does not establish final inclusion.", ...Array(17).fill("")]];
  reviews.getRange("A3:R3").values = [headers];
  const startRow = 4;
  const endRow = startRow + records.length - 1;
  const rows = records.map((record) => [record.record_id, record.title || "", record.abstract || "", record.publication_year ? Number(record.publication_year) : null, ...Array(14).fill(null)]);
  reviews.getRange(`A${startRow}:R${endRow}`).values = rows;
  for (let row = startRow; row <= endRow; row += 1) {
    reviews.getRange(`L${row}`).formulas = [[`=IF(COUNTIF(E${row}:K${row},"NO")>0,"EXCLUDED",IF(COUNTIF(E${row}:K${row},"UNCERTAIN")>0,"UNCERTAIN",IF(COUNTIF(E${row}:K${row},"YES")=7,"ELIGIBLE","")))`]];
  }
  reviews.getRange("A1:R1").format = { fill: "#1F4E78", font: { name: fontFamily, size: 16, bold: true, color: "#FFFFFF" } };
  reviews.getRange("A2:R2").format = { font: { name: fontFamily, size: 10, italic: true, color: "#595959" }, wrapText: true };
  styleHeader(reviews.getRange("A3:R3"));
  reviews.getRange(`A${startRow}:D${endRow}`).format = { fill: "#F2F2F2", font: { name: fontFamily, size: 9 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`A${startRow}:A${endRow}`).format.font = { name: fontFamily, size: 9, bold: true, color: "#1F4E78" };
  reviews.getRange(`E${startRow}:K${endRow}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 9 }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`L${startRow}:L${endRow}`).format = { fill: "#E2F0D9", font: { name: fontFamily, size: 9 }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`M${startRow}:R${endRow}`).format = { fill: "#D9EAF7", font: { name: fontFamily, size: 9 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
  reviews.getRange(`E${startRow}:K${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] } };
  reviews.getRange(`M${startRow}:M${endRow}`).dataValidation = { rule: { type: "list", values: exclusionReasons } };
  reviews.getRange(`N${startRow}:N${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO"] } };
  reviews.getRange(`P${startRow}:P${endRow}`).dataValidation = { rule: { type: "list", values: ["LOW", "MEDIUM", "HIGH"] } };
  reviews.getRange(`D${startRow}:D${endRow}`).format.numberFormat = "0";
  reviews.getRange("A:A").format.columnWidth = 30;
  reviews.getRange("B:B").format.columnWidth = 48;
  reviews.getRange("C:C").format.columnWidth = 100;
  reviews.getRange("D:D").format.columnWidth = 10;
  reviews.getRange("E:K").format.columnWidth = 10;
  reviews.getRange("L:L").format.columnWidth = 18;
  reviews.getRange("M:M").format.columnWidth = 38;
  reviews.getRange("N:N").format.columnWidth = 18;
  reviews.getRange("O:O").format.columnWidth = 42;
  reviews.getRange("P:P").format.columnWidth = 18;
  reviews.getRange("Q:R").format.columnWidth = 42;
  reviews.getRange("A1:R1").format.rowHeight = 26;
  reviews.getRange("A2:R2").format.rowHeight = 42;
  reviews.getRange("A3:R3").format.rowHeight = 48;
  reviews.getRange(`A${startRow}:R${endRow}`).format.rowHeight = 84;
  reviews.freezePanes.freezeRows(3);
  reviews.freezePanes.freezeColumns(4);
  const table = reviews.tables.add(`A3:R${endRow}`, true, `${spec.key.replaceAll("_", "")}Reviews`);
  table.style = "TableStyleMedium2";

  const keyInspect = await workbook.inspect({ kind: "table", range: `Reviews!A1:R${Math.min(endRow, 10)}`, include: "values,formulas", tableMaxRows: 10, tableMaxCols: 18 });
  const errors = await workbook.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!", options: { useRegex: true, maxResults: 100 }, summary: "final formula error scan" });
  const previewsDir = path.join(outputDir, "internal", "workbook_previews");
  await fs.mkdir(previewsDir, { recursive: true });
  for (const [sheetName, range] of [["Instructions", `A1:D${instructionRows.length}`], ["Reviews", `A1:R${Math.min(endRow, 12)}`]]) {
    const blob = await workbook.render({ sheetName, range, scale: 1, format: "png" });
    await fs.writeFile(path.join(previewsDir, `${spec.key}_${sheetName.toLowerCase()}.png`), new Uint8Array(await blob.arrayBuffer()));
  }
  const xlsx = await SpreadsheetFile.exportXlsx(workbook);
  await xlsx.save(path.join(outputDir, spec.file));
  await fs.writeFile(path.join(outputDir, "internal", `${spec.key}.inspect.ndjson`), `${keyInspect.ndjson}\n${errors.ndjson}\n`);
  return { file: spec.file, records: records.length, errorScan: errors.ndjson };
}

await fs.mkdir(outputDir, { recursive: true });
const results = [];
for (const spec of packetSpecs) results.push(await buildWorkbook(spec, await loadPackets(spec.key)));
await fs.writeFile(path.join(outputDir, "internal", "workbook_build_result.json"), JSON.stringify(results, null, 2) + "\n");
await fs.writeFile(path.join(outputDir, "reviewer_instructions.md"), `# Blinded title-and-abstract pilot review

This pilot is bound to the September 20, 2026 retrieval cutoff. Title/abstract screening does not establish final paper eligibility or final inclusion.

## Review procedure

1. Review only the title and abstract shown in the assigned workbook. A missing abstract does not make every criterion unknowable; explicit title evidence may support individual judgments.
2. Choose \`YES\`, \`NO\`, or \`UNCERTAIN\` for every E1-E7 field. Keep these criterion responses distinct from the calculated aggregate outcome and from escalation.
3. Any defensible \`NO\` produces \`EXCLUDED\`, even if another criterion is \`UNCERTAIN\`.
4. If there is no \`NO\` and one or more criteria are \`UNCERTAIN\`, mark full-text escalation \`YES\` and describe the missing evidence. The record advances to further/full-text review.
5. Unknown E6 administrative evidence alone does not require retrieval when another criterion already supports exclusion.
6. Escalation of an excluded record is discretionary. If used, mark it \`YES\` and provide a specific reason in the separate Excluded-record escalation rationale field.
7. For an excluded record, choose the primary exclusion reason. Do not replace eligibility formulas.
8. Do not add model outputs, sampling-stratum information, expected answers, or another reviewer's judgment.
9. Save and return a separate copy. Do not merge reviews from different people.

## Packets

- \`primary_morris_100_records.xlsx\`: primary 100-record packet for Morris. No deadline is assigned.
- \`optional_secondary_a_25_records.xlsx\`: prospectively selected optional secondary subset. Reviewer identity and deadline are unassigned.
- \`optional_secondary_b_25_records.xlsx\`: a second prospectively selected, nonoverlapping optional secondary subset. Reviewer identity and deadline are unassigned.
- \`calibration_10_records.xlsx\`: separate calibration material. Do not include these records in evaluation metrics.

The optional packets are prepared assignments only. They do not represent completed independent reviews.
`, "utf8");
for (const result of results) process.stdout.write(`${JSON.stringify(result)}\n`);
