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

import {
  DEFERRED_E6_VALUE,
  draftOutcomeFormula,
  recomputeDraftOutcome,
  validateDeferredE6,
} from "./e6_deferred_draft_formula.mjs";

if (!process.argv[2]) {
  throw new Error(
    "usage: node build_e6_deferred_panel_draft.mjs <submitted-workbook> [output-dir]",
  );
}
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sourcePath = path.resolve(process.argv[2]);
const outputDir = path.resolve(
  process.argv[3] || path.join(repoRoot, "outputs/e6-deferred-panel-proposal-draft-v2-20260921"),
);
const outputWorkbook = path.join(
  outputDir,
  "primary_100_records_E6deferred_Rumi_analysis_DRAFT_v2.xlsx",
);
const evidenceDir = path.join(outputDir, "submitted_evidence");
const evidenceCopy = path.join(evidenceDir, path.basename(sourcePath));
const internalDir = path.join(outputDir, "internal");
const previewDir = path.join(internalDir, "workbook_previews");
const samplingManifestPath = path.join(
  repoRoot,
  "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json",
);

const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const sourceBytes = await fs.readFile(sourcePath);
const sourceSha256 = sha256(sourceBytes);
const samplingBytes = await fs.readFile(samplingManifestPath);
const samplingSha256 = sha256(samplingBytes);
const sampling = JSON.parse(samplingBytes.toString("utf8"));
const selectedById = new Map(sampling.selected_records.map((item) => [item.canonical_id, item]));

await fs.mkdir(evidenceDir, { recursive: true });
await fs.mkdir(previewDir, { recursive: true });
await fs.copyFile(sourcePath, evidenceCopy);
if (sha256(await fs.readFile(evidenceCopy)) !== sourceSha256) {
  throw new Error("byte-preserving evidence copy hash mismatch");
}

const workbook = await SpreadsheetFile.importXlsx(sourceBytes);
const instructions = workbook.worksheets.getItem("Instructions");
const reviews = workbook.worksheets.getItem("Reviews");
const beforeProtected = reviews.getRange("A4:K103").values.map((row) => [...row]);
const beforeAnnotations = reviews.getRange("M4:R103").values.map((row) => [...row]);

instructions.getRange("A1").values = [["DRAFT panel-review proposal — E6-deferred re-analysis"]];
instructions.getRange("A2").values = [[
  "Draft protocol proposal bound to the September 20, 2026 retrieval cutoff. Not a finalized production protocol. E6 is deferred to retrieval-stage administrative verification and is excluded from this title/abstract aggregate.",
]];
instructions.getRange("B4").values = [["Draft protocol re-analysis (not independent)"]];
instructions.getRange("B6").values = [[
  "Primary 100-record re-analysis by Rumi. This analysis had access to prior judgments in the Morris-annotated workbook; it is not an independent human benchmark result. The submitted workbook is preserved separately as calibration/proposal evidence.",
]];
instructions.getRange("A2:D2").format.rowHeight = 38;
instructions.getRange("A6:D6").format.rowHeight = 70;

reviews.getRange("A1").values = [["DRAFT panel-review proposal — blinded records"]];
reviews.getRange("A2").values = [[
  "Rumi re-analysis with access to prior judgments; not an independent review and not a finalized production protocol. E6 remains deferred. INCLUDE means retain for further assessment, not final inclusion.",
]];
reviews.getRange("A2:R2").format.rowHeight = 48;
reviews.getRange("E4:I103").dataValidation = {
  rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] },
};
reviews.getRange("J4:J103").dataValidation = {
  rule: { type: "list", values: [DEFERRED_E6_VALUE] },
};
reviews.getRange("K4:K103").dataValidation = {
  rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] },
};
for (let row = 4; row <= 103; row += 1) {
  reviews.getRange(`L${row}`).formulas = [[draftOutcomeFormula(row)]];
}

const afterProtected = reviews.getRange("A4:K103").values;
const afterAnnotations = reviews.getRange("M4:R103").values;
if (JSON.stringify(beforeProtected) !== JSON.stringify(afterProtected)) {
  throw new Error("IDs, evidence, or criterion judgments changed during draft repair");
}
if (JSON.stringify(beforeAnnotations) !== JSON.stringify(afterAnnotations)) {
  throw new Error("reviewer annotations changed during draft repair");
}

const rows = [];
for (let offset = 0; offset < afterProtected.length; offset += 1) {
  const excelRow = offset + 4;
  const row = afterProtected[offset];
  const id = row[0];
  const decisions = { E1: row[4], E2: row[5], E3: row[6], E4: row[7], E5: row[8], E7: row[10] };
  validateDeferredE6(row[9]);
  const outcome = recomputeDraftOutcome(decisions);
  const selected = selectedById.get(id);
  if (!selected) throw new Error(`record is absent from frozen sampling manifest: ${id}`);
  rows.push({
    excel_row: excelRow,
    canonical_id: id,
    title: row[1],
    abstract_missing: String(row[2] ?? "").trim() === "",
    selection_group: selected.selection_group,
    challenge_stratum: selected.challenge_stratum,
    decisions,
    e6: row[9],
    outcome,
  });
}

const groups = ["TOTAL", "uniform_random", "challenge"];
const outcomes = ["INCLUDE", "UNCERTAIN", "EXCLUDED"];
const emptyCounts = () => Object.fromEntries(outcomes.map((outcome) => [outcome, 0]));
const counts = Object.fromEntries(groups.map((group) => [group, emptyCounts()]));
const missingAbstractCounts = Object.fromEntries(groups.map((group) => [group, emptyCounts()]));
for (const row of rows) {
  counts.TOTAL[row.outcome] += 1;
  counts[row.selection_group][row.outcome] += 1;
  if (row.abstract_missing) {
    missingAbstractCounts.TOTAL[row.outcome] += 1;
    missingAbstractCounts[row.selection_group][row.outcome] += 1;
  }
}

const formulaInspect = await workbook.inspect({
  kind: "table",
  range: "Reviews!E3:L12",
  include: "values,formulas",
  tableMaxRows: 12,
  tableMaxCols: 8,
  summary: "corrected E6-deferred formulas",
});
const errorInspect = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 100 },
  summary: "formula error scan",
});
for (const [sheetName, range, fileName] of [
  ["Instructions", "A1:D24", "instructions.png"],
  ["Reviews", "A1:R18", "reviews_rows_1_18.png"],
  ["Reviews", "A55:R66", "reviews_rows_55_66.png"],
]) {
  const blob = await workbook.render({ sheetName, range, scale: 1, format: "png" });
  await fs.writeFile(
    path.join(previewDir, fileName),
    new Uint8Array(await blob.arrayBuffer()),
  );
}

const exported = await SpreadsheetFile.exportXlsx(workbook);
await exported.save(outputWorkbook);
const outputSha256 = sha256(await fs.readFile(outputWorkbook));

const savedWorkbook = await SpreadsheetFile.importXlsx(await fs.readFile(outputWorkbook));
const savedReviews = savedWorkbook.worksheets.getItem("Reviews");
const savedValues = savedReviews.getRange("A4:R103").values;
const savedFormulas = savedReviews.getRange("L4:L103").formulas;
let cacheMismatchCount = 0;
let malformedFormulaCount = 0;
let deferredE6Count = 0;
for (let offset = 0; offset < savedValues.length; offset += 1) {
  const excelRow = offset + 4;
  const row = savedValues[offset];
  const expected = recomputeDraftOutcome({
    E1: row[4], E2: row[5], E3: row[6], E4: row[7], E5: row[8], E7: row[10],
  });
  if (row[11] !== expected) cacheMismatchCount += 1;
  if (
    !savedFormulas[offset][0].includes(`E${excelRow}:I${excelRow}`) ||
    savedFormulas[offset][0].includes(`J${excelRow}`)
  ) malformedFormulaCount += 1;
  if (row[9] === DEFERRED_E6_VALUE) deferredE6Count += 1;
}
if (cacheMismatchCount || malformedFormulaCount || deferredE6Count !== 100) {
  throw new Error("post-save formula/cache/E6 verification failed");
}
const savedErrorInspect = await savedWorkbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 100 },
  summary: "post-save formula error scan",
});
const { stdout: reviewsXml } = await execFile(
  "unzip",
  ["-p", outputWorkbook, "xl/worksheets/sheet2.xml"],
  { maxBuffer: 4 * 1024 * 1024 },
);
const dropdownChecks = {
  e1_e5: reviewsXml.includes('sqref="E4:I103"') && reviewsXml.includes('"YES,NO,UNCERTAIN"'),
  e6_deferred: reviewsXml.includes('sqref="J4:J103"') && reviewsXml.includes('"Not assessed at this stage"'),
  e7: reviewsXml.includes('sqref="K4:K103"') && reviewsXml.includes('"YES,NO,UNCERTAIN"'),
  exclusion_reason: reviewsXml.includes('sqref="M4:M103"'),
  escalation: reviewsXml.includes('sqref="N4:N103"') && reviewsXml.includes('"YES,NO"'),
  confidence: reviewsXml.includes('sqref="P4:P103"') && reviewsXml.includes('"LOW,MEDIUM,HIGH"'),
};
if (Object.values(dropdownChecks).includes(false)) {
  throw new Error("post-save dropdown verification failed");
}
const postSavePreviewDir = path.join(internalDir, "post_save_visual_verification");
await fs.mkdir(postSavePreviewDir, { recursive: true });
for (const [sheetName, range, fileName] of [
  ["Instructions", "A1:D24", "instructions.png"],
  ["Reviews", "A1:R18", "reviews_rows_1_18.png"],
  ["Reviews", "A55:R66", "reviews_rows_55_66.png"],
]) {
  const blob = await savedWorkbook.render({ sheetName, range, scale: 1, format: "png" });
  await fs.writeFile(
    path.join(postSavePreviewDir, fileName),
    new Uint8Array(await blob.arrayBuffer()),
  );
}

const includeWithoutAbstract = rows.filter(
  (row) => row.outcome === "INCLUDE" && row.abstract_missing,
);
const e7Distribution = rows.reduce((accumulator, row) => {
  accumulator[row.decisions.E7] = (accumulator[row.decisions.E7] || 0) + 1;
  return accumulator;
}, {});
const e7MechanicalInconsistencies = rows.filter(
  (row) =>
    (row.decisions.E7 === "YES" &&
      [row.decisions.E1, row.decisions.E2, row.decisions.E3, row.decisions.E4, row.decisions.E5].includes("UNCERTAIN")) ||
    row.decisions.E7 === "NO",
);

const validation = {
  status: "validated_draft_proposal",
  source: { path: sourcePath, sha256: sourceSha256, evidence_copy: evidenceCopy },
  output: { path: outputWorkbook, sha256: outputSha256 },
  preserved: {
    record_count: rows.length,
    ids_evidence_judgments_and_order: true,
    reviewer_annotations: true,
    reviewer_identity: instructions.getRange("B3").values[0][0],
  },
  aggregate_definition: {
    included_criteria: ["E1", "E2", "E3", "E4", "E5", "E7"],
    excluded_criterion: "E6",
    e6_value: DEFERRED_E6_VALUE,
    no_precedence: true,
    include_meaning: "retain for further assessment; not final inclusion",
  },
  recomputed_counts: counts,
  missing_abstract_counts: missingAbstractCounts,
  include_without_abstract: includeWithoutAbstract,
  e7: { distribution: e7Distribution, mechanical_inconsistencies: e7MechanicalInconsistencies },
  frozen_sampling_manifest: { path: samplingManifestPath, sha256: samplingSha256 },
  return_schema: {
    production_compatible: false,
    reason: "Production requires E1-E7 tri-state responses and ELIGIBLE; this proposal defers E6 and uses INCLUDE.",
    action: "Do not import; panel approval and a separate versioned protocol contract would be required.",
  },
  formula_inspection: formulaInspect.ndjson,
  formula_error_scan: errorInspect.ndjson,
  post_save_verification: {
    cache_vs_independent_recomputation_mismatches: cacheMismatchCount,
    malformed_formula_count: malformedFormulaCount,
    deferred_e6_count: deferredE6Count,
    dropdown_checks: dropdownChecks,
    formula_error_scan: savedErrorInspect.ndjson,
    visual_previews: postSavePreviewDir,
  },
};
await fs.writeFile(
  path.join(internalDir, "validation_report.json"),
  `${JSON.stringify(validation, null, 2)}\n`,
  "utf8",
);

const row15 = rows.find((row) => row.excel_row === 15);
const report = `# E6-deferred draft panel-review report

Status: **draft protocol re-analysis for panel review**. This is not a finalized production protocol and not an independent human benchmark result. Rumi's re-analysis had access to prior judgments in the Morris-annotated workbook. It must not be counted as an independent review or imported into production screening.

## Submission preservation

- Submitted workbook: \`${sourcePath}\`
- SHA-256: \`${sourceSha256}\`
- Byte-identical evidence copy: \`${evidenceCopy}\`
- Reviewer identity retained: **Rumi**

## Corrected aggregate

The draft aggregate uses E1-E5 and E7 only. E6 remains exactly **${DEFERRED_E6_VALUE}** and is not treated as YES. A defensible NO produces EXCLUDED; otherwise any UNCERTAIN produces UNCERTAIN; six YES responses produce INCLUDE. Here, INCLUDE means retained for further assessment, not final inclusion.

| Frozen sample group | INCLUDE | UNCERTAIN | EXCLUDED | Total |
|---|---:|---:|---:|---:|
| Uniform random | ${counts.uniform_random.INCLUDE} | ${counts.uniform_random.UNCERTAIN} | ${counts.uniform_random.EXCLUDED} | 70 |
| Challenge | ${counts.challenge.INCLUDE} | ${counts.challenge.UNCERTAIN} | ${counts.challenge.EXCLUDED} | 30 |
| **Total** | **${counts.TOTAL.INCLUDE}** | **${counts.TOTAL.UNCERTAIN}** | **${counts.TOTAL.EXCLUDED}** | **100** |

## Missing abstracts by outcome and frozen sample group

| Frozen sample group | INCLUDE | UNCERTAIN | EXCLUDED | Total missing abstracts |
|---|---:|---:|---:|---:|
| Uniform random | ${missingAbstractCounts.uniform_random.INCLUDE} | ${missingAbstractCounts.uniform_random.UNCERTAIN} | ${missingAbstractCounts.uniform_random.EXCLUDED} | ${Object.values(missingAbstractCounts.uniform_random).reduce((a, b) => a + b, 0)} |
| Challenge | ${missingAbstractCounts.challenge.INCLUDE} | ${missingAbstractCounts.challenge.UNCERTAIN} | ${missingAbstractCounts.challenge.EXCLUDED} | ${Object.values(missingAbstractCounts.challenge).reduce((a, b) => a + b, 0)} |
| **Total** | **${missingAbstractCounts.TOTAL.INCLUDE}** | **${missingAbstractCounts.TOTAL.UNCERTAIN}** | **${missingAbstractCounts.TOTAL.EXCLUDED}** | **${Object.values(missingAbstractCounts.TOTAL).reduce((a, b) => a + b, 0)}** |

The 31 missing-abstract UNCERTAIN records are metadata-recovery candidates first: check already-registered alternate source metadata or a documented bibliographic-recovery route before deciding that scientific full-text review is necessary. The 16 UNCERTAIN records with abstracts are stronger candidates for genuine scientific/evidence uncertainty. A missing abstract alone does not force every criterion to UNCERTAIN and does not automatically require human full-text review; title evidence may support individual judgments. No new metadata was retrieved for this re-analysis.

## Title-only INCLUDE calls requiring panel guidance

The following judgments were preserved, not changed:

${includeWithoutAbstract.map((row) => `- Row ${row.excel_row}, \`${row.canonical_id}\`: **${row.title}**`).join("\n")}

Panel questions: Is the title alone sufficient to support YES for each of E1-E5 and E7? If not, should the record be routed first to registered-metadata recovery, then to scientific full-text review only if the missing evidence remains material? What minimum evidence should E7 require when the abstract is absent?

## Row-15 evidence mismatch

Row 15 (\`${row15.canonical_id}\`) pairs the melanoma/autoimmune-disease title with an abstract about asymptomatic COVID-19. The mismatch first appears in the retained source lineage in the raw Semantic Scholar response:

- \`outputs/production/star-external-retrieval-wave-001/execution/SemanticScholar/episodes/episode-004/checkpoint/responses/7a0110fbcafddba5beef7d86edc10b135bacbeed7a75cde2d2812a8cd0e3e8a1.json\`
- Semantic Scholar paper ID: \`0d3033f6d95b959917c872db86378b4fbd54bfc5\`; DOI: \`10.7326/L21-0441\`; HTTP status 200.

The same pairing then appears in the registered Semantic Scholar checkpoint, the global merge, the frozen sampling manifest, the blinded CSV, the frozen packet, and the submitted workbook. The draft does not replace the evidence, move annotations, or modify the registered corpus. The row-15 judgment is flagged as requiring evidence reconciliation and re-review after the authoritative title/abstract pairing is resolved.

Trace locations, in lineage order:

1. Raw source response (first retained occurrence): \`outputs/production/star-external-retrieval-wave-001/execution/SemanticScholar/episodes/episode-004/checkpoint/responses/7a0110fbcafddba5beef7d86edc10b135bacbeed7a75cde2d2812a8cd0e3e8a1.json\`.
2. Registered source checkpoint: \`outputs/production/star-external-retrieval-wave-001/execution/SemanticScholar/episodes/episode-004/checkpoint/review_dataset.json\`.
3. Registered global merge: \`outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/review_dataset.json\`.
4. Frozen sampling manifest: \`outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json\`.
5. Frozen blinded source: \`outputs/title-abstract-benchmark-v1-20260920/blinded_sources/primary_100.csv\`.
6. Frozen packet: \`outputs/title-abstract-benchmark-v1-20260920/review_workspaces/primary_100/packets/record-29d51744c83e236a312c.json\`.

## E7 interpretation

E7 responses are 24 YES, 76 UNCERTAIN, and 0 NO. No mechanical contradiction was found: no E7 YES row contains an E1-E5 UNCERTAIN response, and no title/abstract row uses E7 NO. The two title-only INCLUDE records nevertheless raise a substantive evidence-sufficiency question. The panel should confirm whether draft E7 evaluates defensibility for E1-E5 only while E6 is deferred, and what evidence threshold applies when no abstract is present.

## Return-schema compatibility

This workbook is intentionally **not compatible with the current production return schema**. Production requires complete E1-E7 tri-state responses and the aggregate label ELIGIBLE; this proposal uses a deferred E6 token and INCLUDE. It must not be imported. A separate, versioned protocol/return contract would be needed only after panel approval; none was created here.

## Panel decisions still needed

1. Approve, revise, or reject the proposed E6-deferred aggregate and the meaning of INCLUDE.
2. Set the evidence-sufficiency threshold for E7 when abstracts are missing, including the two preserved title-only INCLUDE calls.
3. Approve a staged route for missing abstracts: registered-metadata recovery first, then scientific full-text review only when material uncertainty remains.
4. Reconcile row 15 against authoritative evidence and decide whether its preserved judgment must be repeated.
5. Decide whether and how this proposal would receive its own versioned return schema; do not modify production import logic until then.
`;
await fs.writeFile(path.join(outputDir, "panel_review_report.md"), report, "utf8");

const manifest = {
  artifact_class: "draft_panel_review_proposal",
  status: "not_finalized_not_for_production_import",
  source_submission_sha256: sourceSha256,
  corrected_workbook_sha256: outputSha256,
  sampling_manifest_sha256: samplingSha256,
  reviewer_identity: "Rumi",
  prior_judgments_accessible: true,
  independent_review: false,
  redraw_or_restratification: false,
  bibliographic_enrichment: false,
  registered_corpus_modified: false,
  paths: {
    corrected_workbook: outputWorkbook,
    source_evidence_copy: evidenceCopy,
    panel_report: path.join(outputDir, "panel_review_report.md"),
    validation_report: path.join(internalDir, "validation_report.json"),
  },
};
await fs.writeFile(path.join(outputDir, "draft_manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);

process.stdout.write(`${JSON.stringify({ outputWorkbook, sourceSha256, outputSha256, counts, missingAbstractCounts }, null, 2)}\n`);
