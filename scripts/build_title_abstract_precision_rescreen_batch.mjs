import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const outputDir = path.resolve(process.argv[2] || path.join(
  repoRoot,
  "outputs/staging/title-abstract-precision-rescreen-v2-2-0-20260923",
));
const priorPackage = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-batch-v2-1-2-repair-gpt-5-6-luna-20260923T093018Z");
const priorIncludePath = path.join(priorPackage, "combined_include.jsonl");
const priorBatchPath = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-v2-1-0-20260922/next_batch_250.jsonl");
const overnightAudit = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-overnight-completion-audit-20260923-v1");
const overnightResults = path.join(overnightAudit, "remote-results");
const overnightIncludePath = path.join(overnightResults, "include.jsonl");
const overnightBatchPath = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-overnight-v2-1-2-20260923/overnight_batch_25000.jsonl");
const overnightRunReportPath = path.join(overnightResults, "run_report.json");
const overnightPackageManifestPath = path.join(overnightResults, "package_manifest.json");
const handoffWorkbookPath = path.join(overnightAudit, "fast_track_include_candidates_890.xlsx");
const completionAuditPath = path.join(overnightAudit, "completion_audit.json");
const relatedManifestPath = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json");
const amendmentPath = path.join(repoRoot, "config/title_abstract_screening_precision_amendment_v2_2_0.json");
const protocolPath = path.join(repoRoot, "config/title_abstract_screening_protocol_v2_0_0.json");
const promptPath = path.join(repoRoot, "prompts/title_abstract_precision_rescreen_v2_2_0.md");

const expected = {
  priorInclude: "dfa921205d0334e6e7354ec53f9824475a8497edd99ba3ebbc7e74b94804f1fe",
  priorBatch: "baab0b22a5c97159f1e8bb58c76b4e7a00df634a74c82beba8fe0f573cbe143d",
  overnightInclude: "c9559522d0a22bc991c39df94051bd8043fa7faa8a07b44c93bee3c9ec5ac713",
  overnightBatch: "e4b62c2ec3c769630a1d9d2bb377554f1875efe49da42287328785ab86673d03",
  overnightRunReport: "26c9ac7b45de01c782a812d8c6faca9b147db670d9e7299ee30f79e8360dc10a",
  overnightPackageManifest: "8dbbd9e376e6ca52331f55dec1c71ffc477c83cd2669aaa6aa6b70198f2a2a0e",
  handoffWorkbook: "cbfb204dc7f9a8339efda922a3fca74e397c47611431f8cb4534ae4a5c08f1e5",
  completionAudit: "fdc497951ca77cc8118fafdb1f2f773c63ee492327d0af1533ff0476a4684582",
};

const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const rel = (target) => path.relative(repoRoot, target).replaceAll(path.sep, "/");
const compact = (value) => String(value ?? "").trim();
const readJson = async (target) => JSON.parse(await fsp.readFile(target, "utf8"));

async function readJsonl(target) {
  const rows = [];
  const input = fs.createReadStream(target);
  const lines = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of lines) if (line.trim()) rows.push(JSON.parse(line));
  return rows;
}

async function metadataFor(target, wantedIds) {
  const found = new Map();
  const input = fs.createReadStream(target);
  const lines = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    const record = JSON.parse(line);
    if (wantedIds.has(record.canonical_id)) found.set(record.canonical_id, record);
  }
  return found;
}

async function verifiedReference(label, target, expectedHash) {
  const bytes = await fsp.readFile(target);
  const actual = sha256(bytes);
  if (actual !== expectedHash) throw new Error(`${label} SHA-256 drift: ${actual}`);
  return { path: rel(target), sha256: actual, bytes: bytes.length };
}

if (fs.existsSync(outputDir)) throw new Error(`output directory already exists: ${outputDir}`);

const references = {
  prior_include: await verifiedReference("prior INCLUDE source", priorIncludePath, expected.priorInclude),
  prior_batch: await verifiedReference("prior batch", priorBatchPath, expected.priorBatch),
  overnight_include: await verifiedReference("overnight INCLUDE source", overnightIncludePath, expected.overnightInclude),
  overnight_batch: await verifiedReference("overnight batch", overnightBatchPath, expected.overnightBatch),
  overnight_run_report: await verifiedReference("overnight run report", overnightRunReportPath, expected.overnightRunReport),
  overnight_package_manifest: await verifiedReference("overnight package manifest", overnightPackageManifestPath, expected.overnightPackageManifest),
  handoff_workbook: await verifiedReference("890-candidate handoff workbook", handoffWorkbookPath, expected.handoffWorkbook),
  completion_audit: await verifiedReference("completion audit", completionAuditPath, expected.completionAudit),
};

const completionAudit = await readJson(completionAuditPath);
const overnightRunReport = await readJson(overnightRunReportPath);
if (
  completionAudit.counts.valid_screenings !== 14431
  || completionAudit.counts.include !== 879
  || completionAudit.candidate_workbook.records !== 890
  || overnightRunReport.counts.include !== 879
  || overnightRunReport.status !== "STOPPED"
) throw new Error("completed overnight audit binding drift");

const priorIncludes = (await readJsonl(priorIncludePath)).sort((a, b) => a.batch_order - b.batch_order);
const overnightIncludes = (await readJsonl(overnightIncludePath)).sort((a, b) => a.batch_order - b.batch_order);
if (priorIncludes.length !== 11 || overnightIncludes.length !== 879) throw new Error("candidate cohort count drift");
const priorIds = new Set(priorIncludes.map((record) => record.canonical_id));
const overnightIds = new Set(overnightIncludes.map((record) => record.canonical_id));
if (priorIds.size !== 11 || overnightIds.size !== 879) throw new Error("candidate cohort contains duplicate IDs");
if ([...priorIds].some((recordId) => overnightIds.has(recordId))) throw new Error("candidate cohorts overlap");

const priorMetadata = await metadataFor(priorBatchPath, priorIds);
const overnightMetadata = await metadataFor(overnightBatchPath, overnightIds);
if (priorMetadata.size !== 11 || overnightMetadata.size !== 879) throw new Error("original title/abstract metadata binding incomplete");

const relatedManifest = await readJson(relatedManifestPath);
const relatedMap = new Map();
for (let index = 0; index < relatedManifest.related_version_components.length; index += 1) {
  for (const recordId of relatedManifest.related_version_components[index]) {
    relatedMap.set(recordId, {
      component_id: `component-${index + 1}`,
      canonical_ids: relatedManifest.related_version_components[index],
    });
  }
}

const rows = [];
for (const [cohort, includes, metadata] of [
  ["PRIOR_250_EFFECTIVE_REPAIR", priorIncludes, priorMetadata],
  ["OVERNIGHT_25000", overnightIncludes, overnightMetadata],
]) {
  for (const result of includes) {
    const source = metadata.get(result.canonical_id);
    rows.push({
      batch_order: rows.length + 1,
      batch_status: "UNPROCESSED_READY_NOT_LAUNCHED",
      canonical_id: result.canonical_id,
      title: compact(source.title),
      abstract: compact(source.abstract),
      publication_year: source.publication_year ?? null,
      doi: compact(source.doi) || null,
      source_database: compact(source.source_database) || null,
      source_identifier: compact(source.source_identifier) || null,
      source_url: compact(source.source_url) || null,
      prior_candidate_number: rows.length + 1,
      prior_screening_outcome: "INCLUDE",
      prior_source_cohort: cohort,
      prior_source_batch_order: result.batch_order,
      prior_result_input_sha256: compact(result.input_hash) || null,
      related_version_component: relatedMap.get(result.canonical_id) || null,
    });
  }
}

const ids = rows.map((record) => record.canonical_id);
if (rows.length !== 890 || new Set(ids).size !== 890) throw new Error("combined 890-candidate binding failed");
for (let index = 0; index < rows.length; index += 1) {
  if (rows[index].batch_order !== index + 1 || rows[index].prior_candidate_number !== index + 1) {
    throw new Error("candidate order is not contiguous and preserved");
  }
}

const reviewedCases = [
  [562, "canonical:6cba34b3fcffe6edee586e7c", "Methods in the Study of African Historical Geography, Landscapes, and Environmental Change"],
  [375, "canonical:63dbecba9805af2d57e4a720", "GIVE: toward portable genome browsers for personal websites"],
  [877, "canonical:7d4fb116a00ffdf960725e82", "3D objects visualization for remote interactive medical applications"],
];
for (const [number, canonicalId, title] of reviewedCases) {
  const record = rows[number - 1];
  if (record.canonical_id !== canonicalId || record.title !== title) {
    throw new Error(`reviewed check-case identity drift for former candidate ${number}`);
  }
}

await fsp.mkdir(outputDir, { recursive: false });
const batchPath = path.join(outputDir, "precision_rescreen_890.jsonl");
await fsp.writeFile(batchPath, `${rows.map((record) => JSON.stringify(record)).join("\n")}\n`, { flag: "wx" });
const batchReference = await verifiedReference("generated precision batch", batchPath, sha256(await fsp.readFile(batchPath)));
const amendment = await readJson(amendmentPath);
if (amendment.amendment_version !== "2.2.0") throw new Error("unexpected amendment version");

const manifest = {
  artifact_class: "title_abstract_precision_rescreen_batch",
  schema_version: "1.0.0",
  status: "FROZEN_READY_FOR_AUTHORIZED_STAGING_EXECUTION",
  batch: { ...batchReference, records: rows.length, unique_ids: new Set(ids).size },
  preserved_order: "11 repaired-batch INCLUDE candidates followed by 879 overnight INCLUDE candidates, each in original processing order",
  source_bindings: references,
  protocol: { path: rel(protocolPath), sha256: sha256(await fsp.readFile(protocolPath)), version: "2.0.0" },
  amendment: { path: rel(amendmentPath), sha256: sha256(await fsp.readFile(amendmentPath)), version: "2.2.0" },
  prompt: { path: rel(promptPath), sha256: sha256(await fsp.readFile(promptPath)), version: "2.2.0" },
  campaign_ledger: {
    path: rel(overnightRunReportPath),
    sha256: references.overnight_run_report.sha256,
    cumulative_conservative_cost_usd: overnightRunReport.cumulative_conservative_cost_usd,
    unresolved_cost_reservations_usd: overnightRunReport.current_inflight_reserved_cost_usd,
    cumulative_usage: overnightRunReport.cumulative_usage,
  },
  counts: {
    records: rows.length,
    prior_250_effective_repair: priorIncludes.length,
    overnight_25000: overnightIncludes.length,
    known_related_version_candidates: rows.filter((record) => record.related_version_component).length,
  },
  reviewed_check_case_bindings: reviewedCases.map(([formerCandidateNumber, canonicalId, title]) => ({ former_candidate_number: formerCandidateNumber, canonical_id: canonicalId, title })),
  inference_input_policy: "Only canonical_id and deterministic units from the original supplied title/abstract are sent to the model; prior outcomes and provenance fields remain local.",
  historical_evidence_regenerated: false,
  staging_only: true,
};
const manifestPath = path.join(outputDir, "precision_rescreen_890_manifest.json");
await fsp.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
await fsp.writeFile(path.join(outputDir, "freeze_complete.json"), `${JSON.stringify({
  batch_path: rel(batchPath), batch_sha256: batchReference.sha256,
  manifest_path: rel(manifestPath), manifest_sha256: sha256(await fsp.readFile(manifestPath)),
  records: rows.length, unique_ids: new Set(ids).size,
}, null, 2)}\n`, { flag: "wx" });

console.log(JSON.stringify({ output_dir: rel(outputDir), batch: batchReference, manifest: rel(manifestPath) }, null, 2));
