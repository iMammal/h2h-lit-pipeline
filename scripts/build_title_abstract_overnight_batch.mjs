import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { TextDecoder } from "node:util";

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const outputDir = path.resolve(process.argv[2] || path.join(
  repoRoot,
  "outputs/staging/title-abstract-fast-track-overnight-v2-1-2-20260923",
));
const corpusPath = path.join(repoRoot, "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/review_dataset.json");
const overlayPath = path.join(repoRoot, "outputs/production/star-external-retrieval-wave-001/execution/PriorSurveyIdentityAdjudication/v1/identity_confirmation_overlay.json");
const samplingPath = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json");
const priorBatchPath = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-v2-1-0-20260922/next_batch_250.jsonl");
const repairDir = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-batch-v2-1-2-repair-gpt-5-6-luna-20260923T093018Z");
const workflowLedgerPath = path.join(repoRoot, "outputs/staging/human-validation-review-workspace-v1-20260920T230844Z/assignment_ledger.json");
const seed = "h2h-title-abstract-fast-track-next-batch-v1";
const batchSize = 25000;
const expected = {
  corpus: "44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2",
  overlay: "c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d",
  sampling: "a2fd6e54c6a955eadbd4ea63d9996b9bbb8ec29c65024a733eefdd5f13747249",
  priorBatch: "baab0b22a5c97159f1e8bb58c76b4e7a00df634a74c82beba8fe0f573cbe143d",
  repairManifest: "f716de85d901c62801134a57a720d4df7e3057ded1d0e9718508a1f1c5974262",
  repairReport: "ba036bb0f6e93a57c229d772f7b86c83244b7c48e165250d5349d44508d0a2c8",
};

const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const compact = (value) => String(value ?? "").trim();
const rel = (target) => path.relative(repoRoot, target).replaceAll(path.sep, "/");
const readJson = async (target) => JSON.parse(await fsp.readFile(target, "utf8"));
const rankHex = (canonicalId) => sha256(`${seed}\u001f${canonicalId}`);

function heapSwap(heap, left, right) {
  [heap[left], heap[right]] = [heap[right], heap[left]];
}

function heapPushMax(heap, value) {
  heap.push(value);
  let index = heap.length - 1;
  while (index > 0) {
    const parent = Math.floor((index - 1) / 2);
    if (heap[parent].selection_rank_sha256 >= heap[index].selection_rank_sha256) break;
    heapSwap(heap, parent, index);
    index = parent;
  }
}

function heapReplaceMax(heap, value) {
  heap[0] = value;
  let index = 0;
  while (true) {
    const left = index * 2 + 1;
    const right = left + 1;
    let largest = index;
    if (left < heap.length && heap[left].selection_rank_sha256 > heap[largest].selection_rank_sha256) largest = left;
    if (right < heap.length && heap[right].selection_rank_sha256 > heap[largest].selection_rank_sha256) largest = right;
    if (largest === index) break;
    heapSwap(heap, index, largest);
    index = largest;
  }
}

function boundedRankAdd(heap, value) {
  if (heap.length < batchSize) heapPushMax(heap, value);
  else if (value.selection_rank_sha256 < heap[0].selection_rank_sha256) heapReplaceMax(heap, value);
}

async function streamCorpus(target, excludedIds) {
  const marker = '"canonical_records":[';
  const digest = crypto.createHash("sha256");
  const decoder = new TextDecoder("utf-8");
  const stream = fs.createReadStream(target, { highWaterMark: 1024 * 1024 });
  let prefix = "";
  let found = false;
  let parsing = true;
  let current = "";
  let depth = 0;
  let inString = false;
  let escaped = false;
  let count = 0;
  const heap = [];

  const accept = (raw) => {
    count += 1;
    const canonicalId = compact(raw.canonical_id);
    if (!canonicalId || excludedIds.has(canonicalId)) return;
    const record = raw.record || {};
    boundedRankAdd(heap, {
      batch_status: "UNPROCESSED_READY_NOT_LAUNCHED",
      canonical_id: canonicalId,
      title: compact(record.title),
      abstract: compact(record.abstract),
      publication_year: record.year ?? null,
      doi: compact(record.doi) || null,
      source_database: compact(record.source_database) || null,
      source_identifier: compact(record.source_identifier) || null,
      source_url: compact(record.source_url) || null,
      selection_rank_sha256: rankHex(canonicalId),
    });
  };

  const consume = (chunk) => {
    let text = chunk;
    if (!found) {
      prefix += text;
      const markerIndex = prefix.indexOf(marker);
      if (markerIndex < 0) {
        prefix = prefix.slice(-(marker.length - 1));
        return;
      }
      found = true;
      text = prefix.slice(markerIndex + marker.length);
      prefix = "";
    }
    if (!parsing) return;
    for (let index = 0; index < text.length; index += 1) {
      const char = text[index];
      if (depth === 0) {
        if (char === "]") {
          parsing = false;
          return;
        }
        if (char !== "{") continue;
        current = "{";
        depth = 1;
        inString = false;
        escaped = false;
        continue;
      }
      current += char;
      if (inString) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') inString = false;
        continue;
      }
      if (char === '"') inString = true;
      else if (char === "{" || char === "[") depth += 1;
      else if (char === "}" || char === "]") depth -= 1;
      if (depth === 0) {
        accept(JSON.parse(current));
        current = "";
      }
    }
  };

  for await (const chunk of stream) {
    digest.update(chunk);
    consume(decoder.decode(chunk, { stream: true }));
  }
  consume(decoder.decode());
  if (!found || parsing || current) throw new Error("registered canonical_records stream was incomplete");
  return {
    count,
    sha256: digest.digest("hex"),
    selected: heap.sort((left, right) => left.selection_rank_sha256.localeCompare(right.selection_rank_sha256)),
  };
}

if (fs.existsSync(outputDir)) throw new Error(`output directory already exists: ${outputDir}`);
await fsp.mkdir(outputDir, { recursive: false });
const repairManifestPath = path.join(repairDir, "package_manifest.json");
const repairReportPath = path.join(repairDir, "combined_coverage_report.json");
for (const [label, target, digest] of [
  ["overlay", overlayPath, expected.overlay],
  ["sampling", samplingPath, expected.sampling],
  ["prior batch", priorBatchPath, expected.priorBatch],
  ["repair manifest", repairManifestPath, expected.repairManifest],
  ["repair report", repairReportPath, expected.repairReport],
]) {
  const actual = sha256(await fsp.readFile(target));
  if (actual !== digest) throw new Error(`${label} SHA-256 changed: ${actual}`);
}

const sampling = await readJson(samplingPath);
const repairReport = await readJson(repairReportPath);
if (repairReport.status !== "COMPLETE" || repairReport.original_valid_results_preserved !== 78 || repairReport.repair_validated !== 168) {
  throw new Error("completed repair binding is not the expected 246-screened state");
}
const frozenIds = new Set(sampling.selected_records.map((record) => record.canonical_id));
const priorBatchLines = (await fsp.readFile(priorBatchPath, "utf8")).trim().split("\n").map(JSON.parse);
const priorIds = new Set(priorBatchLines.map((record) => record.canonical_id));
const workflowLedger = await readJson(workflowLedgerPath);
const workflowIds = new Set(workflowLedger.assignments.map((record) => record.record_id));
const excludedIds = new Set([...frozenIds, ...priorIds, ...workflowIds]);
if (frozenIds.size !== 110 || priorIds.size !== 250) throw new Error("exclusion membership changed");

const streamed = await streamCorpus(corpusPath, excludedIds);
if (streamed.sha256 !== expected.corpus || streamed.count !== 140959 || streamed.selected.length !== batchSize) {
  throw new Error("registered corpus or selected batch binding changed");
}
const batchPath = path.join(outputDir, `overnight_batch_${batchSize}.jsonl`);
const rows = streamed.selected.map((record, index) => ({ batch_order: index + 1, ...record }));
await fsp.writeFile(batchPath, `${rows.map((record) => JSON.stringify(record)).join("\n")}\n`, { flag: "wx" });
const batchHash = sha256(await fsp.readFile(batchPath));
const overlap = rows.filter((record) => excludedIds.has(record.canonical_id));
if (overlap.length) throw new Error("selected batch overlaps excluded membership");

const manifest = {
  artifact_class: "title_abstract_fast_track_overnight_batch",
  schema_version: "1.0.0",
  status: "FROZEN_READY_FOR_AUTHORIZED_STAGING_EXECUTION",
  batch: { path: rel(batchPath), sha256: batchHash, records: rows.length, unique_ids: new Set(rows.map((row) => row.canonical_id)).size },
  frame: { path: rel(corpusPath), sha256: streamed.sha256, canonical_records: streamed.count, loaded_as_review_dataset: false },
  identity_overlay: { path: rel(overlayPath), sha256: expected.overlay, canonical_membership_changed: false },
  order: { algorithm: "ascending_sha256(seed + unit_separator + canonical_id)", seed, independent_of_model_judgments: true },
  exclusions: {
    frozen_evaluation_and_calibration: frozenIds.size,
    original_250_including_four_ambiguous: priorIds.size,
    workflow_test_record_ids_supplied: workflowIds.size,
    selected_overlap: overlap.length,
  },
  prior_screening: {
    repair_package_manifest: { path: rel(repairManifestPath), sha256: expected.repairManifest },
    repair_report: { path: rel(repairReportPath), sha256: expected.repairReport },
    counts: repairReport.counts,
    cumulative_conservative_cost_usd: repairReport.cumulative_conservative_cost_usd,
  },
  constraints: { staging_only: true, model_calls_during_selection: 0, memory_bounded_corpus_stream: true },
};
const manifestPath = path.join(outputDir, "overnight_batch_manifest.json");
await fsp.writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
await fsp.writeFile(path.join(outputDir, "selection_complete.json"), `${JSON.stringify({
  batch_path: rel(batchPath), batch_sha256: batchHash, batch_records: rows.length,
  manifest_path: rel(manifestPath), frame_sha256: streamed.sha256,
}, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify({ output_dir: rel(outputDir), batch_sha256: batchHash, records: rows.length }, null, 2));
