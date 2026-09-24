import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import readline from "node:readline";
import { spawnSync } from "node:child_process";
import { TextDecoder } from "node:util";

const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const outputDir = path.resolve(process.argv[2] || path.join(
  repoRoot,
  "outputs/staging/title-abstract-remaining-campaign-v1-20260924",
));
const corpusPath = path.join(repoRoot, "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/review_dataset.json");
const overlayPath = path.join(repoRoot, "outputs/production/star-external-retrieval-wave-001/execution/PriorSurveyIdentityAdjudication/v1/identity_confirmation_overlay.json");
const samplingPath = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json");
const priorRoot = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-batch-v2-1-2-repair-gpt-5-6-luna-20260923T093018Z");
const overnightRoot = path.join(repoRoot, "outputs/staging/title-abstract-fast-track-overnight-completion-audit-20260923-v1/remote-results");
const precisionRoot = path.join(repoRoot, "outputs/staging/title-abstract-precision-rescreen-completion-audit-20260924-v1/remote-results");
const precisionInput = path.join(repoRoot, "outputs/staging/title-abstract-precision-rescreen-v2-2-0-20260923-v2/precision_rescreen_890.jsonl");
const configPath = path.join(repoRoot, "config/title_abstract_remaining_campaign_v1_0_0.json");
const screeningPromptPath = path.join(repoRoot, "prompts/title_abstract_precision_rescreen_v2_2_0.md");
const codingPromptPath = path.join(repoRoot, "prompts/title_abstract_taxonomy_coding_v1_0_0.md");
const protocolPath = path.join(repoRoot, "config/title_abstract_screening_protocol_v2_0_0.json");
const amendmentPath = path.join(repoRoot, "config/title_abstract_screening_precision_amendment_v2_2_0.json");
const seed = "h2h-title-abstract-fast-track-next-batch-v1";

const expected = {
  corpus: "44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2",
  overlay: "c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d",
  sampling: "a2fd6e54c6a955eadbd4ea63d9996b9bbb8ec29c65024a733eefdd5f13747249",
  priorInclude: "dfa921205d0334e6e7354ec53f9824475a8497edd99ba3ebbc7e74b94804f1fe",
  priorDeferred: "fd56f11e5aca241d6d87174ffe8e452709ebbe8678548edf70301e6f6c45c427",
  priorExcluded: "a55aeb58ce585f4732a425be81e220e3d367bdf20b130fc62763323d5b0609a6",
  priorUnresolved: "8da876a3eb61e1b133957bb5a1f3ac295af54d209fb89f6d0a6eff19b0b1da0c",
  overnightInclude: "c9559522d0a22bc991c39df94051bd8043fa7faa8a07b44c93bee3c9ec5ac713",
  overnightDeferred: "eb454236b329653151152a71a3e149b064e995a6ff19fec5ed215cace18a29e7",
  overnightExcluded: "f681935e8c1dbdf468efac944fbd3ab82661f47eb3195e9a1b5c4e610b661f5a",
  overnightFailed: "7a4b8b56c31340c8feef613cd6535da7157427a07b8497b899bd438ab9381d84",
  overnightUnprocessed: "6afb6c5f288ba40d9de7ecba913db2787b5c5a876889ea8524e5322fe65970c9",
  overnightRunReport: "26c9ac7b45de01c782a812d8c6faca9b147db670d9e7299ee30f79e8360dc10a",
  precisionAdvance: "2215ac88defe3bd0ca027403e268804d31a498067184d7c96f55612a6676831a",
  precisionRunReport: "02777c8ac964683b125edbd3f90bcfe1b42368c4a7102fa5884cf9dc845b8927",
  precisionPackage: "97fa074459c291684178ba0f24c79064c0ec10486c43d6e806f91a55d872ddcd",
  precisionInput: "df60ead61e4fcfe0f7b277beae7f67bf53b6343712c7ee1cc638e1806c4fde9b",
};

const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const compact = (value) => String(value ?? "").trim();
const rel = (target) => path.relative(repoRoot, target).replaceAll(path.sep, "/");
const rankHex = (canonicalId) => sha256(`${seed}\u001f${canonicalId}`);
const readJson = async (target) => JSON.parse(await fsp.readFile(target, "utf8"));

async function verified(label, target, digest) {
  const bytes = await fsp.readFile(target);
  const actual = sha256(bytes);
  if (actual !== digest) throw new Error(`${label} SHA-256 drift: ${actual}`);
  return { path: rel(target), sha256: actual, bytes: bytes.length };
}

async function readJsonl(target) {
  const rows = [];
  const input = fs.createReadStream(target);
  const lines = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of lines) if (line.trim()) rows.push(JSON.parse(line));
  return rows;
}

async function addIds(target, destination) {
  const input = fs.createReadStream(target);
  const lines = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    const value = JSON.parse(line);
    if (!compact(value.canonical_id)) throw new Error(`missing canonical ID in ${target}`);
    destination.add(value.canonical_id);
  }
}

async function streamCorpus(target, excludedIds, retryableIds, priorUnprocessedIds, rankedPath) {
  const marker = '"canonical_records":[';
  const digest = crypto.createHash("sha256");
  const decoder = new TextDecoder("utf-8");
  const stream = fs.createReadStream(target, { highWaterMark: 1024 * 1024 });
  const fd = fs.openSync(rankedPath, "wx");
  let prefix = "";
  let found = false;
  let parsing = true;
  let current = "";
  let depth = 0;
  let inString = false;
  let escaped = false;
  let count = 0;
  let selected = 0;
  const originCounts = { earlier_25000_unprocessed: 0, safe_retry: 0, other_remaining: 0 };

  const accept = (raw) => {
    count += 1;
    const canonicalId = compact(raw.canonical_id);
    if (!canonicalId || excludedIds.has(canonicalId)) return;
    const record = raw.record || {};
    const queueOrigin = retryableIds.has(canonicalId)
      ? "safe_retry"
      : priorUnprocessedIds.has(canonicalId)
        ? "earlier_25000_unprocessed"
        : "other_remaining";
    originCounts[queueOrigin] += 1;
    selected += 1;
    const value = {
      batch_status: "UNPROCESSED_READY_NOT_LAUNCHED",
      canonical_id: canonicalId,
      title: compact(record.title),
      abstract: compact(record.abstract),
      publication_year: record.year ?? null,
      doi: compact(record.doi) || null,
      source_database: compact(record.source_database) || null,
      source_identifier: compact(record.source_identifier) || null,
      source_url: compact(record.source_url) || null,
      prior_candidate_number: null,
      queue_origin: queueOrigin,
      selection_rank_sha256: rankHex(canonicalId),
    };
    fs.writeSync(fd, `${value.selection_rank_sha256}\t${JSON.stringify(value)}\n`);
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
        if (char === "]") { parsing = false; return; }
        if (char !== "{") continue;
        current = "{"; depth = 1; inString = false; escaped = false; continue;
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
      if (depth === 0) { accept(JSON.parse(current)); current = ""; }
    }
  };

  try {
    for await (const chunk of stream) {
      digest.update(chunk);
      consume(decoder.decode(chunk, { stream: true }));
    }
    consume(decoder.decode());
  } finally {
    fs.closeSync(fd);
  }
  if (!found || parsing || current) throw new Error("registered canonical_records stream was incomplete");
  return { count, selected, sha256: digest.digest("hex"), originCounts };
}

if (fs.existsSync(outputDir)) throw new Error(`output directory already exists: ${outputDir}`);
await fsp.mkdir(outputDir, { recursive: false });

const paths = {
  priorInclude: path.join(priorRoot, "combined_include.jsonl"),
  priorDeferred: path.join(priorRoot, "combined_deferred.jsonl"),
  priorExcluded: path.join(priorRoot, "combined_excluded.jsonl"),
  priorUnresolved: path.join(priorRoot, "combined_unresolved.jsonl"),
  overnightInclude: path.join(overnightRoot, "include.jsonl"),
  overnightDeferred: path.join(overnightRoot, "deferred.jsonl"),
  overnightExcluded: path.join(overnightRoot, "excluded.jsonl"),
  overnightFailed: path.join(overnightRoot, "failed.jsonl"),
  overnightUnprocessed: path.join(overnightRoot, "unprocessed.jsonl"),
  overnightRunReport: path.join(overnightRoot, "run_report.json"),
  precisionAdvance: path.join(precisionRoot, "advance.jsonl"),
  precisionRunReport: path.join(precisionRoot, "run_report.json"),
  precisionPackage: path.join(precisionRoot, "package_manifest.json"),
};
const references = {
  overlay: await verified("identity overlay", overlayPath, expected.overlay),
  sampling: await verified("protected sample", samplingPath, expected.sampling),
  precision_input: await verified("precision input", precisionInput, expected.precisionInput),
};
for (const [name, target] of Object.entries(paths)) references[name] = await verified(name, target, expected[name]);

const overlay = await readJson(overlayPath);
if (overlay.current_status_if_authorized?.open_identity_groups !== 0 || overlay.preserved?.canonical_membership_changes !== 0) {
  throw new Error("authorized identity overlay status changed");
}
const sampling = await readJson(samplingPath);
const protectedIds = new Set(sampling.selected_records.map((row) => row.canonical_id));
if (protectedIds.size !== 110) throw new Error("protected sample membership changed");

const validIds = new Set();
for (const target of [paths.priorInclude, paths.priorDeferred, paths.priorExcluded, paths.overnightInclude, paths.overnightDeferred, paths.overnightExcluded]) await addIds(target, validIds);
const ambiguousIds = new Set();
await addIds(paths.priorUnresolved, ambiguousIds);
const retryableRows = await readJsonl(paths.overnightFailed);
const retryableIds = new Set(retryableRows.map((row) => row.canonical_id));
if (retryableIds.size !== 2 || retryableRows.some((row) => row.status !== "FAILED" || row.attempts?.length !== 2 || row.attempts.some((attempt) => attempt.status !== "INVALID" || !attempt.provider_metadata?.provider_response_id))) {
  throw new Error("failed-call retry safety evidence changed");
}
const priorUnprocessedIds = new Set();
await addIds(paths.overnightUnprocessed, priorUnprocessedIds);
if (validIds.size !== 14677 || ambiguousIds.size !== 4 || priorUnprocessedIds.size !== 10567) throw new Error("completed screening partition changed");
const excludedIds = new Set([...protectedIds, ...validIds, ...ambiguousIds]);
if (excludedIds.size !== 14791) throw new Error("queue exclusion union changed");

const rankedPath = path.join(outputDir, ".remaining_ranked.tsv");
const sortedPath = path.join(outputDir, ".remaining_sorted.tsv");
const streamed = await streamCorpus(corpusPath, excludedIds, retryableIds, priorUnprocessedIds, rankedPath);
if (streamed.sha256 !== expected.corpus || streamed.count !== 140959 || streamed.selected !== 126168) throw new Error("registered corpus or remaining-frame binding drift");
const sortedFd = fs.openSync(sortedPath, "wx");
const sortResult = spawnSync("/usr/bin/sort", ["-t", "\t", "-k1,1", rankedPath], {
  env: { ...process.env, LC_ALL: "C", TMPDIR: outputDir },
  stdio: ["ignore", sortedFd, "inherit"],
});
fs.closeSync(sortedFd);
if (sortResult.status !== 0) throw new Error(`external deterministic sort failed: ${sortResult.status}`);
const queuePath = path.join(outputDir, "remaining_screening_queue.jsonl");
const queueFd = fs.openSync(queuePath, "wx");
const sortedLines = readline.createInterface({ input: fs.createReadStream(sortedPath), crlfDelay: Infinity });
let order = 0;
let priorRank = "";
for await (const line of sortedLines) {
  const tab = line.indexOf("\t");
  if (tab < 1) throw new Error("sorted queue row is malformed");
  const rank = line.slice(0, tab);
  if (priorRank && rank <= priorRank) throw new Error("selection rank is not strictly increasing");
  priorRank = rank;
  const record = JSON.parse(line.slice(tab + 1));
  order += 1;
  fs.writeSync(queueFd, `${JSON.stringify({ batch_order: order, ...record })}\n`);
}
fs.closeSync(queueFd);
await fsp.unlink(rankedPath);
await fsp.unlink(sortedPath);
if (order !== 126168) throw new Error("final queue count changed");

const precisionInputRows = await readJsonl(precisionInput);
const precisionById = new Map(precisionInputRows.map((row) => [row.canonical_id, row]));
const advanceRows = await readJsonl(paths.precisionAdvance);
if (advanceRows.length !== 646) throw new Error("precision ADVANCE count changed");
const codingSeedPath = path.join(outputDir, "coding_seed_646.jsonl");
const codingSeed = advanceRows
  .sort((left, right) => left.batch_order - right.batch_order)
  .map((result, index) => {
    const source = precisionById.get(result.canonical_id);
    if (!source) throw new Error(`missing original evidence for ${result.canonical_id}`);
    return { coding_order: index + 1, coding_source: "precision_v2_2_0_advance", ...source };
  });
await fsp.writeFile(codingSeedPath, `${codingSeed.map((row) => JSON.stringify(row)).join("\n")}\n`, { flag: "wx" });

const queueHash = sha256(await fsp.readFile(queuePath));
const codingSeedHash = sha256(await fsp.readFile(codingSeedPath));
const manifest = {
  artifact_class: "title_abstract_remaining_screening_and_coding_inputs",
  schema_version: "1.0.0",
  status: "FROZEN_READY_FOR_AUTHORIZED_EXECUTION",
  frame: { path: rel(corpusPath), sha256: streamed.sha256, canonical_records: streamed.count, loaded_as_review_dataset: false },
  identity_overlay: { ...references.overlay, effective_open_groups: 0, canonical_membership_changed: false },
  order: { algorithm: "ascending_sha256(seed + unit_separator + canonical_id)", seed, independent_of_model_judgments: true },
  partition: {
    canonical_records: 140959,
    protected_evaluation_calibration: protectedIds.size,
    validly_screened: validIds.size,
    ambiguous_not_resubmitted: ambiguousIds.size,
    retryable_completed_invalid: retryableIds.size,
    untouched_or_unprocessed: streamed.selected - retryableIds.size,
    remaining_screening_queue: streamed.selected,
    queue_origins: streamed.originCounts,
    reconciliation_total: protectedIds.size + validIds.size + ambiguousIds.size + streamed.selected,
  },
  screening_queue: { path: rel(queuePath), sha256: queueHash, records: order, unique_ids: order },
  coding_seed: { path: rel(codingSeedPath), sha256: codingSeedHash, records: codingSeed.length, unique_ids: new Set(codingSeed.map((row) => row.canonical_id)).size },
  bindings: {
    references,
    config: { path: rel(configPath), sha256: sha256(await fsp.readFile(configPath)) },
    screening_prompt: { path: rel(screeningPromptPath), sha256: sha256(await fsp.readFile(screeningPromptPath)) },
    coding_prompt: { path: rel(codingPromptPath), sha256: sha256(await fsp.readFile(codingPromptPath)) },
    protocol: { path: rel(protocolPath), sha256: sha256(await fsp.readFile(protocolPath)) },
    amendment: { path: rel(amendmentPath), sha256: sha256(await fsp.readFile(amendmentPath)) },
    historical_starting_ledger: references.precisionRunReport,
  },
  constraints: { staging_only: true, memory_bounded_corpus_stream: true, model_calls_during_selection: 0, historical_artifacts_modified: false },
};
await fsp.writeFile(path.join(outputDir, "remaining_campaign_manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify({ output_dir: rel(outputDir), queue_records: order, queue_sha256: queueHash, coding_seed_records: codingSeed.length, coding_seed_sha256: codingSeedHash, partition: manifest.partition }, null, 2));
