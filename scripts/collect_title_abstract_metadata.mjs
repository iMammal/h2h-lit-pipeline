import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const auditPath = path.resolve(process.argv[2] || "/private/tmp/scoped_existing_evidence.json");
const outputDir = path.resolve(process.argv[3] || path.join(repoRoot, "outputs/staging/title-abstract-evidence-recovery-v1-20260922"));
const archiveDir = path.join(repoRoot, "outputs/staging/title-abstract-screening-v2-return-morris-20260922-v2");
const archiveManifestPath = path.join(archiveDir, "archive_manifest.json");
const recordValidationPath = path.join(archiveDir, "internal/record_validation.json");
const conflictId = "canonical:7d25df89fed662b381109a2e";
const expectedSubmissionSha256 = "ab3bd34d544490b85bebfd08bc8f1a86246595aadb4f94a3d94e501280aa7f28";
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const relative = (target) => path.relative(repoRoot, target);
const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function normalizeDoi(value) {
  return String(value || "").trim().toLowerCase().replace(/^https?:\/\/(dx\.)?doi\.org\//, "");
}

function normalizeTitle(value) {
  return String(value || "")
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function externalIds(record) {
  return record.original_metadata?.externalIds || {};
}

async function assertFreshDirectory(target) {
  try {
    await fs.access(target);
  } catch {
    await fs.mkdir(target, { recursive: false });
    return;
  }
  throw new Error(`output directory already exists: ${target}`);
}

async function requestJson(url, target, source, recordId, options = {}) {
  const retrievedAt = new Date().toISOString();
  let status = null;
  let responseBytes = Buffer.alloc(0);
  let error = null;
  try {
    const response = await fetch(url, {
      method: options.method || "GET",
      headers: {
        Accept: "application/json",
        "User-Agent": "h2h-lit-pipeline-targeted-metadata-recovery/1.0 (research metadata audit)",
        ...(options.body ? { "Content-Type": "application/json" } : {}),
      },
      body: options.body,
      redirect: "follow",
    });
    status = response.status;
    responseBytes = Buffer.from(await response.arrayBuffer());
    if (!response.ok) error = `HTTP_${response.status}`;
  } catch (caught) {
    error = caught instanceof Error ? caught.message : String(caught);
  }
  const responseRef = responseBytes.length
    ? {
        path: relative(target),
        sha256: sha256(responseBytes),
        bytes: responseBytes.length,
      }
    : null;
  if (responseBytes.length) await fs.writeFile(target, responseBytes, { flag: "wx" });
  return {
    record_id: recordId,
    source,
    url,
    retrieved_at_utc: retrievedAt,
    http_status: status,
    error,
    raw_response: responseRef,
  };
}

function safeName(recordId, source) {
  return `${recordId.replace("canonical:", "")}.${source}.json`;
}

const archiveManifestBytes = await fs.readFile(archiveManifestPath);
const archiveManifest = JSON.parse(archiveManifestBytes.toString("utf8"));
if (archiveManifest.source_submission?.sha256 !== expectedSubmissionSha256) {
  throw new Error("archive manifest submission binding differs from the authorized SHA-256");
}
const archivedSubmissionPath = path.join(repoRoot, archiveManifest.artifacts.archived_original.path);
if (sha256(await fs.readFile(archivedSubmissionPath)) !== expectedSubmissionSha256) {
  throw new Error("archived submitted workbook does not match the authorized SHA-256");
}

const validationBytes = await fs.readFile(recordValidationPath);
const validation = JSON.parse(validationBytes.toString("utf8"));
const recoveryIds = validation.records
  .filter((record) => record.actionable_next_action === "METADATA_RECOVERY")
  .map((record) => record.record_id);
if (recoveryIds.length !== 31) throw new Error(`expected 31 metadata-recovery records, found ${recoveryIds.length}`);
const scopedIds = [...recoveryIds, conflictId];
if (new Set(scopedIds).size !== 32) throw new Error("recovery and reconciliation scopes are not disjoint");

const auditBytes = await fs.readFile(auditPath);
const audit = JSON.parse(auditBytes.toString("utf8"));
if (scopedIds.some((recordId) => !audit.canonical_records?.[recordId])) {
  throw new Error("bounded existing-evidence audit is missing one or more scoped records");
}
if (Object.keys(audit.canonical_records).length !== 32) {
  throw new Error("bounded existing-evidence audit contains records outside the authorized scope");
}

await assertFreshDirectory(outputDir);
const rawDir = path.join(outputDir, "raw_responses");
const internalDir = path.join(outputDir, "internal");
await fs.mkdir(rawDir);
await fs.mkdir(internalDir);

const requests = [];
const semanticScholarIds = [];
for (const recordId of scopedIds) {
  const record = audit.canonical_records[recordId].record;
  const doi = normalizeDoi(record.doi || externalIds(record).DOI);
  const pmid = String(record.pmid || externalIds(record).PubMed || "").trim();
  const paperId = String(record.original_metadata?.paperId || "").trim();
  const encodedTitle = encodeURIComponent(record.title);
  const planned = [];
  if (pmid) {
    planned.push({
      source: "europe_pmc_pmid",
      url: `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID%3A${encodeURIComponent(pmid)}&format=json&resultType=core`,
    });
  }
  if (doi) {
    planned.push({
      source: "crossref_doi",
      url: `https://api.crossref.org/works/${encodeURIComponent(doi)}`,
    });
    planned.push({
      source: "europe_pmc_doi",
      url: `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI%3A${encodeURIComponent(doi)}&format=json&resultType=core`,
    });
  }
  if (paperId) {
    semanticScholarIds.push({ record_id: recordId, paper_id: paperId });
  }
  if (!doi && !pmid && !paperId) {
    planned.push({
      source: "crossref_title",
      url: `https://api.crossref.org/works?query.bibliographic=${encodedTitle}&rows=5&select=DOI,title,author,published,container-title,abstract,URL,type`,
    });
    planned.push({
      source: "europe_pmc_title",
      url: `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=TITLE%3A%22${encodedTitle}%22&format=json&resultType=core&pageSize=5`,
    });
    planned.push({
      source: "semantic_scholar_title",
      url: `https://api.semanticscholar.org/graph/v1/paper/search?query=${encodedTitle}&limit=5&fields=paperId,externalIds,title,abstract,year,authors,venue,url`,
    });
  }
  for (const item of planned) {
    const target = path.join(rawDir, safeName(recordId, item.source));
    const result = await requestJson(item.url, target, item.source, recordId);
    requests.push(result);
    await sleep(item.source.startsWith("semantic_scholar") ? 1100 : 150);
  }
}

if (semanticScholarIds.length) {
  const batchUrl = "https://api.semanticscholar.org/graph/v1/paper/batch?fields=paperId,externalIds,title,abstract,year,authors,venue,url";
  requests.push(await requestJson(
    batchUrl,
    path.join(rawDir, "semantic_scholar_paper_batch.json"),
    "semantic_scholar_paper_batch",
    "MULTIPLE_SCOPED_RECORDS",
    { method: "POST", body: JSON.stringify({ ids: semanticScholarIds.map((item) => item.paper_id) }) },
  ));
}

const existingAuditPath = path.join(internalDir, "existing_evidence_audit.json");
await fs.writeFile(existingAuditPath, auditBytes, { flag: "wx" });
const retrievalManifest = {
  artifact_class: "TARGETED_TITLE_ABSTRACT_METADATA_RETRIEVAL",
  schema_version: "1.0.0",
  status: "RAW_RESPONSES_COLLECTED_NOT_SCIENTIFICALLY_ADJUDICATED",
  created_at_utc: new Date().toISOString(),
  scope: {
    metadata_recovery_records: recoveryIds.length,
    evidence_reconciliation_records: 1,
    total: scopedIds.length,
    record_ids: scopedIds,
  },
  bindings: {
    archive_manifest: { path: relative(archiveManifestPath), sha256: sha256(archiveManifestBytes) },
    archived_submission: { path: relative(archivedSubmissionPath), sha256: expectedSubmissionSha256 },
    record_validation: { path: relative(recordValidationPath), sha256: sha256(validationBytes) },
    bounded_existing_evidence_audit: { path: relative(existingAuditPath), sha256: sha256(auditBytes) },
  },
  policy: {
    targeted_identifier_lookups_only: true,
    broad_searches_run: false,
    full_papers_downloaded: false,
    inference_models_called: false,
    note: "Title-query candidates are discovery candidates only; title matching alone is never sufficient to attach an abstract.",
  },
  requests,
  semantic_scholar_batch_order: semanticScholarIds,
};
const manifestBytes = Buffer.from(`${JSON.stringify(retrievalManifest, null, 2)}\n`);
await fs.writeFile(path.join(internalDir, "retrieval_manifest.json"), manifestBytes, { flag: "wx" });
console.log(JSON.stringify({
  output_dir: relative(outputDir),
  scoped_records: scopedIds.length,
  requests: requests.length,
  successful_responses: requests.filter((item) => item.http_status === 200).length,
  failed_responses: requests.filter((item) => item.http_status !== 200).length,
}, null, 2));
