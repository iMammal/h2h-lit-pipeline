import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packageDir = path.resolve(process.argv[2] || path.join(repoRoot, "outputs/staging/title-abstract-evidence-recovery-v3-20260922"));
const rawDir = path.join(packageDir, "raw_responses");
const internalDir = path.join(packageDir, "internal");
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const relative = (target) => path.relative(repoRoot, target);
const plan = [
  ["canonical:99fc5eea34b1a46262eb9eeb", "10.1021/acs.jcim.7b00343"],
  ["canonical:f1920a25ca0c1e7688318f5d", "10.1186/s12944-019-1032-5"],
];
const requests = [];
for (const [recordId, doi] of plan) {
  const url = `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI%3A${encodeURIComponent(doi)}&format=json&resultType=core`;
  const retrievedAt = new Date().toISOString();
  let status = null;
  let bytes = Buffer.alloc(0);
  let error = null;
  try {
    const response = await fetch(url, { headers: { Accept: "application/json", "User-Agent": "h2h-lit-pipeline-targeted-metadata-recovery/1.0 (research metadata audit)" } });
    status = response.status;
    bytes = Buffer.from(await response.arrayBuffer());
    if (!response.ok) error = `HTTP_${response.status}`;
  } catch (caught) {
    error = caught instanceof Error ? caught.message : String(caught);
  }
  const target = path.join(rawDir, `${recordId.replace("canonical:", "")}.europe_pmc_discovered_doi.json`);
  if (bytes.length) await fs.writeFile(target, bytes, { flag: "wx" });
  requests.push({
    record_id: recordId,
    source: "europe_pmc_discovered_doi",
    url,
    retrieved_at_utc: retrievedAt,
    http_status: status,
    error,
    raw_response: bytes.length ? { path: relative(target), sha256: sha256(bytes), bytes: bytes.length } : null,
  });
}
const manifest = {
  artifact_class: "TARGETED_TITLE_ABSTRACT_METADATA_RETRIEVAL_ADDENDUM",
  schema_version: "1.0.0",
  status: "RAW_RESPONSES_COLLECTED_NOT_SCIENTIFICALLY_ADJUDICATED",
  created_at_utc: new Date().toISOString(),
  policy: { targeted_exact_doi_lookups_only: true, full_papers_downloaded: false, inference_models_called: false },
  requests,
};
await fs.writeFile(path.join(internalDir, "retrieval_targeted_addendum_manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify({ requests: requests.length, successful_responses: requests.filter((item) => item.http_status === 200).length }, null, 2));
