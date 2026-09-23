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
const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));
const plan = [
  ["canonical:0c69941f511327fc67135444", "DOI", "10.1016/j.coi.2013.02.006"],
  ["canonical:12ea9e116e6d9c11eae97ebf", "PMID", "27701119"],
  ["canonical:34dca5733dcb284ecc9372a0", "PMID", "2039994"],
  ["canonical:464fc0e8bd6e252aaf643016", "DOI", "10.1109/tvcg.2011.239"],
  ["canonical:469d6531657790b11bd6c979", "DOI", "10.1093/nar/gkab421"],
  ["canonical:4902f873695a98aeeae10030", "PMID", "12364499"],
  ["canonical:6adca6827ff48472a25bf396", "PMID", "11561904"],
  ["canonical:75b2ab5a7c01f3dc076df621", "PMID", "26038570"],
  ["canonical:99fc5eea34b1a46262eb9eeb", "DOI", "10.1021/acs.jcim.7b00343"],
  ["canonical:cf4502e8735bc31a5438b8b1", "PMID", "7589282"],
  ["canonical:f1920a25ca0c1e7688318f5d", "DOI", "10.1186/s12944-019-1032-5"],
  ["canonical:fcdb4bbb190db3b5ff8a19de", "PMID", "29394314"],
];
const requests = [];
for (const [recordId, identifierType, identifier] of plan) {
  const query = identifierType === "PMID" ? `EXT_ID:${identifier}` : `DOI:${identifier}`;
  const url = `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=${encodeURIComponent(query)}&format=json&resultType=core&pageSize=1`;
  let status = null;
  let bytes = Buffer.alloc(0);
  let error = null;
  let retrievedAt = null;
  let attempts = 0;
  while (attempts < 3) {
    attempts += 1;
    retrievedAt = new Date().toISOString();
    try {
      const response = await fetch(url, { headers: { Accept: "application/json", "User-Agent": "h2h-lit-pipeline-targeted-metadata-recovery/1.0 (research metadata audit)" } });
      status = response.status;
      bytes = Buffer.from(await response.arrayBuffer());
      if (!response.ok) error = `HTTP_${response.status}`;
      else {
        const json = JSON.parse(bytes.toString("utf8"));
        if (json.resultList?.result?.[0]?.abstractText) {
          error = null;
          break;
        }
        error = "HTTP_200_WITHOUT_ABSTRACT_RESULT";
      }
    } catch (caught) {
      error = caught instanceof Error ? caught.message : String(caught);
    }
    await sleep(2000);
  }
  const target = path.join(rawDir, `${recordId.replace("canonical:", "")}.europe_pmc_verified_identifier.json`);
  if (bytes.length) await fs.writeFile(target, bytes, { flag: "wx" });
  requests.push({
    record_id: recordId,
    source: "europe_pmc_verified_identifier",
    identifier_type: identifierType,
    identifier,
    url,
    retrieved_at_utc: retrievedAt,
    http_status: status,
    attempts,
    error,
    raw_response: bytes.length ? { path: relative(target), sha256: sha256(bytes), bytes: bytes.length } : null,
  });
  await sleep(750);
}
const manifest = {
  artifact_class: "TARGETED_TITLE_ABSTRACT_EUROPE_PMC_VERIFIED_IDENTIFIER_RETRIEVAL",
  schema_version: "1.0.0",
  status: "RAW_RESPONSES_COLLECTED_NOT_SCIENTIFICALLY_ADJUDICATED",
  created_at_utc: new Date().toISOString(),
  policy: { targeted_exact_identifier_lookups_only: true, full_papers_downloaded: false, inference_models_called: false },
  requests,
};
await fs.writeFile(path.join(internalDir, "retrieval_verified_europe_pmc_manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify({
  requests: requests.length,
  verified_abstract_responses: requests.filter((item) => item.http_status === 200 && item.error === null).length,
  failed_or_empty: requests.filter((item) => item.error !== null).length,
}, null, 2));
