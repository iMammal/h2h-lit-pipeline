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

const unresolvedSemanticScholar = [
  ["canonical:192298dcdffaf27eb19a58b5", "d0a8dacbf9f5173f05c08c80da63a3fb10817772"],
  ["canonical:1db68f451a35455ce68f8a50", "6830270e047fde4788d1eee510a587a999afad6c"],
  ["canonical:3b7e6f6b38cda1a093f1d9e7", "5e081ff74e01ac027484d44af03de3b9de00750d"],
  ["canonical:7f54fbf868cbd7e9872c394b", "1ac3a492ee8610e6fa48bca8aa32b069e2bedc67"],
  ["canonical:88f9627ece65f97d4a6c177f", "9fec4133cfe6549c33fc5338e29c351ac874ac7a"],
  ["canonical:8fe071a1c4abd056937a1e3f", "2ff7d66f89f50e46a2949333fb5340642e3d888d"],
  ["canonical:ae888899d9f05b7b611b1e56", "d78344ed96fa9151eb4a22f8544ba7459c55f679"],
  ["canonical:b70f5e4e3482745a131ad622", "1916c031f6db5e787f07d8692660f6d64d6ebf34"],
  ["canonical:b740e1c489066b96fa478c21", "ad0680b9df425c733b48ba8ac388a3c1901c9f5d"],
  ["canonical:bc420a01aa56fadf2c3fc309", "576128045e74e1def180b6888e613789f875534e"],
  ["canonical:c0a70469b4f8bd99dcf60ac0", "57a04d6979b17b10a74a60e49b78f003270fe8f1"],
  ["canonical:d3eefa7932cc62534147aa54", "bded31a16505e32bc99f2e6cc4559c6557b117a2"],
];

const fixedRequests = [
  {
    record_id: "canonical:2dbf9a56c34b27ee243b4a90",
    source: "arxiv_atom",
    url: "https://export.arxiv.org/api/query?id_list=2005.10612",
    file: "2dbf9a56c34b27ee243b4a90.arxiv_atom.xml",
  },
  ...[
    ["canonical:2dbf9a56c34b27ee243b4a90", "10.20380/GI2020.27"],
    ["canonical:7ab9486528e98dd91830ac74", "10.1109/CSSS.2011.5973926"],
    ["canonical:b10ec8cd76027581c1a31b49", "10.1109/PACIFICVIS.2017.8031597"],
  ].flatMap(([recordId, doi]) => [
    {
      record_id: recordId,
      source: "crossref_discovered_doi",
      url: `https://api.crossref.org/works/${encodeURIComponent(doi)}`,
      file: `${recordId.replace("canonical:", "")}.crossref_discovered_doi.json`,
    },
    {
      record_id: recordId,
      source: "semantic_scholar_discovered_doi",
      url: `https://api.semanticscholar.org/graph/v1/paper/DOI:${doi}?fields=paperId,externalIds,title,abstract,year,authors,venue,url`,
      file: `${recordId.replace("canonical:", "")}.semantic_scholar_discovered_doi.json`,
    },
  ]),
  {
    record_id: "canonical:192298dcdffaf27eb19a58b5",
    source: "publisher_doi_html",
    url: "https://www.tandfonline.com/doi/full/10.1080/13632752.2013.819191",
    file: "192298dcdffaf27eb19a58b5.publisher_doi.html",
  },
  {
    record_id: "canonical:d3eefa7932cc62534147aa54",
    source: "institutional_repository_browse_html",
    url: "https://ethesis-old.helsinki.fi/repository/handle/123456789/6/browse?etal=-1&offset=871&order=ASC&rpp=20&sort_by=1&type=title",
    file: "d3eefa7932cc62534147aa54.institutional_repository_browse.html",
  },
];

async function collect(request) {
  const retrievedAt = new Date().toISOString();
  let status = null;
  let bytes = Buffer.alloc(0);
  let error = null;
  try {
    const response = await fetch(request.url, {
      headers: {
        Accept: "application/json, application/atom+xml, text/html;q=0.9, */*;q=0.8",
        "User-Agent": "h2h-lit-pipeline-targeted-metadata-recovery/1.0 (research metadata audit)",
      },
      redirect: "follow",
    });
    status = response.status;
    bytes = Buffer.from(await response.arrayBuffer());
    if (!response.ok) error = `HTTP_${response.status}`;
  } catch (caught) {
    error = caught instanceof Error ? caught.message : String(caught);
  }
  const target = path.join(rawDir, request.file);
  if (bytes.length) await fs.writeFile(target, bytes, { flag: "wx" });
  return {
    record_id: request.record_id,
    source: request.source,
    url: request.url,
    retrieved_at_utc: retrievedAt,
    http_status: status,
    error,
    raw_response: bytes.length ? { path: relative(target), sha256: sha256(bytes), bytes: bytes.length } : null,
  };
}

await fs.access(path.join(internalDir, "retrieval_manifest.json"));
const requests = [];
for (const request of fixedRequests) {
  requests.push(await collect(request));
  await sleep(request.source.includes("semantic_scholar") ? 2500 : 500);
}
for (const [recordId, paperId] of unresolvedSemanticScholar) {
  requests.push(await collect({
    record_id: recordId,
    source: "semantic_scholar_paper_retry",
    url: `https://api.semanticscholar.org/graph/v1/paper/${paperId}?fields=paperId,externalIds,title,abstract,year,authors,venue,url`,
    file: `${recordId.replace("canonical:", "")}.semantic_scholar_paper_retry.json`,
  }));
  await sleep(2500);
}

const manifest = {
  artifact_class: "TARGETED_TITLE_ABSTRACT_METADATA_RETRIEVAL_SUPPLEMENT",
  schema_version: "1.0.0",
  status: "RAW_RESPONSES_COLLECTED_NOT_SCIENTIFICALLY_ADJUDICATED",
  created_at_utc: new Date().toISOString(),
  policy: {
    targeted_identifier_or_exact_record_lookups_only: true,
    full_papers_downloaded: false,
    inference_models_called: false,
  },
  requests,
};
await fs.writeFile(
  path.join(internalDir, "retrieval_supplement_manifest.json"),
  `${JSON.stringify(manifest, null, 2)}\n`,
  { flag: "wx" },
);
console.log(JSON.stringify({
  requests: requests.length,
  successful_responses: requests.filter((item) => item.http_status === 200).length,
  failed_responses: requests.filter((item) => item.http_status !== 200).length,
}, null, 2));
