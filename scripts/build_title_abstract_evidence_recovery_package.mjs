import crypto from "node:crypto";
import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { E6_STATUS, nextActionFormula, outcomeFormula } from "./title_abstract_screening_contract.mjs";

const artifactRequire = createRequire(import.meta.url);
const { SpreadsheetFile, Workbook } = artifactRequire("@oai/artifact-tool");
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const packageDir = path.resolve(process.argv[2] || path.join(repoRoot, "outputs/staging/title-abstract-evidence-recovery-v3-20260922"));
const rawDir = path.join(packageDir, "raw_responses");
const internalDir = path.join(packageDir, "internal");
const previewDir = path.join(internalDir, "workbook_previews");
const archiveDir = path.join(repoRoot, "outputs/staging/title-abstract-screening-v2-return-morris-20260922-v2");
const archiveManifestPath = path.join(archiveDir, "archive_manifest.json");
const recordValidationPath = path.join(archiveDir, "internal/record_validation.json");
const ledgerPath = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920/review_workspaces/primary_100/assignment_ledger.json");
const samplingPath = path.join(repoRoot, "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json");
const protocolPath = path.join(repoRoot, "config/title_abstract_screening_protocol_v2_0_0.json");
const schemaPath = path.join(repoRoot, "config/title_abstract_screening_return_schema_v2_0_0.json");
const investigationPath = path.join(repoRoot, "outputs/title-abstract-screening-approved-v2-20260921/internal/row15_evidence_reconciliation_task.json");
const auditPath = path.join(internalDir, "existing_evidence_audit.json");
const retrievalManifestPath = path.join(internalDir, "retrieval_manifest.json");
const supplementManifestPath = path.join(internalDir, "retrieval_supplement_manifest.json");
const addendumManifestPath = path.join(internalDir, "retrieval_targeted_addendum_manifest.json");
const verifiedEuropePmcManifestPath = path.join(internalDir, "retrieval_verified_europe_pmc_manifest.json");
const expectedSubmissionSha256 = "ab3bd34d544490b85bebfd08bc8f1a86246595aadb4f94a3d94e501280aa7f28";
const conflictId = "canonical:7d25df89fed662b381109a2e";
const evidenceVersion = "title-abstract-evidence-overlay/1.0.0";
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const relative = (target) => path.relative(repoRoot, target);
const jsonBytes = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8");

function normalizeTitle(value) {
  return String(value || "").normalize("NFKD").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function normalizeDoi(value) {
  return String(value || "").trim().toLowerCase().replace(/^https?:\/\/(dx\.)?doi\.org\//, "");
}

function stripMarkup(value) {
  return String(value || "")
    .replace(/<\/?jats:[^>]+>/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

function decodeHtml(value) {
  return String(value || "")
    .replace(/&#13;/g, "\n")
    .replace(/&#(\d+);/g, (_, number) => String.fromCodePoint(Number(number)))
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

async function readJson(target) {
  const bytes = await fs.readFile(target);
  return { bytes, value: JSON.parse(bytes.toString("utf8")) };
}

async function writeOnce(target, bytes) {
  await fs.writeFile(target, bytes);
  return { path: relative(target), sha256: sha256(bytes), bytes: bytes.length };
}

function europePmcAbstract(json, expectedTitle) {
  const candidate = (json.resultList?.result || []).find((item) => normalizeTitle(item.title) === normalizeTitle(expectedTitle));
  if (!candidate?.abstractText) throw new Error(`Europe PMC abstract not found for ${expectedTitle}`);
  return { abstract: stripMarkup(candidate.abstractText), candidate };
}

function crossrefAbstract(json, expectedTitle, expectedDoi) {
  const candidate = json.message;
  const title = Array.isArray(candidate?.title) ? candidate.title[0] : candidate?.title;
  const normalizedReturned = normalizeTitle(title);
  const normalizedExpected = normalizeTitle(expectedTitle);
  const titleMatches = normalizedReturned === normalizedExpected || normalizedReturned.endsWith(normalizedExpected);
  if (!titleMatches || normalizeDoi(candidate?.DOI) !== normalizeDoi(expectedDoi) || !candidate?.abstract) {
    throw new Error(`Crossref identity/abstract mismatch for ${expectedTitle}`);
  }
  return { abstract: stripMarkup(candidate.abstract), candidate };
}

function semanticScholarAbstract(json, expectedTitle, expectedDoi) {
  if (normalizeTitle(json.title) !== normalizeTitle(expectedTitle) || normalizeDoi(json.externalIds?.DOI) !== normalizeDoi(expectedDoi) || !json.abstract) {
    throw new Error(`Semantic Scholar identity/abstract mismatch for ${expectedTitle}`);
  }
  return { abstract: json.abstract.trim(), candidate: json };
}

const recoverySpecs = {
  "canonical:0c69941f511327fc67135444": { source: "europe_pmc_verified_identifier", match: "Exact DOI, title, authors, year, and journal match.", returned: { doi: "10.1016/j.coi.2013.02.006", pmid: "23465465" } },
  "canonical:12ea9e116e6d9c11eae97ebf": { source: "europe_pmc_verified_identifier", match: "Exact PMID and DOI; title, authors, year, and journal match.", returned: { doi: "10.1093/toxsci/kfw198", pmid: "27701119" } },
  "canonical:2dbf9a56c34b27ee243b4a90": { source: "semantic_scholar_discovered_doi", match: "Exact title plus all six authors, year, and venue agree with the arXiv metadata; the Semantic Scholar DOI record supplies DOI 10.20380/GI2020.27 and the same abstract as arXiv 2005.10612.", returned: { doi: "10.20380/GI2020.27", arxiv: "2005.10612" } },
  "canonical:34dca5733dcb284ecc9372a0": { source: "europe_pmc_verified_identifier", match: "Exact PMID; title, both authors, year, and journal match.", returned: { pmid: "2039994" } },
  "canonical:464fc0e8bd6e252aaf643016": { source: "europe_pmc_verified_identifier", match: "Exact DOI; title, first author, year, and IEEE TVCG venue match; Europe PMC supplies PMID.", returned: { doi: "10.1109/tvcg.2011.239", pmid: "22034362" } },
  "canonical:469d6531657790b11bd6c979": { source: "europe_pmc_verified_identifier", match: "Exact DOI; title, all eight authors, year, and Nucleic Acids Research venue match; Europe PMC supplies PMID.", returned: { doi: "10.1093/nar/gkab421", pmid: "34037798" } },
  "canonical:4902f873695a98aeeae10030": { source: "europe_pmc_verified_identifier", match: "Exact PMID and DOI; title, authors, year, and journal match.", returned: { doi: "10.1152/jn.2002.88.4.1695", pmid: "12364499" } },
  "canonical:6adca6827ff48472a25bf396": { source: "europe_pmc_verified_identifier", match: "Exact PMID and DOI; title, all four authors, year, and journal match.", returned: { doi: "10.1078/0171-9335-00188", pmid: "11561904" } },
  "canonical:70ba984fc5ad46d3848e6907": { source: "crossref_doi", match: "Exact DOI; title, authors, year, and Blood venue match.", returned: { doi: "10.1182/blood.v114.22.2186.2186" } },
  "canonical:75b2ab5a7c01f3dc076df621": { source: "europe_pmc_verified_identifier", match: "Exact PMID and DOI; title, authors, year, and journal match.", returned: { doi: "10.1161/circresaha.117.305781", pmid: "26038570" } },
  "canonical:7e8728cbc0b31af0695403d4": { source: "crossref_doi", match: "Exact DOI; title, author, year, and FASEB Journal venue match.", returned: { doi: "10.1096/fasebj.29.1_supplement.498.1" } },
  "canonical:99fc5eea34b1a46262eb9eeb": { source: "europe_pmc_verified_identifier", match: "Exact DOI discovered from the corroborated title result; title, all four authors, year, and Journal of Chemical Information and Modeling venue match; Europe PMC supplies PMID.", returned: { doi: "10.1021/acs.jcim.7b00343", pmid: "29035535" } },
  "canonical:b241a44b95d56693132aaaef": { source: "crossref_doi", match: "Exact DOI; title, authors, year, and Cancer Research venue match.", returned: { doi: "10.1158/1538-7445.compsysbio-b2-35" } },
  "canonical:cf4502e8735bc31a5438b8b1": { source: "europe_pmc_verified_identifier", match: "Exact PMID; title, authors, year, and journal match.", returned: { pmid: "7589282" } },
  "canonical:d3eefa7932cc62534147aa54": { source: "institutional_repository_browse_html", match: "Exact title, author, year, thesis type, institutional handle 10138/18921, and URN fi-fe19991246 match.", returned: { handle: "10138/18921", urn: "URN:NBN:fi-fe19991246" } },
  "canonical:f1920a25ca0c1e7688318f5d": { source: "europe_pmc_verified_identifier", match: "Exact DOI discovered from the corroborated title result; title, all six authors, year, and Lipids in Health and Disease venue match; Europe PMC supplies PMID.", returned: { doi: "10.1186/s12944-019-1032-5", pmid: "30961613" } },
  "canonical:f8ad1043f0af0d43c85be21b": { source: "crossref_doi", match: "Exact DOI; title, authors, year, and Cancer Research venue match.", returned: { doi: "10.1158/1538-7445.am2015-4137" } },
  "canonical:fcdb4bbb190db3b5ff8a19de": { source: "europe_pmc_verified_identifier", match: "Exact PMID and DOI; title, authors, year, and journal match.", returned: { doi: "10.1093/aob/mcy003", pmid: "29394314" } },
};

const sourceFileSuffix = {
  europe_pmc_doi: "europe_pmc_doi.json",
  europe_pmc_pmid: "europe_pmc_pmid.json",
  europe_pmc_title: "europe_pmc_title.json",
  europe_pmc_discovered_doi: "europe_pmc_discovered_doi.json",
  europe_pmc_verified_identifier: "europe_pmc_verified_identifier.json",
  crossref_doi: "crossref_doi.json",
  semantic_scholar_discovered_doi: "semantic_scholar_discovered_doi.json",
  institutional_repository_browse_html: "institutional_repository_browse.html",
};

const archive = await readJson(archiveManifestPath);
if (archive.value.source_submission?.sha256 !== expectedSubmissionSha256) throw new Error("archive submission binding changed");
const archivedSubmissionPath = path.join(repoRoot, archive.value.artifacts.archived_original.path);
if (sha256(await fs.readFile(archivedSubmissionPath)) !== expectedSubmissionSha256) throw new Error("archived submission hash changed");
const validation = await readJson(recordValidationPath);
const audit = await readJson(auditPath);
const retrieval = await readJson(retrievalManifestPath);
const supplement = await readJson(supplementManifestPath);
const addendum = await readJson(addendumManifestPath);
const verifiedEuropePmc = await readJson(verifiedEuropePmcManifestPath);
const frozenLedger = await readJson(ledgerPath);
const sampling = await readJson(samplingPath);
const protocol = await readJson(protocolPath);
const schema = await readJson(schemaPath);
const investigation = await readJson(investigationPath);

for (const [label, pathValue, expected] of [
  ["sampling manifest", samplingPath, archive.value.frozen_bindings.sampling_manifest.sha256],
  ["protocol", protocolPath, archive.value.frozen_bindings.protocol.sha256],
  ["return schema", schemaPath, archive.value.frozen_bindings.return_schema.sha256],
  ["primary ledger", ledgerPath, archive.value.frozen_bindings.primary_ledger.sha256],
]) {
  if (sha256(await fs.readFile(pathValue)) !== expected) throw new Error(`${label} binding changed`);
}
if (sampling.value.frame.registered_dataset.raw_sha256 !== "44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2") throw new Error("registered corpus binding changed");
if (sampling.value.frame.identity_overlay.raw_sha256 !== "c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d") throw new Error("identity overlay binding changed");

const recoveryIds = validation.value.records.filter((record) => record.actionable_next_action === "METADATA_RECOVERY").map((record) => record.record_id);
if (recoveryIds.length !== 31) throw new Error("metadata-recovery scope changed");
const scopedIds = new Set([...recoveryIds, conflictId]);
if (scopedIds.size !== 32 || Object.keys(recoverySpecs).length !== 18) throw new Error("recovery scope/specification count changed");
if (Object.keys(audit.value.canonical_records).length !== 32 || [...scopedIds].some((id) => !audit.value.canonical_records[id])) throw new Error("bounded evidence audit scope changed");

const attempts = [...retrieval.value.requests, ...supplement.value.requests, ...addendum.value.requests, ...verifiedEuropePmc.value.requests];
const attemptsById = new Map([...scopedIds].map((id) => [id, attempts.filter((item) => item.record_id === id)]));
const assignmentById = new Map(frozenLedger.value.assignments.map((item, index) => [item.record_id, { ...item, sample_order: index + 1 }]));
const records = [];
for (const recordId of scopedIds) {
  const assignment = assignmentById.get(recordId);
  if (!assignment) throw new Error(`record is not in frozen primary packet: ${recordId}`);
  const packetPath = path.join(path.dirname(ledgerPath), assignment.packet_path);
  const packetBytes = await fs.readFile(packetPath);
  const packet = JSON.parse(packetBytes.toString("utf8"));
  records.push({
    record_id: recordId,
    sample_order: assignment.sample_order,
    original: packet.record,
    packet: { path: relative(packetPath), sha256: sha256(packetBytes), source_row_sha256: packet.source_row_sha256 },
    canonical: audit.value.canonical_records[recordId],
  });
}
records.sort((a, b) => a.sample_order - b.sample_order);

const retrievalByPath = new Map(attempts.filter((item) => item.raw_response).map((item) => [item.raw_response.path, item]));
const recovered = [];
for (const record of records) {
  const spec = recoverySpecs[record.record_id];
  if (!spec) continue;
  const suffix = sourceFileSuffix[spec.source];
  const sourcePath = path.join(rawDir, `${record.record_id.replace("canonical:", "")}.${suffix}`);
  const sourceBytes = await fs.readFile(sourcePath);
  const sourceRel = relative(sourcePath);
  const request = retrievalByPath.get(sourceRel);
  if (!request || request.http_status !== 200 || request.raw_response.sha256 !== sha256(sourceBytes)) throw new Error(`raw-response binding failed for ${record.record_id}`);
  let abstract;
  let returnedTitle;
  if (spec.source.startsWith("europe_pmc")) {
    const parsed = europePmcAbstract(JSON.parse(sourceBytes), record.original.title);
    abstract = parsed.abstract;
    returnedTitle = parsed.candidate.title;
  } else if (spec.source === "crossref_doi") {
    const expectedDoi = record.canonical.record.doi || record.canonical.record.original_metadata?.externalIds?.DOI;
    const parsed = crossrefAbstract(JSON.parse(sourceBytes), record.original.title, expectedDoi);
    abstract = parsed.abstract;
    returnedTitle = Array.isArray(parsed.candidate.title) ? parsed.candidate.title[0] : parsed.candidate.title;
  } else if (spec.source === "semantic_scholar_discovered_doi") {
    const parsed = semanticScholarAbstract(JSON.parse(sourceBytes), record.original.title, spec.returned.doi);
    abstract = parsed.abstract;
    returnedTitle = parsed.candidate.title;
  } else if (spec.source === "institutional_repository_browse_html") {
    const html = sourceBytes.toString("utf8");
    const titleAt = html.indexOf(`>${record.original.title}</a>`);
    if (titleAt < 0) throw new Error("institutional repository title not found");
    const itemStart = html.lastIndexOf('<li class="ds-artifact-item', titleAt);
    const itemEnd = html.indexOf("</li>", titleAt);
    const item = html.slice(itemStart, itemEnd);
    const match = item.match(/<div class="artifact-abstract">([\s\S]*?)<\/div>/);
    if (!match) throw new Error("institutional repository abstract not found");
    abstract = decodeHtml(match[1]);
    returnedTitle = record.original.title;
  }
  const normalizedReturnedTitle = normalizeTitle(returnedTitle);
  const normalizedOriginalTitle = normalizeTitle(record.original.title);
  if (!abstract || !(normalizedReturnedTitle === normalizedOriginalTitle || normalizedReturnedTitle.endsWith(normalizedOriginalTitle))) throw new Error(`verified abstract extraction failed for ${record.record_id}`);
  recovered.push({
    ...record,
    proposed_abstract: abstract,
    source: {
      source_type: spec.source,
      url: request.url,
      retrieved_at_utc: request.retrieved_at_utc,
      returned_identifiers: spec.returned,
      raw_response: request.raw_response,
      matching_rationale: spec.match,
      transformation: spec.source === "crossref_doi" || spec.source.startsWith("europe_pmc") ? "Provider XML/HTML tags removed and whitespace normalized; text otherwise preserved." : spec.source === "institutional_repository_browse_html" ? "HTML entities decoded and paragraph breaks normalized; text otherwise preserved." : "Provider abstract text preserved with outer whitespace trimmed.",
    },
  });
}
if (recovered.length !== 18) throw new Error(`expected 18 verified recoveries, found ${recovered.length}`);

const conflictRecord = records.find((record) => record.record_id === conflictId);
const conflictDoiAttempt = attemptsById.get(conflictId).find((item) => item.source === "europe_pmc_doi");
const conflictPmidAttempt = attemptsById.get(conflictId).find((item) => item.source === "europe_pmc_pmid");
const conflictCrossrefAttempt = attemptsById.get(conflictId).find((item) => item.source === "crossref_doi");
for (const attempt of [conflictDoiAttempt, conflictPmidAttempt, conflictCrossrefAttempt]) if (!attempt || attempt.http_status !== 200) throw new Error("conflict reconciliation response missing");
const doiResponse = JSON.parse(await fs.readFile(path.join(repoRoot, conflictDoiAttempt.raw_response.path)));
const pmidResponse = JSON.parse(await fs.readFile(path.join(repoRoot, conflictPmidAttempt.raw_response.path)));
const doiCandidate = doiResponse.resultList.result[0];
const pmidCandidate = pmidResponse.resultList.result[0];
if (normalizeDoi(doiCandidate.doi) !== "10.7326/l21-0441" || doiCandidate.pmid !== "34543597" || normalizeTitle(doiCandidate.title) !== normalizeTitle(conflictRecord.original.title)) throw new Error("melanoma identity was not independently confirmed");
if (pmidCandidate.pmid !== "34543596" || normalizeDoi(pmidCandidate.doi) !== "10.7326/l21-0489" || !/sars-cov-2/i.test(pmidCandidate.title)) throw new Error("COVID identity was not independently confirmed");
const originalProviderPath = path.join(repoRoot, investigation.value.original_provider_response.path);
const originalProviderBytes = await fs.readFile(originalProviderPath);
if (sha256(originalProviderBytes) !== investigation.value.original_provider_response.sha256) throw new Error("original provider response changed");

const ledger = records.map((record) => {
  const recovery = recovered.find((item) => item.record_id === record.record_id);
  const recordAttempts = attemptsById.get(record.record_id).map((attempt) => ({
    source: attempt.source,
    url: attempt.url,
    retrieved_at_utc: attempt.retrieved_at_utc,
    http_status: attempt.http_status,
    error: attempt.error,
    raw_response: attempt.raw_response,
  }));
  if (recovery) {
    return {
      record_id: record.record_id,
      original_primary_order: record.sample_order,
      route_scope: "METADATA_RECOVERY",
      status: "VERIFIED_ABSTRACT_RECOVERED",
      original_evidence: { title: record.original.title, abstract: record.original.abstract, publication_year: record.original.publication_year },
      proposed_evidence: { title: record.original.title, abstract: recovery.proposed_abstract, publication_year: record.original.publication_year, evidence_version: evidenceVersion },
      evidence_change: "ADD_VERIFIED_ABSTRACT",
      accepted_source: recovery.source,
      lookup_attempts: recordAttempts,
      scientific_judgment_changed: false,
    };
  }
  if (record.record_id === conflictId) {
    return {
      record_id: record.record_id,
      original_primary_order: record.sample_order,
      route_scope: "EVIDENCE_RECONCILIATION",
      status: "VERIFIED_MIXED_PROVIDER_ENTITY_PROPOSED_CORRECTION_PENDING_USER_REVIEW",
      original_evidence: { title: record.original.title, abstract: record.original.abstract, publication_year: record.original.publication_year, doi: "10.7326/L21-0441", provider_pmid: "34543596", provider_paper_id: "0d3033f6d95b959917c872db86378b4fbd54bfc5" },
      proposed_evidence: { title: record.original.title, abstract: "", publication_year: record.original.publication_year, doi: "10.7326/L21-0441", pmid: "34543597", evidence_version: evidenceVersion },
      evidence_change: "REMOVE_MISMATCHED_ABSTRACT_AND_CORRECT_EVIDENCE_OVERLAY_PMID",
      identity_findings: {
        supported_identity: { title: doiCandidate.title.replace(/\.$/, ""), doi: doiCandidate.doi, pmid: doiCandidate.pmid, authors: doiCandidate.authorString, year: doiCandidate.pubYear, venue: doiCandidate.journalInfo.journal.title, abstract_available: Boolean(doiCandidate.abstractText) },
        conflicting_identity_retained_separately: { title: pmidCandidate.title.replace(/\.$/, ""), doi: pmidCandidate.doi, pmid: pmidCandidate.pmid, authors: pmidCandidate.authorString, year: pmidCandidate.pubYear, venue: pmidCandidate.journalInfo.journal.title, abstract_available: Boolean(pmidCandidate.abstractText) },
        rationale: "The registered DOI, title, authors, year, and venue agree on the melanoma letter and independently resolve to PMID 34543597. The stored provider PMID 34543596 resolves to a different COVID-19 letter with DOI 10.7326/L21-0489; the stored abstract belongs to that conflicting identity. The mixed Semantic Scholar entity is preserved as provider evidence but is not treated as independent support.",
      },
      provenance: {
        doi_identity_response: conflictDoiAttempt,
        pmid_identity_response: conflictPmidAttempt,
        crossref_identity_response: conflictCrossrefAttempt,
        original_provider_response: { path: relative(originalProviderPath), sha256: sha256(originalProviderBytes) },
        existing_provider_investigation: { path: relative(investigationPath), sha256: sha256(investigation.bytes) },
      },
      lookup_attempts: recordAttempts,
      scientific_judgment_changed: false,
      benchmark_scoring_status: "ON_HOLD_PENDING_ENRICHED_EVIDENCE_RE_REVIEW",
    };
  }
  return {
    record_id: record.record_id,
    original_primary_order: record.sample_order,
    route_scope: "METADATA_RECOVERY",
    status: "UNRESOLVED_NO_VERIFIED_ABSTRACT",
    original_evidence: { title: record.original.title, abstract: record.original.abstract, publication_year: record.original.publication_year },
    proposed_evidence: null,
    evidence_change: "NONE",
    lookup_attempts: recordAttempts,
    resolution_note: "No abstract was attached. Identifier-led sources returned no abstract, no sufficiently corroborated record, or an access/rate-limit response. Title-only hits and full-paper-only sources were not used.",
    scientific_judgment_changed: false,
  };
});

const unresolved = ledger.filter((item) => item.status === "UNRESOLVED_NO_VERIFIED_ABSTRACT");
if (ledger.length !== 32 || unresolved.length !== 13) throw new Error("ledger disposition counts changed");
const titleOnlyExclusions = validation.value.records.filter((record) => record.abstract_missing && record.computed_outcome === "EXCLUDED" && !scopedIds.has(record.record_id));
if (titleOnlyExclusions.length !== 2) throw new Error("expected two out-of-scope title-only exclusions");

const overlayRecords = [
  ...recovered.map((record) => ({
    record_id: record.record_id,
    change: "ADD_VERIFIED_ABSTRACT",
    original_evidence: { title: record.original.title, abstract: record.original.abstract, publication_year: record.original.publication_year },
    proposed_evidence: { title: record.original.title, abstract: record.proposed_abstract, publication_year: record.original.publication_year },
    provenance: record.source,
    original_packet: record.packet,
  })),
  {
    record_id: conflictId,
    change: "REMOVE_MISMATCHED_ABSTRACT_AND_CORRECT_EVIDENCE_OVERLAY_PMID",
    original_evidence: ledger.find((item) => item.record_id === conflictId).original_evidence,
    proposed_evidence: ledger.find((item) => item.record_id === conflictId).proposed_evidence,
    identity_findings: ledger.find((item) => item.record_id === conflictId).identity_findings,
    provenance: ledger.find((item) => item.record_id === conflictId).provenance,
    original_packet: conflictRecord.packet,
  },
].sort((a, b) => assignmentById.get(a.record_id).sample_order - assignmentById.get(b.record_id).sample_order);

const overlay = {
  artifact_class: "TITLE_ABSTRACT_EVIDENCE_OVERLAY",
  evidence_version: evidenceVersion,
  schema_version: "1.0.0",
  status: "STAGING_EVIDENCE_OVERLAY_NOT_APPLIED_TO_REGISTERED_CORPUS",
  created_at_utc: new Date().toISOString(),
  bindings: {
    registered_corpus: sampling.value.frame.registered_dataset,
    identity_confirmation_overlay: sampling.value.frame.identity_overlay,
    frozen_sampling_manifest: { path: relative(samplingPath), sha256: sha256(sampling.bytes), frame_sha256: sampling.value.frame.frame_sha256 },
    protocol: { path: relative(protocolPath), sha256: sha256(protocol.bytes), version: protocol.value.protocol_version },
    return_schema: { path: relative(schemaPath), sha256: sha256(schema.bytes), version: protocol.value.return_schema_version },
    archived_return_manifest: { path: relative(archiveManifestPath), sha256: sha256(archive.bytes), artifact_hash: archive.value.artifact_hash },
    archived_submission: { path: relative(archivedSubmissionPath), sha256: expectedSubmissionSha256, reviewer_id: archive.value.reviewer_id },
  },
  policy: {
    registered_corpus_modified: false,
    frozen_sample_membership_modified: false,
    original_packets_or_submissions_modified: false,
    scientific_judgments_recomputed: false,
    overlay_must_be_named_in_future_comparisons: true,
    follow_up_reviews_must_not_be_silently_pooled_with_original_input_reviews: true,
  },
  records: overlayRecords,
};

const changes = {
  artifact_class: "TITLE_ABSTRACT_ORIGINAL_TO_PROPOSED_EVIDENCE_CHANGES",
  evidence_version: evidenceVersion,
  records: overlayRecords.map((item) => ({ record_id: item.record_id, change: item.change, original_evidence: item.original_evidence, proposed_evidence: item.proposed_evidence, provenance: item.provenance })),
};

await fs.mkdir(previewDir, { recursive: true });
const workbookRecords = overlayRecords.map((overlayRecord) => ({
  record_id: overlayRecord.record_id,
  title: overlayRecord.proposed_evidence.title,
  abstract: overlayRecord.proposed_evidence.abstract,
  publication_year: overlayRecord.proposed_evidence.publication_year,
}));
const workbook = Workbook.create();
const instructions = workbook.worksheets.add("Instructions");
const reviews = workbook.worksheets.add("Reviews");
instructions.showGridLines = false;
reviews.showGridLines = false;
const criteria = [
  ["E1", "Life-science application", "Is the reported system applied to analysis, understanding, monitoring, decision-making, discovery, or scientific practice in an in-scope life-science, biomedical, clinical, health, neuroscience, ecology, or related domain?"],
  ["E2", "Relational, derived-structure, or multiscale relevance", "Does the system analyze explicit relationships or relationships derived from spatial, temporal, multivariate, image-derived, lineage, similarity, or other multiscale data?"],
  ["E3", "Interactive visual analytics", "Does a human use an interactive or analytically meaningful visual representation to inspect, understand, validate, steer, compare, interpret, or act on results?"],
  ["E4", "Substantive computational assistance", "Does a nontrivial computational mechanism operate inside the interactive visual-analytics workflow, beyond ordinary rendering or literal direct manipulation?"],
  ["E5", "Human analytic relationship", "Does a human meaningfully inspect, direct, validate, interpret, collaborate with, supervise, correct, or make decisions from the assisted process?"],
  ["E6", "Administrative scope", "Deferred to administrative verification. Do not assess at this title/abstract stage."],
  ["E7", "Evidence sufficiency", "Does the available title/abstract evidence support a defensible determination for E1-E5 at this stage? A NO or UNCERTAIN means unresolved evidence, not scientific exclusion."],
];
const instructionRows = [
  ["Follow-up enriched-evidence title/abstract re-review", "", "", ""],
  ["Approved v2 procedure. This packet contains only records with verified evidence changes and must not be silently pooled with the original-input benchmark.", "", "", ""],
  ["Reviewer ID", "", "", ""],
  ["Assignment role", "Follow-up enriched-evidence re-review", "", ""],
  ["Deadline", "", "", ""],
  ["Protocol version", "2.0.0", "", ""],
  ["Return schema version", "2.0.0", "", ""],
  ["Evidence version", evidenceVersion, "", ""],
  ["How to complete the packet", "", "", ""],
  ["1", "Review only the supplied title and abstract. These records have versioned evidence changes; no previous judgments are included.", "", ""],
  ["2", "Choose YES, NO, or UNCERTAIN for E1-E5 and E7. E6 is fixed as NOT_ASSESSED_AT_THIS_STAGE and must not be changed.", "", ""],
  ["3", "A defensible scientific NO in E1-E5 produces EXCLUDED even when another response is UNCERTAIN. E7=NO or UNCERTAIN produces UNCERTAIN when E1-E5 contain no NO.", "", ""],
  ["4", "INCLUDE requires YES for E1-E5 and E7. It means retain for further assessment, not final paper inclusion.", "", ""],
  ["5", "Explicit title evidence may support an individual criterion; do not infer that a promising title establishes every criterion.", "", ""],
  ["6", "Mark Evidence conflict YES if the enriched title and abstract remain mismatched or otherwise conflicted. Evidence reconciliation takes routing precedence.", "", ""],
  ["7", "Missing abstracts route to metadata recovery before human full-text assessment. Missing abstracts do not automatically make every criterion unknowable.", "", ""],
  ["8", "Keep criterion responses, calculated outcome, and next action separate. Do not replace formulas. Save and return a separate copy.", "", ""],
  ["9", "Do not add model outputs, sampling strata, expected answers, or another reviewer's judgments.", "", ""],
  ["", "", "", ""],
  ["Criterion", "Label", "Question", "Allowed response"],
  ...criteria.map((item) => [item[0], item[1], item[2], item[0] === "E6" ? E6_STATUS : "YES / NO / UNCERTAIN"]),
];
instructions.getRange(`A1:D${instructionRows.length}`).values = instructionRows;
instructions.mergeCells("A1:D1");
instructions.mergeCells("A2:D2");
for (let row = 3; row <= 8; row += 1) instructions.mergeCells(`B${row}:D${row}`);
instructions.mergeCells("A9:D9");
for (let row = 10; row <= 18; row += 1) instructions.mergeCells(`B${row}:D${row}`);
instructions.getRange(`A1:D${instructionRows.length}`).format.font = { name: "Arial", size: 10, color: "#1F1F1F" };
instructions.getRange("A1:D1").format = { fill: "#1F4E78", font: { name: "Arial", size: 18, bold: true, color: "#FFFFFF" } };
instructions.getRange("A2:D2").format = { font: { name: "Arial", size: 10, italic: true, color: "#595959" }, wrapText: true };
instructions.getRange("A3:A8").format = { fill: "#D9EAF7", font: { name: "Arial", size: 10, bold: true } };
instructions.getRange("B3:D8").format = { fill: "#FFF2CC", font: { name: "Arial", size: 10 }, wrapText: true, borders: { preset: "all", style: "thin", color: "#C9B458" } };
instructions.getRange("A9:D9").format = { fill: "#D9EAF7", font: { name: "Arial", size: 10, bold: true } };
instructions.getRange("A10:A18").format = { font: { name: "Arial", size: 10, bold: true, color: "#1F4E78" }, horizontalAlignment: "center" };
instructions.getRange("B10:D18").format.wrapText = true;
instructions.getRange("A20:D20").format = { fill: "#4472C4", font: { name: "Arial", size: 10, bold: true, color: "#FFFFFF" }, wrapText: true, horizontalAlignment: "center", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
instructions.getRange(`A21:D${instructionRows.length}`).format = { font: { name: "Arial", size: 10 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
instructions.getRange(`A21:A${instructionRows.length}`).format.fill = "#D9EAF7";
instructions.getRange("A:A").format.columnWidth = 22;
instructions.getRange("B:B").format.columnWidth = 44;
instructions.getRange("C:C").format.columnWidth = 88;
instructions.getRange("D:D").format.columnWidth = 30;
instructions.getRange("A1:D1").format.rowHeight = 30;
instructions.getRange("A2:D2").format.rowHeight = 44;
instructions.getRange("A8:D8").format.rowHeight = 42;
instructions.getRange("A10:D18").format.rowHeight = 58;
instructions.getRange(`A21:D${instructionRows.length}`).format.rowHeight = 64;
instructions.freezePanes.freezeRows(2);

const headers = ["Stable record ID", "Title", "Abstract", "Year", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "Evidence conflict", "Targeted second review", "Screening outcome", "Next action", "Primary exclusion reason", "Confidence", "Uncertainty / missing evidence", "Notes"];
reviews.getRange("A1:S1").values = [["Follow-up enriched-evidence review records", ...Array(18).fill("")]];
reviews.getRange("A2:S2").values = [[`Evidence version ${evidenceVersion}. Blank re-review fields; approved v2 aggregation and routing apply.`, ...Array(18).fill("")]];
reviews.getRange("A3:S3").values = [headers];
const endRow = workbookRecords.length + 3;
reviews.getRange(`A4:S${endRow}`).values = workbookRecords.map((record) => [record.record_id, record.title, record.abstract, record.publication_year ? Number(record.publication_year) : null, null, null, null, null, null, E6_STATUS, null, null, null, null, null, null, null, null, null]);
for (let row = 4; row <= endRow; row += 1) {
  reviews.getRange(`N${row}`).formulas = [[outcomeFormula(row)]];
  reviews.getRange(`O${row}`).formulas = [[nextActionFormula(row)]];
}
reviews.getRange("A1:S1").format = { fill: "#1F4E78", font: { name: "Arial", size: 16, bold: true, color: "#FFFFFF" } };
reviews.getRange("A2:S2").format = { font: { name: "Arial", size: 10, italic: true, color: "#595959" }, wrapText: true };
reviews.getRange("A3:S3").format = { fill: "#4472C4", font: { name: "Arial", size: 10, bold: true, color: "#FFFFFF" }, wrapText: true, horizontalAlignment: "center", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`A4:D${endRow}`).format = { fill: "#F2F2F2", font: { name: "Arial", size: 9 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`E4:I${endRow}`).format = { fill: "#D9EAF7", font: { name: "Arial", size: 9 }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`J4:J${endRow}`).format = { fill: "#E7E6E6", font: { name: "Arial", size: 8, italic: true, color: "#595959" }, horizontalAlignment: "center", wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`K4:M${endRow}`).format = { fill: "#D9EAF7", font: { name: "Arial", size: 9 }, horizontalAlignment: "center", verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`N4:O${endRow}`).format = { fill: "#E2F0D9", font: { name: "Arial", size: 9 }, horizontalAlignment: "center", wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`P4:S${endRow}`).format = { fill: "#D9EAF7", font: { name: "Arial", size: 9 }, wrapText: true, verticalAlignment: "top", borders: { preset: "all", style: "thin", color: "#D9E2F3" } };
reviews.getRange(`E4:I${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] } };
reviews.getRange(`J4:J${endRow}`).dataValidation = { rule: { type: "list", values: [E6_STATUS] } };
reviews.getRange(`K4:K${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] } };
reviews.getRange(`L4:M${endRow}`).dataValidation = { rule: { type: "list", values: ["YES", "NO"] } };
reviews.getRange(`Q4:Q${endRow}`).dataValidation = { rule: { type: "list", values: ["LOW", "MEDIUM", "HIGH"] } };
for (const [column, width] of Object.entries({ A: 30, B: 48, C: 100, D: 10, E: 9, F: 9, G: 9, H: 9, I: 9, J: 29, K: 9, L: 18, M: 20, N: 18, O: 25, P: 38, Q: 14, R: 42, S: 42 })) reviews.getRange(`${column}:${column}`).format.columnWidth = width;
reviews.getRange("A1:S1").format.rowHeight = 28;
reviews.getRange("A2:S2").format.rowHeight = 44;
reviews.getRange("A3:S3").format.rowHeight = 52;
reviews.getRange(`A4:S${endRow}`).format.rowHeight = 86;
reviews.freezePanes.freezeRows(3);
reviews.freezePanes.freezeColumns(4);
reviews.tables.add(`A3:S${endRow}`, true, "FollowupEnrichedEvidenceReviews").style = "TableStyleMedium2";

const inspect = await workbook.inspect({ kind: "table", range: `Reviews!A1:S${Math.min(endRow, 10)}`, include: "values,formulas", tableMaxRows: 10, tableMaxCols: 19, summary: "follow-up review workbook layout and formulas" });
const errors = await workbook.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!", options: { useRegex: true, maxResults: 100 }, summary: "formula error scan" });
const instructionPng = await workbook.render({ sheetName: "Instructions", range: "A1:D27", scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "followup_instructions.png"), new Uint8Array(await instructionPng.arrayBuffer()));
const reviewsPng = await workbook.render({ sheetName: "Reviews", range: `A1:S${Math.min(endRow, 12)}`, scale: 1, format: "png" });
await fs.writeFile(path.join(previewDir, "followup_reviews.png"), new Uint8Array(await reviewsPng.arrayBuffer()));
const workbookPath = path.join(packageDir, "followup_enriched_evidence_re_review_APPROVED_v2.xlsx");
const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(workbookPath);
const workbookBytes = await fs.readFile(workbookPath);
const saved = await SpreadsheetFile.importXlsx(workbookBytes);
const savedReviews = saved.worksheets.getItem("Reviews");
const savedIds = savedReviews.getRange(`A4:A${endRow}`).values.flat();
const savedEvidence = savedReviews.getRange(`B4:D${endRow}`).values.map((row) => [row[0], row[1] ?? "", row[2]]);
const savedE6 = savedReviews.getRange(`J4:J${endRow}`).values.flat();
const savedJudgments = savedReviews.getRange(`E4:I${endRow}`).values.flat().concat(savedReviews.getRange(`K4:M${endRow}`).values.flat()).filter((value) => value !== "" && value !== null && value !== undefined);
const savedFormulas = savedReviews.getRange(`N4:O${endRow}`).formulas;
if (JSON.stringify(savedIds) !== JSON.stringify(workbookRecords.map((item) => item.record_id))) throw new Error("saved workbook record order changed");
if (JSON.stringify(savedEvidence) !== JSON.stringify(workbookRecords.map((item) => [item.title, item.abstract, item.publication_year ? Number(item.publication_year) : null]))) throw new Error("saved workbook evidence changed");
if (savedE6.some((value) => value !== E6_STATUS) || savedJudgments.length) throw new Error("saved workbook is not a blank approved-v2 return");
if (savedFormulas.some((row, index) => !row[0].includes(`E${index + 4}:I${index + 4}`) || row[0].includes(`J${index + 4}`) || !row[0].includes(`K${index + 4}`))) throw new Error("saved outcome formula binding failed");
const savedErrors = await saved.inspect({ kind: "match", searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!", options: { useRegex: true, maxResults: 100 }, summary: "post-save formula error scan" });
await fs.writeFile(path.join(internalDir, "followup_workbook.inspect.ndjson"), `${inspect.ndjson}\n${errors.ndjson}\n${savedErrors.ndjson}\n`);

const ledgerArtifact = {
  artifact_class: "TITLE_ABSTRACT_METADATA_RECOVERY_LEDGER",
  evidence_version: evidenceVersion,
  schema_version: "1.0.0",
  scope: { metadata_recovery: 31, evidence_reconciliation: 1, total: 32 },
  counts: { verified_abstract_recoveries: recovered.length, unresolved_metadata_recovery: unresolved.length, verified_mixed_entity_reconciliation: 1, follow_up_re_review_records: workbookRecords.length },
  records: ledger,
};
const ledgerRef = await writeOnce(path.join(packageDir, "recovery_ledger.json"), jsonBytes(ledgerArtifact));
const changesRef = await writeOnce(path.join(packageDir, "original_to_proposed_evidence_changes.json"), jsonBytes(changes));
const overlayRef = await writeOnce(path.join(packageDir, "evidence_overlay.json"), jsonBytes(overlay));

const recoveredLines = ledger.filter((item) => item.status === "VERIFIED_ABSTRACT_RECOVERED").map((item) => `- ${item.record_id} — ${item.original_evidence.title}`).join("\n");
const unresolvedLines = unresolved.map((item) => `- ${item.record_id} — ${item.original_evidence.title}`).join("\n");
const report = `# Targeted title/abstract evidence recovery report\n\n` +
  `Evidence version: \`${evidenceVersion}\`  \nStatus: staging overlay; not applied to the registered corpus or production screening state.\n\n` +
  `## Results\n\n` +
  `- Scoped records: 32 (31 metadata recovery; 1 evidence reconciliation).\n` +
  `- Verified abstract recoveries: ${recovered.length}.\n` +
  `- Unresolved metadata-recovery records: ${unresolved.length}.\n` +
  `- Verified mixed-provider evidence reconciliation: 1, pending user approval and blank enriched-evidence re-review.\n` +
  `- Follow-up workbook records: ${workbookRecords.length}. No previous judgments, model outputs, sampling strata, or expected answers are present.\n` +
  `- Two title-only exclusions remain outside this recovery scope and unchanged: ${titleOnlyExclusions.map((item) => item.record_id).join(", ")}.\n\n` +
  `## Known mismatch\n\n` +
  `For \`${conflictId}\`, DOI \`10.7326/L21-0441\`, the title, authors, year, and venue resolve to the melanoma letter and PMID \`34543597\`. The stored provider PMID \`34543596\` resolves to a different COVID-19 letter, DOI \`10.7326/L21-0489\`; the stored abstract belongs to that conflicting identity. The original mixed provider response is preserved. The overlay proposes retaining the DOI/title identity, removing the mismatched abstract, and recording PMID 34543597 only in this evidence version. The original Morris judgment is unchanged and remains unavailable for benchmark scoring until re-review.\n\n` +
  `## Verified recoveries\n\n${recoveredLines}\n\n` +
  `## Unresolved records\n\n${unresolvedLines}\n\n` +
  `## Limitations and safeguards\n\n` +
  `- Saved corpus occurrences contained no alternate abstract for the 31 metadata-recovery records.\n` +
  `- Title-query results were accepted only with corroborating authors/year/venue and, where returned, DOI/PMID. Title alone was never sufficient.\n` +
  `- Semantic Scholar rate-limited several requests; those failures are retained in the ledger. No abstract was inferred from an unavailable response.\n` +
  `- No full papers were downloaded. Abstracts visible only in full-paper sources or unverified third-party snippets were not used.\n` +
  `- No scientific outcome or routing judgment was recalculated. Follow-up results must name this evidence version and must not be silently pooled with the original-input benchmark.\n`;
const reportRef = await writeOnce(path.join(packageDir, "recovery_report.md"), Buffer.from(report, "utf8"));
const workbookRef = { path: relative(workbookPath), sha256: sha256(workbookBytes), bytes: workbookBytes.length };

const packageManifest = {
  artifact_class: "TITLE_ABSTRACT_EVIDENCE_RECOVERY_PACKAGE",
  evidence_version: evidenceVersion,
  schema_version: "1.0.0",
  status: "STAGING_COMPLETE_NOT_APPLIED_TO_REGISTERED_CORPUS",
  created_at_utc: new Date().toISOString(),
  bindings: overlay.bindings,
  counts: ledgerArtifact.counts,
  artifacts: {
    recovery_ledger: ledgerRef,
    original_to_proposed_changes: changesRef,
    evidence_overlay: overlayRef,
    follow_up_re_review_workbook: workbookRef,
    report: reportRef,
    retrieval_manifest: { path: relative(retrievalManifestPath), sha256: sha256(retrieval.bytes) },
    retrieval_supplement_manifest: { path: relative(supplementManifestPath), sha256: sha256(supplement.bytes) },
    retrieval_targeted_addendum_manifest: { path: relative(addendumManifestPath), sha256: sha256(addendum.bytes) },
    retrieval_verified_europe_pmc_manifest: { path: relative(verifiedEuropePmcManifestPath), sha256: sha256(verifiedEuropePmc.bytes) },
    bounded_existing_evidence_audit: { path: relative(auditPath), sha256: sha256(audit.bytes) },
  },
  state_effects: {
    registered_corpus_modified: false,
    identity_assignments_modified: false,
    frozen_sample_modified: false,
    screening_decisions_modified: false,
    production_state_modified: false,
    prisma_modified: false,
    identification_closed: false,
    full_papers_downloaded: false,
    inference_models_called: false,
  },
};
packageManifest.artifact_hash = sha256(jsonBytes(packageManifest));
const manifestRef = await writeOnce(path.join(packageDir, "package_manifest.json"), jsonBytes(packageManifest));
console.log(JSON.stringify({
  package_dir: relative(packageDir),
  workbook: workbookRef,
  manifest: manifestRef,
  counts: ledgerArtifact.counts,
  title_only_exclusions_unchanged: titleOnlyExclusions.map((item) => item.record_id),
}, null, 2));
