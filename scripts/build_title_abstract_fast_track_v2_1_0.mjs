import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { TextDecoder } from "node:util";
import { fileURLToPath } from "node:url";

import {
  E6_STATUS,
  ASSESSED_CRITERIA,
  outcomeFormula,
  nextActionFormula,
  recomputeOutcome,
  recommendNextAction,
  recommendFastTrackDisposition,
} from "./title_abstract_screening_contract.mjs";

const artifactRequire = createRequire(import.meta.url);
const { FileBlob, SpreadsheetFile, Workbook } = artifactRequire("@oai/artifact-tool");

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outputDir = path.resolve(
  process.argv[2] || path.join(repoRoot, "outputs/staging/title-abstract-fast-track-v2-1-0-20260922"),
);
const completedPath = path.join(
  repoRoot,
  "outputs/staging/human-validation-review-workspace-v1-20260920T230139Z/followup_enriched_evidence_re_review_APPROVED_v2.xlsx",
);
const recoveryDir = path.join(repoRoot, "outputs/staging/title-abstract-evidence-recovery-v3-20260922");
const originalArchiveDir = path.join(
  repoRoot,
  "outputs/staging/title-abstract-screening-v2-return-morris-20260922-v2",
);
const originalWorkbookPath = path.join(
  originalArchiveDir,
  "submitted_evidence/primary_morris_100_records_APPROVED_v2_Finished.xlsx",
);
const originalValidationPath = path.join(originalArchiveDir, "internal/record_validation.json");
const supportedFollowupValidationPath = "/private/tmp/followup_fasttrack_validation.json";
const samplingPath = path.join(
  repoRoot,
  "outputs/title-abstract-benchmark-v1-20260920/internal/sampling_manifest.json",
);
const amendmentPath = path.join(
  repoRoot,
  "config/title_abstract_screening_fast_track_amendment_v2_1_0.json",
);
const protocolPath = path.join(repoRoot, "config/title_abstract_screening_protocol_v2_0_0.json");
const returnSchemaPath = path.join(
  repoRoot,
  "config/title_abstract_screening_return_schema_v2_0_0.json",
);
const datasetPath = path.join(
  repoRoot,
  "outputs/production/star-external-retrieval-wave-001/execution/GlobalIdentificationMerge/v1/review_dataset.json",
);
const overlayPath = path.join(
  repoRoot,
  "outputs/production/star-external-retrieval-wave-001/execution/PriorSurveyIdentityAdjudication/v1/identity_confirmation_overlay.json",
);
const recoveryManifestPath = path.join(recoveryDir, "package_manifest.json");
const evidenceOverlayPath = path.join(recoveryDir, "evidence_overlay.json");
const recoveryLedgerPath = path.join(recoveryDir, "recovery_ledger.json");

const expected = {
  completedSha256: "4c55692793cae8551c2db29228be5c3a78bcf074d4de30c8acab26465bdbce1b",
  blankSha256: "77ee532cb7e8e761356a1f2b64c996e1f15432167444b6a7e0a40c297f37093c",
  originalSubmissionSha256: "ab3bd34d544490b85bebfd08bc8f1a86246595aadb4f94a3d94e501280aa7f28",
  datasetSha256: "44e4187fae472ff349c9a8bc3fd48df0d1263f05674068445d0fb80d9e2415b2",
  overlaySha256: "c287b6b4548ec4a7d0b09fafd84b93d123426d7010032611495dab9a41d1251d",
  samplingSha256: "a2fd6e54c6a955eadbd4ea63d9996b9bbb8ec29c65024a733eefdd5f13747249",
  protocolSha256: "e0fe8b63e33a02645405bc5973adef151f4fd35f79f8411d35c45031ba19e4ea",
  returnSchemaSha256: "4bfeffd0f6328cc2a8845a86e3248f22f70ce499c562c17077b7325c0582ab15",
  frameSha256: "9b92915c94736e6fcfa3211435ee2aa70a973279328f49356a848faafbe7ff86",
};

const nextBatchSize = 250;
const nextBatchSeed = "h2h-title-abstract-fast-track-next-batch-v1";
const evidenceVersion = "title-abstract-evidence-overlay/1.0.0";
const amendmentVersion = "2.1.0";
const sha256 = (bytes) => crypto.createHash("sha256").update(bytes).digest("hex");
const rel = (target) => path.relative(repoRoot, target).replaceAll(path.sep, "/");
const jsonBytes = (value) => Buffer.from(`${JSON.stringify(value, null, 2)}\n`, "utf8");
const readJson = async (target) => JSON.parse(await fsp.readFile(target, "utf8"));
const compact = (value) => String(value ?? "").trim();
const countBy = (records, key) => {
  const result = {};
  for (const record of records) result[record[key]] = (result[record[key]] || 0) + 1;
  return result;
};
const completeCounts = (counts, keys) => Object.fromEntries(keys.map((key) => [key, counts[key] || 0]));

async function assertFreshDirectory(target) {
  try {
    await fsp.access(target);
  } catch {
    await fsp.mkdir(target, { recursive: false });
    return;
  }
  throw new Error(`output directory already exists: ${target}`);
}

async function fileReference(target) {
  const bytes = await fsp.readFile(target);
  return { path: rel(target), sha256: sha256(bytes), bytes: bytes.length };
}

function rankHex(canonicalId) {
  return crypto.createHash("sha256").update(`${nextBatchSeed}\u001f${canonicalId}`).digest("hex");
}

function boundedRankAdd(items, record) {
  if (items.length < nextBatchSize) {
    items.push(record);
    return;
  }
  let worstIndex = 0;
  for (let index = 1; index < items.length; index += 1) {
    if (items[index].selection_rank_sha256 > items[worstIndex].selection_rank_sha256) worstIndex = index;
  }
  if (record.selection_rank_sha256 < items[worstIndex].selection_rank_sha256) items[worstIndex] = record;
}

async function streamCanonicalRecords(target, excludedIds, candidateIds) {
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
  const candidates = new Map();
  const nextBatch = [];

  const accept = (raw) => {
    const canonicalId = compact(raw.canonical_id);
    count += 1;
    if (candidateIds.has(canonicalId)) candidates.set(canonicalId, raw);
    if (excludedIds.has(canonicalId)) return;
    const record = raw.record || {};
    boundedRankAdd(nextBatch, {
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

  const consume = (text) => {
    let start = 0;
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
    for (let index = start; index < text.length; index += 1) {
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
    sha256: digest.digest("hex"),
    count,
    candidates,
    nextBatch: nextBatch.sort((a, b) => a.selection_rank_sha256.localeCompare(b.selection_rank_sha256)),
  };
}

async function workbookRows(target) {
  const workbook = await SpreadsheetFile.importXlsx(await FileBlob.load(target));
  const instructions = workbook.worksheets.getItem("Instructions");
  const instructionValues = Object.fromEntries(
    instructions.getRange("A1:B30").values
      .filter((row) => compact(row[0]))
      .map((row) => [compact(row[0]), compact(row[1])]),
  );
  const sheet = workbook.worksheets.getItem("Reviews");
  const headers = sheet.getRange("A3:S3").values[0].map(compact);
  const values = sheet.getRange("A4:S200").values;
  const formulas = sheet.getRange("A4:S200").formulas;
  const rows = [];
  for (let index = 0; index < values.length; index += 1) {
    if (!compact(values[index][0])) continue;
    const row = Object.fromEntries(headers.map((header, column) => [header, values[index][column]]));
    row.worksheet_row = index + 4;
    row.outcome_formula = formulas[index][13];
    row.next_action_formula = formulas[index][14];
    rows.push(row);
  }
  return { instructionValues, rows };
}

function criteria(row) {
  return Object.fromEntries(ASSESSED_CRITERIA.map((criterion) => [criterion, compact(row[criterion])]));
}

function validateFollowup(followup, evidenceOverlay, supportedValidation) {
  if (followup.instructionValues["Reviewer ID"] !== "Morris") throw new Error("follow-up reviewer ID changed");
  if (followup.instructionValues["Protocol version"] !== "2.0.0") throw new Error("follow-up protocol changed");
  if (followup.instructionValues["Return schema version"] !== "2.0.0") throw new Error("follow-up schema changed");
  if (followup.instructionValues["Evidence version"] !== evidenceVersion) throw new Error("follow-up evidence version changed");
  if (followup.rows.length !== 19 || evidenceOverlay.records.length !== 19) throw new Error("follow-up scope changed");
  if (supportedValidation.records !== 19 || supportedValidation.reviewer_id !== "Morris") {
    throw new Error("supported v2 validation did not confirm the 19-row Morris return");
  }
  const validationById = new Map(supportedValidation.rows.map((row) => [row.record_id, row]));
  for (let index = 0; index < followup.rows.length; index += 1) {
    const row = followup.rows[index];
    const overlay = evidenceOverlay.records[index];
    const recordId = compact(row["Stable record ID"]);
    if (recordId !== overlay.record_id) throw new Error(`follow-up order/binding mismatch at ${recordId}`);
    if (compact(row.Title) !== compact(overlay.proposed_evidence.title)) throw new Error(`follow-up title mismatch for ${recordId}`);
    if (compact(row.Abstract) !== compact(overlay.proposed_evidence.abstract)) throw new Error(`follow-up abstract mismatch for ${recordId}`);
    if (compact(row.Year) !== compact(overlay.proposed_evidence.publication_year)) throw new Error(`follow-up year mismatch for ${recordId}`);
    if (compact(row.E6) !== E6_STATUS) throw new Error(`follow-up E6 changed for ${recordId}`);
    const computedOutcome = recomputeOutcome(criteria(row), compact(row.E6));
    const computedAction = recommendNextAction(criteria(row), {
      abstractMissing: !compact(row.Abstract),
      evidenceConflict: compact(row["Evidence conflict"]) === "YES",
      targetedSecondReviewRequested: compact(row["Targeted second review"]) === "YES",
    });
    if (compact(row["Screening outcome"]) !== computedOutcome) throw new Error(`stale outcome cache for ${recordId}`);
    if (compact(row["Next action"]) !== computedAction) throw new Error(`stale action cache for ${recordId}`);
    // Excel stores the filled-down columns as shared formulas: only the anchor
    // row carries formula text while following cells carry cached results.
    if (row.outcome_formula && row.outcome_formula !== outcomeFormula(row.worksheet_row)) {
      throw new Error(`outcome formula changed for ${recordId}`);
    }
    if (row.next_action_formula && row.next_action_formula !== nextActionFormula(row.worksheet_row)) {
      throw new Error(`next-action formula changed for ${recordId}`);
    }
    const supported = validationById.get(recordId);
    if (!supported || supported.computed_outcome !== computedOutcome || supported.computed_next_action !== computedAction) {
      throw new Error(`supported validator mismatch for ${recordId}`);
    }
  }
  if (followup.rows[0].outcome_formula !== outcomeFormula(followup.rows[0].worksheet_row)) {
    throw new Error("shared outcome formula anchor changed");
  }
  if (followup.rows[0].next_action_formula !== nextActionFormula(followup.rows[0].worksheet_row)) {
    throw new Error("shared next-action formula anchor changed");
  }
}

function effectiveRecords(originalValidation, originalRows, followupRows, sampling, unresolvedIds) {
  const originalRowById = new Map(originalRows.map((row) => [compact(row["Stable record ID"]), row]));
  const followupById = new Map(followupRows.map((row) => [compact(row["Stable record ID"]), row]));
  const sampleById = new Map(sampling.selected_records.map((item) => [item.canonical_id, item]));
  return originalValidation.records.map((original) => {
    const originalRow = originalRowById.get(original.record_id);
    const followup = followupById.get(original.record_id);
    const source = followup || originalRow;
    const scientificOutcome = recomputeOutcome(criteria(source), compact(source.E6));
    const evidenceConflict = compact(source["Evidence conflict"]) === "YES";
    const abstractMissing = !compact(source.Abstract);
    const unresolvedMetadata = unresolvedIds.has(original.record_id);
    return {
      sample_order: original.worksheet_row - 3,
      record_id: original.record_id,
      title: compact(source.Title),
      publication_year: source.Year ?? null,
      selection_group: sampleById.get(original.record_id)?.selection_group || original.selection_group,
      challenge_stratum: sampleById.get(original.record_id)?.challenge_stratum || null,
      scientific_outcome: scientificOutcome,
      operational_disposition_pre_full_report_check:
        scientificOutcome === "EXCLUDED" ? "EXCLUDE" : scientificOutcome === "UNCERTAIN" ? "DEFER" : "PENDING_FULL_REPORT_CHECK",
      disposition_reason:
        scientificOutcome === "EXCLUDED"
          ? compact(source["Primary exclusion reason"]) || "SCIENTIFIC_NO"
          : scientificOutcome === "UNCERTAIN"
            ? unresolvedMetadata
              ? "UNRESOLVED_METADATA_RECOVERY_TIME_BOUNDED_DEFER"
              : evidenceConflict
                ? "EVIDENCE_CONFLICT"
                : "INSUFFICIENT_TITLE_ABSTRACT_EVIDENCE"
            : "PENDING_BOUNDED_FULL_REPORT_AVAILABILITY_CHECK",
      criteria: criteria(source),
      e6_status: compact(source.E6),
      abstract_missing: abstractMissing,
      evidence_conflict: evidenceConflict,
      supporting_evidence: compact(source["Uncertainty / missing evidence"]),
      primary_exclusion_reason: compact(source["Primary exclusion reason"]) || null,
      evidence_version: followup ? evidenceVersion : "original-frozen-title-abstract-evidence",
      judgment_source: followup ? "completed_19_record_enriched_evidence_follow_up" : "archived_original_100_record_return",
      supersedes: followup
        ? {
            judgment: `${rel(originalWorkbookPath)}#Reviews!row=${original.worksheet_row}`,
            evidence: "original-frozen-title-abstract-evidence",
          }
        : null,
      original_protocol_version: "2.0.0",
      original_return_schema_version: "2.0.0",
      fast_track_amendment_applied_prospectively: amendmentVersion,
    };
  });
}

const retrievalChecks = {
  "canonical:469d6531657790b11bd6c979": {
    doi: "10.1093/nar/gkab421",
    availability: "VERIFIED_PUBLIC_FULL_REPORT",
    available: true,
    links: ["https://pmc.ncbi.nlm.nih.gov/articles/PMC8262702/", "https://doi.org/10.1093/nar/gkab421"],
    local: { source: "/private/tmp/fasttrack_PMC8262702.xml", name: "canonical-469d6531657790b11bd6c979.PMC8262702.fullTextXML.xml" },
    rationale: "Exact title and DOI in Europe PMC full-text XML; PMC8262702.",
  },
  "canonical:8e765570e365380d9b08a0fe": {
    doi: "10.1109/2945.537306",
    availability: "FULL_REPORT_NOT_LOCATED_WITH_RIGHTS_CLEAR_ACCESS_IN_BOUNDED_CHECK",
    available: false,
    links: ["https://doi.org/10.1109/2945.537306"],
    rationale: "DOI and bibliographic identity verified; no rights-clear public full report was located in the bounded identifier-led check.",
  },
  "canonical:a802becf7745d7b8d813fe2e": {
    doi: "10.1109/TVCG.2010.244",
    availability: "VERIFIED_PUBLIC_AUTHOR_OR_INSTITUTIONAL_COPY",
    available: true,
    links: ["https://publications.graphics.tudelft.nl/papers/488", "https://doi.org/10.1109/TVCG.2010.244"],
    local: { source: "/private/tmp/fasttrack_fused_dti_hardi.pdf", name: "canonical-a802becf7745d7b8d813fe2e.fused-dti-hardi.pdf" },
    rationale: "TU Delft publication page and public PDF match title, authors, venue, year, and DOI.",
  },
  "canonical:e7dc20de3bf30f187d247da5": {
    doi: "10.1109/TVCG.2023.3286582",
    availability: "VERIFIED_PUBLIC_AUTHOR_MANUSCRIPT_AND_PMC_REPORT",
    available: true,
    links: ["https://pmc.ncbi.nlm.nih.gov/articles/PMC11273209/", "https://doi.org/10.1109/TVCG.2023.3286582"],
    local: { source: "/private/tmp/fasttrack_visual_environment.pdf", name: "canonical-e7dc20de3bf30f187d247da5.author-manuscript.pdf" },
    rationale: "Public University of Utah author manuscript and PMC record match title, authors, and DOI.",
  },
  "canonical:22014b3dc828c9a16ad4d4d3": {
    doi: "10.1002/advs.202405395",
    availability: "VERIFIED_PUBLIC_FULL_REPORT",
    available: true,
    links: ["https://pmc.ncbi.nlm.nih.gov/articles/PMC11600262/", "https://doi.org/10.1002/advs.202405395"],
    local: { source: "/private/tmp/fasttrack_PMC11600262.xml", name: "canonical-22014b3dc828c9a16ad4d4d3.PMC11600262.fullTextXML.xml" },
    rationale: "Exact title and DOI in Europe PMC full-text XML; PMC11600262.",
  },
  "canonical:b241a44b95d56693132aaaef": {
    doi: "10.1158/1538-7445.compsysbio-b2-35",
    availability: "VERIFIED_RELATED_FULL_RESEARCH_REPORT",
    available: true,
    links: [
      "https://doi.org/10.1158/1538-7445.compsysbio-b2-35",
      "https://pmc.ncbi.nlm.nih.gov/articles/PMC4551906/",
      "https://doi.org/10.1093/nar/gkv413",
    ],
    local: { source: "/private/tmp/fasttrack_PMC4551906.xml", name: "related-full-research.PMC4551906.fullTextXML.xml" },
    rationale: "The sampled identity is an AACR conference abstract. A distinct full research paper with the same authors, method, and CancerLandscapes system is publicly available as PMC4551906; the relationship requires E6/document-version review.",
    related_version: {
      relationship: "CONFERENCE_ABSTRACT_TO_CORRESPONDING_FULL_RESEARCH_PAPER",
      sampled_identity_title: "Efficient exploration of multi-cancer networks by generalized covariance selection and interactive web content",
      sampled_identity_doi: "10.1158/1538-7445.compsysbio-b2-35",
      related_title: "Efficient exploration of pan-cancer networks by generalized covariance selection and interactive web content",
      related_doi: "10.1093/nar/gkv413",
      related_pmcid: "PMC4551906",
      identities_merged: false,
    },
  },
};

async function copyFullReports(targetDir) {
  await fsp.mkdir(targetDir, { recursive: true });
  const result = {};
  for (const [recordId, check] of Object.entries(retrievalChecks)) {
    if (!check.local) continue;
    const bytes = await fsp.readFile(check.local.source);
    if (check.local.name.endsWith(".pdf") && bytes.subarray(0, 5).toString("ascii") !== "%PDF-") {
      throw new Error(`download is not a PDF for ${recordId}`);
    }
    if (check.local.name.endsWith(".xml")) {
      const prefix = bytes.toString("utf8", 0, 300);
      if (!prefix.includes("<?xml") && !prefix.includes("<!DOCTYPE article") && !prefix.includes("<article")) {
        throw new Error(`download is not JATS XML/SGML for ${recordId}`);
      }
    }
    const target = path.join(targetDir, check.local.name);
    await fsp.writeFile(target, bytes, { flag: "wx" });
    result[recordId] = await fileReference(target);
  }
  return result;
}

function candidateRows(effective, canonicalRecords, localReports) {
  return effective
    .filter((record) => record.scientific_outcome === "INCLUDE")
    .sort((a, b) => a.sample_order - b.sample_order)
    .map((record, index) => {
      const canonical = canonicalRecords.get(record.record_id)?.record || {};
      const check = retrievalChecks[record.record_id];
      if (!check) throw new Error(`missing bounded full-report check for ${record.record_id}`);
      const disposition = recommendFastTrackDisposition(record.criteria, {
        e6Status: record.e6_status,
        abstractMissing: record.abstract_missing,
        evidenceConflict: record.evidence_conflict,
        fullReportAvailable: check.available,
      });
      return {
        queue_order: index + 1,
        sample_order: record.sample_order,
        record_id: record.record_id,
        title: record.title,
        doi: check.doi || compact(canonical.doi) || null,
        existing_source_links: [...new Set([
          ...check.links,
          compact(canonical.source_url),
          compact(canonical.pdf_url),
        ].filter(Boolean))],
        evidence_version: record.evidence_version,
        supporting_scientific_evidence: record.supporting_evidence,
        scientific_outcome: record.scientific_outcome,
        full_report_availability: check.availability,
        local_full_report: localReports[record.record_id]?.path || null,
        e6_checks_still_needed: check.related_version
          ? "Verify retrieval cutoff, language, report type, and whether the conference abstract or distinct full research paper is the eligible bibliographic unit."
          : "Verify retrieval cutoff, language, full-report document type, and administrative scope; E6 is not yet assessed.",
        related_versions: check.related_version || null,
        fast_track_disposition: disposition,
        defer_reason: disposition === "DEFER" ? "FULL_REPORT_UNAVAILABLE_IN_BOUNDED_PUBLIC_CHECK" : null,
        availability_rationale: check.rationale,
        final_scientific_eligibility: "",
        e6_administrative_verification: "",
        final_paper_eligibility: "",
        synthesis_role_or_code: "",
        assistance_code: "",
        visualization_modality: "",
        task_or_analytic_focus: "",
        assessment_notes: "",
      };
    });
}

async function buildQueueWorkbook(rows, target, previewDir) {
  const workbook = Workbook.create();
  const instructions = workbook.worksheets.add("Instructions");
  const queue = workbook.worksheets.add("Assessment queue");
  instructions.showGridLines = false;
  queue.showGridLines = false;

  instructions.getRange("A1:D1").values = [["Fast-track full-report assessment and synthesis queue", null, null, null]];
  instructions.getRange("A2:D2").values = [["Staging-only queue under amendment 2.1.0. Scientific outcome, operational disposition, E6 verification, final eligibility, and synthesis coding remain distinct.", null, null, null]];
  instructions.getRange("A4:B12").values = [
    ["Amendment version", amendmentVersion],
    ["Base protocol / return schema", "2.0.0 / 2.0.0"],
    ["Evidence versions", "Original frozen evidence plus title-abstract-evidence-overlay/1.0.0 where explicitly shown"],
    ["Scope", "Six effective pilot INCLUDE candidates only"],
    ["E6", E6_STATUS],
    ["INCLUDE meaning", "Retained for further assessment; not final inclusion"],
    ["DEFER meaning", "Workflow disposition, not exclusion"],
    ["Missing/unavailable evidence", "Do not infer or fabricate; retain reason and evidence"],
    ["CancerLandscapes", "Conference abstract and corresponding full research paper remain distinct identities"],
  ];
  instructions.getRange("A14:B20").values = [
    ["Field", "Completion guidance"],
    ["Final scientific eligibility", "Blank input: YES / NO / UNCERTAIN after full-report assessment"],
    ["E6 administrative verification", "Blank input: VERIFIED / FAILED / UNRESOLVED"],
    ["Final paper eligibility", "Blank input: INCLUDE / EXCLUDE / DEFER"],
    ["Synthesis coding", "Complete only from assessed full-report evidence"],
    ["Accessibility", "Unavailable reports stay DEFER in this round"],
    ["Revisit rule", "Revisit deferred records only for a documented synthesis gap"],
  ];

  const headers = [
    "Queue order", "Stable canonical ID", "Title", "DOI", "Existing source links",
    "Evidence version", "Supporting scientific evidence", "Scientific outcome",
    "Full-report availability", "Full-report link / local artifact", "E6 checks still needed",
    "Related-version relationship", "Fast-track disposition", "Defer reason",
    "Final scientific eligibility", "E6 administrative verification", "Final paper eligibility",
    "Synthesis role / code", "Assistance code", "Visualization modality", "Task / analytic focus",
    "Assessment notes",
  ];
  queue.getRange("A1:V1").values = [["Candidate assessment and coding queue", ...Array(21).fill(null)]];
  queue.getRange("A2:V2").values = [["Six effective pilot INCLUDE candidates. Prior title/abstract judgments are supporting evidence, not final paper eligibility or completed synthesis coding.", ...Array(21).fill(null)]];
  queue.getRange("A3:V3").values = [headers];
  const body = rows.map((row) => [
    row.queue_order,
    row.record_id,
    row.title,
    row.doi,
    row.existing_source_links.join("\n"),
    row.evidence_version,
    row.supporting_scientific_evidence,
    row.scientific_outcome,
    row.full_report_availability,
    [row.local_full_report, ...row.existing_source_links].filter(Boolean).join("\n"),
    row.e6_checks_still_needed,
    row.related_versions ? JSON.stringify(row.related_versions) : "No registered related-version component in the frozen sampling manifest",
    row.fast_track_disposition,
    row.defer_reason,
    "", "", "", "", "", "", "", "",
  ]);
  queue.getRange(`A4:V${rows.length + 3}`).values = body;

  const dark = "#1F2937";
  const light = "#E5E7EB";
  const input = "#FFF4CC";
  for (const sheet of [instructions, queue]) {
    sheet.getRange("A1:V1").format.font = { name: "Arial", size: 14, bold: true, color: "#111827" };
    sheet.getRange("A2:V2").format.font = { name: "Arial", size: 10, italic: true, color: "#4B5563" };
  }
  instructions.getRange("A4:B12").format.font = { name: "Arial", size: 10, color: "#111827" };
  instructions.getRange("A14:B20").format.font = { name: "Arial", size: 10, color: "#111827" };
  instructions.getRange("A14:B14").format.fill = dark;
  instructions.getRange("A14:B14").format.font = { name: "Arial", size: 10, bold: true, color: "#FFFFFF" };
  instructions.getRange("A4:A12").format.font = { name: "Arial", size: 10, bold: true, color: "#111827" };
  instructions.getRange("A1:D20").format.wrapText = true;
  instructions.getRange("A1:D20").format.verticalAlignment = "top";
  instructions.getRange("A:A").format.columnWidthPx = 210;
  instructions.getRange("B:B").format.columnWidthPx = 760;

  queue.getRange("A3:V3").format.fill = dark;
  queue.getRange("A3:V3").format.font = { name: "Arial", size: 9, bold: true, color: "#FFFFFF" };
  queue.getRange(`A4:V${rows.length + 3}`).format.font = { name: "Arial", size: 9, color: "#111827" };
  queue.getRange(`A3:V${rows.length + 3}`).format.wrapText = true;
  queue.getRange(`A3:V${rows.length + 3}`).format.verticalAlignment = "top";
  queue.getRange(`O4:V${rows.length + 3}`).format.fill = input;
  queue.getRange(`A3:V${rows.length + 3}`).format.borders = { preset: "insideHorizontal", style: "thin", color: light };
  queue.freezePanes.freezeRows(3);
  const widths = [70, 235, 300, 170, 260, 190, 360, 105, 200, 300, 300, 310, 190, 210, 150, 170, 150, 170, 150, 170, 170, 220];
  const columnLetters = "ABCDEFGHIJKLMNOPQRSTUV";
  widths.forEach((width, index) => { queue.getRange(`${columnLetters[index]}:${columnLetters[index]}`).format.columnWidthPx = width; });
  queue.getRange(`O4:O${rows.length + 3}`).dataValidation = { rule: { type: "list", values: ["YES", "NO", "UNCERTAIN"] } };
  queue.getRange(`P4:P${rows.length + 3}`).dataValidation = { rule: { type: "list", values: ["VERIFIED", "FAILED", "UNRESOLVED"] } };
  queue.getRange(`Q4:Q${rows.length + 3}`).dataValidation = { rule: { type: "list", values: ["INCLUDE", "EXCLUDE", "DEFER"] } };

  const exported = await SpreadsheetFile.exportXlsx(workbook);
  await exported.save(target);
  const reopened = await SpreadsheetFile.importXlsx(await FileBlob.load(target));
  const formulaInspection = await reopened.inspect({ kind: "formula", sheetId: "Assessment queue", range: "A1:V20", maxChars: 2000 });
  if ((formulaInspection.ndjson || "").includes("#REF!")) throw new Error("queue workbook contains a formula error");
  await fsp.mkdir(previewDir, { recursive: true });
  for (const sheetName of ["Instructions", "Assessment queue"]) {
    const preview = await reopened.render({ sheetName, autoCrop: "all", scale: 1, format: "png" });
    await fsp.writeFile(path.join(previewDir, `${sheetName.replaceAll(" ", "_").toLowerCase()}.png`), new Uint8Array(await preview.arrayBuffer()));
  }
  const queueSheet = reopened.worksheets.getItem("Assessment queue");
  if (queueSheet.getRange(`A4:V${rows.length + 3}`).values.length !== rows.length) throw new Error("queue row count changed after export");
}

await assertFreshDirectory(outputDir);
for (const name of ["submitted_evidence", "internal", "full_reports"]) {
  await fsp.mkdir(path.join(outputDir, name), { recursive: true });
}

const completedBytes = await fsp.readFile(completedPath);
if (sha256(completedBytes) !== expected.completedSha256) throw new Error("completed follow-up hash changed");
const originalBytes = await fsp.readFile(originalWorkbookPath);
if (sha256(originalBytes) !== expected.originalSubmissionSha256) throw new Error("original 100-record submission hash changed");
const recoveryManifest = await readJson(recoveryManifestPath);
if (recoveryManifest.artifacts.follow_up_re_review_workbook.sha256 !== expected.blankSha256) throw new Error("recovery blank packet binding changed");
if (sha256(await fsp.readFile(path.join(repoRoot, recoveryManifest.artifacts.follow_up_re_review_workbook.path))) !== expected.blankSha256) {
  throw new Error("recovery blank packet bytes changed");
}
for (const [label, target, digest] of [
  ["sampling", samplingPath, expected.samplingSha256],
  ["protocol", protocolPath, expected.protocolSha256],
  ["return schema", returnSchemaPath, expected.returnSchemaSha256],
  ["identity overlay", overlayPath, expected.overlaySha256],
]) {
  if (sha256(await fsp.readFile(target)) !== digest) throw new Error(`${label} binding changed`);
}

const evidenceOverlay = await readJson(evidenceOverlayPath);
const recoveryLedger = await readJson(recoveryLedgerPath);
const originalValidation = await readJson(originalValidationPath);
const supportedFollowupValidation = await readJson(supportedFollowupValidationPath);
const sampling = await readJson(samplingPath);
const amendment = await readJson(amendmentPath);
if (amendment.amendment_version !== amendmentVersion) throw new Error("fast-track amendment version changed");
if (sampling.frame.frame_sha256 !== expected.frameSha256 || sampling.frame.canonical_records !== 140959) {
  throw new Error("frozen sampling frame changed");
}
const followup = await workbookRows(completedPath);
const original = await workbookRows(originalWorkbookPath);
validateFollowup(followup, evidenceOverlay, supportedFollowupValidation);

const unresolvedIds = new Set(
  recoveryLedger.records
    .filter((record) => record.status === "UNRESOLVED_NO_VERIFIED_ABSTRACT")
    .map((record) => record.record_id),
);
if (unresolvedIds.size !== 13) throw new Error("unresolved metadata scope changed");
const effective = effectiveRecords(originalValidation, original.rows, followup.rows, sampling, unresolvedIds);
const candidateIds = new Set(effective.filter((record) => record.scientific_outcome === "INCLUDE").map((record) => record.record_id));
if (candidateIds.size !== 6) throw new Error("effective pilot INCLUDE count changed");
const frozenIds = new Set(sampling.selected_records.map((record) => record.canonical_id));
if (frozenIds.size !== 110) throw new Error("frozen evaluation/calibration membership changed");

const streamed = await streamCanonicalRecords(datasetPath, frozenIds, candidateIds);
if (streamed.sha256 !== expected.datasetSha256 || streamed.count !== 140959) throw new Error("registered corpus binding changed");
if (streamed.candidates.size !== candidateIds.size) throw new Error("candidate records missing from registered corpus");

const archivedCompletedPath = path.join(outputDir, "submitted_evidence", path.basename(completedPath));
await fsp.writeFile(archivedCompletedPath, completedBytes, { flag: "wx" });
const archivedOriginalBindingPath = path.join(outputDir, "internal", "original_100_record_submission_binding.json");
await fsp.writeFile(archivedOriginalBindingPath, jsonBytes({
  path: rel(originalWorkbookPath),
  sha256: expected.originalSubmissionSha256,
  preserved_and_unmodified: true,
  copied_into_this_package: false,
}), { flag: "wx" });
const supportedValidationOut = path.join(outputDir, "internal", "supported_v2_followup_return_validation.json");
await fsp.writeFile(supportedValidationOut, jsonBytes(supportedFollowupValidation), { flag: "wx" });

const localReports = await copyFullReports(path.join(outputDir, "full_reports"));
const candidates = candidateRows(effective, streamed.candidates, localReports);
const availabilityManifest = {
  artifact_class: "bounded_candidate_full_report_availability",
  created_at_utc: new Date().toISOString(),
  scope_record_ids: candidates.map((record) => record.record_id),
  open_ended_retrieval: false,
  purchased_or_restriction_bypassed: false,
  checks: candidates.map((record) => ({
    record_id: record.record_id,
    title: record.title,
    doi: record.doi,
    availability: record.full_report_availability,
    source_links: record.existing_source_links,
    local_artifact: record.local_full_report,
    rationale: record.availability_rationale,
    related_version: record.related_versions,
  })),
};
const availabilityPath = path.join(outputDir, "candidate_full_report_manifest.json");
await fsp.writeFile(availabilityPath, jsonBytes(availabilityManifest), { flag: "wx" });

const candidateById = new Map(candidates.map((record) => [record.record_id, record]));
for (const record of effective) {
  const candidate = candidateById.get(record.record_id);
  if (candidate) {
    record.operational_disposition = candidate.fast_track_disposition;
    record.disposition_reason = candidate.defer_reason || "CLEAR_MATCH_AND_VERIFIED_REPORT_AVAILABLE";
    record.full_report_availability = candidate.full_report_availability;
  } else {
    record.operational_disposition = record.scientific_outcome === "EXCLUDED" ? "EXCLUDE" : "DEFER";
  }
  delete record.operational_disposition_pre_full_report_check;
}

const outcomeKeys = ["INCLUDE", "UNCERTAIN", "EXCLUDED"];
const dispositionKeys = ["ADVANCE_TO_FULL_REPORT_ASSESSMENT", "DEFER", "EXCLUDE"];
const groupSummary = (records) => ({
  outcomes: completeCounts(countBy(records, "scientific_outcome"), outcomeKeys),
  dispositions: completeCounts(countBy(records, "operational_disposition"), dispositionKeys),
});
const effectiveView = {
  artifact_class: "effective_pilot_view_with_explicit_supersession",
  schema_version: "1.0.0",
  created_at_utc: new Date().toISOString(),
  status: "STAGING_ONLY_NOT_IMPORTED_TO_PRODUCTION",
  bindings: {
    original_return: { path: rel(originalWorkbookPath), sha256: expected.originalSubmissionSha256, protocol_version: "2.0.0" },
    followup_return: { path: rel(archivedCompletedPath), sha256: expected.completedSha256, protocol_version: "2.0.0", evidence_version: evidenceVersion },
    recovery_package: await fileReference(recoveryManifestPath),
    evidence_overlay: await fileReference(evidenceOverlayPath),
    frozen_sampling_manifest: { ...(await fileReference(samplingPath)), frame_sha256: expected.frameSha256 },
    registered_corpus: { path: rel(datasetPath), sha256: expected.datasetSha256, canonical_records: streamed.count },
    identity_confirmation_overlay: await fileReference(overlayPath),
    fast_track_amendment: await fileReference(amendmentPath),
  },
  original_submitted_results: {
    total: groupSummary(originalValidation.records.map((record) => ({
      scientific_outcome: record.computed_outcome,
      operational_disposition: record.computed_outcome === "EXCLUDED" ? "EXCLUDE" : "DEFER",
    }))).outcomes,
    uniform_random: completeCounts(countBy(originalValidation.records.filter((r) => r.selection_group === "uniform_random").map((r) => ({ scientific_outcome: r.computed_outcome })), "scientific_outcome"), outcomeKeys),
    challenge: completeCounts(countBy(originalValidation.records.filter((r) => r.selection_group === "challenge").map((r) => ({ scientific_outcome: r.computed_outcome })), "scientific_outcome"), outcomeKeys),
  },
  effective_results: {
    total: groupSummary(effective),
    uniform_random: groupSummary(effective.filter((record) => record.selection_group === "uniform_random")),
    challenge: groupSummary(effective.filter((record) => record.selection_group === "challenge")),
  },
  reporting_counts: {
    screened: 100,
    excluded: effective.filter((record) => record.operational_disposition === "EXCLUDE").length,
    advanced: effective.filter((record) => record.operational_disposition === "ADVANCE_TO_FULL_REPORT_ASSESSMENT").length,
    deferred: effective.filter((record) => record.operational_disposition === "DEFER").length,
    calibration_records_separate: 10,
    still_unprocessed_excluding_frozen_evaluation_and_calibration: streamed.count - frozenIds.size,
    queued_but_unprocessed_next_batch: nextBatchSize,
  },
  unresolved_metadata_cases_deferred: [...unresolvedIds].sort(),
  policy: {
    original_returns_reinterpreted: false,
    followup_supersession_applies_only_to_effective_staging_view: true,
    deferred_is_excluded: false,
    evidence_or_judgment_overwritten: false,
    independent_validation_claimed: false,
    known_exposure_limitation: "This conversation and protocol-development work exposed sample records and prior model judgments; this pilot is not represented as an independent blinded human reference.",
  },
  records: effective,
};
const effectivePath = path.join(outputDir, "pilot_effective_view.json");
await fsp.writeFile(effectivePath, jsonBytes(effectiveView), { flag: "wx" });

const queuePath = path.join(outputDir, "candidate_assessment_coding_queue.xlsx");
await buildQueueWorkbook(candidates, queuePath, path.join(outputDir, "internal", "workbook_previews"));

const nextBatchPath = path.join(outputDir, `next_batch_${nextBatchSize}.jsonl`);
await fsp.writeFile(nextBatchPath, `${streamed.nextBatch.map((record, index) => JSON.stringify({ batch_order: index + 1, ...record })).join("\n")}\n`, { flag: "wx" });
const nextBatchManifest = {
  artifact_class: "memory_bounded_title_abstract_fast_track_batch",
  schema_version: "1.0.0",
  status: "READY_NOT_LAUNCHED_MISSING_MODEL_AND_BUDGET_APPROVAL",
  frame: { path: rel(datasetPath), sha256: expected.datasetSha256, canonical_records: streamed.count },
  excluded_frozen_membership: { evaluation: 100, calibration: 10, total: frozenIds.size },
  order: { algorithm: "ascending_sha256(seed + unit_separator + canonical_id)", seed: nextBatchSeed, independent_of_model_judgments: true },
  batch_size: nextBatchSize,
  batch_path: rel(nextBatchPath),
  batch_sha256: sha256(await fsp.readFile(nextBatchPath)),
  memory_bounded_extraction: true,
  execution: {
    model_calls: 0,
    existing_authorized_provider_model_and_spending_limit_found: false,
    missing_decisions: ["provider", "model", "hard maximum spend in USD"],
    narrow_interface_gap: "The repository has validated per-record inference components, but no memory-bounded live batch runner that consumes this JSONL without constructing a ReviewDataset. This package intentionally does not add or execute such a runner.",
  },
};
const nextBatchManifestPath = path.join(outputDir, "next_batch_manifest.json");
await fsp.writeFile(nextBatchManifestPath, jsonBytes(nextBatchManifest), { flag: "wx" });
const launchPath = path.join(outputDir, "next_batch_launch_instructions.md");
await fsp.writeFile(launchPath, Buffer.from(`# Next bounded title/abstract batch\n\nStatus: ready input, not launched.\n\n- Input: \`${rel(nextBatchPath)}\`\n- Records: ${nextBatchSize}\n- Order: ascending SHA-256 rank using seed \`${nextBatchSeed}\`; independent of model outputs and judgments.\n- Frozen 100-record evaluation sample and 10 calibration records are excluded.\n- Every row remains \`UNPROCESSED_READY_NOT_LAUNCHED\` until a validated return exists.\n\nBefore launch, approve all three missing controls: provider, exact model, and hard maximum spend in USD. No repository artifact records an authorized combination of those controls.\n\nThe existing inference API validates per-record proposals but does not provide a memory-bounded live JSONL runner. The smallest future implementation, after the three controls are approved, is a bounded iterator over this frozen JSONL that calls the existing inference/validation functions, writes append-only attempts and cost totals, stops at the hard cap, and does not load or mutate the registered ReviewDataset.\n`, "utf8"), { flag: "wx" });

const reportPath = path.join(outputDir, "fast_track_reconciliation_report.md");
const total = effectiveView.effective_results.total;
const random = effectiveView.effective_results.uniform_random;
const challenge = effectiveView.effective_results.challenge;
const originalTotal = effectiveView.original_submitted_results.total;
const originalRandom = effectiveView.original_submitted_results.uniform_random;
const originalChallenge = effectiveView.original_submitted_results.challenge;
await fsp.writeFile(reportPath, Buffer.from(`# Fast-track pilot reconciliation\n\nStatus: staging only. Amendment: \`${amendmentVersion}\`. Base returns remain protocol/schema 2.0.0 and are preserved without retroactive reinterpretation.\n\n## Validation\n\n- Completed enriched-evidence return SHA-256: \`${expected.completedSha256}\`.\n- Reviewer: Morris; 19 unique records; evidence, IDs, order, E1-E5/E7, and deferred E6 bind to \`${evidenceVersion}\`.\n- Supported v2 recomputation: 13 EXCLUDED, 4 UNCERTAIN, 2 INCLUDE; all outcome and routing formula caches match.\n- The archived original 100-record return remains unchanged at SHA-256 \`${expected.originalSubmissionSha256}\`.\n\n## Original versus effective scientific outcomes\n\n| View | Group | INCLUDE | UNCERTAIN | EXCLUDED |\n|---|---:|---:|---:|---:|\n| Original submission | 70 random | ${originalRandom.INCLUDE} | ${originalRandom.UNCERTAIN} | ${originalRandom.EXCLUDED} |\n| Original submission | 30 challenge | ${originalChallenge.INCLUDE} | ${originalChallenge.UNCERTAIN} | ${originalChallenge.EXCLUDED} |\n| Original submission | Total | ${originalTotal.INCLUDE} | ${originalTotal.UNCERTAIN} | ${originalTotal.EXCLUDED} |\n| Effective versioned view | 70 random | ${random.outcomes.INCLUDE} | ${random.outcomes.UNCERTAIN} | ${random.outcomes.EXCLUDED} |\n| Effective versioned view | 30 challenge | ${challenge.outcomes.INCLUDE} | ${challenge.outcomes.UNCERTAIN} | ${challenge.outcomes.EXCLUDED} |\n| Effective versioned view | Total | ${total.outcomes.INCLUDE} | ${total.outcomes.UNCERTAIN} | ${total.outcomes.EXCLUDED} |\n\n## Fast-track operational disposition\n\n| Group | Advance | Defer | Exclude |\n|---|---:|---:|---:|\n| 70 random | ${random.dispositions.ADVANCE_TO_FULL_REPORT_ASSESSMENT} | ${random.dispositions.DEFER} | ${random.dispositions.EXCLUDE} |\n| 30 challenge | ${challenge.dispositions.ADVANCE_TO_FULL_REPORT_ASSESSMENT} | ${challenge.dispositions.DEFER} | ${challenge.dispositions.EXCLUDE} |\n| Total | ${total.dispositions.ADVANCE_TO_FULL_REPORT_ASSESSMENT} | ${total.dispositions.DEFER} | ${total.dispositions.EXCLUDE} |\n\nThe 13 unresolved metadata cases remain DEFER; there is no further recovery campaign in this round. The 1996 digital-brain-atlas INCLUDE is also DEFER because no rights-clear public full report was located in the bounded check. The other five candidates have verified public or institutional reports and advance to full-report assessment. CancerLandscapes is represented as two distinct bibliographic identities: the sampled conference abstract and the corresponding full research paper; their relationship is documented, not merged.\n\n## Required count accounting\n\n- Screened pilot records: 100.\n- Excluded: ${effectiveView.reporting_counts.excluded}.\n- Advanced: ${effectiveView.reporting_counts.advanced}.\n- Deferred: ${effectiveView.reporting_counts.deferred}.\n- Calibration records kept separate: 10.\n- Still unprocessed outside the frozen evaluation/calibration sets: ${effectiveView.reporting_counts.still_unprocessed_excluding_frozen_evaluation_and_calibration}; ${nextBatchSize} are queued but remain unprocessed.\n\n## Limitations\n\nThis is pragmatic, time-bounded selection with missing-evidence and accessibility limitations. It is not exhaustive screening or evidence of unbiased coverage. Prior sample records and model judgments were exposed during protocol development, so this pilot is not claimed as an independent blinded human reference. No production state, corpus evidence, prior return, PRISMA count, or identification status was changed.\n`, "utf8"), { flag: "wx" });

const manifestPath = path.join(outputDir, "package_manifest.json");
const artifactPaths = [
  archivedCompletedPath,
  archivedOriginalBindingPath,
  supportedValidationOut,
  availabilityPath,
  effectivePath,
  queuePath,
  nextBatchPath,
  nextBatchManifestPath,
  launchPath,
  reportPath,
  ...Object.values(localReports).map((item) => path.join(repoRoot, item.path)),
];
const manifest = {
  artifact_class: "title_abstract_fast_track_staging_package",
  schema_version: "1.0.0",
  amendment_version: amendmentVersion,
  created_at_utc: new Date().toISOString(),
  status: "STAGING_ONLY_NO_PRODUCTION_IMPORT",
  counts: effectiveView.reporting_counts,
  bindings: effectiveView.bindings,
  artifacts: Object.fromEntries(await Promise.all(artifactPaths.map(async (target) => [path.basename(target), await fileReference(target)]))),
  prohibited_actions_confirmed: {
    broad_metadata_recovery: 0,
    model_calls: 0,
    production_imports: 0,
    corpus_overwrites: 0,
    messages_sent: 0,
    commits: 0,
    pushes: 0,
  },
};
await fsp.writeFile(manifestPath, jsonBytes(manifest), { flag: "wx" });

console.log(JSON.stringify({
  output_dir: rel(outputDir),
  completed_submission_sha256: expected.completedSha256,
  effective_outcomes: total.outcomes,
  effective_dispositions: total.dispositions,
  queue: rel(queuePath),
  next_batch: rel(nextBatchPath),
  report: rel(reportPath),
}, null, 2));
