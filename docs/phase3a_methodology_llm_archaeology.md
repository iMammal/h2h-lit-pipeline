# Phase 3A: Methodology and LLM Archaeology

**Investigation date:** 2026-08-15  
**Status:** Historical reconstruction complete; no revised methodology implemented  
**Required stop point:** This report is submitted for approval before any LLM or
classification implementation work begins.

## Executive conclusions

1. The historical pipeline was not one stable workflow. It was an exploratory notebook
   lineage with at least three assistance prompts, one separate modality prompt, one
   strict relevance/intersection prompt, and two full-text analysis prompt artifacts.
2. The best-preserved large-corpus branch harvested a 25,266-record merged BibTeX corpus,
   then assigned 19,465 records to `OFF-TOPIC` and 5,801 to one of four assistance
   buckets. The assistance decision and rationale were not written into those BibTeX
   records; only bucket membership survives.
3. A later modality pass did persist the complete model response in
   `llm_classification` and appended `LLM_Reasoning:` to `note`. Its output files overlap,
   so their counts must not be summed as independent PRISMA branches.
4. The full-text stage requested a 1-5 `relevance_score`, but defined only `5 = Critical`.
   No operational anchors for scores 1-4 survive. The scores are therefore historical
   prioritization signals, not a defensible eligibility rubric.
5. The reported Desktop/Algorithmic reduction from 5,512 to 97 is not reproducible as a
   single `year >= 2019 and relevance >= 4` transition. Surviving artifacts yield 93 in
   the most directly named filtered file, 111 from the score-4/5 streams, and 97 records
   dated 2019 or later in a different mixed aggregate. The exact provenance of 97 is
   unresolved.
6. The final curated branch contains 67 RDF/BibTeX items and 56 structured JSON analyses,
   but no surviving transition log explains selection from the 141-item deduplicated RDF
   or from the broad corpus. Prior-survey seed provenance is also absent.
7. No historical evidence demonstrates independent human coding, an override log,
   adjudication, or inter-rater agreement. Named mission files are downstream reading and
   synthesis assignments, not classification validation.
8. Some PRISMA evidence can be recovered as artifact-state counts. Database retrieval
   counts, exact run dates, exclusion reasons, and several transitions must be regenerated
   because mutable notebook state and reused accumulators contaminated nominal source
   files.
9. Reviewer requirements are used below to define what future provenance must support.
   They are not used to rewrite the account of what the historical pipeline did.

## Scope, sources, and evidence standard

The historical workspace `/Users/morris/PyCharmMiscProject` was inspected read-only. No
historical files, notebooks, outputs, or Git objects were modified. No network, literature
API, PDF download, or LLM call was made.

Primary evidence:

- Git commit `c1757aa11027e45cc1e411c1559b9ec27052f898`, dated 2026-01-12,
  especially committed `H2HLitFetcher.ipynb` and `downloader.ipynb`;
- surviving working-tree `H2HLitFetcher.ipynb`;
- committed `H2HLitFetcher12000v1.ipynb`, recoverable only from Git because the
  working-tree file is empty;
- `H2H STAR 3/preprocess.ipynb` and its RDF, BibTeX, PDF, JSON, and task artifacts;
- `H2H_Star4_1/PROJECT_CONTEXT.md`, RDF, JSON analyses, and task-distribution files;
- generated `BroadSearches1`, `BroadSearches2`, and `Broad_LifeSci_VisualAnalytics_*`
  artifacts;
- the clean repository documentation; and
- `docs/H2H2/CGF_Reviewer_Response_Matrix.md` plus the decision letter supplied by the
  user.

`notebook.ipynb` is byte-identical to the committed downloader notebook and is not an
independent lineage. `speechify.ipynb` is unrelated and was excluded.

Evidence labels used in this report:

- **Direct-code:** explicitly present in source or notebook cells.
- **Direct-artifact:** directly counted or read from a generated artifact.
- **Artifact-inferred:** strongly implied by names, overlaps, execution state, or file
  contents, but not recorded as an explicit event.
- **Unrecoverable:** not supported by surviving evidence and must not be claimed.

Notebook execution counts and file modification times are supporting clues, not reliable
run identifiers. The notebooks were run out of order and reuse globals. The committed
notebooks also declare a Python 2.7.6 kernel although their source uses modern Python,
which is further evidence that notebook metadata is not a complete environment record.

For clarity, this report distinguishes four corpus concepts:

- **Historical corpus:** artifacts produced by the exploratory pre-submission notebooks
  and surviving historical workflow.
- **Regenerated corpus:** records retrieved prospectively under a revised, documented,
  reproducible search protocol.
- **Eligible corpus:** regenerated records that satisfy the approved eligibility rubric.
- **Synthesis corpus:** eligible records selected for or used in the STAR's narrative
  synthesis.

These terms are not interchangeable. In particular, a surviving historical artifact
count is not automatically a PRISMA identification count for the revised review.

## 1. Historical prompt inventory

### 1.1 Inventory and evolution

| ID | Purpose | Historical locator | Inputs | Output | Execution/model evidence |
|---|---|---|---|---|---|
| P1 | Assistance classification v1 | committed `H2HLitFetcher.ipynb`, cells 90-91 | title, abstract | `CATEGORY \| Rationale` | Cell 90 executed; Chat Completions, requested `gpt-4o`, temperature `0.0` |
| P2 | Assistance classification v2 | cells 106 and 110 | title, abstract | `CATEGORY \| Rationale` | Duplicate, unexecuted definitions; model inherited from mutable global if used |
| P3 | Assistance classification v3 | cell 113 with classifier in cell 137 | title, abstract; journal only in a separate redefinition at cell 157 | `CATEGORY \| Brief rationale` | Prompt and classifier executed; last executed model assignment was `gpt-5-mini`, temperature `0.0` |
| P4 | Visualization modality | cells 117-118 | title, abstract | `MODALITY \| rationale` | Responses API; requested global model, `max_output_tokens=1800`; temperature omitted |
| P5 | Strict life-science/visualization/AI intersection | cells 178-179 | title, abstract, journal | `CATEGORY \| Rationale` | Chat Completions through shared classifier; requested global model, temperature `0.0` |
| P6 | Full-text extraction and relevance | `H2H STAR 3/preprocess.ipynb`, cell 3 | RDF title, collection name, extracted PDF text truncated at 120,000 characters | free response requested as one JSON object | Chat Completions, requested `gpt-5.1`, temperature `0.3` |
| P7 | Later project-context classification/extraction contract | `H2H_Star4_1/PROJECT_CONTEXT.md`, version 1.0 | intended full-paper content; invocation is absent | one JSON object | No provider, model, settings, execution code, or run metadata survives |

The current working copy of `H2HLitFetcher.ipynb` contains the same classifier prompt
families and no additional prompt lineage. Its execution state differs from the committed
snapshot, so the committed version is the stronger source for reconstructing a coherent
cell history.

Every executable LLM path found uses the OpenAI Python client; no second LLM provider was
found. Except where the table states otherwise, the code records no seed, top-p, penalties,
timeout, structured-output mode, or provider-resolved model snapshot. An omitted setting
must be reported as unrecorded, not reconstructed from present-day API defaults.

Cell-source SHA-256 values are recorded to make the inventory checkable without copying
credential-contaminated notebooks into this repository:

| Cell/artifact | SHA-256 |
|---|---|
| P1 cell 90 | `78deae5ac29930c07c2268ebf0cede166f893f3cc8f7df79e207b4ea427797fe` |
| P1 corrected cell 91 | `9fec314044e187e948d1a63f14ec0c51a6251125831d48e1bfc2ef9dad95a0d6` |
| P2 cells 106 and 110 | `7a79970fc77541f3f547c8b178d5b4bfbb7bdd8ba8355ed8f105994bad8cb78d` |
| P3 cell 113 | `5894c101385361861b0da0e610061ec1097667891f56087044d9c44504b71c55` |
| P4 prompt cell 117 | `b602a329f399c594f52d3626c82bd08f3797e928d8290a327b779b8c83325663` |
| P4 caller cell 118 | `48bb03ea6e6917b6e2d24c9514d40b01241b39b6087eb17731f64fce7ee70bd8` |
| P5 prompt cell 178 | `87c25b372c76d6022d7c24881a9036c618509555f763274e8b83211840ab1697` |
| P5 splitter cell 179 | `ac74f2acd2903b19d677b2578d8f118321b405e75ccf4c0190f09670fbb84396` |
| P6 cell 3 | `b8293ea183b17b254d6428483c7255e6a8af3cf8f9cd5bfc889e5147f58bbf2e` |
| P7 `PROJECT_CONTEXT.md` | `a3a75f6682eeb096bdb3fe333a28ef52d85666996e49986c55ac38df9579c43a` |

These are source-artifact hashes, not sanitized prompt-package versions. The future
migration must create clean prompt artifacts with their own hashes.

### 1.2 Assistance prompt v1 (P1)

The first prompt described the survey as **AI-Assisted Network Visualization in the Life
Sciences** and required one of five labels:

- `ALGORITHMIC`: AI/ML, including GNNs, clustering, force-directed layouts, used as a
  backend to process, simplify, or lay out graph data;
- `ADAPTIVE`: real-time adjustment based on behavior, eye tracking, or context;
- `CONVERSATIONAL`: NLP, chatbot, or voice control;
- `IMMERSIVE`: VR, AR, or CAVE for abstract biological data, with surgical navigation,
  training, and telepresence excluded; or
- `OFF-TOPIC`: clinical studies, general computer science without a biological focus,
  streaming, surgery, or rehabilitation.

It asked for exactly `CATEGORY | Rationale`. Cell 91 corrected the BibTeX parser from
`parse_string` to `loads` and stripped braces from title/abstract. The prompt text itself
did not change. Parse and API exceptions became strings beginning `ERROR |`; there was no
retry, backoff, output-schema validation, or error-specific bucket.

### 1.3 Assistance prompt v2 (P2)

V2 added a priority instruction: a software tool, interface, or system for visualizing
biological data should be assigned to an assistance mode rather than `OFF-TOPIC`. It also:

- moved `ADAPTIVE` first and distinguished user-responsive adaptation from a one-time
  adaptive algorithm;
- excluded biological adaptation from `ADAPTIVE`;
- required an actual language-based user interface for `CONVERSATIONAL` and placed
  noninteractive NLP extraction in `ALGORITHMIC`;
- treated immersive 3D analysis as `IMMERSIVE`; and
- explicitly included dimensionality reduction, force layouts, clustering, community
  detection, and GNNs in `ALGORITHMIC`.

Cells 106 and 110 contain byte-identical definitions and have no execution count. Their
presence proves prompt evolution, not use on a particular artifact.

### 1.4 Assistance prompt v3 (P3)

V3 changed the meaning of the taxonomy materially. It says the taxonomy classifies the
**mode of assistance, not visualization modality**, and that VR/AR/CAVE alone does not
imply `IMMERSIVE`. It then defines:

- `ALGORITHMIC` as backend computation without individual-user adaptation and makes it
  the default for any visualization tool not clearly off-topic;
- `ADAPTIVE` as dynamic response to user, task, or context;
- `CONVERSATIONAL` as language-mediated interaction;
- `IMMERSIVE` only when embodiment, spatial cognition, navigation, or immersion itself
  supports reasoning; and
- `OFF-TOPIC` only when no visualization or visual-analytics system is described, while
  separately listing clinical, wet-lab, segmentation-only, surgery, training, and
  telemedicine exclusions.

The default-to-Algorithmic instruction expanded inclusion beyond the earlier requirement
for explicit AI/ML. This is the most consequential historical taxonomy change.

The broad-corpus classifier in cell 137 used title and abstract. A separate earlier
redefinition in cell 157 added journal. Execution counts show the `gpt-5-mini` assignment
was executed before the relevant classifier and P3 prompt in the same recorded kernel
sequence. Because global state was mutable and no response metadata was persisted, the
requested model association is **execution-state inferred**, not independently auditable
per record.

### 1.5 Modality prompt (P4)

P4 is logically separate from assistance. It required one best-fit label:

- `DESKTOP`, including both 2D and 3D on a standard monitor/browser;
- `LARGEDISPLAY` for walls, tiled displays, or powerwalls;
- `VR` for explicit VR/HMD systems;
- `AR_XR` for augmented, mixed, or extended reality;
- `CAVE` for projection-based immersive rooms.

Its ordered disambiguation rules were VR, then AR/XR, then CAVE, then Large Display, then
Desktop, although a later sentence says the modality central to the contribution or
evaluation should win. This produces an internal conflict for multimodal papers.

The caller used the OpenAI Responses API, the global requested model, and
`max_output_tokens=1800`. `temperature=0.0` is commented out, so provider/model defaults
applied. It logged total tokens to notebook output when available but did not persist
usage. Parse/API failures again became `ERROR |` strings with no retry.

The splitter stored the whole response as BibTeX `llm_classification` and appended a
derived `LLM_Reasoning:` line to `note`. Any unrecognized leading label, including
`ERROR`, was forced into `OFF-TOPIC`; therefore technical failures are confounded with
scientific exclusion.

### 1.6 Strict intersection prompt (P5)

P5 required the intersection of:

1. life sciences;
2. visualization or visual analytics; and
3. AI/automation assisting the user.

It ordered `OFF-TOPIC` for no interactive visualization, no qualifying assistance, or no
life-science application. It gave `ALGORITHMIC` and `OFF-TOPIC` examples and required
`CATEGORY | Rationale`.

The downstream function, however, allowed only `BIO` and `OFF-TOPIC`. The prompt never
instructed the model to emit `BIO`; every other response was coerced to `OFF-TOPIC`.
This is a direct prompt/parser contract defect, not a methodological choice.

### 1.7 Full-text prompt (P6)

P6 extracted PDF text with `pypdf`, truncated it to 120,000 **characters** despite a
comment describing a token limit, and supplied:

- RDF title;
- Zotero collection/folder name as an existing taxonomy category; and
- truncated full text.

The system text framed the STAR around the hairball problem and Immersive, Algorithmic,
and Adaptive approaches. Because adjacent Python string literals were concatenated, some
sentences have no guaranteed separator. The user prompt first requested four prose
sections and then requested one exact JSON object. The JSON schema contained:

- `biblio.title`, `year`, and `first_author`;
- `classification.modality` and `assistance_type`;
- `analysis.core_innovation`, `solves_hairball_mechanism`, `biological_utility`,
  `key_limitations`, and `star_integration_target`; and
- `relevance_score` as integer 1-5, with only `5 = Critical` defined.

The requested assistance values in this schema were Algorithmic, Immersive, or Adaptive,
omitting Conversational even though other historical prompts used it. JSON mode or a
formal response schema was not used. Responses were written with an `.md` suffix and
later renamed or copied into `.json` artifacts. Existing analysis paths caused a skip; a
one-second sleep followed successful writes. API errors were returned as strings and
could be written as if they were analyses. No retry, raw response envelope, request ID,
provider-resolved model version, token usage, prompt hash, or timestamp was stored.

The code requested `MODEL_NAME = "gpt-5.1"`, while its comment says `gpt-4o` was a proxy
for a `gpt-5.2` high-reasoning tier. Only the requested string is direct evidence. The
comment is contradictory context and must not be reported as the model actually used.

### 1.8 Project-context prompt (P7)

`H2H_Star4_1/PROJECT_CONTEXT.md` version 1.0 is a later cleanly formatted analysis
contract. It separates six modalities (`2D Desktop`, `3D Desktop`, `VR`, `AR/MR`, `CAVE`,
`Large Display`) but combines assistance into Algorithmic, Immersive, and
Conversational/Adaptive. Its JSON adds DOI and renames `first_author` to
`first_author_lastname`. It gives no relevance-score anchors.

Fifty-six surviving JSON files broadly follow this contract, with additional ad hoc
analysis keys in some records. No invocation code ties the files to a provider, model,
settings, prompt hash, or timestamp. P7 and its outputs therefore establish a schema and
classification vocabulary, but not reproducible inference provenance.

## 2. Classification lineage

### 2.1 Early assistance-by-modality search lineage

The earliest reconstructed design searched 20 cells formed by four assistance query
families times five modality query families:

```text
Algorithmic, Adaptive, Conversational, Immersive
    x
2D, LargeDisplay, VR, AR_XR, CAVE
```

Each query combined an assistance block, a biological-network base block, and a modality
block. PubMed, Europe PMC, CrossRef, Semantic Scholar, and arXiv were called with a
default cap of 30 per source and a 0.5-second delay. Exceptions were printed and the run
continued. Records were deduplicated DOI-first with title fallback, then screened by a
keyword heuristic.

The heuristic evolved:

1. It initially required both assistance and modality cues. For `2D`, any immersive cue
   caused exclusion.
2. A later version retained assistance cues only as tags and gated solely on modality;
   `2D` still rejected immersive terms.

Only on-topic records were saved to each `results.bib`. Some versions also wrote excluded
titles for manual inspection, but no decisions from that inspection survive. A series of
folders (`H2H_Lit_Collection_Jupyter*`, `Debug*`, `NoNetwork`) are experimental reruns,
not cumulative PRISMA stages. Their counts must not be added.

### 2.2 Broad merged-corpus lineage

The strongest broad branch is:

```text
six broad query families / four PubMed fielded queries
    -> mutable per-source BibTeX exports
    -> DOI, then title, then URL merge/deduplication
    -> fetchALL12000.bib (25,266)
    -> P3 assistance split (5 labels)
    -> P4 modality split within each assistance label
    -> PDF acquisition where possible
    -> P6 full-text structured analysis for a subset
    -> score/year sorting and ad hoc aggregation
```

The six broad query families were visual analytics, XR, biology, graphs/networks,
single-cell/omics, and AI. Europe PMC was called with a nominal limit of 1,000 for each
family. PubMed used four Title/Abstract queries with a nominal `retmax=12000` each. The
notebook also created CrossRef and arXiv exports. Semantic Scholar exploratory code is
present, but no Semantic Scholar source survives in the merged artifact.

The merge loaded, in order, `arxivALL1000.bib`, `epmcALL1000.bib`, `xrfALL1000.bib`, and
`pmcALL12000.bib`; chose DOI, else normalized title, else normalized URL; dropped records
without any key; and retained the first duplicate encountered. It then renamed duplicate
BibTeX IDs. This order affects which source label survives.

P3 produced these disjoint first-stage buckets:

| Assistance result | Records | Share of 25,266 |
|---|---:|---:|
| `OFF-TOPIC` | 19,465 | 77.1% |
| `ALGORITHMIC` | 5,512 | 21.8% |
| `ADAPTIVE` | 121 | 0.48% |
| `IMMERSIVE` | 125 | 0.49% |
| `CONVERSATIONAL` | 43 | 0.17% |
| **Total** | **25,266** | **100%** |

The assistance response is not present in these entries. A record's file membership is
the only surviving label evidence, and no rationale, model record, or failure state is
preserved.

P4 then classified modality. For Adaptive, Conversational, and Immersive, the surviving
files reconcile to their parent counts:

| Parent | Modality artifacts |
|---|---|
| Adaptive 121 | AR/XR 2, Desktop 114, Large Display 1, VR 4 |
| Conversational 43 | Desktop 40, Large Display 2, VR 1 |
| Immersive 125 | AR/XR 12, CAVE 5, Desktop 6, Large Display 1, VR 101 |

Algorithmic outputs are not disjoint. They contain AR/XR 13, CAVE 6, Desktop 5,464,
Large Display 1, VR 28, and an `OFF-TOPIC` file with 3 records that also remain in
Desktop. `DESKTOP1` contains four records, all already present in Desktop. Counting
distinct BibTeX IDs across these files recovers the parent 5,512; summing files does not.

All records in the modality artifacts contain both `llm_classification` and
`LLM_Reasoning`. This makes modality output the best-preserved historical LLM annotation,
although its inference metadata is still absent.

### 2.3 Broad life-science branch

`Broad_LifeSci_VisualAnalytics_*` is another experimental family, not a downstream stage
of the 25,266 corpus. It used a broad visual-analytics/life-science query across the same
general source set, with several query rewrites and per-source limits. Examples of
observed broad-result counts are 898, 1,795, 1,797, 998, 899, 799, and 1,049 in separate
folders. These are alternate runs.

The best-preserved V3 branch contains 1,797 records:

| Assistance result | Count |
|---|---:|
| Algorithmic | 766 |
| Adaptive | 6 |
| Conversational | 15 |
| Immersive | 15 |
| Off-topic | 995 |

The Algorithmic/Desktop bucket contained 761 records. Splitting on abstract presence
produced 707 without abstracts and 54 with abstracts. Only the 54 with abstracts reached
a strict reclassification, yielding 5 Algorithmic and 49 Off-topic. The next `BIO`
splitter was run on the already off-topic 49-record file and, because of the P5 contract
defect, emitted all 49 as Off-topic again. This branch demonstrates both missing-abstract
bias and asymmetric repeated filtering.

### 2.4 Curated RDF/full-text lineage

The curated branch is only partially connected to the broad branches:

```text
H2H STAR 3.rdf (183 items)
    -> notebook RDF deduplication
    -> H2H_STAR_3_Deduplicated.rdf (141 items)
    -> undocumented selection/collection editing
    -> H2H_Star4/H2H_Star4.bib and H2H_Star4_1.rdf (67 items)
    -> locally available/OA full texts
    -> 56 P7-shaped JSON analyses
    -> named reading/synthesis mission files
```

The RDF deduplicator keyed on the pair `(doi, title)`, not DOI-first with title fallback.
Consequently, the same DOI with title variation is not necessarily merged. The code
merged abstracts/notes and collection membership and removed duplicate RDF subjects.
The observed 183-to-141 reduction is direct artifact evidence, but its individual merge
decisions were not logged.

The transition from 141 to 67 is undocumented. The 67-item RDF and BibTeX agree in count,
but 56 JSON analyses do not establish complete analysis coverage. The JSON set contains
relevance scores `2: 20`, `3: 24`, `4: 9`, and `5: 3`, and assistance labels Adaptive 20,
Algorithmic 18, and Immersive 18. Modality strings are not controlled: 31 are exactly
`2D Desktop`, 4 exactly `3D Desktop`, 8 exactly `VR`, and the remainder use several
one-off or hybrid strings.

Mission files assigned downstream reading/synthesis work using roles and collection or
keyword routing. The H2H_Star4_1 assignments were Amira 30, Nicole 15, User 22, and Vinay
10; assignments can overlap and are not corpus counts. They do not document independent
classification, blinded review, overrides, or adjudication.

## 3. Historical corpus state-transition model

The reconstructed state model below describes what the historical pipeline actually
represented. A dashed transition means the relationship is inferred or undocumented.

```text
query definition
    -> source request (not durably logged)
    -> source result dict
    -> mutable all_entries accumulator
    -> per-run deduplication
    -> BibTeX artifact
    -> cross-file merge/deduplication
    -> broad unique artifact
    -> LLM assistance bucket
    -> LLM modality bucket + persisted response
    -> optional missing-abstract/strict filtering
    -> optional PDF discovery/download
    -> extracted full text
    -> LLM JSON-like full-text analysis + relevance score
    -> score/year sort or aggregate
    --?-> Zotero/RDF curated library
    -> RDF tuple deduplication
    --?-> 67-item curated set
    -> 56 structured analyses
    -> human reading/synthesis assignments
```

Historically, these states were represented by directories and filenames, not durable
events. Re-running a cell could create an overlapping output without invalidating the
older one. The following distinctions were absent or unstable:

- source occurrence versus canonical deduplicated record;
- screening exclusion versus API/parse failure;
- eligibility versus assistance label;
- eligibility versus relevance score;
- eligible corpus versus narrative-synthesis sample;
- machine annotation versus human-confirmed annotation;
- superseded output versus current output; and
- retrieval run versus notebook session.

The clean reconstruction should model these as explicit transitions, not derive them from
folder names.

## 4. Recoverable PRISMA stages and count evidence

### 4.1 Recoverable counts

| Stage | Recoverable evidence | Confidence and limitation |
|---|---:|---|
| Historical broad merged artifact | 25,266 | Direct artifact; reproducible from four surviving inputs with historical merge code. This is a reconstructed historical merged-corpus state, not a fully auditable PRISMA identification-stage count. |
| Surviving source label in merged artifact | PubMed 13,766; CrossRef 5,838; Europe PMC 4,662; arXiv 1,000 | Direct artifact; these are retained provenance labels after first-wins dedupe, not raw database retrieval counts |
| Assistance split | 121 Adaptive; 5,512 Algorithmic; 43 Conversational; 125 Immersive; 19,465 Off-topic | Direct artifact; exactly reconciles to 25,266 |
| Modality split | Exact counts listed in Section 2.2 | Direct artifact, but Algorithmic outputs overlap and require distinct-ID reconciliation |
| Algorithmic/Desktop parent | 5,464 | Direct artifact; not the same denominator as the 5,512 Algorithmic parent |
| Full-text analysis directory for Algorithmic/Desktop | 5,402 files | Direct artifact; fewer than 5,464 and not all represented in aggregates |
| Parseable score-stream objects | 4,663 objects, of which 4,612 have a score and 51 lack one | Direct artifact after brace-aware parsing; aggregate files are concatenated objects, not valid JSON arrays |
| Scored objects | score 1: 180; 2: 2,466; 3: 1,718; 4: 243; 5: 5 | Direct artifact; does not reconcile to the analysis directory or parent BibTeX |
| Named `4-5_2019-2025` artifact | 93 objects, all score 4 and year >= 2019 | Direct artifact; filename overstates contents because no score-5 object is present |
| Mixed `...ALL.json` aggregate | 308 objects, 2 parse errors, 130 without score; 97 dated 2019 or later | Direct artifact; it is not a demonstrated eligibility/final-corpus file |
| Initial curated RDF | 183 items | Direct artifact |
| Deduplicated curated RDF | 141 items | Direct artifact; 42 fewer subjects under tuple-key deduplication |
| Later curated RDF/BibTeX | 67 items | Direct artifact; transition from 141 is undocumented |
| Later structured analyses | 56 valid JSON files | Direct artifact; execution provenance absent |

The nominal source exports are contaminated by accumulator reuse:

| Filename | Entries | Embedded source labels |
|---|---:|---|
| `xrfALL1000.bib` | 5,838 | 5,838 CrossRef |
| `arxivALL1000.bib` | 6,839 | 5,838 CrossRef; 1,001 arXiv |
| `epmcALL1000.bib` | 11,501 | 5,838 CrossRef; 1,001 arXiv; 4,662 Europe PMC |
| `pmcALL12000.bib` | 14,168 | 14,168 PubMed |

Thus, the filenames are cumulative snapshots, not four independent source result sets.
The implied increments total 25,669 before the final cross-file merge, and the merged
artifact removes 403 later-source duplicates. This does not recover raw result counts per
query, API-reported totals, or failed pages.

`BroadSearches1` and `BroadSearches2` copies of key artifacts are byte-identical and must
not be counted twice.

### 4.2 The unresolved 5,512 to 97 path

Three surviving calculations conflict:

- the file named for relevance 4-5 and years 2019-2025 has 93 records, all score 4;
- score-specific streams contain 110 score-4 records and 1 score-5 record from 2019 or
  later, totaling 111; and
- a different 308-object mixed aggregate has 97 records from 2019 or later, regardless
  of relevance score.

There is no artifact or code-defined transition that produces exactly 97 by applying
both `relevance >= 4` and `year >= 2019` to the 5,512 Algorithmic records. The historical
claim should be treated as unresolved until the author team identifies an omitted file,
script, or manuscript table source. It must not be encoded as reconstructed behavior.

### 4.3 Evidence that requires prospective regeneration

The following PRISMA elements cannot be reconstructed defensibly from the historical
workspace. They therefore require a new, prospectively logged search under the revised
reproducible protocol; this will generate new review evidence rather than recreate the
exact January 2026 database state:

- raw records returned per database and per query, before within-source deduplication;
- API-reported totals, pagination coverage, cursors, and truncation;
- exact retrieval timestamps and search dates;
- language, publication type, year, and other effective filters;
- IEEE Xplore and ACM Digital Library searches, which were not present;
- prior-survey seed sets EBK25, JFR25, and FP19 and their ingestion point;
- record-level duplicate clusters and reasons;
- assistance-stage raw LLM outputs, failures, and rationales;
- record-level title/abstract screening reasons;
- a complete relation between 5,464 Desktop records, 5,402 analysis files, 4,663 parsed
  aggregate objects, and later curated libraries;
- the selection from 141 deduplicated RDF items to 67 curated items;
- human overrides, audit decisions, disagreements, and adjudication; and
- a final eligible-corpus count distinct from a narrative-synthesis sample.

Prior-survey identifiers EBK25/JFR25/FP19 do not occur in the inspected notebooks,
corpus BibTeX/RDF, or task artifacts. A references bibliography contains survey papers,
but it is not an ingestion manifest. That provenance is unrecoverable from the supplied
historical workspace.

## 5. Methodological inconsistencies and risks

### Critical

1. **Eligibility and assistance were conflated.** Assistance P3 says almost every
   visualization tool should default to Algorithmic, while P5 requires qualifying
   AI/automation. These define different scopes.
2. **The strict prompt and parser disagree.** P5 requests Algorithmic/Off-topic examples,
   but the splitter accepts only Bio/Off-topic, coercing valid responses to Off-topic.
3. **The Desktop/Algorithmic path is asymmetric.** Missing abstracts, repeated LLM
   screens, full-text availability, relevance, and year were applied to one dominant
   cell without equivalent denominators for other cells.
4. **Failures can become exclusions.** Unrecognized responses and `ERROR` strings were
   routed to Off-topic rather than a technical-failure state.
5. **The reported 97 is not reproducible.** Surviving artifacts support several different
   nearby counts and no single documented rule.

### High

6. **Source exports are cumulative.** Reused `all_entries` state contaminates filenames
   and prevents direct per-source retrieval counts.
7. **Assistance provenance is lost.** Bucket files preserve label membership but not raw
   response, rationale, prompt, model, timestamp, or settings.
8. **Model identity is incomplete.** Requested aliases survive; provider snapshots or
   resolved versions do not. P6's code comment contradicts its requested model string.
9. **Relevance is unanchored.** Only score 5 has a meaning, so thresholds cannot be
   interpreted consistently or validated as an ordinal scale.
10. **Taxonomies drift.** Conversational is separate in P1-P5, absent from P6's schema,
    and combined with Adaptive in P7. Desktop changes from one category to 2D/3D.
11. **Modality priority rules conflict.** P4's ordered hardware precedence can disagree
    with its central-contribution instruction.
12. **RDF deduplication semantics differ.** `(doi, title)` tuple matching is weaker than
    DOI-first/title-fallback deduplication and has no decision log.

### Material

13. **Input coverage differs.** Title/abstract is usual, journal appears only in one
    classifier redefinition, and 707 of 761 records in one branch lacked abstracts.
14. **Full-text truncation is not token-aware.** The first 120,000 characters may omit
    methods, evaluation, or limitations and the truncation event is not recorded.
15. **JSON was not enforced.** Concatenated and malformed objects complicate counts and
    may silently omit failed outputs.
16. **Existing-output skips are not version-aware.** A changed prompt/model can leave an
    older analysis silently in place.
17. **Human activity is not validation evidence.** Reading assignments do not record
    independent labels, blindness, corrections, or adjudication.
18. **Historical run dates are weak.** File mtimes and notebook execution counts are not
    search timestamps and should not be presented as such.

## 6. Reviewer requirement to software/provenance mapping

| Reviewer requirement | Historical support | Required future evidence/software capability |
|---|---|---|
| M1 substantive methodology | Fragmented notebook code only | Versioned run manifest plus generated methods summary |
| M2 PRISMA flow | Some artifact-state counts | Event-derived counts with reconciliation checks and flow export |
| M3 prior-survey seeds | Unrecoverable | Named seed-set manifests; citation source; ingestion, dedupe, screening, and eligibility events |
| M4 IEEE/ACM searches | Absent | Source adapters or documented manual export import; exact source/query/run provenance |
| M5 exact queries/dates/filters/constraints | Query strings mostly recoverable; dates/coverage not | Immutable query artifacts, source-specific rendered query, fields, filters, timestamps, pagination, caps, errors |
| M6 LLM reproducibility | Prompts and some requested settings recoverable | Prompt versions/hashes, requested and resolved model identity, API surface, parameters, input snapshot, raw/parsed output, retries/errors |
| M7 human validation | No validation evidence | Probability-sampled audit assignments, independent decisions, adjudication, weighted estimates, uncertainty, correction log |
| M8 relevance/eligibility rubric | Score 1-5 with only score 5 anchored | Criterion-level eligibility rubric separate from an anchored synthesis-priority scale |
| M9 symmetric policy | Historical asymmetry demonstrated | One eligibility policy across cells; separate, explicit, reproducible synthesis sampling |
| M10 exclusion reasons | Mostly undifferentiated Off-topic | Versioned reason taxonomy with one primary reason and optional secondary reasons/evidence |
| M11 machine vs author interpretation | Machine JSON and human missions exist but are not linked | Actor/decision provenance; human verification state; synthesis claims linked to reviewed evidence |
| M12 detailed supplement | Historical detail exists but is scattered | Machine-generated appendix containing exact queries, prompts, schemas, run manifests, and count tables |

The conceptual reviewer concerns also constrain schema design without fixing a new
taxonomy. Assistance, modality, task, eligibility, relevance/priority, evaluation, and
provenance must be separate versioned annotation dimensions. The schema must permit a
future agency/mixed-initiative taxonomy without recoding modality as assistance.

## 7. Proposed revised data and provenance schema

This is a schema proposal for approval, not an implementation in this phase.

### 7.1 Run and artifact entities

**`SearchRun`**

- stable run ID, protocol version, software commit, operator;
- source/database and endpoint/API version;
- query-family ID, exact source-rendered query, fields, filters, locale;
- planned and effective date/language/document-type constraints;
- start/end timestamps, page/cursor history, requested cap, API-reported total;
- retrieved occurrence count, partial/failure status, retries, rate-limit evidence; and
- raw-response or export artifact hashes where retention is permitted.

**`RecordOccurrence`**

- one immutable row per returned database hit before deduplication;
- run/query/source IDs, source rank, page/cursor, source identifier;
- raw bibliographic metadata and normalized projection; and
- artifact/content hash.

**`ArtifactManifest` and `TransformationRun`**

- input/output artifact IDs, paths, media types, hashes, record counts;
- transformation name/version/settings/status;
- parent-child edges and reconciliation assertions; and
- supersedes/current designation without deleting earlier outputs.

### 7.2 Canonical records and deduplication

**`CanonicalRecord`** retains normalized bibliographic fields but links to every source
occurrence. It should never overwrite source metadata.

**`DuplicateDecision`** records candidate cluster, compared IDs, normalized DOI/title
keys, matching rule and score, automatic/manual actor, survivor, reason, timestamp, and
software/rule version. Ambiguous clusters remain reviewable rather than silently merged.

### 7.3 Screening and selection

**`ScreeningDecision`** records:

- record, stage (`title_abstract`, `full_text`, or other), protocol/rubric version;
- actor type and actor ID;
- `include`, `exclude`, or `uncertain`;
- criterion-level responses rather than only a single Off-topic label;
- primary exclusion reason, optional secondary reasons, evidence/rationale;
- input fields and content hash actually reviewed;
- timestamp, confidence, and `supersedes_decision_id`; and
- technical status separate from the scientific decision.

The initial exclusion taxonomy should be versioned and capable of representing at least:
duplicate; no interactive visualization; no qualifying assistance; outside life science
and no approved transferable relevance; presentation-only/static visualization;
insufficient or unavailable evidence; publication-type/language/date restriction; and
other with explanation. Final definitions require author approval.

**`EligibilityDecision`** is the adjudicated result of the screening protocol.

**`SynthesisSelectionDecision`** is separate and records sampling/prioritization criteria,
stratum, selection probability, reason, and whether the record was actually cited or
analyzed in narrative synthesis. A record can be eligible but not sampled.

### 7.4 LLM inference and annotations

**`PromptArtifact`** records prompt name, semantic version, exact system and user template,
hash, output schema, taxonomy/rubric versions, and repository commit.

**`InferenceRun`** records provider, requested model, provider-resolved model/snapshot when
available, API surface, date, parameters including temperature/seed/token limits, prompt
artifact, software commit, batching/concurrency policy, and retention policy.

**`InferenceAttempt`** records record ID, exact input fields and input hash, attempt
number, timestamps, response/request IDs, token usage, raw response, parsed response,
validation errors, retry cause, terminal status, and cost when retained. A failed attempt
cannot become a scientific exclusion.

**`Annotation`** records dimension, taxonomy version, value or values, rationale/evidence,
confidence if used, derivation (`llm`, `human`, `deterministic`, `source`), inference or
human-decision ID, status, and supersession. Separate dimensions are required for:

- eligibility criteria;
- assistance relationship;
- visualization modality;
- task;
- relevance or synthesis priority;
- evaluation characteristics; and
- structured full-text extraction fields.

This extends the clean repository's current `ProvenanceEvent`, `InferenceMetadata`, and
`LLMAnnotation` concepts. The current models already capture provider/model/prompt/settings
and broad event types, but need stable run IDs, occurrence-level lineage, prompt/input/raw
response hashes, duplicate/screening/selection decisions, supersession, and explicit
human-review relationships.

### 7.5 Human review and adjudication

**`HumanReviewAssignment`** records sample design/stratum, selection probability, coder,
blindness to LLM output, assigned rubric version, and completion state.

**`HumanDecision`** uses the same criterion and annotation structures as machine output
but retains coder evidence and uncertainty.

**`Adjudication`** links all prior decisions, records the adjudicator/process, final
decision, resolution rationale, taxonomy clarification, and whether earlier corpus
outputs must be invalidated or regenerated.

**`ValidationReport`** records the frozen population, sample frame, random seed, inclusion
probabilities/weights, missing reviews, metrics with intervals, confusion matrices, error
analysis, correction policy, and workload.

## 8. Proposed human-validation workflow

No fixed sample size, fraction, coder count, or agreement statistic is adopted here. The
historical evidence gives a 25,266-record broad artifact with extreme label imbalance, a
5,512-record dominant Algorithmic class, rare assistance classes of 43-125 records, and a
smaller 67-record curated branch. The taxonomy is hierarchical and drifting, and the
reviewers require both screening validity and classification validity. These facts make a
single unstratified percentage indefensible.

Before selecting a design, freeze:

1. the regenerated corpus and its exact source/query/abstract-availability distribution;
2. operational eligibility criteria and exclusion-reason taxonomy;
3. a modality-independent assistance taxonomy and overlap rules;
4. modality labels and rules for multimodal systems;
5. an anchored relevance/synthesis-priority rubric, if relevance is retained;
6. the validation objective and tolerable uncertainty for each key claim; and
7. available coder expertise, time per abstract/full text, and adjudication capacity.

### Design A: two-phase stratified probability audit

Use separate samples for two distinct questions.

**Eligibility audit:** sample from both machine-included and machine-excluded records,
stratified by source/query family, abstract availability, LLM decision, and any uncertainty
or technical-failure state. Oversample rare exclusion reasons and boundary cases, while
retaining known inclusion probabilities so estimates can be weighted back to the corpus.
Report false-exclusion risk (sensitivity/negative predictive behavior), false-inclusion
risk, reason-specific confusion, and confidence intervals against adjudicated human
decisions.

**Taxonomy audit:** sample from the adjudicated eligible corpus, stratified by assistance
and modality labels and relevant combinations. Rare classes should be deliberately
oversampled; a census of a rare class is an option only when its eventual size and review
cost make that more efficient than sampling. Do not let the dominant Algorithmic/Desktop
cell determine nearly the entire audit.

This is the preferred design when the regenerated corpus remains large and imbalanced. It
supports population estimates through weights while still exposing errors in rare cells.

### Design B: sequential precision-driven audit

Start with a probability sample allocated across the same strata. Use the pilot only to
estimate prevalence, disagreement, review time, and stratum-specific error. Then expand
strata whose confidence intervals are too wide or whose error rates threaten manuscript
claims. Stop according to predeclared precision or error-tolerance rules, not when an
arbitrary percentage is reached.

This design is appropriate because current error rates and the final taxonomy are unknown.
It avoids pretending that historical class proportions supply a valid sample calculation.
The expansion and stopping decisions must be scripted and archived before inspecting
substantive paper results.

### Design C: conditional full verification of a reduced eligible set

If transparent deterministic/title-abstract screening reduces the regenerated population
to a genuinely small eligible set, compare the measured cost of full eligibility review
with Designs A/B. Full human verification may then be more efficient and may eliminate
sampling error for final eligibility, while taxonomy or structured extraction can still
receive independent double-coding on a probability sample. This is a conditional option,
not a requirement and not a justification for repeating the historical asymmetric filter.

### Coder design and adjudication

The strongest feasible design uses independent human judgments made from the frozen
rubric and source evidence, blinded to the LLM label during initial coding. Independence
is necessary for a meaningful agreement estimate. The staffing choice should be made from
the required domain/visualization expertise and workload:

- a resource-minimum design can use one primary human judgment across the sample and an
  independent judgment on the subset used for reliability estimation;
- a stronger design independently codes the entire validation sample before adjudication;
- if more than two coders are available, rotate assignments with planned overlap so coder
  effects can be estimated without requiring everyone to code everything.

Disagreements should be adjudicated after independent coding by a designated process that
records the final decision, rationale, and any rubric change. If adjudication exposes an
ambiguous rule, revise/version the rubric, identify all affected records, and rerun or
re-review them. Never silently overwrite the original machine or human decisions.

### Measures

Choose measures based on the frozen question and coder design rather than naming one
universal statistic:

- eligibility: confusion matrix, sensitivity/recall of eligible records, specificity,
  positive/negative predictive values, and Wilson or design-based confidence intervals;
- nominal assistance/modality: class-wise precision/recall, macro averages, confusion
  matrix, raw agreement, and an agreement coefficient appropriate to coder count;
- severe imbalance: report Gwet's AC1/AC2 or prevalence-adjusted sensitivity analyses in
  addition to, not as a hidden replacement for, kappa-type statistics;
- two fixed coders: Cohen's kappa where assumptions fit;
- multiple/variable coders or missing ratings: Fleiss' kappa or Krippendorff's alpha as
  appropriate;
- ordinal relevance: weighted kappa or an ordinal model only after every level has an
  operational anchor; and
- complex multi-label taxonomy: dimension-level agreement plus exact-set and per-label
  metrics rather than forcing a single nominal category.

All estimates from unequal-probability samples must use sampling weights and design-aware
uncertainty. Report both pre-adjudication human-human agreement and LLM-versus-adjudicated
performance; adjudicated agreement alone is not an independence measure.

### Sample size and workload determination

For each target proportion, choose a tolerable confidence level and half-width, estimate
the proportion conservatively or from the pilot, apply finite-population correction, and
inflate for stratification/design effects and missing reviews. Allocate across strata to
protect rare classes and key manuscript comparisons; use Neyman or cost-aware allocation
when pilot variance and review time are available. Predeclare whether the primary target
is overall performance, worst-class recall, or precision within key cells, because these
produce different sample sizes.

Workload should be estimated before approval as:

```text
screening work = assignments x measured minutes per title/abstract
full-text work = assignments x measured minutes per full text
adjudication work = expected disagreements x measured minutes per resolution
training/calibration = coder sessions + rubric-revision and recoding time
```

The report to the authors should present at least two cost/precision options generated
from the pilot, including expected records per stratum, total independent judgments,
adjudications, hours by expertise type, and the claims each option can support. No
`20 per category`, `20%/200`, or full-corpus rule is adopted by this report.

## 9. Proposed implementation plan

No implementation begins until this report, the conceptual taxonomy, and the validation
design decision are approved.

1. **Archive sanitized methodology artifacts.** Extract P1-P7 and exact historical query
   families into credential-free, hashed prompt/query artifacts. Preserve the historical
   text verbatim and label defects; do not silently repair it. Keep historical prompt
   artifacts distinct from corrected/current descendants (for example,
   `prompts/historical/` versus `prompts/current/`), and assign new hashes to every
   corrected descendant.
2. **Add run/occurrence/artifact schemas.** Extend the current models for search runs,
   record occurrences, transformation manifests, and reconciliation without changing
   existing deterministic behavior unexpectedly.
3. **Add decision schemas.** Implement versioned duplicate, screening, eligibility,
   exclusion, synthesis-selection, human-review, and adjudication records.
4. **Implement LLM provenance offline first.** Add prompt artifacts, inference attempts,
   raw/parsed output validation, retry/error states, and mocked clients. Keep live calls
   disabled by default.
5. **Build importers for historical evidence.** Import surviving artifacts as explicitly
   `historical_observed` states with evidence grade and source hash. Do not invent missing
   events or treat filenames as truth.
6. **Freeze the revised protocol.** Authors approve scope, eligibility, exclusion reasons,
   assistance/modality taxonomy, relevance anchors, and the eligible-versus-synthesis
   distinction.
7. **Implement source-specific reproducible searches.** Include the existing five sources
   plus approved IEEE Xplore/ACM DL procedures and named prior-survey seed manifests. Log
   rendered queries, dates, fields, caps, pages, totals, and failures.
8. **Regenerate corpus and PRISMA evidence.** Run deterministic normalization/deduplication,
   preserve every occurrence and decision, and produce reconciliation-checked flow tables.
9. **Pilot and select validation design.** Measure class balance, error prevalence, coder
   time, and ambiguity; present cost/precision alternatives from Section 8 for author
   approval before the full audit.
10. **Execute validation and corrections.** Preserve independent decisions, adjudication,
    weighted metrics, and correction provenance; rerun affected derived artifacts.
11. **Generate manuscript evidence.** Export the PRISMA diagram data, methods table, prompt
    and query supplement, validation tables, exclusion counts, and machine-versus-author
    provenance statement from the recorded events.
12. **Regression and integrity checks.** Require arithmetic reconciliation, schema
    validation, deterministic offline tests, artifact hashes, secret scanning, and a clean
    historical-workspace check before any release.

## Appendix A: Historical search-query inventory

### A.1 Early 20-cell query family

The base query required biological/molecular/protein interaction networks, interactomes,
systems biology, genetic/metabolic/gene-regulation networks or pathways, together with
network layout, graph visualization, topology, connectivity, node-link diagrams, or
pathway maps.

Assistance blocks:

- Algorithmic: graph layout algorithm, machine-learning layout, automatic visualization,
  clustering, graph embedding;
- Adaptive: adaptive interface, context-aware visualization, personalized visualization,
  user modeling;
- Conversational: natural-language/conversational interface, chatbot, dialog system,
  large language model, LLM;
- Immersive initial: immersive analytics/visualization, AI-assisted VR, intelligent
  virtual environment;
- Immersive revised: spatial guidance, viewpoint recommendation, immersive/embodied agent,
  gaze adaptation, attention guidance, in-situ recommendation, in-environment assistant,
  3D navigation support, virtual assistant, contextual cues.

Modality blocks:

- 2D: graph/network visualization, node-link, topology, with source-dependent exclusion
  of VR/AR/XR/CAVE/immersive/HMD terms;
- Large Display: large/display/tiled wall or powerwall;
- VR: virtual reality, VR, HMD;
- AR/XR: augmented/mixed reality, AR/XR, spatial computing;
- CAVE: CAVE, Cave Automatic Virtual Environment, fish-tank VR, immersive wall.

The exact rendered query depended on source because only PubMed and Europe PMC received
the negative 2D block. Exact source-rendered strings and dates were not persisted.

### A.2 Broad six-family query set

The notebook's exact unfielded query strings were:

```text
visual analytics interactive visualization visualisation information visualization
infovis scientific visualization visual exploration

virtual reality VR augmented reality AR extended reality XR mixed reality MR immersive
visualization stereoscopic CAVE head-mounted display HMD WebXR WebVR powerwall

biological visualization biomedical visualization bioinformatics visualization
neuroscience visualization genomics transcriptomics proteomics metabolomics gene
expression pathway connectome

graph visualization network visualization node-link topology interactome connectome
biological network

single-cell single cell spatial omics transcriptomics proteomics metabolomics omics
transomics

artificial intelligence machine learning deep learning intelligent visualization adaptive
visualization conversational visualization recommendation system
```

These were executed as separate query-family strings, not as one Boolean intersection.
Consequently, the retrieval set was intentionally broad and depended heavily on later
screening.

### A.3 PubMed four-query set

The PubMed branch used four Title/Abstract searches:

- visual analytics/exploratory visualization/visual exploration AND a life-science block;
- visualization/immersive AND an XR block AND a life-science block;
- graph/network/pathway/interactome/connectome AND visualization/visual analytics AND a
  biology/biomedical/neuroscience block; and
- single-cell/spatial/omics/transcriptomics/proteomics/metabolomics/transomics/gene
  expression AND visualization/interactive/exploratory/visual analytics.

The code used `[Title/Abstract]` and `[tiab]` variants during evolution. The final recorded
run list was `query_va_bio`, `query_xr_bio`, `query_graph_bio`, and `query_singlecell`, with
nominal `retmax=12000` per query. Search date, PubMed translation, API count, and effective
pagination coverage were not stored.

## Appendix B: Approval questions for Phase 3B

The next phase should not start until the author team decides:

1. whether this historical account and its uncertainty labels are accepted;
2. which conceptual assistance taxonomy will govern the regenerated corpus;
3. the operational eligibility and exclusion rubric;
4. whether relevance remains an ordinal synthesis-priority field and, if so, its anchors;
5. which historical artifacts are evidence-only versus candidates for import;
6. the reproducible procedure for IEEE Xplore, ACM DL, and prior-survey seeds; and
7. which validation design should be piloted after regenerated class distributions and
   coder-time estimates are available.

Until those decisions are made, no historical count should be promoted to the revised
methodology merely because it appears in a filename or manuscript response matrix.
