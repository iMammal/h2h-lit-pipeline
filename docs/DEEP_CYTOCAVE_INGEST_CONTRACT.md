# DeepCytoCave / NeuroCavePlus Ingest Contract

This document records the ingest contract implemented by the current
NeuroCavePlus front-end code and the bundled demo data. It is written for
generating NeuroCavePlus-ready files from an external graph-analysis pipeline.

The reliable ingest path is the dataset-folder path:

```text
visualization.html?dataset=<dataset-folder>&load=0&lut=<atlas-suffix>
```

The app then loads:

```text
data/<dataset-folder>/index.txt
data/<dataset-folder>/<network file named by index.txt>
data/<dataset-folder>/<topology file named by index.txt>
data/LookupTable_<atlas-suffix>.csv
```

The relevant ingest source files are:

- `js/utils/parsingData.js`: loads dataset index, network, topology, and LUT.
- `js/model.js`: interprets topology headers, clusters, coordinates, matrices,
  source/target/weight triples, and sparse JSON matrices.
- `js/atlas.js`: interprets LUT columns and color-coding fields.
- `js/previewArea.js`: draws nodes and edges from the parsed model.
- `index.html`, `js/globals.js`, `js/main.js`, `js/GUI.js`: URL, folder, atlas,
  and subject-menu behavior.
- `data/SciVisIEEE2023/csv2sparsejson.js`: standalone conversion tool for
  turning large sparse triple CSV files into `mathjs` sparse matrix JSON.

## Dataset Index

Each ingestible dataset folder must contain `index.txt`.

Schema:

```csv
subjectID,network,topology
case001,case001_edges.csv,case001_topology.csv
case002,case002_edges.csv,case002_topology.csv
```

Rules:

- Delimiter: comma.
- Header: required, with exact field names `subjectID`, `network`, `topology`.
- File paths are relative to `data/<dataset-folder>/`.
- File names themselves are not hard-coded. Existing demos use names such as
  `NWfemale.csv` and `topologyfemale.csv`, but the loader uses the names in
  `index.txt`.
- `network` may name a `.csv`, `.php`, or `.json` file. CSV/PHP network files
  are parsed with PapaParse. JSON network files are fetched and passed to
  `math.SparseMatrix.fromJSON()`.
- The left and right subject menus are populated from `subjectID`. Both default
  to the first row.

The landing-page folder chooser does not upload files. It only reads the chosen
folder name and assumes a matching folder already exists under `data/`.

## Topology File

The topology file carries node labels, one or more coordinate systems, and
optional cluster/module columns.

Example:

```csv
label,MNI,,,UPENNModuleClustering,Q
101,-31.2,44.0,12.5,1,2
102,33.1,39.2,10.0,2,1
103,-5.0,-20.5,42.0,1,2
```

Rules:

- Delimiter: comma.
- Parser setting: `header: false`. The first row is still required, but it is
  interpreted manually by `model.setTopology()`.
- The node-label column header must be exactly `label`.
- Each subsequent data row is one node.
- Data-row order defines the node index used by the edge file. Edge IDs are
  zero-based row indices, not LUT labels.
- Label values are used only to look up metadata in the LUT. They may be atlas
  labels such as `101`, but every label value must exist in the LUT.

Coordinate-system columns:

- Any non-empty header cell that is not `label` and is not recognized as a
  cluster header is interpreted as the name of a 3-column coordinate block.
- The named coordinate column consumes that column plus the next two columns as
  `x,y,z`.
- Header cells for the second and third coordinate columns should be empty.
  Do not use `x,y,z` as separate header names; `y` and `z` would be treated as
  new coordinate systems.
- Coordinates must be numeric.
- Coordinates are passed as `(x, y, z)` into `THREE.Vector3`, then globally
  scaled into roughly `[-500, 500]` and centered. Original units are not
  preserved. No RAS/LPS/MNI orientation transform is applied by the loader.
- The first coordinate or cluster topology encountered becomes the default
  active topology.

Valid coordinate-header pattern:

```csv
label,MNI,,,Isomap,,,
101,-31.2,44.0,12.5,0.12,-0.45,1.02
```

Cluster/module columns:

- A column is treated as cluster data only if its header is exactly one of:
  `PLACE`, `PACE`, `Q`, `Q-Modularity`, or contains the substring
  `Clustering`.
- For custom modules, prefer a header such as `UPENNModuleClustering`. The
  displayed/internal name becomes `UPENNModule` because the code removes the
  substring `Clustering`.
- Cluster IDs must be 1-based positive integers. ID `0` is not placed.
- Cluster IDs should be contiguous: `1..K`.
- Non-hierarchical cluster layouts support at most 20 clusters.
- `PLACE` and `PACE` are treated as hierarchical. Keep them to at most 16 final
  clusters because the UI assumes clustering levels 1 through 4.

Validation checklist:

- Exactly one `label` column.
- `N` data rows, where `N` equals the number of graph nodes.
- Every topology label exists in the LUT `label` column.
- Every coordinate header has exactly two following numeric coordinate columns.
- No accidental non-empty axis headers in coordinate padding columns.
- Every cluster/module value is an integer in `1..K`; no zero-based clusters.

## Edge / Network File

The current model accepts dense CSV matrices, sparse triple CSV files, and
`mathjs` sparse matrix JSON. For a graph-analysis pipeline, source/target/weight
triples are the practical source format. For large highly connected datasets,
pre-converted sparse JSON is the faster optional load format.

Triple example:

```csv
0,1,0.82
1,0,0.82
0,2,0.35
2,0,0.35
```

Rules for triple files:

- Delimiter: comma.
- Header: none. If the first row has three columns, every row is processed as
  data. A header such as `source,target,weight` will be treated as graph data.
- Columns are exactly:

```text
source_node_index,target_node_index,weight
```

- `source_node_index` and `target_node_index` are zero-based topology row
  indices in `0..N-1`.
- Node IDs are not LUT labels. If an upstream pipeline emits 1-based node IDs,
  subtract 1 before writing this file.
- `weight` must be numeric. Current edge rendering only uses weights greater
  than zero. Zero and negative weights are ignored by the active edge-drawing
  path.
- Duplicate rows with the same `(source, target)` are not accumulated. The last
  value written to the sparse matrix wins.
- Self-edges are not removed in the current active edge path. A positive
  self-edge can draw a zero-length line. Omit self-edges or set diagonal matrix
  weights to zero.

Direction and symmetry:

- The sparse matrix stores triples at `[source, target]`; there is no automatic
  symmetrization.
- Rendered lines have no arrows and are visually undirected.
- The current `getConnectionMatrixRow()` implementation actually retrieves the
  selected node's incoming column by multiplying the matrix by a one-hot vector.
  For a single triple `0,1,0.82`, selecting node `1` can show the connection to
  node `0`, but selecting node `0` will not show that edge.
- For undirected connectomes, write both directions for every edge:

```csv
0,1,0.82
1,0,0.82
```

- For directed analyses, treat direction as ambiguous in the current renderer.

Triple-file dimension caveat:

- Triple files do not carry an explicit node count. The sparse matrix size is
  inferred from the largest source and target indices that appear.
- If a high-index node is isolated, triple mode may not establish enough matrix
  dimensions for selecting that node. Dense adjacency-matrix mode is safer when
  isolated nodes must be preserved exactly.

Legacy matrix alternative:

- If the first parsed row does not have exactly three columns, the file is
  interpreted as a dense adjacency matrix.
- Matrix files are comma-delimited, numeric, no header, `N` rows by `N` columns.
- Matrix rows/columns are zero-based topology row indices.
- Existing demos mostly use this matrix format.
- A 3-node dense matrix is ambiguous because its first row has exactly three
  columns and will be misclassified as triples.

Sparse JSON alternative:

- If the `network` filename in `index.txt` ends with `.json`, the app fetches
  the file and calls `math.SparseMatrix.fromJSON(jsonData)`.
- The JSON must be the object returned by `mathjs` `SparseMatrix.toJSON()`.
  A typical shape starts like this:

```json
{
  "mathjs": "SparseMatrix",
  "values": [0.82, 0.35, 0.82, 0.35],
  "index": [1, 2, 0, 0],
  "ptr": [0, 2, 3, 4],
  "size": [3, 3]
}
```

- Do not invent a custom JSON edge-list schema unless the loader is changed.
  The current loader expects `mathjs` sparse-matrix JSON, not
  `{source,target,weight}` records.
- The same node-index, direction, duplicate, self-edge, and positive-weight
  rules apply, because the JSON matrix represents the same sparse adjacency
  matrix as the triple CSV.
- The JSON `size` should be at least `[N, N]`. The bundled converter does not
  explicitly force size; it lets `mathjs` infer size from the largest row/column
  indices that are actually set. If isolated high-index nodes must be preserved,
  adjust the converter or use another `mathjs` construction path that writes the
  desired size.

Standalone CSV-to-sparse-JSON converter:

- `data/SciVisIEEE2023/csv2sparsejson.js` scans the current working directory
  for every `.csv` file, reads each one as sparse triples, builds a `mathjs`
  sparse matrix, and writes `<same-basename>.json`.
- Intended input rows are:

```csv
0,1,0.82
1,0,0.82
0,2,0.35
2,0,0.35
```

- Intended output can be referenced directly from the dataset index:

```csv
subjectID,network,topology
case001,case001_edges.json,case001_topology.csv
```

- The converter uses `csv-parser` and `mathjs`, so run it with Node from an
  environment where project dependencies are installed.
- Run it in a staging folder containing only network triple CSVs, or change the
  `files` filter. As written, it attempts to process every `.csv` in the
  current directory, including topology/position CSVs.
- The SciVis copy currently calls `csv()` without `headers: false`. For
  headerless triple files, the safer/correct pattern is the BioVis copy's
  `csv({ headers: false })`; otherwise `csv-parser` may treat the first data row
  as headers. If your triple CSV has a header, remove it before conversion or
  update the script to skip it deliberately.
- `const matrixSize = 50000` is present in the SciVis script but is unused. It
  documents the challenge-scale matrix size; it does not force the output JSON
  dimensions.
- Invalid rows are logged and skipped; the script does not abort conversion on
  the first bad row.

Validation checklist:

- No header row.
- Exactly 3 comma-separated fields per row for triple mode.
- All node IDs are integers in `0..N-1`.
- All visible edge weights are finite and positive.
- No duplicate `(source, target)` rows unless last-value-wins behavior is
  intended.
- No positive self-edges.
- For undirected graphs, emit symmetric pairs.
- For JSON mode, verify the JSON has `"mathjs": "SparseMatrix"` and a size at
  least `[N, N]`.

## LUT File

The LUT maps topology labels to node metadata and color-coding categories.

Required filename:

```text
data/LookupTable_<atlas-suffix>.csv
```

The `<atlas-suffix>` is selected by the URL query parameter `lut=<atlas-suffix>`.
For example, `lut=upenn_gbm` loads:

```text
data/LookupTable_upenn_gbm.csv
```

Example:

```csv
label;Anatomy;region_name;hemisphere;UPENNRegion;TumorNeighborhood
101;leftFrontal;Left frontal ROI 101;left;Frontal;peritumoral
102;rightFrontal;Right frontal ROI 102;right;Frontal;distant
103;leftTemporal;Left temporal ROI 103;left;Temporal;edema
```

Rules:

- Delimiter: semicolon.
- Header: required.
- Required columns, case-sensitive:
  `label`, `Anatomy`, `region_name`, `hemisphere`.
- `label` values must cover all labels used in the topology file.
- `hemisphere` must be exactly `left` or `right`. Rendering creates only those
  two hemisphere buckets.
- `Anatomy` is both required metadata and the default color-coding group.
- `region_name` is used as the displayed node name.
- Any additional column is allowed and becomes a color-coding option. Keep
  high-cardinality fields modest if you want a readable legend.

The top-level file `data/index.txt` lists atlas suffixes for the landing-page
dropdown, comma-separated and without a header:

```csv
freesurfer,baltimore,mni
```

The current landing-page JavaScript only iterates the first three entries in
that list. Direct demo links or manual URLs can still pass any `lut=` value as
long as `data/LookupTable_<lut>.csv` exists.

Validation checklist:

- Semicolon-separated, not comma-separated.
- Exact required header names and case.
- Every topology label exists in the LUT.
- Every `hemisphere` is `left` or `right`.
- No empty `Anatomy` or `region_name` values.


## Glyph Shape and Size Metadata

The current `cytocave-rest-session` branch supports multiple 3D node glyphs and
per-glyph size control through the CytoCave API/session layer.

Current built-in glyph shapes:

```text
sphere
cube
tetra
icosa
star
```

The intended metadata field name for exported node shape is:

```text
glyph_shape
```

Recommended LUT usage:

```csv
label;Anatomy;region_name;hemisphere;glyph_shape;glyph_size
101;paper;Paper A;left;sphere;1.4
102;mechanism;Mechanism A;left;tetra;1.0
103;mechanism;Mechanism B;left;star;1.2
```

Rules and current limitations:

- `glyph_shape` is a categorical node-style attribute.
- Use one of the currently supported values:
  `sphere`, `cube`, `tetra`, `icosa`, or `star`.
- Keep values lowercase in exported data so that producers have one canonical
  spelling.
- `glyph_size` is a numeric scalar intended for per-node glyph-size control.
- A value of `1.0` should be treated by exporters as the neutral/default scale
  unless a consuming API configuration specifies otherwise.
- The five shapes are renderer capabilities, not scientific semantics. External
  pipelines such as H2H/LitCave may map any categorical attribute (for example,
  display type) onto these shapes.
- Shape mappings should therefore remain configurable rather than being
  hard-coded into the general CytoCave ingest contract.
- Additional future shape names should be treated as an extensible registry
  rather than requiring changes to literature or graph-analysis schemas.
- Per-axis aspect-ratio rendering is not part of the current folder ingest
  contract. Exporters may preserve future-facing metadata such as
  `glyph_aspect_x`, `glyph_aspect_y`, and `glyph_aspect_z`, but current CytoCave
  builds may ignore those fields until renderer support is added.
- Arbitrary additional LUT columns are already legal, so `glyph_shape`,
  `glyph_size`, and future aspect-ratio fields can be retained in LUT metadata
  even when a particular front-end path does not automatically apply them.

Important distinction:

- The dataset-folder ingest path documented above loads node metadata from the
  LUT.
- Shape/size rendering in the current REST/session branch is controlled by the
  newer CytoCave API/session styling capabilities.
- Therefore, producers should preserve `glyph_shape` and `glyph_size` in the
  exported metadata, but should not assume that every legacy folder-only load
  path automatically applies those fields without the corresponding
  REST/session styling step.

Recommended exporter validation:

- Every non-empty `glyph_shape` value is in the supported glyph registry.
- Every non-empty `glyph_size` value is finite and greater than zero.
- Missing shape values fall back to the application's default glyph rather than
  causing export failure.
- Missing size values fall back to the application's default size.
- Shape and size mappings are deterministic for reproducible exports.

## Recommended UPENN-GBM Output Set

For a single subject or group:

```text
data/UPENN_GBM/index.txt
data/UPENN_GBM/case001_edges.csv
data/UPENN_GBM/case001_topology.csv
data/LookupTable_upenn_gbm.csv
```

For faster loading of large sparse networks, also generate:

```text
data/UPENN_GBM/case001_edges.json
```

Load with:

```text
visualization.html?dataset=UPENN_GBM&load=0&lut=upenn_gbm
```

Minimal files:

`data/UPENN_GBM/index.txt`

```csv
subjectID,network,topology
case001,case001_edges.csv,case001_topology.csv
```

For faster sparse JSON loading, point `network` at the converted JSON instead:

```csv
subjectID,network,topology
case001,case001_edges.json,case001_topology.csv
```

`data/UPENN_GBM/case001_topology.csv`

```csv
label,MNI,,,UPENNModuleClustering
101,-31.2,44.0,12.5,1
102,33.1,39.2,10.0,2
103,-5.0,-20.5,42.0,1
```

`data/UPENN_GBM/case001_edges.csv`

```csv
0,1,0.82
1,0,0.82
0,2,0.35
2,0,0.35
```

`data/LookupTable_upenn_gbm.csv`

```csv
label;Anatomy;region_name;hemisphere;UPENNRegion
101;leftFrontal;Left frontal ROI 101;left;Frontal
102;rightFrontal;Right frontal ROI 102;right;Frontal
103;leftTemporal;Left temporal ROI 103;left;Temporal
```

## Ambiguities and Gotchas

- The README documents network files as dense adjacency matrices, but
  `model.setConnectionMatrix()` now has a triple-file branch when the first row
  has exactly three columns, and `loadSubjectNetwork()` also supports `.json`
  sparse matrices.
- The SciVis challenge data includes large sparse triple CSVs and a standalone
  `csv2sparsejson.js` converter for producing faster-loading sparse JSON.
  JSON support is active only when `index.txt` names the `.json` file.
- Directed edge semantics are not reliable for interpretation because the
  renderer draws plain lines and the current row accessor returns incoming
  columns.
- Cluster names containing `Clustering` are transformed by simple string
  replacement. A header `XYZ-Clustering` becomes `XYZ-`, while
  `XYZClustering` becomes `XYZ`.
- The upload page appears stale. In `upload.html`, connection inputs are named
  `anatomyConnectionsLeft` and `anatomyConnectionsRight`, while
  `uploadNormalConnections()` looks for `anatomyConnections`.
- The app performs little hard validation. Most schema violations fail later as
  missing LUT records, undefined hemisphere buckets, undefined centroids, or
  empty edge lists.
- The current REST/session branch adds five 3D glyph shapes (`sphere`, `cube`,
  `tetra`, `icosa`, `star`) plus per-glyph size control. These styling
  capabilities are newer than the legacy dataset-folder parser, so metadata can
  preserve `glyph_shape`/`glyph_size` even where a folder-only load does not
  automatically apply them.
