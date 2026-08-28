# Foundational Agency CytoCave Demo

This hand-curated, offline semantic-validation dataset combines four methodology
foundations from `docs/H2H2/BackgroundPapers` with seven representative systems from
`docs/H2H2/SamplePapers`: iCAVE, Biowheel, FathomNet, DTBIA, AEGIS, the single-cell
LLM workflow, and PhenoFlow.

`source_graph.json` records paper metadata, candidate mechanism decompositions, and
curated relations. Supported Delegation, Epistemic Mediation, Target, Agency Direction,
DisplayType, Adaptability, and Interaction Modality assignments are marked
`classification_status = provisional` and retain field-level evidence. Unsupported
assignments remain `unknown`; conceptual mechanism families remain explicitly pending
system-level decomposition. This dataset does not freeze or revise the ontology.

The top-level `publication_order` is a version-controlled, provisional conceptual
ordering of the 11 papers. The exporter maps it to centered, evenly spaced publication
planes; every paper anchor and all of its mechanisms receive the same Z coordinate.
`publication_plane_spacing` is configurable and defaults to `1.0`.

Regenerate the loader-ready `data/` tree from the repository root:

```bash
python -m h2h_lit.cytocave \
  examples/cytocave/foundational_agency/source_graph.json \
  examples/cytocave/foundational_agency \
  --dataset-folder H2H_Foundational_Agency \
  --subject-id foundational_agency \
  --atlas-suffix h2h_foundational_agency \
  --publication-plane-spacing 1.0
```

The output is staged in CytoCave's expected relative layout. It can be copied into a
CytoCave checkout and opened with:

```text
visualization.html?dataset=H2H_Foundational_Agency&load=0&lut=h2h_foundational_agency
```

The JSON sidecar is intentionally richer than the current renderer and preserves node
IDs, original citation direction, classification status and evidence, collaboration
descriptors, publication order and planes, visual mappings, glyph size, and future glyph
aspect ratios.
