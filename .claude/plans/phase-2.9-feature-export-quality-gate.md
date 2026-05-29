---
phase: 2.9
name: Feature Export Tables and Measurement Quality Gate
status: passed
source_plans:
  - phase-2.8-eddie-container-validation.md
  - ../../docs/superpowers/plans/2026-05-19-phase-2.9-feature-export-quality-gate.md
  - ../../docs/superpowers/specs/2026-05-21-phase-2.9-plate-export-join-repair-design.md
loops_total: 7
---

# Phase 2.9: Feature Export Tables and Measurement Quality Gate

## Objective

Convert DeepProfiler native `.npz` outputs into plate-level measurement tables
with `Metadata_*` metadata columns, object/site statistics, and explicit NaN
quality reporting.

## Scope

In scope:

- DeepProfiler `.npz` schema inspection.
- Cell-level CSV export.
- Site-level CSV export.
- Manifest CSV export.
- Quality CSV export.
- Optional Parquet copies.
- CLI backfill with `cptools2 export-features`.
- CPU-only Nextflow export stage.
- Backfill validation on the existing representative Eddie run.
- One clean Eddie full-plate certification run with DataStore stage-in and
  stage-out before the phase is marked passed.

Out of scope:

- Normalization.
- Batch correction.
- Well-level aggregation.
- Hit calling.
- MoA prediction.
- UMAP or visualization.

## Acceptance Target

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8
```

Expected facts:

```text
384 .npz files
384 site rows
62,657 cell rows
672 feature columns
116 files with NaNs
46 all-NaN files
```

## Success Criteria

- `deepprofiler_manifest.csv` has one row per `.npz`.
- `deepprofiler_sites.csv` has one row per image set/site.
- `deepprofiler_cells.csv` has one row per DeepProfiler object.
- `deepprofiler_quality.csv` reports NaN and object statistics per `.npz`.
- NaN feature values are preserved in `deepprofiler_cells.csv` so users can see
  affected objects directly.
- Low object-level NaN burden is treated as a quality warning, while high
  burden or all-NaN sites are explicit site-level quality failures for
  downstream review/filtering.
- The object-level NaN fail threshold is configurable with
  `feature_export.nan_object_fail_fraction`; the default is `0.05`.
- All exported metadata columns use `Metadata_*`.
- Empty, partial-NaN, all-NaN, unsupported-schema, and normal cases are tested.
- Nextflow export runs without GPU resources.

## Backfill Evidence

Representative DeepProfiler feature export completed on Eddie against:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/outputs/sarah-representative/features
```

Evidence and exported tables:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-feature-export-backfill/evidence.md
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-feature-export-backfill/export/tables
```

Observed facts:

```text
manifest rows: 384
site rows: 384
cell rows: 62657
quality rows: 384
cell feature columns: 672
files with NaNs: 116
all-NaN files: 46
NaN values: 5300064
total feature values: 42105504
quality status counts: pass 268, has_nan 70, all_nan 46
```

The first backfill attempt exposed a scalability issue in the exporter: building
the full wide cell table in memory failed with a Polars/Rust allocation error.
The exporter now streams required CSV cell output and keeps only small
manifest/site/quality summaries in memory.

Final representative backfill with the configurable NaN object gate completed
on Eddie on 2026-05-21:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-feature-export-backfill-final-20260521/export/tables
```

The final rerun confirmed the formal acceptance facts above and the added quality
gate fields:

```text
quality gates: pass 268, warn 8, fail_site 108
cell object quality fields:
  Metadata_Object_NaN_Count
  Metadata_Object_NaN_Fraction
  Metadata_Object_All_NaN
  Metadata_Object_Has_NaN
  Metadata_Object_QualityStatus
```

The final rerun also exposed that Polars CSV writing can fail on Eddie with
async executor `Resource temporarily unavailable` panics when running the
representative backfill without a constrained Polars thread pool. Required CSV
tables now use the Python CSV writer; Polars remains only for optional Parquet
copies.

## Nextflow Wiring Smoke

A bounded Nextflow smoke completed on Eddie using two existing feature chunks
and the CPU-only `EXPORT_FEATURES` module:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-nextflow-export-smoke/evidence.md
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-nextflow-export-smoke-venv/evidence.md
```

Observed facts for both published and destaged table copies:

```text
manifest rows: 48
site rows: 48
cell rows: 6579
quality rows: 48
cell feature columns: 672
EXPORT_FEATURES: COMPLETED, exit 0
STAGE_OUT: COMPLETED, exit 0
```

The smoke exposed a runtime dependency issue: the export task must use a Python
runtime with NumPy and Polars installed. The Nextflow module now supports
`params.feature_export_python`, and the Eddie profile points it at the project
virtualenv Python. The project virtualenv was updated with `numpy`, then the
same two-chunk smoke passed using:

```text
/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/.venv/bin/python
```

A fresh one-chunk integrated execution smoke then passed from image processing
through DeepProfiler extraction and CPU-only export using the permanent Eddie
mirror:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-integrated-export-smoke/dp-growth-concurrency8/evidence.md
```

Observed facts:

```text
Nextflow status: 0
FEATURE_EXTRACT: 1/1 completed
EXPORT_FEATURES: 1/1 completed
manifest/site/quality rows: 24
cell rows: 2,855
published and stage-out table digests matched
```

## Clean Full-Plate Certification

Phase 2.9 is not complete until the export path passes inside a clean Eddie
full-plate run that stages the source plate from DataStore and destages outputs
to a fresh destination.

The 2026-05-21 certification run uses:

```text
run root:
  /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-full-plate-clean-20260521
route:
  dp-growth-concurrency8
plate:
  3723-D-100
source:
  /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen
destination:
  /exports/igmm/datastore/Drug-Discovery/Mungo/CellProfiler/test-ai-update-export/phase-2.9-full-plate-clean-20260521
stage_data:
  true
chunk:
  24
max_chunks:
  omitted
feature_export:
  enabled, CSV tables, NaN object fail fraction 0.05
```

The clean run is intentionally separate from the earlier
`sarah-representative` scratch route. That earlier route validated uncapped
chunking and GPU concurrency, but it reused already staged scratch images and
did not certify the Phase 2.9 staging and stage-out path.

The first clean launch failed before SGE submission because the staged export
branch called the Nextflow `STAGE_OUT` component twice in the same workflow.
The workflow now mixes native `FEATURE_EXTRACT` outputs with
`EXPORT_FEATURES` stage-out payloads into one stage-out channel and invokes
`STAGE_OUT` once. The architecture regression is covered in
`tests/test_nextflow_architecture_smoke.py`.

After that fix was synced to the permanent Eddie mirror, the same clean route
was resumed on 2026-05-21. Scheduler acceptance evidence:

```text
stage-in job:
  55783992 nf-STAGE_I
queue state at submission:
  qw
launcher status:
  still running after stage-in submission
```

Phase pass evidence still required from this run:

- launcher status `0`;
- full trace completion for stage-in, image indexing/chunking, Cellpose,
  DeepProfiler, export, and stage-out;
- output counts for `.npz`, masks, manifest, site, cell, and quality tables;
- elapsed time and queue/runtime statistics from trace and `qacct`;
- confirmation that stage-out tables remain export-ready with `Metadata_*`
  metadata and visible NaN quality fields.

The clean run then reached the plate export handoff and exposed a join-contract
failure after all GPU chunks finished:

```text
CELLPOSE_SEGMENT: 96/96 completed
FEATURE_EXTRACT: 96/96 completed
EXPORT_FEATURES: input file name collision on repeated `features` payloads
```

The selected repair is tracked in:

```text
docs/superpowers/specs/2026-05-21-phase-2.9-plate-export-join-repair-design.md
.claude/plans/phase-2.9-ralph-loops.md
```

The repair must make the export payload chunk-aware, keep one CPU plate-level
join, retain deterministic canonical tables, and publish plate-aware CSV names
with an optional caller-provided run label.

Loop 510 completed the reviewed local repair on 2026-05-21:

```text
FEATURE_EXTRACT export handoff:
  explicit plate_id, chunk_id, feature payload directory
EXPORT_FEATURES join:
  one CPU plate task with chunk-scoped staging paths
published names:
  deterministic canonical tables plus plate-aware aliases and optional run_label
review closure:
  spec review clear
  code-quality review clear after format-aware artifact checks and resume-safe staging repair
focused verification:
  107 passed
```

The remaining gate is Eddie certification of that repaired handoff in the
clean DataStore-backed full-plate route.

The first Eddie resume after Loop 510 sync exposed one compile-time handoff bug
before new SGE work was submitted:

```text
Nextflow error:
  No such variable: chunk_manifest
location:
  nextflow/modules/feature_extract.nf export output declaration
root cause:
  the derived chunk_id definition was not available to the output declaration
repair:
  emit val(chunk_manifest.simpleName) directly from the input manifest expression
focused verification:
  tests/test_nextflow_architecture_smoke.py passed
  Phase 2.9 focused suite: 107 passed
```

The corrected 2026-05-21 resume passes workflow compilation. Stage-in,
indexing, chunking, illumination, and Cellpose are cached; `FEATURE_EXTRACT`
recomputes under the new export output contract before the plate-level export
join can be certified.

Final clean full-plate certification completed on Eddie on 2026-05-21:

```text
route status:
  0
Nextflow completion:
  2026-05-21 18:02:08 Europe/London
duration:
  38m 32s
trace:
  FEATURE_EXTRACT completed 96/96
  EXPORT_FEATURES completed 1/1
  STAGE_OUT completed 97/97
cache note:
  stage-in, image indexing/chunking, illumination, and Cellpose reused cache;
  FEATURE_EXTRACT recomputed under the repaired export output contract
```

The repaired plate export join produced canonical tables and plate-aware aliases
on scratch under:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-full-plate-clean-20260521/dp-growth-concurrency8/outputs/3723-D-100/features/tables
```

Observed canonical CSV row counts include the header row:

```text
deepprofiler_manifest.csv: 2305
deepprofiler_sites.csv: 2305
deepprofiler_cells.csv: 457402
deepprofiler_quality.csv: 2305
```

This corresponds to 2,304 exported image-set/site rows and 457,401 exported
cell rows for the clean full plate. The table directory also contains the four
plate-aware aliases prefixed with `3723-D-100__`.

DataStore verification must run on a staging node. A single-slot staging check
on 2026-05-22 verified the final destination contains the same canonical tables
and plate-aware aliases at:

```text
/exports/igmm/datastore/Drug-Discovery/Mungo/CellProfiler/test-ai-update-export/phase-2.9-full-plate-clean-20260521/3723-D-100/features/tables
```

The staging-node destination row counts matched scratch exactly:

```text
deepprofiler_manifest.csv: 2305
deepprofiler_sites.csv: 2305
deepprofiler_cells.csv: 457402
deepprofiler_quality.csv: 2305
```

Phase 2.9 clean full-plate export and stage-out gate is passed.

## Post-Gate Export Hardening

The Phase 2.9 gate review identified two non-blocking exporter follow-ups:

1. explicit `plate_id` previously relabeled DeepProfiler metadata before the
   mismatch check could validate the staged payload;
2. optional cell-level Parquet output still materializes the full wide cell
   table in memory.

The plate identity follow-up was completed on 2026-05-22. The DeepProfiler
export path now:

- derives plate identity from native or chunk-staged DeepProfiler payload paths
  before plate-aware aliases are published;
- rejects wrong-plate, mixed-plate, and malformed payload layouts for normal
  measurement table publication;
- preserves `export_input_manifest.csv` with plate/chunk join routing evidence;
- writes per-plate export status and README artifacts;
- writes a run-level feature export summary that marks exported, failed, and
  missing-status plates so one plate-local export failure does not hide other
  successful exports.

The local closeout verification for this hardening passed:

```text
touched feature export and Nextflow architecture suites: 42 passed
focused CLI/export suite: 11 passed, 34 deselected
ruff on touched feature export Python/tests: passed
```

The user chose to defer a live broken-plate Nextflow smoke until an actual
problem demands it. CSV remains the certified export-ready Phase 2.9 route.
Optional Parquet memory hardening is deferred to the roadmap because the
current Parquet path still constructs the full cell table before writing.

## NaN Investigation and Policy

The DeepProfiler NaN investigation is documented in:

```text
docs/superpowers/plans/2026-05-20-deepprofiler-nan-root-cause-investigation.md
docs/reference/deepprofiler-scalability.md
```

Key conclusions:

```text
NaNs are native DeepProfiler output, not an export artifact.
NaNs are complete 672-feature object vectors, not scattered feature values.
Affected sites have positive object counts.
Simple crop-bound, blank-image, saturation, stale-cache, and wrong-image-size explanations were ruled out for sentinel sites.
```

Policy for Phase 2.9:

- Preserve object rows and NaN feature values in the cell table.
- Report per-site NaN burden in the quality table.
- Allow a small percentage of all-NaN object rows as warning-level data quality.
- Flag high NaN fractions and all-NaN sites as site-level quality failures for
  user review/filtering.
- Use `Metadata_Object_NaN_Count`, `Metadata_Object_NaN_Fraction`,
  `Metadata_Object_All_NaN`, `Metadata_Object_Has_NaN`, and
  `Metadata_Object_QualityStatus` in `deepprofiler_cells.csv` so affected cells
  are visible without dropping rows.
- Use `Metadata_NaN_Object_Count`, `Metadata_NaN_Object_Fraction`,
  `Metadata_NaN_Object_Fail_Fraction`, and `Metadata_QualityGate` in the
  manifest/site/quality tables.
- Configure warning/failure behavior with
  `feature_export.nan_object_fail_fraction`; the Phase 2.9 default is `0.05`.
