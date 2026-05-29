# DeepProfiler Scalability On Eddie

This page records the operational policy for DeepProfiler feature extraction
on Eddie and the evidence needed before changing that policy.

## Default Policy

DeepProfiler defaults to global serialization:

```bash
CPTOOLS2_FEATURE_MAX_FORKS=1
```

This remains the production default until a scalability route passes the
matrix below.

## Why Serialization Exists

DeepProfiler currently runs on an older TensorFlow/CUDA stack. Eddie evidence
shows serialized DeepProfiler runs complete successfully on H200, while
same-node concurrent runs can fail during TensorFlow/cuDNN initialization even
when the jobs have distinct `CUDA_VISIBLE_DEVICES` values.

Typical failure signatures:

```text
Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR
Failed to get convolution algorithm
```

The issue is not that the container image cannot be launched repeatedly. The
problem is concurrent DeepProfiler/TensorFlow GPU work on the same physical
node.

## Routes Under Test

All routes use the generic GPU scheduling policy:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE='-l gpu=1'
```

| Route | Environment |
| --- | --- |
| Serialized baseline | `CPTOOLS2_FEATURE_MAX_FORKS=1` |
| Global concurrency | `CPTOOLS2_FEATURE_MAX_FORKS=2` or higher |
| Host-isolated concurrency | `CPTOOLS2_FEATURE_MAX_FORKS=4`, `CPTOOLS2_DEEPPROFILER_HOST_LOCK=true` |
| TensorFlow memory growth | `CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true` |
| Combined mitigation | host isolation plus TensorFlow memory growth |

## Pass Criteria

A route passes only if all of the following are true:

- Nextflow exits with status `0`.
- All expected `FEATURE_EXTRACT` tasks complete.
- The expected `.npz` count is produced for the tested chunks.
- No unrecovered cuDNN, bus error, or unset CUDA visibility errors remain after
  Nextflow retries.
- `qacct` reports exit status `0` for the feature extraction jobs.
- `CUDA_VISIBLE_DEVICES` is visible in the runtime diagnostics.

## Decision Rules

Use the route outcomes in this order:

1. If global concurrency fails but host-isolated concurrency passes, use
   host-isolated concurrency for the full test plate.
2. If the TensorFlow memory-growth route passes, repeat it at the next larger
   chunk size before raising production concurrency.
3. If only the serialized route passes, run the full test plate with
   `CPTOOLS2_FEATURE_MAX_FORKS=1`.
4. If all DeepProfiler routes fail, stop DeepProfiler scalability work and move
   to the DINO evaluation path.

## Selected Policy For Full Test Plate

Current evidence from the 2026-05-19 real-cell `max_chunks: 5`,
`chunk: 24` matrix:

| Route | Result | Evidence |
| --- | --- | --- |
| `dp-serialized` | Pass: 5/5 FeatureExtract, 360 `.npz`, no errors | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-serialized/evidence.md` |
| `dp-concurrency2` | Fail: 0/4 FeatureExtract completed, cuDNN handle failure | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-concurrency2/evidence.md` |
| `dp-hostlock-concurrency4` | Pass: 5/5 FeatureExtract, 360 `.npz`, no errors | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-hostlock-concurrency4/evidence.md` |
| `dp-growth-concurrency2` | Pass: 5/5 FeatureExtract, 360 `.npz`, no errors | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-growth-concurrency2/evidence.md` |
| `dp-growth-concurrency5` | Pass: 5/5 FeatureExtract, 360 `.npz`, no errors | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-scalability/dp-growth-concurrency5/evidence.md` |

Recommended next step: treat TensorFlow memory growth as the leading route,
because it is the only passing route so far that allows more than one
DeepProfiler task on the same GPU node. The 5-chunk stress route now passes at
`CPTOOLS2_FEATURE_MAX_FORKS=5`, so the next scalability gate is the intended
full-plate chunk count using TensorFlow memory growth. Keep host locking as the
robust fallback if higher allow-growth concurrency fails.

## Full Plate Scalability Gate

Full-plate run root:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability
```

Ramp evidence:

| Gate | Route | Result | Evidence |
| --- | --- | --- | --- |
| `max_chunks=8` | `dp-growth-concurrency8` | Pass: 8/8 FeatureExtract, 576 `.npz`, no errors | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-ramp-max8/dp-growth-concurrency8/evidence.md` |
| `max_chunks=12` | `dp-growth-concurrency8` | Pass: 12/12 FeatureExtract, 864 `.npz`, no errors | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-ramp-max12/dp-growth-concurrency8/evidence.md` |
| uncapped full plate | `dp-growth-concurrency8` | Pass after resume: 16/16 FeatureExtract, 16/16 Cellpose, 384 published `.npz`, 384 unique masks | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency8/evidence.md` |

The full-plate route expanded to 16 chunks at `chunk: 24`. The first uncapped
attempt hit a retryable bus error in Cellpose; after adding exit `135` to the
Eddie retry policy and resuming, all remaining tasks completed. The final
FeatureExtract trace includes one failed attempt for chunk 16 followed by a
successful retry, so the final task outcome is passing with one recovered
attempt failure.

Selected policy for the current full-plate path:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE='-l gpu=1'
CPTOOLS2_FEATURE_MAX_FORKS=8
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true
CPTOOLS2_DEEPPROFILER_HOST_LOCK=false
```

Keep host locking as the fallback policy if future full plates show unrecovered
DeepProfiler bus errors or cuDNN initialization failures after retries.

Phase 2.9 adds a separate CPU-only export stage after `FEATURE_EXTRACT` for
plate-level measurement tables. That export gate does not change the GPU
scalability policy above.

The required Phase 2.9 export-ready route is CSV. DeepProfiler cell CSV export
streams object rows so full-plate output does not need one in-memory wide cell
table. Optional Parquet copies remain available for smaller or explicitly
validated runs, but the current cell-level Parquet writer still materializes the
full wide cell table before writing. Treat batched or streaming Parquet export
as a roadmap scaling task before recommending Parquet for full-plate output.

### Plate Export Identity And Status Reports

The plate-level export join now keeps plate identity checks separate from
publication naming:

- DeepProfiler payload paths are the observed plate identity source;
- Nextflow `plate_id` is a requested assertion for the plate export task;
- wrong-plate, mixed-plate, and malformed payload layouts do not publish
  measurement tables for that plate;
- `export_input_manifest.csv` preserves plate/chunk routing evidence with the
  exported feature artifacts.

Multi-plate export is partial-success aware. A plate that validates publishes
tables plus a per-plate status CSV and README. A plate-local export validation
failure publishes status/readme evidence without measurement tables. The run
summary under `feature_export_summary/` keeps one row per expected plate and
marks absent plate status artifacts as `missing_status`, so an empty table
directory is not the only signal users receive when reviewing outputs.

## Clean DataStore Full-Plate Certification

The uncapped full-plate scalability gate above used the representative plate
already present on Eddie scratch. That is enough to choose the current GPU
route, but it does not certify a fresh DataStore stage-in and stage-out path.

Phase 2.9 therefore launched a clean certification run on 2026-05-21:

```text
scratch root:
  /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-full-plate-clean-20260521
route:
  dp-growth-concurrency8
input plate:
  /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen/3723-D-100
DataStore destination:
  /exports/igmm/datastore/Drug-Discovery/Mungo/CellProfiler/test-ai-update-export/phase-2.9-full-plate-clean-20260521
config shape:
  stage_data=true, chunk=24, max_chunks omitted, feature_export enabled
```

The clean route should be used when validating production behavior because it
exercises:

- DataStore source access through `STAGE_IN`;
- scheduler-managed per-chunk Cellpose and DeepProfiler GPU work on generic
  Eddie GPU requests;
- CPU-only feature export after DeepProfiler;
- DataStore result return through `STAGE_OUT`.

One workflow integration constraint became visible only on this staged export
path: Nextflow process components can be invoked once per workflow context.
When native DeepProfiler outputs and exported tables both need destaging, build
one mixed stage-out channel and call `STAGE_OUT` once. Calling
`STAGE_OUT(FEATURE_EXTRACT.out.features)` and
`STAGE_OUT(EXPORT_FEATURES.out.stageout)` separately fails during workflow
construction before any SGE work is submitted.

Current launch evidence:

```text
2026-05-21 initial launch:
  failed before SGE submission on duplicate STAGE_OUT invocation
2026-05-21 resumed launch after workflow fix:
  STAGE_IN job 55783992 accepted by SGE in qw state
```

Record final trace counts, queue/runtime statistics, exported table counts, and
stage-out verification from this scratch root before treating Phase 2.9 as
passed.

## Feature NaN Quality Policy

DeepProfiler can complete successfully and still write all-NaN feature vectors
for some segmented objects. These NaNs are native DeepProfiler output, not a
CSV export artifact.

Current representative full-plate evidence:

```text
384 feature files
268 pass sites
70 sites with some NaN object vectors
46 all-NaN sites
62,657 objects
7,887 all-NaN object vectors
0 partial-NaN object vectors
672 features per object
```

The NaN investigation found:

- Affected sites are not no-cell sites.
- NaNs occur as complete object vectors, not scattered feature values.
- The generated DeepProfiler config used the correct `2160 x 2160` image size.
- Simple `128 x 128` crop bounds do not explain most all-NaN objects.
- A cache-bypassed rerun of `chunk_0001` reproduced the same sentinel results:
  `A01` site `1` all-NaN, `A01` site `4` passing, and `A02` site `2` mixed.
- Fresh crop checks showed valid, nonblank, nonconstant, non-saturated crops for
  `A01` site `1`, so the failure is most likely inside DeepProfiler
  preprocessing or model inference for specific site/crop content.

Detailed evidence is recorded in:

```text
docs/superpowers/plans/2026-05-20-deepprofiler-nan-root-cause-investigation.md
```

Export behavior must preserve this information:

- Keep one row per object in `deepprofiler_cells.csv`.
- Preserve NaN feature values as visible `NaN` cells.
- Add object-level NaN metadata to `deepprofiler_cells.csv`:
  `Metadata_Object_NaN_Count`, `Metadata_Object_NaN_Fraction`,
  `Metadata_Object_All_NaN`, `Metadata_Object_Has_NaN`, and
  `Metadata_Object_QualityStatus`.
- Report per-site NaN burden in `deepprofiler_quality.csv`.
- Treat low NaN object fractions as warning-level quality flags.
- Treat high NaN object fractions and all-NaN sites as site-level quality
  failures for downstream review or filtering, not as silent cleanup.

Recommended default interpretation:

| Site condition | Meaning |
| --- | --- |
| `0` all-NaN object rows | Pass |
| `>0` and low all-NaN object fraction | Warn, preserve rows and values |
| High all-NaN object fraction | Quality failure, user review/filter |
| Whole site all-NaN | Quality failure, user review/filter |

Phase 2.9 uses `feature_export.nan_object_fail_fraction` to set the site-level
failure threshold. The default is `0.05`: a site with any NaN object rows at or
below this fraction is warned and preserved; a site above this fraction, or a
site where every object row is all-NaN, is flagged as `fail_site` in
`Metadata_QualityGate`. The threshold is also written to
`Metadata_NaN_Object_Fail_Fraction` so exported tables remain self-describing.
