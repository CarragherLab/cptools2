# DeepProfiler NaN Root-Cause Investigation

Date: 2026-05-20

## Question

Do DeepProfiler NaN feature rows correspond to sites with no cells, missing images,
bad export logic, or DeepProfiler crop/model behavior?

## Evidence Sources

- Eddie export evidence:
  `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-feature-export-backfill/nan-investigation.md`
- Eddie quality table:
  `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-feature-export-backfill/export/tables/deepprofiler_quality.csv`
- Eddie cell table:
  `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-feature-export-backfill/export/tables/deepprofiler_cells.csv`
- Representative `.npz` files fetched locally for inspection:
  `A01_1_all_nan.npz`, `A01_4_pass.npz`, `A02_2_has_nan.npz`,
  `A17_2_has_nan_high_fraction.npz`, `B02_1_all_nan_high_objects.npz`

## Confirmed Findings

NaN sites are not no-cell sites.

| Site status | Sites | Object count range | Total objects | Zero-object sites |
| --- | ---: | ---: | ---: | ---: |
| pass | 268 | 4-440 | 45,140 | 0 |
| has_nan | 70 | 131-369 | 13,413 | 0 |
| all_nan | 46 | 3-251 | 4,104 | 0 |

NaNs are full object vectors, not scattered individual features.

| Object vector class | Objects |
| --- | ---: |
| finite | 54,770 |
| all 672 features NaN | 7,887 |
| partial vector NaN | 0 |

The exported table is faithfully reporting DeepProfiler output. The NaN pattern
is already present in the native `.npz` `features` arrays.

## Runtime / Cache Pattern

The Nextflow trace introduced a stronger operational signal: every affected
feature-extraction chunk was reused from cache, while every freshly completed
feature-extraction chunk was clean.

| Chunk | Quality status | Trace status | Native job | Realtime | Peak RSS | Peak VMEM |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| chunk_0001 | affected | CACHED | 55684635 | 1m 20s | 3.5 GB | 20.9 GB |
| chunk_0002 | affected | CACHED | 55684636 | 1m 20s | 3.3 GB | 21.5 GB |
| chunk_0004 | affected | CACHED | 55684640 | 1m 19s | 3.2 GB | 21.5 GB |
| chunk_0005 | affected | CACHED | 55684664 | 1m 28s | 3.5 GB | 21.7 GB |
| chunk_0007 | affected | CACHED | 55684676 | 1m 30s | 3.3 GB | 21.5 GB |
| chunk_0008 | affected | CACHED | 55684665 | 1m 33s | 3.3 GB | 21.5 GB |
| chunk_0003 | clean | COMPLETED | 55684733 | 39.3s | 4.0 GB | 15.2 GB |
| chunk_0006 | clean | COMPLETED | 55684734 | 37.9s | 4.0 GB | 15.2 GB |
| chunk_0009 | clean | COMPLETED | 55684735 | 38.1s | 4.2 GB | 15.5 GB |
| chunk_0010 | clean | COMPLETED | 55684737 | 38.2s | 4.0 GB | 15.3 GB |
| chunk_0011 | clean | COMPLETED | 55684736 | 39.7s | 4.1 GB | 15.3 GB |
| chunk_0012 | clean | COMPLETED | 55684740 | 38.6s | 4.0 GB | 15.2 GB |
| chunk_0013 | clean | COMPLETED | 55698442 | 42.7s | 4.0 GB | 15.2 GB |
| chunk_0014 | clean | COMPLETED | 55697745 | 38.5s | 4.1 GB | 15.3 GB |
| chunk_0015 | clean | COMPLETED | 55699368 | 38.4s | 4.0 GB | 15.2 GB |
| chunk_0016 | clean | COMPLETED | 55699470 | 38.9s | 4.1 GB | 15.3 GB |

SGE accounting for affected native jobs `55684635`, `55684636`,
`55684640`, `55684664`, `55684665`, and `55684676` shows they all ran on:

```text
node1o09.ecdf.ed.ac.uk
queue: gpu
resource: gpu=1
exit_status: 0
```

Representative clean jobs `55684733` and `55699470` ran on:

```text
node1p05.ecdf.ed.ac.uk
queue: gpu
resource: gpu=1
exit_status: 0
```

The affected chunk log for `chunk_0001` reports:

```text
hostname: node1o09.ecdf.ed.ac.uk
CUDA_VISIBLE_DEVICES=3
GPU model: NVIDIA H200 NVL
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true
TF_FORCE_GPU_ALLOW_GROWTH=true
```

No DeepProfiler error was logged. The job exited successfully and wrote `.npz`
files containing all-NaN object vectors.

## Plate And Chunk Pattern

Failures are spatially clustered by well block and by feature-extraction chunk.

Affected chunks:

| Chunk | Status counts | Affected wells |
| --- | --- | --- |
| sarah-representative_chunk_0001 | all_nan 12, has_nan 8, pass 4 | A01-A04 |
| sarah-representative_chunk_0002 | all_nan 5, has_nan 13, pass 6 | A05-A08 |
| sarah-representative_chunk_0004 | all_nan 7, has_nan 14, pass 3 | A13-A16 |
| sarah-representative_chunk_0005 | all_nan 6, has_nan 12, pass 6 | A17-A20 |
| sarah-representative_chunk_0007 | all_nan 12, has_nan 8, pass 4 | B01-B04 |
| sarah-representative_chunk_0008 | all_nan 4, has_nan 15, pass 5 | B05-B08 |

Clean chunks:

`chunk_0003`, `chunk_0006`, `chunk_0009` through `chunk_0016`.

Clean well blocks include `A09-A12`, `A21-A24`, `B09-B24`, and all observed row
`C` wells.

## Representative NPZ Inspection

| File | Shape | NaN rows | Location evidence |
| --- | ---: | ---: | --- |
| A01 site 1 all_nan | 4 x 672 | 4 | all four locations populated, none within 128 px of border |
| A01 site 4 pass | 4 x 672 | 0 | matched well, populated locations |
| A02 site 2 has_nan | 137 x 672 | 9 | NaN rows are mostly near image edge |
| A17 site 2 has_nan | 158 x 672 | 128 | NaN rows are not edge-only |
| B02 site 1 all_nan | 251 x 672 | 251 | high-object all-NaN site, not explained by low object count |

Distance-to-border is not sufficient as a root cause. Some mixed sites have
edge-enriched NaN objects, but high-fraction and all-NaN sites include many
non-edge objects.

## Retry: Crop-Bound Checks For Bad Wells/Sites

While Eddie image transfer was unstable, the exported cell table was rechecked
locally using the actual object centers and the generated `2160 x 2160` image
dimensions. For a `128 x 128` crop, an object is out of bounds if its center is
within 64 px of an image edge.

Aggregate result:

| Site status | Objects | All-NaN objects | Objects out of 64 px crop bounds | All-NaN out of 64 px crop bounds |
| --- | ---: | ---: | ---: | ---: |
| pass | 45,140 | 0 | 6,286 | 0 |
| has_nan | 13,413 | 3,783 | 1,951 | 716 |
| all_nan | 4,104 | 4,104 | 608 | 608 |

Interpretation:

- Border crops exist in both passing and failing sites.
- In mixed `has_nan` sites, NaN objects are somewhat edge-enriched.
- In `all_nan` sites, only 608 of 4,104 NaN objects are within the strict
  `128 x 128` crop-out-of-bounds zone.
- Therefore ordinary crop bounds cannot explain most NaN vectors.

Examples that are not explained by out-of-bounds crops:

| Well | Site | Status | All-NaN objects | All-NaN objects out of bounds | Median distance to edge |
| --- | ---: | --- | ---: | ---: | ---: |
| A01 | 1 | all_nan | 4 | 0 | 353.5 px |
| A02 | 1 | all_nan | 194 | 20 | 345.9 px |
| B02 | 1 | all_nan | 251 | 27 | 272.0 px |
| B05 | 1 | all_nan | 219 | 4 | 415.2 px |

The strongest example remains `A01 site 1`: all four objects are all-NaN, but
none of the centers are close enough to the image edge for a `128 x 128` crop to
leave the `2160 x 2160` image.

## Generated DeepProfiler Config

The generated Eddie config used the actual TIFF dimensions:

```json
"images": {
  "channels": ["DNA", "RNA", "ER", "AGP", "Mito"],
  "file_format": "tif",
  "bits": 16,
  "width": 2160,
  "height": 2160
}
```

DeepProfiler crop/profile settings:

```json
"locations": {
  "mode": "single_cells",
  "area_coverage": 0.25,
  "box_size": 128,
  "mask_objects": false
},
"profile": {
  "feature_layer": "block6a_activation",
  "checkpoint": "Cell_Painting_CNN_v1.hdf5",
  "batch_size": 128
}
```

This rules out a stale 1080x1080 config as the immediate explanation for these
NaNs.

## Working Hypothesis

DeepProfiler is accepting the site inputs and writing valid `.npz` containers,
but for some object crops it produces an all-NaN embedding vector. The issue
occurs before export, likely inside crop preprocessing or the EfficientNet
forward pass used by DeepProfiler profiling.

The strongest current clue is no longer just plate position. It is the perfect
alignment between affected chunks, cached feature-extraction outputs, and early
jobs on `node1o09`. Clean chunks were fresh completions and representative clean
jobs ran on `node1p05`.

This makes the leading hypothesis:

> The NaNs are stale or bad DeepProfiler outputs from an earlier cached
> execution on `node1o09`, not a deterministic export failure.

The remaining uncertainty is whether `node1o09` itself, the earlier runtime
environment, or the specific well-block image content triggered the NaNs. A
cache-bypassed rerun of one affected chunk is the fastest discriminator.

## Next Checks

1. Rerun one affected chunk with Nextflow cache disabled or isolated from the
   previous work directory. Prefer `chunk_0001` because it contains both all-NaN
   and passing sites in the same feature-extraction task.
2. Capture SGE accounting and `.command.out` for the rerun. If it lands on
   `node1p05` or another node and the same sites become finite, the cached
   `node1o09` outputs should be treated as invalid.
3. If the rerun still produces NaNs, reconstruct crops for representative finite
   and all-NaN objects from the
   same sites and measure per-channel crop min, max, mean, standard deviation,
   zero fraction, saturation fraction, and shape.
4. Inspect DeepProfiler crop/preprocessing code inside the exact container,
   especially the crop generator and image normalization path.
5. Rerun a tiny diagnostic package containing one all-NaN site, one mixed site,
   and one passing site. Add a wrapper check for non-finite tensors before model
   prediction and non-finite activations after the feature layer.
6. Treat NaN rows as quality failures for downstream exports until the
   DeepProfiler-side mechanism is fixed or intentionally filtered.

## Current Blocker

Eddie shell access is intermittent. SSH worked when bypassing profile startup,
and SGE accounting was collected, but later image transfers failed with
connection closes or login `fork: Resource temporarily unavailable`. Crop-image
diagnostics should resume once Eddie accepts stable SFTP/SCP transfers.

## Planned Cache-Bypass Rerun

The next diagnostic rerun should use:

```text
route root: /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-nan-rerun-chunk0001
chunk: 24
max_chunks: 1
resume: false
DeepProfiler TF memory growth: true
```

Rationale:

- `max_chunks: 1` isolates the first affected chunk, including `A01 site 1`,
  `A01 site 4`, and `A02 site 2`.
- A new route root and no `--resume` prevents reuse of the prior cached
  `node1o09` DeepProfiler output.
- If the rerun produces finite features for the same sites, the earlier cached
  feature outputs should be invalidated.

Attempted on 2026-05-20, but Eddie rejected command startup before Python ran:

```text
/etc/bashrc: fork: Resource temporarily unavailable
```

## Cache-Bypass Rerun Result

The isolated rerun later completed successfully:

```text
route root: /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-nan-rerun-chunk0001/dp-growth-concurrency8
status: 0
Nextflow duration: 5m32s
succeeded tasks: 6
FEATURE_EXTRACT job: 55772614
FEATURE_EXTRACT host: node1o09.ecdf.ed.ac.uk
FEATURE_EXTRACT exit_status: 0
FEATURE_EXTRACT wallclock: 110.657s
FEATURE_EXTRACT maxvmem: 26.296G
FEATURE_EXTRACT maxrss: 3.499G
```

The fresh run reproduced the same feature status for the sentinel sites:

| Site | Fresh rerun result |
| --- | --- |
| A01 site 1 | 4 objects, all 4 all-NaN |
| A01 site 4 | 4 objects, 0 NaN values |
| A02 site 2 | 137 objects, 9 all-NaN rows |

This rejects the earlier "stale cache only" hypothesis. The cached outputs were
not the primary cause; the NaN pattern reproduces on fresh execution.

## Fresh Crop-Content Check

Crop statistics were computed from the fresh `chunk_0001` work directory for
`A01 site 1`, `A01 site 4`, and `A02 site 2`.

The all-NaN `A01 site 1` crops are valid `128 x 128` crops across all five
channels. They are nonblank, nonconstant, and not saturated. Example object 1:

| Channel | Min | Max | Mean | Std | Zero fraction | Saturated fraction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| DNA | 123 | 5343 | 378.228 | 649.854 | 0 | 0 |
| RNA | 73 | 4318 | 644.961 | 837.328 | 0 | 0 |
| ER | 364 | 13179 | 2114.166 | 2393.500 | 0 | 0 |
| AGP | 96 | 10918 | 1152.803 | 2015.580 | 0 | 0 |
| Mito | 53 | 9632 | 1000.391 | 1663.383 | 0 | 0 |

The passing `A01 site 4` crops are also valid. Some have lower intensity and
variance than the failed `A01 site 1` crops, so raw crop blankness or constant
intensity does not explain the all-NaN embeddings.

For mixed `A02 site 2`, the all-NaN objects are mostly near the bottom edge and
some crops are truncated vertically, but the same site also has finite objects
near the top edge with similarly truncated crops. This means edge truncation may
contribute to some mixed-site failures, but it is not sufficient as a general
explanation.

## Updated Root-Cause Hypothesis

The current evidence points to deterministic DeepProfiler preprocessing/model
behavior for specific site/crop content, not cptools2 export, no-cell sites,
stale Nextflow cache, missing images, blank crops, saturated crops, or simple
out-of-bounds crops.

The failure happens inside DeepProfiler before or during feature generation:

```text
valid image crop + valid location -> DeepProfiler writes all-NaN feature vector
```

The next root-cause step is source-level instrumentation inside the DeepProfiler
container:

1. Inspect the exact crop generator and normalization path.
2. Run the sentinel sites through a wrapper that checks tensors before model
   prediction.
3. Check activations at `block6a_activation`.
4. Determine whether NaNs first appear during crop normalization or during the
   TensorFlow/EfficientNet forward pass.
