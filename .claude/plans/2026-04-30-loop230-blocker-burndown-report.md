# Loop 230 Blocker Burn-Down Report

Date: 2026-04-30
Phase: 2.6 Loop 230 Blocker Burn-Down
Status: Loop 320 complete; Loop 330 next

## Summary

Loop 320 now proves the Eddie substrate through staged data loading, parserix-backed image-set indexing, and pure Nextflow chunk fan-out on the representative Sarah-screen plate `3723-D-100`.

The bounded validation command used `--stages none --max_chunks 1`, so it intentionally stopped before illumination, Cellpose, and feature extraction. This was the right validation boundary for proving the staging/index/chunk contract before spending GPU or full-plate compute.

## Fixes Landed Locally

### Stage-in hardening

`nextflow/modules/stage_in.nf` now:

- Uses `set -euo pipefail`, so `rsync | tee` cannot hide rsync failures.
- Clears stale partial `staged_images` inside the isolated Nextflow work directory before each fresh staging attempt.
- Applies `rsync --chmod=Du+rwx,Dg+rx,Do-rwx,Fu+rw,Fg+r,Fo-rwx`, so restrictive source directory modes do not create unreadable scratch copies.
- Fails if staging completes with zero `.tif` files.

This addressed the original failure mode where an empty or unreadable staged directory could be cached as a successful `STAGE_IN` output.

### Index thread caps

`cptools2/nextflow_chunking.py` and `nextflow/modules/build_imageset_index.nf` now cap:

- `POLARS_MAX_THREADS=1`
- `RAYON_NUM_THREADS=1`
- `OMP_NUM_THREADS=1`
- `MKL_NUM_THREADS=1`

This addressed the Eddie-local `pyo3_runtime.PanicException` / `Resource temporarily unavailable` failure during `df.write_csv("image_sets.csv")`.

## Local Verification

Command:

```bash
pytest tests/test_nextflow_chunking.py tests/test_nextflow_architecture_smoke.py --basetemp=pytest-loop320-index-threadcap -p no:cacheprovider --tb=short
```

Result:

```text
13 passed in 1.04s
```

Note: the same focused tests hit Windows sandbox temp-directory permission issues inside the default sandbox, but passed when run outside the sandbox. The failures were pytest temp cleanup/setup permissions, not test assertions.

## Eddie Verification

Files synced to the shared Eddie checkout:

- `cptools2/nextflow_chunking.py`
- `nextflow/modules/build_imageset_index.nf`
- `nextflow/modules/stage_in.nf`
- `tests/test_nextflow_architecture_smoke.py`
- `tests/test_nextflow_chunking.py`

Eddie command:

```bash
cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2
export NXF_ANSI_LOG=false
export NXF_OPTS='-Xms128m -Xmx384m -XX:+UseSerialGC -XX:ActiveProcessorCount=1 -XX:ParallelGCThreads=1 -XX:ConcGCThreads=1 -XX:CICompilerCount=2'
/exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/env/bin/nextflow run nextflow/main.nf -profile eddie -params-file /exports/eddie/scratch/mharvey2/cptools2-loop230/params.batch_1.json -work-dir /exports/eddie/scratch/mharvey2/cptools2-loop230/work -resume --stages none --max_chunks 1
```

Result:

```text
STAGE_IN (3723-D-100)              CACHED     exit 0
BUILD_IMAGESET_INDEX (3723-D-100)  COMPLETED  exit 0
CHUNK_IMAGESETS (3723-D-100)       COMPLETED  exit 0
```

Evidence:

- Trace: `/exports/eddie/scratch/mharvey2/cptools2-loop230/pipeline_info/trace.txt`
- Staged TIFF count: `23040`
- `STAGE_IN` workdir: `/exports/eddie/scratch/mharvey2/cptools2-loop230/work/65/d84f768ab4b2fc00f77f5b4cdf5829`
- `BUILD_IMAGESET_INDEX` workdir: `/exports/eddie/scratch/mharvey2/cptools2-loop230/work/22/4dcd2fe11c4d421b8b95755a9fa363`
- Image-set manifest: `/exports/eddie/scratch/mharvey2/cptools2-loop230/work/22/4dcd2fe11c4d421b8b95755a9fa363/image_sets.csv`
- Image-set manifest size: `11521` lines, so `11520` data rows
- `CHUNK_IMAGESETS` workdir: `/exports/eddie/scratch/mharvey2/cptools2-loop230/work/1b/5d621d9bf35e0ed367b7e440361816`
- Chunk manifest: `/exports/eddie/scratch/mharvey2/cptools2-loop230/work/1b/5d621d9bf35e0ed367b7e440361816/chunks/3723-D-100_chunk_0001.csv`
- Chunk manifest size: `481` lines, so `480` data rows

No active `qstat` jobs remained after the run, and `.nextflow.log` reported `failedCount=0`.

## Remaining Issues

1. Two files were accidentally copied into the Eddie shared checkout root during sync:
   - `/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/stage_in.nf`
   - `/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/test_nextflow_architecture_smoke.py`

   They were not deleted during validation. They are safe to remove after explicit cleanup approval.

2. Local pytest temp directories created during sandboxed runs may be permission-protected:
   - `pytest-loop320-index-threadcap`
   - `pytest-loop320-single`

   These are local cleanup items, not pipeline blockers.

3. Loop 330 is still needed. The current validation proves staging, indexing, and chunking only. It does not prove Cellpose, DeepProfiler, DINO, or stage-out for AI outputs.

## Verdict

Phase 2.6 should continue with Loop 330. The prior blocker at Loop 320 is resolved: Eddie can now stage the representative plate, build the image-set index, and produce a one-chunk manifest using Nextflow-owned chunking.

Next action: run Loop 330 to determine whether Cellpose and feature extraction modules execute real containerized tooling or still need implementation before an AI smoke test can be considered meaningful.
