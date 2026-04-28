# Progress Report: Loop 230 Eddie Full-Run Attempt

Generated: 2026-04-28

## Summary

Loop 230 now has the core scratch-aware Nextflow path in place and partially validated on Eddie with the Sarah-screen test plate `3723-D-100`.

The SGE-native scratch safety model has been ported into the Nextflow CLI path:

- Scratch availability is checked before execution.
- Plate sizes are converted into scratch loading batches using the same `75%` usable scratch target and `30%` per-plate overhead buffer.
- DataStore-backed staged runs no longer silently fall back to "all plates" when the source path is invisible from the login node.
- Loop 230 config now carries an explicit measured size for `3723-D-100`: `103 GB`, producing a protected batch size of `133.9 GB`.

## Completed

- Added `plate_sizes_gb` config support for staged DataStore runs.
- Added fail-fast behavior when a DataStore input is inaccessible and no explicit plate size is supplied.
- Updated `config/loop230-sarah-screen.yaml` with:
  - `plate_sizes_gb: {3723-D-100: 103}`
  - shared SIF container directory via `container_path`
- Deployed updated source/config/modules to:
  - `/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2`
- Confirmed Eddie dry-run writes:
  - `batch_id = 1`
  - `plates = ['3723-D-100']`
  - `batch_plate_count = 1`
  - `batch_total_size_gb = 133.9`
  - `stage_data = true`
- Confirmed Nextflow preview reaches the intended graph:
  - `STAGE_IN`
  - `BUILD_IMAGESET_INDEX`
  - `CHUNK_IMAGESETS`
  - `ILLUM_CALCULATE`
  - `ILLUM_APPLY`
  - `CELLPOSE_SEGMENT`
  - `FEATURE_EXTRACT`
  - `STAGE_OUT`
- Submitted a full run and confirmed:
  - `STAGE_IN (3723-D-100)` completed and is cacheable.
  - `ILLUM_CALCULATE (3723-D-100)` completed with the shared CellProfiler SIF container.

## Code Changes

Recent commits:

- `24eb903` `fix: require scratch-safe DataStore batching`
- `f5e10e2` `chore: use shared Loop 230 containers`
- `b5e71df` `fix: expose cptools2 to Nextflow helpers`
- `d411519` `fix: point helper PYTHONPATH at repo root`
- `58d5ee8` `chore: lower Eddie Nextflow submit pressure`
- `5139e90` `fix: run Nextflow index helpers locally`

## Verification

Local tests:

- `pytest` full suite: `170 passed`
- Focused parser tests after config update: `29 passed`
- Nextflow architecture/chunking smoke tests after helper patches: `8 passed`
- Eddie profile smoke after submit-pressure/local-index changes: passed

Eddie validation:

- SSH login verified on `login02.ecdf.ed.ac.uk`.
- `cptools2 pipeline config/loop230-sarah-screen.yaml --dry-run` succeeded on Eddie.
- Nextflow preview succeeded using:
  - `-profile eddie`
  - `-params-file /exports/eddie/scratch/mharvey2/cptools2-loop230/params.batch_1.json`
  - `-work-dir /exports/eddie/scratch/mharvey2/cptools2-loop230/work`
- Full run progressed through staging and illumination calculation.

## Blocker

The current full-run blocker is Eddie submit-host thread pressure, not a cptools2 logic failure.

Observed failures while running the Nextflow driver on `login02`:

- `java.io.IOException: Cannot run program "qsub": error=11, Resource temporarily unavailable`
- `qsub` comms failure: `cl_com_setup_commlib failed: can't create thread`
- Nextflow JVM warnings: `pthread_create failed (EAGAIN)`

Diagnostic context:

- Manual `qsub` smoke submission from the login node succeeded.
- Account-level active workload is high, with other jobs running/queued, including `alphafold3`, `nf-CELLRAN`, and `af3-gpu`.
- Thread count observed from the login node was high enough to explain intermittent thread creation failures during Nextflow driver startup and child submission.

## Recommended Unblock

1. Re-run Loop 230 when the account has fewer active Eddie jobs/threads.
2. Keep the Nextflow driver on a login/submit host, not inside an SGE compute job, because compute nodes are not submit hosts.
3. Keep `NXF_OPTS` constrained for Eddie login execution:

```bash
export NXF_ANSI_LOG=false
export NXF_OPTS='-Xms128m -Xmx384m -XX:+UseSerialGC -XX:ActiveProcessorCount=1 -XX:ParallelGCThreads=1 -XX:ConcGCThreads=1 -XX:CICompilerCount=2'
```

4. Resume with:

```bash
cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2
export PATH=$HOME/.local/bin:/exports/applications/gridengine/ge-2024.1.0/bin/lx-amd64:/opt/sge/bin/lx-amd64:/usr/local/bin:/usr/bin:/bin:$PATH
~/.local/bin/nextflow run nextflow/main.nf \
  -c nextflow/nextflow.config \
  -profile eddie \
  -params-file /exports/eddie/scratch/mharvey2/cptools2-loop230/params.batch_1.json \
  -work-dir /exports/eddie/scratch/mharvey2/cptools2-loop230/work \
  -resume
```

## Next Target

The next successful checkpoint should be:

- `BUILD_IMAGESET_INDEX` completes against the staged plate.
- `CHUNK_IMAGESETS` emits chunk manifests with `chunk_size = 96`.
- `ILLUM_APPLY` fans out over those chunks.

That will confirm the new two-step batching model:

1. Scratch-safe plate loading batches.
2. Nextflow-native intra-plate chunk fan-out.
