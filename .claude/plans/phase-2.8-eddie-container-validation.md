---
phase: 2.8
name: Eddie Container Validation and Runtime Certification
status: active
source_plans:
  - phase-2.7-scratch-batch-reproducibility.md
  - ../../docs/plans/seqera-eddie-scratch-batching-plan.md
loops_total: 7
---

# Phase 2.8: Eddie Container Validation and Runtime Certification

## Objective

Prove that cptools2 can run controlled Eddie dry-runs and small smoke tests for CellProfiler, Cellpose, and DeepProfiler from the permanent <group> mirror while keeping all runtime cache, work, logs, traces, and generated run artifacts inside the dedicated scratch root.

## Decisions Locked In

- Permanent project mirror is authoritative: `${CPTOOLS2_PROJECT_ROOT}`.
- Scratch runtime root is `${CPTOOLS2_SCRATCH_ROOT}`.
- Stable configs, environment bootstrap files, container manifests, and container images live in permanent space.
- Runtime cache, Nextflow work directories, generated params, staged test data, logs, traces, reports, and timelines live under the scratch root.
- Use dry-runs and minimal smoke tests before any broader Eddie execution.
- Treat CellProfiler CPU, Cellpose GPU, and DeepProfiler GPU validation as separate gates.
- Record blockers and user-decision points rather than guessing around missing data.

## Phase Outputs

| Output | Location | Purpose |
|--------|----------|---------|
| Eddie runtime plan | `.claude/plans/phase-2.8-eddie-container-validation.md` | Shared decisions, blockers, and evidence log |
| Ralph loops | `.claude/plans/phase-2.8-ralph-loops.md` | Executable development loops |
| TODO reconciliation | `TODOs.md` | Current work queue for unattended/resumed development |
| Permanent bootstrap/config candidates | `config/`, `nextflow/conf/` | Reproducible Eddie module/container setup |
| Scratch evidence | `${CPTOOLS2_SCRATCH_ROOT}/` | Logs, traces, reports, params, and run snapshots |
| Validation notes | this plan and follow-up docs | Evidence for what works, what is blocked, and why |

## Eddie Layout

```text
${CPTOOLS2_PROJECT_ROOT}/
  config/                 # stable project configs and environment bootstrap
  containers/             # .sif images and manifests
  nextflow/               # pipeline code and Eddie config
  cptools2/               # package source

${CPTOOLS2_SCRATCH_ROOT}/
  work/                   # Nextflow work dirs, batch_* work dirs, cache
  params/                 # generated runtime params only
  logs/                   # command logs and scheduler logs
  traces/                 # Nextflow traces/reports/timelines
  staging/                # small staged test data
  snapshots/              # non-destructive inventories of Eddie state
```

## Success Criteria

- Eddie mirror state is snapshotted before any update or destructive action.
- Scratch root exists with clear subdirectories for work, logs, traces, params, staging, and snapshots.
- Module bootstrap uses pinned Eddie modules: Nextflow, Singularity, and Miniforge.
- Container manifest and actual `.sif` inventory agree or the mismatch is documented.
- CellProfiler container starts and passes a minimal CPU validation.
- Cellpose container starts on an Eddie GPU node and proves CUDA visibility or records the exact GPU blocker.
- DeepProfiler container starts on an Eddie GPU node and proves TensorFlow/CUDA visibility or records the exact GPU blocker.
- A small multi-engine dry-run or smoke run records trace/report/timeline evidence before cleanup.
- Cleanup remains opt-in and runs only after successful batches.
- All remaining decisions for the user are captured with context.

## Phase Loops

| Loop | Name | Status | Purpose |
|------|------|--------|---------|
| 390 | Planning State and Eddie Inventory | active | Reconcile plans/TODOs and snapshot permanent/scratch/container/module state |
| 400 | Permanent Bootstrap and Runtime Config | pending | Add reproducible Eddie bootstrap/config files using permanent config and scratch runtime roots |
| 410 | Mirror Sync and Manifest Reconciliation | pending | Safely update or prepare the permanent mirror without clobbering unreviewed remote changes |
| 420 | CellProfiler CPU Container Smoke | pending | Validate CellProfiler startup and a minimal CPU execution path |
| 430 | Cellpose and DeepProfiler GPU Smoke | pending | Validate GPU container startup, CUDA visibility, and minimal engine commands |
| 440 | End-to-End Eddie Smoke and Evidence | pending | Run the smallest practical multi-engine dry-run/smoke and document evidence, blockers, and tuning |
| 450 | DeepProfiler Input Package Handoff | pending | Build the robust Cellpose-to-DeepProfiler handoff package required to complete the integrated smoke |
| 485 | GPU Capacity and Throughput Strategy | pending | Compare full GPU and MIG turnaround, output equivalence, memory headroom, and batching tradeoffs before changing defaults |
| 486 | Gated MIG Functional Validation | pending | Prove cache-safe MIG/full-GPU functional behavior and DeepProfiler handoff before broader validation |

## Current Evidence

- SSH to Eddie succeeds as `<UUN>` on `login02.ecdf.ed.ac.uk`.
- Permanent mirror exists and is writable.
- Permanent mirror is stale relative to local development at commit `062a0da` and has uncommitted/untracked remote changes, so mirror sync needs a snapshot and a conservative update path.
- Scratch root `${CPTOOLS2_SCRATCH_ROOT}` exists with `work`, `params`, `logs`, `traces`, `staging`, and `snapshots`.
- Available validated module candidates include `roslin/nextflow/25.10.2`, `singularity/4.3.4`, and `miniforge/25.3.1-0`.
- Existing permanent containers include `cellprofiler_4.2.8.sif`, `cellpose_sam_1.0.sif`, and `deepprofiler_1.0.sif`.
- Container sizes observed on Eddie: CellProfiler `1,635,794,944` bytes, Cellpose-SAM `7,760,654,336` bytes, DeepProfiler `3,383,758,848` bytes.
- No `*.json` manifest was present in the permanent `containers/` directory during the first inventory, despite the local `cptools2/container_manifest_template.json` expecting one alongside `.sif` files.
- Eddie module inventory includes `cuda/11.8`, `cuda/12.1.1`, `miniforge/25.3.1-0`, `singularity/4.3.4`, `igmm/bac/nextflow/25.10.3`, and `roslin/nextflow/25.10.2`.
- Live Eddie `qconf` confirms GPU syntax should use the `gpu` queue/parallel environment with requestable `-l gpu=N`; `gpus` is not requestable and `gpu-a100` is not listed.
- Windows local dry-run is not a useful substitute for Eddie validation because `/exports/...` paths fail locally with `PermissionError: [WinError 5] Access is denied: 'C:\\exports'`.
- CellProfiler CPU smoke job `55264527` completed on `node1b03.ecdf.ed.ac.uk` with `exit_status 0`, `failed 0`, `ru_wallclock 404.182`, `maxvmem 12.096G`, and version output `4.2.8`.
- CellProfiler smoke caveat: Singularity FUSE mounting failed on the Eddie filesystem and fell back to extracting the SIF into `cptools2-ai-update/work/singularity_tmp`; this made a simple version check take about 6m44s, but cleanup completed and `singularity_tmp` returned to 1 KB.
- Cellpose first GPU smoke job `55264551` ran immediately but used a bad probe (`cellpose.__version__`) and produced a Python traceback; qacct reported `exit_status 0` because the original script piped through `tee`, masking the Python failure.
- Smoke scripts were corrected to preserve the container command exit status and avoid `tee` masking failures. Corrected Cellpose job `55264576` completed with `exit_status 0`, `failed 0`, `ru_wallclock 226.879`, `maxvmem 17.010G`, Cellpose `4.1.1`, and CUDA visible on `NVIDIA H200 NVL`.
- DeepProfiler GPU smoke job `55269157` completed with `exit_status 0`, `failed 0`, `ru_wallclock 210.269`, `maxvmem 16.212G`, TensorFlow `2.5.3`, and a visible GPU device. TensorFlow logs confirm CUDA libraries and an `NVIDIA H200 NVL` were detected.
- After the GPU smoke checks, `qstat -u <UUN>` showed no lingering jobs.

## Blockers and Decision Points

- Remote mirror update strategy: the permanent mirror has local remote modifications. We need either a branch/patch-based update, an rsync with explicit exclusions, or user approval to reset/replace the mirror.
- Container manifest deployment: the expected manifest template exists locally, but the permanent `containers/` directory currently exposes only `.sif` and log files. Loop 410 should decide whether to generate/deploy a manifest before smoke tests.
- Minimal test dataset: choose whether to reuse the Loop 230 example-screen sample, a tiny staged fixture, or a new purpose-built smoke dataset.
- GPU validation queue: Cellpose and DeepProfiler require Eddie GPU availability and `--nv`; live scheduler syntax is `-q gpu -l gpu=1`, not legacy `gpu-a100`/`gpus`.
- DeepProfiler assets: confirm which config, checkpoint, and weights should be mounted for the minimal validation.
- Cleanup policy for validation: default should leave evidence until reviewed; enable `--clean-work` only after a successful evidence-preserving run.
- Singularity startup performance: because FUSE mount failed and extraction fallback was required for CellProfiler, GPU smoke tests should expect slow first startup and should keep cache/tmp under scratch/work. We may need Eddie-specific Singularity guidance if extraction overhead becomes unacceptable.
- Functional smoke scope: startup/version/CUDA checks are complete for all three containers. Functional image-processing smoke still needs tiny input assets and, for DeepProfiler, a decision on config/weights/checkpoint mounting.

## Loop 410 Mirror Strategy

Current comparison:

- Local development HEAD: `d11d205`.
- Permanent mirror HEAD: `062a0da`.
- Permanent mirror has modified tracked source/test files plus many untracked Nextflow, config, container, `.nextflow`, and test artifacts.

## Phase 2.9 Handoff

The 2026-05-19 DeepProfiler scalability run completed the staged representative
input, not a complete 384-well plate. The input geometry was 64 wells x 6 sites,
giving 384 well-site image sets.

Phase 2.8 runtime certification is accepted for representative real-cell
execution. Phase 2.9 owns feature table export, NaN quality reporting, and
measurement-table usability before any true 384-well staging/destaging run.
- Permanent mirror `containers/` has the required `.sif` files but no `cptools2_containers.json` manifest.

Conservative strategy:

1. Do not run `git reset`, `git clean`, `git pull`, or `rsync --delete` in the permanent mirror without explicit approval.
2. Preserve a remote evidence snapshot under `${CPTOOLS2_SCRATCH_ROOT}/snapshots/` before any sync.
3. Prefer deploying reviewed local commits over the permanent mirror only after the remote dirty files have either been archived or judged disposable.
4. Exclude `containers/`, `.nextflow/`, `.nextflow.log*`, and scratch/runtime artifacts from source sync operations.
5. Deploy the container manifest as an additive file only if absent or after confirming the existing file should be replaced.
6. For immediate smoke validation, prefer commands that reference the known permanent `.sif` files directly and write logs under scratch; this avoids depending on the stale package import until mirror sync is resolved.

Approval needed before destructive mirror action:

- Replace or reset the permanent mirror source tree to match local HEAD.
- Delete remote `.nextflow` cache/log artifacts from permanent space.
- Overwrite `nextflow/conf/eddie.config` or any existing stable config in the permanent mirror.
- Remove or replace container `.sif` or `.tar` files.

No approval needed for non-destructive actions already performed:

- Creating the dedicated scratch runtime root.
- Reading `qconf`, module, git, and container inventory.
- Writing local plans, bootstrap files, and tests.

## Loop 440 Smoke Strategy

The smallest safe end-to-end smoke should not reuse the full Loop 230 example-screen plate directly. `max_chunks: 1` limits processing after image-set indexing, but it does not avoid whole-plate staging when `stage_data: true`; the Loop 230 plate is documented as about 103 GB. That is larger than a smoke test and would obscure whether failures come from orchestration or data volume.

Recommended path:

1. Use startup probe evidence from Loops 420 and 430 as the container-runtime gate.
2. Create or identify a tiny staged plate under `${CPTOOLS2_SCRATCH_ROOT}/staging/tiny-plate-set`.
3. Keep `stage_data: false` for the first end-to-end smoke so staging/DataStore transfer is not mixed with Cellpose/DeepProfiler execution.
4. Use `max_chunks: 1`, `batch_size: 1` for Cellpose, and a small DeepProfiler batch size.
5. Write results, params, work, traces, reports, and logs under `${CPTOOLS2_SCRATCH_ROOT}`.
6. Reference stable templates, containers, and config from `${CPTOOLS2_PROJECT_ROOT}`.
7. Do not enable cleanup for the first end-to-end smoke; preserve evidence until reviewed.

Local candidate config:

- `config/loop440-eddie-smoke.yaml`

Current evidence:

- The tiny staged plate exists in scratch and indexes/chunks successfully on Eddie:
  `${CPTOOLS2_SCRATCH_ROOT}/staging/tiny-plate-set/tiny-plate-001`.
- User explicitly approved resetting the shared permanent mirror source/config/docs
  state to GitHub `origin/ai-update`; `containers/` was preserved as permanent
  untracked asset storage.
- The permanent mirror is now on `ai-update` at `5e952d5`.
- A permanent project virtualenv was created at
  `${CPTOOLS2_PROJECT_ROOT}/.venv`; pip cache and
  temporary build work were kept under
  `${CPTOOLS2_SCRATCH_ROOT}/work`.
- The Loop 440 dry-run now passes from the permanent mirror using
  `config/loop440-eddie-smoke.yaml`. It writes params and batch metadata under
  `${CPTOOLS2_SCRATCH_ROOT}/results/loop440` and skips
  Nextflow submission as intended.

Next decision:

- Decide whether to submit the tiny Loop 440 Nextflow smoke after reviewing the
  dry-run output, or stop at dry-run evidence until functional CellProfiler,
  Cellpose, and DeepProfiler tiny inputs/configs are reviewed.

## Loop 450 DeepProfiler Input Package Handoff

Loop 450 is part of Phase 2.8, not a new phase. It exists because the Loop 440
smoke reached DeepProfiler and exposed a data-contract blocker rather than an
Eddie/container blocker.

Objective:

- Build a reusable DeepProfiler input package builder so the handoff from
  `CELLPOSE_SEGMENT` to `FEATURE_EXTRACT` is explicit, testable, and complete.

Why this fits here:

- Loop 420 and Loop 430 proved container startup/runtime compatibility.
- Loop 440 proved Nextflow can dry-run, submit via SGE, run Cellpose on GPU, and
  reach DeepProfiler.
- The remaining blocker is the internal input package expected by DeepProfiler:
  `dp_project/inputs/images`, `dp_project/inputs/metadata/index.csv`,
  `dp_project/inputs/locations`, and `dp_project/inputs/config/config.json`.

Contract:

```text
CHUNK_IMAGESETS
  chunk_manifest.csv
        |
        v
CELLPOSE_SEGMENT
  cellpose_masks/
  cellpose_masks/locations/*_locations.csv
        |
        v
cptools2.nextflow_chunking deepprofiler-package
  dp_project/inputs/images/<plate>/*.tif
  dp_project/inputs/metadata/index.csv
  dp_project/inputs/locations/<plate>/<well>-f<site>-Nuclei.csv
  dp_project/inputs/config/config.json
        |
        v
FEATURE_EXTRACT
  DeepProfiler features
```

Decisions:

- Treat `.tif` as the default image format for this handoff.
- Use the DeepProfiler config as the source of truth for channel names and label
  defaults where possible.
- Zero Cellpose locations is valid. The package builder should emit correctly
  shaped empty nuclei CSVs and allow the pipeline to continue.
- Keep bulky package internals in the Nextflow work directory; publish only small
  audit artifacts and final features under scratch results.

Plan:

- Implementation plan:
  `docs/superpowers/plans/2026-05-06-deepprofiler-metadata-bridge.md`.
- Add failing tests for the DeepProfiler input package contract.
- Implement `build_deepprofiler_input_package(...)` in
  `cptools2.nextflow_chunking`.
- Wire `nextflow/modules/feature_extract.nf` to invoke the helper before
  launching DeepProfiler.
- Fast-forward the Eddie mirror and rerun Loop 440 with `--resume`.

Success criteria:

- Unit tests prove package generation for non-empty and zero-location Cellpose
  output.
- Static Nextflow tests prove `FEATURE_EXTRACT` calls the helper and uses the
  DeepProfiler package layout.
- Loop 440 rerun reaches beyond the current missing `index.csv` blocker.
- If DeepProfiler then fails on checkpoint/model assumptions, that is captured as
  the next blocker rather than conflated with the handoff package.

## Loop 485 GPU Capacity and Throughput Strategy

Loop 485 belongs with the FUSE/root-cause work because GPU class selection affects
whether reproduction tests are short, frequent, and easy to schedule, or longer
and closer to production behavior.

Objective:

- Decide whether validation and production-like cptools2 runs should use full
  GPUs, MIG partitions, or both based on observed total turnaround time and
  output equivalence.

Evaluation model:

```text
effective turnaround =
  queue wait
  + container startup
  + image processing runtime
  + staging/destaging
  + retry or failure cost
```

Current evidence:

- Eddie docs say MIG is a smaller A100 partition, not equivalent to a full GPU.
- `gpu-mig=1` has been accepted and run quickly for a tiny docs-guidance probe.
- `gpu=1` requests have been accepted but can queue longer than MIG when
  full-GPU capacity is constrained.
- First matched container-only probes showed Cellpose and DeepProfiler can both
  import their GPU frameworks on `gpu-mig=1` and `gpu=1` with
  scheduler-managed CUDA visibility. In this run, MIG started much faster; full
  GPU started later but completed faster once scheduled.
- Both MIG and full-GPU probes still hit the Singularity FUSE mount failure and
  fell back to temporary sandbox extraction before succeeding, so GPU class does
  not remove the container startup/FUSE behavior by itself.
- A small real Cellpose diagnostic also completed successfully on a full GPU,
  but with substantially higher memory use than the import-only probes. That
  makes functional MIG testing necessary before recommending MIG beyond
  validation-sized chunks.
- A Nextflow-managed MIG Cellpose functional smoke completed successfully on a
  tiny TIFF input, used CUDA, wrote mask and locations outputs, and handled the
  zero-cell case without failing. A matched same-input full-GPU functional smoke
  is still needed before claiming output equivalence.
- Memory evidence must be interpreted carefully. Eddie enforces CPU resident
  memory through `h_rss`; Nextflow `peak_rss` is the trace-level signal closest
  to that. SGE `maxvmem` and Nextflow `peak_vmem` can be much larger because
  they report virtual address space, not MIG GPU framebuffer. MIG GPU memory is
  separate device memory and remains the key unknown for larger Cellpose and
  DeepProfiler chunks.
- Existing Nextflow config requests full GPUs for all `label 'gpu'` processes,
  via `queue = 'gpu'` and `clusterOptions = '-l gpu=1'`.
- A follow-up GPU/MIG chunk strategy design was approved:
  `docs/superpowers/specs/2026-05-14-gpu-mig-chunk-strategy-design.md`.
  The next comparison should keep `max_chunks: 5` fixed while varying chunk
  sizes 48, 24, and 96 across MIG-only and full-GPU policies. If both pass, run
  a mixed policy candidate with Cellpose on MIG and serialized DeepProfiler on
  full GPU.
- This matrix is validation evidence, but it also informs the future operating
  policy for multi-plate, multi-chunk runs. Results must report normalized
  throughput, failure blast radius, retry cost, and stage-specific resource fit,
  not only whether the smoke tests pass.
- Production interpretation should model the whole plate lifecycle: stage-in,
  indexing/chunking, illumination, parallel Cellpose chunks, serialized
  DeepProfiler chunks, stage-out, retry cost, scratch footprint, and guarded
  cleanup.

Plan:

1. Capture live capacity snapshots with
   `qstat -F gpu,gpu-mig,gputype,h_rss -q gpu` at submission and completion.
2. Record queue wait separately from execution runtime for every GPU diagnostic.
3. Run matched container-only probes on `gpu-mig=1` and `gpu=1`. Completed for
   Cellpose and DeepProfiler startup/import behavior.
4. Run matched tiny Cellpose and DeepProfiler probes on both GPU classes.
5. Compare output presence, counts/dimensions, first error line, runtime,
   CPU RSS, virtual memory context, GPU memory indicators where available, and
   FUSE symptoms.
6. Test larger chunks only after tiny probes pass, stopping when memory,
   runtime, queue delay, or FUSE failures make the class unattractive.
7. Compare many small GPU jobs against fewer longer GPU jobs, including queue
   wait and failure blast radius.
8. Recommend a resource policy before changing Nextflow defaults.
9. Run the approved `max_chunks: 5` matrix before staging/destaging expansion:
   MIG-only, full-GPU, then mixed Cellpose-MIG/DeepProfiler-full-GPU if both
   pass.
10. Before the mixed candidate, add process-specific GPU resource controls so
    Cellpose and DeepProfiler can request different GPU classes in the same
    Nextflow run.
11. Add GPU task diagnostics and benchmark classification before submission:
    host, CUDA visibility, `nvidia-smi -L`, GPU memory where available, retry
    attempt class, image sets per minute, and image sets lost per failed task.
12. Convert matrix results into an operating policy covering validation default,
    production Cellpose resource and concurrency, production DeepProfiler
    resource, default chunk size, retry policy, scratch footprint, and evidence
    required before scaling to multiple plates.
13. Test FUSE mitigations through explicit runtime modes rather than manual
    script edits: `baseline`, `node-local`, `unsquash`, and `scratch-sif`.

Decision rules:

- Do not prefer a GPU type unless queue wait, runtime, memory, output
  equivalence, or failure data makes the choice obvious.
- Treat MIG as a validation candidate until Cellpose and DeepProfiler prove
  functional equivalence and memory headroom.
- Do not infer MIG GPU-memory headroom from `maxvmem` or `peak_vmem`; capture
  device-memory evidence directly where possible.
- Prefer full GPUs for production-like chunks unless evidence shows MIG gives
  better effective turnaround without changing outputs or increasing failures.
- Keep exact job IDs, run roots, dataset names, and host details in scratch
  evidence rather than tracked docs.
- DeepProfiler remains serialized for this matrix. Parallel DeepProfiler is a
  separate future test because concurrent MIG DeepProfiler already failed with
  cuDNN initialization errors.

## Loop 486 Gated MIG Functional Validation

Loop 486 is the gate between promising MIG evidence and using MIG for broader
pipeline validation. It exists because two risks are now clear:

1. A successful SGE job does not prove the intended pipeline stage actually ran;
   Nextflow can reuse cached work if the diagnostic input paths and hashes are
   not made run-specific.
2. A small MIG smoke does not prove larger Cellpose or DeepProfiler chunks fit
   in MIG GPU framebuffer memory.

Gates:

1. **Cache-safe setup**: deploy the per-run staged-root diagnostic generator and
   verify new diagnostic configs cannot reuse the prior Cellpose work hash.
2. **Matched Cellpose comparison**: run fresh same-input `gpu-mig=1` and
   `gpu=1` segment-only smokes; compare output presence, locations row count,
   zero-cell behavior, exit status, FUSE messages, queue wait, execution time,
   CPU RSS, virtual memory context, and GPU-memory evidence where available.
3. **MIG DeepProfiler handoff**: run `stages: [segment, extract]` on MIG and
   verify the DeepProfiler package, zero-cell continuation path, or feature
   artifacts.
4. **Policy checkpoint**: document whether MIG is approved for quick validation,
   approved only for specific stages, or blocked. Full GPU remains the
   production-like default unless MIG output equivalence and memory headroom are
   demonstrated for the intended chunk size.
5. **Downstream unlock**: only after the policy checkpoint proceed to real-cell
   DeepProfiler feature validation, staging/destaging, and larger chunk
   threshold tests.

Evaluation metrics:

- Scheduler: queue wait, resource request, GPU class, accepted/running/failure
  state.
- Runtime: exit status, wallclock, trace status, first error line.
- Outputs: masks, locations, DeepProfiler metadata/index, features or zero-cell
  marker.
- Memory: `peak_rss`/`h_rss` for CPU resident memory, `peak_vmem`/`maxvmem` as
  virtual-memory context only, and direct GPU-memory evidence where available.
- Container behavior: FUSE/squashfuse messages, sandbox extraction fallback,
  `Transport endpoint is not connected`, and cleanup/teardown symptoms.

Current Gate 1 evidence:

- The first concurrent Gate 1 launch exposed a shared permanent-mirror
  `.nextflow/cache` lock. The diagnostic launcher was updated to run Nextflow
  from the scratch run directory with a per-run `NXF_HOME`, avoiding that
  shared lock.
- Fresh same-input Cellpose smokes then completed on both `gpu-mig=1` and
  `gpu=1` without cache reuse.
- Both runs used CUDA, produced a mask TIFF and locations CSV, handled the
  zero-cell image as expected, and exited 0.
- Both runs still emitted FUSE mount messages but did not show
  `Transport endpoint is not connected` or bus-error failure.
- Both runs showed low CPU RSS and high virtual memory. This supports the
  memory-model interpretation, but it still does not prove larger chunks fit in
  MIG GPU framebuffer.

Current Gate 2 evidence:

- A MIG `segment,extract` smoke completed the Nextflow graph with index, chunk,
  Cellpose, and `FEATURE_EXTRACT` all exiting 0.
- Cellpose again used CUDA, wrote the expected mask and locations outputs, and
  showed FUSE mount messages without transport-endpoint or bus-error failure.
- `FEATURE_EXTRACT` built the DeepProfiler input package and published the
  documented zero-cell continuation marker.
- This proves the zero-cell Cellpose-to-DeepProfiler handoff path on MIG. It
  does not prove actual DeepProfiler feature generation or MIG GPU-memory
  headroom, because DeepProfiler proper is skipped when there are no cells.

Current policy checkpoint:

- MIG is approved for quick tiny Cellpose validation and zero-cell handoff
  validation.
- MIG is not yet approved for real-cell DeepProfiler feature generation, larger
  chunks, or production-like runs.
- Full GPU remains the production-like default until a real-cell MIG feature
  run and larger-chunk memory evidence support changing that.

## Out of Scope

- Broad production analysis runs.
- Moving final results into batch-specific output directories.
- Destructive cleanup of permanent mirror state.
- DataStore write-back changes before stage-in/stage-out is independently validated.
