# TODOs

## Phase 2.7: Scratch Batch Reproducibility Hardening

Source plan: `docs/plans/seqera-eddie-scratch-batching-plan.md`

Goal: make `cptools2 pipeline` run scratch-safe Eddie batches as reproducible units without changing final output layout.

### Loop 350: Batch Command Isolation and Provenance

- [x] Verify Loop 350 tests outside the sandbox, because pytest `tmp_path` cleanup hangs under sandbox ACL restrictions on this Windows workspace.
- [x] Confirm the new command-builder test fails red for the expected missing `_build_nextflow_command` helper before implementation.
- [x] Normalize or construct trace path assertions so they work on Windows and Eddie path separators.
- [x] Extract `_build_nextflow_command(...)` from `cmd_pipeline()` so work-dir, params, profile, resume, trace, report, and timeline flags are unit-testable.
- [x] Generate stable `batch_name` values such as `batch_001` and use `work/<batch_name>` as the Nextflow work directory.
- [x] Add per-batch `-with-trace`, `-with-report`, and `-with-timeline` paths under `traces/`.
- [x] Add `batch_name` and `batch_work_dir` to each `params.batch_###.json` while keeping `params.output_dir` unchanged.
- [x] Extend dry-run output so users see plates, estimated scratch, work dir, unchanged output dir, traces, and params path.
- [x] Add focused CLI tests for command construction, dry-run reporting, resume behavior, and unchanged output layout.

### Loop 360: Scratch Sizing Configuration

- [x] Add `utilisation_fraction` and `work_factor` parameters to `cptools2.batch.create_batches()` with defaults preserving current behavior.
- [x] Add config keys `scratch_utilisation_fraction` and `scratch_work_factor`.
- [x] Validate scratch sizing values are positive and sensible.
- [x] Thread sizing overrides from parsed config into `_build_scratch_batches()`.
- [x] Include scratch model values in dry-run output.
- [x] Add unit tests for default behavior and override behavior in `tests/test_batch.py` and `tests/test_cli.py`.

### Loop 370: Guarded Post-Success Cleanup

- [x] Add a `--clean-work` CLI flag.
- [x] Run cleanup only after a batch completes successfully.
- [x] Try `nextflow clean -f -work-dir <batch_work_dir>` first.
- [x] Add a guarded direct deletion fallback for the exact resolved batch work directory only.
- [x] Refuse deletion unless the target is inside the configured work root and contains the expected `batch_###` path component.
- [x] Never clean failed batches.
- [x] Add tests covering success cleanup, failed-batch no-cleanup, and path-safety rejection.

### Loop 380: Eddie Calibration and Documentation

- [ ] Add conservative `maxForks` tuning only after trace/report artifacts exist.
- [x] Document the flat-output, batch-work-dir layout in the plan or Eddie docs.
- [x] Attempt dry-run against the Loop 230 example-screen config locally, blocked by `PermissionError: [WinError 5] Access is denied: 'C:\\exports'` before Nextflow.
- [ ] Run a small Eddie batch and record trace/report/timeline plus peak scratch usage.
- [ ] Tune scratch factor and concurrency from observed Eddie data.

## Phase 2.8: Eddie Container Validation and Runtime Certification

Goal: certify the permanent Eddie mirror and scratch runtime layout, then validate CellProfiler, Cellpose, and DeepProfiler containers with dry-runs and minimal smoke tests.

Permanent root: `${CPTOOLS2_PROJECT_ROOT}`
Scratch root: `${CPTOOLS2_SCRATCH_ROOT}`

### Loop 390: Planning State and Eddie Inventory

- [x] Add Phase 2.8 plan and Ralph loops.
- [x] Reconcile stale Phase 2.7 TODOs for completed Loops 360 and 370.
- [x] Update planning state in `AGENTS.md` and `CLAUDE.md`.
- [x] Create or verify scratch runtime subdirectories: `work`, `params`, `logs`, `traces`, `staging`, `snapshots`.
- [x] Snapshot permanent mirror git status, current commit, and remote-only changes.
- [x] Snapshot actual `.sif` container inventory and manifest state.
- [x] Snapshot Eddie module availability for Nextflow, Singularity, Miniforge, CUDA/GPU, and SGE.
- [x] Document blockers and user-decision points before mirror sync.

### Loop 400: Permanent Bootstrap and Runtime Config

- [x] Create an Eddie environment bootstrap that loads pinned modules and exports permanent/scratch roots.
- [x] Ensure stable config/container references point to permanent space.
- [x] Ensure runtime cache/work/log/trace/generated params point to scratch.
- [x] Add tests or static checks for the permanent-vs-scratch path contract.
- [x] Document dry-run usage from the permanent mirror.

### Loop 410: Mirror Sync and Manifest Reconciliation

- [x] Compare local HEAD with permanent mirror HEAD and list remote-only modifications.
- [x] Prepare a conservative mirror update strategy that preserves remote changes.
- [x] Reconcile container manifest expectations with actual `.sif` files in permanent `containers/`.
- [x] Verify live Eddie GPU scheduling syntax before changing GPU config.
- [x] Record exact user approval needed before destructive mirror action, if any.
- [x] Reset the permanent mirror source/config/docs state to GitHub `origin/ai-update` after explicit approval, preserving `containers/`.
- [x] Create the permanent project virtualenv at the mirror root with pip cache/build temp under scratch.

### Loop 420: CellProfiler CPU Container Smoke

- [x] Run CellProfiler container startup/version check on Eddie.
- [ ] Run or prepare the smallest CPU-only CellProfiler smoke using scratch staging and permanent config.
- [x] Capture logs, resource use, output locations, and blockers.

### Loop 430: Cellpose and DeepProfiler GPU Smoke

- [x] Validate Cellpose container startup with `--nv` and CUDA visibility. Corrected job `55264576` completed successfully with Cellpose `4.1.1` and GPU `NVIDIA H200 NVL`.
- [x] Validate DeepProfiler container startup with `--nv` and TensorFlow/CUDA visibility. Job `55269157` completed successfully with TensorFlow `2.5.3` and visible GPU device.
- [x] Prepare minimal GPU smoke inputs or document missing assets/queue blockers. Startup probes are complete; functional smoke still needs tiny image assets and DeepProfiler config/weights/checkpoint decisions.

### Loop 440: End-to-End Eddie Smoke and Evidence

- [x] Run the smallest practical multi-engine dry-run on Eddie. Candidate config exists at `config/loop440-eddie-smoke.yaml`; tiny staged plate was created and indexed/chunked on Eddie, and the cptools2 dry-run passes from the synced permanent mirror.
- [ ] Capture trace/report/timeline, scheduler evidence, scratch usage, and cleanup behavior. Captured dry-run failure evidence and no-job-left-running status; real trace/report/timeline still pending.
- [x] Update docs with final evidence, blockers, and recommended next production gate.

### Loop 450: DeepProfiler Input Package Handoff

- [ ] Add failing tests for a robust DeepProfiler input package built from a Nextflow chunk manifest, DeepProfiler config, and Cellpose centroids.
- [ ] Implement `build_deepprofiler_input_package(...)` in `cptools2.nextflow_chunking`, including valid zero-location output.
- [ ] Wire `nextflow/modules/feature_extract.nf` to call the package builder and validate package artifacts before launching DeepProfiler.
- [ ] Keep bulky `dp_project` internals in Nextflow work while publishing only small audit artifacts and final features.
- [ ] Rerun Loop 440 on Eddie with `--resume` and capture whether the run passes or reaches a new model/checkpoint blocker.

### Loop 460: DeepProfiler Checkpoint Resolution and Final Smoke

- [ ] Locate or obtain the Cell Painting DeepProfiler checkpoint `combinedset_cellsout_e30.hdf5`.
- [ ] Store the checkpoint in permanent project space outside Git-tracked source, with any runtime/cache artifacts remaining in scratch.
- [ ] Configure `feature_extraction.weights` through a local/site config path, not a committed site-specific path.
- [ ] Add tests/docs that explain the checkpoint contract without exposing local Eddie paths.
- [ ] Resume the Loop 440 Eddie smoke with `--resume --clean-work` and capture trace/report/timeline evidence.
- [ ] If the checkpoint cannot be located, document the blocker, candidate sources, and the exact user decision needed.
