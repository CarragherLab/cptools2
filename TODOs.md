# TODOs

## Phase 3.1: Production Execution Hardening and Durable Stage-Out Evidence

Source plan: `.claude/plans/phase-3.1-durable-stageout-evidence.md`
Loop plan: `.claude/plans/phase-3.1-ralph-loops.md`
Implementation plan: `docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md`
Pivot decision: `docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md`

Goal: harden the production cptools2 execution model so Nextflow-led Eddie runs
launch from isolated scratch roots, preserve stage-out verification evidence,
clean verified batches, and report multi-plate failures clearly.

Status note: active DeepProfiler production development is paused as of
2026-06-09. The batching, scratch, stage-out, and reporting lessons from Phase
3.1 remain active requirements for the DINO successor route. DeepProfiler is now
legacy/maintenance unless explicitly revived for reproducibility or regression
support.

Consolidation plan:
`docs/superpowers/plans/2026-06-09-worktree-remediation-consolidation.md`

Merge map:
`docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`

### Worktree Remediation and Consolidation

- [x] Record the DeepProfiler pause and DINO pivot decision.
- [x] Inventory active worktrees: `ai-update` and clean `ai-update-DINO`.
- [x] Identify that a direct one-shot merge is high risk because DINO combines
  feature work, shared batching hardening, public-package cleanup, and large
  `.claude`/`.context` tracking changes.
- [x] Build a file-level merge map for `ai-update..ai-update-DINO`.
- [ ] Create an isolated `codex/dino-consolidation` branch from `ai-update-DINO`.
- [ ] Port only required DeepProfiler pause/Phase 3.1 decision docs into the
  consolidation branch.
- [ ] Manually reconcile shared batching files before merging any repository
  hygiene changes.
- [ ] Verify DINO-focused local tests and one Eddie dry-run before retiring
  obsolete worktrees.

### Loop 560: Durable Evidence Contract

- [x] Copy each `STAGE_OUT` `stage_out_evidence` bundle to `<output_dir>/stage_out_evidence/<batch_name>/<plate_id>/`.
- [x] Add `durable_evidence_dir` to `verification.json`.
- [x] Make the cleanup gate prefer durable evidence before falling back to work-local evidence.
- [x] Add static and CLI tests for durable evidence lookup and ledger messages.

### Loop 570: Multi-Batch Smoke Preparation

- [x] Dry-run batching smoke passed with reduced `scratch_utilisation_fraction` and explicit `plate_sizes_gb` to force multiple batches. The Eddie dry-run forced the three selected real plates into `batch_001`, `batch_002`, and `batch_003`.
- [ ] Run an Eddie verified-cleanup smoke in a fresh scratch directory and confirm durable evidence survives after `work/batch_###` deletion.
- [x] Record the recommended forced-batching recipe in the operator docs so real execution remains pending until Loop 580 and Loop 590 hardening lands.

### Loop 580: Driver Diagnostics and Isolation

- [x] Add `nextflow_diagnostics: full|minimal|off` and `--nextflow-diagnostics` so Eddie acceptance can reduce optional Nextflow observer load.
- [x] Gate config-level Nextflow `trace`, `report`, and `timeline` observers from params so `minimal` and `off` modes really disable observer startup.
- [x] Keep default diagnostics compatible with existing runs while using `minimal` for the first three-plate acceptance run.
- [x] Ensure Nextflow commands are built from a fresh scratch run root with run-local work, trace, report, and timeline paths.
- [x] Add focused pytest and static Nextflow config coverage for full, minimal, and off diagnostics modes.

### Loop 590: Production Launcher and Reporting

- [x] Add an Eddie `tmux` launcher contract that refuses stale sessions, sources the permanent Eddie bootstrap, records preflight evidence, sets run-local `NXF_HOME`, and runs `cptools2 pipeline` from the scratch run root.
- [x] Add `--continue-on-batch-failure` so independent later batches can finish and failed batch work is preserved after Nextflow failures, unverified stage-out, or cleanup failures.
- [x] Add `run_report.md` summarising batch status, failures, cleanup status, and durable stage-out evidence.
- [x] Document why the Nextflow driver is launched from `tmux` on login and not submitted as a qsub job.

### Loop 600: Three-Plate Production Acceptance

- [x] Run the three-plate acceptance workflow from a fresh Eddie scratch directory using the production launcher and `--nextflow-diagnostics minimal`.
- [x] Confirm `batch_status.csv` records all three batch attempts after the first failure; `--continue-on-batch-failure` worked.
- [x] Confirm durable `stage_out_evidence` survives outside work for completed stage-out tasks. The fresh run produced durable evidence, but verified cleanup did not run because the DeepProfiler route failed after stage-out.
- [x] Confirm `run_report.md`, launcher logs, qstat snapshots, and process snapshots give enough evidence for users to understand pass or failure state.
- [ ] DINO successor follow-up: prove verified cleanup on a successful DINO batch after durable stage-out evidence is written.
- [ ] DINO successor follow-up: if minimal diagnostics passes, test whether `full` diagnostics can be restored without the Eddie driver thread failure recurring.
- [x] First acceptance classified as failed: Cellpose hit GPU OOM under concurrent GPU load, and a deterministic `STAGE_OUT` verification heredoc newline bug was observed after rsync.

### Loop 610: Acceptance Recovery and Conservative GPU Baseline

- [x] Fix the `STAGE_OUT` verification heredoc newline bug observed in the first three-plate acceptance run.
- [x] Add launcher controls for `CPTOOLS2_GPU_MAX_FORKS`, `CPTOOLS2_SEGMENT_MAX_FORKS`, and `CPTOOLS2_FEATURE_MAX_FORKS`, preserving CLI overrides after Eddie bootstrap sourcing.
- [x] Rerun the three-plate acceptance with `CPTOOLS2_GPU_MAX_FORKS=1`, `CPTOOLS2_SEGMENT_MAX_FORKS=1`, and `CPTOOLS2_FEATURE_MAX_FORKS=1`; it failed usefully because all three Cellpose tasks landed on a saturated H200 GPU 0 with only ~5-8 MiB free.
- [x] Add shared hardening from the DINO evidence: thumbnail exclusion, namespaced stage-out, feature-export memory increase, and retryable Cellpose GPU preflight.
- [x] Free Eddie scratch quota by removing approved obsolete Phase 3.1 work, staging, Singularity tmp, FUSE container, and smoke work directories while preserving Phase 2.9 evidence. Removed the later stale post-hardening `outputs/work` again on 2026-06-04 after the mis-rooted retry wrote 102G into the old run.
- [x] Run focused local Phase 3.1 launcher/runtime/Nextflow verification tests before the fresh post-cleanup acceptance. Passed locally: `25 passed, 2 skipped`; only known Windows pytest cache ACL warning remains.
- [x] Sync and verify the current Phase 3.1 shared-hardening files on the permanent Eddie mirror. Eddie syntax/signature checks passed for launcher syntax, thumbnail exclusion, Cellpose GPU preflight, feature export memory, and namespaced feature outputs; intermittent Eddie session-channel failures recovered with retries.
- [x] Launch a fresh three-plate post-cleanup acceptance run in a new timestamped Eddie scratch diagnostics directory: `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-3.1-fresh-acceptance-20260604144945`. Dry-run confirmed all `output_dir`, `commands location`, params, traces, and `work/batch_###` paths are under the fresh root before launch.
- [x] Confirm `driver_status.json`, `batch_status.csv`, `run_report.md`, durable `stage_out_evidence`, and pass/fail classification on the conservative DeepProfiler baseline. The run failed, but it produced enough evidence to support the DeepProfiler pause decision.
- [x] Document the fresh run path, quota state, pass/fail classification, and intervention decision in the DeepProfiler pause/DINO pivot spec.
- [ ] DINO successor follow-up: run a separate GPU scale ladder that increases only Cellpose concurrency first after a conservative DINO baseline passes.

## Phase 3.0: Verified Batch Cleanup and Scratch Relief

Source plan: `.claude/plans/phase-3.0-verified-batch-cleanup.md`

Goal: delete completed batch scratch work by default only after rsync-backed
stage-out evidence verifies user-facing outputs are present.

### Loop 530: Stage-Out Evidence Contract

- [x] Update `STAGE_OUT` to write parseable rsync logs and `stage_out_evidence/verification.json`.
- [x] Include required destination artifact checks for the DeepProfiler feature-export route.
- [x] Add static/module tests proving the rsync log and verification JSON contract.

### Loop 540: Verified Cleanup Lifecycle

- [x] Add `--cleanup-policy keep|success|verified` with verified as the normal pipeline default.
- [x] Preserve `--clean-work` as a compatibility alias for success cleanup.
- [x] Add batch status ledger rows for Nextflow, stage-out, cleanup, scratch, params, trace, attempt, and message state.
- [x] Parse stage-out verification artifacts before cleanup and block cleanup if any required artifact is unverified.
- [x] Re-check scratch availability before launching each batch.
- [x] Keep the existing exact `work/batch_###` path guard for deletion.

### Loop 550: Documentation and Gate Smoke

- [x] Document cleanup policy, rsync evidence, ledger behavior, and diagnostic `keep` mode.
- [ ] Run focused cleanup, CLI, and Nextflow architecture tests. Nextflow architecture and parser tests pass locally; full `tmp_path` CLI tests are currently blocked by Windows sandbox temp-dir permission errors.
- [x] Prepare Eddie smoke instructions for a small multi-batch run that proves cleanup releases scratch after verified stage-out.

## Phase 2.9: Feature Export Tables and Measurement Quality Gate

- [x] Phase 2.9: implement DeepProfiler feature export contract with plate-level CSV tables.
- [x] Phase 2.9: backfill the representative Eddie run into manifest, site, cell, and quality tables.
- [x] Phase 2.9: run the clean full-plate DataStore-backed certification route until it reaches the export join boundary.
- [x] Phase 2.9 Loop 510: repair the plate-level export join with chunk-aware payloads and plate-aware published CSV names.
- [x] Phase 2.9 Loop 520: certify the repaired export join on the clean Eddie full-plate route and record pass evidence.
- [x] Phase 2.9 follow-up: harden DeepProfiler plate identity validation and add per-plate/run-level export status reports for partial multi-plate exports.

### Roadmap Follow-up: Optional Parquet Export Scaling

- [ ] Deferred legacy DeepProfiler: stream or batch cell-level Parquet export so optional Parquet output does not materialize the full wide cell table in Python and Polars memory on full-plate runs.
- [ ] Deferred legacy DeepProfiler: add a Parquet scalability check with a realistic cell-table size before treating Parquet as a full-plate export recommendation. CSV remains the certified Phase 2.9 export-ready route.

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
- [x] Run a small Eddie batch and record trace/report/timeline plus peak scratch usage. Loop 440 tiny smoke completed on Eddie with trace, report, and timeline under `${CPTOOLS2_SCRATCH_ROOT}/results/loop440/traces/`.
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
- [x] Capture trace/report/timeline, scheduler evidence, scratch usage, and cleanup behavior. Final tiny smoke produced trace/report/timeline, published a `no_cells.tsv` marker for the zero-cell DeepProfiler chunk, and removed the batch work directory after the guarded direct-deletion fallback.
- [x] Update docs with final evidence, blockers, and recommended next production gate.

### Loop 450: DeepProfiler Input Package Handoff

- [x] Add failing tests for a robust DeepProfiler input package built from a Nextflow chunk manifest, DeepProfiler config, and Cellpose centroids.
- [x] Implement `build_deepprofiler_input_package(...)` in `cptools2.nextflow_chunking`, including valid zero-location output.
- [x] Wire `nextflow/modules/feature_extract.nf` to call the package builder and validate package artifacts before launching DeepProfiler.
- [x] Keep bulky `dp_project` internals in Nextflow work while publishing only small audit artifacts and final features.
- [x] Rerun Loop 440 on Eddie with `--resume` and capture whether the run passes or reaches a new model/checkpoint blocker.

### Loop 460: DeepProfiler Checkpoint Resolution and Final Smoke

- [x] Locate or obtain the official Cell Painting DeepProfiler checkpoint `Cell_Painting_CNN_v1.hdf5`.
- [x] Store the checkpoint in permanent project space outside Git-tracked source, with any runtime/cache artifacts remaining in scratch.
- [x] Configure `feature_extraction.weights` through `${CPTOOLS2_MODEL_DIR}`, not a committed site-specific path.
- [x] Add tests/docs that explain the checkpoint contract without exposing local Eddie paths.
- [x] Resume the Loop 440 Eddie smoke with `--resume --clean-work` and capture trace/report/timeline evidence.
- [x] Resolve the checkpoint blocker; no user decision is currently needed for the tiny smoke.

### Follow-up: Cleanup Command Tidying

- [ ] Replace or version-gate the current `nextflow clean -f -work-dir <batch_work_dir>` attempt, because Eddie Nextflow 25.10.2 reports `Unknown option: -work-dir`. The guarded direct-deletion fallback removed the exact batch work directory successfully, so this is a log clarity issue rather than a smoke-test blocker.
- [ ] Add a real-cell tiny fixture or small real-cell Eddie smoke to validate actual DeepProfiler feature `.npz` output, not only the zero-cell `no_cells.tsv` continuation path.

## Phase 2.8 Follow-up: Eddie Singularity/FUSE Root-Cause Investigation

Goal: identify why the real-data Cellpose smoke reached 100% image processing and then failed with Singularity/FUSE-style `Transport endpoint is not connected` errors and exit 135, before choosing a mitigation.

Evidence baseline:

- Real-data staged smoke target: one plate, `stage_data: true`, `stages: [segment]`, `max_chunks: 1`, default `chunk_size: 96`. Keep dataset names, plate IDs, and run directories in local scratch evidence, not tracked TODOs.
- `STAGE_IN` completed on `staging` with one slot; trace shows 31m duration and 201.1 GB read/write for the full 103 GB plate copy.
- `BUILD_IMAGESET_INDEX` and `CHUNK_IMAGESETS` completed.
- `CELLPOSE_SEGMENT` ran on the GPU queue, processed 96/96 images, then failed with exit 135 and `Bus error (core dumped)`.
- `.command.err` included repeated `Transport endpoint is not connected` from `grep`, `sed`, `tr`, and `ps` during the Nextflow wrapper's monitoring/teardown path.
- `STAGE_OUT` did not run; no destaged files were produced.
- Failed work remains under scratch for inspection; do not clean it until the investigation has captured needed evidence.

### Loop 470: Failure Signature and Environment Capture

- [ ] Capture the failed Cellpose task wrapper details: `.command.run`, `.command.sh`, `.command.err`, `.command.out`, `.exitcode`, trace row, Nextflow version, Singularity version, host, queue, GPU model, and exact SIF path.
- [ ] Capture mount/runtime state from the failed task context if still inspectable: work-dir symlinks, `cellpose_input` link targets, `cellpose_masks` contents, number of masks written, and whether any stale FUSE mountpoints remain visible.
- [ ] Record whether the failed SIF is mounted via squashfuse/non-setuid mode on Eddie Singularity 4.3.4, and whether runtime temporary/session paths are on shared scratch, permanent group storage, or node-local storage.
- [ ] Verify whether `SINGULARITY_TMPDIR`, `SINGULARITY_CACHEDIR`, `TMPDIR`, `CPTOOLS2_HOME`, and Nextflow work dirs resolve to paths appropriate for long GPU tasks on Eddie.

### Loop 480: Reproduction Matrix

- [x] Create a scratch-only real-data diagnostic config template that keeps local/site paths out of Git and makes each variant explicit: `run_id`, `chunk_args.job_size`, SIF source, temp/cache strategy, output directory, and `data_destination`.
- [x] Fix the scratch launcher so it always writes `status.txt` on success or failure; record launcher PID, run ID, config path, log path, and final exit code for every diagnostic run.
- [x] Add a repo-safe diagnostic generator and local tests before further Eddie submissions; the committed source uses environment-driven paths and defaults to generate-only mode.
- [x] Validate the diagnostic generator locally: focused pytest suite, ruff, byte-compilation, forbidden path scan, and generated launcher `bash -n`.
- [x] Submit tiny Eddie SGE acceptance jobs before further pipeline diagnostics: staging and standard compute jobs were accepted, ran, and exited 0; a GPU no-op job was accepted and remains queued for GPU capacity.
- [x] Submit current-docs GPU probes: `gpu-mig=1` accepted, ran on the GPU queue, received scheduler-managed `CUDA_VISIBLE_DEVICES`, and exited 0; full `gpu=1` was accepted but remains queued for full-GPU capacity.
- [x] Update mirrored Eddie docs so copyable GPU examples use `-q gpu` with `-l gpu=N` or `-l gpu-mig=1`, and copyable memory examples use `h_rss` rather than deprecated `h_vmem`.
- [ ] Rerun the same real-data plate with `chunk_args.job_size: 8` and `max_chunks: 1` using the permanent SIF and current scratch temp/cache settings. Capture trace/report/timeline, Cellpose progress, `.exitcode`, mask count, and whether `Transport endpoint is not connected` appears. The first variant has been launched; keep exact run IDs and SGE job IDs in local scratch evidence, not tracked TODOs.
- [ ] If size 8 passes, rerun with `chunk_args.job_size: 16`, then 32, then 64 if needed, to find the first failure threshold. Stop once the failure signature recurs or 64 passes cleanly.
- [ ] If size 8 fails, rerun size 8 with a scratch-local copy of `cellpose_sam_1.0.sif` to test whether reading the SIF from permanent group storage contributes to the FUSE failure. Scratch-local SIF copy prepared under the diagnostics scratch container directory.
- [ ] If size 8 fails with both permanent and scratch-local SIFs, rerun size 8 with task-local `SINGULARITY_TMPDIR`, `SINGULARITY_CACHEDIR`, and `TMPDIR` under node-local `$TMPDIR` where Eddie permits it, while keeping Nextflow work/results in scratch.
- [ ] Run a container-only GPU probe from the same Cellpose SIF that executes `python -m cellpose --version`, imports `torch`, prints CUDA visibility, writes/reads a small batch of files in the work dir, and exits. Run it with the same bind options to separate image-processing failure from container runtime teardown failure.
- [ ] Run a short image-processing probe on a tiny subset copied from the staged real-data plate outside Nextflow, using the same SIF and GPU queue, to compare raw Singularity behavior against Nextflow-managed wrapper behavior.
- [ ] For every diagnostic run, record SGE job ID, queue, host, GPU model, runtime, exit status, max memory if available, scratch work size, mask count, destaged-file count if `STAGE_OUT` is reached, and the exact first error line.
- [ ] Classify each diagnostic result as one of: `passes`, `fails during Cellpose`, `fails after Cellpose before post-processing`, `fails during post-processing`, `fails during Nextflow wrapper teardown`, or `scheduler/runtime failure`.

### Loop 485: GPU Capacity and Throughput Strategy

Goal: decide whether cptools2 should use full GPUs, MIG partitions, or both for validation and production-like chunks based on total turnaround time and output equivalence, not preference for a node type.

Decision principle:

```text
effective turnaround =
  queue wait
  + container startup
  + image processing runtime
  + staging/destaging
  + retry or failure cost
```

This loop should compare GPU classes by evidence. MIG may start faster, but it is a smaller A100 partition with tighter GPU-memory limits. Full GPUs may queue longer but can support larger batches and may reduce per-image overhead if long jobs are stable.

Memory model to avoid false assumptions:

- `h_rss` is Eddie's enforced CPU RAM request per slot. This is the value we
  should size from observed RSS, not from virtual memory.
- Nextflow `peak_rss` is the best trace-level signal for CPU resident memory.
- SGE `maxvmem` and Nextflow `peak_vmem` can be much larger because they track
  virtual address space. They are useful context, but they are not the MIG GPU
  framebuffer and should not be used to conclude that a MIG partition used more
  than its GPU-memory limit.
- MIG GPU memory is separate device framebuffer. The observed A100 MIG slice is
  materially smaller than a full GPU, so Cellpose and DeepProfiler must prove
  functional fit and memory headroom on realistic chunks before MIG becomes a
  production recommendation.
- Current tiny Cellpose evidence showed low RSS but high virtual memory, which
  is compatible with using MIG. It does not prove that larger batches fit in MIG
  GPU memory.

- [x] Capture live GPU capacity snapshots at submission and completion using the Eddie-documented resource view: `qstat -F gpu,gpu-mig,gputype,h_rss -q gpu`. Initial snapshots were captured in scratch evidence for the matched container-only pass.
- [x] Record queue wait, start host, GPU type, resource request, runtime, exit status, and scheduler wait reason for every GPU diagnostic job. Exact identifiers and paths remain in scratch evidence only.
- [ ] Treat `gpu-mig=1` as a validation candidate only until functional equivalence and memory headroom are proven for Cellpose and DeepProfiler.
- [ ] Run matched container-only probes on `gpu-mig=1` and `gpu=1` with the same SIF, bind options, tmp/cache policy, and environment reporting.
- [x] First Loop 485 container-only pass: Cellpose and DeepProfiler completed on both `gpu-mig=1` and `gpu=1`, saw scheduler-managed CUDA visibility, and imported their GPU frameworks successfully. MIG started much faster in this run; full GPU started later but completed faster once scheduled. Exact job IDs and run roots are stored in scratch evidence only.
- [x] Collect matched full-GPU probe results after they leave the queue, then compare queue wait, runtime, GPU type, framework visibility, and Singularity/FUSE behavior against the completed MIG probes. Both GPU classes still hit the Singularity FUSE mount failure and succeeded through sandbox extraction, so GPU class alone is not the FUSE mitigation.
- [ ] Run matched tiny Cellpose probes on `gpu-mig=1` and `gpu=1`; compare mask counts, output presence, first error line, runtime, and GPU/system memory indicators where available. First Nextflow-managed MIG functional smoke succeeded with CUDA, wrote a mask TIFF and locations CSV, and continued cleanly through the zero-cell case; a matched same-input full-GPU run is still needed for equivalence.
- [ ] Run matched tiny DeepProfiler probes on `gpu-mig=1` and `gpu=1`; compare feature file presence, feature dimensions, zero-cell handling if applicable, runtime, and GPU/system memory indicators where available.
- [ ] Test increasing chunk or batch sizes only after tiny probes pass, and stop when memory pressure, FUSE errors, or queue/runtime tradeoffs make the resource class unattractive.
- [ ] For functional MIG probes, capture GPU memory evidence where possible
  (`nvidia-smi` before/during/after, framework device memory logs, and any OOM
  messages), because CPU RSS and virtual memory do not answer whether MIG
  framebuffer capacity is sufficient.
- [ ] Compare against native cptools2-style batching: fewer longer GPU jobs with larger chunks versus many small jobs, including queue wait and failure blast radius.
- [ ] Define a resource policy recommendation: validation default, production-like default, when to use MIG, when to require full GPU, and what evidence must be captured before changing defaults.
- [ ] Keep exact job IDs, run roots, dataset identifiers, and host details in scratch evidence; tracked docs should describe resource classes and conclusions without site-specific paths.
- [ ] Run the approved `max_chunks: 5` GPU strategy matrix using chunk sizes 48, 24, and 96 across MIG-only and full-GPU policies.
- [ ] If both MIG-only and full-GPU policies pass, run the mixed candidate with Cellpose on MIG and serialized DeepProfiler on full GPU.
- [ ] Compare total turnaround, output counts, queue wait, runtime, CPU RSS, virtual memory context, direct GPU evidence where available, and FUSE/runtime symptoms.
- [ ] Recommend whether MIG can be used for production Cellpose chunks while keeping DeepProfiler serialized and/or on full GPUs.
- [ ] Add process-specific GPU resource controls before the mixed-policy run, so Cellpose and DeepProfiler can request different GPU classes in one Nextflow execution.
- [ ] Add GPU task diagnostics logging for hostname, CUDA visibility, `nvidia-smi -L`, and GPU memory where available.
- [ ] Classify benchmark outcomes as `pass_first_attempt`, `pass_after_retry`, or `failed`, and include retry cost in total turnaround.
- [ ] Normalize chunk-size comparisons by image sets processed per minute and image sets lost per failed task, because fixed `max_chunks: 5` means chunk sizes 24, 48, and 96 process different total work.
- [ ] Treat the matrix as both validation and operating-policy evidence for future multi-plate, multi-chunk runs.
- [ ] Convert the matrix result into a production-scale operating policy covering Cellpose resource class/concurrency, DeepProfiler resource class/concurrency, default chunk size, retry cost, scratch footprint, stage-in/stage-out assumptions, and evidence required before multi-plate scale-up.
- [ ] Use explicit container runtime modes (`baseline`, `node-local`, `unsquash`, `scratch-sif`) to test FUSE mitigations instead of manually editing job scripts between runs.

### Loop 486: Gated MIG Functional Validation

Goal: prove whether MIG is safe for routine validation before using it as the default route for further pipeline testing.

Gate 0 - cache-safe diagnostic setup:

- [x] Deploy the per-run staged-root diagnostic generator to the permanent Eddie mirror.
- [x] Verify a newly generated diagnostic config uses a run-specific `staged-root` path and does not reuse the prior MIG work hash. Gate 0 generation now records `input_dir` and `staged_root` under the run directory; exact paths remain in scratch evidence only.
- [x] Record the Nextflow command, `-work-dir`, trace path, report path, timeline path, `CPTOOLS2_GPU_RESOURCE`, and stage list in scratch evidence.

Gate 1 - matched Cellpose resource comparison:

- [x] Run a fresh same-input Cellpose smoke on `gpu-mig=1` with `stages: [segment]`, preserving trace/report/timeline and work logs.
- [x] Run a fresh same-input Cellpose smoke on `gpu=1` with `stages: [segment]`, preserving trace/report/timeline and work logs.
- [x] Compare mask output presence, locations CSV row count, zero-cell handling, exit status, first error line, FUSE messages, queue wait, execution runtime, `peak_rss`, `peak_vmem`, `qacct` RSS/vmem fields where available, and direct GPU-memory evidence where available. Tiny matched Cellpose runs both completed status 0, used CUDA, produced mask and locations files, had low RSS with high virtual memory, and showed FUSE mount messages without transport-endpoint or bus-error failure.
- [x] Gate decision: pass only if both runs complete or the difference is explained by scheduler/resource behavior rather than output or runtime correctness. Gate 1 passes for the tiny zero-cell Cellpose smoke; this does not establish larger-chunk MIG GPU-memory headroom.

Gate 2 - MIG DeepProfiler handoff:

- [x] Run a MIG smoke with `stages: [segment, extract]` on the tiny staged input.
- [x] Verify the Cellpose-to-DeepProfiler package exists and contains the expected metadata/index, image links, locations handling, and zero-cell continuation artifact if no cells are present.
- [x] Verify DeepProfiler either writes expected feature artifacts or exits through the documented zero-cell path without failing the workflow. Gate 2 validated the zero-cell continuation path; actual DeepProfiler feature generation remains a downstream real-cell validation.
- [x] Capture TensorFlow GPU visibility, device memory logs where available, CPU RSS, virtual memory context, FUSE messages, and first error line. TensorFlow/GPU-memory logs were not expected in this zero-cell run because the workflow exited before launching DeepProfiler proper; CPU/virtual memory and FUSE evidence were captured.

Gate 3 - MIG policy checkpoint:

- [x] Document whether MIG is approved for quick validation runs, still blocked, or approved only for specific stages. Current policy: MIG is approved for quick tiny Cellpose and zero-cell handoff validation only; it is not yet approved for real-cell DeepProfiler feature generation or larger chunks.
- [ ] Define the first larger-chunk test size and stop criteria before increasing batch/chunk size.
- [x] Keep full GPU as the production-like default unless MIG has output equivalence and measured memory headroom for the intended chunk size.

Gate 4 - downstream validation unlock:

- [x] After Gate 3 passes, proceed to DeepProfiler feature-output validation on a real-cell tiny fixture or small real-cell Eddie smoke. A representative single-chunk 96 image-set Eddie run completed end-to-end with illumination, Cellpose, and DeepProfiler; 96 feature files were published. The root cause of the first DeepProfiler zero-feature failure was a package convention mismatch: DeepProfiler required no-`f` location filenames such as `A01-1-Nuclei.csv`, while the first package only wrote `A01-f1`/`A01-f01` aliases. The package builder now writes raw, zero-padded, `f`, and no-`f` aliases, and derives TIFF dimensions from the staged images.
- [x] Stop the representative single-chunk 384 image-set Eddie comparison run after clarifying that `chunk: 384` increases image sets per task rather than creating smaller chunks.
- [x] Compare the successful 96 image-set run against smaller single-chunk runs, starting with 48 image sets and then 24 if needed, using `max_chunks: 1` to keep each test bounded. All three runs completed on Eddie with expected feature counts. Initial evidence favours `chunk: 96` for validation throughput because fixed DeepProfiler/container overhead dominates smaller chunks; `chunk: 24` and `chunk: 48` are useful lower-blast-radius options but process fewer image sets per minute.
- [x] Fix and validate multi-chunk fan-out for `max_chunks > 1`. `CHUNK_IMAGESETS` emits a list of chunk CSVs, so `main.nf` now flattens that list into one tuple per chunk before downstream processes.
- [x] Compare equal total work split as `96 x 1`, `48 x 2`, and `24 x 4`. The expanded runs published 96 feature files each after serializing DeepProfiler. Cellpose can fan out across chunks, but concurrent DeepProfiler jobs on MIG failed with cuDNN initialization errors, so Eddie feature extraction is now `maxForks = 1`.
- [ ] After DeepProfiler feature-output validation passes, proceed to staging/destaging smoke using an approved small real-data subset, with exact dataset details kept in scratch evidence only.
- [ ] After staging/destaging passes, proceed to larger chunk threshold tests and document the operational policy for validation versus production-like runs.

### Loop 490: Mitigation Research and Decision

- [ ] Review official SingularityCE/Apptainer guidance for SIF execution on network filesystems, squashfuse/FUSE behavior, cache/tmp placement, and HPC parallel execution patterns.
- [ ] Review Eddie-specific Singularity guidance and, if needed, document a concise question for Eddie support with the exact trace row and error signature.
- [ ] Evaluate mitigations only after the reproduction matrix: smaller chunks, `maxForks` throttling, scratch-local SIF staging, node-local `TMPDIR`, alternate Singularity options such as temporary sandbox extraction if supported, or moving Cellpose output generation/centroid extraction into separate shorter tasks.
- [ ] Choose the least invasive mitigation that explains the observed data, not merely the first mitigation that passes once.
- [ ] Add static tests/docs for the chosen Eddie runtime contract so future configs do not regress into the same failure mode.
- [ ] Add an operator note explaining when failed real-data diagnostic scratch work can be safely cleaned and what evidence must be retained first.
