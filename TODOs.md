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

- [ ] Add `utilisation_fraction` and `work_factor` parameters to `cptools2.batch.create_batches()` with defaults preserving current behavior.
- [ ] Add config keys `scratch_utilisation_fraction` and `scratch_work_factor`.
- [ ] Validate scratch sizing values are positive and sensible.
- [ ] Thread sizing overrides from parsed config into `_build_scratch_batches()`.
- [ ] Include scratch model values in dry-run output.
- [ ] Add unit tests for default behavior and override behavior in `tests/test_batch.py` and `tests/test_cli.py`.

### Loop 370: Guarded Post-Success Cleanup

- [ ] Add a `--clean-work` CLI flag.
- [ ] Run cleanup only after a batch completes successfully.
- [ ] Try `nextflow clean -f -work-dir <batch_work_dir>` first.
- [ ] Add a guarded direct deletion fallback for the exact resolved batch work directory only.
- [ ] Refuse deletion unless the target is inside the configured work root and contains the expected `batch_###` path component.
- [ ] Never clean failed batches.
- [ ] Add tests covering success cleanup, failed-batch no-cleanup, and path-safety rejection.

### Loop 380: Eddie Calibration and Documentation

- [ ] Add conservative `maxForks` tuning only after trace/report artifacts exist.
- [x] Document the flat-output, batch-work-dir layout in the plan or Eddie docs.
- [x] Attempt dry-run against the Loop 230 Sarah-screen config locally, blocked by `PermissionError: [WinError 5] Access is denied: 'C:\\exports'` before Nextflow.
- [ ] Run a small Eddie batch and record trace/report/timeline plus peak scratch usage.
- [ ] Tune scratch factor and concurrency from observed Eddie data.
