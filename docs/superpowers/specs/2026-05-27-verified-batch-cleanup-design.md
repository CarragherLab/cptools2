# Verified Batch Cleanup Design

Date: 2026-05-27
Status: proposed
Scope: cptools2 Nextflow batch lifecycle, scratch-safe multi-plate runs

## Problem

cptools2 needs to run multi-plate batches on Eddie scratch without keeping every
completed batch resident on scratch. Native cptools2 stages a batch, processes it,
destages results, then removes staged scratch data so the next batch can fit.

The Nextflow-first route must preserve that operational behavior while avoiding
unsafe deletion. A batch must not be cleaned merely because a command returned
success unless the intended outputs have been returned to the durable destination
and enough provenance remains for users to audit what happened.

## Decision

Use verified cleanup as the production default for multi-batch pipeline runs.

Default behavior:

```text
successful batch -> verify stage-out -> clean exact batch scratch work -> continue
```

Failed or unverified batches are never cleaned automatically.

The CLI should support an explicit cleanup policy:

```text
--cleanup-policy keep
--cleanup-policy success
--cleanup-policy verified
```

Recommended defaults:

- `verified` for normal pipeline execution;
- `keep` remains available for debugging and diagnostic runs;
- `success` is available only as a less strict compatibility mode and should be
  documented as weaker than `verified`.

The existing `--clean-work` flag should be retained initially as a compatibility
alias for `--cleanup-policy verified`, with a deprecation note once the new policy
flag is documented.

## Cleanup Contract

A batch is eligible for cleanup only when all required conditions pass:

1. The batch Nextflow command exits with status `0`.
2. The batch trace exists and records completed `STAGE_OUT` task(s) for the
   expected plate outputs.
3. The expected destination directories exist for every plate in the batch.
4. Required destination artifacts are present for the selected stages.
5. The cleanup target resolves to exactly one `batch_###` directory under the
   configured work root.
6. The batch status ledger is updated before launching the next batch.

If any condition fails, cptools2 must:

- keep the batch work directory;
- record the batch as unverified or failed;
- stop before launching the next batch;
- print the exact failed condition and relevant paths.

## Batch Status Ledger

cptools2 should write a batch lifecycle ledger under the run output directory:

```text
<output_dir>/batch_status.csv
```

Required columns:

```csv
batch_id,batch_name,plates,nextflow_status,stage_out_status,cleanup_policy,cleanup_status,work_dir,scratch_before_bytes,scratch_after_bytes,message
```

Status values:

- `nextflow_status`: `not_started`, `success`, `failed`
- `stage_out_status`: `not_required`, `verified`, `unverified`, `failed`
- `cleanup_status`: `not_requested`, `not_eligible`, `cleaned`, `kept`, `failed`

The ledger must be append-safe or rewrite-safe so a resumed command can show the
current state without losing earlier batch evidence.

## Stage-Out Verification

Verification is intentionally stage-aware.

For `stage_data: true`, stage-out verification checks `params.data_destination`
when supplied, otherwise `params.output_dir`.

Verified cleanup must be based on explicit stage-out verification artifacts, not
only on the Nextflow task exit code or ad hoc human-readable log text. `rsync`
is the transfer authority: if it exits `0`, the transfer it was asked to perform
completed successfully. The cleanup gate should therefore verify that rsync
succeeded and that the proposed destination contains the expected user-facing
artifacts. It should not duplicate rsync with a separate source/destination
manifest comparison.

Each `STAGE_OUT` task should emit a machine-readable transfer evidence bundle for
the plate or summary artifact it destages:

```text
stage_out_evidence/
  rsync.log
  verification.json
```

The rsync command should write a stable log file and use a parseable line format,
for example:

```bash
rsync -rtl \
  --partial \
  --delay-updates \
  --timeout=300 \
  --itemize-changes \
  --out-format='RSYNC_ITEM\t%i\t%l\t%n%L' \
  --stats \
  --log-file="stage_out_${plate_id}.rsync.log" \
  "${results_dir}/" "$DEST/"
```

`rsync` exit code `0` is the required transfer-success signal. After rsync
completes, `STAGE_OUT` should check that the destination path contains the
stage-specific artifacts that make the output usable. This destination check is
not a full manifest comparison; it is an output suitability check.

The resulting `verification.json` should include at least:

```json
{
  "plate_id": "3723-D-100",
  "destination": "/exports/.../3723-D-100",
  "rsync_exit_code": 0,
  "rsync_log": "stage_out_3723-D-100.rsync.log",
  "required_artifacts_present": true,
  "required_artifacts": [
    "features/feature_export_status.csv",
    "features/tables/deepprofiler_cells.csv"
  ],
  "verification_status": "verified"
}
```

Batch cleanup may proceed only when every required stage-out evidence artifact for
the batch has `verification_status == "verified"`.

For extraction with DeepProfiler feature export enabled, required destination
artifacts include:

- plate output directory;
- `features/feature_export_status.csv`;
- `features/tables/deepprofiler_manifest.csv`;
- `features/tables/deepprofiler_sites.csv`;
- `features/tables/deepprofiler_cells.csv`;
- `features/tables/deepprofiler_quality.csv`.

For extraction without feature export, required artifacts should be stage-specific
and conservative: verify the plate output directory and at least one feature
artifact.

For segmentation-only runs, verify the plate output directory and at least one mask
or segmentation artifact.

For illumination-only runs, verify the plate output directory and at least one
corrected-image or illumination artifact.

The first implementation can start with DeepProfiler feature-export verification,
because Phase 2.9 established that as the current full-plate route. Other stage
profiles should fail closed with a clear message if verification rules are not yet
implemented and the policy is `verified`.

## Cleanup Implementation

Prefer a supported Nextflow cleanup command only when compatible with the installed
Nextflow version. Eddie evidence shows Nextflow 25.10.2 reports:

```text
Unknown option: -work-dir
```

for the current `nextflow clean -f -work-dir <batch_work_dir>` attempt.

Therefore cleanup should use this order:

1. Detect whether the installed Nextflow supports cleaning a specific work
   directory.
2. If supported, run the compatible Nextflow cleanup command.
3. If unsupported or unsuccessful, use guarded direct deletion.

Guarded direct deletion may only delete a resolved path that:

- is inside `<output_dir>/work`;
- has exactly one relative path component below that work root;
- matches `batch_###`;
- is the current batch work directory.

The existing `_validate_batch_cleanup_target()` path guard in `cptools2.__main__`
is the right basis for this behavior.

## Scratch Accounting

cptools2 should record scratch state at three points:

1. before launching a batch;
2. after stage-out verification;
3. after cleanup.

Before launching each batch, cptools2 should re-check scratch availability. If the
next batch no longer fits the configured scratch model, the command should stop
before submission and report the required and available space.

This prevents a long multi-batch run from assuming that scratch availability stayed
unchanged after the original preflight.

## Resume Behavior

The ledger should drive resume decisions:

- `success + verified + cleaned`: skip unless the user explicitly requests rerun;
- `success + verified + kept`: eligible for cleanup-only resume;
- `success + unverified`: re-run verification before cleanup;
- `failed`: keep work and require `--resume` or manual intervention;
- `cleanup failed`: do not launch the next batch until cleanup is resolved or the
  user explicitly chooses `--cleanup-policy keep`.

Nextflow `-resume` remains per-batch through the batch work directory. Once a batch
work directory is cleaned, that batch should be treated as completed and not
resumable from task cache. Durable outputs and the ledger become the resume record.

## Non-Goals

- Do not clean failed batch work automatically.
- Do not delete final plate outputs from `params.output_dir` or
  `params.data_destination`.
- Do not rely on `publishDir mode: 'move'` for this phase.
- Do not change the user-facing plate output layout to include batch directories.
- Do not make DINO-specific batching changes in this cleanup phase.

## Acceptance Criteria

- Default multi-batch execution uses verified cleanup after successful stage-out.
- `--cleanup-policy keep` preserves batch work for debugging.
- Existing `--clean-work` maps to verified cleanup during the transition.
- Cleanup never runs before parsing successful `STAGE_OUT` verification artifacts.
- Cleanup never runs after a failed batch.
- Cleanup target validation rejects paths outside the exact batch work directory.
- A batch status ledger records stage-out and cleanup state.
- Scratch availability is re-checked before each batch.
- `STAGE_OUT` emits rsync logs and a machine-readable verification summary.
- Verified cleanup requires rsync success and presence of required destination
  artifacts.
- Tests cover:
  - successful verified cleanup;
  - failed batch does not clean;
  - unverified stage-out does not clean;
  - rsync log and verification evidence generation;
  - missing required destination artifact blocks cleanup;
  - cleanup target path safety;
  - `--clean-work` compatibility alias;
  - next-batch scratch re-check failure;
  - ledger rows for cleaned, kept, failed, and unverified batches.
