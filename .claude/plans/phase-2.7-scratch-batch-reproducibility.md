---
phase: 2.7
name: Scratch Batch Reproducibility Hardening
status: active
source_plan: ../../docs/plans/seqera-eddie-scratch-batching-plan.md
loops_total: 4
---

# Phase 2.7: Scratch Batch Reproducibility Hardening

## Objective

Make `cptools2 pipeline` run scratch-safe Eddie batches as reproducible units without changing final output layout.

## Decisions Locked In

- cptools2 remains the wrapper; do not add an external `run_batches.sh`.
- Outputs stay flat and plate-centric under the configured `params.output_dir`.
- Batch isolation is limited to `work/batch_###`, `params.batch_###.json`, and `traces/*batch_###*`.
- Oversized single-plate handling is deferred by user decision because it is extremely unlikely for this workload.
- Guarded post-success deletion is in scope because staged image files must be removed from scratch.
- Command construction must be extracted and tested before adding more Nextflow flags.
- Scratch sizing must live in the batch API, not only in CLI glue.

## Phase Outputs

| Output | Location | Purpose |
|--------|----------|---------|
| Batch-isolated command builder | `cptools2/__main__.py` | Testable Nextflow invocation per batch |
| Configurable scratch model | `cptools2/batch.py`, parser/CLI config | Calibrated Eddie scratch planning |
| Guarded cleanup | `cptools2/__main__.py` | Reclaim scratch after successful batches |
| Tests | `tests/test_batch.py`, `tests/test_cli.py` | Prevent regressions in batch behavior |
| Eddie calibration notes | docs or plan update | Record measured scratch factor and concurrency tuning |

## Success Criteria

- `params.output_dir` remains unchanged across all batch params.
- Each batch uses `work/batch_###`.
- Each batch emits trace/report/timeline files under `traces/`.
- Dry-run shows batch work dirs, unchanged output dir, trace paths, and scratch model values.
- Scratch sizing defaults preserve current behavior and config overrides are tested.
- Cleanup only runs after successful batches and cannot delete outside the exact batch work dir.
- Local tests pass for the changed areas.
- Eddie dry-run confirms paths before real execution.

## Phase Loops

| Loop | Name | Status | Purpose |
|------|------|--------|---------|
| 350 | Batch Command Isolation and Provenance | pending | Add batch work dirs, trace/report/timeline, batch metadata, and tests |
| 360 | Scratch Sizing Configuration | pending | Make utilisation/work factor configurable through batch API and config |
| 370 | Guarded Post-Success Cleanup | pending | Add safe cleanup after successful batches |
| 380 | Eddie Calibration and Documentation | pending | Validate on Eddie, tune from evidence, document layout |

## Out of Scope

- Batched or nested output directories.
- `publishDir mode: 'move'`.
- Node-local `process.scratch = '$TMPDIR'`.
- Rewriting the Nextflow graph.
- External shell wrapper orchestration.
