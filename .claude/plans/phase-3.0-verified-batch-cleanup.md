---
phase: 3.0
name: Verified Batch Cleanup and Scratch Relief
status: active
source_specs:
  - ../../docs/superpowers/specs/2026-05-27-verified-batch-cleanup-design.md
loops_total: 3
---

# Phase 3.0: Verified Batch Cleanup and Scratch Relief

## Objective

Turn batch cleanup into a verified, default batch lifecycle step so completed
Nextflow batches are removed from scratch only after rsync-backed stage-out
evidence proves user-facing outputs are available.

## Scope

### Included

- Stage-out rsync evidence artifacts.
- Machine-readable `STAGE_OUT` verification JSON.
- Stage-aware destination artifact checks for the current DeepProfiler export
  route.
- CLI cleanup policy: `keep`, `success`, and `verified`.
- Compatibility mapping from existing `--clean-work` to verified cleanup.
- Batch status ledger with cleanup, stage-out, scratch, trace, and attempt state.
- Per-batch scratch availability re-check before each Nextflow launch.
- Local unit/static coverage for the cleanup lifecycle.

### Explicitly Not Included

- DINO feature extraction or DINO-specific batching.
- Full source/destination manifest comparison after rsync.
- Checksum verification as a default stage-out requirement.
- Changing the flat plate output layout.
- Cleaning failed or unverified batch work.
- Switching final outputs to `publishDir mode: 'move'`.

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Stage-out rsync evidence contract | Nextflow module update | `nextflow/modules/stage_out.nf` |
| Batch cleanup policy and ledger | Python CLI implementation | `cptools2/__main__.py` or focused helper module |
| Scratch pre-launch re-check | Python CLI implementation | `cptools2/__main__.py` / `cptools2/batch.py` |
| Regression coverage | pytest/static tests | `tests/test_cli.py`, `tests/test_nextflow_architecture_smoke.py`, new focused tests if needed |
| Operator documentation | Markdown | `docs/reference/nextflow-config-yaml.md`, `TODOs.md` |

## Success Criteria

- `STAGE_OUT` writes `stage_out_evidence/rsync.log` and
  `stage_out_evidence/verification.json`.
- `verification.json` includes plate/artifact id, destination, rsync exit code,
  rsync log path, required artifact list, required artifact presence, and
  `verification_status`.
- Verified cleanup requires all required stage-out evidence artifacts in the
  batch to report `verification_status == "verified"`.
- Default cleanup policy for normal `pipeline` runs is `verified`.
- `--cleanup-policy keep` preserves batch work for debugging.
- Existing `--clean-work` maps to `--cleanup-policy verified` during transition.
- Failed batches are not cleaned.
- Successful but unverified batches are not cleaned and stop the next batch.
- Cleanup target validation still rejects anything outside the exact
  `<output_dir>/work/batch_###` directory.
- `batch_status.csv` records nextflow status, stage-out status, cleanup policy,
  cleanup status, work dir, scratch before/after, trace path, params path,
  attempt, timestamps, and message.
- Scratch availability is re-checked before every batch launch; a batch that no
  longer fits stops before submission.
- Focused local tests pass.

## Dependencies

### Must Complete Before

- Phase 2.9: Feature export tables and clean full-plate route must be passed, so
  the current DeepProfiler output contract is known.

### Blocked By

- None for local implementation and static verification.

### Optional

- Eddie smoke validation can follow after local implementation. It is useful for
  proving real DataStore destination visibility but is not required for the first
  code landing.

## Skills Required

- `nextflow-development`: update `STAGE_OUT` safely and preserve output contract.
- `test-driven-development`: pin cleanup and verification behavior before
  implementation.
- `eddie-validate`: later Eddie smoke validation of real stage-out evidence.
- `document-release`: update operator-facing cleanup policy docs.

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---:|---:|---|
| Cleanup deletes useful failed evidence | Low | High | Never clean failed or unverified batches; keep strict path guard |
| Verification trusts wrong destination | Medium | High | Build destination from the same `data_destination ?: output_dir` rule as `STAGE_OUT`; test path construction |
| Stage-out evidence is not published from the task | Medium | Medium | Make `STAGE_OUT` emit evidence paths and test module text |
| Default cleanup surprises diagnostic runs | Medium | Medium | Add `--cleanup-policy keep` and config/env override support |
| Eddie `nextflow clean -work-dir` remains unsupported | High | Low | Detect or skip unsupported `nextflow clean`; guarded direct deletion remains supported |
| Multi-stage runs need different artifact checks | Medium | Medium | Implement DeepProfiler export first; fail closed for unsupported verified profiles |

## Assumptions

- `rsync` exit code `0` is the transfer authority for the requested stage-out
  copy.
- Destination artifact presence is the appropriate output suitability check for
  cleanup; full manifest comparison is intentionally out of scope.
- The Python wrapper owns batch lifecycle and cleanup; Nextflow owns per-batch
  dataflow and stage-out.
- A cleaned batch should be treated as completed from durable outputs and ledger,
  not resumable from task cache.

## Notes / Design Decisions

- Keep one Nextflow run per batch.
- Keep final outputs plate-centric and flat.
- Keep trace/report/timeline outside `work/batch_###` so cleanup preserves
  provenance.
- Prefer explicit cleanup policy over a boolean flag, while retaining
  `--clean-work` compatibility.
- DINO work remains deferred until shared batching architecture is stable.

## Ralph Loops

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 530 | Stage-Out Evidence Contract | Implementation | rsync log + verification JSON emitted by `STAGE_OUT`; static tests |
| 540 | Verified Cleanup Lifecycle | Implementation | cleanup policy, batch ledger, stage-out evidence parsing, scratch re-check |
| 550 | Documentation and Gate Smoke | Validation/Docs | docs, TODO state, focused tests, optional Eddie smoke instructions |

## Eddie Smoke Candidate

After local verification, run a small DataStore-backed multi-batch smoke in a
fresh scratch directory with:

- `stage_data: true`
- `feature_export.enabled: true`
- a bounded representative plate or subset
- `--cleanup-policy verified`
- enough plate/chunk splitting to create at least two `work/batch_###`
  directories

Acceptance evidence:

- `batch_status.csv` has one row per batch with `nextflow_status=success`,
  `stage_out_status=verified`, and `cleanup_status=cleaned`.
- Each cleaned batch directory is absent from `<output_dir>/work`.
- Trace, report, timeline, params, exported CSV tables, feature export status,
  and stage-out evidence remain outside the deleted batch work directory.
- A deliberately preserved diagnostic rerun with `--cleanup-policy keep` keeps
  the same batch work path and records `cleanup_status=kept`.
