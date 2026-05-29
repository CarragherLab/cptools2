---
phase: 3.1
name: Production Execution Hardening and Durable Stage-Out Evidence
status: active
source_specs:
  - ../../docs/superpowers/specs/2026-05-27-verified-batch-cleanup-design.md
implementation_plans:
  - ../../docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md
loops_total: 5
---

# Phase 3.1: Production Execution Hardening and Durable Stage-Out Evidence

## Objective

Harden the production cptools2 execution model so Nextflow-led Eddie runs launch
from isolated scratch roots, preserve stage-out verification evidence outside
deleted batch work directories, clean verified batches, and report partial
failures clearly.

## Scope

### Included

- Durable `stage_out_evidence` copies under `<output_dir>`.
- Cleanup-gate preference for durable evidence paths.
- Static and CLI coverage for durable evidence behavior.
- Operator documentation for evidence location and batching smoke checks.
- Configurable Nextflow diagnostics modes for Eddie driver stability.
- tmux-based Eddie launcher contract with run-local `.nextflow` state.
- Batch continuation and final run reporting for multi-plate workflows.
- Three-plate real-data acceptance run as the first production validation.

### Explicitly Not Included

- Full source/destination manifest comparison.
- Changing the rsync authority model.
- Changing final plate result layout.
- Replacing Nextflow with native SGE scripts as the default execution route.
- DINO feature extraction work.

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Durable evidence copy | Nextflow module update | `nextflow/modules/stage_out.nf` |
| Durable evidence lookup | Python CLI helper | `cptools2/__main__.py` |
| Diagnostics modes | Python CLI helper | `cptools2/__main__.py` |
| Eddie tmux launcher | Shell script | `scripts/eddie_phase31_launch.sh` |
| Run report | Markdown output | `<output_dir>/run_report.md` |
| Regression coverage | pytest/static tests | `tests/test_cli.py`, `tests/test_nextflow_architecture_smoke.py` |
| Operator docs | Markdown | `docs/reference/nextflow-config-yaml.md`, `TODOs.md` |

## Success Criteria

- `STAGE_OUT` writes work-local `stage_out_evidence` for the Nextflow process
  contract.
- `STAGE_OUT` also copies evidence to
  `<output_dir>/stage_out_evidence/<batch_name>/<plate_id>/`.
- `verification.json` records `durable_evidence_dir`.
- The cleanup gate prefers durable evidence when present.
- `batch_status.csv` messages point at durable evidence after verified cleanup.
- `nextflow_diagnostics` supports `full`, `minimal`, and `off` modes.
- Diagnostics modes gate config-level Nextflow `trace`, `report`, and
  `timeline` observers, not only CLI `-with-*` flags.
- Eddie production runs launch from a fresh scratch root inside `tmux`, not as
  a qsub driver job.
- Eddie launcher sources the permanent project bootstrap and records Python,
  Nextflow, Singularity, scheduler, and process-state evidence.
- Independent later batches can finish after an earlier batch fails when the
  continuation flag is enabled, including Nextflow failure, unverified
  stage-out, and cleanup failure cases.
- `run_report.md` summarizes batch status, failures, cleanup, and evidence.
- The three-plate Eddie acceptance run proves batching, workflow execution,
  durable stage-out evidence, and verified cleanup.
- Focused static/parser/lint checks pass locally.

## Dependencies

### Must Complete Before

- Phase 3.0 verified cleanup substrate: durable evidence and launcher hardening
  depend on the existing cleanup policy, ledger, and stage-out verification
  contract.

### Blocked By

- None for local implementation.

### Optional

- Full diagnostics can be restored after the minimal-diagnostics three-plate
  acceptance run proves Eddie driver startup is stable.

## Skills Required

- `phase-plan-creator`: phase definition and success criteria.
- `nextflow-development`: preserve Nextflow process output contracts.
- `eddie-validate`: later runtime smoke design.

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---:|---:|---|
| Evidence is copied after JSON generation but before task failure | Low | Medium | Copy evidence before exiting on rsync failure so failures are visible |
| Durable evidence collides across batches | Low | High | Include `batch_name` and `plate_id` in the durable path |
| Cleanup reads stale evidence from a prior run | Medium | Medium | Use fresh output directories for smoke tests; ledger records attempt and params path |
| Missing `batch_name` in older params | Low | Low | Fall back to `batch_unknown` and surface path in ledger |
| Diagnostics mode only changes CLI flags while config observers remain enabled | Medium | High | Gate `trace`, `report`, and `timeline` from params and add static config tests |
| tmux launcher starts without Eddie project environment | Medium | High | Require project root, source `config/eddie_env.sh`, set `PYTHONPATH`, and record tool versions |
| One failed plate hides successful later plates | Medium | Medium | Centralize batch problem handling and allow continuation for independent failures |

## Assumptions

- `params.output_dir` is available for normal cptools2 pipeline runs.
- Durable evidence under `output_dir` is outside `work/batch_###` and therefore
  survives batch cleanup.
- Fresh scratch directories remain the correct route for certification smokes.

## Notes / Design Decisions

- Keep work-local evidence because Nextflow process outputs should remain
  explicit and inspectable before cleanup.
- Prefer durable evidence in the Python gate, but retain work-local fallback for
  compatibility with older in-flight runs.
- Do not add manifest comparison; rsync exit status plus required artifact
  presence remains the verification model.
- Minimal diagnostics must disable config-level Nextflow observers. Removing
  only CLI `-with-*` flags is insufficient because `nextflow.config` currently
  controls default observers.
- Keep Nextflow as the primary route. Native cptools2-style SGE scripts remain a
  fallback if driver hardening fails.

## Ralph Loops

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 560 | Durable Evidence Contract | Implementation | durable evidence copy and lookup |
| 570 | Multi-Batch Smoke Preparation | Validation/Docs | forced batching recipe and smoke acceptance criteria |
| 580 | Driver Diagnostics and Isolation | Implementation | diagnostics modes and run-local command behavior |
| 590 | Production Launcher and Reporting | Implementation/Docs | tmux launcher, preflight evidence, run report |
| 600 | Three-Plate Acceptance | Validation | real-data batching and verified cleanup evidence |
