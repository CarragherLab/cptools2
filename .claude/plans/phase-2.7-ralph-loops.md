---
phase: 2.7
name: Scratch Batch Reproducibility Hardening
plan: phase-2.7-scratch-batch-reproducibility.md
status: active
loops_total: 4
---

# Phase 2.7: Scratch Batch Reproducibility Hardening Ralph Loops

## Loop 350: Batch Command Isolation and Provenance

```yaml
---
name: "ralph-loop-350"
task_name: "Batch Command Isolation and Provenance"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Loop 350 implemented. Nextflow command construction is in _build_nextflow_command; batches are annotated with batch_### names, work/batch_### directories, trace/report/timeline paths, batch_name, and batch_work_dir. params.output_dir remains unchanged. Dry-run output shows work, output, trace, report, timeline, scratch estimate, and params paths. Focused CLI tests and tests/test_cli.py pass outside the sandbox."
  failed: ""
  needed: "Run pytest outside the Codex sandbox for tmp_path-heavy tests on this Windows workspace, or pytest may hang during temp cleanup/reporting due to ACL restrictions."

todos:
  - id: "loop-350-1"
    content: "Extract a testable Nextflow command builder from cmd_pipeline without changing current command behavior"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/__main__.py contains a helper that builds the Nextflow command list; existing pipeline invocation tests still pass"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-350-2"
    content: "Generate zero-padded batch_name values and use work/<batch_name> as the Nextflow -work-dir"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Tests assert batch 1 uses a work directory ending in work/batch_001 and batch params include batch_name and batch_work_dir"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-350-3"
    content: "Add per-batch trace, report, and timeline flags to the Nextflow command"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Tests assert -with-trace, -with-report, and -with-timeline point at traces/*batch_001* paths"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-350-4"
    content: "Preserve params.output_dir unchanged in every batch params file"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Tests assert params.batch_1.json and params.batch_2.json keep the same output_dir as params.json"
    status: completed
    complexity: low
    priority: high
  - id: "loop-350-5"
    content: "Improve dry-run output to show batch plates, estimated scratch, work dir, unchanged output dir, trace paths, and params path"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Dry-run test captures output containing work/batch_001, unchanged output_dir, traces/trace.batch_001.txt, and params.batch_1.json"
    status: completed
    complexity: medium
    priority: medium
  - id: "loop-350-6"
    content: "Run focused CLI tests for pipeline command and dry-run behavior"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Focused pytest invocation for relevant test_cli tests exits 0"
    status: completed
    complexity: low
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Implement batch-isolated Nextflow command construction while keeping final outputs flat.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-350"

  ## Success criteria
  - [ ] Nextflow command construction is in a testable helper
  - [ ] Each batch uses work/batch_###
  - [ ] Each batch emits trace/report/timeline paths
  - [ ] params.output_dir remains unchanged
  - [ ] Focused CLI tests pass

  ## Required skills
  - `test-driven-development`: write focused tests before or alongside behavior changes
  - `verification-before-completion`: verify the changed behavior before handoff

  ## Inputs
  - Source plan: docs/plans/seqera-eddie-scratch-batching-plan.md
  - CLI code: cptools2/__main__.py
  - Tests: tests/test_cli.py

  ## Expected outputs
  - Updated cptools2/__main__.py
  - Updated tests/test_cli.py

  ## Constraints
  - Do not change params.output_dir
  - Do not add batched output directories
  - Preserve existing single-batch behavior where possible

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-350 - batch command isolation"
  2. Update handoff_summary
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Overview
This loop makes batch execution observable and testable without changing where final results land.

## Success Criteria
- ✓ Command helper exists and is covered by focused tests.
- ✓ Batch 1 uses `work/batch_001`.
- ✓ Trace/report/timeline paths are included per batch.
- ✓ `params.output_dir` remains unchanged.

## Skills Required

### Broad
- `test-driven-development`: protect CLI behavior while changing command construction.

### Specific
- `verification-before-completion`: run focused tests and report any blocked verification.

### Discovered
- None.

## Dependencies
- Must complete before cleanup and Eddie calibration.

## Complexity
**Scope**: Medium
**Estimated effort**: 1 focused coding session
**Key challenges**:
1. Avoiding output layout churn.
2. Keeping command construction testable without over-abstracting.

## Loop 360: Scratch Sizing Configuration

```yaml
---
name: "ralph-loop-360"
task_name: "Scratch Sizing Configuration"
max_iterations: 3
on_max_iterations: escalate

  handoff_summary:
  done: "Loop 360 implemented configurable scratch sizing in batch.py and CLI wiring. create_batches now accepts utilisation_fraction and work_factor with defaults preserving current behavior. Config keys scratch_utilisation_fraction and scratch_work_factor are parsed and validated before batch creation, forwarded into scratch batch planning, and shown in dry-run output. Focused tests in tests/test_batch.py and tests/test_cli.py cover defaults, overrides, invalid values, and dry-run reporting."
  failed: ""
  needed: ""

todos:
  - id: "loop-360-1"
    content: "Add utilisation_fraction and work_factor parameters to create_batches with defaults preserving current 0.75 and 1.3 behavior"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_batch.py proves create_batches defaults match current grouping and accepts explicit sizing overrides"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-360-2"
    content: "Parse scratch_utilisation_fraction and scratch_work_factor from config and pass them into _build_scratch_batches"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_cli.py proves config overrides are passed to create_batches"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-360-3"
    content: "Validate scratch sizing values before batch creation"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Tests prove non-positive or nonsensical sizing values raise a clear ValueError"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-360-4"
    content: "Show scratch utilisation and work factor values in dry-run output"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Dry-run test captures both scratch_utilisation_fraction and scratch_work_factor values"
    status: completed
    complexity: low
    priority: medium
  - id: "loop-360-5"
    content: "Run focused batch and CLI tests for scratch sizing behavior"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Focused pytest invocation for relevant test_batch and test_cli tests exits 0"
    status: completed
    complexity: low
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Make the scratch sizing model configurable while preserving existing defaults.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-360"

  ## Success criteria
  - [ ] create_batches accepts sizing parameters with current defaults
  - [ ] config overrides reach batch creation
  - [ ] invalid sizing values fail clearly
  - [ ] dry-run reports sizing values
  - [ ] Focused tests pass

  ## Required skills
  - `test-driven-development`: protect batch grouping behavior
  - `verification-before-completion`: verify focused suites

  ## Inputs
  - cptools2/batch.py
  - cptools2/__main__.py
  - cptools2/parse_yaml.py
  - tests/test_batch.py
  - tests/test_cli.py

  ## Expected outputs
  - Configurable batch sizing API
  - Config-driven CLI batch sizing
  - Tests for defaults, overrides, and invalid config

  ## Constraints
  - Do not add oversized-plate blocking logic in this loop
  - Preserve default grouping behavior

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-360 - scratch sizing configuration"
  2. Update handoff_summary
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Overview
This loop turns the hard-coded scratch model into an explicit, testable API and config surface.

## Success Criteria
- ✓ Defaults preserve current behavior.
- ✓ Config overrides are tested.
- ✓ Bad values fail before Nextflow starts.

## Dependencies
- Depends on Loop 350 for dry-run structure.

## Complexity
**Scope**: Medium
**Estimated effort**: 1 focused coding session
**Key challenges**:
1. Avoiding behavioral drift in existing batch grouping.
2. Keeping validation strict without blocking the deferred oversized-plate case.

## Loop 370: Guarded Post-Success Cleanup

```yaml
---
name: "ralph-loop-370"
task_name: "Guarded Post-Success Cleanup"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Loop 370 added opt-in post-success cleanup for batch scratch. The pipeline parser now accepts --clean-work and defaults it to false. Successful batch runs attempt nextflow clean -f -work-dir <batch_work_dir> first and fall back only to guarded deletion of the exact validated batch work dir. Cleanup rejects targets outside the configured work root and paths without an exact batch_### component. Focused CLI tests now cover parser defaults, success/failure cleanup gating, path-safety rejection, and the direct-deletion fallback path."
  failed: ""
  needed: "Loop 380 can now handle Eddie calibration and documentation on top of the guarded cleanup path."

todos:
  - id: "loop-370-1"
    content: "Add --clean-work CLI flag defaulting to false"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Parser tests prove --clean-work is accepted and defaults to false"
    status: completed
    complexity: low
    priority: high
  - id: "loop-370-2"
    content: "Run cleanup only after a batch subprocess exits successfully"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "Tests prove cleanup is called after returncode 0 and not called after non-zero returncode"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-370-3"
    content: "Implement cleanup sequence using nextflow clean first and guarded direct deletion fallback for exact batch work dir only"
    skill: "careful"
    agent: "ralph-loop-worker"
    outcome: "Cleanup helper refuses targets outside work root or without expected batch_### component and only deletes validated batch work dirs"
    status: completed
    complexity: high
    priority: high
  - id: "loop-370-4"
    content: "Add path-safety tests for cleanup guard rejection cases"
    skill:
      - "test-driven-development"
      - "careful"
    agent: "ralph-loop-worker"
    outcome: "Tests cover outside-root, non-batch, failed-batch, and successful-batch cleanup paths"
    status: completed
    complexity: high
    priority: high
  - id: "loop-370-5"
    content: "Run focused cleanup and CLI tests"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Focused pytest invocation for cleanup-related tests exits 0"
    status: completed
    complexity: low
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Add opt-in cleanup that reclaims scratch after successful batches without risking broad deletion.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-370"

  ## Success criteria
  - [ ] --clean-work exists and defaults false
  - [ ] Cleanup only runs after successful batch returncode
  - [ ] nextflow clean is attempted first
  - [ ] Direct deletion fallback is guarded by resolved path, work root, and batch_### checks
  - [ ] Focused cleanup tests pass

  ## Required skills
  - `careful`: deletion guardrails
  - `test-driven-development`: cleanup behavior tests
  - `verification-before-completion`: focused test verification

  ## Inputs
  - cptools2/__main__.py
  - tests/test_cli.py

  ## Expected outputs
  - --clean-work implementation
  - Cleanup helper with strict path validation
  - Tests proving failed batches are never cleaned

  ## Constraints
  - Never delete on failed batches
  - Never delete outside exact batch work dir
  - Do not change output layout

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-370 - guarded cleanup"
  2. Update handoff_summary
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Overview
This loop adds opt-in scratch reclamation matching the SGE-native operational need while protecting shared filesystem paths.

## Success Criteria
- ✓ Cleanup is opt-in.
- ✓ Failed batches are never cleaned.
- ✓ Direct deletion fallback cannot escape the exact batch work directory.

## Dependencies
- Depends on Loop 350 for batch work dir naming.

## Complexity
**Scope**: High
**Estimated effort**: 1 focused coding session
**Key challenges**:
1. Deletion safety on Windows tests and Eddie paths.
2. Correctly distinguishing batch work dirs from broader work roots.

## Loop 380: Eddie Calibration and Documentation

```yaml
---
name: "ralph-loop-380"
task_name: "Eddie Calibration and Documentation"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Documented the final flat-output and batch-work-dir layout in the phase plan, operational plan, and TODOs."
  failed: "Local Eddie dry-run could not complete on this Windows workspace: `python -m cptools2 pipeline config/loop230-sarah-screen.yaml --dry-run` failed in `parse_yaml.generate_params_json` with `PermissionError: [WinError 5] Access is denied: 'C:\\exports'` before Nextflow started."
  needed: "Run the Loop 230 example-screen dry-run and a small Eddie batch on Eddie itself, then record trace/report/timeline, peak scratch usage, and measured scratch_work_factor/maxForks tuning."

todos:
  - id: "loop-380-1"
    content: "Document the final flat-output and batch-work-dir layout"
    skill: "document-release"
    agent: "ralph-loop-worker"
    outcome: "Plan or docs explain that outputs remain flat while work dirs and traces are batch-scoped"
    status: pending
    complexity: low
    priority: high
  - id: "loop-380-2"
    content: "Run or prepare Eddie dry-run for the Loop 230 example-screen config and inspect batch paths"
    skill: "eddie-orchestrate"
    agent: "ralph-loop-worker"
    outcome: "Dry-run output confirms work/batch_001, trace paths, unchanged output_dir, and expected params file"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-380-3"
    content: "Run a small Eddie batch when cluster access is available and record trace/report/timeline plus peak scratch usage"
    skill:
      - "eddie-login"
      - "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Trace/report/timeline artifacts and peak scratch measurement are recorded in the plan or Eddie notes"
    status: pending
    complexity: high
    priority: high
  - id: "loop-380-4"
    content: "Tune scratch factor and concurrency from observed Eddie data"
    skill: "eddie-resources"
    agent: "ralph-loop-worker"
    outcome: "Recommended scratch_work_factor and maxForks values are documented with measurement evidence"
    status: pending
    complexity: medium
    priority: medium
  - id: "loop-380-5"
    content: "Run final focused tests and summarize remaining Eddie-only follow-up"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Focused tests pass or blocked verification is documented with exact command and reason"
    status: pending
    complexity: low
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Validate the new batch execution model on Eddie and document the operational layout.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-380"

  ## Success criteria
  - [ ] Docs explain flat outputs and batch-scoped work/traces
  - [ ] Eddie dry-run confirms expected paths
  - [ ] Small Eddie batch evidence is recorded when available
  - [ ] Scratch factor and concurrency recommendations are evidence-based
  - [ ] Final focused tests pass or blockers are documented

  ## Required skills
  - `document-release`: update operational docs
  - `eddie-orchestrate`: Eddie execution planning
  - `eddie-resources`: tune concurrency and scratch assumptions
  - `verification-before-completion`: verify before handoff

  ## Inputs
  - config/loop230-sarah-screen.yaml
  - docs/plans/seqera-eddie-scratch-batching-plan.md
  - Updated CLI behavior from loops 350-370

  ## Expected outputs
  - Updated docs or plan notes
  - Eddie dry-run or execution evidence
  - Final recommendation for scratch factor and concurrency

  ## Constraints
  - Do not change output layout
  - If Eddie access is unavailable, document exact blocked commands and keep the loop checkpointed

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-380 - Eddie calibration docs"
  2. Update handoff_summary
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Overview
This loop turns the implementation into operational practice on Eddie and captures the evidence needed for future tuning.

## Success Criteria
- ✓ Layout is documented.
- ✓ Eddie dry-run or blocked command is recorded.
- ✓ Scratch and concurrency recommendations are evidence-based.

## Dependencies
- Depends on Loops 350, 360, and 370.

## Complexity
**Scope**: Medium to high depending on Eddie access
**Estimated effort**: 1 validation session
**Key challenges**:
1. Scheduler availability.
2. Separating local verification from Eddie-only evidence.
