# Phase 3.1 Ralph Loops: Production Execution Hardening and Durable Stage-Out Evidence

This file decomposes Phase 3.1 into executable Ralph loops. The active phase
plan is `.claude/plans/phase-3.1-durable-stageout-evidence.md`; the detailed
implementation plan is
`docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md`.

---
name: "ralph-loop-560"
task_name: "Durable Evidence Contract"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Added CLI and config-level Nextflow diagnostics modes, wired batch params booleans and observer gating, accepted the config key in parse_yaml, and verified the mode matrix with direct behavior checks plus focused pytest for parser and static config coverage."
  failed: ""
  needed: ""

todos:
  - id: "loop-560-1"
    content: "Verify the existing durable stage-out evidence implementation still matches the Phase 3.1 contract"
    skill: "nextflow-development"
    agent: "gpt-5.4-mini"
    outcome: "nextflow/modules/stage_out.nf writes work-local and durable stage_out_evidence, and cptools2/__main__.py prefers durable verification paths"
    status: completed
    priority: high
  - id: "loop-560-2"
    content: "Verify regression coverage for durable evidence lookup and ledger messages"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "tests/test_cli.py and tests/test_nextflow_architecture_smoke.py contain assertions for durable evidence lookup and stage-out evidence publication"
    status: completed
    priority: high
  - id: "loop-560-3"
    content: "Keep TODOs and phase plan marked complete for durable evidence only after evidence paths are confirmed"
    skill: "document-release"
    agent: "gpt-5.4-mini"
    outcome: "TODOs.md Loop 560 remains checked only for durable evidence tasks that are already implemented and verified"
    status: completed
    priority: medium

prompt: |
  ## Context from prior loop
  Done: Phase 3.0 verified cleanup substrate is present.
  Failed:
  Needed: Start with Loop 570 because Loop 560 is already complete.

  ## Objective
  Confirm the durable evidence contract remains intact before modifying production execution behavior.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-560"

  ## Success criteria
  - [ ] Durable evidence paths are present and tested.
  - [ ] No phase tracker marks unverified work complete.

  ## Required skills
  - `nextflow-development`: preserve process output contracts.
  - `test-driven-development`: verify coverage.

  ## Inputs
  - `.claude/plans/phase-3.1-durable-stageout-evidence.md`
  - `nextflow/modules/stage_out.nf`
  - `cptools2/__main__.py`
  - `tests/test_cli.py`

  ## Expected outputs
  - No code change unless verification finds drift.
  - Updated handoff_summary only if this loop is re-run.

  ## Constraints
  - Do not remove work-local stage_out_evidence because Nextflow process outputs remain part of the contract.

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-560 — verify durable evidence contract"
  2. Update handoff_summary in frontmatter.
  3. Mark all todos completed.

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Loop 560 Overview

Loop 560 is already complete. It remains in the loop file so later execution has
the full Phase 3.1 sequence and can resume from the first unfinished loop.

## Loop 560 Success Criteria

- ✓ `STAGE_OUT` writes durable evidence under `<output_dir>/stage_out_evidence/<batch_name>/<plate_id>/`: verified by existing implementation and tests.
- ✓ Cleanup gate prefers durable evidence before work-local evidence: verified by existing CLI helper behavior.

---
name: "ralph-loop-570"
task_name: "Multi-Batch Smoke Preparation"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Documented the three-plate forced-batching pre-acceptance recipe, measured sizes, expected batch split, tmux/minimal-diagnostics evidence checks, and the Loop 580/590 prerequisite for real execution."
  failed: ""
  needed: "Loop 580 diagnostics gating and Loop 590 launcher hardening before any real three-plate acceptance run."

todos:
  - id: "loop-570-1"
    content: "Confirm the forced-batching dry-run evidence and record the exact three-plate batching recipe"
    skill: "eddie-validate"
    agent: "gpt-5.4-mini"
    outcome: "TODOs.md and docs/reference/nextflow-config-yaml.md document the three real plates, measured plate sizes, scratch_utilisation_fraction forcing policy, and expected batch_001 to batch_003 split"
    status: completed
    priority: high
  - id: "loop-570-2"
    content: "Separate completed dry-run preparation from the blocked real tmux run"
    skill: "document-release"
    agent: "gpt-5.4-mini"
    outcome: "Phase 3.1 docs state that dry-run batching passed, while real execution waits on Loop 580 and Loop 590 hardening"
    status: completed
    priority: high
  - id: "loop-570-3"
    content: "Add a pre-acceptance checklist for three-plate execution"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini"
    outcome: "docs/reference/nextflow-config-yaml.md contains a checklist covering fresh run root, no qsub driver, tmux session, diagnostics mode, qstat snapshot, and durable evidence inspection"
    status: completed
    priority: medium

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Convert the existing dry-run batching evidence into operator documentation and acceptance prerequisites.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-570"

  ## Success criteria
  - [ ] Dry-run preparation is documented as passed, not confused with a completed real workflow.
  - [ ] The three-plate acceptance recipe is copyable and avoids site-specific excess detail in tracked docs.
  - [ ] The next loop starts from a clear driver-hardening prerequisite list.

  ## Required skills
  - `eddie-validate`: validate Eddie execution assumptions.
  - `document-release`: update operator documentation.

  ## Inputs
  - `.claude/plans/phase-3.1-durable-stageout-evidence.md`
  - `TODOs.md`
  - `docs/reference/nextflow-config-yaml.md`
  - Scratch evidence summarized in the current thread.

  ## Expected outputs
  - Updated `docs/reference/nextflow-config-yaml.md`
  - Updated `TODOs.md`
  - Updated handoff_summary for Loop 570

  ## Constraints
  - Do not start a real Eddie run in this loop.
  - Keep exact scratch paths and job IDs out of tracked docs unless they are already intentionally documented.

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-570 — document multi-batch smoke preparation"
  2. Update handoff_summary in frontmatter.
  3. Mark all todos completed.

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Loop 570 Overview

Loop 570 closes the gap between the dry-run batching evidence and the real
acceptance run. It documents what already passed and what must wait until driver
diagnostics and launcher hardening are implemented.

## Loop 570 Success Criteria

- ✓ Operator docs include a forced-batching recipe with measured plate sizes and expected batches.
- ✓ TODOs distinguish dry-run passed from real workflow pending.
- ✓ No real Eddie run is launched before Loop 580 and Loop 590.

---
name: "ralph-loop-580"
task_name: "Driver Diagnostics and Isolation"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Loop 580 diagnostics and cleanup-policy repairs are implemented. `--nextflow-diagnostics full|minimal|off` resolves through shared helper code, params include enable_trace/enable_report/enable_timeline, Nextflow observers are param-gated, nested work-local stage_out_evidence fallback is fixed, and legacy --clean-work maps to success cleanup while default cleanup remains verified."
  failed: "A final subagent review could not complete because the subagent usage limit was reached."
  needed: "Proceed to Loop 590 production launcher, continuation, and reporting."

todos:
  - id: "loop-580-1"
    content: "Add failing tests for full, minimal, and off Nextflow diagnostics modes"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "tests/test_cli.py contains command-builder tests for all diagnostics modes and tests/test_nextflow_architecture_smoke.py contains a config observer gating test"
    status: completed
    priority: high
  - id: "loop-580-2"
    content: "Implement diagnostics mode resolution and batch params observer flags"
    skill: "nextflow-development"
    agent: "gpt-5.4-mini"
    outcome: "cptools2/__main__.py supports --nextflow-diagnostics and writes enable_trace, enable_report, and enable_timeline into each params.batch_*.json"
    status: completed
    priority: high
  - id: "loop-580-3"
    content: "Gate config-level Nextflow trace, report, and timeline observers from params"
    skill: "nextflow-development"
    agent: "gpt-5.4-mini"
    outcome: "nextflow/nextflow.config sets trace/report/timeline enabled values from params instead of unconditional true"
    status: completed
    priority: high
  - id: "loop-580-4"
    content: "Run focused diagnostics tests"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "Focused pytest command for diagnostics modes and Nextflow observer gating passes locally"
    status: completed
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Make Nextflow diagnostics modes real by controlling both CLI flags and config-level observer startup.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-580"

  ## Success criteria
  - [x] `--nextflow-diagnostics full|minimal|off` is accepted by `cptools2 pipeline`.
  - [x] Batch params include `enable_trace`, `enable_report`, and `enable_timeline`.
  - [x] `nextflow/nextflow.config` gates trace/report/timeline observers from params.
  - [x] Focused pytest coverage passes.

  ## Required skills
  - `test-driven-development`: add failing tests before implementation.
  - `nextflow-development`: preserve Nextflow config behavior while gating observers.

  ## Inputs
  - `docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md`, Task 1
  - `cptools2/__main__.py`
  - `nextflow/nextflow.config`
  - `tests/test_cli.py`
  - `tests/test_nextflow_architecture_smoke.py`

  ## Expected outputs
  - Updated CLI diagnostics code and tests.
  - Updated Nextflow observer config.
  - Handoff summary stating exact tests run and result.

  ## Constraints
  - Do not launch Eddie jobs in this loop.
  - Preserve default behavior as `full` diagnostics unless config or CLI says otherwise.

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-580 — add Nextflow diagnostics modes"
  2. Update handoff_summary in frontmatter.
  3. Mark all todos completed.

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Loop 580 Overview

Loop 580 fixes the actual driver startup risk found in review: removing CLI
`-with-*` flags is not enough while `nextflow.config` enables observers by
default.

## Loop 580 Success Criteria

- ✓ Diagnostics modes exist at CLI/config level and are written into batch params.
- ✓ Nextflow config observers are parameter-gated.
- ✓ Focused tests pass.
- ✓ Nested work-local stage-out evidence fallback and legacy `--clean-work`
  compatibility repairs pass focused tests.

---
name: "ralph-loop-590"
task_name: "Production Launcher and Reporting"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Loop 590 is implemented inline. `--continue-on-batch-failure` records Nextflow failures, unverified stage-out, and cleanup failures while allowing later batches to run; `run_report.md` is written before failure exits and after completion; `scripts/eddie_phase31_launch.sh` starts the driver in tmux with run-local Eddie bootstrap state and preflight evidence; operator docs describe the tmux/no-qsub contract."
  failed: "Local bash syntax validation was skipped because Windows resolved bash to the WSL shim, which cannot access the sandbox path. Static launcher contract tests passed; run `bash -n scripts/eddie_phase31_launch.sh` on Eddie or Git Bash before Loop 600 launch."
  needed: "Run the Eddie-side launcher syntax/preflight check, then proceed to Loop 600 three-plate production acceptance."

todos:
  - id: "loop-590-1"
    content: "Implement central batch problem recording and continuation behavior"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "cptools2/__main__.py records Nextflow failure, unverified stage-out, and cleanup failure through a shared path and continues later batches when --continue-on-batch-failure is set"
    status: completed
    priority: high
  - id: "loop-590-2"
    content: "Add final run report generation"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "cptools2 pipeline writes run_report.md summarizing batch_status.csv, failures, cleanup status, and stage_out_evidence path"
    status: completed
    priority: high
  - id: "loop-590-3"
    content: "Add Eddie tmux launcher with bootstrap and preflight evidence"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini"
    outcome: "scripts/eddie_phase31_launch.sh exists, sources config/eddie_env.sh, sets run-local NXF_HOME, writes driver_env/qstat/process/status evidence, and starts tmux from a fresh run root"
    status: completed
    priority: high
  - id: "loop-590-4"
    content: "Document production launch contract"
    skill: "document-release"
    agent: "gpt-5.4-mini"
    outcome: "docs/reference/nextflow-config-yaml.md explains tmux launch, no qsub driver, minimal diagnostics, run report, continuation behavior, and evidence paths"
    status: completed
    priority: medium
  - id: "loop-590-5"
    content: "Run focused launcher, continuation, and report tests"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "Focused pytest commands for continuation, run_report, and launcher static tests pass locally; bash -n is deferred to Eddie/Git Bash because the local Windows bash shim cannot access the sandbox path"
    status: completed
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Build the production launch and reporting contract that lets multi-batch runs fail visibly without hiding successful later batches.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-590"

  ## Success criteria
  - [x] `--continue-on-batch-failure` handles Nextflow failure, unverified stage-out, and cleanup failure.
  - [x] `run_report.md` is written on normal completion and before final failure exits.
  - [x] Eddie launcher uses project bootstrap, run-local Nextflow state, tmux, and preflight evidence.
  - [x] Focused tests pass; launcher syntax check is deferred to Eddie/Git Bash because local bash is an unusable WSL shim.

  ## Required skills
  - `test-driven-development`: drive CLI behavior through tests.
  - `eddie-orchestrate`: validate Eddie launcher assumptions.
  - `document-release`: document operator contract.

  ## Inputs
  - `docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md`, Tasks 2 to 4
  - `cptools2/__main__.py`
  - `docs/reference/nextflow-config-yaml.md`
  - `tests/test_cli.py`

  ## Expected outputs
  - Updated CLI continuation and report behavior.
  - New `scripts/eddie_phase31_launch.sh`.
  - New `tests/test_eddie_phase31_launcher.py`.
  - Updated operator docs.

  ## Constraints
  - Do not submit any qsub jobs.
  - Do not start the real three-plate run in this loop.
  - Preserve failed batch work directories.

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-590 — add production launcher and reporting"
  2. Update handoff_summary in frontmatter.
  3. Mark all todos completed.

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Loop 590 Overview

Loop 590 provides the runtime shell around diagnostics: multi-batch continuation,
user-readable reports, and a launcher that actually reflects how Eddie must run
the Nextflow driver.

## Loop 590 Success Criteria

- ✓ Continuation handles batch-scoped failures without deleting failed work.
- ✓ The run report makes pass/fail state visible to users.
- ✓ Launcher is static-tested; shell syntax validation is queued for Eddie before
  the real Loop 600 launch.

---
name: "ralph-loop-600"
task_name: "Three-Plate Production Acceptance"
max_iterations: 4
on_max_iterations: checkpoint

handoff_summary:
  done: ""
  failed: ""
  needed: ""

todos:
  - id: "loop-600-1"
    content: "Sync Phase 3.1 launcher and diagnostics changes to Eddie permanent mirror without disturbing unrelated remote state"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini"
    outcome: "Eddie permanent mirror contains the local Phase 3.1 code/docs needed for diagnostics modes and launcher execution"
    status: pending
    priority: high
  - id: "loop-600-2"
    content: "Run a fresh dry-run acceptance check in the new scratch run root"
    skill: "eddie-validate"
    agent: "gpt-5.4-mini"
    outcome: "Fresh Eddie dry-run reports three batches for the selected real plates with minimal diagnostics params"
    status: pending
    priority: high
  - id: "loop-600-3"
    content: "Launch the three-plate workflow via tmux using minimal diagnostics"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini"
    outcome: "tmux driver starts from the fresh scratch run root and writes driver_status.json, launcher.log, qstat snapshots, and process snapshots"
    status: pending
    priority: high
  - id: "loop-600-4"
    content: "Monitor qstat, launcher logs, batch_status.csv, run_report.md, and durable stage-out evidence through completion or failure"
    skill: "eddie-validate"
    agent: "gpt-5.4-mini"
    outcome: "Acceptance evidence classifies the run as pass or failure with exact intervention needed, without rerunning completed work unnecessarily"
    status: pending
    priority: high
  - id: "loop-600-5"
    content: "Record final acceptance outcome and decide whether full diagnostics can be retested"
    skill: "document-release"
    agent: "gpt-5.4-mini"
    outcome: "TODOs.md and docs/reference/nextflow-config-yaml.md record pass/fail, evidence locations, cleanup state, and whether full diagnostics is safe to test next"
    status: pending
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Validate the production execution model with a fresh three-plate Eddie run using batching, stage-out evidence, verified cleanup, and minimal diagnostics.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-600"

  ## Success criteria
  - [ ] Fresh dry-run proves three batches.
  - [ ] tmux launcher starts from scratch and not qsub.
  - [ ] Completed batches have durable stage-out evidence and cleaned work dirs.
  - [ ] Failed batches preserve work and report clearly if the run does not pass.
  - [ ] Documentation records evidence and next action.

  ## Required skills
  - `eddie-orchestrate`: run and monitor Eddie workflow.
  - `eddie-validate`: inspect qstat, logs, trace, evidence, and cleanup state.
  - `document-release`: record operator outcome.

  ## Inputs
  - Completed Loop 580 and Loop 590 changes.
  - `scripts/eddie_phase31_launch.sh`
  - Fresh Eddie scratch root under `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/`.

  ## Expected outputs
  - Eddie run evidence in scratch.
  - Updated `TODOs.md` and operator docs.
  - Handoff summary with pass/fail and next intervention.

  ## Constraints
  - Do not submit the Nextflow driver with qsub.
  - Keep exact job IDs and path-heavy evidence in scratch unless needed for concise tracked documentation.
  - Do not clean failed batch work.

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-600 — record three-plate production acceptance"
  2. Update handoff_summary in frontmatter.
  3. Mark all todos completed.

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Loop 600 Overview

Loop 600 is the first real production validation. It should only start after the
driver diagnostics and launcher/reporting loops pass locally.

## Loop 600 Success Criteria

- ✓ Fresh dry-run proves the three-plate batch split.
- ✓ The workflow launches via tmux from scratch.
- ✓ User-facing evidence states pass or failure clearly.
- ✓ Verified completed batches are cleaned, failed batches are preserved.

---
name: "ralph-loop-610"
task_name: "Acceptance Recovery and Conservative GPU Baseline"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "First Loop 600 acceptance launched and produced useful failure evidence: continuation ran all three batches, run_report.md was written, failed work was preserved, Cellpose showed GPU OOM under concurrent GPU load, and STAGE_OUT exposed a deterministic heredoc newline bug after rsync. The conservative Loop 610 rerun also produced useful evidence: all three Cellpose tasks landed on one saturated H200 GPU 0 with only ~5-8 MiB free while the Cellpose process itself used ~1.2-1.3 GiB. The later fresh DeepProfiler run proved the fresh-root batching route, durable stage-out evidence, and continuation/reporting behavior; batch 2 completed DeepProfiler EXPORT_FEATURES and plate-level STAGE_OUT before the summary task failed. On 2026-06-09, DeepProfiler production development was explicitly paused in favor of the DINO successor route; see docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md."
  failed: "The DeepProfiler conservative baseline never reached verified cleanup. The fresh run exposed DeepProfiler-specific fragility: serialized feature extraction remained necessary, SUMMARISE_FEATURE_EXPORTS failed after successful stage-out because feature_export_status.csv was resolved incorrectly, and retained failed work contributed to scratch quota pressure before batch 3 stage-in. These are no longer active Phase 3.1 blockers for DeepProfiler because the route is paused."
  needed: "Carry the Phase 3.1 batching lessons into the DINO workstream: fresh scratch roots, tmux driver, per-batch params/traces/work, durable stage-out evidence, verified cleanup, quota snapshots, and clear run reports. Do not continue DeepProfiler acceptance loops unless explicitly revived for reproducibility or regression support."

todos:
  - id: "loop-610-1"
    content: "Fix the stage-out verification newline bug and add a static regression test"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini"
    outcome: "Stage-out verification JSON generation is Groovy-safe and static tests pass"
    status: completed
    priority: high
  - id: "loop-610-2"
    content: "Add launcher GPU max-forks overrides for conservative acceptance"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini"
    outcome: "Launcher can set GPU, Cellpose, and DeepProfiler maxForks per run and records effective values in status.txt"
    status: completed
    priority: high
  - id: "loop-610-3"
    content: "Run conservative three-plate acceptance from a fresh scratch root"
    skill: "eddie-validate"
    agent: "gpt-5.4-mini"
    outcome: "Acceptance evidence recorded failure clearly: all three batches failed in Cellpose on a saturated assigned H200 GPU 0, while continuation, run_report.md, and failed-work preservation worked"
    status: completed
    priority: high
  - id: "loop-610-4"
    content: "Apply shared production hardening discovered by comparing the failed DeepProfiler route with the successful adjacent DINO workstream"
    skill: "nextflow-development"
    agent: "gpt-5.4-mini"
    outcome: "STAGE_IN excludes ImageXpress thumbnails, STAGE_OUT namespaces per-chunk outputs and evidence, CELLPOSE_SEGMENT retries saturated assigned GPUs via exit 140, and feature_export requests 16 GB on Eddie"
    status: completed
    priority: high
  - id: "loop-610-5"
    content: "Rerun three-plate acceptance after shared hardening lands"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini"
    outcome: "Fresh DeepProfiler acceptance recorded useful pass/fail evidence, but active DeepProfiler production development is now paused; the equivalent acceptance should be rerun on the DINO successor route"
    status: completed
    priority: high

prompt: |
  ## Context from prior loop
  Done: First Loop 600 acceptance launched from tmux and proved continuation/reporting, but all batches failed. The deterministic STAGE_OUT heredoc newline bug and launcher fork controls were fixed. The conservative rerun proved the remaining Cellpose failure was assigned-GPU saturation, not ordinary Cellpose working-set size.
  Failed: Cellpose repeatedly landed on saturated H200 GPU 0 with only ~5-8 MiB free; no batch reached verified cleanup.
  Needed: Rerun after shared hardening from DINO evidence is synced to Eddie.

  ## Objective
  Recover Phase 3.1 acceptance by removing deterministic pipeline issues, adding shared production hardening from the DINO evidence, and rerunning the three-plate workflow with serialized GPU tasks.

  ## Success criteria
  - [x] Stage-out verification JSON generation is safe inside Nextflow/Groovy heredocs.
  - [x] Launcher supports per-run GPU max-forks overrides and records effective values.
  - [x] Conservative tmux run reports failures clearly.
  - [x] Shared hardening excludes thumbnails, namespaces stage-out, raises feature-export memory, and adds retryable Cellpose GPU preflight.
  - [ ] Fresh post-hardening dry-run proves the same three-batch split.
  - [ ] Post-hardening tmux run completes or reports failures clearly.
  - [ ] Verified completed batches have durable stage-out evidence and cleaned work dirs.

  ## Constraints
  - Do not submit the Nextflow driver with qsub.
  - Use `--gpu-max-forks 1 --segment-max-forks 1 --feature-max-forks 1` for the conservative acceptance baseline.
  - Do not begin GPU scale-up until the conservative baseline passes.
  - Do not merge DINO-specific Cell-DINO, Python illumination, or corrected-manifest changes as part of this loop.
---

## Loop 610 Overview

Loop 610 repairs deterministic acceptance failures from Loop 600, incorporates
shared production hardening discovered by comparing the failed DeepProfiler route
with the successful adjacent DINO workstream, and separates workflow correctness
from GPU throughput. It keeps Phase 3.1 focused on a conservative production
baseline before opening a broader GPU scaling matrix.

## Loop 610 Pause Decision

As of 2026-06-09, do not keep iterating on DeepProfiler production acceptance.
The fresh three-plate DeepProfiler run generated enough evidence to preserve the
batching lessons, but DeepProfiler-specific runtime and maintenance risk now
outweigh further investment. The DINO workstream supersedes DeepProfiler as the
active production feature-extraction route. Future acceptance work should reuse
the Phase 3.1 batching architecture on DINO rather than attempting another
DeepProfiler rerun.
