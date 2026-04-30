---
phase: 2.6
name: Loop 230 Blocker Burn-Down
plan: phase-2.6-loop230-blocker-burndown.md
status: in_progress
loops_total: 6
recommended_agent_model: gpt-5.4-mini
---

# Phase 2.6 Ralph Loops: Loop 230 Blocker Burn-Down

## Execution Model

Use GPT-5.4-mini subagents for bounded loops. Each worker is not alone in the codebase: do not revert edits made by other workers, do not broaden scope, and adapt to existing changes.

The parent agent should coordinate integration, commits, and Eddie state. Eddie loops are sequential.

## Advanced Planning Architecture

### Loop State Machine

Every loop moves through:

```text
pending -> context_loaded -> in_progress -> verification -> handoff_ready -> completed
                                      \-> blocked
```

Rules:

- Only one todo may be `in_progress` inside a loop.
- A loop cannot enter `completed` until its verification commands or Eddie evidence are recorded.
- A blocked loop must write `handoff_summary.failed` and `handoff_summary.needed`.
- The parent agent, not the worker, decides whether to advance past a failed quality gate.

### Gate Map

| Gate | Unlocks | Required Prior Loop | Hard Checklist |
|------|---------|---------------------|----------------|
| Gate 1: Local baseline | Loop 300 edits and Loop 310 sync | 290 | Review-fix commit exists; focused tests pass; full pytest passes; `git diff --check` has no real errors. |
| Gate 2: Subset ready | Loop 310 dry-run and Loop 320 resume | 300 | Active `plates` are in params; `batch_id` is in params; subset/chunk limit is present or explicitly not needed for the next command. |
| Gate 3: Eddie params verified | Loop 320 resume | 310 | Eddie checkout matches local commit/files; dry-run exits 0; params are scratch-contained and include `data_destination`. |
| Gate 4: Chunk fan-out proven | Loop 330 AI smoke | 320 | COMPLETE 2026-04-30: `STAGE_IN` cached successfully with 23040 TIFFs, `BUILD_IMAGESET_INDEX` completed with 11520 image-set rows, and `CHUNK_IMAGESETS` completed with one 480-row bounded chunk. |
| Gate 5: AI smoke assessed | Loop 340 final report | 330 | One-chunk AI path succeeds, or scaffold/container/model blocker is converted to next-loop task. |

### Parallel Plan

| Loop | Parallel Status | Reason |
|------|-----------------|--------|
| 290 | Blocking | Owns the dirty patch and commit baseline. |
| 300 | Read-only parallel only | Can inspect subset/batch behaviour while 290 runs, but must not edit shared files until 290 lands. |
| 310 | Sequential | Writes to shared Eddie checkout and scratch params. |
| 320 | Sequential | Uses live scheduler state and Nextflow cache. |
| 330 | Sequential | Depends on chunk subset success and GPU availability. |
| 340 | Sequential | Summarizes all prior loop evidence. |

### Subagent Output Schema

Workers should return this exact shape in prose or YAML:

```yaml
loop_id:
status: completed | blocked | partial
changed_files:
commands_run:
evidence:
job_ids:
log_paths:
decisions:
blockers:
next_owner:
handoff_summary:
  done:
  failed:
  needed:
```

### Recovery Matrix

| Loop | Gate Failure | Worker Action | Parent Action |
|------|--------------|---------------|---------------|
| 290 | Tests fail | Stop, report exact failing tests and first failure traceback | Decide patch fix versus revert partial worker edits. |
| 300 | `max_chunks` missing | Add minimal support only if within owned files; otherwise report blocker | Decide whether to implement now or run full chunk generation. |
| 300 | Active batch fields missing | Patch params generation and tests before Eddie sync | Do not authorize Loop 310 until params are correct. |
| 310 | Eddie dry-run mismatch | Stop, save local and Eddie params paths, report diff | Resync or patch config locally. |
| 320 | Submit-host pressure persists | Stop, record host, `qstat`, command, Nextflow log, exact error | Pause retry until account pressure drops. |
| 330 | Cellpose scaffold only | Stop, identify exact placeholder lines and minimum real invocation | Create implementation loop before claiming AI path. |

### Parent Integration Checklist

- [ ] Confirm worker touched only owned files.
- [ ] Inspect `git diff --stat` before committing.
- [ ] Verify commands actually ran, not just planned.
- [ ] Preserve user/unrelated changes.
- [ ] Commit only after the gate evidence is acceptable.
- [ ] Update loop `handoff_summary` after each completed loop.

## Loop 290: Land Local Review Fixes

```yaml
loop_id: 290
name: Land Local Review Fixes
status: pending
type: implementation
model: gpt-5.4-mini
parallel_safe: false
depends_on: []
blocked_by:
  - "Focused or full local pytest failure"
  - "Dirty tree contains unrelated changes mixed into review-fix patch"
owned_files:
  - cptools2/__main__.py
  - cptools2/parse_yaml.py
  - nextflow/main.nf
  - nextflow/modules/cellpose_segmentation.nf
  - nextflow/modules/illum_calculate.nf
  - nextflow/modules/stage_out.nf
  - tests/test_cli.py
  - tests/test_nextflow_architecture_smoke.py
  - tests/test_parse_yaml.py
handoff_summary:
  done: ""
  failed: ""
  needed: ""
handoff_required_fields:
  evidence: "Focused/full pytest output and git diff-check result"
  commands: "Exact pytest and git commands run"
  job_ids: "none"
  log_paths: "pytest output path if captured, otherwise none"
  next_owner: "parent"
max_iterations: 3
on_max_iterations: escalate
todos:
  - id: loop-290-1
    content: "Inspect the current dirty diff and confirm it only contains the known review-fix patch."
    skill: review
    agent: gpt-5.4-mini-worker
    outcome: "Worker reports whether the dirty diff is limited to scratch work-dir, stage-out destination, publishDir patterns, AI Cellpose stage-out routing, and associated tests."
    status: pending
    priority: high
  - id: loop-290-2
    content: "Run the focused test set for CLI, parse_yaml, and Nextflow architecture smoke tests."
    skill: verification-before-completion
    agent: gpt-5.4-mini-worker
    outcome: "Focused pytest command exits 0 or returns exact failing tests."
    status: pending
    priority: high
  - id: loop-290-3
    content: "Run the full local pytest suite and git diff check."
    skill: verification-before-completion
    agent: gpt-5.4-mini-worker
    outcome: "Full pytest exits 0 and git diff --check has no real errors beyond line-ending warnings."
    status: pending
    priority: high
  - id: loop-290-4
    content: "Ask the parent integrator to commit the review-fix patch with a concise message if verification passes."
    skill: "verification-before-completion"
    agent: parent
    outcome: "Parent confirms local git commit exists on ai-update containing only the review-fix files."
    status: pending
    priority: high
prompt: |
  ## Objective
  Land the current local review-fix patch safely.

  ## Constraints
  - Do not edit files outside owned_files.
  - Do not revert user or other worker changes.
  - Do not push.
  - If verification fails, stop and report exact failures.

  ## Verification
  - pytest tests/test_cli.py tests/test_parse_yaml.py tests/test_nextflow_architecture_smoke.py --basetemp=.tmp/pytest-loop290-focused -p no:cacheprovider --tb=short
  - pytest --basetemp=.tmp/pytest-loop290-full -p no:cacheprovider --tb=short
  - git diff --check

  ## Completion
  Return changed files, commands run, test results, and whether the parent should commit.
```

## Loop 300: Batch, Subset, and Chunk Validation Gate

```yaml
loop_id: 300
name: Batch, Subset, and Chunk Validation Gate
status: pending
type: implementation
model: gpt-5.4-mini
parallel_safe: read_only_until_gap_found
depends_on:
  - "290 for any edits to overlapping local code files"
blocked_by:
  - "Loop 290 not committed when Loop 300 requires edits to overlapping files"
  - "Subset semantics require lead decision"
owned_files:
  - cptools2/nextflow_chunking.py
  - cptools2/batch.py
  - cptools2/__main__.py
  - cptools2/parse_yaml.py
  - nextflow/main.nf
  - nextflow/modules/chunk_imagesets.nf
  - config/loop230-sarah-screen.yaml
  - tests/test_nextflow_chunking.py
  - tests/test_nextflow_architecture_smoke.py
handoff_summary:
  done: ""
  failed: ""
  needed: ""
handoff_required_fields:
  evidence: "Params fields, tests, or exact missing subset link"
  commands: "pytest and dry-run commands used"
  job_ids: "none"
  log_paths: "pytest output path if captured, otherwise none"
  next_owner: "parent or eddie-worker"
max_iterations: 3
on_max_iterations: escalate
todos:
  - id: loop-300-1
    content: "Determine whether a max_chunks or equivalent subset control already exists from YAML to params to Nextflow chunk filtering."
    skill: review
    agent: gpt-5.4-mini-worker
    outcome: "Worker reports exact files/params implementing subset control, or the missing link."
    status: pending
    priority: high
  - id: loop-300-2
    content: "Verify that dry-run params for each scratch-safe batch contain the active batch plate list, batch_id, and batch_total_size_gb."
    skill: review
    agent: gpt-5.4-mini-worker
    outcome: "Worker reports exact dry-run params fields proving Nextflow receives only the active batch's selected plates."
    status: pending
    priority: high
  - id: loop-300-3
    content: "If missing, add a minimal subset control that limits emitted chunk manifests without changing default full-run behaviour."
    skill: test-driven-development
    agent: gpt-5.4-mini-worker
    outcome: "Configurable max_chunks exists and default behaviour remains unlimited."
    status: pending
    priority: high
  - id: loop-300-4
    content: "Add or update tests proving chunk subset behaviour and default full behaviour."
    skill: test-driven-development
    agent: gpt-5.4-mini-worker
    outcome: "Tests fail before the fix or clearly exercise the new behaviour; tests pass after implementation."
    status: pending
    priority: high
  - id: loop-300-5
    content: "Document the subset command for Eddie Loop 230 smoke validation."
    skill: document-release
    agent: gpt-5.4-mini-worker
    outcome: "Runbook or phase plan records how to run a limited chunk smoke test."
    status: pending
    priority: medium
prompt: |
  ## Objective
  Make sure Loop 230 can run a small chunk subset before a full representative plate, and that every Nextflow invocation receives the intended active plate batch.

  ## Constraints
  - Start read-only. Edit only if subset control is missing or untested.
  - Preserve default behaviour: no subset limit unless explicitly configured.
  - Do not touch Eddie sync or remote state.

  ## Verification
  - pytest tests/test_nextflow_chunking.py tests/test_batch.py tests/test_cli.py tests/test_parse_yaml.py tests/test_nextflow_architecture_smoke.py --basetemp=.tmp/pytest-loop300 -p no:cacheprovider --tb=short
  - git diff --check

  ## Completion
  Return whether subset mode existed, whether active batch plate-list params are correct, what changed, test results, and the exact Eddie smoke parameter to use.
```

## Loop 310: Eddie Sync and Dry-Run

```yaml
loop_id: 310
name: Eddie Sync and Dry-Run
status: pending
type: infrastructure
model: gpt-5.4-mini
parallel_safe: false
depends_on:
  - "290"
  - "300"
blocked_by:
  - "Eddie login unavailable"
  - "Local baseline not committed"
  - "Subset/batch params gate unresolved"
owned_files:
  - .claude/plans/2026-04-26-loop230-sarah-screen-runbook.md
  - .claude/plans/2026-04-29-loop230-blocker-burndown-report.md
handoff_summary:
  done: ""
  failed: ""
  needed: ""
handoff_required_fields:
  evidence: "Remote checkout path, commit/hash/checksum evidence, params.batch_1.json path"
  commands: "SSH/sync/dry-run commands"
  job_ids: "none unless dry-run submits unexpectedly"
  log_paths: "Dry-run output/log path if captured"
  next_owner: "eddie-worker"
max_iterations: 2
on_max_iterations: checkpoint
todos:
  - id: loop-310-1
    content: "Sync the verified local source/config to the Eddie group checkout."
    skill: eddie-login
    agent: gpt-5.4-mini-worker
    outcome: "Shared checkout at /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 contains the local blocker-fix commit or matching files."
    status: pending
    priority: high
  - id: loop-310-2
    content: "Run cptools2 pipeline dry-run on Eddie for loop230 config."
    skill: eddie-login
    agent: gpt-5.4-mini-worker
    outcome: "Dry-run exits 0 and writes params.batch_1.json into scratch."
    status: pending
    priority: high
  - id: loop-310-3
    content: "Inspect generated params and command preview for scratch-contained execution."
    skill: eddie-validate
    agent: gpt-5.4-mini-worker
    outcome: "Params show stage_data true, selected plate list, data_destination, scratch output_dir, full pipeline paths, and work-dir is in scratch."
    status: pending
    priority: high
prompt: |
  ## Objective
  Put the verified local blocker fixes onto Eddie and confirm dry-run state before any expensive execution.

  ## Constraints
  - Use Eddie login workflows.
  - Do not start a full run in this loop.
  - Do not overwrite unrelated group-space files except the cptools2 project checkout/config files needed for this branch.

  ## Verification
  - cptools2 pipeline config/loop230-sarah-screen.yaml --dry-run on Eddie
  - Inspect /exports/eddie/scratch/mharvey2/cptools2-loop230/params.batch_1.json

  ## Completion
  Return sync method, Eddie host, command run, params path, and any mismatches.
```

## Loop 320: Eddie Submit-Pressure Guard and Resume

```yaml
loop_id: 320
name: Eddie Submit-Pressure Guard and Resume
status: completed
type: validation
model: gpt-5.4-mini
parallel_safe: false
depends_on:
  - "310"
blocked_by:
  - "Eddie dry-run params mismatch"
  - "Submit-host thread/resource pressure"
owned_files:
  - .claude/plans/2026-04-26-loop230-sarah-screen-runbook.md
  - .claude/plans/2026-04-29-loop230-blocker-burndown-report.md
handoff_summary:
  done: "Loop 320 completed on Eddie: STAGE_IN cached exit 0, BUILD_IMAGESET_INDEX exit 0, and CHUNK_IMAGESETS exit 0 using --stages none --max_chunks 1."
  failed: ""
  needed: "Proceed to Loop 330 Cellpose and feature extraction smoke."
handoff_required_fields:
  evidence: "Nextflow process status, chunk manifest path/count, or exact failure text"
  commands: "NXF_OPTS exports and nextflow resume command"
  job_ids: "SGE job ids if submitted"
  log_paths: "Nextflow log path and SGE stdout/stderr paths if available"
  next_owner: "eddie-worker or parent"
max_iterations: 2
on_max_iterations: checkpoint
todos:
  - id: loop-320-1
    content: "Check current Eddie scheduler/account pressure before resuming."
    skill: eddie-resources
    agent: gpt-5.4-mini-worker
    outcome: "Report qstat/job/thread context and whether conditions are reasonable for a small resume."
    status: completed
    priority: high
  - id: loop-320-2
    content: "Record the constrained NXF_OPTS, queue-size, and resume command that will be used for login-host execution."
    skill: eddie-validate
    agent: gpt-5.4-mini-worker
    outcome: "Runbook/report contains exact environment exports and resume command before execution starts."
    status: completed
    priority: high
  - id: loop-320-3
    content: "Resume Loop 230 with constrained NXF_OPTS and subset controls."
    skill: eddie-login
    agent: gpt-5.4-mini-worker
    outcome: "Nextflow resume command starts from scratch work dir and does not create work under group checkout."
    status: completed
    priority: high
  - id: loop-320-4
    content: "Verify BUILD_IMAGESET_INDEX and CHUNK_IMAGESETS complete, or capture exact blocker."
    skill: eddie-validate
    agent: gpt-5.4-mini-worker
    outcome: "Chunk manifests exist for 3723-D-100, or report includes command, log path, process name, and failure text."
    status: completed
    priority: high
prompt: |
  ## Objective
  Resume Loop 230 only far enough to prove staged plate indexing and chunk fan-out.

  ## Constraints
  - Do not run an unlimited full plate if subset mode is available.
  - Keep NXF_OPTS constrained for login-host execution.
  - Keep work dir in /exports/eddie/scratch/mharvey2/cptools2-loop230/work.

  ## Verification
  - Nextflow log shows BUILD_IMAGESET_INDEX completed.
  - Nextflow log shows CHUNK_IMAGESETS completed.
  - Chunk manifest files exist and preserve complete image sets.

  ## Completion
  Return process statuses, job ids if available, chunk count, and exact next command.
```

## Loop 330: Cellpose and Feature Extract Smoke

```yaml
loop_id: 330
name: Cellpose and Feature Extract Smoke
status: pending
type: validation
model: gpt-5.4-mini
parallel_safe: false
depends_on:
  - "320"
blocked_by:
  - "Chunk fan-out not proven"
  - "GPU queue unavailable"
  - "Cellpose command scaffold-only"
owned_files:
  - nextflow/modules/cellpose_segmentation.nf
  - nextflow/modules/feature_extract.nf
  - tests/test_nextflow_architecture_smoke.py
  - .claude/plans/2026-04-29-loop230-blocker-burndown-report.md
handoff_summary:
  done: ""
  failed: ""
  needed: ""
handoff_required_fields:
  evidence: "Cellpose/feature smoke artifact path or exact scaffold/container blocker"
  commands: "GPU smoke or module inspection commands"
  job_ids: "SGE GPU job ids if submitted"
  log_paths: "Nextflow/SGE/GPU smoke logs if available"
  next_owner: "parent"
max_iterations: 2
on_max_iterations: checkpoint
todos:
  - id: loop-330-1
    content: "Review Cellpose and feature extraction modules for placeholder commands versus real model invocation."
    skill: review
    agent: gpt-5.4-mini-worker
    outcome: "Worker identifies whether modules execute real tools or placeholders, with exact lines."
    status: pending
    priority: high
  - id: loop-330-2
    content: "If still placeholder-only, define the minimum viable smoke command without implementing full DINO or DeepProfiler science logic."
    skill: eddie-validate
    agent: gpt-5.4-mini-worker
    outcome: "Report gives the smallest safe GPU smoke command and required container inputs."
    status: pending
    priority: high
  - id: loop-330-3
    content: "Run or prepare a one-chunk Cellpose smoke on Eddie, depending on scheduler conditions."
    skill: eddie-login
    agent: gpt-5.4-mini-worker
    outcome: "Either one chunk produces Cellpose mask output, or blocker is documented with exact missing model/input/runtime issue."
    status: pending
    priority: high
prompt: |
  ## Objective
  Determine whether the AI path is executable beyond graph wiring, starting with Cellpose.

  ## Constraints
  - Do not implement full DINO.
  - Do not run full representative plate.
  - Use one chunk or the smallest available subset.

  ## Verification
  - Cellpose container starts with GPU where required.
  - One chunk creates a mask/location artifact, or the blocker is exact and reproducible.

  ## Completion
  Return whether this is a real execution path or still a scaffold, with next implementation task.
```

## Loop 340: Progress Report and Handoff

```yaml
loop_id: 340
name: Progress Report and Handoff
status: pending
type: documentation
model: gpt-5.4-mini
parallel_safe: false
depends_on:
  - "290"
  - "300"
  - "310"
  - "320"
  - "330"
blocked_by:
  - "Prior loop evidence missing"
owned_files:
  - .claude/plans/2026-04-29-loop230-blocker-burndown-report.md
  - .claude/plans/PLANS-INDEX.md
  - .claude/plans/phase-2.6-loop230-blocker-burndown.md
  - .claude/plans/phase-2.6-ralph-loops.md
handoff_summary:
  done: ""
  failed: ""
  needed: ""
handoff_required_fields:
  evidence: "Final report path and phase verdict"
  commands: "Documentation validation commands, if any"
  job_ids: "none"
  log_paths: "Referenced run logs from prior loops"
  next_owner: "lead"
max_iterations: 2
on_max_iterations: checkpoint
todos:
  - id: loop-340-1
    content: "Write the final blocker burn-down report with commands, results, and remaining risks."
    skill: progress-report
    agent: gpt-5.4-mini-worker
    outcome: "Report exists and separates fixed blockers, Eddie/environment blockers, and next implementation blockers."
    status: pending
    priority: high
  - id: loop-340-2
    content: "Update phase and loop status in the planning files."
    skill: document-release
    agent: gpt-5.4-mini-worker
    outcome: "Plan files reflect completed/pending loops and current next action."
    status: pending
    priority: medium
  - id: loop-340-3
    content: "Recommend whether Phase 2 is complete, still blocked, or should continue with another validation loop."
    skill: plan-eng-review
    agent: gpt-5.4-mini-worker
    outcome: "Final report contains a clear phase verdict with evidence."
    status: pending
    priority: high
prompt: |
  ## Objective
  Produce the written handoff after blocker burn-down work.

  ## Constraints
  - Do not invent results. Only report commands that actually ran.
  - Include exact paths and job ids where available.
  - Keep the recommendation blunt: complete, blocked, or continue.

  ## Verification
  - Report is saved under .claude/plans.
  - PLANS-INDEX links the phase and loop file.

  ## Completion
  Return the report path, phase verdict, and next recommended loop.
```
