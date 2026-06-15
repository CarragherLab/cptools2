# DINO Worktree Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate the DINO worktree as the forward production route, pause active DeepProfiler scale-up development, and migrate only the useful shared batching, staging, stage-out, cleanup, reporting, and Eddie runtime learnings into a clean branch without one-shot merging unrelated history.

**Architecture:** Treat `ai-update-DINO` as the implementation base, use `ai-update` as a source of documented Phase 3.1 runtime lessons, and manually compose shared pipeline files where both branches changed the same operational surfaces. DeepProfiler remains available as legacy/maintenance code, while DINO becomes the active scalable feature extraction route.

**Tech Stack:** Python, pytest, Nextflow DSL2, SGE/Eddie, Singularity/Apptainer containers, tmux-driven Eddie launchers, git worktrees.

---

## Current Evidence

- Main worktree: `C:\Users\mharvey2\Coding\cptools2`, branch `ai-update`, head `29de2bd22d83b418f2e0235d65f025b37c0f917a`, dirty.
- DINO worktree: `C:\Users\mharvey2\Coding\cptools2-DINO`, branch `ai-update-DINO`, head `32d5b1d690a5bf683ca1e2934fe4023e8355fa80`, clean.
- Existing consolidation documents:
  - `docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md`
  - `docs/superpowers/plans/2026-06-09-worktree-remediation-consolidation.md`
  - `docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`
- DINO branch contains the successor implementation files, including `cptools2/dino_embed.py`, `cptools2/dino_export.py`, `cptools2/feature_extract/contract.py`, `nextflow/modules/export_cell_dino.nf`, DINO container scripts, and DINO tests.
- `ai-update` contains the latest DeepProfiler pause decision and Phase 3.1 batching lessons, but also has unrelated dirty runtime changes that must not be merged blindly.

## Non-Negotiable Constraints

- Do not merge `ai-update-DINO` into `ai-update` directly.
- Do not merge the whole `ai-update` branch into the DINO branch.
- Do not delete or restore `.claude/**` or `.context/**` as part of this consolidation.
- Do not stage unrelated dirty runtime files from `ai-update`.
- Preserve DINO implementation work unless a test proves a regression.
- Preserve Phase 3.1 operational lessons: fresh scratch roots, tmux driver, per-batch work directories, per-batch parameter snapshots, durable stage-out evidence, rsync-verifiable cleanup, quota snapshots, and human-readable run reports.

## Task 1: Checkpoint Planning State

- [ ] From `C:\Users\mharvey2\Coding\cptools2`, run `git status --short`.
- [ ] Confirm these planning files exist and contain the latest decisions:
  - `TODOs.md`
  - `.claude/plans/phase-3.1-ralph-loops.md`
  - `docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md`
  - `docs/superpowers/plans/2026-06-09-worktree-remediation-consolidation.md`
  - `docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`
  - `docs/superpowers/plans/2026-06-15-dino-worktree-consolidation-implementation.md`
  - `.claude/plans/phase-3.2-dino-worktree-consolidation.md`
- [ ] Stage only the planning files listed above plus `.claude/plans/PLANS-INDEX.md` if it changed.
- [ ] Commit with message `docs: plan DINO worktree consolidation`.
- [ ] Re-run `git status --short` and record that runtime/code files remain dirty but unstaged.

## Task 2: Create an Isolated Consolidation Branch

- [ ] Confirm the DINO source worktree is clean:

```powershell
git -C C:\Users\mharvey2\Coding\cptools2-DINO status --short
```

- [ ] Confirm the target consolidation path does not exist:

```powershell
Test-Path C:\Users\mharvey2\Coding\cptools2-dino-consolidation
```

- [ ] Create the consolidation worktree from the DINO branch:

```powershell
git -C C:\Users\mharvey2\Coding\cptools2 worktree add C:\Users\mharvey2\Coding\cptools2-dino-consolidation -b codex/dino-consolidation ai-update-DINO
```

- [ ] In the consolidation worktree, confirm:

```powershell
git -C C:\Users\mharvey2\Coding\cptools2-dino-consolidation branch --show-current
git -C C:\Users\mharvey2\Coding\cptools2-dino-consolidation status --short
```

Expected branch: `codex/dino-consolidation`. Expected status: clean.

## Task 3: Port Planning Documents Into the Consolidation Branch

- [ ] Copy or checkout these files from `ai-update` into `C:\Users\mharvey2\Coding\cptools2-dino-consolidation`:
  - `docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md`
  - `docs/superpowers/plans/2026-06-09-worktree-remediation-consolidation.md`
  - `docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`
  - `docs/superpowers/plans/2026-06-15-dino-worktree-consolidation-implementation.md`
  - `.claude/plans/phase-3.2-dino-worktree-consolidation.md`
- [ ] Do not copy `.claude/.context` changes.
- [ ] Commit the docs in the consolidation branch with message `docs: add DINO consolidation plan`.

## Task 4: Classify and Preserve DINO-Only Implementation

- [ ] In the consolidation branch, verify DINO-specific files are present:
  - `cptools2/dino_embed.py`
  - `cptools2/dino_export.py`
  - `cptools2/feature_extract/contract.py`
  - `cptools2/dockerfiles/Dockerfile.dinov2`
  - `nextflow/modules/export_cell_dino.nf`
  - `scripts/eddie_build_dinov2_sif.sh`
  - `scripts/eddie_interactive_launch.sh`
  - DINO-focused tests under `tests/`
- [ ] Run the DINO unit tests that do not require Eddie:

```powershell
pytest tests/test_feature_extract_contract.py tests/test_dino_embed.py tests/test_dino_export.py tests/test_dinov2_config.py tests/test_dinov2_stageout.py -q
```

- [ ] Fix only failures that are caused by consolidation drift.
- [ ] Commit any DINO-only fixes separately from shared runtime reconciliation.

## Task 5: Manually Compose Shared Runtime Files

For each shared file below, compare `ai-update`, `ai-update-DINO`, and the consolidation branch before editing:

```powershell
git -C C:\Users\mharvey2\Coding\cptools2-dino-consolidation diff ai-update..ai-update-DINO -- <path>
git -C C:\Users\mharvey2\Coding\cptools2-dino-consolidation diff ai-update -- <path>
```

Shared files requiring manual composition:

- `cptools2/__main__.py`
- `cptools2/batch.py`
- `cptools2/parse_yaml.py`
- `cptools2/nextflow_chunking.py`
- `nextflow/conf/eddie.config`
- `nextflow/main.nf`
- `nextflow/modules/stage_in.nf`
- `nextflow/modules/stage_out.nf`
- `nextflow/modules/feature_extract.nf`
- `scripts/eddie_interactive_launch.sh`
- `docs/reference/nextflow-config-yaml.md`

Composition rules:

- Preserve DINO route parameters and feature export contracts.
- Preserve Phase 3.1 batching controls that support scratch-aware batch splitting.
- Preserve verified cleanup only after durable stage-out evidence exists.
- Preserve explicit driver status and run report generation.
- Preserve failure-tolerant plate or batch reporting, so one failed plate does not hide successful outputs.
- Do not reintroduce DeepProfiler-specific memory assumptions as DINO defaults.

- [ ] Edit one related group of files at a time.
- [ ] Run the relevant focused tests after each group.
- [ ] Commit each coherent group with a message that names the runtime surface, for example `runtime: reconcile DINO stage-out cleanup`.

## Task 6: Defer Repository Hygiene

- [ ] Do not accept `.claude/**` deletions from DINO unless they are part of the new Phase 3.2 plan files.
- [ ] Do not accept `.context/**` deletions.
- [ ] Do not change public documentation redaction, `.gitattributes`, `.gitignore`, or `CLAUDE.md` unless a consolidation test requires it.
- [ ] Record deferred hygiene in `TODOs.md` or the phase plan rather than mixing it into runtime commits.

## Task 7: Local Verification Gate

Run this focused DINO and batching verification set in the consolidation worktree:

```powershell
pytest tests/test_feature_extract_contract.py tests/test_dino_embed.py tests/test_dino_export.py tests/test_dinov2_config.py tests/test_dinov2_stageout.py tests/test_batch.py tests/test_batch_reclaim.py tests/test_nextflow_architecture_smoke.py tests/test_eddie_interactive_launcher.py -q
```

Run this regression guard set:

```powershell
pytest tests/test_feature_export_deepprofiler.py tests/test_parse_yaml.py tests/test_cli.py -q
```

Pass criteria:

- DINO export tests pass.
- Batch reclaim tests pass.
- Nextflow architecture smoke tests pass.
- Eddie launcher tests pass locally.
- DeepProfiler legacy export tests still pass or any failures are documented as expected legacy limitations.

## Task 8: Eddie Dry-Run and Acceptance Gate

- [ ] Sync the consolidation branch or selected files to Eddie staging using the project-standard route.
- [ ] Run a DINO dry-run from a fresh scratch root.
- [ ] Confirm generated work, output, and stage-out paths remain under the fresh run root.
- [ ] Run a conservative DINO subset acceptance using tmux and scheduler-managed GPU tasks.
- [ ] Inspect:
  - `driver_status.json`
  - `status.txt`
  - `launcher.log`
  - `outputs/batch_status.csv`
  - `outputs/run_report.md`
  - `stage_out_evidence`
  - quota snapshots
- [ ] Confirm cleanup happens only after stage-out evidence supports it.
- [ ] Confirm failed batches are reported without hiding successful batches.

## Task 9: Merge and Retire

- [ ] Open a review comparing `codex/dino-consolidation` against the intended target branch.
- [ ] Merge only after local tests and Eddie dry-run/acceptance evidence are documented.
- [ ] Mark DeepProfiler scale-up as paused/legacy in phase documentation.
- [ ] Retire or archive obsolete worktrees only after their useful commits and evidence are represented in the consolidation branch.

## Completion Criteria

- A clean `codex/dino-consolidation` branch exists and is based on `ai-update-DINO`.
- DINO route remains functional after shared runtime reconciliation.
- Phase 3.1 batching lessons are represented in the DINO route where relevant.
- DeepProfiler active scale-up development is explicitly paused, not silently deleted.
- Local focused tests pass.
- Eddie dry-run/acceptance evidence exists for the consolidated DINO route.
- Worktree retirement is deferred until after verification, not used as a cleanup shortcut.
