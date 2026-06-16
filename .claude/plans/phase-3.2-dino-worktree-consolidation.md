# Phase 3.2: DINO Worktree Consolidation and DeepProfiler Retirement

## Objective

Consolidate the DINO worktree into the forward production path while pausing active DeepProfiler scale-up development. Preserve the operational lessons from Phase 3.1 batching, stage-out, cleanup, Eddie execution, and reporting without one-shot merging unrelated worktree history.

## Scope

### In Scope

- Create an isolated consolidation branch from `ai-update-DINO`.
- Port the DeepProfiler pause and DINO pivot documentation into the consolidation branch.
- Manually reconcile shared batching, staging, stage-out, cleanup, reporting, and Eddie launcher surfaces.
- Keep DINO as the active feature extraction route.
- Keep DeepProfiler available as legacy/maintenance code where tests and existing users require it.
- Verify DINO local tests, shared batching tests, Nextflow architecture smoke tests, and Eddie launcher behavior.
- Run a conservative Eddie dry-run or acceptance check from a fresh scratch root before closing the phase.

### Out of Scope

- Continuing DeepProfiler memory/runtime scale-up development.
- One-shot branch merges between `ai-update` and `ai-update-DINO`.
- Deleting `.claude/**` or `.context/**` as part of consolidation.
- Public documentation redaction or broad repository hygiene unless required by a failing consolidation test.
- Full production-scale multi-plate DINO execution before the dry-run and conservative acceptance checks pass.

## Deliverables

- `docs/superpowers/plans/2026-06-15-dino-worktree-consolidation-implementation.md`
- `docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`
- `docs/superpowers/plans/2026-06-09-worktree-remediation-consolidation.md`
- `docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md`
- `docs/superpowers/specs/2026-06-16-ai-update-dirty-state-cleanup-design.md`
- Consolidation branch: `codex/dino-consolidation`
- Safety branch: `codex/phase31-dirty-safety-checkpoint`
- Updated DINO route with reconciled shared runtime behavior.
- Local verification log covering DINO, batching, Nextflow, Eddie launcher, and DeepProfiler legacy guards.
- Eddie dry-run or conservative acceptance evidence from a fresh scratch root.

## Success Criteria

- Consolidation work is based on `ai-update-DINO`, not a direct merge into dirty `ai-update`.
- DINO-specific implementation files remain present and tested.
- Shared runtime reconciliation preserves:
  - scratch-aware batching
  - fresh run roots
  - tmux-driven driver launch
  - scheduler-managed GPU work
  - durable stage-out evidence
  - verified cleanup after stage-out
  - quota snapshots
  - run reports and batch status files
- DeepProfiler active scale-up is paused with documented rationale.
- DeepProfiler legacy tests are not broken silently.
- `.claude/.context` cleanup is deferred to a separate hygiene phase.
- Local focused tests pass.
- Dirty `ai-update` runtime state is archived on `codex/phase31-dirty-safety-checkpoint` before merge-back.
- `codex/dino-consolidation` is merged only into a clean `ai-update` planning checkpoint.
- Eddie dry-run or conservative acceptance evidence supports the consolidated route.

## Dependencies

- DINO worktree at `C:\Users\mharvey2\Coding\cptools2-DINO`.
- Main worktree at `C:\Users\mharvey2\Coding\cptools2`.
- Existing Phase 3.1 batching and cleanup lessons.
- Eddie access for dry-run or conservative acceptance validation.
- pytest local test environment.

## Skills Required

- `superpowers:writing-plans`
- `phase-plan-creator`
- `subagent-driven-development`
- `using-git-worktrees`
- `nextflow-development`
- `eddie-orchestrate`
- `eddie-validate`
- `eddie-resources`

## Risk Assessment

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Dirty `ai-update` files are accidentally staged or merged | Runtime regressions and unclear provenance | Stage only explicit planning files; create consolidation branch from clean DINO worktree |
| Dirty `ai-update` cleanup discards useful Phase 3.1 hardening | Loss of recoverable Eddie runtime fixes | Archive dirty state on `codex/phase31-dirty-safety-checkpoint` before cleaning target |
| One-shot merge brings `.claude/.context` deletions or unrelated hygiene | Loss of planning context and noisy review | Manual file-by-file composition; defer hygiene |
| Shared runtime files lose Phase 3.1 cleanup guarantees | Scratch pressure and incomplete output recovery | Preserve stage-out evidence, quota snapshots, batch status, and run reports as explicit acceptance criteria |
| DeepProfiler breaks unnoticed while being paused | Existing users lose legacy route | Run DeepProfiler legacy export guards and document expected limitations |
| DINO route passes local tests but fails on Eddie | False confidence before scale-up | Require fresh-root Eddie dry-run or conservative acceptance before phase close |

## Assumptions

- DINO is the forward feature extraction route and can supersede DeepProfiler for production development.
- DeepProfiler remains useful as legacy functionality, but its memory and maintenance limitations make it unsuitable for continued scale-up work at this point.
- A clean consolidation branch is safer than trying to repair history across dirty worktrees.
- Eddie validation should happen after local tests and before broad scale-up.

## Notes

- The detailed executable plan is `docs/superpowers/plans/2026-06-15-dino-worktree-consolidation-implementation.md`.
- The current file-level merge map is `docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`.
- Do not retire the original worktrees until consolidation verification is complete.

## Ralph Loops

### Loop 620: Planning Checkpoint and Branch Isolation

Goal: Commit only the planning state and create `codex/dino-consolidation` from the clean DINO branch.

Acceptance:

- Planning files are committed without unrelated runtime changes.
- Consolidation branch exists and starts clean from `ai-update-DINO`.

### Loop 630: DINO Implementation Preservation

Goal: Confirm the DINO route is intact in the consolidation branch.

Acceptance:

- DINO files are present.
- DINO-focused local tests pass.
- Any DINO-only consolidation fixes are committed separately.

### Loop 640: Shared Runtime Reconciliation

Goal: Manually compose shared batching, staging, stage-out, cleanup, reporting, and Eddie launcher files.

Acceptance:

- Shared files preserve DINO route behavior and Phase 3.1 operational safeguards.
- Batch reclaim, stage-out, Nextflow architecture, and launcher tests pass locally.

### Loop 650: Eddie Dry-Run and Conservative Acceptance

Goal: Validate the consolidated route from a fresh Eddie scratch root.

Acceptance:

- Dry-run or conservative acceptance produces driver status, batch status, run report, stage-out evidence, and quota snapshots.
- Cleanup occurs only after verified stage-out evidence.

### Loop 660: Merge and Retirement Decision

Goal: Archive dirty `ai-update` state, merge the verified consolidation branch into a clean `ai-update`, and decide which worktrees can be retired.

Acceptance:

- Verification evidence is documented.
- Dirty Phase 3.1 runtime state is preserved on `codex/phase31-dirty-safety-checkpoint`.
- `ai-update` is clean before merge-back.
- `codex/dino-consolidation` merges into `ai-update` without uncommitted target-state interference.
- DeepProfiler pause status is clear.
- Obsolete worktrees are retained or retired only after useful work is represented in the consolidation branch.
