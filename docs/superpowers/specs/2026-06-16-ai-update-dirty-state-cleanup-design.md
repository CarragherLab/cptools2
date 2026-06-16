# ai-update Dirty State Cleanup Design

## Context

The `codex/dino-consolidation` worktree is locally verified and ready for merge-back preparation. The target branch, `ai-update`, is not clean. It contains unstaged Phase 3.1 runtime hardening files and untracked planning documents.

Current target state:

- Branch: `ai-update`
- Last committed checkpoint: `a10d4a3 docs: plan DINO worktree consolidation`
- Dirty runtime files:
  - `docs/reference/nextflow-config-yaml.md`
  - `nextflow/conf/eddie.config`
  - `nextflow/main.nf`
  - `nextflow/modules/cellpose_segmentation.nf`
  - `nextflow/modules/stage_in.nf`
  - `nextflow/modules/stage_out.nf`
  - `scripts/eddie_phase31_launch.sh`
  - `tests/test_eddie_phase31_launcher.py`
  - `tests/test_eddie_runtime_config.py`
  - `tests/test_nextflow_architecture_smoke.py`
- Untracked planning documents:
  - `.claude/plans/draft-dinov2-phase-feature-extraction.md`
  - `docs/superpowers/plans/2026-05-29-phase-3.1-acceptance-recovery-and-gpu-baseline.md`
  - `docs/superpowers/plans/2026-06-02-phase-3.1-shared-hardening-reconciliation.md`
  - `docs/superpowers/plans/2026-06-03-phase-3.1-post-hardening-eddie-acceptance.md`
  - `docs/superpowers/plans/2026-06-15-worktree-consolidation-return-to-ai-update.md`

The dirty runtime files are coherent Phase 3.1 hardening work, but they overlap with the DINO consolidation branch. Merging `codex/dino-consolidation` directly into this dirty target would make provenance and conflict resolution harder.

## Decision

Use a safety branch before cleaning `ai-update`.

Create a temporary archival branch named:

```text
codex/phase31-dirty-safety-checkpoint
```

Commit the current dirty runtime files and untracked planning documents on that branch. Then return to `ai-update`, restore it to the clean committed checkpoint `a10d4a3`, and merge `codex/dino-consolidation` into the clean target.

## Rationale

This keeps the forward path clean without losing work.

The dirty files may contain useful Phase 3.1 details, but `codex/dino-consolidation` has already absorbed the DINO-forward runtime behavior and passed the local gates:

- DINO/batching/launcher gate: `101 passed, 3 skipped`
- DeepProfiler/CLI legacy guard: `120 passed`

Preserving the dirty state on a safety branch lets us recover anything useful by later cherry-pick or file-level comparison. It avoids treating old DeepProfiler-oriented Phase 3.1 edits as part of the clean DINO merge-back.

## Rejected Options

### Commit Dirty Files Directly Onto `ai-update`

Rejected because it would make the target branch harder to reason about before merge-back. It would preserve old runtime changes in the main development line before deciding whether they are still compatible with the DINO consolidation route.

### Discard Dirty Runtime Files Immediately

Rejected because the dirty files include real hardening work around Cellpose GPU preflight, stage-in/stage-out, Eddie launcher behavior, and static tests. Discarding without an archival branch could lose useful evidence or implementation details.

## Cleanup Flow

1. Confirm `codex/dino-consolidation` is clean.
2. Confirm `ai-update` has no staged files.
3. Create `codex/phase31-dirty-safety-checkpoint` from current `ai-update`.
4. Commit only the dirty runtime files and untracked planning documents listed in this spec.
5. Return to `ai-update`.
6. Restore `ai-update` to the clean committed checkpoint `a10d4a3`.
7. Merge `codex/dino-consolidation` into the clean `ai-update`.
8. Run the full local merge-back gate.
9. Inspect the safety branch only if the merge-back gate reveals missing Phase 3.1 behavior.

## Guardrails

- Do not use a broad `git add .`.
- Do not delete dirty files without first committing them to the safety branch.
- Do not merge `codex/dino-consolidation` into dirty `ai-update`.
- Do not cherry-pick from the safety branch unless a concrete missing behavior is identified.
- Keep `.claude/.context` hygiene out of this cleanup.

## Success Criteria

- The safety branch contains the pre-cleanup dirty state.
- `ai-update` becomes clean at `a10d4a3` before merge-back.
- `codex/dino-consolidation` merges into `ai-update` without unrelated dirty-state interference.
- The full local gate passes after merge-back.
- The safety branch remains available until the Eddie dry-run confirms the merged DINO route.

## Self-Review

- No placeholder sections remain.
- The target branch, safety branch, and consolidation branch names are explicit.
- The dirty files are listed explicitly to avoid accidental staging.
- The design preserves work before any cleanup action.
