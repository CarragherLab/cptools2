# Worktree Remediation and Consolidation Plan

Date: 2026-06-09

## Goal

Terminate the active DeepProfiler development path cleanly and consolidate the
work that should survive into the DINO-centered production route.

The plan must preserve the useful Phase 2.8 to Phase 3.1 infrastructure while
avoiding a risky one-shot merge between `ai-update` and `ai-update-DINO`.

## Current Worktrees

| Worktree | Branch | State | Role |
| --- | --- | --- | --- |
| `C:/Users/mharvey2/Coding/cptools2` | `ai-update` | dirty | DeepProfiler, Phase 2.9/3.1 batching, durable stage-out, pause decision |
| `C:/Users/mharvey2/Coding/cptools2-DINO` | `ai-update-DINO` | clean | DINO successor route, public-package generalization, later batching improvements |

The DINO worktree already merged an earlier `ai-update` state, but the branches
have diverged substantially since then.

## Consolidation Principles

1. Do not merge the entire DINO branch into `ai-update` in one step.
2. Treat DeepProfiler as legacy/maintenance, not as the production acceptance
   target.
3. Preserve shared infrastructure that benefits any feature extractor:
   batching, scratch sizing, verified cleanup, stage-out evidence, tmux/interactive
   launchers, run reports, and quota telemetry.
4. Keep DINO model code and DINO-specific Nextflow wiring in the DINO route.
5. Separate repository hygiene/public-packaging changes from scientific/runtime
   changes.
6. Do not delete `.claude`/`.context` planning assets in the main worktree as a
   side effect of consolidating DINO code. Decide that separately.
7. Do not create or remove worktrees while either source worktree has
   uncommitted changes that have not been intentionally checkpointed.
8. Every consolidation step should leave a reviewable commit or an explicitly
   documented no-change decision.

## Engineering Review Findings

The overall plan is sound, but it is not yet safe to execute without these
guardrails:

- The current `ai-update` worktree is dirty. Before creating the consolidation
  branch, checkpoint the DeepProfiler pause and consolidation docs or explicitly
  stash them. Otherwise the plan depends on uncommitted state that cannot be
  reliably ported.
- The DINO branch deletes `.claude/**` and `.context/**`. That may be the right
  public-package direction, but it is not required for DINO runtime correctness.
  Treat it as a separate repository-hygiene decision with its own review.
- The phrase "selectively port" needs a concrete artifact. The next required
  artifact is a merge map with one row per changed file or file group, including
  owner decision and verification requirement.
- Shared runtime files must be manually composed, not accepted wholesale from
  either branch. In particular, `cptools2/__main__.py`, `cptools2/batch.py`,
  `nextflow/main.nf`, `stage_in`, `stage_out`, and `eddie.config` carry
  independent lessons from both branches.
- Retiring worktrees is a final operation only. Do not remove `ai-update` or
  `ai-update-DINO` until the consolidation branch has passed local and Eddie
  validation and all useful evidence has been committed or archived.

## Risk Assessment

### High-Risk Merge Areas

- `.claude/**` and `.context/**`: DINO deletes or stops tracking a large volume
  of planning and local agent assets. This is probably desirable for a public
  branch, but it is not a runtime requirement and should not be bundled with the
  DINO feature merge.
- `nextflow/main.nf`, `nextflow/modules/stage_out.nf`,
  `nextflow/modules/stage_in.nf`, and `nextflow/conf/eddie.config`: both
  branches changed these files for related but not identical reasons.
- `cptools2/__main__.py`, `cptools2/batch.py`, and `tests/test_cli.py`: both
  branches changed batch lifecycle, cleanup, scratch sizing, and reporting
  behavior.
- Docs and examples: DINO generalizes project/site names while `ai-update`
  currently preserves Phase 3.1 DeepProfiler evidence and internal planning
  state.

### Lower-Risk Merge Areas

- New DINO-only modules:
  - `cptools2/dino_embed.py`
  - `cptools2/dino_export.py`
  - `cptools2/feature_extract/contract.py`
  - `nextflow/modules/export_cell_dino.nf`
  - `nextflow/conf/hpc.config`
  - DINO Dockerfile and DINO smoke scripts
- New DINO tests that do not overwrite DeepProfiler tests.
- New DINO reference docs and model-storage docs.

## Remediation Phases

### Phase A: Freeze and Evidence Preservation

Objective: make the DeepProfiler stop-line explicit and preserve evidence before
touching branch topology.

Tasks:

- Checkpoint the current `ai-update` documentation changes in a commit before
  starting branch topology work.
- Keep `docs/superpowers/specs/2026-06-09-deepprofiler-pause-dino-pivot.md` as
  the decision record.
- Ensure `TODOs.md` and `.claude/plans/phase-3.1-ralph-loops.md` say
  DeepProfiler acceptance is paused.
- Preserve the fresh DeepProfiler run evidence on Eddie until the DINO batching
  route has inherited the relevant lessons.
- Do not delete or rewrite old DeepProfiler code in this phase.

Exit criteria:

- Local docs clearly state that DeepProfiler is legacy/maintenance.
- No active TODO implies another DeepProfiler three-plate acceptance rerun is
  required before DINO work continues.
- `git status --short` has been reviewed, and any dirty files are either
  committed as part of the pause/remediation checkpoint or explicitly marked
  unrelated.

### Phase B: Classify DINO Changes Into Merge Buckets

Objective: split `ai-update-DINO` into reviewed buckets.

Buckets:

1. **Must merge into DINO successor path**
   - DINO adapters, embedding/export code, DINO containers, DINO tests.
   - Feature extractor contract shared by DeepProfiler and DINO.
   - DINO Nextflow wiring and stage-out/export tasks.

2. **Should merge as shared runtime hardening**
   - Real scratch quota measurement.
   - Per-batch scratch reclaim/resume-skip improvements.
   - Stage-out byte-level verification fixes.
   - Generic `hpc` profile if it reduces Eddie-specific coupling without
     breaking Eddie.
   - `CPTOOLS2_SCRATCH_ROOT`-first behavior.

3. **Decide separately**
   - Public-package site redaction and docs generalization.
   - `.claude`/`.context` untracking.
   - Renaming site-specific example configs.
   - `.gitattributes` line-ending policy.

4. **Do not carry forward unless needed**
   - DeepProfiler-only continuation work that has no value for DINO.
   - DeepProfiler Parquet scaling work.
   - DeepProfiler summary-path repair, unless the same status-summary pattern is
     reused by DINO.

Exit criteria:

- A short file-level merge map exists before any branch merge.
- Each risky file has an owner decision: take `ai-update`, take
  `ai-update-DINO`, or manually compose.
- The merge map includes at least these columns:
  - path or path group;
  - bucket;
  - source preference;
  - reason;
  - required tests or manual checks;
  - status.

Suggested merge-map path:

`docs/superpowers/plans/2026-06-15-dino-consolidation-merge-map.md`

### Phase C: Create a Consolidation Branch

Objective: integrate in isolation without dirtying either active worktree.

Recommended branch:

`codex/dino-consolidation`

Recommended base:

Start from `ai-update-DINO`, because DINO is the future direction and the branch
is currently clean. Then selectively port the DeepProfiler pause docs and any
remaining Phase 3.1 batching decisions from `ai-update`.

Why not start from `ai-update`:

- DINO has many commits of working feature-extraction implementation and Eddie
  evidence.
- Starting from `ai-update` would require replaying a large DINO stack and would
  increase conflict surface.

Exit criteria:

- New worktree/branch exists and starts clean.
- No direct destructive changes are made to `ai-update` or `ai-update-DINO`.
- The consolidation branch records its base commit and the two source branch
  heads in the merge-map document.

### Phase D: Selective Integration

Objective: bring over only the required changes.

Order:

1. Bring over DeepProfiler pause decision docs from `ai-update`.
2. Bring over any Phase 3.1 TODO state that is not already represented in DINO.
3. Bring over repository-independent DINO additions that do not overwrite shared
   runtime files.
4. Reconcile shared batching/cleanup code:
   - compare `cptools2/__main__.py`;
   - compare `cptools2/batch.py`;
   - compare `nextflow/modules/stage_out.nf`;
   - compare `nextflow/modules/stage_in.nf`;
   - compare `nextflow/conf/eddie.config` and `nextflow/conf/hpc.config`.
5. Keep DINO model modules from `ai-update-DINO`.
6. Keep DeepProfiler code only as legacy-supported code.
7. Reconcile docs after code, not before code.
8. Defer `.claude` and `.context` deletion unless separately approved.

Exit criteria:

- Consolidation branch contains DINO route plus preserved shared batching
  infrastructure.
- DeepProfiler stop-line is documented.
- No accidental `.claude`/`.context` deletion is bundled unless explicitly
  approved.
- The branch history shows small consolidation commits by bucket, not one large
  conflict-resolution commit.

### Phase E: Verification

Objective: verify behavior at three levels before merging.

Local tests:

- DINO adapter/export tests.
- Feature extractor contract tests.
- Batch cleanup/reclaim tests.
- Nextflow architecture smoke tests.
- Eddie launcher/static tests.

Suggested local subset:

```text
pytest tests/test_feature_extract_contract.py tests/test_dino_embed.py tests/test_dino_export.py tests/test_dinov2_config.py tests/test_dinov2_stageout.py tests/test_batch.py tests/test_batch_reclaim.py tests/test_nextflow_architecture_smoke.py tests/test_eddie_interactive_launcher.py -q
```

Eddie validation:

- dry-run DINO batch with fresh scratch root;
- one conservative DINO plate or representative subset;
- confirm durable stage-out evidence;
- confirm verified cleanup reclaims batch work;
- record quota snapshots before and after cleanup.

Exit criteria:

- Local focused tests pass or have documented pre-existing environment skips.
- Eddie dry-run confirms paths and batch split.
- At least one DINO acceptance route proves verified cleanup.
- Any skipped tests are listed in the merge map with the reason and replacement
  evidence.

### Phase F: Merge and Retire

Objective: land the consolidated route and retire obsolete worktrees.

Tasks:

- Merge `codex/dino-consolidation` into the active integration branch.
- Keep `ai-update` as an archive branch until the consolidated branch passes.
- Keep `ai-update-DINO` until the merge is reviewed and the DINO worktree is no
  longer needed.
- After review, remove obsolete worktrees with `git worktree remove` only after
  verifying they are clean.
- Tag or otherwise record the final pre-retirement branch heads so old evidence
  can be recovered if needed.

Exit criteria:

- One active branch represents the DINO production route.
- DeepProfiler is documented as legacy/maintenance.
- Obsolete worktrees are cleanly removed or retained explicitly as archives.

## Recommended Immediate Next Step

Do not merge yet.

Create a merge-map document from `git diff --name-status ai-update..ai-update-DINO`
that classifies files into:

- keep from DINO;
- keep from DeepProfiler/Phase 3.1;
- manually compose;
- defer/remove.

Then create `codex/dino-consolidation` from `ai-update-DINO` and apply the
approved map one bucket at a time.
