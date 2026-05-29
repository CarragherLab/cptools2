# Plans Index — cptools2 Nextflow Imaging Pipeline

**Design spec (current)**: [2026-04-06-nextflow-pipeline-design.md](2026-04-06-nextflow-pipeline-design.md)
**Strategy review (Nextflow AI revamp)**: [2026-04-25-nextflow-ai-revamp-strategy.md](2026-04-25-nextflow-ai-revamp-strategy.md)
**Intra-plate batching design**: [2026-04-27-intra-plate-batching-design.md](2026-04-27-intra-plate-batching-design.md)
**Prior design spec**: [2026-04-06-container-pipeline-design.md](2026-04-06-container-pipeline-design.md) (superseded)
**Old phase plans**: [archive/](archive/) (superseded — custom Python orchestrator approach)

## Architecture

Nextflow DSL2 for orchestration + cptools2 Python companion library for data preparation.
Eddie SGE validated (Nextflow 25.10.4, job ID 55013075). nf-core Eddie config adapted for CMVM.

## Phases

Phases A-D run in **parallel worktrees**. Phase E runs after all 4 merge.

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Phase A  │  │ Phase B  │  │ Phase C  │  │ Phase D  │
│ Polars   │  │ Nextflow │  │ Contain- │  │ CLI +    │
│ Migration│  │ Pipeline │  │ er Builds│  │ Config   │
│ (4 loops)│  │ (5 loops)│  │ (3 loops)│  │ (3 loops)│
└────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │             │
     └─────────────┴─────────────┴─────────────┘
                         │
                    ┌────┴─────┐
                    │ Phase E  │
                    │ Integrate│
                    │ + Review │
                    │ (4 loops)│
                    └──────────┘
```

| Phase | Name | Status | Plan | Loops | Parallel? |
|---|---|---|---|---|---|
| A | Polars Migration | Planned | [phase-A-polars-migration.md](phase-A-polars-migration.md) | 4 | Yes (worktree) |
| B | Nextflow Pipeline | Planned | [phase-B-nextflow-pipeline.md](phase-B-nextflow-pipeline.md) | 5 | Yes (worktree) |
| C | Container Builds & Deploy | Planned | [phase-C-container-builds.md](phase-C-container-builds.md) | 3 | Yes (worktree) |
| D | CLI & Config Refactor | Planned | [phase-D-cli-config-refactor.md](phase-D-cli-config-refactor.md) | 3 | Yes (worktree) |
| E | Integration & Validation | Complete | [phase-E-integration-validation.md](phase-E-integration-validation.md) | 4 | No (sequential, after A-D) |
| 1 | Code Review Fixes + Multi-Assay Hardening | Complete | [2026-04-06-code-review-fixes-design.md](2026-04-06-code-review-fixes-design.md) | 3 | No (sequential) |
| 2 | Eddie Deployment | Active | [phase-2-eddie-deployment.md](phase-2-eddie-deployment.md) | 3 | No (sequential, C1→C2→C3) |
| 2.5 | Intra-Plate Batching and Fan-Out | Planned | [phase-2.5-intra-plate-batching.md](phase-2.5-intra-plate-batching.md) | 5 | No (sequential, after container/staging substrate) |
| 2.6 | Loop 230 Blocker Burn-Down | Active | [phase-2.6-loop230-blocker-burndown.md](phase-2.6-loop230-blocker-burndown.md) | 6 | Loop 320 complete; Loop 330 AI smoke next |
| 2.7 | Scratch Batch Reproducibility Hardening | Blocked on Eddie validation | [phase-2.7-scratch-batch-reproducibility.md](phase-2.7-scratch-batch-reproducibility.md) | 4 | No (350→370 complete; 380 moved to Eddie runtime validation) |
| 2.8 | Eddie Container Validation and Runtime Certification | Active | [phase-2.8-eddie-container-validation.md](phase-2.8-eddie-container-validation.md) | 6 | No (sequential, 390→440) |
| 2.9 | Feature Export Tables and Measurement Quality Gate | Passed | [phase-2.9-feature-export-quality-gate.md](phase-2.9-feature-export-quality-gate.md) | 7 | Hybrid inline + subagent execution |
| 3.0 | Verified Batch Cleanup and Scratch Relief | Active | [phase-3.0-verified-batch-cleanup.md](phase-3.0-verified-batch-cleanup.md) | 3 | Subagent-driven implementation |
| 3.1 | Production Execution Hardening and Durable Stage-Out Evidence | Active | [phase-3.1-durable-stageout-evidence.md](phase-3.1-durable-stageout-evidence.md) | 5 | Subagent-driven implementation, then Eddie acceptance |

## Branch-Scoped Drafts

These plans are not part of the default project loop queue. Agents should only
pick them up when they are on the named branch or the user explicitly asks for
that workstream.

| Branch | Draft | Status | Plan | Activation |
|---|---|---|---|---|
| `ai-update-DINO` | Baseline DINOv2 Phase-Image Feature Extraction | Draft, manual only; not assigned a default phase number | [draft-dinov2-phase-feature-extraction.md](draft-dinov2-phase-feature-extraction.md) | Work only on `ai-update-DINO` or explicit DINOv2 request |

## Loop Summary

| Phase | Loops | Total |
|---|---|---|
| A | A10, A20, A30, A40 | 4 |
| B | B10, B20, B30, B40, B50 | 5 |
| C | C10, C20, C30 | 3 |
| D | D10, D20, D30 | 3 |
| E | E10, E20, E30, E40 | 4 |
| 2.5 | 240, 250, 260, 270, 280 | 5 |
| 2.6 | 290, 300, 310, 320, 330, 340 | 6 |
| 2.7 | 350, 360, 370, 380 | 4 |
| 2.8 | 390, 400, 410, 420, 430, 440 | 6 |
| 2.9 | 510, 520 | 2 |
| 3.0 | 530, 540, 550 | 3 |
| 3.1 | 560, 570, 580, 590, 600 | 5 |
| **Default project loop total** | | **50** |

## Active Ralph Loop Files

| Phase | Loop File | Status |
|---|---|---|
| 3.1 | [phase-3.1-ralph-loops.md](phase-3.1-ralph-loops.md) | Active; first unfinished loop is 570, implementation begins at 580 after doc close-out |

Branch-scoped draft loops are excluded from the default total until promoted:

| Branch | Draft loops | Total |
|---|---|---|
| `ai-update-DINO` | DINO-01, DINO-02, DINO-03, DINO-04, DINO-05, DINO-06, DINO-07, DINO-08 | 8 |

## File Isolation (No Conflicts Between Parallel Lanes)

| Lane | Files touched |
|---|---|
| A | loaddata.py, splitter.py, file_tools.py, utils.py, containers.py, pyproject.toml (polars dep), tests for these |
| B | nextflow/\*, cptools2/templates/\*, tests/nf-test-data/ |
| C | cptools2/dockerfiles/\*, Eddie remote (.sif files, manifest) |
| D | parse_yaml.py, \_\_main\_\_.py, \_\_init\_\_.py, pyproject.toml (scissorhands dep), tests for these |

**Potential conflict**: pyproject.toml is touched by both Lane A (add polars) and Lane D (remove scissorhands). Different sections, easy merge.

## Execution

```bash
# Start all 4 lanes in parallel worktrees:
# Each gets its own agent running /next-loop --auto

# Lane A: polars migration
git worktree add ../cptools2-lane-a ai-update
# Agent: /next-loop --auto (phase-A plan)

# Lane B: Nextflow pipeline  
git worktree add ../cptools2-lane-b ai-update
# Agent: /next-loop --auto (phase-B plan)

# Lane C: container builds (requires Docker + Eddie SSH)
git worktree add ../cptools2-lane-c ai-update
# Agent: /next-loop --auto (phase-C plan)

# Lane D: CLI + config refactor
git worktree add ../cptools2-lane-d ai-update
# Agent: /next-loop --auto (phase-D plan)

# After all 4 complete:
# Phase E: merge, review, Eddie end-to-end test
# Agent: /next-loop --auto (phase-E plan)
```

## Reviews

- **Eng review**: completed 2026-04-06 (5 issues found, all resolved, 1 critical gap: scratch quota)
- **Office hours**: completed 2026-04-06 (design APPROVED, Nextflow hybrid architecture)
- **Eddie validation**: Nextflow SGE executor validated on Eddie (job ID 55013075)
- **Code review**: completed 2026-04-06 (3 critical bugs, 3 high, 21 additional issues)
- **Eng review (Phase 1)**: completed 2026-04-07 (2 issues found, 0 critical gaps, CLEARED)
- **Investigate**: completed 2026-04-07 (root cause: parallel lane interface mismatches, Bug 2 downgraded)
- **Loop 230 burn-down**: [2026-04-30 report](2026-04-30-loop230-blocker-burndown-report.md) records Eddie staging/index/chunk success for `example-plate-001`; Loop 330 remains next.
- **Eng review (Seqera Eddie scratch batching)**: completed 2026-05-01 (cleared for implementation; flat outputs preserved, batch identity limited to work/params/traces, guarded post-success cleanup required).
- **Phase 2.8 container validation**: active from 2026-05-05. Permanent code/config/container authority is `${CPTOOLS2_PROJECT_ROOT}`; transient runtime root is `${CPTOOLS2_SCRATCH_ROOT}`.
- **DeepProfiler NaN investigation**: completed 2026-05-20. NaNs are native DeepProfiler all-feature object vectors, not export artifacts or no-cell sites. Preserve NaNs in exported cell tables and report per-site quality burden. See [phase-2.9-feature-export-quality-gate.md](phase-2.9-feature-export-quality-gate.md), [../docs/reference/deepprofiler-scalability.md](../docs/reference/deepprofiler-scalability.md), and [../docs/superpowers/plans/2026-05-20-deepprofiler-nan-root-cause-investigation.md](../docs/superpowers/plans/2026-05-20-deepprofiler-nan-root-cause-investigation.md).
- **Phase 2.9 plate export join repair**: planned 2026-05-21 after the clean full-plate run completed 96 Cellpose and 96 DeepProfiler chunks but failed when `EXPORT_FEATURES` staged repeated `features` payload names into one plate task. See [phase-2.9-ralph-loops.md](phase-2.9-ralph-loops.md) and [../docs/superpowers/specs/2026-05-21-phase-2.9-plate-export-join-repair-design.md](../docs/superpowers/specs/2026-05-21-phase-2.9-plate-export-join-repair-design.md).
- **Phase 2.9 gate**: passed on 2026-05-22 after the clean DataStore-backed full plate completed `FEATURE_EXTRACT 96/96`, `EXPORT_FEATURES 1/1`, and `STAGE_OUT 97/97`, with staging-node verification of DataStore CSV tables. Gate verdicts are in `plans/gate-verdicts/phase-2.9-attempt-1-*.json`.
- **Phase 2.9 export identity/report hardening**: completed 2026-05-22 after the passed gate. DeepProfiler plate aliases now validate payload-derived plate identity before publication, the plate export join preserves chunk routing evidence, and multi-plate export summaries surface failed or missing plate exports without discarding successful tables. See [../docs/superpowers/specs/2026-05-22-deepprofiler-plate-identity-hardening-design.md](../docs/superpowers/specs/2026-05-22-deepprofiler-plate-identity-hardening-design.md) and [../docs/superpowers/plans/2026-05-22-deepprofiler-plate-identity-hardening.md](../docs/superpowers/plans/2026-05-22-deepprofiler-plate-identity-hardening.md).
- **Phase 2.9 optional Parquet memory follow-up**: deferred to the roadmap on 2026-05-22. Required CSV cell export streams on the certified full-plate route; optional cell-level Parquet output still needs a batched or streaming writer before it is recommended for full-plate scale. Track in [../../TODOs.md](../../TODOs.md).
- **Phase 3.0 verified batch cleanup**: active from 2026-05-27. Goal is to make batch cleanup the default scratch-relief lifecycle step only after rsync-backed `STAGE_OUT` verification confirms user-facing outputs are present. DINO remains deferred until this shared batching architecture is stable.
- **Phase 3.1 production execution hardening**: active from 2026-05-28. Goal is to harden Nextflow-led Eddie production execution with config-level diagnostics modes, tmux scratch-root launch, centralized batch failure continuation, user-readable run reports, and a three-plate acceptance run. See [phase-3.1-durable-stageout-evidence.md](phase-3.1-durable-stageout-evidence.md) and [../docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md](../docs/superpowers/plans/2026-05-28-phase-3.1-production-execution-hardening.md).
