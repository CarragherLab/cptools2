# Plans Index — cptools2 Nextflow Imaging Pipeline

**Design spec (current)**: [2026-04-06-nextflow-pipeline-design.md](2026-04-06-nextflow-pipeline-design.md)
**Strategy review (Nextflow AI revamp)**: [2026-04-25-nextflow-ai-revamp-strategy.md](2026-04-25-nextflow-ai-revamp-strategy.md)
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

## Loop Summary

| Phase | Loops | Total |
|---|---|---|
| A | A10, A20, A30, A40 | 4 |
| B | B10, B20, B30, B40, B50 | 5 |
| C | C10, C20, C30 | 3 |
| D | D10, D20, D30 | 3 |
| E | E10, E20, E30, E40 | 4 |
| **Total** | | **19** |

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
