# Design: Eddie Deployment — Containers, Staging, and End-to-End Testing

Reference: ~/.gstack/projects/CarragherLab-cptools2/mharvey2-ai-update-design-20260407-114442.md
Branch: ai-update
Status: APPROVED

## Summary

Three sub-phases to get cptools2 running on Eddie:
- C1: Build and deploy 3 Singularity containers to /exports/cmvm/.../chandranlabs/cptools2/containers/
- C2: Add STAGE_IN/STAGE_OUT Nextflow processes + batch-aware orchestration (migrate from job.py)
- C3: End-to-end test on a real plate

Key decisions:
- Single cptools2/ folder per group space (containers/ + env/ + nextflow/)
- Shared conda env for zero-install user experience
- os.execvp replaced with subprocess.run for batch loop
- Staging uses rsync --partial --timeout=300 on -q staging queue
- Scratch quota pre-flight via lfs quota (fallback: df or YAML config)
- Drug-Discovery deployment via same install script + container symlinks

See full design doc at the reference path above.

## Eng Review Amendments (2026-04-07)

1. **Parameterize CONTAINER_DIR** in build_containers.sh (accept as argument, default to new cptools2/containers/ path)
2. **Remove eddie_container_dir** from containers.config (dead config, never read by Nextflow)
3. **Add 6-7 unit tests** for batch.py (create_batches edge cases, quota fallbacks). Staging E2E deferred to Eddie.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | — | — |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | — | — |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 2 | CLEAR (PLAN) | 2 issues, 0 critical gaps |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | — |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | — |

**VERDICT:** ENG CLEARED — 2 issues found and resolved (build script path, dead config). 6-7 batch tests to be added during C2.
