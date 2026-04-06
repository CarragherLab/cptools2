# Design: /parallel-lanes Skill — Parallel Phase Execution via Git Worktrees

**Date**: 2026-04-06
**Branch**: ai-update
**Status**: DRAFT

## Problem Statement

The advanced planning infrastructure (ralph loops, phase plans, /next-loop) assumes sequential execution. When a project has multiple independent implementation lanes (e.g., polars migration, Nextflow pipeline, container builds, CLI refactor), they can safely run in parallel because they touch different files. But there's no skill or command that orchestrates this: creating worktrees, spawning agents, monitoring completion, merging results, and triggering a consolidation phase.

## Architecture

```
/parallel-lanes invoked
        │
        ▼
Read PLANS-INDEX.md
  → Identify parallel phases (Parallel? = Yes)
  → Identify consolidation phase (Parallel? = No, depends on all parallel phases)
        │
        ▼
For each parallel phase:
  ┌─────────────────────────────────────────┐
  │ 1. git worktree add ../repo-lane-{X}    │
  │    ai-update -b lane-{X}                │
  │ 2. Spawn Agent (background: true)       │
  │    with phase plan + ralph instructions  │
  │ 3. Track agent ID                       │
  └─────────────────────────────────────────┘
        │ (all agents run concurrently)
        ▼
Wait for all agent completion notifications
        │
        ▼
For each completed lane:
  ┌─────────────────────────────────────────┐
  │ 1. Read agent result (success/failure)  │
  │ 2. If failed: report and stop           │
  └─────────────────────────────────────────┘
        │
        ▼
Merge all lane branches into ai-update:
  git checkout ai-update
  git merge lane-A lane-B lane-C lane-D --no-ff
        │
        ▼
Clean up worktrees:
  git worktree remove ../repo-lane-{X}
  git branch -d lane-{X}
        │
        ▼
Report summary + trigger consolidation:
  "All N lanes complete. Run /next-loop on Phase E."
```

## Skill Interface

### Invocation

```bash
/parallel-lanes                    # reads PLANS-INDEX.md, runs all parallel phases
/parallel-lanes --phases A,B       # run only specific phases
/parallel-lanes --status           # check progress of running lanes
/parallel-lanes --merge            # manually trigger merge (if auto-merge skipped)
```

### Input: PLANS-INDEX.md format

The skill reads `.claude/plans/PLANS-INDEX.md` and looks for the table with a `Parallel?` column:

```markdown
| Phase | Name | Status | Plan | Loops | Parallel? |
|---|---|---|---|---|---|
| A | Polars Migration | Planned | phase-A-polars-migration.md | 4 | Yes (worktree) |
| B | Nextflow Pipeline | Planned | phase-B-nextflow-pipeline.md | 5 | Yes (worktree) |
| E | Integration | Planned | phase-E-integration-validation.md | 4 | No (sequential, after A-D) |
```

Phases with `Yes (worktree)` are parallel lanes. Phases with `No` are sequential.

### Output

For each lane, the skill creates:
- A git worktree at `../{repo-name}-lane-{phase-letter}/`
- A git branch `lane-{phase-letter}` forked from the current branch
- A background agent executing the phase plan

On completion, it produces:
- Merge commit on the current branch incorporating all lane changes
- Summary of what each lane accomplished (from agent results)
- Prompt to run the consolidation phase

## Agent Prompt Template

Each background agent receives this prompt:

```
You are implementing Phase {X}: {phase_name} for the {project_name} project.

Working directory: {worktree_path}
Branch: lane-{X}
Phase plan: .claude/plans/{phase_plan_filename}

## Instructions

1. Read your phase plan at the path above.
2. For each Ralph Loop listed in the plan (in order):
   a. Read the loop description, todos, and success criteria
   b. Implement each todo:
      - Read relevant source files before modifying
      - Write code, run tests, fix failures
      - Commit after each meaningful change
   c. After completing all todos in a loop, commit with message:
      "complete: loop {loop_id} — {one-line summary}"
3. After all loops complete:
   - Run the full test suite: pytest tests/ -v
   - Commit any remaining changes
   - Return a summary of what was accomplished, what tests pass, and any issues.

## Constraints
- Only modify files listed in your phase plan's scope
- Do not touch files belonging to other lanes
- Run tests after each loop to catch regressions early
- If a test fails and you cannot fix it in 3 attempts, note it in your summary and move on

## Success Criteria
{paste success criteria from phase plan}

Begin. Start with Loop {first_loop_id}.
```

## Branch Strategy

```
ai-update (current)
    ├── lane-A (worktree: ../cptools2-lane-A/)
    ├── lane-B (worktree: ../cptools2-lane-B/)
    ├── lane-C (worktree: ../cptools2-lane-C/)
    └── lane-D (worktree: ../cptools2-lane-D/)

After completion:
ai-update ← merge lane-A + lane-B + lane-C + lane-D
    (lane branches deleted, worktrees removed)
```

Each lane branch is created from `ai-update` at invocation time. Lanes don't interact. After all complete, a single merge commit combines all lane changes.

## Failure Handling

| Scenario | Behavior |
|---|---|
| Agent fails mid-loop | Report which lane failed, which loop, agent's last output. Other lanes continue. |
| Merge conflict | Report conflicting files. User resolves manually, then runs `/parallel-lanes --merge` to retry. |
| Agent timeout | Background agents have no timeout. If an agent seems stuck, user can check with `/parallel-lanes --status`. |
| Partial completion | Completed lanes are mergeable. Failed lanes can be retried individually with `/parallel-lanes --phases {X}`. |

## State Tracking

The skill writes a state file at `.claude/state/parallel-lanes.json`:

```json
{
  "started": "2026-04-06T15:00:00Z",
  "base_branch": "ai-update",
  "lanes": {
    "A": {
      "agent_id": "abc123",
      "branch": "lane-A",
      "worktree": "../cptools2-lane-A",
      "phase_plan": "phase-A-polars-migration.md",
      "status": "running",
      "started": "2026-04-06T15:00:05Z"
    },
    "B": { ... }
  },
  "consolidation_phase": "phase-E-integration-validation.md"
}
```

This allows `/parallel-lanes --status` to report progress and `/parallel-lanes --merge` to find completed lanes.

## Plugin Structure

```
.claude/plugins/parallel-lanes/
  plugin.json
  skills/
    parallel-lanes/
      SKILL.md              # Main skill definition
  commands/
    parallel-lanes.md       # Slash command wiring
```

Or simpler: a single skill file at `.claude/skills/parallel-lanes/SKILL.md` that can be installed at user level and used across projects.

## Reusability

The skill is project-agnostic. It reads PLANS-INDEX.md for phase definitions and uses the `Parallel?` column to determine which phases to parallelize. Any project using the ralph planning stack can use this skill by:

1. Adding `Parallel? = Yes (worktree)` to their PLANS-INDEX table
2. Ensuring parallel phases touch different files (no merge conflicts)
3. Running `/parallel-lanes`

## Dependencies

- Git worktree support (standard git)
- Agent tool with `run_in_background: true`
- Ralph planning infrastructure (.claude/plans/, phase plan files)
- Enough disk space for N worktree copies of the repo

## Open Questions

1. **Should the consolidation phase auto-run after merge?** Currently the skill just prompts. Auto-running Phase E would make it fully autonomous.
2. **How to handle Lane C (container builds)?** This lane requires Docker Desktop and Eddie SSH, which may not be available to a background agent. May need to run interactively or with special tool permissions.
3. **Worktree location**: `../repo-lane-X/` (sibling directory) or `.worktrees/lane-X/` (inside repo, gitignored)? Sibling is simpler but clutters the parent directory.
