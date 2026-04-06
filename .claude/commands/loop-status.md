---
description: Show the current status of all ralph loops in the active phase plan. Displays loop names, todo completion counts, handoff summaries, and what comes next.
allowed-tools: Read, Glob
---

# /loop-status

Print a concise status report for all ralph loops in the active plan.

## Steps

### 1. Find plan files

```
Glob(".claude/plans/ralph-loop-*.md")
Glob(".claude/plans/phase-*-ralph-loops.md")
```

Read all found files.

### 2. For each loop, extract

- `name` and `task_name` from frontmatter
- Todo counts: pending / in_progress / completed / cancelled
- `handoff_summary.done`, `.failed`, `.needed`
- Dependencies section from markdown body

### 3. Print status table

```
Phase Plan Status
─────────────────────────────────────────────────────────────
Loop                      │ Todos            │ State
─────────────────────────────────────────────────────────────
ralph-loop-001            │ 4/4 complete     │ ✅ Done
  Task: Schema Definitions│                  │
  Done: All 4 schema docs created in core/schemas/.

ralph-loop-002            │ 2/6 complete     │ 🔄 In progress
  Task: Planning Skills   │                  │
  Done: phase-plan-creator and ralph-loop-planner migrated.
  Failed: —
  Needed: Migrate plan-todos, plan-skill-identification, plan-subagent-identification.

ralph-loop-003            │ 0/5 complete     │ ⏳ Pending
  Task: Agent Roles       │                  │
  Depends on: loop-001, loop-004
─────────────────────────────────────────────────────────────
Next action: run /next-loop to continue ralph-loop-002
```

### 4. Final line

- If all loops complete: `All loops complete. Phase plan finished. ✓`
- If a loop is blocked: `Loop NNN blocked — [dependency] must complete first.`
- Otherwise: `Run /next-loop to continue [loop-name].`

## Usage

```
/loop-status
```

No arguments required. Reads from `.claude/plans/` automatically.
