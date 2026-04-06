---
description: Create a new phase plan and automatically decompose it into fully-specified, execution-ready ralph loops. Runs the complete planning pipeline without manual intervention — phase plan → loops → todos → skills → agents. Pass a description of what you want to accomplish.
allowed-tools: Read, Write, Glob, Edit, Agent
argument-hint: "[description of what you want to accomplish] [--auto]"
---

# /new-phase

Run the complete planning pipeline end-to-end. No manual steps required.

## Pipeline

```
1. phase-plan-creator           → generate structured phase plan
2. ralph-loop-planner           → decompose into loop stubs
3. plan-todos                   → populate todos[] for every loop
4. plan-skill-identification    → assign skills to every todo
5. plan-subagent-identification → assign agents to every todo
6. Ready — run /next-loop to begin execution
```

## Skill & Agent Path Resolution

Resolve all `.claude/skills/` and `.claude/agents/` references in this order:
1. **Project-local** — `.claude/skills/<name>/` (preferred)
2. **Global fallback** — `~/.claude/skills/<name>/` (used when local copy absent)

For Glob operations, search both locations and merge results; local takes precedence
for any duplicate skill/agent names.

## Steps

### 1a. Parse --auto flag

If `$ARGUMENTS` contains `--auto`:
- Set `AUTO_EXECUTE = true`
- Strip `--auto` from `$ARGUMENTS` before passing to skills

Otherwise: `AUTO_EXECUTE = false` (default).

### 1. Load phase planning skill

```
Read .claude/skills/phase-plan-creator/SKILL.md
Read .claude/skills/phase-plan-creator/references/phase-plan-template.md
```

### 2. Generate phase plan

Follow the phase-plan-creator instructions.
- Use `$ARGUMENTS` as the project/task description
- If no arguments: ask for description, constraints, and success definition before proceeding
- Auto-increment phase number N from existing files in `.claude/plans/`
- Save to `.claude/plans/phase-[N].md`
- Print: `✓ Phase plan saved to .claude/plans/phase-[N].md`

### 3. Load loop planning skill

```
Read .claude/skills/ralph-loop-planner/SKILL.md
Read .claude/skills/ralph-loop-planner/references/ralph-loop-template.md
Read .claude/skills/ralph-loop-planner/references/todo-schema.md
```

### 4. Decompose into ralph loops

Follow the ralph-loop-planner instructions.
- Read the phase plan just created
- Generate 3–6 loops with YAML stubs (empty `todos[]`, empty `handoff_summary`)
- Save to `.claude/plans/phase-[N]-ralph-loops.md`
- Print: `✓ [N] loops generated`

### 5. Load todo derivation skill

```
Read .claude/skills/plan-todos/SKILL.md
```

### 6. Populate todos for every loop

Follow the plan-todos instructions.
- For each loop in the ralph-loops file: derive atomic tasks, write `todos[]` in-place
- All todos start with `skill: NA`, `agent: NA`, `status: pending`
- Print: `✓ Todos populated across [N] loops`

### 7. Load skill identification skill

```
Read .claude/skills/plan-skill-identification/SKILL.md
```

### 8. Assign skills to every todo

Follow the plan-skill-identification instructions.
- `Glob(".claude/skills/*/SKILL.md")` to discover available skills
- Update `skill:` field in-place for every todo across all loops
- Print: `✓ Skills assigned`

### 9. Load subagent identification skill

```
Read .claude/skills/plan-subagent-identification/SKILL.md
```

### 10. Assign agents to every todo

Follow the plan-subagent-identification instructions.
- `Glob(".claude/agents/*.md")` to discover available agents
- Update `agent:` field in-place for every todo across all loops
- Print: `✓ Agents assigned`

### 11. Update CLAUDE.md Planning State

Read the `## Planning State` section and update:
- `phase:` — set to N
- `name:` — set to phase name
- `plan_file:` — set to `.claude/plans/phase-[N].md`
- `status: in_progress`
- `loop:` — set to first loop name
- `loop_file:` — set to `.claude/plans/phase-[N]-ralph-loops.md`
- `todos_done: 0`

### 12. Print completion summary and auto-execute

If `AUTO_EXECUTE` is false:
```
✓ Planning complete

Phase [N]: [name]
Loops:     [N] loops ready
Todos:     [N] total tasks

Run /next-loop to begin execution.
```

If `AUTO_EXECUTE` is true:
```
✓ Planning complete. Beginning autonomous execution...

Phase [N]: [name]
Loops:     [N] loops ready
Todos:     [N] total tasks
```

Then immediately chain into `/next-loop --auto` (no confirmation gate — user chose `--auto`).
