# Todo Schema Reference

The canonical schema for todos[] in ralph loop YAML frontmatter. Every todo produced by `plan-todos` must conform to this schema before passing to `plan-skill-identification`.

---

## Canonical Field Order

Fields must appear in this exact order. Maintaining order prevents schema drift and ensures reliable parsing by downstream skills and the Python API.

```yaml
todos:
  - id: "loop-{NNN}-{N}"       # e.g. loop-003-1 — globally unique
    content: ""                  # Verb-first, one atomic action
    skill: "NA"                  # Left as NA by plan-todos; filled by plan-skill-identification (single, array, or NA)
    agent: "NA"                  # Left as NA by plan-todos; filled by plan-subagent-identification
    outcome: ""                  # Observable completion condition
    status: pending              # Always pending when plan-todos writes a todo
    complexity: medium           # low | medium | high — drives worker model tier (optional, default medium)
    priority: high               # high | medium | low
```

---

## Field Rules

| Field | Rule |
|-------|------|
| `id` | Format `loop-{NNN}-{N}` — loop number zero-padded to 3 digits, todo number sequential from 1 |
| `content` | Starts with an imperative verb ("Create", "Run", "Verify", "Update"). One action only — no "and" |
| `skill` | Always `NA` when plan-todos writes; filled by plan-skill-identification |
| `agent` | Always `NA` when plan-todos writes; filled by plan-subagent-identification |
| `outcome` | Observable condition — what must be true, not what effort was made (see below) |
| `status` | Always `pending` when plan-todos writes; never pre-set to in_progress or completed |
| `complexity` | Default `medium`; set `low` for single-file edits, command runs, template fills; `high` for cross-cutting changes |
| `priority` | `high` for blocking tasks; `medium` for important but non-blocking; `low` for optional |

---

## Outcome Writing Rules

The `outcome` field answers: **"What must be true in the world for this todo to be done?"**

### ✅ Valid — observable conditions

```yaml
# File existence with content requirement
outcome: "core/skills/plan-todos/SKILL.md exists with decomposition patterns and outcome writing rules"

# Command exits cleanly
outcome: "python -m pytest platforms/python/tests/ exits 0; all 40 tests pass"

# Negative scan
outcome: "Zero occurrences of '.claude/' in any platforms/cowork/ file"

# Numeric threshold
outcome: "Silhouette score ≥ 0.6 on validation set; result logged to reports/metrics.md"

# Structured content check
outcome: "README.md contains: elevator pitch, quick-start steps, adapter table, and docs index"
```

### ❌ Invalid — effort descriptions

```yaml
outcome: "Done"
outcome: "Task complete"
outcome: "Reviewed the results"
outcome: "Ran the script"
outcome: "Worked on the schema"
```

---

## Decomposition Size Rules

| Loop scope | Target todo count | Warning signs |
|-----------|-------------------|---------------|
| Narrow (1 output) | 2–4 todos | |
| Standard (2–4 outputs) | 4–7 todos | |
| Broad (complex phase) | 6–8 todos | Consider splitting the loop |
| Over 8 todos | 🚨 Loop is too large | Split before proceeding |

A loop with more than 8 todos almost always has scope that would be better split across two loops.

---

## Priority Assignment Guide

```
high   → blocking; the next todo cannot start without this one
medium → important to the loop succeeding but not immediately blocking
low    → nice-to-have; explicitly stated as non-blocking in the loop's Success Criteria
```

Default all todos to `high` unless there is a specific reason otherwise. Over-using `medium` and `low` obscures what actually needs to happen.

---

## Worked Example

Loop: "Create core planning skills" — 4 todos

```yaml
todos:
  - id: "loop-002-1"
    content: "Create core/skills/phase-plan-creator/SKILL.md with platform-agnostic content"
    skill: "NA"
    agent: "NA"
    outcome: "core/skills/phase-plan-creator/SKILL.md exists; zero occurrences of 'Claude Code', 'Cowork', or '.claude/' in the file"
    status: pending
    priority: high

  - id: "loop-002-2"
    content: "Create core/skills/ralph-loop-planner/SKILL.md with v8 handoff_summary schema"
    skill: "NA"
    agent: "NA"
    outcome: "core/skills/ralph-loop-planner/SKILL.md exists; references handoff_summary with done/failed/needed fields (not done/failed/next)"
    status: pending
    priority: high

  - id: "loop-002-3"
    content: "Create core/skills/plan-todos/SKILL.md with five decomposition patterns"
    skill: "NA"
    agent: "NA"
    outcome: "core/skills/plan-todos/SKILL.md exists with at least five named decomposition patterns and an outcome writing rules table"
    status: pending
    priority: high

  - id: "loop-002-4"
    content: "Verify all three skills contain zero platform-specific references"
    skill: "NA"
    agent: "NA"
    outcome: "grep scan returns zero occurrences of 'Claude Code', 'Cowork', 'slash command', 'Agent tool', 'TodoWrite', '.claude/' across all three SKILL.md files"
    status: pending
    priority: high
```
