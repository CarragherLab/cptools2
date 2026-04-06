# Todo Schema Reference

## Canonical Frontmatter Schema (Authoritative)

The extended schema lives in ralph loop frontmatter. **Field order is canonical** — agents must
maintain this order when editing to prevent schema drift and ensure reliable parsing.

```yaml
todos:
  - id: "loop-NNN-N"     # Format: loop-{loop-number}-{todo-number}; globally unique
    content: ""           # Atomic task description (verb-first, one action)
    skill: ""             # Skill name(s): string, array of strings, or "NA"
    agent: ""             # Agent ID from agents directory, or NA
    outcome: ""           # Observable completion condition (not effort description)
    status: pending       # pending | in_progress | completed | cancelled
    complexity: medium    # low | medium | high — drives worker model tier selection (optional, default medium)
    priority: high        # high | medium | low
```

### Field Specifications

| Field | Required | Valid Values | Rules |
|-------|----------|-------------|-------|
| `id` | Yes | `loop-NNN-N` | Globally unique; matches session tracking ID |
| `content` | Yes | Verb-first string | One atomic action per todo; no compound tasks |
| `skill` | Yes | skill-name, `[skill-1, skill-2]`, or `NA` | Single skill, array of skills for multi-domain tasks, or `NA` |
| `agent` | Yes | agent-id or `NA` | Must reference an existing agent, or `NA` |
| `outcome` | Yes | Observable condition | What must exist or pass — not effort description |
| `status` | Yes | See below | Updated in-place during execution |
| `complexity` | No | `low`, `medium`, `high` | Default `medium`. Drives worker model: `low` = Haiku eligible, `medium`/`high` = Sonnet |
| `priority` | Yes | `high`, `medium`, `low` | `high` for blocking tasks (default) |

### Status Values

| Status | Meaning |
|--------|---------|
| `pending` | Not yet started |
| `in_progress` | Currently being executed (max one at a time) |
| `completed` | Done — outcome verified, not just attempted |
| `cancelled` | Explicitly skipped with documented reason |

**Rule**: Only ONE todo may be `in_progress` at a time.
Mark `in_progress` **before** starting. Mark `completed` only after verifying the outcome condition.

---

## Session Tracking Schema (Derived)

Platform adapters maintain a derived session view of todos for real-time visibility during execution.
This is a **subset** of the frontmatter schema — `skill`, `agent`, and `outcome` are frontmatter-only fields.

Embed the `outcome` inline in the display content so it remains visible in the task list UI:

```
content display: "[task description] → [outcome condition]"
```

### Sync Protocol

1. **Loop start**: Map all frontmatter todos to session tracking format; register them
2. **During execution**: Update `status` as each todo changes — do not batch updates
3. **Canonical source**: Frontmatter YAML — if there is any conflict, frontmatter wins

### Priority Assignment

- `high` — blocking; next todo cannot start without this
- `medium` — important but not immediately blocking the loop's success criteria
- `low` — nice-to-have; explicitly non-blocking

Default all todos to `high` unless you have a specific reason for medium or low.

---

## Outcome Writing Standards

The `outcome` field answers: **"What must be true in the world for this todo to be done?"**

### ❌ Effort-based (Invalid)
```yaml
outcome: "Task complete"
outcome: "Code written"
outcome: "Run the script"
outcome: "Review the results"
outcome: "Done"
```

### ✅ Observable conditions (Valid)
```yaml
# File existence with content requirement
outcome: "core/schemas/foo.schema.md exists with all required sections and a validation checklist"

# Test result
outcome: "All tests pass; coverage ≥85%; linter reports 0 warnings"

# Negative scan
outcome: "No occurrences of 'Claude Code', 'Cowork', or 'slash command' appear in any SKILL.md in core/skills/"

# Numeric threshold
outcome: "Silhouette score >0.6 on validation set; result logged to reports/metrics.md"

# Schema validation
outcome: "JSON Schema validates against draft-07; required fields all present; pattern constraint on loop_name confirmed"

# Cross-reference check
outcome: "All field names in agent docs match handoff.schema.md; all state file names match state/README.md"
```

---

## In-Place Editing Rules

When agents update todos in the plan file:

1. **Maintain field order**: `id → content → skill → agent → outcome → status → complexity → priority`
2. **Only update `status`** during execution — never rewrite `id`, `content`, or `outcome`
3. **`skill` and `agent`** may be updated by planning skills before execution; not during
4. **One todo `in_progress` at a time** — set previous to `completed` before starting next
5. **Verify outcome before completing** — read the output, run the check, confirm the file exists

---

## Example: Complete Loop Todos

```yaml
todos:
  - id: "loop-002-1"
    content: "Migrate phase-plan-creator skill into core/skills/phase-plan-creator/"
    skill: "skill-creator"
    agent: "NA"
    outcome: "core/skills/phase-plan-creator/SKILL.md exists, contains no platform-specific references, and includes a references/ subdirectory"
    status: completed
    priority: high

  - id: "loop-002-2"
    content: "Migrate ralph-loop-planner skill into core/skills/ralph-loop-planner/ with updated references"
    skill: "skill-creator"
    agent: "NA"
    outcome: "core/skills/ralph-loop-planner/SKILL.md exists with references/ containing ralph-loop-template.md and todo-schema.md matching the v8 schemas"
    status: in_progress
    priority: high

  - id: "loop-002-3"
    content: "Migrate plan-todos skill into core/skills/plan-todos/SKILL.md"
    skill: "skill-creator"
    agent: "NA"
    outcome: "core/skills/plan-todos/SKILL.md exists, specifies model: opus, and contains at least three decomposition patterns"
    status: pending
    priority: high

  - id: "loop-002-4"
    content: "Verify all five skills contain zero platform-specific references"
    skill: "NA"
    agent: "NA"
    outcome: "No occurrences of 'Claude Code', 'Cowork', 'slash command', 'Agent tool', or 'TodoWrite' appear in any core/skills/ SKILL.md file"
    status: pending
    priority: high
```
