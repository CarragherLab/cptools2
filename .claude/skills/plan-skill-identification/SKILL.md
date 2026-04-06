---
name: plan-skill-identification
description: "Read the todos[] array in a ralph loop's YAML frontmatter and update the skill: field for each todo in-place. Discovers available skills from both project-local (.claude/skills/) and global (~/.claude/skills/) directories. Assigns one skill, multiple skills (as an array), or NA per todo. Run after plan-todos and before plan-subagent-identification. Maintains canonical field order when editing. Triggers: assign skills, identify skills for todos, skill mapping, fill skill fields, match skills to tasks."
---

# Plan Skill Identification

Reads a ralph loop's populated `todos[]` and assigns the most appropriate skill(s) to each task.
Discovers skills from both project-local and global directories. Assigns a single skill, multiple
skills (as a YAML array), or `NA` per todo. Edits the `skill:` field in-place, maintaining
canonical schema order.

## When to Use

- `todos[]` has been populated by `plan-todos` (all skills currently `NA`)
- Part of the full pipeline: `ralph-loop-planner` → `plan-todos` → **`plan-skill-identification`** → `plan-subagent-identification`
- You want to explicitly map skills before execution rather than leaving them for auto-detection

## Your Input

Provide:
- **Loop file path** (e.g. `plans/ralph-loop-002.md`)
- **Skills directory path** — the location of available SKILL.md files for this project

## Process

1. **Read the loop file** — extract all todos with `skill: NA`

2. **Discover available skills from all locations:**
   - **Project-local**: Glob `.claude/skills/*/SKILL.md` (or equivalent project skills path)
   - **Global fallback**: Glob `~/.claude/skills/*/SKILL.md`
   - Merge results; project-local takes precedence for duplicate skill names
   - Read each SKILL.md's frontmatter `name` and `description` fields
   - Build a complete catalogue of available skills across both locations

3. **For each todo, determine skill assignment:**
   - Read `content` and `outcome`
   - Match against skill descriptions using the three-level cascade (see below)
   - Determine how many skills apply:
     - **No skill needed** → set `skill: NA` (straightforward file I/O, git ops, simple tasks)
     - **One skill fits** → set `skill: "skill-name"` (single string)
     - **Multiple skills needed** → set `skill: ["skill-1", "skill-2"]` (YAML array)
   - Multiple skills are appropriate when a task genuinely spans domains (e.g. schema design + documentation, or data processing + statistical analysis)
   - Do NOT assign multiple skills just because they are vaguely related — each must contribute specific, distinct instructions the worker needs

4. **Update `skill:` field in-place** for each todo, maintaining canonical order:
   ```
   id → content → skill → agent → outcome → status → complexity → priority
   ```

5. **Report** any todos where no good skill match was found — flag as `MISSING: [description]`
   rather than leaving `NA`, so skill gaps are surfaced explicitly

## Three-Level Skill Cascade

Skills are assigned at three levels, each more specific than the last:

```
Phase-level (broad):
  `statistical-analysis` — assigned to the whole phase
  ↓
Loop-level (specific):
  `data-processing` — transforming raw inputs to analysis-ready format
  `output-formatting` — structuring results for downstream consumers
  ↓
Todo-level (precise):
  `schema-design` — needed specifically for the canonical field order in state files
```

When running plan-skill-identification, you are operating at the **todo level** — the most
precise tier. The phase and loop level skills inform which specialist sub-skills to look for.

## Skill Assignment Rules

- Use the skill's exact `name` from its SKILL.md frontmatter
- Assign `NA` for tasks that are straightforward file I/O, git operations, simple scripting, or general reference writing
- **Single skill**: when one skill clearly covers the task, assign it as a string: `skill: "skill-name"`
- **Multiple skills**: when a task genuinely requires expertise from two or more distinct domains, assign as an array: `skill: ["skill-1", "skill-2"]`. The worker loads each in order. Use this sparingly — only when each skill contributes distinct instructions.
- **Ordering**: when assigning multiple skills, put the primary/structural skill first and supplementary skills after
- If a task clearly needs a skill that does not exist yet, flag it as `MISSING: [description]` rather than leaving `NA` — this surfaces skill gaps for the project owner to address
- Do not assign skills to the handoff update task or git checkpoint tasks — these are `NA`

## Output Format

Updates `skill:` in each todo in-place. The field accepts three forms:

```yaml
todos:
  # Single skill — one specialist domain
  - id: "loop-002-1"
    content: "Migrate phase-plan-creator skill into core/skills/phase-plan-creator/"
    skill: "skill-creator"
    agent: "NA"
    outcome: "SKILL.md exists at target path; zero platform-specific references present"
    status: pending
    priority: high

  # Multiple skills — task spans two domains
  - id: "loop-002-2"
    content: "Create gate-verdict.schema.json with field documentation and worked examples"
    skill:
      - "schema-design"
      - "documentation"
    agent: "NA"
    outcome: "gate-verdict.schema.json validates as draft-07; inline field docs present"
    status: pending
    priority: high

  # No skill — straightforward task
  - id: "loop-002-3"
    content: "Write git checkpoint commit before starting work"
    skill: "NA"
    agent: "NA"
    outcome: "git log shows checkpoint commit before loop work begins"
    status: pending
    priority: high
```

## Missing Skill Flagging

If no existing skill covers a todo, flag it explicitly:

```yaml
  - id: "loop-005-3"
    content: "Validate loop-ready.json against the JSON Schema"
    skill: "MISSING: json-schema-validator — tool to programmatically validate JSON against draft-07 schema"
    agent: "NA"
    outcome: "Validation command exits 0; 0 validation errors reported"
    status: pending
    priority: high
```

This surfaces skill gaps for the project owner rather than silently falling back to general capability.

## After Completion

Run `plan-subagent-identification` to assign `agent:` fields for tasks that benefit
from subagent delegation.

## See Also

- `ralph-loop-planner/references/todo-schema.md` — Canonical field order reference
- `plan-todos` — Derives todos before this skill runs
- `plan-subagent-identification` — Assigns agents after this skill runs
- `core/schemas/todo.schema.md` — Formal schema for validation
