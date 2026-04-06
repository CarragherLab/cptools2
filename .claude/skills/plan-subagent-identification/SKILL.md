---
name: plan-subagent-identification
description: "Read the todos[] array in a ralph loop's YAML frontmatter and update the agent: field for each todo in-place. Determines which tasks benefit from subagent delegation versus running in the main orchestrator context; assigns agent IDs from the agents directory. Run after plan-skill-identification. Maintains canonical field order when editing. Triggers: assign agents, identify agents for todos, agent mapping, fill agent fields, subagent delegation, who does this task."
---

# Plan Subagent Identification

Reads a ralph loop's todos (with skills already assigned) and categorises which tasks are
execution-focused versus coordination-focused. Updates the `agent:` field in-place.

**Note**: In platforms where the worker cannot spawn subagents (e.g. Claude Code), the `agent:`
field is planning-time metadata — it categorises the task type but does not trigger separate
spawning during loop execution. The worker executes all todos inline. The field remains valuable
for planning clarity, future platform adapters, and cost/complexity signalling.

## When to Use

- `skill:` fields have been assigned by `plan-skill-identification`
- Part of the full pipeline: `ralph-loop-planner` → `plan-todos` → `plan-skill-identification` → **`plan-subagent-identification`**
- The project has subagents configured in its agents directory

## Your Input

Provide:
- **Loop file path** (e.g. `plans/ralph-loop-002.md`)
- **Agents directory path** — the location of agent definition files for this project

## Process

1. **Read the loop file** — extract todos with `agent: NA`

2. **Discover available agents:**
   - List all agent definition files in the agents directory
   - Read each agent file's frontmatter for `name`, `description`, and `model` fields

3. **For each todo, assess delegation suitability:**

   **Delegate to a subagent when:**
   - Task is self-contained with clear inputs and outputs
   - Task requires deep focus in a specific domain (analysis, implementation, research)
   - Task is long-running and would pollute the orchestrator's context if run inline
   - Task benefits from an isolated execution context with its own skill injection cycle

   **Keep in orchestrator context (`agent: NA`) when:**
   - Task coordinates or synthesises results from other tasks
   - Task reads or writes the plan file itself (frontmatter updates, handoff writing)
   - Task is a simple one-liner (git commit, file copy, log write)
   - No suitable subagent exists and the task does not warrant creating one
   - Task requires the orchestrator's full session context to complete

4. **Assign agent IDs** from available agents, or flag `MISSING: [description]` if a suitable
   agent does not exist yet.

5. **Update `agent:` field in-place**, maintaining canonical order:
   ```
   id → content → skill → agent → outcome → status → complexity → priority
   ```

## Model Tier Economics

Subagent assignment also implies model tier selection. The three-tier model hierarchy:

| Role | Typical Model | Scope |
|------|---------------|-------|
| Orchestrator / Planning | Opus | Strategic decisions, phase plans, loop decomposition |
| Loop orchestrator | Sonnet | Loop preparation, context assembly, handoff writing |
| Loop worker | Sonnet (default); Haiku for low-complexity | Bounded task execution, file operations, code runs |

When assigning tasks to a worker subagent, you are delegating execution to an isolated
execution context, keeping the orchestrator focused on coordination. Most worker todos
run at Sonnet tier for reliable compositional reasoning; only todos with `complexity: low`
are eligible for Haiku.

### Complexity Assessment

When assigning agents, also consider the todo's `complexity` field:

| Complexity | Model | Criteria |
|-----------|-------|----------|
| `low` | Haiku eligible | Single-file edits, command execution, file copy, template fills |
| `medium` (default) | Sonnet | Multi-file changes, skill-guided tasks, domain reasoning |
| `high` | Sonnet | Cross-cutting changes, complex verification, architectural decisions |

## Output Format

```yaml
todos:
  - id: "loop-002-1"
    content: "Migrate phase-plan-creator skill into core/skills/"
    skill: "skill-creator"
    agent: "ralph-loop-worker"    # ← was NA, now assigned (bounded execution task)
    outcome: "SKILL.md exists at target path; zero platform-specific references present"
    status: pending
    complexity: medium
    priority: high

  - id: "loop-002-2"
    content: "Verify all five skills are platform-agnostic"
    skill: "NA"
    agent: "ralph-loop-worker"    # ← assigned (self-contained verification)
    outcome: "Zero occurrences of platform-specific terms across all core/skills/ SKILL.md files"
    status: pending
    complexity: low
    priority: high

  - id: "loop-002-3"
    content: "Update handoff_summary in loop frontmatter"
    skill: "NA"
    agent: "NA"                   # ← orchestrator task; touches plan file
    outcome: "handoff_summary.done populated with completed task list"
    status: pending
    complexity: low
    priority: medium
```

## Missing Agent Flagging

If no existing agent covers a delegation candidate, flag it explicitly:

```yaml
agent: "MISSING: data-analysis-worker — agent for long-running Python analysis tasks with isolated output directory"
```

This surfaces agent gaps for the project owner rather than silently falling back to orchestrator inline execution.

If no agents are configured yet, `plan-subagent-identification` will:
- Flag all delegation candidates as `MISSING: [agent-type]`
- Suggest which agent types would benefit this project
- Leave all `agent:` fields as `NA` so execution can proceed without subagents

## After Completion

The loop is now fully specified. Full pipeline summary:

```
ralph-loop-planner          → loop stubs with skeleton todos[]
plan-todos                  → todos[] populated (skill: NA, agent: NA)
plan-skill-identification   → skill: fields assigned
plan-subagent-identification → agent: fields assigned
[execute loop via adapter]  → work begins
```

## See Also

- `ralph-loop-planner/references/todo-schema.md` — Canonical field order reference
- `plan-skill-identification` — Must run before this skill
- `core/agents/` — Agent role definitions (orchestrator.md, worker.md)
- `core/schemas/todo.schema.md` — Formal schema for validation
