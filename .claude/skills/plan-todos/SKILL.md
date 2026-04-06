---
name: plan-todos
description: "Derive atomic TODO tasks from a ralph loop description and populate the todos[] array in its YAML frontmatter. Use after ralph-loop-planner has generated loop stubs, to fill in the concrete task breakdown before skill and agent assignment. Reads the loop's Overview, Success Criteria, Inputs, and Outputs to derive tasks; writes them into the frontmatter in canonical schema order. Triggers: populate todos, break down loop, decompose loop tasks, fill in todos, expand loop."
---

# Plan Todos

Reads a ralph loop file and derives atomic, verifiable TODO tasks from its description.
Populates the `todos[]` array in YAML frontmatter, ready for `plan-skill-identification`
and `plan-subagent-identification` to fill in `skill` and `agent` fields.

## When to Use

- A ralph loop stub exists (from `ralph-loop-planner`) with an empty or skeleton `todos[]`
- You need to break the loop's stated work into atomic tasks before execution
- Part of the full pipeline: `ralph-loop-planner` → **`plan-todos`** → `plan-skill-identification` → `plan-subagent-identification`

## Your Input

Provide:
- **Loop file path** (e.g. `plans/ralph-loop-002.md`)
- Or paste the loop's frontmatter and description directly

## Process

1. **Read the loop file** — extract:
   - `task_name` and `## Overview`
   - `## Success Criteria` — each criterion implies ≥1 TODO
   - `## Inputs` and `## Outputs` — source and destination tasks
   - `## Complexity` — calibrate granularity of decomposition

2. **Derive atomic tasks** following these rules:
   - Each TODO must be completable in a single focused action
   - Each TODO must have a concrete, testable `outcome`
   - TODOs must be ordered: dependencies come first
   - No TODO should span multiple success criteria
   - Prefer 3–7 TODOs per loop; more than 8 suggests the loop is too large

3. **Assign priorities:**
   - `high` — blocking; next TODO cannot start without this
   - `medium` — important but not immediately blocking
   - `low` — nice-to-have; can be deferred if complexity bites

4. **Populate frontmatter in-place** maintaining canonical field order:
   ```
   id → content → skill → agent → outcome → status → complexity → priority
   ```
   Set `skill: NA` and `agent: NA` — these are filled by downstream skills.
   Set `complexity: medium` as the default — adjusted by `plan-subagent-identification` if needed.

5. **Verify** the populated todos collectively satisfy all Success Criteria.
   If any criterion has no corresponding TODO, add one.

## Output Format

Edits the `todos[]` array in the loop's YAML frontmatter:

```yaml
todos:
  - id: "loop-NNN-1"
    content: "[atomic task — verb-first, specific]"
    skill: "NA"
    agent: "NA"
    outcome: "[concrete condition: file exists / test passes / value within range]"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-NNN-2"
    content: "[next atomic task]"
    skill: "NA"
    agent: "NA"
    outcome: "[concrete condition]"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-NNN-3"
    content: "[downstream or verification task]"
    skill: "NA"
    agent: "NA"
    outcome: "[concrete condition]"
    status: pending
    complexity: medium
    priority: medium
```

## Task Decomposition Patterns

### Pattern: Output-driven decomposition
For each stated output, create:
1. A task that *produces* the output
2. A task that *validates* the output (if non-trivial)

### Pattern: Pipeline decomposition
For sequential data transforms, one TODO per stage:
```
Read data → Transform → Validate → Write output → Confirm
```

### Pattern: Research decomposition
```
Gather candidates → Evaluate each → Compare → Document recommendation
```

### Pattern: Infrastructure decomposition
```
Configure → Deploy → Health-check → Document rollback
```

### Pattern: Migration decomposition
```
Read source → Strip platform-specific content → Write to target → Verify portability
```

## Outcome Writing Rules

Outcomes must be **observable conditions**, not descriptions of effort:

| ❌ Effort-based | ✅ Observable condition |
|----------------|------------------------|
| "Run the normalisation script" | "`data/normalised.rds` exists, zero NAs, size factors >0" |
| "Review the results" | "QC plot saved to `plots/qc.pdf`; no samples flagged as outliers" |
| "Fix the failing tests" | "`pytest` exits 0; coverage ≥85%" |
| "Document the approach" | "`docs/approach.md` contains method, params, and reproducibility note" |
| "Migrate the skill" | "SKILL.md exists at target path; no platform-specific terms present" |
| "Check portability" | "Zero occurrences of platform-specific terms across all target files" |

## Next Steps

After `plan-todos` populates the `todos[]` array:

1. **`plan-skill-identification`** — reads todos, updates `skill:` field in-place
2. **`plan-subagent-identification`** — reads todos, updates `agent:` field in-place
3. Execute the loop using the adapter's execution mechanism

## See Also

- `ralph-loop-planner/references/todo-schema.md` — Canonical schema, field order, outcome standards
- `plan-skill-identification` — Fill `skill:` fields after todos are derived
- `plan-subagent-identification` — Fill `agent:` fields after todos are derived
- `ralph-loop-planner` — Generates the loop stubs that this skill populates
- `core/schemas/todo.schema.md` — Formal schema for validation
