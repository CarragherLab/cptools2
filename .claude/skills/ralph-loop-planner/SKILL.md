---
name: ralph-loop-planner
description: "Decompose phase plans into executable ralph loop iterations with verifiable outcomes, concise handoffs, and session task tracking. Use after phase-plan-creator to generate task-based iterations ready for execution. Each iteration includes YAML frontmatter with todos (id/content/skill/agent/outcome/status/priority), a handoff_summary block (done/failed/needed), max_iterations with on_max_iterations recovery, and a complete execution prompt. Triggers: decompose phase, plan iterations, create loops, break down phase, execution planning."
---

# Ralph Loop Planner

Decomposes a phase plan into executable ralph loop iterations. Each iteration is self-contained, has verifiable success criteria, and includes everything needed for execution and resumption across sessions.

## When to Use

- You have a phase plan and need executable loops to deliver it
- Need to assign specific skills to each loop (refined from phase-level broad skills)
- Want verifiable success criteria and explicit outcome conditions per todo
- Need to identify sequencing, dependencies, and recovery behaviour between loops

## Your Input

Provide:
- **Phase plan** (from phase-plan-creator output, or pasted directly)
- **Loop count** (approximate; 3–6 is typical per phase)
- **Output preference** (single file for <10 loops; individual files for 10+)
- **Skill mapping** (optional: list of available skills in the skills directory)

## Process

1. **Analyse the phase plan** to identify:
   - Deliverables → loop outcomes (each loop produces one or more deliverables)
   - Sequencing constraints → loop dependencies
   - Broad skills → refined specific skills per loop
   - Complexity drivers → scope estimate per loop

2. **Generate N ralph loops** following the schema in `references/ralph-loop-template.md`

3. **For each loop, produce:**
   - YAML frontmatter with `todos` array (all fields in canonical order)
   - `handoff_summary` block (empty fields — filled at execution time, not before)
   - `max_iterations` and `on_max_iterations` set appropriately for the loop type
   - Complete execution `prompt` with handoff injection block
   - Markdown body: overview, success criteria, skills, inputs, outputs, dependencies, complexity

4. **Output locations:**
   - Single file: `plans/phase-{N}-ralph-loops.md` (recommended for <10 loops)
   - Individual files: `plans/ralph-loop-{NNN}.md` + update `plans/PLANS-INDEX.md`

## Output Structure per Loop

Each loop follows the full template in `references/ralph-loop-template.md`. Key fields:

```yaml
---
name: "ralph-loop-NNN"
task_name: "Descriptive Task Name"
max_iterations: 3
on_max_iterations: escalate       # escalate | checkpoint | rollback

handoff_summary:
  done: ""                        # Written at completion — empty before execution
  failed: ""
  needed: ""

todos:
  - id: "loop-NNN-1"
    content: "[atomic task — verb-first, specific]"
    skill: "[skill-name or NA]"
    agent: "[agent-id or NA]"
    outcome: "[concrete observable done condition]"
    status: pending
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  [What this loop accomplishes — one sentence]

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before {name}"

  ## Success criteria
  - [ ] [Criterion 1]

  ## Required skills
  - `[skill-name]`: [purpose]

  ## Inputs
  - [Input]: [source, format]

  ## Expected outputs
  - [Output]: [format, location]

  ## Constraints
  - [Constraint]

  ## On completion
  1. git add -A && git commit -m "complete: {name} — [one-line summary]"
  2. Update handoff_summary
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Handoff Convention

The `handoff_summary` block is written at **completion**, not upfront. Three fields, one sentence each:

```yaml
handoff_summary:
  done: "What was completed — files written, tests passing, decisions made."
  failed: "What did not complete and why; exact file or path if relevant; empty if none."
  needed: "Precise action the next loop should start with — not a restatement of the phase goal."
```

The next loop's `prompt` has a `## Context from prior loop` block with `[inject ...]` placeholders. At execution time, the orchestrator injects the prior loop's handoff fields there. This keeps context clean across sessions without dragging forward full prior outputs.

## Skill Refinement Pattern

```
Phase-level skill: `data-processing`
↓ Refined for this loop:
  Broad (from phase plan):
    - `data-processing`: Core transformation and structuring work
  Specific (refined for this loop):
    - `schema-design`: Canonical field order for state files
    - `docx`: Generating structured Word documents from templates
  Discovered (new, identified during planning):
    - `output-formatting`: Needed specifically for downstream consumer compatibility
```

Always distinguish: **Broad** (from phase plan) / **Specific** (refined for this loop) / **Discovered** (new, not in phase plan).

## Recovery Strategy by Loop Type

| Loop Type | Recommended `on_max_iterations` | Rationale |
|-----------|----------------------------------|-----------|
| Implementation / code | `escalate` | Partial code is often worse than none; surface to human |
| Research / exploration | `checkpoint` | Partial findings have value; commit and pause |
| Data processing | `checkpoint` | Preserve processed outputs; resume from checkpoint |
| Infrastructure | `rollback` | Partial infrastructure state is dangerous; reset to pre-loop commit |

## Output Locations

```
plans/phase-{N}-ralph-loops.md      ← All loops for phase N (< 10 loops)
plans/ralph-loop-{NNN}.md           ← Individual file (10+ loops)
plans/PLANS-INDEX.md                ← Update Ralph Loops table with new entries
```

## After Planning

1. Update `plans/PLANS-INDEX.md` — add each loop to the Ralph Loops table
2. Run `plan-todos` to expand any vague todos into atomic tasks
3. **MANDATORY**: Run `plan-skill-identification` to assign skills to each todo. This step is NOT optional — every todo must have a `skill:` field assigned (either a specific skill or `NA`) before execution begins. Skipping this step produces incomplete loop files.
4. Run `plan-subagent-identification` to assign agents where appropriate
5. Execute the first loop using the adapter's execution mechanism

## See Also

- `references/ralph-loop-template.md` — Full schema with examples and best practices
- `references/todo-schema.md` — Canonical todo format and outcome writing standards
- `phase-plan-creator` — Generate phase plans before loop decomposition
- `plan-todos` — Expand vague loop todos into atomic tasks
- `core/schemas/ralph-loop.schema.md` — Formal schema for validation
