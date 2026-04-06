# Skill Catalogue Reference

Authoritative catalogue of all core skills in `core/skills/`. Used by `plan-skill-identification`
to match todos against available skills. Each entry includes the trigger conditions, scope, and
assignment guidance.

---

## How to Use This Catalogue

For each todo with `skill: NA`:

1. Read the todo's `content` and `outcome` fields
2. Identify the primary action (file creation, analysis, planning, execution)
3. Match against the **When to assign** conditions below
4. If multiple skills match, choose the **most specific** one
5. If no skill matches, assign `NA` for general tasks, or flag `MISSING: [description]` for tasks that clearly need a specialist skill that does not yet exist

---

## Core Skills

### `phase-plan-creator`

**Purpose**: Creates a phase plan document from a programme description, defining scope, outputs, loop count, and success criteria for an entire phase.

**When to assign:**

- Todo content describes creating or populating a `phase-{N}.md` file
- Todo involves defining the scope, outputs, or success criteria for a complete programme phase
- Todo is "design the plan for Phase X" or "break the programme into phases"

**When NOT to assign:**

- Creating individual loop files (use `ralph-loop-planner` instead)
- Decomposing a loop into todos (use `plan-todos` instead)
- Any execution work — this skill is planning-only

**Trigger keywords**: phase plan, phase document, programme scope, phase scope, define phases, create phase-N.md

---

### `ralph-loop-planner`

**Purpose**: Decomposes a single phase into bounded ralph loops. Writes the loop YAML stubs (name, task_name, max_iterations, on_max_iterations, handoff_summary skeleton, empty todos[]) into a phase's ralph loops file.

**When to assign:**

- Todo content describes creating or populating `phase-{N}-ralph-loops.md`
- Todo involves deciding how many loops a phase needs and naming them
- Todo is "decompose Phase X into loops" or "plan the loops for this phase"

**When NOT to assign:**

- Decomposing a loop into individual todos (use `plan-todos` instead)
- Writing the content of the loop itself — only the stub structure
- Execution of any loop work

**Trigger keywords**: ralph loops, loop stubs, loop decomposition, decompose phase, loop planning, new-loop, ralph-loop-planner

---

### `plan-todos`

**Purpose**: Reads a ralph loop's Overview, Success Criteria, Inputs, and Outputs and derives an atomic, verifiable `todos[]` array for the YAML frontmatter. Writes each todo with `skill: NA` and `agent: NA` (to be filled by downstream skills).

**When to assign:**

- Todo content describes populating the `todos[]` array in a loop file
- Todo is "decompose loop-{NNN} into todos" or "write the todos for this loop"
- Todo involves translating a loop's success criteria into atomic tasks

**When NOT to assign:**

- Assigning skills to existing todos (use `plan-skill-identification` instead)
- Assigning agents to existing todos (use `plan-subagent-identification` instead)
- Any work that isn't specifically about populating the todos array

**Trigger keywords**: populate todos, decompose into todos, write todos, plan todos, todo array, atomic tasks

---

### `plan-skill-identification`

**Purpose**: Reads todos with `skill: NA` and assigns the best-fit skill from the available SKILL.md files. Updates the `skill:` field in-place using the canonical field order. Flags `MISSING:` when no skill covers a task.

**When to assign:**

- Todo content describes assigning skills to a loop's todos
- Todo is "identify skills for loop-{NNN}" or "fill skill fields"
- Todo involves matching tasks to available SKILL.md files

**When NOT to assign:**

- Populating todos[] from scratch (use `plan-todos` first)
- Assigning agents (use `plan-subagent-identification` instead)
- Any execution work

**Trigger keywords**: assign skills, skill mapping, identify skills, fill skill fields, skill identification, match skills

---

### `plan-subagent-identification`

**Purpose**: Reads todos with skills already assigned and determines which tasks should be delegated to subagents. Updates the `agent:` field in-place. Flags `MISSING:` when a task warrants delegation but no suitable agent exists.

**When to assign:**

- Todo content describes assigning agents to a loop's todos
- Todo is "identify agents for loop-{NNN}" or "fill agent fields"
- Todo involves determining which todos should be delegated to workers vs run in orchestrator context

**When NOT to assign:**

- Assigning skills (use `plan-skill-identification` first)
- Populating todos[] from scratch
- Any execution work

**Trigger keywords**: assign agents, agent mapping, subagent identification, fill agent fields, delegation, who does this task

---

### `verification-before-completion`

**Purpose**: Enforces evidence-based completion claims. Before marking any todo as complete, the worker must run verification commands and confirm output matches expected outcomes. Prevents false completion claims and ensures observable done conditions are actually met.

**When to assign:**

- Todo involves implementation work that produces files, code, or configuration
- Todo has an outcome condition that can be verified by running a command or reading a file
- Todo claims work is "complete", "fixed", or "passing"
- Todo involves file creation, modification, or deletion where the result can be checked
- Todo requires running tests, builds, or validation scripts to confirm success

**When NOT to assign:**

- Planning-only todos (creating plan files, writing design docs)
- Read-only analysis or exploration tasks
- Todos where the outcome is purely observational ("identify the insertion point")
- Git operations (commits, checkpoints) — these are mechanical, not verification-worthy

**Trigger keywords**: verify, confirm, check, validate, ensure, complete, done, passing, implementation, build, test, fix

---

### `progress-report`

**Purpose**: Reads plan files, loop handoff summaries, todo statuses, and git commit history
to produce a structured markdown progress report. Read-only synthesis — never modifies artefacts.

**When to assign:**

- Todo content describes generating a progress summary or status report for a phase or programme
- Todo is "produce a progress report" or "summarise what has been done"
- Todo involves reading handoff summaries and todo statuses to compile an audit trail

**When NOT to assign:**

- Any execution work — this skill is read-only synthesis
- Creating or modifying plan files (use `phase-plan-creator` or `ralph-loop-planner` instead)
- Populating todos (use `plan-todos` instead)

**Trigger keywords**: progress report, status report, what happened, show progress, review progress, audit, summarise completed work, loop summary

---

### `companion-detection`

**Purpose**: Detects whether companion tools (Superpowers, Plannotator) are installed and recommends them when relevant. Produces no files — outputs conversational recommendations only.

**When to assign:**

- Todo content describes checking for companion tools or recommending integrations
- Todo involves session setup or environment verification
- Todo is "check for available integrations" or "recommend companion tools"

**When NOT to assign:**

- Any execution work — this skill is advisory-only
- Todos that need actual companion functionality (use the companion's skills directly)
- Already inside a loop where companions are being used

**Trigger keywords**: companion detection, check integrations, recommend tools, session setup, environment check

---

## Pipeline Position Summary

```
phase-plan-creator          → Phase plan (phase-{N}.md)
       ↓
ralph-loop-planner          → Loop stubs (phase-{N}-ralph-loops.md)
       ↓
plan-todos                  → Populated todos[] (skill: NA, agent: NA)
       ↓
plan-skill-identification   → skill: fields assigned
       ↓
plan-subagent-identification → agent: fields assigned
       ↓
[execution via adapter]     → Work begins
```

Each skill operates on the output of the previous. Running them out of order produces incomplete or invalid results.

---

## Assignment Quick Reference

| Task type | Skill to assign |
|-----------|-----------------|
| Create phase plan document | `phase-plan-creator` |
| Create loop stubs for a phase | `ralph-loop-planner` |
| Populate todos[] in a loop | `plan-todos` |
| Assign skill: fields in todos | `plan-skill-identification` |
| Assign agent: fields in todos | `plan-subagent-identification` |
| Generate progress report or status summary | `progress-report` |
| Implementation todos claiming completion | `verification-before-completion` |
| Check for companion tools, recommend integrations | `companion-detection` |
| General file I/O, git ops, simple scripting | `NA` |
| Verification scans, grep checks | `NA` |
| Writing handoff_summary or loop-complete.json | `NA` |
| Gate review evaluation (phase boundary) | `NA` — handled by gate agents, not skills |
| Programme closeout synthesis | `NA` — handled by `programme-reporter` agent |

---

## `NA` Assignment Rules

Set `skill: NA` when the task is any of the following:

- Plain file creation or editing (no specialist knowledge required)
- Git operations: `git add`, `git commit`, `git tag`
- Running a verification scan (`grep`, `find`, `python -m pytest`)
- Writing structured data that follows a documented schema (loop-ready.json, loop-complete.json)
- Updating `handoff_summary` in a loop file
- Creating a snapshot checkpoint
- Simple bash scripting without domain-specific logic
- **Gate review evaluation** — handled entirely by gate agents (`code-review-agent`, `phase-goals-agent`, `security-agent`, `test-agent`), not by skills. Assign `skill: NA` and leave the task out of the loop system; gate agents are spawned separately by `/run-gate`.
- **Programme closeout** — handled by the `programme-reporter` agent spawned by `/run-closeout`. Assign `skill: NA`; do not flag as `MISSING:`.

---

## Missing Skill Flagging

When a todo clearly needs a specialist skill that does not exist in the catalogue:

```yaml
skill: "MISSING: json-schema-validator — validates JSON files against draft-07 schema definitions"
```

The `MISSING:` prefix makes skill gaps immediately visible to the project owner. Do not silently fall back to `NA` when specialist knowledge is genuinely required and no skill covers it.
