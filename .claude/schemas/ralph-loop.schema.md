# Ralph Loop Schema

A ralph loop is a bounded, self-contained unit of work with verifiable outcomes and a handoff mechanism for cross-session continuity. It is the execution unit of the planning system.

---

## File Location

```
# Single-file mode (recommended for <10 loops per phase):
plans/phase-{N}-ralph-loops.md          ← All loops for phase N in one file

# Individual-file mode (for 10+ loops):
plans/ralph-loop-{NNN}.md              ← One file per loop
```

Loop numbers are zero-padded to three digits (`001`, `002`, ...) and are globally unique across all phases in a programme.

---

## Structure

Each ralph loop has two parts:

1. **YAML Frontmatter** — Machine-readable metadata, todos, handoff, and execution prompt
2. **Markdown Body** — Human-readable overview, success criteria, skills, dependencies, complexity

Both parts are enclosed in a single fenced code block (` ```yaml ... ``` `) when in a shared file, or as standard frontmatter (`---`) when in an individual file.

---

## YAML Frontmatter — Required Fields

```yaml
---
name: "ralph-loop-{NNN}"                # Globally unique identifier
task_name: "{Human-Readable Name}"       # What this loop does (short)
max_iterations: 3                        # Max retry attempts before escalation
on_max_iterations: escalate              # escalate | checkpoint | rollback

handoff_summary:                         # Written at COMPLETION, not before
  done: ""                               # What was completed (one sentence max)
  failed: ""                             # What failed and why, or empty string
  needed: ""                             # What must still happen, or empty string

todos: []                                # Array of todo items (see todo.schema.md)

prompt: |                                # Execution prompt injected at runtime
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

### Field Rules

| Field | Type | Required | Valid Values |
|-------|------|----------|--------------|
| `name` | string | Yes | `ralph-loop-{NNN}` (zero-padded, globally unique) |
| `task_name` | string | Yes | Human-readable, <80 chars |
| `max_iterations` | integer | Yes | 1–5 (default: 3) |
| `on_max_iterations` | enum | Yes | `escalate`, `checkpoint`, `rollback` |
| `handoff_summary` | object | Yes | Always present; fields empty until loop completes |
| `handoff_summary.done` | string | Yes | One sentence max; empty before completion |
| `handoff_summary.failed` | string | Yes | One sentence max; empty string if nothing failed |
| `handoff_summary.needed` | string | Yes | One sentence max; empty string if fully done |
| `todos` | array | Yes | Array of todo objects (see todo.schema.md) |
| `prompt` | string | Yes | Multi-line execution prompt with handoff injection block |
| `gate_failure_context` | object | No | Injected on retry after gate failure; absent on first attempt |

### Gate Failure Context (Optional)

The `gate_failure_context` block is injected into versioned retry loop files when a gate review fails. It is never present on first-attempt loop files. Its purpose is to give the retry attempt the context it needs to avoid repeating the same mistakes.

**When present:** The executing worker must read and respect all `do_not_repeat` constraints before beginning any todo. The `summary` provides a one-sentence account of why the previous attempt failed.

```yaml
gate_failure_context:
  attempt: 1
  verdict_file: gate-verdicts/phase-2-attempt-1.json
  summary: "Code review found unhandled NA values in batch 3 normalisation."
  loops_reverted:
    - loop: ralph-loop-002
      reason: "Normalisation function incomplete — batch 3 edge case"
    - loop: ralph-loop-003
      reason: "Integration depends on normalised.rds which is invalid"
  do_not_repeat:
    - "Do not use na.rm=TRUE as a workaround — address the upstream cause"
    - "Do not skip batch 3 — all samples must be included"
```

#### gate_failure_context Field Rules

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `attempt` | integer | Yes | The failed attempt number that triggered this injection |
| `verdict_file` | string | Yes | Relative path to the gate verdict JSON for the failed attempt |
| `summary` | string | Yes | One sentence explaining why the gate failed |
| `loops_reverted` | array | Yes | Loops whose outputs are invalid and must be re-executed |
| `loops_reverted[].loop` | string | Yes | Loop identifier, e.g. `ralph-loop-002` |
| `loops_reverted[].reason` | string | Yes | Why this loop's outputs are invalid |
| `do_not_repeat` | array | Yes | Actionable prohibitions for the retry; derived from gate findings |

See `core/state/gate-failure-context.schema.json` for the full JSON Schema.

### on_max_iterations Behaviour

| Value | When to Use | What Happens |
|-------|-------------|-------------|
| `escalate` | Code, infrastructure | Stop, write partial handoff, surface to human |
| `checkpoint` | Research, data processing | Commit current state, write handoff, pause |
| `rollback` | Infrastructure, risky changes | Reset to pre-loop checkpoint, restore prior state |

---

## Markdown Body — Required Sections

Follows the YAML frontmatter, outside the fenced block:

| Section | Purpose | Required |
|---------|---------|----------|
| `## Overview` | 1–2 sentences: what and why | Yes |
| `## Success Criteria` | Verifiable conditions with `✓` prefix | Yes |
| `## Skills Required` | Three sub-categories: Broad, Specific, Discovered | Yes |
| `## Inputs` | Source files/data with format | Yes |
| `## Outputs` | Output files/data with format and location | Yes |
| `## Dependencies` | Previous loops, external, parallelisable | Yes |
| `## Complexity` | Scope (Low/Med/High), estimated effort, key challenges | Yes |
| `## Rationale` | Why this structure, alternatives considered | Optional |

### Skills Required Sub-Categories

```markdown
## Skills Required
### Broad (from phase plan):
- `statistical-analysis`: Evaluation metrics

### Specific (refined for this loop):
- `data-processing`: Transforming raw inputs to analysis-ready format
- `output-formatting`: Structuring results for downstream consumers

### Discovered (new, identified during planning):
- `temporal-alignment`: Needed for look-ahead prevention in joins
```

This three-level categorisation documents how skills cascade from phase → loop → todo level.

---

## Prompt Design Rules

The `prompt` field is the instruction set given to the executing agent. It MUST contain:

1. **Handoff injection block** — `## Context from prior loop` with three placeholder fields
2. **Objective** — One sentence
3. **Git checkpoint** — Command to run before starting work
4. **Success criteria** — Checkboxes matching the markdown body
5. **Required skills** — With purpose per skill
6. **Inputs and Outputs** — With paths
7. **Constraints** — Explicit boundaries on what NOT to change
8. **On completion** — Git commit, handoff update, todo marking
9. **"Begin" instruction** — Explicit start signal with the one-in_progress-at-a-time rule

---

## Validation Checklist

Before executing a ralph loop, verify:

- [ ] `name` follows `ralph-loop-{NNN}` format and is unique
- [ ] `todos` array is populated (not empty)
- [ ] Every todo has an `outcome` that is verifiable (see todo.schema.md)
- [ ] `prompt` contains the handoff injection block with `[inject ...]` placeholders
- [ ] `on_max_iterations` matches the loop type (code→escalate, research→checkpoint, infra→rollback)
- [ ] Success criteria in the markdown body match those in the prompt
- [ ] All `skill:` values in todos reference skills that actually exist
