# Ralph Loop Template & Best Practices

## What is a Ralph Loop?

A ralph loop iteration is a self-contained, verifiable unit of work with:
- A bounded scope deliverable in one focused session
- Explicit todos with `outcome` fields defining what "done" means per task
- A concise `handoff_summary` written at completion (done / failed / needed)
- Git checkpoints at start and end for auditability and rollback

---

## Schema: Two Layers

### Layer 1 — Frontmatter (Authoritative)

Extended schema stored in the plan file. Contains `skill`, `agent`, `outcome`, `priority` fields.
This is the source of truth. If there is ever a conflict, frontmatter wins.

### Layer 2 — Session Task Tracking (Display)

Derived from Layer 1. The subset used by the adapter's task tracking mechanism.
Platform adapters are responsible for syncing Layer 2 from Layer 1 at loop start.
Sync rule: at loop start, map each frontmatter todo to the session tracking format.
Update status in both layers as tasks progress — do not batch updates.

---

## Full Template

```yaml
---
name: "ralph-loop-NNN"
task_name: "[Human-Readable Task Name]"
max_iterations: 3
on_max_iterations: escalate         # escalate | checkpoint | rollback

# Written at loop COMPLETION, not before. Next loop injects this as context.
handoff_summary:
  done: ""                          # What was completed (one sentence max)
  failed: ""                        # What failed and why, or empty string
  needed: ""                        # What must still happen, or empty string

# Authoritative todo list. Field order is canonical and must be maintained.
todos:
  - id: "loop-NNN-1"
    content: "[Atomic task description — verb-first, specific]"
    skill: "[skill-name or NA]"
    agent: "[agent-id or NA]"
    outcome: "[What must exist or pass before this todo is complete]"
    status: pending
    priority: high
  - id: "loop-NNN-2"
    content: "[Atomic task description]"
    skill: "[skill-name or NA]"
    agent: "[agent-id or NA]"
    outcome: "[Observable completion condition]"
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
  git add -A && git commit -m "checkpoint: before ralph-loop-NNN"

  ## Success criteria
  - [ ] [Criterion 1]
  - [ ] [Criterion 2]

  ## Required skills
  - `[skill-1]`: [purpose]

  ## Inputs
  - [Input]: [source, format]

  ## Expected outputs
  - [Output]: [format, location]

  ## Constraints
  - [Constraint 1]

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-NNN — [one-line summary of what changed]"
  2. Update handoff_summary in frontmatter (done / failed / needed)
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Overview
[1–2 sentences: what this loop accomplishes and why it exists in sequence]

## Success Criteria
- ✓ [Criterion 1]: [how verified]
- ✓ [Criterion 2]: [how verified]

## Skills Required

### Broad (from phase plan):
- `[skill-domain]`: [purpose from phase]

### Specific (refined for this loop):
- `[specific-skill-1]`: [what you are doing with it]

### Discovered (new, identified during planning):
- `[new-skill]`: [why needed here and not anticipated at phase level]

## Inputs
| Input | Source | Format |
|-------|--------|--------|
| [Input Name] | [path or source] | [format] |

## Outputs
| Output | Location | Format |
|--------|----------|--------|
| [Output Name] | [path] | [format] |

## Dependencies

### Must Complete Before
- ralph-loop-[NNN-1]: [why blocking]

### Blocked By
- [External dependency]: [status or workaround]

### Parallelisable
- ralph-loop-[XXX]: [why safe to run concurrently]

## Complexity
**Scope**: Low / Medium / High
**Estimated effort**: [e.g. "1–2 hours"]
**Key challenges**:
1. [Challenge 1]
2. [Challenge 2]

## Rationale
[Why this structure; alternatives considered; known gotchas]
```

---

## Handoff: Three-Field Format

The handoff_summary is written **at loop completion**, not upfront. One sentence per field maximum:

```yaml
handoff_summary:
  done: "Normalised count matrix written to data/normalised.rds; batch correction applied."
  failed: "UMAP failed on 3 outlier samples (IDs in data/failed_samples.txt)."
  needed: "Re-run UMAP with n_neighbors=30 on remaining samples; skip failed IDs."
```

**Field rules:**
- `done` — completed work that persists: files written, tests passing, decisions made
- `failed` — what didn't complete and why; reference exact file/location if relevant; empty string if nothing failed
- `needed` — the precise action the next loop should start with; not a restatement of the phase goal

The next loop injects this block at the top of its execution prompt before the objective. This preserves continuity without dragging forward full prior session context.

**Anti-patterns:**
```yaml
# ❌ Too vague — next loop has no actionable starting point
needed: "Continue with phase goals"

# ❌ Effort description — not an observable condition
done: "Worked on the schema"

# ❌ Multiple sentences — use the most critical one only
done: "Schema created. Also fixed a bug in the template. And updated the README. Plus found an issue..."
```

---

## Git Checkpointing

Every loop bookends with two commits:

```bash
# Before starting work
git add -A && git commit -m "checkpoint: before ralph-loop-NNN"

# After completing work
git add -A && git commit -m "complete: ralph-loop-NNN — [one-line summary of what changed]"
```

This gives rollback points if a loop corrupts state, and a clean audit trail of what each loop produced. For `on_max_iterations: rollback`, the worker resets to the pre-loop checkpoint commit.

---

## Error Recovery

Defined by `on_max_iterations` in frontmatter:

| Value | Behaviour | When to Use |
|-------|-----------|-------------|
| `escalate` | Stop, write partial handoff (`failed` field), surface to human | Code, infrastructure — partial state is worse than none |
| `checkpoint` | Commit current state, write handoff, pause — resumable later | Research, data processing — partial progress has value |
| `rollback` | `git reset --hard` to the pre-loop checkpoint commit | Infrastructure — partial infrastructure state is dangerous |

---

## Outcome Writing Standards

The `outcome` field answers: *"What must be true in the world for this todo to be done?"*

### ❌ Invalid Outcomes
```yaml
outcome: "Task complete"
outcome: "Code written"
outcome: "Looks good"
outcome: "Done"
```

### ✅ Valid Outcomes
```yaml
# File existence
outcome: "core/schemas/foo.schema.md exists with all required sections and a validation checklist"

# Test passing
outcome: "All unit tests pass; coverage >85%; linter clean with 0 warnings"

# Content requirement
outcome: "reports/qc.md exists; contains sample counts, NA summary, and PCA interpretation"

# Zero-occurrence verification
outcome: "No occurrences of 'Claude Code', 'Cowork', or 'slash command' appear in any core/skills/ SKILL.md"

# Schema validation
outcome: "JSON Schema validates against draft-07; required fields present; pattern constraint on loop_name"
```

---

## Success Criteria Quality Standards

### ❌ Vague
- "Tests pass"
- "Code is clean"
- "Results look right"
- "Schema is complete"

### ✅ Specific and Verifiable
- "All tests in [suite] pass; linter reports 0 warnings"
- "File exists at [path], non-empty, contains [specific sections]"
- "Metric matches reference within [tolerance]%; documented in [file]"
- "Verification scan reports 0 matches for platform-specific terms"

---

## Full Example

```yaml
---
name: "ralph-loop-001"
task_name: "Schema Definitions"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: ""
  failed: ""
  needed: ""

todos:
  - id: "loop-001-1"
    content: "Write phase-plan.schema.md with required sections, field spec table, and validation checklist"
    skill: "NA"
    agent: "NA"
    outcome: "core/schemas/phase-plan.schema.md exists with all required sections, a field spec table, one worked example, and a validation checklist"
    status: pending
    priority: high

  - id: "loop-001-2"
    content: "Write ralph-loop.schema.md with frontmatter field table, on_max_iterations behaviour, and prompt design rules"
    skill: "NA"
    agent: "NA"
    outcome: "core/schemas/ralph-loop.schema.md exists with frontmatter field table, all three on_max_iterations values documented, and a validation checklist"
    status: pending
    priority: high

  - id: "loop-001-3"
    content: "Write todo.schema.md with two-layer architecture, canonical field order, and outcome writing standards"
    skill: "NA"
    agent: "NA"
    outcome: "core/schemas/todo.schema.md exists with two-layer table, all 7 fields in canonical order, status transitions, and outcome standards with anti-pattern examples"
    status: pending
    priority: high

  - id: "loop-001-4"
    content: "Write handoff.schema.md with three-field protocol, injection block, and anti-patterns"
    skill: "NA"
    agent: "NA"
    outcome: "core/schemas/handoff.schema.md exists with all three field specs, three worked examples, the injection block, and an anti-patterns table"
    status: pending
    priority: high

prompt: |
  ## Context from prior loop
  Done: Repository skeleton created; STRUCTURE.md and PLANS-INDEX.md in place.
  Failed:
  Needed:

  ## Objective
  Create four schema reference documents in core/schemas/ precise enough for any contributor to implement from the document alone.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-001"

  ## Success criteria
  - [ ] core/schemas/phase-plan.schema.md exists with field specs and validation checklist
  - [ ] core/schemas/ralph-loop.schema.md exists with frontmatter table, on_max_iterations behaviour, prompt rules
  - [ ] core/schemas/todo.schema.md exists with two-layer architecture, canonical field order, outcome standards
  - [ ] core/schemas/handoff.schema.md exists with three-field specs, worked examples, injection block, anti-patterns

  ## Required skills
  - None (schema design is general documentation work)

  ## Inputs
  - Source schemas: core/schemas/
  - Practical evidence: plans-in-practice-reporting/

  ## Expected outputs
  - core/schemas/phase-plan.schema.md
  - core/schemas/ralph-loop.schema.md
  - core/schemas/todo.schema.md
  - core/schemas/handoff.schema.md

  ## Constraints
  - All schemas must be platform-agnostic
  - Every field must have type, requirement level, valid values, and description
  - Examples must use the generic planning domain, not project-specific content

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-001 — 4 schema documents created"
  2. Update handoff_summary
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---

## Overview
Produce four schema documents in core/schemas/ defining every planning artefact. These are the definitional foundation everything else builds upon.

## Success Criteria
- ✓ All four schema documents exist in core/schemas/
- ✓ Every schema defines every field with type, requirement, valid values, description
- ✓ Each schema contains at least one worked example in the generic domain
- ✓ Validation checklists present in each document

## Skills Required

### Broad (from phase plan):
- `schema-design`: Defining clear, validatable schemas from practical evidence
- `documentation`: Writing reference documents usable without additional context

### Specific (refined for this loop):
- None — schema documents are plain markdown with YAML examples

### Discovered (new, identified during planning):
- None

## Inputs
| Input | Source | Format |
|-------|--------|--------|
| Core skills reference | core/skills/ | Markdown |
| Practical loop evidence | plans-in-practice-reporting/ | Markdown |

## Outputs
| Output | Location | Format |
|--------|----------|--------|
| Phase plan schema | core/schemas/phase-plan.schema.md | Markdown |
| Ralph loop schema | core/schemas/ralph-loop.schema.md | Markdown |
| Todo schema | core/schemas/todo.schema.md | Markdown |
| Handoff schema | core/schemas/handoff.schema.md | Markdown |

## Dependencies

### Must Complete Before
- Repository skeleton with core/schemas/ directory

### Blocked By
- Nothing — first loop in programme

## Complexity
**Scope**: Low — four documentation files, no code
**Estimated effort**: 1–2 hours
**Key challenges**:
1. Distilling practical evidence into minimal, complete schemas without over-specifying
2. Resisting aspirational fields not validated by actual usage
```

---

## Execution Workflow

```
1. Generate ralph loops (ralph-loop-planner)
2. Review structure, dependencies, skills
3. For each loop:
   a. Sync todos to session tracking (platform adapter handles this)
   b. Git checkpoint (before)
   c. Execute, marking todos in_progress → completed with outcome verified
   d. Write handoff_summary (done / failed / needed)
   e. Git checkpoint (complete)
   f. Review against success criteria
   g. If failed: retry up to max_iterations, then apply on_max_iterations behaviour
4. Inject handoff_summary into next loop's prompt context block
5. After all loops: review phase outcomes; update planning state
```
