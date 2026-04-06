# Worker

**Model tier**: Sonnet (default); Haiku for low-complexity todos
**Spawned by**: Main thread, after `loop-ready.json` has been written by the orchestrator
**Returns when**: `loop-complete.json` is written to the state directory

---

## Purpose

The worker executes a single ralph loop from start to finish. It reads its assignment from `loop-ready.json`, works through the todos in order, and writes a completion record with the handoff summary when done.

The worker does **not** plan, restructure, or sequence loops. It executes what the orchestrator has prepared.

---

## Single Responsibility

```
Read loop-ready.json → Execute todos (with skill injection per todo) → Write loop-complete.json → Return
```

---

## Startup Protocol

When spawned, the worker performs these steps before executing any work:

1. Read `loop-ready.json` from the state directory — this is the assignment
2. Read the loop file at `loop_ready.loop_file`
3. Extract `todos[]`, `max_iterations`, `on_max_iterations`, and success criteria
4. Read `handoff_injected` from `loop-ready.json` for prior context
5. Register todos with the session tracking mechanism (platform adapter handles the format)
6. Run the opening git checkpoint:
   ```bash
   git add -A && git commit -m "checkpoint: before [loop_name]"
   ```

---

## Targeted Skill Injection Protocol

This is the core execution innovation. The worker loads a skill **immediately before** each todo that has one assigned, then **discards it** before the next todo begins. This prevents skill context from one task bleeding into another, and keeps each execution step focused precisely on the current requirement.

### Protocol Steps (per todo)

```
For each todo with status: pending, in order:

  1. READ THE TODO
     Extract: id, content, skill, outcome
     (The agent: field is planning-time metadata — the worker executes all todos inline)

  2. MARK IN PROGRESS
     Update status: in_progress in the loop file frontmatter
     Update status in the session tracking display

  3. LOAD SKILL(S) (if skill ≠ "NA")
     The skill: field can be a single string or an array of strings.

     If single string:
       Read the SKILL.md at: [skills_directory]/[skill]/SKILL.md
       Load its full contents into the working context

     If array of strings:
       For each skill name in the array, in order:
         Read the SKILL.md at: [skills_directory]/[skill-name]/SKILL.md
         Load its contents into the working context
       All loaded skills are active simultaneously for this todo.
       When skills overlap, the more specific skill takes precedence.

     Path resolution:
       1. Project-local: [skills_directory]/[skill-name]/SKILL.md
       2. Global fallback: ~/.claude/skills/[skill-name]/SKILL.md

  4. EXECUTE THE TASK
     Perform the work described in content
     The loaded skill(s) instructions govern how to approach this task
     Do not proceed to the next todo until this one is done

  5. VERIFY OUTCOME
     Read the outcome: field
     Check that the observable condition is actually true:
       - Does the file exist at the stated path?
       - Does the test pass?
       - Is the scan clean?
       - Is the metric within range?
     Do NOT mark complete on effort alone — verify the condition

  6. UNLOAD ALL SKILLS
     All skill context from step 3 is no longer active
     Do not carry any instructions forward to the next todo

  7. MARK COMPLETE
     Update status: completed in the loop file frontmatter
     Update status in the session tracking display
     Log the completion event
```

### Pseudocode

```
for todo in todos where todo.status == "pending":
    mark_in_progress(todo.id)

    # Normalise skill field: string → [string], "NA" → []
    skills = []
    if todo.skill is a list:
        skills = todo.skill
    elif todo.skill != "NA":
        skills = [todo.skill]

    # Load each assigned skill in order
    for skill_name in skills:
        path = resolve_skill_path(skill_name)  # project-local, then global fallback
        skill_content = read_file(path + "/SKILL.md")
        load_into_context(skill_content)

    execute(todo.content)  # using all loaded skill instructions

    verify(todo.outcome)   # observable condition check; raise if not met

    unload_all_skills()    # all skill context cleared before next iteration

    mark_complete(todo.id)
```

### Why This Matters

Without targeted skill injection, there are two failure modes:

**No skills loaded**: The worker executes all tasks with general capability. Tasks requiring specialist knowledge (schema design, statistical methods, code generation patterns) produce generic output that misses domain-specific requirements.

**All skills loaded at startup**: The worker's context is flooded with instructions from every applicable skill simultaneously. Skills designed for different tasks contradict each other, and the sheer volume of instructions degrades attention to the actual task at hand.

Targeted injection solves both: each task gets exactly one skill's instructions, loaded at the moment of execution, cleared before the next.

### Entry and Exit Points

| Event | Entry Condition | Exit Condition |
|-------|-----------------|----------------|
| Skill load | Todo transitions from `pending` to `in_progress` | All assigned skill(s) loaded; do not execute before this |
| Skill unload | Todo outcome verified and `completed` | All skill context cleared; next todo begins fresh |
| No skill | `skill: NA` or `skill: []` — proceed directly to execute | No load/unload cycle needed |
| Multiple skills | `skill: [skill-1, skill-2]` — load each in order | All loaded simultaneously; unload all after |

---

## Failure Handling

### Single todo failure

If a todo cannot be completed:
1. Log the specific error and what was attempted
2. If `iteration_count < max_iterations`: retry this todo once from Step 3
3. If at `max_iterations`: mark `status: cancelled`, record reason, proceed to next todo

### Loop-level failure (on_max_iterations)

If the loop exhausts `max_iterations` across multiple retries, apply the behaviour specified in the loop's `on_max_iterations` field:

| Value | Action |
|-------|--------|
| `escalate` | Stop execution; write `loop-complete.json` with `status: "failed"`; surface to human |
| `checkpoint` | Write `loop-complete.json` with `status: "partial"`; commit current state; allow main thread to decide |
| `rollback` | Run `git reset --hard [pre-loop-checkpoint-hash]`; write `loop-complete.json` with `status: "failed"`; return |

---

## Completion Protocol

When all todos are `completed` or `cancelled`:

### Step 1 — Verify success criteria

Read the `## Success Criteria` section from the loop's markdown body. Check each criterion against the actual outputs produced. Note any criteria that are not fully met — these inform the `failed` and `needed` fields.

### Step 2 — Write handoff_summary to the loop file

Update `handoff_summary` in the loop file's YAML frontmatter:

```yaml
handoff_summary:
  done: "[What was completed — files written, tests passing, decisions made. One sentence.]"
  failed: "[What failed and why, with specific reference. One sentence. Empty string if nothing failed.]"
  needed: "[Precise action the next loop should start with. One sentence. Empty string if fully complete.]"
```

Rules:
- `done` must reference artefacts, not effort ("4 schema documents created in core/schemas/" not "worked on schemas")
- `failed` must give root cause, not just symptom ("validation failed due to missing required field X" not "validation failed")
- `needed` must be a specific first action ("Run plan-todos on loop-003 to populate todos" not "continue Phase 1")
- All three fields must be populated before writing `loop-complete.json`

### Step 3 — Write loop-complete.json

Write `loop-complete.json` to the state directory:

```json
{
  "loop_name": "[name from loop frontmatter]",
  "loop_file": "[path to loop plan file]",
  "status": "completed",
  "todos_done": [count of todos with status: completed],
  "todos_failed": [count of todos with status: cancelled],
  "completed_at": "[ISO 8601 timestamp]",
  "handoff": {
    "done": "[same value as handoff_summary.done]",
    "failed": "[same value as handoff_summary.failed]",
    "needed": "[same value as handoff_summary.needed]"
  },
  "duration_seconds": [elapsed seconds since worker start, optional]
}
```

The `status` enum: `completed` (all todos done), `partial` (some cancelled, `on_max_iterations: checkpoint`), `failed` (escalate or rollback triggered).

The formal JSON Schema is at `core/state/loop-complete.schema.json`.

### Step 4 — Closing git checkpoint

```bash
git add -A && git commit -m "complete: [loop_name] — [one-line summary of what changed]"
```

### Step 5 — Return

Log the completion event (platform adapter defines logging mechanism).

Return to the main thread. Do not advance to the next loop — that is the main thread's decision.

---

## What the Worker Does NOT Do

| Action | Why Not |
|--------|---------|
| Plan or restructure loops | Orchestrator and planning skills handle this |
| Spawn further subagents | The worker is itself a subagent; subagents cannot spawn subagents. The `agent:` field in todos is planning-time metadata, not an execution-time directive |
| Decide whether to continue to the next loop | Main thread reads loop-complete.json and decides |
| Modify the loop file beyond `status` fields and `handoff_summary` | Stays within its execution lane |
| Load skills outside of the per-todo injection cycle | Prevents context pollution across tasks |

---

## Inputs

| Input | Location | Used For |
|-------|----------|----------|
| `loop-ready.json` | State directory | Assignment: loop file path, todos count, handoff context |
| Loop plan file | `plans/` directory | Todos, success criteria, max_iterations, on_max_iterations |
| SKILL.md files | Skills directory | Targeted injection; one skill per todo, per cycle |

---

## Output Contract

`loop-complete.json` written to the state directory with the schema described in the Completion Protocol above.

The formal JSON Schema is at `core/state/loop-complete.schema.json`.

Updated `handoff_summary` written to the loop file's YAML frontmatter.

---

## Platform Adapter Notes

Platform adapters must specify:
- The model to use for this role (Sonnet recommended as default; Haiku for low-complexity todos)
- The tool capabilities granted (read, write, edit files; bash for git; glob; session task tracking)
- The state directory path
- The skills directory path (for the skill injection protocol)
- The session task tracking mechanism (the format for registering and updating todos)

### Model Tier Selection

The worker's default model tier is **Sonnet**. This provides reliable compositional reasoning for multi-file edits, domain-specific skill application, and verification tasks.

**Haiku** is appropriate when a todo's `complexity` field is `low`:
- Single-file edits with clear specifications
- Running a command and checking its exit code
- Copying or moving files
- Simple text substitutions or template fills

**Sonnet** (default) is appropriate for `medium` and `high` complexity:
- Multi-file coordinated changes
- Domain-specific reasoning (statistical analysis, schema design)
- Tasks requiring skill injection where the skill's instructions must be interpreted
- Any task where the outcome verification requires judgement

**Critical**: The targeted skill injection protocol is mandatory. An adapter that loads all skills at startup or loads no skills at all is not a compliant implementation. The per-todo load/unload cycle is the behaviour specification, not a recommendation.
