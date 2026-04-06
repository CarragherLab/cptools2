---
description: Advance to the next phase. Runs gate review first; on pass advances. Use --auto to chain across phase boundaries — gate review → plan next phase → execute loops → repeat until programme complete or failure.
allowed-tools: Read, Write, Glob, Bash, Edit, TodoWrite, Agent
argument-hint: "[--auto] [--skip-gate] [--force]"
---

# /next-phase

Advance from the current phase to the next. By default this command runs the full gate
review first. On gate pass, the current phase is marked complete and you are prompted to
plan the next phase. On gate fail, versioned retry files are created with injected failure
context so the retry loop has full information.

Use `--auto` to chain across phase boundaries autonomously: gate review → plan next phase →
execute all loops → gate review → repeat until the programme completes or a gate/loop fails.

## Steps

### 1. Read current phase

Read `CLAUDE.md` and extract the `## Planning State` section. Identify:
- Current phase number `N`
- Current loop file path
- Current phase status

If the current phase already has `status: complete`:
print `✓ Phase [N] is already complete. Run /new-phase to plan Phase [N+1].` and stop.

Print: `→ Current phase: Phase [N]`

### 2. Parse flags

Check `$ARGUMENTS` for:
- `--auto`: autonomous phase chaining — after gate pass, plan and execute the next phase automatically
- `--skip-gate`: bypass gate review entirely, treat as pass
- `--force`: if gate fails, advance anyway (with a warning)

If `--auto` is set:
- Set `AUTO_PHASE_MODE = true`
- Print: `Autonomous phase mode: will chain phases until programme complete or failure.`

If `--skip-gate` is set, skip Steps 3–5 and proceed directly to Step 6 (gate pass path).

If `--force` is set, note it for use in Step 7.

### 3. Run gate review

Run the full gate review inline (same logic as `/run-gate`):

**3a. Verify all loops complete**

```bash
grep -c "status: pending\|status: in_progress" .claude/plans/*.md 2>/dev/null || echo "0"
```

If any incomplete todos found:
print `✗ Gate review blocked: [N] todos are not yet completed. Finish all loops first.`
and stop.

**3b. Determine agents and attempt number**

Default gate agents: `code-review-agent`, `phase-goals-agent`

Count existing verdict files to find attempt number:

```bash
ls plans/gate-verdicts/phase-[N]-attempt-*.json 2>/dev/null | wc -l
```

Set `attempt = (existing_count / agent_count) + 1`, minimum 1.

**3c. Create gate-verdicts directory and sentinel**

```bash
mkdir -p plans/gate-verdicts
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > .claude/state/gate-review-mode
```

**3d. Spawn each gate agent sequentially**

For each agent, spawn via Agent tool with the prompt:

```
You are [agent-name] performing a gate review.

Phase: [N]
Attempt: [attempt]
Phase plan: plans/phase-[N]-*.md
Loop files: .claude/plans/ (all loop files for this phase)

Your verdict output path: plans/gate-verdicts/phase-[N]-attempt-[attempt]-[agent-name].json

Evaluate whether the phase success criteria have been met. Write your verdict to the
output path following gate-verdict.schema.json. Then return.
```

Wait for each agent before spawning the next.

**3e. Remove sentinel**

```bash
rm .claude/state/gate-review-mode
```

**3f. Aggregate verdicts**

Read all verdict files for this phase+attempt. Check each `verdict` field:
- ALL `"pass"` → gate passes; set `GATE_RESULT = pass`
- ANY `"fail"` → gate fails; set `GATE_RESULT = fail`, note the failing agent and verdict file

### 4. Append gate event to history.jsonl

If `GATE_RESULT = pass`:

```bash
echo '{"event":"gate_pass","phase":"phase-[N]","attempt":[attempt],"timestamp":"[ISO timestamp]","agents":["code-review-agent","phase-goals-agent"],"verdict_files":["plans/gate-verdicts/phase-[N]-attempt-[attempt]-code-review-agent.json","plans/gate-verdicts/phase-[N]-attempt-[attempt]-phase-goals-agent.json"]}' >> .claude/state/history.jsonl
```

If `GATE_RESULT = fail`:

```bash
echo '{"event":"gate_fail","phase":"phase-[N]","attempt":[attempt],"timestamp":"[ISO timestamp]","agent":"[failing-agent]","verdict_file":"[failing-verdict-path]","loops_to_revert":[loops JSON array]}' >> .claude/state/history.jsonl
```

### 5. Handle --force flag on gate failure

If `GATE_RESULT = fail` and `--force` was provided:

Print:
```
⚠ WARNING: Gate review FAILED but --force was used. Advancing anyway.
  Failing agent: [agent-name]
  Verdict file:  [path]
  The failure context has NOT been preserved in versioned retry files.
  This is irreversible — the gate failure will be recorded in history.jsonl.
```

Set `GATE_RESULT = pass` and continue to Step 6.

### 6. Gate PASS — advance to next phase

Update `CLAUDE.md` `## Planning State`:
- Set current phase `status: complete`
- Set `phase: [N+1]` as the next active phase (with `status: not_started`)

```bash
git add -A && git commit -m "complete: phase-[N] gate passed — advancing to phase-[N+1]"
```

Print:
```
✓ Phase [N] gate PASSED.
  Status updated in CLAUDE.md.
```

If `AUTO_PHASE_MODE = false`: print `Run /new-phase to plan Phase [N+1].` and stop.

If `AUTO_PHASE_MODE = true`: proceed to Step 8 (auto-continuation).

---

### 8. Auto-continuation (--auto only)

This step runs only when `AUTO_PHASE_MODE = true`. It chains phase planning, loop execution,
and gate review into a continuous autonomous pipeline.

#### 8a. Check for next phase

Read `plans/PLANS-INDEX.md` and `plans/master-plan.md` (if they exist) to determine if more
phases are planned:

- If a master plan exists with a defined Phase [N+1] description: use that description as input
- If PLANS-INDEX.md lists a Phase [N+1] with status `not_started`: use its name as input
- If no more phases are defined anywhere:
  print `Programme complete. All planned phases passed gate review.` and stop
- If uncertain (no master plan, no pre-defined phases):
  print `Phase [N] complete. No next phase defined in master plan. Run /new-phase to plan manually.` and stop

#### 8b. Plan the next phase (inline planning pipeline)

Run the full planning pipeline for Phase [N+1] (same steps as `/new-phase`):

1. Auto-increment phase number: `N+1`
2. Load `.claude/skills/phase-plan-creator/SKILL.md` and follow its Process section
   - Use the description from Step 8a as input
   - Save to `plans/phase-[N+1].md`
3. Load `.claude/skills/ralph-loop-planner/SKILL.md` and follow its Process section
   - Read the phase plan just created
   - Save to `plans/phase-[N+1]-ralph-loops.md`
4. Load `.claude/skills/plan-todos/SKILL.md` and follow its Process section
   - Populate `todos[]` for every loop
5. Load `.claude/skills/plan-skill-identification/SKILL.md` and follow its Process section
   - Glob `.claude/skills/*/SKILL.md` to discover available skills
   - Assign `skill:` fields in-place
6. Load `.claude/skills/plan-subagent-identification/SKILL.md` and follow its Process section
   - Glob `.claude/agents/*.md` to discover available agents
   - Assign `agent:` fields in-place
7. Update `CLAUDE.md` `## Planning State` with the new phase and first loop

```bash
git add -A && git commit -m "plan: phase-[N+1] — [phase name], [loop count] loops, [todo count] todos"
```

Print:
```
✓ Phase [N+1] planned: [phase name]
  Loops: [count]
  Todos: [count]
  Beginning execution...
```

#### 8c. Execute all loops (inline loop chaining)

Run the loop execution cycle for Phase [N+1] (same logic as `/next-loop --auto`):

**For each pending loop:**

1. Git checkpoint:
   ```bash
   git add -A && git commit -m "checkpoint: before next-loop cycle" 2>/dev/null || true
   ```

2. Spawn `ralph-orchestrator` (Sonnet):
   - Identifies next pending loop
   - Populates todos if needed
   - Writes `.claude/state/loop-ready.json`
   - Returns

3. Read `.claude/state/loop-ready.json`
   - If `status: all_complete`: all loops done, proceed to Step 8d
   - Otherwise: print loop summary

4. Spawn `ralph-loop-worker` (Sonnet):
   - Reads `loop-ready.json` for assignment
   - Executes all todos with targeted skill injection
   - Writes `.claude/state/loop-complete.json`
   - Returns

5. Read `.claude/state/loop-complete.json`

6. Update `CLAUDE.md` `## Planning State`:
   - Advance loop pointer
   - Increment `todos_done`

7. Git commit:
   ```bash
   git add -A && git commit -m "complete: [loop_name] — [handoff.done]"
   ```

8. Check continuation:
   - If loop `status: failed`: **STOP**. Print failure details and exit auto mode.
   - If more loops pending: return to sub-step 1 (next loop)
   - If all loops complete: proceed to Step 8d

#### 8d. Return to gate review

All loops in Phase [N+1] are complete. Increment `N` and return to **Step 3** (gate review)
to evaluate this phase before advancing further.

Print:
```
✓ Phase [N+1] loops complete. Running gate review...
```

### 7. Gate FAIL — create versioned retry files

**7a. Determine next attempt number**

`next_attempt = attempt + 1`

**7b. Read failure context from verdict**

Read the failing agent's verdict file. Extract:
- `findings`: list of issues with severity, location, description, evidence
- `loops_to_revert`: list of loop names that must be revisited
- `failure_notes`: prose summary of what went wrong

**7c. Create versioned loop file**

Copy the current active loop file to a versioned path:

```bash
cp plans/phase-[N]-ralph-loops.md plans/phase-[N]-ralph-loops-v[next_attempt].md
```

**7d. Inject gate_failure_context block**

Edit the new versioned file (`plans/phase-[N]-ralph-loops-v[next_attempt].md`). In the
YAML frontmatter of each loop listed in `loops_to_revert`, add a `gate_failure_context`
block:

```yaml
gate_failure_context:
  attempt: [attempt]
  verdict_file: "plans/gate-verdicts/phase-[N]-attempt-[attempt]-[agent-name].json"
  summary: "[failure_notes from verdict]"
  loops_reverted:
    - loop: "[loop-name]"
      reason: "[relevant finding description for this loop]"
  do_not_repeat:
    - "[finding description 1]"
    - "[finding description 2]"
```

**7e. Reset todo statuses in versioned file**

In the versioned file, for each loop listed in `loops_to_revert`, change all
`status: completed` todos to `status: pending` so they are re-executed on retry.

**7f. Freeze the original loop file**

In the original loop file (`plans/phase-[N]-ralph-loops.md`), change any
`status: pending` or `status: in_progress` todos to `status: frozen` to prevent
the original from being modified during retry.

**7g. Update PLANS-INDEX.md**

Read `plans/PLANS-INDEX.md`. Update the Phase [N] entry:
- Add versioned file as the new active file
- Set attempt number to `next_attempt`
- Note: original file is now frozen

If `PLANS-INDEX.md` does not exist, create it with a Phase [N] entry.

**7h. Append phase_retry event to history.jsonl**

```bash
echo '{"event":"phase_retry","phase":"phase-[N]","attempt":[next_attempt],"timestamp":"[ISO timestamp]","new_loop_file":"plans/phase-[N]-ralph-loops-v[next_attempt].md","original_loop_file":"plans/phase-[N]-ralph-loops.md"}' >> .claude/state/history.jsonl
```

**7i. Git commit**

```bash
git add -A && git commit -m "retry: phase-[N] attempt [next_attempt] — gate failed on [failing-agent]"
```

**7j. Print failure summary**

```
✗ Phase [N] gate FAILED (attempt [attempt]).
  Failed agent:  [agent-name]
  Verdict file:  [path]
  Issues found:  [count]

Versioned retry files created:
  New loop file: plans/phase-[N]-ralph-loops-v[next_attempt].md
  Failure context injected into: [list of affected loops]
  Original file frozen: plans/phase-[N]-ralph-loops.md

Run /next-loop to begin Phase [N] retry (attempt [next_attempt]).
```

## Notes

- Gate review is mandatory by default — use `--skip-gate` only when the review has already
  been run separately via `/run-gate`
- `--force` overrides gate failure but does not suppress the `gate_fail` history event
- Versioned retry files preserve all completed work; only `loops_to_revert` are reset
- The `do_not_repeat` field in `gate_failure_context` is carried forward through all retries
- After all phases complete, run `/run-closeout` to produce the programme narrative

### Auto mode stop conditions

| Condition | Behaviour |
|-----------|-----------|
| Gate FAIL | Create versioned retry, STOP (manual review required) |
| Loop FAIL during execution | STOP with loop failure details |
| All planned phases complete | STOP with "Programme complete" message |
| No master plan / no next phase description | STOP, prompt user to run `/new-phase` manually |
| `--skip-gate` combined with `--auto` | Gates skipped, phases advance without review |
| `--force` combined with `--auto` | Gate failures logged but do not stop progression |
