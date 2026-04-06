---
description: Run the gate review sub-phase. Spawns configured gate agents sequentially, reads verdicts, aggregates pass/fail, and writes gate_pass or gate_fail to history.jsonl.
allowed-tools: Read, Write, Glob, Bash, Agent
argument-hint: "[--phase N] [--agents code-review-agent,phase-goals-agent,security-agent,test-agent]"
---

# /run-gate

Run the gate review sub-phase for the current (or specified) phase. Gate agents evaluate
all loop outputs against the phase success criteria and return a structured verdict.

## Steps

### 1. Resolve current phase

Parse `$ARGUMENTS` for `--phase N` argument. If provided, use `N` as the current phase number.

Otherwise read `CLAUDE.md` and extract the `## Planning State` section. Find the current
active phase (the phase with `status: in_progress` or the most recent phase entry).

Print: `→ Gate review: Phase [N]`

### 2. Verify all loops in the phase are complete

Read the active loop file for this phase from `.claude/plans/`. Check all todos across
all loops in the file:

```bash
grep -c "status: pending\|status: in_progress" .claude/plans/*.md 2>/dev/null || echo "0"
```

If any todo has `status: pending` or `status: in_progress`:
print `✗ Cannot run gate: [N] todos are not yet completed. Finish all loops first.` and stop.

Print: `✓ All loops complete — proceeding to gate review.`

### 3. Determine which gate agents to run

Parse `$ARGUMENTS` for `--agents comma-separated-list`. If provided, split on commas to
get the agent list.

Default agent list (if `--agents` not provided):
- `code-review-agent`
- `phase-goals-agent`

Print:
```
Gate agents:
  [bullet list of agents]
```

### 4. Create gate-verdicts directory

```bash
mkdir -p plans/gate-verdicts
```

### 5. Create gate-review-mode sentinel

```bash
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > .claude/state/gate-review-mode
```

Print: `→ Gate review mode active (write access restricted to plans/gate-verdicts/)`

### 6. Determine attempt number

Count existing verdict files for this phase to determine the attempt number:

```bash
ls plans/gate-verdicts/phase-[N]-attempt-*.json 2>/dev/null | wc -l
```

If `N` existing files found, `attempt = floor(N / agent_count) + 1` where `agent_count`
is the number of agents being run. If no existing files, `attempt = 1`.

### 7. Spawn gate agents sequentially

For **each agent** in the agent list, one at a time (never concurrently):

Spawn the agent via the Agent tool with the following prompt:

```
You are [agent-name] performing a gate review.

Phase: [N]
Attempt: [attempt]
Phase plan: plans/phase-[N]-*.md (read to understand success criteria)
Loop files: .claude/plans/ (read all loop files for this phase)
Prior context: [handoff summaries from all loops in this phase]

Your verdict output path: plans/gate-verdicts/phase-[N]-attempt-[attempt]-[agent-name].json

Read the phase plan, all loop outputs, and evaluate whether the phase success criteria
have been met. Write your verdict to the output path. Then return.
```

Wait for each agent to complete before spawning the next.

Print after each: `  ✓ [agent-name] verdict written`

### 8. Remove gate-review-mode sentinel

```bash
rm .claude/state/gate-review-mode
```

Print: `→ Gate review mode deactivated.`

### 9. Aggregate verdicts

Read all verdict files written in this attempt:

```bash
ls plans/gate-verdicts/phase-[N]-attempt-[attempt]-*.json
```

For each verdict file, read the `verdict` field. Aggregate:
- **ALL verdicts = `"pass"`** → gate passes
- **ANY verdict = `"fail"`** → gate fails

Collect:
- `verdict_files`: list of paths to all verdict files
- `failing_agent`: name of first failing agent (if gate fails)
- `loops_to_revert`: list from failing agent's verdict (if gate fails)

### 10. Append event to history.jsonl

If gate **passes**:

```bash
echo '{"event":"gate_pass","phase":"phase-[N]","attempt":[attempt],"timestamp":"[ISO timestamp]","agents":[agent list JSON array],"verdict_files":[verdict paths JSON array]}' >> .claude/state/history.jsonl
```

If gate **fails**:

```bash
echo '{"event":"gate_fail","phase":"phase-[N]","attempt":[attempt],"timestamp":"[ISO timestamp]","agent":"[failing-agent]","verdict_file":"[failing-verdict-path]","loops_to_revert":[loops JSON array]}' >> .claude/state/history.jsonl
```

### 10. Print summary

If gate **passes**:
```
✓ Gate PASSED — Phase [N] approved.
  Agents:  [comma-separated list]
  Attempt: [N]
  Verdicts: [verdict file paths]

Run /next-phase to advance.
```

If gate **fails**:
```
✗ Gate FAILED — Phase [N] did not pass.
  Failed agent: [agent-name]
  Verdict:      [verdict file path]
  Attempt:      [N]

Run /next-phase to create versioned retry files and begin retry.
```

## Notes

- Gate agents are spawned sequentially — never concurrently — to avoid verdict file conflicts
- The `gate-review-mode` sentinel restricts write access to `plans/gate-verdicts/` during review
- Verdict files are immutable: one file per agent per attempt, never overwritten
- Default agents (`code-review-agent`, `phase-goals-agent`) can be overridden with `--agents`
- Run `/run-closeout` after the final phase passes to produce the programme narrative
