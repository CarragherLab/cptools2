---
description: Full diagnostic of whether ralph loops, worker agents, and todos are executing correctly. Reads execution log, plan file frontmatter, and git history to build a complete picture. Run after /next-loop completes or if something seems wrong.
allowed-tools: Read, Glob, Bash
---

# /check-execution

Systematic diagnostic across six areas. Run each check and report findings.

---

## Check 1: Hook Environment Variables

Confirms whether execution log entries contain real model names or `unknown` placeholders.

```bash
grep -E "model:|WORKER" .claude/logs/execution.log 2>/dev/null | head -20 || echo "(no execution log found)"
```

**Healthy**: model names like `claude-sonnet-4-6`, agent names like `ralph-loop-worker`
**Problem**: `model:unknown` or `WORKER START: unknown` — environment variables not set in hook shell; model routing cannot be confirmed from logs alone.

---

## Check 2: Worker Agent Spawning

Did Haiku workers actually run, or did the orchestrator do everything?

```bash
echo "Worker spawns:"
grep "WORKER START" .claude/logs/execution.log 2>/dev/null | wc -l

echo "Worker names:"
grep "WORKER START" .claude/logs/execution.log 2>/dev/null

echo "Todos with agent assignments:"
grep "agent:" .claude/plans/*.md 2>/dev/null | grep -v ": NA" | grep -v "^--"
```

**Healthy**: Worker spawn count matches number of todos with non-NA agent assignments
**Problem**: 0 workers → all execution running inline; worker delegation not working

---

## Check 3: Todo Progression

Did todos move through states, or are they stuck?

```bash
echo "Current todo statuses:"
grep -E "^\s+status:" .claude/plans/*.md 2>/dev/null | sort | uniq -c

echo ""
echo "Todos still pending:"
grep -B3 "status: pending" .claude/plans/*.md 2>/dev/null | grep "content:"

echo ""
echo "Todos completed:"
grep -B3 "status: completed" .claude/plans/*.md 2>/dev/null | grep "content:"
```

**Healthy**: Mix of completed and pending matching execution progress
**Problem**: All pending → nothing executed; All completed instantly → status updated without real work

---

## Check 4: Handoff Summary Population

Was the loop wrap-up step executed?

```bash
grep -A5 "handoff_summary:" .claude/plans/*.md 2>/dev/null
```

**Healthy**: `done:` fields contain real sentences describing completed work
**Problem**: All empty strings → worker skipped the finalisation step; handoffs unreliable

---

## Check 5: Git Checkpoint Trail

Did the loop boundary commits land?

```bash
git log --oneline | grep -E "checkpoint:|complete:" | head -20
```

**Healthy**: Pairs of `checkpoint: before ralph-loop-NNN` and `complete: ralph-loop-NNN`
**Problem**: Checkpoints without completions → loops starting but not finishing cleanly
**Problem**: No checkpoints at all → /next-loop failed before reaching the git step

---

## Check 6: File Write Activity

Were actual output files produced, or only plan file status updates?

```bash
echo "Files written during session:"
grep "WRITE:\|EDIT:" .claude/logs/execution.log 2>/dev/null | grep -v "\.claude/plans\|\.claude/logs\|\.claude/state" | head -30

echo ""
echo "Plan file edits (expected — status updates and handoff writes):"
grep "WRITE:\|EDIT:" .claude/logs/execution.log 2>/dev/null | grep "plans/" | wc -l
```

**Healthy**: Output files appear alongside plan edits
**Problem**: Only plan file edits → agents updated status fields but produced no real outputs

---

## Summary Diagnosis

After running all six checks, report:

```
EXECUTION HEALTH REPORT
────────────────────────────────────────────
Hook env vars:      [real values | unknown — blind]
Workers spawned:    [N workers | 0 — inline only]
Todo progression:   [N/N complete | stuck at X]
Handoff summaries:  [populated | empty]
Git checkpoints:    [N checkpoint+complete pairs | missing]
Output files:       [files produced | plan edits only]
────────────────────────────────────────────
Overall:  HEALTHY / PARTIAL / NOT EXECUTING
```

Flag any item that needs addressing before the next loop runs.
