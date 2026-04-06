---
description: Verify which model is currently running and show the expected model tier for each component in the planning system. Run at any point during execution to confirm model routing is correct.
allowed-tools: Read, Bash
---

# /model-check

Report the current model and verify model assignments across the planning system.

## Skill & Agent Path Resolution

Resolve all `.claude/skills/` and `.claude/agents/` references in this order:
1. **Project-local** — `.claude/skills/<name>/` (preferred)
2. **Global fallback** — `~/.claude/skills/<name>/` (used when local copy absent)

For Glob operations, search both locations and merge results; local takes precedence
for any duplicate skill/agent names.

## Steps

### 1. Print current model

```bash
echo "Current model: ${CLAUDE_MODEL:-unknown}"
```

### 2. Read expected model assignments

Read each skill and agent frontmatter to extract the `model:` field:

```
Read .claude/skills/phase-plan-creator/SKILL.md        → model field
Read .claude/skills/ralph-loop-planner/SKILL.md        → model field
Read .claude/skills/plan-todos/SKILL.md                → model field
Read .claude/skills/plan-skill-identification/SKILL.md → model field
Read .claude/skills/plan-subagent-identification/SKILL.md → model field
Read .claude/agents/ralph-orchestrator.md              → model field
Read .claude/agents/ralph-loop-worker.md               → model field
Read .claude/agents/analysis-worker.md                 → model field
```

### 3. Show recent audit log (if present)

```bash
tail -20 .claude/logs/execution.log 2>/dev/null || echo "(no execution log yet)"
```

### 4. Print summary table

```
Component                        Expected    Frontmatter
─────────────────────────────────────────────────────────
phase-plan-creator (skill)       opus        [read value]
ralph-loop-planner (skill)       opus        [read value]
plan-todos (skill)               opus        [read value]
plan-skill-identification (skill) opus       [read value]
plan-subagent-identification (skill) opus    [read value]
─────────────────────────────────────────────────────────
ralph-orchestrator (agent)       sonnet      [read value]
─────────────────────────────────────────────────────────
ralph-loop-worker (agent)        haiku       [read value]
analysis-worker (agent)          haiku       [read value]
─────────────────────────────────────────────────────────
Current context                  —           ${CLAUDE_MODEL:-unknown}
```

Flag any row where the frontmatter value does not match the expected tier.

## Notes

- Environment variable `$CLAUDE_MODEL` may show `unknown` if the hook shell context differs from the main session — this is a known limitation documented in `check-execution.md` Check 1
- If a skill has no `model:` field, it inherits from the calling context — flag this as a gap
