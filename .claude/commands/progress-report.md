---
description: Generate a structured progress report from plan files, handoff summaries, and git history.
allowed-tools: Read, Glob, Grep, Bash
argument-hint: "[--phase N]"
---

# /progress-report

Retrospective synthesis of programme progress. Reads existing artefacts — plan files,
loop handoff summaries, todo statuses, and git history — and produces a structured
markdown report. Does not modify any files.

## Steps

### 1. Resolve skill path

Check for the progress-report skill in order:
1. `.claude/skills/progress-report/SKILL.md`
2. `~/.claude/skills/progress-report/SKILL.md`

Load the skill if found. If not found, proceed using the steps below directly.

### 2. Parse arguments

If `$ARGUMENTS` contains `--phase N`, scope all data gathering to phase N only.
Otherwise report on the full programme.

### 3. Follow the skill's Process steps

The skill defines data sources and process steps. Follow them in order:

1. Glob `.claude/plans/phase-*.md` for phase plans
2. Glob `.claude/plans/phase-*-ralph-loops.md` and `.claude/plans/ralph-loop-*.md` for loop files
3. Read `.claude/plans/PLANS-INDEX.md` if it exists
4. Read `CLAUDE.md` for current phase/loop pointer and planning state
5. Run `git log --oneline --grep="complete:" --grep="checkpoint:" -20` for the commit trail
6. Read `.claude/state/loop-complete.json` for the most recent loop result
7. Read `.claude/logs/execution.log` (last 50 lines) if the file exists

### 4. Compile and print report

Produce the structured report defined in the skill's Output Format section.
Print to the conversation — do not save to file unless the user asks.

### 5. Optional: save report

If the user asks to save the report:

```bash
# Save to plans directory with today's date
```

Write to `.claude/plans/progress-report-$(date +%Y-%m-%d).md`

## Notes

- This command is read-only — it never modifies plan files or state
- The git trail provides timestamps that plan files themselves do not carry
- Run after `/next-loop --auto` to see a summary of what the autonomous run accomplished
- Use `--phase N` to scope the report when working across a multi-phase programme
