---
name: progress-report
description: "Generate a structured progress report by reading plan files, loop handoff summaries, todo statuses, and git commit history. Use when reviewing programme progress, preparing status updates, or auditing completed work. Triggers: progress report, status report, what happened, show progress, review progress, audit."
---

# Progress Report Skill

## When to Use

- Reviewing progress after one or more loops have executed
- Preparing a status update for stakeholders
- Auditing what was accomplished across a phase or programme
- After an overnight `/next-loop --auto` run to see what happened
- Understanding where a programme stands before resuming work

## Data Sources

The skill reads these existing artefacts. No new logging infrastructure is required.

1. **Plan files** (`Glob .claude/plans/phase-*.md`) — phase objectives, scope, success criteria
2. **Loop files** (`Glob .claude/plans/phase-*-ralph-loops.md` or `Glob .claude/plans/ralph-loop-*.md`) — todo statuses, handoff summaries, max_iterations
3. **PLANS-INDEX.md** (`Read .claude/plans/PLANS-INDEX.md`) — phase/loop status overview if present
4. **CLAUDE.md Planning State** (`Read CLAUDE.md`) — current phase/loop pointer, last handoff
5. **Git history** (`git log --oneline --grep="complete:" --grep="checkpoint:" -20`) — commit trail of loop completions with timestamps
6. **State files** (`Read .claude/state/loop-complete.json`) — most recent loop result
7. **Execution log** (`Read .claude/logs/execution.log`) — agent spawning, model routing, tool usage (last 50 lines, if available)

## Process

1. Glob for all plan and loop files in `.claude/plans/`
2. For each phase plan: extract objective, status, and success criteria from YAML frontmatter
3. For each loop file: parse YAML frontmatter to extract:
   - Todo statuses: count `pending`, `in_progress`, `completed`, `cancelled`
   - Handoff summaries: `done`, `failed`, `needed` fields
   - `max_iterations` and `on_max_iterations` values
4. Count aggregates:
   - Total loops, completed loops (all todos completed or cancelled)
   - Total todos, completed todos, failed/cancelled todos
5. Run `git log --oneline --grep="complete:" --grep="checkpoint:" -20` for timestamps and summaries
6. Read `CLAUDE.md` for current position (which phase/loop is active)
7. Read `.claude/state/loop-complete.json` for the most recent loop result
8. Compile into the output format below

## Output Format

```markdown
# Progress Report: [Programme/Phase Name]
Generated: [timestamp]

## Summary
- **Phase**: [N] — [name] ([status: active/complete/pending])
- **Loops**: [completed]/[total] complete
- **Todos**: [completed]/[total] complete ([failed] failed, [cancelled] cancelled)

## Completed Loops
| Loop | Task | Todos | Status | Done |
|------|------|-------|--------|------|
| ralph-loop-001 | [task_name] | 4/4 | completed | [handoff.done summary] |
| ralph-loop-002 | [task_name] | 3/3 | completed | [handoff.done summary] |

## Current Loop
- **Loop**: [name] — [task_name]
- **Status**: [in_progress/pending]
- **Todos**: [completed]/[total]
- **Last handoff needed**: [handoff.needed from prior loop]

(Omit this section if no loop is currently active.)

## Failed / Blocked Items
| Loop | Todo | Issue |
|------|------|-------|
| [loop] | [todo content] | [handoff.failed or todo with cancelled/failed status] |

(Omit this section entirely if nothing failed.)

## Git Trail
| Time | Commit | Summary |
|------|--------|---------|
| [timestamp] | [short hash] | [commit message] |

## Decisions & Learnings
[Extract from CLAUDE.md "Learnings & Decisions" section if populated. Omit if absent.]

## Next Steps
[From the most recent handoff.needed field + current loop's pending todos]
```

## Notes

- The report is read-only synthesis — it never modifies plan files or state
- For multi-phase programmes, pass `--phase N` to scope to a single phase, or omit for the full programme
- The git trail provides timestamps that plan files themselves do not carry
- If `.claude/plans/` is empty or no loops have run, report that no data is available yet
- Omit table rows and sections that have no data rather than showing empty tables
