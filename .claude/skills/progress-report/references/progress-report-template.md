# Progress Report Template

Reference template for the `progress-report` skill. Shows field descriptions and
examples of good vs weak summaries.

---

## Full Template

```markdown
# Progress Report: [Programme/Phase Name]
Generated: [ISO timestamp — e.g. 2026-03-20T14:32:00]

## Summary
- **Phase**: [N] — [phase name] ([active | complete | pending])
- **Loops**: [completed]/[total] complete
- **Todos**: [completed]/[total] complete ([failed] failed, [cancelled] cancelled)

## Completed Loops
| Loop | Task | Todos | Status | Done |
|------|------|-------|--------|------|
| ralph-loop-001 | schema-definitions | 4/4 | completed | 3 schema files created in core/schemas/ |
| ralph-loop-002 | agent-roles | 3/3 | completed | orchestrator.md and worker.md written |

## Current Loop
- **Loop**: ralph-loop-003 — state-bus-implementation
- **Status**: in_progress
- **Todos**: 2/4 completed
- **Last handoff needed**: Create loop-ready.schema.json before starting state bus tests

## Failed / Blocked Items
| Loop | Todo | Issue |
|------|------|-------|
| ralph-loop-002 | Write analysis-worker.md | File existed from prior run, overwrite skipped |

## Git Trail
| Time | Commit | Summary |
|------|--------|---------|
| 2026-03-20 09:14 | a3f2b1c | complete: ralph-loop-001 - schema files created |
| 2026-03-20 11:47 | d9e8f0a | complete: ralph-loop-002 - agent roles defined |
| 2026-03-20 14:22 | c7b6a5d | checkpoint: before next-loop cycle |

## Decisions & Learnings
- Decided to split handoff schema from loop schema after loop-001 revealed they serve different consumers
- Worker must not read its own loop-complete.json to avoid circular validation

## Next Steps
1. Complete remaining todos in ralph-loop-003 (state bus implementation)
2. Run /next-loop to advance to ralph-loop-004 once loop-003 completes
```

---

## Field Descriptions

### Summary section

| Field | What to extract | Source |
|-------|----------------|--------|
| Phase | Phase number + name from YAML frontmatter `phase_name` | `phase-N.md` |
| Phase status | `active` if current, `complete` if all loops done, `pending` if not started | `CLAUDE.md` Planning State |
| Loops completed | Count loops where all todos are `completed` or `cancelled` | Loop files |
| Todos completed | Count todos with `status: completed` across all loop files | Loop files |
| Todos failed | Count todos with `status: cancelled` that have a failed reason | Loop files + handoffs |

### Completed Loops table

| Column | What to show |
|--------|-------------|
| Loop | Loop name from YAML `name:` field |
| Task | `task_name:` field from loop YAML |
| Todos | `completed/total` counts |
| Status | `completed` / `partial` / `failed` |
| Done | One-sentence `handoff_summary.done` — truncate at 80 chars if longer |

### Current Loop section

Only include if there is an `in_progress` or `pending` loop that has had at least one
todo completed. If no loop has started, omit this section.

### Failed / Blocked Items table

Include a row for:
- Any todo with `status: cancelled` where `handoff_summary.failed` is non-empty
- Any loop where `status: failed` or `status: partial`

Omit the entire section if nothing failed.

### Git Trail table

- Include `complete:` and `checkpoint:` commits only
- Show at most 20 rows
- Timestamps: `YYYY-MM-DD HH:MM` format (local time)
- If no matching commits exist, write: `No complete: or checkpoint: commits found yet.`

### Next Steps

Derive from:
1. The most recent loop's `handoff_summary.needed` field
2. Any remaining `pending` todos in the current loop
3. The next unstarted loop name (from loop file order)

---

## Good vs Weak Summaries

### Done field — handoff_summary.done

**Good**: `3 schema files created in core/schemas/ — phase-plan, ralph-loop, and handoff`
- Artefact-focused, names the output, specific count

**Weak**: `Completed the schema work as planned`
- No artefacts named, vague, not verifiable

### Failed field — handoff_summary.failed

**Good**: `analysis-worker.md write skipped — file existed from prior partial run, manual merge needed`
- Names the file, explains why, says what action is needed

**Weak**: `Some files were not created`
- Which files? Why? What now?

### Needed field — handoff_summary.needed

**Good**: `Create loop-ready.schema.json before starting state bus integration tests`
- Specific file, specific gate, actionable first step

**Weak**: `Continue with the next part`
- No specifics, not actionable

---

## Parsing Notes

### Extracting todo counts from YAML frontmatter

Loop files have YAML frontmatter with a `todos[]` array. Each todo has a `status:` field.
Count occurrences of each status value:

```yaml
todos:
  - id: todo-001
    status: completed   # ← count this
  - id: todo-002
    status: pending     # ← count this
```

Use `grep "status:" <loop-file> | sort | uniq -c` as a quick count, or parse the YAML
properly if the loop file is complex.

### Extracting git timestamps

```bash
git log --oneline --format="%ai %h %s" --grep="complete:" --grep="checkpoint:" -20
```

The `%ai` format gives ISO 8601 timestamps. Truncate to `YYYY-MM-DD HH:MM` for display.
