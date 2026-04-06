# Phase Plan Schema

A phase plan defines **what** needs to be accomplished and **why**, at a strategic level. It is the input to `@ralph-loop-planner` which decomposes it into executable loops.

---

## File Location

```
plans/phase-{N}.md
```

Where `{N}` is the phase number (1-indexed, monotonically increasing).

---

## Required Sections

Every phase plan MUST contain these sections in this order:

| Section | Purpose | Required |
|---------|---------|----------|
| `# Phase {N}: {Name}` | Title | Yes |
| `## Objective` | One sentence: what and why | Yes |
| `## Scope` | Included + explicitly excluded | Yes |
| `## Key Deliverables` | Concrete artefacts with format and location | Yes |
| `## Success Criteria` | Verifiable conditions (not effort-based) | Yes |
| `## Dependencies` | What must exist before this phase starts | Yes |
| `## Skills Required` | Broad skill categories for this phase | Yes |
| `## Risk Assessment` | Risks with likelihood, impact, mitigation | Yes |
| `## Assumptions` | What we believe true; how we'd validate | Yes |
| `## Ralph Loops` | Proposed loop breakdown (name + type + outputs) | Yes |
| `## Notes` | Design decisions, alternatives considered | Optional |

---

## Field Specifications

### Objective

Single sentence. Pattern: `[Action] [thing] [into/for] [goal]`.

```
✅ "Design and implement the platform-independent core of the planning system as a clean foundation for multi-platform adapters."
❌ "Do the core stuff."
```

### Scope — Included

Bullet list of concrete deliverables. Each item should be specific enough that you could verify its existence.

```
✅ "Schema definitions (4): Phase plan, ralph loop, todo, and handoff schemas as standalone markdown reference documents"
❌ "Schemas"
```

### Scope — Explicitly NOT Included

Bullet list of things that are out of scope for this phase, with a note on where they belong.

```
✅ "Claude Code-specific commands, agents, or settings (Phase 2)"
❌ (omitting this section entirely)
```

### Key Deliverables

Table with three columns: Deliverable, Format, Location.

```markdown
| Deliverable | Format | Location |
|-------------|--------|----------|
| Phase plan schema | Markdown + YAML spec | `core/schemas/phase-plan.schema.md` |
```

### Success Criteria

Bullet list prefixed with `✓`. Each criterion must be **objectively verifiable** — a colleague could check it without asking the author.

```
✅ "✓ Schema completeness: All four schemas define every field with type, requirement level, valid values, and one worked example"
❌ "✓ Schemas look good"
```

### Dependencies

Three sub-sections:
- **Must Complete Before**: Hard blockers
- **Blocked By**: External blockers with status
- **Optional**: Nice-to-have but not blocking

### Skills Required

Bullet list of broad skill categories with purpose. These are refined to specific skills at the ralph loop level.

```
- `skill-creator`: Crafting well-structured SKILL.md files
```

### Risk Assessment

Table with four columns: Risk, Likelihood (Low/Med/High), Impact (Low/Med/High), Mitigation (concrete action, not hope).

### Assumptions

Bullet list with format: `` `[Assumption]`: [Why we believe it; how we'd validate] ``

### Ralph Loops

Summary table of proposed loops. Not the full decomposition — that's done by `@ralph-loop-planner`.

```markdown
| Loop | Name | Type | Key Outputs |
|------|------|------|-------------|
| 001 | Schema Definitions | Design | 4 schema documents in `core/schemas/` |
```

---

## Example

See `examples/planning-system-restructure/plans/phase-1.md` for a complete worked example.

---

## Validation Checklist

Before passing a phase plan to `@ralph-loop-planner`, verify:

- [ ] Objective is one sentence with action + thing + goal
- [ ] Scope has both Included and Explicitly NOT included
- [ ] Every deliverable has a format and location
- [ ] Every success criterion is verifiable by a colleague without author context
- [ ] Dependencies distinguish hard blockers from nice-to-haves
- [ ] Risk mitigations are concrete actions, not "monitor" or "hope"
- [ ] Ralph Loops table has 3–8 loops (more suggests the phase is too large)
