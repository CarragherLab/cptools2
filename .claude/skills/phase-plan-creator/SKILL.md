---
name: phase-plan-creator
description: "Generate structured, verifiable phase plans for project phases. Use when starting a new project phase, establishing scope and dependencies, or documenting success criteria before loop decomposition. Creates comprehensive phase plans that feed into the ralph-loop-planner skill. Triggers: new phase, project plan, phase plan, plan a project, scope this out, plan this work, start a project, what's the approach."
---

# Phase Plan Creator

Creates structured phase plans with clear objectives, deliverables, success criteria, and risk assessment. Output feeds directly into `ralph-loop-planner` for loop decomposition.

## When to Use

- Starting a new project phase or major task
- Establishing scope and verifiable success criteria before loop planning
- Breaking complex problems into sequential phases
- Documenting assumptions, dependencies, and risks before execution begins
- Defining broad skill domains required for the phase

## Your Input

Provide:
- **Project or task description** — what you are trying to accomplish
- **Phase context** — which phase number or phase name
- **Success definition** — what "done" looks like (optional; the skill can prompt for this)
- **Constraints** — technical, resource, or scope constraints (optional)

## Process

1. **Ask clarifying questions** if needed about:
   - What must exist or be true before this phase starts?
   - What are the highest risks or uncertainties?
   - Which broad skill domains apply?
   - Are there alternative approaches worth considering?

2. **Generate a structured phase plan** following the template in `references/phase-plan-template.md`

3. **Output as markdown** suitable for:
   - Saving to `plans/` directory
   - Reviewing and editing inline
   - Passing to `ralph-loop-planner` for loop decomposition

## Output Structure

```markdown
# Phase [N]: [Descriptive Name]

## Objective
[One clear sentence. Pattern: "[Action] [thing] [into/for] [goal]"]

## Scope
### Included:
- [Concrete deliverable]

### Explicitly NOT included:
- [Out of scope]

## Key Deliverables
| Deliverable | Format | Location |
|-------------|--------|----------|
| [Description] | [Format] | [Path] |

## Success Criteria
- ✓ [Measurable, verifiable outcome]

## Dependencies
### Must Complete Before:
- [Task/phase]: [Why required]

### Blocked By:
- [External dependency]: [Status]

### Optional:
- [Nice-to-have]: [Why helpful but not blocking]

## Skills Required (Broad Categories)
- `[skill-domain]`: [Purpose in this phase]

## Risk Assessment
| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| [Risk] | Low/Med/High | Low/Med/High | [Concrete action] |

## Assumptions
- `[Assumption]`: [Why we believe it; how we will validate]

## Notes / Design Decisions
[Design choices, alternatives considered, open questions]

## Ralph Loops ([N])
| Loop | Name | Type | Key Outputs |
|------|------|------|-------------|
| NNN | [Name] | [Design/Migration/Implementation] | [Deliverables] |
```

## Output Locations

Save the completed phase plan to:
```
plans/phase-{N}.md              ← Phase plan document
plans/PLANS-INDEX.md            ← Update with new phase entry
```

## Next Steps

1. Review and refine the generated phase plan
2. Update `plans/PLANS-INDEX.md` to register the new phase
3. Pass to `ralph-loop-planner` to decompose into executable loops

## See Also

- `references/phase-plan-template.md` — Detailed template with examples and best practices
- `ralph-loop-planner` — Decomposes phase plans into executable loops
- `core/schemas/phase-plan.schema.md` — Formal schema for phase plan validation
