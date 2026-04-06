# Phase Plan Template & Best Practices

## When to Create Phase Plans

Use the phase-plan-creator skill when starting:
- A new project or major feature
- A distinct research or exploration phase
- A refactoring or migration effort
- A performance improvement cycle
- A validation or testing phase

---

## Template Structure

```markdown
# Phase [N]: [Descriptive Name]

## Objective
[One clear sentence expressing the phase purpose.]

**Pattern:** "[Action] [thing] [into/for] [goal]"

## Scope

### Included:
- [Concrete deliverable 1]
- [Concrete deliverable 2]

### Explicitly NOT included:
- [Out of scope item] (will be Phase [N+1])
- [Out of scope item] (handled by [other phase])

## Key Deliverables

| Deliverable | Format | Location |
|-------------|--------|----------|
| [Description] | [Markdown/JSON/Code] | `[path]` |

## Success Criteria

Each criterion must be objectively verifiable:

- ✓ [Measurable outcome 1]: [How verified]
- ✓ [Measurable outcome 2]: [How verified]

## Dependencies

### Must Complete Before This Phase:
- Phase [N-1]: [Name] — [Why required]
- [External task]: [Why required, timeline if known]

### Blocked By:
- [External dependency]: [Current status or workaround]

### Optional (nice to have):
- [Task]: [Why helpful but not blocking]

## Skills Required (Broad Categories)

Skills needed for this phase (refined to specific skills in ralph loops):

- `[skill-domain-1]`: [Specific purpose in this phase]
- `[skill-domain-2]`: [Specific purpose in this phase]

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| [Specific risk] | Low/Med/High | Low/Med/High | [Concrete mitigation strategy] |

## Assumptions

- `[Assumption 1]`: [Why we believe it; how we will validate]
- `[Assumption 2]`: [Why we believe it; how we will validate]

## Notes / Design Decisions

- [Design choice]: [Why this approach over alternatives]
- [Open question]: [Will resolve in ralph loops]
- [Alternative considered]: [Why not chosen]

## Ralph Loops ([N])

| Loop | Name | Type | Key Outputs |
|------|------|------|-------------|
| NNN | [Name] | [Design/Research/Implementation/Migration] | [Deliverables] |
```

---

## Best Practices

### Objective: Goal + Domain + Approach

✅ Good:
```
"Migrate [system] from [old approach] to [new approach] with [property]"
"Research [domain] approaches for [problem] and document recommendations"
"Design [component] satisfying [constraint] as a foundation for [next phase]"
"Validate [system] against [benchmark] and produce a reproducibility report"
```

❌ Vague:
```
"Improve the system"
"Research stuff"
"Build feature"
```

### Deliverables: Specific Format and Location

✅ Good:
```
| Schema document | Markdown | core/schemas/foo.schema.md |
| Skill definition | SKILL.md + references/ | core/skills/bar/ |
| Protocol spec | Markdown + JSON Schema | core/state/ |
```

❌ Vague:
```
- Code for the feature
- Documentation
- Tests
```

### Success Criteria: Observable, Not Aspirational

✅ Good:
```
- ✓ All four schema documents exist with field specs and validation checklists
- ✓ Verification scan finds zero platform-specific references in core/skills/
- ✓ A colleague reading the protocol document alone can implement the state bus
```

❌ Vague:
```
- ✓ Tests pass
- ✓ Performance is good
- ✓ Code is clean
```

### Risk Mitigation: Concrete Actions

✅ Good:
```
| Risk | Mitigation |
| Schema over-abstraction | Constrain every field to something used in practice; flag aspirational fields |
| Domain examples too specific | Use generic examples in core; domain examples go in examples/ only |
```

❌ Vague:
```
| Risk | Mitigation |
| Things might go wrong | Hope it works |
```

---

## Example Phase Plans

### Example 1: System Design Phase

```markdown
# Phase 1: Core Architecture Design

## Objective
Design the platform-independent core as a clean, documented foundation that adapters can build upon.

## Scope

### Included:
- Schema definitions (4): phase plan, ralph loop, todo, handoff
- Planning skills (5): migrated from prototypes, stripped of platform-specific assumptions
- Agent role definitions (2): orchestrator and worker with skill injection protocol
- State bus protocol: filesystem coordination with JSON Schema validation

### Explicitly NOT included:
- Platform-specific adapters (Phase 2+)
- Installation scripts

## Key Deliverables

| Deliverable | Format | Location |
|-------------|--------|----------|
| Schema documents | Markdown | core/schemas/ |
| Planning skills | SKILL.md + references/ | core/skills/ |
| Agent definitions | Markdown | core/agents/ |
| State bus protocol | Markdown + JSON Schema | core/state/ |

## Success Criteria

- ✓ All four schemas define every field with type, requirement, valid values, and an example
- ✓ All five skills contain zero platform-specific references
- ✓ Worker role definition includes a numbered skill injection protocol
- ✓ State bus schemas are JSON Schema draft-07 and machine-validatable

## Dependencies

### Must Complete Before This Phase:
- Analysis document — provides the evidence base and design rationale

### Blocked By:
- Nothing — this is the first phase

## Skills Required (Broad Categories)

- `schema-design`: Defining validatable schemas from practical evidence
- `documentation`: Writing reference documents for contributors and implementers
- `skill-creator`: Crafting well-structured SKILL.md files

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Over-abstracting schemas | Medium | High | Constrain to fields actually used in practice; flag aspirational additions |
| Skills too tightly coupled to example domain | Low | Medium | Use generic examples in core; domain examples in examples/ only |

## Assumptions

- `Schema stability`: Four schema types are sufficient — validated across multiple programmes with no additional types needed
- `Skill portability`: Planning skills can be written without tool-specific syntax — validated by the self-dogfooding restructure programme
```

### Example 2: Migration Phase

```markdown
# Phase 2: Claude Code Adapter

## Objective
Implement the Claude Code platform adapter, wrapping core components with slash commands, sub-agent configs, and hooks.

## Scope

### Included:
- Slash commands: next-loop, new-phase, check-execution
- Sub-agent definitions: ralph-orchestrator (Sonnet), ralph-loop-worker (Haiku)
- settings.json: permissions whitelist and PostToolUse hooks
- End-to-end test: one full loop executed against the adapter

### Explicitly NOT included:
- Cowork routing skill (Phase 3)
- Python API adapter (Phase 4)

## Key Deliverables

| Deliverable | Format | Location |
|-------------|--------|----------|
| Slash commands | Markdown | platforms/claude-code/commands/ |
| Sub-agent configs | Markdown | platforms/claude-code/agents/ |
| Permissions config | JSON | platforms/claude-code/settings.json |

## Success Criteria

- ✓ /next-loop command executes a loop from start to handoff without manual intervention
- ✓ loop-ready.json and loop-complete.json match the JSON Schema from core/state/
- ✓ All todos are marked correctly in the adapter's session tracking mechanism

## Dependencies

### Must Complete Before This Phase:
- Phase 1 (Core Architecture): all core schemas, skills, and state bus protocol complete

## Skills Required (Broad Categories)

- `adapter-development`: Wrapping core components with platform-specific invocation
- `integration-testing`: Verifying the end-to-end loop execution flow
```

---

## Connecting to Ralph Loop Planning

Once the phase plan is reviewed:

1. Pass it to `ralph-loop-planner`
2. The planner will:
   - Refine broad skills → specific skills per loop
   - Discover additional skills if needed
   - Break deliverables into verifiable loops
   - Generate execution-ready prompts with handoff injection blocks

Skill refinement example:
```
Phase-level skill: `schema-design`
↓ Refined for loop:
  - `json-schema`: Draft-07 pattern constraints for the state bus schemas
  - `markdown-tables`: Field spec table formatting for schema documents
  - (discovered) `ascii-diagrams`: Sequence diagram for the state bus protocol
```
