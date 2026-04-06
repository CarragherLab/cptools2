# Agent Catalogue Reference

Authoritative catalogue of all core agent roles in `core/agents/`. Used by `plan-subagent-identification`
to match todos against available agents. Each entry includes the delegation criteria, model tier,
and the decision framework for when to assign versus keep inline.

---

## How to Use This Catalogue

For each todo with `agent: NA` (skills already assigned):

1. Assess the task against the **Delegate** and **Keep inline** criteria below
2. If delegation is appropriate, find the agent in this catalogue whose scope best fits the task
3. If no agent fits but delegation is warranted, flag `MISSING: [agent-description]`
4. If keeping inline, leave `agent: NA`

The delegation decision is more important than the agent choice — settle that first.

### Platform Constraint: Claude Code

In Claude Code, the `ralph-loop-worker` is a subagent and **cannot spawn further subagents**.
The `agent:` field in todos is **planning-time metadata** that categorises task types — it does
not trigger separate agent spawning during loop execution. The worker executes all todos inline,
using targeted skill injection for quality.

The `agent:` field remains valuable for:
- **Planning clarity**: distinguishing execution tasks from coordination tasks
- **Future platforms**: adapters that support recursive spawning can act on this field
- **Cost signals**: combined with `complexity:`, helps determine appropriate model tier

---

## Delegation Decision Framework

### Categorise as a delegated task when ALL of these are true

- The task is **self-contained**: it has clear inputs (files to read) and a clear output (files to write, command to run, result to produce)
- The task requires **focused execution** in a specific domain without needing to coordinate other tasks
- The task would **pollute the orchestrator's context** if run inline — long code generation, deep analysis, multi-step file operations
- The task benefits from an **isolated execution context** with its own skill injection cycle

### Keep in orchestrator context (`agent: NA`) when ANY of these is true

- The task **reads or writes the plan file itself** (todo status updates, handoff_summary writing)
- The task **coordinates** or synthesises results from other tasks
- The task is a **simple one-liner**: a single git command, a single file copy, one log entry
- The task **requires full session context**: decisions that depend on conversation history or prior outputs not captured in files
- No suitable agent exists and the task does not warrant creating one

---

## Core Agents

### `orchestrator` (`core/agents/orchestrator.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread, before each loop cycle
**Returns when**: `loop-ready.json` written to state directory

**Purpose**: Prepares the next pending ralph loop for execution. Reads the plan, resolves what needs doing, populates under-specified todos (running `plan-todos`, `plan-skill-identification`, `plan-subagent-identification` as needed), and writes `loop-ready.json` to hand off to the worker.

**When to assign to a todo:**

The orchestrator is rarely assigned as a `agent:` value in a todo. It is the entity *running* the loop preparation step, not a delegated subagent for individual todos. In exceptional cases you might assign it when:

- A todo is itself a loop preparation task (e.g. "Prepare loop-006 by running the planning pipeline and writing loop-ready.json")
- A todo requires Sonnet-level reasoning to produce a structured output that Haiku would likely get wrong

**Delegation scope**: Loop-level preparation tasks only. Not for execution work.

---

### `worker` (`core/agents/worker.md`)

**Model tier**: Sonnet (default); Haiku for low-complexity todos
**Spawned by**: Main thread, after `loop-ready.json` written by the orchestrator
**Returns when**: `loop-complete.json` written to state directory

**Purpose**: Executes all pending todos in a single ralph loop. For each todo: loads the assigned SKILL.md immediately before execution (targeted skill injection), executes the task, verifies the outcome condition, unloads the skill, then marks complete. Writes `handoff_summary` and `loop-complete.json` when all todos are done.

**When to assign to a todo:**

Assign `agent: worker` for any todo that is:

- **Bounded file creation**: creating or editing one or more files with a clear specification
- **Code writing**: implementing a function, module, or script from a defined spec
- **Running commands**: executing a test suite, a grep scan, a build step
- **Structured output production**: writing JSON, YAML, or Markdown following a documented schema
- **Verification**: checking that a file exists, a test passes, a scan is clean

**When NOT to assign:**

- Updating todo `status:` fields or `handoff_summary` in the loop file — this is orchestrator work, keep `agent: NA`
- Writing `loop-ready.json` or `loop-complete.json` — these are state bus writes; keep inline or assign to orchestrator
- Tasks requiring full session context or cross-task coordination

**Delegation scope**: Individual execution todos. The most commonly assigned agent in any loop.

---

## Gate Review Agents

Gate review agents are evaluation-only agents spawned at phase boundaries by `/run-gate` or `/run-closeout`. They read outputs and write verdicts; they do not modify code or execute tasks.

### `gate-reviewer` (`core/agents/gate-reviewer.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread after all loops in a phase complete
**Returns when**: Verdict JSON written to gate-verdicts/ directory

**Purpose**: Abstract gate reviewer role. Evaluates a phase's outputs against its stated objectives, applies a 6-step gate protocol, and produces a structured verdict with confidence scoring. If confidence falls below 80, verdict is `fail`; if 80 or above, verdict is `pass`. Triggers versioned retry with injected failure context on fail.

**When to assign to a todo**: Rarely used as a `agent:` value in todos. It is the abstract role that concrete gate agents implement. Assign a concrete gate agent (see below) for specific review tasks.

---

### `code-review-agent` (`platforms/claude-code/agents/code-review-agent.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread via `/run-gate` at phase boundaries
**Returns when**: Verdict JSON written to gate-verdicts/

**Purpose**: Reviews code produced during a phase for quality, patterns, CLAUDE.md compliance, and correctness. Produces a structured verdict with confidence scoring. Read-only (no code modifications).

**When to assign**: Use when a phase produces significant code output that must be evaluated against quality standards and CLAUDE.md conventions before advancement.

---

### `phase-goals-agent` (`platforms/claude-code/agents/phase-goals-agent.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread via `/run-gate` at phase boundaries
**Returns when**: Verdict JSON written to gate-verdicts/

**Purpose**: Verifies that a phase's outputs satisfy all stated success criteria in the phase plan. Reads each criterion, locates the corresponding artefact, and confirms it meets the specification.

**When to assign**: Use for every gate review — this is the primary gate agent that checks phase success criteria are satisfied.

---

### `security-agent` (`platforms/claude-code/agents/security-agent.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread via `/run-gate` when security scanning is enabled
**Returns when**: Verdict JSON written to gate-verdicts/

**Purpose**: Optional gate agent that scans phase outputs for security issues including secret exposure, hardcoded credentials, and injection pattern risks.

**When to assign**: Use when a phase produces code that handles credentials, environment variables, network calls, or user input — any output where accidental secret exposure is a realistic risk.

---

### `test-agent` (`platforms/claude-code/agents/test-agent.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread via `/run-gate` when test verification is enabled
**Returns when**: Verdict JSON written to gate-verdicts/

**Purpose**: Optional gate agent that runs the test suite for phase outputs and verifies coverage thresholds. Executes pytest, captures output, and produces a structured verdict.

**When to assign**: Use when a phase produces Python modules or scripts that have associated tests in the repository. Not applicable to documentation-only or planning phases.

---

### `programme-reporter` (`platforms/claude-code/agents/programme-reporter.md`)

**Model tier**: Sonnet
**Spawned by**: Main thread via `/run-closeout` at programme completion
**Returns when**: Closeout narrative written (uses Write tool)

**Purpose**: Closeout synthesis agent. Reads the complete documentary record of a programme — all phase plans, loop files, handoff summaries, gate verdicts, and history.jsonl — and produces a structured closeout narrative.

**When to assign**: Use only at programme completion, after all phases have passed gate review. Not for individual phase or loop review tasks.

---

## Model Tier Summary

| Agent | Model | Cost profile | Best for |
|-------|-------|-------------|----------|
| `orchestrator` | Sonnet | Medium | Loop preparation, todo population, context assembly |
| `worker` | Sonnet (default); Haiku for low-complexity | Medium (default); Low for Haiku | Bounded task execution, file operations, code runs, verification |
| `gate-reviewer` | Sonnet | Medium | Abstract gate role (use concrete agents below) |
| `code-review-agent` | Sonnet | Medium | Code quality and standards evaluation at phase boundary |
| `phase-goals-agent` | Sonnet | Medium | Success criteria verification at phase boundary |
| `security-agent` | Sonnet | Medium | Secret and credential scanning at phase boundary |
| `test-agent` | Sonnet | Medium | Test suite execution and coverage verification |
| `programme-reporter` | Sonnet | Medium | Programme closeout narrative synthesis |

Delegate execution todos to `worker` for isolated execution context. Most todos run at Sonnet tier; only `complexity: low` todos are eligible for Haiku. See `docs/model-tier-strategy.md` for detailed cost estimates. Gate agents run at Sonnet tier and are spawned outside the loop system — they are never assigned as `agent:` values in regular todos.

---

## Assignment Quick Reference

| Task type | Recommended agent |
|-----------|-------------------|
| Create or edit files from a specification | `worker` |
| Write or run code | `worker` |
| Run a test suite or verification scan | `worker` |
| Produce structured output (JSON, YAML, Markdown) | `worker` |
| Prepare a loop (plan, populate, write loop-ready.json) | `orchestrator` (rare as a todo agent) |
| Update `status:` fields in the loop file | `NA` (orchestrator inline) |
| Write `handoff_summary` in the loop file | `NA` (orchestrator inline) |
| Simple one-liner (git commit, file copy) | `NA` (orchestrator inline) |
| Tasks requiring full session context | `NA` |
| Gate review — verify phase success criteria | `phase-goals-agent` (spawned by `/run-gate`) |
| Gate review — code quality evaluation | `code-review-agent` (spawned by `/run-gate`) |
| Gate review — security scan | `security-agent` (spawned by `/run-gate`) |
| Gate review — test suite execution | `test-agent` (spawned by `/run-gate`) |
| Programme closeout narrative | `programme-reporter` (spawned by `/run-closeout`) |

---

## Missing Agent Flagging

When a todo warrants delegation but no existing agent covers it:

```yaml
agent: "MISSING: data-analysis-worker — agent for long-running Python analysis tasks with isolated output directory and matplotlib/pandas dependencies"
```

Typical missing agent patterns:

- **Domain-specialist worker**: a project-specific execution agent with particular tool permissions or environment setup (e.g. a worker with database access, or with browser tools enabled)
- **Analysis worker**: a Sonnet-tier worker for tasks that require more reasoning than Haiku can reliably provide but do not need full orchestrator context

Flag these explicitly. Do not silently leave `agent: NA` when delegation is clearly appropriate and an agent type does not yet exist — surfacing gaps is more useful than hiding them.

---

## Notes for New Platform Adapters

The `orchestrator` and `worker` roles are platform-agnostic specifications. Each platform adapter provides its own concrete implementation:

- **Claude Code adapter**: `platforms/claude-code/agents/` — slash command-driven invocation
- **Cowork adapter**: `platforms/cowork/agents/` — Agent tool prompts passed to the LLM directly
- **Generic (Python) adapter**: invocation via `platforms/python/` API

When assigning `agent: worker` or `agent: orchestrator`, you are assigning to the *role*. The adapter resolves which concrete implementation to use at execution time.

## See Also

- `core/agents/orchestrator.md` — Full orchestrator protocol
- `core/agents/worker.md` — Full worker protocol, including targeted skill injection
- `plan-skill-identification/references/skill-catalogue.md` — Skills catalogue (run before this step)
- `docs/model-tier-strategy.md` — Cost table and model selection guidance
