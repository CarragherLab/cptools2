# Agent Architecture

The planning system uses a **two-agent pattern** for loop execution. Each loop cycle involves exactly two roles with a clean handoff between them via the filesystem state bus.

---

## The Two Agents

```
Main Thread
    │
    ├─ Spawn ────────────────► Orchestrator (Sonnet)
    │                               Reads plan
    │                               Populates todos/skills/agents
    │                               Writes loop-ready.json
    │                               Returns
    │◄──────────────────────────────┘
    │ Reads loop-ready.json
    │ Reads loop file → extracts skill list
    │ Constructs worker prompt with skill paths + context
    │
    ├─ Spawn ────────────────────────────────────► Worker (Sonnet)
    │                                                  Receives skill paths in prompt
    │                                                  Reads loop-ready.json
    │                                                  Executes ALL todos inline
    │                                                  Loads skill per todo (targeted injection)
    │                                                  Writes loop-complete.json
    │                                                  Returns
    │◄────────────────────────────────────────────────┘
    │ Reads loop-complete.json
    │ Updates planning state
    │ Git commit
    └─ Next cycle
```

Each agent is **spawned once per loop cycle and returns when its work is complete**. Neither agent is persistent across loops. The main thread acts as the sequencer — it spawns the orchestrator, waits, then spawns the worker. It never delegates this sequencing decision to either agent.

---

## Role Definitions

| Role | File | Model Tier | When Spawned | Returns When |
|------|------|-----------|-------------|--------------|
| Orchestrator | `orchestrator.md` | Sonnet | Before each loop | `loop-ready.json` is written |
| Worker | `worker.md` | Sonnet (default) | After orchestrator returns | `loop-complete.json` is written |

---

## Model Tier Economics

The three-tier model hierarchy aligns cost with responsibility:

| Tier | Role | Typical Model | Why This Tier |
|------|------|--------------|---------------|
| Strategic | Planning skills (phase plans, loop decomposition) | Opus | Highest reasoning demand; runs once per phase or loop, not per task |
| Tactical | Orchestrator (loop preparation, context assembly) | Sonnet | Moderate complexity; reads plan, assembles context, makes skill assignments |
| Execution | Worker (bounded task execution) | Sonnet (default); Haiku for low-complexity | High frequency; runs many tasks per loop |

Running a 12-loop programme with 5 todos per loop means the worker executes ~60 tasks. Using Sonnet as the default ensures reliable compositional reasoning. Todos marked `complexity: low` can use Haiku for cost savings on genuinely simple tasks (single-file edits, command runs).

---

## The Subagent Spawning Constraint

In agent frameworks that do not support recursive subagent spawning (such as Claude Code), subagents cannot spawn further subagents. This means a subagent worker **cannot** itself spawn analysis workers or parallel agents.

**The v8 solution**: the main thread handles all spawning explicitly. It spawns the orchestrator, waits for it to return, then spawns the worker. The worker executes **all** todos inline using its own capabilities — it does not delegate further. The `agent:` field in todos is planning-time metadata used for categorisation; it does not trigger subagent spawning at execution time.

The worker's execution quality comes from two sources:
1. **Frontmatter skill injection** — skills listed in the agent definition's `skills:` frontmatter are loaded at spawn time
2. **Targeted per-todo skill injection** — the worker reads the specific SKILL.md for each todo immediately before executing it

If a todo genuinely requires parallel execution, the loop plan should be decomposed into separate sequential loops rather than attempting subagent nesting.

---

## The Targeted Skill Injection Protocol

The highest-value optimisation in the planning system. The worker does **not** load all skills at startup. Instead, it loads the relevant SKILL.md immediately before each todo, and unloads it before the next.

This prevents skill context pollution (irrelevant instructions bleeding between tasks) and keeps each execution step precisely focused on the current task's requirements.

The full protocol is specified in `worker.md` — **all platform adapters must implement it**.

---

## What the Main Thread Does

The main thread (the command or script that drives the cycle) is responsible for:

1. Spawning the orchestrator
2. Reading `loop-ready.json` after the orchestrator returns
3. Printing a summary of the upcoming loop for human visibility
4. Spawning the worker
5. Reading `loop-complete.json` after the worker returns
6. Updating the planning state (CLAUDE.md or equivalent)
7. Appending to `history.jsonl` (optional)
8. Running the completion git commit
9. Deciding whether to continue to the next loop or stop

The main thread is the **only** decision-maker for loop sequencing. Neither the orchestrator nor the worker decides whether to continue.

---

## Files

- `orchestrator.md` — Full orchestrator role specification
- `worker.md` — Full worker role specification with targeted skill injection protocol
- `README.md` — This document (architecture overview)
