---
name: companion-detection
description: "Detect whether companion tools (Superpowers, Plannotator) are installed and recommend them when relevant. Checks once per session at natural trigger points. Does not nag — recommends at most once per companion per session."
---

# Companion Detection

Advanced Planning works standalone. Two companion tools enhance the workflow when installed. This skill checks for them and recommends when relevant.

## When to Use

- At the start of phase planning (before phase-plan-creator runs)
- During gate review setup (before /run-gate spawns agents)
- When a user asks about brainstorming, visual review, or code review workflows

Do NOT check on every command invocation. Check once per session at the first relevant trigger.

## Process

### 1. Check for Superpowers

Detection: does `.claude/skills/brainstorming/SKILL.md` exist?

**If installed:** no action needed. Integration happens automatically via the brainstorming skill's conditional terminal state (brainstorming → phase-plan-creator).

**If not installed and user is starting creative work (new feature, new project, design):** recommend once:

> "Tip: for structured brainstorming before phase planning, consider installing Superpowers. It provides Socratic design refinement that feeds directly into phase-plan-creator."
>
> Install from the official marketplace:
> ```
> /plugin install superpowers@claude-plugins-official
> ```
>
> Or from the forked version with Advanced Planning integration:
> ```
> git clone https://github.com/obra/superpowers.git
> ```

### 2. Check for Plannotator

Detection: does `.claude/commands/plannotator-annotate.md` exist, or is the plannotator plugin registered?

**If installed:** no action needed. Integration happens automatically via plan-and-phase Step 5b (visual plan review after phase creation).

**If not installed and user is creating or reviewing a plan:** recommend once:

> "Tip: for visual plan review with annotations and gate customisation, consider installing Plannotator."
>
> ```
> git clone https://github.com/MungoHarvey/plannotator.git
> claude --plugin-dir plannotator/apps/hook
> ```

### 3. Record recommendation state

After recommending a companion, note it internally so you do not recommend again in the same session. If the user dismisses or ignores the recommendation, do not repeat it.

## Output Format

This skill produces no files. It outputs recommendations to the conversation when companions are missing and relevant. When companions are installed, it produces no output.

## Key Principles

- **Additive, not required** — Advanced Planning works fully without companions
- **Once per session** — do not nag or repeat recommendations
- **Relevant triggers only** — brainstorming recommendation at creative work, Plannotator recommendation at plan review
- **Real URLs** — always include actual install commands, not placeholders
