# Eddie Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert eddie-for-agents into an installable Claude Code plugin with a `/eddie` conversational entry point, specialist subagents, and reusable GitLab CI validation templates.

**Architecture:** Plugin-in-place — two new top-level directories (`plugin/`, `ci/`) added alongside existing `skills/`, `subagents/`, `docs/`. A `.claude-plugin/plugin.json` manifest makes the repo installable via `claude plugin install`. The `/eddie` command activates Claude as an Eddie-aware agent that classifies user intent, asks requirements questions, constructs scripts via existing subagents, and drives a push→CI→fix loop using `glab`.

**Tech Stack:** Claude Code plugin format (`.claude-plugin/plugin.json`, `plugin/commands/`, `plugin/agents/`), GitLab CI YAML, `glab` CLI, `bash`, `shellcheck`, existing Eddie skills/subagents.

**Reference:** `docs/superpowers/specs/2026-04-03-eddie-plugin-design.md`

**Use `plugin-dev` skills during implementation:**
- `plugin-dev:plugin-structure` — scaffold and validate plugin layout
- `plugin-dev:command-development` — write the `/eddie` command
- `plugin-dev:agent-development` — write the eddie-agent

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `.claude-plugin/plugin.json` | Create | Plugin manifest — name, version, author, keywords |
| `plugin/commands/eddie.md` | Create | `/eddie` slash command — classification logic, DEVELOP questions, CI loop |
| `plugin/agents/eddie-agent.md` | Create | Thin agent wrapper — loads eddie-orchestrate skill, routes to subagents |
| `ci/eddie-validate.yml` | Create | Reusable CI template — runs `validate_eddie_script.sh` |
| `ci/eddie-shellcheck.yml` | Create | Reusable CI template — runs shellcheck on `.sh` files |
| `ci/README.md` | Create | Include instructions for downstream projects |
| `README.md` | Modify | Add plugin install instructions |

---

## Task 1: Plugin manifest

**Files:**
- Create: `.claude-plugin/plugin.json`

- [ ] **Step 1: Create the manifest directory**

```bash
mkdir -p .claude-plugin
```

- [ ] **Step 2: Write the manifest**

Create `.claude-plugin/plugin.json`:

```json
{
  "name": "eddie",
  "description": "Eddie HPC script assistant for the University of Edinburgh Eddie cluster — builds, reviews, optimises, and fixes SGE job scripts via conversational AI",
  "version": "0.1.0",
  "author": {
    "name": "Mungo Harvey",
    "email": "m.harvey@ed.ac.uk"
  },
  "repository": "https://git.ecdf.ed.ac.uk/chandranlab/eddie-for-agents",
  "keywords": ["hpc", "eddie", "sge", "gridengine", "bioinformatics", "edinburgh", "slurm"]
}
```

- [ ] **Step 3: Validate JSON**

```bash
python3 -c "import json; json.load(open('.claude-plugin/plugin.json')); print('valid')"
```

Expected: `valid`

- [ ] **Step 4: Commit**

```bash
git add .claude-plugin/plugin.json
git commit -m "feat: add Claude Code plugin manifest"
```

---

## Task 2: `/eddie` slash command

**Files:**
- Create: `plugin/commands/eddie.md`

Use `plugin-dev:command-development` skill when writing this file.

- [ ] **Step 1: Create directory**

```bash
mkdir -p plugin/commands
```

- [ ] **Step 2: Write the command**

Create `plugin/commands/eddie.md`:

````markdown
---
allowed-tools: Bash(glab:*), Bash(git:*), Bash(bash:*), Bash(shellcheck:*), Read, Write, Edit, Glob, Grep, Agent
description: Eddie HPC assistant — builds, reviews, optimises, and fixes job scripts for the University of Edinburgh Eddie cluster
---

You are now acting as an Eddie-aware HPC assistant for the University of Edinburgh Eddie
cluster (Rocky Linux 9, Grid Engine/SGE). You drive the process; the user guides you.

## Before acting

1. Read `skills/eddie-orchestrate/SKILL.md`
2. Read `config/project.md` if it exists in the current project

## Prerequisites check (first invocation only)

- Run `glab version 2>/dev/null || echo MISSING` — if missing, tell the user:
  "Please install glab (GitLab CLI) and authenticate: `glab auth login --hostname git.ecdf.ed.ac.uk`"
- Check if `~/.claude/skills/phase-plan-creator/SKILL.md` exists — note if absent but continue

## Classify the user's request

Read the user's message and classify as REVIEW, DEVELOP, or FIX:

**REVIEW** — user mentions existing scripts, wants a check, validate, or review:
→ Load the scripts (path or GitLab URL) → dispatch `subagents/eddie-pipeline-review.md`
→ Report APPROVED / APPROVED WITH WARNINGS / BLOCKED with line-level feedback

**DEVELOP** — user wants a new pipeline, has existing code to adapt, describes a workflow:
→ Ask the requirements questions below, one at a time
→ After all answered: invoke phase-plan-creator (if available), then
   dispatch `subagents/eddie-pipeline-construction.md` per phase,
   validate each with `subagents/eddie-pipeline-review.md` (max 3 cycles)
→ Then run the push-validate loop below

**FIX** — user mentions CI failures, errors, scripts not working:
→ Run `glab pipeline list --status failed --limit 1` to find the latest failure
→ Run `glab pipeline ci view <id>` to read logs
→ Diagnose using Eddie skills → fix scripts → run push-validate loop

## DEVELOP mode: requirements questions

Ask these one at a time. Skip any the user has already answered:

1. What does each stage of the pipeline do? (e.g. align → sort → call variants)
2. Do you have existing scripts or code to base this on? Share a local path or GitLab URL.
3. Which stages must complete before others can start?
4. CPU or GPU? Approximate cores and RAM per job?
5. Is your data on DataStore, group space, or scratch?
   (DataStore requires `-q staging`; sharedmem PE is incompatible with staging)
6. Are any stages embarrassingly parallel — e.g. one job per sample or per file?
7. Which software modules do you need? (or should I suggest based on the workflow?)

## Push-validate loop

After constructing or fixing scripts:

```
1. git add <scripts> && git commit -m "feat: eddie pipeline <name>"
2. glab mr create --fill  (or git push if branch already has MR)
3. glab pipeline run  (only if not auto-triggered on push)
4. glab pipeline status --wait --interval 30
5. glab pipeline ci view  (read job output)
6. Parse verdict from output: APPROVED / APPROVED WITH WARNINGS / BLOCKED
7a. APPROVED      → confirm to user, optionally run: glab mr merge
7b. BLOCKED       → diagnose with Eddie skills → fix → back to step 1 (max 3 cycles)
7c. WARNINGS      → present findings, ask user: fix or accept?
```

## Eddie rules (never violate)

- Use `h_rss` not `h_vmem` (h_vmem silently gives no memory allocation since Sept 2025)
- DataStore paths (`/exports/.../datastore/`) only work on staging queue (`-q staging`)
- Never put `qsub` inside a job script — chaining uses `-hold_jid` from login node
- Always `. /etc/profile.d/modules.sh` before `module load`; pin versions
- Use `$NSLOTS` for thread counts, never hardcode
````

- [ ] **Step 3: Verify frontmatter is valid YAML**

```bash
python3 -c "
import re, sys
content = open('plugin/commands/eddie.md').read()
match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
print('frontmatter found' if match else 'ERROR: no frontmatter')
"
```

Expected: `frontmatter found`

- [ ] **Step 4: Commit**

```bash
git add plugin/commands/eddie.md
git commit -m "feat: add /eddie slash command"
```

---

## Task 3: Eddie agent

**Files:**
- Create: `plugin/agents/eddie-agent.md`

Use `plugin-dev:agent-development` skill when writing this file.

- [ ] **Step 1: Create directory**

```bash
mkdir -p plugin/agents
```

- [ ] **Step 2: Write the agent**

Create `plugin/agents/eddie-agent.md`:

````markdown
---
name: eddie-agent
description: |
  Use this agent for Eddie HPC job script tasks on the University of Edinburgh Eddie cluster.
  Handles: building new pipelines from a workflow description, reviewing existing scripts
  against Eddie standards (h_rss, staging queue, module init, qsub-in-script rules),
  optimising scripts, and diagnosing CI validation failures. Has access to glab and git
  to push scripts and trigger CI pipelines. Examples:
  <example>user: "build me a pipeline for RNA-seq alignment on Eddie" assistant: [dispatches eddie-agent to ask requirements questions and construct scripts]</example>
  <example>user: "my Eddie scripts failed CI" assistant: [dispatches eddie-agent to fetch CI logs, diagnose, and fix]</example>
model: inherit
---

You are an Eddie HPC expert for the University of Edinburgh Eddie cluster (Rocky Linux 9,
Grid Engine/SGE).

## Startup

1. Read `skills/eddie-orchestrate/SKILL.md`
2. Read `config/project.md` if present

## Classify and act

Classify the task as REVIEW, DEVELOP, or FIX and follow the protocol in
`plugin/commands/eddie.md` exactly.

For construction tasks, dispatch:
- `subagents/eddie-pipeline-construction.md` to build scripts
- `subagents/eddie-pipeline-review.md` to validate (max 3 cycles)

For review-only tasks, dispatch:
- `subagents/eddie-pipeline-review.md` directly

For fix tasks:
- Use glab to fetch CI failure logs, diagnose, fix, then push and re-validate

## Eddie non-negotiables

- `h_rss` only — never `h_vmem`
- DataStore paths require `-q staging`; never combine with `sharedmem` PE
- No `qsub` inside job scripts
- `. /etc/profile.d/modules.sh` before any `module load`
- `$NSLOTS` for all thread/process counts
````

- [ ] **Step 3: Verify frontmatter**

```bash
python3 -c "
import re
content = open('plugin/agents/eddie-agent.md').read()
match = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
print('frontmatter found' if match else 'ERROR: no frontmatter')
"
```

Expected: `frontmatter found`

- [ ] **Step 4: Commit**

```bash
git add plugin/agents/eddie-agent.md
git commit -m "feat: add eddie-agent plugin agent"
```

---

## Task 4: CI templates

**Files:**
- Create: `ci/eddie-validate.yml`
- Create: `ci/eddie-shellcheck.yml`
- Create: `ci/README.md`

- [ ] **Step 1: Create directory**

```bash
mkdir -p ci
```

- [ ] **Step 2: Write eddie-validate.yml**

Create `ci/eddie-validate.yml`:

```yaml
# ci/eddie-validate.yml
# Reusable GitLab CI template — validates Eddie job scripts.
#
# Include in your project's .gitlab-ci.yml:
#   include:
#     - project: 'chandranlab/eddie-for-agents'
#       ref: main
#       file: 'ci/eddie-validate.yml'
#
# The job runs on the eddie-validator shell runner (University of Edinburgh only).
# Set EDDIE_SCRIPTS_DIR to the directory containing your .sh files (default: scripts/).

variables:
  EDDIE_SCRIPTS_DIR:
    value: "scripts/"
    description: "Directory containing Eddie job scripts to validate"
  EDDIE_FOR_AGENTS_URL:
    value: "https://git.ecdf.ed.ac.uk/chandranlab/eddie-for-agents.git"
    description: "URL of the eddie-for-agents repo (provides validate script)"

eddie:validate:
  stage: test
  tags:
    - eddie-validator
  script:
    - git clone --depth 1 "$EDDIE_FOR_AGENTS_URL" _eddie_tools
    - |
      PASS=0; WARN=0; FAIL=0
      for script in $(find "$EDDIE_SCRIPTS_DIR" -name "*.sh" | sort); do
        echo "=== Validating $script ==="
        result=$(bash _eddie_tools/scripts/validate_eddie_script.sh "$script" 2>&1)
        echo "$result"
        if echo "$result" | grep -q "BLOCKED"; then FAIL=$((FAIL+1))
        elif echo "$result" | grep -q "WARNING"; then WARN=$((WARN+1))
        else PASS=$((PASS+1)); fi
      done
      echo ""
      echo "Results: $PASS APPROVED, $WARN WARNINGS, $FAIL BLOCKED"
      [ "$FAIL" -eq 0 ]
    - rm -rf _eddie_tools
  rules:
    - if: $CI_PIPELINE_SOURCE == "push"
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_PIPELINE_SOURCE == "trigger"
```

- [ ] **Step 3: Write eddie-shellcheck.yml**

Create `ci/eddie-shellcheck.yml`:

```yaml
# ci/eddie-shellcheck.yml
# Reusable GitLab CI template — runs shellcheck on all .sh files.
#
# Include in your project's .gitlab-ci.yml:
#   include:
#     - project: 'chandranlab/eddie-for-agents'
#       ref: main
#       file: 'ci/eddie-shellcheck.yml'

variables:
  EDDIE_SCRIPTS_DIR:
    value: "scripts/"
    description: "Directory containing Eddie job scripts to check"

eddie:shellcheck:
  stage: test
  tags:
    - eddie-validator
  script:
    - |
      FAIL=0
      for script in $(find "$EDDIE_SCRIPTS_DIR" -name "*.sh" | sort); do
        echo "=== shellcheck $script ==="
        shellcheck --severity=warning "$script" || FAIL=$((FAIL+1))
      done
      echo ""
      [ "$FAIL" -eq 0 ] || { echo "$FAIL script(s) failed shellcheck"; exit 1; }
  rules:
    - if: $CI_PIPELINE_SOURCE == "push"
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_PIPELINE_SOURCE == "trigger"
```

- [ ] **Step 4: Validate CI YAML**

```bash
python3 -c "
import sys
try:
    import yaml
    yaml.safe_load(open('ci/eddie-validate.yml'))
    yaml.safe_load(open('ci/eddie-shellcheck.yml'))
    print('both valid')
except ImportError:
    print('pyyaml not installed — skipping, validate manually')
except Exception as e:
    print(f'ERROR: {e}'); sys.exit(1)
"
```

Expected: `both valid` (or `pyyaml not installed` — acceptable)

- [ ] **Step 5: Write ci/README.md**

Create `ci/README.md`:

```markdown
# Eddie CI Templates

Reusable GitLab CI templates for validating Eddie HPC job scripts.

## Prerequisites

- Your project must be on `git.ecdf.ed.ac.uk`
- Your project must have access to the `eddie-validator` runner
  (contact the Chandran Lab to request access)

## Usage

Add to your project's `.gitlab-ci.yml`:

```yaml
include:
  - project: 'chandranlab/eddie-for-agents'
    ref: main
    file: 'ci/eddie-validate.yml'   # Eddie directive validation
  - project: 'chandranlab/eddie-for-agents'
    ref: main
    file: 'ci/eddie-shellcheck.yml' # shellcheck (optional)
```

By default, scripts in `scripts/` are checked. Override with:

```yaml
variables:
  EDDIE_SCRIPTS_DIR: "jobs/"   # check files in jobs/ instead
```

## What is validated

`eddie-validate.yml` checks each `.sh` file for:
- bash syntax errors (`bash -n`)
- `h_vmem` usage (silent failure since Sept 2025 — must use `h_rss`)
- Missing `h_rss` directive
- `qsub` inside job scripts (fails silently on compute nodes)
- DataStore paths without staging queue
- Missing module init (`. /etc/profile.d/modules.sh`)
- `sharedmem` PE combined with staging queue

## Verdicts

Each script gets one of: **APPROVED**, **APPROVED WITH WARNINGS**, **BLOCKED**

The CI job fails if any script is BLOCKED.
```

- [ ] **Step 6: Commit**

```bash
git add ci/
git commit -m "feat: add reusable Eddie CI validation templates"
```

---

## Task 5: Update README with install instructions

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read current README**

```bash
head -50 README.md
```

- [ ] **Step 2: Add plugin install section**

Add after the existing introduction (before the first major section):

```markdown
## Installing the Eddie Plugin

Requires Claude Code CLI. Install once:

```bash
# 1. Install the Eddie plugin
claude plugin install https://git.ecdf.ed.ac.uk/chandranlab/eddie-for-agents

# 2. Install advanced-ai-workflows (strongly recommended)
mkdir -p ~/.claude/skills/setup-with-claude
curl -fsSL https://raw.githubusercontent.com/MungoHarvey/advanced-ai-workflows/main/.claude/skills/setup-with-claude/SKILL.md \
  -o ~/.claude/skills/setup-with-claude/SKILL.md
# Then in Claude Code: invoke the setup-with-claude skill

# 3. Install superpowers (optional, adds brainstorming)
claude plugin install https://github.com/obra/superpowers

# 4. Authenticate GitLab CLI
glab auth login --hostname git.ecdf.ed.ac.uk
```

Then in any Claude Code session: type `/eddie` and describe what you need.

For CI integration, see `ci/README.md`.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add plugin install instructions to README"
```

---

## Self-review

**Spec coverage:**
- Plugin manifest ✓ Task 1
- `/eddie` command with classification and DEVELOP questions ✓ Task 2
- Eddie agent ✓ Task 3
- CI templates (validate + shellcheck) ✓ Task 4
- CI README ✓ Task 4
- End-user install story ✓ Task 5
- Runner registration — documented in spec; runner setup is a manual one-time operation outside this plan's scope (no code to write)

**Placeholder scan:** No TBDs. All file content is complete.

**Type consistency:** No shared types across files — each file is standalone markdown/JSON/YAML.
