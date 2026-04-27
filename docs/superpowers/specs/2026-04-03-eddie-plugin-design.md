# Eddie Plugin Design Spec
**Date:** 2026-04-03
**Status:** Approved

## Overview

Convert `eddie-for-agents` into an installable Claude Code plugin hosted on Edinburgh GitLab
(`git.ecdf.ed.ac.uk`). The plugin makes Claude an Eddie-capable agent: equipped with deep HPC
knowledge, the right tools, and the specialist subagents needed to build, validate, and fix
job scripts for the University of Edinburgh Eddie cluster.

**The primary actor is Claude, not the user.** Users guide the process through conversation —
describing what they want to compute, sharing existing scripts, or asking for fixes. Claude
applies Eddie expertise, asks clarifying questions, drives validation via CI, and iterates
until scripts are correct. The user does not need to learn subcommand syntax.

`/eddie` is the activation point: it loads Eddie domain knowledge and grants Claude the tools
(`glab`, `git`) to act autonomously on the user's behalf within a single session.

Access is governed by GitLab permissions: only users with access to the repo can install the
plugin.

---

## Architecture

### Hybrid model

- **Interactive layer (Claude Code):** `/eddie` activates Claude as an Eddie-aware agent.
  Claude holds the conversation, asks questions, constructs scripts, and drives the full
  workflow. Users guide with natural language.
- **Validation layer (CI):** `validate_eddie_script.sh` + shellcheck run on a registered shell
  runner. No AI in CI — fast, free, deterministic.
- **Feedback loop:** `glab` CLI connects the two layers inside a single Claude Code session.
  Claude pushes scripts, waits for CI, reads results, and iterates without the user leaving
  their terminal.

---

## Repository Structure

Two new top-level directories added; all existing content (`skills/`, `subagents/`, `docs/`,
`scripts/`, `tests/`) is unchanged.

```
eddie-for-agents/
├── .claude-plugin/
│   └── plugin.json              # Plugin manifest
├── plugin/
│   ├── commands/
│   │   └── eddie.md             # /eddie slash command definition
│   └── agents/
│       └── eddie-agent.md       # Thin orchestrating agent; loads existing skills
├── ci/
│   ├── eddie-validate.yml       # Reusable CI template: validate_eddie_script.sh
│   ├── eddie-shellcheck.yml     # Reusable CI template: shellcheck
│   └── README.md                # Include instructions for downstream projects
├── skills/                      # unchanged
├── subagents/                   # unchanged
├── docs/                        # unchanged
└── scripts/                     # unchanged
```

`plugin/agents/eddie-agent.md` is a thin wrapper — it does not duplicate skill logic. It loads
`skills/eddie-orchestrate/SKILL.md` and routes to the existing `subagents/` based on the
subcommand invoked.

---

## Plugin Manifest

`.claude-plugin/plugin.json`:

```json
{
  "name": "eddie",
  "description": "Eddie HPC script assistant for the University of Edinburgh Eddie cluster",
  "version": "0.1.0",
  "author": {
    "name": "Mungo Harvey",
    "email": "m.harvey@ed.ac.uk"
  },
  "repository": "https://git.ecdf.ed.ac.uk/<group>/eddie-for-agents",
  "keywords": ["hpc", "eddie", "sge", "gridengine", "bioinformatics", "edinburgh"]
}
```

---

## The `/eddie` Command

Defined in `plugin/commands/eddie.md`. Activates Claude as an Eddie-aware agent for the
session. Grants `Bash(glab:*)` and `Bash(git:*)` so Claude can drive the full
push–wait–read–fix cycle autonomously.

### Design principle: Claude drives, users guide

Once `/eddie` is invoked, Claude takes the lead. The user describes what they need in plain
language; Claude interprets, asks targeted questions, and acts. Users do not need to know
subcommand syntax or Eddie rules — that is Claude's job.

```
User: /eddie
      I have a Python script that aligns sequencing reads. I need to run it on Eddie.

Claude: I can help build an Eddie pipeline around that. A few questions to get the
        details right — what's the rough memory requirement per alignment job?
...
Claude: [asks 4-6 more targeted questions, one at a time]
...
Claude: [invokes phase-plan-creator, constructs scripts, pushes, runs CI, reports back]
```

```
User: /eddie
      Can you check my scripts in scripts/ — I'm not sure they're right for Eddie.

Claude: [reads scripts/, identifies issues, produces APPROVED / WARNINGS / BLOCKED verdict
         with specific line-level feedback and corrected versions]
```

```
User: /eddie
      My pipeline failed CI this morning.

Claude: [runs glab to fetch latest failed run, reads logs, diagnoses, fixes scripts,
         pushes, re-runs CI]
```

### How Claude classifies intent

Claude reads the user's message and classifies into one of three modes before acting:

| Mode | User describes… | Claude's first action |
|---|---|---|
| **REVIEW** | existing scripts, wants a check or validation | Read scripts → eddie-pipeline-review → verdict |
| **DEVELOP** | a workflow to build, existing code to adapt, a new pipeline | Ask requirements questions one at a time |
| **FIX** | CI failure, errors, scripts not working | Fetch CI logs via glab → diagnose → fix |

### DEVELOP mode: what Claude asks

Claude asks these questions one at a time before writing any code. The order and phrasing adapt
to what the user has already shared:

1. **Workflow:** What does each stage do? (e.g. align → sort → call variants)
2. **Existing code:** Any scripts or code to build on? Share a path or GitLab URL.
3. **Stages and dependencies:** Which stages depend on others completing first?
4. **Compute:** CPU or GPU? Approximate cores and RAM per job?
5. **Storage:** Data on DataStore, group space, or scratch? (Drives staging queue decision)
6. **Parallelism:** Any stages that run independently per-sample or per-file?
7. **Modules:** Known software modules, or should Claude suggest based on the workflow?

Once these are answered, Claude invokes `phase-plan-creator` and begins construction via the
Ralph loop.

### Shortcut invocations (optional, for experienced users)

Users who know what they want can skip classification:

| Invocation | Effect |
|---|---|
| `/eddie review scripts/my_job.sh` | Direct to REVIEW mode |
| `/eddie fix https://git.ecdf.ed.ac.uk/...` | Direct to FIX mode |
| `/eddie pull https://git.ecdf.ed.ac.uk/...` | Fetch `.sh` files from a GitLab project into context |

### Full automated loop (within one Claude Code session)

```
1. /eddie  →  classification  →  requirements questioning (DEVELOP mode)
   → superpowers:brainstorming   (if installed: structured exploration)
   → phase-plan-creator           (structured phase plan for the pipeline)
   → Ralph loop (per phase/script):
        eddie-pipeline-construction → eddie-pipeline-review → fix if needed

2. Eddie agent commits and pushes scripts to user's GitLab repo
   → glab mr create  (or git push to existing branch)

3. Eddie agent triggers and monitors CI pipeline
   → glab pipeline run           (if not auto-triggered on push)
   → glab pipeline status --wait (polls until complete)

4. Eddie agent reads pipeline output
   → glab pipeline ci view       (fetches job logs)
   → Parses APPROVED / WARNINGS / BLOCKED verdict

5a. APPROVED  → reports back to user, optionally merges MR
5b. BLOCKED   → diagnoses using Eddie skills → fixes → loops back to step 2 (max 3 cycles)
5c. WARNINGS  → presents findings, asks user whether to fix or accept
```

### Companion plugin behaviour

| Companion | Status | What it unlocks |
|---|---|---|
| `advanced-ai-workflows` | Strongly recommended | `phase-plan-creator` for pipeline planning; Ralph loop for iterative construction |
| `superpowers` | Optional | `superpowers:brainstorming` for requirements exploration before build |

**Graceful degradation:** if companions are not installed, `/eddie` falls back to direct
`eddie-orchestrator` flow — it still works, without the structured planning and loop layers.

---

## GitLab CI Templates

### `ci/eddie-validate.yml`

Reusable template other projects include in their `.gitlab-ci.yml`. Runs
`scripts/validate_eddie_script.sh` on job scripts, reporting APPROVED / WARNINGS / BLOCKED
to the originating pipeline.

Tags jobs with `eddie-validator` so they only run on the registered shell runner.

### `ci/eddie-shellcheck.yml`

Runs `shellcheck` on all `.sh` files in the project. Separate template so projects can
include either or both.

### How downstream projects include these

**Option 1 — include (always-on):**
```yaml
include:
  - project: '<group>/eddie-for-agents'
    ref: main
    file: 'ci/eddie-validate.yml'
```

**Option 2 — manual trigger job (on-demand):**
```yaml
trigger:
  project: <group>/eddie-for-agents
  branch: main
  strategy: depend
variables:
  EDDIE_SCRIPT_PATH: scripts/my_job.sh
```

Both options post the verdict back to the originating MR/pipeline page.

---

## Runner Registration

One-time setup on the maintainer's machine (Windows 11, Git Bash):

```bash
gitlab-runner register \
  --url https://git.ecdf.ed.ac.uk \
  --token <project-runner-token> \
  --executor shell \
  --shell bash \
  --description "eddie-validator" \
  --tag-list "eddie-validator" \
  --run-untagged false
```

**Runner requirements:**
- `bash` (Git Bash in PATH)
- `shellcheck`
- Nothing else — `validate_eddie_script.sh` is self-contained

The runner only picks up jobs tagged `eddie-validator`. It is not shared with other projects'
general CI work.

---

## End-User Install Story

### One-time setup

```bash
# 1. Install the Eddie plugin
claude plugin install https://git.ecdf.ed.ac.uk/<group>/eddie-for-agents

# 2. Install superpowers (optional, for brainstorming)
claude plugin install https://github.com/obra/superpowers

# 3. Install advanced-ai-workflows (strongly recommended)
mkdir -p ~/.claude/skills/setup-with-claude
curl -fsSL https://raw.githubusercontent.com/MungoHarvey/advanced-ai-workflows/main/.claude/skills/setup-with-claude/SKILL.md \
  -o ~/.claude/skills/setup-with-claude/SKILL.md
# Then in Claude Code: invoke setup-with-claude skill

# 4. Authenticate GitLab CLI
glab auth login --hostname git.ecdf.ed.ac.uk

# 5. Add Eddie CI template to their project's .gitlab-ci.yml
#    (see ci/README.md for the include snippet)
```

### Prerequisites

- `glab` (GitLab CLI) installed
- `git` configured with SSH key for Edinburgh GitLab
- GitLab personal access token (handled by `glab auth login`)

### Everyday use

```bash
/eddie build          # new pipeline from scratch
/eddie fix <url>      # diagnose and fix CI failures
/eddie review <path>  # review a local or remote script
```

---

## Out of Scope

- AI running inside CI jobs (AI stays in the interactive Claude Code layer)
- Docker runner support (not available on Edinburgh GitLab)
- Public/external access (access governed by GitLab permissions)
- Automatic MR merge without user confirmation
