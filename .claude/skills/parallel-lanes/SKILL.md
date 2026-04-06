---
name: parallel-lanes
description: |
  Execute multiple phase plans in parallel git worktrees. Reads PLANS-INDEX.md to find
  phases marked with "Yes (worktree)" in the Parallel? column, creates a worktree and
  branch per lane, spawns background agents to implement each phase plan, monitors
  completion, merges lane branches back, and prompts for the consolidation phase.
  Use when: "run lanes in parallel", "start parallel phases", "execute all lanes",
  "spin up worktrees", or when PLANS-INDEX.md has multiple phases marked parallel.
  Also use for: "parallel-lanes --status" (check progress), "--merge" (manual merge),
  "--phases A,B" (run specific lanes only).
---

# /parallel-lanes — Parallel Phase Execution

Execute independent phase plans simultaneously in isolated git worktrees.
Each lane gets its own branch and background agent. When all lanes complete,
merge results back and trigger the consolidation phase.

## When to Use

- PLANS-INDEX.md has 2+ phases marked `Yes (worktree)` in the Parallel? column
- You want to implement independent workstreams concurrently
- Phase plans touch different files (no merge conflicts expected)

## Prerequisites

- `.claude/plans/PLANS-INDEX.md` exists with a phases table containing a `Parallel?` column
- Phase plan files referenced in the table exist
- Git repo with a clean working tree (no uncommitted changes)

## Command Parsing

Parse the user's input to determine which mode to run:

- `/parallel-lanes` — Run all parallel phases
- `/parallel-lanes --phases A,B` — Run only specified phases
- `/parallel-lanes --status` — Check progress of running lanes
- `/parallel-lanes --merge` — Manually trigger merge of completed lanes

## Run Flow

### Step 1: Read PLANS-INDEX.md and parse the phases table

Read `.claude/plans/PLANS-INDEX.md`. Find the markdown table with columns including
`Phase`, `Name`, `Plan`, and `Parallel?`.

Extract:
- **Parallel lanes**: rows where `Parallel?` contains `Yes (worktree)`
- **Consolidation phase**: rows where `Parallel?` contains `No`
- **Phase plan paths**: from the `Plan` column (resolve relative to `.claude/plans/`)

If `--phases` flag was provided, filter to only the specified phase letters.

### Step 2: Verify clean state

```bash
git status --porcelain
```

If there are uncommitted changes, warn the user:
"You have uncommitted changes. Commit or stash them before running parallel lanes,
otherwise worktrees will inherit dirty state."

Use AskUserQuestion:
- A) Commit changes and proceed
- B) Abort

### Step 3: Create state directory

```bash
mkdir -p .claude/state
```

### Step 4: Create worktrees and spawn agents

Get the repo name and current branch:

```bash
REPO_NAME=$(basename "$(git rev-parse --show-toplevel)")
BASE_BRANCH=$(git branch --show-current)
```

For each parallel lane (e.g., phase letter `A`, plan file `phase-A-polars-migration.md`):

**4a. Create worktree with a dedicated branch:**

```bash
git worktree add "../${REPO_NAME}-lane-{LETTER}" "${BASE_BRANCH}" -b "lane-{LETTER}"
```

This creates a worktree at `../{repo}-lane-A/` on branch `lane-A` forked from the
current branch. The worktree has the full repo including `.claude/plans/` and all skills.

**4b. Read the phase plan** to extract the success criteria and loop list. You need
these to build the agent prompt.

**4c. Spawn a background agent** using the Agent tool:

```
Agent(
  prompt: <see Agent Prompt below>,
  run_in_background: true,
  description: "Lane {LETTER}: {phase_name}"
)
```

**4d. Record the agent ID** from the response for tracking.

Spawn ALL lane agents in a single message (multiple tool uses) so they launch
concurrently.

### Agent Prompt

Each agent gets this prompt, with placeholders filled from the phase plan:

```
You are implementing Phase {LETTER}: {phase_name}.

Working directory: {worktree_absolute_path}
Branch: lane-{LETTER}
Phase plan: .claude/plans/{phase_plan_filename}

IMPORTANT: Change to the working directory first:
cd {worktree_absolute_path}

## Instructions

1. Read your phase plan at .claude/plans/{phase_plan_filename}
2. For each Ralph Loop listed in the plan (in order):
   a. Read the loop description, success criteria, and key outputs
   b. Implement each deliverable:
      - Read relevant source files before modifying
      - Write code changes
      - Run tests to verify: pytest tests/ -v --tb=short
      - Fix any test failures
      - Commit after each meaningful change with descriptive message
   c. After completing all deliverables in a loop, commit:
      git add -A && git commit -m "complete: loop {loop_id} — {summary}"
3. After all loops complete:
   - Run the full test suite: pytest tests/ -v
   - Commit any remaining changes
   - Return a completion summary with:
     - What was accomplished (list of changes)
     - Test results (pass count, any failures)
     - Any issues or concerns

## Constraints
- ONLY modify files listed in your phase plan's Scope > Included section
- Do NOT touch files belonging to other lanes (check the File Isolation table in PLANS-INDEX.md)
- Run tests after each loop to catch regressions early
- If a test fails and you cannot fix it in 3 attempts, note it in your summary and continue

## Success Criteria
{success_criteria_from_phase_plan}
```

### Step 5: Write state file

After all agents are spawned, write the tracking state:

```bash
cat > .claude/state/parallel-lanes.json << 'EOF'
{
  "started": "{ISO_TIMESTAMP}",
  "base_branch": "{BASE_BRANCH}",
  "lanes": {
    "{LETTER}": {
      "agent_id": "{AGENT_ID}",
      "branch": "lane-{LETTER}",
      "worktree": "../{REPO_NAME}-lane-{LETTER}",
      "phase_plan": "{PLAN_FILENAME}",
      "status": "running",
      "started": "{ISO_TIMESTAMP}"
    }
  },
  "consolidation_phase": "{CONSOLIDATION_PLAN_FILENAME}"
}
EOF
```

### Step 6: Report launch status

Print a summary:

```
PARALLEL LANES LAUNCHED
========================================
Lane A: Polars Migration         (4 loops)  → ../cptools2-lane-A/
Lane B: Nextflow Pipeline        (5 loops)  → ../cptools2-lane-B/
Lane C: Container Builds         (3 loops)  → ../cptools2-lane-C/
Lane D: CLI & Config Refactor    (3 loops)  → ../cptools2-lane-D/
========================================
Base branch: ai-update
Consolidation: Phase E (after all lanes merge)

Agents are running in the background.
You will be notified as each lane completes.
Run /parallel-lanes --status to check progress.
```

### Step 7: Wait for completion notifications

Background agents send task notifications when they complete. As each notification
arrives:

1. Read the agent's result (success summary or error)
2. Update the state file: set that lane's status to `completed` or `failed`
3. Report to the user: "Lane {X} complete: {one-line summary}"

When ALL lanes have completed (or failed):

- If all succeeded: proceed to Step 8 (merge)
- If any failed: report which lanes failed with the agent's error output.
  Ask the user whether to merge the successful lanes anyway or fix failures first.

### Step 8: Merge lane branches

```bash
git checkout {BASE_BRANCH}
git merge lane-A --no-ff -m "merge: Lane A — Polars Migration"
git merge lane-B --no-ff -m "merge: Lane B — Nextflow Pipeline"
git merge lane-C --no-ff -m "merge: Lane C — Container Builds"
git merge lane-D --no-ff -m "merge: Lane D — CLI & Config Refactor"
```

Merge one at a time. If a merge conflict occurs:
1. Report the conflicting files
2. Stop merging
3. Tell the user: "Merge conflict in {files}. Resolve manually, then run
   `/parallel-lanes --merge` to continue."

### Step 9: Clean up

After successful merge:

```bash
git worktree remove "../{REPO_NAME}-lane-A"
git worktree remove "../{REPO_NAME}-lane-B"
git worktree remove "../{REPO_NAME}-lane-C"
git worktree remove "../{REPO_NAME}-lane-D"
git branch -d lane-A lane-B lane-C lane-D
rm .claude/state/parallel-lanes.json
```

### Step 10: Report and trigger consolidation

```
ALL LANES MERGED
========================================
Lane A: Polars Migration         ✓ merged
Lane B: Nextflow Pipeline        ✓ merged
Lane C: Container Builds         ✓ merged
Lane D: CLI & Config Refactor    ✓ merged
========================================
Consolidation phase: Phase E — Integration & Validation

Run the consolidation phase to verify everything works together:
  /next-loop --auto (using phase-E plan)
```

---

## Status Flow

When invoked with `--status`:

1. Read `.claude/state/parallel-lanes.json`
2. For each lane, check if the agent is still running or has completed
3. Display:

```
PARALLEL LANES STATUS
========================================
Lane A: Polars Migration         running  (started 15:00)
Lane B: Nextflow Pipeline        running  (started 15:00)
Lane C: Container Builds         completed ✓
Lane D: CLI & Config Refactor    running  (started 15:00)
========================================
```

---

## Merge Flow

When invoked with `--merge`:

1. Read `.claude/state/parallel-lanes.json`
2. Find lanes with status `completed`
3. Run Step 8 (merge) for completed lanes only
4. Report which lanes were merged and which are still running/failed

---

## Failure Handling

| Scenario | Behavior |
|---|---|
| Agent fails mid-loop | Lane marked `failed`. Other lanes unaffected. User can retry with `--phases {X}`. |
| Merge conflict | Stop merging. Report conflicting files. User resolves, runs `--merge`. |
| Worktree already exists | Skip creation, warn user. May indicate a previous interrupted run. |
| State file missing for --status | Report "No parallel lanes running." |

---

## Important Rules

- Always spawn ALL lane agents in a single message so they run concurrently
- Never modify code in the main worktree while lanes are running
- Each lane branch is isolated: agents commit to their own branch only
- The merge step is sequential (one branch at a time) to surface conflicts early
- Clean up worktrees and branches after successful merge
