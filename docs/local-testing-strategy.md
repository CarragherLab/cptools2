# Eddie Script Testing Strategy

**Project:** eddie-for-agents  
**Author:** Mungo Harvey  
**Status:** Phase 1 — Design complete, implementation pending

---

## Overview

This document captures the rationale, design decisions, and honest assessment of
limitations behind the local testing and CI strategy for Eddie HPC job scripts.
The goal is to catch bugs in pipeline scripts (job logic, SGE directives, path
handling, array indexing etc.) before they reach the cluster — where failures are
expensive, silent, and slow to surface.

---

## The Problem

Eddie job scripts have several environment dependencies that make them hard to test
locally or in a standard CI environment:

- `#$` SGE scheduler directives that define the runtime environment
- SGE-injected variables (`$NSLOTS`, `$JOB_ID`, `$SGE_TASK_ID` etc.)
- `module load` commands that are only available on Eddie nodes
- Filesystem paths that only exist on staging/compute nodes
  (`/exports/cmvm/datastore/...`, `/exports/cmvm/eddie/...`, scratch)
- Resource constraints (memory via `h_rss`, runtime via `h_rt`)
- Staging queue constraints (DataStore only accessible from staging nodes)

Without a testing layer, bugs in these areas only surface after a job has been
queued, dispatched, and run — often hours later, with cryptic output.

---

## Approaches Evaluated

Three broad approaches were considered, assessed on effort vs. impact.

### 1. `local_qsub` — SGE Environment Emulator

A bash wrapper script that:

- Parses `#$` directives from the job script
- Sets all SGE environment variables (`$JOB_ID`, `$NSLOTS`, `$SGE_TASK_ID` etc.)
- Stubs the `module` command
- Creates temporary directories mirroring Eddie's filesystem structure
- Enforces the runtime limit via `timeout`
- Approximates memory limits via `ulimit`
- Supports array jobs via `-t START-END` flag

**Verdict:** Highest ROI. Catches the majority of real bugs with zero infrastructure
overhead. Works immediately in a local terminal on any machine.

**Location in project:** `scripts/local_qsub.sh`

---

### 2. Docker Container (Rocky Linux 9)

A container that mirrors Eddie's OS (Rocky Linux 9), filesystem skeleton, and
stubbed toolchain. Useful for testing OS-level correctness and ensuring scripts
don't depend on Ubuntu-isms from a developer's local machine.

**Verdict:** Moderate effort for marginal gain over `local_qsub` for pure bash
logic testing. Worth building when a specific bug arises that `local_qsub`
couldn't reproduce. The Dockerfile serves as the definition of the CI environment.

**Location in project:** `Dockerfile` (not yet created)

---

### 3. bats (Bash Automated Testing System)

Proper unit test framework for bash. Allows writing named test cases with setup,
teardown, assertions, and JUnit XML output for CI integration.

**Verdict:** High long-term value for pipelines like DRUGseq that are complex
enough to regress. Not worth writing comprehensively upfront — start with two or
three tests for the most fragile scripts, then grow the suite organically.

**Location in project:** `tests/` (not yet populated)

---

## Chosen Strategy: Phased Implementation

### Phase 1 — Immediate (Low effort, high impact)

- [x] `local_qsub.sh` in repo — runs job scripts in a simulated SGE environment
- [ ] GitLab CI with `bash -n` syntax check across all `.sh` files
- [ ] ShellCheck in CI — catches quoting bugs, bad substitutions, and other
      subtle issues that `bash -n` misses entirely

### Phase 2 — When a pipeline stabilises

- [ ] bats unit tests for the highest-risk scripts:
  - Staging job (DataStore path handling)
  - Array job dispatch (SGE_TASK_ID indexing)
  - DESeq2 invocation (R library path, multicore setup)

### Phase 3 — When the pipeline becomes collaborative

- [ ] Docker image pushed to GitLab container registry
- [ ] Full CI pipeline: lint → unit → integration stages

---

## High-Value Additions to `local_qsub`

Beyond the base emulator, four lightweight additions provide significant value:

### ShellCheck Integration

Runs ShellCheck against the job script before execution. ShellCheck catches a
class of bugs that `bash -n` simply does not: word splitting from unquoted
variables, deprecated syntax, unsafe substitutions, and more.

```bash
# Excluded codes:
# SC2034 — unused variables (common and intentional in job scripts)
# SC1091 — don't follow sourced .env files
shellcheck --severity=warning --exclude=SC2034,SC1091 "$SCRIPT"
```

### Eddie Directive Linter

A targeted set of checks for mistakes specific to Eddie's environment:

| Check | Rationale |
|---|---|
| `h_vmem` detected | Deprecated since September 2025 — must use `h_rss` |
| No `h_rss` | Job will use node default (16–32 GB) — often an oversight |
| `qsub` inside script | Compute nodes cannot submit jobs — must chain from login node |
| DataStore path without `-q staging` | DataStore is only accessible from staging nodes |
| `conda activate` in batch context | Requires careful initialisation; common source of hangs |
| No `h_rt` | Job has no runtime limit — runaway jobs waste resource allocation |

### Environment Variable Audit

Scans the script for variables that are referenced but never defined within the
script itself — and are not known SGE built-ins. These would be silently empty
in a batch context, often causing cryptic failures rather than clear errors.

Particularly relevant for scripts that rely on `.env` files sourced externally,
where a missing `source` line leaves all variables unset.

### Timing and Exit Code Summary

Wraps job execution with wall-clock timing and a structured summary block,
making `local_qsub` output useful as a lightweight execution record:

```
──────────────────────────────────────
 Summary
  Job:      drugseq_star (99999)
  Runtime:  142s (limit: 04:00:00)
  Memory:   16G × 8 slots
  Status:   ✓ PASSED
──────────────────────────────────────
```

---

## GitLab CI: Current State and Intended Design

### Current State

The `.gitlab-ci.yml` in this repo is currently the **default GitLab Auto-DevOps
boilerplate** — it was auto-generated and has no Eddie-specific content. It
includes stages for SAST, secret detection, and a full deployment pipeline that
is entirely irrelevant to this project.

The intended Eddie CI configuration below has not yet been merged into this file.

### Intended Phase 1 CI Configuration

The CI pipeline we designed should replace the Auto-DevOps template. Its purpose
is simple and focused: **catch bad job scripts before they reach Eddie**.

```yaml
# .gitlab-ci.yml — Eddie script linting pipeline
# Replaces the Auto-DevOps boilerplate entirely for this project

stages:
  - lint

lint:
  stage: lint
  image: rockylinux:9          # Match Eddie's OS for maximum relevance
  before_script:
    - dnf install -y shellcheck --quiet
  script:
    # 1. Bash syntax validation — catches parse errors bash -n finds
    - |
      echo "=== Bash syntax check ==="
      find . -name "*.sh" -not -path "./.git/*" -exec bash -n {} \; -print

    # 2. ShellCheck static analysis — catches what bash -n misses
    - |
      echo "=== ShellCheck ==="
      find . -name "*.sh" -not -path "./.git/*" \
        -exec shellcheck --severity=warning --exclude=SC2034,SC1091 {} \;

    # 3. Eddie-specific: deprecated h_vmem directive
    - |
      echo "=== Eddie directive check: h_vmem ==="
      if grep -rl "h_vmem" --include="*.sh" . | grep -v ".git"; then
        echo "ERROR: h_vmem found — replace with h_rss (deprecated Sept 2025)"
        exit 1
      fi

    # 4. Eddie-specific: qsub inside a job script
    - |
      echo "=== Eddie directive check: qsub-in-script ==="
      if grep -rn "^\s*qsub" --include="*.sh" . \
           | grep -v "^Binary\|\.git\|#" \
           | grep -v "submit_pipeline"; then
        echo "ERROR: qsub found inside job script — compute nodes cannot submit jobs"
        exit 1
      fi

  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'
    - if: '$CI_COMMIT_BRANCH == "main"'
```

### What This CI Does and Why

**`bash -n` syntax check**
The cheapest possible validation. Parses every `.sh` file and fails on syntax
errors — unclosed brackets, missing `fi`, bad heredocs. Takes seconds and costs
nothing. Catches errors that would prevent a script from running at all.

**ShellCheck**
A static analyser that goes far beyond `bash -n`. Catches quoting bugs that cause
word splitting, unportable constructs, deprecated syntax, and unsafe variable
substitutions. These are the bugs that *don't* prevent the script from running —
they just make it behave incorrectly in subtle ways, often only under specific
conditions (filenames with spaces, empty arrays, etc.). This is the single
highest-value addition to the whole pipeline.

**h_vmem check**
Enforces the September 2025 memory system migration. Any script using `h_vmem`
will still *submit* to Eddie but will not receive the memory it requested, leading
to confusing OOM failures. Catching this in CI costs one grep and saves a lot of
debugging time.

**qsub-in-script check**
Enforces the architectural constraint that compute nodes cannot submit jobs. A
script with `qsub` in it will appear to succeed locally but will hang or fail
silently on Eddie when it tries to submit from a compute node. This is a
particularly hard bug to diagnose without knowing the constraint.

---

## Limitations of This Approach

This is the honest assessment of what the CI pipeline *cannot* do, and why
expanding it further is not currently worth the effort.

### What the CI Cannot Test

**Actual SGE scheduling behaviour**
The CI has no Grid Engine scheduler. It cannot test that your resource requests
are schedulable, that your `-hold_jid` dependencies resolve correctly, that
your array job task ranges are valid for your data, or that your job will actually
start rather than sit in the queue indefinitely. All of this requires a real
Eddie session.

**Real filesystem paths**
The CI environment has no `/exports/cmvm/...` mounts. It cannot verify that
input files exist, that output directories are writable, or that DataStore is
reachable from a staging job. Path validation is entirely mocked.

**Module availability**
`module load star/2.7.10a` cannot be tested in CI. The module stub just prints
a message and returns 0. Whether the module exists, loads cleanly, and puts the
right binary on `$PATH` can only be verified on Eddie.

**Memory and runtime behaviour**
`ulimit` in CI is a very rough approximation. It cannot replicate how Eddie's
RSS enforcement actually behaves, whether a job would be killed at a specific
memory watermark, or how the scheduler responds to different `h_rss` values.

**Bioinformatics tool correctness**
STAR, DESeq2, HTSeq, FastQC — none of these are installed in the CI environment.
The CI tests pipeline *logic and structure*, not whether the tools produce correct
scientific output. That remains exclusively an on-cluster concern.

### Why Expanding the CI Further Is Currently Unnecessary

**You are the sole developer**
CI adds the most value when multiple people push changes that could break shared
infrastructure. With a single developer, the feedback loop of `local_qsub` is
faster and more informative than waiting for a CI pipeline.

**Installing real tools is expensive**
Building a Docker image with STAR, R/Bioconductor, and the full DRUGseq toolchain
would produce a multi-gigabyte container with significant build and maintenance
overhead. The image would need updating every time a tool version changes. For a
pipeline that runs on Eddie, the container would also diverge from the actual Eddie
module environment over time — creating a testing environment that doesn't match
production.

**bats tests require significant upfront investment**
Writing a comprehensive bats test suite for DRUGseq would likely take 2–3× the
time it took to write the scripts themselves. The return on that investment only
materialises when the scripts regress — which is unlikely if you are the primary
author and have `local_qsub` for rapid local validation.

**The fundamental limitation is irreducible**
Even a perfect CI pipeline cannot fully replicate Eddie. The scheduler, the
module system, the node hardware, and the filesystem topology are all specific
to the cluster. The most rigorous possible local testing environment — Docker +
bats + full tool installation — still cannot catch the class of bugs that only
appear under real scheduling conditions. Investing heavily in CI infrastructure
does not eliminate the need to validate on Eddie; it only reduces the frequency
of naive failures.

### When to Revisit This Decision

The calculus changes when any of these conditions are met:

- **A second developer joins** the pipeline — CI becomes essential to prevent
  regressions introduced by others
- **A specific bug arises** that `local_qsub` could not catch — this identifies
  a concrete gap that justifies the Docker approach
- **The pipeline is promoted to production lab infrastructure** used by multiple
  group members who are not familiar with the codebase
- **Repeated regressions occur** in the same scripts — this signals that bats
  tests for those specific scripts would pay off

Until one of those conditions is met, Phase 1 (`local_qsub` + ShellCheck in CI)
is the correct level of investment.

---

## Repository Structure

```
eddie-for-agents/
├── .gitlab-ci.yml          # Currently: Auto-DevOps boilerplate (needs replacing)
├── Dockerfile              # Rocky Linux 9 test environment (Phase 2 — not yet created)
├── README.md
│
├── docs/
│   ├── local-testing-strategy.md   # This document
│   └── reference/                  # Eddie HPC reference documentation
│
├── scripts/
│   └── local_qsub.sh       # SGE environment emulator (Phase 1)
│
├── skills/                 # Eddie agent skills
├── subagents/              # Eddie subagent definitions
│
└── tests/
    ├── unit/               # bats unit tests (Phase 2 — not yet populated)
    ├── fixtures/           # Minimal mock inputs for integration tests
    └── output/             # Gitignored — populated during CI runs
```

---

## Key Eddie Infrastructure Notes

These inform the linting rules and test design:

- **Memory (Sept 2025 change):** `h_rss` is now the primary memory control.
  `h_vmem` defaults to unlimited and should not be specified. Memory is
  per-slot — total = slots × per-slot value.
- **Staging queue:** DataStore is only accessible from staging nodes. Jobs
  requiring DataStore access must use `-q staging`. The `sharedmem` PE is
  incompatible with the staging queue.
- **No qsub-within-qsub:** Compute nodes cannot submit jobs. All job chaining
  must be orchestrated from login nodes using `-hold_jid`.
- **Conda in batch:** Conda activation in job scripts requires explicit shell
  initialisation. Storing environments on network shares is unreliable.
- **ShellCheck:** Highest-value addition to local testing — catches bugs that
  `bash -n` misses and would otherwise only surface after a queued job runs.

---

## Decision Rationale

The pragmatic 80/20 rule applies: `local_qsub` plus ShellCheck in CI captures
approximately 80% of the bugs that would otherwise reach Eddie, for roughly 5%
of the total implementation effort of a full Docker + bats suite.

The remaining infrastructure is deferred not out of laziness but out of an honest
assessment that the fundamental limitation — CI cannot replicate Eddie — means
the returns diminish rapidly beyond Phase 1. The goal is not a comprehensive
testing pyramid; it is a lightweight safety net that catches naive errors before
they consume cluster time.
