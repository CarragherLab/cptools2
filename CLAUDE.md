# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is cptools2

A CLI tool for orchestrating CellProfiler image-analysis workflows on HPC clusters (primarily Edinburgh's Eddie). It builds image lists, splits work into jobs, creates space-optimized plate batches, generates SGE submission scripts, and joins result CSVs after analysis.

## Commands

```bash
# Install (editable with dev deps)
pip install -e .[dev]

# Run tests
pytest                    # or: uv run pytest

# Lint/format
pre-commit run --all-files

# CLI usage
cptools2 generate config.yml            # generate loaddata, commands, submit script
cptools2 join --location /path --patterns Image.csv Cells.csv
```

## Architecture

The workflow has two main phases, each triggered by a CLI subcommand:

### `generate` phase (config → HPC scripts)

1. **`parse_yaml`** — parses YAML config into a namedtuple; resolves `$USER`, container paths, plate specs
2. **`filelist`** — discovers images under experiment directories; auto-detects old vs. new ImageXpress plate layouts
3. **`splitter`** — chunks image lists by `chunk` size; groups by well/site, sorts by channel
4. **`job.Job`** — central class that manages the plate store, drives chunking, creates LoadData CSVs, and computes batching (75% scratch utilisation, 30% overhead buffer)
5. **`commands`** — generates CellProfiler command strings; uses base64 encoding for paths with special characters
6. **`generate_scripts`** — writes SGE array job scripts (staging/analysis/destaging phases per batch) via **scissorhands** `SGEScript`

### `join` phase (post-analysis)

1. **`file_tools`** — discovers chunked output CSVs in `raw_data/`, enriches each chunk with its corresponding LoadData CSV (joined on 1-based `ImageNumber` before concatenation to avoid duplication), then concatenates into `joined_files/`

### Key external dependencies

- **parserix** (CarragherLab) — parses ImageXpress filenames to extract well, site, channel, plate metadata
- **scissorhands** (CarragherLab) — generates SGE submission scripts

### Container resolution (3-level precedence)

1. `container_path` key in YAML config
2. `CPTOOLS2_CONTAINER_DIR` / `CPTOOLS2_CP_CONTAINER` environment variables
3. Fallback: legacy conda-based execution (no singularity wrapping)

## gstack

Use `/browse` from gstack for all web browsing. Never use `mcp__claude-in-chrome__*` tools.

Available gstack skills: `/office-hours`, `/plan-ceo-review`, `/plan-eng-review`, `/plan-design-review`, `/design-consultation`, `/design-shotgun`, `/design-html`, `/review`, `/ship`, `/land-and-deploy`, `/canary`, `/benchmark`, `/browse`, `/connect-chrome`, `/qa`, `/qa-only`, `/design-review`, `/setup-browser-cookies`, `/setup-deploy`, `/retro`, `/investigate`, `/document-release`, `/codex`, `/cso`, `/autoplan`, `/plan-devex-review`, `/devex-review`, `/careful`, `/freeze`, `/guard`, `/unfreeze`, `/gstack-upgrade`, `/learn`.

## Skill routing

When the user's request matches an available skill, ALWAYS invoke it using the Skill
tool as your FIRST action. Do NOT answer directly, do NOT use other tools first.
The skill has specialized workflows that produce better results than ad-hoc answers.

Key routing rules:
- Product ideas, "is this worth building", brainstorming → invoke office-hours
- Bugs, errors, "why is this broken", 500 errors → invoke investigate
- Ship, deploy, push, create PR → invoke ship
- QA, test the site, find bugs → invoke qa
- Code review, check my diff → invoke review
- Update docs after shipping → invoke document-release
- Weekly retro → invoke retro
- Design system, brand → invoke design-consultation
- Visual audit, design polish → invoke design-review
- Architecture review → invoke plan-eng-review
- Save progress, checkpoint, resume → invoke checkpoint
- Code quality, health check → invoke health

## Planning State

- phase: 2
- phase_name: Eddie Deployment
- phase_plan: .claude/plans/phase-2-eddie-deployment.md
- loop_file: .claude/plans/phase-2-ralph-loops.md
- status: in_progress
- loops_total: 3
- loops_done: 2
- current_loop: 230 (End-to-End Eddie Test)
- next_loop: 230
- todos_total: 29
- todos_done: 21

## Test conventions

- Tests live in `tests/` with fixtures in `tests/fixtures/`
- `tests/new_config.yaml` is the canonical config example used across tests
- Formatting: black (line-length 88), isort (black profile), ruff (E, F, W, C90, I rules)
