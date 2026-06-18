# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is cptools2

A CLI tool for orchestrating **Cell Painting** image-analysis pipelines on HPC clusters (primarily Edinburgh's Eddie). cptools2 is a *thin orchestrator*: it parses a YAML config into a Nextflow `params.json`, splits work into scratch-safe plate batches, and invokes a DSL2 Nextflow pipeline (illumination correction → segmentation → feature extraction → feature export). It is **not** containerised itself — it runs on the login node / locally and drives containerised compute on the cluster.

The project is mid-migration from a CellProfiler-only SGE-script generator to a multi-stage container-based pipeline orchestrator. The old `generate` SGE-script workflow has been **removed** (`cptools2 generate` now errors and points at `pipeline`). Prefer clean rebuilds on this `ai-update` branch over backward-compatible dual-path code.

## Commands

```bash
# Install (editable with dev deps)
pip install -e .[dev]            # or: uv pip install .[dev]

# Run tests
pytest                           # or: uv run pytest
pytest tests/test_parse_yaml.py            # single file
pytest tests/test_parse_yaml.py::test_name # single test

# Lint/format (black line-length 88, isort black profile, ruff E/F/W/C90/I)
pre-commit run --all-files

# Main workflow — parse config, write params.json, run Nextflow per scratch-safe batch
cptools2 pipeline config.yml                 # full run
cptools2 pipeline config.yml --dry-run       # write params.json only, skip Nextflow
cptools2 pipeline config.yml --resume        # continue from last Nextflow checkpoint
cptools2 pipeline config.yml --stages illum segment   # subset of stages
cptools2 pipeline config.yml --cleanup-policy verified # keep|success|verified (default verified)

# Data preparation only (write params.json, do not invoke Nextflow)
cptools2 prepare config.yml

# Post-analysis CSV join (enriches each chunk with LoadData metadata before concat)
cptools2 join --location /path --patterns Image.csv Cells.csv

# Feature-export helpers (also wired into the Nextflow pipeline)
cptools2 export-features <features_dir> --extractor deepprofiler --output-dir <dir>
cptools2 deepprofiler-package --chunk-manifest ... --locations-dir ... --output-root ... --config-path ...
```

CLI version is in `cptools2/__main__.py` (`__version__`); the package version in `pyproject.toml` is separate.

## Architecture

Two layers: a thin **Python orchestrator** (the `cptools2` package) and the **Nextflow pipeline** it drives (`nextflow/`).

### Python orchestrator (config → params.json → batched Nextflow runs)

1. **`parse_yaml`** — parses the YAML config into a plain dict and writes a Nextflow **`params.json`** via `generate_params_json()`. This is the single translation point between the (externally-owned) YAML schema and Nextflow params — add new config handling here. Resolves `$USER`/env vars, stage aliases (`illum`→`illum_calculate,illum_apply`; `segment`→`segmentation`; `extract`→`feature_extract`), and container `.sif` paths. Enforces two contracts: DataStore inputs **must** set `stage_data: true`; AI feature extractors (DeepProfiler/DINOv2) **require** Cellpose segmentation.
2. **`__main__.cmd_pipeline`** — the batch driver. Reads scratch quota (`batch.get_scratch_quota`), computes scratch-safe plate batches (`batch.create_batches`, default 75% utilisation × 1.3 work factor), writes one `params.batch_N.json` per batch, then runs Nextflow per batch with per-batch `-work-dir`, trace/report/timeline. After each batch it parses **stage-out verification** artifacts and applies the **cleanup policy** (`keep`/`success`/`verified`); progress is recorded in a `batch_status.csv` ledger. Verified-but-unverified batches stop the run to protect data.
3. **`batch`** — scratch quota detection, per-plate size measurement, batch packing.
4. **`containers`** — 3-level container `.sif` resolution: (1) `container_path` in YAML, (2) `CPTOOLS2_CONTAINER_DIR` + optional `cptools2_containers.json` manifest (per-role images, versions, `verified`, `gpu` flags), (3) `None` → legacy conda. Roles: `cellprofiler`, `deepprofiler`, `cellpose`/`cellpose_sam`.
5. **`nextflow_chunking`** — domain helpers *called by Nextflow processes* (not job submitters): ImageXpress indexing via **parserix**, chunk-manifest creation, DeepProfiler input packaging. Pins Polars/Rayon/OMP thread counts to 1 to avoid login-node thread-limit blowups.
6. **`feature_export/`** — exports native extractor outputs into canonical measurement tables (`deepprofiler_manifest/sites/cells/quality`) with a NaN-fraction quality gate. `contract.py` (dataclasses + table/metadata contracts), `deepprofiler.py` (exporter), `join.py` (plate-level staging), `writers.py` (Polars CSV/Parquet), `reports.py` (status/summary).
7. **`file_tools`** — `join` phase: discovers chunked output CSVs under `raw_data/`, enriches each with its LoadData CSV on 1-based `ImageNumber` **before** concatenation (avoids ImageNumber collisions), writes `joined_files/`.
8. **`nextflow_diagnostics`** — maps a diagnostics mode (`full`/`minimal`/`off`) to Nextflow observer flags.

Legacy CellProfiler-era modules still present: `splitter`, `job.Job`, `loaddata`, `commands`, `generate_scripts`, `filelist`. Treat as legacy unless a task is explicitly about them.

### Nextflow pipeline (`nextflow/main.nf`, DSL2)

Per-plate channel → optional `STAGE_IN` (DataStore→scratch) → `BUILD_IMAGESET_INDEX` → `CHUNK_IMAGESETS` → stages → optional `STAGE_OUT` (scratch→DataStore, writes verification JSON). Stages, gated by `params.stages`:

- **illum**: `ILLUM_CALCULATE` (per-plate) + `ILLUM_APPLY` (per-chunk, CellProfiler).
- **segment**: `CELLPOSE_SEGMENT` (GPU) when the feature tool is an AI tool, else CellProfiler `SEGMENTATION`.
- **extract**: `FEATURE_EXTRACT` (GPU; DeepProfiler or DINOv2).
- **feature export**: `EXPORT_FEATURES` + `SUMMARISE_FEATURE_EXPORTS` (only for DeepProfiler when `feature_export_enabled`).

Profiles in `nextflow/nextflow.config`: `-profile test` (local + Docker) and `-profile eddie` (SGE + Singularity). `cmd_pipeline` always runs with `-profile eddie`.

### Key external dependencies

- **parserix** (CarragherLab) — parses ImageXpress filenames (well/site/channel/plate). Auto-detects old vs new ImageXpress layouts.
- **scissorhands** (CarragherLab) — SGE script generation (legacy path).
- **polars** + **numpy** — data wrangling (note: `pandas` is *not* a dependency; some docs/README are stale on this).
- Runtime tools live in containers: **CellProfiler**, **DeepProfiler**, **Cellpose-SAM**, **DINOv2**.

## Eddie HPC

Eddie is the deployment target and the source of most real-world constraints.

- **Read `dev_docs/eddie_pipeline_gotchas.md` before any container build/deploy or new SGE/Nextflow work.** It documents real blockers: Docker `--provenance=false --sbom=false`, save as plain `.tar` for Singularity `oci-archive://`, CRLF stripping after scp, `module` needs `. /etc/profile.d/modules.sh`, `SINGULARITY_TMPDIR`+`CACHEDIR` to scratch, GPU resource is `gpu` not `gpus`, DataStore paths only visible from `-q staging`, Nextflow drivers can exhaust the login-node thread limit.
- **Env bootstrap**: `bash scripts/configure_eddie_paths.sh --project-root <...>` then `source config/eddie_env.sh`. Eddie runs require `CPTOOLS2_PROJECT_ROOT` and `CPTOOLS2_SCRATCH_ROOT`; the `eddie` profile (`nextflow/conf/eddie.config`) reads `CPTOOLS2_*` env vars for container dir, model dir, venv, GPU queues/resources, and runtime mode.
- 6 `.sif` containers are deployed (3× chandranlabs, 3× Drug-Discovery); duplicated, not symlinked, because group-space ACLs block cross-group reads.
- For Eddie shell-script / pipeline work, prefer the `eddie-*` skills/agents (`eddie-orchestrator`, `eddie-pipeline-construction`, `eddie-pipeline-review`) over ad-hoc shell advice. Mirrors live in `.agents/` (Codex) and `.claude/agents/eddie-*.md`. Eddie reference docs are under `docs/reference/`, `docs/applications/`, `docs/general/`.

## Config & test conventions

- The YAML config is produced by a **separate GUI app the user built** — treat the YAML schema as a stable external contract. Translate/adapt in `parse_yaml.generate_params_json()` rather than changing the schema. Recognised keys are validated in `parse_yaml.check_yaml_args`.
- Tests live in `tests/` with fixtures in `tests/fixtures/`. `tests/new_config.yaml` is the legacy canonical config; `tests/pipeline_config.yaml` exercises the multi-stage pipeline path.
- A standard plate is 384 wells × 6 sites × 5 channels = 11,520 images (~80–180 GB in flight) — size scratch tests accordingly.
- Numerous `.tmp-*/` dirs and `.test-output/` are scratch from past runs; ignore them.

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
- Eddie HPC pipeline/shell-script work → use the `eddie-*` skills/agents

## Planning State

This project uses the ralph-loop phase/plan workflow (`.claude/plans/`, `.claude/state/history.jsonl`,
index at `.claude/plans/PLANS-INDEX.md`). The block below is a snapshot maintained by that tooling;
re-read the active plan file and the loop file rather than trusting it blindly.

- forward_line: `codex/ai-feature-extraction` @ 7faabef — the clean DINO successor and the single forward development line (siblings `codex/dino-clean-successor`, `codex/dino-consolidation`), in worktree `cptools2-dino-consolidation`. This `ai-update` line is abandoned, leaky, and never pushed — do not develop on it.
- phase: 3.2
- phase_name: DINO Worktree Consolidation and DeepProfiler Retirement
- phase_plan: .claude/plans/phase-3.2-dino-worktree-consolidation.md
- loop_file: .claude/plans/phase-3.2-dino-worktree-consolidation.md
- status: active — consolidation built and DINO is the live feature-extraction route; the closing gate is a real-cluster acceptance run
- loops_total: 5 (620, 630, 640, 650, 660)
- done: clean successor is the single forward line; DINO route active (channel_adaptive_dino / cell_dino / dinov2); DeepProfiler deprecated (config validation rejects it — the DINO variant is selected per-run in the project YAML); install reconciled to a POSIX `.venv`; `eddie_bootstrap.sh` one-shot mirror bootstrap; committed placeholder acceptance template `config/dino-acceptance.example.yaml`
- next: real 2–3 plate Eddie interactive-session acceptance from a fresh scratch root (loops 650/660). No end-to-end cluster run has happened yet on the successor line.
- prior phases: 3.0 (verified batch cleanup + `batch_status.csv` ledger) and 3.1 (durable stage-out evidence, driver diagnostics, interactive launcher) landed; 2.9 (feature-export quality gate) gate-passed. Cleanup policy, batch ledger, and stage-out verification are live in `cptools2/__main__.py`. Baseline DINOv2, Cell-DINO, and channel-adaptive DINO came in via the consolidation.
- staging: forward line pushed to private ECDF GitLab (remote `gitlab`); public GitHub `origin` is untouched and never pushed without explicit say-so.
- before acceptance: verify the illumination-correction fix (LoadData CSV + manifest rewrite) actually applies to ImageXpress GUID filenames — `illum` has silently no-op'd on that layout, and the acceptance config runs `stages: [illum, segment, extract]`.
