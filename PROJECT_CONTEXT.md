# Project Context

## Project Aim

cptools2 is being revamped into a Nextflow-orchestrated image-analysis workflow tool for HPC environments, primarily Edinburgh's Eddie cluster. The branch should preserve the useful ideas and battle-tested methods from the existing SGE/CellProfiler implementation while rebuilding the orchestration model around Nextflow.

The upgrade is not only an orchestration change. The longer-term product direction is to support modular image-analysis engines: Cellpose as a vital segmentation engine, CellProfiler for classical workflows, and AI feature extraction starting with DeepProfiler before adding Cell-DINO and uniDINO. cptools2 should focus on segmentation plus per-site/per-image extraction, with single-cell outputs in scope at the end of the pipeline where the models support them. Aggregation, mechanism-of-action prediction, and hit calling remain out of scope for this stage.

DINO requirements are documented in Notion: "AI Embedding Models - DINO" (`https://www.notion.so/34dc4962c42381ffb5f1dc9ef2578883`), fetched on 2026-04-25.

## Current State

The repository is a Python CLI package with a legacy CellProfiler/SGE implementation and an emerging Nextflow pipeline implementation.

Current code evidence:

- `cptools2.__main__` exposes `pipeline`, `prepare`, and `join` subcommands.
- `generate` still exists as a deprecated command that exits with guidance to use `cptools2 pipeline <config.yml>`.
- `cptools2/job.py`, `commands.py`, `generate_scripts.py`, `filelist.py`, `splitter.py`, and `loaddata.py` contain the legacy SGE-oriented workflow logic.
- `nextflow/main.nf`, `nextflow/conf/*`, and `nextflow/modules/*` contain the Nextflow pipeline skeleton and stage modules.
- `cptools2/batch.py` contains scratch-aware batch logic migrated for the Nextflow path.
- `scripts/install_eddie.sh` exists for shared Eddie deployment setup.

Planning state indicates Phase 2, Eddie Deployment, is in progress. Loop 220, staging and batch orchestration, is marked completed. Loop 230, end-to-end Eddie testing, is the next validation loop.

## Tech Stack

- Core language: Python >=3.8
- CLI: `argparse`, exposed through the `cptools2` console script
- Workflow orchestration target: Nextflow DSL2
- HPC target: Eddie / SGE
- Container runtime target: Singularity
- Data processing: `polars`, `pyyaml`
- External CarragherLab dependencies: `parserix`, `scissorhands`
- Test framework: `pytest`
- Formatting and linting: black, isort, ruff, pre-commit

AI tooling direction:

- CellProfiler remains important as a classical workflow engine and a source of proven image-list/setup mechanics.
- Cellpose is a vital segmentation engine and should be part of the core pipeline architecture.
- DeepProfiler is the first AI feature extractor to target.
- Cell-DINO and uniDINO are the next DINO-family feature extractors to target.
- DINOv2 phase-contrast support is optional later and should not distract from DeepProfiler, Cell-DINO, and uniDINO.
- Users should explicitly choose feature extractors. Cell Painting may use DeepProfiler, Cell-DINO, or both when comparison is wanted.
- Outputs should stay per site/image, preserve method-native segmentation and feature exports, and include `plate_name`, `well`, `site`, and `image_path`.
- Single-cell outputs are in scope as an end-of-pipeline output granularity, but should not be treated as downstream aggregation or analysis.
- All DINO models currently identified are under non-commercial research licences; licence compatibility must be checked before commercial use.

Container architecture:

- `cptools2` container: CLI/runtime, config parsing, staging metadata, and Nextflow parameter generation.
- CellProfiler container: classical CellProfiler pipeline execution.
- Cellpose container: segmentation execution and mask export.
- Feature-extraction model containers: DeepProfiler, Cell-DINO, uniDINO, and any later DINOv2 baseline.

DINO model selection guidance:

- Let users select Cell-DINO, uniDINO, DeepProfiler, or combinations explicitly in config.
- Use Cell-DINO, specifically the `cell_dino_cp` checkpoint, when users want DINO features for standard 5-channel Cell Painting fluorescence experiments.
- Use uniDINO when users want DINO features for fluorescence assays that are not standard 5-channel Cell Painting, including variable channel counts and non-standard stains.
- Treat DINOv2 for phase contrast, brightfield, or other greyscale transmitted-light modalities as a later baseline option.
- Plan a future LoRA-fine-tuned DINOv2 model for phase contrast after baseline validation.

DINO model implementation notes:

- Cell-DINO: ViT-L/16, 384 x 384 input crops, 1024-dimensional image embeddings, PyTorch/DINOv2 repository, weights mounted as `/weights/cell_dino_cp.pth`.
- uniDINO: ViT-S/16, channel-agnostic per-channel inference, 384-dimensional embedding per channel, final embedding size `384 * N_channels`, weights mounted as `/weights/unidino.pth`.
- DINOv2 baseline: ViT-L family baseline for greyscale modalities via channel triplication to pseudo-RGB, 1024-dimensional embeddings, ImageNet-style normalisation.
- Proposed container base for each DINO model: `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`.
- Proposed embedding container interface: `embed.py --model <model_name> --input <tiles_dir> --output <embeddings.parquet>`, with uniDINO also accepting `--channels <N>`.

Current downstream analysis position:

- Do not build aggregation, well-level summaries, UMAP, similarity matrices, MoA classification, or hit calling into cptools2 during this revamp.
- Build single-cell output support late in the pipeline if Cellpose, DeepProfiler, or CellProfiler make it straightforward; otherwise keep the first implementation focused on image/site outputs.
- Make segmentation and per-site/per-image feature outputs easy for downstream tools to consume by preserving `plate_name`, `well`, `site`, and `image_path`.
- Batch can be inferred from `plate_name` where needed; full plate metadata is not a prerequisite for the first feature-extraction scope.

## Architecture Map

- `cptools2/__main__.py`: CLI entry point, command dispatch, Nextflow invocation path.
- `cptools2/parse_yaml.py`: YAML config parsing and params generation.
- `cptools2/containers.py`: container path resolution and manifest handling.
- `cptools2/batch.py`: scratch-aware plate batching for HPC execution.
- `cptools2/job.py`: legacy job orchestration model; useful source of proven SGE-era concepts.
- `cptools2/filelist.py`, `splitter.py`, `loaddata.py`: image discovery, chunking, and CellProfiler LoadData generation.
- `cptools2/file_tools.py`: post-analysis CSV joining and metadata enrichment.
- `nextflow/main.nf`: top-level Nextflow workflow.
- `nextflow/modules/`: stage-specific Nextflow processes, including staging and analysis steps.
- `nextflow/conf/`: Nextflow profiles and container configuration.
- `scripts/install_eddie.sh`: Eddie deployment/install helper.
- `tests/`: pytest coverage for CLI, config parsing, container resolution, batching, file tools, and legacy helpers.
- `.claude/plans/`: current phase and loop planning documents.

## Development Workflow

Install for development:

```bash
pip install -e .[dev]
```

Alternative with uv:

```bash
uv pip install .[dev]
```

Run tests:

```bash
pytest
```

or:

```bash
uv run pytest
```

Run lint/format hooks:

```bash
pre-commit run --all-files
```

Current canonical CLI direction:

```bash
cptools2 pipeline config.yml
cptools2 pipeline config.yml --dry-run
cptools2 prepare config.yml
cptools2 join --location /path/to/results --patterns Image.csv Cells.csv
```

Legacy command:

```bash
cptools2 generate config.yml
```

`generate` is deprecated in code and should be treated as legacy unless explicitly restored.

## Conventions

- Treat the legacy SGE implementation as a reference implementation for domain behavior, batching assumptions, metadata handling, and CellProfiler command construction.
- Treat Nextflow as the target orchestration layer for the revamped branch.
- Prefer test-backed migration of behavior from legacy modules into smaller reusable modules.
- Keep site-specific Eddie paths configurable through YAML, environment variables, or install scripts rather than hardcoding them into library code.
- Keep `tests/new_config.yaml` as the canonical lightweight config fixture unless it is intentionally replaced.
- Use black line length 88, isort black profile, and ruff rules configured in `pyproject.toml`.
- Preserve user or prior-agent work in the dirty worktree; do not revert unrelated plan or state files.

## Current Session Goals

- Establish durable project context for future agents and sessions.
- Record that the branch goal is a Nextflow-first revamp, not a small patch to the legacy SGE generator.
- Capture the clarified engine scope: Cellpose segmentation, CellProfiler classical workflows, DeepProfiler first, then Cell-DINO and uniDINO, with per-site/per-image outputs.
- Prepare the project for focused next-step planning around Eddie validation and AI-tooling design.

## Open Questions

- What are the exact easiest setup tasks that should form the baseline before Cellpose and DeepProfiler?
- Which Cellpose mask/export files define success for the first segmentation engine?
- Which DeepProfiler export files define success for the first usable extractor?
- What exact DINO checkpoints and weight storage paths should be used on Eddie?
- Should DINOv2 remain in the roadmap now, or should the plan focus only on Cell-DINO and uniDINO?
- What exact metadata naming convention should be used: `plate_name`, `well`, `site`, `image_path`, or CellProfiler-style `Metadata_*` names?
- For single-cell outputs, what object identity is required: `object_id`, `x`, `y`, mask label, or all of these?
- What real Eddie dataset should be used for Loop 230 end-to-end validation?
