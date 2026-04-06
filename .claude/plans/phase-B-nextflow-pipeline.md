# Phase B: Nextflow Pipeline

## Objective

Build the Nextflow DSL2 pipeline with processes for illumination correction (calculate + apply), segmentation (nuclear + cell), and feature extraction (DeepProfiler/DINOv2), using the validated Eddie config and CellProfiler pipeline templates.

## Scope

### Included:
- `nextflow/main.nf`: pipeline entry point with configurable stage selection
- `nextflow/modules/illum_calculate.nf`: per-plate illumination function calculation
- `nextflow/modules/illum_apply.nf`: apply correction, output 16-bit PNG
- `nextflow/modules/segmentation.nf`: nuclear + cell body segmentation
- `nextflow/modules/feature_extract.nf`: role-agnostic feature extraction (DeepProfiler, DINOv2)
- `nextflow/modules/data_prep.nf`: cptools2 Python data preparation as a Nextflow process
- `nextflow/nextflow.config`: base config with profile includes
- `nextflow/conf/eddie.config`: already validated (refine as needed)
- `nextflow/conf/test.config`: local executor + Docker for CI testing
- `nextflow/conf/containers.config`: container path configuration per profile
- `cptools2/templates/illum_calculate.cppipe`: CellProfiler pipeline template
- `cptools2/templates/illum_apply.cppipe`: CellProfiler pipeline template
- `cptools2/templates/nuclear_segmentation.cppipe`: CellProfiler pipeline template
- `cptools2/templates/cell_segmentation.cppipe`: CellProfiler pipeline template
- `cptools2/templates/deepprofiler_config.json`: DeepProfiler config template
- Small test dataset for `-profile test` CI runs

### Explicitly NOT included:
- Python library changes (Lane A handles polars migration)
- Container building/deployment to Eddie (Lane C)
- CLI wrapper or parse_yaml changes (Lane D)
- GPU validation on Eddie (Lane C, requires deployed containers)
- nf-core submission (future work)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Pipeline entry point | Nextflow DSL2 | `nextflow/main.nf` |
| Illum calculate process | Nextflow DSL2 | `nextflow/modules/illum_calculate.nf` |
| Illum apply process | Nextflow DSL2 | `nextflow/modules/illum_apply.nf` |
| Segmentation process | Nextflow DSL2 | `nextflow/modules/segmentation.nf` |
| Feature extraction process | Nextflow DSL2 | `nextflow/modules/feature_extract.nf` |
| Data preparation process | Nextflow DSL2 | `nextflow/modules/data_prep.nf` |
| Base config | Nextflow config | `nextflow/nextflow.config` |
| Eddie profile (refined) | Nextflow config | `nextflow/conf/eddie.config` |
| Test profile | Nextflow config | `nextflow/conf/test.config` |
| Container config | Nextflow config | `nextflow/conf/containers.config` |
| Illum calculate pipeline | CellProfiler .cppipe | `cptools2/templates/illum_calculate.cppipe` |
| Illum apply pipeline | CellProfiler .cppipe | `cptools2/templates/illum_apply.cppipe` |
| Nuclear segmentation pipeline | CellProfiler .cppipe | `cptools2/templates/nuclear_segmentation.cppipe` |
| Cell segmentation pipeline | CellProfiler .cppipe | `cptools2/templates/cell_segmentation.cppipe` |
| DeepProfiler config template | JSON | `cptools2/templates/deepprofiler_config.json` |
| CI test data | Images + CSV | `tests/nf-test-data/` |

## Success Criteria

- ✓ `nextflow run nextflow/main.nf -profile test` completes with exit 0 using local executor and Docker
- ✓ Pipeline accepts `--stages` parameter to select which stages to run (illum, segment, extract, all)
- ✓ Each process has correct labels matching `eddie.config` resource definitions (illum_calculate, illum_apply, segmentation, feature_extract, gpu, staging)
- ✓ `illum_calculate` process uses CellProfiler container, runs per-plate with 4 CPUs / 16GB
- ✓ `illum_apply` depends on `illum_calculate` output (channel wiring)
- ✓ `segmentation` depends on `illum_apply` output
- ✓ `feature_extract` depends on `segmentation` output and has `label 'gpu'` for `--nv` flag
- ✓ `feature_extract` container is parameterized via `params.feature_extraction_tool` (role-agnostic)
- ✓ Channel configuration is parameterized via `params.channels` (not hardcoded)
- ✓ `.cppipe` templates load correctly in CellProfiler GUI (manual verification)
- ✓ Pipeline supports `-resume` for Nextflow caching

## Dependencies

### Must Complete Before:
- None — pipeline can be written and tested with `-profile test` (local executor + Docker) without any other lane

### Blocked By:
- Docker Desktop (for local `-profile test` runs with CellProfiler container)

### Optional:
- Lane A (polars migration): `data_prep.nf` process calls cptools2 Python scripts for LoadData generation. Can use pandas versions initially, swap to polars after Lane A merges.
- Lane C (containers): Eddie end-to-end testing requires deployed `.sif` files, but pipeline development uses Docker locally.

## Skills Required (Broad Categories)

- `nextflow`: DSL2 pipeline structure, process definitions, channel operations, config profiles
- `eddie-resources`: SGE resource labels (h_rss, gpu-a100, staging queue)
- `eddie-script-standards`: module loading, bind mounts, SINGULARITY_TMPDIR

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| DSL2 channel wiring complexity for multi-stage pipeline | Medium | Medium | Start with 2-process pipeline (illum_calculate → illum_apply), add stages incrementally |
| CellProfiler .cppipe templates don't parse correctly | Medium | Medium | Validate each template in CellProfiler GUI before integrating into pipeline |
| Feature extraction process is too DeepProfiler-specific | Low | Medium | Parameterize fully: container, command, and config are all from params, not hardcoded |
| Test data too large for repo | Low | Low | Use minimal synthetic images (8x8 pixel TIFFs, 2 channels, 1 plate, 2 wells, 1 site) |
| Nextflow version incompatibility with Eddie's Java | Low | Low | Tested: Nextflow 25.10.4 works with Java 21 on Eddie |

## Assumptions

- `Nextflow DSL2 supports SGE array-like patterns`: Nextflow handles per-plate parallelism natively via channels, no explicit array job configuration needed
- `CellProfiler 4.2.8 headless mode works in Singularity`: validated in dev_docs/guide_01 and guide_02
- `Docker available locally for -profile test`: Docker Desktop on WSL2 can run CellProfiler container
- `Channel wiring follows standard Nextflow patterns`: output of one process feeds as input channel to the next

## Notes / Design Decisions

- **Worktree isolation**: this lane touches ONLY files under `nextflow/` and `cptools2/templates/`. No overlap with Lanes A, C, or D.
- **data_prep.nf process**: wraps cptools2 Python scripts (filelist, loaddata, splitter) as a Nextflow process that runs before the pipeline stages. This keeps the Python library as a data preparation tool, not an orchestrator.
- **Channel-based stage selection**: `params.stages` list controls which processes are included in the workflow. Nextflow conditional workflow inclusion (`if params.stages.contains('illum')`) gates each stage.
- **Role-agnostic feature extraction**: the `feature_extract.nf` process reads `params.feature_extraction.tool` to select the container and command. The process script is a template, not DeepProfiler-specific code.
- **CellProfiler templates**: static `.cppipe` files with placeholder paths. The Nextflow process substitutes actual paths via command-line arguments to CellProfiler. Templates are inspectable in CellProfiler GUI.

## Ralph Loops (5)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| B10 | Nextflow scaffold + configs | Implementation | main.nf skeleton, nextflow.config, eddie.config refined, test.config, containers.config |
| B20 | Illumination correction processes + templates | Implementation | illum_calculate.nf, illum_apply.nf, illum_calculate.cppipe, illum_apply.cppipe |
| B30 | Segmentation process + templates | Implementation | segmentation.nf, nuclear_segmentation.cppipe, cell_segmentation.cppipe |
| B40 | Feature extraction process + templates | Implementation | feature_extract.nf, deepprofiler_config.json, role-agnostic container selection |
| B50 | Test profile + CI data + integration test | Implementation | test.config, nf-test-data/, `nextflow run -profile test` passes |
