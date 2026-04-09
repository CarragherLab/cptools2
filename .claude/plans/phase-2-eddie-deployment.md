# Phase 2: Eddie Deployment — Containers, Staging, Cellpose + DeepProfiler Bridge, End-to-End Testing

> **Scope expanded 2026-04-09**: Loop 230 was originally "end-to-end Eddie test"
> but has been rewritten and split into Loop 230 (code + tests + docs for cellpose
> + DeepProfiler bridge) and Loop 240 (end-to-end Eddie validation). Rationale:
> walkthrough revealed that the segmentation template was a dead-end for the user's
> actual workflow, DeepProfiler needs a pipeline-generated index.csv, and weights
> weren't deployed anywhere. Design doc at
> `.claude/plans/2026-04-09-cellpose-deepprofiler-design.md` (APPROVED via
> /office-hours, CLEARED via /plan-eng-review).

## Objective

Deploy cptools2's Nextflow pipeline to Eddie HPC with Singularity containers, DataStore staging, scratch-aware batch orchestration, Cellpose-SAM segmentation, and a DeepProfiler feature extraction bridge — ready for end-to-end testing on real imaging data.

## Scope

### Included:
**Loop 210 (complete):**
- Build and deploy 3 Singularity containers to chandranlabs + Drug-Discovery group spaces
- Parameterize build_containers.sh for reuse across group spaces
- Remove dead eddie_container_dir from containers.config

**Loop 220 (complete):**
- Create STAGE_IN/STAGE_OUT Nextflow processes on -q staging queue
- Migrate batch logic from job.py to standalone batch.py module
- Wire batch-aware orchestration into cmd_pipeline (subprocess.run loop)
- Add scratch quota pre-flight check (lfs → df → YAML config fallback)
- Create install_eddie.sh for shared conda env deployment
- 8 unit tests for batch.py (148 total tests passing)

**Loop 230 (pending, rewritten — Cellpose + DeepProfiler Bridge):**
- Update illum_calculate.cppipe + illum_apply.cppipe templates for w1/w2/w3 naming + 16-bit TIFF output
- Create cptools2/cellpose_segmentation.py importable library (grouping, centroid extraction, CSV writing)
- Create nextflow/bin/run_cellpose_segmentation.py thin CLI wrapper
- Create nextflow/modules/segmentation_cellpose.nf Nextflow process with OOM retry
- Create cptools2/deepprofiler_index.py importable library (scan, build rows, write CSV, plate_map loader)
- Create nextflow/bin/make_deepprofiler_index.py thin CLI wrapper
- Update nextflow/modules/feature_extract.nf to generate index.csv before profile, bind-mount weights
- Update nextflow/main.nf with params.segmentation tool switch + weights pre-flight
- Update cptools2/containers.py with resolve_weights_path() helper
- Update cptools2/templates/deepprofiler_config.json (channels w1..w5, file_format tiff, bits 16)
- Create scripts/deploy_deepprofiler_weights.sh (Zenodo download + sha256 + group-space deploy)
- Update scripts/install_eddie.sh to export CPTOOLS2_WEIGHTS_DIR in activate.sh
- Create docs/illumination_pipeline_setup.md (user guide for illum template + adaptation)
- Create docs/deepprofiler_integration.md (DeepProfiler project layout, index contract, weights)
- Write 32+ unit tests across 5 test files (test_cellpose_segmentation, test_deepprofiler_index, test_deepprofiler_config, test_containers extension, test_parse_yaml extension)
- All 148 existing tests still pass; 180+ total

**Loop 240 (pending, new — End-to-End Eddie Validation):**
- Deploy cptools2 via install_eddie.sh
- Deploy DeepProfiler weights via deploy_deepprofiler_weights.sh
- Create test YAML config pointing at a small plate
- Dry-run validation (cptools2 pipeline --dry-run)
- Illum-only stage test (cptools2 pipeline --stages illum)
- Segmentation stage test (Cellpose-SAM on Eddie GPU queue)
- Feature extraction stage test (DeepProfiler on Eddie GPU queue)
- Full pipeline end-to-end run
- Scratch quota compliance check
- Record job IDs, wall times, retro notes

### Explicitly NOT included:
- DINOv2 container build (placeholder only, no Dockerfile yet)
- Cell (whole-cell) segmentation — only nucleus segmentation, per user decision
- Pycytominer normalization/aggregation — downstream of Phase 2
- CI/CD for container builds (manual qsub for now)
- Automated scratch cleanup after pipeline completion
- Multi-user permission management on group space
- Pretrained weights baked into container (separated for updateability)
- Automatic plate map generation from LIMS
- CI test for cppipe templates (requires CellProfiler in test env)
- Cross-cell-line feature normalization

## Key Deliverables

**Loop 210 + 220 (complete):**

| Deliverable | Format | Location |
|-------------|--------|----------|
| 3 Singularity .sif containers × 2 groups | .sif files | Eddie: .../{chandranlabs,Drug-Discovery}/cptools2/containers/ |
| Container manifest | JSON | Eddie: .../cptools2/containers/cptools2_containers.json |
| STAGE_IN process | .nf module | nextflow/modules/stage_in.nf |
| STAGE_OUT process | .nf module | nextflow/modules/stage_out.nf |
| Batch orchestration module | Python | cptools2/batch.py |
| Eddie install script | Bash | scripts/install_eddie.sh |
| Batch unit tests | Python | tests/test_batch.py |
| Updated main.nf with staging | .nf | nextflow/main.nf |
| Updated build_containers.sh | Bash | cptools2/dockerfiles/build_containers.sh |

**Loop 230 (new — Cellpose + DeepProfiler Bridge):**

| Deliverable | Format | Location |
|-------------|--------|----------|
| Cellpose segmentation library | Python | cptools2/cellpose_segmentation.py |
| Cellpose CLI wrapper | Python | nextflow/bin/run_cellpose_segmentation.py |
| Cellpose Nextflow module | .nf | nextflow/modules/segmentation_cellpose.nf |
| DeepProfiler index library | Python | cptools2/deepprofiler_index.py |
| DeepProfiler index CLI wrapper | Python | nextflow/bin/make_deepprofiler_index.py |
| Feature_extract module update | .nf | nextflow/modules/feature_extract.nf |
| Main.nf tool switch + pre-flight | .nf | nextflow/main.nf |
| Weights path resolver | Python | cptools2/containers.py (extended) |
| Updated DeepProfiler config | JSON | cptools2/templates/deepprofiler_config.json |
| Weights deployment script | Bash | scripts/deploy_deepprofiler_weights.sh |
| Install script update | Bash | scripts/install_eddie.sh (extended) |
| Updated illum templates | .cppipe × 2 | cptools2/templates/illum_{calculate,apply}.cppipe |
| Illumination user docs | Markdown | docs/illumination_pipeline_setup.md |
| DeepProfiler integration docs | Markdown | docs/deepprofiler_integration.md |
| Cellpose unit tests | Python | tests/test_cellpose_segmentation.py (~11 tests) |
| DeepProfiler index unit tests | Python | tests/test_deepprofiler_index.py (~11 tests) |
| DeepProfiler config validity test | Python | tests/test_deepprofiler_config.py (~4 tests) |
| Container resolver tests | Python | tests/test_containers.py (extended, +4 tests) |
| Parse_yaml tests | Python | tests/test_parse_yaml.py (extended, +3 tests) |

**Loop 240 (new — End-to-End Eddie Validation):**

| Deliverable | Format | Location |
|-------------|--------|----------|
| Test YAML config | YAML | On Eddie, user-specified |
| Dry-run params.json | JSON | Output of --dry-run |
| Illum stage outputs | .npy + .tiff | output_dir/plate_id/{illum_functions,corrected_images}/ |
| Segmentation outputs | .tiff + .csv | output_dir/plate_id/segmentation/ |
| Feature outputs | .npz | output_dir/plate_id/features/ |
| SGE job retrospective | Markdown | dev_docs/retros/2026-xx-xx_loop_240_eddie_validation.md |

## Success Criteria

**Loop 210 + 220 (met):**
- ✓ 3 .sif containers on Eddie × 2 group spaces, all pass validation commands
- ✓ STAGE_IN/STAGE_OUT processes run on -q staging queue
- ✓ batch.py create_batches produces correct batches for test data (75% util, 30% overhead)
- ✓ cmd_pipeline runs subprocess.run per-batch (not os.execvp)
- ✓ Scratch quota pre-flight warns at 80%+
- ✓ build_containers.sh accepts CONTAINER_DIR as parameter
- ✓ eddie_container_dir removed from containers.config
- ✓ 148 tests pass

**Loop 230 (pending):**
- ✓ All 148 existing tests still pass
- ✓ 32+ new unit tests pass across 5 test files
- ✓ `python -m pytest` exits 0 with ~180 tests collected
- ✓ All new Python files pass ruff + black + isort
- ✓ `cptools2/cellpose_segmentation.py` exports group_images_by_site, extract_centroids, write_location_csv, run_cellpose_on_batch
- ✓ `cptools2/deepprofiler_index.py` exports scan_corrected_dir, build_index_rows, write_index_csv, load_plate_map
- ✓ `cptools2/containers.py:resolve_weights_path()` exists with env var + YAML resolution
- ✓ `nextflow/bin/run_cellpose_segmentation.py` and `make_deepprofiler_index.py` are thin CLI wrappers using argparse
- ✓ `nextflow/modules/segmentation_cellpose.nf` has errorStrategy 'retry' + memory scaling
- ✓ `nextflow/modules/feature_extract.nf` calls make_deepprofiler_index.py before profile
- ✓ `nextflow/main.nf` has params.segmentation.tool switch and weights pre-flight
- ✓ `cptools2/templates/illum_{calculate,apply}.cppipe` NamesAndTypes rules match w1..w5 metadata
- ✓ `cptools2/templates/illum_apply.cppipe` SaveImages outputs 16-bit TIFF with w-number naming preserved
- ✓ `cptools2/templates/deepprofiler_config.json` has channels ["w1".."w5"], file_format "tiff", bits 16
- ✓ `scripts/deploy_deepprofiler_weights.sh` downloads from Zenodo, verifies sha256, deploys to parameterized group spaces
- ✓ `scripts/install_eddie.sh` activate.sh template exports CPTOOLS2_WEIGHTS_DIR
- ✓ `docs/illumination_pipeline_setup.md` documents template adaptation for 3/4/5/6 channel assays
- ✓ `docs/deepprofiler_integration.md` documents index.csv contract, plate_map format, weights location
- ✓ Git commit on ai-update branch, clean working tree
- ✓ Handoff summary written to phase-2-ralph-loops.md for Loop 240

**Loop 240 (pending):**
- ✓ install_eddie.sh deploys cleanly; activate.sh works
- ✓ deploy_deepprofiler_weights.sh deploys weights to chandranlabs + Drug-Discovery
- ✓ `cptools2 pipeline test_config.yml --dry-run` produces valid params.json with resolved container + weights paths
- ✓ Illum stage runs on one real small plate; outputs TIFFs in expected layout
- ✓ Cellpose segmentation job queues on gpu-a100 PE, runs to completion, produces masks + DeepProfiler-format location CSVs
- ✓ DeepProfiler feature_extract job queues on gpu-a100 PE, runs to completion, produces .npz files with shape (n_cells, 672)
- ✓ Full pipeline `cptools2 pipeline test_config.yml` runs end-to-end
- ✓ Scratch usage stays below 75% of quota
- ✓ All SGE job exit_status = 0 in qacct
- ✓ STAGE_OUT copies outputs back to DataStore
- ✓ Retrospective written with job IDs, wall times, any gotchas encountered

## Dependencies

### Must Complete Before:
- Phase 1 (Code Review Fixes): COMPLETE — 140 tests pass, all critical bugs fixed

### Blocked By:
- Docker Desktop + WSL2 on local machine: needed for container builds
- Eddie SSH access: needed for all deployment and testing
- GPU node availability on Eddie: needed for DeepProfiler/Cellpose validation
- DataStore access: needed for staging test

### Optional:
- Drug-Discovery group space path: helpful for testing dual-deployment, not blocking

## Skills Required (Broad Categories)

- `docker/singularity`: Container builds, conversion, validation
- `eddie-hpc`: SGE job submission, scratch management, module system
- `nextflow`: DSL2 process definitions, staging label, publishDir
- `python`: batch.py module, subprocess integration, pytest

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Docker build fails (dependency issues) | Med | Med | Test builds locally before transfer. Pin versions in Dockerfiles. |
| Singularity conversion OOM on Eddie | Low | Med | build_containers.sh requests 16G. Increase to 32G if needed. |
| GPU node unavailable for validation | Med | Low | Validation can wait. CPU containers tested first. |
| DataStore NFS flaky during staging | Med | Med | rsync --partial --timeout=300. Staging retry via Nextflow errorStrategy. |
| lfs quota command unavailable | Low | Low | Fallback to df, then YAML config scratch_quota_gb. |
| Scratch quota insufficient for test plate | Low | Med | Use small plate (1 plate, ~100 images). Check quota pre-flight. |
| Container path mismatch between Python and Nextflow | Low | High | params.json resolves .sif paths via containers.py. Single source of truth. |

## Assumptions

- `Docker Desktop with WSL2 backend is available on the local machine`: Needed for building DeepProfiler and Cellpose images. Verify with `docker info`.
- `Eddie scratch has enough space for a small test plate`: ~1GB for 100 images. Check with `lfs quota` before starting C3.
- `Nextflow 25.10.4 is still installed at ~/.local/bin/nextflow on Eddie`: Validated in prior session (job 55013075). Verify with `nextflow -version`.
- `conda is available on Eddie`: Either via module load or existing miniconda install. Needed for install_eddie.sh.
- `The staging queue (-q staging) is accessible to the user`: Eddie staging queue requires no special permissions, just the correct qsub flags.

## Notes / Design Decisions

1. **Single cptools2/ folder per group space** — containers/, env/, and nextflow/ all under one directory. Clean, discoverable, easy to explain to other labs.
2. **subprocess.run instead of os.execvp** — enables the batch loop (multiple Nextflow invocations per cptools2 run). os.execvp replaces the process.
3. **batch.py is a migration, not new logic** — same algorithm as job.py:327-367. job.py kept for legacy generate command backward compatibility.
4. **Staging h_rt override to 4h** — Eddie staging queue default may be 1h, insufficient for large plates.
5. **Container symlinks for Drug-Discovery** — avoid storing 20GB of .sif files twice. Symlink from chandranlabs.
6. **Activation script sets CPTOOLS2_CONTAINER_DIR** — containers.py reads this env var. Zero YAML config needed for container resolution on Eddie.

## Ralph Loops (4) — expanded from 3 per 2026-04-09 scope revision

| Loop | Name | Type | Status | Key Outputs |
|------|------|------|--------|-------------|
| 210 | Container Build & Deploy | Infrastructure | ✓ complete | 3 .sif files × 2 group spaces, manifest deployed, build script parameterized |
| 220 | Staging + Batch Orchestration | Implementation | ✓ complete | stage_in.nf, stage_out.nf, batch.py, cmd_pipeline batch loop, 8 unit tests, 148 tests passing |
| 230 | Cellpose + DeepProfiler Bridge | Implementation | pending | 12 new files + 9 modified files, cellpose-SAM segmentation, DeepProfiler index generator, weights deployment, user docs, 32+ new unit tests |
| 240 | End-to-End Eddie Validation | Validation | pending | Full pipeline run on real plate, output verification, install script tested, retrospective |

### Parallelization strategy (Loop 230)

Three independent lanes, documented in the design doc:

| Lane | Scope | Depends on |
|------|-------|------------|
| A | Cellpose library + CLI + Nextflow module + tests | — |
| B | DeepProfiler index library + CLI + feature_extract update + config + tests | — |
| C | Templates + main.nf wiring + containers.py + install + docs + weights deploy | A, B (main.nf imports) |

Execution: Launch A + B in parallel worktrees. Merge. Then C sequentially.
