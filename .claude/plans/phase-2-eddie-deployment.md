# Phase 2: Eddie Deployment — Containers, Staging, and End-to-End Testing

## Objective

Deploy cptools2's Nextflow pipeline to Eddie HPC with Singularity containers, DataStore staging, and scratch-aware batch orchestration, ready for end-to-end testing on real imaging data.

## Scope

### Included:
- Build and deploy 3 Singularity containers to <group> group space
- Parameterize build_containers.sh for reuse across group spaces
- Create STAGE_IN/STAGE_OUT Nextflow processes on -q staging queue
- Migrate batch logic from job.py to standalone batch.py module
- Wire batch-aware orchestration into cmd_pipeline (subprocess.run loop)
- Add scratch quota pre-flight check (lfs → df → YAML config fallback)
- Remove dead eddie_container_dir from containers.config
- Create install_eddie.sh for shared conda env deployment
- Write 6-7 unit tests for batch.py
- Run end-to-end test on a representative example-screen Cell Painting plate on Eddie

### Explicitly NOT included:
- DINOv2 container build (placeholder only, no Dockerfile yet)
- Cellpose as a segmentation option in main.nf (CellProfiler segmentation only)
- Drug-Discovery group space deployment (path TBD, install script is parameterized)
- CI/CD for container builds (manual qsub for now)
- Automated scratch cleanup after pipeline completion
- Multi-user permission management on group space

## Key Deliverables

| Deliverable | Format | Location |
|-------------|--------|----------|
| 3 Singularity .sif containers | .sif files | Eddie: .../<group>/cptools2/containers/ |
| Container manifest | JSON | Eddie: .../cptools2/containers/cptools2_containers.json |
| STAGE_IN process | .nf module | nextflow/modules/stage_in.nf |
| STAGE_OUT process | .nf module | nextflow/modules/stage_out.nf |
| Batch orchestration module | Python | cptools2/batch.py |
| Eddie install script | Bash | scripts/install_eddie.sh |
| Batch unit tests | Python | tests/test_batch.py |
| Updated main.nf with staging | .nf | nextflow/main.nf |
| Updated build_containers.sh | Bash | cptools2/dockerfiles/build_containers.sh |

## Success Criteria

- ✓ 3 .sif containers on Eddie, all pass validation commands
- ✓ `singularity exec cellprofiler_4.2.8.sif cellprofiler --version` succeeds
- ✓ `singularity exec --nv deepprofiler_1.0.sif python -c "import tensorflow as tf; assert len(tf.config.list_physical_devices('GPU')) > 0"` succeeds
- ✓ `singularity exec --nv cellpose_sam_1.0.sif python -c "import torch; assert torch.cuda.is_available()"` succeeds
- ✓ STAGE_IN/STAGE_OUT processes run on -q staging queue
- ✓ batch.py create_batches produces correct batches for test data (75% util, 30% overhead)
- ✓ cmd_pipeline runs subprocess.run per-batch (not os.execvp)
- ✓ Scratch quota pre-flight warns at 80%+
- ✓ 6+ new unit tests pass for batch.py
- ✓ Full pipeline completes on Eddie with 1 real plate end-to-end
- ✓ Output at output_dir/plate_id/{illum_functions,corrected_images,segmentation,features}/
- ✓ build_containers.sh accepts CONTAINER_DIR as parameter
- ✓ eddie_container_dir removed from containers.config
- ✓ All existing 140 tests still pass

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
| Scratch quota insufficient for representative plate | Med | Med | example-screen plate `example-plate-001` is ~103G. Stage to scratch first and check quota before full compute. |
| Container path mismatch between Python and Nextflow | Low | High | params.json resolves .sif paths via containers.py. Single source of truth. |

## Assumptions

- `Docker Desktop with WSL2 backend is available on the local machine`: Needed for building DeepProfiler and Cellpose images. Verify with `docker info`.
- `Eddie scratch has enough space for the representative example-screen plate`: `example-plate-001` is ~103G before derived outputs. Check scratch before full compute.
- `Nextflow 25.10.4 is still installed at ~/.local/bin/nextflow on Eddie`: Validated in prior session (job 55013075). Verify with `nextflow -version`.
- `conda is available on Eddie`: Either via module load or existing miniconda install. Needed for install_eddie.sh.
- `The staging queue (-q staging) is accessible to the user`: Eddie staging queue requires no special permissions, just the correct qsub flags.

## Notes / Design Decisions

1. **Single cptools2/ folder per group space** — containers/, env/, and nextflow/ all under one directory. Clean, discoverable, easy to explain to other labs.
2. **subprocess.run instead of os.execvp** — enables the batch loop (multiple Nextflow invocations per cptools2 run). os.execvp replaces the process.
3. **batch.py is a migration, not new logic** — same algorithm as job.py:327-367. job.py kept for legacy generate command backward compatibility.
4. **Staging h_rt override to 4h** — Eddie staging queue default may be 1h, insufficient for large plates.
5. **Container symlinks for Drug-Discovery** — avoid storing 20GB of .sif files twice. Symlink from <group>.
6. **Activation script sets CPTOOLS2_CONTAINER_DIR** — containers.py reads this env var. Zero YAML config needed for container resolution on Eddie.
7. **DataStore staging is mandatory** — DataStore paths are visible from staging nodes, not normal compute/GPU jobs. YAML configs backed by `/datastore/` must use `stage_data: true`.
8. **Loop 230 is a representative scaling test** — the example-screen validation plate is not small. The goal is to prove staged Nextflow orchestration on realistic data, not quick local smoke testing.

## Ralph Loops (3)

| Loop | Name | Type | Key Outputs |
|------|------|------|-------------|
| 210 | Container Build & Deploy | Infrastructure | 3 .sif files on Eddie, manifest deployed, build script parameterized |
| 220 | Staging + Batch Orchestration | Implementation | stage_in.nf, stage_out.nf, batch.py, cmd_pipeline batch loop, unit tests |
| 230 | End-to-End Eddie Test | Validation | Representative example-screen staged run, output verification, install script tested |
