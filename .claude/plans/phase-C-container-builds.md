# Phase C: Container Builds & Eddie Deployment

## Objective

Build three Singularity container images (CellProfiler, DeepProfiler, Cellpose-SAM), deploy to Eddie group space, validate on compute nodes (CPU + GPU), and write the container manifest.

## Scope

### Included:
- `cptools2/dockerfiles/Dockerfile.deepprofiler`: custom build (TF 2.5.3-gpu base)
- `cptools2/dockerfiles/Dockerfile.cellpose`: custom build (CUDA 11.8 + PyTorch base)
- `cptools2/dockerfiles/README.md`: build instructions
- Local Docker builds and GPU-passthrough testing on WSL2
- Docker archive creation (`docker save | gzip`)
- Transfer to Eddie group space (`rsync -avzP`)
- Singularity conversion on Eddie (SGE job, not login node)
- CellProfiler validation on CPU node (`qlogin`)
- DeepProfiler + Cellpose validation on GPU node (`-pe gpu-a100 1 -l gpus=1`, `--nv`)
- Container manifest (`cptools2_containers.json`) written to Eddie group space
- SGE build job script stored in repo

### Explicitly NOT included:
- Python library changes (Lane A)
- Nextflow pipeline (Lane B)
- CLI or parse_yaml changes (Lane D)
- DINOv2 container (future, after v1)
- Installer script for end users (admin-only workflow)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| DeepProfiler Dockerfile | Dockerfile | `cptools2/dockerfiles/Dockerfile.deepprofiler` |
| Cellpose-SAM Dockerfile | Dockerfile | `cptools2/dockerfiles/Dockerfile.cellpose` |
| Build instructions | Markdown | `cptools2/dockerfiles/README.md` |
| SGE build job script | Shell | `cptools2/dockerfiles/build_containers.sh` |
| CellProfiler .sif | Singularity image | Eddie: `.../containers/cellprofiler_4.2.8.sif` |
| DeepProfiler .sif | Singularity image | Eddie: `.../containers/deepprofiler_1.0.sif` |
| Cellpose-SAM .sif | Singularity image | Eddie: `.../containers/cellpose_sam_1.0.sif` |
| Container manifest | JSON | Eddie: `.../containers/cptools2_containers.json` |

## Success Criteria

- ✓ `docker run --rm cellprofiler/cellprofiler:4.2.8 cellprofiler --version` returns `4.2.8` locally
- ✓ `docker run --gpus all cptools2/deepprofiler:1.0 python -c "import tensorflow as tf; assert len(tf.config.list_physical_devices('GPU')) > 0"` passes locally
- ✓ `docker run --gpus all cptools2/cellpose-sam:1.0 python -c "import torch; assert torch.cuda.is_available()"` passes locally
- ✓ All 3 `.sif` files exist in Eddie group space at `/exports/<college>/eddie/<school>/groups/<group>/containers/`
- ✓ `singularity exec cellprofiler_4.2.8.sif cellprofiler --version` returns `4.2.8` on Eddie CPU node
- ✓ `singularity exec --nv deepprofiler_1.0.sif python -c "import tensorflow as tf; assert len(tf.config.list_physical_devices('GPU')) > 0"` passes on Eddie GPU node
- ✓ `singularity exec --nv cellpose_sam_1.0.sif python -c "import torch; assert torch.cuda.is_available()"` passes on Eddie GPU node
- ✓ `cptools2_containers.json` manifest lists all 3 containers with correct image names, versions, gpu flags, and `verified: true`
- ✓ Dockerfiles committed to `cptools2/dockerfiles/` in the repo

## Dependencies

### Must Complete Before:
- None — container builds are independent

### Blocked By:
- Docker Desktop with WSL2 backend + NVIDIA Container Toolkit (local builds)
- Eddie SSH access + group space write permissions
- Eddie GPU queue time (for GPU container validation)

### Optional:
- Lane B (Nextflow pipeline): end-to-end Nextflow + container testing requires both Lane B and Lane C complete

## Skills Required (Broad Categories)

- `eddie-login`: SSH access for transfer and validation
- `eddie-resources`: SGE job sizing for container builds (h_rss=16G)
- `eddie-script-standards`: build job script formatting

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Docker Hub unreliable from Eddie | Medium | Medium | Use local archive + rsync approach (docker save → transfer) |
| GPU container build exceeds 16G h_rss | Low | Low | Increase to 32G if needed |
| Eddie GPU queue wait delays validation | Medium | Low | Validate CellProfiler (CPU) first; GPU can wait |
| CUDA version mismatch with A100 drivers | Low | High | Container CUDA runtimes (11.2 TF, 11.8 PyTorch) are compatible with A100; `--nv` maps host drivers |
| /tmp too small for Singularity build | High | Medium | Always set SINGULARITY_TMPDIR to scratch |

## Assumptions

- `Eddie group space writable`: user has write access to `/exports/<college>/eddie/<school>/groups/<group>/containers/`
- `WSL2 GPU passthrough working`: NVIDIA drivers ≥470, Container Toolkit installed
- `Official CellProfiler image compatible`: `cellprofiler/cellprofiler:4.2.8` converts cleanly to Singularity

## Notes / Design Decisions

- **Worktree isolation**: this lane touches ONLY `cptools2/dockerfiles/` and Eddie remote operations. No overlap with Lanes A, B, or D.
- **Local archive approach**: build locally, save as .tar.gz, rsync to Eddie, convert with `docker-archive://`. Avoids Docker Hub access from Eddie compute nodes.
- **Build job script in repo**: `build_containers.sh` is a reusable SGE job for future container rebuilds.
- **Manifest verified flag**: set to `true` only after successful validation on Eddie compute node.

## Ralph Loops (3)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| C10 | Dockerfiles + local builds + GPU testing | Implementation | Dockerfiles, local Docker images, GPU passthrough verified |
| C20 | Eddie transfer + Singularity conversion | Implementation | .sif files on Eddie, SGE build job script |
| C30 | Eddie validation + manifest | Implementation | CPU + GPU validation, cptools2_containers.json with verified: true |
