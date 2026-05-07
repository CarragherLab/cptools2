# Phase 0: Container Build & Deploy

## Objective

Build, test, and deploy three Singularity container images (CellProfiler, DeepProfiler, Cellpose-SAM) to Eddie group space, establishing the container infrastructure that all subsequent phases depend on.

## Scope

### Included:
- Dockerfiles for DeepProfiler and Cellpose-SAM (CellProfiler uses official image)
- Local Docker builds and GPU-passthrough testing on WSL2
- Docker archive creation, transfer to Eddie, Singularity conversion
- On-Eddie validation (CPU for CellProfiler, GPU for DeepProfiler + Cellpose)
- Container manifest (`cptools2_containers.json`) with role → image mapping

### Explicitly NOT included:
- cptools2 Python code changes (Phase 1)
- Illumination correction pipeline templates (Phase 2)
- Installer script for end users (out of scope entirely — admin-only workflow)

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| DeepProfiler Dockerfile | Dockerfile | `cptools2/dockerfiles/Dockerfile.deepprofiler` |
| Cellpose-SAM Dockerfile | Dockerfile | `cptools2/dockerfiles/Dockerfile.cellpose` |
| Dockerfiles README | Markdown | `cptools2/dockerfiles/README.md` |
| CellProfiler .sif | Singularity image | Eddie: `.../containers/cellprofiler_4.2.8.sif` |
| DeepProfiler .sif | Singularity image | Eddie: `.../containers/deepprofiler_1.0.sif` |
| Cellpose-SAM .sif | Singularity image | Eddie: `.../containers/cellpose_sam_1.0.sif` |
| Container manifest | JSON | Eddie: `.../containers/cptools2_containers.json` |
| SGE build job script | Shell | `cptools2/dockerfiles/build_containers.sh` |

## Success Criteria

- ✓ `docker run --rm cellprofiler/cellprofiler:4.2.8 cellprofiler --version` returns `4.2.8` locally
- ✓ `docker run --gpus all cptools2/deepprofiler:1.0` detects GPU via TensorFlow locally
- ✓ `docker run --gpus all cptools2/cellpose-sam:1.0` detects GPU via PyTorch locally
- ✓ All 3 `.sif` files exist in Eddie group space at `/exports/<college>/eddie/<school>/groups/<group>/containers/`
- ✓ `singularity exec cellprofiler_4.2.8.sif cellprofiler --version` returns `4.2.8` on Eddie CPU node
- ✓ `singularity exec --nv deepprofiler_1.0.sif python -c "import tensorflow as tf; assert len(tf.config.list_physical_devices('GPU')) > 0"` passes on Eddie GPU node
- ✓ `singularity exec --nv cellpose_sam_1.0.sif python -c "import torch; assert torch.cuda.is_available()"` passes on Eddie GPU node
- ✓ `cptools2_containers.json` manifest lists all 3 containers with correct image names, versions, and `gpu` flags
- ✓ Dockerfiles committed to `cptools2/dockerfiles/` in the repo

## Dependencies

### Must Complete Before This Phase:
- None — this is a root phase, can start immediately

### Blocked By:
- Eddie cluster access (SSH + group space write permissions)
- Docker Desktop with WSL2 backend + NVIDIA Container Toolkit on local machine
- GPU access on Eddie (need `-pe gpu-a100` queue time for validation)

### Optional:
- Phase 1 code changes: not needed for container build, but nice to validate together

## Skills Required (Broad Categories)

- `eddie-resources`: SGE job sizing for container builds (h_rss, SINGULARITY_TMPDIR)
- `eddie-script-standards`: Build job script formatting and directives
- `eddie-login`: SSH access for validation steps

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Docker Hub unreliable from Eddie compute nodes | Medium | Medium | Use local archive + rsync approach (docker save → transfer → docker-archive://) |
| GPU container build exceeds 16G h_rss | Low | Low | Increase to 32G if needed; SINGULARITY_TMPDIR on scratch |
| Eddie GPU queue wait too long for validation | Medium | Low | Can validate CellProfiler (CPU) immediately; GPU validation can wait |
| Cellpose/DeepProfiler dependency version conflicts | Low | High | Pin versions in Dockerfiles; test locally before transfer |
| /tmp too small for Singularity build extraction | High | Medium | Always set SINGULARITY_TMPDIR to scratch (documented in dev_docs) |

## Assumptions

- `Eddie group space writable`: User has write access to `/exports/<college>/eddie/<school>/groups/<group>/containers/` — validate by `touch` test on Eddie
- `WSL2 GPU passthrough`: Local machine has NVIDIA drivers ≥470 and NVIDIA Container Toolkit — validate with `nvidia-smi` in WSL2
- `Official CellProfiler image compatible`: `cellprofiler/cellprofiler:4.2.8` Docker image converts cleanly to Singularity — validated by many other labs
- `A100 CUDA compatibility`: Container CUDA runtimes (11.2 for TF, 11.8 for PyTorch) are compatible with Eddie A100 drivers — validate during GPU testing

## Notes / Design Decisions

- **Local archive approach preferred over direct pull**: Eddie compute nodes have unreliable Docker Hub access behind the university firewall. Building locally and transferring is deterministic.
- **CellProfiler uses official image**: No custom Dockerfile needed — the Broad Institute image is maintained and tested.
- **GPU containers use different CUDA bases**: DeepProfiler needs TF 2.5.3 (CUDA 11.2), Cellpose needs PyTorch (CUDA 11.8). Both should work with A100 drivers via `--nv` passthrough.
- **Manifest is role-based**: Designed for extensibility — adding DINOv2 later is just a new entry in the JSON.

## Ralph Loops (3)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 010 | Dockerfiles and local builds | Implementation | Dockerfiles, local Docker images, GPU test results |
| 020 | Eddie transfer and conversion | Implementation | .sif files on Eddie, SGE build job script |
| 030 | Eddie validation and manifest | Implementation | Validated containers, cptools2_containers.json |
