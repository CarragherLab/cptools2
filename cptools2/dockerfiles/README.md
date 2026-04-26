# Container Build Instructions

Three containers are used in the Cell Painting pipeline:

| Container | Base Image | GPU | Approx Size |
|---|---|---|---|
| CellProfiler 4.2.8 | `cellprofiler/cellprofiler:4.2.8` (official) | No | ~1.5 GB |
| DeepProfiler 1.0 | `nvidia/cuda:11.2.2-cudnn8-runtime-ubuntu20.04` | Yes | ~8-10 GB |
| Cellpose-SAM 1.0 | `nvidia/cuda:11.8.0-cudnn8-runtime-ubuntu22.04` | Yes | ~20-25 GB |

CellProfiler uses the official Docker Hub image directly (no custom Dockerfile).
DeepProfiler and Cellpose-SAM have custom Dockerfiles in this directory.

## Step 1: Local Docker Build (WSL2)

### Prerequisites

- Docker Desktop with WSL2 backend enabled
- NVIDIA Container Toolkit installed (for GPU testing)
- Windows NVIDIA drivers >= 470

### Verify GPU passthrough

```bash
nvidia-smi
docker run --gpus all --rm nvidia/cuda:11.8.0-base-ubuntu22.04 nvidia-smi
```

### Build images

```bash
# CellProfiler (pull official image)
docker pull cellprofiler/cellprofiler:4.2.8

# DeepProfiler
docker build -f Dockerfile.deepprofiler -t cptools2/deepprofiler:1.0 .

# Cellpose-SAM
docker build -f Dockerfile.cellpose -t cptools2/cellpose_sam:1.0 .
```

## Step 2: GPU Testing (Local)

```bash
# CellProfiler (CPU only)
docker run --rm cellprofiler/cellprofiler:4.2.8 cellprofiler --version

# DeepProfiler
docker run --gpus all --rm cptools2/deepprofiler:1.0 \
    python -m deepprofiler --help

# Cellpose-SAM
docker run --gpus all --rm cptools2/cellpose_sam:1.0 \
    python -c "import torch; print(f'CUDA: {torch.cuda.is_available()}, GPU: {torch.cuda.get_device_name(0)}')"
```

## Step 3: Save Docker Archives

```bash
docker save cellprofiler/cellprofiler:4.2.8 | gzip > cellprofiler_4.2.8.tar.gz
docker save cptools2/deepprofiler:1.0 | gzip > deepprofiler_1.0.tar.gz
docker save cptools2/cellpose_sam:1.0 | gzip > cellpose_sam_1.0.tar.gz
```

## Step 4: Transfer to Eddie

```bash
EDDIE_CONTAINERS=mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/scs/groups/chandranlabs/containers/

rsync -avzP cellprofiler_4.2.8.tar.gz "$EDDIE_CONTAINERS"
rsync -avzP deepprofiler_1.0.tar.gz "$EDDIE_CONTAINERS"
rsync -avzP cellpose_sam_1.0.tar.gz "$EDDIE_CONTAINERS"
```

Use `rsync -avzP` (not `scp`) so transfers are resumable if interrupted.

## Step 5: Singularity Conversion on Eddie

**Never build on the login node.** Submit the build as an SGE job:

```bash
qsub build_containers.sh
```

This script:
- Sets `SINGULARITY_TMPDIR` to scratch (Eddie's `/tmp` is too small)
- Loads the `singularity` module
- Converts all three Docker archives to `.sif` format
- Requests 16G memory; increase to 32G if builds fail with memory errors

## Step 6: Validation on Eddie

### CellProfiler (CPU node)

```bash
qlogin -l h_rss=8G -l h_rt=00:30:00
module load singularity

singularity exec \
    /exports/cmvm/eddie/scs/groups/chandranlabs/containers/cellprofiler_4.2.8.sif \
    cellprofiler --version
```

### DeepProfiler and Cellpose-SAM (GPU node)

```bash
qlogin -pe gpu-a100 1 -l gpu=1 -l h_rss=16G -l h_rt=01:00:00
module load singularity

# DeepProfiler
singularity exec --nv \
    /exports/cmvm/eddie/scs/groups/chandranlabs/containers/deepprofiler_1.0.sif \
    python -c "import tensorflow as tf; assert len(tf.config.list_physical_devices('GPU')) > 0"

# Cellpose-SAM
singularity exec --nv \
    /exports/cmvm/eddie/scs/groups/chandranlabs/containers/cellpose_sam_1.0.sif \
    python -c "import torch; assert torch.cuda.is_available()"
```

The `--nv` flag maps host NVIDIA drivers into the container. Without it, GPU frameworks
silently fall back to CPU. Use `-pe gpu-a100 1 -l gpu=1`; Eddie exposes `gpus`
as a non-requestable complex.
