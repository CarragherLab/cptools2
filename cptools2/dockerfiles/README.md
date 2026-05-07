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

Keep these archives out of Git. They are temporary transfer/build inputs for
the Eddie container directory, not repository assets.

```bash
docker save -o cellprofiler_4.2.8.tar cellprofiler/cellprofiler:4.2.8
docker save -o deepprofiler_1.0.tar cptools2/deepprofiler:1.0
docker save -o cellpose_sam_1.0.tar cptools2/cellpose_sam:1.0
```

## Step 4: Transfer to Eddie or Use Existing Lab Containers

Lab Eddie users should normally reuse the prebuilt `.sif` files in the
configured permanent container directory. The installer checks for:

- `cellprofiler_4.2.8.sif`
- `deepprofiler_1.0.sif`
- `cellpose_sam_1.0.sif`
- `cptools2_containers.json`

External users can build their own containers from the Dockerfiles and Docker
image references recorded in `cptools2/container_manifest_template.json`.

```bash
EDDIE_CONTAINERS=<UUN>@eddie.ecdf.ed.ac.uk:/exports/<college>/eddie/<school>/groups/<group>/cptools2/containers/

rsync -avzP cellprofiler_4.2.8.tar "$EDDIE_CONTAINERS"
rsync -avzP deepprofiler_1.0.tar "$EDDIE_CONTAINERS"
rsync -avzP cellpose_sam_1.0.tar "$EDDIE_CONTAINERS"
rsync -avzP ../container_manifest_template.json "$EDDIE_CONTAINERS/cptools2_containers.json"
```

Use `rsync -avzP` (not `scp`) so transfers are resumable if interrupted.
After conversion, record checksums in the manifest:

```bash
cd /exports/<college>/eddie/<school>/groups/<group>/cptools2/containers
sha256sum *.sif
```

## Step 5: Singularity Conversion on Eddie

**Never build on the login node.** Submit the build as an SGE job:

```bash
qsub build_containers.sh
```

This script:
- Sets `SINGULARITY_TMPDIR` to scratch (Eddie's `/tmp` is too small)
- Loads the `singularity` module
- Converts all three Docker archives to `.sif` format
- Leaves `.sif` files in the permanent container directory for lab reuse
- Requests 16G memory; increase to 32G if builds fail with memory errors

## Step 6: Validation on Eddie

### CellProfiler (CPU node)

```bash
qlogin -l h_rss=8G -l h_rt=00:30:00
module load singularity

singularity exec \
    /exports/<college>/eddie/<school>/groups/<group>/cptools2/containers/cellprofiler_4.2.8.sif \
    cellprofiler --version
```

### DeepProfiler and Cellpose-SAM (GPU node)

```bash
qlogin -q gpu -l gpu=1 -l h_rss=16G -l h_rt=01:00:00
module load singularity

# DeepProfiler
singularity exec --nv \
    /exports/<college>/eddie/<school>/groups/<group>/cptools2/containers/deepprofiler_1.0.sif \
    python -c "import tensorflow as tf; assert len(tf.config.list_physical_devices('GPU')) > 0"

# Cellpose-SAM
singularity exec --nv \
    /exports/<college>/eddie/<school>/groups/<group>/cptools2/containers/cellpose_sam_1.0.sif \
    python -c "import torch; assert torch.cuda.is_available()"
```

The `--nv` flag maps host NVIDIA drivers into the container. Without it, GPU frameworks
silently fall back to CPU. Use `-q gpu -l gpu=1`; Eddie exposes `gpus` as a
non-requestable complex. If a GPU job needs multiple CPU slots, request those
with the standard `sharedmem` PE.
