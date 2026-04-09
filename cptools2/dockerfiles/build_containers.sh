#!/bin/bash
#$ -N build_containers
#$ -l h_rss=16G
#$ -l h_rt=02:00:00
#$ -cwd
#$ -o build_containers.$JOB_ID.log
#$ -e build_containers.$JOB_ID.err

# SGE jobs run in a non-login shell, so `module` is not a function yet.
# Source the Environment Modules init before any `module load`.
. /etc/profile.d/modules.sh

# ============================================================
# SGE job script: convert Docker archives to Singularity .sif
# ============================================================
# Submit from Eddie login node:
#   qsub build_containers.sh
#   qsub build_containers.sh /path/to/other/group/space/cptools2/containers
#
# Prerequisites:
#   - Docker archives (.tar) already transferred to CONTAINER_DIR
#   - Sufficient scratch quota (check: lfs quota)
#
# If build fails with memory errors, increase h_rss to 32G.
# ============================================================

set -euo pipefail

# CRITICAL: Singularity needs large temp AND cache space during OCI layer
# extraction. Eddie's /tmp is a small tmpfs and $HOME has a small quota —
# GPU images are 8-10GB and will blow past both. Redirect to scratch.
export SINGULARITY_TMPDIR=/exports/eddie/scratch/$USER/singularity_tmp
export SINGULARITY_CACHEDIR=/exports/eddie/scratch/$USER/singularity_cache
mkdir -p "$SINGULARITY_TMPDIR" "$SINGULARITY_CACHEDIR"

# Cap mksquashfs parallelism. Eddie compute slots are limited and mksquashfs
# will otherwise spawn one thread per CPU and hit thread-count limits.
export SINGULARITY_MKSQUASHFS_PROCS=2

module load singularity

CONTAINER_DIR="${1:-/exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/containers}"

echo "=== Building CellProfiler ==="
singularity build --force "$CONTAINER_DIR/cellprofiler_4.2.8.sif" \
  oci-archive://"$CONTAINER_DIR/cellprofiler_4.2.8.tar"

echo "=== Building Cellpose-SAM ==="
singularity build --force "$CONTAINER_DIR/cellpose_sam_1.0.sif" \
  oci-archive://"$CONTAINER_DIR/cellpose_sam_1.0.tar"

echo "=== Building DeepProfiler ==="
singularity build --force "$CONTAINER_DIR/deepprofiler_1.0.sif" \
  oci-archive://"$CONTAINER_DIR/deepprofiler_1.0.tar"

echo "=== All containers built successfully ==="
ls -lh "$CONTAINER_DIR"/*.sif
