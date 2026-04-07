#!/bin/bash
#$ -N build_containers
#$ -l h_rss=16G
#$ -l h_rt=02:00:00
#$ -cwd
#$ -o build_containers.$JOB_ID.log
#$ -e build_containers.$JOB_ID.err

# ============================================================
# SGE job script: convert Docker archives to Singularity .sif
# ============================================================
# Submit from Eddie login node:
#   qsub build_containers.sh
#   qsub build_containers.sh /path/to/other/group/space/cptools2/containers
#
# Prerequisites:
#   - Docker archives (.tar.gz) already transferred to CONTAINER_DIR
#   - Sufficient scratch quota (check: lfs quota)
#
# If build fails with memory errors, increase h_rss to 32G.
# ============================================================

set -euo pipefail

# CRITICAL: Singularity needs large temp space during layer extraction.
# Eddie's /tmp is a small tmpfs — GPU images are 8-10GB and will fail.
export SINGULARITY_TMPDIR=/exports/eddie/scratch/$USER/singularity_tmp
mkdir -p "$SINGULARITY_TMPDIR"

module load singularity

CONTAINER_DIR="${1:-/exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/containers}"

echo "=== Building CellProfiler ==="
singularity build "$CONTAINER_DIR/cellprofiler_4.2.8.sif" \
  docker-archive://"$CONTAINER_DIR/cellprofiler_4.2.8.tar.gz"

echo "=== Building Cellpose-SAM ==="
singularity build "$CONTAINER_DIR/cellpose_sam_1.0.sif" \
  docker-archive://"$CONTAINER_DIR/cellpose_sam_1.0.tar.gz"

echo "=== Building DeepProfiler ==="
singularity build "$CONTAINER_DIR/deepprofiler_1.0.sif" \
  docker-archive://"$CONTAINER_DIR/deepprofiler_1.0.tar.gz"

echo "=== All containers built successfully ==="
ls -lh "$CONTAINER_DIR"/*.sif
