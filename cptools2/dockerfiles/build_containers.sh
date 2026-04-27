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
# Environment knobs (set before qsub to override defaults):
#   CLEANUP_SCRATCH=0   keep singularity tmp + cache in scratch (default: 1, wipe)
#   CLEANUP_ARCHIVES=1  remove .tar files after successful build (default: 0, keep)
#
# If build fails with memory errors, increase h_rss to 32G.
# ============================================================

set -euo pipefail

# CRITICAL: Singularity needs large temp AND cache space during OCI layer
# extraction. Eddie's /tmp is a small tmpfs and $HOME has a small quota —
# GPU images are 8-10GB and will blow past both. Redirect to scratch.
SCRATCH_BASE="/exports/eddie/scratch/$USER"
export SINGULARITY_TMPDIR="$SCRATCH_BASE/singularity_tmp"
export SINGULARITY_CACHEDIR="$SCRATCH_BASE/singularity_cache"
mkdir -p "$SINGULARITY_TMPDIR" "$SINGULARITY_CACHEDIR"

# Cap mksquashfs parallelism. Eddie compute slots are limited and mksquashfs
# will otherwise spawn one thread per CPU and hit thread-count limits.
export SINGULARITY_MKSQUASHFS_PROCS=2

module load singularity

CONTAINER_DIR="${1:-/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/containers}"

# Cleanup toggles (can be overridden by env at qsub time via `qsub -v VAR=val ...`)
: "${CLEANUP_SCRATCH:=1}"
: "${CLEANUP_ARCHIVES:=0}"

# Always wipe scratch tmp/cache on exit — they're reproducible and just eat quota.
# CACHEDIR is preserved only if explicitly disabled (e.g. for repeated dev runs).
cleanup_scratch() {
  if [[ "$CLEANUP_SCRATCH" == "1" ]]; then
    echo "=== Cleanup: removing scratch tmp + cache ==="
    du -sh "$SINGULARITY_TMPDIR" "$SINGULARITY_CACHEDIR" 2>/dev/null || true
    rm -rf "$SINGULARITY_TMPDIR" "$SINGULARITY_CACHEDIR"
  else
    echo "=== Cleanup skipped (CLEANUP_SCRATCH=0); scratch state preserved ==="
    du -sh "$SINGULARITY_TMPDIR" "$SINGULARITY_CACHEDIR" 2>/dev/null || true
  fi
}
trap cleanup_scratch EXIT

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

# Archive cleanup only runs on successful build (we're past set -e'd builds).
# Off by default: dropping .tar files means the next rebuild has to re-transfer.
if [[ "$CLEANUP_ARCHIVES" == "1" ]]; then
  echo "=== Cleanup: removing Docker archives (CLEANUP_ARCHIVES=1) ==="
  du -sh "$CONTAINER_DIR"/*.tar 2>/dev/null || true
  rm -f "$CONTAINER_DIR"/*.tar
fi
