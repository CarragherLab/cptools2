#!/bin/sh
# ============================================================
# Script: eddie_smoke_cellprofiler.sh
# Purpose: Validate the CellProfiler Singularity container on an Eddie CPU job.
# Pipeline: cptools2 Phase 2.8
# ============================================================
#$ -N cptools2_cp_smoke
#$ -cwd
#$ -l h_rt=00:20:00
#$ -l h_rss=8G
#$ -o logs/cellprofiler_$JOB_ID.log
#$ -e logs/cellprofiler_$JOB_ID.err

set -eu

PROJECT_ROOT="${CPTOOLS2_PROJECT_ROOT:-/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2}"
SCRATCH_ROOT="${CPTOOLS2_SCRATCH_ROOT:-/exports/eddie/scratch/${USER}/cptools2-ai-update}"
WORK_ROOT="${CPTOOLS2_WORK_ROOT:-${SCRATCH_ROOT}/work}"
SMOKE_ROOT="${SCRATCH_ROOT}/smoke/cellprofiler"
CONTAINER="${CPTOOLS2_CONTAINER_DIR:-${PROJECT_ROOT}/containers}/cellprofiler_4.2.8.sif"

mkdir -p \
    "${WORK_ROOT}/singularity_tmp" \
    "${WORK_ROOT}/singularity_cache" \
    "${SMOKE_ROOT}" \
    "logs"

echo "Job ${JOB_ID:-manual} started: $(date)"
echo "Host: $(hostname)"
echo "Slots: ${NSLOTS:-1}"
echo "Container: ${CONTAINER}"
echo "Smoke root: ${SMOKE_ROOT}"

. /etc/profile.d/modules.sh
module purge
module load singularity/4.3.4

export SINGULARITY_TMPDIR="${WORK_ROOT}/singularity_tmp"
export SINGULARITY_CACHEDIR="${WORK_ROOT}/singularity_cache"

VERSION_LOG="${SMOKE_ROOT}/version.${JOB_ID:-manual}.log"
if singularity exec "${CONTAINER}" cellprofiler --version > "${VERSION_LOG}"; then
    cat "${VERSION_LOG}"
else
    status=$?
    cat "${VERSION_LOG}"
    exit "${status}"
fi

echo "Job ${JOB_ID:-manual} finished: $(date)"
