#!/bin/sh
# ============================================================
# Script: eddie_smoke_deepprofiler.sh
# Purpose: Validate the DeepProfiler Singularity container on an Eddie GPU job.
# Pipeline: cptools2 Phase 2.8
# ============================================================
#$ -N cptools2_dp_smoke
#$ -cwd
#$ -q gpu
#$ -l gpu=1
#$ -l h_rt=00:30:00
#$ -l h_rss=16G
#$ -o logs/deepprofiler_$JOB_ID.log
#$ -e logs/deepprofiler_$JOB_ID.err

set -eu

PROJECT_ROOT="${CPTOOLS2_PROJECT_ROOT:-/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2}"
SCRATCH_ROOT="${CPTOOLS2_SCRATCH_ROOT:-/exports/eddie/scratch/${USER}/cptools2-ai-update}"
WORK_ROOT="${CPTOOLS2_WORK_ROOT:-${SCRATCH_ROOT}/work}"
SMOKE_ROOT="${SCRATCH_ROOT}/smoke/deepprofiler"
CONTAINER="${CPTOOLS2_CONTAINER_DIR:-${PROJECT_ROOT}/containers}/deepprofiler_1.0.sif"

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
if singularity exec --nv "${CONTAINER}" \
    python -c "import tensorflow as tf; print(tf.__version__); gpus = tf.config.list_physical_devices('GPU'); print(gpus); assert gpus" \
    > "${VERSION_LOG}"; then
    cat "${VERSION_LOG}"
else
    status=$?
    cat "${VERSION_LOG}"
    exit "${status}"
fi

echo "Job ${JOB_ID:-manual} finished: $(date)"
