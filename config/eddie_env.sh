#!/usr/bin/env bash

# Eddie permanent-mirror bootstrap for cptools2.
# Source this file before running a dry-run or submitting work from the mirror.
#
# Example:
#   source /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/config/eddie_env.sh
#   cptools2 generate /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/config/loop400.yaml --dry-run

source /etc/profile.d/modules.sh
module purge
module load uge/2024.1.0
module load roslin/nextflow/25.10.2
module load singularity/4.3.4
module load miniforge/25.3.1-0

export CPTOOLS2_PROJECT_ROOT="/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2"
export CPTOOLS2_PERMANENT_ROOT="${CPTOOLS2_PROJECT_ROOT}"
export CPTOOLS2_SCRATCH_ROOT="/exports/eddie/scratch/${USER}/cptools2-ai-update"

# Permanent configuration and container references stay on the mirror.
export CPTOOLS2_CONFIG_ROOT="${CPTOOLS2_PERMANENT_ROOT}/config"
export CPTOOLS2_CONTAINER_DIR="${CPTOOLS2_PERMANENT_ROOT}/containers"
export CPTOOLS2_VENV="${CPTOOLS2_PERMANENT_ROOT}/.venv"

# Runtime state stays on scratch, with work kept under scratch/work.
export CPTOOLS2_RUNTIME_ROOT="${CPTOOLS2_SCRATCH_ROOT}"
export CPTOOLS2_WORK_ROOT="${CPTOOLS2_SCRATCH_ROOT}/work"
export CPTOOLS2_PARAMS_ROOT="${CPTOOLS2_SCRATCH_ROOT}/params"
export CPTOOLS2_CACHE_ROOT="${CPTOOLS2_WORK_ROOT}/cache"
export CPTOOLS2_LOG_ROOT="${CPTOOLS2_SCRATCH_ROOT}/logs"
export CPTOOLS2_TRACE_ROOT="${CPTOOLS2_SCRATCH_ROOT}/traces"
export CPTOOLS2_TEMP_ROOT="${CPTOOLS2_WORK_ROOT}/tmp"
export CPTOOLS2_HOME="${CPTOOLS2_WORK_ROOT}/home/${USER}"

export NXF_HOME="${CPTOOLS2_CACHE_ROOT}/nextflow"
export NXF_TEMP="${CPTOOLS2_TEMP_ROOT}/nextflow"
export NXF_OPTS="${NXF_OPTS:--Xms64m -Xmx1g -XX:ActiveProcessorCount=2 -Djava.io.tmpdir=${NXF_TEMP}}"
export SINGULARITY_CACHEDIR="${CPTOOLS2_CACHE_ROOT}/singularity"
export SINGULARITY_TMPDIR="${CPTOOLS2_TEMP_ROOT}/singularity"
export APPTAINER_CACHEDIR="${CPTOOLS2_CACHE_ROOT}/apptainer"
export APPTAINER_TMPDIR="${CPTOOLS2_TEMP_ROOT}/apptainer"
export TMPDIR="${CPTOOLS2_TEMP_ROOT}"
export TEMP="${CPTOOLS2_TEMP_ROOT}"
export TMP="${CPTOOLS2_TEMP_ROOT}"
export XDG_CACHE_HOME="${CPTOOLS2_CACHE_ROOT}/xdg"
export XDG_RUNTIME_DIR="${CPTOOLS2_TEMP_ROOT}/xdg-runtime"
export PIP_CACHE_DIR="${CPTOOLS2_CACHE_ROOT}/pip"

mkdir -p \
    "${CPTOOLS2_WORK_ROOT}" \
    "${CPTOOLS2_PARAMS_ROOT}" \
    "${CPTOOLS2_CACHE_ROOT}" \
    "${CPTOOLS2_LOG_ROOT}" \
    "${CPTOOLS2_TRACE_ROOT}" \
    "${CPTOOLS2_TEMP_ROOT}" \
    "${CPTOOLS2_HOME}" \
    "${NXF_HOME}" \
    "${NXF_TEMP}" \
    "${SINGULARITY_CACHEDIR}" \
    "${SINGULARITY_TMPDIR}" \
    "${APPTAINER_CACHEDIR}" \
    "${APPTAINER_TMPDIR}" \
    "${XDG_CACHE_HOME}" \
    "${XDG_RUNTIME_DIR}" \
    "${PIP_CACHE_DIR}"

if [ -f "${CPTOOLS2_VENV}/bin/activate" ]; then
    # Keep the Python environment stable in permanent space; runtime caches remain in scratch.
    . "${CPTOOLS2_VENV}/bin/activate"
fi
