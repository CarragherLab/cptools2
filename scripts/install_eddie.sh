#!/usr/bin/env bash
# install_eddie.sh — Deploy cptools2 to Eddie HPC shared group space
#
# Usage:
#   bash scripts/install_eddie.sh [BASE_DIR]
#
# Arguments:
#   BASE_DIR  Base directory for the cptools2 group installation.
#             Default: /exports/cmvm/eddie/scs/groups/chandranlabs
#
# What this script does:
#   1. Creates cptools2/{containers,env,nextflow} directory structure
#   2. Creates a conda environment with cptools2 installed
#   3. Installs nextflow into the env/bin
#   4. Creates an activate.sh convenience script
#
# Requirements:
#   - conda (or miniconda) must be available on PATH
#   - Run from the cptools2 git repo root
#   - SSH to Eddie before running (or use qlogin)
#
# Example:
#   ssh eddie
#   cd /path/to/cptools2
#   bash scripts/install_eddie.sh /exports/cmvm/eddie/scs/groups/chandranlabs

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR="${1:-/exports/cmvm/eddie/scs/groups/chandranlabs}"
CPTOOLS2_DIR="${BASE_DIR}/cptools2"
CONTAINERS_DIR="${CPTOOLS2_DIR}/containers"
ENV_DIR="${CPTOOLS2_DIR}/env"
NEXTFLOW_DIR="${CPTOOLS2_DIR}/nextflow"
CONDA_ENV_NAME="cptools2"
PYTHON_VERSION="3.10"
NEXTFLOW_VERSION="25.10.4"

echo "[install_eddie.sh] Installing cptools2 to: ${CPTOOLS2_DIR}"
echo "[install_eddie.sh] Base dir: ${BASE_DIR}"

# ---------------------------------------------------------------------------
# Step 1: Create directory structure
# ---------------------------------------------------------------------------

echo "[install_eddie.sh] Creating directory structure..."
mkdir -p "${CONTAINERS_DIR}"
mkdir -p "${ENV_DIR}"
mkdir -p "${NEXTFLOW_DIR}"

echo "[install_eddie.sh] Created:"
echo "  ${CONTAINERS_DIR}"
echo "  ${ENV_DIR}"
echo "  ${NEXTFLOW_DIR}"

# ---------------------------------------------------------------------------
# Step 2: Create conda environment
# ---------------------------------------------------------------------------

echo "[install_eddie.sh] Creating conda environment: ${CONDA_ENV_NAME} (Python ${PYTHON_VERSION})..."

# Check if conda is available
if ! command -v conda &>/dev/null; then
    echo "[install_eddie.sh] ERROR: conda not found on PATH."
    echo "  Load it with: module load anaconda or source ~/miniconda3/etc/profile.d/conda.sh"
    exit 1
fi

# Create env in the shared location (not the default user home)
conda create -y -p "${ENV_DIR}" python="${PYTHON_VERSION}"

# Activate the env for subsequent pip install
# shellcheck disable=SC1090
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "${ENV_DIR}"

# ---------------------------------------------------------------------------
# Step 3: Install cptools2 from current directory
# ---------------------------------------------------------------------------

echo "[install_eddie.sh] Installing cptools2 from: $(pwd)"
pip install -e .

echo "[install_eddie.sh] cptools2 installed. Version:"
cptools2 --version

# ---------------------------------------------------------------------------
# Step 4: Install Nextflow
# ---------------------------------------------------------------------------

echo "[install_eddie.sh] Installing Nextflow ${NEXTFLOW_VERSION}..."

NF_BIN="${ENV_DIR}/bin/nextflow"
if [ -f "${NF_BIN}" ]; then
    echo "[install_eddie.sh] Nextflow already at ${NF_BIN}, skipping download."
else
    curl -fsSL "https://github.com/nextflow-io/nextflow/releases/download/v${NEXTFLOW_VERSION}/nextflow" \
        -o "${NF_BIN}"
    chmod +x "${NF_BIN}"
    echo "[install_eddie.sh] Nextflow installed at: ${NF_BIN}"
fi

# Symlink into nextflow dir for discoverability
ln -sf "${NF_BIN}" "${NEXTFLOW_DIR}/nextflow"

# ---------------------------------------------------------------------------
# Step 5: Create activate.sh convenience script
# ---------------------------------------------------------------------------

ACTIVATE_SCRIPT="${CPTOOLS2_DIR}/activate.sh"

cat > "${ACTIVATE_SCRIPT}" <<'ACTIVATE_EOF'
# cptools2 activation script
# Source this file to activate the cptools2 environment:
#   source /path/to/cptools2/activate.sh

_CPTOOLS2_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_ENV_DIR="${_CPTOOLS2_DIR}/env"

# Activate conda environment
# shellcheck disable=SC1090
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "${_ENV_DIR}"

# Set container directory for cptools2 container resolution
export CPTOOLS2_CONTAINER_DIR="${_CPTOOLS2_DIR}/containers"

echo "[cptools2] Environment activated."
echo "[cptools2] Container dir: ${CPTOOLS2_CONTAINER_DIR}"
echo "[cptools2] Version: $(cptools2 --version 2>/dev/null || echo 'unknown')"
ACTIVATE_EOF

chmod +x "${ACTIVATE_SCRIPT}"

echo "[install_eddie.sh] Created activate.sh: ${ACTIVATE_SCRIPT}"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

echo ""
echo "[install_eddie.sh] Installation complete."
echo ""
echo "To activate cptools2:"
echo "  source ${ACTIVATE_SCRIPT}"
echo ""
echo "Directory layout:"
echo "  ${CPTOOLS2_DIR}/"
echo "  ├── containers/   (place .sif files here)"
echo "  ├── env/          (conda environment)"
echo "  ├── nextflow/     (nextflow symlink)"
echo "  └── activate.sh   (source to activate)"
