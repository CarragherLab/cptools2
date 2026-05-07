#!/usr/bin/env bash
# install_eddie.sh - configure and install cptools2 into an Eddie mirror.
#
# This script is safe to commit because it has no site-specific path defaults.
# Real paths are supplied by arguments and written to ignored local config.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  bash scripts/install_eddie.sh \
    --project-root /exports/<college>/eddie/<school>/groups/<group>/cptools2 \
    [--group-root /exports/<college>/eddie/<school>/groups/<group>] \
    [--scratch-root /exports/eddie/scratch/${USER}/cptools2-ai-update] \
    [--container-dir /path/to/containers] \
    [--container-action auto|check|build|skip] \
    [--skip-env] \
    [--skip-nextflow] \
    [--force-paths]

Legacy form is also accepted:
  bash scripts/install_eddie.sh /exports/<college>/eddie/<school>/groups/<group>

What it does:
  1. Writes ignored local/eddie_paths.env with real paths.
  2. Creates permanent mirror and scratch runtime directories.
  3. Creates/reuses a conda environment and installs cptools2 editable.
  4. Installs or reuses Nextflow in the environment.
  5. Writes activate.sh for the permanent mirror.
  6. Checks required .sif containers and can submit the SGE build job if
     archives are present.

The generated local/eddie_paths.env overrides placeholder paths in public repo
configs through config/eddie_env.sh.
EOF
}

log() {
    printf '[install_eddie.sh] %s\n' "$*"
}

die() {
    printf '[install_eddie.sh] ERROR: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "$1 not found on PATH"
}

PROJECT_ROOT=""
GROUP_ROOT=""
SCRATCH_ROOT='/exports/eddie/scratch/${USER}/cptools2-ai-update'
CONTAINER_DIR=""
CONTAINER_ACTION="auto"
SKIP_ENV=0
SKIP_NEXTFLOW=0
FORCE_PATHS=0
PYTHON_VERSION="3.10"
NEXTFLOW_VERSION="25.10.4"
REPO_ROOT="$(pwd -P)"

if [ ! -f "${REPO_ROOT}/pyproject.toml" ] || [ ! -f "${REPO_ROOT}/scripts/configure_eddie_paths.sh" ]; then
    die "run this script from the cptools2 repository root"
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        --project-root)
            PROJECT_ROOT="${2:?--project-root requires a value}"
            shift 2
            ;;
        --group-root)
            GROUP_ROOT="${2:?--group-root requires a value}"
            shift 2
            ;;
        --scratch-root)
            SCRATCH_ROOT="${2:?--scratch-root requires a value}"
            shift 2
            ;;
        --container-dir)
            CONTAINER_DIR="${2:?--container-dir requires a value}"
            shift 2
            ;;
        --container-action)
            CONTAINER_ACTION="${2:?--container-action requires a value}"
            shift 2
            ;;
        --skip-env)
            SKIP_ENV=1
            shift
            ;;
        --skip-nextflow)
            SKIP_NEXTFLOW=1
            shift
            ;;
        --force-paths)
            FORCE_PATHS=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --*)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            if [ -n "$PROJECT_ROOT" ]; then
                echo "Unexpected positional argument: $1" >&2
                usage >&2
                exit 2
            fi
            GROUP_ROOT="$1"
            PROJECT_ROOT="${GROUP_ROOT%/}/cptools2"
            shift
            ;;
    esac
done

case "$CONTAINER_ACTION" in
    auto|check|build|skip) ;;
    *) die "--container-action must be one of: auto, check, build, skip" ;;
esac

if [ -z "$PROJECT_ROOT" ]; then
    echo "Missing --project-root." >&2
    usage >&2
    exit 2
fi

if [ -z "$GROUP_ROOT" ]; then
    GROUP_ROOT="$(dirname "$PROJECT_ROOT")"
fi

if [ -z "$CONTAINER_DIR" ]; then
    CONTAINER_DIR="${PROJECT_ROOT}/containers"
fi

ENV_DIR="${PROJECT_ROOT}/env"
NEXTFLOW_DIR="${PROJECT_ROOT}/nextflow"
ACTIVATE_SCRIPT="${PROJECT_ROOT}/activate.sh"

expanded_scratch_root="${SCRATCH_ROOT//\$\{USER\}/${USER}}"
expanded_scratch_root="${expanded_scratch_root//\$USER/${USER}}"

log "Project root: ${PROJECT_ROOT}"
log "Group root: ${GROUP_ROOT}"
log "Scratch root: ${expanded_scratch_root}"
log "Container dir: ${CONTAINER_DIR}"
log "Source checkout: ${REPO_ROOT}"

if [ "$REPO_ROOT" != "$PROJECT_ROOT" ]; then
    log "Source checkout differs from project root; install will use ${REPO_ROOT}"
    log "For permanent mirror installs, run this script from the mirror checkout when possible"
fi

configure_args=(
    --project-root "$PROJECT_ROOT"
    --group-root "$GROUP_ROOT"
    --scratch-root "$SCRATCH_ROOT"
    --container-dir "$CONTAINER_DIR"
    --output "${PROJECT_ROOT}/local/eddie_paths.env"
    --create-dirs
)
if [ "$FORCE_PATHS" -eq 1 ]; then
    configure_args+=(--force)
fi

bash "${REPO_ROOT}/scripts/configure_eddie_paths.sh" "${configure_args[@]}"

log "Creating permanent and scratch directory layout"
mkdir -p \
    "$PROJECT_ROOT" \
    "$CONTAINER_DIR" \
    "$ENV_DIR" \
    "$NEXTFLOW_DIR" \
    "${PROJECT_ROOT}/config" \
    "${PROJECT_ROOT}/nextflow" \
    "${PROJECT_ROOT}/cptools2/templates" \
    "${expanded_scratch_root}/work" \
    "${expanded_scratch_root}/params" \
    "${expanded_scratch_root}/logs" \
    "${expanded_scratch_root}/traces" \
    "${expanded_scratch_root}/staging" \
    "${expanded_scratch_root}/results" \
    "${expanded_scratch_root}/commands"

if [ "$SKIP_ENV" -eq 0 ]; then
    require_command conda
    log "Creating or reusing conda environment: ${ENV_DIR}"
    if [ ! -x "${ENV_DIR}/bin/python" ]; then
        conda create -y -p "$ENV_DIR" "python=${PYTHON_VERSION}"
    fi

    # shellcheck disable=SC1090
    source "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate "$ENV_DIR"

    log "Installing cptools2 editable from: ${REPO_ROOT}"
    python -m pip install -e "$REPO_ROOT"
    log "Installed cptools2 version: $(cptools2 --version 2>/dev/null || echo unknown)"
else
    log "Skipping conda environment creation and cptools2 install"
fi

if [ "$SKIP_NEXTFLOW" -eq 0 ]; then
    NF_BIN="${ENV_DIR}/bin/nextflow"
    if [ -x "$NF_BIN" ]; then
        log "Nextflow already present: ${NF_BIN}"
    else
        require_command curl
        log "Installing Nextflow ${NEXTFLOW_VERSION}: ${NF_BIN}"
        mkdir -p "$(dirname "$NF_BIN")"
        curl -fsSL "https://github.com/nextflow-io/nextflow/releases/download/v${NEXTFLOW_VERSION}/nextflow" \
            -o "$NF_BIN"
        chmod +x "$NF_BIN"
    fi
    ln -sf "$NF_BIN" "${NEXTFLOW_DIR}/nextflow"
else
    log "Skipping Nextflow install"
fi

cat > "$ACTIVATE_SCRIPT" <<'ACTIVATE_EOF'
# cptools2 activation script.
# Source this file from the Eddie mirror:
#   source /path/to/cptools2/activate.sh

_CPTOOLS2_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$_CPTOOLS2_DIR"

if [ -f "local/eddie_paths.env" ]; then
    . "local/eddie_paths.env"
fi

export CPTOOLS2_PROJECT_ROOT="${CPTOOLS2_PROJECT_ROOT:-${_CPTOOLS2_DIR}}"
export CPTOOLS2_CONTAINER_DIR="${CPTOOLS2_CONTAINER_DIR:-${CPTOOLS2_PROJECT_ROOT}/containers}"
export CPTOOLS2_VENV="${CPTOOLS2_VENV:-${CPTOOLS2_PROJECT_ROOT}/env}"

if command -v conda >/dev/null 2>&1; then
    # shellcheck disable=SC1090
    . "$(conda info --base)/etc/profile.d/conda.sh"
    conda activate "$CPTOOLS2_VENV"
fi

echo "[cptools2] Project root: ${CPTOOLS2_PROJECT_ROOT}"
echo "[cptools2] Container dir: ${CPTOOLS2_CONTAINER_DIR}"
echo "[cptools2] Version: $(cptools2 --version 2>/dev/null || echo unknown)"
ACTIVATE_EOF
chmod +x "$ACTIVATE_SCRIPT"
log "Wrote activation script: ${ACTIVATE_SCRIPT}"

required_containers=(
    cellprofiler_4.2.8.sif
    cellpose_sam_1.0.sif
    deepprofiler_1.0.sif
)

missing_containers=()
for name in "${required_containers[@]}"; do
    if [ ! -s "${CONTAINER_DIR}/${name}" ]; then
        missing_containers+=("$name")
    fi
done

if [ "$CONTAINER_ACTION" = "skip" ]; then
    log "Skipping container checks"
elif [ "${#missing_containers[@]}" -eq 0 ]; then
    log "All required containers are present"
elif [ "$CONTAINER_ACTION" = "check" ]; then
    printf '[install_eddie.sh] Missing containers:\n' >&2
    printf '  %s\n' "${missing_containers[@]}" >&2
    exit 4
else
    log "Missing containers: ${missing_containers[*]}"
    archives_present=1
    for archive in cellprofiler_4.2.8.tar cellpose_sam_1.0.tar deepprofiler_1.0.tar; do
        if [ ! -s "${CONTAINER_DIR}/${archive}" ]; then
            archives_present=0
        fi
    done

    if [ "$archives_present" -eq 1 ]; then
        require_command qsub
        log "Submitting SGE container build job"
        qsub -v "CPTOOLS2_CONTAINER_DIR=${CONTAINER_DIR}" \
            "${REPO_ROOT}/cptools2/dockerfiles/build_containers.sh"
    elif [ "$CONTAINER_ACTION" = "build" ]; then
        die "Container archives are missing from ${CONTAINER_DIR}; expected cellprofiler_4.2.8.tar, cellpose_sam_1.0.tar, deepprofiler_1.0.tar"
    else
        log "Container archives are not all present, so build was not submitted"
        log "Place the Docker archives in ${CONTAINER_DIR} and rerun with --container-action build"
    fi
fi

log "Installation/configuration complete"
log "Next:"
log "  source ${ACTIVATE_SCRIPT}"
log "  source config/eddie_env.sh"
log "  cptools2 pipeline config/loop440-eddie-smoke.yaml --dry-run"
