#!/usr/bin/env bash

# Create the local Eddie path config consumed by config/eddie_env.sh.
#
# This script is safe to commit because it contains no site-specific defaults.
# By default, the generated file is written next to the checkout as
# <project-root>-local/eddie_paths.env so the permanent Git mirror stays clean.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  bash scripts/configure_eddie_paths.sh \
    --project-root /exports/<college>/eddie/<school>/groups/<group>/cptools2 \
    [--group-root /exports/<college>/eddie/<school>/groups/<group>] \
    [--scratch-root /exports/eddie/scratch/${USER}/cptools2-ai-update] \
    [--container-dir /path/to/containers] \
    [--output /path/to/eddie_paths.env] \
    [--create-dirs] \
    [--force]

Creates by default:
  <project-root>-local/eddie_paths.env

The generated file should live in permanent storage but outside the Git checkout
because it contains site/user-specific paths.

This config only supplies paths. The Eddie profile still assumes Eddie modules,
SGE/UGE queues, Singularity, and /exports/eddie scratch. Other HPC sites need an
equivalent Nextflow profile as well as their own path config.
EOF
}

PROJECT_ROOT=""
GROUP_ROOT=""
SCRATCH_ROOT='/exports/eddie/scratch/${USER}/cptools2-ai-update'
CONTAINER_DIR=""
OUTPUT=""
CREATE_DIRS=0
FORCE=0

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
        --output)
            OUTPUT="${2:?--output requires a value}"
            shift 2
            ;;
        --create-dirs)
            CREATE_DIRS=1
            shift
            ;;
        --force)
            FORCE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "$PROJECT_ROOT" ]; then
    echo "ERROR: --project-root is required." >&2
    usage >&2
    exit 2
fi

if [ -z "$GROUP_ROOT" ]; then
    GROUP_ROOT="$(dirname "$PROJECT_ROOT")"
fi

if [ -z "$CONTAINER_DIR" ]; then
    CONTAINER_DIR='${CPTOOLS2_PROJECT_ROOT}/containers'
fi

if [ -z "$OUTPUT" ]; then
    OUTPUT="${PROJECT_ROOT%/}-local/eddie_paths.env"
fi

if [ -e "$OUTPUT" ] && [ "$FORCE" -ne 1 ]; then
    echo "ERROR: $OUTPUT already exists. Use --force to overwrite." >&2
    exit 3
fi

mkdir -p "$(dirname "$OUTPUT")"

cat > "$OUTPUT" <<EOF
# Local Eddie paths for this checkout.
# Keep this site/user-specific file in permanent storage outside the public Git
# checkout.

export CPTOOLS2_PROJECT_ROOT="$PROJECT_ROOT"
export CPTOOLS2_GROUP_ROOT="$GROUP_ROOT"
export CPTOOLS2_SCRATCH_ROOT="$SCRATCH_ROOT"
export CPTOOLS2_CONTAINER_DIR="$CONTAINER_DIR"
EOF

if [ "$CREATE_DIRS" -eq 1 ]; then
    mkdir -p \
        "$PROJECT_ROOT" \
        "$GROUP_ROOT" \
        "${PROJECT_ROOT}/containers" \
        "${PROJECT_ROOT}/config" \
        "${PROJECT_ROOT}/nextflow" \
        "${PROJECT_ROOT}/cptools2/templates"

    expanded_scratch_root="${SCRATCH_ROOT//\$\{USER\}/${USER}}"
    expanded_scratch_root="${expanded_scratch_root//\$USER/${USER}}"
    mkdir -p \
        "${expanded_scratch_root}/work" \
        "${expanded_scratch_root}/params" \
        "${expanded_scratch_root}/logs" \
        "${expanded_scratch_root}/traces" \
        "${expanded_scratch_root}/staging" \
        "${expanded_scratch_root}/results" \
        "${expanded_scratch_root}/commands"
fi

echo "Wrote $OUTPUT"
echo "Next:"
echo "  source config/eddie_env.sh"
echo "  cptools2 pipeline config/loop440-eddie-smoke.yaml --dry-run"
