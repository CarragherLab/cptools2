#!/usr/bin/env bash
set -euo pipefail

SCRATCH_PROJECT="${1:-/exports/eddie/scratch/${USER}/cptools2-loop230}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "${SCRATCH_PROJECT}/pipelines"
mkdir -p "${SCRATCH_PROJECT}/config"
mkdir -p "${SCRATCH_PROJECT}/commands"

cp "${REPO_ROOT}/cptools2/templates/illum_calculate.cppipe" \
   "${SCRATCH_PROJECT}/pipelines/illum_calculate.cppipe"
cp "${REPO_ROOT}/cptools2/templates/illum_apply.cppipe" \
   "${SCRATCH_PROJECT}/pipelines/illum_apply.cppipe"
cp "${REPO_ROOT}/cptools2/templates/nuclear_segmentation.cppipe" \
   "${SCRATCH_PROJECT}/pipelines/nuclear_segmentation.cppipe"
cp "${REPO_ROOT}/cptools2/templates/deepprofiler_config.json" \
   "${SCRATCH_PROJECT}/config/deepprofiler_config.json"
cp "${REPO_ROOT}/config/loop230-sarah-screen.yaml" \
   "${SCRATCH_PROJECT}/config/loop230-sarah-screen.yaml"

cptools2 pipeline "${SCRATCH_PROJECT}/config/loop230-sarah-screen.yaml" --dry-run

echo "Prepared Loop 230 scratch project: ${SCRATCH_PROJECT}"
echo "Config: ${SCRATCH_PROJECT}/config/loop230-sarah-screen.yaml"
echo "Params: ${SCRATCH_PROJECT}/params.json"
