// Illumination correction — apply correction functions
//
// Runs CellProfiler CorrectIlluminationApply per chunk.
// Takes a chunk manifest + illumination functions, produces corrected images.
//
// Note: CellProfiler reads illumination function paths from the .cppipe pipeline
// file, not from command-line arguments. The illum_dir input is passed so it is
// available in the working directory via Nextflow staging (symlink).
//
// Input:  tuple(plate_id, staged_plate_dir, chunk_manifest, illum_dir)
// Output: tuple(plate_id, corrected_dir, chunk_manifest)

process ILLUM_APPLY {
    tag "${plate_id}:${chunk_manifest.simpleName}"
    label 'illum_apply'
    publishDir "${params.output_dir}/${plate_id}/corrected_images/${chunk_manifest.simpleName}", mode: 'copy'

    container params.containers.cellprofiler

    input:
    tuple val(plate_id), path(staged_plate_dir), path(chunk_manifest), path(illum_dir)

    output:
    tuple val(plate_id), path("corrected_images"), path(chunk_manifest), emit: corrected_images

    script:
    """
    mkdir -p corrected_images
    mkdir -p chunk_images

    python - <<'PY'
import csv
import os
from pathlib import Path

with open("${chunk_manifest}", newline="") as handle:
    for row in csv.DictReader(handle):
        src = row["image_path"]
        dst = Path("chunk_images") / Path(src).name
        if not dst.exists():
            os.symlink(src, dst)
PY

    cellprofiler \\
        --run \\
        --run-headless \\
        --pipeline ${params.illum_pipeline_apply} \\
        --image-directory chunk_images \\
        --output-directory corrected_images \\
        --log-level INFO \\
        2>&1 | tee illum_apply_${plate_id}_${chunk_manifest.simpleName}.log
    """
}
