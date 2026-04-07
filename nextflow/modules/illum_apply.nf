// Illumination correction — apply correction functions
//
// Runs CellProfiler CorrectIlluminationApply per plate.
// Takes raw images + illumination functions, produces corrected 16-bit PNGs.
//
// Note: CellProfiler reads illumination function paths from the .cppipe pipeline
// file, not from command-line arguments. The illum_dir input is passed so it is
// available in the working directory via Nextflow staging (symlink).
//
// Input:  tuple(plate_id, plate_dir, illum_dir)
// Output: tuple(plate_id, corrected_dir)

process ILLUM_APPLY {
    tag "${plate_id}"
    label 'illum_apply'
    publishDir "${params.output_dir}/${plate_id}/corrected_images", mode: 'copy'

    container params.containers.cellprofiler

    input:
    tuple val(plate_id), path(plate_dir), path(illum_dir)

    output:
    tuple val(plate_id), path("corrected_images"), emit: corrected_images

    script:
    """
    mkdir -p corrected_images

    cellprofiler \\
        --run \\
        --run-headless \\
        --pipeline ${params.illum_pipeline_apply} \\
        --image-directory ${plate_dir} \\
        --output-directory corrected_images \\
        --log-level INFO \\
        2>&1 | tee illum_apply_${plate_id}.log
    """
}
