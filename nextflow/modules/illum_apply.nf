// Illumination correction — apply correction functions
//
// Runs CellProfiler CorrectIlluminationApply per plate.
// Takes raw images + illumination functions, produces corrected 16-bit PNGs.
//
// Input:  tuple(plate_id, plate_dir, illum_dir)
// Output: tuple(plate_id, corrected_dir)

process ILLUM_APPLY {
    tag "${plate_id}"
    label 'illum_apply'

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
