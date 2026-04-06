// Illumination correction — calculate illumination functions
//
// Runs CellProfiler CorrectIlluminationCalculate per plate.
// Produces one .npy illumination function file per channel per plate.
//
// Input:  tuple(plate_id, plate_dir)
// Output: tuple(plate_id, plate_dir, illum_dir)

process ILLUM_CALCULATE {
    tag "${plate_id}"
    label 'illum_calculate'

    container params.containers.cellprofiler

    input:
    tuple val(plate_id), path(plate_dir)

    output:
    tuple val(plate_id), path(plate_dir), path("illum_functions"), emit: illum_functions

    script:
    """
    mkdir -p illum_functions

    cellprofiler \\
        --run \\
        --run-headless \\
        --pipeline ${params.illum_pipeline_calculate} \\
        --image-directory ${plate_dir} \\
        --output-directory illum_functions \\
        --log-level INFO \\
        2>&1 | tee illum_calculate_${plate_id}.log
    """
}
