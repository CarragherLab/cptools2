// Nuclear segmentation — identify nuclei and export centroid locations
//
// Runs CellProfiler IdentifyPrimaryObjects on the DNA channel.
// Produces per-site CSV files with nucleus centroid coordinates.
//
// Input:  tuple(plate_id, corrected_dir)
// Output: tuple(plate_id, corrected_dir, locations_dir)

process SEGMENTATION {
    tag "${plate_id}"
    label 'segmentation'
    publishDir "${params.output_dir}/${plate_id}/segmentation", mode: 'copy'

    container params.containers.cellprofiler

    input:
    tuple val(plate_id), path(corrected_dir)

    output:
    tuple val(plate_id), path(corrected_dir), path("segmentation"), emit: locations

    script:
    """
    mkdir -p segmentation

    cellprofiler \\
        --run \\
        --run-headless \\
        --pipeline ${params.seg_pipeline} \\
        --image-directory ${corrected_dir} \\
        --output-directory segmentation \\
        --log-level INFO \\
        2>&1 | tee segmentation_${plate_id}.log
    """
}
