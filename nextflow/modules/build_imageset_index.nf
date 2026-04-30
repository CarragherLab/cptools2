// Build a parserix-backed image-set index for a staged plate.
//
// Input:  tuple(plate_id, staged_plate_dir)
// Output: tuple(plate_id, staged_plate_dir, image_sets_csv)

process BUILD_IMAGESET_INDEX {
    tag "${plate_id}"
    label 'index'

    input:
    tuple val(plate_id), path(staged_plate_dir)

    output:
    tuple val(plate_id), path(staged_plate_dir), path("image_sets.csv"), emit: image_sets

    script:
    def expected = params.expected_channels instanceof List
        ? params.expected_channels.join(',')
        : params.expected_channels
    """
    export PYTHONPATH="${projectDir}/..:\${PYTHONPATH:-}"
    export POLARS_MAX_THREADS=1
    export RAYON_NUM_THREADS=1
    export OMP_NUM_THREADS=1
    export MKL_NUM_THREADS=1

    python -m cptools2.nextflow_chunking index \
        --plate-id ${plate_id} \
        --plate-dir ${staged_plate_dir} \
        --output-csv image_sets.csv \
        --expected-channels "${expected}"
    """
}
