// Stage data from DataStore to scratch for analysis
//
// Runs on the staging queue (-q staging) which has DataStore access.
// Uses rsync for resumable, fault-tolerant transfer.
//
// Input:  tuple(plate_id, datastore_path)
// Output: tuple(plate_id, staged_path)

process STAGE_IN {
    tag "${plate_id}"
    label 'staging'

    input:
    tuple val(plate_id), path(datastore_path)

    output:
    tuple val(plate_id), path("staged_images"), emit: staged_images

    script:
    """
    mkdir -p staged_images

    rsync -av --partial --timeout=300 \
        ${datastore_path}/ staged_images/ \
        2>&1 | tee stage_in_${plate_id}.log

    echo "Staged \$(find staged_images -name '*.tif' | wc -l) images for plate ${plate_id}"
    """
}
