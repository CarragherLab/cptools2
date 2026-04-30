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
    tuple val(plate_id), val(datastore_path)

    output:
    tuple val(plate_id), path("staged_images"), emit: staged_images

    script:
    """
    set -euo pipefail

    rm -rf staged_images
    mkdir -p staged_images

    rsync -rtl --chmod=Du+rwx,Dg+rx,Do-rwx,Fu+rw,Fg+r,Fo-rwx --partial --timeout=300 \
        ${datastore_path}/ staged_images/ \
        2>&1 | tee stage_in_${plate_id}.log

    image_count=\$(find staged_images -name '*.tif' | wc -l)
    if [ "\$image_count" -eq 0 ]; then
        echo "ERROR: staged 0 .tif images for plate ${plate_id} from ${datastore_path}" >&2
        exit 1
    fi

    echo "Staged \$image_count images for plate ${plate_id}"
    """
}
