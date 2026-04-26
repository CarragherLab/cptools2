// Stage results from scratch back to DataStore
//
// Runs on the staging queue (-q staging) which has DataStore access.
//
// Input:  tuple(plate_id, results_dir)
// Output: tuple(plate_id, val(true)), emit: done

process STAGE_OUT {
    tag "${plate_id}"
    label 'staging'

    input:
    tuple val(plate_id), path(results_dir)

    output:
    tuple val(plate_id), val(true), emit: done

    script:
    """
    if [ -n "${params.output_dir}" ]; then
        DEST="${params.output_dir}/${plate_id}"
        mkdir -p "\$DEST"

        rsync -rtl --partial --timeout=300 \
            ${results_dir}/ "\$DEST/" \
            2>&1 | tee stage_out_${plate_id}.log

        echo "Destaged results for plate ${plate_id} to \$DEST"
    else
        echo "No output_dir specified, skipping destaging for plate ${plate_id}"
    fi
    """
}
