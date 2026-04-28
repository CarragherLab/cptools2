// Split a plate image-set index into chunk manifest CSVs.
//
// Input:  tuple(plate_id, staged_plate_dir, image_sets_csv)
// Output: tuple(plate_id, staged_plate_dir, chunk_manifest_csv)

process CHUNK_IMAGESETS {
    tag "${plate_id}"
    label 'index'

    input:
    tuple val(plate_id), path(staged_plate_dir), path(image_sets_csv)

    output:
    tuple val(plate_id), path(staged_plate_dir), path("chunks/*.csv"), emit: chunks

    script:
    def maxChunksFlag = params.max_chunks ? "--max-chunks ${params.max_chunks}" : ""
    """
    export PYTHONPATH="${projectDir}:\${PYTHONPATH:-}"

    python -m cptools2.nextflow_chunking chunk \
        --index-csv ${image_sets_csv} \
        --output-dir chunks \
        --chunk-size ${params.chunk_size} \
        ${maxChunksFlag}
    """
}
