// Cellpose segmentation for AI feature extraction paths.
//
// Input:  tuple(plate_id, corrected_dir, chunk_manifest_csv)
// Output: tuple(plate_id, corrected_dir, chunk_manifest_csv, masks_dir)

process CELLPOSE_SEGMENT {
    tag "${plate_id}:${chunk_manifest.simpleName}"
    label 'segmentation'
    label 'gpu'
    publishDir "${params.output_dir}/${plate_id}/cellpose/${chunk_manifest.simpleName}", mode: 'copy'

    container params.containers.cellpose_sam

    input:
    tuple val(plate_id), path(corrected_dir), path(chunk_manifest)

    output:
    tuple val(plate_id), path(corrected_dir), path(chunk_manifest), path("cellpose_masks"), emit: masks

    script:
    """
    mkdir -p cellpose_masks

    # Smoke-safe entry point for the AI segmentation stage. The full Cellpose
    # invocation will consume chunk_manifest once model/runtime parameters are
    # finalized. Keeping the manifest in the output makes the contract testable.
    cp ${chunk_manifest} cellpose_masks/chunk_manifest.csv
    python - <<'PY'
from pathlib import Path
Path("cellpose_masks/SEGMENTATION_PENDING.txt").write_text(
    "Cellpose chunk contract staged. Replace with model invocation.\\n"
)
PY
    """
}
