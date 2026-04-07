// Feature extraction — role-agnostic process
//
// Runs DeepProfiler, DINOv2, or other feature extraction tool.
// Container and command are selected via params.feature_extraction_tool.
//
// Input:  tuple(plate_id, corrected_dir, locations_dir)
// Output: tuple(plate_id, features_dir)

process FEATURE_EXTRACT {
    tag "${plate_id}"
    label 'feature_extract'
    label 'gpu'
    publishDir "${params.output_dir}/${plate_id}/features", mode: 'copy'

    container {
        switch (params.feature_extraction_tool) {
            case 'deepprofiler':
                return params.containers.deepprofiler
            case 'dinov2':
                return params.containers.dinov2
            default:
                return params.containers.deepprofiler
        }
    }

    input:
    tuple val(plate_id), path(corrected_dir), path(locations_dir)

    output:
    tuple val(plate_id), path("features"), emit: features

    script:
    if (params.feature_extraction_tool == 'deepprofiler')
        """
        mkdir -p features

        # Set up DeepProfiler project structure
        mkdir -p dp_project/inputs/images/${plate_id}
        mkdir -p dp_project/inputs/metadata/locations
        mkdir -p dp_project/inputs/config
        mkdir -p dp_project/outputs/cell_painting/checkpoint

        # Link corrected images
        ln -s \$(readlink -f ${corrected_dir})/* dp_project/inputs/images/${plate_id}/

        # Link location CSVs
        if [ -d "${locations_dir}/locations" ]; then
            ln -s \$(readlink -f ${locations_dir}/locations)/* dp_project/inputs/metadata/locations/
        else
            ln -s \$(readlink -f ${locations_dir})/*.csv dp_project/inputs/metadata/locations/ 2>/dev/null || true
        fi

        # Copy config
        cp ${params.feature_extraction_config} dp_project/inputs/config/config.json

        # Link model weights if provided
        if [ -n "${params.feature_extraction_weights}" ] && [ -f "${params.feature_extraction_weights}" ]; then
            ln -s ${params.feature_extraction_weights} dp_project/outputs/cell_painting/checkpoint/
        fi

        # Run DeepProfiler
        python -m deepprofiler \\
            --root dp_project/ \\
            --config config.json \\
            --exp cell_painting \\
            --gpu 0 \\
            profile

        # Move output features to standard location
        cp -r dp_project/outputs/cell_painting/features/* features/ 2>/dev/null || true
        """
    else if (params.feature_extraction_tool == 'dinov2')
        """
        mkdir -p features

        # DINOv2 feature extraction — placeholder command
        # Replace with actual DINOv2 extraction script when container is ready
        python -m extract_features \\
            --image-dir ${corrected_dir} \\
            --locations-dir ${locations_dir} \\
            --output-dir features \\
            --plate-id ${plate_id} \\
            --batch-size ${params.feature_extraction_batch_size}
        """
    else
        error "Unknown feature extraction tool: ${params.feature_extraction_tool}. Supported: deepprofiler, dinov2"
}
