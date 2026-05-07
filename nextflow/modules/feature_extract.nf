// Feature extraction — role-agnostic process
//
// Runs DeepProfiler, DINOv2, or other feature extraction tool.
// Container and command are selected via params.feature_extraction_tool.
//
// Input:  tuple(plate_id, corrected_dir, chunk_manifest, locations_dir)
// Output: tuple(plate_id, features_dir)

process FEATURE_EXTRACT {
    tag "${plate_id}:${chunk_manifest.simpleName}"
    label 'feature_extract'
    label 'gpu'
    publishDir "${params.output_dir}/${plate_id}/features/${chunk_manifest.simpleName}", mode: 'copy'

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
    tuple val(plate_id), path(corrected_dir), path(chunk_manifest), path(locations_dir)

    output:
    tuple val(plate_id), path("features"), emit: features

    script:
    if (params.feature_extraction_tool == 'deepprofiler') {
        def weights_path = params.feature_extraction_weights && params.feature_extraction_weights.toString() != 'null'
            ? params.feature_extraction_weights.toString()
            : ''
        """
        mkdir -p features

        # Set up DeepProfiler project structure
        mkdir -p dp_project/outputs/cell_painting/checkpoint

        if [ -n "\${CPTOOLS2_PROJECT_ROOT:-}" ]; then
            export PYTHONPATH="\${CPTOOLS2_PROJECT_ROOT}:\${PYTHONPATH:-}"
        fi

        python -m cptools2.nextflow_chunking deepprofiler-package \\
            --chunk-manifest ${chunk_manifest} \\
            --locations-dir ${locations_dir} \\
            --output-root dp_project/inputs \\
            --config-path ${params.feature_extraction_config}

        test -f dp_project/inputs/metadata/index.csv
        test -d dp_project/inputs/locations
        test -d dp_project/inputs/images/${plate_id}
        test -f dp_project/inputs/config/config.json

        # Link model weights if configured
        if [ -n "${weights_path}" ]; then
            if [ ! -f "${weights_path}" ]; then
                echo "Configured DeepProfiler weights do not exist: ${weights_path}" >&2
                exit 1
            fi
            ln -s "${weights_path}" dp_project/outputs/cell_painting/checkpoint/
        fi

        # Run DeepProfiler
        python -m deepprofiler \\
            --root dp_project/ \\
            --config config.json \\
            --metadata index.csv \\
            --exp cell_painting \\
            --gpu 0 \\
            profile

        # Move output features to standard location
        test -d dp_project/outputs/cell_painting/features
        feature_count=\$(find dp_project/outputs/cell_painting/features -type f | wc -l)
        if [ "\$feature_count" -eq 0 ]; then
            echo "DeepProfiler completed but produced no feature files" >&2
            exit 1
        fi
        cp -r dp_project/outputs/cell_painting/features/* features/
        """
    } else if (params.feature_extraction_tool == 'dinov2')
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
