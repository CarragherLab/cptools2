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
    tuple val(plate_id), val(chunk_manifest.simpleName), path("features"), emit: export_inputs

    script:
    if (params.feature_extraction_tool == 'deepprofiler') {
        def weights_path = params.feature_extraction_weights && params.feature_extraction_weights.toString() != 'null'
            ? params.feature_extraction_weights.toString()
            : ''
        """
        mkdir -p features

        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT start ==='
        hostname || true
        echo "CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-unset}"
        nvidia-smi -L || true
        nvidia-smi --query-gpu=index,name,memory.total,memory.used --format=csv || true
        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT end ==='

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

        location_count=\$(python - <<'PY'
import csv
from pathlib import Path

count = 0
for path in Path("dp_project/inputs/locations").rglob("*.csv"):
    with path.open(newline="") as handle:
        count += sum(1 for _ in csv.DictReader(handle))
print(count)
PY
        )
        if [ "\$location_count" -eq 0 ]; then
            printf 'plate_id\\tchunk_id\\treason\\n%s\\t%s\\tno_cells\\n' "${plate_id}" "${chunk_manifest.simpleName}" > features/no_cells.tsv
            exit 0
        fi

        # Link model weights if configured
        if [ -n "${weights_path}" ]; then
            if [ ! -f "${weights_path}" ]; then
                echo "Configured DeepProfiler weights do not exist: ${weights_path}" >&2
                exit 1
            fi
            ln -sf "${weights_path}" dp_project/outputs/cell_painting/checkpoint/
        fi

        cat > run_deepprofiler.py <<'PY'
import os
import subprocess
import sys

allow_growth = os.environ.get("CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH", "false").lower()

if allow_growth in {"1", "true", "yes"}:
    os.environ["TF_FORCE_GPU_ALLOW_GROWTH"] = "true"

print("CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=" + allow_growth, flush=True)
print("TF_FORCE_GPU_ALLOW_GROWTH=" + os.environ.get("TF_FORCE_GPU_ALLOW_GROWTH", "unset"), flush=True)

cmd = [
    sys.executable,
    "-m",
    "deepprofiler",
    "--root",
    "dp_project/",
    "--config",
    "config.json",
    # --metadata index.csv
    "--metadata",
    "index.csv",
    "--exp",
    "cell_painting",
    "--gpu",
    "0",
    "profile",
]
result = subprocess.run(cmd, check=False)
if result.returncode < 0:
    os.kill(os.getpid(), -result.returncode)
raise SystemExit(result.returncode)
PY

        run_deepprofiler_command() {
            python run_deepprofiler.py
        }

        if [ "\${CPTOOLS2_DEEPPROFILER_HOST_LOCK:-false}" = "true" ]; then
            lock_dir="\${CPTOOLS2_DEEPPROFILER_LOCK_DIR:-\${PWD}/deepprofiler-host-lock}"
            mkdir -p "\$lock_dir"
            host_name="\$(hostname -f 2>/dev/null || hostname)"
            safe_host_name="\$(printf '%s' "\$host_name" | tr -c 'A-Za-z0-9_.-' '_')"
            lock_file="\$lock_dir/\${safe_host_name}.lock"
            echo "DeepProfiler host lock enabled: \$lock_file"
            flock "\$lock_file" python run_deepprofiler.py
        else
            run_deepprofiler_command
        fi

        # Move output features to standard location
        test -d dp_project/outputs/cell_painting/features
        feature_count=\$(find dp_project/outputs/cell_painting/features -type f | wc -l)
        if [ "\$feature_count" -eq 0 ]; then
            echo "DeepProfiler completed but produced no feature files" >&2
            exit 1
        fi
        cp -r dp_project/outputs/cell_painting/features/* features/

        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT post ==='
        nvidia-smi --query-gpu=index,name,memory.total,memory.used --format=csv || true
        echo '=== cptools2 GPU diagnostics: FEATURE_EXTRACT post end ==='
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
