// CPU-only feature export from native DeepProfiler outputs.
//
// Input:  tuple(plate_id, feature_dirs)
// Output:
//   tuple(plate_id, features_dir)              for scratch publish
//   tuple(plate_id, features/feature_export_status.csv)
//   tuple(plate_id, stageout_dir)              for destination stage-out

process EXPORT_FEATURES {
    tag "${plate_id}"
    label 'feature_export'
    errorStrategy 'ignore'
    publishDir "${params.output_dir}/${plate_id}", mode: 'copy', saveAs: { filename ->
        def published = filename.toString()
        published == 'feature_export_stageout' || published.startsWith('feature_export_stageout/')
            ? null
            : published
    }

    input:
    tuple val(plate_id), val(chunk_ids), val(feature_dirs)

    output:
    tuple val(plate_id), path("features"), emit: features
    tuple val(plate_id), path("features/feature_export_status.csv"), emit: status
    tuple val(plate_id), path("feature_export_stageout"), emit: stageout

    script:
    def chunk_ids_json = groovy.json.JsonOutput.toJson(chunk_ids.collect { it.toString() })
    def feature_dirs_json = groovy.json.JsonOutput.toJson(feature_dirs.collect { it.toString() })
    def export_formats = params.feature_export_formats instanceof List
        ? params.feature_export_formats.collect { it.toString().trim() }.findAll { it }
        : params.feature_export_formats.toString().tokenize(',').collect { it.trim() }
    def export_suffixes = export_formats.collect { it == 'parquet' ? '.parquet' : '.csv' }
    def python_cmd = params.feature_export_python ?: 'python'
    def export_formats_json = groovy.json.JsonOutput.toJson(export_formats)
    def export_suffixes_json = groovy.json.JsonOutput.toJson(export_suffixes)
    def run_label_json = groovy.json.JsonOutput.toJson(params.feature_export_run_label)
    """
    set -euo pipefail

    export PYTHONPATH="\${CPTOOLS2_PROJECT_ROOT}:\${PYTHONPATH:-}"

    ${python_cmd} - <<'PY'
import json
import shutil
from pathlib import Path

from cptools2.feature_export.contract import FeatureExportRequest, normalise_formats
from cptools2.feature_export.deepprofiler import export_deepprofiler
from cptools2.feature_export.join import stage_feature_payloads
from cptools2.feature_export.reports import (
    classify_plate_export_failure,
    write_plate_export_report,
)

plate_id = '''${plate_id}'''
chunk_ids = json.loads('''${chunk_ids_json}''')
feature_dirs = [Path(path) for path in json.loads('''${feature_dirs_json}''')]
export_formats = json.loads('''${export_formats_json}''')
export_suffixes = json.loads('''${export_suffixes_json}''')
run_label = json.loads('''${run_label_json}''')
export_input_dir = Path("export_input")
features_dir = Path("features")
stageout_dir = Path("feature_export_stageout")
manifest_name = "export_input_manifest.csv"
manifest_copy_path = features_dir / manifest_name
stems = [
    "deepprofiler_manifest",
    "deepprofiler_sites",
    "deepprofiler_cells",
    "deepprofiler_quality",
]

if export_input_dir.exists():
    shutil.rmtree(export_input_dir)
if features_dir.exists():
    shutil.rmtree(features_dir)
features_dir.mkdir(parents=True, exist_ok=True)

def publish_stageout():
    if stageout_dir.exists():
        shutil.rmtree(stageout_dir)
    stageout_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(features_dir, stageout_dir / "features", dirs_exist_ok=True)

try:
    stage_feature_payloads(
        plate_id=plate_id,
        chunk_ids=chunk_ids,
        feature_dirs=feature_dirs,
        export_input_dir=export_input_dir,
    )
    shutil.copy2(export_input_dir / manifest_name, manifest_copy_path)
    request = FeatureExportRequest(
        extractor='${params.feature_extraction_tool}',
        features_dir=export_input_dir,
        output_dir=features_dir,
        formats=normalise_formats(export_formats),
        nan_object_fail_fraction=${params.feature_export_nan_object_fail_fraction},
        plate_id=plate_id,
        run_label=run_label,
    )
    export_deepprofiler(request)
except (FileNotFoundError, ValueError) as exc:
    failure_stage = classify_plate_export_failure(exc)
    if failure_stage is None:
        raise
    write_plate_export_report(
        output_dir=features_dir,
        plate_id=plate_id,
        export_status="failed",
        failure_stage=failure_stage,
        failure_reason=str(exc),
        tables_present=False,
        manifest_path=manifest_name if manifest_copy_path.exists() else "",
    )
    publish_stageout()
else:
    missing = []
    for stem in stems:
        for suffix in export_suffixes:
            path = features_dir / "tables" / f"{stem}{suffix}"
            if not path.is_file():
                missing.append(str(path))
    if missing:
        raise SystemExit(
            "Missing expected feature export artifact(s): " + ", ".join(missing)
        )
    write_plate_export_report(
        output_dir=features_dir,
        plate_id=plate_id,
        export_status="exported",
        failure_stage="",
        failure_reason="",
        tables_present=True,
        manifest_path=manifest_name,
    )
    publish_stageout()
PY
    """
}
