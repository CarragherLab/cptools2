// Run-level summary for plate feature export statuses.
//
// Input:
//   val(expected_plate_ids)
//   path(status_paths)
//
// Output:
//   path("feature_export_summary")  for scratch publish and stage-out

process SUMMARISE_FEATURE_EXPORTS {
    tag "feature_export_summary"
    label 'feature_export'
    publishDir "${params.output_dir}", mode: 'copy'

    input:
    val expected_plate_ids
    path status_paths, stageAs: 'feature_export_status??/feature_export_status.csv'

    output:
    path("feature_export_summary"), emit: summary

    script:
    def python_cmd = params.feature_export_python ?: 'python'
    def expected_plate_ids_json = groovy.json.JsonOutput.toJson(
        expected_plate_ids.collect { it.toString() }
    )
    def status_paths_json = groovy.json.JsonOutput.toJson(
        status_paths.collect { it.toString() }
    )
    """
    set -euo pipefail

    export PYTHONPATH="\${CPTOOLS2_PROJECT_ROOT}:\${PYTHONPATH:-}"

    ${python_cmd} - <<'PY'
import json
from pathlib import Path

from cptools2.feature_export.reports import write_feature_export_summary

expected_plate_ids = json.loads('''${expected_plate_ids_json}''')
status_paths = [Path(path) for path in json.loads('''${status_paths_json}''')]

write_feature_export_summary(
    output_dir=Path("feature_export_summary"),
    expected_plate_ids=expected_plate_ids,
    observed_status_paths=status_paths,
)
PY
    """
}
