// Stage results from scratch back to DataStore
//
// Runs on the staging queue (-q staging) which has DataStore access.
//
// Input:  tuple(plate_id, results_dir)
// Output: tuple(plate_id, val(true)), emit: done
//         tuple(plate_id, path("stage_out_evidence")), emit: evidence

process STAGE_OUT {
    tag "${plate_id}"
    label 'staging'

    input:
    tuple val(plate_id), path(results_dir)

    output:
    tuple val(plate_id), val(true), emit: done
    tuple val(plate_id), path("stage_out_evidence"), emit: evidence

    script:
    def destination = params.data_destination ?: params.output_dir ?: ""
    def batchName = params.batch_name ?: "batch_unknown"
    def durableEvidenceDir = params.output_dir ? "${params.output_dir}/stage_out_evidence/${batchName}/${plate_id}" : ""
    """
    set -euo pipefail

    EVIDENCE_DIR="stage_out_evidence"
    RSYNC_LOG="\$EVIDENCE_DIR/rsync.log"
    DURABLE_EVIDENCE_DIR="${durableEvidenceDir}"
    rm -rf "\$EVIDENCE_DIR"
    mkdir -p "\$EVIDENCE_DIR"

    if [ -n "${destination}" ]; then
        DEST="${destination}/${plate_id}"
        mkdir -p "\$DEST"

        set +e
        rsync -rtl --partial --timeout=300 \
            --delay-updates \
            --itemize-changes \
            --out-format='RSYNC_ITEM\t%i\t%l\t%n%L' \
            --stats \
            --log-file="\$RSYNC_LOG" \
            ${results_dir}/ "\$DEST/"
        rsync_exit_code=\$?
        set -e

        export STAGE_OUT_PLATE_ID="${plate_id}"
        export STAGE_OUT_DESTINATION="\$DEST"
        export STAGE_OUT_RSYNC_EXIT_CODE="\$rsync_exit_code"
        export STAGE_OUT_RSYNC_LOG="\$RSYNC_LOG"
        export STAGE_OUT_RESULTS_DIR="${results_dir}"
        export STAGE_OUT_DURABLE_EVIDENCE_DIR="\$DURABLE_EVIDENCE_DIR"

        python - <<'PY'
import json
import os
from pathlib import Path

plate_id = os.environ["STAGE_OUT_PLATE_ID"]
destination = os.environ["STAGE_OUT_DESTINATION"]
rsync_exit_code = int(os.environ["STAGE_OUT_RSYNC_EXIT_CODE"])
rsync_log = os.environ["STAGE_OUT_RSYNC_LOG"]
results_dir = Path(os.environ["STAGE_OUT_RESULTS_DIR"])
durable_evidence_dir = os.environ["STAGE_OUT_DURABLE_EVIDENCE_DIR"]
dest_path = Path(destination)
evidence_dir = Path("stage_out_evidence")

if results_dir.name == "feature_export_stageout":
    required_artifacts = [
        "features/feature_export_status.csv",
        "features/tables/deepprofiler_manifest.csv",
        "features/tables/deepprofiler_sites.csv",
        "features/tables/deepprofiler_cells.csv",
        "features/tables/deepprofiler_quality.csv",
    ]
elif results_dir.name == "feature_export_summary":
    required_artifacts = [
        "feature_export_summary.csv",
        "README.md",
    ]
else:
    required_artifacts = ["."]

required_artifacts_present = all(
    (dest_path / artifact).exists() if artifact != "." else dest_path.exists()
    for artifact in required_artifacts
)
verification_status = (
    "verified" if rsync_exit_code == 0 and required_artifacts_present else "unverified"
)

verification = {
    "plate_id": plate_id,
    "destination": destination,
    "rsync_exit_code": rsync_exit_code,
    "rsync_log": rsync_log,
    "required_artifacts": required_artifacts,
    "required_artifacts_present": required_artifacts_present,
    "durable_evidence_dir": durable_evidence_dir,
    "verification_status": verification_status,
}

evidence_dir.mkdir(parents=True, exist_ok=True)
(evidence_dir / "verification.json").write_text(
    json.dumps(verification, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

        if [ -n "\$DURABLE_EVIDENCE_DIR" ]; then
            mkdir -p "\$DURABLE_EVIDENCE_DIR"
            cp -r "\$EVIDENCE_DIR"/. "\$DURABLE_EVIDENCE_DIR"/
        fi

        if [ "\$rsync_exit_code" -ne 0 ]; then
            exit "\$rsync_exit_code"
        fi

        echo "Destaged results for plate ${plate_id} to \$DEST"
    else
        printf 'No data_destination or output_dir specified, skipping destaging for plate %s\n' "${plate_id}" > "\$RSYNC_LOG"
        python - <<'PY'
import json
from pathlib import Path

evidence_dir = Path("stage_out_evidence")
verification = {
    "plate_id": "${plate_id}",
    "destination": "",
    "rsync_exit_code": None,
    "rsync_log": "stage_out_evidence/rsync.log",
    "required_artifacts": [],
    "required_artifacts_present": False,
    "durable_evidence_dir": "${durableEvidenceDir}",
    "verification_status": "unverified",
}
(evidence_dir / "verification.json").write_text(
    json.dumps(verification, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
        if [ -n "\$DURABLE_EVIDENCE_DIR" ]; then
            mkdir -p "\$DURABLE_EVIDENCE_DIR"
            cp -r "\$EVIDENCE_DIR"/. "\$DURABLE_EVIDENCE_DIR"/
        fi
        echo "No data_destination or output_dir specified, skipping destaging for plate ${plate_id}"
    fi
    """
}
