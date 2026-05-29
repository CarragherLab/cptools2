from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_nextflow_container_tags_match_local_builds():
    config = (ROOT / "nextflow" / "conf" / "containers.config").read_text()

    assert "cellprofiler/cellprofiler:4.2.8" in config
    assert "cptools2/deepprofiler:1.0" in config
    assert "cptools2/cellpose_sam:1.0" in config


def test_nextflow_main_accepts_documented_stage_aliases():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()

    assert "hasStage('illum')" in main_nf
    assert "hasStage('segment')" in main_nf
    assert "hasStage('extract')" in main_nf
    assert "hasStage('illum_calculate')" in main_nf
    assert "hasStage('segmentation')" in main_nf
    assert "hasStage('feature_extract')" in main_nf


def test_nextflow_chunks_are_fanned_out_before_processing():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()

    assert "CHUNK_IMAGESETS.out.chunks.flatMap" in main_nf
    assert "chunk_manifests instanceof List" in main_nf
    assert (
        "def manifests = chunk_manifests instanceof List ? chunk_manifests : "
        "[chunk_manifests]"
        in main_nf
    )
    assert "manifests.collect" in main_nf
    assert "tuple(plate_id, staged_plate_dir, chunk_manifest)" in main_nf


def test_nextflow_publish_dirs_do_not_copy_upstream_image_dirs():
    illum = (ROOT / "nextflow" / "modules" / "illum_calculate.nf").read_text()
    cellpose = (ROOT / "nextflow" / "modules" / "cellpose_segmentation.nf").read_text()

    assert "pattern: 'illum_functions/**'" in illum
    assert "pattern: 'cellpose_masks/**'" in cellpose


def test_nextflow_feature_extract_deepprofiler_uses_packager_contract():
    feature_extract = (ROOT / "nextflow" / "modules" / "feature_extract.nf").read_text()

    assert "deepprofiler-package" in feature_extract
    assert (
        'export PYTHONPATH="\\${CPTOOLS2_PROJECT_ROOT}:\\${PYTHONPATH:-}"'
        in feature_extract
    )
    assert "--metadata index.csv" in feature_extract
    assert "--output-root dp_project/inputs" in feature_extract
    assert "dp_project/inputs/metadata/index.csv" in feature_extract
    assert "dp_project/inputs/locations" in feature_extract
    assert "dp_project/inputs/images/${plate_id}" in feature_extract
    assert "dp_project/inputs/config/config.json" in feature_extract
    assert "location_count=\\$(python" in feature_extract
    assert "features/no_cells.tsv" in feature_extract
    assert "no_cells" in feature_extract
    assert "Configured DeepProfiler weights do not exist" in feature_extract
    assert "params.feature_extraction_weights.toString() != 'null'" in feature_extract
    assert "DeepProfiler completed but produced no feature files" in feature_extract
    assert (
        "tuple val(plate_id), val(chunk_manifest.simpleName), "
        'path("features"), emit: export_inputs'
    ) in feature_extract
    assert (
        "cp -r dp_project/outputs/cell_painting/features/* features/" in feature_extract
    )
    assert (
        "cp -r dp_project/outputs/cell_painting/features/* features/ "
        "2>/dev/null || true" not in feature_extract
    )
    assert "dp_project/inputs/metadata/locations" not in feature_extract
    assert (
        "cp ${chunk_manifest} dp_project/inputs/metadata/chunk_manifest.csv"
        not in feature_extract
    )


def test_deepprofiler_feature_extract_has_scalability_guards():
    feature_extract = (ROOT / "nextflow" / "modules" / "feature_extract.nf").read_text()

    assert "TF_FORCE_GPU_ALLOW_GROWTH" in feature_extract
    assert "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH" in feature_extract
    assert "CPTOOLS2_DEEPPROFILER_HOST_LOCK" in feature_extract
    assert 'flock "\\$lock_file" python run_deepprofiler.py' in feature_extract
    assert 'host_name="\\$(hostname -f 2>/dev/null || hostname)"' in feature_extract
    assert "tr -c 'A-Za-z0-9_.-' '_'" in feature_extract


def test_gpu_processes_emit_best_effort_gpu_diagnostics():
    cellpose = (ROOT / "nextflow" / "modules" / "cellpose_segmentation.nf").read_text()
    feature_extract = (ROOT / "nextflow" / "modules" / "feature_extract.nf").read_text()

    for text, process_name in [
        (cellpose, "CELLPOSE_SEGMENT"),
        (feature_extract, "FEATURE_EXTRACT"),
    ]:
        assert f"cptools2 GPU diagnostics: {process_name} start" in text
        assert f"cptools2 GPU diagnostics: {process_name} post" in text
        assert "CUDA_VISIBLE_DEVICES=\\${CUDA_VISIBLE_DEVICES:-unset}" in text
        assert "nvidia-smi -L || true" in text
        assert (
            "nvidia-smi --query-gpu=index,name,memory.total,memory.used "
            "--format=csv || true" in text
        )


def test_nextflow_stage_out_uses_data_destination_and_cellpose_masks():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()
    stage_out = (ROOT / "nextflow" / "modules" / "stage_out.nf").read_text()

    assert "params.data_destination ?: params.output_dir" in stage_out
    assert (
        'tuple val(plate_id), path("stage_out_evidence"), emit: evidence'
        in stage_out
    )
    assert "rsync.log" in stage_out
    assert "--itemize-changes" in stage_out
    assert "--out-format='RSYNC_ITEM\\t%i\\t%l\\t%n%L'" in stage_out
    assert "--stats" in stage_out
    assert "--delay-updates" in stage_out
    assert "${params.output_dir}/stage_out_evidence/" in stage_out
    assert "params.batch_name ?: \"batch_unknown\"" in stage_out
    assert "DURABLE_EVIDENCE_DIR" in stage_out
    assert "cp -r \"\\$EVIDENCE_DIR\"/. \"\\$DURABLE_EVIDENCE_DIR\"/" in stage_out
    assert 'tuple val(plate_id), val(true), emit: done' in stage_out
    assert "CELLPOSE_SEGMENT.out.masks.map" in main_nf
    assert "tuple(plate_id, masks_dir)" in main_nf


def test_nextflow_stage_out_verification_json_contract_covers_feature_exports():
    stage_out = (ROOT / "nextflow" / "modules" / "stage_out.nf").read_text()

    assert "verification.json" in stage_out
    assert '"plate_id": plate_id' in stage_out
    assert '"destination": destination' in stage_out
    assert '"rsync_exit_code": rsync_exit_code' in stage_out
    assert '"rsync_log": rsync_log' in stage_out
    assert '"required_artifacts": required_artifacts' in stage_out
    assert '"required_artifacts_present": required_artifacts_present' in stage_out
    assert '"durable_evidence_dir": durable_evidence_dir' in stage_out
    assert '"verification_status": verification_status' in stage_out
    assert "feature_export_stageout" in stage_out
    assert "features/feature_export_status.csv" in stage_out
    assert "features/tables/deepprofiler_manifest.csv" in stage_out
    assert "features/tables/deepprofiler_sites.csv" in stage_out
    assert "features/tables/deepprofiler_cells.csv" in stage_out
    assert "features/tables/deepprofiler_quality.csv" in stage_out
    assert "feature_export_summary.csv" in stage_out
    assert '"required_artifacts_present": required_artifacts_present' in stage_out


def test_nextflow_feature_export_is_cpu_only_and_plate_grouped():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()
    export_features = (ROOT / "nextflow" / "modules" / "export_features.nf").read_text()
    summary_features = (
        ROOT / "nextflow" / "modules" / "summarise_feature_exports.nf"
    ).read_text()
    eddie_config = (ROOT / "nextflow" / "conf" / "eddie.config").read_text()

    assert "include { EXPORT_FEATURES } from './modules/export_features'" in main_nf
    assert (
        "include { SUMMARISE_FEATURE_EXPORTS } from "
        "'./modules/summarise_feature_exports'"
        in main_nf
    )
    assert "params.feature_export_enabled" in main_nf
    assert "params.feature_export_formats" in main_nf
    assert "params.feature_export_python" in main_nf
    assert "params.feature_export_nan_object_fail_fraction" in main_nf
    assert "params.feature_export_run_label" in main_nf
    assert "ch_expected_plate_ids = ch_plates.map" in main_nf
    assert "FEATURE_EXTRACT.out.export_inputs" in main_nf
    assert "groupTuple(by: 0)" in main_nf
    assert "run_feature_export" in main_nf
    assert "EXPORT_FEATURES(ch_feature_export_by_plate)" in main_nf
    assert "SUMMARISE_FEATURE_EXPORTS(" in main_nf
    assert (
        "EXPORT_FEATURES.out.status\n"
        "                    .map { plate_id, status_path -> status_path }\n"
        "                    .collect()\n"
        "                    .ifEmpty([])"
        in main_nf
    )
    assert "ch_extract_stageout = FEATURE_EXTRACT.out.features" in main_nf
    assert "ch_extract_stageout.mix(EXPORT_FEATURES.out.stageout)" in main_nf
    assert "tuple('feature_export_summary', summary_dir)" in main_nf
    assert "STAGE_OUT(ch_extract_stageout)" in main_nf
    assert "process EXPORT_FEATURES" in export_features
    assert "label 'feature_export'" in export_features
    assert "errorStrategy 'ignore'" in export_features
    assert "label 'gpu'" not in export_features
    assert (
        "publishDir \"${params.output_dir}/${plate_id}\", mode: 'copy'"
        in export_features
    )
    assert "saveAs: { filename ->" in export_features
    assert "published == 'feature_export_stageout'" in export_features
    assert "published.startsWith('feature_export_stageout/')" in export_features
    assert (
        "tuple val(plate_id), path(\"features/feature_export_status.csv\")"
        in export_features
    )
    assert "write_plate_export_report(" in export_features
    assert "classify_plate_export_failure(" in export_features
    assert "normalise_formats(export_formats)" in export_features
    assert "export_deepprofiler(request)" in export_features
    assert (
        "from cptools2.feature_export.join import stage_feature_payloads"
        in export_features
    )
    assert "stage_feature_payloads(" in export_features
    assert "${python_cmd} - <<'PY'" in export_features
    assert export_features.count("${python_cmd} - <<'PY'") == 1
    assert "Missing expected feature export artifact(s)" in export_features
    assert "feature_export_status.csv" in export_features
    assert "feature_export_stageout" in export_features
    assert "publish_stageout()" in export_features
    assert "manifest_copy_path" in export_features
    assert "process SUMMARISE_FEATURE_EXPORTS" in summary_features
    assert 'path("feature_export_summary"), emit: summary' in summary_features
    assert "publishDir \"${params.output_dir}\", mode: 'copy'" in summary_features
    assert (
        "path status_paths, stageAs: "
        "'feature_export_status??/feature_export_status.csv'"
        in summary_features
    )
    assert "write_feature_export_summary(" in summary_features
    assert "expected_plate_ids.collect" in summary_features
    assert "status_paths.collect" in summary_features
    assert "withLabel: 'feature_export'" in eddie_config
    assert "memory = '8.GB'" in eddie_config
    assert 'feature_export_python = "${cptools2Venv}/bin/python"' in eddie_config


def test_nextflow_observers_are_param_gated():
    """Diagnostics modes must control config-level Nextflow observers."""
    text = (ROOT / "nextflow" / "nextflow.config").read_text()

    assert "timeline {" in text
    assert "report {" in text
    assert "trace {" in text
    assert "enabled = params.enable_timeline" in text
    assert "enabled = params.enable_report" in text
    assert "enabled = params.enable_trace" in text


def test_nextflow_stage_in_fails_on_rsync_or_empty_staging():
    stage_in = (ROOT / "nextflow" / "modules" / "stage_in.nf").read_text()

    assert "set -euo pipefail" in stage_in
    assert "rm -rf staged_images" in stage_in
    assert "--chmod=Du+rwx,Dg+rx,Do-rwx,Fu+rw,Fg+r,Fo-rwx" in stage_in
    assert "image_count=\\$(find staged_images -name '*.tif' | wc -l)" in stage_in
    assert 'if [ "\\$image_count" -eq 0 ]; then' in stage_in
    assert "exit 1" in stage_in


def test_nextflow_index_caps_polars_thread_pools():
    index = (ROOT / "nextflow" / "modules" / "build_imageset_index.nf").read_text()

    assert "export POLARS_MAX_THREADS=1" in index
    assert "export RAYON_NUM_THREADS=1" in index
    assert "export OMP_NUM_THREADS=1" in index
    assert "export MKL_NUM_THREADS=1" in index


def test_loop230_config_is_scratch_self_contained_and_staged():
    config_path = ROOT / "config" / "loop230-sarah-screen.yaml"
    config = yaml.safe_load(config_path.read_text())

    output_dir = config["output_dir"].rstrip("/")
    assert config["stage_data"] is True
    assert "/datastore/" in config["input_dir"]
    assert config["plates"] == ["example-plate-001"]
    assert config["stages"] == ["illum", "segment", "extract"]
    assert config["data_destination"].startswith("${CPTOOLS2_PROJECT_ROOT}/results/")
    assert config["data_destination"].rstrip("/") != output_dir

    scratch_bound_paths = [
        config["illum_pipeline_calculate"],
        config["illum_pipeline_apply"],
        config["seg_pipeline"],
        config["pipeline"],
        config["commands location"],
        config["feature_extraction"]["config"],
    ]
    for path in scratch_bound_paths:
        assert path.startswith(output_dir + "/")
        assert "/home/" not in path


def test_container_dockerfiles_expose_working_cli_contracts():
    deep = (ROOT / "cptools2" / "dockerfiles" / "Dockerfile.deepprofiler").read_text()
    cellpose = (ROOT / "cptools2" / "dockerfiles" / "Dockerfile.cellpose").read_text()
    readme = (ROOT / "cptools2" / "dockerfiles" / "README.md").read_text()

    assert "/opt/DeepProfiler" in deep
    assert "PYTHONPATH=/opt/DeepProfiler" in deep
    assert "cellpose[gpu]" not in cellpose
    assert "cptools2/cellpose_sam:1.0" in readme
    assert "cptools2/cellpose-sam:1.0" not in readme
