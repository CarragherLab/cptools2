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


def test_nextflow_publish_dirs_do_not_copy_upstream_image_dirs():
    illum = (ROOT / "nextflow" / "modules" / "illum_calculate.nf").read_text()
    cellpose = (
        ROOT / "nextflow" / "modules" / "cellpose_segmentation.nf"
    ).read_text()

    assert "pattern: 'illum_functions/**'" in illum
    assert "pattern: 'cellpose_masks/**'" in cellpose


def test_nextflow_feature_extract_deepprofiler_uses_packager_contract():
    feature_extract = (
        ROOT / "nextflow" / "modules" / "feature_extract.nf"
    ).read_text()

    assert "deepprofiler-package" in feature_extract
    assert 'export PYTHONPATH="\\${CPTOOLS2_PROJECT_ROOT}:\\${PYTHONPATH:-}"' in feature_extract
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
    assert "cp -r dp_project/outputs/cell_painting/features/* features/" in feature_extract
    assert "cp -r dp_project/outputs/cell_painting/features/* features/ 2>/dev/null || true" not in feature_extract
    assert "dp_project/inputs/metadata/locations" not in feature_extract
    assert "cp ${chunk_manifest} dp_project/inputs/metadata/chunk_manifest.csv" not in feature_extract


def test_nextflow_stage_out_uses_data_destination_and_cellpose_masks():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()
    stage_out = (ROOT / "nextflow" / "modules" / "stage_out.nf").read_text()

    assert "params.data_destination ?: params.output_dir" in stage_out
    assert "CELLPOSE_SEGMENT.out.masks.map" in main_nf
    assert "tuple(plate_id, masks_dir)" in main_nf


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
